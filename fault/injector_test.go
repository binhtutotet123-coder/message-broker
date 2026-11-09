package fault

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"message-broker/models"
)

// countingDelivery records how many times the wrapped delivery function ran.
type countingDelivery struct {
	calls int
	err   error
}

func (c *countingDelivery) deliver(context.Context, models.MessageEnvelope, string) error {
	c.calls++
	return c.err
}

func testEnvelope(to string) models.MessageEnvelope {
	return models.MessageEnvelope{MessageID: "m1", From: "agent_A", To: to, Type: "request", Payload: map[string]interface{}{}}
}

// TestDisabledInjectorPassesThrough verifies the zero config changes nothing.
func TestDisabledInjectorPassesThrough(t *testing.T) {
	injector := New(nil)
	next := &countingDelivery{}
	if err := injector.Wrap(next.deliver)(context.Background(), testEnvelope("agent_B"), "trace"); err != nil {
		t.Fatal(err)
	}
	if next.calls != 1 {
		t.Fatalf("calls = %d, want 1", next.calls)
	}
	if stats := injector.Stats(); stats != (Stats{Passed: 1}) {
		t.Fatalf("stats = %+v, want one pass", stats)
	}
}

// TestDropSkipsWebhookButReportsSuccess models a message lost on the network.
func TestDropSkipsWebhookButReportsSuccess(t *testing.T) {
	injector := New(nil)
	if err := injector.SetConfig(Config{DropRate: 1}); err != nil {
		t.Fatal(err)
	}
	next := &countingDelivery{}
	if err := injector.Wrap(next.deliver)(context.Background(), testEnvelope("agent_B"), "trace"); err != nil {
		t.Fatalf("drop returned %v, want nil so the broker waits for an ACK", err)
	}
	if next.calls != 0 || injector.Stats().Dropped != 1 {
		t.Fatalf("calls = %d, stats = %+v, want webhook skipped and counted", next.calls, injector.Stats())
	}
}

// TestDuplicateSendsTwiceAndIgnoresCopyFailure verifies duplicates never cause extra retries.
func TestDuplicateSendsTwiceAndIgnoresCopyFailure(t *testing.T) {
	injector := New(nil)
	if err := injector.SetConfig(Config{DuplicateRate: 1}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	deliver := injector.Wrap(func(context.Context, models.MessageEnvelope, string) error {
		calls++
		if calls == 2 {
			return errors.New("copy failed")
		}
		return nil
	})
	if err := deliver(context.Background(), testEnvelope("agent_B"), "trace"); err != nil {
		t.Fatalf("duplicate error = %v, want nil because the original succeeded", err)
	}
	if calls != 2 || injector.Stats().Duplicated != 1 {
		t.Fatalf("calls = %d, stats = %+v, want two sends", calls, injector.Stats())
	}
}

// TestFailedDeliveryIsNotDuplicated keeps transport errors visible to the broker.
func TestFailedDeliveryIsNotDuplicated(t *testing.T) {
	injector := New(nil)
	if err := injector.SetConfig(Config{DuplicateRate: 1}); err != nil {
		t.Fatal(err)
	}
	next := &countingDelivery{err: errors.New("connection refused")}
	if err := injector.Wrap(next.deliver)(context.Background(), testEnvelope("agent_B"), "trace"); err == nil {
		t.Fatal("transport error was swallowed")
	}
	if next.calls != 1 || injector.Stats().Duplicated != 0 {
		t.Fatalf("calls = %d, stats = %+v, want a single failed send", next.calls, injector.Stats())
	}
}

// TestDelayWaitsAndRespectsAttemptDeadline covers both a short delay and a timeout.
func TestDelayWaitsAndRespectsAttemptDeadline(t *testing.T) {
	injector := New(nil)
	if err := injector.SetConfig(Config{DelayMS: 30}); err != nil {
		t.Fatal(err)
	}
	next := &countingDelivery{}
	deliver := injector.Wrap(next.deliver)

	started := time.Now()
	if err := deliver(context.Background(), testEnvelope("agent_B"), "trace"); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 30*time.Millisecond {
		t.Fatalf("delivery took %s, want at least 30ms", elapsed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := deliver(ctx, testEnvelope("agent_B"), "trace"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delay past deadline error = %v, want DeadlineExceeded", err)
	}
	if next.calls != 1 || injector.Stats().Delayed != 2 {
		t.Fatalf("calls = %d, stats = %+v, want only the first attempt delivered", next.calls, injector.Stats())
	}
}

// TestAgentFilterOnlyAffectsTargetAgent protects other teams when the broker is shared.
func TestAgentFilterOnlyAffectsTargetAgent(t *testing.T) {
	injector := New(nil)
	if err := injector.SetConfig(Config{DropRate: 1, Agent: "agent_B"}); err != nil {
		t.Fatal(err)
	}
	next := &countingDelivery{}
	deliver := injector.Wrap(next.deliver)
	if err := deliver(context.Background(), testEnvelope("team_08"), "trace"); err != nil {
		t.Fatal(err)
	}
	if err := deliver(context.Background(), testEnvelope("agent_B"), "trace"); err != nil {
		t.Fatal(err)
	}
	if next.calls != 1 || injector.Stats().Dropped != 1 {
		t.Fatalf("calls = %d, stats = %+v, want only agent_B dropped", next.calls, injector.Stats())
	}
}

// TestRatesUseRandomSource checks probabilistic faults against a scripted random source.
func TestRatesUseRandomSource(t *testing.T) {
	values := []float64{0.9, 0.1}
	injector := New(func() float64 {
		value := values[0]
		values = values[1:]
		return value
	})
	if err := injector.SetConfig(Config{DropRate: 0.5}); err != nil {
		t.Fatal(err)
	}
	next := &countingDelivery{}
	deliver := injector.Wrap(next.deliver)
	for range 2 {
		if err := deliver(context.Background(), testEnvelope("agent_B"), "trace"); err != nil {
			t.Fatal(err)
		}
	}
	if next.calls != 1 || injector.Stats().Dropped != 1 {
		t.Fatalf("calls = %d, stats = %+v, want 0.9 delivered and 0.1 dropped", next.calls, injector.Stats())
	}
}

// TestConfigValidation rejects out-of-range values.
func TestConfigValidation(t *testing.T) {
	for _, config := range []Config{
		{DropRate: -0.1},
		{DropRate: 1.1},
		{DuplicateRate: 2},
		{DelayMS: -1},
		{DelayMS: int(MaxDelay.Milliseconds()) + 1},
	} {
		if err := New(nil).SetConfig(config); err == nil {
			t.Errorf("SetConfig(%+v) succeeded, want error", config)
		}
	}
}

// TestHandlerUpdatesShowsAndResetsConfig exercises the admin API end to end.
func TestHandlerUpdatesShowsAndResetsConfig(t *testing.T) {
	injector := New(nil)
	handler := injector.Handler()

	put := httptest.NewRecorder()
	handler.ServeHTTP(put, httptest.NewRequest(http.MethodPut, "/admin/fault",
		strings.NewReader(`{"drop_rate":0.4,"delay_ms":200,"duplicate_rate":0.3,"agent":"agent_B"}`)))
	if put.Code != http.StatusOK {
		t.Fatalf("PUT status = %d: %s", put.Code, put.Body.String())
	}
	want := Config{DropRate: 0.4, DelayMS: 200, DuplicateRate: 0.3, Agent: "agent_B"}
	if got := injector.Config(); got != want {
		t.Fatalf("config = %+v, want %+v", got, want)
	}

	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/admin/fault", nil))
	var status statusResponse
	if err := json.Unmarshal(get.Body.Bytes(), &status); err != nil || status.Config != want {
		t.Fatalf("GET = (%+v, %v), want config %+v", status, err, want)
	}

	for _, body := range []string{
		`{"drop_rate":3}`,
		`{"drop_rte":0.5}`,
		`{"drop_rate":0.5} {}`,
		`not json`,
	} {
		bad := httptest.NewRecorder()
		handler.ServeHTTP(bad, httptest.NewRequest(http.MethodPut, "/admin/fault", strings.NewReader(body)))
		if bad.Code != http.StatusBadRequest {
			t.Errorf("PUT %s status = %d, want 400", body, bad.Code)
		}
	}
	if got := injector.Config(); got != want {
		t.Fatalf("rejected update changed config to %+v", got)
	}

	reset := httptest.NewRecorder()
	handler.ServeHTTP(reset, httptest.NewRequest(http.MethodDelete, "/admin/fault", nil))
	if reset.Code != http.StatusOK || injector.Config() != (Config{}) {
		t.Fatalf("DELETE = %d, config %+v, want faults off", reset.Code, injector.Config())
	}

	patch := httptest.NewRecorder()
	handler.ServeHTTP(patch, httptest.NewRequest(http.MethodPatch, "/admin/fault", nil))
	if patch.Code != http.StatusMethodNotAllowed {
		t.Fatalf("PATCH status = %d, want 405", patch.Code)
	}
}
