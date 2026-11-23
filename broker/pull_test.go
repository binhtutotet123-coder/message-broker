package broker

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"message-broker/models"
)

// newCountingBroker returns a broker whose webhook deliveries are only counted.
func newCountingBroker(t *testing.T, retryInterval time.Duration) (*Broker, *atomic.Int32) {
	t.Helper()
	var pushes atomic.Int32
	messageBroker, err := New(func(context.Context, models.MessageEnvelope, string) error {
		pushes.Add(1)
		return nil
	}, retryInterval)
	if err != nil {
		t.Fatal(err)
	}
	return messageBroker, &pushes
}

func pullEnvelope(messageID, to string) models.MessageEnvelope {
	envelope := testEnvelope(messageID)
	envelope.To = to
	return envelope
}

// TestReceiveHandsMessagesToPollingAgentUntilACK covers the pull flow: the worker
// leaves the message alone, Receive hands it over, and without an ACK it comes back.
func TestReceiveHandsMessagesToPollingAgentUntilACK(t *testing.T) {
	messageBroker, pushes := newCountingBroker(t, 150*time.Millisecond)
	ctx := context.Background()

	if got, err := messageBroker.Receive(ctx, "laptop", 10, 0); err != nil || len(got) != 0 {
		t.Fatalf("first poll = (%v, %v), want no messages", got, err)
	}
	if !messageBroker.IsPulling("laptop") {
		t.Fatal("agent that polled is not reported as pulling")
	}
	envelope := pullEnvelope("pull-1", "laptop")
	if _, _, err := messageBroker.Accept(envelope, "trace-pull"); err != nil {
		t.Fatal(err)
	}

	messageBroker.dispatchDue(ctx)
	if got := pushes.Load(); got != 0 {
		t.Fatalf("webhook pushes to a polling agent = %d, want 0", got)
	}

	first, err := messageBroker.Receive(ctx, "laptop", 10, 0)
	if err != nil || len(first) != 1 || first[0].Envelope.MessageID != "pull-1" || first[0].TraceID != "trace-pull" || first[0].Attempts != 1 {
		t.Fatalf("poll after send = (%+v, %v), want pull-1 attempt 1 with its trace", first, err)
	}
	if got, _ := messageBroker.Receive(ctx, "laptop", 10, 0); len(got) != 0 {
		t.Fatalf("immediate second poll = %+v, want nothing until the retry interval", got)
	}

	time.Sleep(200 * time.Millisecond)
	again, err := messageBroker.Receive(ctx, "laptop", 10, 0)
	if err != nil || len(again) != 1 || again[0].Attempts != 2 {
		t.Fatalf("poll without ACK = (%+v, %v), want the same message, attempt 2", again, err)
	}

	if _, err := messageBroker.Acknowledge("pull-1", "laptop", AckSuccess); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if got, _ := messageBroker.Receive(ctx, "laptop", 10, 0); len(got) != 0 {
		t.Fatalf("poll after ACK = %+v, want nothing", got)
	}
	if got := pushes.Load(); got != 0 {
		t.Fatalf("webhook pushes = %d, want 0", got)
	}
}

// TestReceiveOnlyReturnsOwnMessagesWithinLimit keeps agents from seeing each other's mail.
func TestReceiveOnlyReturnsOwnMessagesWithinLimit(t *testing.T) {
	messageBroker, _ := newCountingBroker(t, time.Minute)
	for _, envelope := range []models.MessageEnvelope{
		pullEnvelope("a-1", "agent-a"), pullEnvelope("a-2", "agent-a"), pullEnvelope("a-3", "agent-a"),
		pullEnvelope("b-1", "agent-b"),
	} {
		if _, _, err := messageBroker.Accept(envelope, "trace"); err != nil {
			t.Fatal(err)
		}
	}
	got, err := messageBroker.Receive(context.Background(), "agent-a", 2, 0)
	if err != nil || len(got) != 2 {
		t.Fatalf("limited poll = (%+v, %v), want 2 messages", got, err)
	}
	rest, _ := messageBroker.Receive(context.Background(), "agent-a", 10, 0)
	if len(rest) != 1 {
		t.Fatalf("second poll = %+v, want the remaining message", rest)
	}
	for _, message := range append(got, rest...) {
		if message.Envelope.To != "agent-a" {
			t.Fatalf("agent-a received %s addressed to %s", message.Envelope.MessageID, message.Envelope.To)
		}
	}
}

// TestReceiveWaitsForNewMessage checks long polling returns as soon as a message arrives.
func TestReceiveWaitsForNewMessage(t *testing.T) {
	messageBroker, _ := newCountingBroker(t, time.Minute)
	go func() {
		time.Sleep(200 * time.Millisecond)
		if _, _, err := messageBroker.Accept(pullEnvelope("late", "laptop"), "trace"); err != nil {
			t.Error(err)
		}
	}()
	started := time.Now()
	got, err := messageBroker.Receive(context.Background(), "laptop", 10, 5*time.Second)
	if err != nil || len(got) != 1 || got[0].Envelope.MessageID != "late" {
		t.Fatalf("long poll = (%+v, %v), want the late message", got, err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("long poll took %v, want it to return soon after the message arrived", elapsed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	started = time.Now()
	if got, err := messageBroker.Receive(ctx, "laptop", 10, 5*time.Second); err != nil || len(got) != 0 {
		t.Fatalf("canceled long poll = (%+v, %v), want empty", got, err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("canceled long poll took %v, want it to stop with its context", elapsed)
	}
}

// TestWorkerPushesAgainWhenAgentStopsPolling falls back to webhooks after the pull window.
func TestWorkerPushesAgainWhenAgentStopsPolling(t *testing.T) {
	messageBroker, pushes := newCountingBroker(t, time.Minute)
	messageBroker.pullWindow = 200 * time.Millisecond
	ctx := context.Background()

	if _, err := messageBroker.Receive(ctx, "laptop", 10, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := messageBroker.Accept(pullEnvelope("p-1", "laptop"), "trace"); err != nil {
		t.Fatal(err)
	}
	messageBroker.dispatchDue(ctx)
	if got := pushes.Load(); got != 0 {
		t.Fatalf("pushes while polling = %d, want 0", got)
	}

	time.Sleep(250 * time.Millisecond)
	if messageBroker.IsPulling("laptop") {
		t.Fatal("agent still counts as polling after the pull window")
	}
	messageBroker.dispatchDue(ctx)
	if got := pushes.Load(); got != 1 {
		t.Fatalf("pushes after the agent stopped polling = %d, want 1", got)
	}
}

// TestStaticPullAgentsAreNeverPushed covers AGENT_ROUTES entries like "laptop=pull".
func TestStaticPullAgentsAreNeverPushed(t *testing.T) {
	messageBroker, pushes := newCountingBroker(t, time.Minute)
	messageBroker.MarkPullAgents("laptop")
	if !messageBroker.IsPulling("laptop") {
		t.Fatal("static pull agent is not reported as pulling")
	}
	if _, _, err := messageBroker.Accept(pullEnvelope("s-1", "laptop"), "trace"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := messageBroker.Accept(pullEnvelope("s-2", "webhook-agent"), "trace"); err != nil {
		t.Fatal(err)
	}
	messageBroker.dispatchDue(context.Background())
	if got := pushes.Load(); got != 1 {
		t.Fatalf("pushes = %d, want 1 (only the webhook agent)", got)
	}
	if got, _ := messageBroker.Receive(context.Background(), "laptop", 10, 0); len(got) != 1 {
		t.Fatalf("static pull agent poll = %+v, want its message", got)
	}
}

// TestWorkerLeavesNoWebhookRecipientsForReceive covers a registry agent whose endpoint
// is https://pull.invalid: no retry storm, no counted attempt, message ready to poll.
func TestWorkerLeavesNoWebhookRecipientsForReceive(t *testing.T) {
	var calls atomic.Int32
	messageBroker, err := New(func(context.Context, models.MessageEnvelope, string) error {
		calls.Add(1)
		return fmt.Errorf("%w: registry endpoint is https://pull.invalid", ErrPullOnly)
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, _, err := messageBroker.Accept(pullEnvelope("np-1", "registry_laptop"), "trace"); err != nil {
		t.Fatal(err)
	}

	messageBroker.dispatchDue(ctx)
	messageBroker.dispatchDue(ctx)
	if got := calls.Load(); got != 1 {
		t.Fatalf("delivery calls = %d, want 1 (then the agent is left for GET /receive)", got)
	}
	if snapshot, _, _ := messageBroker.Get("np-1"); snapshot.Attempts != 0 || snapshot.Status != StatusPending {
		t.Fatalf("after no-webhook attempt: %+v, want Pending with 0 attempts", snapshot)
	}
	if !messageBroker.IsPulling("registry_laptop") {
		t.Fatal("agent without webhook is not treated as pulling")
	}
	got, err := messageBroker.Receive(ctx, "registry_laptop", 10, 0)
	if err != nil || len(got) != 1 || got[0].Attempts != 1 {
		t.Fatalf("poll = (%+v, %v), want np-1 right away as attempt 1", got, err)
	}
}
