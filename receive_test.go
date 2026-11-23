package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"message-broker/broker"
	"message-broker/models"
	"message-broker/routing"
)

func sendJSON(messageID, to string) string {
	return `{"message_id":"` + messageID + `","timestamp":"2026-11-23T00:00:00Z","from":"agent-a","to":"` + to +
		`","type":"request","payload":{"q":"x"}}`
}

func serve(handler http.Handler, method, target, body string, header map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	for key, value := range header {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// TestReceiveEndpointPullFlow drives GET /receive and ACK over HTTP like a laptop agent.
func TestReceiveEndpointPullFlow(t *testing.T) {
	messageBroker, err := broker.New(func(context.Context, models.MessageEnvelope, string) error {
		return errors.New("no webhook")
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&apiServer{broker: messageBroker}).handler()

	empty := serve(handler, http.MethodGet, "/receive?agent_id=laptop", "", nil)
	if empty.Code != http.StatusOK || strings.TrimSpace(empty.Body.String()) != "[]" {
		t.Fatalf("empty poll = %d %q, want 200 []", empty.Code, empty.Body.String())
	}

	sent := serve(handler, http.MethodPost, "/send", sendJSON("pull-m1", "laptop"), map[string]string{"X-Trace-Id": "trace-laptop"})
	if sent.Code != http.StatusAccepted {
		t.Fatalf("/send = %d %s", sent.Code, sent.Body.String())
	}

	polled := serve(handler, http.MethodGet, "/receive?agent_id=laptop&limit=5&wait=1", "", nil)
	if polled.Code != http.StatusOK {
		t.Fatalf("poll = %d %s", polled.Code, polled.Body.String())
	}
	var messages []struct {
		models.MessageEnvelope
		TraceID string `json:"trace_id"`
	}
	if err := json.Unmarshal(polled.Body.Bytes(), &messages); err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].MessageID != "pull-m1" || messages[0].TraceID != "trace-laptop" ||
		messages[0].From != "agent-a" || messages[0].Payload["q"] != "x" {
		t.Fatalf("poll body = %s, want pull-m1 with its envelope and trace_id", polled.Body.String())
	}

	acked := serve(handler, http.MethodPost, "/messages/pull-m1/ack", `{"agent_id":"laptop","status":"success"}`, nil)
	if acked.Code != http.StatusOK || !strings.Contains(acked.Body.String(), string(broker.StatusDelivered)) {
		t.Fatalf("ACK = %d %s, want Delivered", acked.Code, acked.Body.String())
	}
}

// TestReceiveEndpointValidatesQuery rejects malformed polls.
func TestReceiveEndpointValidatesQuery(t *testing.T) {
	messageBroker, err := broker.New(func(context.Context, models.MessageEnvelope, string) error { return nil }, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&apiServer{broker: messageBroker}).handler()
	for _, target := range []string{
		"/receive",
		"/receive?agent_id=%20laptop",
		"/receive?agent_id=laptop&limit=0",
		"/receive?agent_id=laptop&limit=101",
		"/receive?agent_id=laptop&limit=x",
		"/receive?agent_id=laptop&wait=-1",
		"/receive?agent_id=laptop&wait=1.5",
	} {
		if got := serve(handler, http.MethodGet, target, "", nil); got.Code != http.StatusBadRequest {
			t.Errorf("GET %s = %d, want 400", target, got.Code)
		}
	}
	if got := serve(handler, http.MethodPost, "/receive?agent_id=laptop", "", nil); got.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /receive = %d, want 405", got.Code)
	}
}

// TestSendRejectsUnknownRecipients covers the 404 check and the cases that must still be accepted.
func TestSendRejectsUnknownRecipients(t *testing.T) {
	messageBroker, err := broker.New(func(context.Context, models.MessageEnvelope, string) error { return nil }, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	static, err := routing.NewStaticResolver(map[string]string{
		"webhook_agent": "http://agent:9000/webhook/receive",
		"static_pull":   routing.PullRoute,
	})
	if err != nil {
		t.Fatal(err)
	}
	registryDown := false
	resolver := routing.Chain{static, resolverFunc(func(context.Context, string) (string, error) {
		if registryDown {
			return "", errors.New("registry unavailable")
		}
		return "", routing.ErrAgentNotFound
	})}
	handler := (&apiServer{broker: messageBroker, checkRecipient: recipientChecker(resolver, messageBroker)}).handler()

	unknown := serve(handler, http.MethodPost, "/send", sendJSON("u1", "nobody"), nil)
	if unknown.Code != http.StatusNotFound || !strings.Contains(unknown.Body.String(), "nobody") {
		t.Fatalf("send to unknown agent = %d %s, want 404 naming it", unknown.Code, unknown.Body.String())
	}
	if _, known, _ := messageBroker.Get("u1"); known {
		t.Fatal("message to an unknown agent was stored")
	}

	for _, to := range []string{"webhook_agent", "static_pull"} {
		if got := serve(handler, http.MethodPost, "/send", sendJSON("ok-"+to, to), nil); got.Code != http.StatusAccepted {
			t.Fatalf("send to %s = %d %s, want 202", to, got.Code, got.Body.String())
		}
	}

	// An agent with no route becomes reachable once it polls GET /receive.
	serve(handler, http.MethodGet, "/receive?agent_id=laptop", "", nil)
	if got := serve(handler, http.MethodPost, "/send", sendJSON("ok-laptop", "laptop"), nil); got.Code != http.StatusAccepted {
		t.Fatalf("send to polling agent = %d %s, want 202", got.Code, got.Body.String())
	}

	// A registry outage must not block /send: the broker keeps the message and retries.
	registryDown = true
	if got := serve(handler, http.MethodPost, "/send", sendJSON("outage", "maybe_registered"), nil); got.Code != http.StatusAccepted {
		t.Fatalf("send during registry outage = %d %s, want 202", got.Code, got.Body.String())
	}
	registryDown = false

	// Resending an accepted message stays idempotent even if the recipient is now unknown.
	if got := serve(handler, http.MethodPost, "/send", sendJSON("outage", "maybe_registered"), nil); got.Code != http.StatusAccepted ||
		!strings.Contains(got.Body.String(), `"duplicate":true`) {
		t.Fatalf("resend of accepted message = %d %s, want 202 duplicate", got.Code, got.Body.String())
	}
}

type resolverFunc func(context.Context, string) (string, error)

func (f resolverFunc) Resolve(ctx context.Context, agentID string) (string, error) {
	return f(ctx, agentID)
}
