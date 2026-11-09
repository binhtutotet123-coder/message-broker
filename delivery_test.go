package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"message-broker/models"
	"message-broker/routing"
)

// TestWebhookDeliveryRoutesByRecipient sends each message to the agent named in `to`.
func TestWebhookDeliveryRoutesByRecipient(t *testing.T) {
	type received struct {
		messageID string
		traceID   string
	}
	newAgent := func(inbox chan<- received) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var envelope models.MessageEnvelope
			if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
				t.Errorf("decode webhook: %v", err)
			}
			inbox <- received{messageID: envelope.MessageID, traceID: r.Header.Get(traceIDHeader)}
		}))
	}
	inboxB := make(chan received, 1)
	inboxC := make(chan received, 1)
	agentB := newAgent(inboxB)
	defer agentB.Close()
	agentC := newAgent(inboxC)
	defer agentC.Close()

	resolver, err := routing.NewStaticResolver(map[string]string{
		"agent_B": agentB.URL + "/webhook/receive",
		"agent_C": agentC.URL + "/webhook/receive",
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery := webhookDelivery{resolver: resolver, client: &http.Client{Timeout: time.Second}}

	envelope := models.MessageEnvelope{MessageID: "to-c", From: "agent_A", To: "agent_C", Type: "request", Payload: map[string]interface{}{}}
	if err := delivery.deliver(context.Background(), envelope, "trace-c"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-inboxC:
		if got.messageID != "to-c" || got.traceID != "trace-c" {
			t.Fatalf("agent_C received %+v", got)
		}
	default:
		t.Fatal("agent_C did not receive its message")
	}
	select {
	case got := <-inboxB:
		t.Fatalf("agent_B received a message addressed to agent_C: %+v", got)
	default:
	}
}

// TestWebhookDeliveryFailsForUnknownRecipient keeps unroutable messages pending.
func TestWebhookDeliveryFailsForUnknownRecipient(t *testing.T) {
	var hits atomic.Int32
	agentB := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer agentB.Close()
	resolver, err := routing.NewStaticResolver(map[string]string{"agent_B": agentB.URL})
	if err != nil {
		t.Fatal(err)
	}
	delivery := webhookDelivery{resolver: resolver, client: agentB.Client()}

	envelope := models.MessageEnvelope{MessageID: "lost", From: "agent_A", To: "khong_ton_tai", Type: "request", Payload: map[string]interface{}{}}
	if err := delivery.deliver(context.Background(), envelope, "trace"); !errors.Is(err, routing.ErrAgentNotFound) {
		t.Fatalf("error = %v, want ErrAgentNotFound", err)
	}
	if hits.Load() != 0 {
		t.Fatal("message for an unknown agent was sent to agent_B")
	}
}

// TestWebhookDeliveryReportsAgentErrors makes non-2xx responses retryable.
func TestWebhookDeliveryReportsAgentErrors(t *testing.T) {
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer agent.Close()
	resolver, err := routing.NewStaticResolver(map[string]string{"agent_B": agent.URL})
	if err != nil {
		t.Fatal(err)
	}
	delivery := webhookDelivery{resolver: resolver, client: agent.Client()}
	envelope := models.MessageEnvelope{MessageID: "m", From: "agent_A", To: "agent_B", Type: "request", Payload: map[string]interface{}{}}
	if err := delivery.deliver(context.Background(), envelope, "trace"); err == nil {
		t.Fatal("HTTP 503 from agent was treated as success")
	}
}

// TestHealthAndAdminRoutes checks /healthz and that /admin/fault exists only when enabled.
func TestHealthAndAdminRoutes(t *testing.T) {
	handler := (&apiServer{}).handler()
	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("/healthz status = %d", health.Code)
	}
	admin := httptest.NewRecorder()
	handler.ServeHTTP(admin, httptest.NewRequest(http.MethodGet, "/admin/fault", nil))
	if admin.Code != http.StatusNotFound {
		t.Fatalf("/admin/fault without FAULT_INJECTION status = %d, want 404", admin.Code)
	}
}
