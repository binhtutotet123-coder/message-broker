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
)

type failingAcceptStore struct {
	broker.MessageStore
}

// Accept simulates a storage outage for API error handling tests.
func (failingAcceptStore) Accept(broker.MessageRecord) (broker.MessageRecord, bool, error) {
	return broker.MessageRecord{}, false, errors.New("storage unavailable")
}

// TestSendDeduplicatesAndAcknowledgesMessage covers the primary API reliability flow.
func TestSendDeduplicatesAndAcknowledgesMessage(t *testing.T) {
	messageBroker, err := broker.New(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&apiServer{broker: messageBroker}).handler()
	messageJSON := `{"message_id":"m1","timestamp":"2026-10-24T00:00:00Z","from":"agent-a","to":"agent-b","type":"request","payload":{"action":"test"}}`

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(messageJSON)))
	if first.Code != http.StatusAccepted {
		t.Fatalf("first /send status = %d, want %d: %s", first.Code, http.StatusAccepted, first.Body.String())
	}

	var accepted struct {
		Duplicate bool   `json:"duplicate"`
		Status    string `json:"delivery_status"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Duplicate || accepted.Status != string(broker.StatusPending) {
		t.Fatalf("first /send response = %+v, want non-duplicate Pending", accepted)
	}

	duplicate := httptest.NewRecorder()
	handler.ServeHTTP(duplicate, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(messageJSON)))
	if duplicate.Code != http.StatusAccepted {
		t.Fatalf("duplicate /send status = %d, want %d", duplicate.Code, http.StatusAccepted)
	}
	if err := json.Unmarshal(duplicate.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if !accepted.Duplicate {
		t.Fatal("repeated /send was not marked duplicate")
	}

	ackBody := `{"agent_id":"agent-b","status":"success"}`
	ack := httptest.NewRecorder()
	handler.ServeHTTP(ack, httptest.NewRequest(http.MethodPost, "/messages/m1/ack", strings.NewReader(ackBody)))
	if ack.Code != http.StatusOK {
		t.Fatalf("path ACK status = %d, want %d: %s", ack.Code, http.StatusOK, ack.Body.String())
	}

	ackBody = `{"message_id":"m1","agent_id":"agent-b","status":"success"}`
	aliasAck := httptest.NewRecorder()
	handler.ServeHTTP(aliasAck, httptest.NewRequest(http.MethodPost, "/ack", strings.NewReader(ackBody)))
	if aliasAck.Code != http.StatusOK {
		t.Fatalf("/ack alias status = %d, want %d: %s", aliasAck.Code, http.StatusOK, aliasAck.Body.String())
	}

	snapshot, ok, err := messageBroker.Get("m1")
	if err != nil || !ok || snapshot.Status != broker.StatusDelivered {
		t.Fatalf("final snapshot = (%+v, %t, %v), want Delivered", snapshot, ok, err)
	}

	afterDelivery := httptest.NewRecorder()
	handler.ServeHTTP(afterDelivery, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(messageJSON)))
	if afterDelivery.Code != http.StatusAccepted {
		t.Fatalf("duplicate after delivery status = %d, want %d", afterDelivery.Code, http.StatusAccepted)
	}
	if err := json.Unmarshal(afterDelivery.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if !accepted.Duplicate || accepted.Status != string(broker.StatusDelivered) {
		t.Fatalf("duplicate after delivery response = %+v, want duplicate Delivered", accepted)
	}
}

// TestSendRejectsMessageIDReuseWithDifferentContent verifies conflicting retries fail.
func TestSendRejectsMessageIDReuseWithDifferentContent(t *testing.T) {
	messageBroker, err := broker.New(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&apiServer{broker: messageBroker}).handler()

	first := `{"message_id":"m1","timestamp":"2026-10-24T00:00:00Z","from":"agent-a","to":"agent-b","type":"request","payload":{"action":"first"}}`
	second := `{"message_id":"m1","timestamp":"2026-10-24T00:00:00Z","from":"agent-a","to":"agent-b","type":"request","payload":{"action":"second"}}`
	for _, body := range []string{first, second} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(body)))
		if body == first && response.Code != http.StatusAccepted {
			t.Fatalf("initial /send status = %d, want %d", response.Code, http.StatusAccepted)
		}
		if body == second && response.Code != http.StatusConflict {
			t.Fatalf("conflicting /send status = %d, want %d", response.Code, http.StatusConflict)
		}
	}
}

// TestAckRejectsWrongAgentAndInvalidStatus checks invalid and unauthorized ACK requests.
func TestAckRejectsWrongAgentAndInvalidStatus(t *testing.T) {
	messageBroker, err := broker.New(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := messageBroker.Accept(models.MessageEnvelope{
		MessageID: "m1",
		Timestamp: time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC),
		From:      "agent-a",
		To:        "agent-b",
		Type:      "request",
		Payload:   map[string]interface{}{"action": "test"},
	}, "trace"); err != nil {
		t.Fatal(err)
	}
	handler := (&apiServer{broker: messageBroker}).handler()

	wrongAgent := httptest.NewRecorder()
	handler.ServeHTTP(wrongAgent, httptest.NewRequest(http.MethodPost, "/ack",
		strings.NewReader(`{"message_id":"m1","agent_id":"agent-a","status":"success"}`)))
	if wrongAgent.Code != http.StatusForbidden {
		t.Fatalf("wrong-agent ACK status = %d, want %d", wrongAgent.Code, http.StatusForbidden)
	}

	invalidStatus := httptest.NewRecorder()
	handler.ServeHTTP(invalidStatus, httptest.NewRequest(http.MethodPost, "/ack",
		strings.NewReader(`{"message_id":"m1","agent_id":"agent-b","status":"unknown"}`)))
	if invalidStatus.Code != http.StatusBadRequest {
		t.Fatalf("invalid-status ACK status = %d, want %d", invalidStatus.Code, http.StatusBadRequest)
	}

	unknownMessage := httptest.NewRecorder()
	handler.ServeHTTP(unknownMessage, httptest.NewRequest(http.MethodPost, "/ack",
		strings.NewReader(`{"message_id":"unknown","agent_id":"agent-b","status":"success"}`)))
	if unknownMessage.Code != http.StatusNotFound {
		t.Fatalf("unknown-message ACK status = %d, want %d", unknownMessage.Code, http.StatusNotFound)
	}
}

// TestSendValidatesEnvelopeRequiredFields rejects malformed message envelopes.
func TestSendValidatesEnvelopeRequiredFields(t *testing.T) {
	messageBroker, err := broker.New(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&apiServer{broker: messageBroker}).handler()
	cases := []struct {
		name string
		body string
	}{
		{
			name: "missing recipient",
			body: `{"message_id":"m1","timestamp":"2026-10-24T00:00:00Z","from":"agent-a","type":"request","payload":{}}`,
		},
		{
			name: "invalid type",
			body: `{"message_id":"m2","timestamp":"2026-10-24T00:00:00Z","from":"agent-a","to":"agent-b","type":"unknown","payload":{}}`,
		},
		{
			name: "missing payload",
			body: `{"message_id":"m3","timestamp":"2026-10-24T00:00:00Z","from":"agent-a","to":"agent-b","type":"request"}`,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(testCase.body)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("/send status = %d, want %d: %s", response.Code, http.StatusBadRequest, response.Body.String())
			}
		})
	}
}

// TestSendRejectsIdentifiersWithSurroundingWhitespace keeps stored identity values exact.
func TestSendRejectsIdentifiersWithSurroundingWhitespace(t *testing.T) {
	messageBroker, err := broker.New(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&apiServer{broker: messageBroker}).handler()
	cases := []struct {
		name string
		body string
	}{
		{
			name: "message_id",
			body: `{"message_id":" m1","timestamp":"2026-10-24T00:00:00Z","from":"agent-a","to":"agent-b","type":"request","payload":{}}`,
		},
		{
			name: "from",
			body: `{"message_id":"m2","timestamp":"2026-10-24T00:00:00Z","from":"agent-a ","to":"agent-b","type":"request","payload":{}}`,
		},
		{
			name: "to",
			body: `{"message_id":"m3","timestamp":"2026-10-24T00:00:00Z","from":"agent-a","to":" agent-b ","type":"request","payload":{}}`,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(testCase.body)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("/send status = %d, want %d: %s", response.Code, http.StatusBadRequest, response.Body.String())
			}
		})
	}
}

// TestSendRejectsTrailingJSONAndGarbage rejects request bodies with extra content.
func TestSendRejectsTrailingJSONAndGarbage(t *testing.T) {
	messageBroker, err := broker.New(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&apiServer{broker: messageBroker}).handler()
	validBody := `{"message_id":"m1","timestamp":"2026-10-24T00:00:00Z","from":"agent-a","to":"agent-b","type":"request","payload":{}}`
	cases := []struct {
		name string
		body string
	}{
		{name: "second JSON value", body: validBody + ` {"extra":true}`},
		{name: "trailing garbage", body: validBody + ` trailing`},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(testCase.body)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("/send status = %d, want %d: %s", response.Code, http.StatusBadRequest, response.Body.String())
			}
			if _, found, err := messageBroker.Get("m1"); err != nil || found {
				t.Fatalf("message lookup = (found=%t, err=%v), want message not accepted", found, err)
			}
		})
	}
}

// TestAckRejectsTrailingJSONAndGarbage ensures malformed ACK bodies cannot change message state.
func TestAckRejectsTrailingJSONAndGarbage(t *testing.T) {
	messageBroker, err := broker.New(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	envelope := models.MessageEnvelope{
		MessageID: "m1",
		Timestamp: time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC),
		From:      "agent-a",
		To:        "agent-b",
		Type:      "request",
		Payload:   map[string]interface{}{"action": "test"},
	}
	if _, _, err := messageBroker.Accept(envelope, "trace"); err != nil {
		t.Fatal(err)
	}
	handler := (&apiServer{broker: messageBroker}).handler()
	validBody := `{"agent_id":"agent-b","status":"success"}`
	cases := []struct {
		name string
		body string
	}{
		{name: "second JSON value", body: validBody + ` {"extra":true}`},
		{name: "trailing garbage", body: validBody + ` trailing`},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/messages/m1/ack", strings.NewReader(testCase.body)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("ACK status = %d, want %d: %s", response.Code, http.StatusBadRequest, response.Body.String())
			}
			snapshot, found, err := messageBroker.Get("m1")
			if err != nil || !found || snapshot.Status != broker.StatusPending {
				t.Fatalf("message state = (%+v, found=%t, err=%v), want Pending", snapshot, found, err)
			}
		})
	}
}

// TestAckRejectsIdentifiersWithSurroundingWhitespace prevents recipient mismatches.
func TestAckRejectsIdentifiersWithSurroundingWhitespace(t *testing.T) {
	messageBroker, err := broker.New(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	envelope := models.MessageEnvelope{
		MessageID: "m1",
		Timestamp: time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC),
		From:      "agent-a",
		To:        "agent-b",
		Type:      "request",
		Payload:   map[string]interface{}{"action": "test"},
	}
	if _, _, err := messageBroker.Accept(envelope, "trace"); err != nil {
		t.Fatal(err)
	}
	handler := (&apiServer{broker: messageBroker}).handler()
	cases := []struct {
		name string
		path string
		body string
	}{
		{
			name: "body message_id",
			path: "/ack",
			body: `{"message_id":"m1 ","agent_id":"agent-b","status":"success"}`,
		},
		{
			name: "path message_id",
			path: "/messages/%20m1/ack",
			body: `{"agent_id":"agent-b","status":"success"}`,
		},
		{
			name: "agent_id",
			path: "/messages/m1/ack",
			body: `{"agent_id":"agent-b ","status":"success"}`,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, testCase.path, strings.NewReader(testCase.body)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("ACK status = %d, want %d: %s", response.Code, http.StatusBadRequest, response.Body.String())
			}
			snapshot, found, err := messageBroker.Get("m1")
			if err != nil || !found || snapshot.Status != broker.StatusPending {
				t.Fatalf("message state = (%+v, found=%t, err=%v), want Pending", snapshot, found, err)
			}
		})
	}
}

// TestSendReturnsServerErrorWhenStorageFails prevents backend outages being reported as bad input.
func TestSendReturnsServerErrorWhenStorageFails(t *testing.T) {
	messageBroker, err := broker.NewWithStore(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second, failingAcceptStore{})
	if err != nil {
		t.Fatal(err)
	}
	handler := (&apiServer{broker: messageBroker}).handler()
	body := `{"message_id":"m1","timestamp":"2026-10-24T00:00:00Z","from":"agent-a","to":"agent-b","type":"request","payload":{}}`

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(body)))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("/send status = %d, want %d: %s", response.Code, http.StatusInternalServerError, response.Body.String())
	}
}
