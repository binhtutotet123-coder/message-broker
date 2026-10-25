package main

import (
	"encoding/json"
	"testing"

	"message-broker/models"
)

// TestMessageEnvelopeJSON verifies that the representative envelope decodes correctly.
func TestMessageEnvelopeJSON(t *testing.T) {
	rawJSON := `{"message_id":"m123","timestamp":"2026-10-19T07:40:00Z","from":"agent_A","to":"agent_B","type":"request","payload":{"action":"test"}}`

	var message models.MessageEnvelope
	if err := json.Unmarshal([]byte(rawJSON), &message); err != nil {
		t.Fatalf("Unmarshal message: %v", err)
	}
	if message.MessageID != "m123" {
		t.Fatalf("MessageID = %q, want %q", message.MessageID, "m123")
	}
}
