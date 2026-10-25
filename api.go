package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"message-broker/broker"
	"message-broker/models"
)

type apiServer struct {
	broker *broker.Broker
}

type ackRequest struct {
	MessageID string `json:"message_id,omitempty"`
	AgentID   string `json:"agent_id"`
	Status    string `json:"status"`
}

// handler registers the message ingestion and ACK endpoints.
func (s *apiServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/send", s.sendHandler)
	mux.HandleFunc("/ack", s.ackHandler)
	mux.HandleFunc("/messages/", s.messageAckHandler)
	return mux
}

// sendHandler validates and accepts a new envelope for asynchronous delivery.
func (s *apiServer) sendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var envelope models.MessageEnvelope
	if err := decodeSingleJSON(r.Body, &envelope); err != nil {
		writeError(w, http.StatusBadRequest, 400, "Invalid JSON: "+err.Error())
		return
	}
	if message := validateEnvelope(envelope); message != "" {
		writeError(w, http.StatusBadRequest, 400, message)
		return
	}

	traceID := r.Header.Get(traceIDHeader)
	if traceID == "" {
		traceID = defaultTraceID
	}

	snapshot, duplicate, err := s.broker.Accept(envelope, traceID)
	switch {
	case errors.Is(err, broker.ErrInvalidMessageID):
		writeError(w, http.StatusBadRequest, 400, err.Error())
		return
	case errors.Is(err, broker.ErrMessageIDConflict):
		writeError(w, http.StatusConflict, 409, err.Error())
		return
	case err != nil:
		log.Printf("Accept message %s: %v", envelope.MessageID, err)
		writeError(w, http.StatusInternalServerError, 500, "Could not store message")
		return
	}

	log.Printf("[Broker][%s] Accepted message %s for %s (duplicate=%t)", snapshot.TraceID, snapshot.MessageID, envelope.To, duplicate)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          "Accepted",
		"trace_id":        snapshot.TraceID,
		"message_id":      snapshot.MessageID,
		"delivery_status": snapshot.Status,
		"duplicate":       duplicate,
		"message":         "Broker đã tiếp nhận và đang định tuyến",
	}); err != nil {
		log.Printf("Encode /send response: %v", err)
	}
}

// ackHandler accepts ACK requests with message_id in the JSON body.
func (s *apiServer) ackHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.handleAck(w, r, "")
}

// messageAckHandler accepts ACK requests using the documented message-specific route.
func (s *apiServer) messageAckHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	const prefix = "/messages/"
	const suffix = "/ack"
	path := r.URL.Path
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		http.NotFound(w, r)
		return
	}
	messageID := strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
	if messageID == "" || strings.Contains(messageID, "/") {
		http.NotFound(w, r)
		return
	}
	s.handleAck(w, r, messageID)
}

// handleAck validates an ACK and updates the corresponding delivery status.
func (s *apiServer) handleAck(w http.ResponseWriter, r *http.Request, pathMessageID string) {
	var request ackRequest
	if err := decodeSingleJSON(r.Body, &request); err != nil {
		writeError(w, http.StatusBadRequest, 400, "Invalid JSON: "+err.Error())
		return
	}

	messageID := request.MessageID
	if pathMessageID != "" {
		if messageID != "" && messageID != pathMessageID {
			writeError(w, http.StatusBadRequest, 400, "message_id in path and body must match")
			return
		}
		messageID = pathMessageID
	}
	if message := validateIdentifier("message_id", messageID); message != "" {
		writeError(w, http.StatusBadRequest, 400, message)
		return
	}
	if message := validateIdentifier("agent_id", request.AgentID); message != "" {
		writeError(w, http.StatusBadRequest, 400, message)
		return
	}
	if request.Status != broker.AckSuccess && request.Status != broker.AckFailed {
		writeError(w, http.StatusBadRequest, 400, broker.ErrInvalidAckStatus.Error())
		return
	}

	snapshot, err := s.broker.Acknowledge(messageID, request.AgentID, request.Status)
	switch {
	case errors.Is(err, broker.ErrMessageNotFound):
		writeError(w, http.StatusNotFound, 404, err.Error())
		return
	case errors.Is(err, broker.ErrAgentMismatch):
		writeError(w, http.StatusForbidden, 403, err.Error())
		return
	case err != nil:
		log.Printf("Acknowledge message %s: %v", messageID, err)
		writeError(w, http.StatusInternalServerError, 500, "Could not update message")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"message_id": snapshot.MessageID,
		"status":     snapshot.Status,
		"attempts":   snapshot.Attempts,
	}); err != nil {
		log.Printf("Encode ACK response: %v", err)
	}
}

// writeError encodes an API error using the shared ErrorModel response shape.
func writeError(w http.ResponseWriter, status, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(models.ErrorModel{Code: code, Message: message}); err != nil {
		log.Printf("Encode error response: %v", err)
	}
}

// validateEnvelope checks the required fields defined by the message envelope contract.
func validateEnvelope(envelope models.MessageEnvelope) string {
	for _, identifier := range []struct {
		field string
		value string
	}{
		{field: "message_id", value: envelope.MessageID},
		{field: "from", value: envelope.From},
		{field: "to", value: envelope.To},
	} {
		if message := validateIdentifier(identifier.field, identifier.value); message != "" {
			return message
		}
	}

	switch {
	case envelope.Timestamp.IsZero():
		return "timestamp is required"
	case envelope.Type != "request" && envelope.Type != "response" && envelope.Type != "final":
		return "type must be request, response, or final"
	case envelope.Payload == nil:
		return "payload is required"
	default:
		return ""
	}
}

// validateIdentifier requires a non-empty identifier without surrounding whitespace.
func validateIdentifier(field, value string) string {
	if strings.TrimSpace(value) == "" {
		return field + " is required"
	}
	if strings.TrimSpace(value) != value {
		return field + " must not have leading or trailing whitespace"
	}
	return ""
}

// decodeSingleJSON decodes one JSON value and rejects any non-whitespace trailing content.
func decodeSingleJSON(reader io.Reader, target interface{}) error {
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(target); err != nil {
		return err
	}

	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain exactly one JSON value")
		}
		return err
	}
	return nil
}
