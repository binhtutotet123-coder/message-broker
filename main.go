package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"message-broker/broker"
	"message-broker/models"
)

const (
	agentBURL      = "http://localhost:8081/webhook/receive"
	retryInterval  = 5 * time.Second
	traceIDHeader  = "X-Trace-Id"
	defaultTraceID = "default-trace-xyz"
)

var agentHTTPClient = &http.Client{Timeout: 5 * time.Second}

// main constructs the broker, starts its delivery worker, and serves the HTTP API.
func main() {
	messageBroker, err := broker.New(deliverMessage, retryInterval)
	if err != nil {
		log.Fatal(err)
	}

	go messageBroker.RunWorker(context.Background())

	server := &apiServer{broker: messageBroker}
	fmt.Println("Broker đang khởi động tại cổng :8080...")
	log.Fatal(http.ListenAndServe(":8080", server.handler()))
}

// deliverMessage sends one envelope to the configured receiver webhook.
func deliverMessage(ctx context.Context, envelope models.MessageEnvelope, traceID string) error {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, agentBURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request to agent: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(traceIDHeader, traceID)

	resp, err := agentHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("send message to agent: %w", err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("read agent response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("agent returned HTTP %s", resp.Status)
	}

	log.Printf("[Broker][%s] Agent accepted message %s via HTTP %s; waiting for ACK", traceID, envelope.MessageID, resp.Status)
	return nil
}
