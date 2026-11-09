package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"message-broker/models"
	"message-broker/routing"
)

// webhookDelivery pushes envelopes to the webhook registered for the recipient in `to`.
type webhookDelivery struct {
	resolver routing.Resolver
	client   *http.Client
}

// deliver resolves the recipient's webhook and POSTs the envelope to it.
// It implements broker.DeliveryFunc; any error makes the broker retry later.
func (d webhookDelivery) deliver(ctx context.Context, envelope models.MessageEnvelope, traceID string) error {
	target, err := d.resolver.Resolve(ctx, envelope.To)
	if err != nil {
		return fmt.Errorf("resolve recipient %q: %w", envelope.To, err)
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request to agent: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(traceIDHeader, traceID)

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("send message to agent %q: %w", envelope.To, err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("read agent response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("agent %q returned HTTP %s", envelope.To, resp.Status)
	}

	log.Printf("[Broker][%s] Agent %s accepted message %s at %s via HTTP %s; waiting for ACK",
		traceID, envelope.To, envelope.MessageID, target, resp.Status)
	return nil
}
