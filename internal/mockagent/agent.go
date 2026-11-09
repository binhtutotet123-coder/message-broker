// Package mockagent implements a well-behaved test receiver: it deduplicates webhooks by
// message_id, ACKs every delivery to the broker, and can simulate lost or late ACKs.
package mockagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"message-broker/models"
	"message-broker/routing"
)

const maxWebhookBytes = 1 << 20

// Config configures one mock agent.
type Config struct {
	// ID is the agent_id this agent ACKs as; it must equal the envelope's `to` field.
	ID string
	// BrokerURL is the broker base URL, e.g. http://localhost:8080.
	BrokerURL string
	// AckDropRate is the probability of "forgetting" to send an ACK (simulates a lost ACK).
	AckDropRate float64
	// AckDelay postpones each ACK, e.g. to exceed the broker's retry interval.
	AckDelay time.Duration
	// Client sends ACKs; nil uses a client with a 5s timeout.
	Client *http.Client
	// Random returns values in [0, 1); nil uses math/rand/v2.
	Random func() float64
}

// Stats summarizes what the agent has seen. Processed counts unique messages only.
type Stats struct {
	Received    uint64 `json:"received"`
	Processed   uint64 `json:"processed"`
	Duplicates  uint64 `json:"duplicates"`
	AcksSent    uint64 `json:"acks_sent"`
	AcksDropped uint64 `json:"acks_dropped"`
	AcksFailed  uint64 `json:"acks_failed"`
}

// Agent is an HTTP webhook receiver for broker tests and demos.
type Agent struct {
	config Config

	mu       sync.Mutex
	seen     map[string]struct{}
	randomMu sync.Mutex

	received    atomic.Uint64
	processed   atomic.Uint64
	duplicates  atomic.Uint64
	acksSent    atomic.Uint64
	acksDropped atomic.Uint64
	acksFailed  atomic.Uint64
}

// New validates the configuration and creates an agent.
func New(config Config) (*Agent, error) {
	if strings.TrimSpace(config.ID) == "" {
		return nil, errors.New("agent ID is required")
	}
	if err := routing.ValidateWebhookURL(config.BrokerURL); err != nil {
		return nil, fmt.Errorf("broker URL: %w", err)
	}
	if config.AckDropRate < 0 || config.AckDropRate > 1 {
		return nil, errors.New("ACK drop rate must be between 0 and 1")
	}
	if config.AckDelay < 0 {
		return nil, errors.New("ACK delay must not be negative")
	}
	if config.Client == nil {
		config.Client = &http.Client{Timeout: 5 * time.Second}
	}
	if config.Random == nil {
		config.Random = rand.Float64
	}
	config.BrokerURL = strings.TrimRight(config.BrokerURL, "/")
	return &Agent{config: config, seen: make(map[string]struct{})}, nil
}

// Handler serves POST /webhook/receive, GET /stats and GET /healthz.
func (a *Agent) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook/receive", a.receive)
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a.Stats())
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	return mux
}

// Stats returns a snapshot of the agent counters.
func (a *Agent) Stats() Stats {
	return Stats{
		Received:    a.received.Load(),
		Processed:   a.processed.Load(),
		Duplicates:  a.duplicates.Load(),
		AcksSent:    a.acksSent.Load(),
		AcksDropped: a.acksDropped.Load(),
		AcksFailed:  a.acksFailed.Load(),
	}
}

// Seen reports whether the agent has processed messageID.
func (a *Agent) Seen(messageID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.seen[messageID]
	return ok
}

// receive processes each message_id once and ACKs every delivery, including duplicates,
// because the broker resends precisely when an earlier ACK was lost.
func (a *Agent) receive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var envelope models.MessageEnvelope
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxWebhookBytes)).Decode(&envelope); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if envelope.MessageID == "" {
		http.Error(w, "message_id is required", http.StatusBadRequest)
		return
	}

	a.received.Add(1)
	traceID := r.Header.Get("X-Trace-Id")
	if envelope.To != a.config.ID {
		log.Printf("⚠️  [Agent %s][%s] message %s is addressed to %q; the broker will reject this agent's ACK",
			a.config.ID, traceID, envelope.MessageID, envelope.To)
	}

	if a.markSeen(envelope.MessageID) {
		a.processed.Add(1)
		payload, _ := json.Marshal(envelope.Payload)
		log.Printf("🎯 [Agent %s][%s] processed message %s from %s: %s",
			a.config.ID, traceID, envelope.MessageID, envelope.From, payload)
	} else {
		a.duplicates.Add(1)
		log.Printf("♻️  [Agent %s][%s] duplicate message %s skipped (will ACK again)",
			a.config.ID, traceID, envelope.MessageID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"received"}`))
	go a.ack(envelope.MessageID, traceID)
}

// markSeen records messageID and reports whether this is its first delivery.
func (a *Agent) markSeen(messageID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.seen[messageID]; ok {
		return false
	}
	a.seen[messageID] = struct{}{}
	return true
}

// dropAck decides whether to simulate a lost ACK.
func (a *Agent) dropAck() bool {
	if a.config.AckDropRate <= 0 {
		return false
	}
	a.randomMu.Lock()
	defer a.randomMu.Unlock()
	return a.config.Random() < a.config.AckDropRate
}

// ack reports successful processing to the broker's ACK endpoint.
func (a *Agent) ack(messageID, traceID string) {
	if a.config.AckDelay > 0 {
		time.Sleep(a.config.AckDelay)
	}
	if a.dropAck() {
		a.acksDropped.Add(1)
		log.Printf("📭 [Agent %s][%s] ACK for %s dropped (simulated)", a.config.ID, traceID, messageID)
		return
	}
	if err := a.sendAck(messageID, traceID); err != nil {
		a.acksFailed.Add(1)
		log.Printf("❌ [Agent %s][%s] ACK for %s failed: %v", a.config.ID, traceID, messageID, err)
		return
	}
	a.acksSent.Add(1)
}

// sendAck POSTs {"agent_id": ..., "status": "success"} to /messages/{id}/ack.
func (a *Agent) sendAck(messageID, traceID string) error {
	body, err := json.Marshal(map[string]string{"agent_id": a.config.ID, "status": "success"})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ackURL := a.config.BrokerURL + "/messages/" + url.PathEscape(messageID) + "/ack"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ackURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if traceID != "" {
		req.Header.Set("X-Trace-Id", traceID)
	}
	resp, err := a.config.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("broker returned HTTP %s: %s", resp.Status, strings.TrimSpace(string(responseBody)))
	}
	return nil
}
