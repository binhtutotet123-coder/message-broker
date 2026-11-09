// Package fault injects network faults into webhook delivery so the broker's ACK and
// retry logic can be exercised on demand (roadmap week 4).
package fault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"message-broker/broker"
	"message-broker/models"
)

// MaxDelay bounds injected latency so a typo cannot stall the worker for minutes.
const MaxDelay = time.Minute

// Config controls which faults are injected into outgoing webhook deliveries.
// The zero value injects nothing.
type Config struct {
	// DropRate is the probability that a delivery is silently lost: the webhook is not
	// sent but the attempt looks successful, so only the missing ACK triggers a retry.
	DropRate float64 `json:"drop_rate"`
	// DelayMS delays every affected delivery. Delays longer than the broker's 5s attempt
	// timeout make the attempt fail, which also triggers a retry.
	DelayMS int `json:"delay_ms"`
	// DuplicateRate is the probability that a delivered webhook is sent a second time,
	// so receivers can prove they deduplicate by message_id.
	DuplicateRate float64 `json:"duplicate_rate"`
	// Agent limits faults to messages addressed to this agent ID. Empty means all agents,
	// which matters when the broker is shared by the whole class.
	Agent string `json:"agent,omitempty"`
}

// Validate rejects probabilities outside [0, 1] and negative or excessive delays.
func (c Config) Validate() error {
	for name, rate := range map[string]float64{"drop_rate": c.DropRate, "duplicate_rate": c.DuplicateRate} {
		if rate < 0 || rate > 1 {
			return fmt.Errorf("%s must be between 0 and 1", name)
		}
	}
	if c.DelayMS < 0 || time.Duration(c.DelayMS)*time.Millisecond > MaxDelay {
		return fmt.Errorf("delay_ms must be between 0 and %d", MaxDelay.Milliseconds())
	}
	return nil
}

// Stats counts what the injector has done since the process started.
type Stats struct {
	Passed     uint64 `json:"passed"`
	Dropped    uint64 `json:"dropped"`
	Delayed    uint64 `json:"delayed"`
	Duplicated uint64 `json:"duplicated"`
}

// Injector wraps a broker.DeliveryFunc and applies the current Config to each attempt.
type Injector struct {
	mu     sync.Mutex
	config Config
	random func() float64

	passed     atomic.Uint64
	dropped    atomic.Uint64
	delayed    atomic.Uint64
	duplicated atomic.Uint64
}

// New creates an injector with faults disabled. A nil random source uses math/rand/v2.
func New(random func() float64) *Injector {
	if random == nil {
		random = rand.Float64
	}
	return &Injector{random: random}
}

// Config returns the active fault configuration.
func (i *Injector) Config() Config {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.config
}

// SetConfig validates and activates a new fault configuration.
func (i *Injector) SetConfig(config Config) error {
	if err := config.Validate(); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.config = config
	return nil
}

// Stats returns a snapshot of the fault counters.
func (i *Injector) Stats() Stats {
	return Stats{
		Passed:     i.passed.Load(),
		Dropped:    i.dropped.Load(),
		Delayed:    i.delayed.Load(),
		Duplicated: i.duplicated.Load(),
	}
}

// roll reports whether an event with the given probability happens.
func (i *Injector) roll(probability float64) bool {
	switch {
	case probability <= 0:
		return false
	case probability >= 1:
		return true
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.random() < probability
}

// Wrap returns a DeliveryFunc that injects the configured faults before calling next.
func (i *Injector) Wrap(next broker.DeliveryFunc) broker.DeliveryFunc {
	return func(ctx context.Context, envelope models.MessageEnvelope, traceID string) error {
		config := i.Config()
		if config.Agent != "" && config.Agent != envelope.To {
			i.passed.Add(1)
			return next(ctx, envelope, traceID)
		}

		if i.roll(config.DropRate) {
			i.dropped.Add(1)
			log.Printf("[Fault][%s] DROP message %s to %s (webhook not sent)", traceID, envelope.MessageID, envelope.To)
			return nil
		}

		if config.DelayMS > 0 {
			delay := time.Duration(config.DelayMS) * time.Millisecond
			i.delayed.Add(1)
			log.Printf("[Fault][%s] DELAY message %s to %s by %s", traceID, envelope.MessageID, envelope.To, delay)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("fault injection: delay %s outlived the delivery attempt: %w", delay, ctx.Err())
			case <-timer.C:
			}
		}

		if err := next(ctx, envelope, traceID); err != nil {
			return err
		}
		i.passed.Add(1)

		if i.roll(config.DuplicateRate) {
			i.duplicated.Add(1)
			log.Printf("[Fault][%s] DUPLICATE message %s to %s", traceID, envelope.MessageID, envelope.To)
			if err := next(ctx, envelope, traceID); err != nil {
				// The original delivery succeeded, so a failed copy must not trigger a retry.
				log.Printf("[Fault][%s] duplicate copy of %s failed: %v", traceID, envelope.MessageID, err)
			}
		}
		return nil
	}
}

type statusResponse struct {
	Config Config `json:"config"`
	Stats  Stats  `json:"stats"`
}

const maxConfigBytes = 1 << 16

// Handler serves the admin API: GET shows config and stats, PUT or POST replaces the
// config, and DELETE turns every fault off.
func (i *Injector) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
		case http.MethodPut, http.MethodPost:
			config, err := decodeConfig(http.MaxBytesReader(w, r.Body, maxConfigBytes))
			if err == nil {
				err = i.SetConfig(config)
			}
			if err != nil {
				writeJSON(w, http.StatusBadRequest, models.ErrorModel{Code: http.StatusBadRequest, Message: err.Error()})
				return
			}
			log.Printf("[Fault] config updated: %+v", config)
		case http.MethodDelete:
			i.mu.Lock()
			i.config = Config{}
			i.mu.Unlock()
			log.Printf("[Fault] all faults disabled")
		default:
			w.Header().Set("Allow", "GET, PUT, POST, DELETE")
			writeJSON(w, http.StatusMethodNotAllowed, models.ErrorModel{Code: http.StatusMethodNotAllowed, Message: "Method not allowed"})
			return
		}
		writeJSON(w, http.StatusOK, statusResponse{Config: i.Config(), Stats: i.Stats()})
	})
}

// decodeConfig parses exactly one JSON object and rejects unknown fields to catch typos.
func decodeConfig(body io.Reader) (Config, error) {
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if err := decoder.Decode(&json.RawMessage{}); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("request body must contain exactly one JSON object")
	}
	return config, nil
}

// writeJSON encodes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("[Fault] encode response: %v", err)
	}
}
