package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"message-broker/models"
)

// Status describes whether the broker still expects an ACK for a message.
type Status string

const (
	// StatusPending means the message is awaiting a successful ACK.
	StatusPending Status = "Pending"
	// StatusDelivered means the recipient acknowledged successful processing.
	StatusDelivered Status = "Delivered"

	// AckSuccess is the ACK outcome that marks a message as delivered.
	AckSuccess = "success"
	// AckFailed is the ACK outcome that leaves the message eligible for retry.
	AckFailed = "failed"

	workerPollInterval     = 100 * time.Millisecond
	deliveryAttemptTimeout = 5 * time.Second
	deliveryLeaseGrace     = 2 * workerPollInterval

	// defaultPullWindow is how long after its last GET /receive an agent still counts
	// as polling: the worker does not push webhooks to it during that time.
	defaultPullWindow = 60 * time.Second
	// receivePollInterval is how often a waiting GET /receive checks for new messages.
	receivePollInterval = 100 * time.Millisecond
)

var (
	// ErrInvalidMessageID indicates the envelope has no usable message identifier.
	ErrInvalidMessageID = errors.New("message_id is required")
	// ErrInvalidAckStatus indicates the ACK outcome is unsupported.
	ErrInvalidAckStatus = errors.New("ACK status must be success or failed")
	// ErrPullOnly is returned (wrapped) by a DeliveryFunc when the recipient has no
	// webhook, for example it registered https://pull.invalid in the Service Registry.
	// The worker then leaves its messages for GET /receive instead of retrying.
	ErrPullOnly = errors.New("recipient has no webhook and fetches its messages with GET /receive")
)

// DeliveryFunc sends an envelope to its recipient and reports transport failures.
// Implementations must return when the provided context is canceled.
type DeliveryFunc func(context.Context, models.MessageEnvelope, string) error

// Snapshot is the public delivery state for an accepted message.
type Snapshot struct {
	MessageID string
	Status    Status
	TraceID   string
	Attempts  int
}

// Pulled is one message handed to a polling agent by Receive.
type Pulled struct {
	Envelope models.MessageEnvelope
	TraceID  string
	Attempts int
}

// Broker tracks message state and schedules webhook delivery attempts. Agents
// without a reachable webhook fetch their messages with Receive instead.
type Broker struct {
	store         MessageStore
	deliver       DeliveryFunc
	retryInterval time.Duration

	pullWindow time.Duration
	pullMu     sync.Mutex
	staticPull map[string]bool      // agents configured as pull-only
	lastPoll   map[string]time.Time // agent ID -> time of its latest Receive call
	noWebhook  map[string]time.Time // agent ID -> until when delivery reported ErrPullOnly
	announced  map[string]bool      // agents whose missing webhook was already logged
}

// New creates an in-memory broker with a delivery callback and retry interval.
func New(deliver DeliveryFunc, retryInterval time.Duration) (*Broker, error) {
	return NewWithStore(deliver, retryInterval, NewMemoryStore())
}

// NewWithStore creates a broker using the supplied message storage implementation.
func NewWithStore(deliver DeliveryFunc, retryInterval time.Duration, store MessageStore) (*Broker, error) {
	if deliver == nil {
		return nil, errors.New("delivery function is required")
	}
	if retryInterval <= 0 {
		return nil, errors.New("retry interval must be positive")
	}
	if store == nil {
		return nil, errors.New("message store is required")
	}

	return &Broker{
		store:         store,
		deliver:       deliver,
		retryInterval: retryInterval,
		pullWindow:    defaultPullWindow,
		staticPull:    make(map[string]bool),
		lastPoll:      make(map[string]time.Time),
		noWebhook:     make(map[string]time.Time),
		announced:     make(map[string]bool),
	}, nil
}

// MarkPullAgents records agents that never get webhooks and only use Receive.
func (b *Broker) MarkPullAgents(agentIDs ...string) {
	b.pullMu.Lock()
	defer b.pullMu.Unlock()
	for _, agentID := range agentIDs {
		b.staticPull[agentID] = true
	}
}

// IsPulling reports whether agentID fetches its own messages: it is configured as
// pull-only or called Receive within the pull window.
func (b *Broker) IsPulling(agentID string) bool {
	b.pullMu.Lock()
	defer b.pullMu.Unlock()
	return b.isPullingLocked(agentID, time.Now())
}

func (b *Broker) isPullingLocked(agentID string, now time.Time) bool {
	if b.staticPull[agentID] || now.Before(b.noWebhook[agentID]) {
		return true
	}
	last, ok := b.lastPoll[agentID]
	return ok && now.Sub(last) < b.pullWindow
}

// pullingSnapshot copies the set of agents that currently fetch their own messages,
// so the worker can skip them without holding pullMu inside the store.
func (b *Broker) pullingSnapshot(now time.Time) map[string]bool {
	b.pullMu.Lock()
	defer b.pullMu.Unlock()
	pulling := make(map[string]bool, len(b.staticPull)+len(b.lastPoll))
	for agentID := range b.staticPull {
		pulling[agentID] = true
	}
	for agentID, last := range b.lastPoll {
		if now.Sub(last) < b.pullWindow {
			pulling[agentID] = true
		}
	}
	for agentID, until := range b.noWebhook {
		if now.Before(until) {
			pulling[agentID] = true
		}
	}
	return pulling
}

// noteNoWebhook stops webhook attempts to agentID for a pull window, after delivery
// reported that it has no webhook, and logs this once per agent.
func (b *Broker) noteNoWebhook(agentID, traceID string, now time.Time) {
	b.pullMu.Lock()
	defer b.pullMu.Unlock()
	b.noWebhook[agentID] = now.Add(b.pullWindow)
	if !b.announced[agentID] {
		b.announced[agentID] = true
		log.Printf("[Broker][%s] Agent %s has no webhook (route \"pull\" or endpoint https://pull.invalid): its messages wait at the broker for GET /receive",
			traceID, agentID)
	}
}

// notePoll records a Receive call and logs when an agent starts polling.
func (b *Broker) notePoll(agentID string, now time.Time) {
	b.pullMu.Lock()
	defer b.pullMu.Unlock()
	if !b.isPullingLocked(agentID, now) {
		log.Printf("[Broker] Agent %s is polling GET /receive: its messages wait at the broker instead of being pushed to a webhook", agentID)
	}
	b.lastPoll[agentID] = now
}

// Receive hands up to limit due messages for agentID to the agent itself (GET /receive).
// With no message due it waits up to wait for one, returning early if ctx ends. Each
// handed message is due again after the retry interval unless the agent ACKs it, so a
// lost response or a crash after polling only delays the message.
func (b *Broker) Receive(ctx context.Context, agentID string, limit int, wait time.Duration) ([]Pulled, error) {
	if limit <= 0 {
		return nil, errors.New("limit must be positive")
	}
	deadline := time.Now().Add(wait)
	defer func() { b.notePoll(agentID, time.Now()) }()

	for {
		now := time.Now()
		b.notePoll(agentID, now)
		records, err := b.store.ClaimDueFor(agentID, now, now.Add(b.retryInterval), limit)
		if err != nil {
			return nil, err
		}
		if len(records) > 0 {
			pulled := make([]Pulled, 0, len(records))
			for _, record := range records {
				if record.Attempts > 1 {
					log.Printf("[Broker][%s] No ACK yet for message %s: handing it to %s again via GET /receive (attempt %d)",
						record.TraceID, record.Envelope.MessageID, agentID, record.Attempts)
				}
				log.Printf("[Broker][%s] Agent %s pulled message %s via GET /receive; waiting for ACK",
					record.TraceID, agentID, record.Envelope.MessageID)
				pulled = append(pulled, Pulled{Envelope: record.Envelope, TraceID: record.TraceID, Attempts: record.Attempts})
			}
			return pulled, nil
		}
		if !now.Before(deadline) {
			return []Pulled{}, nil
		}
		timer := time.NewTimer(receivePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return []Pulled{}, nil
		case <-timer.C:
		}
	}
}

// Accept records an envelope once and reports whether the ID was already accepted.
func (b *Broker) Accept(envelope models.MessageEnvelope, traceID string) (Snapshot, bool, error) {
	if strings.TrimSpace(envelope.MessageID) == "" {
		return Snapshot{}, false, ErrInvalidMessageID
	}

	encoded, err := json.Marshal(envelope)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("encode message: %w", err)
	}
	var storedEnvelope models.MessageEnvelope
	if err := json.Unmarshal(encoded, &storedEnvelope); err != nil {
		return Snapshot{}, false, fmt.Errorf("copy message: %w", err)
	}

	record, duplicate, err := b.store.Accept(MessageRecord{
		Envelope:      storedEnvelope,
		Encoded:       encoded,
		TraceID:       traceID,
		Status:        StatusPending,
		NextAttemptAt: time.Now(),
	})
	if err != nil {
		return Snapshot{}, false, err
	}
	return snapshot(record), duplicate, nil
}

// Acknowledge records the recipient's processing result for a pending message.
func (b *Broker) Acknowledge(messageID, agentID, outcome string) (Snapshot, error) {
	if outcome != AckSuccess && outcome != AckFailed {
		return Snapshot{}, ErrInvalidAckStatus
	}

	record, err := b.store.Acknowledge(messageID, agentID, outcome, time.Now().Add(b.retryInterval))
	if err != nil {
		return Snapshot{}, err
	}
	return snapshot(record), nil
}

// Get returns a snapshot, whether the ID is known, and any storage error.
func (b *Broker) Get(messageID string) (Snapshot, bool, error) {
	record, found, err := b.store.Get(messageID)
	if err != nil || !found {
		return Snapshot{}, false, err
	}
	return snapshot(record), true, nil
}

// RunWorker dispatches pending messages and retries them until successful ACK or cancellation.
func (b *Broker) RunWorker(ctx context.Context) {
	ticker := time.NewTicker(workerPollInterval)
	defer ticker.Stop()

	for {
		b.dispatchDue(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// dispatchDue claims and delivers every pending message whose retry time has arrived.
func (b *Broker) dispatchDue(ctx context.Context) {
	for ctx.Err() == nil {
		candidate, ok := b.claimDue(time.Now())
		if !ok {
			return
		}

		if candidate.Attempts > 1 {
			log.Printf("[Broker][%s] No ACK yet for message %s: resending to %s (attempt %d)",
				candidate.TraceID, candidate.Envelope.MessageID, candidate.Envelope.To, candidate.Attempts)
		}
		attemptCtx, cancel := context.WithTimeout(ctx, deliveryAttemptTimeout)
		attemptStartedAt := time.Now()
		err := b.deliver(attemptCtx, candidate.Envelope, candidate.TraceID)
		cancel()
		if errors.Is(err, ErrPullOnly) {
			// Nothing was sent: hand the message straight back for the agent to poll.
			b.noteNoWebhook(candidate.Envelope.To, candidate.TraceID, time.Now())
			if err := b.store.Unclaim(candidate.Envelope.MessageID, candidate.LeaseToken); err != nil {
				log.Printf("Unclaim message %s: %v", candidate.Envelope.MessageID, err)
			}
			continue
		}
		b.finishAttempt(candidate.Envelope.MessageID, candidate.LeaseToken, attemptStartedAt.Add(b.retryInterval))
		if err != nil {
			log.Printf("[Broker][%s] Delivery attempt failed for message %s: %v", candidate.TraceID, candidate.Envelope.MessageID, err)
		}
	}
}

// claimDue marks the earliest eligible pending message as in flight. Messages for
// agents that poll GET /receive are left for them.
func (b *Broker) claimDue(now time.Time) (MessageRecord, bool) {
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		log.Printf("Generate delivery lease token: %v", err)
		return MessageRecord{}, false
	}
	leaseToken := hex.EncodeToString(tokenBytes)
	leaseUntil := now.Add(deliveryAttemptTimeout + deliveryLeaseGrace)
	pulling := b.pullingSnapshot(now)
	record, found, err := b.store.ClaimDue(now, leaseUntil, leaseToken, func(recipient string) bool {
		return pulling[recipient]
	})
	if err != nil {
		log.Printf("Claim due message from store: %v", err)
		return MessageRecord{}, false
	}
	return record, found
}

// finishAttempt releases a delivery claim after the transport attempt completes.
func (b *Broker) finishAttempt(messageID, leaseToken string, retryAt time.Time) {
	if err := b.store.Release(messageID, leaseToken, retryAt); err != nil {
		log.Printf("Release delivery claim for message %s: %v", messageID, err)
	}
}

// snapshot copies the record's externally visible state.
func snapshot(record MessageRecord) Snapshot {
	return Snapshot{
		MessageID: record.Envelope.MessageID,
		Status:    record.Status,
		TraceID:   record.TraceID,
		Attempts:  record.Attempts,
	}
}
