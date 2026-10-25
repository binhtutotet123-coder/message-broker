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
)

var (
	// ErrInvalidMessageID indicates the envelope has no usable message identifier.
	ErrInvalidMessageID = errors.New("message_id is required")
	// ErrInvalidAckStatus indicates the ACK outcome is unsupported.
	ErrInvalidAckStatus = errors.New("ACK status must be success or failed")
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

// Broker tracks message state and schedules webhook delivery attempts.
type Broker struct {
	store         MessageStore
	deliver       DeliveryFunc
	retryInterval time.Duration
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
	}, nil
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

		attemptCtx, cancel := context.WithTimeout(ctx, deliveryAttemptTimeout)
		attemptStartedAt := time.Now()
		err := b.deliver(attemptCtx, candidate.Envelope, candidate.TraceID)
		cancel()
		b.finishAttempt(candidate.Envelope.MessageID, candidate.LeaseToken, attemptStartedAt.Add(b.retryInterval))
		if err != nil {
			log.Printf("[Broker][%s] Delivery attempt failed for message %s: %v", candidate.TraceID, candidate.Envelope.MessageID, err)
		}
	}
}

// claimDue marks the earliest eligible pending message as in flight.
func (b *Broker) claimDue(now time.Time) (MessageRecord, bool) {
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		log.Printf("Generate delivery lease token: %v", err)
		return MessageRecord{}, false
	}
	leaseToken := hex.EncodeToString(tokenBytes)
	leaseUntil := now.Add(deliveryAttemptTimeout + deliveryLeaseGrace)
	record, found, err := b.store.ClaimDue(now, leaseUntil, leaseToken)
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
