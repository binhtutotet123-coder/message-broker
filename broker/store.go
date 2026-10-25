package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"message-broker/models"
)

var (
	// ErrMessageIDConflict indicates an existing ID was reused for different content.
	ErrMessageIDConflict = errors.New("message_id already exists with different content")
	// ErrMessageNotFound indicates an ACK refers to an unknown message.
	ErrMessageNotFound = errors.New("message not found")
	// ErrAgentMismatch indicates the ACK sender is not the message recipient.
	ErrAgentMismatch = errors.New("agent_id does not match message recipient")
)

// MessageRecord is the storage representation of one accepted envelope.
type MessageRecord struct {
	Envelope      models.MessageEnvelope
	Encoded       []byte
	TraceID       string
	Status        Status
	Attempts      int
	NextAttemptAt time.Time
	LeaseUntil    time.Time
	LeaseToken    string
}

// MessageStore defines the persistence operations required by the broker.
// Implementations must be concurrency-safe. Persistent stores should claim due
// records atomically and use LeaseUntil to recover work after a process restart.
type MessageStore interface {
	// Accept inserts a new record or returns the existing record for an identical ID and payload.
	Accept(record MessageRecord) (stored MessageRecord, duplicate bool, err error)
	// Acknowledge validates the recipient and applies a success or failed ACK.
	Acknowledge(messageID, agentID, outcome string, retryAt time.Time) (MessageRecord, error)
	// Get retrieves a record by its idempotency key.
	Get(messageID string) (record MessageRecord, found bool, err error)
	// ClaimDue atomically claims one due record with the provided token and lease expiry.
	ClaimDue(now, leaseUntil time.Time, leaseToken string) (record MessageRecord, found bool, err error)
	// Release ends a delivery claim only if its token still owns the lease.
	Release(messageID, leaseToken string, retryAt time.Time) error
}

// MemoryStore is a process-local implementation of MessageStore.
type MemoryStore struct {
	mu       sync.Mutex
	messages map[string]MessageRecord
}

// NewMemoryStore creates an empty in-memory message store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{messages: make(map[string]MessageRecord)}
}

// Accept inserts a new message once or reports an identical prior submission.
func (s *MemoryStore) Accept(record MessageRecord) (MessageRecord, bool, error) {
	messageID := record.Envelope.MessageID
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.messages[messageID]; ok {
		if !bytes.Equal(existing.Encoded, record.Encoded) {
			copy, err := cloneRecord(existing)
			if err != nil {
				return MessageRecord{}, false, err
			}
			return copy, false, ErrMessageIDConflict
		}
		copy, err := cloneRecord(existing)
		return copy, true, err
	}

	stored, err := cloneRecord(record)
	if err != nil {
		return MessageRecord{}, false, err
	}
	s.messages[messageID] = stored
	copy, err := cloneRecord(stored)
	return copy, false, err
}

// Acknowledge validates the sender and updates the message's delivery status.
func (s *MemoryStore) Acknowledge(messageID, agentID, outcome string, retryAt time.Time) (MessageRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.messages[messageID]
	if !ok {
		return MessageRecord{}, ErrMessageNotFound
	}
	if agentID == "" || agentID != record.Envelope.To {
		copy, err := cloneRecord(record)
		if err != nil {
			return MessageRecord{}, err
		}
		return copy, ErrAgentMismatch
	}

	if outcome == AckSuccess {
		record.Status = StatusDelivered
	} else if record.Status == StatusPending {
		record.NextAttemptAt = retryAt
	}
	s.messages[messageID] = record
	return cloneRecord(record)
}

// Get returns a copy of a stored message.
func (s *MemoryStore) Get(messageID string) (MessageRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.messages[messageID]
	if !ok {
		return MessageRecord{}, false, nil
	}
	copy, err := cloneRecord(record)
	return copy, true, err
}

// ClaimDue atomically reserves the earliest pending message until its lease expires.
func (s *MemoryStore) ClaimDue(now, leaseUntil time.Time, leaseToken string) (MessageRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	messageIDs := make([]string, 0, len(s.messages))
	for messageID, record := range s.messages {
		if record.Status == StatusPending &&
			!record.LeaseUntil.After(now) &&
			!record.NextAttemptAt.After(now) {
			messageIDs = append(messageIDs, messageID)
		}
	}
	if len(messageIDs) == 0 {
		return MessageRecord{}, false, nil
	}
	sort.Slice(messageIDs, func(i, j int) bool {
		left := s.messages[messageIDs[i]]
		right := s.messages[messageIDs[j]]
		if left.NextAttemptAt.Equal(right.NextAttemptAt) {
			return messageIDs[i] < messageIDs[j]
		}
		return left.NextAttemptAt.Before(right.NextAttemptAt)
	})

	record := s.messages[messageIDs[0]]
	record.LeaseUntil = leaseUntil
	record.LeaseToken = leaseToken
	record.Attempts++
	s.messages[messageIDs[0]] = record
	copy, err := cloneRecord(record)
	return copy, true, err
}

// Release clears the lease only for its current owner and delays a pending retry.
func (s *MemoryStore) Release(messageID, leaseToken string, retryAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.messages[messageID]
	if !ok {
		return ErrMessageNotFound
	}
	if record.LeaseToken != leaseToken {
		return nil
	}
	record.LeaseUntil = time.Time{}
	record.LeaseToken = ""
	if record.Status == StatusPending {
		if record.NextAttemptAt.Before(retryAt) {
			record.NextAttemptAt = retryAt
		}
	}
	s.messages[messageID] = record
	return nil
}

// cloneRecord copies mutable fields so callers cannot mutate stored state.
func cloneRecord(record MessageRecord) (MessageRecord, error) {
	record.Encoded = append([]byte(nil), record.Encoded...)
	if err := json.Unmarshal(record.Encoded, &record.Envelope); err != nil {
		return MessageRecord{}, err
	}
	return record, nil
}
