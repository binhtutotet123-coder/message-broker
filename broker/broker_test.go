package broker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"message-broker/models"
)

// TestAcceptIsIdempotentAndDetectsConflicts covers duplicate submissions and ID reuse.
func TestAcceptIsIdempotentAndDetectsConflicts(t *testing.T) {
	messageBroker, err := New(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	envelope := testEnvelope("message-1")
	first, duplicate, err := messageBroker.Accept(envelope, "trace-1")
	if err != nil {
		t.Fatal(err)
	}
	if duplicate {
		t.Fatal("first acceptance was marked as duplicate")
	}
	if first.Status != StatusPending {
		t.Fatalf("status = %q, want %q", first.Status, StatusPending)
	}

	second, duplicate, err := messageBroker.Accept(envelope, "trace-2")
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate {
		t.Fatal("same message_id was not recognized as duplicate")
	}
	if second.TraceID != "trace-1" {
		t.Fatalf("duplicate trace ID = %q, want original trace ID", second.TraceID)
	}

	conflicting := envelope
	conflicting.Payload = map[string]interface{}{"action": "different"}
	if _, _, err := messageBroker.Accept(conflicting, "trace-3"); !errors.Is(err, ErrMessageIDConflict) {
		t.Fatalf("conflicting message error = %v, want ErrMessageIDConflict", err)
	}
}

// TestAcknowledgeValidatesRecipientAndTracksStatus covers ACK authorization and states.
func TestAcknowledgeValidatesRecipientAndTracksStatus(t *testing.T) {
	messageBroker, err := New(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	envelope := testEnvelope("message-2")
	if _, _, err := messageBroker.Accept(envelope, "trace"); err != nil {
		t.Fatal(err)
	}

	if _, err := messageBroker.Acknowledge(envelope.MessageID, "agent-other", AckSuccess); !errors.Is(err, ErrAgentMismatch) {
		t.Fatalf("wrong-recipient ACK error = %v, want ErrAgentMismatch", err)
	}
	failed, err := messageBroker.Acknowledge(envelope.MessageID, envelope.To, AckFailed)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != StatusPending {
		t.Fatalf("failed ACK status = %q, want %q", failed.Status, StatusPending)
	}

	delivered, err := messageBroker.Acknowledge(envelope.MessageID, envelope.To, AckSuccess)
	if err != nil {
		t.Fatal(err)
	}
	if delivered.Status != StatusDelivered {
		t.Fatalf("successful ACK status = %q, want %q", delivered.Status, StatusDelivered)
	}

	repeated, err := messageBroker.Acknowledge(envelope.MessageID, envelope.To, AckSuccess)
	if err != nil || repeated.Status != StatusDelivered {
		t.Fatalf("repeated successful ACK = (%+v, %v), want Delivered and no error", repeated, err)
	}
}

// TestWorkerRetriesPendingMessageUntilAcknowledged verifies retry stops after ACK.
func TestWorkerRetriesPendingMessageUntilAcknowledged(t *testing.T) {
	var attempts atomic.Int32
	delivered := make(chan struct{}, 4)
	releaseSecondAttempt := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseSecondAttempt) })
	}
	defer release()
	messageBroker, err := New(func(context.Context, models.MessageEnvelope, string) error {
		attempt := attempts.Add(1)
		delivered <- struct{}{}
		if attempt == 2 {
			<-releaseSecondAttempt
		}
		return nil
	}, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	envelope := testEnvelope("message-3")
	if _, _, err := messageBroker.Accept(envelope, "trace"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go messageBroker.RunWorker(ctx)

	waitForDelivery(t, delivered)
	waitForDelivery(t, delivered)
	if attempts.Load() < 2 {
		t.Fatalf("delivery attempts = %d, want at least 2", attempts.Load())
	}

	if _, err := messageBroker.Acknowledge(envelope.MessageID, envelope.To, AckSuccess); err != nil {
		t.Fatal(err)
	}
	release()
	time.Sleep(2 * workerPollInterval)
	if got := attempts.Load(); got != 2 {
		t.Fatalf("delivery attempts after ACK = %d, want 2", got)
	}
}

// TestWorkerRetriesSameMessageWhenAgentACKIsLost models a processed webhook whose ACK never arrives.
func TestWorkerRetriesSameMessageWhenAgentACKIsLost(t *testing.T) {
	const retryInterval = 30 * time.Millisecond
	var attempts atomic.Int32
	deliveredIDs := make(chan string, 4)
	secondAttemptStarted := make(chan struct{})
	releaseSecondAttempt := make(chan struct{})
	secondAttemptCompleted := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseSecondAttempt) })
	}
	defer release()

	messageBroker, err := New(func(_ context.Context, envelope models.MessageEnvelope, _ string) error {
		attempt := attempts.Add(1)
		deliveredIDs <- envelope.MessageID
		if attempt == 2 {
			close(secondAttemptStarted)
			<-releaseSecondAttempt
			close(secondAttemptCompleted)
		}
		return nil
	}, retryInterval)
	if err != nil {
		t.Fatal(err)
	}

	envelope := testEnvelope("processed-ack-lost")
	if _, _, err := messageBroker.Accept(envelope, "trace"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go messageBroker.RunWorker(ctx)

	firstID := receiveMessageID(t, deliveredIDs)
	waitForSignal(t, secondAttemptStarted)
	secondID := receiveMessageID(t, deliveredIDs)
	if firstID != envelope.MessageID || secondID != envelope.MessageID {
		t.Fatalf("delivery IDs = [%q, %q], want same message ID %q", firstID, secondID, envelope.MessageID)
	}

	snapshot, found, err := messageBroker.Get(envelope.MessageID)
	if err != nil || !found {
		t.Fatalf("message lookup = (found=%t, err=%v), want found", found, err)
	}
	if snapshot.Status != StatusPending {
		t.Fatalf("status after successful webhook deliveries without ACK = %q, want %q", snapshot.Status, StatusPending)
	}

	if _, err := messageBroker.Acknowledge(envelope.MessageID, envelope.To, AckSuccess); err != nil {
		t.Fatal(err)
	}
	release()
	waitForSignal(t, secondAttemptCompleted)
	time.Sleep(3 * retryInterval)

	snapshot, found, err = messageBroker.Get(envelope.MessageID)
	if err != nil || !found || snapshot.Status != StatusDelivered {
		t.Fatalf("final state = (%+v, found=%t, err=%v), want Delivered", snapshot, found, err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("delivery attempts after ACK = %d, want 2", got)
	}
}

// TestWorkerSchedulesRetryFromAttemptStart verifies a slow attempt does not add extra backoff.
func TestWorkerSchedulesRetryFromAttemptStart(t *testing.T) {
	const retryInterval = 40 * time.Millisecond
	var attempts atomic.Int32
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstStartedAt := make(chan time.Time, 1)
	firstCompleted := make(chan time.Time, 1)
	secondStarted := make(chan time.Time, 1)
	messageBroker, err := New(func(context.Context, models.MessageEnvelope, string) error {
		switch attempts.Add(1) {
		case 1:
			firstStartedAt <- time.Now()
			close(firstStarted)
			<-releaseFirst
			firstCompleted <- time.Now()
			return errors.New("temporary delivery failure")
		default:
			secondStarted <- time.Now()
			return errors.New("temporary delivery failure")
		}
	}, retryInterval)
	if err != nil {
		t.Fatal(err)
	}
	envelope := testEnvelope("slow-failure")
	if _, _, err := messageBroker.Accept(envelope, "trace"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go messageBroker.RunWorker(ctx)
	go messageBroker.RunWorker(ctx)

	waitForSignal(t, firstStarted)
	startedAt := receiveTime(t, firstStartedAt)
	time.Sleep(3 * retryInterval)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts while first delivery is in flight = %d, want 1", got)
	}

	close(releaseFirst)
	completedAt := receiveTime(t, firstCompleted)
	secondAt := receiveTime(t, secondStarted)
	if secondAt.Before(startedAt.Add(retryInterval)) {
		t.Fatalf("second attempt started at %s, before retry time %s", secondAt, startedAt.Add(retryInterval))
	}
	if !secondAt.Before(completedAt.Add(retryInterval)) {
		t.Fatalf("second attempt started at %s, retry was incorrectly delayed from completion %s", secondAt, completedAt)
	}
	cancel()
}

// TestAckSuccessDuringDeliveryPreventsRetry verifies a concurrent ACK wins over attempt completion.
func TestAckSuccessDuringDeliveryPreventsRetry(t *testing.T) {
	const retryInterval = 30 * time.Millisecond
	var attempts atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan struct{})
	messageBroker, err := New(func(context.Context, models.MessageEnvelope, string) error {
		attempts.Add(1)
		close(started)
		<-release
		close(completed)
		return nil
	}, retryInterval)
	if err != nil {
		t.Fatal(err)
	}
	envelope := testEnvelope("ack-success-in-flight")
	if _, _, err := messageBroker.Accept(envelope, "trace"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go messageBroker.RunWorker(ctx)
	waitForSignal(t, started)

	if _, err := messageBroker.Acknowledge(envelope.MessageID, envelope.To, AckSuccess); err != nil {
		t.Fatal(err)
	}
	close(release)
	waitForSignal(t, completed)
	time.Sleep(3 * retryInterval)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts after successful in-flight ACK = %d, want 1", got)
	}
}

// TestAckFailedDuringDeliveryKeepsRetryTime verifies a failed ACK's retry time is not overwritten.
func TestAckFailedDuringDeliveryKeepsRetryTime(t *testing.T) {
	const retryInterval = 40 * time.Millisecond
	var attempts atomic.Int32
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstCompleted := make(chan time.Time, 1)
	secondStarted := make(chan time.Time, 1)
	messageBroker, err := New(func(context.Context, models.MessageEnvelope, string) error {
		switch attempts.Add(1) {
		case 1:
			close(firstStarted)
			<-releaseFirst
			firstCompleted <- time.Now()
			return nil
		default:
			secondStarted <- time.Now()
			return nil
		}
	}, retryInterval)
	if err != nil {
		t.Fatal(err)
	}
	envelope := testEnvelope("ack-failed-in-flight")
	if _, _, err := messageBroker.Accept(envelope, "trace"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go messageBroker.RunWorker(ctx)
	waitForSignal(t, firstStarted)
	acknowledgedAt := time.Now()
	if _, err := messageBroker.Acknowledge(envelope.MessageID, envelope.To, AckFailed); err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * retryInterval)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts while delivery is in flight after failed ACK = %d, want 1", got)
	}
	close(releaseFirst)
	completedAt := receiveTime(t, firstCompleted)
	secondAt := receiveTime(t, secondStarted)
	if secondAt.Before(acknowledgedAt.Add(retryInterval)) {
		t.Fatalf("retry after failed ACK started at %s, before %s", secondAt, acknowledgedAt.Add(retryInterval))
	}
	if !secondAt.Before(completedAt.Add(retryInterval)) {
		t.Fatalf("retry after failed ACK started at %s, incorrectly delayed from completion %s", secondAt, completedAt)
	}
	cancel()
}

// TestAcceptRequiresMessageID rejects envelopes without an idempotency key.
func TestAcceptRequiresMessageID(t *testing.T) {
	messageBroker, err := New(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := messageBroker.Accept(testEnvelope(" "), "trace"); !errors.Is(err, ErrInvalidMessageID) {
		t.Fatalf("empty message_id error = %v, want ErrInvalidMessageID", err)
	}
}

// TestNewWithStoreUsesProvidedStore verifies the broker can be wired to another backend.
func TestNewWithStoreUsesProvidedStore(t *testing.T) {
	store := NewMemoryStore()
	messageBroker, err := NewWithStore(func(context.Context, models.MessageEnvelope, string) error {
		return nil
	}, time.Second, store)
	if err != nil {
		t.Fatal(err)
	}
	envelope := testEnvelope("stored-message")
	if _, _, err := messageBroker.Accept(envelope, "trace"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(envelope.MessageID); err != nil || !found {
		t.Fatalf("injected store lookup = (found=%t, err=%v), want found", found, err)
	}
}

// TestMemoryStoreReclaimsExpiredLease verifies a crashed worker's claim cannot block retries.
func TestMemoryStoreReclaimsExpiredLease(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	envelope := testEnvelope("leased-message")
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Accept(MessageRecord{
		Envelope:      envelope,
		Encoded:       encoded,
		Status:        StatusPending,
		NextAttemptAt: now.Add(-time.Second),
	}); err != nil {
		t.Fatal(err)
	}

	first, found, err := store.ClaimDue(now, now.Add(5*time.Second), "worker-a-token")
	if err != nil || !found || first.Attempts != 1 {
		t.Fatalf("first claim = (%+v, %t, %v), want attempt 1", first, found, err)
	}
	if _, found, err := store.ClaimDue(now.Add(time.Second), now.Add(6*time.Second), "worker-b-token"); err != nil || found {
		t.Fatalf("claim before lease expiry = (found=%t, err=%v), want no claim", found, err)
	}

	reclaimed, found, err := store.ClaimDue(now.Add(6*time.Second), now.Add(11*time.Second), "worker-b-token")
	if err != nil || !found || reclaimed.Attempts != 2 {
		t.Fatalf("claim after lease expiry = (%+v, %t, %v), want attempt 2", reclaimed, found, err)
	}
	if reclaimed.LeaseToken != "worker-b-token" {
		t.Fatalf("reclaimed lease token = %q, want worker-b-token", reclaimed.LeaseToken)
	}

	if err := store.Release(envelope.MessageID, first.LeaseToken, now.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	current, found, err := store.Get(envelope.MessageID)
	if err != nil || !found {
		t.Fatalf("message lookup after stale release = (found=%t, err=%v), want found", found, err)
	}
	if current.LeaseToken != "worker-b-token" || !current.LeaseUntil.Equal(now.Add(11*time.Second)) {
		t.Fatalf("stale release changed new lease: token=%q until=%s", current.LeaseToken, current.LeaseUntil)
	}
	if !current.NextAttemptAt.Equal(now.Add(-time.Second)) {
		t.Fatalf("claim changed retry schedule: got %s, want %s", current.NextAttemptAt, now.Add(-time.Second))
	}

	if err := store.Release(envelope.MessageID, reclaimed.LeaseToken, now.Add(12*time.Second)); err != nil {
		t.Fatal(err)
	}
	current, found, err = store.Get(envelope.MessageID)
	if err != nil || !found || current.LeaseToken != "" || !current.LeaseUntil.IsZero() {
		t.Fatalf("current owner release = (%+v, found=%t, err=%v), want lease cleared", current, found, err)
	}
	if !current.NextAttemptAt.Equal(now.Add(12 * time.Second)) {
		t.Fatalf("current release retry time = %s, want %s", current.NextAttemptAt, now.Add(12*time.Second))
	}
}

// testEnvelope creates a valid message shared by the broker unit tests.
func testEnvelope(messageID string) models.MessageEnvelope {
	return models.MessageEnvelope{
		MessageID: messageID,
		Timestamp: time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC),
		From:      "agent-a",
		To:        "agent-b",
		Type:      "request",
		Payload:   map[string]interface{}{"action": "test"},
	}
}

// waitForDelivery waits for a worker delivery attempt with a bounded timeout.
func waitForDelivery(t *testing.T, delivered <-chan struct{}) {
	t.Helper()
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delivery")
	}
}

// waitForSignal waits for a test event with a bounded timeout.
func waitForSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

// receiveTime waits for a timestamped worker event with a bounded timeout.
func receiveTime(t *testing.T, event <-chan time.Time) time.Time {
	t.Helper()
	select {
	case timestamp := <-event:
		return timestamp
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for timestamp")
		return time.Time{}
	}
}

// receiveMessageID waits for a recorded webhook delivery with a bounded timeout.
func receiveMessageID(t *testing.T, delivered <-chan string) string {
	t.Helper()
	select {
	case messageID := <-delivered:
		return messageID
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivered message ID")
		return ""
	}
}
