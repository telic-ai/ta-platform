package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
)

// memorySource behaves like the Postgres outbox: messages leave only when
// publish succeeds.
type memorySource struct {
	mu       sync.Mutex
	messages []Message
}

func (s *memorySource) Drain(ctx context.Context, limit int, publish func(context.Context, []Message) error) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch := s.messages[:min(limit, len(s.messages))]
	if len(batch) == 0 {
		return 0, nil
	}
	if err := publish(ctx, batch); err != nil {
		return 0, err
	}
	s.messages = s.messages[len(batch):]
	return len(batch), nil
}

func (s *memorySource) pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.messages)
}

type recordingWriter struct {
	mu       sync.Mutex
	failures int
	written  []kafkago.Message
}

func (w *recordingWriter) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failures > 0 {
		w.failures--
		return errors.New("broker unavailable")
	}
	w.written = append(w.written, messages...)
	return nil
}

func message(t *testing.T, sessionID string, sequence int64) Message {
	t.Helper()
	envelope, err := events.New(uuid.NewString(), sequence, events.SessionStarted{SessionID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewMessage(events.SessionEventsTopic, sessionID, envelope)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRelayRetriesUntilDeliveredInOrder(t *testing.T) {
	source := &memorySource{}
	for sequence := range int64(5) {
		source.messages = append(source.messages, message(t, "session-1", sequence))
	}
	writer := &recordingWriter{failures: 2}
	var failures int
	relay, err := NewRelay(source, writer, Config{
		BatchSize: 2, Interval: time.Millisecond,
		OnError: func(error) { failures++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	for source.pending() > 0 && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}

	if failures != 2 {
		t.Errorf("reported failures = %d, want 2", failures)
	}
	if len(writer.written) != 5 {
		t.Fatalf("written = %d, want 5", len(writer.written))
	}
	for i, written := range writer.written {
		if written.Topic != events.SessionEventsTopic || string(written.Key) != "session-1" {
			t.Errorf("message %d routed to %q key %q", i, written.Topic, written.Key)
		}
		if len(written.Headers) != 1 || string(written.Headers[0].Value) != string(events.EventTypeSessionStarted) {
			t.Errorf("message %d headers = %+v", i, written.Headers)
		}
		var envelope events.Envelope
		if err := json.Unmarshal(written.Value, &envelope); err != nil {
			t.Fatalf("decode message %d: %v", i, err)
		}
		if envelope.SequenceNumber != int64(i) {
			t.Errorf("message %d has sequence %d; order was not kept", i, envelope.SequenceNumber)
		}
	}
}

func TestNewMessageValidates(t *testing.T) {
	envelope, err := events.New("not-a-uuid", 1, events.SessionStarted{SessionID: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewMessage(events.SessionEventsTopic, "s", envelope); err == nil {
		t.Error("NewMessage accepted a non-UUID company_id")
	}
	envelope.CompanyID = uuid.NewString()
	if _, err := NewMessage("", "s", envelope); err == nil {
		t.Error("NewMessage accepted an empty topic")
	}
}
