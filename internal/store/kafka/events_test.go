package kafka

import (
	"context"
	"encoding/json"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
)

type recordingMessageWriter struct{ message kafkago.Message }

func (w *recordingMessageWriter) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.message = messages[0]
	return nil
}

func TestEventPublisherWritesSessionStartedEnvelope(t *testing.T) {
	envelope, err := events.New("company-1", 1, events.SessionStarted{SessionID: "session-1"})
	if err != nil {
		t.Fatalf("events.New: %v", err)
	}
	writer := &recordingMessageWriter{}
	publisher := NewEventPublisher(writer)
	if err := publisher.Publish(context.Background(), envelope); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	publisher.Close()
	if string(writer.message.Key) != "session-1" {
		t.Errorf("message key = %q, want session-1", writer.message.Key)
	}
	if len(writer.message.Headers) != 1 || string(writer.message.Headers[0].Value) != "session.started" {
		t.Errorf("event_type header = %+v", writer.message.Headers)
	}
	var got events.Envelope
	if err := json.Unmarshal(writer.message.Value, &got); err != nil {
		t.Fatalf("decode Kafka value: %v", err)
	}
	if got.EventID != envelope.EventID || got.EventType != events.EventTypeSessionStarted {
		t.Errorf("Kafka envelope = %+v", got)
	}
}

func TestEventPublisherRejectsMissingSessionID(t *testing.T) {
	envelope, err := events.New("company-1", 1, events.SessionStarted{})
	if err != nil {
		t.Fatalf("events.New: %v", err)
	}
	publisher := NewEventPublisher(&recordingMessageWriter{})
	defer publisher.Close()
	if err := publisher.Publish(context.Background(), envelope); err == nil {
		t.Fatal("Publish should reject an empty session_id")
	}
}
