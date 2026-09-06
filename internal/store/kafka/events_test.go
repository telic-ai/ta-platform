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
	if err := NewEventPublisher(writer).Publish(context.Background(), envelope); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if string(writer.message.Key) != envelope.CompanyID {
		t.Errorf("message key = %q", writer.message.Key)
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
