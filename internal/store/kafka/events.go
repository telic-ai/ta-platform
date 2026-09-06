package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
)

type MessageWriter interface {
	WriteMessages(context.Context, ...kafkago.Message) error
}

// EventPublisher writes platform envelopes to a single-topic Kafka writer.
type EventPublisher struct{ writer MessageWriter }

func NewEventPublisher(writer MessageWriter) *EventPublisher {
	return &EventPublisher{writer: writer}
}

func (p *EventPublisher) Publish(ctx context.Context, sessionID string, envelope events.Envelope) error {
	if sessionID == "" {
		return fmt.Errorf("publish event envelope: session_id is empty")
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal event envelope: %w", err)
	}
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(sessionID),
		Value:   body,
		Headers: []kafkago.Header{{Key: "event_type", Value: []byte(envelope.EventType)}},
	})
}
