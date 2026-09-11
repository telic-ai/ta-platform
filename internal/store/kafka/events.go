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

func (p *EventPublisher) Publish(ctx context.Context, envelope events.Envelope) error {
	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal event envelope: %w", err)
	}
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(envelope.CompanyID),
		Value:   body,
		Headers: []kafkago.Header{{Key: "event_type", Value: []byte(envelope.EventType)}},
	})
}
