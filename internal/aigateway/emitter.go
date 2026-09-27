package aigateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
)

// Emitter publishes a completion event keyed by session so it shares a
// partition with the session's other events.
type Emitter interface {
	Emit(ctx context.Context, key string, envelope events.Envelope) error
}

// MessageWriter is the part of *kafkago.Writer the emitter uses.
type MessageWriter interface {
	WriteMessages(context.Context, ...kafkago.Message) error
}

// KafkaEmitter writes ai.response.completed synchronously to Kafka.
type KafkaEmitter struct {
	writer MessageWriter
}

// NewKafkaEmitter wraps writer, which must target a fixed topic and wait
// for acknowledgement from all in-sync replicas: a completion event must
// survive a broker failure, because it is the only record of the call.
func NewKafkaEmitter(writer *kafkago.Writer) (*KafkaEmitter, error) {
	if writer == nil {
		return nil, errors.New("aigateway: kafka writer is required")
	}
	if writer.RequiredAcks != kafkago.RequireAll {
		return nil, fmt.Errorf("aigateway: completion producer must use acks=all, got %d", writer.RequiredAcks)
	}
	if writer.Async {
		return nil, errors.New("aigateway: completion producer must be synchronous")
	}
	if writer.Topic == "" {
		return nil, errors.New("aigateway: completion producer needs a topic")
	}
	return &KafkaEmitter{writer: writer}, nil
}

func (e *KafkaEmitter) Emit(ctx context.Context, key string, envelope events.Envelope) error {
	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("aigateway: marshal envelope: %w", err)
	}
	err = e.writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(key),
		Value:   body,
		Headers: []kafkago.Header{{Key: "event_type", Value: []byte(envelope.EventType)}},
	})
	if err != nil {
		return fmt.Errorf("aigateway: write %s: %w", envelope.EventType, err)
	}
	return nil
}
