// Package outbox relays events written to the Postgres transactional outbox
// to Kafka. Writing the event in the same transaction as the state change it
// records means a committed change always produces its event, even when
// Kafka is unavailable at request time.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
)

// Message is one outbox row: a serialized envelope and where it goes.
type Message struct {
	ID        int64
	CompanyID uuid.UUID
	Topic     string
	Key       []byte
	EventType events.EventType
	Envelope  []byte
}

// NewMessage serializes envelope for topic, partitioned by key.
func NewMessage(topic, key string, envelope events.Envelope) (Message, error) {
	if topic == "" || key == "" {
		return Message{}, errors.New("outbox: topic and key are required")
	}
	companyID, err := uuid.Parse(envelope.CompanyID)
	if err != nil {
		return Message{}, fmt.Errorf("outbox: company_id: %w", err)
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return Message{}, fmt.Errorf("outbox: marshal envelope: %w", err)
	}
	return Message{
		CompanyID: companyID, Topic: topic, Key: []byte(key),
		EventType: envelope.EventType, Envelope: body,
	}, nil
}

// Source hands out pending messages in id order. Drain passes up to limit
// messages to publish and removes them only if publish succeeds; it returns
// how many it relayed. Only one Drain at a time may relay across all
// processes, which keeps per-key order.
type Source interface {
	Drain(ctx context.Context, limit int, publish func(context.Context, []Message) error) (int, error)
}

// MessageWriter is a Kafka writer without a fixed topic; each message names
// its own.
type MessageWriter interface {
	WriteMessages(context.Context, ...kafkago.Message) error
}

type Config struct {
	// BatchSize caps the messages relayed per Drain.
	BatchSize int
	// Interval is the wait between polls when the outbox is empty or a
	// relay attempt failed.
	Interval time.Duration
	// OnError is called for each failed relay attempt, which is retried.
	OnError func(error)
}

type Relay struct {
	source Source
	writer MessageWriter
	config Config
}

func NewRelay(source Source, writer MessageWriter, config Config) (*Relay, error) {
	if source == nil || writer == nil {
		return nil, errors.New("outbox: source and writer are required")
	}
	if config.BatchSize <= 0 || config.Interval <= 0 {
		return nil, errors.New("outbox: batch size and interval must be positive")
	}
	return &Relay{source: source, writer: writer, config: config}, nil
}

// Run relays until ctx is done. Delivery is at-least-once: a crash between
// the Kafka write and the outbox delete resends the batch, which consumers
// deduplicate by (company_id, interview_id, sequence_number).
func (r *Relay) Run(ctx context.Context) error {
	for {
		relayed, err := r.source.Drain(ctx, r.config.BatchSize, r.publish)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && r.config.OnError != nil {
			r.config.OnError(err)
		}
		if err == nil && relayed == r.config.BatchSize {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.config.Interval):
		}
	}
}

func (r *Relay) publish(ctx context.Context, messages []Message) error {
	records := make([]kafkago.Message, 0, len(messages))
	for _, message := range messages {
		records = append(records, kafkago.Message{
			Topic:   message.Topic,
			Key:     message.Key,
			Value:   message.Envelope,
			Headers: []kafkago.Header{{Key: "event_type", Value: []byte(message.EventType)}},
		})
	}
	if err := r.writer.WriteMessages(ctx, records...); err != nil {
		return fmt.Errorf("outbox: write %d messages: %w", len(records), err)
	}
	return nil
}
