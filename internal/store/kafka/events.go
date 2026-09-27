package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
)

type MessageWriter interface {
	WriteMessages(context.Context, ...kafkago.Message) error
}

const (
	DefaultPublisherBufferSize = 1_000
	// maxWriteBatch caps how many queued events one WriteMessages call carries.
	maxWriteBatch = 100
)

type queuedEvent struct {
	envelope events.Envelope
	message  kafkago.Message
}

// EventPublisher asynchronously writes session envelopes through one worker,
// preserving enqueue order. Its bounded queue applies backpressure to Publish.
type EventPublisher struct {
	writer     MessageWriter
	queue      chan queuedEvent
	done       chan struct{}
	onDelivery func(events.Envelope, error)
	closeOnce  sync.Once
}

func NewEventPublisher(writer MessageWriter) *EventPublisher {
	return NewEventPublisherWithConfig(writer, DefaultPublisherBufferSize, nil)
}

func NewEventPublisherWithConfig(writer MessageWriter, bufferSize int, onDelivery func(events.Envelope, error)) *EventPublisher {
	if bufferSize <= 0 {
		bufferSize = DefaultPublisherBufferSize
	}
	publisher := &EventPublisher{
		writer: writer, queue: make(chan queuedEvent, bufferSize), done: make(chan struct{}), onDelivery: onDelivery,
	}
	go publisher.run()
	return publisher
}

func (p *EventPublisher) Publish(ctx context.Context, envelope events.Envelope) error {
	var session struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(envelope.Payload, &session); err != nil {
		return fmt.Errorf("decode session event key: %w", err)
	}
	if session.SessionID == "" {
		return fmt.Errorf("publish session event: session_id is required")
	}

	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal event envelope: %w", err)
	}
	message := kafkago.Message{
		Key:     []byte(session.SessionID),
		Value:   body,
		Headers: []kafkago.Header{{Key: "event_type", Value: []byte(envelope.EventType)}},
	}
	select {
	case p.queue <- queuedEvent{envelope: envelope, message: message}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run writes everything already queued in one call, so throughput does not
// depend on one broker round trip per event. A single worker and in-order
// batches keep per-key ordering.
func (p *EventPublisher) run() {
	defer close(p.done)
	batch := make([]queuedEvent, 0, maxWriteBatch)
	messages := make([]kafkago.Message, 0, maxWriteBatch)
	for event := range p.queue {
		batch = append(batch[:0], event)
	drain:
		for len(batch) < maxWriteBatch {
			select {
			case next, ok := <-p.queue:
				if !ok {
					break drain
				}
				batch = append(batch, next)
			default:
				break drain
			}
		}
		messages = messages[:0]
		for _, queued := range batch {
			messages = append(messages, queued.message)
		}
		err := p.writer.WriteMessages(context.Background(), messages...)
		if p.onDelivery != nil {
			var perMessage kafkago.WriteErrors
			if !errors.As(err, &perMessage) || len(perMessage) != len(batch) {
				perMessage = nil
			}
			for i, queued := range batch {
				deliveryErr := err
				if perMessage != nil {
					deliveryErr = perMessage[i]
				}
				p.onDelivery(queued.envelope, deliveryErr)
			}
		}
	}
}

// Close drains the queue and waits for all delivery callbacks.
func (p *EventPublisher) Close() {
	p.closeOnce.Do(func() { close(p.queue) })
	<-p.done
}
