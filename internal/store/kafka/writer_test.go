package kafka

import (
	"context"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

type noOpMessageWriter struct{}

func (noOpMessageWriter) WriteMessages(context.Context, ...kafkago.Message) error { return nil }

func TestEventPublisherBufferIsBounded(t *testing.T) {
	publisher := NewEventPublisherWithConfig(noOpMessageWriter{}, 17, nil)
	defer publisher.Close()

	if capacity := cap(publisher.queue); capacity != 17 {
		t.Errorf("buffer capacity = %d, want 17", capacity)
	}
}

func TestWriterIsDurableAndKeyPartitioned(t *testing.T) {
	writer := New("broker:9092").Writer("session-events")
	defer writer.Close()

	if _, ok := writer.Balancer.(*kafkago.Hash); !ok {
		t.Errorf("Balancer = %T, want *kafka.Hash", writer.Balancer)
	}
	if writer.RequiredAcks != kafkago.RequireAll {
		t.Errorf("RequiredAcks = %d, want RequireAll", writer.RequiredAcks)
	}
}
