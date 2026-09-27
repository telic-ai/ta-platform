package kafka

import (
	"context"
	"slices"
	"sync"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
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
	if writer.BatchTimeout != WriterBatchTimeout {
		t.Errorf("BatchTimeout = %v, want %v", writer.BatchTimeout, WriterBatchTimeout)
	}
}

// blockingWriter holds its first write until released, so later events queue
// up and must be delivered as a batch.
type blockingWriter struct {
	mu      sync.Mutex
	release chan struct{}
	calls   [][]string
}

func (w *blockingWriter) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.mu.Lock()
	first := len(w.calls) == 0
	keys := make([]string, 0, len(messages))
	for _, message := range messages {
		keys = append(keys, string(message.Value))
	}
	w.calls = append(w.calls, keys)
	w.mu.Unlock()
	if first {
		<-w.release
	}
	return nil
}

func TestEventPublisherBatchesQueuedEventsInOrder(t *testing.T) {
	writer := &blockingWriter{release: make(chan struct{})}
	var delivered []int64
	publisher := NewEventPublisherWithConfig(writer, 64, func(envelope events.Envelope, err error) {
		if err != nil {
			t.Errorf("delivery %d: %v", envelope.SequenceNumber, err)
		}
		delivered = append(delivered, envelope.SequenceNumber)
	})
	for sequence := range int64(10) {
		envelope, err := events.New("company-1", sequence, events.SessionStarted{SessionID: "session-1"})
		if err != nil {
			t.Fatal(err)
		}
		if err := publisher.Publish(context.Background(), envelope); err != nil {
			t.Fatal(err)
		}
	}
	close(writer.release)
	publisher.Close()

	want := make([]int64, 10)
	for i := range want {
		want[i] = int64(i)
	}
	if !slices.Equal(delivered, want) {
		t.Fatalf("delivery order = %v, want %v", delivered, want)
	}
	if len(writer.calls) >= 10 {
		t.Fatalf("writes = %d, want queued events batched into fewer writes", len(writer.calls))
	}
}
