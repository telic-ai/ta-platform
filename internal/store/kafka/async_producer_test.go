package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

type blockingWriteCloser struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once

	mu       sync.Mutex
	messages []kafkago.Message
	closed   bool
}

func (w *blockingWriteCloser) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.once.Do(func() { close(w.started) })
	<-w.release
	w.mu.Lock()
	w.messages = append(w.messages, messages...)
	w.mu.Unlock()
	return nil
}

func (w *blockingWriteCloser) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return nil
}

func TestAsyncProducerUsesBoundedBufferAndDrainsOnClose(t *testing.T) {
	writer := &blockingWriteCloser{started: make(chan struct{}), release: make(chan struct{})}
	producer := &AsyncProducer{
		writer: writer,
		queue:  make(chan kafkago.Message, 1),
		done:   make(chan struct{}),
	}
	go producer.run()

	message := func(value string) kafkago.Message { return kafkago.Message{Value: []byte(value)} }
	if err := producer.WriteMessages(context.Background(), message("first")); err != nil {
		t.Fatalf("queue first message: %v", err)
	}
	<-writer.started
	if err := producer.WriteMessages(context.Background(), message("second")); err != nil {
		t.Fatalf("queue second message: %v", err)
	}
	if err := producer.WriteMessages(context.Background(), message("third")); !errors.Is(err, ErrProducerBufferFull) {
		t.Fatalf("queue beyond capacity: got %v, want %v", err, ErrProducerBufferFull)
	}

	close(writer.release)
	if err := producer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := producer.WriteMessages(context.Background(), message("late")); !errors.Is(err, ErrProducerClosed) {
		t.Fatalf("queue after close: got %v, want %v", err, ErrProducerClosed)
	}

	writer.mu.Lock()
	defer writer.mu.Unlock()
	if !writer.closed {
		t.Error("underlying writer was not closed")
	}
	if got := len(writer.messages); got != 2 {
		t.Fatalf("delivered %d messages, want 2", got)
	}
	if string(writer.messages[0].Value) != "first" || string(writer.messages[1].Value) != "second" {
		t.Errorf("delivery order = %q, %q", writer.messages[0].Value, writer.messages[1].Value)
	}
}

func TestWriterUsesHashPartitioning(t *testing.T) {
	writer := New("localhost:9092").Writer("session-events")
	if _, ok := writer.Balancer.(*kafkago.Hash); !ok {
		t.Fatalf("Balancer = %T, want *kafka.Hash", writer.Balancer)
	}
	if writer.Async {
		t.Error("underlying writer must stay synchronous so the bounded producer owns queuing")
	}
}
