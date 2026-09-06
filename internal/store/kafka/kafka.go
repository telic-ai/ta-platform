// Package kafka provides the shared Kafka producer/consumer client for
// services, built on segmentio/kafka-go.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Client wraps Kafka broker connectivity for one or more topics.
type Client struct {
	brokers []string
}

const DefaultProducerBufferSize = 256

var (
	ErrProducerBufferFull = errors.New("kafka: producer buffer is full")
	ErrProducerClosed     = errors.New("kafka: producer is closed")
)

// New parses brokers (comma-separated, see internal/platform/config).
func New(brokers string) *Client {
	return &Client{brokers: strings.Split(brokers, ",")}
}

// Writer returns a writer that publishes to topic.
func (c *Client) Writer(topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(c.brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.Hash{},
		BatchSize:              100,
		BatchTimeout:           10 * time.Millisecond,
		RequiredAcks:           kafkago.RequireAll,
		AllowAutoTopicCreation: true,
	}
}

// AsyncProducer accepts messages without waiting for a broker round trip. Its
// in-memory queue has a fixed capacity; callers receive ErrProducerBufferFull
// instead of allowing an unavailable broker to grow memory without bound.
type AsyncProducer struct {
	writer  messageWriteCloser
	queue   chan kafkago.Message
	onError func(error)

	mu       sync.Mutex
	closed   bool
	closeErr error
	done     chan struct{}
}

type messageWriteCloser interface {
	MessageWriter
	io.Closer
}

// AsyncWriter returns a bounded asynchronous producer. Messages with the same
// non-empty key are hashed to the same partition and submitted in FIFO order.
func (c *Client) AsyncWriter(topic string, bufferSize int, onError func(error)) *AsyncProducer {
	if bufferSize <= 0 {
		bufferSize = DefaultProducerBufferSize
	}
	p := &AsyncProducer{
		writer:  c.Writer(topic),
		queue:   make(chan kafkago.Message, bufferSize),
		onError: onError,
		done:    make(chan struct{}),
	}
	go p.run()
	return p
}

// WriteMessages queues messages for delivery. A successful return means the
// messages were accepted into the local buffer, not yet acknowledged by Kafka.
func (p *AsyncProducer) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrProducerClosed
	}
	if len(messages) > cap(p.queue)-len(p.queue) {
		return ErrProducerBufferFull
	}
	for _, message := range messages {
		p.queue <- message
	}
	return nil
}

// Close drains the accepted messages, closes the Kafka writer, and waits for
// all background delivery to finish.
func (p *AsyncProducer) Close() error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()
	<-p.done
	return p.closeErr
}

func (p *AsyncProducer) run() {
	defer close(p.done)
	for message := range p.queue {
		batch := []kafkago.Message{message}
		for len(batch) < 100 {
			select {
			case next, ok := <-p.queue:
				if !ok {
					p.write(batch)
					p.closeErr = p.writer.Close()
					return
				}
				batch = append(batch, next)
			default:
				p.write(batch)
				goto nextBatch
			}
		}
		p.write(batch)
	nextBatch:
	}
	p.closeErr = p.writer.Close()
}

func (p *AsyncProducer) write(messages []kafkago.Message) {
	if err := p.writer.WriteMessages(context.Background(), messages...); err != nil && p.onError != nil {
		p.onError(err)
	}
}

// Reader returns a reader consuming topic as part of groupID.
func (c *Client) Reader(topic, groupID string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: c.brokers,
		Topic:   topic,
		GroupID: groupID,
	})
}

// Ping verifies at least one broker is reachable.
func (c *Client) Ping(ctx context.Context) error {
	if len(c.brokers) == 0 {
		return fmt.Errorf("kafka: no brokers configured")
	}
	dialer := kafkago.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return fmt.Errorf("kafka: dial %s: %w", c.brokers[0], err)
	}
	defer conn.Close()
	return nil
}
