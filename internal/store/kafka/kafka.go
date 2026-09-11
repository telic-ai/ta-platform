// Package kafka provides the shared Kafka producer/consumer client for
// services, built on segmentio/kafka-go.
package kafka

import (
	"context"
	"fmt"
	"strings"

	kafkago "github.com/segmentio/kafka-go"
)

// Client wraps Kafka broker connectivity for one or more topics.
type Client struct {
	brokers []string
}

// New parses brokers (comma-separated, see internal/platform/config).
func New(brokers string) *Client {
	return &Client{brokers: strings.Split(brokers, ",")}
}

// Writer returns a durable, key-partitioned writer for topic. EventPublisher
// owns asynchronous queueing and bounds the in-memory buffer.
func (c *Client) Writer(topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:         kafkago.TCP(c.brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
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
