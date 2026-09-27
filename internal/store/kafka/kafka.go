// Package kafka provides the shared Kafka producer/consumer client for
// services, built on segmentio/kafka-go.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

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

// WriterBatchTimeout bounds how long a synchronous write waits for a batch to
// fill. kafka-go's 1s default would cap EventPublisher at about one write per
// second, since it writes whatever it has queued and waits for the ack.
const WriterBatchTimeout = 5 * time.Millisecond

// Writer returns a durable, key-partitioned writer for topic. EventPublisher
// owns asynchronous queueing and bounds the in-memory buffer.
func (c *Client) Writer(topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:         kafkago.TCP(c.brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		BatchTimeout: WriterBatchTimeout,
		// kafka-go's shared DefaultTransport caches cluster metadata, so a
		// writer created after a topic would not see it until the cache
		// expires. An owned transport fetches fresh metadata.
		Transport: &kafkago.Transport{},
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

// ReaderTopics returns one consumer-group reader subscribed to multiple
// catalog topics. Kafka assigns each partition to exactly one group member.
func (c *Client) ReaderTopics(topics []string, groupID string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     c.brokers,
		GroupTopics: topics,
		GroupID:     groupID,
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

// CreateTopic creates topic through the cluster controller. An existing topic
// is not an error. Auto-created topics fail the first write, so tests and
// tooling create topics explicitly.
func (c *Client) CreateTopic(ctx context.Context, topic string, partitions, replicationFactor int) error {
	dialer := kafkago.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return fmt.Errorf("kafka: dial %s: %w", c.brokers[0], err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("kafka: find controller: %w", err)
	}
	controllerConn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("kafka: dial controller: %w", err)
	}
	defer controllerConn.Close()
	err = controllerConn.CreateTopics(kafkago.TopicConfig{
		Topic: topic, NumPartitions: partitions, ReplicationFactor: replicationFactor,
	})
	if err != nil && !errors.Is(err, kafkago.TopicAlreadyExists) {
		return fmt.Errorf("kafka: create topic %s: %w", topic, err)
	}
	return waitForLeaders(ctx, conn, topic, partitions)
}

// waitForLeaders polls broker metadata until every partition of topic has a
// leader. Writes before then fail with UnknownTopicOrPartition.
func waitForLeaders(ctx context.Context, conn *kafkago.Conn, topic string, partitions int) error {
	for {
		found, err := conn.ReadPartitions(topic)
		if err == nil && len(found) == partitions {
			ready := true
			for _, partition := range found {
				if partition.Leader.Host == "" {
					ready = false
					break
				}
			}
			if ready {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafka: wait for topic %s leaders: %w", topic, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}
