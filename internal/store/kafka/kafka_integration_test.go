//go:build integration

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
)

func TestClientPing(t *testing.T) {
	cfg, err := config.Load("kafka-integration-test")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	client := New(cfg.KafkaBrokers)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestSessionEventsStayOnOnePartitionInOrder(t *testing.T) {
	cfg, err := config.Load("kafka-session-events-integration-test")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	const eventCount = 20
	sessionID := fmt.Sprintf("session-%d", time.Now().UnixNano())
	deliveryErrors := make(chan error, eventCount)
	producer := New(cfg.KafkaBrokers).AsyncWriter(events.SessionEventsTopic, eventCount, func(err error) {
		deliveryErrors <- err
	})
	publisher := NewEventPublisher(producer)
	for sequence := int64(0); sequence < eventCount; sequence++ {
		envelope, err := events.New("company-1", sequence, events.SessionStarted{SessionID: sessionID})
		if err != nil {
			t.Fatalf("events.New: %v", err)
		}
		if err := publisher.Publish(context.Background(), sessionID, envelope); err != nil {
			t.Fatalf("Publish sequence %d: %v", sequence, err)
		}
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("close producer: %v", err)
	}
	select {
	case err := <-deliveryErrors:
		t.Fatalf("deliver event: %v", err)
	default:
	}
	brokers := strings.Split(cfg.KafkaBrokers, ",")
	metadataCtx, metadataCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer metadataCancel()
	conn, err := kafkago.DialContext(metadataCtx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	partitions, err := conn.ReadPartitions(events.SessionEventsTopic)
	_ = conn.Close()
	if err != nil {
		t.Fatalf("read topic partitions: %v", err)
	}
	if len(partitions) < 2 {
		t.Fatalf("%s has %d partition; want at least 2", events.SessionEventsTopic, len(partitions))
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       events.SessionEventsTopic,
		GroupID:     fmt.Sprintf("session-order-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
		MaxWait:     100 * time.Millisecond,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	partition := -1
	for wantSequence := int64(0); wantSequence < eventCount; {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read sequence %d: %v", wantSequence, err)
		}
		if string(message.Key) != sessionID {
			continue
		}
		if partition == -1 {
			partition = message.Partition
		} else if message.Partition != partition {
			t.Fatalf("sequence %d landed on partition %d, want partition %d", wantSequence, message.Partition, partition)
		}
		var envelope events.Envelope
		if err := json.Unmarshal(message.Value, &envelope); err != nil {
			t.Fatalf("decode sequence %d: %v", wantSequence, err)
		}
		if envelope.SequenceNumber != wantSequence {
			t.Fatalf("received sequence %d, want %d", envelope.SequenceNumber, wantSequence)
		}
		wantSequence++
	}
}
