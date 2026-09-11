//go:build integration

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
)

func TestSessionEventsStayOnOnePartitionInOrder(t *testing.T) {
	cfg, err := config.Load("session-events-integration-test")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const eventCount = 100
	sessionID := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	deliveries := make(chan error, eventCount)
	client := New(cfg.KafkaBrokers)
	writer := client.Writer("session-events")
	publisher := NewEventPublisherWithConfig(writer, 16, func(_ events.Envelope, err error) { deliveries <- err })

	for sequence := range int64(eventCount) {
		envelope, err := events.New("integration-company", sequence, events.SessionStarted{
			SessionID: sessionID,
			UserID:    "integration-user",
			InviteID:  "integration-invite",
			Scope:     "candidate:workspace",
			ExpiresAt: time.Now().Add(time.Hour).UTC(),
		})
		if err != nil {
			t.Fatalf("build sequence %d: %v", sequence, err)
		}
		if err := publisher.Publish(ctx, envelope); err != nil {
			t.Fatalf("publish sequence %d: %v", sequence, err)
		}
	}
	publisher.Close()
	if err := writer.Close(); err != nil {
		t.Fatalf("close producer: %v", err)
	}
	for sequence := range eventCount {
		if err := <-deliveries; err != nil {
			t.Fatalf("deliver sequence %d: %v", sequence, err)
		}
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     client.brokers,
		Topic:       "session-events",
		GroupID:     "session-events-order-" + sessionID,
		StartOffset: kafkago.FirstOffset,
	})
	defer reader.Close()

	partition := -1
	sequences := make([]int64, 0, eventCount)
	for len(sequences) < eventCount {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read after %d session events: %v", len(sequences), err)
		}
		if string(message.Key) != sessionID {
			continue
		}
		if partition == -1 {
			partition = message.Partition
		}
		if message.Partition != partition {
			t.Errorf("event landed on partition %d, want %d", message.Partition, partition)
		}

		var envelope events.Envelope
		if err := json.Unmarshal(message.Value, &envelope); err != nil {
			t.Fatalf("decode offset %d: %v", message.Offset, err)
		}
		sequences = append(sequences, envelope.SequenceNumber)
	}

	want := make([]int64, eventCount)
	for i := range want {
		want[i] = int64(i)
	}
	if !slices.Equal(sequences, want) {
		t.Fatalf("session sequence order = %v, want %v", sequences, want)
	}
}
