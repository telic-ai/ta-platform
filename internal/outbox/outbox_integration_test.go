//go:build integration

package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
)

// TestSessionEventsStayOnOnePartitionInOrder relays interleaved events of two
// sessions through two competing relays and asserts that each session's
// events land on one partition, in order.
func TestSessionEventsStayOnOnePartitionInOrder(t *testing.T) {
	cfg, err := config.Load("outbox-integration-test")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)
	companyID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO companies (id, name, slug) VALUES ($1, 'Outbox', $2)`,
		companyID, "outbox-"+companyID.String()); err != nil {
		t.Fatalf("seed company: %v", err)
	}

	topic := events.TopicSessionEvents + ".outbox." + uuid.NewString()[:8]
	client := kafka.New(cfg.KafkaBrokers)
	if err := client.CreateTopic(ctx, topic, 6, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	const perSession = 50
	sessions := []string{"session-a-" + uuid.NewString(), "session-b-" + uuid.NewString()}
	for sequence := range int64(perSession) {
		for _, sessionID := range sessions {
			envelope, err := events.New(companyID.String(), sequence, events.SessionStarted{SessionID: sessionID})
			if err != nil {
				t.Fatal(err)
			}
			message, err := outbox.NewMessage(topic, sessionID, envelope)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `
				INSERT INTO event_outbox (company_id, topic, message_key, event_type, envelope)
				VALUES ($1, $2, $3, $4, $5)`,
				message.CompanyID, message.Topic, message.Key, string(message.EventType), message.Envelope); err != nil {
				t.Fatalf("seed outbox: %v", err)
			}
		}
	}

	relayCtx, stopRelays := context.WithCancel(ctx)
	defer stopRelays()
	store := postgres.NewOutboxStore(pool)
	relayDone := make(chan error, 2)
	for range 2 {
		writer := client.Writer("")
		defer writer.Close()
		relay, err := outbox.NewRelay(store, writer, outbox.Config{
			BatchSize: 7, Interval: 10 * time.Millisecond,
			OnError: func(err error) { t.Errorf("relay: %v", err) },
		})
		if err != nil {
			t.Fatal(err)
		}
		go func() { relayDone <- relay.Run(relayCtx) }()
	}

	for {
		var pending int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox`).Scan(&pending); err != nil {
			t.Fatalf("count outbox: %v", err)
		}
		if pending == 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("%d outbox rows were never relayed", pending)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopRelays()
	for range 2 {
		if err := <-relayDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("relay stopped: %v", err)
		}
	}

	reader := client.Reader(topic, "outbox-order-"+topic)
	defer reader.Close()
	partitions := map[string]int{}
	sequences := map[string][]int64{}
	for received := 0; received < perSession*len(sessions); received++ {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read after %d events: %v", received, err)
		}
		key := string(message.Key)
		if partition, seen := partitions[key]; seen && partition != message.Partition {
			t.Errorf("%s landed on partitions %d and %d", key, partition, message.Partition)
		}
		partitions[key] = message.Partition
		var envelope events.Envelope
		if err := json.Unmarshal(message.Value, &envelope); err != nil {
			t.Fatalf("decode offset %d: %v", message.Offset, err)
		}
		sequences[key] = append(sequences[key], envelope.SequenceNumber)
	}

	want := make([]int64, perSession)
	for i := range want {
		want[i] = int64(i)
	}
	for _, sessionID := range sessions {
		if !slices.Equal(sequences[sessionID], want) {
			t.Errorf("%s sequence order = %v, want %v", sessionID, sequences[sessionID], fmt.Sprint(want))
		}
	}
}
