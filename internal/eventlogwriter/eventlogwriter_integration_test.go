//go:build integration

package eventlogwriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
	storekafka "github.com/telic-ai/ta-platform/internal/store/kafka"
)

func TestRedeliveryIsVisibleAndDeduplicated(t *testing.T) {
	cfg, err := config.Load("event-log-writer-integration-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clickhouseClient, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseClient.Close()
	store, err := NewStore(clickhouseClient.Conn())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	unique := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := DefaultTopic + "." + unique
	kafkaClient := storekafka.New(cfg.KafkaBrokers)
	reader := kafkaClient.ReaderTopics([]string{topic}, "event-log-writer-integration-"+unique)
	writer, err := New(reader, store, Config{BatchSize: 2, BatchWait: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	writerCtx, stopWriter := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- writer.Run(writerCtx) }()

	envelope, err := events.New("company-"+unique, 1, events.InterviewCompleted{InterviewID: "interview-" + unique, Outcome: "advance"})
	if err != nil {
		t.Fatal(err)
	}
	value, _ := json.Marshal(envelope)
	producer := &kafkago.Writer{Addr: kafkago.TCP(cfg.KafkaBrokers), Topic: topic, AllowAutoTopicCreation: true}
	defer producer.Close()
	if err := producer.WriteMessages(ctx, kafkago.Message{Value: value}, kafkago.Message{Value: value}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		var count uint64
		err := clickhouseClient.Conn().QueryRow(ctx,
			"SELECT count() FROM events FINAL WHERE company_id = ? AND interview_id = ?",
			envelope.CompanyID, "interview-"+unique,
		).Scan(&count)
		if err == nil && count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("deduplicated visible count = %d, query error = %v; want 1", count, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	stopWriter()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("writer stopped: %v", err)
	}
}
