//go:build integration

package eventindexer

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
	storekafka "github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/typesense"
)

func TestTextEventsBecomeSearchableAndRedeliveryIsNoop(t *testing.T) {
	cfg, err := config.Load("event-indexer-integration-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	search := typesense.New(cfg.TypesenseURL, cfg.TypesenseKey)
	if err := search.EnsureCollection(ctx, Schema); err != nil {
		t.Fatal(err)
	}

	unique := fmt.Sprintf("%d", time.Now().UnixNano())
	companyID := "company-" + unique
	topic := DefaultTopic + "." + unique
	kafkaClient := storekafka.New(cfg.KafkaBrokers)
	if err := kafkaClient.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	reader := kafkaClient.ReaderTopics([]string{topic}, "event-indexer-integration-"+unique)
	indexer, err := New(reader, search, Config{BatchSize: 10, BatchWait: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- indexer.Run(runCtx) }()
	defer func() {
		stop()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run = %v", err)
		}
		_ = indexer.Close()
	}()

	var values [][]byte
	for i, payload := range []events.Payload{
		events.PromptSubmitted{SessionID: "s-" + unique, InterviewID: "i-" + unique, PromptID: "p1", Prompt: "how do I memoize fibonacci"},
		events.AIResponseCompleted{SessionID: "s-" + unique, InterviewID: "i-" + unique, PromptID: "p1", Status: "completed", ResponseText: "use a lookup table keyed by n"},
		events.CodeDiff{SessionID: "s-" + unique, InterviewID: "i-" + unique, Origin: "manual", Path: "fib.py", Patch: "+cache = {}"},
		events.SessionStarted{SessionID: "s-" + unique, InterviewID: "i-" + unique},
	} {
		envelope, err := events.New(companyID, int64(i+1), payload)
		if err != nil {
			t.Fatal(err)
		}
		value, _ := json.Marshal(envelope)
		values = append(values, value)
	}
	producer := &kafkago.Writer{Addr: kafkago.TCP(cfg.KafkaBrokers), Topic: topic, Transport: &kafkago.Transport{}}
	defer producer.Close()
	produce := func() {
		for _, value := range values {
			if err := producer.WriteMessages(ctx, kafkago.Message{Key: []byte("s-" + unique), Value: value}); err != nil {
				t.Fatal(err)
			}
		}
	}

	tenant := "company_id:=" + companyID
	produce()
	waitForFound(t, ctx, search, "*", tenant, 3)
	for query, wantType := range map[string]string{
		"memoize":      "prompt.submitted",
		"lookup table": "ai.response.completed",
		"cache":        "code.diff",
	} {
		result, err := search.Search(ctx, Collection, typesense.SearchParams{Query: query, QueryBy: "text", FilterBy: tenant})
		if err != nil {
			t.Fatal(err)
		}
		if result.Found != 1 {
			t.Fatalf("search %q found %d, want 1", query, result.Found)
		}
		var doc Document
		_ = json.Unmarshal(result.Hits[0], &doc)
		if doc.EventType != wantType {
			t.Fatalf("search %q hit %s, want %s", query, doc.EventType, wantType)
		}
	}

	// Redeliver every event, then a sentinel; once the sentinel is indexed
	// the redelivered batch has been processed too.
	produce()
	sentinel, _ := events.New(companyID, 99, events.PromptSubmitted{SessionID: "s-" + unique, InterviewID: "i-" + unique, Prompt: "sentinel"})
	value, _ := json.Marshal(sentinel)
	if err := producer.WriteMessages(ctx, kafkago.Message{Key: []byte("s-" + unique), Value: value}); err != nil {
		t.Fatal(err)
	}
	waitForFound(t, ctx, search, "*", tenant, 4)
}

func waitForFound(t *testing.T, ctx context.Context, search *typesense.Client, query, filter string, want int) {
	t.Helper()
	var found int
	for ctx.Err() == nil {
		result, err := search.Search(ctx, Collection, typesense.SearchParams{Query: query, QueryBy: "text", FilterBy: filter})
		if err != nil {
			t.Fatal(err)
		}
		found = result.Found
		if found == want {
			return
		}
		if found > want {
			t.Fatalf("found %d documents, want %d: redelivery duplicated documents", found, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("found %d documents before timeout, want %d", found, want)
}
