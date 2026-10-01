// Command event-indexer makes candidate prompts, AI responses and code
// diffs searchable in Typesense.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/eventindexer"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/typesense"
)

func main() {
	logger := logging.New()
	if err := run(logger); err != nil {
		logger.Error("event indexer stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load("event-indexer")
	if err != nil {
		return err
	}
	batchSize, err := positiveInt("EVENT_INDEXER_BATCH_SIZE", 200)
	if err != nil {
		return err
	}
	batchWait, err := time.ParseDuration(getenv("EVENT_INDEXER_BATCH_WAIT", "1s"))
	if err != nil || batchWait <= 0 {
		return &configError{key: "EVENT_INDEXER_BATCH_WAIT"}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	search := typesense.New(cfg.TypesenseURL, cfg.TypesenseKey)
	if err := search.EnsureCollection(ctx, eventindexer.Schema); err != nil {
		return err
	}

	var topics []string
	for _, topic := range strings.Split(getenv("EVENT_INDEXER_TOPICS", eventindexer.DefaultTopic), ",") {
		if topic = strings.TrimSpace(topic); topic != "" {
			topics = append(topics, topic)
		}
	}
	reader := kafka.New(cfg.KafkaBrokers).ReaderTopics(topics, getenv("EVENT_INDEXER_GROUP_ID", "event-indexer-v1"))
	indexer, err := eventindexer.New(reader, search, eventindexer.Config{
		BatchSize: batchSize,
		BatchWait: batchWait,
		OnInvalid: func(message kafkago.Message, err error) {
			logger.Error("skip unindexable event",
				slog.String("topic", message.Topic),
				slog.Int("partition", message.Partition),
				slog.Int64("offset", message.Offset),
				slog.Any("error", err))
		},
	})
	if err != nil {
		return err
	}
	defer indexer.Close()
	logger.Info("event indexer started", slog.Any("topics", topics), slog.Int("batch_size", batchSize))
	if err := indexer.Run(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func positiveInt(key string, fallback int) (int, error) {
	parsed, err := strconv.Atoi(getenv(key, strconv.Itoa(fallback)))
	if err != nil || parsed <= 0 {
		return 0, &configError{key: key}
	}
	return parsed, nil
}

type configError struct{ key string }

func (e *configError) Error() string { return e.key + " must be positive" }
