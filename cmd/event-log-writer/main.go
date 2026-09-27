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

	"github.com/telic-ai/ta-platform/internal/eventlogwriter"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
)

func main() {
	logger := logging.New()
	cfg, err := config.Load("event-log-writer")
	if err != nil {
		logger.Error("load config", slog.Any("error", err))
		os.Exit(1)
	}
	batchSize, err := positiveInt("EVENT_LOG_BATCH_SIZE", 500)
	if err != nil {
		logger.Error("load config", slog.Any("error", err))
		os.Exit(1)
	}
	batchWait, err := positiveDuration("EVENT_LOG_BATCH_WAIT", time.Second)
	if err != nil {
		logger.Error("load config", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	clickhouseClient, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		logger.Error("connect ClickHouse", slog.Any("error", err))
		os.Exit(1)
	}
	defer clickhouseClient.Close()
	store, err := eventlogwriter.NewStore(clickhouseClient.Conn())
	if err != nil {
		logger.Error("create event store", slog.Any("error", err))
		os.Exit(1)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		logger.Error("migrate event store", slog.Any("error", err))
		os.Exit(1)
	}

	topics := splitNonEmpty(getenv("EVENT_LOG_TOPICS", eventlogwriter.DefaultTopic))
	reader := kafka.New(cfg.KafkaBrokers).ReaderTopics(topics, getenv("EVENT_LOG_GROUP_ID", "event-log-writer-v1"))
	writer, err := eventlogwriter.New(reader, store, eventlogwriter.Config{BatchSize: batchSize, BatchWait: batchWait})
	if err != nil {
		logger.Error("create event log writer", slog.Any("error", err))
		os.Exit(1)
	}
	defer writer.Close()
	logger.Info("event log writer started", slog.Any("topics", topics), slog.Int("batch_size", batchSize))
	if err := writer.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("event log writer stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func splitNonEmpty(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}

func positiveInt(key string, fallback int) (int, error) {
	value := getenv(key, strconv.Itoa(fallback))
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, &configError{key: key, value: value}
	}
	return parsed, nil
}

func positiveDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := getenv(key, fallback.String())
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, &configError{key: key, value: value}
	}
	return parsed, nil
}

type configError struct{ key, value string }

func (e *configError) Error() string {
	return e.key + " must be positive; got " + strconv.Quote(e.value)
}
