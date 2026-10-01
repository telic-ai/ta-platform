// Command live-monitor streams interview session events to company
// interviewers: it consumes session-events from Kafka into a Redis ring
// buffer and pub/sub, and serves GET /interviews/{id}/live as SSE.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/livemonitor"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/replay"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/store/redis"
)

func main() {
	logger := logging.New()
	cfg, err := config.Load("live-monitor")
	if err != nil {
		logger.Error("load config", slog.Any("error", err))
		os.Exit(1)
	}
	ringSize := 1000
	if raw := os.Getenv("LIVE_RING_SIZE"); raw != "" {
		if ringSize, err = strconv.Atoi(raw); err != nil || ringSize <= 0 {
			logger.Error("LIVE_RING_SIZE must be a positive integer")
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("connect postgres", slog.Any("error", err))
		os.Exit(1)
	}
	defer db.Close()
	redisClient, err := redis.New(cfg.RedisAddr)
	if err != nil {
		logger.Error("connect redis", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = redisClient.Close() }()
	clickhouseClient, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		logger.Error("connect clickhouse", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = clickhouseClient.Close() }()

	bus := livemonitor.NewRedisBus(redisClient.Raw(), "live:", ringSize, 24*time.Hour)

	// Every replica joins one consumer group: each event is ingested once
	// and Redis pub/sub fans it out to every replica's viewers.
	kafkaClient := kafka.New(cfg.KafkaBrokers)
	groupID := getenv("LIVE_GROUP_ID", "live-monitor-v1")
	go func() {
		// Keep ingesting through transient Kafka or Redis failures. Each
		// attempt opens a new reader, which resumes from the committed
		// offset, so the event that failed is delivered again.
		for ctx.Err() == nil {
			reader := kafkaClient.Reader(events.SessionEventsTopic, groupID)
			ingester := livemonitor.NewIngester(reader, bus)
			ingester.OnInvalid = func(message kafkago.Message, err error) {
				logger.Warn("skip invalid event", slog.Int("partition", message.Partition),
					slog.Int64("offset", message.Offset), slog.Any("error", err))
			}
			err := ingester.Run(ctx)
			_ = reader.Close()
			if err != nil && ctx.Err() == nil {
				logger.Error("live ingest stopped; restarting", slog.Any("error", err))
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
				}
			}
		}
	}()

	store := postgres.NewAdminStore(db.Pool())
	resolver := rbac.Resolver{Sessions: postgres.NewSessionStore(db.Pool()), Roles: store}
	handler := livemonitor.NewHandler(bus, replay.NewClickHouseStore(clickhouseClient.Conn()), store,
		livemonitor.Config{Logger: logger})

	addr := cfg.HTTPAddr
	if os.Getenv("HTTP_ADDR") == "" {
		addr = ":8083"
	}
	// No WriteTimeout: live streams stay open while the interview runs.
	server := &http.Server{Addr: addr, Handler: handler.Routes(resolver), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	logger.Info("starting service", slog.String("service", cfg.ServiceName), slog.String("address", addr))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("serve live monitor", slog.Any("error", err))
		os.Exit(1)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
