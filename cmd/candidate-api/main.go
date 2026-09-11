package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/telic-ai/ta-platform/internal/candidateworkspace"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
)

func main() {
	logger := logging.New()

	cfg, err := config.Load("candidate-api")
	if err != nil {
		logger.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("connect postgres", slog.Any("error", err))
		os.Exit(1)
	}
	defer db.Close()
	kafkaClient := kafka.New(cfg.KafkaBrokers)
	writer := kafkaClient.Writer(candidateworkspace.SessionEventsTopic)
	defer writer.Close()
	store := postgres.NewSessionStore(db.Pool())
	publisher := kafka.NewEventPublisher(writer)
	defer publisher.Close()
	service := candidateworkspace.NewService(store, publisher, cfg.SessionTTL)
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: candidateworkspace.NewHTTPHandler(service, store).Routes(), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	logger.Info("starting service", slog.String("service", cfg.ServiceName), slog.String("address", cfg.HTTPAddr))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("serve candidate API", slog.Any("error", err))
		os.Exit(1)
	}
}
