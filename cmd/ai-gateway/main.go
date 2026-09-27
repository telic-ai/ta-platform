package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
)

func main() {
	logger := logging.New()

	cfg, err := config.Load("ai-gateway")
	if err != nil {
		logger.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ai.response.completed is produced directly (acks=all), not through an
	// outbox: the gateway owns no database, and the event must be written
	// even when the caller has disconnected.
	writer := kafka.New(cfg.KafkaBrokers).Writer(events.SessionEventsTopic)
	defer writer.Close()
	emitter, err := aigateway.NewKafkaEmitter(writer)
	if err != nil {
		logger.Error("create completion emitter", slog.Any("error", err))
		os.Exit(1)
	}
	provider := aigateway.NewAnthropicProvider(aigateway.AnthropicConfig{
		APIKey: os.Getenv("ANTHROPIC_API_KEY"), BaseURL: os.Getenv("ANTHROPIC_BASE_URL"), MaxRetries: 2,
	})
	gateway, err := aigateway.NewGateway(provider, emitter, nil, logger)
	if err != nil {
		logger.Error("create gateway", slog.Any("error", err))
		os.Exit(1)
	}

	// No WriteTimeout: completions stream for as long as the model runs.
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: aigateway.NewHTTPHandler(gateway).Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	logger.Info("starting service", slog.String("service", cfg.ServiceName), slog.String("address", cfg.HTTPAddr))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("serve AI gateway", slog.Any("error", err))
		os.Exit(1)
	}
}
