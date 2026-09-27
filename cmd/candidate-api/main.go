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
	"github.com/telic-ai/ta-platform/internal/outbox"
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
	// Session events are committed to the Postgres outbox with the session
	// itself; the relay delivers them to Kafka, retrying until it succeeds.
	writer := kafka.New(cfg.KafkaBrokers).Writer("")
	defer writer.Close()
	relay, err := outbox.NewRelay(postgres.NewOutboxStore(db.Pool()), writer, outbox.Config{
		BatchSize: 100,
		Interval:  100 * time.Millisecond,
		OnError:   func(err error) { logger.Error("relay outbox", slog.Any("error", err)) },
	})
	if err != nil {
		logger.Error("create outbox relay", slog.Any("error", err))
		os.Exit(1)
	}
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		_ = relay.Run(ctx)
	}()
	defer func() { <-relayDone }()

	store := postgres.NewSessionStore(db.Pool())
	service := candidateworkspace.NewService(store, cfg.SessionTTL)
	prompts := candidateworkspace.NewPromptService(postgres.NewEventStore(db.Pool()),
		candidateworkspace.NewHTTPGateway(cfg.AIGatewayURL, nil),
		candidateworkspace.PromptConfig{Model: cfg.AIModel, System: candidateSystemPrompt})
	handler := candidateworkspace.NewHTTPHandler(service, store).WithPrompts(prompts)
	// No WriteTimeout: prompt answers stream for as long as the model runs.
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: handler.Routes(), ReadHeaderTimeout: 5 * time.Second}

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

// candidateSystemPrompt frames the assistant a candidate uses during a
// coding interview.
const candidateSystemPrompt = "You are the coding assistant inside a candidate's technical interview " +
	"workspace. Help the candidate with their task the way a strong pair programmer would: explain " +
	"your reasoning, propose code the candidate can apply, and keep answers focused on their code."
