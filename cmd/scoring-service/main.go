// Command scoring-service scores Candidate Workspace sessions when they end.
// score.computed is written to the Postgres outbox, which the outbox relay
// (run by candidate-api) publishes.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/scoring"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
)

func main() {
	logger := logging.New()
	if err := run(logger); err != nil {
		logger.Error("scoring service stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load("scoring-service")
	if err != nil {
		return err
	}
	maxAttempts, err := positiveInt("SCORING_MAX_ATTEMPTS", 8)
	if err != nil {
		return err
	}
	baseBackoff, err := positiveDuration("SCORING_BASE_BACKOFF", 2*time.Second)
	if err != nil {
		return err
	}
	maxBackoff, err := positiveDuration("SCORING_MAX_BACKOFF", time.Minute)
	if err != nil {
		return err
	}
	recommendTimeout, err := positiveDuration("SCORING_RECOMMEND_TIMEOUT", time.Minute)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	ch, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		return err
	}
	defer ch.Close()

	// Without a managed key, scores are stored with recommendation_error
	// "disabled".
	var recommender scoring.Recommender
	if os.Getenv("ANTHROPIC_API_KEY") != "" {
		recommender = scoring.ProviderRecommender{
			Provider: aigateway.NewAnthropicProvider(aigateway.AnthropicConfig{
				APIKey: os.Getenv("ANTHROPIC_API_KEY"), BaseURL: os.Getenv("ANTHROPIC_BASE_URL"), MaxRetries: 2,
			}),
			Model: os.Getenv("SCORING_MODEL"),
		}
	}
	service, err := scoring.NewService(scoring.NewClickHouseLog(ch.Conn()), postgres.NewScoreStore(db.Pool()), recommender,
		scoring.Config{RecommendTimeout: recommendTimeout})
	if err != nil {
		return err
	}

	reader := kafka.New(cfg.KafkaBrokers).Reader(getenv("SCORING_TOPIC", events.SessionEventsTopic), getenv("SCORING_GROUP_ID", "scoring-service-v1"))
	consumer, err := scoring.NewConsumer(reader, service, scoring.ConsumerConfig{
		MaxAttempts: maxAttempts, BaseBackoff: baseBackoff, MaxBackoff: maxBackoff,
		OnInvalid: func(message kafkago.Message, err error) {
			logger.Error("skip unscorable event",
				slog.Int("partition", message.Partition), slog.Int64("offset", message.Offset), slog.Any("error", err))
		},
		OnScored: func(trigger scoring.Trigger, outcome scoring.Outcome, attempts int) {
			logger.Info("session scored",
				slog.String("company_id", trigger.CompanyID.String()), slog.String("session_id", trigger.SessionID.String()),
				slog.String("outcome", string(outcome)), slog.Int("attempts", attempts))
		},
	})
	if err != nil {
		return err
	}
	defer consumer.Close()
	logger.Info("scoring service started", slog.Bool("ai_recommendations", recommender != nil))
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
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
	value := getenv(key, strconv.Itoa(fallback))
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer; got %q", key, value)
	}
	return parsed, nil
}

func positiveDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := getenv(key, fallback.String())
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration; got %q", key, value)
	}
	return parsed, nil
}
