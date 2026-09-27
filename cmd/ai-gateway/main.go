package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/aigateway/byok"
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
	// BYOK: company keys are envelope-encrypted in Secrets Manager and
	// decrypted in-process through KMS. AWS settings come from the standard
	// environment (region, credentials, AWS_ENDPOINT_URL for local stacks).
	var keys aigateway.KeyResolver
	var resolver *byok.Resolver
	if os.Getenv("BYOK_ENABLED") == "true" {
		awsConfig, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			logger.Error("load AWS config", slog.Any("error", err))
			os.Exit(1)
		}
		ttl, err := time.ParseDuration(getenv("BYOK_KEY_TTL", "5m"))
		if err != nil || ttl <= 0 {
			logger.Error("BYOK_KEY_TTL must be a positive duration")
			os.Exit(1)
		}
		resolver = byok.NewResolver(byok.NewAWSSecrets(secretsmanager.NewFromConfig(awsConfig)),
			byok.NewAWSKMS(kms.NewFromConfig(awsConfig)),
			byok.Config{SecretPrefix: os.Getenv("BYOK_SECRET_PREFIX"), TTL: ttl, Logger: logger})
		// Sweeps idle keys past their TTL and zeroes everything on shutdown.
		go resolver.Run(ctx, 30*time.Second)
		keys = resolver
	}
	gateway, err := aigateway.NewGateway(provider, emitter, keys, logger)
	if err != nil {
		logger.Error("create gateway", slog.Any("error", err))
		os.Exit(1)
	}
	handler := aigateway.NewHTTPHandler(gateway)
	if resolver != nil {
		handler = handler.WithKeyInvalidation(resolver.Invalidate)
	}

	// The gateway listens on :8090 locally (AI_GATEWAY_URL's default) so it
	// can run beside candidate-api; HTTP_ADDR still overrides it.
	addr := cfg.HTTPAddr
	if os.Getenv("HTTP_ADDR") == "" {
		addr = ":8090"
	}
	// No WriteTimeout: completions stream for as long as the model runs.
	server := &http.Server{Addr: addr, Handler: handler.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	logger.Info("starting service", slog.String("service", cfg.ServiceName), slog.String("address", addr))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("serve AI gateway", slog.Any("error", err))
		os.Exit(1)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
