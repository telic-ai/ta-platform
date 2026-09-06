package main

import (
	"log/slog"
	"os"

	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
)

func main() {
	logger := logging.New()

	cfg, err := config.Load("auth-service")
	if err != nil {
		logger.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("starting service", slog.String("service", cfg.ServiceName))
}
