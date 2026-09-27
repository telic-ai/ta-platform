package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/replay"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
)

func main() {
	logger := logging.New()

	cfg, err := config.Load("analytics-service")
	if err != nil {
		logger.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	clickhouseClient, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		logger.Error("connect clickhouse", slog.Any("error", err))
		os.Exit(1)
	}
	defer clickhouseClient.Close()

	handler := replay.NewHTTPHandler(replay.NewClickHouseStore(clickhouseClient.Conn()), replay.HeaderTenantResolver{})
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: handler.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	logger.Info("starting service", slog.String("service", cfg.ServiceName), slog.String("address", cfg.HTTPAddr))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("serve replay API", slog.Any("error", err))
		os.Exit(1)
	}
}
