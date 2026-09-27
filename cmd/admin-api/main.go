package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/telic-ai/ta-platform/internal/adminapi"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
)

func main() {
	logger := logging.New()

	cfg, err := config.Load("admin-api")
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

	store := postgres.NewAdminStore(db.Pool())
	resolver := rbac.Resolver{Sessions: postgres.NewSessionStore(db.Pool()), Roles: store}
	handler := adminapi.NewHandler(store, resolver, adminapi.Config{})

	// The Admin API listens on :8082 locally so it can run beside
	// candidate-api; HTTP_ADDR still overrides it.
	addr := cfg.HTTPAddr
	if os.Getenv("HTTP_ADDR") == "" {
		addr = ":8082"
	}
	server := &http.Server{Addr: addr, Handler: handler.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	logger.Info("starting service", slog.String("service", cfg.ServiceName), slog.String("address", addr))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("serve admin API", slog.Any("error", err))
		os.Exit(1)
	}
}
