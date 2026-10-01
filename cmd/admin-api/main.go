package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/telic-ai/ta-platform/internal/adminapi"
	"github.com/telic-ai/ta-platform/internal/dashboard"
	"github.com/telic-ai/ta-platform/internal/eventlogwriter"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/replay"
	"github.com/telic-ai/ta-platform/internal/search"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
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

	clickhouseClient, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		logger.Error("connect clickhouse", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = clickhouseClient.Close() }()
	// The dashboard views read the event log, so its table must exist first.
	eventLog, err := eventlogwriter.NewStore(clickhouseClient.Conn())
	if err != nil {
		logger.Error("create event log store", slog.Any("error", err))
		os.Exit(1)
	}
	views := dashboard.NewClickHouseStore(clickhouseClient.Conn())
	if err := eventLog.EnsureSchema(ctx); err != nil {
		logger.Error("ensure event log", slog.Any("error", err))
		os.Exit(1)
	}
	if err := views.EnsureViews(ctx); err != nil {
		logger.Error("ensure dashboard views", slog.Any("error", err))
		os.Exit(1)
	}

	store := postgres.NewAdminStore(db.Pool())
	resolver := rbac.Resolver{Sessions: postgres.NewSessionStore(db.Pool()), Roles: store}
	handler := adminapi.NewHandler(store, resolver, adminapi.Config{})
	dashboard.NewHandler(views, replay.NewClickHouseStore(clickhouseClient.Conn()), store).Mount(handler)
	// TYPESENSE_SEARCH_KEY must be a search-only key, never the admin key:
	// every key the app receives is derived from it.
	search.NewIssuer(search.Config{
		ParentKey:   os.Getenv("TYPESENSE_SEARCH_KEY"),
		Host:        getenv("TYPESENSE_URL", "http://localhost:8108"),
		Collections: splitNonEmpty(os.Getenv("TYPESENSE_COLLECTIONS")),
	}).Mount(handler)

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

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitNonEmpty(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
