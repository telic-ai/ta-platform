// Command housekeeper runs one purge pass and exits; it is scheduled as a
// Kubernetes CronJob (deploy/k8s/housekeeper). It exits non-zero if any
// interview failed to purge, so the failure is visible on the Job; that
// interview is retried on the next run.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/telic-ai/ta-platform/internal/housekeeper"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/store/typesense"
)

func main() {
	logger := logging.New()
	if err := run(logger); err != nil {
		logger.Error("housekeeper failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load("housekeeper")
	if err != nil {
		return err
	}
	retention, err := time.ParseDuration(getenv("RETENTION", "8760h"))
	if err != nil || retention <= 0 {
		return fmt.Errorf("RETENTION must be a positive duration")
	}
	batchSize, err := strconv.Atoi(getenv("HOUSEKEEPER_BATCH_SIZE", "500"))
	if err != nil || batchSize <= 0 {
		return fmt.Errorf("HOUSEKEEPER_BATCH_SIZE must be a positive integer")
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

	h, err := housekeeper.New(
		postgres.NewHousekeeperStore(db.Pool()),
		housekeeper.TypesenseSearch{Client: typesense.New(cfg.TypesenseURL, cfg.TypesenseKey)},
		housekeeper.ClickHouseAnalytics{Conn: ch.Conn()},
		housekeeper.Config{Retention: retention, BatchSize: batchSize},
	)
	if err != nil {
		return err
	}
	report, err := h.Run(ctx)
	for _, purged := range report.Purged {
		logger.Info("interview purged", slog.String("company_id", purged.CompanyID.String()),
			slog.String("interview_id", purged.InterviewID.String()), slog.String("reason", purged.Reason))
	}
	for _, failure := range report.Failures {
		logger.Error("interview purge failed", slog.String("company_id", failure.Candidate.CompanyID.String()),
			slog.String("interview_id", failure.Candidate.InterviewID.String()), slog.Any("error", failure.Err))
	}
	logger.Info("housekeeper run finished", slog.Int("candidates", report.Candidates),
		slog.Int("purged", len(report.Purged)), slog.Int("skipped", len(report.Skipped)), slog.Int("failed", len(report.Failures)))
	if err != nil {
		return err
	}
	if len(report.Failures) > 0 {
		return errors.New("some interviews could not be purged; they will be retried next run")
	}
	return nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
