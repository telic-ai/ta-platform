// Command billing-close writes draft invoices for one month from ClickHouse
// usage, then exits; it is scheduled as a Kubernetes CronJob
// (deploy/k8s/billing-close). It is safe to re-run: drafts are recomputed
// and issued invoices are never changed.
//
//	billing-close [-period YYYY-MM]   (default: the previous UTC month)
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/telic-ai/ta-platform/internal/billing"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
)

func main() {
	logger := logging.New()
	if err := run(logger); err != nil {
		logger.Error("billing close failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	periodFlag := flag.String("period", "", "month to close as YYYY-MM; defaults to the previous UTC month")
	flag.Parse()
	period := billing.PreviousPeriod(time.Now())
	if *periodFlag != "" {
		parsed, err := billing.ParsePeriod(*periodFlag)
		if err != nil {
			return err
		}
		period = parsed
	}
	cfg, err := config.Load("billing-close")
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

	closer, err := billing.NewCloser(billing.ClickHouseUsage{Conn: ch.Conn()}, postgres.NewBillingStore(db.Pool()))
	if err != nil {
		return err
	}
	report, err := closer.Close(ctx, period)
	if err != nil {
		return err
	}
	for _, companyID := range report.Unplanned {
		logger.Warn("usage without a plan was not invoiced", slog.String("company_id", companyID.String()))
	}
	for _, failure := range report.Failures {
		logger.Error("invoice not written", slog.String("company_id", failure.CompanyID.String()), slog.Any("error", failure.Err))
	}
	logger.Info("billing close finished", slog.String("period", period.Format("2006-01")),
		slog.Int("created", len(report.Created)), slog.Int("recomputed", len(report.Recomputed)),
		slog.Int("final", len(report.Final)), slog.Int("unplanned", len(report.Unplanned)), slog.Int("failed", len(report.Failures)))
	if len(report.Failures) > 0 {
		return errors.New("some invoices could not be written; re-run to retry them")
	}
	return nil
}
