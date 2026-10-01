package billing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// UsageSource returns every company's usage for a period.
type UsageSource interface {
	Usage(ctx context.Context, period time.Time) (map[uuid.UUID]Usage, error)
}

// Company is a company on a plan.
type Company struct {
	CompanyID  uuid.UUID
	PlanID     string
	Currency   string
	PriceTable []byte
}

// DraftOutcome is what WriteDraft did with a company's invoice.
type DraftOutcome string

const (
	DraftCreated    DraftOutcome = "created"
	DraftRecomputed DraftOutcome = "recomputed"
	// DraftFinal means the invoice is issued or void and was left alone.
	DraftFinal DraftOutcome = "final"
)

// Store persists invoices.
type Store interface {
	// Companies lists every company on a plan.
	Companies(ctx context.Context) ([]Company, error)
	// WriteDraft creates or locks the company's invoice for period. If it is
	// a draft, it calls compute with the credits still available to it —
	// excluding what this invoice itself applied before — and replaces its
	// lines and totals with the result. An issued or void invoice is
	// returned untouched as DraftFinal, without calling compute.
	WriteDraft(ctx context.Context, company Company, period time.Time, compute func([]Credit) ([]Line, error)) (DraftOutcome, error)
}

type Closer struct {
	usage UsageSource
	store Store
}

func NewCloser(usage UsageSource, store Store) (*Closer, error) {
	if usage == nil || store == nil {
		return nil, errors.New("billing: usage source and store are required")
	}
	return &Closer{usage: usage, store: store}, nil
}

// Failure is one company whose invoice could not be written.
type Failure struct {
	CompanyID uuid.UUID
	Err       error
}

// Report summarizes a close run.
type Report struct {
	Period     time.Time
	Created    []uuid.UUID
	Recomputed []uuid.UUID
	Final      []uuid.UUID
	// Unplanned companies had usage but no plan, so nothing could be priced.
	Unplanned []uuid.UUID
	Failures  []Failure
}

// Close writes a draft invoice for every company on a plan for period (the
// first day of a UTC month). Running it again recomputes the drafts.
func (c *Closer) Close(ctx context.Context, period time.Time) (Report, error) {
	if !period.Equal(PeriodStart(period)) {
		return Report{}, fmt.Errorf("billing: period %s is not the start of a UTC month", period)
	}
	report := Report{Period: period}
	usage, err := c.usage.Usage(ctx, period)
	if err != nil {
		return report, fmt.Errorf("billing: read usage: %w", err)
	}
	companies, err := c.store.Companies(ctx)
	if err != nil {
		return report, fmt.Errorf("billing: list companies: %w", err)
	}
	planned := make(map[uuid.UUID]bool, len(companies))
	for _, company := range companies {
		planned[company.CompanyID] = true
		outcome, err := c.closeCompany(ctx, company, period, usage[company.CompanyID])
		switch {
		case err != nil:
			report.Failures = append(report.Failures, Failure{CompanyID: company.CompanyID, Err: err})
		case outcome == DraftCreated:
			report.Created = append(report.Created, company.CompanyID)
		case outcome == DraftRecomputed:
			report.Recomputed = append(report.Recomputed, company.CompanyID)
		default:
			report.Final = append(report.Final, company.CompanyID)
		}
	}
	for companyID := range usage {
		if !planned[companyID] {
			report.Unplanned = append(report.Unplanned, companyID)
		}
	}
	return report, nil
}

func (c *Closer) closeCompany(ctx context.Context, company Company, period time.Time, usage Usage) (DraftOutcome, error) {
	table, err := ParsePriceTable(company.PriceTable)
	if err != nil {
		return "", fmt.Errorf("plan %s: %w", company.PlanID, err)
	}
	charges := Price(usage, table)
	return c.store.WriteDraft(ctx, company, period, func(credits []Credit) ([]Line, error) {
		subtotal, _, _, err := Totals(charges)
		if err != nil {
			return nil, err
		}
		return append(append([]Line(nil), charges...), ApplyCredits(subtotal, credits)...), nil
	})
}

// ClickHouseUsage reads v_billing_usage (internal/analytics), which
// deduplicates the per-event rows mv_billing_usage writes.
type ClickHouseUsage struct{ Conn driver.Conn }

func (u ClickHouseUsage) Usage(ctx context.Context, period time.Time) (map[uuid.UUID]Usage, error) {
	rows, err := u.Conn.Query(ctx, `
		SELECT company_id, sessions, managed_input_tokens, managed_output_tokens,
		       byok_input_tokens, byok_output_tokens, runs, run_duration_ms
		  FROM v_billing_usage
		 WHERE period = toDate(?)`, period)
	if err != nil {
		return nil, fmt.Errorf("query billing usage: %w", err)
	}
	defer rows.Close()
	usage := map[uuid.UUID]Usage{}
	for rows.Next() {
		var (
			companyID string
			row       Usage
		)
		if err := rows.Scan(&companyID, &row.Sessions, &row.ManagedInputTokens, &row.ManagedOutputTokens,
			&row.BYOKInputTokens, &row.BYOKOutputTokens, &row.Runs, &row.RunDurationMS); err != nil {
			return nil, fmt.Errorf("scan billing usage: %w", err)
		}
		id, err := uuid.Parse(companyID)
		if err != nil {
			// Events always carry a UUID company_id; anything else cannot be
			// matched to a plan and is not billable.
			continue
		}
		usage[id] = row
	}
	return usage, rows.Err()
}
