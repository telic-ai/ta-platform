//go:build integration

package billing_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/analytics"
	"github.com/telic-ai/ta-platform/internal/billing"
	"github.com/telic-ai/ta-platform/internal/eventlogwriter"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
)

const priceTable = `{"base_fee_cents":10000,"included_sessions":1,"session_cents":2500,
  "managed_input_per_million_cents":300,"managed_output_per_million_cents":1500,"included_runs":0,"run_cents":10}`

var (
	september = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	october   = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
)

type invoice struct {
	ID                       uuid.UUID
	Status                   string
	Subtotal, Credits, Total int64
	Lines                    int
}

type harness struct {
	pool   *pgxpool.Pool
	log    *eventlogwriter.Store
	closer *billing.Closer
	store  *postgres.BillingStore
}

func newHarness(t *testing.T, ctx context.Context) *harness {
	t.Helper()
	cfg, err := config.Load("billing-integration-test")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	log, _ := eventlogwriter.NewStore(ch.Conn())
	if err := log.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := analytics.Migrate(ctx, ch.Conn()); err != nil {
		t.Fatal(err)
	}
	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)
	store := postgres.NewBillingStore(pool)
	closer, err := billing.NewCloser(billing.ClickHouseUsage{Conn: ch.Conn()}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO plans (id, name, price_table) VALUES ('growth', 'Growth', $1)`, priceTable); err != nil {
		t.Fatal(err)
	}
	return &harness{pool: pool, log: log, closer: closer, store: store}
}

func (h *harness) company(t *testing.T, ctx context.Context, plan any) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := h.pool.Exec(ctx, `INSERT INTO companies (id, name, slug, plan_id) VALUES ($1, 'Acme', $2, $3)`, id, id.String(), plan); err != nil {
		t.Fatal(err)
	}
	return id
}

// usage logs events for companyID, each as its own insert (as redelivered
// records arrive), and returns the envelopes for redelivery.
func (h *harness) usage(t *testing.T, ctx context.Context, companyID uuid.UUID, at time.Time, payloads ...events.Payload) [][]byte {
	t.Helper()
	interview := uuid.NewString()
	var values [][]byte
	for i, payload := range payloads {
		envelope, _ := events.New(companyID.String(), int64(i+1), payload)
		envelope.OccurredAt = at
		// Point every payload at one interview.
		var fields map[string]any
		_ = json.Unmarshal(envelope.Payload, &fields)
		fields["interview_id"] = interview
		envelope.Payload, _ = json.Marshal(fields)
		raw, _ := json.Marshal(envelope)
		values = append(values, raw)
	}
	h.deliver(t, ctx, values)
	return values
}

func (h *harness) deliver(t *testing.T, ctx context.Context, values [][]byte) {
	t.Helper()
	for _, value := range values {
		row, err := eventlogwriter.Decode(value, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := h.log.Insert(ctx, []eventlogwriter.Row{row}); err != nil {
			t.Fatal(err)
		}
	}
}

func (h *harness) invoice(t *testing.T, ctx context.Context, companyID uuid.UUID, period time.Time) invoice {
	t.Helper()
	var inv invoice
	err := h.pool.QueryRow(ctx, `
		SELECT i.id, i.status, i.subtotal_cents, i.credits_cents, i.total_cents,
		       (SELECT count(*) FROM invoice_lines l WHERE l.company_id = i.company_id AND l.invoice_id = i.id)
		  FROM invoices i WHERE company_id = $1 AND period = $2`, companyID, period).Scan(
		&inv.ID, &inv.Status, &inv.Subtotal, &inv.Credits, &inv.Total, &inv.Lines)
	if err != nil {
		t.Fatal(err)
	}
	return inv
}

func (h *harness) close(t *testing.T, ctx context.Context, period time.Time) billing.Report {
	t.Helper()
	report, err := h.closer.Close(ctx, period)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Failures) > 0 {
		t.Fatalf("close failures: %+v", report.Failures)
	}
	return report
}

func contains(ids []uuid.UUID, id uuid.UUID) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}

func TestMonthlyCloseIsIdempotentAndNeverTouchesIssuedInvoices(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	h := newHarness(t, ctx)
	acme := h.company(t, ctx, "growth")
	idle := h.company(t, ctx, "growth")
	unplanned := h.company(t, ctx, nil)
	if _, err := h.pool.Exec(ctx, `INSERT INTO billing_credits (company_id, id, amount_cents, description, granted_at)
		VALUES ($1, $2, 20000, 'launch promo', '2026-08-15')`, acme, uuid.New()); err != nil {
		t.Fatal(err)
	}

	mid := september.Add(14 * 24 * time.Hour)
	sessionEvents := h.usage(t, ctx, acme, mid,
		events.SessionStarted{}, events.SessionStarted{}, events.SessionStarted{},
		events.AIResponseCompleted{Mode: "managed", Status: "completed", InputTokens: 2_000_000, OutputTokens: 1_000_000},
		events.AIResponseCompleted{Mode: "byok", Status: "completed", InputTokens: 9_000_000, OutputTokens: 9_000_000},
		events.ExecutionRequested{}, events.ExecutionRequested{},
	)
	h.usage(t, ctx, unplanned, mid, events.SessionStarted{})

	// 3 sessions (2 beyond the 1 included) = 5000, 2M input = 600,
	// 1M output = 1500, 2 runs = 20, base 10000: 17120. The 20000 credit
	// covers all of it. BYOK tokens are not billed.
	report := h.close(t, ctx, september)
	if !contains(report.Created, acme) || !contains(report.Created, idle) || !contains(report.Unplanned, unplanned) {
		t.Fatalf("report = %+v", report)
	}
	first := h.invoice(t, ctx, acme, september)
	if first != (invoice{ID: first.ID, Status: "draft", Subtotal: 17120, Credits: 17120, Total: 0, Lines: 6}) {
		t.Fatalf("first draft = %+v", first)
	}
	if got := h.invoice(t, ctx, idle, september); got.Total != 10000 || got.Lines != 1 {
		t.Fatalf("idle company invoice = %+v", got)
	}

	// Re-running recomputes the same draft: same id, same totals, the
	// credit applied once.
	report = h.close(t, ctx, september)
	if !contains(report.Recomputed, acme) {
		t.Fatalf("rerun report = %+v", report)
	}
	if again := h.invoice(t, ctx, acme, september); again != first {
		t.Fatalf("rerun draft = %+v, want %+v", again, first)
	}

	// Late usage and a full redelivery: the draft picks up the new session
	// only, and the credit now runs out.
	h.deliver(t, ctx, sessionEvents)
	h.usage(t, ctx, acme, mid, events.SessionStarted{}, events.SessionStarted{})
	h.close(t, ctx, september)
	updated := h.invoice(t, ctx, acme, september)
	if updated.ID != first.ID || updated.Subtotal != 22120 || updated.Credits != 20000 || updated.Total != 2120 {
		t.Fatalf("updated draft = %+v", updated)
	}

	// Once issued, a close run leaves the invoice alone.
	if err := h.store.IssueInvoice(ctx, acme, september); err != nil {
		t.Fatal(err)
	}
	if err := h.store.IssueInvoice(ctx, acme, september); err == nil {
		t.Fatal("issued an invoice twice")
	}
	h.usage(t, ctx, acme, mid, events.SessionStarted{})
	report = h.close(t, ctx, september)
	if !contains(report.Final, acme) {
		t.Fatalf("issued invoice not reported final: %+v", report)
	}
	issued := h.invoice(t, ctx, acme, september)
	if issued.Status != "issued" || issued.Subtotal != updated.Subtotal || issued.Total != updated.Total || issued.Lines != updated.Lines {
		t.Fatalf("issued invoice changed: %+v, was %+v", issued, updated)
	}

	// October: the credit is spent; only the base fee is due.
	h.close(t, ctx, october)
	if got := h.invoice(t, ctx, acme, october); got.Subtotal != 10000 || got.Credits != 0 || got.Total != 10000 {
		t.Fatalf("October invoice = %+v", got)
	}
}

func TestCreditIsSharedAcrossDraftsWithoutDoubleSpending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	h := newHarness(t, ctx)
	acme := h.company(t, ctx, "growth")
	if _, err := h.pool.Exec(ctx, `INSERT INTO billing_credits (company_id, id, amount_cents, granted_at)
		VALUES ($1, $2, 15000, '2026-08-01')`, acme, uuid.New()); err != nil {
		t.Fatal(err)
	}
	h.close(t, ctx, september)
	h.close(t, ctx, october)
	h.close(t, ctx, september)
	h.close(t, ctx, october)
	sep, oct := h.invoice(t, ctx, acme, september), h.invoice(t, ctx, acme, october)
	if sep.Credits+oct.Credits != 15000 || sep.Credits != 10000 || oct.Total != 5000 {
		t.Fatalf("September %+v, October %+v; want the 15000 credit split 10000/5000", sep, oct)
	}
}
