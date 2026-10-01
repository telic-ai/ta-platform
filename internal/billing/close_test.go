package billing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var september = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

func TestClosePricesUsageAppliesCreditsAndReports(t *testing.T) {
	billed, idle, unplanned := uuid.New(), uuid.New(), uuid.New()
	credit := uuid.New()
	store := &fakeStore{
		companies: []Company{
			{CompanyID: billed, PlanID: "growth", Currency: "USD", PriceTable: []byte(`{"base_fee_cents":1000,"session_cents":100}`)},
			{CompanyID: idle, PlanID: "growth", Currency: "USD", PriceTable: []byte(`{"base_fee_cents":1000,"session_cents":100}`)},
		},
		credits: map[uuid.UUID][]Credit{billed: {{ID: credit, RemainingCents: 300}}},
	}
	usage := fakeUsage{billed: {Sessions: 5}, unplanned: {Sessions: 9}}
	closer, _ := NewCloser(usage, store)
	report, err := closer.Close(context.Background(), september)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Created) != 2 || len(report.Unplanned) != 1 || report.Unplanned[0] != unplanned {
		t.Fatalf("report = %+v", report)
	}
	lines := store.lines[billed]
	subtotal, credits, total, _ := Totals(lines)
	if subtotal != 1500 || credits != 300 || total != 1200 || *lines[len(lines)-1].CreditID != credit {
		t.Fatalf("billed invoice = %+v (%d/%d/%d)", lines, subtotal, credits, total)
	}
	if _, _, total, _ := Totals(store.lines[idle]); total != 1000 {
		t.Fatalf("idle company total = %d, want the base fee", total)
	}
}

func TestCloseLeavesFinalInvoicesAndReportsFailures(t *testing.T) {
	final, broken, ok := uuid.New(), uuid.New(), uuid.New()
	store := &fakeStore{
		companies: []Company{
			{CompanyID: final, PriceTable: []byte(`{}`)},
			{CompanyID: broken, PlanID: "bad", PriceTable: []byte(`{"base_fee_cents":-5}`)},
			{CompanyID: ok, PriceTable: []byte(`{}`)},
		},
		final: map[uuid.UUID]bool{final: true},
	}
	closer, _ := NewCloser(fakeUsage{}, store)
	report, err := closer.Close(context.Background(), september)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Final) != 1 || report.Final[0] != final || len(report.Failures) != 1 || report.Failures[0].CompanyID != broken || len(report.Created) != 1 {
		t.Fatalf("report = %+v", report)
	}
	if _, computed := store.lines[final]; computed {
		t.Fatal("computed lines for a final invoice")
	}
}

func TestCloseRejectsMidMonthPeriodAndSourceErrors(t *testing.T) {
	closer, _ := NewCloser(fakeUsage{}, &fakeStore{})
	if _, err := closer.Close(context.Background(), september.Add(time.Hour)); err == nil {
		t.Error("mid-month period accepted")
	}
	closer, _ = NewCloser(failingUsage{}, &fakeStore{})
	if _, err := closer.Close(context.Background(), september); err == nil {
		t.Error("usage error swallowed")
	}
	if _, err := NewCloser(nil, &fakeStore{}); err == nil {
		t.Error("nil usage source accepted")
	}
}

type fakeUsage map[uuid.UUID]Usage

func (u fakeUsage) Usage(context.Context, time.Time) (map[uuid.UUID]Usage, error) { return u, nil }

type failingUsage struct{}

func (failingUsage) Usage(context.Context, time.Time) (map[uuid.UUID]Usage, error) {
	return nil, errors.New("clickhouse down")
}

type fakeStore struct {
	companies []Company
	credits   map[uuid.UUID][]Credit
	final     map[uuid.UUID]bool
	lines     map[uuid.UUID][]Line
}

func (s *fakeStore) Companies(context.Context) ([]Company, error) { return s.companies, nil }

func (s *fakeStore) WriteDraft(_ context.Context, company Company, _ time.Time, compute func([]Credit) ([]Line, error)) (DraftOutcome, error) {
	if s.final[company.CompanyID] {
		return DraftFinal, nil
	}
	lines, err := compute(s.credits[company.CompanyID])
	if err != nil {
		return "", err
	}
	if s.lines == nil {
		s.lines = map[uuid.UUID][]Line{}
	}
	_, existed := s.lines[company.CompanyID]
	s.lines[company.CompanyID] = lines
	if existed {
		return DraftRecomputed, nil
	}
	return DraftCreated, nil
}
