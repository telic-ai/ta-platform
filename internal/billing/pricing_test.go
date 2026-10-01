package billing

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

var table = PriceTable{
	BaseFeeCents: 50_000, IncludedSessions: 10, SessionCents: 1_500,
	ManagedInputPerMillionCents: 300, ManagedOutputPerMillionCents: 1_500,
	IncludedRuns: 100, RunCents: 2,
}

func TestPriceChargesUsageBeyondAllowances(t *testing.T) {
	lines := Price(Usage{
		Sessions: 12, ManagedInputTokens: 2_000_000, ManagedOutputTokens: 1_000_001,
		BYOKInputTokens: 9_000_000, BYOKOutputTokens: 9_000_000, Runs: 150,
	}, table)
	want := []Line{
		{Kind: KindBaseFee, Description: "Monthly platform fee", Quantity: 1, AmountCents: 50_000},
		{Kind: KindSessions, Description: "Candidate sessions beyond 10 included", Quantity: 2, AmountCents: 3_000},
		{Kind: KindManagedInputTokens, Description: "Managed AI input tokens", Quantity: 2_000_000, AmountCents: 600},
		// 1,000,001 tokens at $15/M is 1500.0015 cents: rounded up.
		{Kind: KindManagedOutputTokens, Description: "Managed AI output tokens", Quantity: 1_000_001, AmountCents: 1_501},
		{Kind: KindRuns, Description: "Code runs beyond 100 included", Quantity: 50, AmountCents: 100},
	}
	if !reflect.DeepEqual(lines, want) {
		t.Fatalf("lines = %+v\nwant  %+v", lines, want)
	}
}

func TestPriceWithinAllowancesIsJustTheBaseFee(t *testing.T) {
	lines := Price(Usage{Sessions: 10, Runs: 100, BYOKInputTokens: 5_000_000}, table)
	if len(lines) != 1 || lines[0].Kind != KindBaseFee {
		t.Fatalf("lines = %+v", lines)
	}
	if free := Price(Usage{}, PriceTable{}); len(free) != 1 || free[0].AmountCents != 0 {
		t.Fatalf("free plan lines = %+v", free)
	}
}

func TestPerMillionCeil(t *testing.T) {
	for _, tc := range []struct{ quantity, price, want int64 }{
		{0, 300, 0}, {1, 300, 1}, {1_000_000, 300, 300}, {3_333_333, 300, 1_000},
		{3_333_334, 300, 1_001}, {100, 0, 0},
		// quantity*price overflows int64; the result does not.
		{math.MaxInt64 / 10, 1_000, 922_337_203_685_478},
	} {
		if got := perMillionCeil(tc.quantity, tc.price); got != tc.want {
			t.Errorf("perMillionCeil(%d, %d) = %d, want %d", tc.quantity, tc.price, got, tc.want)
		}
	}
}

func TestApplyCreditsSpendsSoonestExpiringFirstAndNeverGoesNegative(t *testing.T) {
	soon, later := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	never, expiresLater, expiresSoon := uuid.New(), uuid.New(), uuid.New()
	lines := ApplyCredits(10_000, []Credit{
		{ID: never, RemainingCents: 50_000, Description: "goodwill"},
		{ID: expiresLater, RemainingCents: 4_000, ExpiresAt: &later},
		{ID: expiresSoon, RemainingCents: 3_000, ExpiresAt: &soon},
	})
	if len(lines) != 3 {
		t.Fatalf("lines = %+v", lines)
	}
	for i, want := range []struct {
		id     uuid.UUID
		amount int64
	}{{expiresSoon, -3_000}, {expiresLater, -4_000}, {never, -3_000}} {
		if *lines[i].CreditID != want.id || lines[i].AmountCents != want.amount || lines[i].Kind != KindCredit {
			t.Errorf("line %d = %+v, want %v %d", i, lines[i], want.id, want.amount)
		}
	}
	if lines[2].Description != "Credit: goodwill" {
		t.Errorf("description = %q", lines[2].Description)
	}
	if ApplyCredits(0, []Credit{{ID: never, RemainingCents: 5}}) != nil {
		t.Error("credit applied to a zero subtotal")
	}
	if got := ApplyCredits(100, []Credit{{ID: never, RemainingCents: 0}}); got != nil {
		t.Errorf("exhausted credit applied: %+v", got)
	}
}

func TestTotals(t *testing.T) {
	id := uuid.New()
	subtotal, credits, total, err := Totals([]Line{{AmountCents: 500}, {AmountCents: 250}, {Kind: KindCredit, AmountCents: -300, CreditID: &id}})
	if err != nil || subtotal != 750 || credits != 300 || total != 450 {
		t.Fatalf("Totals = %d %d %d %v", subtotal, credits, total, err)
	}
	if _, _, _, err := Totals([]Line{{AmountCents: 1}, {Kind: KindCredit, AmountCents: -2}}); err == nil {
		t.Fatal("negative total accepted")
	}
}

func TestParsePriceTable(t *testing.T) {
	got, err := ParsePriceTable([]byte(`{"base_fee_cents":100,"session_cents":5,"included_sessions":2}`))
	if err != nil || got.BaseFeeCents != 100 || got.SessionCents != 5 || got.IncludedSessions != 2 {
		t.Fatalf("ParsePriceTable = %+v, %v", got, err)
	}
	for name, raw := range map[string]string{
		"unknown field": `{"base_fee_cents":100,"seat_cents":5}`,
		"negative":      `{"run_cents":-1}`,
		"not json":      `nope`,
		"wrong type":    `{"base_fee_cents":"100"}`,
	} {
		if _, err := ParsePriceTable([]byte(raw)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestPeriods(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 5, 0, 0, time.FixedZone("x", 2*3600)) // still Feb 28 in UTC
	if got := PreviousPeriod(now); !got.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("PreviousPeriod = %v", got)
	}
	if got := PreviousPeriod(time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)); !got.Equal(time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("PreviousPeriod across years = %v", got)
	}
	if got, err := ParsePeriod("2026-09"); err != nil || !got.Equal(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("ParsePeriod = %v, %v", got, err)
	}
	if _, err := ParsePeriod("2026-13"); err == nil {
		t.Error("month 13 accepted")
	}
}
