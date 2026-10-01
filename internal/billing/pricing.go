// Package billing closes a month: it reads each company's usage from the
// ClickHouse v_billing_usage view (fed by mv_billing_usage), prices it with
// the company's plan, applies available credits and writes a draft invoice.
//
// Closing is idempotent per (company_id, period): re-running it recomputes
// the draft from current usage and credits, and never changes an invoice
// that has been issued.
//
// NOTE: [[Billing]] was not available when this package was written; the
// price table fields and rounding are reasonable placeholders to reconcile
// against it.
package billing

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
)

// Usage is one company's metered usage for a period.
type Usage struct {
	Sessions            int64
	ManagedInputTokens  int64
	ManagedOutputTokens int64
	// BYOK tokens are paid to the provider by the company; they are
	// reported but not priced.
	BYOKInputTokens  int64
	BYOKOutputTokens int64
	Runs             int64
	RunDurationMS    int64
}

// PriceTable is a plan's price_table. All prices are integer cents.
type PriceTable struct {
	BaseFeeCents     int64 `json:"base_fee_cents"`
	IncludedSessions int64 `json:"included_sessions"`
	SessionCents     int64 `json:"session_cents"`
	// Token prices are per million tokens.
	ManagedInputPerMillionCents  int64 `json:"managed_input_per_million_cents"`
	ManagedOutputPerMillionCents int64 `json:"managed_output_per_million_cents"`
	IncludedRuns                 int64 `json:"included_runs"`
	RunCents                     int64 `json:"run_cents"`
}

// ParsePriceTable decodes a plan's price_table strictly: unknown fields and
// negative prices are rejected rather than silently billed at zero.
func ParsePriceTable(raw []byte) (PriceTable, error) {
	var table PriceTable
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&table); err != nil {
		return PriceTable{}, fmt.Errorf("billing: price table: %w", err)
	}
	for name, value := range map[string]int64{
		"base_fee_cents": table.BaseFeeCents, "included_sessions": table.IncludedSessions,
		"session_cents": table.SessionCents, "managed_input_per_million_cents": table.ManagedInputPerMillionCents,
		"managed_output_per_million_cents": table.ManagedOutputPerMillionCents,
		"included_runs":                    table.IncludedRuns, "run_cents": table.RunCents,
	} {
		if value < 0 {
			return PriceTable{}, fmt.Errorf("billing: price table %s must not be negative", name)
		}
	}
	return table, nil
}

// Line kinds, as stored in invoice_lines.kind.
const (
	KindBaseFee             = "base_fee"
	KindSessions            = "sessions"
	KindManagedInputTokens  = "managed_input_tokens"
	KindManagedOutputTokens = "managed_output_tokens"
	KindRuns                = "runs"
	KindCredit              = "credit"
)

// Line is one invoice line. Credit lines are negative and name their credit.
type Line struct {
	Kind        string
	Description string
	Quantity    int64
	AmountCents int64
	CreditID    *uuid.UUID
}

// Price turns usage into charge lines. Lines with a zero amount are omitted,
// except the base fee, which always appears so every invoice shows its plan.
// Per-million token charges round up to the next whole cent.
func Price(usage Usage, table PriceTable) []Line {
	lines := []Line{{Kind: KindBaseFee, Description: "Monthly platform fee", Quantity: 1, AmountCents: table.BaseFeeCents}}
	if billable := max(usage.Sessions-table.IncludedSessions, 0); billable > 0 && table.SessionCents > 0 {
		lines = append(lines, Line{Kind: KindSessions,
			Description: fmt.Sprintf("Candidate sessions beyond %d included", table.IncludedSessions),
			Quantity:    billable, AmountCents: billable * table.SessionCents})
	}
	if cents := perMillionCeil(usage.ManagedInputTokens, table.ManagedInputPerMillionCents); cents > 0 {
		lines = append(lines, Line{Kind: KindManagedInputTokens, Description: "Managed AI input tokens",
			Quantity: usage.ManagedInputTokens, AmountCents: cents})
	}
	if cents := perMillionCeil(usage.ManagedOutputTokens, table.ManagedOutputPerMillionCents); cents > 0 {
		lines = append(lines, Line{Kind: KindManagedOutputTokens, Description: "Managed AI output tokens",
			Quantity: usage.ManagedOutputTokens, AmountCents: cents})
	}
	if billable := max(usage.Runs-table.IncludedRuns, 0); billable > 0 && table.RunCents > 0 {
		lines = append(lines, Line{Kind: KindRuns,
			Description: fmt.Sprintf("Code runs beyond %d included", table.IncludedRuns),
			Quantity:    billable, AmountCents: billable * table.RunCents})
	}
	return lines
}

// perMillionCeil is ceil(quantity * centsPerMillion / 1e6) without overflow.
func perMillionCeil(quantity, centsPerMillion int64) int64 {
	if quantity <= 0 || centsPerMillion <= 0 {
		return 0
	}
	product := new(big.Int).Mul(big.NewInt(quantity), big.NewInt(centsPerMillion))
	million := big.NewInt(1_000_000)
	quotient, remainder := new(big.Int).QuoRem(product, million, new(big.Int))
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient.Int64()
}

// Credit is a credit with the balance still available to this invoice.
type Credit struct {
	ID             uuid.UUID
	Description    string
	RemainingCents int64
	GrantedAt      time.Time
	ExpiresAt      *time.Time
}

// ApplyCredits returns negative credit lines covering as much of subtotal as
// the credits allow, spending the soonest-expiring credit first (never
// expiring last, then oldest grant). The total never goes below zero.
func ApplyCredits(subtotalCents int64, credits []Credit) []Line {
	ordered := make([]Credit, len(credits))
	copy(ordered, credits)
	sortCredits(ordered)
	var lines []Line
	due := subtotalCents
	for _, credit := range ordered {
		if due <= 0 {
			break
		}
		applied := min(credit.RemainingCents, due)
		if applied <= 0 {
			continue
		}
		id := credit.ID
		description := "Credit"
		if credit.Description != "" {
			description = "Credit: " + credit.Description
		}
		lines = append(lines, Line{Kind: KindCredit, Description: description, Quantity: 1, AmountCents: -applied, CreditID: &id})
		due -= applied
	}
	return lines
}

// Totals sums an invoice's lines.
func Totals(lines []Line) (subtotal, credits, total int64, err error) {
	for _, line := range lines {
		if line.Kind == KindCredit {
			credits -= line.AmountCents
		} else {
			subtotal += line.AmountCents
		}
	}
	total = subtotal - credits
	if total < 0 {
		return 0, 0, 0, errors.New("billing: credits exceed the subtotal")
	}
	return subtotal, credits, total, nil
}

// PeriodStart is the first instant of the UTC calendar month containing t.
func PeriodStart(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// PreviousPeriod is the month before the one containing now: the period a
// close run at the start of a month bills.
func PreviousPeriod(now time.Time) time.Time {
	return PeriodStart(PeriodStart(now).AddDate(0, 0, -1))
}

// ParsePeriod parses "YYYY-MM".
func ParsePeriod(value string) (time.Time, error) {
	period, err := time.Parse("2006-01", value)
	if err != nil {
		return time.Time{}, fmt.Errorf("billing: period must be YYYY-MM: %w", err)
	}
	return period, nil
}
