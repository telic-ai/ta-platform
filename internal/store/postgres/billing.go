package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/billing"
)

// ErrInvoiceNotDraft means an invoice could not be issued because it is not
// a draft (or does not exist).
var ErrInvoiceNotDraft = errors.New("invoice is not a draft")

// BillingStore implements billing.Store.
type BillingStore struct{ pool *pgxpool.Pool }

func NewBillingStore(pool *pgxpool.Pool) *BillingStore { return &BillingStore{pool: pool} }

func (s *BillingStore) Companies(ctx context.Context) ([]billing.Company, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.id, p.id, p.currency, p.price_table
		  FROM companies c JOIN plans p ON p.id = c.plan_id
		 ORDER BY c.id`)
	if err != nil {
		return nil, fmt.Errorf("list billable companies: %w", err)
	}
	defer rows.Close()
	var companies []billing.Company
	for rows.Next() {
		var company billing.Company
		if err := rows.Scan(&company.CompanyID, &company.PlanID, &company.Currency, &company.PriceTable); err != nil {
			return nil, fmt.Errorf("scan billable company: %w", err)
		}
		companies = append(companies, company)
	}
	return companies, rows.Err()
}

func (s *BillingStore) WriteDraft(ctx context.Context, company billing.Company, period time.Time, compute func([]billing.Credit) ([]billing.Line, error)) (billing.DraftOutcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin invoice: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Credits are shared by all of a company's invoices; locking them
	// serializes concurrent closes of different periods so neither can
	// spend what the other just applied.
	if _, err := tx.Exec(ctx, `SELECT 1 FROM billing_credits WHERE company_id = $1 FOR UPDATE`, company.CompanyID); err != nil {
		return "", fmt.Errorf("lock credits: %w", err)
	}

	outcome := billing.DraftRecomputed
	tag, err := tx.Exec(ctx, `
		INSERT INTO invoices (company_id, id, period, plan_id, currency)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (company_id, period) DO NOTHING`,
		company.CompanyID, uuid.New(), period, company.PlanID, company.Currency)
	if err != nil {
		return "", fmt.Errorf("create invoice: %w", err)
	}
	if tag.RowsAffected() == 1 {
		outcome = billing.DraftCreated
	}
	var invoiceID uuid.UUID
	var status string
	if err := tx.QueryRow(ctx, `
		SELECT id, status FROM invoices WHERE company_id = $1 AND period = $2 FOR UPDATE`,
		company.CompanyID, period).Scan(&invoiceID, &status); err != nil {
		return "", fmt.Errorf("lock invoice: %w", err)
	}
	if status != "draft" {
		return billing.DraftFinal, nil
	}

	credits, err := availableCredits(ctx, tx, company.CompanyID, invoiceID, period)
	if err != nil {
		return "", err
	}
	lines, err := compute(credits)
	if err != nil {
		return "", err
	}
	subtotal, creditTotal, total, err := billing.Totals(lines)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM invoice_lines WHERE company_id = $1 AND invoice_id = $2`, company.CompanyID, invoiceID); err != nil {
		return "", fmt.Errorf("clear draft lines: %w", err)
	}
	for i, line := range lines {
		if _, err := tx.Exec(ctx, `
			INSERT INTO invoice_lines (company_id, invoice_id, line_no, kind, description, quantity, amount_cents, credit_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			company.CompanyID, invoiceID, i+1, line.Kind, line.Description, line.Quantity, line.AmountCents, line.CreditID); err != nil {
			return "", fmt.Errorf("write invoice line %d: %w", i+1, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE invoices
		   SET plan_id = $3, currency = $4, subtotal_cents = $5, credits_cents = $6, total_cents = $7, computed_at = now()
		 WHERE company_id = $1 AND id = $2`,
		company.CompanyID, invoiceID, company.PlanID, company.Currency, subtotal, creditTotal, total); err != nil {
		return "", fmt.Errorf("update invoice totals: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit invoice: %w", err)
	}
	return outcome, nil
}

// availableCredits returns the credits valid for period with their balance
// net of every other non-void invoice's credit lines.
func availableCredits(ctx context.Context, tx pgx.Tx, companyID, invoiceID uuid.UUID, period time.Time) ([]billing.Credit, error) {
	rows, err := tx.Query(ctx, `
		SELECT c.id, c.description, c.granted_at, c.expires_at,
		       c.amount_cents + coalesce((
		           SELECT sum(l.amount_cents)
		             FROM invoice_lines l
		             JOIN invoices i ON i.company_id = l.company_id AND i.id = l.invoice_id
		            WHERE l.company_id = c.company_id AND l.credit_id = c.id
		              AND i.id <> $2 AND i.status <> 'void'), 0) AS remaining
		  FROM billing_credits c
		 WHERE c.company_id = $1
		   AND c.granted_at < ($3::date + interval '1 month')
		   AND (c.expires_at IS NULL OR c.expires_at > $3::date)`,
		companyID, invoiceID, period)
	if err != nil {
		return nil, fmt.Errorf("query credits: %w", err)
	}
	defer rows.Close()
	var credits []billing.Credit
	for rows.Next() {
		var credit billing.Credit
		if err := rows.Scan(&credit.ID, &credit.Description, &credit.GrantedAt, &credit.ExpiresAt, &credit.RemainingCents); err != nil {
			return nil, fmt.Errorf("scan credit: %w", err)
		}
		if credit.RemainingCents > 0 {
			credits = append(credits, credit)
		}
	}
	return credits, rows.Err()
}

// IssueInvoice finalizes a draft. The billing-close job never changes it
// afterwards.
func (s *BillingStore) IssueInvoice(ctx context.Context, companyID uuid.UUID, period time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE invoices SET status = 'issued', issued_at = now()
		 WHERE company_id = $1 AND period = $2 AND status = 'draft'`, companyID, period)
	if err != nil {
		return fmt.Errorf("issue invoice: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrInvoiceNotDraft
	}
	return nil
}
