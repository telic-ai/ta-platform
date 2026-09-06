package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/domain"
)

// InterviewStore requires tenant identity on every operation. Its SQL always
// scopes by company_id, including lookups by a globally shaped UUID.
type InterviewStore struct {
	pool *pgxpool.Pool
}

// NewInterviewStore returns an interview store backed by pool.
func NewInterviewStore(pool *pgxpool.Pool) *InterviewStore {
	return &InterviewStore{pool: pool}
}

// Create inserts an interview in its company partition.
func (s *InterviewStore) Create(ctx context.Context, interview domain.Interview) error {
	_, err := s.pool.Exec(ctx, `
        INSERT INTO interviews (
            company_id, id, created_by, candidate_name, candidate_email,
            status, scheduled_at, terminal_at, erase_requested_at, legal_hold,
            purged_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		interview.CompanyID, interview.ID, interview.CreatedBy,
		interview.CandidateName, interview.CandidateEmail, interview.Status,
		interview.ScheduledAt, interview.TerminalAt, interview.EraseRequestedAt,
		interview.LegalHold, interview.PurgedAt)
	if err != nil {
		return fmt.Errorf("create interview: %w", err)
	}
	return nil
}

// Get returns an interview only when both its company and interview IDs match.
func (s *InterviewStore) Get(ctx context.Context, companyID, interviewID uuid.UUID) (domain.Interview, error) {
	var interview domain.Interview
	err := s.pool.QueryRow(ctx, `
        SELECT id, company_id, created_by, candidate_name, candidate_email,
               status, scheduled_at, terminal_at, erase_requested_at,
               legal_hold, purged_at, created_at, updated_at
          FROM interviews
         WHERE company_id = $1 AND id = $2`, companyID, interviewID).Scan(
		&interview.ID, &interview.CompanyID, &interview.CreatedBy,
		&interview.CandidateName, &interview.CandidateEmail, &interview.Status,
		&interview.ScheduledAt, &interview.TerminalAt,
		&interview.EraseRequestedAt, &interview.LegalHold, &interview.PurgedAt,
		&interview.CreatedAt, &interview.UpdatedAt,
	)
	if err != nil {
		return domain.Interview{}, fmt.Errorf("get interview: %w", err)
	}
	return interview, nil
}
