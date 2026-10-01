package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/housekeeper"
)

// ErasedPlaceholder replaces a purged interview's personal fields, which are
// NOT NULL and non-empty.
const ErasedPlaceholder = "[erased]"

// HousekeeperStore implements housekeeper.Store.
type HousekeeperStore struct{ pool *pgxpool.Pool }

func NewHousekeeperStore(pool *pgxpool.Pool) *HousekeeperStore { return &HousekeeperStore{pool: pool} }

// purgeEligible is true for an interview the Housekeeper may purge given
// the retention cutoff in $1. legal_hold always wins.
const purgeEligible = `purged_at IS NULL AND legal_hold = false
   AND (erase_requested_at IS NOT NULL OR terminal_at < $1)`

func (s *HousekeeperStore) Candidates(ctx context.Context, cutoff time.Time, limit int) ([]housekeeper.Candidate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT company_id, id,
		       CASE WHEN erase_requested_at IS NOT NULL THEN 'erasure' ELSE 'retention' END
		  FROM interviews
		 WHERE `+purgeEligible+`
		 ORDER BY coalesce(erase_requested_at, terminal_at), company_id, id
		 LIMIT $2`, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("list purge candidates: %w", err)
	}
	defer rows.Close()
	var candidates []housekeeper.Candidate
	for rows.Next() {
		var c housekeeper.Candidate
		if err := rows.Scan(&c.CompanyID, &c.InterviewID, &c.Reason); err != nil {
			return nil, fmt.Errorf("scan purge candidate: %w", err)
		}
		candidates = append(candidates, c)
	}
	return candidates, rows.Err()
}

// Purge holds the interview's row lock across the external deletes, so a
// concurrent legal hold, sequence allocation (new event or score) or second
// Housekeeper waits for the purge to finish or roll back.
func (s *HousekeeperStore) Purge(ctx context.Context, candidate housekeeper.Candidate, cutoff time.Time, deleteExternal func(context.Context) (housekeeper.Stats, error)) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin purge: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var reason string
	err = tx.QueryRow(ctx, `
		SELECT CASE WHEN erase_requested_at IS NOT NULL THEN 'erasure' ELSE 'retention' END
		  FROM interviews
		 WHERE company_id = $2 AND id = $3 AND `+purgeEligible+`
		   FOR UPDATE`, cutoff, candidate.CompanyID, candidate.InterviewID).Scan(&reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock purge candidate: %w", err)
	}

	stats, err := deleteExternal(ctx)
	if err != nil {
		return false, err
	}

	statements := []struct {
		name string
		sql  string
	}{
		// The skeleton keeps ids, status, lifecycle timestamps, the hold and
		// erasure flags, and the sequence counter.
		{"skeletonize interview", `
			UPDATE interviews
			   SET candidate_name = '` + ErasedPlaceholder + `', candidate_email = '` + ErasedPlaceholder + `',
			       terminal_at = coalesce(terminal_at, now()), purged_at = now(), updated_at = now()
			 WHERE company_id = $1 AND id = $2`},
		{"delete scores", `DELETE FROM scores WHERE company_id = $1 AND interview_id = $2`},
		{"delete sessions", `DELETE FROM sessions WHERE company_id = $1 AND interview_id = $2`},
		{"delete invites", `DELETE FROM invites WHERE company_id = $1 AND interview_id = $2`},
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement.sql, candidate.CompanyID, candidate.InterviewID); err != nil {
			return false, fmt.Errorf("%s: %w", statement.name, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO purge_log (company_id, interview_id, reason, search_documents_deleted, purged_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (company_id, interview_id) DO NOTHING`,
		candidate.CompanyID, candidate.InterviewID, reason, stats.SearchDocumentsDeleted); err != nil {
		return false, fmt.Errorf("write purge log: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit purge: %w", err)
	}
	return true, nil
}
