package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/candidateworkspace"
	"github.com/telic-ai/ta-platform/internal/outbox"
)

// RecordDiff implements candidateworkspace.DiffStore.
func (s *EventStore) RecordDiff(ctx context.Context, companyID, sessionID, interviewID uuid.UUID, clientSequence int64,
	build func(sequence int64) (outbox.Message, error)) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin record diff: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// The row lock serializes concurrent diffs for the session; the guard
	// drops any diff not newer than the last accepted one.
	tag, err := tx.Exec(ctx, `
		UPDATE sessions
		   SET last_diff_sequence = $3
		 WHERE company_id = $1 AND id = $2 AND interview_id = $4 AND last_diff_sequence < $3`,
		companyID, sessionID, clientSequence, interviewID)
	if err != nil {
		return 0, fmt.Errorf("advance diff sequence: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return 0, candidateworkspace.ErrDiffOutOfOrder
	}
	sequence, err := allocateSequence(ctx, tx, companyID, interviewID, 1)
	if err != nil {
		return 0, err
	}
	message, err := build(sequence)
	if err != nil {
		return 0, err
	}
	if err := insertOutbox(ctx, tx, message); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit record diff: %w", err)
	}
	return sequence, nil
}
