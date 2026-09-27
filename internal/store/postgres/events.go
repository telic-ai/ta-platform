package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/outbox"
)

// ErrInterviewNotFound means the interview does not exist for the company
// or has been purged.
var ErrInterviewNotFound = errors.New("interview not found")

// EventStore allocates per-interview sequence numbers and records events
// in the outbox.
type EventStore struct{ pool *pgxpool.Pool }

func NewEventStore(pool *pgxpool.Pool) *EventStore { return &EventStore{pool: pool} }

// RecordEvents implements candidateworkspace.EventStore.
func (s *EventStore) RecordEvents(ctx context.Context, companyID, interviewID uuid.UUID, count int, build func(first int64) ([]outbox.Message, error)) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin record events: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	first, err := allocateSequence(ctx, tx, companyID, interviewID, count)
	if err != nil {
		return 0, err
	}
	messages, err := build(first)
	if err != nil {
		return 0, err
	}
	for _, message := range messages {
		if err := insertOutbox(ctx, tx, message); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit record events: %w", err)
	}
	return first, nil
}

// allocateSequence reserves count consecutive sequence numbers for the
// interview and returns the first. The row lock taken by the increment
// serializes allocation per interview until tx ends.
func allocateSequence(ctx context.Context, tx pgx.Tx, companyID, interviewID uuid.UUID, count int) (int64, error) {
	if count < 1 {
		return 0, errors.New("allocate sequence: count must be positive")
	}
	var last int64
	err := tx.QueryRow(ctx, `
		UPDATE interviews
		   SET last_sequence_number = last_sequence_number + $3, updated_at = now()
		 WHERE company_id = $1 AND id = $2 AND purged_at IS NULL
		 RETURNING last_sequence_number`, companyID, interviewID, count).Scan(&last)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrInterviewNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("allocate interview sequence: %w", err)
	}
	return last - int64(count) + 1, nil
}
