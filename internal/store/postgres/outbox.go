package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/outbox"
)

// outboxLockID serializes relays across processes so rows go out in id order.
const outboxLockID int64 = 0x54414f5554424f58 // "TAOUTBOX"

// OutboxStore is the Postgres side of the transactional outbox.
type OutboxStore struct{ pool *pgxpool.Pool }

func NewOutboxStore(pool *pgxpool.Pool) *OutboxStore { return &OutboxStore{pool: pool} }

// Drain implements outbox.Source. It holds a transaction-scoped advisory
// lock for the whole relay, so a second relay skips its turn instead of
// publishing rows out of order. Rows are deleted only after publish returns.
func (s *OutboxStore) Drain(ctx context.Context, limit int, publish func(context.Context, []outbox.Message) error) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin outbox relay: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, outboxLockID).Scan(&locked); err != nil {
		return 0, fmt.Errorf("lock outbox: %w", err)
	}
	if !locked {
		return 0, nil
	}

	rows, err := tx.Query(ctx, `
		SELECT id, company_id, topic, message_key, event_type, envelope
		  FROM event_outbox
		 ORDER BY id
		 LIMIT $1`, limit)
	if err != nil {
		return 0, fmt.Errorf("read outbox: %w", err)
	}
	messages, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outbox.Message, error) {
		var message outbox.Message
		err := row.Scan(&message.ID, &message.CompanyID, &message.Topic, &message.Key, &message.EventType, &message.Envelope)
		return message, err
	})
	if err != nil {
		return 0, fmt.Errorf("scan outbox: %w", err)
	}
	if len(messages) == 0 {
		return 0, nil
	}
	if err := publish(ctx, messages); err != nil {
		return 0, err
	}

	ids := make([]int64, len(messages))
	for i, message := range messages {
		ids[i] = message.ID
	}
	if _, err := tx.Exec(ctx, `DELETE FROM event_outbox WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("delete relayed outbox rows: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit outbox relay: %w", err)
	}
	return len(messages), nil
}

// insertOutbox records message in tx, so it commits or rolls back with the
// state change it describes.
func insertOutbox(ctx context.Context, tx pgx.Tx, message outbox.Message) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO event_outbox (company_id, topic, message_key, event_type, envelope)
		VALUES ($1, $2, $3, $4, $5)`,
		message.CompanyID, message.Topic, message.Key, string(message.EventType), message.Envelope)
	if err != nil {
		return fmt.Errorf("write outbox: %w", err)
	}
	return nil
}
