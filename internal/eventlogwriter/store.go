package eventlogwriter

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const SchemaDDL = `CREATE TABLE IF NOT EXISTS events
(
    event_id String,
    company_id String,
    interview_id String,
    sequence_number Int64,
    event_type LowCardinality(String),
    schema_version UInt16,
    occurred_at DateTime64(3, 'UTC'),
    payload String,
    candidate_id Nullable(String),
    job_id Nullable(String),
    application_id Nullable(String),
    session_id Nullable(String),
    user_id Nullable(String),
    outcome Nullable(String),
    ingested_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (company_id, interview_id, sequence_number)`

type Store struct{ conn driver.Conn }

func NewStore(conn driver.Conn) (*Store, error) {
	if conn == nil {
		return nil, fmt.Errorf("event-log-writer: ClickHouse connection is required")
	}
	return &Store{conn: conn}, nil
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	if err := s.conn.Exec(ctx, SchemaDDL); err != nil {
		return fmt.Errorf("event-log-writer: ensure events table: %w", err)
	}
	return nil
}

func (s *Store) Insert(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO events (
event_id, company_id, interview_id, sequence_number, event_type, schema_version,
occurred_at, payload, candidate_id, job_id, application_id, session_id, user_id,
outcome, ingested_at)`)
	if err != nil {
		return fmt.Errorf("prepare ClickHouse batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.EventID, row.CompanyID, row.InterviewID, row.SequenceNumber, string(row.EventType), uint16(row.SchemaVersion),
			row.OccurredAt, row.Payload, row.CandidateID, row.JobID, row.ApplicationID, row.SessionID, row.UserID,
			row.Outcome, row.IngestedAt,
		); err != nil {
			return fmt.Errorf("append event %s: %w", row.EventID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send ClickHouse batch: %w", err)
	}
	return nil
}
