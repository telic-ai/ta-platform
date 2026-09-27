// Package replay serves ordered, tenant-scoped interview timelines.
package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Event is the durable event representation returned by the replay API.
type Event struct {
	EventID        string          `json:"event_id"`
	CompanyID      string          `json:"company_id"`
	InterviewID    string          `json:"interview_id"`
	SequenceNumber int64           `json:"sequence_number"`
	EventType      string          `json:"event_type"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
}

// Store makes tenant identity a mandatory part of every timeline lookup.
type Store interface {
	Timeline(context.Context, uuid.UUID, uuid.UUID, int64) ([]Event, error)
}

type ClickHouseStore struct {
	conn driver.Conn
}

func NewClickHouseStore(conn driver.Conn) *ClickHouseStore {
	return &ClickHouseStore{conn: conn}
}

const timelineQuery = `
SELECT event_id, company_id, interview_id, sequence_number,
       event_type, occurred_at, payload
  FROM events FINAL
 WHERE company_id = ?
   AND interview_id = ?
   AND sequence_number > ?
 ORDER BY sequence_number ASC`

// Timeline reads afterSeq exclusively. Filtering by company_id in the same
// query prevents a caller from using a known interview UUID across tenants.
func (s *ClickHouseStore) Timeline(ctx context.Context, companyID, interviewID uuid.UUID, afterSeq int64) ([]Event, error) {
	rows, err := s.conn.Query(ctx, timelineQuery, companyID.String(), interviewID.String(), afterSeq)
	if err != nil {
		return nil, fmt.Errorf("query interview timeline: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		var event Event
		var payload string
		if err := rows.Scan(
			&event.EventID,
			&event.CompanyID,
			&event.InterviewID,
			&event.SequenceNumber,
			&event.EventType,
			&event.OccurredAt,
			&payload,
		); err != nil {
			return nil, fmt.Errorf("scan interview timeline: %w", err)
		}
		event.Payload = json.RawMessage(payload)
		if !json.Valid(event.Payload) {
			return nil, fmt.Errorf("event %q contains invalid JSON payload", event.EventID)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate interview timeline: %w", err)
	}
	return events, nil
}
