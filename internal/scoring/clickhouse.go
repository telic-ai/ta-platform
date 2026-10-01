package scoring

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics"
)

// ClickHouseLog reads the events table written by the event-log-writer.
type ClickHouseLog struct{ conn driver.Conn }

func NewClickHouseLog(conn driver.Conn) *ClickHouseLog { return &ClickHouseLog{conn: conn} }

const progressQuery = `
SELECT max(sequence_number), uniqExactIf(sequence_number, sequence_number BETWEEN 1 AND ?)
  FROM events
 WHERE company_id = ? AND interview_id = ?`

// Progress needs no FINAL: duplicate rows share a sequence number, which
// neither max nor a distinct count can over-count.
func (l *ClickHouseLog) Progress(ctx context.Context, companyID, interviewID uuid.UUID, upTo int64) (Progress, error) {
	var maxSequence int64
	var present uint64
	if err := l.conn.QueryRow(ctx, progressQuery, upTo, companyID.String(), interviewID.String()).Scan(&maxSequence, &present); err != nil {
		return Progress{}, fmt.Errorf("query event log progress: %w", err)
	}
	return Progress{MaxSequence: maxSequence, Present: int64(present)}, nil
}

const sessionTimelineQuery = `
SELECT sequence_number, event_type, occurred_at, payload
  FROM events FINAL
 WHERE company_id = ? AND interview_id = ? AND session_id = ? AND sequence_number <= ?
 ORDER BY sequence_number`

func (l *ClickHouseLog) SessionTimeline(ctx context.Context, companyID, interviewID, sessionID uuid.UUID, upTo int64) ([]metrics.Event, error) {
	rows, err := l.conn.Query(ctx, sessionTimelineQuery, companyID.String(), interviewID.String(), sessionID.String(), upTo)
	if err != nil {
		return nil, fmt.Errorf("query session timeline: %w", err)
	}
	defer rows.Close()
	var timeline []metrics.Event
	for rows.Next() {
		var (
			event     metrics.Event
			eventType string
			at        time.Time
			payload   string
		)
		if err := rows.Scan(&event.SequenceNumber, &eventType, &at, &payload); err != nil {
			return nil, fmt.Errorf("scan session timeline: %w", err)
		}
		event.EventType, event.OccurredAt, event.Payload = events.EventType(eventType), at, json.RawMessage(payload)
		timeline = append(timeline, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate session timeline: %w", err)
	}
	return timeline, nil
}
