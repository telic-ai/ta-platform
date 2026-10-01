// Package dashboard serves the company app's dashboards from ClickHouse
// materialized views over the event log, and interview replay timelines,
// as Admin API routes. Every query is scoped by the member's company.
package dashboard

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Materialized views over the events table (see internal/eventlogwriter).
// They count what they are fed: the events table deduplicates Kafka
// redeliveries at merge time, but a view sees every insert, so counts are
// at-least-once.
var viewDDL = []string{
	`CREATE TABLE IF NOT EXISTS company_daily_events
(
    company_id String,
    day Date,
    event_type LowCardinality(String),
    events UInt64
)
ENGINE = SummingMergeTree
ORDER BY (company_id, day, event_type)`,
	`CREATE MATERIALIZED VIEW IF NOT EXISTS company_daily_events_mv TO company_daily_events AS
SELECT company_id, toDate(occurred_at) AS day, event_type, count() AS events
  FROM events
 GROUP BY company_id, day, event_type`,
	// detail splits an event type by its outcome: a diff's origin, a run's
	// or completion's status.
	`CREATE TABLE IF NOT EXISTS interview_activity
(
    company_id String,
    interview_id String,
    event_type LowCardinality(String),
    detail LowCardinality(String),
    events UInt64,
    lines_added Int64,
    lines_removed Int64,
    first_at SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_at SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = SummingMergeTree
ORDER BY (company_id, interview_id, event_type, detail)`,
	`CREATE MATERIALIZED VIEW IF NOT EXISTS interview_activity_mv TO interview_activity AS
SELECT company_id, interview_id, event_type,
       multiIf(event_type = 'code.diff', JSONExtractString(payload, 'origin'),
               event_type IN ('execution.completed', 'ai.response.completed'), JSONExtractString(payload, 'status'),
               '') AS detail,
       count() AS events,
       sum(JSONExtractInt(payload, 'lines_added')) AS lines_added,
       sum(JSONExtractInt(payload, 'lines_removed')) AS lines_removed,
       min(occurred_at) AS first_at,
       max(occurred_at) AS last_at
  FROM events
 GROUP BY company_id, interview_id, event_type, detail`,
}

const (
	dailyQuery = `
SELECT day, event_type, sum(events)
  FROM company_daily_events
 WHERE company_id = ? AND day >= ?
 GROUP BY day, event_type
 ORDER BY day, event_type`
	activityQuery = `
SELECT interview_id, event_type, detail, sum(events), sum(lines_added), sum(lines_removed), min(first_at), max(last_at)
  FROM interview_activity
 WHERE company_id = ?
 GROUP BY interview_id, event_type, detail`
)

// ClickHouseStore reads the dashboard views.
type ClickHouseStore struct{ conn driver.Conn }

func NewClickHouseStore(conn driver.Conn) *ClickHouseStore { return &ClickHouseStore{conn: conn} }

// EnsureViews creates the views' target tables and the views. Run it after
// the events table exists. Events logged before the views existed are not
// counted.
func (s *ClickHouseStore) EnsureViews(ctx context.Context) error {
	for _, ddl := range viewDDL {
		if err := s.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("dashboard: ensure views: %w", err)
		}
	}
	return nil
}

// DailyEvents implements Store.
func (s *ClickHouseStore) DailyEvents(ctx context.Context, companyID uuid.UUID, since time.Time) ([]DailyCount, error) {
	rows, err := s.conn.Query(ctx, dailyQuery, companyID.String(), since)
	if err != nil {
		return nil, fmt.Errorf("dashboard: daily events: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []DailyCount
	for rows.Next() {
		var c DailyCount
		var day time.Time
		if err := rows.Scan(&day, &c.EventType, &c.Events); err != nil {
			return nil, fmt.Errorf("dashboard: scan daily events: %w", err)
		}
		c.Day = day.Format(time.DateOnly)
		out = append(out, c)
	}
	return out, rows.Err()
}

// ActivityRows implements Store.
func (s *ClickHouseStore) ActivityRows(ctx context.Context, companyID uuid.UUID) ([]ActivityRow, error) {
	rows, err := s.conn.Query(ctx, activityQuery, companyID.String())
	if err != nil {
		return nil, fmt.Errorf("dashboard: interview activity: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []ActivityRow
	for rows.Next() {
		var r ActivityRow
		if err := rows.Scan(&r.InterviewID, &r.EventType, &r.Detail, &r.Events, &r.LinesAdded, &r.LinesRemoved, &r.FirstAt, &r.LastAt); err != nil {
			return nil, fmt.Errorf("dashboard: scan interview activity: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
