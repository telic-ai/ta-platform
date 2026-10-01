// Package analytics owns the ClickHouse materialized views derived from the
// event log (the events table written by the event-log-writer).
//
// A materialized view fires on every insert, while events deduplicates
// redelivered rows only at merge time. Summing in the view would therefore
// count a redelivered event twice. Instead each view either:
//
//   - projects one row per event into a ReplacingMergeTree keyed like events
//     (mv_session_metrics, mv_billing_usage), and a plain view aggregates it
//     with FINAL (v_session_metrics, v_billing_usage); or
//   - aggregates only idempotent states — min/max flags (mv_funnel) or exact
//     distinct counts of event keys (mv_task_daily).
//
// Every view is thus redelivery-safe, and re-running a backfill is a no-op.
//
// NOTE: [[ClickHouse]], [[Analytics Store]] and [[Billing]] were not
// available when this package was written. Events carry no task id yet, so
// mv_task_daily is per company and event type; add task_id to its key once
// the catalog carries it.
package analytics

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics"
)

// View is one materialized view: its target table, and the SELECT over
// events that both the view and a backfill insert into it.
type View struct {
	Name   string
	Target string
	Table  string
	Select string
}

func sqlList[T ~string](values ...T) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = "'" + strings.ReplaceAll(string(v), "'", "\\'") + "'"
	}
	return strings.Join(quoted, ", ")
}

var activityTypes = sqlList(metrics.ActivityTypes...)

// Views are created in order; each target table precedes its view.
var Views = []View{
	{
		Name:   "mv_session_metrics",
		Target: "session_metric_rows",
		// One row per activity event with its contribution to each
		// scoring metric. Must mirror internal/scoring/metrics.Compute.
		Table: `CREATE TABLE IF NOT EXISTS session_metric_rows
(
    company_id String,
    interview_id String,
    session_id String,
    sequence_number Int64,
    event_type LowCardinality(String),
    occurred_at DateTime64(3, 'UTC'),
    prompt UInt8,
    ai_response UInt8,
    ai_error UInt8,
    ai_input_tokens Int64,
    ai_output_tokens Int64,
    run_requested UInt8,
    run_completed UInt8,
    run_succeeded UInt8,
    run_status LowCardinality(String),
    diff UInt8,
    ai_diff UInt8,
    lines_added Int64,
    lines_removed Int64,
    ai_lines_added Int64,
    ingested_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (company_id, interview_id, sequence_number)`,
		Select: `SELECT
    events.company_id AS company_id,
    events.interview_id AS interview_id,
    assumeNotNull(events.session_id) AS session_id,
    events.sequence_number AS sequence_number,
    events.event_type AS event_type,
    events.occurred_at AS occurred_at,
    toUInt8(events.event_type = '` + string(events.EventTypePromptSubmitted) + `') AS prompt,
    toUInt8(events.event_type = '` + string(events.EventTypeAIResponseCompleted) + `') AS ai_response,
    toUInt8(ai_response AND JSONExtractString(events.payload, 'status') = '` + events.AIResponseStatusError + `') AS ai_error,
    if(ai_response = 1, JSONExtractInt(events.payload, 'input_tokens'), 0) AS ai_input_tokens,
    if(ai_response = 1, JSONExtractInt(events.payload, 'output_tokens'), 0) AS ai_output_tokens,
    toUInt8(events.event_type = '` + string(events.EventTypeExecutionRequested) + `') AS run_requested,
    toUInt8(events.event_type = '` + string(events.EventTypeExecutionCompleted) + `') AS run_completed,
    toUInt8(run_completed AND JSONExtractString(events.payload, 'status') = '` + events.ExecutionStatusSucceeded + `') AS run_succeeded,
    if(run_completed = 1, JSONExtractString(events.payload, 'status'), '') AS run_status,
    toUInt8(events.event_type = '` + string(events.EventTypeCodeDiff) + `') AS diff,
    toUInt8(diff AND JSONExtractString(events.payload, 'origin') = '` + events.DiffOriginAIApplied + `') AS ai_diff,
    if(diff = 1, JSONExtractInt(events.payload, 'lines_added'), 0) AS lines_added,
    if(diff = 1, JSONExtractInt(events.payload, 'lines_removed'), 0) AS lines_removed,
    if(ai_diff = 1, JSONExtractInt(events.payload, 'lines_added'), 0) AS ai_lines_added,
    events.ingested_at AS ingested_at
FROM events
WHERE events.event_type IN (` + activityTypes + `) AND events.session_id IS NOT NULL`,
	},
	{
		Name:   "mv_task_daily",
		Target: "task_daily",
		// Distinct events and interviews per company, day and event type.
		// uniqExact over the event key makes redelivery a no-op.
		Table: `CREATE TABLE IF NOT EXISTS task_daily
(
    company_id String,
    day Date,
    event_type LowCardinality(String),
    events AggregateFunction(uniqExact, String, Int64),
    interviews AggregateFunction(uniqExact, String)
)
ENGINE = AggregatingMergeTree
ORDER BY (company_id, day, event_type)`,
		Select: `SELECT
    events.company_id AS company_id,
    toDate(events.occurred_at) AS day,
    events.event_type AS event_type,
    uniqExactState(events.interview_id, events.sequence_number) AS events,
    uniqExactState(events.interview_id) AS interviews
FROM events
WHERE events.event_type IN (` + activityTypes + `)
GROUP BY company_id, day, event_type`,
	},
	{
		Name:   "mv_funnel",
		Target: "funnel_interviews",
		// Per-interview stage flags; max and min are idempotent.
		Table: `CREATE TABLE IF NOT EXISTS funnel_interviews
(
    company_id String,
    interview_id String,
    first_at SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    started SimpleAggregateFunction(max, UInt8),
    prompted SimpleAggregateFunction(max, UInt8),
    ran SimpleAggregateFunction(max, UInt8),
    run_succeeded SimpleAggregateFunction(max, UInt8),
    submitted SimpleAggregateFunction(max, UInt8),
    expired SimpleAggregateFunction(max, UInt8)
)
ENGINE = AggregatingMergeTree
ORDER BY (company_id, interview_id)`,
		Select: `SELECT
    events.company_id AS company_id,
    events.interview_id AS interview_id,
    events.occurred_at AS first_at,
    toUInt8(events.event_type = '` + string(events.EventTypeSessionStarted) + `') AS started,
    toUInt8(events.event_type = '` + string(events.EventTypePromptSubmitted) + `') AS prompted,
    toUInt8(events.event_type = '` + string(events.EventTypeExecutionRequested) + `') AS ran,
    toUInt8(events.event_type = '` + string(events.EventTypeExecutionCompleted) + `'
        AND JSONExtractString(events.payload, 'status') = '` + events.ExecutionStatusSucceeded + `') AS run_succeeded,
    toUInt8(events.event_type = '` + string(events.EventTypeSessionSubmitted) + `') AS submitted,
    toUInt8(events.event_type = '` + string(events.EventTypeSessionExpired) + `') AS expired
FROM events
WHERE events.event_type IN (` + activityTypes + `)`,
	},
	{
		Name:   "mv_billing_usage",
		Target: "billing_usage_rows",
		// One row per billable event, keyed like events.
		Table: `CREATE TABLE IF NOT EXISTS billing_usage_rows
(
    company_id String,
    period Date,
    interview_id String,
    sequence_number Int64,
    session_started UInt8,
    ai_mode LowCardinality(String),
    ai_input_tokens Int64,
    ai_output_tokens Int64,
    run_requested UInt8,
    run_duration_ms Int64,
    ingested_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (company_id, period, interview_id, sequence_number)`,
		Select: `SELECT
    events.company_id AS company_id,
    toStartOfMonth(events.occurred_at) AS period,
    events.interview_id AS interview_id,
    events.sequence_number AS sequence_number,
    toUInt8(events.event_type = '` + string(events.EventTypeSessionStarted) + `') AS session_started,
    if(events.event_type = '` + string(events.EventTypeAIResponseCompleted) + `', JSONExtractString(events.payload, 'mode'), '') AS ai_mode,
    if(ai_mode != '', JSONExtractInt(events.payload, 'input_tokens'), 0) AS ai_input_tokens,
    if(ai_mode != '', JSONExtractInt(events.payload, 'output_tokens'), 0) AS ai_output_tokens,
    toUInt8(events.event_type = '` + string(events.EventTypeExecutionRequested) + `') AS run_requested,
    if(events.event_type = '` + string(events.EventTypeExecutionCompleted) + `', JSONExtractInt(events.payload, 'duration_ms'), 0) AS run_duration_ms,
    events.ingested_at AS ingested_at
FROM events
WHERE events.event_type IN (` + sqlList(
			events.EventTypeSessionStarted, events.EventTypeAIResponseCompleted,
			events.EventTypeExecutionRequested, events.EventTypeExecutionCompleted) + `)`,
	},
}

// QueryViews are the deduplicated read models over the view targets.
var QueryViews = []string{
	// Column names and meaning match metrics.Metrics.
	`CREATE OR REPLACE VIEW v_session_metrics AS
SELECT
    company_id,
    interview_id,
    session_id,
    toInt64(sum(prompt)) AS prompt_count,
    toInt64(sum(ai_response)) AS ai_response_count,
    toInt64(sum(ai_error)) AS ai_error_count,
    sum(ai_input_tokens) AS ai_input_tokens,
    sum(ai_output_tokens) AS ai_output_tokens,
    toInt64(sum(run_completed)) AS run_count,
    toInt64(sum(run_succeeded)) AS run_succeeded_count,
    toString(argMaxIf(run_status, sequence_number, run_completed = 1)) AS last_run_status,
    toInt64(sum(diff)) AS diff_count,
    toInt64(sum(ai_diff)) AS ai_diff_count,
    sum(lines_added) AS lines_added,
    sum(lines_removed) AS lines_removed,
    sum(ai_lines_added) AS ai_lines_added,
    toUnixTimestamp64Milli(max(occurred_at)) - toUnixTimestamp64Milli(min(occurred_at)) AS active_duration_ms,
    if(countIf(run_requested = 1) > 0,
       toUnixTimestamp64Milli(minIf(occurred_at, run_requested = 1)) - toUnixTimestamp64Milli(min(occurred_at)),
       NULL) AS time_to_first_run_ms
FROM session_metric_rows FINAL
GROUP BY company_id, interview_id, session_id`,

	`CREATE OR REPLACE VIEW v_task_daily AS
SELECT
    company_id,
    day,
    toInt64(sumIf(event_count, event_type = '` + string(events.EventTypeSessionStarted) + `')) AS sessions_started,
    toInt64(sumIf(event_count, event_type = '` + string(events.EventTypeSessionSubmitted) + `')) AS sessions_submitted,
    toInt64(sumIf(event_count, event_type = '` + string(events.EventTypeSessionExpired) + `')) AS sessions_expired,
    toInt64(sumIf(event_count, event_type = '` + string(events.EventTypePromptSubmitted) + `')) AS prompts,
    toInt64(sumIf(event_count, event_type = '` + string(events.EventTypeExecutionRequested) + `')) AS runs,
    toInt64(sumIf(event_count, event_type = '` + string(events.EventTypeCodeDiff) + `')) AS diffs
FROM
(
    SELECT company_id, day, event_type, uniqExactMerge(events) AS event_count
    FROM task_daily
    GROUP BY company_id, day, event_type
)
GROUP BY company_id, day`,

	// Cohorts interviews by the day of their first event.
	`CREATE OR REPLACE VIEW v_funnel AS
SELECT
    company_id,
    toDate(first_at) AS cohort_day,
    toInt64(count()) AS interviews,
    toInt64(sum(started)) AS started,
    toInt64(sum(prompted)) AS prompted,
    toInt64(sum(ran)) AS ran,
    toInt64(sum(run_succeeded)) AS run_succeeded,
    toInt64(sum(submitted)) AS submitted,
    toInt64(sum(expired)) AS expired
FROM
(
    SELECT company_id, interview_id, min(first_at) AS first_at,
           max(started) AS started, max(prompted) AS prompted, max(ran) AS ran,
           max(run_succeeded) AS run_succeeded, max(submitted) AS submitted, max(expired) AS expired
    FROM funnel_interviews
    GROUP BY company_id, interview_id
)
GROUP BY company_id, cohort_day`,

	// Monthly usage per company. Tokens are split by mode: only managed
	// tokens are paid for by the platform.
	`CREATE OR REPLACE VIEW v_billing_usage AS
SELECT
    company_id,
    period,
    toInt64(sum(session_started)) AS sessions,
    sumIf(ai_input_tokens, ai_mode = 'managed') AS managed_input_tokens,
    sumIf(ai_output_tokens, ai_mode = 'managed') AS managed_output_tokens,
    sumIf(ai_input_tokens, ai_mode = 'byok') AS byok_input_tokens,
    sumIf(ai_output_tokens, ai_mode = 'byok') AS byok_output_tokens,
    toInt64(sum(run_requested)) AS runs,
    sum(run_duration_ms) AS run_duration_ms
FROM billing_usage_rows FINAL
GROUP BY company_id, period`,
}

// Statements returns the DDL for view, in execution order.
func (v View) Statements() []string {
	return []string{v.Table, "CREATE MATERIALIZED VIEW IF NOT EXISTS " + v.Name + " TO " + v.Target + " AS\n" + v.Select}
}

// Backfill is the INSERT that loads the view's target from existing events.
// Re-running it is a no-op once merged, like redelivery.
func (v View) Backfill() string {
	return "INSERT INTO " + v.Target + "\n" + v.Select
}

// Migrate creates every target table, materialized view and query view
// that does not exist yet, then backfills each newly created view from the
// events already in the log. The events table must exist. It returns the
// names of the views it created.
func Migrate(ctx context.Context, conn driver.Conn) ([]string, error) {
	var created []string
	for _, view := range Views {
		exists, err := tableExists(ctx, conn, view.Name)
		if err != nil {
			return created, err
		}
		for _, statement := range view.Statements() {
			if err := conn.Exec(ctx, statement); err != nil {
				return created, fmt.Errorf("analytics: create %s: %w", view.Name, err)
			}
		}
		if !exists {
			// Events inserted between creating the view and this backfill
			// land twice and are deduplicated like any redelivery.
			if err := conn.Exec(ctx, view.Backfill()); err != nil {
				return created, fmt.Errorf("analytics: backfill %s: %w", view.Name, err)
			}
			created = append(created, view.Name)
		}
	}
	for _, statement := range QueryViews {
		if err := conn.Exec(ctx, statement); err != nil {
			return created, fmt.Errorf("analytics: create query view: %w", err)
		}
	}
	return created, nil
}

func tableExists(ctx context.Context, conn driver.Conn, name string) (bool, error) {
	var count uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?`, name).Scan(&count); err != nil {
		return false, fmt.Errorf("analytics: check %s: %w", name, err)
	}
	return count > 0, nil
}
