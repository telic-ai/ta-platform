//go:build integration

package analytics_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/analytics"
	"github.com/telic-ai/ta-platform/internal/eventlogwriter"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics/fixtures"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
)

func setup(t *testing.T, ctx context.Context) (driver.Conn, *eventlogwriter.Store) {
	t.Helper()
	cfg, err := config.Load("analytics-integration-test")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	store, err := eventlogwriter.NewStore(ch.Conn())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := analytics.Migrate(ctx, ch.Conn()); err != nil {
		t.Fatal(err)
	}
	if created, err := analytics.Migrate(ctx, ch.Conn()); err != nil || len(created) != 0 {
		t.Fatalf("second Migrate created %v, %v; want an idempotent no-op", created, err)
	}
	return ch.Conn(), store
}

// deliver inserts each value as its own insert, the way redelivered Kafka
// records arrive in separate batches, so every copy fires the views.
func deliver(t *testing.T, ctx context.Context, store *eventlogwriter.Store, values [][]byte) {
	t.Helper()
	for _, value := range values {
		row, err := eventlogwriter.Decode(value, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Insert(ctx, []eventlogwriter.Row{row}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSessionMetricsViewEqualsScoringMetricsOnFixtures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, store := setup(t, ctx)
	sessions, err := fixtures.All()
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range sessions {
		t.Run(fixture.Name, func(t *testing.T) {
			// A fresh company per run keeps reruns and other tests apart.
			companyID := uuid.NewString()
			var values [][]byte
			for _, raw := range fixture.Events {
				values = append(values, bytes.ReplaceAll(raw, []byte(fixture.CompanyID), []byte(companyID)))
			}
			deliver(t, ctx, store, values)
			deliver(t, ctx, store, values) // full redelivery

			timeline, err := fixture.Timeline()
			if err != nil {
				t.Fatal(err)
			}
			want, err := metrics.Compute(timeline)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, fixture.Want) {
				t.Fatalf("fixture expectation is stale: Compute = %+v", want)
			}
			got := sessionMetrics(t, ctx, conn, companyID, fixture.InterviewID, fixture.SessionID)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("v_session_metrics = %s\nscoring metrics   = %s", jsonOf(got), jsonOf(want))
			}

			// A manual backfill over existing events changes nothing.
			for _, view := range analytics.Views {
				if err := conn.Exec(ctx, view.Backfill()); err != nil {
					t.Fatal(err)
				}
			}
			if got := sessionMetrics(t, ctx, conn, companyID, fixture.InterviewID, fixture.SessionID); !reflect.DeepEqual(got, want) {
				t.Fatalf("after backfill v_session_metrics = %s", jsonOf(got))
			}
		})
	}
}

func sessionMetrics(t *testing.T, ctx context.Context, conn driver.Conn, companyID, interviewID, sessionID string) metrics.Metrics {
	t.Helper()
	var m metrics.Metrics
	err := conn.QueryRow(ctx, `
		SELECT prompt_count, ai_response_count, ai_error_count, ai_input_tokens, ai_output_tokens,
		       run_count, run_succeeded_count, last_run_status,
		       diff_count, ai_diff_count, lines_added, lines_removed, ai_lines_added,
		       active_duration_ms, time_to_first_run_ms
		  FROM v_session_metrics
		 WHERE company_id = ? AND interview_id = ? AND session_id = ?`, companyID, interviewID, sessionID).Scan(
		&m.PromptCount, &m.AIResponseCount, &m.AIErrorCount, &m.AIInputTokens, &m.AIOutputTokens,
		&m.RunCount, &m.RunSucceededCount, &m.LastRunStatus,
		&m.DiffCount, &m.AIDiffCount, &m.LinesAdded, &m.LinesRemoved, &m.AILinesAdded,
		&m.ActiveDurationMS, &m.TimeToFirstRunMS)
	if err != nil {
		t.Fatalf("query v_session_metrics: %v", err)
	}
	return m
}

func TestFunnelTaskDailyAndBillingAreRedeliverySafe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, store := setup(t, ctx)
	companyID := uuid.NewString()
	day := time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC)

	// Interview A: full journey with a managed AI response and a passing
	// run. Interview B: started, then expired. Interview C: started on the
	// next day, in the next billing period, with a BYOK response.
	var values [][]byte
	add := func(interview string, sequence int64, at time.Time, payload events.Payload) {
		envelope, err := events.New(companyID, sequence, payload)
		if err != nil {
			t.Fatal(err)
		}
		envelope.OccurredAt = at
		value, _ := json.Marshal(envelope)
		values = append(values, value)
	}
	a, b, c := "interview-a", "interview-b", "interview-c"
	add(a, 1, day, events.SessionStarted{SessionID: "sa", InterviewID: a})
	add(a, 2, day.Add(time.Minute), events.PromptSubmitted{SessionID: "sa", InterviewID: a})
	add(a, 3, day.Add(2*time.Minute), events.AIResponseCompleted{SessionID: "sa", InterviewID: a, Mode: "managed", Status: "completed", InputTokens: 100, OutputTokens: 40})
	add(a, 4, day.Add(3*time.Minute), events.ExecutionRequested{SessionID: "sa", InterviewID: a})
	add(a, 5, day.Add(4*time.Minute), events.ExecutionCompleted{SessionID: "sa", InterviewID: a, Status: "succeeded", DurationMS: 1500})
	add(a, 6, day.Add(5*time.Minute), events.SessionSubmitted{SessionID: "sa", InterviewID: a})
	add(b, 1, day.Add(time.Hour), events.SessionStarted{SessionID: "sb", InterviewID: b})
	add(b, 2, day.Add(2*time.Hour), events.SessionExpired{SessionID: "sb", InterviewID: b})
	next := day.Add(24 * time.Hour)
	add(c, 1, next, events.SessionStarted{SessionID: "sc", InterviewID: c})
	add(c, 2, next.Add(time.Minute), events.AIResponseCompleted{SessionID: "sc", InterviewID: c, Mode: "byok", Status: "completed", InputTokens: 7, OutputTokens: 3})

	deliver(t, ctx, store, values)
	deliver(t, ctx, store, values)

	type funnelRow struct{ Interviews, Started, Prompted, Ran, RunSucceeded, Submitted, Expired int64 }
	var funnel funnelRow
	if err := conn.QueryRow(ctx, `SELECT interviews, started, prompted, ran, run_succeeded, submitted, expired
		FROM v_funnel WHERE company_id = ? AND cohort_day = toDate(?)`, companyID, day).Scan(
		&funnel.Interviews, &funnel.Started, &funnel.Prompted, &funnel.Ran, &funnel.RunSucceeded, &funnel.Submitted, &funnel.Expired); err != nil {
		t.Fatal(err)
	}
	if funnel != (funnelRow{Interviews: 2, Started: 2, Prompted: 1, Ran: 1, RunSucceeded: 1, Submitted: 1, Expired: 1}) {
		t.Fatalf("v_funnel = %+v", funnel)
	}

	type dailyRow struct{ Started, Submitted, Expired, Prompts, Runs, Diffs int64 }
	var daily dailyRow
	if err := conn.QueryRow(ctx, `SELECT sessions_started, sessions_submitted, sessions_expired, prompts, runs, diffs
		FROM v_task_daily WHERE company_id = ? AND day = toDate(?)`, companyID, day).Scan(
		&daily.Started, &daily.Submitted, &daily.Expired, &daily.Prompts, &daily.Runs, &daily.Diffs); err != nil {
		t.Fatal(err)
	}
	if daily != (dailyRow{Started: 2, Submitted: 1, Expired: 1, Prompts: 1, Runs: 1}) {
		t.Fatalf("v_task_daily = %+v", daily)
	}

	type usageRow struct{ Sessions, ManagedIn, ManagedOut, BYOKIn, BYOKOut, Runs, RunMS int64 }
	usage := func(period time.Time) usageRow {
		var u usageRow
		if err := conn.QueryRow(ctx, `SELECT sessions, managed_input_tokens, managed_output_tokens, byok_input_tokens,
			byok_output_tokens, runs, run_duration_ms FROM v_billing_usage WHERE company_id = ? AND period = toDate(?)`, companyID, period).Scan(
			&u.Sessions, &u.ManagedIn, &u.ManagedOut, &u.BYOKIn, &u.BYOKOut, &u.Runs, &u.RunMS); err != nil {
			t.Fatal(err)
		}
		return u
	}
	if got := usage(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)); got != (usageRow{Sessions: 2, ManagedIn: 100, ManagedOut: 40, Runs: 1, RunMS: 1500}) {
		t.Fatalf("August usage = %+v", got)
	}
	if got := usage(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)); got != (usageRow{Sessions: 1, BYOKIn: 7, BYOKOut: 3}) {
		t.Fatalf("September usage = %+v", got)
	}
}

func jsonOf(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestMigrateBackfillsEventsLoggedBeforeTheViews(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg, _ := config.Load("analytics-integration-test")
	admin, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	database := "analytics_test_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if err := admin.Conn().Exec(ctx, "CREATE DATABASE "+database); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Conn().Exec(context.Background(), "DROP DATABASE IF EXISTS "+database) })
	dsn, err := url.Parse(cfg.ClickHouseDSN)
	if err != nil {
		t.Fatal(err)
	}
	dsn.Path = "/" + database
	ch, err := clickhouse.New(dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	store, _ := eventlogwriter.NewStore(ch.Conn())
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	sessions, _ := fixtures.All()
	fixture := sessions[0]
	var values [][]byte
	for _, raw := range fixture.Events {
		values = append(values, raw)
	}
	deliver(t, ctx, store, values)

	created, err := analytics.Migrate(ctx, ch.Conn())
	if err != nil {
		t.Fatal(err)
	}
	if len(created) != len(analytics.Views) {
		t.Fatalf("created %v, want every view", created)
	}
	if got := sessionMetrics(t, ctx, ch.Conn(), fixture.CompanyID, fixture.InterviewID, fixture.SessionID); !reflect.DeepEqual(got, fixture.Want) {
		t.Fatalf("backfilled v_session_metrics = %s\nwant %s", jsonOf(got), jsonOf(fixture.Want))
	}
}
