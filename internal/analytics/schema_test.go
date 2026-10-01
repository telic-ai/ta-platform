package analytics

import (
	"strings"
	"testing"

	"github.com/telic-ai/ta-platform/internal/scoring/metrics"
)

func TestSessionMetricsViewCoversExactlyTheScoringActivityTypes(t *testing.T) {
	view := Views[0]
	if view.Name != "mv_session_metrics" {
		t.Fatalf("first view = %s", view.Name)
	}
	for _, eventType := range metrics.ActivityTypes {
		if !strings.Contains(view.Select, "'"+string(eventType)+"'") {
			t.Errorf("mv_session_metrics ignores %s", eventType)
		}
	}
	if strings.Contains(view.Select, "score.computed") {
		t.Error("mv_session_metrics must not count score.computed")
	}
}

func TestEveryViewWritesToItsOwnTarget(t *testing.T) {
	names := map[string]bool{}
	for _, view := range Views {
		statements := view.Statements()
		if len(statements) != 2 {
			t.Fatalf("%s: %d statements", view.Name, len(statements))
		}
		if !strings.Contains(statements[0], "CREATE TABLE IF NOT EXISTS "+view.Target+"\n") {
			t.Errorf("%s: first statement does not create %s", view.Name, view.Target)
		}
		if !strings.HasPrefix(statements[1], "CREATE MATERIALIZED VIEW IF NOT EXISTS "+view.Name+" TO "+view.Target+" AS") {
			t.Errorf("%s: view statement = %q", view.Name, statements[1][:80])
		}
		if !strings.HasPrefix(view.Backfill(), "INSERT INTO "+view.Target+"\nSELECT") {
			t.Errorf("%s: backfill = %q", view.Name, view.Backfill()[:60])
		}
		if names[view.Name] || names[view.Target] {
			t.Errorf("duplicate name in %s", view.Name)
		}
		names[view.Name], names[view.Target] = true, true
	}
	for _, want := range []string{"mv_session_metrics", "mv_task_daily", "mv_funnel", "mv_billing_usage"} {
		if !names[want] {
			t.Errorf("missing %s", want)
		}
	}
}

func TestSumBasedTargetsDeduplicateLikeEvents(t *testing.T) {
	// Targets that sum per-event values must replace redelivered rows on
	// the events sort key, or redelivery would double count.
	for _, view := range Views {
		if !strings.Contains(view.Table, "ReplacingMergeTree") {
			continue
		}
		if !strings.Contains(view.Table, "interview_id, sequence_number)") {
			t.Errorf("%s: replacing key does not end in the event key", view.Target)
		}
	}
}

func TestSQLListEscapesQuotes(t *testing.T) {
	if got := sqlList("a", "b'c"); got != `'a', 'b\'c'` {
		t.Fatalf("sqlList = %s", got)
	}
}
