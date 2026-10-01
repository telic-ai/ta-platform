//go:build integration

package dashboard

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/eventlogwriter"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
)

// TestViewsAggregateLoggedEvents writes events through the event log and
// reads them back through the materialized views, scoped per company.
func TestViewsAggregateLoggedEvents(t *testing.T) {
	cfg, _ := config.Load("dashboard-integration-test")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	log, _ := eventlogwriter.NewStore(client.Conn())
	if err := log.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	store := NewClickHouseStore(client.Conn())
	if err := store.EnsureViews(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureViews(ctx); err != nil {
		t.Fatalf("EnsureViews is not idempotent: %v", err)
	}

	companyA, companyB := uuid.New(), uuid.New()
	interview := uuid.New()
	now := time.Now().UTC()
	var rows []eventlogwriter.Row
	seq := int64(0)
	add := func(company uuid.UUID, payload events.Payload) {
		seq++
		envelope, err := events.New(company.String(), seq, payload)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(envelope)
		row, err := eventlogwriter.Decode(raw, now)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	iv := interview.String()
	add(companyA, events.SessionStarted{InterviewID: iv})
	add(companyA, events.PromptSubmitted{InterviewID: iv})
	add(companyA, events.AIResponseCompleted{InterviewID: iv, Status: events.AIResponseStatusCompleted})
	add(companyA, events.CodeDiff{InterviewID: iv, Origin: events.DiffOriginManual, LinesAdded: 4, LinesRemoved: 1})
	add(companyA, events.CodeDiff{InterviewID: iv, Origin: events.DiffOriginAIApplied, LinesAdded: 6})
	add(companyA, events.ExecutionRequested{InterviewID: iv})
	add(companyA, events.ExecutionCompleted{InterviewID: iv, Status: events.ExecutionStatusSucceeded})
	add(companyB, events.SessionStarted{InterviewID: uuid.NewString()})
	if err := log.Insert(ctx, rows); err != nil {
		t.Fatal(err)
	}

	daily, err := store.DailyEvents(ctx, companyA, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	totals := map[string]uint64{}
	for _, d := range daily {
		totals[d.EventType] += d.Events
		if d.Day != now.Format(time.DateOnly) {
			t.Errorf("day = %s", d.Day)
		}
	}
	if totals["code.diff"] != 2 || totals["session.started"] != 1 || len(totals) != 6 {
		t.Fatalf("company A totals = %v", totals)
	}

	activity, err := store.ActivityRows(ctx, companyA)
	if err != nil {
		t.Fatal(err)
	}
	summary := Summarize(activity)
	if len(summary) != 1 {
		t.Fatalf("company A summaries = %+v", summary)
	}
	s := summary[0]
	if s.InterviewID != iv || s.Events != 7 || s.Prompts != 1 || s.AIResponses != 1 || s.Runs != 1 || s.RunsSucceeded != 1 ||
		s.Diffs != 2 || s.AIAppliedDiffs != 1 || s.LinesAdded != 10 || s.LinesRemoved != 1 || s.FirstAt.IsZero() {
		t.Fatalf("summary = %+v", s)
	}

	other, _ := store.ActivityRows(ctx, companyB)
	if len(Summarize(other)) != 1 || Summarize(other)[0].InterviewID == iv {
		t.Fatalf("company B activity = %+v", other)
	}
}
