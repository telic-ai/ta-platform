//go:build integration

package scoring_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/eventlogwriter"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/scoring"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics/fixtures"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
)

type env struct {
	pool   *pgxpool.Pool
	events *eventlogwriter.Store
	log    *scoring.ClickHouseLog
	scores *postgres.ScoreStore
}

func setup(t *testing.T, ctx context.Context) env {
	t.Helper()
	cfg, err := config.Load("scoring-integration-test")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	eventStore, err := eventlogwriter.NewStore(ch.Conn())
	if err != nil {
		t.Fatal(err)
	}
	if err := eventStore.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)
	return env{pool: pool, events: eventStore, log: scoring.NewClickHouseLog(ch.Conn()), scores: postgres.NewScoreStore(pool)}
}

func (e env) seed(t *testing.T, ctx context.Context, companyID, interviewID, sessionID uuid.UUID, lastSequence int64) {
	t.Helper()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO companies (id, name, slug) VALUES ($1, 'Acme', $2) ON CONFLICT DO NOTHING`, []any{companyID, companyID.String()}},
		{`INSERT INTO interviews (company_id, id, candidate_name, candidate_email, status, last_sequence_number)
		  VALUES ($1, $2, 'Ada', 'ada@example.com', 'in_progress', $3)`, []any{companyID, interviewID, lastSequence}},
		{`INSERT INTO sessions (company_id, id, interview_id, token_hash, expires_at, state)
		  VALUES ($1, $2, $3, $4, now() + interval '1 hour', 'completed')`, []any{companyID, sessionID, interviewID, auth.HashToken(uuid.NewString())}},
	} {
		if _, err := e.pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

func (e env) insertEvents(t *testing.T, ctx context.Context, values ...[]byte) {
	t.Helper()
	rows := make([]eventlogwriter.Row, 0, len(values))
	for _, value := range values {
		row, err := eventlogwriter.Decode(value, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	if err := e.events.Insert(ctx, rows); err != nil {
		t.Fatal(err)
	}
}

func (e env) outboxScoreEvents(t *testing.T, ctx context.Context) []events.Envelope {
	t.Helper()
	rows, err := e.pool.Query(ctx, `SELECT envelope FROM event_outbox WHERE event_type = 'score.computed' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var found []events.Envelope
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var envelope events.Envelope
		_ = json.Unmarshal(raw, &envelope)
		found = append(found, envelope)
	}
	return found
}

func TestScoreFixtureSessionEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	e := setup(t, ctx)
	sessions, _ := fixtures.All()
	var fixture fixtures.Session
	for _, s := range sessions {
		if s.SessionID == "0b6f3a1e-3333-4c1a-9a51-000000000001" {
			fixture = s
		}
	}
	companyID, interviewID, sessionID := uuid.MustParse(fixture.CompanyID), uuid.MustParse(fixture.InterviewID), uuid.MustParse(fixture.SessionID)
	var values [][]byte
	for _, raw := range fixture.Events {
		values = append(values, raw)
	}
	e.insertEvents(t, ctx, values...)
	e.seed(t, ctx, companyID, interviewID, sessionID, 12)

	// The AI returns prose instead of JSON: the score is stored anyway.
	service, err := scoring.NewService(e.log, e.scores, proseRecommender{}, scoring.Config{})
	if err != nil {
		t.Fatal(err)
	}
	trigger := scoring.Trigger{EventType: events.EventTypeSessionSubmitted, CompanyID: companyID, InterviewID: interviewID, SessionID: sessionID, SequenceNumber: 11}
	outcome, err := service.Score(ctx, trigger, false)
	if err != nil || outcome != scoring.OutcomeScored {
		t.Fatalf("Score = %v, %v", outcome, err)
	}
	stored, err := e.scores.GetScore(ctx, companyID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Metrics != fixture.Want && !equalMetrics(stored.Metrics, fixture.Want) {
		t.Fatalf("stored metrics = %+v\nwant %+v", stored.Metrics, fixture.Want)
	}
	if !stored.MetricsComplete || stored.Recommendation != nil || stored.RecommendationError != scoring.RecommendationErrInvalidJSON {
		t.Fatalf("stored = %+v", stored)
	}

	// Redelivery of the trigger is a no-op.
	if outcome, err := service.Score(ctx, trigger, false); err != nil || outcome != scoring.OutcomeAlreadyScored {
		t.Fatalf("second Score = %v, %v", outcome, err)
	}
	emitted := e.outboxScoreEvents(t, ctx)
	if len(emitted) != 1 || emitted[0].SequenceNumber != 13 {
		t.Fatalf("score.computed events = %+v, want one at sequence 13", emitted)
	}
}

func TestConcurrentScoringWritesOneRowAndOneEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	e := setup(t, ctx)
	companyID, interviewID, sessionID := uuid.New(), uuid.New(), uuid.New()
	e.insertEvents(t, ctx, session(t, companyID, interviewID, sessionID, 1, 2)...)
	e.seed(t, ctx, companyID, interviewID, sessionID, 2)
	trigger := scoring.Trigger{EventType: events.EventTypeSessionExpired, CompanyID: companyID, InterviewID: interviewID, SessionID: sessionID, SequenceNumber: 2}

	var wg sync.WaitGroup
	outcomes := make(chan scoring.Outcome, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			service, _ := scoring.NewService(e.log, e.scores, nil, scoring.Config{})
			outcome, err := service.Score(ctx, trigger, false)
			if err != nil {
				t.Error(err)
			}
			outcomes <- outcome
		}()
	}
	wg.Wait()
	close(outcomes)
	scored := 0
	for outcome := range outcomes {
		if outcome == scoring.OutcomeScored {
			scored++
		}
	}
	if scored != 1 || len(e.outboxScoreEvents(t, ctx)) != 1 {
		t.Fatalf("scored %d times, %d events; want exactly one", scored, len(e.outboxScoreEvents(t, ctx)))
	}
}

func TestBehindLogNacksUntilTheMissingEventArrives(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	e := setup(t, ctx)
	companyID, interviewID, sessionID := uuid.New(), uuid.New(), uuid.New()
	values := session(t, companyID, interviewID, sessionID, 1, 2, 3, 4)
	e.insertEvents(t, ctx, values[0], values[1], values[3]) // sequence 3 is late
	e.seed(t, ctx, companyID, interviewID, sessionID, 4)
	service, _ := scoring.NewService(e.log, e.scores, nil, scoring.Config{})
	trigger := scoring.Trigger{EventType: events.EventTypeSessionSubmitted, CompanyID: companyID, InterviewID: interviewID, SessionID: sessionID, SequenceNumber: 4}

	if _, err := service.Score(ctx, trigger, false); !errors.Is(err, scoring.ErrBehind) {
		t.Fatalf("Score with a gap = %v, want ErrBehind", err)
	}
	e.insertEvents(t, ctx, values[2])
	if _, err := service.Score(ctx, trigger, false); err != nil {
		t.Fatalf("Score after catch-up = %v", err)
	}
	stored, _ := e.scores.GetScore(ctx, companyID, sessionID)
	if !stored.MetricsComplete || stored.Metrics.PromptCount != 2 || stored.RecommendationError != scoring.RecommendationErrDisabled {
		t.Fatalf("stored = %+v", stored)
	}
}

func TestPurgedInterviewIsPermanentFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	e := setup(t, ctx)
	companyID, interviewID, sessionID := uuid.New(), uuid.New(), uuid.New()
	e.insertEvents(t, ctx, session(t, companyID, interviewID, sessionID, 1, 2)...)
	e.seed(t, ctx, companyID, interviewID, sessionID, 2)
	if _, err := e.pool.Exec(ctx, `UPDATE interviews SET terminal_at = now(), purged_at = now() WHERE id = $1`, interviewID); err != nil {
		t.Fatal(err)
	}
	service, _ := scoring.NewService(e.log, e.scores, nil, scoring.Config{})
	_, err := service.Score(ctx, scoring.Trigger{EventType: events.EventTypeSessionExpired, CompanyID: companyID, InterviewID: interviewID, SessionID: sessionID, SequenceNumber: 2}, false)
	var permanent *scoring.PermanentError
	if !errors.As(err, &permanent) || !errors.Is(err, scoring.ErrInterviewGone) {
		t.Fatalf("err = %v", err)
	}
}

// session builds envelopes for the given sequence numbers: started first,
// the trigger last, prompts in between.
func session(t *testing.T, companyID, interviewID, sessionID uuid.UUID, sequences ...int64) [][]byte {
	t.Helper()
	var values [][]byte
	for i, sequence := range sequences {
		var payload events.Payload = events.PromptSubmitted{SessionID: sessionID.String(), InterviewID: interviewID.String(), Prompt: "p"}
		switch {
		case i == 0:
			payload = events.SessionStarted{SessionID: sessionID.String(), InterviewID: interviewID.String()}
		case i == len(sequences)-1:
			payload = events.SessionSubmitted{SessionID: sessionID.String(), InterviewID: interviewID.String()}
		}
		envelope, err := events.New(companyID.String(), sequence, payload)
		if err != nil {
			t.Fatal(err)
		}
		value, _ := json.Marshal(envelope)
		values = append(values, value)
	}
	return values
}

func equalMetrics(a, b metrics.Metrics) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}

type proseRecommender struct{}

func (proseRecommender) Recommend(context.Context, metrics.Metrics) (scoring.Recommendation, string, error) {
	_, err := scoring.ParseRecommendation("They seem great, advance!")
	return scoring.Recommendation{}, "m", &scoring.RecommendationError{Code: scoring.RecommendationErrInvalidJSON, Err: err}
}
