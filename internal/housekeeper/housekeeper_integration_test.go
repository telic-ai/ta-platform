//go:build integration

package housekeeper_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/analytics"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/dashboard"
	"github.com/telic-ai/ta-platform/internal/eventindexer"
	"github.com/telic-ai/ta-platform/internal/eventlogwriter"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/housekeeper"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
	storekafka "github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/store/s3"
	"github.com/telic-ai/ta-platform/internal/store/typesense"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
	"github.com/telic-ai/ta-platform/internal/testutil/s3test"
)

type world struct {
	cfg       config.Config
	pool      *pgxpool.Pool
	ch        driver.Conn
	eventLog  *eventlogwriter.Store
	search    *typesense.Client
	bucket    *s3.Client
	topic     string
	producer  *kafkago.Writer
	companyID uuid.UUID
}

func newWorld(t *testing.T, ctx context.Context) *world {
	t.Helper()
	cfg, err := config.Load("housekeeper-integration-test")
	if err != nil {
		t.Fatal(err)
	}
	w := &world{cfg: cfg, companyID: uuid.New(), pool: pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)}
	chClient, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chClient.Close() })
	w.ch = chClient.Conn()
	if w.eventLog, err = eventlogwriter.NewStore(w.ch); err != nil {
		t.Fatal(err)
	}
	if err := w.eventLog.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := analytics.Migrate(ctx, w.ch); err != nil {
		t.Fatal(err)
	}
	if err := dashboard.NewClickHouseStore(w.ch).EnsureViews(ctx); err != nil {
		t.Fatal(err)
	}
	w.search = typesense.New(cfg.TypesenseURL, cfg.TypesenseKey)
	if err := w.search.EnsureCollection(ctx, eventindexer.Schema); err != nil {
		t.Fatal(err)
	}
	w.bucket = s3test.Client(t, "snapshots")
	w.topic = fmt.Sprintf("housekeeper-test.%d", time.Now().UnixNano())
	if err := storekafka.New(cfg.KafkaBrokers).CreateTopic(ctx, w.topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	w.producer = &kafkago.Writer{Addr: kafkago.TCP(cfg.KafkaBrokers), Topic: w.topic, Transport: &kafkago.Transport{}}
	t.Cleanup(func() { _ = w.producer.Close() })
	if _, err := w.pool.Exec(ctx, `INSERT INTO companies (id, name, slug) VALUES ($1, 'Acme', $2)`, w.companyID, w.companyID.String()); err != nil {
		t.Fatal(err)
	}
	return w
}

type interviewSpec struct {
	terminalAgo    time.Duration // 0: not terminal
	eraseRequested bool
	legalHold      bool
}

// seed creates an interview with data in every store: Postgres (interview,
// invite, session, score), ClickHouse events, Typesense documents, an S3
// snapshot and a Kafka record.
func (w *world) seed(t *testing.T, ctx context.Context, spec interviewSpec) uuid.UUID {
	t.Helper()
	interviewID, sessionID := uuid.New(), uuid.New()
	var terminalAt, eraseAt any
	if spec.terminalAgo > 0 {
		terminalAt = time.Now().Add(-spec.terminalAgo)
	}
	if spec.eraseRequested {
		eraseAt = time.Now()
	}
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO interviews (company_id, id, candidate_name, candidate_email, status, terminal_at, erase_requested_at, legal_hold, last_sequence_number)
		  VALUES ($1, $2, 'Ada Lovelace', 'ada@example.com', 'completed', $3, $4, $5, 3)`, []any{w.companyID, interviewID, terminalAt, eraseAt, spec.legalHold}},
		{`INSERT INTO invites (company_id, id, interview_id, email, role, token_hash, expires_at)
		  VALUES ($1, $2, $3, 'ada@example.com', 'candidate', $4, now())`, []any{w.companyID, uuid.New(), interviewID, auth.HashToken(uuid.NewString())}},
		{`INSERT INTO sessions (company_id, id, interview_id, token_hash, expires_at, state)
		  VALUES ($1, $2, $3, $4, now(), 'completed')`, []any{w.companyID, sessionID, interviewID, auth.HashToken(uuid.NewString())}},
		{`INSERT INTO scores (company_id, session_id, interview_id, trigger_event_type, metrics, metrics_complete, recommendation_error, computed_at)
		  VALUES ($1, $2, $3, 'session.submitted', '{}', true, 'disabled', now())`, []any{w.companyID, sessionID, interviewID}},
		{`INSERT INTO interview_scores (company_id, id, interview_id, dimension, proposed_value, rationale)
		  VALUES ($1, $2, $3, 'problem_solving', 3.5, 'used the hint well')`, []any{w.companyID, uuid.New(), interviewID}},
	} {
		if _, err := w.pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}

	var rows []eventlogwriter.Row
	var docs []any
	for i, payload := range []events.Payload{
		events.SessionStarted{SessionID: sessionID.String(), InterviewID: interviewID.String()},
		events.PromptSubmitted{SessionID: sessionID.String(), InterviewID: interviewID.String(), Prompt: "personal question"},
		events.CodeDiff{SessionID: sessionID.String(), InterviewID: interviewID.String(), Origin: "manual", Patch: "+secret"},
	} {
		envelope, _ := events.New(w.companyID.String(), int64(i+1), payload)
		value, _ := json.Marshal(envelope)
		row, err := eventlogwriter.Decode(value, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
		if doc, ok, _ := eventindexer.Decode(value); ok {
			docs = append(docs, doc)
		}
		if err := w.producer.WriteMessages(ctx, kafkago.Message{Key: []byte(interviewID.String()), Value: value}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.eventLog.Insert(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := w.search.Upsert(ctx, eventindexer.Collection, docs); err != nil {
		t.Fatal(err)
	}
	if err := w.bucket.Put(ctx, snapshotKey(w.companyID, interviewID), []byte(`{"files":{}}`), "application/json"); err != nil {
		t.Fatal(err)
	}
	return interviewID
}

func snapshotKey(companyID, interviewID uuid.UUID) string {
	return fmt.Sprintf("companies/%s/interviews/%s/snapshots/x.json", companyID, interviewID)
}

type footprint struct {
	SearchDocs, Events, MetricRows, Activity, Sessions, Invites, Scores, InterviewScores, PurgeLog int64
	Exists, Purged                                                                                 bool
	Name                                                                                           string
}

func (w *world) footprint(t *testing.T, ctx context.Context, interviewID uuid.UUID) footprint {
	t.Helper()
	var f footprint
	result, err := w.search.Search(ctx, eventindexer.Collection, typesense.SearchParams{
		Query: "*", QueryBy: "text", FilterBy: housekeeper.InterviewFilter(w.companyID, interviewID)})
	if err != nil {
		t.Fatal(err)
	}
	f.SearchDocs = int64(result.Found)
	for table, dst := range map[string]*int64{"events": &f.Events, "session_metric_rows": &f.MetricRows, "interview_activity": &f.Activity} {
		var n uint64
		if err := w.ch.QueryRow(ctx, "SELECT count() FROM "+table+" WHERE company_id = ? AND interview_id = ?", w.companyID.String(), interviewID.String()).Scan(&n); err != nil {
			t.Fatal(err)
		}
		*dst = int64(n)
	}
	for table, dst := range map[string]*int64{"sessions": &f.Sessions, "invites": &f.Invites, "scores": &f.Scores, "interview_scores": &f.InterviewScores, "purge_log": &f.PurgeLog} {
		if err := w.pool.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE company_id = $1 AND interview_id = $2", w.companyID, interviewID).Scan(dst); err != nil {
			t.Fatal(err)
		}
	}
	err = w.pool.QueryRow(ctx, `SELECT true, purged_at IS NOT NULL, candidate_name FROM interviews WHERE company_id = $1 AND id = $2`,
		w.companyID, interviewID).Scan(&f.Exists, &f.Purged, &f.Name)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

var intact = footprint{SearchDocs: 2, Events: 3, MetricRows: 3, Activity: 3, Sessions: 1, Invites: 1, Scores: 1, InterviewScores: 1, Exists: true, Name: "Ada Lovelace"}
var skeleton = footprint{PurgeLog: 1, Exists: true, Purged: true, Name: postgres.ErasedPlaceholder}

func (w *world) housekeeper(t *testing.T, analyticsStore housekeeper.Analytics) *housekeeper.Housekeeper {
	t.Helper()
	if analyticsStore == nil {
		analyticsStore = housekeeper.ClickHouseAnalytics{Conn: w.ch}
	}
	h, err := housekeeper.New(postgres.NewHousekeeperStore(w.pool), housekeeper.TypesenseSearch{Client: w.search}, analyticsStore,
		housekeeper.Config{Retention: 365 * 24 * time.Hour, BatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestPurgeAcrossStoresKeepsSkeletonAndIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	expired := w.seed(t, ctx, interviewSpec{terminalAgo: 400 * 24 * time.Hour})
	erased := w.seed(t, ctx, interviewSpec{eraseRequested: true})
	held := w.seed(t, ctx, interviewSpec{terminalAgo: 400 * 24 * time.Hour, eraseRequested: true, legalHold: true})
	recent := w.seed(t, ctx, interviewSpec{terminalAgo: 24 * time.Hour})
	active := w.seed(t, ctx, interviewSpec{})
	for _, id := range []uuid.UUID{expired, erased, held, recent, active} {
		if got := w.footprint(t, ctx, id); got != intact {
			t.Fatalf("seeded footprint = %+v", got)
		}
	}

	report, err := w.housekeeper(t, nil).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Purged) != 2 || len(report.Failures) != 0 {
		t.Fatalf("report = %+v", report)
	}
	for _, id := range []uuid.UUID{expired, erased} {
		if got := w.footprint(t, ctx, id); got != skeleton {
			t.Fatalf("purged footprint = %+v\nwant %+v", got, skeleton)
		}
	}
	for _, id := range []uuid.UUID{held, recent, active} {
		if got := w.footprint(t, ctx, id); got != intact {
			t.Fatalf("kept interview changed: %+v", got)
		}
	}
	reasons := map[uuid.UUID]string{}
	rows, _ := w.pool.Query(ctx, `SELECT interview_id, reason FROM purge_log WHERE company_id = $1`, w.companyID)
	for rows.Next() {
		var id uuid.UUID
		var reason string
		_ = rows.Scan(&id, &reason)
		reasons[id] = reason
	}
	rows.Close()
	if reasons[expired] != housekeeper.ReasonRetention || reasons[erased] != housekeeper.ReasonErasure {
		t.Fatalf("purge_log reasons = %v", reasons)
	}

	// S3 (bucket lifecycle) and Kafka (topic retention) are left alone.
	for _, id := range []uuid.UUID{expired, erased} {
		if _, err := w.bucket.Get(ctx, snapshotKey(w.companyID, id), 1<<20); err != nil {
			t.Fatalf("S3 snapshot of purged interview removed: %v", err)
		}
	}
	if n := w.kafkaRecordsFor(t, ctx, expired); n != 3 {
		t.Fatalf("Kafka holds %d records of the purged interview, want all 3 untouched", n)
	}

	// A second run finds nothing to do and changes nothing.
	again, err := w.housekeeper(t, nil).Run(ctx)
	if err != nil || again.Candidates != 0 || len(again.Purged) != 0 {
		t.Fatalf("second run = %+v, %v", again, err)
	}
	if got := w.footprint(t, ctx, expired); got != skeleton {
		t.Fatalf("after second run = %+v", got)
	}
}

func TestFailedPurgeIsRetriedToCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	id := w.seed(t, ctx, interviewSpec{eraseRequested: true})

	report, err := w.housekeeper(t, failingAnalytics{}).Run(ctx)
	if err != nil || len(report.Failures) != 1 {
		t.Fatalf("report = %+v, %v", report, err)
	}
	partial := w.footprint(t, ctx, id)
	if partial.SearchDocs != 0 || partial.Events != 3 || partial.InterviewScores != 1 || partial.Purged || partial.Sessions != 1 || partial.PurgeLog != 0 {
		t.Fatalf("after failed run = %+v; want search deleted, everything else intact", partial)
	}

	report, err = w.housekeeper(t, nil).Run(ctx)
	if err != nil || len(report.Purged) != 1 {
		t.Fatalf("retry = %+v, %v", report, err)
	}
	if got := w.footprint(t, ctx, id); got != skeleton {
		t.Fatalf("after retry = %+v", got)
	}
}

func TestLegalHoldPlacedAfterListingWins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	id := w.seed(t, ctx, interviewSpec{eraseRequested: true})
	store := postgres.NewHousekeeperStore(w.pool)
	cutoff := time.Now().Add(-365 * 24 * time.Hour)
	candidates, err := store.Candidates(ctx, cutoff, 10)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidates = %v, %v", candidates, err)
	}
	if _, err := w.pool.Exec(ctx, `UPDATE interviews SET legal_hold = true WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	called := false
	purged, err := store.Purge(ctx, candidates[0], cutoff, func(context.Context) (housekeeper.Stats, error) {
		called = true
		return housekeeper.Stats{}, nil
	})
	if err != nil || purged || called {
		t.Fatalf("purged=%v external called=%v err=%v; legal hold must win", purged, called, err)
	}
}

func (w *world) kafkaRecordsFor(t *testing.T, ctx context.Context, interviewID uuid.UUID) int {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{w.cfg.KafkaBrokers}, Topic: w.topic, Partition: 0})
	defer reader.Close()
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	count := 0
	for {
		message, err := reader.ReadMessage(readCtx)
		if err != nil {
			return count
		}
		if string(message.Key) == interviewID.String() {
			count++
		}
	}
}

type failingAnalytics struct{}

func (failingAnalytics) DeleteInterview(context.Context, uuid.UUID, uuid.UUID) error {
	return errors.New("clickhouse unavailable")
}
