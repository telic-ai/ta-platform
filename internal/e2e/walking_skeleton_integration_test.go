//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/candidateworkspace"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/eventlogwriter"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/replay"
	"github.com/telic-ai/ta-platform/internal/store/clickhouse"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/store/postgres/migrations"
)

// TestSessionStartedEndToEnd drives Phase 1's walking skeleton:
// invite exchange -> session.started on Kafka -> event-log-writer ->
// ClickHouse -> tenant-scoped replay timeline.
func TestSessionStartedEndToEnd(t *testing.T) {
	cfg, err := config.Load("walking-skeleton-e2e-test")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := migratedSchema(t, ctx, cfg.PostgresDSN)
	companyA, companyB, interviewID := uuid.New(), uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO companies (id, name, slug) VALUES ($1, 'Tenant A', $2), ($3, 'Tenant B', $4)`,
		companyA, "a-"+companyA.String(), companyB, "b-"+companyB.String()); err != nil {
		t.Fatalf("seed companies: %v", err)
	}
	if err := postgres.NewInterviewStore(pool).Create(ctx, domain.Interview{
		ID: interviewID, CompanyID: companyA, CandidateName: "Candidate",
		CandidateEmail: "candidate@example.test", Status: "scheduled",
	}); err != nil {
		t.Fatalf("seed interview: %v", err)
	}
	for i, token := range []string{"invite-one", "invite-two"} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO invites (company_id, id, interview_id, email, role, token_hash, expires_at)
			VALUES ($1, $2, $3, $4, 'candidate', $5, now() + interval '1 hour')`,
			companyA, uuid.New(), interviewID, fmt.Sprintf("candidate-%d@example.test", i),
			candidateworkspace.HashToken(token)); err != nil {
			t.Fatalf("seed invite: %v", err)
		}
	}

	// A unique topic isolates this run from older records on session-events.
	topic := events.TopicSessionEvents + ".e2e." + uuid.NewString()[:8]
	kafkaClient := kafka.New(cfg.KafkaBrokers)
	if err := kafkaClient.CreateTopic(ctx, topic, 6, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	producer := kafkaClient.Writer(topic)
	defer producer.Close()
	deliveries := make(chan error, 2)
	publisher := kafka.NewEventPublisherWithConfig(producer, 16, func(_ events.Envelope, err error) { deliveries <- err })
	defer publisher.Close()

	sessions := postgres.NewSessionStore(pool)
	candidateAPI := httptest.NewServer(candidateworkspace.NewHTTPHandler(
		candidateworkspace.NewService(sessions, publisher, time.Hour), sessions).Routes())
	defer candidateAPI.Close()

	first := startSession(t, candidateAPI.URL, "invite-one", http.StatusOK)
	second := startSession(t, candidateAPI.URL, "invite-two", http.StatusOK)
	startSession(t, candidateAPI.URL, "invite-one", http.StatusUnauthorized)
	for range 2 {
		select {
		case err := <-deliveries:
			if err != nil {
				t.Fatalf("deliver session.started: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("session.started was not delivered to Kafka")
		}
	}

	// An Active session passes the workspace guard; a completed one gets 409.
	if status := workspaceStatus(t, candidateAPI.URL, first.AccessToken); status != http.StatusNotFound {
		t.Fatalf("active session status = %d, want the guarded 404", status)
	}
	if _, err := pool.Exec(ctx, `UPDATE sessions SET state = 'completed' WHERE id = $1`, first.SessionID); err != nil {
		t.Fatalf("complete session: %v", err)
	}
	if status := workspaceStatus(t, candidateAPI.URL, first.AccessToken); status != http.StatusConflict {
		t.Fatalf("completed session status = %d, want 409", status)
	}

	clickhouseClient, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		t.Fatalf("connect ClickHouse: %v", err)
	}
	defer clickhouseClient.Close()
	eventStore, err := eventlogwriter.NewStore(clickhouseClient.Conn())
	if err != nil {
		t.Fatal(err)
	}
	if err := eventStore.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	reader := kafkaClient.ReaderTopics([]string{topic}, "e2e-"+topic)
	logWriter, err := eventlogwriter.New(reader, eventStore, eventlogwriter.Config{
		BatchSize: 10, BatchWait: 100 * time.Millisecond,
		OnInvalid: func(_ kafkago.Message, err error) { t.Errorf("invalid event: %v", err) },
	})
	if err != nil {
		t.Fatal(err)
	}
	writerCtx, stopWriter := context.WithCancel(ctx)
	writerDone := make(chan error, 1)
	go func() { writerDone <- logWriter.Run(writerCtx) }()
	defer func() {
		stopWriter()
		if err := <-writerDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("event-log-writer: %v", err)
		}
		_ = logWriter.Close()
	}()

	replayAPI := httptest.NewServer(replay.NewHTTPHandler(
		replay.NewClickHouseStore(clickhouseClient.Conn()), replay.HeaderTenantResolver{}).Routes())
	defer replayAPI.Close()

	var timeline []replay.Event
	deadline := time.Now().Add(20 * time.Second)
	for {
		timeline = readTimeline(t, replayAPI.URL, companyA, interviewID, "")
		if len(timeline) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeline has %d events, want 2", len(timeline))
		}
		time.Sleep(100 * time.Millisecond)
	}
	var sequences []int64
	var sessionIDs []string
	for _, event := range timeline {
		if event.EventType != string(events.EventTypeSessionStarted) || event.InterviewID != interviewID.String() {
			t.Errorf("timeline event = %+v", event)
		}
		var payload events.SessionStarted
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		sequences = append(sequences, event.SequenceNumber)
		sessionIDs = append(sessionIDs, payload.SessionID)
	}
	if !slices.Equal(sequences, []int64{1, 2}) {
		t.Errorf("sequences = %v, want [1 2]", sequences)
	}
	if !slices.Equal(sessionIDs, []string{first.SessionID, second.SessionID}) {
		t.Errorf("session order = %v, want [%s %s]", sessionIDs, first.SessionID, second.SessionID)
	}
	if after := readTimeline(t, replayAPI.URL, companyA, interviewID, "1"); len(after) != 1 || after[0].SequenceNumber != 2 {
		t.Errorf("after_seq=1 timeline = %+v, want only sequence 2", after)
	}
	if other := readTimeline(t, replayAPI.URL, companyB, interviewID, ""); len(other) != 0 {
		t.Errorf("cross-tenant timeline returned %d events, want 0", len(other))
	}
}

func migratedSchema(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()
	admin, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect Postgres: %v", err)
	}
	t.Cleanup(admin.Close)
	schema := pgx.Identifier{"e2e_" + uuid.NewString()[:8]}.Sanitize()
	if _, err := admin.Pool().Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Pool().Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	})

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatalf("connect schema pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.New(pool).Up(ctx); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	return pool
}

func startSession(t *testing.T, baseURL, inviteToken string, wantStatus int) candidateworkspace.StartResponse {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"inviteToken": inviteToken})
	response, err := http.Post(baseURL+"/session/start", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /session/start: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		t.Fatalf("POST /session/start status = %d, want %d", response.StatusCode, wantStatus)
	}
	var started candidateworkspace.StartResponse
	if wantStatus == http.StatusOK {
		if err := json.NewDecoder(response.Body).Decode(&started); err != nil {
			t.Fatalf("decode start response: %v", err)
		}
		if started.AccessToken == "" || started.Scope != candidateworkspace.ScopeCandidateWorkspace {
			t.Fatalf("start response = %+v", started)
		}
	}
	return started
}

func workspaceStatus(t *testing.T, baseURL, token string) int {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, baseURL+"/candidate/workspace", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("GET workspace: %v", err)
	}
	response.Body.Close()
	return response.StatusCode
}

func readTimeline(t *testing.T, baseURL string, companyID, interviewID uuid.UUID, afterSeq string) []replay.Event {
	t.Helper()
	url := baseURL + "/interviews/" + interviewID.String() + "/timeline"
	if afterSeq != "" {
		url += "?after_seq=" + afterSeq
	}
	request, _ := http.NewRequest(http.MethodGet, url, nil)
	request.Header.Set("X-Company-ID", companyID.String())
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("GET timeline: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET timeline status = %d", response.StatusCode)
	}
	var body struct {
		Events []replay.Event `json:"events"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode timeline: %v", err)
	}
	return body.Events
}
