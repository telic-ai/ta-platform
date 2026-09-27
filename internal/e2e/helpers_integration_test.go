//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
)

// candidateFixture is one company with one interview and an Active
// candidate session for it.
type candidateFixture struct {
	CompanyID, InterviewID, SessionID uuid.UUID
	Token                             string
}

func seedCandidateSession(t *testing.T, ctx context.Context, pool *pgxpool.Pool) candidateFixture {
	t.Helper()
	f := candidateFixture{CompanyID: uuid.New(), InterviewID: uuid.New(), SessionID: uuid.New(), Token: "candidate-" + uuid.NewString()}
	if _, err := pool.Exec(ctx, `INSERT INTO companies (id, name, slug) VALUES ($1, 'Tenant', $2)`,
		f.CompanyID, "t-"+f.CompanyID.String()); err != nil {
		t.Fatalf("seed company: %v", err)
	}
	if err := postgres.NewInterviewStore(pool).Create(ctx, domain.Interview{
		ID: f.InterviewID, CompanyID: f.CompanyID, CandidateName: "Candidate",
		CandidateEmail: "candidate@example.test", Status: "in_progress",
	}); err != nil {
		t.Fatalf("seed interview: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO sessions (company_id, id, interview_id, token_hash, expires_at)
		VALUES ($1, $2, $3, $4, now() + interval '1 hour')`,
		f.CompanyID, f.SessionID, f.InterviewID, auth.HashToken(f.Token)); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	return f
}

// isolatedTopic creates a fresh topic so a run never sees older records.
func isolatedTopic(t *testing.T, ctx context.Context, client *kafka.Client, name string) string {
	t.Helper()
	topic := events.SessionEventsTopic + "." + name + "." + uuid.NewString()[:8]
	if err := client.CreateTopic(ctx, topic, 3, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	return topic
}

// relayOutboxTo points every pending outbox row at topic and relays them
// until the outbox is empty.
func relayOutboxTo(t *testing.T, ctx context.Context, pool *pgxpool.Pool, client *kafka.Client, topic string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE event_outbox SET topic = $1`, topic); err != nil {
		t.Fatalf("retarget outbox: %v", err)
	}
	producer := client.Writer("")
	defer producer.Close()
	relay, err := outbox.NewRelay(postgres.NewOutboxStore(pool), producer, outbox.Config{
		BatchSize: 50, Interval: 20 * time.Millisecond, OnError: func(err error) { t.Errorf("relay: %v", err) },
	})
	if err != nil {
		t.Fatal(err)
	}
	relayCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- relay.Run(relayCtx) }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var pending int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox`).Scan(&pending); err != nil {
			t.Fatalf("count outbox: %v", err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("outbox still holds %d rows", pending)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("relay: %v", err)
	}
}

// readEnvelopes reads want envelopes from every partition of topic.
func readEnvelopes(t *testing.T, ctx context.Context, brokers []string, topic string, want int) []events.Envelope {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, GroupID: "e2e-" + topic, Topic: topic, StartOffset: kafkago.FirstOffset})
	defer reader.Close()
	readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var out []events.Envelope
	for len(out) < want {
		message, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("read %s after %d of %d envelopes: %v", topic, len(out), want, err)
		}
		var envelope events.Envelope
		if err := json.Unmarshal(message.Value, &envelope); err != nil {
			t.Fatalf("decode envelope: %v", err)
		}
		out = append(out, envelope)
	}
	return out
}
