//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
)

func seedInterview(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (uuid.UUID, uuid.UUID) {
	t.Helper()
	companyID, interviewID := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO companies (id, name, slug) VALUES ($1, 'Co', $2)`, companyID, companyID.String()); err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewInterviewStore(pool).Create(ctx, domain.Interview{ID: interviewID, CompanyID: companyID,
		CandidateName: "C", CandidateEmail: "c@example.test", Status: "in_progress"}); err != nil {
		t.Fatal(err)
	}
	return companyID, interviewID
}

func TestRecordEventsAllocatesAndRollsBack(t *testing.T) {
	cfg, _ := config.Load("events-integration-test")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)
	companyID, interviewID := seedInterview(t, ctx, pool)
	store := postgres.NewEventStore(pool)

	message := func(seq int64) outbox.Message {
		envelope, _ := events.New(companyID.String(), seq, events.PromptSubmitted{InterviewID: interviewID.String()})
		m, err := outbox.NewMessage(events.SessionEventsTopic, "session", envelope)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	first, err := store.RecordEvents(ctx, companyID, interviewID, 2, func(first int64) ([]outbox.Message, error) {
		return []outbox.Message{message(first)}, nil
	})
	if err != nil || first != 1 {
		t.Fatalf("first allocation = %d, %v; want 1", first, err)
	}
	second, err := store.RecordEvents(ctx, companyID, interviewID, 1, func(first int64) ([]outbox.Message, error) {
		return []outbox.Message{message(first)}, nil
	})
	if err != nil || second != 3 {
		t.Fatalf("second allocation = %d, %v; want 3 (2 was reserved)", second, err)
	}

	boom := errors.New("builder failed")
	if _, err := store.RecordEvents(ctx, companyID, interviewID, 1, func(int64) ([]outbox.Message, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	var last int64
	var outboxRows int
	_ = pool.QueryRow(ctx, `SELECT last_sequence_number FROM interviews WHERE id = $1`, interviewID).Scan(&last)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox`).Scan(&outboxRows)
	if last != 3 || outboxRows != 2 {
		t.Errorf("after failed build: last=%d outbox=%d, want 3 and 2 (rolled back)", last, outboxRows)
	}

	if _, err := store.RecordEvents(ctx, uuid.New(), interviewID, 1, func(int64) ([]outbox.Message, error) { return nil, nil }); !errors.Is(err, postgres.ErrInterviewNotFound) {
		t.Errorf("wrong tenant err = %v, want ErrInterviewNotFound", err)
	}
	if _, err := store.RecordEvents(ctx, companyID, interviewID, 0, nil); err == nil {
		t.Error("count 0 accepted")
	}
}
