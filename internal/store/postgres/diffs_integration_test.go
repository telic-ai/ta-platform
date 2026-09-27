//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/candidateworkspace"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
)

func TestRecordDiffDropsOutOfOrderAtomically(t *testing.T) {
	cfg, _ := config.Load("diffs-integration-test")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)
	companyID, interviewID := seedInterview(t, ctx, pool)
	sessionID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO sessions (company_id, id, interview_id, token_hash, expires_at)
		VALUES ($1, $2, $3, $4, now() + interval '1 hour')`, companyID, sessionID, interviewID, auth.HashToken(uuid.NewString())); err != nil {
		t.Fatal(err)
	}
	store := postgres.NewEventStore(pool)
	record := func(clientSeq int64) (int64, error) {
		return store.RecordDiff(ctx, companyID, sessionID, interviewID, clientSeq, func(seq int64) (outbox.Message, error) {
			envelope, _ := events.New(companyID.String(), seq, events.CodeDiff{ClientSequence: clientSeq, InterviewID: interviewID.String()})
			return outbox.NewMessage(events.SessionEventsTopic, sessionID.String(), envelope)
		})
	}

	if seq, err := record(2); err != nil || seq != 1 {
		t.Fatalf("first diff = %d, %v", seq, err)
	}
	for _, stale := range []int64{1, 2} {
		if _, err := record(stale); !errors.Is(err, candidateworkspace.ErrDiffOutOfOrder) {
			t.Errorf("client seq %d: err = %v, want ErrDiffOutOfOrder", stale, err)
		}
	}
	var last, outboxRows int64
	_ = pool.QueryRow(ctx, `SELECT last_sequence_number FROM interviews WHERE id = $1`, interviewID).Scan(&last)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox`).Scan(&outboxRows)
	if last != 1 || outboxRows != 1 {
		t.Errorf("dropped diffs had side effects: last_sequence_number=%d outbox=%d", last, outboxRows)
	}

	// Concurrent, shuffled diffs: whatever is accepted must have client
	// sequences increasing with the allocated event sequence.
	clientSeqs := make([]int64, 40)
	for i := range clientSeqs {
		clientSeqs[i] = int64(i + 3)
	}
	rand.Shuffle(len(clientSeqs), func(i, j int) { clientSeqs[i], clientSeqs[j] = clientSeqs[j], clientSeqs[i] })
	var mu sync.Mutex
	accepted := map[int64]int64{} // event seq -> client seq
	var wg sync.WaitGroup
	for _, clientSeq := range clientSeqs {
		wg.Add(1)
		go func(clientSeq int64) {
			defer wg.Done()
			seq, err := record(clientSeq)
			if errors.Is(err, candidateworkspace.ErrDiffOutOfOrder) {
				return
			}
			if err != nil {
				t.Errorf("record %d: %v", clientSeq, err)
				return
			}
			mu.Lock()
			accepted[seq] = clientSeq
			mu.Unlock()
		}(clientSeq)
	}
	wg.Wait()
	if len(accepted) == 0 {
		t.Fatal("no concurrent diff was accepted")
	}
	previous := int64(2)
	for seq := int64(2); seq < int64(2+len(accepted)); seq++ {
		clientSeq, ok := accepted[seq]
		if !ok {
			t.Fatalf("event sequence %d missing; accepted = %v", seq, accepted)
		}
		if clientSeq <= previous {
			t.Fatalf("event seq %d has client seq %d after %d", seq, clientSeq, previous)
		}
		previous = clientSeq
	}
	var stored int64
	_ = pool.QueryRow(ctx, `SELECT last_diff_sequence FROM sessions WHERE id = $1`, sessionID).Scan(&stored)
	if stored != previous {
		t.Errorf("last_diff_sequence = %d, want %d", stored, previous)
	}
	t.Logf("accepted %d of %d concurrent shuffled diffs, in order", len(accepted), len(clientSeqs))
}
