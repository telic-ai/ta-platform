//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/telic-ai/ta-platform/internal/candidateworkspace"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/sandbox"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/store/redis"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
	"github.com/telic-ai/ta-platform/internal/testutil/s3test"
)

type redisRunLock struct{ locker *redis.Locker }

func (l redisRunLock) Acquire(ctx context.Context, key string, ttl time.Duration) (func(context.Context) error, error) {
	release, err := l.locker.Acquire(ctx, key, ttl)
	if errors.Is(err, redis.ErrLocked) {
		return nil, candidateworkspace.ErrLockHeld
	}
	return release, err
}

// sandboxRunner prefers gVisor and falls back to runc.
func sandboxRunner(t *testing.T) *sandbox.ContainerRunner {
	t.Helper()
	out, err := exec.Command("docker", "info", "--format", "{{json .Runtimes}}").Output()
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	if exec.Command("docker", "image", "inspect", "python:3.12-alpine").Run() != nil {
		t.Skip("python:3.12-alpine not pulled")
	}
	if bytes.Contains(out, []byte(`"runsc"`)) {
		return sandbox.NewGVisorRunner(sandbox.ContainerConfig{})
	}
	return sandbox.NewDockerRunner(sandbox.ContainerConfig{})
}

// TestRunFlowTimeoutAndSingleFlight drives POST /session/run through the
// real sandbox: a second run while one is active gets 429, and a program
// that outlives its wall clock ends as execution.completed{timed_out}.
func TestRunFlowTimeoutAndSingleFlight(t *testing.T) {
	cfg, err := config.Load("run-flow-e2e-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	runner := sandboxRunner(t)
	t.Logf("sandbox runtime: %s", runner.Runtime())

	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)
	fixture := seedCandidateSession(t, ctx, pool)
	snapshots := s3test.Client(t, "snapshots")
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	sandboxAPI := httptest.NewServer(sandbox.NewService(runner, snapshots, 2, quiet).Routes())
	defer sandboxAPI.Close()

	redisClient, err := redis.New(cfg.RedisAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer redisClient.Close()
	runs := candidateworkspace.NewRunService(postgres.NewEventStore(pool), snapshots,
		candidateworkspace.NewHTTPExecutor(sandboxAPI.URL, nil),
		redisRunLock{redis.NewLocker(redisClient, "e2e-"+fixture.SessionID.String()+":")},
		candidateworkspace.RunConfig{Timeout: 2 * time.Second, Logger: quiet})
	workspace := httptest.NewServer(candidateworkspace.NewHTTPHandler(nil, postgres.NewSessionStore(pool)).WithRuns(runs).Routes())
	defer workspace.Close()

	post := func(body string) (int, []byte) {
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, workspace.URL+"/session/run", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+fixture.Token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Errorf("POST /session/run: %v", err)
			return 0, nil
		}
		defer response.Body.Close()
		payload, _ := io.ReadAll(response.Body)
		return response.StatusCode, payload
	}

	type reply struct {
		status int
		body   []byte
	}
	first := make(chan reply, 1)
	go func() {
		status, body := post(`{"language":"python","entrypoint":"main.py","files":{"main.py":"print('spinning', flush=True)\nwhile True:\n    pass\n"}}`)
		first <- reply{status, body}
	}()
	// execution.requested is recorded just before the sandbox is called.
	deadline := time.Now().Add(20 * time.Second)
	for {
		var requested int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox WHERE event_type = $1`, events.EventTypeExecutionRequested).Scan(&requested)
		if requested == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first run never reached the sandbox")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status, body := post(`{"language":"python","entrypoint":"main.py","files":{"main.py":"print(1)"}}`); status != http.StatusTooManyRequests {
		t.Errorf("second run while active: %d %s, want 429", status, body)
	}

	got := <-first
	var result candidateworkspace.RunResult
	if err := json.Unmarshal(got.body, &result); err != nil || got.status != http.StatusOK {
		t.Fatalf("first run: %d %s", got.status, got.body)
	}
	if result.Status != events.ExecutionStatusTimedOut || !strings.Contains(result.Stdout, "spinning") {
		t.Fatalf("first run result = %+v", result)
	}
	if _, err := snapshots.Get(ctx, candidateworkspace.SnapshotKey(fixture.CompanyID, fixture.InterviewID, result.ExecutionID), 1<<20); err != nil {
		t.Errorf("snapshot not in S3: %v", err)
	}

	// The lock is released: a new run goes through.
	status, body := post(`{"language":"python","entrypoint":"main.py","files":{"main.py":"print(6*7)"}}`)
	if status != http.StatusOK || !strings.Contains(string(body), `"stdout":"42\n"`) {
		t.Errorf("run after timeout: %d %s", status, body)
	}

	kafkaClient := kafka.New(cfg.KafkaBrokers)
	topic := isolatedTopic(t, ctx, kafkaClient, "run")
	relayOutboxTo(t, ctx, pool, kafkaClient, topic)
	envelopes := readEnvelopes(t, ctx, strings.Split(cfg.KafkaBrokers, ","), topic, 4)
	bySequence := map[int64]events.Envelope{}
	for _, envelope := range envelopes {
		bySequence[envelope.SequenceNumber] = envelope
	}
	var requested events.ExecutionRequested
	var completed events.ExecutionCompleted
	if err := bySequence[1].Decode(&requested); err != nil {
		t.Fatalf("sequence 1: %v", err)
	}
	if err := bySequence[2].Decode(&completed); err != nil {
		t.Fatalf("sequence 2: %v", err)
	}
	if requested.ExecutionID != result.ExecutionID || completed.ExecutionID != result.ExecutionID {
		t.Errorf("execution ids: requested %s completed %s result %s", requested.ExecutionID, completed.ExecutionID, result.ExecutionID)
	}
	if completed.Status != events.ExecutionStatusTimedOut {
		t.Errorf("execution.completed status = %q, want timed_out", completed.Status)
	}
}
