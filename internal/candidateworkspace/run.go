package candidateworkspace

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
	"github.com/telic-ai/ta-platform/internal/sandbox"
)

var (
	// ErrInvalidRun is returned for runs rejected before any side effect.
	ErrInvalidRun = errors.New("invalid run")
	// ErrRunInProgress means the session already has a run in flight.
	ErrRunInProgress = errors.New("a run is already in progress for this session")
	// ErrLockHeld is what a RunLock returns when the lock is taken.
	ErrLockHeld = errors.New("lock is held")
)

// eventOutputBytes caps stdout and stderr copied into execution.completed.
const eventOutputBytes = 64 << 10

// RunLock is a per-key, expiring, single-holder lock.
type RunLock interface {
	// Acquire returns ErrLockHeld if another holder has key.
	Acquire(ctx context.Context, key string, ttl time.Duration) (release func(context.Context) error, err error)
}

// SnapshotStore stores workspace snapshots.
type SnapshotStore interface {
	Put(ctx context.Context, key string, body []byte, contentType string) error
}

// Executor calls the Execution Sandbox synchronously.
type Executor interface {
	Execute(ctx context.Context, request sandbox.ExecuteRequest) (sandbox.ExecuteResponse, error)
}

// RunRequest is the body of POST /session/run.
type RunRequest struct {
	Language   string            `json:"language"`
	Entrypoint string            `json:"entrypoint"`
	Files      map[string]string `json:"files"`
	Stdin      string            `json:"stdin,omitempty"`
}

// RunResult is returned to the candidate.
type RunResult struct {
	ExecutionID     string `json:"executionId"`
	Status          string `json:"status"`
	ExitCode        int    `json:"exitCode"`
	Stdout          string `json:"stdout"`
	Stderr          string `json:"stderr"`
	StdoutTruncated bool   `json:"stdoutTruncated"`
	StderrTruncated bool   `json:"stderrTruncated"`
	DurationMS      int64  `json:"durationMs"`
}

// RunConfig tunes the run flow.
type RunConfig struct {
	// Timeout is the program's wall-clock limit; zero means the sandbox
	// default.
	Timeout time.Duration
	// LockTTL bounds how long a crashed run can block the session. It must
	// exceed the longest run including snapshot upload and sandbox start.
	LockTTL time.Duration
	Logger  *slog.Logger
}

// RunService runs a candidate's code: snapshot to S3, sandbox call, and
// execution.requested / execution.completed, one run per session at a time.
type RunService struct {
	store     EventStore
	snapshots SnapshotStore
	executor  Executor
	lock      RunLock
	config    RunConfig
}

func NewRunService(store EventStore, snapshots SnapshotStore, executor Executor, lock RunLock, config RunConfig) *RunService {
	if config.LockTTL <= 0 {
		config.LockTTL = sandbox.MaxLimits.WallClock + time.Minute
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &RunService{store: store, snapshots: snapshots, executor: executor, lock: lock, config: config}
}

// SnapshotKey is where a run's snapshot is stored; tenant first so
// retention and erasure can work by prefix.
func SnapshotKey(companyID, interviewID uuid.UUID, executionID string) string {
	return fmt.Sprintf("companies/%s/interviews/%s/snapshots/%s.json", companyID, interviewID, executionID)
}

// Run executes request for session. Once the lock is taken the run is
// detached from ctx: a candidate who disconnects still gets a complete,
// recorded run, and the lock is always released.
func (s *RunService) Run(ctx context.Context, session domain.Session, request RunRequest) (RunResult, error) {
	interviewID, err := interviewOf(session)
	if err != nil {
		return RunResult{}, err
	}
	executionID := uuid.NewString()
	if err := validateRun(executionID, request); err != nil {
		return RunResult{}, err
	}
	body, err := sandbox.EncodeSnapshot(request.Files)
	if err != nil {
		return RunResult{}, fmt.Errorf("%w: %v", ErrInvalidRun, err)
	}

	release, err := s.lock.Acquire(ctx, "run:"+session.ID.String(), s.config.LockTTL)
	if errors.Is(err, ErrLockHeld) {
		return RunResult{}, ErrRunInProgress
	}
	if err != nil {
		return RunResult{}, fmt.Errorf("acquire run lock: %w", err)
	}
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.config.LockTTL)
	defer cancel()
	defer func() {
		if err := release(runCtx); err != nil {
			s.config.Logger.Warn("release run lock", slog.String("session_id", session.ID.String()), slog.Any("error", err))
		}
	}()

	key := SnapshotKey(session.CompanyID, interviewID, executionID)
	if err := s.snapshots.Put(runCtx, key, body, sandbox.SnapshotContentType); err != nil {
		return RunResult{}, fmt.Errorf("store snapshot: %w", err)
	}
	if _, err := s.record(runCtx, session, interviewID, events.ExecutionRequested{
		SessionID: session.ID.String(), InterviewID: interviewID.String(), ExecutionID: executionID,
		Language: request.Language, Entrypoint: request.Entrypoint, SnapshotKey: key,
		FileCount: len(request.Files), SnapshotBytes: len(body),
	}); err != nil {
		return RunResult{}, fmt.Errorf("record execution.requested: %w", err)
	}

	response, execErr := s.executor.Execute(runCtx, sandbox.ExecuteRequest{
		ExecutionID: executionID, SnapshotKey: key, Language: request.Language,
		Entrypoint: request.Entrypoint, Stdin: request.Stdin, TimeoutMS: s.config.Timeout.Milliseconds(),
	})
	result := RunResult{
		ExecutionID: executionID, Status: string(response.Status), ExitCode: response.ExitCode,
		Stdout: response.Stdout, Stderr: response.Stderr,
		StdoutTruncated: response.StdoutTruncated, StderrTruncated: response.StderrTruncated,
		DurationMS: response.DurationMS,
	}
	completed := events.ExecutionCompleted{
		SessionID: session.ID.String(), InterviewID: interviewID.String(), ExecutionID: executionID,
		Status: result.Status, ExitCode: result.ExitCode, DurationMS: result.DurationMS,
	}
	completed.Stdout, completed.StdoutTruncated = capOutput(result.Stdout, result.StdoutTruncated)
	completed.Stderr, completed.StderrTruncated = capOutput(result.Stderr, result.StderrTruncated)
	if execErr != nil {
		s.config.Logger.Error("sandbox execution failed", slog.String("execution_id", executionID), slog.Any("error", execErr))
		result = RunResult{ExecutionID: executionID, Status: events.ExecutionStatusError, ExitCode: -1}
		completed = events.ExecutionCompleted{
			SessionID: session.ID.String(), InterviewID: interviewID.String(), ExecutionID: executionID,
			Status: events.ExecutionStatusError, ExitCode: -1, ErrorCode: "sandbox_unavailable",
		}
	}
	if _, err := s.record(runCtx, session, interviewID, completed); err != nil {
		return result, fmt.Errorf("record execution.completed: %w", err)
	}
	return result, nil
}

func (s *RunService) record(ctx context.Context, session domain.Session, interviewID uuid.UUID, payload events.Payload) (int64, error) {
	return s.store.RecordEvents(ctx, session.CompanyID, interviewID, 1, func(first int64) ([]outbox.Message, error) {
		message, err := sessionMessage(session, first, payload)
		return []outbox.Message{message}, err
	})
}

func validateRun(executionID string, request RunRequest) error {
	files := make(map[string][]byte, len(request.Files))
	for name, content := range request.Files {
		files[name] = []byte(content)
	}
	if !utf8.ValidString(request.Stdin) || len(request.Stdin) > 64<<10 {
		return fmt.Errorf("%w: stdin must be UTF-8 of at most 64 KiB", ErrInvalidRun)
	}
	spec := sandbox.Spec{ExecutionID: executionID, Language: request.Language, Entrypoint: request.Entrypoint, Files: files}
	if err := spec.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRun, err)
	}
	return nil
}

// capOutput trims output for the event log without splitting a UTF-8
// sequence.
func capOutput(output string, truncated bool) (string, bool) {
	if len(output) <= eventOutputBytes {
		return output, truncated
	}
	cut := eventOutputBytes
	for cut > 0 && !utf8.RuneStart(output[cut]) {
		cut--
	}
	return output[:cut], true
}
