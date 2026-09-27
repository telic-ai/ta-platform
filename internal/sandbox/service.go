package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
	"unicode/utf8"
)

// Snapshot is the S3 object format of a workspace snapshot.
type Snapshot struct {
	Files map[string]string `json:"files"`
}

// SnapshotContentType is the stored snapshot's MIME type.
const SnapshotContentType = "application/json"

// EncodeSnapshot serializes files for storage.
func EncodeSnapshot(files map[string]string) ([]byte, error) {
	for name, content := range files {
		if !utf8.ValidString(content) {
			return nil, fmt.Errorf("%w: file %q is not UTF-8", ErrInvalidSpec, name)
		}
	}
	return json.Marshal(Snapshot{Files: files})
}

func decodeSnapshot(body []byte) (map[string][]byte, error) {
	var snapshot Snapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	files := make(map[string][]byte, len(snapshot.Files))
	for name, content := range snapshot.Files {
		files[name] = []byte(content)
	}
	return files, nil
}

// Validate checks a spec without running it.
func (s Spec) Validate() error {
	_, _, err := normalize(s)
	return err
}

// ExecuteRequest is the body of POST /v1/executions.
type ExecuteRequest struct {
	ExecutionID string `json:"execution_id"`
	SnapshotKey string `json:"snapshot_key"`
	Language    string `json:"language"`
	Entrypoint  string `json:"entrypoint"`
	Stdin       string `json:"stdin,omitempty"`
	TimeoutMS   int64  `json:"timeout_ms,omitempty"`
}

// ExecuteResponse is the synchronous result of an execution.
type ExecuteResponse struct {
	ExecutionID     string `json:"execution_id"`
	Status          Status `json:"status"`
	ExitCode        int    `json:"exit_code"`
	Stdout          string `json:"stdout"`
	Stderr          string `json:"stderr"`
	StdoutTruncated bool   `json:"stdout_truncated"`
	StderrTruncated bool   `json:"stderr_truncated"`
	DurationMS      int64  `json:"duration_ms"`
}

// SnapshotReader fetches stored snapshots.
type SnapshotReader interface {
	Get(ctx context.Context, key string, maxBytes int64) ([]byte, error)
}

// ErrBusy means every execution slot is taken.
var ErrBusy = errors.New("sandbox: at capacity")

// Service runs executions from stored snapshots with bounded concurrency.
type Service struct {
	runner    Runner
	snapshots SnapshotReader
	slots     chan struct{}
	logger    *slog.Logger
}

func NewService(runner Runner, snapshots SnapshotReader, concurrency int, logger *slog.Logger) *Service {
	if concurrency < 1 {
		concurrency = 1
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{runner: runner, snapshots: snapshots, slots: make(chan struct{}, concurrency), logger: logger}
}

// Execute loads the snapshot and runs it. It fails fast with ErrBusy
// rather than queueing, so the caller's wall clock stays meaningful.
func (s *Service) Execute(ctx context.Context, request ExecuteRequest) (ExecuteResponse, error) {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return ExecuteResponse{}, ErrBusy
	}
	if request.SnapshotKey == "" {
		return ExecuteResponse{}, fmt.Errorf("%w: snapshot_key is required", ErrInvalidSpec)
	}
	body, err := s.snapshots.Get(ctx, request.SnapshotKey, maxSnapshotBytes*2)
	if err != nil {
		return ExecuteResponse{}, fmt.Errorf("load snapshot: %w", err)
	}
	files, err := decodeSnapshot(body)
	if err != nil {
		return ExecuteResponse{}, fmt.Errorf("%w: %v", ErrInvalidSpec, err)
	}
	result, err := s.runner.Run(ctx, Spec{
		ExecutionID: request.ExecutionID, Language: request.Language, Entrypoint: request.Entrypoint,
		Files: files, Stdin: []byte(request.Stdin),
		Limits: Limits{WallClock: time.Duration(request.TimeoutMS) * time.Millisecond},
	})
	if err != nil {
		return ExecuteResponse{}, err
	}
	s.logger.Info("execution finished", slog.String("execution_id", request.ExecutionID),
		slog.String("status", string(result.Status)), slog.Duration("duration", result.Duration))
	return ExecuteResponse{
		ExecutionID: request.ExecutionID, Status: result.Status, ExitCode: result.ExitCode,
		Stdout: string(result.Stdout), Stderr: string(result.Stderr),
		StdoutTruncated: result.StdoutTruncated, StderrTruncated: result.StderrTruncated,
		DurationMS: result.Duration.Milliseconds(),
	}, nil
}

// Routes serves POST /v1/executions.
func (s *Service) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/executions", s.handleExecute)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

func (s *Service) handleExecute(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var request ExecuteRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_request", "message": "body must be an execution request"})
		return
	}
	response, err := s.Execute(r.Context(), request)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, response)
	case errors.Is(err, ErrInvalidSpec):
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_spec", "message": err.Error()})
	case errors.Is(err, ErrBusy):
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "busy", "message": err.Error()})
	default:
		s.logger.Error("execution failed", slog.String("execution_id", request.ExecutionID), slog.Any("error", err))
		writeJSON(w, http.StatusBadGateway, map[string]string{"code": "sandbox_error", "message": "execution could not be run"})
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
