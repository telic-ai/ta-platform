package candidateworkspace

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/telic-ai/ta-platform/internal/sandbox"
	"github.com/telic-ai/ta-platform/internal/testutil/s3test"
)

type okRunner struct{}

func (okRunner) Run(_ context.Context, spec sandbox.Spec) (sandbox.Result, error) {
	return sandbox.Result{Status: sandbox.StatusFailed, ExitCode: 3, Stderr: spec.Files[spec.Entrypoint]}, nil
}

func TestHTTPExecutorAgainstSandboxService(t *testing.T) {
	store := s3test.Client(t, "snapshots")
	body, _ := sandbox.EncodeSnapshot(map[string]string{"main.py": "boom"})
	_ = store.Put(context.Background(), "k", body, sandbox.SnapshotContentType)
	server := httptest.NewServer(sandbox.NewService(okRunner{}, store, 1, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes())
	defer server.Close()

	executor := NewHTTPExecutor(server.URL+"/", nil)
	response, err := executor.Execute(context.Background(), sandbox.ExecuteRequest{ExecutionID: "e1", SnapshotKey: "k", Language: "python", Entrypoint: "main.py"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if response.Status != sandbox.StatusFailed || response.ExitCode != 3 || response.Stderr != "boom" || response.ExecutionID != "e1" {
		t.Errorf("response = %+v", response)
	}
	if _, err := executor.Execute(context.Background(), sandbox.ExecuteRequest{ExecutionID: "e1", SnapshotKey: "missing", Language: "python", Entrypoint: "main.py"}); err == nil {
		t.Error("non-200 accepted")
	}
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "{") }))
	defer broken.Close()
	if _, err := NewHTTPExecutor(broken.URL, nil).Execute(context.Background(), sandbox.ExecuteRequest{}); err == nil {
		t.Error("bad JSON accepted")
	}
}
