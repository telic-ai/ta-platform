package sandbox_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/telic-ai/ta-platform/internal/sandbox"
	"github.com/telic-ai/ta-platform/internal/testutil/s3test"
)

type fakeRunner struct {
	spec    sandbox.Spec
	result  sandbox.Result
	err     error
	gate    chan struct{}
	entered chan struct{}
}

func (r *fakeRunner) Run(_ context.Context, spec sandbox.Spec) (sandbox.Result, error) {
	r.spec = spec
	if r.entered != nil {
		close(r.entered)
	}
	if r.gate != nil {
		<-r.gate
	}
	if r.err != nil {
		return sandbox.Result{}, r.err
	}
	if err := spec.Validate(); err != nil {
		return sandbox.Result{}, err
	}
	return r.result, nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestSnapshotRoundTrip(t *testing.T) {
	body, err := sandbox.EncodeSnapshot(map[string]string{"main.py": "print(1)"})
	if err != nil || !strings.Contains(string(body), `"main.py":"print(1)"`) {
		t.Fatalf("EncodeSnapshot = %s, %v", body, err)
	}
	if _, err := sandbox.EncodeSnapshot(map[string]string{"bad": "\xff"}); !errors.Is(err, sandbox.ErrInvalidSpec) {
		t.Errorf("non-UTF-8 err = %v", err)
	}
}

func TestExecuteLoadsSnapshotAndRuns(t *testing.T) {
	store := s3test.Client(t, "snapshots")
	body, _ := sandbox.EncodeSnapshot(map[string]string{"main.py": "print(1)", "lib/a.py": "A=1"})
	if err := store.Put(context.Background(), "k/1.json", body, sandbox.SnapshotContentType); err != nil {
		t.Fatal(err)
	}
	runner := &fakeRunner{result: sandbox.Result{Status: sandbox.StatusTimedOut, ExitCode: -1, Stdout: []byte("partial"), StdoutTruncated: true, Duration: 1500 * time.Millisecond}}
	service := sandbox.NewService(runner, store, 2, quiet())
	response, err := service.Execute(context.Background(), sandbox.ExecuteRequest{
		ExecutionID: "e1", SnapshotKey: "k/1.json", Language: "python", Entrypoint: "main.py", Stdin: "in", TimeoutMS: 2000,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if response.Status != sandbox.StatusTimedOut || response.Stdout != "partial" || !response.StdoutTruncated || response.DurationMS != 1500 || response.ExecutionID != "e1" {
		t.Errorf("response = %+v", response)
	}
	if string(runner.spec.Files["lib/a.py"]) != "A=1" || string(runner.spec.Stdin) != "in" || runner.spec.Limits.WallClock != 2*time.Second {
		t.Errorf("spec = %+v", runner.spec)
	}
}

func TestExecuteErrors(t *testing.T) {
	store := s3test.Client(t, "snapshots")
	_ = store.Put(context.Background(), "garbage", []byte("not json"), "application/json")
	service := sandbox.NewService(&fakeRunner{}, store, 1, quiet())
	if _, err := service.Execute(context.Background(), sandbox.ExecuteRequest{ExecutionID: "e", SnapshotKey: ""}); !errors.Is(err, sandbox.ErrInvalidSpec) {
		t.Errorf("empty key err = %v", err)
	}
	if _, err := service.Execute(context.Background(), sandbox.ExecuteRequest{ExecutionID: "e", SnapshotKey: "missing"}); err == nil {
		t.Error("missing snapshot accepted")
	}
	if _, err := service.Execute(context.Background(), sandbox.ExecuteRequest{ExecutionID: "e", SnapshotKey: "garbage"}); !errors.Is(err, sandbox.ErrInvalidSpec) {
		t.Errorf("garbage snapshot err = %v", err)
	}
}

func TestExecuteFailsFastAtCapacity(t *testing.T) {
	store := s3test.Client(t, "snapshots")
	body, _ := sandbox.EncodeSnapshot(map[string]string{"main.py": "x"})
	_ = store.Put(context.Background(), "k", body, sandbox.SnapshotContentType)
	runner := &fakeRunner{gate: make(chan struct{}), entered: make(chan struct{})}
	service := sandbox.NewService(runner, store, 1, quiet())
	request := sandbox.ExecuteRequest{ExecutionID: "e", SnapshotKey: "k", Language: "python", Entrypoint: "main.py"}
	done := make(chan error, 1)
	go func() { _, err := service.Execute(context.Background(), request); done <- err }()
	<-runner.entered
	if _, err := service.Execute(context.Background(), request); !errors.Is(err, sandbox.ErrBusy) {
		t.Errorf("err = %v, want ErrBusy", err)
	}
	close(runner.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestExecuteHTTPStatusCodes(t *testing.T) {
	store := s3test.Client(t, "snapshots")
	body, _ := sandbox.EncodeSnapshot(map[string]string{"main.py": "x"})
	_ = store.Put(context.Background(), "k", body, sandbox.SnapshotContentType)
	runner := &fakeRunner{result: sandbox.Result{Status: sandbox.StatusSucceeded, Stdout: []byte("ok")}}
	server := httptest.NewServer(sandbox.NewService(runner, store, 1, quiet()).Routes())
	defer server.Close()
	for body, want := range map[string]int{
		`{"execution_id":"e1","snapshot_key":"k","language":"python","entrypoint":"main.py"}`:    http.StatusOK,
		`{"execution_id":"e1","snapshot_key":"k","language":"cobol","entrypoint":"main.py"}`:     http.StatusBadRequest,
		`{"execution_id":"e1","snapshot_key":"nope","language":"python","entrypoint":"main.py"}`: http.StatusBadGateway,
		`{"unknown":true}`: http.StatusBadRequest,
	} {
		response, err := http.Post(server.URL+"/v1/executions", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != want {
			t.Errorf("%s: status %d, want %d", body, response.StatusCode, want)
		}
	}
	runner.err = errors.New("engine down")
	response, _ := http.Post(server.URL+"/v1/executions", "application/json",
		strings.NewReader(`{"execution_id":"e1","snapshot_key":"k","language":"python","entrypoint":"main.py"}`))
	payload, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusBadGateway || strings.Contains(string(payload), "engine down") {
		t.Errorf("runner failure: %d %s", response.StatusCode, payload)
	}
}
