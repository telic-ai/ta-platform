package candidateworkspace

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/sandbox"
)

// memoryLock is an in-process RunLock.
type memoryLock struct {
	mu       sync.Mutex
	held     map[string]bool
	ttls     []time.Duration
	released int
}

func (l *memoryLock) Acquire(_ context.Context, key string, ttl time.Duration) (func(context.Context) error, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held == nil {
		l.held = map[string]bool{}
	}
	if l.held[key] {
		return nil, ErrLockHeld
	}
	l.held[key] = true
	l.ttls = append(l.ttls, ttl)
	return func(context.Context) error {
		l.mu.Lock()
		defer l.mu.Unlock()
		delete(l.held, key)
		l.released++
		return nil
	}, nil
}

type fakeSnapshots struct {
	keys   []string
	bodies map[string][]byte
	err    error
}

func (s *fakeSnapshots) Put(_ context.Context, key string, body []byte, contentType string) error {
	if s.err != nil {
		return s.err
	}
	if s.bodies == nil {
		s.bodies = map[string][]byte{}
	}
	s.keys = append(s.keys, key)
	s.bodies[key] = body
	return nil
}

type fakeExecutor struct {
	store    *memoryEventStore
	request  sandbox.ExecuteRequest
	response sandbox.ExecuteResponse
	err      error
	// gate, when set, blocks Execute until closed.
	gate    chan struct{}
	entered chan struct{}
	ctxErr  error
	seen    int
}

func (e *fakeExecutor) Execute(ctx context.Context, request sandbox.ExecuteRequest) (sandbox.ExecuteResponse, error) {
	e.request = request
	if e.store != nil {
		e.store.mu.Lock()
		e.seen = len(e.store.messages)
		e.store.mu.Unlock()
	}
	if e.entered != nil {
		close(e.entered)
	}
	if e.gate != nil {
		<-e.gate
	}
	e.ctxErr = ctx.Err()
	response := e.response
	response.ExecutionID = request.ExecutionID
	return response, e.err
}

type runFixture struct {
	store     *memoryEventStore
	snapshots *fakeSnapshots
	executor  *fakeExecutor
	lock      *memoryLock
	service   *RunService
}

func newRunFixture(response sandbox.ExecuteResponse) *runFixture {
	f := &runFixture{store: &memoryEventStore{}, snapshots: &fakeSnapshots{}, lock: &memoryLock{}}
	f.executor = &fakeExecutor{store: f.store, response: response}
	f.service = NewRunService(f.store, f.snapshots, f.executor, f.lock, RunConfig{Timeout: 5 * time.Second, LockTTL: time.Minute})
	return f
}

func pythonRun() RunRequest {
	return RunRequest{Language: "python", Entrypoint: "main.py", Files: map[string]string{"main.py": "print('hi')"}, Stdin: "in"}
}

func decodeRun(t *testing.T, store *memoryEventStore) (events.ExecutionRequested, events.ExecutionCompleted, []int64) {
	t.Helper()
	envelopes := store.envelopes(t)
	if len(envelopes) != 2 {
		t.Fatalf("recorded %d events, want execution.requested + execution.completed", len(envelopes))
	}
	var requested events.ExecutionRequested
	var completed events.ExecutionCompleted
	if err := envelopes[0].Decode(&requested); err != nil {
		t.Fatalf("first event: %v", err)
	}
	if err := envelopes[1].Decode(&completed); err != nil {
		t.Fatalf("second event: %v", err)
	}
	return requested, completed, []int64{envelopes[0].SequenceNumber, envelopes[1].SequenceNumber}
}

func TestRunSnapshotsThenRequestsThenCompletes(t *testing.T) {
	f := newRunFixture(sandbox.ExecuteResponse{Status: sandbox.StatusSucceeded, Stdout: "hi\n", DurationMS: 42})
	session := activeCandidate()
	result, err := f.service.Run(context.Background(), session, pythonRun())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != "succeeded" || result.Stdout != "hi\n" || result.DurationMS != 42 || result.ExecutionID == "" {
		t.Errorf("result = %+v", result)
	}
	requested, completed, seqs := decodeRun(t, f.store)
	if seqs[0] >= seqs[1] {
		t.Errorf("sequence numbers %v not ordered", seqs)
	}
	if f.executor.seen != 1 {
		t.Errorf("sandbox called with %d events recorded, want execution.requested first", f.executor.seen)
	}
	wantKey := SnapshotKey(session.CompanyID, *session.InterviewID, result.ExecutionID)
	if len(f.snapshots.keys) != 1 || f.snapshots.keys[0] != wantKey || !strings.HasPrefix(wantKey, "companies/"+session.CompanyID.String()+"/") {
		t.Errorf("snapshot keys = %v, want %s", f.snapshots.keys, wantKey)
	}
	if !strings.Contains(string(f.snapshots.bodies[wantKey]), `"main.py":"print('hi')"`) {
		t.Errorf("snapshot body = %s", f.snapshots.bodies[wantKey])
	}
	request := f.executor.request
	if request.SnapshotKey != wantKey || request.ExecutionID != result.ExecutionID || request.TimeoutMS != 5000 || request.Stdin != "in" {
		t.Errorf("sandbox request = %+v", request)
	}
	if requested.ExecutionID != result.ExecutionID || requested.SnapshotKey != wantKey || requested.FileCount != 1 || requested.Language != "python" {
		t.Errorf("requested = %+v", requested)
	}
	if completed.Status != "succeeded" || completed.Stdout != "hi\n" || completed.DurationMS != 42 {
		t.Errorf("completed = %+v", completed)
	}
	if f.lock.released != 1 || len(f.lock.held) != 0 {
		t.Errorf("lock not released: %+v", f.lock)
	}
}

func TestRunTimeoutSurfacesAsTimedOut(t *testing.T) {
	f := newRunFixture(sandbox.ExecuteResponse{Status: sandbox.StatusTimedOut, ExitCode: -1, Stdout: "partial", DurationMS: 5000})
	result, err := f.service.Run(context.Background(), activeCandidate(), pythonRun())
	if err != nil {
		t.Fatal(err)
	}
	_, completed, _ := decodeRun(t, f.store)
	if result.Status != events.ExecutionStatusTimedOut || completed.Status != events.ExecutionStatusTimedOut || completed.Stdout != "partial" {
		t.Errorf("result %+v completed %+v", result, completed)
	}
}

func TestSecondRunWhileActiveIsRejected(t *testing.T) {
	f := newRunFixture(sandbox.ExecuteResponse{Status: sandbox.StatusSucceeded})
	f.executor.gate, f.executor.entered = make(chan struct{}), make(chan struct{})
	session := activeCandidate()
	done := make(chan error, 1)
	go func() {
		_, err := f.service.Run(context.Background(), session, pythonRun())
		done <- err
	}()
	<-f.executor.entered

	if _, err := f.service.Run(context.Background(), session, pythonRun()); !errors.Is(err, ErrRunInProgress) {
		t.Errorf("second run err = %v, want ErrRunInProgress", err)
	}
	// The lock is per session: another session's run is not blocked.
	otherService := NewRunService(f.store, f.snapshots,
		&fakeExecutor{response: sandbox.ExecuteResponse{Status: sandbox.StatusSucceeded}}, f.lock, RunConfig{})
	if _, err := otherService.Run(context.Background(), activeCandidate(), pythonRun()); err != nil {
		t.Errorf("another session was blocked: %v", err)
	}

	close(f.executor.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	f.executor.entered = nil
	if _, err := f.service.Run(context.Background(), session, pythonRun()); err != nil {
		t.Errorf("run after the first finished: %v", err)
	}
}

func TestRunSandboxFailureRecordsError(t *testing.T) {
	f := newRunFixture(sandbox.ExecuteResponse{})
	f.executor.err = errors.New("sandbox down")
	result, err := f.service.Run(context.Background(), activeCandidate(), pythonRun())
	if err != nil {
		t.Fatal(err)
	}
	_, completed, _ := decodeRun(t, f.store)
	if result.Status != events.ExecutionStatusError || completed.Status != events.ExecutionStatusError || completed.ErrorCode != "sandbox_unavailable" {
		t.Errorf("result %+v completed %+v", result, completed)
	}
	if f.lock.released != 1 {
		t.Error("lock not released after a sandbox failure")
	}
}

func TestRunSurvivesClientDisconnect(t *testing.T) {
	f := newRunFixture(sandbox.ExecuteResponse{Status: sandbox.StatusSucceeded})
	f.executor.gate, f.executor.entered = make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := f.service.Run(ctx, activeCandidate(), pythonRun())
		done <- err
	}()
	<-f.executor.entered
	cancel()
	close(f.executor.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if f.executor.ctxErr != nil {
		t.Errorf("sandbox call saw cancellation: %v", f.executor.ctxErr)
	}
	decodeRun(t, f.store)
	if f.lock.released != 1 {
		t.Error("lock not released")
	}
}

func TestRunRejectsInvalidWithoutSideEffects(t *testing.T) {
	cases := []RunRequest{
		{Language: "cobol", Entrypoint: "main.py", Files: map[string]string{"main.py": "x"}},
		{Language: "python", Entrypoint: "missing.py", Files: map[string]string{"main.py": "x"}},
		{Language: "python", Entrypoint: "main.py", Files: map[string]string{"main.py": "x", "../escape": "x"}},
		{Language: "python", Entrypoint: "main.py", Files: map[string]string{"main.py": "\xff"}},
		{Language: "python", Entrypoint: "main.py", Files: map[string]string{"main.py": "x"}, Stdin: strings.Repeat("x", 65<<10)},
		{Language: "python", Entrypoint: "main.py"},
	}
	for i, request := range cases {
		f := newRunFixture(sandbox.ExecuteResponse{})
		if _, err := f.service.Run(context.Background(), activeCandidate(), request); !errors.Is(err, ErrInvalidRun) {
			t.Errorf("case %d: err = %v", i, err)
		}
		if len(f.lock.ttls) != 0 || len(f.snapshots.keys) != 0 || len(f.store.messages) != 0 {
			t.Errorf("case %d: side effects on an invalid run", i)
		}
	}
}

func TestRunSnapshotFailureRecordsNothing(t *testing.T) {
	f := newRunFixture(sandbox.ExecuteResponse{})
	f.snapshots.err = errors.New("s3 down")
	if _, err := f.service.Run(context.Background(), activeCandidate(), pythonRun()); err == nil {
		t.Fatal("snapshot failure not reported")
	}
	if len(f.store.messages) != 0 || f.executor.request.ExecutionID != "" || f.lock.released != 1 {
		t.Errorf("events=%d sandbox called=%v released=%d", len(f.store.messages), f.executor.request.ExecutionID != "", f.lock.released)
	}
}

func TestCapOutput(t *testing.T) {
	if out, truncated := capOutput("short", false); out != "short" || truncated {
		t.Error("short output changed")
	}
	long := strings.Repeat("a", eventOutputBytes-1) + "é" + "tail"
	out, truncated := capOutput(long, false)
	if !truncated || len(out) > eventOutputBytes || !utf8.ValidString(out) {
		t.Errorf("len=%d truncated=%v valid=%v", len(out), truncated, utf8.ValidString(out))
	}
}

func runServer(t *testing.T, service *RunService) *httptest.Server {
	t.Helper()
	session := activeCandidate()
	session.ExpiresAt = time.Now().Add(time.Hour)
	server := httptest.NewServer(NewHTTPHandler(nil, fixedFinder{session: session}).WithRuns(service).Routes())
	t.Cleanup(server.Close)
	return server
}

func postRun(t *testing.T, server *httptest.Server, body string) (int, string) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/session/run", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer opaque")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(payload)
}

func TestRunEndpointReturns429WhileRunActive(t *testing.T) {
	f := newRunFixture(sandbox.ExecuteResponse{Status: sandbox.StatusSucceeded, Stdout: "ok"})
	f.executor.gate, f.executor.entered = make(chan struct{}), make(chan struct{})
	server := runServer(t, f.service)
	body := `{"language":"python","entrypoint":"main.py","files":{"main.py":"print(1)"}}`
	first := make(chan string, 1)
	go func() {
		status, response := postRun(t, server, body)
		first <- strings.TrimSpace(response) + " " + http.StatusText(status)
	}()
	<-f.executor.entered
	status, response := postRun(t, server, body)
	if status != http.StatusTooManyRequests || !strings.Contains(response, "run_in_progress") {
		t.Errorf("second run: %d %s", status, response)
	}
	close(f.executor.gate)
	if got := <-first; !strings.Contains(got, `"status":"succeeded"`) || !strings.HasSuffix(got, "OK") {
		t.Errorf("first run: %s", got)
	}
}

func TestRunEndpointErrors(t *testing.T) {
	f := newRunFixture(sandbox.ExecuteResponse{})
	server := runServer(t, f.service)
	for body, want := range map[string]int{
		`not json`: http.StatusBadRequest,
		`{"language":"cobol","entrypoint":"a","files":{"a":"x"}}`:            http.StatusBadRequest,
		`{"language":"python","entrypoint":"a","files":{"a":"x"},"extra":1}`: http.StatusBadRequest,
	} {
		if status, response := postRun(t, server, body); status != want {
			t.Errorf("%s: %d %s", body, status, response)
		}
	}
	f.snapshots.err = errors.New("s3 down")
	if status, _ := postRun(t, server, `{"language":"python","entrypoint":"a.py","files":{"a.py":"x"}}`); status != http.StatusServiceUnavailable {
		t.Errorf("snapshot failure status = %d", status)
	}
}
