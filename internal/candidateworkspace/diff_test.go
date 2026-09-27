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

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
)

// memoryDiffStore mirrors the Postgres guard: accept only a client
// sequence greater than the last accepted one.
type memoryDiffStore struct {
	mu       sync.Mutex
	last     map[uuid.UUID]int64
	events   *memoryEventStore
	err      error
	messages []outbox.Message
}

func (s *memoryDiffStore) RecordDiff(ctx context.Context, companyID, sessionID, interviewID uuid.UUID, clientSequence int64,
	build func(int64) (outbox.Message, error)) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, s.err
	}
	if s.last == nil {
		s.last = map[uuid.UUID]int64{}
	}
	if clientSequence <= s.last[sessionID] {
		return 0, ErrDiffOutOfOrder
	}
	s.last[sessionID] = clientSequence
	return s.events.RecordEvents(ctx, companyID, interviewID, 1, func(first int64) ([]outbox.Message, error) {
		message, err := build(first)
		return []outbox.Message{message}, err
	})
}

const samplePatch = "--- a/main.py\n+++ b/main.py\n@@ -1,2 +1,3 @@\n-print(1)\n+print(2)\n+print(3)\n x = 1\n"

func diffRequest(seq int64, origin string) DiffRequest {
	return DiffRequest{ClientSequence: seq, Origin: origin, Path: "main.py", Patch: samplePatch}
}

func TestSubmitDiffRecordsCodeDiff(t *testing.T) {
	recorded := &memoryEventStore{}
	service := NewDiffService(&memoryDiffStore{events: recorded})
	session := activeCandidate()
	promptID := uuid.NewString()
	request := diffRequest(1, events.DiffOriginAIApplied)
	request.PromptID = promptID
	result, err := service.Submit(context.Background(), session, request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || result.SequenceNumber != 1 || result.LinesAdded != 2 || result.LinesRemoved != 1 {
		t.Errorf("result = %+v", result)
	}
	envelopes := recorded.envelopes(t)
	var diff events.CodeDiff
	if err := envelopes[0].Decode(&diff); err != nil {
		t.Fatal(err)
	}
	if diff.Origin != events.DiffOriginAIApplied || diff.PromptID != promptID || diff.ClientSequence != 1 ||
		diff.LinesAdded != 2 || diff.LinesRemoved != 1 || diff.Patch != samplePatch || diff.Path != "main.py" ||
		diff.SessionID != session.ID.String() || diff.InterviewID != session.InterviewID.String() {
		t.Errorf("code.diff = %+v", diff)
	}
}

func TestSubmitDiffOriginIsPreserved(t *testing.T) {
	recorded := &memoryEventStore{}
	service := NewDiffService(&memoryDiffStore{events: recorded})
	session := activeCandidate()
	for i, origin := range []string{events.DiffOriginManual, events.DiffOriginAIApplied, events.DiffOriginManual} {
		if _, err := service.Submit(context.Background(), session, diffRequest(int64(i+1), origin)); err != nil {
			t.Fatal(err)
		}
	}
	var origins []string
	for _, envelope := range recorded.envelopes(t) {
		var diff events.CodeDiff
		_ = envelope.Decode(&diff)
		origins = append(origins, diff.Origin)
	}
	if strings.Join(origins, ",") != "manual,ai_applied,manual" {
		t.Errorf("origins = %v", origins)
	}
}

func TestSubmitDiffDropsOutOfOrder(t *testing.T) {
	recorded := &memoryEventStore{}
	service := NewDiffService(&memoryDiffStore{events: recorded})
	session := activeCandidate()
	var accepted []int64
	for _, seq := range []int64{1, 3, 2, 3, 5, 4} {
		result, err := service.Submit(context.Background(), session, diffRequest(seq, events.DiffOriginManual))
		if err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
		if result.Accepted {
			accepted = append(accepted, seq)
		} else if result.Reason != "out_of_order" {
			t.Errorf("seq %d dropped with reason %q", seq, result.Reason)
		}
	}
	if len(accepted) != 3 || accepted[0] != 1 || accepted[1] != 3 || accepted[2] != 5 {
		t.Errorf("accepted = %v, want [1 3 5]", accepted)
	}
	if n := len(recorded.envelopes(t)); n != 3 {
		t.Errorf("recorded %d code.diff events, want 3", n)
	}
	// Another session's sequence is independent.
	if result, _ := service.Submit(context.Background(), activeCandidate(), diffRequest(1, events.DiffOriginManual)); !result.Accepted {
		t.Error("a different session's first diff was dropped")
	}
}

func TestSubmitDiffRejectsInvalid(t *testing.T) {
	cases := map[string]func(*DiffRequest){
		"zero seq":           func(r *DiffRequest) { r.ClientSequence = 0 },
		"bad origin":         func(r *DiffRequest) { r.Origin = "copilot" },
		"manual with prompt": func(r *DiffRequest) { r.PromptID = uuid.NewString() },
		"bad prompt id": func(r *DiffRequest) {
			r.Origin, r.PromptID = events.DiffOriginAIApplied, "nope"
		},
		"abs path":    func(r *DiffRequest) { r.Path = "/etc/passwd" },
		"escape path": func(r *DiffRequest) { r.Path = "../x" },
		"empty patch": func(r *DiffRequest) { r.Patch = "" },
		"huge patch":  func(r *DiffRequest) { r.Patch = "@@ -1 +1 @@\n-a\n+" + strings.Repeat("b", maxPatchBytes) + "\n" },
		"not a diff":  func(r *DiffRequest) { r.Patch = "print(1)\n" },
		"binary":      func(r *DiffRequest) { r.Patch = "@@ -1 +1 @@\n-a\n+\x00\n" },
		"bad utf8":    func(r *DiffRequest) { r.Patch = "@@ -1 +1 @@\n-a\n+\xff\n" },
	}
	for name, breakIt := range cases {
		store := &memoryDiffStore{events: &memoryEventStore{}}
		request := diffRequest(1, events.DiffOriginManual)
		breakIt(&request)
		if _, err := NewDiffService(store).Submit(context.Background(), activeCandidate(), request); !errors.Is(err, ErrInvalidDiff) {
			t.Errorf("%s: err = %v", name, err)
		}
		if len(store.last) != 0 {
			t.Errorf("%s: an invalid diff advanced the sequence", name)
		}
	}
}

func diffServer(t *testing.T, store DiffStore) *httptest.Server {
	t.Helper()
	session := activeCandidate()
	session.ExpiresAt = time.Now().Add(time.Hour)
	server := httptest.NewServer(NewHTTPHandler(nil, fixedFinder{session: session}).WithDiffs(NewDiffService(store)).Routes())
	t.Cleanup(server.Close)
	return server
}

func postDiff(t *testing.T, server *httptest.Server, body string) (int, string) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/session/diff", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer opaque")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(payload)
}

func TestDiffEndpoint(t *testing.T) {
	store := &memoryDiffStore{events: &memoryEventStore{}}
	server := diffServer(t, store)
	body := func(seq int) string {
		return `{"clientSeq":` + string(rune('0'+seq)) + `,"origin":"manual","path":"main.py","patch":"@@ -1 +1 @@\n-a\n+b\n"}`
	}
	if status, payload := postDiff(t, server, body(2)); status != http.StatusAccepted || !strings.Contains(payload, `"accepted":true`) {
		t.Errorf("first: %d %s", status, payload)
	}
	if status, payload := postDiff(t, server, body(1)); status != http.StatusOK || !strings.Contains(payload, `"reason":"out_of_order"`) {
		t.Errorf("stale: %d %s", status, payload)
	}
	for _, bad := range []string{`nope`, `{"clientSeq":3,"origin":"x","path":"a","patch":"@@ -1 +1 @@\n-a\n+b\n"}`, `{"clientSeq":3,"extra":1}`} {
		if status, _ := postDiff(t, server, bad); status != http.StatusBadRequest {
			t.Errorf("%s: %d", bad, status)
		}
	}
	store.err = errors.New("db down")
	if status, _ := postDiff(t, server, body(5)); status != http.StatusServiceUnavailable {
		t.Errorf("store failure: %d", status)
	}
}
