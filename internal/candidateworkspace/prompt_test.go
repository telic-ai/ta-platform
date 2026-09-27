package candidateworkspace

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
)

// fakeCompleter records the request and replays deltas; if block is set it
// waits for ctx cancellation instead of finishing.
type fakeCompleter struct {
	store     *memoryEventStore
	request   aigateway.CompleteRequest
	recorded  int // outbox messages already written when Complete was called
	deltas    []string
	outcome   aigateway.Outcome
	err       error
	block     bool
	cancelled chan struct{}
}

func (f *fakeCompleter) Complete(ctx context.Context, request aigateway.CompleteRequest, onDelta func(string) error) (aigateway.Outcome, error) {
	f.request = request
	if f.store != nil {
		f.store.mu.Lock()
		f.recorded = len(f.store.messages)
		f.store.mu.Unlock()
	}
	for _, delta := range f.deltas {
		if err := onDelta(delta); err != nil {
			return aigateway.Outcome{}, err
		}
	}
	if f.block {
		<-ctx.Done()
		if f.cancelled != nil {
			close(f.cancelled)
		}
		return aigateway.Outcome{}, ctx.Err()
	}
	return f.outcome, f.err
}

func activeCandidate() domain.Session {
	interviewID := uuid.New()
	return domain.Session{ID: uuid.New(), CompanyID: uuid.New(), InterviewID: &interviewID, State: domain.SessionStateActive}
}

func TestSubmitRecordsPromptBeforeCallingGateway(t *testing.T) {
	store := &memoryEventStore{}
	gateway := &fakeCompleter{store: store, deltas: []string{"a", "b"}, outcome: aigateway.Outcome{Status: events.AIResponseStatusCompleted}}
	service := NewPromptService(store, gateway, PromptConfig{Model: "m", System: "sys", MaxTokens: 99})
	session := activeCandidate()

	var accepted PromptAccepted
	var order []string
	outcome, err := service.Submit(context.Background(), session,
		PromptRequest{Prompt: "fix my loop", History: []aigateway.Message{{Role: "user", Content: "hi"}, {Role: "assistant", Content: "hello"}}},
		func(a PromptAccepted) error { accepted = a; order = append(order, "accepted"); return nil },
		func(d string) error { order = append(order, d); return nil })
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if outcome.Status != events.AIResponseStatusCompleted {
		t.Errorf("outcome = %+v", outcome)
	}
	if strings.Join(order, ",") != "accepted,a,b" {
		t.Errorf("callback order = %v", order)
	}
	if gateway.recorded != 1 {
		t.Fatalf("gateway called with %d outbox messages recorded, want prompt.submitted first", gateway.recorded)
	}

	envelopes := store.envelopes(t)
	var prompt events.PromptSubmitted
	if err := envelopes[0].Decode(&prompt); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if envelopes[0].SequenceNumber != accepted.SequenceNumber || prompt.PromptID != accepted.PromptID {
		t.Errorf("accepted %+v does not match event %+v", accepted, prompt)
	}
	if gateway.request.SequenceNumber <= envelopes[0].SequenceNumber ||
		gateway.request.SequenceNumber != prompt.CompletionSequenceNumber {
		t.Errorf("completion seq %d must follow prompt seq %d (reserved %d)",
			gateway.request.SequenceNumber, envelopes[0].SequenceNumber, prompt.CompletionSequenceNumber)
	}
	request := gateway.request
	if request.PromptID != prompt.PromptID || request.SessionID != session.ID.String() ||
		request.CompanyID != session.CompanyID.String() || request.InterviewID != session.InterviewID.String() {
		t.Errorf("gateway identity = %+v", request)
	}
	if request.Mode != aigateway.ModeManaged || request.Model != "m" || request.System != "sys" || request.MaxTokens != 99 {
		t.Errorf("gateway config = %+v", request)
	}
	if len(request.Messages) != 3 || request.Messages[2] != (aigateway.Message{Role: "user", Content: "fix my loop"}) {
		t.Errorf("messages = %+v", request.Messages)
	}
	if prompt.Prompt != "fix my loop" || prompt.HistoryTurns != 2 {
		t.Errorf("prompt event = %+v", prompt)
	}
}

func TestSubmitSequencesSuccessivePrompts(t *testing.T) {
	store := &memoryEventStore{}
	gateway := &fakeCompleter{}
	service := NewPromptService(store, gateway, PromptConfig{})
	session := activeCandidate()
	var seqs []int64
	for range 2 {
		_, err := service.Submit(context.Background(), session, PromptRequest{Prompt: "x"},
			func(a PromptAccepted) error { seqs = append(seqs, a.SequenceNumber); return nil },
			func(string) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, gateway.request.SequenceNumber)
	}
	for i, want := range []int64{1, 2, 3, 4} {
		if seqs[i] != want {
			t.Fatalf("sequence numbers = %v, want 1..4", seqs)
		}
	}
}

func TestSubmitRejectsInvalidPromptWithoutSideEffects(t *testing.T) {
	cases := []PromptRequest{
		{Prompt: "   "},
		{Prompt: strings.Repeat("x", maxPromptBytes+1)},
		{Prompt: "\xff"},
		{Prompt: "x", History: []aigateway.Message{{Role: "system", Content: "x"}}},
		{Prompt: "x", History: []aigateway.Message{{Role: "user", Content: ""}}},
		{Prompt: "x", History: make([]aigateway.Message, maxHistoryTurns+1)},
		{Prompt: "x", History: []aigateway.Message{{Role: "user", Content: strings.Repeat("y", maxHistoryBytes+1)}}},
	}
	for i, request := range cases {
		store := &memoryEventStore{}
		gateway := &fakeCompleter{}
		_, err := NewPromptService(store, gateway, PromptConfig{}).Submit(context.Background(), activeCandidate(), request,
			func(PromptAccepted) error { t.Fatal("accepted an invalid prompt"); return nil }, func(string) error { return nil })
		if !errors.Is(err, ErrInvalidPrompt) {
			t.Errorf("case %d: err = %v, want ErrInvalidPrompt", i, err)
		}
		if len(store.messages) != 0 {
			t.Errorf("case %d: recorded an event for an invalid prompt", i)
		}
	}
}

func TestSubmitDoesNotCallGatewayWhenRecordFails(t *testing.T) {
	store := &memoryEventStore{err: errors.New("db down")}
	gateway := &fakeCompleter{}
	_, err := NewPromptService(store, gateway, PromptConfig{}).Submit(context.Background(), activeCandidate(),
		PromptRequest{Prompt: "x"}, func(PromptAccepted) error { return nil }, func(string) error { return nil })
	if err == nil || gateway.request.PromptID != "" {
		t.Errorf("err = %v, gateway called = %v", err, gateway.request.PromptID != "")
	}
}

func TestSubmitUsesCompanyMode(t *testing.T) {
	gateway := &fakeCompleter{}
	session := activeCandidate()
	service := NewPromptService(&memoryEventStore{}, gateway, PromptConfig{Mode: func(_ context.Context, company uuid.UUID) (string, error) {
		if company != session.CompanyID {
			t.Errorf("mode asked for company %s", company)
		}
		return aigateway.ModeBYOK, nil
	}})
	if _, err := service.Submit(context.Background(), session, PromptRequest{Prompt: "x"},
		func(PromptAccepted) error { return nil }, func(string) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if gateway.request.Mode != aigateway.ModeBYOK {
		t.Errorf("mode = %q, want byok", gateway.request.Mode)
	}
}

func TestSubmitRequiresCandidateInterview(t *testing.T) {
	session := activeCandidate()
	session.InterviewID = nil
	_, err := NewPromptService(&memoryEventStore{}, &fakeCompleter{}, PromptConfig{}).Submit(context.Background(), session,
		PromptRequest{Prompt: "x"}, func(PromptAccepted) error { return nil }, func(string) error { return nil })
	if err == nil {
		t.Error("accepted a session without an interview")
	}
}
