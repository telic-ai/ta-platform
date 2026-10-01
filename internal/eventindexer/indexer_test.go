package eventindexer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/store/typesense"
)

func TestDecodeIndexesPromptResponseAndDiffText(t *testing.T) {
	tests := []struct {
		name    string
		payload events.Payload
		want    Document
	}{
		{
			name:    "prompt",
			payload: events.PromptSubmitted{SessionID: "s1", InterviewID: "i1", PromptID: "p1", Prompt: "explain binary search"},
			want:    Document{SessionID: "s1", InterviewID: "i1", EventType: "prompt.submitted", PromptID: "p1", Text: "explain binary search"},
		},
		{
			name:    "response",
			payload: events.AIResponseCompleted{SessionID: "s1", InterviewID: "i1", PromptID: "p1", ResponseText: "split the range in half", Status: "completed"},
			want:    Document{SessionID: "s1", InterviewID: "i1", EventType: "ai.response.completed", PromptID: "p1", Text: "split the range in half"},
		},
		{
			name:    "diff",
			payload: events.CodeDiff{SessionID: "s1", InterviewID: "i1", Origin: events.DiffOriginAIApplied, PromptID: "p1", Path: "main.go", Patch: "+func search() {}"},
			want:    Document{SessionID: "s1", InterviewID: "i1", EventType: "code.diff", PromptID: "p1", Text: "+func search() {}", Path: "main.go", Origin: "ai_applied"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			envelope := envelopeFor(t, 7, tc.payload)
			doc, indexed, err := Decode(marshal(t, envelope))
			if err != nil || !indexed {
				t.Fatalf("Decode = %v, indexed %v", err, indexed)
			}
			tc.want.ID, tc.want.CompanyID, tc.want.SequenceNumber = envelope.EventID, "company-1", 7
			tc.want.OccurredAt = envelope.OccurredAt.UnixMilli()
			if doc != tc.want {
				t.Fatalf("doc = %+v\nwant %+v", doc, tc.want)
			}
		})
	}
}

func TestDecodeSkipsEventsWithoutText(t *testing.T) {
	for _, payload := range []events.Payload{
		events.SessionStarted{SessionID: "s1", InterviewID: "i1"},
		events.ExecutionRequested{SessionID: "s1", InterviewID: "i1"},
		events.AIResponseCompleted{SessionID: "s1", InterviewID: "i1", Status: "error"},
	} {
		_, indexed, err := Decode(marshal(t, envelopeFor(t, 1, payload)))
		if err != nil || indexed {
			t.Errorf("%s: indexed=%v err=%v", payload.EventType(), indexed, err)
		}
	}
}

func TestDecodeRejectsInvalidEvents(t *testing.T) {
	missingInterview := marshal(t, envelopeFor(t, 1, events.PromptSubmitted{SessionID: "s1", Prompt: "hi"}))
	noID := envelopeFor(t, 1, events.PromptSubmitted{InterviewID: "i1", Prompt: "hi"})
	noID.EventID = ""
	badPayload := envelopeFor(t, 1, events.PromptSubmitted{InterviewID: "i1", Prompt: "hi"})
	badPayload.Payload = json.RawMessage(`{"prompt": 5}`)
	for name, value := range map[string][]byte{
		"not json":          []byte("nope"),
		"missing interview": missingInterview,
		"missing event id":  marshal(t, noID),
		"bad payload":       marshal(t, badPayload),
	} {
		if _, _, err := Decode(value); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestIndexerUpsertsBatchBeforeCommitting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	prompt := envelopeFor(t, 1, events.PromptSubmitted{SessionID: "s1", InterviewID: "i1", Prompt: "hi"})
	reader := &fakeReader{cancel: cancel, messages: []kafkago.Message{
		{Offset: 1, Value: marshal(t, prompt)},
		{Offset: 2, Value: marshal(t, envelopeFor(t, 2, events.SessionStarted{SessionID: "s1", InterviewID: "i1"}))},
		{Offset: 3, Value: marshal(t, envelopeFor(t, 3, events.CodeDiff{SessionID: "s1", InterviewID: "i1", Patch: "+x"}))},
	}}
	upserter := &fakeUpserter{}
	indexer, err := New(reader, upserter, Config{BatchSize: 3, BatchWait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := indexer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if len(upserter.calls) != 1 || len(upserter.calls[0]) != 2 {
		t.Fatalf("upsert calls = %+v, want one batch of 2 text events", upserter.calls)
	}
	if upserter.calls[0][0].(Document).ID != prompt.EventID {
		t.Fatalf("document id is not the event id: %+v", upserter.calls[0][0])
	}
	if reader.committed != 3 {
		t.Fatalf("committed = %d, want all 3 (non-text events are consumed too)", reader.committed)
	}
}

func TestIndexerDoesNotCommitFailedUpsert(t *testing.T) {
	reader := &fakeReader{messages: []kafkago.Message{
		{Value: marshal(t, envelopeFor(t, 1, events.PromptSubmitted{InterviewID: "i1", Prompt: "hi"}))},
	}}
	unavailable := errors.New("typesense down")
	indexer, _ := New(reader, &fakeUpserter{err: unavailable}, Config{BatchSize: 1, BatchWait: time.Second})
	if err := indexer.Run(context.Background()); !errors.Is(err, unavailable) {
		t.Fatalf("Run = %v", err)
	}
	if reader.committed != 0 {
		t.Fatalf("committed = %d, want 0", reader.committed)
	}
}

func TestIndexerReportsRejectedAndUndecodableThenCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &fakeReader{cancel: cancel, messages: []kafkago.Message{
		{Offset: 1, Value: []byte("junk")},
		{Offset: 2, Value: marshal(t, envelopeFor(t, 1, events.PromptSubmitted{InterviewID: "i1", Prompt: "a"}))},
		{Offset: 3, Value: marshal(t, envelopeFor(t, 2, events.PromptSubmitted{InterviewID: "i1", Prompt: "b"}))},
	}}
	upserter := &fakeUpserter{err: &typesense.ImportError{Failures: []typesense.ImportFailure{{Index: 1, Error: "bad"}}}}
	var invalid []int64
	indexer, _ := New(reader, upserter, Config{
		BatchSize: 3, BatchWait: time.Second,
		OnInvalid: func(m kafkago.Message, _ error) { invalid = append(invalid, m.Offset) },
	})
	if err := indexer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if len(invalid) != 2 || invalid[0] != 1 || invalid[1] != 3 {
		t.Fatalf("invalid offsets = %v, want [1 3]", invalid)
	}
	if reader.committed != 3 {
		t.Fatalf("committed = %d, want 3", reader.committed)
	}
}

func TestIndexerFlushesPartialBatchAfterWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &fakeReader{cancel: cancel, messages: []kafkago.Message{
		{Value: marshal(t, envelopeFor(t, 1, events.PromptSubmitted{InterviewID: "i1", Prompt: "a"}))},
	}}
	upserter := &fakeUpserter{}
	indexer, _ := New(reader, upserter, Config{BatchSize: 100, BatchWait: 10 * time.Millisecond})
	if err := indexer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if len(upserter.calls) != 1 || reader.committed != 1 {
		t.Fatalf("calls=%d committed=%d", len(upserter.calls), reader.committed)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := New(&fakeReader{}, &fakeUpserter{}, Config{BatchSize: 0, BatchWait: time.Second}); err == nil {
		t.Error("zero batch size accepted")
	}
	if _, err := New(nil, &fakeUpserter{}, Config{BatchSize: 1, BatchWait: time.Second}); err == nil {
		t.Error("nil reader accepted")
	}
}

func envelopeFor(t *testing.T, sequence int64, payload events.Payload) events.Envelope {
	t.Helper()
	envelope, err := events.New("company-1", sequence, payload)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func marshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type fakeReader struct {
	messages  []kafkago.Message
	committed int
	cancel    context.CancelFunc
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	if len(r.messages) == 0 {
		<-ctx.Done()
		return kafkago.Message{}, ctx.Err()
	}
	message := r.messages[0]
	r.messages = r.messages[1:]
	return message, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, messages ...kafkago.Message) error {
	r.committed += len(messages)
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

func (r *fakeReader) Close() error { return nil }

type fakeUpserter struct {
	calls [][]any
	err   error
}

func (u *fakeUpserter) Upsert(_ context.Context, _ string, documents []any) error {
	u.calls = append(u.calls, documents)
	return u.err
}
