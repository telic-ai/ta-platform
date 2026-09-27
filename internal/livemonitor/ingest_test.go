package livemonitor

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
)

func envelope(t *testing.T, stream Stream, seq int64) []byte {
	t.Helper()
	e, err := events.New(stream.CompanyID.String(), seq, events.CodeDiff{SessionID: "s", InterviewID: stream.InterviewID.String(), Path: "main.go"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestDecodeMessage(t *testing.T) {
	stream := newStream()
	got, e, err := DecodeMessage(envelope(t, stream, 7), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got != stream || e.SequenceNumber != 7 || e.EventType != "code.diff" || e.CompanyID != stream.CompanyID.String() ||
		e.InterviewID != stream.InterviewID.String() || e.EventID == "" || len(e.Payload) == 0 {
		t.Fatalf("decoded %+v %+v", got, e)
	}
	for name, value := range map[string][]byte{
		"not json":         []byte("nope"),
		"no interview":     []byte(`{"event_id":"e","event_type":"x","company_id":"` + uuid.NewString() + `","sequence_number":1,"occurred_at":"2026-01-01T00:00:00Z","payload":{}}`),
		"company not uuid": []byte(`{"event_id":"e","event_type":"x","company_id":"acme","sequence_number":1,"occurred_at":"2026-01-01T00:00:00Z","payload":{"interview_id":"` + uuid.NewString() + `"}}`),
		"interview not uuid": []byte(`{"event_id":"e","event_type":"x","company_id":"` + uuid.NewString() +
			`","sequence_number":1,"occurred_at":"2026-01-01T00:00:00Z","payload":{"interview_id":"i-1"}}`),
	} {
		if _, _, err := DecodeMessage(value, time.Now()); err == nil {
			t.Errorf("%s decoded", name)
		}
	}
}

type fakeReader struct {
	mu        sync.Mutex
	messages  []kafkago.Message
	committed []int64
	fetchErr  error
	commitErr error
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	if len(r.messages) == 0 {
		r.mu.Unlock()
		if r.fetchErr != nil {
			return kafkago.Message{}, r.fetchErr
		}
		<-ctx.Done()
		return kafkago.Message{}, ctx.Err()
	}
	m := r.messages[0]
	r.messages = r.messages[1:]
	r.mu.Unlock()
	return m, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, messages ...kafkago.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.commitErr != nil {
		return r.commitErr
	}
	for _, m := range messages {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

type failingBus struct {
	Bus
	err error
}

func (b failingBus) Publish(context.Context, Stream, Event) (bool, error) { return false, b.err }

func TestIngesterPublishesThenCommits(t *testing.T) {
	stream := newStream()
	bus := NewMemoryBus(10)
	reader := &fakeReader{messages: []kafkago.Message{
		{Offset: 1, Value: envelope(t, stream, 1)},
		{Offset: 2, Value: []byte("garbage")},
		{Offset: 3, Value: envelope(t, stream, 2)},
		{Offset: 4, Value: envelope(t, stream, 2)}, // redelivery
	}}
	var invalid []int64
	ingester := NewIngester(reader, bus)
	ingester.OnInvalid = func(m kafkago.Message, _ error) { invalid = append(invalid, m.Offset) }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ingester.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for len(committedSnapshot(reader)) < 4 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if !equal(reader.committed, []int64{1, 2, 3, 4}) || !equal(invalid, []int64{2}) {
		t.Fatalf("committed %v invalid %v", reader.committed, invalid)
	}
	events, _, _ := bus.Recent(context.Background(), stream, 0)
	if !equal(seqs(events), []int64{1, 2}) {
		t.Fatalf("ring = %v", seqs(events))
	}
}

func committedSnapshot(r *fakeReader) []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.committed...)
}

func TestIngesterDoesNotCommitWhenBusFails(t *testing.T) {
	stream := newStream()
	reader := &fakeReader{messages: []kafkago.Message{{Offset: 1, Value: envelope(t, stream, 1)}}}
	boom := errors.New("redis down")
	err := NewIngester(reader, failingBus{err: boom}).Run(context.Background())
	if !errors.Is(err, boom) || len(reader.committed) != 0 {
		t.Fatalf("Run = %v, committed %v", err, reader.committed)
	}
}

func TestIngesterSurfacesKafkaErrors(t *testing.T) {
	boom := errors.New("broker gone")
	if err := NewIngester(&fakeReader{fetchErr: boom}, NewMemoryBus(1)).Run(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("fetch err = %v", err)
	}
	stream := newStream()
	reader := &fakeReader{messages: []kafkago.Message{{Value: envelope(t, stream, 1)}}, commitErr: boom}
	if err := NewIngester(reader, NewMemoryBus(1)).Run(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("commit err = %v", err)
	}
}
