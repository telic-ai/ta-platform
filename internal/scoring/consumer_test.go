package scoring

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
)

func TestConsumerScoresTriggersAndCommitsEverything(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &fakeReader{cancelAfter: 3, cancel: cancel, messages: []kafkago.Message{
		{Offset: 1, Value: value(t, 1, events.SessionStarted{SessionID: sessionID.String(), InterviewID: interviewID.String()})},
		{Offset: 2, Value: []byte("junk")},
		{Offset: 3, Value: value(t, 4, events.SessionSubmitted{SessionID: sessionID.String(), InterviewID: interviewID.String()})},
	}}
	scorer := &fakeScorer{results: []error{nil}}
	var invalid []int64
	consumer := newTestConsumer(t, reader, scorer, func(m kafkago.Message, _ error) { invalid = append(invalid, m.Offset) })
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if len(scorer.calls) != 1 || scorer.calls[0].SequenceNumber != 4 {
		t.Fatalf("scored = %+v", scorer.calls)
	}
	if len(invalid) != 1 || invalid[0] != 2 || reader.committed != 3 {
		t.Fatalf("invalid=%v committed=%d", invalid, reader.committed)
	}
}

func TestConsumerRetriesBehindWithBackoffThenScoresFinal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &fakeReader{cancelAfter: 1, cancel: cancel, messages: []kafkago.Message{
		{Value: value(t, 4, events.SessionSubmitted{SessionID: sessionID.String(), InterviewID: interviewID.String()})},
	}}
	scorer := &fakeScorer{results: []error{ErrBehind, ErrBehind, ErrBehind, nil}}
	consumer := newTestConsumer(t, reader, scorer, nil)
	var slept []time.Duration
	consumer.sleep = func(_ context.Context, d time.Duration) error {
		if reader.committed != 0 {
			t.Error("offset committed while the trigger was still behind")
		}
		slept = append(slept, d)
		return nil
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	if len(slept) != 3 || slept[0] != want[0] || slept[1] != want[1] || slept[2] != want[2] {
		t.Fatalf("backoff = %v, want %v", slept, want)
	}
	if finals := scorer.finals; len(finals) != 4 || finals[2] || !finals[3] {
		t.Fatalf("final flags = %v, want only the 4th attempt final", finals)
	}
	if reader.committed != 1 {
		t.Fatalf("committed = %d", reader.committed)
	}
}

func TestConsumerStopsOnTransientErrorWithoutCommitting(t *testing.T) {
	down := errors.New("postgres down")
	reader := &fakeReader{messages: []kafkago.Message{
		{Value: value(t, 4, events.SessionSubmitted{SessionID: sessionID.String(), InterviewID: interviewID.String()})},
	}}
	consumer := newTestConsumer(t, reader, &fakeScorer{results: []error{down}}, nil)
	if err := consumer.Run(context.Background()); !errors.Is(err, down) {
		t.Fatalf("Run = %v", err)
	}
	if reader.committed != 0 {
		t.Fatal("committed a trigger that failed transiently")
	}
}

func TestConsumerSkipsPermanentFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &fakeReader{cancelAfter: 1, cancel: cancel, messages: []kafkago.Message{
		{Value: value(t, 4, events.SessionSubmitted{SessionID: sessionID.String(), InterviewID: interviewID.String()})},
	}}
	var invalid int
	consumer := newTestConsumer(t, reader, &fakeScorer{results: []error{&PermanentError{Err: ErrInterviewGone}}}, func(kafkago.Message, error) { invalid++ })
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if invalid != 1 || reader.committed != 1 {
		t.Fatalf("invalid=%d committed=%d", invalid, reader.committed)
	}
}

func TestBackoffIsCapped(t *testing.T) {
	consumer := newTestConsumer(t, &fakeReader{}, &fakeScorer{}, nil)
	if got := consumer.Backoff(30); got != 50*time.Millisecond {
		t.Fatalf("Backoff(30) = %v, want the 50ms cap", got)
	}
}

func TestNewConsumerValidatesConfig(t *testing.T) {
	if _, err := NewConsumer(&fakeReader{}, &fakeScorer{}, ConsumerConfig{MaxAttempts: 1, BaseBackoff: time.Second, MaxBackoff: time.Millisecond}); err == nil {
		t.Fatal("max backoff below base accepted")
	}
}

func newTestConsumer(t *testing.T, reader MessageReader, scorer Scorer, onInvalid func(kafkago.Message, error)) *Consumer {
	t.Helper()
	consumer, err := NewConsumer(reader, scorer, ConsumerConfig{
		MaxAttempts: 4, BaseBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, OnInvalid: onInvalid,
	})
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}

func value(t *testing.T, sequence int64, payload events.Payload) []byte {
	t.Helper()
	envelope, err := events.New(companyID.String(), sequence, payload)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(envelope)
	return b
}

type fakeScorer struct {
	results []error
	calls   []Trigger
	finals  []bool
}

func (s *fakeScorer) Score(_ context.Context, trigger Trigger, final bool) (Outcome, error) {
	s.calls = append(s.calls, trigger)
	s.finals = append(s.finals, final)
	if len(s.results) == 0 {
		return OutcomeScored, nil
	}
	err := s.results[0]
	s.results = s.results[1:]
	if err != nil {
		return "", err
	}
	return OutcomeScored, nil
}

type fakeReader struct {
	messages    []kafkago.Message
	committed   int
	cancelAfter int
	cancel      context.CancelFunc
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
	if r.cancel != nil && r.committed >= r.cancelAfter {
		r.cancel()
	}
	return nil
}

func (r *fakeReader) Close() error { return nil }
