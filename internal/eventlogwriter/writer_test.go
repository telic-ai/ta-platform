package eventlogwriter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
)

func TestDecodePromotesCatalogPayloadColumns(t *testing.T) {
	envelope, err := events.New("company-1", 9, events.InterviewCompleted{
		InterviewID: "interview-1", ApplicationID: "application-1", Outcome: "advance", Feedback: "strong",
	})
	if err != nil {
		t.Fatal(err)
	}
	value, _ := json.Marshal(envelope)
	row, err := Decode(value, time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if row.InterviewID != "interview-1" || row.ApplicationID == nil || *row.ApplicationID != "application-1" {
		t.Fatalf("promoted row = %+v", row)
	}
	if row.Outcome == nil || *row.Outcome != "advance" || row.Payload == "" {
		t.Fatalf("outcome/payload not preserved: %+v", row)
	}
}

func TestWriterInsertsBatchBeforeCommittingOffsets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &fakeReader{messages: []kafkago.Message{
		{Partition: 0, Offset: 1, Value: eventValue(t, "interview-1", 1)},
		{Partition: 0, Offset: 2, Value: eventValue(t, "interview-1", 2)},
	}, cancel: cancel}
	store := &fakeInserter{}
	writer, err := New(reader, store, Config{BatchSize: 2, BatchWait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
	if len(store.rows) != 2 {
		t.Fatalf("inserted rows = %d, want 2", len(store.rows))
	}
	if reader.committed != 2 {
		t.Fatalf("committed messages = %d, want 2", reader.committed)
	}
}

func TestWriterDoesNotCommitFailedInsert(t *testing.T) {
	reader := &fakeReader{messages: []kafkago.Message{{Value: eventValue(t, "interview-1", 1)}}}
	insertErr := errors.New("ClickHouse unavailable")
	writer, err := New(reader, &fakeInserter{err: insertErr}, Config{BatchSize: 1, BatchWait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Run(context.Background())
	if !errors.Is(err, insertErr) {
		t.Fatalf("Run error = %v", err)
	}
	if reader.committed != 0 {
		t.Fatalf("committed messages = %d, want 0", reader.committed)
	}
}

func eventValue(t *testing.T, interviewID string, sequence int64) []byte {
	t.Helper()
	envelope, err := events.New("company-1", sequence, events.InterviewCompleted{InterviewID: interviewID})
	if err != nil {
		t.Fatal(err)
	}
	value, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return value
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

type fakeInserter struct {
	rows []Row
	err  error
}

func (s *fakeInserter) Insert(_ context.Context, rows []Row) error {
	s.rows = append(s.rows, rows...)
	return s.err
}
