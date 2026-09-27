// Package eventlogwriter consumes catalog events and persists their immutable
// log projection in ClickHouse.
package eventlogwriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
)

const DefaultTopic = "ta.interview.events"

type MessageReader interface {
	FetchMessage(context.Context) (kafkago.Message, error)
	CommitMessages(context.Context, ...kafkago.Message) error
	Close() error
}

type Inserter interface {
	Insert(context.Context, []Row) error
}

type Config struct {
	BatchSize int
	BatchWait time.Duration
}

type Writer struct {
	reader   MessageReader
	inserter Inserter
	config   Config
	now      func() time.Time
}

func New(reader MessageReader, inserter Inserter, config Config) (*Writer, error) {
	if reader == nil {
		return nil, errors.New("event-log-writer: reader is required")
	}
	if inserter == nil {
		return nil, errors.New("event-log-writer: inserter is required")
	}
	if config.BatchSize <= 0 {
		return nil, errors.New("event-log-writer: batch size must be positive")
	}
	if config.BatchWait <= 0 {
		return nil, errors.New("event-log-writer: batch wait must be positive")
	}
	return &Writer{reader: reader, inserter: inserter, config: config, now: time.Now}, nil
}

func (w *Writer) Close() error { return w.reader.Close() }

// Run provides at-least-once delivery: Kafka offsets are committed only after
// ClickHouse acknowledges the complete batch. A crash before the commit causes
// redelivery, which the events table's ReplacingMergeTree sort key deduplicates.
func (w *Writer) Run(ctx context.Context) error {
	for {
		first, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("event-log-writer: fetch event: %w", err)
		}

		messages := []kafkago.Message{first}
		batchCtx, cancel := context.WithTimeout(ctx, w.config.BatchWait)
		for len(messages) < w.config.BatchSize {
			message, fetchErr := w.reader.FetchMessage(batchCtx)
			if fetchErr == nil {
				messages = append(messages, message)
				continue
			}
			if errors.Is(fetchErr, context.DeadlineExceeded) && ctx.Err() == nil {
				break
			}
			cancel()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("event-log-writer: fill batch: %w", fetchErr)
		}
		cancel()

		rows := make([]Row, 0, len(messages))
		for _, message := range messages {
			row, decodeErr := Decode(message.Value, w.now().UTC())
			if decodeErr != nil {
				return fmt.Errorf("event-log-writer: decode partition %d offset %d: %w", message.Partition, message.Offset, decodeErr)
			}
			rows = append(rows, row)
		}
		if err := w.inserter.Insert(ctx, rows); err != nil {
			return fmt.Errorf("event-log-writer: insert batch: %w", err)
		}
		if err := w.reader.CommitMessages(ctx, messages...); err != nil {
			return fmt.Errorf("event-log-writer: commit offsets: %w", err)
		}
	}
}

type Row struct {
	EventID        string
	CompanyID      string
	InterviewID    string
	SequenceNumber int64
	EventType      events.EventType
	SchemaVersion  int
	OccurredAt     time.Time
	Payload        string
	CandidateID    *string
	JobID          *string
	ApplicationID  *string
	SessionID      *string
	UserID         *string
	Outcome        *string
	IngestedAt     time.Time
}

type promotedPayload struct {
	InterviewID   string  `json:"interview_id"`
	CandidateID   *string `json:"candidate_id"`
	JobID         *string `json:"job_id"`
	ApplicationID *string `json:"application_id"`
	SessionID     *string `json:"session_id"`
	UserID        *string `json:"user_id"`
	Outcome       *string `json:"outcome"`
}

func Decode(value []byte, ingestedAt time.Time) (Row, error) {
	var envelope events.Envelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return Row{}, fmt.Errorf("envelope: %w", err)
	}
	if envelope.EventID == "" || envelope.CompanyID == "" || envelope.EventType == "" || envelope.SequenceNumber < 0 || envelope.OccurredAt.IsZero() {
		return Row{}, errors.New("invalid event envelope")
	}
	if !json.Valid(envelope.Payload) {
		return Row{}, errors.New("payload is not valid JSON")
	}
	var promoted promotedPayload
	if err := json.Unmarshal(envelope.Payload, &promoted); err != nil {
		return Row{}, fmt.Errorf("typed payload projection: %w", err)
	}
	if promoted.InterviewID == "" {
		return Row{}, errors.New("payload.interview_id is required")
	}
	return Row{
		EventID: envelope.EventID, CompanyID: envelope.CompanyID, InterviewID: promoted.InterviewID,
		SequenceNumber: envelope.SequenceNumber, EventType: envelope.EventType, SchemaVersion: envelope.SchemaVersion,
		OccurredAt: envelope.OccurredAt.UTC(), Payload: string(envelope.Payload), CandidateID: promoted.CandidateID,
		JobID: promoted.JobID, ApplicationID: promoted.ApplicationID, SessionID: promoted.SessionID,
		UserID: promoted.UserID, Outcome: promoted.Outcome, IngestedAt: ingestedAt.UTC(),
	}, nil
}
