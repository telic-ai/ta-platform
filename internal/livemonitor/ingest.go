package livemonitor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/eventlogwriter"
)

// MessageReader is the Kafka consumer-group reader the Ingester drives.
type MessageReader interface {
	FetchMessage(context.Context) (kafkago.Message, error)
	CommitMessages(context.Context, ...kafkago.Message) error
}

// Ingester moves session events from Kafka onto the Bus. Offsets are
// committed only after the Bus accepts an event, so a crash (or a restart
// on a new reader after Run fails) redelivers it and the ring's
// sequence-number check drops the duplicate.
type Ingester struct {
	reader MessageReader
	bus    Bus
	// OnInvalid is called for each message that is not a decodable
	// interview event. Such messages are skipped and committed.
	OnInvalid func(kafkago.Message, error)
	now       func() time.Time
}

func NewIngester(reader MessageReader, bus Bus) *Ingester {
	return &Ingester{reader: reader, bus: bus, now: time.Now}
}

// Run consumes until ctx ends or Kafka or the Bus fails.
func (in *Ingester) Run(ctx context.Context) error {
	for {
		message, err := in.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("live ingest: fetch: %w", err)
		}
		if err := in.Handle(ctx, message); err != nil {
			return err
		}
		if err := in.reader.CommitMessages(ctx, message); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("live ingest: commit: %w", err)
		}
	}
}

// Handle publishes one Kafka message. Undecodable messages are reported and
// skipped; only a Bus failure is an error.
func (in *Ingester) Handle(ctx context.Context, message kafkago.Message) error {
	stream, event, err := DecodeMessage(message.Value, in.now())
	if err != nil {
		if in.OnInvalid != nil {
			in.OnInvalid(message, err)
		}
		return nil
	}
	if _, err := in.bus.Publish(ctx, stream, event); err != nil {
		return fmt.Errorf("live ingest: %w", err)
	}
	return nil
}

// DecodeMessage turns a Kafka envelope into its stream and live event,
// using the event log's validation so live and replayed events agree.
func DecodeMessage(value []byte, now time.Time) (Stream, Event, error) {
	row, err := eventlogwriter.Decode(value, now)
	if err != nil {
		return Stream{}, Event{}, err
	}
	companyID, err := uuid.Parse(row.CompanyID)
	if err != nil {
		return Stream{}, Event{}, errors.New("company_id is not a UUID")
	}
	interviewID, err := uuid.Parse(row.InterviewID)
	if err != nil {
		return Stream{}, Event{}, errors.New("interview_id is not a UUID")
	}
	return Stream{CompanyID: companyID, InterviewID: interviewID}, Event{
		EventID: row.EventID, CompanyID: row.CompanyID, InterviewID: row.InterviewID,
		SequenceNumber: row.SequenceNumber, EventType: string(row.EventType), OccurredAt: row.OccurredAt,
		Payload: []byte(row.Payload),
	}, nil
}
