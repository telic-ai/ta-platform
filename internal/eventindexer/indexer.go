// Package eventindexer consumes catalog events and makes their text —
// candidate prompts, AI responses and code diffs — searchable in Typesense.
//
// Every document's id is the event_id and is written with an upsert, so a
// redelivered event replaces its own document with identical content:
// redelivery is a no-op.
//
// NOTE: [[Event Indexer]] was not available when this package was written;
// the collection schema and the set of text-bearing events are reasonable
// placeholders to reconcile against it.
package eventindexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/store/typesense"
)

// DefaultTopic is the topic Candidate Workspace publishes session events to.
const DefaultTopic = events.SessionEventsTopic

// Collection is the Typesense collection holding indexed events. It is
// shared by every tenant; queries must always filter by company_id.
const Collection = "session_events"

// Schema is the Typesense schema of Collection.
var Schema = typesense.Schema{
	Name: Collection,
	Fields: []typesense.Field{
		{Name: "company_id", Type: "string", Facet: true},
		{Name: "interview_id", Type: "string", Facet: true},
		{Name: "session_id", Type: "string", Facet: true},
		{Name: "event_type", Type: "string", Facet: true},
		{Name: "sequence_number", Type: "int64"},
		{Name: "occurred_at", Type: "int64"},
		{Name: "text", Type: "string"},
		{Name: "prompt_id", Type: "string", Optional: true},
		{Name: "path", Type: "string", Optional: true},
		{Name: "origin", Type: "string", Facet: true, Optional: true},
	},
	DefaultSortingField: "occurred_at",
}

// Document is one indexed event.
type Document struct {
	ID             string `json:"id"`
	CompanyID      string `json:"company_id"`
	InterviewID    string `json:"interview_id"`
	SessionID      string `json:"session_id"`
	EventType      string `json:"event_type"`
	SequenceNumber int64  `json:"sequence_number"`
	// OccurredAt is Unix milliseconds; Typesense sorts on numbers only.
	OccurredAt int64  `json:"occurred_at"`
	Text       string `json:"text"`
	PromptID   string `json:"prompt_id,omitempty"`
	Path       string `json:"path,omitempty"`
	Origin     string `json:"origin,omitempty"`
}

// Decode maps a Kafka record to its search document. indexed is false,
// with a nil error, for events that carry no searchable text.
func Decode(value []byte) (doc Document, indexed bool, err error) {
	var envelope events.Envelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return Document{}, false, fmt.Errorf("envelope: %w", err)
	}
	if envelope.EventID == "" || envelope.CompanyID == "" || envelope.EventType == "" || envelope.OccurredAt.IsZero() {
		return Document{}, false, errors.New("invalid event envelope")
	}
	doc = Document{
		ID: envelope.EventID, CompanyID: envelope.CompanyID, EventType: string(envelope.EventType),
		SequenceNumber: envelope.SequenceNumber, OccurredAt: envelope.OccurredAt.UnixMilli(),
	}
	switch envelope.EventType {
	case events.EventTypePromptSubmitted:
		var p events.PromptSubmitted
		if err := envelope.Decode(&p); err != nil {
			return Document{}, false, err
		}
		doc.SessionID, doc.InterviewID, doc.PromptID, doc.Text = p.SessionID, p.InterviewID, p.PromptID, p.Prompt
	case events.EventTypeAIResponseCompleted:
		var p events.AIResponseCompleted
		if err := envelope.Decode(&p); err != nil {
			return Document{}, false, err
		}
		doc.SessionID, doc.InterviewID, doc.PromptID, doc.Text = p.SessionID, p.InterviewID, p.PromptID, p.ResponseText
	case events.EventTypeCodeDiff:
		var p events.CodeDiff
		if err := envelope.Decode(&p); err != nil {
			return Document{}, false, err
		}
		doc.SessionID, doc.InterviewID, doc.PromptID, doc.Text = p.SessionID, p.InterviewID, p.PromptID, p.Patch
		doc.Path, doc.Origin = p.Path, p.Origin
	default:
		return Document{}, false, nil
	}
	if doc.InterviewID == "" {
		return Document{}, false, errors.New("payload.interview_id is required")
	}
	// An empty AI response (an error before any text streamed) has nothing
	// to find.
	if doc.Text == "" {
		return Document{}, false, nil
	}
	return doc, true, nil
}

type MessageReader interface {
	FetchMessage(context.Context) (kafkago.Message, error)
	CommitMessages(context.Context, ...kafkago.Message) error
	Close() error
}

// Upserter writes documents by id; typesense.Client implements it.
type Upserter interface {
	Upsert(ctx context.Context, collection string, documents []any) error
}

type Config struct {
	BatchSize int
	BatchWait time.Duration
	// OnInvalid is called for each message that cannot be decoded, or whose
	// document Typesense rejected. Such messages are committed with their
	// batch: redelivering them could never succeed and would stall the
	// partition.
	OnInvalid func(kafkago.Message, error)
}

type Indexer struct {
	reader   MessageReader
	upserter Upserter
	config   Config
}

func New(reader MessageReader, upserter Upserter, config Config) (*Indexer, error) {
	if reader == nil || upserter == nil {
		return nil, errors.New("event-indexer: reader and upserter are required")
	}
	if config.BatchSize <= 0 || config.BatchWait <= 0 {
		return nil, errors.New("event-indexer: batch size and wait must be positive")
	}
	return &Indexer{reader: reader, upserter: upserter, config: config}, nil
}

func (x *Indexer) Close() error { return x.reader.Close() }

// Run indexes until ctx is done, at least once: offsets are committed only
// after Typesense has acknowledged every document in the batch.
func (x *Indexer) Run(ctx context.Context) error {
	for {
		messages, err := x.fetchBatch(ctx)
		if err != nil {
			return err
		}
		if err := x.index(ctx, messages); err != nil {
			return err
		}
		if err := x.reader.CommitMessages(ctx, messages...); err != nil {
			return fmt.Errorf("event-indexer: commit offsets: %w", err)
		}
	}
}

func (x *Indexer) fetchBatch(ctx context.Context) ([]kafkago.Message, error) {
	first, err := x.reader.FetchMessage(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("event-indexer: fetch event: %w", err)
	}
	messages := []kafkago.Message{first}
	batchCtx, cancel := context.WithTimeout(ctx, x.config.BatchWait)
	defer cancel()
	for len(messages) < x.config.BatchSize {
		message, err := x.reader.FetchMessage(batchCtx)
		if err == nil {
			messages = append(messages, message)
			continue
		}
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			break
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("event-indexer: fill batch: %w", err)
	}
	return messages, nil
}

func (x *Indexer) index(ctx context.Context, messages []kafkago.Message) error {
	documents := make([]any, 0, len(messages))
	sources := make([]kafkago.Message, 0, len(messages))
	for _, message := range messages {
		doc, indexed, err := Decode(message.Value)
		if err != nil {
			x.invalid(message, err)
			continue
		}
		if indexed {
			documents = append(documents, doc)
			sources = append(sources, message)
		}
	}
	err := x.upserter.Upsert(ctx, Collection, documents)
	var importErr *typesense.ImportError
	if errors.As(err, &importErr) {
		for _, failure := range importErr.Failures {
			x.invalid(sources[failure.Index], fmt.Errorf("typesense rejected document: %s", failure.Error))
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("event-indexer: upsert batch: %w", err)
	}
	return nil
}

func (x *Indexer) invalid(message kafkago.Message, err error) {
	if x.config.OnInvalid != nil {
		x.config.OnInvalid(message, err)
	}
}
