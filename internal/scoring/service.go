// Package scoring implements the Scoring Service: when a Candidate
// Workspace session ends (session.submitted or session.expired) it waits
// for the event log to hold the whole session, computes the session's
// metrics, asks an AI for an advisory recommendation, stores the score and
// emits score.computed.
//
// Scoring is idempotent on session_id: a session already scored is skipped
// before any AI call, and the score insert itself is conflict-free.
//
// NOTE: [[Scoring Service – Internals]] was not available when this package
// was written; see internal/scoring/metrics for the metric placeholders.
package scoring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics"
)

var (
	// ErrBehind means the event log does not yet hold every event up to the
	// trigger. The trigger must be retried later (NACK + backoff).
	ErrBehind = errors.New("scoring: event log is behind the trigger")
	// ErrInterviewGone means the interview was purged or deleted; there is
	// nothing left to score.
	ErrInterviewGone = errors.New("scoring: interview no longer exists")
)

// PermanentError wraps a failure that retrying the same trigger can never
// fix. The consumer reports it and moves on instead of stalling.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Trigger is a session-ending event that requests a score.
type Trigger struct {
	EventType      events.EventType
	CompanyID      uuid.UUID
	InterviewID    uuid.UUID
	SessionID      uuid.UUID
	SequenceNumber int64
}

// ParseTrigger extracts a Trigger from an envelope. ok is false, with a nil
// error, for every other event type.
func ParseTrigger(envelope events.Envelope) (trigger Trigger, ok bool, err error) {
	var ids struct {
		SessionID   string `json:"session_id"`
		InterviewID string `json:"interview_id"`
	}
	switch envelope.EventType {
	case events.EventTypeSessionSubmitted, events.EventTypeSessionExpired:
	default:
		return Trigger{}, false, nil
	}
	if err := json.Unmarshal(envelope.Payload, &ids); err != nil {
		return Trigger{}, false, fmt.Errorf("decode %s payload: %w", envelope.EventType, err)
	}
	trigger = Trigger{EventType: envelope.EventType, SequenceNumber: envelope.SequenceNumber}
	for _, field := range []struct {
		name  string
		value string
		dst   *uuid.UUID
	}{
		{"company_id", envelope.CompanyID, &trigger.CompanyID},
		{"interview_id", ids.InterviewID, &trigger.InterviewID},
		{"session_id", ids.SessionID, &trigger.SessionID},
	} {
		if *field.dst, err = uuid.Parse(field.value); err != nil {
			return Trigger{}, false, fmt.Errorf("%s %s must be a UUID", envelope.EventType, field.name)
		}
	}
	if trigger.SequenceNumber < 1 {
		return Trigger{}, false, errors.New("trigger sequence_number must be positive")
	}
	return trigger, true, nil
}

// Progress is how much of an interview's sequence the event log holds up to
// a trigger: the highest sequence number, and how many distinct sequence
// numbers in [1, trigger] are present.
type Progress struct {
	MaxSequence int64
	Present     int64
}

// CaughtUp reports whether the log holds every event up to sequence. The
// max check alone is not enough: ai.response.completed is produced by the AI
// Gateway directly, so it can land after later events of the interview.
func (p Progress) CaughtUp(sequence int64) bool {
	return p.MaxSequence >= sequence && p.Present >= sequence
}

// EventLog reads the ClickHouse event log.
type EventLog interface {
	Progress(ctx context.Context, companyID, interviewID uuid.UUID, upTo int64) (Progress, error)
	SessionTimeline(ctx context.Context, companyID, interviewID, sessionID uuid.UUID, upTo int64) ([]metrics.Event, error)
}

// Score is one stored session score.
type Score struct {
	CompanyID           uuid.UUID
	InterviewID         uuid.UUID
	SessionID           uuid.UUID
	Trigger             events.EventType
	Metrics             metrics.Metrics
	MetricsComplete     bool
	Recommendation      *Recommendation
	RecommendationModel string
	RecommendationError string
	ComputedAt          time.Time
}

// Store persists scores. SaveScore inserts the score unless the session
// already has one; only when it inserts does it call build with a freshly
// allocated interview sequence number and write the returned message to the
// outbox in the same transaction. It returns ErrInterviewGone if the
// interview was purged.
type Store interface {
	ScoreExists(ctx context.Context, companyID, sessionID uuid.UUID) (bool, error)
	SaveScore(ctx context.Context, score Score, build func(sequence int64) (outbox.Message, error)) (bool, error)
}

type Config struct {
	// RecommendTimeout bounds the AI call; a timeout stores the score with
	// a null recommendation.
	RecommendTimeout time.Duration
}

type Service struct {
	log         EventLog
	store       Store
	recommender Recommender
	config      Config
	now         func() time.Time
}

// NewService builds the service. recommender may be nil, which stores every
// score without a recommendation.
func NewService(log EventLog, store Store, recommender Recommender, config Config) (*Service, error) {
	if log == nil || store == nil {
		return nil, errors.New("scoring: event log and store are required")
	}
	if config.RecommendTimeout <= 0 {
		config.RecommendTimeout = 60 * time.Second
	}
	return &Service{log: log, store: store, recommender: recommender, config: config, now: time.Now}, nil
}

// Outcome describes what Score did.
type Outcome string

const (
	OutcomeScored        Outcome = "scored"
	OutcomeAlreadyScored Outcome = "already_scored"
)

// Score scores the trigger's session. Unless final is set, it returns
// ErrBehind while the event log is missing events up to the trigger; with
// final set it scores what the log holds and marks the metrics incomplete.
func (s *Service) Score(ctx context.Context, trigger Trigger, final bool) (Outcome, error) {
	exists, err := s.store.ScoreExists(ctx, trigger.CompanyID, trigger.SessionID)
	if err != nil {
		return "", fmt.Errorf("scoring: check existing score: %w", err)
	}
	if exists {
		return OutcomeAlreadyScored, nil
	}

	progress, err := s.log.Progress(ctx, trigger.CompanyID, trigger.InterviewID, trigger.SequenceNumber)
	if err != nil {
		return "", fmt.Errorf("scoring: read event log progress: %w", err)
	}
	complete := progress.CaughtUp(trigger.SequenceNumber)
	if !complete && !final {
		return "", fmt.Errorf("%w: have max %d with %d of %d events", ErrBehind, progress.MaxSequence, progress.Present, trigger.SequenceNumber)
	}

	timeline, err := s.log.SessionTimeline(ctx, trigger.CompanyID, trigger.InterviewID, trigger.SessionID, trigger.SequenceNumber)
	if err != nil {
		return "", fmt.Errorf("scoring: read session timeline: %w", err)
	}
	computed, err := metrics.Compute(timeline)
	if err != nil {
		return "", &PermanentError{Err: fmt.Errorf("scoring: compute metrics: %w", err)}
	}

	score := Score{
		CompanyID: trigger.CompanyID, InterviewID: trigger.InterviewID, SessionID: trigger.SessionID,
		Trigger: trigger.EventType, Metrics: computed, MetricsComplete: complete, ComputedAt: s.now().UTC(),
	}
	s.recommend(ctx, &score)

	saved, err := s.store.SaveScore(ctx, score, func(sequence int64) (outbox.Message, error) {
		return scoreComputedMessage(score, sequence)
	})
	if errors.Is(err, ErrInterviewGone) {
		return "", &PermanentError{Err: err}
	}
	if err != nil {
		return "", fmt.Errorf("scoring: save score: %w", err)
	}
	if !saved {
		return OutcomeAlreadyScored, nil
	}
	return OutcomeScored, nil
}

// recommend fills the score's recommendation, or its error code. It never
// fails the score.
func (s *Service) recommend(ctx context.Context, score *Score) {
	if s.recommender == nil {
		score.RecommendationError = RecommendationErrDisabled
		return
	}
	recommendCtx, cancel := context.WithTimeout(ctx, s.config.RecommendTimeout)
	defer cancel()
	recommendation, model, err := s.recommender.Recommend(recommendCtx, score.Metrics)
	score.RecommendationModel = model
	if err != nil {
		score.RecommendationError = recommendationErrorCode(recommendCtx, err)
		return
	}
	score.Recommendation = &recommendation
}

func scoreComputedMessage(score Score, sequence int64) (outbox.Message, error) {
	metricsJSON, err := json.Marshal(score.Metrics)
	if err != nil {
		return outbox.Message{}, err
	}
	payload := events.ScoreComputed{
		SessionID: score.SessionID.String(), InterviewID: score.InterviewID.String(),
		Trigger: score.Trigger, MetricsComplete: score.MetricsComplete, Metrics: metricsJSON,
		RecommendationErr: score.RecommendationError,
	}
	if score.Recommendation != nil {
		payload.Recommendation = score.Recommendation.Decision
	}
	envelope, err := events.New(score.CompanyID.String(), sequence, payload)
	if err != nil {
		return outbox.Message{}, err
	}
	return outbox.NewMessage(events.SessionEventsTopic, score.SessionID.String(), envelope)
}
