package candidateworkspace

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
)

// EventStore allocates interview sequence numbers and records events in the
// transactional outbox.
type EventStore interface {
	// RecordEvents allocates count consecutive sequence numbers for the
	// interview, passes the first to build, and writes the messages build
	// returns to the outbox in the same transaction. It returns the first
	// allocated number.
	RecordEvents(ctx context.Context, companyID, interviewID uuid.UUID, count int, build func(first int64) ([]outbox.Message, error)) (int64, error)
}

// sessionMessage wraps payload in an envelope for session and routes it to
// the session events topic, keyed by session.
func sessionMessage(session domain.Session, sequence int64, payload events.Payload) (outbox.Message, error) {
	envelope, err := events.New(session.CompanyID.String(), sequence, payload)
	if err != nil {
		return outbox.Message{}, err
	}
	return outbox.NewMessage(SessionEventsTopic, session.ID.String(), envelope)
}

func interviewOf(session domain.Session) (uuid.UUID, error) {
	if session.InterviewID == nil {
		return uuid.Nil, errors.New("candidate session has no interview")
	}
	return *session.InterviewID, nil
}
