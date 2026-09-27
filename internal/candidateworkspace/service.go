// Package candidateworkspace implements the Candidate Workspace session
// boundary: invite exchange, scoped opaque tokens, and Active-session checks.
package candidateworkspace

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
)

const (
	ScopeCandidateWorkspace = "candidate:workspace"
	SessionEventsTopic      = events.TopicSessionEvents
)

var ErrInvalidInvite = errors.New("invalid, expired, or already-used invite")

// StartParams are the generated session values persisted during an atomic
// invite exchange. The raw bearer token is intentionally absent.
type StartParams struct {
	SessionID uuid.UUID
	TokenHash []byte
	ExpiresAt time.Time
}

// Started is the result of an atomic invite exchange.
type Started struct {
	Session  domain.Session
	InviteID uuid.UUID
	// SequenceNumber is the interview event sequence allocated for
	// session.started in the same transaction.
	SequenceNumber int64
}

// EventBuilder turns a started session into its session.started outbox
// message. The store calls it inside the invite-exchange transaction, once
// the sequence number is allocated.
type EventBuilder func(Started) (outbox.Message, error)

// Store owns durable invite and session state and writes the session's event
// to the outbox atomically with it.
type Store interface {
	StartSession(context.Context, []byte, StartParams, EventBuilder) (Started, error)
}

// Service performs Candidate Workspace session operations.
type Service struct {
	store  Store
	ttl    time.Duration
	now    func() time.Time
	random io.Reader
}

func NewService(store Store, ttl time.Duration) *Service {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Service{store: store, ttl: ttl, now: time.Now, random: rand.Reader}
}

// Start exchanges an invite token exactly once for a scoped opaque session
// token and records session.started in the outbox. Only the token hash
// crosses the store.
func (s *Service) Start(ctx context.Context, inviteToken string) (StartResponse, error) {
	if inviteToken == "" {
		return StartResponse{}, ErrInvalidInvite
	}

	raw := make([]byte, 32)
	if _, err := io.ReadFull(s.random, raw); err != nil {
		return StartResponse{}, fmt.Errorf("generate session token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	now := s.now().UTC()
	started, err := s.store.StartSession(ctx, auth.HashToken(inviteToken), StartParams{
		SessionID: uuid.New(), TokenHash: auth.HashToken(token), ExpiresAt: now.Add(s.ttl),
	}, sessionStartedMessage)
	if err != nil {
		return StartResponse{}, err
	}

	return StartResponse{
		SessionID: started.Session.ID.String(), AccessToken: token,
		TokenType: "Bearer", Scope: ScopeCandidateWorkspace,
		ExpiresAt: started.Session.ExpiresAt,
	}, nil
}

func sessionStartedMessage(started Started) (outbox.Message, error) {
	if started.Session.InterviewID == nil {
		return outbox.Message{}, errors.New("session.started: candidate session has no interview")
	}
	envelope, err := events.New(started.Session.CompanyID.String(), started.SequenceNumber, events.SessionStarted{
		SessionID: started.Session.ID.String(), InterviewID: started.Session.InterviewID.String(),
		InviteID: started.InviteID.String(), Scope: ScopeCandidateWorkspace,
		ExpiresAt: started.Session.ExpiresAt,
	})
	if err != nil {
		return outbox.Message{}, fmt.Errorf("build session.started: %w", err)
	}
	return outbox.NewMessage(SessionEventsTopic, started.Session.ID.String(), envelope)
}

type StartResponse struct {
	SessionID   string    `json:"sessionId"`
	AccessToken string    `json:"accessToken"`
	TokenType   string    `json:"tokenType"`
	Scope       string    `json:"scope"`
	ExpiresAt   time.Time `json:"expiresAt"`
}
