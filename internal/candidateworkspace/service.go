// Package candidateworkspace implements the Candidate Workspace session
// boundary: invite exchange, scoped opaque tokens, and Active-session checks.
package candidateworkspace

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
)

const (
	ScopeCandidateWorkspace = "candidate:workspace"
	SessionEventsTopic      = "session-events"
)

var (
	ErrInvalidInvite   = errors.New("invalid, expired, or already-used invite")
	ErrSessionNotFound = errors.New("session not found")
)

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
}

// Store owns durable invite and session state.
type Store interface {
	StartSession(context.Context, []byte, StartParams) (Started, error)
}

// Publisher publishes the platform event envelope to Kafka.
type Publisher interface {
	Publish(context.Context, events.Envelope) error
}

// Service performs Candidate Workspace session operations.
type Service struct {
	store     Store
	publisher Publisher
	ttl       time.Duration
	now       func() time.Time
	random    io.Reader
}

func NewService(store Store, publisher Publisher, ttl time.Duration) *Service {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Service{store: store, publisher: publisher, ttl: ttl, now: time.Now, random: rand.Reader}
}

// Start exchanges an invite token exactly once for a scoped opaque session
// token, then emits session.started. Only the token hash crosses the store.
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
	started, err := s.store.StartSession(ctx, HashToken(inviteToken), StartParams{
		SessionID: uuid.New(), TokenHash: HashToken(token), ExpiresAt: now.Add(s.ttl),
	})
	if err != nil {
		return StartResponse{}, err
	}

	envelope, err := events.New(started.Session.CompanyID.String(), 1, events.SessionStarted{
		SessionID: started.Session.ID.String(), UserID: started.Session.UserID.String(),
		InviteID: started.InviteID.String(), Scope: ScopeCandidateWorkspace,
		ExpiresAt: started.Session.ExpiresAt,
	})
	if err != nil {
		return StartResponse{}, fmt.Errorf("build session.started: %w", err)
	}
	if err := s.publisher.Publish(ctx, envelope); err != nil {
		return StartResponse{}, fmt.Errorf("publish session.started: %w", err)
	}

	return StartResponse{
		SessionID: started.Session.ID.String(), AccessToken: token,
		TokenType: "Bearer", Scope: ScopeCandidateWorkspace,
		ExpiresAt: started.Session.ExpiresAt,
	}, nil
}

// HashToken returns the one-way representation used for invite and session
// lookups. Raw tokens must never be persisted or logged.
func HashToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

type StartResponse struct {
	SessionID   string    `json:"sessionId"`
	AccessToken string    `json:"accessToken"`
	TokenType   string    `json:"tokenType"`
	Scope       string    `json:"scope"`
	ExpiresAt   time.Time `json:"expiresAt"`
}
