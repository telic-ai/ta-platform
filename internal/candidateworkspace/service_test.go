package candidateworkspace

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
)

type recordingStore struct {
	inviteHash []byte
	params     StartParams
	result     Started
}

func (s *recordingStore) StartSession(_ context.Context, inviteHash []byte, params StartParams) (Started, error) {
	s.inviteHash, s.params = inviteHash, params
	s.result.Session.ID = params.SessionID
	s.result.Session.TokenHash = params.TokenHash
	s.result.Session.ExpiresAt = params.ExpiresAt
	s.result.Session.State = domain.SessionStateActive
	return s.result, nil
}

type recordingPublisher struct{ envelope events.Envelope }

func (p *recordingPublisher) Publish(_ context.Context, envelope events.Envelope) error {
	p.envelope = envelope
	return nil
}

func TestStartReturnsScopedTokenAndPublishesSessionStarted(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	store := &recordingStore{result: Started{
		Session:  domain.Session{CompanyID: uuid.New(), UserID: uuid.New()},
		InviteID: uuid.New(),
	}}
	publisher := &recordingPublisher{}
	service := NewService(store, publisher, 2*time.Hour)
	service.now = func() time.Time { return now }
	service.random = bytes.NewReader(bytes.Repeat([]byte{7}, 32))

	response, err := service.Start(context.Background(), "one-time-invite")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if response.AccessToken == "" || response.TokenType != "Bearer" || response.Scope != ScopeCandidateWorkspace {
		t.Fatalf("response is not a scoped bearer token: %+v", response)
	}
	if !bytes.Equal(store.inviteHash, HashToken("one-time-invite")) {
		t.Error("store did not receive the invite token hash")
	}
	if bytes.Equal(store.params.TokenHash, []byte(response.AccessToken)) ||
		!bytes.Equal(store.params.TokenHash, HashToken(response.AccessToken)) {
		t.Error("store must receive only the session token hash")
	}
	if !response.ExpiresAt.Equal(now.Add(2 * time.Hour)) {
		t.Errorf("ExpiresAt = %v", response.ExpiresAt)
	}
	if publisher.envelope.EventType != events.EventTypeSessionStarted {
		t.Fatalf("published event = %q", publisher.envelope.EventType)
	}
	var payload events.SessionStarted
	if err := publisher.envelope.Decode(&payload); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if payload.SessionID != response.SessionID || payload.Scope != ScopeCandidateWorkspace {
		t.Errorf("event payload = %+v", payload)
	}
	if bytes.Contains(publisher.envelope.Payload, []byte(response.AccessToken)) {
		t.Error("session.started leaked the bearer token")
	}
}
