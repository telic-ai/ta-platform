package candidateworkspace

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
)

// recordingStore mimics the Postgres store: it builds the outbox message
// inside the "transaction" and keeps it.
type recordingStore struct {
	inviteHash []byte
	params     StartParams
	result     Started
	message    outbox.Message
}

func (s *recordingStore) StartSession(_ context.Context, inviteHash []byte, params StartParams, build EventBuilder) (Started, error) {
	s.inviteHash, s.params = inviteHash, params
	s.result.Session.ID = params.SessionID
	s.result.Session.TokenHash = params.TokenHash
	s.result.Session.ExpiresAt = params.ExpiresAt
	s.result.Session.State = domain.SessionStateActive
	message, err := build(s.result)
	if err != nil {
		return Started{}, err
	}
	s.message = message
	return s.result, nil
}

func TestStartReturnsScopedTokenAndRecordsSessionStarted(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	interviewID := uuid.New()
	store := &recordingStore{result: Started{
		Session:        domain.Session{CompanyID: uuid.New(), InterviewID: &interviewID},
		InviteID:       uuid.New(),
		SequenceNumber: 4,
	}}
	service := NewService(store, 2*time.Hour)
	service.now = func() time.Time { return now }
	service.random = bytes.NewReader(bytes.Repeat([]byte{7}, 32))

	response, err := service.Start(context.Background(), "one-time-invite")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if response.AccessToken == "" || response.TokenType != "Bearer" || response.Scope != ScopeCandidateWorkspace {
		t.Fatalf("response is not a scoped bearer token: %+v", response)
	}
	if !bytes.Equal(store.inviteHash, auth.HashToken("one-time-invite")) {
		t.Error("store did not receive the invite token hash")
	}
	if bytes.Equal(store.params.TokenHash, []byte(response.AccessToken)) ||
		!bytes.Equal(store.params.TokenHash, auth.HashToken(response.AccessToken)) {
		t.Error("store must receive only the session token hash")
	}
	if !response.ExpiresAt.Equal(now.Add(2 * time.Hour)) {
		t.Errorf("ExpiresAt = %v", response.ExpiresAt)
	}

	message := store.message
	if message.Topic != events.TopicSessionEvents || string(message.Key) != response.SessionID {
		t.Fatalf("outbox message routed to %q key %q", message.Topic, message.Key)
	}
	if message.CompanyID != store.result.Session.CompanyID || message.EventType != events.EventTypeSessionStarted {
		t.Fatalf("outbox message = %+v", message)
	}
	var envelope events.Envelope
	if err := json.Unmarshal(message.Envelope, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	var payload events.SessionStarted
	if err := envelope.Decode(&payload); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if payload.SessionID != response.SessionID || payload.Scope != ScopeCandidateWorkspace {
		t.Errorf("event payload = %+v", payload)
	}
	if payload.InterviewID != interviewID.String() {
		t.Errorf("interview_id = %q, want %q", payload.InterviewID, interviewID)
	}
	if envelope.SequenceNumber != 4 {
		t.Errorf("sequence_number = %d, want the store-allocated 4", envelope.SequenceNumber)
	}
	if bytes.Contains(message.Envelope, []byte(response.AccessToken)) {
		t.Error("session.started leaked the bearer token")
	}
}
