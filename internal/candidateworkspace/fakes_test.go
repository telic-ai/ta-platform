package candidateworkspace

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
)

// memoryEventStore allocates sequence numbers per interview and keeps the
// "outbox" in memory.
type memoryEventStore struct {
	mu       sync.Mutex
	last     map[uuid.UUID]int64
	messages []outbox.Message
	err      error
}

func (s *memoryEventStore) RecordEvents(_ context.Context, _, interviewID uuid.UUID, count int, build func(int64) ([]outbox.Message, error)) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, s.err
	}
	if s.last == nil {
		s.last = map[uuid.UUID]int64{}
	}
	first := s.last[interviewID] + 1
	messages, err := build(first)
	if err != nil {
		return 0, err
	}
	s.last[interviewID] += int64(count)
	s.messages = append(s.messages, messages...)
	return first, nil
}

func (s *memoryEventStore) envelopes(t *testing.T) []events.Envelope {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]events.Envelope, 0, len(s.messages))
	for _, message := range s.messages {
		var envelope events.Envelope
		if err := json.Unmarshal(message.Envelope, &envelope); err != nil {
			t.Fatalf("decode outbox envelope: %v", err)
		}
		out = append(out, envelope)
	}
	return out
}
