// Package livemonitor streams an interview's session events to company
// interviewers as they happen.
//
// A Kafka consumer (Ingester) appends each event to a capped per-interview
// ring buffer and fans it out over pub/sub (Bus, backed by Redis in
// production). GET /interviews/{id}/live subscribes, replays whatever the
// viewer missed since Last-Event-ID from the ring (or ClickHouse, when the
// ring no longer reaches back that far), then streams live events.
package livemonitor

import (
	"context"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/replay"
)

// Event is the live-stream representation of a session event. It is the
// replay API's shape, so the UI renders live and replayed events alike.
type Event = replay.Event

// Stream identifies one interview's event stream within its company.
type Stream struct {
	CompanyID   uuid.UUID
	InterviewID uuid.UUID
}

// Bus is the fan-out and short-term history for live streams.
type Bus interface {
	// Publish appends event to its stream's ring buffer and fans it out to
	// subscribers. It reports false, and publishes nothing, when the ring
	// already holds the event's sequence number (a redelivery).
	Publish(ctx context.Context, stream Stream, event Event) (bool, error)
	// Recent returns buffered events with sequence numbers above afterSeq,
	// in order, and the lowest sequence number still buffered (0 if the
	// ring is empty).
	Recent(ctx context.Context, stream Stream, afterSeq int64) (events []Event, oldest int64, err error)
	// Subscribe starts receiving the stream's published events. Delivery is
	// best-effort: a slow subscriber may miss events, which callers repair
	// from Recent.
	Subscribe(ctx context.Context, stream Stream) (Subscription, error)
}

// Subscription delivers one stream's events until closed.
type Subscription interface {
	Events() <-chan Event
	Close() error
}

// MemoryBus is a single-process Bus for tests and local development.
type MemoryBus struct {
	capacity int
	mu       sync.Mutex
	rings    map[Stream][]Event
	subs     map[Stream]map[*memorySubscription]struct{}
}

// NewMemoryBus keeps the newest capacity events per stream.
func NewMemoryBus(capacity int) *MemoryBus {
	return &MemoryBus{capacity: capacity, rings: map[Stream][]Event{}, subs: map[Stream]map[*memorySubscription]struct{}{}}
}

func (b *MemoryBus) Publish(_ context.Context, stream Stream, event Event) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ring := b.rings[stream]
	for _, existing := range ring {
		if existing.SequenceNumber == event.SequenceNumber {
			return false, nil
		}
	}
	ring = append(ring, event)
	sort.Slice(ring, func(i, j int) bool { return ring[i].SequenceNumber < ring[j].SequenceNumber })
	if len(ring) > b.capacity {
		ring = ring[len(ring)-b.capacity:]
	}
	b.rings[stream] = ring
	for sub := range b.subs[stream] {
		select {
		case sub.ch <- event:
		default: // slow subscriber: drop, like Redis pub/sub
		}
	}
	return true, nil
}

func (b *MemoryBus) Recent(_ context.Context, stream Stream, afterSeq int64) ([]Event, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ring := b.rings[stream]
	var oldest int64
	if len(ring) > 0 {
		oldest = ring[0].SequenceNumber
	}
	var out []Event
	for _, event := range ring {
		if event.SequenceNumber > afterSeq {
			out = append(out, event)
		}
	}
	return out, oldest, nil
}

func (b *MemoryBus) Subscribe(_ context.Context, stream Stream) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sub := &memorySubscription{bus: b, stream: stream, ch: make(chan Event, subscriptionBuffer)}
	if b.subs[stream] == nil {
		b.subs[stream] = map[*memorySubscription]struct{}{}
	}
	b.subs[stream][sub] = struct{}{}
	return sub, nil
}

// subscriptionBuffer is how many undelivered events a subscriber may lag.
const subscriptionBuffer = 256

type memorySubscription struct {
	bus    *MemoryBus
	stream Stream
	ch     chan Event
	once   sync.Once
}

func (s *memorySubscription) Events() <-chan Event { return s.ch }

func (s *memorySubscription) Close() error {
	s.once.Do(func() {
		s.bus.mu.Lock()
		defer s.bus.mu.Unlock()
		delete(s.bus.subs[s.stream], s)
		close(s.ch)
	})
	return nil
}
