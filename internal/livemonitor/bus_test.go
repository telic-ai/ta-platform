package livemonitor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func event(stream Stream, seq int64) Event {
	return Event{
		EventID: uuid.NewString(), CompanyID: stream.CompanyID.String(), InterviewID: stream.InterviewID.String(),
		SequenceNumber: seq, EventType: "code.diff", OccurredAt: time.Unix(seq, 0).UTC(), Payload: json.RawMessage(`{"n":1}`),
	}
}

func newStream() Stream { return Stream{CompanyID: uuid.New(), InterviewID: uuid.New()} }

func seqs(events []Event) []int64 {
	out := make([]int64, 0, len(events))
	for _, e := range events {
		out = append(out, e.SequenceNumber)
	}
	return out
}

func equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// busContract runs the Bus behaviors every implementation must share.
func busContract(t *testing.T, bus Bus, capacity int) {
	t.Helper()
	ctx := context.Background()
	stream, other := newStream(), newStream()

	if events, oldest, err := bus.Recent(ctx, stream, 0); err != nil || len(events) != 0 || oldest != 0 {
		t.Fatalf("empty Recent = %v, %d, %v", events, oldest, err)
	}

	a, err := bus.Subscribe(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	b, _ := bus.Subscribe(ctx, stream)
	defer func() { _ = b.Close() }()
	c, _ := bus.Subscribe(ctx, other)
	defer func() { _ = c.Close() }()

	if added, err := bus.Publish(ctx, stream, event(stream, 1)); !added || err != nil {
		t.Fatalf("Publish = %v, %v", added, err)
	}
	for name, sub := range map[string]Subscription{"a": a, "b": b} {
		select {
		case got := <-sub.Events():
			if got.SequenceNumber != 1 {
				t.Fatalf("%s got %+v", name, got)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %s did not receive the event", name)
		}
	}
	select {
	case got := <-c.Events():
		t.Fatalf("other stream received %+v", got)
	case <-time.After(50 * time.Millisecond):
	}

	// A redelivered sequence number is neither buffered nor published.
	if added, _ := bus.Publish(ctx, stream, event(stream, 1)); added {
		t.Fatal("duplicate accepted")
	}
	select {
	case got := <-a.Events():
		t.Fatalf("duplicate published: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}

	for seq := int64(2); seq <= int64(capacity)+2; seq++ {
		if _, err := bus.Publish(ctx, stream, event(stream, seq)); err != nil {
			t.Fatal(err)
		}
	}
	all, oldest, err := bus.Recent(ctx, stream, 0)
	if err != nil || len(all) != capacity || oldest != 3 || all[0].SequenceNumber != 3 || all[len(all)-1].SequenceNumber != int64(capacity)+2 {
		t.Fatalf("capped Recent = %v, oldest %d, %v", seqs(all), oldest, err)
	}
	tail, _, _ := bus.Recent(ctx, stream, int64(capacity))
	if !equal(seqs(tail), []int64{int64(capacity) + 1, int64(capacity) + 2}) {
		t.Fatalf("Recent after %d = %v", capacity, seqs(tail))
	}
	if events, _, _ := bus.Recent(ctx, other, 0); len(events) != 0 {
		t.Fatal("streams share a ring")
	}
	// Payloads survive the round trip.
	if string(all[0].Payload) != `{"n":1}` || all[0].EventType != "code.diff" {
		t.Fatalf("event = %+v", all[0])
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	_ = a.Close() // idempotent
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-a.Events():
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("closed subscription's channel stayed open")
		}
	}
}

func TestMemoryBusContract(t *testing.T) { busContract(t, NewMemoryBus(4), 4) }

func TestMemoryBusOrdersOutOfOrderPublishes(t *testing.T) {
	bus, stream := NewMemoryBus(10), newStream()
	for _, seq := range []int64{3, 1, 2} {
		_, _ = bus.Publish(context.Background(), stream, event(stream, seq))
	}
	events, oldest, _ := bus.Recent(context.Background(), stream, 0)
	if !equal(seqs(events), []int64{1, 2, 3}) || oldest != 1 {
		t.Fatalf("Recent = %v, oldest %d", seqs(events), oldest)
	}
}

func TestMemoryBusDropsForSlowSubscriber(t *testing.T) {
	bus, stream := NewMemoryBus(1000), newStream()
	sub, _ := bus.Subscribe(context.Background(), stream)
	defer func() { _ = sub.Close() }()
	for seq := int64(1); seq <= subscriptionBuffer+10; seq++ {
		if _, err := bus.Publish(context.Background(), stream, event(stream, seq)); err != nil {
			t.Fatal(err)
		}
	}
	if len(sub.Events()) != subscriptionBuffer {
		t.Fatalf("buffered %d", len(sub.Events()))
	}
	// The ring still has everything for gap repair.
	if events, _, _ := bus.Recent(context.Background(), stream, 0); len(events) != subscriptionBuffer+10 {
		t.Fatalf("ring holds %d", len(events))
	}
}
