package metrics_test

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics/fixtures"
)

func TestComputeMatchesFixtures(t *testing.T) {
	sessions, err := fixtures.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) < 3 {
		t.Fatalf("loaded %d fixtures, want at least 3", len(sessions))
	}
	for _, session := range sessions {
		t.Run(session.Name, func(t *testing.T) {
			timeline, err := session.Timeline()
			if err != nil {
				t.Fatal(err)
			}
			got, err := metrics.Compute(timeline)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, session.Want) {
				t.Fatalf("metrics = %s\nwant      %s", jsonOf(got), jsonOf(session.Want))
			}
		})
	}
}

func TestComputeIsOrderIndependent(t *testing.T) {
	sessions, _ := fixtures.All()
	for _, session := range sessions {
		timeline, _ := session.Timeline()
		random := rand.New(rand.NewSource(1))
		for range 20 {
			random.Shuffle(len(timeline), func(i, j int) { timeline[i], timeline[j] = timeline[j], timeline[i] })
			got, err := metrics.Compute(timeline)
			if err != nil || !reflect.DeepEqual(got, session.Want) {
				t.Fatalf("%s: shuffled metrics = %s, %v", session.Name, jsonOf(got), err)
			}
		}
	}
}

func TestComputeEmptyTimeline(t *testing.T) {
	got, err := metrics.Compute(nil)
	if err != nil || !reflect.DeepEqual(got, metrics.Metrics{}) {
		t.Fatalf("Compute(nil) = %+v, %v", got, err)
	}
}

func TestComputeIgnoresNonActivityEvents(t *testing.T) {
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	timeline := []metrics.Event{
		{SequenceNumber: 1, EventType: events.EventTypeSessionStarted, OccurredAt: base, Payload: json.RawMessage(`{}`)},
		{SequenceNumber: 2, EventType: events.EventTypeScoreComputed, OccurredAt: base.Add(time.Hour), Payload: json.RawMessage(`{}`)},
	}
	got, err := metrics.Compute(timeline)
	if err != nil || got.ActiveDurationMS != 0 {
		t.Fatalf("score.computed extended the session: %+v, %v", got, err)
	}
}

func TestComputeRejectsMalformedPayload(t *testing.T) {
	timeline := []metrics.Event{{SequenceNumber: 1, EventType: events.EventTypeCodeDiff, Payload: json.RawMessage(`{"lines_added":"many"}`)}}
	if _, err := metrics.Compute(timeline); err == nil {
		t.Fatal("malformed payload accepted")
	}
}

func TestDedupeKeepsFirstCopyInSequenceOrder(t *testing.T) {
	got := metrics.Dedupe([]metrics.Event{
		{SequenceNumber: 3, EventType: "a"}, {SequenceNumber: 1}, {SequenceNumber: 3, EventType: "b"}, {SequenceNumber: 2},
	})
	if len(got) != 3 || got[0].SequenceNumber != 1 || got[2].EventType != "a" {
		t.Fatalf("Dedupe = %+v", got)
	}
}

func TestRatios(t *testing.T) {
	m := metrics.Metrics{RunCount: 4, RunSucceededCount: 1, LinesAdded: 40, AILinesAdded: 10}
	if m.RunSuccessRate() != 0.25 || m.AILineShare() != 0.25 {
		t.Fatalf("ratios = %v, %v", m.RunSuccessRate(), m.AILineShare())
	}
	var zero metrics.Metrics
	if zero.RunSuccessRate() != 0 || zero.AILineShare() != 0 {
		t.Fatal("zero denominators must yield 0")
	}
}

func jsonOf(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
