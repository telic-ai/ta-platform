package scoring

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics"
)

var (
	companyID   = uuid.MustParse("0b6f3a1e-1111-4c1a-9a51-000000000001")
	interviewID = uuid.MustParse("0b6f3a1e-2222-4c1a-9a51-000000000001")
	sessionID   = uuid.MustParse("0b6f3a1e-3333-4c1a-9a51-000000000001")
)

func testTrigger() Trigger {
	return Trigger{EventType: events.EventTypeSessionSubmitted, CompanyID: companyID, InterviewID: interviewID, SessionID: sessionID, SequenceNumber: 4}
}

func TestParseTrigger(t *testing.T) {
	envelope, _ := events.New(companyID.String(), 4, events.SessionExpired{SessionID: sessionID.String(), InterviewID: interviewID.String()})
	got, ok, err := ParseTrigger(envelope)
	if err != nil || !ok {
		t.Fatalf("ParseTrigger = %v, %v", ok, err)
	}
	if got.EventType != events.EventTypeSessionExpired || got.SessionID != sessionID || got.SequenceNumber != 4 {
		t.Fatalf("trigger = %+v", got)
	}

	other, _ := events.New(companyID.String(), 1, events.SessionStarted{SessionID: sessionID.String()})
	if _, ok, err := ParseTrigger(other); ok || err != nil {
		t.Fatalf("non-trigger: ok=%v err=%v", ok, err)
	}

	bad, _ := events.New(companyID.String(), 4, events.SessionSubmitted{SessionID: "not-a-uuid", InterviewID: interviewID.String()})
	if _, _, err := ParseTrigger(bad); err == nil {
		t.Fatal("non-UUID session accepted")
	}
	zero, _ := events.New(companyID.String(), 0, events.SessionSubmitted{SessionID: sessionID.String(), InterviewID: interviewID.String()})
	if _, _, err := ParseTrigger(zero); err == nil {
		t.Fatal("zero sequence accepted")
	}
}

func TestProgressCaughtUpNeedsMaxAndNoGaps(t *testing.T) {
	for _, tc := range []struct {
		progress Progress
		want     bool
	}{
		{Progress{MaxSequence: 4, Present: 4}, true},
		{Progress{MaxSequence: 6, Present: 4}, true},
		{Progress{MaxSequence: 3, Present: 3}, false},
		{Progress{MaxSequence: 4, Present: 3}, false}, // a late ai.response.completed
		{Progress{}, false},
	} {
		if got := tc.progress.CaughtUp(4); got != tc.want {
			t.Errorf("%+v.CaughtUp(4) = %v", tc.progress, got)
		}
	}
}

func TestScoreStoresMetricsRecommendationAndEvent(t *testing.T) {
	log := caughtUpLog()
	store := &fakeStore{}
	service := newTestService(t, log, store, fakeRecommender{rec: Recommendation{Decision: DecisionAdvance, Confidence: 0.7, Rationale: "r"}, model: "m1"})
	outcome, err := service.Score(context.Background(), testTrigger(), false)
	if err != nil || outcome != OutcomeScored {
		t.Fatalf("Score = %v, %v", outcome, err)
	}
	score := store.saved[0]
	if !score.MetricsComplete || score.Metrics.PromptCount != 1 || score.Metrics.RunCount != 1 {
		t.Fatalf("score = %+v", score)
	}
	if score.Recommendation == nil || score.Recommendation.Decision != DecisionAdvance || score.RecommendationModel != "m1" || score.RecommendationError != "" {
		t.Fatalf("recommendation = %+v / %q", score.Recommendation, score.RecommendationError)
	}
	if log.upTo != 4 {
		t.Fatalf("timeline read up to %d, want the trigger's sequence 4", log.upTo)
	}

	message := store.messages[0]
	if message.Topic != events.SessionEventsTopic || string(message.Key) != sessionID.String() || message.EventType != events.EventTypeScoreComputed {
		t.Fatalf("message = %+v", message)
	}
	var envelope events.Envelope
	_ = json.Unmarshal(message.Envelope, &envelope)
	var payload events.ScoreComputed
	if err := envelope.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if envelope.SequenceNumber != 9 || payload.Recommendation != DecisionAdvance || payload.Trigger != events.EventTypeSessionSubmitted || !payload.MetricsComplete {
		t.Fatalf("event = %+v %+v", envelope, payload)
	}
	var emitted metrics.Metrics
	if err := json.Unmarshal(payload.Metrics, &emitted); err != nil || emitted != score.Metrics {
		t.Fatalf("emitted metrics = %+v, %v", emitted, err)
	}
}

func TestScoreAIFailureStoresNullRecommendation(t *testing.T) {
	for name, recommender := range map[string]Recommender{
		"provider error": fakeRecommender{err: &RecommendationError{Code: RecommendationErrInvalidJSON, Err: errors.New("prose")}},
		"plain error":    fakeRecommender{err: errors.New("boom")},
		"disabled":       nil,
	} {
		store := &fakeStore{}
		service := newTestService(t, caughtUpLog(), store, recommender)
		outcome, err := service.Score(context.Background(), testTrigger(), false)
		if err != nil || outcome != OutcomeScored {
			t.Fatalf("%s: Score = %v, %v", name, outcome, err)
		}
		score := store.saved[0]
		if score.Recommendation != nil || score.RecommendationError == "" || score.Metrics.PromptCount != 1 {
			t.Fatalf("%s: score = %+v", name, score)
		}
		var payload events.ScoreComputed
		var envelope events.Envelope
		_ = json.Unmarshal(store.messages[0].Envelope, &envelope)
		_ = envelope.Decode(&payload)
		if payload.Recommendation != "" || payload.RecommendationErr != score.RecommendationError {
			t.Fatalf("%s: event = %+v", name, payload)
		}
	}
}

func TestScoreAITimeoutDoesNotBlockTheRow(t *testing.T) {
	store := &fakeStore{}
	service := newTestService(t, caughtUpLog(), store, blockingRecommender{})
	service.config.RecommendTimeout = 20 * time.Millisecond
	if _, err := service.Score(context.Background(), testTrigger(), false); err != nil {
		t.Fatal(err)
	}
	if store.saved[0].RecommendationError != RecommendationErrTimeout {
		t.Fatalf("error code = %q", store.saved[0].RecommendationError)
	}
}

func TestScoreSkipsAlreadyScoredSessionBeforeAnyWork(t *testing.T) {
	recommender := &countingRecommender{}
	log := caughtUpLog()
	service := newTestService(t, log, &fakeStore{exists: true}, recommender)
	outcome, err := service.Score(context.Background(), testTrigger(), false)
	if err != nil || outcome != OutcomeAlreadyScored {
		t.Fatalf("Score = %v, %v", outcome, err)
	}
	if recommender.calls != 0 || log.timelineCalls != 0 {
		t.Fatalf("did work for a scored session: AI calls %d, timeline reads %d", recommender.calls, log.timelineCalls)
	}
}

func TestScoreLostInsertRaceIsAlreadyScored(t *testing.T) {
	service := newTestService(t, caughtUpLog(), &fakeStore{conflict: true}, nil)
	if outcome, err := service.Score(context.Background(), testTrigger(), false); err != nil || outcome != OutcomeAlreadyScored {
		t.Fatalf("Score = %v, %v", outcome, err)
	}
}

func TestScoreBehindLogNacksUntilFinalAttempt(t *testing.T) {
	log := caughtUpLog()
	log.progress = Progress{MaxSequence: 4, Present: 3}
	store := &fakeStore{}
	service := newTestService(t, log, store, nil)
	if _, err := service.Score(context.Background(), testTrigger(), false); !errors.Is(err, ErrBehind) {
		t.Fatalf("err = %v, want ErrBehind", err)
	}
	if len(store.saved) != 0 {
		t.Fatal("stored a score while behind")
	}
	if _, err := service.Score(context.Background(), testTrigger(), true); err != nil {
		t.Fatal(err)
	}
	if store.saved[0].MetricsComplete {
		t.Fatal("final attempt on a behind log must mark metrics incomplete")
	}
}

func TestScorePermanentFailures(t *testing.T) {
	log := caughtUpLog()
	log.timeline = []metrics.Event{{SequenceNumber: 1, EventType: events.EventTypeCodeDiff, Payload: json.RawMessage(`{"lines_added":"x"}`)}}
	var permanent *PermanentError
	if _, err := newTestService(t, log, &fakeStore{}, nil).Score(context.Background(), testTrigger(), false); !errors.As(err, &permanent) {
		t.Fatalf("malformed timeline err = %v", err)
	}
	_, err := newTestService(t, caughtUpLog(), &fakeStore{err: ErrInterviewGone}, nil).Score(context.Background(), testTrigger(), false)
	if !errors.As(err, &permanent) || !errors.Is(err, ErrInterviewGone) {
		t.Fatalf("purged interview err = %v", err)
	}
}

func TestScoreTransientFailuresAreReturned(t *testing.T) {
	down := errors.New("connection refused")
	log := caughtUpLog()
	log.err = down
	var permanent *PermanentError
	_, err := newTestService(t, log, &fakeStore{}, nil).Score(context.Background(), testTrigger(), false)
	if !errors.Is(err, down) || errors.As(err, &permanent) {
		t.Fatalf("err = %v", err)
	}
}

func newTestService(t *testing.T, log EventLog, store Store, recommender Recommender) *Service {
	t.Helper()
	service, err := NewService(log, store, recommender, Config{})
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC) }
	return service
}

func caughtUpLog() *fakeLog {
	at := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	return &fakeLog{
		progress: Progress{MaxSequence: 4, Present: 4},
		timeline: []metrics.Event{
			{SequenceNumber: 1, EventType: events.EventTypeSessionStarted, OccurredAt: at, Payload: json.RawMessage(`{}`)},
			{SequenceNumber: 2, EventType: events.EventTypePromptSubmitted, OccurredAt: at.Add(time.Minute), Payload: json.RawMessage(`{}`)},
			{SequenceNumber: 3, EventType: events.EventTypeExecutionCompleted, OccurredAt: at.Add(2 * time.Minute), Payload: json.RawMessage(`{"status":"succeeded"}`)},
			{SequenceNumber: 4, EventType: events.EventTypeSessionSubmitted, OccurredAt: at.Add(3 * time.Minute), Payload: json.RawMessage(`{}`)},
		},
	}
}

type fakeLog struct {
	progress      Progress
	timeline      []metrics.Event
	err           error
	upTo          int64
	timelineCalls int
}

func (l *fakeLog) Progress(context.Context, uuid.UUID, uuid.UUID, int64) (Progress, error) {
	return l.progress, l.err
}

func (l *fakeLog) SessionTimeline(_ context.Context, _, _, _ uuid.UUID, upTo int64) ([]metrics.Event, error) {
	l.timelineCalls++
	l.upTo = upTo
	return l.timeline, l.err
}

type fakeStore struct {
	exists   bool
	conflict bool
	err      error
	saved    []Score
	messages []outbox.Message
}

func (s *fakeStore) ScoreExists(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	return s.exists, nil
}

func (s *fakeStore) SaveScore(_ context.Context, score Score, build func(int64) (outbox.Message, error)) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if s.conflict {
		return false, nil
	}
	message, err := build(9)
	if err != nil {
		return false, err
	}
	s.saved = append(s.saved, score)
	s.messages = append(s.messages, message)
	return true, nil
}

type fakeRecommender struct {
	rec   Recommendation
	model string
	err   error
}

func (r fakeRecommender) Recommend(context.Context, metrics.Metrics) (Recommendation, string, error) {
	return r.rec, r.model, r.err
}

type blockingRecommender struct{}

func (blockingRecommender) Recommend(ctx context.Context, _ metrics.Metrics) (Recommendation, string, error) {
	<-ctx.Done()
	return Recommendation{}, "", ctx.Err()
}

type countingRecommender struct{ calls int }

func (r *countingRecommender) Recommend(context.Context, metrics.Metrics) (Recommendation, string, error) {
	r.calls++
	return Recommendation{}, "", errors.New("unused")
}
