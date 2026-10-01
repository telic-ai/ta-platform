// Package metrics computes a Candidate Workspace session's scoring metrics
// from its event timeline. Every function is pure: the same events always
// yield the same metrics, whatever their order or duplication.
//
// The ClickHouse view v_session_metrics (internal/analytics) computes the
// same counters in SQL; a fixture test keeps the two in agreement, so any
// change here must be mirrored there.
//
// NOTE: [[Scoring Service – Internals]] was not available when this package
// was written; the metric set is a reasonable placeholder to reconcile
// against it.
package metrics

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/telic-ai/ta-platform/internal/events"
)

// Event is one timeline entry as stored in the event log.
type Event struct {
	SequenceNumber int64
	EventType      events.EventType
	OccurredAt     time.Time
	Payload        json.RawMessage
}

// Metrics are a session's raw counters. Derived ratios are methods so that
// stored metrics stay exact integers.
type Metrics struct {
	PromptCount     int64 `json:"prompt_count"`
	AIResponseCount int64 `json:"ai_response_count"`
	AIErrorCount    int64 `json:"ai_error_count"`
	AIInputTokens   int64 `json:"ai_input_tokens"`
	AIOutputTokens  int64 `json:"ai_output_tokens"`

	RunCount          int64  `json:"run_count"`
	RunSucceededCount int64  `json:"run_succeeded_count"`
	LastRunStatus     string `json:"last_run_status"`

	DiffCount    int64 `json:"diff_count"`
	AIDiffCount  int64 `json:"ai_diff_count"`
	LinesAdded   int64 `json:"lines_added"`
	LinesRemoved int64 `json:"lines_removed"`
	AILinesAdded int64 `json:"ai_lines_added"`

	// ActiveDurationMS spans the session's first to last activity event.
	ActiveDurationMS int64 `json:"active_duration_ms"`
	// TimeToFirstRunMS is from the first activity event to the first run;
	// nil when the candidate never ran their code.
	TimeToFirstRunMS *int64 `json:"time_to_first_run_ms"`
}

// RunSuccessRate is the share of finished runs that succeeded, 0 with no runs.
func (m Metrics) RunSuccessRate() float64 { return ratio(m.RunSucceededCount, m.RunCount) }

// AILineShare is the share of added lines that came from applied AI
// suggestions, 0 with no added lines.
func (m Metrics) AILineShare() float64 { return ratio(m.AILinesAdded, m.LinesAdded) }

func ratio(part, whole int64) float64 {
	if whole == 0 {
		return 0
	}
	return float64(part) / float64(whole)
}

// ActivityTypes are the event types that describe a session. Anything else,
// including the score.computed written after it, is ignored.
var ActivityTypes = []events.EventType{
	events.EventTypeSessionStarted,
	events.EventTypePromptSubmitted,
	events.EventTypeAIResponseCompleted,
	events.EventTypeExecutionRequested,
	events.EventTypeExecutionCompleted,
	events.EventTypeCodeDiff,
	events.EventTypeSessionSubmitted,
	events.EventTypeSessionExpired,
}

func isActivity(eventType events.EventType) bool {
	for _, t := range ActivityTypes {
		if t == eventType {
			return true
		}
	}
	return false
}

// Compute derives a session's metrics. Events sharing a sequence number are
// redeliveries of one event; only the first is counted.
func Compute(timeline []Event) (Metrics, error) {
	ordered := Dedupe(timeline)
	var m Metrics
	var first, last, firstRun time.Time
	lastRunSequence := int64(-1)
	for _, event := range ordered {
		if !isActivity(event.EventType) {
			continue
		}
		at := event.OccurredAt
		if first.IsZero() || at.Before(first) {
			first = at
		}
		if at.After(last) {
			last = at
		}
		switch event.EventType {
		case events.EventTypePromptSubmitted:
			m.PromptCount++
		case events.EventTypeAIResponseCompleted:
			var p events.AIResponseCompleted
			if err := decode(event, &p); err != nil {
				return Metrics{}, err
			}
			m.AIResponseCount++
			if p.Status == events.AIResponseStatusError {
				m.AIErrorCount++
			}
			m.AIInputTokens += p.InputTokens
			m.AIOutputTokens += p.OutputTokens
		case events.EventTypeExecutionRequested:
			if firstRun.IsZero() || at.Before(firstRun) {
				firstRun = at
			}
		case events.EventTypeExecutionCompleted:
			var p events.ExecutionCompleted
			if err := decode(event, &p); err != nil {
				return Metrics{}, err
			}
			m.RunCount++
			if p.Status == events.ExecutionStatusSucceeded {
				m.RunSucceededCount++
			}
			if event.SequenceNumber > lastRunSequence {
				lastRunSequence, m.LastRunStatus = event.SequenceNumber, p.Status
			}
		case events.EventTypeCodeDiff:
			var p events.CodeDiff
			if err := decode(event, &p); err != nil {
				return Metrics{}, err
			}
			m.DiffCount++
			m.LinesAdded += int64(p.LinesAdded)
			m.LinesRemoved += int64(p.LinesRemoved)
			if p.Origin == events.DiffOriginAIApplied {
				m.AIDiffCount++
				m.AILinesAdded += int64(p.LinesAdded)
			}
		}
	}
	if !first.IsZero() {
		m.ActiveDurationMS = last.Sub(first).Milliseconds()
	}
	if !firstRun.IsZero() {
		ms := firstRun.Sub(first).Milliseconds()
		m.TimeToFirstRunMS = &ms
	}
	return m, nil
}

// Dedupe returns timeline ordered by sequence number with later copies of
// a sequence number dropped.
func Dedupe(timeline []Event) []Event {
	ordered := make([]Event, len(timeline))
	copy(ordered, timeline)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].SequenceNumber < ordered[j].SequenceNumber })
	result := make([]Event, 0, len(ordered))
	for _, event := range ordered {
		if n := len(result); n > 0 && result[n-1].SequenceNumber == event.SequenceNumber {
			continue
		}
		result = append(result, event)
	}
	return result
}

func decode(event Event, dst any) error {
	if err := json.Unmarshal(event.Payload, dst); err != nil {
		return fmt.Errorf("metrics: decode %s #%d: %w", event.EventType, event.SequenceNumber, err)
	}
	return nil
}
