// Package fixtures holds recorded session timelines and the metrics each one
// must produce. The metrics unit tests and the ClickHouse view test share
// them, so the Go and SQL computations are checked against the same data.
package fixtures

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"

	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics"
)

//go:embed sessions/*.json
var files embed.FS

// Session is one fixture: the envelopes as delivered (possibly duplicated
// or out of order) and the metrics they must yield.
type Session struct {
	Name        string            `json:"name"`
	CompanyID   string            `json:"company_id"`
	InterviewID string            `json:"interview_id"`
	SessionID   string            `json:"session_id"`
	Events      []json.RawMessage `json:"events"`
	Want        metrics.Metrics   `json:"want"`
}

// Timeline decodes the fixture's envelopes into metric events.
func (s Session) Timeline() ([]metrics.Event, error) {
	timeline := make([]metrics.Event, 0, len(s.Events))
	for i, raw := range s.Events {
		var envelope events.Envelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, fmt.Errorf("fixture %q event %d: %w", s.Name, i, err)
		}
		timeline = append(timeline, metrics.Event{
			SequenceNumber: envelope.SequenceNumber, EventType: envelope.EventType,
			OccurredAt: envelope.OccurredAt, Payload: envelope.Payload,
		})
	}
	return timeline, nil
}

// All returns every fixture, ordered by file name.
func All() ([]Session, error) {
	names, err := fs.Glob(files, "sessions/*.json")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	sessions := make([]Session, 0, len(names))
	for _, name := range names {
		raw, err := files.ReadFile(name)
		if err != nil {
			return nil, err
		}
		var session Session
		if err := json.Unmarshal(raw, &session); err != nil {
			return nil, fmt.Errorf("fixture %s: %w", name, err)
		}
		sessions = append(sessions, session)
	}
	return sessions, nil
}
