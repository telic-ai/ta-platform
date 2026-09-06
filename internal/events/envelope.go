// Package events defines the Kafka event envelope and the typed payloads
// for every event in the platform's event catalog.
//
// NOTE: the source design doc ([[Event Catalog]]) was not available when
// this package was scaffolded; the event types below are a reasonable
// placeholder set for a talent-acquisition platform (candidates, jobs,
// applications, interviews) and should be reconciled against the real
// catalog once it is accessible.
package events

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// EventType identifies the shape of an envelope's payload.
type EventType string

// Payload is implemented by every typed event payload in the catalog.
type Payload interface {
	EventType() EventType
	SchemaVersion() int
}

// Envelope is the outer structure written to every Kafka topic. Consumers
// can always inspect CompanyID, SequenceNumber, and SchemaVersion without
// decoding the inner Payload.
type Envelope struct {
	EventID        string          `json:"event_id"`
	EventType      EventType       `json:"event_type"`
	SchemaVersion  int             `json:"schema_version"`
	CompanyID      string          `json:"company_id"`
	SequenceNumber int64           `json:"sequence_number"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
}

// New builds an Envelope around payload, stamping it with a fresh event ID
// and the current time.
func New(companyID string, sequenceNumber int64, payload Payload) (Envelope, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("events: marshal payload: %w", err)
	}

	id, err := newEventID()
	if err != nil {
		return Envelope{}, fmt.Errorf("events: generate event id: %w", err)
	}

	return Envelope{
		EventID:        id,
		EventType:      payload.EventType(),
		SchemaVersion:  payload.SchemaVersion(),
		CompanyID:      companyID,
		SequenceNumber: sequenceNumber,
		OccurredAt:     time.Now().UTC(),
		Payload:        body,
	}, nil
}

// Decode unmarshals the envelope's payload into dst, which must be a
// pointer to the concrete payload type matching e.EventType.
func (e Envelope) Decode(dst Payload) error {
	if e.EventType != dst.EventType() {
		return fmt.Errorf("events: envelope type %q does not match target type %q", e.EventType, dst.EventType())
	}
	if err := json.Unmarshal(e.Payload, dst); err != nil {
		return fmt.Errorf("events: unmarshal payload: %w", err)
	}
	return nil
}

func newEventID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
