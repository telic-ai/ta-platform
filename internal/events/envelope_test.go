package events

import (
	"encoding/json"
	"testing"
)

func TestNewAndDecodeRoundTrip(t *testing.T) {
	payload := CandidateApplied{
		CandidateID:   "cand_1",
		JobID:         "job_1",
		ApplicationID: "app_1",
		ResumeURL:     "s3://bucket/resume.pdf",
	}

	env, err := New("company_1", 42, payload)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if env.CompanyID != "company_1" {
		t.Errorf("CompanyID = %q, want %q", env.CompanyID, "company_1")
	}
	if env.SequenceNumber != 42 {
		t.Errorf("SequenceNumber = %d, want 42", env.SequenceNumber)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1", env.SchemaVersion)
	}
	if env.EventType != EventTypeCandidateApplied {
		t.Errorf("EventType = %q, want %q", env.EventType, EventTypeCandidateApplied)
	}
	if env.EventID == "" {
		t.Error("EventID is empty")
	}
	if env.OccurredAt.IsZero() {
		t.Error("OccurredAt is zero")
	}

	var decoded CandidateApplied
	if err := env.Decode(&decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded != payload {
		t.Errorf("decoded payload = %+v, want %+v", decoded, payload)
	}
}

func TestEnvelopeJSONRoundTrip(t *testing.T) {
	payload := JobPosted{
		JobID:    "job_1",
		Title:    "Staff Engineer",
		Location: "Remote",
		PostedBy: "user_1",
		RemoteOK: true,
	}

	env, err := New("company_1", 7, payload)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var roundTripped Envelope
	if err := json.Unmarshal(raw, &roundTripped); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if roundTripped.CompanyID != env.CompanyID ||
		roundTripped.SequenceNumber != env.SequenceNumber ||
		roundTripped.SchemaVersion != env.SchemaVersion ||
		roundTripped.EventType != env.EventType ||
		roundTripped.EventID != env.EventID {
		t.Fatalf("round-tripped envelope = %+v, want %+v", roundTripped, env)
	}

	var decoded JobPosted
	if err := roundTripped.Decode(&decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded != payload {
		t.Errorf("decoded payload = %+v, want %+v", decoded, payload)
	}
}

func TestDecodeMismatchedTypeFails(t *testing.T) {
	env, err := New("company_1", 1, JobPosted{JobID: "job_1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var wrong CandidateApplied
	if err := env.Decode(&wrong); err == nil {
		t.Error("Decode with mismatched type should have failed")
	}
}

func TestEachEventTypeRoundTrips(t *testing.T) {
	payloads := []Payload{
		JobPosted{JobID: "job_1"},
		JobClosed{JobID: "job_1", Reason: "filled"},
		CandidateApplied{CandidateID: "cand_1"},
		CandidateStageChanged{CandidateID: "cand_1", FromStage: "screen", ToStage: "onsite"},
		CandidateRejected{CandidateID: "cand_1", Reason: "no fit"},
		CandidateHired{CandidateID: "cand_1"},
		InterviewScheduled{InterviewID: "iv_1"},
		InterviewCompleted{InterviewID: "iv_1", Outcome: "pass"},
	}

	for _, p := range payloads {
		p := p
		t.Run(string(p.EventType()), func(t *testing.T) {
			env, err := New("company_1", 1, p)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if env.EventType != p.EventType() {
				t.Errorf("EventType = %q, want %q", env.EventType, p.EventType())
			}
			if env.SchemaVersion != p.SchemaVersion() {
				t.Errorf("SchemaVersion = %d, want %d", env.SchemaVersion, p.SchemaVersion())
			}
		})
	}
}
