package events

// Event type constants form the platform's event catalog. Kafka topic names
// are expected to follow the pattern "ta.<domain>.<event-type>".
const (
	EventTypeJobPosted             EventType = "job.posted"
	EventTypeJobClosed             EventType = "job.closed"
	EventTypeCandidateApplied      EventType = "candidate.applied"
	EventTypeCandidateStageChanged EventType = "candidate.stage_changed"
	EventTypeCandidateRejected     EventType = "candidate.rejected"
	EventTypeCandidateHired        EventType = "candidate.hired"
	EventTypeInterviewScheduled    EventType = "interview.scheduled"
	EventTypeInterviewCompleted    EventType = "interview.completed"
)

// JobPosted is emitted when a company publishes a new job requisition.
type JobPosted struct {
	JobID    string `json:"job_id"`
	Title    string `json:"title"`
	Location string `json:"location"`
	PostedBy string `json:"posted_by"`
	RemoteOK bool   `json:"remote_ok"`
}

func (JobPosted) EventType() EventType { return EventTypeJobPosted }
func (JobPosted) SchemaVersion() int   { return 1 }

// JobClosed is emitted when a job requisition is closed or filled.
type JobClosed struct {
	JobID  string `json:"job_id"`
	Reason string `json:"reason"`
}

func (JobClosed) EventType() EventType { return EventTypeJobClosed }
func (JobClosed) SchemaVersion() int   { return 1 }

// CandidateApplied is emitted when a candidate submits an application.
type CandidateApplied struct {
	CandidateID   string `json:"candidate_id"`
	JobID         string `json:"job_id"`
	ApplicationID string `json:"application_id"`
	ResumeURL     string `json:"resume_url"`
}

func (CandidateApplied) EventType() EventType { return EventTypeCandidateApplied }
func (CandidateApplied) SchemaVersion() int   { return 1 }

// CandidateStageChanged is emitted whenever a candidate moves through the
// hiring pipeline (e.g. "screening" -> "onsite").
type CandidateStageChanged struct {
	CandidateID   string `json:"candidate_id"`
	ApplicationID string `json:"application_id"`
	FromStage     string `json:"from_stage"`
	ToStage       string `json:"to_stage"`
	ChangedBy     string `json:"changed_by"`
}

func (CandidateStageChanged) EventType() EventType { return EventTypeCandidateStageChanged }
func (CandidateStageChanged) SchemaVersion() int   { return 1 }

// CandidateRejected is emitted when a candidate is rejected from a pipeline.
type CandidateRejected struct {
	CandidateID   string `json:"candidate_id"`
	ApplicationID string `json:"application_id"`
	Reason        string `json:"reason"`
}

func (CandidateRejected) EventType() EventType { return EventTypeCandidateRejected }
func (CandidateRejected) SchemaVersion() int   { return 1 }

// CandidateHired is emitted when an offer is accepted and the candidate is
// hired.
type CandidateHired struct {
	CandidateID   string `json:"candidate_id"`
	ApplicationID string `json:"application_id"`
	JobID         string `json:"job_id"`
	StartDate     string `json:"start_date"`
}

func (CandidateHired) EventType() EventType { return EventTypeCandidateHired }
func (CandidateHired) SchemaVersion() int   { return 1 }

// InterviewScheduled is emitted when an interview is booked.
type InterviewScheduled struct {
	InterviewID   string `json:"interview_id"`
	ApplicationID string `json:"application_id"`
	InterviewerID string `json:"interviewer_id"`
	ScheduledAt   string `json:"scheduled_at"`
}

func (InterviewScheduled) EventType() EventType { return EventTypeInterviewScheduled }
func (InterviewScheduled) SchemaVersion() int   { return 1 }

// InterviewCompleted is emitted when an interview's feedback is submitted.
type InterviewCompleted struct {
	InterviewID   string `json:"interview_id"`
	ApplicationID string `json:"application_id"`
	Outcome       string `json:"outcome"`
	Feedback      string `json:"feedback"`
}

func (InterviewCompleted) EventType() EventType { return EventTypeInterviewCompleted }
func (InterviewCompleted) SchemaVersion() int   { return 1 }
