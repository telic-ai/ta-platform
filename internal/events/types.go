package events

import "time"

// SessionEventsTopic is the Event Catalog topic for session lifecycle events.
// Records are keyed by session_id so one session's events stay ordered on one
// partition.
const SessionEventsTopic = "session-events"

// Event type constants form the platform's event catalog.
const (
	EventTypeJobPosted             EventType = "job.posted"
	EventTypeJobClosed             EventType = "job.closed"
	EventTypeCandidateApplied      EventType = "candidate.applied"
	EventTypeCandidateStageChanged EventType = "candidate.stage_changed"
	EventTypeCandidateRejected     EventType = "candidate.rejected"
	EventTypeCandidateHired        EventType = "candidate.hired"
	EventTypeInterviewScheduled    EventType = "interview.scheduled"
	EventTypeInterviewCompleted    EventType = "interview.completed"
	EventTypeSessionStarted        EventType = "session.started"
	EventTypeAIResponseCompleted   EventType = "ai.response.completed"
	EventTypePromptSubmitted       EventType = "prompt.submitted"
	EventTypeExecutionRequested    EventType = "execution.requested"
	EventTypeExecutionCompleted    EventType = "execution.completed"
	EventTypeCodeDiff              EventType = "code.diff"
)

// SessionStarted is emitted after an invite has been exchanged for an
// Active Candidate Workspace session. Tokens are never included in events.
type SessionStarted struct {
	SessionID   string    `json:"session_id"`
	InterviewID string    `json:"interview_id"`
	InviteID    string    `json:"invite_id"`
	Scope       string    `json:"scope"`
	ExpiresAt   time.Time `json:"expires_at"`
}

func (SessionStarted) EventType() EventType { return EventTypeSessionStarted }
func (SessionStarted) SchemaVersion() int   { return 1 }

// PromptSubmitted records a candidate prompt before it is sent to the AI
// Gateway. CompletionSequenceNumber is reserved for the prompt's
// ai.response.completed, so the pair is ordered by sequence number.
type PromptSubmitted struct {
	SessionID                string `json:"session_id"`
	InterviewID              string `json:"interview_id"`
	PromptID                 string `json:"prompt_id"`
	Prompt                   string `json:"prompt"`
	HistoryTurns             int    `json:"history_turns"`
	CompletionSequenceNumber int64  `json:"completion_sequence_number"`
}

func (PromptSubmitted) EventType() EventType { return EventTypePromptSubmitted }
func (PromptSubmitted) SchemaVersion() int   { return 1 }

// ExecutionRequested records a run after its snapshot is stored and before
// the sandbox is called.
type ExecutionRequested struct {
	SessionID     string `json:"session_id"`
	InterviewID   string `json:"interview_id"`
	ExecutionID   string `json:"execution_id"`
	Language      string `json:"language"`
	Entrypoint    string `json:"entrypoint"`
	SnapshotKey   string `json:"snapshot_key"`
	FileCount     int    `json:"file_count"`
	SnapshotBytes int    `json:"snapshot_bytes"`
}

func (ExecutionRequested) EventType() EventType { return EventTypeExecutionRequested }
func (ExecutionRequested) SchemaVersion() int   { return 1 }

// Execution statuses. error means the sandbox itself failed; the others
// describe the candidate's program.
const (
	ExecutionStatusSucceeded = "succeeded"
	ExecutionStatusFailed    = "failed"
	ExecutionStatusTimedOut  = "timed_out"
	ExecutionStatusOOMKilled = "oom_killed"
	ExecutionStatusError     = "error"
)

// ExecutionCompleted records a run's outcome. Output is capped; the
// truncated flags say whether anything was cut.
type ExecutionCompleted struct {
	SessionID       string `json:"session_id"`
	InterviewID     string `json:"interview_id"`
	ExecutionID     string `json:"execution_id"`
	Status          string `json:"status"`
	ExitCode        int    `json:"exit_code"`
	DurationMS      int64  `json:"duration_ms"`
	Stdout          string `json:"stdout"`
	Stderr          string `json:"stderr"`
	StdoutTruncated bool   `json:"stdout_truncated"`
	StderrTruncated bool   `json:"stderr_truncated"`
	ErrorCode       string `json:"error_code,omitempty"`
}

func (ExecutionCompleted) EventType() EventType { return EventTypeExecutionCompleted }
func (ExecutionCompleted) SchemaVersion() int   { return 1 }

// Code diff origins: typed by the candidate, or an AI suggestion the
// candidate applied. Their ratio is a scoring signal.
const (
	DiffOriginManual    = "manual"
	DiffOriginAIApplied = "ai_applied"
)

// CodeDiff records one accepted edit to a workspace file as a unified diff.
// ClientSequence is the workspace client's per-session edit counter.
type CodeDiff struct {
	SessionID      string `json:"session_id"`
	InterviewID    string `json:"interview_id"`
	ClientSequence int64  `json:"client_sequence"`
	Origin         string `json:"origin"`
	PromptID       string `json:"prompt_id,omitempty"`
	Path           string `json:"path"`
	Patch          string `json:"patch"`
	LinesAdded     int    `json:"lines_added"`
	LinesRemoved   int    `json:"lines_removed"`
}

func (CodeDiff) EventType() EventType { return EventTypeCodeDiff }
func (CodeDiff) SchemaVersion() int   { return 1 }

// AI response statuses. ai.response.completed is emitted for every
// completion attempt, whatever its outcome.
const (
	AIResponseStatusCompleted = "completed"
	AIResponseStatusTruncated = "truncated"
	AIResponseStatusRefused   = "refused"
	AIResponseStatusError     = "error"
	AIResponseStatusCancelled = "cancelled"
)

// AIResponseCompleted records the outcome of one AI Gateway completion.
// ResponseText holds whatever was streamed before the completion ended.
// Credentials are never included.
type AIResponseCompleted struct {
	SessionID    string `json:"session_id"`
	InterviewID  string `json:"interview_id"`
	PromptID     string `json:"prompt_id"`
	Mode         string `json:"mode"`
	Provider     string `json:"provider"`
	Model        string `json:"model"`
	Status       string `json:"status"`
	StopReason   string `json:"stop_reason,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
	ResponseText string `json:"response_text"`
	InputTokens  int64  `json:"input_tokens"`
	OutputTokens int64  `json:"output_tokens"`
	LatencyMS    int64  `json:"latency_ms"`
}

func (AIResponseCompleted) EventType() EventType { return EventTypeAIResponseCompleted }
func (AIResponseCompleted) SchemaVersion() int   { return 1 }

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
