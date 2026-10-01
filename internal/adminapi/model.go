package adminapi

import (
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/rbac"
)

// ErrInvalid wraps every request validation failure.
var ErrInvalid = errors.New("invalid request")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

const (
	maxNameLength  = 200
	maxTextLength  = 10_000
	defaultLimit   = 50
	maxLimit       = 200
	candidateRole  = "candidate"
	maxInviteTTL   = 30 * 24 * time.Hour
	defaultInvite  = 7 * 24 * time.Hour
	inviteTokenLen = 32
)

// Policy keys a company can toggle, with their defaults when unset.
var policyDefaults = map[string]bool{
	"ai_assistance":   true,
	"code_execution":  true,
	"live_monitoring": true,
	"replay":          true,
	"ai_scoring":      true,
}

// PolicyKeys returns the known policy keys in a stable order.
func PolicyKeys() []string {
	return []string{"ai_assistance", "ai_scoring", "code_execution", "live_monitoring", "replay"}
}

// PolicyDefault reports a policy's default and whether the key is known.
func PolicyDefault(key string) (enabled, known bool) {
	enabled, known = policyDefaults[key]
	return enabled, known
}

// MergePolicies fills every known key missing from stored with its default
// and drops unknown stored keys, in PolicyKeys order.
func MergePolicies(stored []domain.Policy) []domain.Policy {
	byKey := make(map[string]domain.Policy, len(stored))
	for _, p := range stored {
		byKey[p.Key] = p
	}
	merged := make([]domain.Policy, 0, len(policyDefaults))
	for _, key := range PolicyKeys() {
		if p, ok := byKey[key]; ok {
			merged = append(merged, p)
			continue
		}
		merged = append(merged, domain.Policy{Key: key, Enabled: policyDefaults[key]})
	}
	return merged
}

func validName(field, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", invalid("%s is required", field)
	}
	if len(value) > maxNameLength {
		return "", invalid("%s must be at most %d bytes", field, maxNameLength)
	}
	return value, nil
}

func validText(field, value string) (string, error) {
	if len(value) > maxTextLength {
		return "", invalid("%s must be at most %d bytes", field, maxTextLength)
	}
	return value, nil
}

// ValidEmail normalizes an email address, rejecting display-name forms.
func ValidEmail(value string) (string, error) {
	value = strings.TrimSpace(value)
	address, err := mail.ParseAddress(value)
	if err != nil || address.Address != value || len(value) > maxNameLength {
		return "", invalid("email must be a plain email address")
	}
	return strings.ToLower(value), nil
}

// ValidInterviewStatus reports whether status is a known interview status.
func ValidInterviewStatus(status string) bool {
	switch status {
	case domain.InterviewStatusScheduled, domain.InterviewStatusInProgress,
		domain.InterviewStatusCompleted, domain.InterviewStatusCancelled:
		return true
	}
	return false
}

// TerminalInterviewStatus reports whether status ends an interview.
func TerminalInterviewStatus(status string) bool {
	return status == domain.InterviewStatusCompleted || status == domain.InterviewStatusCancelled
}

// Task statuses.
const (
	TaskStatusOpen       = "open"
	TaskStatusInProgress = "in_progress"
	TaskStatusDone       = "done"
)

// ValidTaskStatus reports whether status is a known task status.
func ValidTaskStatus(status string) bool {
	return status == TaskStatusOpen || status == TaskStatusInProgress || status == TaskStatusDone
}

// ValidateScoreDecision enforces that a decision is a human one: status is
// one of the human_* statuses, an adjustment carries a final value, and
// only an adjustment may set one.
func ValidateScoreDecision(status string, finalValue *float64) error {
	switch status {
	case domain.ScoreStatusHumanApproved, domain.ScoreStatusHumanRejected:
		if finalValue != nil {
			return invalid("final_value is only accepted with %s", domain.ScoreStatusHumanAdjusted)
		}
	case domain.ScoreStatusHumanAdjusted:
		if finalValue == nil {
			return invalid("%s requires final_value", domain.ScoreStatusHumanAdjusted)
		}
	default:
		return invalid("status must be one of %s, %s, %s",
			domain.ScoreStatusHumanApproved, domain.ScoreStatusHumanRejected, domain.ScoreStatusHumanAdjusted)
	}
	return nil
}

// CanManageRole reports whether actor may grant target, or change or remove
// a member who holds target. Only owners manage owners; admins manage
// everyone else.
func CanManageRole(actor, target rbac.Role) bool {
	if !rbac.Can(actor, rbac.PermUsersWrite) {
		return false
	}
	return target != rbac.RoleOwner || actor == rbac.RoleOwner
}

// --- JSON views ---

type companyView struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func viewCompany(c domain.Company) companyView {
	return companyView{ID: c.ID, Name: c.Name, Slug: c.Slug, CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt}
}

type userView struct {
	ID          uuid.UUID `json:"id"`
	Email       string    `json:"email"`
	DisplayName string    `json:"display_name"`
	Role        string    `json:"role"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func viewUser(u domain.User) userView {
	return userView{ID: u.ID, Email: u.Email, DisplayName: u.DisplayName, Role: u.Role, CreatedAt: u.CreatedAt, UpdatedAt: u.UpdatedAt}
}

type interviewView struct {
	ID               uuid.UUID  `json:"id"`
	CandidateName    string     `json:"candidate_name"`
	CandidateEmail   string     `json:"candidate_email"`
	Status           string     `json:"status"`
	CreatedBy        *uuid.UUID `json:"created_by"`
	ScheduledAt      *time.Time `json:"scheduled_at"`
	TerminalAt       *time.Time `json:"terminal_at"`
	EraseRequestedAt *time.Time `json:"erase_requested_at"`
	LegalHold        bool       `json:"legal_hold"`
	PurgedAt         *time.Time `json:"purged_at"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

func viewInterview(i domain.Interview) interviewView {
	return interviewView{
		ID: i.ID, CandidateName: i.CandidateName, CandidateEmail: i.CandidateEmail, Status: i.Status,
		CreatedBy: i.CreatedBy, ScheduledAt: i.ScheduledAt, TerminalAt: i.TerminalAt,
		EraseRequestedAt: i.EraseRequestedAt, LegalHold: i.LegalHold, PurgedAt: i.PurgedAt,
		CreatedAt: i.CreatedAt, UpdatedAt: i.UpdatedAt,
	}
}

type taskView struct {
	ID          uuid.UUID  `json:"id"`
	InterviewID *uuid.UUID `json:"interview_id"`
	AssigneeID  *uuid.UUID `json:"assignee_id"`
	Title       string     `json:"title"`
	Description string     `json:"description"`
	Status      string     `json:"status"`
	DueAt       *time.Time `json:"due_at"`
	CompletedAt *time.Time `json:"completed_at"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func viewTask(t domain.Task) taskView {
	return taskView{
		ID: t.ID, InterviewID: t.InterviewID, AssigneeID: t.AssigneeID, Title: t.Title,
		Description: t.Description, Status: t.Status, DueAt: t.DueAt, CompletedAt: t.CompletedAt,
		CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt,
	}
}

type scoreView struct {
	ID            uuid.UUID  `json:"id"`
	InterviewID   uuid.UUID  `json:"interview_id"`
	Dimension     string     `json:"dimension"`
	ProposedValue float64    `json:"proposed_value"`
	Rationale     string     `json:"rationale"`
	Status        string     `json:"status"`
	FinalValue    *float64   `json:"final_value"`
	DecisionNote  string     `json:"decision_note"`
	DecidedBy     *uuid.UUID `json:"decided_by"`
	DecidedAt     *time.Time `json:"decided_at"`
}

func viewScore(s domain.Score) scoreView {
	return scoreView{
		ID: s.ID, InterviewID: s.InterviewID, Dimension: s.Dimension, ProposedValue: s.ProposedValue,
		Rationale: s.Rationale, Status: s.Status, FinalValue: s.FinalValue, DecisionNote: s.DecisionNote,
		DecidedBy: s.DecidedBy, DecidedAt: s.DecidedAt,
	}
}

type policyView struct {
	Key       string     `json:"key"`
	Enabled   bool       `json:"enabled"`
	UpdatedBy *uuid.UUID `json:"updated_by"`
	UpdatedAt *time.Time `json:"updated_at"`
}

func viewPolicy(p domain.Policy) policyView {
	return policyView{Key: p.Key, Enabled: p.Enabled, UpdatedBy: p.UpdatedBy, UpdatedAt: p.UpdatedAt}
}

type inviteView struct {
	ID          uuid.UUID  `json:"id"`
	Email       string     `json:"email"`
	Role        string     `json:"role"`
	InterviewID *uuid.UUID `json:"interview_id"`
	ExpiresAt   time.Time  `json:"expires_at"`
	// Token is returned exactly once, at creation; only its hash is stored.
	Token string `json:"token"`
}

func mapSlice[T, V any](in []T, view func(T) V) []V {
	out := make([]V, 0, len(in))
	for _, item := range in {
		out = append(out, view(item))
	}
	return out
}
