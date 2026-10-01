// Package adminapi is the company-facing Admin API: tenant-scoped CRUD for
// the company, its members, interviews and tasks; invite creation; human
// score decisions; policy toggles; and erasure requests.
//
// Every operation takes the company ID from the authenticated principal
// (see internal/rbac), never from the request, and every Store method is
// scoped by it.
package adminapi

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/rbac"
)

var (
	// ErrNotFound means no record with the ID exists in the caller's company.
	ErrNotFound = errors.New("not found")
	// ErrConflict means the change collides with an existing record.
	ErrConflict = errors.New("conflict")
	// ErrInvalidReference means a referenced record (interview, assignee) is
	// not in the caller's company.
	ErrInvalidReference = errors.New("invalid reference")
)

// Store is the Admin API's persistence. Every method is scoped by companyID;
// implementations must never read or write another company's rows.
type Store interface {
	rbac.RoleFinder

	GetCompany(ctx context.Context, companyID uuid.UUID) (domain.Company, error)
	UpdateCompany(ctx context.Context, companyID uuid.UUID, patch CompanyPatch) (domain.Company, error)

	ListUsers(ctx context.Context, companyID uuid.UUID) ([]domain.User, error)
	GetUser(ctx context.Context, companyID, userID uuid.UUID) (domain.User, error)
	CreateUser(ctx context.Context, user domain.User) (domain.User, error)
	UpdateUser(ctx context.Context, companyID, userID uuid.UUID, patch UserPatch) (domain.User, error)
	DeleteUser(ctx context.Context, companyID, userID uuid.UUID) error

	ListInterviews(ctx context.Context, companyID uuid.UUID, filter InterviewFilter) ([]domain.Interview, error)
	GetInterview(ctx context.Context, companyID, interviewID uuid.UUID) (domain.Interview, error)
	CreateInterview(ctx context.Context, interview domain.Interview) (domain.Interview, error)
	UpdateInterview(ctx context.Context, companyID, interviewID uuid.UUID, patch InterviewPatch) (domain.Interview, error)
	// RequestErasure only marks the interview: it sets erase_requested_at
	// (keeping the first request's time) and deletes nothing.
	RequestErasure(ctx context.Context, companyID, interviewID uuid.UUID, at time.Time) (domain.Interview, error)

	ListTasks(ctx context.Context, companyID uuid.UUID, filter TaskFilter) ([]domain.Task, error)
	GetTask(ctx context.Context, companyID, taskID uuid.UUID) (domain.Task, error)
	CreateTask(ctx context.Context, task domain.Task) (domain.Task, error)
	UpdateTask(ctx context.Context, companyID, taskID uuid.UUID, patch TaskPatch) (domain.Task, error)
	DeleteTask(ctx context.Context, companyID, taskID uuid.UUID) error

	CreateInvite(ctx context.Context, invite domain.Invite) (domain.Invite, error)

	ListScores(ctx context.Context, companyID, interviewID uuid.UUID) ([]domain.Score, error)
	DecideScore(ctx context.Context, companyID, interviewID, scoreID uuid.UUID, decision ScoreDecision) (domain.Score, error)

	ListPolicies(ctx context.Context, companyID uuid.UUID) ([]domain.Policy, error)
	SetPolicy(ctx context.Context, companyID uuid.UUID, key string, enabled bool, by uuid.UUID) (domain.Policy, error)
}

// CompanyPatch holds the company fields a member may change.
type CompanyPatch struct {
	Name *string
}

// UserPatch holds the member fields an admin may change.
type UserPatch struct {
	DisplayName *string
	Role        *rbac.Role
}

// InterviewFilter narrows ListInterviews.
type InterviewFilter struct {
	Status string
	Limit  int
}

// InterviewPatch holds the interview fields a member may change. TerminalAt
// is set by the API when Status becomes terminal.
type InterviewPatch struct {
	CandidateName  *string
	CandidateEmail *string
	Status         *string
	ScheduledAt    *time.Time
	TerminalAt     *time.Time
}

// TaskFilter narrows ListTasks.
type TaskFilter struct {
	InterviewID *uuid.UUID
	AssigneeID  *uuid.UUID
	Status      string
	Limit       int
}

// TaskPatch holds the task fields a member may change. A non-nil pointer to
// uuid.Nil clears the assignee.
type TaskPatch struct {
	Title       *string
	Description *string
	Status      *string
	AssigneeID  *uuid.UUID
	DueAt       *time.Time
	CompletedAt *time.Time
}

// ScoreDecision is a human's decision on a proposed score.
type ScoreDecision struct {
	Status     string
	FinalValue *float64
	Note       string
	DecidedBy  uuid.UUID
	DecidedAt  time.Time
}
