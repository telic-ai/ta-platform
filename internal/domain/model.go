// Package domain contains the persistence-neutral platform domain model.
package domain

import (
	"time"

	"github.com/google/uuid"
)

// Company is a tenant. All other persisted domain records belong to one
// company and carry its ID explicitly.
type Company struct {
	ID        uuid.UUID
	Name      string
	Slug      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// User is a company member.
type User struct {
	ID          uuid.UUID
	CompanyID   uuid.UUID
	Email       string
	DisplayName string
	Role        string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Task is tenant-scoped interview work assigned to an optional user.
type Task struct {
	ID          uuid.UUID
	CompanyID   uuid.UUID
	InterviewID *uuid.UUID
	AssigneeID  *uuid.UUID
	Title       string
	Description string
	Status      string
	DueAt       *time.Time
	CompletedAt *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Interview is a tenant-scoped candidate interview and its retention state.
type Interview struct {
	ID               uuid.UUID
	CompanyID        uuid.UUID
	CreatedBy        *uuid.UUID
	CandidateName    string
	CandidateEmail   string
	Status           string
	ScheduledAt      *time.Time
	TerminalAt       *time.Time
	EraseRequestedAt *time.Time
	LegalHold        bool
	PurgedAt         *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// Invite is a time-limited invitation to join a company.
type Invite struct {
	ID         uuid.UUID
	CompanyID  uuid.UUID
	InvitedBy  *uuid.UUID
	Email      string
	Role       string
	TokenHash  []byte
	ExpiresAt  time.Time
	AcceptedAt *time.Time
	CreatedAt  time.Time
}

// Session is an authenticated user session within a company.
type Session struct {
	ID         uuid.UUID
	CompanyID  uuid.UUID
	UserID     uuid.UUID
	State      SessionState
	TokenHash  []byte
	ExpiresAt  time.Time
	RevokedAt  *time.Time
	CreatedAt  time.Time
	LastSeenAt *time.Time
}

// SessionState is the lifecycle state of a Candidate Workspace session.
type SessionState string

const (
	SessionStateActive    SessionState = "active"
	SessionStateCompleted SessionState = "completed"
	SessionStateExpired   SessionState = "expired"
	SessionStateRevoked   SessionState = "revoked"
)

// CanTransitionTo defines the deliberately small initial session state
// machine. Terminal sessions cannot be reactivated.
func (s SessionState) CanTransitionTo(next SessionState) bool {
	if s == next {
		return true
	}
	return s == SessionStateActive &&
		(next == SessionStateCompleted || next == SessionStateExpired || next == SessionStateRevoked)
}
