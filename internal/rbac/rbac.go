// Package rbac resolves the authenticated company member behind a request
// (company_id, user_id, role) and decides what that role may do.
//
// The principal comes only from the bearer token's session and the member's
// stored role; nothing in the request path, query or body can select a
// tenant or raise a role.
package rbac

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/auth"
)

// Role is a company member's role. The set is fixed by a check constraint on
// users.role.
type Role string

const (
	RoleOwner       Role = "owner"
	RoleAdmin       Role = "admin"
	RoleRecruiter   Role = "recruiter"
	RoleInterviewer Role = "interviewer"
	RoleViewer      Role = "viewer"
)

// Roles lists every valid role, most privileged first.
var Roles = []Role{RoleOwner, RoleAdmin, RoleRecruiter, RoleInterviewer, RoleViewer}

// Valid reports whether r is a known role.
func (r Role) Valid() bool {
	for _, known := range Roles {
		if r == known {
			return true
		}
	}
	return false
}

// Permission is one guarded action.
type Permission string

const (
	PermCompanyRead      Permission = "company:read"
	PermCompanyWrite     Permission = "company:write"
	PermUsersRead        Permission = "users:read"
	PermUsersWrite       Permission = "users:write"
	PermInterviewsRead   Permission = "interviews:read"
	PermInterviewsWrite  Permission = "interviews:write"
	PermInterviewsErase  Permission = "interviews:erase"
	PermInterviewsLive   Permission = "interviews:live"
	PermTasksRead        Permission = "tasks:read"
	PermTasksWrite       Permission = "tasks:write"
	PermInvitesCandidate Permission = "invites:candidate"
	PermInvitesMember    Permission = "invites:member"
	PermScoresRead       Permission = "scores:read"
	PermScoresApprove    Permission = "scores:approve"
	PermPoliciesRead     Permission = "policies:read"
	PermPoliciesWrite    Permission = "policies:write"
	PermAnalyticsRead    Permission = "analytics:read"
	PermSearch           Permission = "search"
)

var (
	everyone   = []Role{RoleOwner, RoleAdmin, RoleRecruiter, RoleInterviewer, RoleViewer}
	admins     = []Role{RoleOwner, RoleAdmin}
	hiring     = []Role{RoleOwner, RoleAdmin, RoleRecruiter}
	evaluators = []Role{RoleOwner, RoleAdmin, RoleInterviewer}
	staff      = []Role{RoleOwner, RoleAdmin, RoleRecruiter, RoleInterviewer}
)

// policy is the permission matrix. A permission missing from it is denied to
// everyone.
var policy = map[Permission][]Role{
	PermCompanyRead:      everyone,
	PermCompanyWrite:     admins,
	PermUsersRead:        everyone,
	PermUsersWrite:       admins,
	PermInterviewsRead:   everyone,
	PermInterviewsWrite:  hiring,
	PermInterviewsErase:  admins,
	PermInterviewsLive:   staff,
	PermTasksRead:        everyone,
	PermTasksWrite:       staff,
	PermInvitesCandidate: hiring,
	PermInvitesMember:    admins,
	PermScoresRead:       staff,
	PermScoresApprove:    evaluators,
	PermPoliciesRead:     everyone,
	PermPoliciesWrite:    admins,
	PermAnalyticsRead:    everyone,
	PermSearch:           everyone,
}

// Can reports whether role holds permission.
func Can(role Role, permission Permission) bool {
	for _, allowed := range policy[permission] {
		if role == allowed {
			return true
		}
	}
	return false
}

// Principal is the authenticated company member making a request.
type Principal struct {
	CompanyID uuid.UUID
	UserID    uuid.UUID
	Role      Role
}

// Can reports whether the principal's role holds permission.
func (p Principal) Can(permission Permission) bool { return Can(p.Role, permission) }

// ErrUnauthenticated means the request carries no usable member session.
var ErrUnauthenticated = errors.New("company member identity is required")

// ErrUserNotFound means the session's user no longer exists in its company.
var ErrUserNotFound = errors.New("user not found")

// RoleFinder reads a member's role within their company.
type RoleFinder interface {
	UserRole(ctx context.Context, companyID, userID uuid.UUID) (Role, error)
}

// Resolver authenticates a company member's bearer token.
type Resolver struct {
	Sessions auth.SessionFinder
	Roles    RoleFinder
	Now      func() time.Time
}

// Resolve returns the request's principal. Candidate Workspace sessions,
// inactive sessions, and members whose user row is gone are unauthenticated.
// Other errors mean identity could not be checked.
func (r Resolver) Resolve(req *http.Request) (Principal, error) {
	token, ok := auth.BearerToken(req)
	if !ok {
		return Principal{}, ErrUnauthenticated
	}
	session, err := r.Sessions.FindSessionByTokenHash(req.Context(), auth.HashToken(token))
	if errors.Is(err, auth.ErrSessionNotFound) {
		return Principal{}, ErrUnauthenticated
	}
	if err != nil {
		return Principal{}, fmt.Errorf("resolve member session: %w", err)
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	if session.UserID == nil || session.IsCandidate() || !session.IsActive(now()) {
		return Principal{}, ErrUnauthenticated
	}
	role, err := r.Roles.UserRole(req.Context(), session.CompanyID, *session.UserID)
	if errors.Is(err, ErrUserNotFound) {
		return Principal{}, ErrUnauthenticated
	}
	if err != nil {
		return Principal{}, fmt.Errorf("resolve member role: %w", err)
	}
	if !role.Valid() {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{CompanyID: session.CompanyID, UserID: *session.UserID, Role: role}, nil
}

// PrincipalResolver is what Middleware needs; Resolver implements it.
type PrincipalResolver interface {
	Resolve(*http.Request) (Principal, error)
}

// ResolverFunc adapts a function to PrincipalResolver.
type ResolverFunc func(*http.Request) (Principal, error)

func (f ResolverFunc) Resolve(r *http.Request) (Principal, error) { return f(r) }

type principalKey struct{}

// WithPrincipal returns ctx carrying p.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext returns the principal Middleware stored on ctx.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// Middleware authenticates every request, answering 401 or 503 itself, and
// stores the principal on the request context for Require and handlers.
func Middleware(resolver PrincipalResolver, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := resolver.Resolve(r)
		if errors.Is(err, ErrUnauthenticated) {
			WriteError(w, http.StatusUnauthorized, "unauthorized", "authenticated company member is required")
			return
		}
		if err != nil {
			WriteError(w, http.StatusServiceUnavailable, "identity_unavailable", "could not verify identity")
			return
		}
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), principal)))
	})
}

// Require wraps a handler so it runs only for principals holding permission.
// It must sit behind Middleware.
func Require(permission Permission, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, ok := FromContext(r.Context())
		if !ok {
			WriteError(w, http.StatusUnauthorized, "unauthorized", "authenticated company member is required")
			return
		}
		if !principal.Can(permission) {
			WriteError(w, http.StatusForbidden, "forbidden", fmt.Sprintf("role %q may not %s", principal.Role, permission))
			return
		}
		next(w, r)
	}
}

// WriteError writes the platform's JSON error body.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": message})
}
