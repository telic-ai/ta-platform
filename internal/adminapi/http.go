package adminapi

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/rbac"
)

const maxBodyBytes = 64 << 10

// Config holds the Admin API's clock, ID and token sources and invite TTL.
// Zero values select production defaults.
type Config struct {
	Now       func() time.Time
	NewID     func() uuid.UUID
	NewToken  func() (string, error)
	InviteTTL time.Duration
}

// Handler serves the Admin API. Every route runs behind rbac.Middleware and
// is guarded by one permission.
type Handler struct {
	store    Store
	resolver rbac.PrincipalResolver
	cfg      Config
	mux      *http.ServeMux
}

// NewHandler returns an Admin API handler over store, authenticating with
// resolver.
func NewHandler(store Store, resolver rbac.PrincipalResolver, cfg Config) *Handler {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NewID == nil {
		cfg.NewID = uuid.New
	}
	if cfg.NewToken == nil {
		cfg.NewToken = NewInviteToken
	}
	if cfg.InviteTTL <= 0 || cfg.InviteTTL > maxInviteTTL {
		cfg.InviteTTL = defaultInvite
	}
	h := &Handler{store: store, resolver: resolver, cfg: cfg, mux: http.NewServeMux()}
	h.routes()
	return h
}

// Handle mounts another guarded route (live monitoring, analytics, search
// keys) behind the same authentication.
func (h *Handler) Handle(pattern string, permission rbac.Permission, handler http.HandlerFunc) *Handler {
	h.mux.HandleFunc(pattern, rbac.Require(permission, handler))
	return h
}

// Routes returns the authenticated handler.
func (h *Handler) Routes() http.Handler { return rbac.Middleware(h.resolver, h.mux) }

func (h *Handler) routes() {
	h.Handle("GET /company", rbac.PermCompanyRead, h.getCompany)
	h.Handle("PATCH /company", rbac.PermCompanyWrite, h.updateCompany)

	h.Handle("GET /users", rbac.PermUsersRead, h.listUsers)
	h.Handle("POST /users", rbac.PermUsersWrite, h.createUser)
	h.Handle("GET /users/{id}", rbac.PermUsersRead, h.getUser)
	h.Handle("PATCH /users/{id}", rbac.PermUsersWrite, h.updateUser)
	h.Handle("DELETE /users/{id}", rbac.PermUsersWrite, h.deleteUser)

	h.Handle("GET /interviews", rbac.PermInterviewsRead, h.listInterviews)
	h.Handle("POST /interviews", rbac.PermInterviewsWrite, h.createInterview)
	h.Handle("GET /interviews/{id}", rbac.PermInterviewsRead, h.getInterview)
	h.Handle("PATCH /interviews/{id}", rbac.PermInterviewsWrite, h.updateInterview)
	h.Handle("POST /interviews/{id}/erase", rbac.PermInterviewsErase, h.eraseInterview)
	h.Handle("GET /interviews/{id}/scores", rbac.PermScoresRead, h.listScores)
	h.Handle("POST /interviews/{id}/scores/{scoreId}/decision", rbac.PermScoresApprove, h.decideScore)

	h.Handle("GET /tasks", rbac.PermTasksRead, h.listTasks)
	h.Handle("POST /tasks", rbac.PermTasksWrite, h.createTask)
	h.Handle("GET /tasks/{id}", rbac.PermTasksRead, h.getTask)
	h.Handle("PATCH /tasks/{id}", rbac.PermTasksWrite, h.updateTask)
	h.Handle("DELETE /tasks/{id}", rbac.PermTasksWrite, h.deleteTask)

	// Member invites need PermInvitesMember; createInvite checks it once the
	// body says which kind of invite this is.
	h.Handle("POST /invites", rbac.PermInvitesCandidate, h.createInvite)

	h.Handle("GET /policies", rbac.PermPoliciesRead, h.listPolicies)
	h.Handle("PUT /policies/{key}", rbac.PermPoliciesWrite, h.setPolicy)
}

// NewInviteToken returns a random, URL-safe bearer token.
func NewInviteToken() (string, error) {
	raw := make([]byte, inviteTokenLen)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate invite token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// --- company ---

func (h *Handler) getCompany(w http.ResponseWriter, r *http.Request) {
	p := principal(r)
	company, err := h.store.GetCompany(r.Context(), p.CompanyID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewCompany(company))
}

func (h *Handler) updateCompany(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name *string `json:"name"`
	}
	if !decode(w, r, &body) {
		return
	}
	var patch CompanyPatch
	if body.Name != nil {
		name, err := validName("name", *body.Name)
		if err != nil {
			writeInvalid(w, err)
			return
		}
		patch.Name = &name
	}
	company, err := h.store.UpdateCompany(r.Context(), principal(r).CompanyID, patch)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewCompany(company))
}

// --- users ---

func (h *Handler) listUsers(w http.ResponseWriter, r *http.Request) {
	users, err := h.store.ListUsers(r.Context(), principal(r).CompanyID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": mapSlice(users, viewUser)})
}

func (h *Handler) getUser(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	user, err := h.store.GetUser(r.Context(), principal(r).CompanyID, id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewUser(user))
}

func (h *Handler) createUser(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Email       string `json:"email"`
		DisplayName string `json:"display_name"`
		Role        string `json:"role"`
	}
	if !decode(w, r, &body) {
		return
	}
	p := principal(r)
	email, err := ValidEmail(body.Email)
	if err != nil {
		writeInvalid(w, err)
		return
	}
	role := rbac.Role(body.Role)
	if !role.Valid() {
		writeInvalid(w, invalid("role must be one of owner, admin, recruiter, interviewer, viewer"))
		return
	}
	if !CanManageRole(p.Role, role) {
		rbac.WriteError(w, http.StatusForbidden, "forbidden", fmt.Sprintf("role %q may not grant %q", p.Role, role))
		return
	}
	displayName, err := validText("display_name", body.DisplayName)
	if err != nil {
		writeInvalid(w, err)
		return
	}
	user, err := h.store.CreateUser(r.Context(), domain.User{
		ID: h.cfg.NewID(), CompanyID: p.CompanyID, Email: email, DisplayName: displayName, Role: string(role),
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, viewUser(user))
}

func (h *Handler) updateUser(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	var body struct {
		DisplayName *string `json:"display_name"`
		Role        *string `json:"role"`
	}
	if !decode(w, r, &body) {
		return
	}
	p := principal(r)
	target, err := h.store.GetUser(r.Context(), p.CompanyID, id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !CanManageRole(p.Role, rbac.Role(target.Role)) {
		rbac.WriteError(w, http.StatusForbidden, "forbidden", fmt.Sprintf("role %q may not change a %q", p.Role, target.Role))
		return
	}
	var patch UserPatch
	if body.DisplayName != nil {
		name, err := validText("display_name", *body.DisplayName)
		if err != nil {
			writeInvalid(w, err)
			return
		}
		patch.DisplayName = &name
	}
	if body.Role != nil {
		role := rbac.Role(*body.Role)
		if !role.Valid() {
			writeInvalid(w, invalid("role must be one of owner, admin, recruiter, interviewer, viewer"))
			return
		}
		if !CanManageRole(p.Role, role) {
			rbac.WriteError(w, http.StatusForbidden, "forbidden", fmt.Sprintf("role %q may not grant %q", p.Role, role))
			return
		}
		// Members cannot demote themselves, so a company is never left
		// without the member who is managing it.
		if id == p.UserID && role != p.Role {
			rbac.WriteError(w, http.StatusConflict, "self_role_change", "members cannot change their own role")
			return
		}
		patch.Role = &role
	}
	user, err := h.store.UpdateUser(r.Context(), p.CompanyID, id, patch)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewUser(user))
}

func (h *Handler) deleteUser(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	p := principal(r)
	if id == p.UserID {
		rbac.WriteError(w, http.StatusConflict, "self_delete", "members cannot remove themselves")
		return
	}
	target, err := h.store.GetUser(r.Context(), p.CompanyID, id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !CanManageRole(p.Role, rbac.Role(target.Role)) {
		rbac.WriteError(w, http.StatusForbidden, "forbidden", fmt.Sprintf("role %q may not remove a %q", p.Role, target.Role))
		return
	}
	if err := h.store.DeleteUser(r.Context(), p.CompanyID, id); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- interviews ---

func (h *Handler) listInterviews(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	if status != "" && !ValidInterviewStatus(status) {
		writeInvalid(w, invalid("unknown status %q", status))
		return
	}
	limit, ok := queryLimit(w, r)
	if !ok {
		return
	}
	interviews, err := h.store.ListInterviews(r.Context(), principal(r).CompanyID, InterviewFilter{Status: status, Limit: limit})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"interviews": mapSlice(interviews, viewInterview)})
}

func (h *Handler) getInterview(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	interview, err := h.store.GetInterview(r.Context(), principal(r).CompanyID, id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewInterview(interview))
}

func (h *Handler) createInterview(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CandidateName  string     `json:"candidate_name"`
		CandidateEmail string     `json:"candidate_email"`
		ScheduledAt    *time.Time `json:"scheduled_at"`
	}
	if !decode(w, r, &body) {
		return
	}
	name, err := validName("candidate_name", body.CandidateName)
	if err != nil {
		writeInvalid(w, err)
		return
	}
	email, err := ValidEmail(body.CandidateEmail)
	if err != nil {
		writeInvalid(w, err)
		return
	}
	p := principal(r)
	createdBy := p.UserID
	interview, err := h.store.CreateInterview(r.Context(), domain.Interview{
		ID: h.cfg.NewID(), CompanyID: p.CompanyID, CreatedBy: &createdBy, CandidateName: name,
		CandidateEmail: email, Status: domain.InterviewStatusScheduled, ScheduledAt: body.ScheduledAt,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, viewInterview(interview))
}

func (h *Handler) updateInterview(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	var body struct {
		CandidateName  *string    `json:"candidate_name"`
		CandidateEmail *string    `json:"candidate_email"`
		Status         *string    `json:"status"`
		ScheduledAt    *time.Time `json:"scheduled_at"`
	}
	if !decode(w, r, &body) {
		return
	}
	patch := InterviewPatch{ScheduledAt: body.ScheduledAt}
	if body.CandidateName != nil {
		name, err := validName("candidate_name", *body.CandidateName)
		if err != nil {
			writeInvalid(w, err)
			return
		}
		patch.CandidateName = &name
	}
	if body.CandidateEmail != nil {
		email, err := ValidEmail(*body.CandidateEmail)
		if err != nil {
			writeInvalid(w, err)
			return
		}
		patch.CandidateEmail = &email
	}
	if body.Status != nil {
		if !ValidInterviewStatus(*body.Status) {
			writeInvalid(w, invalid("unknown status %q", *body.Status))
			return
		}
		patch.Status = body.Status
		if TerminalInterviewStatus(*body.Status) {
			now := h.cfg.Now().UTC()
			patch.TerminalAt = &now
		}
	}
	interview, err := h.store.UpdateInterview(r.Context(), principal(r).CompanyID, id, patch)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewInterview(interview))
}

// eraseInterview records an erasure request and returns 202: the purge
// itself is asynchronous and honors legal holds. Nothing is deleted here.
func (h *Handler) eraseInterview(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	interview, err := h.store.RequestErasure(r.Context(), principal(r).CompanyID, id, h.cfg.Now().UTC())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"id":                 interview.ID,
		"erase_requested_at": interview.EraseRequestedAt,
		"legal_hold":         interview.LegalHold,
	})
}

// --- scores ---

func (h *Handler) listScores(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	p := principal(r)
	if _, err := h.store.GetInterview(r.Context(), p.CompanyID, id); err != nil {
		writeStoreError(w, err)
		return
	}
	scores, err := h.store.ListScores(r.Context(), p.CompanyID, id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scores": mapSlice(scores, viewScore)})
}

func (h *Handler) decideScore(w http.ResponseWriter, r *http.Request) {
	interviewID, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	scoreID, ok := pathID(w, r, "scoreId")
	if !ok {
		return
	}
	var body struct {
		Status     string   `json:"status"`
		FinalValue *float64 `json:"final_value"`
		Note       string   `json:"note"`
	}
	if !decode(w, r, &body) {
		return
	}
	if err := ValidateScoreDecision(body.Status, body.FinalValue); err != nil {
		writeInvalid(w, err)
		return
	}
	note, err := validText("note", body.Note)
	if err != nil {
		writeInvalid(w, err)
		return
	}
	p := principal(r)
	score, err := h.store.DecideScore(r.Context(), p.CompanyID, interviewID, scoreID, ScoreDecision{
		Status: body.Status, FinalValue: body.FinalValue, Note: note, DecidedBy: p.UserID, DecidedAt: h.cfg.Now().UTC(),
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewScore(score))
}

// --- tasks ---

func (h *Handler) listTasks(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	var filter TaskFilter
	for name, target := range map[string]**uuid.UUID{"interview_id": &filter.InterviewID, "assignee_id": &filter.AssigneeID} {
		if raw := query.Get(name); raw != "" {
			id, err := uuid.Parse(raw)
			if err != nil {
				writeInvalid(w, invalid("%s must be a UUID", name))
				return
			}
			*target = &id
		}
	}
	filter.Status = query.Get("status")
	if filter.Status != "" && !ValidTaskStatus(filter.Status) {
		writeInvalid(w, invalid("unknown status %q", filter.Status))
		return
	}
	limit, ok := queryLimit(w, r)
	if !ok {
		return
	}
	filter.Limit = limit
	tasks, err := h.store.ListTasks(r.Context(), principal(r).CompanyID, filter)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": mapSlice(tasks, viewTask)})
}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	task, err := h.store.GetTask(r.Context(), principal(r).CompanyID, id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewTask(task))
}

func (h *Handler) createTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		InterviewID *uuid.UUID `json:"interview_id"`
		AssigneeID  *uuid.UUID `json:"assignee_id"`
		Title       string     `json:"title"`
		Description string     `json:"description"`
		DueAt       *time.Time `json:"due_at"`
	}
	if !decode(w, r, &body) {
		return
	}
	title, err := validName("title", body.Title)
	if err != nil {
		writeInvalid(w, err)
		return
	}
	description, err := validText("description", body.Description)
	if err != nil {
		writeInvalid(w, err)
		return
	}
	task, err := h.store.CreateTask(r.Context(), domain.Task{
		ID: h.cfg.NewID(), CompanyID: principal(r).CompanyID, InterviewID: body.InterviewID,
		AssigneeID: body.AssigneeID, Title: title, Description: description, Status: TaskStatusOpen, DueAt: body.DueAt,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, viewTask(task))
}

func (h *Handler) updateTask(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	var body struct {
		Title       *string             `json:"title"`
		Description *string             `json:"description"`
		Status      *string             `json:"status"`
		AssigneeID  optional[uuid.UUID] `json:"assignee_id"`
		DueAt       *time.Time          `json:"due_at"`
	}
	if !decode(w, r, &body) {
		return
	}
	patch := TaskPatch{DueAt: body.DueAt}
	if body.Title != nil {
		title, err := validName("title", *body.Title)
		if err != nil {
			writeInvalid(w, err)
			return
		}
		patch.Title = &title
	}
	if body.Description != nil {
		description, err := validText("description", *body.Description)
		if err != nil {
			writeInvalid(w, err)
			return
		}
		patch.Description = &description
	}
	if body.Status != nil {
		if !ValidTaskStatus(*body.Status) {
			writeInvalid(w, invalid("unknown status %q", *body.Status))
			return
		}
		patch.Status = body.Status
		if *body.Status == TaskStatusDone {
			now := h.cfg.Now().UTC()
			patch.CompletedAt = &now
		}
	}
	if body.AssigneeID.Set {
		assignee := uuid.Nil
		if body.AssigneeID.Value != nil {
			assignee = *body.AssigneeID.Value
		}
		patch.AssigneeID = &assignee
	}
	task, err := h.store.UpdateTask(r.Context(), principal(r).CompanyID, id, patch)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewTask(task))
}

func (h *Handler) deleteTask(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	if err := h.store.DeleteTask(r.Context(), principal(r).CompanyID, id); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- invites ---

// createInvite issues a tokenized invite. With interview_id it invites a
// candidate to that interview's Candidate Workspace; otherwise it invites a
// company member with role. The raw token is returned once and only its
// hash is stored.
func (h *Handler) createInvite(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Email       string     `json:"email"`
		Role        string     `json:"role"`
		InterviewID *uuid.UUID `json:"interview_id"`
		ExpiresIn   int64      `json:"expires_in_seconds"`
	}
	if !decode(w, r, &body) {
		return
	}
	p := principal(r)
	email, err := ValidEmail(body.Email)
	if err != nil {
		writeInvalid(w, err)
		return
	}
	role := candidateRole
	if body.InterviewID == nil {
		if !p.Can(rbac.PermInvitesMember) {
			rbac.WriteError(w, http.StatusForbidden, "forbidden", fmt.Sprintf("role %q may not %s", p.Role, rbac.PermInvitesMember))
			return
		}
		memberRole := rbac.Role(body.Role)
		if !memberRole.Valid() {
			writeInvalid(w, invalid("role must be one of owner, admin, recruiter, interviewer, viewer"))
			return
		}
		if !CanManageRole(p.Role, memberRole) {
			rbac.WriteError(w, http.StatusForbidden, "forbidden", fmt.Sprintf("role %q may not grant %q", p.Role, memberRole))
			return
		}
		role = string(memberRole)
	} else if body.Role != "" && body.Role != candidateRole {
		writeInvalid(w, invalid("candidate invites take no role"))
		return
	}
	ttl := h.cfg.InviteTTL
	if body.ExpiresIn != 0 {
		ttl = time.Duration(body.ExpiresIn) * time.Second
		if body.ExpiresIn < 0 || ttl > maxInviteTTL {
			writeInvalid(w, invalid("expires_in_seconds must be between 1 and %d", int64(maxInviteTTL/time.Second)))
			return
		}
	}
	token, err := h.cfg.NewToken()
	if err != nil {
		rbac.WriteError(w, http.StatusServiceUnavailable, "token_unavailable", "could not issue an invite token")
		return
	}
	invitedBy := p.UserID
	invite, err := h.store.CreateInvite(r.Context(), domain.Invite{
		ID: h.cfg.NewID(), CompanyID: p.CompanyID, InterviewID: body.InterviewID, InvitedBy: &invitedBy,
		Email: email, Role: role, TokenHash: auth.HashToken(token), ExpiresAt: h.cfg.Now().UTC().Add(ttl),
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, inviteView{
		ID: invite.ID, Email: invite.Email, Role: invite.Role, InterviewID: invite.InterviewID,
		ExpiresAt: invite.ExpiresAt, Token: token,
	})
}

// --- policies ---

func (h *Handler) listPolicies(w http.ResponseWriter, r *http.Request) {
	stored, err := h.store.ListPolicies(r.Context(), principal(r).CompanyID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"policies": mapSlice(MergePolicies(stored), viewPolicy)})
}

func (h *Handler) setPolicy(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if _, known := PolicyDefault(key); !known {
		rbac.WriteError(w, http.StatusNotFound, "unknown_policy", fmt.Sprintf("unknown policy %q", key))
		return
	}
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if !decode(w, r, &body) {
		return
	}
	if body.Enabled == nil {
		writeInvalid(w, invalid("enabled is required"))
		return
	}
	p := principal(r)
	policy, err := h.store.SetPolicy(r.Context(), p.CompanyID, key, *body.Enabled, p.UserID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewPolicy(policy))
}

// --- helpers ---

// principal returns the request's principal. Routes are only reachable
// through rbac.Middleware, which always stores one.
func principal(r *http.Request) rbac.Principal {
	p, _ := rbac.FromContext(r.Context())
	return p
}

// optional distinguishes an absent JSON field from an explicit null.
type optional[T any] struct {
	Set   bool
	Value *T
}

func (o *optional[T]) UnmarshalJSON(data []byte) error {
	o.Set = true
	if string(data) == "null" {
		o.Value = nil
		return nil
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	o.Value = &value
	return nil
}

func decode(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			rbac.WriteError(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body is too large")
			return false
		}
		rbac.WriteError(w, http.StatusBadRequest, "invalid_json", "request body must be a single JSON object")
		return false
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		rbac.WriteError(w, http.StatusBadRequest, "invalid_json", "request body must be a single JSON object")
		return false
	}
	return true
}

func pathID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		rbac.WriteError(w, http.StatusBadRequest, "invalid_"+name, name+" must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

func queryLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return defaultLimit, true
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > maxLimit {
		writeInvalid(w, invalid("limit must be between 1 and %d", maxLimit))
		return 0, false
	}
	return limit, true
}

func writeInvalid(w http.ResponseWriter, err error) {
	rbac.WriteError(w, http.StatusBadRequest, "invalid_request", err.Error())
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		rbac.WriteError(w, http.StatusNotFound, "not_found", "not found")
	case errors.Is(err, ErrConflict):
		rbac.WriteError(w, http.StatusConflict, "conflict", "conflicts with an existing record")
	case errors.Is(err, ErrInvalid):
		writeInvalid(w, err)
	case errors.Is(err, ErrInvalidReference):
		rbac.WriteError(w, http.StatusUnprocessableEntity, "invalid_reference", "a referenced record does not exist")
	default:
		rbac.WriteError(w, http.StatusServiceUnavailable, "store_unavailable", "temporarily unavailable")
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
