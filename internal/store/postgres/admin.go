package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/adminapi"
	"github.com/telic-ai/ta-platform/internal/dashboard"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/livemonitor"
	"github.com/telic-ai/ta-platform/internal/rbac"
)

// AdminStore implements adminapi.Store. Every statement is scoped by
// company_id (see adminQueries and its test).
type AdminStore struct{ pool *pgxpool.Pool }

var (
	_ adminapi.Store         = (*AdminStore)(nil)
	_ livemonitor.Access     = (*AdminStore)(nil)
	_ dashboard.ReplayAccess = (*AdminStore)(nil)
)

func NewAdminStore(pool *pgxpool.Pool) *AdminStore { return &AdminStore{pool: pool} }

const (
	userColumns      = `id, company_id, email, display_name, role, created_at, updated_at`
	interviewColumns = `id, company_id, created_by, candidate_name, candidate_email, status, scheduled_at,
		terminal_at, erase_requested_at, legal_hold, purged_at, last_sequence_number, created_at, updated_at`
	taskColumns  = `id, company_id, interview_id, assignee_id, title, description, status, due_at, completed_at, created_at, updated_at`
	scoreColumns = `id, company_id, interview_id, dimension, proposed_value, rationale, status, final_value,
		decision_note, decided_by, decided_at, created_at, updated_at`
)

// adminQueries holds every statement AdminStore runs, so a test can check
// that each one is company-scoped.
var adminQueries = map[string]string{
	"userRole":      `SELECT role FROM users WHERE company_id = $1 AND id = $2`,
	"getCompany":    `SELECT id, name, slug, created_at, updated_at FROM companies WHERE id = $1`,
	"updateCompany": `UPDATE companies SET name = COALESCE($2, name), updated_at = now() WHERE id = $1 RETURNING id, name, slug, created_at, updated_at`,

	"listUsers":  `SELECT ` + userColumns + ` FROM users WHERE company_id = $1 ORDER BY email`,
	"getUser":    `SELECT ` + userColumns + ` FROM users WHERE company_id = $1 AND id = $2`,
	"createUser": `INSERT INTO users (company_id, id, email, display_name, role) VALUES ($1, $2, $3, $4, $5) RETURNING ` + userColumns,
	"updateUser": `UPDATE users SET display_name = COALESCE($3, display_name), role = COALESCE($4, role), updated_at = now()
		WHERE company_id = $1 AND id = $2 RETURNING ` + userColumns,
	"deleteUser": `DELETE FROM users WHERE company_id = $1 AND id = $2`,

	"listInterviews": `SELECT ` + interviewColumns + ` FROM interviews
		WHERE company_id = $1 AND ($2 = '' OR status = $2) ORDER BY created_at DESC, id LIMIT $3`,
	"getInterview": `SELECT ` + interviewColumns + ` FROM interviews WHERE company_id = $1 AND id = $2`,
	"createInterview": `INSERT INTO interviews (company_id, id, created_by, candidate_name, candidate_email, status, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING ` + interviewColumns,
	"updateInterview": `UPDATE interviews SET
		candidate_name = COALESCE($3, candidate_name), candidate_email = COALESCE($4, candidate_email),
		status = COALESCE($5, status), scheduled_at = COALESCE($6, scheduled_at),
		terminal_at = COALESCE(terminal_at, $7), updated_at = now()
		WHERE company_id = $1 AND id = $2 RETURNING ` + interviewColumns,
	// Erasure only marks: the first request's time is kept and nothing is
	// deleted. The purge job acts on erase_requested_at later.
	"requestErasure": `UPDATE interviews SET erase_requested_at = COALESCE(erase_requested_at, $3), updated_at = now()
		WHERE company_id = $1 AND id = $2 RETURNING ` + interviewColumns,

	"listTasks": `SELECT ` + taskColumns + ` FROM tasks
		WHERE company_id = $1 AND ($2::uuid IS NULL OR interview_id = $2) AND ($3::uuid IS NULL OR assignee_id = $3)
		  AND ($4 = '' OR status = $4)
		ORDER BY created_at DESC, id LIMIT $5`,
	"getTask": `SELECT ` + taskColumns + ` FROM tasks WHERE company_id = $1 AND id = $2`,
	"createTask": `INSERT INTO tasks (company_id, id, interview_id, assignee_id, title, description, status, due_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING ` + taskColumns,
	// $6 true means "set assignee_id to $7", which may be NULL.
	"updateTask": `UPDATE tasks SET
		title = COALESCE($3, title), description = COALESCE($4, description), status = COALESCE($5, status),
		assignee_id = CASE WHEN $6 THEN $7::uuid ELSE assignee_id END,
		due_at = COALESCE($8, due_at), completed_at = COALESCE($9, completed_at), updated_at = now()
		WHERE company_id = $1 AND id = $2 RETURNING ` + taskColumns,
	"deleteTask": `DELETE FROM tasks WHERE company_id = $1 AND id = $2`,

	"createInvite": `INSERT INTO invites (company_id, id, interview_id, invited_by, email, role, token_hash, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING created_at`,

	"listScores": `SELECT ` + scoreColumns + ` FROM interview_scores
		WHERE company_id = $1 AND interview_id = $2 ORDER BY dimension`,
	// Approval copies the proposal, rejection clears the final value, and an
	// adjustment takes the human's value.
	"decideScore": `UPDATE interview_scores SET status = $4,
		final_value = CASE $4 WHEN 'human_approved' THEN proposed_value WHEN 'human_adjusted' THEN $5 ELSE NULL END,
		decision_note = $6, decided_by = $7, decided_at = $8, updated_at = now()
		WHERE company_id = $1 AND interview_id = $2 AND id = $3 AND $4::text LIKE 'human\_%'
		RETURNING ` + scoreColumns,

	// Whether policy $3 lets the company use interview $2. A missing policy
	// row means the default (enabled); a purged interview has nothing left
	// to watch or replay.
	"interviewPolicy": `SELECT COALESCE((SELECT enabled FROM company_policies WHERE company_id = $1 AND key = $3), true)
		FROM interviews WHERE company_id = $1 AND id = $2 AND purged_at IS NULL`,

	"listPolicies": `SELECT key, enabled, updated_by, updated_at FROM company_policies WHERE company_id = $1 ORDER BY key`,
	"setPolicy": `INSERT INTO company_policies (company_id, key, enabled, updated_by) VALUES ($1, $2, $3, $4)
		ON CONFLICT (company_id, key) DO UPDATE SET enabled = EXCLUDED.enabled, updated_by = EXCLUDED.updated_by, updated_at = now()
		WHERE company_policies.company_id = $1
		RETURNING key, enabled, updated_by, updated_at`,
}

// UserRole implements rbac.RoleFinder.
func (s *AdminStore) UserRole(ctx context.Context, companyID, userID uuid.UUID) (rbac.Role, error) {
	var role string
	err := s.pool.QueryRow(ctx, adminQueries["userRole"], companyID, userID).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", rbac.ErrUserNotFound
	}
	if err != nil {
		return "", fmt.Errorf("read user role: %w", err)
	}
	return rbac.Role(role), nil
}

func (s *AdminStore) GetCompany(ctx context.Context, companyID uuid.UUID) (domain.Company, error) {
	return scanCompany(s.pool.QueryRow(ctx, adminQueries["getCompany"], companyID))
}

func (s *AdminStore) UpdateCompany(ctx context.Context, companyID uuid.UUID, patch adminapi.CompanyPatch) (domain.Company, error) {
	return scanCompany(s.pool.QueryRow(ctx, adminQueries["updateCompany"], companyID, patch.Name))
}

func scanCompany(row pgx.Row) (domain.Company, error) {
	var c domain.Company
	if err := row.Scan(&c.ID, &c.Name, &c.Slug, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return domain.Company{}, adminError("company", err)
	}
	return c, nil
}

func (s *AdminStore) ListUsers(ctx context.Context, companyID uuid.UUID) ([]domain.User, error) {
	rows, err := s.pool.Query(ctx, adminQueries["listUsers"], companyID)
	return collectRows(rows, err, scanUser)
}

func (s *AdminStore) GetUser(ctx context.Context, companyID, userID uuid.UUID) (domain.User, error) {
	return one(scanUser(s.pool.QueryRow(ctx, adminQueries["getUser"], companyID, userID)))("user")
}

func (s *AdminStore) CreateUser(ctx context.Context, u domain.User) (domain.User, error) {
	return one(scanUser(s.pool.QueryRow(ctx, adminQueries["createUser"], u.CompanyID, u.ID, u.Email, u.DisplayName, u.Role)))("user")
}

func (s *AdminStore) UpdateUser(ctx context.Context, companyID, userID uuid.UUID, patch adminapi.UserPatch) (domain.User, error) {
	var role *string
	if patch.Role != nil {
		r := string(*patch.Role)
		role = &r
	}
	return one(scanUser(s.pool.QueryRow(ctx, adminQueries["updateUser"], companyID, userID, patch.DisplayName, role)))("user")
}

func (s *AdminStore) DeleteUser(ctx context.Context, companyID, userID uuid.UUID) error {
	return exec(s.pool.Exec(ctx, adminQueries["deleteUser"], companyID, userID))
}

func scanUser(row pgx.Row) (domain.User, error) {
	var u domain.User
	err := row.Scan(&u.ID, &u.CompanyID, &u.Email, &u.DisplayName, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	return u, err
}

func (s *AdminStore) ListInterviews(ctx context.Context, companyID uuid.UUID, filter adminapi.InterviewFilter) ([]domain.Interview, error) {
	rows, err := s.pool.Query(ctx, adminQueries["listInterviews"], companyID, filter.Status, filter.Limit)
	return collectRows(rows, err, scanInterview)
}

func (s *AdminStore) GetInterview(ctx context.Context, companyID, interviewID uuid.UUID) (domain.Interview, error) {
	return one(scanInterview(s.pool.QueryRow(ctx, adminQueries["getInterview"], companyID, interviewID)))("interview")
}

func (s *AdminStore) CreateInterview(ctx context.Context, i domain.Interview) (domain.Interview, error) {
	return one(scanInterview(s.pool.QueryRow(ctx, adminQueries["createInterview"], i.CompanyID, i.ID, i.CreatedBy,
		i.CandidateName, i.CandidateEmail, i.Status, i.ScheduledAt)))("interview")
}

func (s *AdminStore) UpdateInterview(ctx context.Context, companyID, interviewID uuid.UUID, p adminapi.InterviewPatch) (domain.Interview, error) {
	return one(scanInterview(s.pool.QueryRow(ctx, adminQueries["updateInterview"], companyID, interviewID,
		p.CandidateName, p.CandidateEmail, p.Status, p.ScheduledAt, p.TerminalAt)))("interview")
}

func (s *AdminStore) RequestErasure(ctx context.Context, companyID, interviewID uuid.UUID, at time.Time) (domain.Interview, error) {
	return one(scanInterview(s.pool.QueryRow(ctx, adminQueries["requestErasure"], companyID, interviewID, at)))("interview")
}

func scanInterview(row pgx.Row) (domain.Interview, error) {
	var i domain.Interview
	err := row.Scan(&i.ID, &i.CompanyID, &i.CreatedBy, &i.CandidateName, &i.CandidateEmail, &i.Status,
		&i.ScheduledAt, &i.TerminalAt, &i.EraseRequestedAt, &i.LegalHold, &i.PurgedAt,
		&i.LastSequenceNumber, &i.CreatedAt, &i.UpdatedAt)
	return i, err
}

func (s *AdminStore) ListTasks(ctx context.Context, companyID uuid.UUID, f adminapi.TaskFilter) ([]domain.Task, error) {
	rows, err := s.pool.Query(ctx, adminQueries["listTasks"], companyID, f.InterviewID, f.AssigneeID, f.Status, f.Limit)
	return collectRows(rows, err, scanTask)
}

func (s *AdminStore) GetTask(ctx context.Context, companyID, taskID uuid.UUID) (domain.Task, error) {
	return one(scanTask(s.pool.QueryRow(ctx, adminQueries["getTask"], companyID, taskID)))("task")
}

func (s *AdminStore) CreateTask(ctx context.Context, t domain.Task) (domain.Task, error) {
	return one(scanTask(s.pool.QueryRow(ctx, adminQueries["createTask"], t.CompanyID, t.ID, t.InterviewID,
		t.AssigneeID, t.Title, t.Description, t.Status, t.DueAt)))("task")
}

func (s *AdminStore) UpdateTask(ctx context.Context, companyID, taskID uuid.UUID, p adminapi.TaskPatch) (domain.Task, error) {
	var assignee *uuid.UUID
	if p.AssigneeID != nil && *p.AssigneeID != uuid.Nil {
		assignee = p.AssigneeID
	}
	return one(scanTask(s.pool.QueryRow(ctx, adminQueries["updateTask"], companyID, taskID, p.Title, p.Description,
		p.Status, p.AssigneeID != nil, assignee, p.DueAt, p.CompletedAt)))("task")
}

func (s *AdminStore) DeleteTask(ctx context.Context, companyID, taskID uuid.UUID) error {
	return exec(s.pool.Exec(ctx, adminQueries["deleteTask"], companyID, taskID))
}

func scanTask(row pgx.Row) (domain.Task, error) {
	var t domain.Task
	err := row.Scan(&t.ID, &t.CompanyID, &t.InterviewID, &t.AssigneeID, &t.Title, &t.Description, &t.Status,
		&t.DueAt, &t.CompletedAt, &t.CreatedAt, &t.UpdatedAt)
	return t, err
}

func (s *AdminStore) CreateInvite(ctx context.Context, inv domain.Invite) (domain.Invite, error) {
	err := s.pool.QueryRow(ctx, adminQueries["createInvite"], inv.CompanyID, inv.ID, inv.InterviewID, inv.InvitedBy,
		inv.Email, inv.Role, inv.TokenHash, inv.ExpiresAt).Scan(&inv.CreatedAt)
	if err != nil {
		return domain.Invite{}, adminError("invite", err)
	}
	return inv, nil
}

func (s *AdminStore) ListScores(ctx context.Context, companyID, interviewID uuid.UUID) ([]domain.Score, error) {
	rows, err := s.pool.Query(ctx, adminQueries["listScores"], companyID, interviewID)
	return collectRows(rows, err, scanScore)
}

func (s *AdminStore) DecideScore(ctx context.Context, companyID, interviewID, scoreID uuid.UUID, d adminapi.ScoreDecision) (domain.Score, error) {
	if err := adminapi.ValidateScoreDecision(d.Status, d.FinalValue); err != nil {
		return domain.Score{}, err
	}
	return one(scanScore(s.pool.QueryRow(ctx, adminQueries["decideScore"], companyID, interviewID, scoreID,
		d.Status, d.FinalValue, d.Note, d.DecidedBy, d.DecidedAt)))("score")
}

func scanScore(row pgx.Row) (domain.Score, error) {
	var s domain.Score
	err := row.Scan(&s.ID, &s.CompanyID, &s.InterviewID, &s.Dimension, &s.ProposedValue, &s.Rationale, &s.Status,
		&s.FinalValue, &s.DecisionNote, &s.DecidedBy, &s.DecidedAt, &s.CreatedAt, &s.UpdatedAt)
	return s, err
}

func (s *AdminStore) ListPolicies(ctx context.Context, companyID uuid.UUID) ([]domain.Policy, error) {
	rows, err := s.pool.Query(ctx, adminQueries["listPolicies"], companyID)
	return collectRows(rows, err, scanPolicy)
}

func (s *AdminStore) SetPolicy(ctx context.Context, companyID uuid.UUID, key string, enabled bool, by uuid.UUID) (domain.Policy, error) {
	return one(scanPolicy(s.pool.QueryRow(ctx, adminQueries["setPolicy"], companyID, key, enabled, by)))("policy")
}

// LiveMonitoring implements livemonitor.Access.
func (s *AdminStore) LiveMonitoring(ctx context.Context, companyID, interviewID uuid.UUID) error {
	enabled, err := s.interviewPolicy(ctx, companyID, interviewID, "live_monitoring")
	if errors.Is(err, adminapi.ErrNotFound) {
		return livemonitor.ErrInterviewNotFound
	}
	if err == nil && !enabled {
		return livemonitor.ErrMonitoringDisabled
	}
	return err
}

// ReplayAccess implements dashboard.ReplayAccess.
func (s *AdminStore) ReplayAccess(ctx context.Context, companyID, interviewID uuid.UUID) error {
	enabled, err := s.interviewPolicy(ctx, companyID, interviewID, "replay")
	if err == nil && !enabled {
		return dashboard.ErrReplayDisabled
	}
	return err
}

func (s *AdminStore) interviewPolicy(ctx context.Context, companyID, interviewID uuid.UUID, key string) (bool, error) {
	var enabled bool
	err := s.pool.QueryRow(ctx, adminQueries["interviewPolicy"], companyID, interviewID, key).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, adminapi.ErrNotFound
	}
	if err != nil {
		return false, fmt.Errorf("check %s policy: %w", key, err)
	}
	return enabled, nil
}

func scanPolicy(row pgx.Row) (domain.Policy, error) {
	var p domain.Policy
	var updatedAt time.Time
	err := row.Scan(&p.Key, &p.Enabled, &p.UpdatedBy, &updatedAt)
	p.UpdatedAt = &updatedAt
	return p, err
}

// one maps a single-row scan's error for kind.
func one[T any](value T, err error) func(kind string) (T, error) {
	return func(kind string) (T, error) {
		if err != nil {
			var zero T
			return zero, adminError(kind, err)
		}
		return value, nil
	}
}

// collectRows scans every row of a query.
func collectRows[T any](rows pgx.Rows, err error, scan func(pgx.Row) (T, error)) ([]T, error) {
	if err != nil {
		return nil, adminError("query", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (T, error) { return scan(row) })
	if err != nil {
		return nil, adminError("scan", err)
	}
	return out, nil
}

// exec maps a delete's result: no affected row means not found.
func exec(tag pgconn.CommandTag, err error) error {
	if err != nil {
		return adminError("delete", err)
	}
	if tag.RowsAffected() == 0 {
		return adminapi.ErrNotFound
	}
	return nil
}

// adminError maps Postgres errors to the Admin API's sentinel errors.
func adminError(kind string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", kind, adminapi.ErrNotFound)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return fmt.Errorf("%s: %w", kind, adminapi.ErrConflict)
		case "23503": // foreign_key_violation
			return fmt.Errorf("%s: %w", kind, adminapi.ErrInvalidReference)
		case "23514": // check_violation
			return fmt.Errorf("%s: %w: %s", kind, adminapi.ErrInvalid, pgErr.ConstraintName)
		}
	}
	return fmt.Errorf("%s: %w", kind, err)
}
