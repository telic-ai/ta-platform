//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/adminapi"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/livemonitor"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
)

type adminFixture struct {
	pool       *pgxpool.Pool
	store      *postgres.AdminStore
	companyA   uuid.UUID
	companyB   uuid.UUID
	ownerA     uuid.UUID
	ownerB     uuid.UUID
	interviewA uuid.UUID
	interviewB uuid.UUID
}

func newAdminFixture(t *testing.T, ctx context.Context) adminFixture {
	t.Helper()
	cfg, _ := config.Load("admin-integration-test")
	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)
	f := adminFixture{pool: pool, store: postgres.NewAdminStore(pool), companyA: uuid.New(), companyB: uuid.New(),
		ownerA: uuid.New(), ownerB: uuid.New(), interviewA: uuid.New(), interviewB: uuid.New()}
	for _, c := range []struct{ company, owner, interview uuid.UUID }{
		{f.companyA, f.ownerA, f.interviewA}, {f.companyB, f.ownerB, f.interviewB},
	} {
		mustExec(t, ctx, pool, `INSERT INTO companies (id, name, slug) VALUES ($1, 'Co', $2)`, c.company, c.company.String())
		mustExec(t, ctx, pool, `INSERT INTO users (company_id, id, email, role) VALUES ($1, $2, 'owner@x.test', 'owner')`, c.company, c.owner)
		mustExec(t, ctx, pool, `INSERT INTO interviews (company_id, id, created_by, candidate_name, candidate_email, status)
			VALUES ($1, $2, $3, 'Cand', 'cand@x.test', 'in_progress')`, c.company, c.interview, c.owner)
	}
	return f
}

func mustExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func count(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAdminStoreCompanyAndUsers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newAdminFixture(t, ctx)
	s := f.store

	if role, err := s.UserRole(ctx, f.companyA, f.ownerA); err != nil || role != rbac.RoleOwner {
		t.Fatalf("UserRole = %q, %v", role, err)
	}
	if _, err := s.UserRole(ctx, f.companyB, f.ownerA); !errors.Is(err, rbac.ErrUserNotFound) {
		t.Fatalf("cross-tenant UserRole err = %v", err)
	}

	name := "Acme"
	company, err := s.UpdateCompany(ctx, f.companyA, adminapi.CompanyPatch{Name: &name})
	if err != nil || company.Name != "Acme" {
		t.Fatalf("UpdateCompany = %+v, %v", company, err)
	}
	if other, _ := s.GetCompany(ctx, f.companyB); other.Name != "Co" {
		t.Fatal("UpdateCompany touched another company")
	}
	if _, err := s.GetCompany(ctx, uuid.New()); !errors.Is(err, adminapi.ErrNotFound) {
		t.Fatalf("GetCompany unknown err = %v", err)
	}

	user, err := s.CreateUser(ctx, domain.User{ID: uuid.New(), CompanyID: f.companyA, Email: "i@x.test", Role: "interviewer"})
	if err != nil || user.CreatedAt.IsZero() {
		t.Fatalf("CreateUser = %+v, %v", user, err)
	}
	if _, err := s.CreateUser(ctx, domain.User{ID: uuid.New(), CompanyID: f.companyA, Email: "i@x.test", Role: "viewer"}); !errors.Is(err, adminapi.ErrConflict) {
		t.Fatalf("duplicate email err = %v", err)
	}
	// Same email in another company is fine.
	if _, err := s.CreateUser(ctx, domain.User{ID: uuid.New(), CompanyID: f.companyB, Email: "i@x.test", Role: "viewer"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateUser(ctx, domain.User{ID: uuid.New(), CompanyID: f.companyA, Email: "r@x.test", Role: "root"}); !errors.Is(err, adminapi.ErrInvalid) {
		t.Fatalf("unknown role err = %v", err)
	}

	users, err := s.ListUsers(ctx, f.companyA)
	if err != nil || len(users) != 2 {
		t.Fatalf("ListUsers = %d, %v", len(users), err)
	}
	role := rbac.RoleRecruiter
	display := "Ivy"
	updated, err := s.UpdateUser(ctx, f.companyA, user.ID, adminapi.UserPatch{Role: &role, DisplayName: &display})
	if err != nil || updated.Role != "recruiter" || updated.DisplayName != "Ivy" {
		t.Fatalf("UpdateUser = %+v, %v", updated, err)
	}
	if _, err := s.UpdateUser(ctx, f.companyB, user.ID, adminapi.UserPatch{Role: &role}); !errors.Is(err, adminapi.ErrNotFound) {
		t.Fatalf("cross-tenant UpdateUser err = %v", err)
	}
	if err := s.DeleteUser(ctx, f.companyB, user.ID); !errors.Is(err, adminapi.ErrNotFound) {
		t.Fatalf("cross-tenant DeleteUser err = %v", err)
	}
	if err := s.DeleteUser(ctx, f.companyA, user.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetUser(ctx, f.companyA, user.ID); !errors.Is(err, adminapi.ErrNotFound) {
		t.Fatalf("deleted user err = %v", err)
	}
}

func TestAdminStoreInterviewsAndErasure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newAdminFixture(t, ctx)
	s := f.store

	created, err := s.CreateInterview(ctx, domain.Interview{ID: uuid.New(), CompanyID: f.companyA, CreatedBy: &f.ownerA,
		CandidateName: "New", CandidateEmail: "n@x.test", Status: "scheduled"})
	if err != nil || created.Status != "scheduled" {
		t.Fatalf("CreateInterview = %+v, %v", created, err)
	}
	list, err := s.ListInterviews(ctx, f.companyA, adminapi.InterviewFilter{Limit: 10})
	if err != nil || len(list) != 2 {
		t.Fatalf("ListInterviews = %d, %v", len(list), err)
	}
	list, _ = s.ListInterviews(ctx, f.companyA, adminapi.InterviewFilter{Status: "scheduled", Limit: 10})
	if len(list) != 1 || list[0].ID != created.ID {
		t.Fatalf("filtered = %+v", list)
	}
	if list, _ = s.ListInterviews(ctx, f.companyA, adminapi.InterviewFilter{Limit: 1}); len(list) != 1 {
		t.Fatalf("limit ignored: %d", len(list))
	}
	status := "completed"
	first := time.Now().UTC().Truncate(time.Microsecond)
	updated, err := s.UpdateInterview(ctx, f.companyA, created.ID, adminapi.InterviewPatch{Status: &status, TerminalAt: &first})
	if err != nil || updated.Status != "completed" || !updated.TerminalAt.Equal(first) {
		t.Fatalf("UpdateInterview = %+v, %v", updated, err)
	}
	later := first.Add(time.Hour)
	updated, _ = s.UpdateInterview(ctx, f.companyA, created.ID, adminapi.InterviewPatch{Status: &status, TerminalAt: &later})
	if !updated.TerminalAt.Equal(first) {
		t.Fatal("terminal_at moved")
	}
	if _, err := s.GetInterview(ctx, f.companyB, created.ID); !errors.Is(err, adminapi.ErrNotFound) {
		t.Fatalf("cross-tenant GetInterview err = %v", err)
	}

	// Attach everything erasure could plausibly delete.
	scoreID := uuid.New()
	mustExec(t, ctx, f.pool, `INSERT INTO tasks (company_id, id, interview_id, title, status) VALUES ($1, $2, $3, 't', 'open')`, f.companyA, uuid.New(), f.interviewA)
	mustExec(t, ctx, f.pool, `INSERT INTO interview_scores (company_id, id, interview_id, dimension, proposed_value) VALUES ($1, $2, $3, 'd', 1)`, f.companyA, scoreID, f.interviewA)
	mustExec(t, ctx, f.pool, `INSERT INTO invites (company_id, id, interview_id, email, role, token_hash, expires_at) VALUES ($1, $2, $3, 'c@x.test', 'candidate', $4, now() + interval '1 day')`,
		f.companyA, uuid.New(), f.interviewA, auth.HashToken(uuid.NewString()))
	tables := []string{"interviews", "tasks", "interview_scores", "invites", "users"}
	before := map[string]int{}
	for _, table := range tables {
		before[table] = count(t, ctx, f.pool, `SELECT count(*) FROM `+table)
	}

	if _, err := s.RequestErasure(ctx, f.companyB, f.interviewA, first); !errors.Is(err, adminapi.ErrNotFound) {
		t.Fatalf("cross-tenant erasure err = %v", err)
	}
	erased, err := s.RequestErasure(ctx, f.companyA, f.interviewA, first)
	if err != nil || erased.EraseRequestedAt == nil || !erased.EraseRequestedAt.Equal(first) || erased.PurgedAt != nil {
		t.Fatalf("RequestErasure = %+v, %v", erased, err)
	}
	again, _ := s.RequestErasure(ctx, f.companyA, f.interviewA, later)
	if !again.EraseRequestedAt.Equal(first) {
		t.Fatal("repeat erasure moved erase_requested_at")
	}
	for _, table := range tables {
		if after := count(t, ctx, f.pool, `SELECT count(*) FROM `+table); after != before[table] {
			t.Errorf("%s rows %d -> %d after erasure", table, before[table], after)
		}
	}
	if other, _ := s.GetInterview(ctx, f.companyB, f.interviewB); other.EraseRequestedAt != nil {
		t.Fatal("erasure marked another company's interview")
	}
}

func TestAdminStoreTasks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newAdminFixture(t, ctx)
	s := f.store

	task, err := s.CreateTask(ctx, domain.Task{ID: uuid.New(), CompanyID: f.companyA, InterviewID: &f.interviewA,
		AssigneeID: &f.ownerA, Title: "Review", Status: "open"})
	if err != nil || task.AssigneeID == nil {
		t.Fatalf("CreateTask = %+v, %v", task, err)
	}
	// Composite foreign keys keep references inside the company.
	if _, err := s.CreateTask(ctx, domain.Task{ID: uuid.New(), CompanyID: f.companyA, InterviewID: &f.interviewB, Title: "x", Status: "open"}); !errors.Is(err, adminapi.ErrInvalidReference) {
		t.Fatalf("cross-tenant interview err = %v", err)
	}
	if _, err := s.CreateTask(ctx, domain.Task{ID: uuid.New(), CompanyID: f.companyA, AssigneeID: &f.ownerB, Title: "x", Status: "open"}); !errors.Is(err, adminapi.ErrInvalidReference) {
		t.Fatalf("cross-tenant assignee err = %v", err)
	}

	list, err := s.ListTasks(ctx, f.companyA, adminapi.TaskFilter{InterviewID: &f.interviewA, AssigneeID: &f.ownerA, Status: "open", Limit: 10})
	if err != nil || len(list) != 1 {
		t.Fatalf("ListTasks = %d, %v", len(list), err)
	}
	if list, _ := s.ListTasks(ctx, f.companyB, adminapi.TaskFilter{Limit: 10}); len(list) != 0 {
		t.Fatal("company B sees company A's tasks")
	}

	title := "Renamed"
	updated, err := s.UpdateTask(ctx, f.companyA, task.ID, adminapi.TaskPatch{Title: &title})
	if err != nil || updated.Title != "Renamed" || updated.AssigneeID == nil {
		t.Fatalf("partial UpdateTask = %+v, %v", updated, err)
	}
	clear := uuid.Nil
	done := "done"
	completed := time.Now().UTC().Truncate(time.Microsecond)
	updated, err = s.UpdateTask(ctx, f.companyA, task.ID, adminapi.TaskPatch{AssigneeID: &clear, Status: &done, CompletedAt: &completed})
	if err != nil || updated.AssigneeID != nil || updated.Status != "done" || !updated.CompletedAt.Equal(completed) {
		t.Fatalf("clearing UpdateTask = %+v, %v", updated, err)
	}
	if _, err := s.UpdateTask(ctx, f.companyA, task.ID, adminapi.TaskPatch{AssigneeID: &f.ownerB}); !errors.Is(err, adminapi.ErrInvalidReference) {
		t.Fatalf("cross-tenant reassignment err = %v", err)
	}
	if _, err := s.GetTask(ctx, f.companyB, task.ID); !errors.Is(err, adminapi.ErrNotFound) {
		t.Fatalf("cross-tenant GetTask err = %v", err)
	}
	if err := s.DeleteTask(ctx, f.companyB, task.ID); !errors.Is(err, adminapi.ErrNotFound) {
		t.Fatalf("cross-tenant DeleteTask err = %v", err)
	}
	if err := s.DeleteTask(ctx, f.companyA, task.ID); err != nil {
		t.Fatal(err)
	}
}

func TestAdminStoreInvites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newAdminFixture(t, ctx)
	s := f.store
	hash := auth.HashToken("raw-token")
	invite, err := s.CreateInvite(ctx, domain.Invite{ID: uuid.New(), CompanyID: f.companyA, InterviewID: &f.interviewA,
		InvitedBy: &f.ownerA, Email: "c@x.test", Role: "candidate", TokenHash: hash, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil || invite.CreatedAt.IsZero() {
		t.Fatalf("CreateInvite = %+v, %v", invite, err)
	}
	// The stored invite is exchangeable by the Candidate Workspace.
	if n := count(t, ctx, f.pool, `SELECT count(*) FROM invites WHERE token_hash = $1 AND company_id = $2 AND interview_id = $3`, hash, f.companyA, f.interviewA); n != 1 {
		t.Fatalf("stored invites = %d", n)
	}
	if _, err := s.CreateInvite(ctx, domain.Invite{ID: uuid.New(), CompanyID: f.companyA, InterviewID: &f.interviewB,
		Email: "c@x.test", Role: "candidate", TokenHash: auth.HashToken("other"), ExpiresAt: time.Now().Add(time.Hour)}); !errors.Is(err, adminapi.ErrInvalidReference) {
		t.Fatalf("cross-tenant interview invite err = %v", err)
	}
	if _, err := s.CreateInvite(ctx, domain.Invite{ID: uuid.New(), CompanyID: f.companyA, Email: "m@x.test", Role: "viewer",
		TokenHash: hash, ExpiresAt: time.Now().Add(time.Hour)}); !errors.Is(err, adminapi.ErrConflict) {
		t.Fatalf("duplicate token err = %v", err)
	}
}

func TestAdminStoreScoresAndPolicies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newAdminFixture(t, ctx)
	s := f.store
	scoreID := uuid.New()
	mustExec(t, ctx, f.pool, `INSERT INTO interview_scores (company_id, id, interview_id, dimension, proposed_value, rationale)
		VALUES ($1, $2, $3, 'correctness', 3.5, 'tests pass')`, f.companyA, scoreID, f.interviewA)

	scores, err := s.ListScores(ctx, f.companyA, f.interviewA)
	if err != nil || len(scores) != 1 || scores[0].Status != "proposed" || scores[0].FinalValue != nil {
		t.Fatalf("ListScores = %+v, %v", scores, err)
	}
	if scores, _ := s.ListScores(ctx, f.companyB, f.interviewA); len(scores) != 0 {
		t.Fatal("company B sees company A's scores")
	}

	at := time.Now().UTC().Truncate(time.Microsecond)
	decide := func(company uuid.UUID, status string, value *float64) (domain.Score, error) {
		return s.DecideScore(ctx, company, f.interviewA, scoreID, adminapi.ScoreDecision{Status: status, FinalValue: value, Note: "n", DecidedBy: f.ownerA, DecidedAt: at})
	}
	for _, status := range []string{"proposed", "ai_approved"} {
		if _, err := decide(f.companyA, status, nil); !errors.Is(err, adminapi.ErrInvalid) {
			t.Fatalf("%s err = %v", status, err)
		}
	}
	if _, err := decide(f.companyB, "human_approved", nil); !errors.Is(err, adminapi.ErrNotFound) {
		t.Fatalf("cross-tenant decision err = %v", err)
	}
	approved, err := decide(f.companyA, "human_approved", nil)
	if err != nil || approved.Status != "human_approved" || *approved.FinalValue != 3.5 || *approved.DecidedBy != f.ownerA || !approved.DecidedAt.Equal(at) {
		t.Fatalf("approved = %+v, %v", approved, err)
	}
	v := 2.0
	if adjusted, err := decide(f.companyA, "human_adjusted", &v); err != nil || *adjusted.FinalValue != 2.0 {
		t.Fatalf("adjusted = %+v, %v", adjusted, err)
	}
	if rejected, err := decide(f.companyA, "human_rejected", nil); err != nil || rejected.FinalValue != nil {
		t.Fatalf("rejected = %+v, %v", rejected, err)
	}
	// The schema itself refuses a decided score without a decision time.
	if _, err := f.pool.Exec(ctx, `UPDATE interview_scores SET decided_at = NULL WHERE id = $1`, scoreID); err == nil {
		t.Fatal("decided score without decided_at accepted")
	}

	if policies, err := s.ListPolicies(ctx, f.companyA); err != nil || len(policies) != 0 {
		t.Fatalf("ListPolicies = %+v, %v", policies, err)
	}
	p, err := s.SetPolicy(ctx, f.companyA, "replay", false, f.ownerA)
	if err != nil || p.Enabled || *p.UpdatedBy != f.ownerA || p.UpdatedAt == nil {
		t.Fatalf("SetPolicy = %+v, %v", p, err)
	}
	if p, _ = s.SetPolicy(ctx, f.companyA, "replay", true, f.ownerA); !p.Enabled {
		t.Fatal("upsert did not update")
	}
	if policies, _ := s.ListPolicies(ctx, f.companyB); len(policies) != 0 {
		t.Fatal("company B sees company A's policies")
	}
	if _, err := s.SetPolicy(ctx, f.companyA, "replay", true, f.ownerB); !errors.Is(err, adminapi.ErrInvalidReference) {
		t.Fatalf("cross-tenant updated_by err = %v", err)
	}
}

func TestAdminStoreLiveMonitoringAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newAdminFixture(t, ctx)
	s := f.store
	if err := s.LiveMonitoring(ctx, f.companyA, f.interviewA); err != nil {
		t.Fatalf("default access = %v", err)
	}
	if err := s.LiveMonitoring(ctx, f.companyB, f.interviewA); !errors.Is(err, livemonitor.ErrInterviewNotFound) {
		t.Fatalf("cross-tenant access = %v", err)
	}
	if _, err := s.SetPolicy(ctx, f.companyA, "live_monitoring", false, f.ownerA); err != nil {
		t.Fatal(err)
	}
	if err := s.LiveMonitoring(ctx, f.companyA, f.interviewA); !errors.Is(err, livemonitor.ErrMonitoringDisabled) {
		t.Fatalf("disabled access = %v", err)
	}
	if err := s.LiveMonitoring(ctx, f.companyB, f.interviewB); err != nil {
		t.Fatalf("company B affected by company A's policy: %v", err)
	}
	mustExec(t, ctx, f.pool, `UPDATE interviews SET terminal_at = now(), purged_at = now() WHERE company_id = $1 AND id = $2`, f.companyB, f.interviewB)
	if err := s.LiveMonitoring(ctx, f.companyB, f.interviewB); !errors.Is(err, livemonitor.ErrInterviewNotFound) {
		t.Fatalf("purged access = %v", err)
	}
}
