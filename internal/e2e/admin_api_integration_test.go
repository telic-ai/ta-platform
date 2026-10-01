//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/adminapi"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/candidateworkspace"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
)

// seedMember seeds a company member with an Active session and returns
// the member's bearer token.
func seedMember(t *testing.T, ctx context.Context, pool *pgxpool.Pool, companyID uuid.UUID, role rbac.Role) (uuid.UUID, string) {
	t.Helper()
	userID, token := uuid.New(), "member-"+uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO users (company_id, id, email, role) VALUES ($1, $2, $3, $4)`,
		companyID, userID, userID.String()+"@example.test", string(role)); err != nil {
		t.Fatalf("seed member: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO sessions (company_id, id, user_id, token_hash, expires_at)
		VALUES ($1, $2, $3, $4, now() + interval '1 hour')`, companyID, uuid.New(), userID, auth.HashToken(token)); err != nil {
		t.Fatalf("seed member session: %v", err)
	}
	return userID, token
}

// TestAdminAPIEndToEnd runs the Admin API over Postgres with real member
// sessions: role checks, tenant scoping, erasure marking, and a candidate
// invite the Candidate Workspace can exchange.
func TestAdminAPIEndToEnd(t *testing.T) {
	cfg, _ := config.Load("admin-api-e2e-test")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)
	companyA, companyB := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO companies (id, name, slug) VALUES ($1, 'A', $2), ($3, 'B', $4)`,
		companyA, "a-"+companyA.String(), companyB, "b-"+companyB.String()); err != nil {
		t.Fatal(err)
	}
	_, admin := seedMember(t, ctx, pool, companyA, rbac.RoleAdmin)
	_, recruiter := seedMember(t, ctx, pool, companyA, rbac.RoleRecruiter)
	_, viewer := seedMember(t, ctx, pool, companyA, rbac.RoleViewer)
	_, ownerB := seedMember(t, ctx, pool, companyB, rbac.RoleOwner)

	store := postgres.NewAdminStore(pool)
	sessions := postgres.NewSessionStore(pool)
	server := httptest.NewServer(adminapi.NewHandler(store, rbac.Resolver{Sessions: sessions, Roles: store}, adminapi.Config{}).Routes())
	defer server.Close()

	call := func(token, method, path string, body any) (int, map[string]any) {
		t.Helper()
		var reader *bytes.Reader
		if body != nil {
			raw, _ := json.Marshal(body)
			reader = bytes.NewReader(raw)
		} else {
			reader = bytes.NewReader(nil)
		}
		req, _ := http.NewRequestWithContext(ctx, method, server.URL+path, reader)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	// Candidate tokens never reach the Admin API.
	candidate := seedCandidateSession(t, ctx, pool)
	if status, _ := call(candidate.Token, "GET", "/company", nil); status != 401 {
		t.Fatalf("candidate token status = %d", status)
	}

	status, created := call(recruiter, "POST", "/interviews", map[string]any{"candidate_name": "Ada", "candidate_email": "ada@example.test"})
	if status != 201 {
		t.Fatalf("create interview = %d %v", status, created)
	}
	interviewPath := "/interviews/" + created["id"].(string)
	if status, _ := call(viewer, "POST", "/interviews", map[string]any{"candidate_name": "X", "candidate_email": "x@example.test"}); status != 403 {
		t.Fatalf("viewer create interview = %d", status)
	}
	if status, _ := call(ownerB, "GET", interviewPath, nil); status != 404 {
		t.Fatalf("company B read company A's interview: %d", status)
	}

	// A recruiter's candidate invite is exchangeable by the workspace.
	status, invite := call(recruiter, "POST", "/invites", map[string]any{"email": "ada@example.test", "interview_id": created["id"]})
	if status != 201 {
		t.Fatalf("create invite = %d %v", status, invite)
	}
	started, err := candidateworkspace.NewService(sessions, time.Hour).Start(ctx, invite["token"].(string))
	if err != nil {
		t.Fatalf("exchange Admin API invite: %v", err)
	}
	var sessionInterview string
	if err := pool.QueryRow(ctx, `SELECT interview_id::text FROM sessions WHERE id = $1`, started.SessionID).Scan(&sessionInterview); err != nil ||
		sessionInterview != created["id"].(string) {
		t.Fatalf("session interview = %q, %v", sessionInterview, err)
	}

	// Erasure: role-checked, 202, marks only.
	if status, _ := call(recruiter, "POST", interviewPath+"/erase", nil); status != 403 {
		t.Fatalf("recruiter erase = %d", status)
	}
	var before int
	_ = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM interviews) + (SELECT count(*) FROM sessions) + (SELECT count(*) FROM invites) + (SELECT count(*) FROM event_outbox)`).Scan(&before)
	status, erased := call(admin, "POST", interviewPath+"/erase", nil)
	if status != http.StatusAccepted || erased["erase_requested_at"] == nil {
		t.Fatalf("erase = %d %v", status, erased)
	}
	var after int
	var eraseRequested *time.Time
	var purged *time.Time
	_ = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM interviews) + (SELECT count(*) FROM sessions) + (SELECT count(*) FROM invites) + (SELECT count(*) FROM event_outbox)`).Scan(&after)
	_ = pool.QueryRow(ctx, `SELECT erase_requested_at, purged_at FROM interviews WHERE company_id = $1 AND id = $2`, companyA, created["id"]).Scan(&eraseRequested, &purged)
	if after != before || eraseRequested == nil || purged != nil {
		t.Fatalf("erase deleted rows (%d -> %d) or did not mark (%v, %v)", before, after, eraseRequested, purged)
	}
	if status, _ := call(ownerB, "POST", interviewPath+"/erase", nil); status != 404 {
		t.Fatalf("cross-tenant erase = %d", status)
	}
}
