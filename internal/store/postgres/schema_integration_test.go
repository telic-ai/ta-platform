//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/postgres/migrations"
)

func TestMigrationsAndTenantIsolation(t *testing.T) {
	cfg, err := config.Load("postgres-schema-integration-test")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	admin, err := New(ctx, cfg.PostgresDSN)
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}
	defer admin.Close()

	schema := "test_schema_" + uuid.NewString()[:8]
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Pool().Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := admin.Pool().Exec(cleanupCtx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	}()

	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		t.Fatalf("parse Postgres DSN: %v", err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatalf("connect schema pool: %v", err)
	}
	defer pool.Close()

	runner := migrations.New(pool)
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("second migrate up must be idempotent: %v", err)
	}

	assertTenantIndexes(t, ctx, pool, schema)
	assertInterviewRetentionColumns(t, ctx, pool, schema)
	assertSessionStateColumn(t, ctx, pool, schema)
	assertColumns(t, ctx, pool, schema, map[string]string{
		"invites": "interview_id", "sessions": "interview_id", "interviews": "last_sequence_number",
		"event_outbox": "envelope",
	})
	assertColumns(t, ctx, pool, schema, map[string]string{"sessions": "last_diff_sequence"})
	assertColumns(t, ctx, pool, schema, map[string]string{"scores": "metrics"})
	assertCrossTenantQueryReturnsNothing(t, ctx, pool)
	assertSessionHasExactlyOnePrincipal(t, ctx, pool)

	// Roll back every migration, newest first.
	const migrationCount = 8
	for version := migrationCount; version > 0; version-- {
		if err := runner.Down(ctx); err != nil {
			t.Fatalf("migrate down from version %d: %v", version, err)
		}
	}
	for _, table := range []string{"companies", "users", "tasks", "interviews", "invites", "sessions", "event_outbox", "scores"} {
		var exists bool
		qualified := fmt.Sprintf("%s.%s", schema, table)
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, qualified).Scan(&exists); err != nil {
			t.Fatalf("check table %s after down: %v", table, err)
		}
		if exists {
			t.Errorf("table %s still exists after down", table)
		}
	}
	if err := runner.Down(ctx); err != nil {
		t.Fatalf("down at version zero must be a no-op: %v", err)
	}
}

func assertSessionStateColumn(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string) {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			 WHERE table_schema = $1 AND table_name = 'sessions' AND column_name = 'state'
		)`, schema).Scan(&exists); err != nil {
		t.Fatalf("query sessions.state: %v", err)
	}
	if !exists {
		t.Error("sessions.state is missing")
	}
}

func assertColumns(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string, columns map[string]string) {
	t.Helper()
	for table, column := range columns {
		var exists bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				 WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
			)`, schema, table, column).Scan(&exists); err != nil {
			t.Fatalf("query %s.%s: %v", table, column, err)
		}
		if !exists {
			t.Errorf("%s.%s is missing", table, column)
		}
	}
}

func assertInterviewRetentionColumns(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string) {
	t.Helper()
	rows, err := pool.Query(ctx, `
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = $1
           AND table_name = 'interviews'
           AND column_name = ANY($2)`, schema,
		[]string{"terminal_at", "erase_requested_at", "legal_hold", "purged_at"})
	if err != nil {
		t.Fatalf("query interview retention columns: %v", err)
	}
	defer rows.Close()
	found := map[string]bool{}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scan retention column: %v", err)
		}
		found[column] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate retention columns: %v", err)
	}
	for _, column := range []string{"terminal_at", "erase_requested_at", "legal_hold", "purged_at"} {
		if !found[column] {
			t.Errorf("interviews.%s is missing", column)
		}
	}
}

func assertTenantIndexes(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string) {
	t.Helper()
	rows, err := pool.Query(ctx, `
        SELECT DISTINCT table_rel.relname
          FROM pg_index idx
          JOIN pg_class table_rel ON table_rel.oid = idx.indrelid
          JOIN pg_namespace ns ON ns.oid = table_rel.relnamespace
          JOIN pg_attribute first_col
            ON first_col.attrelid = table_rel.oid
           AND first_col.attnum = idx.indkey[0]
         WHERE ns.nspname = $1
           AND table_rel.relname = ANY($2)
           AND idx.indnkeyatts >= 2
           AND first_col.attname = 'company_id'`,
		schema, []string{"users", "tasks", "interviews", "invites", "sessions"})
	if err != nil {
		t.Fatalf("query tenant indexes: %v", err)
	}
	defer rows.Close()
	found := map[string]bool{}
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatalf("scan indexed table: %v", err)
		}
		found[table] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tenant indexes: %v", err)
	}
	for _, table := range []string{"users", "tasks", "interviews", "invites", "sessions"} {
		if !found[table] {
			t.Errorf("%s has no composite index leading on company_id", table)
		}
	}
}

func assertCrossTenantQueryReturnsNothing(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	companyA := uuid.New()
	companyB := uuid.New()
	if _, err := pool.Exec(ctx, `
        INSERT INTO companies (id, name, slug)
        VALUES ($1, 'Tenant A', $2), ($3, 'Tenant B', $4)`,
		companyA, "tenant-a-"+companyA.String(), companyB, "tenant-b-"+companyB.String()); err != nil {
		t.Fatalf("seed companies: %v", err)
	}

	interviewID := uuid.New()
	store := NewInterviewStore(pool)
	if err := store.Create(ctx, domain.Interview{
		ID:             interviewID,
		CompanyID:      companyA,
		CandidateName:  "Candidate",
		CandidateEmail: "candidate@example.test",
		Status:         "scheduled",
	}); err != nil {
		t.Fatalf("create interview: %v", err)
	}
	if _, err := store.Get(ctx, companyA, interviewID); err != nil {
		t.Fatalf("own-tenant query: %v", err)
	}
	if _, err := store.Get(ctx, companyB, interviewID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("cross-tenant query error = %v, want pgx.ErrNoRows", err)
	}
}

// A session belongs to a company member or to a candidate's interview, never
// both and never neither.
func assertSessionHasExactlyOnePrincipal(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	companyID, userID, interviewID := uuid.New(), uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO companies (id, name, slug) VALUES ($1, 'Principal', $2)`,
		companyID, "principal-"+companyID.String()); err != nil {
		t.Fatalf("seed company: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO users (company_id, id, email, role) VALUES ($1, $2, 'member@example.test', 'recruiter')`,
		companyID, userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if err := NewInterviewStore(pool).Create(ctx, domain.Interview{
		ID: interviewID, CompanyID: companyID, CandidateName: "Candidate",
		CandidateEmail: "candidate@example.test", Status: "scheduled",
	}); err != nil {
		t.Fatalf("seed interview: %v", err)
	}
	insert := func(user, interview *uuid.UUID) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO sessions (company_id, id, user_id, interview_id, token_hash, expires_at)
			VALUES ($1, $2, $3, $4, $5, now() + interval '1 hour')`,
			companyID, uuid.New(), user, interview, []byte(uuid.NewString()))
		return err
	}
	if err := insert(&userID, nil); err != nil {
		t.Errorf("member session rejected: %v", err)
	}
	if err := insert(nil, &interviewID); err != nil {
		t.Errorf("candidate session rejected: %v", err)
	}
	if err := insert(&userID, &interviewID); err == nil {
		t.Error("session with both user_id and interview_id was accepted")
	}
	if err := insert(nil, nil); err == nil {
		t.Error("session with neither user_id nor interview_id was accepted")
	}
}
