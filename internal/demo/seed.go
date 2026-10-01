// Package demo seeds a local company for demos: members with ready-made
// bearer sessions, since member sign-in is not built yet.
package demo

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/adminapi"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/rbac"
)

// Member is a seeded company member and their bearer token.
type Member struct {
	UserID uuid.UUID `json:"user_id"`
	Email  string    `json:"email"`
	Role   rbac.Role `json:"role"`
	Token  string    `json:"token"`
}

// Result is what Seed created.
type Result struct {
	CompanyID uuid.UUID `json:"company_id"`
	Members   []Member  `json:"members"`
}

// Seed creates a company named name with an owner, an interviewer and a
// viewer, each with a session valid for ttl, in one transaction. Tokens
// are returned once; only their hashes are stored.
func Seed(ctx context.Context, pool *pgxpool.Pool, name string, ttl time.Duration) (Result, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("begin seed: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	result := Result{CompanyID: uuid.New()}
	slug := "demo-" + result.CompanyID.String()[:8]
	if _, err := tx.Exec(ctx, `INSERT INTO companies (id, name, slug) VALUES ($1, $2, $3)`, result.CompanyID, name, slug); err != nil {
		return Result{}, fmt.Errorf("seed company: %w", err)
	}
	for _, role := range []rbac.Role{rbac.RoleOwner, rbac.RoleInterviewer, rbac.RoleViewer} {
		token, err := adminapi.NewInviteToken()
		if err != nil {
			return Result{}, err
		}
		m := Member{UserID: uuid.New(), Email: string(role) + "@" + slug + ".test", Role: role, Token: token}
		if _, err := tx.Exec(ctx, `INSERT INTO users (company_id, id, email, display_name, role) VALUES ($1, $2, $3, $4, $5)`,
			result.CompanyID, m.UserID, m.Email, "Demo "+string(role), string(role)); err != nil {
			return Result{}, fmt.Errorf("seed %s: %w", role, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO sessions (company_id, id, user_id, token_hash, expires_at) VALUES ($1, $2, $3, $4, $5)`,
			result.CompanyID, uuid.New(), m.UserID, auth.HashToken(token), time.Now().Add(ttl)); err != nil {
			return Result{}, fmt.Errorf("seed %s session: %w", role, err)
		}
		result.Members = append(result.Members, m)
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("commit seed: %w", err)
	}
	return result, nil
}
