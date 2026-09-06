package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/candidateworkspace"
	"github.com/telic-ai/ta-platform/internal/domain"
)

// SessionStore persists Candidate Workspace invite exchanges and sessions.
type SessionStore struct{ pool *pgxpool.Pool }

func NewSessionStore(pool *pgxpool.Pool) *SessionStore { return &SessionStore{pool: pool} }

func (s *SessionStore) StartSession(ctx context.Context, inviteHash []byte, params candidateworkspace.StartParams) (candidateworkspace.Started, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return candidateworkspace.Started{}, fmt.Errorf("begin invite exchange: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var inviteID, companyID uuid.UUID
	var email, role string
	err = tx.QueryRow(ctx, `
		UPDATE invites
		   SET accepted_at = now()
		 WHERE token_hash = $1 AND accepted_at IS NULL AND expires_at > now()
		 RETURNING id, company_id, email, role`, inviteHash).Scan(&inviteID, &companyID, &email, &role)
	if errors.Is(err, pgx.ErrNoRows) {
		return candidateworkspace.Started{}, candidateworkspace.ErrInvalidInvite
	}
	if err != nil {
		return candidateworkspace.Started{}, fmt.Errorf("consume invite: %w", err)
	}

	var userID uuid.UUID
	err = tx.QueryRow(ctx, `
		INSERT INTO users (company_id, id, email, role)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (company_id, email) DO UPDATE SET updated_at = now()
		RETURNING id`, companyID, uuid.New(), email, role).Scan(&userID)
	if err != nil {
		return candidateworkspace.Started{}, fmt.Errorf("resolve invited user: %w", err)
	}

	session := domain.Session{
		ID:        params.SessionID,
		CompanyID: companyID,
		UserID:    userID,
		State:     domain.SessionStateActive,
		TokenHash: params.TokenHash,
		ExpiresAt: params.ExpiresAt,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO sessions (company_id, id, user_id, state, token_hash, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at`, session.CompanyID, session.ID, session.UserID,
		session.State, session.TokenHash, session.ExpiresAt).Scan(&session.CreatedAt)
	if err != nil {
		return candidateworkspace.Started{}, fmt.Errorf("create session: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return candidateworkspace.Started{}, fmt.Errorf("commit invite exchange: %w", err)
	}
	return candidateworkspace.Started{Session: session, InviteID: inviteID}, nil
}

func (s *SessionStore) FindSessionByTokenHash(ctx context.Context, tokenHash []byte) (domain.Session, error) {
	var session domain.Session
	err := s.pool.QueryRow(ctx, `
		SELECT id, company_id, user_id, state, token_hash, expires_at,
		       revoked_at, created_at, last_seen_at
		  FROM sessions WHERE token_hash = $1`, tokenHash).Scan(
		&session.ID, &session.CompanyID, &session.UserID, &session.State,
		&session.TokenHash, &session.ExpiresAt, &session.RevokedAt,
		&session.CreatedAt, &session.LastSeenAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Session{}, candidateworkspace.ErrSessionNotFound
	}
	if err != nil {
		return domain.Session{}, fmt.Errorf("find session: %w", err)
	}
	return session, nil
}
