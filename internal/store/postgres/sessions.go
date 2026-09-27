package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/candidateworkspace"
	"github.com/telic-ai/ta-platform/internal/domain"
)

// SessionStore persists Candidate Workspace invite exchanges and sessions.
type SessionStore struct{ pool *pgxpool.Pool }

func NewSessionStore(pool *pgxpool.Pool) *SessionStore { return &SessionStore{pool: pool} }

// StartSession consumes the invite, allocates the interview's next event
// sequence number, creates the candidate session, and writes its event to
// the outbox, all in one transaction.
func (s *SessionStore) StartSession(ctx context.Context, inviteHash []byte, params candidateworkspace.StartParams, build candidateworkspace.EventBuilder) (candidateworkspace.Started, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return candidateworkspace.Started{}, fmt.Errorf("begin invite exchange: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Only candidate invites (those bound to an interview) can start a
	// workspace session. Rolling back on any later failure keeps the invite
	// unconsumed.
	var inviteID, companyID, interviewID uuid.UUID
	err = tx.QueryRow(ctx, `
		UPDATE invites
		   SET accepted_at = now()
		 WHERE token_hash = $1 AND accepted_at IS NULL AND expires_at > now()
		   AND interview_id IS NOT NULL
		 RETURNING id, company_id, interview_id`, inviteHash).Scan(&inviteID, &companyID, &interviewID)
	if errors.Is(err, pgx.ErrNoRows) {
		return candidateworkspace.Started{}, candidateworkspace.ErrInvalidInvite
	}
	if err != nil {
		return candidateworkspace.Started{}, fmt.Errorf("consume invite: %w", err)
	}

	// The row lock taken by this increment serializes sequence allocation
	// per interview until the transaction commits.
	var sequence int64
	err = tx.QueryRow(ctx, `
		UPDATE interviews
		   SET last_sequence_number = last_sequence_number + 1, updated_at = now()
		 WHERE company_id = $1 AND id = $2 AND purged_at IS NULL
		 RETURNING last_sequence_number`, companyID, interviewID).Scan(&sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		return candidateworkspace.Started{}, candidateworkspace.ErrInvalidInvite
	}
	if err != nil {
		return candidateworkspace.Started{}, fmt.Errorf("allocate interview sequence: %w", err)
	}

	session := domain.Session{
		ID:          params.SessionID,
		CompanyID:   companyID,
		InterviewID: &interviewID,
		State:       domain.SessionStateActive,
		TokenHash:   params.TokenHash,
		ExpiresAt:   params.ExpiresAt,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO sessions (company_id, id, interview_id, state, token_hash, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at`, session.CompanyID, session.ID, session.InterviewID,
		session.State, session.TokenHash, session.ExpiresAt).Scan(&session.CreatedAt)
	if err != nil {
		return candidateworkspace.Started{}, fmt.Errorf("create session: %w", err)
	}

	started := candidateworkspace.Started{Session: session, InviteID: inviteID, SequenceNumber: sequence}
	message, err := build(started)
	if err != nil {
		return candidateworkspace.Started{}, err
	}
	if err := insertOutbox(ctx, tx, message); err != nil {
		return candidateworkspace.Started{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return candidateworkspace.Started{}, fmt.Errorf("commit invite exchange: %w", err)
	}
	return started, nil
}

// FindSessionByTokenHash implements auth.SessionFinder for both company
// member and candidate sessions.
func (s *SessionStore) FindSessionByTokenHash(ctx context.Context, tokenHash []byte) (domain.Session, error) {
	var session domain.Session
	err := s.pool.QueryRow(ctx, `
		SELECT id, company_id, user_id, interview_id, state, token_hash, expires_at,
		       revoked_at, created_at, last_seen_at
		  FROM sessions WHERE token_hash = $1`, tokenHash).Scan(
		&session.ID, &session.CompanyID, &session.UserID, &session.InterviewID, &session.State,
		&session.TokenHash, &session.ExpiresAt, &session.RevokedAt,
		&session.CreatedAt, &session.LastSeenAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Session{}, auth.ErrSessionNotFound
	}
	if err != nil {
		return domain.Session{}, fmt.Errorf("find session: %w", err)
	}
	return session, nil
}
