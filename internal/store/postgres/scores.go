package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/outbox"
	"github.com/telic-ai/ta-platform/internal/scoring"
)

const foreignKeyViolation = "23503"

// ScoreStore implements scoring.Store.
type ScoreStore struct{ pool *pgxpool.Pool }

func NewScoreStore(pool *pgxpool.Pool) *ScoreStore { return &ScoreStore{pool: pool} }

func (s *ScoreStore) ScoreExists(ctx context.Context, companyID, sessionID uuid.UUID) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM scores WHERE company_id = $1 AND session_id = $2)`,
		companyID, sessionID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check score: %w", err)
	}
	return exists, nil
}

// SaveScore inserts the score and, only if it was new, allocates the next
// interview sequence number and writes score.computed to the outbox in the
// same transaction. Concurrent writers for one session cannot both insert.
func (s *ScoreStore) SaveScore(ctx context.Context, score scoring.Score, build func(int64) (outbox.Message, error)) (bool, error) {
	metricsJSON, err := json.Marshal(score.Metrics)
	if err != nil {
		return false, fmt.Errorf("marshal metrics: %w", err)
	}
	var recommendationJSON, recommendationErr any
	if score.Recommendation != nil {
		if recommendationJSON, err = json.Marshal(score.Recommendation); err != nil {
			return false, fmt.Errorf("marshal recommendation: %w", err)
		}
	} else {
		recommendationErr = score.RecommendationError
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin save score: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	// The interview row lock taken by the sequence allocation below also
	// keeps the Housekeeper from purging the interview mid-transaction.
	tag, err := tx.Exec(ctx, `
		INSERT INTO scores (
			company_id, session_id, interview_id, trigger_event_type, metrics,
			metrics_complete, recommendation, recommendation_model,
			recommendation_error, computed_at
		)
		SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		  FROM interviews
		 WHERE company_id = $1 AND id = $3 AND purged_at IS NULL
		ON CONFLICT (company_id, session_id) DO NOTHING`,
		score.CompanyID, score.SessionID, score.InterviewID, string(score.Trigger), metricsJSON,
		score.MetricsComplete, recommendationJSON, score.RecommendationModel, recommendationErr, score.ComputedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == foreignKeyViolation {
		// The session row is gone: the interview was erased.
		return false, scoring.ErrInterviewGone
	}
	if err != nil {
		return false, fmt.Errorf("insert score: %w", err)
	}
	if tag.RowsAffected() == 0 {
		exists, err := scoreExists(ctx, tx, score.CompanyID, score.SessionID)
		if err != nil {
			return false, err
		}
		if !exists {
			return false, scoring.ErrInterviewGone
		}
		return false, nil
	}
	sequence, err := allocateSequence(ctx, tx, score.CompanyID, score.InterviewID, 1)
	if errors.Is(err, ErrInterviewNotFound) {
		return false, scoring.ErrInterviewGone
	}
	if err != nil {
		return false, err
	}
	message, err := build(sequence)
	if err != nil {
		return false, err
	}
	if err := insertOutbox(ctx, tx, message); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit save score: %w", err)
	}
	return true, nil
}

func scoreExists(ctx context.Context, tx pgx.Tx, companyID, sessionID uuid.UUID) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM scores WHERE company_id = $1 AND session_id = $2)`,
		companyID, sessionID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check score: %w", err)
	}
	return exists, nil
}

// GetScore reads a session's score, scoped by company.
func (s *ScoreStore) GetScore(ctx context.Context, companyID, sessionID uuid.UUID) (scoring.Score, error) {
	var (
		score          scoring.Score
		trigger        string
		metricsJSON    []byte
		recommendation []byte
		recErr         *string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT company_id, session_id, interview_id, trigger_event_type, metrics,
		       metrics_complete, recommendation, recommendation_model,
		       recommendation_error, computed_at
		  FROM scores WHERE company_id = $1 AND session_id = $2`, companyID, sessionID).Scan(
		&score.CompanyID, &score.SessionID, &score.InterviewID, &trigger, &metricsJSON,
		&score.MetricsComplete, &recommendation, &score.RecommendationModel, &recErr, &score.ComputedAt)
	if err != nil {
		return scoring.Score{}, fmt.Errorf("get score: %w", err)
	}
	score.Trigger = events.EventType(trigger)
	if err := json.Unmarshal(metricsJSON, &score.Metrics); err != nil {
		return scoring.Score{}, fmt.Errorf("decode metrics: %w", err)
	}
	if recommendation != nil {
		score.Recommendation = &scoring.Recommendation{}
		if err := json.Unmarshal(recommendation, score.Recommendation); err != nil {
			return scoring.Score{}, fmt.Errorf("decode recommendation: %w", err)
		}
	}
	if recErr != nil {
		score.RecommendationError = *recErr
	}
	return score, nil
}
