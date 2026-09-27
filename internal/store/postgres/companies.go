package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrCompanyNotFound means no company has the ID.
var ErrCompanyNotFound = errors.New("company not found")

// CompanyStore reads company settings.
type CompanyStore struct{ pool *pgxpool.Pool }

func NewCompanyStore(pool *pgxpool.Pool) *CompanyStore { return &CompanyStore{pool: pool} }

// AIKeyMode returns "managed" or "byok" for the company.
func (s *CompanyStore) AIKeyMode(ctx context.Context, companyID uuid.UUID) (string, error) {
	var mode string
	err := s.pool.QueryRow(ctx, `SELECT ai_key_mode FROM companies WHERE id = $1`, companyID).Scan(&mode)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrCompanyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("read ai_key_mode: %w", err)
	}
	return mode, nil
}
