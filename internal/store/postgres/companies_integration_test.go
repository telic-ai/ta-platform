//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
)

func TestAIKeyMode(t *testing.T) {
	cfg, _ := config.Load("companies-integration-test")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)
	store := postgres.NewCompanyStore(pool)
	companyID, _ := seedInterview(t, ctx, pool)

	if mode, err := store.AIKeyMode(ctx, companyID); err != nil || mode != "managed" {
		t.Fatalf("default mode = %q, %v", mode, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE companies SET ai_key_mode = 'byok' WHERE id = $1`, companyID); err != nil {
		t.Fatal(err)
	}
	if mode, _ := store.AIKeyMode(ctx, companyID); mode != "byok" {
		t.Errorf("mode = %q, want byok", mode)
	}
	if _, err := pool.Exec(ctx, `UPDATE companies SET ai_key_mode = 'free' WHERE id = $1`, companyID); err == nil {
		t.Error("check constraint accepted an unknown mode")
	}
	if _, err := store.AIKeyMode(ctx, uuid.New()); !errors.Is(err, postgres.ErrCompanyNotFound) {
		t.Errorf("unknown company err = %v", err)
	}
}
