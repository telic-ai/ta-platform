//go:build integration

package demo

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/rbac"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
)

func TestSeedCreatesMembersWithWorkingSessions(t *testing.T) {
	cfg, _ := config.Load("demo-seed-integration-test")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)

	result, err := Seed(ctx, pool, "Demo Co", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Members) != 3 {
		t.Fatalf("members = %+v", result.Members)
	}
	store := postgres.NewAdminStore(pool)
	resolver := rbac.Resolver{Sessions: postgres.NewSessionStore(pool), Roles: store}
	for _, m := range result.Members {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+m.Token)
		p, err := resolver.Resolve(r)
		if err != nil || p.CompanyID != result.CompanyID || p.UserID != m.UserID || p.Role != m.Role {
			t.Fatalf("%s resolves to %+v, %v", m.Role, p, err)
		}
	}
	// A second seed is a separate company.
	again, err := Seed(ctx, pool, "Demo Co", time.Hour)
	if err != nil || again.CompanyID == result.CompanyID {
		t.Fatalf("second seed = %+v, %v", again, err)
	}
}
