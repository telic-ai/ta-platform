//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/telic-ai/ta-platform/internal/platform/config"
)

func TestClientPing(t *testing.T) {
	cfg, err := config.Load("postgres-integration-test")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := New(ctx, cfg.PostgresDSN)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
