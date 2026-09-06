//go:build integration

package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/telic-ai/ta-platform/internal/platform/config"
)

func TestClientPing(t *testing.T) {
	cfg, err := config.Load("clickhouse-integration-test")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	client, err := New(cfg.ClickHouseDSN)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
