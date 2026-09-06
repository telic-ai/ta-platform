//go:build integration

package s3

import (
	"context"
	"testing"
	"time"

	"github.com/telic-ai/ta-platform/internal/platform/config"
)

func TestClientPing(t *testing.T) {
	cfg, err := config.Load("s3-integration-test")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	client := New(Config{
		Endpoint:  cfg.S3Endpoint,
		Bucket:    cfg.S3Bucket,
		AccessKey: cfg.S3AccessKey,
		SecretKey: cfg.S3SecretKey,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
