//go:build integration

package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/platform/config"
)

func TestLockerSingleHolderAndExpiry(t *testing.T) {
	cfg, _ := config.Load("redis-lock-integration-test")
	client, _ := New(cfg.RedisAddr)
	defer client.Close()
	ctx := context.Background()
	locker := NewLocker(client, "test-lock:"+uuid.NewString()+":")

	release, err := locker.Acquire(ctx, "session-1", time.Minute)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if _, err := locker.Acquire(ctx, "session-1", time.Minute); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Acquire err = %v, want ErrLocked", err)
	}
	if other, err := locker.Acquire(ctx, "session-2", time.Minute); err != nil {
		t.Fatalf("other key blocked: %v", err)
	} else {
		_ = other(ctx)
	}
	if err := release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	again, err := locker.Acquire(ctx, "session-1", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}

	// Once expired, a new holder takes over and the stale release must not
	// remove the new holder's lock.
	time.Sleep(200 * time.Millisecond)
	fresh, err := locker.Acquire(ctx, "session-1", time.Minute)
	if err != nil {
		t.Fatalf("Acquire after expiry: %v", err)
	}
	_ = again(ctx)
	if _, err := locker.Acquire(ctx, "session-1", time.Minute); !errors.Is(err, ErrLocked) {
		t.Errorf("stale release freed the new holder's lock: %v", err)
	}
	_ = fresh(ctx)
}
