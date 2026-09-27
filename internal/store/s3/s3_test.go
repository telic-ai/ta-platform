package s3_test

import (
	"context"
	"errors"
	"testing"

	"github.com/telic-ai/ta-platform/internal/store/s3"
	"github.com/telic-ai/ta-platform/internal/testutil/s3test"
)

func TestPutGetRoundTrip(t *testing.T) {
	client := s3test.Client(t, "snapshots")
	ctx := context.Background()
	if err := client.Put(ctx, "a/b.json", []byte(`{"x":1}`), "application/json"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := client.Get(ctx, "a/b.json", 1024)
	if err != nil || string(got) != `{"x":1}` {
		t.Fatalf("Get = %q, %v", got, err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Errorf("Ping: %v", err)
	}
	if err := client.EnsureBucket(ctx); err != nil {
		t.Errorf("EnsureBucket on existing bucket: %v", err)
	}
}

func TestGetMissingAndOversized(t *testing.T) {
	client := s3test.Client(t, "snapshots")
	ctx := context.Background()
	if _, err := client.Get(ctx, "missing", 10); !errors.Is(err, s3.ErrNotFound) {
		t.Errorf("missing err = %v", err)
	}
	_ = client.Put(ctx, "big", make([]byte, 11), "application/octet-stream")
	if _, err := client.Get(ctx, "big", 10); err == nil {
		t.Error("oversized object accepted")
	}
	if _, err := client.Get(ctx, "big", 11); err != nil {
		t.Errorf("exact-size object rejected: %v", err)
	}
}
