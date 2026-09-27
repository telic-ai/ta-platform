// Package s3test serves an in-process S3-compatible endpoint for tests.
package s3test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/telic-ai/ta-platform/internal/store/s3"
)

// Client returns a client for a fresh in-memory bucket.
func Client(t *testing.T, bucket string) *s3.Client {
	t.Helper()
	server := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(server.Close)
	client := s3.New(s3.Config{Endpoint: server.URL, Bucket: bucket, AccessKey: "test", SecretKey: "test"})
	if err := client.EnsureBucket(context.Background()); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return client
}
