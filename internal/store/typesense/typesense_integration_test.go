//go:build integration

package typesense

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/telic-ai/ta-platform/internal/platform/config"
)

func TestCollectionUpsertSearchDelete(t *testing.T) {
	cfg, err := config.Load("typesense-integration-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := New(cfg.TypesenseURL, cfg.TypesenseKey)
	if err := client.Health(ctx); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("test_%d", time.Now().UnixNano())
	schema := Schema{Name: name, Fields: []Field{
		{Name: "company_id", Type: "string", Facet: true},
		{Name: "text", Type: "string"},
	}}
	if err := client.EnsureCollection(ctx, schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.DeleteCollection(context.Background(), name) })
	if err := client.EnsureCollection(ctx, schema); err != nil {
		t.Fatalf("second EnsureCollection: %v", err)
	}

	docs := []any{
		map[string]string{"id": "1", "company_id": "a", "text": "hello world"},
		map[string]string{"id": "2", "company_id": "b", "text": "hello there"},
	}
	for range 2 {
		if err := client.Upsert(ctx, name, docs); err != nil {
			t.Fatal(err)
		}
	}
	result, err := client.Search(ctx, name, SearchParams{Query: "hello", QueryBy: "text"})
	if err != nil || result.Found != 2 {
		t.Fatalf("Search = %+v, %v; want 2 after repeated upsert", result, err)
	}
	n, err := client.DeleteByFilter(ctx, name, "company_id:=a")
	if err != nil || n != 1 {
		t.Fatalf("DeleteByFilter = %d, %v", n, err)
	}
	if n, _ := client.DeleteByFilter(ctx, name, "company_id:=a"); n != 0 {
		t.Fatalf("second delete removed %d", n)
	}
	result, _ = client.Search(ctx, name, SearchParams{Query: "hello", QueryBy: "text"})
	if result.Found != 1 {
		t.Fatalf("found %d after delete, want 1", result.Found)
	}
	if err := client.Upsert(ctx, name, []any{map[string]any{"id": "3", "company_id": 5}}); err == nil {
		t.Fatal("invalid document accepted")
	}
}
