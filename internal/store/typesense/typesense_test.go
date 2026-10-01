package typesense

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newServer(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-TYPESENSE-API-KEY"); got != "key" {
			t.Errorf("API key header = %q", got)
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	return New(server.URL+"/", "key")
}

func TestUpsertSendsJSONLWithUpsertAction(t *testing.T) {
	var gotBody, gotQuery, gotPath string
	client := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody, gotQuery, gotPath = string(body), r.URL.RawQuery, r.URL.Path
		_, _ = io.WriteString(w, "{\"success\":true}\n{\"success\":true}")
	})
	docs := []any{map[string]string{"id": "a"}, map[string]string{"id": "b"}}
	if err := client.Upsert(context.Background(), "events", docs); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if gotPath != "/collections/events/documents/import" || gotQuery != "action=upsert" {
		t.Fatalf("request = %s?%s", gotPath, gotQuery)
	}
	if gotBody != "{\"id\":\"a\"}\n{\"id\":\"b\"}\n" {
		t.Fatalf("body = %q", gotBody)
	}
}

func TestUpsertEmptyIsNoop(t *testing.T) {
	client := newServer(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected request") })
	if err := client.Upsert(context.Background(), "events", nil); err != nil {
		t.Fatal(err)
	}
}

func TestUpsertReportsRejectedDocuments(t *testing.T) {
	client := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "{\"success\":true}\n{\"success\":false,\"error\":\"bad field\"}\n")
	})
	err := client.Upsert(context.Background(), "events", []any{1, 2})
	var importErr *ImportError
	if !errors.As(err, &importErr) || len(importErr.Failures) != 1 || importErr.Failures[0].Index != 1 {
		t.Fatalf("err = %v", err)
	}
}

func TestUpsertRejectsShortResultSet(t *testing.T) {
	client := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "{\"success\":true}")
	})
	if err := client.Upsert(context.Background(), "events", []any{1, 2}); err == nil {
		t.Fatal("missing import result accepted")
	}
}

func TestEnsureCollectionToleratesConflict(t *testing.T) {
	client := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/collections" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"message":"already exists"}`)
	})
	if err := client.EnsureCollection(context.Background(), Schema{Name: "events"}); err != nil {
		t.Fatal(err)
	}
}

func TestErrorsCarryStatusAndMessage(t *testing.T) {
	client := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"message":"bad schema"}`)
	})
	err := client.EnsureCollection(context.Background(), Schema{Name: "events"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 400 || apiErr.Message != "bad schema" {
		t.Fatalf("err = %v", err)
	}
}

func TestDeleteByFilter(t *testing.T) {
	client := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Query().Get("filter_by") != "company_id:=c && interview_id:=i" {
			t.Errorf("request = %s %s", r.Method, r.URL.String())
		}
		_, _ = io.WriteString(w, `{"num_deleted":3}`)
	})
	n, err := client.DeleteByFilter(context.Background(), "events", "company_id:=c && interview_id:=i")
	if err != nil || n != 3 {
		t.Fatalf("DeleteByFilter = %d, %v", n, err)
	}
}

func TestDeleteByFilterMissingCollectionDeletesNothing(t *testing.T) {
	client := newServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) })
	n, err := client.DeleteByFilter(context.Background(), "events", "company_id:=c")
	if err != nil || n != 0 {
		t.Fatalf("DeleteByFilter = %d, %v", n, err)
	}
}

func TestDeleteByFilterRefusesEmptyFilter(t *testing.T) {
	client := newServer(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected request") })
	if _, err := client.DeleteByFilter(context.Background(), "events", "  "); err == nil {
		t.Fatal("empty filter accepted")
	}
}

func TestSearchDecodesHits(t *testing.T) {
	client := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("q") != "binary search" || q.Get("query_by") != "text" || q.Get("filter_by") != "company_id:=c" || q.Get("per_page") != "5" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		_, _ = io.WriteString(w, `{"found":1,"hits":[{"document":{"id":"e1"}}]}`)
	})
	result, err := client.Search(context.Background(), "events", SearchParams{Query: "binary search", QueryBy: "text", FilterBy: "company_id:=c", PerPage: 5})
	if err != nil || result.Found != 1 || len(result.Hits) != 1 || !strings.Contains(string(result.Hits[0]), "e1") {
		t.Fatalf("Search = %+v, %v", result, err)
	}
}

func TestHealthAndDeleteCollection(t *testing.T) {
	client := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = io.WriteString(w, `{"ok":true}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	if err := client.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteCollection(context.Background(), "missing"); err != nil {
		t.Fatal(err)
	}
}
