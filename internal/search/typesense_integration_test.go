//go:build integration

package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/rbac"
)

func principal(company uuid.UUID) rbac.Principal {
	return rbac.Principal{CompanyID: company, UserID: uuid.New(), Role: rbac.RoleViewer}
}

func typesense(t *testing.T, method, path, key string, body []byte) (int, map[string]any) {
	t.Helper()
	base := os.Getenv("TYPESENSE_URL")
	if base == "" {
		base = "http://localhost:8108"
	}
	req, _ := http.NewRequestWithContext(context.Background(), method, base+path, bytes.NewReader(body))
	req.Header.Set("X-TYPESENSE-API-KEY", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("typesense: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func titles(t *testing.T, key, collection, filter string) (int, []string) {
	t.Helper()
	q := url.Values{"q": {"*"}, "query_by": {"title"}}
	if filter != "" {
		q.Set("filter_by", filter)
	}
	status, body := typesense(t, "GET", "/collections/"+collection+"/documents/search?"+q.Encode(), key, nil)
	var out []string
	hits, _ := body["hits"].([]any)
	for _, h := range hits {
		out = append(out, h.(map[string]any)["document"].(map[string]any)["title"].(string))
	}
	return status, out
}

// TestScopedKeyConfinesSearchToOneCompany proves against a real Typesense
// that an issued key only ever returns its company's documents.
func TestScopedKeyConfinesSearchToOneCompany(t *testing.T) {
	admin := os.Getenv("TYPESENSE_API_KEY")
	if admin == "" {
		admin = "local-dev-key"
	}
	collection := "search_test_" + uuid.NewString()[:8]
	schema, _ := json.Marshal(map[string]any{"name": collection, "fields": []map[string]string{
		{"name": "company_id", "type": "string"}, {"name": "title", "type": "string"}}})
	if status, body := typesense(t, "POST", "/collections", admin, schema); status != 201 {
		t.Fatalf("create collection: %d %v", status, body)
	}
	t.Cleanup(func() { typesense(t, "DELETE", "/collections/"+collection, admin, nil) })

	a, b := uuid.New(), uuid.New()
	// A near-miss ID guards against prefix matching.
	nearA := a.String()[:30]
	docs := fmt.Sprintf(`{"company_id":%q,"title":"alpha"}`+"\n"+`{"company_id":%q,"title":"beta"}`+"\n"+`{"company_id":%q,"title":"near"}`, a, b, nearA)
	if status, _ := typesense(t, "POST", "/collections/"+collection+"/documents/import?action=create", admin, []byte(docs)); status != 200 {
		t.Fatalf("import: %d", status)
	}
	keySpec, _ := json.Marshal(map[string]any{"description": "search-only parent for " + collection,
		"actions": []string{"documents:search"}, "collections": []string{collection}})
	status, created := typesense(t, "POST", "/keys", admin, keySpec)
	if status != 201 {
		t.Fatalf("create parent key: %d %v", status, created)
	}
	parent := created["value"].(string)
	t.Cleanup(func() { typesense(t, "DELETE", fmt.Sprintf("/keys/%v", created["id"]), admin, nil) })

	issuer := NewIssuer(Config{ParentKey: parent, Collections: []string{collection}})
	keyFor := func(company uuid.UUID) string {
		var issued IssuedKey
		w := issue(t, issuer, principal(company))
		_ = json.Unmarshal(w.Body.Bytes(), &issued)
		return issued.Key
	}
	keyA := keyFor(a)

	if status, got := titles(t, keyA, collection, ""); status != 200 || len(got) != 1 || got[0] != "alpha" {
		t.Fatalf("company A search = %d %v", status, got)
	}
	// A caller-supplied filter is ANDed with the embedded one, never replaces it.
	for _, filter := range []string{"company_id:=" + b.String(), "company_id:!=" + a.String(), "title:=beta"} {
		if _, got := titles(t, keyA, collection, filter); len(got) != 0 {
			t.Fatalf("filter %q escaped the scope: %v", filter, got)
		}
	}
	if _, got := titles(t, keyFor(b), collection, ""); len(got) != 1 || got[0] != "beta" {
		t.Fatalf("company B search = %v", got)
	}
	// A key whose embedded filter is edited is rejected outright.
	forged, _ := GenerateScopedKey("xxxx"+parent[4:], EmbeddedParams{FilterBy: CompanyFilter(b)})
	if status, _ := titles(t, forged, collection, ""); status != 401 {
		t.Fatalf("forged key status = %d", status)
	}
	// An expired key is rejected.
	expired, _ := GenerateScopedKey(parent, EmbeddedParams{FilterBy: CompanyFilter(a), ExpiresAt: time.Now().Add(-time.Minute).Unix()})
	if status, _ := titles(t, expired, collection, ""); status != 401 {
		t.Fatalf("expired key status = %d", status)
	}
}
