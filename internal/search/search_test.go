package search

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/adminapi"
	"github.com/telic-ai/ta-platform/internal/rbac"
)

// The example from Typesense's scoped-key documentation.
func TestGenerateScopedKeyMatchesTypesenseExample(t *testing.T) {
	key, err := GenerateScopedKey("RN23GFr1s6jQ9kgSNg2O7fYcAUXU7127", EmbeddedParams{FilterBy: "company_id:124", ExpiresAt: 1906054106})
	if err != nil {
		t.Fatal(err)
	}
	const want = "OW9DYWZGS1Q1RGdSbmo0S1QrOWxhbk9PL2kxbTU1eXA3bCthdmE5eXJKRT1STjIzeyJmaWx0ZXJfYnkiOiJjb21wYW55X2lkOjEyNCIsImV4cGlyZXNfYXQiOjE5MDYwNTQxMDZ9"
	if key != want {
		t.Fatalf("key = %s", key)
	}
}

func TestGenerateScopedKeyRejects(t *testing.T) {
	if _, err := GenerateScopedKey("abc", EmbeddedParams{FilterBy: "x"}); err == nil {
		t.Error("short parent key accepted")
	}
	if _, err := GenerateScopedKey("parent-key", EmbeddedParams{}); err == nil {
		t.Error("key without a filter accepted")
	}
}

func TestVerifyScopedKey(t *testing.T) {
	company := uuid.New()
	params := EmbeddedParams{FilterBy: CompanyFilter(company), ExpiresAt: 1906054106}
	key, _ := GenerateScopedKey("parent-key-1234", params)
	got, err := VerifyScopedKey("parent-key-1234", key)
	if err != nil || got != params {
		t.Fatalf("verify = %+v, %v", got, err)
	}
	if _, err := VerifyScopedKey("other-parent-key", key); err == nil {
		t.Error("verified against the wrong parent")
	}
	// Swapping the embedded company breaks the HMAC.
	raw, _ := base64.StdEncoding.DecodeString(key)
	other := uuid.New()
	forged := base64.StdEncoding.EncodeToString([]byte(strings.Replace(string(raw), company.String(), other.String(), 1)))
	if _, err := VerifyScopedKey("parent-key-1234", forged); err == nil {
		t.Error("forged filter verified")
	}
	for _, bad := range []string{"", "!!", base64.StdEncoding.EncodeToString([]byte("short"))} {
		if _, err := VerifyScopedKey("parent-key-1234", bad); err == nil {
			t.Errorf("%q verified", bad)
		}
	}
}

func TestCompanyFilter(t *testing.T) {
	id := uuid.MustParse("aaaaaaaa-1111-2222-3333-444444444444")
	if got := CompanyFilter(id); got != "company_id:=aaaaaaaa-1111-2222-3333-444444444444" {
		t.Fatalf("filter = %s", got)
	}
}

type noStore struct{ adminapi.Store }

func issue(t *testing.T, issuer *Issuer, p rbac.Principal) *httptest.ResponseRecorder {
	t.Helper()
	api := adminapi.NewHandler(noStore{}, rbac.ResolverFunc(func(*http.Request) (rbac.Principal, error) { return p, nil }), adminapi.Config{})
	issuer.Mount(api)
	w := httptest.NewRecorder()
	api.Routes().ServeHTTP(w, httptest.NewRequest("POST", "/search/key", nil))
	return w
}

func TestIssuedKeyHardEmbedsTheCallersCompany(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	issuer := NewIssuer(Config{ParentKey: "search-only-parent", Host: "http://localhost:8108", Collections: []string{"interviews"},
		TTL: 30 * time.Minute, Now: func() time.Time { return now }})
	for _, role := range rbac.Roles {
		company := uuid.New()
		w := issue(t, issuer, rbac.Principal{CompanyID: company, UserID: uuid.New(), Role: role})
		if w.Code != http.StatusCreated || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: status %d %s", role, w.Code, w.Body)
		}
		var body IssuedKey
		_ = json.Unmarshal(w.Body.Bytes(), &body)
		params, err := VerifyScopedKey("search-only-parent", body.Key)
		if err != nil {
			t.Fatal(err)
		}
		want := "company_id:=" + company.String()
		if params.FilterBy != want || body.FilterBy != want {
			t.Fatalf("%s: embedded %q, reported %q, want %q", role, params.FilterBy, body.FilterBy, want)
		}
		if params.ExpiresAt != now.Add(30*time.Minute).Unix() || !body.ExpiresAt.Equal(now.Add(30*time.Minute)) {
			t.Fatalf("expiry = %d / %s", params.ExpiresAt, body.ExpiresAt)
		}
		if body.Host != "http://localhost:8108" || len(body.Collections) != 1 {
			t.Fatalf("body = %+v", body)
		}
	}
}

func TestIssuerCapsTTLAndNeedsAParentKey(t *testing.T) {
	now := time.Now()
	issuer := NewIssuer(Config{ParentKey: "parent-key", TTL: 48 * time.Hour, Now: func() time.Time { return now }})
	w := issue(t, issuer, rbac.Principal{CompanyID: uuid.New(), Role: rbac.RoleViewer})
	var body IssuedKey
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body.ExpiresAt.After(now.Add(time.Hour)) || body.Collections == nil {
		t.Fatalf("body = %+v", body)
	}
	if w := issue(t, NewIssuer(Config{}), rbac.Principal{CompanyID: uuid.New(), Role: rbac.RoleOwner}); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured status = %d", w.Code)
	}
}

func TestSearchKeyRequiresAuthentication(t *testing.T) {
	api := adminapi.NewHandler(noStore{}, rbac.ResolverFunc(func(*http.Request) (rbac.Principal, error) {
		return rbac.Principal{}, rbac.ErrUnauthenticated
	}), adminapi.Config{})
	NewIssuer(Config{ParentKey: "parent-key"}).Mount(api)
	w := httptest.NewRecorder()
	api.Routes().ServeHTTP(w, httptest.NewRequest("POST", "/search/key", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", w.Code)
	}
}
