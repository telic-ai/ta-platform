package search

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/telic-ai/ta-platform/internal/adminapi"
	"github.com/telic-ai/ta-platform/internal/rbac"
)

// Config holds the parent key scoped keys derive from.
type Config struct {
	// ParentKey must be a search-only Typesense key (actions
	// documents:search) limited to Collections; never the admin key.
	ParentKey string
	// Host is the Typesense URL the app should search against.
	Host string
	// Collections the parent key allows, reported to the app.
	Collections []string
	// TTL is how long an issued key lives (default and cap: 1h).
	TTL time.Duration
	Now func() time.Time
}

const maxTTL = time.Hour

// Issuer hands out company-scoped keys.
type Issuer struct{ cfg Config }

func NewIssuer(cfg Config) *Issuer {
	if cfg.TTL <= 0 || cfg.TTL > maxTTL {
		cfg.TTL = maxTTL
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Issuer{cfg: cfg}
}

// IssuedKey is a scoped key and what it allows.
type IssuedKey struct {
	Key         string    `json:"key"`
	FilterBy    string    `json:"filter_by"`
	ExpiresAt   time.Time `json:"expires_at"`
	Host        string    `json:"host"`
	Collections []string  `json:"collections"`
}

// Mount adds POST /search/key to the Admin API.
func (i *Issuer) Mount(api *adminapi.Handler) {
	api.Handle("POST /search/key", rbac.PermSearch, i.issue)
}

// issue derives a key for the caller's company. The company comes only
// from the authenticated principal; the request has no body to choose it.
func (i *Issuer) issue(w http.ResponseWriter, r *http.Request) {
	p, _ := rbac.FromContext(r.Context())
	if i.cfg.ParentKey == "" {
		rbac.WriteError(w, http.StatusServiceUnavailable, "search_unconfigured", "search is not configured")
		return
	}
	expires := i.cfg.Now().Add(i.cfg.TTL).UTC().Truncate(time.Second)
	filter := CompanyFilter(p.CompanyID)
	key, err := GenerateScopedKey(i.cfg.ParentKey, EmbeddedParams{FilterBy: filter, ExpiresAt: expires.Unix()})
	if err != nil {
		rbac.WriteError(w, http.StatusServiceUnavailable, "search_unavailable", "could not issue a search key")
		return
	}
	collections := i.cfg.Collections
	if collections == nil {
		collections = []string{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(IssuedKey{Key: key, FilterBy: filter, ExpiresAt: expires, Host: i.cfg.Host, Collections: collections})
}
