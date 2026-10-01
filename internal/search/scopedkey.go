// Package search issues company-scoped Typesense search keys to the
// company app. A scoped key is derived offline from a search-only parent
// key and embeds filter_by company_id:=<company>; Typesense ANDs that
// filter into every search made with the key and rejects the key if its
// embedded parameters are altered, so a browser holding it can only ever
// see its own company's documents.
package search

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// EmbeddedParams are the search parameters baked into a scoped key.
type EmbeddedParams struct {
	FilterBy string `json:"filter_by"`
	// ExpiresAt is a Unix time after which Typesense rejects the key.
	ExpiresAt int64 `json:"expires_at,omitempty"`
}

// CompanyFilter is the filter every company-scoped key embeds.
func CompanyFilter(companyID uuid.UUID) string { return "company_id:=" + companyID.String() }

const prefixLen = 4

// GenerateScopedKey derives a Typesense scoped search key from parentKey:
// base64(base64(HMAC-SHA256(parentKey, params)) + parentKey[:4] + params).
func GenerateScopedKey(parentKey string, params EmbeddedParams) (string, error) {
	if len(parentKey) < prefixLen {
		return "", errors.New("search: parent key is too short")
	}
	if params.FilterBy == "" {
		return "", errors.New("search: a scoped key must embed a filter")
	}
	body, err := json.Marshal(params)
	if err != nil {
		return "", fmt.Errorf("search: encode embedded params: %w", err)
	}
	return base64.StdEncoding.EncodeToString([]byte(digest(parentKey, body) + parentKey[:prefixLen] + string(body))), nil
}

// VerifyScopedKey checks that key was derived from parentKey and returns
// its embedded parameters.
func VerifyScopedKey(parentKey, key string) (EmbeddedParams, error) {
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return EmbeddedParams{}, errors.New("search: key is not base64")
	}
	// The digest is base64 of 32 bytes: 44 characters.
	const digestLen = 44
	if len(raw) < digestLen+prefixLen || len(parentKey) < prefixLen {
		return EmbeddedParams{}, errors.New("search: key is too short")
	}
	sum, prefix, body := string(raw[:digestLen]), string(raw[digestLen:digestLen+prefixLen]), raw[digestLen+prefixLen:]
	if prefix != parentKey[:prefixLen] || !hmac.Equal([]byte(sum), []byte(digest(parentKey, body))) {
		return EmbeddedParams{}, errors.New("search: key was not derived from this parent key")
	}
	var params EmbeddedParams
	if err := json.Unmarshal(body, &params); err != nil {
		return EmbeddedParams{}, fmt.Errorf("search: embedded params: %w", err)
	}
	return params, nil
}

func digest(parentKey string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(parentKey))
	mac.Write(body)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
