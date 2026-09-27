// Package auth holds the opaque bearer-token primitives shared by services
// that authenticate against Postgres sessions.
package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"strings"

	"github.com/telic-ai/ta-platform/internal/domain"
)

var ErrSessionNotFound = errors.New("session not found")

// SessionFinder looks up a session by the hash of its bearer token.
type SessionFinder interface {
	FindSessionByTokenHash(context.Context, []byte) (domain.Session, error)
}

// HashToken returns the one-way representation used for invite and session
// lookups. Raw tokens must never be persisted or logged.
func HashToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// BearerToken extracts the token from an "Authorization: Bearer <token>"
// header.
func BearerToken(r *http.Request) (string, bool) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.Contains(token, " ") {
		return "", false
	}
	return token, true
}
