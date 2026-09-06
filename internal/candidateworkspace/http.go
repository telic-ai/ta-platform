package candidateworkspace

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/telic-ai/ta-platform/internal/domain"
)

type SessionFinder interface {
	FindSessionByTokenHash(context.Context, []byte) (domain.Session, error)
}

type contextKey struct{}

// ActiveSessionFromContext returns the tenant and user scope established by
// RequireActiveSession.
func ActiveSessionFromContext(ctx context.Context) (domain.Session, bool) {
	session, ok := ctx.Value(contextKey{}).(domain.Session)
	return session, ok
}

// RequireActiveSession authenticates an opaque bearer token. Known sessions
// outside the Active state intentionally return 409, not 401.
func RequireActiveSession(finder SessionFinder, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.Contains(token, " ") {
			writeError(w, http.StatusUnauthorized, "invalid_session_token", "a valid bearer token is required")
			return
		}
		session, err := finder.FindSessionByTokenHash(r.Context(), HashToken(token))
		if errors.Is(err, ErrSessionNotFound) {
			writeError(w, http.StatusUnauthorized, "invalid_session_token", "a valid bearer token is required")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not validate session")
			return
		}
		if session.State != domain.SessionStateActive || session.RevokedAt != nil || !session.ExpiresAt.After(time.Now()) {
			writeError(w, http.StatusConflict, "session_not_active", "session is not Active")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), contextKey{}, session)))
	})
}

type HTTPHandler struct {
	service *Service
	finder  SessionFinder
}

func NewHTTPHandler(service *Service, finder SessionFinder) *HTTPHandler {
	return &HTTPHandler{service: service, finder: finder}
}

func (h *HTTPHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /session/start", h.startSession)
	// Candidate Workspace endpoints are introduced incrementally. Mounting the
	// guard at the workspace boundary makes every current and future request
	// subject to the state machine.
	mux.Handle("/candidate/", RequireActiveSession(h.finder, http.NotFoundHandler()))
	return mux
}

func (h *HTTPHandler) startSession(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var request struct {
		InviteToken string `json:"inviteToken"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.InviteToken == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "inviteToken is required")
		return
	}
	response, err := h.service.Start(r.Context(), request.InviteToken)
	if errors.Is(err, ErrInvalidInvite) {
		writeError(w, http.StatusUnauthorized, "invalid_invite", ErrInvalidInvite.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "session_start_failed", "could not start session")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, response)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"code": code, "message": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
