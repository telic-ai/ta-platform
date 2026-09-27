package candidateworkspace

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/telic-ai/ta-platform/internal/auth"
	"github.com/telic-ai/ta-platform/internal/domain"
)

type contextKey struct{}

// ActiveSessionFromContext returns the tenant and user scope established by
// RequireActiveSession.
func ActiveSessionFromContext(ctx context.Context) (domain.Session, bool) {
	session, ok := ctx.Value(contextKey{}).(domain.Session)
	return session, ok
}

// RequireActiveSession authenticates a candidate's opaque bearer token.
// Company member tokens are rejected. Known candidate sessions outside the
// Active state intentionally return 409, not 401.
func RequireActiveSession(finder auth.SessionFinder, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := auth.BearerToken(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "invalid_session_token", "a valid bearer token is required")
			return
		}
		session, err := finder.FindSessionByTokenHash(r.Context(), auth.HashToken(token))
		if errors.Is(err, auth.ErrSessionNotFound) || (err == nil && !session.IsCandidate()) {
			writeError(w, http.StatusUnauthorized, "invalid_session_token", "a valid bearer token is required")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not validate session")
			return
		}
		if !session.IsActive(time.Now()) {
			writeError(w, http.StatusConflict, "session_not_active", "session is not Active")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), contextKey{}, session)))
	})
}

type HTTPHandler struct {
	service *Service
	finder  auth.SessionFinder
	prompts *PromptService
	runs    *RunService
	diffs   *DiffService
}

func NewHTTPHandler(service *Service, finder auth.SessionFinder) *HTTPHandler {
	return &HTTPHandler{service: service, finder: finder}
}

// WithPrompts enables POST /session/prompt.
func (h *HTTPHandler) WithPrompts(prompts *PromptService) *HTTPHandler {
	h.prompts = prompts
	return h
}

// WithRuns enables POST /session/run.
func (h *HTTPHandler) WithRuns(runs *RunService) *HTTPHandler {
	h.runs = runs
	return h
}

// WithDiffs enables POST /session/diff.
func (h *HTTPHandler) WithDiffs(diffs *DiffService) *HTTPHandler {
	h.diffs = diffs
	return h
}

func (h *HTTPHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /session/start", h.startSession)
	if h.prompts != nil {
		mux.Handle("POST /session/prompt", RequireActiveSession(h.finder, http.HandlerFunc(h.submitPrompt)))
	}
	if h.runs != nil {
		mux.Handle("POST /session/run", RequireActiveSession(h.finder, http.HandlerFunc(h.runCode)))
	}
	if h.diffs != nil {
		mux.Handle("POST /session/diff", RequireActiveSession(h.finder, http.HandlerFunc(h.submitDiff)))
	}
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
