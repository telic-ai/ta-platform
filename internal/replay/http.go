package replay

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/google/uuid"
)

var ErrUnauthenticated = errors.New("company identity is required")

// TenantResolver supplies the authenticated company. It is intentionally
// separate from request parameters so clients cannot select another tenant.
type TenantResolver interface {
	CompanyID(*http.Request) (uuid.UUID, error)
}

type HeaderTenantResolver struct{}

// CompanyID resolves the development gateway's authenticated tenant header.
// Production ingress must strip user-supplied X-Company-ID and inject its own.
func (HeaderTenantResolver) CompanyID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(r.Header.Get("X-Company-ID"))
	if err != nil {
		return uuid.Nil, ErrUnauthenticated
	}
	return id, nil
}

type HTTPHandler struct {
	store   Store
	tenants TenantResolver
}

func NewHTTPHandler(store Store, tenants TenantResolver) *HTTPHandler {
	return &HTTPHandler{store: store, tenants: tenants}
}

func (h *HTTPHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /interviews/{id}/timeline", h.timeline)
	return mux
}

func (h *HTTPHandler) timeline(w http.ResponseWriter, r *http.Request) {
	companyID, err := h.tenants.CompanyID(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized", "authenticated company is required")
		return
	}
	interviewID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_interview_id", "id must be a UUID")
		return
	}
	afterSeq, err := parseAfterSeq(r.URL.Query().Get("after_seq"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_after_seq", "after_seq must be an unsigned integer")
		return
	}
	events, err := h.store.Timeline(r.Context(), companyID, interviewID, afterSeq)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "timeline_unavailable", "timeline is temporarily unavailable")
		return
	}
	if events == nil {
		events = []Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func parseAfterSeq(raw string) (int64, error) {
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, errors.New("after_seq must be non-negative")
	}
	return value, nil
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"code": code, "message": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
