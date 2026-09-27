package candidateworkspace

import (
	"encoding/json"
	"errors"
	"net/http"
)

func (h *HTTPHandler) runCode(w http.ResponseWriter, r *http.Request) {
	session, _ := ActiveSessionFromContext(r.Context())
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	var request RunRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "body must be a run request")
		return
	}
	result, err := h.runs.Run(r.Context(), session, request)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, result)
	case errors.Is(err, ErrRunInProgress):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "run_in_progress", err.Error())
	case errors.Is(err, ErrInvalidRun):
		writeError(w, http.StatusBadRequest, "invalid_run", err.Error())
	default:
		writeError(w, http.StatusServiceUnavailable, "run_failed", "could not run code")
	}
}
