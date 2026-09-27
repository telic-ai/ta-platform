package candidateworkspace

import (
	"encoding/json"
	"errors"
	"net/http"
)

func (h *HTTPHandler) submitDiff(w http.ResponseWriter, r *http.Request) {
	session, _ := ActiveSessionFromContext(r.Context())
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var request DiffRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "body must be a diff")
		return
	}
	result, err := h.diffs.Submit(r.Context(), session, request)
	switch {
	case err == nil && result.Accepted:
		writeJSON(w, http.StatusAccepted, result)
	case err == nil:
		// Dropped as out of order: not an error, and not worth retrying.
		writeJSON(w, http.StatusOK, result)
	case errors.Is(err, ErrInvalidDiff):
		writeError(w, http.StatusBadRequest, "invalid_diff", err.Error())
	default:
		writeError(w, http.StatusServiceUnavailable, "diff_failed", "could not record diff")
	}
}
