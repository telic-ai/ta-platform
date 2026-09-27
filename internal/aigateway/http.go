package aigateway

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/sse"
)

// SSE event names on POST /v1/complete. Every stream ends with exactly one
// done event carrying the Outcome.
const (
	EventDelta = "delta"
	EventDone  = "done"
)

// DeltaEvent is the data of a delta event.
type DeltaEvent struct {
	Text string `json:"text"`
}

type HTTPHandler struct {
	gateway    *Gateway
	invalidate func(companyID string)
}

func NewHTTPHandler(gateway *Gateway) *HTTPHandler { return &HTTPHandler{gateway: gateway} }

// WithKeyInvalidation enables POST /v1/byok/invalidate, which a key
// rotation notification (or an operator) calls to drop a company's cached
// BYOK key immediately.
func (h *HTTPHandler) WithKeyInvalidation(invalidate func(companyID string)) *HTTPHandler {
	h.invalidate = invalidate
	return h
}

func (h *HTTPHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/complete", h.complete)
	if h.invalidate != nil {
		mux.HandleFunc("POST /v1/byok/invalidate", h.invalidateKey)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

func (h *HTTPHandler) complete(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4<<20)
	var request CompleteRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be a JSON completion request")
		return
	}
	if err := request.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	stream, err := sse.NewWriter(w)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "streaming_unsupported", "streaming is not supported")
		return
	}
	// A failed write means the client disconnected; the request context is
	// cancelled too, which stops the provider call.
	outcome, _ := h.gateway.Complete(r.Context(), request, func(delta string) error {
		if err := stream.Send(EventDelta, DeltaEvent{Text: delta}); err != nil {
			return fmt.Errorf("%w: %v", errClientGone, err)
		}
		return nil
	})
	_ = stream.Send(EventDone, outcome)
}

func (h *HTTPHandler) invalidateKey(w http.ResponseWriter, r *http.Request) {
	var request struct {
		CompanyID string `json:"company_id"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || uuid.Validate(request.CompanyID) != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "company_id must be a UUID")
		return
	}
	h.invalidate(request.CompanyID)
	w.WriteHeader(http.StatusNoContent)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": message})
}
