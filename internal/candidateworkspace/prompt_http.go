package candidateworkspace

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/sse"
)

// SSE events on POST /session/prompt: one prompt event once the prompt is
// recorded, delta events with model text, and exactly one done event.
const (
	PromptEventAccepted = "prompt"
	PromptEventDelta    = "delta"
	PromptEventDone     = "done"
)

// PromptDone is the data of the final done event.
type PromptDone struct {
	Status     string `json:"status"`
	StopReason string `json:"stopReason,omitempty"`
	ErrorCode  string `json:"errorCode,omitempty"`
}

func (h *HTTPHandler) submitPrompt(w http.ResponseWriter, r *http.Request) {
	session, _ := ActiveSessionFromContext(r.Context())
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var request PromptRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "prompt is required")
		return
	}

	var stream *sse.Writer
	outcome, err := h.prompts.Submit(r.Context(), session, request,
		func(accepted PromptAccepted) error {
			var err error
			if stream, err = sse.NewWriter(w); err != nil {
				return err
			}
			return stream.Send(PromptEventAccepted, accepted)
		},
		func(delta string) error {
			return stream.Send(PromptEventDelta, aigateway.DeltaEvent{Text: delta})
		})
	if stream == nil {
		switch {
		case errors.Is(err, ErrInvalidPrompt):
			writeError(w, http.StatusBadRequest, "invalid_prompt", err.Error())
		default:
			writeError(w, http.StatusServiceUnavailable, "prompt_failed", "could not submit prompt")
		}
		return
	}
	done := PromptDone{Status: outcome.Status, StopReason: outcome.StopReason, ErrorCode: outcome.ErrorCode}
	if err != nil {
		done = PromptDone{Status: "error", ErrorCode: "gateway_unavailable"}
	}
	// The client may be gone; there is nobody to report a failed write to.
	_ = stream.Send(PromptEventDone, done)
}
