package aigateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/events"
)

// Modes select whose credential pays for a completion.
const (
	ModeManaged = "managed"
	ModeBYOK    = "byok"
)

const (
	defaultMaxTokens = 16000
	maxMaxTokens     = 64000
	// emitTimeout bounds the completion event write, which runs even after
	// the caller has gone away.
	emitTimeout = 15 * time.Second
)

// CompleteRequest is the body of POST /v1/complete. The caller (the
// Candidate Workspace) allocates SequenceNumber for the completion event so
// it orders after the prompt that caused it.
type CompleteRequest struct {
	CompanyID      string    `json:"company_id"`
	InterviewID    string    `json:"interview_id"`
	SessionID      string    `json:"session_id"`
	PromptID       string    `json:"prompt_id"`
	SequenceNumber int64     `json:"sequence_number"`
	Mode           string    `json:"mode,omitempty"`
	Model          string    `json:"model,omitempty"`
	System         string    `json:"system,omitempty"`
	Messages       []Message `json:"messages"`
	MaxTokens      int64     `json:"max_tokens,omitempty"`
}

// Validate checks the request and fills defaults.
func (r *CompleteRequest) Validate() error {
	for name, value := range map[string]string{
		"company_id": r.CompanyID, "interview_id": r.InterviewID,
		"session_id": r.SessionID, "prompt_id": r.PromptID,
	} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("%s must be a UUID", name)
		}
	}
	if r.SequenceNumber < 1 {
		return errors.New("sequence_number must be positive")
	}
	if r.Mode == "" {
		r.Mode = ModeManaged
	}
	if r.Mode != ModeManaged && r.Mode != ModeBYOK {
		return errors.New("mode must be managed or byok")
	}
	if len(r.Messages) == 0 {
		return errors.New("messages must not be empty")
	}
	for i, message := range r.Messages {
		if message.Role != RoleUser && message.Role != RoleAssistant {
			return fmt.Errorf("messages[%d].role must be user or assistant", i)
		}
		if strings.TrimSpace(message.Content) == "" {
			return fmt.Errorf("messages[%d].content must not be empty", i)
		}
	}
	if r.Messages[len(r.Messages)-1].Role != RoleUser {
		return errors.New("the last message must be from the user")
	}
	if r.MaxTokens == 0 {
		r.MaxTokens = defaultMaxTokens
	}
	if r.MaxTokens < 1 || r.MaxTokens > maxMaxTokens {
		return fmt.Errorf("max_tokens must be between 1 and %d", maxMaxTokens)
	}
	if r.Model == "" {
		r.Model = DefaultModel
	}
	return nil
}

// Outcome summarizes a completion for the caller's final SSE event.
type Outcome struct {
	Status     string `json:"status"`
	StopReason string `json:"stop_reason,omitempty"`
	ErrorCode  string `json:"error_code,omitempty"`
	Model      string `json:"model"`
	Usage      Usage  `json:"usage"`
}

// KeyResolver returns the company's own provider key for BYOK requests.
type KeyResolver interface {
	ResolveKey(ctx context.Context, companyID string) (*Key, error)
}

// Gateway runs completions and records each one.
type Gateway struct {
	provider Provider
	emitter  Emitter
	keys     KeyResolver
	logger   *slog.Logger
	now      func() time.Time
}

// NewGateway builds a gateway. keys may be nil, which disables BYOK.
func NewGateway(provider Provider, emitter Emitter, keys KeyResolver, logger *slog.Logger) (*Gateway, error) {
	if provider == nil || emitter == nil {
		return nil, errors.New("aigateway: provider and emitter are required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Gateway{provider: provider, emitter: emitter, keys: keys, logger: logger, now: time.Now}, nil
}

// Complete streams the completion to onDelta and then emits
// ai.response.completed whatever the outcome: success, truncation, refusal,
// provider error, or cancellation by the caller. The returned error is
// non-nil only if that event could not be written.
func (g *Gateway) Complete(ctx context.Context, request CompleteRequest, onDelta func(string) error) (Outcome, error) {
	started := g.now()
	var text strings.Builder
	outcome := Outcome{Model: request.Model}

	providerRequest := ProviderRequest{
		Model: request.Model, System: request.System,
		Messages: request.Messages, MaxTokens: request.MaxTokens,
	}
	var err error
	if request.Mode == ModeBYOK {
		providerRequest.APIKey, err = g.resolveKey(ctx, request.CompanyID)
	}
	var result ProviderResult
	if err == nil {
		result, err = g.provider.Stream(ctx, providerRequest, func(delta string) error {
			text.WriteString(delta)
			return onDelta(delta)
		})
	}
	outcome.Usage = result.Usage
	if result.Model != "" {
		outcome.Model = result.Model
	}
	outcome.StopReason = result.StopReason

	switch {
	case err == nil && result.FinishReason == FinishLength:
		outcome.Status = events.AIResponseStatusTruncated
	case err == nil && result.FinishReason == FinishRefusal:
		outcome.Status = events.AIResponseStatusRefused
	case err == nil:
		outcome.Status = events.AIResponseStatusCompleted
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, errClientGone):
		outcome.Status, outcome.ErrorCode = events.AIResponseStatusCancelled, "cancelled"
	default:
		outcome.Status, outcome.ErrorCode = events.AIResponseStatusError, errorCode(err)
		g.logger.Warn("ai completion failed",
			slog.String("prompt_id", request.PromptID), slog.String("error_code", outcome.ErrorCode))
	}

	envelope, buildErr := events.New(request.CompanyID, request.SequenceNumber, events.AIResponseCompleted{
		SessionID: request.SessionID, InterviewID: request.InterviewID, PromptID: request.PromptID,
		Mode: request.Mode, Provider: g.provider.Name(), Model: outcome.Model,
		Status: outcome.Status, StopReason: outcome.StopReason, ErrorCode: outcome.ErrorCode,
		ResponseText: text.String(),
		InputTokens:  outcome.Usage.InputTokens, OutputTokens: outcome.Usage.OutputTokens,
		LatencyMS: g.now().Sub(started).Milliseconds(),
	})
	if buildErr != nil {
		return outcome, buildErr
	}
	// The caller may have disconnected; the event must still be written.
	emitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), emitTimeout)
	defer cancel()
	if err := g.emitter.Emit(emitCtx, request.SessionID, envelope); err != nil {
		g.logger.Error("emit ai.response.completed",
			slog.String("prompt_id", request.PromptID), slog.Any("error", err))
		return outcome, err
	}
	return outcome, nil
}

func (g *Gateway) resolveKey(ctx context.Context, companyID string) (*Key, error) {
	if g.keys == nil {
		return nil, &ProviderError{Code: "byok_unavailable", Err: errors.New("BYOK is not configured")}
	}
	key, err := g.keys.ResolveKey(ctx, companyID)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &ProviderError{Code: "byok_key_unavailable", Err: err}
	}
	return key, nil
}

// errClientGone marks a delta that could not be delivered to the caller.
var errClientGone = errors.New("aigateway: client went away")
