// Package aigateway implements the AI Gateway: a single internal endpoint
// that streams a model completion back as Server-Sent Events and always
// records the outcome as an ai.response.completed event.
//
// NOTE: [[AI Gateway – Internals]] was not available when this package was
// written; the request shape, statuses, and event payload below are
// reasonable placeholders to reconcile against it.
package aigateway

import (
	"context"
	"errors"
)

// Message is one conversation turn sent to the provider.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

const (
	RoleUser      = "user"
	RoleAssistant = "assistant"
)

// ProviderRequest is what a provider adapter receives. APIKey is empty in
// managed mode (the adapter uses its own credential) and set in BYOK mode.
type ProviderRequest struct {
	Model     string
	System    string
	Messages  []Message
	MaxTokens int64
	APIKey    *Key
}

// Usage reports token consumption.
type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// FinishReason is the provider-neutral reason a completion stopped.
type FinishReason string

const (
	// FinishStop: the model finished its turn.
	FinishStop FinishReason = "stop"
	// FinishLength: output was cut off by max_tokens or the context window.
	FinishLength FinishReason = "length"
	// FinishRefusal: the provider declined to answer.
	FinishRefusal FinishReason = "refusal"
)

// ProviderResult is the outcome of a completed provider stream.
type ProviderResult struct {
	Model        string
	FinishReason FinishReason
	// StopReason is the provider's raw stop reason, kept for diagnostics.
	StopReason string
	Usage      Usage
}

// Provider streams one completion. onDelta is called with each text
// fragment in order; an error from onDelta aborts the stream and is
// returned. Adapters must honour ctx cancellation promptly.
type Provider interface {
	Name() string
	Stream(ctx context.Context, request ProviderRequest, onDelta func(string) error) (ProviderResult, error)
}

// ProviderError is a provider failure with a stable, non-sensitive code
// that is safe to put in events and responses.
type ProviderError struct {
	Code string
	Err  error
}

func (e *ProviderError) Error() string { return "provider error " + e.Code + ": " + e.Err.Error() }
func (e *ProviderError) Unwrap() error { return e.Err }

// errorCode returns the stable code for err, defaulting to provider_error.
func errorCode(err error) string {
	var providerErr *ProviderError
	if errors.As(err, &providerErr) && providerErr.Code != "" {
		return providerErr.Code
	}
	return "provider_error"
}
