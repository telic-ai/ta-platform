package aigateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// DefaultModel is used when a request does not name one.
const DefaultModel = "claude-opus-5"

// AnthropicConfig configures the Claude adapter.
type AnthropicConfig struct {
	// APIKey is the platform's managed-mode credential. BYOK requests
	// override it per call.
	APIKey string
	// BaseURL overrides the API endpoint (tests, proxies).
	BaseURL    string
	HTTPClient *http.Client
	// MaxRetries bounds SDK retries before the first byte streams.
	MaxRetries int
}

// AnthropicProvider streams completions from the Claude Messages API.
type AnthropicProvider struct {
	client anthropic.Client
}

func NewAnthropicProvider(config AnthropicConfig) *AnthropicProvider {
	opts := []option.RequestOption{option.WithMaxRetries(config.MaxRetries)}
	if config.APIKey != "" {
		opts = append(opts, option.WithAPIKey(config.APIKey))
	}
	if config.BaseURL != "" {
		opts = append(opts, option.WithBaseURL(config.BaseURL))
	}
	if config.HTTPClient != nil {
		opts = append(opts, option.WithHTTPClient(config.HTTPClient))
	}
	return &AnthropicProvider{client: anthropic.NewClient(opts...)}
}

func (*AnthropicProvider) Name() string { return "anthropic" }

// Stream calls the Messages API with streaming and relays text deltas.
// Refusals are re-served by the server-side fallback model where possible.
func (p *AnthropicProvider) Stream(ctx context.Context, request ProviderRequest, onDelta func(string) error) (ProviderResult, error) {
	params := anthropic.BetaMessageNewParams{
		Model:     anthropic.Model(request.Model),
		MaxTokens: request.MaxTokens,
		Messages:  make([]anthropic.BetaMessageParam, 0, len(request.Messages)),
		Betas:     []anthropic.AnthropicBeta{anthropic.AnthropicBetaServerSideFallback2026_07_01},
		Fallbacks: anthropic.BetaFallbacksParamOfDefault(),
	}
	if request.System != "" {
		params.System = []anthropic.BetaTextBlockParam{{Text: request.System}}
	}
	for _, message := range request.Messages {
		role := anthropic.BetaMessageParamRoleUser
		if message.Role == RoleAssistant {
			role = anthropic.BetaMessageParamRoleAssistant
		}
		params.Messages = append(params.Messages, anthropic.BetaMessageParam{
			Role: role, Content: []anthropic.BetaContentBlockParamUnion{anthropic.NewBetaTextBlock(message.Content)},
		})
	}

	var opts []option.RequestOption
	if request.APIKey != nil {
		opts = append(opts, option.WithAPIKey(request.APIKey.Reveal()))
	}

	stream := p.client.Beta.Messages.NewStreaming(ctx, params, opts...)
	defer stream.Close()

	result := ProviderResult{Model: request.Model}
	for stream.Next() {
		switch event := stream.Current().AsAny().(type) {
		case anthropic.BetaRawMessageStartEvent:
			if event.Message.Model != "" {
				result.Model = string(event.Message.Model)
			}
			result.Usage.InputTokens = event.Message.Usage.InputTokens
		case anthropic.BetaRawContentBlockDeltaEvent:
			if text, ok := event.Delta.AsAny().(anthropic.BetaTextDelta); ok && text.Text != "" {
				if err := onDelta(text.Text); err != nil {
					return result, err
				}
			}
		case anthropic.BetaRawMessageDeltaEvent:
			result.StopReason = string(event.Delta.StopReason)
			if event.Usage.InputTokens > 0 {
				result.Usage.InputTokens = event.Usage.InputTokens
			}
			result.Usage.OutputTokens = event.Usage.OutputTokens
		}
	}
	if err := stream.Err(); err != nil {
		return result, classifyAnthropicError(ctx, err)
	}
	if result.StopReason == "" {
		return result, &ProviderError{Code: "incomplete_stream", Err: errors.New("stream ended without a stop reason")}
	}
	result.FinishReason = finishReason(result.StopReason)
	return result, nil
}

func finishReason(stopReason string) FinishReason {
	switch anthropic.BetaStopReason(stopReason) {
	case anthropic.BetaStopReasonMaxTokens, anthropic.BetaStopReasonModelContextWindowExceeded:
		return FinishLength
	case anthropic.BetaStopReasonRefusal:
		return FinishRefusal
	default:
		return FinishStop
	}
}

// classifyAnthropicError maps SDK errors to stable codes. The SDK's error
// text includes the request URL and response body, never the API key
// header, but only the code leaves the gateway.
func classifyAnthropicError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var apiErr *anthropic.Error
	if errors.As(err, &apiErr) {
		code := "provider_error"
		switch {
		case apiErr.StatusCode == http.StatusUnauthorized || apiErr.StatusCode == http.StatusForbidden:
			code = "provider_auth"
		case apiErr.StatusCode == http.StatusTooManyRequests:
			code = "provider_rate_limited"
		case apiErr.StatusCode == http.StatusBadRequest || apiErr.StatusCode == http.StatusNotFound:
			code = "provider_invalid_request"
		case apiErr.StatusCode >= 500:
			code = "provider_unavailable"
		}
		return &ProviderError{Code: code, Err: fmt.Errorf("status %d", apiErr.StatusCode)}
	}
	return &ProviderError{Code: "provider_unavailable", Err: err}
}
