package scoring

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics"
)

// Decisions an AI recommendation may make.
const (
	DecisionAdvance = "advance"
	DecisionHold    = "hold"
	DecisionReject  = "reject"
)

// Recommendation is the AI's advisory verdict on a session. It never
// replaces the metrics, which are stored whether or not it exists.
type Recommendation struct {
	Decision   string  `json:"decision"`
	Confidence float64 `json:"confidence"`
	Rationale  string  `json:"rationale"`
}

// Recommender produces a recommendation from a session's metrics.
type Recommender interface {
	Recommend(ctx context.Context, m metrics.Metrics) (Recommendation, string, error)
}

// Stable recommendation error codes, safe to store and emit.
const (
	RecommendationErrInvalidJSON = "invalid_json"
	RecommendationErrProvider    = "provider_error"
	RecommendationErrTimeout     = "timeout"
	RecommendationErrDisabled    = "disabled"
)

// RecommendationError carries a stable code for a failed recommendation.
type RecommendationError struct {
	Code string
	Err  error
}

func (e *RecommendationError) Error() string {
	return "recommendation " + e.Code + ": " + e.Err.Error()
}
func (e *RecommendationError) Unwrap() error { return e.Err }

// ParseRecommendation accepts exactly one JSON object with exactly the
// Recommendation fields, a known decision, a confidence in [0, 1] and a
// non-empty rationale. Anything else — prose around the JSON, code fences,
// extra or missing fields — is rejected rather than repaired.
func ParseRecommendation(text string) (Recommendation, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.DisallowUnknownFields()
	var fields struct {
		Decision   *string  `json:"decision"`
		Confidence *float64 `json:"confidence"`
		Rationale  *string  `json:"rationale"`
	}
	if err := decoder.Decode(&fields); err != nil {
		return Recommendation{}, fmt.Errorf("not a recommendation object: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Recommendation{}, errors.New("unexpected content after the JSON object")
	}
	if fields.Decision == nil || fields.Confidence == nil || fields.Rationale == nil {
		return Recommendation{}, errors.New("decision, confidence and rationale are required")
	}
	switch *fields.Decision {
	case DecisionAdvance, DecisionHold, DecisionReject:
	default:
		return Recommendation{}, fmt.Errorf("unknown decision %q", *fields.Decision)
	}
	if *fields.Confidence < 0 || *fields.Confidence > 1 {
		return Recommendation{}, errors.New("confidence must be between 0 and 1")
	}
	if strings.TrimSpace(*fields.Rationale) == "" {
		return Recommendation{}, errors.New("rationale must not be empty")
	}
	return Recommendation{Decision: *fields.Decision, Confidence: *fields.Confidence, Rationale: *fields.Rationale}, nil
}

const recommendationSystem = `You review metrics from a candidate's timed coding session and recommend whether to advance them.
Reply with exactly one JSON object and nothing else — no prose, no code fences:
{"decision": "advance" | "hold" | "reject", "confidence": <number from 0 to 1>, "rationale": "<one or two sentences>"}
AI assistance is allowed in the session; judge how effectively it was used, not whether it was used.`

// ProviderRecommender asks a model directly through an AI Gateway provider
// adapter. It deliberately bypasses the gateway's HTTP endpoint, which
// records every completion as an ai.response.completed in the candidate's
// own session timeline.
type ProviderRecommender struct {
	Provider  aigateway.Provider
	Model     string
	MaxTokens int64
}

// Recommend returns the recommendation and the model that produced it.
func (r ProviderRecommender) Recommend(ctx context.Context, m metrics.Metrics) (Recommendation, string, error) {
	input, err := json.Marshal(struct {
		metrics.Metrics
		RunSuccessRate float64 `json:"run_success_rate"`
		AILineShare    float64 `json:"ai_line_share"`
	}{m, m.RunSuccessRate(), m.AILineShare()})
	if err != nil {
		return Recommendation{}, "", err
	}
	model, maxTokens := r.Model, r.MaxTokens
	if model == "" {
		model = aigateway.DefaultModel
	}
	if maxTokens == 0 {
		maxTokens = 1024
	}
	var text bytes.Buffer
	result, err := r.Provider.Stream(ctx, aigateway.ProviderRequest{
		Model: model, System: recommendationSystem, MaxTokens: maxTokens,
		Messages: []aigateway.Message{{Role: aigateway.RoleUser, Content: "Session metrics:\n" + string(input)}},
	}, func(delta string) error {
		text.WriteString(delta)
		return nil
	})
	if result.Model != "" {
		model = result.Model
	}
	if err != nil {
		if ctx.Err() != nil {
			return Recommendation{}, model, &RecommendationError{Code: RecommendationErrTimeout, Err: err}
		}
		return Recommendation{}, model, &RecommendationError{Code: RecommendationErrProvider, Err: err}
	}
	recommendation, err := ParseRecommendation(text.String())
	if err != nil {
		return Recommendation{}, model, &RecommendationError{Code: RecommendationErrInvalidJSON, Err: err}
	}
	return recommendation, model, nil
}

// recommendationErrorCode maps any recommender error to a stable code.
func recommendationErrorCode(ctx context.Context, err error) string {
	var recErr *RecommendationError
	if errors.As(err, &recErr) && recErr.Code != "" {
		return recErr.Code
	}
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		return RecommendationErrTimeout
	}
	return RecommendationErrProvider
}
