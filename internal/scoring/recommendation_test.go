package scoring

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/scoring/metrics"
)

func TestParseRecommendationAcceptsStrictObject(t *testing.T) {
	got, err := ParseRecommendation(` {"decision":"advance","confidence":0.8,"rationale":"Fixed the failing run quickly."} `)
	if err != nil {
		t.Fatal(err)
	}
	if got != (Recommendation{Decision: "advance", Confidence: 0.8, Rationale: "Fixed the failing run quickly."}) {
		t.Fatalf("got %+v", got)
	}
}

func TestParseRecommendationRejectsAnythingElse(t *testing.T) {
	for name, text := range map[string]string{
		"empty":            "",
		"prose before":     `Sure! {"decision":"hold","confidence":0.5,"rationale":"x"}`,
		"trailing prose":   `{"decision":"hold","confidence":0.5,"rationale":"x"} hope that helps`,
		"two objects":      `{"decision":"hold","confidence":0.5,"rationale":"x"}{"decision":"hold","confidence":0.5,"rationale":"x"}`,
		"code fence":       "```json\n{\"decision\":\"hold\",\"confidence\":0.5,\"rationale\":\"x\"}\n```",
		"unknown field":    `{"decision":"hold","confidence":0.5,"rationale":"x","score":9}`,
		"missing field":    `{"decision":"hold","confidence":0.5}`,
		"bad decision":     `{"decision":"hire","confidence":0.5,"rationale":"x"}`,
		"confidence > 1":   `{"decision":"hold","confidence":1.5,"rationale":"x"}`,
		"confidence < 0":   `{"decision":"hold","confidence":-0.1,"rationale":"x"}`,
		"string conf":      `{"decision":"hold","confidence":"high","rationale":"x"}`,
		"blank rationale":  `{"decision":"hold","confidence":0.5,"rationale":"  "}`,
		"array":            `[{"decision":"hold","confidence":0.5,"rationale":"x"}]`,
		"null decision":    `{"decision":null,"confidence":0.5,"rationale":"x"}`,
		"truncated output": `{"decision":"hold","confidence":0.5,"rati`,
	} {
		if _, err := ParseRecommendation(text); err == nil {
			t.Errorf("%s: accepted %q", name, text)
		}
	}
}

func TestProviderRecommenderParsesStreamedJSON(t *testing.T) {
	provider := &fakeProvider{deltas: []string{`{"decision":"reject",`, `"confidence":0.9,"rationale":"never ran the code"}`}, model: "served-model"}
	recommender := ProviderRecommender{Provider: provider, Model: "requested-model"}
	got, model, err := recommender.Recommend(context.Background(), metrics.Metrics{LinesAdded: 10, AILinesAdded: 5})
	if err != nil || got.Decision != DecisionReject || model != "served-model" {
		t.Fatalf("Recommend = %+v, %q, %v", got, model, err)
	}
	if provider.request.Model != "requested-model" || provider.request.System == "" {
		t.Fatalf("request = %+v", provider.request)
	}
	if !strings.Contains(provider.request.Messages[0].Content, `"ai_line_share":0.5`) {
		t.Fatalf("derived ratios not sent: %s", provider.request.Messages[0].Content)
	}
}

func TestProviderRecommenderErrorCodes(t *testing.T) {
	cases := map[string]struct {
		provider *fakeProvider
		want     string
	}{
		"invalid json": {&fakeProvider{deltas: []string{"I think they should advance."}}, RecommendationErrInvalidJSON},
		"provider":     {&fakeProvider{err: errors.New("overloaded")}, RecommendationErrProvider},
	}
	for name, tc := range cases {
		_, _, err := ProviderRecommender{Provider: tc.provider}.Recommend(context.Background(), metrics.Metrics{})
		var recErr *RecommendationError
		if !errors.As(err, &recErr) || recErr.Code != tc.want {
			t.Errorf("%s: err = %v, want code %s", name, err, tc.want)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := ProviderRecommender{Provider: &fakeProvider{err: context.Canceled}}.Recommend(ctx, metrics.Metrics{})
	if code := recommendationErrorCode(ctx, err); code != RecommendationErrTimeout {
		t.Errorf("cancelled call code = %s", code)
	}
}

func TestProviderRecommenderDefaults(t *testing.T) {
	provider := &fakeProvider{deltas: []string{`{"decision":"hold","confidence":0,"rationale":"r"}`}}
	_, model, err := ProviderRecommender{Provider: provider}.Recommend(context.Background(), metrics.Metrics{})
	if err != nil || model != aigateway.DefaultModel || provider.request.MaxTokens != 1024 {
		t.Fatalf("model=%q maxTokens=%d err=%v", model, provider.request.MaxTokens, err)
	}
}

type fakeProvider struct {
	deltas  []string
	model   string
	err     error
	request aigateway.ProviderRequest
}

func (p *fakeProvider) Name() string { return "fake" }

func (p *fakeProvider) Stream(_ context.Context, request aigateway.ProviderRequest, onDelta func(string) error) (aigateway.ProviderResult, error) {
	p.request = request
	if p.err != nil {
		return aigateway.ProviderResult{}, p.err
	}
	for _, delta := range p.deltas {
		if err := onDelta(delta); err != nil {
			return aigateway.ProviderResult{}, err
		}
	}
	return aigateway.ProviderResult{Model: p.model, FinishReason: aigateway.FinishStop}, nil
}
