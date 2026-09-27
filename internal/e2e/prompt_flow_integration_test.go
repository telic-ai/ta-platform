//go:build integration

package e2e

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/candidateworkspace"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/testutil/pgtest"
)

type cannedProvider struct{}

func (cannedProvider) Name() string { return "canned" }
func (cannedProvider) Stream(_ context.Context, _ aigateway.ProviderRequest, onDelta func(string) error) (aigateway.ProviderResult, error) {
	for _, d := range []string{"use ", "a map"} {
		if err := onDelta(d); err != nil {
			return aigateway.ProviderResult{}, err
		}
	}
	return aigateway.ProviderResult{FinishReason: aigateway.FinishStop, StopReason: "end_turn"}, nil
}

// TestPromptSubmittedPrecedesCompletion drives POST /session/prompt through
// the Postgres outbox and the real AI Gateway (acks=all producer) and
// checks both events arrive in Kafka ordered by sequence_number.
func TestPromptSubmittedPrecedesCompletion(t *testing.T) {
	cfg, err := config.Load("prompt-flow-e2e-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := pgtest.MigratedSchema(t, ctx, cfg.PostgresDSN)
	fixture := seedCandidateSession(t, ctx, pool)
	kafkaClient := kafka.New(cfg.KafkaBrokers)
	topic := isolatedTopic(t, ctx, kafkaClient, "prompt")

	completionWriter := kafkaClient.Writer(topic)
	defer completionWriter.Close()
	emitter, err := aigateway.NewKafkaEmitter(completionWriter)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := aigateway.NewGateway(cannedProvider{}, emitter, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	gatewayAPI := httptest.NewServer(aigateway.NewHTTPHandler(gateway).Routes())
	defer gatewayAPI.Close()

	sessions := postgres.NewSessionStore(pool)
	prompts := candidateworkspace.NewPromptService(postgres.NewEventStore(pool),
		candidateworkspace.NewHTTPGateway(gatewayAPI.URL, nil), candidateworkspace.PromptConfig{})
	workspace := httptest.NewServer(candidateworkspace.NewHTTPHandler(nil, sessions).WithPrompts(prompts).Routes())
	defer workspace.Close()

	request, _ := http.NewRequest(http.MethodPost, workspace.URL+"/session/prompt", strings.NewReader(`{"prompt":"how do I dedupe?"}`))
	request.Header.Set("Authorization", "Bearer "+fixture.Token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"status":"completed"`) {
		t.Fatalf("status %d body %s", response.StatusCode, body)
	}

	relayOutboxTo(t, ctx, pool, kafkaClient, topic)
	envelopes := readEnvelopes(t, ctx, strings.Split(cfg.KafkaBrokers, ","), topic, 2)
	sequences := map[events.EventType]int64{}
	var prompt events.PromptSubmitted
	var completion events.AIResponseCompleted
	for _, envelope := range envelopes {
		sequences[envelope.EventType] = envelope.SequenceNumber
		switch envelope.EventType {
		case events.EventTypePromptSubmitted:
			_ = envelope.Decode(&prompt)
		case events.EventTypeAIResponseCompleted:
			_ = envelope.Decode(&completion)
		}
	}
	if sequences[events.EventTypePromptSubmitted] != 1 || sequences[events.EventTypeAIResponseCompleted] != 2 {
		t.Fatalf("sequences = %v, want prompt.submitted=1 < ai.response.completed=2", sequences)
	}
	if prompt.PromptID == "" || completion.PromptID != prompt.PromptID || completion.ResponseText != "use a map" {
		t.Errorf("prompt %+v completion %+v", prompt, completion)
	}
	var last int64
	if err := pool.QueryRow(ctx, `SELECT last_sequence_number FROM interviews WHERE id = $1`, fixture.InterviewID).Scan(&last); err != nil || last != 2 {
		t.Errorf("last_sequence_number = %d, %v; want 2", last, err)
	}
}
