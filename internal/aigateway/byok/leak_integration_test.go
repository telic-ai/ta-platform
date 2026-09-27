//go:build integration

package byok

import (
	"bytes"
	"context"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/kafka"
)

// TestBYOKKeyNeverReachesKafka runs BYOK completions through the real
// acks=all producer and scans the raw records on the topic.
func TestBYOKKeyNeverReachesKafka(t *testing.T) {
	const companyKey = "sk-ant-api03-KAFKA-LEAK-CANARY-424242"
	cfg, _ := config.Load("byok-kafka-integration-test")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := kafka.New(cfg.KafkaBrokers)
	topic := events.SessionEventsTopic + ".byok." + uuid.NewString()[:8]
	if err := client.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	writer := client.Writer(topic)
	defer writer.Close()
	emitter, err := aigateway.NewKafkaEmitter(writer)
	if err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	secrets, kmsFake := &fakeSecrets{}, newFakeKMS()
	resolver := NewResolver(secrets, kmsFake, Config{Logger: logger})
	companyID := uuid.NewString()
	sealed, _ := Seal(ctx, kmsFake, "alias/byok", companyID, []byte(companyKey))
	secrets.put(resolver.SecretName(companyID), sealed, "v1")

	api := &messagesAPI{respond: streamOK}
	apiServer := httptest.NewServer(api)
	defer apiServer.Close()
	gateway, err := aigateway.NewGateway(aigateway.NewAnthropicProvider(aigateway.AnthropicConfig{APIKey: "managed", BaseURL: apiServer.URL}),
		emitter, resolver, logger)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		request := aigateway.CompleteRequest{
			CompanyID: companyID, InterviewID: uuid.NewString(), SessionID: uuid.NewString(), PromptID: uuid.NewString(),
			SequenceNumber: 2, Mode: aigateway.ModeBYOK, Messages: []aigateway.Message{{Role: "user", Content: "hi"}},
		}
		if err := request.Validate(); err != nil {
			t.Fatal(err)
		}
		if _, err := gateway.Complete(ctx, request, func(string) error { return nil }); err != nil {
			t.Fatalf("Complete: %v", err)
		}
	}
	if api.keys[0] != companyKey {
		t.Fatalf("provider did not receive the company key")
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: strings.Split(cfg.KafkaBrokers, ","), Topic: topic, Partition: 0})
	defer reader.Close()
	var raw bytes.Buffer
	for i := 0; i < 3; i++ {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read record %d: %v", i, err)
		}
		raw.Write(message.Key)
		raw.Write(message.Value)
		for _, header := range message.Headers {
			raw.WriteString(header.Key)
			raw.Write(header.Value)
		}
	}
	for name, surface := range map[string]string{"kafka": raw.String(), "logs": logs.String()} {
		if strings.Contains(surface, "KAFKA-LEAK-CANARY") {
			t.Errorf("key material found in %s", name)
		}
	}
	if !strings.Contains(raw.String(), `"mode":"byok"`) || !strings.Contains(raw.String(), `"status":"completed"`) {
		t.Errorf("unexpected records: %s", raw.String())
	}
}
