package byok

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/events"
)

// recordingKeys wraps the resolver to check the gateway zeroes each key.
type recordingKeys struct {
	inner  *Resolver
	mu     sync.Mutex
	issued []*aigateway.Key
}

func (r *recordingKeys) ResolveKey(ctx context.Context, companyID string) (*aigateway.Key, error) {
	key, err := r.inner.ResolveKey(ctx, companyID)
	if key != nil {
		r.mu.Lock()
		r.issued = append(r.issued, key)
		r.mu.Unlock()
	}
	return key, err
}

// serializingEmitter records each envelope exactly as KafkaEmitter would
// write it to the topic.
type serializingEmitter struct {
	mu      sync.Mutex
	records [][]byte
}

func (e *serializingEmitter) Emit(_ context.Context, key string, envelope events.Envelope) error {
	value, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records = append(e.records, append([]byte(key+"|"+string(envelope.EventType)+"|"), value...))
	return nil
}

// messagesAPI is a fake Claude Messages API that records the key it was
// called with and replays a scripted outcome.
type messagesAPI struct {
	mu      sync.Mutex
	keys    []string
	respond func(w http.ResponseWriter)
}

func (m *messagesAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.keys = append(m.keys, r.Header.Get("X-Api-Key"))
	respond := m.respond
	m.mu.Unlock()
	_, _ = io.Copy(io.Discard, r.Body)
	respond(w)
}

func streamOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	for _, event := range []string{
		`{"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"claude-opus-5","content":[],"usage":{"input_tokens":5,"output_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":2}}`,
		`{"type":"message_stop"}`,
	} {
		var typed struct{ Type string }
		_ = json.Unmarshal([]byte(event), &typed)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", typed.Type, event)
	}
}

func TestBYOKKeyNeverReachesLogsKafkaOrClient(t *testing.T) {
	const (
		firstKey  = "sk-ant-api03-BYOK-FIRST-KEY-0123456789"
		secondKey = "sk-ant-api03-BYOK-ROTATED-KEY-9876543210"
		managed   = "sk-ant-MANAGED-PLATFORM-KEY"
	)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	fakeSecrets, kmsFake := &fakeSecrets{}, newFakeKMS()
	resolver := NewResolver(fakeSecrets, kmsFake, Config{Logger: logger})
	companyID := uuid.NewString()
	storeKey := func(key, version string) {
		sealed, err := Seal(context.Background(), kmsFake, "alias/byok", companyID, []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		fakeSecrets.put(resolver.SecretName(companyID), sealed, version)
	}
	storeKey(firstKey, "v1")

	api := &messagesAPI{respond: streamOK}
	apiServer := httptest.NewServer(api)
	defer apiServer.Close()
	provider := aigateway.NewAnthropicProvider(aigateway.AnthropicConfig{APIKey: managed, BaseURL: apiServer.URL})
	emitter := &serializingEmitter{}
	keys := &recordingKeys{inner: resolver}
	gateway, err := aigateway.NewGateway(provider, emitter, keys, logger)
	if err != nil {
		t.Fatal(err)
	}
	gatewayServer := httptest.NewServer(aigateway.NewHTTPHandler(gateway).Routes())
	defer gatewayServer.Close()

	var clientBytes bytes.Buffer
	complete := func() {
		body, _ := json.Marshal(aigateway.CompleteRequest{
			CompanyID: companyID, InterviewID: uuid.NewString(), SessionID: uuid.NewString(),
			PromptID: uuid.NewString(), SequenceNumber: 2, Mode: aigateway.ModeBYOK,
			Messages: []aigateway.Message{{Role: aigateway.RoleUser, Content: "hi"}},
		})
		response, err := http.Post(gatewayServer.URL+"/v1/complete", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		_, _ = io.Copy(&clientBytes, response.Body)
	}

	complete()                                  // success
	api.respond = func(w http.ResponseWriter) { // provider rejects the key
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`)
	}
	complete()
	api.respond = func(w http.ResponseWriter) { // error mid-stream
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n")
	}
	complete()
	api.respond = streamOK
	storeKey(secondKey, "v2") // rotation
	complete()

	// The company's key, not the platform's, went to the provider, and the
	// rotated key was used on the very next call.
	api.mu.Lock()
	sent := append([]string{}, api.keys...)
	api.mu.Unlock()
	if len(sent) < 4 || sent[0] != firstKey || sent[len(sent)-1] != secondKey {
		t.Fatalf("provider saw keys %q", sent)
	}
	for _, key := range sent {
		if key == managed {
			t.Fatal("BYOK request used the managed key")
		}
	}

	emitter.mu.Lock()
	kafka := bytes.Join(emitter.records, []byte("\n"))
	statuses := len(emitter.records)
	emitter.mu.Unlock()
	if statuses != 4 {
		t.Fatalf("emitted %d completion events, want 4", statuses)
	}
	for name, surface := range map[string]string{"logs": logs.String(), "kafka": string(kafka), "client": clientBytes.String()} {
		for _, secret := range []string{firstKey, secondKey, "BYOK-FIRST", "BYOK-ROTATED"} {
			if strings.Contains(surface, secret) {
				t.Errorf("%s contains key material %q:\n%s", name, secret, surface)
			}
		}
	}
	for _, want := range []string{`"status":"completed"`, `"error_code":"provider_auth"`, `"mode":"byok"`} {
		if !strings.Contains(string(kafka), want) {
			t.Errorf("kafka records missing %s", want)
		}
	}

	keys.mu.Lock()
	defer keys.mu.Unlock()
	for i, key := range keys.issued {
		if !key.Zeroed() {
			t.Errorf("key copy %d still holds material after its completion", i)
		}
	}
}
