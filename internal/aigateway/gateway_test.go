package aigateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/sse"
)

// scriptedProvider streams fixed deltas, then returns result/err.
type scriptedProvider struct {
	deltas []string
	result ProviderResult
	err    error
	// block, when set, makes Stream wait for ctx cancellation after the
	// deltas, like a slow upstream.
	block   bool
	started chan struct{}
	gotKey  string
}

func (p *scriptedProvider) Name() string { return "scripted" }

func (p *scriptedProvider) Stream(ctx context.Context, request ProviderRequest, onDelta func(string) error) (ProviderResult, error) {
	if request.APIKey != nil {
		p.gotKey = request.APIKey.Reveal()
	}
	for _, delta := range p.deltas {
		if err := onDelta(delta); err != nil {
			return ProviderResult{}, err
		}
	}
	if p.started != nil {
		close(p.started)
	}
	if p.block {
		<-ctx.Done()
		return ProviderResult{}, ctx.Err()
	}
	return p.result, p.err
}

type recordingEmitter struct {
	mu        sync.Mutex
	keys      []string
	envelopes []events.Envelope
	ctxErr    error
	err       error
}

func (e *recordingEmitter) Emit(ctx context.Context, key string, envelope events.Envelope) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.keys = append(e.keys, key)
	e.envelopes = append(e.envelopes, envelope)
	e.ctxErr = ctx.Err()
	return e.err
}

func (e *recordingEmitter) only(t *testing.T) (string, events.AIResponseCompleted, events.Envelope) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.envelopes) != 1 {
		t.Fatalf("emitted %d events, want exactly 1", len(e.envelopes))
	}
	var payload events.AIResponseCompleted
	if err := e.envelopes[0].Decode(&payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return e.keys[0], payload, e.envelopes[0]
}

func validRequest() CompleteRequest {
	return CompleteRequest{
		CompanyID: uuid.NewString(), InterviewID: uuid.NewString(),
		SessionID: uuid.NewString(), PromptID: uuid.NewString(),
		SequenceNumber: 8,
		Messages:       []Message{{Role: RoleUser, Content: "write fizzbuzz"}},
	}
}

func newTestGateway(t *testing.T, provider Provider, emitter Emitter) *Gateway {
	t.Helper()
	gateway, err := NewGateway(provider, emitter, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	return gateway
}

func TestCompleteEmitsForEveryOutcome(t *testing.T) {
	cases := []struct {
		name       string
		provider   *scriptedProvider
		wantStatus string
		wantCode   string
		wantText   string
	}{
		{"success", &scriptedProvider{deltas: []string{"a", "b"}, result: ProviderResult{FinishReason: FinishStop, StopReason: "end_turn", Usage: Usage{3, 2}}},
			events.AIResponseStatusCompleted, "", "ab"},
		{"truncation", &scriptedProvider{deltas: []string{"part"}, result: ProviderResult{FinishReason: FinishLength, StopReason: "max_tokens"}},
			events.AIResponseStatusTruncated, "", "part"},
		{"refusal", &scriptedProvider{result: ProviderResult{FinishReason: FinishRefusal, StopReason: "refusal"}},
			events.AIResponseStatusRefused, "", ""},
		{"provider error", &scriptedProvider{deltas: []string{"x"}, err: &ProviderError{Code: "provider_unavailable", Err: errors.New("503")}},
			events.AIResponseStatusError, "provider_unavailable", "x"},
		{"unclassified error", &scriptedProvider{err: errors.New("boom")},
			events.AIResponseStatusError, "provider_error", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			emitter := &recordingEmitter{}
			request := validRequest()
			if err := request.Validate(); err != nil {
				t.Fatal(err)
			}
			var relayed strings.Builder
			outcome, err := newTestGateway(t, tc.provider, emitter).Complete(context.Background(), request, func(d string) error {
				relayed.WriteString(d)
				return nil
			})
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if outcome.Status != tc.wantStatus || outcome.ErrorCode != tc.wantCode {
				t.Errorf("outcome = %+v, want status %q code %q", outcome, tc.wantStatus, tc.wantCode)
			}
			if relayed.String() != tc.wantText {
				t.Errorf("relayed %q, want %q", relayed.String(), tc.wantText)
			}
			key, payload, envelope := emitter.only(t)
			if key != request.SessionID {
				t.Errorf("event key = %q, want session id", key)
			}
			if envelope.SequenceNumber != 8 || envelope.CompanyID != request.CompanyID {
				t.Errorf("envelope = %+v", envelope)
			}
			if payload.Status != tc.wantStatus || payload.ErrorCode != tc.wantCode || payload.ResponseText != tc.wantText {
				t.Errorf("payload = %+v", payload)
			}
			if payload.PromptID != request.PromptID || payload.Mode != ModeManaged || payload.Provider != "scripted" {
				t.Errorf("payload identity = %+v", payload)
			}
		})
	}
}

func TestCompleteEmitsWhenCallerCancels(t *testing.T) {
	provider := &scriptedProvider{deltas: []string{"partial"}, block: true, started: make(chan struct{})}
	emitter := &recordingEmitter{}
	request := validRequest()
	_ = request.Validate()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-provider.started; cancel() }()

	outcome, err := newTestGateway(t, provider, emitter).Complete(ctx, request, func(string) error { return nil })
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if outcome.Status != events.AIResponseStatusCancelled {
		t.Errorf("status = %q, want cancelled", outcome.Status)
	}
	_, payload, _ := emitter.only(t)
	if payload.Status != events.AIResponseStatusCancelled || payload.ResponseText != "partial" {
		t.Errorf("payload = %+v", payload)
	}
	if emitter.ctxErr != nil {
		t.Errorf("emit ran with a cancelled context: %v", emitter.ctxErr)
	}
}

func TestCompleteReturnsEmitFailure(t *testing.T) {
	emitter := &recordingEmitter{err: errors.New("kafka down")}
	request := validRequest()
	_ = request.Validate()
	provider := &scriptedProvider{result: ProviderResult{FinishReason: FinishStop}}
	if _, err := newTestGateway(t, provider, emitter).Complete(context.Background(), request, func(string) error { return nil }); err == nil {
		t.Error("Complete hid the emit failure")
	}
}

func TestCompleteBYOKWithoutResolverIsAnError(t *testing.T) {
	emitter := &recordingEmitter{}
	request := validRequest()
	request.Mode = ModeBYOK
	_ = request.Validate()
	provider := &scriptedProvider{result: ProviderResult{FinishReason: FinishStop}}
	outcome, _ := newTestGateway(t, provider, emitter).Complete(context.Background(), request, func(string) error { return nil })
	if outcome.Status != events.AIResponseStatusError || outcome.ErrorCode != "byok_unavailable" {
		t.Errorf("outcome = %+v", outcome)
	}
	emitter.only(t)
}

func TestNewGatewayRequiresDependencies(t *testing.T) {
	if _, err := NewGateway(nil, &recordingEmitter{}, nil, nil); err == nil {
		t.Error("accepted nil provider")
	}
	if _, err := NewGateway(&scriptedProvider{}, nil, nil, nil); err == nil {
		t.Error("accepted nil emitter")
	}
}

func TestValidate(t *testing.T) {
	request := validRequest()
	if err := request.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if request.Mode != ModeManaged || request.Model != DefaultModel || request.MaxTokens != defaultMaxTokens {
		t.Errorf("defaults not applied: %+v", request)
	}
	broken := []func(*CompleteRequest){
		func(r *CompleteRequest) { r.CompanyID = "nope" },
		func(r *CompleteRequest) { r.PromptID = "" },
		func(r *CompleteRequest) { r.SequenceNumber = 0 },
		func(r *CompleteRequest) { r.Mode = "free" },
		func(r *CompleteRequest) { r.Messages = nil },
		func(r *CompleteRequest) { r.Messages = []Message{{Role: "system", Content: "x"}} },
		func(r *CompleteRequest) { r.Messages = []Message{{Role: RoleUser, Content: "  "}} },
		func(r *CompleteRequest) {
			r.Messages = append(r.Messages, Message{Role: RoleAssistant, Content: "prefill"})
		},
		func(r *CompleteRequest) { r.MaxTokens = maxMaxTokens + 1 },
	}
	for i, breakIt := range broken {
		request := validRequest()
		breakIt(&request)
		if err := request.Validate(); err == nil {
			t.Errorf("case %d: invalid request accepted: %+v", i, request)
		}
	}
}

func TestErrorCode(t *testing.T) {
	if got := errorCode(&ProviderError{Code: "provider_auth", Err: errors.New("x")}); got != "provider_auth" {
		t.Errorf("errorCode = %q", got)
	}
	if got := errorCode(errors.New("x")); got != "provider_error" {
		t.Errorf("errorCode = %q", got)
	}
}

func postComplete(t *testing.T, server *httptest.Server, ctx context.Context, request CompleteRequest) *http.Response {
	t.Helper()
	body, _ := json.Marshal(request)
	httpRequest, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/complete", bytes.NewReader(body))
	response, err := server.Client().Do(httpRequest)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	return response
}

func TestHTTPRelaysTokensAsSSE(t *testing.T) {
	provider := &scriptedProvider{deltas: []string{"Hel", "lo"}, result: ProviderResult{FinishReason: FinishStop, StopReason: "end_turn"}}
	emitter := &recordingEmitter{}
	server := httptest.NewServer(NewHTTPHandler(newTestGateway(t, provider, emitter)).Routes())
	defer server.Close()

	response := postComplete(t, server, context.Background(), validRequest())
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("status %d content-type %q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	reader := sse.NewReader(response.Body)
	var text strings.Builder
	var done Outcome
	for {
		event, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		switch event.Name {
		case EventDelta:
			var delta DeltaEvent
			_ = json.Unmarshal(event.Data, &delta)
			text.WriteString(delta.Text)
		case EventDone:
			_ = json.Unmarshal(event.Data, &done)
		}
	}
	if text.String() != "Hello" || done.Status != events.AIResponseStatusCompleted {
		t.Errorf("text %q done %+v", text.String(), done)
	}
	emitter.only(t)
}

func TestHTTPClientDisconnectCancelsUpstreamAndStillEmits(t *testing.T) {
	provider := &scriptedProvider{deltas: []string{"first"}, block: true}
	emitter := &recordingEmitter{}
	server := httptest.NewServer(NewHTTPHandler(newTestGateway(t, provider, emitter)).Routes())
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	response := postComplete(t, server, ctx, validRequest())
	line, _ := bufio.NewReader(response.Body).ReadString('\n')
	if !strings.HasPrefix(line, "event: delta") {
		t.Fatalf("first line = %q", line)
	}
	cancel()
	response.Body.Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		emitter.mu.Lock()
		n := len(emitter.envelopes)
		emitter.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no completion event after client disconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, payload, _ := emitter.only(t)
	if payload.Status != events.AIResponseStatusCancelled {
		t.Errorf("status = %q, want cancelled", payload.Status)
	}
}

func TestHTTPRejectsInvalidRequests(t *testing.T) {
	server := httptest.NewServer(NewHTTPHandler(newTestGateway(t, &scriptedProvider{}, &recordingEmitter{})).Routes())
	defer server.Close()
	for _, body := range []string{"not json", `{"unknown":1}`, `{"messages":[]}`} {
		response, err := server.Client().Post(server.URL+"/v1/complete", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Errorf("body %q: status %d, want 400", body, response.StatusCode)
		}
	}
}

type captureWriter struct{ messages []kafkago.Message }

func (w *captureWriter) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.messages = append(w.messages, messages...)
	return nil
}

func TestNewKafkaEmitterRequiresAcksAll(t *testing.T) {
	for name, writer := range map[string]*kafkago.Writer{
		"acks=1":  {Topic: events.SessionEventsTopic, RequiredAcks: kafkago.RequireOne},
		"acks=0":  {Topic: events.SessionEventsTopic, RequiredAcks: kafkago.RequireNone},
		"async":   {Topic: events.SessionEventsTopic, RequiredAcks: kafkago.RequireAll, Async: true},
		"notopic": {RequiredAcks: kafkago.RequireAll},
	} {
		if _, err := NewKafkaEmitter(writer); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := NewKafkaEmitter(&kafkago.Writer{Topic: events.SessionEventsTopic, RequiredAcks: kafkago.RequireAll}); err != nil {
		t.Errorf("acks=all rejected: %v", err)
	}
}

func TestKafkaEmitterWritesKeyedEnvelope(t *testing.T) {
	writer := &captureWriter{}
	emitter := &KafkaEmitter{writer: writer}
	envelope, _ := events.New(uuid.NewString(), 3, events.AIResponseCompleted{SessionID: "s", Status: "completed"})
	if err := emitter.Emit(context.Background(), "session-1", envelope); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if len(writer.messages) != 1 || string(writer.messages[0].Key) != "session-1" {
		t.Fatalf("messages = %+v", writer.messages)
	}
	message := writer.messages[0]
	if message.Headers[0].Key != "event_type" || string(message.Headers[0].Value) != string(events.EventTypeAIResponseCompleted) {
		t.Errorf("headers = %+v", message.Headers)
	}
	var decoded events.Envelope
	if err := json.Unmarshal(message.Value, &decoded); err != nil || decoded.EventID != envelope.EventID {
		t.Errorf("value did not round-trip: %v", err)
	}
}
