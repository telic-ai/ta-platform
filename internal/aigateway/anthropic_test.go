package aigateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeMessagesAPI serves a scripted Messages API event stream and records
// the request it received.
type fakeMessagesAPI struct {
	status  int
	events  []string
	gotKey  string
	gotBody map[string]any
	gotBeta string
}

func (f *fakeMessagesAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.gotKey = r.Header.Get("X-Api-Key")
	f.gotBeta = r.Header.Get("Anthropic-Beta")
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &f.gotBody)
	if f.status != 0 && f.status != http.StatusOK {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.status)
		_, _ = io.WriteString(w, `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	for _, event := range f.events {
		_, _ = io.WriteString(w, event)
	}
}

func sseEvent(name, data string) string { return fmt.Sprintf("event: %s\ndata: %s\n\n", name, data) }

func messageStream(stopReason string, deltas ...string) []string {
	out := []string{
		sseEvent("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":12,"output_tokens":1}}}`),
		sseEvent("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`),
	}
	for _, delta := range deltas {
		text, _ := json.Marshal(delta)
		out = append(out, sseEvent("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":`+string(text)+`}}`))
	}
	return append(out,
		sseEvent("content_block_stop", `{"type":"content_block_stop","index":0}`),
		sseEvent("message_delta", `{"type":"message_delta","delta":{"stop_reason":"`+stopReason+`","stop_sequence":null},"usage":{"output_tokens":7}}`),
		sseEvent("message_stop", `{"type":"message_stop"}`),
	)
}

func streamFrom(t *testing.T, api *fakeMessagesAPI, key *Key) (string, ProviderResult, error) {
	t.Helper()
	server := httptest.NewServer(api)
	defer server.Close()
	provider := NewAnthropicProvider(AnthropicConfig{APIKey: "managed-key", BaseURL: server.URL, HTTPClient: server.Client()})
	var text strings.Builder
	result, err := provider.Stream(context.Background(), ProviderRequest{
		Model: DefaultModel, System: "be brief", MaxTokens: 100, APIKey: key,
		Messages: []Message{{Role: RoleUser, Content: "hi"}, {Role: RoleAssistant, Content: "hello"}, {Role: RoleUser, Content: "code?"}},
	}, func(delta string) error { text.WriteString(delta); return nil })
	return text.String(), result, err
}

func TestAnthropicStreamsTextAndUsage(t *testing.T) {
	api := &fakeMessagesAPI{events: messageStream("end_turn", "Hel", "lo")}
	text, result, err := streamFrom(t, api, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if text != "Hello" || result.FinishReason != FinishStop || result.StopReason != "end_turn" {
		t.Errorf("text %q result %+v", text, result)
	}
	if result.Usage.InputTokens != 12 || result.Usage.OutputTokens != 7 || result.Model != "claude-opus-5" {
		t.Errorf("result = %+v", result)
	}
	if api.gotKey != "managed-key" {
		t.Errorf("managed mode sent key %q", api.gotKey)
	}
	if api.gotBody["stream"] != true || api.gotBody["model"] != DefaultModel || api.gotBody["fallbacks"] != "default" {
		t.Errorf("request body = %v", api.gotBody)
	}
	if !strings.Contains(api.gotBeta, "server-side-fallback-2026-07-01") {
		t.Errorf("beta header = %q", api.gotBeta)
	}
	if messages, _ := api.gotBody["messages"].([]any); len(messages) != 3 {
		t.Errorf("messages = %v", api.gotBody["messages"])
	}
}

func TestAnthropicMaxTokensIsTruncation(t *testing.T) {
	for _, stop := range []string{"max_tokens", "model_context_window_exceeded"} {
		_, result, err := streamFrom(t, &fakeMessagesAPI{events: messageStream(stop, "cut")}, nil)
		if err != nil || result.FinishReason != FinishLength {
			t.Errorf("%s: result %+v err %v", stop, result, err)
		}
	}
}

func TestAnthropicRefusal(t *testing.T) {
	_, result, err := streamFrom(t, &fakeMessagesAPI{events: messageStream("refusal")}, nil)
	if err != nil || result.FinishReason != FinishRefusal {
		t.Errorf("result %+v err %v", result, err)
	}
}

func TestAnthropicBYOKKeyOverridesManagedKey(t *testing.T) {
	api := &fakeMessagesAPI{events: messageStream("end_turn", "ok")}
	if _, _, err := streamFrom(t, api, NewKey([]byte("sk-company-own"))); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if api.gotKey != "sk-company-own" {
		t.Errorf("sent key %q, want the BYOK key", api.gotKey)
	}
}

func TestAnthropicMidStreamErrorEvent(t *testing.T) {
	api := &fakeMessagesAPI{events: append(messageStream("end_turn", "partial")[:3],
		sseEvent("error", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`))}
	text, _, err := streamFrom(t, api, nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("err = %v, want ProviderError", err)
	}
	if text != "partial" {
		t.Errorf("text before error = %q", text)
	}
}

func TestAnthropicStreamWithoutStopReasonIsAnError(t *testing.T) {
	api := &fakeMessagesAPI{events: messageStream("end_turn", "x")[:3]}
	_, _, err := streamFrom(t, api, nil)
	if errorCode(err) != "incomplete_stream" {
		t.Errorf("err = %v", err)
	}
}

func TestAnthropicHTTPErrorsAreClassified(t *testing.T) {
	for status, want := range map[int]string{
		http.StatusUnauthorized: "provider_auth", http.StatusTooManyRequests: "provider_rate_limited",
		http.StatusBadRequest: "provider_invalid_request", http.StatusInternalServerError: "provider_unavailable",
	} {
		_, _, err := streamFrom(t, &fakeMessagesAPI{status: status}, NewKey([]byte("sk-secret-value")))
		if got := errorCode(err); got != want {
			t.Errorf("status %d: code %q, want %q", status, got, want)
		}
		if err != nil && strings.Contains(err.Error(), "sk-secret-value") {
			t.Errorf("status %d: error text leaked the key", status)
		}
	}
}

func TestAnthropicDeltaErrorAbortsStream(t *testing.T) {
	server := httptest.NewServer(&fakeMessagesAPI{events: messageStream("end_turn", "a", "b")})
	defer server.Close()
	provider := NewAnthropicProvider(AnthropicConfig{APIKey: "k", BaseURL: server.URL})
	stop := errors.New("client gone")
	_, err := provider.Stream(context.Background(), ProviderRequest{Model: DefaultModel, MaxTokens: 5,
		Messages: []Message{{Role: RoleUser, Content: "x"}}}, func(string) error { return stop })
	if !errors.Is(err, stop) {
		t.Errorf("err = %v, want the delta error", err)
	}
}

func TestFinishReason(t *testing.T) {
	for stop, want := range map[string]FinishReason{
		"end_turn": FinishStop, "stop_sequence": FinishStop, "tool_use": FinishStop,
		"max_tokens": FinishLength, "model_context_window_exceeded": FinishLength, "refusal": FinishRefusal,
	} {
		if got := finishReason(stop); got != want {
			t.Errorf("finishReason(%q) = %q, want %q", stop, got, want)
		}
	}
}
