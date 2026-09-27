package candidateworkspace

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/events"
	"github.com/telic-ai/ta-platform/internal/sse"
)

func promptServer(t *testing.T, store EventStore, gateway Completer) *httptest.Server {
	t.Helper()
	session := activeCandidate()
	session.ExpiresAt = time.Now().Add(time.Hour)
	handler := NewHTTPHandler(nil, fixedFinder{session: session}).WithPrompts(NewPromptService(store, gateway, PromptConfig{}))
	server := httptest.NewServer(handler.Routes())
	t.Cleanup(server.Close)
	return server
}

func postPrompt(t *testing.T, ctx context.Context, server *httptest.Server, body string, token bool) *http.Response {
	t.Helper()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/session/prompt", strings.NewReader(body))
	if token {
		request.Header.Set("Authorization", "Bearer opaque")
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	return response
}

func TestPromptEndpointStreamsPromptDeltasDone(t *testing.T) {
	store := &memoryEventStore{}
	gateway := &fakeCompleter{deltas: []string{"He", "llo"}, outcome: aigateway.Outcome{Status: events.AIResponseStatusCompleted, StopReason: "end_turn"}}
	response := postPrompt(t, context.Background(), promptServer(t, store, gateway), `{"prompt":"hi"}`, true)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("status %d", response.StatusCode)
	}
	reader := sse.NewReader(response.Body)
	var names []string
	var accepted PromptAccepted
	var done PromptDone
	for {
		event, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, event.Name)
		switch event.Name {
		case PromptEventAccepted:
			_ = json.Unmarshal(event.Data, &accepted)
		case PromptEventDone:
			_ = json.Unmarshal(event.Data, &done)
		}
	}
	if strings.Join(names, ",") != "prompt,delta,delta,done" {
		t.Errorf("events = %v", names)
	}
	if accepted.SequenceNumber != 1 || accepted.PromptID == "" || done.Status != "completed" || done.StopReason != "end_turn" {
		t.Errorf("accepted %+v done %+v", accepted, done)
	}
}

func TestPromptEndpointReportsGatewayFailureInDone(t *testing.T) {
	gateway := &fakeCompleter{err: ErrGatewayUnavailable}
	response := postPrompt(t, context.Background(), promptServer(t, &memoryEventStore{}, gateway), `{"prompt":"hi"}`, true)
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if !strings.Contains(string(body), `event: done`) || !strings.Contains(string(body), `"errorCode":"gateway_unavailable"`) {
		t.Errorf("body = %s", body)
	}
}

func TestPromptEndpointRejections(t *testing.T) {
	cases := []struct {
		body   string
		token  bool
		store  *memoryEventStore
		status int
	}{
		{`{"prompt":"hi"}`, false, &memoryEventStore{}, http.StatusUnauthorized},
		{`not json`, true, &memoryEventStore{}, http.StatusBadRequest},
		{`{"prompt":"hi","extra":1}`, true, &memoryEventStore{}, http.StatusBadRequest},
		{`{"prompt":"  "}`, true, &memoryEventStore{}, http.StatusBadRequest},
		{`{"prompt":"hi"}`, true, &memoryEventStore{err: errors.New("db down")}, http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		response := postPrompt(t, context.Background(), promptServer(t, tc.store, &fakeCompleter{}), tc.body, tc.token)
		response.Body.Close()
		if response.StatusCode != tc.status {
			t.Errorf("%s (token %v): status %d, want %d", tc.body, tc.token, response.StatusCode, tc.status)
		}
	}
}

func TestPromptEndpointRequiresActiveSession(t *testing.T) {
	session := activeCandidate()
	session.State = domain.SessionStateCompleted
	session.ExpiresAt = time.Now().Add(time.Hour)
	handler := NewHTTPHandler(nil, fixedFinder{session: session}).WithPrompts(NewPromptService(&memoryEventStore{}, &fakeCompleter{}, PromptConfig{}))
	server := httptest.NewServer(handler.Routes())
	defer server.Close()
	response := postPrompt(t, context.Background(), server, `{"prompt":"hi"}`, true)
	response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Errorf("status %d, want 409", response.StatusCode)
	}
}

func TestPromptEndpointClientDisconnectCancelsUpstream(t *testing.T) {
	gateway := &fakeCompleter{deltas: []string{"partial"}, block: true, cancelled: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	response := postPrompt(t, ctx, promptServer(t, &memoryEventStore{}, gateway), `{"prompt":"hi"}`, true)
	reader := bufio.NewReader(response.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if strings.HasPrefix(line, "event: delta") {
			break
		}
	}
	cancel()
	response.Body.Close()
	select {
	case <-gateway.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("client disconnect did not cancel the upstream call")
	}
}
