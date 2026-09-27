package candidateworkspace

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/events"
)

type stubProvider struct {
	deltas []string
	result aigateway.ProviderResult
	block  bool
	done   chan struct{}
}

func (p *stubProvider) Name() string { return "stub" }
func (p *stubProvider) Stream(ctx context.Context, _ aigateway.ProviderRequest, onDelta func(string) error) (aigateway.ProviderResult, error) {
	for _, d := range p.deltas {
		if err := onDelta(d); err != nil {
			return aigateway.ProviderResult{}, err
		}
	}
	if p.block {
		<-ctx.Done()
		close(p.done)
		return aigateway.ProviderResult{}, ctx.Err()
	}
	return p.result, nil
}

type nopEmitter struct{ emitted chan events.Envelope }

func (e nopEmitter) Emit(_ context.Context, _ string, envelope events.Envelope) error {
	if e.emitted != nil {
		e.emitted <- envelope
	}
	return nil
}

func gatewayServer(t *testing.T, provider aigateway.Provider, emitter aigateway.Emitter) *httptest.Server {
	t.Helper()
	gateway, err := aigateway.NewGateway(provider, emitter, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(aigateway.NewHTTPHandler(gateway).Routes())
	t.Cleanup(server.Close)
	return server
}

func completeRequest() aigateway.CompleteRequest {
	return aigateway.CompleteRequest{
		CompanyID: uuid.NewString(), InterviewID: uuid.NewString(), SessionID: uuid.NewString(),
		PromptID: uuid.NewString(), SequenceNumber: 2,
		Messages: []aigateway.Message{{Role: aigateway.RoleUser, Content: "hi"}},
	}
}

func TestHTTPGatewayRelaysDeltasAndOutcome(t *testing.T) {
	server := gatewayServer(t, &stubProvider{deltas: []string{"x", "y"}, result: aigateway.ProviderResult{FinishReason: aigateway.FinishLength, StopReason: "max_tokens"}}, nopEmitter{})
	var text strings.Builder
	outcome, err := NewHTTPGateway(server.URL+"/", nil).Complete(context.Background(), completeRequest(), func(d string) error {
		text.WriteString(d)
		return nil
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if text.String() != "xy" || outcome.Status != events.AIResponseStatusTruncated || outcome.StopReason != "max_tokens" {
		t.Errorf("text %q outcome %+v", text.String(), outcome)
	}
}

func TestHTTPGatewayCancellationReachesProvider(t *testing.T) {
	provider := &stubProvider{deltas: []string{"first"}, block: true, done: make(chan struct{})}
	server := gatewayServer(t, provider, nopEmitter{})
	ctx, cancel := context.WithCancel(context.Background())
	_, err := NewHTTPGateway(server.URL, nil).Complete(ctx, completeRequest(), func(string) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	select {
	case <-provider.done:
	case <-time.After(5 * time.Second):
		t.Fatal("provider call was not cancelled")
	}
}

func TestHTTPGatewayErrors(t *testing.T) {
	badStatus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	defer badStatus.Close()
	cutOff := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: delta\ndata: {\"text\":\"a\"}\n\n")
	}))
	defer cutOff.Close()
	badJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "event: done\ndata: nope\n\n")
	}))
	defer badJSON.Close()
	unreachable := httptest.NewServer(http.NotFoundHandler())
	unreachable.Close()

	for name, url := range map[string]string{"status": badStatus.URL, "cut off": cutOff.URL, "bad json": badJSON.URL, "unreachable": unreachable.URL} {
		_, err := NewHTTPGateway(url, nil).Complete(context.Background(), completeRequest(), func(string) error { return nil })
		if !errors.Is(err, ErrGatewayUnavailable) {
			t.Errorf("%s: err = %v, want ErrGatewayUnavailable", name, err)
		}
	}
}

func TestHTTPGatewayStopsOnDeltaError(t *testing.T) {
	server := gatewayServer(t, &stubProvider{deltas: []string{"a", "b"}, result: aigateway.ProviderResult{FinishReason: aigateway.FinishStop}}, nopEmitter{})
	stop := errors.New("stop")
	if _, err := NewHTTPGateway(server.URL, nil).Complete(context.Background(), completeRequest(), func(string) error { return stop }); !errors.Is(err, stop) {
		t.Errorf("err = %v", err)
	}
}
