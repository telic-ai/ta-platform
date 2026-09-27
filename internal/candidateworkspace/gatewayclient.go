package candidateworkspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/telic-ai/ta-platform/internal/aigateway"
	"github.com/telic-ai/ta-platform/internal/sse"
)

// ErrGatewayUnavailable means the gateway could not be reached or broke off
// the stream before its done event.
var ErrGatewayUnavailable = errors.New("AI gateway unavailable")

// HTTPGateway calls the AI Gateway's POST /v1/complete.
type HTTPGateway struct {
	baseURL string
	client  *http.Client
}

// NewHTTPGateway targets baseURL. client must not set a Timeout, which would
// cut long completions off; cancellation comes from the request context.
func NewHTTPGateway(baseURL string, client *http.Client) *HTTPGateway {
	if client == nil {
		client = &http.Client{}
	}
	return &HTTPGateway{baseURL: strings.TrimRight(baseURL, "/"), client: client}
}

func (g *HTTPGateway) Complete(ctx context.Context, request aigateway.CompleteRequest, onDelta func(string) error) (aigateway.Outcome, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return aigateway.Outcome{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, g.baseURL+"/v1/complete", bytes.NewReader(body))
	if err != nil {
		return aigateway.Outcome{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "text/event-stream")
	response, err := g.client.Do(httpRequest)
	if err != nil {
		if ctx.Err() != nil {
			return aigateway.Outcome{}, ctx.Err()
		}
		return aigateway.Outcome{}, fmt.Errorf("%w: %v", ErrGatewayUnavailable, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return aigateway.Outcome{}, fmt.Errorf("%w: status %d: %s", ErrGatewayUnavailable, response.StatusCode, bytes.TrimSpace(detail))
	}

	reader := sse.NewReader(response.Body)
	for {
		event, err := reader.Next()
		if err != nil {
			if ctx.Err() != nil {
				return aigateway.Outcome{}, ctx.Err()
			}
			return aigateway.Outcome{}, fmt.Errorf("%w: stream ended before done: %v", ErrGatewayUnavailable, err)
		}
		switch event.Name {
		case aigateway.EventDelta:
			var delta aigateway.DeltaEvent
			if err := json.Unmarshal(event.Data, &delta); err != nil {
				return aigateway.Outcome{}, fmt.Errorf("%w: bad delta: %v", ErrGatewayUnavailable, err)
			}
			if err := onDelta(delta.Text); err != nil {
				return aigateway.Outcome{}, err
			}
		case aigateway.EventDone:
			var outcome aigateway.Outcome
			if err := json.Unmarshal(event.Data, &outcome); err != nil {
				return aigateway.Outcome{}, fmt.Errorf("%w: bad done: %v", ErrGatewayUnavailable, err)
			}
			return outcome, nil
		}
	}
}
