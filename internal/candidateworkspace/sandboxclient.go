package candidateworkspace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/telic-ai/ta-platform/internal/sandbox"
)

// HTTPExecutor calls the Execution Sandbox's POST /v1/executions.
type HTTPExecutor struct {
	baseURL string
	client  *http.Client
}

func NewHTTPExecutor(baseURL string, client *http.Client) *HTTPExecutor {
	if client == nil {
		client = &http.Client{}
	}
	return &HTTPExecutor{baseURL: strings.TrimRight(baseURL, "/"), client: client}
}

func (e *HTTPExecutor) Execute(ctx context.Context, request sandbox.ExecuteRequest) (sandbox.ExecuteResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return sandbox.ExecuteResponse{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/v1/executions", bytes.NewReader(body))
	if err != nil {
		return sandbox.ExecuteResponse{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := e.client.Do(httpRequest)
	if err != nil {
		return sandbox.ExecuteResponse{}, fmt.Errorf("call sandbox: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return sandbox.ExecuteResponse{}, fmt.Errorf("sandbox returned %d: %s", response.StatusCode, bytes.TrimSpace(detail))
	}
	var result sandbox.ExecuteResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&result); err != nil {
		return sandbox.ExecuteResponse{}, fmt.Errorf("decode sandbox response: %w", err)
	}
	return result, nil
}
