// Package typesense is a thin client for the Typesense REST API, covering
// what the platform needs: collection bootstrap, idempotent batch upserts,
// delete-by-filter for purges, and search.
package typesense

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client talks to one Typesense node.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

// New returns a client for baseURL (e.g. "http://localhost:8108", see
// internal/platform/config) authenticating with apiKey.
func New(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// Field is one collection schema field.
type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Facet    bool   `json:"facet,omitempty"`
	Optional bool   `json:"optional,omitempty"`
	Index    *bool  `json:"index,omitempty"`
}

// Schema describes a collection.
type Schema struct {
	Name                string  `json:"name"`
	Fields              []Field `json:"fields"`
	DefaultSortingField string  `json:"default_sorting_field,omitempty"`
}

// APIError is a non-2xx Typesense response.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("typesense: HTTP %d: %s", e.Status, e.Message)
}

// IsNotFound reports whether err is a Typesense 404.
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

// Health verifies the node is up and ready.
func (c *Client) Health(ctx context.Context) error {
	var body struct {
		OK bool `json:"ok"`
	}
	if err := c.do(ctx, http.MethodGet, "/health", nil, "", &body); err != nil {
		return err
	}
	if !body.OK {
		return errors.New("typesense: node not healthy")
	}
	return nil
}

// EnsureCollection creates schema's collection. An existing collection is
// not an error and is left unchanged.
func (c *Client) EnsureCollection(ctx context.Context, schema Schema) error {
	body, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("typesense: marshal schema: %w", err)
	}
	err = c.do(ctx, http.MethodPost, "/collections", body, "application/json", nil)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict {
		return nil
	}
	return err
}

// DeleteCollection drops a collection. A missing collection is not an error.
func (c *Client) DeleteCollection(ctx context.Context, name string) error {
	err := c.do(ctx, http.MethodDelete, "/collections/"+url.PathEscape(name), nil, "", nil)
	if IsNotFound(err) {
		return nil
	}
	return err
}

// ImportError reports documents Typesense rejected within an import. The
// import is not transactional: the other documents were written.
type ImportError struct {
	Failures []ImportFailure
}

// ImportFailure is one rejected document.
type ImportFailure struct {
	Index int
	Error string
}

func (e *ImportError) Error() string {
	first := e.Failures[0]
	return fmt.Sprintf("typesense: %d document(s) rejected; first at %d: %s", len(e.Failures), first.Index, first.Error)
}

// Upsert writes documents in one import request with action=upsert, so a
// document whose id already exists is replaced. Re-sending the same
// documents is therefore a no-op. It returns *ImportError if any document
// was rejected.
func (c *Client) Upsert(ctx context.Context, collection string, documents []any) error {
	if len(documents) == 0 {
		return nil
	}
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for i, document := range documents {
		if err := encoder.Encode(document); err != nil {
			return fmt.Errorf("typesense: encode document %d: %w", i, err)
		}
	}
	path := "/collections/" + url.PathEscape(collection) + "/documents/import?action=upsert"
	var raw []byte
	if err := c.do(ctx, http.MethodPost, path, body.Bytes(), "text/plain", &raw); err != nil {
		return err
	}
	return parseImportResults(raw, len(documents))
}

func parseImportResults(raw []byte, want int) error {
	lines := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))
	if len(lines) != want {
		return fmt.Errorf("typesense: import returned %d results for %d documents", len(lines), want)
	}
	var failures []ImportFailure
	for i, line := range lines {
		var result struct {
			Success bool   `json:"success"`
			Error   string `json:"error"`
		}
		if err := json.Unmarshal(line, &result); err != nil {
			return fmt.Errorf("typesense: decode import result %d: %w", i, err)
		}
		if !result.Success {
			failures = append(failures, ImportFailure{Index: i, Error: result.Error})
		}
	}
	if len(failures) > 0 {
		return &ImportError{Failures: failures}
	}
	return nil
}

// DeleteByFilter deletes every document in collection matching filter
// (Typesense filter_by syntax) and returns how many were deleted. A missing
// collection deletes nothing.
func (c *Client) DeleteByFilter(ctx context.Context, collection, filter string) (int, error) {
	if strings.TrimSpace(filter) == "" {
		return 0, errors.New("typesense: refusing to delete with an empty filter")
	}
	path := "/collections/" + url.PathEscape(collection) + "/documents?filter_by=" + url.QueryEscape(filter)
	var body struct {
		NumDeleted int `json:"num_deleted"`
	}
	err := c.do(ctx, http.MethodDelete, path, nil, "", &body)
	if IsNotFound(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return body.NumDeleted, nil
}

// SearchParams are the Typesense search parameters the platform uses.
type SearchParams struct {
	Query    string
	QueryBy  string
	FilterBy string
	PerPage  int
}

// SearchResult holds the matched documents, undecoded.
type SearchResult struct {
	Found int               `json:"found"`
	Hits  []json.RawMessage `json:"-"`
}

// Search runs a query against collection.
func (c *Client) Search(ctx context.Context, collection string, params SearchParams) (SearchResult, error) {
	values := url.Values{}
	values.Set("q", params.Query)
	values.Set("query_by", params.QueryBy)
	if params.FilterBy != "" {
		values.Set("filter_by", params.FilterBy)
	}
	if params.PerPage > 0 {
		values.Set("per_page", fmt.Sprint(params.PerPage))
	}
	path := "/collections/" + url.PathEscape(collection) + "/documents/search?" + values.Encode()
	var body struct {
		Found int `json:"found"`
		Hits  []struct {
			Document json.RawMessage `json:"document"`
		} `json:"hits"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, "", &body); err != nil {
		return SearchResult{}, err
	}
	result := SearchResult{Found: body.Found}
	for _, hit := range body.Hits {
		result.Hits = append(result.Hits, hit.Document)
	}
	return result, nil
}

// do sends a request. out may be nil (discard), *[]byte (raw body), or a
// pointer to decode JSON into.
func (c *Client) do(ctx context.Context, method, path string, body []byte, contentType string, out any) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("typesense: build request: %w", err)
	}
	request.Header.Set("X-TYPESENSE-API-KEY", c.apiKey)
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("typesense: %s %s: %w", method, strings.SplitN(path, "?", 2)[0], err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("typesense: read response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		var failure struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(raw, &failure)
		if failure.Message == "" {
			failure.Message = strings.TrimSpace(string(raw))
		}
		return &APIError{Status: response.StatusCode, Message: failure.Message}
	}
	switch target := out.(type) {
	case nil:
		return nil
	case *[]byte:
		*target = raw
		return nil
	default:
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("typesense: decode response: %w", err)
		}
		return nil
	}
}
