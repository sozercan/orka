/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/workerenv"
)

func TestWebSearchTool_Name(t *testing.T) {
	tool := NewWebSearchTool()
	if got := tool.Name(); got != webSearchToolName {
		t.Errorf("Name() = %v, want %v", got, webSearchToolName)
	}
}

func TestWebSearchTool_Description(t *testing.T) {
	tool := NewWebSearchTool()
	desc := tool.Description()
	if desc == "" {
		t.Error("Description() returned empty string")
	}
}

func TestWebSearchTool_Parameters(t *testing.T) {
	tool := NewWebSearchTool()
	params := tool.Parameters()
	if params == nil {
		t.Fatal("Parameters() returned nil")
	}

	// Verify it's valid JSON
	var schema map[string]any
	if err := json.Unmarshal(params, &schema); err != nil {
		t.Errorf("Parameters() returned invalid JSON: %v", err)
	}

	// Check required fields
	if schema[jsonSchemaTypeField] != typeObject {
		t.Error("Parameters schema should have type: object")
	}
}

func TestNewBrokeredWebSearchToolIgnoresWorkerConfigurationAndRejectsPrivateEndpoints(t *testing.T) {
	t.Setenv(workerenv.SearchAPIKey, "configured-worker-value")
	t.Setenv(workerenv.SearchAPIURL, "http://127.0.0.1:8080/search")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:8081")

	tool := NewBrokeredWebSearchTool()
	if tool.apiKey != "" || tool.baseURL != "" {
		t.Fatalf("brokered search inherited worker configuration: apiKey=%t baseURL=%q", tool.apiKey != "", tool.baseURL)
	}
	transport, ok := tool.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("brokered search transport = %T, want *http.Transport", tool.client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("brokered search transport inherited proxy configuration")
	}
	if transport.DialContext == nil {
		t.Fatal("brokered search transport has no public-endpoint dial guard")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if connection, err := transport.DialContext(ctx, "tcp", "127.0.0.1:80"); err == nil {
		connection.Close() //nolint:errcheck
		t.Fatal("brokered search transport dialed a private endpoint")
	}
	privateRedirect := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/internal", nil)
	if err := tool.client.CheckRedirect(privateRedirect, nil); err == nil {
		t.Fatal("brokered search client accepted a private redirect")
	}
}

func TestBrokeredWebSearchReturnsErrorsInsteadOfSyntheticResults(t *testing.T) {
	for _, test := range []struct {
		name      string
		transport http.RoundTripper
		want      string
	}{
		{
			name: "request failure",
			transport: webSearchRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("offline")
			}),
			want: "DuckDuckGo request failed",
		},
		{
			name: "unparseable response",
			transport: webSearchRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("markup changed")),
				}, nil
			}),
			want: "no parseable results",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tool := NewBrokeredWebSearchTool()
			tool.client = &http.Client{Transport: test.transport}
			result, err := tool.Execute(context.Background(), json.RawMessage(`{"query":"test"}`))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Execute() result = %q, error = %v, want error containing %q", result, err, test.want)
			}
			if strings.Contains(result, "Search Result") {
				t.Fatalf("Execute() returned synthetic search data: %q", result)
			}
		})
	}
}

func TestBrokeredWebSearchRejectsOversizedArgumentsBeforeRequest(t *testing.T) {
	tests := []struct {
		name    string
		args    WebSearchArgs
		wantErr string
	}{
		{
			name:    "query",
			args:    WebSearchArgs{Query: strings.Repeat("q", brokeredWebSearchMaxQueryChars+1)},
			wantErr: "query must be no greater than",
		},
		{
			name:    "limit",
			args:    WebSearchArgs{Query: "test", Limit: brokeredWebSearchMaxResults + 1},
			wantErr: "limit must be no greater than",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			tool := NewBrokeredWebSearchTool()
			tool.client = &http.Client{Transport: webSearchRoundTripFunc(func(*http.Request) (*http.Response, error) {
				called = true
				return nil, errors.New("unexpected request")
			})}
			args, err := json.Marshal(test.args)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tool.Execute(t.Context(), args); err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Execute() error = %v, want %q", err, test.wantErr)
			}
			if called {
				t.Fatal("oversized brokered web_search arguments reached the HTTP client")
			}
		})
	}
}

func TestBrokeredWebSearchQueryLimitCountsUnicodeCharacters(t *testing.T) {
	query := strings.Repeat("é", brokeredWebSearchMaxQueryChars)
	if len(query) <= brokeredWebSearchMaxQueryChars {
		t.Fatalf("test query bytes = %d, want greater than character limit %d", len(query), brokeredWebSearchMaxQueryChars)
	}

	called := false
	tool := NewBrokeredWebSearchTool()
	tool.baseURL = "https://search.example.test"
	tool.client = &http.Client{Transport: webSearchRoundTripFunc(func(*http.Request) (*http.Response, error) {
		called = true
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`[]`)),
		}, nil
	})}

	args, err := json.Marshal(WebSearchArgs{Query: query})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tool.Execute(t.Context(), args); err != nil {
		t.Fatalf("Execute() rejected query at character limit: %v", err)
	}
	if !called {
		t.Fatal("query at character limit did not reach the HTTP client")
	}

	called = false
	args, err = json.Marshal(WebSearchArgs{Query: query + "é"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tool.Execute(t.Context(), args); err == nil || !strings.Contains(err.Error(), "characters") {
		t.Fatalf("Execute() error = %v, want character-limit error", err)
	}
	if called {
		t.Fatal("query above character limit reached the HTTP client")
	}
}

func TestWebSearchTool_Execute(t *testing.T) {
	tests := []struct {
		name    string
		args    json.RawMessage
		wantErr bool
	}{
		{
			name:    "valid query",
			args:    json.RawMessage(`{"query": "test search"}`),
			wantErr: false,
		},
		{
			name:    "valid query with limit",
			args:    json.RawMessage(`{"query": "test search", "limit": 3}`),
			wantErr: false,
		},
		{
			name:    "empty query",
			args:    json.RawMessage(`{"query": ""}`),
			wantErr: true,
		},
		{
			name:    "missing query",
			args:    json.RawMessage(`{}`),
			wantErr: true,
		},
		{
			name:    "invalid JSON",
			args:    json.RawMessage(invalidJSONText),
			wantErr: true,
		},
		{
			name:    "negative limit uses default",
			args:    json.RawMessage(`{"query": "test", "limit": -1}`),
			wantErr: false,
		},
		{
			name:    "zero limit uses default",
			args:    json.RawMessage(`{"query": "test", "limit": 0}`),
			wantErr: false,
		},
	}

	tool := NewWebSearchTool()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tool.Execute(context.Background(), tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result == "" {
				t.Error("Execute() returned empty result")
			}
		})
	}
}

func TestWebSearchTool_Execute_MockSearch(t *testing.T) {
	// Test mock search (no API configured)
	tool := NewWebSearchTool()
	// Ensure no API URL is set
	tool.baseURL = ""

	args := json.RawMessage(`{"query": "test query"}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Verify result is valid JSON
	var results []WebSearchResult
	if err := json.Unmarshal([]byte(result), &results); err != nil {
		t.Errorf("Execute() returned invalid JSON: %v", err)
	}

	if len(results) == 0 {
		t.Error("Execute() returned empty results")
	}
}

func TestWebSearchTool_Execute_APISearch(t *testing.T) {
	// Create a test server
	expectedResponse := `[{"title": "Test", "url": "https://example.com", "snippet": "Test result"}]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		if r.Method != http.MethodGet {
			t.Errorf("expected GET request, got %s", r.Method)
		}

		query := r.URL.Query().Get("q")
		if query != "test query" {
			t.Errorf("expected query 'test query', got '%s'", query)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(expectedResponse)) //nolint:errcheck
	}))
	defer server.Close()

	tool := &WebSearchTool{
		baseURL: server.URL,
		apiKey:  "test-key",
		client:  server.Client(),
	}

	args := json.RawMessage(`{"query": "test query", "limit": 5}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if result != expectedResponse {
		t.Errorf("Execute() = %v, want %v", result, expectedResponse)
	}
}

func TestWebSearchTool_Execute_APIError(t *testing.T) {
	// Create a test server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error")) //nolint:errcheck
	}))
	defer server.Close()

	tool := &WebSearchTool{
		baseURL: server.URL,
		client:  server.Client(),
	}

	args := json.RawMessage(`{"query": "test query"}`)
	_, err := tool.Execute(context.Background(), args)
	if err == nil {
		t.Error("Execute() expected error for API failure")
	}
}

func TestWebSearchTool_Execute_WithAuthHeader(t *testing.T) {
	var receivedAuthHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuthHeader = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`)) //nolint:errcheck
	}))
	defer server.Close()

	tool := &WebSearchTool{
		baseURL: server.URL,
		apiKey:  "test-api-key",
		client:  server.Client(),
	}

	args := json.RawMessage(`{"query": "test"}`)
	_, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	expected := "Bearer test-api-key"
	if receivedAuthHeader != expected {
		t.Errorf("Authorization header = %v, want %v", receivedAuthHeader, expected)
	}
}

func TestWebSearchTool_Execute_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// Slow response
		<-r.Context().Done()
	}))
	defer server.Close()

	tool := &WebSearchTool{
		baseURL: server.URL,
		client:  server.Client(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	args := json.RawMessage(`{"query": "test"}`)
	_, err := tool.Execute(ctx, args)
	if err == nil {
		t.Error("Execute() expected error for cancelled context")
	}
}

func TestParseDDGResults(t *testing.T) {
	html := `<div class="result">
		<a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fpage1&rut=abc">Example Page 1</a>
		<a class="result__snippet" href="#">This is the first result snippet</a>
	</div>
	<div class="result">
		<a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fpage2&rut=def"><b>Bold</b> Page 2</a>
		<a class="result__snippet" href="#">Second result with <b>bold</b></a>
	</div>`

	results := parseDDGResults(html, 10)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0].URL != "https://example.com/page1" {
		t.Errorf("result[0].URL = %q, want %q", results[0].URL, "https://example.com/page1")
	}
	if results[0].Title != "Example Page 1" {
		t.Errorf("result[0].Title = %q, want %q", results[0].Title, "Example Page 1")
	}

	// Tags should be stripped from title
	if results[1].Title != "Bold Page 2" {
		t.Errorf("result[1].Title = %q, want %q", results[1].Title, "Bold Page 2")
	}
}

func TestDecodeDDGURL(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{"with uddg param", "https://duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Ftest&rut=abc", "https://example.com/test"},
		{"direct URL", "https://example.com/direct", "https://example.com/direct"},
		{"non-http", "javascript:void(0)", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decodeDDGURL(tt.input)
			if got != tt.expect {
				t.Errorf("decodeDDGURL(%q) = %q, want %q", tt.input, got, tt.expect)
			}
		})
	}
}

func TestStripHTMLTags(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{"<b>bold</b>", "bold"},
		{"no tags", "no tags"},
		{"<a href='#'>link &amp; text</a>", "link & text"},
		{"<span>a</span> <span>b</span>", "a b"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := stripHTMLTags(tt.input)
			if got != tt.expect {
				t.Errorf("stripHTMLTags(%q) = %q, want %q", tt.input, got, tt.expect)
			}
		})
	}
}

type webSearchRoundTripFunc func(*http.Request) (*http.Response, error)

func (f webSearchRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
