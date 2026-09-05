/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

func TestWebFetchTool_Name(t *testing.T) {
	tool := NewWebFetchTool()
	if got := tool.Name(); got != webFetchToolName {
		t.Errorf("Name() = %v, want %v", got, webFetchToolName)
	}
}

func TestWebFetchTool_Description(t *testing.T) {
	tool := NewWebFetchTool()
	if desc := tool.Description(); desc == "" {
		t.Error("Description() returned empty string")
	}
}

func TestWebFetchTool_Parameters(t *testing.T) {
	tool := NewWebFetchTool()
	params := tool.Parameters()
	if params == nil {
		t.Fatal("Parameters() returned nil")
	}
	var schema map[string]any
	if err := json.Unmarshal(params, &schema); err != nil {
		t.Errorf("Parameters() returned invalid JSON: %v", err)
	}
	if schema[jsonSchemaTypeField] != typeObject {
		t.Error("Parameters schema should have type: object")
	}
}

func TestWebFetchTool_Execute_HTML(t *testing.T) {
	html := `<html><head><title>Test</title><script>var x=1;</script><style>body{}</style></head>
	<body><h1>Hello World</h1><p>This is a test page.</p></body></html>`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(html)) //nolint:errcheck
	}))
	defer server.Close()

	tool := &WebFetchTool{client: server.Client(), allowPrivateForTests: true}
	args := json.RawMessage(`{"url": "` + server.URL + `"}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var fetchResult WebFetchResult
	if err := json.Unmarshal([]byte(result), &fetchResult); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if fetchResult.Extractor != "html_text" {
		t.Errorf("extractor = %q, want %q", fetchResult.Extractor, "html_text")
	}
	// Script and style content should be stripped
	if strContains(fetchResult.Content, "var x=1") {
		t.Error("content should not contain script content")
	}
	if strContains(fetchResult.Content, "body{}") {
		t.Error("content should not contain style content")
	}
	if !strContains(fetchResult.Content, "Hello World") {
		t.Error("content should contain text")
	}
	if !strContains(fetchResult.Content, "This is a test page") {
		t.Error("content should contain paragraph text")
	}
}

func TestWebFetchTool_Execute_JSON(t *testing.T) {
	jsonData := `{"key":"value","num":42}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(jsonData)) //nolint:errcheck
	}))
	defer server.Close()

	tool := &WebFetchTool{client: server.Client(), allowPrivateForTests: true}
	args := json.RawMessage(`{"url": "` + server.URL + `"}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var fetchResult WebFetchResult
	if err := json.Unmarshal([]byte(result), &fetchResult); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if fetchResult.Extractor != "json" {
		t.Errorf("extractor = %q, want %q", fetchResult.Extractor, "json")
	}
	if !strContains(fetchResult.Content, `"key": "value"`) {
		t.Error("content should be pretty-printed JSON")
	}
}

func TestWebFetchTool_Execute_Raw(t *testing.T) {
	html := `<html><body><h1>Test</h1></body></html>`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(html)) //nolint:errcheck
	}))
	defer server.Close()

	tool := &WebFetchTool{client: server.Client(), allowPrivateForTests: true}
	args := json.RawMessage(`{"url": "` + server.URL + `", "raw": true}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var fetchResult WebFetchResult
	if err := json.Unmarshal([]byte(result), &fetchResult); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if fetchResult.Extractor != "raw" {
		t.Errorf("extractor = %q, want %q", fetchResult.Extractor, "raw")
	}
	if fetchResult.Content != html {
		t.Errorf("content = %q, want %q", fetchResult.Content, html)
	}
}

func TestWebFetchTool_Execute_URLValidation(t *testing.T) {
	tool := NewWebFetchTool()

	tests := []struct {
		name string
		url  string
	}{
		{"file scheme", `file:///etc/passwd`},
		{"empty host", `http://`},
		{"no scheme", `example.com`},
		{"ftp scheme", `ftp://example.com`},
		{"loopback", `http://127.0.0.1/`},
		{"link local", `http://169.254.169.254/latest/meta-data/`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := json.RawMessage(`{"url": "` + tt.url + `"}`)
			_, err := tool.Execute(context.Background(), args)
			if err == nil {
				t.Error("Execute() expected error for invalid URL")
			}
		})
	}
}

func TestWebFetchTool_Execute_EmptyURL(t *testing.T) {
	tool := NewWebFetchTool()
	args := json.RawMessage(`{"url": ""}`)
	_, err := tool.Execute(context.Background(), args)
	if err == nil {
		t.Error("Execute() expected error for empty URL")
	}
}

func TestWebFetchTool_Execute_Truncation(t *testing.T) {
	longContent := make([]byte, 1000)
	for i := range longContent {
		longContent[i] = 'a'
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write(longContent) //nolint:errcheck
	}))
	defer server.Close()

	tool := &WebFetchTool{client: server.Client(), allowPrivateForTests: true}
	args := json.RawMessage(`{"url": "` + server.URL + `", "max_chars": 100}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var fetchResult WebFetchResult
	if err := json.Unmarshal([]byte(result), &fetchResult); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if !fetchResult.Truncated {
		t.Error("expected truncated = true")
	}
	if fetchResult.Length != 100 {
		t.Errorf("length = %d, want 100", fetchResult.Length)
	}
}

func TestWebFetchTool_Execute_TruncatesUnicodeByCharacter(t *testing.T) {
	const content = "a😀éz"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(content))
	}))
	defer server.Close()

	tool := &WebFetchTool{client: server.Client(), allowPrivateForTests: true}
	args, err := json.Marshal(WebFetchArgs{URL: server.URL, MaxChars: 3})
	if err != nil {
		t.Fatal(err)
	}
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var fetchResult WebFetchResult
	if err := json.Unmarshal([]byte(result), &fetchResult); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if fetchResult.Content != "a😀é" {
		t.Fatalf("content = %q, want %q", fetchResult.Content, "a😀é")
	}
	if fetchResult.Length != 3 {
		t.Fatalf("length = %d, want 3", fetchResult.Length)
	}
	if !fetchResult.Truncated {
		t.Fatal("expected truncated = true")
	}
	if !utf8.ValidString(fetchResult.Content) {
		t.Fatalf("content is not valid UTF-8: %q", fetchResult.Content)
	}
}

func TestBrokeredWebFetchToolRejectsOversizedMaxCharsBeforeRequest(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_, _ = w.Write([]byte("unexpected"))
	}))
	defer server.Close()

	tool := NewBrokeredWebFetchTool()
	tool.client = server.Client()
	tool.allowPrivateForTests = true
	args := json.RawMessage(fmt.Sprintf(`{"url":%q,"max_chars":%d}`, server.URL, brokeredWebFetchMaxChars+1))
	if _, err := tool.Execute(context.Background(), args); err == nil {
		t.Fatal("Execute() accepted max_chars above the brokered limit")
	}
	if requests != 0 {
		t.Fatalf("oversized brokered web_fetch made %d HTTP requests, want 0", requests)
	}
}

func TestBrokeredWebFetchToolRejectsOversizedURLBeforeRequest(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_, _ = w.Write([]byte("unexpected"))
	}))
	defer server.Close()

	tool := NewBrokeredWebFetchTool()
	tool.client = server.Client()
	tool.allowPrivateForTests = true
	oversizedURL := server.URL + "/?" + strings.Repeat("&", brokeredWebFetchMaxURLBytes)
	args, err := json.Marshal(WebFetchArgs{URL: oversizedURL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tool.Execute(context.Background(), args); err == nil {
		t.Fatal("Execute() accepted a URL above the brokered limit")
	}
	if requests != 0 {
		t.Fatalf("oversized brokered web_fetch URL made %d HTTP requests, want 0", requests)
	}
}

func TestBrokeredWebFetchToolAllowsSchemaMaxUnicodeURL(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	tool := NewBrokeredWebFetchTool()
	tool.client = server.Client()
	tool.allowPrivateForTests = true
	var schema struct {
		Properties struct {
			URL struct {
				MaxLength int `json:"maxLength"`
			} `json:"url"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(tool.Parameters(), &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Properties.URL.MaxLength != brokeredWebFetchMaxURLBytes/utf8.UTFMax {
		t.Fatalf("url maxLength = %d, want %d", schema.Properties.URL.MaxLength, brokeredWebFetchMaxURLBytes/utf8.UTFMax)
	}

	prefix := server.URL + "/"
	url := prefix + strings.Repeat("😀", schema.Properties.URL.MaxLength-utf8.RuneCountInString(prefix))
	if utf8.RuneCountInString(url) != schema.Properties.URL.MaxLength {
		t.Fatalf("URL characters = %d, want %d", utf8.RuneCountInString(url), schema.Properties.URL.MaxLength)
	}
	args, err := json.Marshal(WebFetchArgs{URL: url})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tool.Execute(context.Background(), args); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if requests != 1 {
		t.Fatalf("schema-max brokered web_fetch made %d HTTP requests, want 1", requests)
	}
}

func TestBrokeredWebFetchToolWorstCaseEscapingFitsMCPResultLimit(t *testing.T) {
	content := bytes.Repeat([]byte{0}, brokeredWebFetchMaxChars)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(content)
	}))
	defer server.Close()

	tool := NewBrokeredWebFetchTool()
	tool.client = server.Client()
	tool.allowPrivateForTests = true
	urlPrefix := server.URL + "/?"
	worstCaseURL := urlPrefix + strings.Repeat("&", brokeredWebFetchMaxURLBytes-len(urlPrefix))
	args, err := json.Marshal(WebFetchArgs{URL: worstCaseURL, MaxChars: brokeredWebFetchMaxChars})
	if err != nil {
		t.Fatal(err)
	}
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result) > harnessv2.MaxMCPResultBytes {
		t.Fatalf("brokered web_fetch result bytes = %d, max = %d", len(result), harnessv2.MaxMCPResultBytes)
	}
}

func TestWebFetchTool_Execute_Redirect(t *testing.T) {
	finalServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("final destination")) //nolint:errcheck
	}))
	defer finalServer.Close()

	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, finalServer.URL, http.StatusFound)
	}))
	defer redirectServer.Close()

	tool := &WebFetchTool{client: redirectServer.Client(), allowPrivateForTests: true}
	args := json.RawMessage(`{"url": "` + redirectServer.URL + `"}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var fetchResult WebFetchResult
	if err := json.Unmarshal([]byte(result), &fetchResult); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if !strContains(fetchResult.Content, "final destination") {
		t.Error("should follow redirect to final destination")
	}
}

func TestWebFetchTool_Execute_InvalidJSON(t *testing.T) {
	tool := NewWebFetchTool()
	args := json.RawMessage(invalidJSONText)
	_, err := tool.Execute(context.Background(), args)
	if err == nil {
		t.Error("Execute() expected error for invalid JSON")
	}
}

func strContains(s, substr string) bool {
	return len(s) >= len(substr) && len(substr) > 0 && strContainsCheck(s, substr)
}

func strContainsCheck(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
