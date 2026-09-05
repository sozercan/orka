/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/orka-agents/orka/internal/tokenexchange"
	"github.com/orka-agents/orka/internal/workerenv"
)

// WebSearchTool implements web search functionality
type WebSearchTool struct {
	apiKey          string
	baseURL         string
	client          *http.Client
	useMockFallback bool
	maxQueryChars   int
	maxResults      int
}

// WebSearchArgs are the arguments for the web search tool
type WebSearchArgs struct {
	Query string `json:"query"`
	Limit int    `json:"limit,omitempty"`
}

// WebSearchResult represents a search result
type WebSearchResult struct {
	Title   string `json:"title"`
	URL     string `json:"url"`
	Snippet string `json:"snippet"`
}

// NewWebSearchTool creates a new web search tool
func NewWebSearchTool() *WebSearchTool {
	return &WebSearchTool{
		apiKey:          os.Getenv(workerenv.SearchAPIKey),
		baseURL:         os.Getenv(workerenv.SearchAPIURL),
		useMockFallback: true,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// NewBrokeredWebSearchTool creates a credential-free web search tool for the
// controller MCP broker. It intentionally ignores worker search configuration
// and permits only direct connections to public endpoints.
func NewBrokeredWebSearchTool() *WebSearchTool {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = tokenexchange.PublicEndpointDialContext
	transport.DisableKeepAlives = true
	return &WebSearchTool{
		maxQueryChars: brokeredWebSearchMaxQueryChars,
		maxResults:    brokeredWebSearchMaxResults,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
			CheckRedirect: func(request *http.Request, via []*http.Request) error {
				if len(via) >= 5 {
					return fmt.Errorf("too many redirects (max 5)")
				}
				return validateWebFetchURL(request.URL, false)
			},
		},
	}
}

const (
	webSearchToolName              = "web_search"
	defaultWebSearchResults        = 5
	brokeredWebSearchMaxQueryChars = 4 << 10
	brokeredWebSearchMaxResults    = 10
)

// Name returns the tool name
func (t *WebSearchTool) Name() string {
	return webSearchToolName
}

// Description returns the tool description
func (t *WebSearchTool) Description() string {
	return "Search the web for information. Use this when you need to find current information or facts."
}

// Parameters returns the JSON Schema for parameters
func (t *WebSearchTool) Parameters() json.RawMessage {
	maxLength := ""
	if t.maxQueryChars > 0 {
		maxLength = fmt.Sprintf(",\n\t\t\t\t\"maxLength\": %d", t.maxQueryChars)
	}
	maximum := ""
	if t.maxResults > 0 {
		maximum = fmt.Sprintf(",\n\t\t\t\t\"maximum\": %d", t.maxResults)
	}
	return json.RawMessage(fmt.Sprintf(`{
		"type": "object",
		"properties": {
			"query": {
				"type": "string",
				"description": "The search query"%s
			},
			"limit": {
				"type": "integer",
				"description": "Maximum number of results to return (default: 5)",
				"default": %d%s
			}
		},
		"required": ["query"]
	}`, maxLength, defaultWebSearchResults, maximum))
}

// Execute performs the web search
func (t *WebSearchTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	var searchArgs WebSearchArgs
	if err := json.Unmarshal(args, &searchArgs); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}

	if searchArgs.Query == "" {
		return "", fmt.Errorf("query is required")
	}
	if t.maxQueryChars > 0 && utf8.RuneCountInString(searchArgs.Query) > t.maxQueryChars {
		return "", fmt.Errorf("query must be no greater than %d characters", t.maxQueryChars)
	}

	if searchArgs.Limit <= 0 {
		searchArgs.Limit = defaultWebSearchResults
	}
	if t.maxResults > 0 && searchArgs.Limit > t.maxResults {
		return "", fmt.Errorf("limit must be no greater than %d", t.maxResults)
	}

	// If no API configured, use DuckDuckGo fallback
	if t.baseURL == "" {
		return t.duckDuckGoSearch(ctx, searchArgs)
	}

	// Build request URL
	reqURL := fmt.Sprintf("%s?q=%s&limit=%d",
		t.baseURL,
		url.QueryEscape(searchArgs.Query),
		searchArgs.Limit,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	if t.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+t.apiKey)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("search request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return "", fmt.Errorf("search API returned %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	return string(body), nil
}

// mockSearch returns a mock search response when no API is configured
func (t *WebSearchTool) mockSearch(args WebSearchArgs) (string, error) {
	results := []WebSearchResult{
		{
			Title:   "Search Result 1",
			URL:     "https://example.com/result1",
			Snippet: fmt.Sprintf("This is a mock search result for query: %s", args.Query),
		},
		{
			Title:   "Search Result 2",
			URL:     "https://example.com/result2",
			Snippet: "Web search API not configured. Set SEARCH_API_URL and SEARCH_API_KEY environment variables.",
		},
	}

	output, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return "", err
	}

	return string(output), nil
}

var (
	ddgLinkRe    = regexp.MustCompile(`<a[^>]+class="result__a"[^>]*href="([^"]*)"[^>]*>(.*?)</a>`)
	ddgSnippetRe = regexp.MustCompile(`<a[^>]+class="result__snippet"[^>]*>(.*?)</a>`)
	ddgUddgRe    = regexp.MustCompile(`uddg=([^&]+)`)
	ddgTagRe     = regexp.MustCompile(`<[^>]+>`)
)

// duckDuckGoSearch performs a search via DuckDuckGo HTML
func (t *WebSearchTool) duckDuckGoSearch(ctx context.Context, args WebSearchArgs) (string, error) {
	ddgURL := fmt.Sprintf("https://html.duckduckgo.com/html/?q=%s", url.QueryEscape(args.Query))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ddgURL, nil)
	if err != nil {
		return t.handleDuckDuckGoFailure(args, fmt.Errorf("create DuckDuckGo request: %w", err))
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := t.client.Do(req)
	if err != nil {
		return t.handleDuckDuckGoFailure(args, fmt.Errorf("DuckDuckGo request failed: %w", err))
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return t.handleDuckDuckGoFailure(args, fmt.Errorf("DuckDuckGo returned HTTP %d", resp.StatusCode))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return t.handleDuckDuckGoFailure(args, fmt.Errorf("read DuckDuckGo response: %w", err))
	}

	results := parseDDGResults(string(body), args.Limit)
	if len(results) == 0 {
		return t.handleDuckDuckGoFailure(args, fmt.Errorf("DuckDuckGo returned no parseable results"))
	}

	output, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func (t *WebSearchTool) handleDuckDuckGoFailure(args WebSearchArgs, err error) (string, error) {
	if t.useMockFallback {
		return t.mockSearch(args)
	}
	return "", err
}

// parseDDGResults extracts search results from DuckDuckGo HTML
func parseDDGResults(html string, limit int) []WebSearchResult {
	linkMatches := ddgLinkRe.FindAllStringSubmatch(html, -1)
	snippetMatches := ddgSnippetRe.FindAllStringSubmatch(html, -1)

	results := make([]WebSearchResult, 0, len(linkMatches))
	for i, m := range linkMatches {
		if len(results) >= limit {
			break
		}
		rawURL := m[1]
		title := stripHTMLTags(m[2])

		// Decode DDG redirect URL
		actualURL := decodeDDGURL(rawURL)
		if actualURL == "" || title == "" {
			continue
		}

		snippet := ""
		if i < len(snippetMatches) && len(snippetMatches[i]) > 1 {
			snippet = stripHTMLTags(snippetMatches[i][1])
		}

		results = append(results, WebSearchResult{
			Title:   title,
			URL:     actualURL,
			Snippet: snippet,
		})
	}
	return results
}

// decodeDDGURL extracts the actual URL from a DuckDuckGo redirect URL
func decodeDDGURL(rawURL string) string {
	matches := ddgUddgRe.FindStringSubmatch(rawURL)
	if len(matches) > 1 {
		decoded, err := url.QueryUnescape(matches[1])
		if err != nil {
			return rawURL
		}
		return decoded
	}
	// Not a redirect URL, return as-is if it looks like a URL
	if strings.HasPrefix(rawURL, "http") {
		return rawURL
	}
	return ""
}

// stripHTMLTags removes HTML tags from a string
func stripHTMLTags(s string) string {
	s = ddgTagRe.ReplaceAllString(s, "")
	s = strings.ReplaceAll(s, "&amp;", "&")
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	s = strings.ReplaceAll(s, "&quot;", `"`)
	s = strings.ReplaceAll(s, "&#39;", "'")
	s = strings.ReplaceAll(s, "&nbsp;", " ")
	return strings.TrimSpace(s)
}

// Ensure WebSearchTool implements Tool
var _ Tool = (*WebSearchTool)(nil)
