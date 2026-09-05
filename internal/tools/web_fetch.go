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
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/orka-agents/orka/internal/tokenexchange"
)

const extractorRaw = "raw"

// WebFetchTool implements URL content fetching and extraction
type WebFetchTool struct {
	client               *http.Client
	allowPrivateForTests bool
	maxChars             int
	maxURLBytes          int
}

// WebFetchArgs are the arguments for the web fetch tool
type WebFetchArgs struct {
	URL      string `json:"url"`
	MaxChars int    `json:"max_chars,omitempty"`
	Raw      bool   `json:"raw,omitempty"`
}

// WebFetchResult represents the fetch result
type WebFetchResult struct {
	URL       string `json:"url"`
	Status    int    `json:"status"`
	Content   string `json:"content"`
	Length    int    `json:"length"`
	Truncated bool   `json:"truncated"`
	Extractor string `json:"extractor"`
}

const (
	maxBodySize                 = 5 * 1024 * 1024 // 5MB
	defaultWebFetchMaxChars     = 50000
	brokeredWebFetchMaxChars    = defaultWebFetchMaxChars
	brokeredWebFetchMaxURLBytes = 64 << 10
)

// NewWebFetchTool creates a new web fetch tool
func NewWebFetchTool() *WebFetchTool {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = tokenexchange.PublicEndpointDialContext
	transport.DisableKeepAlives = true
	tool := &WebFetchTool{}
	tool.client = &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects (max 5)")
			}
			return validateWebFetchURL(request.URL, false)
		},
	}
	return tool
}

// NewBrokeredWebFetchTool creates the bounded web fetch implementation exposed
// through the controller MCP broker.
func NewBrokeredWebFetchTool() *WebFetchTool {
	tool := NewWebFetchTool()
	tool.maxChars = brokeredWebFetchMaxChars
	tool.maxURLBytes = brokeredWebFetchMaxURLBytes
	return tool
}

func validateWebFetchURL(parsed *url.URL, allowPrivate bool) error {
	if parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("only http and https URLs are supported")
	}
	if parsed.Host == "" || parsed.Hostname() == "" {
		return fmt.Errorf("URL must have a host")
	}
	if parsed.User != nil {
		return fmt.Errorf("URL userinfo is not supported")
	}
	if !allowPrivate {
		if address := net.ParseIP(parsed.Hostname()); address != nil && !tokenexchange.IsPublicAddress(address) {
			return fmt.Errorf("URL must not target private, loopback, or link-local addresses")
		}
	}
	return nil
}

// Name returns the tool name
func (t *WebFetchTool) Name() string {
	return webFetchToolName
}

// Description returns the tool description
func (t *WebFetchTool) Description() string {
	return "Fetch and extract content from a URL. Returns extracted text from HTML pages, pretty-printed JSON, or raw content."
}

// Parameters returns the JSON Schema for parameters
func (t *WebFetchTool) Parameters() json.RawMessage {
	maximum := ""
	if t.maxChars > 0 {
		maximum = fmt.Sprintf(",\n\t\t\t\t\"maximum\": %d", t.maxChars)
	}
	maxLength := ""
	if t.maxURLBytes > 0 {
		maxLength = fmt.Sprintf(",\n\t\t\t\t\"maxLength\": %d", t.maxURLBytes/utf8.UTFMax)
	}
	return json.RawMessage(fmt.Sprintf(`{
		"type": "object",
		"properties": {
			"url": {
				"type": "string",
				"description": "The URL to fetch (http or https only)"%s
			},
			"max_chars": {
				"type": "integer",
				"description": "Maximum characters to return (default: 50000)",
				"default": %d%s
			},
			"raw": {
				"type": "boolean",
				"description": "Return raw HTML instead of extracted text (default: false)",
				"default": false
			}
		},
		"required": ["url"]
	}`, maxLength, defaultWebFetchMaxChars, maximum))
}

// Execute fetches the URL and extracts content
func (t *WebFetchTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	var fetchArgs WebFetchArgs
	if err := json.Unmarshal(args, &fetchArgs); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}

	if fetchArgs.URL == "" {
		return "", fmt.Errorf("url is required")
	}
	if t.maxURLBytes > 0 && len(fetchArgs.URL) > t.maxURLBytes {
		return "", fmt.Errorf("url must be no greater than %d bytes", t.maxURLBytes)
	}

	parsed, err := url.Parse(fetchArgs.URL)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}
	if err := validateWebFetchURL(parsed, t.allowPrivateForTests); err != nil {
		return "", err
	}

	if fetchArgs.MaxChars <= 0 {
		fetchArgs.MaxChars = defaultWebFetchMaxChars
	}
	if t.maxChars > 0 && fetchArgs.MaxChars > t.maxChars {
		return "", fmt.Errorf("max_chars must be no greater than %d", t.maxChars)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; OrkaBot/1.0)")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	resp, err := t.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	contentType := resp.Header.Get("Content-Type")
	var content string
	var extractor string

	switch {
	case strings.Contains(contentType, "application/json"):
		content, extractor = t.extractJSON(body)
	case strings.Contains(contentType, "text/html"):
		if fetchArgs.Raw {
			content = string(body)
			extractor = extractorRaw
		} else {
			content = extractText(body)
			extractor = "html_text"
		}
	default:
		content = string(body)
		extractor = extractorRaw
	}

	content, contentLength, truncated := truncateByRuneCount(content, fetchArgs.MaxChars)

	result := WebFetchResult{
		URL:       fetchArgs.URL,
		Status:    resp.StatusCode,
		Content:   content,
		Length:    contentLength,
		Truncated: truncated,
		Extractor: extractor,
	}

	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", err
	}

	return string(output), nil
}

func truncateByRuneCount(value string, maxRunes int) (string, int, bool) {
	runeCount := 0
	for byteOffset := range value {
		if runeCount == maxRunes {
			return value[:byteOffset], runeCount, true
		}
		runeCount++
	}
	return value, runeCount, false
}

// extractJSON pretty-prints JSON content
func (t *WebFetchTool) extractJSON(body []byte) (string, string) {
	var parsed any
	if err := json.Unmarshal(body, &parsed); err != nil {
		return string(body), extractorRaw
	}
	pretty, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		return string(body), extractorRaw
	}
	return string(pretty), "json"
}

var (
	scriptRe     = regexp.MustCompile(`(?is)<script[^>]*>.*?</script>`)
	styleRe      = regexp.MustCompile(`(?is)<style[^>]*>.*?</style>`)
	tagRe        = regexp.MustCompile(`<[^>]+>`)
	whitespaceRe = regexp.MustCompile(`\s+`)
)

// extractText strips HTML tags, scripts, styles, and collapses whitespace
func extractText(body []byte) string {
	s := string(body)
	s = scriptRe.ReplaceAllString(s, " ")
	s = styleRe.ReplaceAllString(s, " ")
	s = tagRe.ReplaceAllString(s, " ")
	s = strings.ReplaceAll(s, "&amp;", "&")
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	s = strings.ReplaceAll(s, "&quot;", `"`)
	s = strings.ReplaceAll(s, "&#39;", "'")
	s = strings.ReplaceAll(s, "&nbsp;", " ")
	s = whitespaceRe.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

// Ensure WebFetchTool implements Tool
var _ Tool = (*WebFetchTool)(nil)
