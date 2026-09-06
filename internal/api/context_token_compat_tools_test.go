/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/llm"
)

func TestContextTokenAllowedToolsFiltersInjectedProxyTools(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	handler, app := setupTestOpenAIHandler()
	authz, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{Mode: ContextTokenAuthorizationModeEnforce})
	if err != nil {
		t.Fatalf("NewContextTokenAuthorizationConfig returned error: %v", err)
	}
	handler.contextTokenAuthorization = authz

	var gotNames []string
	app.Use(NewAuthMiddleware(handler.client, AuthConfig{ContextTokens: ctxTokenConfig}))
	app.Get("/filter", func(c fiber.Ctx) error {
		compReq := &llm.CompletionRequest{}
		injectOrkaTools(compReq)
		gotNames = completionToolNames(filterCompletionToolsForContextToken(c, handler.contextTokenAuthorization, compReq.Tools))
		return c.SendStatus(http.StatusNoContent)
	})

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeToolsUse,
		"tctx": map[string]any{
			"allowedTools": []string{"file_read"},
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/filter", nil)
	req.Header.Set(TransactionTokenHeaderName, token)

	resp, err := app.Test(req, fiber.TestConfig{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("StatusCode = %d, want %d. body: %s", resp.StatusCode, http.StatusNoContent, string(body))
	}
	assertCompatToolNames(t, gotNames, []string{"file_read"})
}

func TestOpenAICompat_ContextTokenAllowedToolsFiltersInjectedProxyTools(t *testing.T) {
	captured := make(chan map[string]any, 1)
	mockAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/responses"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprint(w, `{"error":{"message":"not found","type":"invalid_request_error","code":"invalid_url"}}`)
		case strings.HasSuffix(r.URL.Path, "/chat/completions"):
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			select {
			case captured <- body:
			default:
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"id":"chatcmpl-test","object":"chat.completion","created":0,"model":"gpt-4o-mini","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockAPI.Close()

	llmProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "openai", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeOpenAI,
			DefaultModel: "gpt-4o-mini",
			BaseURL:      mockAPI.URL,
			SecretRef:    corev1alpha1.ProviderSecretRef{Name: "openai-secret", Key: "api-key"},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "openai-secret", Namespace: "default"},
		Data:       map[string][]byte{"api-key": []byte("test-key")},
	}

	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	handler, app := setupTestOpenAIHandler(llmProvider, secret)
	authz, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{Mode: ContextTokenAuthorizationModeEnforce})
	if err != nil {
		t.Fatalf("NewContextTokenAuthorizationConfig returned error: %v", err)
	}
	handler.contextTokenAuthorization = authz

	app.Use(NewAuthMiddleware(handler.client, AuthConfig{ContextTokens: ctxTokenConfig}))
	app.Post("/openai/v1/chat/completions", handler.HandleChatCompletions)

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeProvidersUse + " " + ContextTokenScopeToolsUse,
		"tctx": map[string]any{
			"allowedProviders": []string{"openai"},
			"allowedTools":     []string{"file_read"},
		},
	})
	body := []byte(`{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"hello"}],"max_tokens":100}`)
	req := httptest.NewRequest(http.MethodPost, "/openai/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, fiber.TestConfig{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want %d. body: %s", resp.StatusCode, http.StatusOK, string(respBody))
	}

	var upstream map[string]any
	select {
	case upstream = <-captured:
	default:
		t.Fatal("expected OpenAI upstream request to be captured")
	}
	assertCompatToolNames(t, compatRequestToolNames(t, upstream), []string{"file_read"})
}

func TestAnthropicCompat_ContextTokenAllowedToolsFiltersInjectedProxyTools(t *testing.T) {
	captured := make(chan map[string]any, 1)
	mockAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/messages") {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		select {
		case captured <- body:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"msg_test","type":"message","role":"assistant","model":"claude-sonnet-4-20250514","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer mockAPI.Close()

	llmProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeAnthropic,
			DefaultModel: "claude-sonnet-4-20250514",
			BaseURL:      mockAPI.URL,
			SecretRef:    corev1alpha1.ProviderSecretRef{Name: "anthropic-secret", Key: "api-key"},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "anthropic-secret", Namespace: "default"},
		Data:       map[string][]byte{"api-key": []byte("test-key")},
	}

	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	handler, app := setupTestAnthropicHandler(llmProvider, secret)
	authz, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{Mode: ContextTokenAuthorizationModeEnforce})
	if err != nil {
		t.Fatalf("NewContextTokenAuthorizationConfig returned error: %v", err)
	}
	handler.contextTokenAuthorization = authz

	app.Use(NewAuthMiddleware(handler.client, AuthConfig{ContextTokens: ctxTokenConfig}))
	app.Post("/anthropic/v1/messages", handler.HandleMessages)

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeProvidersUse + " " + ContextTokenScopeToolsUse,
		"tctx": map[string]any{
			"allowedProviders": []string{"anthropic"},
			"allowedTools":     []string{"file_read"},
		},
	})
	body := []byte(`{"model":"anthropic/claude-sonnet-4-20250514","messages":[{"role":"user","content":"hello"}],"max_tokens":100}`)
	req := httptest.NewRequest(http.MethodPost, "/anthropic/v1/messages", bytes.NewReader(body))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, fiber.TestConfig{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want %d. body: %s", resp.StatusCode, http.StatusOK, string(respBody))
	}

	var upstream map[string]any
	select {
	case upstream = <-captured:
	default:
		t.Fatal("expected Anthropic upstream request to be captured")
	}
	assertCompatToolNames(t, compatRequestToolNames(t, upstream), []string{"file_read"})
}

func compatRequestToolNames(t *testing.T, req map[string]any) []string {
	t.Helper()
	toolsAny, ok := req["tools"]
	if !ok {
		t.Fatalf("request did not include tools: %#v", req)
	}
	tools, ok := toolsAny.([]any)
	if !ok {
		t.Fatalf("request tools = %T, want []any", toolsAny)
	}

	names := make([]string, 0, len(tools))
	for _, toolAny := range tools {
		tool, ok := toolAny.(map[string]any)
		if !ok {
			t.Fatalf("tool = %T, want map[string]any", toolAny)
		}
		if name, ok := tool["name"].(string); ok {
			names = append(names, name)
			continue
		}
		function, ok := tool["function"].(map[string]any)
		if !ok {
			t.Fatalf("tool did not include name or function.name: %#v", tool)
		}
		name, ok := function["name"].(string)
		if !ok {
			t.Fatalf("tool function.name = %T, want string", function["name"])
		}
		names = append(names, name)
	}
	return names
}

func assertCompatToolNames(t *testing.T, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tool names = %#v, want %#v", got, want)
	}
}
