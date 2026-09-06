/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package openai

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/orka-agents/orka/internal/llm"
)

const (
	testProviderOpenAI = "openai"
	testToolNameSearch = "search"
	testToolArguments  = `{"q":"test"}`
	testResponsesPath  = "/responses"
	testFallbackWorks  = "Fallback works!"
	testFallbackStream = "Fallback"
	testCopilotBaseURL = "https://api.githubcopilot.com"
)

func TestNewProvider(t *testing.T) {
	tests := []struct {
		name    string
		config  llm.ProviderConfig
		wantErr bool
	}{
		{
			name: "with API key",
			config: llm.ProviderConfig{
				APIKey: "test-api-key",
			},
			wantErr: false,
		},
		{
			name: "without API key",
			config: llm.ProviderConfig{
				APIKey: "",
			},
			wantErr: true,
		},
		{
			name: "with base URL",
			config: llm.ProviderConfig{
				APIKey:  "test-api-key",
				BaseURL: "https://custom.api.com",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := NewProvider(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProvider() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && provider == nil {
				t.Error("NewProvider() returned nil provider")
			}
		})
	}
}

func TestProvider_Name(t *testing.T) {
	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test"})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	if name := provider.Name(); name != testProviderOpenAI {
		t.Errorf("Name() = %v, want openai", name)
	}
}

func TestNewProvider_APIKeyRequired(t *testing.T) {
	_, err := NewProvider(llm.ProviderConfig{})
	if err == nil {
		t.Error("NewProvider() expected error for missing API key")
	}
	if err != llm.ErrAPIKeyRequired {
		t.Errorf("NewProvider() error = %v, want ErrAPIKeyRequired", err)
	}
}

func TestProvider_Implements_Interface(t *testing.T) {
	// Verify that Provider implements llm.Provider at compile time
	var _ llm.Provider = (*Provider)(nil)
}

func TestProvider_ConfigStorage(t *testing.T) {
	config := llm.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: "https://api.example.com",
	}

	provider, err := NewProvider(config)
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	if provider == nil {
		t.Fatal("NewProvider() returned nil provider")
	}
}

func TestProvider_ClientNotNil(t *testing.T) {
	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test"})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	// Verify the provider was created (client is a value type, always initialized)
	if provider == nil {
		t.Error("provider should not be nil")
	}
}

func TestIsUnsupportedAPIError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"404 in message", fmt.Errorf("got 404 Not Found"), true},
		{"403 unsupported responses message", fmt.Errorf("got 403 Forbidden: model does not support /responses"), true},
		{"403 provider unsupported responses", &llm.ProviderError{Provider: testProviderOpenAI, StatusCode: http.StatusForbidden, Message: "model does not support /responses"}, true},
		{"403 provider forbidden", &llm.ProviderError{Provider: testProviderOpenAI, StatusCode: http.StatusForbidden, Message: "forbidden"}, false},
		{"403 bare responses forbidden", &llm.ProviderError{Provider: testProviderOpenAI, StatusCode: http.StatusForbidden, Message: `POST "https://proxy.example/v1/responses": 403 Forbidden`}, false},
		{"403 Forbidden in message", fmt.Errorf("got 403 Forbidden"), false},
		{"403 provider error", &llm.ProviderError{StatusCode: http.StatusForbidden, Message: "forbidden"}, false},
		{
			"403 provider error with 404-like URL text",
			&llm.ProviderError{
				StatusCode: http.StatusForbidden,
				Message:    `POST "http://127.0.0.1:54040/responses": 403 Forbidden`,
			},
			false,
		},
		{
			"400 provider error with unsupported code",
			&llm.ProviderError{
				StatusCode: http.StatusBadRequest,
				Message:    `{"error":{"message":"Unsupported","type":"invalid_request_error","code":"unsupported_api_for_model"}}`,
			},
			true,
		},
		{"Not Found in message", fmt.Errorf("Not Found"), true},
		{"invalid_url in message", fmt.Errorf("invalid_url"), true},
		{"unsupported_api_for_model in message", fmt.Errorf("unsupported_api_for_model"), true},
		{"normal error", fmt.Errorf("connection refused"), false},
		{"rate limited", fmt.Errorf("rate limited 429"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUnsupportedAPIError(tt.err); got != tt.want {
				t.Errorf("isUnsupportedAPIError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProvider_ShouldFallbackToChatCompletions_CustomBareResponsesForbidden(t *testing.T) {
	err := &llm.ProviderError{
		Provider:   testProviderOpenAI,
		StatusCode: http.StatusForbidden,
		Message:    `POST "https://proxy.example/v1/responses": 403 Forbidden`,
	}

	customProvider := &Provider{allowBareResponsesForbiddenFallback: true}
	if !customProvider.shouldFallbackToChatCompletions(err) {
		t.Fatal("expected custom OpenAI-compatible provider to fallback on bare /responses 403")
	}

	officialProvider := &Provider{}
	if officialProvider.shouldFallbackToChatCompletions(err) {
		t.Fatal("expected official OpenAI provider to keep bare /responses 403 as an error")
	}
}

func TestIsCustomOpenAIBaseURL(t *testing.T) {
	tests := []struct {
		name         string
		providerType string
		baseURL      string
		want         bool
	}{
		{"default OpenAI", "openai", "", false},
		{"official OpenAI", "openai", "https://api.openai.com/v1", false},
		{"custom OpenAI-compatible", "openai", "http://copilot-proxy.default.svc.cluster.local:1337/v1", true},
		{"azure endpoint", "azure-openai", "https://example.openai.azure.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isCustomOpenAIBaseURL(tt.providerType, tt.baseURL); got != tt.want {
				t.Errorf("isCustomOpenAIBaseURL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsCopilotResponsesForbiddenError(t *testing.T) {
	provider := &Provider{baseURL: testCopilotBaseURL}
	if !provider.isCopilotResponsesForbiddenError(&llm.ProviderError{StatusCode: http.StatusForbidden, Message: "forbidden"}) {
		t.Fatal("expected Copilot Responses 403 to be eligible for chat fallback")
	}

	provider = &Provider{baseURL: "https://api.openai.com/v1"}
	if provider.isCopilotResponsesForbiddenError(&llm.ProviderError{StatusCode: http.StatusForbidden, Message: "forbidden"}) {
		t.Fatal("expected non-Copilot 403 to remain an authorization failure")
	}

	if provider.isCopilotResponsesForbiddenError(&llm.ProviderError{
		StatusCode: http.StatusForbidden,
		Message:    `POST "https://api.openai.com/v1/responses": 403 Forbidden: copilot access denied`,
	}) {
		t.Fatal("expected non-Copilot 403 message text not to trigger Copilot fallback")
	}
}

func TestConvertInputItems(t *testing.T) {
	tests := []struct {
		name     string
		messages []llm.Message
		wantLen  int
	}{
		{"empty", nil, 0},
		{"user message", []llm.Message{{Role: "user", Content: "hi"}}, 1},
		{"system message", []llm.Message{{Role: "system", Content: "be helpful"}}, 1},
		{
			"assistant with tool calls",
			[]llm.Message{{
				Role:    "assistant",
				Content: "thinking",
				ToolCalls: []llm.ToolCall{
					{ID: "tc1", Name: testToolNameSearch, Arguments: json.RawMessage(testToolArguments)},
				},
			}},
			2, // function_call + assistant content message
		},
		{
			"assistant with tool calls no content",
			[]llm.Message{{
				Role: "assistant",
				ToolCalls: []llm.ToolCall{
					{ID: "tc1", Name: testToolNameSearch, Arguments: json.RawMessage(testToolArguments)},
				},
			}},
			1, // just function_call
		},
		{
			"tool result",
			[]llm.Message{{Role: "tool", Content: "result", ToolCallID: "tc1"}},
			1,
		},
		{
			"full conversation",
			[]llm.Message{
				{Role: "user", Content: testToolNameSearch},
				{Role: "assistant", ToolCalls: []llm.ToolCall{{ID: "tc1", Name: "fn", Arguments: json.RawMessage(`{}`)}}},
				{Role: "tool", Content: "data", ToolCallID: "tc1"},
				{Role: "assistant", Content: "done"},
			},
			4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertInputItems(tt.messages)
			if len(result) != tt.wantLen {
				t.Errorf("convertInputItems() returned %d items, want %d", len(result), tt.wantLen)
			}
		})
	}
}

func TestConvertResponsesTools(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		result := convertResponsesTools(nil)
		if len(result) != 0 {
			t.Errorf("expected 0 tools, got %d", len(result))
		}
	})

	t.Run("single tool", func(t *testing.T) {
		tools := []llm.Tool{
			{
				Name:        testToolNameSearch,
				Description: "Search the web",
				Parameters:  json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`),
			},
		}
		result := convertResponsesTools(tools)
		if len(result) != 1 {
			t.Fatalf("expected 1 tool, got %d", len(result))
		}
		if result[0].OfFunction == nil {
			t.Fatal("expected OfFunction to be set")
		}
		if result[0].OfFunction.Name != testToolNameSearch {
			t.Errorf("expected name 'search', got %q", result[0].OfFunction.Name)
		}
	})

	t.Run("multiple tools", func(t *testing.T) {
		tools := []llm.Tool{
			{Name: "a", Description: "tool a", Parameters: json.RawMessage(`{"type":"object"}`)},
			{Name: "b", Description: "tool b", Parameters: json.RawMessage(`{"type":"object"}`)},
		}
		result := convertResponsesTools(tools)
		if len(result) != 2 {
			t.Errorf("expected 2 tools, got %d", len(result))
		}
	})
}

func TestConvertMessages(t *testing.T) {
	tests := []struct {
		name         string
		messages     []llm.Message
		systemPrompt string
		wantLen      int
	}{
		{"empty no system", nil, "", 0},
		{"empty with system", nil, "be helpful", 1},
		{
			"user message",
			[]llm.Message{{Role: "user", Content: "hi"}},
			"",
			1,
		},
		{
			"system in messages",
			[]llm.Message{{Role: "system", Content: "sys"}},
			"",
			1,
		},
		{
			"assistant with tool calls",
			[]llm.Message{{
				Role: "assistant",
				ToolCalls: []llm.ToolCall{
					{ID: "tc1", Name: testToolNameSearch, Arguments: json.RawMessage(testToolArguments)},
				},
			}},
			"",
			1,
		},
		{
			"tool result",
			[]llm.Message{{Role: "tool", Content: "data", ToolCallID: "tc1"}},
			"",
			1,
		},
		{
			"full with system prompt",
			[]llm.Message{
				{Role: "user", Content: "hi"},
				{Role: "assistant", Content: "hello"},
			},
			"system",
			3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertMessages(tt.messages, tt.systemPrompt)
			if len(result) != tt.wantLen {
				t.Errorf("convertMessages() returned %d messages, want %d", len(result), tt.wantLen)
			}
		})
	}
}

func TestConvertChatTools(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		result := convertChatTools(nil)
		if len(result) != 0 {
			t.Errorf("expected 0 tools, got %d", len(result))
		}
	})

	t.Run("single tool", func(t *testing.T) {
		tools := []llm.Tool{
			{
				Name:        testToolNameSearch,
				Description: "Search",
				Parameters:  json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`),
			},
		}
		result := convertChatTools(tools)
		if len(result) != 1 {
			t.Errorf("expected 1 tool, got %d", len(result))
		}
	})
}

func TestToProviderError(t *testing.T) {
	t.Run("generic error", func(t *testing.T) {
		err := fmt.Errorf("something wrong")
		pe := toProviderError(err)
		if pe.Provider != testProviderOpenAI {
			t.Errorf("expected provider 'openai', got %q", pe.Provider)
		}
		if pe.Message != "something wrong" {
			t.Errorf("expected message 'something wrong', got %q", pe.Message)
		}
		if pe.StatusCode != 0 {
			t.Errorf("expected status code 0, got %d", pe.StatusCode)
		}
	})
}

func TestNewProvider_AzureOpenAI(t *testing.T) {
	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:       "test-key",
		BaseURL:      "https://myresource.openai.azure.com",
		ProviderType: "azure-openai",
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	if provider == nil {
		t.Error("provider should not be nil")
	}
}

func TestNewProvider_AzureOpenAI_CustomAPIVersion(t *testing.T) {
	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:          "test-key",
		BaseURL:         "https://myresource.openai.azure.com",
		ProviderType:    "azure-openai",
		AzureAPIVersion: "2024-06-01",
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	if provider == nil {
		t.Error("provider should not be nil")
	}
}

func TestComplete_ChatCompletions_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Return 404 for Responses API probe, then handle Chat Completions
		if r.URL.Path == testResponsesPath {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"message":"Not Found","type":"invalid_request_error","code":"invalid_url"}}`) //nolint:errcheck
			return
		}

		//nolint:errcheck // multiline test response
		_, _ = fmt.Fprint(w, `{
			"id": "chatcmpl-123",
			"object": "chat.completion",
			"model": "gpt-4",
			"choices": [{
				"index": 0,
				"message": {"role": "assistant", "content": "Hello!"},
				"finish_reason": "stop"
			}],
			"usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
		}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	// Force chat completions mode
	provider.mode.Store(int32(apiModeChatCompletions))

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.Content != "Hello!" {
		t.Errorf("expected content 'Hello!', got %q", resp.Content)
	}
	if resp.InputTokens != 10 {
		t.Errorf("expected 10 input tokens, got %d", resp.InputTokens)
	}
}

func TestComplete_ChatCompletions_Refusal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{
			"id": "chatcmpl-refusal",
			"object": "chat.completion",
			"model": "gpt-4",
			"choices": [{
				"index": 0,
				"message": {
					"role": "assistant",
					"content": "",
					"refusal": "Cannot comply",
					"tool_calls": [{
						"id": "call_1",
						"type": "function",
						"function": {"name": "search", "arguments": "{}"}
					}]
				},
				"finish_reason": "tool_calls"
			}],
			"usage": {"prompt_tokens": 10, "completion_tokens": 1, "total_tokens": 11}
		}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeChatCompletions))

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.StopReason != stopReasonRefusal {
		t.Fatalf("StopReason = %q, want refusal", resp.StopReason)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("ToolCalls = %d, want 1", len(resp.ToolCalls))
	}
}

func TestComplete_ChatCompletions_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"error":{"message":"server error","type":"server_error"}}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	// Force chat completions mode
	provider.mode.Store(int32(apiModeChatCompletions))

	_, err = provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestComplete_ChatCompletions_WithToolCalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		//nolint:errcheck // multiline test response
		fmt.Fprint(w, `{
			"id": "chatcmpl-456",
			"object": "chat.completion",
			"model": "gpt-4",
			"choices": [{
				"index": 0,
				"message": {
					"role": "assistant",
					"content": "",
					"tool_calls": [{
						"id": "call_1",
						"type": "function",
						"function": {"name": "search", "arguments": "{\"q\":\"test\"}"}
					}]
				},
				"finish_reason": "tool_calls"
			}],
			"usage": {"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30}
		}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	provider.mode.Store(int32(apiModeChatCompletions))

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "search for test"}},
		Tools: []llm.Tool{
			{Name: testToolNameSearch, Description: testToolNameSearch, Parameters: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`)},
		},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(resp.ToolCalls))
	}
	if resp.ToolCalls[0].Name != testToolNameSearch {
		t.Errorf("expected tool call name 'search', got %q", resp.ToolCalls[0].Name)
	}
}

func TestComplete_ChatCompletions_WithLegacyFunctionCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{
			"id": "chatcmpl-legacy",
			"object": "chat.completion",
			"model": "gpt-4",
			"choices": [{
				"index": 0,
				"message": {
					"role": "assistant",
					"content": "",
					"function_call": {"name": "search", "arguments": "{\"q\":\"test\"}"}
				},
				"finish_reason": "function_call"
			}],
			"usage": {"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30}
		}`)
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeChatCompletions))

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "search for test"}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("ToolCalls = %d, want 1", len(resp.ToolCalls))
	}
	if resp.ToolCalls[0].ID != "chatcmpl-legacy_function_call" {
		t.Fatalf("tool call ID = %q", resp.ToolCalls[0].ID)
	}
	if resp.ToolCalls[0].Name != testToolNameSearch {
		t.Fatalf("tool call name = %q", resp.ToolCalls[0].Name)
	}
	if string(resp.ToolCalls[0].Arguments) != testToolArguments {
		t.Fatalf("tool call arguments = %q", resp.ToolCalls[0].Arguments)
	}
	if got := llm.NormalizeCompletionOutcome(resp); got != llm.CompletionOutcomeToolCalls {
		t.Fatalf("outcome = %q, want %q", got, llm.CompletionOutcomeToolCalls)
	}
}

func TestComplete_AutoDetect_FallbackToChatCompletions(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == testResponsesPath {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"message":"Not Found","type":"invalid_request_error","code":"invalid_url"}}`) //nolint:errcheck
			return
		}

		//nolint:errcheck // multiline test response
		fmt.Fprintf(w, `{
			"id": "chatcmpl-789",
			"object": "chat.completion",
			"model": "gpt-4",
			"choices": [{
				"index": 0,
				"message": {"role": "assistant", "content": %q},
				"finish_reason": "stop"
			}],
			"usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}
		}`, testFallbackWorks) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.Content != testFallbackWorks {
		t.Errorf("expected content %q, got %q", testFallbackWorks, resp.Content)
	}
	// Should have tried responses first, then chat completions
	if apiMode(provider.mode.Load()) != apiModeChatCompletions {
		t.Error("expected mode to be set to apiModeChatCompletions after fallback")
	}
}

func TestComplete_AutoDetect_FallbackToChatCompletions_OnUnsupported403(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == testResponsesPath {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":{"message":"model does not support /responses","type":"invalid_request_error"}}`) //nolint:errcheck
			return
		}

		//nolint:errcheck // multiline test response
		fmt.Fprintf(w, `{
			"id": "chatcmpl-403-fallback",
			"object": "chat.completion",
			"model": "gpt-4",
			"choices": [{
				"index": 0,
				"message": {"role": "assistant", "content": %q},
				"finish_reason": "stop"
			}],
			"usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}
		}`, testFallbackWorks) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.Content != testFallbackWorks {
		t.Errorf("expected content %q, got %q", testFallbackWorks, resp.Content)
	}
	if apiMode(provider.mode.Load()) != apiModeChatCompletions {
		t.Error("expected mode to be set to apiModeChatCompletions after unsupported Responses 403")
	}
}

func TestComplete_AutoDetect_FallbackToChatCompletions_OnCustomBare403(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == testResponsesPath {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		//nolint:errcheck // multiline test response
		fmt.Fprintf(w, `{
			"id": "chatcmpl-bare-403-fallback",
			"object": "chat.completion",
			"model": "gpt-4",
			"choices": [{
				"index": 0,
				"message": {"role": "assistant", "content": %q},
				"finish_reason": "stop"
			}],
			"usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}
		}`, testFallbackWorks) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.Content != testFallbackWorks {
		t.Errorf("expected content %q, got %q", testFallbackWorks, resp.Content)
	}
	if apiMode(provider.mode.Load()) != apiModeChatCompletions {
		t.Error("expected mode to be set to apiModeChatCompletions after custom bare Responses 403")
	}
}

func TestComplete_AutoDetect_FallbackToChatCompletionsOnForbiddenResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == testResponsesPath {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":{"message":"Forbidden","type":"forbidden"}}`) //nolint:errcheck
			return
		}

		//nolint:errcheck // multiline test response
		fmt.Fprint(w, `{
			"id": "chatcmpl-forbidden-fallback",
			"object": "chat.completion",
			"model": "gpt-4",
			"choices": [{
				"index": 0,
				"message": {"role": "assistant", "content": "Fallback after forbidden"},
				"finish_reason": "stop"
			}],
			"usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}
		}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.baseURL = testCopilotBaseURL

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.Content != "Fallback after forbidden" {
		t.Errorf("expected content 'Fallback after forbidden', got %q", resp.Content)
	}
	if apiMode(provider.mode.Load()) != apiModeChatCompletions {
		t.Error("expected mode to be set to apiModeChatCompletions after fallback")
	}
}

func TestComplete_ResponsesModeFallbackToChatCompletionsOnForbiddenResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == testResponsesPath {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":{"message":"Forbidden","type":"forbidden"}}`) //nolint:errcheck
			return
		}

		//nolint:errcheck // multiline test response
		fmt.Fprint(w, `{
			"id": "chatcmpl-cached-forbidden-fallback",
			"object": "chat.completion",
			"model": "gpt-4",
			"choices": [{
				"index": 0,
				"message": {"role": "assistant", "content": "Cached fallback after forbidden"},
				"finish_reason": "stop"
			}],
			"usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}
		}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))
	provider.baseURL = testCopilotBaseURL

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.Content != "Cached fallback after forbidden" {
		t.Errorf("expected content 'Cached fallback after forbidden', got %q", resp.Content)
	}
	if apiMode(provider.mode.Load()) != apiModeResponses {
		t.Error("expected pinned Responses mode to remain unchanged after scoped Copilot fallback")
	}
}

func TestComplete_AutoDetect_DoesNotFallbackOnGenericForbiddenResponses(t *testing.T) {
	var requestPaths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPaths = append(requestPaths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"error":{"message":"Forbidden","type":"forbidden"}}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	_, err = provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected generic 403 to be returned, not converted into chat fallback")
	}
	if apiMode(provider.mode.Load()) != apiModeUnknown {
		t.Error("expected generic 403 to leave API mode unknown")
	}
	if len(requestPaths) != 1 || requestPaths[0] != testResponsesPath {
		t.Fatalf("request paths = %v, want only %s", requestPaths, testResponsesPath)
	}
}

func TestStream_ChatCompletions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" there\"},\"finish_reason\":null}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	provider.mode.Store(int32(apiModeChatCompletions))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var content strings.Builder
	var inputTokens, outputTokens int
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected error: %v", chunk.Error)
		}
		if chunk.InputTokens > 0 || chunk.OutputTokens > 0 {
			inputTokens = chunk.InputTokens
			outputTokens = chunk.OutputTokens
		}
		content.WriteString(chunk.Content) //nolint:errcheck
	}
	if inputTokens != 10 || outputTokens != 5 {
		t.Fatalf("stream usage = input:%d output:%d, want input:10 output:5", inputTokens, outputTokens)
	}
	if got := content.String(); got != "Hi there" {
		t.Errorf("expected 'Hi there', got %q", got)
	}
}

func TestStream_ChatCompletions_NormalizesTerminalOutcome(t *testing.T) {
	tests := []struct {
		name       string
		chunk      string
		wantReason string
	}{
		{
			name:       "refusal overrides tool calls",
			chunk:      `{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"refusal":"Cannot comply"},"finish_reason":"tool_calls"}]}`,
			wantReason: stopReasonRefusal,
		},
		{
			name:       "blank stop is incomplete",
			chunk:      `{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			wantReason: stopReasonIncomplete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", tt.chunk)
			}))
			defer server.Close()

			provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
			if err != nil {
				t.Fatalf("NewProvider() error = %v", err)
			}
			provider.mode.Store(int32(apiModeChatCompletions))

			ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
				Model:    "gpt-4",
				Messages: []llm.Message{{Role: "user", Content: "hi"}},
			})
			if err != nil {
				t.Fatalf("Stream() error = %v", err)
			}

			var stopReason string
			for chunk := range ch {
				if chunk.Error != nil {
					t.Fatalf("stream error = %v", chunk.Error)
				}
				if chunk.Done {
					stopReason = chunk.StopReason
				}
			}
			if stopReason != tt.wantReason {
				t.Fatalf("stop reason = %q, want %q", stopReason, tt.wantReason)
			}
		})
	}
}

func TestStream_ChatCompletions_LegacyFunctionCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"id\":\"chatcmpl-legacy\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"function_call\":{\"name\":\"se\",\"arguments\":\"{\\\"q\\\":\"}},\"finish_reason\":null}]}\n\n")
		_, _ = fmt.Fprint(w, "data: {\"id\":\"chatcmpl-legacy\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"function_call\":{\"name\":\"arch\",\"arguments\":\"\\\"test\\\"}\"}},\"finish_reason\":null}]}\n\n")
		_, _ = fmt.Fprint(w, "data: {\"id\":\"chatcmpl-legacy\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"function_call\"}]}\n\n")
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeChatCompletions))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "search for test"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var toolCalls []llm.ToolCall
	var stopReason string
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("stream error = %v", chunk.Error)
		}
		if chunk.ToolCall != nil {
			toolCalls = append(toolCalls, *chunk.ToolCall)
		}
		if chunk.Done {
			stopReason = chunk.StopReason
		}
	}
	if len(toolCalls) != 1 {
		t.Fatalf("tool calls = %d, want 1", len(toolCalls))
	}
	if toolCalls[0].ID != "chatcmpl-legacy_function_call" || toolCalls[0].Name != testToolNameSearch {
		t.Fatalf("tool call = %#v", toolCalls[0])
	}
	if string(toolCalls[0].Arguments) != testToolArguments {
		t.Fatalf("tool call arguments = %q", toolCalls[0].Arguments)
	}
	if stopReason != stopReasonToolCalls {
		t.Fatalf("stop reason = %q, want %q", stopReason, stopReasonToolCalls)
	}
}

// responsesJSON returns a valid Responses API JSON body.
func responsesJSON(text string, inputTokens, outputTokens int) string {
	return fmt.Sprintf(`{
		"id": "resp_test",
		"object": "response",
		"created_at": 1700000000,
		"status": "completed",
		"model": "gpt-4",
		"output": [
			{
				"id": "msg_1",
				"type": "message",
				"role": "assistant",
				"status": "completed",
				"content": [{"type": "output_text", "text": %q}]
			}
		],
		"usage": {"input_tokens": %d, "output_tokens": %d, "total_tokens": %d}
	}`, text, inputTokens, outputTokens, inputTokens+outputTokens)
}

func TestConvertResponsesTextFormat(t *testing.T) {
	tests := []struct {
		name string
		rf   *llm.ResponseFormat
	}{
		{"json_object", &llm.ResponseFormat{Type: "json_object"}},
		{
			"json_schema with all fields",
			&llm.ResponseFormat{
				Type: "json_schema",
				JSONSchema: &llm.JSONSchemaFormat{
					Name:        "test_schema",
					Schema:      map[string]any{"type": "object"},
					Strict:      new(true),
					Description: "A test schema",
				},
			},
		},
		{
			"json_schema without strict and description",
			&llm.ResponseFormat{
				Type: "json_schema",
				JSONSchema: &llm.JSONSchemaFormat{
					Name:   "test_schema",
					Schema: map[string]any{"type": "object"},
				},
			},
		},
		{"json_schema nil schema", &llm.ResponseFormat{Type: "json_schema"}},
		{"text type", &llm.ResponseFormat{Type: "text"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = convertResponsesTextFormat(tt.rf)
		})
	}
}

func TestConvertChatResponseFormat(t *testing.T) {
	tests := []struct {
		name string
		rf   *llm.ResponseFormat
	}{
		{"json_object", &llm.ResponseFormat{Type: "json_object"}},
		{
			"json_schema with all fields",
			&llm.ResponseFormat{
				Type: "json_schema",
				JSONSchema: &llm.JSONSchemaFormat{
					Name:        "test_schema",
					Schema:      map[string]any{"type": "object"},
					Strict:      new(true),
					Description: "A test schema",
				},
			},
		},
		{
			"json_schema without optional fields",
			&llm.ResponseFormat{
				Type: "json_schema",
				JSONSchema: &llm.JSONSchemaFormat{
					Name:   "test_schema",
					Schema: map[string]any{"type": "object"},
				},
			},
		},
		{"json_schema nil schema", &llm.ResponseFormat{Type: "json_schema"}},
		{"text type", &llm.ResponseFormat{Type: "text"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = convertChatResponseFormat(tt.rf)
		})
	}
}

func TestComplete_ResponsesAPI_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, responsesJSON("Hello from Responses!", 10, 5)) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.Content != "Hello from Responses!" {
		t.Errorf("expected 'Hello from Responses!', got %q", resp.Content)
	}
	if resp.StopReason != stopReasonStop {
		t.Errorf("expected stop reason 'stop', got %q", resp.StopReason)
	}
	if resp.InputTokens != 10 {
		t.Errorf("expected 10 input tokens, got %d", resp.InputTokens)
	}
	if resp.OutputTokens != 5 {
		t.Errorf("expected 5 output tokens, got %d", resp.OutputTokens)
	}
}

func TestComplete_ResponsesAPI_NormalizesStructuredOutcomes(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		output     string
		wantReason string
	}{
		{
			name:   "refusal",
			status: "completed",
			output: `{
				"id": "msg_1",
				"type": "message",
				"role": "assistant",
				"status": "completed",
				"content": [{"type": "refusal", "refusal": "Cannot comply"}]
			}`,
			wantReason: stopReasonRefusal,
		},
		{
			name:   "incomplete output item",
			status: "completed",
			output: `{
				"id": "msg_1",
				"type": "message",
				"role": "assistant",
				"status": "incomplete",
				"content": [{"type": "output_text", "text": "partial"}]
			}`,
			wantReason: stopReasonIncomplete,
		},
		{
			name:   "failed response with partial tool call",
			status: "failed",
			output: `{
				"id": "fc_1",
				"type": "function_call",
				"status": "incomplete",
				"call_id": "call_1",
				"name": "search",
				"arguments": "{}"
			}`,
			wantReason: "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{
					"id": "resp_test",
					"object": "response",
					"created_at": 1700000000,
					"status": %q,
					"model": "gpt-4",
					"output": [%s],
					"usage": {"input_tokens": 10, "output_tokens": 1, "total_tokens": 11}
				}`, tt.status, tt.output) //nolint:errcheck
			}))
			defer server.Close()

			provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
			if err != nil {
				t.Fatalf("NewProvider() error = %v", err)
			}
			provider.mode.Store(int32(apiModeResponses))

			resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
				Model:    "gpt-4",
				Messages: []llm.Message{{Role: "user", Content: "hi"}},
			})
			if err != nil {
				t.Fatalf("Complete() error = %v", err)
			}
			if resp.StopReason != tt.wantReason {
				t.Fatalf("StopReason = %q, want %q", resp.StopReason, tt.wantReason)
			}
		})
	}
}

func TestNormalizeResponsesStopReason(t *testing.T) {
	tests := []struct {
		name              string
		status            string
		output            []responses.ResponseOutputItemUnion
		blankIsIncomplete bool
		want              string
	}{
		{name: "completed text", status: "completed", want: stopReasonStop},
		{name: "blank completed", status: "completed", blankIsIncomplete: true, want: stopReasonIncomplete},
		{
			name:   "refusal",
			status: "completed",
			output: []responses.ResponseOutputItemUnion{{
				Type:    "message",
				Content: []responses.ResponseOutputMessageContentUnion{{Type: stopReasonRefusal}},
			}},
			want: stopReasonRefusal,
		},
		{
			name:   "incomplete item",
			status: "completed",
			output: []responses.ResponseOutputItemUnion{{
				Type:   "message",
				Status: stopReasonIncomplete,
			}},
			want: stopReasonIncomplete,
		},
		{
			name:   "tool call",
			status: "completed",
			output: []responses.ResponseOutputItemUnion{{
				Type: eventTypeFunctionCall,
			}},
			blankIsIncomplete: true,
			want:              stopReasonToolCalls,
		},
		{
			name:   "failed response",
			status: "failed",
			output: []responses.ResponseOutputItemUnion{{
				Type: eventTypeFunctionCall,
			}},
			want: "failed",
		},
		{
			name:   "missing response status with tool call",
			output: []responses.ResponseOutputItemUnion{{Type: eventTypeFunctionCall}},
			want:   "",
		},
		{
			name:   "in progress output item",
			status: "completed",
			output: []responses.ResponseOutputItemUnion{{
				Type:   "message",
				Status: "in_progress",
			}},
			want: "in_progress",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeResponsesStopReason(tt.status, tt.output, tt.blankIsIncomplete); got != tt.want {
				t.Fatalf("normalizeResponsesStopReason() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeChatStreamStopReason(t *testing.T) {
	tests := []struct {
		name         string
		finishReason string
		hasContent   bool
		hasRefusal   bool
		hasToolCalls bool
		want         string
	}{
		{name: "content stop", finishReason: stopReasonStop, hasContent: true, want: stopReasonStop},
		{name: "tool calls stop", finishReason: stopReasonStop, hasToolCalls: true, want: stopReasonToolCalls},
		{name: "missing reason with tool calls", hasToolCalls: true, want: stopReasonIncomplete},
		{name: "blank stop", finishReason: stopReasonStop, want: stopReasonIncomplete},
		{name: "tool reason without calls", finishReason: stopReasonToolCalls, want: stopReasonIncomplete},
		{name: "legacy function reason without calls", finishReason: stopReasonFunctionCall, want: stopReasonIncomplete},
		{name: "legacy function reason with call", finishReason: stopReasonFunctionCall, hasToolCalls: true, want: stopReasonToolCalls},
		{name: "refusal overrides calls", finishReason: stopReasonToolCalls, hasRefusal: true, hasToolCalls: true, want: stopReasonRefusal},
		{name: "length preserved", finishReason: "length", hasContent: true, want: "length"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeChatStreamStopReason(tt.finishReason, tt.hasContent, tt.hasRefusal, tt.hasToolCalls)
			if got != tt.want {
				t.Fatalf("normalizeChatStreamStopReason() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStream_ResponsesAPI_NormalizesTerminalOutcome(t *testing.T) {
	tests := []struct {
		name       string
		output     string
		wantReason string
	}{
		{
			name: "refusal",
			output: `[{"id":"msg_1","type":"message","role":"assistant","status":"completed",` +
				`"content":[{"type":"refusal","refusal":"Cannot comply"}]}]`,
			wantReason: stopReasonRefusal,
		},
		{
			name: "incomplete item",
			output: `[{"id":"msg_1","type":"message","role":"assistant","status":"incomplete",` +
				`"content":[{"type":"output_text","text":"partial"}]}]`,
			wantReason: stopReasonIncomplete,
		},
		{
			name:       "blank output",
			output:     `[]`,
			wantReason: stopReasonIncomplete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w,
					"event: response.completed\ndata: "+
						`{"type":"response.completed","response":{"id":"resp_1","status":"completed",`+
						`"model":"gpt-4","output":%s,"usage":{"input_tokens":5,"output_tokens":1,"total_tokens":6}}}`+
						"\n\n",
					tt.output,
				)
			}))
			defer server.Close()

			provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
			if err != nil {
				t.Fatalf("NewProvider() error = %v", err)
			}
			provider.mode.Store(int32(apiModeResponses))

			ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
				Model:    "gpt-4",
				Messages: []llm.Message{{Role: "user", Content: "hi"}},
			})
			if err != nil {
				t.Fatalf("Stream() error = %v", err)
			}

			var stopReason string
			for chunk := range ch {
				if chunk.Error != nil {
					t.Fatalf("stream error = %v", chunk.Error)
				}
				if chunk.Done {
					stopReason = chunk.StopReason
				}
			}
			if stopReason != tt.wantReason {
				t.Fatalf("stop reason = %q, want %q", stopReason, tt.wantReason)
			}
		})
	}
}

func TestComplete_ResponsesAPI_WithToolCalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		//nolint:errcheck // multiline test response
		fmt.Fprint(w, `{
			"id": "resp_tc",
			"object": "response",
			"created_at": 1700000000,
			"status": "completed",
			"model": "gpt-4",
			"output": [
				{
					"id": "fc_1",
					"type": "function_call",
					"status": "completed",
					"call_id": "call_1",
					"name": "search",
					"arguments": "{\"q\":\"test\"}"
				}
			],
			"usage": {"input_tokens": 20, "output_tokens": 10, "total_tokens": 30}
		}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: testToolNameSearch}},
		Tools:    []llm.Tool{{Name: testToolNameSearch, Description: testToolNameSearch, Parameters: json.RawMessage(`{"type":"object"}`)}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(resp.ToolCalls))
	}
	if resp.ToolCalls[0].Name != testToolNameSearch {
		t.Errorf("expected tool name 'search', got %q", resp.ToolCalls[0].Name)
	}
	if resp.ToolCalls[0].ID != "call_1" {
		t.Errorf("expected tool call ID 'call_1', got %q", resp.ToolCalls[0].ID)
	}
	if resp.StopReason != stopReasonToolCalls {
		t.Errorf("expected stop reason 'tool_calls', got %q", resp.StopReason)
	}
}

func TestComplete_ResponsesAPI_MaxOutputTokens(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{
			"id": "resp_limit",
			"object": "response",
			"created_at": 1700000000,
			"status": "incomplete",
			"incomplete_details": {"reason": "max_output_tokens"},
			"model": "gpt-4",
			"output": [{
				"id": "msg_1",
				"type": "message",
				"role": "assistant",
				"status": "incomplete",
				"content": [{"type": "output_text", "text": "partial"}]
			}],
			"usage": {"input_tokens": 5, "output_tokens": 3, "total_tokens": 8}
		}`)
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.Content != "partial" || resp.StopReason != stopReasonLength {
		t.Fatalf("response = content %q, stop reason %q; want partial content with %q", resp.Content, resp.StopReason, stopReasonLength)
	}
}

func TestComplete_ResponsesAPI_WithAllOptions(t *testing.T) {
	var receivedBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&receivedBody)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, responsesJSON("ok", 1, 1)) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	_, err = provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:        "gpt-4",
		Messages:     []llm.Message{{Role: "user", Content: "hi"}},
		SystemPrompt: "be helpful",
		MaxTokens:    100,
		Temperature:  0.7,
		Tools:        []llm.Tool{{Name: "fn", Description: "d", Parameters: json.RawMessage(`{"type":"object"}`)}},
		ResponseFormat: &llm.ResponseFormat{
			Type: "json_object",
		},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	// Verify options were sent in the request body
	if receivedBody["instructions"] == nil {
		t.Error("expected instructions to be set")
	}
	if receivedBody["max_output_tokens"] == nil {
		t.Error("expected max_output_tokens to be set")
	}
	if receivedBody["temperature"] == nil {
		t.Error("expected temperature to be set")
	}
	if receivedBody["tools"] == nil {
		t.Error("expected tools to be set")
	}
}

func TestComplete_AutoDetect_ResponsesSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, responsesJSON("Responses works!", 5, 3)) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.Content != "Responses works!" {
		t.Errorf("expected 'Responses works!', got %q", resp.Content)
	}
	if apiMode(provider.mode.Load()) != apiModeResponses {
		t.Error("expected mode to be set to apiModeResponses")
	}
}

func TestComplete_AutoDetect_NonRecoverableError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"error":{"message":"rate limited","type":"rate_limit_error","code":"rate_limit_exceeded"}}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	_, err = provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected error for rate limited request")
	}
}

func TestComplete_AutoDetect_DoesNotFallbackOnForbidden(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"error":{"message":"forbidden","type":"permission_error"}}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	_, err = provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected error for forbidden request")
	}
	if requestCount != 1 {
		t.Fatalf("expected only Responses request, got %d requests", requestCount)
	}
	if apiMode(provider.mode.Load()) != apiModeUnknown {
		t.Error("expected mode to stay unknown after plain 403")
	}
}

func TestComplete_ResponsesAPI_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"error":{"message":"server error","type":"server_error"}}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	_, err = provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestComplete_ChatCompletions_WithResponseFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		//nolint:errcheck // multiline test response
		fmt.Fprint(w, `{
			"id": "chatcmpl-rf",
			"object": "chat.completion",
			"model": "gpt-4",
			"choices": [{
				"index": 0,
				"message": {"role": "assistant", "content": "{\"key\":\"value\"}"},
				"finish_reason": "stop"
			}],
			"usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
		}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeChatCompletions))

	resp, err := provider.Complete(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "return json"}},
		ResponseFormat: &llm.ResponseFormat{
			Type: "json_schema",
			JSONSchema: &llm.JSONSchemaFormat{
				Name:        "test",
				Schema:      map[string]any{"type": "object"},
				Strict:      new(true),
				Description: "test schema",
			},
		},
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.Content != `{"key":"value"}` {
		t.Errorf("expected JSON content, got %q", resp.Content)
	}
}

func TestStream_ResponsesAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, _ := w.(http.Flusher)

		fmt.Fprint(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello\",\"item_id\":\"item_1\",\"output_index\":0,\"content_index\":0}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\" World\",\"item_id\":\"item_1\",\"output_index\":0,\"content_index\":0}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"model\":\"gpt-4\",\"output\":[{\"type\":\"message\"}],\"usage\":{\"input_tokens\":5,\"output_tokens\":3,\"total_tokens\":8}}}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var content strings.Builder
	var gotDone bool
	var inputTokens, outputTokens int
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected error: %v", chunk.Error)
		}
		if chunk.InputTokens > 0 || chunk.OutputTokens > 0 {
			inputTokens = chunk.InputTokens
			outputTokens = chunk.OutputTokens
		}
		content.WriteString(chunk.Content) //nolint:errcheck
		if chunk.Done {
			gotDone = true
		}
	}
	if inputTokens != 5 || outputTokens != 3 {
		t.Fatalf("stream usage = input:%d output:%d, want input:5 output:3", inputTokens, outputTokens)
	}
	if got := content.String(); got != "Hello World" {
		t.Errorf("expected 'Hello World', got %q", got)
	}
	if !gotDone {
		t.Error("expected done chunk")
	}
}

func TestStream_ResponsesAPI_WithToolCalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, _ := w.(http.Flusher)

		fmt.Fprint(w, "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_item\",\"type\":\"function_call\",\"call_id\":\"call_123\",\"name\":\"search\"}}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_item\",\"output_index\":0,\"delta\":\"{\\\"q\\\":\"}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "event: response.function_call_arguments.done\ndata: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_item\",\"output_index\":0,\"arguments\":\"{\\\"q\\\":\\\"test\\\"}\",\"name\":\"search\"}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_tc\",\"status\":\"completed\",\"model\":\"gpt-4\",\"output\":[{\"type\":\"function_call\",\"call_id\":\"call_123\",\"name\":\"search\",\"arguments\":\"{\\\"q\\\":\\\"test\\\"}\"}],\"usage\":{\"input_tokens\":5,\"output_tokens\":3,\"total_tokens\":8}}}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: testToolNameSearch}},
		Tools:    []llm.Tool{{Name: testToolNameSearch, Description: testToolNameSearch, Parameters: json.RawMessage(`{"type":"object"}`)}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var gotToolCall bool
	var stopReason string
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected error: %v", chunk.Error)
		}
		if chunk.ToolCall != nil {
			gotToolCall = true
			if chunk.ToolCall.Name != testToolNameSearch {
				t.Errorf("expected tool name 'search', got %q", chunk.ToolCall.Name)
			}
		}
		if chunk.Done {
			stopReason = chunk.StopReason
		}
	}
	if !gotToolCall {
		t.Error("expected tool call chunk")
	}
	if stopReason != stopReasonToolCalls {
		t.Errorf("expected stop reason 'tool_calls', got %q", stopReason)
	}
}

func TestStream_ResponsesAPI_ToolCallNameFromCompletedOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, _ := w.(http.Flusher)

		fmt.Fprint(w, "event: response.function_call_arguments.done\ndata: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_item\",\"output_index\":0,\"arguments\":\"{\\\"q\\\":\\\"test\\\"}\"}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_tc\",\"status\":\"completed\",\"model\":\"gpt-4\",\"output\":[{\"type\":\"function_call\",\"id\":\"fc_item\",\"call_id\":\"call_123\",\"name\":\"search\",\"arguments\":\"{\\\"q\\\":\\\"test\\\"}\"}],\"usage\":{\"input_tokens\":5,\"output_tokens\":3,\"total_tokens\":8}}}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: testToolNameSearch}},
		Tools:    []llm.Tool{{Name: testToolNameSearch, Description: testToolNameSearch, Parameters: json.RawMessage(`{"type":"object"}`)}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var toolCalls []llm.ToolCall
	var stopReason string
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected error: %v", chunk.Error)
		}
		if chunk.ToolCall != nil {
			toolCalls = append(toolCalls, *chunk.ToolCall)
		}
		if chunk.Done {
			stopReason = chunk.StopReason
		}
	}

	if len(toolCalls) != 1 {
		t.Fatalf("expected exactly one tool call, got %d", len(toolCalls))
	}
	if toolCalls[0].Name != testToolNameSearch {
		t.Errorf("expected tool name 'search', got %q", toolCalls[0].Name)
	}
	if toolCalls[0].ID != "call_123" {
		t.Errorf("expected tool ID 'call_123', got %q", toolCalls[0].ID)
	}
	if string(toolCalls[0].Arguments) != testToolArguments {
		t.Errorf("expected arguments %q, got %q", testToolArguments, string(toolCalls[0].Arguments))
	}
	if stopReason != stopReasonToolCalls {
		t.Errorf("expected stop reason 'tool_calls', got %q", stopReason)
	}
}

func TestStream_ResponsesAPI_WithAllOptions(t *testing.T) {
	var receivedBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == testResponsesPath {
			_ = json.NewDecoder(r.Body).Decode(&receivedBody)
			w.Header().Set("Content-Type", "text/event-stream")
			flusher, _ := w.(http.Flusher)
			fmt.Fprint(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\",\"item_id\":\"i1\",\"output_index\":0,\"content_index\":0}\n\n") //nolint:errcheck
			flusher.Flush()
			fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\",\"model\":\"gpt-4\",\"output\":[{\"type\":\"message\"}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n") //nolint:errcheck
			flusher.Flush()
			fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
			flusher.Flush()
			return
		}
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:        "gpt-4",
		Messages:     []llm.Message{{Role: "user", Content: "hi"}},
		SystemPrompt: "be helpful",
		MaxTokens:    50,
		Tools:        []llm.Tool{{Name: "fn", Description: "d", Parameters: json.RawMessage(`{"type":"object"}`)}},
		ResponseFormat: &llm.ResponseFormat{
			Type: "json_object",
		},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	for range ch {
	}
	if receivedBody["instructions"] == nil {
		t.Error("expected instructions in streaming request")
	}
	if receivedBody["max_output_tokens"] == nil {
		t.Error("expected max_output_tokens in streaming request")
	}
}

func TestStream_ResponsesAPI_Failed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, "event: response.failed\ndata: {\"type\":\"response.failed\"}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var stopReason string
	for chunk := range ch {
		if chunk.Done {
			stopReason = chunk.StopReason
		}
	}
	if stopReason != "response.failed" {
		t.Errorf("expected stop reason 'response.failed', got %q", stopReason)
	}
}

func TestStream_ResponsesAPI_MaxOutputTokens(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\",\"item_id\":\"i1\",\"output_index\":0,\"content_index\":0}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_limit\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"model\":\"gpt-4\",\"output\":[],\"usage\":{\"input_tokens\":5,\"output_tokens\":3,\"total_tokens\":8}}}\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var content, stopReason string
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected error: %v", chunk.Error)
		}
		content += chunk.Content
		if chunk.Done {
			stopReason = chunk.StopReason
		}
	}
	if content != "partial" || stopReason != stopReasonLength {
		t.Fatalf("stream = content %q, stop reason %q; want partial content with %q", content, stopReason, stopReasonLength)
	}
}

func TestStream_ResponsesAPI_MaxOutputTokensPreservesUnfinishedToolCall(t *testing.T) {
	tests := []struct {
		name           string
		functionEvents string
		terminalOutput string
	}{
		{
			name: "tracked partial function call",
			functionEvents: "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"id\":\"fc_item\",\"type\":\"function_call\",\"call_id\":\"call_123\",\"name\":\"search\"}}\n\n" +
				"event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_item\",\"output_index\":1,\"delta\":\"{\\\"q\\\":\"}\n\n",
			terminalOutput: "[]",
		},
		{
			name:           "terminal output-only function call",
			terminalOutput: "[{\"id\":\"fc_item\",\"type\":\"function_call\",\"call_id\":\"call_123\",\"name\":\"search\",\"arguments\":\"{\\\"q\\\":\"}]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				flusher, _ := w.(http.Flusher)
				fmt.Fprint(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\",\"item_id\":\"i1\",\"output_index\":0,\"content_index\":0}\n\n") //nolint:errcheck
				flusher.Flush()
				fmt.Fprint(w, tt.functionEvents) //nolint:errcheck
				flusher.Flush()
				fmt.Fprintf(w, "event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_limit\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"model\":\"gpt-4\",\"output\":%s,\"usage\":{\"input_tokens\":5,\"output_tokens\":3,\"total_tokens\":8}}}\n\n", tt.terminalOutput) //nolint:errcheck
				flusher.Flush()
			}))
			defer server.Close()

			provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
			if err != nil {
				t.Fatalf("NewProvider() error = %v", err)
			}
			provider.mode.Store(int32(apiModeResponses))

			ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
				Model:    "gpt-4",
				Messages: []llm.Message{{Role: "user", Content: "hi"}},
			})
			if err != nil {
				t.Fatalf("Stream() error = %v", err)
			}

			var content, stopReason string
			var toolCalls int
			for chunk := range ch {
				if chunk.Error != nil {
					t.Fatalf("unexpected error: %v", chunk.Error)
				}
				content += chunk.Content
				if chunk.ToolCall != nil {
					toolCalls++
				}
				if chunk.Done {
					stopReason = chunk.StopReason
				}
			}
			if content != "partial" || stopReason != eventTypeResponseIncomplete || toolCalls != 0 {
				t.Fatalf("stream = content %q, stop reason %q, tool calls %d; want partial content with an incomplete terminal and no emitted call", content, stopReason, toolCalls)
			}
		})
	}
}

func TestStream_ResponsesAPI_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, "event: error\ndata: {\"type\":\"error\",\"message\":\"something went wrong\"}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeResponses))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var gotError bool
	for chunk := range ch {
		if chunk.Error != nil {
			gotError = true
		}
	}
	if !gotError {
		t.Error("expected error chunk")
	}
}

func TestStream_AutoDetect_FallbackToChatCompletions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == testResponsesPath {
			// Check if this is the streaming request (has stream in body) or probe
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"message":"Not Found","type":"invalid_request_error","code":"invalid_url"}}`) //nolint:errcheck
			return
		}
		// Chat completions streaming
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"id\":\"cc-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Fallback\"},\"finish_reason\":null}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: {\"id\":\"cc-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var content strings.Builder
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected error: %v", chunk.Error)
		}
		content.WriteString(chunk.Content) //nolint:errcheck
	}
	if got := content.String(); got != testFallbackStream {
		t.Errorf("expected 'Fallback', got %q", got)
	}
	if apiMode(provider.mode.Load()) != apiModeChatCompletions {
		t.Error("expected mode apiModeChatCompletions after fallback")
	}
}

func TestStream_AutoDetect_FallbackToChatCompletions_OnCustomBare403(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == testResponsesPath {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"id\":\"cc-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Fallback\"},\"finish_reason\":null}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: {\"id\":\"cc-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var content strings.Builder
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected error: %v", chunk.Error)
		}
		content.WriteString(chunk.Content) //nolint:errcheck
	}
	if got := content.String(); got != testFallbackStream {
		t.Errorf("expected 'Fallback', got %q", got)
	}
	if apiMode(provider.mode.Load()) != apiModeChatCompletions {
		t.Error("expected mode apiModeChatCompletions after custom bare Responses 403 fallback")
	}
}

func TestStream_AutoDetect_FallbackToChatCompletionsOnForbiddenResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == testResponsesPath {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":{"message":"Forbidden","type":"forbidden"}}`) //nolint:errcheck
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"id\":\"cc-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Fallback after forbidden\"},\"finish_reason\":null}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: {\"id\":\"cc-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.baseURL = testCopilotBaseURL

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var content strings.Builder
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected error: %v", chunk.Error)
		}
		content.WriteString(chunk.Content) //nolint:errcheck
	}
	if got := content.String(); got != "Fallback after forbidden" {
		t.Errorf("expected 'Fallback after forbidden', got %q", got)
	}
	if apiMode(provider.mode.Load()) != apiModeChatCompletions {
		t.Error("expected mode apiModeChatCompletions after fallback")
	}
}

func TestStream_AutoDetect_ResponsesSuccess(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.URL.Path == testResponsesPath {
			if requestCount == 1 {
				// First call is probe (non-streaming)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, responsesJSON("probe", 1, 1)) //nolint:errcheck
				return
			}
			// Second call is the actual streaming request
			w.Header().Set("Content-Type", "text/event-stream")
			flusher, _ := w.(http.Flusher)
			fmt.Fprint(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Streamed\",\"item_id\":\"i1\",\"output_index\":0,\"content_index\":0}\n\n") //nolint:errcheck
			flusher.Flush()
			fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\",\"model\":\"gpt-4\",\"output\":[{\"type\":\"message\"}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n") //nolint:errcheck
			flusher.Flush()
			fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
			flusher.Flush()
			return
		}
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var content strings.Builder
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected error: %v", chunk.Error)
		}
		content.WriteString(chunk.Content) //nolint:errcheck
	}
	if got := content.String(); got != "Streamed" {
		t.Errorf("expected 'Streamed', got %q", got)
	}
	if apiMode(provider.mode.Load()) != apiModeResponses {
		t.Error("expected mode apiModeResponses after successful probe")
	}
}

func TestStream_AutoDetect_NonRecoverableError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"error":{"message":"rate limited","type":"rate_limit_error"}}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}

	_, err = provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected error for rate limited request")
	}
}

func TestStream_ChatCompletions_WithToolCalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, _ := w.(http.Flusher)
		// Tool call delta chunks
		fmt.Fprint(w, "data: {\"id\":\"cc-tc\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"search\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: {\"id\":\"cc-tc\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"q\\\":\\\"test\\\"}\"}}]},\"finish_reason\":null}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: {\"id\":\"cc-tc\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeChatCompletions))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:     "gpt-4",
		Messages:  []llm.Message{{Role: "user", Content: testToolNameSearch}},
		Tools:     []llm.Tool{{Name: testToolNameSearch, Description: testToolNameSearch, Parameters: json.RawMessage(`{"type":"object"}`)}}, //nolint:errcheck
		MaxTokens: 100,
		ResponseFormat: &llm.ResponseFormat{
			Type: "json_object",
		},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var gotToolCall bool
	var stopReason string
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected error: %v", chunk.Error)
		}
		if chunk.ToolCall != nil {
			gotToolCall = true
			if chunk.ToolCall.Name != testToolNameSearch {
				t.Errorf("expected tool name 'search', got %q", chunk.ToolCall.Name)
			}
		}
		if chunk.Done {
			stopReason = chunk.StopReason
		}
	}
	if !gotToolCall {
		t.Error("expected tool call chunk")
	}
	if stopReason != stopReasonToolCalls {
		t.Errorf("expected stop reason 'tool_calls', got %q", stopReason)
	}
}

func TestStream_ChatCompletions_RetriesWithoutUnsupportedStreamOptions(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "text/event-stream")
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "stream_options") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":{"message":"unsupported parameter: stream_options","type":"invalid_request_error"}}`) //nolint:errcheck
			return
		}
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"id\":\"chatcmpl-retry\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Fallback\"},\"finish_reason\":null}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: {\"id\":\"chatcmpl-retry\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n") //nolint:errcheck
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n") //nolint:errcheck
		flusher.Flush()
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeChatCompletions))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	var content strings.Builder
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected error: %v", chunk.Error)
		}
		content.WriteString(chunk.Content) //nolint:errcheck
	}
	if callCount != 2 {
		t.Fatalf("call count = %d, want retry without stream_options", callCount)
	}
	if got := content.String(); got != testFallbackStream {
		t.Fatalf("content = %q, want Fallback", got)
	}
}

func TestStream_ChatCompletions_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"error":{"message":"server error","type":"server_error"}}`) //nolint:errcheck
	}))
	defer server.Close()

	provider, err := NewProvider(llm.ProviderConfig{APIKey: "test-key", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	provider.mode.Store(int32(apiModeChatCompletions))

	ch, err := provider.Stream(context.Background(), &llm.CompletionRequest{
		Model:    "gpt-4",
		Messages: []llm.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	var gotError bool
	for chunk := range ch {
		if chunk.Error != nil {
			gotError = true
		}
	}
	if !gotError {
		t.Error("expected error in stream")
	}
}

func TestNewProvider_AdditionalConfigs(t *testing.T) {
	tests := []struct {
		name    string
		config  llm.ProviderConfig
		wantErr bool
	}{
		{
			name: "azure-openai with trailing slash in base URL",
			config: llm.ProviderConfig{
				APIKey:       "test-key",
				BaseURL:      "https://myresource.openai.azure.com/",
				ProviderType: "azure-openai",
			},
			wantErr: false,
		},
		{
			name: "openai with empty base URL",
			config: llm.ProviderConfig{
				APIKey:  "test-key",
				BaseURL: "",
			},
			wantErr: false,
		},
		{
			name: "azure-openai with empty API version uses default",
			config: llm.ProviderConfig{
				APIKey:          "test-key",
				BaseURL:         "https://myresource.openai.azure.com",
				ProviderType:    "azure-openai",
				AzureAPIVersion: "",
			},
			wantErr: false,
		},
		{
			name: "custom provider type with all options",
			config: llm.ProviderConfig{
				APIKey:  "test-key",
				BaseURL: "https://custom.api.com/v1",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := NewProvider(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProvider() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && provider == nil {
				t.Error("NewProvider() returned nil provider")
			}
		})
	}
}

func TestProviderTelemetryProviderNameNormalizesAzure(t *testing.T) {
	p := &Provider{providerType: "azure-openai"}
	if got := p.TelemetryProviderName(); got != "azure.ai.openai" {
		t.Fatalf("TelemetryProviderName() = %q, want azure.ai.openai", got)
	}
}
