/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/llm"
	"github.com/orka-agents/orka/internal/tools"

	_ "github.com/orka-agents/orka/internal/llm/anthropic"
	_ "github.com/orka-agents/orka/internal/llm/openai"
)

const (
	testHelloContent    = "Hello!"
	testToolNameSearch  = "search"
	testRoleUser        = "user"
	testRoleTool        = "tool"
	testRoleAssistant   = "assistant"
	testInvalidReqError = "invalid_request_error"
	testToolUseID       = "tu_1"
)

func setupTestAnthropicHandler(objs ...runtime.Object) (*AnthropicCompatHandler, *fiber.App) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	config := DefaultChatConfig()
	resolver := NewProviderResolver(fakeClient, config)
	handler := NewAnthropicCompatHandler(fakeClient, nil, "default", false, config, resolver, nil)

	app := fiber.New()
	return handler, app
}

// --- Tests: parseAnthropicContent ---

func TestParseAnthropicContent(t *testing.T) {
	tests := []struct {
		name      string
		raw       json.RawMessage
		wantLen   int
		wantFirst string
		wantErr   bool
	}{
		{
			name:      "string content",
			raw:       json.RawMessage(`"hello world"`),
			wantLen:   1,
			wantFirst: "hello world",
		},
		{
			name:    "array of content blocks",
			raw:     json.RawMessage(`[{"type":"text","text":"first"},{"type":"text","text":"second"}]`),
			wantLen: 2,
		},
		{
			name:    "empty content",
			raw:     nil,
			wantLen: 0,
		},
		{
			name:    "empty raw message",
			raw:     json.RawMessage(``),
			wantLen: 0,
		},
		{
			name:    "invalid JSON",
			raw:     json.RawMessage(`{not valid`),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blocks, err := parseAnthropicContent(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(blocks) != tt.wantLen {
				t.Fatalf("expected %d blocks, got %d", tt.wantLen, len(blocks))
			}
			if tt.wantFirst != "" && len(blocks) > 0 {
				if blocks[0].Text != tt.wantFirst {
					t.Errorf("expected first block text %q, got %q", tt.wantFirst, blocks[0].Text)
				}
				if blocks[0].Type != oaiContentTypeText {
					t.Errorf("expected first block type 'text', got %q", blocks[0].Type)
				}
			}
		})
	}
}

func TestParseAnthropicContent_ArrayDetails(t *testing.T) {
	raw := json.RawMessage(`[{"type":"text","text":"hello"},{"type":"tool_use","id":"tu_1","name":"search","input":{"q":"test"}}]`)
	blocks, err := parseAnthropicContent(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks, got %d", len(blocks))
	}
	if blocks[0].Type != oaiContentTypeText || blocks[0].Text != "hello" {
		t.Errorf("block 0: type=%q text=%q", blocks[0].Type, blocks[0].Text)
	}
	if blocks[1].Type != oaiStopReasonToolUse || blocks[1].ID != testToolUseID || blocks[1].Name != testToolNameSearch {
		t.Errorf("block 1: type=%q id=%q name=%q", blocks[1].Type, blocks[1].ID, blocks[1].Name)
	}
}

// --- Tests: parseAnthropicSystem ---

func TestParseAnthropicSystem(t *testing.T) {
	tests := []struct {
		name    string
		raw     json.RawMessage
		want    string
		wantErr bool
	}{
		{
			name: "string system",
			raw:  json.RawMessage(`"You are a helpful assistant."`),
			want: "You are a helpful assistant.",
		},
		{
			name: "array of text blocks",
			raw:  json.RawMessage(`[{"type":"text","text":"First instruction."},{"type":"text","text":"Second instruction."}]`),
			want: "First instruction.\nSecond instruction.",
		},
		{
			name: "empty",
			raw:  nil,
			want: "",
		},
		{
			name: "empty raw message",
			raw:  json.RawMessage(``),
			want: "",
		},
		{
			name: "single text block array",
			raw:  json.RawMessage(`[{"type":"text","text":"Only one."}]`),
			want: "Only one.",
		},
		{
			name:    "invalid JSON",
			raw:     json.RawMessage(`{invalid`),
			wantErr: true,
		},
		{
			name: "array with non-text blocks ignored",
			raw:  json.RawMessage(`[{"type":"image","text":"ignored"},{"type":"text","text":"kept"}]`),
			want: "kept",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAnthropicSystem(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

// --- Tests: convertAnthropicMessages ---

func TestConvertAnthropicMessages(t *testing.T) { //nolint:gocyclo
	tests := []struct {
		name     string
		messages []AnthropicMessage
		wantLen  int
		check    func(t *testing.T, msgs []llm.Message)
	}{
		{
			name: "user text message",
			messages: []AnthropicMessage{
				{Role: "user", Content: json.RawMessage(`"Hello!"`)},
			},
			wantLen: 1,
			check: func(t *testing.T, msgs []llm.Message) {
				if msgs[0].Role != testRoleUser {
					t.Errorf("role = %q, want user", msgs[0].Role)
				}
				if msgs[0].Content != testHelloContent {
					t.Errorf("content = %q, want Hello!", msgs[0].Content)
				}
			},
		},
		{
			name: "user with tool_result blocks",
			messages: []AnthropicMessage{
				{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tu_1","content":"result text"}]`)},
			},
			wantLen: 1,
			check: func(t *testing.T, msgs []llm.Message) {
				if msgs[0].Role != testRoleTool {
					t.Errorf("role = %q, want tool", msgs[0].Role)
				}
				if msgs[0].ToolCallID != testToolUseID {
					t.Errorf("tool_call_id = %q, want tu_1", msgs[0].ToolCallID)
				}
				if msgs[0].Content != "result text" {
					t.Errorf("content = %q, want 'result text'", msgs[0].Content)
				}
			},
		},
		{
			name: "user with tool_result with nested content blocks",
			messages: []AnthropicMessage{
				{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tu_2","content":[{"type":"text","text":"line1"},{"type":"text","text":"line2"}]}]`)},
			},
			wantLen: 1,
			check: func(t *testing.T, msgs []llm.Message) {
				if msgs[0].Role != testRoleTool {
					t.Errorf("role = %q, want tool", msgs[0].Role)
				}
				if msgs[0].Content != "line1\nline2" {
					t.Errorf("content = %q, want 'line1\\nline2'", msgs[0].Content)
				}
			},
		},
		{
			name: "assistant with text blocks",
			messages: []AnthropicMessage{
				{Role: "assistant", Content: json.RawMessage(`[{"type":"text","text":"I can help."}]`)},
			},
			wantLen: 1,
			check: func(t *testing.T, msgs []llm.Message) {
				if msgs[0].Role != testRoleAssistant {
					t.Errorf("role = %q, want assistant", msgs[0].Role)
				}
				if msgs[0].Content != "I can help." {
					t.Errorf("content = %q, want 'I can help.'", msgs[0].Content)
				}
			},
		},
		{
			name: "assistant with tool_use blocks",
			messages: []AnthropicMessage{
				{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","id":"tu_1","name":"search","input":{"q":"test"}}]`)},
			},
			wantLen: 1,
			check: func(t *testing.T, msgs []llm.Message) {
				if msgs[0].Role != testRoleAssistant {
					t.Errorf("role = %q, want assistant", msgs[0].Role)
				}
				if len(msgs[0].ToolCalls) != 1 {
					t.Fatalf("expected 1 tool call, got %d", len(msgs[0].ToolCalls))
				}
				if msgs[0].ToolCalls[0].ID != testToolUseID {
					t.Errorf("tool call ID = %q, want tu_1", msgs[0].ToolCalls[0].ID)
				}
				if msgs[0].ToolCalls[0].Name != testToolNameSearch {
					t.Errorf("tool call name = %q, want search", msgs[0].ToolCalls[0].Name)
				}
			},
		},
		{
			name: "mixed assistant message text and tool_use",
			messages: []AnthropicMessage{
				{Role: "assistant", Content: json.RawMessage(`[{"type":"text","text":"Let me search."},{"type":"tool_use","id":"tu_1","name":"search","input":{"q":"test"}}]`)},
			},
			wantLen: 1,
			check: func(t *testing.T, msgs []llm.Message) {
				if msgs[0].Content != "Let me search." {
					t.Errorf("content = %q, want 'Let me search.'", msgs[0].Content)
				}
				if len(msgs[0].ToolCalls) != 1 {
					t.Fatalf("expected 1 tool call, got %d", len(msgs[0].ToolCalls))
				}
				if msgs[0].ToolCalls[0].Name != testToolNameSearch {
					t.Errorf("tool call name = %q", msgs[0].ToolCalls[0].Name)
				}
			},
		},
		{
			name: "multiple messages in conversation",
			messages: []AnthropicMessage{
				{Role: "user", Content: json.RawMessage(`"Hello"`)},
				{Role: "assistant", Content: json.RawMessage(`"Hi there!"`)},
				{Role: "user", Content: json.RawMessage(`"How are you?"`)},
			},
			wantLen: 3,
			check: func(t *testing.T, msgs []llm.Message) {
				if msgs[0].Role != testRoleUser || msgs[0].Content != "Hello" {
					t.Errorf("msg 0: role=%q content=%q", msgs[0].Role, msgs[0].Content)
				}
				if msgs[1].Role != testRoleAssistant || msgs[1].Content != "Hi there!" {
					t.Errorf("msg 1: role=%q content=%q", msgs[1].Role, msgs[1].Content)
				}
				if msgs[2].Role != testRoleUser || msgs[2].Content != "How are you?" {
					t.Errorf("msg 2: role=%q content=%q", msgs[2].Role, msgs[2].Content)
				}
			},
		},
		{
			name: "user with mixed text and tool_result",
			messages: []AnthropicMessage{
				{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"Here is context."},{"type":"tool_result","tool_use_id":"tu_1","content":"tool output"}]`)},
			},
			wantLen: 2,
			check: func(t *testing.T, msgs []llm.Message) {
				// tool_result messages come first, then text
				toolMsg := msgs[0]
				textMsg := msgs[1]
				if toolMsg.Role != testRoleTool || toolMsg.ToolCallID != "tu_1" {
					t.Errorf("tool msg: role=%q id=%q", toolMsg.Role, toolMsg.ToolCallID)
				}
				if textMsg.Role != testRoleUser || textMsg.Content != "Here is context." {
					t.Errorf("text msg: role=%q content=%q", textMsg.Role, textMsg.Content)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msgs, err := convertAnthropicMessages(tt.messages)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(msgs) != tt.wantLen {
				t.Fatalf("expected %d messages, got %d", tt.wantLen, len(msgs))
			}
			if tt.check != nil {
				tt.check(t, msgs)
			}
		})
	}
}

func TestConvertAnthropicMessages_InvalidContent(t *testing.T) {
	msgs := []AnthropicMessage{
		{Role: "user", Content: json.RawMessage(`{invalid}`)},
	}
	_, err := convertAnthropicMessages(msgs)
	if err == nil {
		t.Fatal("expected error for invalid content")
	}
}

// --- Tests: convertAnthropicTools ---

func TestConvertAnthropicTools(t *testing.T) {
	tests := []struct {
		name    string
		tools   []AnthropicTool
		wantLen int
	}{
		{
			name: "single tool with input_schema",
			tools: []AnthropicTool{
				{
					Name:        "get_weather",
					Description: "Get weather info",
					InputSchema: json.RawMessage(`{"type":"object","properties":{"location":{"type":"string"}}}`),
				},
			},
			wantLen: 1,
		},
		{
			name: "multiple tools",
			tools: []AnthropicTool{
				{Name: "tool_a", Description: "First tool", InputSchema: json.RawMessage(`{"type":"object"}`)},
				{Name: "tool_b", Description: "Second tool", InputSchema: json.RawMessage(`{"type":"object"}`)},
			},
			wantLen: 2,
		},
		{
			name:    "empty tools",
			tools:   nil,
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertAnthropicTools(tt.tools)
			if tt.wantLen == 0 {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			if len(result) != tt.wantLen {
				t.Fatalf("expected %d tools, got %d", tt.wantLen, len(result))
			}
		})
	}
}

func TestConvertAnthropicTools_FieldMapping(t *testing.T) {
	anthropicTools := []AnthropicTool{
		{
			Name:        "search",
			Description: "Search the web",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}`),
		},
	}
	result := convertAnthropicTools(anthropicTools)
	if len(result) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(result))
	}
	if result[0].Name != testToolNameSearch {
		t.Errorf("name = %q, want search", result[0].Name)
	}
	if result[0].Description != "Search the web" {
		t.Errorf("description = %q", result[0].Description)
	}
	if string(result[0].Parameters) != string(anthropicTools[0].InputSchema) {
		t.Errorf("parameters = %s, want %s", result[0].Parameters, anthropicTools[0].InputSchema)
	}
}

// --- Tests: convertToAnthropicResponse ---

func TestConvertToAnthropicResponse(t *testing.T) {
	tests := []struct {
		name             string
		resp             *llm.CompletionResponse
		model            string
		wantContentLen   int
		wantStopReason   string
		wantInputTokens  int
		wantOutputTokens int
		checkContent     func(t *testing.T, content []AnthropicContentBlock)
	}{
		{
			name: "text-only response",
			resp: &llm.CompletionResponse{
				Content:      testHelloContent,
				StopReason:   "stop",
				InputTokens:  10,
				OutputTokens: 5,
			},
			model:            "claude-sonnet-4-20250514",
			wantContentLen:   1,
			wantStopReason:   "end_turn",
			wantInputTokens:  10,
			wantOutputTokens: 5,
			checkContent: func(t *testing.T, content []AnthropicContentBlock) {
				if content[0].Type != oaiContentTypeText {
					t.Errorf("type = %q, want text", content[0].Type)
				}
				if content[0].Text != testHelloContent {
					t.Errorf("text = %q, want Hello!", content[0].Text)
				}
			},
		},
		{
			name: "tool calls response",
			resp: &llm.CompletionResponse{
				StopReason: "tool_calls",
				ToolCalls: []llm.ToolCall{
					{ID: testToolUseID, Name: "search", Arguments: json.RawMessage(`{"q":"test"}`)},
				},
			},
			model:          "claude-sonnet-4-20250514",
			wantContentLen: 1,
			wantStopReason: oaiStopReasonToolUse,
			checkContent: func(t *testing.T, content []AnthropicContentBlock) {
				if content[0].Type != oaiStopReasonToolUse {
					t.Errorf("type = %q, want tool_use", content[0].Type)
				}
				if content[0].ID != testToolUseID {
					t.Errorf("id = %q, want tu_1", content[0].ID)
				}
				if content[0].Name != testToolNameSearch {
					t.Errorf("name = %q, want search", content[0].Name)
				}
			},
		},
		{
			name: "mixed text and tool calls",
			resp: &llm.CompletionResponse{
				Content:    "Let me search.",
				StopReason: oaiStopReasonToolUse,
				ToolCalls: []llm.ToolCall{
					{ID: testToolUseID, Name: "search", Arguments: json.RawMessage(`{"q":"test"}`)},
				},
			},
			model:          "claude-sonnet-4-20250514",
			wantContentLen: 2,
			wantStopReason: oaiStopReasonToolUse,
			checkContent: func(t *testing.T, content []AnthropicContentBlock) {
				if content[0].Type != oaiContentTypeText || content[0].Text != "Let me search." {
					t.Errorf("block 0: type=%q text=%q", content[0].Type, content[0].Text)
				}
				if content[1].Type != oaiStopReasonToolUse || content[1].Name != testToolNameSearch {
					t.Errorf("block 1: type=%q name=%q", content[1].Type, content[1].Name)
				}
			},
		},
		{
			name: "stop reason mapping - max_tokens",
			resp: &llm.CompletionResponse{
				Content:    "Truncated...",
				StopReason: "max_tokens",
			},
			model:          "claude-sonnet-4-20250514",
			wantContentLen: 1,
			wantStopReason: "max_tokens",
		},
		{
			name: "empty content with no tool calls",
			resp: &llm.CompletionResponse{
				StopReason: "stop",
			},
			model:          "claude-sonnet-4-20250514",
			wantContentLen: 0,
			wantStopReason: "end_turn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertToAnthropicResponse(tt.resp, tt.model, tt.wantStopReason)

			if result.Type != "message" {
				t.Errorf("type = %q, want message", result.Type)
			}
			if result.Role != testRoleAssistant {
				t.Errorf("role = %q, want assistant", result.Role)
			}
			if result.Model != tt.model {
				t.Errorf("model = %q, want %q", result.Model, tt.model)
			}
			if !strings.HasPrefix(result.ID, "msg_") {
				t.Errorf("ID = %q, expected msg_ prefix", result.ID)
			}
			if len(result.Content) != tt.wantContentLen {
				t.Fatalf("content len = %d, want %d", len(result.Content), tt.wantContentLen)
			}
			if result.StopReason == nil {
				t.Fatal("stop_reason is nil")
			}
			if *result.StopReason != tt.wantStopReason {
				t.Errorf("stop_reason = %q, want %q", *result.StopReason, tt.wantStopReason)
			}
			if tt.wantInputTokens > 0 && result.Usage.InputTokens != tt.wantInputTokens {
				t.Errorf("input_tokens = %d, want %d", result.Usage.InputTokens, tt.wantInputTokens)
			}
			if tt.wantOutputTokens > 0 && result.Usage.OutputTokens != tt.wantOutputTokens {
				t.Errorf("output_tokens = %d, want %d", result.Usage.OutputTokens, tt.wantOutputTokens)
			}
			if tt.checkContent != nil {
				tt.checkContent(t, result.Content)
			}
		})
	}
}

func TestMapAnthropicCompletionStopReason(t *testing.T) {
	tests := []struct {
		name string
		resp *llm.CompletionResponse
		want string
		ok   bool
	}{
		{name: "nil response"},
		{name: "blank completion", resp: &llm.CompletionResponse{StopReason: "end_turn"}, want: "end_turn", ok: true},
		{name: "completed", resp: &llm.CompletionResponse{Content: "done", StopReason: "end_turn"}, want: "end_turn", ok: true},
		{name: "completed tool call", resp: &llm.CompletionResponse{StopReason: "end_turn", ToolCalls: []llm.ToolCall{{ID: "call-1", Name: "search"}}}, want: "tool_use", ok: true},
		{name: "refusal", resp: &llm.CompletionResponse{StopReason: "refusal"}, want: "refusal", ok: true},
		{name: "pause turn", resp: &llm.CompletionResponse{StopReason: "pause_turn"}, want: "pause_turn", ok: true},
		{name: "failed", resp: &llm.CompletionResponse{StopReason: "failed"}},
		{name: "missing reason", resp: &llm.CompletionResponse{Content: "partial"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := mapAnthropicCompletionStopReason(tt.resp)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("mapAnthropicCompletionStopReason() = (%q, %t), want (%q, %t)", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestConvertToAnthropicResponse_PreservesGoalStateSentinelForTransparentProxy(t *testing.T) {
	result := convertToAnthropicResponse(&llm.CompletionResponse{
		Content:    goalStateSentinel + "\nPR ready: https://example.test/pr/3",
		StopReason: oaiStopReasonEndTurn,
	}, "claude-sonnet-4-20250514", oaiStopReasonEndTurn)

	if len(result.Content) != 1 {
		t.Fatalf("expected one text block, got %d", len(result.Content))
	}
	if result.Content[0].Text != goalStateSentinel+"\nPR ready: https://example.test/pr/3" {
		t.Fatalf("text = %q", result.Content[0].Text)
	}
	if !strings.Contains(result.Content[0].Text, goalStateSentinel) {
		t.Fatalf("transparent converter should preserve sentinel-like text: %q", result.Content[0].Text)
	}
}

func TestAnthropicToolLoopFormatting_StripsGoalStateSentinel(t *testing.T) {
	resp := stripGoalStateSentinelFromResponse(&llm.CompletionResponse{
		Content:    goalStateSentinel + "\nPR ready: https://example.test/pr/3",
		StopReason: oaiStopReasonEndTurn,
	})
	result := convertToAnthropicResponse(resp, "claude-sonnet-4-20250514", oaiStopReasonEndTurn)

	if len(result.Content) != 1 {
		t.Fatalf("expected one text block, got %d", len(result.Content))
	}
	if result.Content[0].Text != "PR ready: https://example.test/pr/3" {
		t.Fatalf("text = %q", result.Content[0].Text)
	}
	if strings.Contains(result.Content[0].Text, goalStateSentinel) {
		t.Fatalf("tool-loop formatted response leaked sentinel: %q", result.Content[0].Text)
	}
}

// --- Tests: anthropicError ---

func TestAnthropicError(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		errType    string
		message    string
		wantStatus int
	}{
		{
			name:       "400 invalid request",
			status:     400,
			errType:    testInvalidReqError,
			message:    "model is required",
			wantStatus: 400,
		},
		{
			name:       "500 api error",
			status:     500,
			errType:    "api_error",
			message:    "completion failed",
			wantStatus: 500,
		},
		{
			name:       "403 permission error",
			status:     403,
			errType:    "permission_error",
			message:    "namespace not allowed",
			wantStatus: 403,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := fiber.New()
			app.Get("/test", func(c fiber.Ctx) error {
				return anthropicError(c, tt.status, tt.errType, tt.message)
			})

			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}

			var errResp AnthropicError
			if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}
			if errResp.Type != "error" {
				t.Errorf("type = %q, want 'error'", errResp.Type)
			}
			if errResp.Error.Type != tt.errType {
				t.Errorf("error.type = %q, want %q", errResp.Error.Type, tt.errType)
			}
			if errResp.Error.Message != tt.message {
				t.Errorf("error.message = %q, want %q", errResp.Error.Message, tt.message)
			}
		})
	}
}

// --- Tests: HandleMessages validation ---

func TestHandleMessages_MissingModel(t *testing.T) {
	handler, app := setupTestAnthropicHandler()
	app.Post("/anthropic/v1/messages", handler.HandleMessages)

	body := `{"messages":[{"role":"user","content":"hello"}],"max_tokens":100}`
	req := httptest.NewRequest(http.MethodPost, "/anthropic/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}

	var errResp AnthropicError
	json.NewDecoder(resp.Body).Decode(&errResp) //nolint:errcheck
	if errResp.Error.Type != testInvalidReqError {
		t.Errorf("error type = %q", errResp.Error.Type)
	}
	if !strings.Contains(errResp.Error.Message, "model") {
		t.Errorf("error message = %q, expected mention of model", errResp.Error.Message)
	}
}

func TestHandleMessages_EmptyMessages(t *testing.T) {
	handler, app := setupTestAnthropicHandler()
	app.Post("/anthropic/v1/messages", handler.HandleMessages)

	body := `{"model":"claude-sonnet-4-20250514","messages":[],"max_tokens":100}`
	req := httptest.NewRequest(http.MethodPost, "/anthropic/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}

	var errResp AnthropicError
	json.NewDecoder(resp.Body).Decode(&errResp) //nolint:errcheck
	if !strings.Contains(errResp.Error.Message, "messages") {
		t.Errorf("error message = %q, expected mention of messages", errResp.Error.Message)
	}
}

func TestHandleMessages_MissingMaxTokens(t *testing.T) {
	handler, app := setupTestAnthropicHandler()
	app.Post("/anthropic/v1/messages", handler.HandleMessages)

	body := `{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/anthropic/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}

	var errResp AnthropicError
	json.NewDecoder(resp.Body).Decode(&errResp) //nolint:errcheck
	if !strings.Contains(errResp.Error.Message, "max_tokens") {
		t.Errorf("error message = %q, expected mention of max_tokens", errResp.Error.Message)
	}
}

func TestHandleMessages_InvalidBody(t *testing.T) {
	handler, app := setupTestAnthropicHandler()
	app.Post("/anthropic/v1/messages", handler.HandleMessages)

	req := httptest.NewRequest(http.MethodPost, "/anthropic/v1/messages", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandleMessages_NoProvider(t *testing.T) {
	handler, app := setupTestAnthropicHandler()
	app.Post("/anthropic/v1/messages", handler.HandleMessages)

	body := `{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hello"}],"max_tokens":100}`
	req := httptest.NewRequest(http.MethodPost, "/anthropic/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}

	var errResp AnthropicError
	json.NewDecoder(resp.Body).Decode(&errResp) //nolint:errcheck
	if !strings.Contains(errResp.Error.Message, "provider") {
		t.Errorf("error message = %q, expected mention of provider", errResp.Error.Message)
	}
}

func TestHandleMessages_ProviderSlashModel(t *testing.T) {
	mockAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"authentication_error","message":"invalid key"}}`))
	}))
	defer mockAPI.Close()

	provider := &corev1alpha1.Provider{
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

	handler, app := setupTestAnthropicHandler(provider, secret)
	app.Post("/anthropic/v1/messages", handler.HandleMessages)

	body := `{"model":"anthropic/claude-sonnet-4-20250514","messages":[{"role":"user","content":"hello"}],"max_tokens":100}`
	req := httptest.NewRequest(http.MethodPost, "/anthropic/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get past provider resolution (not 400)
	if resp.StatusCode == 400 {
		t.Errorf("did not expect 400; provider/model resolution should have succeeded")
	}
}

// --- Tests: HandleListModels ---

func TestAnthropicHandleListModels_Empty(t *testing.T) {
	handler, app := setupTestAnthropicHandler()
	app.Get("/anthropic/v1/models", handler.HandleListModels)

	req := httptest.NewRequest(http.MethodGet, "/anthropic/v1/models", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var modelList OAIModelList
	json.NewDecoder(resp.Body).Decode(&modelList) //nolint:errcheck
	if modelList.Object != "list" {
		t.Errorf("object = %q, want list", modelList.Object)
	}
	if len(modelList.Data) != 0 {
		t.Errorf("expected 0 models, got %d", len(modelList.Data))
	}
}

func TestAnthropicHandleListModels_WithProviders(t *testing.T) {
	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeAnthropic,
			DefaultModel: "claude-sonnet-4-20250514",
			SecretRef:    corev1alpha1.ProviderSecretRef{Name: "secret"},
		},
	}

	handler, app := setupTestAnthropicHandler(provider)
	app.Get("/anthropic/v1/models", handler.HandleListModels)

	req := httptest.NewRequest(http.MethodGet, "/anthropic/v1/models", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var modelList OAIModelList
	json.NewDecoder(resp.Body).Decode(&modelList) //nolint:errcheck
	if len(modelList.Data) != 2 {
		t.Fatalf("expected 2 models (prefixed and plain), got %d", len(modelList.Data))
	}

	ids := map[string]bool{}
	for _, m := range modelList.Data {
		ids[m.ID] = true
	}
	if !ids["anthropic/claude-sonnet-4-20250514"] {
		t.Error("expected model 'anthropic/claude-sonnet-4-20250514' not found")
	}
	if !ids["claude-sonnet-4-20250514"] {
		t.Error("expected model 'claude-sonnet-4-20250514' not found")
	}
}

func TestAnthropicCompat_ContextTokenAuthorizationFiltersListModels(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	allowedProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeAnthropic,
			DefaultModel: "claude-sonnet-4-20250514",
		},
	}
	disallowedModelProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "anthropic-haiku", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeAnthropic,
			DefaultModel: "claude-3-5-haiku-20241022",
		},
	}
	disallowedProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "openai", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeOpenAI,
			DefaultModel: "gpt-4o",
		},
	}
	handler, app := setupTestAnthropicHandler(allowedProvider, disallowedModelProvider, disallowedProvider)
	authz, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{Mode: ContextTokenAuthorizationModeEnforce})
	if err != nil {
		t.Fatalf("NewContextTokenAuthorizationConfig returned error: %v", err)
	}
	handler.contextTokenAuthorization = authz

	app.Use(NewAuthMiddleware(handler.client, AuthConfig{ContextTokens: ctxTokenConfig}))
	app.Get("/anthropic/v1/models", handler.HandleListModels)

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeProvidersUse,
		"tctx": map[string]any{
			"allowedProviders": []string{"anthropic", "anthropic-haiku"},
			"allowedModels":    []string{"claude-sonnet-4-20250514"},
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/anthropic/v1/models", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var models OAIModelList
	if err := json.NewDecoder(resp.Body).Decode(&models); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	got := map[string]bool{}
	for _, model := range models.Data {
		got[model.ID] = true
	}
	for _, id := range []string{"anthropic/claude-sonnet-4-20250514", "claude-sonnet-4-20250514"} {
		if !got[id] {
			t.Fatalf("expected allowed model ID %q in response, got %#v", id, got)
		}
	}
	for _, id := range []string{"anthropic-haiku/claude-3-5-haiku-20241022", "claude-3-5-haiku-20241022", "openai/gpt-4o", "gpt-4o"} {
		if got[id] {
			t.Fatalf("did not expect disallowed model ID %q in response: %#v", id, got)
		}
	}
}

// --- Mock provider for tool loop tests ---

type mockAnthropicProvider struct {
	responses    []*llm.CompletionResponse
	errors       []error
	requests     []*llm.CompletionRequest
	callIdx      int
	streamChunks []llm.StreamChunk
	streamErr    error
}

func (m *mockAnthropicProvider) Complete(_ context.Context, req *llm.CompletionRequest) (*llm.CompletionResponse, error) {
	if m.callIdx >= len(m.responses) {
		return nil, fmt.Errorf("unexpected call %d", m.callIdx)
	}
	if req != nil {
		reqCopy := *req
		reqCopy.Messages = append([]llm.Message(nil), req.Messages...)
		reqCopy.Tools = append([]llm.Tool(nil), req.Tools...)
		m.requests = append(m.requests, &reqCopy)
	}
	idx := m.callIdx
	m.callIdx++
	if m.errors != nil && idx < len(m.errors) && m.errors[idx] != nil {
		return nil, m.errors[idx]
	}
	return m.responses[idx], nil
}

func (m *mockAnthropicProvider) Stream(_ context.Context, _ *llm.CompletionRequest) (<-chan llm.StreamChunk, error) {
	if m.streamErr != nil {
		return nil, m.streamErr
	}
	if m.streamChunks == nil {
		return nil, fmt.Errorf("streaming not supported by mock")
	}
	ch := make(chan llm.StreamChunk, len(m.streamChunks))
	for _, chunk := range m.streamChunks {
		ch <- chunk
	}
	close(ch)
	return ch, nil
}

func (m *mockAnthropicProvider) Name() string {
	return "mock-anthropic"
}

func TestHandleStreamingMessages_ForwardsTerminalUsage(t *testing.T) {
	mock := &mockAnthropicProvider{
		streamChunks: []llm.StreamChunk{
			{Content: "hello"},
			{Done: true, StopReason: oaiStopReasonEndTurn, OutputTokens: 11},
		},
	}

	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingMessages(
			c, context.Background(), mock,
			&llm.CompletionRequest{Model: "claude-sonnet-4-20250514"},
			"claude-sonnet-4-20250514", nil,
		)
	})

	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, `"usage":{"input_tokens":0,"output_tokens":11`) {
		t.Fatalf("expected streamed tool-loop usage in body, got: %s", bodyStr)
	}
}

func TestHandleStreamingMessages_FallsBackAfterInitialChunkError(t *testing.T) {
	mock := &mockAnthropicProvider{
		streamChunks: []llm.StreamChunk{{Error: fmt.Errorf("stream unavailable"), Done: true}},
		responses: []*llm.CompletionResponse{{
			Content:    goalStateSentinel + "fallback response",
			StopReason: oaiStopReasonEndTurn,
		}},
	}

	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingMessages(
			c, context.Background(), mock,
			&llm.CompletionRequest{Model: "claude-sonnet-4-20250514"},
			"claude-sonnet-4-20250514", nil,
		)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/test", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if strings.Contains(bodyStr, "event: error") {
		t.Fatalf("initial stream error prevented fallback: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "fallback response") || !strings.Contains(bodyStr, "message_stop") {
		t.Fatalf("expected completed fallback response, got: %s", bodyStr)
	}
	if mock.callIdx != 1 {
		t.Fatalf("Complete calls = %d, want 1", mock.callIdx)
	}
}

func TestHandleStreamingMessages_RejectsInvalidProviderOutcome(t *testing.T) {
	tests := []struct {
		name string
		mock *mockAnthropicProvider
	}{
		{name: "failed stream", mock: &mockAnthropicProvider{streamChunks: []llm.StreamChunk{{Done: true, StopReason: "failed"}}}},
		{name: "missing terminal", mock: &mockAnthropicProvider{streamChunks: []llm.StreamChunk{{Content: "partial"}}}},
		{name: "fallback failed", mock: &mockAnthropicProvider{streamErr: fmt.Errorf("streaming not supported"), responses: []*llm.CompletionResponse{{StopReason: "failed"}}}},
		{name: "fallback nil", mock: &mockAnthropicProvider{streamErr: fmt.Errorf("streaming not supported"), responses: []*llm.CompletionResponse{nil}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, app := setupTestAnthropicHandler()
			app.Post("/test", func(c fiber.Ctx) error {
				return handler.handleStreamingMessages(
					c, context.Background(), tt.mock,
					&llm.CompletionRequest{Model: "claude-sonnet-4-20250514"},
					"claude-sonnet-4-20250514", nil,
				)
			})

			resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/test", nil))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			bodyStr := string(body)
			if !strings.Contains(bodyStr, "event: error") {
				t.Fatalf("expected stream error, got: %s", bodyStr)
			}
			if strings.Contains(bodyStr, "message_stop") {
				t.Fatalf("invalid tool loop completed successfully: %s", bodyStr)
			}
		})
	}
}

func TestHandleStreamingRawMessages_ForwardsTerminalUsage(t *testing.T) {
	mock := &mockAnthropicProvider{
		streamChunks: []llm.StreamChunk{
			{Content: "hello"},
			{Done: true, StopReason: oaiStopReasonEndTurn, OutputTokens: 9},
		},
	}

	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingProxy(
			c, context.Background(), mock,
			&llm.CompletionRequest{Model: "claude-sonnet-4-20250514"},
			"claude-sonnet-4-20250514",
		)
	})

	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, `"usage":{"input_tokens":0,"output_tokens":9`) {
		t.Fatalf("expected streamed usage in body, got: %s", bodyStr)
	}
}

func TestHandleStreamingRawMessages_PreservesRefusal(t *testing.T) {
	mock := &mockAnthropicProvider{
		streamChunks: []llm.StreamChunk{{Done: true, StopReason: "refusal"}},
	}

	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingProxy(
			c, context.Background(), mock,
			&llm.CompletionRequest{Model: "claude-sonnet-4-20250514"},
			"claude-sonnet-4-20250514",
		)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/test", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, `"stop_reason":"refusal"`) {
		t.Fatalf("expected refusal stop reason, got: %s", bodyStr)
	}
	if strings.Contains(bodyStr, "event: error") {
		t.Fatalf("refusal was emitted as an error: %s", bodyStr)
	}
}

func TestHandleStreamingRawMessages_RejectsFailedOutcome(t *testing.T) {
	mock := &mockAnthropicProvider{
		streamChunks: []llm.StreamChunk{{Done: true, StopReason: "failed"}},
	}

	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingProxy(
			c, context.Background(), mock,
			&llm.CompletionRequest{Model: "claude-sonnet-4-20250514"},
			"claude-sonnet-4-20250514",
		)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/test", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "event: error") || !strings.Contains(bodyStr, `non-success outcome \"failed\"`) {
		t.Fatalf("expected stream error, got: %s", bodyStr)
	}
	if strings.Contains(bodyStr, `"stop_reason":"end_turn"`) || strings.Contains(bodyStr, "message_stop") {
		t.Fatalf("failed stream was completed successfully: %s", bodyStr)
	}
}

func TestHandleStreamingRawMessages_PreservesBlankEndTurn(t *testing.T) {
	mock := &mockAnthropicProvider{
		streamChunks: []llm.StreamChunk{{Done: true, StopReason: "end_turn"}},
	}

	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingProxy(
			c, context.Background(), mock,
			&llm.CompletionRequest{Model: "claude-sonnet-4-20250514"},
			"claude-sonnet-4-20250514",
		)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/test", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, `"stop_reason":"end_turn"`) || !strings.Contains(bodyStr, "message_stop") {
		t.Fatalf("expected blank end_turn completion, got: %s", bodyStr)
	}
	if strings.Contains(bodyStr, "event: error") {
		t.Fatalf("blank end_turn emitted an error: %s", bodyStr)
	}
}

func TestHandleStreamingRawMessages_FallbackPreservesBlankEndTurn(t *testing.T) {
	mock := &mockAnthropicProvider{
		streamErr: fmt.Errorf("streaming not supported"),
		responses: []*llm.CompletionResponse{{Content: " \n", StopReason: "end_turn"}},
	}

	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingProxy(
			c, context.Background(), mock,
			&llm.CompletionRequest{Model: "claude-sonnet-4-20250514"},
			"claude-sonnet-4-20250514",
		)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/test", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, `"stop_reason":"end_turn"`) || !strings.Contains(bodyStr, "message_stop") {
		t.Fatalf("expected blank fallback end_turn, got: %s", bodyStr)
	}
	if strings.Contains(bodyStr, "event: error") {
		t.Fatalf("blank fallback emitted an error: %s", bodyStr)
	}
}

func TestHandleStreamingRawMessages_FallbackRejectsFailedOutcome(t *testing.T) {
	mock := &mockAnthropicProvider{
		streamErr: fmt.Errorf("streaming not supported"),
		responses: []*llm.CompletionResponse{{Content: "partial", StopReason: "failed"}},
	}

	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingProxy(
			c, context.Background(), mock,
			&llm.CompletionRequest{Model: "claude-sonnet-4-20250514"},
			"claude-sonnet-4-20250514",
		)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/test", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "event: error") || !strings.Contains(bodyStr, `non-success outcome \"failed\"`) {
		t.Fatalf("expected fallback stream error, got: %s", bodyStr)
	}
	if strings.Contains(bodyStr, "partial") || strings.Contains(bodyStr, "message_stop") {
		t.Fatalf("failed fallback emitted completion data: %s", bodyStr)
	}
}

func TestHandleStreamingRawMessages_FallbackProviderError(t *testing.T) {
	mock := &mockAnthropicProvider{
		streamErr: fmt.Errorf("streaming not supported"),
		responses: []*llm.CompletionResponse{nil},
		errors:    []error{fmt.Errorf("completion failed")},
	}

	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingProxy(
			c, context.Background(), mock,
			&llm.CompletionRequest{Model: "claude-sonnet-4-20250514"},
			"claude-sonnet-4-20250514",
		)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/test", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "event: error") || !strings.Contains(bodyStr, `non-success outcome \"provider_error\"`) {
		t.Fatalf("expected provider error event, got: %s", bodyStr)
	}
	if strings.Contains(bodyStr, "completion failed") || strings.Contains(bodyStr, "message_stop") {
		t.Fatalf("provider error leaked or completed successfully: %s", bodyStr)
	}
}

func TestHandleStreamingRawMessages_FallbackNilResponse(t *testing.T) {
	mock := &mockAnthropicProvider{
		streamErr: fmt.Errorf("streaming not supported"),
		responses: []*llm.CompletionResponse{nil},
	}

	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingProxy(
			c, context.Background(), mock,
			&llm.CompletionRequest{Model: "claude-sonnet-4-20250514"},
			"claude-sonnet-4-20250514",
		)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/test", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "event: error") || !strings.Contains(bodyStr, "without a successful terminal outcome") {
		t.Fatalf("expected nil response error event, got: %s", bodyStr)
	}
	if strings.Contains(bodyStr, "message_stop") {
		t.Fatalf("nil response completed successfully: %s", bodyStr)
	}
}

func TestMapAnthropicStreamStopReason(t *testing.T) {
	tests := []struct {
		name         string
		reason       string
		hasToolCalls bool
		want         string
		wantOK       bool
	}{
		{name: "completed", reason: "end_turn", want: "end_turn", wantOK: true},
		{name: "blank completed", reason: "end_turn", want: "end_turn", wantOK: true},
		{name: "completed with calls", reason: "stop", hasToolCalls: true, want: "tool_use", wantOK: true},
		{name: "empty stop sequence", reason: finishReasonStopSequence, want: finishReasonStopSequence, wantOK: true},
		{name: "stop sequence with calls", reason: finishReasonStopSequence, hasToolCalls: true, want: oaiStopReasonToolUse, wantOK: true},
		{name: "tool use", reason: "tool_calls", hasToolCalls: true, want: "tool_use", wantOK: true},
		{name: "tool reason without calls", reason: "tool_use"},
		{name: "max tokens", reason: "length", want: "max_tokens", wantOK: true},
		{name: "pause turn", reason: "pause_turn", want: "pause_turn", wantOK: true},
		{name: "refusal", reason: "refusal", want: "refusal", wantOK: true},
		{name: "context window exceeded", reason: anthropicStopReasonModelContextWindowExceeded, want: anthropicStopReasonModelContextWindowExceeded, wantOK: true},
		{name: "failed", reason: "failed"},
		{name: "missing reason"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := mapAnthropicStreamStopReason(tt.reason, tt.hasToolCalls)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("mapAnthropicStreamStopReason() = (%q, %t), want (%q, %t)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestHandleStreamingMessages_StripsSentinelAndStreamsSafeToolProgress(t *testing.T) {
	filePath := fmt.Sprintf("/tmp/orka-anthropic-stream-%d.txt", time.Now().UnixNano())
	if err := os.WriteFile(filePath, []byte("anthropic streaming secret content"), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(filePath) })

	mock := &mockAnthropicProvider{
		responses: []*llm.CompletionResponse{
			{
				Content:    "Reading file before final answer.\n",
				StopReason: oaiStopReasonToolUse,
				ToolCalls: []llm.ToolCall{{
					ID:        "call_file_read",
					Name:      "file_read",
					Arguments: json.RawMessage(fmt.Sprintf(`{"path":%q}`, filePath)),
				}},
			},
			{
				Content:    goalStateSentinel + "\nPR ready: https://example.test/pr/4",
				StopReason: oaiStopReasonEndTurn,
			},
		},
	}

	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingMessages(
			c, context.Background(), mock,
			&llm.CompletionRequest{
				Model:    "claude-sonnet-4-20250514",
				Messages: []llm.Message{{Role: testRoleUser, Content: "read then report"}},
				Tools:    []llm.Tool{{Name: "file_read"}},
			},
			"claude-sonnet-4-20250514", nil,
		)
	})

	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	for _, want := range []string{
		"Reading file before final answer.",
		"[Tool file_read completed]",
		"PR ready: https://example.test/pr/4",
		"message_stop",
	} {
		if !strings.Contains(bodyStr, want) {
			t.Fatalf("expected stream body to contain %q, got: %s", want, bodyStr)
		}
	}
	for _, forbidden := range []string{
		goalStateSentinel,
		"anthropic streaming secret content",
		"premature end",
		"call_file_read",
	} {
		if strings.Contains(bodyStr, forbidden) {
			t.Fatalf("stream body leaked internal content %q: %s", forbidden, bodyStr)
		}
	}
	if mock.callIdx != 2 {
		t.Fatalf("expected 2 LLM calls, got %d", mock.callIdx)
	}
}

func TestHandleStreamingMessages_DoesNotStreamPrematureCoordinatorText(t *testing.T) {
	mock := &mockAnthropicProvider{
		responses: []*llm.CompletionResponse{
			{Content: "premature anthropic secret progress", StopReason: oaiStopReasonEndTurn},
			{Content: goalStateSentinel + "\nPR ready: https://example.test/pr/6", StopReason: oaiStopReasonEndTurn},
		},
	}

	handler, app := setupTestAnthropicHandler()
	handler.config.MaxPrematureEndRetries = 1
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingMessages(
			c, context.Background(), mock,
			&llm.CompletionRequest{
				Model:    "claude-sonnet-4-20250514",
				Messages: []llm.Message{{Role: testRoleUser, Content: "ship"}},
			},
			"claude-sonnet-4-20250514", nil,
		)
	})

	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if strings.Contains(bodyStr, "premature anthropic secret progress") {
		t.Fatalf("stream body leaked premature coordinator text: %s", bodyStr)
	}
	for _, want := range []string{"[Continuing workflow...]", "PR ready: https://example.test/pr/6", "message_stop"} {
		if !strings.Contains(bodyStr, want) {
			t.Fatalf("expected stream body to contain %q, got: %s", want, bodyStr)
		}
	}
	if mock.callIdx != 2 {
		t.Fatalf("expected 2 LLM calls, got %d", mock.callIdx)
	}
}

// --- Tests: injectOrkaTools ---

func TestInjectOrkaTools_BuiltinTools(t *testing.T) {
	handler, _ := setupTestAnthropicHandler()
	req := &llm.CompletionRequest{}
	injectOrkaTools(context.Background(), handler.client, req, "default")

	if len(req.Tools) < len(builtinProxyTools) {
		t.Fatalf("expected at least %d tools, got %d", len(builtinProxyTools), len(req.Tools))
	}

	names := map[string]bool{}
	for _, tool := range req.Tools {
		names[tool.Name] = true
	}
	for _, expected := range builtinProxyTools {
		if !names[expected] {
			t.Errorf("expected built-in tool %q not found", expected)
		}
	}
}

func TestInjectOrkaTools_PreservesClientTools(t *testing.T) {
	handler, _ := setupTestAnthropicHandler()
	clientTool := llm.Tool{
		Name:        "my_custom_tool",
		Description: "A client-provided tool",
		Parameters:  json.RawMessage(`{"type":"object"}`),
	}
	req := &llm.CompletionRequest{Tools: []llm.Tool{clientTool}}

	injectOrkaTools(context.Background(), handler.client, req, "default")

	// Client tool should still be first
	if req.Tools[0].Name != "my_custom_tool" {
		t.Errorf("first tool = %q, want my_custom_tool", req.Tools[0].Name)
	}
	// Built-in tools should also be present
	names := map[string]bool{}
	for _, tool := range req.Tools {
		names[tool.Name] = true
	}
	for _, expected := range builtinProxyTools {
		if !names[expected] {
			t.Errorf("expected built-in tool %q not found after injection", expected)
		}
	}
	// Total should be client + builtins
	if len(req.Tools) < 1+len(builtinProxyTools) {
		t.Errorf("expected at least %d tools, got %d", 1+len(builtinProxyTools), len(req.Tools))
	}
}

func TestInjectOrkaTools_WithToolCRDs(t *testing.T) {
	toolCRD := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "custom-tool", Namespace: "default"},
		Spec: corev1alpha1.ToolSpec{
			Description: "A custom tool",
			Parameters:  &apiextensionsv1.JSON{Raw: json.RawMessage(`{"type":"object"}`)},
			HTTP:        &corev1alpha1.HTTPExecution{URL: "http://example.com/tool"},
		},
	}

	handler, _ := setupTestAnthropicHandler(toolCRD)
	req := &llm.CompletionRequest{}
	injectOrkaTools(context.Background(), handler.client, req, "default")

	names := map[string]bool{}
	for _, tool := range req.Tools {
		names[tool.Name] = true
	}
	if !names["custom-tool"] {
		t.Error("expected Tool CRD 'custom-tool' not found in injected tools")
	}
	for _, expected := range builtinProxyTools {
		if !names[expected] {
			t.Errorf("expected built-in tool %q not found", expected)
		}
	}
}

// TestInjectOrkaTools_CoordinatorToolsAllRegistered guards against a class of
// outages where coordinatorProxyTools lists a tool name that is not registered
// in DefaultRegistry. When that happens ToLLMTools silently drops the tool, the
// model never sees it in its tool list, but the system prompt still tells the
// model to call it. The chat-to-PR demo finished all the real work and then
// blew up with "tool create_pull_request is not available in this request"
// because exactly this drift had crept in.
//
// The test calls every registration path the controller's main.go uses so the
// assertion is "all advertised tools are reachable in DefaultRegistry once the
// controller is fully wired", not "this single registration path covers them".
func TestInjectOrkaTools_CoordinatorToolsAllRegistered(t *testing.T) {
	handler, _ := setupTestAnthropicHandler()
	tools.RegisterProxyPRTools(handler.client)

	req := &llm.CompletionRequest{}
	injectOrkaTools(context.Background(), handler.client, req, "default")

	names := map[string]bool{}
	for _, tool := range req.Tools {
		names[tool.Name] = true
	}
	for _, expected := range coordinatorProxyTools {
		if !names[expected] {
			t.Errorf("coordinatorProxyTools advertises %q but it is not registered in DefaultRegistry; "+
				"add it to RegisterChatTools, RegisterProxyPRTools, or another initializer the controller calls before serving traffic", expected)
		}
	}
}

// --- Tests: runNonStreamingToolLoop ---

// Demo 10 run 2026-06-01 07:40 PT regressed: coordinator stopped at iter 9
// emitting "## Progress Summary" + "Hard-limit budget remaining" markdown
// (instead of a tool_use) right after wait_for_task on the discovery
// container task. This terminated the SSE stream — validation, review, PR
// never ran.
//
// Two structural fixes in this commit:
//
//  1. anthropic_tool_loop.go (and anthropic_streaming.go) used to inject
//     "[System: Progress check — summarize what you've done so far and what
//     remains.]" every 5 iterations. That message literally instructed the
//     model to emit the progress-summary pattern we'd then blame it for.
//     REMOVED in both files.
//
//  2. The "no tool calls → return" branch in both paths now requires the
//     response to contain the literal goalStateSentinel
//     ("<ORKA_GOAL_STATE_REACHED>"). Without it, the server treats the text
//     as a premature progress summary, injects a "continue with tool_use"
//     reminder, and re-loops up to ChatConfig.MaxPrematureEndRetries times
//     before giving up and returning the response anyway.
//
// This test pins behavior (1)+(2) for the non-streaming path. Streaming
// equivalent: TestStreamingToolLoop_PrematureEndIsRetried below.
func TestRunNonStreamingToolLoop_PrematureEndIsRetried(t *testing.T) {
	_, _ = setupTestAnthropicHandler()
	mock := &mockAnthropicProvider{
		responses: []*llm.CompletionResponse{
			// Iteration 0: model emits markdown progress summary, no tool_use.
			// Without the safety net, this would be returned to the client.
			{Content: "## Progress Summary\n\nI've done X.\n\n### Remaining\n1. Y", StopReason: "end_turn"},
			// Iteration 1 (re-prompt): model still hasn't included sentinel.
			// Still no tool_use; still must retry.
			{Content: "Continuing... I'll do Y next.", StopReason: "end_turn"},
			// Iteration 2 (re-prompt): model finally emits the sentinel + body.
			// This response must be returned to the client without further re-prompting.
			{Content: "<ORKA_GOAL_STATE_REACHED>\nPR ready: https://example.test/pr/1", StopReason: "end_turn"},
		},
	}
	req := &llm.CompletionRequest{
		Model:    "test-model",
		Messages: []llm.Message{{Role: "user", Content: "ship a PR"}},
	}

	resp, err := runNonStreamingToolLoop(context.Background(), mock, req, "test-model",
		ChatConfig{MaxIterations: 20, ToolTimeout: 30 * time.Second, MaxPrematureEndRetries: 3}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(resp.Content, "ORKA_GOAL_STATE_REACHED") {
		t.Errorf("final content should contain the sentinel, got %q", resp.Content)
	}
	if mock.callIdx != 3 {
		t.Errorf("expected 3 LLM calls (one initial + 2 retries until sentinel), got %d", mock.callIdx)
	}
}

// If the model never emits the sentinel, the loop must still terminate after
// MaxPrematureEndRetries+1 attempts. Otherwise a stubborn model could pin a
// chat session forever.
func TestRunNonStreamingToolLoop_PrematureEndExhaustsBudget(t *testing.T) {
	_, _ = setupTestAnthropicHandler()
	mock := &mockAnthropicProvider{
		responses: []*llm.CompletionResponse{
			{Content: "summary 1", StopReason: "end_turn"},
			{Content: "summary 2", StopReason: "end_turn"},
			{Content: "summary 3 (final, but still no sentinel)", StopReason: "end_turn"},
			// If a 4th call happens, the test fails.
			{Content: "should not happen", StopReason: "end_turn"},
		},
	}
	req := &llm.CompletionRequest{
		Model:    "test-model",
		Messages: []llm.Message{{Role: "user", Content: "ship a PR"}},
	}

	resp, err := runNonStreamingToolLoop(context.Background(), mock, req, "test-model",
		ChatConfig{MaxIterations: 20, ToolTimeout: 30 * time.Second, MaxPrematureEndRetries: 2}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Initial call + 2 retries = 3 calls. The 3rd response is returned as-is.
	if mock.callIdx != 3 {
		t.Errorf("expected 3 LLM calls, got %d", mock.callIdx)
	}
	if !strings.Contains(resp.Content, "summary 3") {
		t.Errorf("expected final summary content, got %q", resp.Content)
	}
}

func TestRunNonStreamingToolLoop_NoToolCalls(t *testing.T) {
	_, _ = setupTestAnthropicHandler()
	mock := &mockAnthropicProvider{
		responses: []*llm.CompletionResponse{
			{Content: testHelloContent, StopReason: "end_turn"},
		},
	}
	req := &llm.CompletionRequest{
		Model:    "test-model",
		Messages: []llm.Message{{Role: "user", Content: "Hi"}},
	}

	resp, err := runNonStreamingToolLoop(context.Background(), mock, req, "test-model", ChatConfig{MaxIterations: 20, ToolTimeout: 30 * time.Second}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Content != testHelloContent {
		t.Errorf("content = %q, want Hello!", resp.Content)
	}
	if mock.callIdx != 1 {
		t.Errorf("expected 1 LLM call, got %d", mock.callIdx)
	}
}

func TestRunNonStreamingToolLoop_SingleToolCall(t *testing.T) {
	_, _ = setupTestAnthropicHandler()
	mock := &mockAnthropicProvider{
		responses: []*llm.CompletionResponse{
			{
				Content:    "Let me read the file.",
				StopReason: oaiStopReasonToolUse,
				ToolCalls: []llm.ToolCall{
					{ID: "tc_1", Name: "file_read", Arguments: json.RawMessage(`{"path":"test.txt"}`)},
				},
			},
			{Content: "The file contains: hello world", StopReason: "end_turn"},
		},
	}
	req := &llm.CompletionRequest{
		Model:    "test-model",
		Messages: []llm.Message{{Role: "user", Content: "Read test.txt"}},
	}

	resp, err := runNonStreamingToolLoop(context.Background(), mock, req, "test-model", ChatConfig{MaxIterations: 20, ToolTimeout: 30 * time.Second}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Content != "The file contains: hello world" {
		t.Errorf("content = %q, want final response", resp.Content)
	}
	if mock.callIdx != 2 {
		t.Errorf("expected 2 LLM calls, got %d", mock.callIdx)
	}
}

func TestRunNonStreamingToolLoop_MultiStepToolLoop(t *testing.T) {
	_, _ = setupTestAnthropicHandler()
	mock := &mockAnthropicProvider{
		responses: []*llm.CompletionResponse{
			{
				StopReason: oaiStopReasonToolUse,
				ToolCalls:  []llm.ToolCall{{ID: "tc_1", Name: "web_search", Arguments: json.RawMessage(`{"query":"test"}`)}},
			},
			{
				StopReason: oaiStopReasonToolUse,
				ToolCalls:  []llm.ToolCall{{ID: "tc_2", Name: "file_read", Arguments: json.RawMessage(`{"path":"result.txt"}`)}},
			},
			{Content: "Here is the final answer.", StopReason: "end_turn"},
		},
	}
	req := &llm.CompletionRequest{
		Model:    "test-model",
		Messages: []llm.Message{{Role: "user", Content: "Search and read"}},
	}

	resp, err := runNonStreamingToolLoop(context.Background(), mock, req, "test-model", ChatConfig{MaxIterations: 20, ToolTimeout: 30 * time.Second}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Content != "Here is the final answer." {
		t.Errorf("content = %q, want final answer", resp.Content)
	}
	if mock.callIdx != 3 {
		t.Errorf("expected 3 LLM calls, got %d", mock.callIdx)
	}
}

func TestRunNonStreamingToolLoop_IterationLimit(t *testing.T) {
	config := DefaultChatConfig()
	config.MaxIterations = 2

	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	_ = NewAnthropicCompatHandler(fakeClient, nil, "default", false, config, NewProviderResolver(fakeClient, config), nil)

	mock := &mockAnthropicProvider{
		responses: []*llm.CompletionResponse{
			// Iteration 0: tool call
			{StopReason: oaiStopReasonToolUse, ToolCalls: []llm.ToolCall{{ID: "tc_1", Name: "web_search", Arguments: json.RawMessage(`{"query":"a"}`)}}},
			// Iteration 1: tool call
			{StopReason: oaiStopReasonToolUse, ToolCalls: []llm.ToolCall{{ID: "tc_2", Name: "web_search", Arguments: json.RawMessage(`{"query":"b"}`)}}},
			// Iteration 2: hits limit, summary call
			{Content: "Summary of work done.", StopReason: "end_turn"},
		},
	}
	req := &llm.CompletionRequest{
		Model:    "test-model",
		Messages: []llm.Message{{Role: "user", Content: "Do many things"}},
	}

	resp, err := runNonStreamingToolLoop(context.Background(), mock, req, "test-model", ChatConfig{MaxIterations: 20, ToolTimeout: 30 * time.Second}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Content != "Summary of work done." {
		t.Errorf("content = %q, want summary", resp.Content)
	}
	if mock.callIdx != 3 {
		t.Errorf("expected 3 LLM calls (2 iterations + 1 summary), got %d", mock.callIdx)
	}
}

func TestRunNonStreamingToolLoop_LLMError(t *testing.T) {
	_, _ = setupTestAnthropicHandler()
	mock := &mockAnthropicProvider{
		responses: []*llm.CompletionResponse{nil},
		errors:    []error{fmt.Errorf("provider unavailable")},
	}
	req := &llm.CompletionRequest{
		Model:    "test-model",
		Messages: []llm.Message{{Role: "user", Content: "Hi"}},
	}

	_, err := runNonStreamingToolLoop(context.Background(), mock, req, "test-model", ChatConfig{MaxIterations: 20, ToolTimeout: 30 * time.Second}, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "provider unavailable") {
		t.Errorf("error = %q, want 'provider unavailable'", err.Error())
	}
}

func TestRunNonStreamingToolLoop_ToolExecutionError(t *testing.T) {
	_, _ = setupTestAnthropicHandler()
	mock := &mockAnthropicProvider{
		responses: []*llm.CompletionResponse{
			{
				StopReason: oaiStopReasonToolUse,
				ToolCalls:  []llm.ToolCall{{ID: "tc_1", Name: "nonexistent_tool", Arguments: json.RawMessage(`{}`)}},
			},
			{Content: "I encountered an error but here is my response.", StopReason: "end_turn"},
		},
	}
	req := &llm.CompletionRequest{
		Model:    "test-model",
		Messages: []llm.Message{{Role: "user", Content: "Use a bad tool"}},
	}

	resp, err := runNonStreamingToolLoop(context.Background(), mock, req, "test-model", ChatConfig{MaxIterations: 20, ToolTimeout: 30 * time.Second}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Content != "I encountered an error but here is my response." {
		t.Errorf("content = %q, want error recovery response", resp.Content)
	}
	if mock.callIdx != 2 {
		t.Errorf("expected 2 LLM calls, got %d", mock.callIdx)
	}
}

func TestRunNonStreamingToolLoop_RejectsToolNotExposedInRequest(t *testing.T) {
	mock := &mockAnthropicProvider{
		responses: []*llm.CompletionResponse{
			{
				StopReason: oaiStopReasonToolUse,
				ToolCalls:  []llm.ToolCall{{ID: "tc_1", Name: "disallowed_tool", Arguments: json.RawMessage(`{}`)}},
			},
			{Content: "done", StopReason: "end_turn"},
		},
	}
	req := &llm.CompletionRequest{
		Model:    "test-model",
		Messages: []llm.Message{{Role: "user", Content: "Use a disallowed tool"}},
		Tools:    []llm.Tool{{Name: "allowed_tool"}},
	}

	resp, err := runNonStreamingToolLoop(context.Background(), mock, req, "test-model", ChatConfig{MaxIterations: 20, ToolTimeout: 30 * time.Second}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Content != "done" {
		t.Fatalf("content = %q, want final response", resp.Content)
	}
	if len(mock.requests) < 2 {
		t.Fatalf("expected at least 2 LLM calls, got %d", len(mock.requests))
	}

	var toolResult string
	for _, msg := range mock.requests[1].Messages {
		if msg.Role == testRoleTool && msg.ToolCallID == "tc_1" {
			toolResult = msg.Content
			break
		}
	}
	if toolResult == "" {
		t.Fatal("expected disallowed tool result to be added to next request")
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(toolResult), &parsed); err != nil {
		t.Fatalf("tool result is not valid JSON: %v", err)
	}
	if parsed["success"] != false {
		t.Fatalf("expected success=false, got %v", parsed["success"])
	}
	errMsg, ok := parsed["error"].(string)
	if !ok || !strings.Contains(errMsg, `tool "disallowed_tool" is not available in this request`) {
		t.Fatalf("expected unavailable tool error, got %v", parsed["error"])
	}
}

// --- Tests: executeToolCall ---

func TestExecuteToolCall_UnknownTool(t *testing.T) {
	result := executeToolCall(context.Background(), llm.ToolCall{
		ID:        "tc_1",
		Name:      "nonexistent_tool",
		Arguments: json.RawMessage(`{}`),
	}, 10*time.Second, nil)

	var parsed map[string]any
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if parsed["success"] != false {
		t.Errorf("expected success=false, got %v", parsed["success"])
	}
	errMsg, ok := parsed["error"].(string)
	if !ok || errMsg == "" {
		t.Errorf("expected non-empty error message, got %v", parsed["error"])
	}
}

func TestExecuteToolCall_Timeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// Use an unknown tool so the registry returns an error, validating the timeout/error path
	result := executeToolCall(ctx, llm.ToolCall{
		ID:        "tc_1",
		Name:      "nonexistent_timeout_tool",
		Arguments: json.RawMessage(`{}`),
	}, 1*time.Millisecond, nil)

	var parsed map[string]any
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if parsed["success"] != false {
		t.Errorf("expected success=false, got %v", parsed["success"])
	}
}

func TestAnthropicCompat_ContextTokenAuthorizationRejectsDisallowedProvider(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	llmProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeAnthropic,
			DefaultModel: "claude-sonnet-4-20250514",
			SecretRef:    corev1alpha1.ProviderSecretRef{Name: "anthropic-secret", Key: "api-key"},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "anthropic-secret", Namespace: "default"},
		Data:       map[string][]byte{"api-key": []byte("test-key")},
	}
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
			"allowedProviders": []string{"openai"},
		},
	})
	body := `{"model":"anthropic/claude-sonnet-4-20250514","messages":[{"role":"user","content":"hello"}],"max_tokens":100}`
	req := httptest.NewRequest(http.MethodPost, "/anthropic/v1/messages", strings.NewReader(body))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
}

func TestHandleStreamingMessages_MaxTokensTextTerminatesWithMaxTokens(t *testing.T) {
	// A text-only response truncated by the caller's max_tokens budget must be
	// delivered with stop_reason max_tokens instead of entering the
	// premature-end retry loop and ending as end_turn.
	mock := &mockAnthropicProvider{
		streamChunks: []llm.StreamChunk{
			{Content: "partial answer that ran out of"},
			{Done: true, StopReason: oaiParamMaxTokens, OutputTokens: 7},
		},
	}
	handler, app := setupTestAnthropicHandler()
	app.Post("/test", func(c fiber.Ctx) error {
		return handler.handleStreamingMessages(
			c, context.Background(), mock,
			&llm.CompletionRequest{Model: "claude-sonnet-4-20250514", MaxTokens: 7},
			"claude-sonnet-4-20250514", nil,
		)
	})
	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/test", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, `"stop_reason":"max_tokens"`) {
		t.Fatalf("expected max_tokens stop reason in stream, got: %s", bodyStr)
	}
	if strings.Contains(bodyStr, `"stop_reason":"end_turn"`) || strings.Contains(bodyStr, "Continuing workflow") {
		t.Fatalf("truncated text was retried or reported as end_turn: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "message_stop") {
		t.Fatalf("stream did not terminate cleanly: %s", bodyStr)
	}
	if mock.callIdx > 0 {
		t.Fatalf("expected no non-streaming retry after the truncated text, got %d Complete calls", mock.callIdx)
	}
}
