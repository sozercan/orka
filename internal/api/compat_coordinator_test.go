/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"

	"github.com/orka-agents/orka/internal/llm"
)

func TestPrepareCompatCoordinatorToolsDisabledPreservesRequest(t *testing.T) {
	clientTool := llm.Tool{Name: "client_tool"}
	req := &llm.CompletionRequest{
		Tools:        []llm.Tool{clientTool},
		SystemPrompt: "original prompt",
		Messages: []llm.Message{
			{Role: testRoleAssistant, ToolCalls: []llm.ToolCall{{ID: "call-1", Name: "client_tool"}}},
			{Role: testRoleTool, Content: "client result"},
		},
	}
	var enabled bool
	var setupErr error
	app := fiber.New()
	app.Post("/", func(c fiber.Ctx) error {
		enabled, setupErr = prepareCompatCoordinatorTools(c, req, compatCoordinatorSetup{
			Namespace:           "default",
			ToolUseAction:       "testTools",
			AuthorizationConfig: ContextTokenAuthorizationConfig{},
		})
		return nil
	})
	httpReq := httptest.NewRequest(http.MethodPost, "/", nil)
	httpReq.Header.Set("X-Orka-Tools", "disabled")
	resp, err := app.Test(httpReq)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if setupErr != nil || enabled {
		t.Fatalf("enabled=%v err=%v, want disabled nil error", enabled, setupErr)
	}
	if len(req.Tools) != 1 || req.Tools[0].Name != clientTool.Name {
		t.Fatalf("tools = %#v, want preserved client tool", req.Tools)
	}
	if req.SystemPrompt != "original prompt" || len(req.Messages) != 2 {
		t.Fatalf("request mutated while disabled: prompt=%q messages=%#v", req.SystemPrompt, req.Messages)
	}
}

func TestPrepareCompatCoordinatorToolsEnabledInjectsPromptToolsAndStripsClientToolMessages(t *testing.T) {
	req := &llm.CompletionRequest{
		Tools:        []llm.Tool{{Name: "client_tool"}},
		SystemPrompt: "original prompt",
		Messages: []llm.Message{
			{Role: testRoleUser, Content: "hello"},
			{Role: testRoleAssistant, Content: "running tool", ToolCalls: []llm.ToolCall{{ID: "call-1", Name: "client_tool"}}},
			{Role: testRoleTool, Content: "client result"},
		},
	}
	var enabled bool
	var setupErr error
	app := fiber.New()
	app.Post("/", func(c fiber.Ctx) error {
		enabled, setupErr = prepareCompatCoordinatorTools(c, req, compatCoordinatorSetup{
			Namespace:           "default",
			ToolUseAction:       "testTools",
			AuthorizationConfig: ContextTokenAuthorizationConfig{},
		})
		return nil
	})
	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if setupErr != nil || !enabled {
		t.Fatalf("enabled=%v err=%v, want enabled nil error", enabled, setupErr)
	}
	toolNames := completionToolNames(req.Tools)
	if slices.Contains(toolNames, "client_tool") {
		t.Fatalf("client tool was preserved in coordinator mode: %#v", toolNames)
	}
	if !slices.Contains(toolNames, "web_search") || !slices.Contains(toolNames, "create_agent_task") {
		t.Fatalf("injected tool names = %#v, want builtin and coordinator tools", toolNames)
	}
	if !strings.HasPrefix(req.SystemPrompt, coordinatorSystemPrompt("default")+"\n\n") {
		t.Fatalf("system prompt = %q, want coordinator prefix", req.SystemPrompt)
	}
	if len(req.Messages) != 2 || req.Messages[1].Role != testRoleAssistant || len(req.Messages[1].ToolCalls) != 0 {
		t.Fatalf("messages = %#v, want tool result removed and assistant tool calls stripped", req.Messages)
	}
}

func TestPrepareCompatCoordinatorToolsPropagatesToolAuthorizationAction(t *testing.T) {
	authz, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{Mode: ContextTokenAuthorizationModeEnforce})
	if err != nil {
		t.Fatalf("NewContextTokenAuthorizationConfig: %v", err)
	}
	req := &llm.CompletionRequest{}
	var setupErr error
	app := fiber.New()
	app.Post("/", func(c fiber.Ctx) error {
		c.Locals(UserInfoContextKey, &UserInfo{AuthType: AuthTypeContextToken, ContextToken: &ContextToken{Scopes: []string{}}})
		_, setupErr = prepareCompatCoordinatorTools(c, req, compatCoordinatorSetup{
			Namespace:           "default",
			ToolUseAction:       "openAITools",
			AuthorizationConfig: authz,
		})
		return nil
	})
	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if setupErr == nil || !strings.Contains(setupErr.Error(), "openAITools") {
		t.Fatalf("setupErr = %v, want tool-use auth error with action", setupErr)
	}
}

func TestPrepareCompatCoordinatorToolsFiltersAllowedToolsBeforeAuthorizing(t *testing.T) {
	authz, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{Mode: ContextTokenAuthorizationModeEnforce})
	if err != nil {
		t.Fatalf("NewContextTokenAuthorizationConfig: %v", err)
	}
	req := &llm.CompletionRequest{}
	var setupErr error
	app := fiber.New()
	app.Post("/", func(c fiber.Ctx) error {
		c.Locals(UserInfoContextKey, &UserInfo{AuthType: AuthTypeContextToken, ContextToken: &ContextToken{
			Scopes:             []string{ContextTokenScopeToolsUse},
			TransactionContext: map[string]any{"allowedTools": []string{"file_read"}},
		}})
		_, setupErr = prepareCompatCoordinatorTools(c, req, compatCoordinatorSetup{
			Namespace:           "default",
			ToolUseAction:       "anthropicTools",
			AuthorizationConfig: authz,
		})
		return nil
	})
	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if setupErr != nil {
		t.Fatalf("setupErr = %v, want allowed filtered tools", setupErr)
	}
	if got := completionToolNames(req.Tools); len(got) != 1 || got[0] != "file_read" {
		t.Fatalf("tool names = %#v, want only file_read", got)
	}
}

func TestPrepareCompatCoordinatorToolsToolUseAuthErrorDoesNotApplyPromptOrStripMessages(t *testing.T) {
	authz, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{Mode: ContextTokenAuthorizationModeEnforce})
	if err != nil {
		t.Fatalf("NewContextTokenAuthorizationConfig: %v", err)
	}
	req := &llm.CompletionRequest{
		SystemPrompt: "original prompt",
		Messages: []llm.Message{
			{Role: testRoleAssistant, Content: "running", ToolCalls: []llm.ToolCall{{ID: "call-1", Name: "client_tool"}}},
			{Role: testRoleTool, Content: "client result"},
		},
	}
	var setupErr error
	app := fiber.New()
	app.Post("/", func(c fiber.Ctx) error {
		c.Locals(UserInfoContextKey, &UserInfo{AuthType: AuthTypeContextToken, ContextToken: &ContextToken{Scopes: []string{}}})
		_, setupErr = prepareCompatCoordinatorTools(c, req, compatCoordinatorSetup{
			Namespace:           "default",
			ToolUseAction:       "openAITools",
			AuthorizationConfig: authz,
		})
		return nil
	})
	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if setupErr == nil || !strings.Contains(setupErr.Error(), "openAITools") {
		t.Fatalf("setupErr = %v, want tool auth failure", setupErr)
	}
	if req.SystemPrompt != "original prompt" {
		t.Fatalf("system prompt = %q, want unchanged", req.SystemPrompt)
	}
	if len(req.Messages) != 2 || len(req.Messages[0].ToolCalls) != 1 || req.Messages[1].Role != testRoleTool {
		t.Fatalf("messages = %#v, want unstripped client tool history", req.Messages)
	}
}
