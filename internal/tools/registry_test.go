/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/orka-agents/orka/internal/executionmode"
)

// mockTool is a simple mock tool for testing
type mockTool struct {
	name        string
	description string
	parameters  json.RawMessage
	executeFunc func(ctx context.Context, args json.RawMessage) (string, error)
}

func (m *mockTool) Name() string {
	return m.name
}

func (m *mockTool) Description() string {
	return m.description
}

func (m *mockTool) Parameters() json.RawMessage {
	return m.parameters
}

func (m *mockTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, args)
	}
	return "executed", nil
}

func TestNewRegistry(t *testing.T) {
	r := NewRegistry()
	if r == nil {
		t.Fatal("NewRegistry returned nil")
	}
	if r.tools == nil {
		t.Fatal("tools map is nil")
	}
	if len(r.tools) != 0 {
		t.Errorf("expected empty registry, got %d tools", len(r.tools))
	}
}

func TestRegistry_Register(t *testing.T) {
	r := NewRegistry()
	tool := &mockTool{name: testToolName, description: testToolDescription}

	r.Register(tool)

	if len(r.tools) != 1 {
		t.Errorf("expected 1 tool, got %d", len(r.tools))
	}
	if _, ok := r.tools[testToolName]; !ok {
		t.Error("tool not registered with correct name")
	}
}

func TestRegistry_Register_Overwrite(t *testing.T) {
	r := NewRegistry()
	tool1 := &mockTool{name: testToolName, description: "First tool"}
	tool2 := &mockTool{name: testToolName, description: testSecondToolDescription}

	r.Register(tool1)
	r.Register(tool2)

	if len(r.tools) != 1 {
		t.Errorf("expected 1 tool, got %d", len(r.tools))
	}
	if r.tools[testToolName].Description() != testSecondToolDescription {
		t.Error("tool was not overwritten")
	}
}

func TestRegistry_Get(t *testing.T) {
	tests := []struct {
		name      string
		toolName  string
		wantFound bool
	}{
		{
			name:      "found",
			toolName:  testToolName,
			wantFound: true,
		},
		{
			name:      notFoundMessage,
			toolName:  testNonexistentName,
			wantFound: false,
		},
	}

	r := NewRegistry()
	r.Register(&mockTool{name: testToolName, description: testToolDescription})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tool, ok := r.Get(tt.toolName)
			if ok != tt.wantFound {
				t.Errorf("Get() found = %v, want %v", ok, tt.wantFound)
			}
			if tt.wantFound && tool == nil {
				t.Error("Get() returned nil tool when found")
			}
			if !tt.wantFound && tool != nil {
				t.Error("Get() returned non-nil tool when not found")
			}
		})
	}
}

func TestRegistry_Execute(t *testing.T) {
	tests := []struct {
		name     string
		toolName string
		args     json.RawMessage
		wantErr  bool
		wantResp string
	}{
		{
			name:     "tool found",
			toolName: testToolName,
			args:     json.RawMessage(`{"key": "value"}`),
			wantErr:  false,
			wantResp: "executed",
		},
		{
			name:     "tool not found",
			toolName: testNonexistentName,
			args:     json.RawMessage(`{}`),
			wantErr:  true,
		},
	}

	r := NewRegistry()
	r.Register(&mockTool{name: testToolName})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := r.Execute(context.Background(), tt.toolName, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result != tt.wantResp {
				t.Errorf("Execute() = %v, want %v", result, tt.wantResp)
			}
		})
	}
}

func TestRegistry_ToLLMTools(t *testing.T) {
	tests := []struct {
		name    string
		tools   []Tool
		names   []string
		wantLen int
	}{
		{
			name: "all tools exist",
			tools: []Tool{
				&mockTool{name: testTool1Name, description: testTool1Description, parameters: json.RawMessage(`{"type": "object"}`)},
				&mockTool{name: testTool2Name, description: "Tool 2", parameters: json.RawMessage(`{"type": "object"}`)},
			},
			names:   []string{testTool1Name, testTool2Name},
			wantLen: 2,
		},
		{
			name: "some tools don't exist",
			tools: []Tool{
				&mockTool{name: testTool1Name, description: testTool1Description, parameters: json.RawMessage(`{"type": "object"}`)},
			},
			names:   []string{testTool1Name, testTool2Name, "tool3"},
			wantLen: 1,
		},
		{
			name: "no tools exist",
			tools: []Tool{
				&mockTool{name: testTool1Name, description: testTool1Description, parameters: json.RawMessage(`{"type": "object"}`)},
			},
			names:   []string{"nonexistent1", "nonexistent2"},
			wantLen: 0,
		},
		{
			name:    "empty names",
			tools:   []Tool{&mockTool{name: testTool1Name}},
			names:   []string{},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry()
			for _, tool := range tt.tools {
				r.Register(tool)
			}

			llmTools := r.ToLLMTools(tt.names)
			if len(llmTools) != tt.wantLen {
				t.Errorf("ToLLMTools() len = %d, want %d", len(llmTools), tt.wantLen)
			}
		})
	}
}

func TestRegistryNamesSorted(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockTool{name: "zeta", description: "z"})
	r.Register(&mockTool{name: "alpha", description: "a"})
	r.Register(&mockTool{name: "middle", description: "m"})

	got := r.Names()
	want := []string{"alpha", "middle", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Names() = %#v, want %#v", got, want)
	}
}

func TestCoordinationToolNamesReflectRegisteredCoordinationTools(t *testing.T) {
	got := CoordinationToolNames()
	if slices.Contains(got, updateAgentToolName) {
		t.Fatalf("CoordinationToolNames() contains chat-only tool %q: %#v", updateAgentToolName, got)
	}
	if !slices.Contains(ChatToolNames(), updateAgentToolName) {
		t.Fatalf("ChatToolNames() missing %q", updateAgentToolName)
	}
}

func TestKnownBuiltInToolNamesIncludesRegisteredChatAndCoordinationTools(t *testing.T) {
	wantNames := []string{
		requestApprovalToolName,
		webSearchToolName,
		createAITaskToolName,
		createToolCRDToolName,
		delegateTaskToolName,
		sendMessageToolName,
		checkPRReviewMarkerToolName,
		commentOnIssueToolName,
		updateAgentToolName,
	}
	got := KnownBuiltInToolNames()
	for _, want := range wantNames {
		if !slices.Contains(got, want) {
			t.Fatalf("KnownBuiltInToolNames() missing %q from %#v", want, got)
		}
	}
}

func TestDefaultRegistry(t *testing.T) {
	// DefaultRegistry should have built-in tools registered via init()
	if DefaultRegistry == nil {
		t.Fatal("DefaultRegistry is nil")
	}

	// Check that built-in tools are registered
	expectedTools := []string{webSearchToolName, codeExecToolName, fileReadToolName, webFetchToolName, fileWriteToolName}
	for _, name := range expectedTools {
		if _, ok := DefaultRegistry.Get(name); !ok {
			t.Errorf("expected built-in tool %q to be registered", name)
		}
	}
}

func TestRegisterCoordinationTools(t *testing.T) {
	// Create a fresh registry to test RegisterCoordinationTools
	origRegistry := DefaultRegistry
	DefaultRegistry = NewRegistry()
	defer func() { DefaultRegistry = origRegistry }()

	k8sClient := newFakeClient()
	RegisterCoordinationTools(k8sClient, executionmode.HarnessV2)

	expectedTools := []string{
		delegateTaskToolName,
		waitForTasksToolName,
		createContainerTaskToolName,
		cancelTaskToolName,
		sendMessageToolName,
		checkMessagesToolName,
		createPullRequestToolName,
		checkPullRequestCIToolName,
		mergePullRequestToolName,
		autoMergePullRequestToolName,
		reviewPullRequestToolName,
		postReviewCommentToolName,
		createAgentToolName,
		deleteAgentToolName,
		updatePlanToolName,
		"recall_memory",
		"remember",
		"propose_memory",
		"search_transcript",
	}
	for _, name := range expectedTools {
		if _, ok := DefaultRegistry.Get(name); !ok {
			t.Errorf("expected coordination tool %q to be registered", name)
		}
	}
}

func TestRegisterBuiltinTools(t *testing.T) {
	origRegistry := DefaultRegistry
	DefaultRegistry = NewRegistry()
	defer func() { DefaultRegistry = origRegistry }()

	RegisterBuiltinTools()

	expectedTools := []string{webSearchToolName, codeExecToolName, fileReadToolName}
	for _, name := range expectedTools {
		if _, ok := DefaultRegistry.Get(name); !ok {
			t.Errorf("expected built-in tool %q to be registered", name)
		}
	}
}
