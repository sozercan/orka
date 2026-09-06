/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/controller"
	"github.com/orka-agents/orka/internal/executionmode"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/llm"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
)

const (
	testAgentCreatedMsg  = "Agent created and task started"
	testLimitReached     = "limit_reached"
	testDefaultNamespace = "default"
)

// --- test helpers ---

// checkTaskLimit is a test helper for checking task limit logic.
func (e *ToolExecutor) checkTaskLimit() *ToolResult {
	if e.tasksCreated >= e.maxTasks {
		r := toolError(testLimitReached, fmt.Sprintf("task creation limit reached (max %d per turn)", e.maxTasks), "Wait for existing tasks to complete before creating new ones")
		return &r
	}
	return nil
}

// checkNamespaceScope is a test helper for checking namespace scope logic.
func (e *ToolExecutor) checkNamespaceScope(namespace string) *ToolResult {
	if e.watchNamespace != "" && namespace != e.watchNamespace {
		r := toolError("permission_denied", fmt.Sprintf("cannot create resources in namespace %q, restricted to %q", namespace, e.watchNamespace), "Use the allowed namespace")
		return &r
	}
	if e.enforceNamespaceIsolation && namespace != e.namespace {
		r := toolError("permission_denied", fmt.Sprintf("cannot create resources in namespace %q, restricted to %q", namespace, e.namespace), "Use your namespace")
		return &r
	}
	return nil
}

// executeTool is a test helper that executes a tool by name with map args.
func (e *ToolExecutor) executeTool(ctx context.Context, name string, args map[string]any) ToolResult {
	tc := &tools.ToolContext{
		Client:                    e.client,
		Namespace:                 e.namespace,
		ExecutionMode:             e.executionMode,
		WatchNamespace:            e.watchNamespace,
		EnforceNamespaceIsolation: e.enforceNamespaceIsolation,
		ResultStore:               e.resultStore,
		SessionDeleter:            e.sessionManager,
		GenerateTaskName:          e.generateTaskName,
		TaskLabels:                e.taskLabels,
		CheckTaskLimit: func() *tools.ChatToolError {
			if e.tasksCreated >= e.maxTasks {
				return &tools.ChatToolError{
					Type:       testLimitReached,
					Message:    fmt.Sprintf("task creation limit reached (max %d per turn)", e.maxTasks),
					Suggestion: "Wait for existing tasks to complete before creating new ones",
				}
			}
			return nil
		},
		IncrementTasks: func() { e.tasksCreated++ },
	}
	ctx = tools.WithToolContext(ctx, tc)

	argsJSON, err := json.Marshal(args)
	if err != nil {
		return toolError("internal_error", fmt.Sprintf("failed to marshal arguments: %v", err), "")
	}

	resultStr, err := e.registry.Execute(ctx, name, argsJSON)
	if err != nil {
		return toolError("unknown_tool", err.Error(), "Use one of the available tools")
	}

	var tr ToolResult
	if jsonErr := json.Unmarshal([]byte(resultStr), &tr); jsonErr == nil {
		return tr
	}
	return ToolResult{Success: true, Data: resultStr}
}

// classifyK8sError classifies Kubernetes API errors for test assertions.
func classifyK8sError(err error) ToolResult {
	if apierrors.IsNotFound(err) {
		return toolError("not_found", err.Error(), "Check the resource name and namespace")
	}
	if apierrors.IsAlreadyExists(err) {
		return toolError("already_exists", err.Error(), "Use a different name or delete the existing resource first")
	}
	if apierrors.IsForbidden(err) {
		return toolError("permission_denied", err.Error(), "Check RBAC permissions")
	}
	return toolError("internal_error", err.Error(), "")
}

func getStringArg(args map[string]any, key string) string {
	v, ok := args[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	return s
}

func getStringArgDefault(args map[string]any, key, defaultVal string) string {
	v := getStringArg(args, key)
	if v == "" {
		return defaultVal
	}
	return v
}

func getIntArg(args map[string]any, key string, defaultVal int) int {
	v, ok := args[key]
	if !ok {
		return defaultVal
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	default:
		return defaultVal
	}
}

func getStringSliceArg(args map[string]any, key string) []string {
	v, ok := args[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			result = append(result, s)
		} else {
			result = append(result, fmt.Sprintf("%v", item))
		}
	}
	return result
}

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

func newTestExecutor(objs ...runtime.Object) *ToolExecutor {
	scheme := testScheme()
	cb := fake.NewClientBuilder().WithScheme(scheme)
	for _, o := range objs {
		cb = cb.WithRuntimeObjects(o)
	}
	c := cb.Build()
	sm := controller.NewSessionManager(&fakeSessionStore{})
	executor := NewToolExecutor(c, sm, testDefaultNamespace, "sess-12345678", "", false, 5, 30*time.Second, &fakeResultStore{})
	executor.SetExecutionMode(executionmode.HarnessV2)
	return executor
}

// fakeResultStore implements store.ResultStore for testing.
type fakeResultStore struct {
	data     map[string][]byte
	errOnGet error
}

func (f *fakeResultStore) SaveResult(_ context.Context, namespace, taskName string, data []byte) error {
	if f.data == nil {
		f.data = make(map[string][]byte)
	}
	f.data[namespace+"/"+taskName] = data
	return nil
}

func (f *fakeResultStore) GetResult(_ context.Context, namespace, taskName string) ([]byte, error) {
	if f.errOnGet != nil {
		return nil, f.errOnGet
	}
	if f.data == nil {
		return nil, store.ErrNotFound
	}
	d, ok := f.data[namespace+"/"+taskName]
	if !ok {
		return nil, store.ErrNotFound
	}
	return d, nil
}

func (f *fakeResultStore) DeleteResult(_ context.Context, _, _ string) error {
	return nil
}

// fakeSessionStore implements store.SessionStore for testing.
type fakeSessionStore struct {
	deleted  map[string]bool
	errOnDel error
}

func (f *fakeSessionStore) CreateSession(_ context.Context, _ *store.SessionRecord) error { return nil }
func (f *fakeSessionStore) GetSession(_ context.Context, _, _ string) (*store.SessionRecord, error) {
	return &store.SessionRecord{}, nil
}
func (f *fakeSessionStore) ListSessions(_ context.Context, _ string) ([]store.SessionMetadata, error) {
	return nil, nil
}

func (f *fakeSessionStore) ListSessionsPage(_ context.Context, _, _ string, _ int, _ string) ([]store.SessionMetadata, bool, error) {
	return nil, false, nil
}
func (f *fakeSessionStore) DeleteSession(_ context.Context, ns, name string) error {
	if f.errOnDel != nil {
		return f.errOnDel
	}
	if f.deleted == nil {
		f.deleted = make(map[string]bool)
	}
	f.deleted[ns+"/"+name] = true
	return nil
}
func (f *fakeSessionStore) AcquireLock(_ context.Context, _, _, _, _ string) error { return nil }
func (f *fakeSessionStore) ReleaseLock(_ context.Context, _, _, _, _ string) error { return nil }
func (f *fakeSessionStore) IsLocked(_ context.Context, _, _, _, _ string) (bool, error) {
	return false, nil
}
func (f *fakeSessionStore) AppendMessages(_ context.Context, _, _ string, _ []store.SessionMessage) error {
	return nil
}
func (f *fakeSessionStore) LoadTranscript(_ context.Context, _, _ string, _ int) ([]store.SessionMessage, error) {
	return nil, nil
}
func (f *fakeSessionStore) LoadTranscriptThrough(_ context.Context, _, _, _ string, _ int) ([]store.SessionMessage, error) {
	return nil, nil
}
func (f *fakeSessionStore) SearchTranscript(_ context.Context, _ store.TranscriptSearchFilter) ([]store.TranscriptSearchResult, error) {
	return nil, nil
}
func (f *fakeSessionStore) UpdateTokenCounts(_ context.Context, _, _ string, _, _ int) error {
	return nil
}

func mustJSON(v map[string]any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// --- Tests: helper functions ---

func TestGetStringArg(t *testing.T) {
	tests := []struct {
		name string
		args map[string]any
		key  string
		want string
	}{
		{"present string", map[string]any{"k": "val"}, "k", "val"},
		{"missing key", map[string]any{}, "k", ""},
		{"non-string value", map[string]any{"k": 42}, "k", "42"},
		{"bool value", map[string]any{"k": true}, "k", "true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getStringArg(tt.args, tt.key)
			if got != tt.want {
				t.Errorf("getStringArg() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetStringArgDefault(t *testing.T) {
	tests := []struct {
		name       string
		args       map[string]any
		key        string
		defaultVal string
		want       string
	}{
		{"present", map[string]any{"k": "val"}, "k", "def", "val"},
		{"missing uses default", map[string]any{}, "k", "def", "def"},
		{"empty string uses default", map[string]any{"k": ""}, "k", "def", "def"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getStringArgDefault(tt.args, tt.key, tt.defaultVal)
			if got != tt.want {
				t.Errorf("getStringArgDefault() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetIntArg(t *testing.T) {
	tests := []struct {
		name       string
		args       map[string]any
		key        string
		defaultVal int
		want       int
	}{
		{"float64 value", map[string]any{"k": float64(42)}, "k", 0, 42},
		{"int value", map[string]any{"k": int(7)}, "k", 0, 7},
		{"int64 value", map[string]any{"k": int64(99)}, "k", 0, 99},
		{"missing uses default", map[string]any{}, "k", 10, 10},
		{"string falls back to default", map[string]any{"k": "abc"}, "k", 5, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getIntArg(tt.args, tt.key, tt.defaultVal)
			if got != tt.want {
				t.Errorf("getIntArg() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestGetStringSliceArg(t *testing.T) {
	tests := []struct {
		name string
		args map[string]any
		key  string
		want []string
	}{
		{"string slice", map[string]any{"k": []any{"a", "b"}}, "k", []string{"a", "b"}},
		{"missing key", map[string]any{}, "k", nil},
		{"non-slice value", map[string]any{"k": "scalar"}, "k", nil},
		{"mixed types", map[string]any{"k": []any{"a", 42}}, "k", []string{"a", "42"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getStringSliceArg(tt.args, tt.key)
			if tt.want == nil {
				if got != nil {
					t.Errorf("getStringSliceArg() = %v, want nil", got)
				}
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("getStringSliceArg() len = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("getStringSliceArg()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestToolError(t *testing.T) {
	r := toolError("my_type", "something failed", "try again")
	if r.Success {
		t.Fatal("expected Success=false")
	}
	if r.ErrorType != "my_type" {
		t.Errorf("ErrorType = %q, want %q", r.ErrorType, "my_type")
	}
	if r.Error != "something failed" {
		t.Errorf("Error = %q, want %q", r.Error, "something failed")
	}
	if r.Suggestion != "try again" {
		t.Errorf("Suggestion = %q, want %q", r.Suggestion, "try again")
	}
}

func TestClassifyK8sError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantType string
	}{
		{
			"not found",
			apierrors.NewNotFound(schema.GroupResource{Group: "", Resource: "tasks"}, "my-task"),
			"not_found",
		},
		{
			"already exists",
			apierrors.NewAlreadyExists(schema.GroupResource{Group: "", Resource: "tasks"}, "my-task"),
			"already_exists",
		},
		{
			"forbidden",
			apierrors.NewForbidden(schema.GroupResource{Group: "", Resource: "tasks"}, "my-task", fmt.Errorf("denied")),
			"permission_denied",
		},
		{
			"generic error",
			fmt.Errorf("some random error"),
			"internal_error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := classifyK8sError(tt.err)
			if r.Success {
				t.Fatal("expected Success=false")
			}
			if r.ErrorType != tt.wantType {
				t.Errorf("ErrorType = %q, want %q", r.ErrorType, tt.wantType)
			}
		})
	}
}

func TestMarshalResult(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		r := ToolResult{Success: true, Data: "hello"}
		s, err := marshalResult(r)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(s, `"success":true`) {
			t.Errorf("marshalResult() = %s, expected success:true", s)
		}
	})
	t.Run("error result", func(t *testing.T) {
		r := toolError("test", "msg", "sug")
		s, err := marshalResult(r)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(s, `"success":false`) {
			t.Errorf("marshalResult() = %s, expected success:false", s)
		}
	})
}

// --- Tests: ToolExecutor construction & basic methods ---

func TestNewToolExecutor(t *testing.T) {
	e := newTestExecutor()
	if e == nil {
		t.Fatal("expected non-nil executor")
	}
	if e.namespace != testDefaultNamespace {
		t.Errorf("namespace = %q, want %q", e.namespace, testDefaultNamespace)
	}
	if e.sessionID != "sess-12345678" {
		t.Errorf("sessionID = %q, want %q", e.sessionID, "sess-12345678")
	}
	if e.maxTasks != 5 {
		t.Errorf("maxTasks = %d, want %d", e.maxTasks, 5)
	}
}

func TestGenerateTaskName(t *testing.T) {
	e := newTestExecutor()
	name1 := e.generateTaskName()
	name2 := e.generateTaskName()

	if !strings.HasPrefix(name1, "chat-sess-12345678-") {
		t.Errorf("name1 = %q, expected prefix chat-sess-12345678-", name1)
	}
	if name1 == name2 {
		t.Errorf("expected unique names, got %q twice", name1)
	}
}

func TestGenerateTaskName_AvoidsCrossSessionCollisions(t *testing.T) {
	e1 := newTestExecutor()
	e1.sessionID = "demo-magic-chat-pr-one"
	e2 := newTestExecutor()
	e2.sessionID = "demo-magic-chat-pr-two"

	name1 := e1.generateTaskName()
	name2 := e2.generateTaskName()

	if name1 == name2 {
		t.Fatalf("expected distinct names for different sessions, got %q", name1)
	}
	if !strings.HasPrefix(name1, "chat-demo-magic-chat-pr-") {
		t.Fatalf("name1 = %q, expected sanitized session prefix", name1)
	}
	if !strings.HasPrefix(name2, "chat-demo-magic-chat-pr-") {
		t.Fatalf("name2 = %q, expected sanitized session prefix", name2)
	}
}

func TestGenerateTaskNameTruncatesLongSessionToDNSLabelLimit(t *testing.T) {
	e := newTestExecutor()
	e.sessionID = strings.Repeat("a", 80)

	name := e.generateTaskName()

	if len(name) > 63 {
		t.Fatalf("len(name) = %d, want <= 63: %q", len(name), name)
	}
	if !strings.HasPrefix(name, "chat-") {
		t.Fatalf("name = %q, expected chat prefix", name)
	}
	if strings.Contains(name, "--") {
		t.Fatalf("name = %q, expected no empty DNS label components", name)
	}
}

func TestSanitizeTaskNameComponent(t *testing.T) {
	tests := map[string]string{
		"Demo Session/ABC": "demo-session-abc",
		"---already---":    "already",
		"MiXeD___123":      "mixed-123",
	}

	for input, want := range tests {
		if got := sanitizeTaskNameComponent(input); got != want {
			t.Fatalf("sanitizeTaskNameComponent(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestTaskLabels(t *testing.T) {
	e := newTestExecutor()
	taskLabels := e.taskLabels()
	if taskLabels[labels.LabelCreatedBy] != "orchestrator" {
		t.Errorf("created-by label = %q", taskLabels[labels.LabelCreatedBy])
	}
	if taskLabels[labels.LabelChatSession] != "sess-12345678" {
		t.Errorf("chat-session label = %q", taskLabels[labels.LabelChatSession])
	}
}

func TestCheckTaskLimit(t *testing.T) {
	e := newTestExecutor()
	e.maxTasks = 2
	e.tasksCreated = 0

	if r := e.checkTaskLimit(); r != nil {
		t.Fatalf("expected nil when under limit, got %+v", r)
	}

	e.tasksCreated = 2
	r := e.checkTaskLimit()
	if r == nil {
		t.Fatal("expected non-nil when at limit")
	}
	if r.ErrorType != testLimitReached {
		t.Errorf("ErrorType = %q, want limit_reached", r.ErrorType)
	}
}

func TestCheckNamespaceScope(t *testing.T) {
	tests := []struct {
		name      string
		watchNS   string
		enforceNS bool
		execNS    string
		targetNS  string
		expectErr bool
	}{
		{"allowed - same namespace", "", false, "default", "default", false},
		{"allowed - no restrictions", "", false, "default", "other", false},
		{"watchNamespace mismatch", "prod", false, "default", "dev", true},
		{"watchNamespace match", "prod", false, "default", "prod", false},
		{"enforce isolation mismatch", "", true, "default", "other", true},
		{"enforce isolation match", "", true, "default", "default", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestExecutor()
			e.watchNamespace = tt.watchNS
			e.enforceNamespaceIsolation = tt.enforceNS
			e.namespace = tt.execNS
			r := e.checkNamespaceScope(tt.targetNS)
			if tt.expectErr && r == nil {
				t.Fatal("expected error result")
			}
			if !tt.expectErr && r != nil {
				t.Fatalf("expected nil, got %+v", r)
			}
		})
	}
}

// --- Tests: Execute dispatch ---

func TestExecute_InvalidJSON(t *testing.T) {
	e := newTestExecutor()
	tc := llm.ToolCall{
		ID:        "1",
		Name:      "create_ai_task",
		Arguments: json.RawMessage(`{invalid`),
	}
	result, err := e.Execute(context.Background(), tc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "invalid_arguments") {
		t.Errorf("expected invalid_arguments error, got: %s", result)
	}
}

func TestExecute_UnknownTool(t *testing.T) {
	e := newTestExecutor()
	tc := llm.ToolCall{
		ID:        "1",
		Name:      "nonexistent_tool",
		Arguments: mustJSON(map[string]any{}),
	}
	result, err := e.Execute(context.Background(), tc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "unknown_tool") {
		t.Errorf("expected unknown_tool error, got: %s", result)
	}
}

func TestExecute_AllowedToolsRejectsDisallowedToolBeforeDispatch(t *testing.T) {
	e := newTestExecutor()
	e.SetAllowedTools([]llm.Tool{{Name: "check_task_progress"}})

	disallowed := llm.ToolCall{
		ID:        "1",
		Name:      "create_ai_task",
		Arguments: json.RawMessage(`{invalid`),
	}
	result, err := e.Execute(context.Background(), disallowed)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "unauthorized_tool") || !strings.Contains(result, "not authorized") {
		t.Fatalf("expected unauthorized_tool error, got: %s", result)
	}
	if strings.Contains(result, "invalid_arguments") {
		t.Fatalf("expected allowlist rejection before argument parsing, got: %s", result)
	}

	allowed := llm.ToolCall{
		ID:        "2",
		Name:      "check_task_progress",
		Arguments: mustJSON(map[string]any{}),
	}
	result, err = e.Execute(context.Background(), allowed)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "name is required") {
		t.Fatalf("expected allowed tool to dispatch to validation, got: %s", result)
	}
}

func TestExecute_CreateAgentUsesAgentCreateAuthorizer(t *testing.T) {
	e := newTestExecutor()
	e.SetAgentCreateAuthorizer(func(context.Context, *corev1alpha1.Agent) error {
		return errors.New("agent denied")
	})

	result, err := e.Execute(context.Background(), llm.ToolCall{
		ID:        "1",
		Name:      "create_agent",
		Arguments: mustJSON(map[string]any{"name": "blocked-agent"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "authorization_failed") {
		t.Fatalf("expected authorization_failed, got: %s", result)
	}

	var agent corev1alpha1.Agent
	err = e.client.Get(context.Background(), apitypes.NamespacedName{Name: "blocked-agent", Namespace: "default"}, &agent)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("agent should not have been created, get err=%v", err)
	}
}

func TestExecute_CancelTaskUsesTaskDeleteAuthorizer(t *testing.T) {
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "blocked-task", Namespace: "default"}}
	e := newTestExecutor(task)
	e.SetTaskDeleteAuthorizer(func(context.Context, *corev1alpha1.Task) error {
		return errors.New("task delete denied")
	})

	result, err := e.Execute(context.Background(), llm.ToolCall{
		ID:        "1",
		Name:      "cancel_task",
		Arguments: mustJSON(map[string]any{"name": "blocked-task"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "authorization_failed") {
		t.Fatalf("expected authorization_failed, got: %s", result)
	}

	var remaining corev1alpha1.Task
	if err := e.client.Get(context.Background(), apitypes.NamespacedName{Name: "blocked-task", Namespace: "default"}, &remaining); err != nil {
		t.Fatalf("task should not have been deleted: %v", err)
	}
}

func TestExecute_UpdateAgentUsesAgentUpdateAuthorizer(t *testing.T) {
	agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "blocked-agent", Namespace: "default"}}
	e := newTestExecutor(agent)
	e.SetAgentUpdateAuthorizer(func(context.Context, *corev1alpha1.Agent) error {
		return errors.New("agent update denied")
	})

	result, err := e.Execute(context.Background(), llm.ToolCall{
		ID:        "1",
		Name:      "update_agent",
		Arguments: mustJSON(map[string]any{"name": "blocked-agent", "systemPrompt": "blocked"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "authorization_failed") {
		t.Fatalf("expected authorization_failed, got: %s", result)
	}

	var remaining corev1alpha1.Agent
	if err := e.client.Get(context.Background(), apitypes.NamespacedName{Name: "blocked-agent", Namespace: "default"}, &remaining); err != nil {
		t.Fatalf("agent should still exist: %v", err)
	}
	if remaining.Spec.SystemPrompt != nil {
		t.Fatalf("agent update should not have been persisted: %#v", remaining.Spec.SystemPrompt)
	}
}

func TestExecute_DeleteAgentUsesAgentDeleteAuthorizer(t *testing.T) {
	agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "blocked-agent", Namespace: "default"}}
	e := newTestExecutor(agent)
	e.SetAgentDeleteAuthorizer(func(context.Context, *corev1alpha1.Agent) error {
		return errors.New("agent delete denied")
	})

	result, err := e.Execute(context.Background(), llm.ToolCall{
		ID:        "1",
		Name:      "delete_agent",
		Arguments: mustJSON(map[string]any{"name": "blocked-agent"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "authorization_failed") {
		t.Fatalf("expected authorization_failed, got: %s", result)
	}

	var remaining corev1alpha1.Agent
	if err := e.client.Get(context.Background(), apitypes.NamespacedName{Name: "blocked-agent", Namespace: "default"}, &remaining); err != nil {
		t.Fatalf("agent should not have been deleted: %v", err)
	}
}

func TestExecute_Dispatch(t *testing.T) {
	tests := []struct {
		name     string
		toolName string
		args     map[string]any
		contains string
	}{
		{"create_ai_task missing prompt", "create_ai_task", map[string]any{}, "prompt is required"},
		{"create_agent_task missing prompt", "create_agent_task", map[string]any{}, "prompt is required"},
		{"create_agent_task missing agentRef", "create_agent_task", map[string]any{"prompt": "test"}, "agentRef is required"},
		{"check_task_progress missing name", "check_task_progress", map[string]any{}, "name is required"},
		{"fetch_task_output missing name", "fetch_task_output", map[string]any{}, "name is required"},
		{"wait_for_task missing name", "wait_for_task", map[string]any{}, "name is required"},
		{"cancel_task missing name", "cancel_task", map[string]any{}, "name is required"},
		{"create_agent missing name", "create_agent", map[string]any{}, "name is required"},
		{"update_agent missing name", "update_agent", map[string]any{}, "name is required"},
		{"delete_agent missing name", "delete_agent", map[string]any{}, "name is required"},
		{"create_tool missing name", "create_tool", map[string]any{}, "name is required"},
		{"create_tool missing desc", "create_tool", map[string]any{"name": "t1"}, "description is required"},
		{"create_tool missing url", "create_tool", map[string]any{"name": "t1", "description": "d"}, "url is required"},
		{"delete_tool missing name", "delete_tool", map[string]any{}, "name is required"},
		{"delete_session missing id", "delete_session", map[string]any{}, "sessionId is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestExecutor()
			tc := llm.ToolCall{
				ID:        "1",
				Name:      tt.toolName,
				Arguments: mustJSON(tt.args),
			}
			result, err := e.Execute(context.Background(), tc)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(result, tt.contains) {
				t.Errorf("expected %q in result, got: %s", tt.contains, result)
			}
		})
	}
}

// --- Tests: executeCreateAITask ---

func TestExecuteCreateAITask_Success(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"prompt": "summarize this",
	}
	r := e.executeTool(context.Background(), "create_ai_task", args)
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if data["phase"] != "Pending" {
		t.Errorf("phase = %v, want Pending", data["phase"])
	}
	if data["message"] != taskCreatedMsg {
		t.Errorf("message = %v", data["message"])
	}
}

func TestExecuteCreateAITask_WithSchedule(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"prompt":   "do something",
		"schedule": "*/5 * * * *",
	}
	r := e.executeTool(context.Background(), "create_ai_task", args)
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if !strings.Contains(data["message"].(string), "Recurring task scheduled") {
		t.Errorf("message = %v, expected scheduled message", data["message"])
	}
}

func TestExecuteCreateAITask_InvalidTimeout(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"prompt":  "test",
		"timeout": "not-a-duration",
	}
	r := e.executeTool(context.Background(), "create_ai_task", args)
	if r.Success {
		t.Fatal("expected failure for invalid timeout")
	}
	if r.ErrorType != "invalid_arguments" {
		t.Errorf("ErrorType = %q", r.ErrorType)
	}
}

func TestExecuteCreateAITask_TaskLimit(t *testing.T) {
	e := newTestExecutor()
	e.maxTasks = 0
	r := e.executeTool(context.Background(), "create_ai_task", map[string]any{"prompt": "test"})
	if r.Success {
		t.Fatal("expected failure due to task limit")
	}
	if r.ErrorType != testLimitReached {
		t.Errorf("ErrorType = %q", r.ErrorType)
	}
}

func TestExecuteCreateAITask_WithOptionalFields(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"prompt":      "test",
		"agentRef":    "my-agent",
		"providerRef": "my-provider",
		"timeout":     "5m",
		"priority":    float64(10),
		"sessionRef":  "my-session",
	}
	r := e.executeTool(context.Background(), "create_ai_task", args)
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
}

// --- Tests: executeCreateContainerTask ---

func TestExecuteCreateContainerTask_Success(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"image":   "alpine:latest",
		"command": []any{"echo", "hello"},
		"args":    []any{"world"},
	}
	r := e.executeTool(context.Background(), "create_container_task", args)
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if data["phase"] != "Pending" {
		t.Errorf("phase = %v", data["phase"])
	}
}

func TestExecuteCreateContainerTask_TaskLimit(t *testing.T) {
	e := newTestExecutor()
	e.maxTasks = 0
	r := e.executeTool(context.Background(), "create_container_task", map[string]any{"image": "alpine"})
	if r.Success {
		t.Fatal("expected failure")
	}
}

func TestExecuteCreateContainerTask_WithSchedule(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"image":    "alpine",
		"schedule": "0 * * * *",
	}
	r := e.executeTool(context.Background(), "create_container_task", args)
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if !strings.Contains(data["message"].(string), "Recurring") {
		t.Errorf("expected recurring message, got %v", data["message"])
	}
}

func TestExecuteCreateContainerTask_InvalidTimeout(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"image":   "alpine",
		"timeout": "bad",
	}
	r := e.executeTool(context.Background(), "create_container_task", args)
	if r.Success {
		t.Fatal("expected failure for invalid timeout")
	}
}

// --- Tests: executeCreateAgentTask ---

func TestExecuteCreateAgentTask_Success(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"prompt":   "do work",
		"agentRef": "my-agent",
	}
	r := e.executeTool(context.Background(), "create_agent_task", args)
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
}

func TestExecuteCreateAgentTask_MissingPrompt(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "create_agent_task", map[string]any{"agentRef": "a"})
	if r.Success {
		t.Fatal("expected failure")
	}
}

func TestExecuteCreateAgentTask_MissingAgentRef(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "create_agent_task", map[string]any{"prompt": "test"})
	if r.Success {
		t.Fatal("expected failure")
	}
}

func TestExecuteCreateAgentTask_WithWorkspace(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "github-credentials",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"token": []byte("ghp_test"),
		},
	}
	e := newTestExecutor(secret)
	args := map[string]any{
		"prompt":   "work on repo",
		"agentRef": "my-agent",
		"workspace": map[string]any{
			"gitRepo":                  "https://github.com/org/repo",
			"branch":                   "main",
			"subPath":                  "src",
			"pushBranch":               "feature-1",
			"publicationCredentialRef": "github-publication-credentials",
		},
		"maxTurns": float64(10),
	}
	r := e.executeTool(context.Background(), "create_agent_task", args)
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	task := &corev1alpha1.Task{}
	if err := e.client.Get(context.Background(), apitypes.NamespacedName{Name: data["name"].(string), Namespace: "default"}, task); err != nil {
		t.Fatalf("failed to get created task: %v", err)
	}
	if task.Spec.AgentRuntime == nil || task.Spec.Workspace == nil {
		t.Fatal("expected workspace to be set")
	}
	if task.Spec.Workspace.ReadCredentialRef == nil {
		t.Fatal("expected readCredentialRef to be auto-discovered")
	}
	if task.Spec.Workspace.ReadCredentialRef.Name != "github-credentials" {
		t.Fatalf("readCredentialRef = %q, want %q", task.Spec.Workspace.ReadCredentialRef.Name, "github-credentials")
	}
}

func TestExecuteCreateAgentTask_WithExplicitGitSecret(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"prompt":   "work on repo",
		"agentRef": "my-agent",
		"workspace": map[string]any{
			"gitRepo":           "https://github.com/org/repo",
			"readCredentialRef": "my-secret",
		},
	}
	r := e.executeTool(context.Background(), "create_agent_task", args)
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	task := &corev1alpha1.Task{}
	if err := e.client.Get(context.Background(), apitypes.NamespacedName{Name: data["name"].(string), Namespace: "default"}, task); err != nil {
		t.Fatalf("failed to get created task: %v", err)
	}
	if task.Spec.Workspace == nil || task.Spec.Workspace.ReadCredentialRef == nil {
		t.Fatal("expected explicit readCredentialRef to be preserved")
	}
	if task.Spec.Workspace.ReadCredentialRef.Name != "my-secret" {
		t.Errorf("readCredentialRef = %q, want %q", task.Spec.Workspace.ReadCredentialRef.Name, "my-secret")
	}
}

// --- Tests: executeCheckTaskProgress ---

func TestExecuteCheckTaskProgress_Success(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-task",
			Namespace: "default",
		},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			Message: "running",
		},
	}
	e := newTestExecutor(task)
	r := e.executeTool(context.Background(), "check_task_progress", map[string]any{"name": "my-task"})
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if data["name"] != "my-task" {
		t.Errorf("name = %v", data["name"])
	}
}

func TestExecuteCheckTaskProgress_NotFound(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "check_task_progress", map[string]any{"name": "nonexistent"})
	if r.Success {
		t.Fatal("expected failure")
	}
	if r.ErrorType != "not_found" {
		t.Errorf("ErrorType = %q", r.ErrorType)
	}
}

func TestExecuteCheckTaskProgress_WithConditions(t *testing.T) {
	now := metav1.Now()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cond-task",
			Namespace: "default",
		},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			StartTime: &now,
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: "True", Reason: "Scheduled", Message: "ok"},
			},
		},
	}
	e := newTestExecutor(task)
	r := e.executeTool(context.Background(), "check_task_progress", map[string]any{"name": "cond-task"})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if _, ok := data["duration"]; !ok {
		t.Error("expected duration field")
	}
	if _, ok := data["conditions"]; !ok {
		t.Error("expected conditions field")
	}
}

// --- Tests: executeFetchTaskOutput ---

func TestExecuteFetchTaskOutput_NoResult(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "no-result",
			Namespace: "default",
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseSucceeded,
		},
	}
	e := newTestExecutor(task)
	r := e.executeTool(context.Background(), "fetch_task_output", map[string]any{"name": "no-result"})
	if r.Success {
		t.Fatal("expected failure - no result available")
	}
}

func TestExecuteFetchTaskOutput_Success(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "done-task",
			Namespace: "default",
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseSucceeded,
			ResultRef: &corev1alpha1.ResultReference{
				Available: true,
			},
		},
	}
	rs := &fakeResultStore{
		data: map[string][]byte{
			"default/done-task": []byte("result output here"),
		},
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(task).Build()
	sm := controller.NewSessionManager(&fakeSessionStore{})
	e := NewToolExecutor(c, sm, "default", "sess-123", "", false, 5, 30*time.Second, rs)

	r := e.executeTool(context.Background(), "fetch_task_output", map[string]any{"name": "done-task"})
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if data["output"] != "result output here" {
		t.Errorf("output = %v", data["output"])
	}
}

func TestExecuteFetchTaskOutput_StoreNotFound(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "store-miss",
			Namespace: "default",
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseSucceeded,
			ResultRef: &corev1alpha1.ResultReference{
				Available: true,
			},
		},
	}
	rs := &fakeResultStore{} // empty store
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(task).Build()
	sm := controller.NewSessionManager(&fakeSessionStore{})
	e := NewToolExecutor(c, sm, "default", "sess-123", "", false, 5, 30*time.Second, rs)

	r := e.executeTool(context.Background(), "fetch_task_output", map[string]any{"name": "store-miss"})
	if r.Success {
		t.Fatal("expected failure")
	}
	if r.ErrorType != "not_found" {
		t.Errorf("ErrorType = %q, want not_found", r.ErrorType)
	}
}

func TestExecuteFetchTaskOutput_StoreError(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "store-err",
			Namespace: "default",
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseSucceeded,
			ResultRef: &corev1alpha1.ResultReference{
				Available: true,
			},
		},
	}
	rs := &fakeResultStore{errOnGet: errors.New("db error")}
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(task).Build()
	sm := controller.NewSessionManager(&fakeSessionStore{})
	e := NewToolExecutor(c, sm, "default", "sess-123", "", false, 5, 30*time.Second, rs)

	r := e.executeTool(context.Background(), "fetch_task_output", map[string]any{"name": "store-err"})
	if r.Success {
		t.Fatal("expected failure")
	}
	if r.ErrorType != "internal_error" {
		t.Errorf("ErrorType = %q, want internal_error", r.ErrorType)
	}
}

// --- Tests: executeWaitForTask ---

func TestExecuteWaitForTask_AlreadyComplete(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "done",
			Namespace: "default",
		},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseSucceeded,
			Message: "done",
		},
	}
	e := newTestExecutor(task)
	r := e.executeTool(context.Background(), "wait_for_task", map[string]any{"name": "done", "timeout": float64(1)})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteWaitForTask_NotFound(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "wait_for_task", map[string]any{"name": "missing"})
	if r.Success {
		t.Fatal("expected failure")
	}
}

// --- Tests: executeCancelTask ---

func TestExecuteCancelTask_Success(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cancel-me",
			Namespace: "default",
		},
	}
	e := newTestExecutor(task)
	r := e.executeTool(context.Background(), "cancel_task", map[string]any{"name": "cancel-me"})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if !strings.Contains(data["message"].(string), "cancelled") {
		t.Errorf("message = %v", data["message"])
	}
}

func TestExecuteCancelTask_NotFound(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "cancel_task", map[string]any{"name": "nope"})
	if r.Success {
		t.Fatal("expected failure")
	}
}

// --- Tests: executeListAgents ---

func TestExecuteListAgents_Empty(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "list_agents", map[string]any{})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteListAgents_WithAgents(t *testing.T) {
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-agent",
			Namespace: "default",
			Annotations: map[string]string{
				"description": "A test agent",
			},
		},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "openai",
				Name:     "gpt-4",
			},
			Tools: []corev1alpha1.ToolReference{
				{Name: "tool1"},
			},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type: "copilot",
			},
		},
	}
	e := newTestExecutor(agent)
	r := e.executeTool(context.Background(), "list_agents", map[string]any{})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	agentsRaw := r.Data.([]any)
	if len(agentsRaw) != 1 {
		t.Fatalf("expected 1 agent, got %d", len(agentsRaw))
	}
	agents := agentsRaw[0].(map[string]any)
	if agents["name"] != "test-agent" {
		t.Errorf("name = %v", agents["name"])
	}
	if agents["description"] != "A test agent" {
		t.Errorf("description = %v", agents["description"])
	}
}

// --- Tests: executeListTools ---

func TestExecuteListTools_Empty(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "list_tools", map[string]any{})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteListTools_WithTools(t *testing.T) {
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-tool",
			Namespace: "default",
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "does stuff",
		},
	}
	e := newTestExecutor(tool)
	r := e.executeTool(context.Background(), "list_tools", map[string]any{})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	toolsData := r.Data.([]any)
	if len(toolsData) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(toolsData))
	}
}

// --- Tests: executeListTasks ---

func TestExecuteListTasks_Empty(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "list_tasks", map[string]any{})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteListTasks_WithFilter(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "t1",
			Namespace:         "default",
			CreationTimestamp: metav1.Now(),
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseSucceeded,
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
		},
	}
	e := newTestExecutor(task)

	// Filter matching
	r := e.executeTool(context.Background(), "list_tasks", map[string]any{"status": "Succeeded"})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	tasks := r.Data.([]any)
	if len(tasks) != 1 {
		t.Errorf("expected 1 task, got %d", len(tasks))
	}

	// Filter non-matching
	r2 := e.executeTool(context.Background(), "list_tasks", map[string]any{"status": "Failed"})
	if !r2.Success {
		t.Fatalf("expected success: %s", r2.Error)
	}
	tasks2 := r2.Data.([]any)
	if len(tasks2) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(tasks2))
	}
}

// --- Tests: executeCreateAgent ---

func TestExecuteCreateAgent_Success(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"name": "new-agent",
	}
	r := e.executeTool(context.Background(), "create_agent", args)
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if data["name"] != "new-agent" {
		t.Errorf("name = %v", data["name"])
	}
}

func TestExecuteCreateAgent_WithModel(t *testing.T) {
	tests := []struct {
		name  string
		model any
	}{
		{"map model", map[string]any{"name": "gpt-4", "provider": "openai", "temperature": float64(0.7)}},
		{"string model with provider", "openai/gpt-4"},
		{"string model without provider", "gpt-4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestExecutor()
			args := map[string]any{
				"name":  "agent-" + tt.name,
				"model": tt.model,
			}
			r := e.executeTool(context.Background(), "create_agent", args)
			if !r.Success {
				t.Fatalf("expected success: %s", r.Error)
			}
		})
	}
}

func TestExecuteCreateAgent_WithSystemPromptAndTools(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"name":         "agent-full",
		"systemPrompt": "You are helpful",
		"tools":        []any{"tool1", "tool2"},
	}
	r := e.executeTool(context.Background(), "create_agent", args)
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteCreateAgent_WithRuntime(t *testing.T) {
	e := newTestExecutor(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "copilot-token",
			Namespace: "default",
		},
	})
	args := map[string]any{
		"name":  "runtime-agent",
		"model": map[string]any{"name": "test-model"},
		"runtime": map[string]any{
			"type": "copilot",
		},
	}
	r := e.executeTool(context.Background(), "create_agent", args)
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteCreateAgent_WithCoordination(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"name": "coord-agent",
		"coordination": map[string]any{
			"enabled":               true,
			"maxConcurrentChildren": float64(3),
			"maxDepth":              float64(2),
			"allowedAgents": []any{
				map[string]any{"name": "helper", "namespace": "default"},
			},
		},
	}
	r := e.executeTool(context.Background(), "create_agent", args)
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteCreateAgent_NamespaceRestriction(t *testing.T) {
	e := newTestExecutor()
	e.enforceNamespaceIsolation = true
	args := map[string]any{
		"name":      "bad-ns",
		"namespace": "other-ns",
	}
	r := e.executeTool(context.Background(), "create_agent", args)
	if r.Success {
		t.Fatal("expected failure due to namespace restriction")
	}
	if r.ErrorType != "permission_denied" {
		t.Errorf("ErrorType = %q", r.ErrorType)
	}
}

func TestExecuteCreateAgent_WithInitialPrompt(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"name":          "init-agent",
		"initialPrompt": "do initial work",
	}
	r := e.executeTool(context.Background(), "create_agent", args)
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if data["message"] != testAgentCreatedMsg {
		t.Errorf("message = %v", data["message"])
	}
}

// --- Tests: executeUpdateAgent ---

func TestExecuteUpdateAgent_Success(t *testing.T) {
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "upd-agent",
			Namespace: "default",
		},
		Spec: corev1alpha1.AgentSpec{},
	}
	e := newTestExecutor(agent)
	args := map[string]any{
		"name":         "upd-agent",
		"model":        "openai/gpt-4o",
		"systemPrompt": "Be concise",
		"tools":        []any{"search"},
	}
	r := e.executeTool(context.Background(), "update_agent", args)
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteUpdateAgent_NotFound(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "update_agent", map[string]any{"name": "missing"})
	if r.Success {
		t.Fatal("expected failure")
	}
}

// --- Tests: executeDeleteAgent ---

func TestExecuteDeleteAgent_Success(t *testing.T) {
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "del-agent",
			Namespace: "default",
		},
	}
	e := newTestExecutor(agent)
	r := e.executeTool(context.Background(), "delete_agent", map[string]any{"name": "del-agent"})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteDeleteAgent_NotFound(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "delete_agent", map[string]any{"name": "missing"})
	if r.Success {
		t.Fatal("expected failure")
	}
}

// --- Tests: executeCreateTool ---

func TestExecuteCreateTool_Success(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"name":        "my-tool",
		"description": "does stuff",
		"url":         "https://example.com/api",
		"method":      "GET",
	}
	r := e.executeTool(context.Background(), "create_tool", args)
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteCreateTool_DefaultMethod(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"name":        "my-tool2",
		"description": "does more stuff",
		"url":         "https://example.com/api",
	}
	r := e.executeTool(context.Background(), "create_tool", args)
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteCreateTool_NamespaceRestriction(t *testing.T) {
	e := newTestExecutor()
	e.enforceNamespaceIsolation = true
	args := map[string]any{
		"name":        "t",
		"description": "d",
		"url":         "http://x",
		"namespace":   "other",
	}
	r := e.executeTool(context.Background(), "create_tool", args)
	if r.Success {
		t.Fatal("expected failure")
	}
}

// --- Tests: executeDeleteTool ---

func TestExecuteDeleteTool_Success(t *testing.T) {
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "del-tool",
			Namespace: "default",
		},
	}
	e := newTestExecutor(tool)
	r := e.executeTool(context.Background(), "delete_tool", map[string]any{"name": "del-tool"})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteDeleteTool_NotFound(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "delete_tool", map[string]any{"name": "missing"})
	if r.Success {
		t.Fatal("expected failure")
	}
}

// --- Tests: executeDeleteSession ---

func TestExecuteDeleteSession_Success(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "delete_session", map[string]any{"sessionId": "sess-abc"})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if data["sessionId"] != "sess-abc" {
		t.Errorf("sessionId = %v", data["sessionId"])
	}
}

// --- Tests: taskStatusResult & taskTimeoutResult ---

func TestTaskStatusResult_WithTimes(t *testing.T) {
	now := metav1.Now()
	later := metav1.NewTime(now.Add(5 * time.Second))
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Status: corev1alpha1.TaskStatus{
			Phase:          corev1alpha1.TaskPhaseSucceeded,
			Message:        "completed",
			StartTime:      &now,
			CompletionTime: &later,
		},
	}
	e := newTestExecutor(task)
	r := e.executeTool(context.Background(), "wait_for_task", map[string]any{"name": "t1", "timeout": float64(1)})
	if !r.Success {
		t.Fatalf("expected success")
	}
	data := r.Data.(map[string]any)
	if data["elapsed"] != "5s" {
		t.Errorf("elapsed = %v, want 5s", data["elapsed"])
	}
}

func TestTaskStatusResult_NoTimes(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t2", Namespace: "default"},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseFailed,
			Message: "failed",
		},
	}
	e := newTestExecutor(task)
	r := e.executeTool(context.Background(), "wait_for_task", map[string]any{"name": "t2", "timeout": float64(1)})
	if !r.Success {
		t.Fatalf("expected success")
	}
	data := r.Data.(map[string]any)
	if _, ok := data["elapsed"]; ok {
		t.Error("expected no elapsed field when no start time")
	}
}

func TestTaskStatusResult_StartTimeOnly(t *testing.T) {
	now := metav1.Now()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t3", Namespace: "default"},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseSucceeded,
			StartTime: &now,
		},
	}
	e := newTestExecutor(task)
	r := e.executeTool(context.Background(), "wait_for_task", map[string]any{"name": "t3", "timeout": float64(1)})
	if !r.Success {
		t.Fatalf("expected success")
	}
	data := r.Data.(map[string]any)
	if _, ok := data["elapsed"]; !ok {
		t.Error("expected elapsed field with start time only")
	}
}

func TestTaskTimeoutResult_WithStartTime(t *testing.T) {
	now := metav1.Now()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			StartTime: &now,
		},
	}
	e := newTestExecutor(task)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := e.executeTool(ctx, "wait_for_task", map[string]any{"name": "t1", "timeout": float64(1)})
	if !r.Success {
		t.Fatalf("expected success")
	}
	data := r.Data.(map[string]any)
	if _, ok := data["elapsed"]; !ok {
		t.Error("expected elapsed field")
	}
	if !strings.Contains(data["message"].(string), "still running") {
		t.Errorf("message = %v", data["message"])
	}
}

func TestTaskTimeoutResult_NoStartTime(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t2", Namespace: "default"},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhasePending,
		},
	}
	e := newTestExecutor(task)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := e.executeTool(ctx, "wait_for_task", map[string]any{"name": "t2", "timeout": float64(1)})
	if !r.Success {
		t.Fatalf("expected success")
	}
	data := r.Data.(map[string]any)
	if _, ok := data["elapsed"]; ok {
		t.Error("expected no elapsed field when no start time")
	}
}

// --- Tests: executeWaitForTask additional cases ---

func TestExecuteWaitForTask_ContextCancelled(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "running-task",
			Namespace: "default",
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseRunning,
		},
	}
	e := newTestExecutor(task)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	r := e.executeTool(ctx, "wait_for_task", map[string]any{"name": "running-task", "timeout": float64(1)})
	// Should return timeout result since context is cancelled
	if !r.Success {
		t.Fatalf("expected success (timeout result), got error: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if !strings.Contains(data["message"].(string), "still running") {
		t.Errorf("expected timeout message, got: %v", data["message"])
	}
}

func TestExecuteWaitForTask_AlreadyFailed(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "failed-task",
			Namespace: "default",
		},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseFailed,
			Message: "OOM killed",
		},
	}
	e := newTestExecutor(task)
	r := e.executeTool(context.Background(), "wait_for_task", map[string]any{"name": "failed-task"})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if data["phase"] != "Failed" {
		t.Errorf("phase = %v, want Failed", data["phase"])
	}
}

// --- Tests: handleInitialPrompt ---

func TestHandleInitialPrompt_NoPrompt(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "create_agent", map[string]any{"name": "a1"})
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if data["message"] != "Agent created" {
		t.Errorf("message = %v, want 'Agent created'", data["message"])
	}
}

func TestHandleInitialPrompt_TaskLimitReached(t *testing.T) {
	e := newTestExecutor()
	e.maxTasks = 0 // limit reached
	r := e.executeTool(context.Background(), "create_agent", map[string]any{"name": "a2", "initialPrompt": "do work"})
	if !r.Success {
		t.Fatal("expected success=true")
	}
	data := r.Data.(map[string]any)
	if !strings.Contains(data["message"].(string), "task limit reached") {
		t.Errorf("message = %v", data["message"])
	}
}

func TestHandleInitialPrompt_WithRuntimeAgent(t *testing.T) {
	e := newTestExecutor(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "copilot-token",
			Namespace: "default",
		},
	})
	r := e.executeTool(context.Background(), "create_agent", map[string]any{
		"name":          "rt-agent",
		"model":         map[string]any{"name": "test-model"},
		"runtime":       map[string]any{"type": "copilot"},
		"initialPrompt": "do work",
	})
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if data["message"] != testAgentCreatedMsg {
		t.Errorf("message = %v", data["message"])
	}
}

func TestHandleInitialPrompt_WithProviderRef(t *testing.T) {
	e := newTestExecutor()
	r := e.executeTool(context.Background(), "create_agent", map[string]any{
		"name":          "ai-agent",
		"providerRef":   "my-provider",
		"initialPrompt": "summarize",
	})
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if data["message"] != testAgentCreatedMsg {
		t.Errorf("message = %v", data["message"])
	}
}

// --- Tests: executeCreateContainerTask priority ---

func TestExecuteCreateContainerTask_WithPriority(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"image":    "alpine:latest",
		"priority": float64(100),
	}
	r := e.executeTool(context.Background(), "create_container_task", args)
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

func TestExecuteCreateContainerTask_NamespaceRestriction(t *testing.T) {
	e := newTestExecutor()
	e.enforceNamespaceIsolation = true
	args := map[string]any{
		"image":     "alpine",
		"namespace": "other-ns",
	}
	r := e.executeTool(context.Background(), "create_container_task", args)
	if r.Success {
		t.Fatal("expected failure due to namespace restriction")
	}
	if r.ErrorType != "permission_denied" {
		t.Errorf("ErrorType = %q", r.ErrorType)
	}
}

// --- Tests: executeCreateAgentTask with schedule ---

func TestExecuteCreateAgentTask_WithSchedule(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"prompt":   "run periodically",
		"agentRef": "my-agent",
		"schedule": "0 * * * *",
	}
	r := e.executeTool(context.Background(), "create_agent_task", args)
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	data := r.Data.(map[string]any)
	if !strings.Contains(data["message"].(string), "Recurring") {
		t.Errorf("expected recurring message, got %v", data["message"])
	}
}

func TestExecuteCreateAgentTask_NamespaceRestriction(t *testing.T) {
	e := newTestExecutor()
	e.enforceNamespaceIsolation = true
	args := map[string]any{
		"prompt":    "test",
		"agentRef":  "a",
		"namespace": "other-ns",
	}
	r := e.executeTool(context.Background(), "create_agent_task", args)
	if r.Success {
		t.Fatal("expected failure due to namespace restriction")
	}
}

func TestExecuteCreateAgentTask_InvalidTimeout(t *testing.T) {
	e := newTestExecutor()
	args := map[string]any{
		"prompt":   "test",
		"agentRef": "a",
		"timeout":  "bad",
	}
	r := e.executeTool(context.Background(), "create_agent_task", args)
	if r.Success {
		t.Fatal("expected failure for invalid timeout")
	}
	if r.ErrorType != "invalid_arguments" {
		t.Errorf("ErrorType = %q", r.ErrorType)
	}
}

func TestExecuteCreateAgentTask_TaskLimit(t *testing.T) {
	e := newTestExecutor()
	e.maxTasks = 0
	r := e.executeTool(context.Background(), "create_agent_task", map[string]any{"prompt": "test", "agentRef": "a"})
	if r.Success {
		t.Fatal("expected failure due to task limit")
	}
	if r.ErrorType != testLimitReached {
		t.Errorf("ErrorType = %q", r.ErrorType)
	}
}

// --- Tests: marshalResult error path ---

func TestMarshalResult_WithComplexData(t *testing.T) {
	r := ToolResult{
		Success: true,
		Data: map[string]any{
			"list":   []string{"a", "b"},
			"nested": map[string]any{"k": "v"},
		},
	}
	s, err := marshalResult(r)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(s, `"success":true`) {
		t.Errorf("expected success:true in %s", s)
	}
}

// --- Tests: executeDeleteSession with error ---

func TestExecuteDeleteSession_StoreError(t *testing.T) {
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	sm := controller.NewSessionManager(&fakeSessionStore{errOnDel: errors.New("db down")})
	e := NewToolExecutor(c, sm, "default", "sess-123", "", false, 5, 30*time.Second, &fakeResultStore{})

	r := e.executeTool(context.Background(), "delete_session", map[string]any{"sessionId": "sess-bad"})
	if r.Success {
		t.Fatal("expected failure")
	}
}

// --- Tests: executeUpdateAgent with model-only update ---

func TestExecuteUpdateAgent_ModelOnly(t *testing.T) {
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "upd-model",
			Namespace: "default",
		},
		Spec: corev1alpha1.AgentSpec{},
	}
	e := newTestExecutor(agent)
	r := e.executeTool(context.Background(), "update_agent", map[string]any{
		"name":  "upd-model",
		"model": "gpt-4",
	})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
}

// --- Tests: Execute dispatch for list/discovery tools ---

func TestExecute_ListAgents(t *testing.T) {
	e := newTestExecutor()
	tc := llm.ToolCall{
		ID:        "1",
		Name:      "list_agents",
		Arguments: mustJSON(map[string]any{}),
	}
	result, err := e.Execute(context.Background(), tc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, `"success":true`) {
		t.Errorf("expected success, got: %s", result)
	}
}

func TestExecute_ListTools(t *testing.T) {
	e := newTestExecutor()
	tc := llm.ToolCall{
		ID:        "1",
		Name:      "list_tools",
		Arguments: mustJSON(map[string]any{}),
	}
	result, err := e.Execute(context.Background(), tc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, `"success":true`) {
		t.Errorf("expected success, got: %s", result)
	}
}

func TestExecute_ListTasks(t *testing.T) {
	e := newTestExecutor()
	tc := llm.ToolCall{
		ID:        "1",
		Name:      "list_tasks",
		Arguments: mustJSON(map[string]any{}),
	}
	result, err := e.Execute(context.Background(), tc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, `"success":true`) {
		t.Errorf("expected success, got: %s", result)
	}
}

// --- Tests: executeListTasks with limit ---

func TestExecuteListTasks_WithLimit(t *testing.T) {
	tasks := make([]runtime.Object, 5)
	for i := range 5 {
		tasks[i] = &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{
				Name:              fmt.Sprintf("task-%d", i),
				Namespace:         "default",
				CreationTimestamp: metav1.Now(),
			},
			Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		}
	}
	e := newTestExecutor(tasks...)
	r := e.executeTool(context.Background(), "list_tasks", map[string]any{"limit": float64(2)})
	if !r.Success {
		t.Fatalf("expected success: %s", r.Error)
	}
	data := r.Data.([]any)
	if len(data) != 2 {
		t.Errorf("expected 2 tasks (limit), got %d", len(data))
	}
}
