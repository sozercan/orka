/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

const (
	msgTaskCreated           = "Task created"
	errTypeAlreadyExists     = "already_exists"
	testGitCredentialsSecret = "git-credentials"
)

func TestCreateAgentTaskTool_Name(t *testing.T) {
	tool := &CreateAgentTaskTool{}
	if got := tool.Name(); got != createAgentTaskToolName {
		t.Errorf("Name() = %v, want %v", got, createAgentTaskToolName)
	}
}

func TestCreateAgentTaskTool_Description(t *testing.T) {
	tool := &CreateAgentTaskTool{}
	got := tool.Description()
	if got == "" {
		t.Error("Description() returned empty string")
	}
	if want := "external CLI runtime (Copilot, Claude Code, Codex, OpenCode)"; !strings.Contains(got, want) {
		t.Errorf("Description() = %q, want it to contain %q", got, want)
	}
}

func TestCreateAgentTaskTool_Parameters(t *testing.T) {
	tool := &CreateAgentTaskTool{}
	params := tool.Parameters()
	if params == nil {
		t.Fatal("Parameters() returned nil")
	}
	var schema map[string]any
	if err := json.Unmarshal(params, &schema); err != nil {
		t.Fatalf("Parameters() returned invalid JSON: %v", err)
	}
	if schema[jsonSchemaTypeField] != typeObject {
		t.Error("Parameters schema should have type: object")
	}
	props, ok := schema[jsonSchemaPropertiesField].(map[string]any)
	if !ok {
		t.Fatal("missing properties")
	}
	for _, key := range []string{nameField, promptField, agentRefField, namespaceField, timeoutField, "maxTurns", workspaceField, scheduleField} {
		if _, ok := props[key]; !ok {
			t.Errorf("missing %s property", key)
		}
	}
	workspaceSchema, ok := props[workspaceField].(map[string]any)
	if !ok {
		t.Fatal("workspace schema is not an object")
	}
	workspaceProps, ok := workspaceSchema[jsonSchemaPropertiesField].(map[string]any)
	if !ok {
		t.Fatal("workspace schema is missing properties")
	}
	for _, key := range []string{"publicationReadCredentialRef", "publicationCredentialRef", "forgeCredentialRef"} {
		if _, ok := workspaceProps[key]; !ok {
			t.Errorf("workspace schema missing %s property", key)
		}
	}
}

func newCreateAgentTaskToolCtx(fc client.Client) context.Context {
	taskCounter := 0
	tc := &ToolContext{
		Client:    fc,
		Namespace: defaultNamespace,
		GenerateTaskName: func() string {
			taskCounter++
			return testAgentTaskGeneratedName
		},
		TaskLabels: func() map[string]string {
			return map[string]string{managedByLabelValue: trueStr}
		},
		CheckTaskLimit: func() *ChatToolError { return nil },
		IncrementTasks: func() { taskCounter++ },
	}
	return WithToolContext(context.Background(), tc)
}

func TestCreateAgentTaskTool_Execute(t *testing.T) {
	tests := []struct {
		name        string
		args        json.RawMessage
		objects     []client.Object
		checkResult func(t *testing.T, result string)
	}{
		{
			name: "happy path",
			args: json.RawMessage(`{"name":"my-agent-task","prompt":"Fix the bug","agentRef":"copilot-agent"}`),
			checkResult: func(t *testing.T, result string) {
				var r ChatToolResult
				if err := json.Unmarshal([]byte(result), &r); err != nil {
					t.Fatalf("failed to parse result: %v", err)
				}
				if !r.Success {
					t.Errorf("expected success, got error: %s", r.Error)
				}
				data := r.Data.(map[string]any)
				if data[nameField] != testAgentTaskGeneratedName {
					t.Errorf("name = %v, want agent-task-1", data[nameField])
				}
				if data[namespaceField] != defaultNamespace {
					t.Errorf("namespace = %v, want default", data[namespaceField])
				}
				if data[phaseField] != taskPhasePendingString {
					t.Errorf("phase = %v, want Pending", data[phaseField])
				}
			},
		},
		{
			name: "with workspace and maxTurns",
			args: json.RawMessage(`{
				"name":"ws-task",
				"prompt":"Refactor module",
				"agentRef":"claude-agent",
				"maxTurns": 10,
					"workspace": {
						"gitRepo": "https://github.com/example/repo",
						"branch": "main",
						"pushBranch": "feature/refactor",
						"publicationCredentialRef": "git-publish",
						"subPath": "src"
					}
			}`),
			checkResult: func(t *testing.T, result string) {
				var r ChatToolResult
				if err := json.Unmarshal([]byte(result), &r); err != nil {
					t.Fatalf("failed to parse result: %v", err)
				}
				if !r.Success {
					t.Errorf("expected success, got error: %s", r.Error)
				}
			},
		},
		{
			name: "with schedule",
			args: json.RawMessage(`{"name":"sched-task","prompt":"Run nightly","agentRef":"agent","schedule":"0 0 * * *"}`),
			checkResult: func(t *testing.T, result string) {
				var r ChatToolResult
				if err := json.Unmarshal([]byte(result), &r); err != nil {
					t.Fatalf("failed to parse result: %v", err)
				}
				if !r.Success {
					t.Errorf("expected success, got error: %s", r.Error)
				}
				data := r.Data.(map[string]any)
				msg := data[messageField].(string)
				if msg == msgTaskCreated {
					t.Error("expected scheduled message, got one-time message")
				}
			},
		},
		{
			name: "missing prompt",
			args: json.RawMessage(`{"name":"t","agentRef":"a"}`),
			checkResult: func(t *testing.T, result string) {
				var r ChatToolResult
				if err := json.Unmarshal([]byte(result), &r); err != nil {
					t.Fatalf("failed to parse result: %v", err)
				}
				if r.Success {
					t.Error("expected failure for missing prompt")
				}
				if r.ErrorType != errTypeInvalidArgs {
					t.Errorf("errorType = %v, want invalid_arguments", r.ErrorType)
				}
			},
		},
		{
			name: "missing agentRef",
			args: json.RawMessage(`{"name":"t","prompt":"do it"}`),
			checkResult: func(t *testing.T, result string) {
				var r ChatToolResult
				if err := json.Unmarshal([]byte(result), &r); err != nil {
					t.Fatalf("failed to parse result: %v", err)
				}
				if r.Success {
					t.Error("expected failure for missing agentRef")
				}
				if r.ErrorType != errTypeInvalidArgs {
					t.Errorf("errorType = %v, want invalid_arguments", r.ErrorType)
				}
			},
		},
		{
			name: invalidJSONArgsCaseName,
			args: json.RawMessage(`{bad`),
			checkResult: func(t *testing.T, result string) {
				var r ChatToolResult
				if err := json.Unmarshal([]byte(result), &r); err != nil {
					t.Fatalf("failed to parse result: %v", err)
				}
				if r.Success {
					t.Error("expected failure for invalid JSON")
				}
				if r.ErrorType != errTypeInvalidArgs {
					t.Errorf("errorType = %v, want invalid_arguments", r.ErrorType)
				}
			},
		},
		{
			name: invalidTimeoutCaseName,
			args: json.RawMessage(`{"name":"t","prompt":"p","agentRef":"a","timeout":"bad"}`),
			checkResult: func(t *testing.T, result string) {
				var r ChatToolResult
				if err := json.Unmarshal([]byte(result), &r); err != nil {
					t.Fatalf("failed to parse result: %v", err)
				}
				if r.Success {
					t.Error("expected failure for invalid timeout")
				}
				if r.ErrorType != errTypeInvalidArgs {
					t.Errorf("errorType = %v, want invalid_arguments", r.ErrorType)
				}
			},
		},
		{
			name: k8sAlreadyExistsErrorCaseName,
			args: json.RawMessage(`{"name":"existing","prompt":"do it","agentRef":"a"}`),
			objects: []client.Object{
				&corev1alpha1.Task{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testAgentTaskGeneratedName,
						Namespace: defaultNamespace,
					},
					Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent},
				},
			},
			checkResult: func(t *testing.T, result string) {
				var r ChatToolResult
				if err := json.Unmarshal([]byte(result), &r); err != nil {
					t.Fatalf("failed to parse result: %v", err)
				}
				if r.Success {
					t.Error("expected failure for already exists")
				}
				if r.ErrorType != errTypeAlreadyExists {
					t.Errorf("errorType = %v, want already_exists", r.ErrorType)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := newFakeClient(tt.objects...)
			ctx := newCreateAgentTaskToolCtx(fc)
			tool := &CreateAgentTaskTool{}

			result, err := tool.Execute(ctx, tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.checkResult != nil {
				tt.checkResult(t, result)
			}
		})
	}
}

func TestCreateAgentTaskTool_Execute_RejectsNonObjectWorkspace(t *testing.T) {
	for _, workspace := range []string{`"repo"`, `["repo"]`, `null`} {
		t.Run(workspace, func(t *testing.T) {
			fc := newFakeClient()
			ctx := newCreateAgentTaskToolCtx(fc)
			tool := &CreateAgentTaskTool{}
			args := json.RawMessage(`{"prompt":"Fix the bug","agentRef":"codex-agent","workspace":` + workspace + `}`)

			result, err := tool.Execute(ctx, args)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			var response ChatToolResult
			if err := json.Unmarshal([]byte(result), &response); err != nil {
				t.Fatal(err)
			}
			if response.Success || response.ErrorType != errTypeInvalidArgs || !strings.Contains(response.Error, "workspace must be an object") {
				t.Fatalf("response = %#v, want invalid workspace arguments", response)
			}
		})
	}
}

func TestCreateAgentTaskTool_Execute_ParsesStringBooleanCreatePR(t *testing.T) {
	fc := newFakeClient()
	ctx := newCreateAgentTaskToolCtx(fc)
	tool := &CreateAgentTaskTool{}
	args := json.RawMessage(`{
		"prompt":"Fix the bug",
		"agentRef":"codex-agent",
		"workspace":{
			"gitRepo":"https://github.com/example/source.git",
			"pushBranch":"orka/fix",
			"prBaseBranch":"main",
			"createPR":"true",
			"readCredentialRef":"source-read",
			"publicationCredentialRef":"target-write",
			"forgeCredentialRef":"forge"
		}
	}`)

	result, err := tool.Execute(ctx, args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var response ChatToolResult
	if err := json.Unmarshal([]byte(result), &response); err != nil {
		t.Fatal(err)
	}
	if !response.Success {
		t.Fatalf("response = %#v, want success", response)
	}
	task := &corev1alpha1.Task{}
	if err := fc.Get(t.Context(), apitypes.NamespacedName{Name: testAgentTaskGeneratedName, Namespace: defaultNamespace}, task); err != nil {
		t.Fatal(err)
	}
	if task.Spec.Workspace == nil || !task.Spec.Workspace.CreatePR {
		t.Fatalf("workspace = %#v, want createPR true from string boolean", task.Spec.Workspace)
	}
}

func TestCreateAgentTaskTool_Execute_RejectsInvalidCreatePRAndMaxTurns(t *testing.T) {
	tests := []struct {
		name string
		args string
		want string
	}{
		{
			name: "createPR",
			args: `{"prompt":"Fix","agentRef":"codex-agent","workspace":{"gitRepo":"https://github.com/example/repo","createPR":"yes-please"}}`,
			want: "workspace.createPR must be a boolean",
		},
		{
			name: "maxTurns",
			args: `{"prompt":"Fix","agentRef":"codex-agent","maxTurns":"lots"}`,
			want: "maxTurns must be an integer",
		},
		{
			name: "maxTurnsOutOfRange",
			args: `{"prompt":"Fix","agentRef":"codex-agent","maxTurns":2147483648}`,
			want: "maxTurns must be between 1 and 1000",
		},
		{
			name: "maxTurnsNonPositive",
			args: `{"prompt":"Fix","agentRef":"codex-agent","maxTurns":0}`,
			want: "maxTurns must be between 1 and 1000",
		},
		{
			name: "maxTurnsFractional",
			args: `{"prompt":"Fix","agentRef":"codex-agent","maxTurns":1.9}`,
			want: "maxTurns must be an integer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := newFakeClient()
			ctx := newCreateAgentTaskToolCtx(fc)
			tool := &CreateAgentTaskTool{}

			result, err := tool.Execute(ctx, json.RawMessage(tt.args))
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			var response ChatToolResult
			if err := json.Unmarshal([]byte(result), &response); err != nil {
				t.Fatal(err)
			}
			if response.Success || response.ErrorType != errTypeInvalidArgs || !strings.Contains(response.Error, tt.want) {
				t.Fatalf("response = %#v, want %q", response, tt.want)
			}
		})
	}
}

func TestCreateAgentTaskTool_Execute_RequiresExplicitPublicationCredentialForWrite(t *testing.T) {
	fc := newFakeClient()
	ctx := newCreateAgentTaskToolCtx(fc)
	tool := &CreateAgentTaskTool{}
	args := json.RawMessage(`{
		"prompt":"Fix the bug",
		"agentRef":"codex-agent",
		"workspace":{"gitRepo":"https://github.com/example/repo","intent":"write","readCredentialRef":"source-read"}
	}`)

	result, err := tool.Execute(ctx, args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var response ChatToolResult
	if err := json.Unmarshal([]byte(result), &response); err != nil {
		t.Fatal(err)
	}
	if response.Success || response.ErrorType != errTypeInvalidArgs || !strings.Contains(response.Error, "publicationCredentialRef is required") {
		t.Fatalf("response = %#v, want explicit publication credential denial", response)
	}
}

func TestCreateAgentTaskTool_Execute_RejectsInvalidSourceSelectors(t *testing.T) {
	tests := []struct {
		name      string
		workspace string
		want      string
	}{
		{
			name:      "unsupported ref namespace",
			workspace: `{"gitRepo":"https://github.com/example/repo","ref":"refs/remotes/origin/main"}`,
			want:      "workspace.ref is invalid",
		},
		{
			name:      "malformed ref",
			workspace: `{"gitRepo":"https://github.com/example/repo","ref":"refs/heads/bad..ref"}`,
			want:      "workspace.ref is invalid",
		},
		{
			name:      "malformed branch",
			workspace: `{"gitRepo":"https://github.com/example/repo","branch":"bad..branch"}`,
			want:      "workspace.branch is invalid",
		},
		{
			name:      "traversal subPath",
			workspace: `{"gitRepo":"https://github.com/example/repo","subPath":"../private"}`,
			want:      "workspace.subPath contains an unsafe segment",
		},
		{
			name:      "absolute subPath",
			workspace: `{"gitRepo":"https://github.com/example/repo","subPath":"/absolute"}`,
			want:      "workspace.subPath must be a relative slash-separated path",
		},
		{
			name:      "empty subPath segment",
			workspace: `{"gitRepo":"https://github.com/example/repo","subPath":"a//b"}`,
			want:      "workspace.subPath contains an unsafe segment",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := newFakeClient()
			ctx := newCreateAgentTaskToolCtx(fc)
			tool := &CreateAgentTaskTool{}
			args := json.RawMessage(`{"prompt":"Fix the bug","agentRef":"codex-agent","workspace":` + tt.workspace + `}`)

			result, err := tool.Execute(ctx, args)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			var response ChatToolResult
			if err := json.Unmarshal([]byte(result), &response); err != nil {
				t.Fatal(err)
			}
			if response.Success || response.ErrorType != errTypeInvalidArgs || !strings.Contains(response.Error, tt.want) {
				t.Fatalf("response = %#v, want %q rejection", response, tt.want)
			}
		})
	}
}

func TestCreateAgentTaskTool_Execute_BindsPublicationCredentialRoles(t *testing.T) {
	fc := newFakeClient()
	ctx := newCreateAgentTaskToolCtx(fc)
	tool := &CreateAgentTaskTool{}
	args := json.RawMessage(`{
		"prompt":"Fix the bug",
		"agentRef":"codex-agent",
		"workspace":{
			"gitRepo":"https://github.com/example/source.git",
			"publicationGitRepo":"https://github.com/example/target.git",
			"pushBranch":"orka/fix",
			"prBaseBranch":"main",
			"createPR":true,
			"readCredentialRef":"source-read",
			"publicationReadCredentialRef":"target-read",
			"publicationCredentialRef":"target-write",
			"forgeCredentialRef":"forge"
		}
	}`)

	result, err := tool.Execute(ctx, args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var response ChatToolResult
	if err := json.Unmarshal([]byte(result), &response); err != nil {
		t.Fatal(err)
	}
	if !response.Success {
		t.Fatalf("response = %#v, want success", response)
	}
	task := &corev1alpha1.Task{}
	if err := fc.Get(t.Context(), apitypes.NamespacedName{Name: testAgentTaskGeneratedName, Namespace: defaultNamespace}, task); err != nil {
		t.Fatal(err)
	}
	workspace := task.Spec.Workspace
	if workspace == nil || workspace.ReadCredentialRef == nil || workspace.ReadCredentialRef.Name != "source-read" {
		t.Fatalf("readCredentialRef = %#v, want source-read", workspace)
	}
	if workspace.PublicationReadCredentialRef == nil || workspace.PublicationReadCredentialRef.Name != "target-read" {
		t.Fatalf("publicationReadCredentialRef = %#v, want target-read", workspace.PublicationReadCredentialRef)
	}
	if workspace.PublicationCredentialRef == nil || workspace.PublicationCredentialRef.Name != "target-write" {
		t.Fatalf("publicationCredentialRef = %#v, want target-write", workspace.PublicationCredentialRef)
	}
	if workspace.ForgeCredentialRef == nil || workspace.ForgeCredentialRef.Name != "forge" {
		t.Fatalf("forgeCredentialRef = %#v, want forge", workspace.ForgeCredentialRef)
	}
}

func TestCreateAgentTaskTool_Execute_PreservesExplicitReadCredentialRef(t *testing.T) {
	fc := newFakeClient(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "my-secret", Namespace: defaultNamespace}},
	)
	ctx := newCreateAgentTaskToolCtx(fc)
	tool := &CreateAgentTaskTool{}

	result, err := tool.Execute(ctx, json.RawMessage(`{
		"prompt":"Refactor repo",
		"agentRef":"claude-agent",
		"workspace":{
			"gitRepo":"https://github.com/example/repo",
			"readCredentialRef":"my-secret"
		}
	}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var r ChatToolResult
	if err := json.Unmarshal([]byte(result), &r); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}

	task := &corev1alpha1.Task{}
	if err := fc.Get(context.Background(), apitypes.NamespacedName{Name: testAgentTaskGeneratedName, Namespace: defaultNamespace}, task); err != nil {
		t.Fatalf("failed to get task: %v", err)
	}
	if task.Spec.Workspace == nil {
		t.Fatal("expected workspace to be set")
	}
	if task.Spec.Workspace.ReadCredentialRef == nil {
		t.Fatal("expected readCredentialRef to be preserved")
	}
	if task.Spec.Workspace.ReadCredentialRef.Name != "my-secret" {
		t.Errorf("readCredentialRef = %q, want %q", task.Spec.Workspace.ReadCredentialRef.Name, "my-secret")
	}
}

func TestCreateAgentTaskTool_Execute_AutoDiscoversReadCredentialRefWhenOmitted(t *testing.T) {
	fc := newFakeClient(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: testGitCredentialsSecret, Namespace: defaultNamespace}},
	)
	ctx := newCreateAgentTaskToolCtx(fc)
	tool := &CreateAgentTaskTool{}

	result, err := tool.Execute(ctx, json.RawMessage(`{
		"prompt":"Refactor repo",
		"agentRef":"claude-agent",
		"workspace":{
			"gitRepo":"https://github.com/example/repo"
		}
	}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var r ChatToolResult
	if err := json.Unmarshal([]byte(result), &r); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}

	task := &corev1alpha1.Task{}
	if err := fc.Get(context.Background(), apitypes.NamespacedName{Name: testAgentTaskGeneratedName, Namespace: defaultNamespace}, task); err != nil {
		t.Fatalf("failed to get task: %v", err)
	}
	if task.Spec.Workspace == nil {
		t.Fatal("expected workspace to be set")
	}
	if task.Spec.Workspace.ReadCredentialRef == nil {
		t.Fatal("expected readCredentialRef to be auto-discovered")
	}
	if task.Spec.Workspace.ReadCredentialRef.Name != testGitCredentialsSecret {
		t.Fatalf("readCredentialRef = %q, want %q", task.Spec.Workspace.ReadCredentialRef.Name, testGitCredentialsSecret)
	}
}

func TestCreateAgentTaskTool_Execute_UsesCopilotAgentSecretForGitCredentials(t *testing.T) {
	fc := newFakeClient(
		&corev1alpha1.Agent{
			ObjectMeta: metav1.ObjectMeta{Name: "copilot-agent", Namespace: defaultNamespace},
			Spec: corev1alpha1.AgentSpec{
				Runtime:   &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCopilot},
				SecretRef: &corev1.LocalObjectReference{Name: testCustomCopilotSecretName},
			},
		},
	)
	ctx := newCreateAgentTaskToolCtx(fc)
	tool := &CreateAgentTaskTool{}

	result, err := tool.Execute(ctx, json.RawMessage(`{
		"prompt":"Refactor repo",
		"agentRef":"copilot-agent",
		"workspace":{
			"gitRepo":"https://github.com/example/repo"
		}
	}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var r ChatToolResult
	if err := json.Unmarshal([]byte(result), &r); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}

	task := &corev1alpha1.Task{}
	if err := fc.Get(context.Background(), apitypes.NamespacedName{Name: testAgentTaskGeneratedName, Namespace: defaultNamespace}, task); err != nil {
		t.Fatalf("failed to get task: %v", err)
	}
	if task.Spec.Workspace == nil || task.Spec.Workspace.ReadCredentialRef == nil {
		t.Fatal("expected readCredentialRef to be populated from the copilot agent")
	}
	if task.Spec.Workspace.ReadCredentialRef.Name != testCustomCopilotSecretName {
		t.Fatalf("readCredentialRef = %q, want %q", task.Spec.Workspace.ReadCredentialRef.Name, testCustomCopilotSecretName)
	}
}

func TestCreateAgentTaskTool_Execute_MirrorsWorkspacePreflight(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		wantErr string
	}{
		{
			name:    "branch requires gitRepo",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"branch":"main"}}`,
			wantErr: "workspace.branch requires workspace.gitRepo",
		},
		{
			name:    "ref requires gitRepo",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"ref":"abc123"}}`,
			wantErr: "workspace.ref requires workspace.gitRepo",
		},
		{
			name:    "subPath requires gitRepo",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"subPath":"src"}}`,
			wantErr: "workspace.subPath requires workspace.gitRepo",
		},
		{
			name:    "readCredentialRef requires gitRepo",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"readCredentialRef":"source-read"}}`,
			wantErr: "workspace.readCredentialRef requires workspace.gitRepo",
		},
		{
			name:    "publicationReadCredentialRef requires write intent",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://github.com/example/repo","publicationReadCredentialRef":"target-read"}}`,
			wantErr: "workspace.publicationReadCredentialRef requires write workspace intent",
		},
		{
			name:    "publicationCredentialRef requires write intent",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://github.com/example/repo","publicationCredentialRef":"target-write"}}`,
			wantErr: "workspace.publicationCredentialRef requires write workspace intent",
		},
		{
			name:    "forgeCredentialRef requires write intent",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://github.com/example/repo","forgeCredentialRef":"forge"}}`,
			wantErr: "workspace.forgeCredentialRef requires write workspace intent",
		},
		{
			name:    "write intent requires gitRepo",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"intent":"write","publicationCredentialRef":"target-write"}}`,
			wantErr: "workspace.gitRepo is required for write intent",
		},
		{
			name:    "invalid pushBranch",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://github.com/example/repo","pushBranch":"bad..branch","publicationCredentialRef":"target-write"}}`,
			wantErr: "workspace.pushBranch is invalid",
		},
		{
			name:    "createPR requires prBaseBranch",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://github.com/example/repo","pushBranch":"orka/fix","createPR":true,"publicationCredentialRef":"target-write","forgeCredentialRef":"forge"}}`,
			wantErr: "workspace.createPR requires workspace.prBaseBranch",
		},
		{
			name:    "createPR with invalid prBaseBranch",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://github.com/example/repo","pushBranch":"orka/fix","prBaseBranch":"bad branch","createPR":true,"publicationCredentialRef":"target-write","forgeCredentialRef":"forge"}}`,
			wantErr: "workspace.prBaseBranch is invalid",
		},
		{
			name:    "createPR requires forgeCredentialRef",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://github.com/example/repo","pushBranch":"orka/fix","prBaseBranch":"main","createPR":true,"publicationCredentialRef":"target-write"}}`,
			wantErr: "workspace.createPR requires workspace.forgeCredentialRef",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := newFakeClient()
			ctx := newCreateAgentTaskToolCtx(fc)
			tool := &CreateAgentTaskTool{}

			result, err := tool.Execute(ctx, json.RawMessage(tt.args))
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			var response ChatToolResult
			if err := json.Unmarshal([]byte(result), &response); err != nil {
				t.Fatal(err)
			}
			if response.Success || response.ErrorType != errTypeInvalidArgs || !strings.Contains(response.Error, tt.wantErr) {
				t.Fatalf("response = %#v, want %q", response, tt.wantErr)
			}
			taskList := &corev1alpha1.TaskList{}
			if err := fc.List(context.Background(), taskList); err != nil {
				t.Fatal(err)
			}
			if len(taskList.Items) != 0 {
				t.Fatalf("expected no Task to be created, got %d", len(taskList.Items))
			}
		})
	}
}

func TestCreateAgentTaskTool_Execute_CanonicalizesWorkspaceRepositoryArgs(t *testing.T) {
	tests := []struct {
		name        string
		args        string
		wantErr     string
		wantGitRepo string
		wantPubRepo string
	}{
		{
			name:        "github ssh gitRepo canonicalized",
			args:        `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"git@github.com:example/repo.git"}}`,
			wantGitRepo: "https://github.com/example/repo",
		},
		{
			name:        "plain https gitRepo accepted",
			args:        `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://github.com/example/repo"}}`,
			wantGitRepo: "https://github.com/example/repo",
		},
		{
			name:    "http gitRepo rejected",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"http://github.com/example/repo"}}`,
			wantErr: "workspace.gitRepo must be a credential-free HTTPS URL",
		},
		{
			name:    "credential-embedding gitRepo rejected",
			args:    `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://user:pass@github.com/example/repo"}}`,
			wantErr: "workspace.gitRepo must be a credential-free HTTPS URL",
		},
		{
			name: "github ssh publicationGitRepo canonicalized",
			args: `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://github.com/example/repo",` +
				`"publicationGitRepo":"git@github.com:example/fork.git","publicationCredentialRef":"target-write"}}`,
			wantGitRepo: "https://github.com/example/repo",
			wantPubRepo: "https://github.com/example/fork",
		},
		{
			name: "http publicationGitRepo rejected",
			args: `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://github.com/example/repo",` +
				`"publicationGitRepo":"http://github.com/example/fork","publicationCredentialRef":"target-write"}}`,
			wantErr: "workspace.publicationGitRepo must be a credential-free HTTPS URL",
		},
		{
			name: "credential-embedding publicationGitRepo rejected",
			args: `{"prompt":"p","agentRef":"a","workspace":{"gitRepo":"https://github.com/example/repo",` +
				`"publicationGitRepo":"https://user:pass@github.com/example/fork","publicationCredentialRef":"target-write"}}`,
			wantErr: "workspace.publicationGitRepo must be a credential-free HTTPS URL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := newFakeClient()
			ctx := newCreateAgentTaskToolCtx(fc)
			tool := &CreateAgentTaskTool{}

			result, err := tool.Execute(ctx, json.RawMessage(tt.args))
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			var response ChatToolResult
			if err := json.Unmarshal([]byte(result), &response); err != nil {
				t.Fatal(err)
			}
			if tt.wantErr != "" {
				if response.Success || response.ErrorType != errTypeInvalidArgs || !strings.Contains(response.Error, tt.wantErr) {
					t.Fatalf("response = %#v, want invalid_arguments containing %q", response, tt.wantErr)
				}
				taskList := &corev1alpha1.TaskList{}
				if err := fc.List(context.Background(), taskList); err != nil {
					t.Fatal(err)
				}
				if len(taskList.Items) != 0 {
					t.Fatalf("expected no Task to be created, got %d", len(taskList.Items))
				}
				return
			}
			if !response.Success {
				t.Fatalf("expected success, got error: %s", response.Error)
			}
			task := &corev1alpha1.Task{}
			if err := fc.Get(context.Background(), apitypes.NamespacedName{Name: testAgentTaskGeneratedName, Namespace: defaultNamespace}, task); err != nil {
				t.Fatal(err)
			}
			if task.Spec.Workspace == nil {
				t.Fatal("expected workspace to be set")
			}
			if task.Spec.Workspace.GitRepo != tt.wantGitRepo {
				t.Fatalf("gitRepo = %q, want %q", task.Spec.Workspace.GitRepo, tt.wantGitRepo)
			}
			if task.Spec.Workspace.PublicationGitRepo != tt.wantPubRepo {
				t.Fatalf("publicationGitRepo = %q, want %q", task.Spec.Workspace.PublicationGitRepo, tt.wantPubRepo)
			}
		})
	}
}

func TestCreateAgentTaskTool_Execute_AllowsReadWorkspaceWithoutGitRepo(t *testing.T) {
	// The controller preflight rejects readCredentialRef without gitRepo, so
	// auto-discovery must not attach one to a repository-free read workspace.
	fc := newFakeClient(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: testGitCredentialsSecret, Namespace: defaultNamespace}},
	)
	ctx := newCreateAgentTaskToolCtx(fc)
	tool := &CreateAgentTaskTool{}

	result, err := tool.Execute(ctx, json.RawMessage(`{"prompt":"p","agentRef":"a","workspace":{"intent":"read"}}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var r ChatToolResult
	if err := json.Unmarshal([]byte(result), &r); err != nil {
		t.Fatal(err)
	}
	if !r.Success {
		t.Fatalf("expected success, got error: %s", r.Error)
	}
	task := &corev1alpha1.Task{}
	if err := fc.Get(context.Background(), apitypes.NamespacedName{Name: testAgentTaskGeneratedName, Namespace: defaultNamespace}, task); err != nil {
		t.Fatal(err)
	}
	if task.Spec.Workspace == nil {
		t.Fatal("expected workspace to be set")
	}
	if task.Spec.Workspace.ReadCredentialRef != nil {
		t.Fatalf("readCredentialRef = %#v, want nil without gitRepo", task.Spec.Workspace.ReadCredentialRef)
	}
}

func TestCreateAgentTaskTool_Execute_MaterializesRuntimeRefAllowedTools(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	for _, tt := range []struct {
		name    string
		allowed []string
	}{
		{name: "nonempty allowlist", allowed: []string{"check_messages", "web_search"}},
		{name: "explicit deny all", allowed: []string{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const runtimeName = "external-runtime"
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "external-agent", Namespace: defaultNamespace},
				Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
					RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: runtimeName},
				}},
			}
			runtime := &corev1alpha1.AgentRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: runtimeName, Namespace: defaultNamespace},
				Spec: corev1alpha1.AgentRuntimeRegistrySpec{
					ContractVersion: &contract,
					Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
						Profile: &corev1alpha1.AgentRuntimeProfileSpec{
							ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
						},
						MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
							AllowedTools:          append([]string{}, tt.allowed...),
							DisallowedTools:       []string{},
							ApprovalRequiredTools: []string{},
						},
					},
				},
			}
			fc := newFakeClient(agent, runtime)
			result, err := (&CreateAgentTaskTool{}).Execute(
				newCreateAgentTaskToolCtx(fc),
				json.RawMessage(`{"name":"external-task","prompt":"work","agentRef":"external-agent"}`),
			)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			var response ChatToolResult
			if err := json.Unmarshal([]byte(result), &response); err != nil {
				t.Fatal(err)
			}
			if !response.Success {
				t.Fatalf("Execute() result = %#v", response)
			}

			task := &corev1alpha1.Task{}
			if err := fc.Get(context.Background(), apitypes.NamespacedName{
				Name: testAgentTaskGeneratedName, Namespace: defaultNamespace,
			}, task); err != nil {
				t.Fatal(err)
			}
			if task.Spec.AgentRuntime == nil {
				t.Fatal("agentRuntime = nil, want materialized runtime policy")
			}
			if !slices.Equal(task.Spec.AgentRuntime.AllowedTools, tt.allowed) {
				t.Fatalf("allowedTools = %#v, want %#v", task.Spec.AgentRuntime.AllowedTools, tt.allowed)
			}
			if task.Spec.AgentRuntime.AllowedTools == nil {
				t.Fatal("allowedTools = nil, want explicit list")
			}
		})
	}
}

func TestCreateAgentTaskTool_Execute_UsesRuntimePolicyReader(t *testing.T) {
	cachedAgent, cachedRuntime := externalRuntimePolicyFixtures([]string{"Read"})
	liveAgent, liveRuntime := externalRuntimePolicyFixtures([]string{"Write"})
	cachedClient := newFakeClient(cachedAgent, cachedRuntime)
	liveReader := newFakeClient(liveAgent, liveRuntime)
	ctx := newCreateAgentTaskToolCtx(cachedClient)
	GetToolContext(ctx).PolicyReader = liveReader

	result, err := (&CreateAgentTaskTool{}).Execute(
		ctx,
		json.RawMessage(`{"name":"external-task","prompt":"work","agentRef":"external-agent"}`),
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var response ChatToolResult
	if err := json.Unmarshal([]byte(result), &response); err != nil {
		t.Fatal(err)
	}
	if !response.Success {
		t.Fatalf("Execute() result = %#v", response)
	}

	task := &corev1alpha1.Task{}
	if err := cachedClient.Get(context.Background(), apitypes.NamespacedName{
		Name: testAgentTaskGeneratedName, Namespace: defaultNamespace,
	}, task); err != nil {
		t.Fatal(err)
	}
	if task.Spec.AgentRuntime == nil || !slices.Equal(task.Spec.AgentRuntime.AllowedTools, []string{"Write"}) {
		t.Fatalf("agentRuntime = %#v, want current live policy", task.Spec.AgentRuntime)
	}
}

func TestCreateAgentTaskTool_Execute_RejectsRuntimeRefMaxTurns(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	const runtimeName = "external-runtime"
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "external-agent", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: runtimeName},
		}},
	}
	runtime := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: runtimeName, Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
				},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:          []string{},
					DisallowedTools:       []string{},
					ApprovalRequiredTools: []string{},
				},
			},
		},
	}
	fc := newFakeClient(agent, runtime)
	result, err := (&CreateAgentTaskTool{}).Execute(
		newCreateAgentTaskToolCtx(fc),
		json.RawMessage(`{"name":"external-task","prompt":"work","agentRef":"external-agent","maxTurns":10}`),
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var response ChatToolResult
	if err := json.Unmarshal([]byte(result), &response); err != nil {
		t.Fatal(err)
	}
	if response.Success || !strings.Contains(response.Error, "do not support maxTurns") {
		t.Fatalf("Execute() result = %#v, want unsupported maxTurns error", response)
	}
	tasks := &corev1alpha1.TaskList{}
	if err := fc.List(context.Background(), tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks.Items) != 0 {
		t.Fatalf("created %d Tasks after unsupported maxTurns override", len(tasks.Items))
	}
}

func TestCreateAgentTaskTool_Execute_RejectsRuntimeRefWithoutExplicitAllowedTools(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	const runtimeName = "external-runtime"
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "external-agent", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: runtimeName},
		}},
	}
	runtime := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: runtimeName, Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
				},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					DisallowedTools:       []string{},
					ApprovalRequiredTools: []string{},
				},
			},
		},
	}
	fc := newFakeClient(agent, runtime)
	result, err := (&CreateAgentTaskTool{}).Execute(
		newCreateAgentTaskToolCtx(fc),
		json.RawMessage(`{"name":"external-task","prompt":"work","agentRef":"external-agent"}`),
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var response ChatToolResult
	if err := json.Unmarshal([]byte(result), &response); err != nil {
		t.Fatal(err)
	}
	if response.Success || !strings.Contains(response.Error, "allowedTools must be an explicit list") {
		t.Fatalf("Execute() result = %#v, want fail-closed policy error", response)
	}
	tasks := &corev1alpha1.TaskList{}
	if err := fc.List(context.Background(), tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks.Items) != 0 {
		t.Fatalf("created %d Tasks after invalid runtime policy", len(tasks.Items))
	}
}

func TestCreateAgentTaskTool_Execute_MissingContext(t *testing.T) {
	tool := &CreateAgentTaskTool{}
	result, err := tool.Execute(context.Background(), json.RawMessage(`{"name":"t","prompt":"p","agentRef":"a"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var r ChatToolResult
	if err := json.Unmarshal([]byte(result), &r); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}
	if r.Success {
		t.Error("expected failure for missing context")
	}
	if r.ErrorType != errTypeInternalError {
		t.Errorf("errorType = %v, want internal_error", r.ErrorType)
	}
}
