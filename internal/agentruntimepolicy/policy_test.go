package agentruntimepolicy

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

func TestMaterializeRuntimeRefAllowedToolsPreservesExplicitEmpty(t *testing.T) {
	task := &corev1alpha1.Task{}
	if err := MaterializeRuntimeRefAllowedTools(task, &RuntimeRefPolicy{
		WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
		AllowedTools:    []string{},
	}); err != nil {
		t.Fatal(err)
	}
	if task.Spec.AgentRuntime == nil || task.Spec.AgentRuntime.AllowedTools == nil || len(task.Spec.AgentRuntime.AllowedTools) != 0 {
		t.Fatalf("agentRuntime = %#v, want explicit empty allowedTools", task.Spec.AgentRuntime)
	}
}

func TestMaterializeRuntimeRefAllowedToolsValidatesWorkspaceIntent(t *testing.T) {
	for _, test := range []struct {
		name            string
		taskIntent      corev1alpha1.WorkspaceIntent
		profileIntent   corev1alpha1.WorkspaceIntent
		wantErrorIntent corev1alpha1.WorkspaceIntent
	}{
		{name: "default read matches", profileIntent: corev1alpha1.WorkspaceIntentRead},
		{name: "explicit write matches", taskIntent: corev1alpha1.WorkspaceIntentWrite, profileIntent: corev1alpha1.WorkspaceIntentWrite},
		{name: "default read rejects write profile", profileIntent: corev1alpha1.WorkspaceIntentWrite, wantErrorIntent: corev1alpha1.WorkspaceIntentRead},
		{name: "write rejects read profile", taskIntent: corev1alpha1.WorkspaceIntentWrite, profileIntent: corev1alpha1.WorkspaceIntentRead, wantErrorIntent: corev1alpha1.WorkspaceIntentWrite},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := &corev1alpha1.Task{}
			if test.taskIntent != "" {
				task.Spec.Workspace = &corev1alpha1.WorkspaceConfig{Intent: test.taskIntent}
			}
			err := MaterializeRuntimeRefAllowedTools(task, &RuntimeRefPolicy{
				WorkspaceIntent: test.profileIntent,
				AllowedTools:    []string{"read_evidence"},
			})
			if test.wantErrorIntent != "" {
				if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("Task intent %q", test.wantErrorIntent)) {
					t.Fatalf("error = %v, want Task intent %q mismatch", err, test.wantErrorIntent)
				}
				if task.Spec.AgentRuntime != nil {
					t.Fatalf("agentRuntime = %#v, want no mutation after rejected intent", task.Spec.AgentRuntime)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if task.Spec.AgentRuntime == nil || !slices.Equal(task.Spec.AgentRuntime.AllowedTools, []string{"read_evidence"}) {
				t.Fatalf("agentRuntime = %#v, want materialized allowedTools", task.Spec.AgentRuntime)
			}
		})
	}
}

func TestPolicyForRuntimeCopiesRegisteredPolicy(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	runtimeObject := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "external-runtime"},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentWrite,
				},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:    []string{"read_evidence"},
					DisallowedTools: []string{},
				},
			},
		},
	}
	policy, err := PolicyForRuntime(runtimeObject)
	if err != nil {
		t.Fatal(err)
	}
	runtimeObject.Spec.Capabilities.MCPPolicy.AllowedTools[0] = "changed"
	if policy == nil || policy.WorkspaceIntent != corev1alpha1.WorkspaceIntentWrite || len(policy.AllowedTools) != 1 || policy.AllowedTools[0] != "read_evidence" || policy.DisallowedTools == nil {
		t.Fatalf("resolved policy = %#v, want copied explicit lists", policy)
	}
}

func TestResolveAndMaterializeTaskRuntimeRefAllowedTools(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	for _, tt := range []struct {
		name           string
		agentNamespace string
		referenceNS    string
		allowedTools   []string
	}{
		{name: "default Agent namespace", allowedTools: []string{"read_evidence"}},
		{name: "explicit Agent namespace with deny all", agentNamespace: "agent-catalog", referenceNS: "agent-catalog", allowedTools: []string{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const taskNamespace = "task-namespace"
			agentNamespace := tt.agentNamespace
			if agentNamespace == "" {
				agentNamespace = taskNamespace
			}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "analysis", Namespace: agentNamespace},
				Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
					RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
				}},
			}
			runtimeObject := &corev1alpha1.AgentRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: taskNamespace},
				Spec: corev1alpha1.AgentRuntimeRegistrySpec{
					ContractVersion: &contract,
					Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
						Profile: &corev1alpha1.AgentRuntimeProfileSpec{
							ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
						},
						MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
							AllowedTools:    append([]string{}, tt.allowedTools...),
							DisallowedTools: []string{},
						},
					},
				},
			}
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "generated", Namespace: taskNamespace},
				Spec: corev1alpha1.TaskSpec{
					Type:     corev1alpha1.TaskTypeAgent,
					AgentRef: &corev1alpha1.AgentReference{Name: agent.Name, Namespace: tt.referenceNS},
				},
			}
			scheme := runtime.NewScheme()
			if err := corev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent, runtimeObject).Build()

			if err := ResolveAndMaterializeTaskRuntimeRefAllowedTools(context.Background(), reader, task); err != nil {
				t.Fatal(err)
			}
			if task.Spec.AgentRuntime == nil || task.Spec.AgentRuntime.AllowedTools == nil {
				t.Fatalf("agentRuntime = %#v, want explicit allowedTools", task.Spec.AgentRuntime)
			}
			if len(task.Spec.AgentRuntime.AllowedTools) != len(tt.allowedTools) {
				t.Fatalf("allowedTools = %#v, want %#v", task.Spec.AgentRuntime.AllowedTools, tt.allowedTools)
			}
			for i := range tt.allowedTools {
				if task.Spec.AgentRuntime.AllowedTools[i] != tt.allowedTools[i] {
					t.Fatalf("allowedTools = %#v, want %#v", task.Spec.AgentRuntime.AllowedTools, tt.allowedTools)
				}
			}
		})
	}
}

func TestResolveAndMaterializeTaskRuntimeRefAllowedToolsBuiltInNoOp(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "analysis", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: &contract,
		}},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "generated", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: agent.Name}},
	}
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent).Build()

	if err := ResolveAndMaterializeTaskRuntimeRefAllowedTools(context.Background(), reader, task); err != nil {
		t.Fatal(err)
	}
	if task.Spec.AgentRuntime != nil {
		t.Fatalf("agentRuntime = %#v, want nil for built-in runtime", task.Spec.AgentRuntime)
	}
}

func TestResolveAndMaterializeTaskRuntimeRefAllowedToolsFailsClosed(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	for _, tt := range []struct {
		name        string
		runtime     *corev1alpha1.AgentRuntime
		wantMessage string
	}{
		{name: "missing AgentRuntime", wantMessage: "not found"},
		{
			name: "missing MCP policy",
			runtime: &corev1alpha1.AgentRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: "default"},
				Spec: corev1alpha1.AgentRuntimeRegistrySpec{
					ContractVersion: &contract,
					Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
						Profile: &corev1alpha1.AgentRuntimeProfileSpec{
							ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
						},
					},
				},
			},
			wantMessage: "missing capabilities.mcpPolicy",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "analysis", Namespace: "default"},
				Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
					RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
				}},
			}
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "generated", Namespace: "default"},
				Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: agent.Name}},
			}
			scheme := runtime.NewScheme()
			if err := corev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent)
			if tt.runtime != nil {
				builder = builder.WithObjects(tt.runtime)
			}
			reader := builder.Build()

			err := ResolveAndMaterializeTaskRuntimeRefAllowedTools(context.Background(), reader, task)
			if err == nil || !strings.Contains(err.Error(), tt.wantMessage) {
				t.Fatalf("error = %v, want message containing %q", err, tt.wantMessage)
			}
		})
	}
}
