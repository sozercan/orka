/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
)

func TestValidateChildTaskAgainstParentTransactionUsesAllowedAgentsForDelegation(t *testing.T) {
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"agent":         "coordinator",
		"allowedAgents": `["coordinator","researcher"]`,
	}
	child := childTaskForResearcherAgent()

	if err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(researcherAgent()), parent, child, testResearcherAgentName); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionRejectsDisallowedAgent(t *testing.T) {
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"agent":         "coordinator",
		"allowedAgents": `["coordinator"]`,
	}
	child := childTaskForResearcherAgent()

	err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(researcherAgent()), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), "is not allowed by transaction context") {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want allowedAgents denial", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionAuthorizesAllWorkspaceCredentialRoles(t *testing.T) {
	firstName := "workspace-a"
	secondName := "workspace-b"
	tests := []struct {
		name string
		role string
		set  func(*corev1alpha1.WorkspaceConfig)
	}{
		{name: "source read", role: "source-read", set: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.ReadCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: secondName}
		}},
		{name: "target read", role: "target-read", set: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.PublicationReadCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: secondName}
		}},
		{name: "target write", role: "target-write", set: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.PublicationCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: secondName}
		}},
		{name: "forge", role: "forge", set: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.ForgeCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: secondName}
		}},
	}
	for _, test := range tests {
		t.Run(test.name+" requires scope", func(t *testing.T) {
			parent := parentTask()
			parent.Spec.Transaction.Scope = ""
			parent.Spec.Transaction.Scopes = nil
			parent.Spec.Transaction.Context = map[string]string{}
			child := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: defaultNamespace},
				Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer, Workspace: &corev1alpha1.WorkspaceConfig{}},
			}
			test.set(child.Spec.Workspace)

			err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(), parent, child, "")
			if err == nil || !strings.Contains(err.Error(), "child task "+test.role+" credential") || !strings.Contains(err.Error(), "requires transaction scope") {
				t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want %s scope denial", err, test.role)
			}
		})

		t.Run(test.name+" enforces secret subset", func(t *testing.T) {
			parent := parentTask()
			parent.Spec.Transaction.Scopes = []string{"orka:secrets:credentials:read"}
			parent.Spec.Transaction.Context = map[string]string{"secret": firstName}
			child := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: defaultNamespace},
				Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer, Workspace: &corev1alpha1.WorkspaceConfig{}},
			}
			test.set(child.Spec.Workspace)

			err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(), parent, child, "")
			if err == nil || !strings.Contains(err.Error(), "child task "+test.role+" credential") || !strings.Contains(err.Error(), "does not match transaction context") {
				t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want %s subset denial", err, test.role)
			}
		})
	}
}

func TestValidateChildTaskAgainstParentTransactionRejectsDisallowedProviderModelAndTool(t *testing.T) {
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":        defaultNamespace,
		"allowedAgents":    `["researcher"]`,
		"allowedProviders": `["approved-provider"]`,
		"allowedModels":    `["approved-provider/approved-model"]`,
		"allowedTools":     `["file_read"]`,
	}
	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "disallowed-provider", Namespace: defaultNamespace},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeOpenAI,
			DefaultModel: "disallowed-model",
		},
	}
	agent := researcherAgent()
	agent.Spec.ProviderRef = &corev1alpha1.ProviderReference{Name: provider.Name}
	agent.Spec.Tools = []corev1alpha1.ToolReference{{Name: "web_search"}}
	child := childTaskForResearcherAgent()

	err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(provider, agent), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), "provider") {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want provider denial", err)
	}

	parent.Spec.Transaction.Context["allowedProviders"] = `["disallowed-provider"]`
	parent.Spec.Transaction.Context["allowedModels"] = `["disallowed-provider/disallowed-model"]`
	err = validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(provider, agent), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), `tool "web_search"`) {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want tool denial", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionRejectsCrossNamespaceDependencies(t *testing.T) {
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{"namespace": defaultNamespace}

	crossAgent := researcherAgent()
	crossAgent.Namespace = "other-namespace"
	child := childTaskForResearcherAgent()
	child.Spec.AgentRef = &corev1alpha1.AgentReference{Name: testResearcherAgentName, Namespace: "other-namespace"}
	err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(crossAgent), parent, child, "")
	if err == nil || !strings.Contains(err.Error(), `agent namespace "other-namespace" does not match transaction context`) {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want cross-namespace agent denial", err)
	}

	crossProviderAgent := researcherAgent()
	crossProviderAgent.Spec.ProviderRef = &corev1alpha1.ProviderReference{Name: "remote-provider", Namespace: "other-namespace"}
	child = childTaskForResearcherAgent()
	err = validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(crossProviderAgent), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), `provider namespace "other-namespace" does not match transaction context`) {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want cross-namespace provider denial", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionRejectsRefOverridingBranchConstraint(t *testing.T) {
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace": defaultNamespace,
		"branch":    "main",
	}
	agent := researcherAgent()
	child := childTaskForResearcherAgent()
	child.Spec.Workspace = &corev1alpha1.WorkspaceConfig{
		GitRepo: "https://github.com/example/repo",
		Branch:  "main",
		Ref:     "refs/heads/attacker-selected",
	}

	err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), "overrides the branch constrained by transaction context") {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want ref-overrides-branch denial", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionRejectsProviderlessChildUnderProviderConstraints(t *testing.T) {
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":        defaultNamespace,
		"allowedProviders": `["approved-provider"]`,
	}
	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: defaultNamespace},
		Spec: corev1alpha1.TaskSpec{
			Type:  corev1alpha1.TaskTypeContainer,
			Image: "alpine:3.20",
		},
	}

	err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(), parent, child, "")
	if err == nil || !strings.Contains(err.Error(), "provider") {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want provider denial", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionDerivesOpenCodeProviderFromModelName(t *testing.T) {
	const model = "openrouter/anthropic/claude-sonnet-4"
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":        defaultNamespace,
		"allowedAgents":    `["researcher"]`,
		"allowedProviders": `["openrouter"]`,
		"allowedModels":    `["` + model + `"]`,
	}
	agent := researcherAgent()
	agent.Spec.Model = &corev1alpha1.ModelConfig{Name: model}
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeOpencode}
	overrideProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "override-provider", Namespace: defaultNamespace},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeAnthropic,
			SecretRef:    corev1alpha1.ProviderSecretRef{Name: "override-secret"},
			DefaultModel: "claude-sonnet-4",
		},
	}
	agent.Spec.Model.Provider = string(corev1alpha1.ProviderTypeAnthropic)
	agent.Spec.ProviderRef = &corev1alpha1.ProviderReference{Name: overrideProvider.Name}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent
	child.Spec.AI = &corev1alpha1.AISpec{
		ProviderRef: &corev1alpha1.ProviderReference{Name: overrideProvider.Name},
		Provider:    string(corev1alpha1.ProviderTypeAnthropic),
		Model:       "claude-sonnet-4",
	}

	if err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent, overrideProvider), parent, child, testResearcherAgentName); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() rejected provider-qualified OpenCode model: %v", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionRejectsUnrestrictedAgentRuntimeTools(t *testing.T) {
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"allowedAgents": `["researcher"]`,
		"allowedTools":  `["Read"]`,
	}
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent

	err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), "agent runtime tools are unrestricted") {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want unrestricted runtime tools denial", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionRejectsBlankAgentRuntimeTools(t *testing.T) {
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"allowedAgents": `["researcher"]`,
		"allowedTools":  `["Read"]`,
	}
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		Type:                corev1alpha1.AgentRuntimeCodex,
		DefaultAllowedTools: []string{" "},
	}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent

	err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), "agent runtime tools are unrestricted") {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want unrestricted runtime tools denial", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionAcceptsExplicitAllowlistWithoutBash(t *testing.T) {
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"allowedAgents": `["researcher"]`,
		"allowedTools":  `["Read"]`,
	}
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		Type:                corev1alpha1.AgentRuntimeCodex,
		DefaultAllowedTools: []string{"Read"},
	}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent

	if err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent), parent, child, testResearcherAgentName); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want explicit Read-only subset accepted", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionRequiresInjectedChildMessagingTools(t *testing.T) {
	allowBash := false
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"allowedAgents": `["researcher"]`,
		"allowedTools":  `["Read"]`,
	}
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		Type:                corev1alpha1.AgentRuntimeCodex,
		DefaultAllowedTools: []string{"Read"},
		DefaultAllowBash:    &allowBash,
	}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent
	child.Labels = map[string]string{labels.LabelParentTask: labels.SelectorValue(parent.Name)}

	err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), `tool "send_message"`) {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want injected messaging tool denial", err)
	}

	parent.Spec.Transaction.Context["allowedTools"] = `["Read","send_message","check_messages"]`
	if err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent), parent, child, testResearcherAgentName); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() rejected allowed injected messaging tools: %v", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionAcceptsRuntimeRefDenyAllWithoutImplicitTools(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"allowedAgents": `["researcher"]`,
		"allowedTools":  `[]`,
	}
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
	}
	agent.Spec.Coordination = &corev1alpha1.CoordinationConfig{Enabled: true}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent
	child.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{}}
	child.Labels = map[string]string{labels.LabelParentTask: labels.SelectorValue(parent.Name)}
	runtime := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{ProviderKind: "codex", Model: "gpt-5.6"},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:          []string{},
					DisallowedTools:       []string{},
					ApprovalRequiredTools: []string{},
				},
			},
		},
	}

	if err := validateChildTaskAgainstParentTransaction(
		context.Background(), newFakeClient(agent, runtime), parent, child, testResearcherAgentName,
	); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() rejected authoritative runtimeRef deny-all policy: %v", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionUsesRuntimeRefProfile(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":        defaultNamespace,
		"allowedAgents":    `["researcher"]`,
		"allowedProviders": `["codex"]`,
		"allowedModels":    `["codex/gpt-5.6"]`,
		"allowedTools":     `["Read"]`,
	}
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
	}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent
	child.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{"Read"}}
	runtime := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{ProviderKind: "codex", Model: "gpt-5.6"},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:          []string{"Read"},
					DisallowedTools:       []string{},
					ApprovalRequiredTools: []string{},
				},
			},
		},
	}

	if err := validateChildTaskAgainstParentTransaction(
		context.Background(), newFakeClient(agent, runtime), parent, child, testResearcherAgentName,
	); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() rejected registered runtime profile: %v", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionUsesEffectiveRuntimeRefTools(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	for _, test := range []struct {
		name       string
		allowed    []string
		disallowed []string
		allowBash  bool
	}{
		{
			name:    "deny rule removes an allowed tool",
			allowed: []string{"Read", "Write"}, disallowed: []string{"Write"}, allowBash: true,
		},
		{
			name:    "bash gate removes an allowed Bash tool",
			allowed: []string{"Bash", "Read"}, disallowed: []string{}, allowBash: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			parent := parentTask()
			parent.Spec.Transaction.Context = map[string]string{
				"namespace":     defaultNamespace,
				"allowedAgents": `["researcher"]`,
				"allowedTools":  `["Read"]`,
			}
			agent := researcherAgent()
			agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
				RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
			}
			child := childTaskForResearcherAgent()
			child.Spec.Type = corev1alpha1.TaskTypeAgent
			child.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: append([]string{}, test.allowed...)}
			externalRuntime := &corev1alpha1.AgentRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: defaultNamespace},
				Spec: corev1alpha1.AgentRuntimeRegistrySpec{
					ContractVersion: &contract,
					Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
						Profile: &corev1alpha1.AgentRuntimeProfileSpec{ProviderKind: "codex", Model: "gpt-5.6"},
						MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
							AllowedTools:          append([]string{}, test.allowed...),
							DisallowedTools:       append([]string{}, test.disallowed...),
							AllowBash:             test.allowBash,
							ApprovalRequiredTools: []string{},
						},
					},
				},
			}

			if err := validateChildTaskAgainstParentTransaction(
				context.Background(), newFakeClient(agent, externalRuntime), parent, child, testResearcherAgentName,
			); err != nil {
				t.Fatalf("validateChildTaskAgainstParentTransaction() rejected effective runtimeRef tool subset: %v", err)
			}
		})
	}
}

func TestValidateChildTaskAgainstParentTransactionIgnoresDeniedRuntimeRefToolCredentials(t *testing.T) {
	const deniedToolName = "credentialed-tool"
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"allowedAgents": `["researcher"]`,
		"allowedTools":  `["Read","credentialed-tool"]`,
	}
	parent.Spec.Transaction.Scope = ""
	parent.Spec.Transaction.Scopes = nil
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
	}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent
	child.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{"Read", deniedToolName}}
	externalRuntime := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{ProviderKind: "codex", Model: "gpt-5.6"},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:          []string{"Read", deniedToolName},
					DisallowedTools:       []string{deniedToolName},
					AllowBash:             true,
					ApprovalRequiredTools: []string{},
				},
			},
		},
	}
	deniedTool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: deniedToolName, Namespace: defaultNamespace},
		Spec: corev1alpha1.ToolSpec{HTTP: &corev1alpha1.HTTPExecution{
			AuthSecretRef: &corev1alpha1.SecretKeySelector{Name: "tool-credentials", Key: "token"},
		}},
	}

	if err := validateChildTaskAgainstParentTransaction(
		context.Background(), newFakeClient(agent, externalRuntime, deniedTool), parent, child, testResearcherAgentName,
	); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() enforced credentials for denied runtimeRef tool: %v", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionRejectsRuntimeRefWithoutProfile(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{}
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
	}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent
	child.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{"Read"}}
	runtime := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
				AllowedTools:          []string{"Read"},
				DisallowedTools:       []string{},
				ApprovalRequiredTools: []string{},
			}},
		},
	}

	err := validateChildTaskAgainstParentTransaction(
		context.Background(), newFakeClient(agent, runtime), parent, child, testResearcherAgentName,
	)
	if err == nil || !strings.Contains(err.Error(), "missing capabilities.profile") {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want missing profile denial", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionRejectsRuntimeRefPolicyMismatch(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{}
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
	}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent
	child.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{}}
	runtime := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{ProviderKind: "codex", Model: "gpt-5.6"},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:          []string{"Read"},
					DisallowedTools:       []string{},
					ApprovalRequiredTools: []string{},
				},
			},
		},
	}

	err := validateChildTaskAgainstParentTransaction(
		context.Background(), newFakeClient(agent, runtime), parent, child, testResearcherAgentName,
	)
	if err == nil || !strings.Contains(err.Error(), "allowedTools do not exactly match") {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want runtime policy mismatch denial", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionDoesNotInjectMessagingIntoContainerChildren(t *testing.T) {
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":    defaultNamespace,
		"allowedTools": `[]`,
	}
	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "container-child",
			Namespace: defaultNamespace,
			Labels:    map[string]string{labels.LabelParentTask: labels.SelectorValue(parent.Name)},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}

	if err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(), parent, child, ""); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() injected messaging into container child: %v", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionRejectsInjectedMessagingForOpenCodeDenyAll(t *testing.T) {
	allowBash := false
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"allowedAgents": `["researcher"]`,
		"allowedTools":  `[]`,
	}
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		Type:                corev1alpha1.AgentRuntimeOpencode,
		DefaultAllowedTools: []string{},
		DefaultAllowBash:    &allowBash,
	}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent
	child.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{}, AllowBash: &allowBash}
	child.Labels = map[string]string{labels.LabelParentTask: labels.SelectorValue(parent.Name)}

	err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), `tool "send_message"`) {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want explicit deny-all to reject injected messaging tools", err)
	}

	parent.Spec.Transaction.Context["allowedTools"] = `["send_message","check_messages"]`
	if err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent), parent, child, testResearcherAgentName); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() rejected authorized injected messaging tools: %v", err)
	}

	parent.Spec.Transaction.Context["allowedTools"] = `[]`
	child.Annotations = map[string]string{labels.AnnotationDisableCoordinationToolInject: trueStr}
	if err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent), parent, child, testResearcherAgentName); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() rejected deny-all with messaging injection disabled: %v", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionAuthorizesCustomToolHTTPSecret(t *testing.T) {
	const (
		toolName   = "custom-search"
		secretName = "custom-search-credentials"
	)
	parent, agent, child := delegatedOpenCodeCustomToolTransaction(toolName)
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: toolName, Namespace: defaultNamespace},
		Spec: corev1alpha1.ToolSpec{HTTP: &corev1alpha1.HTTPExecution{
			AuthSecretRef: &corev1alpha1.SecretKeySelector{Name: secretName, Key: "token"},
		}},
	}

	parent.Spec.Transaction.Scope = ""
	parent.Spec.Transaction.Scopes = nil
	err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent, tool), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), `requires transaction scope "orka:secrets:credentials:read"`) {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want custom Tool credential scope denial", err)
	}

	parent.Spec.Transaction.Scopes = []string{"orka:secrets:credentials:read"}
	parent.Spec.Transaction.Context["secret"] = "different-secret"
	err = validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent, tool), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), secretName) || !strings.Contains(err.Error(), "does not match transaction context") {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want custom Tool secret binding denial", err)
	}

	parent.Spec.Transaction.Context["secret"] = secretName
	if err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent, tool), parent, child, testResearcherAgentName); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() rejected authorized custom Tool credential: %v", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionAuthorizesCustomToolOutboundCredentials(t *testing.T) {
	const (
		toolName   = "custom-search"
		policyName = "resource-api"
		secretName = "resource-assertion"
	)
	parent, agent, child := delegatedOpenCodeCustomToolTransaction(toolName)
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: toolName, Namespace: defaultNamespace},
		Spec: corev1alpha1.ToolSpec{HTTP: &corev1alpha1.HTTPExecution{
			OutboundAccessPolicyRef: &corev1alpha1.LocalObjectReference{Name: policyName},
		}},
	}
	policy := readyChildTransactionOutboundPolicy(policyName, corev1alpha1.OutboundAccessPolicySpec{
		Direct: &corev1alpha1.DirectOutboundAccess{
			Subject: corev1alpha1.OutboundTokenSource{
				Source:    corev1alpha1.OutboundTokenSourceSecretRef,
				TokenType: "urn:example:assertion",
				SecretRef: &corev1alpha1.NamespacedSecretKeySelector{Name: secretName, Key: "token"},
			},
			TokenEndpoint:           corev1alpha1.OutboundTokenEndpoint{URL: "https://identity.example.test/token"},
			ExpectedIssuedTokenType: "urn:example:resource",
		},
	})

	parent.Spec.Transaction.Scope = ""
	parent.Spec.Transaction.Scopes = nil
	err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent, tool, policy), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), `requires transaction scope "orka:secrets:credentials:read"`) {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want outbound credential scope denial", err)
	}

	parent.Spec.Transaction.Scopes = []string{"orka:secrets:credentials:read"}
	parent.Spec.Transaction.Context["secret"] = "different-secret"
	err = validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent, tool, policy), parent, child, testResearcherAgentName)
	if err == nil || !strings.Contains(err.Error(), secretName) || !strings.Contains(err.Error(), "does not match transaction context") {
		t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want outbound secret binding denial", err)
	}

	parent.Spec.Transaction.Context["secret"] = secretName
	if err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent, tool, policy), parent, child, testResearcherAgentName); err != nil {
		t.Fatalf("validateChildTaskAgainstParentTransaction() rejected authorized outbound credential: %v", err)
	}
}

func TestValidateChildTaskAgainstParentTransactionFailsClosedOnCustomToolDependencies(t *testing.T) {
	const (
		toolName   = "custom-search"
		policyName = "resource-api"
	)
	parent, agent, child := delegatedOpenCodeCustomToolTransaction(toolName)

	t.Run("unresolved Tool", func(t *testing.T) {
		err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent), parent, child, testResearcherAgentName)
		if err == nil || !strings.Contains(err.Error(), `tool "custom-search" is unresolved`) {
			t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want unresolved Tool denial", err)
		}
	})

	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: toolName, Namespace: defaultNamespace},
		Spec: corev1alpha1.ToolSpec{HTTP: &corev1alpha1.HTTPExecution{
			OutboundAccessPolicyRef: &corev1alpha1.LocalObjectReference{Name: policyName},
		}},
	}
	t.Run("unresolved OutboundAccessPolicy", func(t *testing.T) {
		err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent, tool), parent, child, testResearcherAgentName)
		if err == nil || !strings.Contains(err.Error(), `references unresolved OutboundAccessPolicy "resource-api"`) {
			t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want unresolved policy denial", err)
		}
	})

	policy := readyChildTransactionOutboundPolicy(policyName, corev1alpha1.OutboundAccessPolicySpec{
		Direct: &corev1alpha1.DirectOutboundAccess{
			Subject:       corev1alpha1.OutboundTokenSource{Source: corev1alpha1.OutboundTokenSourceTransactionToken},
			TokenEndpoint: corev1alpha1.OutboundTokenEndpoint{URL: "https://identity.example.test/token"},
		},
	})
	policy.Status.Conditions[1].Status = metav1.ConditionFalse
	t.Run("not-ready OutboundAccessPolicy", func(t *testing.T) {
		err := validateChildTaskAgainstParentTransaction(context.Background(), newFakeClient(agent, tool, policy), parent, child, testResearcherAgentName)
		if err == nil || !strings.Contains(err.Error(), `references not-ready OutboundAccessPolicy "resource-api"`) {
			t.Fatalf("validateChildTaskAgainstParentTransaction() error = %v, want not-ready policy denial", err)
		}
	})
}

func TestChildTransactionCredentialRequirementsCollectsOutboundCredentialSources(t *testing.T) {
	tests := []struct {
		name        string
		spec        corev1alpha1.OutboundAccessPolicySpec
		wantSecrets []string
	}{
		{
			name: "direct secret sources",
			spec: corev1alpha1.OutboundAccessPolicySpec{Direct: &corev1alpha1.DirectOutboundAccess{
				Subject: corev1alpha1.OutboundTokenSource{
					Source:    corev1alpha1.OutboundTokenSourceSecretRef,
					SecretRef: &corev1alpha1.NamespacedSecretKeySelector{Name: "subject-secret", Key: "token"},
				},
				Actor: &corev1alpha1.OutboundTokenSource{
					Source:    corev1alpha1.OutboundTokenSourceSecretRef,
					SecretRef: &corev1alpha1.NamespacedSecretKeySelector{Name: "actor-secret", Key: "token"},
				},
				TokenEndpoint: corev1alpha1.OutboundTokenEndpoint{TLS: &corev1alpha1.OutboundTLSConfig{
					CASecretRef: &corev1alpha1.NamespacedSecretKeySelector{Name: "token-ca", Key: "ca.crt"},
				}},
				ClientAuthentication: &corev1alpha1.OutboundClientAuthentication{
					ClientSecretRef: &corev1alpha1.NamespacedSecretKeySelector{Name: "client-secret", Key: "secret"},
					PrivateKeyRef:   &corev1alpha1.NamespacedSecretKeySelector{Name: "private-key", Key: "key.pem"},
				},
			}},
			wantSecrets: []string{"actor-secret", "client-secret", "private-key", "subject-secret", "token-ca"},
		},
		{
			name: "service account source",
			spec: corev1alpha1.OutboundAccessPolicySpec{Direct: &corev1alpha1.DirectOutboundAccess{
				Subject: corev1alpha1.OutboundTokenSource{Source: corev1alpha1.OutboundTokenSourceServiceAccount},
			}},
		},
		{
			name: "gateway CA",
			spec: corev1alpha1.OutboundAccessPolicySpec{Gateway: &corev1alpha1.GatewayOutboundAccess{
				TLS: &corev1alpha1.OutboundTLSConfig{CASecretRef: &corev1alpha1.NamespacedSecretKeySelector{
					Name: "gateway-ca", Key: "ca.crt",
				}},
			}},
			wantSecrets: []string{"gateway-ca"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requirements := childTransactionCredentialRequirements{}
			policy := readyChildTransactionOutboundPolicy("resource-api", test.spec)
			if err := requirements.addOutboundAccessPolicy(policy); err != nil {
				t.Fatalf("addOutboundAccessPolicy() error = %v", err)
			}
			if !requirements.requiresScope {
				t.Fatal("addOutboundAccessPolicy() did not require credential-read scope")
			}
			gotSecrets := make([]string, 0, len(requirements.secretNames))
			for name := range requirements.secretNames {
				gotSecrets = append(gotSecrets, name)
			}
			slices.Sort(gotSecrets)
			if !slices.Equal(gotSecrets, test.wantSecrets) {
				t.Fatalf("credential secrets = %#v, want %#v", gotSecrets, test.wantSecrets)
			}
		})
	}
}

func delegatedOpenCodeCustomToolTransaction(toolName string) (*corev1alpha1.Task, *corev1alpha1.Agent, *corev1alpha1.Task) {
	allowBash := false
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"allowedAgents": `["researcher"]`,
		"allowedTools":  `["` + toolName + `","send_message","check_messages"]`,
	}
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		Type:                corev1alpha1.AgentRuntimeOpencode,
		DefaultAllowedTools: []string{toolName},
		DefaultAllowBash:    &allowBash,
	}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent
	child.Labels = map[string]string{labels.LabelParentTask: labels.SelectorValue(parent.Name)}
	return parent, agent, child
}

func readyChildTransactionOutboundPolicy(name string, spec corev1alpha1.OutboundAccessPolicySpec) *corev1alpha1.OutboundAccessPolicy {
	const generation = int64(2)
	return &corev1alpha1.OutboundAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: defaultNamespace, Generation: generation},
		Spec:       spec,
		Status: corev1alpha1.OutboundAccessPolicyStatus{
			ObservedGeneration: generation,
			Conditions: []metav1.Condition{
				{Type: corev1alpha1.OutboundAccessPolicyConditionAccepted, Status: metav1.ConditionTrue, ObservedGeneration: generation},
				{Type: corev1alpha1.OutboundAccessPolicyConditionResolvedRefs, Status: metav1.ConditionTrue, ObservedGeneration: generation},
			},
		},
	}
}

func TestChildTransactionEffectiveAIToolsSkipsDisabledCoordinationInjection(t *testing.T) {
	agent := researcherAgent()
	agent.Spec.Coordination = &corev1alpha1.CoordinationConfig{Enabled: true}
	child := childTaskForResearcherAgent()
	child.Annotations = map[string]string{labels.AnnotationDisableCoordinationToolInject: "true"}
	child.Spec.Type = corev1alpha1.TaskTypeAI
	child.Spec.AI = &corev1alpha1.AISpec{
		Tools: []string{"list_pull_requests", "check_pr_review_marker"},
	}

	got := strings.Join(childTransactionEffectiveAITools(child, agent), ",")
	for _, tool := range []string{"list_pull_requests", "check_pr_review_marker"} {
		if !strings.Contains(got, tool) {
			t.Fatalf("expected explicit tool %q in %q", tool, got)
		}
	}
	for _, tool := range []string{"recall_memory", "remember", "propose_memory", "search_transcript"} {
		if !strings.Contains(got, tool) {
			t.Fatalf("expected memory tool %q in %q", tool, got)
		}
	}
	for _, tool := range []string{"delegate_task", "merge_pull_request", "auto_merge_pull_request"} {
		if strings.Contains(got, tool) {
			t.Fatalf("unexpected coordination tool %q in %q", tool, got)
		}
	}
}

func TestChildTransactionEffectiveAIToolsIncludesPRReviewCoordinationTools(t *testing.T) {
	agent := researcherAgent()
	agent.Spec.Coordination = &corev1alpha1.CoordinationConfig{Enabled: true}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAI

	got := strings.Join(childTransactionEffectiveAITools(child, agent), ",")
	for _, tool := range []string{"list_pull_requests", "check_pr_review_marker"} {
		if !strings.Contains(got, tool) {
			t.Fatalf("expected PR review coordination tool %q in %q", tool, got)
		}
	}
}

func TestChildTransactionEffectiveAIToolsSkipsRuntimeRefChildMessagingInjection(t *testing.T) {
	agent := researcherAgent()
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
	}
	child := childTaskForResearcherAgent()
	child.Spec.Type = corev1alpha1.TaskTypeAgent
	child.Labels = map[string]string{labels.LabelParentTask: "parent"}

	got := childTransactionEffectiveAITools(child, agent)
	for _, tool := range append(transactionCoordinationToolNames(), transactionMemoryToolNames()...) {
		if slices.Contains(got, tool) {
			t.Fatalf("childTransactionEffectiveAITools() = %#v, unexpectedly injected coordination tool %q for runtimeRef Agent", got, tool)
		}
	}
}

func childTaskForResearcherAgent() *corev1alpha1.Task {
	return &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: defaultNamespace},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{
				Name: testResearcherAgentName,
			},
		},
	}
}

func TestChildTransactionEffectiveRuntimePolicyNormalizesOpenCode(t *testing.T) {
	allowBash := false
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
		Type: corev1alpha1.AgentRuntimeOpencode,
	}}}
	child := &corev1alpha1.Task{}
	tools, bash := childTransactionEffectiveRuntimePolicy(child, agent)
	if want := []string{"glob", "read"}; !slices.Equal(tools, want) {
		t.Fatalf("read-intent tools = %#v, want %#v", tools, want)
	}
	if bash {
		t.Fatal("read-intent policy retained bash")
	}

	agent.Spec.Runtime.DefaultAllowedTools = []string{"Edit"}
	agent.Spec.Runtime.DefaultAllowBash = &allowBash
	child.Spec.Workspace = &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentWrite}
	tools, bash = childTransactionEffectiveRuntimePolicy(child, agent)
	if want := []string{"apply_patch", "edit", "write"}; !slices.Equal(tools, want) {
		t.Fatalf("write-intent tools = %#v, want %#v", tools, want)
	}
	if bash {
		t.Fatal("write-intent policy changed explicit bash denial")
	}

	child.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{DisallowedTools: []string{"write"}, AllowBash: &allowBash}
	tools, _ = childTransactionEffectiveRuntimePolicy(child, agent)
	if tools == nil || len(tools) != 0 {
		t.Fatalf("disallowed mutation alias tools = %#v, want non-nil empty", tools)
	}

	agent.Spec.Runtime.DefaultAllowedTools = []string{}
	child.Spec.AgentRuntime = nil
	tools, _ = childTransactionEffectiveRuntimePolicy(child, agent)
	if tools == nil || len(tools) != 0 {
		t.Fatalf("explicit empty tools = %#v, want non-nil empty", tools)
	}

	agent.Spec.Runtime.DefaultAllowedTools = nil
	child.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{}}
	tools, _ = childTransactionEffectiveRuntimePolicy(child, agent)
	if tools == nil || len(tools) != 0 {
		t.Fatalf("explicit empty task override = %#v, want non-nil empty", tools)
	}
}

func TestChildTransactionEffectiveRuntimePolicyNormalizesBuiltInNarrowing(t *testing.T) {
	allowBash := false
	runtimes := []corev1alpha1.AgentRuntimeType{
		corev1alpha1.AgentRuntimeClaude,
		corev1alpha1.AgentRuntimeCodex,
		corev1alpha1.AgentRuntimeCopilot,
	}
	for _, runtimeType := range runtimes {
		t.Run(string(runtimeType), func(t *testing.T) {
			agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{Type: runtimeType}}}

			t.Run("unrestricted remains nil", func(t *testing.T) {
				tools, bash := childTransactionEffectiveRuntimePolicy(&corev1alpha1.Task{}, agent)
				if tools != nil || !bash {
					t.Fatalf("unrestricted policy = %#v allowBash=%v, want nil allowBash=true", tools, bash)
				}
			})

			t.Run("explicit empty remains deny all", func(t *testing.T) {
				child := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
					Type:         corev1alpha1.TaskTypeAgent,
					AgentRuntime: &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{}, AllowBash: &allowBash},
				}}
				tools, bash := childTransactionEffectiveRuntimePolicy(child, agent)
				if tools == nil || len(tools) != 0 || bash {
					t.Fatalf("explicit deny-all policy = %#v allowBash=%v, want non-nil empty allowBash=false", tools, bash)
				}
				if err := validateChildToolConstraints(map[string]string{"allowedTools": `[]`}, childTransactionContext{
					childType: child.Spec.Type, agent: agent, runtimeTools: tools, runtimeBash: bash,
				}); err != nil {
					t.Fatalf("explicit deny-all subset rejected: %v", err)
				}
			})

			t.Run("explicit allowlist without bash closes effective bash", func(t *testing.T) {
				explicitAgent := agent.DeepCopy()
				explicitAgent.Spec.Runtime.DefaultAllowedTools = []string{"Read"}
				tools, bash := childTransactionEffectiveRuntimePolicy(&corev1alpha1.Task{}, explicitAgent)
				if !slices.Equal(tools, []string{"Read"}) || bash {
					t.Fatalf("explicit read-only policy = %#v allowBash=%v, want Read with allowBash=false", tools, bash)
				}
			})

			t.Run("deny only expands implicit native tools", func(t *testing.T) {
				child := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
					Type:         corev1alpha1.TaskTypeAgent,
					AgentRuntime: &corev1alpha1.AgentRuntimeSpec{DisallowedTools: []string{"Write"}},
				}}
				tools, bash := childTransactionEffectiveRuntimePolicy(child, agent)
				want := []string{"Bash", "Edit", "Glob", "Grep", "Read", "WebFetch", "WebSearch"}
				if !slices.Equal(tools, want) || !bash {
					t.Fatalf("deny-only policy = %#v allowBash=%v, want %#v allowBash=true", tools, bash, want)
				}
				if err := validateChildToolConstraints(map[string]string{
					"allowedTools": `["Bash","Edit","Glob","Grep","Read","WebFetch","WebSearch"]`,
				}, childTransactionContext{
					childType: child.Spec.Type, agent: agent, runtimeTools: tools, runtimeBash: bash,
				}); err != nil {
					t.Fatalf("deny-only subset rejected: %v", err)
				}
			})

			t.Run("bash disabled expands implicit native tools", func(t *testing.T) {
				child := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
					Type:         corev1alpha1.TaskTypeAgent,
					AgentRuntime: &corev1alpha1.AgentRuntimeSpec{AllowBash: &allowBash},
				}}
				tools, bash := childTransactionEffectiveRuntimePolicy(child, agent)
				want := []string{"Edit", "Glob", "Grep", "Read", "WebFetch", "WebSearch", "Write"}
				if !slices.Equal(tools, want) || bash {
					t.Fatalf("bash-disabled policy = %#v allowBash=%v, want %#v allowBash=false", tools, bash, want)
				}
				if err := validateChildToolConstraints(map[string]string{
					"allowedTools": `["Edit","Glob","Grep","Read","WebFetch","WebSearch","Write"]`,
				}, childTransactionContext{
					childType: child.Spec.Type, agent: agent, runtimeTools: tools, runtimeBash: bash,
				}); err != nil {
					t.Fatalf("bash-disabled subset rejected: %v", err)
				}
			})

			t.Run("disallowed bash is removed before subset checks", func(t *testing.T) {
				child := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
					Type:         corev1alpha1.TaskTypeAgent,
					AgentRuntime: &corev1alpha1.AgentRuntimeSpec{DisallowedTools: []string{"bAsH"}},
				}}
				tools, bash := childTransactionEffectiveRuntimePolicy(child, agent)
				want := []string{"Edit", "Glob", "Grep", "Read", "WebFetch", "WebSearch", "Write"}
				if !slices.Equal(tools, want) || bash {
					t.Fatalf("bash-denied policy = %#v allowBash=%v, want %#v allowBash=false", tools, bash, want)
				}
				if err := validateChildToolConstraints(map[string]string{
					"allowedTools": `["Edit","Glob","Grep","Read","WebFetch","WebSearch","Write"]`,
				}, childTransactionContext{
					childType: child.Spec.Type, agent: agent, runtimeTools: tools, runtimeBash: bash,
				}); err != nil {
					t.Fatalf("bash-denied subset rejected: %v", err)
				}
			})
		})
	}
}

func TestValidateChildToolConstraintsAcceptsOpenCodeDenyAll(t *testing.T) {
	allowBash := false
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
		Type: corev1alpha1.AgentRuntimeOpencode, DefaultAllowedTools: []string{}, DefaultAllowBash: &allowBash,
	}}}
	err := validateChildToolConstraints(map[string]string{"allowedTools": "[]"}, childTransactionContext{
		childType: corev1alpha1.TaskTypeAgent, agent: agent, runtimeTools: []string{}, runtimeBash: false,
	})
	if err != nil {
		t.Fatalf("deny-all OpenCode policy rejected: %v", err)
	}
}

func TestValidateChildToolConstraintsNormalizesOpenCodePublicAllowlist(t *testing.T) {
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
		Type: corev1alpha1.AgentRuntimeOpencode,
	}}}
	err := validateChildToolConstraints(map[string]string{
		"allowedTools": `["Read","Edit","Bash"]`,
	}, childTransactionContext{
		childType:    corev1alpha1.TaskTypeAgent,
		agent:        agent,
		runtimeTools: []string{"apply_patch", "bash", "edit", "read", "write"},
		runtimeBash:  true,
	})
	if err != nil {
		t.Fatalf("public OpenCode allowlist rejected: %v", err)
	}
}

func TestValidateChildToolConstraintsAcceptsGenericDenyAll(t *testing.T) {
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
		Type: corev1alpha1.AgentRuntimeCodex, DefaultAllowedTools: []string{},
	}}}
	err := validateChildToolConstraints(map[string]string{"allowedTools": "[]"}, childTransactionContext{
		childType: corev1alpha1.TaskTypeAgent, agent: agent, runtimeTools: []string{}, runtimeBash: false,
	})
	if err != nil {
		t.Fatalf("deny-all generic runtime policy rejected: %v", err)
	}
}
