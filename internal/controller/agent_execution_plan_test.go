/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxextv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

func TestPlanAgentExecutionMatrix(t *testing.T) {
	scheme := newTestScheme()
	baseAgent := validPlannerAgent()

	tests := []struct {
		name                        string
		mutateTask                  func(*corev1alpha1.Task)
		mutateAgent                 func(*corev1alpha1.Agent)
		objects                     []client.Object
		agentSandboxEnabled         bool
		substrateEnabled            bool
		acpRuntimeEnabled           bool
		acpWorkspaceDispatchEnabled bool
		harnessV1Enabled            bool
		wantPath                    agentExecutionPath
		wantReason                  string
		wantWorkspaceStatusErr      string
	}{
		{
			name:              "built-in agent task uses ACP RuntimePool",
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathACP,
		},
		{
			name: "built-in Copilot task uses ACP RuntimePool",
			mutateAgent: func(agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.Type = corev1alpha1.AgentRuntimeCopilot
			},
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathACP,
		},
		{
			name:       "disabled ACP runtime fails closed without legacy fallback",
			wantPath:   agentExecutionPathRejected,
			wantReason: "no fallback execution path",
		},
		{
			name: "conformant external runtimeRef remains fail-closed",
			mutateAgent: func(agent *corev1alpha1.Agent) {
				agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
					RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-v2"},
				}
			},
			objects:           []client.Object{plannerExternalRuntime()},
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathRejected,
			wantReason:        "Task dispatch is not supported until the v2 dispatcher is wired",
		},
		{
			name: "OpenCode uses ACP RuntimePool",
			mutateAgent: func(agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.Type = corev1alpha1.AgentRuntimeOpencode
			},
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathACP,
		},
		{
			name: "built-in agent without contractVersion is rejected as unclassified",
			mutateAgent: func(agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.ContractVersion = nil
			},
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathRejected,
			wantReason:        "unclassified",
		},
		{
			name: "built-in agent classified orka.harness.v1 is rejected",
			mutateAgent: func(agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.ContractVersion = new(corev1alpha1.AgentRuntimeContractHarnessV1)
			},
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathRejected,
			wantReason:        "orka.harness.v1",
		},
		{
			name: "priorTaskRef continuation is rejected",
			mutateTask: func(task *corev1alpha1.Task) {
				task.Spec.PriorTaskRef = &corev1alpha1.PriorTaskReference{Name: "parent"}
			},
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathRejected,
			wantReason:        "use sessionRef",
		},
		{
			name: "transaction token delegation is rejected before ACP admission",
			mutateTask: func(task *corev1alpha1.Task) {
				task.Spec.Transaction = &corev1alpha1.TaskTransaction{ID: "txn-1"}
			},
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathRejected,
			wantReason:        "transaction token delegation",
		},
		{
			name: "task resources are rejected until a RuntimePool class is selected",
			mutateTask: func(task *corev1alpha1.Task) {
				task.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
			},
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathRejected,
			wantReason:        "custom Kubernetes resources",
		},
		{
			name: "agent resources are rejected until a RuntimePool class is selected",
			mutateAgent: func(agent *corev1alpha1.Agent) {
				agent.Spec.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}
			},
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathRejected,
			wantReason:        "custom Kubernetes resources",
		},
		{
			name: "task execution placement is rejected before ACP admission",
			mutateTask: func(task *corev1alpha1.Task) {
				task.Spec.Execution = &corev1alpha1.ExecutionSpec{RuntimeClassName: "kata"}
			},
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathRejected,
			wantReason:        "execution placement",
		},
		{
			name: "agent execution placement is rejected before ACP admission",
			mutateAgent: func(agent *corev1alpha1.Agent) {
				agent.Spec.Execution = &corev1alpha1.ExecutionSpec{NodeSelector: map[string]string{"disk": "ssd"}}
			},
			acpRuntimeEnabled: true,
			wantPath:          agentExecutionPathRejected,
			wantReason:        "execution placement",
		},
		{
			name:                        "workspace-backed agent task uses ACP RuntimePool when dispatch is enabled",
			mutateTask:                  plannerWorkspaceTask(nil),
			agentSandboxEnabled:         true,
			acpRuntimeEnabled:           true,
			acpWorkspaceDispatchEnabled: true,
			wantPath:                    agentExecutionPathACP,
		},
		{
			name:                   "workspace-backed dispatch disabled fails closed",
			mutateTask:             plannerWorkspaceTask(nil),
			agentSandboxEnabled:    true,
			acpRuntimeEnabled:      true,
			wantPath:               agentExecutionPathRejected,
			wantReason:             "acp-workspace-dispatch-enabled",
			wantWorkspaceStatusErr: "acp-workspace-dispatch-enabled",
		},
		{
			name:                        "workspace-backed agent task fails closed when agent-sandbox is disabled",
			mutateTask:                  plannerWorkspaceTask(nil),
			acpRuntimeEnabled:           true,
			acpWorkspaceDispatchEnabled: true,
			wantPath:                    agentExecutionPathRejected,
			wantReason:                  "agent-sandbox-enabled",
			wantWorkspaceStatusErr:      "agent-sandbox-enabled",
		},
		{
			name: "workspace templateRef is rejected for ACP RuntimeSessions",
			mutateTask: plannerWorkspaceTask(func(workspace *corev1alpha1.ExecutionWorkspaceSpec) {
				workspace.TemplateRef = &corev1alpha1.WorkspaceTemplateReference{Name: runtimePoolSandboxTemplateSuffix}
			}),
			objects: []client.Object{
				&sandboxextv1beta1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Name: runtimePoolSandboxTemplateSuffix, Namespace: defaultNS}},
				&sandboxextv1beta1.SandboxWarmPool{ObjectMeta: metav1.ObjectMeta{Name: runtimePoolSandboxTemplateSuffix, Namespace: defaultNS}},
			},
			agentSandboxEnabled:         true,
			acpRuntimeEnabled:           true,
			acpWorkspaceDispatchEnabled: true,
			wantPath:                    agentExecutionPathRejected,
			wantReason:                  acpWorkspaceTestTemplateRefForbiddenError,
			wantWorkspaceStatusErr:      acpWorkspaceTestTemplateRefForbiddenError,
		},
		{
			name: "substrate execution workspace without templateRef fails closed before any demand",
			mutateTask: plannerWorkspaceTask(func(workspace *corev1alpha1.ExecutionWorkspaceSpec) {
				workspace.Provider = corev1alpha1.WorkspaceProviderSubstrate
			}),
			substrateEnabled:            true,
			acpRuntimeEnabled:           true,
			acpWorkspaceDispatchEnabled: true,
			wantPath:                    agentExecutionPathRejected,
			wantReason:                  acpWorkspaceTestTemplateRefRequiredError,
			wantWorkspaceStatusErr:      acpWorkspaceTestTemplateRefRequiredError,
		},
		{
			name: "substrate-backed agent task uses ACP RuntimePool when dispatch is enabled",
			mutateTask: plannerWorkspaceTask(func(workspace *corev1alpha1.ExecutionWorkspaceSpec) {
				workspace.Provider = corev1alpha1.WorkspaceProviderSubstrate
				workspace.TemplateRef = &corev1alpha1.WorkspaceTemplateReference{Name: substrateTestBaseTemplateName, Namespace: substrateTestTemplateNamespace}
			}),
			substrateEnabled:            true,
			acpRuntimeEnabled:           true,
			acpWorkspaceDispatchEnabled: true,
			wantPath:                    agentExecutionPathACP,
		},
		{
			name: "substrate-backed agent task fails closed when substrate is disabled",
			mutateTask: plannerWorkspaceTask(func(workspace *corev1alpha1.ExecutionWorkspaceSpec) {
				workspace.Provider = corev1alpha1.WorkspaceProviderSubstrate
				workspace.TemplateRef = &corev1alpha1.WorkspaceTemplateReference{Name: substrateTestBaseTemplateName, Namespace: substrateTestTemplateNamespace}
			}),
			acpRuntimeEnabled:           true,
			acpWorkspaceDispatchEnabled: true,
			wantPath:                    agentExecutionPathRejected,
			wantReason:                  "substrate-enabled",
			wantWorkspaceStatusErr:      "substrate-enabled",
		},
		{
			name: "workspace cleanupPolicy retain fails closed",
			mutateTask: plannerWorkspaceTask(func(workspace *corev1alpha1.ExecutionWorkspaceSpec) {
				workspace.CleanupPolicy = corev1alpha1.WorkspaceCleanupPolicyRetain
			}),
			agentSandboxEnabled:         true,
			acpRuntimeEnabled:           true,
			acpWorkspaceDispatchEnabled: true,
			wantPath:                    agentExecutionPathRejected,
			wantReason:                  acpWorkspaceTestCleanupDeleteError,
			wantWorkspaceStatusErr:      acpWorkspaceTestCleanupDeleteError,
		},
		{
			name: "workspace session reuse without sessionRef fails closed",
			mutateTask: plannerWorkspaceTask(func(workspace *corev1alpha1.ExecutionWorkspaceSpec) {
				workspace.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
			}),
			agentSandboxEnabled:         true,
			acpRuntimeEnabled:           true,
			acpWorkspaceDispatchEnabled: true,
			wantPath:                    agentExecutionPathRejected,
			wantReason:                  acpWorkspaceTestSessionReferenceRequiredError,
			wantWorkspaceStatusErr:      acpWorkspaceTestSessionReferenceRequiredError,
		},
		{
			name: "harness v1 agent with execution workspace is rejected with a v1-specific message",
			mutateTask: func(task *corev1alpha1.Task) {
				plannerWorkspaceTask(nil)(task)
			},
			mutateAgent: func(agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.Type = corev1alpha1.AgentRuntimeCodex
				agent.Spec.Runtime.ContractVersion = new(corev1alpha1.AgentRuntimeContractHarnessV1)
			},
			agentSandboxEnabled:         true,
			acpRuntimeEnabled:           true,
			acpWorkspaceDispatchEnabled: true,
			harnessV1Enabled:            true,
			wantPath:                    agentExecutionPathRejected,
			wantReason:                  "harness v1 execution path",
			wantWorkspaceStatusErr:      "harness v1 execution path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := validPlannerTask()
			agent := baseAgent.DeepCopy()
			if tt.mutateTask != nil {
				tt.mutateTask(task)
			}
			if tt.mutateAgent != nil {
				tt.mutateAgent(agent)
			}

			r := newUnitReconciler(scheme, tt.objects...)
			r.AgentSandboxEnabled = tt.agentSandboxEnabled
			r.SubstrateEnabled = tt.substrateEnabled
			r.ACPRuntimeEnabled = tt.acpRuntimeEnabled
			r.ACPWorkspaceDispatchEnabled = tt.acpWorkspaceDispatchEnabled
			r.HarnessV1Enabled = tt.harnessV1Enabled

			plan := r.planAgentExecution(context.Background(), task, agent)
			if plan.path != tt.wantPath {
				t.Fatalf("plan path = %q, want %q (plan=%#v)", plan.path, tt.wantPath, plan)
			}
			if tt.wantReason != "" && !strings.Contains(plan.rejectionReason, tt.wantReason) {
				t.Fatalf("rejection reason = %q, want substring %q", plan.rejectionReason, tt.wantReason)
			}
			if tt.wantWorkspaceStatusErr == "" {
				if plan.workspaceStatusError != nil {
					t.Fatalf("workspaceStatusError = %v, want nil", plan.workspaceStatusError)
				}
				return
			}
			if plan.workspaceStatusError == nil || !strings.Contains(plan.workspaceStatusError.Error(), tt.wantWorkspaceStatusErr) {
				t.Fatalf("workspaceStatusError = %v, want substring %q", plan.workspaceStatusError, tt.wantWorkspaceStatusErr)
			}
		})
	}
}

func plannerExternalRuntime() *corev1alpha1.AgentRuntime {
	digest := func(char string) string { return "sha256:" + strings.Repeat(char, 64) }
	governance := corev1alpha1.AgentRuntimeWorkspaceGovernanceCapabilities{
		Mode:                     corev1alpha1.AgentRuntimeWorkspaceGovernanceStrict,
		OrkaOwnedWorkspaceDeltas: true, PromptScopedBrokerAuthorization: true,
		NoDirectSCMPublication: true, OrkaOwnedCleanRoomPublication: true,
		ExactInstanceFencing: true, DuplicateSafeMutations: true, CancellationSettlement: true,
	}
	profile := corev1alpha1.AgentRuntimeProfileSpec{
		Digest: digest("a"), DigestSchemaVersion: int32(harnessv2.ProfileDigestSchemaVersion), ACPProfile: "acp.v1", AdapterName: "external",
		AdapterDigest: digest("b"), ProviderKind: "external", Model: "model",
		AgentConfigurationDigest: digest("c"), ToolPolicyDigest: digest("d"), ApprovalPolicyDigest: digest("e"),
		MCPConfigurationDigest: digest("f"), WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
		ProxyCredentialRole: "provider", ProxyCredentialScope: "model:model", ResourceClass: "standard",
	}
	return &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "external-v2", Namespace: defaultNS, UID: "external-runtime-uid", Generation: 1},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			Capabilities:    &corev1alpha1.AgentRuntimeCapabilitiesSpec{RuntimeInstanceID: "external-instance", Profile: &profile, WorkspaceGovernance: &governance},
		},
		Status: corev1alpha1.AgentRuntimeStatus{Ready: true, ObservedGeneration: 1, ObservedCapabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
			RuntimeInstanceID: "external-instance", RuntimeProfileDigest: profile.Digest, WorkspaceGovernance: &governance,
		}},
	}
}

// plannerWorkspaceTask enables a canonical agent-sandbox execution workspace
// on the planner task and applies an optional mutation to it.
func plannerWorkspaceTask(mutate func(*corev1alpha1.ExecutionWorkspaceSpec)) func(*corev1alpha1.Task) {
	return func(task *corev1alpha1.Task) {
		task.UID = "task-uid-workspace"
		workspace := &corev1alpha1.ExecutionWorkspaceSpec{
			Enabled:  true,
			Provider: corev1alpha1.WorkspaceProviderAgentSandbox,
		}
		if mutate != nil {
			mutate(workspace)
		}
		task.Spec.Execution = &corev1alpha1.ExecutionSpec{Workspace: workspace}
	}
}

func validPlannerTask() *corev1alpha1.Task {
	return &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAgent,
			Prompt: "do work",
		},
	}
}

func validPlannerAgent() *corev1alpha1.Agent {
	return &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeCodex,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
}
