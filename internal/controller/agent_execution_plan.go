/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

type agentExecutionPath string

const (
	agentExecutionPathACP       agentExecutionPath = "acp-runtime-pool"
	agentExecutionPathHarnessV1 agentExecutionPath = "harness-v1"
	agentExecutionPathRejected  agentExecutionPath = "rejected"
)

type agentExecutionPlan struct {
	path                 agentExecutionPath
	externalRuntimeName  string
	rejectionReason      string
	workspaceStatusError error
	// transientError defers planning instead of rejecting: the reconcile
	// returns it so a brief read outage requeues rather than permanently
	// failing the Task.
	transientError error
}

func agentACPPlan() agentExecutionPlan {
	return agentExecutionPlan{path: agentExecutionPathACP}
}

func agentHarnessV1Plan(runtimeName string) agentExecutionPlan {
	return agentExecutionPlan{path: agentExecutionPathHarnessV1, externalRuntimeName: strings.TrimSpace(runtimeName)}
}

func rejectAgentExecutionPlan(reason string) agentExecutionPlan {
	return agentExecutionPlan{path: agentExecutionPathRejected, rejectionReason: reason}
}

func rejectAgentExecutionPlanWithWorkspaceStatus(reason string, err error) agentExecutionPlan {
	plan := rejectAgentExecutionPlan(reason)
	plan.workspaceStatusError = err
	return plan
}

// planAgentExecution owns the controller routing decision for type: agent
// Tasks. Built-in Codex, Claude, Copilot, and OpenCode runtimes use only the ACP v2
// RuntimePool path; a Task.spec.execution.workspace request additionally binds
// that path to a workspace-provider-backed RuntimePool when enabled and fails
// closed otherwise. External runtimeRef registrations and conformance remain
// available, but Task dispatch fails closed until the v2 dispatcher support
// boundary is enabled. There is no legacy turn or Job fallback, and no
// cross-mode harness-v1 fallback for workspace-backed v2 work.
func (r *TaskReconciler) planAgentExecution(
	ctx context.Context,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
) agentExecutionPlan {
	workspaceRequested := taskRequestsExecutionWorkspace(task)

	if agent == nil || agent.Spec.Runtime == nil {
		return rejectAgentExecutionPlan("agent runtime configuration is required")
	}
	if agent.Spec.Runtime.RuntimeRef != nil && strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) != "" {
		name := strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name)
		reader := r.APIReader
		if reader == nil {
			reader = r.Client
		}
		runtime := &corev1alpha1.AgentRuntime{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: name}, runtime); err != nil {
			return rejectAgentExecutionPlan(fmt.Sprintf("resolve AgentRuntime %q: %v", name, err))
		}
		switch runtime.RegisteredContractVersion() {
		case corev1alpha1.AgentRuntimeContractHarnessV1:
			if !r.HarnessV1Enabled {
				return rejectAgentExecutionPlan("AgentRuntime is classified orka.harness.v1, but harness v1 admission is disabled; v2 execution never substitutes for it")
			}
			if workspaceRequested {
				err := errors.New(harnessV1ExecutionWorkspaceUnsupportedReason) //nolint:staticcheck // Field path begins the user-facing validation message.
				return rejectAgentExecutionPlanWithWorkspaceStatus(err.Error(), err)
			}
			return agentHarnessV1Plan(name)
		case corev1alpha1.AgentRuntimeContractHarnessV2:
			if workspaceRequested {
				err := fmt.Errorf("Task.spec.execution.workspace is not supported for external AgentRuntime dispatch; %s", externalAgentRuntimeDispatchUnsupportedReason(name)) //nolint:staticcheck // Field path begins the user-facing validation message.
				return rejectAgentExecutionPlanWithWorkspaceStatus(err.Error(), err)
			}
			return rejectAgentExecutionPlan(externalAgentRuntimeDispatchUnsupportedReason(name))
		default:
			return rejectAgentExecutionPlan(fmt.Sprintf("AgentRuntime %q is unclassified; a missing selector is never protocol evidence", name))
		}
	}

	switch agent.Spec.Runtime.Type {
	case corev1alpha1.AgentRuntimeCodex, corev1alpha1.AgentRuntimeClaude, corev1alpha1.AgentRuntimeCopilot, corev1alpha1.AgentRuntimeOpencode:
	default:
		return rejectAgentExecutionPlan(fmt.Sprintf("agent runtime %q is not supported by the ACP core runtime", agent.Spec.Runtime.Type))
	}

	contract := agent.BuiltInContractVersion()
	switch contract {
	case corev1alpha1.AgentRuntimeContractHarnessV2:
		if reason := agentACPRuntimeUnsupportedReason(task, agent); reason != "" {
			return rejectAgentExecutionPlan(reason)
		}
		if task.Spec.PriorTaskRef != nil {
			return rejectAgentExecutionPlan("priorTaskRef continuation is not supported by the ACP core runtime; use sessionRef")
		}
		if !r.ACPRuntimeEnabled {
			return rejectAgentExecutionPlan("ACP core runtime is disabled; built-in v2 agent runtimes have no fallback execution path")
		}
		if workspaceRequested {
			if plan, rejected := r.rejectUnsupportedACPWorkspacePlan(ctx, task); rejected {
				return plan
			}
		}
		return agentACPPlan()
	case corev1alpha1.AgentRuntimeContractHarnessV1:
		if !r.HarnessV1Enabled {
			return rejectAgentExecutionPlan("agent is classified orka.harness.v1, but harness v1 admission is disabled; v2 execution never substitutes for it")
		}
		if workspaceRequested {
			err := errors.New(harnessV1ExecutionWorkspaceUnsupportedReason) //nolint:staticcheck // Field path begins the user-facing validation message.
			return rejectAgentExecutionPlanWithWorkspaceStatus(err.Error(), err)
		}
		if agent.Spec.Runtime.Type == corev1alpha1.AgentRuntimeOpencode {
			return rejectAgentExecutionPlan("new harness v1 OpenCode bindings are prohibited; only sealed-inventory legacy adoption may use the v1 OpenCode path")
		}
		if reason := agentHarnessV1InheritedAuthorityUnsupportedReason(agent); reason != "" {
			return rejectAgentExecutionPlan(reason)
		}
		return agentHarnessV1Plan("")
	default:
		return rejectAgentExecutionPlan("agent runtime.contractVersion is unclassified; a missing selector is never interpreted as either protocol and execution admission fails closed")
	}
}

// harnessV1ExecutionWorkspaceUnsupportedReason names the harness v1 path
// exactly; workspace-provider-backed execution is a v2 RuntimePool capability
// and never dispatches through, or falls back to, a harness-v1 installation.
//
//nolint:staticcheck // Field path begins the user-facing validation message.
const harnessV1ExecutionWorkspaceUnsupportedReason = "Task.spec.execution.workspace is not supported on the harness v1 execution path; workspace-provider-backed RuntimeSessions require the ACP v2 RuntimePool path, and repository access uses Task.spec.workspace"

// taskRequestsExecutionWorkspace reports whether the Task carries an enabled
// legacy-shaped or class-shaped execution-workspace request.
func taskRequestsExecutionWorkspace(task *corev1alpha1.Task) bool {
	return task != nil && task.Spec.Execution != nil && task.Spec.Execution.Workspace != nil &&
		(task.Spec.Execution.Workspace.Enabled || task.Spec.Execution.Workspace.ClassRef != nil)
}

// rejectUnsupportedACPWorkspacePlan fails closed on every workspace request the
// ACP RuntimePool path cannot host, before any workspace or RuntimePool demand
// exists. A nil plan with rejected=false admits the workspace-backed ACP path.
func (r *TaskReconciler) rejectUnsupportedACPWorkspacePlan(ctx context.Context, task *corev1alpha1.Task) (agentExecutionPlan, bool) {
	if task.Status.AgentExecutionBinding != nil {
		// The execution binding is frozen: new-allocation readiness (live
		// class resolution, provider Active lifecycle) no longer applies -
		// core deliberately admits Draining providers for already-admitted
		// workspaces, and ensureAgentExecutionBinding re-verifies the frozen
		// snapshot on every pass. The configuration-level dispatch gates are
		// enforced against the VERIFIED frozen plan at the queue chokepoint
		// (queueACPRuntimeTask), which both this path and bound-task
		// recovery flow through - the public status projection may still be
		// nil here and is never the gate's authority.
		return agentExecutionPlan{}, false
	}
	resolvedClass, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		if errors.Is(err, errACPWorkspacePlanningTransient) {
			// A brief API-server or control-store outage must requeue, not
			// permanently reject new capped-Suspend Tasks. Actual quota
			// exhaustion and validation failures stay rejections.
			return agentExecutionPlan{path: agentExecutionPathRejected, transientError: err}, true
		}
		return rejectAgentExecutionPlanWithWorkspaceStatus(err.Error(), err), true
	}
	binding, err := validateACPWorkspaceBindingRequestWithClass(task, r.ExecutionWorkspaceDefaultProvider, r.EnforceNamespaceIsolation, resolvedClass)
	if err != nil {
		return rejectAgentExecutionPlanWithWorkspaceStatus(err.Error(), err), true
	}
	if binding == nil {
		return agentExecutionPlan{}, false
	}
	switch binding.Provider {
	case corev1alpha1.WorkspaceProviderAgentSandbox:
		if !r.AgentSandboxEnabled {
			err := fmt.Errorf("execution workspace provider agent-sandbox is disabled; enable --agent-sandbox-enabled")
			return rejectAgentExecutionPlanWithWorkspaceStatus(err.Error(), err), true
		}
	case corev1alpha1.WorkspaceProviderSubstrate:
		if !r.SubstrateEnabled {
			err := fmt.Errorf("execution workspace provider substrate is disabled; enable --substrate-enabled")
			return rejectAgentExecutionPlanWithWorkspaceStatus(err.Error(), err), true
		}
	}
	if !r.ACPWorkspaceDispatchEnabled {
		err := fmt.Errorf("workspace-provider-backed RuntimeSession dispatch is disabled; enable --acp-workspace-dispatch-enabled to host this Task's RuntimeSession in a %s workspace", binding.Provider)
		return rejectAgentExecutionPlanWithWorkspaceStatus(err.Error(), err), true
	}
	return agentExecutionPlan{}, false
}

func externalAgentRuntimeDispatchUnsupportedReason(name string) string {
	return fmt.Sprintf("external AgentRuntime %q Task dispatch is not supported until the v2 dispatcher is wired", strings.TrimSpace(name))
}

func externalAgentRuntimeReadinessReason(task *corev1alpha1.Task, runtime *corev1alpha1.AgentRuntime) string {
	if runtime == nil || runtime.RegisteredContractVersion() != corev1alpha1.AgentRuntimeContractHarnessV2 {
		return "external AgentRuntime must use orka.harness.v2"
	}
	if !runtime.Status.Ready || runtime.Status.ObservedGeneration != runtime.Generation || runtime.Status.ObservedCapabilities == nil {
		return fmt.Sprintf("external AgentRuntime %q has not passed current-generation v2 conformance", runtime.Name)
	}
	if runtime.Spec.Capabilities == nil || runtime.Spec.Capabilities.Profile == nil ||
		runtime.Status.ObservedCapabilities.RuntimeInstanceID == "" ||
		runtime.Status.ObservedCapabilities.RuntimeProfileDigest != runtime.Spec.Capabilities.Profile.Digest {
		return fmt.Sprintf("external AgentRuntime %q does not have an exact observed runtime identity/profile", runtime.Name)
	}
	if task != nil {
		intent := effectiveACPWorkspaceIntent(task)
		if runtime.Spec.Capabilities.Profile.WorkspaceIntent != intent {
			return fmt.Sprintf("external AgentRuntime %q profile workspace intent %q does not match Task intent %q", runtime.Name, runtime.Spec.Capabilities.Profile.WorkspaceIntent, intent)
		}
	}
	if runtime.Spec.Capabilities.WorkspaceGovernance == nil ||
		runtime.Status.ObservedCapabilities.WorkspaceGovernance == nil ||
		!runtime.Spec.Capabilities.WorkspaceGovernance.Strict() ||
		!runtime.Status.ObservedCapabilities.WorkspaceGovernance.Strict() {
		return fmt.Sprintf("external AgentRuntime %q does not provide strict workspace governance", runtime.Name)
	}
	return ""
}

func (r *TaskReconciler) rejectPlannedAgentExecution(
	ctx context.Context,
	task *corev1alpha1.Task,
	plan agentExecutionPlan,
) (ctrl.Result, error) {
	if plan.transientError != nil {
		// Requeue with the controller's error backoff; nothing terminal has
		// been decided about this Task.
		return ctrl.Result{}, plan.transientError
	}
	if plan.workspaceStatusError != nil {
		if statusErr := r.markExecutionWorkspaceValidationFailed(ctx, task, plan.workspaceStatusError); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
	}
	return r.failTask(ctx, task, plan.rejectionReason)
}

func agentACPRuntimeUnsupportedReason(task *corev1alpha1.Task, agent *corev1alpha1.Agent) string {
	if task == nil {
		return ""
	}
	switch {
	case task.Spec.Transaction != nil:
		return "ACP core runtime tasks do not support transaction token delegation"
	case agent != nil && agent.Spec.Coordination != nil && agent.Spec.Coordination.Autonomous:
		return "ACP core runtime tasks do not support Agent.spec.coordination.autonomous; disable autonomous coordination"
	case task.Spec.RetryPolicy != nil && task.Spec.RetryPolicy.MaxRetries > 0:
		return "ACP core runtime tasks do not support effective Task.spec.retryPolicy retries; maxRetries must be 0"
	case effectiveAgentResources(task, agent):
		return "ACP core runtime tasks do not support custom Kubernetes resources until a reviewed RuntimePool resource class is selected"
	case resolveExecution(task, agent) != nil:
		return "ACP core runtime tasks do not support per-Task execution placement"
	default:
		return ""
	}
}

func effectiveAgentResources(task *corev1alpha1.Task, agent *corev1alpha1.Agent) bool {
	if task != nil && (len(task.Spec.Resources.Requests) > 0 || len(task.Spec.Resources.Limits) > 0) {
		return true
	}
	return agent != nil && (len(agent.Spec.Resources.Requests) > 0 || len(agent.Spec.Resources.Limits) > 0)
}

func agentHarnessV1InheritedAuthorityUnsupportedReason(agent *corev1alpha1.Agent) string {
	switch {
	case effectiveAgentResources(nil, agent):
		return "harness v1 built-in runtimes do not support inherited Agent.spec.resources"
	case resolveExecution(nil, agent) != nil:
		return "harness v1 built-in runtimes do not support inherited Agent.spec.execution placement"
	default:
		return ""
	}
}

// frozenWorkspaceDispatchDisabledReason enforces the configuration-level
// workspace dispatch gates against the VERIFIED frozen execution plan. It is
// the single authority for bound Tasks: both ordinary planning and bound-task
// recovery reach it through the queue chokepoint, so a flag disabled after
// the binding froze can never create new RuntimePool demand.
func (r *TaskReconciler) frozenWorkspaceDispatchDisabledReason(binding *ACPRuntimeWorkspaceBinding) string {
	if binding == nil {
		return ""
	}
	switch binding.Provider {
	case corev1alpha1.WorkspaceProviderAgentSandbox:
		if !r.AgentSandboxEnabled {
			return "execution workspace provider agent-sandbox is disabled; enable --agent-sandbox-enabled"
		}
	case corev1alpha1.WorkspaceProviderSubstrate:
		if !r.SubstrateEnabled {
			return "execution workspace provider substrate is disabled; enable --substrate-enabled"
		}
	}
	if !r.ACPWorkspaceDispatchEnabled {
		return "workspace-provider-backed RuntimeSession dispatch is disabled; enable --acp-workspace-dispatch-enabled"
	}
	if binding.Class != nil && !r.WorkspaceProviderAPIEnabled {
		// The class-backed lifecycle needs the core workspace reconciler; in
		// cleanup-only mode a newly materialized workspace would never be
		// admitted and the Task would stay Pending forever.
		return "class-backed execution workspaces require --enable-workspace-provider-api"
	}
	return ""
}
