/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

// AgentReconciler reconciles a Agent object
type AgentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	agentReasoningEffortLow    = "low"
	agentReasoningEffortMedium = "medium"
	agentReasoningEffortHigh   = "high"
	agentReasoningEffortXHigh  = "xhigh"
	agentReasoningEffortMax    = "max"
)

// +kubebuilder:rbac:groups=core.orka.ai,resources=agents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.orka.ai,resources=agents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.orka.ai,resources=agents/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.orka.ai,resources=tasks,verbs=list
// +kubebuilder:rbac:groups=core.orka.ai,resources=providers,verbs=get
// +kubebuilder:rbac:groups=core.orka.ai,resources=tools,verbs=get
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

// Reconcile validates the Agent configuration and updates its status.
func (r *AgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Agent
	agent := &corev1alpha1.Agent{}
	if err := r.Get(ctx, req.NamespacedName, agent); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("Reconciling Agent", "agent", agent.Name)

	// Validate the agent configuration
	validationErr := r.validateAgent(ctx, agent)

	// Count active tasks referencing this agent
	activeTasks, err := r.countActiveTasks(ctx, agent)
	if err != nil {
		logger.Error(err, "Failed to count active tasks")
		// Non-fatal: continue with status update
	}

	// TTL-based cleanup: if agent has TTL, no active tasks, and TTL has expired, delete it
	if result, deleted := r.checkTTLExpiry(ctx, agent, activeTasks); deleted {
		return result, nil
	}

	// Update status
	return r.updateStatus(ctx, agent, activeTasks, validationErr)
}

// validateAgent validates the Agent's referenced resources exist and config is coherent.
func (r *AgentReconciler) validateAgent(ctx context.Context, agent *corev1alpha1.Agent) error {
	// Runtime and providerRef are mutually exclusive
	if agent.Spec.Runtime != nil && agent.Spec.ProviderRef != nil {
		return fmt.Errorf("runtime and providerRef are mutually exclusive")
	}

	// For non-runtime agents, validate model config
	if agent.Spec.Runtime == nil {
		if agent.Spec.ProviderRef == nil && (agent.Spec.Model == nil || agent.Spec.Model.Provider == "") {
			return fmt.Errorf("either providerRef or model.provider must be specified")
		}
	} else {
		if agent.Spec.Runtime.RuntimeRef != nil {
			if strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) == "" {
				return fmt.Errorf("runtimeRef.name is required")
			}
			if err := validateRuntimeRefAgentTaskRestrictions(nil, agent); err != nil {
				return err
			}
		}
		if err := validateACPAgentModelControls(agent.Spec.Runtime, agent.Spec.Model); err != nil {
			return err
		}
		if err := validateAgentRuntimeReasoningEffort(agent.Spec.Runtime); err != nil {
			return err
		}
		if err := validateBuiltInACPAgentCredentialSecretRef(agent); err != nil {
			return err
		}
	}

	if err := ValidateOpenCodeAgentSpec(agent); err != nil {
		return err
	}

	if err := r.validateProviderRef(ctx, agent); err != nil {
		return err
	}
	if err := r.validateSecretRef(ctx, agent); err != nil {
		return err
	}
	if err := r.validateTools(ctx, agent); err != nil {
		return err
	}
	if err := r.validateSkills(ctx, agent); err != nil {
		return err
	}
	if err := r.validateSystemPromptConfigMap(ctx, agent); err != nil {
		return err
	}
	return r.validateCoordination(ctx, agent)
}

func isBuiltInACPProviderRuntime(runtimeType corev1alpha1.AgentRuntimeType) bool {
	switch runtimeType {
	case corev1alpha1.AgentRuntimeCodex, corev1alpha1.AgentRuntimeClaude,
		corev1alpha1.AgentRuntimeCopilot, corev1alpha1.AgentRuntimeOpencode:
		return true
	default:
		return false
	}
}

func validateBuiltInACPAgentCredentialSecretRef(agent *corev1alpha1.Agent) error {
	if agent == nil || agent.Spec.Runtime == nil || agent.Spec.SecretRef == nil ||
		!isBuiltInACPProviderRuntime(agent.Spec.Runtime.Type) {
		return nil
	}
	return fmt.Errorf("built-in ACP runtime %q does not support agent secretRef; provider credentials are controller-managed", agent.Spec.Runtime.Type)
}

func validateAgentRuntimeReasoningEffort(runtimeCfg *corev1alpha1.AgentCLIRuntime) error {
	if runtimeCfg == nil {
		return nil
	}
	effort := strings.ToLower(strings.TrimSpace(runtimeCfg.DefaultReasoningEffort))
	if effort == "" {
		return nil
	}
	switch runtimeCfg.Type {
	case corev1alpha1.AgentRuntimeCodex:
		switch effort {
		case agentReasoningEffortLow, agentReasoningEffortMedium, agentReasoningEffortHigh, agentReasoningEffortXHigh:
			return nil
		default:
			return fmt.Errorf("codex runtime defaultReasoningEffort %q must be low, medium, high, or xhigh", effort)
		}
	case corev1alpha1.AgentRuntimeClaude:
		switch effort {
		case agentReasoningEffortLow, agentReasoningEffortMedium, agentReasoningEffortHigh,
			agentReasoningEffortXHigh, agentReasoningEffortMax:
			return nil
		default:
			return fmt.Errorf("claude runtime defaultReasoningEffort %q must be low, medium, high, xhigh, or max", effort)
		}
	case corev1alpha1.AgentRuntimeCopilot:
		return fmt.Errorf("copilot runtime does not support defaultReasoningEffort")
	default:
		if runtimeCfg.RuntimeRef != nil {
			return fmt.Errorf("runtimeRef custom runtimes do not support defaultReasoningEffort")
		}
		return fmt.Errorf("runtime %q does not support defaultReasoningEffort", runtimeCfg.Type)
	}
}

// validateProviderRef validates the providerRef if set.
func (r *AgentReconciler) validateProviderRef(ctx context.Context, agent *corev1alpha1.Agent) error {
	if agent.Spec.ProviderRef == nil {
		return nil
	}
	provider := &corev1alpha1.Provider{}
	ns := agent.Namespace
	if agent.Spec.ProviderRef.Namespace != "" {
		ns = agent.Spec.ProviderRef.Namespace
	}
	key := client.ObjectKey{Name: agent.Spec.ProviderRef.Name, Namespace: ns}
	if err := r.Get(ctx, key, provider); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("referenced provider %q not found", agent.Spec.ProviderRef.Name)
		}
		return fmt.Errorf("failed to get provider %q: %w", agent.Spec.ProviderRef.Name, err)
	}
	if !provider.Status.Ready {
		return fmt.Errorf("referenced provider %q is not ready", agent.Spec.ProviderRef.Name)
	}
	return nil
}

// validateSecretRef validates the secretRef if set.
func (r *AgentReconciler) validateSecretRef(ctx context.Context, agent *corev1alpha1.Agent) error {
	if agent.Spec.SecretRef == nil {
		return nil
	}
	secret := &corev1.Secret{}
	key := client.ObjectKey{Name: agent.Spec.SecretRef.Name, Namespace: agent.Namespace}
	if err := r.Get(ctx, key, secret); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("referenced secret %q not found", agent.Spec.SecretRef.Name)
		}
		return fmt.Errorf("failed to get secret %q: %w", agent.Spec.SecretRef.Name, err)
	}
	return nil
}

// validateTools validates that referenced tools exist.
func (r *AgentReconciler) validateTools(ctx context.Context, agent *corev1alpha1.Agent) error {
	for _, toolRef := range agent.Spec.Tools {
		if toolRef.Enabled != nil && !*toolRef.Enabled {
			continue
		}
		tool := &corev1alpha1.Tool{}
		key := client.ObjectKey{Name: toolRef.Name, Namespace: agent.Namespace}
		if err := r.Get(ctx, key, tool); err != nil {
			if errors.IsNotFound(err) {
				// Tool might be a built-in; only warn, don't fail
				continue
			}
			return fmt.Errorf("failed to check tool %q: %w", toolRef.Name, err)
		}
	}
	return nil
}

// validateSkills validates that referenced Skill CRDs or ConfigMap-backed skills exist.
func (r *AgentReconciler) validateSkills(ctx context.Context, agent *corev1alpha1.Agent) error {
	for _, skillRef := range agent.Spec.Skills {
		switch {
		case skillRef.Name != "":
			skill := &corev1alpha1.Skill{}
			skillName := skillRef.Name
			key := client.ObjectKey{Name: skillName, Namespace: agent.Namespace}
			if err := r.Get(ctx, key, skill); err != nil {
				if errors.IsNotFound(err) {
					return fmt.Errorf("referenced Skill %q not found", skillName)
				}
				return fmt.Errorf("failed to get Skill %q: %w", skillName, err)
			}
		case skillRef.ConfigMapRef != nil:
			cm := &corev1.ConfigMap{}
			key := client.ObjectKey{Name: skillRef.ConfigMapRef.Name, Namespace: agent.Namespace}
			if err := r.Get(ctx, key, cm); err != nil {
				if errors.IsNotFound(err) {
					return fmt.Errorf("referenced skill ConfigMap %q not found", skillRef.ConfigMapRef.Name)
				}
				return fmt.Errorf("failed to get skill ConfigMap %q: %w", skillRef.ConfigMapRef.Name, err)
			}
			if _, ok := cm.Data[skillRef.ConfigMapRef.Key]; !ok {
				return fmt.Errorf("key %q not found in skill ConfigMap %q", skillRef.ConfigMapRef.Key, skillRef.ConfigMapRef.Name)
			}
		default:
			return fmt.Errorf("skill reference must set either name or configMapRef")
		}
	}
	return nil
}

// validateSystemPromptConfigMap validates the systemPrompt configMapRef if set.
func (r *AgentReconciler) validateSystemPromptConfigMap(ctx context.Context, agent *corev1alpha1.Agent) error {
	if agent.Spec.SystemPrompt == nil || agent.Spec.SystemPrompt.ConfigMapRef == nil {
		return nil
	}
	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Name: agent.Spec.SystemPrompt.ConfigMapRef.Name, Namespace: agent.Namespace}
	if err := r.Get(ctx, key, cm); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("systemPrompt ConfigMap %q not found", agent.Spec.SystemPrompt.ConfigMapRef.Name)
		}
		return fmt.Errorf("failed to get systemPrompt ConfigMap %q: %w", agent.Spec.SystemPrompt.ConfigMapRef.Name, err)
	}
	if _, ok := cm.Data[agent.Spec.SystemPrompt.ConfigMapRef.Key]; !ok {
		return fmt.Errorf("key %q not found in systemPrompt ConfigMap %q", agent.Spec.SystemPrompt.ConfigMapRef.Key, agent.Spec.SystemPrompt.ConfigMapRef.Name)
	}
	return nil
}

// validateCoordination validates the coordination config and referenced agents.
func (r *AgentReconciler) validateCoordination(ctx context.Context, agent *corev1alpha1.Agent) error {
	if agent.Spec.Coordination == nil || !agent.Spec.Coordination.Enabled {
		return nil
	}
	for _, allowed := range agent.Spec.Coordination.AllowedAgents {
		ns := agent.Namespace
		if allowed.Namespace != "" {
			ns = allowed.Namespace
		}
		delegateAgent := &corev1alpha1.Agent{}
		key := client.ObjectKey{Name: allowed.Name, Namespace: ns}
		if err := r.Get(ctx, key, delegateAgent); err != nil {
			if errors.IsNotFound(err) {
				return fmt.Errorf("coordination target agent %q not found", allowed.Name)
			}
			return fmt.Errorf("failed to check coordination target agent %q: %w", allowed.Name, err)
		}
	}
	return nil
}

// countActiveTasks counts tasks that reference this agent and are not complete.
func (r *AgentReconciler) countActiveTasks(ctx context.Context, agent *corev1alpha1.Agent) (int32, error) {
	taskList := &corev1alpha1.TaskList{}
	if err := r.List(ctx, taskList, client.InNamespace(agent.Namespace)); err != nil {
		return 0, err
	}

	var count int32
	for i := range taskList.Items {
		task := &taskList.Items[i]
		if task.Spec.AgentRef != nil && task.Spec.AgentRef.Name == agent.Name {
			phase := task.Status.Phase
			if phase != corev1alpha1.TaskPhaseSucceeded && phase != corev1alpha1.TaskPhaseFailed {
				count++
			}
		}
	}
	return count, nil
}

// updateStatus updates the Agent's status with validation results and active task count.
func (r *AgentReconciler) updateStatus(ctx context.Context, agent *corev1alpha1.Agent, activeTasks int32, validationErr error) (ctrl.Result, error) {
	now := metav1.Now()

	agent.Status.ActiveTasks = activeTasks
	agent.Status.Ready = validationErr == nil
	if activeTasks > 0 {
		agent.Status.LastUsed = &now
	}

	condition := metav1.Condition{
		Type:               "Ready",
		LastTransitionTime: now,
		ObservedGeneration: agent.Generation,
	}

	if validationErr != nil {
		condition.Status = metav1.ConditionFalse
		condition.Reason = reasonValidationFailed
		condition.Message = validationErr.Error()
	} else {
		condition.Status = metav1.ConditionTrue
		condition.Reason = reasonValidationSucceeded
		condition.Message = "Agent configuration is valid"
	}

	meta.SetStatusCondition(&agent.Status.Conditions, condition)

	activeCount := activeTasks // capture for closure
	lastUsed := agent.Status.LastUsed
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if err := r.Get(ctx, types.NamespacedName{Name: agent.Name, Namespace: agent.Namespace}, agent); err != nil {
			return err
		}
		agent.Status.ActiveTasks = activeCount
		agent.Status.Ready = validationErr == nil
		agent.Status.LastUsed = lastUsed
		meta.SetStatusCondition(&agent.Status.Conditions, condition)
		return r.Status().Update(ctx, agent)
	}); err != nil {
		return ctrl.Result{}, err
	}

	// Schedule requeue for TTL check if agent has TTL
	if agent.Spec.TTLAfterLastTask != nil {
		if activeTasks == 0 && agent.Status.LastUsed != nil {
			ttl := agent.Spec.TTLAfterLastTask.Duration
			elapsed := time.Since(agent.Status.LastUsed.Time)
			if remaining := ttl - elapsed; remaining > 0 {
				return ctrl.Result{RequeueAfter: remaining}, nil
			}
		} else if activeTasks > 0 {
			// Tasks still running; requeue to re-check after they finish.
			// The Task watch will also trigger reconciliation on completion.
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}

	return ctrl.Result{}, nil
}

// checkTTLExpiry deletes the agent if its TTL has expired and no tasks are active.
func (r *AgentReconciler) checkTTLExpiry(ctx context.Context, agent *corev1alpha1.Agent, activeTasks int32) (ctrl.Result, bool) {
	if agent.Spec.TTLAfterLastTask == nil || agent.Spec.TTLAfterLastTask.Duration == 0 {
		return ctrl.Result{}, false
	}
	if activeTasks > 0 {
		return ctrl.Result{}, false
	}

	// Need a LastUsed timestamp to compute expiry
	if agent.Status.LastUsed == nil {
		// Never used — use creation time as fallback
		agent.Status.LastUsed = &metav1.Time{Time: agent.CreationTimestamp.Time}
	}

	elapsed := time.Since(agent.Status.LastUsed.Time)
	ttl := agent.Spec.TTLAfterLastTask.Duration
	if elapsed < ttl {
		return ctrl.Result{}, false
	}

	logger := log.FromContext(ctx)
	logger.Info("TTL expired, deleting agent", "agent", agent.Name, "ttl", ttl, "lastUsed", agent.Status.LastUsed.Time)
	if err := r.Delete(ctx, agent); err != nil {
		logger.Error(err, "Failed to delete expired agent")
		return ctrl.Result{RequeueAfter: time.Second}, false //nolint:staticcheck
	}
	return ctrl.Result{}, true
}

// SetupWithManager sets up the controller with the Manager.
func (r *AgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1alpha1.Agent{}).
		// Watch Tasks so that when a task completes, the referenced agent
		// gets reconciled for TTL checking.
		Watches(&corev1alpha1.Task{}, handler.EnqueueRequestsFromMapFunc(
			func(ctx context.Context, obj client.Object) []reconcile.Request {
				task, ok := obj.(*corev1alpha1.Task)
				if !ok || task.Spec.AgentRef == nil {
					return nil
				}
				return []reconcile.Request{{
					NamespacedName: types.NamespacedName{
						Name:      task.Spec.AgentRef.Name,
						Namespace: task.Namespace,
					},
				}}
			})).
		Named("agent").
		Complete(r)
}
