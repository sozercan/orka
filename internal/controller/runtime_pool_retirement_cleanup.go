package controller

import (
	"context"
	"fmt"
	"strings"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// recordDrainedRuntimePoolTaskCleanup preserves exact cleanup evidence before
// the pool lifecycle owner forgets a retired instance. Callers have validated
// the authenticated probe against the selected Pod and its deployed profile.
func (r *RuntimePoolReconciler) recordDrainedRuntimePoolTaskCleanup(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	active *corev1alpha1.RuntimePoolActiveInstanceStatus,
	status harnessv2.StatusResponse,
) error {
	if !runtimePoolRetirementFenceMatches(pool, active, status) {
		return fmt.Errorf("%w: RuntimePool retirement requires exact authenticated quiescence", store.ErrConflict)
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	var tasks corev1alpha1.TaskList
	if err := reader.List(ctx, &tasks, client.InNamespace(pool.Namespace)); err != nil {
		return fmt.Errorf("list Tasks before RuntimePool retirement: %w", err)
	}
	for i := range tasks.Items {
		task := &tasks.Items[i]
		execution := task.Status.Execution
		if execution == nil || execution.RuntimePoolName != pool.Name || execution.RuntimePoolUID != string(pool.UID) ||
			execution.RuntimeInstanceID != active.RuntimeInstanceID || execution.RuntimeSessionUID == "" {
			continue
		}
		taskUID := acpTaskControlUID(task)
		if runtimeSessionCleanupCompleteForUID(task, taskUID) {
			continue
		}
		binding := executionBinding(task, corev1alpha1.AgentRuntimeContractHarnessV2)
		if binding == nil || binding.Backend != corev1alpha1.AgentExecutionBackendRuntimePool || binding.Task.UID != taskUID ||
			binding.RuntimeProfileDigest != active.ProfileDigest || execution.RuntimeSessionProfileDigest != active.ProfileDigest ||
			execution.RuntimeSessionSupervisorBootID != active.BootID || execution.AgentRuntimeName != "" || execution.AgentRuntimeUID != "" {
			return fmt.Errorf("%w: Task %s/%s lacks exact RuntimePool retirement authority", store.ErrConflict, task.Namespace, task.Name)
		}
		digest, err := canonicalAgentExecutionBindingDigest(*binding)
		if err != nil || digest != binding.BindingDigest {
			return fmt.Errorf("%w: Task %s/%s RuntimePool binding failed integrity verification", store.ErrConflict, task.Namespace, task.Name)
		}
		if err := persistTaskScopedRuntimeSessionCleanupReceipt(ctx, r.Client, task, taskUID,
			execution.RuntimeInstanceID, execution.RuntimeSessionUID, execution.RuntimeSessionGeneration); err != nil {
			return fmt.Errorf("record RuntimePool retirement proof for Task %s/%s: %w", task.Namespace, task.Name, err)
		}
	}
	return nil
}

func runtimePoolRetirementFenceMatches(
	pool *corev1alpha1.RuntimePool,
	active *corev1alpha1.RuntimePoolActiveInstanceStatus,
	status harnessv2.StatusResponse,
) bool {
	return pool != nil && pool.UID != "" && active != nil &&
		strings.TrimSpace(active.RuntimeInstanceID) != "" && strings.TrimSpace(active.BootID) != "" &&
		upgradeDrainSupervisorIsQuiescent(status) &&
		string(status.Fence.RuntimePoolUID) == string(pool.UID) && status.Fence.RuntimePoolGeneration == uint64(pool.Generation) &&
		string(status.Fence.RuntimeInstanceID) == active.RuntimeInstanceID && string(status.Fence.SupervisorBootID) == active.BootID &&
		status.Fence.ControllerEpoch == uint64(active.ControllerEpoch) &&
		string(status.Fence.RuntimeProfileDigest) == active.ProfileDigest && active.ProfileDigest == pool.Spec.Runtime.Profile.Digest &&
		status.Fence.ProfileDigestSchemaVersion == harnessv2.ProfileDigestSchemaVersion
}
