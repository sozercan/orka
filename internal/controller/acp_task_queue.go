package controller

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/workspace/statusrules"
)

const (
	acpRuntimePoolLabel                          = "orka.ai/acp-runtime-pool"
	acpRuntimeTrustLabel                         = "orka.ai/acp-trust-domain"
	acpRuntimeProfileLabel                       = "orka.ai/acp-profile"
	acpRuntimeWorkspaceProviderLabel             = "orka.ai/acp-execution-workspace-provider"
	acpRuntimeTaskPoolLabel                      = "orka.ai/runtime-pool"
	acpRuntimeSessionCleanupAnnotation           = "orka.ai/runtime-session-cleanup"
	acpExternalRuntimeTaskAnnotation             = "orka.ai/agent-runtime"
	acpRuntimeLastDemandAnnotation               = "orka.ai/acp-last-demand-at"
	acpRuntimeQueuedAtAnnotation                 = "orka.ai/acp-queued-at"
	acpRuntimePoolImageProvenanceCondition       = "ImageProvenance"
	acpRuntimePoolImageProvenanceReason          = "VerifiedExecutionPlan"
	defaultACPTaskPriority                 int32 = 500
	defaultACPQueueAgingStep               int32 = 25
)

const (
	DefaultACPQueueAgingInterval = 30 * time.Second
	DefaultACPQueueMaximumWait   = 5 * time.Minute
)

//nolint:gocyclo // ACP queueing keeps durable planning, recovery, and binding gates auditable together.
func (r *TaskReconciler) queueACPRuntimeTask(ctx context.Context, task *corev1alpha1.Task, _ *corev1alpha1.Agent) (ctrl.Result, error) {
	if task == nil || task.Status.AgentExecutionBinding == nil {
		return ctrl.Result{}, errors.New("immutable v2 execution binding is required before ACP queueing")
	}
	bound, err := r.loadVerifiedBoundExecution(ctx, task, task.Status.AgentExecutionBinding)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("verify immutable v2 execution before ACP queueing: %w", err)
	}
	frozenTask := bound.frozenTask
	externalRuntime := bound.externalRuntime
	externalDispatch := bound.binding.Backend == corev1alpha1.AgentExecutionBackendExternalEndpoint
	if externalDispatch && externalRuntime == nil {
		return ctrl.Result{}, errors.New("verified external v2 execution is missing its AgentRuntime target")
	}
	if reason := r.frozenWorkspaceDispatchDisabledReason(bound.plan.Workspace); reason != "" {
		// The single configuration gate for bound Tasks: ordinary planning
		// AND bound-task recovery both flow through this chokepoint before
		// any workspace or RuntimePool demand exists.
		return r.failACPPlanningTask(ctx, task, corev1alpha1.TaskExecutionReason("WorkspaceUnsupported"), reason)
	}
	if err := r.ACPAdmissionGate.Check(); err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if r.DurableControlStore == nil || r.ControllerEpochManager == nil {
		return r.failTask(ctx, task, "durable ACP control store and controller epoch manager are required")
	}
	if handled, result, err := r.reconcileDurableACPPlanningFailure(ctx, task); handled || err != nil {
		return result, err
	}
	attempt, err := r.queuedACPPromptAttempt(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	plan := bound.plan
	var delivery acpRuntimeDeliverySelection
	if !externalDispatch {
		delivery, err = r.acpRuntimeDeliveryPlanForTaskAttempt(ctx, task, bound.plan, attempt)
		if err != nil {
			return r.failACPPlanningTask(
				ctx,
				task,
				corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile"),
				fmt.Sprintf("resolve current ACP runtime delivery plan: %v", err),
			)
		}
		plan = delivery.plan
	}
	if err := validateACPWorkspacePreflight(frozenTask); err != nil {
		return r.failACPPlanningTask(ctx, task, corev1alpha1.TaskExecutionReason("InvalidWorkspace"), err.Error())
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	var pool *corev1alpha1.RuntimePool
	poolPreexisting := false
	if !externalDispatch {
		workspaceName, workspaceReady, err := r.ensureACPClassWorkspace(ctx, task, plan)
		if err != nil {
			if errors.Is(err, errACPWorkspaceBindingConflict) {
				return r.failACPPlanningTask(ctx, task, corev1alpha1.TaskExecutionReason("InvalidWorkspace"), err.Error())
			}
			if errors.Is(err, errACPWorkspaceTerminalFailure) {
				return r.failACPPlanningTask(ctx, task, corev1alpha1.TaskExecutionReason("WorkspaceFailed"), err.Error())
			}
			return ctrl.Result{}, err
		}
		if !workspaceReady {
			// The controller-first workspace is not yet admitted, attachable, or
			// exclusively held by this Task; no RuntimePool demand exists yet.
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		runtimeBound := taskExecutionHasRuntimeOrSessionBinding(task.Status.Execution) ||
			promptAttemptHasRuntimeOrSessionBinding(attempt)
		allowCreate := delivery.allowPoolCreation && !runtimeBound
		requiredUID := delivery.requiredRuntimePoolUID
		if runtimeBound {
			execution := task.Status.Execution
			if execution == nil || execution.RuntimePoolName != plan.PoolName || strings.TrimSpace(execution.RuntimePoolUID) == "" {
				return r.failACPPlanningTask(
					ctx,
					task,
					corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile"),
					"the runtime-bound ACP attempt is missing its exact frozen RuntimePool identity",
				)
			}
			requiredUID = types.UID(execution.RuntimePoolUID)
		}
		if !allowCreate && requiredUID == "" {
			return r.failACPPlanningTask(
				ctx,
				task,
				corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile"),
				"the frozen ACP runtime delivery plan requires an exact preexisting RuntimePool identity",
			)
		}
		pool, poolPreexisting, err = r.ensureACPRuntimePoolWithPolicy(
			ctx, task.Namespace, plan, workspaceName,
			task.Annotations[acpExecutionWorkspaceUIDAnnotation], string(task.UID),
			allowCreate, requiredUID,
		)
		if err != nil {
			if errors.Is(err, errACPRuntimeWorkspaceNamespace) {
				return r.failACPPlanningTask(ctx, task, corev1alpha1.TaskExecutionReason("InvalidWorkspace"), err.Error())
			}
			if errors.Is(err, store.ErrValidation) {
				return r.failACPPlanningTask(ctx, task, corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile"), err.Error())
			}
			return ctrl.Result{}, err
		}
	}
	if task.Status.Execution != nil && !taskExecutionStateTerminal(task.Status.Execution.State) {
		if !externalDispatch && (task.Status.Execution.State == corev1alpha1.TaskExecutionStateQueued ||
			task.Status.Execution.State == corev1alpha1.TaskExecutionStateReserved) {
			rebound, rebindErr := r.rebindQueuedACPRuntimeTask(ctx, task, bound, pool)
			if rebindErr != nil {
				return ctrl.Result{}, rebindErr
			}
			if rebound && r.Recorder != nil {
				r.Recorder.Eventf(task, corev1.EventTypeNormal, "ACPRuntimePoolRebound", "Rebound pre-submission attempt %d to replacement RuntimePool %s", task.Status.Execution.Attempt, pool.Name)
			}
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if task.UID == "" {
		return r.failTask(ctx, task, "Task UID is required for ACP prompt identity")
	}
	fence, err := r.ControllerEpochManager.CurrentFence(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	attemptNumber := task.Status.Attempts + 1
	queuedAt := time.Now().UTC()
	promptID := fmt.Sprintf("prompt-%s-%d", task.UID, attemptNumber)
	requestDigest, err := acpBoundTaskRequestDigest(bound, attemptNumber, promptID)
	if err != nil {
		return ctrl.Result{}, err
	}
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: int64(attemptNumber), PromptID: promptID}
	attemptID, err := key.CanonicalID()
	if err != nil {
		return ctrl.Result{}, err
	}
	credentialBindings, credentialVersions, err := resolvePromptCredentialBindings(ctx, reader, frozenTask)
	if err != nil {
		credentialErr := fmt.Errorf("freeze ACP credential bindings: %w", err)
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, credentialErr
		}
		return r.failACPPlanningTask(ctx, task, corev1alpha1.TaskExecutionReason("InvalidWorkspace"), credentialErr.Error())
	}
	attempt, err = r.DurableControlStore.CreatePromptAttempt(ctx, &store.PromptAttempt{
		ID: attemptID, Key: key, RequestDigest: requestDigest,
		BindingDigest: bound.binding.BindingDigest, SnapshotDigest: bound.snapshot.Digest,
		CredentialBindings: credentialBindings,
		ExecutionState:     store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested, CreatedAt: queuedAt,
	}, fence)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("persist ACP prompt attempt: %w", err)
	}

	metadataBase := task.DeepCopy()
	if task.Labels == nil {
		task.Labels = make(map[string]string)
	}
	if task.Annotations == nil {
		task.Annotations = make(map[string]string)
	}
	if externalDispatch {
		task.Annotations[acpExternalRuntimeTaskAnnotation] = externalRuntime.Name
		delete(task.Labels, acpExternalRuntimeTaskAnnotation)
		delete(task.Labels, acpRuntimeTaskPoolLabel)
	} else {
		task.Labels[acpRuntimeTaskPoolLabel] = pool.Name
		delete(task.Labels, acpExternalRuntimeTaskAnnotation)
		delete(task.Annotations, acpExternalRuntimeTaskAnnotation)
	}
	task.Annotations[acpRuntimeQueuedAtAnnotation] = queuedAt.Format(time.RFC3339Nano)
	if err := r.Patch(ctx, task, client.MergeFrom(metadataBase)); err != nil {
		return ctrl.Result{}, err
	}
	statusBase := task.DeepCopy()
	now := metav1.NewTime(queuedAt)
	task.Status.Attempts = attemptNumber
	execution := &corev1alpha1.TaskExecutionStatus{
		State: corev1alpha1.TaskExecutionStateQueued, Attempt: attemptNumber, PromptID: promptID,
		ControllerEpoch:                          fence.Epoch,
		RequestDigest:                            attempt.RequestDigest,
		ReadCredentialResourceVersion:            credentialVersions.SourceRead,
		PublicationReadCredentialResourceVersion: credentialVersions.TargetRead,
		PublicationCredentialResourceVersion:     credentialVersions.TargetWrite,
		ForgeCredentialResourceVersion:           credentialVersions.Forge,
		LastTransitionTime:                       &now,
	}
	if externalDispatch {
		execution.AgentRuntimeName = externalRuntime.Name
		execution.AgentRuntimeUID = string(externalRuntime.UID)
	} else {
		execution.RuntimePoolName = pool.Name
		execution.RuntimePoolUID = string(pool.UID)
	}
	task.Status.Execution = execution
	task.Status.Delivery = &corev1alpha1.TaskDeliveryStatus{
		State: corev1alpha1.TaskDeliveryStateNotRequested, Outcome: corev1alpha1.TaskDeliveryOutcomeNotRequested, LastTransitionTime: &now,
	}
	if plan.Workspace != nil {
		// Provider-neutral projection only: no claim, sandbox, or other
		// provider-native identifier ever enters public Task status.
		task.Status.ExecutionWorkspace = statusrules.Update{
			Provider:      plan.Workspace.Provider,
			Phase:         corev1alpha1.ExecutionWorkspacePhasePending,
			Reason:        corev1alpha1.ExecutionWorkspaceReasonPending,
			ReusePolicy:   plan.Workspace.ReusePolicy,
			CleanupPolicy: plan.Workspace.CleanupPolicy,
			Reused:        acpWorkspaceRuntimePoolReused(pool, poolPreexisting),
			Message:       "RuntimeSession is queued for a workspace-provider-backed RuntimePool",
			ObservedAt:    &now,
		}.Status()
	}
	if err := r.Status().Patch(ctx, task, client.MergeFrom(statusBase)); err != nil {
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		if externalDispatch {
			r.Recorder.Eventf(task, corev1.EventTypeNormal, "ACPTaskQueued", "Queued attempt %d for external AgentRuntime %s", attemptNumber, externalRuntime.Name)
		} else {
			r.Recorder.Eventf(task, corev1.EventTypeNormal, "ACPTaskQueued", "Queued attempt %d for RuntimePool %s", attemptNumber, pool.Name)
		}
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func acpWorkspaceRuntimePoolReused(pool *corev1alpha1.RuntimePool, preexisting bool) bool {
	return preexisting && pool != nil && pool.Spec.ExecutionWorkspace != nil &&
		pool.Status.ActiveInstance != nil && pool.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopped
}

//nolint:gocyclo // Rebinding keeps every queued-attempt fence and optimistic-lock check auditable together.
func (r *TaskReconciler) rebindQueuedACPRuntimeTask(
	ctx context.Context,
	task *corev1alpha1.Task,
	bound *verifiedAgentExecution,
	pool *corev1alpha1.RuntimePool,
) (bool, error) {
	if task == nil || task.Status.Execution == nil ||
		(task.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued &&
			task.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved) ||
		pool == nil || pool.UID == "" {
		return false, nil
	}
	if acpRuntimePoolBindingMatches(task.Status.Execution, pool) {
		return r.patchACPRuntimePoolTaskLabel(ctx, task, pool)
	}
	requestMatches, err := acpQueuedTaskRequestMatchesBinding(bound, task.Status.Execution)
	if err != nil {
		return false, err
	}
	if !requestMatches {
		return false, nil
	}
	attemptID, err := promptAttemptIDFromTask(task)
	if err != nil {
		return false, err
	}
	attempt, err := r.DurableControlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return false, err
	}
	expectedAttemptState := store.PromptExecutionQueued
	if task.Status.Execution.State == corev1alpha1.TaskExecutionStateReserved {
		expectedAttemptState = store.PromptExecutionReserved
	}
	if !queuedPromptAttemptMatchesTask(attempt, task) || attempt.ExecutionState != expectedAttemptState ||
		attempt.DeliveryState != store.PromptDeliveryNotRequested || promptAttemptHasRuntimeOrSessionBinding(attempt) ||
		taskExecutionHasRuntimeOrSessionBinding(task.Status.Execution) {
		return false, nil
	}
	fence, err := r.ControllerEpochManager.CurrentFence(ctx)
	if err != nil {
		return false, err
	}
	if expectedAttemptState == store.PromptExecutionReserved {
		safe, safeErr := r.reservedACPRuntimeTaskRebindSafe(ctx, task, attempt, fence)
		if safeErr != nil {
			return false, safeErr
		}
		if !safe {
			return false, nil
		}
		attempt, err = r.refreshReservedACPRuntimeTaskRebind(ctx, task, attempt, pool, fence)
		if err != nil {
			return false, err
		}
	}

	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	expectedTaskUID := task.UID
	expectedAttempt := task.Status.Execution.Attempt
	expectedPromptID := task.Status.Execution.PromptID
	expectedRequestDigest := task.Status.Execution.RequestDigest
	expectedAttemptVersion := attempt.Version
	rebound := false
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &corev1alpha1.Task{}
		if err := r.taskMetadataReader().Get(ctx, key, current); err != nil {
			return err
		}
		status := current.Status.Execution
		if current.UID != expectedTaskUID || status == nil || status.State != task.Status.Execution.State ||
			status.Attempt != expectedAttempt || status.PromptID != expectedPromptID || status.RequestDigest != expectedRequestDigest ||
			taskExecutionHasRuntimeOrSessionBinding(status) {
			return nil
		}
		if acpRuntimePoolBindingMatches(status, pool) {
			rebound = true
			return nil
		}
		currentAttempt, err := r.DurableControlStore.GetPromptAttempt(ctx, attemptID)
		if err != nil {
			return err
		}
		if currentAttempt.Version != expectedAttemptVersion || currentAttempt.ExecutionState != expectedAttemptState ||
			!queuedPromptAttemptMatchesTask(currentAttempt, current) ||
			currentAttempt.DeliveryState != store.PromptDeliveryNotRequested || promptAttemptHasRuntimeOrSessionBinding(currentAttempt) {
			return nil
		}
		if expectedAttemptState == store.PromptExecutionReserved {
			safe, safeErr := r.reservedACPRuntimeTaskRebindSafe(ctx, current, currentAttempt, fence)
			if safeErr != nil {
				return safeErr
			}
			if !safe {
				return nil
			}
		}
		base := current.DeepCopy()
		status.RuntimePoolName = pool.Name
		status.RuntimePoolUID = string(pool.UID)
		status.ControllerEpoch = fence.Epoch
		status.Reason = ""
		status.Message = ""
		status.LastTransitionTime = nowMeta()
		if err := r.Status().Patch(ctx, current, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		rebound = true
		return nil
	}); err != nil {
		return false, err
	}
	if !rebound {
		return false, nil
	}
	labelPatched, err := r.patchACPRuntimePoolTaskLabel(ctx, task, pool)
	if err != nil {
		return false, err
	}
	return rebound || labelPatched, nil
}

func (r *TaskReconciler) patchACPRuntimePoolTaskLabel(
	ctx context.Context,
	task *corev1alpha1.Task,
	pool *corev1alpha1.RuntimePool,
) (bool, error) {
	if task == nil || pool == nil {
		return false, nil
	}
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	expectedTaskUID := task.UID
	patched := false
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &corev1alpha1.Task{}
		if err := r.taskMetadataReader().Get(ctx, key, current); err != nil {
			return err
		}
		if current.UID != expectedTaskUID || !acpRuntimePoolBindingMatches(current.Status.Execution, pool) {
			return nil
		}
		if current.Labels != nil && current.Labels[acpRuntimeTaskPoolLabel] == pool.Name {
			return nil
		}
		base := current.DeepCopy()
		if current.Labels == nil {
			current.Labels = make(map[string]string)
		}
		current.Labels[acpRuntimeTaskPoolLabel] = pool.Name
		if err := r.Patch(ctx, current, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		patched = true
		return nil
	}); err != nil {
		return false, err
	}
	return patched, nil
}

func taskExecutionHasRuntimeOrSessionBinding(status *corev1alpha1.TaskExecutionStatus) bool {
	return status != nil && (strings.TrimSpace(status.RuntimeInstanceID) != "" ||
		strings.TrimSpace(status.RuntimeSessionUID) != "" || status.RuntimeSessionGeneration != 0 ||
		strings.TrimSpace(status.RuntimeSessionSupervisorBootID) != "" ||
		strings.TrimSpace(status.RuntimeSessionProfileDigest) != "" || strings.TrimSpace(status.RuntimeSessionMCPDigest) != "" ||
		strings.TrimSpace(status.RuntimeSessionWorkspaceDigest) != "" || status.RuntimeSessionRecreationPending)
}

func (r *TaskReconciler) queuedACPPromptAttempt(
	ctx context.Context,
	task *corev1alpha1.Task,
) (*store.PromptAttempt, error) {
	if task == nil || task.Status.Execution == nil || task.Status.Execution.Attempt < 1 ||
		(task.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued &&
			task.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved) {
		return nil, nil
	}
	attemptID, err := promptAttemptIDFromTask(task)
	if err != nil {
		return nil, err
	}
	attempt, err := r.DurableControlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return nil, fmt.Errorf("load ACP PromptAttempt before delivery-plan selection: %w", err)
	}
	if !queuedPromptAttemptMatchesTask(attempt, task) {
		return nil, fmt.Errorf("%w: ACP PromptAttempt does not match the queued Task", store.ErrConflict)
	}
	return attempt, nil
}

func acpRuntimeDeliveryPlanForAttempt(
	plan ACPRuntimePlan,
	execution *corev1alpha1.TaskExecutionStatus,
	attempt *store.PromptAttempt,
	images ACPRuntimeImages,
	selectedPool *corev1alpha1.RuntimePool,
) (acpRuntimeDeliverySelection, error) {
	if taskExecutionHasRuntimeOrSessionBinding(execution) || promptAttemptHasRuntimeOrSessionBinding(attempt) {
		deliveryPlan, err := acpRuntimeDeliveryPlanForBoundPool(plan, execution, selectedPool)
		if err != nil {
			return acpRuntimeDeliverySelection{}, err
		}
		return acpRuntimeDeliverySelection{plan: deliveryPlan, requiredRuntimePoolUID: selectedPool.UID}, nil
	}
	delivery, err := currentACPRuntimeDeliveryPlan(plan, images)
	if err != nil {
		return acpRuntimeDeliverySelection{}, err
	}
	return acpRuntimeDeliverySelectionForPreexistingPool(delivery, selectedPool)
}

func (r *TaskReconciler) acpRuntimeDeliveryPlanForTaskAttempt(
	ctx context.Context,
	task *corev1alpha1.Task,
	plan ACPRuntimePlan,
	attempt *store.PromptAttempt,
) (acpRuntimeDeliverySelection, error) {
	if !taskExecutionHasRuntimeOrSessionBinding(task.Status.Execution) && !promptAttemptHasRuntimeOrSessionBinding(attempt) {
		delivery, err := currentACPRuntimeDeliveryPlan(plan, r.ACPRuntimeImages)
		if err != nil || delivery.allowPoolCreation {
			return delivery, err
		}
		pool := &corev1alpha1.RuntimePool{}
		key := types.NamespacedName{Namespace: task.Namespace, Name: delivery.plan.PoolName}
		if err := r.taskMetadataReader().Get(ctx, key, pool); err != nil {
			return acpRuntimeDeliverySelection{}, fmt.Errorf("load the frozen workspace ACP RuntimePool required for historical image delivery: %w", err)
		}
		if execution := task.Status.Execution; execution != nil &&
			(strings.TrimSpace(execution.RuntimePoolName) != "" || strings.TrimSpace(execution.RuntimePoolUID) != "") &&
			(execution.RuntimePoolName != pool.Name || strings.TrimSpace(execution.RuntimePoolUID) != string(pool.UID)) {
			return acpRuntimeDeliverySelection{}, errors.New("the pre-submission ACP attempt does not match the exact historical workspace RuntimePool identity")
		}
		return acpRuntimeDeliverySelectionForPreexistingPool(delivery, pool)
	}
	execution := task.Status.Execution
	if execution == nil || strings.TrimSpace(execution.RuntimePoolName) == "" {
		return acpRuntimeDeliverySelection{}, errors.New("runtime-bound ACP attempt is missing its selected RuntimePool name")
	}
	pool := &corev1alpha1.RuntimePool{}
	key := types.NamespacedName{Namespace: task.Namespace, Name: strings.TrimSpace(execution.RuntimePoolName)}
	if err := r.taskMetadataReader().Get(ctx, key, pool); err != nil {
		return acpRuntimeDeliverySelection{}, fmt.Errorf("load the runtime-bound ACP attempt's selected RuntimePool: %w", err)
	}
	return acpRuntimeDeliveryPlanForAttempt(plan, execution, attempt, r.ACPRuntimeImages, pool)
}

func acpRuntimeDeliverySelectionForPreexistingPool(
	delivery acpRuntimeDeliverySelection,
	pool *corev1alpha1.RuntimePool,
) (acpRuntimeDeliverySelection, error) {
	if delivery.allowPoolCreation {
		return delivery, nil
	}
	if delivery.plan.Workspace == nil {
		return acpRuntimeDeliverySelection{}, errors.New("ACP runtime delivery forbids pool creation without a frozen workspace plan")
	}
	selected, err := acpRuntimeDeliveryPlanForSelectedPool(delivery.plan, pool)
	if err != nil {
		return acpRuntimeDeliverySelection{}, fmt.Errorf("validate the exact preexisting workspace ACP RuntimePool: %w", err)
	}
	delivery.plan = selected
	delivery.requiredRuntimePoolUID = pool.UID
	return delivery, nil
}

func acpRuntimeDeliveryPlanForBoundPool(
	plan ACPRuntimePlan,
	execution *corev1alpha1.TaskExecutionStatus,
	pool *corev1alpha1.RuntimePool,
) (ACPRuntimePlan, error) {
	if execution == nil || pool == nil {
		return ACPRuntimePlan{}, errors.New("runtime-bound ACP attempt and selected RuntimePool are required")
	}
	poolName := strings.TrimSpace(execution.RuntimePoolName)
	poolUID := strings.TrimSpace(execution.RuntimePoolUID)
	if poolName == "" || poolUID == "" || pool.Name != poolName || string(pool.UID) != poolUID {
		return ACPRuntimePlan{}, errors.New("runtime-bound ACP attempt does not match its exact selected RuntimePool identity")
	}
	return acpRuntimeDeliveryPlanForSelectedPool(plan, pool)
}

func acpRuntimeDeliveryPlanForSelectedPool(
	plan ACPRuntimePlan,
	pool *corev1alpha1.RuntimePool,
) (ACPRuntimePlan, error) {
	if pool == nil || pool.UID == "" {
		return ACPRuntimePlan{}, errors.New("selected ACP RuntimePool identity is required")
	}
	if !pool.DeletionTimestamp.IsZero() {
		return ACPRuntimePlan{}, errors.New("selected ACP RuntimePool is deleting")
	}
	if err := validateRuntimePoolImageReference(pool); err != nil {
		return ACPRuntimePlan{}, fmt.Errorf("validate selected ACP RuntimePool image: %w", err)
	}
	if _, _, err := validateRuntimePoolProfile(pool); err != nil {
		return ACPRuntimePlan{}, fmt.Errorf("validate selected ACP RuntimePool profile: %w", err)
	}
	if pool.Spec.Runtime.Profile.Digest != string(plan.Digest) {
		return ACPRuntimePlan{}, errors.New("selected ACP RuntimePool profile does not match the immutable execution snapshot")
	}
	if !acpRuntimePoolWorkspaceMatchesPlan(pool, plan) {
		return ACPRuntimePlan{}, errors.New("selected ACP RuntimePool workspace does not match the immutable execution snapshot")
	}
	if plan.Workspace != nil {
		if pool.Name != plan.PoolName || strings.TrimSpace(pool.Spec.Runtime.Image) != plan.Image {
			return ACPRuntimePlan{}, errors.New("selected workspace RuntimePool identity or image does not match the immutable execution snapshot")
		}
	} else {
		identity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
			acpRuntimePoolIdentityProfileDigestKey: string(plan.Digest),
			acpRuntimePoolIdentityRuntimeImageKey:  strings.TrimSpace(pool.Spec.Runtime.Image),
		})
		if err != nil {
			return ACPRuntimePlan{}, err
		}
		if pool.Name != acpRuntimePoolName(plan.Profile.ProviderKind, harnessv2.ProfileDigest(identity)) {
			return ACPRuntimePlan{}, errors.New("selected ACP RuntimePool name does not match its immutable image and profile")
		}
	}
	plan.PoolName = pool.Name
	plan.Image = strings.TrimSpace(pool.Spec.Runtime.Image)
	return plan, nil
}

func promptAttemptHasRuntimeOrSessionBinding(attempt *store.PromptAttempt) bool {
	return attempt != nil && (strings.TrimSpace(attempt.RuntimeInstanceID) != "" ||
		strings.TrimSpace(attempt.SessionUID) != "" || attempt.SessionLeaseGeneration != 0)
}

func (r *TaskReconciler) refreshReservedACPRuntimeTaskRebind(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
	pool *corev1alpha1.RuntimePool,
	fence store.ControllerEpochFence,
) (*store.PromptAttempt, error) {
	if task == nil || task.Status.Execution == nil || attempt == nil || pool == nil {
		return nil, fmt.Errorf("reserved RuntimePool rebind identity is incomplete")
	}
	operationID := store.CanonicalControlID(
		"rebind-reserved-runtime-pool", attempt.ID, task.Status.Execution.RuntimePoolUID, string(pool.UID),
	)
	operationDigest, err := acpDomainDigest("reserved-runtime-pool-rebind", map[string]any{
		"attemptID": attempt.ID, "requestDigest": attempt.RequestDigest,
		"fromPoolUID": task.Status.Execution.RuntimePoolUID, "toPoolUID": string(pool.UID),
		"controllerEpoch": fence.Epoch,
	})
	if err != nil {
		return nil, err
	}
	return r.DurableControlStore.RecoverPromptAttemptPreSubmission(ctx, store.PromptAttemptPreSubmissionRecovery{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: store.PromptExecutionReserved,
		OperationID: operationID, OperationDigest: operationDigest, RecoveredAt: time.Now().UTC(),
	})
}

func (r *TaskReconciler) reservedACPRuntimeTaskRebindSafe(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
	fence store.ControllerEpochFence,
) (bool, error) {
	if task == nil || task.Status.Execution == nil || attempt == nil ||
		task.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved ||
		attempt.ExecutionState != store.PromptExecutionReserved || taskExecutionHasRuntimeOrSessionBinding(task.Status.Execution) ||
		promptAttemptHasRuntimeOrSessionBinding(attempt) || task.Status.Execution.ControllerEpoch != fence.Epoch ||
		attempt.ControllerEpoch != fence.Epoch || attempt.ControllerEpochName != fence.Name {
		return false, nil
	}
	oldPool := &corev1alpha1.RuntimePool{}
	key := types.NamespacedName{Namespace: task.Namespace, Name: strings.TrimSpace(task.Status.Execution.RuntimePoolName)}
	if err := r.taskMetadataReader().Get(ctx, key, oldPool); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if string(oldPool.UID) != strings.TrimSpace(task.Status.Execution.RuntimePoolUID) {
		return true, nil
	}
	if oldPool.Status.Lifecycle == corev1alpha1.RuntimePoolLifecycleServing &&
		oldPool.Status.AdmissionState == corev1alpha1.RuntimePoolAdmissionAccepting {
		return false, nil
	}
	now := time.Now().UTC()
	for i := range oldPool.Status.Capacity.Reservations {
		reservation := oldPool.Status.Capacity.Reservations[i]
		if reservation.PoolUID == string(oldPool.UID) && reservation.TaskUID == string(task.UID) &&
			reservation.Attempt == task.Status.Execution.Attempt && reservation.ControllerEpoch == fence.Epoch &&
			reservation.ExpiresAt.After(now) {
			return false, nil
		}
	}
	return true, nil
}

func queuedPromptAttemptMatchesTask(attempt *store.PromptAttempt, task *corev1alpha1.Task) bool {
	return attempt != nil && task != nil && task.Status.Execution != nil &&
		attempt.Key.Namespace == task.Namespace && attempt.Key.TaskUID == string(task.UID) &&
		attempt.Key.Attempt == int64(task.Status.Execution.Attempt) && attempt.Key.PromptID == task.Status.Execution.PromptID &&
		attempt.RequestDigest == task.Status.Execution.RequestDigest
}

type acpTaskQueueRank struct {
	promoted          bool
	effectivePriority int32
	queuedAt          time.Time
	createdAt         time.Time
	namespace         string
	name              string
	uid               string
}

func sortACPTasksByQueuePriority(tasks []*corev1alpha1.Task, now time.Time) {
	sortTasksByQueuePriority(tasks, now, acpTaskQueuedAt)
}

func sortTasksByQueuePriority(
	tasks []*corev1alpha1.Task,
	now time.Time,
	queuedAt func(*corev1alpha1.Task) time.Time,
) {
	now = now.UTC()
	ranks := make(map[*corev1alpha1.Task]acpTaskQueueRank, len(tasks))
	for _, task := range tasks {
		ranks[task] = rankTaskForQueue(task, now, queuedAt(task))
	}
	sort.SliceStable(tasks, func(i, j int) bool {
		return taskQueueRankLess(ranks[tasks[i]], ranks[tasks[j]])
	})
}

func rankTaskForQueue(task *corev1alpha1.Task, now, queuedAt time.Time) acpTaskQueueRank {
	age := max(now.Sub(queuedAt), 0)
	priority := defaultACPTaskPriority
	if task != nil && task.Spec.Priority != nil {
		priority = *task.Spec.Priority
	}
	agingSteps := int64(age / DefaultACPQueueAgingInterval)
	agedPriority := min(int64(priority)+agingSteps*int64(defaultACPQueueAgingStep), 1000)
	createdAt := queuedAt
	var namespace, name, uid string
	if task != nil {
		if !task.CreationTimestamp.IsZero() {
			createdAt = task.CreationTimestamp.UTC()
		}
		namespace, name, uid = task.Namespace, task.Name, string(task.UID)
	}
	return acpTaskQueueRank{
		promoted: age >= DefaultACPQueueMaximumWait, effectivePriority: int32(agedPriority),
		queuedAt: queuedAt, createdAt: createdAt, namespace: namespace, name: name, uid: uid,
	}
}

func taskQueueRankLess(left, right acpTaskQueueRank) bool {
	if left.promoted != right.promoted {
		return left.promoted
	}
	if !left.promoted && left.effectivePriority != right.effectivePriority {
		return left.effectivePriority > right.effectivePriority
	}
	if !left.queuedAt.Equal(right.queuedAt) {
		return left.queuedAt.Before(right.queuedAt)
	}
	if !left.createdAt.Equal(right.createdAt) {
		return left.createdAt.Before(right.createdAt)
	}
	if left.namespace != right.namespace {
		return left.namespace < right.namespace
	}
	if left.name != right.name {
		return left.name < right.name
	}
	return left.uid < right.uid
}

func acpTaskQueuedAt(task *corev1alpha1.Task) time.Time {
	if task == nil {
		return time.Unix(0, 0).UTC()
	}
	if value := strings.TrimSpace(task.Annotations[acpRuntimeQueuedAtAnnotation]); value != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return parsed.UTC()
		}
	}
	if task.Status.Execution != nil && task.Status.Execution.LastTransitionTime != nil && !task.Status.Execution.LastTransitionTime.IsZero() {
		return task.Status.Execution.LastTransitionTime.UTC()
	}
	if !task.CreationTimestamp.IsZero() {
		return task.CreationTimestamp.UTC()
	}
	return time.Unix(0, 0).UTC()
}

func (r *TaskReconciler) cancelACPTaskBeforeDurableAttempt(ctx context.Context, task *corev1alpha1.Task, message string) (ctrl.Result, error) {
	reader := r.taskMetadataReader()
	latest := &corev1alpha1.Task{}
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	if err := reader.Get(ctx, key, latest); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	nowUTC := time.Now().UTC()
	deadline, expired := r.pendingAgentTaskDeadline(ctx, latest, nowUTC)
	if latest.UID != task.UID || latest.Spec.Type != corev1alpha1.TaskTypeAgent || !expired || nowUTC.Before(deadline) {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if latest.Status.Execution != nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	var attempt *store.PromptAttempt
	if r.DurableControlStore != nil && latest.UID != "" {
		attemptNumber := latest.Status.Attempts + 1
		promptID := fmt.Sprintf("prompt-%s-%d", latest.UID, attemptNumber)
		attemptKey := store.PromptAttemptKey{
			Namespace: latest.Namespace, TaskUID: string(latest.UID), Attempt: int64(attemptNumber), PromptID: promptID,
		}
		attemptID, err := attemptKey.CanonicalID()
		if err != nil {
			return ctrl.Result{}, err
		}
		attempt, err = r.DurableControlStore.GetPromptAttempt(ctx, attemptID)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return ctrl.Result{}, err
		}
		if attempt != nil {
			if attempt.ExecutionState == store.PromptExecutionQueued {
				if r.ControllerEpochManager == nil {
					return ctrl.Result{}, fmt.Errorf("controller epoch manager is required to cancel queued ACP attempt")
				}
				fence, fenceErr := r.ControllerEpochManager.CurrentFence(ctx)
				if fenceErr != nil {
					return ctrl.Result{}, fenceErr
				}
				operationID := "timeout-before-status-" + fmt.Sprint(attempt.Version)
				digest, digestErr := acpDomainDigest("attempt-transition", map[string]any{
					"id": attempt.ID, "from": attempt.ExecutionState, "to": store.PromptExecutionCancelled,
					"operation": operationID, "version": attempt.Version,
				})
				if digestErr != nil {
					return ctrl.Result{}, digestErr
				}
				attempt, err = r.DurableControlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
					ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: store.PromptExecutionQueued,
					NewState: store.PromptExecutionCancelled, OperationID: operationID, OperationDigest: digest,
					TerminalReason: acpTaskTimeoutReason, OutcomeMarker: message, UpdatedAt: time.Now().UTC(),
				})
				if err != nil {
					return ctrl.Result{}, err
				}
			} else if attempt.ExecutionState != store.PromptExecutionCancelled {
				return ctrl.Result{}, fmt.Errorf("prompt attempt %q is %s while Task execution status is empty", attempt.ID, attempt.ExecutionState)
			}
		}
	}

	now := metav1.Now()
	statusBound := false
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &corev1alpha1.Task{}
		if err := reader.Get(ctx, key, current); err != nil {
			return err
		}
		currentNow := time.Now().UTC()
		currentDeadline, currentHasDeadline := r.pendingAgentTaskDeadline(ctx, current, currentNow)
		if current.UID != task.UID || current.Spec.Type != corev1alpha1.TaskTypeAgent ||
			!currentHasDeadline || currentNow.Before(currentDeadline) {
			statusBound = true
			return nil
		}
		if current.Status.Execution != nil {
			statusBound = true
			return nil
		}
		base := current.DeepCopy()
		current.Status.Phase = corev1alpha1.TaskPhaseCancelled
		current.Status.CompletionTime = &now
		current.Status.Message = message
		current.Status.Execution = &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateCancelled, Outcome: corev1alpha1.TaskExecutionOutcomeCancelled,
			Reason: acpTaskTimeoutReason, Message: message, LastTransitionTime: &now,
		}
		if attempt != nil {
			if current.Status.Attempts < int32(attempt.Key.Attempt) {
				current.Status.Attempts = int32(attempt.Key.Attempt)
			}
			current.Status.Execution.Attempt = int32(attempt.Key.Attempt)
			current.Status.Execution.PromptID = attempt.Key.PromptID
			current.Status.Execution.RequestDigest = attempt.RequestDigest
			current.Status.Execution.ControllerEpoch = attempt.ControllerEpoch
		}
		current.Status.Delivery = &corev1alpha1.TaskDeliveryStatus{
			State: corev1alpha1.TaskDeliveryStateNotRequested, Outcome: corev1alpha1.TaskDeliveryOutcomeNotRequested,
			LastTransitionTime: &now,
		}
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type: ConditionTypeWaitingForApproval, Status: metav1.ConditionFalse, LastTransitionTime: now,
			Reason: "TaskCancelled", Message: "task is terminal",
		})
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type: ConditionTypeComplete, Status: metav1.ConditionTrue, LastTransitionTime: now,
			Reason: "TaskCancelled", Message: message,
		})
		return r.Status().Patch(ctx, current, client.MergeFrom(base))
	}); err != nil {
		return ctrl.Result{}, err
	}
	if statusBound {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *TaskReconciler) failACPPlanningTask(
	ctx context.Context,
	task *corev1alpha1.Task,
	reason corev1alpha1.TaskExecutionReason,
	message string,
) (ctrl.Result, error) {
	attempt, err := r.settleACPPlanningFailureAttempt(ctx, task, reason, message)
	if errors.Is(err, store.ErrNotReady) {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if attempt != nil {
		canonicalReason := corev1alpha1.TaskExecutionReason(attempt.TerminalReason)
		canonicalMessage := strings.TrimSpace(attempt.OutcomeMarker)
		if canonicalReason != reason || canonicalMessage == "" {
			return ctrl.Result{}, fmt.Errorf("%w: settled ACP planning failure marker is inconsistent", store.ErrConflict)
		}
		reason = canonicalReason
		message = canonicalMessage
		if err := r.enqueueACPPlanningFailureProjection(ctx, task, attempt, reason, message); err != nil {
			return ctrl.Result{}, err
		}
	}
	return r.projectACPPlanningFailureTask(ctx, task, attempt, reason, message)
}

func (r *TaskReconciler) reconcileDurableACPPlanningFailure(
	ctx context.Context,
	task *corev1alpha1.Task,
) (bool, ctrl.Result, error) {
	if task == nil || task.Status.Execution == nil || task.Status.Execution.Attempt < 1 ||
		(task.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued &&
			task.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved) {
		return false, ctrl.Result{}, nil
	}
	attemptID, err := promptAttemptIDFromTask(task)
	if err != nil {
		return false, ctrl.Result{}, err
	}
	attempt, err := r.DurableControlStore.GetPromptAttempt(ctx, attemptID)
	if errors.Is(err, store.ErrNotFound) {
		return false, ctrl.Result{}, nil
	}
	if err != nil {
		return false, ctrl.Result{}, err
	}
	if attempt.ExecutionState != store.PromptExecutionFailed {
		return false, ctrl.Result{}, nil
	}
	reason := corev1alpha1.TaskExecutionReason(attempt.TerminalReason)
	if reason != corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile") &&
		reason != corev1alpha1.TaskExecutionReason("InvalidWorkspace") {
		return false, ctrl.Result{}, nil
	}
	message := strings.TrimSpace(attempt.OutcomeMarker)
	if message == "" || !queuedPromptAttemptMatchesTask(attempt, task) {
		return true, ctrl.Result{}, fmt.Errorf("%w: durable ACP planning failure is missing its canonical Task projection", store.ErrConflict)
	}
	if err := r.enqueueACPPlanningFailureProjection(ctx, task, attempt, reason, message); err != nil {
		return true, ctrl.Result{}, err
	}
	result, err := r.projectACPPlanningFailureTask(ctx, task, attempt, reason, message)
	return true, result, err
}

func (r *TaskReconciler) projectACPPlanningFailureTask(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
	reason corev1alpha1.TaskExecutionReason,
	message string,
) (ctrl.Result, error) {
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest := &corev1alpha1.Task{}
		if err := r.taskMetadataReader().Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if latest.UID != task.UID {
			return fmt.Errorf("%w: Task identity changed while projecting ACP planning failure", store.ErrConflict)
		}
		base := latest.DeepCopy()
		terminalTime := metav1.Now()
		if attempt != nil {
			terminalTime = metav1.NewTime(attempt.UpdatedAt.UTC())
		} else if latest.Status.Execution != nil && latest.Status.Execution.State == corev1alpha1.TaskExecutionStateFailed &&
			latest.Status.Execution.Reason == reason && latest.Status.Execution.LastTransitionTime != nil {
			terminalTime = *latest.Status.Execution.LastTransitionTime
		}
		if attempt == nil {
			execution := latest.Status.Execution
			switch {
			case execution == nil:
				latest.Status.Execution = &corev1alpha1.TaskExecutionStatus{
					State: corev1alpha1.TaskExecutionStateFailed, Outcome: corev1alpha1.TaskExecutionOutcomeFailed,
					Reason: reason, Message: message, LastTransitionTime: &terminalTime,
				}
			case execution.State == corev1alpha1.TaskExecutionStateFailed && execution.Reason == reason &&
				execution.Attempt == 0 && execution.PromptID == "" && execution.RequestDigest == "":
				execution.Outcome = corev1alpha1.TaskExecutionOutcomeFailed
				execution.Message = message
				execution.LastTransitionTime = &terminalTime
			default:
				return fmt.Errorf("%w: Task acquired durable execution identity while projecting ACP planning failure", store.ErrConflict)
			}
		} else {
			execution := latest.Status.Execution
			if execution == nil || !queuedPromptAttemptMatchesTask(attempt, latest) {
				return fmt.Errorf("%w: Task execution identity changed after ACP planning failure settlement", store.ErrConflict)
			}
			if execution.State != corev1alpha1.TaskExecutionStateQueued &&
				execution.State != corev1alpha1.TaskExecutionStateReserved &&
				(execution.State != corev1alpha1.TaskExecutionStateFailed || execution.Reason != reason) {
				return fmt.Errorf("%w: Task execution advanced to %s after ACP planning failure settlement", store.ErrConflict, execution.State)
			}
			execution.State = corev1alpha1.TaskExecutionStateFailed
			execution.Outcome = corev1alpha1.TaskExecutionOutcomeFailed
			execution.Reason = reason
			execution.Message = message
			execution.ControllerEpoch = attempt.ControllerEpoch
			execution.LastTransitionTime = &terminalTime
		}
		if latest.Status.Delivery == nil {
			latest.Status.Delivery = &corev1alpha1.TaskDeliveryStatus{
				State: corev1alpha1.TaskDeliveryStateNotRequested, Outcome: corev1alpha1.TaskDeliveryOutcomeNotRequested,
				LastTransitionTime: &terminalTime,
			}
		} else if latest.Status.Delivery.State != corev1alpha1.TaskDeliveryStateNotRequested ||
			latest.Status.Delivery.Outcome != corev1alpha1.TaskDeliveryOutcomeNotRequested {
			return fmt.Errorf("%w: ACP planning failure cannot replace delivery state %s", store.ErrConflict, latest.Status.Delivery.State)
		} else {
			latest.Status.Delivery.LastTransitionTime = &terminalTime
		}
		latest.Status.Phase = corev1alpha1.TaskPhaseFailed
		if attempt != nil || latest.Status.CompletionTime == nil {
			latest.Status.CompletionTime = &terminalTime
		}
		latest.Status.Message = message
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: ConditionTypeWaitingForApproval, Status: metav1.ConditionFalse, LastTransitionTime: terminalTime,
			Reason: "TaskFailed", Message: "task is terminal",
		})
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: ConditionTypeComplete, Status: metav1.ConditionFalse, LastTransitionTime: terminalTime,
			Reason: "TaskFailed", Message: message,
		})
		return r.Status().Patch(ctx, latest, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TaskReconciler) enqueueACPPlanningFailureProjection(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
	reason corev1alpha1.TaskExecutionReason,
	message string,
) error {
	if task == nil || attempt == nil || r.DurableControlStore == nil || r.ControllerEpochManager == nil {
		return fmt.Errorf("ACP planning failure projection dependencies are incomplete")
	}
	latest := &corev1alpha1.Task{}
	if err := r.taskMetadataReader().Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, latest); err != nil {
		return client.IgnoreNotFound(err)
	}
	if latest.UID != task.UID || latest.Status.Execution == nil || !queuedPromptAttemptMatchesTask(attempt, latest) ||
		attempt.ExecutionState != store.PromptExecutionFailed || attempt.TerminalReason != string(reason) {
		return fmt.Errorf("%w: ACP planning failure projection does not match its Task and PromptAttempt", store.ErrConflict)
	}
	state := latest.Status.Execution.State
	if state != corev1alpha1.TaskExecutionStateQueued && state != corev1alpha1.TaskExecutionStateReserved &&
		(state != corev1alpha1.TaskExecutionStateFailed || latest.Status.Execution.Reason != reason) {
		return fmt.Errorf("%w: Task execution advanced to %s before ACP planning failure projection", store.ErrConflict, state)
	}
	execution := *latest.Status.Execution
	execution.State = corev1alpha1.TaskExecutionStateFailed
	execution.Outcome = corev1alpha1.TaskExecutionOutcomeFailed
	execution.Reason = reason
	execution.Message = message
	execution.ControllerEpoch = attempt.ControllerEpoch
	transitionTime := metav1.NewTime(attempt.UpdatedAt.UTC())
	execution.LastTransitionTime = &transitionTime
	delivery := latest.Status.Delivery
	if delivery == nil {
		delivery = &corev1alpha1.TaskDeliveryStatus{
			State: corev1alpha1.TaskDeliveryStateNotRequested, Outcome: corev1alpha1.TaskDeliveryOutcomeNotRequested,
			LastTransitionTime: &transitionTime,
		}
	} else if delivery.State != corev1alpha1.TaskDeliveryStateNotRequested ||
		delivery.Outcome != corev1alpha1.TaskDeliveryOutcomeNotRequested {
		return fmt.Errorf("%w: ACP planning failure projection cannot replace delivery state %s", store.ErrConflict, delivery.State)
	} else {
		copyDelivery := *delivery
		copyDelivery.LastTransitionTime = &transitionTime
		delivery = &copyDelivery
	}
	fence, err := r.ControllerEpochManager.CurrentFence(ctx)
	if err != nil {
		return err
	}
	return enqueueDurableTaskTerminalProjection(ctx, r.DurableControlStore, fence, latest, taskTerminalProjection{
		Namespace: latest.Namespace, Task: latest.Name, TaskUID: string(latest.UID), Attempt: int32(attempt.Key.Attempt),
		Phase: corev1alpha1.TaskPhaseFailed, Message: message, Execution: execution, Delivery: delivery,
	})
}

func (r *TaskReconciler) reservedACPPlanningFailureSafe(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
) (bool, error) {
	if task == nil || task.Status.Execution == nil || attempt == nil ||
		taskExecutionHasRuntimeOrSessionBinding(task.Status.Execution) || promptAttemptHasRuntimeOrSessionBinding(attempt) {
		return false, nil
	}
	poolName := strings.TrimSpace(task.Status.Execution.RuntimePoolName)
	poolUID := strings.TrimSpace(task.Status.Execution.RuntimePoolUID)
	if poolName == "" || poolUID == "" {
		return false, nil
	}
	pool := &corev1alpha1.RuntimePool{}
	if err := r.taskMetadataReader().Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: poolName}, pool); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if string(pool.UID) != poolUID {
		return true, nil
	}
	now := time.Now().UTC()
	for i := range pool.Status.Capacity.Reservations {
		reservation := pool.Status.Capacity.Reservations[i]
		if reservation.PoolUID == poolUID && reservation.TaskUID == string(task.UID) &&
			reservation.Attempt == task.Status.Execution.Attempt && reservation.ExpiresAt.After(now) {
			return false, nil
		}
	}
	return true, nil
}

func (r *TaskReconciler) settleACPPlanningFailureAttempt(
	ctx context.Context,
	task *corev1alpha1.Task,
	reason corev1alpha1.TaskExecutionReason,
	message string,
) (*store.PromptAttempt, error) {
	if task == nil {
		return nil, nil
	}
	latest := &corev1alpha1.Task{}
	if err := r.taskMetadataReader().Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, latest); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if latest.UID != task.UID {
		return nil, fmt.Errorf("%w: Task identity changed while settling ACP planning failure", store.ErrConflict)
	}
	execution := latest.Status.Execution
	if execution == nil {
		return nil, nil
	}
	projectedFailure := execution.State == corev1alpha1.TaskExecutionStateFailed && execution.Reason == reason
	if execution.State != corev1alpha1.TaskExecutionStateQueued &&
		execution.State != corev1alpha1.TaskExecutionStateReserved && !projectedFailure {
		return nil, fmt.Errorf("%w: cannot settle ACP planning failure from Task execution state %s", store.ErrConflict, execution.State)
	}
	if projectedFailure && execution.Attempt == 0 {
		return nil, nil
	}
	attemptID, err := promptAttemptIDFromTask(latest)
	if err != nil {
		return nil, err
	}
	fence, err := r.ControllerEpochManager.CurrentFence(ctx)
	if err != nil {
		return nil, err
	}
	for range 3 {
		attempt, getErr := r.DurableControlStore.GetPromptAttempt(ctx, attemptID)
		if getErr != nil {
			return nil, getErr
		}
		if !queuedPromptAttemptMatchesTask(attempt, latest) || attempt.DeliveryState != store.PromptDeliveryNotRequested {
			return nil, fmt.Errorf("%w: durable PromptAttempt does not match the Task planning-failure projection", store.ErrConflict)
		}
		if attempt.ExecutionState == store.PromptExecutionFailed && attempt.TerminalReason == string(reason) {
			return attempt, nil
		}
		if attempt.ExecutionState != store.PromptExecutionQueued && attempt.ExecutionState != store.PromptExecutionReserved {
			return nil, fmt.Errorf("%w: durable PromptAttempt advanced to %s before planning failure settlement", store.ErrConflict, attempt.ExecutionState)
		}
		if attempt.ExecutionState == store.PromptExecutionReserved {
			safe, safeErr := r.reservedACPPlanningFailureSafe(ctx, latest, attempt)
			if safeErr != nil {
				return nil, safeErr
			}
			if !safe {
				return nil, fmt.Errorf("%w: reserved ACP attempt still owns live RuntimePool capacity", store.ErrNotReady)
			}
		}
		operationID := store.CanonicalControlID("fail-acp-planning", attempt.ID, fmt.Sprint(attempt.Version), string(reason))
		operationDigest, digestErr := acpDomainDigest("planning-failure-attempt-transition", map[string]any{
			"attemptID": attempt.ID, "requestDigest": attempt.RequestDigest, "from": attempt.ExecutionState,
			"to": store.PromptExecutionFailed, "version": attempt.Version, "reason": reason, "message": message,
		})
		if digestErr != nil {
			return nil, digestErr
		}
		settled, transitionErr := r.DurableControlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: store.PromptExecutionFailed, OperationID: operationID, OperationDigest: operationDigest,
			TerminalReason: string(reason), OutcomeMarker: message, UpdatedAt: time.Now().UTC(),
		})
		if errors.Is(transitionErr, store.ErrConflict) {
			continue
		}
		return settled, transitionErr
	}
	return nil, fmt.Errorf("%w: durable PromptAttempt changed repeatedly during planning failure settlement", store.ErrConflict)
}

//nolint:gocyclo // Workspace preflight intentionally keeps all fail-closed publication gates together.
func validateACPWorkspacePreflight(task *corev1alpha1.Task) error {
	if task == nil {
		return nil
	}
	intent := effectiveACPWorkspaceIntent(task)
	workspace := task.Spec.Workspace
	if workspace == nil {
		if intent == corev1alpha1.WorkspaceIntentWrite {
			return fmt.Errorf("write workspace intent requires Task.spec.workspace")
		}
		return nil
	}
	if strings.TrimSpace(workspace.GitRepo) == "" {
		switch {
		case strings.TrimSpace(workspace.Branch) != "":
			return fmt.Errorf("branch requires gitRepo")
		case strings.TrimSpace(workspace.Ref) != "":
			return fmt.Errorf("ref requires gitRepo")
		case strings.TrimSpace(workspace.SubPath) != "":
			return fmt.Errorf("subPath requires gitRepo")
		case workspace.SourceRepository != nil:
			return fmt.Errorf("sourceRepository requires gitRepo")
		case workspace.ReadCredentialRef != nil:
			return fmt.Errorf("readCredentialRef requires gitRepo")
		}
	} else {
		if _, err := workspaceRepository(workspace); err != nil {
			return err
		}
	}
	// Apply the RuntimeSession relative-root rule here so an unsafe subPath
	// fails preflight before repository resolution and archive preparation
	// spend SCM and artifact work on a Task session creation must reject.
	if err := harnessv2.ValidateWorkspaceRelativeRoot(workspace.SubPath); err != nil {
		return fmt.Errorf("subPath is invalid: %w", err)
	}
	if workspace.MaxChangedFiles != nil && *workspace.MaxChangedFiles <= 0 {
		return fmt.Errorf("maxChangedFiles must be positive when configured")
	}
	for _, pattern := range workspace.AllowedPaths {
		cleaned := strings.TrimPrefix(strings.TrimSpace(pattern), "./")
		if cleaned == "" || len(cleaned) > 1024 || strings.ContainsAny(cleaned, "\\\x00") || strings.HasPrefix(cleaned, "/") || cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.Contains(cleaned, "/../") {
			return fmt.Errorf("allowedPaths contains an invalid pattern")
		}
		if _, err := path.Match(cleaned, ""); err != nil {
			return fmt.Errorf("allowedPaths contains an invalid pattern: %w", err)
		}
	}
	if intent != corev1alpha1.WorkspaceIntentWrite {
		switch {
		case strings.TrimSpace(workspace.PublicationGitRepo) != "":
			return fmt.Errorf("publicationGitRepo requires write workspace intent")
		case workspace.PublicationRepository != nil:
			return fmt.Errorf("publicationRepository requires write workspace intent")
		case workspace.PublicationReadCredentialRef != nil:
			return fmt.Errorf("publicationReadCredentialRef requires write workspace intent")
		case workspace.PublicationCredentialRef != nil:
			return fmt.Errorf("publicationCredentialRef requires write workspace intent")
		case workspace.ForgeCredentialRef != nil:
			return fmt.Errorf("forgeCredentialRef requires write workspace intent")
		case strings.TrimSpace(workspace.PushBranch) != "":
			return fmt.Errorf("pushBranch requires write workspace intent")
		case strings.TrimSpace(workspace.PRBaseBranch) != "":
			return fmt.Errorf("prBaseBranch requires write workspace intent")
		case workspace.CreatePR:
			return fmt.Errorf("createPR requires write workspace intent")
		}
		if strings.TrimSpace(workspace.ExpectedRemoteSHA) != "" || workspace.MaxChangedFiles != nil || len(workspace.AllowedPaths) > 0 || workspace.DenyRepositoryControlPaths || workspace.RejectBinaryFiles || workspace.RejectSecretLikeContent {
			return fmt.Errorf("publication expectations and change policies require write workspace intent")
		}
		return nil
	}
	if workspace.PublicationCredentialRef == nil || strings.TrimSpace(workspace.PublicationCredentialRef.Name) == "" {
		return fmt.Errorf("write workspace intent requires publicationCredentialRef before prompt execution")
	}
	if strings.TrimSpace(workspace.GitRepo) == "" {
		return fmt.Errorf("write workspace intent requires a source gitRepo")
	}
	if _, err := workspacePublicationRepository(workspace); err != nil {
		return err
	}
	if pushBranch := strings.TrimSpace(workspace.PushBranch); pushBranch != "" {
		if _, err := canonicalWorkspaceBranchRef(pushBranch); err != nil {
			return fmt.Errorf("pushBranch is invalid: %w", err)
		}
	}
	if workspace.CreatePR && strings.TrimSpace(workspace.PRBaseBranch) == "" {
		return fmt.Errorf("createPR requires prBaseBranch")
	}
	if workspace.CreatePR {
		if _, err := canonicalWorkspaceBranchRef(workspace.PRBaseBranch); err != nil {
			return fmt.Errorf("prBaseBranch is invalid: %w", err)
		}
	}
	if workspace.CreatePR && (workspace.ForgeCredentialRef == nil || strings.TrimSpace(workspace.ForgeCredentialRef.Name) == "") {
		return fmt.Errorf("createPR requires forgeCredentialRef before prompt execution")
	}
	return nil
}

var errACPRuntimeWorkspaceNamespace = errors.New("invalid ACP runtime workspace namespace")

func validateACPRuntimeWorkspaceNamespace(
	plan ACPRuntimePlan,
	taskNamespace, configuredRuntimeNamespace string,
) error {
	if plan.Workspace == nil || plan.Workspace.Provider != corev1alpha1.WorkspaceProviderSubstrate {
		return nil
	}
	runtimeNamespace := strings.TrimSpace(configuredRuntimeNamespace)
	if runtimeNamespace == "" {
		runtimeNamespace = strings.TrimSpace(taskNamespace)
	}
	if err := validateSubstrateTemplateRuntimeNamespace(plan.Workspace.TemplateNamespace, runtimeNamespace); err != nil {
		return fmt.Errorf("%w: %v", errACPRuntimeWorkspaceNamespace, err)
	}
	return nil
}

func (r *TaskReconciler) ensureACPRuntimePool(
	ctx context.Context,
	namespace string,
	plan ACPRuntimePlan,
	workspaceName string,
	workspaceUID string,
	workspaceTaskUID string,
) (*corev1alpha1.RuntimePool, bool, error) {
	return r.ensureACPRuntimePoolWithPolicy(
		ctx, namespace, plan, workspaceName, workspaceUID, workspaceTaskUID, true, "",
	)
}

//nolint:gocyclo // Pool creation, immutable identity checks, and activation form one optimistic state transition.
func (r *TaskReconciler) ensureACPRuntimePoolWithPolicy(
	ctx context.Context,
	namespace string,
	plan ACPRuntimePlan,
	workspaceName string,
	workspaceUID string,
	workspaceTaskUID string,
	allowCreate bool,
	requiredUID types.UID,
) (*corev1alpha1.RuntimePool, bool, error) {
	pool := &corev1alpha1.RuntimePool{}
	key := types.NamespacedName{Namespace: namespace, Name: plan.PoolName}
	poolPreexisting := true
	err := r.Get(ctx, key, pool)
	if apierrors.IsNotFound(err) {
		if !allowCreate {
			return nil, false, store.ValidationErrorf(
				"the already-bound RuntimePool %s for the frozen ACP runtime profile no longer exists; create a new Task for the upgraded runtime",
				plan.PoolName,
			)
		}
		// The pool's runtime namespace is FROZEN from the linked workspace's
		// creation-time annotation, not re-read from the mutable controller
		// flag: workspace creation and pool creation happen on different
		// reconciles, and a flag change between them would realize the
		// SandboxClaim and durable PVC in a namespace the workspace's
		// deletion proofs never probe (a false NotFound would then report
		// the volume deleted while repository data remains). An explicit
		// frozen namespace the current controller no longer permits fails
		// the pool visibly instead of silently splitting the two.
		poolRuntimeNamespace := strings.TrimSpace(r.ACPRuntimeNamespace)
		if plan.Workspace != nil && workspaceName != "" {
			reader := client.Reader(r.Client)
			if r.APIReader != nil {
				reader = r.APIReader
			}
			workspace := &workspacev1alpha1.ExecutionWorkspace{}
			if getErr := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: workspaceName}, workspace); getErr != nil {
				return nil, false, fmt.Errorf("resolve the linked workspace's frozen runtime namespace: %w", getErr)
			}
			if frozen := strings.TrimSpace(workspace.Annotations[acpWorkspaceRuntimeNamespaceAnnotation]); frozen != "" {
				poolRuntimeNamespace = frozen
			}
		}
		if namespaceErr := validateACPRuntimeWorkspaceNamespace(plan, namespace, poolRuntimeNamespace); namespaceErr != nil {
			return nil, false, namespaceErr
		}
		capacity := &corev1alpha1.RuntimePoolCapacitySpec{
			MaxResidentSessions: corev1alpha1.DefaultRuntimePoolMaxResidentSessions,
			MaxRunningPrompts:   corev1alpha1.DefaultRuntimePoolMaxRunningPrompts,
		}
		labels := map[string]string{
			acpRuntimePoolLabel: booleanTrueValue, acpRuntimeTrustLabel: namespace,
			acpRuntimeProfileLabel: strings.TrimPrefix(string(plan.Digest), "sha256:")[:16],
		}
		annotations := map[string]string{acpRuntimeLastDemandAnnotation: time.Now().UTC().Format(time.RFC3339Nano)}
		var executionWorkspace *corev1alpha1.RuntimePoolExecutionWorkspaceSpec
		if plan.Workspace != nil {
			// A workspace-backed pool hosts exactly one logical RuntimeSession
			// inside one provider-owned physical workspace.
			capacity = &corev1alpha1.RuntimePoolCapacitySpec{MaxResidentSessions: 1, MaxRunningPrompts: 1}
			labels[acpRuntimeWorkspaceProviderLabel] = string(plan.Workspace.Provider)
			if workspaceName != "" {
				// Links the pool to its controller-first ExecutionWorkspace so
				// the ACP workspace adapter can prove exact ownership before
				// tearing the pool down. The name link is paired with the
				// workspace-incarnation UID pin: the adapter deletes only a
				// pool carrying both.
				labels[acpExecutionWorkspaceLinkLabel] = workspaceName
				if workspaceUID != "" {
					annotations[acpExecutionWorkspaceUIDAnnotation] = workspaceUID
				}
			}
			executionWorkspace = &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
				Provider:      plan.Workspace.Provider,
				BindingDigest: plan.Workspace.BindingDigest,
			}
			if plan.Workspace.Provider == corev1alpha1.WorkspaceProviderSubstrate {
				executionWorkspace.Substrate = &corev1alpha1.RuntimePoolSubstrateWorkspaceSpec{
					BaseTemplateNamespace: plan.Workspace.TemplateNamespace,
					BaseTemplateName:      plan.Workspace.TemplateName,
					SuspendMode:           acpSubstratePoolSuspendMode(plan.Workspace),
				}
			}
			if plan.Workspace.Provider == corev1alpha1.WorkspaceProviderAgentSandbox &&
				plan.Workspace.Class != nil && plan.Workspace.Class.SandboxVolume != nil {
				executionWorkspace.AgentSandbox = &corev1alpha1.RuntimePoolAgentSandboxWorkspaceSpec{
					SuspendMode: plan.Workspace.Class.SuspendMode,
					SuspendVolume: &corev1alpha1.RuntimePoolSandboxDurableVolumeSpec{
						StorageClassName: plan.Workspace.Class.SandboxVolume.StorageClassName,
						StorageClassUID:  plan.Workspace.Class.SandboxVolume.StorageClassUID,
						AccessModes:      append([]string(nil), plan.Workspace.Class.SandboxVolume.AccessModes...),
						Capacity:         plan.Workspace.Class.SandboxVolume.Capacity,
					},
				}
			}
		}
		pool = &corev1alpha1.RuntimePool{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: plan.PoolName,
				Annotations: annotations,
				Labels:      labels,
			},
			Spec: corev1alpha1.RuntimePoolSpec{
				TrustDomain:             corev1alpha1.RuntimePoolTrustDomain{Namespace: namespace, Identity: "namespace:" + namespace},
				RuntimeNamespace:        poolRuntimeNamespace,
				Runtime:                 corev1alpha1.RuntimePoolRuntimeSpec{Image: plan.Image, Profile: RuntimePoolProfileFromPlan(plan)},
				ExecutionWorkspace:      executionWorkspace,
				DesiredReplicas:         1,
				Capacity:                capacity,
				ColdStartTimeoutSeconds: corev1alpha1.DefaultRuntimePoolColdStartTimeoutSeconds,
			},
		}
		createErr := r.Create(ctx, pool)
		if createErr != nil {
			if !apierrors.IsAlreadyExists(createErr) {
				return nil, false, fmt.Errorf("create RuntimePool: %w", createErr)
			}
			if getErr := r.Get(ctx, key, pool); getErr != nil {
				return nil, false, getErr
			}
			// Another creator won the deterministic-name race. Validate and
			// activate that observed object through the same path as a pool that
			// existed before this reconcile; never bind a Task to an unchecked
			// same-name winner.
			err = nil
		} else {
			poolPreexisting = false
			err = nil
		}
	}
	if err != nil {
		return nil, false, err
	}
	if !allowCreate && !pool.DeletionTimestamp.IsZero() {
		return nil, false, store.ValidationErrorf(
			"the already-bound RuntimePool %s is deleting; create a new Task for the upgraded runtime",
			plan.PoolName,
		)
	}
	if requiredUID != "" && pool.UID != requiredUID {
		return nil, false, store.ValidationErrorf(
			"the already-bound RuntimePool %s was replaced with a different object; create a new Task for the upgraded runtime",
			plan.PoolName,
		)
	}
	poolRuntimeNamespace := strings.TrimSpace(pool.Spec.RuntimeNamespace)
	if poolRuntimeNamespace == "" {
		poolRuntimeNamespace = strings.TrimSpace(r.ACPRuntimeNamespace)
	}
	if namespaceErr := validateACPRuntimeWorkspaceNamespace(plan, namespace, poolRuntimeNamespace); namespaceErr != nil {
		return nil, false, namespaceErr
	}
	if !acpRuntimePoolWorkspaceMatchesPlan(pool, plan) {
		if plan.Workspace != nil && plan.Workspace.ReusePolicy == corev1alpha1.WorkspaceReusePolicySession {
			return nil, false, store.ValidationErrorf(
				"execution workspace reusePolicy session cannot change the workspace provider, template, cleanup policy, or slot without replacing the physical workspace; create a new Session or keep the original workspace configuration",
			)
		}
		return nil, false, fmt.Errorf("RuntimePool %s execution workspace binding does not match queued Task", pool.Name)
	}
	if workspaceName != "" && pool.Labels[acpExecutionWorkspaceLinkLabel] != workspaceName {
		return nil, false, fmt.Errorf(
			"RuntimePool %s is not linked to controller-first execution workspace %s", pool.Name, workspaceName,
		)
	}
	if workspaceName != "" && workspaceUID != "" &&
		pool.Annotations[acpExecutionWorkspaceUIDAnnotation] != workspaceUID {
		return nil, false, fmt.Errorf(
			"RuntimePool %s is not pinned to the exact execution workspace incarnation", pool.Name,
		)
	}
	if pool.Spec.Runtime.Image != plan.Image || pool.Spec.Runtime.Profile.Digest != string(plan.Digest) {
		if plan.Workspace != nil && plan.Workspace.ReusePolicy == corev1alpha1.WorkspaceReusePolicySession {
			return nil, false, store.ValidationErrorf(
				"execution workspace reusePolicy session cannot rotate the runtime image or profile without replacing the physical workspace; create a new Session or keep the original runtime configuration",
			)
		}
		return nil, false, fmt.Errorf("RuntimePool %s profile does not match queued Task", pool.Name)
	}
	base := pool.DeepCopy()
	changed := false
	if pool.Spec.DesiredReplicas == 0 {
		pool.Spec.DesiredReplicas = 1
		changed = true
	}
	if pool.Annotations == nil {
		pool.Annotations = make(map[string]string)
	}
	lastDemand, _ := time.Parse(time.RFC3339Nano, pool.Annotations[acpRuntimeLastDemandAnnotation])
	if time.Since(lastDemand) >= time.Minute {
		pool.Annotations[acpRuntimeLastDemandAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
		changed = true
	}
	if changed {
		if err := r.Patch(ctx, pool, client.MergeFrom(base)); err != nil {
			return nil, false, err
		}
	}
	if handshakeErr := r.verifyACPWorkspaceReadyForPool(
		ctx, pool, workspaceName, workspaceUID, workspaceTaskUID,
	); handshakeErr != nil {
		return nil, false, handshakeErr
	}
	if err := r.recordACPRuntimePoolImageProvenance(ctx, pool); err != nil {
		return nil, false, err
	}
	return pool, poolPreexisting, nil
}

func (r *TaskReconciler) recordACPRuntimePoolImageProvenance(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
) error {
	if pool == nil {
		return errors.New("RuntimePool is required for image provenance")
	}
	key := types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}
	expectedUID := pool.UID
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &corev1alpha1.RuntimePool{}
		if err := r.Get(ctx, key, current); err != nil {
			return err
		}
		if expectedUID != "" && current.UID != expectedUID {
			return store.ValidationErrorf("RuntimePool %s was replaced before image provenance could be recorded", pool.Name)
		}
		condition := meta.FindStatusCondition(current.Status.Conditions, acpRuntimePoolImageProvenanceCondition)
		if condition != nil && condition.Status == metav1.ConditionTrue &&
			condition.ObservedGeneration == current.Generation && condition.Reason == acpRuntimePoolImageProvenanceReason {
			*pool = *current.DeepCopy()
			return nil
		}
		base := current.DeepCopy()
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type:               acpRuntimePoolImageProvenanceCondition,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: current.Generation,
			Reason:             acpRuntimePoolImageProvenanceReason,
			Message:            "RuntimePool image and profile match a verified immutable Task execution plan",
		})
		if err := r.Status().Patch(ctx, current, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		*pool = *current.DeepCopy()
		return nil
	})
}

// verifyACPWorkspaceReadyForPool repeats the complete workspace admission and
// attachment handshake through the uncached reader after a RuntimePool has
// materialized. Any withdrawn lifecycle gate deletes the pool before prompt
// demand can survive against an orphaned, quarantined, expired, or revoked
// workspace.
func (r *TaskReconciler) verifyACPWorkspaceReadyForPool(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	workspaceName, workspaceUID, workspaceTaskUID string,
) error {
	if workspaceName == "" && workspaceUID == "" {
		return nil
	}
	abortReason := ""
	if pool == nil {
		return errors.New("RuntimePool is required for the post-materialization workspace handshake")
	} else if workspaceName == "" || workspaceUID == "" || workspaceTaskUID == "" {
		abortReason = "the workspace identity or attached Task UID is incomplete"
	} else if pool.Labels[acpExecutionWorkspaceLinkLabel] != workspaceName ||
		pool.Annotations[acpExecutionWorkspaceUIDAnnotation] != workspaceUID {
		abortReason = "the RuntimePool does not carry the exact workspace link"
	}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	if abortReason == "" {
		workspace := &workspacev1alpha1.ExecutionWorkspace{}
		err := reader.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: workspaceName}, workspace)
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("revalidate linked execution workspace: %w", err)
		}
		if apierrors.IsNotFound(err) {
			abortReason = "the linked workspace no longer exists"
		} else {
			abortReason = acpWorkspacePoolReadinessFailure(
				workspace, pool, workspaceUID, workspaceTaskUID, time.Now(),
			)
			if abortReason == "" {
				return nil
			}
		}
	}
	if deleteErr := r.deleteExactACPRuntimePool(ctx, reader, pool); deleteErr != nil {
		return fmt.Errorf(
			"linked execution workspace %s is not ready after RuntimePool materialization: %s; abort RuntimePool %s: %w",
			workspaceName, abortReason, pool.Name, deleteErr,
		)
	}
	return fmt.Errorf(
		"linked execution workspace %s is not ready after RuntimePool materialization: %s; the pool was aborted before any prompt",
		workspaceName, abortReason,
	)
}

func (r *TaskReconciler) deleteExactACPRuntimePool(
	ctx context.Context,
	reader client.Reader,
	expected *corev1alpha1.RuntimePool,
) error {
	if expected == nil || expected.UID == "" {
		return errors.New("exact RuntimePool identity is required for deletion")
	}
	key := client.ObjectKeyFromObject(expected)
	expectedWorkspaceName := expected.Labels[acpExecutionWorkspaceLinkLabel]
	expectedWorkspaceUID := expected.Annotations[acpExecutionWorkspaceUIDAnnotation]
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &corev1alpha1.RuntimePool{}
		if err := reader.Get(ctx, key, current); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if current.UID != expected.UID {
			return fmt.Errorf("RuntimePool %s was replaced before deletion", key)
		}
		if current.Labels[acpExecutionWorkspaceLinkLabel] != expectedWorkspaceName ||
			current.Annotations[acpExecutionWorkspaceUIDAnnotation] != expectedWorkspaceUID {
			return fmt.Errorf("RuntimePool %s workspace link changed before deletion", key)
		}
		if err := r.Delete(ctx, current, deleteCurrentObjectPreconditions(current)...); err != nil &&
			!apierrors.IsNotFound(err) {
			return err
		}
		return nil
	})
}

func acpWorkspacePoolReadinessFailure(
	workspace *workspacev1alpha1.ExecutionWorkspace,
	pool *corev1alpha1.RuntimePool,
	workspaceUID, workspaceTaskUID string,
	now time.Time,
) string {
	attached := meta.FindStatusCondition(
		workspace.Status.Conditions,
		string(workspacev1alpha1.ConditionWorkspaceAttached),
	)
	switch {
	case string(workspace.UID) != workspaceUID:
		return "the linked workspace was replaced"
	case !workspace.DeletionTimestamp.IsZero():
		return "the linked workspace is deleting"
	case workspace.Annotations[acpExecutionWorkspacePoolAnnotation] != pool.Name:
		return "the linked workspace does not name this RuntimePool"
	case workspace.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady:
		return fmt.Sprintf("the linked workspace desired state is %q", workspace.Spec.DesiredState)
	case !workspaceCurrentlyAdmittedByCore(workspace):
		return "core admission is no longer current"
	case workspace.Status.ObservedGeneration != workspace.Generation:
		return "provider status has not observed the current workspace generation"
	case workspace.Status.State != workspacev1alpha1.ExecutionWorkspaceStateAttached:
		return fmt.Sprintf("the linked workspace state is %q", workspace.Status.State)
	case workspace.Spec.Attachment == nil:
		return "the linked workspace attachment is being revoked"
	case string(workspace.Spec.Attachment.TaskRef.UID) != workspaceTaskUID:
		return "the linked workspace is attached to a different Task"
	case workspace.Spec.Attachment.Epoch <= 0 ||
		workspace.Spec.AttachmentEpoch != workspace.Spec.Attachment.Epoch ||
		workspace.Status.AttachedEpoch != workspace.Spec.Attachment.Epoch:
		return "the linked workspace attachment epoch is not fully enforced"
	case attached == nil || attached.Status != metav1.ConditionTrue ||
		attached.ObservedGeneration != workspace.Generation:
		return "the linked workspace attachment condition is not current"
	case !workspace.Spec.Attachment.ExpiresAt.After(now):
		return "the linked workspace attachment has expired"
	case strings.TrimSpace(workspace.Annotations[acpWorkspaceRevocationStartedAnnotation]) != "":
		return "attachment revocation has started for the linked workspace"
	default:
		if remaining, bounded := acpWorkspaceMaxLifetimeRemaining(workspace, now); bounded && remaining <= 0 {
			return "the linked workspace maximum lifetime has elapsed"
		}
		return ""
	}
}

func acpBoundTaskRequestDigest(bound *verifiedAgentExecution, attempt int32, promptID string) (string, error) {
	if bound == nil || bound.binding == nil || bound.snapshot == nil {
		return "", errors.New("verified binding and execution snapshot are required for ACP request identity")
	}
	return acpDomainDigest("task-request", map[string]any{
		"taskUID": bound.binding.Task.UID, "taskGeneration": bound.binding.Task.BoundSpecGeneration,
		"attempt": attempt, "promptID": promptID, "prompt": bound.body.Prompt,
		"agentUID": bound.body.Agent.UID, "agentGeneration": bound.body.Agent.Generation,
		"agentConfiguration":   bound.configuration,
		"runtimeProfileDigest": bound.body.ProfileDigest, "workspace": bound.body.Workspace,
		"bindingDigest": bound.binding.BindingDigest, "snapshotDigest": bound.snapshot.Digest,
	})
}

func acpQueuedTaskRequestMatchesBinding(bound *verifiedAgentExecution, status *corev1alpha1.TaskExecutionStatus) (bool, error) {
	if bound == nil || status == nil || status.Attempt < 1 || strings.TrimSpace(status.PromptID) == "" ||
		strings.TrimSpace(status.RequestDigest) == "" {
		return false, nil
	}
	digest, err := acpBoundTaskRequestDigest(bound, status.Attempt, status.PromptID)
	if err != nil {
		return false, err
	}
	return digest == status.RequestDigest, nil
}

func taskExecutionStateTerminal(state corev1alpha1.TaskExecutionState) bool {
	switch state {
	case corev1alpha1.TaskExecutionStateSucceeded, corev1alpha1.TaskExecutionStateFailed,
		corev1alpha1.TaskExecutionStateCancelled, corev1alpha1.TaskExecutionStateOutcomeUnknown:
		return true
	default:
		return false
	}
}

func taskManagedByACP(task *corev1alpha1.Task) bool {
	return task != nil && task.Spec.Type == corev1alpha1.TaskTypeAgent && task.Status.Execution != nil &&
		(strings.TrimSpace(task.Status.Execution.RuntimePoolName) != "" || strings.TrimSpace(task.Status.Execution.AgentRuntimeName) != "")
}
