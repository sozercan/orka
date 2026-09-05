package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/taskterminal"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type sessionCleanupTurnReader interface {
	ListSessionCleanupTurns(context.Context, store.SessionCleanupIntent) ([]store.SessionTurn, error)
}

type sessionRuntimeCleanupIdentity struct {
	poolName, poolUID, agentRuntimeName, agentRuntimeUID string
	instanceID, bootID, profileDigest, sessionUID        string
	generation                                           int64
}

type sessionRuntimeCleanupTarget struct {
	task            *corev1alpha1.Task
	taskUID         types.UID
	leaseGeneration int64
	identity        sessionRuntimeCleanupIdentity
	runtimeEpoch    uint64
}

// sessionRuntimeCleanupFence separates the current cleanup owner from the
// immutable terminal turn's resident runtime. Only Session teardown may use
// this older runtime epoch; it grants no admission or publication authority.
type sessionRuntimeCleanupFence struct {
	controller   store.ControllerEpochFence
	runtimeEpoch uint64
}

func (d *ACPDispatcher) externalRuntimeCleanupEpoch(ctx context.Context, cleanup *sessionRuntimeCleanupFence) (uint64, error) {
	current, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return 0, err
	}
	if cleanup == nil {
		return uint64(current.Epoch), nil
	}
	if cleanup.controller != current || cleanup.runtimeEpoch == 0 || cleanup.runtimeEpoch > uint64(current.Epoch) {
		return 0, fmt.Errorf("%w: Session runtime cleanup epoch authority changed", store.ErrConflict)
	}
	reader, ok := d.Store.(interface {
		GetControllerEpochFence(context.Context, string) (store.ControllerEpochFence, error)
	})
	if !ok {
		return 0, errors.New("session runtime cleanup requires authoritative epoch reads")
	}
	// ReclaimSession already holds the epoch mutation lock. A fresh read must
	// not reacquire it or rely on the manager's cached leadership fence.
	authoritative, err := reader.GetControllerEpochFence(ctx, current.Name)
	if err != nil {
		return 0, err
	}
	if authoritative != cleanup.controller {
		return 0, fmt.Errorf("%w: Session runtime cleanup controller lost authority", store.ErrConflict)
	}
	return cleanup.runtimeEpoch, nil
}

// CleanupSessionRuntime retires resident conversation processes after the
// durable Session cleanup intent has blocked new turns. All runtime authority
// comes from finalized turns and verified immutable Task execution snapshots.
// It is also used when replaying an interrupted Session deletion.
func (d *ACPDispatcher) CleanupSessionRuntime(
	ctx context.Context,
	intent store.SessionCleanupIntent,
	fence store.ControllerEpochFence,
) error {
	turnReader, ok := d.ResultStore.(sessionCleanupTurnReader)
	if !ok {
		return errors.New("session runtime cleanup requires durable turn enumeration")
	}
	turns, err := turnReader.ListSessionCleanupTurns(ctx, intent)
	if err != nil {
		return err
	}
	if len(turns) == 0 {
		return nil
	}
	if d.APIReader == nil || d.Client == nil || d.Store == nil || d.Epochs == nil || d.Snapshots == nil {
		return errors.New("session runtime cleanup authority is not configured")
	}
	currentFence, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return err
	}
	if currentFence != fence {
		return fmt.Errorf("%w: Session runtime cleanup controller epoch changed", store.ErrConflict)
	}
	targets := make([]sessionRuntimeCleanupTarget, 0, len(turns))
	for i := range turns {
		target, err := d.sessionRuntimeCleanupTarget(ctx, intent, &turns[i])
		if err != nil {
			return fmt.Errorf("prepare Session runtime cleanup for turn %q: %w", turns[i].ID, err)
		}
		if target != nil {
			targets = append(targets, *target)
		}
	}
	// A continued RuntimeSession belongs to its newest completed turn. Delete
	// each exact runtime incarnation once, then record proof for every Task
	// which retained the same conversation process.
	sort.Slice(targets, func(i, j int) bool { return targets[i].leaseGeneration > targets[j].leaseGeneration })
	cleaned := make(map[sessionRuntimeCleanupIdentity]struct{})
	for i := range targets {
		target := &targets[i]
		cleanupFence := &sessionRuntimeCleanupFence{controller: fence, runtimeEpoch: uint64(fence.Epoch)}
		if target.runtimeEpoch > 0 {
			cleanupFence.runtimeEpoch = target.runtimeEpoch
		}
		if _, err := d.externalRuntimeCleanupEpoch(ctx, cleanupFence); err != nil {
			return err
		}
		if _, done := cleaned[target.identity]; !done {
			ready, err := d.reconcileRecoveredRuntimeSession(ctx, target.task, target.taskUID, true, cleanupFence)
			if err != nil {
				return err
			}
			if !ready {
				return fmt.Errorf("%w: Session runtime cleanup is awaiting exact runtime retirement", store.ErrNotReady)
			}
			cleaned[target.identity] = struct{}{}
		}
		if _, err := d.externalRuntimeCleanupEpoch(ctx, cleanupFence); err != nil {
			return err
		}
		if err := d.recordSessionRuntimeCleanupForTask(ctx, target); err != nil {
			return err
		}
	}
	return nil
}

//nolint:gocyclo // Verify every finalized-turn and frozen runtime ownership field in one fail-closed boundary.
func (d *ACPDispatcher) sessionRuntimeCleanupTarget(
	ctx context.Context,
	intent store.SessionCleanupIntent,
	turn *store.SessionTurn,
) (*sessionRuntimeCleanupTarget, error) {
	turnID, err := turn.Key.CanonicalID()
	if err != nil || turn.ID != turnID || turn.Key.SessionUID != intent.SessionUID ||
		turn.State != store.SessionTurnFinalized || turn.FinalizedAt == nil {
		return nil, fmt.Errorf("%w: Session runtime cleanup turn identity is incomplete", store.ErrConflict)
	}
	projection, err := d.Store.GetOutboxProjection(ctx, turn.ProjectionID)
	if err != nil {
		return nil, fmt.Errorf("load Session runtime cleanup projection: %w", err)
	}
	if projection.AggregateKind != "SessionTurn" || projection.AggregateID != turn.ID ||
		projection.ProjectionKind != taskterminal.ProjectionKind || turn.ProjectionKind != projection.ProjectionKind ||
		projection.ID != store.CanonicalControlID("outbox", turn.ID, taskterminal.ProjectionKind) ||
		projection.PayloadDigest != store.CanonicalBytesDigest(projection.Payload) ||
		projection.PayloadDigest != turn.ProjectionDigest {
		return nil, fmt.Errorf("%w: Session runtime cleanup projection identity changed", store.ErrConflict)
	}
	var payload taskterminal.Projection
	if err := json.Unmarshal(projection.Payload, &payload); err != nil {
		return nil, fmt.Errorf("%w: Session runtime cleanup projection is malformed", store.ErrConflict)
	}
	if payload.Namespace != intent.Namespace || payload.TaskUID != turn.Key.TaskUID ||
		int64(payload.Attempt) != turn.Key.Attempt || strings.TrimSpace(payload.Task) == "" {
		return nil, fmt.Errorf("%w: Session runtime cleanup projection Task identity changed", store.ErrConflict)
	}
	if payload.HarnessRuntime != nil {
		// Harness v1 has no resident v2 RuntimeSession. Its immutable terminal
		// projection remains sufficient even after its Task was reclaimed.
		if payload.Execution.RuntimeSessionUID != "" || payload.Execution.RuntimeInstanceID != "" {
			return nil, fmt.Errorf("%w: harness v1 projection contains v2 runtime ownership", store.ErrConflict)
		}
		if payload.HarnessRuntime.Attempt != payload.Attempt ||
			!store.IsTerminalPromptExecutionState(store.PromptExecutionState(payload.HarnessRuntime.State)) ||
			string(payload.HarnessRuntime.State) != string(payload.HarnessRuntime.Outcome) {
			return nil, fmt.Errorf("%w: harness v1 projection lacks a terminal attempt", store.ErrConflict)
		}
		// V1 has no exact resident-runtime retirement proof for an unknown
		// outcome. Retain that barrier even when its control object is absent.
		if payload.HarnessRuntime.State == corev1alpha1.TaskExecutionStateOutcomeUnknown {
			return nil, fmt.Errorf("%w: harness v1 unknown outcome requires reconciliation", store.ErrConflict)
		}
		return nil, nil
	}
	if payload.Execution.Attempt != payload.Attempt || payload.Execution.PromptID != turn.Key.PromptID ||
		!store.IsTerminalPromptExecutionState(store.PromptExecutionState(payload.Execution.State)) ||
		payload.Delivery == nil || !store.IsTerminalPromptDeliveryState(store.PromptDeliveryState(payload.Delivery.State)) {
		return nil, fmt.Errorf("%w: Session runtime cleanup projection lacks a terminal attempt", store.ErrConflict)
	}
	task := &corev1alpha1.Task{}
	if err := d.APIReader.Get(ctx, client.ObjectKey{Namespace: intent.Namespace, Name: payload.Task}, task); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: Session runtime cleanup is missing frozen Task authority for %s/%s", store.ErrConflict, intent.Namespace, payload.Task)
		}
		return nil, err
	}
	if acpTaskControlUID(task) != types.UID(turn.Key.TaskUID) {
		return nil, fmt.Errorf("%w: Session runtime cleanup Task UID changed", store.ErrConflict)
	}
	frozen, err := d.frozenSessionRuntimeCleanupTask(ctx, task, intent)
	if err != nil {
		return nil, err
	}
	attempt, err := d.Store.GetPromptAttempt(ctx, turn.PromptAttemptID)
	if err != nil {
		return nil, fmt.Errorf("load Session runtime cleanup PromptAttempt: %w", err)
	}
	binding := frozen.Status.AgentExecutionBinding
	if attempt.BindingDigest != binding.BindingDigest || attempt.SnapshotDigest != binding.Snapshot.Digest {
		return nil, fmt.Errorf("%w: Session runtime cleanup PromptAttempt binding changed", store.ErrConflict)
	}
	if payload.Execution.RuntimeSessionUID != "" {
		frozen.Status.Execution = payload.Execution.DeepCopy()
		frozen.Status.Delivery = payload.Delivery.DeepCopy()
		frozen.Status.Phase = payload.Phase
	}
	validated, err := taskterminal.ValidateFinalizedSessionProjection(projection.Payload, frozen, turn.Key.TaskUID, attempt, turn)
	if err != nil {
		return nil, err
	}
	frozen.Status.Execution = validated.Execution.DeepCopy()
	frozen.Status.Delivery = validated.Delivery.DeepCopy()
	execution := frozen.Status.Execution
	if execution.RuntimeSessionUID != intent.SessionUID || execution.RuntimeSessionGeneration < 1 ||
		execution.RuntimeSessionSupervisorBootID == "" || execution.RuntimeInstanceID == "" ||
		execution.RuntimeSessionProfileDigest != binding.RuntimeProfileDigest {
		return nil, fmt.Errorf("%w: Session runtime cleanup lacks an exact runtime fence", store.ErrConflict)
	}
	identity := sessionRuntimeCleanupIdentityForExecution(execution)
	if task.Status.Execution != nil && task.Status.Execution.Attempt == execution.Attempt &&
		sessionRuntimeCleanupIdentityForExecution(task.Status.Execution) == identity {
		frozen.Status.Execution.RuntimeSessionCleanupDigest = task.Status.Execution.RuntimeSessionCleanupDigest
	}
	if frozen.Spec.Workspace != nil && frozen.Spec.Workspace.Intent == corev1alpha1.WorkspaceIntentWrite {
		// Write sessions are Task-scoped and already pass their runtime cleanup
		// barrier independently of conversation deletion. Their Task finalizers
		// retain the frozen authority until this Session cleanup completes.
		if !runtimeSessionCleanupCompleteForUID(frozen, types.UID(turn.Key.TaskUID)) {
			return nil, fmt.Errorf("%w: write Task runtime cleanup is incomplete", store.ErrNotReady)
		}
		return nil, nil
	}
	target := &sessionRuntimeCleanupTarget{
		task: frozen, taskUID: types.UID(turn.Key.TaskUID), leaseGeneration: turn.Key.LeaseGeneration, identity: identity,
	}
	// Legacy projections may recover omitted identity from mutable Task status.
	// They cannot authorize a prior-epoch mutation. Only a complete, explicitly
	// frozen execution can supply the resident runtime's original epoch.
	if payload.Execution.RuntimeSessionUID != "" && payload.Execution.ControllerEpoch > 0 {
		target.runtimeEpoch = uint64(payload.Execution.ControllerEpoch)
	}
	return target, nil
}

func (d *ACPDispatcher) frozenSessionRuntimeCleanupTask(
	ctx context.Context,
	task *corev1alpha1.Task,
	intent store.SessionCleanupIntent,
) (*corev1alpha1.Task, error) {
	binding := executionBinding(task, corev1alpha1.AgentRuntimeContractHarnessV2)
	if binding == nil || binding.Task.UID != acpTaskControlUID(task) {
		return nil, fmt.Errorf("%w: Session runtime cleanup requires the frozen Task binding", store.ErrConflict)
	}
	digest, err := canonicalAgentExecutionBindingDigest(*binding)
	if err != nil || digest != binding.BindingDigest {
		return nil, fmt.Errorf("%w: Session runtime cleanup binding failed integrity verification", store.ErrConflict)
	}
	snapshot, err := d.Snapshots.GetAgentExecutionSnapshot(ctx, store.AgentExecutionSnapshotKey{
		TaskUID: string(binding.Task.UID), Digest: binding.Snapshot.Digest,
	})
	if err != nil {
		return nil, fmt.Errorf("load Session runtime cleanup execution snapshot: %w", err)
	}
	body, err := decodeAgentExecutionSnapshot(snapshot.Body)
	if err != nil {
		return nil, err
	}
	if _, _, _, err := validateAgentExecutionSnapshot(binding, snapshot, body); err != nil {
		return nil, fmt.Errorf("validate Session runtime cleanup execution snapshot: %w", err)
	}
	if body.SessionRef == nil || body.SessionRef.Name != intent.SessionName {
		return nil, fmt.Errorf("%w: Session runtime cleanup snapshot belongs to another Session", store.ErrConflict)
	}
	return frozenTaskFromAgentExecutionSnapshot(task, binding, body), nil
}

func (d *ACPDispatcher) recordSessionRuntimeCleanupForTask(ctx context.Context, target *sessionRuntimeCleanupTarget) error {
	current := &corev1alpha1.Task{}
	if err := d.APIReader.Get(ctx, client.ObjectKeyFromObject(target.task), current); err != nil {
		return fmt.Errorf("read Task before recording Session runtime cleanup: %w", err)
	}
	if acpTaskControlUID(current) != target.taskUID {
		return fmt.Errorf("%w: Task identity changed before recording Session runtime cleanup", store.ErrConflict)
	}
	if current.Status.Execution == nil {
		return fmt.Errorf("%w: Task runtime cleanup execution identity is missing", store.ErrConflict)
	}
	if current.Status.Execution.Attempt != target.task.Status.Execution.Attempt ||
		sessionRuntimeCleanupIdentityForExecution(current.Status.Execution) != target.identity {
		// An older attempt's immutable turn remains archived independently;
		// only the Task's current runtime incarnation receives a status receipt.
		return nil
	}
	return d.markTaskScopedRuntimeSessionCleanupComplete(ctx, current, target.taskUID,
		target.identity.instanceID, target.identity.sessionUID, target.identity.generation)
}

func sessionRuntimeCleanupIdentityForExecution(execution *corev1alpha1.TaskExecutionStatus) sessionRuntimeCleanupIdentity {
	return sessionRuntimeCleanupIdentity{
		poolName: execution.RuntimePoolName, poolUID: execution.RuntimePoolUID,
		agentRuntimeName: execution.AgentRuntimeName, agentRuntimeUID: execution.AgentRuntimeUID,
		instanceID: execution.RuntimeInstanceID, bootID: execution.RuntimeSessionSupervisorBootID,
		profileDigest: execution.RuntimeSessionProfileDigest,
		sessionUID:    execution.RuntimeSessionUID, generation: execution.RuntimeSessionGeneration,
	}
}

func validateSessionRuntimeCleanupStatus(expected harnessv2.Fence, status *harnessv2.StatusResponse) error {
	if status == nil || harnessv2.CompareFence(expected, status.Fence, false) != harnessv2.FenceMatch {
		return fmt.Errorf("%w: authenticated Session runtime cleanup status fence changed", store.ErrConflict)
	}
	return nil
}
