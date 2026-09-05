package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	v2conformance "github.com/orka-agents/orka/internal/harness/v2/conformance"
	v2eventjournal "github.com/orka-agents/orka/internal/harness/v2/eventjournal"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/taskterminal"
)

const (
	taskResourceKind                     = "Task"
	taskTerminalProjectionKind           = "TaskTerminalStatus"
	acpControllerRestartRecoveredReason  = "ControllerRestartRecovered"
	acpControllerRestartRecoveredMessage = "pre-submission attempt recovered under the new controller epoch"
	acpRestoreIdentityChangedReason      = "RestoreIdentityChanged"
	acpRestoreIdentityChangedOperation   = "restore-identity-changed"
	acpRestorePreSubmissionMessage       = "Task incarnation changed during restore before prompt submission; source execution was not replayed"
	acpRestorePostWriteMessage           = "Task incarnation changed during restore after the prompt request-write boundary; outcome is unknown and was not replayed"
	acpRestoreTerminalPreservedMessage   = "Task incarnation changed during restore after source execution reached a durable terminal state; execution was preserved and was not replayed"
	agentRuntimeDrainBindingDigestKey    = "bindingDigest"
)

func acpTaskControlUID(task *corev1alpha1.Task) types.UID {
	if task == nil {
		return ""
	}
	if acpTaskUsesRestoredSourceIdentity(task) {
		binding := task.Status.AgentExecutionBinding
		return binding.Task.UID
	}
	return task.UID
}

func acpTaskUsesRestoredSourceIdentity(task *corev1alpha1.Task) bool {
	if task == nil || task.Status.Execution == nil {
		return false
	}
	switch task.Status.Phase {
	case corev1alpha1.TaskPhaseSucceeded, corev1alpha1.TaskPhaseFailed, corev1alpha1.TaskPhaseCancelled:
	default:
		return false
	}
	binding := executionBinding(task, corev1alpha1.AgentRuntimeContractHarnessV2)
	if binding == nil || binding.Task.UID == "" || binding.Task.UID == task.UID ||
		task.Status.Execution.Reason != corev1alpha1.TaskExecutionReason(acpRestoreIdentityChangedReason) {
		return false
	}
	switch task.Status.Execution.State {
	case corev1alpha1.TaskExecutionStateSucceeded:
		return task.Status.Execution.Outcome == corev1alpha1.TaskExecutionOutcomeSucceeded
	case corev1alpha1.TaskExecutionStateFailed:
		return task.Status.Execution.Outcome == corev1alpha1.TaskExecutionOutcomeFailed
	case corev1alpha1.TaskExecutionStateCancelled:
		return task.Status.Execution.Outcome == corev1alpha1.TaskExecutionOutcomeCancelled
	case corev1alpha1.TaskExecutionStateOutcomeUnknown:
		return task.Status.Execution.Outcome == corev1alpha1.TaskExecutionOutcomeOutcomeUnknown
	default:
		return false
	}
}

func acpTaskHasUnvalidatedSourceIdentity(task *corev1alpha1.Task) bool {
	return acpTaskHasRestoredSourceIdentityBinding(task) && !acpTaskUsesRestoredSourceIdentity(task)
}

func acpTaskHasRestoredSourceIdentityBinding(task *corev1alpha1.Task) bool {
	binding := executionBinding(task, corev1alpha1.AgentRuntimeContractHarnessV2)
	return binding != nil && binding.Task.UID != "" && binding.Task.UID != task.UID
}

// acpTaskRecoveryErrors identifies failures isolated to surviving Tasks. Store
// initialization, epoch acquisition, and listing failures remain startup errors.
type acpTaskRecoveryErrors struct{ error }

func (e *acpTaskRecoveryErrors) Unwrap() error { return e.error }

// recoverStaleAttempts classifies every old-epoch ACP attempt before the new
// leader admits work. It resumes only states that provably crossed no prompt
// request-write boundary and makes all potentially accepted prompts terminally
// OutcomeUnknown without allocating a replacement attempt.
func (d *ACPDispatcher) recoverStaleAttempts(ctx context.Context) error {
	fence, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return err
	}
	var tasks corev1alpha1.TaskList
	if err := d.Client.List(ctx, &tasks); err != nil {
		return err
	}
	var blocked []error
	for i := range tasks.Items {
		recoveryCtx, cancel := context.WithTimeout(ctx, acpPreSubmissionCleanupTimeout)
		err := d.recoverStaleCandidate(recoveryCtx, &tasks.Items[i], fence)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			blocked = append(blocked, err)
		}
	}
	if len(blocked) > 0 {
		return &acpTaskRecoveryErrors{error: errors.Join(blocked...)}
	}
	return nil
}

// scheduleStaleAttemptRecoveries retries the whole stale batch on one worker.
// Its bounded runtime/network waits never occupy prompt admission slots, and
// the existing active-Task map still excludes simultaneous recovery owners.
func (d *ACPDispatcher) scheduleStaleAttemptRecoveries(ctx context.Context, tasks []corev1alpha1.Task, fence store.ControllerEpochFence) {
	stale := make([]*corev1alpha1.Task, 0)
	for i := range tasks {
		task := &tasks[i]
		if taskDispatchableByACP(task) && task.Status.Execution != nil &&
			(task.Status.Execution.ControllerEpoch < fence.Epoch || acpTaskHasUnvalidatedSourceIdentity(task)) {
			stale = append(stale, task.DeepCopy())
		}
	}
	if len(stale) == 0 || !d.staleRecoveryMu.TryLock() {
		return
	}
	go func() {
		defer d.staleRecoveryMu.Unlock()
		for _, task := range stale {
			if ctx.Err() != nil {
				return
			}
			if !d.markActive(task.UID) {
				continue
			}
			recoveryCtx, cancel := context.WithTimeout(ctx, acpPreSubmissionCleanupTimeout)
			err := d.recoverStaleCandidate(recoveryCtx, task, fence)
			cancel()
			d.unmarkActive(task.UID)
			if err != nil && !errors.Is(err, context.Canceled) {
				logf.FromContext(ctx).Error(err, "ACP Task remains blocked on epoch recovery", "namespace", task.Namespace, "task", task.Name)
			}
		}
	}()
}

func (d *ACPDispatcher) recoverStaleCandidate(ctx context.Context, candidate *corev1alpha1.Task, fence store.ControllerEpochFence) error {
	task, recoverable, err := d.readRecoverableTask(ctx, candidate)
	if err != nil {
		return fmt.Errorf("refresh stale ACP task %s/%s: %w", candidate.Namespace, candidate.Name, err)
	}
	if !recoverable {
		return nil
	}
	if acpTaskHasRestoredSourceIdentityBinding(task) {
		if restored, restoreErr := d.recoverRestoredTaskIncarnation(ctx, task, fence); restoreErr != nil {
			return fmt.Errorf("recover restored ACP task %s/%s: %w", task.Namespace, task.Name, restoreErr)
		} else if !restored {
			return fmt.Errorf("recover restored ACP task %s/%s: %w: restored Task source identity was not classified",
				task.Namespace, task.Name, store.ErrConflict)
		}
		return nil
	}
	if !taskManagedByACP(task) || !taskDispatchableByACP(task) || task.Status.Execution == nil ||
		task.Status.Execution.ControllerEpoch >= fence.Epoch {
		return nil
	}
	if err := d.recoverStaleTask(ctx, task, fence); err != nil {
		if errors.Is(err, store.ErrNotFound) || apierrors.IsNotFound(err) {
			_, stillRecoverable, recheckErr := d.readRecoverableTask(ctx, task)
			if recheckErr != nil {
				return fmt.Errorf("recheck stale ACP task %s/%s after not found: %w", task.Namespace, task.Name, recheckErr)
			}
			if !stillRecoverable {
				return nil
			}
		}
		return fmt.Errorf("recover stale ACP task %s/%s: %w", task.Namespace, task.Name, err)
	}
	return nil
}

// recoverRestoredTaskIncarnation settles a Task whose Kubernetes UID changed
// during a clean-cluster restore. Kubernetes UIDs are incarnation fences, not
// portable logical identifiers: all durable source records remain keyed by the
// UID frozen in AgentExecutionBinding and are never rewritten or replayed under
// the restored Task UID.
func (d *ACPDispatcher) recoverRestoredTaskIncarnation(
	ctx context.Context,
	task *corev1alpha1.Task,
	fence store.ControllerEpochFence,
) (bool, error) {
	if task == nil {
		return false, nil
	}
	binding := executionBinding(task, corev1alpha1.AgentRuntimeContractHarnessV2)
	if binding == nil {
		return false, nil
	}
	sourceUID := strings.TrimSpace(string(binding.Task.UID))
	if sourceUID == "" {
		return false, nil
	}
	if sourceUID == string(task.UID) {
		return false, nil
	}
	if task.Status.Execution == nil {
		return true, fmt.Errorf("%w: restored Task execution status is missing", store.ErrConflict)
	}
	if task.Status.Execution.Attempt < 1 || strings.TrimSpace(task.Status.Execution.PromptID) == "" {
		return true, fmt.Errorf("%w: restored Task execution identity is incomplete", store.ErrConflict)
	}
	attemptID, err := (store.PromptAttemptKey{
		Namespace: task.Namespace,
		TaskUID:   sourceUID,
		Attempt:   int64(task.Status.Execution.Attempt),
		PromptID:  task.Status.Execution.PromptID,
	}).CanonicalID()
	if err != nil {
		return true, err
	}
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return true, err
	}
	if err := validateRestoredTaskSourceAttempt(task, attempt, attemptID); err != nil {
		return true, err
	}
	attempt, err = d.reconcileRestoredJournaledPromptTerminal(
		ctx, task, types.UID(sourceUID), attempt, fence,
	)
	if err != nil {
		return true, err
	}

	alreadySettled := acpTaskUsesRestoredSourceIdentity(task)

	settlement, err := d.settleRestoredTaskExecution(ctx, attempt, fence)
	if err != nil {
		return true, err
	}
	attempt, err = d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return true, err
	}
	if err := validateRestoredTaskSourceAttempt(task, attempt, attemptID); err != nil {
		return true, err
	}
	attempt, err = d.settleRestoredTerminalDelivery(ctx, task, types.UID(sourceUID), attempt, fence)
	if err != nil {
		return true, err
	}
	delivery, err := restoredTerminalDeliveryStatus(task, attempt.DeliveryState)
	if err != nil {
		return true, err
	}
	cleanupComplete, err := d.cleanupRecoveredTaskScopedRuntimeSessionForUID(ctx, task, types.UID(sourceUID))
	if err != nil {
		return true, err
	}
	if !cleanupComplete {
		return true, fmt.Errorf("%w: restored Task RuntimeSession cleanup is not complete", store.ErrNotReady)
	}
	if settlement.requiresProjection || alreadySettled {
		if err := d.ensureRestoredTaskTerminalProjection(
			ctx, task, types.UID(sourceUID), attempt, fence,
			settlement.state, settlement.outcome, settlement.message, delivery,
		); err != nil {
			return true, err
		}
	}
	return true, d.patchRestoredTaskTerminal(
		ctx, task, fence.Epoch, settlement.state, settlement.outcome, settlement.message, delivery,
	)
}

type restoredTaskExecutionSettlement struct {
	state              corev1alpha1.TaskExecutionState
	outcome            corev1alpha1.TaskExecutionOutcome
	message            string
	requiresProjection bool
}

func (d *ACPDispatcher) settleRestoredTaskExecution(
	ctx context.Context,
	attempt *store.PromptAttempt,
	fence store.ControllerEpochFence,
) (restoredTaskExecutionSettlement, error) {
	settlement := restoredTaskExecutionSettlement{
		state:   corev1alpha1.TaskExecutionStateFailed,
		outcome: corev1alpha1.TaskExecutionOutcomeFailed,
		message: acpRestorePreSubmissionMessage,
	}
	if attempt == nil {
		return settlement, fmt.Errorf("%w: restored Task source PromptAttempt is missing", store.ErrConflict)
	}
	switch attempt.ExecutionState {
	case store.PromptExecutionQueued, store.PromptExecutionReserved,
		store.PromptExecutionSessionStarting, store.PromptExecutionPlanned:
		if err := d.persistRestoredPreSubmissionFailure(ctx, attempt, fence); err != nil {
			return settlement, err
		}
		settlement.requiresProjection = true
	case store.PromptExecutionSubmitting, store.PromptExecutionSubmittedUnknown,
		store.PromptExecutionAccepted, store.PromptExecutionRunning, store.PromptExecutionSettling:
		settlement.state = corev1alpha1.TaskExecutionStateOutcomeUnknown
		settlement.outcome = corev1alpha1.TaskExecutionOutcomeOutcomeUnknown
		settlement.message = acpRestorePostWriteMessage
		if err := d.persistOutcomeUnknown(ctx, attempt.ID, fence, acpRestoreIdentityChangedReason, settlement.message); err != nil {
			return settlement, err
		}
		settlement.requiresProjection = true
	case store.PromptExecutionSucceeded, store.PromptExecutionFailed,
		store.PromptExecutionCancelled, store.PromptExecutionOutcomeUnknown:
		settlement.requiresProjection = true
		if attempt.ExecutionState == store.PromptExecutionFailed &&
			attempt.TerminalReason == acpRestoreIdentityChangedReason &&
			attempt.OutcomeMarker == acpRestorePreSubmissionMessage {
			settlement.message = acpRestorePreSubmissionMessage
		} else if attempt.ExecutionState == store.PromptExecutionOutcomeUnknown &&
			attempt.TerminalReason == acpRestoreIdentityChangedReason &&
			attempt.OutcomeMarker == acpRestorePostWriteMessage {
			settlement.state = corev1alpha1.TaskExecutionStateOutcomeUnknown
			settlement.outcome = corev1alpha1.TaskExecutionOutcomeOutcomeUnknown
			settlement.message = acpRestorePostWriteMessage
		} else {
			settlement.state, settlement.outcome = restoredTerminalExecutionState(attempt.ExecutionState)
			settlement.message = acpRestoreTerminalPreservedMessage
		}
	default:
		return settlement, fmt.Errorf("%w: restored Task source PromptAttempt has unsupported state %q", store.ErrConflict, attempt.ExecutionState)
	}
	return settlement, nil
}

func restoredTerminalExecutionState(state store.PromptExecutionState) (corev1alpha1.TaskExecutionState, corev1alpha1.TaskExecutionOutcome) {
	switch state {
	case store.PromptExecutionSucceeded:
		return corev1alpha1.TaskExecutionStateSucceeded, corev1alpha1.TaskExecutionOutcomeSucceeded
	case store.PromptExecutionCancelled:
		return corev1alpha1.TaskExecutionStateCancelled, corev1alpha1.TaskExecutionOutcomeCancelled
	case store.PromptExecutionOutcomeUnknown:
		return corev1alpha1.TaskExecutionStateOutcomeUnknown, corev1alpha1.TaskExecutionOutcomeOutcomeUnknown
	default:
		return corev1alpha1.TaskExecutionStateFailed, corev1alpha1.TaskExecutionOutcomeFailed
	}
}

func restoredTaskPhase(state corev1alpha1.TaskExecutionState, delivery store.PromptDeliveryState) corev1alpha1.TaskPhase {
	switch state {
	case corev1alpha1.TaskExecutionStateCancelled:
		return corev1alpha1.TaskPhaseCancelled
	case corev1alpha1.TaskExecutionStateSucceeded:
		switch delivery {
		case store.PromptDeliveryNotRequested, store.PromptDeliveryReadValidated, store.PromptDeliveryNoChange,
			store.PromptDeliveryVerifiedExact, store.PromptDeliveryDeliveredSuperseded:
			return corev1alpha1.TaskPhaseSucceeded
		case store.PromptDeliveryCancelledBeforePublish:
			return corev1alpha1.TaskPhaseCancelled
		}
	}
	return corev1alpha1.TaskPhaseFailed
}

func restoredTerminalDeliveryStatus(task *corev1alpha1.Task, state store.PromptDeliveryState) (*corev1alpha1.TaskDeliveryStatus, error) {
	if !store.IsTerminalPromptDeliveryState(state) {
		return nil, fmt.Errorf("%w: restored Task delivery state %q is not terminal", store.ErrConflict, state)
	}
	var delivery *corev1alpha1.TaskDeliveryStatus
	if task != nil && task.Status.Delivery != nil {
		delivery = task.Status.Delivery.DeepCopy()
	} else {
		delivery = &corev1alpha1.TaskDeliveryStatus{}
	}
	if store.PromptDeliveryState(delivery.State) == state && string(delivery.Outcome) == string(state) {
		return delivery, nil
	}
	delivery.State = corev1alpha1.TaskDeliveryState(state)
	delivery.Outcome = corev1alpha1.TaskDeliveryOutcome(state)
	delivery.LastTransitionTime = nowMeta()
	return delivery, nil
}

func (d *ACPDispatcher) persistRestoredPreSubmissionFailure(
	ctx context.Context,
	attempt *store.PromptAttempt,
	fence store.ControllerEpochFence,
) error {
	if attempt == nil {
		return fmt.Errorf("%w: restored Task source PromptAttempt is missing", store.ErrConflict)
	}
	if attempt.ExecutionState == store.PromptExecutionFailed {
		if attempt.TerminalReason == acpRestoreIdentityChangedReason && attempt.OutcomeMarker == acpRestorePreSubmissionMessage {
			return nil
		}
		return fmt.Errorf("%w: restored Task source PromptAttempt has a conflicting terminal failure", store.ErrConflict)
	}
	if err := store.ValidatePromptExecutionTransition(attempt.ExecutionState, store.PromptExecutionFailed); err != nil {
		return err
	}
	digest, err := acpDomainDigest("attempt-transition", map[string]any{
		"id": attempt.ID, "from": attempt.ExecutionState, "to": store.PromptExecutionFailed,
		"operation": acpRestoreIdentityChangedOperation, "version": attempt.Version,
	})
	if err != nil {
		return err
	}
	_, err = d.Store.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
		NewState:        store.PromptExecutionFailed,
		OperationID:     acpRestoreIdentityChangedOperation + "-" + strconv.FormatInt(attempt.Version, 10),
		OperationDigest: digest, TerminalReason: acpRestoreIdentityChangedReason,
		OutcomeMarker: acpRestorePreSubmissionMessage, UpdatedAt: time.Now().UTC(),
	})
	return err
}

func (d *ACPDispatcher) ensureRestoredTaskTerminalProjection(
	ctx context.Context,
	task *corev1alpha1.Task,
	sourceUID types.UID,
	attempt *store.PromptAttempt,
	fence store.ControllerEpochFence,
	state corev1alpha1.TaskExecutionState,
	outcome corev1alpha1.TaskExecutionOutcome,
	message string,
	delivery *corev1alpha1.TaskDeliveryStatus,
) error {
	if task == nil || task.Status.Execution == nil || attempt == nil || delivery == nil || sourceUID == "" || sourceUID == task.UID ||
		store.PromptDeliveryState(delivery.State) != attempt.DeliveryState || string(delivery.Outcome) != string(attempt.DeliveryState) {
		return fmt.Errorf("%w: restored Task projection identity is incomplete", store.ErrConflict)
	}
	projectionID := standaloneTaskTerminalProjectionIDForUID(task.Namespace, sourceUID, task.Status.Execution.Attempt)
	existing, err := d.Store.GetOutboxProjection(ctx, projectionID)
	if err == nil {
		return validateRestoredSourceTerminalProjection(task, sourceUID, attempt, existing)
	}
	if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	execution := *task.Status.Execution
	execution.State = state
	execution.Outcome = outcome
	phase := restoredTaskPhase(state, attempt.DeliveryState)
	projectionMessage := message
	if attempt.TerminalReason == acpRestoreIdentityChangedReason {
		execution.Reason = corev1alpha1.TaskExecutionReason(acpRestoreIdentityChangedReason)
		execution.Message = message
	}
	execution.ControllerEpoch = fence.Epoch
	payload := taskTerminalProjection{
		Namespace: task.Namespace,
		Task:      task.Name,
		TaskUID:   string(sourceUID),
		Attempt:   execution.Attempt,
		Phase:     phase,
		Message:   projectionMessage,
		Execution: execution,
		Delivery:  delivery,
	}
	return enqueueDurableTaskTerminalProjectionForUID(ctx, d.Store, fence, task, sourceUID, payload)
}

func validateRestoredSourceTerminalProjection(task *corev1alpha1.Task, sourceUID types.UID, attempt *store.PromptAttempt, projection *store.OutboxProjection) error {
	if projection == nil || projection.ID != standaloneTaskTerminalProjectionIDForUID(task.Namespace, sourceUID, task.Status.Execution.Attempt) ||
		projection.AggregateKind != taskResourceKind || projection.AggregateID != string(sourceUID) || projection.ProjectionKind != taskTerminalProjectionKind {
		return fmt.Errorf("%w: restored source terminal projection identity mismatch", store.ErrConflict)
	}
	_, err := taskterminal.ValidateRestoredProjection(projection.Payload, task, string(sourceUID), attempt)
	return err
}

func validateRestoredTaskSourceAttempt(task *corev1alpha1.Task, attempt *store.PromptAttempt, attemptID string) error {
	if task == nil || task.Status.Execution == nil || task.Status.AgentExecutionBinding == nil || attempt == nil {
		return fmt.Errorf("%w: restored Task source execution evidence is incomplete", store.ErrConflict)
	}
	binding := task.Status.AgentExecutionBinding
	if binding.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV2 ||
		attempt.ID != attemptID || attempt.Key.Namespace != task.Namespace ||
		attempt.Key.TaskUID != string(binding.Task.UID) || attempt.Key.Attempt != int64(task.Status.Execution.Attempt) ||
		attempt.Key.PromptID != task.Status.Execution.PromptID || attempt.BindingDigest != binding.BindingDigest ||
		attempt.SnapshotDigest != binding.Snapshot.Digest ||
		(strings.TrimSpace(task.Status.Execution.RequestDigest) != "" && attempt.RequestDigest != task.Status.Execution.RequestDigest) {
		return fmt.Errorf("%w: restored Task source PromptAttempt does not match its frozen execution binding", store.ErrConflict)
	}
	return nil
}

//nolint:gocyclo // Restore settlement must mirror every durable delivery/publication state without losing fail-closed cases.
func (d *ACPDispatcher) settleRestoredTerminalDelivery(
	ctx context.Context,
	task *corev1alpha1.Task,
	sourceUID types.UID,
	attempt *store.PromptAttempt,
	fence store.ControllerEpochFence,
) (*store.PromptAttempt, error) {
	if task == nil || task.Status.Execution == nil || attempt == nil || !store.IsTerminalPromptExecutionState(attempt.ExecutionState) {
		return nil, fmt.Errorf("%w: restored delivery settlement requires terminal source execution", store.ErrConflict)
	}
	publicationState := store.PublicationState("")
	if task.Spec.Workspace != nil && task.Spec.Workspace.Intent == corev1alpha1.WorkspaceIntentWrite {
		publicationID := publicationIDForTaskUID(task, sourceUID)
		publication, err := d.Store.GetPublication(ctx, publicationID)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				return nil, err
			}
			switch {
			case store.IsTerminalPromptDeliveryState(attempt.DeliveryState):
				return attempt, nil
			case attempt.DeliveryState == store.PromptDeliveryValidating:
				// Publication creation precedes the Validating -> Preparing
				// transition. A restored Validating attempt may therefore have no
				// Publication, and can settle fail-closed without inventing one.
			default:
				return nil, err
			}
		}
		if publication != nil {
			if publication.Namespace != task.Namespace || publication.TaskUID != string(sourceUID) ||
				publication.Attempt != int64(task.Status.Execution.Attempt) || publication.PromptID != task.Status.Execution.PromptID {
				return nil, fmt.Errorf("%w: restored Publication does not match frozen source identity", store.ErrConflict)
			}
			publicationState = publication.State
			if !store.IsTerminalPublicationState(publication.State) {
				const reason = "clean-cluster restore cannot prove the in-flight publication outcome"
				switch publication.State {
				case store.PublicationPreparing, store.PublicationPublishing:
					if err := d.transitionPublicationTerminal(ctx, publication, fence, store.PublicationOutcomeUnknown, reason); err != nil {
						return nil, err
					}
					publicationState = store.PublicationOutcomeUnknown
				case store.PublicationPrepared:
					if err := d.transitionPublicationTerminal(ctx, publication, fence, store.PublicationCancelledBeforePublish, reason); err != nil {
						return nil, err
					}
					publicationState = store.PublicationCancelledBeforePublish
				case store.PublicationVerifying:
					if publication.PreparedReceipt == nil || publication.PublishReceipt == nil {
						return nil, fmt.Errorf("%w: restored verifying Publication lacks durable prepare/publish receipts", store.ErrConflict)
					}
					op := publicationOperationID("restore-unknown", nil)
					digest, digestErr := acpDomainDigest("publication-restore-unknown", map[string]any{
						"id": publication.ID, "generation": publication.Generation, "version": publication.Version,
					})
					if digestErr != nil {
						return nil, digestErr
					}
					receipt := &store.PublicationVerificationReceipt{
						OperationID: op, RequestDigest: digest, Outcome: store.PublicationOutcomeUnknown,
						ExpectedCommitSHA: publication.PreparedReceipt.CommitSHA, VerifiedAt: time.Now().UTC(),
					}
					if _, err := d.transitionPublication(ctx, publication, fence, store.PublicationOutcomeUnknown, op, digest, nil, nil, receipt, reason); err != nil {
						return nil, err
					}
					publicationState = store.PublicationOutcomeUnknown
				default:
					return nil, fmt.Errorf("%w: unsupported restored Publication state %q", store.ErrConflict, publication.State)
				}
			}
		}
	}
	if store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
		return attempt, nil
	}
	to := store.PromptDeliveryPublicationOutcomeUnknown
	if publicationState != "" {
		to = promptDeliveryForPublication(publicationState)
	}
	switch attempt.DeliveryState {
	case store.PromptDeliveryValidating:
		if publicationState == "" {
			to = store.PromptDeliveryConflict
		}
	case store.PromptDeliveryPrepared:
	case store.PromptDeliveryPreparing, store.PromptDeliveryPublishing, store.PromptDeliveryVerifying:
	default:
		return nil, fmt.Errorf("%w: unsupported restored delivery state %q", store.ErrConflict, attempt.DeliveryState)
	}
	const reason = "clean-cluster restore terminalized an in-flight delivery without replay"
	if err := d.transitionDelivery(ctx, attempt.ID, fence, attempt.DeliveryState, to, "restore-delivery-settlement", reason); err != nil {
		return nil, err
	}
	return d.Store.GetPromptAttempt(ctx, attempt.ID)
}

func (d *ACPDispatcher) patchRestoredTaskTerminal(
	ctx context.Context,
	task *corev1alpha1.Task,
	epoch int64,
	state corev1alpha1.TaskExecutionState,
	outcome corev1alpha1.TaskExecutionOutcome,
	message string,
	delivery *corev1alpha1.TaskDeliveryStatus,
) error {
	if delivery == nil || !store.IsTerminalPromptDeliveryState(store.PromptDeliveryState(delivery.State)) ||
		string(delivery.Outcome) != string(delivery.State) {
		return fmt.Errorf("%w: restored Task terminal delivery evidence is incomplete", store.ErrConflict)
	}
	deliveryState := store.PromptDeliveryState(delivery.State)
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if latest.UID != task.UID || latest.Status.Execution == nil || latest.Status.AgentExecutionBinding == nil ||
			latest.Status.AgentExecutionBinding.Task.UID == latest.UID || task.Status.AgentExecutionBinding == nil ||
			latest.Status.AgentExecutionBinding.Task.UID != task.Status.AgentExecutionBinding.Task.UID ||
			latest.Status.AgentExecutionBinding.BindingDigest != task.Status.AgentExecutionBinding.BindingDigest ||
			latest.Status.Execution.Attempt != task.Status.Execution.Attempt ||
			latest.Status.Execution.PromptID != task.Status.Execution.PromptID {
			return fmt.Errorf("%w: restored Task incarnation changed during settlement", store.ErrConflict)
		}
		if acpTaskUsesRestoredSourceIdentity(latest) && latest.Status.Execution.State == state &&
			latest.Status.Execution.Outcome == outcome && latest.Status.Execution.ControllerEpoch == epoch &&
			reflect.DeepEqual(latest.Status.Delivery, delivery) {
			return nil
		}
		base := latest.DeepCopy()
		now := metav1.Now()
		latest.Status.Phase = restoredTaskPhase(state, deliveryState)
		latest.Status.Message = message
		latest.Status.Execution.State = state
		latest.Status.Execution.Outcome = outcome
		latest.Status.Execution.Reason = corev1alpha1.TaskExecutionReason(acpRestoreIdentityChangedReason)
		latest.Status.Execution.Message = message
		latest.Status.Execution.ControllerEpoch = epoch
		latest.Status.Execution.LastTransitionTime = &now
		latest.Status.Delivery = delivery.DeepCopy()
		latest.Status.CompletionTime = &now
		return d.Client.Status().Patch(ctx, latest, client.MergeFrom(base))
	})
}

func (d *ACPDispatcher) readRecoverableTask(
	ctx context.Context,
	candidate *corev1alpha1.Task,
) (*corev1alpha1.Task, bool, error) {
	if candidate == nil {
		return nil, false, nil
	}
	reader := d.APIReader
	if reader == nil {
		reader = d.Client
	}
	if reader == nil {
		return nil, false, fmt.Errorf("ACP recovery requires a Kubernetes reader")
	}
	latest := &corev1alpha1.Task{}
	err := reader.Get(ctx, types.NamespacedName{Namespace: candidate.Namespace, Name: candidate.Name}, latest)
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if latest.UID != candidate.UID ||
		(!latest.DeletionTimestamp.IsZero() && !acpTaskHasUnvalidatedSourceIdentity(latest)) {
		return nil, false, nil
	}
	return latest, true, nil
}

func (d *ACPDispatcher) recoverStaleTask(ctx context.Context, task *corev1alpha1.Task, fence store.ControllerEpochFence) error {
	attemptID, err := promptAttemptIDFromTask(task)
	if err != nil {
		return err
	}
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	sessionBound, err := promptAttemptSessionBound(attempt)
	if err != nil {
		return err
	}
	continuitySession := task.Spec.SessionRef != nil && sessionBound
	if store.IsTerminalPromptExecutionState(attempt.ExecutionState) && store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
		if continuitySession {
			if archived, err := d.recoverArchivedTerminalSession(ctx, task, attempt, fence); err != nil || archived {
				return err
			}
			if err := d.finalizeRecoveredTerminalSession(ctx, task, attempt, fence); err != nil {
				return err
			}
		}
		cleanupComplete, cleanupErr := d.cleanupRecoveredTaskScopedRuntimeSession(ctx, task)
		if cleanupErr != nil {
			return cleanupErr
		}
		if !cleanupComplete {
			return nil
		}
		exists, projectionErr := d.validateExistingStandaloneTaskProjection(ctx, task, attempt)
		if projectionErr != nil {
			return projectionErr
		}
		if exists {
			return d.patchRecoveredTerminalEpoch(ctx, task, fence.Epoch)
		}
	}
	switch attempt.ExecutionState {
	case store.PromptExecutionQueued, store.PromptExecutionReserved:
		return d.patchRecoveredTaskReserved(ctx, task, fence.Epoch, attempt.ExecutionState == store.PromptExecutionQueued)
	case store.PromptExecutionSessionStarting, store.PromptExecutionPlanned:
		digest, err := acpDomainDigest("pre-submission-recovery", map[string]any{
			"attemptID": attempt.ID, "state": attempt.ExecutionState, "version": attempt.Version, "epoch": fence.Epoch,
		})
		if err != nil {
			return err
		}
		if _, err := d.Store.RecoverPromptAttemptPreSubmission(ctx, store.PromptAttemptPreSubmissionRecovery{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			OperationID: "recover-pre-submit-" + strconv.FormatInt(fence.Epoch, 10), OperationDigest: digest, RecoveredAt: time.Now().UTC(),
		}); err != nil {
			return err
		}
		return d.patchRecoveredTaskReserved(ctx, task, fence.Epoch, false)
	case store.PromptExecutionSubmitting, store.PromptExecutionSubmittedUnknown,
		store.PromptExecutionAccepted, store.PromptExecutionRunning, store.PromptExecutionSettling:
		if recovered, err := d.recoverJournaledPromptTerminal(ctx, task, attempt, fence); err != nil {
			return err
		} else if recovered {
			return nil
		}
		const reason = "RuntimeLost"
		const message = "controller leadership changed after the prompt request-write boundary; outcome is unknown and was not replayed"
		recoveredAt := time.Now().UTC()
		if err := d.closeRecoveredPromptJournal(ctx, task, recoveredAt, message); err != nil {
			return err
		}
		if err := d.persistOutcomeUnknown(ctx, attempt.ID, fence, reason, message); err != nil {
			return err
		}
		if err := d.finalizeRecoveredSessionUnknown(ctx, task, fence, attempt.ID, reason); err != nil {
			return err
		}
		return d.failTask(ctx, task, corev1alpha1.TaskExecutionStateOutcomeUnknown, corev1alpha1.TaskExecutionOutcomeOutcomeUnknown, reason, message)
	case store.PromptExecutionSucceeded:
		if err := d.recoverSucceededTaskProjection(ctx, task, attempt, fence); err != nil {
			return err
		}
		return d.patchRecoveredTerminalEpoch(ctx, task, fence.Epoch)
	case store.PromptExecutionFailed, store.PromptExecutionCancelled, store.PromptExecutionOutcomeUnknown:
		if !continuitySession {
			return d.recoverMissingStandaloneTerminalProjection(ctx, task, attempt)
		}
		if err := d.finalizeRecoveredTerminalSession(ctx, task, attempt, fence); err != nil {
			return err
		}
		return d.patchRecoveredTerminalExecution(ctx, task, attempt, fence.Epoch)
	default:
		return fmt.Errorf("unsupported stale prompt attempt state %s", attempt.ExecutionState)
	}
}

// recoverArchivedTerminalSession recognizes completed Session deletion without
// recreating its control, transcript, turn, or outbox rows. Only the Task's epoch
// may advance after its immutable delivered projection and runtime retirement
// proof both validate. Legacy deletions without that evidence remain blocked.
func (d *ACPDispatcher) recoverArchivedTerminalSession(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
	fence store.ControllerEpochFence,
) (bool, error) {
	if _, err := d.Store.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name); err == nil {
		return false, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return false, err
	}
	reader, ok := d.Store.(store.SessionTurnCleanupReceiptStore)
	if !ok {
		return true, fmt.Errorf("%w: deleted Session has no cleanup receipt store", store.ErrNotFound)
	}
	receipt, err := reader.GetSessionTurnCleanupReceipt(ctx, task.Namespace, task.Spec.SessionRef.Name, attempt.ID)
	if err != nil {
		return true, fmt.Errorf("load archived Session terminal proof: %w", err)
	}
	if err := validateArchivedSessionTask(task, attempt, receipt); err != nil {
		return true, err
	}
	key := client.ObjectKeyFromObject(task)
	return true, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		reader := d.APIReader
		if reader == nil {
			reader = d.Client
		}
		if err := reader.Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if latest.UID != task.UID || !reflect.DeepEqual(latest.Spec.SessionRef, task.Spec.SessionRef) ||
			!reflect.DeepEqual(latest.Status.AgentExecutionBinding, task.Status.AgentExecutionBinding) {
			return fmt.Errorf("%w: archived Session Task identity changed during recovery", store.ErrConflict)
		}
		if err := validateArchivedSessionTask(latest, attempt, receipt); err != nil {
			return err
		}
		if latest.Status.Execution.ControllerEpoch >= fence.Epoch {
			return nil
		}
		base := latest.DeepCopy()
		latest.Status.Execution.ControllerEpoch = fence.Epoch
		return d.Client.Status().Patch(ctx, latest, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	})
}

func validateArchivedSessionTask(task *corev1alpha1.Task, attempt *store.PromptAttempt, receipt *store.SessionTurnCleanupReceipt) error {
	if task == nil || task.Spec.SessionRef == nil || task.Status.Execution == nil || attempt == nil || receipt == nil {
		return fmt.Errorf("%w: archived Session terminal proof is incomplete", store.ErrConflict)
	}
	key := store.SessionTurnKey{
		SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration,
		TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
	}
	turnID, err := key.CanonicalID()
	if err != nil {
		return err
	}
	if err := receipt.Validate(task.Namespace, task.Spec.SessionRef.Name, turnID); err != nil {
		return err
	}
	if receipt.Key != key || receipt.PromptAttemptID != attempt.ID || receipt.ProjectionState != store.OutboxProjectionDelivered {
		return fmt.Errorf("%w: archived Session proof is not the delivered terminal projection for this attempt", store.ErrConflict)
	}
	projection, err := taskterminal.ValidateFinalizedSessionProjection(receipt.Payload, task, string(task.UID), attempt, receipt.SessionTurn())
	if err != nil {
		return err
	}
	if task.Status.Phase != projection.Phase || task.Status.Execution.State != projection.Execution.State ||
		task.Status.Execution.Outcome != projection.Execution.Outcome {
		return fmt.Errorf("%w: archived Session terminal classification differs from the Task", store.ErrConflict)
	}
	if !runtimeSessionCleanupCompleteForUID(task, task.UID) {
		return fmt.Errorf("%w: archived Session Task has no exact runtime retirement proof", store.ErrNotReady)
	}
	return nil
}

func (d *ACPDispatcher) closeRecoveredPromptJournal(
	ctx context.Context,
	task *corev1alpha1.Task,
	at time.Time,
	diagnostic string,
) error {
	identity, ok := mappedPromptRecoveryIdentity(task)
	if !ok || d.EventStore == nil {
		return nil
	}
	state, err := (v2eventjournal.Journal{
		EventStore:       d.EventStore,
		MapContext:       mappedPromptRecoveryContext(task),
		RecoveryIdentity: identity,
	}).Open(ctx)
	if err != nil {
		return err
	}
	if err := state.AppendPersistedToolClosuresIfNew(ctx, at); err != nil {
		return err
	}
	_, _, err = state.AppendPromptStreamFailureIfNew(ctx, at, diagnostic)
	return err
}

func mappedPromptRecoveryIdentity(task *corev1alpha1.Task) (v2eventjournal.MappedUpdateIdentity, bool) {
	if task == nil {
		return v2eventjournal.MappedUpdateIdentity{}, false
	}
	return mappedPromptRecoveryIdentityForTaskUID(task, task.UID)
}

func mappedPromptRecoveryIdentityForTaskUID(
	task *corev1alpha1.Task,
	taskUID types.UID,
) (v2eventjournal.MappedUpdateIdentity, bool) {
	if task == nil || task.Status.Execution == nil || taskUID == "" {
		return v2eventjournal.MappedUpdateIdentity{}, false
	}
	execution := task.Status.Execution
	if execution.Attempt < 1 || strings.TrimSpace(execution.PromptID) == "" ||
		strings.TrimSpace(execution.RuntimeInstanceID) == "" ||
		strings.TrimSpace(execution.RuntimeSessionSupervisorBootID) == "" ||
		strings.TrimSpace(execution.RuntimeSessionUID) == "" || execution.RuntimeSessionGeneration < 1 {
		return v2eventjournal.MappedUpdateIdentity{}, false
	}
	return v2eventjournal.MappedUpdateIdentity{
		Protocol:                 harnessv2.ProtocolVersion,
		RuntimeInstanceID:        harnessv2.RuntimeInstanceID(execution.RuntimeInstanceID),
		SupervisorBootID:         harnessv2.SupervisorBootID(execution.RuntimeSessionSupervisorBootID),
		RuntimeSessionUID:        harnessv2.RuntimeSessionUID(execution.RuntimeSessionUID),
		RuntimeSessionGeneration: uint64(execution.RuntimeSessionGeneration),
		TaskUID:                  harnessv2.TaskUID(taskUID),
		TaskAttempt:              uint32(execution.Attempt),
		PromptID:                 harnessv2.PromptID(execution.PromptID),
	}, true
}

func (d *ACPDispatcher) reconcileRestoredJournaledPromptTerminal(
	ctx context.Context,
	task *corev1alpha1.Task,
	sourceUID types.UID,
	attempt *store.PromptAttempt,
	fence store.ControllerEpochFence,
) (*store.PromptAttempt, error) {
	if attempt == nil || d.EventStore == nil {
		return attempt, nil
	}
	switch attempt.ExecutionState {
	case store.PromptExecutionAccepted, store.PromptExecutionRunning, store.PromptExecutionSettling,
		store.PromptExecutionSucceeded:
	default:
		return attempt, nil
	}
	identity, ok := mappedPromptRecoveryIdentityForTaskUID(task, sourceUID)
	if !ok {
		return attempt, nil
	}
	evidence, err := (v2eventjournal.Journal{
		EventStore:       d.EventStore,
		MapContext:       mappedPromptRecoveryContext(task),
		RecoveryIdentity: identity,
	}).FindPromptTerminal(ctx)
	if err != nil {
		return nil, err
	}
	if evidence == nil {
		evidence, err = d.recoverSettlingResultTerminal(ctx, task, attempt, identity)
		if err != nil || evidence == nil {
			return attempt, err
		}
	}
	switch evidence.TerminalEvent {
	case harnessv2.EventCompleted:
		if _, err := d.ResultStore.GetResult(ctx, task.Namespace, task.Name); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return attempt, nil
			}
			return nil, err
		}
		if attempt.ExecutionState == store.PromptExecutionSucceeded {
			if err := d.publishTaskResultReference(ctx, task); err != nil {
				return nil, err
			}
			return attempt, nil
		}
		if attempt.ExecutionState != store.PromptExecutionSettling {
			if err := d.transitionAttempt(
				ctx, attempt.ID, fence, attempt.ExecutionState, store.PromptExecutionSettling,
				"recover-restored-journal-terminal-settling", nil,
			); err != nil {
				return nil, err
			}
		}
		if err := d.transitionAttempt(
			ctx, attempt.ID, fence, store.PromptExecutionSettling, store.PromptExecutionSucceeded,
			"recover-restored-journal-terminal-succeeded", nil,
		); err != nil {
			return nil, err
		}
		if err := d.publishTaskResultReference(ctx, task); err != nil {
			return nil, err
		}
	case harnessv2.EventCancelled:
		if err := d.transitionAttemptToTerminal(
			ctx, attempt.ID, fence, store.PromptExecutionCancelled, "recover-restored-journal-terminal-cancelled",
		); err != nil {
			return nil, err
		}
	case harnessv2.EventOutcomeUnknown:
		if err := d.persistOutcomeUnknown(
			ctx, attempt.ID, fence, "RuntimeLost", "journaled prompt outcome is unknown",
		); err != nil {
			return nil, err
		}
	default:
		// Recover with the same durable classification the live path writes:
		// the journaled (already redacted) failure code/message become the
		// PromptAttempt's TerminalReason/OutcomeMarker instead of the generic
		// "prompt failed" default.
		failureMessage := acpPromptFailureMessage(harnessv2.Event{
			Type:   harnessv2.EventFailed,
			Failed: &harnessv2.FailedEvent{Code: evidence.FailureCode, Message: evidence.FailureMessage},
		})
		if err := d.transitionAttemptToFailed(
			ctx, attempt.ID, fence, "recover-restored-journal-terminal-failed", acpPromptFailedReason, failureMessage,
		); err != nil {
			return nil, err
		}
	}
	return d.Store.GetPromptAttempt(ctx, attempt.ID)
}

// recoveredTerminalEvent rebuilds the terminal event from journaled evidence
// so a recovered failure keeps the (already redacted) code/message the live
// path would have projected instead of the generic "prompt failed".
func recoveredTerminalEvent(evidence *v2eventjournal.PromptTerminalEvidence) harnessv2.Event {
	event := harnessv2.Event{Type: evidence.TerminalEvent}
	if evidence.TerminalEvent == harnessv2.EventFailed && (evidence.FailureCode != "" || evidence.FailureMessage != "") {
		event.Failed = &harnessv2.FailedEvent{Code: evidence.FailureCode, Message: evidence.FailureMessage}
	}
	return event
}

func mappedPromptRecoveryContext(task *corev1alpha1.Task) v2eventjournal.MapContext {
	if task == nil {
		return v2eventjournal.MapContext{}
	}
	return v2eventjournal.MapContext{
		Namespace: task.Namespace, TaskName: task.Name, StreamID: task.Name, SessionName: taskSessionName(task),
	}
}

func (d *ACPDispatcher) recoverJournaledPromptTerminal(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
	fence store.ControllerEpochFence,
) (bool, error) {
	identity, ok := mappedPromptRecoveryIdentity(task)
	if !ok || d.EventStore == nil {
		return false, nil
	}
	evidence, err := (v2eventjournal.Journal{
		EventStore:       d.EventStore,
		MapContext:       mappedPromptRecoveryContext(task),
		RecoveryIdentity: identity,
	}).FindPromptTerminal(ctx)
	if err != nil {
		return false, err
	}
	if evidence == nil {
		evidence, err = d.recoverSettlingResultTerminal(ctx, task, attempt, identity)
		if err != nil || evidence == nil {
			return false, err
		}
	}
	if evidence.TerminalEvent != harnessv2.EventCompleted {
		session, err := d.recoveredTaskSession(ctx, task, attempt)
		if err != nil {
			return true, err
		}
		return true, d.finishNonSuccessWithCancellationReason(
			ctx, task, attempt.ID, fence, session, recoveredTerminalEvent(evidence), evidence.CancellationReason,
		)
	}
	if _, err := d.ResultStore.GetResult(ctx, task.Namespace, task.Name); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return true, err
	}
	switch attempt.ExecutionState {
	case store.PromptExecutionAccepted, store.PromptExecutionRunning:
		if err := d.transitionAttempt(
			ctx, attempt.ID, fence, attempt.ExecutionState, store.PromptExecutionSettling,
			"recover-journal-terminal-settling", nil,
		); err != nil {
			return true, err
		}
	case store.PromptExecutionSettling:
	default:
		return false, nil
	}
	if err := d.transitionAttempt(
		ctx, attempt.ID, fence, store.PromptExecutionSettling, store.PromptExecutionSucceeded,
		"recover-journal-terminal-succeeded", nil,
	); err != nil {
		return true, err
	}
	attempt, err = d.Store.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		return true, err
	}
	if attempt.DeliveryState == store.PromptDeliveryNotRequested {
		if err := d.transitionDelivery(
			ctx, attempt.ID, fence, store.PromptDeliveryNotRequested, store.PromptDeliveryValidating,
			"recover-journal-terminal-workspace-validation", "",
		); err != nil {
			return true, err
		}
		attempt, err = d.Store.GetPromptAttempt(ctx, attempt.ID)
		if err != nil {
			return true, err
		}
	}
	if err := d.publishTaskResultReference(ctx, task); err != nil {
		return true, err
	}
	if err := d.recoverSucceededTaskProjection(ctx, task, attempt, fence); err != nil {
		return true, err
	}
	return true, d.patchRecoveredTerminalEpoch(ctx, task, fence.Epoch)
}

func (d *ACPDispatcher) recoverSettlingResultTerminal(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
	identity v2eventjournal.MappedUpdateIdentity,
) (*v2eventjournal.PromptTerminalEvidence, error) {
	if attempt.ExecutionState != store.PromptExecutionSettling || attempt.Version < 2 {
		return nil, nil
	}
	receiptVersion := attempt.Version - 1
	operationID := acpSettlingOperation + "-" + strconv.FormatInt(receiptVersion, 10)
	var result []byte
	receipts, ok := d.ResultStore.(store.PromptResultReceiptStore)
	if !ok {
		return nil, fmt.Errorf("ACP settling recovery requires a result store with prompt result receipts")
	}
	receipt, receiptErr := receipts.GetPromptResultReceipt(ctx, attempt.ID)
	switch {
	case receiptErr == nil:
		digest, err := acpSettlingTransitionDigest(attempt.ID, receiptVersion, receipt.Data)
		if err != nil {
			return nil, err
		}
		if receipt.Namespace != task.Namespace || receipt.TaskName != task.Name ||
			receipt.OperationID != operationID || receipt.OperationDigest != digest ||
			attempt.LastOperationID != operationID || attempt.LastOperationDigest != digest {
			return nil, nil
		}
		result = receipt.Data
		if err := d.persistTaskResult(ctx, task, result); err != nil {
			return nil, err
		}
	case errors.Is(receiptErr, store.ErrNotFound):
		var err error
		result, err = d.ResultStore.GetResult(ctx, task.Namespace, task.Name)
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		digest, err := acpSettlingTransitionDigest(attempt.ID, receiptVersion, result)
		if err != nil {
			return nil, err
		}
		if attempt.LastOperationID != operationID || attempt.LastOperationDigest != digest {
			return nil, nil
		}
	default:
		return nil, receiptErr
	}
	if attempt.UpdatedAt.IsZero() {
		return nil, fmt.Errorf("settling prompt attempt %s has no receipt timestamp", attempt.ID)
	}
	journal := v2eventjournal.Journal{
		EventStore:       d.EventStore,
		MapContext:       mappedPromptRecoveryContext(task),
		RecoveryIdentity: identity,
	}
	state, err := journal.Open(ctx)
	if err != nil {
		return nil, err
	}
	if err := state.AppendPersistedToolClosuresIfNew(ctx, attempt.UpdatedAt.UTC()); err != nil {
		return nil, err
	}
	settlement := harnessv2.PromptSettlement{
		TerminalEvent: harnessv2.EventCompleted,
		Outcome:       harnessv2.PromptOutcomeSucceeded,
		StopReason:    harnessv2.ACPStopReasonEndTurn,
		SettledAt:     attempt.UpdatedAt.UTC(),
	}
	if _, _, err := state.AppendPromptSettlementIfNew(ctx, settlement, ""); err != nil {
		return nil, err
	}
	return journal.FindPromptTerminal(ctx)
}

func taskScopedRuntimeSessionCleanupDigest(
	taskUID types.UID,
	attempt int32,
	runtimeInstanceID string,
	runtimeSessionUID string,
	runtimeSessionGeneration int64,
) (string, error) {
	if taskUID == "" || attempt < 1 || strings.TrimSpace(runtimeInstanceID) == "" ||
		strings.TrimSpace(runtimeSessionUID) == "" || runtimeSessionGeneration < 1 {
		return "", fmt.Errorf("%w: task-scoped RuntimeSession cleanup identity is incomplete", store.ErrConflict)
	}
	return acpDomainDigest("task-runtime-session-cleanup", map[string]any{
		"taskUID": string(taskUID), "attempt": attempt, "runtimeInstanceID": runtimeInstanceID,
		"runtimeSessionUID": runtimeSessionUID, "runtimeSessionGeneration": runtimeSessionGeneration,
	})
}

func taskScopedRuntimeSessionCleanupComplete(task *corev1alpha1.Task) bool {
	return taskScopedRuntimeSessionCleanupCompleteForUID(task, acpTaskControlUID(task))
}

func taskScopedRuntimeSessionCleanupCompleteForUID(task *corev1alpha1.Task, taskUID types.UID) bool {
	if task == nil || task.Status.Execution == nil || strings.TrimSpace(task.Status.Execution.RuntimeSessionUID) == "" {
		return true
	}
	if task.DeletionTimestamp.IsZero() && task.Spec.SessionRef != nil &&
		(task.Spec.Workspace == nil || task.Spec.Workspace.Intent != corev1alpha1.WorkspaceIntentWrite) {
		// A live read Session retains its conversation process between Tasks.
		// Deleting Tasks must keep their frozen authority until Session cleanup
		// has recorded an exact runtime cleanup receipt.
		return true
	}
	return runtimeSessionCleanupCompleteForUID(task, taskUID)
}

func runtimeSessionCleanupCompleteForUID(task *corev1alpha1.Task, taskUID types.UID) bool {
	if task == nil || task.Status.Execution == nil || strings.TrimSpace(task.Status.Execution.RuntimeSessionUID) == "" {
		return true
	}
	if taskHasAgentRuntimeDrainCleanupProofForUID(task, taskUID) {
		return true
	}
	digest, err := taskScopedRuntimeSessionCleanupDigest(
		taskUID, task.Status.Execution.Attempt, task.Status.Execution.RuntimeInstanceID,
		task.Status.Execution.RuntimeSessionUID, task.Status.Execution.RuntimeSessionGeneration,
	)
	return err == nil && task.Status.Execution.RuntimeSessionCleanupDigest == digest
}

func agentRuntimeDrainCleanupProofDigest(
	taskUID types.UID,
	binding *corev1alpha1.AgentExecutionBinding,
) (string, error) {
	if taskUID == "" || binding == nil || binding.Task.UID != taskUID ||
		binding.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV2 ||
		binding.Backend != corev1alpha1.AgentExecutionBackendExternalEndpoint || binding.RuntimeRef == nil ||
		strings.TrimSpace(binding.BindingDigest) == "" || strings.TrimSpace(binding.RuntimeRef.Name) == "" ||
		binding.RuntimeRef.UID == "" || binding.RuntimeRef.Generation < 1 {
		return "", fmt.Errorf("%w: AgentRuntime drain cleanup proof identity is incomplete", store.ErrConflict)
	}
	canonicalDigest, err := canonicalAgentExecutionBindingDigest(*binding)
	if err != nil || canonicalDigest != binding.BindingDigest {
		return "", fmt.Errorf("%w: AgentRuntime drain cleanup proof binding failed canonical integrity verification", store.ErrConflict)
	}
	return acpDomainDigest("agent-runtime-drain-cleanup", map[string]any{
		"taskUID": string(taskUID), agentRuntimeDrainBindingDigestKey: binding.BindingDigest,
		"agentRuntimeName": binding.RuntimeRef.Name, "agentRuntimeUID": string(binding.RuntimeRef.UID),
		"agentRuntimeGeneration": binding.RuntimeRef.Generation,
	})
}

func taskHasAgentRuntimeDrainCleanupProofForUID(task *corev1alpha1.Task, taskUID types.UID) bool {
	if task == nil || task.Status.Execution == nil {
		return false
	}
	binding := executionBinding(task, corev1alpha1.AgentRuntimeContractHarnessV2)
	execution := task.Status.Execution
	if binding == nil || binding.Backend != corev1alpha1.AgentExecutionBackendExternalEndpoint ||
		binding.RuntimeRef == nil || execution.AgentRuntimeName != binding.RuntimeRef.Name ||
		execution.AgentRuntimeUID != string(binding.RuntimeRef.UID) ||
		execution.RuntimePoolName != "" || execution.RuntimePoolUID != "" {
		return false
	}
	digest, err := agentRuntimeDrainCleanupProofDigest(taskUID, binding)
	return err == nil && execution.RuntimeSessionCleanupDigest == digest
}

func (d *ACPDispatcher) markTaskScopedRuntimeSessionCleanupComplete(
	ctx context.Context,
	task *corev1alpha1.Task,
	taskUID types.UID,
	runtimeInstanceID string,
	runtimeSessionUID string,
	runtimeSessionGeneration int64,
) error {
	return persistTaskScopedRuntimeSessionCleanupReceipt(
		ctx, d.Client, task, taskUID, runtimeInstanceID, runtimeSessionUID, runtimeSessionGeneration,
	)
}

func persistTaskScopedRuntimeSessionCleanupReceipt(
	ctx context.Context,
	kubeClient client.Client,
	task *corev1alpha1.Task,
	taskUID types.UID,
	runtimeInstanceID string,
	runtimeSessionUID string,
	runtimeSessionGeneration int64,
) error {
	if task == nil || task.Status.Execution == nil {
		return nil
	}
	digest, err := taskScopedRuntimeSessionCleanupDigest(
		taskUID, task.Status.Execution.Attempt, runtimeInstanceID, runtimeSessionUID, runtimeSessionGeneration,
	)
	if err != nil {
		return err
	}
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := kubeClient.Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if latest.Status.Execution == nil {
			return fmt.Errorf("%w: Task execution status is missing during RuntimeSession cleanup receipt", store.ErrConflict)
		}
		if latest.Status.Execution.Attempt != task.Status.Execution.Attempt ||
			latest.Status.Execution.RuntimeInstanceID != runtimeInstanceID ||
			latest.Status.Execution.RuntimeSessionUID != runtimeSessionUID ||
			latest.Status.Execution.RuntimeSessionGeneration != runtimeSessionGeneration ||
			sessionRuntimeCleanupIdentityForExecution(latest.Status.Execution) != sessionRuntimeCleanupIdentityForExecution(task.Status.Execution) {
			return fmt.Errorf("%w: Task execution identity changed during RuntimeSession cleanup receipt", store.ErrConflict)
		}
		if taskUID != latest.UID {
			binding := executionBinding(latest, corev1alpha1.AgentRuntimeContractHarnessV2)
			if binding == nil || binding.Task.UID != taskUID {
				return fmt.Errorf("%w: restored Task source identity changed during RuntimeSession cleanup receipt", store.ErrConflict)
			}
		}
		if taskHasAgentRuntimeDrainCleanupProofForUID(latest, taskUID) {
			return nil
		}
		if latest.Status.Execution.RuntimeSessionCleanupDigest == digest {
			return nil
		}
		latest.Status.Execution.RuntimeSessionCleanupDigest = digest
		return kubeClient.Status().Update(ctx, latest)
	})
}

func persistAgentRuntimeDrainCleanupProof(
	ctx context.Context,
	kubeClient client.Client,
	task *corev1alpha1.Task,
	taskUID types.UID,
	authority drainedAgentRuntimeTaskAuthority,
) error {
	if task == nil || task.Status.Execution == nil || task.Status.AgentExecutionBinding == nil {
		return fmt.Errorf("%w: Task AgentRuntime drain cleanup proof identity is incomplete", store.ErrConflict)
	}
	expectedBindingDigest := task.Status.AgentExecutionBinding.BindingDigest
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := kubeClient.Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		binding := executionBinding(latest, corev1alpha1.AgentRuntimeContractHarnessV2)
		if latest.Status.Execution == nil || binding == nil || binding.BindingDigest != expectedBindingDigest ||
			acpTaskControlUID(latest) != taskUID ||
			binding.Backend != corev1alpha1.AgentExecutionBackendExternalEndpoint || binding.RuntimeRef == nil ||
			binding.RuntimeRef.Name != authority.name || binding.RuntimeRef.UID != authority.uid ||
			binding.RuntimeRef.Generation != authority.generation || binding.RuntimeProfileDigest != authority.profileDigest ||
			latest.Status.Execution.AgentRuntimeName != binding.RuntimeRef.Name ||
			latest.Status.Execution.AgentRuntimeUID != string(binding.RuntimeRef.UID) ||
			latest.Status.Execution.RuntimePoolName != "" || latest.Status.Execution.RuntimePoolUID != "" {
			return fmt.Errorf("%w: Task identity changed during AgentRuntime drain cleanup proof", store.ErrConflict)
		}
		execution := latest.Status.Execution
		sessionIdentityAbsent := execution.RuntimeInstanceID == "" && execution.RuntimeSessionUID == "" &&
			execution.RuntimeSessionGeneration == 0 && execution.RuntimeSessionSupervisorBootID == ""
		sessionIdentityMatches := execution.RuntimeInstanceID == authority.runtimeInstanceID &&
			strings.TrimSpace(execution.RuntimeSessionUID) != "" && execution.RuntimeSessionGeneration >= 1 &&
			(execution.RuntimeSessionSupervisorBootID == "" ||
				execution.RuntimeSessionSupervisorBootID == authority.supervisorBootID)
		if !sessionIdentityAbsent && !sessionIdentityMatches {
			return fmt.Errorf("%w: Task RuntimeSession authority changed during AgentRuntime drain cleanup proof", store.ErrConflict)
		}
		digest, err := agentRuntimeDrainCleanupProofDigest(taskUID, binding)
		if err != nil {
			return err
		}
		if latest.Status.Execution.RuntimeSessionCleanupDigest == digest {
			return nil
		}
		latest.Status.Execution.RuntimeSessionCleanupDigest = digest
		return kubeClient.Status().Update(ctx, latest)
	})
}

func (d *ACPDispatcher) prepareRecoveredTaskScopedRuntimeSessionForSettlement(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	return d.reconcileRecoveredTaskScopedRuntimeSession(ctx, task, acpTaskControlUID(task), false)
}

func (d *ACPDispatcher) cleanupRecoveredTaskScopedRuntimeSession(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	return d.cleanupRecoveredTaskScopedRuntimeSessionForUID(ctx, task, acpTaskControlUID(task))
}

func (d *ACPDispatcher) cleanupRecoveredTaskScopedRuntimeSessionForUID(ctx context.Context, task *corev1alpha1.Task, taskUID types.UID) (bool, error) {
	return d.reconcileRecoveredTaskScopedRuntimeSession(ctx, task, taskUID, true)
}

func (d *ACPDispatcher) verifiedExternalRuntimeRecoveryTarget(
	ctx context.Context,
	task *corev1alpha1.Task,
	taskUID types.UID,
	runtime *corev1alpha1.AgentRuntime,
) (*agentExecutionSnapshotExternalRuntime, harnessv2.RuntimeProfile, harnessv2.MCPPolicyConfiguration, error) {
	if d.Snapshots == nil {
		return nil, harnessv2.RuntimeProfile{}, harnessv2.MCPPolicyConfiguration{}, errors.New("immutable execution snapshot store is required for external RuntimeSession recovery")
	}
	binding := executionBinding(task, corev1alpha1.AgentRuntimeContractHarnessV2)
	if binding == nil || binding.Backend != corev1alpha1.AgentExecutionBackendExternalEndpoint ||
		binding.RuntimeRef == nil || binding.Task.UID != taskUID || task.Status.Execution == nil {
		return nil, harnessv2.RuntimeProfile{}, harnessv2.MCPPolicyConfiguration{}, errors.New("external RuntimeSession recovery binding is incomplete")
	}
	if binding.RuntimeRef.Name != task.Status.Execution.AgentRuntimeName ||
		string(binding.RuntimeRef.UID) != task.Status.Execution.AgentRuntimeUID ||
		task.Status.Execution.RuntimePoolName != "" || task.Status.Execution.RuntimePoolUID != "" {
		return nil, harnessv2.RuntimeProfile{}, harnessv2.MCPPolicyConfiguration{}, errors.New("external RuntimeSession recovery identity does not match the immutable binding")
	}
	canonicalDigest, err := canonicalAgentExecutionBindingDigest(*binding)
	if err != nil || canonicalDigest != binding.BindingDigest {
		return nil, harnessv2.RuntimeProfile{}, harnessv2.MCPPolicyConfiguration{}, errors.New("external RuntimeSession recovery binding failed canonical integrity verification")
	}
	snapshot, err := d.Snapshots.GetAgentExecutionSnapshot(ctx, store.AgentExecutionSnapshotKey{
		TaskUID: string(binding.Task.UID), Digest: binding.Snapshot.Digest,
	})
	if err != nil {
		return nil, harnessv2.RuntimeProfile{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("load immutable external RuntimeSession recovery snapshot: %w", err)
	}
	body, err := decodeAgentExecutionSnapshot(snapshot.Body)
	if err != nil {
		return nil, harnessv2.RuntimeProfile{}, harnessv2.MCPPolicyConfiguration{}, err
	}
	plan, _, mcpConfiguration, err := validateAgentExecutionSnapshot(binding, snapshot, body)
	if err != nil {
		return nil, harnessv2.RuntimeProfile{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("validate immutable external RuntimeSession recovery snapshot: %w", err)
	}
	if body.ExternalRuntime == nil || body.ExternalRuntime.Namespace != task.Namespace || runtime == nil ||
		runtime.Name != binding.RuntimeRef.Name || runtime.UID != binding.RuntimeRef.UID {
		return nil, harnessv2.RuntimeProfile{}, harnessv2.MCPPolicyConfiguration{}, errors.New("external AgentRuntime identity changed after binding")
	}
	return body.ExternalRuntime, plan.Profile, mcpConfiguration, nil
}

// externalRuntimeRotatedEndpointCleanupClient reconstructs cleanup authority
// from the immutable binding when the live AgentRuntime now points elsewhere.
// The frozen endpoint must authenticate the exact resident runtime fence; the
// live object remains authoritative only for the AgentRuntime UID.
func (d *ACPDispatcher) externalRuntimeRotatedEndpointCleanupClient(
	ctx context.Context,
	current *corev1alpha1.AgentRuntime,
	frozen *agentExecutionSnapshotExternalRuntime,
	runtimeProfileDigest harnessv2.ProfileDigest,
	protocolLimits harnessv2.ProtocolLimits,
	expectedRuntimeInstanceID harnessv2.RuntimeInstanceID,
	expectedSupervisorBootID harnessv2.SupervisorBootID,
	sessionCleanup *sessionRuntimeCleanupFence,
) (*harnessv2.Client, harnessv2.Fence, error) {
	if current == nil || frozen == nil || runtimeProfileDigest == "" ||
		expectedRuntimeInstanceID == "" || expectedSupervisorBootID == "" {
		return nil, harnessv2.Fence{}, errors.New("external AgentRuntime rotated-endpoint cleanup target is incomplete")
	}
	if err := protocolLimits.Validate(); err != nil {
		return nil, harnessv2.Fence{}, fmt.Errorf("external AgentRuntime rotated-endpoint cleanup limits are invalid: %w", err)
	}
	frozenRuntime, err := frozenAgentRuntimeForCleanup(current, frozen)
	if err != nil {
		return nil, harnessv2.Fence{}, err
	}
	reconciler := &AgentRuntimeReconciler{Client: d.Client, APIReader: d.APIReader}
	pins, err := reconciler.AgentRuntimeServiceBackendPins(ctx, frozenRuntime)
	if err != nil {
		return nil, harnessv2.Fence{}, err
	}
	auth, err := reconciler.agentRuntimeAuthMaterial(ctx, frozenRuntime)
	if err != nil {
		return nil, harnessv2.Fence{}, err
	}
	if string(auth.controllerSecretUID) != frozen.ControllerAuth.UID ||
		auth.controllerResourceVersion != frozen.ControllerAuth.ResourceVersion ||
		string(auth.capabilitySecretUID) != frozen.OperationCapability.UID ||
		auth.capabilityResourceVersion != frozen.OperationCapability.ResourceVersion {
		return nil, harnessv2.Fence{}, errors.New("external AgentRuntime frozen cleanup authentication authority changed")
	}
	runtimeEpoch, err := d.externalRuntimeCleanupEpoch(ctx, sessionCleanup)
	if err != nil {
		return nil, harnessv2.Fence{}, err
	}
	probeAuthority := &externalRuntimeCleanupAuthority{
		runtimeKey:           types.NamespacedName{Namespace: current.Namespace, Name: current.Name},
		runtimeUID:           current.UID,
		frozenRuntime:        frozenRuntime.DeepCopy(),
		auth:                 auth,
		serviceBackendPins:   slices.Clone(pins),
		runtimeInstanceID:    expectedRuntimeInstanceID,
		supervisorBootID:     expectedSupervisorBootID,
		runtimeProfileDigest: runtimeProfileDigest,
		protocolLimits:       protocolLimits,
	}
	probe, err := d.newExternalRuntimeCleanupHTTPClient(probeAuthority, runtimeProfileDigest, false)
	if err != nil {
		return nil, harnessv2.Fence{}, err
	}
	status, err := probe.Status(ctx)
	if err != nil {
		return nil, harnessv2.Fence{}, err
	}
	if status == nil || status.Fence.RuntimeInstanceID != expectedRuntimeInstanceID ||
		status.Fence.SupervisorBootID != expectedSupervisorBootID ||
		status.Fence.ControllerEpoch != runtimeEpoch ||
		status.Fence.RuntimePoolUID == "" || status.Fence.RuntimePoolGeneration < 1 ||
		status.Fence.RuntimeProfileDigest != runtimeProfileDigest ||
		status.Fence.ProfileDigestSchemaVersion != harnessv2.ProfileDigestSchemaVersion {
		return nil, harnessv2.Fence{}, errors.New("external AgentRuntime authenticated rotated-endpoint cleanup status fence changed")
	}
	authority, err := newExternalRuntimeCleanupAuthority(
		current, frozenRuntime, auth, pins, expectedRuntimeInstanceID, expectedSupervisorBootID,
		status.Fence.RuntimePoolUID, status.Fence.RuntimePoolGeneration, runtimeProfileDigest, protocolLimits,
	)
	if err != nil {
		return nil, harnessv2.Fence{}, err
	}
	authority.sessionCleanup = sessionCleanup
	runtimeFence := harnessv2.Fence{
		RuntimeInstanceID: authority.runtimeInstanceID, SupervisorBootID: authority.supervisorBootID,
		ControllerEpoch: runtimeEpoch, RuntimePoolUID: authority.runtimePoolUID,
		RuntimePoolGeneration: authority.runtimePoolGeneration, RuntimeProfileDigest: authority.runtimeProfileDigest,
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	if err := validateExternalRuntimeRotatedEndpointCleanupStatus(runtimeFence, status); err != nil {
		return nil, harnessv2.Fence{}, err
	}
	runtimeClient, err := d.newExternalRuntimeRotatedEndpointCleanupHTTPClient(authority)
	if err != nil {
		return nil, harnessv2.Fence{}, err
	}
	return runtimeClient, runtimeFence, nil
}

func (d *ACPDispatcher) newExternalRuntimeRotatedEndpointCleanupHTTPClient(
	authority *externalRuntimeCleanupAuthority,
) (*harnessv2.Client, error) {
	if authority == nil || authority.frozenRuntime == nil || authority.runtimeProfileDigest == "" {
		return nil, errors.New("external AgentRuntime rotated-endpoint cleanup authority is incomplete")
	}
	options := []harnessv2.ClientOption{
		harnessv2.WithControlTimeout(runtimeSessionCreateTimeout(acpDispatchTarget{external: authority.frozenRuntime})),
		harnessv2.WithControllerBearerToken(authority.auth.controllerBearerToken),
		harnessv2.WithOperationCapabilitySecret(authority.auth.operationCapabilitySecret),
		harnessv2.WithProtocolLimits(authority.protocolLimits),
		harnessv2.WithStatusCapabilityBinding(harnessv2.StatusCapabilityBinding{
			RuntimeProfileDigest: authority.runtimeProfileDigest, RuntimeInstanceID: authority.runtimeInstanceID,
		}),
		harnessv2.WithBeforeMutation(func(validateCtx context.Context, operation string) error {
			if !externalRuntimeCleanupMutationAllowed(authority, operation) {
				return errors.New("external AgentRuntime cleanup client cannot perform admission or non-cleanup mutations")
			}
			return d.revalidateExternalRuntimeRotatedEndpointCleanupMutation(validateCtx, authority)
		}),
	}
	if len(authority.serviceBackendPins) > 0 {
		options = append(options, harnessv2.WithHTTPClient(
			externalRuntimeHTTPClient(PinnedBackendDialTransport(authority.serviceBackendPins)),
		))
	} else if agentRuntimeEndpointRequiresPublicDial(authority.frozenRuntime.Spec.Deployment.Endpoint) {
		options = append(options, harnessv2.WithHTTPClient(
			externalRuntimeHTTPClient(v2conformance.PublicAddressDialTransport()),
		))
	}
	return harnessv2.NewClient(authority.frozenRuntime.Spec.Deployment.Endpoint, options...)
}

func (d *ACPDispatcher) revalidateExternalRuntimeRotatedEndpointCleanupMutation(
	ctx context.Context,
	authority *externalRuntimeCleanupAuthority,
) error {
	if authority == nil || authority.frozenRuntime == nil {
		return errors.New("external AgentRuntime rotated-endpoint cleanup authority is incomplete")
	}
	reader := d.APIReader
	if reader == nil {
		reader = d.Client
	}
	current := &corev1alpha1.AgentRuntime{}
	if err := reader.Get(ctx, authority.runtimeKey, current); err != nil {
		return markExternalRuntimeMutationReadRetryable(fmt.Errorf("re-read external AgentRuntime before rotated-endpoint cleanup mutation: %w", err))
	}
	if current.Namespace != authority.runtimeKey.Namespace || current.Name != authority.runtimeKey.Name ||
		current.UID == "" || current.UID != authority.runtimeUID {
		return errors.New("external AgentRuntime identity changed before rotated-endpoint cleanup mutation")
	}
	runtimeEpoch, err := d.externalRuntimeCleanupEpoch(ctx, authority.sessionCleanup)
	if err != nil {
		return err
	}
	reconciler := &AgentRuntimeReconciler{Client: d.Client, APIReader: d.APIReader}
	currentPins, err := reconciler.AgentRuntimeServiceBackendPins(ctx, authority.frozenRuntime)
	if err != nil {
		return markExternalRuntimeMutationReadRetryable(err)
	}
	if !slices.Equal(currentPins, authority.serviceBackendPins) {
		return errors.New("external AgentRuntime frozen cleanup backend set changed before mutation")
	}
	currentAuth, err := reconciler.agentRuntimeAuthMaterial(ctx, authority.frozenRuntime)
	if err != nil {
		return markExternalRuntimeMutationReadRetryable(err)
	}
	if currentAuth.controllerSecretUID != authority.auth.controllerSecretUID ||
		currentAuth.capabilitySecretUID != authority.auth.capabilitySecretUID ||
		currentAuth.controllerResourceVersion != authority.auth.controllerResourceVersion ||
		currentAuth.capabilityResourceVersion != authority.auth.capabilityResourceVersion ||
		currentAuth.controllerBearerToken != authority.auth.controllerBearerToken ||
		!bytes.Equal(currentAuth.operationCapabilitySecret, authority.auth.operationCapabilitySecret) {
		return errors.New("external AgentRuntime frozen cleanup authentication changed before mutation")
	}
	probe, err := d.newExternalRuntimeCleanupHTTPClient(authority, authority.runtimeProfileDigest, false)
	if err != nil {
		return err
	}
	status, err := probe.Status(ctx)
	if err != nil {
		return markExternalRuntimeMutationReadRetryable(err)
	}
	if err := validateExternalRuntimeRotatedEndpointCleanupStatus(harnessv2.Fence{
		RuntimeInstanceID: authority.runtimeInstanceID, SupervisorBootID: authority.supervisorBootID,
		ControllerEpoch: runtimeEpoch, RuntimePoolUID: authority.runtimePoolUID,
		RuntimePoolGeneration: authority.runtimePoolGeneration, RuntimeProfileDigest: authority.runtimeProfileDigest,
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}, status); err != nil {
		return err
	}
	if authority.sessionCleanup != nil {
		_, err = d.externalRuntimeCleanupEpoch(ctx, authority.sessionCleanup)
	}
	return err
}

func validateExternalRuntimeRotatedEndpointCleanupStatus(
	expected harnessv2.Fence,
	status *harnessv2.StatusResponse,
) error {
	if status == nil ||
		status.Fence.RuntimeInstanceID != expected.RuntimeInstanceID ||
		status.Fence.SupervisorBootID != expected.SupervisorBootID ||
		status.Fence.ControllerEpoch != expected.ControllerEpoch ||
		status.Fence.RuntimePoolUID != expected.RuntimePoolUID ||
		status.Fence.RuntimePoolGeneration != expected.RuntimePoolGeneration ||
		status.Fence.RuntimeProfileDigest != expected.RuntimeProfileDigest ||
		status.Fence.ProfileDigestSchemaVersion != harnessv2.ProfileDigestSchemaVersion {
		return errors.New("external AgentRuntime authenticated rotated-endpoint cleanup status fence changed")
	}
	return nil
}

func (d *ACPDispatcher) reconcileRecoveredTaskScopedRuntimeSession(
	ctx context.Context,
	task *corev1alpha1.Task,
	taskUID types.UID,
	deleteAfterSettlement bool,
) (bool, error) {
	if task != nil && task.Spec.SessionRef != nil &&
		(task.Spec.Workspace == nil || task.Spec.Workspace.Intent != corev1alpha1.WorkspaceIntentWrite) {
		return true, nil
	}
	return d.reconcileRecoveredRuntimeSession(ctx, task, taskUID, deleteAfterSettlement, nil)
}

//nolint:gocyclo // Recovery keeps exact runtime-state and fence cleanup decisions in one fail-closed boundary.
func (d *ACPDispatcher) reconcileRecoveredRuntimeSession(
	ctx context.Context,
	task *corev1alpha1.Task,
	taskUID types.UID,
	deleteAfterSettlement bool,
	sessionCleanup *sessionRuntimeCleanupFence,
) (bool, error) {
	sessionDeletion := sessionCleanup != nil
	if runtimeSessionCleanupCompleteForUID(task, taskUID) {
		return true, nil
	}
	if task.Status.Execution.RuntimeSessionGeneration < 1 {
		return false, fmt.Errorf("%w: RuntimeSession cleanup generation is missing", store.ErrConflict)
	}
	execution := task.Status.Execution
	var target acpDispatchTarget
	var mcpConfiguration harnessv2.MCPPolicyConfiguration
	var runtimeClient *harnessv2.Client
	var runtimeFence harnessv2.Fence
	var externalEndpointRotated bool
	if poolName := strings.TrimSpace(execution.RuntimePoolName); poolName != "" {
		pool := &corev1alpha1.RuntimePool{}
		if err := d.APIReader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: poolName}, pool); err != nil {
			if apierrors.IsNotFound(err) {
				if sessionDeletion {
					return false, fmt.Errorf("%w: Session runtime cleanup cannot prove the missing RuntimePool was retired", store.ErrConflict)
				}
				if !deleteAfterSettlement {
					return true, nil
				}
				if markErr := d.markTaskScopedRuntimeSessionCleanupComplete(ctx, task, taskUID, execution.RuntimeInstanceID, execution.RuntimeSessionUID, execution.RuntimeSessionGeneration); markErr != nil {
					return false, markErr
				}
				return true, nil
			}
			return false, err
		}
		active := pool.Status.ActiveInstance
		currentFence, err := d.Epochs.CurrentFence(ctx)
		if err != nil {
			return false, err
		}
		if active == nil || active.RuntimeInstanceID != execution.RuntimeInstanceID {
			if sessionDeletion {
				return false, fmt.Errorf("%w: Session runtime cleanup requires the frozen RuntimePool instance", store.ErrConflict)
			}
			if !deleteAfterSettlement {
				return true, nil
			}
			if markErr := d.markTaskScopedRuntimeSessionCleanupComplete(ctx, task, taskUID, execution.RuntimeInstanceID, execution.RuntimeSessionUID, execution.RuntimeSessionGeneration); markErr != nil {
				return false, markErr
			}
			return true, nil
		}
		if active.ControllerEpoch != currentFence.Epoch {
			return false, nil
		}
		if sessionDeletion && (string(pool.UID) != execution.RuntimePoolUID ||
			active.BootID != execution.RuntimeSessionSupervisorBootID ||
			pool.Spec.Runtime.Profile.Digest != execution.RuntimeSessionProfileDigest ||
			active.ProfileDigest != execution.RuntimeSessionProfileDigest) {
			return false, fmt.Errorf("%w: Session RuntimePool cleanup authority changed", store.ErrConflict)
		}
		target.pool = pool
	} else if runtimeName := strings.TrimSpace(execution.AgentRuntimeName); runtimeName != "" {
		runtime := &corev1alpha1.AgentRuntime{}
		if err := d.APIReader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: runtimeName}, runtime); err != nil {
			if apierrors.IsNotFound(err) && taskHasAgentRuntimeDrainCleanupProofForUID(task, taskUID) {
				return true, nil
			}
			return false, fmt.Errorf("load external AgentRuntime for RuntimeSession cleanup: %w", err)
		}
		if string(runtime.UID) != execution.AgentRuntimeUID {
			if sessionDeletion {
				return false, fmt.Errorf("%w: Session AgentRuntime cleanup identity changed", store.ErrConflict)
			}
			if !deleteAfterSettlement {
				return true, nil
			}
			if markErr := d.markTaskScopedRuntimeSessionCleanupComplete(ctx, task, taskUID, execution.RuntimeInstanceID, execution.RuntimeSessionUID, execution.RuntimeSessionGeneration); markErr != nil {
				return false, markErr
			}
			return true, nil
		}
		frozenRuntime, frozenProfile, frozenMCPConfiguration, err := d.verifiedExternalRuntimeRecoveryTarget(ctx, task, taskUID, runtime)
		if err != nil {
			return false, err
		}
		mcpConfiguration = frozenMCPConfiguration
		endpointRotated := strings.TrimSpace(runtime.Spec.Deployment.Endpoint) != strings.TrimSpace(frozenRuntime.Endpoint)
		externalEndpointRotated = endpointRotated
		observed := runtime.Status.ObservedCapabilities
		if !endpointRotated && !agentRuntimeObservedStatusIdentityComplete(observed) {
			// The same runtime may still own the session. Wait for a complete
			// authenticated status identity before deciding it was replaced.
			return false, nil
		}
		if !endpointRotated && (observed == nil ||
			observed.RuntimeInstanceID != execution.RuntimeInstanceID ||
			observed.SupervisorBootID != execution.RuntimeSessionSupervisorBootID) {
			if !deleteAfterSettlement {
				return true, nil
			}
			// A different boot on the same AgentRuntime object cannot prove that
			// the frozen boot deleted its RuntimeSession. Wait for exact cleanup
			// proof instead of minting a generic receipt from identity drift.
			return false, nil
		}
		currentFence, err := d.Epochs.CurrentFence(ctx)
		if err != nil {
			return false, err
		}
		expectedRuntimeEpoch := uint64(currentFence.Epoch)
		if sessionCleanup != nil {
			expectedRuntimeEpoch, err = d.externalRuntimeCleanupEpoch(ctx, sessionCleanup)
			if err != nil {
				return false, err
			}
		}
		if !endpointRotated && uint64(observed.ControllerEpoch) != expectedRuntimeEpoch {
			return false, nil
		}
		if frozenRuntime.RuntimeInstanceID != execution.RuntimeInstanceID ||
			strings.TrimSpace(execution.RuntimeSessionSupervisorBootID) == "" {
			return false, errors.New("external RuntimeSession recovery instance or boot identity does not match the immutable snapshot")
		}
		profileDigest, err := harnessv2.CanonicalProfileDigest(frozenProfile)
		if err != nil {
			return false, fmt.Errorf("canonicalize frozen external RuntimeSession recovery profile: %w", err)
		}
		if endpointRotated {
			runtimeClient, runtimeFence, err = d.externalRuntimeRotatedEndpointCleanupClient(
				ctx, runtime, frozenRuntime, profileDigest, frozenRuntime.Limits,
				harnessv2.RuntimeInstanceID(execution.RuntimeInstanceID),
				harnessv2.SupervisorBootID(execution.RuntimeSessionSupervisorBootID), sessionCleanup,
			)
		} else {
			runtimeClient, runtimeFence, err = d.externalRuntimeCleanupClient(
				ctx, runtime, frozenRuntime, profileDigest, frozenRuntime.Limits,
				harnessv2.RuntimeInstanceID(execution.RuntimeInstanceID),
				harnessv2.SupervisorBootID(execution.RuntimeSessionSupervisorBootID), sessionCleanup,
			)
		}
		if err != nil {
			return false, err
		}
	} else {
		if sessionDeletion {
			return false, fmt.Errorf("%w: Session runtime cleanup target is missing", store.ErrConflict)
		}
		if !deleteAfterSettlement {
			return true, nil
		}
		if markErr := d.markTaskScopedRuntimeSessionCleanupComplete(ctx, task, taskUID, execution.RuntimeInstanceID, execution.RuntimeSessionUID, execution.RuntimeSessionGeneration); markErr != nil {
			return false, markErr
		}
		return true, nil
	}
	if runtimeClient == nil {
		var err error
		runtimeClient, runtimeFence, _, _, err = d.runtimeClient(ctx, target, mcpConfiguration, false)
		if err != nil {
			return false, err
		}
	}
	runtimeFence.RuntimeSessionUID = harnessv2.RuntimeSessionUID(execution.RuntimeSessionUID)
	runtimeFence.RuntimeSessionGeneration = uint64(execution.RuntimeSessionGeneration)
	status, statusErr := runtimeClient.Status(ctx)
	if statusErr != nil {
		return false, statusErr
	}
	if sessionDeletion {
		if err := validateSessionRuntimeCleanupStatus(runtimeFence, status); err != nil {
			return false, err
		}
	}
	if externalEndpointRotated {
		if err := validateExternalRuntimeRotatedEndpointCleanupStatus(runtimeFence, status); err != nil {
			return false, err
		}
	}
	observed, present := runtimeSessionStatusForFence(status.Sessions, runtimeFence)
	if !present {
		if !deleteAfterSettlement || sessionDeletion {
			return true, nil
		}
		if markErr := d.markTaskScopedRuntimeSessionCleanupComplete(
			ctx, task, taskUID, execution.RuntimeInstanceID, execution.RuntimeSessionUID, execution.RuntimeSessionGeneration,
		); markErr != nil {
			return false, markErr
		}
		return true, nil
	}
	switch observed.State {
	case harnessv2.RuntimeSessionStatePublicationPrepared:
		if task.Spec.Workspace == nil || task.Spec.Workspace.Intent != corev1alpha1.WorkspaceIntentWrite {
			return false, fmt.Errorf("recover RuntimeSession publication finalization: prepared session is not bound to a write workspace")
		}
		deltaID := harnessv2.WorkspaceDeltaID("delta-" + execution.PromptID)
		finalization, finalizationErr := d.runtimeSessionPublicationFinalization(ctx, publicationIDForTaskUID(task, taskUID), deltaID)
		if errors.Is(finalizationErr, store.ErrNotFound) && task.Status.Delivery != nil &&
			task.Status.Delivery.Outcome != corev1alpha1.TaskDeliveryOutcomeNoChange {
			finalization, finalizationErr = runtimeSessionDeltaAbandonmentFinalizationForTaskUID(task, taskUID, deltaID, *task.Status.Delivery)
		}
		if finalizationErr != nil {
			return false, fmt.Errorf("recover task-scoped RuntimeSession publication finalization: %w", finalizationErr)
		}
		if err := d.finalizeRuntimeSessionPublicationForTaskUID(
			context.WithoutCancel(ctx), runtimeClient, harnessv2.RuntimeSessionID(runtimeSessionID(runtimeFence)), task, taskUID, runtimeFence, finalization,
		); err != nil {
			return false, fmt.Errorf("recover task-scoped RuntimeSession publication finalization: %w", err)
		}
	case harnessv2.RuntimeSessionStateFinalizing, harnessv2.RuntimeSessionStateIdle, harnessv2.RuntimeSessionStatePoisoned:
		// Ready for exact deletion.
	case harnessv2.RuntimeSessionStateDeleting:
		return false, nil
	default:
		return false, nil
	}
	if !deleteAfterSettlement {
		return true, nil
	}
	reason := "terminal_recovery"
	if sessionDeletion {
		reason = "session_deleted"
	}
	if err := d.deleteRuntimeSessionForTaskUID(
		context.WithoutCancel(ctx), runtimeClient, harnessv2.RuntimeSessionID(runtimeSessionID(runtimeFence)), task, taskUID, runtimeFence, reason,
	); err != nil {
		return false, fmt.Errorf("recover task-scoped RuntimeSession cleanup: %w", err)
	}
	if sessionDeletion {
		status, err := runtimeClient.Status(ctx)
		if err != nil {
			return false, err
		}
		if err := validateSessionRuntimeCleanupStatus(runtimeFence, status); err != nil {
			return false, err
		}
		if _, present := runtimeSessionStatusForFence(status.Sessions, runtimeFence); present {
			return false, nil
		}
		return true, nil
	}
	if err := d.markTaskScopedRuntimeSessionCleanupComplete(
		ctx, task, taskUID, execution.RuntimeInstanceID, execution.RuntimeSessionUID, execution.RuntimeSessionGeneration,
	); err != nil {
		return false, err
	}
	return true, nil
}

func runtimeSessionStatusForFence(sessions []harnessv2.RuntimeSessionStatus, fence harnessv2.Fence) (harnessv2.RuntimeSessionStatus, bool) {
	for i := range sessions {
		if sessions[i].RuntimeSessionUID == fence.RuntimeSessionUID && sessions[i].Generation == fence.RuntimeSessionGeneration {
			return sessions[i], true
		}
	}
	return harnessv2.RuntimeSessionStatus{}, false
}

func (d *ACPDispatcher) recoverMissingStandaloneTerminalProjection(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
) error {
	if task == nil || attempt == nil || task.Status.Execution == nil {
		return fmt.Errorf("%w: standalone terminal recovery identity is incomplete", store.ErrConflict)
	}
	state := corev1alpha1.TaskExecutionState(attempt.ExecutionState)
	outcome := corev1alpha1.TaskExecutionOutcome(attempt.ExecutionState)
	reason, message, err := recoveredTerminalExecutionReasonMessage(attempt, task.Status.Execution)
	if err != nil {
		return err
	}
	recoveryTask := task.DeepCopy()
	if recoveryTask.Status.Delivery == nil || store.PromptDeliveryState(recoveryTask.Status.Delivery.State) != attempt.DeliveryState {
		recoveryTask.Status.Delivery = deliveryStatusFromPromptState(attempt.DeliveryState)
	}
	bound, err := promptAttemptSessionBound(attempt)
	if err != nil {
		return err
	}
	if task.Spec.SessionRef != nil && !bound {
		return d.failTaskBeforeSessionBinding(ctx, recoveryTask, state, outcome, reason, message)
	}
	return d.failTask(ctx, recoveryTask, state, outcome, reason, message)
}

func recoveredTerminalExecutionReasonMessage(
	attempt *store.PromptAttempt,
	projected *corev1alpha1.TaskExecutionStatus,
) (corev1alpha1.TaskExecutionReason, string, error) {
	if attempt == nil {
		return "", "", fmt.Errorf("%w: terminal recovery requires a PromptAttempt", store.ErrConflict)
	}
	var state corev1alpha1.TaskExecutionState
	var outcome corev1alpha1.TaskExecutionOutcome
	var defaultReason corev1alpha1.TaskExecutionReason
	var defaultMessage string
	switch attempt.ExecutionState {
	case store.PromptExecutionFailed:
		state = corev1alpha1.TaskExecutionStateFailed
		outcome = corev1alpha1.TaskExecutionOutcomeFailed
		defaultReason = "PromptFailed"
		defaultMessage = "prompt failed"
	case store.PromptExecutionCancelled:
		state = corev1alpha1.TaskExecutionStateCancelled
		outcome = corev1alpha1.TaskExecutionOutcomeCancelled
		defaultReason = "Cancelled"
		defaultMessage = "prompt cancelled"
	case store.PromptExecutionOutcomeUnknown:
		state = corev1alpha1.TaskExecutionStateOutcomeUnknown
		outcome = corev1alpha1.TaskExecutionOutcomeOutcomeUnknown
		defaultReason = "RuntimeLost"
		defaultMessage = "prompt outcome is unknown"
	default:
		return "", "", fmt.Errorf("unsupported terminal recovery state %s", attempt.ExecutionState)
	}
	reason := corev1alpha1.TaskExecutionReason(attempt.TerminalReason)
	message := attempt.OutcomeMarker
	if attempt.ExecutionState == store.PromptExecutionFailed && attempt.TerminalReason == acpCredentialBlockedOperation {
		reason = acpCredentialBlockedExecutionReason
		if message == "" {
			message = acpCredentialBlockedMessage
		}
	}
	if projected != nil && projected.State == state && projected.Outcome == outcome &&
		projected.Reason != corev1alpha1.TaskExecutionReason(acpControllerRestartRecoveredReason) {
		if reason == "" {
			reason = projected.Reason
		}
		if message == "" {
			message = projected.Message
		}
	}
	if reason == "" {
		reason = defaultReason
	}
	if message == "" {
		message = defaultMessage
	}
	return reason, message, nil
}

func (d *ACPDispatcher) validateExistingStandaloneTaskProjection(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
) (bool, error) {
	if task == nil || attempt == nil {
		return false, nil
	}
	bound, err := promptAttemptSessionBound(attempt)
	if err != nil {
		return false, err
	}
	if task.Spec.SessionRef != nil && bound {
		return false, nil
	}
	projectionID := standaloneTaskTerminalProjectionID(task, int32(attempt.Key.Attempt))
	projection, err := d.Store.GetOutboxProjection(ctx, projectionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if projection.AggregateKind != taskResourceKind || projection.AggregateID != string(task.UID) || projection.ProjectionKind != taskTerminalProjectionKind {
		return false, fmt.Errorf("%w: standalone terminal projection %q has mismatched identity", store.ErrConflict, projectionID)
	}
	var payload taskTerminalProjection
	if err := json.Unmarshal(projection.Payload, &payload); err != nil {
		return false, fmt.Errorf("decode standalone terminal projection %q: %w", projectionID, err)
	}
	if payload.Namespace != task.Namespace || payload.Task != task.Name || payload.TaskUID != string(task.UID) || int64(payload.Attempt) != attempt.Key.Attempt {
		return false, fmt.Errorf("%w: standalone terminal projection %q payload identity does not match its Task", store.ErrConflict, projectionID)
	}
	if string(payload.Execution.State) != string(attempt.ExecutionState) {
		return false, fmt.Errorf("%w: standalone terminal projection %q execution state does not match its PromptAttempt", store.ErrConflict, projectionID)
	}
	projectedDelivery := store.PromptDeliveryNotRequested
	if payload.Delivery != nil {
		projectedDelivery = store.PromptDeliveryState(payload.Delivery.State)
	}
	if projectedDelivery != attempt.DeliveryState {
		return false, fmt.Errorf("%w: standalone terminal projection %q delivery state does not match its PromptAttempt", store.ErrConflict, projectionID)
	}
	return true, nil
}

func (d *ACPDispatcher) patchRecoveredTerminalEpoch(ctx context.Context, task *corev1alpha1.Task, epoch int64) error {
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if latest.Status.Execution == nil || latest.Status.Execution.ControllerEpoch >= epoch {
			return nil
		}
		latest.Status.Execution.ControllerEpoch = epoch
		return d.Client.Status().Update(ctx, latest)
	})
}

func (d *ACPDispatcher) patchRecoveredTaskReserved(ctx context.Context, task *corev1alpha1.Task, epoch int64, keepQueued bool) error {
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		base := latest.DeepCopy()
		if latest.Status.Execution == nil {
			return nil
		}
		if keepQueued {
			latest.Status.Execution.State = corev1alpha1.TaskExecutionStateQueued
		} else {
			latest.Status.Execution.State = corev1alpha1.TaskExecutionStateReserved
		}
		latest.Status.Execution.ControllerEpoch = epoch
		latest.Status.Execution.RuntimeInstanceID = ""
		latest.Status.Execution.RuntimeSessionUID = ""
		latest.Status.Execution.RuntimeSessionGeneration = 0
		latest.Status.Execution.Reason = acpControllerRestartRecoveredReason
		latest.Status.Execution.Message = acpControllerRestartRecoveredMessage
		latest.Status.Execution.LastTransitionTime = nowMeta()
		return d.Client.Status().Patch(ctx, latest, client.MergeFrom(base))
	})
}

func (d *ACPDispatcher) finalizeRecoveredSessionUnknown(ctx context.Context, task *corev1alpha1.Task, fence store.ControllerEpochFence, attemptID, reason string) error {
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	session, err := d.recoveredTaskSession(ctx, task, attempt)
	if err != nil || session == nil {
		return err
	}
	return d.finalizeTaskSessionUnknown(ctx, task, fence, session, reason+": controller takeover classified "+attemptID+" as OutcomeUnknown")
}

func (d *ACPDispatcher) recoveredTaskSession(ctx context.Context, task *corev1alpha1.Task, attempt *store.PromptAttempt) (*acpTaskSession, error) {
	if task.Spec.SessionRef == nil || d.Sessions == nil {
		return nil, nil
	}
	if attempt == nil {
		return nil, fmt.Errorf("%w: session-backed ACP recovery requires a PromptAttempt", store.ErrConflict)
	}
	if attempt.Key.TaskUID != string(task.UID) || attempt.Key.Attempt != int64(task.Status.Execution.Attempt) || attempt.Key.PromptID != task.Status.Execution.PromptID {
		return nil, fmt.Errorf("%w: recovered PromptAttempt does not match Task execution identity", store.ErrConflict)
	}
	if strings.TrimSpace(attempt.SessionUID) == "" || attempt.SessionLeaseGeneration < 1 {
		return nil, fmt.Errorf("%w: session-backed terminal PromptAttempt lacks its durable SessionTurn identity", store.ErrConflict)
	}
	control, err := d.Store.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name)
	if err != nil {
		return nil, err
	}
	if control.SessionUID != attempt.SessionUID {
		return nil, fmt.Errorf("%w: recovered SessionControl UID does not match PromptAttempt", store.ErrConflict)
	}
	key := store.SessionTurnKey{
		SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration,
		TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
	}
	turnID, err := key.CanonicalID()
	if err != nil {
		return nil, err
	}
	turn, err := d.Store.GetSessionTurn(ctx, turnID)
	if err != nil {
		return nil, fmt.Errorf("load recovered ACP SessionTurn: %w", err)
	}
	if turn.Key != key || turn.PromptAttemptID != attempt.ID {
		return nil, fmt.Errorf("%w: recovered SessionTurn does not match PromptAttempt identity", store.ErrConflict)
	}
	appendPolicy := acpSessionTranscriptAppendPolicyForTask(task)
	expectedTurnDigest, err := acpSessionTurnDigest(
		turn.ID, attempt.ID, attempt.RequestDigest, turn.UserPrompt,
		appendPolicy.skipTranscriptAppend, appendPolicy.skipUserPromptAppend,
	)
	if err != nil {
		return nil, err
	}
	if turn.RequestDigest != expectedTurnDigest && (appendPolicy.skipTranscriptAppend || appendPolicy.skipUserPromptAppend) {
		legacyDigest, legacyErr := acpSessionTurnDigest(
			turn.ID, attempt.ID, attempt.RequestDigest, turn.UserPrompt, false, false,
		)
		if legacyErr == nil && turn.RequestDigest == legacyDigest {
			appendPolicy = acpSessionTranscriptAppendPolicy{}
			expectedTurnDigest = legacyDigest
		}
	}
	if turn.RequestDigest != expectedTurnDigest {
		return nil, fmt.Errorf("%w: recovered SessionTurn transcript append policy does not match its durable digest", store.ErrConflict)
	}
	if turn.State == store.SessionTurnOpen {
		lease := control.Lease
		if lease == nil || lease.Generation != key.LeaseGeneration || lease.TaskUID != key.TaskUID || lease.Attempt != key.Attempt || lease.PromptID != key.PromptID {
			return nil, fmt.Errorf("%w: open recovered SessionTurn lacks its matching mutation lease", store.ErrConflict)
		}
	}
	bootstrap, userPrompt, err := d.resolveTaskSessionBootstrap(ctx, task, control)
	if err != nil {
		return nil, err
	}
	if turn.UserPrompt != userPrompt {
		return nil, fmt.Errorf("%w: recovered SessionTurn prompt does not match bounded Task input", store.ErrConflict)
	}
	return &acpTaskSession{
		Turn: &ACPSessionTurn{
			Lease: ACPSessionLease{Session: *control, Key: key}, Turn: *turn,
			SkipTranscriptAppend: appendPolicy.skipTranscriptAppend,
			SkipUserPromptAppend: appendPolicy.skipUserPromptAppend,
		},
		Binding:    recoveredRuntimeSessionBinding(task, control.SessionUID),
		Bootstrap:  bootstrap,
		UserPrompt: userPrompt,
	}, nil
}

// recoveredRuntimeSessionBinding rebuilds the RuntimeSession binding of a
// recovered Task from its durable execution status so recovery-owned
// settlement carries the same generation and digests the live path recorded.
// A Task whose status never bound a RuntimeSession yields the identity-only
// binding, which callers must not treat as a reusable live binding.
func recoveredRuntimeSessionBinding(task *corev1alpha1.Task, sessionUID string) ACPRuntimeSessionBinding {
	if binding, err := runtimeSessionBindingFromTaskStatus(task, sessionUID, "", "", ""); err == nil && binding != nil && binding.Generation > 0 {
		return *binding
	}
	return ACPRuntimeSessionBinding{SessionUID: sessionUID}
}

func (d *ACPDispatcher) finalizeRecoveredTerminalSession(ctx context.Context, task *corev1alpha1.Task, attempt *store.PromptAttempt, fence store.ControllerEpochFence) error {
	prepared, err := d.prepareRecoveredTaskScopedRuntimeSessionForSettlement(ctx, task)
	if err != nil {
		return err
	}
	if !prepared {
		return fmt.Errorf("%w: RuntimeSession publication finalization is not ready for Session settlement", store.ErrNotReady)
	}
	session, err := d.recoveredTaskSession(ctx, task, attempt)
	if err != nil || session == nil {
		return err
	}
	if session.Turn.Turn.State == store.SessionTurnFinalized {
		_, err := d.Sessions.ResumeSessionTurnFinalization(ctx, ACPResumeSessionTurnFinalizationRequest{SessionTurn: *session.Turn, Fence: fence})
		if err == nil {
			session.finalized = true
			d.rememberFinalizedSessionTurn(task.UID, session.Turn.Turn.ID)
			d.retireRecoveredRuntimeSessionBinding(task, attempt, session.Binding)
		}
		return err
	}
	var finalizeErr error
	switch attempt.ExecutionState {
	case store.PromptExecutionOutcomeUnknown:
		finalizeErr = d.finalizeTaskSessionUnknown(ctx, task, fence, session, "controller restart recovered terminal OutcomeUnknown")
	case store.PromptExecutionCancelled:
		reason, message, reasonErr := recoveredTerminalExecutionReasonMessage(attempt, task.Status.Execution)
		if reasonErr != nil {
			return reasonErr
		}
		execution := corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateCancelled, Outcome: corev1alpha1.TaskExecutionOutcomeCancelled,
			Attempt: task.Status.Execution.Attempt, PromptID: task.Status.Execution.PromptID, Reason: reason, Message: message,
		}
		finalizeErr = d.finalizeTaskSessionMarker(ctx, task, fence, session, "Cancelled", "controller restart recovered terminal cancellation", corev1alpha1.TaskPhaseCancelled, execution)
	case store.PromptExecutionFailed:
		reason, message, reasonErr := recoveredTerminalExecutionReasonMessage(attempt, task.Status.Execution)
		if reasonErr != nil {
			return reasonErr
		}
		execution := corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateFailed, Outcome: corev1alpha1.TaskExecutionOutcomeFailed,
			Attempt: task.Status.Execution.Attempt, PromptID: task.Status.Execution.PromptID, Reason: reason, Message: message,
		}
		finalizeErr = d.finalizeTaskSessionMarker(ctx, task, fence, session, "Failed", "controller restart recovered terminal failure", corev1alpha1.TaskPhaseFailed, execution)
	case store.PromptExecutionSucceeded:
		delivery := task.Status.Delivery
		if delivery == nil {
			delivery = deliveryStatusFromPromptState(attempt.DeliveryState)
		}
		phase := corev1alpha1.TaskPhaseFailed
		switch attempt.DeliveryState {
		case store.PromptDeliveryNotRequested, store.PromptDeliveryReadValidated, store.PromptDeliveryNoChange,
			store.PromptDeliveryVerifiedExact, store.PromptDeliveryDeliveredSuperseded:
			phase = corev1alpha1.TaskPhaseSucceeded
		case store.PromptDeliveryCancelledBeforePublish:
			phase = corev1alpha1.TaskPhaseCancelled
		}
		result, resultErr := d.ResultStore.GetResult(ctx, task.Namespace, task.Name)
		if resultErr != nil {
			return resultErr
		}
		publicationID := ""
		if attempt.DeliveryState == store.PromptDeliveryVerifiedExact || attempt.DeliveryState == store.PromptDeliveryDeliveredSuperseded ||
			attempt.DeliveryState == store.PromptDeliveryConflict || attempt.DeliveryState == store.PromptDeliveryPublicationOutcomeUnknown ||
			attempt.DeliveryState == store.PromptDeliveryCancelledBeforePublish {
			candidate := publicationIDForTask(task)
			if _, publicationErr := d.Store.GetPublication(ctx, candidate); publicationErr == nil {
				publicationID = candidate
			} else if !errors.Is(publicationErr, store.ErrNotFound) {
				return publicationErr
			}
		}
		finalizeErr = d.finalizeTaskSessionResult(ctx, task, fence, session, string(result), publicationID, phase, *delivery)
	}
	if finalizeErr == nil {
		session.finalized = true
		d.rememberFinalizedSessionTurn(task.UID, session.Turn.Turn.ID)
		d.retireRecoveredRuntimeSessionBinding(task, attempt, session.Binding)
	}
	return finalizeErr
}

func (d *ACPDispatcher) recoverSucceededTaskProjection(ctx context.Context, task *corev1alpha1.Task, attempt *store.PromptAttempt, fence store.ControllerEpochFence) error {
	if _, err := d.ResultStore.GetResult(ctx, task.Namespace, task.Name); err != nil {
		return err
	}
	if err := d.publishTaskResultReference(ctx, task); err != nil {
		return err
	}
	if !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
		switch attempt.DeliveryState {
		case store.PromptDeliveryValidating:
			if err := d.transitionDelivery(ctx, attempt.ID, fence, store.PromptDeliveryValidating, store.PromptDeliveryConflict,
				"controller-restart-workspace-lost", "runtime workspace was lost before durable validation completed"); err != nil {
				return err
			}
			updatedAttempt, err := d.Store.GetPromptAttempt(ctx, attempt.ID)
			if err != nil {
				return err
			}
			attempt = updatedAttempt
			if err := d.finalizeRecoveredTerminalSession(ctx, task, attempt, fence); err != nil {
				return err
			}
			status := corev1alpha1.TaskDeliveryStatus{
				State: corev1alpha1.TaskDeliveryStateDeliveryConflict, Outcome: corev1alpha1.TaskDeliveryOutcomeDeliveryConflict,
				Reason: "RuntimeWorkspaceLost", Message: "runtime workspace was lost before durable validation completed", LastTransitionTime: nowMeta(),
			}
			return d.failTaskForDelivery(ctx, task, status, status.Message)
		case store.PromptDeliveryPreparing, store.PromptDeliveryPrepared, store.PromptDeliveryPublishing, store.PromptDeliveryVerifying:
			result, err := d.reconcilePersistedPublication(ctx, task, attempt.ID, fence)
			if err != nil {
				return err
			}
			if err := d.patchDeliveryStatus(ctx, task, result.Status); err != nil {
				return err
			}
			attempt, err = d.Store.GetPromptAttempt(ctx, attempt.ID)
			if err != nil {
				return err
			}
			if err := d.finalizeRecoveredTerminalSession(ctx, task, attempt, fence); err != nil {
				return err
			}
			switch result.Status.Outcome {
			case corev1alpha1.TaskDeliveryOutcomeVerifiedExact, corev1alpha1.TaskDeliveryOutcomeDeliveredSuperseded:
				return d.completeSuccessWithDelivery(ctx, task, result.Status, "ACP publication settled from the durable attempt record after the live settlement was interrupted")
			case corev1alpha1.TaskDeliveryOutcomeCancelledBeforePublish:
				return d.cancelTaskAfterExecution(ctx, task, result.Status, "publication cancelled before push during recovery")
			default:
				return d.failTaskForDelivery(ctx, task, result.Status, "ACP publication recovery reached a terminal delivery failure")
			}
		default:
			return fmt.Errorf("unsupported nonterminal delivery state %s during recovery", attempt.DeliveryState)
		}
	}
	if err := d.finalizeRecoveredTerminalSession(ctx, task, attempt, fence); err != nil {
		return err
	}
	return d.patchRecoveredTerminalExecution(ctx, task, attempt, fence.Epoch)
}

func (d *ACPDispatcher) recoveredTerminalDeliveryStatus(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
) (*corev1alpha1.TaskDeliveryStatus, error) {
	switch attempt.DeliveryState {
	case store.PromptDeliveryVerifiedExact, store.PromptDeliveryDeliveredSuperseded,
		store.PromptDeliveryConflict, store.PromptDeliveryCredentialBlocked,
		store.PromptDeliveryPublicationOutcomeUnknown, store.PromptDeliveryCancelledBeforePublish:
		publication, err := d.Store.GetPublication(ctx, publicationIDForTask(task))
		if errors.Is(err, store.ErrNotFound) && task.Status.Delivery != nil && task.Status.Delivery.Outcome != "" {
			status := *task.Status.Delivery
			return &status, nil
		}
		if err != nil {
			return nil, err
		}
		artifact := harnessv2.ArtifactReference{
			ArtifactID: harnessv2.ArtifactID(publication.ArtifactID), Digest: publication.ArtifactDigest,
			SizeBytes: publication.ArtifactSizeBytes, MediaType: publication.ArtifactMediaType,
		}
		delta := harnessv2.WorkspaceDeltaDescriptor{
			State: harnessv2.WorkspaceDeltaPrepared, Intent: harnessv2.WorkspaceIntentWrite,
			VerifiedBaseline: harnessv2.WorkspaceBaseline{RepositoryIdentity: publication.SourceRepositoryID, Revision: publication.SourceBaselineSHA},
			RelativeRoot:     strings.TrimSpace(task.Spec.Workspace.SubPath), Artifact: &artifact,
			PublicationSafe: true, NoFollowVerified: true,
		}
		status := publicationTaskDeliveryStatus(task.Spec.Workspace, delta.VerifiedBaseline, delta, publication, strings.TrimPrefix(publication.TargetRef, "refs/heads/"))
		if task.Status.Delivery != nil && task.Status.Delivery.Reason == corev1alpha1.TaskDeliveryReasonCancellationRequestedAfterPublish {
			status.Reason = task.Status.Delivery.Reason
			status.Message = task.Status.Delivery.Message
		}
		return &status, nil
	default:
		if task.Status.Delivery != nil && store.PromptDeliveryState(task.Status.Delivery.State) == attempt.DeliveryState {
			return task.Status.Delivery.DeepCopy(), nil
		}
		return deliveryStatusFromPromptState(attempt.DeliveryState), nil
	}
}

func (d *ACPDispatcher) patchRecoveredTerminalExecution(ctx context.Context, task *corev1alpha1.Task, attempt *store.PromptAttempt, epoch int64) error {
	if attempt.ExecutionState == store.PromptExecutionSucceeded {
		status, err := d.recoveredTerminalDeliveryStatus(ctx, task, attempt)
		if err != nil {
			return err
		}
		switch attempt.DeliveryState {
		case store.PromptDeliveryNotRequested, store.PromptDeliveryReadValidated, store.PromptDeliveryNoChange,
			store.PromptDeliveryVerifiedExact, store.PromptDeliveryDeliveredSuperseded:
			return d.completeSuccessWithDelivery(ctx, task, *status, "ACP task settled from the durable attempt record after the live settlement was interrupted")
		default:
			// The authoritative attempt settles the Task; it may reach this
			// path after a controller restart or when the live settlement
			// was interrupted, so the message must not claim a restart and
			// should carry the delivery failure the user needs to act on.
			message := "ACP delivery failed"
			if detail := strings.TrimSpace(status.Message); detail != "" {
				message += ": " + detail
			}
			return d.failTaskForDelivery(ctx, task, *status, message)
		}
	}
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		base := latest.DeepCopy()
		if latest.Status.Execution == nil {
			return nil
		}
		reason, message, err := recoveredTerminalExecutionReasonMessage(attempt, latest.Status.Execution)
		if err != nil {
			return err
		}
		latest.Status.Execution.ControllerEpoch = epoch
		latest.Status.Execution.Reason = reason
		latest.Status.Execution.Message = message
		latest.Status.Execution.LastTransitionTime = nowMeta()
		return d.Client.Status().Patch(ctx, latest, client.MergeFrom(base))
	})
}

func deliveryStatusFromPromptState(state store.PromptDeliveryState) *corev1alpha1.TaskDeliveryStatus {
	now := metav1.Now()
	status := &corev1alpha1.TaskDeliveryStatus{State: corev1alpha1.TaskDeliveryState(state), Outcome: corev1alpha1.TaskDeliveryOutcome(state), LastTransitionTime: &now}
	return status
}
