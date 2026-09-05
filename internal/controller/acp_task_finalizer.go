package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/artifactcap"
	"github.com/orka-agents/orka/internal/publisher"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/store"
)

const conditionTypeACPArtifactsRetired = "ACPArtifactsRetired"

// ACPPublicationReclaimer removes Publisher-local cache state after every
// durable publication/session barrier for a deleting Task has settled.
type ACPPublicationReclaimer interface {
	ReclaimPublication(context.Context, publisherservice.PublicationReclaimRequest) (publisherservice.PublicationReclaimResponse, error)
}

type acpPublicationReclaimTarget struct {
	id         string
	generation int64
}

//nolint:gocyclo // Deletion readiness keeps every durable attempt, publication, effect, and projection barrier in one fail-closed boundary.
func (r *TaskReconciler) acpTaskDeletionReady(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	if task == nil || task.Spec.Type != corev1alpha1.TaskTypeAgent || r.DurableControlStore == nil {
		return true, nil
	}
	if acpTaskHasUnvalidatedSourceIdentity(task) {
		return false, fmt.Errorf("%w: Task execution binding UID differs from the live Task before validated restore settlement", store.ErrConflict)
	}
	if acpTaskRequiresAuthoritativeAttemptDiscovery(task) {
		if task.Status.Execution != nil && task.Status.Execution.Attempt > 0 && task.Status.Execution.PromptID != "" {
			projectionID := standaloneTaskTerminalProjectionIDForUID(task.Namespace, acpTaskControlUID(task), task.Status.Execution.Attempt)
			projection, err := r.DurableControlStore.GetOutboxProjection(ctx, projectionID)
			if err == nil {
				if projection.State != store.OutboxProjectionDelivered {
					return false, nil
				}
				// A queued timeout settled by the dispatcher already has a durable
				// terminal projection, so keep the normal projected barriers below.
			} else if !errors.Is(err, store.ErrNotFound) {
				return false, err
			} else {
				publicationID := publicationIDForTaskUID(task, acpTaskControlUID(task))
				unsettled, effectErr := r.acpTaskHasUnsettledExternalEffects(ctx, task, publicationID)
				return !unsettled, effectErr
			}
		} else {
			// A PromptAttempt is persisted before Task.status.execution. Continue to
			// the authoritative reclamation preflight so it can either settle that
			// unbound attempt or durably prove that no attempt ever existed.
			publicationID := ""
			if task.Status.Execution != nil {
				publicationID = publicationIDForTaskUID(task, acpTaskControlUID(task))
			}
			unsettled, err := r.acpTaskHasUnsettledExternalEffects(ctx, task, publicationID)
			return !unsettled, err
		}
	}
	if task.Status.Execution.RuntimeSessionUID != "" && !taskScopedRuntimeSessionCleanupComplete(task) {
		return false, nil
	}
	if acpTaskTerminalBeforeDurableAttempt(task) {
		publicationID := publicationIDForTaskUID(task, acpTaskControlUID(task))
		unsettled, err := r.acpTaskHasUnsettledExternalEffects(ctx, task, publicationID)
		return !unsettled, err
	}
	attemptID, err := promptAttemptIDFromTaskUID(task, acpTaskControlUID(task))
	if err != nil {
		return false, err
	}
	attempt, err := r.DurableControlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// The final attempt may already have been removed by a prepared
			// reclamation. Let the store validate its durable marker on retry.
			return r.acpTaskSessionDeletionReady(ctx, task, nil)
		}
		return false, err
	}
	if acpTaskUsesRestoredSourceIdentity(task) {
		if err := validateRestoredTaskSourceAttempt(task, attempt, attemptID); err != nil {
			return false, err
		}
	}
	if !store.IsTerminalPromptExecutionState(attempt.ExecutionState) || !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
		return false, nil
	}
	if ready, err := r.acpTaskSessionDeletionReady(ctx, task, attempt); err != nil || !ready {
		return ready, err
	}
	publicationID := publicationIDForTaskUID(task, acpTaskControlUID(task))
	if task.Spec.Workspace != nil && task.Spec.Workspace.Intent == corev1alpha1.WorkspaceIntentWrite {
		publication, publicationErr := r.DurableControlStore.GetPublication(ctx, publicationID)
		if publicationErr != nil {
			if !errors.Is(publicationErr, store.ErrNotFound) {
				return false, publicationErr
			}
		} else if !store.IsTerminalPublicationState(publication.State) {
			return false, nil
		}
	}
	if unsettled, err := r.acpTaskHasUnsettledExternalEffects(ctx, task, publicationID); err != nil {
		return false, err
	} else if unsettled {
		return false, nil
	}
	projectionID, err := r.acpTaskTerminalProjectionID(ctx, task, attempt)
	if err != nil {
		return false, err
	}
	projection, err := r.DurableControlStore.GetOutboxProjection(ctx, projectionID)
	if errors.Is(err, store.ErrNotFound) {
		receipt, receiptErr := r.acpTaskSessionCleanupReceipt(ctx, task, attempt)
		if receiptErr == nil {
			if receipt.ProjectionID != projectionID {
				return false, fmt.Errorf("%w: Session cleanup receipt does not match Task terminal projection", store.ErrConflict)
			}
			projection, err = receipt.OutboxProjection(), nil
		} else {
			err = receiptErr
		}
	}
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return projection.State == store.OutboxProjectionDelivered, nil
}

func (r *TaskReconciler) acpTaskSessionDeletionReady(ctx context.Context, task *corev1alpha1.Task, attempt *store.PromptAttempt) (bool, error) {
	if task.DeletionTimestamp.IsZero() || task.Spec.SessionRef == nil {
		return true, nil
	}
	bound := task.Status.Execution.RuntimeSessionUID != "" || task.Status.Execution.RuntimeSessionGeneration != 0
	if attempt != nil {
		var err error
		bound, err = promptAttemptSessionBound(attempt)
		if err != nil {
			return false, err
		}
	}
	if !bound {
		return true, nil
	}
	// Runtime retirement precedes the Session archival transaction. Keep the
	// Task's frozen cleanup authority until the archive is durable, so a
	// crash between those steps can still resume the Session cleanup intent.
	receipt, err := r.acpTaskSessionCleanupReceipt(ctx, task, attempt)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return receipt.ProjectionState == store.OutboxProjectionDelivered, nil
}

func acpTaskTerminalBeforeDurableAttempt(task *corev1alpha1.Task) bool {
	if task == nil || task.Status.Execution == nil || task.Status.Delivery == nil {
		return false
	}
	execution := task.Status.Execution
	delivery := task.Status.Delivery
	terminalBeforeAttempt := execution.State == corev1alpha1.TaskExecutionStateFailed &&
		execution.Outcome == corev1alpha1.TaskExecutionOutcomeFailed &&
		(execution.Reason == corev1alpha1.TaskExecutionReason("InvalidWorkspace") || execution.Reason == corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile"))
	if !terminalBeforeAttempt {
		return false
	}
	if execution.Attempt != 0 || execution.PromptID != "" || execution.RuntimePoolName != "" || execution.RuntimePoolUID != "" ||
		execution.AgentRuntimeName != "" || execution.AgentRuntimeUID != "" || execution.RuntimeInstanceID != "" || execution.RuntimeSessionUID != "" ||
		execution.RequestDigest != "" {
		return false
	}
	return delivery.State == corev1alpha1.TaskDeliveryStateNotRequested && delivery.Outcome == corev1alpha1.TaskDeliveryOutcomeNotRequested
}

func acpTaskRequiresAuthoritativeAttemptDiscovery(task *corev1alpha1.Task) bool {
	if task == nil || task.Status.Execution == nil {
		return true
	}
	execution := task.Status.Execution
	if task.Status.Delivery == nil || task.Status.Delivery.State != corev1alpha1.TaskDeliveryStateNotRequested ||
		task.Status.Delivery.Outcome != corev1alpha1.TaskDeliveryOutcomeNotRequested {
		return false
	}
	return execution.State == corev1alpha1.TaskExecutionStateCancelled &&
		execution.Outcome == corev1alpha1.TaskExecutionOutcomeCancelled &&
		execution.Reason == corev1alpha1.TaskExecutionReason("TaskTimeout") &&
		execution.RuntimePoolName == "" && execution.RuntimePoolUID == "" &&
		execution.AgentRuntimeName == "" && execution.AgentRuntimeUID == "" &&
		execution.RuntimeInstanceID == "" && execution.RuntimeSessionUID == "" &&
		execution.RuntimeSessionGeneration == 0
}

func (r *TaskReconciler) acpTaskHasUnsettledExternalEffects(
	ctx context.Context,
	task *corev1alpha1.Task,
	publicationID string,
) (bool, error) {
	var effects corev1alpha1.ExternalEffectList
	if err := r.List(ctx, &effects, client.InNamespace(task.Namespace)); err != nil {
		return false, err
	}
	related := map[string]struct{}{string(acpTaskControlUID(task)): {}, publicationID: {}}
	if task.Status.Execution != nil && task.Status.Execution.RuntimeSessionUID != "" {
		related[task.Status.Execution.RuntimeSessionUID] = struct{}{}
	}
	for i := range effects.Items {
		effect := &effects.Items[i]
		if _, ok := related[effect.Spec.AggregateID]; !ok {
			continue
		}
		switch store.ExternalEffectState(effect.Status.State) {
		case store.ExternalEffectSucceeded, store.ExternalEffectFailed, store.ExternalEffectOutcomeUnknown:
		default:
			return true, nil
		}
	}
	return false, nil
}

func (r *TaskReconciler) acpTaskTerminalProjectionID(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
) (string, error) {
	if task == nil || task.Status.Execution == nil {
		return "", fmt.Errorf("task execution status is missing")
	}
	sessionUID := task.Status.Execution.RuntimeSessionUID
	leaseGeneration := task.Status.Execution.RuntimeSessionGeneration
	bound := sessionUID != "" || leaseGeneration != 0
	if attempt != nil {
		var err error
		bound, err = promptAttemptSessionBound(attempt)
		if err != nil {
			return "", err
		}
		sessionUID = attempt.SessionUID
		leaseGeneration = attempt.SessionLeaseGeneration
	}
	if task.Spec.SessionRef == nil || !bound {
		return standaloneTaskTerminalProjectionIDForUID(task.Namespace, acpTaskControlUID(task), task.Status.Execution.Attempt), nil
	}
	if attempt == nil {
		// After reclamation removes the attempt, the Task's runtime incarnation
		// generation cannot reconstruct its per-prompt mutation lease. The
		// archived receipt still binds the exact attempt to its finalized turn.
		receipt, err := r.acpTaskSessionCleanupReceipt(ctx, task, nil)
		if err == nil {
			return receipt.ProjectionID, nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return "", err
		}
	}
	if sessionUID == "" || leaseGeneration < 1 {
		return "", fmt.Errorf("session-backed ACP attempt lacks a frozen SessionTurn identity")
	}
	key := store.SessionTurnKey{
		SessionUID: sessionUID, LeaseGeneration: leaseGeneration,
		TaskUID: string(acpTaskControlUID(task)), Attempt: int64(task.Status.Execution.Attempt), PromptID: task.Status.Execution.PromptID,
	}
	turnID, err := key.CanonicalID()
	if err != nil {
		return "", err
	}
	turn, err := r.DurableControlStore.GetSessionTurn(ctx, turnID)
	if errors.Is(err, store.ErrNotFound) {
		receipt, receiptErr := r.acpTaskSessionCleanupReceipt(ctx, task, attempt)
		if receiptErr != nil {
			return "", receiptErr
		}
		turn, err = receipt.SessionTurn(), nil
	}
	if err != nil {
		return "", err
	}
	if turn.State != store.SessionTurnFinalized {
		return "", fmt.Errorf("SessionTurn %s is not finalized", turnID)
	}
	return store.CanonicalControlID("outbox", turnID, "TaskTerminalStatus"), nil
}

func (r *TaskReconciler) acpTaskSessionCleanupReceipt(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
) (*store.SessionTurnCleanupReceipt, error) {
	reader, ok := r.DurableControlStore.(store.SessionTurnCleanupReceiptStore)
	if !ok || task.Spec.SessionRef == nil || task.Status.Execution == nil {
		return nil, store.ErrNotFound
	}
	key := store.SessionTurnKey{
		SessionUID: task.Status.Execution.RuntimeSessionUID, LeaseGeneration: task.Status.Execution.RuntimeSessionGeneration,
		TaskUID: string(acpTaskControlUID(task)), Attempt: int64(task.Status.Execution.Attempt), PromptID: task.Status.Execution.PromptID,
	}
	if attempt != nil {
		key.SessionUID, key.LeaseGeneration = attempt.SessionUID, attempt.SessionLeaseGeneration
	}
	attemptID, err := promptAttemptIDFromTaskUID(task, acpTaskControlUID(task))
	if err != nil {
		return nil, err
	}
	receipt, err := reader.GetSessionTurnCleanupReceipt(ctx, task.Namespace, task.Spec.SessionRef.Name, attemptID)
	if err != nil {
		return nil, err
	}
	if receipt == nil || receipt.PromptAttemptID != attemptID {
		return nil, fmt.Errorf("%w: Session cleanup receipt does not match Task attempt", store.ErrConflict)
	}
	if err := receipt.Validate(task.Namespace, task.Spec.SessionRef.Name, receipt.TurnID); err != nil {
		return nil, err
	}
	if attempt == nil {
		key.LeaseGeneration = receipt.Key.LeaseGeneration
	}
	if receipt.Key != key || attempt != nil && receipt.PromptAttemptID != attempt.ID {
		return nil, fmt.Errorf("%w: Session cleanup receipt does not match Task attempt", store.ErrConflict)
	}
	return receipt, nil
}

func (r *TaskReconciler) reclaimACPTaskPublicationBundles(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	if task == nil || task.Spec.Type != corev1alpha1.TaskTypeAgent || task.Spec.Workspace == nil ||
		task.Spec.Workspace.Intent != corev1alpha1.WorkspaceIntentWrite || r.DurableControlStore == nil || r.ACPPublicationReclaimer == nil {
		return true, nil
	}
	targets, ready, err := r.acpPublicationReclaimTargets(ctx, task)
	if err != nil || !ready {
		return ready, err
	}
	for _, target := range targets {
		operationDigest, digestErr := acpDomainDigest("publication-reclaim-operation", map[string]any{
			"namespace": task.Namespace, "taskUID": string(acpTaskControlUID(task)),
			"publicationID": target.id, "publicationGeneration": target.generation,
		})
		if digestErr != nil {
			return false, digestErr
		}
		operationID := "publication-reclaim-" + operationDigest[len("sha256:"):len("sha256:")+32]
		request := publisherservice.PublicationReclaimRequest{
			Metadata: publisherservice.OperationMetadata{
				Namespace: task.Namespace, OperationID: operationID, PublicationID: target.id,
			},
			Request: publisherReclaimRequest(target),
		}
		response, reclaimErr := r.ACPPublicationReclaimer.ReclaimPublication(ctx, request)
		if reclaimErr != nil {
			return false, fmt.Errorf("reclaim Publisher publication cache %s generation %d: %w", target.id, target.generation, reclaimErr)
		}
		if response.OperationID != operationID || response.Result.PublicationID != target.id ||
			response.Result.PublicationGeneration != target.generation || !response.Result.Reclaimed {
			return false, fmt.Errorf("publisher returned a mismatched publication reclamation receipt")
		}
	}
	return true, nil
}

func publisherReclaimRequest(target acpPublicationReclaimTarget) publisher.ReclaimRequest {
	return publisher.ReclaimRequest{PublicationID: target.id, PublicationGeneration: target.generation}
}

func (r *TaskReconciler) acpPublicationReclaimTargets(
	ctx context.Context, task *corev1alpha1.Task,
) ([]acpPublicationReclaimTarget, bool, error) {
	identities, ready, err := r.acpArtifactRetirementIdentities(ctx, task)
	if err != nil || !ready {
		return nil, ready, err
	}

	generations := make(map[string]int64)
	var publications corev1alpha1.PublicationList
	if err := r.taskMetadataReader().List(ctx, &publications, client.InNamespace(task.Namespace)); err != nil {
		return nil, false, fmt.Errorf("list publications for Publisher cache reclamation: %w", err)
	}
	for i := range publications.Items {
		publication := &publications.Items[i]
		if publication.Spec.TaskUID != string(acpTaskControlUID(task)) {
			continue
		}
		if publication.Spec.ID == "" || publication.Spec.Generation < 1 {
			return nil, false, fmt.Errorf("publication cache reclamation identity is incomplete")
		}
		if existing, ok := generations[publication.Spec.ID]; ok && existing != publication.Spec.Generation {
			return nil, false, fmt.Errorf("publication %s has conflicting generations", publication.Spec.ID)
		}
		generations[publication.Spec.ID] = publication.Spec.Generation
	}

	targets := make([]acpPublicationReclaimTarget, 0, len(identities))
	for _, identity := range identities {
		if identity.PublicationID == "" {
			continue
		}
		generation := generations[identity.PublicationID]
		publication, publicationErr := r.DurableControlStore.GetPublication(ctx, identity.PublicationID)
		if publicationErr == nil {
			if publication.ID != identity.PublicationID || publication.Generation < 1 {
				return nil, false, fmt.Errorf("durable publication cache reclamation identity is invalid")
			}
			if generation != 0 && generation != publication.Generation {
				return nil, false, fmt.Errorf("publication %s generation differs between durable stores", identity.PublicationID)
			}
			generation = publication.Generation
		} else if !errors.Is(publicationErr, store.ErrNotFound) {
			return nil, false, publicationErr
		}
		if generation == 0 {
			// Reclamation is ordered before durable Publication removal. A missing
			// record therefore means no cache was ever created or an earlier
			// reclaim completed; the current schema's initial generation is enough
			// for the endpoint's already-absent idempotency check.
			generation = acpPublicationGeneration
		}
		targets = append(targets, acpPublicationReclaimTarget{id: identity.PublicationID, generation: generation})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].id < targets[j].id })
	return targets, true, nil
}

func (r *TaskReconciler) retireACPArtifactIdentities(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	if task == nil || task.Spec.Type != corev1alpha1.TaskTypeAgent || r.DurableControlStore == nil {
		return true, nil
	}
	if !task.DeletionTimestamp.IsZero() {
		request, err := r.acpPromptAttemptReclamationRequest(ctx, task)
		if err != nil {
			return false, err
		}
		if err := r.DurableControlStore.PreparePromptAttemptReclamation(ctx, request); err != nil {
			if errors.Is(err, store.ErrNotReady) {
				return false, nil
			}
			return false, fmt.Errorf("prepare ACP prompt attempt reclamation: %w", err)
		}
		if !meta.IsStatusConditionTrue(task.Status.Conditions, conditionTypeACPArtifactsRetired) && r.ACPArtifactRetirer != nil {
			identities, ready, identityErr := r.acpArtifactRetirementIdentities(ctx, task)
			if identityErr != nil || !ready {
				return ready, identityErr
			}
			if err := r.ACPArtifactRetirer.Retire(ctx, identities...); err != nil {
				return false, fmt.Errorf("retire ACP artifact identities: %w", err)
			}
		}
		return r.reclaimACPTaskPromptAttempts(ctx, request)
	}
	if task.Status.Execution == nil || r.ACPArtifactRetirer == nil ||
		meta.IsStatusConditionTrue(task.Status.Conditions, conditionTypeACPArtifactsRetired) {
		return true, nil
	}
	identities, ready, err := r.acpArtifactRetirementIdentities(ctx, task)
	if err != nil || !ready {
		return ready, err
	}
	if err := r.ACPArtifactRetirer.Retire(ctx, identities...); err != nil {
		return false, fmt.Errorf("retire ACP artifact identities: %w", err)
	}

	now := metav1.Now()
	if err := r.updateStatusWithRetry(ctx, task, func(current *corev1alpha1.Task) {
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type: conditionTypeACPArtifactsRetired, Status: metav1.ConditionTrue, Reason: "ArtifactReferencesReleased",
			Message: "ACP artifact objects and replay records were released after terminal control-plane settlement", LastTransitionTime: now,
		})
	}); err != nil {
		return false, fmt.Errorf("record ACP artifact retirement: %w", err)
	}
	return true, nil
}

func (r *TaskReconciler) acpPromptAttemptReclamationRequest(
	ctx context.Context,
	task *corev1alpha1.Task,
) (store.ReclaimPromptAttemptsRequest, error) {
	if task == nil {
		return store.ReclaimPromptAttemptsRequest{}, fmt.Errorf("task is required for ACP prompt attempt reclamation")
	}
	request := store.ReclaimPromptAttemptsRequest{
		Namespace: task.Namespace, TaskName: task.Name, TaskUID: string(acpTaskControlUID(task)),
		ContinuitySession: task.Spec.SessionRef != nil,
	}
	if acpTaskRequiresAuthoritativeAttemptDiscovery(task) {
		request.Mode = store.PromptAttemptReclamationUnbound
	} else {
		if task.Status.Execution.RuntimeSessionUID != "" {
			request.RelatedExternalEffectAggregateIDs = append(request.RelatedExternalEffectAggregateIDs, task.Status.Execution.RuntimeSessionUID)
		}
		request.RelatedExternalEffectAggregateIDs = append(request.RelatedExternalEffectAggregateIDs, publicationIDForTaskUID(task, acpTaskControlUID(task)))
		if acpTaskTerminalBeforeDurableAttempt(task) {
			request.Mode = store.PromptAttemptReclamationNoAttempt
			request.FinalContinuitySession = false
		} else {
			request.Mode = store.PromptAttemptReclamationProjected
			attemptID, err := promptAttemptIDFromTaskUID(task, acpTaskControlUID(task))
			if err != nil {
				return store.ReclaimPromptAttemptsRequest{}, err
			}
			request.FinalPromptAttemptID = attemptID
			attempt, err := r.DurableControlStore.GetPromptAttempt(ctx, attemptID)
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return store.ReclaimPromptAttemptsRequest{}, err
			}
			if attempt != nil {
				bound, bindingErr := promptAttemptSessionBound(attempt)
				if bindingErr != nil {
					return store.ReclaimPromptAttemptsRequest{}, bindingErr
				}
				request.FinalContinuitySession = task.Spec.SessionRef != nil && bound
			} else {
				request.FinalContinuitySession = task.Spec.SessionRef != nil &&
					task.Status.Execution.RuntimeSessionUID != "" && task.Status.Execution.RuntimeSessionGeneration > 0
			}
			if attempt == nil && task.Spec.SessionRef != nil && !request.FinalContinuitySession {
				// The prepared marker is authoritative after the final attempt has
				// been reclaimed; the Task projection alone cannot reconstruct the
				// frozen SessionTurn identity.
				request.TerminalProjectionID = ""
			} else {
				projectionID, err := r.acpTaskTerminalProjectionID(ctx, task, attempt)
				if err != nil {
					return store.ReclaimPromptAttemptsRequest{}, err
				}
				request.TerminalProjectionID = projectionID
			}
		}
	}
	if r.ControllerEpochManager == nil {
		return store.ReclaimPromptAttemptsRequest{}, fmt.Errorf("controller epoch manager is required to reclaim ACP prompt attempts")
	}
	fence, err := r.ControllerEpochManager.CurrentFence(ctx)
	if err != nil {
		return store.ReclaimPromptAttemptsRequest{}, err
	}
	request.Fence = fence
	return request, nil
}

func (r *TaskReconciler) reclaimACPTaskPromptAttempts(
	ctx context.Context,
	request store.ReclaimPromptAttemptsRequest,
) (bool, error) {
	if _, err := r.DurableControlStore.ReclaimPromptAttempts(ctx, request); err != nil {
		if errors.Is(err, store.ErrNotReady) {
			return false, nil
		}
		return false, fmt.Errorf("reclaim ACP prompt attempts: %w", err)
	}
	return true, nil
}

func (r *TaskReconciler) acpArtifactRetirementIdentities(
	ctx context.Context, task *corev1alpha1.Task,
) ([]artifactcap.Identity, bool, error) {
	reader := r.taskMetadataReader()
	var attempts corev1alpha1.PromptAttemptList
	if err := reader.List(ctx, &attempts, client.InNamespace(task.Namespace)); err != nil {
		return nil, false, fmt.Errorf("list prompt attempts for artifact retirement: %w", err)
	}
	for i := range attempts.Items {
		attempt := &attempts.Items[i]
		if attempt.Spec.TaskUID != string(acpTaskControlUID(task)) {
			continue
		}
		if !store.IsTerminalPromptExecutionState(store.PromptExecutionState(attempt.Status.ExecutionState)) ||
			!store.IsTerminalPromptDeliveryState(store.PromptDeliveryState(attempt.Status.DeliveryState)) {
			return nil, false, nil
		}
	}

	publicationIDs := map[string]struct{}{}
	var publications corev1alpha1.PublicationList
	if err := reader.List(ctx, &publications, client.InNamespace(task.Namespace)); err != nil {
		return nil, false, fmt.Errorf("list publications for artifact retirement: %w", err)
	}
	for i := range publications.Items {
		publication := &publications.Items[i]
		if publication.Spec.TaskUID != string(acpTaskControlUID(task)) {
			continue
		}
		if !store.IsTerminalPublicationState(store.PublicationState(publication.Status.State)) {
			return nil, false, nil
		}
		publicationIDs[publication.Spec.ID] = struct{}{}
	}
	if task.Status.Execution != nil && task.Spec.Workspace != nil && task.Spec.Workspace.Intent == corev1alpha1.WorkspaceIntentWrite {
		publicationIDs[publicationIDForTaskUID(task, acpTaskControlUID(task))] = struct{}{}
	}

	relatedEffects := map[string]struct{}{string(acpTaskControlUID(task)): {}}
	if task.Status.Execution != nil && task.Status.Execution.RuntimeSessionUID != "" {
		relatedEffects[task.Status.Execution.RuntimeSessionUID] = struct{}{}
	}
	for publicationID := range publicationIDs {
		relatedEffects[publicationID] = struct{}{}
	}
	var effects corev1alpha1.ExternalEffectList
	if err := reader.List(ctx, &effects, client.InNamespace(task.Namespace)); err != nil {
		return nil, false, fmt.Errorf("list external effects for artifact retirement: %w", err)
	}
	for i := range effects.Items {
		effect := &effects.Items[i]
		if _, related := relatedEffects[effect.Spec.AggregateID]; !related {
			continue
		}
		switch store.ExternalEffectState(effect.Status.State) {
		case store.ExternalEffectSucceeded, store.ExternalEffectFailed, store.ExternalEffectOutcomeUnknown:
		default:
			return nil, false, nil
		}
	}

	identities := []artifactcap.Identity{{Namespace: task.Namespace, TaskID: string(acpTaskControlUID(task))}}
	orderedPublications := make([]string, 0, len(publicationIDs))
	for publicationID := range publicationIDs {
		orderedPublications = append(orderedPublications, publicationID)
	}
	sort.Strings(orderedPublications)
	for _, publicationID := range orderedPublications {
		identities = append(identities, artifactcap.Identity{Namespace: task.Namespace, PublicationID: publicationID})
	}
	return identities, true, nil
}
