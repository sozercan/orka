package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/taskterminal"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CreatePromptAttempt creates the immutable PromptAttempt spec and initializes
// its status under the exact controller-epoch fence.
func (s *Store) CreatePromptAttempt(ctx context.Context, attempt *store.PromptAttempt, fence store.ControllerEpochFence) (*store.PromptAttempt, error) {
	if err := s.requireClient(); err != nil {
		return nil, err
	}
	normalized, normalizedFence, err := normalizePromptAttemptForCreate(attempt, fence)
	if err != nil {
		return nil, err
	}
	_, snapshot, err := s.requireControllerEpoch(ctx, normalizedFence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	if err := s.rejectPromptAttemptCreationAfterReclamation(ctx, normalized.Key.Namespace, normalized.Key.TaskUID); err != nil {
		return nil, err
	}

	name := objectName(promptAttemptNamePrefix, normalized.ID)
	key := client.ObjectKey{Namespace: normalized.Key.Namespace, Name: name}
	object := &corev1alpha1.PromptAttempt{}
	err = s.readClient().Get(ctx, key, object)
	if err == nil {
		return s.completePromptAttemptCreationWithReclamationFence(ctx, object, normalized, normalizedFence, snapshot, false)
	}
	if !apierrors.IsNotFound(err) {
		return nil, mapKubernetesError("get prompt attempt", err)
	}

	labels := controlLabels(normalized.ID)
	labelIfValid(labels, corev1alpha1.ControlRecordTaskUIDLabel, normalized.Key.TaskUID)
	object = &corev1alpha1.PromptAttempt{
		ObjectMeta: metav1.ObjectMeta{Namespace: normalized.Key.Namespace, Name: name, Labels: labels},
		Spec: corev1alpha1.PromptAttemptSpec{
			ID: normalized.ID, TaskUID: normalized.Key.TaskUID, Attempt: normalized.Key.Attempt,
			PromptID: normalized.Key.PromptID, RequestDigest: normalized.RequestDigest,
			BindingDigest: normalized.BindingDigest, SnapshotDigest: normalized.SnapshotDigest,
			CredentialBindings: promptCredentialBindingsToAPI(normalized.CredentialBindings),
		},
	}
	if err := s.client.Create(ctx, object); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if getErr := s.readClient().Get(ctx, key, object); getErr != nil {
				return nil, mapKubernetesError("get concurrently created prompt attempt", getErr)
			}
			return s.completePromptAttemptCreationWithReclamationFence(ctx, object, normalized, normalizedFence, snapshot, false)
		}
		return nil, mapKubernetesError("create prompt attempt", err)
	}
	return s.completePromptAttemptCreationWithReclamationFence(ctx, object, normalized, normalizedFence, snapshot, true)
}

func (s *Store) completePromptAttemptCreationWithReclamationFence(
	ctx context.Context,
	object *corev1alpha1.PromptAttempt,
	normalized store.PromptAttempt,
	fence store.ControllerEpochFence,
	snapshot epochSnapshot,
	created bool,
) (*store.PromptAttempt, error) {
	result, err := s.completePromptAttemptCreation(ctx, object, normalized, fence, snapshot)
	if err != nil {
		return nil, err
	}
	if err := s.rejectPromptAttemptCreationAfterReclamation(ctx, normalized.Key.Namespace, normalized.Key.TaskUID); err != nil {
		if created {
			uid, resourceVersion := object.UID, object.ResourceVersion
			deleteErr := s.client.Delete(ctx, object, &client.DeleteOptions{Preconditions: &metav1.Preconditions{
				UID: &uid, ResourceVersion: &resourceVersion,
			}})
			if deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
				return nil, fmt.Errorf("rollback PromptAttempt created after reclamation preparation: %v (original error: %w)", deleteErr, err)
			}
		}
		return nil, err
	}
	return result, nil
}

func (s *Store) rejectPromptAttemptCreationAfterReclamation(ctx context.Context, namespace, taskUID string) error {
	task, err := s.findPromptAttemptOwnerTask(ctx, namespace, taskUID)
	if err != nil {
		return err
	}
	if task.Spec.Type != corev1alpha1.TaskTypeAgent {
		return store.ConflictErrorf("Task %s/%s is not an agent Task", task.Namespace, task.Name)
	}
	if !task.DeletionTimestamp.IsZero() {
		return store.ConflictErrorf("Task %s/%s is being deleted", task.Namespace, task.Name)
	}
	if condition := meta.FindStatusCondition(task.Status.Conditions, promptAttemptReclamationCompleteCondition); condition != nil {
		return store.ConflictErrorf("Task %q has already completed PromptAttempt reclamation", taskUID)
	}

	marker := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: s.controlNamespace, Name: promptAttemptReclamationMarkerName(taskUID)}
	if err := s.readClient().Get(ctx, key, marker); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return mapKubernetesError("get prompt attempt reclamation marker before create", err)
	}
	prepared, err := promptAttemptReclamationMarkerFromConfigMap(marker)
	if err != nil {
		return err
	}
	if prepared.Namespace != namespace || prepared.TaskUID != taskUID {
		return store.ConflictErrorf("PromptAttempt reclamation marker does not match Task %q", taskUID)
	}
	return store.ConflictErrorf("Task %q has already prepared PromptAttempt reclamation", taskUID)
}

func (s *Store) findPromptAttemptOwnerTask(ctx context.Context, namespace, taskUID string) (*corev1alpha1.Task, error) {
	tasks := &corev1alpha1.TaskList{}
	if err := s.readClient().List(ctx, tasks, client.InNamespace(namespace)); err != nil {
		return nil, mapKubernetesError("list Tasks for PromptAttempt owner", err)
	}
	var found *corev1alpha1.Task
	for i := range tasks.Items {
		task := &tasks.Items[i]
		if string(task.UID) != taskUID {
			continue
		}
		if found != nil {
			return nil, store.ConflictErrorf("Task UID %q is not unique in namespace %q", taskUID, namespace)
		}
		found = task.DeepCopy()
	}
	if found == nil {
		return nil, store.ConflictErrorf("Task UID %q does not identify a live Task in namespace %q", taskUID, namespace)
	}
	return found, nil
}

// GetPromptAttempt returns a PromptAttempt by canonical ID within the configured watch scope.
func (s *Store) GetPromptAttempt(ctx context.Context, id string) (*store.PromptAttempt, error) {
	if err := s.requireClient(); err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	if err := store.ValidateControlIdentifier("prompt attempt ID", id); err != nil {
		return nil, err
	}
	object, err := s.findPromptAttemptByID(ctx, id)
	if err != nil {
		return nil, err
	}
	result := promptAttemptFromObject(object)
	return &result, nil
}

const (
	promptAttemptReclamationNamePrefix = "prompt-reclaim-"
	promptAttemptReclamationDataKey    = "reclamation.json"
	promptAttemptReclamationVersion    = 1
	restoredTaskIdentityChangedReason  = corev1alpha1.TaskExecutionReason("RestoreIdentityChanged")

	promptAttemptReclamationCompleteCondition = "ACPPromptAttemptsReclaimed"
	promptAttemptReclamationCompleteReason    = "PromptAttemptsReclaimed"
)

type promptAttemptReclamationCompletionReceipt struct {
	Version                           int                                `json:"version"`
	Namespace                         string                             `json:"namespace"`
	TaskName                          string                             `json:"taskName"`
	TaskUID                           string                             `json:"taskUid"`
	Mode                              store.PromptAttemptReclamationMode `json:"mode"`
	ContinuitySession                 bool                               `json:"continuitySession,omitempty"`
	FinalContinuitySession            bool                               `json:"finalContinuitySession,omitempty"`
	FinalPromptAttemptID              string                             `json:"finalPromptAttemptId,omitempty"`
	TerminalProjectionID              string                             `json:"terminalProjectionId,omitempty"`
	RelatedExternalEffectAggregateIDs []string                           `json:"relatedExternalEffectAggregateIds,omitempty"`
}

type promptAttemptReclamationMarker struct {
	Version                             int                                `json:"version"`
	Namespace                           string                             `json:"namespace"`
	TaskName                            string                             `json:"taskName"`
	TaskUID                             string                             `json:"taskUid"`
	Mode                                store.PromptAttemptReclamationMode `json:"mode"`
	ContinuitySession                   bool                               `json:"continuitySession,omitempty"`
	FinalContinuitySession              bool                               `json:"finalContinuitySession,omitempty"`
	FinalPromptAttemptID                string                             `json:"finalPromptAttemptId,omitempty"`
	TerminalProjectionID                string                             `json:"terminalProjectionId,omitempty"`
	TerminalProjectionPayloadDigest     string                             `json:"terminalProjectionPayloadDigest,omitempty"`
	FinalSessionTurnID                  string                             `json:"finalSessionTurnId,omitempty"`
	TerminalProjectionAggregateKind     string                             `json:"terminalProjectionAggregateKind,omitempty"`
	TerminalProjectionAggregateID       string                             `json:"terminalProjectionAggregateId,omitempty"`
	CandidateIDs                        []string                           `json:"candidateIds,omitempty"`
	RequestedExternalEffectAggregateIDs []string                           `json:"requestedExternalEffectAggregateIds,omitempty"`
	RelatedExternalEffectAggregateIDs   []string                           `json:"relatedExternalEffectAggregateIds,omitempty"`
}

// PreparePromptAttemptReclamation proves the deletion barriers and persists a
// durable marker before any PromptAttempt is removed. The marker makes partial
// deletion retries safe until ReclaimPromptAttempts removes it after completion.
func (s *Store) PreparePromptAttemptReclamation(ctx context.Context, request store.ReclaimPromptAttemptsRequest) error {
	if err := s.requireClient(); err != nil {
		return err
	}
	normalized, err := normalizePromptAttemptReclamationRequestKube(request)
	if err != nil {
		return err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, normalized.Fence)
	if err != nil {
		return err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	normalized.Fence = fence
	_, _, err = s.preparePromptAttemptReclamationLocked(ctx, normalized, fence, snapshot)
	return err
}

// ReclaimPromptAttempts removes every PromptAttempt authorized by the durable
// reclamation marker. If no marker exists, it performs the same preflight and
// creates the marker before issuing the first delete. The final attempt is
// deleted last so a partial Kubernetes deletion remains safely retryable. Once
// every authorized PromptAttempt is absent, the marker is removed with exact
// object preconditions; a later projected retry with no marker or candidates is
// treated as an already-completed deletion.
func (s *Store) ReclaimPromptAttempts(ctx context.Context, request store.ReclaimPromptAttemptsRequest) (int, error) {
	if err := s.requireClient(); err != nil {
		return 0, err
	}
	normalized, err := normalizePromptAttemptReclamationRequestKube(request)
	if err != nil {
		return 0, err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, normalized.Fence)
	if err != nil {
		return 0, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	normalized.Fence = fence

	marker, candidates, err := s.preparePromptAttemptReclamationLocked(ctx, normalized, fence, snapshot)
	if err != nil {
		return 0, err
	}
	if marker.Version == 0 {
		return 0, nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		leftFinal := candidates[i].Spec.ID == marker.FinalPromptAttemptID
		rightFinal := candidates[j].Spec.ID == marker.FinalPromptAttemptID
		if leftFinal != rightFinal {
			return !leftFinal
		}
		if candidates[i].Spec.Attempt != candidates[j].Spec.Attempt {
			return candidates[i].Spec.Attempt < candidates[j].Spec.Attempt
		}
		return candidates[i].Spec.ID < candidates[j].Spec.ID
	})
	deleted := 0
	for _, object := range candidates {
		uid := object.UID
		resourceVersion := object.ResourceVersion
		err := s.client.Delete(ctx, object, &client.DeleteOptions{Preconditions: &metav1.Preconditions{
			UID: &uid, ResourceVersion: &resourceVersion,
		}})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return deleted, mapKubernetesError("delete reclaimed prompt attempt", err)
		}
		deleted++
	}
	if err := s.removeCompletedPromptAttemptReclamationMarker(ctx, marker, func() error {
		return s.recordPromptAttemptReclamationCompletion(ctx, normalized)
	}); err != nil {
		return deleted, err
	}
	return deleted, nil
}

func (s *Store) preparePromptAttemptReclamationLocked(
	ctx context.Context,
	request store.ReclaimPromptAttemptsRequest,
	fence store.ControllerEpochFence,
	snapshot epochSnapshot,
) (promptAttemptReclamationMarker, []*corev1alpha1.PromptAttempt, error) {
	task := &corev1alpha1.Task{}
	taskKey := client.ObjectKey{Namespace: request.Namespace, Name: request.TaskName}
	if err := s.readClient().Get(ctx, taskKey, task); err != nil {
		return promptAttemptReclamationMarker{}, nil, mapKubernetesError("get Task for prompt attempt reclamation", err)
	}
	if task.Spec.Type != corev1alpha1.TaskTypeAgent || task.DeletionTimestamp.IsZero() {
		return promptAttemptReclamationMarker{}, nil, store.ConflictErrorf("Task %s/%s does not match deleting agent Task UID %q", request.Namespace, request.TaskName, request.TaskUID)
	}

	candidates, err := s.listTaskPromptAttempts(ctx, request.Namespace, request.TaskUID)
	if err != nil {
		return promptAttemptReclamationMarker{}, nil, err
	}
	if err := validatePromptAttemptReclamationTaskIdentityKube(task, request, candidates); err != nil {
		return promptAttemptReclamationMarker{}, nil, err
	}
	completed, err := promptAttemptReclamationCompleted(task, request)
	if err != nil {
		return promptAttemptReclamationMarker{}, nil, err
	}
	if completed && len(candidates) != 0 {
		return promptAttemptReclamationMarker{}, nil, store.ConflictErrorf("Task %q has a reclamation completion receipt but still owns PromptAttempts", request.TaskUID)
	}
	markerObject := &corev1.ConfigMap{}
	markerKey := client.ObjectKey{Namespace: s.controlNamespace, Name: promptAttemptReclamationMarkerName(request.TaskUID)}
	markerErr := s.readClient().Get(ctx, markerKey, markerObject)
	if markerErr == nil {
		marker, parseErr := promptAttemptReclamationMarkerFromConfigMap(markerObject)
		if parseErr != nil {
			return promptAttemptReclamationMarker{}, nil, parseErr
		}
		if err := validatePromptAttemptReclamationMarkerRequestKube(marker, request, task); err != nil {
			return promptAttemptReclamationMarker{}, nil, err
		}
		if err := s.verifyPreparedPromptAttemptReclamationKube(ctx, marker, candidates); err != nil {
			return promptAttemptReclamationMarker{}, nil, err
		}
		return marker, candidates, nil
	}
	if !apierrors.IsNotFound(markerErr) {
		return promptAttemptReclamationMarker{}, nil, mapKubernetesError("get prompt attempt reclamation marker", markerErr)
	}
	if completed {
		return promptAttemptReclamationMarker{}, nil, nil
	}

	if request.Mode == store.PromptAttemptReclamationUnbound {
		candidates, err = s.settleUnboundPromptAttemptsForReclamation(ctx, candidates, fence, snapshot)
		if err != nil {
			return promptAttemptReclamationMarker{}, nil, err
		}
	}
	marker, err := s.buildPromptAttemptReclamationMarkerKube(ctx, request, candidates)
	if err != nil {
		return promptAttemptReclamationMarker{}, nil, err
	}
	encoded, err := json.Marshal(marker)
	if err != nil {
		return promptAttemptReclamationMarker{}, nil, fmt.Errorf("encode prompt attempt reclamation marker: %w", err)
	}
	immutable := true
	markerObject = &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: s.controlNamespace,
			Name:      markerKey.Name,
			Labels:    controlLabels(store.CanonicalControlID("prompt-attempt-reclamation", request.Namespace, request.TaskUID)),
		},
		Immutable: &immutable,
		Data:      map[string]string{promptAttemptReclamationDataKey: string(encoded)},
	}
	labelIfValid(markerObject.Labels, corev1alpha1.ControlRecordTaskUIDLabel, request.TaskUID)
	createdMarker := false
	if err := s.client.Create(ctx, markerObject); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return promptAttemptReclamationMarker{}, nil, mapKubernetesError("create prompt attempt reclamation marker", err)
		}
		if getErr := s.readClient().Get(ctx, markerKey, markerObject); getErr != nil {
			return promptAttemptReclamationMarker{}, nil, mapKubernetesError("get concurrently created prompt attempt reclamation marker", getErr)
		}
		existing, parseErr := promptAttemptReclamationMarkerFromConfigMap(markerObject)
		if parseErr != nil {
			return promptAttemptReclamationMarker{}, nil, parseErr
		}
		if err := validatePromptAttemptReclamationMarkerRequestKube(existing, request, task); err != nil {
			return promptAttemptReclamationMarker{}, nil, err
		}
		marker = existing
	} else {
		createdMarker = true
	}
	freshCandidates, err := s.listTaskPromptAttempts(ctx, request.Namespace, request.TaskUID)
	if err == nil {
		err = s.verifyPreparedPromptAttemptReclamationKube(ctx, marker, freshCandidates)
	}
	if err != nil {
		if createdMarker {
			err = errors.Join(err, s.rollbackPromptAttemptReclamationMarker(ctx, markerObject))
		}
		return promptAttemptReclamationMarker{}, nil, err
	}
	return marker, freshCandidates, nil
}

func (s *Store) rollbackPromptAttemptReclamationMarker(ctx context.Context, marker *corev1.ConfigMap) error {
	if marker == nil {
		return nil
	}
	uid := marker.UID
	resourceVersion := marker.ResourceVersion
	err := s.client.Delete(ctx, marker, &client.DeleteOptions{Preconditions: &metav1.Preconditions{
		UID: &uid, ResourceVersion: &resourceVersion,
	}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return mapKubernetesError("roll back raced prompt attempt reclamation marker", err)
}

func (s *Store) removeCompletedPromptAttemptReclamationMarker(
	ctx context.Context,
	expected promptAttemptReclamationMarker,
	recordCompletion func() error,
) error {
	remaining, err := s.listTaskPromptAttempts(ctx, expected.Namespace, expected.TaskUID)
	if err != nil {
		return err
	}
	markerObject := &corev1.ConfigMap{}
	markerKey := client.ObjectKey{Namespace: s.controlNamespace, Name: promptAttemptReclamationMarkerName(expected.TaskUID)}
	if err := s.readClient().Get(ctx, markerKey, markerObject); err != nil {
		if apierrors.IsNotFound(err) && len(remaining) == 0 {
			return recordCompletion()
		}
		if apierrors.IsNotFound(err) {
			return store.ConflictErrorf("prompt attempt reclamation marker disappeared while Task %q still owns PromptAttempts", expected.TaskUID)
		}
		return mapKubernetesError("get completed prompt attempt reclamation marker", err)
	}
	current, err := promptAttemptReclamationMarkerFromConfigMap(markerObject)
	if err != nil {
		return err
	}
	expected.CandidateIDs = normalizePromptAttemptReclamationIDs(expected.CandidateIDs)
	expected.RequestedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDs(expected.RequestedExternalEffectAggregateIDs)
	expected.RelatedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDs(expected.RelatedExternalEffectAggregateIDs)
	if !reflect.DeepEqual(current, expected) {
		return store.ConflictErrorf("prompt attempt reclamation marker changed before cleanup")
	}
	if err := s.verifyPreparedPromptAttemptReclamationKube(ctx, current, remaining); err != nil {
		return err
	}
	if len(remaining) != 0 {
		return promptAttemptReclaimNotReady("Task %q still has %d PromptAttempts after reclamation", expected.TaskUID, len(remaining))
	}
	if err := recordCompletion(); err != nil {
		return err
	}
	uid := markerObject.UID
	resourceVersion := markerObject.ResourceVersion
	err = s.client.Delete(ctx, markerObject, &client.DeleteOptions{Preconditions: &metav1.Preconditions{
		UID: &uid, ResourceVersion: &resourceVersion,
	}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return mapKubernetesError("delete completed prompt attempt reclamation marker", err)
}

func promptAttemptReclamationCompletionDigest(request store.ReclaimPromptAttemptsRequest) (string, error) {
	terminalProjectionID := request.TerminalProjectionID
	if request.ContinuitySession && !request.FinalContinuitySession {
		// Before a continuity Session is bound, the initial cleanup proves the
		// standalone Task projection through the durable marker. After the final
		// PromptAttempt is gone the controller intentionally cannot reconstruct
		// that ID, so the Task-scoped completion receipt treats it as a wildcard.
		terminalProjectionID = ""
	}
	receipt := promptAttemptReclamationCompletionReceipt{
		Version:                           promptAttemptReclamationVersion,
		Namespace:                         request.Namespace,
		TaskName:                          request.TaskName,
		TaskUID:                           request.TaskUID,
		Mode:                              request.Mode,
		ContinuitySession:                 request.ContinuitySession,
		FinalContinuitySession:            request.FinalContinuitySession,
		FinalPromptAttemptID:              request.FinalPromptAttemptID,
		TerminalProjectionID:              terminalProjectionID,
		RelatedExternalEffectAggregateIDs: append([]string(nil), request.RelatedExternalEffectAggregateIDs...),
	}
	receipt.RelatedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDs(receipt.RelatedExternalEffectAggregateIDs)
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return "", fmt.Errorf("encode PromptAttempt reclamation completion receipt: %w", err)
	}
	return store.CanonicalBytesDigest(encoded), nil
}

func promptAttemptReclamationCompleted(task *corev1alpha1.Task, request store.ReclaimPromptAttemptsRequest) (bool, error) {
	condition := meta.FindStatusCondition(task.Status.Conditions, promptAttemptReclamationCompleteCondition)
	if condition == nil {
		return false, nil
	}
	expectedDigest, err := promptAttemptReclamationCompletionDigest(request)
	if err != nil {
		return false, err
	}
	if condition.Status != metav1.ConditionTrue || condition.Reason != promptAttemptReclamationCompleteReason || condition.Message != expectedDigest {
		return false, store.ConflictErrorf("Task %s/%s PromptAttempt reclamation completion receipt does not match the requested cleanup", task.Namespace, task.Name)
	}
	return true, nil
}

func (s *Store) recordPromptAttemptReclamationCompletion(ctx context.Context, request store.ReclaimPromptAttemptsRequest) error {
	digest, err := promptAttemptReclamationCompletionDigest(request)
	if err != nil {
		return err
	}
	key := client.ObjectKey{Namespace: request.Namespace, Name: request.TaskName}
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		task := &corev1alpha1.Task{}
		if err := s.readClient().Get(ctx, key, task); err != nil {
			return err
		}
		if task.Spec.Type != corev1alpha1.TaskTypeAgent || task.DeletionTimestamp.IsZero() {
			return store.ConflictErrorf("Task %s/%s no longer matches deleting agent Task UID %q", request.Namespace, request.TaskName, request.TaskUID)
		}
		if err := validatePromptAttemptReclamationTaskIdentityKube(task, request, nil); err != nil {
			return err
		}
		if completed, completedErr := promptAttemptReclamationCompleted(task, request); completedErr != nil {
			return completedErr
		} else if completed {
			return nil
		}
		meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
			Type:               promptAttemptReclamationCompleteCondition,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: task.Generation,
			LastTransitionTime: metav1.Now(),
			Reason:             promptAttemptReclamationCompleteReason,
			Message:            digest,
		})
		if err := s.client.Status().Update(ctx, task); err != nil {
			return err
		}
		return nil
	})
	return mapKubernetesError("record PromptAttempt reclamation completion", err)
}

func validatePromptAttemptReclamationTaskIdentityKube(
	task *corev1alpha1.Task,
	request store.ReclaimPromptAttemptsRequest,
	candidates []*corev1alpha1.PromptAttempt,
) error {
	if task == nil {
		return store.ConflictErrorf("prompt attempt reclamation Task is missing")
	}
	if string(task.UID) == request.TaskUID {
		return nil
	}
	if request.Mode != store.PromptAttemptReclamationProjected {
		return store.ConflictErrorf("restored Task %s/%s may only reclaim its projected source PromptAttempt", task.Namespace, task.Name)
	}
	binding := task.Status.AgentExecutionBinding
	execution := task.Status.Execution
	if binding == nil || execution == nil || binding.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV2 ||
		string(binding.Task.UID) != request.TaskUID || binding.Task.UID == task.UID ||
		execution.Reason != restoredTaskIdentityChangedReason ||
		execution.Attempt < 1 || strings.TrimSpace(execution.PromptID) == "" ||
		!restoredTaskExecutionIsTerminal(task) {
		return store.ConflictErrorf("Task %s/%s does not contain validated restored source identity %q", task.Namespace, task.Name, request.TaskUID)
	}
	snapshotKey := store.AgentExecutionSnapshotKey{TaskUID: request.TaskUID, Digest: binding.Snapshot.Digest}
	if binding.Snapshot.ID != snapshotKey.ID() || binding.Snapshot.SchemaVersion != store.AgentExecutionSnapshotSchemaVersion {
		return store.ConflictErrorf("Task %s/%s restored execution snapshot does not match source identity %q", task.Namespace, task.Name, request.TaskUID)
	}
	attemptID, err := (store.PromptAttemptKey{
		Namespace: task.Namespace,
		TaskUID:   request.TaskUID,
		Attempt:   int64(execution.Attempt),
		PromptID:  execution.PromptID,
	}).CanonicalID()
	if err != nil {
		return err
	}
	if request.FinalPromptAttemptID != attemptID {
		return store.ConflictErrorf("Task %s/%s restored final PromptAttempt does not match source identity %q", task.Namespace, task.Name, request.TaskUID)
	}
	if len(candidates) == 0 {
		// A prepared retry may observe no candidates after deletion. The immutable
		// marker and Task-scoped completion digest still revalidate the full request.
		return nil
	}
	for _, object := range candidates {
		if object.Spec.ID != attemptID {
			continue
		}
		if object.Spec.TaskUID != request.TaskUID || object.Spec.Attempt != int64(execution.Attempt) ||
			object.Spec.PromptID != execution.PromptID || object.Spec.BindingDigest != binding.BindingDigest ||
			object.Spec.SnapshotDigest != binding.Snapshot.Digest ||
			(strings.TrimSpace(execution.RequestDigest) != "" && object.Spec.RequestDigest != execution.RequestDigest) {
			return store.ConflictErrorf("Task %s/%s restored final PromptAttempt does not match its frozen execution binding", task.Namespace, task.Name)
		}
		return nil
	}
	return store.ConflictErrorf("Task %s/%s restored final PromptAttempt %q is not owned by the source identity", task.Namespace, task.Name, attemptID)
}

func restoredTaskExecutionIsTerminal(task *corev1alpha1.Task) bool {
	if task == nil || task.Status.Execution == nil || task.Status.Delivery == nil {
		return false
	}
	execution := task.Status.Execution
	delivery := task.Status.Delivery
	deliveryState := store.PromptDeliveryState(delivery.State)
	if !store.IsTerminalPromptDeliveryState(deliveryState) ||
		string(delivery.Outcome) != string(delivery.State) {
		return false
	}
	switch execution.State {
	case corev1alpha1.TaskExecutionStateSucceeded:
		if execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded {
			return false
		}
		switch deliveryState {
		case store.PromptDeliveryNotRequested, store.PromptDeliveryReadValidated,
			store.PromptDeliveryNoChange, store.PromptDeliveryVerifiedExact,
			store.PromptDeliveryDeliveredSuperseded:
			return task.Status.Phase == corev1alpha1.TaskPhaseSucceeded
		case store.PromptDeliveryCancelledBeforePublish:
			return task.Status.Phase == corev1alpha1.TaskPhaseCancelled
		case store.PromptDeliveryReadOnlyWorkspaceModified, store.PromptDeliveryConflict,
			store.PromptDeliveryCredentialBlocked, store.PromptDeliveryPublicationOutcomeUnknown:
			return task.Status.Phase == corev1alpha1.TaskPhaseFailed
		default:
			return false
		}
	case corev1alpha1.TaskExecutionStateCancelled:
		return execution.Outcome == corev1alpha1.TaskExecutionOutcomeCancelled &&
			task.Status.Phase == corev1alpha1.TaskPhaseCancelled
	case corev1alpha1.TaskExecutionStateFailed:
		return execution.Outcome == corev1alpha1.TaskExecutionOutcomeFailed &&
			task.Status.Phase == corev1alpha1.TaskPhaseFailed
	case corev1alpha1.TaskExecutionStateOutcomeUnknown:
		return execution.Outcome == corev1alpha1.TaskExecutionOutcomeOutcomeUnknown &&
			task.Status.Phase == corev1alpha1.TaskPhaseFailed
	default:
		return false
	}
}

func normalizePromptAttemptReclamationRequestKube(request store.ReclaimPromptAttemptsRequest) (store.ReclaimPromptAttemptsRequest, error) {
	request.Namespace = strings.TrimSpace(request.Namespace)
	if err := validateKubernetesNamespace(request.Namespace); err != nil {
		return store.ReclaimPromptAttemptsRequest{}, err
	}
	request.TaskName = strings.TrimSpace(request.TaskName)
	if err := store.ValidateControlIdentifier("prompt attempt Task name", request.TaskName); err != nil {
		return store.ReclaimPromptAttemptsRequest{}, err
	}
	request.TaskUID = strings.TrimSpace(request.TaskUID)
	if err := store.ValidateControlIdentifier("prompt attempt Task UID", request.TaskUID); err != nil {
		return store.ReclaimPromptAttemptsRequest{}, err
	}
	switch request.Mode {
	case store.PromptAttemptReclamationProjected, store.PromptAttemptReclamationUnbound, store.PromptAttemptReclamationNoAttempt:
	default:
		return store.ReclaimPromptAttemptsRequest{}, store.ValidationErrorf("unsupported prompt attempt reclamation mode %q", request.Mode)
	}
	if request.FinalContinuitySession && (!request.ContinuitySession || request.Mode != store.PromptAttemptReclamationProjected) {
		return store.ReclaimPromptAttemptsRequest{}, store.ValidationErrorf("final continuity SessionTurn binding requires projected continuity reclamation")
	}
	request.FinalPromptAttemptID = strings.TrimSpace(request.FinalPromptAttemptID)
	request.TerminalProjectionID = strings.TrimSpace(request.TerminalProjectionID)
	if request.Mode == store.PromptAttemptReclamationProjected {
		if err := store.ValidateControlIdentifier("final prompt attempt ID", request.FinalPromptAttemptID); err != nil {
			return store.ReclaimPromptAttemptsRequest{}, err
		}
		if request.TerminalProjectionID != "" {
			if err := store.ValidateControlIdentifier("terminal projection ID", request.TerminalProjectionID); err != nil {
				return store.ReclaimPromptAttemptsRequest{}, err
			}
		}
	} else if request.FinalPromptAttemptID != "" || request.TerminalProjectionID != "" {
		return store.ReclaimPromptAttemptsRequest{}, store.ValidationErrorf("only projected prompt attempt reclamation accepts final attempt and projection IDs")
	}
	request.RelatedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDs(request.RelatedExternalEffectAggregateIDs)
	for _, id := range request.RelatedExternalEffectAggregateIDs {
		if err := store.ValidateControlIdentifier("related external effect aggregate ID", id); err != nil {
			return store.ReclaimPromptAttemptsRequest{}, err
		}
	}
	return request, nil
}

func normalizePromptAttemptReclamationIDs(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func promptAttemptReclamationMarkerName(taskUID string) string {
	return objectName(promptAttemptReclamationNamePrefix, taskUID)
}

func promptAttemptReclamationMarkerFromConfigMap(object *corev1.ConfigMap) (promptAttemptReclamationMarker, error) {
	if object == nil || object.Data == nil || strings.TrimSpace(object.Data[promptAttemptReclamationDataKey]) == "" {
		return promptAttemptReclamationMarker{}, store.ConflictErrorf("prompt attempt reclamation marker is incomplete")
	}
	var marker promptAttemptReclamationMarker
	if err := json.Unmarshal([]byte(object.Data[promptAttemptReclamationDataKey]), &marker); err != nil {
		return promptAttemptReclamationMarker{}, store.ConflictErrorf("prompt attempt reclamation marker is invalid: %v", err)
	}
	if marker.Version != promptAttemptReclamationVersion {
		return promptAttemptReclamationMarker{}, store.ConflictErrorf("prompt attempt reclamation marker version %d is unsupported", marker.Version)
	}
	marker.CandidateIDs = normalizePromptAttemptReclamationIDs(marker.CandidateIDs)
	marker.RequestedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDs(marker.RequestedExternalEffectAggregateIDs)
	marker.RelatedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDs(marker.RelatedExternalEffectAggregateIDs)
	return marker, nil
}

func validatePromptAttemptReclamationMarkerRequestKube(
	marker promptAttemptReclamationMarker,
	request store.ReclaimPromptAttemptsRequest,
	task *corev1alpha1.Task,
) error {
	modeMatches := marker.Mode == request.Mode ||
		request.Mode == store.PromptAttemptReclamationUnbound && marker.Mode == store.PromptAttemptReclamationNoAttempt
	if marker.Namespace != request.Namespace || marker.TaskName != request.TaskName || marker.TaskUID != request.TaskUID ||
		!modeMatches || marker.ContinuitySession != request.ContinuitySession ||
		!reflect.DeepEqual(marker.RequestedExternalEffectAggregateIDs, request.RelatedExternalEffectAggregateIDs) {
		return store.ConflictErrorf("prompt attempt reclamation marker does not match the current Task request")
	}
	if request.Mode == store.PromptAttemptReclamationProjected &&
		(marker.FinalPromptAttemptID != request.FinalPromptAttemptID ||
			request.TerminalProjectionID != "" && (marker.TerminalProjectionID != request.TerminalProjectionID || marker.FinalContinuitySession != request.FinalContinuitySession)) {
		return store.ConflictErrorf("prompt attempt reclamation marker does not match the current final attempt/projection")
	}
	if task == nil || len(marker.CandidateIDs) == 0 && marker.Mode == store.PromptAttemptReclamationProjected {
		return store.ConflictErrorf("prompt attempt reclamation marker has no projected candidate set")
	}
	return nil
}

func (s *Store) listTaskPromptAttempts(ctx context.Context, namespace, taskUID string) ([]*corev1alpha1.PromptAttempt, error) {
	list := &corev1alpha1.PromptAttemptList{}
	if err := s.readClient().List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, mapKubernetesError("list prompt attempts for Task reclamation", err)
	}
	candidates := make([]*corev1alpha1.PromptAttempt, 0)
	seen := make(map[string]struct{})
	for i := range list.Items {
		object := &list.Items[i]
		if object.Spec.TaskUID != taskUID {
			continue
		}
		if _, exists := seen[object.Spec.ID]; exists {
			return nil, store.ConflictErrorf("Task %q has duplicate PromptAttempt identity %q", taskUID, object.Spec.ID)
		}
		seen[object.Spec.ID] = struct{}{}
		candidates = append(candidates, object.DeepCopy())
	}
	return candidates, nil
}

func (s *Store) settleUnboundPromptAttemptsForReclamation(
	ctx context.Context,
	candidates []*corev1alpha1.PromptAttempt,
	fence store.ControllerEpochFence,
	snapshot epochSnapshot,
) ([]*corev1alpha1.PromptAttempt, error) {
	result := make([]*corev1alpha1.PromptAttempt, 0, len(candidates))
	var newestID string
	if len(candidates) > 0 {
		attempts := make([]store.PromptAttempt, 0, len(candidates))
		for _, object := range candidates {
			attempts = append(attempts, promptAttemptFromObject(object))
		}
		newest, err := newestPromptAttemptForReclamationKube(attempts)
		if err != nil {
			return nil, err
		}
		newestID = newest.ID
	}
	for _, object := range candidates {
		attempt := promptAttemptFromObject(object)
		if attempt.ID != newestID {
			if !store.IsTerminalPromptExecutionState(attempt.ExecutionState) || !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
				return nil, promptAttemptReclaimNotReady("historical prompt attempt %q is not terminal", attempt.ID)
			}
			result = append(result, object)
			continue
		}
		if attempt.SessionUID != "" || attempt.SessionLeaseGeneration != 0 || attempt.RuntimeInstanceID != "" {
			return nil, promptAttemptReclaimNotReady("unbound prompt attempt %q already has a runtime or Session binding", attempt.ID)
		}
		if store.IsTerminalPromptExecutionState(attempt.ExecutionState) {
			if !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
				return nil, promptAttemptReclaimNotReady("unbound prompt attempt %q delivery is not terminal", attempt.ID)
			}
			result = append(result, object)
			continue
		}
		if attempt.ExecutionState != store.PromptExecutionQueued || attempt.DeliveryState != store.PromptDeliveryNotRequested {
			return nil, promptAttemptReclaimNotReady("unbound prompt attempt %q is not safely cancellable from state %s", attempt.ID, attempt.ExecutionState)
		}
		operationID := store.CanonicalControlID("reclaim-unbound-cancel", attempt.ID)
		operationDigest := store.CanonicalBytesDigest([]byte("reclaim-unbound-cancel:" + attempt.ID))
		updated := object.DeepCopy()
		updated.Status.ExecutionState = corev1alpha1.PromptAttemptExecutionState(store.PromptExecutionCancelled)
		updated.Status.TerminalReason = "Task deleted before durable execution status binding"
		setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, attempt.Version+1, operationID, operationDigest, attempt.CreatedAt, store.NormalizeControlTime(time.Time{}))
		if err := s.client.Status().Update(ctx, updated); err != nil {
			return nil, mapKubernetesError("cancel unbound prompt attempt for reclamation", err)
		}
		result = append(result, updated)
	}
	return result, nil
}

func (s *Store) buildPromptAttemptReclamationMarkerKube(
	ctx context.Context,
	request store.ReclaimPromptAttemptsRequest,
	candidates []*corev1alpha1.PromptAttempt,
) (promptAttemptReclamationMarker, error) {
	effectiveMode := request.Mode
	if effectiveMode == store.PromptAttemptReclamationUnbound && len(candidates) == 0 {
		effectiveMode = store.PromptAttemptReclamationNoAttempt
	}
	marker := promptAttemptReclamationMarker{
		Version: promptAttemptReclamationVersion, Namespace: request.Namespace, TaskName: request.TaskName,
		TaskUID: request.TaskUID, Mode: effectiveMode, ContinuitySession: request.ContinuitySession,
		FinalContinuitySession:              request.FinalContinuitySession,
		RequestedExternalEffectAggregateIDs: append([]string(nil), request.RelatedExternalEffectAggregateIDs...),
	}
	if effectiveMode == store.PromptAttemptReclamationNoAttempt {
		if len(candidates) != 0 {
			return promptAttemptReclamationMarker{}, store.ConflictErrorf("Task %q declared no durable attempt but owns %d PromptAttempts", request.TaskUID, len(candidates))
		}
		marker.RelatedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDs(append([]string{request.TaskUID}, request.RelatedExternalEffectAggregateIDs...))
		if _, err := s.verifyPromptAttemptReferencesKube(ctx, marker, nil); err != nil {
			return promptAttemptReclamationMarker{}, err
		}
		return marker, nil
	}
	if len(candidates) == 0 {
		return promptAttemptReclamationMarker{}, promptAttemptReclaimNotReady("Task %q has no durable PromptAttempt and no reclamation marker", request.TaskUID)
	}

	attempts := make([]store.PromptAttempt, 0, len(candidates))
	for _, object := range candidates {
		attempt := promptAttemptFromObject(object)
		if !store.IsTerminalPromptExecutionState(attempt.ExecutionState) || !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
			return promptAttemptReclamationMarker{}, promptAttemptReclaimNotReady("prompt attempt %q is not terminal", attempt.ID)
		}
		attempts = append(attempts, attempt)
		marker.CandidateIDs = append(marker.CandidateIDs, attempt.ID)
		if attempt.SessionUID != "" {
			marker.RelatedExternalEffectAggregateIDs = append(marker.RelatedExternalEffectAggregateIDs, attempt.SessionUID)
		}
	}
	marker.CandidateIDs = normalizePromptAttemptReclamationIDs(marker.CandidateIDs)
	marker.RelatedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDs(append(marker.RelatedExternalEffectAggregateIDs, append([]string{request.TaskUID}, request.RelatedExternalEffectAggregateIDs...)...))
	finalAttempt, err := newestPromptAttemptForReclamationKube(attempts)
	if err != nil {
		return promptAttemptReclamationMarker{}, err
	}
	marker.FinalPromptAttemptID = finalAttempt.ID
	if request.Mode == store.PromptAttemptReclamationProjected && request.FinalPromptAttemptID != finalAttempt.ID {
		return promptAttemptReclamationMarker{}, store.ConflictErrorf("final prompt attempt %q is not the newest Task attempt %q", request.FinalPromptAttemptID, finalAttempt.ID)
	}

	turns, err := s.verifyPromptAttemptReferencesKube(ctx, marker, candidates)
	if err != nil {
		return promptAttemptReclamationMarker{}, err
	}
	if request.Mode == store.PromptAttemptReclamationProjected {
		if request.TerminalProjectionID == "" {
			return promptAttemptReclamationMarker{}, store.ValidationErrorf("terminal projection ID is required before preparing projected reclamation")
		}
		marker.TerminalProjectionID = request.TerminalProjectionID
		if request.FinalContinuitySession {
			turn := turns[finalAttempt.ID]
			if turn == nil {
				return promptAttemptReclamationMarker{}, promptAttemptReclaimNotReady("final continuity prompt attempt %q has no finalized SessionTurn", finalAttempt.ID)
			}
			marker.FinalSessionTurnID = turn.ID
			marker.TerminalProjectionAggregateKind = sessionTurnAggregateKind
			marker.TerminalProjectionAggregateID = turn.ID
		} else {
			expectedID := store.CanonicalControlID("task-terminal-projection", request.Namespace, request.TaskUID, fmt.Sprint(finalAttempt.Key.Attempt))
			if request.TerminalProjectionID != expectedID {
				return promptAttemptReclamationMarker{}, store.ConflictErrorf("terminal projection %q does not match newest standalone prompt attempt %q", request.TerminalProjectionID, finalAttempt.ID)
			}
			marker.TerminalProjectionAggregateKind = "Task"
			marker.TerminalProjectionAggregateID = request.TaskUID
		}
		payloadDigest, err := s.verifyPromptAttemptTerminalProjectionMarkerKube(ctx, marker)
		if err != nil {
			return promptAttemptReclamationMarker{}, err
		}
		marker.TerminalProjectionPayloadDigest = payloadDigest
	}
	return marker, nil
}

func newestPromptAttemptForReclamationKube(attempts []store.PromptAttempt) (store.PromptAttempt, error) {
	if len(attempts) == 0 {
		return store.PromptAttempt{}, store.ErrNotFound
	}
	sort.Slice(attempts, func(i, j int) bool {
		if attempts[i].Key.Attempt != attempts[j].Key.Attempt {
			return attempts[i].Key.Attempt > attempts[j].Key.Attempt
		}
		return attempts[i].ID < attempts[j].ID
	})
	if len(attempts) > 1 && attempts[0].Key.Attempt == attempts[1].Key.Attempt {
		return store.PromptAttempt{}, store.ConflictErrorf("Task %q has multiple PromptAttempts at newest attempt %d", attempts[0].Key.TaskUID, attempts[0].Key.Attempt)
	}
	return attempts[0], nil
}

func (s *Store) verifyPreparedPromptAttemptReclamationKube(
	ctx context.Context,
	marker promptAttemptReclamationMarker,
	candidates []*corev1alpha1.PromptAttempt,
) error {
	authorized := make(map[string]struct{}, len(marker.CandidateIDs))
	for _, id := range marker.CandidateIDs {
		authorized[id] = struct{}{}
	}
	for _, object := range candidates {
		if _, ok := authorized[object.Spec.ID]; !ok {
			return store.ConflictErrorf("PromptAttempt %q was created after Task reclamation was prepared", object.Spec.ID)
		}
		attempt := promptAttemptFromObject(object)
		if !store.IsTerminalPromptExecutionState(attempt.ExecutionState) || !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
			return promptAttemptReclaimNotReady("prompt attempt %q is not terminal", attempt.ID)
		}
	}
	if marker.Mode == store.PromptAttemptReclamationNoAttempt && len(candidates) != 0 {
		return store.ConflictErrorf("no-attempt reclamation marker now has PromptAttempt candidates")
	}
	if _, err := s.verifyPromptAttemptReferencesKube(ctx, marker, candidates); err != nil {
		return err
	}
	if marker.Mode == store.PromptAttemptReclamationProjected {
		_, err := s.verifyPromptAttemptTerminalProjectionMarkerKube(ctx, marker)
		return err
	}
	return nil
}

func (s *Store) verifyPromptAttemptReferencesKube(
	ctx context.Context,
	marker promptAttemptReclamationMarker,
	candidates []*corev1alpha1.PromptAttempt,
) (map[string]*store.SessionTurn, error) {
	protected := make(map[string]struct{}, len(marker.CandidateIDs))
	for _, id := range marker.CandidateIDs {
		protected[id] = struct{}{}
	}
	controls := &corev1alpha1.RuntimeSessionControlList{}
	if err := s.readClient().List(ctx, controls, client.InNamespace(marker.Namespace)); err != nil {
		return nil, mapKubernetesError("list Session controls for prompt attempt reclamation", err)
	}
	for i := range controls.Items {
		control := &controls.Items[i]
		if control.Status.MutationLease != nil && control.Status.MutationLease.TaskUID == marker.TaskUID {
			return nil, promptAttemptReclaimNotReady("Session %s/%s still has a mutation lease for Task %q", control.Namespace, control.Spec.SessionName, marker.TaskUID)
		}
		if _, related := protected[control.Status.RelatedPromptAttemptID]; related {
			return nil, promptAttemptReclaimNotReady("Session %s/%s still references prompt attempt %q", control.Namespace, control.Spec.SessionName, control.Status.RelatedPromptAttemptID)
		}
	}

	turns := make(map[string]*store.SessionTurn)
	if marker.ContinuitySession {
		for _, object := range candidates {
			attempt := promptAttemptFromObject(object)
			if attempt.SessionUID == "" && attempt.SessionLeaseGeneration == 0 {
				continue
			}
			if attempt.SessionUID == "" || attempt.SessionLeaseGeneration < 1 {
				return nil, store.ConflictErrorf("prompt attempt %q has an incomplete SessionTurn binding", attempt.ID)
			}
			if s.sessionTurns == nil {
				return nil, ErrSessionTurnStoreNotConfigured
			}
			key := store.SessionTurnKey{
				SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration,
				TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
			}
			turnID, err := key.CanonicalID()
			if err != nil {
				return nil, err
			}
			turn, err := s.sessionTurns.GetSessionTurn(ctx, turnID)
			if errors.Is(err, store.ErrNotFound) {
				return nil, promptAttemptReclaimNotReady("SessionTurn %q for prompt attempt %q is not durable", turnID, attempt.ID)
			}
			if err != nil {
				return nil, fmt.Errorf("load SessionTurn for prompt attempt reclamation: %w", err)
			}
			if turn.Key != key || turn.PromptAttemptID != attempt.ID {
				return nil, store.ConflictErrorf("SessionTurn %q does not match prompt attempt %q", turnID, attempt.ID)
			}
			if turn.State != store.SessionTurnFinalized || turn.FinalizedAt == nil || strings.TrimSpace(turn.ProjectionID) == "" {
				return nil, promptAttemptReclaimNotReady("SessionTurn %q for prompt attempt %q is not finalized", turnID, attempt.ID)
			}
			turns[attempt.ID] = turn
		}
	}
	if err := s.verifyPromptAttemptPublicationsAndEffectsKube(ctx, marker); err != nil {
		return nil, err
	}
	return turns, nil
}

func (s *Store) verifyPromptAttemptPublicationsAndEffectsKube(ctx context.Context, marker promptAttemptReclamationMarker) error {
	publications := &corev1alpha1.PublicationList{}
	if err := s.readClient().List(ctx, publications, client.InNamespace(marker.Namespace)); err != nil {
		return mapKubernetesError("list publications for prompt attempt reclamation", err)
	}
	relatedEffects := make(map[string]struct{}, len(marker.RelatedExternalEffectAggregateIDs))
	relatedEffects[marker.TaskUID] = struct{}{}
	for _, id := range marker.RelatedExternalEffectAggregateIDs {
		relatedEffects[id] = struct{}{}
	}
	for i := range publications.Items {
		publication := &publications.Items[i]
		if publication.Spec.TaskUID != marker.TaskUID {
			continue
		}
		if !store.IsTerminalPublicationState(store.PublicationState(publication.Status.State)) {
			return promptAttemptReclaimNotReady("publication %q is not terminal", publication.Spec.ID)
		}
		relatedEffects[publication.Spec.ID] = struct{}{}
	}
	effects := &corev1alpha1.ExternalEffectList{}
	if err := s.readClient().List(ctx, effects, client.InNamespace(marker.Namespace)); err != nil {
		return mapKubernetesError("list external effects for prompt attempt reclamation", err)
	}
	for i := range effects.Items {
		effect := &effects.Items[i]
		if _, related := relatedEffects[effect.Spec.AggregateID]; !related {
			continue
		}
		switch store.ExternalEffectState(effect.Status.State) {
		case store.ExternalEffectSucceeded, store.ExternalEffectFailed, store.ExternalEffectOutcomeUnknown:
		default:
			return promptAttemptReclaimNotReady("external effect %q is not terminal", effect.Spec.ID)
		}
	}
	return nil
}

func (s *Store) verifyPromptAttemptTerminalProjectionMarkerKube(ctx context.Context, marker promptAttemptReclamationMarker) (string, error) {
	if s.outbox == nil {
		return "", ErrOutboxStoreNotConfigured
	}
	projection, err := s.outbox.GetOutboxProjection(ctx, marker.TerminalProjectionID)
	if errors.Is(err, store.ErrNotFound) {
		return "", promptAttemptReclaimNotReady("terminal projection %q is not durable", marker.TerminalProjectionID)
	}
	if err != nil {
		return "", fmt.Errorf("load terminal projection for prompt attempt reclamation: %w", err)
	}
	if projection.State != store.OutboxProjectionDelivered {
		return "", promptAttemptReclaimNotReady("terminal projection %q is not delivered", projection.ID)
	}
	if projection.ProjectionKind != "TaskTerminalStatus" ||
		projection.AggregateKind != marker.TerminalProjectionAggregateKind || projection.AggregateID != marker.TerminalProjectionAggregateID {
		return "", store.ConflictErrorf("terminal projection %q does not match the prepared Task finalization aggregate", projection.ID)
	}
	payloadDigest := store.CanonicalBytesDigest(projection.Payload)
	if projection.PayloadDigest != payloadDigest {
		return "", store.ConflictErrorf("terminal projection %q payload does not match its digest", projection.ID)
	}
	if marker.TerminalProjectionPayloadDigest != "" && marker.TerminalProjectionPayloadDigest != payloadDigest {
		return "", store.ConflictErrorf("terminal projection %q changed after prompt attempt reclamation was prepared", projection.ID)
	}
	var finalTurn *store.SessionTurn
	if marker.FinalSessionTurnID != "" {
		if s.sessionTurns == nil {
			return "", ErrSessionTurnStoreNotConfigured
		}
		turn, err := s.sessionTurns.GetSessionTurn(ctx, marker.FinalSessionTurnID)
		if err != nil {
			return "", fmt.Errorf("load final SessionTurn for prompt attempt reclamation: %w", err)
		}
		if turn.State != store.SessionTurnFinalized || turn.FinalizedAt == nil || turn.ProjectionID != projection.ID {
			return "", store.ConflictErrorf("terminal projection %q does not match final SessionTurn %q", projection.ID, marker.FinalSessionTurnID)
		}
		finalTurn = turn
	}
	task := &corev1alpha1.Task{}
	if err := s.readClient().Get(ctx, client.ObjectKey{Namespace: marker.Namespace, Name: marker.TaskName}, task); err != nil {
		return "", mapKubernetesError("load Task for terminal projection validation", err)
	}
	attemptMissing := false
	attempt, err := s.GetPromptAttempt(ctx, marker.FinalPromptAttemptID)
	if errors.Is(err, store.ErrNotFound) {
		if marker.TerminalProjectionPayloadDigest != payloadDigest {
			return "", promptAttemptReclaimNotReady("final prompt attempt %q is not durable", marker.FinalPromptAttemptID)
		}
		attemptMissing = true
	}
	if err != nil && !attemptMissing {
		return "", fmt.Errorf("load final prompt attempt for terminal projection validation: %w", err)
	}
	if !attemptMissing {
		var validationErr error
		if finalTurn != nil {
			_, validationErr = taskterminal.ValidateFinalizedSessionProjection(
				projection.Payload, task, marker.TaskUID, attempt, finalTurn,
			)
		} else {
			_, validationErr = taskterminal.ValidateRestoredProjection(projection.Payload, task, marker.TaskUID, attempt)
		}
		if validationErr != nil {
			return "", validationErr
		}
	}
	return payloadDigest, nil
}

func promptAttemptReclaimNotReady(format string, args ...any) error {
	return fmt.Errorf("%w: %s", store.ErrNotReady, fmt.Sprintf(format, args...))
}

// RecoverPromptAttemptPreSubmission refreshes Reserved or returns SessionStarting/Planned to
// Reserved after an unambiguous no-write recovery decision.
func (s *Store) RecoverPromptAttemptPreSubmission(ctx context.Context, recovery store.PromptAttemptPreSubmissionRecovery) (*store.PromptAttempt, error) {
	recovery.ID = strings.TrimSpace(recovery.ID)
	if err := store.ValidateControlIdentifier("prompt attempt ID", recovery.ID); err != nil {
		return nil, err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, recovery.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	recovery.Fence = fence
	if recovery.ExpectedVersion < 1 {
		return nil, store.ValidationErrorf("prompt attempt expected version must be at least 1")
	}
	if recovery.ExpectedState != store.PromptExecutionReserved &&
		recovery.ExpectedState != store.PromptExecutionSessionStarting && recovery.ExpectedState != store.PromptExecutionPlanned {
		return nil, store.ValidationErrorf("only Reserved, SessionStarting, or Planned attempts may be recovered before submission")
	}
	recovery.OperationID = strings.TrimSpace(recovery.OperationID)
	if err := store.ValidateControlIdentifier("prompt attempt recovery operation ID", recovery.OperationID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("prompt attempt recovery operation digest", recovery.OperationDigest); err != nil {
		return nil, err
	}
	recovery.RecoveredAt = store.NormalizeControlTime(recovery.RecoveredAt)

	object, err := s.findPromptAttemptByID(ctx, recovery.ID)
	if err != nil {
		return nil, err
	}
	attempt := promptAttemptFromObject(object)
	if attempt.LastOperationID == recovery.OperationID {
		if attempt.LastOperationDigest != recovery.OperationDigest {
			return nil, store.ConflictErrorf("prompt attempt recovery operation %q was reused with a different digest", recovery.OperationID)
		}
		if attempt.ExecutionState == store.PromptExecutionReserved {
			return &attempt, nil
		}
		return nil, store.ConflictErrorf("prompt attempt recovery operation %q was already applied with a different state", recovery.OperationID)
	}
	if attempt.Version != recovery.ExpectedVersion || attempt.ExecutionState != recovery.ExpectedState {
		return nil, store.ConflictErrorf("prompt attempt %q no longer matches pre-submission recovery version/state", attempt.ID)
	}

	updated := object.DeepCopy()
	updated.Status.ExecutionState = corev1alpha1.PromptAttemptExecutionState(store.PromptExecutionReserved)
	updated.Status.RuntimeInstanceID = ""
	updated.Status.SessionUID = ""
	updated.Status.SessionLeaseGeneration = 0
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, attempt.Version+1, recovery.OperationID, recovery.OperationDigest, attempt.CreatedAt, recovery.RecoveredAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return nil, mapKubernetesError("recover prompt attempt before submission", err)
	}
	result := promptAttemptFromObject(updated)
	return &result, nil
}

// TransitionPromptAttemptExecution applies exact domain-version, state,
// immutable binding, resourceVersion, and controller-epoch fences.
func (s *Store) TransitionPromptAttemptExecution(ctx context.Context, transition store.PromptAttemptExecutionTransition) (*store.PromptAttempt, error) {
	transition.ID = strings.TrimSpace(transition.ID)
	if err := store.ValidateControlIdentifier("prompt attempt ID", transition.ID); err != nil {
		return nil, err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, transition.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	transition.Fence = fence
	if err := validatePromptExecutionTransition(&transition); err != nil {
		return nil, err
	}

	object, err := s.findPromptAttemptByID(ctx, transition.ID)
	if err != nil {
		return nil, err
	}
	attempt := promptAttemptFromObject(object)
	if attempt.LastOperationID == transition.OperationID {
		if attempt.LastOperationDigest != transition.OperationDigest {
			return nil, store.ConflictErrorf("prompt attempt operation %q was reused with a different digest", transition.OperationID)
		}
		if attempt.ExecutionState == transition.NewState {
			return &attempt, nil
		}
		return nil, store.ConflictErrorf("prompt attempt operation %q was already applied to state %s", transition.OperationID, attempt.ExecutionState)
	}
	if attempt.Version != transition.ExpectedVersion || attempt.ExecutionState != transition.ExpectedState {
		return nil, store.ConflictErrorf("prompt attempt %q is version %d state %s, expected version %d state %s", attempt.ID, attempt.Version, attempt.ExecutionState, transition.ExpectedVersion, transition.ExpectedState)
	}
	if transition.RuntimeInstanceID != "" && attempt.RuntimeInstanceID != "" && attempt.RuntimeInstanceID != transition.RuntimeInstanceID {
		return nil, store.ConflictErrorf("prompt attempt %q runtime instance is immutable once set", attempt.ID)
	}
	if transition.SessionUID != "" && attempt.SessionUID != "" && attempt.SessionUID != transition.SessionUID {
		return nil, store.ConflictErrorf("prompt attempt %q session UID is immutable once set", attempt.ID)
	}
	if transition.SessionLeaseGeneration > 0 && attempt.SessionLeaseGeneration > 0 && attempt.SessionLeaseGeneration != transition.SessionLeaseGeneration {
		return nil, store.ConflictErrorf("prompt attempt %q session lease generation is immutable once set", attempt.ID)
	}

	updated := object.DeepCopy()
	updated.Status.ExecutionState = corev1alpha1.PromptAttemptExecutionState(transition.NewState)
	if transition.RuntimeInstanceID != "" {
		updated.Status.RuntimeInstanceID = transition.RuntimeInstanceID
	}
	if transition.SessionUID != "" {
		updated.Status.SessionUID = transition.SessionUID
	}
	if transition.SessionLeaseGeneration > 0 {
		updated.Status.SessionLeaseGeneration = transition.SessionLeaseGeneration
	}
	if transition.TerminalReason != "" || store.IsTerminalPromptExecutionState(transition.NewState) {
		updated.Status.TerminalReason = transition.TerminalReason
	}
	if transition.OutcomeMarker != "" {
		updated.Status.OutcomeMarker = transition.OutcomeMarker
	}
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, attempt.Version+1, transition.OperationID, transition.OperationDigest, attempt.CreatedAt, transition.UpdatedAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return nil, mapKubernetesError("transition prompt attempt execution", err)
	}
	result := promptAttemptFromObject(updated)
	return &result, nil
}

// TransitionPromptAttemptDelivery applies an exact delivery-state CAS.
func (s *Store) TransitionPromptAttemptDelivery(ctx context.Context, transition store.PromptAttemptDeliveryTransition) (*store.PromptAttempt, error) {
	transition.ID = strings.TrimSpace(transition.ID)
	if err := store.ValidateControlIdentifier("prompt attempt ID", transition.ID); err != nil {
		return nil, err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, transition.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	transition.Fence = fence
	if transition.ExpectedVersion < 1 {
		return nil, store.ValidationErrorf("prompt attempt expected version must be at least 1")
	}
	if err := store.ValidatePromptDeliveryTransition(transition.ExpectedState, transition.NewState); err != nil {
		return nil, err
	}
	transition.OperationID = strings.TrimSpace(transition.OperationID)
	if err := store.ValidateControlIdentifier("prompt attempt operation ID", transition.OperationID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("prompt attempt operation digest", transition.OperationDigest); err != nil {
		return nil, err
	}
	if err := store.ValidateControlReason("prompt delivery terminal reason", transition.TerminalReason); err != nil {
		return nil, err
	}
	transition.UpdatedAt = store.NormalizeControlTime(transition.UpdatedAt)

	object, err := s.findPromptAttemptByID(ctx, transition.ID)
	if err != nil {
		return nil, err
	}
	attempt := promptAttemptFromObject(object)
	if attempt.LastOperationID == transition.OperationID {
		if attempt.LastOperationDigest != transition.OperationDigest {
			return nil, store.ConflictErrorf("prompt attempt operation %q was reused with a different digest", transition.OperationID)
		}
		if attempt.DeliveryState == transition.NewState {
			return &attempt, nil
		}
		return nil, store.ConflictErrorf("prompt attempt operation %q was already applied to delivery state %s", transition.OperationID, attempt.DeliveryState)
	}
	if attempt.Version != transition.ExpectedVersion || attempt.DeliveryState != transition.ExpectedState {
		return nil, store.ConflictErrorf("prompt attempt %q is version %d delivery state %s, expected version %d state %s", attempt.ID, attempt.Version, attempt.DeliveryState, transition.ExpectedVersion, transition.ExpectedState)
	}

	updated := object.DeepCopy()
	updated.Status.DeliveryState = corev1alpha1.PromptAttemptDeliveryState(transition.NewState)
	if transition.TerminalReason != "" || store.IsTerminalPromptDeliveryState(transition.NewState) {
		updated.Status.TerminalReason = transition.TerminalReason
	}
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, attempt.Version+1, transition.OperationID, transition.OperationDigest, attempt.CreatedAt, transition.UpdatedAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return nil, mapKubernetesError("transition prompt attempt delivery", err)
	}
	result := promptAttemptFromObject(updated)
	return &result, nil
}

func (s *Store) completePromptAttemptCreation(ctx context.Context, object *corev1alpha1.PromptAttempt, normalized store.PromptAttempt, fence store.ControllerEpochFence, snapshot epochSnapshot) (*store.PromptAttempt, error) {
	if !samePromptAttemptSpec(object, normalized) {
		return nil, store.ConflictErrorf("prompt attempt %q was reused with a different request digest or immutable identity", normalized.ID)
	}
	if object.Status.Version > 0 {
		existing := promptAttemptFromObject(object)
		return &existing, nil
	}
	updated := object.DeepCopy()
	updated.Status.SessionUID = normalized.SessionUID
	updated.Status.SessionLeaseGeneration = normalized.SessionLeaseGeneration
	updated.Status.RuntimeInstanceID = normalized.RuntimeInstanceID
	updated.Status.ExecutionState = corev1alpha1.PromptAttemptExecutionState(normalized.ExecutionState)
	updated.Status.DeliveryState = corev1alpha1.PromptAttemptDeliveryState(normalized.DeliveryState)
	updated.Status.TerminalReason = normalized.TerminalReason
	updated.Status.OutcomeMarker = normalized.OutcomeMarker
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, 1, "", "", normalized.CreatedAt, normalized.UpdatedAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		if apierrors.IsConflict(err) {
			fresh := &corev1alpha1.PromptAttempt{}
			if getErr := s.readClient().Get(ctx, client.ObjectKeyFromObject(object), fresh); getErr == nil && samePromptAttemptSpec(fresh, normalized) && fresh.Status.Version > 0 {
				result := promptAttemptFromObject(fresh)
				return &result, nil
			}
		}
		return nil, mapKubernetesError("initialize prompt attempt status", err)
	}
	result := promptAttemptFromObject(updated)
	return &result, nil
}

func (s *Store) findPromptAttemptByID(ctx context.Context, id string) (*corev1alpha1.PromptAttempt, error) {
	list := &corev1alpha1.PromptAttemptList{}
	if err := s.readClient().List(ctx, list, s.namespacedListOptions(
		client.MatchingLabels{corev1alpha1.ControlRecordIDHashLabel: dnsDigest(id)},
	)...); err != nil {
		return nil, mapKubernetesError("list prompt attempts", err)
	}
	var match *corev1alpha1.PromptAttempt
	for i := range list.Items {
		if list.Items[i].Spec.ID != id {
			continue
		}
		if match != nil {
			return nil, store.ConflictErrorf("multiple prompt attempts exist for canonical ID %q", id)
		}
		match = list.Items[i].DeepCopy()
	}
	if match == nil {
		return nil, store.ErrNotFound
	}
	return match, nil
}

func validatePromptExecutionTransition(transition *store.PromptAttemptExecutionTransition) error {
	if transition.ExpectedVersion < 1 {
		return store.ValidationErrorf("prompt attempt expected version must be at least 1")
	}
	if err := store.ValidatePromptExecutionTransition(transition.ExpectedState, transition.NewState); err != nil {
		return err
	}
	transition.OperationID = strings.TrimSpace(transition.OperationID)
	if err := store.ValidateControlIdentifier("prompt attempt operation ID", transition.OperationID); err != nil {
		return err
	}
	if err := store.ValidateCanonicalDigest("prompt attempt operation digest", transition.OperationDigest); err != nil {
		return err
	}
	transition.RuntimeInstanceID = strings.TrimSpace(transition.RuntimeInstanceID)
	transition.SessionUID = strings.TrimSpace(transition.SessionUID)
	if transition.SessionLeaseGeneration < 0 {
		return store.ValidationErrorf("session lease generation must not be negative")
	}
	if transition.SessionLeaseGeneration > 0 && transition.SessionUID == "" {
		return store.ValidationErrorf("session UID is required when setting a session lease generation")
	}
	if transition.SessionUID != "" {
		if err := store.ValidateControlIdentifier("session UID", transition.SessionUID); err != nil {
			return err
		}
	}
	if transition.RuntimeInstanceID != "" {
		if err := store.ValidateControlIdentifier("runtime instance ID", transition.RuntimeInstanceID); err != nil {
			return err
		}
	}
	if err := store.ValidateControlReason("prompt attempt terminal reason", transition.TerminalReason); err != nil {
		return err
	}
	if err := store.ValidateControlReason("prompt attempt outcome marker", transition.OutcomeMarker); err != nil {
		return err
	}
	if transition.NewState == store.PromptExecutionOutcomeUnknown && strings.TrimSpace(transition.OutcomeMarker) == "" {
		return store.ValidationErrorf("OutcomeUnknown requires an explicit outcome marker")
	}
	transition.UpdatedAt = store.NormalizeControlTime(transition.UpdatedAt)
	return nil
}

func samePromptAttemptSpec(object *corev1alpha1.PromptAttempt, attempt store.PromptAttempt) bool {
	return object.Namespace == attempt.Key.Namespace && object.Spec.ID == attempt.ID && object.Spec.TaskUID == attempt.Key.TaskUID &&
		object.Spec.Attempt == attempt.Key.Attempt && object.Spec.PromptID == attempt.Key.PromptID && object.Spec.RequestDigest == attempt.RequestDigest &&
		object.Spec.BindingDigest == attempt.BindingDigest && object.Spec.SnapshotDigest == attempt.SnapshotDigest &&
		reflect.DeepEqual(promptCredentialBindingsFromAPI(object.Spec.CredentialBindings), attempt.CredentialBindings)
}

func promptCredentialBindingsToAPI(values []store.PromptCredentialBinding) []corev1alpha1.PromptCredentialBinding {
	if len(values) == 0 {
		return nil
	}
	result := make([]corev1alpha1.PromptCredentialBinding, 0, len(values))
	for _, value := range values {
		result = append(result, corev1alpha1.PromptCredentialBinding{
			Role: string(value.Role), Namespace: value.Namespace, SecretName: value.SecretName,
			SecretKey: value.SecretKey, SecretUID: value.SecretUID, ResourceVersion: value.ResourceVersion,
		})
	}
	return result
}

func promptCredentialBindingsFromAPI(values []corev1alpha1.PromptCredentialBinding) []store.PromptCredentialBinding {
	if len(values) == 0 {
		return nil
	}
	result := make([]store.PromptCredentialBinding, 0, len(values))
	for _, value := range values {
		result = append(result, store.PromptCredentialBinding{
			Role: store.PromptCredentialRole(value.Role), Namespace: value.Namespace, SecretName: value.SecretName,
			SecretKey: value.SecretKey, SecretUID: value.SecretUID, ResourceVersion: value.ResourceVersion,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Role < result[j].Role })
	return result
}

func promptAttemptFromObject(object *corev1alpha1.PromptAttempt) store.PromptAttempt {
	return store.PromptAttempt{
		ID: object.Spec.ID,
		Key: store.PromptAttemptKey{
			Namespace: object.Namespace,
			TaskUID:   object.Spec.TaskUID,
			Attempt:   object.Spec.Attempt,
			PromptID:  object.Spec.PromptID,
		},
		SessionUID:             object.Status.SessionUID,
		SessionLeaseGeneration: object.Status.SessionLeaseGeneration,
		RuntimeInstanceID:      object.Status.RuntimeInstanceID,
		RequestDigest:          object.Spec.RequestDigest,
		BindingDigest:          object.Spec.BindingDigest,
		SnapshotDigest:         object.Spec.SnapshotDigest,
		CredentialBindings:     promptCredentialBindingsFromAPI(object.Spec.CredentialBindings),
		ExecutionState:         store.PromptExecutionState(object.Status.ExecutionState),
		DeliveryState:          store.PromptDeliveryState(object.Status.DeliveryState),
		TerminalReason:         object.Status.TerminalReason,
		OutcomeMarker:          object.Status.OutcomeMarker,
		ControllerEpochName:    object.Status.ControllerEpochName,
		ControllerEpoch:        object.Status.ControllerEpoch,
		LastOperationID:        object.Status.LastOperationID,
		LastOperationDigest:    object.Status.LastOperationDigest,
		Version:                object.Status.Version,
		CreatedAt:              timeValue(object.Status.CreatedAt),
		UpdatedAt:              timeValue(object.Status.UpdatedAt),
	}
}

// normalizePromptAttemptForCreate applies the shared prompt attempt contract
// and additionally freezes role-separated credential bindings, which only the
// Kubernetes store persists.
func normalizePromptAttemptForCreate(attempt *store.PromptAttempt, fence store.ControllerEpochFence) (store.PromptAttempt, store.ControllerEpochFence, error) {
	normalized, normalizedFence, err := store.NormalizePromptAttemptForCreate(attempt, fence)
	if err != nil {
		return store.PromptAttempt{}, store.ControllerEpochFence{}, err
	}
	if err := store.NormalizePromptCredentialBindings(&normalized); err != nil {
		return store.PromptAttempt{}, store.ControllerEpochFence{}, err
	}
	return normalized, normalizedFence, nil
}
