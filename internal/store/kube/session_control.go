package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	sessionControlFieldNamespace = "session namespace"
	sessionControlFieldName      = "session name"
	sessionControlFieldUID       = "session UID"
	prePromptLeaseReleasePrefix  = "release-pre-prompt:"
)

type sessionLeaseState struct {
	Mode          string
	Generation    int64
	TaskUID       string
	Attempt       int64
	PromptID      string
	RequestDigest string
	LineageDigest string
	AcquiredAt    time.Time
	ExpiresAt     *time.Time
	OperationID   string
	OperationHash string
}

// CreateSessionControl creates the immutable Session identity, its empty
// serialization Lease, and the initial status.
func (s *Store) CreateSessionControl(ctx context.Context, control *store.SessionControl, fence store.ControllerEpochFence) (*store.SessionControl, error) {
	if err := s.requireClient(); err != nil {
		return nil, err
	}
	normalized, normalizedFence, err := normalizeSessionControlForCreate(control, fence)
	if err != nil {
		return nil, err
	}
	_, snapshot, err := s.requireControllerEpoch(ctx, normalizedFence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	if pending, pendingErr := s.sessionCleanupPending(ctx, normalized.Namespace, normalized.SessionName); pendingErr != nil {
		return nil, pendingErr
	} else if pending {
		return nil, store.ConflictErrorf("session %s/%s is being deleted", normalized.Namespace, normalized.SessionName)
	}
	if fenced, fenceErr := s.sessionCleanupFencedForUID(ctx, normalized.SessionUID); fenceErr != nil {
		return nil, fenceErr
	} else if fenced {
		return nil, store.ConflictErrorf("Session UID %q is being deleted or was already deleted", normalized.SessionUID)
	}
	if existingByUID, getErr := s.findSessionControlByUID(ctx, normalized.SessionUID); getErr == nil {
		if existingByUID.Namespace != normalized.Namespace || existingByUID.Spec.SessionName != normalized.SessionName {
			return nil, store.ConflictErrorf("Session UID %q is already owned by %s/%s", normalized.SessionUID, existingByUID.Namespace, existingByUID.Spec.SessionName)
		}
		return s.completeSessionControlCreation(ctx, existingByUID, normalized, normalizedFence, snapshot)
	} else if !errors.Is(getErr, store.ErrNotFound) {
		return nil, getErr
	}

	key := client.ObjectKey{Namespace: normalized.Namespace, Name: runtimeSessionObjectName(normalized.SessionName)}
	object := &corev1alpha1.RuntimeSessionControl{}
	err = s.readClient().Get(ctx, key, object)
	if err == nil {
		return s.completeSessionControlCreation(ctx, object, normalized, normalizedFence, snapshot)
	}
	if !apierrors.IsNotFound(err) {
		return nil, mapKubernetesError("get runtime session control", err)
	}
	if s.sessionCleanup != nil {
		if err := s.sessionCleanup.BindSessionCleanupIdentity(ctx, normalized.Namespace, normalized.SessionName, normalized.SessionUID); err != nil {
			return nil, err
		}
	}

	labels := controlLabels(sessionControlLogicalID(normalized.Namespace, normalized.SessionName))
	object = &corev1alpha1.RuntimeSessionControl{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: normalized.Namespace,
			Name:      key.Name,
			Labels:    labels,
		},
		Spec: corev1alpha1.RuntimeSessionControlSpec{
			SessionName:   normalized.SessionName,
			SessionUID:    normalized.SessionUID,
			RequestDigest: normalized.RequestDigest,
			Owner: corev1alpha1.ControlRecordOwner{
				Kind: "Session",
				UID:  normalized.SessionUID,
			},
		},
	}
	if err := s.client.Create(ctx, object); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if getErr := s.readClient().Get(ctx, key, object); getErr != nil {
				return nil, mapKubernetesError("get concurrently created runtime session control", getErr)
			}
			return s.completeSessionControlCreation(ctx, object, normalized, normalizedFence, snapshot)
		}
		return nil, mapKubernetesError("create runtime session control", err)
	}
	return s.completeSessionControlCreation(ctx, object, normalized, normalizedFence, snapshot)
}

// GetSessionControl reads the exact namespaced Session control record.
func (s *Store) GetSessionControl(ctx context.Context, namespace, sessionName string) (*store.SessionControl, error) {
	if err := s.requireClient(); err != nil {
		return nil, err
	}
	namespace = strings.TrimSpace(namespace)
	sessionName = strings.TrimSpace(sessionName)
	if err := store.ValidateControlIdentifier(sessionControlFieldNamespace, namespace); err != nil {
		return nil, err
	}
	if err := store.ValidateControlIdentifier(sessionControlFieldName, sessionName); err != nil {
		return nil, err
	}
	object := &corev1alpha1.RuntimeSessionControl{}
	key := client.ObjectKey{Namespace: namespace, Name: runtimeSessionObjectName(sessionName)}
	if err := s.readClient().Get(ctx, key, object); err != nil {
		return nil, mapKubernetesError("get runtime session control", err)
	}
	if object.Spec.SessionName != sessionName {
		return nil, store.ConflictErrorf("runtime session control %s/%s has a different immutable Session name", namespace, object.Name)
	}
	result := sessionControlFromObject(object)
	return &result, nil
}

// AcquireSessionMutationLease acquires the next monotonic Session lease
// generation by first CAS-updating the coordination Lease and then mirroring
// the exact Lease revision into RuntimeSessionControl status.
func (s *Store) AcquireSessionMutationLease(ctx context.Context, request store.AcquireSessionMutationLeaseRequest) (*store.SessionControl, error) {
	if err := normalizeAcquireSessionMutationLeaseRequest(&request); err != nil {
		return nil, err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, request.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	request.Fence = fence
	if pending, pendingErr := s.sessionCleanupPending(ctx, request.Namespace, request.SessionName); pendingErr != nil {
		return nil, pendingErr
	} else if pending {
		return nil, store.ConflictErrorf("session %s/%s is being deleted", request.Namespace, request.SessionName)
	}

	object, err := s.getSessionControlObject(ctx, request.Namespace, request.SessionName)
	if err != nil {
		return nil, err
	}
	control := sessionControlFromObject(object)
	if control.SessionUID != request.SessionUID {
		return nil, store.ConflictErrorf("session %s/%s UID does not match immutable control record", request.Namespace, request.SessionName)
	}
	if _, err := resolveSessionLineage(control.Lineage, *request.Lineage, request.AcquiredAt); err != nil {
		return nil, err
	}
	if control.Lease != nil && control.Lease.TaskUID == request.TaskUID && control.Lease.Attempt == request.Attempt && control.Lease.PromptID == request.PromptID {
		if control.Lease.RequestDigest != request.RequestDigest {
			return nil, store.ConflictErrorf("session lease identity was reused with a different request digest")
		}
		if err := s.verifyMirroredSessionLease(ctx, object, *control.Lease); err != nil {
			return nil, err
		}
		return s.completeExistingSessionLineage(ctx, object, control, request, fence, snapshot)
	}
	if control.Version != request.ExpectedVersion || control.LeaseGeneration != request.ExpectedLeaseGeneration {
		return nil, store.ConflictErrorf("session %s/%s is version %d lease generation %d, expected version %d generation %d", request.Namespace, request.SessionName, control.Version, control.LeaseGeneration, request.ExpectedVersion, request.ExpectedLeaseGeneration)
	}
	if control.Availability != store.SessionAvailable {
		return nil, store.ConflictErrorf("session %s/%s is reconciliation-blocked: %s", request.Namespace, request.SessionName, control.BlockedReason)
	}
	if control.Lease != nil {
		return nil, store.ConflictErrorf("session %s/%s is already leased by task %s", request.Namespace, request.SessionName, control.Lease.TaskUID)
	}

	lease, leaseState, err := s.getSessionLease(ctx, object.Namespace, object.Spec.SessionName, object.Spec.SessionUID)
	if err != nil {
		return nil, err
	}
	lease, leaseState, err = s.finishPendingPrePromptLeaseRelease(ctx, control, lease, leaseState)
	if err != nil {
		return nil, err
	}
	if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" {
		if sameSessionMutationLease(leaseState, request) && leaseState.Generation == request.ExpectedLeaseGeneration+1 {
			return s.completeSessionLeaseStatus(ctx, object, control, request, fence, snapshot, lease)
		}
		return nil, store.ConflictErrorf("session %s/%s coordination Lease is already held", request.Namespace, request.SessionName)
	}
	if leaseState.Generation != request.ExpectedLeaseGeneration {
		return nil, store.ConflictErrorf("session %s/%s Lease generation %d does not match expected %d", request.Namespace, request.SessionName, leaseState.Generation, request.ExpectedLeaseGeneration)
	}

	updatedLease := lease.DeepCopy()
	setSessionMutationLease(updatedLease, request, request.ExpectedLeaseGeneration+1)
	if err := s.client.Update(ctx, updatedLease); err != nil {
		return nil, mapKubernetesError("acquire Session mutation Lease", err)
	}
	return s.completeSessionLeaseStatus(ctx, object, control, request, fence, snapshot, updatedLease)
}

// CommitSessionRuntimeGeneration records the newest provider RuntimeSession
// generation proven live under the exact active Session lease.
func (s *Store) CommitSessionRuntimeGeneration(ctx context.Context, request store.CommitSessionRuntimeGenerationRequest) (*store.SessionControl, error) {
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.SessionName = strings.TrimSpace(request.SessionName)
	request.SessionUID = strings.TrimSpace(request.SessionUID)
	for field, value := range map[string]string{
		sessionControlFieldNamespace: request.Namespace,
		sessionControlFieldName:      request.SessionName,
		sessionControlFieldUID:       request.SessionUID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return nil, err
		}
	}
	if err := request.Key.Validate(); err != nil {
		return nil, err
	}
	if request.Key.SessionUID != request.SessionUID || request.ExpectedSessionVersion < 1 || request.Generation < 1 {
		return nil, store.ValidationErrorf("Session RuntimeSession generation commit fence is invalid")
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, request.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	committedAt := store.NormalizeControlTime(request.CommittedAt)

	object, err := s.getSessionControlObject(ctx, request.Namespace, request.SessionName)
	if err != nil {
		return nil, err
	}
	control := sessionControlFromObject(object)
	if control.SessionUID != request.SessionUID || control.Availability != store.SessionAvailable ||
		!sessionLeaseMatchesKey(control.Lease, request.Key) {
		return nil, store.ConflictErrorf("session %s/%s no longer matches the active RuntimeSession generation commit fence", request.Namespace, request.SessionName)
	}
	if err := s.verifyMirroredSessionLease(ctx, object, *control.Lease); err != nil {
		return nil, err
	}
	if control.RuntimeSessionGeneration > request.Generation {
		return nil, store.ConflictErrorf("session %s/%s RuntimeSession generation is %d, not %d", request.Namespace, request.SessionName, control.RuntimeSessionGeneration, request.Generation)
	}
	if control.RuntimeSessionGeneration == request.Generation {
		return &control, nil
	}
	if control.Version != request.ExpectedSessionVersion {
		return nil, store.ConflictErrorf("session %s/%s is version %d, expected %d", request.Namespace, request.SessionName, control.Version, request.ExpectedSessionVersion)
	}

	updated := object.DeepCopy()
	updated.Status.Generation = request.Generation
	setMutationStatus(
		&updated.Status.ControlRecordMutationStatus,
		fence,
		snapshot,
		control.Version+1,
		control.LastOperationID,
		control.LastOperationDigest,
		control.CreatedAt,
		committedAt,
	)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return nil, mapKubernetesError("commit Session RuntimeSession generation", err)
	}
	result := sessionControlFromObject(updated)
	return &result, nil
}

// ReleaseSessionMutationLease aborts an exact pre-prompt lease after the
// caller has independently established that no SessionTurn was persisted.
// SessionControl commits first; retries then finish the coordination-Lease
// clear by matching the durable release operation.
func (s *Store) ReleaseSessionMutationLease(ctx context.Context, request store.ReleaseSessionMutationLeaseRequest) (*store.SessionControl, error) {
	if err := normalizeReleaseSessionMutationLeaseRequest(&request); err != nil {
		return nil, err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, request.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	request.Fence = fence
	if pending, pendingErr := s.sessionCleanupPending(ctx, request.Namespace, request.SessionName); pendingErr != nil {
		return nil, pendingErr
	} else if pending {
		return nil, store.ConflictErrorf("session %s/%s is being deleted", request.Namespace, request.SessionName)
	}

	object, err := s.getSessionControlObject(ctx, request.Namespace, request.SessionName)
	if err != nil {
		return nil, err
	}
	control := sessionControlFromObject(object)
	if control.SessionUID != request.SessionUID {
		return nil, store.ConflictErrorf("session %s/%s UID does not match immutable control record", request.Namespace, request.SessionName)
	}
	if control.LastOperationID == request.OperationID {
		if control.LastOperationDigest != request.OperationDigest || control.Lease != nil {
			return nil, store.ConflictErrorf("session lease release operation %q was already applied with different target values", request.OperationID)
		}
	} else {
		if control.Version != request.ExpectedSessionVersion || !sessionLeaseMatchesKey(control.Lease, request.Key) || control.Lease.RequestDigest != request.LeaseRequestDigest {
			return nil, store.ConflictErrorf("session %s/%s no longer matches the pre-prompt lease release fence", request.Namespace, request.SessionName)
		}
		if err := s.verifyMirroredSessionLease(ctx, object, *control.Lease); err != nil {
			return nil, err
		}
		updated := object.DeepCopy()
		updated.Status.MutationLease = nil
		setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, control.Version+1, request.OperationID, request.OperationDigest, control.CreatedAt, request.ReleasedAt)
		if err := s.client.Status().Update(ctx, updated); err != nil {
			return nil, mapKubernetesError("release pre-prompt Session mutation Lease status", err)
		}
		object = updated
		control = sessionControlFromObject(updated)
	}
	if err := s.releaseFinalizedSessionLease(ctx, object, request.Key, request.LeaseRequestDigest); err != nil {
		return nil, err
	}
	return &control, nil
}

func (s *Store) finishPendingPrePromptLeaseRelease(ctx context.Context, control store.SessionControl, lease *coordinationv1.Lease, state sessionLeaseState) (*coordinationv1.Lease, sessionLeaseState, error) {
	if control.Lease != nil || !strings.HasPrefix(control.LastOperationID, prePromptLeaseReleasePrefix) || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		return lease, state, nil
	}
	key := store.SessionTurnKey{
		SessionUID: control.SessionUID, LeaseGeneration: state.Generation,
		TaskUID: state.TaskUID, Attempt: state.Attempt, PromptID: state.PromptID,
	}
	turnID, err := key.CanonicalID()
	if err != nil || control.LastOperationID != prePromptLeaseReleasePrefix+turnID || state.Mode != leaseModeMutation {
		return nil, sessionLeaseState{}, store.ConflictErrorf("pending pre-prompt Session lease release does not match the authoritative Lease")
	}
	expectedDigest, err := store.SessionLeaseReleaseOperationDigest(turnID, state.RequestDigest)
	if err != nil || control.LastOperationDigest != expectedDigest {
		return nil, sessionLeaseState{}, store.ConflictErrorf("pending pre-prompt Session lease release digest does not match the authoritative Lease")
	}
	updated := lease.DeepCopy()
	clearSessionLease(updated, state.Generation)
	if err := s.client.Update(ctx, updated); err != nil {
		return nil, sessionLeaseState{}, mapKubernetesError("finish pre-prompt Session Lease release", err)
	}
	fresh, freshState, err := s.getSessionLease(ctx, control.Namespace, control.SessionName, control.SessionUID)
	return fresh, freshState, err
}

// ReconcileSessionControl establishes the independently observed baseline and
// returns both SessionControl and BranchClaim to Available. A Session Lease is
// held in reconciliation mode while the two status CAS operations complete;
// retries finish any partially committed same-operation reconciliation.
func (s *Store) ReconcileSessionControl(ctx context.Context, request store.ReconcileSessionControlRequest) (*store.SessionControl, error) {
	if err := normalizeReconcileSessionControlRequest(&request); err != nil {
		return nil, err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, request.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	request.Fence = fence
	if pending, pendingErr := s.sessionCleanupPending(ctx, request.Namespace, request.SessionName); pendingErr != nil {
		return nil, pendingErr
	} else if pending {
		return nil, store.ConflictErrorf("session %s/%s is being deleted", request.Namespace, request.SessionName)
	}

	object, err := s.getSessionControlObject(ctx, request.Namespace, request.SessionName)
	if err != nil {
		return nil, err
	}
	control := sessionControlFromObject(object)
	alreadyCommitted, err := sessionReconciliationAlreadyCommitted(control, request)
	if err != nil {
		return nil, err
	}
	if alreadyCommitted {
		return s.finishCommittedSessionReconciliation(ctx, object, control, request, fence, snapshot)
	}
	if err := validateBlockedSessionReconciliationFence(control, request); err != nil {
		return nil, err
	}
	if err := s.acquireSessionReconciliationLease(ctx, object, request); err != nil {
		return nil, err
	}
	if err := s.ensureBranchReconciliationApplied(ctx, request, fence, snapshot); err != nil {
		return nil, err
	}
	fresh, freshControl, err := s.commitSessionReconciliationStatus(ctx, request, fence, snapshot)
	if err != nil {
		return nil, err
	}
	if err := s.clearReconciliationSessionLease(ctx, fresh, request); err != nil {
		return nil, err
	}
	return &freshControl, nil
}

func sessionReconciliationAlreadyCommitted(control store.SessionControl, request store.ReconcileSessionControlRequest) (bool, error) {
	if control.LastOperationID != request.OperationID {
		return false, nil
	}
	if control.LastOperationDigest != request.OperationDigest {
		return false, store.ConflictErrorf("session reconciliation operation %q was reused with a different digest", request.OperationID)
	}
	if control.Availability != store.SessionAvailable || control.Lease != nil || control.VerifiedBaseline == nil || !reflect.DeepEqual(*control.VerifiedBaseline, request.VerifiedBaseline) {
		return false, store.ConflictErrorf("session reconciliation operation %q was already applied with different target values", request.OperationID)
	}
	return true, nil
}

func validateBlockedSessionReconciliationFence(control store.SessionControl, request store.ReconcileSessionControlRequest) error {
	if control.SessionUID != request.SessionUID || control.Version != request.ExpectedVersion ||
		control.LeaseGeneration != request.ExpectedLeaseGeneration || control.Lease != nil ||
		control.Availability != store.SessionReconciliationBlocked ||
		control.RelatedPublicationID != request.ExpectedRelatedPublicationID {
		return store.ConflictErrorf("session %s/%s no longer matches the blocked reconciliation fence", request.Namespace, request.SessionName)
	}
	return nil
}

func (s *Store) finishCommittedSessionReconciliation(ctx context.Context, object *corev1alpha1.RuntimeSessionControl, control store.SessionControl, request store.ReconcileSessionControlRequest, fence store.ControllerEpochFence, snapshot epochSnapshot) (*store.SessionControl, error) {
	if err := s.ensureBranchReconciliationApplied(ctx, request, fence, snapshot); err != nil {
		return nil, err
	}
	if err := s.clearReconciliationSessionLease(ctx, object, request); err != nil {
		return nil, err
	}
	return &control, nil
}

func (s *Store) acquireSessionReconciliationLease(ctx context.Context, object *corev1alpha1.RuntimeSessionControl, request store.ReconcileSessionControlRequest) error {
	lease, leaseState, err := s.getSessionLease(ctx, object.Namespace, object.Spec.SessionName, object.Spec.SessionUID)
	if err != nil {
		return err
	}
	if leaseState.Generation != request.ExpectedLeaseGeneration {
		return store.ConflictErrorf("session reconciliation Lease generation no longer matches")
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		updatedLease := lease.DeepCopy()
		setSessionReconciliationLease(updatedLease, request)
		if err := s.client.Update(ctx, updatedLease); err != nil {
			return mapKubernetesError("acquire Session reconciliation Lease", err)
		}
		return nil
	}
	if leaseState.Mode != leaseModeReconciliation || leaseState.OperationID != request.OperationID || leaseState.OperationHash != request.OperationDigest {
		return store.ConflictErrorf("session %s/%s coordination Lease is held by another operation", request.Namespace, request.SessionName)
	}
	return nil
}

func (s *Store) commitSessionReconciliationStatus(ctx context.Context, request store.ReconcileSessionControlRequest, fence store.ControllerEpochFence, snapshot epochSnapshot) (*corev1alpha1.RuntimeSessionControl, store.SessionControl, error) {
	fresh, err := s.getSessionControlObject(ctx, request.Namespace, request.SessionName)
	if err != nil {
		return nil, store.SessionControl{}, err
	}
	freshControl := sessionControlFromObject(fresh)
	alreadyCommitted, err := sessionReconciliationAlreadyCommitted(freshControl, request)
	if err != nil {
		return nil, store.SessionControl{}, err
	}
	if alreadyCommitted {
		return fresh, freshControl, nil
	}
	if err := validateBlockedSessionReconciliationFence(freshControl, request); err != nil {
		return nil, store.SessionControl{}, err
	}
	updated := fresh.DeepCopy()
	updated.Status.Availability = corev1alpha1.RuntimeSessionControlAvailability(store.SessionAvailable)
	updated.Status.BlockedReason = ""
	updated.Status.RelatedPromptAttemptID = ""
	updated.Status.RelatedPublicationID = ""
	updated.Status.VerifiedBaseline = verifiedBaselineToAPI(&request.VerifiedBaseline)
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, freshControl.Version+1, request.OperationID, request.OperationDigest, freshControl.CreatedAt, request.ReconciledAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return nil, store.SessionControl{}, mapKubernetesError("reconcile runtime session control", err)
	}
	return updated, sessionControlFromObject(updated), nil
}

func (s *Store) completeSessionControlCreation(ctx context.Context, object *corev1alpha1.RuntimeSessionControl, normalized store.SessionControl, fence store.ControllerEpochFence, snapshot epochSnapshot) (*store.SessionControl, error) {
	if !sameSessionControlSpec(object, normalized) {
		return nil, store.ConflictErrorf("session control %s/%s was reused with a different UID or request digest", normalized.Namespace, normalized.SessionName)
	}
	_, err := s.ensureSessionLease(ctx, object.Namespace, object.Spec.SessionName, object.Spec.SessionUID, normalized.LeaseGeneration)
	if err != nil {
		return nil, err
	}
	if object.Status.Version > 0 {
		existing := sessionControlFromObject(object)
		return &existing, nil
	}
	updated := object.DeepCopy()
	updated.Status.Availability = corev1alpha1.RuntimeSessionControlAvailability(normalized.Availability)
	updated.Status.MutationLeaseGeneration = normalized.LeaseGeneration
	updated.Status.BlockedReason = normalized.BlockedReason
	updated.Status.RelatedPromptAttemptID = normalized.RelatedPromptAttemptID
	updated.Status.RelatedPublicationID = normalized.RelatedPublicationID
	updated.Status.VerifiedBaseline = verifiedBaselineToAPI(normalized.VerifiedBaseline)
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, 1, "", "", normalized.CreatedAt, normalized.UpdatedAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		if apierrors.IsConflict(err) {
			fresh, getErr := s.getSessionControlObject(ctx, normalized.Namespace, normalized.SessionName)
			if getErr == nil && sameSessionControlSpec(fresh, normalized) && fresh.Status.Version > 0 {
				result := sessionControlFromObject(fresh)
				return &result, nil
			}
		}
		return nil, mapKubernetesError("initialize runtime session control status", err)
	}
	result := sessionControlFromObject(updated)
	return &result, nil
}

func (s *Store) completeSessionLeaseStatus(ctx context.Context, object *corev1alpha1.RuntimeSessionControl, control store.SessionControl, request store.AcquireSessionMutationLeaseRequest, fence store.ControllerEpochFence, snapshot epochSnapshot, lease *coordinationv1.Lease) (*store.SessionControl, error) {
	state, err := sessionLeaseFromObject(lease, object.Spec.SessionName, object.Spec.SessionUID)
	if err != nil {
		return nil, err
	}
	if !sameSessionMutationLease(state, request) || state.Generation != request.ExpectedLeaseGeneration+1 {
		return nil, store.ConflictErrorf("Session mutation Lease does not match requested ownership")
	}
	fresh, err := s.getSessionControlObject(ctx, request.Namespace, request.SessionName)
	if err != nil {
		return nil, err
	}
	freshControl := sessionControlFromObject(fresh)
	if freshControl.Lease != nil {
		if sameSessionMutationLeaseValue(*freshControl.Lease, request) && freshControl.LeaseGeneration == state.Generation {
			return s.completeExistingSessionLineage(ctx, fresh, freshControl, request, fence, snapshot)
		}
		return nil, store.ConflictErrorf("session %s/%s status is already leased by another operation", request.Namespace, request.SessionName)
	}
	if freshControl.Version != request.ExpectedVersion || freshControl.LeaseGeneration != request.ExpectedLeaseGeneration || freshControl.Availability != store.SessionAvailable {
		return nil, store.ConflictErrorf("session %s/%s changed before Lease status could be committed", request.Namespace, request.SessionName)
	}
	lineage, err := resolveSessionLineage(freshControl.Lineage, *request.Lineage, request.AcquiredAt)
	if err != nil {
		return nil, err
	}
	updated := fresh.DeepCopy()
	updated.Status.MutationLeaseGeneration = state.Generation
	updated.Status.MutationLease = sessionMutationLeaseToAPI(lease, state)
	updated.Status.Lineage = sessionLineageToAPI(lineage)
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, freshControl.Version+1, freshControl.LastOperationID, freshControl.LastOperationDigest, freshControl.CreatedAt, request.AcquiredAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return nil, mapKubernetesError("commit Session mutation Lease status", err)
	}
	result := sessionControlFromObject(updated)
	_ = control
	return &result, nil
}

// completeExistingSessionLineage handles an idempotent retry whose exact
// coordination Lease is already mirrored. Pre-coexistence controls may have a
// mirrored Lease but no lineage; appending the lineage remains fenced by that
// exact Lease and one RuntimeSessionControl status CAS.
func (s *Store) completeExistingSessionLineage(ctx context.Context, object *corev1alpha1.RuntimeSessionControl, control store.SessionControl, request store.AcquireSessionMutationLeaseRequest, fence store.ControllerEpochFence, snapshot epochSnapshot) (*store.SessionControl, error) {
	lineage, err := resolveSessionLineage(control.Lineage, *request.Lineage, request.AcquiredAt)
	if err != nil {
		return nil, err
	}
	if control.Lineage != nil {
		return &control, nil
	}
	updated := object.DeepCopy()
	updated.Status.Lineage = sessionLineageToAPI(lineage)
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, control.Version+1, control.LastOperationID, control.LastOperationDigest, control.CreatedAt, request.AcquiredAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return nil, mapKubernetesError("commit Session lineage under existing mutation Lease", err)
	}
	result := sessionControlFromObject(updated)
	return &result, nil
}

func (s *Store) verifyMirroredSessionLease(ctx context.Context, object *corev1alpha1.RuntimeSessionControl, expected store.SessionMutationLease) error {
	lease, state, err := s.getSessionLease(ctx, object.Namespace, object.Spec.SessionName, object.Spec.SessionUID)
	if err != nil {
		return err
	}
	control := sessionControlFromObject(object)
	if control.Lineage == nil || lease.ResourceVersion != object.Status.MutationLease.LeaseResourceVersion || state.Mode != leaseModeMutation || state.Generation != expected.Generation || state.TaskUID != expected.TaskUID || state.Attempt != expected.Attempt || state.PromptID != expected.PromptID || state.RequestDigest != expected.RequestDigest || state.LineageDigest != sessionLineageDigest(control.Lineage) {
		return store.ConflictErrorf("runtime session status does not match its authoritative mutation Lease")
	}
	return nil
}

func (s *Store) ensureBranchReconciliationApplied(ctx context.Context, request store.ReconcileSessionControlRequest, fence store.ControllerEpochFence, snapshot epochSnapshot) error {
	object, err := s.getBranchClaimObject(ctx, request.BranchClaimID)
	if err != nil {
		return err
	}
	claim := branchClaimFromObject(object)
	if claim.LastOperationID == request.OperationID {
		if claim.LastOperationDigest == request.OperationDigest && claim.Availability == store.BranchClaimAvailable && claim.RelatedPublicationID == "" && claim.BlockedReason == "" && claim.LastVerified.Equal(store.RemoteRefState{SHA: request.VerifiedBaseline.SHA}) {
			return nil
		}
		return store.ConflictErrorf("branch reconciliation operation %q was already applied with different target values", request.OperationID)
	}
	if claim.Version != request.ExpectedBranchClaimVersion || claim.Generation != request.ExpectedBranchClaimGeneration || !claim.LastVerified.Equal(request.ExpectedBranchBaseline) || claim.Availability != store.BranchClaimReconciliationBlocked || claim.RelatedPublicationID != request.ExpectedRelatedPublicationID || claim.RepositoryID != request.VerifiedBaseline.RepositoryID || claim.Ref != request.VerifiedBaseline.Ref {
		return store.ConflictErrorf("branch claim %q no longer matches the blocked reconciliation fence", claim.ID)
	}
	updated := object.DeepCopy()
	verified := remoteRefToAPI(store.RemoteRefState{SHA: request.VerifiedBaseline.SHA})
	updated.Status.LastVerified = &verified
	updated.Status.Availability = corev1alpha1.BranchClaimAvailability(store.BranchClaimAvailable)
	updated.Status.BlockedReason = ""
	updated.Status.RelatedPublicationID = ""
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, claim.Version+1, request.OperationID, request.OperationDigest, claim.CreatedAt, request.ReconciledAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return mapKubernetesError("reconcile branch claim", err)
	}
	return nil
}

func (s *Store) clearReconciliationSessionLease(ctx context.Context, object *corev1alpha1.RuntimeSessionControl, request store.ReconcileSessionControlRequest) error {
	lease, state, err := s.getSessionLease(ctx, object.Namespace, object.Spec.SessionName, object.Spec.SessionUID)
	if err != nil {
		return err
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		return nil
	}
	if state.Mode != leaseModeReconciliation || state.OperationID != request.OperationID || state.OperationHash != request.OperationDigest {
		return store.ConflictErrorf("Session reconciliation Lease is held by a different operation")
	}
	updated := lease.DeepCopy()
	clearSessionLease(updated, state.Generation)
	if err := s.client.Update(ctx, updated); err != nil {
		return mapKubernetesError("release Session reconciliation Lease", err)
	}
	return nil
}

func (s *Store) getSessionControlObject(ctx context.Context, namespace, sessionName string) (*corev1alpha1.RuntimeSessionControl, error) {
	object := &corev1alpha1.RuntimeSessionControl{}
	key := client.ObjectKey{Namespace: namespace, Name: runtimeSessionObjectName(sessionName)}
	if err := s.readClient().Get(ctx, key, object); err != nil {
		return nil, mapKubernetesError("get runtime session control", err)
	}
	if object.Spec.SessionName != sessionName {
		return nil, store.ConflictErrorf("runtime session control %s/%s has a different immutable Session name", namespace, object.Name)
	}
	return object, nil
}

func (s *Store) ensureSessionLease(ctx context.Context, namespace, sessionName, sessionUID string, generation int64) (*coordinationv1.Lease, error) {
	key := client.ObjectKey{Namespace: namespace, Name: runtimeSessionLeaseName(sessionUID)}
	lease := &coordinationv1.Lease{}
	if err := s.readClient().Get(ctx, key, lease); err == nil {
		state, parseErr := sessionLeaseFromObject(lease, sessionName, sessionUID)
		if parseErr != nil {
			return nil, parseErr
		}
		if state.Generation != generation {
			return nil, store.ConflictErrorf("Session Lease generation %d does not match initial generation %d", state.Generation, generation)
		}
		return lease, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, mapKubernetesError("get Session Lease", err)
	}
	lease = &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      key.Name,
			Labels:    controlLabels(sessionControlLogicalID(namespace, sessionName)),
			Annotations: map[string]string{
				annotationSessionName:     sessionName,
				annotationSessionUID:      sessionUID,
				annotationLeaseGeneration: strconv.FormatInt(generation, 10),
				annotationLeaseMode:       leaseModeEmpty,
			},
		},
	}
	if err := s.client.Create(ctx, lease); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if getErr := s.readClient().Get(ctx, key, lease); getErr != nil {
				return nil, mapKubernetesError("get concurrently created Session Lease", getErr)
			}
			state, parseErr := sessionLeaseFromObject(lease, sessionName, sessionUID)
			if parseErr != nil {
				return nil, parseErr
			}
			if state.Generation != generation {
				return nil, store.ConflictErrorf("Session Lease generation %d does not match initial generation %d", state.Generation, generation)
			}
			return lease, nil
		}
		return nil, mapKubernetesError("create Session Lease", err)
	}
	return lease, nil
}

func (s *Store) getSessionLease(ctx context.Context, namespace, sessionName, sessionUID string) (*coordinationv1.Lease, sessionLeaseState, error) {
	lease := &coordinationv1.Lease{}
	key := client.ObjectKey{Namespace: namespace, Name: runtimeSessionLeaseName(sessionUID)}
	if err := s.readClient().Get(ctx, key, lease); err != nil {
		return nil, sessionLeaseState{}, mapKubernetesError("get Session Lease", err)
	}
	state, err := sessionLeaseFromObject(lease, sessionName, sessionUID)
	return lease, state, err
}

func sessionLeaseFromObject(lease *coordinationv1.Lease, sessionName, sessionUID string) (sessionLeaseState, error) {
	annotations := lease.Annotations
	if annotations[annotationSessionName] != sessionName || annotations[annotationSessionUID] != sessionUID {
		return sessionLeaseState{}, fmt.Errorf("session Lease %s/%s identity mismatch", lease.Namespace, lease.Name)
	}
	generation, err := parseNonNegativeInt64("Session Lease generation", annotations[annotationLeaseGeneration])
	if err != nil {
		return sessionLeaseState{}, err
	}
	state := sessionLeaseState{Mode: annotations[annotationLeaseMode], Generation: generation}
	switch state.Mode {
	case leaseModeEmpty:
		if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" {
			return sessionLeaseState{}, fmt.Errorf("empty Session Lease %s/%s has a holder", lease.Namespace, lease.Name)
		}
	case leaseModeMutation:
		state.TaskUID = annotations[annotationTaskUID]
		state.PromptID = annotations[annotationPromptID]
		state.RequestDigest = annotations[annotationRequestDigest]
		state.LineageDigest = annotations[annotationSessionLineage]
		if err := store.ValidateCanonicalDigest("Session Lease lineage digest", state.LineageDigest); err != nil {
			return sessionLeaseState{}, err
		}
		state.Attempt, err = parsePositiveInt64("Session Lease attempt", annotations[annotationAttempt])
		if err != nil {
			return sessionLeaseState{}, err
		}
		state.AcquiredAt, err = parseTime("Session Lease acquisition time", annotations[annotationAcquiredAt])
		if err != nil {
			return sessionLeaseState{}, err
		}
		if encoded := annotations[annotationLeaseExpiresAt]; encoded != "" {
			expiresAt, parseErr := parseTime("Session Lease expiry", encoded)
			if parseErr != nil {
				return sessionLeaseState{}, parseErr
			}
			state.ExpiresAt = &expiresAt
		}
		if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != sessionMutationLeaseHolder(state.TaskUID, state.Attempt, state.PromptID) {
			return sessionLeaseState{}, store.ConflictErrorf("Session mutation Lease holder identity does not match its immutable annotations")
		}
	case leaseModeReconciliation:
		state.OperationID = annotations[annotationOperationID]
		state.OperationHash = annotations[annotationOperationDigest]
		if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != sessionReconciliationLeaseHolder(state.OperationID) {
			return sessionLeaseState{}, store.ConflictErrorf("Session reconciliation Lease holder identity does not match its immutable annotations")
		}
	default:
		return sessionLeaseState{}, fmt.Errorf("session Lease %s/%s has unsupported mode %q", lease.Namespace, lease.Name, state.Mode)
	}
	return state, nil
}

func sessionMutationLeaseHolder(taskUID string, attempt int64, promptID string) string {
	return "mutation:" + dnsDigest(taskUID+"\x00"+strconv.FormatInt(attempt, 10)+"\x00"+promptID)
}

func sessionReconciliationLeaseHolder(operationID string) string {
	return "reconcile:" + dnsDigest(operationID)
}

func setSessionMutationLease(lease *coordinationv1.Lease, request store.AcquireSessionMutationLeaseRequest, generation int64) {
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[annotationLeaseMode] = leaseModeMutation
	lease.Annotations[annotationLeaseGeneration] = strconv.FormatInt(generation, 10)
	lease.Annotations[annotationTaskUID] = request.TaskUID
	lease.Annotations[annotationAttempt] = strconv.FormatInt(request.Attempt, 10)
	lease.Annotations[annotationPromptID] = request.PromptID
	lease.Annotations[annotationRequestDigest] = request.RequestDigest
	lease.Annotations[annotationSessionLineage] = sessionLineageClaimDigest(*request.Lineage)
	lease.Annotations[annotationAcquiredAt] = formatTime(request.AcquiredAt)
	delete(lease.Annotations, annotationOperationID)
	delete(lease.Annotations, annotationOperationDigest)
	if request.ExpiresAt != nil {
		lease.Annotations[annotationLeaseExpiresAt] = formatTime(*request.ExpiresAt)
	} else {
		delete(lease.Annotations, annotationLeaseExpiresAt)
	}
	holder := sessionMutationLeaseHolder(request.TaskUID, request.Attempt, request.PromptID)
	lease.Spec.HolderIdentity = &holder
	acquired := metav1.NewMicroTime(request.AcquiredAt)
	lease.Spec.AcquireTime = &acquired
	lease.Spec.RenewTime = &acquired
	lease.Spec.LeaseDurationSeconds = leaseDurationSeconds(request.AcquiredAt, request.ExpiresAt)
}

func setSessionReconciliationLease(lease *coordinationv1.Lease, request store.ReconcileSessionControlRequest) {
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[annotationLeaseMode] = leaseModeReconciliation
	lease.Annotations[annotationOperationID] = request.OperationID
	lease.Annotations[annotationOperationDigest] = request.OperationDigest
	delete(lease.Annotations, annotationTaskUID)
	delete(lease.Annotations, annotationAttempt)
	delete(lease.Annotations, annotationPromptID)
	delete(lease.Annotations, annotationRequestDigest)
	delete(lease.Annotations, annotationSessionLineage)
	delete(lease.Annotations, annotationAcquiredAt)
	delete(lease.Annotations, annotationLeaseExpiresAt)
	holder := sessionReconciliationLeaseHolder(request.OperationID)
	lease.Spec.HolderIdentity = &holder
	acquired := metav1.NewMicroTime(request.ReconciledAt)
	lease.Spec.AcquireTime = &acquired
	lease.Spec.RenewTime = &acquired
	lease.Spec.LeaseDurationSeconds = nil
}

func clearSessionLease(lease *coordinationv1.Lease, generation int64) {
	lease.Annotations[annotationLeaseMode] = leaseModeEmpty
	lease.Annotations[annotationLeaseGeneration] = strconv.FormatInt(generation, 10)
	for _, key := range []string{annotationTaskUID, annotationAttempt, annotationPromptID, annotationRequestDigest, annotationSessionLineage, annotationAcquiredAt, annotationLeaseExpiresAt, annotationOperationID, annotationOperationDigest} {
		delete(lease.Annotations, key)
	}
	lease.Spec.HolderIdentity = nil
	lease.Spec.AcquireTime = nil
	lease.Spec.RenewTime = nil
	lease.Spec.LeaseDurationSeconds = nil
}

func leaseDurationSeconds(acquired time.Time, expires *time.Time) *int32 {
	if expires == nil {
		return nil
	}
	seconds := int64(expires.Sub(acquired).Round(time.Second) / time.Second)
	seconds = max(seconds, 1)
	seconds = min(seconds, int64(math.MaxInt32))
	result := int32(seconds)
	return &result
}

func sessionMutationLeaseToAPI(lease *coordinationv1.Lease, state sessionLeaseState) *corev1alpha1.RuntimeSessionMutationLeaseStatus {
	result := &corev1alpha1.RuntimeSessionMutationLeaseStatus{
		LeaseName:            lease.Name,
		LeaseResourceVersion: lease.ResourceVersion,
		Generation:           state.Generation,
		TaskUID:              state.TaskUID,
		Attempt:              state.Attempt,
		PromptID:             state.PromptID,
		RequestDigest:        state.RequestDigest,
		AcquiredAt:           metav1.NewTime(state.AcquiredAt),
	}
	if state.ExpiresAt != nil {
		value := metav1.NewTime(*state.ExpiresAt)
		result.ExpiresAt = &value
	}
	return result
}

func sameSessionMutationLease(state sessionLeaseState, request store.AcquireSessionMutationLeaseRequest) bool {
	return state.Mode == leaseModeMutation && state.TaskUID == request.TaskUID && state.Attempt == request.Attempt && state.PromptID == request.PromptID && state.RequestDigest == request.RequestDigest && state.LineageDigest == sessionLineageClaimDigest(*request.Lineage)
}

func sameSessionMutationLeaseValue(value store.SessionMutationLease, request store.AcquireSessionMutationLeaseRequest) bool {
	return value.TaskUID == request.TaskUID && value.Attempt == request.Attempt && value.PromptID == request.PromptID && value.RequestDigest == request.RequestDigest
}

func sessionLineageClaimDigest(claim store.ClaimSessionLineageRequest) string {
	canonical, _ := json.Marshal(struct {
		Namespace         string `json:"namespace"`
		SessionName       string `json:"sessionName"`
		NamespaceUID      string `json:"namespaceUID"`
		SessionUID        string `json:"sessionUID"`
		ContractVersion   string `json:"contractVersion"`
		LineageGeneration int64  `json:"lineageGeneration"`
		RuntimeIdentity   string `json:"runtimeIdentity"`
		ConfigDigest      string `json:"configDigest"`
	}{
		Namespace: claim.Namespace, SessionName: claim.SessionName,
		NamespaceUID: claim.NamespaceUID, SessionUID: claim.SessionUID,
		ContractVersion: claim.ContractVersion, LineageGeneration: claim.LineageGeneration,
		RuntimeIdentity: claim.RuntimeIdentity, ConfigDigest: claim.ConfigDigest,
	})
	return store.CanonicalBytesDigest(append([]byte("orka.session-lineage.v2\x00"), canonical...))
}

func sessionLineageDigest(lineage *store.SessionLineage) string {
	return sessionLineageClaimDigest(store.ClaimSessionLineageRequest{
		Namespace: lineage.Namespace, SessionName: lineage.SessionName,
		NamespaceUID: lineage.NamespaceUID, SessionUID: lineage.SessionUID,
		ContractVersion: lineage.ContractVersion, RuntimeIdentity: lineage.RuntimeIdentity,
		LineageGeneration: lineage.LineageGeneration,
		ConfigDigest:      lineage.ConfigDigest,
	})
}

func runtimeSessionObjectName(sessionName string) string {
	return objectName(runtimeSessionNamePrefix, sessionName)
}

func runtimeSessionLeaseName(sessionUID string) string {
	return objectName(runtimeSessionLeasePrefix, sessionUID)
}

func sessionControlLogicalID(namespace, sessionName string) string {
	return store.CanonicalControlID("runtime-session-control", namespace, sessionName)
}

func sameSessionControlSpec(object *corev1alpha1.RuntimeSessionControl, control store.SessionControl) bool {
	return object.Namespace == control.Namespace && object.Spec.SessionName == control.SessionName && object.Spec.SessionUID == control.SessionUID && object.Spec.RequestDigest == control.RequestDigest
}

func sessionControlFromObject(object *corev1alpha1.RuntimeSessionControl) store.SessionControl {
	result := store.SessionControl{
		Namespace:                object.Namespace,
		SessionName:              object.Spec.SessionName,
		SessionUID:               object.Spec.SessionUID,
		RequestDigest:            object.Spec.RequestDigest,
		Availability:             store.SessionAvailability(object.Status.Availability),
		RuntimeSessionGeneration: object.Status.Generation,
		LeaseGeneration:          object.Status.MutationLeaseGeneration,
		BlockedReason:            object.Status.BlockedReason,
		RelatedPromptAttemptID:   object.Status.RelatedPromptAttemptID,
		RelatedPublicationID:     object.Status.RelatedPublicationID,
		VerifiedBaseline:         verifiedBaselineFromAPI(object.Status.VerifiedBaseline),
		Lineage:                  sessionLineageFromAPI(object),
		ControllerEpochName:      object.Status.ControllerEpochName,
		ControllerEpoch:          object.Status.ControllerEpoch,
		LastOperationID:          object.Status.LastOperationID,
		LastOperationDigest:      object.Status.LastOperationDigest,
		Version:                  object.Status.Version,
		CreatedAt:                timeValue(object.Status.CreatedAt),
		UpdatedAt:                timeValue(object.Status.UpdatedAt),
	}
	if object.Status.MutationLease != nil {
		result.Lease = &store.SessionMutationLease{
			Generation:    object.Status.MutationLease.Generation,
			TaskUID:       object.Status.MutationLease.TaskUID,
			Attempt:       object.Status.MutationLease.Attempt,
			PromptID:      object.Status.MutationLease.PromptID,
			RequestDigest: object.Status.MutationLease.RequestDigest,
			AcquiredAt:    object.Status.MutationLease.AcquiredAt.UTC(),
			ExpiresAt:     optionalTimeValue(object.Status.MutationLease.ExpiresAt),
		}
	}
	return result
}

func sessionLineageFromAPI(object *corev1alpha1.RuntimeSessionControl) *store.SessionLineage {
	if object == nil || object.Status.Lineage == nil {
		return nil
	}
	lineage := object.Status.Lineage
	establishedAt := lineage.EstablishedAt.UTC()
	return &store.SessionLineage{
		Namespace: object.Namespace, SessionName: object.Spec.SessionName,
		NamespaceUID: string(lineage.NamespaceUID), SessionUID: lineage.SessionUID,
		ContractVersion: string(lineage.ContractVersion), LineageGeneration: lineage.Generation,
		RuntimeIdentity: lineage.RuntimeIdentity, ConfigDigest: lineage.ConfigDigest, Version: 1,
		CreatedAt: establishedAt, UpdatedAt: establishedAt,
	}
}

func sessionLineageToAPI(lineage *store.SessionLineage) *corev1alpha1.RuntimeSessionLineageStatus {
	if lineage == nil {
		return nil
	}
	return &corev1alpha1.RuntimeSessionLineageStatus{
		NamespaceUID: types.UID(lineage.NamespaceUID), SessionUID: lineage.SessionUID,
		ContractVersion: corev1alpha1.AgentRuntimeContractVersion(lineage.ContractVersion),
		Generation:      lineage.LineageGeneration, RuntimeIdentity: lineage.RuntimeIdentity,
		ConfigDigest:  lineage.ConfigDigest,
		EstablishedAt: metav1.NewTime(lineage.CreatedAt.UTC()),
	}
}

func resolveSessionLineage(existing *store.SessionLineage, claim store.ClaimSessionLineageRequest, establishedAt time.Time) (*store.SessionLineage, error) {
	if existing == nil {
		if !claim.EstablishIfAbsent {
			return nil, store.ConflictErrorf("session %s/%s is nonempty or otherwise unclassified and has no Kubernetes-authoritative lineage", claim.Namespace, claim.SessionName)
		}
		if claim.LineageGeneration != 1 {
			return nil, store.ConflictErrorf("new session %s/%s lineage must start at generation 1", claim.Namespace, claim.SessionName)
		}
		lineage := &store.SessionLineage{
			Namespace: claim.Namespace, SessionName: claim.SessionName,
			NamespaceUID: claim.NamespaceUID, SessionUID: claim.SessionUID,
			ContractVersion: claim.ContractVersion, LineageGeneration: claim.LineageGeneration,
			RuntimeIdentity: claim.RuntimeIdentity, ConfigDigest: claim.ConfigDigest, Version: 1,
			CreatedAt: establishedAt.UTC(), UpdatedAt: establishedAt.UTC(),
		}
		if err := lineage.Validate(); err != nil {
			return nil, err
		}
		return lineage, nil
	}
	switch {
	case existing.Namespace != claim.Namespace || existing.SessionName != claim.SessionName:
		return nil, store.ConflictErrorf("session lineage belongs to a different namespace or Session name")
	case existing.NamespaceUID != claim.NamespaceUID:
		return nil, store.ConflictErrorf("session %s/%s lineage belongs to a different namespace UID", claim.Namespace, claim.SessionName)
	case existing.SessionUID != claim.SessionUID:
		return nil, store.ConflictErrorf("session %s/%s lineage belongs to a different Session UID", claim.Namespace, claim.SessionName)
	case existing.ContractVersion != claim.ContractVersion:
		return nil, store.ConflictErrorf("session %s/%s lineage contract is %s, not %s", claim.Namespace, claim.SessionName, existing.ContractVersion, claim.ContractVersion)
	case existing.RuntimeIdentity != claim.RuntimeIdentity:
		return nil, store.ConflictErrorf("session %s/%s lineage runtime identity is %q, not %q", claim.Namespace, claim.SessionName, existing.RuntimeIdentity, claim.RuntimeIdentity)
	case existing.ConfigDigest != claim.ConfigDigest:
		return nil, store.ConflictErrorf("session %s/%s lineage configuration digest does not match", claim.Namespace, claim.SessionName)
	case existing.LineageGeneration != claim.LineageGeneration:
		return nil, store.ConflictErrorf("session %s/%s lineage generation is %d, not %d", claim.Namespace, claim.SessionName, existing.LineageGeneration, claim.LineageGeneration)
	default:
		return existing, nil
	}
}

func normalizeAcquireSessionMutationLeaseRequest(request *store.AcquireSessionMutationLeaseRequest) error {
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.SessionName = strings.TrimSpace(request.SessionName)
	request.SessionUID = strings.TrimSpace(request.SessionUID)
	request.TaskUID = strings.TrimSpace(request.TaskUID)
	request.PromptID = strings.TrimSpace(request.PromptID)
	for field, value := range map[string]string{
		sessionControlFieldNamespace: request.Namespace,
		sessionControlFieldName:      request.SessionName,
		sessionControlFieldUID:       request.SessionUID,
		"lease task UID":             request.TaskUID,
		"lease prompt ID":            request.PromptID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return err
		}
	}
	if request.ExpectedVersion < 1 || request.ExpectedLeaseGeneration < 0 || request.Attempt < 1 {
		return store.ValidationErrorf("session expected version and attempt must be at least 1 and lease generation must not be negative")
	}
	if err := store.ValidateCanonicalDigest("session lease request digest", request.RequestDigest); err != nil {
		return err
	}
	if request.Lineage == nil {
		return store.ValidationErrorf("session lineage claim is required for Kubernetes mutation Lease acquisition")
	}
	lineage := *request.Lineage
	lineage.Namespace = strings.TrimSpace(lineage.Namespace)
	lineage.SessionName = strings.TrimSpace(lineage.SessionName)
	lineage.NamespaceUID = strings.TrimSpace(lineage.NamespaceUID)
	lineage.SessionUID = strings.TrimSpace(lineage.SessionUID)
	lineage.ContractVersion = strings.TrimSpace(lineage.ContractVersion)
	lineage.RuntimeIdentity = strings.TrimSpace(lineage.RuntimeIdentity)
	lineage.ConfigDigest = strings.TrimSpace(lineage.ConfigDigest)
	if err := lineage.Validate(); err != nil {
		return err
	}
	if lineage.Namespace != request.Namespace || lineage.SessionName != request.SessionName || lineage.SessionUID != request.SessionUID {
		return store.ValidationErrorf("session lineage namespace, name, and UID must match the mutation Lease request")
	}
	request.Lineage = &lineage
	request.AcquiredAt = store.NormalizeControlTime(request.AcquiredAt)
	request.ExpiresAt = store.NormalizeOptionalControlTime(request.ExpiresAt)
	if request.ExpiresAt != nil && !request.ExpiresAt.After(request.AcquiredAt) {
		return store.ValidationErrorf("session lease expiry must be after acquisition")
	}
	return nil
}

func normalizeReleaseSessionMutationLeaseRequest(request *store.ReleaseSessionMutationLeaseRequest) error {
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.SessionName = strings.TrimSpace(request.SessionName)
	request.SessionUID = strings.TrimSpace(request.SessionUID)
	request.OperationID = strings.TrimSpace(request.OperationID)
	for field, value := range map[string]string{
		sessionControlFieldNamespace: request.Namespace, sessionControlFieldName: request.SessionName,
		sessionControlFieldUID: request.SessionUID, "session lease release operation ID": request.OperationID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return err
		}
	}
	if err := request.Key.Validate(); err != nil {
		return err
	}
	if request.Key.SessionUID != request.SessionUID || request.ExpectedSessionVersion < 1 || request.OperationID != prePromptLeaseReleasePrefix+mustSessionTurnID(request.Key) {
		return store.ValidationErrorf("session lease release identity/version is invalid")
	}
	if err := store.ValidateCanonicalDigest("session lease request digest", request.LeaseRequestDigest); err != nil {
		return err
	}
	if err := store.ValidateCanonicalDigest("session lease release operation digest", request.OperationDigest); err != nil {
		return err
	}
	expectedDigest, err := store.SessionLeaseReleaseOperationDigest(mustSessionTurnID(request.Key), request.LeaseRequestDigest)
	if err != nil {
		return err
	}
	if request.OperationDigest != expectedDigest {
		return store.ValidationErrorf("session lease release operation digest does not match its Lease request digest")
	}
	request.ReleasedAt = store.NormalizeControlTime(request.ReleasedAt)
	return nil
}

func mustSessionTurnID(key store.SessionTurnKey) string {
	id, _ := key.CanonicalID()
	return id
}

func normalizeReconcileSessionControlRequest(request *store.ReconcileSessionControlRequest) error {
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.SessionName = strings.TrimSpace(request.SessionName)
	request.SessionUID = strings.TrimSpace(request.SessionUID)
	request.ExpectedRelatedPublicationID = strings.TrimSpace(request.ExpectedRelatedPublicationID)
	request.BranchClaimID = strings.TrimSpace(request.BranchClaimID)
	request.OperationID = strings.TrimSpace(request.OperationID)
	for field, value := range map[string]string{
		sessionControlFieldNamespace:          request.Namespace,
		sessionControlFieldName:               request.SessionName,
		sessionControlFieldUID:                request.SessionUID,
		"branch claim ID":                     request.BranchClaimID,
		"session reconciliation operation ID": request.OperationID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return err
		}
	}
	if request.ExpectedRelatedPublicationID != "" {
		if err := store.ValidateControlIdentifier("related publication ID", request.ExpectedRelatedPublicationID); err != nil {
			return err
		}
	}
	if request.ExpectedVersion < 1 || request.ExpectedLeaseGeneration < 1 || request.ExpectedBranchClaimVersion < 1 || request.ExpectedBranchClaimGeneration < 1 {
		return store.ValidationErrorf("session/branch reconciliation versions and generations must be at least 1")
	}
	if err := request.ExpectedBranchBaseline.Validate("expected reconciliation branch baseline"); err != nil {
		return err
	}
	request.VerifiedBaseline.RepositoryID = strings.TrimSpace(request.VerifiedBaseline.RepositoryID)
	request.VerifiedBaseline.Ref = strings.TrimSpace(request.VerifiedBaseline.Ref)
	request.VerifiedBaseline.SHA = strings.TrimSpace(request.VerifiedBaseline.SHA)
	if err := validateVerifiedBaseline(request.VerifiedBaseline); err != nil {
		return err
	}
	if err := store.ValidateCanonicalDigest("session reconciliation operation digest", request.OperationDigest); err != nil {
		return err
	}
	request.ReconciledAt = store.NormalizeControlTime(request.ReconciledAt)
	return nil
}

func verifiedBaselineToAPI(value *store.VerifiedBranchBaseline) *corev1alpha1.ControlVerifiedBranchBaseline {
	if value == nil {
		return nil
	}
	return &corev1alpha1.ControlVerifiedBranchBaseline{RepositoryID: value.RepositoryID, Ref: value.Ref, SHA: value.SHA}
}

func verifiedBaselineFromAPI(value *corev1alpha1.ControlVerifiedBranchBaseline) *store.VerifiedBranchBaseline {
	if value == nil {
		return nil
	}
	return &store.VerifiedBranchBaseline{RepositoryID: value.RepositoryID, Ref: value.Ref, SHA: value.SHA}
}

// normalizeSessionControlForCreate applies the shared session control contract
// and additionally rejects lineage on create (lineage is established with
// mutation Lease acquisition) and trims the related record IDs.
func normalizeSessionControlForCreate(control *store.SessionControl, fence store.ControllerEpochFence) (store.SessionControl, store.ControllerEpochFence, error) {
	if control == nil {
		return store.NormalizeSessionControlForCreate(nil, fence)
	}
	if control.Lineage != nil {
		return store.SessionControl{}, store.ControllerEpochFence{}, store.ValidationErrorf("new session control lineage must be established with mutation Lease acquisition")
	}
	trimmed := *control
	trimmed.RelatedPromptAttemptID = strings.TrimSpace(trimmed.RelatedPromptAttemptID)
	trimmed.RelatedPublicationID = strings.TrimSpace(trimmed.RelatedPublicationID)
	return store.NormalizeSessionControlForCreate(&trimmed, fence)
}

// validateVerifiedBaseline tolerates surrounding whitespace on the SHA before
// applying the shared verified baseline contract.
func validateVerifiedBaseline(baseline store.VerifiedBranchBaseline) error {
	baseline.SHA = strings.TrimSpace(baseline.SHA)
	return store.ValidateVerifiedBaseline(baseline)
}
