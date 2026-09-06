package kube

import (
	"context"
	"errors"
	"strings"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ store.BranchClaimCreationStore = (*Store)(nil)

// CreateBranchClaim creates the cluster-scoped canonical repository/ref claim.
func (s *Store) CreateBranchClaim(ctx context.Context, claim *store.BranchClaim, fence store.ControllerEpochFence) (*store.BranchClaim, error) {
	created, _, err := s.CreateBranchClaimWithResult(ctx, claim, fence)
	return created, err
}

// CreateBranchClaimWithResult reports true only when this call created the
// immutable Kubernetes object rather than observing an existing equivalent.
func (s *Store) CreateBranchClaimWithResult(ctx context.Context, claim *store.BranchClaim, fence store.ControllerEpochFence) (*store.BranchClaim, bool, error) {
	if err := s.requireClient(); err != nil {
		return nil, false, err
	}
	if err := s.requireBranchClaimAccess(); err != nil {
		return nil, false, err
	}
	normalized, normalizedFence, err := store.NormalizeBranchClaimForCreate(claim, fence)
	if err != nil {
		return nil, false, err
	}
	_, snapshot, err := s.requireControllerEpoch(ctx, normalizedFence)
	if err != nil {
		return nil, false, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	if normalized.OwnerKind == store.BranchClaimOwnerSession {
		fenced, fenceErr := s.sessionCleanupFencedForUID(ctx, normalized.OwnerUID)
		if fenceErr != nil {
			return nil, false, fenceErr
		}
		if fenced {
			return nil, false, store.ConflictErrorf("Session UID %q is being deleted or was already deleted", normalized.OwnerUID)
		}
	}

	key := client.ObjectKey{Name: objectName(branchClaimNamePrefix, normalized.ID)}
	object := &corev1alpha1.BranchClaim{}
	err = s.readClient().Get(ctx, key, object)
	if err == nil {
		result, completeErr := s.completeBranchClaimCreation(ctx, object, normalized, normalizedFence, snapshot)
		return result, false, completeErr
	}
	if !apierrors.IsNotFound(err) {
		return nil, false, mapKubernetesError("get branch claim", err)
	}

	object = &corev1alpha1.BranchClaim{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Labels: controlLabels(normalized.ID)},
		Spec: corev1alpha1.BranchClaimSpec{
			ID:            normalized.ID,
			RepositoryID:  normalized.RepositoryID,
			Ref:           normalized.Ref,
			OwnerKind:     corev1alpha1.BranchClaimOwnerKind(normalized.OwnerKind),
			OwnerUID:      normalized.OwnerUID,
			RequestDigest: normalized.RequestDigest,
		},
	}
	if createErr := s.client.Create(ctx, object); createErr != nil {
		fresh := &corev1alpha1.BranchClaim{}
		if getErr := s.readClient().Get(ctx, key, fresh); getErr == nil {
			result, completeErr := s.completeBranchClaimCreation(ctx, fresh, normalized, normalizedFence, snapshot)
			return result, false, completeErr
		} else if apierrors.IsAlreadyExists(createErr) {
			return nil, false, mapKubernetesError("get concurrently created branch claim", getErr)
		}
		return nil, false, mapKubernetesError("create branch claim", createErr)
	}
	result, completeErr := s.completeBranchClaimCreation(ctx, object, normalized, normalizedFence, snapshot)
	return result, completeErr == nil, completeErr
}

// GetBranchClaim returns a cluster-scoped claim by canonical ID.
func (s *Store) GetBranchClaim(ctx context.Context, id string) (*store.BranchClaim, error) {
	if err := s.requireClient(); err != nil {
		return nil, err
	}
	if err := s.requireBranchClaimAccess(); err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	if err := store.ValidateControlIdentifier("branch claim ID", id); err != nil {
		return nil, err
	}
	object, err := s.getBranchClaimObject(ctx, id)
	if err != nil {
		return nil, err
	}
	result := branchClaimFromObject(object)
	return &result, nil
}

// ReclaimBranchClaim deletes only the exact available owner-fenced object. UID
// and resourceVersion preconditions close the read/delete race. Absence or a
// different immutable owner/request identity means the original claim was
// already reclaimed and possibly replaced, so the retry is a safe no-op.
func (s *Store) ReclaimBranchClaim(ctx context.Context, request store.ReclaimBranchClaimRequest) error {
	if err := s.requireClient(); err != nil {
		return err
	}
	if err := s.requireBranchClaimAccess(); err != nil {
		return err
	}
	normalized, err := store.NormalizeBranchClaimReclamationRequest(request)
	if err != nil {
		return err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, normalized.Fence)
	if err != nil {
		return err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	normalized.Fence = fence

	object, err := s.getBranchClaimObject(ctx, normalized.ID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	claim := branchClaimFromObject(object)
	if store.BranchClaimReclamationIdentityReplaced(claim, normalized) {
		return nil
	}
	if !store.BranchClaimMatchesReclamation(claim, normalized) {
		return store.ConflictErrorf("branch claim %q no longer matches the exact Task-owner reclamation fence", claim.ID)
	}
	uid := object.UID
	resourceVersion := object.ResourceVersion
	deleteErr := s.client.Delete(ctx, object, &client.DeleteOptions{Preconditions: &metav1.Preconditions{
		UID: &uid, ResourceVersion: &resourceVersion,
	}})
	if deleteErr == nil || apierrors.IsNotFound(deleteErr) {
		return nil
	}
	fresh, getErr := s.getBranchClaimObject(ctx, normalized.ID)
	if errors.Is(getErr, store.ErrNotFound) {
		return nil
	}
	if getErr == nil && store.BranchClaimReclamationIdentityReplaced(branchClaimFromObject(fresh), normalized) {
		return nil
	}
	if getErr != nil {
		return getErr
	}
	return mapKubernetesError("delete reclaimed branch claim", deleteErr)
}

// CompareAndSwapBranchClaim applies exact version, generation, baseline,
// availability, resourceVersion, and controller-epoch fences.
func (s *Store) CompareAndSwapBranchClaim(ctx context.Context, change store.BranchClaimCAS) (*store.BranchClaim, error) {
	if err := s.requireBranchClaimAccess(); err != nil {
		return nil, err
	}
	change.ID = strings.TrimSpace(change.ID)
	if err := store.ValidateControlIdentifier("branch claim ID", change.ID); err != nil {
		return nil, err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, change.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	change.Fence = fence
	if err := validateBranchClaimCAS(&change); err != nil {
		return nil, err
	}

	object, err := s.getBranchClaimObject(ctx, change.ID)
	if err != nil {
		return nil, err
	}
	claim := branchClaimFromObject(object)
	if claim.LastOperationID == change.OperationID {
		if claim.LastOperationDigest != change.OperationDigest {
			return nil, store.ConflictErrorf("branch claim operation %q was reused with a different digest", change.OperationID)
		}
		if claim.Generation == change.NewGeneration && claim.LastVerified.Equal(change.NewLastVerified) && claim.Availability == change.NewAvailability && claim.BlockedReason == change.BlockedReason && claim.RelatedPublicationID == change.RelatedPublicationID {
			return &claim, nil
		}
		return nil, store.ConflictErrorf("branch claim operation %q was already applied with different target values", change.OperationID)
	}
	if claim.Version != change.ExpectedVersion || claim.Generation != change.ExpectedGeneration || !claim.LastVerified.Equal(change.ExpectedLastVerified) || claim.Availability != change.ExpectedAvailability {
		return nil, store.ConflictErrorf("branch claim %q no longer matches expected version, generation, baseline, or availability", claim.ID)
	}

	updated := object.DeepCopy()
	updated.Status.Generation = change.NewGeneration
	remote := remoteRefToAPI(change.NewLastVerified)
	updated.Status.LastVerified = &remote
	updated.Status.Availability = corev1alpha1.BranchClaimAvailability(change.NewAvailability)
	updated.Status.BlockedReason = change.BlockedReason
	updated.Status.RelatedPublicationID = change.RelatedPublicationID
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, claim.Version+1, change.OperationID, change.OperationDigest, claim.CreatedAt, change.UpdatedAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return nil, mapKubernetesError("compare-and-swap branch claim", err)
	}
	result := branchClaimFromObject(updated)
	return &result, nil
}

func (s *Store) completeBranchClaimCreation(ctx context.Context, object *corev1alpha1.BranchClaim, normalized store.BranchClaim, fence store.ControllerEpochFence, snapshot epochSnapshot) (*store.BranchClaim, error) {
	if !sameBranchClaimSpec(object, normalized) {
		return nil, store.ConflictErrorf("branch claim %q was reused with a different owner or request digest", normalized.ID)
	}
	if object.Status.Version > 0 {
		existing := branchClaimFromObject(object)
		return &existing, nil
	}
	updated := object.DeepCopy()
	updated.Status.Generation = normalized.Generation
	remote := remoteRefToAPI(normalized.LastVerified)
	updated.Status.LastVerified = &remote
	updated.Status.Availability = corev1alpha1.BranchClaimAvailability(normalized.Availability)
	updated.Status.BlockedReason = normalized.BlockedReason
	updated.Status.RelatedPublicationID = normalized.RelatedPublicationID
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, 1, "", "", normalized.CreatedAt, normalized.UpdatedAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		if apierrors.IsConflict(err) {
			fresh, getErr := s.getBranchClaimObject(ctx, normalized.ID)
			if getErr == nil && sameBranchClaimSpec(fresh, normalized) && fresh.Status.Version > 0 {
				result := branchClaimFromObject(fresh)
				if result.LastVerified.Equal(normalized.LastVerified) {
					return &result, nil
				}
			}
		}
		return nil, mapKubernetesError("initialize branch claim status", err)
	}
	result := branchClaimFromObject(updated)
	return &result, nil
}

func (s *Store) getBranchClaimObject(ctx context.Context, id string) (*corev1alpha1.BranchClaim, error) {
	if err := s.requireBranchClaimAccess(); err != nil {
		return nil, err
	}
	object := &corev1alpha1.BranchClaim{}
	key := client.ObjectKey{Name: objectName(branchClaimNamePrefix, id)}
	if err := s.readClient().Get(ctx, key, object); err != nil {
		return nil, mapKubernetesError("get branch claim", err)
	}
	if object.Spec.ID != id {
		return nil, store.ConflictErrorf("branch claim object %q has a different canonical ID", object.Name)
	}
	return object, nil
}

func validateBranchClaimCAS(change *store.BranchClaimCAS) error {
	if change.ExpectedVersion < 1 || change.ExpectedGeneration < 1 {
		return store.ValidationErrorf("branch claim expected version and generation must be at least 1")
	}
	if change.NewGeneration != change.ExpectedGeneration && change.NewGeneration != change.ExpectedGeneration+1 {
		return store.ValidationErrorf("branch claim generation may be preserved or incremented exactly by one")
	}
	if err := change.ExpectedLastVerified.Validate("expected branch baseline"); err != nil {
		return err
	}
	if err := change.NewLastVerified.Validate("new branch baseline"); err != nil {
		return err
	}
	if !store.IsKnownBranchClaimAvailability(change.ExpectedAvailability) || !store.IsKnownBranchClaimAvailability(change.NewAvailability) {
		return store.ValidationErrorf("unsupported branch claim availability transition %q -> %q", change.ExpectedAvailability, change.NewAvailability)
	}
	change.BlockedReason = strings.TrimSpace(change.BlockedReason)
	change.RelatedPublicationID = strings.TrimSpace(change.RelatedPublicationID)
	if err := store.ValidateControlReason("branch claim blocked reason", change.BlockedReason); err != nil {
		return err
	}
	if change.NewAvailability == store.BranchClaimAvailable && (change.BlockedReason != "" || change.RelatedPublicationID != "") {
		return store.ValidationErrorf("available branch claim must clear blocked reason and related publication")
	}
	if change.NewAvailability == store.BranchClaimReconciliationBlocked && change.BlockedReason == "" {
		return store.ValidationErrorf("reconciliation-blocked branch claim requires a reason")
	}
	change.OperationID = strings.TrimSpace(change.OperationID)
	if err := store.ValidateControlIdentifier("branch claim operation ID", change.OperationID); err != nil {
		return err
	}
	if err := store.ValidateCanonicalDigest("branch claim operation digest", change.OperationDigest); err != nil {
		return err
	}
	change.UpdatedAt = store.NormalizeControlTime(change.UpdatedAt)
	return nil
}

func sameBranchClaimSpec(object *corev1alpha1.BranchClaim, claim store.BranchClaim) bool {
	return object.Spec.ID == claim.ID && object.Spec.RepositoryID == claim.RepositoryID && object.Spec.Ref == claim.Ref &&
		store.BranchClaimOwnerKind(object.Spec.OwnerKind) == claim.OwnerKind && object.Spec.OwnerUID == claim.OwnerUID && object.Spec.RequestDigest == claim.RequestDigest
}

func branchClaimFromObject(object *corev1alpha1.BranchClaim) store.BranchClaim {
	result := store.BranchClaim{
		ID:                   object.Spec.ID,
		RepositoryID:         object.Spec.RepositoryID,
		Ref:                  object.Spec.Ref,
		OwnerKind:            store.BranchClaimOwnerKind(object.Spec.OwnerKind),
		OwnerUID:             object.Spec.OwnerUID,
		Generation:           object.Status.Generation,
		Availability:         store.BranchClaimAvailability(object.Status.Availability),
		BlockedReason:        object.Status.BlockedReason,
		RelatedPublicationID: object.Status.RelatedPublicationID,
		RequestDigest:        object.Spec.RequestDigest,
		ControllerEpochName:  object.Status.ControllerEpochName,
		ControllerEpoch:      object.Status.ControllerEpoch,
		LastOperationID:      object.Status.LastOperationID,
		LastOperationDigest:  object.Status.LastOperationDigest,
		Version:              object.Status.Version,
		CreatedAt:            timeValue(object.Status.CreatedAt),
		UpdatedAt:            timeValue(object.Status.UpdatedAt),
	}
	if object.Status.LastVerified != nil {
		result.LastVerified = remoteRefFromAPI(*object.Status.LastVerified)
	}
	return result
}

func remoteRefToAPI(value store.RemoteRefState) corev1alpha1.ControlRemoteRefState {
	return corev1alpha1.ControlRemoteRefState{Absent: value.Absent, SHA: value.SHA}
}

func remoteRefFromAPI(value corev1alpha1.ControlRemoteRefState) store.RemoteRefState {
	return store.RemoteRefState{Absent: value.Absent, SHA: value.SHA}
}
