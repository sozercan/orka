package kube

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrSessionCleanupStoreNotConfigured is returned when a standalone
// Kubernetes store has no SQLite cleanup persistence adapter.
var ErrSessionCleanupStoreNotConfigured = errors.New("session cleanup persistence is not configured")

// ReclaimSession coordinates the hard-cutover Session deletion protocol under
// one controller-epoch mutation lock. The SQLite intent is durable before any
// Kubernetes object is deleted; transcript state is deleted only after every
// exact authoritative object has been reclaimed.
func (s *Store) ReclaimSession(ctx context.Context, request store.ReclaimSessionRequest) error {
	if err := s.requireClient(); err != nil {
		return err
	}
	if s.sessionCleanup == nil {
		return ErrSessionCleanupStoreNotConfigured
	}
	if err := normalizeReclaimSessionRequest(&request); err != nil {
		return err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, request.Fence)
	if err != nil {
		return err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	request.Fence = fence

	intent, err := s.sessionCleanup.GetSessionCleanupIntent(ctx, request.Namespace, request.SessionName)
	if errors.Is(err, store.ErrNotFound) {
		completion, completionErr := s.sessionCleanup.GetSessionCleanupCompletion(ctx, request.Namespace, request.SessionName)
		if completionErr == nil {
			if completion.OperationID != request.OperationID || completion.OperationDigest != request.OperationDigest {
				return store.ConflictErrorf("session cleanup for %s/%s completed under a different operation", request.Namespace, request.SessionName)
			}
			return s.ensureCompletedSessionKubernetesStateAbsent(ctx, *completion)
		}
		if !errors.Is(completionErr, store.ErrNotFound) {
			return completionErr
		}
		intent, err = s.prepareSessionCleanupIntent(ctx, request)
	}
	if err != nil {
		return err
	}
	if intent.OperationID != request.OperationID || intent.OperationDigest != request.OperationDigest {
		return store.ConflictErrorf("session cleanup for %s/%s belongs to a different operation", request.Namespace, request.SessionName)
	}
	if err := s.validateSessionCleanupBranchClaimScope(*intent); err != nil {
		return err
	}
	if err := s.reclaimSessionBranchClaims(ctx, *intent); err != nil {
		return err
	}
	if err := s.ensureNoSessionBranchClaims(ctx, intent.SessionUID); err != nil {
		return err
	}
	if err := s.reclaimSessionLease(ctx, *intent); err != nil {
		return err
	}
	if err := s.ensureNoSessionLease(ctx, *intent); err != nil {
		return err
	}
	if err := s.reclaimSessionControl(ctx, *intent); err != nil {
		return err
	}
	return s.sessionCleanup.CompleteSessionCleanup(ctx, store.CompleteSessionCleanupRequest{
		Namespace: intent.Namespace, SessionName: intent.SessionName,
		OperationID: intent.OperationID, OperationDigest: intent.OperationDigest,
	})
}

func (s *Store) ensureCompletedSessionKubernetesStateAbsent(ctx context.Context, completion store.SessionCleanupCompletion) error {
	if _, err := s.getSessionControlObject(ctx, completion.Namespace, completion.SessionName); err == nil {
		return store.ConflictErrorf("deleted session %s/%s regained a RuntimeSessionControl", completion.Namespace, completion.SessionName)
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	if err := s.ensureNoSessionBranchClaims(ctx, completion.SessionUID); err != nil {
		return err
	}
	if completion.SessionUID == "" {
		return nil
	}
	if object, err := s.findSessionControlByUID(ctx, completion.SessionUID); err == nil {
		return store.ConflictErrorf("deleted Session UID %q regained RuntimeSessionControl %s/%s", completion.SessionUID, object.Namespace, object.Spec.SessionName)
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	lease := &coordinationv1.Lease{}
	key := client.ObjectKey{Namespace: completion.Namespace, Name: runtimeSessionLeaseName(completion.SessionUID)}
	if err := s.readClient().Get(ctx, key, lease); err == nil {
		return store.ConflictErrorf("deleted session %s/%s regained its coordination Lease", completion.Namespace, completion.SessionName)
	} else if !apierrors.IsNotFound(err) {
		return mapKubernetesError("verify completed Session Lease absence", err)
	}
	return nil
}

// ResumeSessionCleanups replays every durable intent under the current
// controller epoch. Individual failures are joined so one blocked Session does
// not prevent independent cleanup plans from making progress.
func (s *Store) ResumeSessionCleanups(ctx context.Context, fence store.ControllerEpochFence) error {
	if err := s.requireClient(); err != nil {
		return err
	}
	if s.sessionCleanup == nil {
		return ErrSessionCleanupStoreNotConfigured
	}
	intents, err := s.sessionCleanup.ListSessionCleanupIntents(ctx)
	if err != nil {
		return err
	}
	var result error
	for _, intent := range intents {
		reclaimErr := s.ReclaimSession(ctx, store.ReclaimSessionRequest{
			Namespace: intent.Namespace, SessionName: intent.SessionName, Fence: fence,
			OperationID: intent.OperationID, OperationDigest: intent.OperationDigest, RequestedAt: intent.PreparedAt,
		})
		if reclaimErr != nil {
			result = errors.Join(result, fmt.Errorf("resume Session cleanup %s/%s: %w", intent.Namespace, intent.SessionName, reclaimErr))
		}
	}
	return result
}

func (s *Store) prepareSessionCleanupIntent(ctx context.Context, request store.ReclaimSessionRequest) (*store.SessionCleanupIntent, error) {
	intent := store.SessionCleanupIntent{
		Namespace: request.Namespace, SessionName: request.SessionName,
		OperationID: request.OperationID, OperationDigest: request.OperationDigest,
		PreparedAt: request.RequestedAt,
	}
	object, err := s.getSessionControlObject(ctx, request.Namespace, request.SessionName)
	if errors.Is(err, store.ErrNotFound) {
		sessionUID, identityErr := s.sessionCleanup.GetSessionCleanupIdentity(ctx, request.Namespace, request.SessionName)
		if identityErr != nil && !errors.Is(identityErr, store.ErrNotFound) {
			return nil, identityErr
		}
		if strings.TrimSpace(sessionUID) != "" {
			claims, claimErr := s.sessionBranchClaimCleanupPlan(ctx, sessionUID)
			if claimErr != nil {
				return nil, claimErr
			}
			intent.SessionUID = sessionUID
			intent.BranchClaims = claims
			lease := &coordinationv1.Lease{}
			leaseKey := client.ObjectKey{Namespace: request.Namespace, Name: runtimeSessionLeaseName(sessionUID)}
			if leaseErr := s.readClient().Get(ctx, leaseKey, lease); leaseErr == nil {
				state, stateErr := sessionLeaseFromObject(lease, request.SessionName, sessionUID)
				if stateErr != nil {
					return nil, stateErr
				}
				if (lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "") || state.Mode != leaseModeEmpty {
					return nil, store.ConflictErrorf("session %s/%s coordination Lease is active without a control record", request.Namespace, request.SessionName)
				}
				intent.ExpectedLeaseGeneration = state.Generation
				intent.LeaseName = lease.Name
				intent.LeaseObjectUID = string(lease.UID)
			} else if !apierrors.IsNotFound(leaseErr) {
				return nil, mapKubernetesError("get orphan Session cleanup Lease", leaseErr)
			}
			return s.sessionCleanup.PrepareSessionCleanup(ctx, intent)
		}
		return s.sessionCleanup.PrepareSessionCleanup(ctx, intent)
	}
	if err != nil {
		return nil, err
	}
	control := sessionControlFromObject(object)
	if control.Availability != store.SessionAvailable || control.Lease != nil ||
		control.BlockedReason != "" || control.RelatedPromptAttemptID != "" || control.RelatedPublicationID != "" {
		return nil, store.ConflictErrorf("session %s/%s has active or unresolved authoritative state", request.Namespace, request.SessionName)
	}
	lease, leaseState, err := s.getSessionLease(ctx, object.Namespace, object.Spec.SessionName, object.Spec.SessionUID)
	if err != nil {
		return nil, err
	}
	if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" {
		return nil, store.ConflictErrorf("session %s/%s coordination Lease is held", request.Namespace, request.SessionName)
	}
	if leaseState.Mode != leaseModeEmpty || leaseState.Generation != control.LeaseGeneration {
		return nil, store.ConflictErrorf("session %s/%s coordination Lease does not match the quiescent control generation", request.Namespace, request.SessionName)
	}
	claims, err := s.sessionBranchClaimCleanupPlan(ctx, control.SessionUID)
	if err != nil {
		return nil, err
	}
	intent.SessionUID = control.SessionUID
	intent.ControlObjectUID = string(object.UID)
	intent.ControlRequestDigest = control.RequestDigest
	intent.ExpectedControlVersion = control.Version
	intent.ExpectedLeaseGeneration = control.LeaseGeneration
	intent.ExpectedControlLastOperationID = control.LastOperationID
	intent.ExpectedControlLastDigest = control.LastOperationDigest
	intent.ExpectedVerifiedBaseline = cloneVerifiedBranchBaseline(control.VerifiedBaseline)
	intent.LeaseName = lease.Name
	intent.LeaseObjectUID = string(lease.UID)
	intent.BranchClaims = claims
	return s.sessionCleanup.PrepareSessionCleanup(ctx, intent)
}

func (s *Store) sessionBranchClaimCleanupPlan(
	ctx context.Context,
	sessionUID string,
) ([]store.SessionCleanupBranchClaim, error) {
	if !s.branchClaimsEnabled {
		return nil, nil
	}
	list := &corev1alpha1.BranchClaimList{}
	if err := s.readClient().List(ctx, list); err != nil {
		return nil, mapKubernetesError("list Session-owned branch claims", err)
	}
	claims := make([]store.SessionCleanupBranchClaim, 0)
	for i := range list.Items {
		object := &list.Items[i]
		if store.BranchClaimOwnerKind(object.Spec.OwnerKind) != store.BranchClaimOwnerSession || object.Spec.OwnerUID != sessionUID {
			continue
		}
		claim := branchClaimFromObject(object)
		available := claim.Availability == store.BranchClaimAvailable &&
			claim.BlockedReason == "" && claim.RelatedPublicationID == ""
		if !available {
			return nil, store.ConflictErrorf("Session-owned branch claim %q is reconciliation-blocked", claim.ID)
		}
		claims = append(claims, store.SessionCleanupBranchClaim{
			ID: claim.ID, ObjectUID: string(object.UID),
			ExpectedVersion: claim.Version, ExpectedGeneration: claim.Generation,
			ExpectedRepositoryID: claim.RepositoryID, ExpectedRef: claim.Ref,
			ExpectedOwnerUID: claim.OwnerUID, ExpectedLastVerified: claim.LastVerified,
			ExpectedAvailability: claim.Availability, ExpectedRequestDigest: claim.RequestDigest,
			ExpectedBlockedReason: claim.BlockedReason, ExpectedPublicationID: claim.RelatedPublicationID,
		})
	}
	sort.Slice(claims, func(i, j int) bool { return claims[i].ID < claims[j].ID })
	return claims, nil
}

func (s *Store) reclaimSessionBranchClaims(ctx context.Context, intent store.SessionCleanupIntent) error {
	if !s.branchClaimsEnabled {
		return s.validateSessionCleanupBranchClaimScope(intent)
	}
	for _, expected := range intent.BranchClaims {
		object, err := s.getBranchClaimObject(ctx, expected.ID)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		claim := branchClaimFromObject(object)
		if claim.OwnerKind != store.BranchClaimOwnerSession || claim.OwnerUID != expected.ExpectedOwnerUID || claim.RequestDigest != expected.ExpectedRequestDigest {
			continue
		}
		if expected.ObjectUID != "" && string(object.UID) != expected.ObjectUID {
			return store.ConflictErrorf("Session-owned branch claim %q was recreated during cleanup", expected.ID)
		}
		if claim.Version != expected.ExpectedVersion || claim.Generation != expected.ExpectedGeneration ||
			claim.RepositoryID != expected.ExpectedRepositoryID || claim.Ref != expected.ExpectedRef ||
			!claim.LastVerified.Equal(expected.ExpectedLastVerified) || claim.Availability != expected.ExpectedAvailability ||
			claim.BlockedReason != expected.ExpectedBlockedReason ||
			claim.RelatedPublicationID != expected.ExpectedPublicationID {
			return store.ConflictErrorf("Session-owned branch claim %q no longer matches its cleanup fence", expected.ID)
		}
		if err := deleteObjectWithExactPreconditions(ctx, s.client, object); err != nil && !apierrors.IsNotFound(err) {
			return mapKubernetesError("delete Session-owned branch claim", err)
		}
		fresh, getErr := s.getBranchClaimObject(ctx, expected.ID)
		if errors.Is(getErr, store.ErrNotFound) {
			continue
		}
		if getErr != nil {
			return getErr
		}
		freshClaim := branchClaimFromObject(fresh)
		if freshClaim.OwnerKind != store.BranchClaimOwnerSession || freshClaim.OwnerUID != expected.ExpectedOwnerUID || freshClaim.RequestDigest != expected.ExpectedRequestDigest {
			continue
		}
		return store.ConflictErrorf("Session-owned branch claim %q still exists after cleanup", expected.ID)
	}
	return nil
}

func (s *Store) ensureNoSessionBranchClaims(ctx context.Context, sessionUID string) error {
	if sessionUID == "" {
		return nil
	}
	if !s.branchClaimsEnabled {
		return nil
	}
	list := &corev1alpha1.BranchClaimList{}
	if err := s.readClient().List(ctx, list); err != nil {
		return mapKubernetesError("verify Session-owned branch claim cleanup", err)
	}
	for i := range list.Items {
		claim := &list.Items[i]
		if store.BranchClaimOwnerKind(claim.Spec.OwnerKind) == store.BranchClaimOwnerSession && claim.Spec.OwnerUID == sessionUID {
			return store.ConflictErrorf("Session UID %q still owns branch claim %q after cleanup", sessionUID, claim.Spec.ID)
		}
	}
	return nil
}

func (s *Store) validateSessionCleanupBranchClaimScope(intent store.SessionCleanupIntent) error {
	if s.branchClaimsEnabled {
		return nil
	}
	if len(intent.BranchClaims) != 0 || intent.ExpectedVerifiedBaseline != nil {
		return fmt.Errorf("%w: Session cleanup contains publication state", ErrBranchClaimAccessDisabled)
	}
	return nil
}

func (s *Store) reclaimSessionLease(ctx context.Context, intent store.SessionCleanupIntent) error {
	if intent.SessionUID == "" || intent.LeaseName == "" {
		return nil
	}
	lease := &coordinationv1.Lease{}
	key := client.ObjectKey{Namespace: intent.Namespace, Name: intent.LeaseName}
	if err := s.readClient().Get(ctx, key, lease); apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return mapKubernetesError("get Session cleanup Lease", err)
	}
	if intent.LeaseObjectUID != "" && string(lease.UID) != intent.LeaseObjectUID {
		return store.ConflictErrorf("session %s/%s Lease was recreated during cleanup", intent.Namespace, intent.SessionName)
	}
	state, err := sessionLeaseFromObject(lease, intent.SessionName, intent.SessionUID)
	if err != nil {
		return err
	}
	if (lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "") ||
		state.Mode != leaseModeEmpty || state.Generation != intent.ExpectedLeaseGeneration {
		return store.ConflictErrorf("session %s/%s Lease is active or changed during cleanup", intent.Namespace, intent.SessionName)
	}
	if err := deleteObjectWithExactPreconditions(ctx, s.client, lease); err != nil && !apierrors.IsNotFound(err) {
		return mapKubernetesError("delete Session cleanup Lease", err)
	}
	fresh := &coordinationv1.Lease{}
	if err := s.readClient().Get(ctx, key, fresh); err == nil {
		return store.ConflictErrorf("session %s/%s Lease still exists after cleanup", intent.Namespace, intent.SessionName)
	} else if !apierrors.IsNotFound(err) {
		return mapKubernetesError("verify Session cleanup Lease deletion", err)
	}
	return nil
}

func (s *Store) reclaimSessionControl(ctx context.Context, intent store.SessionCleanupIntent) error {
	if intent.SessionUID == "" {
		return nil
	}
	object, err := s.getSessionControlObject(ctx, intent.Namespace, intent.SessionName)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if intent.ControlRequestDigest == "" {
		return store.ConflictErrorf("session %s/%s RuntimeSessionControl appeared after UID-only cleanup preparation", intent.Namespace, intent.SessionName)
	}
	if intent.ControlObjectUID != "" && string(object.UID) != intent.ControlObjectUID {
		return store.ConflictErrorf("session %s/%s control was recreated during cleanup", intent.Namespace, intent.SessionName)
	}
	control := sessionControlFromObject(object)
	if control.SessionUID != intent.SessionUID || control.RequestDigest != intent.ControlRequestDigest ||
		control.Version != intent.ExpectedControlVersion || control.LeaseGeneration != intent.ExpectedLeaseGeneration ||
		control.LastOperationID != intent.ExpectedControlLastOperationID || control.LastOperationDigest != intent.ExpectedControlLastDigest ||
		!reflect.DeepEqual(control.VerifiedBaseline, intent.ExpectedVerifiedBaseline) {
		return store.ConflictErrorf("session %s/%s control no longer matches its cleanup fence", intent.Namespace, intent.SessionName)
	}
	if control.Availability != store.SessionAvailable || control.Lease != nil || control.BlockedReason != "" ||
		control.RelatedPromptAttemptID != "" || control.RelatedPublicationID != "" {
		return store.ConflictErrorf("session %s/%s control no longer matches its cleanup fence", intent.Namespace, intent.SessionName)
	}
	if err := deleteObjectWithExactPreconditions(ctx, s.client, object); err != nil && !apierrors.IsNotFound(err) {
		return mapKubernetesError("delete RuntimeSessionControl", err)
	}
	if _, err := s.getSessionControlObject(ctx, intent.Namespace, intent.SessionName); !errors.Is(err, store.ErrNotFound) {
		if err != nil {
			return err
		}
		return store.ConflictErrorf("session %s/%s control still exists after cleanup", intent.Namespace, intent.SessionName)
	}
	return nil
}

func (s *Store) ensureNoSessionLease(ctx context.Context, intent store.SessionCleanupIntent) error {
	if intent.SessionUID == "" {
		return nil
	}
	lease := &coordinationv1.Lease{}
	key := client.ObjectKey{Namespace: intent.Namespace, Name: runtimeSessionLeaseName(intent.SessionUID)}
	if err := s.readClient().Get(ctx, key, lease); err == nil {
		return store.ConflictErrorf("session %s/%s coordination Lease still exists after cleanup", intent.Namespace, intent.SessionName)
	} else if !apierrors.IsNotFound(err) {
		return mapKubernetesError("verify Session cleanup Lease absence", err)
	}
	return nil
}

func (s *Store) sessionCleanupPending(ctx context.Context, namespace, sessionName string) (bool, error) {
	if s.sessionCleanup == nil {
		return false, nil
	}
	pending, err := s.sessionCleanup.HasSessionCleanupIntent(ctx, namespace, sessionName)
	if err != nil || pending {
		return pending, err
	}
	if _, err := s.sessionCleanup.GetSessionCleanupCompletion(ctx, namespace, sessionName); err == nil {
		return true, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return false, err
	}
	return false, nil
}

func (s *Store) sessionCleanupFencedForUID(ctx context.Context, sessionUID string) (bool, error) {
	if s.sessionCleanup == nil {
		return false, nil
	}
	return s.sessionCleanup.HasSessionCleanupFenceForUID(ctx, sessionUID)
}

func normalizeReclaimSessionRequest(request *store.ReclaimSessionRequest) error {
	namespace := strings.TrimSpace(request.Namespace)
	if request.Namespace != namespace {
		return store.ValidationErrorf("session namespace must not contain surrounding whitespace")
	}
	sessionName := strings.TrimSpace(request.SessionName)
	if request.SessionName != sessionName {
		return store.ValidationErrorf("session name must not contain surrounding whitespace")
	}
	request.Namespace = namespace
	request.SessionName = sessionName
	request.OperationID = strings.TrimSpace(request.OperationID)
	request.OperationDigest = strings.TrimSpace(request.OperationDigest)
	for field, value := range map[string]string{
		"session namespace": request.Namespace, "session name": request.SessionName,
		"session cleanup operation ID": request.OperationID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return err
		}
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return err
	}
	request.Fence = fence
	if err := store.ValidateCanonicalDigest("session cleanup operation digest", request.OperationDigest); err != nil {
		return err
	}
	if request.RequestedAt.IsZero() {
		request.RequestedAt = time.Now().UTC()
	} else {
		request.RequestedAt = request.RequestedAt.UTC()
	}
	return nil
}

func deleteObjectWithExactPreconditions(ctx context.Context, kubeClient client.Client, object client.Object) error {
	uid := object.GetUID()
	resourceVersion := object.GetResourceVersion()
	return kubeClient.Delete(ctx, object, &client.DeleteOptions{Preconditions: &metav1.Preconditions{
		UID: &uid, ResourceVersion: &resourceVersion,
	}})
}

func cloneVerifiedBranchBaseline(value *store.VerifiedBranchBaseline) *store.VerifiedBranchBaseline {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}
