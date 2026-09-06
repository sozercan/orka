package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

const (
	annotationControllerEpochInitializationHolder    = "core.orka.ai/controller-epoch-initialization-holder"
	annotationControllerEpochInitializationLeaseName = "core.orka.ai/controller-epoch-initialization-lease-name"
	annotationControllerEpochLeaseUID                = "core.orka.ai/controller-epoch-lease-uid"
	annotationControllerEpochPredecessorDigest       = "core.orka.ai/controller-epoch-predecessor-digest"
)

var errControllerEpochLeaseSnapshotChanged = errors.New("controller epoch Lease snapshot changed")

// GetControllerEpoch reads the authoritative controller-epoch Lease.
func (s *Store) GetControllerEpoch(ctx context.Context, name string) (*store.ControllerEpoch, error) {
	if err := s.requireClient(); err != nil {
		return nil, err
	}
	normalized, err := store.NormalizeControllerEpochName(name)
	if err != nil {
		return nil, err
	}
	var result store.ControllerEpoch
	err = retry.OnError(retry.DefaultBackoff, func(retryErr error) bool {
		return errors.Is(retryErr, errControllerEpochLeaseSnapshotChanged)
	}, func() error {
		lease := &coordinationv1.Lease{}
		key := types.NamespacedName{Namespace: s.controlNamespace, Name: controllerEpochLeaseName(normalized)}
		if getErr := s.readClient().Get(ctx, key, lease); getErr != nil {
			return mapKubernetesError("get controller epoch Lease", getErr)
		}
		current, currentErr := controllerEpochFromLease(normalized, lease)
		if currentErr != nil {
			return currentErr
		}
		predecessorDigest, digestErr := controllerEpochPredecessorDigest(lease)
		if digestErr != nil {
			return digestErr
		}
		if syncErr := s.syncControllerEpochStatus(ctx, current, lease, predecessorDigest); syncErr != nil {
			return syncErr
		}
		result = current
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// GetControllerEpochFence reads and validates the current authoritative fence
// without synchronizing the inspection mirror. Broker authorization paths use
// this read-only form because unrelated control-store mutations legitimately
// change the Lease resourceVersion while leaving its epoch authority intact.
func (s *Store) GetControllerEpochFence(ctx context.Context, name string) (store.ControllerEpochFence, error) {
	if err := s.requireClient(); err != nil {
		return store.ControllerEpochFence{}, err
	}
	normalized, err := store.NormalizeControllerEpochName(name)
	if err != nil {
		return store.ControllerEpochFence{}, err
	}
	lease := &coordinationv1.Lease{}
	key := types.NamespacedName{Namespace: s.controlNamespace, Name: controllerEpochLeaseName(normalized)}
	if err := s.readClient().Get(ctx, key, lease); err != nil {
		return store.ControllerEpochFence{}, mapKubernetesError("get controller epoch fence Lease", err)
	}
	epoch, err := controllerEpochFromLease(normalized, lease)
	if err != nil {
		return store.ControllerEpochFence{}, err
	}
	object, err := s.getControllerEpochObject(ctx, normalized)
	if errors.Is(err, store.ErrNotFound) {
		return store.ControllerEpochFence{}, store.ConflictErrorf(
			"controller epoch object %q is missing while authoritative Lease %q exists",
			controllerEpochObjectName(normalized), lease.Name,
		)
	}
	if err != nil {
		return store.ControllerEpochFence{}, err
	}
	if err := validateControllerEpochObjectLeaseUID(object, lease); err != nil {
		return store.ControllerEpochFence{}, err
	}
	if err := validateControllerEpochObjectForLease(object, epoch, lease.Name); err != nil {
		return store.ControllerEpochFence{}, err
	}
	latestLease := &coordinationv1.Lease{}
	if err := s.readClient().Get(ctx, key, latestLease); err != nil {
		return store.ControllerEpochFence{}, mapKubernetesError("revalidate controller epoch fence Lease", err)
	}
	latestEpoch, err := controllerEpochFromLease(normalized, latestLease)
	if err != nil {
		return store.ControllerEpochFence{}, err
	}
	if latestLease.UID != lease.UID || latestEpoch != epoch {
		return store.ControllerEpochFence{}, store.ConflictErrorf("controller epoch authority changed during fence read")
	}
	return store.ControllerEpochFence{Name: epoch.Name, Epoch: epoch.Epoch, HolderID: epoch.HolderID}, nil
}

// CompareAndSwapControllerEpoch creates or advances the authoritative Lease.
// A same-holder, same-digest retry of an already committed target epoch is
// idempotent even when the caller's expected values are stale.
//
//nolint:gocyclo // Creation, idempotency, mutation-lock, and epoch CAS checks form one boundary.
func (s *Store) CompareAndSwapControllerEpoch(ctx context.Context, change store.ControllerEpochCAS) (*store.ControllerEpoch, error) {
	if err := s.requireClient(); err != nil {
		return nil, err
	}
	name, err := store.NormalizeControllerEpochName(change.Name)
	if err != nil {
		return nil, err
	}
	change.Name = name
	change.HolderID = strings.TrimSpace(change.HolderID)
	if err := store.ValidateControlIdentifier("controller epoch holder ID", change.HolderID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("controller epoch request digest", change.RequestDigest); err != nil {
		return nil, err
	}
	if change.ExpectedVersion < 0 || change.ExpectedEpoch < 0 {
		return nil, store.ValidationErrorf("controller epoch expected version and epoch must not be negative")
	}
	if change.NewEpoch < 1 {
		return nil, store.ValidationErrorf("controller new epoch must be at least 1")
	}
	change.UpdatedAt = store.NormalizeControlTime(change.UpdatedAt)

	key := types.NamespacedName{Namespace: s.controlNamespace, Name: controllerEpochLeaseName(change.Name)}
	lease := &coordinationv1.Lease{}
	err = s.readClient().Get(ctx, key, lease)
	if apierrors.IsNotFound(err) {
		if change.ExpectedVersion != 0 || change.ExpectedEpoch != 0 || change.NewEpoch != 1 {
			return nil, store.ConflictErrorf("controller epoch %q does not exist; creation requires expected version/epoch 0 and new epoch 1", change.Name)
		}
		// Creating the inspection object is the API-server-serialized proof that
		// this is a genuinely fresh control store. A pre-existing object, even
		// with blank status, may be a restored authority whose Lease has not been
		// applied yet; it must never authorize epoch reuse.
		if objectErr := s.createFreshControllerEpochObject(ctx, change); objectErr != nil {
			return nil, objectErr
		}
		createdLease := newControllerEpochLease(s.controlNamespace, change)
		if createErr := s.client.Create(ctx, createdLease); createErr != nil {
			if apierrors.IsAlreadyExists(createErr) {
				return s.CompareAndSwapControllerEpoch(ctx, change)
			}
			return nil, mapKubernetesError("create controller epoch Lease", createErr)
		}
		result, parseErr := controllerEpochFromLease(change.Name, createdLease)
		if parseErr != nil {
			return nil, parseErr
		}
		if syncErr := s.syncControllerEpochStatus(ctx, result, createdLease, ""); syncErr != nil {
			return nil, syncErr
		}
		return &result, nil
	}
	if err != nil {
		return nil, mapKubernetesError("get controller epoch Lease", err)
	}

	current, err := controllerEpochFromLease(change.Name, lease)
	if err != nil {
		return nil, err
	}
	predecessorDigest, err := controllerEpochPredecessorDigest(lease)
	if err != nil {
		return nil, err
	}
	// Reconcile an authenticated prior CAS crash tail, consume the one-shot
	// epoch-1 initialization marker, and establish the immutable Lease UID
	// binding before either returning an idempotent target or advancing again.
	if syncErr := s.syncControllerEpochStatus(ctx, current, lease, predecessorDigest); syncErr != nil {
		return nil, syncErr
	}
	if current.Epoch == change.NewEpoch {
		if current.HolderID != change.HolderID || current.RequestDigest != change.RequestDigest {
			return nil, store.ConflictErrorf("controller epoch %q target %d already exists with different holder or digest", change.Name, change.NewEpoch)
		}
		return &current, nil
	}
	if current.Version != change.ExpectedVersion || current.Epoch != change.ExpectedEpoch {
		return nil, store.ConflictErrorf("controller epoch %q is version %d epoch %d, expected version %d epoch %d", change.Name, current.Version, current.Epoch, change.ExpectedVersion, change.ExpectedEpoch)
	}
	if change.NewEpoch != change.ExpectedEpoch+1 {
		return nil, store.ValidationErrorf("controller epoch must advance exactly by one: expected new epoch %d", change.ExpectedEpoch+1)
	}
	if active, expiresAt, lockErr := controllerEpochMutationLock(lease); lockErr != nil {
		return nil, lockErr
	} else if active && expiresAt.After(time.Now().UTC()) {
		return nil, store.ConflictErrorf("controller epoch %q has an active control-store mutation until %s", change.Name, expiresAt.Format(time.RFC3339Nano))
	}
	object, err := s.getControllerEpochObject(ctx, change.Name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("controller epoch object %q is missing while authoritative Lease %q exists", controllerEpochObjectName(change.Name), lease.Name)
	}
	if err != nil {
		return nil, err
	}
	if err := validateControllerEpochObjectLeaseUID(object, lease); err != nil {
		return nil, err
	}
	if err := validateControllerEpochObjectForLease(object, current, lease.Name); err != nil {
		return nil, err
	}
	preCASMirrorDigest, err := controllerEpochMirrorDigest(object)
	if err != nil {
		return nil, err
	}

	updated := lease.DeepCopy()
	setControllerEpochLease(updated, change, current.Version+1, preCASMirrorDigest)
	if err := s.client.Update(ctx, updated); err != nil {
		return nil, mapKubernetesError("compare-and-swap controller epoch Lease", err)
	}
	result, err := controllerEpochFromLease(change.Name, updated)
	if err != nil {
		return nil, err
	}
	if err := s.syncControllerEpochStatus(ctx, result, updated, preCASMirrorDigest); err != nil {
		return nil, err
	}
	return &result, nil
}

func controllerEpochLeaseName(name string) string {
	return objectName(controllerEpochLeasePrefix, name)
}

func controllerEpochObjectName(name string) string {
	return objectName(controllerEpochNamePrefix, name)
}

func newControllerEpochLease(namespace string, change store.ControllerEpochCAS) *coordinationv1.Lease {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      controllerEpochLeaseName(change.Name),
			Labels:    controlLabels(change.Name),
		},
	}
	setControllerEpochLease(lease, change, 1, "")
	return lease
}

func setControllerEpochLease(lease *coordinationv1.Lease, change store.ControllerEpochCAS, version int64, predecessorDigest string) {
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[annotationLogicalName] = change.Name
	lease.Annotations[annotationControllerEpoch] = strconv.FormatInt(change.NewEpoch, 10)
	lease.Annotations[annotationDomainVersion] = strconv.FormatInt(version, 10)
	lease.Annotations[annotationRequestDigest] = change.RequestDigest
	lease.Annotations[annotationAcquiredAt] = formatTime(change.UpdatedAt)
	if predecessorDigest == "" {
		delete(lease.Annotations, annotationControllerEpochPredecessorDigest)
	} else {
		lease.Annotations[annotationControllerEpochPredecessorDigest] = predecessorDigest
	}
	delete(lease.Annotations, annotationMutationToken)
	delete(lease.Annotations, annotationMutationExpiresAt)
	holder := change.HolderID
	acquired := metav1.NewMicroTime(change.UpdatedAt)
	lease.Spec.HolderIdentity = &holder
	lease.Spec.AcquireTime = &acquired
	lease.Spec.RenewTime = &acquired
}

func controllerEpochFromLease(name string, lease *coordinationv1.Lease) (store.ControllerEpoch, error) {
	if lease == nil || lease.Spec.HolderIdentity == nil || strings.TrimSpace(*lease.Spec.HolderIdentity) == "" {
		return store.ControllerEpoch{}, fmt.Errorf("controller epoch Lease %q has no holder", controllerEpochLeaseName(name))
	}
	if _, err := controllerEpochLeaseUID(lease); err != nil {
		return store.ControllerEpoch{}, err
	}
	holderID := strings.TrimSpace(*lease.Spec.HolderIdentity)
	if holderID != *lease.Spec.HolderIdentity {
		return store.ControllerEpoch{}, fmt.Errorf("controller epoch Lease %q holder is not canonical", controllerEpochLeaseName(name))
	}
	if err := store.ValidateControlIdentifier("controller epoch Lease holder ID", holderID); err != nil {
		return store.ControllerEpoch{}, fmt.Errorf("invalid controller epoch Lease: %w", err)
	}
	annotations := lease.Annotations
	if annotations[annotationLogicalName] != name {
		return store.ControllerEpoch{}, fmt.Errorf("controller epoch Lease %q logical name mismatch", lease.Name)
	}
	epoch, err := parsePositiveInt64("controller epoch", annotations[annotationControllerEpoch])
	if err != nil {
		return store.ControllerEpoch{}, err
	}
	version, err := parsePositiveInt64("controller epoch version", annotations[annotationDomainVersion])
	if err != nil {
		return store.ControllerEpoch{}, err
	}
	if version != epoch {
		return store.ControllerEpoch{}, fmt.Errorf(
			"controller epoch Lease %q version %d does not match epoch %d",
			controllerEpochLeaseName(name), version, epoch,
		)
	}
	requestDigest := annotations[annotationRequestDigest]
	if err := store.ValidateCanonicalDigest("controller epoch request digest", requestDigest); err != nil {
		return store.ControllerEpoch{}, fmt.Errorf("invalid controller epoch Lease: %w", err)
	}
	if _, err := controllerEpochPredecessorDigest(lease); err != nil {
		return store.ControllerEpoch{}, err
	}
	acquiredAt, err := parseTime("controller epoch acquisition time", annotations[annotationAcquiredAt])
	if err != nil {
		return store.ControllerEpoch{}, err
	}
	return store.ControllerEpoch{
		Name:          name,
		Epoch:         epoch,
		HolderID:      holderID,
		RequestDigest: requestDigest,
		Version:       version,
		AcquiredAt:    acquiredAt,
		UpdatedAt:     acquiredAt,
	}, nil
}

func controllerEpochLeaseUID(lease *coordinationv1.Lease) (string, error) {
	if lease == nil {
		return "", fmt.Errorf("controller epoch Lease is required")
	}
	uid := string(lease.UID)
	if uid == "" || strings.TrimSpace(uid) != uid {
		return "", fmt.Errorf("controller epoch Lease %q has no canonical UID", lease.Name)
	}
	return uid, nil
}

func controllerEpochPredecessorDigest(lease *coordinationv1.Lease) (string, error) {
	if lease == nil {
		return "", fmt.Errorf("controller epoch Lease is required")
	}
	raw := lease.Annotations[annotationControllerEpochPredecessorDigest]
	digest := strings.TrimSpace(raw)
	if raw != digest {
		return "", fmt.Errorf("controller epoch Lease %q predecessor digest is not canonical", lease.Name)
	}
	if digest == "" {
		return "", nil
	}
	if err := store.ValidateCanonicalDigest("controller epoch predecessor digest", digest); err != nil {
		return "", fmt.Errorf("invalid controller epoch Lease: %w", err)
	}
	return digest, nil
}

func (s *Store) getControllerEpochObject(ctx context.Context, name string) (*corev1alpha1.ControllerEpoch, error) {
	key := types.NamespacedName{Namespace: s.controlNamespace, Name: controllerEpochObjectName(name)}
	object := &corev1alpha1.ControllerEpoch{}
	if err := s.readClient().Get(ctx, key, object); err != nil {
		return nil, mapKubernetesError("get controller epoch object", err)
	}
	if object.Spec.Name != name {
		return nil, store.ConflictErrorf("controller epoch object %s/%s has a different logical name", key.Namespace, key.Name)
	}
	return object, nil
}

func (s *Store) createFreshControllerEpochObject(ctx context.Context, change store.ControllerEpochCAS) error {
	name := change.Name
	object := &corev1alpha1.ControllerEpoch{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: s.controlNamespace,
			Name:      controllerEpochObjectName(name),
			Labels:    controlLabels(name),
			Annotations: map[string]string{
				annotationLogicalName:                            name,
				annotationControllerEpoch:                        "1",
				annotationDomainVersion:                          "1",
				annotationRequestDigest:                          change.RequestDigest,
				annotationAcquiredAt:                             formatTime(change.UpdatedAt),
				annotationControllerEpochInitializationHolder:    change.HolderID,
				annotationControllerEpochInitializationLeaseName: controllerEpochLeaseName(name),
			},
		},
		Spec: corev1alpha1.ControllerEpochSpec{Name: name},
	}
	if err := s.client.Create(ctx, object); err == nil {
		return nil
	} else if !apierrors.IsAlreadyExists(err) {
		return mapKubernetesError("create fresh controller epoch object", err)
	}

	existing, getErr := s.getControllerEpochObject(ctx, name)
	if getErr != nil {
		return getErr
	}
	return fmt.Errorf(
		"controller epoch %q authoritative Lease is missing while ControllerEpoch object already exists (epoch=%d version=%d holder=%q)",
		name, existing.Status.Epoch, existing.Status.Version, existing.Status.HolderID,
	)
}

func controllerEpochStatusIsEmpty(status corev1alpha1.ControllerEpochStatus) bool {
	return status.Epoch == 0 && status.Version == 0 && strings.TrimSpace(status.HolderID) == "" &&
		strings.TrimSpace(status.RequestDigest) == "" && status.AcquiredAt == nil && status.UpdatedAt == nil &&
		strings.TrimSpace(status.LeaseName) == "" && strings.TrimSpace(status.LeaseResourceVersion) == ""
}

func controllerEpochInitializationMarkerPresent(object *corev1alpha1.ControllerEpoch) bool {
	if object == nil || object.Annotations == nil {
		return false
	}
	_, holderPresent := object.Annotations[annotationControllerEpochInitializationHolder]
	_, leaseNamePresent := object.Annotations[annotationControllerEpochInitializationLeaseName]
	return holderPresent || leaseNamePresent
}

func validateControllerEpochObjectForLease(
	object *corev1alpha1.ControllerEpoch,
	epoch store.ControllerEpoch,
	leaseName string,
) error {
	if object == nil {
		return fmt.Errorf("controller epoch object is required for authoritative Lease %q", leaseName)
	}
	status := object.Status
	if controllerEpochStatusIsEmpty(status) {
		return validateControllerEpochInitializationMarker(object, epoch, leaseName)
	}
	if status.Epoch < 1 || status.Version < 1 || status.Epoch != status.Version {
		return fmt.Errorf(
			"controller epoch object %s/%s has invalid mirrored epoch/version %d/%d",
			object.Namespace, object.Name, status.Epoch, status.Version,
		)
	}
	if err := store.ValidateControlIdentifier("controller epoch status holder ID", status.HolderID); err != nil {
		return fmt.Errorf("invalid controller epoch status: %w", err)
	}
	if err := store.ValidateCanonicalDigest("controller epoch status request digest", status.RequestDigest); err != nil {
		return fmt.Errorf("invalid controller epoch status: %w", err)
	}
	if status.AcquiredAt == nil || status.AcquiredAt.IsZero() || status.UpdatedAt == nil || status.UpdatedAt.IsZero() {
		return fmt.Errorf("controller epoch object %s/%s has incomplete mirrored timestamps", object.Namespace, object.Name)
	}
	if status.LeaseName != leaseName || strings.TrimSpace(status.LeaseResourceVersion) == "" {
		return fmt.Errorf(
			"controller epoch object %s/%s does not identify authoritative Lease %q",
			object.Namespace, object.Name, leaseName,
		)
	}
	if status.Epoch != epoch.Epoch || status.Version != epoch.Version {
		return fmt.Errorf(
			"controller epoch object %s/%s epoch/version %d/%d conflicts with authoritative Lease epoch/version %d/%d",
			object.Namespace, object.Name, status.Epoch, status.Version, epoch.Epoch, epoch.Version,
		)
	}
	if status.HolderID != epoch.HolderID {
		return fmt.Errorf(
			"controller epoch object %s/%s holder %q conflicts with authoritative Lease holder %q at epoch %d",
			object.Namespace, object.Name, status.HolderID, epoch.HolderID, epoch.Epoch,
		)
	}
	if status.RequestDigest != epoch.RequestDigest {
		return fmt.Errorf(
			"controller epoch object %s/%s request digest conflicts with authoritative Lease at epoch %d",
			object.Namespace, object.Name, epoch.Epoch,
		)
	}
	if !status.AcquiredAt.Time.Truncate(time.Second).Equal(epoch.AcquiredAt.Truncate(time.Second)) {
		return fmt.Errorf(
			"controller epoch object %s/%s acquisition time conflicts with authoritative Lease at epoch %d",
			object.Namespace, object.Name, epoch.Epoch,
		)
	}
	if !status.UpdatedAt.Time.Truncate(time.Second).Equal(epoch.UpdatedAt.Truncate(time.Second)) {
		return fmt.Errorf(
			"controller epoch object %s/%s update time conflicts with authoritative Lease at epoch %d",
			object.Namespace, object.Name, epoch.Epoch,
		)
	}
	if controllerEpochInitializationMarkerPresent(object) {
		return validateControllerEpochInitializationMarker(object, epoch, leaseName)
	}
	return nil
}

func validateControllerEpochInitializationMarker(
	object *corev1alpha1.ControllerEpoch,
	epoch store.ControllerEpoch,
	leaseName string,
) error {
	annotations := object.Annotations
	if epoch.Epoch != 1 || epoch.Version != 1 || annotations[annotationLogicalName] != epoch.Name ||
		annotations[annotationControllerEpochInitializationLeaseName] != leaseName {
		return fmt.Errorf(
			"controller epoch object %s/%s has a blank status without an exact epoch-1 initialization marker",
			object.Namespace, object.Name,
		)
	}
	markedEpoch, err := parsePositiveInt64("controller epoch initialization epoch", annotations[annotationControllerEpoch])
	if err != nil || markedEpoch != epoch.Epoch {
		return fmt.Errorf("controller epoch object %s/%s has an invalid initialization epoch", object.Namespace, object.Name)
	}
	markedVersion, err := parsePositiveInt64("controller epoch initialization version", annotations[annotationDomainVersion])
	if err != nil || markedVersion != epoch.Version {
		return fmt.Errorf("controller epoch object %s/%s has an invalid initialization version", object.Namespace, object.Name)
	}
	markedHolder := annotations[annotationControllerEpochInitializationHolder]
	if err := store.ValidateControlIdentifier("controller epoch initialization holder ID", markedHolder); err != nil || markedHolder != epoch.HolderID {
		return fmt.Errorf("controller epoch object %s/%s has an invalid initialization holder", object.Namespace, object.Name)
	}
	markedDigest := annotations[annotationRequestDigest]
	if err := store.ValidateCanonicalDigest("controller epoch initialization request digest", markedDigest); err != nil || markedDigest != epoch.RequestDigest {
		return fmt.Errorf("controller epoch object %s/%s has an invalid initialization request digest", object.Namespace, object.Name)
	}
	markedAt, err := parseTime("controller epoch initialization acquisition time", annotations[annotationAcquiredAt])
	if err != nil || !markedAt.Equal(epoch.AcquiredAt) {
		return fmt.Errorf("controller epoch object %s/%s has an invalid initialization acquisition time", object.Namespace, object.Name)
	}
	return nil
}

func validateControllerEpochObjectLeaseUID(object *corev1alpha1.ControllerEpoch, lease *coordinationv1.Lease) error {
	if object == nil {
		return fmt.Errorf("controller epoch object is required for Lease UID validation")
	}
	leaseUID, err := controllerEpochLeaseUID(lease)
	if err != nil {
		return err
	}
	raw, present := object.Annotations[annotationControllerEpochLeaseUID]
	boundUID := strings.TrimSpace(raw)
	if !present || boundUID == "" || raw != boundUID {
		return store.ConflictErrorf("controller epoch object %s/%s has no immutable authoritative Lease UID binding", object.Namespace, object.Name)
	}
	if boundUID != leaseUID {
		return store.ConflictErrorf(
			"controller epoch object %s/%s is bound to Lease UID %q, not current UID %q",
			object.Namespace, object.Name, boundUID, leaseUID,
		)
	}
	return nil
}

func controllerEpochMirrorDigest(object *corev1alpha1.ControllerEpoch) (string, error) {
	if object == nil {
		return "", fmt.Errorf("controller epoch object is required for mirror digest")
	}
	leaseUID := strings.TrimSpace(object.Annotations[annotationControllerEpochLeaseUID])
	if leaseUID == "" || leaseUID != object.Annotations[annotationControllerEpochLeaseUID] {
		return "", fmt.Errorf("controller epoch object %s/%s has no canonical Lease UID binding", object.Namespace, object.Name)
	}
	status := object.Status
	if status.AcquiredAt == nil || status.UpdatedAt == nil {
		return "", fmt.Errorf("controller epoch object %s/%s has incomplete mirrored timestamps", object.Namespace, object.Name)
	}
	payload, err := json.Marshal(struct {
		Domain               string `json:"domain"`
		Namespace            string `json:"namespace"`
		ObjectName           string `json:"objectName"`
		LogicalName          string `json:"logicalName"`
		LeaseUID             string `json:"leaseUid"`
		Epoch                int64  `json:"epoch"`
		HolderID             string `json:"holderId"`
		RequestDigest        string `json:"requestDigest"`
		Version              int64  `json:"version"`
		AcquiredAt           string `json:"acquiredAt"`
		UpdatedAt            string `json:"updatedAt"`
		LeaseName            string `json:"leaseName"`
		LeaseResourceVersion string `json:"leaseResourceVersion"`
	}{
		Domain: "orka-controller-epoch-mirror-v1", Namespace: object.Namespace, ObjectName: object.Name,
		LogicalName: object.Spec.Name, LeaseUID: leaseUID, Epoch: status.Epoch, HolderID: status.HolderID,
		RequestDigest: status.RequestDigest, Version: status.Version,
		AcquiredAt: formatTime(status.AcquiredAt.Time), UpdatedAt: formatTime(status.UpdatedAt.Time),
		LeaseName: status.LeaseName, LeaseResourceVersion: status.LeaseResourceVersion,
	})
	if err != nil {
		return "", fmt.Errorf("encode controller epoch mirror digest: %w", err)
	}
	return store.CanonicalBytesDigest(payload), nil
}

func validateControllerEpochLeaseSnapshot(
	ctx context.Context,
	s *Store,
	epoch store.ControllerEpoch,
	snapshot *coordinationv1.Lease,
	predecessorDigest string,
) (*coordinationv1.Lease, error) {
	if snapshot == nil || snapshot.Name != controllerEpochLeaseName(epoch.Name) || strings.TrimSpace(snapshot.ResourceVersion) == "" {
		return nil, store.ConflictErrorf("controller epoch %q status sync has an invalid Lease snapshot", epoch.Name)
	}
	snapshotUID, err := controllerEpochLeaseUID(snapshot)
	if err != nil {
		return nil, err
	}
	snapshotPredecessorDigest, err := controllerEpochPredecessorDigest(snapshot)
	if err != nil {
		return nil, err
	}
	if snapshotPredecessorDigest != predecessorDigest {
		return nil, store.ConflictErrorf("controller epoch Lease %q predecessor digest changed before status sync", snapshot.Name)
	}
	lease := &coordinationv1.Lease{}
	key := types.NamespacedName{Namespace: s.controlNamespace, Name: snapshot.Name}
	if err := s.readClient().Get(ctx, key, lease); err != nil {
		return nil, mapKubernetesError("verify controller epoch Lease before status sync", err)
	}
	leaseUID, err := controllerEpochLeaseUID(lease)
	if err != nil {
		return nil, err
	}
	if leaseUID != snapshotUID {
		return nil, store.ConflictErrorf(
			"controller epoch Lease %q changed from UID/resourceVersion %q/%q to %q/%q before status sync",
			snapshot.Name, snapshotUID, snapshot.ResourceVersion, leaseUID, lease.ResourceVersion,
		)
	}
	if lease.ResourceVersion != snapshot.ResourceVersion {
		return nil, fmt.Errorf(
			"%w: %w: controller epoch Lease %q changed resourceVersion from %q to %q before status sync",
			store.ErrConflict, errControllerEpochLeaseSnapshotChanged,
			snapshot.Name, snapshot.ResourceVersion, lease.ResourceVersion,
		)
	}
	current, err := controllerEpochFromLease(epoch.Name, lease)
	if err != nil {
		return nil, err
	}
	currentPredecessorDigest, err := controllerEpochPredecessorDigest(lease)
	if err != nil {
		return nil, err
	}
	if currentPredecessorDigest != predecessorDigest {
		return nil, store.ConflictErrorf("controller epoch Lease %q predecessor digest no longer matches status sync", lease.Name)
	}
	if current.Epoch != epoch.Epoch || current.Version != epoch.Version || current.HolderID != epoch.HolderID ||
		current.RequestDigest != epoch.RequestDigest || !current.AcquiredAt.Equal(epoch.AcquiredAt) ||
		!current.UpdatedAt.Equal(epoch.UpdatedAt) {
		return nil, store.ConflictErrorf(
			"controller epoch Lease %q no longer matches epoch/version %d/%d holder %q before status sync",
			lease.Name, epoch.Epoch, epoch.Version, epoch.HolderID,
		)
	}
	return lease, nil
}

type controllerEpochStatusSyncDecision struct {
	writeStatus               bool
	bindLeaseUID              bool
	clearInitializationMarker bool
}

func validateControllerEpochLeaseUIDBinding(
	object *corev1alpha1.ControllerEpoch,
	epoch store.ControllerEpoch,
	lease *coordinationv1.Lease,
	predecessorDigest string,
) (bindingPresent bool, finishInitialization bool, err error) {
	leaseUID, err := controllerEpochLeaseUID(lease)
	if err != nil {
		return false, false, err
	}
	rawBoundUID, bindingPresent := object.Annotations[annotationControllerEpochLeaseUID]
	boundUID := strings.TrimSpace(rawBoundUID)
	if bindingPresent {
		if boundUID == "" || rawBoundUID != boundUID {
			return false, false, fmt.Errorf("controller epoch object %s/%s has a non-canonical Lease UID binding", object.Namespace, object.Name)
		}
		if boundUID != leaseUID {
			return false, false, store.ConflictErrorf(
				"controller epoch object %s/%s is bound to Lease UID %q, not current UID %q",
				object.Namespace, object.Name, boundUID, leaseUID,
			)
		}
		return true, false, nil
	}
	if controllerEpochStatusIsEmpty(object.Status) {
		return false, false, nil
	}
	if !controllerEpochInitializationMarkerPresent(object) {
		return false, false, store.ConflictErrorf(
			"controller epoch object %s/%s has no immutable authoritative Lease UID binding",
			object.Namespace, object.Name,
		)
	}
	if predecessorDigest != "" {
		return false, false, store.ConflictErrorf(
			"controller epoch object %s/%s initialization marker cannot consume a predecessor digest",
			object.Namespace, object.Name,
		)
	}
	active, expiresAt, lockErr := controllerEpochMutationLock(lease)
	if lockErr != nil {
		return false, false, lockErr
	}
	if active && expiresAt.After(time.Now().UTC()) {
		return false, false, store.ConflictErrorf(
			"controller epoch object %s/%s cannot finish epoch-1 Lease UID binding while a control-store mutation is active until %s",
			object.Namespace, object.Name, expiresAt.Format(time.RFC3339Nano),
		)
	}
	if err := validateControllerEpochObjectForLease(object, epoch, lease.Name); err != nil {
		return false, false, err
	}
	return false, true, nil
}

func validateControllerEpochStatusSyncState(
	object *corev1alpha1.ControllerEpoch,
	epoch store.ControllerEpoch,
	lease *coordinationv1.Lease,
	predecessorDigest string,
) (controllerEpochStatusSyncDecision, error) {
	decision := controllerEpochStatusSyncDecision{}
	if object == nil {
		return decision, fmt.Errorf("controller epoch object is required for status sync")
	}
	bindingPresent, finishInitialization, err := validateControllerEpochLeaseUIDBinding(object, epoch, lease, predecessorDigest)
	if err != nil {
		return decision, err
	}

	status := object.Status
	if controllerEpochStatusIsEmpty(status) {
		if predecessorDigest != "" {
			return decision, store.ConflictErrorf("controller epoch %q blank mirror cannot consume a predecessor digest", epoch.Name)
		}
		if err := validateControllerEpochInitializationMarker(object, epoch, lease.Name); err != nil {
			return decision, err
		}
		decision.writeStatus = true
		decision.bindLeaseUID = !bindingPresent
		decision.clearInitializationMarker = true
		return decision, nil
	}

	if finishInitialization {
		decision.bindLeaseUID = true
		decision.clearInitializationMarker = true
	}

	if status.Epoch == epoch.Epoch && status.Version == epoch.Version {
		if err := validateControllerEpochObjectForLease(object, epoch, lease.Name); err != nil {
			return decision, err
		}
		decision.writeStatus = status.LeaseResourceVersion != lease.ResourceVersion
		decision.clearInitializationMarker = controllerEpochInitializationMarkerPresent(object)
		return decision, nil
	}
	if status.Epoch > epoch.Epoch || status.Version > epoch.Version {
		return decision, store.ConflictErrorf(
			"controller epoch object %s/%s mirror %d/%d advanced beyond stale status sync target %d/%d",
			object.Namespace, object.Name, status.Epoch, status.Version, epoch.Epoch, epoch.Version,
		)
	}
	if status.Epoch != epoch.Epoch-1 || status.Version != epoch.Version-1 {
		return decision, store.ConflictErrorf(
			"controller epoch object %s/%s mirror %d/%d is not the exact predecessor of status sync target %d/%d",
			object.Namespace, object.Name, status.Epoch, status.Version, epoch.Epoch, epoch.Version,
		)
	}
	if controllerEpochInitializationMarkerPresent(object) {
		return decision, store.ConflictErrorf(
			"controller epoch object %s/%s still has an initialization marker before advancing mirror %d/%d",
			object.Namespace, object.Name, epoch.Epoch, epoch.Version,
		)
	}
	if predecessorDigest == "" {
		return decision, store.ConflictErrorf(
			"controller epoch Lease %q has no authenticated predecessor digest for mirror %d/%d",
			lease.Name, status.Epoch, status.Version,
		)
	}
	if status.AcquiredAt == nil || status.AcquiredAt.IsZero() || status.UpdatedAt == nil || status.UpdatedAt.IsZero() {
		return decision, fmt.Errorf(
			"controller epoch object %s/%s predecessor mirror has incomplete timestamps",
			object.Namespace, object.Name,
		)
	}
	previous := store.ControllerEpoch{
		Name: epoch.Name, Epoch: status.Epoch, HolderID: status.HolderID,
		RequestDigest: status.RequestDigest, Version: status.Version,
		AcquiredAt: status.AcquiredAt.Time, UpdatedAt: status.UpdatedAt.Time,
	}
	if err := validateControllerEpochObjectForLease(object, previous, lease.Name); err != nil {
		return decision, fmt.Errorf("invalid predecessor controller epoch mirror: %w", err)
	}
	actualPredecessorDigest, err := controllerEpochMirrorDigest(object)
	if err != nil {
		return decision, err
	}
	if actualPredecessorDigest != predecessorDigest {
		return decision, store.ConflictErrorf(
			"controller epoch object %s/%s predecessor mirror digest does not match authoritative Lease",
			object.Namespace, object.Name,
		)
	}
	decision.writeStatus = true
	return decision, nil
}

func (s *Store) syncControllerEpochStatus(
	ctx context.Context,
	epoch store.ControllerEpoch,
	leaseSnapshot *coordinationv1.Lease,
	predecessorDigest string,
) error {
	key := types.NamespacedName{Namespace: s.controlNamespace, Name: controllerEpochObjectName(epoch.Name)}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		lease, err := validateControllerEpochLeaseSnapshot(ctx, s, epoch, leaseSnapshot, predecessorDigest)
		if err != nil {
			return err
		}
		object := &corev1alpha1.ControllerEpoch{}
		if err := s.readClient().Get(ctx, key, object); err != nil {
			return err
		}
		if object.Spec.Name != epoch.Name {
			return store.ConflictErrorf("controller epoch object %s/%s has a different logical name", key.Namespace, key.Name)
		}
		decision, err := validateControllerEpochStatusSyncState(object, epoch, lease, predecessorDigest)
		if err != nil {
			return err
		}
		if decision.writeStatus {
			object.Status = corev1alpha1.ControllerEpochStatus{
				Epoch:                epoch.Epoch,
				HolderID:             epoch.HolderID,
				RequestDigest:        epoch.RequestDigest,
				Version:              epoch.Version,
				AcquiredAt:           metaTime(epoch.AcquiredAt),
				UpdatedAt:            metaTime(epoch.UpdatedAt),
				LeaseName:            lease.Name,
				LeaseResourceVersion: lease.ResourceVersion,
			}
			if err := s.client.Status().Update(ctx, object); err != nil {
				return err
			}
		}
		if !decision.bindLeaseUID && !decision.clearInitializationMarker {
			return nil
		}
		if _, err := validateControllerEpochLeaseSnapshot(ctx, s, epoch, leaseSnapshot, predecessorDigest); err != nil {
			return err
		}
		if object.Annotations == nil {
			object.Annotations = map[string]string{}
		}
		if decision.bindLeaseUID {
			object.Annotations[annotationControllerEpochLeaseUID] = string(lease.UID)
		}
		if decision.clearInitializationMarker {
			delete(object.Annotations, annotationControllerEpochInitializationHolder)
			delete(object.Annotations, annotationControllerEpochInitializationLeaseName)
		}
		return s.client.Update(ctx, object)
	})
}

func (s *Store) requireControllerEpoch(ctx context.Context, fence store.ControllerEpochFence) (store.ControllerEpochFence, epochSnapshot, error) {
	normalized, err := store.NormalizeEpochFence(fence)
	if err != nil {
		return store.ControllerEpochFence{}, epochSnapshot{}, err
	}
	const localQueueTimeout = 30 * time.Second
	localSlot := false
	if s.epochMutations != nil {
		queueCtx, cancel := context.WithTimeout(ctx, localQueueTimeout)
		err = s.epochMutations.Acquire(queueCtx, 1)
		cancel()
		if err != nil {
			return store.ControllerEpochFence{}, epochSnapshot{}, fmt.Errorf("wait for controller epoch mutation slot: %w", err)
		}
		localSlot = true
	}
	releaseLocalSlot := true
	defer func() {
		if releaseLocalSlot && localSlot {
			s.epochMutations.Release(1)
		}
	}()
	const (
		acquireTimeout = 2 * time.Second
		maxRetryDelay  = 200 * time.Millisecond
	)
	deadline := time.Now().Add(acquireTimeout)
	retryDelay := 10 * time.Millisecond
	var lastConflict error
	for {
		lease := &coordinationv1.Lease{}
		key := types.NamespacedName{Namespace: s.controlNamespace, Name: controllerEpochLeaseName(normalized.Name)}
		if err := s.readClient().Get(ctx, key, lease); err != nil {
			if apierrors.IsNotFound(err) {
				return store.ControllerEpochFence{}, epochSnapshot{}, store.ConflictErrorf("controller epoch %q does not exist", normalized.Name)
			}
			return store.ControllerEpochFence{}, epochSnapshot{}, mapKubernetesError("get controller epoch fence Lease", err)
		}
		current, err := controllerEpochFromLease(normalized.Name, lease)
		if err != nil {
			return store.ControllerEpochFence{}, epochSnapshot{}, err
		}
		if current.Epoch != normalized.Epoch || current.HolderID != normalized.HolderID {
			return store.ControllerEpochFence{}, epochSnapshot{}, store.ConflictErrorf("controller epoch fence %s/%d/%s does not match current %d/%s", normalized.Name, normalized.Epoch, normalized.HolderID, current.Epoch, current.HolderID)
		}
		object, err := s.getControllerEpochObject(ctx, normalized.Name)
		if errors.Is(err, store.ErrNotFound) {
			return store.ControllerEpochFence{}, epochSnapshot{}, store.ConflictErrorf(
				"controller epoch object %q is missing while authoritative Lease %q exists",
				controllerEpochObjectName(normalized.Name), lease.Name,
			)
		}
		if err != nil {
			return store.ControllerEpochFence{}, epochSnapshot{}, err
		}
		if err := validateControllerEpochObjectLeaseUID(object, lease); err != nil {
			return store.ControllerEpochFence{}, epochSnapshot{}, err
		}
		if err := validateControllerEpochObjectForLease(object, current, lease.Name); err != nil {
			return store.ControllerEpochFence{}, epochSnapshot{}, err
		}
		active, expiresAt, err := controllerEpochMutationLock(lease)
		if err != nil {
			return store.ControllerEpochFence{}, epochSnapshot{}, err
		}
		now := time.Now().UTC()
		if active && expiresAt.After(now) {
			lastConflict = store.ConflictErrorf("controller epoch %q is serializing another control-store mutation until %s", normalized.Name, expiresAt.Format(time.RFC3339Nano))
		} else {
			updated := lease.DeepCopy()
			if updated.Annotations == nil {
				updated.Annotations = map[string]string{}
			}
			token := uuid.NewString()
			updated.Annotations[annotationMutationToken] = token
			updated.Annotations[annotationMutationExpiresAt] = formatTime(now.Add(2 * time.Minute))
			if err := s.client.Update(ctx, updated); err == nil {
				releaseLocalSlot = false
				return normalized, epochSnapshot{
					Name: current.Name, Epoch: current.Epoch, HolderID: current.HolderID,
					LeaseResourceVersion: updated.ResourceVersion, MutationToken: token, MutationLease: updated.DeepCopy(),
					LocalMutationSlot: localSlot,
				}, nil
			} else if apierrors.IsConflict(err) {
				lastConflict = mapKubernetesError("acquire controller epoch mutation fence", err)
			} else {
				return store.ControllerEpochFence{}, epochSnapshot{}, mapKubernetesError("acquire controller epoch mutation fence", err)
			}
		}

		if !time.Now().Before(deadline) {
			return store.ControllerEpochFence{}, epochSnapshot{}, lastConflict
		}
		wait := retryDelay
		if remaining := time.Until(deadline); wait > remaining {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return store.ControllerEpochFence{}, epochSnapshot{}, ctx.Err()
		case <-timer.C:
		}
		retryDelay = min(retryDelay*2, maxRetryDelay)
	}
}

func controllerEpochMutationLock(lease *coordinationv1.Lease) (bool, time.Time, error) {
	if lease == nil || lease.Annotations == nil || lease.Annotations[annotationMutationToken] == "" {
		return false, time.Time{}, nil
	}
	expiresAt, err := parseTime("controller epoch mutation expiry", lease.Annotations[annotationMutationExpiresAt])
	if err != nil {
		return false, time.Time{}, err
	}
	return true, expiresAt, nil
}

func (s *Store) releaseControllerEpochMutation(snapshot epochSnapshot) {
	if snapshot.LocalMutationSlot && s.epochMutations != nil {
		defer s.epochMutations.Release(1)
	}
	if snapshot.MutationToken == "" || snapshot.MutationLease == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease := snapshot.MutationLease.DeepCopy()
	_ = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if lease.Annotations[annotationMutationToken] != snapshot.MutationToken || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != snapshot.HolderID {
			return nil
		}
		epoch, err := parsePositiveInt64("controller epoch", lease.Annotations[annotationControllerEpoch])
		if err != nil || epoch != snapshot.Epoch {
			return nil
		}
		updated := lease.DeepCopy()
		delete(updated.Annotations, annotationMutationToken)
		delete(updated.Annotations, annotationMutationExpiresAt)
		if err := s.client.Update(ctx, updated); err != nil {
			if !apierrors.IsConflict(err) {
				return err
			}
			refreshed := &coordinationv1.Lease{}
			key := types.NamespacedName{Namespace: lease.Namespace, Name: lease.Name}
			if getErr := s.readClient().Get(ctx, key, refreshed); getErr != nil {
				return getErr
			}
			lease = refreshed
			return err
		}
		return nil
	})
}
