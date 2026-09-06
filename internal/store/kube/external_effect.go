package kube

import (
	"bytes"
	"context"
	"strings"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ReserveExternalEffect creates or returns the same-digest canonical effect.
func (s *Store) ReserveExternalEffect(ctx context.Context, request store.ReserveExternalEffectRequest) (*store.ExternalEffect, error) {
	if err := s.requireClient(); err != nil {
		return nil, err
	}
	request.Identity.Kind = strings.TrimSpace(request.Identity.Kind)
	request.Identity.Namespace = strings.TrimSpace(request.Identity.Namespace)
	request.Identity.AggregateID = strings.TrimSpace(request.Identity.AggregateID)
	request.Identity.OperationID = strings.TrimSpace(request.Identity.OperationID)
	if err := request.Identity.Validate(); err != nil {
		return nil, err
	}
	id, err := request.Identity.CanonicalID()
	if err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("external effect request digest", request.RequestDigest); err != nil {
		return nil, err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, request.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	request.Fence = fence
	request.CreatedAt = store.NormalizeControlTime(request.CreatedAt)

	key := client.ObjectKey{Namespace: request.Identity.Namespace, Name: objectName(externalEffectNamePrefix, id)}
	object := &corev1alpha1.ExternalEffect{}
	err = s.readClient().Get(ctx, key, object)
	if err == nil {
		return s.completeExternalEffectCreation(ctx, object, request, id, fence, snapshot)
	}
	if !apierrors.IsNotFound(err) {
		return nil, mapKubernetesError("get external effect", err)
	}
	object = &corev1alpha1.ExternalEffect{
		ObjectMeta: metav1.ObjectMeta{Namespace: request.Identity.Namespace, Name: key.Name, Labels: controlLabels(id)},
		Spec: corev1alpha1.ExternalEffectSpec{
			ID:                id,
			Kind:              request.Identity.Kind,
			IdentityNamespace: request.Identity.Namespace,
			AggregateID:       request.Identity.AggregateID,
			OperationID:       request.Identity.OperationID,
			RequestDigest:     request.RequestDigest,
		},
	}
	if err := s.client.Create(ctx, object); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if getErr := s.readClient().Get(ctx, key, object); getErr != nil {
				return nil, mapKubernetesError("get concurrently created external effect", getErr)
			}
			return s.completeExternalEffectCreation(ctx, object, request, id, fence, snapshot)
		}
		return nil, mapKubernetesError("create external effect", err)
	}
	return s.completeExternalEffectCreation(ctx, object, request, id, fence, snapshot)
}

// GetExternalEffect returns an effect by canonical ID within the configured watch scope.
func (s *Store) GetExternalEffect(ctx context.Context, id string) (*store.ExternalEffect, error) {
	if err := s.requireClient(); err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	if err := store.ValidateControlIdentifier("external effect ID", id); err != nil {
		return nil, err
	}
	object, err := s.findExternalEffectByID(ctx, id)
	if err != nil {
		return nil, err
	}
	result := externalEffectFromObject(object)
	return &result, nil
}

// GetExternalEffectByIdentity resolves an external effect through its exact
// deterministic Kubernetes object key. Authorization paths must not depend on
// namespace-wide LIST visibility immediately after the effect lease CAS.
func (s *Store) GetExternalEffectByIdentity(ctx context.Context, identity store.ExternalEffectIdentity) (*store.ExternalEffect, error) {
	if err := s.requireClient(); err != nil {
		return nil, err
	}
	identity.Kind = strings.TrimSpace(identity.Kind)
	identity.Namespace = strings.TrimSpace(identity.Namespace)
	identity.AggregateID = strings.TrimSpace(identity.AggregateID)
	identity.OperationID = strings.TrimSpace(identity.OperationID)
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	id, err := identity.CanonicalID()
	if err != nil {
		return nil, err
	}
	object := &corev1alpha1.ExternalEffect{}
	key := client.ObjectKey{Namespace: identity.Namespace, Name: objectName(externalEffectNamePrefix, id)}
	if err := s.readClient().Get(ctx, key, object); err != nil {
		return nil, mapKubernetesError("get external effect by identity", err)
	}
	if object.Spec.ID != id || object.Spec.Kind != identity.Kind || object.Spec.IdentityNamespace != identity.Namespace ||
		object.Spec.AggregateID != identity.AggregateID || object.Spec.OperationID != identity.OperationID {
		return nil, store.ConflictErrorf("external effect %q does not match its deterministic identity", id)
	}
	result := externalEffectFromObject(object)
	return &result, nil
}

// TransitionExternalEffect applies an exact version/state/lease-owner,
// resourceVersion, request-digest, and controller-epoch CAS.
func (s *Store) TransitionExternalEffect(ctx context.Context, transition store.ExternalEffectTransition) (*store.ExternalEffect, error) {
	transition.ID = strings.TrimSpace(transition.ID)
	if err := store.ValidateControlIdentifier("external effect ID", transition.ID); err != nil {
		return nil, err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, transition.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	transition.Fence = fence
	if err := validateExternalEffectTransition(&transition); err != nil {
		return nil, err
	}

	object, err := s.findExternalEffectByID(ctx, transition.ID)
	if err != nil {
		return nil, err
	}
	effect := externalEffectFromObject(object)
	if effect.RequestDigest != transition.RequestDigest {
		return nil, store.ConflictErrorf("external effect %q request digest does not match reserved identity", effect.ID)
	}
	if effect.State == transition.NewState && effect.ResponseDigest == transition.ResponseDigest && bytes.Equal(effect.Response, transition.Response) && effect.LeaseOwner == transition.LeaseOwner && sameOptionalTime(effect.LeaseExpiresAt, transition.LeaseExpiresAt) {
		return &effect, nil
	}
	if effect.Version != transition.ExpectedVersion || effect.State != transition.ExpectedState || effect.LeaseOwner != transition.ExpectedLeaseOwner {
		return nil, store.ConflictErrorf("external effect %q no longer matches expected version, state, or lease owner", effect.ID)
	}
	if transition.ExpectedState == store.ExternalEffectInFlight && transition.NewState == store.ExternalEffectInFlight && effect.LeaseOwner != transition.LeaseOwner && effect.LeaseExpiresAt != nil && effect.LeaseExpiresAt.After(transition.UpdatedAt) {
		return nil, store.ConflictErrorf("external effect %q is still leased by %q", effect.ID, effect.LeaseOwner)
	}

	updated := object.DeepCopy()
	updated.Status.State = corev1alpha1.ExternalEffectControlState(transition.NewState)
	updated.Status.ResponseDigest = transition.ResponseDigest
	updated.Status.Response = externalEffectResponseToAPI(transition.Response)
	updated.Status.LeaseOwner = transition.LeaseOwner
	updated.Status.LeaseExpiresAt = metaTimePtr(transition.LeaseExpiresAt)
	if transition.NewState == store.ExternalEffectInFlight {
		updated.Status.Attempts++
	}
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, effect.Version+1, "", "", effect.CreatedAt, transition.UpdatedAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return nil, mapKubernetesError("transition external effect", err)
	}
	result := externalEffectFromObject(updated)
	return &result, nil
}

func (s *Store) completeExternalEffectCreation(ctx context.Context, object *corev1alpha1.ExternalEffect, request store.ReserveExternalEffectRequest, id string, fence store.ControllerEpochFence, snapshot epochSnapshot) (*store.ExternalEffect, error) {
	if !sameExternalEffectSpec(object, request, id) {
		return nil, store.ConflictErrorf("external effect %q was reused with a different identity or request digest", id)
	}
	if object.Status.Version > 0 {
		existing := externalEffectFromObject(object)
		return &existing, nil
	}
	updated := object.DeepCopy()
	updated.Status.State = corev1alpha1.ExternalEffectControlState(store.ExternalEffectPending)
	updated.Status.Attempts = 0
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, 1, "", "", request.CreatedAt, request.CreatedAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		if apierrors.IsConflict(err) {
			fresh, getErr := s.findExternalEffectByID(ctx, id)
			if getErr == nil && sameExternalEffectSpec(fresh, request, id) && fresh.Status.Version > 0 {
				result := externalEffectFromObject(fresh)
				return &result, nil
			}
		}
		return nil, mapKubernetesError("initialize external effect status", err)
	}
	result := externalEffectFromObject(updated)
	return &result, nil
}

func (s *Store) findExternalEffectByID(ctx context.Context, id string) (*corev1alpha1.ExternalEffect, error) {
	list := &corev1alpha1.ExternalEffectList{}
	if err := s.readClient().List(ctx, list, s.namespacedListOptions(
		client.MatchingLabels{corev1alpha1.ControlRecordIDHashLabel: dnsDigest(id)},
	)...); err != nil {
		return nil, mapKubernetesError("list external effects", err)
	}
	var match *corev1alpha1.ExternalEffect
	for i := range list.Items {
		if list.Items[i].Spec.ID != id {
			continue
		}
		if match != nil {
			return nil, store.ConflictErrorf("multiple external effects exist for canonical ID %q", id)
		}
		match = list.Items[i].DeepCopy()
	}
	if match == nil {
		return nil, store.ErrNotFound
	}
	return match, nil
}

func validateExternalEffectTransition(transition *store.ExternalEffectTransition) error {
	if transition.ExpectedVersion < 1 {
		return store.ValidationErrorf("external effect expected version must be at least 1")
	}
	if !store.IsKnownExternalEffectState(transition.ExpectedState) || !store.IsKnownExternalEffectState(transition.NewState) {
		return store.ValidationErrorf("unsupported external effect transition %q -> %q", transition.ExpectedState, transition.NewState)
	}
	if !store.ValidExternalEffectTransition(transition.ExpectedState, transition.NewState) {
		return store.ValidationErrorf("external effect transition %s -> %s is not allowed", transition.ExpectedState, transition.NewState)
	}
	if err := store.ValidateCanonicalDigest("external effect request digest", transition.RequestDigest); err != nil {
		return err
	}
	transition.ExpectedLeaseOwner = strings.TrimSpace(transition.ExpectedLeaseOwner)
	transition.LeaseOwner = strings.TrimSpace(transition.LeaseOwner)
	transition.LeaseExpiresAt = store.NormalizeOptionalControlTime(transition.LeaseExpiresAt)
	transition.UpdatedAt = store.NormalizeControlTime(transition.UpdatedAt)
	if transition.ExpectedState == store.ExternalEffectInFlight {
		if err := store.ValidateControlIdentifier("expected external effect lease owner", transition.ExpectedLeaseOwner); err != nil {
			return err
		}
	} else if transition.ExpectedLeaseOwner != "" {
		return store.ValidationErrorf("pending external effect transition must not set an expected lease owner")
	}
	if transition.NewState == store.ExternalEffectInFlight {
		if err := store.ValidateControlIdentifier("external effect lease owner", transition.LeaseOwner); err != nil {
			return err
		}
		if transition.LeaseExpiresAt == nil || !transition.LeaseExpiresAt.After(transition.UpdatedAt) {
			return store.ValidationErrorf("in-flight external effect requires a future lease expiry")
		}
		if len(transition.Response) > 0 || transition.ResponseDigest != "" {
			return store.ValidationErrorf("in-flight external effect must not include a response")
		}
	} else {
		if transition.LeaseOwner != "" || transition.LeaseExpiresAt != nil {
			return store.ValidationErrorf("non-in-flight external effect must clear lease fields")
		}
		if len(transition.Response) > 0 {
			if err := store.ValidateControlPayload("external effect response", transition.Response); err != nil {
				return err
			}
			if err := store.ValidateCanonicalDigest("external effect response digest", transition.ResponseDigest); err != nil {
				return err
			}
			if store.CanonicalBytesDigest(transition.Response) != transition.ResponseDigest {
				return store.ValidationErrorf("external effect response digest does not match response bytes")
			}
		} else if transition.ResponseDigest != "" {
			return store.ValidationErrorf("external effect response digest requires response bytes")
		}
	}
	return nil
}

func sameExternalEffectSpec(object *corev1alpha1.ExternalEffect, request store.ReserveExternalEffectRequest, id string) bool {
	return object.Namespace == request.Identity.Namespace && object.Spec.ID == id && object.Spec.Kind == request.Identity.Kind && object.Spec.IdentityNamespace == request.Identity.Namespace && object.Spec.AggregateID == request.Identity.AggregateID && object.Spec.OperationID == request.Identity.OperationID && object.Spec.RequestDigest == request.RequestDigest
}

func externalEffectFromObject(object *corev1alpha1.ExternalEffect) store.ExternalEffect {
	return store.ExternalEffect{
		ID: object.Spec.ID,
		Identity: store.ExternalEffectIdentity{
			Kind:        object.Spec.Kind,
			Namespace:   object.Spec.IdentityNamespace,
			AggregateID: object.Spec.AggregateID,
			OperationID: object.Spec.OperationID,
		},
		RequestDigest:       object.Spec.RequestDigest,
		State:               store.ExternalEffectState(object.Status.State),
		ResponseDigest:      object.Status.ResponseDigest,
		Response:            externalEffectResponseFromAPI(object.Status.Response),
		LeaseOwner:          object.Status.LeaseOwner,
		LeaseExpiresAt:      optionalTimeValue(object.Status.LeaseExpiresAt),
		Attempts:            object.Status.Attempts,
		ControllerEpochName: object.Status.ControllerEpochName,
		ControllerEpoch:     object.Status.ControllerEpoch,
		Version:             object.Status.Version,
		CreatedAt:           timeValue(object.Status.CreatedAt),
		UpdatedAt:           timeValue(object.Status.UpdatedAt),
	}
}

func externalEffectResponseToAPI(value []byte) *apiextensionsv1.JSON {
	if len(value) == 0 {
		return nil
	}
	return &apiextensionsv1.JSON{Raw: append([]byte(nil), value...)}
}

func externalEffectResponseFromAPI(value *apiextensionsv1.JSON) []byte {
	if value == nil || len(value.Raw) == 0 {
		return nil
	}
	return append([]byte(nil), value.Raw...)
}

func metaTimePtr(value *time.Time) *metav1.Time {
	if value == nil {
		return nil
	}
	result := metav1.NewTime(value.UTC())
	return &result
}
