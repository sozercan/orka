package controller

import (
	"bytes"
	"context"
	"errors"
	"reflect"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// sessionRuntimeCleanupAuthSnapshot proves a restored credential version was
// conformed before takeover. The admission Ready bit cannot prove this once
// the surviving supervisor's startup epoch is older than the controller's.
func (d *ACPDispatcher) sessionRuntimeCleanupAuthSnapshot(
	ctx context.Context,
	current, frozenRuntime *corev1alpha1.AgentRuntime,
	auth agentRuntimeAuthMaterial,
	profile harnessv2.ProfileDigest,
	instance harnessv2.RuntimeInstanceID,
	boot harnessv2.SupervisorBootID,
	cleanup *sessionRuntimeCleanupFence,
) (*corev1.Secret, error) {
	if cleanup == nil || current == nil || cleanup.runtimeEpoch >= uint64(cleanup.controller.Epoch) {
		return nil, errors.New("persisted Session cleanup authentication requires a prior runtime epoch")
	}
	r := &AgentRuntimeReconciler{Client: d.Client, APIReader: d.APIReader}
	secret, err := r.agentRuntimeCleanupSecret(ctx, current)
	if err != nil {
		return nil, err
	}
	if secret == nil || secret.UID == "" || secret.ResourceVersion == "" || !secret.DeletionTimestamp.IsZero() {
		return nil, errors.New("persisted Session cleanup authentication is unavailable")
	}
	conformed, savedAuth, err := decodeAgentRuntimeDeletionSnapshot(current, secret)
	if err != nil {
		return nil, err
	}
	if conformed.Generation != current.Generation || !reflect.DeepEqual(conformed.Spec, current.Spec) ||
		savedAuth.controllerSecretUID != auth.controllerSecretUID ||
		savedAuth.capabilitySecretUID != auth.capabilitySecretUID ||
		savedAuth.controllerResourceVersion != auth.controllerResourceVersion ||
		savedAuth.capabilityResourceVersion != auth.capabilityResourceVersion ||
		savedAuth.controllerBearerToken != auth.controllerBearerToken ||
		!bytes.Equal(savedAuth.operationCapabilitySecret, auth.operationCapabilitySecret) {
		return nil, errors.New("persisted Session cleanup authentication no longer matches the registration")
	}
	if err := validateExternalRuntimeReconformedCleanupAuthentication(conformed, frozenRuntime, auth, profile); err != nil {
		return nil, err
	}
	if _, err := validateExternalRuntimeCleanupIdentity(conformed, client.ObjectKeyFromObject(current), current.UID,
		instance, boot, "", 0, cleanup.runtimeEpoch); err != nil {
		return nil, err
	}
	return secret, nil
}

func (d *ACPDispatcher) revalidateSessionRuntimeCleanupAuthSnapshot(
	ctx context.Context,
	current *corev1alpha1.AgentRuntime,
	authority *externalRuntimeCleanupAuthority,
) error {
	secret, err := d.sessionRuntimeCleanupAuthSnapshot(ctx, current, authority.frozenRuntime, authority.auth,
		authority.runtimeProfileDigest, authority.runtimeInstanceID, authority.supervisorBootID, authority.sessionCleanup)
	if err != nil {
		return err
	}
	expected := authority.sessionCleanupAuth
	if expected == nil || secret.UID != expected.UID || secret.ResourceVersion != expected.ResourceVersion ||
		!reflect.DeepEqual(secret.Data, expected.Data) {
		return errors.New("persisted Session cleanup authentication changed before mutation")
	}
	return nil
}
