package controller

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSessionRuntimeCleanupRetiresPreviousControllerEpoch(t *testing.T) {
	fixture, tasks := newContinuedSessionCleanupFixture(t)
	originalEpoch := tasks[1].Status.Execution.ControllerEpoch
	projections := make(map[string][]byte, len(tasks))
	for _, task := range tasks {
		_, projection := sessionRuntimeCleanupTurnProjection(t, fixture, task)
		projections[task.Name] = bytes.Clone(projection.Payload)
	}
	// A restored credential has a newer resourceVersion than the admitted
	// Task, with a passing conformance snapshot persisted before takeover.
	rotateExternalRuntimeCredentials(t, fixture, true)
	persistSessionCleanupAuthFixture(t, fixture)
	epochs, stop := startACPRecoveryEpochManager(t, fixture.ctx, fixture.controlStore, "session-cleanup-successor")
	defer stop()
	fixture.epochs, fixture.dispatcher.Epochs = epochs, epochs
	fence, err := epochs.CurrentFence(fixture.ctx)
	if err != nil || fence.Epoch <= originalEpoch {
		t.Fatalf("successor controller fence = %#v, err=%v", fence, err)
	}
	runtime := fixture.runtime.DeepCopy()
	runtime.Status.Ready = false
	if err := fixture.client.Status().Update(fixture.ctx, runtime); err != nil {
		t.Fatal(err)
	}
	// Recovery can advance mutable Task status while the immutable terminal
	// projection still owns the resident session on the original runtime boot.
	for _, task := range tasks {
		current := &corev1alpha1.Task{}
		if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
			t.Fatal(err)
		}
		current.Status.Execution.ControllerEpoch = fence.Epoch
		if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
			t.Fatal(err)
		}
	}
	cleanup := sessionRuntimeCleanupStore(t, fixture, fixture.dispatcher.CleanupSessionRuntime)
	fixture.reconciler.DurableControlStore = cleanup
	manager := NewSessionManager(fixture.persistence)
	manager.SetACPSessionCleanup(cleanup, epochs)
	if err := manager.DeleteSession(fixture.ctx, defaultNS, "cleanup-conversation"); err != nil {
		t.Fatalf("successor controller DeleteSession(): %v", err)
	}
	if fixture.deleteCalls.Load() != 1 {
		t.Fatalf("runtime DELETE calls = %d, want one exact retirement", fixture.deleteCalls.Load())
	}
	request := <-fixture.deleteRequests
	if request.Metadata.Fence.ControllerEpoch != uint64(originalEpoch) {
		t.Fatalf("runtime retirement epoch = %d, want original epoch %d", request.Metadata.Fence.ControllerEpoch, originalEpoch)
	}
	assertSessionRuntimeCleanupCompleted(t, fixture, cleanup, tasks)
	for _, task := range tasks {
		current := &corev1alpha1.Task{}
		if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
			t.Fatal(err)
		}
		if current.Status.Execution.ControllerEpoch != fence.Epoch {
			t.Fatal("retirement overwrote the current Task controller epoch")
		}
		attemptID, err := promptAttemptIDFromTask(task)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := fixture.persistence.GetSessionTurnCleanupReceipt(fixture.ctx, defaultNS, "cleanup-conversation", attemptID)
		if err != nil || !bytes.Equal(receipt.Payload, projections[task.Name]) {
			t.Fatalf("retirement changed the immutable terminal projection: %v", err)
		}
	}
}

func TestSessionRuntimeCleanupRejectsChangedConformanceProof(t *testing.T) {
	for _, change := range []string{"removed owner", "changed boot", "new credential version", "proof replaced after client creation"} {
		t.Run(change, func(t *testing.T) {
			fixture := newSessionCleanupAuthorityClientFixture(t, false, nil)
			rotateExternalRuntimeCredentials(t, fixture.base, true)
			proof := persistSessionCleanupAuthFixture(t, fixture.base)
			runtime := fixture.base.runtime.DeepCopy()
			runtime.Status.Ready = false
			if err := fixture.base.client.Status().Update(fixture.base.ctx, runtime); err != nil {
				t.Fatal(err)
			}
			cleanupClient, fence, err := fixture.cleanupClient(fixture.scope())
			if err != nil {
				t.Fatalf("valid frozen conformance proof rejected: %v", err)
			}
			switch change {
			case "removed owner":
				proof.OwnerReferences = nil
			case "changed boot":
				var snapshot agentRuntimeDeletionSnapshot
				if err := json.Unmarshal(proof.Data[agentRuntimeCleanupSecretAuthorityKey], &snapshot); err != nil {
					t.Fatal(err)
				}
				snapshot.ObservedCapabilities.SupervisorBootID = "unproven-boot"
				proof.Data[agentRuntimeCleanupSecretAuthorityKey], err = harnessv2.CanonicalValue(snapshot)
				if err != nil {
					t.Fatal(err)
				}
			case "new credential version":
				secret := &corev1.Secret{}
				key := client.ObjectKey{Namespace: runtime.Namespace, Name: runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef.Name}
				if err := fixture.base.client.Get(fixture.base.ctx, key, secret); err != nil {
					t.Fatal(err)
				}
				if secret.Annotations == nil {
					secret.Annotations = make(map[string]string)
				}
				secret.Annotations["changed"] = "after-conformance"
				if err := fixture.base.client.Update(fixture.base.ctx, secret); err != nil {
					t.Fatal(err)
				}
			case "proof replaced after client creation":
				proof.UID = "replacement-proof-uid"
			}
			if err := fixture.base.client.Update(fixture.base.ctx, proof); err != nil {
				t.Fatal(err)
			}
			err = fixture.base.dispatcher.deleteRuntimeSession(fixture.base.ctx, cleanupClient,
				harnessv2.RuntimeSessionID(runtimeSessionID(fence)), fixture.task, fence, "session_deleted")
			if err == nil || !strings.Contains(err.Error(), "authentication") && !strings.Contains(err.Error(), "cleanup") {
				t.Fatalf("changed conformance proof authorized deletion: %v", err)
			}
			if fixture.base.deleteCalls.Load() != 0 {
				t.Fatal("changed conformance proof reached runtime DELETE")
			}
		})
	}
}

func persistSessionCleanupAuthFixture(t *testing.T, fixture *externalACPDispatchFixture) *corev1.Secret {
	t.Helper()
	r := &AgentRuntimeReconciler{Client: fixture.client, APIReader: fixture.client, Scheme: fixture.client.Scheme()}
	runtime := fixture.runtime
	if err := r.persistAgentRuntimeDeletionSnapshot(fixture.ctx, runtime, runtime.Status.ObservedCapabilities,
		runtime.Status.ObservedControllerAuthRefResourceVersion, runtime.Status.ObservedOperationCapabilityRefResourceVersion); err != nil {
		t.Fatal(err)
	}
	secret, err := r.agentRuntimeCleanupSecret(fixture.ctx, runtime)
	if err != nil || secret == nil {
		t.Fatalf("persisted conformance proof unavailable: %v", err)
	}
	secret.UID = "session-cleanup-auth-secret-uid"
	if err := fixture.client.Update(fixture.ctx, secret); err != nil {
		t.Fatal(err)
	}
	return secret
}
