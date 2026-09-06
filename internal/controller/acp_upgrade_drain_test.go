/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
)

const acpUpgradeDrainTestBaseTemplateName = "orka-codex"

func TestACPUpgradeDrainOptionsBindManifestFlags(t *testing.T) {
	options := DefaultACPUpgradeDrainOptions()
	flags := flag.NewFlagSet("upgrade-drain", flag.ContinueOnError)
	options.BindFlags(flags)
	err := flags.Parse([]string{
		"--acp-upgrade-drain-bind-address=127.0.0.1:18083",
		"--acp-upgrade-drain-timeout=4m",
		"--acp-upgrade-drain-poll-interval=2s",
		"--acp-upgrade-drain-marker-namespace=orka-system",
		"--acp-upgrade-drain-trigger-url=http://127.0.0.1:18083/acp/upgrade-drain",
		"--acp-upgrade-drain-trigger-timeout=4m15s",
	})
	if err != nil {
		t.Fatalf("Parse flags: %v", err)
	}
	if handled, err := RunACPUpgradeDrainTriggerMode(context.Background(), ACPUpgradeDrainOptions{}); err != nil || handled {
		t.Fatalf("empty trigger mode = %t, %v, want unhandled", handled, err)
	}
	if options.BindAddress != "127.0.0.1:18083" || options.Timeout != 4*time.Minute ||
		options.PollInterval != 2*time.Second || options.MarkerNamespace != runtimePoolDefaultControllerNamespace ||
		options.TriggerTimeout != 4*time.Minute+15*time.Second ||
		options.TriggerURL != "http://127.0.0.1:18083/acp/upgrade-drain" {
		t.Fatalf("parsed options = %#v", options)
	}
}

func TestACPUpgradeDrainRequiresWatchNamespace(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	options := testUpgradeDrainOptions()
	options.WatchNamespace = ""
	coordinator := NewACPUpgradeDrainCoordinator(
		kubeClient,
		kubeClient,
		fixedUpgradeDrainEpochSource{},
		&fakeUpgradeDrainEpochStore{},
		ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
			return ACPUpgradeDrainBarrierSnapshot{}, nil
		}),
		NewACPAdmissionGate(),
		options,
	)

	err := coordinator.initialize()
	if err == nil || !strings.Contains(err.Error(), "watch namespace is required") {
		t.Fatalf("initialize() error = %v, want missing watch namespace", err)
	}
}

func TestACPUpgradeDrainCoordinatorCompletesExactInstanceDrain(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	pool, pod, auth, supervisor := upgradeDrainRuntimePoolFixture(t)
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 7, HolderID: "controller-7"}
	epochRecord, epochLease := upgradeDrainEpochObjects(fence)
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithObjects(pool, &pod, auth, epochRecord, epochLease).
		Build()
	epochStore := &fakeUpgradeDrainEpochStore{current: store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID}}
	gate := NewACPAdmissionGate()
	coordinator := NewACPUpgradeDrainCoordinator(
		kubeClient,
		kubeClient,
		fixedUpgradeDrainEpochSource{fence: fence},
		epochStore,
		ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
			return ACPUpgradeDrainBarrierSnapshot{}, nil
		}),
		gate,
		testUpgradeDrainOptions(),
	)
	coordinator.SupervisorClient = supervisor

	if err := coordinator.ReadyzChecker()(httptest.NewRequest(http.MethodGet, "/readyz", nil)); err != nil {
		t.Fatalf("readiness before drain = %v, want nil", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Trigger(ctx); err != nil {
		t.Fatalf("Trigger() error = %v", err)
	}
	if supervisor.drainCallCount() != 1 {
		t.Fatalf("drain calls = %d, want 1", supervisor.drainCallCount())
	}
	if err := gate.Check(); !errors.Is(err, ErrACPAdmissionClosed) {
		t.Fatalf("admission gate error = %v, want ErrACPAdmissionClosed", err)
	}
	if err := coordinator.ReadyzChecker()(httptest.NewRequest(http.MethodGet, "/readyz", nil)); err == nil {
		t.Fatal("readiness remained true after planned drain began")
	}

	var current corev1alpha1.RuntimePool
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, &current); err != nil {
		t.Fatalf("Get RuntimePool: %v", err)
	}
	if current.Spec.DesiredReplicas != 0 {
		t.Fatalf("desiredReplicas = %d, want 0", current.Spec.DesiredReplicas)
	}
	if current.Status.AdmissionState == corev1alpha1.RuntimePoolAdmissionAccepting {
		t.Fatalf("admissionState = %s, want closed or draining", current.Status.AdmissionState)
	}
	marker, err := ReadACPUpgradeDrainMarker(ctx, kubeClient, coordinator.Options.MarkerNamespace, fence.Name)
	if err != nil {
		t.Fatalf("ReadACPUpgradeDrainMarker: %v", err)
	}
	if marker.State != ACPUpgradeDrainMarkerCompleted || marker.CompletedAt == nil || !marker.Snapshot.Quiescent() {
		t.Fatalf("marker = %#v, want quiescent Completed marker", marker)
	}
	completed, err := ACPUpgradeDrainCompletedForEpoch(ctx, kubeClient, coordinator.Options.MarkerNamespace, fence.Name, fence.Epoch)
	if err != nil || !completed {
		t.Fatalf("ACPUpgradeDrainCompletedForEpoch() = %t, %v, want true", completed, err)
	}
	if completed, err := ACPUpgradeDrainCompletedForEpoch(ctx, kubeClient, coordinator.Options.MarkerNamespace, fence.Name, fence.Epoch-1); err != nil || completed {
		t.Fatalf("previous unrelated epoch completion = %t, %v, want false", completed, err)
	}
}

func TestACPUpgradeDrainCoordinatorDrainsPreservedOldGenerationInstance(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	pool, pod, auth, supervisor := upgradeDrainRuntimePoolFixture(t)
	pool.Generation++
	pool.Status.ObservedGeneration = pool.Generation
	pool.Status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
	pool.Status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               corev1alpha1.RuntimePoolConditionRolloutReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: pool.Generation,
		Reason:             runtimePoolRolloutReasonTimedOut,
		Message:            "preserving the previous-generation runtime Pod",
	})
	if got := supervisor.probe.Status.Fence.RuntimePoolGeneration; got >= uint64(pool.Generation) {
		t.Fatalf("fixture probe generation = %d, want older than current pool generation %d", got, pool.Generation)
	}

	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 7, HolderID: "controller-7"}
	epochRecord, epochLease := upgradeDrainEpochObjects(fence)
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithObjects(pool, &pod, auth, epochRecord, epochLease).
		Build()
	coordinator := NewACPUpgradeDrainCoordinator(
		kubeClient,
		kubeClient,
		fixedUpgradeDrainEpochSource{fence: fence},
		&fakeUpgradeDrainEpochStore{current: store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID}},
		ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
			return ACPUpgradeDrainBarrierSnapshot{}, nil
		}),
		NewACPAdmissionGate(),
		testUpgradeDrainOptions(),
	)
	coordinator.SupervisorClient = supervisor

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Trigger(ctx); err != nil {
		t.Fatalf("Trigger() with preserved old-generation instance: %v", err)
	}
	if supervisor.drainCallCount() != 1 {
		t.Fatalf("old-generation drain calls = %d, want 1", supervisor.drainCallCount())
	}
}

func TestACPUpgradeDrainCoordinatorDrainsSubstrateActorWithoutPod(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	pool, pod, auth, supervisor := upgradeDrainRuntimePoolFixture(t)
	pool.Spec.ExecutionWorkspace = &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
		Provider:      corev1alpha1.WorkspaceProviderSubstrate,
		BindingDigest: "sha256:" + strings.Repeat("a", 64),
		Substrate: &corev1alpha1.RuntimePoolSubstrateWorkspaceSpec{
			BaseTemplateNamespace: substrateTestTemplateNamespace,
			BaseTemplateName:      acpUpgradeDrainTestBaseTemplateName,
		},
	}
	const runtimeNamespace = "runtime-system"
	pool.Spec.RuntimeNamespace = runtimeNamespace
	pool.Status.ActiveInstance.PodNamespace = runtimeNamespace
	auth.Namespace = runtimeNamespace
	auth.Name = runtimePoolChildName(runtimePoolResourceName(pool.Namespace, pool.Name), "auth-e7-"+strings.Repeat("a", 24))
	auth.UID = "bound-auth-secret-uid"
	immutable := true
	auth.Immutable = &immutable
	auth.Labels = map[string]string{
		runtimePoolManagedByLabel:       runtimePoolManagedByLabelValue,
		runtimePoolApplicationLabel:     runtimePoolApplicationLabelValue,
		runtimePoolKeyLabel:             runtimePoolKey(pool.Namespace, pool.Name),
		runtimePoolNameLabel:            pool.Name,
		runtimePoolNamespaceLabel:       pool.Namespace,
		runtimePoolUIDLabel:             string(pool.UID),
		runtimePoolNetworkRoleLabel:     "provider-client",
		runtimePoolAuthLabel:            booleanTrueValue,
		runtimePoolCredentialEpochLabel: "7",
	}
	auth.Data[runtimePoolBootstrapNonceKey] = []byte(strings.Repeat("n", 32))
	auth.Data[runtimePoolBootstrapSigningSeedKey] = []byte(strings.Repeat("s", 32))
	if pool.Annotations == nil {
		pool.Annotations = map[string]string{}
	}
	pool.Annotations[runtimePoolPrivateAuthSecretBindingAnnotation(7)] = auth.Name + "/" + string(auth.UID)
	const actorID = "acp-ws-codex-actor"
	routeHost := actorID + ".actors.local"
	instanceUID := substrateActorInstanceUID(actorID)
	providerGeneration := pod.Annotations[runtimePoolProviderTokenGenerationAnnotation]
	pool.Status.ActiveInstance.PodName = actorID
	pool.Status.ActiveInstance.PodAddress = routeHost
	pool.Status.ActiveInstance.PodUID = instanceUID
	pool.Status.ActiveInstance.ProviderTokenGeneration = providerGeneration
	pool.Status.ActiveInstance.RuntimeInstanceID = runtimePoolRuntimeInstanceID(types.UID(instanceUID), "boot-upgrade")
	syntheticPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(instanceUID)}}
	supervisor.probe = runtimePoolValidProbe(pool, syntheticPod, "boot-upgrade", false)
	supervisor.expectedEndpoint = "http://" + routeHost

	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 7, HolderID: "controller-7"}
	epochRecord, epochLease := upgradeDrainEpochObjects(fence)
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithObjects(pool, auth, epochRecord, epochLease).
		Build()
	coordinator := NewACPUpgradeDrainCoordinator(
		kubeClient,
		kubeClient,
		fixedUpgradeDrainEpochSource{fence: fence},
		&fakeUpgradeDrainEpochStore{current: store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID}},
		ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
			return ACPUpgradeDrainBarrierSnapshot{}, nil
		}),
		NewACPAdmissionGate(),
		testUpgradeDrainOptions(),
	)
	coordinator.SupervisorClient = supervisor

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Trigger(ctx); err != nil {
		t.Fatalf("Trigger() error = %v", err)
	}
	if supervisor.drainCallCount() != 1 {
		t.Fatalf("Substrate actor drain calls = %d, want 1", supervisor.drainCallCount())
	}
}

func TestACPUpgradeDrainRequiresStoppedSubstrateLifecycleWithoutActiveInstance(t *testing.T) {
	pool, _, _, _ := upgradeDrainRuntimePoolFixture(t)
	pool.Spec.ExecutionWorkspace = &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
		Provider: corev1alpha1.WorkspaceProviderSubstrate,
		Substrate: &corev1alpha1.RuntimePoolSubstrateWorkspaceSpec{
			BaseTemplateNamespace: substrateTestTemplateNamespace,
			BaseTemplateName:      acpUpgradeDrainTestBaseTemplateName,
		},
	}
	pool.Status.ActiveInstance = nil
	pool.Status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
	fence := store.ControllerEpochFence{Epoch: pool.Status.ControllerEpoch}
	coordinator := &ACPUpgradeDrainCoordinator{}

	err := coordinator.observeAndDrainRuntimePool(context.Background(), fence, pool, &ACPUpgradeDrainSnapshot{})
	if err == nil || !strings.Contains(err.Error(), "does not prove the provider workspace is stopped") {
		t.Fatalf("Stopping Substrate pool without active instance error = %v, want teardown proof rejection", err)
	}
	pool.Status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopped
	pool.Status.ObservedGeneration = pool.Generation - 1
	if err := coordinator.observeAndDrainRuntimePool(context.Background(), fence, pool, &ACPUpgradeDrainSnapshot{}); err == nil ||
		!strings.Contains(err.Error(), "instead of current generation") {
		t.Fatalf("stale Stopped Substrate pool error = %v, want generation-fence rejection", err)
	}
	pool.Status.ObservedGeneration = pool.Generation
	if err := coordinator.observeAndDrainRuntimePool(context.Background(), fence, pool, &ACPUpgradeDrainSnapshot{}); err != nil {
		t.Fatalf("fully stopped Substrate pool rejected: %v", err)
	}
}

func TestACPUpgradeDrainRequiresStoppedAgentSandboxLifecycleWithoutActiveInstance(t *testing.T) {
	pool, _, _, _ := upgradeDrainRuntimePoolFixture(t)
	pool.Spec.ExecutionWorkspace = &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
		Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
		BindingDigest: "sha256:" + strings.Repeat("b", 64),
	}
	pool.Status.ActiveInstance = nil
	pool.Status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStarting
	fence := store.ControllerEpochFence{Epoch: pool.Status.ControllerEpoch}
	coordinator := &ACPUpgradeDrainCoordinator{}

	err := coordinator.observeAndDrainRuntimePool(context.Background(), fence, pool, &ACPUpgradeDrainSnapshot{})
	if err == nil || !strings.Contains(err.Error(), "does not prove the provider workspace is stopped") {
		t.Fatalf("Starting Agent Sandbox pool without active instance error = %v, want teardown proof rejection", err)
	}
	pool.Status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopped
	if err := coordinator.observeAndDrainRuntimePool(context.Background(), fence, pool, &ACPUpgradeDrainSnapshot{}); err != nil {
		t.Fatalf("fully stopped Agent Sandbox pool rejected: %v", err)
	}
}

func TestACPUpgradeDrainRejectsStaleIdentityAndInvalidCredentials(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1alpha1.RuntimePool, *corev1.Secret)
	}{
		{
			name: "stale runtime instance",
			mutate: func(pool *corev1alpha1.RuntimePool, _ *corev1.Secret) {
				pool.Status.ActiveInstance.RuntimeInstanceID = "stale-runtime-instance"
			},
		},
		{
			name: "invalid controller credential",
			mutate: func(_ *corev1alpha1.RuntimePool, auth *corev1.Secret) {
				auth.Data[runtimePoolControllerTokenKey] = []byte("wrong-token")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := upgradeDrainTestScheme(t)
			pool, pod, auth, supervisor := upgradeDrainRuntimePoolFixture(t)
			tt.mutate(pool, auth)
			fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 7, HolderID: "controller-7"}
			epochRecord, epochLease := upgradeDrainEpochObjects(fence)
			kubeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&corev1alpha1.RuntimePool{}).
				WithObjects(pool, &pod, auth, epochRecord, epochLease).
				Build()
			options := testUpgradeDrainOptions()
			options.Timeout = 40 * time.Millisecond
			coordinator := NewACPUpgradeDrainCoordinator(
				kubeClient,
				kubeClient,
				fixedUpgradeDrainEpochSource{fence: fence},
				&fakeUpgradeDrainEpochStore{current: store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID}},
				ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
					return ACPUpgradeDrainBarrierSnapshot{}, nil
				}),
				NewACPAdmissionGate(),
				options,
			)
			coordinator.SupervisorClient = supervisor
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := coordinator.Trigger(ctx); !errors.Is(err, ErrACPUpgradeDrainTimedOut) {
				t.Fatalf("Trigger() error = %v, want safe timeout fallback", err)
			}
			if supervisor.drainCallCount() != 0 {
				t.Fatalf("drain calls = %d, want 0 for rejected identity or credential", supervisor.drainCallCount())
			}
			marker, err := ReadACPUpgradeDrainMarker(ctx, kubeClient, options.MarkerNamespace, fence.Name)
			if err != nil {
				t.Fatalf("ReadACPUpgradeDrainMarker: %v", err)
			}
			if marker.State == ACPUpgradeDrainMarkerCompleted {
				t.Fatal("rejected exact-instance boundary published a Completed marker")
			}
		})
	}
}

func TestACPUpgradeDrainPreservesQueuedWorkAcrossPlannedHandoff(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 7, HolderID: "controller-7"}
	epochRecord, epochLease := upgradeDrainEpochObjects(fence)
	pool := runtimePoolTestObject(1)
	pool.Status = corev1alpha1.RuntimePoolStatus{
		ControllerEpoch: fence.Epoch, Lifecycle: corev1alpha1.RuntimePoolLifecycleStopped,
		AdmissionState: corev1alpha1.RuntimePoolAdmissionClosed,
		Capacity:       corev1alpha1.RuntimePoolCapacityStatus{QueuedTasks: 2},
	}
	attempt := &corev1alpha1.PromptAttempt{
		ObjectMeta: metav1.ObjectMeta{Name: "queued-attempt", Namespace: pool.Namespace},
		Status: corev1alpha1.PromptAttemptStatus{
			ExecutionState: corev1alpha1.PromptAttemptExecutionState(store.PromptExecutionQueued),
			DeliveryState:  corev1alpha1.PromptAttemptDeliveryState(store.PromptDeliveryNotRequested),
		},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithObjects(pool, attempt, epochRecord, epochLease).
		Build()
	options := testUpgradeDrainOptions()
	options.WatchNamespace = pool.Namespace
	barriers := &KubernetesACPUpgradeDrainBarrierObserver{
		Reader: kubeClient, Namespace: pool.Namespace,
		Outbox: ACPUpgradeDrainOutboxBarrierFunc(func(context.Context) (int64, error) { return 0, nil }),
	}
	coordinator := NewACPUpgradeDrainCoordinator(
		kubeClient,
		kubeClient,
		fixedUpgradeDrainEpochSource{fence: fence},
		&fakeUpgradeDrainEpochStore{current: store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID}},
		barriers,
		NewACPAdmissionGate(),
		options,
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Trigger(ctx); err != nil {
		t.Fatalf("Trigger() error = %v", err)
	}
	status := coordinator.Status()
	if status.Phase != ACPUpgradeDrainCompleted || status.Snapshot.QueuedTasks != 2 ||
		status.Snapshot.Barriers.NonterminalPromptAttempts != 0 {
		t.Fatalf("status = %#v, want completed handoff preserving two queued Tasks", status)
	}
	var currentAttempt corev1alpha1.PromptAttempt
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(attempt), &currentAttempt); err != nil {
		t.Fatalf("Get queued PromptAttempt: %v", err)
	}
	if currentAttempt.Status.ExecutionState != corev1alpha1.PromptAttemptExecutionState(store.PromptExecutionQueued) ||
		currentAttempt.Status.DeliveryState != corev1alpha1.PromptAttemptDeliveryState(store.PromptDeliveryNotRequested) {
		t.Fatalf("queued PromptAttempt was mutated: %#v", currentAttempt.Status)
	}
}

func TestACPUpgradeDrainDeletingPoolWaitsForLivePodQuiescence(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	pool, pod, auth, supervisor := upgradeDrainRuntimePoolFixture(t)
	now := metav1.Now()
	pool.DeletionTimestamp = &now
	pool.Finalizers = []string{runtimePoolFinalizer}
	supervisor.probe.Status.Lifecycle = harnessv2.SupervisorLifecycleDraining
	supervisor.probe.Status.Drain = harnessv2.DrainStatus{
		Requested: true, AcceptingNewSessions: false, RequestedAt: time.Now().UTC(), Reason: "delete",
	}
	supervisor.probe.Status.Sessions = []harnessv2.RuntimeSessionStatus{{
		RuntimeSessionID: "runtime-session-1", RuntimeSessionUID: "runtime-session-uid-1",
		Generation: 1, State: harnessv2.RuntimeSessionStateIdle, LastTransitionAt: time.Now().UTC(),
	}}
	supervisor.probe.Status.Pressure.ResidentSessions = 1
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 7, HolderID: "controller-7"}
	epochRecord, epochLease := upgradeDrainEpochObjects(fence)
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithObjects(pool, &pod, auth, epochRecord, epochLease).
		Build()
	options := testUpgradeDrainOptions()
	options.Timeout = 45 * time.Millisecond
	coordinator := NewACPUpgradeDrainCoordinator(
		kubeClient,
		kubeClient,
		fixedUpgradeDrainEpochSource{fence: fence},
		&fakeUpgradeDrainEpochStore{current: store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID}},
		ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
			return ACPUpgradeDrainBarrierSnapshot{}, nil
		}),
		NewACPAdmissionGate(),
		options,
	)
	coordinator.SupervisorClient = supervisor
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Trigger(ctx); !errors.Is(err, ErrACPUpgradeDrainTimedOut) {
		t.Fatalf("Trigger() error = %v, want live deleting-pool timeout", err)
	}
	if got := coordinator.Status().Snapshot.ResidentSessions; got != 1 {
		t.Fatalf("resident sessions = %d, want authenticated deleting-pool count 1", got)
	}
}

func TestACPUpgradeDrainScopesRuntimePoolsBeforeMutation(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 7, HolderID: "controller-7"}
	epochRecord, epochLease := upgradeDrainEpochObjects(fence)
	owned := runtimePoolTestObject(1)
	owned.Status.ControllerEpoch = fence.Epoch
	owned.Status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopped
	owned.Labels["orka.ai/controller-scope"] = "release-a"
	foreign := runtimePoolTestObject(1)
	foreign.Name = "foreign"
	foreign.Namespace = "tenant-b"
	foreign.UID = "foreign-uid"
	foreign.Spec.TrustDomain.Namespace = foreign.Namespace
	foreign.Status.ControllerEpoch = fence.Epoch
	foreign.Status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopped
	foreign.Labels["orka.ai/controller-scope"] = "release-b"
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithObjects(owned, foreign, epochRecord, epochLease).
		Build()
	options := testUpgradeDrainOptions()
	options.WatchNamespace = owned.Namespace
	options.RuntimePoolLabels = map[string]string{"orka.ai/controller-scope": "release-a"}
	coordinator := NewACPUpgradeDrainCoordinator(
		kubeClient,
		kubeClient,
		fixedUpgradeDrainEpochSource{fence: fence},
		&fakeUpgradeDrainEpochStore{current: store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID}},
		ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
			return ACPUpgradeDrainBarrierSnapshot{}, nil
		}),
		NewACPAdmissionGate(),
		options,
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Trigger(ctx); err != nil {
		t.Fatalf("Trigger() error = %v", err)
	}
	var gotOwned, gotForeign corev1alpha1.RuntimePool
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(owned), &gotOwned); err != nil {
		t.Fatalf("Get owned pool: %v", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(foreign), &gotForeign); err != nil {
		t.Fatalf("Get foreign pool: %v", err)
	}
	if gotOwned.Spec.DesiredReplicas != 0 {
		t.Fatalf("owned desired replicas = %d, want 0", gotOwned.Spec.DesiredReplicas)
	}
	if gotForeign.Spec.DesiredReplicas != 1 {
		t.Fatalf("foreign desired replicas = %d, want unchanged 1", gotForeign.Spec.DesiredReplicas)
	}
}

func TestACPUpgradeDrainRequiresStableQuiescentRecheck(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 31, HolderID: "controller-31"}
	epochRecord, epochLease := upgradeDrainEpochObjects(fence)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(epochRecord, epochLease).Build()
	var mu sync.Mutex
	calls := 0
	options := testUpgradeDrainOptions()
	options.Timeout = 45 * time.Millisecond
	coordinator := NewACPUpgradeDrainCoordinator(
		kubeClient,
		kubeClient,
		fixedUpgradeDrainEpochSource{fence: fence},
		&fakeUpgradeDrainEpochStore{current: store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID}},
		ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if calls == 1 {
				return ACPUpgradeDrainBarrierSnapshot{}, nil
			}
			return ACPUpgradeDrainBarrierSnapshot{UnsettledOutboxProjections: 1}, nil
		}),
		NewACPAdmissionGate(),
		options,
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Trigger(ctx); !errors.Is(err, ErrACPUpgradeDrainTimedOut) {
		t.Fatalf("Trigger() error = %v, want timeout after unstable quiescence", err)
	}
	marker, err := ReadACPUpgradeDrainMarker(ctx, kubeClient, options.MarkerNamespace, fence.Name)
	if err != nil {
		t.Fatalf("ReadACPUpgradeDrainMarker: %v", err)
	}
	if marker.State == ACPUpgradeDrainMarkerCompleted {
		t.Fatal("single transient quiescent pass published completion")
	}
}

func TestACPUpgradeDrainCoordinatorTimeoutFallsBackToUnplanned(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 11, HolderID: "controller-11"}
	epochRecord, epochLease := upgradeDrainEpochObjects(fence)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(epochRecord, epochLease).Build()
	epochStore := &fakeUpgradeDrainEpochStore{current: store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID}}
	options := testUpgradeDrainOptions()
	options.Timeout = 35 * time.Millisecond
	options.PollInterval = 5 * time.Millisecond
	coordinator := NewACPUpgradeDrainCoordinator(
		kubeClient,
		kubeClient,
		fixedUpgradeDrainEpochSource{fence: fence},
		epochStore,
		ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
			return ACPUpgradeDrainBarrierSnapshot{UnsettledOutboxProjections: 1}, nil
		}),
		NewACPAdmissionGate(),
		options,
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := coordinator.Trigger(ctx)
	if !errors.Is(err, ErrACPUpgradeDrainTimedOut) {
		t.Fatalf("Trigger() error = %v, want ErrACPUpgradeDrainTimedOut", err)
	}
	status := coordinator.Status()
	if status.Phase != ACPUpgradeDrainTimedOut || status.Snapshot.Barriers.UnsettledOutboxProjections != 1 {
		t.Fatalf("status = %#v, want timed out on outbox barrier", status)
	}
	marker, err := ReadACPUpgradeDrainMarker(ctx, kubeClient, options.MarkerNamespace, fence.Name)
	if err != nil {
		t.Fatalf("ReadACPUpgradeDrainMarker: %v", err)
	}
	if marker.State != ACPUpgradeDrainMarkerTimedOut || marker.TimedOutAt == nil || marker.CompletedAt != nil {
		t.Fatalf("marker = %#v, want TimedOut without completion", marker)
	}
	completed, err := ACPUpgradeDrainCompletedForEpoch(ctx, kubeClient, options.MarkerNamespace, fence.Name, fence.Epoch)
	if err != nil || completed {
		t.Fatalf("timed-out marker planned predicate = %t, %v, want false", completed, err)
	}
}

func TestACPUpgradeDrainCoordinatorEpochLossCannotWriteCompletion(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 19, HolderID: "controller-19"}
	epochRecord, epochLease := upgradeDrainEpochObjects(fence)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(epochRecord, epochLease).Build()
	epochStore := &fakeUpgradeDrainEpochStore{
		current:   store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID},
		loseAfter: 2,
	}
	coordinator := NewACPUpgradeDrainCoordinator(
		kubeClient,
		kubeClient,
		fixedUpgradeDrainEpochSource{fence: fence},
		epochStore,
		ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
			return ACPUpgradeDrainBarrierSnapshot{}, nil
		}),
		NewACPAdmissionGate(),
		testUpgradeDrainOptions(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := coordinator.Trigger(ctx)
	if !errors.Is(err, ErrACPUpgradeDrainEpochLost) {
		t.Fatalf("Trigger() error = %v, want ErrACPUpgradeDrainEpochLost", err)
	}
	marker, readErr := ReadACPUpgradeDrainMarker(ctx, kubeClient, coordinator.Options.MarkerNamespace, fence.Name)
	if readErr != nil {
		t.Fatalf("ReadACPUpgradeDrainMarker: %v", readErr)
	}
	if marker.State != ACPUpgradeDrainMarkerIntent || marker.CompletedAt != nil {
		t.Fatalf("marker after epoch loss = %#v, want incomplete Intent", marker)
	}
}

func TestACPUpgradeDrainCompletionLosesCASWhenEpochTurnsOver(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 29, HolderID: "controller-29"}
	epochRecord, epochLease := upgradeDrainEpochObjects(fence)
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(epochRecord, epochLease).Build()
	epochStore := &fakeUpgradeDrainEpochStore{current: store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID}}
	racingClient := &epochTurnoverOnCompletionClient{Client: baseClient, epochStore: epochStore}
	coordinator := NewACPUpgradeDrainCoordinator(
		racingClient,
		baseClient,
		fixedUpgradeDrainEpochSource{fence: fence},
		epochStore,
		ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
			return ACPUpgradeDrainBarrierSnapshot{}, nil
		}),
		NewACPAdmissionGate(),
		testUpgradeDrainOptions(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := coordinator.Trigger(ctx)
	if !errors.Is(err, ErrACPUpgradeDrainEpochLost) {
		t.Fatalf("Trigger() error = %v, want epoch-loss failure", err)
	}
	marker, readErr := ReadACPUpgradeDrainMarker(ctx, baseClient, coordinator.Options.MarkerNamespace, fence.Name)
	if readErr != nil {
		t.Fatalf("ReadACPUpgradeDrainMarker: %v", readErr)
	}
	if marker.State != ACPUpgradeDrainMarkerIntent || marker.CompletedAt != nil {
		t.Fatalf("marker after epoch turnover = %#v, want incomplete Intent", marker)
	}
}

func TestKubernetesACPUpgradeDrainBarrierObserver(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	objects := []client.Object{
		&corev1alpha1.PromptAttempt{ObjectMeta: metav1.ObjectMeta{Name: "queued", Namespace: "tenant-a"}, Status: corev1alpha1.PromptAttemptStatus{ExecutionState: corev1alpha1.PromptAttemptExecutionState(store.PromptExecutionQueued), DeliveryState: corev1alpha1.PromptAttemptDeliveryState(store.PromptDeliveryNotRequested)}},
		&corev1alpha1.PromptAttempt{ObjectMeta: metav1.ObjectMeta{Name: "reserved", Namespace: "tenant-a"}, Status: corev1alpha1.PromptAttemptStatus{ExecutionState: corev1alpha1.PromptAttemptExecutionState(store.PromptExecutionReserved), DeliveryState: corev1alpha1.PromptAttemptDeliveryState(store.PromptDeliveryNotRequested)}},
		&corev1alpha1.PromptAttempt{ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "tenant-a"}, Status: corev1alpha1.PromptAttemptStatus{ExecutionState: corev1alpha1.PromptAttemptExecutionState(store.PromptExecutionRunning), DeliveryState: corev1alpha1.PromptAttemptDeliveryState(store.PromptDeliveryNotRequested)}},
		&corev1alpha1.PromptAttempt{ObjectMeta: metav1.ObjectMeta{Name: "complete", Namespace: "tenant-a"}, Status: corev1alpha1.PromptAttemptStatus{ExecutionState: corev1alpha1.PromptAttemptExecutionState(store.PromptExecutionSucceeded), DeliveryState: corev1alpha1.PromptAttemptDeliveryState(store.PromptDeliveryReadValidated)}},
		&corev1alpha1.RuntimeSessionControl{ObjectMeta: metav1.ObjectMeta{Name: "leased", Namespace: "tenant-a"}, Status: corev1alpha1.RuntimeSessionControlStatus{Lifecycle: corev1alpha1.RuntimeSessionControlLifecycle("Idle"), MutationLease: &corev1alpha1.RuntimeSessionMutationLeaseStatus{}}},
		&corev1alpha1.RuntimeSessionControl{ObjectMeta: metav1.ObjectMeta{Name: "idle", Namespace: "tenant-a"}, Status: corev1alpha1.RuntimeSessionControlStatus{Lifecycle: corev1alpha1.RuntimeSessionControlLifecycle("Idle")}},
		&corev1alpha1.Publication{ObjectMeta: metav1.ObjectMeta{Name: "preparing", Namespace: "tenant-a"}, Status: corev1alpha1.PublicationStatus{State: corev1alpha1.PublicationControlState(store.PublicationPreparing)}},
		&corev1alpha1.Publication{ObjectMeta: metav1.ObjectMeta{Name: "verified", Namespace: "tenant-a"}, Status: corev1alpha1.PublicationStatus{State: corev1alpha1.PublicationControlState(store.PublicationVerifiedExact)}},
		&corev1alpha1.Publication{ObjectMeta: metav1.ObjectMeta{Name: "foreign-preparing", Namespace: "tenant-b"}, Status: corev1alpha1.PublicationStatus{State: corev1alpha1.PublicationControlState(store.PublicationPreparing)}},
		&corev1alpha1.ExternalEffect{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "tenant-a"}, Status: corev1alpha1.ExternalEffectStatus{State: corev1alpha1.ExternalEffectControlState(store.ExternalEffectPending)}},
		&corev1alpha1.ExternalEffect{ObjectMeta: metav1.ObjectMeta{Name: "succeeded", Namespace: "tenant-a"}, Status: corev1alpha1.ExternalEffectStatus{State: corev1alpha1.ExternalEffectControlState(store.ExternalEffectSucceeded)}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	observer := &KubernetesACPUpgradeDrainBarrierObserver{
		Reader: kubeClient, Namespace: "tenant-a",
		Outbox: ACPUpgradeDrainOutboxBarrierFunc(func(context.Context) (int64, error) { return 2, nil }),
	}
	snapshot, err := observer.ObserveACPUpgradeDrainBarriers(context.Background())
	if err != nil {
		t.Fatalf("ObserveACPUpgradeDrainBarriers: %v", err)
	}
	if snapshot.UnsettledOutboxProjections != 2 || snapshot.NonterminalPromptAttempts != 2 ||
		snapshot.ActiveSessionControls != 1 || snapshot.NonterminalPublications != 1 || snapshot.NonterminalExternalEffects != 1 {
		t.Fatalf("snapshot = %#v, want queued handoff excluded but Reserved and Running attempts blocked", snapshot)
	}
	if snapshot.Quiescent() {
		t.Fatal("non-zero durable barriers reported quiescent")
	}
}

func TestKubernetesACPUpgradeDrainBarrierObserverFencesOutboxMigration(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	counts := []int64{1, 0}
	var mu sync.Mutex
	observer := &KubernetesACPUpgradeDrainBarrierObserver{
		Reader: kubeClient,
		Outbox: ACPUpgradeDrainOutboxBarrierFunc(func(context.Context) (int64, error) {
			mu.Lock()
			defer mu.Unlock()
			value := counts[0]
			counts = counts[1:]
			return value, nil
		}),
	}
	snapshot, err := observer.ObserveACPUpgradeDrainBarriers(context.Background())
	if err != nil {
		t.Fatalf("ObserveACPUpgradeDrainBarriers: %v", err)
	}
	if snapshot.UnsettledOutboxProjections != 1 || snapshot.Quiescent() {
		t.Fatalf("snapshot = %#v, want pre-observation outbox work to remain a barrier", snapshot)
	}
}

func TestACPUpgradeDrainHandlerSupportsSameBinaryPreStop(t *testing.T) {
	scheme := upgradeDrainTestScheme(t)
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 23, HolderID: "controller-23"}
	epochRecord, epochLease := upgradeDrainEpochObjects(fence)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(epochRecord, epochLease).Build()
	coordinator := NewACPUpgradeDrainCoordinator(
		kubeClient,
		kubeClient,
		fixedUpgradeDrainEpochSource{fence: fence},
		&fakeUpgradeDrainEpochStore{current: store.ControllerEpoch{Name: fence.Name, Epoch: fence.Epoch, HolderID: fence.HolderID}},
		ACPUpgradeDrainBarrierObserverFunc(func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
			return ACPUpgradeDrainBarrierSnapshot{}, nil
		}),
		NewACPAdmissionGate(),
		testUpgradeDrainOptions(),
	)
	server := httptest.NewServer(coordinator.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	handled, err := RunACPUpgradeDrainTriggerMode(ctx, ACPUpgradeDrainOptions{
		TriggerURL: server.URL + DefaultACPUpgradeDrainPath, TriggerTimeout: time.Second,
	})
	if err != nil || !handled {
		t.Fatalf("RunACPUpgradeDrainTriggerMode() = %t, %v, want handled success", handled, err)
	}
	if status := coordinator.Status(); status.Phase != ACPUpgradeDrainCompleted {
		t.Fatalf("status phase = %s, want Completed", status.Phase)
	}
}

func TestRequestACPUpgradeDrainUsesLoopbackSameBinaryEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != DefaultACPUpgradeDrainPath {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := RequestACPUpgradeDrain(ctx, server.URL+DefaultACPUpgradeDrainPath); err != nil {
		t.Fatalf("RequestACPUpgradeDrain: %v", err)
	}
	if err := RequestACPUpgradeDrain(ctx, "http://example.com"+DefaultACPUpgradeDrainPath); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("non-loopback trigger error = %v, want rejection", err)
	}
}

func TestUpgradeDrainSupervisorQuiescenceIsStrict(t *testing.T) {
	status := harnessv2.StatusResponse{Drain: harnessv2.DrainStatus{Requested: true, AcceptingNewSessions: false}}
	if !upgradeDrainSupervisorIsQuiescent(status) {
		t.Fatal("zero authenticated drain status was not quiescent")
	}
	status.Pressure.LiveDescendants = 1
	if upgradeDrainSupervisorIsQuiescent(status) {
		t.Fatal("live descendant was accepted as quiescent")
	}
	status.Pressure.LiveDescendants = 0
	status.PendingPermissions = []harnessv2.PendingPermissionStatus{{}}
	if upgradeDrainSupervisorIsQuiescent(status) {
		t.Fatal("pending permission record was accepted as quiescent")
	}
}

func upgradeDrainTestScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	scheme := runtimePoolTestScheme(t)
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatalf("Add coordination/v1 to scheme: %v", err)
	}
	return scheme
}

func upgradeDrainEpochObjects(fence store.ControllerEpochFence) (*corev1alpha1.ControllerEpoch, *coordinationv1.Lease) {
	const namespace = runtimePoolDefaultControllerNamespace
	leaseName := "controller-epoch-authority"
	holder := fence.HolderID
	return &corev1alpha1.ControllerEpoch{
		ObjectMeta: metav1.ObjectMeta{Name: "controller-epoch", Namespace: namespace},
		Spec:       corev1alpha1.ControllerEpochSpec{Name: fence.Name},
		Status: corev1alpha1.ControllerEpochStatus{
			Epoch: fence.Epoch, HolderID: fence.HolderID, LeaseName: leaseName, LeaseResourceVersion: "1",
		},
	}, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: namespace},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder},
	}
}

func testUpgradeDrainOptions() ACPUpgradeDrainOptions {
	return ACPUpgradeDrainOptions{
		BindAddress:     "127.0.0.1:0",
		Timeout:         500 * time.Millisecond,
		PollInterval:    5 * time.Millisecond,
		MarkerNamespace: runtimePoolDefaultControllerNamespace,
		WatchNamespace:  "tenant-a",
		TriggerTimeout:  time.Second,
	}
}

func upgradeDrainRuntimePoolFixture(t *testing.T) (*corev1alpha1.RuntimePool, corev1.Pod, *corev1.Secret, *fakeUpgradeDrainSupervisor) {
	t.Helper()
	pool := runtimePoolTestObject(1)
	pod := runtimePoolReadyPod(pool, pool.Namespace, "codex-pod", "pod-uid-upgrade", "10.0.0.31")
	probe := runtimePoolValidProbe(pool, &pod, "boot-upgrade", false)
	pool.Status = corev1alpha1.RuntimePoolStatus{
		ObservedGeneration: pool.Generation,
		ControllerEpoch:    7,
		DesiredReplicas:    1,
		CurrentReplicas:    1,
		Lifecycle:          corev1alpha1.RuntimePoolLifecycleServing,
		AdmissionState:     corev1alpha1.RuntimePoolAdmissionAccepting,
		ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{
			PodNamespace: pool.Namespace, PodName: pod.Name, PodAddress: pod.Status.PodIP,
			PodUID: string(pod.UID), BootID: "boot-upgrade",
			RuntimeInstanceID: runtimePoolRuntimeInstanceID(pod.UID, "boot-upgrade"), ControllerEpoch: 7,
			ProtocolVersion: corev1alpha1.RuntimePoolProtocolHarnessV2,
			ProfileDigest:   pool.Spec.Runtime.Profile.Digest, ProfileDigestSchemaVersion: pool.Spec.Runtime.Profile.DigestSchemaVersion,
		},
		Capacity: corev1alpha1.RuntimePoolCapacityStatus{MaxResidentSessions: 10, MaxRunningPrompts: 4},
	}
	auth := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pool.Namespace,
			Name:      runtimePoolChildName(runtimePoolResourceName(pool.Namespace, pool.Name), "auth-e7"),
			Labels: map[string]string{
				runtimePoolAuthLabel:            booleanTrueValue,
				runtimePoolUIDLabel:             string(pool.UID),
				runtimePoolCredentialEpochLabel: "7",
			},
		},
		Data: map[string][]byte{
			runtimePoolControllerTokenKey:  []byte("controller-token"),
			runtimePoolCapabilitySecretKey: []byte("capability-secret-capability-secret"),
		},
	}
	return pool, pod, auth, &fakeUpgradeDrainSupervisor{
		probe: probe, expectedEndpoint: runtimePoolPodEndpoint(&pod), expectedToken: "controller-token",
		expectedCapability: bytes.Clone(auth.Data[runtimePoolCapabilitySecretKey]),
	}
}

type epochTurnoverOnCompletionClient struct {
	client.Client
	epochStore *fakeUpgradeDrainEpochStore
	once       sync.Once
}

func (c *epochTurnoverOnCompletionClient) Update(
	ctx context.Context,
	object client.Object,
	opts ...client.UpdateOption,
) error {
	lease, ok := object.(*coordinationv1.Lease)
	if ok {
		marker, present, err := decodeACPUpgradeDrainMarker(lease.Annotations[acpUpgradeDrainMarkerAnnotation])
		if err == nil && present && marker.State == ACPUpgradeDrainMarkerCompleted {
			c.once.Do(func() {
				current := &coordinationv1.Lease{}
				if getErr := c.Get(ctx, client.ObjectKeyFromObject(lease), current); getErr != nil {
					return
				}
				replacement := current.DeepCopy()
				holder := "replacement-controller"
				replacement.Spec.HolderIdentity = &holder
				if updateErr := c.Client.Update(ctx, replacement); updateErr != nil {
					return
				}
				c.epochStore.setCurrent(store.ControllerEpoch{
					Name: marker.ControllerEpochName, Epoch: marker.ControllerEpoch + 1, HolderID: holder,
				})
			})
		}
	}
	return c.Client.Update(ctx, object, opts...)
}

type fixedUpgradeDrainEpochSource struct {
	fence store.ControllerEpochFence
}

func (s fixedUpgradeDrainEpochSource) CurrentFence(context.Context) (store.ControllerEpochFence, error) {
	return s.fence, nil
}

type fakeUpgradeDrainEpochStore struct {
	mu        sync.Mutex
	current   store.ControllerEpoch
	reads     int
	loseAfter int
}

func (s *fakeUpgradeDrainEpochStore) GetControllerEpoch(context.Context, string) (*store.ControllerEpoch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	current := s.current
	if s.loseAfter > 0 && s.reads > s.loseAfter {
		current.Epoch++
		current.HolderID = "replacement-controller"
	}
	return &current, nil
}

func (s *fakeUpgradeDrainEpochStore) CompareAndSwapControllerEpoch(context.Context, store.ControllerEpochCAS) (*store.ControllerEpoch, error) {
	return nil, errors.New("not implemented")
}

func (s *fakeUpgradeDrainEpochStore) setCurrent(current store.ControllerEpoch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current = current
}

type fakeUpgradeDrainSupervisor struct {
	mu                 sync.Mutex
	probe              RuntimePoolProbeResult
	drainCalls         int
	expectedEndpoint   string
	expectedToken      string
	expectedCapability []byte
}

func (s *fakeUpgradeDrainSupervisor) Probe(_ context.Context, endpoint, token string, _ []byte) (RuntimePoolProbeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if endpoint != s.expectedEndpoint {
		return RuntimePoolProbeResult{}, errors.New("unexpected exact-Pod endpoint")
	}
	if token != s.expectedToken {
		return RuntimePoolProbeResult{}, errors.New("unexpected RuntimePool controller token")
	}
	return s.probe, nil
}

func (s *fakeUpgradeDrainSupervisor) RequestDrain(
	_ context.Context,
	endpoint, token string,
	capability []byte,
	prior harnessv2.StatusResponse,
	reason string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if endpoint != s.expectedEndpoint || token != s.expectedToken || !bytes.Equal(capability, s.expectedCapability) {
		return errors.New("unexpected authenticated drain target or credential")
	}
	if prior.Fence.RuntimeInstanceID != s.probe.Status.Fence.RuntimeInstanceID ||
		prior.Fence.SupervisorBootID != s.probe.Status.Fence.SupervisorBootID ||
		prior.Fence.RuntimePoolUID != s.probe.Status.Fence.RuntimePoolUID {
		return errors.New("unexpected exact-instance status fence")
	}
	s.drainCalls++
	s.probe.Status.Lifecycle = harnessv2.SupervisorLifecycleDraining
	s.probe.Status.Drain = harnessv2.DrainStatus{
		Requested: true, AcceptingNewSessions: false,
		RequestedAt: time.Now().UTC(), Reason: reason,
	}
	return nil
}

func (s *fakeUpgradeDrainSupervisor) drainCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.drainCalls
}

func TestRuntimeSessionControlAvailableWithoutLeaseIsQuiescent(t *testing.T) {
	control := &corev1alpha1.RuntimeSessionControl{Status: corev1alpha1.RuntimeSessionControlStatus{
		Availability: corev1alpha1.RuntimeSessionControlAvailability(store.SessionAvailable),
	}}
	if runtimeSessionControlHasUpgradeDrainWork(control) {
		t.Fatal("lease-free Available SessionControl blocked planned drain")
	}
	control.Status.MutationLease = &corev1alpha1.RuntimeSessionMutationLeaseStatus{Generation: 1}
	if !runtimeSessionControlHasUpgradeDrainWork(control) {
		t.Fatal("active Session mutation lease did not block planned drain")
	}
}

// ReadACPUpgradeDrainMarker returns the marker atomically ordered on the
// authoritative controller-epoch Lease.
func ReadACPUpgradeDrainMarker(
	ctx context.Context,
	reader client.Reader,
	namespace, epochName string,
) (ACPUpgradeDrainMarker, error) {
	lease, err := readACPUpgradeDrainControllerEpochLease(ctx, reader, namespace, epochName)
	if err != nil {
		return ACPUpgradeDrainMarker{}, err
	}
	marker, present, err := decodeACPUpgradeDrainMarker(lease.Annotations[acpUpgradeDrainMarkerAnnotation])
	if err != nil {
		return ACPUpgradeDrainMarker{}, err
	}
	if !present {
		return ACPUpgradeDrainMarker{}, store.ErrNotFound
	}
	return marker, nil
}

// ACPUpgradeDrainCompletedForEpoch is the only planned-takeover predicate. A
// timeout or merely persisted intent intentionally returns false.
func ACPUpgradeDrainCompletedForEpoch(
	ctx context.Context,
	reader client.Reader,
	namespace, epochName string,
	epoch int64,
) (bool, error) {
	marker, err := ReadACPUpgradeDrainMarker(ctx, reader, namespace, epochName)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return marker.ControllerEpoch == epoch && marker.State == ACPUpgradeDrainMarkerCompleted, nil
}
