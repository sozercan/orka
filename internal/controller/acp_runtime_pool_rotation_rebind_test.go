package controller

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

//nolint:gocyclo // The regression verifies frozen binding and no-replay invariants in one durable attempt lifecycle.
func TestQueueACPRuntimeTaskRebindsPreSubmissionAttemptAfterRuntimeImageRotation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  "default",
			Name:       "image-rotation",
			UID:        types.UID("4a2b6ec0-6dc9-4d81-8810-a5e93e8ed301"),
			Generation: 2,
		},
		Spec: corev1alpha1.TaskSpec{
			Type:         corev1alpha1.TaskTypeAgent,
			Prompt:       "inspect the repository",
			AgentRuntime: &corev1alpha1.AgentRuntimeSpec{},
			Workspace: &corev1alpha1.WorkspaceConfig{
				Intent:  corev1alpha1.WorkspaceIntentRead,
				GitRepo: "https://github.com/orka-agents/orka.git",
			},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  "default",
			Name:       "codex",
			UID:        types.UID("ca9f870c-14d5-4e69-9577-2dcc6698d509"),
			Generation: 3,
		},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeCodex,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	oldImage := "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)
	newImage := "docker.io/example/codex@sha256:" + strings.Repeat("b", 64)
	oldPlan, err := PlanACPRuntime(task, agent, ACPRuntimeImages{Codex: oldImage})
	if err != nil {
		t.Fatal(err)
	}
	newPlan, err := PlanACPRuntime(task, agent, ACPRuntimeImages{Codex: newImage})
	if err != nil {
		t.Fatal(err)
	}
	oldPoolFixture := runtimePoolForImageRotationTest(
		task.Namespace, types.UID("4e7811bf-5202-4903-b8a6-54adca168a80"), oldPlan,
	)
	newPoolFixture := runtimePoolForImageRotationTest(
		task.Namespace, types.UID("d6906fa9-f0d6-4633-b8c8-33d392aef7e9"), newPlan,
	)
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).
		WithObjects(task, oldPoolFixture, newPoolFixture).
		Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "controller-test")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancelEpoch()
		if err := <-epochDone; err != nil {
			t.Errorf("epoch manager shutdown: %v", err)
		}
	}()

	reconciler := &TaskReconciler{
		Client: kubeClient, Scheme: scheme, DurableControlStore: controlStore,
		ControllerEpochManager: epochs, ACPRuntimeEnabled: true, ACPRuntimeNamespace: "orka-runtimes",
		ACPRuntimeImages: ACPRuntimeImages{Codex: oldImage},
	}
	bound := bindACPQueueTaskForTest(t, ctx, reconciler, task, agent)
	if _, err := reconciler.queueACPRuntimeTask(ctx, bound, agent); err != nil {
		t.Fatalf("initial queue: %v", err)
	}

	queuedBefore := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, queuedBefore); err != nil {
		t.Fatal(err)
	}
	before := queuedBefore.Status.Execution.DeepCopy()
	attemptID, err := storePromptKey(queuedBefore, before).CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	attemptBefore, err := controlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	oldPool := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: before.RuntimePoolName}, oldPool); err != nil {
		t.Fatal(err)
	}
	oldPool.Status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
	oldPool.Status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	if err := kubeClient.Status().Update(ctx, oldPool); err != nil {
		t.Fatal(err)
	}

	reconciler.ACPRuntimeImages = ACPRuntimeImages{Codex: newImage}
	if _, err := reconciler.queueACPRuntimeTask(ctx, queuedBefore.DeepCopy(), agent); err != nil {
		t.Fatalf("queue after image rotation: %v", err)
	}

	queuedAfter := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, queuedAfter); err != nil {
		t.Fatal(err)
	}
	if queuedAfter.Status.Execution == nil {
		t.Fatal("queued Task lost execution status")
	}
	after := queuedAfter.Status.Execution
	if after.State != corev1alpha1.TaskExecutionStateQueued {
		t.Fatalf("execution state = %s, want Queued", after.State)
	}
	if after.RuntimePoolName != newPlan.PoolName || after.RuntimePoolUID != string(newPoolFixture.UID) {
		t.Fatalf("queued Task did not move to the approved RuntimePool after image rotation: before=%#v after=%#v", before, after)
	}
	if queuedAfter.Labels[acpRuntimeTaskPoolLabel] != after.RuntimePoolName {
		t.Fatalf("pool label = %q, want %q", queuedAfter.Labels[acpRuntimeTaskPoolLabel], after.RuntimePoolName)
	}
	approvedPool := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: after.RuntimePoolName}, approvedPool); err != nil {
		t.Fatal(err)
	}
	if approvedPool.Spec.Runtime.Image != newImage || string(approvedPool.UID) != after.RuntimePoolUID {
		t.Fatalf("approved pool binding = image %q UID %q, status = %#v", approvedPool.Spec.Runtime.Image, approvedPool.UID, after)
	}
	if queuedAfter.Status.Attempts != queuedBefore.Status.Attempts || after.Attempt != before.Attempt || after.PromptID != before.PromptID || after.RequestDigest != before.RequestDigest {
		t.Fatalf("rebind changed durable attempt identity: before=%#v after=%#v attempts=%d/%d", before, after, queuedBefore.Status.Attempts, queuedAfter.Status.Attempts)
	}
	attemptAfter, err := controlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attemptAfter.ExecutionState != store.PromptExecutionQueued || attemptAfter.ID != attemptBefore.ID || attemptAfter.Key != attemptBefore.Key || attemptAfter.RequestDigest != attemptBefore.RequestDigest || attemptAfter.Version != attemptBefore.Version {
		t.Fatalf("durable attempt changed during pool rebind: before=%#v after=%#v", attemptBefore, attemptAfter)
	}

	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	accepted := attemptAfter
	for _, transition := range []struct {
		to        store.PromptExecutionState
		operation string
	}{
		{to: store.PromptExecutionReserved, operation: "test-reserve-before-next-rotation"},
		{to: store.PromptExecutionSessionStarting, operation: "test-start-session-before-next-rotation"},
		{to: store.PromptExecutionPlanned, operation: "test-plan-before-next-rotation"},
		{to: store.PromptExecutionSubmitting, operation: "test-submit-before-next-rotation"},
		{to: store.PromptExecutionAccepted, operation: "test-accept-before-next-rotation"},
	} {
		accepted = transitionPromptAttemptForImageRotationTest(t, ctx, controlStore, fence, accepted, transition.to, transition.operation)
	}
	thirdImage := "docker.io/example/codex@sha256:" + strings.Repeat("c", 64)
	thirdPlan, err := PlanACPRuntime(task, agent, ACPRuntimeImages{Codex: thirdImage})
	if err != nil {
		t.Fatal(err)
	}
	thirdPool := runtimePoolForImageRotationTest(
		task.Namespace, types.UID("0d53b279-1778-4539-8236-85e2ddc1a765"), thirdPlan,
	)
	if err := kubeClient.Create(ctx, thirdPool); err != nil {
		t.Fatal(err)
	}
	reconciler.ACPRuntimeImages = ACPRuntimeImages{Codex: thirdImage}
	if _, err := reconciler.queueACPRuntimeTask(ctx, queuedAfter.DeepCopy(), agent); err != nil {
		t.Fatalf("queue after accepted attempt and second image rotation: %v", err)
	}
	acceptedTask := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, acceptedTask); err != nil {
		t.Fatal(err)
	}
	if acceptedTask.Status.Execution == nil || acceptedTask.Status.Execution.RuntimePoolName != after.RuntimePoolName ||
		acceptedTask.Status.Execution.RuntimePoolUID != after.RuntimePoolUID || acceptedTask.Status.Attempts != queuedAfter.Status.Attempts {
		t.Fatalf("accepted prompt was rebound or replayed after later image rotation: before=%#v after=%#v", after, acceptedTask.Status.Execution)
	}
	acceptedAfter, err := controlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if acceptedAfter.ExecutionState != store.PromptExecutionAccepted || acceptedAfter.ID != accepted.ID || acceptedAfter.Key != accepted.Key {
		t.Fatalf("accepted prompt attempt changed after later image rotation: before=%#v after=%#v", accepted, acceptedAfter)
	}
}

func runtimePoolForImageRotationTest(namespace string, uid types.UID, plan ACPRuntimePlan) *corev1alpha1.RuntimePool {
	return &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: plan.PoolName, UID: uid},
		Spec: corev1alpha1.RuntimePoolSpec{
			TrustDomain:      corev1alpha1.RuntimePoolTrustDomain{Namespace: namespace, Identity: "namespace:" + namespace},
			RuntimeNamespace: "orka-runtimes",
			Runtime: corev1alpha1.RuntimePoolRuntimeSpec{
				Image:   plan.Image,
				Profile: RuntimePoolProfileFromPlan(plan),
			},
			DesiredReplicas: 1,
			Capacity: &corev1alpha1.RuntimePoolCapacitySpec{
				MaxResidentSessions: corev1alpha1.DefaultRuntimePoolMaxResidentSessions,
				MaxRunningPrompts:   corev1alpha1.DefaultRuntimePoolMaxRunningPrompts,
			},
			ColdStartTimeoutSeconds: corev1alpha1.DefaultRuntimePoolColdStartTimeoutSeconds,
		},
	}
}

func transitionPromptAttemptForImageRotationTest(
	t *testing.T,
	ctx context.Context,
	controlStore store.DurableControlStore,
	fence store.ControllerEpochFence,
	attempt *store.PromptAttempt,
	to store.PromptExecutionState,
	operation string,
) *store.PromptAttempt {
	t.Helper()
	digest, err := acpDomainDigest("attempt-transition", map[string]any{
		"id": attempt.ID, "from": attempt.ExecutionState, "to": to,
		"operation": operation, "version": attempt.Version,
	})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState, NewState: to,
		OperationID: operation, OperationDigest: digest, UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("transition prompt attempt to %s: %v", to, err)
	}
	return updated
}

func TestACPDispatcherRejectsStalePoolBindingAfterQueuedRebind(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	const requestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	current := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "reservation-race",
			UID:       types.UID("78428d45-9b6f-488a-b29b-b2fbbce69e04"),
		},
		Status: corev1alpha1.TaskStatus{
			Execution: &corev1alpha1.TaskExecutionStatus{
				State:           corev1alpha1.TaskExecutionStateQueued,
				Attempt:         1,
				PromptID:        "prompt-reservation-race-1",
				RequestDigest:   requestDigest,
				RuntimePoolName: "replacement-pool",
				RuntimePoolUID:  "replacement-pool-uid",
			},
		},
	}
	stale := current.DeepCopy()
	stale.Status.Execution.RuntimePoolName = "degraded-pool"
	stale.Status.Execution.RuntimePoolUID = "degraded-pool-uid"
	degradedPool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: current.Namespace, Name: "degraded-pool", UID: "degraded-pool-uid"},
	}
	replacementPool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: current.Namespace, Name: "replacement-pool", UID: "replacement-pool-uid"},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}).
		WithObjects(current, degradedPool, replacementPool).
		Build()
	dispatcher := &ACPDispatcher{Client: kubeClient, APIReader: kubeClient}

	bound, err := dispatcher.refreshTaskRuntimePoolBinding(context.Background(), stale, degradedPool)
	if err != nil {
		t.Fatal(err)
	}
	if bound {
		t.Fatal("stale degraded pool binding remained eligible after queued Task rebind")
	}
	bound, err = dispatcher.refreshTaskRuntimePoolBinding(context.Background(), stale, replacementPool)
	if err != nil {
		t.Fatal(err)
	}
	if !bound || stale.Status.Execution.RuntimePoolName != replacementPool.Name || stale.Status.Execution.RuntimePoolUID != string(replacementPool.UID) {
		t.Fatalf("replacement binding was not refreshed: bound=%t status=%#v", bound, stale.Status.Execution)
	}
}
