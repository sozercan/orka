package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestRuntimePoolDrainPreservesSessionTaskCleanupBeforeReplacement(t *testing.T) {
	ctx := context.Background()
	pool := runtimePoolTestObject(1)
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, runtimePoolTestScheme(t), supervisor, pool)
	_, pod := runtimePoolTestStartServing(t, r, pool, supervisor, "retiring-pod", "retiring-pod-uid", "10.0.0.81", "retiring-boot")
	current := runtimePoolTestGetPool(t, r, pool)
	tasks := make([]*corev1alpha1.Task, 0, 2)
	for i := range 2 {
		task := runtimePoolRetirementTask(t, &current, fmt.Sprintf("retired-turn-%d", i+1))
		if err := r.Create(ctx, task); err != nil {
			t.Fatal(err)
		}
		tasks = append(tasks, task)
	}
	current.Spec.DesiredReplicas = 0
	if err := r.Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	runtimePoolReconcile(t, r, pool)
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "retiring-boot", true)
	runtimePoolReconcile(t, r, pool)
	runtimePoolReconcile(t, r, pool)
	for _, task := range tasks {
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
			t.Fatal(err)
		}
		if !runtimeSessionCleanupCompleteForUID(task, task.UID) {
			t.Fatalf("authenticated pool drain stopped the runtime without cleanup proof for Session Task %s", task.Name)
		}
	}
	if err := r.Delete(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	runtimePoolReconcile(t, r, pool)
	stopped := runtimePoolTestGetPool(t, r, pool)
	if stopped.Status.ActiveInstance != nil || stopped.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopped {
		t.Fatalf("pool after exact retirement = %#v", stopped.Status)
	}
	dispatcher := &ACPDispatcher{Client: r.Client, APIReader: r.Client}
	for _, task := range tasks {
		if ready, err := dispatcher.reconcileRecoveredRuntimeSession(ctx, task, task.UID, true, &sessionRuntimeCleanupFence{}); err != nil || !ready {
			t.Fatalf("Session cleanup lost retired runtime proof for %s: ready:%v err:%v", task.Name, ready, err)
		}
	}
}

func TestRuntimePoolReplacementRetainsSessionCleanupProof(t *testing.T) {
	for _, replacement := range []string{"profile", "identity capacity"} {
		t.Run(replacement, func(t *testing.T) {
			ctx := context.Background()
			pool := runtimePoolTestObject(1)
			supervisor := &fakeRuntimePoolSupervisorClient{}
			r := runtimePoolTestReconciler(t, runtimePoolTestScheme(t), supervisor, pool)
			deployment, pod := runtimePoolTestStartServing(t, r, pool, supervisor, "old-pod", "old-pod-uid", "10.0.0.81", "old-boot")
			current := runtimePoolTestGetPool(t, r, pool)
			task := runtimePoolRetirementTask(t, &current, "old-task")
			if err := r.Create(ctx, task); err != nil {
				t.Fatal(err)
			}
			if replacement == "profile" {
				current.Spec.Runtime.Profile.Model = runtimePoolTestNextModel
				current.Spec.Runtime.Profile.ProxyCredentialScope = "model:" + runtimePoolTestNextModel
				current.Generation++
				runtimePoolTestRefreshProfileDigest(t, &current)
				if err := r.Update(ctx, &current); err != nil {
					t.Fatal(err)
				}
			} else {
				supervisor.probe.Status.SessionIdentityCapacity.Remaining = supervisor.probe.Status.SessionIdentityCapacity.ExhaustionReserve
				supervisor.probe.Status.Drain.AcceptingNewSessions = false
				runtimePoolReconcile(t, r, pool)
			}
			runtimePoolReconcile(t, r, pool)
			if supervisor.drainCalls != 1 {
				t.Fatalf("replacement drain calls = %d, want 1", supervisor.drainCalls)
			}
			supervisor.probe.Status.Lifecycle = harnessv2.SupervisorLifecycleDraining
			supervisor.probe.Status.Drain = harnessv2.DrainStatus{Requested: true, Reason: supervisor.drainReason, RequestedAt: runtimePoolTestNow}
			failReceipt := true
			r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if _, isTask := obj.(*corev1alpha1.Task); isTask && failReceipt {
						failReceipt = false
						return errors.New("injected retirement receipt failure")
					}
					return c.SubResource(subresource).Update(ctx, obj, opts...)
				},
			})
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)}); err == nil || !strings.Contains(err.Error(), "injected retirement receipt failure") {
				t.Fatalf("replacement before durable cleanup proof = %v", err)
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(&pod), &corev1.Pod{}); err != nil {
				t.Fatalf("receipt failure removed the old Pod: %v", err)
			}
			if got := ptr.Deref(runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name).Spec.Replicas, -1); got != 1 {
				t.Fatalf("receipt failure changed old Deployment replicas to %d", got)
			}
			runtimePoolReconcile(t, r, pool)
			if got := runtimePoolTestGetPool(t, r, pool).Status.Lifecycle; got != corev1alpha1.RuntimePoolLifecycleQuiescent {
				t.Fatalf("receipt retry lifecycle = %s, want Quiescent", got)
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
				t.Fatal(err)
			}
			if !runtimeSessionCleanupCompleteForUID(task, task.UID) {
				t.Fatal("replacement barrier persisted without Task cleanup proof")
			}
			// A controller upgraded after persisting the quiescent barrier must
			// still record receipts before it destroys the old runtime.
			task.Status.Execution.RuntimeSessionCleanupDigest = ""
			if err := r.Status().Update(ctx, task); err != nil {
				t.Fatal(err)
			}
			runtimePoolReconcile(t, r, pool)
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
				t.Fatal(err)
			}
			if !runtimeSessionCleanupCompleteForUID(task, task.UID) {
				t.Fatal("resumed replacement forgot old runtime cleanup proof")
			}
			if replacement == "profile" {
				if err := r.Delete(ctx, &pod); err != nil {
					t.Fatal(err)
				}
			} else if err := r.Get(ctx, client.ObjectKeyFromObject(&pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Fatalf("identity replacement did not delete the retired Pod: %v", err)
			}
			runtimePoolReconcile(t, r, pool)
			current = runtimePoolTestGetPool(t, r, pool)
			deployment = runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name)
			newPod := runtimePoolReadyPodForDeployment(&current, deployment, "new-pod", "new-pod-uid", "10.0.0.82")
			runtimePoolTestCreatePod(t, r, &newPod)
			supervisor.probe = runtimePoolValidProbe(&current, &newPod, "new-boot", false)
			supervisor.probe.Capabilities.Provider.Models = []string{current.Spec.Runtime.Profile.Model}
			runtimePoolReconcile(t, r, pool)
			current = runtimePoolTestGetPool(t, r, pool)
			if current.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleServing || current.Status.ActiveInstance.RuntimeInstanceID == task.Status.Execution.RuntimeInstanceID {
				t.Fatalf("replacement failed to serve a new exact instance: %#v", current.Status)
			}
			dispatcher := &ACPDispatcher{Client: r.Client, APIReader: r.Client}
			if ready, err := dispatcher.reconcileRecoveredRuntimeSession(ctx, task, task.UID, true, &sessionRuntimeCleanupFence{}); err != nil || !ready {
				t.Fatalf("Session cleanup after replacement = ready:%v err:%v", ready, err)
			}
		})
	}
}

func TestRuntimePoolRetirementRejectsIncompleteQuiescenceOrIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*harnessv2.StatusResponse, *corev1alpha1.Task)
	}{
		{"resident Session", func(s *harnessv2.StatusResponse, _ *corev1alpha1.Task) { s.Pressure.ResidentSessions = 1 }},
		{"accepting sessions", func(s *harnessv2.StatusResponse, _ *corev1alpha1.Task) { s.Drain.AcceptingNewSessions = true }},
		{"pool UID", func(s *harnessv2.StatusResponse, _ *corev1alpha1.Task) { s.Fence.RuntimePoolUID = "other-pool" }},
		{"pool generation", func(s *harnessv2.StatusResponse, _ *corev1alpha1.Task) { s.Fence.RuntimePoolGeneration++ }},
		{"controller epoch", func(s *harnessv2.StatusResponse, _ *corev1alpha1.Task) { s.Fence.ControllerEpoch++ }},
		{"boot", func(s *harnessv2.StatusResponse, _ *corev1alpha1.Task) { s.Fence.SupervisorBootID = "other-boot" }},
		{"task boot", func(_ *harnessv2.StatusResponse, task *corev1alpha1.Task) {
			task.Status.Execution.RuntimeSessionSupervisorBootID = "other-boot"
		}},
		{"binding digest", func(_ *harnessv2.StatusResponse, task *corev1alpha1.Task) {
			task.Status.AgentExecutionBinding.BindingDigest = "tampered"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pool := runtimePoolTestObject(1)
			supervisor := &fakeRuntimePoolSupervisorClient{}
			r := runtimePoolTestReconciler(t, runtimePoolTestScheme(t), supervisor, pool)
			_, pod := runtimePoolTestStartServing(t, r, pool, supervisor, "old-pod", "old-pod-uid", "10.0.0.81", "old-boot")
			current := runtimePoolTestGetPool(t, r, pool)
			task := runtimePoolRetirementTask(t, &current, "old-task")
			status := runtimePoolValidProbe(pool, &pod, "old-boot", true).Status
			tt.mutate(&status, task)
			if err := r.Create(ctx, task); err != nil {
				t.Fatal(err)
			}
			if err := r.recordDrainedRuntimePoolTaskCleanup(ctx, &current, current.Status.ActiveInstance, status); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("invalid retirement evidence = %v, want conflict", err)
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
				t.Fatal(err)
			}
			if task.Status.Execution.RuntimeSessionCleanupDigest != "" {
				t.Fatal("invalid retirement evidence created cleanup proof")
			}
		})
	}
}

func runtimePoolRetirementTask(t *testing.T, pool *corev1alpha1.RuntimePool, name string) *corev1alpha1.Task {
	t.Helper()
	uid := types.UID(name + "-uid")
	snapshotDigest := store.CanonicalBytesDigest([]byte(name))
	binding := &corev1alpha1.AgentExecutionBinding{
		SchemaVersion: 1, ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
		Backend: corev1alpha1.AgentExecutionBackendRuntimePool, RuntimeType: corev1alpha1.AgentRuntimeCodex,
		Task:                 corev1alpha1.AgentExecutionBindingTaskRef{NamespaceUID: "tenant-a-uid", UID: uid, BoundSpecGeneration: 1},
		Agent:                &corev1alpha1.AgentExecutionAgentRef{Namespace: pool.Namespace, Name: "agent", UID: "agent-uid", Generation: 1},
		Snapshot:             corev1alpha1.AgentExecutionSnapshotRef{ID: string(uid) + "/" + snapshotDigest, Digest: snapshotDigest, SchemaVersion: 1},
		RuntimeProfileDigest: pool.Spec.Runtime.Profile.Digest, RuntimeProfileDigestSchemaVersion: 1,
	}
	var err error
	binding.BindingDigest, err = canonicalAgentExecutionBindingDigest(*binding)
	if err != nil {
		t.Fatal(err)
	}
	active := pool.Status.ActiveInstance
	return &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: pool.Namespace, Name: name, UID: uid, Generation: 1, Finalizers: []string{labels.TaskFinalizer}},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, SessionRef: &corev1alpha1.SessionReference{Name: "continued-session"}},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseSucceeded, AgentExecutionBinding: binding,
			Execution: &corev1alpha1.TaskExecutionStatus{
				Attempt: 1, PromptID: "prompt-" + name, State: corev1alpha1.TaskExecutionStateSucceeded, Outcome: corev1alpha1.TaskExecutionOutcomeSucceeded,
				RuntimePoolName: pool.Name, RuntimePoolUID: string(pool.UID), RuntimeInstanceID: active.RuntimeInstanceID,
				RuntimeSessionUID: "continued-session-uid", RuntimeSessionGeneration: 1,
				RuntimeSessionSupervisorBootID: active.BootID, RuntimeSessionProfileDigest: active.ProfileDigest,
				RequestDigest: "sha256:" + strings.Repeat("a", 64),
			},
			Delivery: &corev1alpha1.TaskDeliveryStatus{State: corev1alpha1.TaskDeliveryStateNotRequested, Outcome: corev1alpha1.TaskDeliveryOutcomeNotRequested},
		},
	}
}
