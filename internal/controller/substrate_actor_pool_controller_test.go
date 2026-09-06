/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"strings"
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/workspace"
)

func newSubstrateActorPoolTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatalf("coordination AddToScheme() error = %v", err)
	}
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   "ate.dev",
		Version: "v1alpha1",
		Kind:    "ActorTemplate",
	}, &unstructured.Unstructured{})
	return scheme
}

func TestSubstrateActorPoolReconcilerPrecreatesActorsAndUpdatesDensity(t *testing.T) {
	scheme := newSubstrateActorPoolTestScheme(t)
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{Name: "codex-pool", Namespace: "default"},
		Spec: corev1alpha1.SubstrateActorPoolSpec{
			TemplateRef:     corev1alpha1.WorkspaceTemplateReference{Name: "codex", Namespace: "ate-demo"},
			TargetActors:    3,
			PrecreateActors: true,
		},
	}
	template := readySubstrateActorTemplateForTest([]any{
		map[string]any{
			"name":  "ORKA_WORKSPACE_BOOTSTRAP_TOKEN",
			"value": "bootstrap-token",
		},
	})
	template.SetName("codex")
	template.SetNamespace("ate-demo")
	executor := &recordingSubstratePoolExecutor{
		density: workspace.Density{
			WorkerCount:         1,
			ActorCount:          3,
			RunningActorCount:   1,
			SuspendedActorCount: 2,
			ActorsPerWorker:     "3.00",
		},
	}
	reconciler := &SubstrateActorPoolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.SubstrateActorPool{}).WithObjects(pool, template).Build(),
		Scheme:           scheme,
		SubstrateEnabled: true,
		SubstrateExecutorFactory: func(SubstrateConfig) (SubstratePoolExecutor, error) {
			return executor, nil
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "codex-pool", Namespace: "default"}}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() add finalizer error = %v", err)
	}
	var got corev1alpha1.SubstrateActorPool
	if err := reconciler.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatalf("Get pool after finalizer: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, substrateActorPoolFinalizer) {
		t.Fatal("pool finalizer was not added before actor convergence")
	}
	if executor.convergeCalled {
		t.Fatal("ConvergeSubstrateActors was called before finalizer was persisted")
	}

	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if executor.convergeTarget != 3 {
		t.Fatalf("converge target = %d, want 3", executor.convergeTarget)
	}
	if !executor.convergeCalled {
		t.Fatal("ConvergeSubstrateActors was not called")
	}
	if !executor.closeCalled {
		t.Fatal("Substrate pool executor was not closed after reconcile")
	}
	if err := reconciler.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatalf("Get pool: %v", err)
	}
	if got.Status.Phase != corev1alpha1.SubstrateActorPoolPhaseReady ||
		got.Status.ActorCount != 3 ||
		got.Status.WorkerCount != 1 ||
		got.Status.ActorsPerWorker != "3.00" {
		t.Fatalf("status = %#v, want ready density", got.Status)
	}
}

func TestSubstrateActorPoolReconcilerAcceptsMCPOnlyTemplate(t *testing.T) {
	scheme := newSubstrateActorPoolTestScheme(t)
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-pool", Namespace: "default"},
		Spec: corev1alpha1.SubstrateActorPoolSpec{
			TemplateRef:     corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
			TargetActors:    1,
			PrecreateActors: true,
		},
	}
	template := approvedMCPActorTemplateForTest()
	executor := &recordingSubstratePoolExecutor{
		density: workspace.Density{
			WorkerCount:     1,
			ActorCount:      1,
			ActorsPerWorker: "1.00",
		},
	}
	reconciler := &SubstrateActorPoolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.SubstrateActorPool{}).WithObjects(pool, template).Build(),
		Scheme:           scheme,
		SubstrateEnabled: true,
		SubstrateExecutorFactory: func(SubstrateConfig) (SubstratePoolExecutor, error) {
			return executor, nil
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "mcp-pool", Namespace: "default"}}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() add finalizer error = %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !executor.convergeCalled {
		t.Fatal("ConvergeSubstrateActors was not called for MCP-only pool template")
	}
	var got corev1alpha1.SubstrateActorPool
	if err := reconciler.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatalf("Get pool: %v", err)
	}
	if got.Status.Phase != corev1alpha1.SubstrateActorPoolPhaseReady {
		t.Fatalf("phase = %q, want Ready; status=%#v", got.Status.Phase, got.Status)
	}
}

func TestSubstrateActorPoolReconcilerPrecreatesZeroTarget(t *testing.T) {
	scheme := newSubstrateActorPoolTestScheme(t)
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{Name: "codex-pool", Namespace: "default"},
		Spec: corev1alpha1.SubstrateActorPoolSpec{
			TemplateRef:     corev1alpha1.WorkspaceTemplateReference{Name: "codex", Namespace: "ate-demo"},
			TargetActors:    0,
			PrecreateActors: true,
		},
	}
	template := readySubstrateActorTemplateForTest([]any{
		map[string]any{
			"name":  "ORKA_WORKSPACE_BOOTSTRAP_TOKEN",
			"value": "bootstrap-token",
		},
	})
	template.SetName("codex")
	template.SetNamespace("ate-demo")
	executor := &recordingSubstratePoolExecutor{}
	reconciler := &SubstrateActorPoolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.SubstrateActorPool{}).WithObjects(pool, template).Build(),
		Scheme:           scheme,
		SubstrateEnabled: true,
		SubstrateExecutorFactory: func(SubstrateConfig) (SubstratePoolExecutor, error) {
			return executor, nil
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "codex-pool", Namespace: "default"}}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() add finalizer error = %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !executor.convergeCalled {
		t.Fatal("ConvergeSubstrateActors was not called for zero target")
	}
	if executor.convergeTarget != 0 {
		t.Fatalf("converge target = %d, want 0", executor.convergeTarget)
	}
}

func TestSubstrateActorPoolReconcilerRejectsOversizedTargetBeforeConverge(t *testing.T) {
	scheme := newSubstrateActorPoolTestScheme(t)
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{Name: "codex-pool", Namespace: "default"},
		Spec: corev1alpha1.SubstrateActorPoolSpec{
			TemplateRef:     corev1alpha1.WorkspaceTemplateReference{Name: "codex", Namespace: "ate-demo"},
			TargetActors:    corev1alpha1.MaxSubstrateActorPoolTargetActors + 1,
			PrecreateActors: true,
		},
	}
	executor := &recordingSubstratePoolExecutor{}
	reconciler := &SubstrateActorPoolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.SubstrateActorPool{}).WithObjects(pool).Build(),
		Scheme:           scheme,
		SubstrateEnabled: true,
		SubstrateExecutorFactory: func(SubstrateConfig) (SubstratePoolExecutor, error) {
			return executor, nil
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "codex-pool", Namespace: "default"}}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if executor.convergeCalled {
		t.Fatal("ConvergeSubstrateActors was called for oversized target")
	}
	var got corev1alpha1.SubstrateActorPool
	if err := reconciler.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatalf("Get pool: %v", err)
	}
	if got.Status.Phase != corev1alpha1.SubstrateActorPoolPhaseFailed {
		t.Fatalf("status phase = %s, want Failed", got.Status.Phase)
	}
	if !strings.Contains(got.Status.Message, "no greater than") {
		t.Fatalf("status message = %q, want target cap context", got.Status.Message)
	}
	if controllerutil.ContainsFinalizer(&got, substrateActorPoolFinalizer) {
		t.Fatal("pool finalizer was added despite invalid targetActors")
	}
}

func TestSubstrateActorPoolReconcilerPrunesActorsWithoutPrecreate(t *testing.T) {
	scheme := newSubstrateActorPoolTestScheme(t)
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{Name: "codex-pool", Namespace: "default"},
		Spec: corev1alpha1.SubstrateActorPoolSpec{
			TemplateRef:  corev1alpha1.WorkspaceTemplateReference{Name: "codex", Namespace: "ate-demo"},
			TargetActors: 3,
		},
	}
	template := readySubstrateActorTemplateForTest([]any{
		map[string]any{
			"name":  "ORKA_WORKSPACE_BOOTSTRAP_TOKEN",
			"value": "bootstrap-token",
		},
	})
	template.SetName("codex")
	template.SetNamespace("ate-demo")
	executor := &recordingSubstratePoolExecutor{}
	reconciler := &SubstrateActorPoolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.SubstrateActorPool{}).WithObjects(pool, template).Build(),
		Scheme:           scheme,
		SubstrateEnabled: true,
		SubstrateExecutorFactory: func(SubstrateConfig) (SubstratePoolExecutor, error) {
			return executor, nil
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "codex-pool", Namespace: "default"}}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() add finalizer error = %v", err)
	}
	var got corev1alpha1.SubstrateActorPool
	if err := reconciler.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatalf("Get pool after finalizer: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, substrateActorPoolFinalizer) {
		t.Fatal("pool finalizer was not added")
	}
	if executor.convergeCalled {
		t.Fatal("ConvergeSubstrateActors was called for non-precreating pool")
	}
	if executor.pruneCalled {
		t.Fatal("PruneSubstrateActors was called before finalizer was persisted")
	}

	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if executor.convergeCalled {
		t.Fatal("ConvergeSubstrateActors was called for non-precreating pool")
	}
	if !executor.pruneCalled {
		t.Fatal("PruneSubstrateActors was not called for non-precreating pool")
	}
	if executor.pruneTarget != 3 {
		t.Fatalf("prune target = %d, want 3", executor.pruneTarget)
	}
}

func TestSubstrateActorPoolReconcilerFinalizesPrecreatedActors(t *testing.T) {
	scheme := newSubstrateActorPoolTestScheme(t)
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "codex-pool",
			Namespace:  "default",
			Finalizers: []string{substrateActorPoolFinalizer},
		},
	}
	executor := &recordingSubstratePoolExecutor{}
	reconciler := &SubstrateActorPoolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool).Build(),
		Scheme: scheme,
		SubstrateExecutorFactory: func(SubstrateConfig) (SubstratePoolExecutor, error) {
			return executor, nil
		},
	}

	if _, err := reconciler.finalizeSubstrateActorPool(context.Background(), pool, deterministicSubstratePoolActorPrefix(pool.Namespace, pool.Name)); err != nil {
		t.Fatalf("finalizeSubstrateActorPool() error = %v", err)
	}
	if !executor.convergeCalled || executor.convergeTarget != 0 {
		t.Fatalf("converge called=%t target=%d, want called target 0", executor.convergeCalled, executor.convergeTarget)
	}
	if !executor.closeCalled {
		t.Fatal("Substrate pool executor was not closed after finalization")
	}
	var got corev1alpha1.SubstrateActorPool
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: "codex-pool", Namespace: "default"}, &got); err != nil {
		t.Fatalf("Get pool: %v", err)
	}
	if controllerutil.ContainsFinalizer(&got, substrateActorPoolFinalizer) {
		t.Fatal("pool finalizer was not removed after actor cleanup")
	}
}

func TestSubstrateActorPoolReconcilerDefersScaleDownWithActiveLease(t *testing.T) {
	scheme := newSubstrateActorPoolTestScheme(t)
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "codex-pool",
			Namespace:  "default",
			Finalizers: []string{substrateActorPoolFinalizer},
		},
		Spec: corev1alpha1.SubstrateActorPoolSpec{
			TemplateRef:     corev1alpha1.WorkspaceTemplateReference{Name: "codex", Namespace: "ate-demo"},
			TargetActors:    2,
			PrecreateActors: true,
		},
	}
	template := readySubstrateActorTemplateForTest([]any{
		map[string]any{
			"name":  "ORKA_WORKSPACE_BOOTSTRAP_TOKEN",
			"value": "bootstrap-token",
		},
	})
	template.SetName("codex")
	template.SetNamespace("ate-demo")
	prefix := deterministicSubstratePoolActorPrefix(pool.Namespace, pool.Name)
	holder := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "running-tool", Namespace: "default", UID: "running-tool-uid"},
	}
	lease := newSubstrateMCPPoolActorLease(holder, pool.Namespace, deterministicSubstratePoolActorID(prefix, 2), deterministicSubstratePoolActorID(prefix, 2))
	executor := &recordingSubstratePoolExecutor{}
	reconciler := &SubstrateActorPoolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.SubstrateActorPool{}).WithObjects(pool, template, holder, lease).Build(),
		Scheme:           scheme,
		SubstrateEnabled: true,
		SubstrateExecutorFactory: func(SubstrateConfig) (SubstratePoolExecutor, error) {
			return executor, nil
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "codex-pool", Namespace: "default"}}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if executor.convergeCalled {
		t.Fatal("ConvergeSubstrateActors was called while actor above target is actively leased")
	}
	var got corev1alpha1.SubstrateActorPool
	if err := reconciler.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatalf("Get pool: %v", err)
	}
	if got.Status.Phase != corev1alpha1.SubstrateActorPoolPhasePending {
		t.Fatalf("status phase = %s, want Pending", got.Status.Phase)
	}
	if !strings.Contains(got.Status.Message, "active actor lease") {
		t.Fatalf("status message = %q, want active lease context", got.Status.Message)
	}
}

func TestSubstrateActorPoolReconcilerFinalizerWaitsForActiveLeases(t *testing.T) {
	scheme := newSubstrateActorPoolTestScheme(t)
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "codex-pool",
			Namespace:  "default",
			Finalizers: []string{substrateActorPoolFinalizer},
		},
	}
	prefix := deterministicSubstratePoolActorPrefix(pool.Namespace, pool.Name)
	holder := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "running-tool", Namespace: "default", UID: "running-tool-uid"},
	}
	lease := newSubstrateMCPPoolActorLease(holder, pool.Namespace, deterministicSubstratePoolActorID(prefix, 0), deterministicSubstratePoolActorID(prefix, 0))
	executor := &recordingSubstratePoolExecutor{}
	reconciler := &SubstrateActorPoolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, holder, lease).Build(),
		Scheme: scheme,
		SubstrateExecutorFactory: func(SubstrateConfig) (SubstratePoolExecutor, error) {
			return executor, nil
		},
	}

	result, err := reconciler.finalizeSubstrateActorPool(context.Background(), pool, prefix)
	if err != nil {
		t.Fatalf("finalizeSubstrateActorPool() error = %v", err)
	}
	if result.RequeueAfter != substrateActorPoolRequeue {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, substrateActorPoolRequeue)
	}
	if executor.convergeCalled {
		t.Fatal("ConvergeSubstrateActors was called while active leases exist")
	}
	var got corev1alpha1.SubstrateActorPool
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: "codex-pool", Namespace: "default"}, &got); err != nil {
		t.Fatalf("Get pool: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, substrateActorPoolFinalizer) {
		t.Fatal("pool finalizer was removed despite active lease")
	}
}

func TestSubstrateActorPoolReconcilerFinalizerWaitsForActiveToolLease(t *testing.T) {
	scheme := newSubstrateActorPoolTestScheme(t)
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-pool",
			Namespace:  "default",
			Finalizers: []string{substrateActorPoolFinalizer},
		},
	}
	prefix := deterministicSubstratePoolActorPrefix(pool.Namespace, pool.Name)
	actorID := deterministicSubstratePoolActorID(prefix, 0)
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: "default", UID: "mcp-tool-uid"},
	}
	lease := newSubstrateMCPPoolActorLease(tool, pool.Namespace, actorID, actorID)
	executor := &recordingSubstratePoolExecutor{}
	reconciler := &SubstrateActorPoolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, tool, lease).Build(),
		Scheme: scheme,
		SubstrateExecutorFactory: func(SubstrateConfig) (SubstratePoolExecutor, error) {
			return executor, nil
		},
	}

	result, err := reconciler.finalizeSubstrateActorPool(context.Background(), pool, prefix)
	if err != nil {
		t.Fatalf("finalizeSubstrateActorPool() error = %v", err)
	}
	if result.RequeueAfter != substrateActorPoolRequeue {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, substrateActorPoolRequeue)
	}
	if executor.convergeCalled {
		t.Fatal("ConvergeSubstrateActors was called while active tool lease exists")
	}
	var got corev1alpha1.SubstrateActorPool
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: "mcp-pool", Namespace: "default"}, &got); err != nil {
		t.Fatalf("Get pool: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, substrateActorPoolFinalizer) {
		t.Fatal("pool finalizer was removed despite active tool lease")
	}
}

type recordingSubstratePoolExecutor struct {
	convergeCalled bool
	convergeTarget int
	pruneCalled    bool
	pruneTarget    int
	closeCalled    bool
	density        workspace.Density
}

func (e *recordingSubstratePoolExecutor) ConvergeSubstrateActors(
	ctx context.Context,
	prefix string,
	target int,
	template workspace.TemplateRef,
) (int, int, error) {
	e.convergeCalled = true
	e.convergeTarget = target
	return target, 0, nil
}

func (e *recordingSubstratePoolExecutor) PruneSubstrateActors(
	ctx context.Context,
	prefix string,
	target int,
) (int, error) {
	e.pruneCalled = true
	e.pruneTarget = target
	return 0, nil
}

func (e *recordingSubstratePoolExecutor) SubstratePoolTelemetry(
	ctx context.Context,
	prefix string,
	template workspace.TemplateRef,
	workerPool workspace.TemplateRef,
) (workspace.Density, error) {
	return e.density, nil
}

func (e *recordingSubstratePoolExecutor) Close() error {
	e.closeCalled = true
	return nil
}
