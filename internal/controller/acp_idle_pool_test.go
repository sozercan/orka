package controller

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

func TestWorkspaceResumeTransitionPendingUsesAPIReader(t *testing.T) {
	const (
		testNamespace     = "default"
		testWorkspaceName = "workspace"
		testRuntimePool   = "pool"
	)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testWorkspaceName,
			UID:       types.UID("workspace-uid"),
		},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			DesiredState: workspacev1alpha1.ExecutionWorkspaceDesiredReady,
		},
		Status: workspacev1alpha1.ExecutionWorkspaceStatus{
			State: workspacev1alpha1.ExecutionWorkspaceStateSuspended,
		},
	}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testRuntimePool,
			Labels: map[string]string{
				acpExecutionWorkspaceLinkLabel: workspace.Name,
			},
			Annotations: map[string]string{
				acpExecutionWorkspaceUIDAnnotation: string(workspace.UID),
			},
		},
		Spec: corev1alpha1.RuntimePoolSpec{
			DesiredReplicas: 1,
			ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
				Provider: corev1alpha1.WorkspaceProviderSubstrate,
			},
		},
	}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(workspace).Build()
	staleWorkspace := workspace.DeepCopy()
	staleWorkspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	staleCache := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(
			ctx context.Context,
			delegate client.WithWatch,
			key client.ObjectKey,
			object client.Object,
			options ...client.GetOption,
		) error {
			if current, ok := object.(*workspacev1alpha1.ExecutionWorkspace); ok &&
				key == client.ObjectKeyFromObject(staleWorkspace) {
				staleWorkspace.DeepCopyInto(current)
				return nil
			}
			return delegate.Get(ctx, key, object, options...)
		},
	})

	if hold, err := (&ACPDispatcher{Client: staleCache}).workspaceResumeTransitionPending(context.Background(), pool); err != nil {
		t.Fatalf("read stale cached workspace: %v", err)
	} else if hold {
		t.Fatal("stale suspended intent unexpectedly held the resumed pool")
	}
	if hold, err := (&ACPDispatcher{Client: staleCache, APIReader: base}).workspaceResumeTransitionPending(context.Background(), pool); err != nil {
		t.Fatalf("read fresh workspace: %v", err)
	} else if !hold {
		t.Fatal("fresh resumed workspace did not hold the pool above zero")
	}
}

func TestReapIdlePoolsStartsTTLWhenActiveDemandSettles(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	settledAt := metav1.NewTime(now.Add(-5 * time.Second))
	oldDemand := now.Add(-2 * time.Hour)
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pool",
			UID:       types.UID("pool-uid"),
			Annotations: map[string]string{
				acpRuntimeLastDemandAnnotation: oldDemand.Format(time.RFC3339Nano),
			},
		},
		Spec: corev1alpha1.RuntimePoolSpec{DesiredReplicas: 1},
	}
	task := corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "completed",
			UID:       types.UID("task-uid"),
			Labels:    map[string]string{acpRuntimeTaskPoolLabel: pool.Name},
		},
		Status: corev1alpha1.TaskStatus{
			Execution: &corev1alpha1.TaskExecutionStatus{
				State:              corev1alpha1.TaskExecutionStateSucceeded,
				LastTransitionTime: &settledAt,
			},
			Delivery: &corev1alpha1.TaskDeliveryStatus{
				State:              corev1alpha1.TaskDeliveryStateVerifiedExact,
				LastTransitionTime: &settledAt,
			},
		},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RuntimePool{}).WithObjects(pool).Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "idle-pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "idle-pool-controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}

	dispatcher := &ACPDispatcher{Client: kubeClient, Epochs: epochs, IdlePoolTTL: time.Minute}
	if err := dispatcher.reapIdlePools(ctx, []corev1alpha1.Task{task}); err != nil {
		t.Fatal(err)
	}
	got := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.DesiredReplicas != 1 {
		t.Fatalf("DesiredReplicas = %d, want warm pool retained after demand settled", got.Spec.DesiredReplicas)
	}
	lastDemand, err := time.Parse(time.RFC3339Nano, got.Annotations[acpRuntimeLastDemandAnnotation])
	if err != nil {
		t.Fatalf("last demand annotation = %q: %v", got.Annotations[acpRuntimeLastDemandAnnotation], err)
	}
	if !lastDemand.Equal(settledAt.Time) {
		t.Fatalf("last demand = %s, want terminal demand time %s", lastDemand, settledAt.Time)
	}

	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestReapIdlePoolsRechecksFreshCapacityBeforeScaleDown(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	settledAt := metav1.NewTime(now.Add(-2 * time.Minute))
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "pool", UID: types.UID("pool-uid"),
			Annotations: map[string]string{acpRuntimeLastDemandAnnotation: now.Add(-time.Hour).Format(time.RFC3339Nano)},
		},
		Spec: corev1alpha1.RuntimePoolSpec{DesiredReplicas: 1},
	}
	task := corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "completed", Labels: map[string]string{acpRuntimeTaskPoolLabel: pool.Name}},
		Status: corev1alpha1.TaskStatus{
			Execution: &corev1alpha1.TaskExecutionStatus{State: corev1alpha1.TaskExecutionStateSucceeded, LastTransitionTime: &settledAt},
			Delivery:  &corev1alpha1.TaskDeliveryStatus{State: corev1alpha1.TaskDeliveryStateVerifiedExact, LastTransitionTime: &settledAt},
		},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RuntimePool{}).WithObjects(pool).Build()
	kubeClient := &capacityAppearsOnRefreshClient{
		Client: baseClient, pool: types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, activateOnRefresh: true,
	}
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "idle-pool-refresh.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "idle-pool-refresh-controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}

	dispatcher := &ACPDispatcher{Client: kubeClient, Epochs: epochs, IdlePoolTTL: time.Minute}
	if err := dispatcher.reapIdlePools(ctx, []corev1alpha1.Task{task}); err != nil {
		t.Fatal(err)
	}
	got := &corev1alpha1.RuntimePool{}
	if err := baseClient.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.DesiredReplicas != 1 {
		t.Fatalf("DesiredReplicas = %d, want active refreshed capacity to retain pool", got.Spec.DesiredReplicas)
	}

	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestReapIdlePoolsRetainsAttachedWorkspaceBeforeTaskDemandMetadata(t *testing.T) {
	const (
		namespace     = "default"
		poolName      = "pool"
		workspaceName = "workspace"
		taskName      = "task"
	)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	workspace := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: workspaceName, UID: types.UID("workspace-uid"),
			Annotations: map[string]string{acpExecutionWorkspacePoolAnnotation: poolName},
		},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			Lifecycle: workspacev1alpha1.ExecutionWorkspaceLifecycle{
				IdleTimeout: &metav1.Duration{Duration: time.Second},
			},
			Attachment: &workspacev1alpha1.ExecutionWorkspaceAttachment{
				TaskRef:   workspacev1alpha1.ObjectIdentityReference{Name: taskName, UID: types.UID("task-uid")},
				Epoch:     1,
				ExpiresAt: metav1.NewTime(now.Add(time.Hour)),
			},
		},
	}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: poolName, UID: types.UID("pool-uid"),
			Labels: map[string]string{acpExecutionWorkspaceLinkLabel: workspace.Name},
			Annotations: map[string]string{
				acpExecutionWorkspaceUIDAnnotation: string(workspace.UID),
				acpRuntimeLastDemandAnnotation:     now.Add(-time.Hour).Format(time.RFC3339Nano),
			},
		},
		Spec: corev1alpha1.RuntimePoolSpec{
			DesiredReplicas: 1,
			ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
				Provider: corev1alpha1.WorkspaceProviderAgentSandbox,
			},
		},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithObjects(workspace, pool).
		Build()
	epochs := NewControllerEpochManager(nil, "idle-pool-attachment-test")
	epochs.current = &store.ControllerEpoch{
		Name: store.DefaultControllerEpochName, Epoch: 1, HolderID: "idle-pool-attachment-test",
	}
	close(epochs.ready)
	dispatcher := &ACPDispatcher{
		Client: kubeClient, APIReader: kubeClient, Epochs: epochs, IdlePoolTTL: time.Minute,
	}

	if err := dispatcher.reapIdlePools(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	current := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(pool), current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.DesiredReplicas != 1 {
		t.Fatalf("attached workspace pool replicas = %d, want 1", current.Spec.DesiredReplicas)
	}
}

func TestReapIdlePoolsUsesOptimisticLockForFinalScaleDown(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	settledAt := metav1.NewTime(now.Add(-2 * time.Minute))
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "pool", UID: types.UID("pool-uid"),
			Annotations: map[string]string{acpRuntimeLastDemandAnnotation: settledAt.Format(time.RFC3339Nano)},
		},
		Spec: corev1alpha1.RuntimePoolSpec{DesiredReplicas: 1},
	}
	task := corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "completed", Labels: map[string]string{acpRuntimeTaskPoolLabel: pool.Name}},
		Status: corev1alpha1.TaskStatus{
			Execution: &corev1alpha1.TaskExecutionStatus{State: corev1alpha1.TaskExecutionStateSucceeded, LastTransitionTime: &settledAt},
			Delivery:  &corev1alpha1.TaskDeliveryStatus{State: corev1alpha1.TaskDeliveryStateVerifiedExact, LastTransitionTime: &settledAt},
		},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RuntimePool{}).WithObjects(pool).Build()
	kubeClient := &capacityAppearsOnRefreshClient{
		Client: baseClient, pool: types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, activateBeforeScalePatch: true,
	}
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "idle-pool-final-cas.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "idle-pool-final-cas-controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}

	dispatcher := &ACPDispatcher{Client: kubeClient, Epochs: epochs, IdlePoolTTL: time.Minute}
	if err := dispatcher.reapIdlePools(ctx, []corev1alpha1.Task{task}); err != nil {
		t.Fatal(err)
	}
	got := &corev1alpha1.RuntimePool{}
	if err := baseClient.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.DesiredReplicas != 1 || got.Status.Capacity.RunningPrompts != 1 {
		t.Fatalf("pool after final CAS race = replicas %d capacity %#v", got.Spec.DesiredReplicas, got.Status.Capacity)
	}

	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestACPTaskDemandSettledAtUsesStableLegacyFallback(t *testing.T) {
	created := metav1.NewTime(time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC))
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{CreationTimestamp: created},
		Status:     corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{State: corev1alpha1.TaskExecutionStateSucceeded}},
	}
	if got := acpTaskDemandSettledAt(task); !got.Equal(created.Time) {
		t.Fatalf("acpTaskDemandSettledAt() = %s, want stable creation fallback %s", got, created.Time)
	}
}

func TestRecordPoolLastDemandAtPreservesConcurrentNewerTimestamp(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	oldDemand := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	settledAt := oldDemand.Add(time.Minute)
	newerDemand := settledAt.Add(time.Minute)
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "pool",
			Annotations: map[string]string{acpRuntimeLastDemandAnnotation: oldDemand.Format(time.RFC3339Nano)},
		},
		Spec: corev1alpha1.RuntimePoolSpec{DesiredReplicas: 1},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool).Build()
	kubeClient := &capacityAppearsOnRefreshClient{
		Client: baseClient, pool: types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name},
		advanceDemandBeforeTimestampPatch: true, newerDemand: newerDemand,
	}
	dispatcher := &ACPDispatcher{Client: kubeClient}
	updated, err := dispatcher.recordPoolLastDemandAt(context.Background(), pool, settledAt)
	if err != nil {
		t.Fatal(err)
	}
	got, err := time.Parse(time.RFC3339Nano, updated.Annotations[acpRuntimeLastDemandAnnotation])
	if err != nil || !got.Equal(newerDemand) {
		t.Fatalf("last demand = %s, %v, want concurrent newer value %s", got, err, newerDemand)
	}
}

func TestReapStoppedSupersededPlainPoolDeletesAfterGrace(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	oldImage := "docker.io/sozercan/orka-acp@sha256:" + strings.Repeat("a", 64)
	newImage := "docker.io/sozercan/orka-acp@sha256:" + strings.Repeat("9", 64)

	tests := []struct {
		name        string
		images      ACPRuntimeImages
		activeTasks int
		wantDeleted bool
	}{
		{name: "retired and idle", images: ACPRuntimeImages{Codex: newImage}, wantDeleted: true},
		{name: "active Task still references pool", images: ACPRuntimeImages{Codex: newImage}, activeTasks: 1},
		{name: "image is still approved", images: ACPRuntimeImages{Codex: oldImage}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := runtimePoolTestObject(0)
			identity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
				"profileDigest": pool.Spec.Runtime.Profile.Digest,
				"runtimeImage":  pool.Spec.Runtime.Image,
			})
			if err != nil {
				t.Fatal(err)
			}
			pool.Name = acpRuntimePoolName(pool.Spec.Runtime.Profile.ProviderKind, harnessv2.ProfileDigest(identity))
			pool.UID = types.UID(pool.Namespace + "-retired-pool-uid")
			pool.Annotations = map[string]string{
				acpRuntimeLastDemandAnnotation: now.Add(-3 * time.Minute).Format(time.RFC3339Nano),
			}
			pool.Status = corev1alpha1.RuntimePoolStatus{
				ObservedGeneration: pool.Generation,
				DesiredReplicas:    0,
				CurrentReplicas:    0,
				Lifecycle:          corev1alpha1.RuntimePoolLifecycleStopped,
				AdmissionState:     corev1alpha1.RuntimePoolAdmissionClosed,
			}
			kubeClient := fake.NewClientBuilder().
				WithScheme(runtimePoolTestScheme(t)).
				WithStatusSubresource(&corev1alpha1.RuntimePool{}).
				WithObjects(pool).
				Build()
			dispatcher := &ACPDispatcher{Client: kubeClient, IdlePoolTTL: time.Minute, ACPRuntimeImages: tt.images}
			if err := dispatcher.reapStoppedSupersededPlainPool(context.Background(), pool, tt.activeTasks, now); err != nil {
				t.Fatal(err)
			}
			var pools corev1alpha1.RuntimePoolList
			if err := kubeClient.List(context.Background(), &pools); err != nil {
				t.Fatal(err)
			}
			if deleted := len(pools.Items) == 0; deleted != tt.wantDeleted {
				t.Fatalf("pool deleted = %t, want %t", deleted, tt.wantDeleted)
			}
		})
	}
}

type capacityAppearsOnRefreshClient struct {
	client.Client
	pool                              types.NamespacedName
	poolGets                          int
	activateOnRefresh                 bool
	activateBeforeScalePatch          bool
	advanceDemandBeforeTimestampPatch bool
	newerDemand                       time.Time
	demandAdvanced                    bool
}

func (c *capacityAppearsOnRefreshClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if err := c.Client.Get(ctx, key, object, options...); err != nil {
		return err
	}
	pool, ok := object.(*corev1alpha1.RuntimePool)
	if !ok || key != c.pool {
		return nil
	}
	c.poolGets++
	if c.activateOnRefresh && c.poolGets == 2 {
		pool.Status.Capacity.RunningPrompts = 1
		if err := c.Client.Status().Update(ctx, pool); err != nil {
			return err
		}
	}
	return nil
}

func (c *capacityAppearsOnRefreshClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	pool, ok := object.(*corev1alpha1.RuntimePool)
	if c.advanceDemandBeforeTimestampPatch && !c.demandAdvanced && ok && client.ObjectKeyFromObject(pool) == c.pool &&
		pool.Spec.DesiredReplicas == 1 && pool.Annotations[acpRuntimeLastDemandAnnotation] != "" {
		latest := &corev1alpha1.RuntimePool{}
		if err := c.Client.Get(ctx, c.pool, latest); err != nil {
			return err
		}
		if latest.Annotations == nil {
			latest.Annotations = make(map[string]string)
		}
		latest.Annotations[acpRuntimeLastDemandAnnotation] = c.newerDemand.Format(time.RFC3339Nano)
		if err := c.Update(ctx, latest); err != nil {
			return err
		}
		c.demandAdvanced = true
	}
	if c.activateBeforeScalePatch && ok && client.ObjectKeyFromObject(pool) == c.pool && pool.Spec.DesiredReplicas == 0 {
		latest := &corev1alpha1.RuntimePool{}
		if err := c.Client.Get(ctx, c.pool, latest); err != nil {
			return err
		}
		latest.Status.Capacity.RunningPrompts = 1
		if err := c.Client.Status().Update(ctx, latest); err != nil {
			return err
		}
	}
	return c.Client.Patch(ctx, object, patch, options...)
}
