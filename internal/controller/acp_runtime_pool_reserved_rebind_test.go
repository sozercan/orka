package controller

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

func TestValidateFrozenACPDispatchTargetKeepsBoundRetryOnOriginalPool(t *testing.T) {
	profile := harnessProfileForTest()
	profile.AdapterDigests = acp.BuiltInRuntimeAdapterDigests(profile.ProviderKind)
	digest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	image := "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)
	identity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
		"profileDigest": string(digest), "runtimeImage": image,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := ACPRuntimePlan{
		PoolName: acpRuntimePoolName(profile.ProviderKind, harnessv2.ProfileDigest(identity)),
		Image:    image, Profile: profile, Digest: digest,
	}
	pool := runtimePoolForImageRotationTest("default", types.UID("old-pool-uid"), plan)
	task := &corev1alpha1.Task{
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			RuntimePoolName: pool.Name, RuntimePoolUID: string(pool.UID),
			RuntimeInstanceID: "old-runtime-instance", RuntimeSessionUID: "old-session-uid",
		}},
	}
	bound := &verifiedAgentExecution{
		binding: &corev1alpha1.AgentExecutionBinding{},
		body:    agentExecutionSnapshotBody{ProfileDigest: string(plan.Digest)},
		plan:    plan,
	}
	newImage := "docker.io/example/codex@sha256:" + strings.Repeat("b", 64)
	delivery, err := acpRuntimeDeliveryPlanForAttempt(
		plan, task.Status.Execution, nil, ACPRuntimeImages{Codex: newImage}, pool,
	)
	if err != nil {
		t.Fatal(err)
	}
	deliveryPlan := delivery.plan
	if !reflect.DeepEqual(deliveryPlan, plan) {
		t.Fatalf("runtime-bound retry delivery plan = %#v, want frozen %#v", deliveryPlan, plan)
	}
	if err := validateFrozenACPDispatchTarget(task, acpDispatchTarget{pool: pool}, bound, deliveryPlan); err != nil {
		t.Fatalf("bound retry no longer matched its exact original RuntimePool: %v", err)
	}
}

func TestTaskReconcilerKeepsBoundRetryOnReboundRuntimePool(t *testing.T) {
	profile := harnessProfileForTest()
	profile.AdapterDigests = acp.BuiltInRuntimeAdapterDigests(profile.ProviderKind)
	digest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	oldImage := "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)
	oldIdentity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
		"profileDigest": string(digest), "runtimeImage": oldImage,
	})
	if err != nil {
		t.Fatal(err)
	}
	frozenPlan := ACPRuntimePlan{
		PoolName: acpRuntimePoolName(profile.ProviderKind, harnessv2.ProfileDigest(oldIdentity)),
		Image:    oldImage, Profile: profile, Digest: digest,
	}
	reboundDelivery, err := currentACPRuntimeDeliveryPlan(
		frozenPlan,
		ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("b", 64)},
	)
	if err != nil {
		t.Fatal(err)
	}
	reboundPlan := reboundDelivery.plan
	pool := runtimePoolForImageRotationTest("default", types.UID("rebound-pool-uid"), reboundPlan)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: pool.Namespace, Name: "rebound-task"},
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			RuntimePoolName: pool.Name, RuntimePoolUID: string(pool.UID),
			RuntimeInstanceID: "rebound-runtime-instance", RuntimeSessionUID: "rebound-session-uid",
		}},
	}
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool).Build()
	reconciler := &TaskReconciler{
		Client:           kubeClient,
		ACPRuntimeImages: ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("c", 64)},
	}
	execution := task.Status.Execution
	delivery, err := reconciler.acpRuntimeDeliveryPlanForTaskAttempt(
		context.Background(),
		task,
		frozenPlan,
		&store.PromptAttempt{RuntimeInstanceID: execution.RuntimeInstanceID, SessionUID: execution.RuntimeSessionUID},
	)
	if err != nil {
		t.Fatal(err)
	}
	deliveryPlan := delivery.plan
	if !reflect.DeepEqual(deliveryPlan, reboundPlan) {
		t.Fatalf("runtime-bound retry delivery plan = %#v, want rebound plan %#v", deliveryPlan, reboundPlan)
	}
}

func TestACPRuntimeDeliveryPlanForBoundWorkspacePoolRejectsChangedImage(t *testing.T) {
	profile := harnessProfileForTest()
	profile.AdapterDigests = acp.BuiltInRuntimeAdapterDigests(profile.ProviderKind)
	digest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	plan := ACPRuntimePlan{
		PoolName: "acp-ws-codex-0123456789abcdef",
		Image:    "docker.io/example/codex@sha256:" + strings.Repeat("a", 64),
		Profile:  profile,
		Digest:   digest,
		Workspace: &ACPRuntimeWorkspaceBinding{
			Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
			BindingDigest: "sha256:" + strings.Repeat("b", 64),
		},
	}
	pool := runtimePoolForImageRotationTest("default", types.UID("workspace-pool-uid"), plan)
	pool.Spec.ExecutionWorkspace = &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
		Provider:      plan.Workspace.Provider,
		BindingDigest: plan.Workspace.BindingDigest,
	}
	pool.Spec.Runtime.Image = "docker.io/example/codex@sha256:" + strings.Repeat("c", 64)
	execution := &corev1alpha1.TaskExecutionStatus{
		RuntimePoolName:   pool.Name,
		RuntimePoolUID:    string(pool.UID),
		RuntimeInstanceID: "workspace-runtime-instance",
	}

	if _, err := acpRuntimeDeliveryPlanForAttempt(
		plan, execution, nil, ACPRuntimeImages{Codex: pool.Spec.Runtime.Image}, pool,
	); err == nil || !strings.Contains(err.Error(), "identity or image") {
		t.Fatalf("changed workspace image error = %v, want immutable image rejection", err)
	}
}

func TestValidateFrozenACPDispatchPlanKeepsUnboundReservedAttemptOnSelectedPool(t *testing.T) {
	profile := harnessProfileForTest()
	profile.AdapterDigests = acp.BuiltInRuntimeAdapterDigests(profile.ProviderKind)
	digest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	oldImage := "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)
	identity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
		"profileDigest": string(digest), "runtimeImage": oldImage,
	})
	if err != nil {
		t.Fatal(err)
	}
	frozenPlan := ACPRuntimePlan{
		PoolName: acpRuntimePoolName(profile.ProviderKind, harnessv2.ProfileDigest(identity)),
		Image:    oldImage, Profile: profile, Digest: digest,
	}
	currentDelivery, err := currentACPRuntimeDeliveryPlan(
		frozenPlan,
		ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("b", 64)},
	)
	if err != nil {
		t.Fatal(err)
	}
	currentPlan := currentDelivery.plan
	pool := runtimePoolForImageRotationTest("default", types.UID("old-pool-uid"), frozenPlan)
	task := &corev1alpha1.Task{
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			State:           corev1alpha1.TaskExecutionStateReserved,
			RuntimePoolName: pool.Name,
			RuntimePoolUID:  string(pool.UID),
		}},
	}
	bound := &verifiedAgentExecution{
		binding: &corev1alpha1.AgentExecutionBinding{},
		body:    agentExecutionSnapshotBody{ProfileDigest: string(frozenPlan.Digest)},
		plan:    frozenPlan,
	}

	target := acpDispatchTarget{pool: pool}
	if err := validateFrozenACPDispatchTarget(task, target, bound, currentPlan); err == nil {
		t.Fatal("current-image plan unexpectedly matched the selected old-image pool")
	}
	if err := validateFrozenACPDispatchPlan(task, target, bound, currentPlan); err != nil {
		t.Fatalf("validate reserved delivery plan: %v", err)
	}
}

func TestValidateFrozenACPDispatchTargetAcceptsCompatiblePreSubmissionRebind(t *testing.T) {
	profile := harnessProfileForTest()
	profile.AdapterDigests = acp.BuiltInRuntimeAdapterDigests(profile.ProviderKind)
	digest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	oldImage := "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)
	identity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
		"profileDigest": string(digest), "runtimeImage": oldImage,
	})
	if err != nil {
		t.Fatal(err)
	}
	frozenPlan := ACPRuntimePlan{
		PoolName: acpRuntimePoolName(profile.ProviderKind, harnessv2.ProfileDigest(identity)),
		Image:    oldImage, Profile: profile, Digest: digest,
	}
	newImage := "docker.io/example/codex@sha256:" + strings.Repeat("b", 64)
	delivery, err := acpRuntimeDeliveryPlanForAttempt(
		frozenPlan, nil, &store.PromptAttempt{}, ACPRuntimeImages{Codex: newImage}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	deliveryPlan := delivery.plan
	if deliveryPlan.Image != newImage || deliveryPlan.PoolName == frozenPlan.PoolName {
		t.Fatalf("compatible delivery plan = %#v, want replacement image and pool", deliveryPlan)
	}
	pool := runtimePoolForImageRotationTest("default", types.UID("replacement-pool-uid"), deliveryPlan)
	task := &corev1alpha1.Task{
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			RuntimePoolName: pool.Name, RuntimePoolUID: string(pool.UID),
		}},
	}
	bound := &verifiedAgentExecution{
		binding: &corev1alpha1.AgentExecutionBinding{},
		body:    agentExecutionSnapshotBody{ProfileDigest: string(frozenPlan.Digest)},
		plan:    frozenPlan,
	}
	if err := validateFrozenACPDispatchTarget(task, acpDispatchTarget{pool: pool}, bound, deliveryPlan); err != nil {
		t.Fatalf("compatible pre-submission rebind was not dispatchable: %v", err)
	}
	if err := validateFrozenACPDispatchTarget(task, acpDispatchTarget{pool: pool}, bound, frozenPlan); err == nil {
		t.Fatal("replacement pool unexpectedly matched the stale frozen delivery plan")
	}
}

func TestEnsureACPRuntimePoolWithPolicyDoesNotRecreateRetainedPool(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &TaskReconciler{Client: kubeClient}
	plan := ACPRuntimePlan{PoolName: "acp-codex-retained"}
	if _, _, err := reconciler.ensureACPRuntimePoolWithPolicy(
		context.Background(), "default", plan, "", "", "", false, types.UID("missing-pool-uid"),
	); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("missing retained RuntimePool error = %v, want validation failure", err)
	}
	var pools corev1alpha1.RuntimePoolList
	if err := kubeClient.List(context.Background(), &pools); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 0 {
		t.Fatalf("missing retained RuntimePool was recreated: %#v", pools.Items)
	}
}

func TestHistoricalWorkspaceDeliveryDoesNotRecreateDeletedPool(t *testing.T) {
	profile := harnessProfileForTest()
	profile.AdapterDigests = acp.BuiltInRuntimeAdapterDigests(profile.ProviderKind)
	digest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	plan := ACPRuntimePlan{
		PoolName: "acp-ws-codex-0123456789abcdef",
		Image:    "docker.io/example/codex@sha256:" + strings.Repeat("a", 64),
		Profile:  profile,
		Digest:   digest,
		Workspace: &ACPRuntimeWorkspaceBinding{
			Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
			BindingDigest: "sha256:" + strings.Repeat("b", 64),
		},
	}
	pool := runtimePoolForImageRotationTest("default", types.UID("historical-workspace-pool-uid"), plan)
	pool.Spec.ExecutionWorkspace = &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
		Provider:      plan.Workspace.Provider,
		BindingDigest: plan.Workspace.BindingDigest,
	}
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool).Build()
	reconciler := &TaskReconciler{
		Client: kubeClient,
		ACPRuntimeImages: ACPRuntimeImages{
			Codex: "docker.io/example/codex@sha256:" + strings.Repeat("c", 64),
		},
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Namespace: pool.Namespace}}
	delivery, err := reconciler.acpRuntimeDeliveryPlanForTaskAttempt(context.Background(), task, plan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.allowPoolCreation || delivery.requiredRuntimePoolUID != pool.UID || !reflect.DeepEqual(delivery.plan, plan) {
		t.Fatalf("historical workspace delivery = %#v, want exact preexisting pool %q with creation forbidden", delivery, pool.UID)
	}
	if err := kubeClient.Delete(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reconciler.ensureACPRuntimePoolWithPolicy(
		context.Background(), pool.Namespace, delivery.plan, "", "", "",
		delivery.allowPoolCreation, delivery.requiredRuntimePoolUID,
	); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("deleted historical workspace RuntimePool error = %v, want validation failure", err)
	}
	var pools corev1alpha1.RuntimePoolList
	if err := kubeClient.List(context.Background(), &pools); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 0 {
		t.Fatalf("deleted historical workspace RuntimePool was recreated: %#v", pools.Items)
	}
}

//nolint:gocyclo // The regression recreates an older immutable snapshot and verifies its fail-closed queue path.
func TestQueueACPRuntimeTaskRejectsStatuslessFrozenDemandAfterAdapterRotation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "statusless-adapter-rotation",
			UID: types.UID("4b76dd89-c9c4-4a9e-9ba4-2990e7e9de07"), Generation: 1,
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "inspect the repository",
			AgentRuntime: &corev1alpha1.AgentRuntimeSpec{},
			Workspace: &corev1alpha1.WorkspaceConfig{
				Intent: corev1alpha1.WorkspaceIntentRead, GitRepo: "https://github.com/orka-agents/orka.git",
			},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "codex",
			UID: types.UID("f8cf2e3d-0870-4cc1-a740-40f15f62478b"), Generation: 1,
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
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).
		WithObjects(task).
		Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "controller-test")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	t.Cleanup(func() {
		cancelEpoch()
		if err := <-epochDone; err != nil {
			t.Errorf("controller epoch manager shutdown: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}
	reconciler := &TaskReconciler{
		Client: kubeClient, Scheme: scheme, DurableControlStore: controlStore,
		ControllerEpochManager: epochs, ACPRuntimeEnabled: true, ACPRuntimeNamespace: "orka-runtimes",
		ACPRuntimeImages: ACPRuntimeImages{Codex: oldImage},
	}
	current := configureAgentExecutionBindingTest(t, ctx, reconciler, task)
	candidate, err := reconciler.resolveAgentExecutionCandidate(ctx, current, agent)
	if err != nil {
		t.Fatal(err)
	}
	body, err := decodeAgentExecutionSnapshot(candidate.snapshotBody)
	if err != nil {
		t.Fatal(err)
	}
	body.RuntimeProfile.AdapterDigests = cloneMap(body.RuntimeProfile.AdapterDigests)
	body.RuntimeProfile.AdapterDigests["codex-acp"] = "sha256:" + strings.Repeat("9", 64)
	profileDigest, err := harnessv2.CanonicalProfileDigest(body.RuntimeProfile)
	if err != nil {
		t.Fatal(err)
	}
	poolIdentity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
		"profileDigest": string(profileDigest), "runtimeImage": body.RuntimeImage,
	})
	if err != nil {
		t.Fatal(err)
	}
	body.ProfileDigest = string(profileDigest)
	body.PoolName = acpRuntimePoolName(body.RuntimeType, harnessv2.ProfileDigest(poolIdentity))
	candidate.snapshotBody, err = canonicalAgentExecutionSnapshotBody(body)
	if err != nil {
		t.Fatal(err)
	}
	snapshotDigest := store.CanonicalAgentExecutionSnapshotDigest(candidate.snapshotBody)
	candidate.binding.Snapshot = corev1alpha1.AgentExecutionSnapshotRef{
		ID:            store.AgentExecutionSnapshotKey{TaskUID: string(task.UID), Digest: snapshotDigest}.ID(),
		Digest:        snapshotDigest,
		SchemaVersion: store.AgentExecutionSnapshotSchemaVersion,
	}
	candidate.binding.RuntimeProfileDigest = string(profileDigest)
	candidate.binding.BindingDigest, err = canonicalAgentExecutionBindingDigest(candidate.binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.persistAgentExecutionSnapshot(ctx, current, candidate); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.persistAgentExecutionBinding(ctx, current, candidate); err != nil {
		t.Fatal(err)
	}
	bound := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(task), bound); err != nil {
		t.Fatal(err)
	}
	if bound.Status.Execution != nil {
		t.Fatalf("test setup unexpectedly created execution status: %#v", bound.Status.Execution)
	}

	reconciler.ACPRuntimeImages = ACPRuntimeImages{Codex: newImage}
	if _, err := reconciler.queueACPRuntimeTask(ctx, bound, agent); err != nil {
		t.Fatalf("reject statusless frozen demand after adapter rotation: %v", err)
	}
	failed := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(task), failed); err != nil {
		t.Fatal(err)
	}
	if failed.Status.Phase != corev1alpha1.TaskPhaseFailed || failed.Status.Execution == nil ||
		failed.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
		failed.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile") ||
		!strings.Contains(failed.Status.Execution.Message, "do not match the frozen runtime profile") {
		t.Fatalf("statusless adapter-rotation failure = %#v, want terminal InvalidRuntimeProfile", failed.Status)
	}
	var pools corev1alpha1.RuntimePoolList
	if err := kubeClient.List(ctx, &pools); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 0 {
		t.Fatalf("statusless adapter-rotation failure recreated RuntimePools: %#v", pools.Items)
	}
}

//nolint:gocyclo // The table verifies safe reserved rebind and reservation lifecycle invariants together.
func TestQueueACPRuntimeTaskRebindsSafeReservedAttemptAfterRuntimeImageRotation(t *testing.T) {
	for _, tc := range []struct {
		name              string
		activeReservation bool
		wantRebound       bool
	}{
		{name: "recovered without active reservation", wantRebound: true},
		{name: "active old-pool reservation blocks rebind", activeReservation: true, wantRebound: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default", Name: "reserved-image-rotation",
					UID: types.UID("91d91425-e317-4f86-9c46-ee63ae111c42"), Generation: 2,
				},
				Spec: corev1alpha1.TaskSpec{
					Type: corev1alpha1.TaskTypeAgent, Prompt: "inspect the repository",
					AgentRuntime: &corev1alpha1.AgentRuntimeSpec{},
					Workspace: &corev1alpha1.WorkspaceConfig{
						Intent: corev1alpha1.WorkspaceIntentRead, GitRepo: "https://github.com/orka-agents/orka.git",
					},
				},
			}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default", Name: "codex",
					UID: types.UID("33220495-a4a7-4f3e-9e07-8f3901048de5"), Generation: 3,
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
				task.Namespace, types.UID("101a20ba-2f11-44b5-8983-a912dc97800f"), oldPlan,
			)
			newPoolFixture := runtimePoolForImageRotationTest(
				task.Namespace, types.UID("1d863930-2412-4c27-9f6c-bdf55ca10ac0"), newPlan,
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
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("close durable control database: %v", err)
				}
			})
			controlStore := sqlite.NewStore(db, "test")
			epochs := NewControllerEpochManager(controlStore, "controller-test")
			epochCtx, cancelEpoch := context.WithCancel(context.Background())
			epochDone := make(chan error, 1)
			go func() { epochDone <- epochs.Start(epochCtx) }()
			t.Cleanup(func() {
				cancelEpoch()
				if err := <-epochDone; err != nil {
					t.Errorf("controller epoch manager shutdown: %v", err)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fence, err := epochs.CurrentFence(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reconciler := &TaskReconciler{
				Client: kubeClient, Scheme: scheme, DurableControlStore: controlStore,
				ControllerEpochManager: epochs, ACPRuntimeEnabled: true, ACPRuntimeNamespace: "orka-runtimes",
				ACPRuntimeImages: ACPRuntimeImages{Codex: oldImage},
			}
			bound := bindACPQueueTaskForTest(t, ctx, reconciler, task, agent)
			if _, err := reconciler.queueACPRuntimeTask(ctx, bound, agent); err != nil {
				t.Fatalf("initial queue: %v", err)
			}

			reserved := &corev1alpha1.Task{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, reserved); err != nil {
				t.Fatal(err)
			}
			attemptID, err := promptAttemptIDFromTask(reserved)
			if err != nil {
				t.Fatal(err)
			}
			attempt, err := controlStore.GetPromptAttempt(ctx, attemptID)
			if err != nil {
				t.Fatal(err)
			}
			attempt = transitionPromptAttemptForImageRotationTest(
				t, ctx, controlStore, fence, attempt, store.PromptExecutionReserved, "recover-reserved-before-image-rotation",
			)
			base := reserved.DeepCopy()
			reserved.Status.Execution.State = corev1alpha1.TaskExecutionStateReserved
			reserved.Status.Execution.ControllerEpoch = fence.Epoch
			reserved.Status.Execution.Reason = acpControllerRestartRecoveredReason
			reserved.Status.Execution.Message = acpControllerRestartRecoveredMessage
			reserved.Status.Execution.LastTransitionTime = nowMeta()
			if err := kubeClient.Status().Patch(ctx, reserved, client.MergeFrom(base)); err != nil {
				t.Fatal(err)
			}
			if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, reserved); err != nil {
				t.Fatal(err)
			}
			beforeExecution := reserved.Status.Execution.DeepCopy()
			beforeAttempt := *attempt

			oldPool := &corev1alpha1.RuntimePool{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: oldPlan.PoolName}, oldPool); err != nil {
				t.Fatal(err)
			}
			oldPool.Status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			oldPool.Status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			if tc.activeReservation {
				oldPool.Status.Capacity.Reservations = []corev1alpha1.RuntimePoolCapacityReservationStatus{{
					PoolUID: string(oldPool.UID), TaskUID: string(task.UID), Attempt: beforeExecution.Attempt,
					ControllerEpoch: fence.Epoch, RuntimeInstanceID: "old-pool-instance",
					ResidentSlots: 1, PromptSlots: 1, ReservedAt: metav1.Now(),
					ExpiresAt: metav1.NewTime(time.Now().UTC().Add(time.Minute)),
				}}
				updateRuntimePoolReservationCounters(&oldPool.Status.Capacity)
			}
			if err := kubeClient.Status().Update(ctx, oldPool); err != nil {
				t.Fatal(err)
			}

			reconciler.ACPRuntimeImages = ACPRuntimeImages{Codex: newImage}
			if _, err := reconciler.queueACPRuntimeTask(ctx, reserved.DeepCopy(), agent); err != nil {
				t.Fatalf("queue after image rotation: %v", err)
			}
			current := &corev1alpha1.Task{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, current); err != nil {
				t.Fatal(err)
			}
			if current.Status.Execution == nil || current.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved {
				t.Fatalf("reserved execution status = %#v", current.Status.Execution)
			}
			if tc.wantRebound {
				if current.Status.Execution.RuntimePoolName != newPlan.PoolName || current.Status.Execution.RuntimePoolUID != string(newPoolFixture.UID) {
					t.Fatalf("reserved attempt was not rebound to replacement pool: %#v", current.Status.Execution)
				}
				wantExecution := beforeExecution.DeepCopy()
				wantExecution.RuntimePoolName = newPlan.PoolName
				wantExecution.RuntimePoolUID = string(newPoolFixture.UID)
				wantExecution.Reason = ""
				wantExecution.Message = ""
				wantExecution.LastTransitionTime = current.Status.Execution.LastTransitionTime
				if !reflect.DeepEqual(current.Status.Execution, wantExecution) {
					t.Fatalf("reserved rebind changed frozen attempt fields:\n got: %#v\nwant: %#v", current.Status.Execution, wantExecution)
				}
				if current.Labels[acpRuntimeTaskPoolLabel] != newPlan.PoolName {
					t.Fatalf("pool label = %q, want %q", current.Labels[acpRuntimeTaskPoolLabel], newPlan.PoolName)
				}
			} else if current.Status.Execution.RuntimePoolName != beforeExecution.RuntimePoolName ||
				current.Status.Execution.RuntimePoolUID != beforeExecution.RuntimePoolUID {
				t.Fatalf("active old-pool reservation was rebound: before=%#v after=%#v", beforeExecution, current.Status.Execution)
			}
			persisted, err := controlStore.GetPromptAttempt(ctx, attemptID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantRebound {
				wantOperationID := store.CanonicalControlID(
					"rebind-reserved-runtime-pool", beforeAttempt.ID, beforeExecution.RuntimePoolUID, string(newPoolFixture.UID),
				)
				if persisted.ExecutionState != store.PromptExecutionReserved || persisted.Version != beforeAttempt.Version+1 ||
					persisted.LastOperationID != wantOperationID || persisted.RequestDigest != beforeAttempt.RequestDigest ||
					persisted.RuntimeInstanceID != "" || persisted.SessionUID != "" || persisted.SessionLeaseGeneration != 0 {
					t.Fatalf("reserved rebind did not durably fence the old dispatcher: before=%#v after=%#v", beforeAttempt, persisted)
				}
			} else if !reflect.DeepEqual(persisted, &beforeAttempt) {
				t.Fatalf("blocked reserved rebind mutated durable attempt: before=%#v after=%#v", beforeAttempt, persisted)
			}
		})
	}
}

func TestRebindPreSubmissionACPRuntimeTaskRejectsPostSubmissionStates(t *testing.T) {
	pool := &corev1alpha1.RuntimePool{ObjectMeta: metav1.ObjectMeta{Name: "replacement", Namespace: "default", UID: "replacement-uid"}}
	for _, state := range []corev1alpha1.TaskExecutionState{
		corev1alpha1.TaskExecutionStateSubmitting,
		corev1alpha1.TaskExecutionStateAccepted,
		corev1alpha1.TaskExecutionStateRunning,
	} {
		t.Run(string(state), func(t *testing.T) {
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "post-submission", UID: "task-uid"},
				Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
					State: state, Attempt: 1, PromptID: "prompt-1",
					RequestDigest:   "sha256:" + strings.Repeat("a", 64),
					RuntimePoolName: "old", RuntimePoolUID: "old-uid",
				}},
			}
			rebound, err := (&TaskReconciler{}).rebindQueuedACPRuntimeTask(
				context.Background(), task, nil, pool,
			)
			if err != nil {
				t.Fatal(err)
			}
			if rebound {
				t.Fatalf("post-submission state %s was rebound", state)
			}
		})
	}
}
