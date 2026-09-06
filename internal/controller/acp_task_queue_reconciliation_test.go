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
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

//nolint:gocyclo // The table verifies immutable snapshot behavior in queued and reserved states.
func TestQueueACPRuntimeTaskIgnoresLiveAgentMutationAfterBinding(t *testing.T) {
	for _, state := range []struct {
		name       string
		taskState  corev1alpha1.TaskExecutionState
		storeState store.PromptExecutionState
	}{
		{name: "queued", taskState: corev1alpha1.TaskExecutionStateQueued, storeState: store.PromptExecutionQueued},
		{name: "reserved", taskState: corev1alpha1.TaskExecutionStateReserved, storeState: store.PromptExecutionReserved},
	} {
		t.Run(state.name, func(t *testing.T) {
			fixture := newACPQueuePlanningFailureFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			queued, attempt := fixture.queueValidTask(t, ctx)
			if state.storeState == store.PromptExecutionReserved {
				fence, err := fixture.epochs.CurrentFence(ctx)
				if err != nil {
					t.Fatal(err)
				}
				attempt = transitionPromptAttemptForImageRotationTest(
					t, ctx, fixture.controlStore, fence, attempt, store.PromptExecutionReserved, "reserve-before-configuration-drift",
				)
				base := queued.DeepCopy()
				queued.Status.Execution.State = state.taskState
				queued.Status.Execution.ControllerEpoch = fence.Epoch
				queued.Status.Execution.Reason = acpControllerRestartRecoveredReason
				queued.Status.Execution.Message = acpControllerRestartRecoveredMessage
				if err := fixture.kubeClient.Status().Patch(ctx, queued, client.MergeFrom(base)); err != nil {
					t.Fatal(err)
				}
				if err := fixture.kubeClient.Get(ctx, types.NamespacedName{Namespace: queued.Namespace, Name: queued.Name}, queued); err != nil {
					t.Fatal(err)
				}
			}

			beforeExecution := queued.Status.Execution.DeepCopy()
			beforeAttempt := *attempt
			invalidAgent := fixture.agent.DeepCopy()
			invalidAgent.Spec.Skills = []corev1alpha1.SkillReference{{Name: "added-after-queue"}}
			if _, err := fixture.reconciler.queueACPRuntimeTask(ctx, queued.DeepCopy(), invalidAgent); err != nil {
				t.Fatalf("queue after live Agent mutation: %v", err)
			}

			current := &corev1alpha1.Task{}
			if err := fixture.kubeClient.Get(ctx, types.NamespacedName{Namespace: queued.Namespace, Name: queued.Name}, current); err != nil {
				t.Fatal(err)
			}
			if current.Status.Phase != queued.Status.Phase || !reflect.DeepEqual(current.Status.Execution, beforeExecution) ||
				current.Status.Attempts != queued.Status.Attempts {
				t.Fatalf("live Agent mutation changed frozen execution: before=%#v after=%#v", queued.Status, current.Status)
			}

			persistedAttempt, err := fixture.controlStore.GetPromptAttempt(ctx, attempt.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(persistedAttempt, &beforeAttempt) {
				t.Fatalf("live Agent mutation changed durable attempt: before=%#v after=%#v", beforeAttempt, persistedAttempt)
			}
		})
	}
}

var errPlanningFailureTransition = errors.New("injected planning failure transition error")

type failingPlanningFailureTransitionStore struct {
	store.DurableControlStore
}

func (s *failingPlanningFailureTransitionStore) TransitionPromptAttemptExecution(
	ctx context.Context,
	transition store.PromptAttemptExecutionTransition,
) (*store.PromptAttempt, error) {
	if transition.NewState == store.PromptExecutionFailed && transition.TerminalReason == "InvalidRuntimeProfile" {
		return nil, errPlanningFailureTransition
	}
	return s.DurableControlStore.TransitionPromptAttemptExecution(ctx, transition)
}

func TestQueueACPRuntimeTaskDoesNotSettleAttemptFromLiveAgentDrift(t *testing.T) {
	fixture := newACPQueuePlanningFailureFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queued, attempt := fixture.queueValidTask(t, ctx)
	fixture.reconciler.DurableControlStore = &failingPlanningFailureTransitionStore{DurableControlStore: fixture.controlStore}

	invalidAgent := fixture.agent.DeepCopy()
	invalidAgent.Spec.Skills = []corev1alpha1.SkillReference{{Name: "added-after-queue"}}
	_, err := fixture.reconciler.queueACPRuntimeTask(ctx, queued.DeepCopy(), invalidAgent)
	if err != nil {
		t.Fatalf("queue after live Agent drift: %v", err)
	}

	current := &corev1alpha1.Task{}
	if err := fixture.kubeClient.Get(ctx, types.NamespacedName{Namespace: queued.Namespace, Name: queued.Name}, current); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current.Status.Execution, queued.Status.Execution) || current.Status.Phase != queued.Status.Phase {
		t.Fatalf("Task was projected terminal before durable settlement: before=%#v after=%#v", queued.Status, current.Status)
	}
	persisted, err := fixture.controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.ExecutionState != store.PromptExecutionQueued || persisted.Version != attempt.Version {
		t.Fatalf("durable attempt changed from live Agent drift: before=%#v after=%#v", attempt, persisted)
	}
}

type acpQueuePlanningFailureFixture struct {
	task         *corev1alpha1.Task
	agent        *corev1alpha1.Agent
	kubeClient   client.Client
	controlStore store.DurableControlStore
	epochs       *ControllerEpochManager
	reconciler   *TaskReconciler
}

func newACPQueuePlanningFailureFixture(t *testing.T) *acpQueuePlanningFailureFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "post-queue-configuration-failure",
			UID: types.UID("c72243b6-8b2f-4466-a321-644bff8b21c1"), Generation: 1,
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
			UID: types.UID("4fba4a25-3c83-465c-9105-ded0b9c8029a"), Generation: 1,
		},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeCodex,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	runtimeImage := "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)
	plan, err := PlanACPRuntime(task, agent, ACPRuntimeImages{Codex: runtimeImage})
	if err != nil {
		t.Fatal(err)
	}
	pool := runtimePoolForImageRotationTest(
		task.Namespace, types.UID("73ab20fa-2e2b-4f46-a410-ae4ed9c57751"), plan,
	)
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).
		WithObjects(task, pool).
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
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}
	reconciler := &TaskReconciler{
		Client: kubeClient, Scheme: scheme, DurableControlStore: controlStore,
		ControllerEpochManager: epochs, ACPRuntimeEnabled: true, ACPRuntimeNamespace: "orka-runtimes",
		ACPRuntimeImages: ACPRuntimeImages{Codex: runtimeImage},
	}
	return &acpQueuePlanningFailureFixture{
		task: task, agent: agent, kubeClient: kubeClient, controlStore: controlStore, epochs: epochs, reconciler: reconciler,
	}
}

func (f *acpQueuePlanningFailureFixture) queueValidTask(t *testing.T, ctx context.Context) (*corev1alpha1.Task, *store.PromptAttempt) {
	t.Helper()
	bound := bindACPQueueTaskForTest(t, ctx, f.reconciler, f.task, f.agent)
	if _, err := f.reconciler.queueACPRuntimeTask(ctx, bound, f.agent.DeepCopy()); err != nil {
		t.Fatalf("initial queue: %v", err)
	}
	queued := &corev1alpha1.Task{}
	if err := f.reconciler.Get(ctx, types.NamespacedName{Namespace: f.task.Namespace, Name: f.task.Name}, queued); err != nil {
		t.Fatal(err)
	}
	attemptID, err := promptAttemptIDFromTask(queued)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := f.controlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	return queued, attempt
}

func TestQueueACPRuntimeTaskFailsClosedWhenApprovedImageIsRemoved(t *testing.T) {
	fixture := newACPQueuePlanningFailureFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queued, attempt := fixture.queueValidTask(t, ctx)

	var pools corev1alpha1.RuntimePoolList
	if err := fixture.kubeClient.List(ctx, &pools); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 1 {
		t.Fatalf("RuntimePools before image removal = %d, want 1", len(pools.Items))
	}
	if err := fixture.kubeClient.Delete(ctx, &pools.Items[0]); err != nil {
		t.Fatal(err)
	}
	fixture.reconciler.ACPRuntimeImages.Codex = ""

	if _, err := fixture.reconciler.queueACPRuntimeTask(ctx, queued.DeepCopy(), fixture.agent.DeepCopy()); err != nil {
		t.Fatalf("queue after approved image removal: %v", err)
	}

	current := &corev1alpha1.Task{}
	if err := fixture.kubeClient.Get(ctx, client.ObjectKeyFromObject(queued), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != corev1alpha1.TaskPhaseFailed || current.Status.Execution == nil ||
		current.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
		current.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile") ||
		!strings.Contains(current.Status.Execution.Message, "configured digest-pinned image") {
		t.Fatalf("Task status after approved image removal = %#v, want terminal InvalidRuntimeProfile", current.Status)
	}
	persisted, err := fixture.controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.ExecutionState != store.PromptExecutionFailed || persisted.TerminalReason != "InvalidRuntimeProfile" ||
		!strings.Contains(persisted.OutcomeMarker, "configured digest-pinned image") {
		t.Fatalf("PromptAttempt after approved image removal = %#v, want terminal InvalidRuntimeProfile", persisted)
	}
	pools = corev1alpha1.RuntimePoolList{}
	if err := fixture.kubeClient.List(ctx, &pools); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 0 {
		t.Fatalf("removed-image RuntimePool was recreated: %#v", pools.Items)
	}
}

func TestFailACPPlanningTaskIsIdempotentBeforeDurableAttempt(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "pre-attempt-planning-failure",
			UID: types.UID("da90c496-1f82-44a4-a610-8b46c9a90f55"),
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}).
		WithObjects(task).
		Build()
	reconciler := &TaskReconciler{Client: kubeClient, Scheme: scheme}
	reason := corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile")
	message := "agent configuration is unsupported"

	for attempt := 1; attempt <= 2; attempt++ {
		current := &corev1alpha1.Task{}
		if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(task), current); err != nil {
			t.Fatal(err)
		}
		if _, err := reconciler.failACPPlanningTask(context.Background(), current, reason, message); err != nil {
			t.Fatalf("failACPPlanningTask() call %d error = %v", attempt, err)
		}
	}

	current := &corev1alpha1.Task{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != corev1alpha1.TaskPhaseFailed || current.Status.Execution == nil ||
		current.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
		current.Status.Execution.Reason != reason || current.Status.Execution.Attempt != 0 ||
		current.Status.Execution.PromptID != "" || current.Status.Execution.RequestDigest != "" {
		t.Fatalf("idempotent planning failure status = %#v", current.Status)
	}
}

var errPlanningFailureProjectionEnqueue = errors.New("injected planning failure projection enqueue error")

type failingPlanningFailureProjectionStore struct {
	store.DurableControlStore
	failed bool
}

func (s *failingPlanningFailureProjectionStore) EnqueueOutboxProjection(
	ctx context.Context,
	projection *store.OutboxProjection,
	fence store.ControllerEpochFence,
) (*store.OutboxProjection, error) {
	if !s.failed {
		s.failed = true
		return nil, errPlanningFailureProjectionEnqueue
	}
	return s.DurableControlStore.EnqueueOutboxProjection(ctx, projection, fence)
}

func TestQueueACPRuntimeTaskDoesNotEnqueuePlanningFailureFromLiveAgentDrift(t *testing.T) {
	fixture := newACPQueuePlanningFailureFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queued, attempt := fixture.queueValidTask(t, ctx)
	failingStore := &failingPlanningFailureProjectionStore{DurableControlStore: fixture.controlStore}
	fixture.reconciler.DurableControlStore = failingStore

	invalidAgent := fixture.agent.DeepCopy()
	invalidAgent.Spec.Skills = []corev1alpha1.SkillReference{{Name: "added-after-queue"}}
	_, err := fixture.reconciler.queueACPRuntimeTask(ctx, queued.DeepCopy(), invalidAgent)
	if err != nil {
		t.Fatalf("queue after live Agent drift: %v", err)
	}
	if failingStore.failed {
		t.Fatal("live Agent drift reached terminal outbox projection")
	}
	persisted, err := fixture.controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, attempt) {
		t.Fatalf("live Agent drift changed durable attempt: before=%#v after=%#v", attempt, persisted)
	}
	current := &corev1alpha1.Task{}
	if err := fixture.kubeClient.Get(ctx, client.ObjectKeyFromObject(queued), current); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current.Status.Execution, queued.Status.Execution) || current.Status.Phase != queued.Status.Phase {
		t.Fatalf("live Agent drift changed Task projection: before=%#v after=%#v", queued.Status, current.Status)
	}
}
