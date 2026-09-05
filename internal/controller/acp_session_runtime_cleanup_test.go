package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	storekube "github.com/orka-agents/orka/internal/store/kube"
	"github.com/orka-agents/orka/internal/taskterminal"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSessionRuntimeCleanupRetiresContinuedSessionInEitherDeletionOrder(t *testing.T) {
	for _, deleting := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("%d Tasks deleted before Session", deleting), func(t *testing.T) {
			fixture, tasks := newContinuedSessionCleanupFixture(t)
			cleanup := sessionRuntimeCleanupStore(t, fixture, fixture.dispatcher.CleanupSessionRuntime)
			fixture.reconciler.DurableControlStore = cleanup
			for i := range tasks {
				if i < deleting {
					tasks[i] = markSessionCleanupTaskDeleting(t, fixture, tasks[i])
					ready, err := fixture.reconciler.acpTaskDeletionReady(fixture.ctx, tasks[i])
					if err != nil || ready {
						t.Fatalf("Task deletion before Session cleanup = ready:%v err:%v, want retained authority", ready, err)
					}
				}
				if ready, err := fixture.dispatcher.cleanupRecoveredTaskScopedRuntimeSession(fixture.ctx, tasks[i]); err != nil || !ready {
					t.Fatalf("ordinary Task recovery = ready:%v err:%v", ready, err)
				}
			}
			if fixture.deleteCalls.Load() != 0 {
				t.Fatal("ordinary Task recovery destroyed resident conversation history")
			}
			manager := NewSessionManager(fixture.persistence)
			manager.SetACPSessionCleanup(cleanup, fixture.epochs)
			if err := manager.DeleteSession(fixture.ctx, defaultNS, "cleanup-conversation"); err != nil {
				t.Fatalf("DeleteSession(): %v", err)
			}
			if fixture.deleteCalls.Load() != 1 {
				t.Fatalf("RuntimeSession DELETE calls = %d, want one shared-session deletion", fixture.deleteCalls.Load())
			}
			select {
			case request := <-fixture.deleteRequests:
				if request.Metadata.TaskUID != harnessv2.TaskUID(tasks[1].UID) ||
					request.Metadata.Fence.RuntimeSessionUID != harnessv2.RuntimeSessionUID(tasks[1].Status.Execution.RuntimeSessionUID) ||
					request.Metadata.Fence.RuntimeSessionGeneration != uint64(tasks[1].Status.Execution.RuntimeSessionGeneration) {
					t.Fatalf("Session deletion did not use the newest exact Task/session fence: %#v", request.Metadata)
				}
			default:
				t.Fatal("RuntimeSession deletion was not captured")
			}
			assertSessionRuntimeCleanupCompleted(t, fixture, cleanup, tasks)
			if err := manager.DeleteSession(fixture.ctx, defaultNS, "cleanup-conversation"); err != nil || fixture.deleteCalls.Load() != 1 {
				t.Fatalf("idempotent Session deletion = %v; DELETE calls = %d", err, fixture.deleteCalls.Load())
			}
			for _, task := range tasks {
				current := &corev1alpha1.Task{}
				if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
					t.Fatal(err)
				}
				if current.DeletionTimestamp.IsZero() {
					current = markSessionCleanupTaskDeleting(t, fixture, current)
				}
				if _, err := fixture.reconciler.handleDeletion(fixture.ctx, current); err != nil {
					t.Fatalf("finish Task deletion after Session cleanup: %v", err)
				}
				if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), &corev1alpha1.Task{}); !apierrors.IsNotFound(err) {
					t.Fatalf("Task deletion did not release its finalizer after Session cleanup: %v", err)
				}
			}
		})
	}
}

func TestSessionRuntimeCleanupRecoversAfterDeletionBeforeReceipt(t *testing.T) {
	fixture, tasks := newContinuedSessionCleanupFixture(t)
	failingReader := &failSessionCleanupReceiptRead{Reader: fixture.client, key: client.ObjectKeyFromObject(tasks[1])}
	fixture.dispatcher.APIReader = failingReader
	cleanup := sessionRuntimeCleanupStore(t, fixture, fixture.dispatcher.CleanupSessionRuntime)
	manager := NewSessionManager(fixture.persistence)
	manager.SetACPSessionCleanup(cleanup, fixture.epochs)
	if err := manager.DeleteSession(fixture.ctx, defaultNS, "cleanup-conversation"); err == nil || !strings.Contains(err.Error(), "injected receipt read failure") {
		t.Fatalf("interrupted DeleteSession() = %v", err)
	}
	if fixture.deleteCalls.Load() != 1 {
		t.Fatalf("interrupted cleanup DELETE calls = %d, want 1", fixture.deleteCalls.Load())
	}
	if _, err := fixture.persistence.GetSession(fixture.ctx, defaultNS, "cleanup-conversation"); err != nil {
		t.Fatalf("interrupted cleanup removed the transcript: %v", err)
	}
	if _, err := cleanup.GetSessionControl(fixture.ctx, defaultNS, "cleanup-conversation"); err != nil {
		t.Fatalf("interrupted cleanup removed authoritative control: %v", err)
	}
	if _, err := fixture.persistence.GetSessionCleanupIntent(fixture.ctx, defaultNS, "cleanup-conversation"); err != nil {
		t.Fatalf("interrupted cleanup lost its durable intent: %v", err)
	}
	// A fresh dispatcher has no in-memory session state. Recovery must prove
	// authenticated absence on the exact frozen runtime and finish the intent.
	restarted := &ACPDispatcher{
		Client: fixture.client, APIReader: fixture.client, Store: fixture.controlStore,
		ResultStore: fixture.persistence, Snapshots: fixture.persistence, Epochs: fixture.epochs,
	}
	resumed := sessionRuntimeCleanupStore(t, fixture, restarted.CleanupSessionRuntime)
	fence, err := fixture.epochs.CurrentFence(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.ResumeSessionCleanups(fixture.ctx, fence); err != nil {
		t.Fatalf("resume interrupted Session runtime cleanup: %v", err)
	}
	if fixture.deleteCalls.Load() != 1 {
		t.Fatalf("recovery repeated a completed RuntimeSession deletion %d times", fixture.deleteCalls.Load())
	}
	fixture.reconciler.DurableControlStore = resumed
	assertSessionRuntimeCleanupCompleted(t, fixture, resumed, tasks)
}

func TestSessionRuntimeCleanupRetainsTaskAuthorityUntilDeletionCompletes(t *testing.T) {
	fixture, tasks := newContinuedSessionCleanupFixture(t)
	cleanup := sessionRuntimeCleanupStore(t, fixture, func(ctx context.Context, intent store.SessionCleanupIntent, fence store.ControllerEpochFence) error {
		if err := fixture.dispatcher.CleanupSessionRuntime(ctx, intent, fence); err != nil {
			return err
		}
		return errors.New("injected interruption after runtime cleanup receipts")
	})
	fixture.reconciler.DurableControlStore = cleanup
	manager := NewSessionManager(fixture.persistence)
	manager.SetACPSessionCleanup(cleanup, fixture.epochs)
	if err := manager.DeleteSession(fixture.ctx, defaultNS, "cleanup-conversation"); err == nil ||
		!strings.Contains(err.Error(), "injected interruption after runtime cleanup receipts") {
		t.Fatalf("interrupted DeleteSession() = %v", err)
	}
	for _, task := range tasks {
		current := markSessionCleanupTaskDeleting(t, fixture, task)
		if !runtimeSessionCleanupCompleteForUID(current, current.UID) {
			t.Fatalf("Task %s lacks runtime retirement proof before the simulated crash", current.Name)
		}
		if ready, err := fixture.reconciler.acpTaskDeletionReady(fixture.ctx, current); err != nil || ready {
			t.Fatalf("Task %s released frozen authority before Session deletion completed: ready:%v err:%v", current.Name, ready, err)
		}
	}
	restarted := &ACPDispatcher{
		Client: fixture.client, APIReader: fixture.client, Store: fixture.controlStore,
		ResultStore: fixture.persistence, Snapshots: fixture.persistence, Epochs: fixture.epochs,
	}
	resumed := sessionRuntimeCleanupStore(t, fixture, restarted.CleanupSessionRuntime)
	fence, err := fixture.epochs.CurrentFence(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.ResumeSessionCleanups(fixture.ctx, fence); err != nil {
		t.Fatalf("resume after runtime cleanup receipts: %v", err)
	}
	if fixture.deleteCalls.Load() != 1 {
		t.Fatalf("recovery repeated runtime deletion %d times", fixture.deleteCalls.Load())
	}
	fixture.reconciler.DurableControlStore = resumed
	assertSessionRuntimeCleanupCompleted(t, fixture, resumed, tasks)
}

func TestSessionRuntimeCleanupRejectsMalformedProjection(t *testing.T) {
	fixture, tasks := newContinuedSessionCleanupFixture(t)
	turn, projection := sessionRuntimeCleanupTurnProjection(t, fixture, tasks[0])
	intent := store.SessionCleanupIntent{Namespace: defaultNS, SessionName: "cleanup-conversation", SessionUID: turn.Key.SessionUID}
	for _, test := range []struct {
		name   string
		mutate func(map[string]json.RawMessage)
	}{
		{name: "missing execution", mutate: func(p map[string]json.RawMessage) { delete(p, "execution") }},
		{name: "null execution", mutate: func(p map[string]json.RawMessage) { p["execution"] = json.RawMessage(`null`) }},
		{name: "missing delivery", mutate: func(p map[string]json.RawMessage) { delete(p, "delivery") }},
		{name: "null delivery", mutate: func(p map[string]json.RawMessage) { p["delivery"] = json.RawMessage(`null`) }},
		{name: "unknown field", mutate: func(p map[string]json.RawMessage) { p["unexpected"] = json.RawMessage(`true`) }},
		{name: "v1 mixed with v2 ownership", mutate: func(p map[string]json.RawMessage) {
			p["harnessRuntime"] = json.RawMessage(`{"attempt":1,"state":"Succeeded","outcome":"Succeeded"}`)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var payload map[string]json.RawMessage
			if err := json.Unmarshal(projection.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			test.mutate(payload)
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			changedTurn, changedProjection := *turn, *projection
			changedProjection.Payload = body
			changedProjection.PayloadDigest = store.CanonicalBytesDigest(body)
			changedTurn.ProjectionDigest = changedProjection.PayloadDigest
			dispatcher := &ACPDispatcher{
				APIReader: fixture.client, Snapshots: fixture.persistence,
				Store: &sessionRuntimeCleanupProjectionStore{DurableControlStore: fixture.controlStore, projection: &changedProjection},
			}
			if target, err := dispatcher.sessionRuntimeCleanupTarget(fixture.ctx, intent, &changedTurn); err == nil || target != nil {
				t.Fatalf("malformed projection was accepted: target:%v err:%v", target, err)
			}
		})
	}
}

func TestSessionRuntimeCleanupAcceptsTerminalV1ProjectionWithoutTask(t *testing.T) {
	fixture, tasks := newContinuedSessionCleanupFixture(t)
	turn, projection := sessionRuntimeCleanupTurnProjection(t, fixture, tasks[0])
	body, err := json.Marshal(taskterminal.Projection{
		Namespace: defaultNS, Task: "previously-reclaimed-v1-task", TaskUID: turn.Key.TaskUID, Attempt: int32(turn.Key.Attempt),
		Phase: corev1alpha1.TaskPhaseSucceeded,
		HarnessRuntime: &corev1alpha1.HarnessRuntimeStatus{
			Attempt: int32(turn.Key.Attempt), State: corev1alpha1.TaskExecutionStateSucceeded,
			Outcome: corev1alpha1.TaskExecutionOutcomeSucceeded,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	projection.Payload, projection.PayloadDigest = body, store.CanonicalBytesDigest(body)
	turn.ProjectionDigest = projection.PayloadDigest
	dispatcher := &ACPDispatcher{
		Store: &sessionRuntimeCleanupProjectionStore{DurableControlStore: fixture.controlStore, projection: projection},
	}
	intent := store.SessionCleanupIntent{Namespace: defaultNS, SessionName: "cleanup-conversation", SessionUID: turn.Key.SessionUID}
	if target, err := dispatcher.sessionRuntimeCleanupTarget(fixture.ctx, intent, turn); err != nil || target != nil {
		t.Fatalf("terminal harness v1 projection requires resident runtime cleanup: target:%v err:%v", target, err)
	}
}

func TestSessionRuntimeCleanupChecksWriteTaskRetirementProof(t *testing.T) {
	fixture := newExternalACPDispatchFixtureWithOptions(t, "write-cleanup", testAgentRuntimeMCPPolicy(), externalACPDispatchFixtureOptions{
		profileTransform:                func(profile *harnessv2.RuntimeProfile) { profile.WorkspaceIntent = harnessv2.WorkspaceIntentWrite },
		supportsPublicationFinalization: true,
	})
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: defaultNS, Name: "write-cleanup", UID: "write-cleanup-uid", Generation: 1, Finalizers: []string{labels.TaskFinalizer}},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: fixture.agent.Name}, Prompt: "make no changes",
			AgentRuntime: &corev1alpha1.AgentRuntimeSpec{AllowedTools: append([]string{}, fixture.mcpPolicy.AllowedTools...)},
			SessionRef:   &corev1alpha1.SessionReference{Name: "write-conversation"},
			Workspace:    &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentWrite},
		},
	}
	candidate, err := fixture.reconciler.resolveExternalAgentExecutionCandidate(fixture.ctx, task, fixture.agent)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.reconciler.persistAgentExecutionSnapshot(fixture.ctx, task, candidate); err != nil {
		t.Fatal(err)
	}
	task.Status = corev1alpha1.TaskStatus{
		Phase: corev1alpha1.TaskPhaseSucceeded, AgentExecutionBinding: candidate.binding.DeepCopy(),
		Execution: &corev1alpha1.TaskExecutionStatus{
			Attempt: 1, PromptID: "write-cleanup-prompt", State: corev1alpha1.TaskExecutionStateSucceeded, Outcome: corev1alpha1.TaskExecutionOutcomeSucceeded,
			AgentRuntimeName: fixture.runtime.Name, AgentRuntimeUID: string(fixture.runtime.UID),
			RuntimeInstanceID: fixture.runtime.Status.ObservedCapabilities.RuntimeInstanceID,
			RuntimeSessionUID: "write-conversation-uid", RuntimeSessionGeneration: 1,
			RuntimeSessionSupervisorBootID: fixture.runtime.Status.ObservedCapabilities.SupervisorBootID,
			RuntimeSessionProfileDigest:    candidate.binding.RuntimeProfileDigest,
			RequestDigest:                  store.CanonicalBytesDigest([]byte("write-cleanup-request")),
		},
		Delivery: &corev1alpha1.TaskDeliveryStatus{State: corev1alpha1.TaskDeliveryStateNoChange, Outcome: corev1alpha1.TaskDeliveryOutcomeNoChange},
	}
	if err := fixture.client.Create(fixture.ctx, task); err != nil {
		t.Fatal(err)
	}
	task = markSessionCleanupTaskDeleting(t, fixture, task)
	attempt := &store.PromptAttempt{
		Key:           store.PromptAttemptKey{Namespace: defaultNS, TaskUID: string(task.UID), Attempt: 1, PromptID: task.Status.Execution.PromptID},
		BindingDigest: candidate.binding.BindingDigest, SnapshotDigest: candidate.binding.Snapshot.Digest,
		RuntimeInstanceID: task.Status.Execution.RuntimeInstanceID, RequestDigest: task.Status.Execution.RequestDigest,
		SessionUID: task.Status.Execution.RuntimeSessionUID, SessionLeaseGeneration: 4,
		ExecutionState: store.PromptExecutionSucceeded, DeliveryState: store.PromptDeliveryNoChange,
	}
	attempt.ID, err = attempt.Key.CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	turn := &store.SessionTurn{
		Key:             store.SessionTurnKey{SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration, TaskUID: string(task.UID), Attempt: 1, PromptID: attempt.Key.PromptID},
		PromptAttemptID: attempt.ID, State: store.SessionTurnFinalized, FinalizedAt: &now,
		TerminalKind: store.SessionTurnAssistantResult, ProjectionKind: taskterminal.ProjectionKind,
	}
	turn.ID, err = turn.Key.CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	turn.ProjectionID = store.CanonicalControlID("outbox", turn.ID, turn.ProjectionKind)
	body, err := json.Marshal(taskterminal.Projection{
		Namespace: defaultNS, Task: task.Name, TaskUID: string(task.UID), Attempt: 1,
		Phase: task.Status.Phase, BindingDigest: candidate.binding.BindingDigest,
		Execution: *task.Status.Execution.DeepCopy(), Delivery: task.Status.Delivery.DeepCopy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	turn.ProjectionDigest = store.CanonicalBytesDigest(body)
	projection := &store.OutboxProjection{
		ID: turn.ProjectionID, AggregateKind: "SessionTurn", AggregateID: turn.ID,
		ProjectionKind: turn.ProjectionKind, Payload: body, PayloadDigest: turn.ProjectionDigest, State: store.OutboxProjectionDelivered,
	}
	dispatcher := &ACPDispatcher{
		APIReader: fixture.client, Snapshots: fixture.persistence,
		Store: &sessionRuntimeCleanupProjectionStore{DurableControlStore: fixture.controlStore, projection: projection, attempt: attempt},
	}
	intent := store.SessionCleanupIntent{Namespace: defaultNS, SessionName: task.Spec.SessionRef.Name, SessionUID: turn.Key.SessionUID}
	if target, err := dispatcher.sessionRuntimeCleanupTarget(fixture.ctx, intent, turn); !errors.Is(err, store.ErrNotReady) || target != nil {
		t.Fatalf("write Task without runtime retirement proof = target:%v err:%v", target, err)
	}
	task.Status.Execution.RuntimeSessionCleanupDigest, err = taskScopedRuntimeSessionCleanupDigest(task.UID, 1, attempt.RuntimeInstanceID, attempt.SessionUID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Status().Update(fixture.ctx, task); err != nil {
		t.Fatal(err)
	}
	if target, err := dispatcher.sessionRuntimeCleanupTarget(fixture.ctx, intent, turn); err != nil || target != nil {
		t.Fatalf("retired deleting write Task = target:%v err:%v", target, err)
	}
	if fixture.deleteCalls.Load() != 0 {
		t.Fatal("write Task cleanup repeated an already-proven runtime deletion")
	}
}

func TestSessionRuntimeCleanupKeepsDurableStateWhenAuthorityIsUnavailable(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *externalACPDispatchFixture, []*corev1alpha1.Task)
	}{
		{
			name: "missing older Task",
			mutate: func(t *testing.T, fixture *externalACPDispatchFixture, tasks []*corev1alpha1.Task) {
				current := &corev1alpha1.Task{}
				if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(tasks[0]), current); err != nil {
					t.Fatal(err)
				}
				current.Finalizers = nil
				if err := fixture.client.Update(fixture.ctx, current); err != nil {
					t.Fatal(err)
				}
				if err := fixture.client.Delete(fixture.ctx, current); err != nil {
					t.Fatal(err)
				}
				if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), &corev1alpha1.Task{}); !apierrors.IsNotFound(err) {
					t.Fatalf("missing-authority fixture still retains its Task: %v", err)
				}
			},
		},
		{
			name: "missing immutable snapshot",
			mutate: func(t *testing.T, fixture *externalACPDispatchFixture, tasks []*corev1alpha1.Task) {
				if err := fixture.persistence.DeleteAgentExecutionSnapshots(fixture.ctx, string(tasks[0].UID)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "supervisor boot drift",
			mutate: func(t *testing.T, fixture *externalACPDispatchFixture, _ []*corev1alpha1.Task) {
				runtime := fixture.runtime.DeepCopy()
				runtime.Status.ObservedCapabilities.SupervisorBootID = "another-boot"
				if err := fixture.client.Status().Update(fixture.ctx, runtime); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, tasks := newContinuedSessionCleanupFixture(t)
			test.mutate(t, fixture, tasks)
			cleanup := sessionRuntimeCleanupStore(t, fixture, fixture.dispatcher.CleanupSessionRuntime)
			manager := NewSessionManager(fixture.persistence)
			manager.SetACPSessionCleanup(cleanup, fixture.epochs)
			if err := manager.DeleteSession(fixture.ctx, defaultNS, "cleanup-conversation"); err == nil {
				t.Fatal("Session deletion accepted missing or changed runtime authority")
			}
			if fixture.deleteCalls.Load() != 0 {
				t.Fatal("Session deletion mutated a runtime without exact cleanup authority")
			}
			if _, err := fixture.persistence.GetSession(fixture.ctx, defaultNS, "cleanup-conversation"); err != nil {
				t.Fatalf("failed cleanup removed the transcript: %v", err)
			}
			if _, err := cleanup.GetSessionControl(fixture.ctx, defaultNS, "cleanup-conversation"); err != nil {
				t.Fatalf("failed cleanup removed authoritative control: %v", err)
			}
		})
	}
}

func newContinuedSessionCleanupFixture(t *testing.T) (*externalACPDispatchFixture, []*corev1alpha1.Task) {
	t.Helper()
	fixture := newExternalACPDispatchFixture(t)
	tasks := make([]*corev1alpha1.Task, 0, 2)
	for i := range 2 {
		name := fmt.Sprintf("cleanup-turn-%d", i+1)
		queued := fixture.queueTask(t, name, types.UID(name+"-uid"), name, &corev1alpha1.SessionReference{
			Name: "cleanup-conversation", Create: i == 0, Append: true,
		})
		completed := fixture.dispatch(t, queued)
		if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
			t.Fatalf("cleanup fixture turn did not succeed: %#v", completed.Status)
		}
		tasks = append(tasks, completed)
	}
	projector := &ACPOutboxProjector{Client: fixture.client, Store: fixture.controlStore, Epochs: fixture.epochs, WorkerID: "cleanup-test"}
	if err := projector.projectOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if fixture.createCalls.Load() != 1 || fixture.deleteCalls.Load() != 0 {
		t.Fatalf("continued Session fixture = create:%d delete:%d, want 1/0", fixture.createCalls.Load(), fixture.deleteCalls.Load())
	}
	return fixture, tasks
}

func sessionRuntimeCleanupTurnProjection(t *testing.T, fixture *externalACPDispatchFixture, task *corev1alpha1.Task) (*store.SessionTurn, *store.OutboxProjection) {
	t.Helper()
	attemptID, err := promptAttemptIDFromTask(task)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := fixture.controlStore.GetPromptAttempt(fixture.ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	turnID, err := (store.SessionTurnKey{
		SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration,
		TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
	}).CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	turn, err := fixture.controlStore.GetSessionTurn(fixture.ctx, turnID)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := fixture.controlStore.GetOutboxProjection(fixture.ctx, turn.ProjectionID)
	if err != nil {
		t.Fatal(err)
	}
	return turn, projection
}

type sessionRuntimeCleanupProjectionStore struct {
	store.DurableControlStore
	projection *store.OutboxProjection
	attempt    *store.PromptAttempt
}

func (s *sessionRuntimeCleanupProjectionStore) GetPromptAttempt(ctx context.Context, id string) (*store.PromptAttempt, error) {
	if s.attempt != nil && id == s.attempt.ID {
		return s.attempt, nil
	}
	return s.DurableControlStore.GetPromptAttempt(ctx, id)
}

func (s *sessionRuntimeCleanupProjectionStore) GetOutboxProjection(ctx context.Context, id string) (*store.OutboxProjection, error) {
	if id == s.projection.ID {
		return s.projection, nil
	}
	return s.DurableControlStore.GetOutboxProjection(ctx, id)
}

func sessionRuntimeCleanupStore(t *testing.T, fixture *externalACPDispatchFixture, cleanup store.SessionRuntimeCleanupFunc) *storekube.Store {
	t.Helper()
	result, err := storekube.NewComposite(fixture.client, defaultNS, fixture.persistence,
		storekube.WithAPIReader(fixture.client), storekube.WithSessionRuntimeCleanup(cleanup))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func markSessionCleanupTaskDeleting(t *testing.T, fixture *externalACPDispatchFixture, task *corev1alpha1.Task) *corev1alpha1.Task {
	t.Helper()
	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(current.Finalizers, labels.TaskFinalizer) {
		current.Finalizers = append(current.Finalizers, labels.TaskFinalizer)
	}
	if err := fixture.client.Update(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Delete(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	return current
}

func assertSessionRuntimeCleanupCompleted(t *testing.T, fixture *externalACPDispatchFixture, cleanup *storekube.Store, tasks []*corev1alpha1.Task) {
	t.Helper()
	if _, err := fixture.persistence.GetSession(fixture.ctx, defaultNS, "cleanup-conversation"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted Session transcript = %v, want not found", err)
	}
	if _, err := cleanup.GetSessionControl(fixture.ctx, defaultNS, "cleanup-conversation"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted Session control = %v, want not found", err)
	}
	for _, task := range tasks {
		current := &corev1alpha1.Task{}
		if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
			t.Fatal(err)
		}
		if !runtimeSessionCleanupCompleteForUID(current, current.UID) {
			t.Fatalf("Task %s lacks its exact shared-runtime cleanup receipt", current.Name)
		}
		ready, err := fixture.reconciler.acpTaskDeletionReady(fixture.ctx, current)
		if err != nil || !ready {
			t.Fatalf("Task %s deletion after Session cleanup = ready:%v err:%v", current.Name, ready, err)
		}
		attemptID, err := promptAttemptIDFromTask(current)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := fixture.persistence.GetSessionTurnCleanupReceipt(fixture.ctx, defaultNS, "cleanup-conversation", attemptID)
		if err != nil || receipt == nil || receipt.ProjectionState != store.OutboxProjectionDelivered {
			t.Fatalf("Task %s lost its archived finalized-turn receipt: %v", current.Name, err)
		}
	}
}

type failSessionCleanupReceiptRead struct {
	client.Reader
	key   client.ObjectKey
	reads atomic.Int32
}

func (r *failSessionCleanupReceiptRead) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*corev1alpha1.Task); ok && key == r.key && r.reads.Add(1) == 2 {
		return errors.New("injected receipt read failure")
	}
	return r.Reader.Get(ctx, key, object, options...)
}
