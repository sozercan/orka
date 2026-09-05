package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/taskterminal"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestDeletedSessionTaskFinalizerUsesReceiptAcrossAttemptRemoval(t *testing.T) {
	for _, attemptRemoved := range []bool{false, true} {
		name := "terminal attempt present"
		if attemptRemoved {
			name = "restart after prepared attempt removal"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			task, controlStore := deletedSessionTaskFixture(t)
			if attemptRemoved {
				controlStore.attempt = nil
			}
			reconciler := newUnitReconciler(newTestScheme(), task)
			reconciler.DurableControlStore = controlStore
			reconciler.ControllerEpochManager = readyPromptAttemptReclaimEpochManager()
			if _, err := reconciler.handleDeletion(ctx, task); err != nil {
				t.Fatalf("Task finalizer after Session deletion: %v", err)
			}
			if len(controlStore.reclaimRequests) != 1 {
				t.Fatalf("Task finalizer submitted %d reclamation requests, want one", len(controlStore.reclaimRequests))
			}
			request := controlStore.reclaimRequests[0]
			if !request.ContinuitySession || !request.FinalContinuitySession || request.FinalPromptAttemptID != controlStore.receipt.PromptAttemptID ||
				request.TerminalProjectionID != controlStore.receipt.ProjectionID {
				t.Fatalf("reclamation lost the exact finalized attempt/projection: %#v", request)
			}
			var remaining corev1alpha1.Task
			if err := reconciler.Get(ctx, client.ObjectKeyFromObject(task), &remaining); !apierrors.IsNotFound(err) {
				t.Fatalf("Task remains after successful finalizer cleanup: %v", err)
			}
		})
	}
}

func TestDeletedSessionTaskCleanupRejectsMissingOrInvalidReceipt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*store.SessionTurnCleanupReceipt)
	}{
		{name: "missing legacy proof"},
		{name: "wrong namespace", mutate: func(r *store.SessionTurnCleanupReceipt) { r.Namespace = "another-namespace" }},
		{name: "wrong Session name", mutate: func(r *store.SessionTurnCleanupReceipt) { r.SessionName = "another-session" }},
		{name: "wrong prompt attempt", mutate: func(r *store.SessionTurnCleanupReceipt) { r.PromptAttemptID = "another-attempt" }},
		{name: "wrong Session UID", mutate: func(r *store.SessionTurnCleanupReceipt) { r.Key.SessionUID = "another-session-uid" }},
		{name: "wrong Task UID", mutate: func(r *store.SessionTurnCleanupReceipt) { r.Key.TaskUID = "another-task" }},
		{name: "payload altered", mutate: func(r *store.SessionTurnCleanupReceipt) { r.Payload = []byte(`{}`) }},
		{name: "missing delivery proof", mutate: func(r *store.SessionTurnCleanupReceipt) { r.DeliveryDigest = "" }},
		{name: "dead letter", mutate: func(r *store.SessionTurnCleanupReceipt) {
			r.ProjectionState, r.DeliveryDigest, r.DeliveredAt = store.OutboxProjectionDeadLetter, "", nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task, controlStore := deletedSessionTaskFixture(t)
			if tc.mutate == nil {
				controlStore.receipt = nil
			} else {
				tc.mutate(controlStore.receipt)
			}
			reconciler := newUnitReconciler(newTestScheme(), task)
			reconciler.DurableControlStore = controlStore
			ready, err := reconciler.acpTaskDeletionReady(context.Background(), task)
			if ready {
				t.Fatalf("Task cleanup accepted missing/invalid Session proof: %v", err)
			}
		})
	}
}

func TestSessionBoundTaskDeletionRetainsAuthorityUntilArchiveCommits(t *testing.T) {
	for _, intent := range []corev1alpha1.WorkspaceIntent{corev1alpha1.WorkspaceIntentRead, corev1alpha1.WorkspaceIntentWrite} {
		t.Run(string(intent), func(t *testing.T) {
			task, controlStore := deletedSessionTaskFixture(t)
			task.Spec.Workspace.Intent = intent
			receipt := controlStore.receipt
			controlStore.projection = receipt.OutboxProjection()
			controlStore.turn = receipt.SessionTurn()
			controlStore.receipt = nil
			reconciler := newUnitReconciler(newTestScheme(), task)
			reconciler.DurableControlStore = controlStore
			if !runtimeSessionCleanupCompleteForUID(task, task.UID) {
				t.Fatal("fixture lacks runtime retirement proof")
			}
			if ready, err := reconciler.acpTaskDeletionReady(context.Background(), task); err != nil || ready {
				t.Fatalf("Task deletion before Session archival = %v, %v; want retained authority", ready, err)
			}
			controlStore.turn, controlStore.projection = nil, nil
			controlStore.receipt = receipt
			if ready, err := reconciler.acpTaskDeletionReady(context.Background(), task); err != nil || !ready {
				t.Fatalf("Task deletion after Session archival = %v, %v; want ready", ready, err)
			}
		})
	}
}

type sessionCleanupReceiptControlStore struct {
	*promptAttemptReclaimControlStore
	receipt *store.SessionTurnCleanupReceipt
	turn    *store.SessionTurn
}

func (s *sessionCleanupReceiptControlStore) GetSessionTurn(_ context.Context, id string) (*store.SessionTurn, error) {
	if s.turn != nil && s.turn.ID == id {
		turn := *s.turn
		return &turn, nil
	}
	return nil, store.ErrNotFound
}

func (s *sessionCleanupReceiptControlStore) GetSessionTurnCleanupReceipt(context.Context, string, string, string) (*store.SessionTurnCleanupReceipt, error) {
	if s.receipt == nil {
		return nil, store.ErrNotFound
	}
	receipt := *s.receipt
	receipt.Payload = append([]byte(nil), receipt.Payload...)
	return &receipt, nil
}

func deletedSessionTaskFixture(t *testing.T) (*corev1alpha1.Task, *sessionCleanupReceiptControlStore) {
	t.Helper()
	task, attempt := promptAttemptReclaimFinalizerFixture(t)
	task.Finalizers = []string{labels.TaskFinalizer}
	task.Spec.Workspace = &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentRead}
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "deleted-session"}
	task.Status.Delivery = &corev1alpha1.TaskDeliveryStatus{
		State: corev1alpha1.TaskDeliveryStateNotRequested, Outcome: corev1alpha1.TaskDeliveryOutcomeNotRequested,
	}
	attempt.DeliveryState = store.PromptDeliveryNotRequested
	attempt.SessionUID, attempt.SessionLeaseGeneration = "session-uid", 4
	attempt.RuntimeInstanceID = "runtime-instance"
	attempt.RequestDigest = store.CanonicalBytesDigest([]byte("request"))
	task.Status.Execution.RuntimeSessionUID = attempt.SessionUID
	task.Status.Execution.RuntimeSessionGeneration = 1
	task.Status.Execution.RuntimeInstanceID = attempt.RuntimeInstanceID
	task.Status.Execution.RequestDigest = attempt.RequestDigest
	cleanupDigest, err := taskScopedRuntimeSessionCleanupDigest(task.UID, task.Status.Execution.Attempt, attempt.RuntimeInstanceID, attempt.SessionUID, 1)
	if err != nil {
		t.Fatal(err)
	}
	task.Status.Execution.RuntimeSessionCleanupDigest = cleanupDigest
	key := store.SessionTurnKey{
		SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration,
		TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
	}
	turnID, err := key.CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(taskterminal.Projection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: string(task.UID), Attempt: task.Status.Execution.Attempt,
		Phase: task.Status.Phase, Execution: *task.Status.Execution.DeepCopy(), Delivery: task.Status.Delivery.DeepCopy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	digest := store.CanonicalBytesDigest(payload)
	receipt := &store.SessionTurnCleanupReceipt{
		Namespace: task.Namespace, SessionName: task.Spec.SessionRef.Name, OperationID: "delete-session",
		OperationDigest: store.CanonicalBytesDigest([]byte("delete-session")),
		TurnID:          turnID, Key: key, PromptAttemptID: attempt.ID, TerminalKind: store.SessionTurnAssistantResult, FinalizedAt: now,
		ProjectionID: store.CanonicalControlID("outbox", turnID, "TaskTerminalStatus"), ProjectionKind: "TaskTerminalStatus", ProjectionDigest: digest,
		AggregateKind: "SessionTurn", AggregateID: turnID, Payload: payload, PayloadDigest: digest,
		ProjectionState: store.OutboxProjectionDelivered, DeliveryDigest: store.CanonicalBytesDigest([]byte("delivered")), DeliveredAt: &now,
	}
	if err := receipt.Validate(task.Namespace, task.Spec.SessionRef.Name, turnID); err != nil {
		t.Fatal(err)
	}
	return task, &sessionCleanupReceiptControlStore{
		promptAttemptReclaimControlStore: &promptAttemptReclaimControlStore{attempt: attempt, reclaimed: 1}, receipt: receipt,
	}
}
