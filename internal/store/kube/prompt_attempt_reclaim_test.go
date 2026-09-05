package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	controlstore "github.com/orka-agents/orka/internal/store"
	sqlitestore "github.com/orka-agents/orka/internal/store/sqlite"
	"github.com/orka-agents/orka/internal/taskterminal"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestReclaimPromptAttemptsDefersWhileReferencesRemain(t *testing.T) {
	tests := []struct {
		name  string
		block func(t *testing.T, ctx context.Context, kubeStore *Store, kubeClient client.Client, fence controlstore.ControllerEpochFence, attempt *controlstore.PromptAttempt)
	}{
		{
			name: "nonterminal attempt",
			block: func(t *testing.T, ctx context.Context, kubeStore *Store, _ client.Client, fence controlstore.ControllerEpochFence, attempt *controlstore.PromptAttempt) {
				t.Helper()
				if _, err := kubeStore.CreatePromptAttempt(ctx, boundPromptAttemptForKubeTest(&controlstore.PromptAttempt{
					Key: controlstore.PromptAttemptKey{
						Namespace: attempt.Key.Namespace, TaskUID: attempt.Key.TaskUID, Attempt: 1, PromptID: "prompt-old",
					},
					RequestDigest: testDigest("prompt-old"),
				}), fence); err != nil {
					t.Fatalf("create nonterminal attempt: %v", err)
				}
			},
		},
		{
			name: "active publication",
			block: func(t *testing.T, ctx context.Context, _ *Store, kubeClient client.Client, _ controlstore.ControllerEpochFence, attempt *controlstore.PromptAttempt) {
				t.Helper()
				publication := &corev1alpha1.Publication{
					ObjectMeta: metav1.ObjectMeta{Namespace: attempt.Key.Namespace, Name: "publication-active"},
					Spec: corev1alpha1.PublicationSpec{
						ID: "publication-active", TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
					},
				}
				if err := kubeClient.Create(ctx, publication); err != nil {
					t.Fatalf("create active publication: %v", err)
				}
				publication.Status.State = corev1alpha1.PublicationControlState(controlstore.PublicationPublishing)
				if err := kubeClient.Status().Update(ctx, publication); err != nil {
					t.Fatalf("mark publication active: %v", err)
				}
			},
		},
		{
			name: "session reconciliation reference",
			block: func(t *testing.T, ctx context.Context, _ *Store, kubeClient client.Client, _ controlstore.ControllerEpochFence, attempt *controlstore.PromptAttempt) {
				t.Helper()
				control := &corev1alpha1.RuntimeSessionControl{
					ObjectMeta: metav1.ObjectMeta{Namespace: attempt.Key.Namespace, Name: "session-reconciliation"},
					Spec: corev1alpha1.RuntimeSessionControlSpec{
						SessionName: "session-reconciliation", SessionUID: "session-reconciliation-uid",
						RequestDigest: testDigest("session-reconciliation"),
						Owner:         corev1alpha1.ControlRecordOwner{Kind: "Session", UID: "session-reconciliation-uid"},
					},
				}
				if err := kubeClient.Create(ctx, control); err != nil {
					t.Fatalf("create Session control: %v", err)
				}
				control.Status.Availability = corev1alpha1.RuntimeSessionControlAvailability(controlstore.SessionReconciliationBlocked)
				control.Status.BlockedReason = "publication outcome requires reconciliation"
				control.Status.RelatedPromptAttemptID = attempt.ID
				if err := kubeClient.Status().Update(ctx, control); err != nil {
					t.Fatalf("record Session reconciliation reference: %v", err)
				}
			},
		},
		{
			name: "undelivered terminal projection",
			block: func(t *testing.T, _ context.Context, _ *Store, _ client.Client, _ controlstore.ControllerEpochFence, _ *controlstore.PromptAttempt) {
				t.Helper()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
			attempt := createFailedPromptAttempt(t, ctx, kubeStore, fence, "task-reclaim-blocked", 2, "prompt-final")
			projectionID := enqueueTaskTerminalProjection(t, ctx, kubeStore, fence, attempt, tt.name != "undelivered terminal projection")
			tt.block(t, ctx, kubeStore, kubeClient, fence, attempt)
			taskName := createDeletingTaskForPromptAttempt(t, ctx, kubeClient, attempt)

			deleted, err := kubeStore.ReclaimPromptAttempts(ctx, controlstore.ReclaimPromptAttemptsRequest{
				Namespace: attempt.Key.Namespace, TaskName: taskName, TaskUID: attempt.Key.TaskUID,
				Mode:                 controlstore.PromptAttemptReclamationProjected,
				FinalPromptAttemptID: attempt.ID, TerminalProjectionID: projectionID, Fence: fence,
			})
			if !errors.Is(err, controlstore.ErrNotReady) {
				t.Fatalf("ReclaimPromptAttempts() error = %v, want ErrNotReady", err)
			}
			if deleted != 0 {
				t.Fatalf("ReclaimPromptAttempts() deleted = %d, want 0", deleted)
			}
			if _, err := kubeStore.GetPromptAttempt(ctx, attempt.ID); err != nil {
				t.Fatalf("final prompt attempt was deleted while still referenced: %v", err)
			}
		})
	}
}

func TestReclaimPromptAttemptsDeletesTerminalTaskRecords(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
	oldAttempt := createFailedPromptAttempt(t, ctx, kubeStore, fence, "task-reclaim", 1, "prompt-old")
	finalAttempt := createFailedPromptAttempt(t, ctx, kubeStore, fence, "task-reclaim", 2, "prompt-final")
	otherAttempt := createFailedPromptAttempt(t, ctx, kubeStore, fence, "task-other", 1, "prompt-other")
	taskName := createDeletingTaskForPromptAttempt(t, ctx, kubeClient, finalAttempt)
	projectionID := enqueueTaskTerminalProjection(t, ctx, kubeStore, fence, finalAttempt, true)

	request := controlstore.ReclaimPromptAttemptsRequest{
		Namespace: finalAttempt.Key.Namespace, TaskName: taskName, TaskUID: finalAttempt.Key.TaskUID,
		Mode:                 controlstore.PromptAttemptReclamationProjected,
		FinalPromptAttemptID: finalAttempt.ID, TerminalProjectionID: projectionID, Fence: fence,
	}
	deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request)
	if err != nil {
		t.Fatalf("ReclaimPromptAttempts(): %v", err)
	}
	if deleted != 2 {
		t.Fatalf("ReclaimPromptAttempts() deleted = %d, want 2", deleted)
	}
	for _, id := range []string{oldAttempt.ID, finalAttempt.ID} {
		if _, err := kubeStore.GetPromptAttempt(ctx, id); !errors.Is(err, controlstore.ErrNotFound) {
			t.Fatalf("GetPromptAttempt(%q) error = %v, want ErrNotFound", id, err)
		}
	}
	if _, err := kubeStore.GetPromptAttempt(ctx, otherAttempt.ID); err != nil {
		t.Fatalf("unrelated PromptAttempt was deleted: %v", err)
	}
	assertPromptAttemptReclamationMarker(t, ctx, kubeClient, finalAttempt.Key.TaskUID, false)
	assertPromptAttemptReclamationCompleted(t, ctx, kubeClient, request, true)
	if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); err != nil {
		t.Fatalf("idempotent PreparePromptAttemptReclamation() error = %v", err)
	}
	assertPromptAttemptReclamationMarker(t, ctx, kubeClient, finalAttempt.Key.TaskUID, false)
	if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request); err != nil || deleted != 0 {
		t.Fatalf("idempotent ReclaimPromptAttempts() = deleted:%d err:%v, want 0,nil", deleted, err)
	}
	assertPromptAttemptReclamationMarker(t, ctx, kubeClient, finalAttempt.Key.TaskUID, false)
}

func TestReclaimPromptAttemptsDeletesRestoredSourceRecordsIdempotently(t *testing.T) {
	tests := []struct {
		name      string
		execution controlstore.PromptExecutionState
		delivery  controlstore.PromptDeliveryState
		phase     corev1alpha1.TaskPhase
		outcome   corev1alpha1.TaskExecutionOutcome
	}{
		{name: "failed", execution: controlstore.PromptExecutionFailed, delivery: controlstore.PromptDeliveryNotRequested, phase: corev1alpha1.TaskPhaseFailed, outcome: corev1alpha1.TaskExecutionOutcomeFailed},
		{name: "outcome unknown", execution: controlstore.PromptExecutionOutcomeUnknown, delivery: controlstore.PromptDeliveryNotRequested, phase: corev1alpha1.TaskPhaseFailed, outcome: corev1alpha1.TaskExecutionOutcomeOutcomeUnknown},
		{name: "succeeded verified exact", execution: controlstore.PromptExecutionSucceeded, delivery: controlstore.PromptDeliveryVerifiedExact, phase: corev1alpha1.TaskPhaseSucceeded, outcome: corev1alpha1.TaskExecutionOutcomeSucceeded},
		{name: "succeeded cancelled before publish", execution: controlstore.PromptExecutionSucceeded, delivery: controlstore.PromptDeliveryCancelledBeforePublish, phase: corev1alpha1.TaskPhaseCancelled, outcome: corev1alpha1.TaskExecutionOutcomeSucceeded},
		{name: "succeeded delivery conflict", execution: controlstore.PromptExecutionSucceeded, delivery: controlstore.PromptDeliveryConflict, phase: corev1alpha1.TaskPhaseFailed, outcome: corev1alpha1.TaskExecutionOutcomeSucceeded},
		{name: "cancelled", execution: controlstore.PromptExecutionCancelled, delivery: controlstore.PromptDeliveryNotRequested, phase: corev1alpha1.TaskPhaseCancelled, outcome: corev1alpha1.TaskExecutionOutcomeCancelled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
			taskUID := "task-restored-" + strings.ReplaceAll(tt.name, " ", "-")
			attempt := createTerminalPromptAttempt(t, ctx, kubeStore, fence, taskUID, 1, "prompt-final", tt.execution, tt.delivery)
			taskName := attempt.Key.TaskUID
			projectionID := enqueueTaskTerminalProjection(t, ctx, kubeStore, fence, attempt, true)
			setRestoredPromptAttemptOwner(t, ctx, kubeClient, taskName, attempt, restoredPromptAttemptTerminalStatus{
				phase: tt.phase, execution: corev1alpha1.TaskExecutionState(tt.execution), outcome: tt.outcome, delivery: tt.delivery,
			})
			request := controlstore.ReclaimPromptAttemptsRequest{
				Namespace: attempt.Key.Namespace, TaskName: taskName, TaskUID: attempt.Key.TaskUID,
				Mode:                 controlstore.PromptAttemptReclamationProjected,
				FinalPromptAttemptID: attempt.ID, TerminalProjectionID: projectionID, Fence: fence,
			}

			if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); err != nil {
				t.Fatalf("PreparePromptAttemptReclamation(restored source): %v", err)
			}
			if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request); err != nil || deleted != 1 {
				t.Fatalf("ReclaimPromptAttempts(restored source) = deleted:%d err:%v, want 1,nil", deleted, err)
			}
			if _, err := kubeStore.GetPromptAttempt(ctx, attempt.ID); !errors.Is(err, controlstore.ErrNotFound) {
				t.Fatalf("GetPromptAttempt(restored source) error = %v, want ErrNotFound", err)
			}
			assertPromptAttemptReclamationCompleted(t, ctx, kubeClient, request, true)
			if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); err != nil {
				t.Fatalf("PreparePromptAttemptReclamation(restored retry): %v", err)
			}
			if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request); err != nil || deleted != 0 {
				t.Fatalf("ReclaimPromptAttempts(restored retry) = deleted:%d err:%v, want 0,nil", deleted, err)
			}
		})
	}
}

func TestPreparePromptAttemptReclamationRejectsUnvalidatedRestoredSourceIdentity(t *testing.T) {
	tests := []struct {
		name          string
		mutateTask    func(task *corev1alpha1.Task)
		mutateRequest func(request *controlstore.ReclaimPromptAttemptsRequest)
	}{
		{name: "phase mismatch", mutateTask: func(task *corev1alpha1.Task) {
			task.Status.Phase = corev1alpha1.TaskPhaseFailed
		}},
		{name: "execution outcome mismatch", mutateTask: func(task *corev1alpha1.Task) {
			task.Status.Execution.Outcome = corev1alpha1.TaskExecutionOutcomeFailed
		}},
		{name: "delivery outcome mismatch", mutateTask: func(task *corev1alpha1.Task) {
			task.Status.Delivery.Outcome = corev1alpha1.TaskDeliveryOutcomeDeliveryConflict
		}},
		{name: "settlement reason missing", mutateTask: func(task *corev1alpha1.Task) {
			task.Status.Execution.Reason = ""
		}},
		{name: "binding digest mismatch", mutateTask: func(task *corev1alpha1.Task) {
			task.Status.AgentExecutionBinding.BindingDigest = testDigest("foreign-binding")
		}},
		{name: "snapshot digest mismatch", mutateTask: func(task *corev1alpha1.Task) {
			task.Status.AgentExecutionBinding.Snapshot.Digest = testDigest("foreign-snapshot")
		}},
		{name: "request digest mismatch", mutateTask: func(task *corev1alpha1.Task) {
			task.Status.Execution.RequestDigest = testDigest("foreign-request")
		}},
		{name: "source UID mismatch", mutateTask: func(task *corev1alpha1.Task) {
			task.Status.AgentExecutionBinding.Task.UID = types.UID("foreign-source")
		}},
		{name: "final attempt mismatch", mutateRequest: func(request *controlstore.ReclaimPromptAttemptsRequest) {
			request.FinalPromptAttemptID = "foreign-attempt"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
			attempt := createTerminalPromptAttempt(t, ctx, kubeStore, fence, "task-restored-reject", 1, "prompt-final",
				controlstore.PromptExecutionSucceeded, controlstore.PromptDeliveryVerifiedExact)
			taskName := attempt.Key.TaskUID
			projectionID := enqueueTaskTerminalProjection(t, ctx, kubeStore, fence, attempt, true)
			task := setRestoredPromptAttemptOwner(t, ctx, kubeClient, taskName, attempt, restoredPromptAttemptTerminalStatus{
				phase: corev1alpha1.TaskPhaseSucceeded, execution: corev1alpha1.TaskExecutionStateSucceeded,
				outcome: corev1alpha1.TaskExecutionOutcomeSucceeded, delivery: controlstore.PromptDeliveryVerifiedExact,
			})
			if tt.mutateTask != nil {
				tt.mutateTask(task)
			}
			if err := kubeClient.Status().Update(ctx, task); err != nil {
				t.Fatalf("mutate restored Task status: %v", err)
			}
			request := controlstore.ReclaimPromptAttemptsRequest{
				Namespace: attempt.Key.Namespace, TaskName: taskName, TaskUID: attempt.Key.TaskUID,
				Mode:                 controlstore.PromptAttemptReclamationProjected,
				FinalPromptAttemptID: attempt.ID, TerminalProjectionID: projectionID, Fence: fence,
			}
			if tt.mutateRequest != nil {
				tt.mutateRequest(&request)
			}
			if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); !errors.Is(err, controlstore.ErrConflict) {
				t.Fatalf("PreparePromptAttemptReclamation() error = %v, want ErrConflict", err)
			}
			if _, err := kubeStore.GetPromptAttempt(ctx, attempt.ID); err != nil {
				t.Fatalf("unvalidated restored source attempt was mutated: %v", err)
			}
			assertPromptAttemptReclamationMarker(t, ctx, kubeClient, attempt.Key.TaskUID, false)
		})
	}
}

func TestPreparePromptAttemptReclamationRejectsIncompleteTerminalProjectionPayload(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
	attempt := createTerminalPromptAttempt(t, ctx, kubeStore, fence, "task-restored-incomplete-projection", 1, "prompt-final",
		controlstore.PromptExecutionSucceeded, controlstore.PromptDeliveryVerifiedExact)
	taskName := attempt.Key.TaskUID
	setRestoredPromptAttemptOwner(t, ctx, kubeClient, taskName, attempt, restoredPromptAttemptTerminalStatus{
		phase: corev1alpha1.TaskPhaseSucceeded, execution: corev1alpha1.TaskExecutionStateSucceeded,
		outcome: corev1alpha1.TaskExecutionOutcomeSucceeded, delivery: controlstore.PromptDeliveryVerifiedExact,
	})
	projectionID := enqueueRawTaskTerminalProjection(t, ctx, kubeStore, fence, attempt, []byte(`{"phase":"Succeeded"}`), true)
	request := controlstore.ReclaimPromptAttemptsRequest{
		Namespace: attempt.Key.Namespace, TaskName: taskName, TaskUID: attempt.Key.TaskUID,
		Mode:                 controlstore.PromptAttemptReclamationProjected,
		FinalPromptAttemptID: attempt.ID, TerminalProjectionID: projectionID, Fence: fence,
	}

	if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("PreparePromptAttemptReclamation() error = %v, want ErrConflict", err)
	}
	if _, err := kubeStore.GetPromptAttempt(ctx, attempt.ID); err != nil {
		t.Fatalf("incomplete terminal projection mutated its source PromptAttempt: %v", err)
	}
	assertPromptAttemptReclamationMarker(t, ctx, kubeClient, attempt.Key.TaskUID, false)
}

func TestReclaimPromptAttemptsRecoversMarkerCleanupCrash(t *testing.T) {
	tests := []struct {
		name                 string
		persistMarkerDelete  bool
		wantMarkerAfterError bool
	}{
		{name: "before marker delete", wantMarkerAfterError: true},
		{name: "after marker delete before acknowledgement", persistMarkerDelete: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
			attempt := createFailedPromptAttempt(t, ctx, kubeStore, fence, "task-reclaim-cleanup-crash", 1, "prompt-final")
			taskName := createDeletingTaskForPromptAttempt(t, ctx, kubeClient, attempt)
			projectionID := enqueueTaskTerminalProjection(t, ctx, kubeStore, fence, attempt, true)
			request := controlstore.ReclaimPromptAttemptsRequest{
				Namespace: attempt.Key.Namespace, TaskName: taskName, TaskUID: attempt.Key.TaskUID,
				Mode:                 controlstore.PromptAttemptReclamationProjected,
				FinalPromptAttemptID: attempt.ID, TerminalProjectionID: projectionID, Fence: fence,
			}
			if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); err != nil {
				t.Fatalf("PreparePromptAttemptReclamation(): %v", err)
			}
			assertPromptAttemptReclamationMarker(t, ctx, kubeClient, attempt.Key.TaskUID, true)

			withWatch, ok := kubeClient.(client.WithWatch)
			if !ok {
				t.Fatal("fake client does not implement client.WithWatch")
			}
			injectedErr := errors.New("simulated marker cleanup crash")
			markerName := promptAttemptReclamationMarkerName(attempt.Key.TaskUID)
			kubeStore.client = interceptor.NewClient(withWatch, interceptor.Funcs{
				Delete: func(ctx context.Context, c client.WithWatch, object client.Object, options ...client.DeleteOption) error {
					marker, isMarker := object.(*corev1.ConfigMap)
					if !isMarker || marker.Namespace != testControlNamespace || marker.Name != markerName {
						return c.Delete(ctx, object, options...)
					}
					if tt.persistMarkerDelete {
						if err := c.Delete(ctx, object, options...); err != nil {
							return err
						}
					}
					return injectedErr
				},
			})
			deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request)
			if !errors.Is(err, injectedErr) {
				t.Fatalf("ReclaimPromptAttempts() error = %v, want injected cleanup error", err)
			}
			if deleted != 1 {
				t.Fatalf("ReclaimPromptAttempts() deleted = %d, want 1", deleted)
			}
			if _, err := kubeStore.GetPromptAttempt(ctx, attempt.ID); !errors.Is(err, controlstore.ErrNotFound) {
				t.Fatalf("GetPromptAttempt() after cleanup crash error = %v, want ErrNotFound", err)
			}
			assertPromptAttemptReclamationMarker(t, ctx, kubeClient, attempt.Key.TaskUID, tt.wantMarkerAfterError)
			assertPromptAttemptReclamationCompleted(t, ctx, kubeClient, request, true)

			kubeStore.client = kubeClient
			if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); err != nil {
				t.Fatalf("PreparePromptAttemptReclamation() retry: %v", err)
			}
			if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request); err != nil || deleted != 0 {
				t.Fatalf("ReclaimPromptAttempts() retry = deleted:%d err:%v, want 0,nil", deleted, err)
			}
			assertPromptAttemptReclamationMarker(t, ctx, kubeClient, attempt.Key.TaskUID, false)
		})
	}
}

func TestReclaimPromptAttemptsRecordsCompletionOnlyAfterDeletionIsObserved(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
	attempt := createFailedPromptAttempt(t, ctx, kubeStore, fence, "task-reclaim-finalized-delete", 1, "prompt-final")
	object := &corev1alpha1.PromptAttempt{}
	key := client.ObjectKey{Namespace: attempt.Key.Namespace, Name: objectName(promptAttemptNamePrefix, attempt.ID)}
	if err := kubeClient.Get(ctx, key, object); err != nil {
		t.Fatalf("get PromptAttempt fixture: %v", err)
	}
	object.Finalizers = []string{"reclamation-test"}
	if err := kubeClient.Update(ctx, object); err != nil {
		t.Fatalf("add PromptAttempt finalizer: %v", err)
	}
	taskName := createDeletingTaskForPromptAttempt(t, ctx, kubeClient, attempt)
	projectionID := enqueueTaskTerminalProjection(t, ctx, kubeStore, fence, attempt, true)
	request := controlstore.ReclaimPromptAttemptsRequest{
		Namespace: attempt.Key.Namespace, TaskName: taskName, TaskUID: attempt.Key.TaskUID,
		Mode:                 controlstore.PromptAttemptReclamationProjected,
		FinalPromptAttemptID: attempt.ID, TerminalProjectionID: projectionID, Fence: fence,
	}
	if _, err := kubeStore.ReclaimPromptAttempts(ctx, request); !errors.Is(err, controlstore.ErrNotReady) {
		t.Fatalf("ReclaimPromptAttempts(finalizer retained) error = %v, want ErrNotReady", err)
	}
	assertPromptAttemptReclamationCompleted(t, ctx, kubeClient, request, false)
	assertPromptAttemptReclamationMarker(t, ctx, kubeClient, attempt.Key.TaskUID, true)

	if err := kubeClient.Get(ctx, key, object); err != nil {
		t.Fatalf("get deleting PromptAttempt: %v", err)
	}
	object.Finalizers = nil
	if err := kubeClient.Update(ctx, object); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("remove PromptAttempt finalizer: %v", err)
	}
	if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); err != nil {
		t.Fatalf("PreparePromptAttemptReclamation(retry): %v", err)
	}
	if _, err := kubeStore.ReclaimPromptAttempts(ctx, request); err != nil {
		t.Fatalf("ReclaimPromptAttempts(retry): %v", err)
	}
	assertPromptAttemptReclamationCompleted(t, ctx, kubeClient, request, true)
}

func TestReclaimPromptAttemptsCleansNoAttemptMarker(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
	const taskUID = "task-reclaim-no-attempt"
	createDeletingAgentTask(t, ctx, kubeClient, "tenant-a", taskUID, taskUID)
	request := controlstore.ReclaimPromptAttemptsRequest{
		Namespace: "tenant-a", TaskName: taskUID, TaskUID: taskUID,
		Mode: controlstore.PromptAttemptReclamationNoAttempt, Fence: fence,
	}
	if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); err != nil {
		t.Fatalf("PreparePromptAttemptReclamation(): %v", err)
	}
	assertPromptAttemptReclamationMarker(t, ctx, kubeClient, taskUID, true)
	if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request); err != nil || deleted != 0 {
		t.Fatalf("ReclaimPromptAttempts() = deleted:%d err:%v, want 0,nil", deleted, err)
	}
	assertPromptAttemptReclamationMarker(t, ctx, kubeClient, taskUID, false)
	assertPromptAttemptReclamationCompleted(t, ctx, kubeClient, request, true)
	if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request); err != nil || deleted != 0 {
		t.Fatalf("ReclaimPromptAttempts() retry = deleted:%d err:%v, want 0,nil", deleted, err)
	}
	assertPromptAttemptReclamationMarker(t, ctx, kubeClient, taskUID, false)
}

func TestReclaimPromptAttemptsCompletionAllowsUnboundContinuityProjectionRetry(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
	attempt := createFailedPromptAttempt(t, ctx, kubeStore, fence, "task-reclaim-unbound-continuity", 1, "prompt-final")
	taskName := createDeletingTaskForPromptAttempt(t, ctx, kubeClient, attempt)
	projectionID := enqueueTaskTerminalProjection(t, ctx, kubeStore, fence, attempt, true)
	request := controlstore.ReclaimPromptAttemptsRequest{
		Namespace: attempt.Key.Namespace, TaskName: taskName, TaskUID: attempt.Key.TaskUID,
		Mode:                 controlstore.PromptAttemptReclamationProjected,
		ContinuitySession:    true,
		FinalPromptAttemptID: attempt.ID, TerminalProjectionID: projectionID, Fence: fence,
	}
	if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request); err != nil || deleted != 1 {
		t.Fatalf("ReclaimPromptAttempts() = deleted:%d err:%v, want 1,nil", deleted, err)
	}
	retryRequest := request
	retryRequest.TerminalProjectionID = ""
	if err := kubeStore.PreparePromptAttemptReclamation(ctx, retryRequest); err != nil {
		t.Fatalf("PreparePromptAttemptReclamation(post-attempt retry): %v", err)
	}
	if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, retryRequest); err != nil || deleted != 0 {
		t.Fatalf("ReclaimPromptAttempts(post-attempt retry) = deleted:%d err:%v, want 0,nil", deleted, err)
	}
}

func TestReclaimPromptAttemptsAcceptsPinnedLegacySessionProjectionAfterPreparedDelete(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
	fixture := seedFinalizedSessionReclaimTurn(t, ctx, kubeStore, kubeClient, fence, true)
	taskUID, namespace := string(fixture.task.UID), fixture.task.Namespace
	attempt, request := fixture.attempt, fixture.request
	createDeletingAgentTask(t, ctx, kubeClient, namespace, taskUID, taskUID)
	if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); err != nil {
		t.Fatalf("PreparePromptAttemptReclamation(legacy Session): %v", err)
	}
	assertPromptAttemptReclamationMarker(t, ctx, kubeClient, taskUID, true)
	attemptObject := &corev1alpha1.PromptAttempt{}
	if err := kubeClient.Get(ctx, client.ObjectKey{
		Namespace: namespace, Name: objectName(promptAttemptNamePrefix, attempt.ID),
	}, attemptObject); err != nil {
		t.Fatalf("load prepared PromptAttempt: %v", err)
	}
	if err := kubeClient.Delete(ctx, attemptObject); err != nil {
		t.Fatalf("simulate committed PromptAttempt deletion: %v", err)
	}
	if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); err != nil {
		t.Fatalf("PreparePromptAttemptReclamation(post-delete retry): %v", err)
	}
	if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request); err != nil || deleted != 0 {
		t.Fatalf("ReclaimPromptAttempts(post-delete retry) = deleted:%d err:%v, want 0,nil", deleted, err)
	}
	assertPromptAttemptReclamationMarker(t, ctx, kubeClient, taskUID, false)
	assertPromptAttemptReclamationCompleted(t, ctx, kubeClient, request, true)
}

type finalizedSessionReclaimFixture struct {
	task       *corev1alpha1.Task
	attempt    *controlstore.PromptAttempt
	turn       *controlstore.SessionTurn
	projection *controlstore.OutboxProjection
	request    controlstore.ReclaimPromptAttemptsRequest
}

func seedFinalizedSessionReclaimTurn(
	t *testing.T,
	ctx context.Context,
	kubeStore *Store,
	kubeClient client.Client,
	fence controlstore.ControllerEpochFence,
	legacy bool,
) finalizedSessionReclaimFixture {
	t.Helper()
	persistence, ok := kubeStore.sessionTurns.(*sqlitestore.Store)
	if !ok {
		t.Fatalf("SessionTurn persistence = %T, want *sqlite.Store", kubeStore.sessionTurns)
	}
	const (
		namespace   = "tenant-a"
		taskUID     = "task-legacy-session-projection"
		sessionName = "legacy-session-projection"
		sessionUID  = "legacy-session-projection-uid"
		promptID    = "prompt-legacy-session-projection"
	)
	if err := persistence.CreateSession(ctx, &controlstore.SessionRecord{
		Namespace: namespace, Name: sessionName, SessionType: "task",
	}); err != nil {
		t.Fatalf("CreateSession(): %v", err)
	}
	task := ensureActiveAgentTask(t, ctx, kubeClient, namespace, taskUID, taskUID)
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: sessionName, Create: true}
	if err := kubeClient.Update(ctx, task); err != nil {
		t.Fatalf("record Task SessionRef: %v", err)
	}
	control, err := kubeStore.CreateSessionControl(ctx, &controlstore.SessionControl{
		Namespace: namespace, SessionName: sessionName, SessionUID: sessionUID,
		RequestDigest: testDigest("legacy-session-control"), LeaseGeneration: 1,
	}, fence)
	if err != nil {
		t.Fatalf("CreateSessionControl(): %v", err)
	}
	promptRequestDigest := testDigest("legacy-session-prompt")
	leaseRequestDigest, err := controlstore.SessionMutationLeaseRequestDigest(
		control.SessionUID, control.LeaseGeneration+1, taskUID, 1, promptID, promptRequestDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	leaseExpires := testNow.Add(15 * time.Minute)
	control, err = kubeStore.AcquireSessionMutationLease(ctx, controlstore.AcquireSessionMutationLeaseRequest{
		Namespace: namespace, SessionName: sessionName, SessionUID: sessionUID,
		Fence: fence, ExpectedVersion: control.Version, ExpectedLeaseGeneration: control.LeaseGeneration,
		TaskUID: taskUID, Attempt: 1, PromptID: promptID, RequestDigest: leaseRequestDigest,
		AcquiredAt: testNow, ExpiresAt: &leaseExpires, Lineage: testSessionLineageClaim(control),
	})
	if err != nil {
		t.Fatalf("AcquireSessionMutationLease(): %v", err)
	}
	attempt, err := kubeStore.CreatePromptAttempt(ctx, boundPromptAttemptForKubeTest(&controlstore.PromptAttempt{
		Key:           controlstore.PromptAttemptKey{Namespace: namespace, TaskUID: taskUID, Attempt: 1, PromptID: promptID},
		RequestDigest: promptRequestDigest,
	}), fence)
	if err != nil {
		t.Fatalf("CreatePromptAttempt(): %v", err)
	}
	for i, state := range []controlstore.PromptExecutionState{
		controlstore.PromptExecutionReserved,
		controlstore.PromptExecutionSessionStarting,
		controlstore.PromptExecutionPlanned,
	} {
		transition := controlstore.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: state, OperationID: "legacy-session-" + string(state),
			OperationDigest: testDigest("legacy-session-" + string(state)), UpdatedAt: testNow.Add(time.Duration(i+1) * time.Minute),
		}
		if state == controlstore.PromptExecutionSessionStarting {
			transition.SessionUID = sessionUID
			transition.SessionLeaseGeneration = control.LeaseGeneration
			transition.RuntimeInstanceID = "legacy-runtime-instance"
		}
		attempt, err = kubeStore.TransitionPromptAttemptExecution(ctx, transition)
		if err != nil {
			t.Fatalf("TransitionPromptAttemptExecution(%s): %v", state, err)
		}
	}
	turnKey := controlstore.SessionTurnKey{
		SessionUID: sessionUID, LeaseGeneration: control.LeaseGeneration,
		TaskUID: taskUID, Attempt: 1, PromptID: promptID,
	}
	turn, err := kubeStore.CreateSessionTurn(ctx, controlstore.CreateSessionTurnRequest{
		Turn: controlstore.SessionTurn{
			Key: turnKey, PromptAttemptID: attempt.ID, RequestDigest: testDigest("legacy-session-turn"), UserPrompt: "cancel safely",
		},
		Fence: fence, ExpectedSessionVersion: control.Version,
	})
	if err != nil {
		t.Fatalf("CreateSessionTurn(): %v", err)
	}
	attempt, err = kubeStore.TransitionPromptAttemptExecution(ctx, controlstore.PromptAttemptExecutionTransition{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
		NewState: controlstore.PromptExecutionCancelled, OperationID: "legacy-session-cancelled",
		OperationDigest: testDigest("legacy-session-cancelled"), TerminalReason: "Cancelled",
		UpdatedAt: testNow.Add(4 * time.Minute),
	})
	if err != nil {
		t.Fatalf("TransitionPromptAttemptExecution(Cancelled): %v", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: taskUID}, task); err != nil {
		t.Fatalf("reload Task: %v", err)
	}
	task.Status.Phase = corev1alpha1.TaskPhaseCancelled
	task.Status.Execution = &corev1alpha1.TaskExecutionStatus{
		State: corev1alpha1.TaskExecutionStateCancelled, Outcome: corev1alpha1.TaskExecutionOutcomeCancelled,
		Reason: "Cancelled", Attempt: 1, PromptID: promptID, RuntimeInstanceID: attempt.RuntimeInstanceID,
		RuntimeSessionUID: sessionUID, RuntimeSessionGeneration: 1,
		RequestDigest: promptRequestDigest, ControllerEpoch: fence.Epoch,
	}
	task.Status.Delivery = &corev1alpha1.TaskDeliveryStatus{
		State: corev1alpha1.TaskDeliveryStateNotRequested, Outcome: corev1alpha1.TaskDeliveryOutcomeNotRequested,
	}
	if err := kubeClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("record terminal Task status: %v", err)
	}
	terminal := taskterminal.Projection{
		Namespace: namespace, Task: taskUID, TaskUID: taskUID, Attempt: 1, Phase: corev1alpha1.TaskPhaseCancelled,
		Message:   "controller restart recovered terminal cancellation",
		Execution: *task.Status.Execution.DeepCopy(),
		Delivery:  task.Status.Delivery.DeepCopy(),
	}
	if legacy {
		terminal.Execution = corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateCancelled, Outcome: corev1alpha1.TaskExecutionOutcomeCancelled,
			Attempt: 1, PromptID: promptID,
		}
	}
	legacyPayload, err := json.MarshalIndent(terminal, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	projectionID := controlstore.CanonicalControlID("outbox", turn.ID, "TaskTerminalStatus")
	projection := controlstore.OutboxProjection{
		ID: projectionID, AggregateKind: sessionTurnAggregateKind, AggregateID: turn.ID,
		ProjectionKind: "TaskTerminalStatus", Payload: legacyPayload, PayloadDigest: testBytesDigest(legacyPayload),
		AvailableAt: testNow.Add(5 * time.Minute),
	}
	turn, err = kubeStore.FinalizeSessionTurn(ctx, controlstore.FinalizeSessionTurnRequest{
		Key: turnKey, Fence: fence, ExpectedSessionVersion: control.Version, ExpectedTurnVersion: turn.Version,
		FinalizationDigest: testDigest("legacy-session-finalization"), TerminalKind: controlstore.SessionTurnOutcomeMarker,
		TerminalContent: `{"kind":"Cancelled","reason":"controller restart recovered terminal cancellation","assistantResultRecorded":false}`,
		Projection:      projection, FinalizedAt: testNow.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("FinalizeSessionTurn(): %v", err)
	}
	claims, err := kubeStore.ClaimOutboxProjections(ctx, controlstore.ClaimOutboxProjectionsRequest{
		Fence: fence, WorkerID: "legacy-session-projector", Limit: 1,
		LeaseDuration: time.Minute, Now: testNow.Add(6 * time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimOutboxProjections(): %v", err)
	}
	if len(claims) != 1 || claims[0].ID != projectionID {
		t.Fatalf("claimed projections = %#v, want %q", claims, projectionID)
	}
	claimed := claims[0]
	delivered, err := kubeStore.CompleteOutboxProjection(ctx, controlstore.CompleteOutboxProjectionRequest{
		ID: claimed.ID, Fence: fence, ExpectedVersion: claimed.Version, LeaseOwner: claimed.LeaseOwner,
		OperationID: "deliver-legacy-session-projection", OperationDigest: testDigest("deliver-legacy-session-projection"),
		NewState: controlstore.OutboxProjectionDelivered, DeliveryDigest: testDigest("legacy-session-projection-delivered"),
		UpdatedAt: testNow.Add(7 * time.Minute),
	})
	if err != nil {
		t.Fatalf("CompleteOutboxProjection(Delivered): %v", err)
	}
	request := controlstore.ReclaimPromptAttemptsRequest{
		Namespace: namespace, TaskName: taskUID, TaskUID: taskUID,
		Mode: controlstore.PromptAttemptReclamationProjected, ContinuitySession: true, FinalContinuitySession: true,
		FinalPromptAttemptID: attempt.ID, TerminalProjectionID: projectionID, Fence: fence,
	}
	return finalizedSessionReclaimFixture{task: task, attempt: attempt, turn: turn, projection: delivered, request: request}
}

func TestPreparePromptAttemptReclamationRejectsUnprovenEmptyProjectedState(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
	const taskUID = "task-reclaim-missing-attempt"
	createDeletingAgentTask(t, ctx, kubeClient, "tenant-a", taskUID, taskUID)
	request := controlstore.ReclaimPromptAttemptsRequest{
		Namespace: "tenant-a", TaskName: taskUID, TaskUID: taskUID,
		Mode: controlstore.PromptAttemptReclamationProjected, FinalPromptAttemptID: "missing-attempt",
		TerminalProjectionID: "missing-projection", Fence: fence,
	}
	if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); !errors.Is(err, controlstore.ErrNotReady) {
		t.Fatalf("PreparePromptAttemptReclamation() error = %v, want ErrNotReady", err)
	}
	assertPromptAttemptReclamationMarker(t, ctx, kubeClient, taskUID, false)
	assertPromptAttemptReclamationCompleted(t, ctx, kubeClient, request, false)
}

func TestCreatePromptAttemptRejectsDeletingOrMissingTaskOwner(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, fence := newPromptAttemptReclaimStore(t)
	const taskUID = "task-reclaim-late-create"
	createDeletingAgentTask(t, ctx, kubeClient, "tenant-a", taskUID, taskUID)
	attempt := boundPromptAttemptForKubeTest(&controlstore.PromptAttempt{
		Key:           controlstore.PromptAttemptKey{Namespace: "tenant-a", TaskUID: taskUID, Attempt: 1, PromptID: "late"},
		RequestDigest: testDigest("late-create"),
	})
	if _, err := kubeStore.CreatePromptAttempt(ctx, attempt, fence); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("CreatePromptAttempt(deleting Task) error = %v, want ErrConflict", err)
	}

	missing := *attempt
	missing.Key.TaskUID = "task-reclaim-missing-owner"
	missing.Key.PromptID = "missing-owner"
	missing.ID = ""
	missing.RequestDigest = testDigest("missing-owner")
	if _, err := kubeStore.CreatePromptAttempt(ctx, &missing, fence); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("CreatePromptAttempt(missing Task) error = %v, want ErrConflict", err)
	}
}

func createDeletingTaskForPromptAttempt(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	attempt *controlstore.PromptAttempt,
) string {
	t.Helper()
	name := attempt.Key.TaskUID
	createDeletingAgentTask(t, ctx, kubeClient, attempt.Key.Namespace, name, attempt.Key.TaskUID)
	return name
}

func createDeletingAgentTask(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	namespace, name, uid string,
) {
	t.Helper()
	task := ensureActiveAgentTask(t, ctx, kubeClient, namespace, name, uid)
	if len(task.Finalizers) == 0 {
		task.Finalizers = []string{"reclamation-test"}
		if err := kubeClient.Update(ctx, task); err != nil {
			t.Fatalf("add deleting Task finalizer: %v", err)
		}
	}
	if err := kubeClient.Delete(ctx, task); err != nil {
		t.Fatalf("mark Task deleting: %v", err)
	}
}

type restoredPromptAttemptTerminalStatus struct {
	phase     corev1alpha1.TaskPhase
	execution corev1alpha1.TaskExecutionState
	outcome   corev1alpha1.TaskExecutionOutcome
	delivery  controlstore.PromptDeliveryState
}

func setRestoredPromptAttemptOwner(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	taskName string,
	attempt *controlstore.PromptAttempt,
	terminal restoredPromptAttemptTerminalStatus,
) *corev1alpha1.Task {
	t.Helper()
	task := &corev1alpha1.Task{}
	key := client.ObjectKey{Namespace: attempt.Key.Namespace, Name: taskName}
	if err := kubeClient.Get(ctx, key, task); err != nil {
		t.Fatalf("get source Task for restore: %v", err)
	}
	if err := kubeClient.Delete(ctx, task); err != nil {
		t.Fatalf("remove source Task fixture: %v", err)
	}
	restored := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: task.Namespace, Name: task.Name, UID: types.UID("task-restored-target"),
			Finalizers: []string{"reclamation-test"},
		},
		Spec: task.Spec,
	}
	if err := kubeClient.Create(ctx, restored); err != nil {
		t.Fatalf("create restored Task fixture: %v", err)
	}
	restored.Status.Phase = terminal.phase
	restored.Status.Execution = &corev1alpha1.TaskExecutionStatus{
		State: terminal.execution, Outcome: terminal.outcome,
		Reason: restoredTaskIdentityChangedReason, Attempt: int32(attempt.Key.Attempt), PromptID: attempt.Key.PromptID,
		RequestDigest: attempt.RequestDigest,
	}
	restored.Status.Delivery = &corev1alpha1.TaskDeliveryStatus{
		State:   corev1alpha1.TaskDeliveryState(terminal.delivery),
		Outcome: corev1alpha1.TaskDeliveryOutcome(terminal.delivery),
	}
	restored.Status.AgentExecutionBinding = &corev1alpha1.AgentExecutionBinding{
		SchemaVersion: 1, ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
		BindingDigest: attempt.BindingDigest,
		Task:          corev1alpha1.AgentExecutionBindingTaskRef{UID: types.UID(attempt.Key.TaskUID)},
		Snapshot: corev1alpha1.AgentExecutionSnapshotRef{
			ID:            (controlstore.AgentExecutionSnapshotKey{TaskUID: attempt.Key.TaskUID, Digest: attempt.SnapshotDigest}).ID(),
			Digest:        attempt.SnapshotDigest,
			SchemaVersion: controlstore.AgentExecutionSnapshotSchemaVersion,
		},
	}
	if err := kubeClient.Status().Update(ctx, restored); err != nil {
		t.Fatalf("record restored Task status: %v", err)
	}
	if err := kubeClient.Delete(ctx, restored); err != nil {
		t.Fatalf("mark restored Task deleting: %v", err)
	}
	if err := kubeClient.Get(ctx, key, restored); err != nil {
		t.Fatalf("refresh restored deleting Task: %v", err)
	}
	return restored
}

func assertPromptAttemptReclamationMarker(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	taskUID string,
	want bool,
) {
	t.Helper()
	marker := &corev1.ConfigMap{}
	err := kubeClient.Get(ctx, client.ObjectKey{
		Namespace: testControlNamespace,
		Name:      promptAttemptReclamationMarkerName(taskUID),
	}, marker)
	if want && err != nil {
		t.Fatalf("get prompt attempt reclamation marker: %v", err)
	}
	if !want && !apierrors.IsNotFound(err) {
		t.Fatalf("prompt attempt reclamation marker still exists or lookup failed: %v", err)
	}
}

func assertPromptAttemptReclamationCompleted(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	request controlstore.ReclaimPromptAttemptsRequest,
	want bool,
) {
	t.Helper()
	task := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Namespace: request.Namespace, Name: request.TaskName}, task); err != nil {
		t.Fatalf("get Task reclamation completion receipt: %v", err)
	}
	condition := meta.FindStatusCondition(task.Status.Conditions, promptAttemptReclamationCompleteCondition)
	if !want {
		if condition != nil {
			t.Fatalf("unexpected PromptAttempt reclamation completion condition: %#v", condition)
		}
		return
	}
	digest, err := promptAttemptReclamationCompletionDigest(request)
	if err != nil {
		t.Fatalf("promptAttemptReclamationCompletionDigest(): %v", err)
	}
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != promptAttemptReclamationCompleteReason || condition.Message != digest {
		t.Fatalf("PromptAttempt reclamation completion condition = %#v, want digest %q", condition, digest)
	}
}

func newPromptAttemptReclaimStore(t *testing.T) (*Store, client.Client, controlstore.ControllerEpochFence) {
	t.Helper()
	_, kubeClient, fence := newTestStoreWithEpoch(t)
	db, err := sqlitestore.NewDB(filepath.Join(t.TempDir(), "prompt-attempt-reclaim.db"))
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	kubeStore, err := NewComposite(kubeClient, testControlNamespace, sqlitestore.NewStore(db, ""))
	if err != nil {
		t.Fatalf("NewComposite: %v", err)
	}
	return kubeStore, kubeClient, fence
}

func createFailedPromptAttempt(
	t *testing.T,
	ctx context.Context,
	kubeStore *Store,
	fence controlstore.ControllerEpochFence,
	taskUID string,
	attemptNumber int64,
	promptID string,
) *controlstore.PromptAttempt {
	return createTerminalPromptAttempt(t, ctx, kubeStore, fence, taskUID, attemptNumber, promptID,
		controlstore.PromptExecutionFailed, controlstore.PromptDeliveryNotRequested)
}

func createTerminalPromptAttempt(
	t *testing.T,
	ctx context.Context,
	kubeStore *Store,
	fence controlstore.ControllerEpochFence,
	taskUID string,
	attemptNumber int64,
	promptID string,
	executionState controlstore.PromptExecutionState,
	deliveryState controlstore.PromptDeliveryState,
) *controlstore.PromptAttempt {
	t.Helper()
	ensureActiveAgentTask(t, ctx, kubeStore.client, "tenant-a", taskUID, taskUID)
	attempt, err := kubeStore.CreatePromptAttempt(ctx, boundPromptAttemptForKubeTest(&controlstore.PromptAttempt{
		Key: controlstore.PromptAttemptKey{
			Namespace: "tenant-a", TaskUID: taskUID, Attempt: attemptNumber, PromptID: promptID,
		},
		RequestDigest: testDigest(taskUID + ":" + promptID),
	}), fence)
	if err != nil {
		t.Fatalf("CreatePromptAttempt: %v", err)
	}
	executionPath := []controlstore.PromptExecutionState{executionState}
	switch executionState {
	case controlstore.PromptExecutionSucceeded:
		executionPath = []controlstore.PromptExecutionState{
			controlstore.PromptExecutionReserved, controlstore.PromptExecutionSessionStarting,
			controlstore.PromptExecutionPlanned, controlstore.PromptExecutionSubmitting,
			controlstore.PromptExecutionAccepted, controlstore.PromptExecutionRunning,
			controlstore.PromptExecutionSettling, controlstore.PromptExecutionSucceeded,
		}
	case controlstore.PromptExecutionOutcomeUnknown:
		executionPath = []controlstore.PromptExecutionState{
			controlstore.PromptExecutionReserved, controlstore.PromptExecutionSessionStarting,
			controlstore.PromptExecutionPlanned, controlstore.PromptExecutionSubmitting,
			controlstore.PromptExecutionAccepted, controlstore.PromptExecutionOutcomeUnknown,
		}
	case controlstore.PromptExecutionFailed, controlstore.PromptExecutionCancelled:
	default:
		t.Fatalf("unsupported terminal execution fixture state %q", executionState)
	}
	for i, next := range executionPath {
		terminalReason := ""
		outcomeMarker := ""
		if controlstore.IsTerminalPromptExecutionState(next) {
			terminalReason = "test terminal " + string(executionState)
		}
		if next == controlstore.PromptExecutionOutcomeUnknown {
			outcomeMarker = "test outcome unknown"
		}
		attempt, err = kubeStore.TransitionPromptAttemptExecution(ctx, controlstore.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: next, OperationID: fmt.Sprintf("terminal-%s-%d", promptID, i),
			OperationDigest: testDigest(fmt.Sprintf("terminal-%s-%s-%d", taskUID, promptID, i)),
			TerminalReason:  terminalReason, OutcomeMarker: outcomeMarker,
			UpdatedAt: testNow.Add(time.Duration(i+1) * time.Minute),
		})
		if err != nil {
			t.Fatalf("TransitionPromptAttemptExecution(%s): %v", next, err)
		}
	}
	deliveryPath := []controlstore.PromptDeliveryState{}
	switch deliveryState {
	case controlstore.PromptDeliveryNotRequested:
	case controlstore.PromptDeliveryVerifiedExact:
		deliveryPath = []controlstore.PromptDeliveryState{
			controlstore.PromptDeliveryValidating, controlstore.PromptDeliveryPreparing,
			controlstore.PromptDeliveryPrepared, controlstore.PromptDeliveryPublishing,
			controlstore.PromptDeliveryVerifying, controlstore.PromptDeliveryVerifiedExact,
		}
	case controlstore.PromptDeliveryCancelledBeforePublish:
		deliveryPath = []controlstore.PromptDeliveryState{
			controlstore.PromptDeliveryValidating, controlstore.PromptDeliveryPreparing,
			controlstore.PromptDeliveryPrepared, controlstore.PromptDeliveryCancelledBeforePublish,
		}
	case controlstore.PromptDeliveryConflict:
		deliveryPath = []controlstore.PromptDeliveryState{controlstore.PromptDeliveryValidating, controlstore.PromptDeliveryConflict}
	default:
		t.Fatalf("unsupported terminal delivery fixture state %q", deliveryState)
	}
	for i, next := range deliveryPath {
		attempt, err = kubeStore.TransitionPromptAttemptDelivery(ctx, controlstore.PromptAttemptDeliveryTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.DeliveryState,
			NewState: next, OperationID: fmt.Sprintf("delivery-%s-%d", promptID, i),
			OperationDigest: testDigest(fmt.Sprintf("delivery-%s-%s-%d", taskUID, promptID, i)),
			UpdatedAt:       testNow.Add(time.Duration(len(executionPath)+i+1) * time.Minute),
		})
		if err != nil {
			t.Fatalf("TransitionPromptAttemptDelivery(%s): %v", next, err)
		}
	}
	return attempt
}

func enqueueTaskTerminalProjection(
	t *testing.T,
	ctx context.Context,
	kubeStore *Store,
	fence controlstore.ControllerEpochFence,
	attempt *controlstore.PromptAttempt,
	delivered bool,
) string {
	t.Helper()
	task := &corev1alpha1.Task{}
	taskKey := client.ObjectKey{Namespace: attempt.Key.Namespace, Name: attempt.Key.TaskUID}
	if err := kubeStore.client.Get(ctx, taskKey, task); err != nil {
		t.Fatalf("get terminal projection Task: %v", err)
	}
	executionState, executionOutcome, ok := terminalPromptAttemptExecutionForTest(attempt.ExecutionState)
	if !ok {
		t.Fatalf("unsupported terminal execution fixture state %q", attempt.ExecutionState)
	}
	phase := terminalPromptAttemptPhaseForTest(executionState, attempt.DeliveryState)
	task.Status.Phase = phase
	task.Status.Execution = &corev1alpha1.TaskExecutionStatus{
		State: executionState, Outcome: executionOutcome, Attempt: int32(attempt.Key.Attempt),
		PromptID: attempt.Key.PromptID, RequestDigest: attempt.RequestDigest, ControllerEpoch: attempt.ControllerEpoch,
	}
	task.Status.Delivery = &corev1alpha1.TaskDeliveryStatus{
		State: corev1alpha1.TaskDeliveryState(attempt.DeliveryState), Outcome: corev1alpha1.TaskDeliveryOutcome(attempt.DeliveryState),
	}
	if err := kubeStore.client.Status().Update(ctx, task); err != nil {
		t.Fatalf("record terminal projection Task status: %v", err)
	}
	payload, err := json.Marshal(taskterminal.Projection{
		Namespace: attempt.Key.Namespace, Task: task.Name, TaskUID: attempt.Key.TaskUID,
		Attempt: int32(attempt.Key.Attempt), Phase: phase, Execution: *task.Status.Execution.DeepCopy(),
		Delivery: task.Status.Delivery.DeepCopy(),
	})
	if err != nil {
		t.Fatalf("encode terminal projection payload: %v", err)
	}
	return enqueueRawTaskTerminalProjection(t, ctx, kubeStore, fence, attempt, payload, delivered)
}

func enqueueRawTaskTerminalProjection(
	t *testing.T,
	ctx context.Context,
	kubeStore *Store,
	fence controlstore.ControllerEpochFence,
	attempt *controlstore.PromptAttempt,
	payload []byte,
	delivered bool,
) string {
	t.Helper()
	projectionID := controlstore.CanonicalControlID(
		"task-terminal-projection", attempt.Key.Namespace, attempt.Key.TaskUID, fmt.Sprint(attempt.Key.Attempt),
	)
	projection, err := kubeStore.EnqueueOutboxProjection(ctx, &controlstore.OutboxProjection{
		ID: projectionID, AggregateKind: "Task", AggregateID: attempt.Key.TaskUID,
		ProjectionKind: "TaskTerminalStatus", PayloadDigest: testBytesDigest(payload), Payload: payload,
		AvailableAt: testNow, CreatedAt: testNow,
	}, fence)
	if err != nil {
		t.Fatalf("EnqueueOutboxProjection: %v", err)
	}
	if !delivered {
		return projectionID
	}
	claims, err := kubeStore.ClaimOutboxProjections(ctx, controlstore.ClaimOutboxProjectionsRequest{
		Fence: fence, WorkerID: "reclaim-test", Limit: 1, LeaseDuration: time.Minute, Now: testNow.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimOutboxProjections: %v", err)
	}
	if len(claims) != 1 || claims[0].ID != projection.ID {
		t.Fatalf("claimed projections = %#v, want %q", claims, projection.ID)
	}
	claimed := claims[0]
	if _, err := kubeStore.CompleteOutboxProjection(ctx, controlstore.CompleteOutboxProjectionRequest{
		ID: claimed.ID, Fence: fence, ExpectedVersion: claimed.Version, LeaseOwner: claimed.LeaseOwner,
		OperationID: "deliver-" + attempt.Key.PromptID, OperationDigest: testDigest("deliver-" + attempt.Key.PromptID),
		NewState: controlstore.OutboxProjectionDelivered, DeliveryDigest: testDigest("delivered-" + attempt.Key.PromptID),
		UpdatedAt: testNow.Add(2 * time.Minute),
	}); err != nil {
		t.Fatalf("CompleteOutboxProjection(Delivered): %v", err)
	}
	return projectionID
}

func terminalPromptAttemptExecutionForTest(state controlstore.PromptExecutionState) (
	corev1alpha1.TaskExecutionState,
	corev1alpha1.TaskExecutionOutcome,
	bool,
) {
	switch state {
	case controlstore.PromptExecutionSucceeded:
		return corev1alpha1.TaskExecutionStateSucceeded, corev1alpha1.TaskExecutionOutcomeSucceeded, true
	case controlstore.PromptExecutionFailed:
		return corev1alpha1.TaskExecutionStateFailed, corev1alpha1.TaskExecutionOutcomeFailed, true
	case controlstore.PromptExecutionCancelled:
		return corev1alpha1.TaskExecutionStateCancelled, corev1alpha1.TaskExecutionOutcomeCancelled, true
	case controlstore.PromptExecutionOutcomeUnknown:
		return corev1alpha1.TaskExecutionStateOutcomeUnknown, corev1alpha1.TaskExecutionOutcomeOutcomeUnknown, true
	default:
		return "", "", false
	}
}

func terminalPromptAttemptPhaseForTest(
	state corev1alpha1.TaskExecutionState,
	delivery controlstore.PromptDeliveryState,
) corev1alpha1.TaskPhase {
	if state == corev1alpha1.TaskExecutionStateCancelled {
		return corev1alpha1.TaskPhaseCancelled
	}
	if state == corev1alpha1.TaskExecutionStateSucceeded {
		switch delivery {
		case controlstore.PromptDeliveryNotRequested, controlstore.PromptDeliveryReadValidated,
			controlstore.PromptDeliveryNoChange, controlstore.PromptDeliveryVerifiedExact,
			controlstore.PromptDeliveryDeliveredSuperseded:
			return corev1alpha1.TaskPhaseSucceeded
		case controlstore.PromptDeliveryCancelledBeforePublish:
			return corev1alpha1.TaskPhaseCancelled
		}
	}
	return corev1alpha1.TaskPhaseFailed
}
