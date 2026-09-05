package controller

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/taskterminal"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSessionRuntimeCleanupRetiresFinalizedOutcomeUnknown(t *testing.T) {
	for _, continued := range []bool{false, true} {
		t.Run(fmt.Sprintf("continued=%t", continued), func(t *testing.T) {
			var prompts atomic.Int32
			fixture := newExternalACPDispatchFixtureWithOptions(t, "unknown-cleanup", testAgentRuntimeMCPPolicy(), externalACPDispatchFixtureOptions{
				terminalEvents: map[harnessv2.PromptID]harnessv2.EventType{
					"prompt-cleanup-outcome-2-uid-1": harnessv2.EventOutcomeUnknown,
				},
				promptObserver: func(harnessv2.StartPromptRequest) { prompts.Add(1) },
			})
			turnCount := 2
			if continued {
				turnCount++
			}
			tasks := make([]*corev1alpha1.Task, 0, turnCount)
			for i := range turnCount {
				name := fmt.Sprintf("cleanup-outcome-%d", i+1)
				queued := fixture.queueTask(t, name, types.UID(name+"-uid"), name, &corev1alpha1.SessionReference{
					Name: "cleanup-conversation", Create: i == 0, Append: true,
				})
				completed := fixture.dispatch(t, queued)
				if completed.Status.Execution.State == corev1alpha1.TaskExecutionStateReserved &&
					completed.Status.Execution.Reason == corev1alpha1.TaskExecutionReasonAtCapacity {
					completed = fixture.dispatch(t, completed)
				}
				want := corev1alpha1.TaskExecutionOutcomeSucceeded
				if i == 1 {
					want = corev1alpha1.TaskExecutionOutcomeOutcomeUnknown
				}
				if completed.Status.Execution == nil || completed.Status.Execution.Outcome != want {
					t.Fatalf("turn %d outcome = %#v, want %s", i, completed.Status.Execution, want)
				}
				tasks = append(tasks, completed)
			}
			projector := &ACPOutboxProjector{Client: fixture.client, Store: fixture.controlStore, Epochs: fixture.epochs, WorkerID: "unknown-cleanup-test"}
			if err := projector.projectOnce(fixture.ctx); err != nil {
				t.Fatal(err)
			}
			transcript, err := fixture.persistence.LoadTranscript(fixture.ctx, defaultNS, "cleanup-conversation", 0)
			if err != nil || len(transcript) != 2*turnCount || transcript[3].Role != "system" {
				t.Fatalf("canonical unknown turn = %#v, err=%v", transcript, err)
			}
			var marker acpOutcomeUnknownMarker
			if err := json.Unmarshal([]byte(transcript[3].Content), &marker); err != nil || marker.Kind != "OutcomeUnknown" || marker.AssistantResultRecorded {
				t.Fatalf("canonical outcome marker = %#v, err=%v", marker, err)
			}
			cleanup := sessionRuntimeCleanupStore(t, fixture, fixture.dispatcher.CleanupSessionRuntime)
			fixture.reconciler.DurableControlStore = cleanup
			control, err := cleanup.GetSessionControl(fixture.ctx, defaultNS, "cleanup-conversation")
			if err != nil || control.Availability != store.SessionAvailable || control.Lease != nil {
				t.Fatalf("finalized unknown Session = %#v, err=%v", control, err)
			}
			manager := NewSessionManager(fixture.persistence)
			manager.SetACPSessionCleanup(cleanup, fixture.epochs)
			deletesBefore := fixture.deleteCalls.Load()
			if continued {
				assertUnknownCleanupRequiresOriginalRuntime(t, fixture, manager, tasks)
			}
			if err := manager.DeleteSession(fixture.ctx, defaultNS, "cleanup-conversation"); err != nil {
				t.Fatalf("DeleteSession after finalized unknown turn: %v", err)
			}
			if fixture.deleteCalls.Load() != deletesBefore+1 || prompts.Load() != int32(turnCount) {
				t.Fatalf("cleanup calls = deletes:%d prompts:%d, want %d/%d", fixture.deleteCalls.Load(), prompts.Load(), deletesBefore+1, turnCount)
			}
			assertSessionRuntimeCleanupCompleted(t, fixture, cleanup, tasks)
			attemptID, err := promptAttemptIDFromTask(tasks[1])
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := fixture.persistence.GetSessionTurnCleanupReceipt(fixture.ctx, defaultNS, "cleanup-conversation", attemptID)
			if err != nil {
				t.Fatal(err)
			}
			var projection taskterminal.Projection
			if err := json.Unmarshal(receipt.Payload, &projection); err != nil || receipt.TerminalKind != store.SessionTurnOutcomeMarker ||
				projection.Execution.State != corev1alpha1.TaskExecutionStateOutcomeUnknown ||
				projection.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeOutcomeUnknown {
				t.Fatalf("cleanup lost the unknown terminal outcome: %#v, err=%v", projection.Execution, err)
			}
		})
	}
}

func assertUnknownCleanupRequiresOriginalRuntime(t *testing.T, fixture *externalACPDispatchFixture, manager *SessionManager, tasks []*corev1alpha1.Task) {
	t.Helper()
	// A replacement boot cannot certify the old boot's retirement,
	// even after the canonical conversation has continued.
	deletesBefore := fixture.deleteCalls.Load()
	runtime := fixture.runtime.DeepCopy()
	runtime.Status.ObservedCapabilities.SupervisorBootID = "replacement-boot"
	if err := fixture.client.Status().Update(fixture.ctx, runtime); err != nil {
		t.Fatal(err)
	}
	if err := manager.DeleteSession(fixture.ctx, defaultNS, "cleanup-conversation"); err == nil {
		t.Fatal("unknown turn cleanup accepted replacement runtime authority")
	}
	if fixture.deleteCalls.Load() != deletesBefore {
		t.Fatal("blocked cleanup mutated the runtime")
	}
	if _, err := fixture.persistence.GetSession(fixture.ctx, defaultNS, "cleanup-conversation"); err != nil {
		t.Fatalf("blocked cleanup removed the canonical transcript: %v", err)
	}
	for _, task := range tasks {
		current := &corev1alpha1.Task{}
		if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
			t.Fatal(err)
		}
		if runtimeSessionCleanupCompleteForUID(current, current.UID) {
			t.Fatal("blocked cleanup minted a runtime retirement receipt")
		}
	}
	runtime.Status.ObservedCapabilities.SupervisorBootID = fixture.runtime.Status.ObservedCapabilities.SupervisorBootID
	if err := fixture.client.Status().Update(fixture.ctx, runtime); err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeCleanupRejectsUnknownV1WithoutControlOrTask(t *testing.T) {
	fixture, tasks := newContinuedSessionCleanupFixture(t)
	turn, projection := sessionRuntimeCleanupTurnProjection(t, fixture, tasks[0])
	body, err := json.Marshal(taskterminal.Projection{
		Namespace: defaultNS, Task: "missing-v1-task", TaskUID: turn.Key.TaskUID, Attempt: int32(turn.Key.Attempt),
		Phase: corev1alpha1.TaskPhaseFailed,
		HarnessRuntime: &corev1alpha1.HarnessRuntimeStatus{
			Attempt: int32(turn.Key.Attempt), State: corev1alpha1.TaskExecutionStateOutcomeUnknown,
			Outcome: corev1alpha1.TaskExecutionOutcomeOutcomeUnknown,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	projection.Payload, projection.PayloadDigest = body, store.CanonicalBytesDigest(body)
	turn.TerminalKind, turn.ProjectionDigest = store.SessionTurnOutcomeMarker, projection.PayloadDigest
	dispatcher := &ACPDispatcher{
		Store: &sessionRuntimeCleanupProjectionStore{DurableControlStore: fixture.controlStore, projection: projection},
	}
	intent := store.SessionCleanupIntent{Namespace: defaultNS, SessionName: "cleanup-conversation", SessionUID: turn.Key.SessionUID}
	if target, err := dispatcher.sessionRuntimeCleanupTarget(fixture.ctx, intent, turn); err == nil || target != nil {
		t.Fatalf("orphaned v1 unknown outcome lost its reconciliation barrier: target:%v err:%v", target, err)
	}
}
