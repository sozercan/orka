package kube

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	controlstore "github.com/orka-agents/orka/internal/store"
	sqlitestore "github.com/orka-agents/orka/internal/store/sqlite"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSessionTurnCleanupReceiptAllowsTaskReclamationAfterSessionDeletion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		legacy   bool
		prepared bool
	}{
		{name: "current projection"},
		{name: "prepared attempt deletion", prepared: true},
		{name: "legacy prepared attempt deletion", legacy: true, prepared: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			kubeStore, kubeClient, persistence, db, fence := newSessionCleanupTestStore(t, nil)
			fixture := seedFinalizedSessionReclaimTurn(t, ctx, kubeStore, kubeClient, fence, tc.legacy)
			if fixture.turn.Key.LeaseGeneration == fixture.task.Status.Execution.RuntimeSessionGeneration {
				t.Fatal("fixture must exercise different mutation lease and runtime incarnation generations")
			}
			if tc.prepared {
				createDeletingAgentTask(t, ctx, kubeClient, fixture.task.Namespace, fixture.task.Name, string(fixture.task.UID))
				if err := kubeStore.PreparePromptAttemptReclamation(ctx, fixture.request); err != nil {
					t.Fatalf("prepare before Session deletion: %v", err)
				}
			}
			deleteRequest := sessionTurnCleanupRequest(fixture, fence)
			if err := kubeStore.ReclaimSession(ctx, deleteRequest); err != nil {
				t.Fatalf("delete Session: %v", err)
			}
			assertSessionTurnCleanupOrdinaryStateAbsent(t, ctx, persistence, fixture)
			receipt, err := kubeStore.GetSessionTurnCleanupReceipt(ctx, fixture.task.Namespace, fixture.task.Spec.SessionRef.Name, fixture.attempt.ID)
			if err != nil {
				t.Fatalf("read cleanup receipt: %v", err)
			}
			assertSessionTurnCleanupReceiptEvidence(t, ctx, db, receipt, fixture)
			if !tc.prepared {
				createDeletingAgentTask(t, ctx, kubeClient, fixture.task.Namespace, fixture.task.Name, string(fixture.task.UID))
			} else {
				attemptObject := &corev1alpha1.PromptAttempt{}
				if err := kubeClient.Get(ctx, client.ObjectKey{Namespace: fixture.task.Namespace, Name: objectName(promptAttemptNamePrefix, fixture.attempt.ID)}, attemptObject); err != nil {
					t.Fatal(err)
				}
				if err := kubeClient.Delete(ctx, attemptObject); err != nil {
					t.Fatal(err)
				}
			}

			// Reopen the on-disk database and reconstruct the composite store,
			// preserving only authoritative Kubernetes resources across restart.
			var sequence int
			var databaseName, databasePath string
			if err := db.QueryRowContext(ctx, "PRAGMA database_list").Scan(&sequence, &databaseName, &databasePath); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := sqlitestore.NewDB(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			persistence = sqlitestore.NewStore(reopened, databasePath)
			kubeStore, err = NewComposite(kubeClient, testControlNamespace, persistence)
			if err != nil {
				t.Fatal(err)
			}
			if err := kubeStore.ReclaimSession(ctx, deleteRequest); err != nil {
				t.Fatalf("repeat Session deletion after restart: %v", err)
			}
			if err := kubeStore.PreparePromptAttemptReclamation(ctx, fixture.request); err != nil {
				t.Fatalf("prepare Task reclamation after Session deletion/restart: %v", err)
			}
			deleted, err := kubeStore.ReclaimPromptAttempts(ctx, fixture.request)
			wantDeleted := 1
			if tc.prepared {
				wantDeleted = 0
			}
			if err != nil || deleted != wantDeleted {
				t.Fatalf("Task reclamation after restart = %d, %v; want %d, nil", deleted, err, wantDeleted)
			}
			assertPromptAttemptReclamationMarker(t, ctx, kubeClient, fixture.request.TaskUID, false)
			assertPromptAttemptReclamationCompleted(t, ctx, kubeClient, fixture.request, true)
			if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, fixture.request); err != nil || deleted != 0 {
				t.Fatalf("idempotent Task reclamation = %d, %v", deleted, err)
			}
			assertSessionTurnCleanupOrdinaryStateAbsent(t, ctx, persistence, fixture)
		})
	}
}

func TestSessionTurnCleanupReceiptRejectsInvalidReclamationEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, context.Context, *Store, client.Client, *sql.DB, finalizedSessionReclaimFixture)
	}{
		{name: "receipt absent", mutate: func(t *testing.T, ctx context.Context, _ *Store, _ client.Client, db *sql.DB, f finalizedSessionReclaimFixture) {
			execSessionCleanupMutation(t, ctx, db, `DELETE FROM session_turn_cleanup_receipts WHERE turn_id = ?`, f.turn.ID)
		}},
		{name: "tombstone absent", mutate: func(t *testing.T, ctx context.Context, _ *Store, _ client.Client, db *sql.DB, f finalizedSessionReclaimFixture) {
			execSessionCleanupMutation(t, ctx, db, `DELETE FROM session_cleanup_completions WHERE namespace = ? AND session_name = ?`, f.task.Namespace, f.task.Spec.SessionRef.Name)
		}},
		{name: "tombstone Session UID differs", mutate: func(t *testing.T, ctx context.Context, _ *Store, _ client.Client, db *sql.DB, f finalizedSessionReclaimFixture) {
			execSessionCleanupMutation(t, ctx, db, `UPDATE session_cleanup_completions SET session_uid = 'replacement-session' WHERE namespace = ? AND session_name = ?`, f.task.Namespace, f.task.Spec.SessionRef.Name)
		}},
		{name: "tombstone operation differs", mutate: func(t *testing.T, ctx context.Context, _ *Store, _ client.Client, db *sql.DB, f finalizedSessionReclaimFixture) {
			execSessionCleanupMutation(t, ctx, db, `UPDATE session_cleanup_completions SET operation_digest = ? WHERE namespace = ? AND session_name = ?`, testDigest("different-operation"), f.task.Namespace, f.task.Spec.SessionRef.Name)
		}},
		{name: "archive digest differs", mutate: func(t *testing.T, ctx context.Context, _ *Store, _ client.Client, db *sql.DB, f finalizedSessionReclaimFixture) {
			execSessionCleanupMutation(t, ctx, db, `UPDATE session_turn_cleanup_receipts SET receipt_digest = ? WHERE turn_id = ?`, testDigest("different-receipt"), f.turn.ID)
		}},
		{name: "payload digest differs", mutate: func(t *testing.T, ctx context.Context, _ *Store, _ client.Client, db *sql.DB, f finalizedSessionReclaimFixture) {
			mutateSessionTurnCleanupReceipt(t, ctx, db, f.turn.ID, func(r *controlstore.SessionTurnCleanupReceipt) { r.Payload = []byte(`{}`) })
		}},
		{name: "finalization digest differs", mutate: func(t *testing.T, ctx context.Context, _ *Store, _ client.Client, db *sql.DB, f finalizedSessionReclaimFixture) {
			mutateSessionTurnCleanupReceipt(t, ctx, db, f.turn.ID, func(r *controlstore.SessionTurnCleanupReceipt) { r.ProjectionDigest = testDigest("other-projection") })
		}},
		{name: "dead letter is not delivery proof", mutate: func(t *testing.T, ctx context.Context, _ *Store, _ client.Client, db *sql.DB, f finalizedSessionReclaimFixture) {
			mutateSessionTurnCleanupReceipt(t, ctx, db, f.turn.ID, func(r *controlstore.SessionTurnCleanupReceipt) {
				r.ProjectionState, r.DeliveryDigest, r.DeliveredAt = controlstore.OutboxProjectionDeadLetter, "", nil
			})
		}},
		{name: "turn belongs to different Task", mutate: func(t *testing.T, ctx context.Context, _ *Store, _ client.Client, db *sql.DB, f finalizedSessionReclaimFixture) {
			mutateSessionTurnCleanupReceipt(t, ctx, db, f.turn.ID, func(r *controlstore.SessionTurnCleanupReceipt) { r.Key.TaskUID = "other-task" })
		}},
		{name: "turn lease differs", mutate: func(t *testing.T, ctx context.Context, _ *Store, _ client.Client, db *sql.DB, f finalizedSessionReclaimFixture) {
			mutateSessionTurnCleanupReceipt(t, ctx, db, f.turn.ID, func(r *controlstore.SessionTurnCleanupReceipt) { r.Key.LeaseGeneration++ })
		}},
		{name: "Task Session name differs", mutate: func(t *testing.T, ctx context.Context, _ *Store, c client.Client, _ *sql.DB, f finalizedSessionReclaimFixture) {
			task := f.task.DeepCopy()
			task.Spec.SessionRef.Name = "another-session"
			if err := c.Update(ctx, task); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "Task terminal delivery differs", mutate: func(t *testing.T, ctx context.Context, _ *Store, c client.Client, _ *sql.DB, f finalizedSessionReclaimFixture) {
			task := f.task.DeepCopy()
			task.Status.Delivery.StartingSHA = strings.Repeat("a", 40)
			if err := c.Status().Update(ctx, task); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "coordination Lease reappears", mutate: func(t *testing.T, ctx context.Context, _ *Store, c client.Client, _ *sql.DB, f finalizedSessionReclaimFixture) {
			if err := c.Create(ctx, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: f.task.Namespace, Name: runtimeSessionLeaseName(f.attempt.SessionUID)}}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "Session control reappears", mutate: func(t *testing.T, ctx context.Context, _ *Store, c client.Client, _ *sql.DB, f finalizedSessionReclaimFixture) {
			if err := c.Create(ctx, &corev1alpha1.RuntimeSessionControl{
				ObjectMeta: metav1.ObjectMeta{Namespace: f.task.Namespace, Name: runtimeSessionObjectName(f.task.Spec.SessionRef.Name)},
				Spec:       corev1alpha1.RuntimeSessionControlSpec{SessionName: f.task.Spec.SessionRef.Name, SessionUID: f.attempt.SessionUID},
			}); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			kubeStore, kubeClient, _, db, fence := newSessionCleanupTestStore(t, nil)
			fixture := seedFinalizedSessionReclaimTurn(t, ctx, kubeStore, kubeClient, fence, false)
			if err := kubeStore.ReclaimSession(ctx, sessionTurnCleanupRequest(fixture, fence)); err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, ctx, kubeStore, kubeClient, db, fixture)
			createDeletingAgentTask(t, ctx, kubeClient, fixture.task.Namespace, fixture.task.Name, string(fixture.task.UID))
			deleted, err := kubeStore.ReclaimPromptAttempts(ctx, fixture.request)
			if err == nil || deleted != 0 {
				t.Fatalf("reclamation with invalid evidence = %d, %v", deleted, err)
			}
			if _, err := kubeStore.GetPromptAttempt(ctx, fixture.attempt.ID); err != nil {
				t.Fatalf("invalid evidence removed the Task attempt: %v", err)
			}
			assertPromptAttemptReclamationCompleted(t, ctx, kubeClient, fixture.request, false)
		})
	}
}

func TestSessionTurnCleanupReceiptLookupAndProjectionAreExactlyBound(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, _, _, fence := newSessionCleanupTestStore(t, nil)
	fixture := seedFinalizedSessionReclaimTurn(t, ctx, kubeStore, kubeClient, fence, false)
	if err := kubeStore.ReclaimSession(ctx, sessionTurnCleanupRequest(fixture, fence)); err != nil {
		t.Fatal(err)
	}
	for _, request := range [][3]string{
		{"another-namespace", fixture.task.Spec.SessionRef.Name, fixture.attempt.ID},
		{fixture.task.Namespace, "another-session", fixture.attempt.ID},
		{fixture.task.Namespace, fixture.task.Spec.SessionRef.Name, "another-attempt"},
	} {
		if _, err := kubeStore.GetSessionTurnCleanupReceipt(ctx, request[0], request[1], request[2]); !errors.Is(err, controlstore.ErrNotFound) {
			t.Fatalf("lookup for another identity returned %v, want NotFound", err)
		}
	}
	createDeletingAgentTask(t, ctx, kubeClient, fixture.task.Namespace, fixture.task.Name, string(fixture.task.UID))
	request := fixture.request
	request.TerminalProjectionID = "nonexistent-projection"
	if err := kubeStore.PreparePromptAttemptReclamation(ctx, request); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("mismatched projection ID was not rejected: %v", err)
	}
	if err := WithWatchNamespace("other-namespace")(kubeStore); err != nil {
		t.Fatal(err)
	}
	if _, err := kubeStore.GetSessionTurnCleanupReceipt(ctx, fixture.task.Namespace, fixture.task.Spec.SessionRef.Name, fixture.attempt.ID); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("cross-watch namespace receipt was not rejected: %v", err)
	}
}

func TestSessionTurnCleanupRetainsEarlierAttemptAfterUnboundRetry(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, _, _, fence := newSessionCleanupTestStore(t, nil)
	fixture := seedFinalizedSessionReclaimTurn(t, ctx, kubeStore, kubeClient, fence, false)
	finalAttempt := createFailedPromptAttempt(t, ctx, kubeStore, fence, string(fixture.task.UID), 2, "unbound-retry")
	projectionID := enqueueTaskTerminalProjection(t, ctx, kubeStore, fence, finalAttempt, true)
	createDeletingAgentTask(t, ctx, kubeClient, fixture.task.Namespace, fixture.task.Name, string(fixture.task.UID))
	request := fixture.request
	request.FinalPromptAttemptID = finalAttempt.ID
	request.TerminalProjectionID = projectionID
	request.FinalContinuitySession = false
	if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request); !errors.Is(err, controlstore.ErrNotReady) || deleted != 0 {
		t.Fatalf("reclaim before earlier Session archival = %d, %v; want 0, NotReady", deleted, err)
	}
	for _, id := range []string{fixture.attempt.ID, finalAttempt.ID} {
		if _, err := kubeStore.GetPromptAttempt(ctx, id); err != nil {
			t.Fatalf("pending Session archival lost attempt %q: %v", id, err)
		}
	}
	if err := kubeStore.ReclaimSession(ctx, sessionTurnCleanupRequest(fixture, fence)); err != nil {
		t.Fatal(err)
	}
	if deleted, err := kubeStore.ReclaimPromptAttempts(ctx, request); err != nil || deleted != 2 {
		t.Fatalf("reclaim after earlier Session archival = %d, %v; want 2, nil", deleted, err)
	}
}

func TestSessionTurnCleanupMissingProjectionDoesNotDeleteEvidence(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, persistence, db, fence := newSessionCleanupTestStore(t, nil)
	fixture := seedFinalizedSessionReclaimTurn(t, ctx, kubeStore, kubeClient, fence, false)
	execSessionCleanupMutation(t, ctx, db, `DELETE FROM outbox_projections WHERE id = ?`, fixture.projection.ID)
	if err := kubeStore.ReclaimSession(ctx, sessionTurnCleanupRequest(fixture, fence)); err == nil {
		t.Fatal("Session cleanup accepted missing terminal projection")
	}
	if _, err := persistence.GetSession(ctx, fixture.task.Namespace, fixture.task.Spec.SessionRef.Name); err != nil {
		t.Fatalf("failed archival deleted transcript: %v", err)
	}
	if _, err := persistence.GetSessionTurn(ctx, fixture.turn.ID); err != nil {
		t.Fatalf("failed archival deleted finalized turn: %v", err)
	}
	if _, err := persistence.GetSessionCleanupCompletion(ctx, fixture.task.Namespace, fixture.task.Spec.SessionRef.Name); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("failed archival created a completion tombstone: %v", err)
	}
	var receipts int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM session_turn_cleanup_receipts`).Scan(&receipts); err != nil || receipts != 0 {
		t.Fatalf("failed archival committed %d receipts: %v", receipts, err)
	}
}

func TestSessionTurnCleanupReceiptAndDeletionCommitAtomically(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, persistence, db, fence := newSessionCleanupTestStore(t, nil)
	fixture := seedFinalizedSessionReclaimTurn(t, ctx, kubeStore, kubeClient, fence, false)
	// Abort after receipt insertion and outbox deletion, while the cleanup
	// transaction is deleting its turn. Neither earlier write may survive.
	execSessionCleanupMutation(t, ctx, db, `CREATE TRIGGER fail_session_cleanup BEFORE DELETE ON session_turns
		BEGIN SELECT RAISE(ABORT, 'injected Session cleanup failure'); END`)
	if err := kubeStore.ReclaimSession(ctx, sessionTurnCleanupRequest(fixture, fence)); err == nil {
		t.Fatal("cleanup ignored injected transaction failure")
	}
	if _, err := persistence.GetSession(ctx, fixture.task.Namespace, fixture.task.Spec.SessionRef.Name); err != nil {
		t.Fatalf("failed cleanup deleted Session transcript: %v", err)
	}
	if _, err := persistence.GetSessionTurn(ctx, fixture.turn.ID); err != nil {
		t.Fatalf("failed cleanup deleted finalized turn: %v", err)
	}
	if _, err := persistence.GetOutboxProjection(ctx, fixture.projection.ID); err != nil {
		t.Fatalf("failed cleanup deleted terminal projection: %v", err)
	}
	var receipts int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM session_turn_cleanup_receipts`).Scan(&receipts); err != nil || receipts != 0 {
		t.Fatalf("failed cleanup committed %d receipts: %v", receipts, err)
	}
	execSessionCleanupMutation(t, ctx, db, `DROP TRIGGER fail_session_cleanup`)
	if err := kubeStore.ResumeSessionCleanups(ctx, fence); err != nil {
		t.Fatalf("recover interrupted cleanup transaction: %v", err)
	}
	assertSessionTurnCleanupOrdinaryStateAbsent(t, ctx, persistence, fixture)
	if _, err := kubeStore.GetSessionTurnCleanupReceipt(ctx, fixture.task.Namespace, fixture.task.Spec.SessionRef.Name, fixture.attempt.ID); err != nil {
		t.Fatalf("recovered cleanup lacks its receipt: %v", err)
	}
}

func TestSessionRuntimeCleanupRunsUnderDurableIntentBeforeAuthorityDeletion(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, persistence, _, fence := newSessionCleanupTestStore(t, nil)
	fixture := seedFinalizedSessionReclaimTurn(t, ctx, kubeStore, kubeClient, fence, false)
	wantFailure := errors.New("runtime cleanup incomplete")
	calls := 0
	if err := WithSessionRuntimeCleanup(func(ctx context.Context, intent controlstore.SessionCleanupIntent, gotFence controlstore.ControllerEpochFence) error {
		calls++
		if gotFence != fence {
			t.Fatal("runtime cleanup received another controller fence")
		}
		if _, err := persistence.GetSessionCleanupIntent(ctx, intent.Namespace, intent.SessionName); err != nil {
			t.Fatal(err)
		}
		if _, err := kubeStore.GetSessionControl(ctx, intent.Namespace, intent.SessionName); err != nil {
			t.Fatalf("authority deleted before runtime cleanup: %v", err)
		}
		turns, err := persistence.ListSessionCleanupTurns(ctx, intent)
		if err != nil || len(turns) != 1 {
			t.Fatalf("cleanup turn metadata = %d, %v", len(turns), err)
		}
		if turns[0].Key != fixture.turn.Key || turns[0].UserPrompt != "" || turns[0].TerminalContent != "" {
			t.Fatal("cleanup turn metadata lost its key or retained transcript")
		}
		wrong := intent
		wrong.OperationDigest = testDigest("other-cleanup")
		if _, err := persistence.ListSessionCleanupTurns(ctx, wrong); !errors.Is(err, controlstore.ErrConflict) {
			t.Fatalf("mismatched cleanup intent was accepted: %v", err)
		}
		if calls == 1 {
			return wantFailure
		}
		return nil
	})(kubeStore); err != nil {
		t.Fatal(err)
	}
	if err := kubeStore.ReclaimSession(ctx, sessionTurnCleanupRequest(fixture, fence)); !errors.Is(err, wantFailure) {
		t.Fatalf("runtime cleanup failure was not propagated: %v", err)
	}
	if _, err := persistence.GetSession(ctx, fixture.task.Namespace, fixture.task.Spec.SessionRef.Name); err != nil {
		t.Fatal(err)
	}
	if err := kubeStore.ResumeSessionCleanups(ctx, fence); err != nil {
		t.Fatalf("resume runtime cleanup: %v", err)
	}
	if calls != 2 {
		t.Fatalf("runtime cleanup called %d times, want initial and recovery", calls)
	}
	assertSessionTurnCleanupOrdinaryStateAbsent(t, ctx, persistence, fixture)
}

func sessionTurnCleanupRequest(f finalizedSessionReclaimFixture, fence controlstore.ControllerEpochFence) controlstore.ReclaimSessionRequest {
	return controlstore.ReclaimSessionRequest{
		Namespace: f.task.Namespace, SessionName: f.task.Spec.SessionRef.Name, Fence: fence,
		OperationID: "delete-finalized-session", OperationDigest: testDigest("delete-finalized-session"), RequestedAt: testNow,
	}
}

func assertSessionTurnCleanupOrdinaryStateAbsent(t *testing.T, ctx context.Context, persistence *sqlitestore.Store, f finalizedSessionReclaimFixture) {
	t.Helper()
	if _, err := persistence.GetSession(ctx, f.task.Namespace, f.task.Spec.SessionRef.Name); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("ordinary Session read = %v", err)
	}
	if _, err := persistence.GetSessionTurn(ctx, f.turn.ID); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("ordinary SessionTurn read = %v", err)
	}
	if _, err := persistence.GetOutboxProjection(ctx, f.projection.ID); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("ordinary outbox read = %v", err)
	}
}

func assertSessionTurnCleanupReceiptEvidence(t *testing.T, ctx context.Context, db *sql.DB, receipt *controlstore.SessionTurnCleanupReceipt, f finalizedSessionReclaimFixture) {
	t.Helper()
	if receipt.Key != f.turn.Key || receipt.PromptAttemptID != f.attempt.ID ||
		!bytes.Equal(receipt.Payload, f.projection.Payload) || receipt.DeliveryDigest != f.projection.DeliveryDigest {
		t.Fatal("cleanup receipt lost exact finalization or projection evidence")
	}
	if receipt.SessionTurn().UserPrompt != "" || receipt.SessionTurn().TerminalContent != "" {
		t.Fatal("cleanup receipt retained prompt or transcript content")
	}
	var encoded []byte
	if err := db.QueryRowContext(ctx, `SELECT receipt FROM session_turn_cleanup_receipts WHERE turn_id = ?`, f.turn.ID).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	for _, privateContent := range []string{f.turn.UserPrompt, f.turn.TerminalContent, `"userPrompt"`, `"terminalContent"`} {
		if privateContent == "" || bytes.Contains(encoded, []byte(privateContent)) || bytes.Contains(receipt.Payload, []byte(privateContent)) {
			t.Fatal("receipt contains deleted Session content or fixture lacks private content")
		}
	}
}

func execSessionCleanupMutation(t *testing.T, ctx context.Context, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

func mutateSessionTurnCleanupReceipt(t *testing.T, ctx context.Context, db *sql.DB, turnID string, mutate func(*controlstore.SessionTurnCleanupReceipt)) {
	t.Helper()
	var encoded []byte
	if err := db.QueryRowContext(ctx, `SELECT receipt FROM session_turn_cleanup_receipts WHERE turn_id = ?`, turnID).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var receipt controlstore.SessionTurnCleanupReceipt
	if err := json.Unmarshal(encoded, &receipt); err != nil {
		t.Fatal(err)
	}
	mutate(&receipt)
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	execSessionCleanupMutation(t, ctx, db, `UPDATE session_turn_cleanup_receipts SET receipt = ?, receipt_digest = ? WHERE turn_id = ?`, encoded, controlstore.CanonicalBytesDigest(encoded), turnID)
}
