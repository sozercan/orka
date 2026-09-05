package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

const (
	controlTestGitSHA           = "0123456789012345678901234567890123456789"
	controlTestGitSHA2          = "abcdefabcdefabcdefabcdefabcdefabcdefabcd"
	controlTestSessionType      = "chat"
	controlTestControllerB      = "controller-b"
	controlTestAggregateKind    = "SessionTurn"
	controlTestProjectionKind   = "TaskStatus"
	controlTestPrepareOperation = "prepare-1"
	controlTestSessionName      = "session-1"
	controlTestSessionUID       = "session-uid-1"
	controlTestProjectorA       = "projector-a"
	controlTestProjectorB       = "projector-b"
	controlTestWorkerA          = "worker-a"
	controlTestUnknownSuffix    = "unknown"
)

func controlTestDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func boundPromptAttemptForSQLiteTest(attempt *store.PromptAttempt) *store.PromptAttempt {
	if attempt == nil {
		return nil
	}
	if attempt.BindingDigest == "" {
		attempt.BindingDigest = controlTestDigest("test-v2-binding")
	}
	if attempt.SnapshotDigest == "" {
		attempt.SnapshotDigest = controlTestDigest("test-v2-snapshot")
	}
	return attempt
}

func seedControlEpoch(t *testing.T, s *Store) store.ControllerEpochFence {
	t.Helper()
	epoch, err := s.CompareAndSwapControllerEpoch(context.Background(), store.ControllerEpochCAS{
		ExpectedVersion: 0,
		ExpectedEpoch:   0,
		NewEpoch:        1,
		HolderID:        "controller-a",
		RequestDigest:   controlTestDigest("epoch-1"),
		UpdatedAt:       time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("seed controller epoch: %v", err)
	}
	return store.ControllerEpochFence{Name: epoch.Name, Epoch: epoch.Epoch, HolderID: epoch.HolderID}
}

func createControlTranscriptSession(t *testing.T, s *Store, namespace, name string, now time.Time) {
	t.Helper()
	if err := s.CreateSession(context.Background(), &store.SessionRecord{
		Namespace: namespace, Name: name, SessionType: controlTestSessionType, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
}

func TestControllerEpochCASIdempotencyAndFencing(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	create := store.ControllerEpochCAS{
		ExpectedVersion: 0, ExpectedEpoch: 0, NewEpoch: 1, HolderID: "controller-a",
		RequestDigest: controlTestDigest("epoch-create"), UpdatedAt: now,
	}
	first, err := s.CompareAndSwapControllerEpoch(ctx, create)
	if err != nil {
		t.Fatalf("create epoch: %v", err)
	}
	if first.Version != 1 || first.Epoch != 1 || first.Name != store.DefaultControllerEpochName {
		t.Fatalf("created epoch = %#v", first)
	}
	retry, err := s.CompareAndSwapControllerEpoch(ctx, create)
	if err != nil {
		t.Fatalf("same-digest retry: %v", err)
	}
	if retry.Version != first.Version {
		t.Fatalf("retry advanced version: got %d want %d", retry.Version, first.Version)
	}
	mismatch := create
	mismatch.RequestDigest = controlTestDigest("different-create")
	if _, err := s.CompareAndSwapControllerEpoch(ctx, mismatch); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("same epoch different digest error = %v, want ErrConflict", err)
	}
	if _, err := s.CompareAndSwapControllerEpoch(ctx, store.ControllerEpochCAS{
		ExpectedVersion: 99, ExpectedEpoch: 1, NewEpoch: 2, HolderID: controlTestControllerB,
		RequestDigest: controlTestDigest("stale"), UpdatedAt: now.Add(time.Minute),
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale epoch error = %v, want ErrConflict", err)
	}
	if _, err := s.CompareAndSwapControllerEpoch(ctx, store.ControllerEpochCAS{
		Name: "alternate-controller-domain", ExpectedVersion: 0, ExpectedEpoch: 0, NewEpoch: 1,
		HolderID: "rogue-controller", RequestDigest: controlTestDigest("rogue-epoch"), UpdatedAt: now,
	}); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("alternate epoch domain error = %v, want ErrValidation", err)
	}
	second, err := s.CompareAndSwapControllerEpoch(ctx, store.ControllerEpochCAS{
		ExpectedVersion: first.Version, ExpectedEpoch: first.Epoch, NewEpoch: 2, HolderID: controlTestControllerB,
		RequestDigest: controlTestDigest("epoch-2"), UpdatedAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("advance epoch: %v", err)
	}
	if second.Version != 2 || second.Epoch != 2 || second.HolderID != "controller-b" {
		t.Fatalf("advanced epoch = %#v", second)
	}
}

func TestPromptAttemptSubmittedUnknownBecomesTerminalOutcomeUnknown(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	fence := seedControlEpoch(t, s)
	key := store.PromptAttemptKey{Namespace: "ns", TaskUID: "task-uid-1", Attempt: 1, PromptID: "prompt-1"}
	id, err := key.CanonicalID()
	if err != nil {
		t.Fatalf("canonical prompt attempt ID: %v", err)
	}
	created, err := s.CreatePromptAttempt(ctx, boundPromptAttemptForSQLiteTest(&store.PromptAttempt{
		Key: key, RequestDigest: controlTestDigest("prompt-request"),
	}), fence)
	if err != nil {
		t.Fatalf("CreatePromptAttempt: %v", err)
	}
	if created.ID != id || created.Version != 1 || created.ExecutionState != store.PromptExecutionQueued {
		t.Fatalf("created prompt attempt = %#v", created)
	}
	if created.BindingDigest != controlTestDigest("test-v2-binding") ||
		created.SnapshotDigest != controlTestDigest("test-v2-snapshot") {
		t.Fatalf("created prompt attempt binding = %q/%q", created.BindingDigest, created.SnapshotDigest)
	}
	retry, err := s.CreatePromptAttempt(ctx, boundPromptAttemptForSQLiteTest(&store.PromptAttempt{Key: key, RequestDigest: created.RequestDigest}), fence)
	if err != nil || retry.Version != created.Version {
		t.Fatalf("same-digest prompt retry = %#v, %v", retry, err)
	}
	if _, err := s.CreatePromptAttempt(ctx, boundPromptAttemptForSQLiteTest(&store.PromptAttempt{Key: key, RequestDigest: controlTestDigest("other-prompt")}), fence); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("prompt digest mismatch error = %v, want ErrConflict", err)
	}
	mismatchedBinding := boundPromptAttemptForSQLiteTest(&store.PromptAttempt{Key: key, RequestDigest: created.RequestDigest})
	mismatchedBinding.BindingDigest = controlTestDigest("different-binding")
	if _, err := s.CreatePromptAttempt(ctx, mismatchedBinding, fence); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("prompt binding digest mismatch error = %v, want ErrConflict", err)
	}
	unboundKey := store.PromptAttemptKey{Namespace: "ns", TaskUID: "task-uid-unbound", Attempt: 1, PromptID: "prompt-unbound"}
	if _, err := s.CreatePromptAttempt(ctx, &store.PromptAttempt{
		Key: unboundKey, RequestDigest: controlTestDigest("unbound-prompt"),
	}, fence); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("unbound prompt attempt error = %v, want ErrValidation", err)
	}

	current := created
	states := []store.PromptExecutionState{
		store.PromptExecutionReserved,
		store.PromptExecutionSessionStarting,
		store.PromptExecutionPlanned,
		store.PromptExecutionSubmitting,
		store.PromptExecutionSubmittedUnknown,
		store.PromptExecutionOutcomeUnknown,
	}
	for index, next := range states {
		transition := store.PromptAttemptExecutionTransition{
			ID: current.ID, Fence: fence, ExpectedVersion: current.Version,
			ExpectedState: current.ExecutionState, NewState: next,
			OperationID:     "execution-op-" + string(rune('a'+index)),
			OperationDigest: controlTestDigest("execution-op-" + string(rune('a'+index))),
			TerminalReason:  "runtime acceptance was ambiguous",
			UpdatedAt:       created.CreatedAt.Add(time.Duration(index+1) * time.Second),
		}
		if next == store.PromptExecutionOutcomeUnknown {
			transition.OutcomeMarker = "The prior prompt may have been accepted; it was not replayed."
		}
		current, err = s.TransitionPromptAttemptExecution(ctx, transition)
		if err != nil {
			t.Fatalf("transition to %s: %v", next, err)
		}
	}
	if current.ExecutionState != store.PromptExecutionOutcomeUnknown || current.OutcomeMarker == "" {
		t.Fatalf("terminal ambiguous attempt = %#v", current)
	}
	afterMutationRetry, err := s.CreatePromptAttempt(ctx, boundPromptAttemptForSQLiteTest(&store.PromptAttempt{Key: key, RequestDigest: created.RequestDigest}), fence)
	if err != nil || afterMutationRetry.ExecutionState != store.PromptExecutionOutcomeUnknown {
		t.Fatalf("post-transition create retry = %#v, %v", afterMutationRetry, err)
	}
	if _, err := s.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
		ID: current.ID, Fence: fence, ExpectedVersion: current.Version,
		ExpectedState: current.ExecutionState, NewState: store.PromptExecutionQueued,
		OperationID: "illegal-retry", OperationDigest: controlTestDigest("illegal-retry"),
	}); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("OutcomeUnknown replay transition error = %v, want ErrValidation", err)
	}
}

func TestPromptAttemptProvenNotAcceptedRecoveryPreservesBindings(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	fence := seedControlEpoch(t, s)
	key := store.PromptAttemptKey{Namespace: "ns", TaskUID: "retryable-unsent-task", Attempt: 1, PromptID: "retryable-unsent-prompt"}
	attempt, err := s.CreatePromptAttempt(ctx, boundPromptAttemptForSQLiteTest(&store.PromptAttempt{
		Key: key, RequestDigest: controlTestDigest("retryable-unsent-request"),
	}), fence)
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []store.PromptExecutionState{
		store.PromptExecutionReserved,
		store.PromptExecutionSessionStarting,
		store.PromptExecutionPlanned,
		store.PromptExecutionSubmitting,
	} {
		transition := store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: next, OperationID: "to-" + string(next), OperationDigest: controlTestDigest("to-" + string(next)),
			UpdatedAt: time.Now().UTC(),
		}
		if next == store.PromptExecutionPlanned {
			transition.RuntimeInstanceID = "runtime-instance"
			transition.SessionUID = "session-uid"
			transition.SessionLeaseGeneration = 3
		}
		attempt, err = s.TransitionPromptAttemptExecution(ctx, transition)
		if err != nil {
			t.Fatal(err)
		}
	}
	withoutProof := store.PromptAttemptPreSubmissionRecovery{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
		PreserveBindings: true, OperationID: "recover-without-proof",
		OperationDigest: controlTestDigest("recover-without-proof"), RecoveredAt: time.Now().UTC(),
	}
	if _, err := s.RecoverPromptAttemptPreSubmission(ctx, withoutProof); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("unproven binding-preserving recovery error = %v, want ErrValidation", err)
	}
	recovered, err := s.RecoverPromptAttemptPreSubmission(ctx, store.PromptAttemptPreSubmissionRecovery{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
		ProvenNotAccepted: true, PreserveBindings: true, OperationID: "recover-retryable-unsent",
		OperationDigest: controlTestDigest("recover-retryable-unsent"), RecoveredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ExecutionState != store.PromptExecutionReserved || recovered.ID != attempt.ID || recovered.Key != attempt.Key ||
		recovered.RequestDigest != attempt.RequestDigest || recovered.RuntimeInstanceID != attempt.RuntimeInstanceID ||
		recovered.SessionUID != attempt.SessionUID || recovered.SessionLeaseGeneration != attempt.SessionLeaseGeneration {
		t.Fatalf("binding-preserving recovery = %#v, want identity and bindings from %#v", recovered, attempt)
	}
}

func TestPromptAttemptBindingDigestMigrationPreservesLegacyRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-prompt-attempt.db")
	legacyDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacyDB.Exec(`CREATE TABLE prompt_attempts (
		id TEXT PRIMARY KEY,
		namespace TEXT NOT NULL,
		task_uid TEXT NOT NULL,
		attempt INTEGER NOT NULL,
		prompt_id TEXT NOT NULL,
		session_uid TEXT NOT NULL DEFAULT '',
		session_lease_generation INTEGER NOT NULL DEFAULT 0,
		runtime_instance_id TEXT NOT NULL DEFAULT '',
		request_digest TEXT NOT NULL,
		execution_state TEXT NOT NULL,
		delivery_state TEXT NOT NULL,
		terminal_reason TEXT NOT NULL DEFAULT '',
		outcome_marker TEXT NOT NULL DEFAULT '',
		controller_epoch_name TEXT NOT NULL,
		controller_epoch INTEGER NOT NULL,
		last_operation_id TEXT NOT NULL DEFAULT '',
		last_operation_digest TEXT NOT NULL DEFAULT '',
		version INTEGER NOT NULL,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
		UNIQUE(namespace, task_uid, attempt, prompt_id)
	);
	INSERT INTO prompt_attempts(
		id, namespace, task_uid, attempt, prompt_id, request_digest, execution_state,
		delivery_state, controller_epoch_name, controller_epoch, version, created_at, updated_at
	) VALUES (?, 'legacy-ns', 'legacy-task-uid', 1, 'legacy-prompt', ?, 'Running',
		'NotRequested', 'orka-controller', 1, 4, ?, ?)`,
		"legacy-attempt", controlTestDigest("legacy-request"),
		time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC), time.Date(2026, 7, 1, 10, 5, 0, 0, time.UTC))
	if err != nil {
		_ = legacyDB.Close()
		t.Fatalf("seed legacy PromptAttempt: %v", err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatal(err)
	}

	migratedDB, err := NewDB(path)
	if err != nil {
		t.Fatalf("migrate legacy PromptAttempt: %v", err)
	}
	defer migratedDB.Close() //nolint:errcheck
	attempt, err := NewStore(migratedDB, path).GetPromptAttempt(context.Background(), "legacy-attempt")
	if err != nil {
		t.Fatalf("read migrated legacy PromptAttempt: %v", err)
	}
	if attempt.BindingDigest != "" || attempt.SnapshotDigest != "" || attempt.ExecutionState != store.PromptExecutionRunning {
		t.Fatalf("migrated legacy PromptAttempt = %#v", attempt)
	}
}

func TestBranchClaimAndPublicationExactReceipts(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	fence := seedControlEpoch(t, s)
	now := time.Date(2026, 7, 24, 13, 0, 0, 0, time.UTC)
	claim, err := s.CreateBranchClaim(ctx, &store.BranchClaim{
		RepositoryID: "github:orka/test", Ref: "refs/heads/orka/session-1",
		OwnerKind: store.BranchClaimOwnerSession, OwnerUID: controlTestSessionUID, Generation: 1,
		LastVerified: store.RemoteRefState{Absent: true}, RequestDigest: controlTestDigest("claim"),
		CreatedAt: now,
	}, fence)
	if err != nil {
		t.Fatalf("CreateBranchClaim: %v", err)
	}
	if _, err := s.CreateBranchClaim(ctx, &store.BranchClaim{
		RepositoryID: claim.RepositoryID, Ref: claim.Ref, OwnerKind: claim.OwnerKind,
		OwnerUID: claim.OwnerUID, Generation: 1, LastVerified: claim.LastVerified,
		RequestDigest: claim.RequestDigest, CreatedAt: now,
	}, fence); err != nil {
		t.Fatalf("same branch claim retry: %v", err)
	}
	if _, err := s.CompareAndSwapBranchClaim(ctx, store.BranchClaimCAS{
		ID: claim.ID, Fence: fence, ExpectedVersion: claim.Version, ExpectedGeneration: claim.Generation,
		NewGeneration: claim.Generation, ExpectedLastVerified: store.RemoteRefState{SHA: controlTestGitSHA},
		NewLastVerified:      store.RemoteRefState{SHA: controlTestGitSHA2},
		ExpectedAvailability: store.BranchClaimAvailable, NewAvailability: store.BranchClaimAvailable,
		OperationID: "wrong-baseline", OperationDigest: controlTestDigest("wrong-baseline"), UpdatedAt: now.Add(time.Second),
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("branch baseline mismatch error = %v, want ErrConflict", err)
	}

	publication := &store.Publication{
		ID: "publication-1", Namespace: "ns", Generation: 1, TaskUID: "task-uid-1", Attempt: 1,
		PromptID: "prompt-1", SessionUID: claim.OwnerUID, BranchClaimID: claim.ID,
		BranchClaimGeneration: claim.Generation, SourceRepositoryID: "github:orka/source", SourceBaselineSHA: controlTestGitSHA,
		SourceRef: "refs/heads/main", TargetRepositoryID: claim.RepositoryID, TargetRef: claim.Ref,
		Baseline: claim.LastVerified, ArtifactID: "sha256-artifact", ArtifactDigest: controlTestDigest("artifact"),
		ArtifactSizeBytes: 128, ArtifactMediaType: "application/vnd.orka.workspace-delta.v1+tar",
		PublicationCredentialRef: "secret/ns/publisher#token", CommitIdentity: "Orka <orka@example.invalid>",
		CommitMessage: "feat: apply agent changes", CommitTimestamp: now,
		RequestDigest: controlTestDigest("publication-intent"), CreatedAt: now,
	}
	current, err := s.CreatePublication(ctx, publication, fence)
	if err != nil {
		t.Fatalf("CreatePublication: %v", err)
	}
	prepared := store.PreparedPublicationReceipt{
		OperationID: controlTestPrepareOperation, RequestDigest: controlTestDigest("prepare-response"),
		TreeSHA: controlTestGitSHA2, CommitSHA: controlTestGitSHA, ManifestDigest: controlTestDigest("manifest"),
		BundleArtifactID: "bundle-artifact", BundleDigest: controlTestDigest("bundle"), BundleSizeBytes: 256,
		BundleMediaType: store.PreparedBundleMediaType, BundleRef: "refs/orka/publications/" + strings.Repeat("f", 64),
		PreparedAt: now.Add(time.Minute),
	}
	current, err = s.TransitionPublication(ctx, store.PublicationTransition{
		ID: current.ID, Fence: fence, ExpectedVersion: current.Version, ExpectedGeneration: current.Generation,
		ExpectedState: current.State, NewState: store.PublicationPrepared,
		OperationID: controlTestPrepareOperation, OperationDigest: controlTestDigest(controlTestPrepareOperation),
		PreparedReceipt: &prepared, UpdatedAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("prepare publication: %v", err)
	}
	duplicate, err := s.TransitionPublication(ctx, store.PublicationTransition{
		ID: current.ID, Fence: fence, ExpectedVersion: 1, ExpectedGeneration: current.Generation,
		ExpectedState: store.PublicationPreparing, NewState: store.PublicationPrepared,
		OperationID: controlTestPrepareOperation, OperationDigest: controlTestDigest(controlTestPrepareOperation),
		PreparedReceipt: &prepared, UpdatedAt: now.Add(time.Minute),
	})
	if err != nil || duplicate.Version != current.Version {
		t.Fatalf("idempotent prepare retry = %#v, %v", duplicate, err)
	}
	if _, err := s.TransitionPublication(ctx, store.PublicationTransition{
		ID: current.ID, Fence: fence, ExpectedVersion: 1, ExpectedGeneration: current.Generation,
		ExpectedState: store.PublicationPreparing, NewState: store.PublicationPrepared,
		OperationID: controlTestPrepareOperation, OperationDigest: controlTestDigest("different-prepare"),
		PreparedReceipt: &prepared,
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("prepare digest mismatch error = %v, want ErrConflict", err)
	}
	prIntent := store.PullRequestIntent{
		BaseRepositoryID: publication.SourceRepositoryID, BaseRef: "refs/heads/main",
		HeadRepositoryID: publication.TargetRepositoryID, HeadRef: publication.TargetRef,
		PublicationGeneration: publication.Generation, ExpectedHeadSHA: prepared.CommitSHA,
	}
	current, err = s.SetPublicationPRIntent(ctx, store.SetPublicationPRIntentRequest{
		ID: current.ID, Fence: fence, ExpectedVersion: current.Version, ExpectedGeneration: current.Generation,
		ExpectedState: store.PublicationPrepared, Intent: prIntent,
		OperationID: "set-pr-intent", OperationDigest: controlTestDigest("set-pr-intent"), UpdatedAt: now.Add(90 * time.Second),
	})
	if err != nil {
		t.Fatalf("SetPublicationPRIntent: %v", err)
	}
	if current.PRIntent == nil || current.PRIntent.ExpectedHeadSHA != prepared.CommitSHA {
		t.Fatalf("PR intent = %#v", current.PRIntent)
	}
	current, err = s.TransitionPublication(ctx, store.PublicationTransition{
		ID: current.ID, Fence: fence, ExpectedVersion: current.Version, ExpectedGeneration: current.Generation,
		ExpectedState: current.State, NewState: store.PublicationPublishing,
		OperationID: "publish-cas", OperationDigest: controlTestDigest("publish-cas"), UpdatedAt: now.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("choose publishing: %v", err)
	}
	publishReceipt := store.PublishOperationReceipt{
		OperationID: "publish-call", RequestDigest: controlTestDigest("publish-call"),
		TargetRepositoryID: current.TargetRepositoryID, TargetRef: current.TargetRef,
		RemoteBefore: current.Baseline, ExpectedCommitSHA: prepared.CommitSHA,
		AcknowledgementUnknown: true, PublishedAt: now.Add(3 * time.Minute),
	}
	current, err = s.TransitionPublication(ctx, store.PublicationTransition{
		ID: current.ID, Fence: fence, ExpectedVersion: current.Version, ExpectedGeneration: current.Generation,
		ExpectedState: current.State, NewState: store.PublicationVerifying,
		OperationID: publishReceipt.OperationID, OperationDigest: publishReceipt.RequestDigest,
		PublishReceipt: &publishReceipt, UpdatedAt: now.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatalf("persist publish receipt: %v", err)
	}
	verification := store.PublicationVerificationReceipt{
		OperationID: "verify-call", RequestDigest: controlTestDigest("verify-call"),
		Outcome: store.PublicationVerifiedExact, ExpectedCommitSHA: prepared.CommitSHA,
		ObservedRemote: store.RemoteRefState{SHA: prepared.CommitSHA}, VerifiedAt: now.Add(4 * time.Minute),
	}
	current, err = s.TransitionPublication(ctx, store.PublicationTransition{
		ID: current.ID, Fence: fence, ExpectedVersion: current.Version, ExpectedGeneration: current.Generation,
		ExpectedState: current.State, NewState: store.PublicationVerifiedExact,
		OperationID: verification.OperationID, OperationDigest: verification.RequestDigest,
		VerificationReceipt: &verification, UpdatedAt: now.Add(4 * time.Minute),
	})
	if err != nil {
		t.Fatalf("verify publication: %v", err)
	}
	if current.State != store.PublicationVerifiedExact || current.VerificationReceipt.ObservedRemote.SHA != prepared.CommitSHA {
		t.Fatalf("verified publication = %#v", current)
	}
	afterMutationRetry, err := s.CreatePublication(ctx, publication, fence)
	if err != nil || afterMutationRetry.State != store.PublicationVerifiedExact {
		t.Fatalf("post-transition publication create retry = %#v, %v", afterMutationRetry, err)
	}
}

//nolint:gocyclo // The test verifies the complete atomic finalization and restart contract.
func TestSessionTurnAtomicFinalizationPersistsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "control.db")
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	s := NewStore(db, dbPath)
	fence := seedControlEpoch(t, s)
	now := time.Date(2026, 7, 24, 14, 0, 0, 0, time.UTC)
	createControlTranscriptSession(t, s, "ns", controlTestSessionName, now)
	control, err := s.CreateSessionControl(ctx, &store.SessionControl{
		Namespace: "ns", SessionName: controlTestSessionName, SessionUID: controlTestSessionUID,
		RequestDigest: controlTestDigest("session-control-final"), CreatedAt: now,
	}, fence)
	if err != nil {
		t.Fatalf("CreateSessionControl: %v", err)
	}
	claim, publication := createVerifiedPublicationFixture(t, s, fence, now, control.SessionUID)
	control, err = s.AcquireSessionMutationLease(ctx, store.AcquireSessionMutationLeaseRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, SessionUID: control.SessionUID,
		Fence: fence, ExpectedVersion: control.Version, ExpectedLeaseGeneration: control.LeaseGeneration,
		TaskUID: "task-uid-final", Attempt: 1, PromptID: "prompt-final",
		RequestDigest: controlTestDigest("lease-final"), AcquiredAt: now.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("AcquireSessionMutationLease: %v", err)
	}
	attemptKey := store.PromptAttemptKey{Namespace: "ns", TaskUID: "task-uid-final", Attempt: 1, PromptID: "prompt-final"}
	attempt, err := s.CreatePromptAttempt(ctx, boundPromptAttemptForSQLiteTest(&store.PromptAttempt{
		Key: attemptKey, SessionUID: control.SessionUID, SessionLeaseGeneration: control.LeaseGeneration,
		RequestDigest: controlTestDigest("prompt-final"), CreatedAt: now.Add(5 * time.Minute),
	}), fence)
	if err != nil {
		t.Fatalf("CreatePromptAttempt: %v", err)
	}
	attempt = completePromptAttemptForFinalization(t, s, fence, attempt, store.PromptDeliveryVerifiedExact)
	turnKey := store.SessionTurnKey{
		SessionUID: control.SessionUID, LeaseGeneration: control.LeaseGeneration,
		TaskUID: attemptKey.TaskUID, Attempt: attemptKey.Attempt, PromptID: attemptKey.PromptID,
	}
	turnID, _ := turnKey.CanonicalID()
	turn, err := s.CreateSessionTurn(ctx, store.CreateSessionTurnRequest{
		Turn: store.SessionTurn{Key: turnKey, PromptAttemptID: attempt.ID,
			RequestDigest: controlTestDigest("turn-final"), UserPrompt: "Please make the verified change.",
			CreatedAt: now.Add(6 * time.Minute)},
		Fence: fence, ExpectedSessionVersion: control.Version,
	})
	if err != nil {
		t.Fatalf("CreateSessionTurn: %v", err)
	}
	payload := json.RawMessage(`{"phase":"Succeeded","delivery":"VerifiedExact"}`)
	finalized, err := s.FinalizeSessionTurn(ctx, store.FinalizeSessionTurnRequest{
		Key: turnKey, Fence: fence, ExpectedSessionVersion: control.Version, ExpectedTurnVersion: turn.Version,
		FinalizationDigest: controlTestDigest("finalization"), TerminalKind: store.SessionTurnAssistantResult,
		TerminalContent: "The change was published and independently verified.", PublicationID: publication.ID,
		Projection: store.OutboxProjection{
			ID: "outbox-session-turn-final", AggregateKind: controlTestAggregateKind, AggregateID: turnID,
			ProjectionKind: controlTestProjectionKind, Payload: payload, PayloadDigest: controlTestDigest(string(payload)),
		},
		FinalizedAt: now.Add(7 * time.Minute),
	})
	if err != nil {
		t.Fatalf("FinalizeSessionTurn: %v", err)
	}
	if finalized.State != store.SessionTurnFinalized || finalized.PublicationReceipt == nil || finalized.PublicationReceipt.State != store.PublicationVerifiedExact {
		t.Fatalf("finalized turn = %#v", finalized)
	}
	transcript, err := s.LoadTranscript(ctx, "ns", controlTestSessionName, 0)
	if err != nil {
		t.Fatalf("LoadTranscript: %v", err)
	}
	if len(transcript) != 2 || transcript[0].Role != roleUser || transcript[1].Role != roleAssistant {
		t.Fatalf("transcript = %#v", transcript)
	}
	retry, err := s.FinalizeSessionTurn(ctx, store.FinalizeSessionTurnRequest{
		Key: turnKey, Fence: fence, ExpectedSessionVersion: control.Version, ExpectedTurnVersion: turn.Version,
		FinalizationDigest: finalized.FinalizationDigest, TerminalKind: finalized.TerminalKind,
		TerminalContent: finalized.TerminalContent, PublicationID: publication.ID,
		Projection: store.OutboxProjection{
			ID: "outbox-session-turn-final", AggregateKind: controlTestAggregateKind, AggregateID: turnID,
			ProjectionKind: controlTestProjectionKind, Payload: payload, PayloadDigest: controlTestDigest(string(payload)),
		},
		FinalizedAt: now.Add(7 * time.Minute),
	})
	if err != nil || retry.Version != finalized.Version {
		t.Fatalf("idempotent finalization retry = %#v, %v", retry, err)
	}
	transcript, _ = s.LoadTranscript(ctx, "ns", controlTestSessionName, 0)
	if len(transcript) != 2 {
		t.Fatalf("retry duplicated transcript: %#v", transcript)
	}
	updatedControl, err := s.GetSessionControl(ctx, "ns", controlTestSessionName)
	if err != nil {
		t.Fatalf("GetSessionControl: %v", err)
	}
	if updatedControl.Lease != nil || updatedControl.Availability != store.SessionAvailable || updatedControl.VerifiedBaseline == nil || updatedControl.VerifiedBaseline.SHA != controlTestGitSHA {
		t.Fatalf("finalized session control = %#v", updatedControl)
	}
	controlRetry, err := s.CreateSessionControl(ctx, &store.SessionControl{
		Namespace: "ns", SessionName: controlTestSessionName, SessionUID: controlTestSessionUID,
		RequestDigest: controlTestDigest("session-control-final"), CreatedAt: now,
	}, fence)
	if err != nil || controlRetry.Version != updatedControl.Version {
		t.Fatalf("post-finalization session-control retry = %#v, %v", controlRetry, err)
	}
	updatedClaim, err := s.GetBranchClaim(ctx, claim.ID)
	if err != nil || updatedClaim.LastVerified.SHA != controlTestGitSHA {
		t.Fatalf("finalized branch claim = %#v, %v", updatedClaim, err)
	}
	claimRetry, err := s.CreateBranchClaim(ctx, &store.BranchClaim{
		RepositoryID: claim.RepositoryID, Ref: claim.Ref, OwnerKind: claim.OwnerKind, OwnerUID: claim.OwnerUID,
		Generation: 1, LastVerified: store.RemoteRefState{Absent: true}, RequestDigest: claim.RequestDigest, CreatedAt: now,
	}, fence)
	if err != nil || claimRetry.LastVerified.SHA != controlTestGitSHA {
		t.Fatalf("post-finalization branch-claim retry = %#v, %v", claimRetry, err)
	}
	outbox, err := s.GetOutboxProjection(ctx, "outbox-session-turn-final")
	if err != nil || outbox.State != store.OutboxProjectionPending {
		t.Fatalf("outbox = %#v, %v", outbox, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}
	db, err = NewDB(dbPath)
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	defer db.Close() //nolint:errcheck
	restarted := NewStore(db, dbPath)
	if got, err := restarted.GetSessionTurn(ctx, finalized.ID); err != nil || got.State != store.SessionTurnFinalized {
		t.Fatalf("restarted session turn = %#v, %v", got, err)
	}
	if got, err := restarted.GetOutboxProjection(ctx, outbox.ID); err != nil || got.State != store.OutboxProjectionPending {
		t.Fatalf("restarted outbox = %#v, %v", got, err)
	}
}

func TestSessionRuntimeGenerationCommit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "runtime-generation.db")
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	s := NewStore(db, dbPath)
	ctx := context.Background()
	fence := seedControlEpoch(t, s)
	now := time.Date(2026, 7, 24, 14, 30, 0, 0, time.UTC)
	createControlTranscriptSession(t, s, "ns", "runtime-generation", now)
	control, err := s.CreateSessionControl(ctx, &store.SessionControl{
		Namespace: "ns", SessionName: "runtime-generation", SessionUID: "runtime-generation-uid",
		RequestDigest: controlTestDigest("runtime-generation"), CreatedAt: now,
	}, fence)
	if err != nil {
		t.Fatal(err)
	}
	control, err = s.AcquireSessionMutationLease(ctx, store.AcquireSessionMutationLeaseRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, SessionUID: control.SessionUID,
		Fence: fence, ExpectedVersion: control.Version, ExpectedLeaseGeneration: control.LeaseGeneration,
		TaskUID: "runtime-generation-task", Attempt: 1, PromptID: "runtime-generation-prompt",
		RequestDigest: controlTestDigest("runtime-generation-lease"), AcquiredAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	key := store.SessionTurnKey{
		SessionUID: control.SessionUID, LeaseGeneration: control.LeaseGeneration,
		TaskUID: control.Lease.TaskUID, Attempt: control.Lease.Attempt, PromptID: control.Lease.PromptID,
	}
	request := store.CommitSessionRuntimeGenerationRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, SessionUID: control.SessionUID,
		Key: key, Fence: fence, ExpectedSessionVersion: control.Version,
		Generation: 3, CommittedAt: now.Add(2 * time.Minute),
	}
	committed, err := s.CommitSessionRuntimeGeneration(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if committed.RuntimeSessionGeneration != 3 || committed.Version != control.Version+1 {
		t.Fatalf("committed Session RuntimeSession generation = %#v", committed)
	}
	retry, err := s.CommitSessionRuntimeGeneration(ctx, request)
	if err != nil || retry.Version != committed.Version {
		t.Fatalf("idempotent generation commit = %#v, %v", retry, err)
	}
	regression := request
	regression.Generation = 2
	if _, err := s.CommitSessionRuntimeGeneration(ctx, regression); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("generation regression error = %v, want conflict", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = NewDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	restarted := NewStore(db, dbPath)
	persisted, err := restarted.GetSessionControl(ctx, control.Namespace, control.SessionName)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.RuntimeSessionGeneration != 3 || persisted.Version != committed.Version {
		t.Fatalf("restarted Session RuntimeSession generation = %#v", persisted)
	}
}

func TestPublicationOutcomeUnknownBlocksSessionAndBranch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "reconcile.db")
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	s := NewStore(db, dbPath)
	ctx := context.Background()
	fence := seedControlEpoch(t, s)
	now := time.Date(2026, 7, 24, 15, 0, 0, 0, time.UTC)
	createControlTranscriptSession(t, s, "ns", "blocked-session", now)
	control, err := s.CreateSessionControl(ctx, &store.SessionControl{
		Namespace: "ns", SessionName: "blocked-session", SessionUID: "blocked-session-uid",
		RequestDigest: controlTestDigest("session-control-blocked"), CreatedAt: now,
	}, fence)
	if err != nil {
		t.Fatalf("CreateSessionControl: %v", err)
	}
	claim, publication := createOutcomeUnknownPublicationFixture(t, s, fence, now, control.SessionUID)
	control, err = s.AcquireSessionMutationLease(ctx, store.AcquireSessionMutationLeaseRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, SessionUID: control.SessionUID,
		Fence: fence, ExpectedVersion: control.Version, ExpectedLeaseGeneration: control.LeaseGeneration,
		TaskUID: "task-uid-unknown", Attempt: 1, PromptID: "prompt-unknown",
		RequestDigest: controlTestDigest("lease-unknown"), AcquiredAt: now.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("AcquireSessionMutationLease: %v", err)
	}
	attemptKey := store.PromptAttemptKey{Namespace: "ns", TaskUID: "task-uid-unknown", Attempt: 1, PromptID: "prompt-unknown"}
	attempt, err := s.CreatePromptAttempt(ctx, boundPromptAttemptForSQLiteTest(&store.PromptAttempt{
		Key: attemptKey, SessionUID: control.SessionUID, SessionLeaseGeneration: control.LeaseGeneration,
		RequestDigest: controlTestDigest("prompt-unknown"), CreatedAt: now.Add(5 * time.Minute),
	}), fence)
	if err != nil {
		t.Fatalf("CreatePromptAttempt: %v", err)
	}
	attempt = completePromptAttemptForFinalization(t, s, fence, attempt, store.PromptDeliveryPublicationOutcomeUnknown)
	turnKey := store.SessionTurnKey{SessionUID: control.SessionUID, LeaseGeneration: control.LeaseGeneration, TaskUID: attemptKey.TaskUID, Attempt: 1, PromptID: attemptKey.PromptID}
	turnID, _ := turnKey.CanonicalID()
	turn, err := s.CreateSessionTurn(ctx, store.CreateSessionTurnRequest{
		Turn: store.SessionTurn{Key: turnKey, PromptAttemptID: attempt.ID,
			RequestDigest: controlTestDigest("turn-unknown"), UserPrompt: "Publish a change.", CreatedAt: now.Add(6 * time.Minute)},
		Fence: fence, ExpectedSessionVersion: control.Version,
	})
	if err != nil {
		t.Fatalf("CreateSessionTurn: %v", err)
	}
	payload := json.RawMessage(`{"phase":"Failed","execution":"OutcomeUnknown","delivery":"PublicationOutcomeUnknown"}`)
	_, err = s.FinalizeSessionTurn(ctx, store.FinalizeSessionTurnRequest{
		Key: turnKey, Fence: fence, ExpectedSessionVersion: control.Version, ExpectedTurnVersion: turn.Version,
		FinalizationDigest: controlTestDigest("unknown-finalization"), TerminalKind: store.SessionTurnOutcomeMarker,
		TerminalContent: "Publication may have occurred; no assistant result was invented.", PublicationID: publication.ID,
		BlockReason: "remote ref could not be observed after bounded verification",
		Projection: store.OutboxProjection{ID: "outbox-unknown", AggregateKind: controlTestAggregateKind, AggregateID: turnID,
			ProjectionKind: controlTestProjectionKind, Payload: payload, PayloadDigest: controlTestDigest(string(payload))},
		FinalizedAt: now.Add(7 * time.Minute),
	})
	if err != nil {
		t.Fatalf("FinalizeSessionTurn unknown: %v", err)
	}
	blocked, err := s.GetSessionControl(ctx, control.Namespace, control.SessionName)
	if err != nil || blocked.Availability != store.SessionReconciliationBlocked || blocked.Lease != nil {
		t.Fatalf("blocked session = %#v, %v", blocked, err)
	}
	blockedClaim, err := s.GetBranchClaim(ctx, claim.ID)
	if err != nil || blockedClaim.Availability != store.BranchClaimReconciliationBlocked || blockedClaim.RelatedPublicationID != publication.ID {
		t.Fatalf("blocked branch claim = %#v, %v", blockedClaim, err)
	}
	if _, err := s.AcquireSessionMutationLease(ctx, store.AcquireSessionMutationLeaseRequest{
		Namespace: blocked.Namespace, SessionName: blocked.SessionName, SessionUID: blocked.SessionUID,
		Fence: fence, ExpectedVersion: blocked.Version, ExpectedLeaseGeneration: blocked.LeaseGeneration,
		TaskUID: "new-task", Attempt: 1, PromptID: "new-prompt", RequestDigest: controlTestDigest("new-lease"),
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("blocked session lease error = %v, want ErrConflict", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before reconciliation: %v", err)
	}
	db, err = NewDB(dbPath)
	if err != nil {
		t.Fatalf("reopen before reconciliation: %v", err)
	}
	defer db.Close() //nolint:errcheck
	s = NewStore(db, dbPath)
	reconcileOperationID := "reconcile-unknown-publication"
	reconcileOperationDigest := controlTestDigest(reconcileOperationID)
	reconciled, err := s.ReconcileSessionControl(ctx, store.ReconcileSessionControlRequest{
		Namespace: blocked.Namespace, SessionName: blocked.SessionName, SessionUID: blocked.SessionUID,
		Fence: fence, ExpectedVersion: blocked.Version, ExpectedLeaseGeneration: blocked.LeaseGeneration,
		ExpectedRelatedPublicationID: publication.ID, BranchClaimID: blockedClaim.ID,
		ExpectedBranchClaimVersion: blockedClaim.Version, ExpectedBranchClaimGeneration: blockedClaim.Generation,
		ExpectedBranchBaseline: blockedClaim.LastVerified,
		VerifiedBaseline:       store.VerifiedBranchBaseline{RepositoryID: blockedClaim.RepositoryID, Ref: blockedClaim.Ref, SHA: controlTestGitSHA},
		OperationID:            reconcileOperationID, OperationDigest: reconcileOperationDigest,
		ReconciledAt: now.Add(8 * time.Minute),
	})
	if err != nil {
		t.Fatalf("ReconcileSessionControl: %v", err)
	}
	if reconciled.Availability != store.SessionAvailable || reconciled.VerifiedBaseline == nil || reconciled.VerifiedBaseline.SHA != controlTestGitSHA {
		t.Fatalf("reconciled session = %#v", reconciled)
	}
	if retry, err := s.ReconcileSessionControl(ctx, store.ReconcileSessionControlRequest{
		Namespace: blocked.Namespace, SessionName: blocked.SessionName, SessionUID: blocked.SessionUID,
		Fence: fence, ExpectedVersion: blocked.Version, ExpectedLeaseGeneration: blocked.LeaseGeneration,
		ExpectedRelatedPublicationID: publication.ID, BranchClaimID: blockedClaim.ID,
		ExpectedBranchClaimVersion: blockedClaim.Version, ExpectedBranchClaimGeneration: blockedClaim.Generation,
		ExpectedBranchBaseline: blockedClaim.LastVerified,
		VerifiedBaseline:       *reconciled.VerifiedBaseline, OperationID: reconcileOperationID,
		OperationDigest: reconcileOperationDigest, ReconciledAt: now.Add(8 * time.Minute),
	}); err != nil || retry.Version != reconciled.Version {
		t.Fatalf("idempotent session reconciliation retry = %#v, %v", retry, err)
	}
	reconciledClaim, err := s.GetBranchClaim(ctx, blockedClaim.ID)
	if err != nil || reconciledClaim.Availability != store.BranchClaimAvailable || reconciledClaim.LastVerified.SHA != controlTestGitSHA {
		t.Fatalf("reconciled branch claim = %#v, %v", reconciledClaim, err)
	}
	if _, err := s.AcquireSessionMutationLease(ctx, store.AcquireSessionMutationLeaseRequest{
		Namespace: reconciled.Namespace, SessionName: reconciled.SessionName, SessionUID: reconciled.SessionUID,
		Fence: fence, ExpectedVersion: reconciled.Version, ExpectedLeaseGeneration: reconciled.LeaseGeneration,
		TaskUID: "new-task", Attempt: 1, PromptID: "new-prompt", RequestDigest: controlTestDigest("new-lease"),
		AcquiredAt: now.Add(9 * time.Minute),
	}); err != nil {
		t.Fatalf("lease after reconciliation: %v", err)
	}
}

//nolint:gocyclo // The test covers the full reserve, lease, retry, delivery, and dead-letter lifecycle.
func TestExternalEffectAndOutboxRestartSafeLeases(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	fence := seedControlEpoch(t, s)
	now := time.Date(2026, 7, 24, 16, 0, 0, 0, time.UTC)
	identity := store.ExternalEffectIdentity{Kind: "publisher.prepare", Namespace: "ns", AggregateID: "publication-1", OperationID: "prepare-1"}
	effect, err := s.ReserveExternalEffect(ctx, store.ReserveExternalEffectRequest{
		Identity: identity, RequestDigest: controlTestDigest("prepare-request"), Fence: fence, CreatedAt: now,
	})
	if err != nil {
		t.Fatalf("ReserveExternalEffect: %v", err)
	}
	if _, err := s.ReserveExternalEffect(ctx, store.ReserveExternalEffectRequest{
		Identity: identity, RequestDigest: effect.RequestDigest, Fence: fence, CreatedAt: now,
	}); err != nil {
		t.Fatalf("same external effect retry: %v", err)
	}
	if _, err := s.ReserveExternalEffect(ctx, store.ReserveExternalEffectRequest{
		Identity: identity, RequestDigest: controlTestDigest("different-request"), Fence: fence, CreatedAt: now,
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("effect digest mismatch error = %v, want ErrConflict", err)
	}
	leaseExpiry := now.Add(time.Minute)
	effect, err = s.TransitionExternalEffect(ctx, store.ExternalEffectTransition{
		ID: effect.ID, Fence: fence, ExpectedVersion: effect.Version, ExpectedState: effect.State,
		NewState: store.ExternalEffectInFlight, RequestDigest: effect.RequestDigest,
		LeaseOwner: controlTestWorkerA, LeaseExpiresAt: &leaseExpiry, UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("claim external effect: %v", err)
	}
	response := json.RawMessage(`{"tree":"0123456789012345678901234567890123456789"}`)
	responseDigest := controlTestDigest(string(response))
	if _, err := s.TransitionExternalEffect(ctx, store.ExternalEffectTransition{
		ID: effect.ID, Fence: fence, ExpectedVersion: effect.Version, ExpectedState: effect.State,
		NewState: store.ExternalEffectSucceeded, RequestDigest: effect.RequestDigest,
		ExpectedLeaseOwner: "worker-b", Response: response, ResponseDigest: responseDigest,
		UpdatedAt: now.Add(30 * time.Second),
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("non-owner external effect completion error = %v, want ErrConflict", err)
	}
	effect, err = s.TransitionExternalEffect(ctx, store.ExternalEffectTransition{
		ID: effect.ID, Fence: fence, ExpectedVersion: effect.Version, ExpectedState: effect.State,
		NewState: store.ExternalEffectSucceeded, RequestDigest: effect.RequestDigest,
		ExpectedLeaseOwner: controlTestWorkerA, Response: response, ResponseDigest: responseDigest, UpdatedAt: now.Add(30 * time.Second),
	})
	if err != nil {
		t.Fatalf("complete external effect: %v", err)
	}
	if effect.State != store.ExternalEffectSucceeded || effect.Attempts != 1 {
		t.Fatalf("completed effect = %#v", effect)
	}
	if retry, err := s.TransitionExternalEffect(ctx, store.ExternalEffectTransition{
		ID: effect.ID, Fence: fence, ExpectedVersion: 2, ExpectedState: store.ExternalEffectInFlight,
		NewState: store.ExternalEffectSucceeded, RequestDigest: effect.RequestDigest,
		ExpectedLeaseOwner: controlTestWorkerA, Response: response, ResponseDigest: responseDigest, UpdatedAt: now.Add(30 * time.Second),
	}); err != nil || retry.Version != effect.Version {
		t.Fatalf("idempotent effect completion = %#v, %v", retry, err)
	}

	payload := json.RawMessage(`{"task":"task-1","phase":"Succeeded"}`)
	projection, err := s.EnqueueOutboxProjection(ctx, &store.OutboxProjection{
		ID: "outbox-lease-test", AggregateKind: controlTestAggregateKind, AggregateID: "turn-1",
		ProjectionKind: controlTestProjectionKind, Payload: payload, PayloadDigest: controlTestDigest(string(payload)),
		CreatedAt: now,
	}, fence)
	if err != nil {
		t.Fatalf("EnqueueOutboxProjection: %v", err)
	}
	if _, err := s.EnqueueOutboxProjection(ctx, &store.OutboxProjection{
		ID: projection.ID, AggregateKind: projection.AggregateKind, AggregateID: projection.AggregateID,
		ProjectionKind: projection.ProjectionKind, Payload: payload, PayloadDigest: projection.PayloadDigest,
		CreatedAt: now, AvailableAt: now.Add(5 * time.Minute),
	}, fence); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("outbox schedule mismatch error = %v, want ErrConflict", err)
	}
	claimed, err := s.ClaimOutboxProjections(ctx, store.ClaimOutboxProjectionsRequest{
		Fence: fence, WorkerID: controlTestProjectorA, Limit: 10, LeaseDuration: time.Minute, Now: now,
	})
	if err != nil || len(claimed) != 1 || claimed[0].ID != projection.ID {
		t.Fatalf("claimed outbox = %#v, %v", claimed, err)
	}
	projection = &claimed[0]
	retryOperationID := "outbox-retry-1"
	retryOperationDigest := controlTestDigest(retryOperationID)
	pending, err := s.CompleteOutboxProjection(ctx, store.CompleteOutboxProjectionRequest{
		ID: projection.ID, Fence: fence, ExpectedVersion: projection.Version, LeaseOwner: controlTestProjectorA,
		OperationID: retryOperationID, OperationDigest: retryOperationDigest,
		NewState: store.OutboxProjectionPending, LastError: "temporary API failure",
		AvailableAt: now.Add(2 * time.Minute), UpdatedAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("retry outbox: %v", err)
	}
	if retried, err := s.CompleteOutboxProjection(ctx, store.CompleteOutboxProjectionRequest{
		ID: projection.ID, Fence: fence, ExpectedVersion: projection.Version, LeaseOwner: controlTestProjectorA,
		OperationID: retryOperationID, OperationDigest: retryOperationDigest,
		NewState: store.OutboxProjectionPending, LastError: "temporary API failure",
		AvailableAt: now.Add(2 * time.Minute), UpdatedAt: now.Add(time.Minute),
	}); err != nil || retried.Version != pending.Version {
		t.Fatalf("idempotent outbox retry completion = %#v, %v", retried, err)
	}
	if claimed, err := s.ClaimOutboxProjections(ctx, store.ClaimOutboxProjectionsRequest{
		Fence: fence, WorkerID: controlTestProjectorB, Limit: 10, LeaseDuration: time.Minute, Now: now.Add(90 * time.Second),
	}); err != nil || len(claimed) != 0 {
		t.Fatalf("early claim = %#v, %v", claimed, err)
	}
	claimed, err = s.ClaimOutboxProjections(ctx, store.ClaimOutboxProjectionsRequest{
		Fence: fence, WorkerID: controlTestProjectorB, Limit: 10, LeaseDuration: time.Minute, Now: now.Add(2 * time.Minute),
	})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("retry claim = %#v, %v", claimed, err)
	}
	deliveryDigest := controlTestDigest("task-status-resource-version-9")
	delivered, err := s.CompleteOutboxProjection(ctx, store.CompleteOutboxProjectionRequest{
		ID: claimed[0].ID, Fence: fence, ExpectedVersion: claimed[0].Version, LeaseOwner: controlTestProjectorB,
		OperationID: "outbox-deliver-1", OperationDigest: controlTestDigest("outbox-deliver-1"),
		NewState: store.OutboxProjectionDelivered, DeliveryDigest: deliveryDigest,
		UpdatedAt: now.Add(150 * time.Second),
	})
	if err != nil {
		t.Fatalf("deliver outbox: %v", err)
	}
	if delivered.State != store.OutboxProjectionDelivered || delivered.Attempts != 2 || delivered.DeliveredAt == nil {
		t.Fatalf("delivered outbox = %#v", delivered)
	}
	if retry, err := s.CompleteOutboxProjection(ctx, store.CompleteOutboxProjectionRequest{
		ID: delivered.ID, Fence: fence, ExpectedVersion: 1, LeaseOwner: controlTestProjectorB,
		OperationID: "outbox-deliver-1", OperationDigest: controlTestDigest("outbox-deliver-1"),
		NewState: store.OutboxProjectionDelivered, DeliveryDigest: deliveryDigest,
		UpdatedAt: now.Add(150 * time.Second),
	}); err != nil || retry.Version != delivered.Version {
		t.Fatalf("idempotent delivery retry = %#v, %v", retry, err)
	}
	if retry, err := s.EnqueueOutboxProjection(ctx, &store.OutboxProjection{
		ID: delivered.ID, AggregateKind: delivered.AggregateKind, AggregateID: delivered.AggregateID,
		ProjectionKind: delivered.ProjectionKind, Payload: payload, PayloadDigest: delivered.PayloadDigest,
	}, fence); err != nil || retry.State != store.OutboxProjectionDelivered {
		t.Fatalf("schedule-omitted enqueue retry after delivery = %#v, %v", retry, err)
	}

	deadPayload := json.RawMessage(`{"task":"task-dead","phase":"Failed"}`)
	dead, err := s.EnqueueOutboxProjection(ctx, &store.OutboxProjection{
		ID: "outbox-dead-letter", AggregateKind: controlTestAggregateKind, AggregateID: "turn-dead",
		ProjectionKind: controlTestProjectionKind, Payload: deadPayload,
		PayloadDigest: controlTestDigest(string(deadPayload)), CreatedAt: now.Add(3 * time.Minute),
	}, fence)
	if err != nil {
		t.Fatalf("enqueue dead-letter fixture: %v", err)
	}
	deadClaim, err := s.ClaimOutboxProjections(ctx, store.ClaimOutboxProjectionsRequest{
		Fence: fence, WorkerID: controlTestProjectorA, Limit: 10, LeaseDuration: time.Minute,
		Now: now.Add(3 * time.Minute),
	})
	if err != nil || len(deadClaim) != 1 || deadClaim[0].ID != dead.ID {
		t.Fatalf("claim dead-letter fixture = %#v, %v", deadClaim, err)
	}
	deadOperationID := "outbox-dead-letter-1"
	deadOperationDigest := controlTestDigest(deadOperationID)
	dead, err = s.CompleteOutboxProjection(ctx, store.CompleteOutboxProjectionRequest{
		ID: dead.ID, Fence: fence, ExpectedVersion: deadClaim[0].Version, LeaseOwner: controlTestProjectorA,
		OperationID: deadOperationID, OperationDigest: deadOperationDigest,
		NewState: store.OutboxProjectionDeadLetter, LastError: "permanent projection rejection",
		UpdatedAt: now.Add(3*time.Minute + 30*time.Second),
	})
	if err != nil {
		t.Fatalf("dead-letter outbox: %v", err)
	}
	if retry, err := s.CompleteOutboxProjection(ctx, store.CompleteOutboxProjectionRequest{
		ID: dead.ID, Fence: fence, ExpectedVersion: deadClaim[0].Version, LeaseOwner: controlTestProjectorA,
		OperationID: deadOperationID, OperationDigest: deadOperationDigest,
		NewState: store.OutboxProjectionDeadLetter, LastError: "permanent projection rejection",
		UpdatedAt: now.Add(3*time.Minute + 30*time.Second),
	}); err != nil || retry.Version != dead.Version {
		t.Fatalf("idempotent dead-letter retry = %#v, %v", retry, err)
	}
}

func completePromptAttemptForFinalization(t *testing.T, s *Store, fence store.ControllerEpochFence, attempt *store.PromptAttempt, delivery store.PromptDeliveryState) *store.PromptAttempt {
	t.Helper()
	ctx := context.Background()
	for _, state := range []store.PromptExecutionState{
		store.PromptExecutionReserved, store.PromptExecutionSessionStarting, store.PromptExecutionPlanned,
		store.PromptExecutionSubmitting, store.PromptExecutionAccepted, store.PromptExecutionRunning,
		store.PromptExecutionSettling, store.PromptExecutionSucceeded,
	} {
		operationID := "complete-execution-" + string(state)
		updated, err := s.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: state, OperationID: operationID, OperationDigest: controlTestDigest(operationID),
			UpdatedAt: attempt.UpdatedAt.Add(time.Second),
		})
		if err != nil {
			t.Fatalf("complete prompt execution to %s: %v", state, err)
		}
		attempt = updated
	}
	if delivery == store.PromptDeliveryNotRequested {
		return attempt
	}
	for _, state := range []store.PromptDeliveryState{
		store.PromptDeliveryValidating, store.PromptDeliveryPreparing, store.PromptDeliveryPrepared,
		store.PromptDeliveryPublishing, store.PromptDeliveryVerifying, delivery,
	} {
		operationID := "complete-delivery-" + string(state)
		updated, err := s.TransitionPromptAttemptDelivery(ctx, store.PromptAttemptDeliveryTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.DeliveryState,
			NewState: state, OperationID: operationID, OperationDigest: controlTestDigest(operationID),
			TerminalReason: "delivery reconciliation completed", UpdatedAt: attempt.UpdatedAt.Add(time.Second),
		})
		if err != nil {
			t.Fatalf("complete prompt delivery to %s: %v", state, err)
		}
		attempt = updated
	}
	return attempt
}

func createVerifiedPublicationFixture(t *testing.T, s *Store, fence store.ControllerEpochFence, now time.Time, sessionUID string) (*store.BranchClaim, *store.Publication) {
	t.Helper()
	return createPublicationFixture(t, s, fence, now, sessionUID, store.PublicationVerifiedExact)
}

func createOutcomeUnknownPublicationFixture(t *testing.T, s *Store, fence store.ControllerEpochFence, now time.Time, sessionUID string) (*store.BranchClaim, *store.Publication) {
	t.Helper()
	return createPublicationFixture(t, s, fence, now, sessionUID, store.PublicationOutcomeUnknown)
}

func createPublicationFixture(t *testing.T, s *Store, fence store.ControllerEpochFence, now time.Time, sessionUID string, outcome store.PublicationState) (*store.BranchClaim, *store.Publication) {
	t.Helper()
	ctx := context.Background()
	suffix := "verified"
	taskSuffix := "final"
	if outcome == store.PublicationOutcomeUnknown {
		suffix = controlTestUnknownSuffix
		taskSuffix = controlTestUnknownSuffix
	}
	claim, err := s.CreateBranchClaim(ctx, &store.BranchClaim{
		RepositoryID: "github:orka/" + suffix, Ref: "refs/heads/orka/" + suffix,
		OwnerKind: store.BranchClaimOwnerSession, OwnerUID: sessionUID, Generation: 1,
		LastVerified: store.RemoteRefState{Absent: true}, RequestDigest: controlTestDigest("claim-" + suffix), CreatedAt: now,
	}, fence)
	if err != nil {
		t.Fatalf("fixture CreateBranchClaim: %v", err)
	}
	publication, err := s.CreatePublication(ctx, &store.Publication{
		ID: "publication-" + suffix, Namespace: "ns", Generation: 1,
		TaskUID: "task-uid-" + taskSuffix,
		Attempt: 1, PromptID: "prompt-" + taskSuffix,
		SessionUID: sessionUID, BranchClaimID: claim.ID, BranchClaimGeneration: claim.Generation,
		SourceRepositoryID: "github:orka/source", SourceBaselineSHA: controlTestGitSHA, SourceRef: "refs/heads/main",
		TargetRepositoryID: claim.RepositoryID, TargetRef: claim.Ref, Baseline: claim.LastVerified,
		ArtifactID: "sha256-artifact", ArtifactDigest: controlTestDigest("artifact-" + suffix), ArtifactSizeBytes: 128, ArtifactMediaType: "application/vnd.orka.workspace-delta.v1+tar", PublicationCredentialRef: "secret/ns/publisher#token",
		CommitIdentity: "Orka <orka@example.invalid>", CommitMessage: "feat: " + suffix,
		CommitTimestamp: now, RequestDigest: controlTestDigest("publication-" + suffix), CreatedAt: now,
	}, fence)
	if err != nil {
		t.Fatalf("fixture CreatePublication: %v", err)
	}
	prepared := store.PreparedPublicationReceipt{
		OperationID: "prepare-" + suffix, RequestDigest: controlTestDigest("prepare-response-" + suffix),
		TreeSHA: controlTestGitSHA2, CommitSHA: controlTestGitSHA, ManifestDigest: controlTestDigest("manifest-" + suffix),
		BundleArtifactID: "bundle-artifact-" + suffix, BundleDigest: controlTestDigest("bundle-" + suffix), BundleSizeBytes: 256,
		BundleMediaType: store.PreparedBundleMediaType, BundleRef: "refs/orka/publications/" + strings.Repeat("f", 64),
		PreparedAt: now.Add(time.Minute),
	}
	publication, err = s.TransitionPublication(ctx, store.PublicationTransition{
		ID: publication.ID, Fence: fence, ExpectedVersion: publication.Version, ExpectedGeneration: publication.Generation,
		ExpectedState: publication.State, NewState: store.PublicationPrepared,
		OperationID: "prepare-" + suffix, OperationDigest: controlTestDigest("prepare-" + suffix), PreparedReceipt: &prepared, UpdatedAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("fixture prepare: %v", err)
	}
	publication, err = s.TransitionPublication(ctx, store.PublicationTransition{
		ID: publication.ID, Fence: fence, ExpectedVersion: publication.Version, ExpectedGeneration: publication.Generation,
		ExpectedState: publication.State, NewState: store.PublicationPublishing,
		OperationID: "publish-cas-" + suffix, OperationDigest: controlTestDigest("publish-cas-" + suffix), UpdatedAt: now.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("fixture publishing: %v", err)
	}
	publishReceipt := store.PublishOperationReceipt{
		OperationID: "publish-" + suffix, RequestDigest: controlTestDigest("publish-" + suffix),
		TargetRepositoryID: publication.TargetRepositoryID, TargetRef: publication.TargetRef,
		RemoteBefore: publication.Baseline, ExpectedCommitSHA: prepared.CommitSHA,
		AcknowledgementUnknown: outcome == store.PublicationOutcomeUnknown, PublishedAt: now.Add(3 * time.Minute),
	}
	publication, err = s.TransitionPublication(ctx, store.PublicationTransition{
		ID: publication.ID, Fence: fence, ExpectedVersion: publication.Version, ExpectedGeneration: publication.Generation,
		ExpectedState: publication.State, NewState: store.PublicationVerifying,
		OperationID: publishReceipt.OperationID, OperationDigest: publishReceipt.RequestDigest,
		PublishReceipt: &publishReceipt, UpdatedAt: now.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatalf("fixture verifying: %v", err)
	}
	verification := store.PublicationVerificationReceipt{
		OperationID: "verify-" + suffix, RequestDigest: controlTestDigest("verify-" + suffix),
		Outcome: outcome, ExpectedCommitSHA: prepared.CommitSHA, VerifiedAt: now.Add(4 * time.Minute),
	}
	terminalReason := ""
	if outcome == store.PublicationVerifiedExact {
		verification.ObservedRemote = store.RemoteRefState{SHA: prepared.CommitSHA}
	} else {
		terminalReason = "remote observation remained unavailable"
	}
	publication, err = s.TransitionPublication(ctx, store.PublicationTransition{
		ID: publication.ID, Fence: fence, ExpectedVersion: publication.Version, ExpectedGeneration: publication.Generation,
		ExpectedState: publication.State, NewState: outcome, OperationID: verification.OperationID,
		OperationDigest: verification.RequestDigest, VerificationReceipt: &verification,
		TerminalReason: terminalReason, UpdatedAt: now.Add(4 * time.Minute),
	})
	if err != nil {
		t.Fatalf("fixture verification outcome %s: %v", outcome, err)
	}
	return claim, publication
}
