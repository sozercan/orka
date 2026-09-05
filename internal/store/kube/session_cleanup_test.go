package kube

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	controlstore "github.com/orka-agents/orka/internal/store"
	sqlitestore "github.com/orka-agents/orka/internal/store/sqlite"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestReclaimSessionDeletesPublishedCrossStoreStateAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, sqliteStore, db, fence := newSessionCleanupTestStore(t, nil)
	control, claim := seedPublishedSessionCleanupState(t, ctx, kubeStore, sqliteStore, db, fence, "session-cleanup")
	request := controlstore.ReclaimSessionRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, Fence: fence,
		OperationID: "delete-session-cleanup", OperationDigest: testDigest("delete-session-cleanup"), RequestedAt: testNow,
	}
	if err := kubeStore.ReclaimSession(ctx, request); err != nil {
		t.Fatalf("ReclaimSession(): %v", err)
	}
	if _, err := sqliteStore.GetSession(ctx, control.Namespace, control.SessionName); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("GetSession() error = %v, want ErrNotFound", err)
	}
	if _, err := kubeStore.GetSessionControl(ctx, control.Namespace, control.SessionName); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("GetSessionControl() error = %v, want ErrNotFound", err)
	}
	if _, err := kubeStore.GetBranchClaim(ctx, claim.ID); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("GetBranchClaim() error = %v, want ErrNotFound", err)
	}
	lease := &coordinationv1.Lease{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Namespace: control.Namespace, Name: runtimeSessionLeaseName(control.SessionUID)}, lease); !apierrors.IsNotFound(err) {
		t.Fatalf("Session Lease lookup error = %v, want NotFound", err)
	}
	completion, err := sqliteStore.GetSessionCleanupCompletion(ctx, control.Namespace, control.SessionName)
	if err != nil {
		t.Fatalf("GetSessionCleanupCompletion(): %v", err)
	}
	if completion.OperationID != request.OperationID || completion.OperationDigest != request.OperationDigest {
		t.Fatalf("completion receipt = %#v", completion)
	}
	if completion.SessionUID != control.SessionUID {
		t.Fatalf("completion Session UID = %q, want %q", completion.SessionUID, control.SessionUID)
	}
	if err := kubeStore.ReclaimSession(ctx, request); err != nil {
		t.Fatalf("ReclaimSession(idempotent retry): %v", err)
	}
	if _, err := kubeStore.CreateSessionControl(ctx, &controlstore.SessionControl{
		Namespace: control.Namespace, SessionName: control.SessionName, SessionUID: control.SessionUID,
		RequestDigest: control.RequestDigest, LeaseGeneration: 1, Availability: controlstore.SessionAvailable, CreatedAt: testNow,
	}, fence); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("CreateSessionControl(after deletion) error = %v, want ErrConflict", err)
	}
	if _, err := kubeStore.CreateSessionControl(ctx, &controlstore.SessionControl{
		Namespace: control.Namespace, SessionName: control.SessionName + "-renamed", SessionUID: control.SessionUID,
		RequestDigest: testDigest("renamed-deleted-session-control"), LeaseGeneration: 1,
		Availability: controlstore.SessionAvailable, CreatedAt: testNow,
	}, fence); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("CreateSessionControl(reused deleted UID) error = %v, want ErrConflict", err)
	}
	if _, err := kubeStore.CreateBranchClaim(ctx, &controlstore.BranchClaim{
		RepositoryID: "github.com/orka/reappeared", Ref: "refs/heads/reappeared",
		OwnerKind: controlstore.BranchClaimOwnerSession, OwnerUID: control.SessionUID,
		LastVerified: controlstore.RemoteRefState{Absent: true}, RequestDigest: testDigest("reappeared-claim"), CreatedAt: testNow,
	}, fence); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("CreateBranchClaim(after deletion) error = %v, want ErrConflict", err)
	}
	if err := sqliteStore.CreateSession(ctx, &controlstore.SessionRecord{
		Namespace: control.Namespace, Name: control.SessionName, SessionType: "chat", CreatedAt: testNow, UpdatedAt: testNow,
	}); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("CreateSession(recently deleted name) error = %v, want ErrConflict", err)
	}
}

func TestReclaimSessionWithoutClusterScopedBranchClaims(t *testing.T) {
	ctx := context.Background()
	_, kubeClient, fence := newTestStoreWithEpoch(t)
	withWatch, ok := kubeClient.(client.WithWatch)
	if !ok {
		t.Fatal("fake client does not implement client.WithWatch")
	}
	branchClaimLists := 0
	branchBlockingReader := interceptor.NewClient(withWatch, interceptor.Funcs{
		List: func(ctx context.Context, delegate client.WithWatch, list client.ObjectList, options ...client.ListOption) error {
			if _, isBranchClaims := list.(*corev1alpha1.BranchClaimList); isBranchClaims {
				branchClaimLists++
				return errors.New("unexpected cluster-scoped BranchClaim list")
			}
			return delegate.List(ctx, list, options...)
		},
	})
	db, err := sqlitestore.NewDB(filepath.Join(t.TempDir(), "branchless-session-cleanup.db"))
	if err != nil {
		t.Fatalf("NewDB(): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sqliteStore := sqlitestore.NewStore(db, "")
	kubeStore, err := NewComposite(
		kubeClient,
		testControlNamespace,
		sqliteStore,
		WithAPIReader(branchBlockingReader),
		WithoutClusterScopedBranchClaims(),
	)
	if err != nil {
		t.Fatalf("NewComposite(): %v", err)
	}
	const sessionName = "harness-v1-session-cleanup"
	if err := sqliteStore.CreateSession(ctx, &controlstore.SessionRecord{
		Namespace: "tenant-a", Name: sessionName, SessionType: "task", CreatedAt: testNow, UpdatedAt: testNow,
	}); err != nil {
		t.Fatalf("CreateSession(): %v", err)
	}
	control, err := kubeStore.CreateSessionControl(ctx, &controlstore.SessionControl{
		Namespace: "tenant-a", SessionName: sessionName, SessionUID: sessionName + "-uid",
		RequestDigest: testDigest(sessionName + "-control"), LeaseGeneration: 1,
		Availability: controlstore.SessionAvailable, CreatedAt: testNow,
	}, fence)
	if err != nil {
		t.Fatalf("CreateSessionControl(): %v", err)
	}
	if err := kubeStore.ReclaimSession(ctx, controlstore.ReclaimSessionRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, Fence: fence,
		OperationID: "delete-harness-v1-session", OperationDigest: testDigest("delete-harness-v1-session"), RequestedAt: testNow,
	}); err != nil {
		t.Fatalf("ReclaimSession(): %v", err)
	}
	if branchClaimLists != 0 {
		t.Fatalf("BranchClaim list calls = %d, want 0", branchClaimLists)
	}
	if _, err := kubeStore.GetBranchClaim(ctx, "forbidden"); !errors.Is(err, ErrBranchClaimAccessDisabled) {
		t.Fatalf("GetBranchClaim() error = %v, want ErrBranchClaimAccessDisabled", err)
	}
	if _, err := sqliteStore.GetSession(ctx, control.Namespace, control.SessionName); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("GetSession() error = %v, want ErrNotFound", err)
	}
}

func TestReclaimSessionCompletionDetectsRawKubernetesOwnershipReappearance(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, sqliteStore, _, fence := newSessionCleanupTestStore(t, nil)
	control, _ := seedSessionCleanupState(t, ctx, kubeStore, sqliteStore, fence, "session-reappeared-raw")
	request := controlstore.ReclaimSessionRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, Fence: fence,
		OperationID: "delete-session-reappeared", OperationDigest: testDigest("delete-session-reappeared"), RequestedAt: testNow,
	}
	if err := kubeStore.ReclaimSession(ctx, request); err != nil {
		t.Fatalf("ReclaimSession(): %v", err)
	}
	late := &corev1alpha1.BranchClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "raw-reappeared-session-claim"},
		Spec: corev1alpha1.BranchClaimSpec{
			ID: "raw-reappeared-session-claim", RepositoryID: "github.com/orka/raw", Ref: "refs/heads/raw",
			OwnerKind: corev1alpha1.BranchClaimOwnerKind(controlstore.BranchClaimOwnerSession), OwnerUID: control.SessionUID,
			RequestDigest: testDigest("raw-reappeared-session-claim"),
		},
	}
	if err := kubeClient.Create(ctx, late); err != nil {
		t.Fatalf("inject raw Session-owned BranchClaim: %v", err)
	}
	if err := kubeStore.ReclaimSession(ctx, request); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("ReclaimSession(reappeared ownership) error = %v, want ErrConflict", err)
	}
}

func TestReclaimSessionFailsClosedWhenLateSessionClaimAppears(t *testing.T) {
	ctx := context.Background()
	kubeStore, kubeClient, sqliteStore, _, fence := newSessionCleanupTestStore(t, nil)
	control, original := seedSessionCleanupState(t, ctx, kubeStore, sqliteStore, fence, "session-late-claim")
	withWatch, ok := kubeClient.(client.WithWatch)
	if !ok {
		t.Fatal("fake client does not implement client.WithWatch")
	}
	late := &corev1alpha1.BranchClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "late-session-claim"},
		Spec: corev1alpha1.BranchClaimSpec{
			ID: "late-session-claim", RepositoryID: "github.com/orka/late", Ref: "refs/heads/late",
			OwnerKind: corev1alpha1.BranchClaimOwnerKind(controlstore.BranchClaimOwnerSession), OwnerUID: control.SessionUID,
			RequestDigest: testDigest("late-session-claim"),
		},
	}
	injected := false
	kubeStore.client = interceptor.NewClient(withWatch, interceptor.Funcs{
		Delete: func(ctx context.Context, delegate client.WithWatch, object client.Object, options ...client.DeleteOption) error {
			claim, isClaim := object.(*corev1alpha1.BranchClaim)
			if !isClaim || claim.Spec.ID != original.ID || injected {
				return delegate.Delete(ctx, object, options...)
			}
			if err := delegate.Delete(ctx, object, options...); err != nil {
				return err
			}
			injected = true
			return delegate.Create(ctx, late)
		},
	})
	request := controlstore.ReclaimSessionRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, Fence: fence,
		OperationID: "delete-session-late-claim", OperationDigest: testDigest("delete-session-late-claim"), RequestedAt: testNow,
	}
	if err := kubeStore.ReclaimSession(ctx, request); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("ReclaimSession() error = %v, want ErrConflict", err)
	}
	if _, err := sqliteStore.GetSession(ctx, control.Namespace, control.SessionName); err != nil {
		t.Fatalf("Session transcript was deleted despite late claim: %v", err)
	}
	if _, err := kubeStore.GetSessionControl(ctx, control.Namespace, control.SessionName); err != nil {
		t.Fatalf("Session control was deleted despite late claim: %v", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: late.Name}, &corev1alpha1.BranchClaim{}); err != nil {
		t.Fatalf("late Session claim missing: %v", err)
	}
	if _, err := sqliteStore.GetSessionCleanupIntent(ctx, control.Namespace, control.SessionName); err != nil {
		t.Fatalf("cleanup intent was not retained: %v", err)
	}
}

func TestResumeSessionCleanupsCompletesCrashAfterKubernetesReclamation(t *testing.T) {
	ctx := context.Background()
	injectedErr := errors.New("simulated SQLite completion outage")
	persistence := &failOnceSessionCleanupPersistence{err: injectedErr}
	kubeStore, _, sqliteStore, _, fence := newSessionCleanupTestStore(t, persistence)
	control, _ := seedSessionCleanupState(t, ctx, kubeStore, sqliteStore, fence, "session-resume-cleanup")
	request := controlstore.ReclaimSessionRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, Fence: fence,
		OperationID: "delete-session-resume", OperationDigest: testDigest("delete-session-resume"), RequestedAt: testNow,
	}
	if err := kubeStore.ReclaimSession(ctx, request); !errors.Is(err, injectedErr) {
		t.Fatalf("ReclaimSession() error = %v, want injected completion error", err)
	}
	if _, err := kubeStore.GetSessionControl(ctx, control.Namespace, control.SessionName); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("GetSessionControl() error = %v, want ErrNotFound after partial cleanup", err)
	}
	if _, err := sqliteStore.GetSession(ctx, control.Namespace, control.SessionName); err != nil {
		t.Fatalf("Session transcript was removed before completion retry: %v", err)
	}
	if err := kubeStore.ResumeSessionCleanups(ctx, fence); err != nil {
		t.Fatalf("ResumeSessionCleanups(): %v", err)
	}
	if _, err := sqliteStore.GetSession(ctx, control.Namespace, control.SessionName); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("GetSession() after recovery error = %v, want ErrNotFound", err)
	}
}

func TestReclaimSessionRecoversLegacyKubernetesOrphanAfterTranscriptDeletion(t *testing.T) {
	ctx := context.Background()
	kubeStore, _, sqliteStore, _, fence := newSessionCleanupTestStore(t, nil)
	control, claim := seedSessionCleanupState(t, ctx, kubeStore, sqliteStore, fence, "session-legacy-orphan")
	if err := sqliteStore.DeleteSession(ctx, control.Namespace, control.SessionName); err != nil {
		t.Fatalf("legacy SQLite-only DeleteSession(): %v", err)
	}
	request := controlstore.ReclaimSessionRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, Fence: fence,
		OperationID: "delete-session-legacy-orphan", OperationDigest: testDigest("delete-session-legacy-orphan"), RequestedAt: testNow,
	}
	if err := kubeStore.ReclaimSession(ctx, request); err != nil {
		t.Fatalf("ReclaimSession(legacy orphan): %v", err)
	}
	if _, err := kubeStore.GetSessionControl(ctx, control.Namespace, control.SessionName); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("GetSessionControl() error = %v, want ErrNotFound", err)
	}
	if _, err := kubeStore.GetBranchClaim(ctx, claim.ID); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("GetBranchClaim() error = %v, want ErrNotFound", err)
	}
	completion, err := sqliteStore.GetSessionCleanupCompletion(ctx, control.Namespace, control.SessionName)
	if err != nil || completion.SessionUID != control.SessionUID {
		t.Fatalf("legacy orphan completion = %#v, err=%v", completion, err)
	}
}

func TestReclaimSessionRejectsPreCutoverReplacementWithOldKubernetesIdentity(t *testing.T) {
	ctx := context.Background()
	kubeStore, _, sqliteStore, db, fence := newSessionCleanupTestStore(t, nil)
	control, _ := seedSessionCleanupState(t, ctx, kubeStore, sqliteStore, fence, "session-precutover-replacement")
	if err := sqliteStore.DeleteSession(ctx, control.Namespace, control.SessionName); err != nil {
		t.Fatalf("legacy SQLite-only DeleteSession(): %v", err)
	}
	if err := sqliteStore.CreateSession(ctx, &controlstore.SessionRecord{
		Namespace: control.Namespace, Name: control.SessionName, SessionType: "chat",
		CreatedAt: testNow.Add(time.Hour), UpdatedAt: testNow.Add(time.Hour),
	}); err != nil {
		t.Fatalf("CreateSession(replacement): %v", err)
	}
	if _, err := kubeStore.CreateSessionControl(ctx, &controlstore.SessionControl{
		Namespace: control.Namespace, SessionName: control.SessionName, SessionUID: control.SessionUID,
		RequestDigest: control.RequestDigest, LeaseGeneration: control.LeaseGeneration,
		Availability: control.Availability, CreatedAt: control.CreatedAt, UpdatedAt: control.UpdatedAt,
	}, fence); err != nil {
		t.Fatalf("CreateSessionControl(idempotent legacy retry): %v", err)
	}
	var replacementBinding string
	if err := db.QueryRowContext(ctx,
		`SELECT control_session_uid FROM sessions WHERE namespace = ? AND name = ?`, control.Namespace, control.SessionName,
	).Scan(&replacementBinding); err != nil {
		t.Fatalf("read replacement cleanup identity: %v", err)
	}
	if replacementBinding != "" {
		t.Fatalf("legacy control retry bound replacement transcript to %q", replacementBinding)
	}
	request := controlstore.ReclaimSessionRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, Fence: fence,
		OperationID: "delete-session-precutover-replacement", OperationDigest: testDigest("delete-session-precutover-replacement"), RequestedAt: testNow,
	}
	if err := kubeStore.ReclaimSession(ctx, request); !errors.Is(err, controlstore.ErrConflict) {
		t.Fatalf("ReclaimSession(replacement) error = %v, want ErrConflict", err)
	}
	if _, err := sqliteStore.GetSession(ctx, control.Namespace, control.SessionName); err != nil {
		t.Fatalf("replacement Session was deleted: %v", err)
	}
	if _, err := kubeStore.GetSessionControl(ctx, control.Namespace, control.SessionName); err != nil {
		t.Fatalf("legacy control was deleted despite identity mismatch: %v", err)
	}
}

func TestReclaimSessionRecoversBindBeforeControlCrash(t *testing.T) {
	ctx := context.Background()
	kubeStore, _, sqliteStore, _, fence := newSessionCleanupTestStore(t, nil)
	const (
		namespace   = "tenant-a"
		sessionName = "session-bind-before-control"
		sessionUID  = "session-bind-before-control-uid"
	)
	if err := sqliteStore.CreateSession(ctx, &controlstore.SessionRecord{
		Namespace: namespace, Name: sessionName, SessionType: "task", CreatedAt: testNow, UpdatedAt: testNow,
	}); err != nil {
		t.Fatalf("CreateSession(): %v", err)
	}
	if err := sqliteStore.BindSessionCleanupIdentity(ctx, namespace, sessionName, sessionUID); err != nil {
		t.Fatalf("BindSessionCleanupIdentity(): %v", err)
	}
	request := controlstore.ReclaimSessionRequest{
		Namespace: namespace, SessionName: sessionName, Fence: fence,
		OperationID: "delete-bind-before-control", OperationDigest: testDigest("delete-bind-before-control"), RequestedAt: testNow,
	}
	if err := kubeStore.ReclaimSession(ctx, request); err != nil {
		t.Fatalf("ReclaimSession(bind-before-control crash): %v", err)
	}
	if _, err := sqliteStore.GetSession(ctx, namespace, sessionName); !errors.Is(err, controlstore.ErrNotFound) {
		t.Fatalf("GetSession() error = %v, want ErrNotFound", err)
	}
	completion, err := sqliteStore.GetSessionCleanupCompletion(ctx, namespace, sessionName)
	if err != nil || completion.SessionUID != sessionUID {
		t.Fatalf("completion = %#v, err=%v", completion, err)
	}
}

func TestReclaimSessionUsesLegacyTurnUIDAsIdentityProof(t *testing.T) {
	ctx := context.Background()
	kubeStore, _, sqliteStore, db, fence := newSessionCleanupTestStore(t, nil)
	control, _ := seedPublishedSessionCleanupState(t, ctx, kubeStore, sqliteStore, db, fence, "session-legacy-turn-proof")
	if _, err := db.ExecContext(ctx,
		`UPDATE sessions SET control_session_uid = '' WHERE namespace = ? AND name = ?`, control.Namespace, control.SessionName,
	); err != nil {
		t.Fatalf("clear migrated cleanup identity: %v", err)
	}
	request := controlstore.ReclaimSessionRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, Fence: fence,
		OperationID: "delete-legacy-turn-proof", OperationDigest: testDigest("delete-legacy-turn-proof"), RequestedAt: testNow,
	}
	if err := kubeStore.ReclaimSession(ctx, request); err != nil {
		t.Fatalf("ReclaimSession(legacy turn proof): %v", err)
	}
}

type failOnceSessionCleanupPersistence struct {
	*sqlitestore.Store
	err error
}

func (p *failOnceSessionCleanupPersistence) CompleteSessionCleanup(ctx context.Context, request controlstore.CompleteSessionCleanupRequest) error {
	if p.err != nil {
		err := p.err
		p.err = nil
		return err
	}
	return p.Store.CompleteSessionCleanup(ctx, request)
}

func newSessionCleanupTestStore(
	t *testing.T,
	failing *failOnceSessionCleanupPersistence,
) (*Store, client.Client, *sqlitestore.Store, *sql.DB, controlstore.ControllerEpochFence) {
	t.Helper()
	_, kubeClient, fence := newTestStoreWithEpoch(t)
	db, err := sqlitestore.NewDB(filepath.Join(t.TempDir(), "session-cleanup.db"))
	if err != nil {
		t.Fatalf("NewDB(): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sqliteStore := sqlitestore.NewStore(db, "")
	var persistence SQLitePersistence = sqliteStore
	if failing != nil {
		failing.Store = sqliteStore
		persistence = failing
	}
	kubeStore, err := NewComposite(kubeClient, testControlNamespace, persistence)
	if err != nil {
		t.Fatalf("NewComposite(): %v", err)
	}
	return kubeStore, kubeClient, sqliteStore, db, fence
}

func seedSessionCleanupState(
	t *testing.T,
	ctx context.Context,
	kubeStore *Store,
	sqliteStore *sqlitestore.Store,
	fence controlstore.ControllerEpochFence,
	name string,
) (*controlstore.SessionControl, *controlstore.BranchClaim) {
	t.Helper()
	if err := sqliteStore.CreateSession(ctx, &controlstore.SessionRecord{
		Namespace: "tenant-a", Name: name, SessionType: "task", CreatedAt: testNow, UpdatedAt: testNow,
	}); err != nil {
		t.Fatalf("CreateSession(): %v", err)
	}
	control, err := kubeStore.CreateSessionControl(ctx, &controlstore.SessionControl{
		Namespace: "tenant-a", SessionName: name, SessionUID: name + "-uid",
		RequestDigest: testDigest(name + "-control"), LeaseGeneration: 1,
		Availability: controlstore.SessionAvailable, CreatedAt: testNow,
	}, fence)
	if err != nil {
		t.Fatalf("CreateSessionControl(): %v", err)
	}
	claim, err := kubeStore.CreateBranchClaim(ctx, &controlstore.BranchClaim{
		RepositoryID: "github.com/orka/target", Ref: "refs/heads/" + name,
		OwnerKind: controlstore.BranchClaimOwnerSession, OwnerUID: control.SessionUID,
		LastVerified:  controlstore.RemoteRefState{SHA: strings.Repeat("a", 40)},
		RequestDigest: testDigest(name + "-branch"), CreatedAt: testNow,
	}, fence)
	if err != nil {
		t.Fatalf("CreateBranchClaim(): %v", err)
	}
	return control, claim
}

func seedPublishedSessionCleanupState(
	t *testing.T,
	ctx context.Context,
	kubeStore *Store,
	sqliteStore *sqlitestore.Store,
	db *sql.DB,
	fence controlstore.ControllerEpochFence,
	name string,
) (*controlstore.SessionControl, *controlstore.BranchClaim) {
	t.Helper()
	control, claim := seedSessionCleanupState(t, ctx, kubeStore, sqliteStore, fence, name)
	key := controlstore.SessionTurnKey{
		SessionUID: control.SessionUID, LeaseGeneration: control.LeaseGeneration,
		TaskUID: "task-published", Attempt: 1, PromptID: "prompt-published",
	}
	turnID, err := key.CanonicalID()
	if err != nil {
		t.Fatalf("CanonicalID(): %v", err)
	}
	projectionID := controlstore.CanonicalControlID("outbox", turnID, "TaskTerminalStatus")
	payloadDigest := controlstore.CanonicalBytesDigest([]byte(`{}`))
	if _, err := db.ExecContext(ctx, `INSERT INTO session_turns(
		id, namespace, session_name, session_uid, lease_generation, task_uid, attempt, prompt_id,
		prompt_attempt_id, request_digest, user_prompt, state, terminal_kind, terminal_content,
		finalization_digest, publication_id, controller_epoch_name, controller_epoch, version,
		created_at, finalized_at, updated_at, projection_id, projection_kind, projection_digest
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'publish', 'Finalized', 'AssistantResult', 'published', ?,
		'publication-cleanup', ?, ?, 2, ?, ?, ?, ?, 'TaskTerminalStatus', ?)`,
		turnID, control.Namespace, control.SessionName, key.SessionUID, key.LeaseGeneration, key.TaskUID, key.Attempt, key.PromptID,
		"attempt-published", testDigest("published-turn"), testDigest("published-finalization"),
		fence.Name, fence.Epoch, testNow, testNow.Add(time.Minute), testNow.Add(time.Minute),
		projectionID, payloadDigest,
	); err != nil {
		t.Fatalf("insert published SessionTurn: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO outbox_projections(
		id, aggregate_kind, aggregate_id, projection_kind, payload_digest, payload, state,
		initial_available_at, available_at, controller_epoch_name, controller_epoch, version,
		created_at, updated_at, delivered_at, delivery_digest
	) VALUES (?, ?, ?, 'TaskTerminalStatus', ?, '{}', 'Delivered', ?, ?, ?, ?, 1, ?, ?, ?, ?)`,
		projectionID, sessionTurnAggregateKind, turnID, payloadDigest, testNow, testNow,
		fence.Name, fence.Epoch, testNow, testNow, testNow, testDigest("published-projection-delivery"),
	); err != nil {
		t.Fatalf("insert delivered projection: %v", err)
	}
	return control, claim
}

func TestReclaimSessionRejectsWhitespaceAliasedIdentity(t *testing.T) {
	request := controlstore.ReclaimSessionRequest{
		Namespace: "tenant-a", SessionName: "session-cleanup ",
		Fence:       controlstore.ControllerEpochFence{Name: controlstore.DefaultControllerEpochName, Epoch: 1, HolderID: "controller-a"},
		OperationID: "delete-whitespace-alias", OperationDigest: testDigest("delete-whitespace-alias"), RequestedAt: testNow,
	}
	if err := normalizeReclaimSessionRequest(&request); !errors.Is(err, controlstore.ErrValidation) {
		t.Fatalf("normalizeReclaimSessionRequest() error = %v, want ErrValidation", err)
	}
}
