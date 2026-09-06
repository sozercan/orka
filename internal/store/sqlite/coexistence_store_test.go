/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

func TestAgentExecutionMigrationRemovesLegacySessionLineageProvenance(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy-lineage.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE session_lineages (
		namespace TEXT NOT NULL, session_name TEXT NOT NULL, namespace_uid TEXT NOT NULL,
		session_uid TEXT NOT NULL, contract_version TEXT NOT NULL,
		lineage_generation INTEGER NOT NULL, runtime_identity TEXT NOT NULL,
		config_digest TEXT NOT NULL, provenance TEXT NOT NULL, version INTEGER NOT NULL,
		created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
		PRIMARY KEY(namespace, session_name), UNIQUE(session_uid)
	)`); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 7, 9, 0, 0, 0, time.UTC)
	digest := store.CanonicalAgentExecutionSnapshotDigest([]byte("legacy-lineage"))
	if _, err := db.Exec(`INSERT INTO session_lineages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"tenant", "chat", "namespace-uid", "session-uid", "orka.harness.v2", 1,
		"codex", digest, "legacy-adopted", 1, now, now); err != nil {
		t.Fatal(err)
	}
	if err := migrateAgentExecution(db); err != nil {
		t.Fatalf("migrateAgentExecution: %v", err)
	}
	if sqliteTableHasColumn(t, db, "session_lineages", "provenance") {
		t.Fatal("legacy provenance column remains after static-mode migration")
	}
	var gotUID, gotDigest string
	if err := db.QueryRow(`SELECT session_uid, config_digest FROM session_lineages
		WHERE namespace = ? AND session_name = ?`, "tenant", "chat").Scan(&gotUID, &gotDigest); err != nil {
		t.Fatal(err)
	}
	if gotUID != "session-uid" || gotDigest != digest {
		t.Fatalf("migrated lineage = (%q, %q), want preserved identity", gotUID, gotDigest)
	}
}

var (
	_ store.AgentExecutionSnapshotStore          = (*Store)(nil)
	_ store.AgentExecutionSnapshotLifecycleStore = (*Store)(nil)
	_ store.SessionLineageStore                  = (*Store)(nil)
	_ store.HarnessV1AttemptStore                = (*Store)(nil)
)

func newCoexistenceTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "coexistence.db")
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewStore(db, dbPath)
}

func testSnapshotCipher(t *testing.T) *AgentExecutionSnapshotCipher {
	t.Helper()
	cipher, err := NewAgentExecutionSnapshotCipher(bytes.Repeat([]byte{0x42}, AgentExecutionSnapshotKeyBytes))
	if err != nil {
		t.Fatalf("NewAgentExecutionSnapshotCipher: %v", err)
	}
	return cipher
}

func persistLifecycleSnapshot(
	t *testing.T,
	ctx context.Context,
	s *Store,
	taskUID string,
	body []byte,
	createdAt time.Time,
) store.AgentExecutionSnapshotKey {
	t.Helper()
	key := store.AgentExecutionSnapshotKey{
		TaskUID: taskUID,
		Digest:  store.CanonicalAgentExecutionSnapshotDigest(body),
	}
	if err := s.PersistAgentExecutionSnapshot(ctx, store.AgentExecutionSnapshot{
		TaskUID:       key.TaskUID,
		Digest:        key.Digest,
		SchemaVersion: store.AgentExecutionSnapshotSchemaVersion,
		Body:          body,
		CreatedAt:     createdAt,
	}); err != nil {
		t.Fatalf("persist lifecycle snapshot %s: %v", key.ID(), err)
	}
	return key
}

func TestAgentExecutionSnapshotFailsClosedWithoutCipher(t *testing.T) {
	ctx := context.Background()
	s := newCoexistenceTestStore(t)
	body := []byte(`{"prompt":"resolved"}`)
	snapshot := store.AgentExecutionSnapshot{
		TaskUID:       "task-uid-1",
		Digest:        store.CanonicalAgentExecutionSnapshotDigest(body),
		SchemaVersion: store.AgentExecutionSnapshotSchemaVersion,
		Body:          body,
	}
	if err := s.PersistAgentExecutionSnapshot(ctx, snapshot); err == nil {
		t.Fatal("persist without a cipher must fail closed")
	}
	if _, err := s.GetAgentExecutionSnapshot(ctx, store.AgentExecutionSnapshotKey{TaskUID: "task-uid-1", Digest: snapshot.Digest}); err == nil {
		t.Fatal("get without a cipher must fail closed")
	}
}

func TestAgentExecutionSnapshotRoundTripEncryptedAtRest(t *testing.T) {
	ctx := context.Background()
	s := newCoexistenceTestStore(t)
	if err := s.SetAgentExecutionSnapshotCipher(testSnapshotCipher(t)); err != nil {
		t.Fatal(err)
	}

	body := []byte(`{"prompt":"SENSITIVE-RESOLVED-PROMPT","model":"provider/model"}`)
	snapshot := store.AgentExecutionSnapshot{
		TaskUID:       "task-uid-1",
		Digest:        store.CanonicalAgentExecutionSnapshotDigest(body),
		SchemaVersion: store.AgentExecutionSnapshotSchemaVersion,
		Body:          body,
	}
	if err := s.PersistAgentExecutionSnapshot(ctx, snapshot); err != nil {
		t.Fatalf("persist snapshot: %v", err)
	}
	// Idempotent identical persist.
	if err := s.PersistAgentExecutionSnapshot(ctx, snapshot); err != nil {
		t.Fatalf("idempotent persist: %v", err)
	}
	// Same key with different content is rejected.
	different := snapshot
	different.SchemaVersion = snapshot.SchemaVersion + 1
	if err := s.PersistAgentExecutionSnapshot(ctx, different); !errors.Is(err, store.ErrDuplicateMismatch) {
		t.Fatalf("expected ErrDuplicateMismatch, got %v", err)
	}

	got, err := s.GetAgentExecutionSnapshot(ctx, store.AgentExecutionSnapshotKey{TaskUID: "task-uid-1", Digest: snapshot.Digest})
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if !bytes.Equal(got.Body, body) || got.SchemaVersion != snapshot.SchemaVersion {
		t.Fatalf("snapshot round trip mismatch: %+v", got)
	}

	// The stored bytes must not contain the plaintext.
	var ciphertext []byte
	if err := s.db.QueryRow(`SELECT ciphertext FROM agent_execution_snapshots WHERE task_uid = ?`, "task-uid-1").Scan(&ciphertext); err != nil {
		t.Fatalf("read raw ciphertext: %v", err)
	}
	if bytes.Contains(ciphertext, []byte("SENSITIVE-RESOLVED-PROMPT")) {
		t.Fatal("snapshot body is stored in plaintext")
	}

	// Digest mismatch on persist is rejected.
	bad := snapshot
	bad.Digest = store.CanonicalAgentExecutionSnapshotDigest([]byte("other"))
	if err := s.PersistAgentExecutionSnapshot(ctx, bad); err == nil {
		t.Fatal("digest/body mismatch must be rejected")
	}

	if err := s.DeleteAgentExecutionSnapshots(ctx, "task-uid-1"); err != nil {
		t.Fatalf("delete snapshots: %v", err)
	}
	if _, err := s.GetAgentExecutionSnapshot(ctx, store.AgentExecutionSnapshotKey{TaskUID: "task-uid-1", Digest: snapshot.Digest}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestAgentExecutionSnapshotCipherActivationRejectsRotationWithRetainedSnapshots(t *testing.T) {
	ctx := context.Background()
	s := newCoexistenceTestStore(t)
	previousCipher := testSnapshotCipher(t)
	if err := s.SetAgentExecutionSnapshotCipher(previousCipher); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"prompt":"retained"}`)
	snapshot := store.AgentExecutionSnapshot{
		TaskUID:       "task-uid-rotation",
		Digest:        store.CanonicalAgentExecutionSnapshotDigest(body),
		SchemaVersion: store.AgentExecutionSnapshotSchemaVersion,
		Body:          body,
	}
	if err := s.PersistAgentExecutionSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}

	rotatedCipher, err := NewAgentExecutionSnapshotCipher(bytes.Repeat([]byte{0x43}, AgentExecutionSnapshotKeyBytes))
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewStore(s.db, "restart-with-rotated-key")
	if err := restarted.SetAgentExecutionSnapshotCipher(rotatedCipher); err == nil || !strings.Contains(err.Error(), "cannot authenticate retained snapshot") {
		t.Fatalf("rotated key activation error = %v", err)
	}
	if err := restarted.PersistAgentExecutionSnapshot(ctx, snapshot); !errors.Is(err, errSnapshotCipherRequired) {
		t.Fatalf("failed key activation must leave snapshot persistence closed, got %v", err)
	}

	restartedWithPreviousKey := NewStore(s.db, "restart-with-previous-key")
	if err := restartedWithPreviousKey.SetAgentExecutionSnapshotCipher(previousCipher); err != nil {
		t.Fatalf("activate previous key after restart: %v", err)
	}
	if _, err := restartedWithPreviousKey.GetAgentExecutionSnapshot(ctx, store.AgentExecutionSnapshotKey{
		TaskUID: snapshot.TaskUID,
		Digest:  snapshot.Digest,
	}); err != nil {
		t.Fatalf("read retained snapshot with previous key: %v", err)
	}

	if err := s.DeleteAgentExecutionSnapshots(ctx, snapshot.TaskUID); err != nil {
		t.Fatal(err)
	}
	restartedAfterRetention := NewStore(s.db, "restart-after-retention")
	if err := restartedAfterRetention.SetAgentExecutionSnapshotCipher(rotatedCipher); err != nil {
		t.Fatalf("activate rotated key after retained snapshots are removed: %v", err)
	}
}

func TestAgentExecutionSnapshotLifecycleMetadataOrderingAndStrictCutoff(t *testing.T) {
	ctx := context.Background()
	s := newCoexistenceTestStore(t)
	if err := s.SetAgentExecutionSnapshotCipher(testSnapshotCipher(t)); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 8, 6, 8, 0, 0, 0, time.UTC)
	early := persistLifecycleSnapshot(t, ctx, s, "task-z", []byte(`{"snapshot":"early"}`), base)
	tiedAt := base.Add(time.Minute)
	tied := []store.AgentExecutionSnapshotKey{
		persistLifecycleSnapshot(t, ctx, s, "task-b", []byte(`{"snapshot":"task-b"}`), tiedAt),
		persistLifecycleSnapshot(t, ctx, s, "task-a", []byte(`{"snapshot":"task-a-1"}`), tiedAt),
		persistLifecycleSnapshot(t, ctx, s, "task-a", []byte(`{"snapshot":"task-a-2"}`), tiedAt),
	}
	sort.Slice(tied, func(i, j int) bool {
		if tied[i].TaskUID != tied[j].TaskUID {
			return tied[i].TaskUID < tied[j].TaskUID
		}
		return tied[i].Digest < tied[j].Digest
	})
	cutoff := base.Add(2 * time.Minute)
	_ = persistLifecycleSnapshot(t, ctx, s, "task-boundary", []byte(`{"snapshot":"boundary"}`), cutoff)
	_ = persistLifecycleSnapshot(t, ctx, s, "task-late", []byte(`{"snapshot":"late"}`), cutoff.Add(time.Nanosecond))

	metadata, err := s.ListAgentExecutionSnapshotMetadataBefore(ctx, cutoff)
	if err != nil {
		t.Fatalf("ListAgentExecutionSnapshotMetadataBefore: %v", err)
	}
	wantKeys := append([]store.AgentExecutionSnapshotKey{early}, tied...)
	if len(metadata) != len(wantKeys) {
		t.Fatalf("metadata length = %d, want %d: %#v", len(metadata), len(wantKeys), metadata)
	}
	for index, wantKey := range wantKeys {
		if metadata[index].Key != wantKey {
			t.Fatalf("metadata[%d].Key = %#v, want %#v", index, metadata[index].Key, wantKey)
		}
		wantCreatedAt := tiedAt
		if index == 0 {
			wantCreatedAt = base
		}
		if !metadata[index].CreatedAt.Equal(wantCreatedAt) ||
			metadata[index].SchemaVersion != store.AgentExecutionSnapshotSchemaVersion {
			t.Fatalf("metadata[%d] = %#v, want createdAt=%s schemaVersion=%d",
				index, metadata[index], wantCreatedAt, store.AgentExecutionSnapshotSchemaVersion)
		}
	}
	if _, err := s.ListAgentExecutionSnapshotMetadataBefore(ctx, time.Time{}); err == nil {
		t.Fatal("zero metadata cutoff must fail validation")
	}
}

func TestAgentExecutionSnapshotLifecycleMetadataRejectsCorruptStoredIdentity(t *testing.T) {
	ctx := context.Background()
	s := newCoexistenceTestStore(t)
	now := time.Date(2026, 8, 6, 9, 0, 0, 0, time.UTC)
	if _, err := s.db.ExecContext(ctx, `INSERT INTO agent_execution_snapshots
		(task_uid, digest, schema_version, nonce, ciphertext, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		"task-corrupt", "not-a-canonical-digest", 1, []byte{1}, []byte{2}, now); err != nil {
		t.Fatalf("seed corrupt snapshot metadata: %v", err)
	}

	if _, err := s.ListAgentExecutionSnapshotMetadataBefore(ctx, now.Add(time.Minute)); err == nil ||
		!strings.Contains(err.Error(), "snapshot digest") {
		t.Fatalf("corrupt stored identity error = %v, want digest integrity failure", err)
	}
}

func TestAgentExecutionSnapshotLifecycleReferenceCounts(t *testing.T) {
	ctx := context.Background()
	s := newCoexistenceTestStore(t)
	if err := s.SetAgentExecutionSnapshotCipher(testSnapshotCipher(t)); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 6, 10, 0, 0, 0, time.UTC)
	key := persistLifecycleSnapshot(t, ctx, s, "task-uid", []byte(`{"snapshot":"target"}`), now)
	otherDigestKey := persistLifecycleSnapshot(t, ctx, s, key.TaskUID, []byte(`{"snapshot":"other"}`), now)
	otherTaskKey := persistLifecycleSnapshot(t, ctx, s, "other-task-uid", []byte(`{"snapshot":"target"}`), now)
	bindingDigest := store.CanonicalAgentExecutionSnapshotDigest([]byte("binding"))
	requestDigest := store.CanonicalAgentExecutionSnapshotDigest([]byte("request"))

	mustExec := func(statement string, args ...any) {
		t.Helper()
		if _, err := s.db.ExecContext(ctx, statement, args...); err != nil {
			t.Fatalf("seed snapshot reference: %v", err)
		}
	}
	seedHarnessAttempt := func(id, taskUID string, attempt int, snapshotDigest string) {
		mustExec(`INSERT INTO harness_v1_attempts
			(id, namespace, task_name, task_uid, attempt, binding_digest,
			 snapshot_digest, request_digest, turn_id, state, retry_class,
			 version, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'Prepared', 'none', 1, ?, ?)`,
			id, "ns", "task-"+id, taskUID, attempt, bindingDigest,
			snapshotDigest, requestDigest, "turn-"+id, now, now)
	}
	seedPromptAttempt := func(id, taskUID string, attempt int, snapshotDigest string) {
		mustExec(`INSERT INTO prompt_attempts
			(id, namespace, task_uid, attempt, prompt_id, request_digest,
			 binding_digest, snapshot_digest, execution_state, delivery_state,
			 controller_epoch_name, controller_epoch, version, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Queued', 'NotRequested', 'epoch', 1, 1, ?, ?)`,
			id, "ns", taskUID, attempt, "prompt-"+id, requestDigest,
			bindingDigest, snapshotDigest, now, now)
	}
	seedLineage := func(id, configDigest string) {
		mustExec(`INSERT INTO session_lineages
			(namespace, session_name, namespace_uid, session_uid, contract_version,
			 lineage_generation, runtime_identity, config_digest, version, created_at, updated_at)
			VALUES (?, ?, ?, ?, 'orka.harness.v2', 1, 'codex', ?, 1, ?, ?)`,
			"ns", "session-"+id, "namespace-uid", "session-uid-"+id,
			configDigest, now, now)
	}
	seedSessionTurn := func(id, taskUID string, attempt int) {
		sessionName := "turn-session-" + id
		mustExec(`INSERT INTO sessions(namespace, name, session_type) VALUES (?, ?, 'task')`, "ns", sessionName)
		mustExec(`INSERT INTO session_turns
			(id, namespace, session_name, session_uid, lease_generation, task_uid,
			 attempt, prompt_id, prompt_attempt_id, request_digest, user_prompt,
			 state, controller_epoch_name, controller_epoch, version, created_at, updated_at)
			VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, 'prompt', 'Open', 'epoch', 1, 1, ?, ?)`,
			"turn-"+id, "ns", sessionName, "turn-session-uid-"+id, taskUID, attempt,
			"turn-prompt-"+id, "turn-attempt-"+id, requestDigest, now, now)
	}

	seedHarnessAttempt("harness-match-1", key.TaskUID, 1, key.Digest)
	seedHarnessAttempt("harness-match-2", key.TaskUID, 2, key.Digest)
	seedHarnessAttempt("harness-other-digest", key.TaskUID, 3, otherDigestKey.Digest)
	seedHarnessAttempt("harness-other-task", otherTaskKey.TaskUID, 1, key.Digest)
	seedPromptAttempt("prompt-match-1", key.TaskUID, 1, key.Digest)
	seedPromptAttempt("prompt-match-2", key.TaskUID, 2, key.Digest)
	seedPromptAttempt("prompt-other-digest", key.TaskUID, 3, otherDigestKey.Digest)
	seedPromptAttempt("prompt-other-task", otherTaskKey.TaskUID, 1, key.Digest)
	seedSessionTurn("match-1", key.TaskUID, 1)
	seedSessionTurn("match-2", key.TaskUID, 2)
	seedSessionTurn("other-task", otherTaskKey.TaskUID, 1)
	// A lineage configuration digest is not an execution snapshot reference.
	seedLineage("match-1", key.Digest)
	seedLineage("match-2", key.Digest)
	seedLineage("other", otherDigestKey.Digest)

	counts, err := s.CountAgentExecutionSnapshotReferences(ctx, key)
	if err != nil {
		t.Fatalf("CountAgentExecutionSnapshotReferences: %v", err)
	}
	want := store.AgentExecutionSnapshotReferenceCounts{
		HarnessV1Attempts: 2,
		PromptAttempts:    2,
		SessionTurns:      2,
	}
	if counts != want || counts.Total() != 6 {
		t.Fatalf("reference counts = %#v (total %d), want %#v (total 6)", counts, counts.Total(), want)
	}

	otherTaskCounts, err := s.CountAgentExecutionSnapshotReferences(ctx, otherTaskKey)
	if err != nil {
		t.Fatalf("count other Task references: %v", err)
	}
	wantOtherTask := store.AgentExecutionSnapshotReferenceCounts{
		HarnessV1Attempts: 1,
		PromptAttempts:    1,
		SessionTurns:      1,
	}
	if otherTaskCounts != wantOtherTask {
		t.Fatalf("other Task reference counts = %#v, want %#v", otherTaskCounts, wantOtherTask)
	}
	if _, err := s.CountAgentExecutionSnapshotReferences(ctx, store.AgentExecutionSnapshotKey{}); err == nil {
		t.Fatal("incomplete snapshot key must fail reference-count validation")
	}
}

func TestAgentExecutionSnapshotLifecycleDeletesOnlyExactKey(t *testing.T) {
	ctx := context.Background()
	s := newCoexistenceTestStore(t)
	if err := s.SetAgentExecutionSnapshotCipher(testSnapshotCipher(t)); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 6, 11, 0, 0, 0, time.UTC)
	targetBody := []byte(`{"snapshot":"target"}`)
	target := persistLifecycleSnapshot(t, ctx, s, "task-uid", targetBody, now)
	sibling := persistLifecycleSnapshot(t, ctx, s, target.TaskUID, []byte(`{"snapshot":"sibling"}`), now)
	otherTask := persistLifecycleSnapshot(t, ctx, s, "other-task-uid", targetBody, now)

	if err := s.DeleteAgentExecutionSnapshot(ctx, target); err != nil {
		t.Fatalf("DeleteAgentExecutionSnapshot: %v", err)
	}
	if _, err := s.GetAgentExecutionSnapshot(ctx, target); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted target error = %v, want ErrNotFound", err)
	}
	for _, retained := range []store.AgentExecutionSnapshotKey{sibling, otherTask} {
		if _, err := s.GetAgentExecutionSnapshot(ctx, retained); err != nil {
			t.Fatalf("retained snapshot %s: %v", retained.ID(), err)
		}
	}
	if err := s.DeleteAgentExecutionSnapshot(ctx, target); err != nil {
		t.Fatalf("idempotent exact delete: %v", err)
	}
	if err := s.DeleteAgentExecutionSnapshot(ctx, store.AgentExecutionSnapshotKey{}); err == nil {
		t.Fatal("incomplete exact-delete key must fail validation")
	}
}

func TestSessionLineageProjectionIsIdempotentAndRejectsDivergence(t *testing.T) {
	ctx := context.Background()
	s := newCoexistenceTestStore(t)

	now := time.Date(2026, 8, 5, 9, 0, 0, 0, time.UTC)
	lineage := store.SessionLineage{
		Namespace:         "ns",
		SessionName:       "chat",
		NamespaceUID:      "ns-uid",
		SessionUID:        "session-uid",
		ContractVersion:   "orka.harness.v2",
		LineageGeneration: 1,
		RuntimeIdentity:   "codex",
		ConfigDigest:      store.CanonicalAgentExecutionSnapshotDigest([]byte("cfg")),
		Version:           1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	if _, err := s.GetSessionLineage(ctx, "ns", "chat"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound before projection, got %v", err)
	}

	projected, err := s.ProjectSessionLineage(ctx, lineage)
	if err != nil {
		t.Fatalf("project lineage: %v", err)
	}
	if projected.LineageGeneration != 1 || projected.Version != 1 {
		t.Fatalf("unexpected projected lineage: %+v", projected)
	}

	// An identical authoritative record projects idempotently.
	again, err := s.ProjectSessionLineage(ctx, lineage)
	if err != nil {
		t.Fatalf("verify projection: %v", err)
	}
	if again.SessionUID != lineage.SessionUID {
		t.Fatalf("verified lineage mismatch: %+v", again)
	}

	// Cross-protocol continuation is rejected.
	crossProtocol := lineage
	crossProtocol.ContractVersion = "orka.harness.v1"
	crossProtocol.RuntimeIdentity = "opencode"
	if _, err := s.ProjectSessionLineage(ctx, crossProtocol); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected ErrConflict for cross-protocol projection, got %v", err)
	}

	// A recreated same-name Session (new UID) never attaches to old state.
	recreated := lineage
	recreated.SessionUID = "different-session-uid"
	if _, err := s.ProjectSessionLineage(ctx, recreated); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected ErrConflict for recreated session UID, got %v", err)
	}

	// A recreated same-name namespace never attaches to old state.
	recreatedNamespace := lineage
	recreatedNamespace.NamespaceUID = "different-ns-uid"
	if _, err := s.ProjectSessionLineage(ctx, recreatedNamespace); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected ErrConflict for recreated namespace UID, got %v", err)
	}

	changedConfig := lineage
	changedConfig.ConfigDigest = store.CanonicalAgentExecutionSnapshotDigest([]byte("different-cfg"))
	if _, err := s.ProjectSessionLineage(ctx, changedConfig); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected ErrConflict for changed configuration, got %v", err)
	}
}

func testHarnessV1Attempt() *store.HarnessV1Attempt {
	digest := store.CanonicalAgentExecutionSnapshotDigest
	return &store.HarnessV1Attempt{
		Namespace:                 "ns",
		TaskName:                  "task-1",
		TaskUID:                   "task-uid-1",
		Attempt:                   1,
		BindingDigest:             digest([]byte("binding")),
		SnapshotDigest:            digest([]byte("snapshot")),
		RequestDigest:             digest([]byte("request")),
		TurnID:                    "turn-1",
		Backend:                   "harness-wrapper",
		AuthSecretNamespace:       "ns",
		AuthSecretName:            "harness-auth",
		AuthSecretKey:             "token",
		AuthSecretUID:             "secret-uid-1",
		AuthSecretResourceVersion: "17",
		State:                     store.HarnessV1AttemptPrepared,
		RetryClass:                store.HarnessV1RetryClassNone,
	}
}

func harnessV1Transition(
	key store.HarnessV1AttemptKey,
	fence store.ControllerEpochFence,
	version int64,
	from, to store.HarnessV1AttemptState,
	op string,
) store.HarnessV1AttemptTransition {
	return store.HarnessV1AttemptTransition{
		Key:             key,
		ExpectedVersion: version,
		ExpectedState:   from,
		TargetState:     to,
		OperationID:     op,
		OperationDigest: store.CanonicalAgentExecutionSnapshotDigest([]byte(op)),
		Fence:           fence,
	}
}

func TestHarnessV1AttemptLifecycleAndCAS(t *testing.T) {
	ctx := context.Background()
	s := newCoexistenceTestStore(t)
	fence := seedControlEpoch(t, s)
	attempt := testHarnessV1Attempt()
	key := store.HarnessV1AttemptKey{Namespace: attempt.Namespace, TaskUID: attempt.TaskUID, Attempt: attempt.Attempt}

	if err := s.CreateHarnessV1Attempt(ctx, attempt, fence); err != nil {
		t.Fatalf("create attempt: %v", err)
	}
	if err := s.CreateHarnessV1Attempt(ctx, attempt, fence); err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	different := *attempt
	different.RequestDigest = store.CanonicalAgentExecutionSnapshotDigest([]byte("other-request"))
	if err := s.CreateHarnessV1Attempt(ctx, &different, fence); !errors.Is(err, store.ErrDuplicateMismatch) {
		t.Fatalf("expected ErrDuplicateMismatch, got %v", err)
	}

	// Persist Submitting before writing StartTurn.
	current, err := s.TransitionHarnessV1Attempt(ctx, harnessV1Transition(key, fence, 1, store.HarnessV1AttemptPrepared, store.HarnessV1AttemptSubmitting, "submit"))
	if err != nil {
		t.Fatalf("Prepared->Submitting: %v", err)
	}
	if err := s.CreateHarnessV1Attempt(ctx, attempt, fence); err != nil {
		t.Fatalf("idempotent create after transition: %v", err)
	}
	// Replay of the same operation is idempotent.
	replay, err := s.TransitionHarnessV1Attempt(ctx, harnessV1Transition(key, fence, 1, store.HarnessV1AttemptPrepared, store.HarnessV1AttemptSubmitting, "submit"))
	if err != nil || replay.Version != current.Version {
		t.Fatalf("replay transition = %+v, %v", replay, err)
	}
	// Same operation ID with a different digest is rejected.
	conflicting := harnessV1Transition(key, fence, 1, store.HarnessV1AttemptPrepared, store.HarnessV1AttemptSubmitting, "submit")
	conflicting.OperationDigest = store.CanonicalAgentExecutionSnapshotDigest([]byte("tampered"))
	if _, err := s.TransitionHarnessV1Attempt(ctx, conflicting); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected ErrConflict for tampered replay, got %v", err)
	}

	// Stale version CAS fails.
	if _, err := s.TransitionHarnessV1Attempt(ctx, harnessV1Transition(key, fence, 1, store.HarnessV1AttemptSubmitting, store.HarnessV1AttemptAccepted, "accept-stale")); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected ErrConflict for stale version, got %v", err)
	}

	// Illegal transition fails.
	if _, err := s.TransitionHarnessV1Attempt(ctx, harnessV1Transition(key, fence, current.Version, store.HarnessV1AttemptSubmitting, store.HarnessV1AttemptSucceeded, "skip")); err == nil {
		t.Fatal("Submitting->Succeeded must be rejected")
	}

	sessionID := "runtime-session-1"
	acceptTransition := harnessV1Transition(key, fence, current.Version, store.HarnessV1AttemptSubmitting, store.HarnessV1AttemptAccepted, "accept")
	acceptTransition.Updates.RuntimeSessionID = &sessionID
	current, err = s.TransitionHarnessV1Attempt(ctx, acceptTransition)
	if err != nil || current.RuntimeSessionID != sessionID {
		t.Fatalf("Submitting->Accepted = %+v, %v", current, err)
	}

	// OutcomeUnknown requires a terminal reason.
	if _, err := s.TransitionHarnessV1Attempt(ctx, harnessV1Transition(key, fence, current.Version, store.HarnessV1AttemptAccepted, store.HarnessV1AttemptOutcomeUnknown, "unknown-no-reason")); err == nil {
		t.Fatal("OutcomeUnknown without a reason must be rejected")
	}

	reason := "RuntimeLost"
	unknownTransition := harnessV1Transition(key, fence, current.Version, store.HarnessV1AttemptAccepted, store.HarnessV1AttemptOutcomeUnknown, "unknown")
	unknownTransition.Updates.TerminalReason = &reason
	current, err = s.TransitionHarnessV1Attempt(ctx, unknownTransition)
	if err != nil || current.State != store.HarnessV1AttemptOutcomeUnknown {
		t.Fatalf("Accepted->OutcomeUnknown = %+v, %v", current, err)
	}

	// Terminal states admit no further transitions.
	if _, err := s.TransitionHarnessV1Attempt(ctx, harnessV1Transition(key, fence, current.Version, store.HarnessV1AttemptOutcomeUnknown, store.HarnessV1AttemptSucceeded, "resurrect")); err == nil {
		t.Fatal("OutcomeUnknown is terminal and must not transition")
	}

	attempts, err := s.ListHarnessV1AttemptsByTask(ctx, "ns", "task-uid-1")
	if err != nil || len(attempts) != 1 || !strings.Contains(string(attempts[0].State), "OutcomeUnknown") {
		t.Fatalf("attempts by task = %v, %v", attempts, err)
	}
}

func TestHarnessV1AttemptCreateRejectsEveryImmutableMismatch(t *testing.T) {
	digest := store.CanonicalAgentExecutionSnapshotDigest
	tests := []struct {
		name   string
		mutate func(*store.HarnessV1Attempt)
	}{
		{name: "task name", mutate: func(attempt *store.HarnessV1Attempt) { attempt.TaskName = "other-task" }},
		{name: "binding digest", mutate: func(attempt *store.HarnessV1Attempt) { attempt.BindingDigest = digest([]byte("other-binding")) }},
		{name: "snapshot digest", mutate: func(attempt *store.HarnessV1Attempt) { attempt.SnapshotDigest = digest([]byte("other-snapshot")) }},
		{name: "request digest", mutate: func(attempt *store.HarnessV1Attempt) { attempt.RequestDigest = digest([]byte("other-request")) }},
		{name: "turn ID", mutate: func(attempt *store.HarnessV1Attempt) { attempt.TurnID = "other-turn" }},
		{name: "backend", mutate: func(attempt *store.HarnessV1Attempt) { attempt.Backend = "external-runtime" }},
		{name: "auth namespace", mutate: func(attempt *store.HarnessV1Attempt) { attempt.AuthSecretNamespace = "other-ns" }},
		{name: "auth name", mutate: func(attempt *store.HarnessV1Attempt) { attempt.AuthSecretName = "other-auth" }},
		{name: "auth key", mutate: func(attempt *store.HarnessV1Attempt) { attempt.AuthSecretKey = "other-token" }},
		{name: "auth UID", mutate: func(attempt *store.HarnessV1Attempt) { attempt.AuthSecretUID = "other-secret-uid" }},
		{name: "auth resource version", mutate: func(attempt *store.HarnessV1Attempt) { attempt.AuthSecretResourceVersion = "18" }},
		{name: "duplicate safety", mutate: func(attempt *store.HarnessV1Attempt) { attempt.DuplicateSafe = true }},
		{name: "retry class", mutate: func(attempt *store.HarnessV1Attempt) { attempt.RetryClass = store.HarnessV1RetryClassDuplicateSafe }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			s := newCoexistenceTestStore(t)
			fence := seedControlEpoch(t, s)
			attempt := testHarnessV1Attempt()
			if err := s.CreateHarnessV1Attempt(ctx, attempt, fence); err != nil {
				t.Fatalf("create attempt: %v", err)
			}
			candidate := *attempt
			tt.mutate(&candidate)
			if err := s.CreateHarnessV1Attempt(ctx, &candidate, fence); !errors.Is(err, store.ErrDuplicateMismatch) {
				t.Fatalf("create with mismatched %s = %v, want ErrDuplicateMismatch", tt.name, err)
			}
		})
	}
}

func TestHarnessV1AttemptCreateAndTransitionRequireCurrentControllerEpoch(t *testing.T) {
	ctx := context.Background()
	s := newCoexistenceTestStore(t)
	fence := seedControlEpoch(t, s)
	attempt := testHarnessV1Attempt()
	key := store.HarnessV1AttemptKey{Namespace: attempt.Namespace, TaskUID: attempt.TaskUID, Attempt: attempt.Attempt}

	wrongHolder := fence
	wrongHolder.HolderID = "controller-b"
	if err := s.CreateHarnessV1Attempt(ctx, attempt, wrongHolder); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("create with wrong holder = %v, want ErrConflict", err)
	}
	if err := s.CreateHarnessV1Attempt(ctx, attempt, fence); err != nil {
		t.Fatalf("create attempt: %v", err)
	}

	epoch, err := s.GetControllerEpoch(ctx, fence.Name)
	if err != nil {
		t.Fatalf("get controller epoch: %v", err)
	}
	advanced, err := s.CompareAndSwapControllerEpoch(ctx, store.ControllerEpochCAS{
		Name:            epoch.Name,
		ExpectedVersion: epoch.Version,
		ExpectedEpoch:   epoch.Epoch,
		NewEpoch:        epoch.Epoch + 1,
		HolderID:        "controller-b",
		RequestDigest:   controlTestDigest("harness-v1-epoch-2"),
		UpdatedAt:       time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("advance controller epoch: %v", err)
	}
	currentFence := store.ControllerEpochFence{Name: advanced.Name, Epoch: advanced.Epoch, HolderID: advanced.HolderID}

	if err := s.CreateHarnessV1Attempt(ctx, attempt, fence); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("idempotent create with stale fence = %v, want ErrConflict", err)
	}
	transition := harnessV1Transition(key, fence, 1, store.HarnessV1AttemptPrepared, store.HarnessV1AttemptSubmitting, "stale-submit")
	if _, err := s.TransitionHarnessV1Attempt(ctx, transition); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("transition with stale fence = %v, want ErrConflict", err)
	}
	transition = harnessV1Transition(key, currentFence, 1, store.HarnessV1AttemptPrepared, store.HarnessV1AttemptSubmitting, "current-submit")
	if _, err := s.TransitionHarnessV1Attempt(ctx, transition); err != nil {
		t.Fatalf("transition with current fence: %v", err)
	}
}

func TestHarnessV1AttemptSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "coexistence-restart.db")
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	s := NewStore(db, dbPath)
	fence := seedControlEpoch(t, s)
	attempt := testHarnessV1Attempt()
	if err := s.CreateHarnessV1Attempt(ctx, attempt, fence); err != nil {
		t.Fatalf("create attempt: %v", err)
	}
	key := store.HarnessV1AttemptKey{Namespace: attempt.Namespace, TaskUID: attempt.TaskUID, Attempt: attempt.Attempt}
	if _, err := s.TransitionHarnessV1Attempt(ctx, harnessV1Transition(key, fence, 1, store.HarnessV1AttemptPrepared, store.HarnessV1AttemptSubmitting, "submit")); err != nil {
		t.Fatalf("transition: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	restarted := NewStore(reopened, dbPath)
	got, err := restarted.GetHarnessV1Attempt(ctx, key)
	if err != nil || got.State != store.HarnessV1AttemptSubmitting || got.Version != 2 {
		t.Fatalf("attempt after restart = %+v, %v", got, err)
	}
}
