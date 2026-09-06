/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/harness"
	"github.com/orka-agents/orka/internal/store"
)

const (
	runtimeSessionTestNamespace = "runtime-ns"
	runtimeSessionTestName      = "runtime-session"
	runtimeSessionTestTask      = "runtime-task"
	runtimeSessionTestAgent     = "runtime-agent"
	runtimeSessionNamespaceA    = "runtime-ns-a"
	runtimeSessionNamespaceB    = "runtime-ns-b"
)

func TestRuntimeSessionStoreCreateGetRoundTrip(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	createdAt := time.Date(2026, 6, 24, 10, 0, 0, 123, time.FixedZone("offset", -7*60*60))
	updatedAt := createdAt.Add(time.Minute)
	session := runtimeSessionFixture("runtime-a")
	session.CreatedAt = createdAt
	session.UpdatedAt = updatedAt
	session.IdleTimeout = 5 * time.Minute
	session.MaxLifetime = 2 * time.Hour

	if err := s.CreateRuntimeSession(ctx, &session); err != nil {
		t.Fatalf("CreateRuntimeSession: %v", err)
	}
	if !session.CreatedAt.Equal(createdAt.UTC()) || !session.UpdatedAt.Equal(updatedAt.UTC()) {
		t.Fatalf("normalized timestamps = %s/%s, want UTC %s/%s", session.CreatedAt, session.UpdatedAt, createdAt.UTC(), updatedAt.UTC())
	}

	got, err := s.GetRuntimeSession(ctx, runtimeSessionTestNamespace, "runtime-a")
	if err != nil {
		t.Fatalf("GetRuntimeSession: %v", err)
	}
	assertRuntimeSessionEqual(t, *got, session)
}

func TestRuntimeSessionStoreCreateDefaults(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	session := runtimeSessionFixture(" runtime-defaults ")
	session.Owner.Namespace = " " + runtimeSessionTestNamespace + " "
	session.Owner.SessionName = " " + runtimeSessionTestName + " "
	session.Owner.ActiveTask = " " + runtimeSessionTestTask + " "
	session.Owner.AgentName = " " + runtimeSessionTestAgent + " "
	session.Owner.Provider = " kubernetes-service "
	session.State = ""
	session.CleanupPolicy = ""

	if err := s.CreateRuntimeSession(ctx, &session); err != nil {
		t.Fatalf("CreateRuntimeSession: %v", err)
	}
	if session.ID != "runtime-defaults" || session.Owner.Namespace != runtimeSessionTestNamespace || session.Owner.SessionName != runtimeSessionTestName {
		t.Fatalf("normalized identity = %#v", session)
	}
	if session.State != harness.RuntimeSessionStatePending {
		t.Fatalf("state = %q, want Pending", session.State)
	}
	if session.CleanupPolicy != harness.RuntimeCleanupPolicyDelete {
		t.Fatalf("cleanup policy = %q, want delete", session.CleanupPolicy)
	}
	if session.CreatedAt.IsZero() || session.UpdatedAt.IsZero() || !session.UpdatedAt.Equal(session.CreatedAt) {
		t.Fatalf("timestamps = %s/%s, want populated equal values", session.CreatedAt, session.UpdatedAt)
	}

	got, err := s.GetRuntimeSession(ctx, runtimeSessionTestNamespace, "runtime-defaults")
	if err != nil {
		t.Fatalf("GetRuntimeSession: %v", err)
	}
	assertRuntimeSessionEqual(t, *got, session)
}

func TestRuntimeSessionStoreNamespaceOwnership(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	nsA := runtimeSessionFixture("runtime-shared")
	nsA.Owner.Namespace = runtimeSessionNamespaceA
	nsA.Owner.SessionName = runtimeSessionTestName
	nsB := runtimeSessionFixture("runtime-shared")
	nsB.Owner.Namespace = runtimeSessionNamespaceB
	nsB.Owner.SessionName = "session-b"

	if err := s.CreateRuntimeSession(ctx, &nsA); err != nil {
		t.Fatalf("CreateRuntimeSession ns-a: %v", err)
	}
	if err := s.CreateRuntimeSession(ctx, &nsB); err != nil {
		t.Fatalf("CreateRuntimeSession ns-b: %v", err)
	}

	gotA, err := s.GetRuntimeSession(ctx, runtimeSessionNamespaceA, "runtime-shared")
	if err != nil {
		t.Fatalf("GetRuntimeSession ns-a: %v", err)
	}
	gotB, err := s.GetRuntimeSession(ctx, runtimeSessionNamespaceB, "runtime-shared")
	if err != nil {
		t.Fatalf("GetRuntimeSession ns-b: %v", err)
	}
	if gotA.Owner.Namespace != runtimeSessionNamespaceA || gotA.Owner.SessionName != runtimeSessionTestName {
		t.Fatalf("ns-a row = %#v", gotA)
	}
	if gotB.Owner.Namespace != runtimeSessionNamespaceB || gotB.Owner.SessionName != "session-b" {
		t.Fatalf("ns-b row = %#v", gotB)
	}
	if _, err := s.GetRuntimeSession(ctx, "ns-c", "runtime-shared"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetRuntimeSession ns-c error = %v, want ErrNotFound", err)
	}
	duplicate := runtimeSessionFixture("runtime-shared")
	duplicate.Owner.Namespace = runtimeSessionNamespaceA
	if err := s.CreateRuntimeSession(ctx, &duplicate); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("CreateRuntimeSession duplicate error = %v, want ErrConflict", err)
	}
}

func TestRuntimeSessionStoreTransitionValidatesStateMachine(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	session := runtimeSessionFixture("runtime-transition")
	if err := s.CreateRuntimeSession(ctx, &session); err != nil {
		t.Fatalf("CreateRuntimeSession: %v", err)
	}

	transitionAt := time.Date(2026, 6, 24, 11, 0, 0, 0, time.UTC)
	updated, err := s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{
		Namespace: runtimeSessionTestNamespace,
		ID:        session.ID,
		From:      harness.RuntimeSessionStatePending,
		To:        harness.RuntimeSessionStateBooting,
		UpdatedAt: transitionAt,
	})
	if err != nil {
		t.Fatalf("TransitionRuntimeSession: %v", err)
	}
	if updated.State != harness.RuntimeSessionStateBooting || !updated.UpdatedAt.Equal(transitionAt) {
		t.Fatalf("updated session = %#v, want Booting at transition time", updated)
	}

	_, err = s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{
		Namespace: runtimeSessionTestNamespace,
		ID:        session.ID,
		From:      harness.RuntimeSessionStateBooting,
		To:        harness.RuntimeSessionStateTurnRunning,
		UpdatedAt: transitionAt.Add(time.Minute),
	})
	if !errors.Is(err, store.ErrValidation) {
		t.Fatalf("invalid TransitionRuntimeSession error = %v, want ErrValidation", err)
	}
	got, err := s.GetRuntimeSession(ctx, runtimeSessionTestNamespace, session.ID)
	if err != nil {
		t.Fatalf("GetRuntimeSession after invalid transition: %v", err)
	}
	if got.State != harness.RuntimeSessionStateBooting || !got.UpdatedAt.Equal(transitionAt) {
		t.Fatalf("session changed after invalid transition: %#v", got)
	}
}

func TestRuntimeSessionStoreTransitionUsesExpectedFromState(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	session := runtimeSessionFixture("runtime-cas")
	if err := s.CreateRuntimeSession(ctx, &session); err != nil {
		t.Fatalf("CreateRuntimeSession: %v", err)
	}
	if _, err := s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{
		Namespace: runtimeSessionTestNamespace,
		ID:        session.ID,
		From:      harness.RuntimeSessionStatePending,
		To:        harness.RuntimeSessionStateBooting,
	}); err != nil {
		t.Fatalf("initial TransitionRuntimeSession: %v", err)
	}

	_, err := s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{
		Namespace: runtimeSessionTestNamespace,
		ID:        session.ID,
		From:      harness.RuntimeSessionStatePending,
		To:        harness.RuntimeSessionStateFailed,
	})
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale TransitionRuntimeSession error = %v, want ErrConflict", err)
	}
	got, err := s.GetRuntimeSession(ctx, runtimeSessionTestNamespace, session.ID)
	if err != nil {
		t.Fatalf("GetRuntimeSession: %v", err)
	}
	if got.State != harness.RuntimeSessionStateBooting {
		t.Fatalf("state = %q, want Booting after stale transition", got.State)
	}
}

func TestRuntimeSessionStoreTransitionCanSetAndClearActiveTask(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	session := runtimeSessionFixture("runtime-active-task")
	session.State = harness.RuntimeSessionStateReady
	session.Owner.ActiveTask = ""
	if err := s.CreateRuntimeSession(ctx, &session); err != nil {
		t.Fatalf("CreateRuntimeSession: %v", err)
	}

	activeTask := runtimeSessionTestTask
	updated, err := s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{
		Namespace:  runtimeSessionTestNamespace,
		ID:         session.ID,
		From:       harness.RuntimeSessionStateReady,
		To:         harness.RuntimeSessionStateTurnRunning,
		ActiveTask: &activeTask,
	})
	if err != nil {
		t.Fatalf("set active task transition: %v", err)
	}
	if updated.Owner.ActiveTask != runtimeSessionTestTask {
		t.Fatalf("active task = %q, want runtime task", updated.Owner.ActiveTask)
	}

	updated, err = s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{
		Namespace: runtimeSessionTestNamespace,
		ID:        session.ID,
		From:      harness.RuntimeSessionStateTurnRunning,
		To:        harness.RuntimeSessionStateIdle,
	})
	if err != nil {
		t.Fatalf("preserve active task transition: %v", err)
	}
	if updated.Owner.ActiveTask != runtimeSessionTestTask {
		t.Fatalf("active task = %q, want preserved runtime task", updated.Owner.ActiveTask)
	}

	if _, err := s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{
		Namespace: runtimeSessionTestNamespace,
		ID:        session.ID,
		From:      harness.RuntimeSessionStateIdle,
		To:        harness.RuntimeSessionStateTurnRunning,
	}); err != nil {
		t.Fatalf("back to running transition: %v", err)
	}
	clearActiveTask := ""
	updated, err = s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{
		Namespace:  runtimeSessionTestNamespace,
		ID:         session.ID,
		From:       harness.RuntimeSessionStateTurnRunning,
		To:         harness.RuntimeSessionStateIdle,
		ActiveTask: &clearActiveTask,
	})
	if err != nil {
		t.Fatalf("clear active task transition: %v", err)
	}
	if updated.Owner.ActiveTask != "" {
		t.Fatalf("active task = %q, want cleared", updated.Owner.ActiveTask)
	}
}

func TestRuntimeSessionStoreDeleteRequiresDeletedState(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	session := runtimeSessionFixture("runtime-delete")
	if err := s.CreateRuntimeSession(ctx, &session); err != nil {
		t.Fatalf("CreateRuntimeSession: %v", err)
	}
	if err := s.DeleteRuntimeSession(ctx, runtimeSessionTestNamespace, session.ID); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("DeleteRuntimeSession active error = %v, want ErrValidation", err)
	}
	if _, err := s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{Namespace: runtimeSessionTestNamespace, ID: session.ID, From: harness.RuntimeSessionStatePending, To: harness.RuntimeSessionStateDeleting}); err != nil {
		t.Fatalf("transition to deleting: %v", err)
	}
	if _, err := s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{Namespace: runtimeSessionTestNamespace, ID: session.ID, From: harness.RuntimeSessionStateDeleting, To: harness.RuntimeSessionStateDeleted}); err != nil {
		t.Fatalf("transition to deleted: %v", err)
	}
	if err := s.DeleteRuntimeSession(ctx, runtimeSessionTestNamespace, session.ID); err != nil {
		t.Fatalf("DeleteRuntimeSession deleted: %v", err)
	}
	if _, err := s.GetRuntimeSession(ctx, runtimeSessionTestNamespace, session.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetRuntimeSession after delete error = %v, want ErrNotFound", err)
	}
}

func TestRuntimeSessionStoreValidationErrors(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	assertValidationError := func(name string, fn func() error) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			if err := fn(); !errors.Is(err, store.ErrValidation) {
				t.Fatalf("error = %v, want ErrValidation", err)
			}
		})
	}

	assertValidationError("nil create", func() error { return s.CreateRuntimeSession(ctx, nil) })
	assertValidationError("empty id", func() error {
		session := runtimeSessionFixture("")
		return s.CreateRuntimeSession(ctx, &session)
	})
	assertValidationError("empty namespace", func() error {
		session := runtimeSessionFixture("runtime-invalid")
		session.Owner.Namespace = ""
		return s.CreateRuntimeSession(ctx, &session)
	})
	assertValidationError("empty session", func() error {
		session := runtimeSessionFixture("runtime-invalid")
		session.Owner.SessionName = ""
		return s.CreateRuntimeSession(ctx, &session)
	})
	assertValidationError("empty provider", func() error {
		session := runtimeSessionFixture("runtime-invalid")
		session.Owner.Provider = ""
		return s.CreateRuntimeSession(ctx, &session)
	})
	assertValidationError("unknown state", func() error {
		session := runtimeSessionFixture("runtime-invalid")
		session.State = "Mystery"
		return s.CreateRuntimeSession(ctx, &session)
	})
	assertValidationError("unknown cleanup policy", func() error {
		session := runtimeSessionFixture("runtime-invalid")
		session.CleanupPolicy = "archive"
		return s.CreateRuntimeSession(ctx, &session)
	})
	assertValidationError("negative idle timeout", func() error {
		session := runtimeSessionFixture("runtime-invalid")
		session.IdleTimeout = -time.Second
		return s.CreateRuntimeSession(ctx, &session)
	})
	assertValidationError("negative max lifetime", func() error {
		session := runtimeSessionFixture("runtime-invalid")
		session.MaxLifetime = -time.Second
		return s.CreateRuntimeSession(ctx, &session)
	})
	assertValidationError("get empty namespace", func() error {
		_, err := s.GetRuntimeSession(ctx, "", "runtime")
		return err
	})
	assertValidationError("get empty id", func() error {
		_, err := s.GetRuntimeSession(ctx, runtimeSessionTestNamespace, "")
		return err
	})
	assertValidationError("transition empty namespace", func() error {
		_, err := s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{ID: "runtime", From: harness.RuntimeSessionStatePending, To: harness.RuntimeSessionStateBooting})
		return err
	})
	assertValidationError("transition invalid state", func() error {
		_, err := s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{Namespace: runtimeSessionTestNamespace, ID: "runtime", From: "Mystery", To: harness.RuntimeSessionStateBooting})
		return err
	})
	assertValidationError("delete empty namespace", func() error { return s.DeleteRuntimeSession(ctx, "", "runtime") })

	if _, err := s.GetRuntimeSession(ctx, runtimeSessionTestNamespace, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetRuntimeSession missing error = %v, want ErrNotFound", err)
	}
	if _, err := s.TransitionRuntimeSession(ctx, harness.RuntimeSessionTransition{Namespace: runtimeSessionTestNamespace, ID: "missing", From: harness.RuntimeSessionStatePending, To: harness.RuntimeSessionStateBooting}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("TransitionRuntimeSession missing error = %v, want ErrNotFound", err)
	}
	if err := s.DeleteRuntimeSession(ctx, runtimeSessionTestNamespace, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("DeleteRuntimeSession missing error = %v, want ErrNotFound", err)
	}
}

func runtimeSessionFixture(id harness.RuntimeSessionID) harness.RuntimeSession {
	return harness.RuntimeSession{
		ID: id,
		Owner: harness.RuntimeSessionOwner{
			Namespace:   runtimeSessionTestNamespace,
			SessionName: runtimeSessionTestName,
			ActiveTask:  runtimeSessionTestTask,
			AgentName:   runtimeSessionTestAgent,
			Provider:    harness.ProviderKindKubernetesService,
		},
		State:         harness.RuntimeSessionStatePending,
		CleanupPolicy: harness.RuntimeCleanupPolicyDelete,
	}
}

func assertRuntimeSessionEqual(t *testing.T, got, want harness.RuntimeSession) {
	t.Helper()
	if got.ID != want.ID || got.Owner != want.Owner || got.State != want.State || got.CleanupPolicy != want.CleanupPolicy {
		t.Fatalf("session identity/state = %#v, want %#v", got, want)
	}
	if got.IdleTimeout != want.IdleTimeout || got.MaxLifetime != want.MaxLifetime {
		t.Fatalf("session durations = %s/%s, want %s/%s", got.IdleTimeout, got.MaxLifetime, want.IdleTimeout, want.MaxLifetime)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("session times = %s/%s, want %s/%s", got.CreatedAt, got.UpdatedAt, want.CreatedAt, want.UpdatedAt)
	}
}
