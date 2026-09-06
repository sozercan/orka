package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
	"github.com/orka-agents/orka/internal/taskterminal"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	acpSessionTestSHA  = "0123456789012345678901234567890123456789"
	acpSessionTestSHA2 = "abcdefabcdefabcdefabcdefabcdefabcdefabcd"
)

func TestTaskSessionProjectionExecutionPreservesFrozenIdentity(t *testing.T) {
	transition := metav1.NewTime(time.Date(2026, 8, 11, 22, 9, 40, 0, time.UTC))
	execution := &corev1alpha1.TaskExecutionStatus{
		State: corev1alpha1.TaskExecutionStateRunning, Attempt: 1, PromptID: "prompt-session-terminal",
		RuntimePoolName: "codex-pool", RuntimePoolUID: "pool-uid", RuntimeInstanceID: "runtime-instance",
		RuntimeSessionUID: "session-uid", RuntimeSessionGeneration: 3, RuntimeSessionSupervisorBootID: "boot-id",
		RuntimeSessionProfileDigest: acpSessionTestDigest("profile"), RuntimeSessionMCPDigest: acpSessionTestDigest("mcp"),
		RuntimeSessionWorkspaceDigest: acpSessionTestDigest("workspace"), RuntimeSessionRecreationPending: true,
		RuntimeSessionCleanupDigest: acpSessionTestDigest("cleanup"), RequestDigest: acpSessionTestDigest("request"),
		ControllerEpoch: 7, ReadCredentialResourceVersion: "read-rv", PublicationReadCredentialResourceVersion: "target-read-rv",
		PublicationCredentialResourceVersion: "target-write-rv", ForgeCredentialResourceVersion: "forge-rv",
		Message: "running", LastTransitionTime: &transition,
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "orka-system", Name: "session-terminal", UID: types.UID("task-session-terminal")},
		Spec: corev1alpha1.TaskSpec{
			Type:       corev1alpha1.TaskTypeAgent,
			SessionRef: &corev1alpha1.SessionReference{Name: "session-transcript", Create: true},
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseCancelled, Execution: execution,
			Delivery: &corev1alpha1.TaskDeliveryStatus{
				State: corev1alpha1.TaskDeliveryStateNotRequested, Outcome: corev1alpha1.TaskDeliveryOutcomeNotRequested,
			},
		},
	}
	terminal := corev1alpha1.TaskExecutionStatus{
		State: corev1alpha1.TaskExecutionStateCancelled, Outcome: corev1alpha1.TaskExecutionOutcomeCancelled,
		Attempt: 1, PromptID: execution.PromptID, Reason: "Cancelled", Message: "prompt cancelled",
	}
	projected, err := taskSessionProjectionExecution(task, terminal)
	if err != nil {
		t.Fatalf("taskSessionProjectionExecution() error = %v", err)
	}
	expected := *execution.DeepCopy()
	expected.State = terminal.State
	expected.Outcome = terminal.Outcome
	expected.Reason = terminal.Reason
	expected.Message = terminal.Message
	if !reflect.DeepEqual(projected, expected) {
		t.Fatalf("projected execution = %#v, want %#v", projected, expected)
	}
	attempt := &store.PromptAttempt{
		ID: "prompt-attempt-session-terminal",
		Key: store.PromptAttemptKey{
			Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: execution.PromptID,
		},
		RequestDigest: execution.RequestDigest, RuntimeInstanceID: execution.RuntimeInstanceID,
		SessionUID: execution.RuntimeSessionUID, SessionLeaseGeneration: execution.RuntimeSessionGeneration,
		ExecutionState: store.PromptExecutionCancelled, DeliveryState: store.PromptDeliveryNotRequested,
	}
	payload, err := json.Marshal(taskTerminalProjection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: string(task.UID), Attempt: 1,
		Phase: task.Status.Phase, Execution: projected, Delivery: task.Status.Delivery,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := taskterminal.ValidateRestoredProjection(payload, task, string(task.UID), attempt); err != nil {
		t.Fatalf("new Session terminal projection failed reclamation validation: %v", err)
	}

	terminal.Attempt = 2
	if _, err := taskSessionProjectionExecution(task, terminal); err == nil {
		t.Fatal("mismatched terminal attempt was accepted")
	}
}

func TestACPSessionContinuityConcurrentLeaseDenial(t *testing.T) {
	ctx := context.Background()
	s, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "concurrent.db"))
	defer closeStore()
	continuity := newACPSessionTestContinuity(t, s, ACPBootstrapLimits{})
	control := ensureACPSessionForTest(t, continuity, fence, "concurrent")

	start := make(chan struct{})
	results := make(chan error, 2)
	for _, taskUID := range []string{"task-a", "task-b"} {
		go func() {
			<-start
			_, err := continuity.AcquireMutationLease(ctx, ACPAcquireSessionLeaseRequest{
				Session: *control, Fence: fence, TaskUID: taskUID, Attempt: 1,
				PromptID: "prompt-" + taskUID, PromptRequestDigest: acpSessionTestDigest("prompt-" + taskUID),
				AcquiredAt: time.Date(2026, 7, 24, 17, 0, 0, 0, time.UTC),
			})
			results <- err
		}()
	}
	close(start)
	var succeeded, conflicted int
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, store.ErrConflict):
			conflicted++
		default:
			t.Fatalf("lease result error = %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("lease results succeeded=%d conflicted=%d", succeeded, conflicted)
	}
	current, err := s.GetSessionControl(ctx, control.Namespace, control.SessionName)
	if err != nil {
		t.Fatal(err)
	}
	if current.LeaseGeneration != 1 || current.Lease == nil {
		t.Fatalf("current lease = %#v", current)
	}
}

func TestACPSessionContinuityRejectsStaleLeaseGeneration(t *testing.T) {
	ctx := context.Background()
	s, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "stale.db"))
	defer closeStore()
	continuity := newACPSessionTestContinuity(t, s, ACPBootstrapLimits{})
	control := ensureACPSessionForTest(t, continuity, fence, "stale")
	turn, attempt := openACPSessionTurnForTest(t, continuity, s, fence, control, "task-stale", "prompt-stale", "first prompt")
	completeACPAttemptExecutionForTest(t, s, fence, attempt, true)
	finalized, err := continuity.FinalizeOutcomeUnknown(ctx, ACPFinalizeOutcomeUnknownRequest{
		SessionTurn: *turn, Fence: fence, Reason: "runtime settlement could not be proven",
		Projection:  acpSessionProjectionForTest("stale-final", "OutcomeUnknown"),
		FinalizedAt: time.Date(2026, 7, 24, 17, 5, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	stale := finalized.Session
	stale.LeaseGeneration--
	if _, err := continuity.AcquireMutationLease(ctx, ACPAcquireSessionLeaseRequest{
		Session: stale, Fence: fence, TaskUID: "task-next", Attempt: 1, PromptID: "prompt-next",
		PromptRequestDigest: acpSessionTestDigest("prompt-next"),
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale generation error = %v, want ErrConflict", err)
	}
}

func TestACPSessionContinuityRestartContinuity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart.db")
	s, fence, closeStore := newACPSessionTestStore(t, path)
	continuity := newACPSessionTestContinuity(t, s, ACPBootstrapLimits{})
	control := ensureACPSessionForTest(t, continuity, fence, "restart")
	turn, attempt := openACPSessionTurnForTest(t, continuity, s, fence, control, "task-restart", "prompt-restart", "remember this")
	completeACPAttemptExecutionForTest(t, s, fence, attempt, false)
	finalized, err := continuity.FinalizeAssistantResult(ctx, ACPFinalizeAssistantRequest{
		SessionTurn: *turn, Fence: fence, AssistantResult: "durable answer",
		Projection:  acpSessionProjectionForTest("restart-final", "Succeeded"),
		FinalizedAt: time.Date(2026, 7, 24, 17, 10, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := continuity.BuildBootstrapTranscript(ctx, finalized.Session)
	if err != nil {
		t.Fatal(err)
	}
	closeStore()

	db, err := sqlite.NewDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	restartedStore := sqlite.NewStore(db, path)
	fence = advanceACPSessionTestEpoch(t, restartedStore, "controller-restarted")
	restarted := newACPSessionTestContinuity(t, restartedStore, ACPBootstrapLimits{})
	loaded, err := restarted.EnsureSession(ctx, ACPEnsureSessionRequest{
		Namespace: "ns", SessionName: "restart", SessionType: "task",
		ExpectedSessionUID: control.SessionUID, Fence: fence,
	})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SessionUID != control.SessionUID {
		t.Fatalf("restarted Session UID = %q, want %q", loaded.SessionUID, control.SessionUID)
	}
	after, err := restarted.BuildBootstrapTranscript(ctx, *loaded)
	if err != nil {
		t.Fatal(err)
	}
	if after.Digest != before.Digest || string(after.Artifact) != string(before.Artifact) || after.MessageCount != 2 {
		t.Fatalf("restart bootstrap changed: before=%#v after=%#v", before, after)
	}
	profile := harnessv2.ProfileDigest(acpSessionTestDigest("profile-a"))
	mcpDigest := acpSessionTestDigest("mcp-a")
	plan, err := PlanACPRuntimeSession(*loaded, &ACPRuntimeSessionBinding{
		SessionUID: loaded.SessionUID, Generation: 1, ProfileDigest: profile, MCPDigest: mcpDigest,
		RuntimeInstanceID: "old-instance", SupervisorBootID: "old-boot",
	}, profile, mcpDigest, "new-instance", "new-boot")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Binding.Generation != 2 || !plan.Recreate || !plan.BootstrapRequired || plan.Reason != "runtime-lost" {
		t.Fatalf("runtime-loss plan = %#v", plan)
	}
}

//nolint:goconst // Repeated role literals make transcript assertions explicit.
func TestACPSessionContinuityOutcomeUnknownMarkerHasNoAssistantContent(t *testing.T) {
	ctx := context.Background()
	s, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "unknown.db"))
	defer closeStore()
	continuity := newACPSessionTestContinuity(t, s, ACPBootstrapLimits{})
	control := ensureACPSessionForTest(t, continuity, fence, "unknown")
	turn, attempt := openACPSessionTurnForTest(t, continuity, s, fence, control, "task-unknown", "prompt-unknown", "do not replay")
	completeACPAttemptExecutionForTest(t, s, fence, attempt, true)
	result, err := continuity.FinalizeOutcomeUnknown(ctx, ACPFinalizeOutcomeUnknownRequest{
		SessionTurn: *turn, Fence: fence, Reason: "ambiguous prompt acceptance",
		Projection: acpSessionProjectionForTest("unknown-final", "OutcomeUnknown"),
	})
	if err != nil {
		t.Fatal(err)
	}
	transcript, err := s.LoadTranscript(ctx, control.Namespace, control.SessionName, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(transcript) != 2 || transcript[0].Role != "user" || transcript[1].Role != "system" {
		t.Fatalf("OutcomeUnknown transcript = %#v", transcript)
	}
	for _, message := range transcript {
		if message.Role == "assistant" {
			t.Fatalf("invented assistant transcript entry: %#v", message)
		}
	}
	var marker acpOutcomeUnknownMarker
	if err := json.Unmarshal([]byte(transcript[1].Content), &marker); err != nil {
		t.Fatal(err)
	}
	if marker.Kind != "OutcomeUnknown" || marker.AssistantResultRecorded || marker.Reason != "ambiguous prompt acceptance" {
		t.Fatalf("outcome marker = %#v", marker)
	}
	if result.Session.Lease != nil || result.Session.Availability != store.SessionAvailable {
		t.Fatalf("resolved unknown prompt session = %#v", result.Session)
	}
}

//nolint:gocyclo // The table of generation-rotation invariants is intentionally exercised together.
func TestPlanACPRuntimeSessionProfileGenerationRotation(t *testing.T) {
	session := store.SessionControl{SessionUID: "immutable-session", Availability: store.SessionAvailable}
	profileA := harnessv2.ProfileDigest(acpSessionTestDigest("profile-a"))
	profileB := harnessv2.ProfileDigest(acpSessionTestDigest("profile-b"))
	mcpA := acpSessionTestDigest("mcp-a")
	mcpB := acpSessionTestDigest("mcp-b")
	initial, err := PlanACPRuntimeSession(session, nil, profileA, mcpA, "instance-a", "boot-a")
	if err != nil {
		t.Fatal(err)
	}
	if initial.Binding.Generation != 1 || !initial.Recreate || !initial.BootstrapRequired {
		t.Fatalf("initial plan = %#v", initial)
	}
	reused, err := PlanACPRuntimeSession(session, &initial.Binding, profileA, mcpA, "instance-a", "boot-a")
	if err != nil {
		t.Fatal(err)
	}
	if reused.Binding.Generation != 1 || reused.Recreate || reused.BootstrapRequired {
		t.Fatalf("reuse plan = %#v", reused)
	}
	rotated, err := PlanACPRuntimeSession(session, &initial.Binding, profileB, mcpA, "instance-a", "boot-a")
	if err != nil {
		t.Fatal(err)
	}
	if rotated.Binding.Generation != 2 || rotated.Binding.ProfileDigest != profileB || !rotated.Recreate || !rotated.BootstrapRequired || rotated.Reason != "runtime-profile-rotated" {
		t.Fatalf("rotated plan = %#v", rotated)
	}
	mcpRotated, err := PlanACPRuntimeSession(session, &initial.Binding, profileA, mcpB, "instance-a", "boot-a")
	if err != nil {
		t.Fatal(err)
	}
	if mcpRotated.Binding.Generation != 2 || mcpRotated.Binding.MCPDigest != mcpB ||
		!mcpRotated.Recreate || mcpRotated.Reason != "runtime-mcp-rotated" {
		t.Fatalf("MCP rotation plan = %#v", mcpRotated)
	}
	pendingBinding := rotated.Binding
	pendingBinding.Generation = 7
	pendingBinding.WorkspaceDigest = acpSessionTestDigest("pending-workspace")
	pendingBinding.RecreationRequired = true
	pending, err := PlanACPRuntimeSession(session, &pendingBinding, profileA, mcpA, "instance-b", "boot-b")
	if err != nil {
		t.Fatal(err)
	}
	if pending.Binding.Generation != pendingBinding.Generation+1 || !pending.Binding.RecreationRequired ||
		pending.Binding.ProfileDigest != profileA || pending.Binding.RuntimeInstanceID != "instance-b" ||
		pending.Binding.SupervisorBootID != "boot-b" || !pending.Recreate || !pending.BootstrapRequired ||
		pending.Reason != "runtime-session-recreation-pending" {
		t.Fatalf("pending recreation plan = %#v", pending)
	}
	sameSupervisorRotated, err := PlanACPRuntimeSession(
		session, &pendingBinding, profileA, mcpA, pendingBinding.RuntimeInstanceID, pendingBinding.SupervisorBootID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if sameSupervisorRotated.Binding.Generation != pendingBinding.Generation+1 || !sameSupervisorRotated.Recreate {
		t.Fatalf("same-supervisor pending rotation plan = %#v", sameSupervisorRotated)
	}
	bootRotated, err := PlanACPRuntimeSession(
		session, &pendingBinding, pendingBinding.ProfileDigest, pendingBinding.MCPDigest, pendingBinding.RuntimeInstanceID, "replacement-boot",
	)
	if err != nil {
		t.Fatal(err)
	}
	if bootRotated.Binding.Generation != pendingBinding.Generation+1 || !bootRotated.Recreate {
		t.Fatalf("boot-only pending recreation plan = %#v", bootRotated)
	}
	exhaustedPending := pendingBinding
	exhaustedPending.Generation = maxControllerRuntimeSessionGeneration
	if _, err := PlanACPRuntimeSession(session, &exhaustedPending, profileB, mcpA, "replacement-instance", "replacement-boot"); err == nil {
		t.Fatal("pending recreation migrated an exhausted generation to a replacement runtime")
	}
}

func TestEnforceACPRuntimeSessionGenerationFloor(t *testing.T) {
	live := ACPRuntimeSessionPlan{Binding: ACPRuntimeSessionBinding{Generation: 3}}
	got, err := enforceACPRuntimeSessionGenerationFloor(live, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got.Binding.Generation != 3 || got.Recreate || got.BootstrapRequired {
		t.Fatalf("live plan at durable floor = %#v, want unchanged reuse", got)
	}

	recreate := ACPRuntimeSessionPlan{Binding: ACPRuntimeSessionBinding{Generation: 3}, Recreate: true}
	got, err = enforceACPRuntimeSessionGenerationFloor(recreate, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got.Binding.Generation != 4 || !got.Recreate || !got.BootstrapRequired || got.Reason != "durable-workspace-generation-floor" {
		t.Fatalf("recreation plan at durable floor = %#v, want generation 4 bootstrap", got)
	}

	if _, err := enforceACPRuntimeSessionGenerationFloor(recreate, maxControllerRuntimeSessionGeneration); err == nil {
		t.Fatal("recreation advanced an exhausted durable workspace generation")
	}
}

func TestRuntimeSessionBindingFromTaskStatus(t *testing.T) {
	digest := acpSessionTestDigest("persisted-workspace")
	profileDigest := harnessv2.ProfileDigest(acpSessionTestDigest("profile"))
	mcpDigest := acpSessionTestDigest("mcp")
	task := &corev1alpha1.Task{Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
		RuntimeInstanceID:               "old-instance",
		RuntimeSessionUID:               "session-uid",
		RuntimeSessionGeneration:        7,
		RuntimeSessionSupervisorBootID:  "old-boot",
		RuntimeSessionProfileDigest:     string(profileDigest),
		RuntimeSessionMCPDigest:         mcpDigest,
		RuntimeSessionWorkspaceDigest:   digest,
		RuntimeSessionRecreationPending: true,
	}}}
	binding, err := runtimeSessionBindingFromTaskStatus(
		task, "session-uid", harnessv2.ProfileDigest(acpSessionTestDigest("new-profile")), "new-instance", "new-boot",
	)
	if err != nil {
		t.Fatal(err)
	}
	if binding == nil || binding.Generation != 7 || binding.RuntimeInstanceID != "old-instance" ||
		binding.SupervisorBootID != "old-boot" || binding.ProfileDigest != profileDigest ||
		binding.MCPDigest != mcpDigest || binding.WorkspaceDigest != digest || !binding.RecreationRequired {
		t.Fatalf("persisted RuntimeSession binding = %#v", binding)
	}
}

func TestACPRuntimeSessionWorkspaceBindingRotation(t *testing.T) {
	base := harnessv2.WorkspaceSpec{
		Intent: harnessv2.WorkspaceIntentRead,
		Baseline: harnessv2.WorkspaceBaseline{
			RepositoryIdentity: "github.com/orka-agents/orka",
			Revision:           acpSessionTestSHA,
			TreeDigest:         acpSessionTestDigest("workspace-tree"),
		},
		RelativeRoot: "website",
	}
	baseDigest, err := acpRuntimeWorkspaceBindingDigest("refs/heads/main", base)
	if err != nil {
		t.Fatal(err)
	}
	changedRefDigest, err := acpRuntimeWorkspaceBindingDigest("refs/heads/release", base)
	if err != nil {
		t.Fatal(err)
	}
	changedRepository := base
	changedRepository.Baseline.RepositoryIdentity = "github.com/orka-agents/other"
	changedRepositoryDigest, err := acpRuntimeWorkspaceBindingDigest("refs/heads/main", changedRepository)
	if err != nil {
		t.Fatal(err)
	}
	changedBaseline := base
	changedBaseline.Baseline.Revision = acpSessionTestSHA2
	changedBaselineDigest, err := acpRuntimeWorkspaceBindingDigest("refs/heads/main", changedBaseline)
	if err != nil {
		t.Fatal(err)
	}
	changedRoot := base
	changedRoot.RelativeRoot = "docs"
	changedRootDigest, err := acpRuntimeWorkspaceBindingDigest("refs/heads/main", changedRoot)
	if err != nil {
		t.Fatal(err)
	}
	for name, digest := range map[string]string{
		"source ref": changedRefDigest, "repository": changedRepositoryDigest,
		"baseline": changedBaselineDigest, "relative root": changedRootDigest,
	} {
		if digest == baseDigest {
			t.Fatalf("%s change did not rotate workspace binding digest", name)
		}
	}

	binding := ACPRuntimeSessionBinding{SessionUID: "session", Generation: 3, WorkspaceDigest: baseDigest}
	same, rotated, err := bindACPRuntimeSessionWorkspace(binding, true, baseDigest)
	if err != nil {
		t.Fatal(err)
	}
	if rotated || same != binding {
		t.Fatalf("matching workspace binding = %#v, rotated=%v", same, rotated)
	}
	updated, rotated, err := bindACPRuntimeSessionWorkspace(binding, true, changedRootDigest)
	if err != nil {
		t.Fatal(err)
	}
	if !rotated || updated.Generation != 4 || updated.WorkspaceDigest != changedRootDigest {
		t.Fatalf("changed workspace binding = %#v, rotated=%v", updated, rotated)
	}
	initial, rotated, err := bindACPRuntimeSessionWorkspace(
		ACPRuntimeSessionBinding{SessionUID: "new-session", Generation: 1}, false, baseDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	if rotated || initial.Generation != 1 || initial.WorkspaceDigest != baseDigest {
		t.Fatalf("initial workspace binding = %#v, rotated=%v", initial, rotated)
	}
	if _, _, err := bindACPRuntimeSessionWorkspace(
		ACPRuntimeSessionBinding{SessionUID: "exhausted", Generation: maxControllerRuntimeSessionGeneration, WorkspaceDigest: baseDigest},
		true, changedRootDigest,
	); err == nil {
		t.Fatal("expected workspace binding generation exhaustion")
	}
	pendingWorkspace, rotated, err := bindACPRuntimeSessionWorkspace(
		ACPRuntimeSessionBinding{
			SessionUID: "pending", Generation: 4, WorkspaceDigest: baseDigest, RecreationRequired: true,
		}, false, changedRootDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	if rotated || pendingWorkspace.Generation != 5 || pendingWorkspace.WorkspaceDigest != changedRootDigest {
		t.Fatalf("pending workspace rotation = %#v, rotated=%v", pendingWorkspace, rotated)
	}
}

func TestPrepareRuntimeWorkspaceDoesNotCollapseMissingSessionIdentity(t *testing.T) {
	dispatcher := &ACPDispatcher{}
	firstTask := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{UID: types.UID("empty-task-a")}}
	secondTask := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{UID: types.UID("empty-task-b")}}
	first, err := dispatcher.prepareRuntimeWorkspace(context.Background(), firstTask, store.ControllerEpochFence{}, &acpTaskSession{}, time.Now().UTC(), false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := dispatcher.prepareRuntimeWorkspace(context.Background(), secondTask, store.ControllerEpochFence{}, &acpTaskSession{}, time.Now().UTC(), false)
	if err != nil {
		t.Fatal(err)
	}
	if first.baseline.RepositoryIdentity == second.baseline.RepositoryIdentity {
		t.Fatal("task-scoped empty workspace baselines unexpectedly share an identity")
	}
	if first.bindingDigest == second.bindingDigest {
		t.Fatal("an incomplete Session binding must not erase the task-scoped workspace identity")
	}
}

func TestACPSessionContinuityBlockedSessionRecovery(t *testing.T) {
	ctx := context.Background()
	s, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "blocked.db"))
	defer closeStore()
	continuity := newACPSessionTestContinuity(t, s, ACPBootstrapLimits{})
	control := ensureACPSessionForTest(t, continuity, fence, "blocked")
	turn, attempt := openACPSessionTurnForTest(t, continuity, s, fence, control, "task-blocked", "prompt-blocked", "publish this")
	claim, publication := createACPOutcomeUnknownPublicationForTest(t, s, fence, turn.Lease, time.Date(2026, 7, 24, 18, 0, 0, 0, time.UTC))
	attempt = completeACPAttemptExecutionForTest(t, s, fence, attempt, false)
	completeACPAttemptDeliveryForTest(t, s, fence, attempt, store.PromptDeliveryPublicationOutcomeUnknown)
	finalized, err := continuity.FinalizeAssistantResult(ctx, ACPFinalizeAssistantRequest{
		SessionTurn: *turn, Fence: fence, AssistantResult: "the local change was prepared",
		PublicationID: publication.ID, Projection: acpSessionProjectionForTest("blocked-final", "PublicationOutcomeUnknown"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if finalized.Session.Availability != store.SessionReconciliationBlocked || finalized.Session.Lease != nil || finalized.Session.VerifiedBaseline != nil {
		t.Fatalf("blocked finalization = %#v", finalized.Session)
	}
	if _, err := continuity.BuildBootstrapTranscript(ctx, finalized.Session); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("blocked bootstrap error = %v, want ErrConflict", err)
	}
	receipt, err := NewACPIndependentBranchReceipt(
		"reconcile-blocked", claim.RepositoryID, claim.Ref, acpSessionTestSHA2,
		time.Date(2026, 7, 24, 18, 10, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	tampered := receipt
	tampered.SHA = acpSessionTestSHA
	if _, err := continuity.RecoverBlockedSession(ctx, ACPRecoverBlockedSessionRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, SessionUID: control.SessionUID,
		Fence: fence, Receipt: tampered,
	}); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("tampered independent receipt error = %v, want ErrValidation", err)
	}
	recovered, err := continuity.RecoverBlockedSession(ctx, ACPRecoverBlockedSessionRequest{
		Namespace: control.Namespace, SessionName: control.SessionName, SessionUID: control.SessionUID,
		Fence: fence, Receipt: receipt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Availability != store.SessionAvailable || recovered.VerifiedBaseline == nil || recovered.VerifiedBaseline.SHA != acpSessionTestSHA2 {
		t.Fatalf("recovered session = %#v", recovered)
	}
	recoveredClaim, err := s.GetBranchClaim(ctx, claim.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredClaim.Availability != store.BranchClaimAvailable || recoveredClaim.LastVerified.SHA != acpSessionTestSHA2 {
		t.Fatalf("recovered branch claim = %#v", recoveredClaim)
	}
	next, err := continuity.AcquireMutationLease(ctx, ACPAcquireSessionLeaseRequest{
		Session: *recovered, Fence: fence, TaskUID: "task-after-recovery", Attempt: 1, PromptID: "prompt-after-recovery",
		PromptRequestDigest: acpSessionTestDigest("prompt-after-recovery"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if next.Key.LeaseGeneration != 2 {
		t.Fatalf("post-recovery lease generation = %d, want 2", next.Key.LeaseGeneration)
	}
}

func TestACPBootstrapTranscriptIsCanonicalAndBounded(t *testing.T) {
	messages := []store.SessionMessage{
		{Role: "user", Content: "old"}, {Role: "assistant", Content: "old answer"},
		{Role: "user", Content: strings.Repeat("x", 600)}, {Role: "assistant", Content: "new answer"},
	}
	limits := ACPBootstrapLimits{MaxMessages: 2, MaxBytes: 1024, MaxMessageBytes: 256}
	first, err := buildACPBootstrapTranscript(messages, limits)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildACPBootstrapTranscript(messages, limits)
	if err != nil {
		t.Fatal(err)
	}
	if first.MessageCount != 2 || !first.Truncated || first.Messages[0].Role != "user" || first.Messages[1].Content != "new answer" {
		t.Fatalf("bounded bootstrap = %#v", first)
	}
	if first.Digest != second.Digest || string(first.Artifact) != string(second.Artifact) || len(first.Artifact) > limits.MaxBytes {
		t.Fatalf("bootstrap is not deterministic/bounded")
	}
}

func newACPSessionTestStore(t *testing.T, path string) (*sqlite.Store, store.ControllerEpochFence, func()) {
	t.Helper()
	db, err := sqlite.NewDB(path)
	if err != nil {
		t.Fatal(err)
	}
	s := sqlite.NewStore(db, path)
	epoch, err := s.CompareAndSwapControllerEpoch(context.Background(), store.ControllerEpochCAS{
		ExpectedVersion: 0, ExpectedEpoch: 0, NewEpoch: 1, HolderID: "controller-test",
		RequestDigest: acpSessionTestDigest("epoch-1"), UpdatedAt: time.Date(2026, 7, 24, 16, 0, 0, 0, time.UTC),
	})
	if err != nil {
		db.Close() //nolint:errcheck
		t.Fatal(err)
	}
	return s, store.ControllerEpochFence{Name: epoch.Name, Epoch: epoch.Epoch, HolderID: epoch.HolderID}, func() {
		_ = db.Close()
	}
}

func advanceACPSessionTestEpoch(t *testing.T, s *sqlite.Store, holder string) store.ControllerEpochFence {
	t.Helper()
	current, err := s.GetControllerEpoch(context.Background(), store.DefaultControllerEpochName)
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.CompareAndSwapControllerEpoch(context.Background(), store.ControllerEpochCAS{
		ExpectedVersion: current.Version, ExpectedEpoch: current.Epoch, NewEpoch: current.Epoch + 1,
		HolderID: holder, RequestDigest: acpSessionTestDigest("epoch-" + holder), UpdatedAt: current.UpdatedAt.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	return store.ControllerEpochFence{Name: next.Name, Epoch: next.Epoch, HolderID: next.HolderID}
}

//nolint:unparam // The stable parameter keeps call sites explicit across related test cases.
func newACPSessionTestContinuity(t *testing.T, s *sqlite.Store, limits ACPBootstrapLimits) *ACPSessionContinuity {
	t.Helper()
	continuity, err := NewACPSessionContinuity(ACPSessionContinuityConfig{
		SessionControls: s, Transcripts: s, Publications: s, BranchClaims: s, BootstrapLimits: limits,
		NewSessionUID: func() (string, error) { return "acp-session-fixed-uid", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return continuity
}

func ensureACPSessionForTest(t *testing.T, continuity *ACPSessionContinuity, fence store.ControllerEpochFence, name string) *store.SessionControl {
	t.Helper()
	control, err := continuity.EnsureSession(context.Background(), ACPEnsureSessionRequest{
		Namespace: "ns", SessionName: name, SessionType: "task", Fence: fence,
		CreatedAt: time.Date(2026, 7, 24, 16, 30, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	return control
}

func openACPSessionTurnForTest(
	t *testing.T,
	continuity *ACPSessionContinuity,
	s *sqlite.Store,
	fence store.ControllerEpochFence,
	control *store.SessionControl,
	taskUID, promptID, userPrompt string,
) (*ACPSessionTurn, *store.PromptAttempt) {
	t.Helper()
	ctx := context.Background()
	requestDigest := acpSessionTestDigest("request-" + taskUID)
	lease, err := continuity.AcquireMutationLease(ctx, ACPAcquireSessionLeaseRequest{
		Session: *control, Fence: fence, TaskUID: taskUID, Attempt: 1, PromptID: promptID,
		PromptRequestDigest: requestDigest, AcquiredAt: time.Date(2026, 7, 24, 16, 40, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	key := store.PromptAttemptKey{Namespace: control.Namespace, TaskUID: taskUID, Attempt: 1, PromptID: promptID}
	attemptID, err := key.CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := s.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		ID: attemptID, Key: key, SessionUID: control.SessionUID, SessionLeaseGeneration: lease.Key.LeaseGeneration,
		RequestDigest: requestDigest, CreatedAt: time.Date(2026, 7, 24, 16, 41, 0, 0, time.UTC),
	}), fence)
	if err != nil {
		t.Fatal(err)
	}
	turn, err := continuity.OpenTurn(ctx, ACPOpenSessionTurnRequest{
		Lease: *lease, Fence: fence, PromptAttemptID: attempt.ID,
		PromptRequestDigest: requestDigest, UserPrompt: userPrompt,
		OpenedAt: time.Date(2026, 7, 24, 16, 42, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	return turn, attempt
}

func completeACPAttemptExecutionForTest(t *testing.T, s *sqlite.Store, fence store.ControllerEpochFence, attempt *store.PromptAttempt, unknown bool) *store.PromptAttempt {
	t.Helper()
	path := []store.PromptExecutionState{
		store.PromptExecutionReserved, store.PromptExecutionSessionStarting, store.PromptExecutionPlanned,
		store.PromptExecutionSubmitting,
	}
	if unknown {
		path = append(path, store.PromptExecutionSubmittedUnknown, store.PromptExecutionOutcomeUnknown)
	} else {
		path = append(path, store.PromptExecutionAccepted, store.PromptExecutionRunning, store.PromptExecutionSettling, store.PromptExecutionSucceeded)
	}
	for _, next := range path {
		op := "execution-" + attempt.Key.TaskUID + "-" + string(next)
		transition := store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: next, OperationID: op, OperationDigest: acpSessionTestDigest(op),
			UpdatedAt: attempt.UpdatedAt.Add(time.Second),
		}
		if next == store.PromptExecutionOutcomeUnknown {
			transition.TerminalReason = "runtime outcome unknown"
			transition.OutcomeMarker = "OutcomeUnknown"
		}
		updated, err := s.TransitionPromptAttemptExecution(context.Background(), transition)
		if err != nil {
			t.Fatalf("transition execution to %s: %v", next, err)
		}
		attempt = updated
	}
	return attempt
}

func completeACPAttemptDeliveryForTest(t *testing.T, s *sqlite.Store, fence store.ControllerEpochFence, attempt *store.PromptAttempt, terminal store.PromptDeliveryState) *store.PromptAttempt {
	t.Helper()
	path := []store.PromptDeliveryState{
		store.PromptDeliveryValidating, store.PromptDeliveryPreparing, store.PromptDeliveryPrepared,
		store.PromptDeliveryPublishing, store.PromptDeliveryVerifying, terminal,
	}
	for _, next := range path {
		op := "delivery-" + attempt.Key.TaskUID + "-" + string(next)
		updated, err := s.TransitionPromptAttemptDelivery(context.Background(), store.PromptAttemptDeliveryTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.DeliveryState,
			NewState: next, OperationID: op, OperationDigest: acpSessionTestDigest(op),
			TerminalReason: "delivery terminal", UpdatedAt: attempt.UpdatedAt.Add(time.Second),
		})
		if err != nil {
			t.Fatalf("transition delivery to %s: %v", next, err)
		}
		attempt = updated
	}
	return attempt
}

func createACPOutcomeUnknownPublicationForTest(t *testing.T, s *sqlite.Store, fence store.ControllerEpochFence, lease ACPSessionLease, now time.Time) (*store.BranchClaim, *store.Publication) {
	t.Helper()
	ctx := context.Background()
	claim, err := s.CreateBranchClaim(ctx, &store.BranchClaim{
		RepositoryID: "github:orka/acp-session", Ref: "refs/heads/orka/acp-session",
		OwnerKind: store.BranchClaimOwnerSession, OwnerUID: lease.Session.SessionUID, Generation: 1,
		LastVerified: store.RemoteRefState{Absent: true}, RequestDigest: acpSessionTestDigest("claim"), CreatedAt: now,
	}, fence)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := s.CreatePublication(ctx, &store.Publication{
		ID: "publication-blocked", Namespace: lease.Session.Namespace, Generation: 1,
		TaskUID: lease.Key.TaskUID, Attempt: lease.Key.Attempt, PromptID: lease.Key.PromptID,
		SessionUID: lease.Session.SessionUID, BranchClaimID: claim.ID, BranchClaimGeneration: claim.Generation,
		SourceRepositoryID: "github:orka/source", SourceBaselineSHA: acpSessionTestSHA, SourceRef: "refs/heads/main",
		TargetRepositoryID: claim.RepositoryID, TargetRef: claim.Ref, Baseline: claim.LastVerified,
		ArtifactID: "sha256-artifact", ArtifactDigest: acpSessionTestDigest("artifact"), ArtifactSizeBytes: 128,
		ArtifactMediaType: "application/vnd.orka.workspace-delta.v1+tar", PublicationCredentialRef: "secret/ns/publisher#token",
		CommitIdentity: "Orka <orka@example.invalid>", CommitMessage: "feat: ACP session change", CommitTimestamp: now,
		RequestDigest: acpSessionTestDigest("publication"), CreatedAt: now,
	}, fence)
	if err != nil {
		t.Fatal(err)
	}
	prepared := store.PreparedPublicationReceipt{
		OperationID: "prepare", RequestDigest: acpSessionTestDigest("prepare-response"),
		TreeSHA: acpSessionTestSHA2, CommitSHA: acpSessionTestSHA, ManifestDigest: acpSessionTestDigest("manifest"),
		BundleArtifactID: "bundle-artifact", BundleDigest: acpSessionTestDigest("bundle"), BundleSizeBytes: 256,
		BundleMediaType: store.PreparedBundleMediaType, BundleRef: "refs/orka/publications/" + strings.Repeat("f", 64),
		PreparedAt: now.Add(time.Minute),
	}
	publication = transitionACPPublicationForTest(t, s, fence, publication, store.PublicationPrepared, "prepare", now.Add(time.Minute), &prepared, nil, nil, "")
	publication = transitionACPPublicationForTest(t, s, fence, publication, store.PublicationPublishing, "publishing", now.Add(2*time.Minute), nil, nil, nil, "")
	publish := store.PublishOperationReceipt{
		OperationID: "publish", RequestDigest: acpSessionTestDigest("publish"),
		TargetRepositoryID: publication.TargetRepositoryID, TargetRef: publication.TargetRef,
		RemoteBefore: publication.Baseline, ExpectedCommitSHA: prepared.CommitSHA,
		AcknowledgementUnknown: true, PublishedAt: now.Add(3 * time.Minute),
	}
	publication = transitionACPPublicationForTest(t, s, fence, publication, store.PublicationVerifying, publish.OperationID, now.Add(3*time.Minute), nil, &publish, nil, "")
	verification := store.PublicationVerificationReceipt{
		OperationID: "verify", RequestDigest: acpSessionTestDigest("verify"),
		Outcome: store.PublicationOutcomeUnknown, ExpectedCommitSHA: prepared.CommitSHA, VerifiedAt: now.Add(4 * time.Minute),
	}
	publication = transitionACPPublicationForTest(t, s, fence, publication, store.PublicationOutcomeUnknown, verification.OperationID, now.Add(4*time.Minute), nil, nil, &verification, "remote ref remained unobservable")
	return claim, publication
}

func transitionACPPublicationForTest(
	t *testing.T,
	s *sqlite.Store,
	fence store.ControllerEpochFence,
	publication *store.Publication,
	state store.PublicationState,
	operationID string,
	updatedAt time.Time,
	prepared *store.PreparedPublicationReceipt,
	publish *store.PublishOperationReceipt,
	verification *store.PublicationVerificationReceipt,
	reason string,
) *store.Publication {
	t.Helper()
	operationDigest := acpSessionTestDigest(operationID)
	if publish != nil {
		operationDigest = publish.RequestDigest
	}
	if verification != nil {
		operationDigest = verification.RequestDigest
	}
	updated, err := s.TransitionPublication(context.Background(), store.PublicationTransition{
		ID: publication.ID, Fence: fence, ExpectedVersion: publication.Version, ExpectedGeneration: publication.Generation,
		ExpectedState: publication.State, NewState: state, OperationID: operationID, OperationDigest: operationDigest,
		PreparedReceipt: prepared, PublishReceipt: publish, VerificationReceipt: verification,
		TerminalReason: reason, UpdatedAt: updatedAt,
	})
	if err != nil {
		t.Fatalf("transition publication to %s: %v", state, err)
	}
	return updated
}

func acpSessionProjectionForTest(id, outcome string) ACPFinalizationProjection {
	payload, _ := json.Marshal(map[string]string{"outcome": outcome})
	return ACPFinalizationProjection{ID: "outbox-" + id, ProjectionKind: "TaskStatus", Payload: payload}
}

func acpSessionTestDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func TestACPSessionTurnDigestPreservesLegacyAppendIdentity(t *testing.T) {
	const (
		turnID        = "turn-compatibility"
		attemptID     = "attempt-compatibility"
		userPrompt    = "compatibility prompt"
		requestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)
	legacy, err := acpDomainDigest("session-turn", map[string]any{
		"turnID": turnID, "promptAttemptID": attemptID,
		"promptRequestDigest": requestDigest, "userPrompt": userPrompt,
	})
	if err != nil {
		t.Fatal(err)
	}
	appendDigest, err := acpSessionTurnDigest(turnID, attemptID, requestDigest, userPrompt, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if appendDigest != legacy {
		t.Fatalf("normal append digest = %q, want legacy %q", appendDigest, legacy)
	}
	skipDigest, err := acpSessionTurnDigest(turnID, attemptID, requestDigest, userPrompt, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if skipDigest == legacy {
		t.Fatal("skip-transcript append policy did not rotate the SessionTurn digest")
	}
	legacySkip, err := acpDomainDigest("session-turn", map[string]any{
		"turnID": turnID, "promptAttemptID": attemptID,
		"promptRequestDigest": requestDigest, "userPrompt": userPrompt,
		"skipTranscriptAppend": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if skipDigest != legacySkip {
		t.Fatalf("full-suppression digest = %q, want legacy %q", skipDigest, legacySkip)
	}
	assistantOnlyDigest, err := acpSessionTurnDigest(turnID, attemptID, requestDigest, userPrompt, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if assistantOnlyDigest == legacy || assistantOnlyDigest == skipDigest {
		t.Fatal("assistant-only append policy did not receive a distinct SessionTurn digest")
	}
}
