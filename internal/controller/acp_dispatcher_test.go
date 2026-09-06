package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/artifactcap"
	executionevents "github.com/orka-agents/orka/internal/events"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	v2eventjournal "github.com/orka-agents/orka/internal/harness/v2/eventjournal"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/publisher"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/store"
	kubestore "github.com/orka-agents/orka/internal/store/kube"
	"github.com/orka-agents/orka/internal/store/sqlite"
	"github.com/orka-agents/orka/internal/store/storetest"
	"github.com/orka-agents/orka/internal/tasktrace"
	orkatracing "github.com/orka-agents/orka/internal/tracing"
	tracingtest "github.com/orka-agents/orka/internal/tracing/testutil"
)

const (
	acpDispatcherTestNamespace       = "default"
	acpDispatcherTestTaskName        = "task"
	acpDispatcherTestPoolUID         = "pool-uid"
	acpDispatcherMissingTerminalTask = "missing-terminal"
	acpDispatcherRuntimeSession      = "runtime-session"
	acpDispatcherTaskUID             = "task-uid"
	acpDispatcherRuntimeSessionUID   = "runtime-session-uid"
	acpDispatcherToolCallID          = "call-1"
	acpDispatcherToolTitle           = "Inspect repository"
	acpDispatcherToolKind            = "file_read"
	acpDispatcherToolResultPath      = "README.md"
)

// dispatchQueuedTask reserves the queued Task on its RuntimePool and executes
// the reserved attempt, mirroring the dispatcher's production reserve/execute
// sequence.
func dispatchQueuedTask(ctx context.Context, t *testing.T, dispatcher *ACPDispatcher, queued *corev1alpha1.Task) {
	t.Helper()
	if dispatcher.EventStore == nil {
		if eventStore, ok := dispatcher.ResultStore.(store.ExecutionEventStore); ok {
			dispatcher.EventStore = eventStore
		} else if eventStore, ok := dispatcher.Store.(store.ExecutionEventStore); ok {
			dispatcher.EventStore = eventStore
		}
	}
	if dispatcher.PlanStore == nil {
		if planStore, ok := dispatcher.ResultStore.(store.PlanStore); ok {
			dispatcher.PlanStore = planStore
		} else if planStore, ok := dispatcher.Store.(store.PlanStore); ok {
			dispatcher.PlanStore = planStore
		}
	}
	if dispatcher.EventStore == nil || dispatcher.PlanStore == nil {
		t.Fatal("ACP dispatcher test requires execution event and plan stores")
	}
	task, target, err := dispatcher.reserveTask(ctx, queued)
	if err != nil {
		t.Fatal(err)
	}
	if task == nil {
		t.Fatal("task was not reserved for ACP dispatch")
	}
	if err := dispatcher.executeReservedTask(ctx, task, target); err != nil {
		t.Fatal(err)
	}
}

func cancelRuntimeContextAfterPromptRunning(
	ctx context.Context,
	attempts store.PromptAttemptStore,
	attemptID string,
	accepted <-chan struct{},
	deadlineCancels <-chan context.CancelCauseFunc,
) <-chan error {
	result := make(chan error, 1)
	go func() {
		select {
		case <-accepted:
		case <-ctx.Done():
			result <- ctx.Err()
			return
		}
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			observed, err := attempts.GetPromptAttempt(ctx, attemptID)
			if err != nil {
				result <- err
				return
			}
			if observed.ExecutionState == store.PromptExecutionRunning {
				select {
				case cancelCause := <-deadlineCancels:
					cancelCause(context.DeadlineExceeded)
					result <- nil
				case <-ctx.Done():
					result <- ctx.Err()
				}
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				result <- ctx.Err()
				return
			}
		}
	}()
	return result
}

type baseOnlyExecutionEventStore struct {
	store.ExecutionEventStore
}

type dedupeOnlyExecutionEventStore struct {
	store.DeduplicatingExecutionEventStore
}

func TestACPDispatcherStartRequiresAtomicEventDeduplication(t *testing.T) {
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "start-event-store.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	persistence := sqlite.NewStore(db, "start-event-store-test")
	dispatcher := &ACPDispatcher{
		Client:      fake.NewClientBuilder().Build(),
		Store:       persistence,
		ResultStore: persistence,
		EventStore:  baseOnlyExecutionEventStore{ExecutionEventStore: persistence},
		PlanStore:   persistence,
		Snapshots:   persistence,
		Epochs:      NewControllerEpochManager(persistence, "start-event-store-controller"),
	}

	err = dispatcher.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "execution event store with atomic deduplication") {
		t.Fatalf("Start() error = %v, want atomic deduplication requirement", err)
	}
}

func TestACPDispatcherStartRequiresAtomicPlanProjection(t *testing.T) {
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "start-plan-store.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	persistence := sqlite.NewStore(db, "start-plan-store-test")
	dispatcher := &ACPDispatcher{
		Client:      fake.NewClientBuilder().Build(),
		Store:       persistence,
		ResultStore: persistence,
		EventStore: dedupeOnlyExecutionEventStore{
			DeduplicatingExecutionEventStore: persistence,
		},
		PlanStore: persistence,
		Snapshots: persistence,
		Epochs:    NewControllerEpochManager(persistence, "start-plan-store-controller"),
	}

	err = dispatcher.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "execution event store with atomic plan projection") {
		t.Fatalf("Start() error = %v, want atomic plan projection requirement", err)
	}
}

func stampACPTaskTrace(t *testing.T, task *corev1alpha1.Task) (*tracingtest.SpanHarness, string) {
	t.Helper()
	if _, err := orkatracing.Init("acp-controller-test", false); err != nil {
		t.Fatalf("initialize tracing: %v", err)
	}
	harness := tracingtest.NewSpanHarness(t)
	ctx, parent := orkatracing.Tracer("test").Start(context.Background(), "task.creator")
	parentID := parent.SpanContext().SpanID().String()
	orkatracing.StampTaskTraceContext(ctx, task)
	parent.End()
	return harness, parentID
}

func acpSpanForTask(
	t *testing.T,
	spans []sdktrace.ReadOnlySpan,
	spanName string,
	taskName string,
) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range tracingtest.SpansNamed(spans, spanName) {
		if tracingtest.AttributeMap(span)[orkatracing.AttrTaskID].AsString() == taskName {
			return span
		}
	}
	t.Fatalf("missing %s span for Task %s", spanName, taskName)
	return nil
}

func TestCompletedPromptResultTextPrefersTerminalContent(t *testing.T) {
	terminal := &harnessv2.Event{Completed: &harnessv2.CompletedEvent{Result: harnessv2.PromptResult{
		Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: `{"schemaVersion":1,"ok":true}`}},
	}}}
	got, err := completedPromptResultText(terminal, "commentary that must not become the result", true)
	if err != nil {
		t.Fatalf("terminal result selection: %v", err)
	}
	if got != `{"schemaVersion":1,"ok":true}` {
		t.Fatalf("terminal result = %q", got)
	}

	got, err = completedPromptResultText(&harnessv2.Event{}, " legacy result ", false)
	if err != nil || got != "legacy result" {
		t.Fatalf("legacy fallback = %q, err=%v", got, err)
	}
	if _, err = completedPromptResultText(&harnessv2.Event{}, "truncated", true); err == nil {
		t.Fatal("overflowed fallback without terminal result was accepted")
	}
}

func TestAssistantTranscriptForPersistenceOmitsOverflowedPrefix(t *testing.T) {
	if got, omitted := assistantTranscriptForPersistence("credential prefix", true); got != "" || !omitted {
		t.Fatalf("overflowed assistant transcript = %q omitted=%t, want empty/true", got, omitted)
	}
	if got, omitted := assistantTranscriptForPersistence("complete transcript", false); got != "complete transcript" || omitted {
		t.Fatalf("complete assistant transcript = %q omitted=%t", got, omitted)
	}
	if got, omitted := completedAssistantTranscriptForPersistence("commentary and final", false, "final"); got != "commentary and final" || omitted {
		t.Fatalf("completed streamed transcript = %q omitted=%t", got, omitted)
	}
	if got, omitted := completedAssistantTranscriptForPersistence("", false, "terminal fallback"); got != "terminal fallback" || omitted {
		t.Fatalf("completed terminal fallback = %q omitted=%t", got, omitted)
	}
}

func TestSaveACPPlanUpdateWithRetry(t *testing.T) {
	t.Run("transient failure", func(t *testing.T) {
		planStore := &retryPlanStore{saveErrors: []error{errors.New("transient")}}
		plan := &store.PlanState{TaskName: acpDispatcherTestTaskName, Namespace: acpDispatcherTestNamespace, Summary: "working"}
		if err := saveACPPlanUpdateWithRetry(context.Background(), planStore, acpDispatcherTestNamespace, acpDispatcherTestTaskName, plan); err != nil {
			t.Fatalf("save plan with retry: %v", err)
		}
		if planStore.saveCalls != 2 || planStore.lastPlan != plan {
			t.Fatalf("save calls = %d plan=%p, want 2 and %p", planStore.saveCalls, planStore.lastPlan, plan)
		}
	})

	t.Run("persistent failure", func(t *testing.T) {
		firstErr := errors.New("first")
		retryErr := errors.New("retry")
		planStore := &retryPlanStore{saveErrors: []error{firstErr, retryErr}}
		err := saveACPPlanUpdateWithRetry(context.Background(), planStore, acpDispatcherTestNamespace, acpDispatcherTestTaskName, &store.PlanState{})
		if !errors.Is(err, firstErr) || !errors.Is(err, retryErr) {
			t.Fatalf("persistent save error = %v", err)
		}
		if planStore.saveCalls != 2 {
			t.Fatalf("save calls = %d, want 2", planStore.saveCalls)
		}
	})

	t.Run("reconciles after journal append", func(t *testing.T) {
		firstErr := errors.New("first")
		retryErr := errors.New("retry")
		planStore := &retryPlanStore{saveErrors: []error{firstErr, retryErr}}
		plan := &store.PlanState{TaskName: acpDispatcherTestTaskName, Namespace: acpDispatcherTestNamespace, Summary: "working"}
		planErr := saveACPPlanUpdateWithRetry(context.Background(), planStore, acpDispatcherTestNamespace, acpDispatcherTestTaskName, plan)
		if err := reconcileACPPlanUpdateAfterJournal(
			context.Background(), planStore, acpDispatcherTestNamespace, acpDispatcherTestTaskName, plan, planErr, nil,
		); err != nil {
			t.Fatalf("reconcile plan after journal append: %v", err)
		}
		if planStore.saveCalls != 3 || planStore.lastPlan != plan {
			t.Fatalf("save calls = %d plan=%p, want 3 and %p", planStore.saveCalls, planStore.lastPlan, plan)
		}
	})

	t.Run("does not reconcile before journal append", func(t *testing.T) {
		planErr := errors.New("plan unavailable")
		journalErr := errors.New("journal unavailable")
		planStore := &retryPlanStore{}
		err := reconcileACPPlanUpdateAfterJournal(
			context.Background(), planStore, acpDispatcherTestNamespace, acpDispatcherTestTaskName, &store.PlanState{}, planErr, journalErr,
		)
		if !errors.Is(err, planErr) || planStore.saveCalls != 0 {
			t.Fatalf("pre-journal reconciliation error = %v calls=%d", err, planStore.saveCalls)
		}
	})
}

func TestACPUpdatePersistenceError(t *testing.T) {
	if err := acpUpdatePersistenceError(nil, nil); err != nil {
		t.Fatalf("nil persistence errors = %v", err)
	}
	journalErr := errors.New("journal unavailable")
	planErr := errors.New("plan unavailable")
	err := acpUpdatePersistenceError(journalErr, planErr)
	if !errors.Is(err, journalErr) || !errors.Is(err, planErr) {
		t.Fatalf("joined persistence error = %v", err)
	}
	var persistenceErr *acpExecutionUpdatePersistenceError
	if !errors.As(err, &persistenceErr) {
		t.Fatalf("persistence error type = %T, want *acpExecutionUpdatePersistenceError", err)
	}
	if !persistenceErr.journalFailed() {
		t.Fatal("combined persistence error did not retain journal failure provenance")
	}
	if !strings.Contains(err.Error(), "persist ACP execution update") ||
		!strings.Contains(err.Error(), "persist ACP plan update") {
		t.Fatalf("persistence error context = %v", err)
	}
	planOnly := acpUpdatePersistenceError(nil, planErr)
	if !errors.As(planOnly, &persistenceErr) || persistenceErr.journalFailed() {
		t.Fatalf("plan-only persistence error provenance = %#v", persistenceErr)
	}
}

type retryPlanStore struct {
	saveErrors []error
	saveCalls  int
	lastPlan   *store.PlanState
}

func (s *retryPlanStore) SavePlan(_ context.Context, _, _ string, plan *store.PlanState) error {
	s.lastPlan = plan
	s.saveCalls++
	if s.saveCalls <= len(s.saveErrors) {
		return s.saveErrors[s.saveCalls-1]
	}
	return nil
}

func (*retryPlanStore) GetPlan(context.Context, string, string) (*store.PlanState, error) {
	return nil, store.ErrNotFound
}

func (*retryPlanStore) DeletePlan(context.Context, string, string) error { return nil }

func prepareBoundACPDispatcherTaskForTest(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	scheme *runtime.Scheme,
	controlStore *sqlite.Store,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
	images ACPRuntimeImages,
) *corev1alpha1.Task {
	return prepareBoundACPDispatcherTaskWithStoresForTest(
		t, ctx, kubeClient, scheme, controlStore, controlStore, task, agent, images,
	)
}

func prepareBoundACPDispatcherTaskWithStoresForTest(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	scheme *runtime.Scheme,
	controlStore store.DurableControlStore,
	persistence *sqlite.Store,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
	images ACPRuntimeImages,
) *corev1alpha1.Task {
	t.Helper()
	cipher, err := sqlite.NewAgentExecutionSnapshotCipher(bytes.Repeat([]byte{0x61}, sqlite.AgentExecutionSnapshotKeyBytes))
	if err != nil {
		t.Fatal(err)
	}
	if err := persistence.SetAgentExecutionSnapshotCipher(cipher); err != nil {
		t.Fatal(err)
	}
	binder := &TaskReconciler{
		Client:                  kubeClient,
		APIReader:               kubeClient,
		Scheme:                  scheme,
		DurableControlStore:     controlStore,
		AgentExecutionSnapshots: persistence,
		ACPRuntimeEnabled:       true,
		ACPRuntimeImages:        images,
	}
	bound := bindACPQueueTaskForTest(t, ctx, binder, task, agent)
	verified, err := binder.loadVerifiedBoundExecution(ctx, bound, bound.Status.AgentExecutionBinding)
	if err != nil {
		t.Fatalf("load frozen dispatcher test execution: %v", err)
	}
	requestDigest, err := acpBoundTaskRequestDigest(
		verified, bound.Status.Execution.Attempt, bound.Status.Execution.PromptID,
	)
	if err != nil {
		t.Fatal(err)
	}
	current := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(bound), current); err != nil {
		t.Fatal(err)
	}
	current.Status.Execution.RequestDigest = requestDigest
	if err := kubeClient.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(bound), current); err != nil {
		t.Fatal(err)
	}
	return current
}

func frozenACPDispatcherPlanForTest(
	t *testing.T,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
	images ACPRuntimeImages,
) ACPRuntimePlan {
	t.Helper()
	configuration, err := buildACPAgentSessionConfiguration(task, agent, "")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanACPRuntimeWithConfiguration(task, agent, images, configuration)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestRuntimeSessionCreateTimeoutCoversColdAdapterInitialization(t *testing.T) {
	t.Parallel()
	minimum := time.Duration(corev1alpha1.DefaultRuntimePoolColdStartTimeoutSeconds) * time.Second
	for _, test := range []struct {
		name   string
		target acpDispatchTarget
		want   time.Duration
	}{
		{name: "external runtime", target: acpDispatchTarget{external: &corev1alpha1.AgentRuntime{}}, want: minimum},
		{name: "unset pool timeout", target: acpDispatchTarget{pool: &corev1alpha1.RuntimePool{}}, want: minimum},
		{name: "short pool timeout", target: acpDispatchTarget{pool: &corev1alpha1.RuntimePool{Spec: corev1alpha1.RuntimePoolSpec{ColdStartTimeoutSeconds: 30}}}, want: minimum},
		{name: "long pool timeout", target: acpDispatchTarget{pool: &corev1alpha1.RuntimePool{Spec: corev1alpha1.RuntimePoolSpec{ColdStartTimeoutSeconds: 300}}}, want: 5 * time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := runtimeSessionCreateTimeout(test.target); got != test.want {
				t.Fatalf("runtimeSessionCreateTimeout() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestRuntimeSessionCreateExpiresAtUsesDurableIssuedAt(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, time.September, 2, 7, 0, 0, 0, time.UTC)
	target := acpDispatchTarget{pool: &corev1alpha1.RuntimePool{Spec: corev1alpha1.RuntimePoolSpec{ColdStartTimeoutSeconds: 600}}}
	if got, want := runtimeSessionCreateExpiresAt(issuedAt, target), issuedAt.Add(10*time.Minute); !got.Equal(want) {
		t.Fatalf("RuntimeSession create expiry = %s, want %s", got, want)
	}
}

func TestRuntimeSessionCreateRenewalExpiresAtUsesShortestAuthorization(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, time.September, 2, 7, 0, 0, 0, time.UTC)
	createExpiresAt := issuedAt.Add(10 * time.Minute)
	if got, want := runtimeSessionCreateRenewalExpiresAt(issuedAt, createExpiresAt, true), issuedAt.Add(artifactcap.MaxCapabilityTTL); !got.Equal(want) {
		t.Fatalf("RuntimeSession renewal expiry with workspace authorization = %s, want %s", got, want)
	}
	if got := runtimeSessionCreateRenewalExpiresAt(issuedAt, createExpiresAt, false); !got.Equal(createExpiresAt) {
		t.Fatalf("RuntimeSession renewal expiry without workspace authorization = %s, want %s", got, createExpiresAt)
	}
}

func TestRuntimeSessionCreateAuthorizationNeedsRenewal(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.September, 2, 7, 0, 0, 0, time.UTC)
	if !runtimeSessionCreateAuthorizationNeedsRenewal(now.Add(runtimeSessionCreateRenewalMargin), now) {
		t.Fatal("authorization at the renewal margin was treated as reusable")
	}
	if runtimeSessionCreateAuthorizationNeedsRenewal(now.Add(runtimeSessionCreateRenewalMargin+time.Nanosecond), now) {
		t.Fatal("authorization beyond the renewal margin was rotated early")
	}
}

func TestReconcilePlannedTaskScopedRuntimeSessionReusesAdmissibleSession(t *testing.T) {
	profileDigest := harnessv2.ProfileDigest(testControlDigestForDispatcher("task-scoped-recovery-profile"))
	runtimeFence := harnessv2.Fence{
		RuntimeInstanceID: "pod-uid.boot-id", SupervisorBootID: "boot-id", ControllerEpoch: 1,
		RuntimePoolUID: acpDispatcherTestPoolUID, RuntimePoolGeneration: 1,
		RuntimeProfileDigest: profileDigest, ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		RuntimeSessionUID: "task-scoped-recovery-uid", RuntimeSessionGeneration: 1,
	}
	descriptor := harnessv2.RuntimeSessionDescriptor{
		RuntimeSessionID:  harnessv2.RuntimeSessionID(runtimeSessionID(runtimeFence)),
		RuntimeSessionUID: runtimeFence.RuntimeSessionUID, Generation: runtimeFence.RuntimeSessionGeneration,
		State: harnessv2.RuntimeSessionStateIdle, LastTransitionAt: time.Now().UTC(),
	}
	statusResponse := dispatcherRuntimeStatusResponse(profileDigest, descriptor)
	if err := statusResponse.Validate(); err != nil {
		t.Fatalf("build task-scoped recovery status: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != harnessv2.StatusPath {
			http.NotFound(w, r)
			return
		}
		writeDispatcherJSON(w, statusResponse)
	}))
	defer server.Close()
	runtimeClient, err := harnessv2.NewClient(
		server.URL,
		harnessv2.WithControllerBearerToken(strings.Repeat("t", 32)),
		harnessv2.WithOperationCapabilitySecret([]byte(strings.Repeat("s", 32))),
		harnessv2.WithStatusCapabilityBinding(harnessv2.StatusCapabilityBinding{RuntimeProfileDigest: profileDigest}),
	)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "task-scoped-recovery"}}
	reused, requeued, err := (&ACPDispatcher{}).reconcilePlannedTaskScopedRuntimeSession(
		context.Background(), runtimeClient, task, "attempt", store.ControllerEpochFence{}, runtimeFence,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reused || requeued {
		t.Fatalf("task-scoped RuntimeSession recovery = reused %t, requeued %t; want true, false", reused, requeued)
	}
}

func TestReconcilePlannedTaskScopedRuntimeSessionRequeuesNonAdmissibleSession(t *testing.T) {
	ctx := context.Background()
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "task-scoped-recovery.db"))
	defer closeStore()
	task := runtimePoolReservationTestTask(
		"task-scoped-terminal-recovery", "99999999-1111-2222-3333-444444444444", acpDispatcherTestPoolUID,
	)
	key := store.PromptAttemptKey{
		Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: task.Status.Execution.PromptID,
	}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		Key: key, RequestDigest: task.Status.Execution.RequestDigest,
		ExecutionState: store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
	}), fence)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
		NewState: store.PromptExecutionReserved, OperationID: "task-scoped-reserved",
		OperationDigest: testControlDigestForDispatcher("task-scoped-reserved"), UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	initialGeneration, err := taskScopedRuntimeSessionGeneration(attempt)
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []store.PromptExecutionState{store.PromptExecutionSessionStarting, store.PromptExecutionPlanned} {
		operation := "task-scoped-" + string(next)
		attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: next, OperationID: operation, OperationDigest: testControlDigestForDispatcher(operation),
			RuntimeInstanceID: "pod-uid.boot-id", SessionUID: string(task.UID),
			SessionLeaseGeneration: int64(initialGeneration), UpdatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	profileDigest := harnessv2.ProfileDigest(testControlDigestForDispatcher("task-scoped-terminal-profile"))
	runtimeFence := harnessv2.Fence{
		RuntimeInstanceID: "pod-uid.boot-id", SupervisorBootID: "boot-id", ControllerEpoch: 1,
		RuntimePoolUID: acpDispatcherTestPoolUID, RuntimePoolGeneration: 1,
		RuntimeProfileDigest: profileDigest, ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		RuntimeSessionUID: harnessv2.RuntimeSessionUID(task.UID), RuntimeSessionGeneration: initialGeneration,
	}
	descriptor := harnessv2.RuntimeSessionDescriptor{
		RuntimeSessionID:  harnessv2.RuntimeSessionID(runtimeSessionID(runtimeFence)),
		RuntimeSessionUID: runtimeFence.RuntimeSessionUID, Generation: runtimeFence.RuntimeSessionGeneration,
		State: harnessv2.RuntimeSessionStatePoisoned, LastTransitionAt: time.Now().UTC(),
	}
	statusResponse := dispatcherRuntimeStatusResponse(profileDigest, descriptor)
	if err := statusResponse.Validate(); err != nil {
		t.Fatalf("build non-admissible task-scoped status: %v", err)
	}
	var deleteCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+harnessv2.StatusPath, func(w http.ResponseWriter, _ *http.Request) {
		writeDispatcherJSON(w, statusResponse)
	})
	mux.HandleFunc("DELETE /v2/runtime-sessions/{sessionID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.DeleteRuntimeSessionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode task-scoped RuntimeSession delete: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		deleteCalls.Add(1)
		writeDispatcherJSON(w, harnessv2.DeleteRuntimeSessionResponse{
			Protocol:       harnessv2.ProtocolVersion,
			Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			State:          harnessv2.RuntimeSessionStateDeleted,
			Tombstone:      testDeleteTombstone(request, time.Now().UTC()),
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	runtimeClient, err := harnessv2.NewClient(
		server.URL,
		harnessv2.WithControllerBearerToken(strings.Repeat("t", 32)),
		harnessv2.WithOperationCapabilitySecret([]byte(strings.Repeat("s", 32))),
		harnessv2.WithStatusCapabilityBinding(harnessv2.StatusCapabilityBinding{RuntimeProfileDigest: profileDigest}),
	)
	if err != nil {
		t.Fatal(err)
	}
	task.Status.Execution.State = corev1alpha1.TaskExecutionStatePlanned
	task.Status.Execution.RuntimeInstanceID = string(runtimeFence.RuntimeInstanceID)
	task.Status.Execution.RuntimeSessionUID = string(runtimeFence.RuntimeSessionUID)
	task.Status.Execution.RuntimeSessionGeneration = int64(runtimeFence.RuntimeSessionGeneration)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}).
		WithObjects(task.DeepCopy()).Build()
	dispatcher := &ACPDispatcher{Client: kubeClient, Store: controlStore}
	reused, requeued, err := dispatcher.reconcilePlannedTaskScopedRuntimeSession(
		ctx, runtimeClient, task, attempt.ID, fence, runtimeFence,
	)
	if err != nil {
		t.Fatal(err)
	}
	if reused || !requeued || deleteCalls.Load() != 1 {
		t.Fatalf("task-scoped RuntimeSession recovery = reused %t, requeued %t, deletes %d; want false, true, 1", reused, requeued, deleteCalls.Load())
	}
	recoveredAttempt, err := controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredAttempt.ExecutionState != store.PromptExecutionReserved {
		t.Fatalf("recovered PromptAttempt state = %s, want %s", recoveredAttempt.ExecutionState, store.PromptExecutionReserved)
	}
	nextGeneration, err := taskScopedRuntimeSessionGeneration(recoveredAttempt)
	if err != nil {
		t.Fatal(err)
	}
	if nextGeneration <= initialGeneration {
		t.Fatalf("recovered task-scoped RuntimeSession generation = %d, want greater than %d", nextGeneration, initialGeneration)
	}
	currentTask := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(task), currentTask); err != nil {
		t.Fatal(err)
	}
	if currentTask.Status.Execution == nil || currentTask.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved ||
		currentTask.Status.Execution.RuntimeSessionUID != "" || currentTask.Status.Execution.RuntimeSessionGeneration != 0 {
		t.Fatalf("requeued Task execution = %#v", currentTask.Status.Execution)
	}
}

func TestRotateExpiredSessionBoundRuntimeSessionCreation(t *testing.T) {
	ctx := context.Background()
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "expired-session-create.db"))
	defer closeStore()
	task := runtimePoolReservationTestTask("expired-session-create", "expired-session-create-uid", acpDispatcherTestPoolUID)
	task.Status.Execution.State = corev1alpha1.TaskExecutionStatePlanned
	task.Status.Execution.RuntimeSessionUID = "durable-session-uid"
	task.Status.Execution.RuntimeSessionGeneration = 1
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}).
		WithObjects(task.DeepCopy()).Build()
	key := store.PromptAttemptKey{
		Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: task.Status.Execution.PromptID,
	}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		Key: key, RequestDigest: task.Status.Execution.RequestDigest,
		ExecutionState: store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
	}), fence)
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []store.PromptExecutionState{
		store.PromptExecutionReserved, store.PromptExecutionSessionStarting, store.PromptExecutionPlanned,
	} {
		operation := "expired-session-create-" + string(next)
		attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: next, OperationID: operation, OperationDigest: testControlDigestForDispatcher(operation),
			UpdatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	profileDigest := harnessv2.ProfileDigest(testControlDigestForDispatcher("expired-session-create-profile"))
	session := &acpTaskSession{
		Binding: ACPRuntimeSessionBinding{
			SessionUID: "durable-session-uid", Generation: 1, ProfileDigest: profileDigest,
			RuntimeInstanceID: "pod-uid.boot-id", SupervisorBootID: "boot-id",
			WorkspaceDigest: testControlDigestForDispatcher("old-workspace"),
		},
		LeaseGeneration: 1,
	}
	runtimeFence := harnessv2.Fence{
		RuntimeInstanceID: session.Binding.RuntimeInstanceID, SupervisorBootID: session.Binding.SupervisorBootID,
		RuntimeProfileDigest: profileDigest, RuntimeSessionUID: harnessv2.RuntimeSessionUID(session.Binding.SessionUID),
		RuntimeSessionGeneration: session.Binding.Generation,
	}
	dispatcher := &ACPDispatcher{Client: kubeClient, Store: controlStore}
	if err := dispatcher.rotateExpiredSessionBoundRuntimeSessionCreation(
		ctx, task, attempt.ID, fence, session, &runtimeFence,
	); err != nil {
		t.Fatal(err)
	}
	if session.Binding.Generation != 2 || runtimeFence.RuntimeSessionGeneration != 2 ||
		!session.Binding.RecreationRequired || session.Binding.WorkspaceDigest != "" || !session.requeued {
		t.Fatalf("rotated session binding = %#v, fence generation = %d, requeued = %t", session.Binding, runtimeFence.RuntimeSessionGeneration, session.requeued)
	}
	currentAttempt, err := controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if currentAttempt.ExecutionState != store.PromptExecutionReserved {
		t.Fatalf("rotated PromptAttempt state = %s, want %s", currentAttempt.ExecutionState, store.PromptExecutionReserved)
	}
	currentTask := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(task), currentTask); err != nil {
		t.Fatal(err)
	}
	if currentTask.Status.Execution == nil || currentTask.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved ||
		currentTask.Status.Execution.RuntimeSessionGeneration != 2 || !currentTask.Status.Execution.RuntimeSessionRecreationPending {
		t.Fatalf("rotated Task execution = %#v", currentTask.Status.Execution)
	}
}

func TestACPTaskDeadlineIncludesTimeBeforeRuntimeAdmission(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC)
	timeout := metav1.Duration{Duration: 5 * time.Minute}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(now.Add(-4 * time.Minute)),
			Annotations:       map[string]string{acpRuntimeQueuedAtAnnotation: now.Add(-time.Minute).Format(time.RFC3339Nano)},
		},
		Spec: corev1alpha1.TaskSpec{Timeout: &timeout},
	}
	deadline, ok := acpTaskDeadline(task, now)
	if !ok || !deadline.Equal(now.Add(time.Minute)) {
		t.Fatalf("deadline = %s, %v; want %s, true", deadline, ok, now.Add(time.Minute))
	}

	task.CreationTimestamp = metav1.Time{}
	deadline, ok = acpTaskDeadline(task, now)
	if !ok || !deadline.Equal(now.Add(4*time.Minute)) {
		t.Fatalf("queue-derived deadline = %s, %v; want %s, true", deadline, ok, now.Add(4*time.Minute))
	}

	task.Spec.Timeout = nil
	if deadline, ok = acpTaskDeadline(task, now); ok || !deadline.IsZero() {
		t.Fatalf("unbound default deadline = %s, %v; want zero, false", deadline, ok)
	}
	task.Status.AgentExecutionBinding = &corev1alpha1.AgentExecutionBinding{
		ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV1,
	}
	if deadline, ok = acpTaskDeadline(task, now); ok || !deadline.IsZero() {
		t.Fatalf("harness v1 default deadline = %s, %v; want zero, false", deadline, ok)
	}
	task.Status.AgentExecutionBinding = testACPExecuteBindingForDispatcher()
	deadline, ok = acpTaskDeadline(task, now)
	if !ok || !deadline.Equal(now.Add(defaultACPTaskTimeout-time.Minute)) {
		t.Fatalf("default deadline = %s, %v; want %s, true", deadline, ok, now.Add(defaultACPTaskTimeout-time.Minute))
	}
	for _, duration := range []time.Duration{0, -time.Minute} {
		task.Spec.Timeout = &metav1.Duration{Duration: duration}
		deadline, ok = acpTaskDeadline(task, now)
		if !ok || !deadline.Equal(now.Add(defaultACPTaskTimeout-time.Minute)) {
			t.Fatalf("nonpositive %s deadline = %s, %v; want %s, true", duration, deadline, ok, now.Add(defaultACPTaskTimeout-time.Minute))
		}
	}
}

func TestACPTaskRuntimeContextUsesDefaultDeadline(t *testing.T) {
	t.Parallel()
	createdAt := time.Now().UTC().Add(-time.Minute)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(createdAt)},
		Status: corev1alpha1.TaskStatus{
			AgentExecutionBinding: testACPExecuteBindingForDispatcher(),
		},
	}
	runtimeCtx, cancel := (&ACPDispatcher{}).newTaskRuntimeContext(context.Background(), task)
	defer cancel()

	deadline, ok := runtimeCtx.Deadline()
	want := createdAt.Add(defaultACPTaskTimeout)
	if !ok || !deadline.Equal(want) {
		t.Fatalf("runtime context deadline = %s, %v; want %s, true", deadline, ok, want)
	}
	if err := runtimeCtx.Err(); err != nil {
		t.Fatalf("runtime context expired before the default deadline: %v", err)
	}
}

func TestRuntimeSessionStartFailureMessageAllowsOnlyKnownStages(t *testing.T) {
	t.Parallel()
	const fallback = "runtime session failed to start"
	allowed := &harnessv2.ClientError{
		StatusCode: http.StatusInternalServerError,
		Code:       harnessv2.ErrorCodeSessionPoisoned,
		Message:    "runtime session failed during provider adapter initialization",
	}
	if got := runtimeSessionStartFailureMessage(allowed); got != allowed.Message {
		t.Fatalf("known stage message = %q", got)
	}
	status, code, diagnostic := runtimeSessionStartDiagnostic(allowed)
	if status != http.StatusInternalServerError || code != harnessv2.ErrorCodeSessionPoisoned || diagnostic != allowed.Message {
		t.Fatalf("known stage diagnostic = %d/%s/%q", status, code, diagnostic)
	}
	unknown := &harnessv2.ClientError{StatusCode: http.StatusBadRequest, Code: harnessv2.ErrorCodeInvalidRequest, Message: "runtime session failed during provider-secret-must-not-leak"}
	if got := runtimeSessionStartFailureMessage(unknown); got != fallback {
		t.Fatalf("unknown stage message = %q", got)
	}
	status, code, diagnostic = runtimeSessionStartDiagnostic(unknown)
	if status != http.StatusBadRequest || code != harnessv2.ErrorCodeInvalidRequest || diagnostic != "runtime session request rejected before classified creation" {
		t.Fatalf("unknown stage diagnostic = %d/%s/%q", status, code, diagnostic)
	}
	// Separators are dropped, not spaced, so a line-wrapped credential
	// reassembles for the redactor; prose merely loses the break.
	unknown.Message = "  fixed server detail\nwith control  "
	if got := boundedRuntimeSessionServerMessage(unknown); got != "fixed server detailwith control" {
		t.Fatalf("bounded server message = %q", got)
	}
	if got := runtimeSessionStartFailureMessage(errors.New("raw-secret")); got != fallback {
		t.Fatalf("raw error message = %q", got)
	}
}

func TestMarkTaskRuntimePoolWorkspaceResumeLost(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := runtimePoolReservationTestTask("resume-lost", "resume-lost-uid", acpDispatcherTestPoolUID)
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: task.Namespace, Name: task.Status.Execution.RuntimePoolName, UID: acpDispatcherTestPoolUID},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool).Build()
	dispatcher := &ACPDispatcher{Client: kubeClient, APIReader: kubeClient}
	resumeLost := &harnessv2.ClientError{
		Kind: harnessv2.ClientErrorHTTP, StatusCode: http.StatusConflict,
		Code: harnessv2.ErrorCodeWorkspaceResumeLost, Retryable: false,
	}
	if !runtimeSessionWorkspaceResumeLost(resumeLost) {
		t.Fatal("workspace resume loss was not recognized")
	}
	resumeLost.Retryable = true
	if runtimeSessionWorkspaceResumeLost(resumeLost) {
		t.Fatal("retryable error was classified as terminal workspace resume loss")
	}
	resumeLost.Retryable = false
	if err := dispatcher.markTaskRuntimePoolWorkspaceResumeLost(context.Background(), task); err != nil {
		t.Fatalf("mark workspace resume loss: %v", err)
	}
	current := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(pool), current); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(current.Annotations[runtimePoolWorkspaceResumeLostAnnotation]) == "" {
		t.Fatal("RuntimePool workspace resume loss was not recorded")
	}
}

func TestRuntimeSessionCreationMayHaveApplied(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "ambiguous write", err: &harnessv2.ClientError{WriteEvidence: harnessv2.RequestWriteEvidence{State: harnessv2.RequestWriteAmbiguous}}, want: true},
		{name: "zero bytes", err: &harnessv2.ClientError{WriteEvidence: harnessv2.RequestWriteEvidence{State: harnessv2.RequestWriteZeroBytes}}, want: false},
		{name: "definitive HTTP rejection", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorHTTP, StatusCode: http.StatusUnprocessableEntity, WriteEvidence: harnessv2.RequestWriteEvidence{State: harnessv2.RequestWriteComplete}}, want: false},
		{name: "ambiguous server failure", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorHTTP, StatusCode: http.StatusInternalServerError, WriteEvidence: harnessv2.RequestWriteEvidence{State: harnessv2.RequestWriteComplete}}, want: true},
		{name: "create digest conflict records an earlier send", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorHTTP, StatusCode: http.StatusConflict, Code: harnessv2.ErrorCodeDigestConflict, WriteEvidence: harnessv2.RequestWriteEvidence{State: harnessv2.RequestWriteComplete}}, want: true},
		{name: "other conflict rejection", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorHTTP, StatusCode: http.StatusConflict, Code: harnessv2.ErrorCodeInvalidRequest, WriteEvidence: harnessv2.RequestWriteEvidence{State: harnessv2.RequestWriteComplete}}, want: false},
		{name: "protocol failure after write", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorProtocol, StatusCode: http.StatusOK, WriteEvidence: harnessv2.RequestWriteEvidence{State: harnessv2.RequestWriteComplete}}, want: true},
		{name: "non client error", err: errors.New("transport setup failed"), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := runtimeSessionCreationMayHaveApplied(tc.err); got != tc.want {
				t.Fatalf("runtimeSessionCreationMayHaveApplied() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPublicationTerminalStateMapping(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		state store.PublicationState
		want  harnessv2.PublicationTerminalState
	}{
		{store.PublicationVerifiedExact, harnessv2.PublicationTerminalVerifiedExact},
		{store.PublicationDeliveredSuperseded, harnessv2.PublicationTerminalDeliveredSuperseded},
		{store.PublicationCancelledBeforePublish, harnessv2.PublicationTerminalCancelledBeforePublish},
		{store.PublicationDeliveryConflict, harnessv2.PublicationTerminalDeliveryConflict},
		{store.PublicationCredentialBlocked, harnessv2.PublicationTerminalCredentialBlocked},
		{store.PublicationPreparationFailed, harnessv2.PublicationTerminalPreparationFailed},
		{store.PublicationOutcomeUnknown, harnessv2.PublicationTerminalOutcomeUnknown},
	} {
		got, err := publicationTerminalState(test.state)
		if err != nil || got != test.want {
			t.Fatalf("publicationTerminalState(%s) = %q, %v, want %q", test.state, got, err, test.want)
		}
	}
	if _, err := publicationTerminalState(store.PublicationPublishing); err == nil {
		t.Fatal("nonterminal publication state was accepted")
	}
}

func TestFinalizeRuntimeSessionPublicationTreatsNotFoundAsSuccess(t *testing.T) {
	var operationIDs []harnessv2.OperationID
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.FinalizeRuntimeSessionPublicationRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode publication finalization: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		operationIDs = append(operationIDs, request.Metadata.OperationID)
		writeDispatcherJSONStatus(w, http.StatusNotFound, harnessv2.ErrorResponse{
			Protocol: harnessv2.ProtocolVersion, Code: harnessv2.ErrorCodeInvalidRequest, Message: "runtime session not found",
		})
	}))
	defer server.Close()
	runtimeClient, err := harnessv2.NewClient(
		server.URL, harnessv2.WithControllerBearerToken(strings.Repeat("t", 32)),
		harnessv2.WithOperationCapabilitySecret([]byte(strings.Repeat("s", 32))),
	)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "finalize", UID: types.UID("88888888-8888-8888-8888-888888888888")},
		Status:     corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{Attempt: 1, PromptID: "prompt-finalize"}},
	}
	fence := harnessv2.Fence{
		RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot-id", ControllerEpoch: 1,
		RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 1, RuntimeProfileDigest: harnessv2.ProfileDigest(testControlDigestForDispatcher("finalize-profile")),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion, RuntimeSessionUID: "session-finalize", RuntimeSessionGeneration: 1,
	}
	finalization := runtimeSessionPublicationFinalization{
		WorkspaceDeltaID: "delta-prompt-finalize", PublicationID: "publication-1", PublicationGeneration: 1, PublicationVersion: 7,
		TerminalState: harnessv2.PublicationTerminalVerifiedExact, TerminalReceiptDigest: testControlDigestForDispatcher("receipt"),
	}
	dispatcher := &ACPDispatcher{}
	for range 2 {
		if err := dispatcher.finalizeRuntimeSessionPublication(
			context.Background(), runtimeClient, "runtime-session-finalize-g1", task, fence, finalization,
		); err != nil {
			t.Fatalf("finalizeRuntimeSessionPublication(not found) = %v, want success", err)
		}
	}
	if len(operationIDs) != 2 || operationIDs[0] == operationIDs[1] {
		t.Fatalf("publication finalization operation IDs = %v, want two unique recovery-safe identities", operationIDs)
	}
	for _, operationID := range operationIDs {
		if !strings.HasPrefix(string(operationID), "fp-") {
			t.Fatalf("publication finalization operation ID = %q, want fp- prefix", operationID)
		}
	}
}

func TestDeleteRuntimeSessionTreatsNotFoundAsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeDispatcherJSONStatus(w, http.StatusNotFound, harnessv2.ErrorResponse{
			Protocol: harnessv2.ProtocolVersion, Code: harnessv2.ErrorCodeInvalidRequest,
			Message: "runtime session not found", Retryable: false,
		})
	}))
	defer server.Close()
	runtimeClient, err := harnessv2.NewClient(
		server.URL,
		harnessv2.WithControllerBearerToken(strings.Repeat("t", 32)),
		harnessv2.WithOperationCapabilitySecret([]byte(strings.Repeat("s", 32))),
	)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "cleanup", UID: types.UID("99999999-9999-9999-9999-999999999999")},
		Status:     corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{Attempt: 1, PromptID: "prompt-cleanup"}},
	}
	fence := harnessv2.Fence{
		RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot-id", ControllerEpoch: 1,
		RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 1,
		RuntimeProfileDigest: harnessv2.ProfileDigest(testControlDigestForDispatcher("cleanup-profile")), ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		RuntimeSessionUID: "session-cleanup", RuntimeSessionGeneration: 1,
	}
	if err := (&ACPDispatcher{}).deleteRuntimeSession(context.Background(), runtimeClient, "runtime-session-cleanup-g1", task, fence, "test"); err != nil {
		t.Fatalf("deleteRuntimeSession(not found) = %v, want success", err)
	}
}

func TestDeleteRuntimeSessionAcceptsExactTombstoneReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.DeleteRuntimeSessionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode delete replay: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		now := time.Now().UTC()
		writeDispatcherJSON(w, harnessv2.DeleteRuntimeSessionResponse{
			Protocol: harnessv2.ProtocolVersion,
			Classification: harnessv2.Classification{
				Class: harnessv2.RequestClassificationDuplicate, Phase: harnessv2.OperationPhaseDeleted,
			},
			State: harnessv2.RuntimeSessionStateDeleted,
			Tombstone: harnessv2.RuntimeSessionTombstone{
				RuntimeSessionUID:        request.Metadata.Fence.RuntimeSessionUID,
				RuntimeSessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration,
				RuntimeProfileDigest:     request.Metadata.Fence.RuntimeProfileDigest,
				DeletedAt:                now,
				Operations: []harnessv2.OperationRecord{{
					OperationID: request.Metadata.OperationID, RequestDigest: request.Metadata.RequestDigest,
					Phase: harnessv2.OperationPhaseDeleted, RecordedAt: now, UpdatedAt: now,
				}},
			},
		})
	}))
	defer server.Close()
	runtimeClient, err := harnessv2.NewClient(
		server.URL,
		harnessv2.WithControllerBearerToken(strings.Repeat("t", 32)),
		harnessv2.WithOperationCapabilitySecret([]byte(strings.Repeat("s", 32))),
	)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "cleanup-replay", UID: types.UID("98989898-9898-9898-9898-989898989898")},
		Status:     corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{Attempt: 1, PromptID: "prompt-cleanup-replay"}},
	}
	fence := harnessv2.Fence{
		RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot-id", ControllerEpoch: 1,
		RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 1,
		RuntimeProfileDigest:       harnessv2.ProfileDigest(testControlDigestForDispatcher("cleanup-replay-profile")),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		RuntimeSessionUID:          "session-cleanup-replay",
		RuntimeSessionGeneration:   1,
	}
	if err := (&ACPDispatcher{}).deleteRuntimeSession(
		context.Background(), runtimeClient, "runtime-session-cleanup-replay-g1", task, fence, "retry",
	); err != nil {
		t.Fatalf("deleteRuntimeSession(tombstone replay) = %v, want success", err)
	}
}

func TestPromptStreamDiagnosticIsBoundedAndClassified(t *testing.T) {
	if got := promptStreamDiagnostic(fmt.Errorf("wrapped: %w", harnessv2.ErrMissingTerminalEvent)); got != promptStreamMissingTerminalDiagnostic {
		t.Fatalf("missing-terminal diagnostic = %q", got)
	}
	if got := promptStreamDiagnostic(fmt.Errorf("wrapped: %w", harnessv2.ErrEventRateExceeded)); got != "runtime update rate exceeded the negotiated limit" {
		t.Fatalf("rate diagnostic = %q", got)
	}
	if got := promptStreamDiagnostic(fmt.Errorf("wrapped: %w", harnessv2.ErrEventByteRateExceeded)); got != "runtime update byte rate exceeded the protocol limit" {
		t.Fatalf("byte-rate diagnostic = %q", got)
	}
	if got := promptStreamDiagnostic(errors.New("provider-secret-must-not-leak")); got != "non-client prompt stream error" {
		t.Fatalf("generic diagnostic = %q", got)
	}
	if got := promptStreamDiagnostic(acpUpdatePersistenceError(errors.New("store unavailable"), nil)); got != "local execution update persistence failed" {
		t.Fatalf("persistence diagnostic = %q", got)
	}
}

func TestAppendPromptStreamFailureLifecycleIfNewClosesMissingTerminal(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	journalState, err := (v2eventjournal.Journal{
		EventStore: eventStore,
		MapContext: v2eventjournal.MapContext{
			Namespace: acpDispatcherTestNamespace, TaskName: acpDispatcherMissingTerminalTask, StreamID: acpDispatcherMissingTerminalTask,
		},
	}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	accepted := harnessv2.Event{
		Protocol: harnessv2.ProtocolVersion,
		Type:     harnessv2.EventAccepted,
		Identity: harnessv2.EventIdentity{
			RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot-id",
			RuntimeSessionUID: acpDispatcherRuntimeSession, RuntimeSessionGeneration: 1,
			TaskUID: acpDispatcherTaskUID, TaskAttempt: 1, PromptID: "prompt-missing-terminal", Sequence: 1,
			RequestDigest: harnessv2.RequestDigest(testControlDigestForDispatcher(acpDispatcherMissingTerminalTask)), Timestamp: now,
		},
		Accepted: &harnessv2.AcceptedEvent{
			AcceptedAt: now,
			Lease: harnessv2.PromptLease{
				Generation: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute),
			},
			ACPVersion: harnessv2.ACPProfileV1,
		},
	}
	if appended, isNew, err := journalState.AppendPromptLifecycleIfNew(ctx, accepted); err != nil || !isNew || appended == nil {
		t.Fatalf("append accepted lifecycle = %#v new=%t err=%v", appended, isNew, err)
	}
	if err := appendPromptStreamFailureLifecycleIfNew(
		ctx, journalState, now.Add(time.Second), harnessv2.ErrMissingTerminalEvent,
	); err != nil {
		t.Fatal(err)
	}
	listed, err := eventStore.ListExecutionEvents(ctx, store.ExecutionEventFilter{
		Namespace: acpDispatcherTestNamespace, StreamType: store.ExecutionEventStreamTypeTask, StreamID: acpDispatcherMissingTerminalTask,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || listed[0].Type != executionevents.ExecutionEventTypeModelRequestStarted ||
		listed[1].Type != executionevents.ExecutionEventTypeModelRequestFailed {
		t.Fatalf("missing-terminal lifecycle = %#v", listed)
	}
	var content map[string]any
	if err := json.Unmarshal(listed[1].Content, &content); err != nil {
		t.Fatal(err)
	}
	if content["controllerSynthesized"] != true || content["message"] != promptStreamMissingTerminalDiagnostic {
		t.Fatalf("missing-terminal lifecycle content = %#v", content)
	}
}

func TestPromptUpdatePersistenceFailureCancelsAndFailsWithoutRuntimeLost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "event-persistence.db"))
	defer closeStore()

	taskUID := types.UID("96969696-9696-9696-9696-969696969696")
	promptID := "prompt-event-persistence-1"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: acpDispatcherTestNamespace, Name: "event-persistence", UID: taskUID},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "persist updates"},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning, Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateRunning, Attempt: 1, PromptID: promptID,
			RequestDigest: testControlDigestForDispatcher("event-persistence"), ControllerEpoch: fence.Epoch,
		}},
	}
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(task).Build()
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		Key: key, RequestDigest: task.Status.Execution.RequestDigest,
	}), fence)
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []store.PromptExecutionState{
		store.PromptExecutionReserved, store.PromptExecutionSessionStarting, store.PromptExecutionPlanned,
		store.PromptExecutionSubmitting, store.PromptExecutionAccepted, store.PromptExecutionRunning,
	} {
		operation := "event-persistence-" + string(next)
		attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: next, OperationID: operation, OperationDigest: testControlDigestForDispatcher(operation), UpdatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	var cancelCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, "/cancel") {
			http.NotFound(w, r)
			return
		}
		var request harnessv2.CancelPromptRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode cancellation: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.Reason != harnessv2.CancelReasonStreamDisconnected {
			t.Errorf("cancel reason = %q", request.Reason)
		}
		cancelCalls.Add(1)
		writeDispatcherJSON(w, harnessv2.CancelPromptResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			BarrierState: harnessv2.CancellationBarrierSettled, SettlementProven: true,
			Settlement: harnessv2.PromptSettlement{
				TerminalEvent: harnessv2.EventCancelled, Outcome: harnessv2.PromptOutcomeCancelled,
				StopReason: harnessv2.ACPStopReasonCancelled, SettledAt: time.Now().UTC(),
			},
		})
	}))
	defer server.Close()
	runtimeClient, err := harnessv2.NewClient(
		server.URL,
		harnessv2.WithControllerBearerToken(strings.Repeat("t", 32)),
		harnessv2.WithOperationCapabilitySecret([]byte(strings.Repeat("s", 32))),
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeFence := harnessv2.Fence{
		RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot-id", ControllerEpoch: uint64(fence.Epoch),
		RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 1,
		RuntimeProfileDigest:       harnessv2.ProfileDigest(testControlDigestForDispatcher("event-persistence-profile")),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		RuntimeSessionUID:          acpDispatcherRuntimeSessionUID, RuntimeSessionGeneration: 1,
	}
	currentEpoch, err := controlStore.GetControllerEpoch(ctx, fence.Name)
	if err != nil {
		t.Fatal(err)
	}
	epochs := NewControllerEpochManager(controlStore, fence.HolderID)
	epochs.current = currentEpoch
	close(epochs.ready)
	dispatcher := &ACPDispatcher{Client: kubeClient, Store: controlStore, Epochs: epochs}
	if err := dispatcher.handlePromptStreamError(
		ctx, nil, runtimeClient, "runtime-session-1", task.DeepCopy(), attempt.ID, fence, runtimeFence,
		nil, true, harnessv2.RequestWriteEvidence{}, nil,
		acpUpdatePersistenceError(errors.New("event store unavailable"), nil),
	); err != nil {
		t.Fatal(err)
	}
	if cancelCalls.Load() != 1 {
		t.Fatalf("cancel calls = %d, want 1", cancelCalls.Load())
	}
	completed := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(task), completed); err != nil {
		t.Fatal(err)
	}
	if completed.Status.Execution == nil || completed.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
		completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeFailed ||
		completed.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("ExecutionEventPersistenceFailed") {
		t.Fatalf("persistence failure status = %#v", completed.Status.Execution)
	}
	if completed.Status.Execution.Reason == corev1alpha1.TaskExecutionReason("RuntimeLost") {
		t.Fatalf("persistence failure was misclassified as runtime loss: %#v", completed.Status.Execution)
	}
	attempt, err = controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionFailed {
		t.Fatalf("persistence failure attempt state = %s, want %s", attempt.ExecutionState, store.PromptExecutionFailed)
	}
}

type promptStreamLifecycleFixture struct {
	ctx            context.Context
	dispatcher     *ACPDispatcher
	runtimeClient  *harnessv2.Client
	task           *corev1alpha1.Task
	attemptID      string
	fence          store.ControllerEpochFence
	runtimeFence   harnessv2.Fence
	journalState   *v2eventjournal.State
	eventStore     *storetest.FakeExecutionEventStore
	controlStore   *sqlite.Store
	cancelRequests chan harnessv2.CancelPromptRequest
}

func newPromptStreamLifecycleFixture(
	t *testing.T,
	settlement harnessv2.PromptSettlement,
) *promptStreamLifecycleFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "stream-lifecycle.db"))
	t.Cleanup(closeStore)

	const promptID = "prompt-stream-lifecycle-1"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpDispatcherTestNamespace, Name: "stream-lifecycle", UID: types.UID("95959595-9595-9595-9595-959595959595"),
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "persist prompt lifecycle"},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning, Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateRunning, Attempt: 1, PromptID: promptID,
			RequestDigest: testControlDigestForDispatcher("stream-lifecycle"), ControllerEpoch: fence.Epoch,
		}},
	}
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(task).Build()
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		Key: key, RequestDigest: task.Status.Execution.RequestDigest,
	}), fence)
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []store.PromptExecutionState{
		store.PromptExecutionReserved, store.PromptExecutionSessionStarting, store.PromptExecutionPlanned,
		store.PromptExecutionSubmitting, store.PromptExecutionAccepted, store.PromptExecutionRunning,
	} {
		operation := "stream-lifecycle-" + string(next)
		attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: next, OperationID: operation, OperationDigest: testControlDigestForDispatcher(operation), UpdatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	cancelRequests := make(chan harnessv2.CancelPromptRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, "/cancel") {
			http.NotFound(w, r)
			return
		}
		var request harnessv2.CancelPromptRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode cancellation: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		cancelRequests <- request
		writeDispatcherJSON(w, harnessv2.CancelPromptResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			BarrierState: harnessv2.CancellationBarrierSettled, SettlementProven: true, Settlement: settlement,
		})
	}))
	t.Cleanup(server.Close)
	runtimeClient, err := harnessv2.NewClient(
		server.URL,
		harnessv2.WithControllerBearerToken(strings.Repeat("t", 32)),
		harnessv2.WithOperationCapabilitySecret([]byte(strings.Repeat("s", 32))),
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeFence := harnessv2.Fence{
		RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot-id", ControllerEpoch: uint64(fence.Epoch),
		RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 1,
		RuntimeProfileDigest:       harnessv2.ProfileDigest(testControlDigestForDispatcher("stream-lifecycle-profile")),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		RuntimeSessionUID:          acpDispatcherRuntimeSessionUID, RuntimeSessionGeneration: 1,
	}
	currentEpoch, err := controlStore.GetControllerEpoch(ctx, fence.Name)
	if err != nil {
		t.Fatal(err)
	}
	epochs := NewControllerEpochManager(controlStore, fence.HolderID)
	epochs.current = currentEpoch
	close(epochs.ready)

	eventStore := storetest.NewFakeExecutionEventStore()
	journalState, err := (v2eventjournal.Journal{
		EventStore: eventStore,
		MapContext: v2eventjournal.MapContext{
			Namespace: task.Namespace, TaskName: task.Name, StreamID: task.Name,
		},
	}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	accepted := harnessv2.Event{
		Protocol: harnessv2.ProtocolVersion,
		Type:     harnessv2.EventAccepted,
		Identity: harnessv2.EventIdentity{
			RuntimeInstanceID: runtimeFence.RuntimeInstanceID, SupervisorBootID: runtimeFence.SupervisorBootID,
			RuntimeSessionUID: runtimeFence.RuntimeSessionUID, RuntimeSessionGeneration: runtimeFence.RuntimeSessionGeneration,
			TaskUID: harnessv2.TaskUID(task.UID), TaskAttempt: 1, PromptID: harnessv2.PromptID(promptID), Sequence: 1,
			RequestDigest: harnessv2.RequestDigest(task.Status.Execution.RequestDigest), Timestamp: now,
		},
		Accepted: &harnessv2.AcceptedEvent{
			AcceptedAt: now,
			Lease: harnessv2.PromptLease{
				Generation: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute),
			},
			ACPVersion: harnessv2.ACPProfileV1,
		},
	}
	if appended, isNew, err := journalState.AppendPromptLifecycleIfNew(ctx, accepted); err != nil || !isNew || appended == nil {
		t.Fatalf("append accepted lifecycle = %#v new=%t err=%v", appended, isNew, err)
	}
	return &promptStreamLifecycleFixture{
		ctx: ctx, dispatcher: &ACPDispatcher{Client: kubeClient, Store: controlStore, Epochs: epochs},
		runtimeClient: runtimeClient, task: task, attemptID: attempt.ID, fence: fence, runtimeFence: runtimeFence,
		journalState: journalState, eventStore: eventStore, controlStore: controlStore, cancelRequests: cancelRequests,
	}
}

func (f *promptStreamLifecycleFixture) assertTerminalLifecycle(
	t *testing.T,
	terminal harnessv2.EventType,
	settlementProven bool,
) {
	t.Helper()
	listed, err := f.eventStore.ListExecutionEvents(f.ctx, store.ExecutionEventFilter{
		Namespace: f.task.Namespace, StreamType: store.ExecutionEventStreamTypeTask, StreamID: f.task.Name,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || listed[0].Type != executionevents.ExecutionEventTypeModelRequestStarted ||
		listed[1].Type != executionevents.ExecutionEventTypeModelRequestFailed {
		t.Fatalf("prompt lifecycle = %#v", listed)
	}
	var content map[string]any
	if err := json.Unmarshal(listed[1].Content, &content); err != nil {
		t.Fatal(err)
	}
	proven, _ := content["settlementProven"].(bool)
	if content["terminalEvent"] != string(terminal) || content["controllerSynthesized"] != true || proven != settlementProven {
		t.Fatalf("terminal lifecycle content = %#v", content)
	}
	trace := tasktrace.BuildTaskTrace(tasktrace.MetadataFromTask(f.task), listed, time.Now().UTC())
	if len(trace.ModelRequests) != 1 || trace.ModelRequests[0].Status == tasktrace.StatusRunning || trace.ModelRequests[0].EndSeq == 0 {
		t.Fatalf("terminal lifecycle trace = %#v", trace.ModelRequests)
	}
}

func TestPromptPlanPersistenceFailureClosesLifecycleAfterProvenSettlement(t *testing.T) {
	fixture := newPromptStreamLifecycleFixture(t, harnessv2.PromptSettlement{
		TerminalEvent: harnessv2.EventCancelled, Outcome: harnessv2.PromptOutcomeCancelled,
		StopReason: harnessv2.ACPStopReasonCancelled, SettledAt: time.Now().UTC(),
	})
	if err := fixture.dispatcher.handlePromptStreamError(
		fixture.ctx, nil, fixture.runtimeClient, "runtime-session-1", fixture.task.DeepCopy(), fixture.attemptID,
		fixture.fence, fixture.runtimeFence, fixture.journalState, true, harnessv2.RequestWriteEvidence{}, nil,
		acpUpdatePersistenceError(nil, errors.New("plan store unavailable")),
	); err != nil {
		t.Fatal(err)
	}
	request := <-fixture.cancelRequests
	if request.Reason != harnessv2.CancelReasonStreamDisconnected {
		t.Fatalf("cancel reason = %q", request.Reason)
	}
	completed := &corev1alpha1.Task{}
	if err := fixture.dispatcher.Client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.task), completed); err != nil {
		t.Fatal(err)
	}
	if completed.Status.Execution == nil || completed.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
		completed.Status.Execution.Reason != corev1alpha1.TaskExecutionReason(acpExecutionEventPersistenceFailureReason) {
		t.Fatalf("plan persistence failure status = %#v", completed.Status.Execution)
	}
	fixture.assertTerminalLifecycle(t, harnessv2.EventCancelled, true)
}

func TestPromptStreamFailureClosesLifecycleBeforeOutcomeUnknown(t *testing.T) {
	fixture := newPromptStreamLifecycleFixture(t, harnessv2.PromptSettlement{
		TerminalEvent: harnessv2.EventCancelled, Outcome: harnessv2.PromptOutcomeCancelled,
		StopReason: harnessv2.ACPStopReasonCancelled, SettledAt: time.Now().UTC(),
	})
	if err := fixture.dispatcher.handlePromptStreamError(
		fixture.ctx, nil, fixture.runtimeClient, "runtime-session-1", fixture.task.DeepCopy(), fixture.attemptID,
		fixture.fence, fixture.runtimeFence, fixture.journalState, true, harnessv2.RequestWriteEvidence{}, nil,
		errors.New("stream disconnected"),
	); err != nil {
		t.Fatal(err)
	}
	if len(fixture.cancelRequests) != 0 {
		t.Fatal("non-context stream failure unexpectedly cancelled the prompt")
	}
	completed := &corev1alpha1.Task{}
	if err := fixture.dispatcher.Client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.task), completed); err != nil {
		t.Fatal(err)
	}
	if completed.Status.Execution == nil || completed.Status.Execution.State != corev1alpha1.TaskExecutionStateOutcomeUnknown ||
		completed.Status.Execution.Reason != "RuntimeLost" {
		t.Fatalf("stream failure status = %#v", completed.Status.Execution)
	}
	fixture.assertTerminalLifecycle(t, harnessv2.EventOutcomeUnknown, false)
}

func TestPromptTimeoutPersistsProvenCancellationSettlement(t *testing.T) {
	for _, test := range []struct {
		name           string
		settlement     harnessv2.PromptSettlement
		wantState      corev1alpha1.TaskExecutionState
		wantReason     corev1alpha1.TaskExecutionReason
		wantAttempt    store.PromptExecutionState
		attemptReason  string
		attemptMessage string
		wantTerminal   harnessv2.EventType
	}{
		{
			name: acpCancelledOperation,
			settlement: harnessv2.PromptSettlement{
				TerminalEvent: harnessv2.EventCancelled, Outcome: harnessv2.PromptOutcomeCancelled,
				StopReason: harnessv2.ACPStopReasonCancelled, SettledAt: time.Now().UTC(),
			},
			wantState: corev1alpha1.TaskExecutionStateCancelled, wantReason: acpTaskTimeoutReason,
			wantAttempt: store.PromptExecutionCancelled, attemptReason: string(acpTaskTimeoutReason),
			attemptMessage: acpTaskTimeoutCancellationSettledMessage, wantTerminal: harnessv2.EventCancelled,
		},
		{
			name: acpPromptOutcomeFailed,
			settlement: harnessv2.PromptSettlement{
				TerminalEvent: harnessv2.EventFailed, Outcome: harnessv2.PromptOutcomeFailed,
				StopReason: harnessv2.ACPStopReasonRefusal, SettledAt: time.Now().UTC(),
			},
			wantState: corev1alpha1.TaskExecutionStateFailed, wantReason: harnessV1ReasonFailed,
			wantAttempt: store.PromptExecutionFailed, wantTerminal: harnessv2.EventFailed,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPromptStreamLifecycleFixture(t, test.settlement)
			if err := fixture.dispatcher.handlePromptStreamError(
				fixture.ctx, nil, fixture.runtimeClient, "runtime-session-1", fixture.task.DeepCopy(), fixture.attemptID,
				fixture.fence, fixture.runtimeFence, fixture.journalState, true, harnessv2.RequestWriteEvidence{},
				context.DeadlineExceeded, context.DeadlineExceeded,
			); err != nil {
				t.Fatal(err)
			}
			request := <-fixture.cancelRequests
			if request.Reason != harnessv2.CancelReasonTaskTimeout {
				t.Fatalf("cancel reason = %q", request.Reason)
			}
			completed := &corev1alpha1.Task{}
			if err := fixture.dispatcher.Client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.task), completed); err != nil {
				t.Fatal(err)
			}
			if completed.Status.Execution == nil || completed.Status.Execution.State != test.wantState ||
				completed.Status.Execution.Reason != test.wantReason {
				t.Fatalf("timeout settlement status = %#v", completed.Status.Execution)
			}
			attempt, err := fixture.controlStore.GetPromptAttempt(fixture.ctx, fixture.attemptID)
			if err != nil {
				t.Fatal(err)
			}
			if attempt.ExecutionState != test.wantAttempt || attempt.TerminalReason != test.attemptReason ||
				attempt.OutcomeMarker != test.attemptMessage {
				t.Fatalf(
					"timeout settlement attempt = state %s reason %q message %q, want %s/%q/%q",
					attempt.ExecutionState, attempt.TerminalReason, attempt.OutcomeMarker,
					test.wantAttempt, test.attemptReason, test.attemptMessage,
				)
			}
			fixture.assertTerminalLifecycle(t, test.wantTerminal, true)
		})
	}
}

func TestACPDispatcherExplicitRuntimeRPCFailureIsPromptFailed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "rpc-failure.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs, stopEpoch := startACPRecoveryEpochManager(t, ctx, controlStore, "rpc-failure")
	defer stopEpoch()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	taskUID := types.UID("97979797-9797-9797-9797-979797979797")
	promptID := "prompt-" + string(taskUID) + "-1"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "rpc-failure", UID: taskUID},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "fail explicitly"},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning, Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateRunning, Attempt: 1, PromptID: promptID,
			RequestDigest: testControlDigestForDispatcher("rpc-failure"), ControllerEpoch: fence.Epoch,
		}},
	}
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(task).Build()
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{Key: key, RequestDigest: task.Status.Execution.RequestDigest}), fence)
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []store.PromptExecutionState{
		store.PromptExecutionReserved, store.PromptExecutionSessionStarting, store.PromptExecutionPlanned,
		store.PromptExecutionSubmitting, store.PromptExecutionAccepted, store.PromptExecutionRunning,
	} {
		operation := "rpc-failure-" + string(next)
		attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: next, OperationID: operation, OperationDigest: testControlDigestForDispatcher(operation), UpdatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	dispatcher := &ACPDispatcher{Client: kubeClient, Store: controlStore, Epochs: epochs}
	terminal := harnessv2.Event{
		Protocol: harnessv2.ProtocolVersion,
		Type:     harnessv2.EventFailed,
		Failed: &harnessv2.FailedEvent{
			StopReason: harnessv2.ACPStopReasonRefusal, Code: "acp_prompt_failed",
			Message: "ACP prompt failed", Retryable: false,
		},
	}
	if err := dispatcher.finishNonSuccess(ctx, task.DeepCopy(), attempt.ID, fence, nil, terminal); err != nil {
		t.Fatal(err)
	}
	completed := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, completed); err != nil {
		t.Fatal(err)
	}
	if completed.Status.Phase != corev1alpha1.TaskPhaseFailed || completed.Status.Execution == nil ||
		completed.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
		completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeFailed ||
		completed.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("PromptFailed") {
		t.Fatalf("explicit runtime RPC failure status = %#v", completed.Status)
	}
	if completed.Status.Execution.Reason == corev1alpha1.TaskExecutionReason("RuntimeLost") ||
		completed.Status.Execution.Outcome == corev1alpha1.TaskExecutionOutcomeOutcomeUnknown {
		t.Fatalf("explicit runtime RPC failure was misclassified as runtime loss: %#v", completed.Status.Execution)
	}
	attempt, err = controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionFailed {
		t.Fatalf("explicit runtime RPC failure attempt state = %s, want %s", attempt.ExecutionState, store.PromptExecutionFailed)
	}
}

//nolint:goconst,gocyclo // Repeated literals and end-to-end assertions intentionally stay together.
func TestACPDispatcherExecutesNoChangeTask(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	taskUID := types.UID("11111111-1111-1111-1111-111111111111")
	promptID := "prompt-" + string(taskUID) + "-1"
	continuedTaskUID := types.UID("22222222-2222-2222-2222-222222222222")
	failedTaskUID := types.UID("33333333-3333-3333-3333-333333333333")
	cancelledTaskUID := types.UID("44444444-4444-4444-4444-444444444444")
	unknownTaskUID := types.UID("55555555-5555-5555-5555-555555555555")
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "task", UID: taskUID, Labels: map[string]string{acpRuntimeTaskPoolLabel: "pool"}},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "respond briefly", AgentRuntime: &corev1alpha1.AgentRuntimeSpec{},
			AgentRef:   &corev1alpha1.AgentReference{Name: "agent"},
			SessionRef: &corev1alpha1.SessionReference{Name: "session", Create: true, Append: true},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending, Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateQueued, Attempt: 1, PromptID: promptID, RuntimePoolName: "pool", RuntimePoolUID: "pool-uid",
			RequestDigest: testControlDigestForDispatcher("task-request"), ControllerEpoch: 1,
		}},
	}
	spanHarness, parentSpanID := stampACPTaskTrace(t, task)
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "agent", UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	images := ACPRuntimeImages{Codex: "docker.io/example/acp@sha256:" + strings.Repeat("a", 64)}
	plan := frozenACPDispatcherPlanForTest(t, task, agent, images)
	profile := plan.Profile
	profileDigest := plan.Digest
	task.Labels[acpRuntimeTaskPoolLabel] = plan.PoolName
	task.Status.Execution.RuntimePoolName = plan.PoolName
	server := newDispatcherRuntimeServerWithTerminalEvents(t, profile, profileDigest, map[harnessv2.PromptID]harnessv2.EventType{
		harnessv2.PromptID("prompt-" + string(failedTaskUID) + "-1"):    harnessv2.EventFailed,
		harnessv2.PromptID("prompt-" + string(cancelledTaskUID) + "-1"): harnessv2.EventCancelled,
		harnessv2.PromptID("prompt-" + string(unknownTaskUID) + "-1"):   harnessv2.EventOutcomeUnknown,
	})
	defer server.Close()
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: plan.PoolName, UID: types.UID("pool-uid"), Generation: 1},
		Spec: corev1alpha1.RuntimePoolSpec{
			RuntimeNamespace: "orka-runtimes", Runtime: corev1alpha1.RuntimePoolRuntimeSpec{
				Image: plan.Image, Profile: RuntimePoolProfileFromPlan(plan),
			}, DesiredReplicas: 1,
		},
		Status: corev1alpha1.RuntimePoolStatus{
			Lifecycle: corev1alpha1.RuntimePoolLifecycleServing, AdmissionState: corev1alpha1.RuntimePoolAdmissionAccepting,
			ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{
				PodNamespace: "orka-runtimes", PodName: "runtime-pod", PodAddress: parsed.Host,
				PodUID: "pod-uid", BootID: "boot-id", RuntimeInstanceID: "pod-uid.boot-id", ControllerEpoch: 1,
				ProtocolVersion: corev1alpha1.RuntimePoolProtocolHarnessV2, ProfileDigest: string(profileDigest), ProfileDigestSchemaVersion: strconv.FormatUint(uint64(harnessv2.ProfileDigestSchemaVersion), 10),
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "orka-runtimes", Name: "pool-auth-e1", Labels: map[string]string{
			runtimePoolAuthLabel: "true", runtimePoolUIDLabel: string(pool.UID),
		}},
		Data: map[string][]byte{runtimePoolControllerTokenKey: []byte(strings.Repeat("t", 32)), runtimePoolCapabilitySecretKey: []byte(strings.Repeat("s", 32))},
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: task.Namespace, UID: types.UID("default-namespace-uid"),
	}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(
		&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}, &corev1alpha1.ControllerEpoch{},
		&corev1alpha1.PromptAttempt{}, &corev1alpha1.RuntimeSessionControl{},
		&corev1alpha1.BranchClaim{}, &corev1alpha1.Publication{}, &corev1alpha1.ExternalEffect{},
	).WithObjects(task, pool, secret, agent, namespace).Build()
	kubeClient = withControllerEpochLeaseUIDs(t, kubeClient)
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	persistence := sqlite.NewStore(db, "test")
	controlStore, err := kubestore.NewComposite(kubeClient, "orka-system", persistence, kubestore.WithAPIReader(kubeClient))
	if err != nil {
		t.Fatal(err)
	}
	epochs := NewControllerEpochManager(controlStore, "controller-test").WithMirror(persistence)
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task = prepareBoundACPDispatcherTaskWithStoresForTest(
		t, ctx, kubeClient, scheme, controlStore, persistence, task, agent, images,
	)
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attemptID, err := key.CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		ID: attemptID, Key: key, RequestDigest: task.Status.Execution.RequestDigest,
		BindingDigest: task.Status.AgentExecutionBinding.BindingDigest, SnapshotDigest: task.Status.AgentExecutionBinding.Snapshot.Digest,
		ExecutionState: store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
	}), fence); err != nil {
		t.Fatal(err)
	}
	continuity, err := NewACPSessionContinuity(ACPSessionContinuityConfig{
		SessionControls: controlStore, Transcripts: persistence, Publications: controlStore, BranchClaims: controlStore,
		Lineages: persistence,
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &ACPDispatcher{
		Client: kubeClient, APIReader: kubeClient, Store: controlStore, ResultStore: persistence,
		Snapshots: persistence, Epochs: epochs, Sessions: continuity,
	}
	prepareAdditionalTask := func(
		name string,
		uid types.UID,
		prompt string,
		sessionRef *corev1alpha1.SessionReference,
	) (*corev1alpha1.Task, string, string) {
		t.Helper()
		additionalPromptID := "prompt-" + string(uid) + "-1"
		additional := &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: task.Namespace,
				Name:      name,
				UID:       uid,
				Labels:    map[string]string{acpRuntimeTaskPoolLabel: plan.PoolName},
			},
			Spec: corev1alpha1.TaskSpec{
				Type:         corev1alpha1.TaskTypeAgent,
				Prompt:       prompt,
				AgentRuntime: &corev1alpha1.AgentRuntimeSpec{},
				AgentRef:     &corev1alpha1.AgentReference{Name: agent.Name},
				SessionRef:   sessionRef,
			},
			Status: corev1alpha1.TaskStatus{
				Phase:    corev1alpha1.TaskPhasePending,
				Attempts: 1,
				Execution: &corev1alpha1.TaskExecutionStatus{
					State:           corev1alpha1.TaskExecutionStateQueued,
					Attempt:         1,
					PromptID:        additionalPromptID,
					RuntimePoolName: plan.PoolName,
					RuntimePoolUID:  string(pool.UID),
					RequestDigest:   testControlDigestForDispatcher(name + "-request"),
					ControllerEpoch: fence.Epoch,
				},
			},
		}
		traceCtx, parent := orkatracing.Tracer("test").Start(context.Background(), "task.creator."+name)
		parentID := parent.SpanContext().SpanID().String()
		orkatracing.StampTaskTraceContext(traceCtx, additional)
		parent.End()
		if err := kubeClient.Create(ctx, additional); err != nil {
			t.Fatal(err)
		}
		additional = prepareBoundACPDispatcherTaskWithStoresForTest(
			t, ctx, kubeClient, scheme, controlStore, persistence, additional, agent, images,
		)
		key := store.PromptAttemptKey{
			Namespace: additional.Namespace,
			TaskUID:   string(additional.UID),
			Attempt:   1,
			PromptID:  additionalPromptID,
		}
		additionalAttemptID, err := key.CanonicalID()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
			ID:             additionalAttemptID,
			Key:            key,
			RequestDigest:  additional.Status.Execution.RequestDigest,
			BindingDigest:  additional.Status.AgentExecutionBinding.BindingDigest,
			SnapshotDigest: additional.Status.AgentExecutionBinding.Snapshot.Digest,
			ExecutionState: store.PromptExecutionQueued,
			DeliveryState:  store.PromptDeliveryNotRequested,
		}), fence); err != nil {
			t.Fatal(err)
		}
		return additional, additionalAttemptID, parentID
	}
	dispatchQueuedTask(ctx, t, dispatcher, task.DeepCopy())
	completed := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, completed); err != nil {
		t.Fatal(err)
	}
	if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded || completed.Status.Execution == nil || completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded ||
		completed.Status.Delivery == nil || completed.Status.Delivery.Outcome != corev1alpha1.TaskDeliveryOutcomeReadValidated ||
		completed.Status.ResultRef == nil || !completed.Status.ResultRef.Available {
		t.Fatalf("unexpected completed status: %#v", completed.Status)
	}
	result, err := persistence.GetResult(ctx, task.Namespace, task.Name)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "from runtime" {
		t.Fatalf("result = %q", result)
	}
	transcript, err := persistence.LoadTranscript(ctx, "default", "session", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(transcript) != 2 || transcript[0].Role != "user" || transcript[1].Role != "assistant" {
		t.Fatalf("session transcript = %#v", transcript)
	}
	timeline, err := persistence.ListExecutionEvents(ctx, store.ExecutionEventFilter{
		Namespace: task.Namespace, StreamType: store.ExecutionEventStreamTypeTask, StreamID: task.Name,
		Limit: store.MaxExecutionEventLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	typeCounts := map[string]int{}
	var usageContent map[string]any
	var terminalLifecycleSeq, terminalMessageSeq, terminalUsageSeq int64
	var terminalMessageText string
	for _, event := range timeline {
		typeCounts[event.Type]++
		switch event.Type {
		case executionevents.ExecutionEventTypeModelRequestCompleted:
			terminalLifecycleSeq = event.Seq
		case executionevents.ExecutionEventTypeModelMessage:
			terminalMessageSeq = event.Seq
			terminalMessageText = event.ContentText
		case executionevents.ExecutionEventTypeModelUsageUpdated:
			terminalUsageSeq = event.Seq
			if err := json.Unmarshal(event.Content, &usageContent); err != nil {
				t.Fatal(err)
			}
		}
	}
	for eventType, want := range map[string]int{
		executionevents.ExecutionEventTypeModelRequestStarted:        1,
		executionevents.ExecutionEventTypeModelRequestCompleted:      1,
		executionevents.ExecutionEventTypeModelMessage:               1,
		executionevents.ExecutionEventTypeToolCallStarted:            1,
		executionevents.ExecutionEventTypeToolCallCompleted:          1,
		executionevents.ExecutionEventTypePlanUpdated:                1,
		executionevents.ExecutionEventTypeModelUsageUpdated:          1,
		executionevents.ExecutionEventTypeAgentRuntimeCommandStarted: 1,
	} {
		if typeCounts[eventType] != want {
			t.Fatalf("event count for %s = %d, want %d; timeline=%#v", eventType, typeCounts[eventType], want, timeline)
		}
	}
	if usageContent["inputTokens"] != float64(100) || usageContent["outputTokens"] != float64(25) ||
		usageContent["cachedInputTokens"] != float64(40) || usageContent["model"] != "served-model" {
		t.Fatalf("usage content = %#v", usageContent)
	}
	if terminalLifecycleSeq <= terminalMessageSeq || terminalLifecycleSeq <= terminalUsageSeq ||
		terminalMessageSeq == 0 || terminalUsageSeq == 0 {
		t.Fatalf(
			"terminal event order lifecycle=%d message=%d usage=%d; timeline=%#v",
			terminalLifecycleSeq, terminalMessageSeq, terminalUsageSeq, timeline,
		)
	}
	if terminalMessageText != "hello from runtime" {
		t.Fatalf("terminal assistant transcript = %q", terminalMessageText)
	}
	trace := tasktrace.BuildTaskTrace(tasktrace.MetadataFromTask(completed), timeline, time.Now().UTC())
	if len(trace.ModelRequests) != 1 || trace.ModelRequests[0].Status != tasktrace.StatusCompleted ||
		trace.ModelRequests[0].StartSeq == 0 || trace.ModelRequests[0].EndSeq != terminalLifecycleSeq ||
		trace.ModelRequests[0].EndSeq <= trace.ModelRequests[0].StartSeq {
		t.Fatalf("ACP trace model requests = %#v", trace.ModelRequests)
	}
	if len(trace.ToolCalls) != 1 || trace.ToolCalls[0].Status != tasktrace.StatusCompleted || trace.ToolCalls[0].StartSeq == 0 || trace.ToolCalls[0].EndSeq <= trace.ToolCalls[0].StartSeq {
		t.Fatalf("ACP trace tool calls = %#v", trace.ToolCalls)
	}
	planState, err := persistence.GetPlan(ctx, task.Namespace, task.Name)
	if err != nil {
		t.Fatal(err)
	}
	if planState.ProgressPct != 50 || planState.GoalComplete || !strings.Contains(planState.PlanDocument, "verify result") {
		t.Fatalf("ACP plan state = %#v", planState)
	}
	attempt, err := controlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionSucceeded || attempt.DeliveryState != store.PromptDeliveryReadValidated {
		t.Fatalf("attempt = %#v", attempt)
	}
	completedPool := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, completedPool); err != nil {
		t.Fatal(err)
	}
	if len(completedPool.Status.Capacity.Reservations) != 0 || completedPool.Status.Capacity.ReservedSessions != 0 || completedPool.Status.Capacity.ReservedPrompts != 0 {
		t.Fatalf("capacity reservation leaked after acceptance: %#v", completedPool.Status.Capacity)
	}

	continuedTask, continuedAttemptID, continuedParentID := prepareAdditionalTask(
		"continued-task",
		continuedTaskUID,
		"continue briefly",
		&corev1alpha1.SessionReference{Name: "session", Create: false, Append: true},
	)
	dispatchQueuedTask(ctx, t, dispatcher, continuedTask.DeepCopy())
	continuedCompleted := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(continuedTask), continuedCompleted); err != nil {
		t.Fatal(err)
	}
	if continuedCompleted.Status.Phase != corev1alpha1.TaskPhaseSucceeded || continuedCompleted.Status.Execution == nil ||
		continuedCompleted.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded {
		t.Fatalf("unexpected continued Task status: %#v", continuedCompleted.Status)
	}
	if continuedCompleted.Status.Execution.RuntimeSessionUID != completed.Status.Execution.RuntimeSessionUID {
		t.Fatalf(
			"continued RuntimeSession UID = %q, want reused %q",
			continuedCompleted.Status.Execution.RuntimeSessionUID,
			completed.Status.Execution.RuntimeSessionUID,
		)
	}
	continuedAttempt, err := controlStore.GetPromptAttempt(ctx, continuedAttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if continuedAttempt.ExecutionState != store.PromptExecutionSucceeded || continuedAttempt.DeliveryState != store.PromptDeliveryReadValidated {
		t.Fatalf("continued attempt = %#v", continuedAttempt)
	}
	transcript, err = persistence.LoadTranscript(ctx, "default", "session", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(transcript) != 4 || transcript[2].Role != "user" || transcript[3].Role != "assistant" {
		t.Fatalf("continued session transcript = %#v", transcript)
	}
	continuedSessionSpan := acpSpanForTask(t, spanHarness.Recorder.Ended(), acpSessionContinueSpanName, continuedTask.Name)
	if got := continuedSessionSpan.Parent().SpanID().String(); got != continuedParentID {
		t.Fatalf("acp.session.continue parent = %s, want Task trace parent %s", got, continuedParentID)
	}
	if got := tracingtest.AttributeMap(continuedSessionSpan)[acpAttrSessionOutcome].AsString(); got != acpSessionOutcomeContinued {
		t.Fatalf("acp.session.continue outcome = %q, want %q", got, acpSessionOutcomeContinued)
	}

	terminalTests := []struct {
		name             string
		uid              types.UID
		wantPhase        corev1alpha1.TaskPhase
		wantState        corev1alpha1.TaskExecutionState
		wantOutcome      corev1alpha1.TaskExecutionOutcome
		wantAttemptState store.PromptExecutionState
		wantSpanOutcome  string
		wantSpanError    bool
		wantErrorType    string
	}{
		{
			name: "failed", uid: failedTaskUID,
			wantPhase: corev1alpha1.TaskPhaseFailed, wantState: corev1alpha1.TaskExecutionStateFailed,
			wantOutcome: corev1alpha1.TaskExecutionOutcomeFailed, wantAttemptState: store.PromptExecutionFailed,
			wantSpanOutcome: acpPromptOutcomeFailed, wantSpanError: true, wantErrorType: "acp.prompt.failed",
		},
		{
			name: "cancelled", uid: cancelledTaskUID,
			wantPhase: corev1alpha1.TaskPhaseCancelled, wantState: corev1alpha1.TaskExecutionStateCancelled,
			wantOutcome: corev1alpha1.TaskExecutionOutcomeCancelled, wantAttemptState: store.PromptExecutionCancelled,
			wantSpanOutcome: acpPromptOutcomeCancelled,
		},
		{
			name: "outcome-unknown", uid: unknownTaskUID,
			wantPhase: corev1alpha1.TaskPhaseFailed, wantState: corev1alpha1.TaskExecutionStateOutcomeUnknown,
			wantOutcome: corev1alpha1.TaskExecutionOutcomeOutcomeUnknown, wantAttemptState: store.PromptExecutionOutcomeUnknown,
			wantSpanOutcome: acpPromptOutcomeUnknown, wantSpanError: true, wantErrorType: "acp.prompt.outcome_unknown",
		},
	}
	for _, test := range terminalTests {
		terminalTask, terminalAttemptID, terminalParentID := prepareAdditionalTask(
			"terminal-"+test.name,
			test.uid,
			"exercise "+test.name+" telemetry",
			nil,
		)
		dispatchQueuedTask(ctx, t, dispatcher, terminalTask.DeepCopy())
		terminalCompleted := &corev1alpha1.Task{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(terminalTask), terminalCompleted); err != nil {
			t.Fatal(err)
		}
		if terminalCompleted.Status.Phase != test.wantPhase || terminalCompleted.Status.Execution == nil ||
			terminalCompleted.Status.Execution.State != test.wantState || terminalCompleted.Status.Execution.Outcome != test.wantOutcome {
			t.Fatalf("%s terminal status = %#v", test.name, terminalCompleted.Status)
		}
		terminalAttempt, err := controlStore.GetPromptAttempt(ctx, terminalAttemptID)
		if err != nil {
			t.Fatal(err)
		}
		if terminalAttempt.ExecutionState != test.wantAttemptState {
			t.Fatalf("%s attempt state = %s, want %s", test.name, terminalAttempt.ExecutionState, test.wantAttemptState)
		}
		terminalSpan := acpSpanForTask(t, spanHarness.Recorder.Ended(), acpPromptSpanName, terminalTask.Name)
		if got := terminalSpan.Parent().SpanID().String(); got != terminalParentID {
			t.Fatalf("%s acp.prompt parent = %s, want Task trace parent %s", test.name, got, terminalParentID)
		}
		terminalAttrs := tracingtest.AttributeMap(terminalSpan)
		if got := terminalAttrs[acpAttrPromptOutcome].AsString(); got != test.wantSpanOutcome {
			t.Fatalf("%s acp.prompt outcome = %q, want %q", test.name, got, test.wantSpanOutcome)
		}
		if got := terminalSpan.Status().Code == codes.Error; got != test.wantSpanError {
			t.Fatalf("%s acp.prompt error status = %t, want %t", test.name, got, test.wantSpanError)
		}
		errorType, hasErrorType := terminalAttrs["error.type"]
		if test.wantErrorType == "" && hasErrorType {
			t.Fatalf("%s acp.prompt error.type = %q, want absent", test.name, errorType.AsString())
		}
		if test.wantErrorType != "" && (!hasErrorType || errorType.AsString() != test.wantErrorType) {
			t.Fatalf("%s acp.prompt error.type = %q, want %q", test.name, errorType.AsString(), test.wantErrorType)
		}
	}
	spans := spanHarness.Recorder.Ended()
	promptSpan := tracingtest.SpanNamed(spans, acpPromptSpanName)
	if promptSpan == nil {
		t.Fatal("missing acp.prompt span")
	}
	if got := promptSpan.Parent().SpanID().String(); got != parentSpanID {
		t.Fatalf("acp.prompt parent = %s, want Task trace parent %s", got, parentSpanID)
	}
	promptAttrs := tracingtest.AttributeMap(promptSpan)
	if got := promptAttrs[orkatracing.AttrTaskID].AsString(); got != task.Name {
		t.Fatalf("acp.prompt task id = %q, want %q", got, task.Name)
	}
	if got := promptAttrs[acpAttrRuntimePoolName].AsString(); got != plan.PoolName {
		t.Fatalf("acp.prompt runtime pool = %q, want %q", got, plan.PoolName)
	}
	if got := promptAttrs[acpAttrRuntimeSessionUID].AsString(); got != completed.Status.Execution.RuntimeSessionUID {
		t.Fatalf("acp.prompt runtime session UID = %q, want %q", got, completed.Status.Execution.RuntimeSessionUID)
	}
	if got := promptAttrs[acpAttrRuntimeSessionGen].AsInt64(); got != completed.Status.Execution.RuntimeSessionGeneration {
		t.Fatalf("acp.prompt runtime session generation = %d, want %d", got, completed.Status.Execution.RuntimeSessionGeneration)
	}
	if got := promptAttrs[acpAttrPromptOutcome].AsString(); got != acpPromptOutcomeSucceeded {
		t.Fatalf("acp.prompt outcome = %q, want %q", got, acpPromptOutcomeSucceeded)
	}
	sessionSpan := tracingtest.SpanNamed(spans, acpSessionCreateSpanName)
	if sessionSpan == nil {
		t.Fatal("missing acp.session.create span")
	}
	if got := sessionSpan.Parent().SpanID().String(); got != parentSpanID {
		t.Fatalf("acp.session.create parent = %s, want Task trace parent %s", got, parentSpanID)
	}
	if got := tracingtest.AttributeMap(sessionSpan)[acpAttrSessionOutcome].AsString(); got != acpSessionOutcomeCreated {
		t.Fatalf("acp.session.create outcome = %q, want %q", got, acpSessionOutcomeCreated)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestACPDispatcherUsesFrozenAgentAndToolAfterLiveResourcesChange(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(context.Context, client.Client, *corev1alpha1.Agent, *corev1alpha1.Tool) error
	}{
		{
			name: "mutated",
			change: func(ctx context.Context, kubeClient client.Client, agent *corev1alpha1.Agent, tool *corev1alpha1.Tool) error {
				currentAgent := &corev1alpha1.Agent{}
				if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(agent), currentAgent); err != nil {
					return err
				}
				currentAgent.Generation++
				currentAgent.Spec.Model.Name = "mutated-model"
				currentAgent.Spec.Runtime.DefaultAllowedTools = []string{"different-tool"}
				if err := kubeClient.Update(ctx, currentAgent); err != nil {
					return err
				}
				currentTool := &corev1alpha1.Tool{}
				if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(tool), currentTool); err != nil {
					return err
				}
				currentTool.Generation++
				currentTool.Spec.Description = "mutated tool description"
				currentTool.Status.Endpoint = "https://mutated.example.test/tool"
				return kubeClient.Update(ctx, currentTool)
			},
		},
		{
			name: "deleted",
			change: func(ctx context.Context, kubeClient client.Client, agent *corev1alpha1.Agent, tool *corev1alpha1.Tool) error {
				if err := kubeClient.Delete(ctx, agent.DeepCopy()); err != nil {
					return err
				}
				return kubeClient.Delete(ctx, tool.DeepCopy())
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			scheme := runtime.NewScheme()
			if err := corev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}

			const toolName = "frozen-custom-tool"
			taskUID := types.UID("frozen-" + test.name + "-task-uid")
			promptID := "prompt-" + string(taskUID) + "-1"
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default", Name: "frozen-" + test.name, UID: taskUID,
					Labels: map[string]string{acpRuntimeTaskPoolLabel: "pending"},
				},
				Spec: corev1alpha1.TaskSpec{
					Type: corev1alpha1.TaskTypeAgent, Prompt: "use the frozen configuration",
					AgentRef: &corev1alpha1.AgentReference{Name: "frozen-agent"},
				},
				Status: corev1alpha1.TaskStatus{
					Phase: corev1alpha1.TaskPhasePending, Attempts: 1,
					Execution: &corev1alpha1.TaskExecutionStatus{
						State: corev1alpha1.TaskExecutionStateQueued, Attempt: 1, PromptID: promptID,
						RuntimePoolName: "pending", RuntimePoolUID: "frozen-pool-uid",
						RequestDigest: testControlDigestForDispatcher("pre-binding-request"), ControllerEpoch: 1,
					},
				},
			}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default", Name: "frozen-agent", UID: types.UID("frozen-agent-uid"), Generation: 1,
				},
				Spec: corev1alpha1.AgentSpec{
					Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
					Runtime: &corev1alpha1.AgentCLIRuntime{
						Type: corev1alpha1.AgentRuntimeClaude, ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
						DefaultAllowedTools: []string{toolName},
					},
				},
			}
			tool := &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default", Name: toolName, UID: types.UID("frozen-tool-uid"), Generation: 1,
				},
				Spec: corev1alpha1.ToolSpec{
					Description:       "original frozen tool description",
					BrokeredToolClass: corev1alpha1.AgentRuntimeBrokeredToolClassRead,
					HTTP:              &corev1alpha1.HTTPExecution{URL: "https://original.example.test/tool", Method: http.MethodPost},
				},
				Status: corev1alpha1.ToolStatus{Available: true, Endpoint: "https://original.example.test/tool"},
			}
			images := ACPRuntimeImages{Claude: "docker.io/example/claude@sha256:" + strings.Repeat("a", 64)}
			plan := frozenACPDispatcherPlanForTest(t, task, agent, images)
			task.Labels[acpRuntimeTaskPoolLabel] = plan.PoolName
			task.Status.Execution.RuntimePoolName = plan.PoolName

			createRequests := make(chan harnessv2.CreateRuntimeSessionRequest, 1)
			server := newDispatcherRuntimeServerForPool(t, plan.Profile, plan.Digest, "frozen-pool-uid", func(request harnessv2.CreateRuntimeSessionRequest) {
				createRequests <- request
			})
			defer server.Close()
			parsed, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			pool := &corev1alpha1.RuntimePool{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default", Name: plan.PoolName, UID: types.UID("frozen-pool-uid"), Generation: 1,
				},
				Spec: corev1alpha1.RuntimePoolSpec{
					RuntimeNamespace: "orka-runtimes",
					Runtime:          corev1alpha1.RuntimePoolRuntimeSpec{Image: plan.Image, Profile: RuntimePoolProfileFromPlan(plan)},
					DesiredReplicas:  1,
				},
				Status: corev1alpha1.RuntimePoolStatus{
					Lifecycle:      corev1alpha1.RuntimePoolLifecycleServing,
					AdmissionState: corev1alpha1.RuntimePoolAdmissionAccepting,
					ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{
						PodNamespace: "orka-runtimes", PodName: "runtime-pod", PodAddress: parsed.Host,
						PodUID: "pod-uid", BootID: "boot-id", RuntimeInstanceID: "pod-uid.boot-id", ControllerEpoch: 1,
						ProtocolVersion:            corev1alpha1.RuntimePoolProtocolHarnessV2,
						ProfileDigest:              string(plan.Digest),
						ProfileDigestSchemaVersion: strconv.FormatUint(uint64(harnessv2.ProfileDigestSchemaVersion), 10),
					},
				},
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "orka-runtimes", Name: "frozen-pool-auth-e1",
					Labels: map[string]string{runtimePoolAuthLabel: "true", runtimePoolUIDLabel: string(pool.UID)},
				},
				Data: map[string][]byte{
					runtimePoolControllerTokenKey:  []byte(strings.Repeat("t", 32)),
					runtimePoolCapabilitySecretKey: []byte(strings.Repeat("s", 32)),
				},
			}
			kubeClient := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).
				WithObjects(task, pool, secret, agent, tool).Build()
			db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "frozen-dispatch.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close() //nolint:errcheck
			controlStore := sqlite.NewStore(db, "frozen-dispatch-test")
			epochs, stopEpoch := startACPRecoveryEpochManager(t, ctx, controlStore, "frozen-"+test.name)
			defer stopEpoch()
			fence, err := epochs.CurrentFence(ctx)
			if err != nil {
				t.Fatal(err)
			}
			task = prepareBoundACPDispatcherTaskForTest(t, ctx, kubeClient, scheme, controlStore, task, agent, images)
			verifier := TaskReconciler{
				Client: kubeClient, APIReader: kubeClient,
				AgentExecutionSnapshots: controlStore,
			}
			frozen, err := verifier.loadVerifiedBoundExecution(ctx, task, task.Status.AgentExecutionBinding)
			if err != nil {
				t.Fatal(err)
			}
			wantAgentConfiguration := frozen.configuration
			wantMCPConfiguration := frozen.mcpConfiguration
			wantDescriptor, ok := wantMCPConfiguration.ToolPolicy.Descriptor(toolName)
			if !ok || wantDescriptor.Description != tool.Spec.Description || wantDescriptor.DefinitionDigest == "" {
				t.Fatalf("frozen custom Tool descriptor = %#v", wantDescriptor)
			}
			attemptKey := store.PromptAttemptKey{
				Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID,
			}
			attemptID, err := attemptKey.CanonicalID()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
				ID: attemptID, Key: attemptKey, RequestDigest: task.Status.Execution.RequestDigest,
				BindingDigest:  task.Status.AgentExecutionBinding.BindingDigest,
				SnapshotDigest: task.Status.AgentExecutionBinding.Snapshot.Digest,
				ExecutionState: store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
			}), fence); err != nil {
				t.Fatal(err)
			}
			if err := test.change(ctx, kubeClient, agent, tool); err != nil {
				t.Fatal(err)
			}

			dispatcher := &ACPDispatcher{
				Client: kubeClient, APIReader: kubeClient, Store: controlStore, ResultStore: controlStore,
				Snapshots: controlStore, Epochs: epochs,
			}
			dispatchQueuedTask(ctx, t, dispatcher, task.DeepCopy())
			var got harnessv2.CreateRuntimeSessionRequest
			select {
			case got = <-createRequests:
			default:
				t.Fatal("runtime did not receive CreateRuntimeSession")
			}
			if got.AgentConfiguration == nil || *got.AgentConfiguration != wantAgentConfiguration {
				t.Fatalf("runtime Agent configuration = %#v, want frozen %#v", got.AgentConfiguration, wantAgentConfiguration)
			}
			if !got.MCPConfiguration.Matches(wantMCPConfiguration) {
				t.Fatalf("runtime MCP configuration drifted from frozen snapshot: got=%#v want=%#v", got.MCPConfiguration, wantMCPConfiguration)
			}
			gotDescriptor, ok := got.MCPConfiguration.ToolPolicy.Descriptor(toolName)
			if !ok || gotDescriptor.Description != wantDescriptor.Description ||
				gotDescriptor.DefinitionDigest != wantDescriptor.DefinitionDigest {
				t.Fatalf("runtime custom Tool descriptor = %#v, want frozen %#v", gotDescriptor, wantDescriptor)
			}
		})
	}
}

func TestACPDispatcherWriteSessionFinalizesPublicationBeforeDeleteAndPersistsCleanupReceipt(t *testing.T) {
	testACPDispatcherWriteSessionFinalization(t, false)
}

func TestACPDispatcherWriteSessionSurvivesCreateConflictRequeue(t *testing.T) {
	testACPDispatcherWriteSessionFinalization(t, true)
}

//nolint:goconst,gocyclo // The end-to-end write-session lifecycle assertions intentionally stay together.
func testACPDispatcherWriteSessionFinalization(t *testing.T, requeueAfterCreateConflict bool) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	taskUID := types.UID("12121212-1212-1212-1212-121212121212")
	promptID := "prompt-" + string(taskUID) + "-1"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "write-session", UID: taskUID, Labels: map[string]string{acpRuntimeTaskPoolLabel: "pool"}},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "update the workspace", AgentRuntime: &corev1alpha1.AgentRuntimeSpec{},
			AgentRef:   &corev1alpha1.AgentReference{Name: "agent"},
			SessionRef: &corev1alpha1.SessionReference{Name: "write-session", Create: true, Append: true},
			Workspace: &corev1alpha1.WorkspaceConfig{
				Intent: corev1alpha1.WorkspaceIntentWrite, GitRepo: "https://github.com/orka-agents/orka.git", Ref: defaultACPSourceBranch,
				ReadCredentialRef:            &corev1alpha1.WorkspaceCredentialReference{Name: "workspace-credential"},
				PublicationGitRepo:           "https://github.com/orka-agents/orka.git",
				PublicationReadCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "workspace-credential"},
				PublicationCredentialRef:     &corev1alpha1.WorkspaceCredentialReference{Name: "workspace-credential"},
				ForgeCredentialRef:           &corev1alpha1.WorkspaceCredentialReference{Name: "workspace-credential"},
				CreatePR:                     true,
				PRBaseBranch:                 "main",
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending, Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateQueued, Attempt: 1, PromptID: promptID, RuntimePoolName: "pool", RuntimePoolUID: "pool-uid",
			RequestDigest: testControlDigestForDispatcher("write-session-request"), ControllerEpoch: 1,
		}},
	}
	spanHarness, parentSpanID := stampACPTaskTrace(t, task)
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "agent", UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	images := ACPRuntimeImages{Codex: "docker.io/example/acp@sha256:" + strings.Repeat("a", 64)}
	plan := frozenACPDispatcherPlanForTest(t, task, agent, images)
	profile := plan.Profile
	profileDigest := plan.Digest
	task.Labels[acpRuntimeTaskPoolLabel] = plan.PoolName
	task.Status.Execution.RuntimePoolName = plan.PoolName
	var operationMu sync.Mutex
	operations := make([]string, 0, 2)
	var finalizationRequest harnessv2.FinalizeRuntimeSessionPublicationRequest
	var creationPending atomic.Bool
	creationPending.Store(requeueAfterCreateConflict)
	runtimeServer := newDispatcherWriteRuntimeServer(
		t, profile, profileDigest, &creationPending,
		func(request harnessv2.FinalizeRuntimeSessionPublicationRequest) {
			operationMu.Lock()
			defer operationMu.Unlock()
			operations = append(operations, "finalize")
			finalizationRequest = request
		},
		func(harnessv2.DeleteRuntimeSessionRequest) {
			operationMu.Lock()
			defer operationMu.Unlock()
			operations = append(operations, "delete")
		},
	)
	defer runtimeServer.Close()
	parsed, err := url.Parse(runtimeServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: plan.PoolName, UID: types.UID("pool-uid"), Generation: 1},
		Spec: corev1alpha1.RuntimePoolSpec{
			RuntimeNamespace: "orka-runtimes", Runtime: corev1alpha1.RuntimePoolRuntimeSpec{
				Image: plan.Image, Profile: RuntimePoolProfileFromPlan(plan),
			}, DesiredReplicas: 1,
		},
		Status: corev1alpha1.RuntimePoolStatus{
			Lifecycle: corev1alpha1.RuntimePoolLifecycleServing, AdmissionState: corev1alpha1.RuntimePoolAdmissionAccepting,
			ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{
				PodNamespace: "orka-runtimes", PodName: "runtime-pod", PodAddress: parsed.Host,
				PodUID: "pod-uid", BootID: "boot-id", RuntimeInstanceID: "pod-uid.boot-id", ControllerEpoch: 1,
				ProtocolVersion: corev1alpha1.RuntimePoolProtocolHarnessV2, ProfileDigest: string(profileDigest), ProfileDigestSchemaVersion: strconv.FormatUint(uint64(harnessv2.ProfileDigestSchemaVersion), 10),
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "orka-runtimes", Name: "pool-auth-e1", Labels: map[string]string{
			runtimePoolAuthLabel: "true", runtimePoolUIDLabel: string(pool.UID),
		}},
		Data: map[string][]byte{runtimePoolControllerTokenKey: []byte(strings.Repeat("t", 32)), runtimePoolCapabilitySecretKey: []byte(strings.Repeat("s", 32))},
	}
	workspaceCredential := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "workspace-credential", UID: types.UID("workspace-credential-uid"), ResourceVersion: "1",
		},
		Data: map[string][]byte{defaultACPWorkspaceCredentialKey: []byte("test-token")},
	}
	task.Status.Execution.ReadCredentialResourceVersion = workspaceCredential.ResourceVersion
	task.Status.Execution.PublicationReadCredentialResourceVersion = workspaceCredential.ResourceVersion
	task.Status.Execution.PublicationCredentialResourceVersion = workspaceCredential.ResourceVersion
	task.Status.Execution.ForgeCredentialResourceVersion = workspaceCredential.ResourceVersion
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).WithObjects(task, pool, secret, workspaceCredential, agent).Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "write-session.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "controller-write-session")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task = prepareBoundACPDispatcherTaskForTest(t, ctx, kubeClient, scheme, controlStore, task, agent, images)
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attemptID, err := key.CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	credentialBindings := make([]store.PromptCredentialBinding, 0, 4)
	for _, role := range []store.PromptCredentialRole{
		store.PromptCredentialSourceRead, store.PromptCredentialTargetRead,
		store.PromptCredentialTargetWrite, store.PromptCredentialForge,
	} {
		credentialBindings = append(credentialBindings, store.PromptCredentialBinding{
			Role: role, Namespace: task.Namespace, SecretName: workspaceCredential.Name,
			SecretKey: defaultACPWorkspaceCredentialKey, SecretUID: string(workspaceCredential.UID),
			ResourceVersion: workspaceCredential.ResourceVersion,
		})
	}
	if _, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		ID: attemptID, Key: key, RequestDigest: task.Status.Execution.RequestDigest,
		BindingDigest: task.Status.AgentExecutionBinding.BindingDigest, SnapshotDigest: task.Status.AgentExecutionBinding.Snapshot.Digest,
		CredentialBindings: credentialBindings,
		ExecutionState:     store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
	}), fence); err != nil {
		t.Fatal(err)
	}
	recordingControls := &orderedSessionControlStore{
		SessionControlStore: controlStore,
		onFinalized: func() {
			operationMu.Lock()
			defer operationMu.Unlock()
			operations = append(operations, "settle")
		},
	}
	continuity, err := NewACPSessionContinuity(ACPSessionContinuityConfig{
		SessionControls: recordingControls, Transcripts: controlStore, Publications: controlStore, BranchClaims: controlStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	baselineOID := strings.Repeat("1", 40)
	treeOID := strings.Repeat("2", 40)
	commitOID := strings.Repeat("3", 40)
	bundleDigest := "sha256:" + strings.Repeat("4", 64)
	publisherServer := newDispatcherPublisherServer(t, treeOID, commitOID, bundleDigest)
	defer publisherServer.Close()
	publisherClient, err := publisherservice.NewClient(publisherservice.ClientConfig{
		BaseURL: publisherServer.URL, BearerToken: []byte(strings.Repeat("b", 32)),
		CapabilitySecret: []byte(strings.Repeat("c", 32)), HTTPClient: publisherServer.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &ACPDispatcher{
		Client: kubeClient, APIReader: kubeClient, Store: controlStore, ResultStore: controlStore, Epochs: epochs,
		Snapshots: controlStore,
		Sessions:  continuity, Publisher: publisherClient, ArtifactCapabilitySecret: []byte(strings.Repeat("d", 32)),
		ArtifactReservations: acceptingArtifactReservations{},
	}
	var preservedGeneration int64
	if requeueAfterCreateConflict {
		reserved, target, err := dispatcher.reserveTask(ctx, task.DeepCopy())
		if err != nil || reserved == nil {
			t.Fatalf("reserve write task: task=%v err=%v", reserved, err)
		}
		if err := dispatcher.executeReservedTask(ctx, reserved, target); err != nil {
			t.Fatalf("first dispatch: %v", err)
		}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
			t.Fatal(err)
		}
		if task.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved ||
			!task.Status.Execution.RuntimeSessionRecreationPending || task.Status.Execution.RuntimeSessionUID == "" ||
			task.Status.Execution.RuntimeSessionGeneration == 0 || task.Status.Execution.RuntimeSessionCleanupDigest != "" {
			t.Fatalf("write session binding was not preserved for retry: %#v", task.Status.Execution)
		}
		preservedGeneration = task.Status.Execution.RuntimeSessionGeneration
		operationMu.Lock()
		beforeRetry := append([]string(nil), operations...)
		operationMu.Unlock()
		if len(beforeRetry) != 0 {
			t.Fatalf("write session was finalized or deleted before retry: %v", beforeRetry)
		}
		creationPending.Store(false)
	}
	dispatchQueuedTask(ctx, t, dispatcher, task.DeepCopy())
	completed := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, completed); err != nil {
		t.Fatal(err)
	}
	if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded || completed.Status.Execution == nil ||
		completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded || completed.Status.Delivery == nil ||
		completed.Status.Delivery.Outcome != corev1alpha1.TaskDeliveryOutcomeVerifiedExact || completed.Status.Delivery.StartingSHA != baselineOID {
		t.Fatalf("unexpected completed write status: status=%#v execution=%#v delivery=%#v", completed.Status, completed.Status.Execution, completed.Status.Delivery)
	}
	if requeueAfterCreateConflict && completed.Status.Execution.RuntimeSessionGeneration != preservedGeneration {
		t.Fatalf("completed generation = %d, want preserved generation %d", completed.Status.Execution.RuntimeSessionGeneration, preservedGeneration)
	}
	publication, err := controlStore.GetPublication(ctx, publicationIDForTask(completed))
	if err != nil {
		t.Fatal(err)
	}
	if publication.SourceRef != baselineOID {
		t.Fatalf("publication source ref = %q, want immutable baseline %q", publication.SourceRef, baselineOID)
	}
	wantCleanupDigest, err := taskScopedRuntimeSessionCleanupDigest(
		completed.UID, completed.Status.Execution.Attempt, completed.Status.Execution.RuntimeInstanceID,
		completed.Status.Execution.RuntimeSessionUID, completed.Status.Execution.RuntimeSessionGeneration,
	)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status.Execution.RuntimeSessionCleanupDigest != wantCleanupDigest {
		t.Fatalf("cleanup receipt = %q, want %q", completed.Status.Execution.RuntimeSessionCleanupDigest, wantCleanupDigest)
	}
	operationMu.Lock()
	gotOperations := append([]string(nil), operations...)
	gotFinalization := finalizationRequest
	operationMu.Unlock()
	if fmt.Sprint(gotOperations) != "[finalize settle delete]" {
		t.Fatalf("runtime cleanup operations = %v, want publication finalization, Session settlement, then deletion", gotOperations)
	}
	if gotFinalization.WorkspaceDeltaID != harnessv2.WorkspaceDeltaID("delta-"+promptID) ||
		gotFinalization.PublicationID != publicationIDForTask(completed) ||
		gotFinalization.TerminalState != harnessv2.PublicationTerminalVerifiedExact || gotFinalization.TerminalReceiptDigest == "" {
		t.Fatalf("publication finalization request = %#v", gotFinalization)
	}
	spans := spanHarness.Recorder.Ended()
	promptSpan := tracingtest.SpanNamed(spans, acpPromptSpanName)
	publicationSpan := tracingtest.SpanNamed(spans, acpPublicationSpanName)
	if promptSpan == nil || publicationSpan == nil {
		t.Fatalf("ACP spans missing: prompt=%v publication=%v", promptSpan != nil, publicationSpan != nil)
	}
	if got := promptSpan.Parent().SpanID().String(); got != parentSpanID {
		t.Fatalf("acp.prompt parent = %s, want Task trace parent %s", got, parentSpanID)
	}
	if got := publicationSpan.Parent().SpanID(); got != promptSpan.SpanContext().SpanID() {
		t.Fatalf("acp.publication.reconcile parent = %s, want acp.prompt %s", got, promptSpan.SpanContext().SpanID())
	}
	publicationAttrs := tracingtest.AttributeMap(publicationSpan)
	if got := publicationAttrs[acpAttrPublicationID].AsString(); got != publicationIDForTask(completed) {
		t.Fatalf("publication span id = %q, want %q", got, publicationIDForTask(completed))
	}
	if publicationAttrs[acpAttrPublicationRecovery].AsBool() {
		t.Fatal("live publication span was marked as recovery")
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestACPDispatcherDeletesTaskScopedRuntimeSessionAfterTimeoutCancellation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	taskUID := types.UID("66666666-6666-6666-6666-666666666666")
	promptID := "prompt-" + string(taskUID) + "-1"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "timeout-task", UID: taskUID, Labels: map[string]string{acpRuntimeTaskPoolLabel: "pool"}},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "block until timeout", AgentRuntime: &corev1alpha1.AgentRuntimeSpec{},
			AgentRef: &corev1alpha1.AgentReference{Name: "agent"}, Timeout: &metav1.Duration{Duration: 30 * time.Second},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending, Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateQueued, Attempt: 1, PromptID: promptID, RuntimePoolName: "pool", RuntimePoolUID: "pool-uid",
			RequestDigest: testControlDigestForDispatcher("timeout-task-request"), ControllerEpoch: 1,
		}},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "agent", UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	images := ACPRuntimeImages{Codex: "docker.io/example/acp@sha256:" + strings.Repeat("a", 64)}
	plan := frozenACPDispatcherPlanForTest(t, task, agent, images)
	profile := plan.Profile
	profileDigest := plan.Digest
	task.Labels[acpRuntimeTaskPoolLabel] = plan.PoolName
	task.Status.Execution.RuntimePoolName = plan.PoolName
	var deleteCalls atomic.Int32
	deadlineCancels := make(chan context.CancelCauseFunc, 1)
	accepted := make(chan struct{})
	server := newDispatcherTimeoutRuntimeServer(t, profile, profileDigest, &deleteCalls, func() {
		close(accepted)
	})
	defer server.Close()
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: plan.PoolName, UID: types.UID("pool-uid"), Generation: 1},
		Spec: corev1alpha1.RuntimePoolSpec{
			RuntimeNamespace: "orka-runtimes", Runtime: corev1alpha1.RuntimePoolRuntimeSpec{
				Image: plan.Image, Profile: RuntimePoolProfileFromPlan(plan),
			}, DesiredReplicas: 1,
		},
		Status: corev1alpha1.RuntimePoolStatus{
			Lifecycle: corev1alpha1.RuntimePoolLifecycleServing, AdmissionState: corev1alpha1.RuntimePoolAdmissionAccepting,
			ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{
				PodNamespace: "orka-runtimes", PodName: "runtime-pod", PodAddress: parsed.Host,
				PodUID: "pod-uid", BootID: "boot-id", RuntimeInstanceID: "pod-uid.boot-id", ControllerEpoch: 1,
				ProtocolVersion: corev1alpha1.RuntimePoolProtocolHarnessV2, ProfileDigest: string(profileDigest), ProfileDigestSchemaVersion: strconv.FormatUint(uint64(harnessv2.ProfileDigestSchemaVersion), 10),
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "orka-runtimes", Name: "pool-auth-e1", Labels: map[string]string{
			runtimePoolAuthLabel: "true", runtimePoolUIDLabel: string(pool.UID),
		}},
		Data: map[string][]byte{runtimePoolControllerTokenKey: []byte(strings.Repeat("t", 32)), runtimePoolCapabilitySecretKey: []byte(strings.Repeat("s", 32))},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).WithObjects(task, pool, secret, agent).Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "timeout-store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "controller-timeout-test")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task = prepareBoundACPDispatcherTaskForTest(t, ctx, kubeClient, scheme, controlStore, task, agent, images)
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attemptID, err := key.CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		ID: attemptID, Key: key, RequestDigest: task.Status.Execution.RequestDigest,
		BindingDigest: task.Status.AgentExecutionBinding.BindingDigest, SnapshotDigest: task.Status.AgentExecutionBinding.Snapshot.Digest,
		ExecutionState: store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
	}), fence); err != nil {
		t.Fatal(err)
	}
	dispatcher := &ACPDispatcher{
		Client: kubeClient, APIReader: kubeClient, Store: controlStore, ResultStore: controlStore, Epochs: epochs,
		Snapshots: controlStore,
		runtimeContextFactory: func(parent context.Context, _ *corev1alpha1.Task) (context.Context, context.CancelFunc) {
			runtimeCtx, cancelCause := context.WithCancelCause(parent)
			deadlineCancels <- cancelCause
			return runtimeCtx, func() { cancelCause(context.Canceled) }
		},
	}
	cancelAfterAcceptance := cancelRuntimeContextAfterPromptRunning(
		ctx, controlStore, attemptID, accepted, deadlineCancels,
	)
	dispatchQueuedTask(ctx, t, dispatcher, task.DeepCopy())
	if err := <-cancelAfterAcceptance; err != nil {
		t.Fatalf("cancel after prompt acceptance: %v", err)
	}
	completed := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, completed); err != nil {
		t.Fatal(err)
	}
	if completed.Status.Phase != corev1alpha1.TaskPhaseCancelled || completed.Status.Execution == nil ||
		completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeCancelled ||
		completed.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("TaskTimeout") {
		t.Fatalf("unexpected timeout status: %#v", completed.Status)
	}
	timeline, err := controlStore.ListExecutionEvents(ctx, store.ExecutionEventFilter{
		Namespace: task.Namespace, StreamType: store.ExecutionEventStreamTypeTask, StreamID: task.Name,
		Limit: store.MaxExecutionEventLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	trace := tasktrace.BuildTaskTrace(tasktrace.MetadataFromTask(completed), timeline, time.Now().UTC())
	if len(trace.ModelRequests) != 1 || trace.ModelRequests[0].Status != tasktrace.StatusFailed ||
		trace.ModelRequests[0].StartSeq == 0 || trace.ModelRequests[0].EndSeq <= trace.ModelRequests[0].StartSeq {
		t.Fatalf("timeout ACP trace model requests = %#v; timeline=%#v", trace.ModelRequests, timeline)
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("RuntimeSession DELETE calls = %d, want 1", got)
	}
	attempt, err := controlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionCancelled {
		t.Fatalf("attempt execution state = %s, want %s", attempt.ExecutionState, store.PromptExecutionCancelled)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestACPDispatcherOpensSessionTurnBeforePrePromptFailureAndReleasesLease(t *testing.T) {
	ctx := context.Background()
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "pre-prompt.db"))
	defer closeStore()
	continuity := newACPSessionTestContinuity(t, controlStore, ACPBootstrapLimits{})
	taskUID := types.UID("55555555-5555-5555-5555-555555555555")
	promptID := "prompt-" + string(taskUID) + "-1"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pre-prompt", UID: taskUID},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "fail before runtime creation",
			SessionRef: &corev1alpha1.SessionReference{Name: "pre-prompt", Create: true, Append: true},
		},
		Status: corev1alpha1.TaskStatus{Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateReserved, Attempt: 1, PromptID: promptID,
			RequestDigest: testControlDigestForDispatcher("pre-prompt"), ControllerEpoch: fence.Epoch,
		}},
	}
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{Key: key, RequestDigest: task.Status.Execution.RequestDigest}), fence)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
		NewState: store.PromptExecutionReserved, OperationID: "reserve-pre-prompt",
		OperationDigest: testControlDigestForDispatcher("reserve-pre-prompt"), UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &ACPDispatcher{Store: controlStore, Sessions: continuity, runtimeSessions: make(map[string]ACPRuntimeSessionBinding)}
	session, err := dispatcher.prepareTaskSession(
		ctx, task, fence, harnessv2.ProfileDigest(testControlDigestForDispatcher("profile-pre-prompt")),
		testControlDigestForDispatcher("mcp-pre-prompt"),
		harnessv2.RuntimeInstanceID("runtime-instance"), harnessv2.SupervisorBootID("boot-id"),
		acpSessionLineageIdentity{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if session == nil || session.Turn == nil || session.Turn.Turn.State != store.SessionTurnOpen {
		t.Fatalf("SessionTurn was not opened immediately after lease acquisition: %#v", session)
	}
	attempt, err = controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionSessionStarting || attempt.SessionUID == "" || attempt.SessionLeaseGeneration < 1 {
		t.Fatalf("PromptAttempt was not bound before opening SessionTurn: %#v", attempt)
	}
	attempt, err = controlStore.RecoverPromptAttemptPreSubmission(ctx, store.PromptAttemptPreSubmissionRecovery{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
		OperationID: "retry-pre-prompt", OperationDigest: testControlDigestForDispatcher("retry-pre-prompt"),
		RecoveredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	retried, err := dispatcher.prepareTaskSession(
		ctx, task, fence, harnessv2.ProfileDigest(testControlDigestForDispatcher("profile-pre-prompt")),
		testControlDigestForDispatcher("mcp-pre-prompt"),
		harnessv2.RuntimeInstanceID("runtime-instance"), harnessv2.SupervisorBootID("boot-id"),
		acpSessionLineageIdentity{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if retried == nil || retried.Turn == nil || retried.Turn.Turn.ID != session.Turn.Turn.ID {
		t.Fatalf("pre-submission retry did not reuse the open SessionTurn: first=%#v retry=%#v", session, retried)
	}
	session = retried
	if err := dispatcher.transitionAttemptToTerminal(ctx, attempt.ID, fence, store.PromptExecutionFailed, "pre-prompt-failed"); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.reconcileUnfinalizedTaskSession(ctx, task, fence, session, errors.New("workspace preparation failed")); err != nil {
		t.Fatal(err)
	}
	finalizedTurn, err := controlStore.GetSessionTurn(ctx, session.Turn.Turn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finalizedTurn.State != store.SessionTurnFinalized || finalizedTurn.TerminalKind != store.SessionTurnOutcomeMarker {
		t.Fatalf("pre-prompt failure did not finalize SessionTurn: %#v", finalizedTurn)
	}
	control, err := controlStore.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name)
	if err != nil {
		t.Fatal(err)
	}
	if control.Lease != nil {
		t.Fatalf("pre-prompt failure leaked Session mutation lease: %#v", control.Lease)
	}
}

//nolint:gocyclo // The explicit state-machine branches are easier to audit together.
func TestACPDispatcherPublishesPreparedWorkspaceDelta(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	taskUID := types.UID("22222222-2222-2222-2222-222222222222")
	promptID := "prompt-" + string(taskUID) + "-1"
	baselineOID := strings.Repeat("1", 40)
	sessionUID := "session-publication-test"
	targetRef := "refs/heads/orka/session-" + sessionUID
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "write-task", UID: taskUID, CreationTimestamp: metav1.NewTime(time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC))},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "update the workspace",
			Workspace: &corev1alpha1.WorkspaceConfig{
				Intent: corev1alpha1.WorkspaceIntentWrite, GitRepo: "https://github.com/orka-agents/orka.git", Branch: "main", Ref: baselineOID,
				SubPath:                      "services/app",
				PublicationGitRepo:           "https://github.com/sozercan/orka-fork.git",
				PublicationReadCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "github-read"},
				PublicationCredentialRef:     &corev1alpha1.WorkspaceCredentialReference{Name: "github-publish"},
				ForgeCredentialRef:           &corev1alpha1.WorkspaceCredentialReference{Name: "github-forge"},
				CreatePR:                     true, PRBaseBranch: "main",
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning, Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateSucceeded, Outcome: corev1alpha1.TaskExecutionOutcomeSucceeded,
			Attempt: 1, PromptID: promptID, RequestDigest: testControlDigestForDispatcher("write-task"), ControllerEpoch: 1,
		}},
	}

	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(task).Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "controller-test")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	claimID, err := store.CanonicalBranchClaimID("github.com/sozercan/orka-fork", targetRef)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlStore.CreateBranchClaim(ctx, &store.BranchClaim{
		ID: claimID, RepositoryID: "github.com/sozercan/orka-fork", Ref: targetRef,
		OwnerKind: store.BranchClaimOwnerSession, OwnerUID: sessionUID, Generation: 1,
		LastVerified: store.RemoteRefState{SHA: baselineOID}, Availability: store.BranchClaimAvailable,
		RequestDigest: testControlDigestForDispatcher("existing-session-branch"), CreatedAt: time.Now().UTC(),
	}, fence); err != nil {
		t.Fatal(err)
	}
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attemptID, err := key.CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		ID: attemptID, Key: key, RequestDigest: task.Status.Execution.RequestDigest,
		ExecutionState: store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
	}), fence)
	if err != nil {
		t.Fatal(err)
	}
	attempt = completeACPAttemptExecutionForTest(t, controlStore, fence, attempt, false)
	transitionDigest := testControlDigestForDispatcher("delivery-validating")
	_, err = controlStore.TransitionPromptAttemptDelivery(ctx, store.PromptAttemptDeliveryTransition{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: store.PromptDeliveryNotRequested,
		NewState: store.PromptDeliveryValidating, OperationID: "delivery-validating", OperationDigest: transitionDigest, UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}

	treeOID := strings.Repeat("2", 40)
	commitOID := strings.Repeat("3", 40)
	bundleDigest := "sha256:" + strings.Repeat("4", 64)
	deltaDigest := "sha256:" + strings.Repeat("5", 64)
	deltaID, err := artifactcap.ArtifactIDForDigest(deltaDigest)
	if err != nil {
		t.Fatal(err)
	}
	prIntents := make(chan publisher.PullRequestIntent, 1)
	prepareRoots := make(chan string, 1)
	publishRoots := make(chan string, 1)
	publisherServer := newDispatcherPublisherServer(t, treeOID, commitOID, bundleDigest, dispatcherPublisherServerOptions{
		inspectPullRequest: func(intent publisher.PullRequestIntent) { prIntents <- intent },
		inspectPrepare:     func(request publisherservice.PublicationPrepareRequest) { prepareRoots <- request.Request.RelativeRoot },
		inspectPublish: func(request publisherservice.PublicationPublishRequest) {
			publishRoots <- request.Prepared.RelativeRoot
		},
	})
	defer publisherServer.Close()
	publisherClient, err := publisherservice.NewClient(publisherservice.ClientConfig{
		BaseURL: publisherServer.URL, BearerToken: []byte(strings.Repeat("b", 32)),
		CapabilitySecret: []byte(strings.Repeat("c", 32)), HTTPClient: publisherServer.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ambiguousPublicationStore := &persistPublicationThenErrorStore{DurableControlStore: controlStore}
	dispatcher := &ACPDispatcher{Client: kubeClient, APIReader: kubeClient, Store: ambiguousPublicationStore, ResultStore: controlStore, Epochs: epochs, Publisher: publisherClient}
	delta := harnessv2.WorkspaceDeltaDescriptor{
		DeltaID: "delta-1", RuntimeSessionUID: "runtime-session-1", SessionGeneration: 1,
		State: harnessv2.WorkspaceDeltaPrepared, Intent: harnessv2.WorkspaceIntentWrite,
		VerifiedBaseline: harnessv2.WorkspaceBaseline{RepositoryIdentity: "github.com/sozercan/orka-fork", Revision: baselineOID, TreeDigest: "sha256:" + strings.Repeat("6", 64)},
		RelativeRoot:     "services/app",
		ManifestDigest:   "sha256:" + strings.Repeat("7", 64),
		Artifact:         &harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(deltaID), Digest: deltaDigest, SizeBytes: 128, MediaType: artifactcap.MediaTypeWorkspaceDelta},
		EntryCount:       1, ChangedFileCount: 1, NoFollowVerified: true, PublicationSafe: true, FrozenAt: time.Now().UTC(),
	}
	session := &acpTaskSession{
		Binding: ACPRuntimeSessionBinding{SessionUID: sessionUID},
		VerifiedBaseline: &store.VerifiedBranchBaseline{
			RepositoryID: "github.com/sozercan/orka-fork", Ref: targetRef, SHA: baselineOID,
		},
	}
	result, err := dispatcher.publishWorkspaceDelta(ctx, task, attemptID, fence, delta.VerifiedBaseline, delta, session)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status.Outcome != corev1alpha1.TaskDeliveryOutcomeVerifiedExact || result.Status.ExpectedCommitSHA != commitOID || result.Status.VerifiedRemoteSHA != commitOID ||
		result.Status.PRReceipt == nil || result.Status.PRReceipt.ID != "42" || result.Status.PRReceipt.HeadSHA != commitOID {
		t.Fatalf("publication result = %#v", result)
	}
	select {
	case root := <-prepareRoots:
		if root != "services/app" {
			t.Fatalf("publisher prepare relative root = %q, want services/app", root)
		}
	default:
		t.Fatal("publisher did not receive the workspace relative root")
	}
	select {
	case root := <-publishRoots:
		if root != "services/app" {
			t.Fatalf("publisher publish relative root = %q, want services/app", root)
		}
	default:
		t.Fatal("publisher publish did not receive the prepared workspace relative root")
	}
	select {
	case intent := <-prIntents:
		if intent.BaseRepository.ID != "github.com/orka-agents/orka" || intent.HeadRepository.ID != "github.com/sozercan/orka-fork" {
			t.Fatalf("continuation PR repositories = base %#v head %#v", intent.BaseRepository, intent.HeadRepository)
		}
	default:
		t.Fatal("publisher did not receive a pull request intent")
	}
	publicationRecord, err := controlStore.GetPublication(ctx, result.PublicationID)
	if err != nil {
		t.Fatal(err)
	}
	if publicationRecord.State != store.PublicationVerifiedExact || publicationRecord.PreparedReceipt == nil || publicationRecord.PublishReceipt == nil || publicationRecord.VerificationReceipt == nil ||
		publicationRecord.PullRequestReceipt == nil || publicationRecord.PullRequestReceipt.ForgeID != "42" ||
		publicationRecord.SourceRef != baselineOID || publicationRecord.TargetRef != targetRef ||
		publicationRecord.Baseline.Absent || publicationRecord.Baseline.SHA != baselineOID {
		t.Fatalf("publication = %#v", publicationRecord)
	}
	attempt, err = controlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.DeliveryState != store.PromptDeliveryVerifiedExact {
		t.Fatalf("delivery state = %s", attempt.DeliveryState)
	}
	claim, err := controlStore.GetBranchClaim(ctx, publicationRecord.BranchClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if claim.LastVerified.SHA != baselineOID || claim.LastVerified.Absent {
		t.Fatalf("branch claim = %#v", claim)
	}
	publishEffectID, err := (store.ExternalEffectIdentity{
		Kind: "publisher.publish", Namespace: task.Namespace, AggregateID: result.PublicationID,
		OperationID: publicationOperationID("publish", task),
	}).CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	publishEffect, err := controlStore.GetExternalEffect(ctx, publishEffectID)
	if err != nil {
		t.Fatal(err)
	}
	if publishEffect.State != store.ExternalEffectSucceeded || len(publishEffect.Response) == 0 {
		t.Fatalf("publish effect = %#v", publishEffect)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

type acceptingArtifactReservations struct{}

func (acceptingArtifactReservations) Reserve(context.Context, artifactcap.OperationRequest, time.Time) error {
	return nil
}

type dispatcherPublisherServerOptions struct {
	inspectPrepare     func(publisherservice.PublicationPrepareRequest)
	inspectPublish     func(publisherservice.PublicationPublishRequest)
	inspectPullRequest func(publisher.PullRequestIntent)
}

func newDispatcherPublisherServer(t *testing.T, treeOID, commitOID, bundleDigest string, options ...dispatcherPublisherServerOptions) *httptest.Server {
	t.Helper()
	bundleArtifactID, err := artifactcap.ArtifactIDForDigest(bundleDigest)
	if err != nil {
		t.Fatal(err)
	}
	workspaceDigest := "sha256:" + strings.Repeat("9", 64)
	workspaceArtifactID, err := artifactcap.ArtifactIDForDigest(workspaceDigest)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case publisherservice.WorkspaceResolvePath:
			var request publisherservice.WorkspaceResolveRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode workspace resolve: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			resolvedRef := request.SourceRef
			if resolvedRef == defaultACPSourceBranch {
				resolvedRef = "refs/heads/" + defaultACPSourceBranch
			}
			writeDispatcherJSON(w, publisherservice.WorkspaceResolveResponse{
				OperationID: request.Metadata.OperationID, RequestDigest: testControlDigestForDispatcher("workspace-resolve"),
				RepositoryID: request.Source.ID, SourceRef: resolvedRef, BaselineOID: strings.Repeat("1", 40),
			})
		case publisherservice.WorkspacePreparePath:
			var request publisherservice.WorkspacePrepareRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode workspace prepare: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if request.SourceRef == defaultACPSourceBranch {
				t.Error("workspace prepare reused unresolved bare source ref")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			writeDispatcherJSON(w, publisherservice.WorkspacePrepareResponse{
				OperationID: request.Metadata.OperationID, RequestDigest: testControlDigestForDispatcher("workspace-prepare"),
				RepositoryID: request.Source.ID, SourceRef: request.SourceRef, BaselineOID: request.BaselineOID,
				TreeOID: strings.Repeat("a", 40), ManifestDigest: "sha256:" + strings.Repeat("8", 64),
				EntryCount: 1, ExpandedBytes: 128,
				Artifact: harnessv2.ArtifactReference{
					ArtifactID: harnessv2.ArtifactID(workspaceArtifactID), Digest: workspaceDigest,
					SizeBytes: 128, MediaType: artifactcap.MediaTypeWorkspaceTar,
				},
			})
		case publisherservice.PublicationPreflightPath:
			var request publisherservice.PublicationPreflightRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode preflight: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			writeDispatcherJSON(w, publisherservice.PublicationPreflightResponse{
				OperationID: request.Metadata.OperationID, RequestDigest: testControlDigestForDispatcher("preflight"),
				Result: publisher.PreflightResult{Expected: request.Request.Claim.LastVerified, Observed: request.Request.Claim.LastVerified, Matches: true},
			})
		case publisherservice.PublicationPreparePath:
			var request publisherservice.PublicationPrepareRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode prepare: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if len(options) > 0 && options[0].inspectPrepare != nil {
				options[0].inspectPrepare(request)
			}
			writeDispatcherJSON(w, publisherservice.PublicationPrepareResponse{
				OperationID: request.Metadata.OperationID, RequestDigest: testControlDigestForDispatcher("prepare"),
				Prepared: publisherservice.PreparedPublication{
					PublicationID: request.Request.PublicationID, PublicationGeneration: request.Request.PublicationGeneration,
					OperationID: request.Request.OperationID, RequestDigest: testControlDigestForDispatcher("prepared"),
					Source: request.Request.Source, SourceRef: request.Request.SourceRef, Target: request.Request.Target, TargetRef: request.Request.TargetRef,
					BranchClaimGeneration: request.Request.BranchClaimGeneration, BaselineOID: request.Request.BaselineOID,
					RemoteBefore: request.Request.RemoteBefore, DeltaArtifactDigest: request.Request.DeltaArtifactDigest,
					RelativeRoot:   request.Request.RelativeRoot,
					ManifestDigest: "sha256:" + strings.Repeat("8", 64), TreeOID: treeOID, CommitOID: commitOID,
					BundleDigest: bundleDigest, BundleSize: 256, BundleRef: "refs/orka/publications/" + strings.Repeat("f", 64),
					BundleArtifact: harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(bundleArtifactID), Digest: bundleDigest, SizeBytes: 256, MediaType: artifactcap.MediaTypeGitBundle},
					CommitMessage:  request.Request.CommitMessage, CommitTimestamp: request.Request.CommitTimestamp,
				},
			})
		case publisherservice.PublicationPublishPath:
			var request publisherservice.PublicationPublishRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode publish: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if len(options) > 0 && options[0].inspectPublish != nil {
				options[0].inspectPublish(request)
			}
			writeDispatcherJSON(w, publisherservice.PublicationPublishResponse{
				OperationID: request.Metadata.OperationID, RequestDigest: testControlDigestForDispatcher("publish-http"),
				Receipt: publisher.PublishReceipt{
					PublicationID: request.Request.PublicationID, PublicationGeneration: request.Request.PublicationGeneration,
					OperationID: request.Request.OperationID, RequestDigest: testControlDigestForDispatcher("publish"),
					Outcome: publisher.PublishAcknowledged, TargetRepositoryID: request.Request.Target.ID, TargetRef: request.Request.TargetRef,
					RemoteBefore: request.Request.RemoteBefore, ExpectedCommitOID: request.Request.ExpectedCommitOID,
					ObservedRemote: publisher.RemoteRef{OID: request.Request.ExpectedCommitOID},
				},
			})
		case publisherservice.PublicationVerifyPath:
			var request publisherservice.PublicationVerifyRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode verify: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			writeDispatcherJSON(w, publisherservice.PublicationVerifyResponse{
				OperationID: request.Metadata.OperationID, RequestDigest: testControlDigestForDispatcher("verify-http"),
				Receipt: publisher.VerificationReceipt{
					PublicationID: request.Request.PublicationID, PublicationGeneration: request.Request.PublicationGeneration,
					OperationID: request.Request.OperationID, RequestDigest: testControlDigestForDispatcher("verify"),
					Outcome: publisher.VerifiedExact, TargetRepositoryID: request.Request.Target.ID, TargetRef: request.Request.TargetRef,
					ExpectedCommitOID: request.Request.ExpectedCommitOID, ObservedRemote: publisher.RemoteRef{OID: request.Request.ExpectedCommitOID},
				},
			})
		case publisherservice.PullRequestReconcilePath:
			var request publisherservice.PullRequestReconcileRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode pull request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			intentKey, err := request.Intent.Key()
			if err != nil {
				t.Errorf("PR intent key: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if len(options) > 0 && options[0].inspectPullRequest != nil {
				options[0].inspectPullRequest(request.Intent)
			}
			writeDispatcherJSON(w, publisherservice.PullRequestReconcileResponse{
				OperationID: request.Metadata.OperationID, RequestDigest: testControlDigestForDispatcher("pr-http"),
				Receipt: publisher.PullRequestReceipt{IntentKey: intentKey, ForgeID: "42", URL: "https://github.com/orka-agents/orka/pull/42", State: publisher.PullRequestOpen, HeadOID: request.Intent.ExpectedHeadOID},
			})
		default:
			http.NotFound(w, r)
		}
	}))
}

func newDispatcherTimeoutRuntimeServer(
	t *testing.T,
	profile harnessv2.RuntimeProfile,
	digest harnessv2.ProfileDigest,
	deleteCalls *atomic.Int32,
	onAccepted func(),
) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	limits := harnessv2.DefaultProtocolLimits()
	var acceptedOnce sync.Once
	var descriptorMu sync.Mutex
	var descriptor harnessv2.RuntimeSessionDescriptor
	mux.HandleFunc("GET "+harnessv2.StatusPath, func(w http.ResponseWriter, _ *http.Request) {
		descriptorMu.Lock()
		current := descriptor
		descriptorMu.Unlock()
		writeDispatcherJSON(w, dispatcherRuntimeStatusResponse(digest, current))
	})
	mux.HandleFunc("GET "+harnessv2.CapabilitiesPath, func(w http.ResponseWriter, _ *http.Request) {
		writeDispatcherJSON(w, harnessv2.CapabilitiesResponse{
			Protocol: harnessv2.ProtocolVersion, Transport: "http+ndjson", ACPVersion: harnessv2.ACPProfileV1,
			RuntimeProfileDigest: digest, ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
			AdapterDigests: profile.AdapterDigests, Limits: limits,
			Provider:                          harnessv2.ProviderCapabilities{ProviderKinds: []string{profile.ProviderKind}, Models: []string{profile.Model}, SupportsCancel: true, SupportsPermissions: true, SupportsTools: true},
			WorkspaceGovernance:               harnessv2.StrictWorkspaceGovernanceCapabilities(),
			SupportsDrain:                     true,
			SupportsAgentSessionConfiguration: true,
		})
	})
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.CreateRuntimeSessionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode create: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		now := time.Now().UTC()
		descriptorMu.Lock()
		descriptor = harnessv2.RuntimeSessionDescriptor{
			RuntimeSessionID: request.RuntimeSessionID, RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID,
			Generation: request.Metadata.Fence.RuntimeSessionGeneration, RuntimeInstanceID: request.Metadata.Fence.RuntimeInstanceID,
			SupervisorBootID: request.Metadata.Fence.SupervisorBootID, RuntimeProfileDigest: request.Metadata.Fence.RuntimeProfileDigest,
			State: harnessv2.RuntimeSessionStateIdle, ProviderSessionID: "provider-session", WorkspaceBaseline: request.Workspace.Baseline,
			CreatedAt: now, LastTransitionAt: now,
		}
		created := descriptor
		descriptorMu.Unlock()
		writeDispatcherJSONStatus(w, http.StatusCreated, harnessv2.CreateRuntimeSessionResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			Session: created,
		})
	})
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/prompts/{promptID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.StartPromptRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode prompt: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", harnessv2.NDJSONMediaType)
		encoder, err := harnessv2.NewEventEncoder(w, harnessv2.EventStreamLimits{
			MaxLineBytes: limits.MaxEventLineBytes, MaxTerminalResultBytes: limits.MaxTerminalResultBytes,
			MaxBufferedEvents: limits.MaxBufferedEvents, MaxUpdateEventsPerSecond: limits.MaxUpdateEventsPerSecond,
		}, harnessv2.EventExpectationFromMetadata(request.Metadata))
		if err != nil {
			t.Errorf("event encoder: %v", err)
			return
		}
		now := time.Now().UTC()
		identity := harnessv2.EventIdentity{
			RuntimeInstanceID: request.Metadata.Fence.RuntimeInstanceID, SupervisorBootID: request.Metadata.Fence.SupervisorBootID,
			RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID, RuntimeSessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration,
			TaskUID: request.Metadata.TaskUID, TaskAttempt: request.Metadata.TaskAttempt, PromptID: request.Metadata.PromptID,
			Sequence: 1, RequestDigest: request.Metadata.RequestDigest, Timestamp: now,
		}
		if err := encoder.Encode(harnessv2.Event{
			Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventAccepted, Identity: identity,
			Accepted: &harnessv2.AcceptedEvent{AcceptedAt: now, Lease: request.Lease, ACPVersion: harnessv2.ACPProfileV1},
		}); err != nil {
			t.Errorf("encode accepted event: %v", err)
			return
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		if onAccepted != nil {
			acceptedOnce.Do(onAccepted)
		}
		<-r.Context().Done()
	})
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/prompts/{promptID}/cancel", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.CancelPromptRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode cancellation: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		now := time.Now().UTC()
		writeDispatcherJSON(w, harnessv2.CancelPromptResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			BarrierState: harnessv2.CancellationBarrierSettled, SettlementProven: true,
			Settlement: harnessv2.PromptSettlement{
				TerminalEvent: harnessv2.EventCancelled, Outcome: harnessv2.PromptOutcomeCancelled,
				StopReason: harnessv2.ACPStopReasonCancelled, SettledAt: now,
			},
		})
	})
	mux.HandleFunc("DELETE /v2/runtime-sessions/{sessionID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.DeleteRuntimeSessionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode delete: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		deleteCalls.Add(1)
		descriptorMu.Lock()
		descriptor = harnessv2.RuntimeSessionDescriptor{}
		descriptorMu.Unlock()
		writeDispatcherJSON(w, harnessv2.DeleteRuntimeSessionResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh}, State: harnessv2.RuntimeSessionStateDeleted,
			Tombstone: testDeleteTombstone(request, time.Now().UTC()),
		})
	})
	return httptest.NewServer(mux)
}

func newDispatcherWriteRuntimeServer(
	t *testing.T,
	profile harnessv2.RuntimeProfile,
	digest harnessv2.ProfileDigest,
	creationPending *atomic.Bool,
	onFinalize func(harnessv2.FinalizeRuntimeSessionPublicationRequest),
	onDelete func(harnessv2.DeleteRuntimeSessionRequest),
) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	limits := harnessv2.DefaultProtocolLimits()
	deltaDigest := "sha256:" + strings.Repeat("5", 64)
	deltaArtifactID, err := artifactcap.ArtifactIDForDigest(deltaDigest)
	if err != nil {
		t.Fatal(err)
	}
	var descriptorMu sync.Mutex
	var descriptor harnessv2.RuntimeSessionDescriptor
	mux.HandleFunc("GET "+harnessv2.StatusPath, func(w http.ResponseWriter, _ *http.Request) {
		descriptorMu.Lock()
		current := descriptor
		descriptorMu.Unlock()
		if current.RuntimeSessionID != "" && creationPending.Load() {
			current.State = harnessv2.RuntimeSessionStateCreating
			current.LastTransitionAt = time.Now().UTC().Add(-3 * time.Minute)
		}
		writeDispatcherJSON(w, dispatcherRuntimeStatusResponse(digest, current))
	})
	mux.HandleFunc("GET "+harnessv2.CapabilitiesPath, func(w http.ResponseWriter, _ *http.Request) {
		writeDispatcherJSON(w, harnessv2.CapabilitiesResponse{
			Protocol: harnessv2.ProtocolVersion, Transport: "http+ndjson", ACPVersion: harnessv2.ACPProfileV1,
			RuntimeProfileDigest: digest, ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
			AdapterDigests: profile.AdapterDigests, Limits: limits,
			Provider: harnessv2.ProviderCapabilities{
				ProviderKinds: []string{profile.ProviderKind}, Models: []string{profile.Model},
				SupportsCancel: true, SupportsPermissions: true, SupportsTools: true,
			},
			WorkspaceGovernance:               harnessv2.StrictWorkspaceGovernanceCapabilities(),
			SupportsDrain:                     true,
			SupportsPublicationFinalization:   true,
			SupportsAgentSessionConfiguration: true,
		})
	})
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.CreateRuntimeSessionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode create: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		now := time.Now().UTC()
		descriptorMu.Lock()
		descriptor = harnessv2.RuntimeSessionDescriptor{
			RuntimeSessionID: request.RuntimeSessionID, RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID,
			Generation: request.Metadata.Fence.RuntimeSessionGeneration, RuntimeInstanceID: request.Metadata.Fence.RuntimeInstanceID,
			SupervisorBootID: request.Metadata.Fence.SupervisorBootID, RuntimeProfileDigest: request.Metadata.Fence.RuntimeProfileDigest,
			State: harnessv2.RuntimeSessionStateIdle, ProviderSessionID: "provider-session", WorkspaceBaseline: request.Workspace.Baseline,
			CreatedAt: now, LastTransitionAt: now,
		}
		responseDescriptor := descriptor
		descriptorMu.Unlock()
		if creationPending.Load() {
			writeDispatcherJSONStatus(w, http.StatusConflict, digestConflictErrorResponse())
			return
		}
		writeDispatcherJSONStatus(w, http.StatusCreated, harnessv2.CreateRuntimeSessionResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			Session: responseDescriptor,
		})
	})
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/prompts/{promptID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.StartPromptRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode prompt: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", harnessv2.NDJSONMediaType)
		encoder, err := harnessv2.NewEventEncoder(w, harnessv2.EventStreamLimits{
			MaxLineBytes: limits.MaxEventLineBytes, MaxTerminalResultBytes: limits.MaxTerminalResultBytes,
			MaxBufferedEvents: limits.MaxBufferedEvents, MaxUpdateEventsPerSecond: limits.MaxUpdateEventsPerSecond,
		}, harnessv2.EventExpectationFromMetadata(request.Metadata))
		if err != nil {
			t.Errorf("event encoder: %v", err)
			return
		}
		now := time.Now().UTC()
		identity := func(sequence uint64, at time.Time) harnessv2.EventIdentity {
			return harnessv2.EventIdentity{
				RuntimeInstanceID: request.Metadata.Fence.RuntimeInstanceID, SupervisorBootID: request.Metadata.Fence.SupervisorBootID,
				RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID, RuntimeSessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration,
				TaskUID: request.Metadata.TaskUID, TaskAttempt: request.Metadata.TaskAttempt, PromptID: request.Metadata.PromptID,
				Sequence: sequence, RequestDigest: request.Metadata.RequestDigest, Timestamp: at,
			}
		}
		_ = encoder.Encode(harnessv2.Event{
			Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventAccepted, Identity: identity(1, now),
			Accepted: &harnessv2.AcceptedEvent{AcceptedAt: now, Lease: request.Lease, ACPVersion: harnessv2.ACPProfileV1},
		})
		_ = encoder.Encode(harnessv2.Event{
			Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventUpdate, Identity: identity(2, now.Add(time.Millisecond)),
			Update: &harnessv2.UpdateEvent{Kind: harnessv2.UpdateAssistantMessageChunk, AssistantMessage: &harnessv2.AssistantMessageChunk{Text: "workspace updated"}},
		})
		_ = encoder.Encode(harnessv2.Event{
			Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventCompleted, Identity: identity(3, now.Add(2*time.Millisecond)),
			Completed: &harnessv2.CompletedEvent{
				StopReason: harnessv2.ACPStopReasonEndTurn,
				Result:     harnessv2.PromptResult{Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: "workspace updated"}}, Model: profile.Model},
			},
		})
		if err := encoder.Close(); err != nil {
			t.Errorf("close encoder: %v", err)
		}
	})
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/workspace-deltas/{deltaID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.CreateWorkspaceDeltaRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode workspace delta: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		writeDispatcherJSON(w, harnessv2.CreateWorkspaceDeltaResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			Delta: harnessv2.WorkspaceDeltaDescriptor{
				DeltaID: request.DeltaID, RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID,
				SessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration, State: harnessv2.WorkspaceDeltaPrepared,
				Intent: request.Intent, VerifiedBaseline: request.VerifiedBaseline, ManifestDigest: "sha256:" + strings.Repeat("6", 64),
				Artifact: &harnessv2.ArtifactReference{
					ArtifactID: harnessv2.ArtifactID(deltaArtifactID), Digest: deltaDigest,
					SizeBytes: 128, MediaType: artifactcap.MediaTypeWorkspaceDelta,
				},
				EntryCount: 1, ChangedFileCount: 1, NoFollowVerified: true, PublicationSafe: true, FrozenAt: time.Now().UTC(),
			},
		})
	})
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/publication-finalization", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.FinalizeRuntimeSessionPublicationRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode publication finalization: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if onFinalize != nil {
			onFinalize(request)
		}
		now := time.Now().UTC()
		descriptorMu.Lock()
		responseDescriptor := descriptor
		responseDescriptor.State = harnessv2.RuntimeSessionStateFinalizing
		responseDescriptor.LastTransitionAt = now
		descriptor = responseDescriptor
		descriptorMu.Unlock()
		writeDispatcherJSON(w, harnessv2.FinalizeRuntimeSessionPublicationResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			Session: responseDescriptor,
			Finalization: harnessv2.PublicationFinalizationReceipt{
				WorkspaceDeltaID: request.WorkspaceDeltaID, PublicationID: request.PublicationID,
				PublicationGeneration: request.PublicationGeneration, PublicationVersion: request.PublicationVersion,
				TerminalState: request.TerminalState, TerminalReceiptDigest: request.TerminalReceiptDigest, AppliedAt: now,
			},
		})
	})
	mux.HandleFunc("DELETE /v2/runtime-sessions/{sessionID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.DeleteRuntimeSessionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode delete: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if onDelete != nil {
			onDelete(request)
		}
		descriptorMu.Lock()
		descriptor = harnessv2.RuntimeSessionDescriptor{}
		descriptorMu.Unlock()
		writeDispatcherJSON(w, harnessv2.DeleteRuntimeSessionResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			State:     harnessv2.RuntimeSessionStateDeleted,
			Tombstone: testDeleteTombstone(request, time.Now().UTC()),
		})
	})
	return httptest.NewServer(mux)
}

func newDispatcherRuntimeServer(
	t *testing.T,
	profile harnessv2.RuntimeProfile,
	digest harnessv2.ProfileDigest,
	onCreate ...func(harnessv2.CreateRuntimeSessionRequest),
) *httptest.Server {
	t.Helper()
	return newDispatcherRuntimeServerForPool(t, profile, digest, acpDispatcherTestPoolUID, onCreate...)
}

func newDispatcherRuntimeServerForPool(
	t *testing.T,
	profile harnessv2.RuntimeProfile,
	digest harnessv2.ProfileDigest,
	poolUID string,
	onCreate ...func(harnessv2.CreateRuntimeSessionRequest),
) *httptest.Server {
	t.Helper()
	return newDispatcherRuntimeServerForPoolWithOptions(
		t, profile, digest, poolUID, dispatcherRuntimeServerOptions{}, onCreate...,
	)
}

func newDispatcherRuntimeServerWithTerminalEvents(
	t *testing.T,
	profile harnessv2.RuntimeProfile,
	digest harnessv2.ProfileDigest,
	terminalEvents map[harnessv2.PromptID]harnessv2.EventType,
	onCreate ...func(harnessv2.CreateRuntimeSessionRequest),
) *httptest.Server {
	t.Helper()
	return newDispatcherRuntimeServerForPoolWithOptions(
		t, profile, digest, acpDispatcherTestPoolUID,
		dispatcherRuntimeServerOptions{terminalEvents: terminalEvents}, onCreate...,
	)
}

type dispatcherRuntimeServerOptions struct {
	terminalEvents map[harnessv2.PromptID]harnessv2.EventType
	// rejectCreate answers a create request with the returned error response
	// and status instead of creating the session when the response is non-nil.
	// resident makes the runtime keep reporting the exact requested session as
	// Idle, as a supervisor does after an earlier send of the same create
	// already created it.
	rejectCreate func(request harnessv2.CreateRuntimeSessionRequest) (status int, response *harnessv2.ErrorResponse, resident bool)
	onDelete     func(harnessv2.DeleteRuntimeSessionRequest)
}

func newDispatcherRuntimeServerWithOptions(
	t *testing.T,
	profile harnessv2.RuntimeProfile,
	digest harnessv2.ProfileDigest,
	options dispatcherRuntimeServerOptions,
	onCreate ...func(harnessv2.CreateRuntimeSessionRequest),
) *httptest.Server {
	t.Helper()
	return newDispatcherRuntimeServerForPoolWithOptions(
		t, profile, digest, acpDispatcherTestPoolUID, options, onCreate...,
	)
}

func newDispatcherRuntimeServerForPoolWithOptions(
	t *testing.T,
	profile harnessv2.RuntimeProfile,
	digest harnessv2.ProfileDigest,
	poolUID string,
	options dispatcherRuntimeServerOptions,
	onCreate ...func(harnessv2.CreateRuntimeSessionRequest),
) *httptest.Server {
	t.Helper()
	terminalEvents := options.terminalEvents
	mux := http.NewServeMux()
	limits := harnessv2.DefaultProtocolLimits()
	var descriptorMu sync.Mutex
	var descriptor harnessv2.RuntimeSessionDescriptor
	mux.HandleFunc("GET "+harnessv2.StatusPath, func(w http.ResponseWriter, _ *http.Request) {
		descriptorMu.Lock()
		current := descriptor
		descriptorMu.Unlock()
		writeDispatcherJSON(w, dispatcherRuntimeStatusResponseForPool(digest, poolUID, current))
	})
	mux.HandleFunc("GET "+harnessv2.CapabilitiesPath, func(w http.ResponseWriter, _ *http.Request) {
		writeDispatcherJSON(w, harnessv2.CapabilitiesResponse{
			Protocol: harnessv2.ProtocolVersion, Transport: "http+ndjson", ACPVersion: harnessv2.ACPProfileV1,
			RuntimeProfileDigest: digest, ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
			AdapterDigests: profile.AdapterDigests, Limits: limits,
			Provider:                          harnessv2.ProviderCapabilities{ProviderKinds: []string{profile.ProviderKind}, Models: []string{profile.Model}, SupportsCancel: true, SupportsPermissions: true, SupportsTools: true},
			WorkspaceGovernance:               harnessv2.StrictWorkspaceGovernanceCapabilities(),
			SupportsDrain:                     true,
			SupportsAgentSessionConfiguration: true,
		})
	})
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.CreateRuntimeSessionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode create: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for _, inspect := range onCreate {
			if inspect != nil {
				inspect(request)
			}
		}
		now := time.Now().UTC()
		created := harnessv2.RuntimeSessionDescriptor{
			RuntimeSessionID: request.RuntimeSessionID, RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID,
			Generation: request.Metadata.Fence.RuntimeSessionGeneration, RuntimeInstanceID: request.Metadata.Fence.RuntimeInstanceID,
			SupervisorBootID: request.Metadata.Fence.SupervisorBootID, RuntimeProfileDigest: request.Metadata.Fence.RuntimeProfileDigest,
			State: harnessv2.RuntimeSessionStateIdle, ProviderSessionID: "provider-session", WorkspaceBaseline: request.Workspace.Baseline,
			CreatedAt: now, LastTransitionAt: now,
		}
		if options.rejectCreate != nil {
			if status, response, resident := options.rejectCreate(request); response != nil {
				if resident {
					descriptorMu.Lock()
					descriptor = created
					descriptorMu.Unlock()
				}
				writeDispatcherJSONStatus(w, status, *response)
				return
			}
		}
		descriptorMu.Lock()
		descriptor = created
		descriptorMu.Unlock()
		writeDispatcherJSONStatus(w, http.StatusCreated, harnessv2.CreateRuntimeSessionResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			Session: created,
		})
	})
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/prompts/{promptID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.StartPromptRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode prompt: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", harnessv2.NDJSONMediaType)
		encoder, err := harnessv2.NewEventEncoder(w, harnessv2.EventStreamLimits{
			MaxLineBytes: limits.MaxEventLineBytes, MaxTerminalResultBytes: limits.MaxTerminalResultBytes,
			MaxBufferedEvents: limits.MaxBufferedEvents, MaxUpdateEventsPerSecond: limits.MaxUpdateEventsPerSecond,
		}, harnessv2.EventExpectationFromMetadata(request.Metadata))
		if err != nil {
			t.Errorf("event encoder: %v", err)
			return
		}
		now := time.Now().UTC()
		identity := func(seq uint64, at time.Time) harnessv2.EventIdentity {
			return harnessv2.EventIdentity{
				RuntimeInstanceID: request.Metadata.Fence.RuntimeInstanceID, SupervisorBootID: request.Metadata.Fence.SupervisorBootID,
				RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID, RuntimeSessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration,
				TaskUID: request.Metadata.TaskUID, TaskAttempt: request.Metadata.TaskAttempt, PromptID: request.Metadata.PromptID,
				Sequence: seq, RequestDigest: request.Metadata.RequestDigest, Timestamp: at,
			}
		}
		_ = encoder.Encode(harnessv2.Event{Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventAccepted, Identity: identity(1, now), Accepted: &harnessv2.AcceptedEvent{AcceptedAt: now, Lease: request.Lease, ACPVersion: harnessv2.ACPProfileV1}})
		_ = encoder.Encode(harnessv2.Event{Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventUpdate, Identity: identity(2, now.Add(time.Millisecond)), Update: &harnessv2.UpdateEvent{Kind: harnessv2.UpdateAssistantMessageChunk, AssistantMessage: &harnessv2.AssistantMessageChunk{Text: "hello "}}})
		_ = encoder.Encode(harnessv2.Event{Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventUpdate, Identity: identity(3, now.Add(2*time.Millisecond)), Update: &harnessv2.UpdateEvent{Kind: harnessv2.UpdateToolCall, ToolCall: &harnessv2.ToolCallUpdate{ToolCallID: acpDispatcherToolCallID, Title: acpDispatcherToolTitle, Kind: acpDispatcherToolKind, Status: harnessv2.ToolCallStatusPending}}})
		_ = encoder.Encode(harnessv2.Event{Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventUpdate, Identity: identity(4, now.Add(3*time.Millisecond)), Update: &harnessv2.UpdateEvent{Kind: harnessv2.UpdateToolCallUpdate, ToolCall: &harnessv2.ToolCallUpdate{ToolCallID: acpDispatcherToolCallID, Title: acpDispatcherToolTitle, Status: harnessv2.ToolCallStatusCompleted, Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: acpDispatcherToolResultPath}}}}})
		_ = encoder.Encode(harnessv2.Event{Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventUpdate, Identity: identity(5, now.Add(4*time.Millisecond)), Update: &harnessv2.UpdateEvent{Kind: harnessv2.UpdatePlan, Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{Content: "inspect repository", Status: harnessv2.PlanEntryCompleted}, {Content: "verify result", Status: harnessv2.PlanEntryInProgress}}}}})
		_ = encoder.Encode(harnessv2.Event{Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventUpdate, Identity: identity(6, now.Add(5*time.Millisecond)), Update: &harnessv2.UpdateEvent{Kind: harnessv2.UpdateDiagnostic, Diagnostic: &harnessv2.DiagnosticUpdate{Code: "provider_retry", Message: "provider retry recovered", Retryable: true}}})
		_ = encoder.Encode(harnessv2.Event{Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventUpdate, Identity: identity(7, now.Add(6*time.Millisecond)), Update: &harnessv2.UpdateEvent{Kind: harnessv2.UpdateAssistantMessageChunk, AssistantMessage: &harnessv2.AssistantMessageChunk{Text: "from runtime"}}})
		terminalType := harnessv2.EventCompleted
		if selected := terminalEvents[request.Metadata.PromptID]; selected != "" {
			terminalType = selected
		}
		terminal := harnessv2.Event{
			Protocol: harnessv2.ProtocolVersion,
			Type:     terminalType,
			Identity: identity(8, now.Add(7*time.Millisecond)),
		}
		switch terminalType {
		case harnessv2.EventCompleted:
			terminal.Completed = &harnessv2.CompletedEvent{
				StopReason: harnessv2.ACPStopReasonEndTurn,
				Result: harnessv2.PromptResult{
					Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: "from runtime"}},
					Model:   "served-model",
					Usage:   harnessv2.UsageUpdate{InputTokens: 100, OutputTokens: 25, CachedInputTokens: 40},
				},
			}
		case harnessv2.EventCancelled:
			terminal.Cancelled = &harnessv2.CancelledEvent{
				StopReason: harnessv2.ACPStopReasonCancelled,
				Reason:     "cancelled by deterministic test runtime",
			}
		case harnessv2.EventFailed:
			terminal.Failed = &harnessv2.FailedEvent{
				StopReason: harnessv2.ACPStopReasonRefusal,
				Code:       "deterministic_failure",
				Message:    "deterministic test runtime failure",
			}
		case harnessv2.EventOutcomeUnknown:
			terminal.OutcomeUnknown = &harnessv2.OutcomeUnknownEvent{
				Code:              "deterministic_outcome_unknown",
				Message:           "deterministic test runtime could not prove settlement",
				ForcedTermination: true,
			}
		default:
			t.Errorf("unsupported deterministic terminal event %q", terminalType)
			return
		}
		if err := encoder.Encode(terminal); err != nil {
			t.Errorf("encode terminal event: %v", err)
			return
		}
		if err := encoder.Close(); err != nil {
			t.Errorf("close encoder: %v", err)
		}
	})
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/workspace-deltas/{deltaID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.CreateWorkspaceDeltaRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode delta: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		writeDispatcherJSON(w, harnessv2.CreateWorkspaceDeltaResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			Delta: harnessv2.WorkspaceDeltaDescriptor{
				DeltaID: request.DeltaID, RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID,
				SessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration, State: harnessv2.WorkspaceDeltaNoChange,
				Intent: request.Intent, VerifiedBaseline: request.VerifiedBaseline, NoFollowVerified: true, PublicationSafe: true, FrozenAt: time.Now().UTC(),
			},
		})
	})
	mux.HandleFunc("DELETE /v2/runtime-sessions/{sessionID}", func(w http.ResponseWriter, r *http.Request) {
		var request harnessv2.DeleteRuntimeSessionRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		if options.onDelete != nil {
			options.onDelete(request)
		}
		descriptorMu.Lock()
		descriptor = harnessv2.RuntimeSessionDescriptor{}
		descriptorMu.Unlock()
		writeDispatcherJSON(w, harnessv2.DeleteRuntimeSessionResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh}, State: harnessv2.RuntimeSessionStateDeleted,
			Tombstone: testDeleteTombstone(request, time.Now().UTC()),
		})
	})
	return httptest.NewServer(mux)
}

func dispatcherRuntimeStatusResponse(
	digest harnessv2.ProfileDigest,
	descriptor harnessv2.RuntimeSessionDescriptor,
) harnessv2.StatusResponse {
	return dispatcherRuntimeStatusResponseForPool(digest, acpDispatcherTestPoolUID, descriptor)
}

func dispatcherRuntimeStatusResponseForPool(
	digest harnessv2.ProfileDigest,
	poolUID string,
	descriptor harnessv2.RuntimeSessionDescriptor,
) harnessv2.StatusResponse {
	response := harnessv2.StatusResponse{
		Protocol: harnessv2.ProtocolVersion,
		Fence: harnessv2.Fence{
			RuntimeInstanceID: "pod-uid.boot-id", SupervisorBootID: "boot-id", ControllerEpoch: 1,
			RuntimePoolUID: harnessv2.RuntimePoolUID(poolUID), RuntimePoolGeneration: 1, RuntimeProfileDigest: digest,
			ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		},
		Lifecycle: harnessv2.SupervisorLifecycleReady,
		Drain:     harnessv2.DrainStatus{AcceptingNewSessions: true},
		Timestamp: time.Now().UTC(),
	}
	if descriptor.RuntimeSessionUID != "" {
		response.Sessions = append(response.Sessions, harnessv2.RuntimeSessionStatus{
			RuntimeSessionID: descriptor.RuntimeSessionID, RuntimeSessionUID: descriptor.RuntimeSessionUID,
			Generation: descriptor.Generation, State: descriptor.State, LastTransitionAt: descriptor.LastTransitionAt,
		})
		response.Pressure.ResidentSessions = 1
	}
	return response
}

func testDeleteTombstone(request harnessv2.DeleteRuntimeSessionRequest, now time.Time) harnessv2.RuntimeSessionTombstone {
	return harnessv2.RuntimeSessionTombstone{
		RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID, RuntimeSessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration,
		RuntimeProfileDigest: request.Metadata.Fence.RuntimeProfileDigest, DeletedAt: now,
		Operations: []harnessv2.OperationRecord{{
			OperationID: request.Metadata.OperationID, RequestDigest: request.Metadata.RequestDigest,
			Phase: harnessv2.OperationPhaseDeleted, RecordedAt: now, UpdatedAt: now,
		}},
	}
}

func writeDispatcherJSON(w http.ResponseWriter, value any) {
	writeDispatcherJSONStatus(w, http.StatusOK, value)
}
func writeDispatcherJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func testControlDigestForDispatcher(value string) string {
	digest, _ := acpDomainDigest("dispatcher-test", value)
	return digest
}

func boundPromptAttemptForTest(attempt *store.PromptAttempt) *store.PromptAttempt {
	if attempt == nil {
		return nil
	}
	if attempt.BindingDigest == "" {
		attempt.BindingDigest = testControlDigestForDispatcher("test-v2-binding")
	}
	if attempt.SnapshotDigest == "" {
		attempt.SnapshotDigest = testControlDigestForDispatcher("test-v2-snapshot")
	}
	return attempt
}

func testACPExecuteBindingForDispatcher() *corev1alpha1.AgentExecutionBinding {
	return &corev1alpha1.AgentExecutionBinding{
		SchemaVersion:   1,
		ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
		Backend:         corev1alpha1.AgentExecutionBackendRuntimePool,
		BindingDigest:   testControlDigestForDispatcher("test-v2-binding"),
		Snapshot: corev1alpha1.AgentExecutionSnapshotRef{
			Digest: testControlDigestForDispatcher("test-v2-snapshot"),
		},
	}
}

func TestACPDispatcherRejectsPersistedExternalRuntimeAttemptsWithoutExecution(t *testing.T) {
	for _, state := range []corev1alpha1.TaskExecutionState{
		corev1alpha1.TaskExecutionStateQueued,
		corev1alpha1.TaskExecutionStateReserved,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var runtimeCalls atomic.Int32
			runtimeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				runtimeCalls.Add(1)
				http.Error(w, "external runtime dispatch must remain unreachable", http.StatusInternalServerError)
			}))
			defer runtimeServer.Close()

			db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "store.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close() //nolint:errcheck
			controlStore := sqlite.NewStore(db, "test")
			epochs, stopEpoch := startACPRecoveryEpochManager(t, ctx, controlStore, "external-cutover")
			defer stopEpoch()
			fence, err := epochs.CurrentFence(ctx)
			if err != nil {
				t.Fatal(err)
			}

			uid := types.UID("persisted-external-" + strings.ToLower(string(state)))
			promptID := "prompt-" + string(uid) + "-1"
			requestDigest := testControlDigestForDispatcher("external-cutover-" + string(state))
			runtimeRegistration := plannerExternalRuntime()
			runtimeRegistration.Spec.Deployment.Endpoint = runtimeServer.URL
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default", Name: "persisted-external-" + strings.ToLower(string(state)), UID: uid,
					Labels: map[string]string{acpExternalRuntimeTaskLabel: runtimeRegistration.Name},
				},
				Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do not dispatch"},
				Status: corev1alpha1.TaskStatus{
					Phase: corev1alpha1.TaskPhasePending, Attempts: 1,
					AgentExecutionBinding: testACPExecuteBindingForDispatcher(),
					Execution: &corev1alpha1.TaskExecutionStatus{
						State: state, Attempt: 1, PromptID: promptID, RequestDigest: requestDigest,
						AgentRuntimeName: runtimeRegistration.Name, AgentRuntimeUID: string(runtimeRegistration.UID), ControllerEpoch: fence.Epoch,
					},
					Delivery: &corev1alpha1.TaskDeliveryStatus{
						State: corev1alpha1.TaskDeliveryStateNotRequested, Outcome: corev1alpha1.TaskDeliveryOutcomeNotRequested,
					},
				},
			}
			scheme := runtime.NewScheme()
			if err := corev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			kubeClient := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.AgentRuntime{}).
				WithObjects(task, runtimeRegistration).Build()
			key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
			attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
				Key: key, RequestDigest: requestDigest, ExecutionState: store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
			}), fence)
			if err != nil {
				t.Fatal(err)
			}
			if state == corev1alpha1.TaskExecutionStateReserved {
				attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
					ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: store.PromptExecutionQueued,
					NewState: store.PromptExecutionReserved, OperationID: "persisted-reservation", OperationDigest: testControlDigestForDispatcher("persisted-reservation"), UpdatedAt: time.Now().UTC(),
				})
				if err != nil {
					t.Fatal(err)
				}
			}

			dispatcher := &ACPDispatcher{
				Client: kubeClient, APIReader: kubeClient, Store: controlStore, ResultStore: controlStore, Epochs: epochs,
				active: make(map[types.UID]struct{}), sem: make(chan struct{}, 1),
			}
			if err := dispatcher.dispatchOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if got := runtimeCalls.Load(); got != 0 {
				t.Fatalf("external runtime received %d execution client calls", got)
			}
			terminalAttempt, err := controlStore.GetPromptAttempt(ctx, attempt.ID)
			if err != nil {
				t.Fatal(err)
			}
			if terminalAttempt.ExecutionState != store.PromptExecutionFailed || terminalAttempt.TerminalReason != string(acpExternalRuntimeDispatchUnsupportedExecutionReason) {
				t.Fatalf("persisted attempt was not failed closed: %#v", terminalAttempt)
			}
			failed := &corev1alpha1.Task{}
			if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(task), failed); err != nil {
				t.Fatal(err)
			}
			wantMessage := externalAgentRuntimeDispatchUnsupportedReason(runtimeRegistration.Name)
			if failed.Status.Phase != corev1alpha1.TaskPhaseFailed || failed.Status.Execution == nil ||
				failed.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed || failed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeFailed ||
				failed.Status.Execution.Reason != acpExternalRuntimeDispatchUnsupportedExecutionReason || failed.Status.Execution.Message != wantMessage {
				t.Fatalf("persisted external Task was not failed closed: %#v", failed.Status)
			}
		})
	}
}

func TestACPDispatcherCapacityBackpressureRunsIdlePoolMaintenance(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	objects := make([]runtime.Object, 0, 4)
	for i := range 3 {
		objects = append(objects, &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: fmt.Sprintf("task-%d", i), UID: types.UID(fmt.Sprintf("task-uid-%d", i)), CreationTimestamp: metav1.NewTime(now.Add(time.Duration(i) * time.Second))},
			Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent},
			Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
				State: corev1alpha1.TaskExecutionStateQueued, RuntimePoolName: "busy-pool",
			}},
		})
	}
	idlePool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "unrelated-idle-pool",
			Annotations: map[string]string{acpRuntimeLastDemandAnnotation: now.Add(-time.Hour).Format(time.RFC3339Nano)},
		},
		Spec: corev1alpha1.RuntimePoolSpec{DesiredReplicas: 1},
	}
	objects = append(objects, idlePool)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithRuntimeObjects(objects...).Build()
	epochs := NewControllerEpochManager(nil, "dispatcher-backpressure-test")
	epochs.current = &store.ControllerEpoch{Name: store.DefaultControllerEpochName, Epoch: 1, HolderID: "dispatcher-backpressure-test"}
	close(epochs.ready)
	dispatcher := &ACPDispatcher{
		Client: kubeClient, APIReader: kubeClient, Epochs: epochs, IdlePoolTTL: time.Minute,
		active: make(map[types.UID]struct{}), sem: make(chan struct{}, 1),
	}
	dispatcher.sem <- struct{}{}
	if err := dispatcher.dispatchOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	dispatcher.mu.Lock()
	activeCount := len(dispatcher.active)
	dispatcher.mu.Unlock()
	if activeCount != 0 {
		t.Fatalf("backpressured active task count = %d, want 0", activeCount)
	}
	maintained := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(idlePool), maintained); err != nil {
		t.Fatal(err)
	}
	if maintained.Spec.DesiredReplicas != 0 {
		t.Fatalf("unrelated expired idle pool replicas = %d, want 0 under dispatch saturation", maintained.Spec.DesiredReplicas)
	}
}

func TestRuntimePoolHasActiveDemandAllowsIdleWorkspaceSessionScaleDown(t *testing.T) {
	pool := &corev1alpha1.RuntimePool{
		Spec: corev1alpha1.RuntimePoolSpec{
			DesiredReplicas: 1,
			ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
				Provider: corev1alpha1.WorkspaceProviderAgentSandbox,
			},
		},
		Status: corev1alpha1.RuntimePoolStatus{
			Capacity: corev1alpha1.RuntimePoolCapacityStatus{ResidentSessions: 1},
		},
	}

	if runtimePoolHasActiveDemand(pool, 0) {
		t.Fatal("idle workspace-backed resident session blocked authenticated scale-down")
	}

	pool.Spec.ExecutionWorkspace = nil
	if !runtimePoolHasActiveDemand(pool, 0) {
		t.Fatal("plain shared pool ignored its resident session")
	}
}

func TestACPDispatcherRuntimePoolReservationCASAndIdempotentRelease(t *testing.T) {
	ctx := context.Background()
	dispatcher, kubeClient, pool, first, second := newRuntimePoolReservationTestFixture(t)
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 1}

	claimed, firstIdentity, err := dispatcher.claimRuntimePoolReservation(ctx, first, pool.Name, fence, 1)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status.ActiveInstance == nil || firstIdentity.RuntimeInstanceID != claimed.Status.ActiveInstance.RuntimeInstanceID {
		t.Fatalf("reservation was not bound to the exact active instance: %#v", firstIdentity)
	}
	if _, duplicateIdentity, err := dispatcher.claimRuntimePoolReservation(ctx, first, pool.Name, fence, 1); err != nil {
		t.Fatal(err)
	} else if *duplicateIdentity != *firstIdentity {
		t.Fatalf("idempotent claim changed identity: got %#v want %#v", duplicateIdentity, firstIdentity)
	}
	if _, _, err := dispatcher.claimRuntimePoolReservation(ctx, second, pool.Name, fence, 1); !errors.Is(err, errACPRuntimePoolAtCapacity) {
		t.Fatalf("second claim error = %v, want capacity", err)
	}

	current := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
		t.Fatal(err)
	}
	if len(current.Status.Capacity.Reservations) != 1 || current.Status.Capacity.ReservedSessions != 1 || current.Status.Capacity.ReservedPrompts != 1 {
		t.Fatalf("unexpected reservation status: %#v", current.Status.Capacity)
	}
	if err := dispatcher.releaseRuntimePoolReservation(ctx, *firstIdentity); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.releaseRuntimePoolReservation(ctx, *firstIdentity); err != nil {
		t.Fatalf("idempotent release failed: %v", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
		t.Fatal(err)
	}
	if len(current.Status.Capacity.Reservations) != 0 || current.Status.Capacity.ReservedSessions != 0 || current.Status.Capacity.ReservedPrompts != 0 {
		t.Fatalf("reservation was not released: %#v", current.Status.Capacity)
	}
}

func TestACPDispatcherRuntimePoolConcurrentClaimsAreAtomic(t *testing.T) {
	ctx := context.Background()
	dispatcher, kubeClient, pool, first, second := newRuntimePoolReservationTestFixture(t)
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 1}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, task := range []*corev1alpha1.Task{first, second} {
		wg.Add(1)
		go func(task *corev1alpha1.Task) {
			defer wg.Done()
			<-start
			_, _, err := dispatcher.claimRuntimePoolReservation(ctx, task, pool.Name, fence, 1)
			results <- err
		}(task)
	}
	close(start)
	wg.Wait()
	close(results)

	var claimed, atCapacity int
	for err := range results {
		switch {
		case err == nil:
			claimed++
		case errors.Is(err, errACPRuntimePoolAtCapacity):
			atCapacity++
		default:
			t.Fatalf("unexpected claim error: %v", err)
		}
	}
	if claimed != 1 || atCapacity != 1 {
		t.Fatalf("claim results: claimed=%d atCapacity=%d", claimed, atCapacity)
	}
	current := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
		t.Fatal(err)
	}
	if len(current.Status.Capacity.Reservations) != 1 {
		t.Fatalf("atomic claim stored %d reservations", len(current.Status.Capacity.Reservations))
	}
}

func TestACPDispatcherSettlesDeletingTaskBeforeRuntimePoolAdmission(t *testing.T) {
	deletingAt := metav1.Now()
	runACPDispatcherPreAdmissionSettlementTest(t, func(task *corev1alpha1.Task) {
		task.DeletionTimestamp = &deletingAt
		task.Finalizers = []string{labels.TaskFinalizer}
	}, corev1alpha1.TaskExecutionReason("Cancelled"), "task cancelled before runtime admission", false)
}

func TestACPDispatcherSettlesDeletingTaskBeforeClosedAdmissionAndWorkerCapacity(t *testing.T) {
	deletingAt := metav1.Now()
	runACPDispatcherPreAdmissionSettlementTest(t, func(task *corev1alpha1.Task) {
		task.DeletionTimestamp = &deletingAt
		task.Finalizers = []string{labels.TaskFinalizer}
	}, corev1alpha1.TaskExecutionReason("Cancelled"), "task cancelled before runtime admission", true)
}

func TestACPDispatcherSettlesExpiredTaskBeforeRuntimePoolAdmission(t *testing.T) {
	timeout := metav1.Duration{Duration: time.Minute}
	runACPDispatcherPreAdmissionSettlementTest(t, func(task *corev1alpha1.Task) {
		task.CreationTimestamp = metav1.NewTime(time.Now().UTC().Add(-2 * time.Minute))
		task.Spec.Timeout = &timeout
	}, corev1alpha1.TaskExecutionReason("TaskTimeout"), "task deadline exceeded before runtime admission", false)
}

func TestACPDispatcherSettlesDefaultExpiredTaskBeforeRuntimePoolAdmission(t *testing.T) {
	runACPDispatcherPreAdmissionSettlementTest(t, func(task *corev1alpha1.Task) {
		task.CreationTimestamp = metav1.NewTime(time.Now().UTC().Add(-defaultACPTaskTimeout - time.Minute))
	}, corev1alpha1.TaskExecutionReason("TaskTimeout"), "task deadline exceeded before runtime admission", false)
}

func TestACPDispatcherSettlesExpiredSessionTaskBeforeRuntimePoolAdmission(t *testing.T) {
	timeout := metav1.Duration{Duration: time.Minute}
	runACPDispatcherPreAdmissionSettlementTest(t, func(task *corev1alpha1.Task) {
		task.CreationTimestamp = metav1.NewTime(time.Now().UTC().Add(-2 * time.Minute))
		task.Spec.Timeout = &timeout
		task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "pre-admission-session", Create: true, Append: true}
	}, corev1alpha1.TaskExecutionReason("TaskTimeout"), "task deadline exceeded before runtime admission", false)
}

func runACPDispatcherPreAdmissionSettlementTest(
	t *testing.T,
	mutateTask func(*corev1alpha1.Task),
	wantReason corev1alpha1.TaskExecutionReason,
	wantMessage string,
	throughDispatch bool,
) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := runtimePoolReservationTestTask("pre-admission", "pre-admission-uid", "pool-uid")
	task.Status.AgentExecutionBinding = testACPExecuteBindingForDispatcher()
	mutateTask(task)
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: task.Namespace, Name: "pool", UID: types.UID("pool-uid"), Generation: 1},
		Status: corev1alpha1.RuntimePoolStatus{
			Lifecycle:      corev1alpha1.RuntimePoolLifecycleServing,
			AdmissionState: corev1alpha1.RuntimePoolAdmissionDraining,
			ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{RuntimeInstanceID: "pod-uid.boot-id", ControllerEpoch: 1},
			Capacity:       corev1alpha1.RuntimePoolCapacityStatus{MaxResidentSessions: 1, MaxRunningPrompts: 1},
		},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).
		WithObjects(task.DeepCopy(), pool.DeepCopy()).Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "pre-admission.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "pre-admission-test")
	epochs := NewControllerEpochManager(controlStore, "pre-admission-controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	key := store.PromptAttemptKey{
		Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: task.Status.Execution.PromptID,
	}
	attemptID, err := key.CanonicalID()
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if _, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		ID: attemptID, Key: key, RequestDigest: task.Status.Execution.RequestDigest,
		ExecutionState: store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
	}), fence); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	dispatcher := &ACPDispatcher{Client: kubeClient, APIReader: kubeClient, Store: controlStore, ResultStore: controlStore, Epochs: epochs}
	if throughDispatch {
		dispatcher.AdmissionGate = NewACPAdmissionGate()
		dispatcher.AdmissionGate.Close("planned drain", time.Now().UTC())
		dispatcher.sem = make(chan struct{}, 1)
		dispatcher.sem <- struct{}{}
		dispatcher.active = make(map[types.UID]struct{})
		if err := dispatcher.dispatchOnce(ctx); err != nil {
			cancelEpoch()
			t.Fatal(err)
		}
	} else {
		reserved, _, err := dispatcher.reserveTask(ctx, task.DeepCopy())
		if err != nil {
			cancelEpoch()
			t.Fatal(err)
		}
		if reserved != nil {
			cancelEpoch()
			t.Fatalf("reserveTask() returned task %#v, want pre-admission settlement", reserved)
		}
	}
	attempt, err := controlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionCancelled {
		cancelEpoch()
		t.Fatalf("attempt state = %s, want %s", attempt.ExecutionState, store.PromptExecutionCancelled)
	}
	updated := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(task), updated); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseCancelled || updated.Status.Execution == nil ||
		updated.Status.Execution.State != corev1alpha1.TaskExecutionStateCancelled ||
		updated.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeCancelled ||
		updated.Status.Execution.Reason != wantReason || updated.Status.Execution.Message != wantMessage {
		cancelEpoch()
		t.Fatalf("settled task status = %#v, want reason=%s message=%q", updated.Status, wantReason, wantMessage)
	}
	projectionID := standaloneTaskTerminalProjectionID(task, task.Status.Execution.Attempt)
	if _, err := controlStore.GetOutboxProjection(ctx, projectionID); err != nil {
		cancelEpoch()
		t.Fatalf("terminal projection %q: %v", projectionID, err)
	}
	updatedPool := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(pool), updatedPool); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if len(updatedPool.Status.Capacity.Reservations) != 0 {
		cancelEpoch()
		t.Fatalf("pre-admission settlement claimed RuntimePool capacity: %#v", updatedPool.Status.Capacity.Reservations)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestACPDispatcherRuntimePoolReservationReclaimsExpiredAndReplacedInstance(t *testing.T) {
	ctx := context.Background()
	dispatcher, kubeClient, pool, first, _ := newRuntimePoolReservationTestFixture(t)
	now := time.Now().UTC()
	pool.Status.Capacity.Reservations = []corev1alpha1.RuntimePoolCapacityReservationStatus{
		{
			PoolUID: string(pool.UID), TaskUID: "expired", Attempt: 1, ControllerEpoch: 1,
			RuntimeInstanceID: pool.Status.ActiveInstance.RuntimeInstanceID, ResidentSlots: 1, PromptSlots: 1,
			ReservedAt: metav1.NewTime(now.Add(-time.Minute)), ExpiresAt: metav1.NewTime(now.Add(-time.Second)),
		},
		{
			PoolUID: string(pool.UID), TaskUID: "old-instance", Attempt: 1, ControllerEpoch: 1,
			RuntimeInstanceID: "old-pod.old-boot", ResidentSlots: 1, PromptSlots: 1,
			ReservedAt: metav1.NewTime(now.Add(-time.Minute)), ExpiresAt: metav1.NewTime(now.Add(time.Minute)),
		},
	}
	pool.Status.Capacity.ReservedSessions = 2
	pool.Status.Capacity.ReservedPrompts = 2
	if err := kubeClient.Status().Update(ctx, pool); err != nil {
		t.Fatal(err)
	}
	fence := store.ControllerEpochFence{Name: store.DefaultControllerEpochName, Epoch: 1}
	if _, _, err := dispatcher.claimRuntimePoolReservation(ctx, first, pool.Name, fence, 1); err != nil {
		t.Fatal(err)
	}
	current := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
		t.Fatal(err)
	}
	if len(current.Status.Capacity.Reservations) != 1 || current.Status.Capacity.Reservations[0].TaskUID != string(first.UID) {
		t.Fatalf("stale reservations were not reclaimed: %#v", current.Status.Capacity.Reservations)
	}
}

func TestACPDispatcherPreAcceptanceRateLimitRequeuesWithoutTerminalFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := runtimePoolReservationTestTask("rate-limited", "rate-limited-uid", "pool-uid")
	task.Status.Execution.State = corev1alpha1.TaskExecutionStatePlanned
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(task).Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "controller-test")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: task.Status.Execution.PromptID}
	attemptID, err := key.CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		ID: attemptID, Key: key, RequestDigest: task.Status.Execution.RequestDigest,
		ExecutionState: store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
	}), fence); err != nil {
		t.Fatal(err)
	}
	dispatcher := &ACPDispatcher{Client: kubeClient, Store: controlStore, Epochs: epochs}
	for _, transition := range []struct {
		from, to store.PromptExecutionState
	}{
		{store.PromptExecutionQueued, store.PromptExecutionReserved},
		{store.PromptExecutionReserved, store.PromptExecutionSessionStarting},
		{store.PromptExecutionSessionStarting, store.PromptExecutionPlanned},
	} {
		if err := dispatcher.transitionAttempt(ctx, attemptID, fence, transition.from, transition.to, "test-"+string(transition.to), nil); err != nil {
			t.Fatal(err)
		}
	}
	rateLimited := &harnessv2.ClientError{StatusCode: http.StatusTooManyRequests, Code: harnessv2.ErrorCodeRateLimited, Retryable: true}
	retrying, err := dispatcher.handlePrePromptClientError(ctx, task.DeepCopy(), attemptID, fence, rateLimited)
	if err != nil {
		t.Fatal(err)
	}
	if !retrying {
		t.Fatal("rate-limited RuntimeSession start was not classified as a retry")
	}
	attempt, err := controlStore.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionReserved {
		t.Fatalf("rate-limited attempt state = %s, want Reserved", attempt.ExecutionState)
	}
	updated := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(task), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Execution == nil || updated.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved || updated.Status.Execution.Reason != corev1alpha1.TaskExecutionReasonAtCapacity || taskExecutionStateTerminal(updated.Status.Execution.State) {
		t.Fatalf("rate-limited task was not nonterminal: %#v", updated.Status.Execution)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestACPDispatcherReservedRetryReportsOnlyBoundedStage(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := runtimePoolReservationTestTask("session-retry", "session-retry-uid", "pool-uid")
	task.Status.Execution.State = corev1alpha1.TaskExecutionStateReserved
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}).
		WithObjects(task).
		Build()
	dispatcher := &ACPDispatcher{Client: kubeClient}

	if err := dispatcher.requeueReservedTask(
		context.Background(), task.DeepCopy(), acpReservedRetrySessionPreparation,
		errors.New("sensitive provider diagnostic"),
	); err != nil {
		t.Fatal(err)
	}

	updated := &corev1alpha1.Task{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(task), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Execution == nil {
		t.Fatal("reserved retry removed execution status")
	}
	if got, want := updated.Status.Execution.Message, "RuntimePool admission will be retried (stage: session-preparation)"; got != want {
		t.Fatalf("retry message = %q, want %q", got, want)
	}
	if strings.Contains(updated.Status.Execution.Message, "sensitive provider diagnostic") {
		t.Fatalf("retry message exposed the underlying cause: %q", updated.Status.Execution.Message)
	}
}

//nolint:gocyclo // The recovery proof keeps the cross-store state transitions visible in one test.
func TestACPDispatcherReserveTaskRecoversIncompletePreSubmissionRollback(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	task := runtimePoolReservationTestTask("session-rollback", "session-rollback-uid", "pool-uid")
	task.Status.Execution.State = corev1alpha1.TaskExecutionStateReserved
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "session-rollback", Create: true, Append: true}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: task.Namespace, Name: "pool", UID: types.UID("pool-uid"), Generation: 1},
		Spec: corev1alpha1.RuntimePoolSpec{
			DesiredReplicas: 1,
			Capacity: &corev1alpha1.RuntimePoolCapacitySpec{
				MaxResidentSessions: 1,
				MaxRunningPrompts:   1,
			},
		},
		Status: corev1alpha1.RuntimePoolStatus{
			Lifecycle:      corev1alpha1.RuntimePoolLifecycleServing,
			AdmissionState: corev1alpha1.RuntimePoolAdmissionAccepting,
			ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{
				RuntimeInstanceID: "pod-uid.boot-id",
				ControllerEpoch:   1,
			},
			Capacity: corev1alpha1.RuntimePoolCapacityStatus{
				MaxResidentSessions: 1,
				MaxRunningPrompts:   1,
			},
		},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{},
			&corev1alpha1.ControllerEpoch{}, &corev1alpha1.PromptAttempt{},
		).
		WithObjects(task.DeepCopy(), pool.DeepCopy()).
		Build()
	kubeClient = withControllerEpochLeaseUIDs(t, kubeClient)
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "session-rollback.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	sqliteStore := sqlite.NewStore(db, "session-rollback-test")
	controlStore, err := kubestore.NewComposite(kubeClient, "orka-system", sqliteStore)
	if err != nil {
		t.Fatal(err)
	}
	epochs := NewControllerEpochManager(controlStore, "session-rollback-controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if fence.Epoch != pool.Status.ActiveInstance.ControllerEpoch {
		cancelEpoch()
		t.Fatalf("controller epoch = %d, want %d", fence.Epoch, pool.Status.ActiveInstance.ControllerEpoch)
	}

	key := store.PromptAttemptKey{
		Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: task.Status.Execution.PromptID,
	}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		Key: key, RequestDigest: task.Status.Execution.RequestDigest,
	}), fence)
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	dispatcher := &ACPDispatcher{
		Client: kubeClient, APIReader: kubeClient, Store: controlStore, Epochs: epochs,
		ReservationTTL: time.Minute,
	}
	if err := dispatcher.transitionAttempt(
		ctx, attempt.ID, fence, store.PromptExecutionQueued, store.PromptExecutionReserved, "reserve-session-rollback", nil,
	); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if err := dispatcher.transitionAttempt(
		ctx, attempt.ID, fence, store.PromptExecutionReserved, store.PromptExecutionSessionStarting,
		"session-starting-session-rollback", &attemptRuntimeBinding{
			RuntimeInstanceID: "pod-uid.boot-id", SessionUID: "session-uid", SessionGeneration: 1,
		},
	); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}

	reserved, target, err := dispatcher.reserveTask(ctx, task.DeepCopy())
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if reserved == nil || target.pool == nil || target.reservation == nil {
		cancelEpoch()
		t.Fatalf("reserveTask() = task %#v, target %#v; want recovered reservation", reserved, target)
	}
	reservedPool := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(pool), reservedPool); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if len(reservedPool.Status.Capacity.Reservations) != 1 ||
		reservedPool.Status.Capacity.Reservations[0].ResidentSlots != 0 ||
		reservedPool.Status.Capacity.Reservations[0].PromptSlots != 1 ||
		reservedPool.Status.Capacity.ReservedSessions != 0 ||
		reservedPool.Status.Capacity.ReservedPrompts != 1 {
		cancelEpoch()
		t.Fatalf("Session-backed reservation = %#v, want prompt-only capacity", reservedPool.Status.Capacity)
	}
	recovered, err := controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if recovered.ExecutionState != store.PromptExecutionReserved || recovered.SessionUID != "" || recovered.SessionLeaseGeneration != 0 {
		cancelEpoch()
		t.Fatalf("recovered PromptAttempt = %#v, want unbound Reserved", recovered)
	}
	if reserved.Status.Execution == nil || reserved.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved ||
		reserved.Status.Execution.Reason != "" || reserved.Status.Execution.Message != "" {
		cancelEpoch()
		t.Fatalf("recovered Task execution = %#v, want clean Reserved", reserved.Status.Execution)
	}
	if err := dispatcher.releaseRuntimePoolReservation(ctx, *target.reservation); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func newRuntimePoolReservationTestFixture(t *testing.T) (*ACPDispatcher, client.Client, *corev1alpha1.RuntimePool, *corev1alpha1.Task, *corev1alpha1.Task) {
	t.Helper()
	const maxResident, maxPrompts int32 = 1, 1
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pool", UID: types.UID("pool-uid"), Generation: 1},
		Spec:       corev1alpha1.RuntimePoolSpec{Capacity: &corev1alpha1.RuntimePoolCapacitySpec{MaxResidentSessions: maxResident, MaxRunningPrompts: maxPrompts}},
		Status: corev1alpha1.RuntimePoolStatus{
			Lifecycle: corev1alpha1.RuntimePoolLifecycleServing, AdmissionState: corev1alpha1.RuntimePoolAdmissionAccepting,
			ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{RuntimeInstanceID: "pod-uid.boot-id", ControllerEpoch: 1},
			Capacity:       corev1alpha1.RuntimePoolCapacityStatus{MaxResidentSessions: maxResident, MaxRunningPrompts: maxPrompts},
		},
	}
	first := runtimePoolReservationTestTask("first", "first-uid", string(pool.UID))
	second := runtimePoolReservationTestTask("second", "second-uid", string(pool.UID))
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RuntimePool{}).WithObjects(pool).Build()
	return &ACPDispatcher{Client: kubeClient, ReservationTTL: time.Minute}, kubeClient, pool.DeepCopy(), first, second
}

func TestACPDispatcherRuntimePoolReservationResidentUpgradeChecksCapacity(t *testing.T) {
	dispatcher, kubeClient, pool, task, _ := newRuntimePoolReservationTestFixture(t)
	ctx := context.Background()
	current := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
		t.Fatal(err)
	}
	current.Status.Capacity.ResidentSessions = 1
	if err := kubeClient.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}

	_, identity, err := dispatcher.claimRuntimePoolReservation(
		ctx, task, pool.Name, store.ControllerEpochFence{Epoch: 1}, 0,
	)
	if err != nil {
		t.Fatalf("claim prompt-only reservation: %v", err)
	}
	lease := newACPRuntimePoolReservationLease(dispatcher, identity)
	if err := lease.setSlots(ctx, 1); !errors.Is(err, errACPRuntimePoolAtCapacity) {
		t.Fatalf("resident upgrade error = %v, want RuntimePool capacity", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
		t.Fatal(err)
	}
	if len(current.Status.Capacity.Reservations) != 1 ||
		current.Status.Capacity.Reservations[0].ResidentSlots != 0 ||
		current.Status.Capacity.Reservations[0].PromptSlots != 1 ||
		current.Status.Capacity.ReservedSessions != 0 || current.Status.Capacity.ReservedPrompts != 1 {
		t.Fatalf("capacity-denied upgrade mutated reservation: %#v", current.Status.Capacity)
	}

	current.Status.Capacity.ResidentSessions = 0
	if err := kubeClient.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := lease.setSlots(ctx, 1); err != nil {
		t.Fatalf("upgrade reservation after capacity release: %v", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
		t.Fatal(err)
	}
	if len(current.Status.Capacity.Reservations) != 1 ||
		current.Status.Capacity.Reservations[0].ResidentSlots != 1 ||
		current.Status.Capacity.Reservations[0].PromptSlots != 1 ||
		current.Status.Capacity.ReservedSessions != 1 || current.Status.Capacity.ReservedPrompts != 1 {
		t.Fatalf("resident upgrade was not committed atomically: %#v", current.Status.Capacity)
	}
}

func runtimePoolReservationTestTask(name, uid, poolUID string) *corev1alpha1.Task {
	return &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, UID: types.UID(uid)},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent},
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateQueued, Attempt: 1, PromptID: "prompt-" + uid + "-1",
			RuntimePoolName: "pool", RuntimePoolUID: poolUID, RequestDigest: testControlDigestForDispatcher(name), ControllerEpoch: 1,
		}},
	}
}

func TestACPDispatcherQuiescesInterruptedSessionPreparation(t *testing.T) {
	for _, openTurn := range []bool{false, true} {
		name := "lease-only"
		if openTurn {
			name = "open-turn"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), name+".db"))
			defer closeStore()
			continuity := newACPSessionTestContinuity(t, controlStore, ACPBootstrapLimits{})
			taskUID := types.UID("77777777-7777-7777-7777-777777777777")
			promptID := "prompt-" + string(taskUID) + "-1"
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "interrupted-" + name, UID: taskUID},
				Spec: corev1alpha1.TaskSpec{
					Type: corev1alpha1.TaskTypeAgent, Prompt: "deadline during session preparation",
					SessionRef: &corev1alpha1.SessionReference{Name: "interrupted-" + name, Create: true, Append: true},
				},
				Status: corev1alpha1.TaskStatus{Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
					State: corev1alpha1.TaskExecutionStateReserved, Attempt: 1, PromptID: promptID,
					RequestDigest: testControlDigestForDispatcher("interrupted-" + name), ControllerEpoch: fence.Epoch,
				}},
			}
			scheme := runtime.NewScheme()
			if err := corev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(task.DeepCopy()).Build()
			key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
			attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{Key: key, RequestDigest: task.Status.Execution.RequestDigest}), fence)
			if err != nil {
				t.Fatal(err)
			}
			attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
				ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
				NewState: store.PromptExecutionReserved, OperationID: "reserve-" + name,
				OperationDigest: testControlDigestForDispatcher("reserve-" + name), UpdatedAt: time.Now().UTC(),
			})
			if err != nil {
				t.Fatal(err)
			}
			dispatcher := &ACPDispatcher{
				Client: kubeClient, APIReader: kubeClient, Store: controlStore, Sessions: continuity,
				runtimeSessions: make(map[string]ACPRuntimeSessionBinding),
			}

			var opened *acpTaskSession
			if openTurn {
				opened, err = dispatcher.prepareTaskSession(
					ctx, task, fence, harnessv2.ProfileDigest(testControlDigestForDispatcher("profile-"+name)),
					testControlDigestForDispatcher("mcp-"+name),
					harnessv2.RuntimeInstanceID("runtime-instance"), harnessv2.SupervisorBootID("boot-id"),
					acpSessionLineageIdentity{},
				)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				control, err := continuity.EnsureSession(ctx, ACPEnsureSessionRequest{
					Namespace: task.Namespace, SessionName: task.Spec.SessionRef.Name, SessionType: "task", Fence: fence, CreatedAt: time.Now().UTC(),
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := dispatcher.transitionAttempt(ctx, attempt.ID, fence, store.PromptExecutionReserved, store.PromptExecutionSessionStarting, "session-starting-"+name, &attemptRuntimeBinding{
					RuntimeInstanceID: "runtime-instance", SessionUID: control.SessionUID, SessionGeneration: control.LeaseGeneration + 1,
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := dispatcher.acquireTaskSessionLease(ctx, task, fence, control, acpSessionLineageIdentity{}); err != nil {
					t.Fatal(err)
				}
			}

			recovered, err := dispatcher.quiesceInterruptedTaskSessionPreparation(ctx, task, attempt.ID, fence)
			if err != nil {
				t.Fatal(err)
			}
			if openTurn {
				if recovered == nil || recovered.Turn == nil || opened == nil || recovered.Turn.Turn.ID != opened.Turn.Turn.ID {
					t.Fatalf("recovered session = %#v, opened = %#v", recovered, opened)
				}
				if err := dispatcher.transitionAttemptToTerminal(ctx, attempt.ID, fence, store.PromptExecutionCancelled, "deadline-"+name); err != nil {
					t.Fatal(err)
				}
				if err := dispatcher.reconcileUnfinalizedTaskSession(ctx, task, fence, recovered, context.DeadlineExceeded); err != nil {
					t.Fatal(err)
				}
				turn, err := controlStore.GetSessionTurn(ctx, recovered.Turn.Turn.ID)
				if err != nil {
					t.Fatal(err)
				}
				if turn.State != store.SessionTurnFinalized {
					t.Fatalf("SessionTurn state = %s, want %s", turn.State, store.SessionTurnFinalized)
				}
			} else {
				if recovered != nil {
					t.Fatalf("lease-only cleanup recovered SessionTurn %#v", recovered)
				}
				attempt, err := controlStore.GetPromptAttempt(ctx, attempt.ID)
				if err != nil {
					t.Fatal(err)
				}
				if attempt.ExecutionState != store.PromptExecutionReserved || attempt.SessionUID != "" || attempt.SessionLeaseGeneration != 0 {
					t.Fatalf("quiesced PromptAttempt = %#v", attempt)
				}
			}
			control, err := controlStore.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name)
			if err != nil {
				t.Fatal(err)
			}
			if control.Lease != nil {
				t.Fatalf("interrupted preparation leaked mutation lease: %#v", control.Lease)
			}
		})
	}
}

func TestACPDispatcherAbortsLeaseWhenSessionTurnOpenFails(t *testing.T) {
	ctx := context.Background()
	baseStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "open-failure.db"))
	defer closeStore()
	failingStore := &failSessionTurnOpenStore{DurableControlStore: baseStore}
	continuity, err := NewACPSessionContinuity(ACPSessionContinuityConfig{
		SessionControls: failingStore, Transcripts: baseStore, Publications: failingStore, BranchClaims: failingStore,
		NewSessionUID: func() (string, error) { return "open-failure-session-uid", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	taskUID := types.UID("66666666-6666-6666-6666-666666666666")
	promptID := "prompt-" + string(taskUID) + "-1"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "open-failure", UID: taskUID},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "fail open", SessionRef: &corev1alpha1.SessionReference{Name: "open-failure", Create: true, Append: true}},
		Status: corev1alpha1.TaskStatus{Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateReserved, Attempt: 1, PromptID: promptID,
			RequestDigest: testControlDigestForDispatcher("open-failure"), ControllerEpoch: fence.Epoch,
		}},
	}
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(task.DeepCopy()).Build()
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attempt, err := baseStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{Key: key, RequestDigest: task.Status.Execution.RequestDigest}), fence)
	if err != nil {
		t.Fatal(err)
	}
	_, err = baseStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
		NewState: store.PromptExecutionReserved, OperationID: "reserve-open-failure", OperationDigest: testControlDigestForDispatcher("reserve-open-failure"), UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &ACPDispatcher{Client: kubeClient, Store: failingStore, Sessions: continuity, runtimeSessions: make(map[string]ACPRuntimeSessionBinding)}
	if _, err := dispatcher.prepareTaskSession(
		ctx, task, fence, harnessv2.ProfileDigest(testControlDigestForDispatcher("profile-open-failure")),
		testControlDigestForDispatcher("mcp-open-failure"), "runtime", "boot",
		acpSessionLineageIdentity{},
	); err == nil {
		t.Fatal("prepareTaskSession unexpectedly succeeded")
	}
	control, err := baseStore.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name)
	if err != nil {
		t.Fatal(err)
	}
	if control.Lease != nil {
		t.Fatalf("failed SessionTurn open leaked lease: %#v", control.Lease)
	}
	attempt, err = baseStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionReserved {
		t.Fatalf("attempt state = %s", attempt.ExecutionState)
	}
}

type failSessionTurnOpenStore struct{ store.DurableControlStore }

func (s *failSessionTurnOpenStore) CreateSessionTurn(context.Context, store.CreateSessionTurnRequest) (*store.SessionTurn, error) {
	return nil, store.ValidationErrorf("simulated SessionTurn persistence failure")
}
func (s *failSessionTurnOpenStore) GetSessionTurn(context.Context, string) (*store.SessionTurn, error) {
	return nil, store.ErrNotFound
}

type orderedSessionControlStore struct {
	store.SessionControlStore
	onFinalized func()
}

func (s *orderedSessionControlStore) FinalizeSessionTurn(
	ctx context.Context,
	request store.FinalizeSessionTurnRequest,
) (*store.SessionTurn, error) {
	turn, err := s.SessionControlStore.FinalizeSessionTurn(ctx, request)
	if err == nil && s.onFinalized != nil {
		s.onFinalized()
	}
	return turn, err
}

func TestACPWorkspaceDeltaLimitsFromTaskPolicy(t *testing.T) {
	maxFiles := int32(7)
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Workspace: &corev1alpha1.WorkspaceConfig{
		MaxChangedFiles:            &maxFiles,
		AllowedPaths:               []string{"internal/**", "README.md"},
		DenyRepositoryControlPaths: true,
		RejectBinaryFiles:          true,
		RejectSecretLikeContent:    true,
	}}}
	limits := acpWorkspaceDeltaLimits(task)
	if limits.MaxChangedFiles != 7 || !slices.Equal(limits.AllowedPaths, []string{"internal/**", "README.md"}) || !limits.DenyRepositoryControlPaths || !limits.RejectBinaryFiles || !limits.RejectSecretLikeContent {
		t.Fatalf("limits = %#v", limits)
	}
	limits.AllowedPaths[0] = "changed"
	if task.Spec.Workspace.AllowedPaths[0] != "internal/**" {
		t.Fatal("limits alias Task workspace policy")
	}
}

func TestACPDispatcherGatewaySessionUsesTranscriptBackedPrompt(t *testing.T) {
	ctx := context.Background()
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "gateway-transcript.db"))
	defer closeStore()
	continuity := newACPSessionTestContinuity(t, controlStore, ACPBootstrapLimits{})
	const (
		sessionName   = "gateway-session"
		gatewayPrompt = "answer the gateway request from the canonical transcript"
	)
	throughMessageID := store.GatewayUserMessageID("gateway-event")
	if err := controlStore.CreateSession(ctx, &store.SessionRecord{
		Namespace: "ns", Name: sessionName, SessionType: store.SessionTypeGateway,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := controlStore.AppendMessages(ctx, "ns", sessionName, []store.SessionMessage{
		{ID: "gateway:prior:user", Role: "user", Content: "earlier request", Timestamp: time.Now().UTC()},
		{ID: "gateway:prior:assistant", Role: "assistant", Content: "earlier response", Timestamp: time.Now().UTC()},
		{ID: throughMessageID, Role: "user", Content: gatewayPrompt, Timestamp: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	taskUID := types.UID("88888888-8888-8888-8888-888888888888")
	promptID := "prompt-" + string(taskUID) + "-1"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "gateway-task", UID: taskUID},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent,
			SessionRef: &corev1alpha1.SessionReference{
				Name: sessionName, Create: false, Append: false,
				MaxMessages:      int32(store.GatewayTranscriptMessageLimit),
				ThroughMessageID: throughMessageID, PromptIncluded: true,
			},
		},
		Status: corev1alpha1.TaskStatus{Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateReserved, Attempt: 1, PromptID: promptID,
			RequestDigest: testControlDigestForDispatcher("gateway-transcript"), ControllerEpoch: fence.Epoch,
		}},
	}
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{Key: key, RequestDigest: task.Status.Execution.RequestDigest}), fence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
		NewState: store.PromptExecutionReserved, OperationID: "reserve-gateway-transcript",
		OperationDigest: testControlDigestForDispatcher("reserve-gateway-transcript"), UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	dispatcher := &ACPDispatcher{Store: controlStore, Sessions: continuity, runtimeSessions: make(map[string]ACPRuntimeSessionBinding)}
	session, err := dispatcher.prepareTaskSession(
		ctx, task, fence, harnessv2.ProfileDigest(testControlDigestForDispatcher("profile-gateway-transcript")),
		testControlDigestForDispatcher("mcp-gateway-transcript"),
		harnessv2.RuntimeInstanceID("runtime-instance"), harnessv2.SupervisorBootID("boot-id"),
		acpSessionLineageIdentity{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if session == nil || session.Turn == nil || session.Bootstrap == nil {
		t.Fatalf("gateway session preparation = %#v", session)
	}
	if !session.Turn.SkipTranscriptAppend {
		t.Fatalf("gateway transcript append policy = %#v", session.Turn)
	}
	if session.UserPrompt != gatewayPrompt || session.Turn.Turn.UserPrompt != gatewayPrompt {
		t.Fatalf("gateway prompt binding = %#v, turn=%#v", session, session.Turn.Turn)
	}
	if len(session.Bootstrap.Messages) != 2 || strings.Contains(string(session.Bootstrap.Artifact), gatewayPrompt) {
		t.Fatalf("gateway bootstrap duplicated the current prompt: %#v", session.Bootstrap.Messages)
	}
	recovered, err := dispatcher.recoveredTaskSession(ctx, task, mustPromptAttemptForDispatcherTest(t, controlStore, attempt.ID))
	if err != nil {
		t.Fatal(err)
	}
	if recovered == nil || recovered.UserPrompt != gatewayPrompt || recovered.Bootstrap == nil ||
		strings.Contains(string(recovered.Bootstrap.Artifact), gatewayPrompt) {
		t.Fatalf("recovered gateway session = %#v", recovered)
	}
	content := acpPromptInputContent(bootstrapPromptText(session.Bootstrap), session.UserPrompt)
	if len(content) != 2 || !strings.Contains(content[0].Text, "earlier response") || content[1].Text != gatewayPrompt {
		t.Fatalf("gateway provider input = %#v, want history then exact current prompt", content)
	}

	attempt, err = controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []store.PromptExecutionState{
		store.PromptExecutionPlanned, store.PromptExecutionSubmitting, store.PromptExecutionAccepted,
		store.PromptExecutionRunning, store.PromptExecutionSettling, store.PromptExecutionSucceeded,
	} {
		operationID := "gateway-execution-" + string(state)
		attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: state, OperationID: operationID, OperationDigest: testControlDigestForDispatcher(operationID),
			UpdatedAt: attempt.UpdatedAt.Add(time.Second),
		})
		if err != nil {
			t.Fatalf("transition gateway attempt to %s: %v", state, err)
		}
	}
	if _, err := continuity.FinalizeAssistantResult(ctx, ACPFinalizeAssistantRequest{
		SessionTurn: *session.Turn, Fence: fence, AssistantResult: "gateway response",
		Projection: acpSessionProjectionForTest("gateway-final", "Succeeded"), FinalizedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	transcript, err := controlStore.LoadTranscript(ctx, task.Namespace, sessionName, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(transcript) != 3 || transcript[2].ID != throughMessageID {
		t.Fatalf("gateway ACP finalization duplicated transcript entries: %#v", transcript)
	}
}

func TestACPDispatcherGatewayCreateFalseDoesNotCreateMissingTranscript(t *testing.T) {
	ctx := context.Background()
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "gateway-missing.db"))
	defer closeStore()
	continuity := newACPSessionTestContinuity(t, controlStore, ACPBootstrapLimits{})
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "gateway-missing", UID: types.UID("99999999-9999-9999-9999-999999999999")},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent,
			SessionRef: &corev1alpha1.SessionReference{
				Name: "missing-gateway-session", Create: false, Append: false,
				MaxMessages:      int32(store.GatewayTranscriptMessageLimit),
				ThroughMessageID: store.GatewayUserMessageID("missing-event"), PromptIncluded: true,
			},
		},
	}
	dispatcher := &ACPDispatcher{Store: controlStore, Sessions: continuity}
	_, err := dispatcher.planTaskSession(
		ctx, task, fence, harnessv2.ProfileDigest(testControlDigestForDispatcher("profile-gateway-missing")),
		testControlDigestForDispatcher("mcp-gateway-missing"), "runtime", "boot", "",
	)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing gateway transcript error = %v, want ErrNotFound", err)
	}
	if _, err := controlStore.GetSession(ctx, task.Namespace, task.Spec.SessionRef.Name); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing gateway transcript was created: %v", err)
	}
	if _, err := controlStore.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing gateway SessionControl was created: %v", err)
	}
}

func TestACPDispatcherRejectsFrozenWorkspaceSessionUIDMismatch(t *testing.T) {
	ctx := context.Background()
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "workspace-session-uid.db"))
	defer closeStore()
	continuity := newACPSessionTestContinuity(t, controlStore, ACPBootstrapLimits{})
	control := ensureACPSessionForTest(t, continuity, fence, "review-loop")
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: control.Namespace, Name: "workspace-session-uid", UID: types.UID("88888888-8888-8888-8888-888888888888")},
		Spec: corev1alpha1.TaskSpec{
			Type:       corev1alpha1.TaskTypeAgent,
			Prompt:     "continue",
			SessionRef: &corev1alpha1.SessionReference{Name: control.SessionName, Append: true},
		},
	}
	dispatcher := &ACPDispatcher{Store: controlStore, Sessions: continuity}
	_, err := dispatcher.planTaskSession(
		ctx, task, fence, harnessv2.ProfileDigest(testControlDigestForDispatcher("profile-workspace-session-uid")),
		testControlDigestForDispatcher("mcp-workspace-session-uid"), "runtime", "boot", "different-session-incarnation",
	)
	if !errors.Is(err, store.ErrConflict) || !strings.Contains(err.Error(), "immutable UID") {
		t.Fatalf("Session UID mismatch error = %v, want immutable-identity conflict", err)
	}
}

func mustPromptAttemptForDispatcherTest(t *testing.T, storeValue store.PromptAttemptStore, id string) *store.PromptAttempt {
	t.Helper()
	attempt, err := storeValue.GetPromptAttempt(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return attempt
}

func TestACPDispatcherHonorsMaxMessagesWithoutThroughMessageID(t *testing.T) {
	ctx := context.Background()
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "max-messages.db"))
	defer closeStore()
	continuity := newACPSessionTestContinuity(t, controlStore, ACPBootstrapLimits{})
	control := ensureACPSessionForTest(t, continuity, fence, "max-messages")
	if err := controlStore.AppendMessages(ctx, control.Namespace, control.SessionName, []store.SessionMessage{
		{Role: "user", Content: "oldest", Timestamp: time.Now().UTC()},
		{Role: "assistant", Content: "middle", Timestamp: time.Now().UTC()},
		{Role: "user", Content: "newest", Timestamp: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: control.Namespace},
		Spec: corev1alpha1.TaskSpec{
			Prompt: "continue",
			SessionRef: &corev1alpha1.SessionReference{
				Name: control.SessionName, Append: true, MaxMessages: 1,
			},
		},
	}
	bootstrap, userPrompt, err := (&ACPDispatcher{Sessions: continuity}).resolveTaskSessionBootstrap(ctx, task, control)
	if err != nil {
		t.Fatal(err)
	}
	if userPrompt != task.Spec.Prompt {
		t.Fatalf("user prompt = %q, want %q", userPrompt, task.Spec.Prompt)
	}
	if bootstrap == nil || len(bootstrap.Messages) != 1 || bootstrap.Messages[0].Content != "newest" {
		t.Fatalf("bounded bootstrap = %#v, want only newest message", bootstrap)
	}
}

func TestACPDispatcherPromptIncludedSessionAppendsAssistantOnly(t *testing.T) {
	ctx := context.Background()
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "prompt-included-append.db"))
	defer closeStore()
	continuity := newACPSessionTestContinuity(t, controlStore, ACPBootstrapLimits{})
	control := ensureACPSessionForTest(t, continuity, fence, "prompt-included-append")
	const (
		throughMessageID = "canonical-user"
		currentPrompt    = "canonical transcript prompt"
		assistantResult  = "assistant result"
	)
	if err := controlStore.AppendMessages(ctx, control.Namespace, control.SessionName, []store.SessionMessage{
		{ID: "earlier-assistant", Role: "assistant", Content: "earlier response", Timestamp: time.Now().UTC()},
		{ID: throughMessageID, Role: "user", Content: currentPrompt, Timestamp: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	taskUID := types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	promptID := "prompt-" + string(taskUID) + "-1"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: control.Namespace, Name: "prompt-included-append", UID: taskUID},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent,
			SessionRef: &corev1alpha1.SessionReference{
				Name: control.SessionName, Create: false, Append: true, MaxMessages: 50,
				ThroughMessageID: throughMessageID, PromptIncluded: true,
			},
		},
		Status: corev1alpha1.TaskStatus{Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateReserved, Attempt: 1, PromptID: promptID,
			RequestDigest: testControlDigestForDispatcher("prompt-included-append"), ControllerEpoch: fence.Epoch,
		}},
	}
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{Key: key, RequestDigest: task.Status.Execution.RequestDigest}), fence)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
		ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
		NewState: store.PromptExecutionReserved, OperationID: "reserve-prompt-included-append",
		OperationDigest: testControlDigestForDispatcher("reserve-prompt-included-append"), UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}

	dispatcher := &ACPDispatcher{Store: controlStore, Sessions: continuity, runtimeSessions: make(map[string]ACPRuntimeSessionBinding)}
	session, err := dispatcher.prepareTaskSession(
		ctx, task, fence, harnessv2.ProfileDigest(testControlDigestForDispatcher("profile-prompt-included-append")),
		testControlDigestForDispatcher("mcp-prompt-included-append"), "runtime", "boot",
		acpSessionLineageIdentity{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if session == nil || session.Turn == nil || session.Bootstrap == nil {
		t.Fatalf("prompt-included session preparation = %#v", session)
	}
	if session.Turn.SkipTranscriptAppend || !session.Turn.SkipUserPromptAppend {
		t.Fatalf("prompt-included append policy = %#v", session.Turn)
	}
	if session.UserPrompt != currentPrompt || session.Turn.Turn.UserPrompt != currentPrompt {
		t.Fatalf("prompt binding = %#v, turn=%#v", session, session.Turn.Turn)
	}
	if len(session.Bootstrap.Messages) != 1 || session.Bootstrap.Messages[0].Content != "earlier response" {
		t.Fatalf("prompt-included bootstrap = %#v", session.Bootstrap.Messages)
	}
	recovered, err := dispatcher.recoveredTaskSession(ctx, task, mustPromptAttemptForDispatcherTest(t, controlStore, attempt.ID))
	if err != nil {
		t.Fatal(err)
	}
	if recovered == nil || recovered.Turn == nil || recovered.Turn.SkipTranscriptAppend || !recovered.Turn.SkipUserPromptAppend {
		t.Fatalf("recovered prompt-included append policy = %#v", recovered)
	}

	attempt, err = controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []store.PromptExecutionState{
		store.PromptExecutionPlanned, store.PromptExecutionSubmitting, store.PromptExecutionAccepted,
		store.PromptExecutionRunning, store.PromptExecutionSettling, store.PromptExecutionSucceeded,
	} {
		operationID := "prompt-included-append-" + string(state)
		attempt, err = controlStore.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: state, OperationID: operationID, OperationDigest: testControlDigestForDispatcher(operationID),
			UpdatedAt: attempt.UpdatedAt.Add(time.Second),
		})
		if err != nil {
			t.Fatalf("transition prompt-included attempt to %s: %v", state, err)
		}
	}
	if _, err := continuity.FinalizeAssistantResult(ctx, ACPFinalizeAssistantRequest{
		SessionTurn: *session.Turn, Fence: fence, AssistantResult: assistantResult,
		Projection: acpSessionProjectionForTest("prompt-included-final", "Succeeded"), FinalizedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	transcript, err := controlStore.LoadTranscript(ctx, task.Namespace, control.SessionName, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(transcript) != 3 || transcript[0].Content != "earlier response" || transcript[1].ID != throughMessageID ||
		transcript[2].Role != "assistant" || transcript[2].Content != assistantResult {
		t.Fatalf("prompt-included final transcript = %#v, want existing messages plus assistant only", transcript)
	}
}

func TestRecoveredTaskSessionPreservesLegacyNonAppendTurnPolicy(t *testing.T) {
	ctx := context.Background()
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "legacy-non-append.db"))
	defer closeStore()
	continuity := newACPSessionTestContinuity(t, controlStore, ACPBootstrapLimits{})
	control := ensureACPSessionForTest(t, continuity, fence, "legacy-non-append")
	const (
		taskUID    = "legacy-non-append-task"
		promptID   = "legacy-non-append-prompt"
		userPrompt = "legacy prompt"
	)
	turn, attempt := openACPSessionTurnForTest(
		t, continuity, controlStore, fence, control, taskUID, promptID, userPrompt,
	)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: control.Namespace, Name: "legacy-non-append", UID: types.UID(taskUID)},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: userPrompt,
			SessionRef: &corev1alpha1.SessionReference{Name: control.SessionName, Create: false, Append: false},
		},
		Status: corev1alpha1.TaskStatus{Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateReserved, Attempt: 1, PromptID: promptID,
			RequestDigest: attempt.RequestDigest, ControllerEpoch: fence.Epoch,
		}},
	}
	dispatcher := &ACPDispatcher{Store: controlStore, Sessions: continuity}
	recovered, err := dispatcher.recoveredTaskSession(ctx, task, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if recovered == nil || recovered.Turn == nil || recovered.Turn.Turn.ID != turn.Turn.ID {
		t.Fatalf("recovered legacy session = %#v", recovered)
	}
	if recovered.Turn.SkipTranscriptAppend {
		t.Fatal("legacy non-append turn was migrated to transcript suppression instead of preserving its durable digest")
	}
}

func TestPromptLeaseRenewalRetryable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "cancelled", err: context.Canceled, want: false},
		{name: "transport", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorTransport}, want: true},
		{name: "protocol", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorProtocol, StatusCode: 502}, want: true},
		{name: "http 503 retryable", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorHTTP, StatusCode: 503, Retryable: true}, want: true},
		{name: "http 500", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorHTTP, StatusCode: 500}, want: true},
		{name: "http 410 settled", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorHTTP, StatusCode: 410, Code: harnessv2.ErrorCodeSettled}, want: false},
		{name: "http 409 digest conflict", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorHTTP, StatusCode: 409, Code: harnessv2.ErrorCodeDigestConflict}, want: false},
		{name: "validation", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorValidation}, want: false},
		{name: "plain error", err: errors.New("boom"), want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := promptLeaseRenewalRetryable(tc.err); got != tc.want {
				t.Fatalf("promptLeaseRenewalRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRenewPromptLeaseLoopRetriesTransientFailures(t *testing.T) {
	var calls atomic.Int32
	var digestsMu sync.Mutex
	var digests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		var request harnessv2.RenewPromptLeaseRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode renew: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		digestsMu.Lock()
		digests = append(digests, string(request.Metadata.OperationID)+"|"+string(request.Metadata.RequestDigest))
		digestsMu.Unlock()
		if n == 1 {
			// A busy supervisor answering through a proxy: a non-v2 502 body.
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "<html>bad gateway</html>")
			return
		}
		writeDispatcherJSONStatus(w, http.StatusOK, harnessv2.PromptLeaseResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
			Lease: request.Lease,
		})
	}))
	defer server.Close()
	runtimeClient, err := harnessv2.NewClient(
		server.URL, harnessv2.WithControllerBearerToken(strings.Repeat("t", 32)),
		harnessv2.WithOperationCapabilitySecret([]byte(strings.Repeat("s", 32))),
	)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "renew", UID: types.UID("99999999-9999-9999-9999-999999999999")},
		Status:     corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{Attempt: 1, PromptID: "prompt-renew"}},
	}
	fence := harnessv2.Fence{
		RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot-id", ControllerEpoch: 1,
		RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 1, RuntimeProfileDigest: harnessv2.ProfileDigest(testControlDigestForDispatcher("renew-profile")),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion, RuntimeSessionUID: "session-renew", RuntimeSessionGeneration: 1,
	}
	now := time.Now().UTC()
	lease := harnessv2.PromptLease{Generation: 1, IssuedAt: now, ExpiresAt: now.Add(12 * time.Second)}
	descriptor := harnessv2.MCPToolDescriptor{
		Name: acpMCPTestToolName, Description: "renew test tool", InputSchema: json.RawMessage(`{"type":"object"}`),
		Source: harnessv2.MCPToolSourceBrokeredBuiltin, Effect: harnessv2.MCPToolEffectReadOnly,
	}
	descriptorDigest, err := harnessv2.CanonicalMCPToolDescriptorDigest([]harnessv2.MCPToolDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	toolPolicy := harnessv2.MCPToolPolicy{
		AllowedToolNames: []string{acpMCPTestToolName}, Tools: []harnessv2.MCPToolDescriptor{descriptor}, DescriptorDigest: descriptorDigest,
	}
	approvalPolicy := harnessv2.MCPApprovalPolicy{}
	toolDigest, _ := harnessv2.CanonicalRuntimeToolPolicyDigest(toolPolicy.AllowedToolNames, toolPolicy.DisallowedToolNames, toolPolicy.AllowBash)
	approvalDigest, _ := harnessv2.CanonicalMCPApprovalPolicyDigest(approvalPolicy)
	mcpDigest, _ := harnessv2.CanonicalMCPConfigurationDigest(toolPolicy.AllowedToolNames)
	authorization := harnessv2.PromptMCPAuthorization{
		RuntimeSessionUID: fence.RuntimeSessionUID, SessionGeneration: fence.RuntimeSessionGeneration,
		TaskUID: harnessv2.TaskUID(task.UID), TaskAttempt: 1, PromptID: "prompt-renew",
		LeaseGeneration: lease.Generation, ToolPolicyDigest: toolDigest, ApprovalPolicyDigest: approvalDigest,
		MCPConfigurationDigest: mcpDigest, ToolPolicy: toolPolicy, ApprovalPolicy: approvalPolicy, ExpiresAt: lease.ExpiresAt,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelled := make(chan struct{}, 1)
	cancelRuntime := func() {
		select {
		case cancelled <- struct{}{}:
		default:
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&ACPDispatcher{}).renewPromptLeaseLoop(ctx, cancelRuntime, runtimeClient, "runtime-session-renew-g1", task, fence, lease, authorization)
	}()
	deadline := time.After(20 * time.Second)
	for calls.Load() < 2 {
		select {
		case <-cancelled:
			t.Fatal("runtime context was cancelled after a transient renewal failure while the lease was still valid")
		case <-deadline:
			t.Fatalf("renewal was not retried in time (calls=%d)", calls.Load())
		case <-time.After(100 * time.Millisecond):
		}
	}
	select {
	case <-cancelled:
		t.Fatal("runtime context was cancelled although the retried renewal succeeded")
	case <-time.After(500 * time.Millisecond):
	}
	cancel()
	<-done
	digestsMu.Lock()
	defer digestsMu.Unlock()
	if len(digests) < 2 || digests[0] != digests[1] {
		t.Fatalf("retried renewal was not an identical replay of the sealed mutation: %v", digests)
	}
}

func TestRenewPromptLeaseLoopStopsWithoutCancelWhenPromptSettled(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeDispatcherJSONStatus(w, http.StatusGone, harnessv2.ErrorResponse{
			Protocol: harnessv2.ProtocolVersion, Code: harnessv2.ErrorCodeSettled, Message: "prompt is no longer active",
		})
	}))
	defer server.Close()
	runtimeClient, err := harnessv2.NewClient(
		server.URL, harnessv2.WithControllerBearerToken(strings.Repeat("t", 32)),
		harnessv2.WithOperationCapabilitySecret([]byte(strings.Repeat("s", 32))),
	)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "renew-settled", UID: types.UID("99999999-9999-9999-9999-999999999998")},
		Status:     corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{Attempt: 1, PromptID: "prompt-renew-settled"}},
	}
	fence := harnessv2.Fence{
		RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot-id", ControllerEpoch: 1,
		RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 1, RuntimeProfileDigest: harnessv2.ProfileDigest(testControlDigestForDispatcher("renew-profile")),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion, RuntimeSessionUID: "session-renew-settled", RuntimeSessionGeneration: 1,
	}
	now := time.Now().UTC()
	lease := harnessv2.PromptLease{Generation: 1, IssuedAt: now, ExpiresAt: now.Add(12 * time.Second)}
	descriptor := harnessv2.MCPToolDescriptor{
		Name: acpMCPTestToolName, Description: "renew test tool", InputSchema: json.RawMessage(`{"type":"object"}`),
		Source: harnessv2.MCPToolSourceBrokeredBuiltin, Effect: harnessv2.MCPToolEffectReadOnly,
	}
	descriptorDigest, err := harnessv2.CanonicalMCPToolDescriptorDigest([]harnessv2.MCPToolDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	toolPolicy := harnessv2.MCPToolPolicy{
		AllowedToolNames: []string{acpMCPTestToolName}, Tools: []harnessv2.MCPToolDescriptor{descriptor}, DescriptorDigest: descriptorDigest,
	}
	approvalPolicy := harnessv2.MCPApprovalPolicy{}
	toolDigest, _ := harnessv2.CanonicalRuntimeToolPolicyDigest(toolPolicy.AllowedToolNames, toolPolicy.DisallowedToolNames, toolPolicy.AllowBash)
	approvalDigest, _ := harnessv2.CanonicalMCPApprovalPolicyDigest(approvalPolicy)
	mcpDigest, _ := harnessv2.CanonicalMCPConfigurationDigest(toolPolicy.AllowedToolNames)
	authorization := harnessv2.PromptMCPAuthorization{
		RuntimeSessionUID: fence.RuntimeSessionUID, SessionGeneration: fence.RuntimeSessionGeneration,
		TaskUID: harnessv2.TaskUID(task.UID), TaskAttempt: 1, PromptID: "prompt-renew-settled",
		LeaseGeneration: lease.Generation, ToolPolicyDigest: toolDigest, ApprovalPolicyDigest: approvalDigest,
		MCPConfigurationDigest: mcpDigest, ToolPolicy: toolPolicy, ApprovalPolicy: approvalPolicy, ExpiresAt: lease.ExpiresAt,
	}
	ctx := t.Context()
	cancelled := make(chan struct{}, 1)
	cancelRuntime := func() {
		select {
		case cancelled <- struct{}{}:
		default:
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&ACPDispatcher{}).renewPromptLeaseLoop(ctx, cancelRuntime, runtimeClient, "runtime-session-renew-settled-g1", task, fence, lease, authorization)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("renewal loop did not stop after the supervisor reported the prompt settled")
	}
	select {
	case <-cancelled:
		t.Fatal("runtime context was cancelled although the prompt had already settled; the stream must deliver the terminal event")
	default:
	}
	if calls.Load() != 1 {
		t.Fatalf("renewal calls = %d, want exactly one before stopping", calls.Load())
	}
}
