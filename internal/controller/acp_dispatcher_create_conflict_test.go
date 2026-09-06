package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

func TestRuntimeSessionCreateDigestConflict(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "digest conflict", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorHTTP, StatusCode: http.StatusConflict, Code: harnessv2.ErrorCodeDigestConflict}, want: true},
		{name: "duplicate conflict", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorHTTP, StatusCode: http.StatusConflict, Code: harnessv2.ErrorCodeInvalidRequest}, want: false},
		{name: "protocol digest conflict", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorProtocol, StatusCode: http.StatusConflict, Code: harnessv2.ErrorCodeDigestConflict}, want: false},
		{name: "stale fence", err: &harnessv2.ClientError{Kind: harnessv2.ClientErrorHTTP, StatusCode: http.StatusGone, Code: harnessv2.ErrorCodeStaleFence}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := runtimeSessionCreateDigestConflict(tc.err); got != tc.want {
				t.Fatalf("runtimeSessionCreateDigestConflict() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReconcileRuntimeSessionCreateDigestConflict(t *testing.T) {
	digest := harnessv2.ProfileDigest("sha256:" + strings.Repeat("d", 64))
	const sessionUID = harnessv2.RuntimeSessionUID("session-adopt")
	sessionID := harnessv2.RuntimeSessionID(runtimeSessionID(harnessv2.Fence{RuntimeSessionUID: sessionUID, RuntimeSessionGeneration: 2}))
	newClient := func(t *testing.T, descriptors ...harnessv2.RuntimeSessionDescriptor) *harnessv2.Client {
		t.Helper()
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != harnessv2.StatusPath {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			index := min(int(calls.Add(1))-1, len(descriptors)-1)
			writeDispatcherJSON(w, dispatcherRuntimeStatusResponse(digest, descriptors[index]))
		}))
		t.Cleanup(server.Close)
		runtimeClient, err := harnessv2.NewClient(
			server.URL, harnessv2.WithControllerBearerToken(strings.Repeat("t", 32)),
			harnessv2.WithOperationCapabilitySecret([]byte(strings.Repeat("s", 32))),
			harnessv2.WithStatusCapabilityBinding(harnessv2.StatusCapabilityBinding{RuntimeProfileDigest: digest, RuntimeInstanceID: "pod-uid.boot-id"}),
		)
		if err != nil {
			t.Fatal(err)
		}
		return runtimeClient
	}
	descriptor := func(state harnessv2.RuntimeSessionState, generation uint64) harnessv2.RuntimeSessionDescriptor {
		return harnessv2.RuntimeSessionDescriptor{
			RuntimeSessionID:  harnessv2.RuntimeSessionID(runtimeSessionID(harnessv2.Fence{RuntimeSessionUID: sessionUID, RuntimeSessionGeneration: generation})),
			RuntimeSessionUID: sessionUID, Generation: generation, State: state, LastTransitionAt: time.Now().UTC(),
		}
	}
	ctx := context.Background()

	t.Run("adopts an idle session of the exact generation", func(t *testing.T) {
		adopted, err := reconcileRuntimeSessionCreateDigestConflict(ctx, newClient(t, descriptor(harnessv2.RuntimeSessionStateIdle, 2)), sessionID, sessionUID, 2, 5*time.Second)
		if err != nil || !adopted {
			t.Fatalf("adopted=%v err=%v, want adoption", adopted, err)
		}
	})
	t.Run("waits while the earlier send is still creating", func(t *testing.T) {
		adopted, err := reconcileRuntimeSessionCreateDigestConflict(ctx, newClient(t,
			descriptor(harnessv2.RuntimeSessionStateCreating, 2), descriptor(harnessv2.RuntimeSessionStateCreating, 2), descriptor(harnessv2.RuntimeSessionStateIdle, 2),
		), sessionID, sessionUID, 2, 5*time.Second)
		if err != nil || !adopted {
			t.Fatalf("adopted=%v err=%v, want adoption after creation settles", adopted, err)
		}
	})
	t.Run("reports a tombstoned generation absent without polling", func(t *testing.T) {
		started := time.Now()
		adopted, err := reconcileRuntimeSessionCreateDigestConflict(ctx, newClient(t, harnessv2.RuntimeSessionDescriptor{}), sessionID, sessionUID, 2, 5*time.Second)
		if err != nil || adopted {
			t.Fatalf("adopted=%v err=%v, want absent without error", adopted, err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("absent session was polled for %s instead of answering immediately", elapsed)
		}
	})
	t.Run("rejects a different generation under the same session UID", func(t *testing.T) {
		adopted, err := reconcileRuntimeSessionCreateDigestConflict(ctx, newClient(t, descriptor(harnessv2.RuntimeSessionStateIdle, 3)), sessionID, sessionUID, 2, 5*time.Second)
		if adopted || err == nil {
			t.Fatalf("adopted=%v err=%v, want generation conflict", adopted, err)
		}
	})
	t.Run("reports a session still creating at window close as inconclusive", func(t *testing.T) {
		adopted, err := reconcileRuntimeSessionCreateDigestConflict(ctx, newClient(t, descriptor(harnessv2.RuntimeSessionStateCreating, 2)), sessionID, sessionUID, 2, 600*time.Millisecond)
		if adopted || !errors.Is(err, errRuntimeSessionAdoptionInconclusive) {
			t.Fatalf("adopted=%v err=%v, want inconclusive", adopted, err)
		}
	})
	t.Run("uses the remaining creation budget", func(t *testing.T) {
		creating := descriptor(harnessv2.RuntimeSessionStateCreating, 2)
		creating.LastTransitionAt = time.Now().UTC().Add(-4500 * time.Millisecond)
		started := time.Now()
		adopted, err := reconcileRuntimeSessionCreateDigestConflict(ctx, newClient(t, creating), sessionID, sessionUID, 2, 5*time.Second)
		if adopted || !errors.Is(err, errRuntimeSessionAdoptionInconclusive) {
			t.Fatalf("adopted=%v err=%v, want inconclusive", adopted, err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("adoption waited %s, want only the remaining creation budget", elapsed)
		}
	})
	t.Run("does not wait after the creation budget expires", func(t *testing.T) {
		creating := descriptor(harnessv2.RuntimeSessionStateCreating, 2)
		creating.LastTransitionAt = time.Now().UTC().Add(-10 * time.Second)
		started := time.Now()
		adopted, err := reconcileRuntimeSessionCreateDigestConflict(ctx, newClient(t, creating), sessionID, sessionUID, 2, 5*time.Second)
		if adopted || !errors.Is(err, errRuntimeSessionAdoptionInconclusive) {
			t.Fatalf("adopted=%v err=%v, want inconclusive", adopted, err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("adoption waited %s after the creation budget expired", elapsed)
		}
	})
	t.Run("reports unavailable status as inconclusive instead of absent", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		t.Cleanup(server.Close)
		runtimeClient, err := harnessv2.NewClient(
			server.URL, harnessv2.WithControllerBearerToken(strings.Repeat("t", 32)),
			harnessv2.WithOperationCapabilitySecret([]byte(strings.Repeat("s", 32))),
			harnessv2.WithStatusCapabilityBinding(harnessv2.StatusCapabilityBinding{RuntimeProfileDigest: digest, RuntimeInstanceID: "pod-uid.boot-id"}),
		)
		if err != nil {
			t.Fatal(err)
		}
		adopted, err := reconcileRuntimeSessionCreateDigestConflict(ctx, runtimeClient, sessionID, sessionUID, 2, 600*time.Millisecond)
		if adopted || !errors.Is(err, errRuntimeSessionAdoptionInconclusive) {
			t.Fatalf("adopted=%v err=%v, want inconclusive", adopted, err)
		}
	})
	t.Run("rejects a session settled in a non-admissible state", func(t *testing.T) {
		adopted, err := reconcileRuntimeSessionCreateDigestConflict(ctx, newClient(t, descriptor(harnessv2.RuntimeSessionStatePoisoned, 2)), sessionID, sessionUID, 2, 5*time.Second)
		if adopted || err == nil {
			t.Fatalf("adopted=%v err=%v, want non-admissible conflict", adopted, err)
		}
	})
}

type taskScopedCreateConflictFixture struct {
	kubeClient client.Client
	task       *corev1alpha1.Task
	attemptID  string
	dispatcher *ACPDispatcher
	stop       func()
}

// newTaskScopedCreateConflictFixture prepares a bound task-scoped agent Task
// reserved against one Serving RuntimePool whose supervisor endpoint is the
// given fake runtime server. Runtime server construction is deferred to the
// caller through serverFactory so its hooks can observe the Kubernetes client.
func newTaskScopedCreateConflictFixture(
	t *testing.T,
	ctx context.Context,
	name string,
	taskUID types.UID,
	serverFactory func(profile harnessv2.RuntimeProfile, digest harnessv2.ProfileDigest, kubeClient *client.Client) *httptest.Server,
) *taskScopedCreateConflictFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	promptID := "prompt-" + string(taskUID) + "-1"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, UID: taskUID, Labels: map[string]string{acpRuntimeTaskPoolLabel: "pool"}},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "review the slice", AgentRuntime: &corev1alpha1.AgentRuntimeSpec{},
			AgentRef: &corev1alpha1.AgentReference{Name: "agent"},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending, Attempts: 1, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateQueued, Attempt: 1, PromptID: promptID, RuntimePoolName: "pool", RuntimePoolUID: "pool-uid",
			RequestDigest: testControlDigestForDispatcher(name + "-request"), ControllerEpoch: 1,
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
	task.Labels[acpRuntimeTaskPoolLabel] = plan.PoolName
	task.Status.Execution.RuntimePoolName = plan.PoolName
	var kubeClient client.Client
	server := serverFactory(plan.Profile, plan.Digest, &kubeClient)
	t.Cleanup(server.Close)
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
				ProtocolVersion: corev1alpha1.RuntimePoolProtocolHarnessV2, ProfileDigest: string(plan.Digest),
				ProfileDigestSchemaVersion: strconv.FormatUint(uint64(harnessv2.ProfileDigestSchemaVersion), 10),
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "orka-runtimes", Name: "pool-auth-e1", Labels: map[string]string{
			runtimePoolAuthLabel: "true", runtimePoolUIDLabel: string(pool.UID),
		}},
		Data: map[string][]byte{runtimePoolControllerTokenKey: []byte(strings.Repeat("t", 32)), runtimePoolCapabilitySecretKey: []byte(strings.Repeat("s", 32))},
	}
	kubeClient = fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).
		WithObjects(task, pool, secret, agent).Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), name+".db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "controller-"+name)
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	task = prepareBoundACPDispatcherTaskForTest(t, ctx, kubeClient, scheme, controlStore, task, agent, images)
	key := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attemptID, err := key.CanonicalID()
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if _, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		ID: attemptID, Key: key, RequestDigest: task.Status.Execution.RequestDigest,
		BindingDigest: task.Status.AgentExecutionBinding.BindingDigest, SnapshotDigest: task.Status.AgentExecutionBinding.Snapshot.Digest,
		ExecutionState: store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
	}), fence); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	dispatcher := &ACPDispatcher{
		Client: kubeClient, APIReader: kubeClient, Store: controlStore, ResultStore: controlStore, Epochs: epochs,
		Snapshots: controlStore,
	}
	return &taskScopedCreateConflictFixture{
		kubeClient: kubeClient, task: task, attemptID: attemptID, dispatcher: dispatcher,
		stop: func() {
			cancelEpoch()
			if err := <-epochDone; err != nil {
				t.Error(err)
			}
		},
	}
}

func (f *taskScopedCreateConflictFixture) currentTask(t *testing.T, ctx context.Context) *corev1alpha1.Task {
	t.Helper()
	current := &corev1alpha1.Task{}
	if err := f.kubeClient.Get(ctx, client.ObjectKeyFromObject(f.task), current); err != nil {
		t.Fatal(err)
	}
	return current
}

func digestConflictErrorResponse() *harnessv2.ErrorResponse {
	return &harnessv2.ErrorResponse{
		Protocol: harnessv2.ProtocolVersion, Code: harnessv2.ErrorCodeDigestConflict, Message: string(harnessv2.RequestClassificationDigestConflict),
		Classification: &harnessv2.Classification{Class: harnessv2.RequestClassificationDigestConflict, Phase: harnessv2.OperationPhaseApplied},
	}
}

// A create request rebuilt for the same attempt carries a fresh expiry and
// workspace capability, so a supervisor that already created the session from
// an earlier send answers digest_conflict while still hosting it. The attempt
// must adopt that session instead of failing.
func TestACPDispatcherAdoptsTaskScopedRuntimeSessionAfterCreateDigestConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var createCalls, deleteCalls atomic.Int32
	var createdGeneration atomic.Uint64
	fixture := newTaskScopedCreateConflictFixture(t, ctx, "adopt-after-conflict", types.UID("77777777-7777-7777-7777-777777777777"),
		func(profile harnessv2.RuntimeProfile, digest harnessv2.ProfileDigest, _ *client.Client) *httptest.Server {
			return newDispatcherRuntimeServerWithOptions(t, profile, digest, dispatcherRuntimeServerOptions{
				rejectCreate: func(request harnessv2.CreateRuntimeSessionRequest) (int, *harnessv2.ErrorResponse, bool) {
					createCalls.Add(1)
					createdGeneration.Store(request.Metadata.Fence.RuntimeSessionGeneration)
					return http.StatusConflict, digestConflictErrorResponse(), true
				},
				onDelete: func(harnessv2.DeleteRuntimeSessionRequest) { deleteCalls.Add(1) },
			})
		})
	defer fixture.stop()

	dispatchQueuedTask(ctx, t, fixture.dispatcher, fixture.task.DeepCopy())

	completed := fixture.currentTask(t, ctx)
	if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded || completed.Status.Execution == nil ||
		completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded {
		t.Fatalf("task did not succeed on the adopted RuntimeSession: %#v", completed.Status)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("CreateRuntimeSession calls = %d, want 1", got)
	}
	if generation := createdGeneration.Load(); generation == 0 ||
		completed.Status.Execution.RuntimeSessionGeneration != int64(generation) {
		t.Fatalf("adopted RuntimeSession generation = %d, created generation = %d",
			completed.Status.Execution.RuntimeSessionGeneration, generation)
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("task-scoped RuntimeSession DELETE calls = %d, want 1 terminal retirement of the adopted session", got)
	}
	if completed.Status.Execution.RuntimeSessionCleanupDigest == "" {
		t.Fatalf("terminal retirement was not recorded: %#v", completed.Status.Execution)
	}
	attempt, err := fixture.dispatcher.Store.GetPromptAttempt(ctx, fixture.attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionSucceeded {
		t.Fatalf("attempt execution state = %s, want %s", attempt.ExecutionState, store.PromptExecutionSucceeded)
	}
}

// Losing the RuntimePool capacity reservation after the RuntimeSession was
// created retires that session at the runtime and re-admits the same attempt.
// The runtime keeps a deletion tombstone for the retired generation, so the
// re-admitted attempt must use a later PromptAttempt-backed generation instead
// of rebuilding the tombstoned create identity.
func TestACPDispatcherReadmittedTaskScopedAttemptAdvancesPromptAttemptGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	recorder := &readmissionRuntimeRecorder{t: t, ctx: ctx}
	fixture := newTaskScopedCreateConflictFixture(t, ctx, "readmitted-generation", types.UID("88888888-8888-8888-8888-888888888888"), recorder.server)
	defer fixture.stop()

	// Cycle 1: the session is created, the reservation is lost, the session is
	// retired, and the attempt is re-admitted without a terminal failure.
	reserved, target, err := fixture.dispatcher.reserveTask(ctx, fixture.task.DeepCopy())
	if err != nil {
		t.Fatal(err)
	}
	if reserved == nil {
		t.Fatal("task was not reserved for the first dispatch")
	}
	if err := fixture.dispatcher.executeReservedTask(ctx, reserved, target); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if !recorder.reservationDropped.Load() {
		t.Fatal("test hook did not drop the RuntimePool reservation during the create")
	}
	requeued := fixture.currentTask(t, ctx)
	if requeued.Status.Execution == nil || requeued.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved ||
		requeued.Status.Execution.Reason != corev1alpha1.TaskExecutionReasonAtCapacity {
		t.Fatalf("attempt was not re-admitted after the reservation loss: %#v", requeued.Status)
	}
	if got := recorder.deleteCalls.Load(); got != 1 {
		t.Fatalf("RuntimeSession DELETE calls after capacity loss = %d, want 1", got)
	}
	attempt, err := fixture.dispatcher.Store.GetPromptAttempt(ctx, fixture.attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionReserved {
		t.Fatalf("attempt state after re-admission = %s, want Reserved", attempt.ExecutionState)
	}
	firstGenerations, _ := recorder.creates()
	if len(firstGenerations) != 1 {
		t.Fatalf("CreateRuntimeSession generations after first dispatch = %v, want one generation", firstGenerations)
	}
	firstGeneration := firstGenerations[0]

	// Cycle 2: the re-admitted attempt creates a later generation and completes.
	dispatchQueuedTask(ctx, t, fixture.dispatcher, requeued)
	completed := fixture.currentTask(t, ctx)
	if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded || completed.Status.Execution == nil ||
		completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded {
		t.Fatalf("re-admitted task did not succeed: %#v", completed.Status)
	}
	generations, sessionIDs := recorder.creates()
	if len(generations) != 2 || generations[0] != firstGeneration || generations[1] <= firstGeneration {
		t.Fatalf("CreateRuntimeSession generations = %v, want the second generation after %d", generations, firstGeneration)
	}
	secondGeneration := generations[1]
	wantSessionUID := harnessv2.RuntimeSessionUID(taskRuntimeSessionUID(fixture.task))
	if sessionIDs[1] != harnessv2.RuntimeSessionID(runtimeSessionID(harnessv2.Fence{RuntimeSessionUID: wantSessionUID, RuntimeSessionGeneration: secondGeneration})) {
		t.Fatalf("second create RuntimeSession ID = %s, want generation %d of %s", sessionIDs[1], secondGeneration, wantSessionUID)
	}
	if completed.Status.Execution.RuntimeSessionGeneration != int64(secondGeneration) {
		t.Fatalf("completed execution generation = %d, want %d: %#v",
			completed.Status.Execution.RuntimeSessionGeneration, secondGeneration, completed.Status.Execution)
	}
	if got := recorder.deleteCalls.Load(); got != 2 {
		t.Fatalf("RuntimeSession DELETE calls = %d, want 2 (capacity loss and terminal retirement)", got)
	}
	attempt, err = fixture.dispatcher.Store.GetPromptAttempt(ctx, fixture.attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionSucceeded || attempt.SessionLeaseGeneration != int64(secondGeneration) {
		t.Fatalf("attempt = state %s generation %d, want Succeeded at generation %d",
			attempt.ExecutionState, attempt.SessionLeaseGeneration, secondGeneration)
	}
}

// readmissionRuntimeRecorder drives the fake runtime for the re-admission
// scenario: the RuntimePool reservation vanishes while the first create is in
// flight and records each generation selected after re-admission.
type readmissionRuntimeRecorder struct {
	t   *testing.T
	ctx context.Context

	mu                 sync.Mutex
	createdGenerations []uint64
	createdSessionIDs  []harnessv2.RuntimeSessionID
	deleteCalls        atomic.Int32
	reservationDropped atomic.Bool
}

func (r *readmissionRuntimeRecorder) creates() ([]uint64, []harnessv2.RuntimeSessionID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.createdGenerations...), append([]harnessv2.RuntimeSessionID(nil), r.createdSessionIDs...)
}

func (r *readmissionRuntimeRecorder) recordCreate(request harnessv2.CreateRuntimeSessionRequest) (int, *harnessv2.ErrorResponse, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.createdGenerations = append(r.createdGenerations, request.Metadata.Fence.RuntimeSessionGeneration)
	r.createdSessionIDs = append(r.createdSessionIDs, request.RuntimeSessionID)
	return 0, nil, false
}

func (r *readmissionRuntimeRecorder) server(profile harnessv2.RuntimeProfile, digest harnessv2.ProfileDigest, kubeClient *client.Client) *httptest.Server {
	return newDispatcherRuntimeServerWithOptions(r.t, profile, digest, dispatcherRuntimeServerOptions{
		rejectCreate: r.recordCreate,
		onDelete:     func(harnessv2.DeleteRuntimeSessionRequest) { r.deleteCalls.Add(1) },
	}, func(request harnessv2.CreateRuntimeSessionRequest) {
		if !r.reservationDropped.CompareAndSwap(false, true) {
			return
		}
		// The reservation vanishes while the create is in flight, as it does
		// when the pool's active instance or lifecycle changes underneath it.
		var pools corev1alpha1.RuntimePoolList
		if err := (*kubeClient).List(r.ctx, &pools); err != nil || len(pools.Items) != 1 {
			r.t.Errorf("list RuntimePools: %v (items=%d)", err, len(pools.Items))
			return
		}
		pool := &pools.Items[0]
		pool.Status.Capacity.Reservations = nil
		updateRuntimePoolReservationCounters(&pool.Status.Capacity)
		if err := (*kubeClient).Status().Update(r.ctx, pool); err != nil {
			r.t.Errorf("drop RuntimePool reservation: %v", err)
		}
	})
}
