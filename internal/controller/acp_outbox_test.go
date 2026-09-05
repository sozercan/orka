package controller

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

func TestACPOutboxProjectorRepairsTerminalTaskStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "task", UID: types.UID("task-uid")},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning, Execution: &corev1alpha1.TaskExecutionStatus{
			Attempt: 1, PromptID: "prompt-1", State: corev1alpha1.TaskExecutionStateSettling,
			RuntimePoolName: "pool", RuntimePoolUID: "pool-uid", RuntimeInstanceID: "runtime-1",
			RuntimeSessionUID: "session-1", RuntimeSessionGeneration: 3,
			RequestDigest: "sha256:" + strings.Repeat("a", 64), ControllerEpoch: 7,
			ReadCredentialResourceVersion: "11", PublicationReadCredentialResourceVersion: "12",
			PublicationCredentialResourceVersion: "13", ForgeCredentialResourceVersion: "14",
		}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(task).Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(taskTerminalProjection{
		Namespace: "default", Task: "task", TaskUID: "task-uid", Attempt: 1, Phase: corev1alpha1.TaskPhaseSucceeded,
		Execution: corev1alpha1.TaskExecutionStatus{Attempt: 1, State: corev1alpha1.TaskExecutionStateSucceeded, Outcome: corev1alpha1.TaskExecutionOutcomeSucceeded},
	})
	projection := &store.OutboxProjection{
		ID: store.CanonicalControlID("outbox", "turn", "TaskTerminalStatus"), AggregateKind: "SessionTurn", AggregateID: "turn",
		ProjectionKind: "TaskTerminalStatus", Payload: payload, PayloadDigest: canonicalACPPayloadDigest(payload), AvailableAt: time.Now().UTC(),
	}
	if _, err := controlStore.EnqueueOutboxProjection(ctx, projection, fence); err != nil {
		t.Fatal(err)
	}
	projector := &ACPOutboxProjector{Client: kubeClient, Store: controlStore, Epochs: epochs, WorkerID: "worker"}
	if err := projector.projectOnce(ctx); err != nil {
		t.Fatal(err)
	}
	updated := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "task"}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseSucceeded || updated.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded {
		t.Fatalf("updated status = %#v", updated.Status)
	}
	gotExecution := updated.Status.Execution
	if gotExecution.RuntimePoolUID != "pool-uid" || gotExecution.RuntimeInstanceID != "runtime-1" ||
		gotExecution.RuntimeSessionUID != "session-1" || gotExecution.RuntimeSessionGeneration != 3 ||
		gotExecution.ControllerEpoch != 7 || gotExecution.RequestDigest == "" ||
		gotExecution.ReadCredentialResourceVersion != "11" || gotExecution.PublicationReadCredentialResourceVersion != "12" ||
		gotExecution.PublicationCredentialResourceVersion != "13" || gotExecution.ForgeCredentialResourceVersion != "14" {
		t.Fatalf("terminal projection erased execution identity: %#v", gotExecution)
	}
	stored, err := controlStore.GetOutboxProjection(ctx, projection.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != store.OutboxProjectionDelivered || stored.DeliveryDigest == "" {
		t.Fatalf("stored projection = %#v", stored)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

//nolint:gocyclo // This regression intentionally exercises status projection and finalizer settlement end to end.
func TestACPNoWorkspaceTerminalProjectionSettlesStatusAndFinalizer(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "no-workspace", UID: types.UID("no-workspace-task-uid"),
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:      corev1alpha1.TaskTypeAgent,
			Workspace: &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentRead},
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseRunning,
			Execution: &corev1alpha1.TaskExecutionStatus{
				Attempt: 1, PromptID: "prompt-1", State: corev1alpha1.TaskExecutionStateSettling,
			},
			Delivery: &corev1alpha1.TaskDeliveryStatus{
				State: corev1alpha1.TaskDeliveryStateNotRequested, Outcome: corev1alpha1.TaskDeliveryOutcomeNotRequested,
			},
		},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}).
		WithObjects(task).
		Build()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	noWorkspaceDelivery := corev1alpha1.TaskDeliveryStatus{
		State: corev1alpha1.TaskDeliveryStateNoChange, Outcome: corev1alpha1.TaskDeliveryOutcomeNoChange,
		StartingSHA: acpNoWorkspaceRevision,
	}
	workspaceTask := task.DeepCopy()
	workspaceTask.Spec.Workspace = &corev1alpha1.WorkspaceConfig{
		Intent: corev1alpha1.WorkspaceIntentRead, GitRepo: "https://github.com/example/repo.git",
	}
	if got := taskDeliveryStatusForKubernetes(workspaceTask, noWorkspaceDelivery); got.StartingSHA != acpNoWorkspaceRevision {
		t.Fatalf("workspace delivery startingSHA = %q, want invalid sentinel preserved for API validation", got.StartingSHA)
	}
	dispatcher := &ACPDispatcher{Client: kubeClient}
	if err := dispatcher.patchDeliveryStatus(ctx, task, noWorkspaceDelivery); err != nil {
		t.Fatalf("patch no-workspace delivery status: %v", err)
	}
	patched := &corev1alpha1.Task{}
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	if err := kubeClient.Get(ctx, key, patched); err != nil {
		t.Fatal(err)
	}
	if patched.Status.Delivery == nil || patched.Status.Delivery.StartingSHA != "" {
		t.Fatalf("pre-terminal delivery status = %#v, want omitted startingSHA", patched.Status.Delivery)
	}
	encodedStatus, err := json.Marshal(patched.Status.Delivery)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encodedStatus), `"startingSHA"`) {
		t.Fatalf("pre-terminal delivery JSON contains unavailable startingSHA: %s", encodedStatus)
	}

	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	defer func() {
		cancelEpoch()
		if err := <-epochDone; err != nil {
			t.Errorf("stop epoch manager: %v", err)
		}
	}()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Bypass any future projection encoding normalization so this remains a
	// regression for rows persisted by the broken controller version.
	type legacyTaskTerminalProjection taskTerminalProjection
	payload, err := json.Marshal(legacyTaskTerminalProjection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: string(task.UID), Attempt: 1,
		Phase: corev1alpha1.TaskPhaseSucceeded,
		Execution: corev1alpha1.TaskExecutionStatus{
			Attempt: 1, PromptID: "prompt-1", State: corev1alpha1.TaskExecutionStateSucceeded,
			Outcome: corev1alpha1.TaskExecutionOutcomeSucceeded,
		},
		Delivery: &noWorkspaceDelivery,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"startingSHA":"empty"`) {
		t.Fatalf("legacy projection payload = %s, want unavailable startingSHA sentinel", payload)
	}
	projection := &store.OutboxProjection{
		ID: standaloneTaskTerminalProjectionID(task, 1), AggregateKind: "Task", AggregateID: string(task.UID),
		ProjectionKind: "TaskTerminalStatus", Payload: payload, PayloadDigest: canonicalACPPayloadDigest(payload),
		AvailableAt: time.Now().UTC(),
	}
	if _, err := controlStore.EnqueueOutboxProjection(ctx, projection, fence); err != nil {
		t.Fatal(err)
	}
	projector := &ACPOutboxProjector{Client: kubeClient, Store: controlStore, Epochs: epochs, WorkerID: "worker"}
	if err := projector.projectOnce(ctx); err != nil {
		t.Fatal(err)
	}

	terminal := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, key, terminal); err != nil {
		t.Fatal(err)
	}
	if terminal.Status.Phase != corev1alpha1.TaskPhaseSucceeded || terminal.Status.Execution == nil ||
		terminal.Status.Execution.State != corev1alpha1.TaskExecutionStateSucceeded || terminal.Status.Delivery == nil ||
		terminal.Status.Delivery.State != corev1alpha1.TaskDeliveryStateNoChange || terminal.Status.Delivery.StartingSHA != "" {
		t.Fatalf("terminal no-workspace Task status = %#v", terminal.Status)
	}
	storedProjection, err := controlStore.GetOutboxProjection(ctx, projection.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedProjection.State != store.OutboxProjectionDelivered {
		t.Fatalf("terminal projection state = %s, want %s", storedProjection.State, store.OutboxProjectionDelivered)
	}

	attemptKey := store.PromptAttemptKey{
		Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: "prompt-1",
	}
	attemptID, err := attemptKey.CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	finalizerStore := &promptAttemptReclaimControlStore{
		attempt: &store.PromptAttempt{
			ID: attemptID, Key: attemptKey, ExecutionState: store.PromptExecutionSucceeded, DeliveryState: store.PromptDeliveryNoChange,
		},
		projection: &store.OutboxProjection{ID: projection.ID, State: store.OutboxProjectionDelivered},
		reclaimed:  1,
	}
	if err := kubeClient.Delete(ctx, terminal); err != nil {
		t.Fatal(err)
	}
	deleting := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, key, deleting); err != nil {
		t.Fatal(err)
	}
	reconciler := &TaskReconciler{
		Client: kubeClient, Scheme: scheme, DurableControlStore: finalizerStore,
		ControllerEpochManager: readyPromptAttemptReclaimEpochManager(), EnforceNamespaceIsolation: true,
	}
	if _, err := reconciler.handleDeletion(ctx, deleting); err != nil {
		t.Fatalf("settle no-workspace Task finalizer: %v", err)
	}
	settled := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, key, settled); err != nil {
		if !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
	} else if controllerutil.ContainsFinalizer(settled, labels.TaskFinalizer) {
		t.Fatalf("Task finalizer remains after terminal projection settlement: %#v", settled.Finalizers)
	}
	if len(finalizerStore.reclaimRequests) != 1 || finalizerStore.reclaimRequests[0].TerminalProjectionID != projection.ID {
		t.Fatalf("finalizer reclamation requests = %#v, want delivered projection %q", finalizerStore.reclaimRequests, projection.ID)
	}
}

func TestACPOutboxProjectorAppliesHarnessV1ResultReference(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	bindingDigest := "sha256:" + strings.Repeat("a", 64)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "task", UID: types.UID("task-uid")},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseRunning,
			AgentExecutionBinding: &corev1alpha1.AgentExecutionBinding{
				ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV1,
				BindingDigest:   bindingDigest,
			},
		},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(task).Build()
	payload, err := json.Marshal(taskTerminalProjection{
		Namespace: "default", Task: "task", TaskUID: "task-uid", Attempt: 1,
		Phase: corev1alpha1.TaskPhaseSucceeded, BindingDigest: bindingDigest,
		HarnessRuntime: &corev1alpha1.HarnessRuntimeStatus{
			Attempt: 1, State: corev1alpha1.TaskExecutionStateSucceeded,
			Outcome: corev1alpha1.TaskExecutionOutcomeSucceeded,
		},
		ResultRef: &corev1alpha1.ResultReference{Available: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	projection := store.OutboxProjection{
		ID: "session-turn-projection", AggregateKind: "SessionTurn", AggregateID: "turn",
		ProjectionKind: "TaskTerminalStatus", Payload: payload, PayloadDigest: canonicalACPPayloadDigest(payload),
	}
	projector := &ACPOutboxProjector{Client: kubeClient}
	if _, err := projector.deliver(context.Background(), projection); err != nil {
		t.Fatal(err)
	}
	updated := &corev1alpha1.Task{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "task"}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.ResultRef == nil || !updated.Status.ResultRef.Available {
		t.Fatalf("Session outbox result reference = %#v, want available", updated.Status.ResultRef)
	}
}

func TestACPOutboxProjectorDeliveryFailureClassification(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	validPayload, err := json.Marshal(taskTerminalProjection{
		Namespace: "default", Task: "task", TaskUID: "task-uid", Attempt: 1, Phase: corev1alpha1.TaskPhaseSucceeded,
		Execution: corev1alpha1.TaskExecutionStatus{Attempt: 1, State: corev1alpha1.TaskExecutionStateSucceeded, Outcome: corev1alpha1.TaskExecutionOutcomeSucceeded},
	})
	if err != nil {
		t.Fatal(err)
	}
	corruptPayload := []byte(`{}`)
	cases := []struct {
		name      string
		payload   []byte
		kubeError error
		objects   []client.Object
		wantState store.OutboxProjectionState
	}{
		{
			// A Kubernetes control-plane outage must never dead-letter the
			// terminal projection: recovery accepts any existing projection and
			// Task deletion requires Delivered, so the projection has to stay
			// retryable until the API server recovers.
			name: "infrastructure-failure-stays-pending", payload: validPayload,
			kubeError: apierrors.NewInternalError(context.DeadlineExceeded), wantState: store.OutboxProjectionPending,
		},
		{
			name: "permanent-payload-failure-dead-letters", payload: corruptPayload,
			wantState: store.OutboxProjectionDeadLetter,
		},
		{
			// A Task name recreated with a different UID (and no restored
			// source identity) can never satisfy this projection: after
			// MaxAttempts it must dead-letter rather than being claimed and
			// retried forever.
			name: "identity-mismatch-dead-letters", payload: validPayload,
			objects: []client.Object{&corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
				Namespace: "default", Name: "task", UID: "recreated-uid",
			}}},
			wantState: store.OutboxProjectionDeadLetter,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{})
			if len(testCase.objects) > 0 {
				builder = builder.WithObjects(testCase.objects...)
			}
			if testCase.kubeError != nil {
				builder = builder.WithInterceptorFuncs(interceptor.Funcs{
					Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
						return testCase.kubeError
					},
				})
			}
			kubeClient := builder.Build()
			db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "store.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close() //nolint:errcheck
			controlStore := sqlite.NewStore(db, "test")
			epochs := NewControllerEpochManager(controlStore, "controller")
			epochCtx, cancelEpoch := context.WithCancel(context.Background())
			epochDone := make(chan error, 1)
			go func() { epochDone <- epochs.Start(epochCtx) }()
			defer func() {
				cancelEpoch()
				if err := <-epochDone; err != nil {
					t.Error(err)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			fence, err := epochs.CurrentFence(ctx)
			if err != nil {
				t.Fatal(err)
			}
			projection := &store.OutboxProjection{
				ID: store.CanonicalControlID("outbox", "turn-"+testCase.name, "TaskTerminalStatus"), AggregateKind: "Task", AggregateID: "task-uid",
				ProjectionKind: "TaskTerminalStatus", Payload: testCase.payload, PayloadDigest: canonicalACPPayloadDigest(testCase.payload), AvailableAt: time.Now().UTC(),
			}
			if _, err := controlStore.EnqueueOutboxProjection(ctx, projection, fence); err != nil {
				t.Fatal(err)
			}
			projector := &ACPOutboxProjector{Client: kubeClient, Store: controlStore, Epochs: epochs, WorkerID: "worker", MaxAttempts: 1}
			if err := projector.projectOnce(ctx); err != nil {
				t.Fatal(err)
			}
			stored, err := controlStore.GetOutboxProjection(ctx, projection.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.State != testCase.wantState {
				t.Fatalf("projection state after exhausted attempts = %s, want %s (lastError %q)", stored.State, testCase.wantState, stored.LastError)
			}
			if stored.LastError == "" {
				t.Fatalf("projection last error should be recorded")
			}
		})
	}
}
