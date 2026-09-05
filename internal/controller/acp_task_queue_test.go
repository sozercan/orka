package controller

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

const (
	acpTestRuntimeNamespace  = "orka-runtimes"
	acpTestRuntimeInstanceID = "runtime-1"
)

func TestQueueACPRuntimeTaskCreatesPoolAndDurableAttempt(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "task", UID: types.UID("11111111-1111-1111-1111-111111111111"), Generation: 2},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "implement", AgentRuntime: &corev1alpha1.AgentRuntimeSpec{},
			Workspace: &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentRead, GitRepo: "https://github.com/orka-agents/orka.git"},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "agent", UID: types.UID("22222222-2222-2222-2222-222222222222"), Generation: 3},
		Spec: corev1alpha1.AgentSpec{Model: &corev1alpha1.ModelConfig{Name: acpTestModel}, Runtime: &corev1alpha1.AgentCLIRuntime{
			Type:            corev1alpha1.AgentRuntimeCodex,
			ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
		}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).WithObjects(task).Build()
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
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}
	reconciler := &TaskReconciler{
		Client: kubeClient, Scheme: scheme, Recorder: record.NewFakeRecorder(10), DurableControlStore: controlStore,
		ControllerEpochManager: epochs, ACPRuntimeEnabled: true, ACPRuntimeNamespace: acpTestRuntimeNamespace,
		ACPRuntimeImages: ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)},
	}
	bound := bindACPQueueTaskForTest(t, ctx, reconciler, task, agent)
	if _, err := reconciler.queueACPRuntimeTask(ctx, bound, agent); err != nil {
		t.Fatal(err)
	}
	var pools corev1alpha1.RuntimePoolList
	if err := kubeClient.List(ctx, &pools); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 1 || pools.Items[0].Spec.DesiredReplicas != 1 || pools.Items[0].Spec.RuntimeNamespace != acpTestRuntimeNamespace {
		t.Fatalf("unexpected pools: %#v", pools.Items)
	}
	queued := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, queued); err != nil {
		t.Fatal(err)
	}
	if queued.Status.Execution == nil || queued.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued || queued.Status.Execution.RuntimePoolName != pools.Items[0].Name {
		t.Fatalf("unexpected queued status: %#v", queued.Status.Execution)
	}
	if queued.Labels[acpRuntimeTaskPoolLabel] != pools.Items[0].Name {
		t.Fatalf("pool label = %q", queued.Labels[acpRuntimeTaskPoolLabel])
	}
	if _, err := time.Parse(time.RFC3339Nano, queued.Annotations[acpRuntimeQueuedAtAnnotation]); err != nil {
		t.Fatalf("queued-at annotation = %q: %v", queued.Annotations[acpRuntimeQueuedAtAnnotation], err)
	}
	key := queued.Status.Execution
	attemptID, err := (storePromptKey(task, key)).CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlStore.GetPromptAttempt(ctx, attemptID); err != nil {
		t.Fatal(err)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func storePromptKey(task *corev1alpha1.Task, status *corev1alpha1.TaskExecutionStatus) store.PromptAttemptKey {
	return store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: int64(status.Attempt), PromptID: status.PromptID}
}

func TestACPWorkspaceRuntimePoolReusedRequiresLiveInstance(t *testing.T) {
	pool := &corev1alpha1.RuntimePool{
		Spec: corev1alpha1.RuntimePoolSpec{
			ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{Provider: corev1alpha1.WorkspaceProviderAgentSandbox},
		},
	}
	if acpWorkspaceRuntimePoolReused(pool, false) {
		t.Fatal("new workspace RuntimePool reported reuse")
	}
	if acpWorkspaceRuntimePoolReused(pool, true) {
		t.Fatal("preexisting RuntimePool without a live instance reported reuse")
	}
	pool.Status.ActiveInstance = &corev1alpha1.RuntimePoolActiveInstanceStatus{RuntimeInstanceID: acpTestRuntimeInstanceID}
	pool.Status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopped
	if acpWorkspaceRuntimePoolReused(pool, true) {
		t.Fatal("stopped workspace RuntimePool reported reuse")
	}
	pool.Status.Lifecycle = corev1alpha1.RuntimePoolLifecycleServing
	if !acpWorkspaceRuntimePoolReused(pool, true) {
		t.Fatal("serving workspace RuntimePool with a live instance did not report reuse")
	}
}

func TestQueueACPRuntimeTaskRejectsUnsafeRepositoryBeforePoolDemand(t *testing.T) {
	tests := []struct {
		name      string
		workspace *corev1alpha1.WorkspaceConfig
	}{
		{
			name: "source remote helper",
			workspace: &corev1alpha1.WorkspaceConfig{
				Intent:  corev1alpha1.WorkspaceIntentRead,
				GitRepo: "ext::https://github.com/orka-agents/orka.git",
			},
		},
		{
			name: "publication remote helper",
			workspace: &corev1alpha1.WorkspaceConfig{
				Intent:                   corev1alpha1.WorkspaceIntentWrite,
				GitRepo:                  "https://github.com/orka-agents/orka.git",
				PublicationGitRepo:       "ext::https://github.com/sozercan/orka-acp-release-gate.git",
				PublicationCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "publish"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "unsafe-task", UID: types.UID("66666666-6666-6666-6666-666666666666"), Generation: 1},
				Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "inspect", Workspace: test.workspace},
			}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "agent", UID: types.UID("77777777-7777-7777-7777-777777777777"), Generation: 1},
				Spec: corev1alpha1.AgentSpec{Model: &corev1alpha1.ModelConfig{Name: acpTestModel}, Runtime: &corev1alpha1.AgentCLIRuntime{
					Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
				}},
			}
			kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).WithObjects(task).Build()
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
			if _, err := epochs.CurrentFence(ctx); err != nil {
				t.Fatal(err)
			}
			reconciler := &TaskReconciler{
				Client: kubeClient, Scheme: scheme, Recorder: record.NewFakeRecorder(10), DurableControlStore: controlStore,
				ControllerEpochManager: epochs, ACPRuntimeEnabled: true, ACPRuntimeNamespace: acpTestRuntimeNamespace,
				ACPRuntimeImages: ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)},
			}
			bound := bindACPQueueTaskForTest(t, ctx, reconciler, task, agent)
			if _, err := reconciler.queueACPRuntimeTask(ctx, bound, agent); err != nil {
				t.Fatal(err)
			}
			var pools corev1alpha1.RuntimePoolList
			if err := kubeClient.List(ctx, &pools); err != nil {
				t.Fatal(err)
			}
			if len(pools.Items) != 0 {
				t.Fatalf("unsafe queue created RuntimePools: %#v", pools.Items)
			}
			failed := &corev1alpha1.Task{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, failed); err != nil {
				t.Fatal(err)
			}
			if failed.Status.Execution == nil || failed.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
				failed.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("InvalidWorkspace") || failed.Status.Execution.RuntimePoolName != "" ||
				failed.Status.Delivery == nil || failed.Status.Delivery.State != corev1alpha1.TaskDeliveryStateNotRequested ||
				failed.Status.Delivery.Outcome != corev1alpha1.TaskDeliveryOutcomeNotRequested {
				t.Fatalf("unsafe queue status: execution=%#v delivery=%#v", failed.Status.Execution, failed.Status.Delivery)
			}
			ready, err := reconciler.acpTaskDeletionReady(ctx, failed)
			if err != nil || !ready {
				t.Fatalf("preflight-failed Task deletion readiness = %v, err=%v", ready, err)
			}
			cancelEpoch()
			if err := <-epochDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQueueACPRuntimeTaskRejectsSubstrateTemplateInRuntimeNamespaceBeforePoolDemand(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "shared-substrate-namespace", UID: types.UID("88888888-8888-8888-8888-888888888888"), Generation: 1},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "inspect",
			Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
				Enabled: true, Provider: corev1alpha1.WorkspaceProviderSubstrate,
				TemplateRef: &corev1alpha1.WorkspaceTemplateReference{Name: substrateTestBaseTemplateName, Namespace: acpTestRuntimeNamespace},
			}},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Namespace: task.Namespace, Name: "agent", UID: types.UID("99999999-9999-9999-9999-999999999999"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{Model: &corev1alpha1.ModelConfig{Name: acpTestModel}, Runtime: &corev1alpha1.AgentCLIRuntime{
			Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
		}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).WithObjects(task).Build()
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
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}
	reconciler := &TaskReconciler{
		Client: kubeClient, Scheme: scheme, Recorder: record.NewFakeRecorder(10), DurableControlStore: controlStore,
		ControllerEpochManager: epochs, ACPRuntimeEnabled: true, ACPRuntimeNamespace: acpTestRuntimeNamespace,
		ACPWorkspaceDispatchEnabled: true, SubstrateEnabled: true,
		ACPRuntimeImages: ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)},
	}
	bound := bindACPQueueTaskForTest(t, ctx, reconciler, task, agent)
	if _, err := reconciler.queueACPRuntimeTask(ctx, bound, agent); err != nil {
		t.Fatal(err)
	}
	var pools corev1alpha1.RuntimePoolList
	if err := kubeClient.List(ctx, &pools); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 0 {
		t.Fatalf("shared substrate/runtime namespace created RuntimePools: %#v", pools.Items)
	}
	failed := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, failed); err != nil {
		t.Fatal(err)
	}
	if failed.Status.Execution == nil || failed.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
		failed.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("InvalidWorkspace") ||
		failed.Status.Execution.Attempt != 0 || failed.Status.Execution.RuntimePoolName != "" ||
		!strings.Contains(failed.Status.Message, "must differ") {
		t.Fatalf("shared substrate/runtime namespace status = %#v message=%q", failed.Status.Execution, failed.Status.Message)
	}
	attemptID, err := (store.PromptAttemptKey{
		Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1,
		PromptID: "prompt-" + string(task.UID) + "-1",
	}).CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlStore.GetPromptAttempt(ctx, attemptID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("shared substrate/runtime namespace durable attempt error = %v, want not found", err)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestQueueACPRuntimeTaskReportsInvalidWorkspaceWhenReadCredentialDoesNotExist(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "missing-read-credential", UID: types.UID("99999999-9999-9999-9999-999999999999"), Generation: 1},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "inspect",
			Workspace: &corev1alpha1.WorkspaceConfig{
				Intent: corev1alpha1.WorkspaceIntentRead, GitRepo: "https://github.com/orka-agents/orka.git",
				ReadCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "does-not-exist"},
			},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "agent", UID: types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{Model: &corev1alpha1.ModelConfig{Name: acpTestModel}, Runtime: &corev1alpha1.AgentCLIRuntime{
			Type:            corev1alpha1.AgentRuntimeCodex,
			ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
		}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).WithObjects(task).Build()
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
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}
	reconciler := &TaskReconciler{
		Client: kubeClient, Scheme: scheme, Recorder: record.NewFakeRecorder(10), DurableControlStore: controlStore,
		ControllerEpochManager: epochs, ACPRuntimeEnabled: true, ACPRuntimeNamespace: acpTestRuntimeNamespace,
		ACPRuntimeImages: ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)},
	}
	bound := bindACPQueueTaskForTest(t, ctx, reconciler, task, agent)
	if _, err := reconciler.queueACPRuntimeTask(ctx, bound, agent); err != nil {
		t.Fatalf("queueACPRuntimeTask() error = %v, want terminal InvalidWorkspace status", err)
	}
	failed := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, failed); err != nil {
		t.Fatal(err)
	}
	if failed.Status.Phase != corev1alpha1.TaskPhaseFailed || failed.Status.Execution == nil ||
		failed.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
		failed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeFailed ||
		failed.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("InvalidWorkspace") ||
		failed.Status.Execution.Attempt != 0 || failed.Status.Execution.PromptID != "" || failed.Status.Execution.RuntimePoolName != "" ||
		failed.Status.Delivery == nil || failed.Status.Delivery.State != corev1alpha1.TaskDeliveryStateNotRequested ||
		failed.Status.Delivery.Outcome != corev1alpha1.TaskDeliveryOutcomeNotRequested {
		t.Fatalf("missing credential status: %#v", failed.Status)
	}
	if !strings.Contains(failed.Status.Message, "freeze ACP credential bindings") || !strings.Contains(failed.Status.Message, "does-not-exist") {
		t.Fatalf("missing credential message = %q", failed.Status.Message)
	}
	attemptID, err := (store.PromptAttemptKey{
		Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: "prompt-" + string(task.UID) + "-1",
	}).CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlStore.GetPromptAttempt(ctx, attemptID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing credential durable attempt error = %v, want not found", err)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestQueueACPRuntimeTaskReportsInvalidRuntimeProfile(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "invalid-profile-task", UID: types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), Generation: 1},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "inspect",
			Workspace: &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentRead, GitRepo: "https://github.com/orka-agents/orka.git"},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "agent", UID: types.UID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"), Generation: 1},
		Spec:       corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.RuntimePool{}).WithObjects(task).Build()
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
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}
	reconciler := &TaskReconciler{
		Client: kubeClient, Scheme: scheme, Recorder: record.NewFakeRecorder(10), DurableControlStore: controlStore,
		ControllerEpochManager: epochs, ACPRuntimeEnabled: true, ACPRuntimeNamespace: acpTestRuntimeNamespace,
		ACPRuntimeImages: ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)},
	}
	current := configureAgentExecutionBindingTest(t, ctx, reconciler, task)
	if result, err, handled := reconciler.ensureAgentExecutionBinding(ctx, current, agent); err != nil || !handled {
		t.Fatalf("invalid profile binding result=%#v handled=%v err=%v", result, handled, err)
	}
	failed := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, failed); err != nil {
		t.Fatal(err)
	}
	if failed.Status.Execution == nil || failed.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile") {
		t.Fatalf("runtime profile failure status = %#v", failed.Status.Execution)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestValidateACPWorkspacePreflightRejectsUnsafeRepositoriesBeforeDemand(t *testing.T) {
	tests := []struct {
		name      string
		workspace *corev1alpha1.WorkspaceConfig
		want      string
	}{
		{
			name: "read source remote helper",
			workspace: &corev1alpha1.WorkspaceConfig{
				Intent:  corev1alpha1.WorkspaceIntentRead,
				GitRepo: "ext::https://github.com/orka-agents/orka.git",
			},
			want: "credential-free HTTPS",
		},
		{
			name: "read source empty query marker",
			workspace: &corev1alpha1.WorkspaceConfig{
				Intent:  corev1alpha1.WorkspaceIntentRead,
				GitRepo: "https://github.com/orka-agents/orka.git?",
			},
			want: "credential-free HTTPS",
		},
		{
			name: "read source non-default HTTPS port",
			workspace: &corev1alpha1.WorkspaceConfig{
				Intent:  corev1alpha1.WorkspaceIntentRead,
				GitRepo: "https://github.com:8443/orka-agents/orka.git",
			},
			want: errWorkspaceRepositoryHTTPSPort.Error(),
		},
		{
			name: "write publication remote helper",
			workspace: &corev1alpha1.WorkspaceConfig{
				Intent:                   corev1alpha1.WorkspaceIntentWrite,
				GitRepo:                  "https://github.com/orka-agents/orka.git",
				PublicationGitRepo:       "ext::https://github.com/sozercan/orka-acp-release-gate.git",
				PublicationCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "publish"},
			},
			want: "publication repository",
		},
		{
			name: "write publication empty query marker",
			workspace: &corev1alpha1.WorkspaceConfig{
				Intent:                   corev1alpha1.WorkspaceIntentWrite,
				GitRepo:                  "https://github.com/orka-agents/orka.git",
				PublicationGitRepo:       "https://github.com/sozercan/orka-acp-release-gate.git?",
				PublicationCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "publish"},
			},
			want: "publication repository",
		},
		{
			name: "write publication non-default HTTPS port",
			workspace: &corev1alpha1.WorkspaceConfig{
				Intent:                   corev1alpha1.WorkspaceIntentWrite,
				GitRepo:                  "https://github.com/orka-agents/orka.git",
				PublicationGitRepo:       "https://github.com:8443/sozercan/orka-acp-release-gate.git",
				PublicationCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "publish"},
			},
			want: errWorkspaceRepositoryHTTPSPort.Error(),
		},
		{
			name: "traversal subPath",
			workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo: "https://github.com/orka-agents/orka.git",
				SubPath: "../private",
			},
			want: "subPath is invalid",
		},
		{
			name: "absolute subPath",
			workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo: "https://github.com/orka-agents/orka.git",
				SubPath: "/absolute",
			},
			want: "subPath is invalid",
		},
		{
			name: "empty subPath segment",
			workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo: "https://github.com/orka-agents/orka.git",
				SubPath: "a//b",
			},
			want: "subPath is invalid",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Workspace: test.workspace}}
			err := validateACPWorkspacePreflight(task)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateACPWorkspacePreflight() = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateACPWorkspacePreflightRejectsWriteWithoutPublicationCredential(t *testing.T) {
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
		Type:      corev1alpha1.TaskTypeAgent,
		Workspace: &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentWrite, GitRepo: "https://github.com/orka-agents/orka.git"},
	}}
	if err := validateACPWorkspacePreflight(task); err == nil || !strings.Contains(err.Error(), "publicationCredentialRef") {
		t.Fatalf("validateACPWorkspacePreflight() = %v", err)
	}
}

func TestValidateACPWorkspacePreflightRejectsMalformedAllowedPathGlobs(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		wantErr bool
	}{
		{name: "unclosed character class", pattern: "[", wantErr: true},
		{name: "unclosed nested character class", pattern: "src/[abc", wantErr: true},
		{name: "malformed recursive prefix", pattern: "[/**", wantErr: true},
		{name: "malformed nested recursive prefix", pattern: "src/[abc/**", wantErr: true},
		{name: "valid glob", pattern: "src/*.go"},
		{name: "custom recursive prefix", pattern: "src/**"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
				Workspace: &corev1alpha1.WorkspaceConfig{
					Intent:                   corev1alpha1.WorkspaceIntentWrite,
					GitRepo:                  "https://github.com/orka-agents/orka.git",
					PublicationCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "publish"},
					AllowedPaths:             []string{test.pattern},
				},
			}}

			err := validateACPWorkspacePreflight(task)
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "allowedPaths") {
					t.Fatalf("validateACPWorkspacePreflight() = %v, want invalid allowedPaths error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateACPWorkspacePreflight() = %v, want nil", err)
			}
		})
	}
}

func TestValidateACPWorkspacePreflightRejectsRepositorySelectorsWithoutGitRepo(t *testing.T) {
	tests := []struct {
		name      string
		field     string
		configure func(*corev1alpha1.WorkspaceConfig)
	}{
		{name: "branch", field: "branch", configure: func(workspace *corev1alpha1.WorkspaceConfig) { workspace.Branch = defaultACPSourceBranch }},
		{name: "ref", field: "ref", configure: func(workspace *corev1alpha1.WorkspaceConfig) { workspace.Ref = "refs/tags/v1.0.0" }},
		{name: "subPath", field: "subPath", configure: func(workspace *corev1alpha1.WorkspaceConfig) { workspace.SubPath = "cmd/orka" }},
		{name: "sourceRepository", field: "sourceRepository", configure: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.SourceRepository = &corev1alpha1.RepositoryIdentity{Provider: "github", ID: "github.com/orka-agents/orka"}
		}},
		{name: "readCredentialRef", field: "readCredentialRef", configure: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.ReadCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: "source-read"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workspace := &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentRead, GitRepo: "  "}
			test.configure(workspace)
			task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Workspace: workspace}}

			err := validateACPWorkspacePreflight(task)
			want := test.field + " requires gitRepo"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("validateACPWorkspacePreflight() = %v, want %q", err, want)
			}
		})
	}
}

func TestValidateACPWorkspacePreflightRejectsPublicationFieldsOnReadIntent(t *testing.T) {
	tests := []struct {
		name      string
		field     string
		configure func(*corev1alpha1.WorkspaceConfig)
	}{
		{name: "publicationGitRepo", field: "publicationGitRepo", configure: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.PublicationGitRepo = "https://github.com/orka-agents/orka-fork.git"
		}},
		{name: "publicationRepository", field: "publicationRepository", configure: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.PublicationRepository = &corev1alpha1.RepositoryIdentity{Provider: "github", ID: "github.com/orka-agents/orka-fork"}
		}},
		{name: "publicationReadCredentialRef", field: "publicationReadCredentialRef", configure: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.PublicationReadCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: "publication-read"}
		}},
		{name: "publicationCredentialRef", field: "publicationCredentialRef", configure: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.PublicationCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: "publication-write"}
		}},
		{name: "forgeCredentialRef", field: "forgeCredentialRef", configure: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.ForgeCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: "forge"}
		}},
		{name: "pushBranch", field: "pushBranch", configure: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.PushBranch = "orka/task"
		}},
		{name: "prBaseBranch", field: "prBaseBranch", configure: func(workspace *corev1alpha1.WorkspaceConfig) {
			workspace.PRBaseBranch = defaultACPSourceBranch
		}},
		{name: "createPR", field: "createPR", configure: func(workspace *corev1alpha1.WorkspaceConfig) { workspace.CreatePR = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workspace := &corev1alpha1.WorkspaceConfig{
				Intent:  corev1alpha1.WorkspaceIntentRead,
				GitRepo: "https://github.com/orka-agents/orka.git",
			}
			test.configure(workspace)
			task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Workspace: workspace}}

			err := validateACPWorkspacePreflight(task)
			want := test.field + " requires write workspace intent"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("validateACPWorkspacePreflight() = %v, want %q", err, want)
			}
		})
	}
}

func TestValidateACPWorkspacePreflightRejectsInvalidPublicationBranches(t *testing.T) {
	credential := &corev1alpha1.WorkspaceCredentialReference{Name: "publish"}
	forgeCredential := &corev1alpha1.WorkspaceCredentialReference{Name: "forge"}
	base := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
		Type: corev1alpha1.TaskTypeAgent,
		Workspace: &corev1alpha1.WorkspaceConfig{
			Intent: corev1alpha1.WorkspaceIntentWrite, GitRepo: "https://github.com/orka-agents/orka.git",
			PublicationGitRepo: "https://github.com/orka-agents/orka.git", PublicationCredentialRef: credential,
		},
	}}

	invalidPush := base.DeepCopy()
	invalidPush.Spec.Workspace.PushBranch = "bad branch"
	if err := validateACPWorkspacePreflight(invalidPush); err == nil || !strings.Contains(err.Error(), "pushBranch is invalid") {
		t.Fatalf("invalid pushBranch error = %v", err)
	}

	invalidBase := base.DeepCopy()
	invalidBase.Spec.Workspace.CreatePR = true
	invalidBase.Spec.Workspace.PRBaseBranch = "bad branch"
	invalidBase.Spec.Workspace.ForgeCredentialRef = forgeCredential
	if err := validateACPWorkspacePreflight(invalidBase); err == nil || !strings.Contains(err.Error(), "prBaseBranch is invalid") {
		t.Fatalf("invalid prBaseBranch error = %v", err)
	}

	valid := base.DeepCopy()
	valid.Spec.Workspace.PushBranch = "refs/heads/orka/task-valid"
	valid.Spec.Workspace.CreatePR = true
	valid.Spec.Workspace.PRBaseBranch = "release/v2"
	valid.Spec.Workspace.ForgeCredentialRef = forgeCredential
	if err := validateACPWorkspacePreflight(valid); err != nil {
		t.Fatalf("valid publication branches rejected: %v", err)
	}
}

func TestSortACPTasksByQueuePriorityPromotesMaximumWait(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	lowPriority := int32(0)
	highPriority := int32(1000)
	old := queuedTaskForOrderingTest("old-low", "old-low-uid", lowPriority, now.Add(-DefaultACPQueueMaximumWait))
	newer := queuedTaskForOrderingTest("new-high", "new-high-uid", highPriority, now.Add(-time.Second))
	tasks := []*corev1alpha1.Task{newer, old}

	sortACPTasksByQueuePriority(tasks, now)

	if tasks[0] != old {
		t.Fatalf("maximum-wait task was not promoted: first=%s", tasks[0].Name)
	}
}

func TestSortACPTasksByQueuePriorityAgesBeforePromotion(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	agedPriority := int32(500)
	freshPriority := int32(575)
	aged := queuedTaskForOrderingTest("aged", "aged-uid", agedPriority, now.Add(-2*time.Minute))
	fresh := queuedTaskForOrderingTest("fresh", "fresh-uid", freshPriority, now.Add(-time.Second))
	tasks := []*corev1alpha1.Task{fresh, aged}

	sortACPTasksByQueuePriority(tasks, now)

	if tasks[0] != aged {
		t.Fatalf("aged effective priority did not win: first=%s", tasks[0].Name)
	}
}

func TestSortACPTasksByQueuePriorityUsesFIFOForPromotedTasks(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	lowPriority := int32(1)
	highPriority := int32(999)
	oldest := queuedTaskForOrderingTest("oldest", "oldest-uid", lowPriority, now.Add(-7*time.Minute))
	newer := queuedTaskForOrderingTest("newer", "newer-uid", highPriority, now.Add(-6*time.Minute))
	tasks := []*corev1alpha1.Task{newer, oldest}

	sortACPTasksByQueuePriority(tasks, now)

	if tasks[0] != oldest {
		t.Fatalf("promoted FIFO order was not preserved: first=%s", tasks[0].Name)
	}
}

func queuedTaskForOrderingTest(name, uid string, priority int32, queuedAt time.Time) *corev1alpha1.Task {
	transition := metav1.NewTime(queuedAt.Add(time.Minute))
	return &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: name, UID: types.UID(uid), CreationTimestamp: metav1.NewTime(queuedAt.Add(-time.Minute)),
			Annotations: map[string]string{acpRuntimeQueuedAtAnnotation: queuedAt.Format(time.RFC3339Nano)},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Priority: &priority},
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateQueued, LastTransitionTime: &transition,
		}},
	}
}

func TestACPTaskDeletionReadyWaitsForWriteSessionRefRuntimeRetirement(t *testing.T) {
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	reconciler := &TaskReconciler{DurableControlStore: sqlite.NewStore(db, "test")}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "write-session", UID: types.UID("77777777-7777-7777-7777-777777777777")},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent,
			SessionRef: &corev1alpha1.SessionReference{
				Name: "write-session", Create: true, Append: true,
			},
			Workspace: &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentWrite},
		},
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			Attempt: 1, RuntimeInstanceID: "runtime-instance", RuntimeSessionUID: "runtime-session", RuntimeSessionGeneration: 1,
		}},
	}

	ready, err := reconciler.acpTaskDeletionReady(context.Background(), task)
	if err != nil {
		t.Fatalf("acpTaskDeletionReady() error = %v, want cleanup barrier before attempt lookup", err)
	}
	if ready {
		t.Fatal("write SessionRef Task became deletion-ready before exact RuntimeSession cleanup receipt")
	}
}

func TestValidateACPWorkspacePreflightRejectsUnsafeChangePolicies(t *testing.T) {
	credential := &corev1alpha1.WorkspaceCredentialReference{Name: "publish"}
	base := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
		Type: corev1alpha1.TaskTypeAgent,
		Workspace: &corev1alpha1.WorkspaceConfig{
			Intent: corev1alpha1.WorkspaceIntentWrite, GitRepo: "https://github.com/orka-agents/orka.git",
			PublicationGitRepo: "https://github.com/orka-agents/orka.git", PublicationCredentialRef: credential,
		},
	}}

	zero := int32(0)
	base.Spec.Workspace.MaxChangedFiles = &zero
	if err := validateACPWorkspacePreflight(base); err == nil || !strings.Contains(err.Error(), "maxChangedFiles") {
		t.Fatalf("zero maxChangedFiles error = %v", err)
	}

	maxFiles := int32(2)
	base.Spec.Workspace.MaxChangedFiles = &maxFiles
	base.Spec.Workspace.AllowedPaths = []string{"../outside"}
	if err := validateACPWorkspacePreflight(base); err == nil || !strings.Contains(err.Error(), "allowedPaths") {
		t.Fatalf("unsafe allowedPaths error = %v", err)
	}

	base.Spec.Workspace.AllowedPaths = []string{"internal/**"}
	if err := validateACPWorkspacePreflight(base); err != nil {
		t.Fatalf("valid change policy error = %v", err)
	}
}
