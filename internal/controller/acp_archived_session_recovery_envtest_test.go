package controller

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	storekube "github.com/orka-agents/orka/internal/store/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestACPDispatcherArchivedSessionRestartWithAPIServer(t *testing.T) {
	apiClient := newArchivedSessionAPIClient(t)

	// Obtain real Kubernetes Task UIDs before exercising the existing runtime
	// fixture. Its public dispatch/finalization/deletion paths produce receipts
	// for those exact UIDs; only the runtime and control API transport is fake.
	fixture := newExternalACPDispatchFixture(t)
	tasks := make([]*corev1alpha1.Task, 0, 2)
	for i := range 2 {
		name := fmt.Sprintf("archive-restart-turn-%d", i+1)
		sessionRef := &corev1alpha1.SessionReference{Name: "cleanup-conversation", Create: i == 0, Append: true}
		actual := &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Namespace: defaultNS, Name: name},
			Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: fixture.agent.Name},
				Prompt: name, SessionRef: sessionRef, AgentRuntime: &corev1alpha1.AgentRuntimeSpec{},
			},
		}
		if err := apiClient.Create(fixture.ctx, actual); err != nil {
			t.Fatal(err)
		}
		queued := fixture.queueTask(t, name, actual.UID, name, sessionRef)
		completed := fixture.dispatch(t, queued)
		if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
			t.Fatal("runtime fixture did not complete its continued Session turn")
		}
		tasks = append(tasks, actual)
	}
	projector := &ACPOutboxProjector{Client: fixture.client, Store: fixture.controlStore, Epochs: fixture.epochs, WorkerID: "archive-api-test"}
	if err := projector.projectOnce(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	cleanup := sessionRuntimeCleanupStore(t, fixture, fixture.dispatcher.CleanupSessionRuntime)
	manager := NewSessionManager(fixture.persistence)
	manager.SetACPSessionCleanup(cleanup, fixture.epochs)
	if err := manager.DeleteSession(fixture.ctx, defaultNS, "cleanup-conversation"); err != nil {
		t.Fatal(err)
	}
	assertSessionRuntimeCleanupCompleted(t, fixture, cleanup, tasks)

	// Restart with the real API server as the authoritative Task/attempt store.
	// No deleted Session control or outbox object is imported or reconstructed.
	for _, actual := range tasks {
		completed := &corev1alpha1.Task{}
		if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(actual), completed); err != nil {
			t.Fatal(err)
		}
		actual.Status = *completed.Status.DeepCopy()
		if err := apiClient.Status().Update(fixture.ctx, actual); err != nil {
			t.Fatalf("persist completed Task through real status validation: %v", err)
		}
	}
	var attempts corev1alpha1.PromptAttemptList
	if err := fixture.client.List(fixture.ctx, &attempts); err != nil {
		t.Fatal(err)
	}
	for _, original := range attempts.Items {
		attempt := &corev1alpha1.PromptAttempt{
			ObjectMeta: metav1.ObjectMeta{Namespace: original.Namespace, Name: original.Name, Labels: original.Labels},
			Spec:       *original.Spec.DeepCopy(),
		}
		if err := apiClient.Create(fixture.ctx, attempt); err != nil {
			t.Fatal(err)
		}
		attempt.Status = *original.Status.DeepCopy()
		if err := apiClient.Status().Update(fixture.ctx, attempt); err != nil {
			t.Fatal(err)
		}
	}
	control, err := storekube.NewComposite(apiClient, defaultNS, fixture.persistence, storekube.WithAPIReader(apiClient))
	if err != nil {
		t.Fatal(err)
	}
	restarted := &externalACPDispatchFixture{ctx: fixture.ctx, client: apiClient, controlStore: control, persistence: fixture.persistence}
	before := captureArchivedSessionEvidence(t, restarted, control, tasks)
	_, stopInitial := startArchivedRecoveryEpoch(t, fixture.ctx, control, nil, "archive-api-initial")
	stopInitial()
	blockedID, err := promptAttemptIDFromTask(tasks[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, epoch := range []int64{2, 3, 4, 5} {
		epochs, stop := startArchivedRecoveryEpoch(t, fixture.ctx, control, fixture.persistence, fmt.Sprintf("archive-api-%d", epoch))
		var recovering store.DurableControlStore = control
		if epoch == 3 || epoch == 4 {
			var missingOrCorrupt = store.ErrNotFound
			if epoch == 4 {
				missingOrCorrupt = store.ConflictErrorf("archived receipt digest mismatch")
			}
			recovering = &archivedSessionRecoveryStore{DurableControlStore: control, receipts: control, attemptID: blockedID, err: missingOrCorrupt}
		}
		dispatcher := archivedSessionRecoveryDispatcher(restarted, recovering, epoch)
		dispatcher.Epochs = epochs
		runCtx, cancel := context.WithTimeout(fixture.ctx, 400*time.Millisecond)
		err := dispatcher.Start(runCtx)
		cancel()
		stop()
		if err != nil {
			t.Fatalf("real API dispatcher restart at epoch %d: %v", epoch, err)
		}
		for i, task := range tasks {
			current := &corev1alpha1.Task{}
			if err := apiClient.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
				t.Fatal(err)
			}
			wantEpoch := epoch
			if i == 0 && (epoch == 3 || epoch == 4) {
				wantEpoch = 2
			}
			if current.Status.Execution.ControllerEpoch != wantEpoch {
				t.Fatalf("Task %s recovered epoch = %d, want %d", task.Name, current.Status.Execution.ControllerEpoch, wantEpoch)
			}
		}
		if !reflect.DeepEqual(before, captureArchivedSessionEvidence(t, restarted, control, tasks)) {
			t.Fatal("real API restart changed immutable evidence or terminal Task status beyond its epoch")
		}
	}
	if fixture.createCalls.Load() != 1 || fixture.deleteCalls.Load() != 1 {
		t.Fatal("real API restart replayed runtime work")
	}
}

func newArchivedSessionAPIClient(t *testing.T) client.Client {
	t.Helper()
	environment := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true, BinaryAssetsDirectory: getFirstFoundEnvTestBinaryDir(),
	}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Errorf("stop archive recovery API server: %v", err)
		}
	})
	config.QPS, config.Burst = 200, 200
	apiClient, err := client.New(config, client.Options{Scheme: newTestScheme()})
	if err != nil {
		t.Fatal(err)
	}
	return apiClient
}

func startArchivedRecoveryEpoch(t *testing.T, ctx context.Context, control store.ControllerEpochStore, mirror store.ControllerEpochMirror, holder string) (*ControllerEpochManager, func()) {
	t.Helper()
	epochs := NewControllerEpochManager(control, holder)
	if mirror != nil {
		epochs.WithMirror(mirror)
	}
	epochCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- epochs.Start(epochCtx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("stop archive recovery epoch manager: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}
	return epochs, stop
}
