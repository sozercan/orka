package controller

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	storekube "github.com/orka-agents/orka/internal/store/kube"
	"github.com/orka-agents/orka/internal/store/sqlite"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPrepareTaskSessionCompositeStoreOpensTurn(t *testing.T) {
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

	const (
		namespaceName = "orka-system"
		namespaceUID  = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
		taskUID       = "11111111-2222-3333-4444-555555555555"
	)
	task := runtimePoolReservationTestTask("session-composite", taskUID, "runtime-pool-uid")
	task.Namespace = namespaceName
	task.Spec.Prompt = "first message in session"
	task.Spec.SessionRef = &corev1alpha1.SessionReference{
		Name: "session-composite", Create: true, Append: true,
	}
	task.Status.Attempts = 1
	task.Status.Execution.State = corev1alpha1.TaskExecutionStateReserved
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: namespaceName, UID: types.UID(namespaceUID),
	}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&corev1alpha1.Task{},
			&corev1alpha1.ControllerEpoch{},
			&corev1alpha1.PromptAttempt{},
			&corev1alpha1.RuntimeSessionControl{},
		).
		WithObjects(namespace, task.DeepCopy()).
		Build()
	kubeClient = withControllerEpochLeaseUIDs(t, kubeClient)
	testPrepareTaskSessionCompositeStoreOpensTurn(t, kubeClient, task, namespaceUID, sessionCompositeTestOptions{
		expectedPrompt: task.Spec.Prompt,
	})
}

func TestPrepareTaskSessionCompositeStoreEstablishesGatewayLineage(t *testing.T) {
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

	const (
		namespaceName = "orka-system"
		namespaceUID  = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
		taskUID       = "66666666-7777-8888-9999-000000000000"
		eventID       = "gateway-composite-event"
		gatewayPrompt = "answer the gateway request from the canonical transcript"
	)
	throughMessageID := store.GatewayUserMessageID(eventID)
	task := runtimePoolReservationTestTask("gateway-session-composite", taskUID, "runtime-pool-uid")
	task.Namespace = namespaceName
	task.Spec.Prompt = "this Task prompt must not be used"
	task.Spec.SessionRef = &corev1alpha1.SessionReference{
		Name: "gateway-session-composite", Create: false, Append: false,
		MaxMessages:      int32(store.GatewayTranscriptMessageLimit),
		ThroughMessageID: throughMessageID, PromptIncluded: true,
	}
	task.Status.Attempts = 1
	task.Status.Execution.State = corev1alpha1.TaskExecutionStateReserved
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: namespaceName, UID: types.UID(namespaceUID),
	}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&corev1alpha1.Task{},
			&corev1alpha1.ControllerEpoch{},
			&corev1alpha1.PromptAttempt{},
			&corev1alpha1.RuntimeSessionControl{},
		).
		WithObjects(namespace, task.DeepCopy()).
		Build()
	kubeClient = withControllerEpochLeaseUIDs(t, kubeClient)
	testPrepareTaskSessionCompositeStoreOpensTurn(t, kubeClient, task, namespaceUID, sessionCompositeTestOptions{
		expectedPrompt:               gatewayPrompt,
		expectedTranscriptMessageIDs: []string{throughMessageID},
		expectSkipTranscriptAppend:   true,
		seedTranscript: func(ctx context.Context, transcripts *sqlite.Store) {
			now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
			if _, created, err := transcripts.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
				Event: store.GatewayEvent{
					ID: eventID, Namespace: namespaceName, NamespaceUID: namespaceUID,
					GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "gateway",
					ExternalEventID: "external-event", ProtocolVersion: "orka.gateway.v1", EventType: "text",
					AccountID: "account", ContextID: "context", SenderID: "sender", Text: gatewayPrompt,
					SessionName: task.Spec.SessionRef.Name, TaskName: task.Name,
					ReceivedAt: now, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
				},
				AppendUserMessage: true,
				PendingLimit:      100,
			}); err != nil {
				t.Fatal(err)
			} else if !created {
				t.Fatal("gateway event was not admitted")
			}
			if _, err := transcripts.ClaimNextGatewayEvent(ctx, namespaceName, "session-composite-owner", now, time.Minute); err != nil {
				t.Fatal(err)
			}
			if err := transcripts.MarkGatewayEventTaskCreated(
				ctx, namespaceName, eventID, task.Name, string(task.UID), "session-composite-owner", now,
			); err != nil {
				t.Fatal(err)
			}
		},
	})
}

var _ = ginkgo.Describe("ACP dispatcher Session continuity", func() {
	ginkgo.It("opens a SessionTurn through the composite store against a real API server", func() {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("session-composite-%d", time.Now().UnixNano()),
		}}
		gomega.Expect(k8sClient.Create(ctx, namespace)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			gomega.Expect(client.IgnoreNotFound(k8sClient.Delete(context.Background(), namespace))).To(gomega.Succeed())
		})

		task := runtimePoolReservationTestTask("session-composite", "11111111-2222-3333-4444-555555555555", "runtime-pool-uid")
		task.Namespace = namespace.Name
		task.Spec.Prompt = "first message in session"
		task.Spec.SessionRef = &corev1alpha1.SessionReference{
			Name: "session-composite", Create: true, Append: true,
		}
		task.Status.Attempts = 1
		task.Status.Execution.State = corev1alpha1.TaskExecutionStateReserved
		desiredStatus := *task.Status.DeepCopy()
		task.UID = ""
		task.Status = corev1alpha1.TaskStatus{}
		gomega.Expect(k8sClient.Create(ctx, task)).To(gomega.Succeed())
		task.Status = desiredStatus
		task.Status.Execution.PromptID = "prompt-" + string(task.UID) + "-1"

		testPrepareTaskSessionCompositeStoreOpensTurn(ginkgo.GinkgoT(), k8sClient, task, string(namespace.UID), sessionCompositeTestOptions{
			expectedPrompt: task.Spec.Prompt,
		})
	})
})

type sessionCompositeTestTB interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Errorf(format string, args ...any)
	Cleanup(func())
	TempDir() string
}

type sessionCompositeTestOptions struct {
	expectedPrompt               string
	expectedTranscriptMessageIDs []string
	expectSkipTranscriptAppend   bool
	seedTranscript               func(context.Context, *sqlite.Store)
}

func testPrepareTaskSessionCompositeStoreOpensTurn(
	t sessionCompositeTestTB,
	kubeClient client.Client,
	task *corev1alpha1.Task,
	namespaceUID string,
	options sessionCompositeTestOptions,
) {
	t.Helper()
	const namespaceName = "orka-system"
	if task.Namespace == "" {
		task.Namespace = namespaceName
	}

	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "session-composite.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sqliteStore := sqlite.NewStore(db, "session-composite-test")
	if options.seedTranscript != nil {
		options.seedTranscript(context.Background(), sqliteStore)
	}
	controlStore, err := storekube.NewComposite(kubeClient, task.Namespace, sqliteStore, storekube.WithAPIReader(kubeClient))
	if err != nil {
		t.Fatal(err)
	}

	epochs := NewControllerEpochManager(controlStore, "session-composite-controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	t.Cleanup(func() {
		cancelEpoch()
		if err := <-epochDone; err != nil {
			t.Errorf("stop controller epoch manager: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}

	attemptKey := store.PromptAttemptKey{
		Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1,
		PromptID: task.Status.Execution.PromptID,
	}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		Key: attemptKey, RequestDigest: task.Status.Execution.RequestDigest,
	}), fence)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &ACPDispatcher{Client: kubeClient, APIReader: kubeClient, Store: controlStore}
	if err := dispatcher.transitionAttempt(
		ctx, attempt.ID, fence, store.PromptExecutionQueued, store.PromptExecutionReserved,
		"reserve-session-composite", nil,
	); err != nil {
		t.Fatal(err)
	}
	continuity, err := NewACPSessionContinuity(ACPSessionContinuityConfig{
		SessionControls: controlStore,
		Transcripts:     sqliteStore,
		GatewayEvents:   sqliteStore,
		Publications:    controlStore,
		BranchClaims:    controlStore,
		Lineages:        sqliteStore,
		NewSessionUID:   func() (string, error) { return "session-composite-uid", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.Sessions = continuity

	profileDigest := harnessv2.ProfileDigest(testControlDigestForDispatcher("session-composite-profile"))
	session, err := dispatcher.prepareTaskSession(
		ctx,
		task,
		fence,
		profileDigest,
		testControlDigestForDispatcher("session-composite-mcp"),
		"runtime-instance",
		"supervisor-boot",
		acpSessionLineageIdentity{
			NamespaceUID: namespaceUID, RuntimeIdentity: "claude", ConfigDigest: string(profileDigest),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if session == nil || session.Turn == nil || session.Turn.Turn.State != store.SessionTurnOpen {
		t.Fatalf("prepared session = %#v, want an open SessionTurn", session)
	}
	if session.UserPrompt != options.expectedPrompt || session.Turn.Turn.UserPrompt != options.expectedPrompt {
		t.Fatalf("prepared prompt = %q, turn prompt = %q, want %q", session.UserPrompt, session.Turn.Turn.UserPrompt, options.expectedPrompt)
	}
	if session.Turn.SkipTranscriptAppend != options.expectSkipTranscriptAppend {
		t.Fatalf("SkipTranscriptAppend = %v, want %v", session.Turn.SkipTranscriptAppend, options.expectSkipTranscriptAppend)
	}
	persistedAttempt, err := controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persistedAttempt.ExecutionState != store.PromptExecutionSessionStarting ||
		persistedAttempt.SessionUID != session.Binding.SessionUID ||
		persistedAttempt.SessionLeaseGeneration != session.LeaseGeneration {
		t.Fatalf("prepared PromptAttempt = %#v, want the exact SessionTurn binding", persistedAttempt)
	}
	authoritative, err := controlStore.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name)
	if err != nil {
		t.Fatal(err)
	}
	if authoritative.Lineage == nil || authoritative.Lease == nil ||
		authoritative.Lease.Generation != session.LeaseGeneration {
		t.Fatalf("authoritative Session lineage and Lease = %#v, want committed lineage and generation %d", authoritative, session.LeaseGeneration)
	}
	if _, err := sqliteStore.GetSessionLineage(ctx, task.Namespace, task.Spec.SessionRef.Name); err != nil {
		t.Fatalf("load projected Session lineage: %v", err)
	}
	if len(options.expectedTranscriptMessageIDs) > 0 {
		record, err := sqliteStore.GetSession(ctx, task.Namespace, task.Spec.SessionRef.Name)
		if err != nil {
			t.Fatal(err)
		}
		if len(record.Messages) != len(options.expectedTranscriptMessageIDs) {
			t.Fatalf("transcript messages = %#v, want IDs %#v", record.Messages, options.expectedTranscriptMessageIDs)
		}
		for i, wantID := range options.expectedTranscriptMessageIDs {
			if record.Messages[i].ID != wantID {
				t.Fatalf("transcript message %d ID = %q, want %q", i, record.Messages[i].ID, wantID)
			}
		}
	}
}
