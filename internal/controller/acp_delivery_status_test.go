package controller

import (
	"context"
	"encoding/json"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

var _ = Describe("ACP delivery projection", func() {
	DescribeTable("settles Tasks without a repository through the Task API",
		func(workspace *corev1alpha1.WorkspaceConfig, external bool) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			db, err := sqlite.NewDB(filepath.Join(GinkgoT().TempDir(), "control.db"))
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(db.Close()).To(Succeed()) }()
			controlStore := sqlite.NewStore(db, "test")
			epochs := NewControllerEpochManager(controlStore, "controller")
			epochCtx, cancelEpoch := context.WithCancel(ctx)
			epochDone := make(chan error, 1)
			go func() { epochDone <- epochs.Start(epochCtx) }()
			defer func() {
				cancelEpoch()
				Expect(<-epochDone).To(Succeed())
			}()
			_, err = epochs.CurrentFence(ctx)
			Expect(err).NotTo(HaveOccurred())

			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Namespace: defaultNS, GenerateName: "delivery-no-repository-"},
				Spec: corev1alpha1.TaskSpec{
					Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: "agent"},
					Prompt: "Complete the task", Workspace: workspace.DeepCopy(),
				},
			}
			Expect(k8sClient.Create(ctx, task)).To(Succeed())
			key := client.ObjectKeyFromObject(task)
			defer cleanupTask(context.Background(), key)
			Expect(k8sClient.Get(ctx, key, task)).To(Succeed())
			Expect(task.Spec.Workspace).To(Equal(workspace))
			task.Status.Phase = corev1alpha1.TaskPhaseRunning
			task.Status.Execution = &corev1alpha1.TaskExecutionStatus{
				State: corev1alpha1.TaskExecutionStateSettling, Attempt: 1, PromptID: "prompt-1",
			}
			if external {
				task.Status.Execution.AgentRuntimeName = "agentkit-v2"
				task.Status.Execution.AgentRuntimeUID = "external-runtime-uid"
			} else {
				task.Status.Execution.RuntimePoolName = "builtin-pool"
				task.Status.Execution.RuntimePoolUID = "builtin-pool-uid"
			}
			Expect(k8sClient.Status().Update(ctx, task)).To(Succeed())

			baseline, _, err := emptyRuntimeWorkspace(task, string(task.UID))
			Expect(err).NotTo(HaveOccurred())
			Expect(baseline.Revision).To(Equal(acpNoWorkspaceRevision))
			delivery := corev1alpha1.TaskDeliveryStatus{
				State: corev1alpha1.TaskDeliveryStateReadValidated, Outcome: corev1alpha1.TaskDeliveryOutcomeReadValidated,
				StartingSHA: baseline.Revision,
			}

			By("proving the real API rejects the internal workspace revision")
			invalid := task.DeepCopy()
			invalid.Status.Delivery = delivery.DeepCopy()
			err = k8sClient.Status().Update(ctx, invalid)
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("status.delivery.startingSHA"))

			By("settling execution with an omitted public starting SHA")
			dispatcher := &ACPDispatcher{Client: k8sClient, Store: controlStore, Epochs: epochs}
			Expect(dispatcher.patchDeliveryStatus(ctx, task, delivery)).To(Succeed())
			Expect(dispatcher.completeSuccessWithDelivery(ctx, task, delivery, "completed")).To(Succeed())
			completed := &corev1alpha1.Task{}
			Expect(k8sClient.Get(ctx, key, completed)).To(Succeed())
			Expect(completed.Status.Phase).To(Equal(corev1alpha1.TaskPhaseSucceeded))
			Expect(completed.Status.Execution.Outcome).To(Equal(corev1alpha1.TaskExecutionOutcomeSucceeded))
			Expect(completed.Status.Delivery.StartingSHA).To(BeEmpty())
			Expect(completed.Status.Delivery.Outcome).To(Equal(corev1alpha1.TaskDeliveryOutcomeReadValidated))

			projection, err := controlStore.GetOutboxProjection(ctx, standaloneTaskTerminalProjectionID(task, 1))
			Expect(err).NotTo(HaveOccurred())
			var payload taskTerminalProjection
			Expect(json.Unmarshal(projection.Payload, &payload)).To(Succeed())
			Expect(payload.Delivery.StartingSHA).To(Equal(acpNoWorkspaceRevision))

			By("replaying the immutable outbox record after an interrupted status write")
			completed.Status = *task.Status.DeepCopy()
			Expect(k8sClient.Status().Update(ctx, completed)).To(Succeed())
			projector := &ACPOutboxProjector{Client: k8sClient, Store: controlStore, Epochs: epochs, WorkerID: "worker"}
			Expect(projector.projectOnce(ctx)).To(Succeed())
			Expect(k8sClient.Get(ctx, key, completed)).To(Succeed())
			Expect(completed.Status.Phase).To(Equal(corev1alpha1.TaskPhaseSucceeded))
			Expect(completed.Status.Execution.Outcome).To(Equal(corev1alpha1.TaskExecutionOutcomeSucceeded))
			Expect(completed.Status.Delivery.StartingSHA).To(BeEmpty())
			Expect(completed.Status.Delivery.Outcome).To(Equal(corev1alpha1.TaskDeliveryOutcomeReadValidated))
			Expect(completed.Status.Execution.RuntimePoolUID).To(Equal(task.Status.Execution.RuntimePoolUID))
			Expect(completed.Status.Execution.AgentRuntimeUID).To(Equal(task.Status.Execution.AgentRuntimeUID))
			stored, err := controlStore.GetOutboxProjection(ctx, projection.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(stored.State).To(Equal(store.OutboxProjectionDelivered))
			Expect(stored.Payload).To(Equal(projection.Payload))
		},
		Entry("built-in runtime, omitted workspace", nil, false),
		Entry("built-in runtime, explicit read workspace", &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentRead}, false),
		Entry("external runtime, omitted workspace", nil, true),
		Entry("external runtime, explicit read workspace", &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentRead}, true),
	)
})
