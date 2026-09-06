/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package admission

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
)

const (
	admissionTestNamespace                  = "tenant-a"
	admissionTestTaskName                   = "admission-task"
	untrustedUsername                       = "system:serviceaccount:tenant-a:tenant-user"
	trustedControllerUser                   = "system:serviceaccount:orka-system:orka-controller-manager"
	trustedWorkerUser                       = "system:serviceaccount:tenant-a:orka-ai-worker"
	admissionTestTransactionID              = "txn-1"
	admissionTestParentTaskUID              = "parent-task-uid"
	admissionTestWorkspaceSettledAnnotation = "acp.workspace.orka.ai/workspace-settled"
	admissionWorkspaceUIDKey                = "acp.workspace.orka.ai/execution-workspace-uid"
	admissionWorkspaceLinkKey               = "acp.workspace.orka.ai/execution-workspace"
	admissionWorkspaceName                  = "acp-ws-x"
)

func TestTaskProvenanceValidator_Create(t *testing.T) {
	validator := newTestTaskProvenanceValidator(t)

	tests := []struct {
		name     string
		user     string
		task     *corev1alpha1.Task
		allowed  bool
		contains string
	}{
		{
			name:    "untrusted create without provenance allowed",
			user:    untrustedUsername,
			task:    newAdmissionTestTask(),
			allowed: true,
		},
		{
			name:     "untrusted create with requestedBy denied",
			user:     untrustedUsername,
			task:     withRequestedBy(newAdmissionTestTask()),
			contains: fieldSpecRequestedBy,
		},
		{
			name:     "untrusted create with transaction denied",
			user:     untrustedUsername,
			task:     withTransaction(newAdmissionTestTask()),
			contains: fieldSpecTransaction,
		},
		{
			name:     "untrusted create with transaction metadata denied",
			user:     untrustedUsername,
			task:     withTransactionMetadata(newAdmissionTestTask()),
			contains: labels.LabelTransactionID,
		},
		{
			name:     "untrusted create with transaction token pending annotation denied",
			user:     untrustedUsername,
			task:     withTransactionTokenPending(newAdmissionTestTask()),
			contains: labels.AnnotationTransactionTokenPending,
		},
		{
			name:     "untrusted create with trace annotation denied",
			user:     untrustedUsername,
			task:     withTraceAnnotation(newAdmissionTestTask(), "00-"+strings.Repeat("1", 32)+"-"+strings.Repeat("2", 16)+"-01"),
			contains: labels.AnnotationTraceParent,
		},
		{
			name:     "untrusted create with parent task UID denied",
			user:     untrustedUsername,
			task:     withParentTaskUID(newAdmissionTestTask()),
			contains: labels.AnnotationParentTaskUID,
		},
		{
			name:     "untrusted create with workspace settlement marker denied",
			user:     untrustedUsername,
			task:     withWorkspaceSettledAnnotation(newAdmissionTestTask()),
			contains: admissionTestWorkspaceSettledAnnotation,
		},
		{
			name: "untrusted create with workspace incarnation pin denied",
			user: untrustedUsername,
			task: func() *corev1alpha1.Task {
				task := newAdmissionTestTask()
				task.Annotations = map[string]string{admissionWorkspaceUIDKey: "forged-uid"}
				return task
			}(),
			contains: admissionWorkspaceUIDKey,
		},
		{
			name: "untrusted create with workspace link label denied",
			user: untrustedUsername,
			task: func() *corev1alpha1.Task {
				task := newAdmissionTestTask()
				task.Labels = map[string]string{admissionWorkspaceLinkKey: admissionWorkspaceName}
				return task
			}(),
			contains: admissionWorkspaceLinkKey,
		},
		{
			name:     "trusted worker with workspace settlement marker denied",
			user:     trustedWorkerUser,
			task:     withWorkspaceSettledAnnotation(newAdmissionTestTask()),
			contains: admissionTestWorkspaceSettledAnnotation,
		},
		{
			name: "trusted worker with workspace incarnation pin denied",
			user: trustedWorkerUser,
			task: func() *corev1alpha1.Task {
				task := newAdmissionTestTask()
				task.Annotations = map[string]string{admissionWorkspaceUIDKey: "forged-uid"}
				return task
			}(),
			contains: admissionWorkspaceUIDKey,
		},
		{
			name: "trusted worker with workspace link label denied",
			user: trustedWorkerUser,
			task: func() *corev1alpha1.Task {
				task := newAdmissionTestTask()
				task.Labels = map[string]string{admissionWorkspaceLinkKey: admissionWorkspaceName}
				return task
			}(),
			contains: admissionWorkspaceLinkKey,
		},
		{
			name:     "trusted worker with parent task UID denied",
			user:     trustedWorkerUser,
			task:     withParentTaskUID(newAdmissionTestTask()),
			contains: labels.AnnotationParentTaskUID,
		},
		{
			name:    "trusted controller can create with provenance",
			user:    trustedControllerUser,
			task:    withParentTaskUID(withTransaction(withRequestedBy(newAdmissionTestTask()))),
			allowed: true,
		},
		{
			name:    "trusted namespace worker can create child with provenance",
			user:    trustedWorkerUser,
			task:    withTransactionMetadata(withTransaction(newAdmissionTestTask())),
			allowed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := validator.Handle(context.Background(), admissionRequest(t, admissionv1.Create, tt.user, tt.task, nil, ""))
			require.Equal(t, tt.allowed, resp.Allowed)
			if tt.contains != "" {
				require.Contains(t, resp.Result.Message, tt.contains)
			}
		})
	}
}

func TestTaskProvenanceValidator_Update(t *testing.T) {
	validator := newTestTaskProvenanceValidator(t)

	oldTask := newAdmissionTestTask()
	oldWithProvenance := withTransactionMetadata(withTransaction(withRequestedBy(newAdmissionTestTask())))

	tests := []struct {
		name        string
		user        string
		oldTask     *corev1alpha1.Task
		newTask     *corev1alpha1.Task
		subresource string
		allowed     bool
		contains    string
	}{
		{
			name:    "untrusted update without provenance changes allowed",
			user:    untrustedUsername,
			oldTask: oldTask,
			newTask: withImage(oldTask.DeepCopy(), "alpine"),
			allowed: true,
		},
		{
			name:     "untrusted update adding requestedBy denied",
			user:     untrustedUsername,
			oldTask:  oldTask,
			newTask:  withRequestedBy(oldTask.DeepCopy()),
			contains: fieldSpecRequestedBy,
		},
		{
			name:     "untrusted update changing transaction denied",
			user:     untrustedUsername,
			oldTask:  oldWithProvenance,
			newTask:  withTransactionID(oldWithProvenance.DeepCopy(), "txn-2"),
			contains: fieldSpecTransaction,
		},
		{
			name:     "untrusted update changing transaction annotation denied",
			user:     untrustedUsername,
			oldTask:  oldWithProvenance,
			newTask:  withTransactionAnnotation(oldWithProvenance.DeepCopy(), "txn-2"),
			contains: labels.AnnotationTransactionID,
		},
		{
			name:     "untrusted update adding transaction token pending annotation denied",
			user:     untrustedUsername,
			oldTask:  oldTask,
			newTask:  withTransactionTokenPending(oldTask.DeepCopy()),
			contains: labels.AnnotationTransactionTokenPending,
		},
		{
			name:     "untrusted update adding trace annotation denied",
			user:     untrustedUsername,
			oldTask:  oldTask,
			newTask:  withTraceAnnotation(oldTask.DeepCopy(), "00-"+strings.Repeat("3", 32)+"-"+strings.Repeat("4", 16)+"-01"),
			contains: labels.AnnotationTraceParent,
		},
		{
			name:     "untrusted update adding parent task UID denied",
			user:     untrustedUsername,
			oldTask:  oldTask,
			newTask:  withParentTaskUID(oldTask.DeepCopy()),
			contains: labels.AnnotationParentTaskUID,
		},
		{
			name:     "trusted worker update adding parent task UID denied",
			user:     trustedWorkerUser,
			oldTask:  oldTask,
			newTask:  withParentTaskUID(oldTask.DeepCopy()),
			contains: labels.AnnotationParentTaskUID,
		},
		{
			name:    "trusted controller can update provenance",
			user:    trustedControllerUser,
			oldTask: oldTask,
			newTask: withParentTaskUID(withTransaction(withRequestedBy(oldTask.DeepCopy()))),
			allowed: true,
		},
		{
			name:    "status subresource without provenance changes allowed",
			user:    untrustedUsername,
			oldTask: oldTask,
			newTask: func() *corev1alpha1.Task {
				task := oldTask.DeepCopy()
				task.Status.Phase = corev1alpha1.TaskPhaseRunning
				return task
			}(),
			subresource: statusSubresource,
			allowed:     true,
		},
		{
			name:    "status subresource cannot add workspace settlement metadata",
			user:    untrustedUsername,
			oldTask: oldTask,
			newTask: func() *corev1alpha1.Task {
				task := oldTask.DeepCopy()
				task.Labels = map[string]string{admissionWorkspaceLinkKey: admissionWorkspaceName}
				return task
			}(),
			subresource: statusSubresource,
			contains:    admissionWorkspaceLinkKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := validator.Handle(context.Background(), admissionRequest(t, admissionv1.Update, tt.user, tt.newTask, tt.oldTask, tt.subresource))
			require.Equal(t, tt.allowed, resp.Allowed)
			if tt.contains != "" {
				require.Contains(t, resp.Result.Message, tt.contains)
			}
		})
	}
}

func newTestTaskProvenanceValidator(t *testing.T) *TaskProvenanceValidator {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	return NewTaskProvenanceValidator(scheme, NewTaskProvenanceConfig(true, "", "", "", "orka-system"))
}

func admissionRequest(
	t *testing.T,
	operation admissionv1.Operation,
	username string,
	task *corev1alpha1.Task,
	oldTask *corev1alpha1.Task,
	subresource string,
) ctrladmission.Request {
	t.Helper()
	req := ctrladmission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation:   operation,
		Namespace:   admissionTestNamespace,
		SubResource: subresource,
		UserInfo: authenticationv1.UserInfo{
			Username: username,
		},
		Object: runtime.RawExtension{Raw: mustMarshalAdmissionTask(t, task)},
	}}
	if oldTask != nil {
		req.OldObject = runtime.RawExtension{Raw: mustMarshalAdmissionTask(t, oldTask)}
	}
	return req
}

func mustMarshalAdmissionTask(t *testing.T, task *corev1alpha1.Task) []byte {
	t.Helper()
	copy := task.DeepCopy()
	copy.TypeMeta = metav1.TypeMeta{
		APIVersion: corev1alpha1.GroupVersion.String(),
		Kind:       "Task",
	}
	data, err := json.Marshal(copy)
	require.NoError(t, err)
	return data
}

func newAdmissionTestTask() *corev1alpha1.Task {
	return &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      admissionTestTaskName,
			Namespace: admissionTestNamespace,
		},
		Spec: corev1alpha1.TaskSpec{
			Type:  corev1alpha1.TaskTypeContainer,
			Image: "busybox",
		},
	}
}

func withRequestedBy(task *corev1alpha1.Task) *corev1alpha1.Task {
	task.Spec.RequestedBy = &corev1alpha1.RequestedBy{Subject: "subject"}
	return task
}

func withTransaction(task *corev1alpha1.Task) *corev1alpha1.Task {
	task.Spec.Transaction = &corev1alpha1.TaskTransaction{ID: admissionTestTransactionID, Scope: "orka:tasks:create"}
	return task
}

func withTransactionID(task *corev1alpha1.Task, id string) *corev1alpha1.Task {
	task.Spec.Transaction.ID = id
	return task
}

func withTransactionMetadata(task *corev1alpha1.Task) *corev1alpha1.Task {
	if task.Labels == nil {
		task.Labels = map[string]string{}
	}
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Labels[labels.LabelTransactionID] = admissionTestTransactionID
	task.Annotations[labels.AnnotationTransactionID] = admissionTestTransactionID
	return task
}

func withTransactionAnnotation(task *corev1alpha1.Task, id string) *corev1alpha1.Task {
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Annotations[labels.AnnotationTransactionID] = id
	return task
}

func withTransactionTokenPending(task *corev1alpha1.Task) *corev1alpha1.Task {
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Annotations[labels.AnnotationTransactionTokenPending] = "true"
	task.Annotations[labels.AnnotationTransactionTokenPendingSince] = "2026-01-01T00:00:00Z"
	return task
}

func withWorkspaceSettledAnnotation(task *corev1alpha1.Task) *corev1alpha1.Task {
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Annotations[admissionTestWorkspaceSettledAnnotation] = "true"
	return task
}

func withTraceAnnotation(task *corev1alpha1.Task, traceparent string) *corev1alpha1.Task {
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Annotations[labels.AnnotationTraceParent] = traceparent
	task.Annotations[labels.AnnotationTraceBaggage] = "tenant=untrusted"
	return task
}

func withParentTaskUID(task *corev1alpha1.Task) *corev1alpha1.Task {
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Annotations[labels.AnnotationParentTaskUID] = admissionTestParentTaskUID
	return task
}

func withImage(task *corev1alpha1.Task, image string) *corev1alpha1.Task {
	task.Spec.Image = image
	return task
}

func TestNewTaskProvenanceConfigDefaults(t *testing.T) {
	cfg := NewTaskProvenanceConfig(true, "", "", "", "orka-system")
	require.True(t, cfg.Enabled)
	require.Contains(t, cfg.TrustedUsernames, trustedControllerUser)
	require.ElementsMatch(t, []string{"orka-ai-worker", "orka-vendor-worker"}, cfg.TrustedServiceAccountNames)
}

// Deployment-supplied controller identities (the release-specific Helm
// ServiceAccount passed through --execution-mode-controller-usernames) are
// EXCLUSIVE: only the explicit list receives controller-only settlement
// trust, and the namespace-derived ServiceAccount defaults apply solely when
// no explicit identities were configured - appending them would silently
// widen authorization to any same-named ServiceAccount in the namespace.
func TestNewTaskProvenanceConfigUsesExplicitControllerUsernamesExclusively(t *testing.T) {
	release := "system:serviceaccount:orka-system:my-release-orka"
	exclusive := NewTaskProvenanceConfig(true, release, "", "", "orka-system")
	require.Contains(t, exclusive.ControllerUsernames, release)
	require.NotContains(t, exclusive.ControllerUsernames, trustedControllerUser,
		"an explicit controller identity list must not be widened with the namespace-derived defaults")

	cfg := NewTaskProvenanceConfig(true, release+", "+trustedControllerUser, "", "", "orka-system")
	require.Contains(t, cfg.ControllerUsernames, release)
	require.Contains(t, cfg.ControllerUsernames, trustedControllerUser)

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	validator := NewTaskProvenanceValidator(scheme, cfg)
	task := newAdmissionTestTask()
	task.Labels = map[string]string{admissionWorkspaceLinkKey: admissionWorkspaceName}
	response := validator.Handle(context.Background(),
		admissionRequest(t, admissionv1.Create, release, task, nil, ""))
	require.True(t, response.Allowed,
		"the deployment-supplied controller identity must be allowed to write workspace settlement metadata")
}

// Flag-listed trusted users keep only the provenance-field allowance: the
// reserved workspace settlement metadata stays controller-only.
func TestTaskProvenanceFlagTrustedUserCannotWriteWorkspaceMetadata(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	validator := NewTaskProvenanceValidator(
		scheme,
		NewTaskProvenanceConfig(true, "", "system:serviceaccount:custom:provenance-writer", "", "orka-system"),
	)

	task := newAdmissionTestTask()
	task.Labels = map[string]string{admissionWorkspaceLinkKey: admissionWorkspaceName}
	response := validator.Handle(context.Background(),
		admissionRequest(t, admissionv1.Create, "system:serviceaccount:custom:provenance-writer", task, nil, ""))
	if response.Allowed {
		t.Fatal("a flag-trusted provenance writer must not set workspace settlement metadata")
	}

	// The same principal keeps the provenance allowance.
	provenance := newAdmissionTestTask()
	provenance.Spec.RequestedBy = &corev1alpha1.RequestedBy{Subject: "someone"}
	response = validator.Handle(context.Background(),
		admissionRequest(t, admissionv1.Create, "system:serviceaccount:custom:provenance-writer", provenance, nil, ""))
	if !response.Allowed {
		t.Fatalf("a flag-trusted provenance writer must keep the provenance allowance: %v", response.Result)
	}
}
