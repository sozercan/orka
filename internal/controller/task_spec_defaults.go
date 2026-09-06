package controller

import (
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

// CRD-level defaults the API server stamps onto every persisted TaskSpec
// (see the +kubebuilder:default markers under api/v1alpha1). A controller that
// compares a freshly built desired spec against a stored Task must apply the
// same defaults first, otherwise the two never match. Defaults whose value is
// the Go zero value (false, 0, "") need no mirroring.
const (
	taskSpecDefaultPriority                = int32(500)
	taskSpecDefaultStartingDeadlineSeconds = int64(100)
	taskSpecDefaultSuccessfulRunsHistory   = int32(3)
	taskSpecDefaultFailedRunsHistory       = int32(1)
	taskSpecDefaultRetryBackoffMultiplier  = float64(2)
	taskSpecDefaultSessionMaxMessages      = int32(50)
	taskSpecDefaultCredentialKey           = "token"
	taskSpecDefaultWorkspaceReusePolicy    = corev1alpha1.WorkspaceReusePolicy("none")
	taskSpecDefaultWorkspaceSlot           = "default"
)

// taskSpecWithServerDefaults returns a copy of spec with the CRD defaults
// applied exactly as the API server would on create. Nested defaults only
// apply when the enclosing object is present, mirroring OpenAPI defaulting.
func taskSpecWithServerDefaults(spec corev1alpha1.TaskSpec) corev1alpha1.TaskSpec {
	out := *spec.DeepCopy()
	if out.Priority == nil {
		out.Priority = new(taskSpecDefaultPriority)
	}
	if out.ConcurrencyPolicy == "" {
		out.ConcurrencyPolicy = corev1alpha1.ForbidConcurrent
	}
	if out.StartingDeadlineSeconds == nil {
		out.StartingDeadlineSeconds = new(taskSpecDefaultStartingDeadlineSeconds)
	}
	if out.SuccessfulRunsHistoryLimit == nil {
		out.SuccessfulRunsHistoryLimit = new(taskSpecDefaultSuccessfulRunsHistory)
	}
	if out.FailedRunsHistoryLimit == nil {
		out.FailedRunsHistoryLimit = new(taskSpecDefaultFailedRunsHistory)
	}
	if out.RetryPolicy != nil && out.RetryPolicy.BackoffMultiplier == 0 {
		out.RetryPolicy.BackoffMultiplier = taskSpecDefaultRetryBackoffMultiplier
	}
	if out.SessionRef != nil && out.SessionRef.MaxMessages == 0 {
		out.SessionRef.MaxMessages = taskSpecDefaultSessionMaxMessages
	}
	if out.Workspace != nil {
		for _, ref := range []*corev1alpha1.WorkspaceCredentialReference{
			out.Workspace.ReadCredentialRef,
			out.Workspace.PublicationReadCredentialRef,
			out.Workspace.PublicationCredentialRef,
			out.Workspace.ForgeCredentialRef,
		} {
			if ref != nil && ref.Key == "" {
				ref.Key = taskSpecDefaultCredentialKey
			}
		}
	}
	if out.Execution != nil && out.Execution.Workspace != nil {
		if out.Execution.Workspace.ReusePolicy == "" {
			out.Execution.Workspace.ReusePolicy = taskSpecDefaultWorkspaceReusePolicy
		}
		if out.Execution.Workspace.WorkspaceSlot == "" {
			out.Execution.Workspace.WorkspaceSlot = taskSpecDefaultWorkspaceSlot
		}
	}
	return out
}
