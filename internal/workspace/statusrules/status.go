// Package statusrules owns provider-neutral Execution Workspace status safety
// rules shared by API, worker, and controller adapters.
package statusrules

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

// Update is the internal worker-to-API Execution Workspace status update shape.
type Update struct {
	Provider      corev1alpha1.WorkspaceProvider                  `json:"provider"`
	TemplateRef   *corev1alpha1.WorkspaceTemplateReference        `json:"templateRef,omitempty"`
	Phase         corev1alpha1.ExecutionWorkspacePhase            `json:"phase"`
	Reason        corev1alpha1.ExecutionWorkspaceReason           `json:"reason"`
	ReusePolicy   corev1alpha1.WorkspaceReusePolicy               `json:"reusePolicy,omitempty"`
	CleanupPolicy corev1alpha1.WorkspaceCleanupPolicy             `json:"cleanupPolicy,omitempty"`
	Reused        bool                                            `json:"reused,omitempty"`
	Placement     *corev1alpha1.ExecutionWorkspacePlacementStatus `json:"placement,omitempty"`
	Density       *corev1alpha1.ExecutionWorkspaceDensityStatus   `json:"density,omitempty"`
	ResumeLatency *metav1.Duration                                `json:"resumeLatency,omitempty"`
	Message       string                                          `json:"message,omitempty"`
	ObservedAt    *metav1.Time                                    `json:"observedAt,omitempty"`
}

func (u Update) Status() *corev1alpha1.ExecutionWorkspaceStatus {
	updateTime := u.ObservedAt
	if updateTime == nil {
		now := metav1.Now()
		updateTime = &now
	}
	return &corev1alpha1.ExecutionWorkspaceStatus{
		Provider:       u.Provider,
		TemplateRef:    u.TemplateRef,
		Phase:          u.Phase,
		Reason:         u.Reason,
		ReusePolicy:    u.ReusePolicy,
		CleanupPolicy:  u.CleanupPolicy,
		Reused:         u.Reused,
		Placement:      SanitizePlacement(u.Placement),
		Density:        SanitizeDensity(u.Density),
		ResumeLatency:  u.ResumeLatency,
		Message:        SanitizeMessage(u.Message),
		LastUpdateTime: updateTime,
	}
}

func HasRequiredInboundFields(status *corev1alpha1.ExecutionWorkspaceStatus) bool {
	return status != nil && status.Provider != "" && status.Phase != "" && status.Reason != ""
}

func IsSupportedProvider(provider corev1alpha1.WorkspaceProvider) bool {
	switch provider {
	case corev1alpha1.WorkspaceProviderAgentSandbox, corev1alpha1.WorkspaceProviderSubstrate:
		return true
	default:
		return false
	}
}

func IsValidPhase(phase corev1alpha1.ExecutionWorkspacePhase) bool {
	switch phase {
	case corev1alpha1.ExecutionWorkspacePhasePending,
		corev1alpha1.ExecutionWorkspacePhaseReady,
		corev1alpha1.ExecutionWorkspacePhaseReleased,
		corev1alpha1.ExecutionWorkspacePhaseRetained,
		corev1alpha1.ExecutionWorkspacePhaseDeleted,
		corev1alpha1.ExecutionWorkspacePhaseFailed:
		return true
	default:
		return false
	}
}

func IsValidReason(reason corev1alpha1.ExecutionWorkspaceReason) bool {
	switch reason {
	case corev1alpha1.ExecutionWorkspaceReasonPending,
		corev1alpha1.ExecutionWorkspaceReasonClaimed,
		corev1alpha1.ExecutionWorkspaceReasonReady,
		corev1alpha1.ExecutionWorkspaceReasonReleased,
		corev1alpha1.ExecutionWorkspaceReasonRetained,
		corev1alpha1.ExecutionWorkspaceReasonDeleted,
		corev1alpha1.ExecutionWorkspaceReasonValidationFailed,
		corev1alpha1.ExecutionWorkspaceReasonAttachmentLocked,
		corev1alpha1.ExecutionWorkspaceReasonClaimFailed,
		corev1alpha1.ExecutionWorkspaceReasonReadinessFailed,
		corev1alpha1.ExecutionWorkspaceReasonHandoffFailed,
		corev1alpha1.ExecutionWorkspaceReasonCommandFailed,
		corev1alpha1.ExecutionWorkspaceReasonSecretScrubFailed,
		corev1alpha1.ExecutionWorkspaceReasonCleanupFailed,
		corev1alpha1.ExecutionWorkspaceReasonStatusUpdateFailed:
		return true
	default:
		return false
	}
}

func IsConcreteReusePolicy(policy corev1alpha1.WorkspaceReusePolicy) bool {
	switch policy {
	case corev1alpha1.WorkspaceReusePolicyNone, corev1alpha1.WorkspaceReusePolicySession:
		return true
	default:
		return false
	}
}

func IsOptionalReusePolicy(policy corev1alpha1.WorkspaceReusePolicy) bool {
	return policy == "" || IsConcreteReusePolicy(policy)
}

func IsConcreteCleanupPolicy(policy corev1alpha1.WorkspaceCleanupPolicy) bool {
	switch policy {
	case corev1alpha1.WorkspaceCleanupPolicyDelete, corev1alpha1.WorkspaceCleanupPolicyRetain:
		return true
	default:
		return false
	}
}

func IsOptionalCleanupPolicy(policy corev1alpha1.WorkspaceCleanupPolicy) bool {
	return policy == "" || IsConcreteCleanupPolicy(policy)
}

func StatusCleanupPolicy(policy, defaultPolicy corev1alpha1.WorkspaceCleanupPolicy) (corev1alpha1.WorkspaceCleanupPolicy, bool) {
	if policy == "" {
		if IsConcreteCleanupPolicy(defaultPolicy) {
			return defaultPolicy, true
		}
		return "", false
	}
	if IsConcreteCleanupPolicy(policy) {
		return policy, true
	}
	return "", false
}

func ValidInboundStatus(status *corev1alpha1.ExecutionWorkspaceStatus) bool {
	if status == nil {
		return false
	}
	return IsSupportedProvider(status.Provider) &&
		IsValidPhase(status.Phase) &&
		IsValidReason(status.Reason) &&
		IsOptionalReusePolicy(status.ReusePolicy) &&
		IsOptionalCleanupPolicy(status.CleanupPolicy)
}

type ValidationFailure struct {
	Provider      corev1alpha1.WorkspaceProvider
	TemplateRef   *corev1alpha1.WorkspaceTemplateReference
	ReusePolicy   corev1alpha1.WorkspaceReusePolicy
	CleanupPolicy corev1alpha1.WorkspaceCleanupPolicy
	Message       string
	ObservedAt    *metav1.Time
}

func ValidationFailedStatus(failure ValidationFailure) *corev1alpha1.ExecutionWorkspaceStatus {
	return Update{
		Provider:      failure.Provider,
		TemplateRef:   failure.TemplateRef,
		Phase:         corev1alpha1.ExecutionWorkspacePhaseFailed,
		Reason:        corev1alpha1.ExecutionWorkspaceReasonValidationFailed,
		ReusePolicy:   failure.ReusePolicy,
		CleanupPolicy: failure.CleanupPolicy,
		Message:       failure.Message,
		ObservedAt:    failure.ObservedAt,
	}.Status()
}

func CleanupSucceeded(status *corev1alpha1.ExecutionWorkspaceStatus) bool {
	if status == nil {
		return false
	}
	switch status.Reason {
	case corev1alpha1.ExecutionWorkspaceReasonRetained:
		return status.Phase == corev1alpha1.ExecutionWorkspacePhaseRetained
	case corev1alpha1.ExecutionWorkspaceReasonDeleted:
		return status.Phase == corev1alpha1.ExecutionWorkspacePhaseDeleted
	case corev1alpha1.ExecutionWorkspaceReasonReleased:
		return status.Phase == corev1alpha1.ExecutionWorkspacePhaseReleased
	default:
		return false
	}
}

func PreserveReadyTelemetry(status *corev1alpha1.ExecutionWorkspaceStatus, previous *corev1alpha1.ExecutionWorkspaceStatus) {
	if status == nil || previous == nil {
		return
	}
	if !ShouldPreserveReadyTelemetry(status, previous) {
		return
	}
	if status.Placement == nil && previous.Placement != nil {
		placement := *previous.Placement
		status.Placement = &placement
	}
	if status.Density == nil && previous.Density != nil {
		density := *previous.Density
		status.Density = &density
	}
	if status.ResumeLatency == nil && previous.ResumeLatency != nil {
		resumeLatency := *previous.ResumeLatency
		status.ResumeLatency = &resumeLatency
	}
}

func ShouldPreserveReadyTelemetry(status *corev1alpha1.ExecutionWorkspaceStatus, previous *corev1alpha1.ExecutionWorkspaceStatus) bool {
	if previous.Phase != corev1alpha1.ExecutionWorkspacePhaseReady {
		return false
	}
	switch status.Reason {
	case corev1alpha1.ExecutionWorkspaceReasonReleased,
		corev1alpha1.ExecutionWorkspaceReasonRetained,
		corev1alpha1.ExecutionWorkspaceReasonDeleted,
		corev1alpha1.ExecutionWorkspaceReasonHandoffFailed,
		corev1alpha1.ExecutionWorkspaceReasonCommandFailed,
		corev1alpha1.ExecutionWorkspaceReasonSecretScrubFailed,
		corev1alpha1.ExecutionWorkspaceReasonCleanupFailed:
		return true
	default:
		return false
	}
}

func SanitizeMessage(message string) string {
	message = strings.TrimSpace(message)
	if len(message) > 1024 {
		return message[:1024] + "...<truncated>"
	}
	return message
}

func SanitizePlacement(placement *corev1alpha1.ExecutionWorkspacePlacementStatus) *corev1alpha1.ExecutionWorkspacePlacementStatus {
	if placement == nil {
		return nil
	}
	sanitized := &corev1alpha1.ExecutionWorkspacePlacementStatus{
		WorkerNamespace: sanitizePlacementValue(placement.WorkerNamespace),
		WorkerPool:      sanitizePlacementValue(placement.WorkerPool),
		WorkerPodName:   sanitizePlacementValue(placement.WorkerPodName),
	}
	if sanitized.WorkerNamespace == "" && sanitized.WorkerPool == "" && sanitized.WorkerPodName == "" {
		return nil
	}
	return sanitized
}

func SanitizeDensity(density *corev1alpha1.ExecutionWorkspaceDensityStatus) *corev1alpha1.ExecutionWorkspaceDensityStatus {
	if density == nil {
		return nil
	}
	sanitized := &corev1alpha1.ExecutionWorkspaceDensityStatus{
		WorkerCount:         max(density.WorkerCount, 0),
		ActorCount:          max(density.ActorCount, 0),
		RunningActorCount:   max(density.RunningActorCount, 0),
		SuspendedActorCount: max(density.SuspendedActorCount, 0),
		ActorsPerWorker:     sanitizePlacementValue(density.ActorsPerWorker),
	}
	if sanitized.WorkerCount == 0 &&
		sanitized.ActorCount == 0 &&
		sanitized.RunningActorCount == 0 &&
		sanitized.SuspendedActorCount == 0 &&
		sanitized.ActorsPerWorker == "" {
		return nil
	}
	return sanitized
}

func sanitizePlacementValue(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 256 {
		return value[:256]
	}
	return value
}
