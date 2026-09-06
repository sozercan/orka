package statusrules

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

func TestUpdateStatusSanitizesInboundFields(t *testing.T) {
	observed := metav1.NewTime(time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC))
	status := Update{
		Provider: corev1alpha1.WorkspaceProviderSubstrate,
		Phase:    corev1alpha1.ExecutionWorkspacePhaseReady,
		Reason:   corev1alpha1.ExecutionWorkspaceReasonReady,
		Placement: &corev1alpha1.ExecutionWorkspacePlacementStatus{
			WorkerNamespace: "  substrate-system  ",
			WorkerPool:      strings.Repeat("p", 300),
			WorkerPodName:   " actor-pod ",
		},
		Density: &corev1alpha1.ExecutionWorkspaceDensityStatus{
			WorkerCount:         -1,
			ActorCount:          2,
			RunningActorCount:   -3,
			SuspendedActorCount: 4,
			ActorsPerWorker:     "  2.00  ",
		},
		Message:    "  " + strings.Repeat("m", 1100) + "  ",
		ObservedAt: &observed,
	}.Status()

	if status.LastUpdateTime == nil || !status.LastUpdateTime.Equal(&observed) {
		t.Fatalf("LastUpdateTime = %#v, want observed", status.LastUpdateTime)
	}
	if status.Placement == nil || status.Placement.WorkerNamespace != "substrate-system" || status.Placement.WorkerPodName != "actor-pod" || len(status.Placement.WorkerPool) != 256 {
		t.Fatalf("sanitized placement = %#v", status.Placement)
	}
	if status.Density == nil || status.Density.WorkerCount != 0 || status.Density.ActorCount != 2 || status.Density.RunningActorCount != 0 || status.Density.SuspendedActorCount != 4 || status.Density.ActorsPerWorker != "2.00" {
		t.Fatalf("sanitized density = %#v", status.Density)
	}
	if !strings.HasSuffix(status.Message, "...<truncated>") || len(status.Message) != len("...<truncated>")+1024 {
		t.Fatalf("sanitized message len=%d suffix=%q", len(status.Message), status.Message[len(status.Message)-15:])
	}
}

func TestUpdateStatusDropsEmptyPlacementAndDensity(t *testing.T) {
	status := Update{
		Provider:  corev1alpha1.WorkspaceProviderAgentSandbox,
		Phase:     corev1alpha1.ExecutionWorkspacePhasePending,
		Reason:    corev1alpha1.ExecutionWorkspaceReasonClaimed,
		Placement: &corev1alpha1.ExecutionWorkspacePlacementStatus{WorkerNamespace: " ", WorkerPool: " ", WorkerPodName: " "},
		Density:   &corev1alpha1.ExecutionWorkspaceDensityStatus{WorkerCount: -1, ActorCount: -1, RunningActorCount: -1, SuspendedActorCount: -1, ActorsPerWorker: " "},
	}.Status()
	if status.Placement != nil || status.Density != nil {
		t.Fatalf("status placement=%#v density=%#v, want nil/nil", status.Placement, status.Density)
	}
}

func TestValidInboundStatusAllowlistsValues(t *testing.T) {
	valid := &corev1alpha1.ExecutionWorkspaceStatus{
		Provider:      corev1alpha1.WorkspaceProviderSubstrate,
		Phase:         corev1alpha1.ExecutionWorkspacePhaseReady,
		Reason:        corev1alpha1.ExecutionWorkspaceReasonReady,
		ReusePolicy:   corev1alpha1.WorkspaceReusePolicySession,
		CleanupPolicy: corev1alpha1.WorkspaceCleanupPolicyDelete,
	}
	if !HasRequiredInboundFields(valid) || !ValidInboundStatus(valid) {
		t.Fatalf("valid status rejected")
	}
	invalid := *valid
	invalid.Provider = "provider-native"
	if ValidInboundStatus(&invalid) {
		t.Fatalf("unsupported provider accepted")
	}
	invalid = *valid
	invalid.CleanupPolicy = "snapshot"
	if ValidInboundStatus(&invalid) {
		t.Fatalf("unsupported cleanup policy accepted")
	}
}

func TestPreserveReadyTelemetry(t *testing.T) {
	previous := &corev1alpha1.ExecutionWorkspaceStatus{
		Phase: corev1alpha1.ExecutionWorkspacePhaseReady,
		Placement: &corev1alpha1.ExecutionWorkspacePlacementStatus{
			WorkerNamespace: "substrate-system",
		},
		Density:       &corev1alpha1.ExecutionWorkspaceDensityStatus{ActorCount: 3},
		ResumeLatency: &metav1.Duration{Duration: 2 * time.Second},
	}
	status := &corev1alpha1.ExecutionWorkspaceStatus{Reason: corev1alpha1.ExecutionWorkspaceReasonCommandFailed}
	PreserveReadyTelemetry(status, previous)
	if status.Placement == nil || status.Density == nil || status.ResumeLatency == nil {
		t.Fatalf("ready telemetry was not preserved: %#v", status)
	}
	status = &corev1alpha1.ExecutionWorkspaceStatus{Reason: corev1alpha1.ExecutionWorkspaceReasonReadinessFailed}
	PreserveReadyTelemetry(status, previous)
	if status.Placement != nil || status.Density != nil || status.ResumeLatency != nil {
		t.Fatalf("unexpected telemetry preservation for readiness failure: %#v", status)
	}
}

func TestCleanupSucceeded(t *testing.T) {
	tests := map[string]struct {
		status *corev1alpha1.ExecutionWorkspaceStatus
		want   bool
	}{
		"nil":                   {status: nil, want: false},
		"retained":              {status: &corev1alpha1.ExecutionWorkspaceStatus{Phase: corev1alpha1.ExecutionWorkspacePhaseRetained, Reason: corev1alpha1.ExecutionWorkspaceReasonRetained}, want: true},
		"deleted":               {status: &corev1alpha1.ExecutionWorkspaceStatus{Phase: corev1alpha1.ExecutionWorkspacePhaseDeleted, Reason: corev1alpha1.ExecutionWorkspaceReasonDeleted}, want: true},
		"released":              {status: &corev1alpha1.ExecutionWorkspaceStatus{Phase: corev1alpha1.ExecutionWorkspacePhaseReleased, Reason: corev1alpha1.ExecutionWorkspaceReasonReleased}, want: true},
		"phase reason mismatch": {status: &corev1alpha1.ExecutionWorkspaceStatus{Phase: corev1alpha1.ExecutionWorkspacePhaseFailed, Reason: corev1alpha1.ExecutionWorkspaceReasonDeleted}, want: false},
		"non cleanup reason":    {status: &corev1alpha1.ExecutionWorkspaceStatus{Phase: corev1alpha1.ExecutionWorkspacePhaseReady, Reason: corev1alpha1.ExecutionWorkspaceReasonReady}, want: false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := CleanupSucceeded(tt.status); got != tt.want {
				t.Fatalf("CleanupSucceeded() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestValidationFailedStatus(t *testing.T) {
	observed := metav1.Now()
	status := ValidationFailedStatus(ValidationFailure{
		Provider:      corev1alpha1.WorkspaceProviderSubstrate,
		TemplateRef:   &corev1alpha1.WorkspaceTemplateReference{Name: "tmpl", Namespace: "default"},
		ReusePolicy:   corev1alpha1.WorkspaceReusePolicyNone,
		CleanupPolicy: corev1alpha1.WorkspaceCleanupPolicyDelete,
		Message:       "  " + strings.Repeat("x", 1100),
		ObservedAt:    &observed,
	})
	if status.Provider != corev1alpha1.WorkspaceProviderSubstrate || status.Phase != corev1alpha1.ExecutionWorkspacePhaseFailed || status.Reason != corev1alpha1.ExecutionWorkspaceReasonValidationFailed {
		t.Fatalf("validation failure status core fields = %#v", status)
	}
	if status.TemplateRef == nil || status.TemplateRef.Name != "tmpl" || status.TemplateRef.Namespace != "default" {
		t.Fatalf("validation failure templateRef = %#v", status.TemplateRef)
	}
	if status.ReusePolicy != corev1alpha1.WorkspaceReusePolicyNone || status.CleanupPolicy != corev1alpha1.WorkspaceCleanupPolicyDelete {
		t.Fatalf("validation failure policies = %q/%q", status.ReusePolicy, status.CleanupPolicy)
	}
	if status.LastUpdateTime == nil || !status.LastUpdateTime.Equal(&observed) {
		t.Fatalf("LastUpdateTime = %#v, want observed", status.LastUpdateTime)
	}
	if !strings.HasSuffix(status.Message, "...<truncated>") {
		t.Fatalf("message was not sanitized/truncated: len=%d", len(status.Message))
	}
}

func TestStatusPolicyHelpers(t *testing.T) {
	if !IsSupportedProvider(corev1alpha1.WorkspaceProviderSubstrate) || IsSupportedProvider("provider-native") {
		t.Fatalf("provider support helper returned unexpected result")
	}
	for _, policy := range []corev1alpha1.WorkspaceReusePolicy{"", corev1alpha1.WorkspaceReusePolicyNone, corev1alpha1.WorkspaceReusePolicySession} {
		if !IsOptionalReusePolicy(policy) {
			t.Fatalf("IsOptionalReusePolicy(%q) = false", policy)
		}
	}
	if IsOptionalReusePolicy("forever") {
		t.Fatalf("unsupported reuse policy accepted")
	}
	for _, policy := range []corev1alpha1.WorkspaceCleanupPolicy{"", corev1alpha1.WorkspaceCleanupPolicyDelete, corev1alpha1.WorkspaceCleanupPolicyRetain} {
		if !IsOptionalCleanupPolicy(policy) {
			t.Fatalf("IsOptionalCleanupPolicy(%q) = false", policy)
		}
	}
	if IsOptionalCleanupPolicy("archive") {
		t.Fatalf("unsupported cleanup policy accepted")
	}
}

func TestStatusPolicyDefaults(t *testing.T) {
	if got, ok := StatusCleanupPolicy("", corev1alpha1.WorkspaceCleanupPolicyDelete); !ok || got != corev1alpha1.WorkspaceCleanupPolicyDelete {
		t.Fatalf("StatusCleanupPolicy(empty, delete) = %q, %t; want delete true", got, ok)
	}
	if got, ok := StatusCleanupPolicy(corev1alpha1.WorkspaceCleanupPolicyRetain, corev1alpha1.WorkspaceCleanupPolicyDelete); !ok || got != corev1alpha1.WorkspaceCleanupPolicyRetain {
		t.Fatalf("StatusCleanupPolicy(retain, delete) = %q, %t; want retain true", got, ok)
	}
	if _, ok := StatusCleanupPolicy("", "archive"); ok {
		t.Fatalf("StatusCleanupPolicy accepted unsupported default")
	}
	if _, ok := StatusCleanupPolicy("archive", corev1alpha1.WorkspaceCleanupPolicyDelete); ok {
		t.Fatalf("StatusCleanupPolicy accepted unsupported explicit policy")
	}
}
