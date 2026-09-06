/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

// Workspace-provider-backed RuntimePools materialize their single runtime
// instance through an externally operated execution-workspace provider
// (Phase 1: kubernetes-sigs Agent Sandbox) instead of a controller-owned
// Deployment. The rendered supervisor Pod template and image allowlist remain
// controller-owned, but the provider-visible template carries no credentials:
// after exact Sandbox materialization is attested, the controller seeds the
// supervisor through the signed one-time bootstrap endpoint. The authenticated
// fence probe, admission gates, drain semantics, and every prompt-level
// operation remain identical to plain pools. Provider-native identifiers
// (claim, sandbox, template names) never enter public Task status; the sanitized
// RuntimePool status is the only projection surface.
//
// Lifecycle mapping onto the provider-neutral execution-workspace contract:
//   - Acquire      -> ensure SandboxTemplate + SandboxWarmPool + SandboxClaim
//     for the exact RuntimePool generation (template revision).
//   - WaitReady    -> claim -> sandbox -> Pod Ready, then the authenticated
//     exact-instance fence probe selects the ActiveInstance.
//   - CreateRuntimeSession / ExecutePrompt / Cancel -> the existing fenced
//     orka.harness.v2 protocol against the ActiveInstance, unchanged.
//   - Drain        -> the existing authenticated supervisor drain.
//   - Delete       -> claim deletion (cascades sandbox + Pod) plus finalizer
//     child cleanup; restart-safe and idempotent.
//   - Recover      -> the existing exact-fence recovery; a missing pool or a
//     replaced instance is already proof of session cleanup.
//   - Suspend/Resume -> not supported by Agent Sandbox; unsupported requests
//     fail closed before any workspace or RuntimePool demand exists.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxcontrollers "sigs.k8s.io/agent-sandbox/controllers"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandboxextv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	sandboxextcontrollers "sigs.k8s.io/agent-sandbox/extensions/controllers"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const (
	runtimePoolSandboxTemplateSuffix             = "sandbox-template"
	runtimePoolSandboxWarmPoolSuffix             = "sandbox-pool"
	runtimePoolSandboxClaimSuffix                = "sandbox-claim"
	runtimePoolSandboxTemplateRevisionAnnotation = "orka.ai/sandbox-template-revision"
)

var (
	errWorkspaceCredentialConflict = errors.New("workspace supervisor credential bootstrap conflict")
	errWorkspaceDurableLineageLost = errors.New("resumed durable workspace lineage is lost")
)

// +kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxtemplates;sandboxwarmpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=agents.x-k8s.io,resources=sandboxes,verbs=get;list;watch;update;patch

func runtimePoolSandboxTemplateName(base string) string {
	return runtimePoolChildName(base, runtimePoolSandboxTemplateSuffix)
}

func runtimePoolSandboxWarmPoolName(base string) string {
	return runtimePoolChildName(base, runtimePoolSandboxWarmPoolSuffix)
}

func runtimePoolSandboxClaimName(base string) string {
	return runtimePoolChildName(base, runtimePoolSandboxClaimSuffix)
}

func validateRuntimePoolExecutionWorkspace(pool *corev1alpha1.RuntimePool) error {
	workspace := pool.Spec.ExecutionWorkspace
	if workspace == nil {
		return nil
	}
	switch workspace.Provider {
	case corev1alpha1.WorkspaceProviderAgentSandbox:
		if workspace.Substrate != nil {
			return fmt.Errorf("spec.executionWorkspace.substrate is only valid for provider substrate")
		}
		if workspace.AgentSandbox != nil {
			if (workspace.AgentSandbox.SuspendMode != "") != (workspace.AgentSandbox.SuspendVolume != nil) {
				return fmt.Errorf("spec.executionWorkspace.agentSandbox.suspendMode and suspendVolume must be set together")
			}
			if volume := workspace.AgentSandbox.SuspendVolume; volume != nil {
				if _, err := validateACPSandboxDurableVolumeShape(
					volume.Capacity,
					volume.AccessModes,
					volume.StorageClassName,
				); err != nil {
					return fmt.Errorf("spec.executionWorkspace.agentSandbox.suspendVolume %w", err)
				}
			}
		}
	case corev1alpha1.WorkspaceProviderSubstrate:
		if workspace.AgentSandbox != nil {
			return fmt.Errorf("spec.executionWorkspace.agentSandbox is only valid for provider agent-sandbox")
		}
		if workspace.Substrate == nil ||
			strings.TrimSpace(workspace.Substrate.BaseTemplateNamespace) == "" ||
			strings.TrimSpace(workspace.Substrate.BaseTemplateName) == "" {
			return fmt.Errorf("spec.executionWorkspace.substrate must name the operator-owned infrastructure ActorTemplate")
		}
	default:
		return fmt.Errorf("spec.executionWorkspace.provider %q is not supported", workspace.Provider)
	}
	if !validSHA256Digest(workspace.BindingDigest) {
		return fmt.Errorf("spec.executionWorkspace.bindingDigest must be a sha256 digest")
	}
	if pool.Spec.Capacity == nil || pool.Spec.Capacity.MaxResidentSessions != 1 || pool.Spec.Capacity.MaxRunningPrompts != 1 {
		return fmt.Errorf("workspace-backed RuntimePools host exactly one resident RuntimeSession; spec.capacity must be 1/1")
	}
	return nil
}

func validateRuntimePoolExecutionWorkspaceNamespace(
	pool *corev1alpha1.RuntimePool,
	runtimeNamespace string,
) error {
	if pool == nil || pool.Spec.ExecutionWorkspace == nil ||
		pool.Spec.ExecutionWorkspace.Provider != corev1alpha1.WorkspaceProviderSubstrate ||
		pool.Spec.ExecutionWorkspace.Substrate == nil {
		return nil
	}
	return validateSubstrateTemplateRuntimeNamespace(
		pool.Spec.ExecutionWorkspace.Substrate.BaseTemplateNamespace,
		runtimeNamespace,
	)
}

func validateSubstrateTemplateRuntimeNamespace(templateNamespace, runtimeNamespace string) error {
	templateNamespace = strings.TrimSpace(templateNamespace)
	runtimeNamespace = strings.TrimSpace(runtimeNamespace)
	if templateNamespace != "" && templateNamespace == runtimeNamespace {
		return fmt.Errorf("substrate infrastructure template namespace must differ from the resolved runtime namespace so provider templates cannot resolve RuntimePool Secrets")
	}
	return nil
}

// reconcileWorkspaceBackedRuntimePool converges a workspace-provider-backed
// pool. It mirrors the Deployment path exactly, replacing only workload
// materialization: SandboxTemplate + SandboxWarmPool render the supervisor Pod
// and one SandboxClaim hosts the single exact instance.
//
//nolint:gocyclo // Workload materialization decisions stay auditable together, mirroring the Deployment path.
func (r *RuntimePoolReconciler) reconcileWorkspaceBackedRuntimePool(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	authSecret *corev1.Secret,
	providerSecret *corev1.Secret,
) (ctrl.Result, error) {
	sandboxTemplate, err := r.getRuntimePoolSandboxTemplate(ctx, cfg)
	if err != nil {
		return r.finishWorkspacePoolProviderReadFailure(ctx, pool, cfg, err)
	}
	claim, err := r.getRuntimePoolSandboxClaim(ctx, cfg)
	if err != nil {
		return r.finishWorkspacePoolProviderReadFailure(ctx, pool, cfg, err)
	}
	pods, err := r.listWorkspaceRuntimePoolPods(ctx, pool, cfg)
	if err != nil {
		return ctrl.Result{}, err
	}

	status := r.baseRuntimePoolStatus(pool, countRuntimePoolPods(pods))
	if pool.DeletionTimestamp.IsZero() && sandboxConsensualSuspendRecordMalformed(pool) {
		// A nonempty checkpoint annotation means the provider suspension may
		// already have been requested. Treating malformed metadata as absent
		// would let ordinary scale-down or rollout delete the claim and its
		// durable PVC. Only explicit workspace deletion may clean it up.
		status.ActiveInstance = pool.Status.ActiveInstance
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "recorded Sandbox suspension checkpoint metadata is malformed; retaining the provider claim and durable volume until explicit deletion"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	if pool.DeletionTimestamp.IsZero() && sandboxDurableLineageRecordMalformed(pool) {
		status.ActiveInstance = pool.Status.ActiveInstance
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "recorded durable workspace lineage metadata is malformed; retaining the provider claim and durable volume until explicit deletion"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	durableLineage := sandboxRecordedDurableLineage(pool) != nil
	durableClaimProtected := sandboxSuspendRecordAnnotationPresent(pool) || durableLineage
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionPodSecurityReady, metav1.ConditionTrue, "PodSecurityConfigured", "runtime Pod security controls are configured")
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionQuotaReady, metav1.ConditionTrue, "ResourcesAdmitted", "runtime resources were admitted")

	if claim != nil && !runtimePoolSandboxChildOwnedByPool(claim, pool, cfg) {
		return r.finishWorkspacePoolFailurePreservingDurableState(
			ctx,
			pool,
			cfg,
			"provider SandboxClaim ownership validation failed",
			fmt.Errorf("same-name SandboxClaim does not carry the exact RuntimePool ownership identity"),
		)
	}
	if claim != nil && !runtimePoolSandboxClaimMatchesPool(claim, pool, cfg) {
		// The permanent durable-lineage fence outlives the consent record:
		// after a cold resume reaches Serving, the claim's PVC is the SOLE
		// copy of the resumed data, and a drifted claim must degrade
		// fail-closed instead of being deleted and replaced blank.
		preserveDriftedClaim := durableClaimProtected
		if !preserveDriftedClaim && pool.DeletionTimestamp.IsZero() {
			// The consent record is written only after authentication, drain,
			// and the checkpoint request: a claim that drifts (for example a
			// provider webhook rewriting its warm-pool reference or PVC
			// template) in the interval while suspension intent is pending
			// must be preserved too, or the deletion below cascades the
			// durable PVC before the suspension state machine ever runs.
			if sandboxWorkspaceSuspendRequested(pool) {
				preserveDriftedClaim = true
			} else if pending, pendingErr := r.linkedWorkspaceSuspendIntentPending(ctx, pool); pendingErr != nil {
				return ctrl.Result{}, pendingErr
			} else if pending {
				preserveDriftedClaim = true
			}
		}
		if preserveDriftedClaim && pool.DeletionTimestamp.IsZero() {
			// The drifted claim still holds (or is about to hold) the only
			// preserved copy of a consensually suspended workspace, and
			// provider deletion cascades to the durable PVC. Degrade
			// fail-closed while retaining the claim; only explicit workspace
			// deletion may destroy it.
			status.ActiveInstance = pool.Status.ActiveInstance
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = "provider SandboxClaim drifted from the controller-owned binding; retaining the consensually suspended workspace claim"
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		if err := r.deleteRuntimePoolSandboxClaim(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
		status.ActiveInstance = nil
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "provider SandboxClaim contents do not match the controller-owned RuntimePool binding; recycling it before use"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}
	claimFailed, err := r.applySandboxClaimFailureConditions(
		ctx, pool, claim, &status, sandboxConsensualSuspendRecord(pool) != nil,
	)
	if err != nil {
		return ctrl.Result{}, err
	}
	if claimFailed && pool.Spec.DesiredReplicas != 0 {
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	if claim == nil && durableLineage && pool.DeletionTimestamp.IsZero() && pool.Spec.DesiredReplicas != 0 &&
		strings.TrimSpace(pool.Annotations[runtimePoolWorkspaceResumeLostAnnotation]) == "" {
		// A successfully resumed lineage permanently pins its one owning
		// SandboxClaim. Once that claim disappears, recreating the deterministic
		// name would provision a blank PVC under the old workspace identity.
		if err := r.patchRuntimePoolAnnotation(ctx, pool, runtimePoolWorkspaceResumeLostAnnotation,
			"the SandboxClaim holding the resumed durable workspace lineage is missing"); err != nil {
			return ctrl.Result{}, err
		}
	}
	if strings.TrimSpace(pool.Annotations[runtimePoolWorkspaceResumeLostAnnotation]) != "" && pool.Spec.DesiredReplicas != 0 {
		if sandboxSuspendRecordAnnotationPresent(pool) {
			// Resume-loss cleanup is write-ahead: the terminal annotation is
			// persisted before deleting the claim. If that deletion failed or
			// remained in progress, keep retrying it before the terminal status
			// short-circuit below. The checkpoint record remains the exact
			// Sandbox finalization identity after the claim is absent; pool
			// deletion retires it only with the pool after that Sandbox is gone.
			if claim != nil {
				if err := r.deleteRuntimePoolSandboxClaim(ctx, claim); err != nil {
					return ctrl.Result{}, err
				}
				status.ActiveInstance = nil
				status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
				status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
				status.Message = "durable workspace data was lost during cold resume; deleting the failed provider claim"
				return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
			}
		}
		// The durable data of a consensually suspended workspace was lost;
		// the failure is terminal and the pool never reprovisions a fresh
		// claim, or the continuation would silently execute against a new
		// PVC. Scale-down and deletion still proceed normally.
		status.ActiveInstance = nil
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "durable workspace data was lost during a cold resume; the workspace fails closed and is never reprovisioned"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	readyPods := readyRuntimePoolPods(pods)
	if len(readyPods) > 1 {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleAmbiguous
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionAmbiguous
		status.ActiveInstance = nil
		preserveSuspendFence := sandboxWorkspaceSuspendRequested(pool)
		if !preserveSuspendFence && pool.Status.ActiveInstance != nil {
			pending, pendingErr := r.linkedWorkspaceSuspendIntentPending(ctx, pool)
			if pendingErr != nil {
				return ctrl.Result{}, pendingErr
			}
			preserveSuspendFence = pending
		}
		if preserveSuspendFence && pool.Status.ActiveInstance != nil {
			// A transient multi-Pod blip while suspension is requested must
			// keep the suspend fence, including the interval before the
			// adapter stamps the pool annotation. Clearing the admitted
			// identity would make the converged Pod set fall through to
			// unadmitted scale-down and delete the SandboxClaim plus durable
			// PVC without an authenticated checkpoint.
			status.ActiveInstance = pool.Status.ActiveInstance
		}
		status.Message = fmt.Sprintf("found %d Ready runtime Pods; exact-instance admission is closed", len(readyPods))
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRuntimeAmbiguous, status.Message)
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRuntimeAmbiguous, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	if pool.Spec.DesiredReplicas == 0 && sandboxWorkspaceSuspendRequested(pool) {
		if !pool.DeletionTimestamp.IsZero() {
			// Explicit deletion never waits for a provider checkpoint to
			// settle. The suspension record is written only after the
			// authenticated Quiescent barrier persisted, so deletion can
			// proceed through ordinary destructive scale-down even when the
			// Sandbox API is unavailable.
			return r.reconcileWorkspaceRuntimePoolScaleDown(ctx, pool, cfg, claim, pods, readyPods, status)
		}
		return r.reconcileWorkspaceRuntimePoolSuspend(ctx, pool, cfg, claim, pods, readyPods, status)
	}
	if pool.Spec.DesiredReplicas == 0 {
		if suspendPending, err := r.linkedWorkspaceSuspendIntentPending(ctx, pool); err != nil {
			return ctrl.Result{}, err
		} else if suspendPending {
			// The linked workspace was patched to Suspended but the adapter
			// has not recorded the pool's suspension intent yet (the idle
			// reaper's scale-to-zero or a restart can land first). Ordinary
			// scale-down here would delete the SandboxClaim and cascade the
			// durable PVC away, destroying the data the class froze a
			// Suspend action for; wait for the durable intent instead. The
			// admitted-identity fence is PRESERVED across the wait: clearing
			// ActiveInstance would send the suspend flow that follows the
			// recorded intent into unadmitted scale-down, deleting the claim
			// and durable PVC without a checkpoint.
			status.ActiveInstance = pool.Status.ActiveInstance
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
			status.Message = "linked workspace requests suspension; waiting for the durable pool suspension intent before any teardown"
			return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
		}
	}
	if pool.Spec.DesiredReplicas == 0 {
		// Scale-down depends on the independently ownership-validated claim and
		// exact runtime Pod, not mutable SandboxTemplate metadata. Always let an
		// idle pool drain and delete its live claim before surfacing template
		// ownership drift that matters only to rollout or replacement.
		return r.reconcileWorkspaceRuntimePoolScaleDown(ctx, pool, cfg, claim, pods, readyPods, status)
	}
	if !r.AgentSandboxEnabled {
		if claim == nil || len(readyPods) == 0 {
			status.ActiveInstance = nil
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		}
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "agent-sandbox provider is disabled; existing RuntimePool admission remains closed until it scales to zero"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	if record := sandboxConsensualSuspendRecord(pool); record != nil && pool.Status.Lifecycle == corev1alpha1.RuntimePoolLifecycleServing {
		// The resumed instance passed the authenticated exact-instance Serving
		// fence and that admission is durably persisted; only now does the
		// checkpoint record retire. Any earlier failure (a failed post-seed
		// probe, an unhealthy resumed supervisor) keeps the record standing so
		// recycling paths preserve the claim and its preserved PVC. The
		// permanent lineage fence is stamped BEFORE the consent retires: the
		// claim now holds the sole copy of the resumed durable data for the
		// pool's remaining lifetime, and later recycling paths (an unhealthy
		// resumed supervisor, identity exhaustion) must fail closed instead
		// of replacing the preserved PVC with a blank one under the same
		// pool identity.
		lineage, err := json.Marshal(sandboxDurableLineageRecord{
			Name:   record.Name,
			UID:    record.UID,
			PVCUID: record.PVCUID,
			PVName: record.PVName,
			PVUID:  record.PVUID,
		})
		if err != nil {
			return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, fmt.Errorf("encode durable workspace lineage record: %w", err))
		}
		if err := r.patchRuntimePoolAnnotation(ctx, pool, runtimePoolDurableLineageAnnotation, string(lineage)); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.patchRuntimePoolAnnotation(ctx, pool, sandboxSuspendedAnnotation, ""); err != nil {
			return ctrl.Result{}, err
		}
		// Re-read the permanent lineage before any claim materialization path.
		// The local durableLineage value predates the annotation patch, so
		// continuing here could replace a claim that disappeared during this
		// transition with a blank durable volume.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if len(pods) == 0 && (claim == nil || sandboxAwaitingWorkspaceResume(pool)) {
		rotating, rotateErr := r.rotateConsumedWorkspaceRuntimePoolAuthSecret(ctx, pool, cfg, authSecret)
		if rotateErr != nil {
			return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, rotateErr)
		}
		if rotating {
			status.ActiveInstance = nil
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStarting
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = "rotating one-time credential bootstrap material before acquiring a replacement workspace"
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
		}
	}

	selector := map[string]string{runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel]}
	desiredTemplate := r.runtimePoolPodTemplate(pool, cfg, selector, authSecret.Name, providerSecret.Name)
	bootstrapNonce := strings.TrimSpace(string(authSecret.Data[runtimePoolBootstrapNonceKey]))
	if bootstrapNonce == "" {
		return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, fmt.Errorf("RuntimePool auth Secret is missing the credential bootstrap nonce"))
	}
	bootstrapPublicKey, err := harnessv2.CredentialBootstrapPublicKey(authSecret.Data[runtimePoolBootstrapSigningSeedKey])
	if err != nil {
		return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, fmt.Errorf("derive RuntimePool credential bootstrap public key: %w", err))
	}
	if sandboxRuntimePoolSuspendCapable(pool) {
		desiredTemplate = runtimePoolDurableWorkspaceTemplate(desiredTemplate)
	}
	desiredTemplate = runtimePoolWorkspaceBootstrapTemplate(desiredTemplate, bootstrapNonce, bootstrapPublicKey)

	if sandboxTemplate != nil {
		if !runtimePoolSandboxChildOwnedByPool(sandboxTemplate, pool, cfg) {
			return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, fmt.Errorf("same-name SandboxTemplate does not carry the exact RuntimePool ownership identity"))
		}
		trustedRevision := strings.TrimSpace(pool.Annotations[runtimePoolSandboxTemplateRevisionAnnotation])
		observedRevision, revisionErr := runtimePoolSandboxTemplateObjectRevision(sandboxTemplate)
		desiredRevision := runtimePoolSandboxTemplateSpecRevision(runtimePoolSandboxTemplateSpec(desiredTemplate, sandboxRuntimePoolSuspendCapable(pool)))
		if trustedRevision == "" {
			if claim != nil && durableClaimProtected {
				// The claim holds the only preserved copy of a consensually
				// suspended workspace; a lost integrity annotation is
				// operational metadata damage, not license to cascade the
				// durable PVC away. Degrade fail-closed while retaining the
				// claim, exactly as the trusted-revision-mismatch path does
				// during a resume.
				status.ActiveInstance = pool.Status.ActiveInstance
				status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
				status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
				status.Message = "provider SandboxTemplate integrity record is missing; retaining the protected durable workspace claim"
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
				return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
			}
			if claim != nil || len(pods) > 0 {
				if claim != nil {
					if err := r.deleteRuntimePoolSandboxClaim(ctx, claim); err != nil {
						return ctrl.Result{}, err
					}
				}
				status.ActiveInstance = nil
				status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
				status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
				status.Message = "provider SandboxTemplate has no controller-owned integrity record; recycling its workspace before trust is established"
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
				return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
			}
			if revisionErr != nil || observedRevision != desiredRevision {
				if err := r.updateRuntimePoolSandboxTemplate(ctx, sandboxTemplate, desiredTemplate, sandboxRuntimePoolSuspendCapable(pool)); err != nil {
					return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
				}
			}
			if err := r.setRuntimePoolSandboxTemplateRevision(ctx, pool, desiredRevision); err != nil {
				return ctrl.Result{}, err
			}
		} else if revisionErr != nil || observedRevision != trustedRevision {
			if claim != nil && durableClaimProtected && len(pods) > 0 {
				// A live resumed or resuming lineage cannot use ordinary template
				// repair because deleting the claim would cascade the only durable
				// PVC. Hold admission closed until the Pod drains or the workspace
				// is explicitly deleted.
				status.ActiveInstance = pool.Status.ActiveInstance
				status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
				status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
				status.Message = "provider SandboxTemplate failed its controller-owned integrity check; retaining the live durable workspace claim"
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
				return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
			}
			repairInPlace := durableClaimProtected && len(pods) == 0
			if claim != nil && !repairInPlace {
				if err := r.deleteRuntimePoolSandboxClaim(ctx, claim); err != nil {
					return ctrl.Result{}, err
				}
			}
			if (claim != nil && !repairInPlace) || len(pods) > 0 {
				status.ActiveInstance = nil
				status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
				status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
				status.Message = "provider SandboxTemplate failed its controller-owned integrity check; recycling the workspace before template repair"
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
				return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
			}
			if observedRevision != desiredRevision {
				if err := r.updateRuntimePoolSandboxTemplate(ctx, sandboxTemplate, desiredTemplate, sandboxRuntimePoolSuspendCapable(pool)); err != nil {
					return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
				}
			}
			if err := r.setRuntimePoolSandboxTemplateRevision(ctx, pool, desiredRevision); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	if sandboxTemplate != nil && runtimePoolSandboxTemplateNeedsRollout(sandboxTemplate, desiredTemplate) {
		if durableClaimProtected && len(pods) > 0 {
			// A live resumed or resuming workspace must NEVER enter ordinary rollout: the
			// rollout deletes the SandboxClaim, cascading the sole preserved
			// PVC. Before Serving, the checkpoint record provides this fence;
			// afterward, the permanent lineage record does. Hold Degraded with
			// the claim retained until the pool drains or the workspace is
			// deleted explicitly.
			status.ActiveInstance = pool.Status.ActiveInstance
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = "the derived template changed for a live durable workspace; holding the preserved claim instead of rolling out"
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		if durableClaimProtected && len(pods) == 0 {
			// A consensual cold resume rebuilds the replacement Pod with the
			// rotated bootstrap material, so the derived template is replaced
			// in place instead of rolling out — a rollout would delete the
			// claim whose durable PVC is the whole point of the suspension.
			// The Sandbox has no Pod while suspended, so nothing can observe
			// the template transition.
			if err := r.updateRuntimePoolSandboxTemplate(ctx, sandboxTemplate, desiredTemplate, sandboxRuntimePoolSuspendCapable(pool)); err != nil {
				return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
			}
			if err := r.setRuntimePoolSandboxTemplateRevision(
				ctx, pool, runtimePoolSandboxTemplateSpecRevision(runtimePoolSandboxTemplateSpec(desiredTemplate, sandboxRuntimePoolSuspendCapable(pool))),
			); err != nil {
				return ctrl.Result{}, err
			}
			status.ActiveInstance = nil
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStarting
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = "refreshing the provider workspace template with rotated bootstrap material before cold resume"
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
		}
		return r.reconcileWorkspaceRuntimePoolRollout(ctx, pool, cfg, sandboxTemplate, claim, pods, desiredTemplate, status)
	}

	if sandboxTemplate == nil {
		if err := r.createRuntimePoolSandboxTemplate(ctx, pool, cfg, desiredTemplate); err != nil {
			return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
		}
		if sandboxTemplate, err = r.getRuntimePoolSandboxTemplate(ctx, cfg); err != nil || sandboxTemplate == nil {
			if err == nil {
				err = fmt.Errorf("created RuntimePool sandbox template is not readable yet")
			}
			return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
		}
		if !runtimePoolSandboxChildOwnedByPool(sandboxTemplate, pool, cfg) {
			return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, fmt.Errorf("created SandboxTemplate does not carry the exact RuntimePool ownership identity"))
		}
		desiredRevision := runtimePoolSandboxTemplateSpecRevision(runtimePoolSandboxTemplateSpec(desiredTemplate, sandboxRuntimePoolSuspendCapable(pool)))
		observedRevision, revisionErr := runtimePoolSandboxTemplateObjectRevision(sandboxTemplate)
		if revisionErr != nil || observedRevision != desiredRevision {
			return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, fmt.Errorf("created SandboxTemplate contents do not match the controller-rendered RuntimePool template"))
		}
		if err := r.setRuntimePoolSandboxTemplateRevision(ctx, pool, desiredRevision); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.ensureRuntimePoolSandboxWarmPool(ctx, pool, cfg); err != nil {
		return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
	}
	if err := r.pruneStaleWorkspaceRuntimePoolSecrets(ctx, pool, cfg, sandboxTemplate, authSecret.Name, providerSecret.Name); err != nil {
		return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
	}

	if record := sandboxConsensualSuspendRecord(pool); record != nil {
		done, result, err := r.resumeSuspendedWorkspaceSandbox(ctx, pool, cfg, claim, sandboxTemplate, desiredTemplate, readyPods, &status, record)
		if !done {
			return result, err
		}
	}
	if claim != nil && !claim.DeletionTimestamp.IsZero() {
		status.ActiveInstance = nil
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "replaced workspace claim is terminating before a fresh provider workspace is acquired"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}
	if claim == nil {
		if err := r.createRuntimePoolSandboxClaim(ctx, pool, cfg); err != nil {
			return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
		}
		r.applyProviderRuntimePoolColdStartStatus(pool, &status, "waiting to observe the new provider SandboxClaim before credential bootstrap")
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}
	if len(readyPods) == 1 {
		materialized, err := r.attestWorkspaceRuntimePoolMaterialization(ctx, pool, claim, sandboxTemplate, &readyPods[0])
		if err != nil {
			if durableClaimProtected {
				if errors.Is(err, errWorkspaceDurableLineageLost) &&
					strings.TrimSpace(pool.Annotations[runtimePoolWorkspaceResumeLostAnnotation]) == "" {
					if annotationErr := r.patchRuntimePoolAnnotation(ctx, pool, runtimePoolWorkspaceResumeLostAnnotation,
						sanitizeRuntimePoolMessage(err.Error())); annotationErr != nil {
						return ctrl.Result{}, annotationErr
					}
				}
				// The claim holds the only preserved copy of the suspended
				// workspace; a rejected resumed instance (for example an
				// admission webhook mutating the Pod while suspended)
				// degrades fail-closed WITHOUT deleting the claim and its
				// PVC, so the data survives for a corrected resume.
				status.ActiveInstance = nil
				status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
				status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
				status.Message = sanitizeRuntimePoolMessage("resumed provider workspace failed attestation; retaining the preserved claim: " + err.Error())
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
				return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
			}
			if deleteErr := r.deleteRuntimePoolSandboxClaim(ctx, claim); deleteErr != nil {
				return ctrl.Result{}, errors.Join(err, deleteErr)
			}
			status.ActiveInstance = nil
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = sanitizeRuntimePoolMessage("provider workspace materialization does not match the validated controller template: " + err.Error())
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
		}
		if !materialized {
			r.applyProviderRuntimePoolColdStartStatus(pool, &status, "waiting for the provider Sandbox materialization record before credential bootstrap")
			return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
		}
		if err := r.bindWorkspaceRuntimePoolBootstrapInstance(ctx, pool, authSecret, readyPods[0].UID); err != nil {
			if errors.Is(err, errRuntimePoolBootstrapInstanceConflict) {
				if durableClaimProtected {
					// The claim still holds the only preserved copy of the
					// suspended workspace; a resume-specific instance conflict
					// degrades without deleting it.
					status.ActiveInstance = nil
					status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
					status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
					status.Message = "resumed provider workspace instance conflicted before credential bootstrap; retaining the preserved claim"
					r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
					r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
					return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
				}
				if deleteErr := r.deleteRuntimePoolSandboxClaim(ctx, claim); deleteErr != nil {
					return ctrl.Result{}, errors.Join(err, deleteErr)
				}
				status.ActiveInstance = nil
				status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
				status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
				status.Message = "provider workspace physical instance changed; recycling it before credential bootstrap rotation"
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
				return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
			}
			return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
		}
		alreadyComplete, seedErr := r.seedWorkspaceSupervisorCredentials(
			ctx, runtimePoolInstanceEndpoint(pool, &readyPods[0]), bootstrapNonce, authSecret, providerSecret,
		)
		if errors.Is(seedErr, errWorkspaceCredentialConflict) {
			if durableClaimProtected {
				status.ActiveInstance = nil
				status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
				status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
				status.Message = "resumed provider workspace was credential-seeded by another party; retaining the preserved claim"
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
				r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
				return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
			}
			if deleteErr := r.deleteRuntimePoolSandboxClaim(ctx, claim); deleteErr != nil {
				return ctrl.Result{}, errors.Join(seedErr, deleteErr)
			}
			status.ActiveInstance = nil
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = "provider workspace was credential-seeded by another party; recycling the exact instance"
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
		}
		if seedErr != nil {
			r.applyProviderRuntimePoolColdStartStatus(pool, &status, sanitizeRuntimePoolMessage("credential bootstrap is not complete: "+seedErr.Error()))
			return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
		}
		if alreadyComplete {
			// A 404 means the one-time bootstrap listener has already handed off
			// to the authenticated supervisor. The exact fence probe below is
			// still required before this instance can be admitted.
			status.Message = "provider workspace credential bootstrap already completed; verifying the exact authenticated instance"
		}
	}
	return r.reconcileRuntimePoolServing(ctx, pool, cfg, pods, readyPods, authSecret, status)
}

// attestDurableWorkspacePVC verifies the realized durable workspace PVC:
// owned by the exact adopted Sandbox, spec matching the frozen pool binding,
// and no foreign data source.
//
//nolint:gocyclo // Durable PVC attestation keeps all fail-closed identity and lineage checks in one auditable boundary.
func (r *RuntimePoolReconciler) attestDurableWorkspacePVC(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	sandbox *sandboxv1beta1.Sandbox,
	pod *corev1.Pod,
) error {
	if sandboxDurableLineageRecordMalformed(pool) {
		return fmt.Errorf("recorded durable workspace lineage metadata is malformed")
	}
	checkpoint := sandboxConsensualSuspendRecord(pool)
	lineage := sandboxRecordedDurableLineage(pool)
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.sandboxReader().Get(ctx, types.NamespacedName{
		Namespace: sandbox.Namespace, Name: injectedDurableWorkspaceClaimName(sandbox),
	}, pvc); err != nil {
		if apierrors.IsNotFound(err) && lineage != nil {
			return fmt.Errorf("%w: realized durable workspace PVC is missing", errWorkspaceDurableLineageLost)
		}
		return fmt.Errorf("read the realized durable workspace PVC: %w", err)
	}
	if !metav1.IsControlledBy(pvc, sandbox) {
		return fmt.Errorf("realized durable workspace PVC is not controller-owned by the exact provider Sandbox")
	}
	if pvc.DeletionTimestamp != nil {
		// A deleted PVC held only by pvc-protection while its Pod runs is
		// already irreversibly going away: once the Pod stops or
		// cold-suspends, protection releases and the session workspace
		// vanishes. Admitting the runtime against it would seed credentials
		// into doomed storage.
		if lineage != nil {
			return fmt.Errorf("%w: realized durable workspace PVC is terminating", errWorkspaceDurableLineageLost)
		}
		return fmt.Errorf("realized durable workspace PVC is terminating; its deletion is already irreversible")
	}
	// The pinned StorageClass identity is re-verified at the first successful
	// materialization attestation, even if provisioning raced ahead and bound
	// the PVC before this reconcile. The exact Pod's bootstrap binding is
	// persisted immediately after attestation and proves later passes are
	// observing already-established storage. Checkpoint and lineage records
	// independently pin their exact PVC/PV identities. Those established
	// volumes no longer depend on the live StorageClass object, so retiring or
	// replacing the class later cannot invalidate valid workspace data.
	bootstrapBinding, err := runtimePoolBootstrapInstanceBindingFromAnnotation(pool)
	if err != nil {
		return err
	}
	attestationPersisted := bootstrapBinding != nil && pod != nil && bootstrapBinding.WorkloadUID == pod.UID
	if checkpoint == nil && lineage == nil && !attestationPersisted {
		if err := r.verifyPinnedDurableStorageClass(ctx, pool); err != nil {
			return err
		}
	}
	expected, err := runtimePoolDurableVolumeClaimTemplate(pool)
	if err != nil {
		return err
	}
	if !apiequality.Semantic.DeepEqual(expected.Spec.AccessModes, pvc.Spec.AccessModes) {
		return fmt.Errorf("realized durable workspace PVC access modes differ from the frozen binding")
	}
	expectedStorage := expected.Spec.Resources.Requests[corev1.ResourceStorage]
	realizedStorage := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if expectedStorage.Cmp(realizedStorage) != 0 {
		return fmt.Errorf("realized durable workspace PVC storage request differs from the frozen binding")
	}
	if expected.Spec.StorageClassName != nil &&
		(pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != *expected.Spec.StorageClassName) {
		return fmt.Errorf("realized durable workspace PVC storage class differs from the frozen binding")
	}
	if pvc.Spec.DataSource != nil || pvc.Spec.DataSourceRef != nil {
		return fmt.Errorf("realized durable workspace PVC carries a foreign data source")
	}
	// The runtime populates only volumeName on bind; every other selector or
	// attribute the frozen template never declared must stay unset, or a
	// mutated claim could bind a pre-existing matching PV the binding never
	// authorized.
	if pvc.Spec.Selector != nil {
		return fmt.Errorf("realized durable workspace PVC carries a label selector the frozen binding never declared")
	}
	if pvc.Spec.VolumeAttributesClassName != nil && *pvc.Spec.VolumeAttributesClassName != "" {
		return fmt.Errorf("realized durable workspace PVC carries a volume attributes class the frozen binding never declared")
	}
	if checkpoint != nil && pvc.UID != checkpoint.PVCUID {
		return fmt.Errorf("realized durable workspace PVC is not the recorded suspended checkpoint volume")
	}
	if lineage != nil && pvc.UID != lineage.PVCUID {
		return fmt.Errorf("%w: realized durable workspace PVC is not the recorded resumed-lineage volume", errWorkspaceDurableLineageLost)
	}
	// The bound PV itself must be dynamically provisioned for exactly this
	// claim with Delete reclaim semantics: a webhook-set volumeName could
	// otherwise bind a pre-existing (possibly Retain) PV whose foreign data
	// the runtime would serve and whose storage would survive teardown.
	volumeName := strings.TrimSpace(pvc.Spec.VolumeName)
	if checkpoint != nil && volumeName != checkpoint.PVName {
		return fmt.Errorf("realized durable workspace PV is not the recorded suspended checkpoint volume")
	}
	if lineage != nil && volumeName != lineage.PVName {
		return fmt.Errorf("%w: realized durable workspace PV is not the recorded resumed-lineage volume", errWorkspaceDurableLineageLost)
	}
	if volumeName != "" {
		pv := &corev1.PersistentVolume{}
		if err := r.sandboxReader().Get(ctx, types.NamespacedName{Name: volumeName}, pv); err != nil {
			if apierrors.IsNotFound(err) && lineage != nil {
				return fmt.Errorf("%w: bound durable workspace PV is missing", errWorkspaceDurableLineageLost)
			}
			return fmt.Errorf("read the bound durable workspace PV: %w", err)
		}
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID {
			if lineage != nil {
				return fmt.Errorf("%w: bound durable workspace PV is no longer claimed by the recorded PVC", errWorkspaceDurableLineageLost)
			}
			return fmt.Errorf("bound durable workspace PV is not claimed by the exact realized PVC")
		}
		if checkpoint != nil && pv.UID != checkpoint.PVUID {
			return fmt.Errorf("bound durable workspace PV is not the recorded suspended checkpoint volume")
		}
		if lineage != nil && pv.UID != lineage.PVUID {
			return fmt.Errorf("%w: bound durable workspace PV is not the recorded resumed-lineage volume", errWorkspaceDurableLineageLost)
		}
		if pv.Annotations["pv.kubernetes.io/provisioned-by"] == "" {
			return fmt.Errorf("bound durable workspace PV was not dynamically provisioned; static prebinding is not authorized")
		}
		if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
			return fmt.Errorf("bound durable workspace PV reclaim policy %q violates the all-Delete lifecycle", pv.Spec.PersistentVolumeReclaimPolicy)
		}
		expectedClass := pool.Spec.ExecutionWorkspace.AgentSandbox.SuspendVolume.StorageClassName
		if expectedClass != "" && pv.Spec.StorageClassName != expectedClass {
			return fmt.Errorf("bound durable workspace PV storage class %q differs from the frozen binding", pv.Spec.StorageClassName)
		}
	}
	return nil
}

func (r *RuntimePoolReconciler) listWorkspaceRuntimePoolPods(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := r.List(ctx, list, client.InNamespace(cfg.namespace), client.MatchingLabels{
		runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel],
		runtimePoolUIDLabel: string(pool.UID),
	}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// reconcileWorkspaceRuntimePoolMissingAuthSecret destroys the exact provider
// workspace before unpublishing a binding whose immutable Secret disappeared.
// The next normal reconcile creates fresh credentials and reuses the existing
// consumed-bootstrap rotation barrier before acquiring a replacement claim.
func (r *RuntimePoolReconciler) reconcileWorkspaceRuntimePoolMissingAuthSecret(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) (ctrl.Result, error) {
	claim, err := r.getRuntimePoolSandboxClaim(ctx, cfg)
	if err != nil {
		return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
	}
	pods, err := r.listWorkspaceRuntimePoolPods(ctx, pool, cfg)
	if err != nil {
		return ctrl.Result{}, err
	}
	status := r.baseRuntimePoolStatus(pool, countRuntimePoolPods(pods))
	status.ActiveInstance = nil
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, "bound runtime credentials are unavailable")

	if claim != nil && !runtimePoolSandboxChildOwnedByPool(claim, pool, cfg) {
		return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, fmt.Errorf("same-name SandboxClaim does not carry the exact RuntimePool ownership identity"))
	}
	authLossPreservesClaim := sandboxSuspendRecordAnnotationPresent(pool) ||
		sandboxWorkspaceSuspendRequested(pool) ||
		sandboxDurableLineageAnnotationPresent(pool)
	if claim != nil && !authLossPreservesClaim && pool.DeletionTimestamp.IsZero() {
		// The consent record is written only after the checkpoint request; a
		// bound-Secret loss in the window where the workspace already
		// requested suspension (durable pool intent pending) must equally
		// preserve the claim, or the deletion below cascades the durable PVC
		// away before the suspension state machine ever runs.
		if pending, pendingErr := r.linkedWorkspaceSuspendIntentPending(ctx, pool); pendingErr != nil {
			return ctrl.Result{}, pendingErr
		} else if pending {
			authLossPreservesClaim = true
		}
	}
	if claim != nil && authLossPreservesClaim && pool.DeletionTimestamp.IsZero() {
		// The claim holds the only preserved copy of a consensually suspended
		// workspace; an operational credential Secret loss must not destroy
		// it. Degrade fail-closed so the credentials can be replaced (or the
		// workspace deleted explicitly) without losing the data. A DELETING
		// pool proceeds: the Delete lifecycle must remain executable even
		// with the credentials gone, or the finalizer could never remove the
		// claim and PVC and the linked workspace would be stuck deleting.
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.Message = "bound runtime credentials are unavailable; retaining the consensually suspended workspace claim"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	if claim != nil {
		if err := r.deleteRuntimePoolSandboxClaim(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
	}
	if claim != nil || len(pods) > 0 {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
		status.Message = "bound runtime credentials disappeared; destroying the exact provider workspace before credential rotation"
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if err := r.patchRuntimePoolAnnotation(
		ctx,
		pool,
		runtimePoolPrivateAuthSecretBindingAnnotation(cfg.controllerEpoch),
		"",
	); err != nil {
		return ctrl.Result{}, err
	}
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStarting
	if pool.Spec.DesiredReplicas == 0 {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
	}
	status.Message = "provider workspace absence is proven; rotating credentials before any replacement"
	return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
}

// reconcileWorkspaceRuntimePoolRollout mirrors the Deployment Recreate rollout:
// authenticated drain of the exact old instance, a persisted quiescence
// barrier, claim deletion, and only then the new immutable template.
//
//nolint:gocyclo // The rollout barriers intentionally mirror the audited Deployment path.
func (r *RuntimePoolReconciler) reconcileWorkspaceRuntimePoolRollout(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	sandboxTemplate *sandboxextv1beta1.SandboxTemplate,
	claim *sandboxextv1beta1.SandboxClaim,
	pods []corev1.Pod,
	desiredTemplate corev1.PodTemplateSpec,
	status corev1alpha1.RuntimePoolStatus,
) (ctrl.Result, error) {
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
	status.Message = "runtime template changed; admission is closed before provider workspace replacement"
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)

	readyPods := readyRuntimePoolPods(pods)
	if claim == nil || !claim.DeletionTimestamp.IsZero() {
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		if claim != nil || len(pods) > 0 {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
			status.Message = "waiting for the drained provider workspace to terminate before applying the new template"
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonStopping, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
		}
		if err := r.updateRuntimePoolSandboxTemplate(ctx, sandboxTemplate, desiredTemplate, sandboxRuntimePoolSuspendCapable(pool)); err != nil {
			return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
		}
		status.ActiveInstance = nil
		if pool.Spec.DesiredReplicas == 0 {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopped
			status.Message = "new runtime template is staged with no provider workspace demand"
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionTrue, "RolloutConverged", status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStarting
		status.Message = "old provider workspace terminated; starting the new immutable runtime template"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonStarting, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}
	if pool.Status.ActiveInstance == nil {
		if !runtimePoolRolloutControllerWorkIsQuiescent(status.Capacity) {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
			status.Message = "waiting for controller reservations or finalization work before replacing an unadmitted provider workspace"
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonDraining, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		if err := r.deleteRuntimePoolSandboxClaim(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
		status.ActiveInstance = nil
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "replacing an unadmitted provider workspace before applying the new template"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonStopping, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}
	if len(readyPods) == 0 {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "cannot authenticate the previous active runtime instance before workspace replacement"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	pod := &readyPods[0]
	deployedTemplate := sandboxTemplatePodTemplateSpec(sandboxTemplate)
	validationPool, validationConfig, err := runtimePoolPodTemplateValidationTarget(pool, deployedTemplate)
	if err != nil {
		return r.finishRuntimePoolRolloutFailure(ctx, pool, status, err)
	}
	deployedAuthSecret, err := r.runtimePoolPodTemplateAuthSecret(ctx, pool, cfg.namespace, deployedTemplate.Spec)
	if err != nil {
		return r.finishRuntimePoolRolloutFailure(ctx, pool, status, err)
	}
	probe, err := r.supervisorClientForPool(pool).Probe(ctx, runtimePoolInstanceEndpoint(pool, pod), string(deployedAuthSecret.Data[runtimePoolControllerTokenKey]), deployedAuthSecret.Data[runtimePoolCapabilitySecretKey])
	if err != nil {
		return r.finishRuntimePoolRolloutFailure(ctx, pool, status, fmt.Errorf("authenticated rollout status probe failed: %w", err))
	}
	active, err := validateRuntimePoolProbeForRollout(validationPool, validationConfig, pod, probe, r.now())
	if err != nil {
		return r.finishRuntimePoolRolloutFailure(ctx, pool, status, err)
	}
	if runtimePoolSupervisorRestartedInPlace(pool.Status.ActiveInstance, active) {
		return r.reconcileRuntimePoolInPlaceSupervisorRestart(ctx, pool, nil, pod, active, status)
	}
	if !runtimePoolRolloutActiveInstanceMatches(pool.Status.ActiveInstance, active) {
		return r.finishRuntimePoolRolloutFailure(ctx, pool, status, fmt.Errorf("authenticated runtime identity changed before rollout drain"))
	}
	status.ActiveInstance = active
	applyRuntimePoolProbeCapacity(&status, cfg, probe)
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionTrue, "PodScheduled", "selected old runtime Pod remains scheduled during rollout drain")

	if !probe.Status.Drain.Requested {
		reason := "runtime_pool_rollout_" + runtimePoolShortRevision(desiredTemplate.Annotations[runtimePoolTemplateRevisionAnnotation])
		if err := r.supervisorClientForPool(pool).RequestDrain(
			ctx,
			runtimePoolInstanceEndpoint(pool, pod),
			string(deployedAuthSecret.Data[runtimePoolControllerTokenKey]),
			deployedAuthSecret.Data[runtimePoolCapabilitySecretKey],
			probe.Status,
			reason,
		); err != nil {
			return r.finishRuntimePoolRolloutFailure(ctx, pool, status, fmt.Errorf("authenticated rollout drain request failed: %w", err))
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
		status.Message = runtimePoolMessageRolloutDrainRequested
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonDraining, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if !runtimePoolRolloutProbeIsQuiescent(status.Capacity, probe.Status) {
		if r.runtimePoolRolloutTimedOut(pool) {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = "timed out waiting for authenticated rollout drain barriers; preserving the old provider workspace"
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, runtimePoolRolloutReasonTimedOut, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
		status.Message = runtimePoolMessageRolloutSettling
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonDraining, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	if !runtimePoolRolloutQuiescencePersisted(pool) {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleQuiescent
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
		status.Message = runtimePoolMessageRolloutQuiescent
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonQuiescent, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if err := r.deleteRuntimePoolSandboxClaim(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	status.Message = "quiescent old provider workspace is stopping before the template changes"
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonStopping, status.Message)
	return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
}

// reconcileWorkspaceRuntimePoolScaleDown mirrors the Deployment scale-to-zero
// barriers: authenticated drain, an observed quiescent status, a persisted
// Quiescent barrier, then claim deletion.
//
//nolint:gocyclo // The scale-down barriers intentionally mirror the audited Deployment path.
func (r *RuntimePoolReconciler) reconcileWorkspaceRuntimePoolScaleDown(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	claim *sandboxextv1beta1.SandboxClaim,
	pods []corev1.Pod,
	readyPods []corev1.Pod,
	status corev1alpha1.RuntimePoolStatus,
) (ctrl.Result, error) {
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, "runtime pool is scaling down")

	if claim == nil || !claim.DeletionTimestamp.IsZero() {
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		if claim == nil && len(pods) == 0 {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopped
			status.ActiveInstance = nil
			status.Message = runtimePoolMessageStopped
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionUnknown, "ScaledToZero", status.Message)
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionTrue, "ScaledToZero", status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
		status.Message = "waiting for the quiescent provider workspace to terminate"
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}
	checkpointProtected := sandboxSuspendRecordAnnotationPresent(pool)
	if pool.DeletionTimestamp.IsZero() && (checkpointProtected || sandboxDurableLineageAnnotationPresent(pool)) {
		// A resumed lineage makes this claim the sole copy of the continued
		// workspace data. A standing checkpoint record provides the same fence
		// before the permanent lineage annotation is stamped. Ordinary
		// scale-to-zero is not consent to destroy either one; only explicit
		// workspace deletion may enter the destructive path.
		status.ActiveInstance = pool.Status.ActiveInstance
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		if checkpointProtected {
			status.Message = "consensual durable workspace checkpoint requires explicit workspace deletion before provider claim teardown"
		} else if pool.Status.ActiveInstance == nil {
			if strings.TrimSpace(pool.Annotations[runtimePoolWorkspaceResumeLostAnnotation]) == "" {
				if err := r.patchRuntimePoolAnnotation(ctx, pool, runtimePoolWorkspaceResumeLostAnnotation,
					"the resumed durable workspace lineage has no admitted runtime identity to checkpoint"); err != nil {
					return ctrl.Result{}, err
				}
			}
			status.Message = "resumed durable workspace lineage has no admitted runtime identity to checkpoint; the suspension fails closed with the provider claim preserved"
		} else {
			status.Message = "durable workspace lineage requires explicit workspace deletion before provider claim teardown"
		}
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	if pool.Status.ActiveInstance == nil {
		status.ActiveInstance = nil
		if !runtimePoolControllerWorkIsQuiescent(pool.Status.Capacity) {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
			status.Message = "waiting for controller reservations or finalization work before stopping an unadmitted provider workspace"
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		if err := r.deleteRuntimePoolSandboxClaim(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "stopping a provider workspace that never became active"
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}
	if len(readyPods) == 0 {
		status.ActiveInstance = nil
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = runtimePoolMessageDrainUnauthenticated
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	pod := &readyPods[0]
	deployedTemplate := runtimePoolPodTemplateSpec(pod)
	validationPool, validationConfig, err := runtimePoolPodTemplateValidationTarget(pool, deployedTemplate)
	if err != nil {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.ActiveInstance = nil
		status.Message = sanitizeRuntimePoolMessage("deployed runtime identity is invalid during scale-down: " + err.Error())
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	deployedAuthSecret, err := r.runtimePoolPodTemplateAuthSecret(ctx, pool, cfg.namespace, deployedTemplate.Spec)
	if err != nil {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.ActiveInstance = nil
		status.Message = sanitizeRuntimePoolMessage("resolve deployed runtime credentials during scale-down: " + err.Error())
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	probe, err := r.supervisorClientForPool(pool).Probe(ctx, runtimePoolInstanceEndpoint(pool, pod), string(deployedAuthSecret.Data[runtimePoolControllerTokenKey]), deployedAuthSecret.Data[runtimePoolCapabilitySecretKey])
	if err != nil {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.ActiveInstance = nil
		status.Message = sanitizeRuntimePoolMessage("authenticated drain status probe failed: " + err.Error())
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	active, err := validateRuntimePoolProbe(validationPool, validationConfig, pod, probe, r.now())
	if err != nil {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.ActiveInstance = nil
		status.Message = sanitizeRuntimePoolMessage(err.Error())
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	if pool.Status.ActiveInstance != nil && !runtimePoolRolloutActiveInstanceMatches(pool.Status.ActiveInstance, active) {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.ActiveInstance = nil
		status.Message = "authenticated runtime identity changed before scale-down drain"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	status.ActiveInstance = active
	applyRuntimePoolProbeCapacity(&status, cfg, probe)
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionTrue, "PodScheduled", "selected runtime Pod is scheduled")
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionTrue, "ExactInstanceReady", "selected runtime Pod and supervisor profile are ready")

	if !probe.Status.Drain.Requested {
		if err := r.supervisorClientForPool(pool).RequestDrain(
			ctx,
			runtimePoolInstanceEndpoint(pool, pod),
			string(deployedAuthSecret.Data[runtimePoolControllerTokenKey]),
			deployedAuthSecret.Data[runtimePoolCapabilitySecretKey],
			probe.Status,
			"runtime_pool_scale_to_zero",
		); err != nil {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = sanitizeRuntimePoolMessage("authenticated drain request failed: " + err.Error())
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
		status.Message = runtimePoolMessageDrainRequested
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if !runtimePoolProbeIsQuiescent(pool.Status.Capacity, probe.Status) {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
		status.Message = runtimePoolMessageDrainSettling
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	if pool.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleQuiescent {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleQuiescent
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
		status.Message = runtimePoolMessageDrainQuiescent
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if err := r.deleteRuntimePoolSandboxClaim(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	status.Message = "quiescent provider workspace is stopping"
	return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
}

// recycleRuntimePoolInstance replaces the exact selected runtime instance:
// Deployment-backed pools delete the runtime Pod for controller-owned emptyDir
// replacement; workspace-backed pools delete the SandboxClaim so the provider
// cascades the sandbox and Pod.
func (r *RuntimePoolReconciler) recycleRuntimePoolInstance(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	pod *corev1.Pod,
) error {
	if pool.Spec.ExecutionWorkspace == nil {
		if err := r.Delete(ctx, pod, deleteCurrentObjectPreconditions(pod)...); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	cfg, err := r.runtimePoolConfigForDeletion(pool)
	if err != nil {
		return err
	}
	if runtimePoolIsSubstrateBacked(pool) {
		control, controlErr := r.substrateActorControlForCleanup()
		if controlErr != nil {
			return controlErr
		}
		defer control.Close() //nolint:errcheck // best-effort connection teardown
		return r.recycleSubstrateActor(ctx, pool, control, runtimePoolSubstrateActorID(cfg.baseName))
	}
	claim, err := r.getRuntimePoolSandboxClaim(ctx, cfg)
	if err != nil {
		return err
	}
	if claim == nil {
		return nil
	}
	if sandboxSuspendRecordAnnotationPresent(pool) {
		// The claim holds the only preserved copy of a consensually suspended
		// workspace whose resume has not passed the Serving fence yet, or the
		// checkpoint metadata is malformed after suspension began. Recycling
		// would delete the claim and its PVC. Fail closed instead.
		return fmt.Errorf("refusing to recycle the workspace claim while it holds a consensually suspended checkpoint or malformed checkpoint metadata")
	}
	if sandboxDurableLineageAnnotationPresent(pool) {
		// The pool served a resumed durable lineage: its claim's PVC holds
		// the SOLE copy of that data even after the checkpoint consent
		// retired. Recycling would delete the claim, cascade the PVC away,
		// and let the same pool identity acquire a blank replacement the
		// adapter cannot distinguish; the workspace must fail terminally
		// instead of silently losing its preserved data.
		if err := r.patchRuntimePoolAnnotation(ctx, pool, runtimePoolWorkspaceResumeLostAnnotation,
			"the resumed durable workspace lineage cannot be recycled safely because its preserved data has no other copy"); err != nil {
			return err
		}
		return fmt.Errorf("refusing to recycle the workspace claim of a resumed durable lineage; the preserved data has no other copy")
	}
	return r.deleteRuntimePoolSandboxClaim(ctx, claim)
}

// sandboxReader returns the uncached reader for provider workload objects:
// the namespace-scoped manager cache does not watch sandbox extension kinds in
// the runtime namespace, and the provider CRDs may legitimately be absent.
func (r *RuntimePoolReconciler) sandboxReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *RuntimePoolReconciler) getRuntimePoolSandboxTemplate(ctx context.Context, cfg runtimePoolConfig) (*sandboxextv1beta1.SandboxTemplate, error) {
	template := &sandboxextv1beta1.SandboxTemplate{}
	err := r.sandboxReader().Get(ctx, types.NamespacedName{Namespace: cfg.namespace, Name: runtimePoolSandboxTemplateName(cfg.baseName)}, template)
	if err != nil {
		return nil, ignoreSandboxAPIAbsence("read RuntimePool sandbox template", err)
	}
	return template, nil
}

func (r *RuntimePoolReconciler) getRuntimePoolSandboxClaim(ctx context.Context, cfg runtimePoolConfig) (*sandboxextv1beta1.SandboxClaim, error) {
	claim := &sandboxextv1beta1.SandboxClaim{}
	err := r.sandboxReader().Get(ctx, types.NamespacedName{Namespace: cfg.namespace, Name: runtimePoolSandboxClaimName(cfg.baseName)}, claim)
	if err != nil {
		return nil, ignoreSandboxAPIAbsence("read RuntimePool sandbox claim", err)
	}
	return claim, nil
}

// ignoreSandboxAPIAbsence maps NotFound to nil and a missing agent-sandbox CRD
// installation to an explicit fail-closed configuration error.
func ignoreSandboxAPIAbsence(operation string, err error) error {
	if apierrors.IsNotFound(err) {
		return nil
	}
	if apimeta.IsNoMatchError(err) || k8sRuntimeIsMissingKindError(err) {
		return fmt.Errorf("%s: the agent-sandbox provider CRDs are not installed; workspace-backed RuntimePools require an externally operated agent-sandbox installation", operation)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func k8sRuntimeIsMissingKindError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no kind is registered")
}

func runtimePoolWorkspaceBootstrapTemplate(
	template corev1.PodTemplateSpec,
	nonce, publicKey string,
) corev1.PodTemplateSpec {
	result := *template.DeepCopy()
	if len(result.Spec.Containers) != 1 {
		return result
	}
	container := &result.Spec.Containers[0]
	env := make([]corev1.EnvVar, 0, len(container.Env)+2)
	for i := range container.Env {
		switch container.Env[i].Name {
		case runtimePoolControllerTokenFileEnv, runtimePoolCapabilitySecretFileEnv, runtimePoolProviderTokenFileEnv,
			"ORKA_ACP_CONTROLLER_TOKEN_BOOTSTRAP", "ORKA_ACP_CAPABILITY_SECRET_BOOTSTRAP", "ORKA_ACP_PROVIDER_TOKEN_BOOTSTRAP":
			continue
		default:
			env = append(env, container.Env[i])
		}
	}
	env = append(env,
		corev1.EnvVar{Name: "ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE", Value: nonce},
		corev1.EnvVar{Name: harnessv2.CredentialBootstrapPublicKeyEnv, Value: publicKey},
	)
	container.Env = env
	container.EnvFrom = nil
	mounts := container.VolumeMounts[:0]
	for i := range container.VolumeMounts {
		if container.VolumeMounts[i].Name != runtimePoolAuthVolume && container.VolumeMounts[i].Name != runtimePoolProviderCapabilityVolume {
			mounts = append(mounts, container.VolumeMounts[i])
		}
	}
	container.VolumeMounts = mounts
	volumes := result.Spec.Volumes[:0]
	for i := range result.Spec.Volumes {
		if result.Spec.Volumes[i].Name != runtimePoolAuthVolume && result.Spec.Volumes[i].Name != runtimePoolProviderCapabilityVolume {
			volumes = append(volumes, result.Spec.Volumes[i])
		}
	}
	result.Spec.Volumes = volumes
	result.Annotations[runtimePoolTemplateRevisionAnnotation] = runtimePoolPodTemplateRevision(result)
	return result
}

func (r *RuntimePoolReconciler) attestWorkspaceRuntimePoolMaterialization(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	claim *sandboxextv1beta1.SandboxClaim,
	template *sandboxextv1beta1.SandboxTemplate,
	pod *corev1.Pod,
) (bool, error) {
	if claim == nil || template == nil || pod == nil {
		return false, nil
	}
	sandbox := &sandboxv1beta1.Sandbox{}
	err := r.sandboxReader().Get(ctx, types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}, sandbox)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, ignoreSandboxAPIAbsence("read RuntimePool sandbox materialization", err)
	}
	if claim.UID != "" && !metav1.IsControlledBy(sandbox, claim) {
		return false, fmt.Errorf("provider Sandbox is not controlled by the exact SandboxClaim")
	}
	if claim.UID != "" && sandbox.Labels[sandboxextv1beta1.SandboxIDLabel] != string(claim.UID) {
		return false, fmt.Errorf("provider Sandbox does not carry the exact SandboxClaim identity")
	}
	if sandbox.Annotations[sandboxv1beta1.SandboxTemplateRefAnnotation] != template.Name {
		return false, fmt.Errorf("provider Sandbox does not record the validated SandboxTemplate")
	}
	expectedSpec := template.Spec.PodTemplate.Spec.DeepCopy()
	sandboxextcontrollers.ApplySandboxSecureDefaults(template, expectedSpec)
	if !apiequality.Semantic.DeepEqual(*expectedSpec, sandbox.Spec.PodTemplate.Spec) {
		return false, fmt.Errorf("provider Sandbox PodSpec differs from the validated SandboxTemplate revision")
	}
	// Durable workspace volume claims are injected by the SandboxClaim, not the
	// blueprint template: the Sandbox must materialize exactly the claim's
	// volume claims (empty for non-suspendable pools), and the claim's own
	// volume claims are separately attested against the frozen pool binding.
	if !apiequality.Semantic.DeepEqual(claim.Spec.VolumeClaimTemplates, sandbox.Spec.VolumeClaimTemplates) {
		return false, fmt.Errorf("provider Sandbox volume claims differ from the validated SandboxClaim")
	}
	if !reflect.DeepEqual(template.Spec.PodTemplate.ObjectMeta.Annotations, sandbox.Spec.PodTemplate.ObjectMeta.Annotations) {
		return false, fmt.Errorf("provider Sandbox Pod annotations differ from the validated SandboxTemplate revision")
	}
	if claim.UID != "" && sandbox.Spec.PodTemplate.ObjectMeta.Labels[sandboxextv1beta1.SandboxIDLabel] != string(claim.UID) {
		return false, fmt.Errorf("provider Sandbox Pod template does not carry the exact SandboxClaim identity")
	}
	materializedLabels := cloneStringMap(sandbox.Spec.PodTemplate.ObjectMeta.Labels)
	delete(materializedLabels, sandboxextv1beta1.SandboxIDLabel)
	delete(materializedLabels, sandboxv1beta1.SandboxTemplateRefHashLabel)
	if !reflect.DeepEqual(template.Spec.PodTemplate.ObjectMeta.Labels, materializedLabels) {
		return false, fmt.Errorf("provider Sandbox Pod labels differ from the validated SandboxTemplate revision")
	}
	if sandbox.UID != "" && !metav1.IsControlledBy(pod, sandbox) {
		return false, fmt.Errorf("runtime Pod is not controlled by the attested provider Sandbox")
	}
	if !runtimePoolWorkspacePodLabelsMatch(sandbox, pod) {
		return false, fmt.Errorf("runtime Pod labels differ from the attested provider Sandbox")
	}
	if !runtimePoolWorkspacePodAnnotationsMatch(sandbox, pod) {
		return false, fmt.Errorf("runtime Pod annotations differ from the attested provider Sandbox")
	}
	if !runtimePoolWorkspacePodSpecsMatch(sandbox.Spec.PodTemplate.Spec, pod.Spec, injectedDurableWorkspaceClaimName(sandbox)) {
		return false, fmt.Errorf("runtime PodSpec differs from the attested provider Sandbox")
	}
	if sandboxRuntimePoolSuspendCapable(pool) {
		// The nested claim/Sandbox templates prove only what was REQUESTED;
		// the realized PVC named by the Pod is what the workload actually
		// mounts. A stale or tampered provider reusing the deterministic
		// same-name PVC with a foreign owner or a spec populated from another
		// data source must fail attestation before credential seeding.
		if err := r.attestDurableWorkspacePVC(ctx, pool, sandbox, pod); err != nil {
			return false, err
		}
	}
	return true, nil
}

func runtimePoolWorkspacePodLabelsMatch(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) bool {
	if sandbox == nil || pod == nil {
		return false
	}
	expected := cloneStringMap(sandbox.Spec.PodTemplate.ObjectMeta.Labels)
	// agent-sandbox v1.0 propagates the claim UID from the Sandbox PodTemplate
	// onto the Pod. Keep it in the exact label comparison so the realized Pod
	// remains bound to the claim -> Sandbox -> Pod identity attested above.
	expected[sandboxcontrollers.SandboxNameHashLabel] = sandboxcontrollers.NameHash(sandbox.Name)
	return reflect.DeepEqual(expected, pod.Labels)
}

func runtimePoolWorkspacePodAnnotationsMatch(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) bool {
	if sandbox == nil || pod == nil {
		return false
	}
	actual := cloneStringMap(pod.Annotations)
	// agent-sandbox adds only these bookkeeping annotations while propagating
	// the attested PodTemplate metadata. Admission-added annotations are not
	// allowlisted: annotations can select network and runtime integrations, so
	// any other realized mutation must recycle the workspace before bootstrap.
	delete(actual, sandboxv1beta1.SandboxPropagatedLabelsAnnotation)
	delete(actual, sandboxv1beta1.SandboxPropagatedAnnotationsAnnotation)
	expected := sandbox.Spec.PodTemplate.ObjectMeta.Annotations
	if len(expected) == 0 {
		expected = nil
	}
	if len(actual) == 0 {
		actual = nil
	}
	return reflect.DeepEqual(expected, actual)
}

// runtimePoolWorkspacePodSpecsMatch compares the realized Pod against the
// attested Sandbox template before any credentials cross the network. The
// provider may adopt an existing Pod without reconciling its spec, so owner
// references and identity labels are not sufficient proof. Normalize only
// fields populated by the core API server, admission, or scheduler; every
// container, volume, security, network, and runtime field remains fail-closed.
func runtimePoolWorkspacePodSpecsMatch(expected, actual corev1.PodSpec, injectedDurableClaimName string) bool {
	expectedSpec := normalizeRuntimePoolWorkspacePodSpec(expected)
	actualSpec := normalizeRuntimePoolWorkspacePodSpec(actual)

	// The provider injects the durable workspace PVC volume from the claim's
	// volumeClaimTemplates; its per-sandbox claim name cannot be rendered into
	// the template, so the reserved-name PVC volume is compared by presence of
	// its mount rather than by claim identity. Any other unexpected volume
	// still fails the match.
	actualSpec.Volumes = stripInjectedDurableWorkspaceVolume(expectedSpec.Volumes, actualSpec.Volumes, injectedDurableClaimName)

	// These fields are derived from cluster scheduling/admission state rather
	// than from the provider-visible Sandbox template.
	expectedSpec.NodeName, actualSpec.NodeName = "", ""
	expectedSpec.Priority, actualSpec.Priority = nil, nil
	expectedSpec.PreemptionPolicy, actualSpec.PreemptionPolicy = nil, nil
	expectedSpec.Overhead, actualSpec.Overhead = nil, nil
	if len(expectedSpec.ImagePullSecrets) == 0 {
		actualSpec.ImagePullSecrets = nil
	}
	if expectedSpec.PriorityClassName == "" {
		actualSpec.PriorityClassName = ""
	}

	return apiequality.Semantic.DeepEqual(expectedSpec, actualSpec)
}

func normalizeRuntimePoolWorkspacePodSpec(spec corev1.PodSpec) corev1.PodSpec {
	result := *spec.DeepCopy()
	if result.DNSPolicy == "" {
		result.DNSPolicy = corev1.DNSClusterFirst
	}
	if result.RestartPolicy == "" {
		result.RestartPolicy = corev1.RestartPolicyAlways
	}
	if result.SecurityContext == nil {
		result.SecurityContext = &corev1.PodSecurityContext{}
	}
	if result.TerminationGracePeriodSeconds == nil {
		result.TerminationGracePeriodSeconds = new(int64)
		*result.TerminationGracePeriodSeconds = corev1.DefaultTerminationGracePeriodSeconds
	}
	if result.SchedulerName == "" {
		result.SchedulerName = corev1.DefaultSchedulerName
	}
	if result.EnableServiceLinks == nil {
		result.EnableServiceLinks = new(bool)
		*result.EnableServiceLinks = corev1.DefaultEnableServiceLinks
	}
	if result.ServiceAccountName == "" {
		result.ServiceAccountName = runtimePoolDefaultServiceAccountName
	}
	// The core API's internal-to-v1 conversion mirrors the effective service
	// account into this deprecated alias on Pods. Embedded PodSpecs in CRDs do
	// not receive that conversion, so compare the canonical field once here.
	result.DeprecatedServiceAccount = result.ServiceAccountName
	for i := range result.InitContainers {
		normalizeRuntimePoolWorkspaceContainer(&result.InitContainers[i])
	}
	for i := range result.Containers {
		normalizeRuntimePoolWorkspaceContainer(&result.Containers[i])
	}
	result.Tolerations = runtimePoolWorkspaceExplicitTolerations(result.Tolerations)
	return result
}

func normalizeRuntimePoolWorkspaceContainer(container *corev1.Container) {
	if container == nil {
		return
	}
	if container.TerminationMessagePath == "" {
		container.TerminationMessagePath = corev1.TerminationMessagePathDefault
	}
	if container.TerminationMessagePolicy == "" {
		container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	for i := range container.Ports {
		if container.Ports[i].Protocol == "" {
			container.Ports[i].Protocol = corev1.ProtocolTCP
		}
	}
	for i := range container.Env {
		fieldRef := container.Env[i].ValueFrom
		if fieldRef != nil && fieldRef.FieldRef != nil && fieldRef.FieldRef.APIVersion == "" {
			fieldRef.FieldRef.APIVersion = "v1"
		}
	}
}

func runtimePoolWorkspaceExplicitTolerations(tolerations []corev1.Toleration) []corev1.Toleration {
	result := make([]corev1.Toleration, 0, len(tolerations))
	for i := range tolerations {
		toleration := tolerations[i]
		if toleration.Operator == corev1.TolerationOpExists && toleration.Effect == corev1.TaintEffectNoExecute &&
			(toleration.Key == corev1.TaintNodeNotReady || toleration.Key == corev1.TaintNodeUnreachable) {
			continue
		}
		result = append(result, toleration)
	}
	return result
}

func (r *RuntimePoolReconciler) seedWorkspaceSupervisorCredentials(
	ctx context.Context,
	endpoint, nonce string,
	authSecret, providerSecret *corev1.Secret,
) (bool, error) {
	request := harnessv2.CredentialBootstrapRequest{
		ControllerToken:  strings.TrimSpace(string(authSecret.Data[runtimePoolControllerTokenKey])),
		CapabilitySecret: strings.TrimSpace(string(authSecret.Data[runtimePoolCapabilitySecretKey])),
		ProviderToken:    strings.TrimSpace(string(providerSecret.Data[runtimePoolProviderTokenKey])),
	}
	if err := request.Validate(); err != nil {
		return false, fmt.Errorf("pool credentials are incomplete: %w", err)
	}
	if r.WorkspaceCredentialSeeder != nil {
		return r.WorkspaceCredentialSeeder(ctx, endpoint, nonce, authSecret.Data[runtimePoolBootstrapSigningSeedKey], request)
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return false, err
	}
	seedCtx, cancel := context.WithTimeout(ctx, runtimePoolProbeTimeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(
		seedCtx, http.MethodPut, strings.TrimRight(endpoint, "/")+harnessv2.CredentialBootstrapPath, bytes.NewReader(payload),
	)
	if err != nil {
		return false, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set(harnessv2.CredentialBootstrapNonceHeader, nonce)
	signature, err := harnessv2.SignCredentialBootstrap(authSecret.Data[runtimePoolBootstrapSigningSeedKey], nonce, payload)
	if err != nil {
		return false, fmt.Errorf("sign credential bootstrap request: %w", err)
	}
	httpRequest.Header.Set(harnessv2.CredentialBootstrapSignatureHeader, signature)
	httpClient := r.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: runtimePoolProbeTimeout, Transport: harnessv2.NewProxylessTransport()}
	}
	isolationClient := *httpClient
	isolationClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	response, err := isolationClient.Do(httpRequest)
	if err != nil {
		return false, err
	}
	defer response.Body.Close() //nolint:errcheck // response body is unused
	switch response.StatusCode {
	case http.StatusCreated, http.StatusOK:
		return false, nil
	case http.StatusConflict:
		return false, errWorkspaceCredentialConflict
	case http.StatusNotFound:
		return true, nil
	default:
		return false, fmt.Errorf("credential bootstrap returned status %d", response.StatusCode)
	}
}

func (r *RuntimePoolReconciler) createRuntimePoolSandboxTemplate(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	desiredTemplate corev1.PodTemplateSpec,
) error {
	template := &sandboxextv1beta1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      runtimePoolSandboxTemplateName(cfg.baseName),
			Namespace: cfg.namespace,
			Labels:    cloneStringMap(cfg.labels),
		},
		Spec: runtimePoolSandboxTemplateSpec(desiredTemplate, sandboxRuntimePoolSuspendCapable(pool)),
	}
	if err := r.setRuntimePoolControllerReference(pool, template); err != nil {
		return err
	}
	if err := r.Create(ctx, template); err != nil && !apierrors.IsAlreadyExists(err) {
		return ignoreSandboxAPIAbsence("create RuntimePool sandbox template", err)
	}
	return nil
}

func (r *RuntimePoolReconciler) updateRuntimePoolSandboxTemplate(
	ctx context.Context,
	template *sandboxextv1beta1.SandboxTemplate,
	desiredTemplate corev1.PodTemplateSpec,
	suspendCapable bool,
) error {
	if template == nil {
		return fmt.Errorf("RuntimePool sandbox template is required for a template update")
	}
	base := template.DeepCopy()
	template.Spec = runtimePoolSandboxTemplateSpec(desiredTemplate, suspendCapable)
	if err := r.Patch(ctx, template, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("update RuntimePool sandbox template: %w", err)
	}
	return nil
}

// runtimePoolSandboxTemplateSpec renders the exact controller-owned supervisor
// Pod template into the provider's template shape. The provider's managed
// NetworkPolicy is disabled: the pool's own default-deny NetworkPolicies select
// the workspace Pod through the propagated pool labels, and claim-side env or
// volume injection stays disallowed so credentials never cross the provider API.
func runtimePoolSandboxTemplateSpec(desiredTemplate corev1.PodTemplateSpec, suspendCapable bool) sandboxextv1beta1.SandboxTemplateSpec {
	// A suspend-capable pool's claim injects exactly the controller-owned
	// durable workspace PVC template (forcing a cold start); every other pool
	// keeps claim-side volume injection disallowed.
	vctPolicy := sandboxextv1beta1.VolumeClaimTemplatesPolicyDisallowed
	if suspendCapable {
		vctPolicy = sandboxextv1beta1.VolumeClaimTemplatesPolicyAllowed
	}
	return sandboxextv1beta1.SandboxTemplateSpec{
		SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
			PodTemplate: sandboxv1beta1.PodTemplate{
				ObjectMeta: sandboxv1beta1.PodMetadata{
					Labels:      cloneStringMap(desiredTemplate.Labels),
					Annotations: cloneStringMap(desiredTemplate.Annotations),
				},
				Spec: *desiredTemplate.Spec.DeepCopy(),
			},
		},
		NetworkPolicyManagement:    sandboxextv1beta1.NetworkPolicyManagementUnmanaged,
		EnvVarsInjectionPolicy:     sandboxextv1beta1.EnvVarsInjectionPolicyDisallowed,
		VolumeClaimTemplatesPolicy: vctPolicy,
	}
}

func sandboxTemplatePodTemplateSpec(template *sandboxextv1beta1.SandboxTemplate) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      cloneStringMap(template.Spec.PodTemplate.ObjectMeta.Labels),
			Annotations: cloneStringMap(template.Spec.PodTemplate.ObjectMeta.Annotations),
		},
		Spec: *template.Spec.PodTemplate.Spec.DeepCopy(),
	}
}

func runtimePoolPodTemplateSpec(pod *corev1.Pod) corev1.PodTemplateSpec {
	if pod == nil {
		return corev1.PodTemplateSpec{}
	}
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      cloneStringMap(pod.Labels),
			Annotations: cloneStringMap(pod.Annotations),
		},
		Spec: *pod.Spec.DeepCopy(),
	}
}

func runtimePoolSandboxTemplateSpecRevision(spec sandboxextv1beta1.SandboxTemplateSpec) string {
	revision, err := runtimePoolJSONRevision(spec)
	if err != nil {
		panic(fmt.Sprintf("marshal RuntimePool SandboxTemplate revision: %v", err))
	}
	return revision
}

func runtimePoolSandboxTemplateObjectRevision(template *sandboxextv1beta1.SandboxTemplate) (string, error) {
	if template == nil {
		return "", fmt.Errorf("RuntimePool SandboxTemplate is required")
	}
	revision, err := runtimePoolJSONRevision(template.Spec)
	if err != nil {
		return "", fmt.Errorf("compute RuntimePool SandboxTemplate revision: %w", err)
	}
	return revision, nil
}

func runtimePoolSandboxChildOwnedByPool(object client.Object, pool *corev1alpha1.RuntimePool, cfg runtimePoolConfig) bool {
	if object == nil || pool == nil || object.GetNamespace() != cfg.namespace {
		return false
	}
	labels := object.GetLabels()
	for key, value := range cfg.labels {
		if value == "" || labels[key] != value {
			return false
		}
	}
	if object.GetNamespace() == pool.Namespace && !metav1.IsControlledBy(object, pool) {
		return false
	}
	return true
}

func runtimePoolSandboxClaimMatchesPool(claim *sandboxextv1beta1.SandboxClaim, pool *corev1alpha1.RuntimePool, cfg runtimePoolConfig) bool {
	return claim != nil && claim.Spec.WarmPoolRef.Name == runtimePoolSandboxWarmPoolName(cfg.baseName) &&
		len(claim.Spec.Env) == 0 && runtimePoolDurableVolumeClaimTemplatesMatch(claim, pool)
}

func (r *RuntimePoolReconciler) setRuntimePoolSandboxTemplateRevision(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	revision string,
) error {
	if strings.TrimSpace(revision) == "" {
		return fmt.Errorf("RuntimePool SandboxTemplate revision is required")
	}
	if pool.Annotations[runtimePoolSandboxTemplateRevisionAnnotation] == revision {
		return nil
	}
	base := pool.DeepCopy()
	if pool.Annotations == nil {
		pool.Annotations = map[string]string{}
	}
	pool.Annotations[runtimePoolSandboxTemplateRevisionAnnotation] = revision
	if err := r.Patch(ctx, pool, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("record RuntimePool SandboxTemplate revision: %w", err)
	}
	return nil
}

func runtimePoolSandboxTemplateNeedsRollout(template *sandboxextv1beta1.SandboxTemplate, desiredTemplate corev1.PodTemplateSpec) bool {
	if template == nil {
		return false
	}
	desiredRevision := strings.TrimSpace(desiredTemplate.Annotations[runtimePoolTemplateRevisionAnnotation])
	deployedRevision := strings.TrimSpace(template.Spec.PodTemplate.ObjectMeta.Annotations[runtimePoolTemplateRevisionAnnotation])
	return desiredRevision == "" || deployedRevision != desiredRevision
}

func (r *RuntimePoolReconciler) ensureRuntimePoolSandboxWarmPool(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) error {
	warmPool := &sandboxextv1beta1.SandboxWarmPool{}
	name := runtimePoolSandboxWarmPoolName(cfg.baseName)
	err := r.sandboxReader().Get(ctx, types.NamespacedName{Namespace: cfg.namespace, Name: name}, warmPool)
	if apierrors.IsNotFound(err) {
		// Zero replicas: every claim cold-starts from the exact current
		// template, so a stale pre-warmed Pod can never be adopted.
		warmPool = &sandboxextv1beta1.SandboxWarmPool{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cfg.namespace, Labels: cloneStringMap(cfg.labels)},
			Spec: sandboxextv1beta1.SandboxWarmPoolSpec{
				Replicas:    new(int32),
				TemplateRef: sandboxextv1beta1.SandboxTemplateRef{Name: runtimePoolSandboxTemplateName(cfg.baseName)},
			},
		}
		if err := r.setRuntimePoolControllerReference(pool, warmPool); err != nil {
			return err
		}
		if err := r.Create(ctx, warmPool); err != nil && !apierrors.IsAlreadyExists(err) {
			return ignoreSandboxAPIAbsence("create RuntimePool sandbox warm pool", err)
		}
		return nil
	}
	if err != nil {
		return ignoreSandboxAPIAbsence("read RuntimePool sandbox warm pool", err)
	}
	if !runtimePoolSandboxChildOwnedByPool(warmPool, pool, cfg) {
		return fmt.Errorf("same-name SandboxWarmPool does not carry the exact RuntimePool ownership identity")
	}
	if warmPool.Spec.TemplateRef.Name != runtimePoolSandboxTemplateName(cfg.baseName) ||
		warmPool.Spec.Replicas == nil || *warmPool.Spec.Replicas != 0 {
		base := warmPool.DeepCopy()
		warmPool.Spec.Replicas = new(int32)
		warmPool.Spec.TemplateRef = sandboxextv1beta1.SandboxTemplateRef{Name: runtimePoolSandboxTemplateName(cfg.baseName)}
		if err := r.Patch(ctx, warmPool, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("update RuntimePool sandbox warm pool: %w", err)
		}
	}
	return nil
}

// verifyPinnedDurableStorageClass reverifies, immediately before the durable
// PVC is requested, that the live StorageClass still carries the exact UID
// pinned at class resolution and Delete reclaim semantics: a same-name
// replacement class must never let teardown report deletion while Kubernetes
// retains the PV and its repository data.
func (r *RuntimePoolReconciler) verifyPinnedDurableStorageClass(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
) error {
	volume := pool.Spec.ExecutionWorkspace.AgentSandbox.SuspendVolume
	if strings.TrimSpace(volume.StorageClassUID) == "" {
		// Older bindings predate the UID pin, but they must still re-resolve
		// the named or default class and enforce the all-Delete lifecycle.
		// This cannot detect a same-name replacement that still uses Delete;
		// current producers stamp the UID and take the exact-identity path
		// below.
		if _, err := validateDurableStorageClassReclaim(
			ctx,
			r.sandboxReader(),
			strings.TrimSpace(volume.StorageClassName),
			"legacy RuntimePool "+pool.Namespace+"/"+pool.Name,
		); err != nil {
			return fmt.Errorf("reverify unpinned durable storage class: %w", err)
		}
		return nil
	}
	class := &storagev1.StorageClass{}
	if err := r.sandboxReader().Get(ctx, types.NamespacedName{Name: volume.StorageClassName}, class); err != nil {
		return fmt.Errorf("reverify the pinned durable storage class: %w", err)
	}
	if string(class.UID) != volume.StorageClassUID {
		return fmt.Errorf(
			"durable storage class %q was replaced (uid %s, pinned %s); the frozen binding fails closed",
			volume.StorageClassName, class.UID, volume.StorageClassUID,
		)
	}
	if !class.DeletionTimestamp.IsZero() {
		return fmt.Errorf(
			"durable storage class %q is being deleted; refusing to provision against a terminating class",
			volume.StorageClassName,
		)
	}
	if class.ReclaimPolicy != nil && *class.ReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		return fmt.Errorf(
			"durable storage class %q no longer has Delete reclaim semantics; the frozen binding fails closed",
			volume.StorageClassName,
		)
	}
	return nil
}

func (r *RuntimePoolReconciler) createRuntimePoolSandboxClaim(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) error {
	claim := &sandboxextv1beta1.SandboxClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      runtimePoolSandboxClaimName(cfg.baseName),
			Namespace: cfg.namespace,
			Labels:    cloneStringMap(cfg.labels),
		},
		Spec: sandboxextv1beta1.SandboxClaimSpec{
			WarmPoolRef: sandboxextv1beta1.SandboxWarmPoolRef{Name: runtimePoolSandboxWarmPoolName(cfg.baseName)},
		},
	}
	if sandboxRuntimePoolSuspendCapable(pool) {
		// The dedicated durable workspace PVC forces a cold start instead of
		// warm-pool adoption; that capacity tradeoff is the price of a
		// suspendable workspace.
		if err := r.verifyPinnedDurableStorageClass(ctx, pool); err != nil {
			return err
		}
		durable, err := runtimePoolDurableVolumeClaimTemplate(pool)
		if err != nil {
			return err
		}
		claim.Spec.VolumeClaimTemplates = []sandboxv1beta1.PersistentVolumeClaimTemplate{durable}
	}
	if err := r.setRuntimePoolControllerReference(pool, claim); err != nil {
		return err
	}
	if err := r.Create(ctx, claim); err != nil && !apierrors.IsAlreadyExists(err) {
		return ignoreSandboxAPIAbsence("create RuntimePool sandbox claim", err)
	}
	return nil
}

func (r *RuntimePoolReconciler) deleteRuntimePoolSandboxClaim(ctx context.Context, claim *sandboxextv1beta1.SandboxClaim) error {
	if claim == nil || !claim.DeletionTimestamp.IsZero() {
		return nil
	}
	if err := r.Delete(ctx, claim, deleteCurrentObjectPreconditions(claim)...); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete RuntimePool sandbox claim: %w", err)
	}
	return nil
}

// applySandboxClaimFailureConditions surfaces provider claim failures through
// the sanitized RolloutReady condition without exposing provider identifiers.
func (r *RuntimePoolReconciler) applySandboxClaimFailureConditions(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	claim *sandboxextv1beta1.SandboxClaim,
	status *corev1alpha1.RuntimePoolStatus,
	expectSuspended bool,
) (bool, error) {
	if claim == nil || status == nil {
		return false, nil
	}
	for i := range claim.Status.Conditions {
		condition := claim.Status.Conditions[i]
		if condition.Type != string(sandboxv1beta1.SandboxConditionReady) || condition.Status != metav1.ConditionFalse {
			continue
		}
		reason := strings.TrimSpace(condition.Reason)
		if reason == sandboxv1beta1.SandboxReasonDependenciesNotReady {
			// A provisioning workspace is expected while starting; readiness
			// gating is owned by the Ready-Pod and fence probes.
			continue
		}
		if expectSuspended && (reason == sandboxv1beta1.SandboxReasonSuspended || reason == "SandboxNotReady") {
			// A consensually suspended Sandbox keeps exactly these claim
			// states by design: the claim forwards the Sandbox's
			// SandboxSuspended Ready reason, or falls back to the generic
			// SandboxNotReady while the Sandbox republishes conditions.
			// Holding on them would strand every cold resume. Any OTHER claim
			// failure during resume - template loss, adoption conflict,
			// volume errors, expiry - still degrades the pool instead of
			// being hidden behind the suspend record.
			continue
		}
		message := sanitizeRuntimePoolMessage("provider workspace claim is not ready: " + condition.Message)
		preserveSuspendFence := sandboxWorkspaceSuspendRequested(pool)
		if !preserveSuspendFence && pool.DeletionTimestamp.IsZero() && pool.Status.ActiveInstance != nil {
			pending, err := r.linkedWorkspaceSuspendIntentPending(ctx, pool)
			if err != nil {
				return false, err
			}
			preserveSuspendFence = pending
		}
		if preserveSuspendFence && pool.Status.ActiveInstance != nil {
			status.ActiveInstance = pool.Status.ActiveInstance
		} else {
			status.ActiveInstance = nil
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = message
		r.setRuntimePoolCondition(pool, status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, message)
		r.setRuntimePoolCondition(pool, status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, message)
		return true, nil
	}
	return false, nil
}

// pruneStaleWorkspaceRuntimePoolSecrets removes epoch-scoped credential Secrets
// no longer referenced by the current names, the provider template, or any live
// workspace Pod. It mirrors the Deployment-path pruning ownership rules.
func (r *RuntimePoolReconciler) pruneStaleWorkspaceRuntimePoolSecrets(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	sandboxTemplate *sandboxextv1beta1.SandboxTemplate,
	currentNames ...string,
) error {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	keep := make(map[string]struct{}, len(currentNames)+2)
	for _, name := range currentNames {
		addRuntimeSecretName(keep, name)
	}
	if sandboxTemplate != nil {
		addRuntimePoolSecretReferences(keep, sandboxTemplate.Spec.PodTemplate.Spec)
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(cfg.namespace), client.MatchingLabels{
		runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel],
		runtimePoolUIDLabel: string(pool.UID),
	}); err != nil {
		return fmt.Errorf("list RuntimePool workspace Pods for stale credential cleanup: %w", err)
	}
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodSucceeded || pods.Items[i].Status.Phase == corev1.PodFailed {
			continue
		}
		addRuntimePoolSecretReferences(keep, pods.Items[i].Spec)
	}
	var secrets corev1.SecretList
	if err := reader.List(ctx, &secrets, client.InNamespace(cfg.namespace), client.MatchingLabels{
		runtimePoolManagedByLabel: runtimePoolManagedByLabelValue,
		runtimePoolKeyLabel:       cfg.labels[runtimePoolKeyLabel],
		runtimePoolUIDLabel:       string(pool.UID),
	}); err != nil {
		return fmt.Errorf("list managed RuntimePool Secrets for stale credential cleanup: %w", err)
	}
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		if _, current := keep[secret.Name]; current || !runtimePoolManagedCredentialSecret(secret, cfg) {
			continue
		}
		if err := r.deleteRuntimePoolManagedSecret(ctx, secret); err != nil {
			return err
		}
	}
	return nil
}

// deleteRuntimePoolWorkspaceChildren removes provider workload objects during
// pool finalization. Missing CRDs are tolerated: nothing provider-owned can
// exist without the provider installation.
func (r *RuntimePoolReconciler) deleteRuntimePoolWorkspaceChildren(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) (bool, error) {
	identityRecorded, err := r.recoverLegacyDurableLineageSandboxIdentity(ctx, pool, cfg)
	if err != nil {
		return false, err
	}
	if identityRecorded {
		// The recovered identity is a write-ahead fence. Re-read it from the
		// API before deleting the claim whose background collection owns the
		// Sandbox, so a restart cannot lose the only finalization identity.
		return true, nil
	}

	remaining := false
	objects := []client.Object{
		&sandboxextv1beta1.SandboxClaim{ObjectMeta: metav1.ObjectMeta{Name: runtimePoolSandboxClaimName(cfg.baseName), Namespace: cfg.namespace}},
		&sandboxextv1beta1.SandboxWarmPool{ObjectMeta: metav1.ObjectMeta{Name: runtimePoolSandboxWarmPoolName(cfg.baseName), Namespace: cfg.namespace}},
		&sandboxextv1beta1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Name: runtimePoolSandboxTemplateName(cfg.baseName), Namespace: cfg.namespace}},
	}
	for _, obj := range objects {
		key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
		if err := r.sandboxReader().Get(ctx, key, obj); err != nil {
			if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) || k8sRuntimeIsMissingKindError(err) {
				continue
			}
			return false, err
		}
		if !runtimePoolSandboxChildOwnedByPool(obj, pool, cfg) {
			return false, fmt.Errorf("refusing to delete foreign same-name provider resource %T %s/%s", obj, obj.GetNamespace(), obj.GetName())
		}
		if err := r.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		remaining = true
	}
	if name, uid := sandboxRecordedFinalizationIdentity(pool); name != "" && uid != "" {
		sandbox := &sandboxv1beta1.Sandbox{}
		err := r.sandboxReader().Get(ctx, types.NamespacedName{Namespace: cfg.namespace, Name: name}, sandbox)
		switch {
		case apierrors.IsNotFound(err), apimeta.IsNoMatchError(err), k8sRuntimeIsMissingKindError(err):
		case err != nil:
			return false, err
		case sandbox.UID == uid:
			// SandboxClaim deletion relies on background owner-reference
			// collection. Keep the RuntimePool finalizer until the exact
			// checkpointed Sandbox is actually absent; a provider finalizer may
			// otherwise leave it alive after the claim disappears.
			remaining = true
		}
	}
	return remaining, nil
}

// recoverLegacyDurableLineageSandboxIdentity upgrades the short-lived legacy
// lineage shape that pinned only PVC/PV identity. New records always include
// the Sandbox name and UID, but deletion must remain executable for an older
// object without letting background claim collection orphan its Sandbox.
func (r *RuntimePoolReconciler) recoverLegacyDurableLineageSandboxIdentity(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) (bool, error) {
	lineage := sandboxRecordedDurableLineage(pool)
	if lineage == nil || (strings.TrimSpace(lineage.Name) != "" && lineage.UID != "") {
		return false, nil
	}
	sandbox, found, err := r.findLegacyDurableLineageSandbox(ctx, pool, cfg)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	if name := strings.TrimSpace(lineage.Name); name != "" && name != sandbox.Name {
		return false, fmt.Errorf("legacy durable workspace lineage names Sandbox %q, but the owned Sandbox is %q", name, sandbox.Name)
	}
	if lineage.UID != "" && lineage.UID != sandbox.UID {
		return false, fmt.Errorf("legacy durable workspace lineage Sandbox UID does not match the owned Sandbox")
	}
	if strings.TrimSpace(sandbox.Name) == "" || sandbox.UID == "" {
		return false, fmt.Errorf("owned Sandbox is missing the identity required for durable-lineage finalization")
	}
	lineage.Name = sandbox.Name
	lineage.UID = sandbox.UID
	encoded, err := json.Marshal(lineage)
	if err != nil {
		return false, fmt.Errorf("encode recovered durable workspace lineage: %w", err)
	}
	if err := r.patchRuntimePoolAnnotation(ctx, pool, runtimePoolDurableLineageAnnotation, string(encoded)); err != nil {
		return false, fmt.Errorf("record recovered durable workspace lineage Sandbox identity: %w", err)
	}
	return true, nil
}

func (r *RuntimePoolReconciler) findLegacyDurableLineageSandbox(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) (*sandboxv1beta1.Sandbox, bool, error) {
	reader := r.sandboxReader()
	claim := &sandboxextv1beta1.SandboxClaim{}
	claimKey := types.NamespacedName{Namespace: cfg.namespace, Name: runtimePoolSandboxClaimName(cfg.baseName)}
	claimErr := reader.Get(ctx, claimKey, claim)
	claimPresent := claimErr == nil
	switch {
	case claimPresent:
		if !runtimePoolSandboxChildOwnedByPool(claim, pool, cfg) {
			return nil, false, fmt.Errorf("refusing to recover durable-lineage identity from a foreign same-name SandboxClaim")
		}
		if claim.UID == "" {
			return nil, false, fmt.Errorf("owned SandboxClaim is missing the UID required for durable-lineage recovery")
		}
	case apierrors.IsNotFound(claimErr), apimeta.IsNoMatchError(claimErr), k8sRuntimeIsMissingKindError(claimErr):
		// The claim may already have been deleted by an older controller.
		// Discover any surviving Sandbox by its frozen pool labels instead.
	default:
		return nil, false, fmt.Errorf("read SandboxClaim for durable-lineage recovery: %w", claimErr)
	}

	sandboxes := &sandboxv1beta1.SandboxList{}
	if err := reader.List(ctx, sandboxes, client.InNamespace(cfg.namespace)); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) || k8sRuntimeIsMissingKindError(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("list Sandboxes for durable-lineage recovery: %w", err)
	}
	matches := make([]*sandboxv1beta1.Sandbox, 0, 1)
	for i := range sandboxes.Items {
		sandbox := &sandboxes.Items[i]
		owner := metav1.GetControllerOf(sandbox)
		if claimPresent {
			if owner == nil || owner.UID != claim.UID {
				continue
			}
		} else if !legacyDurableLineageSandboxMatchesPool(sandbox, owner, pool, cfg) {
			continue
		}
		matches = append(matches, sandbox)
	}
	if len(matches) == 0 {
		return nil, false, nil
	}
	if len(matches) != 1 {
		return nil, false, fmt.Errorf("durable-lineage recovery found %d candidate Sandboxes; refusing ambiguous finalization", len(matches))
	}
	return matches[0], true, nil
}

func legacyDurableLineageSandboxMatchesPool(
	sandbox *sandboxv1beta1.Sandbox,
	owner *metav1.OwnerReference,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) bool {
	if sandbox == nil || owner == nil || owner.Kind != "SandboxClaim" ||
		owner.Name != runtimePoolSandboxClaimName(cfg.baseName) || owner.UID == "" || sandbox.UID == "" {
		return false
	}
	labels := sandbox.Spec.PodTemplate.ObjectMeta.Labels
	for key, value := range cfg.labels {
		if value == "" || labels[key] != value {
			return false
		}
	}
	claimUID := string(owner.UID)
	return sandbox.Labels[sandboxextv1beta1.SandboxIDLabel] == claimUID &&
		labels[sandboxextv1beta1.SandboxIDLabel] == claimUID &&
		pool != nil && labels[runtimePoolUIDLabel] == string(pool.UID)
}

// runtimePoolPodTemplateValidationTarget reconstructs the deployed pool
// identity (generation, epoch, profile) from a rendered Pod template so a
// draining old instance validates against the exact profile it was booted with.
func runtimePoolPodTemplateValidationTarget(
	pool *corev1alpha1.RuntimePool,
	template corev1.PodTemplateSpec,
) (*corev1alpha1.RuntimePool, runtimePoolConfig, error) {
	if pool == nil || len(template.Spec.Containers) != 1 {
		return nil, runtimePoolConfig{}, fmt.Errorf("deployed RuntimePool template is invalid")
	}
	return runtimePoolValidationTargetFromTemplate(pool, template)
}

func (r *RuntimePoolReconciler) runtimePoolPodTemplateAuthSecret(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	namespace string,
	podSpec corev1.PodSpec,
) (*corev1.Secret, error) {
	secretName := ""
	for i := range podSpec.Volumes {
		volume := podSpec.Volumes[i]
		if volume.Name == runtimePoolAuthVolume && volume.Secret != nil {
			secretName = strings.TrimSpace(volume.Secret.SecretName)
			break
		}
	}
	var secret *corev1.Secret
	if secretName != "" {
		secret = &corev1.Secret{}
		if err := r.sandboxReader().Get(ctx, types.NamespacedName{Namespace: namespace, Name: secretName}, secret); err != nil {
			return nil, fmt.Errorf("get deployed RuntimePool auth Secret: %w", err)
		}
	} else {
		epochText := ""
		if len(podSpec.Containers) == 1 {
			epochText = strings.TrimSpace(runtimePoolLiteralEnvironment(podSpec.Containers[0].Env)["ORKA_ACP_CONTROLLER_EPOCH"])
		}
		epoch, err := strconv.ParseInt(epochText, 10, 64)
		if err != nil || epoch <= 0 {
			return nil, fmt.Errorf("deployed RuntimePool auth Secret reference is missing")
		}
		secret, err = resolveRuntimePoolAuthSecret(ctx, r.sandboxReader(), pool, namespace, epoch)
		if err != nil {
			return nil, err
		}
	}
	if strings.TrimSpace(string(secret.Data[runtimePoolControllerTokenKey])) == "" ||
		len(secret.Data[runtimePoolCapabilitySecretKey]) == 0 {
		return nil, fmt.Errorf("deployed RuntimePool auth Secret is incomplete")
	}
	return secret, nil
}

// finishWorkspacePoolPrerequisiteFailure preserves durable workspace state
// across failures that occur before the workspace backend can run.
func (r *RuntimePoolReconciler) finishWorkspacePoolPrerequisiteFailure(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	failureContext string,
	failureErr error,
) (ctrl.Result, error) {
	return r.finishWorkspacePoolFailurePreservingDurableState(ctx, pool, cfg, failureContext, failureErr)
}

// finishWorkspacePoolProviderReadFailure handles a transient provider read
// failure before the suspend dispatch.
func (r *RuntimePoolReconciler) finishWorkspacePoolProviderReadFailure(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	readErr error,
) (ctrl.Result, error) {
	return r.finishWorkspacePoolFailurePreservingDurableState(ctx, pool, cfg, "provider read failed", readErr)
}

// finishWorkspacePoolFailurePreservingDurableState keeps the admitted
// identity across transient prerequisite failures while suspension or a
// durable lineage stands. Clearing it would let the recovered reconcile enter
// unadmitted scale-down and delete the SandboxClaim plus its durable PVC.
func (r *RuntimePoolReconciler) finishWorkspacePoolFailurePreservingDurableState(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	failureContext string,
	failureErr error,
) (ctrl.Result, error) {
	preserveFence, pendingErr := r.workspacePoolFailureRequiresDurableStatePreservation(ctx, pool)
	if pendingErr != nil {
		return ctrl.Result{}, errors.Join(failureErr, fmt.Errorf("check linked workspace suspension intent: %w", pendingErr))
	}
	if preserveFence {
		return r.finishWorkspacePoolFailureWithPreservedDurableState(ctx, pool, failureContext, failureErr)
	}
	return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, failureErr)
}

func (r *RuntimePoolReconciler) workspacePoolFailureRequiresDurableStatePreservation(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
) (bool, error) {
	preserveFence := sandboxWorkspaceSuspendRequested(pool) || sandboxSuspendRecordAnnotationPresent(pool) ||
		sandboxDurableLineageAnnotationPresent(pool) || substrateWorkspaceDurableStateProtectionPresent(pool)
	if preserveFence {
		return true, nil
	}
	return r.linkedWorkspaceSuspendIntentPending(ctx, pool)
}

func (r *RuntimePoolReconciler) finishWorkspacePoolFailureWithPreservedDurableState(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	failureContext string,
	failureErr error,
) (ctrl.Result, error) {
	status := r.baseRuntimePoolStatus(pool, 0)
	status.ActiveInstance = pool.Status.ActiveInstance
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	status.Message = sanitizeRuntimePoolMessage(failureContext + " while a suspension or durable lineage stands; retrying with the admitted identity preserved: " + failureErr.Error())
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
	return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
}
