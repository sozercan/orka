/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	acpworkspacev1alpha1 "github.com/orka-agents/orka/api/acp.workspace/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/workspace/statusrules"
)

// defaultWorkspaceSlotName mirrors the API default for
// Task.spec.execution.workspace.workspaceSlot.
const (
	defaultWorkspaceSlotName     = "default"
	acpWorkspaceSessionUIDMapKey = "sessionUID"
	acpWorkspaceSlotMapKey       = "workspaceSlot"
)

// ACPRuntimeWorkspaceBinding is the resolved, canonical execution-workspace
// binding for one ACP RuntimePool. It carries no provider-native identifiers
// and no secrets; it is frozen into the immutable execution snapshot and
// recomputed exactly during snapshot verification.
type ACPRuntimeWorkspaceBinding struct {
	Provider      corev1alpha1.WorkspaceProvider
	ReusePolicy   corev1alpha1.WorkspaceReusePolicy
	CleanupPolicy corev1alpha1.WorkspaceCleanupPolicy
	WorkspaceSlot string
	// SessionUID is the immutable durable SessionControl identity when
	// ReusePolicy is session. It is empty for per-Task workspaces.
	SessionUID string
	// SessionKey scopes the physical workspace to one logical RuntimeSession:
	// the immutable Session UID under reusePolicy session, or the exact Task UID
	// otherwise.
	SessionKey string
	// TemplateNamespace and TemplateName reference the operator-owned Substrate
	// infrastructure ActorTemplate. They are set exactly when Provider is
	// substrate: agent-sandbox workloads run only controller-rendered templates.
	TemplateNamespace string
	TemplateName      string
	// Class is the frozen controller-first ExecutionWorkspaceClass binding for
	// class-selected workspaces. It is nil for legacy provider-shaped requests.
	Class *ACPWorkspaceClassBinding
	// BindingDigest is the canonical digest over the fields above. It is part
	// of the RuntimePool identity.
	BindingDigest string
}

func acpSubstratePoolSuspendMode(binding *ACPRuntimeWorkspaceBinding) string {
	if binding == nil || binding.Provider != corev1alpha1.WorkspaceProviderSubstrate || binding.Class == nil ||
		!slices.Contains(binding.Class.AllowedOnDetach, string(workspacev1alpha1.WorkspaceOnDetachSuspend)) {
		return ""
	}
	return binding.Class.SuspendMode
}

func acpSubstratePoolSuspendModeMatches(binding *ACPRuntimeWorkspaceBinding, poolMode string) bool {
	permittedMode := acpSubstratePoolSuspendMode(binding)
	if poolMode == permittedMode {
		return true
	}
	// RuntimePool executionWorkspace is API-immutable. Older controllers
	// copied the profile's DataOnly mode even when a class allowed only Delete.
	// Accept that exact encoding so a frozen session can continue; new pools
	// use the empty executable mode, and every other mismatch stays rejected.
	return permittedMode == "" &&
		poolMode == string(acpworkspacev1alpha1.SubstrateSuspendModeDataOnly) &&
		binding != nil && binding.Provider == corev1alpha1.WorkspaceProviderSubstrate && binding.Class != nil &&
		binding.Class.SuspendMode == string(acpworkspacev1alpha1.SubstrateSuspendModeDataOnly) &&
		!slices.Contains(binding.Class.AllowedOnDetach, string(workspacev1alpha1.WorkspaceOnDetachSuspend))
}

// resolveACPWorkspaceBindingWithClass distills a legacy provider-shaped or
// class-shaped execution-workspace request into the canonical ACP binding. The
// class-shaped path consumes the pre-resolved, frozen class data so the
// function stays pure over its inputs.
//
//nolint:gocyclo // Every unsupported-capability rejection is audited in one place.
func resolveACPWorkspaceBindingWithClass(
	task *corev1alpha1.Task,
	defaultProvider corev1alpha1.WorkspaceProvider,
	enforceNamespaceIsolation bool,
	sessionUID string,
	resolvedClass *acpResolvedWorkspaceClass,
) (*ACPRuntimeWorkspaceBinding, error) {
	if task == nil || task.Spec.Execution == nil || task.Spec.Execution.Workspace == nil {
		return nil, nil
	}
	ws := task.Spec.Execution.Workspace
	if ws.ClassRef != nil {
		if resolvedClass == nil {
			return nil, fmt.Errorf("execution workspace classRef requires a resolved workspace class before binding")
		}
		return resolveACPClassWorkspaceBinding(task, sessionUID, resolvedClass)
	}
	if resolvedClass != nil {
		return nil, fmt.Errorf("a resolved workspace class was supplied without a classRef-shaped request")
	}
	if !ws.Enabled {
		return nil, nil
	}
	if task.UID == "" {
		return nil, fmt.Errorf("task UID is required for an execution workspace binding")
	}
	provider := resolveWorkspaceProvider(ws, defaultProvider)
	templateNamespace := ""
	templateName := ""
	switch provider {
	case corev1alpha1.WorkspaceProviderAgentSandbox:
		if ws.TemplateRef != nil {
			return nil, fmt.Errorf(
				"execution workspace templateRef selects a legacy worker-path sandbox template; ACP RuntimeSessions run only in controller-rendered sandbox templates, so templateRef must be omitted",
			)
		}
	case corev1alpha1.WorkspaceProviderSubstrate:
		// Substrate templates carry operator-owned infrastructure (worker pool
		// placement, runsc build, snapshot location) that the controller cannot
		// invent, so an explicit base template reference is required. The
		// controller renders its own derived runtime template from it; the
		// referenced template's containers never execute ACP work.
		if ws.TemplateRef == nil || strings.TrimSpace(ws.TemplateRef.Name) == "" {
			return nil, fmt.Errorf("execution workspace provider substrate requires templateRef.name naming the operator-owned infrastructure ActorTemplate")
		}
		templateName = strings.TrimSpace(ws.TemplateRef.Name)
		templateNamespace = strings.TrimSpace(ws.TemplateRef.Namespace)
		if templateNamespace == "" {
			templateNamespace = task.Namespace
		}
		if err := validateSubstrateWorkspaceTemplateReference(templateNamespace, templateName); err != nil {
			return nil, err
		}
		if enforceNamespaceIsolation && templateNamespace != task.Namespace {
			return nil, fmt.Errorf(
				"cross-namespace execution workspace templateRef is not allowed when namespace isolation is enforced: template %q is in namespace %q, task is in %q",
				templateName, templateNamespace, task.Namespace,
			)
		}
	default:
		return nil, fmt.Errorf(
			"execution workspace provider %q does not support ACP RuntimeSessions; there is no fallback execution path",
			provider,
		)
	}
	if ws.PoolRef != nil || ws.Boot || ws.Snapshot != nil || ws.Hibernation != nil {
		return nil, fmt.Errorf("execution workspace boot, poolRef, snapshot, and hibernation options are not supported for ACP RuntimeSessions")
	}
	if ws.OnDetach != "" {
		return nil, fmt.Errorf("execution workspace onDetach is not supported for ACP RuntimeSessions yet")
	}
	cleanup := ws.CleanupPolicy
	if cleanup == "" {
		cleanup = corev1alpha1.WorkspaceCleanupPolicyDelete
	}
	if cleanup != corev1alpha1.WorkspaceCleanupPolicyDelete {
		return nil, fmt.Errorf(
			"execution workspace cleanupPolicy %q is not supported for ACP RuntimeSessions; the execution workspace is always deleted after authenticated drain",
			cleanup,
		)
	}
	reuse, slot, sessionUID, sessionKey, err := resolveACPWorkspaceSessionScope(task, sessionUID)
	if err != nil {
		return nil, err
	}
	binding := &ACPRuntimeWorkspaceBinding{
		Provider: provider, ReusePolicy: reuse, CleanupPolicy: cleanup,
		WorkspaceSlot: slot, SessionUID: sessionUID, SessionKey: sessionKey,
		TemplateNamespace: templateNamespace, TemplateName: templateName,
	}
	digest, err := acpWorkspaceBindingDigest(binding)
	if err != nil {
		return nil, err
	}
	binding.BindingDigest = digest
	return binding, nil
}

// resolveACPWorkspaceSessionScope resolves the reuse policy, workspace slot,
// immutable Session UID, and session key shared by the legacy and class-backed
// binding paths.
func resolveACPWorkspaceSessionScope(
	task *corev1alpha1.Task,
	sessionUID string,
) (corev1alpha1.WorkspaceReusePolicy, string, string, string, error) {
	ws := task.Spec.Execution.Workspace
	reuse := ws.ReusePolicy
	if reuse == "" {
		reuse = corev1alpha1.WorkspaceReusePolicyNone
	}
	slot := strings.TrimSpace(ws.WorkspaceSlot)
	if slot == "" {
		slot = defaultWorkspaceSlotName
	}
	sessionKey := ""
	switch reuse {
	case corev1alpha1.WorkspaceReusePolicyNone:
		if task.Spec.SessionRef != nil && strings.TrimSpace(task.Spec.SessionRef.Name) != "" {
			return "", "", "", "", fmt.Errorf("execution workspace reusePolicy none cannot be used with spec.sessionRef; use reusePolicy session")
		}
		sessionUID = ""
		sessionKey = "task:" + string(task.UID)
	case corev1alpha1.WorkspaceReusePolicySession:
		if task.Spec.SessionRef == nil || strings.TrimSpace(task.Spec.SessionRef.Name) == "" {
			return "", "", "", "", fmt.Errorf("execution workspace reusePolicy session requires spec.sessionRef.name")
		}
		if slot != defaultWorkspaceSlotName {
			return "", "", "", "", fmt.Errorf("execution workspace reusePolicy session supports only workspaceSlot %q until RuntimeSession controls are slot-scoped", defaultWorkspaceSlotName)
		}
		sessionUID = strings.TrimSpace(sessionUID)
		if sessionUID == "" {
			return "", "", "", "", fmt.Errorf("execution workspace reusePolicy session requires an immutable Session UID")
		}
		if err := store.ValidateControlIdentifier("execution workspace Session UID", sessionUID); err != nil {
			return "", "", "", "", err
		}
		sessionKey = "session:" + sessionUID
	default:
		return "", "", "", "", fmt.Errorf("execution workspace reusePolicy %q is not supported", reuse)
	}
	return reuse, slot, sessionUID, sessionKey, nil
}

// resolveACPClassWorkspaceBinding builds the canonical binding for a
// class-shaped request from the pre-resolved class data. The CRD forbids
// combining classRef with the legacy request fields; the checks here keep that
// invariant fail-closed even without the admission layer.
func resolveACPClassWorkspaceBinding(
	task *corev1alpha1.Task,
	sessionUID string,
	resolvedClass *acpResolvedWorkspaceClass,
) (*ACPRuntimeWorkspaceBinding, error) {
	ws := task.Spec.Execution.Workspace
	if ws.Enabled || ws.Provider != "" || ws.TemplateRef != nil || ws.CleanupPolicy != "" ||
		ws.PoolRef != nil || ws.Boot || ws.Snapshot != nil || ws.Hibernation != nil {
		return nil, fmt.Errorf("execution workspace classRef cannot be combined with legacy enabled, provider, template, pool, cleanup, boot, snapshot, or hibernation settings")
	}
	if task.UID == "" {
		return nil, fmt.Errorf("task UID is required for an execution workspace binding")
	}
	reuse, slot, sessionUID, sessionKey, err := resolveACPWorkspaceSessionScope(task, sessionUID)
	if err != nil {
		return nil, err
	}
	if !acpWorkspaceReuseScopeAllowed(reuse, resolvedClass) {
		return nil, fmt.Errorf(
			"execution workspace reusePolicy %q is not allowed by class %q; allowed reuse scopes are %v",
			reuse, resolvedClass.Binding.Name, resolvedClass.AllowedReuseScopes,
		)
	}
	effectiveOnDetach, err := effectiveACPWorkspaceOnDetach(ws.OnDetach, resolvedClass)
	if err != nil {
		return nil, err
	}
	if effectiveOnDetach == workspacev1alpha1.WorkspaceOnDetachSuspend &&
		reuse != corev1alpha1.WorkspaceReusePolicySession {
		return nil, fmt.Errorf(
			"execution workspace onDetach Suspend requires reusePolicy session; a per-Task workspace has no continuation to resume into",
		)
	}
	templateNamespace := ""
	templateName := ""
	switch resolvedClass.Backend {
	case corev1alpha1.WorkspaceProviderAgentSandbox:
		if resolvedClass.SubstrateTemplateNamespace != "" || resolvedClass.SubstrateTemplateName != "" {
			return nil, fmt.Errorf("agent-sandbox execution workspace classes must not carry a Substrate template reference")
		}
	case corev1alpha1.WorkspaceProviderSubstrate:
		templateNamespace = resolvedClass.SubstrateTemplateNamespace
		templateName = resolvedClass.SubstrateTemplateName
		if err := validateSubstrateWorkspaceTemplateReference(templateNamespace, templateName); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf(
			"execution workspace class backend %q does not support ACP RuntimeSessions; there is no fallback execution path",
			resolvedClass.Backend,
		)
	}
	class := resolvedClass.Binding
	class.EffectiveOnDetach = string(effectiveOnDetach)
	binding := &ACPRuntimeWorkspaceBinding{
		Provider:      resolvedClass.Backend,
		ReusePolicy:   reuse,
		CleanupPolicy: corev1alpha1.WorkspaceCleanupPolicyDelete,
		WorkspaceSlot: slot, SessionUID: sessionUID, SessionKey: sessionKey,
		TemplateNamespace: templateNamespace, TemplateName: templateName,
		Class: &class,
	}
	digest, err := acpWorkspaceBindingDigest(binding)
	if err != nil {
		return nil, err
	}
	binding.BindingDigest = digest
	return binding, nil
}

func validateSubstrateWorkspaceTemplateReference(namespace, name string) error {
	if errs := validation.IsDNS1123Label(namespace); len(errs) != 0 {
		return fmt.Errorf("execution workspace substrate templateRef.namespace %q is invalid: %s", namespace, strings.Join(errs, "; "))
	}
	if errs := validation.IsDNS1123Subdomain(name); len(errs) != 0 {
		return fmt.Errorf("execution workspace substrate templateRef.name %q is invalid: %s", name, strings.Join(errs, "; "))
	}
	return nil
}

// validateACPWorkspaceBindingRequestWithClass validates a legacy or
// class-shaped request with a validation-only Session placeholder that is
// never persisted or used for pool identity.
func validateACPWorkspaceBindingRequestWithClass(
	task *corev1alpha1.Task,
	defaultProvider corev1alpha1.WorkspaceProvider,
	enforceNamespaceIsolation bool,
	resolvedClass *acpResolvedWorkspaceClass,
) (*ACPRuntimeWorkspaceBinding, error) {
	validationSessionUID := ""
	if task != nil && task.Spec.Execution != nil && task.Spec.Execution.Workspace != nil &&
		task.Spec.Execution.Workspace.ReusePolicy == corev1alpha1.WorkspaceReusePolicySession {
		validationSessionUID = "validation-only"
	}
	return resolveACPWorkspaceBindingWithClass(task, defaultProvider, enforceNamespaceIsolation, validationSessionUID, resolvedClass)
}

func acpWorkspaceSessionIdentityRequest(task *corev1alpha1.Task) (string, string, bool, error) {
	if task == nil || task.Spec.Execution == nil || task.Spec.Execution.Workspace == nil ||
		(!task.Spec.Execution.Workspace.Enabled && task.Spec.Execution.Workspace.ClassRef == nil) ||
		task.Spec.Execution.Workspace.ReusePolicy != corev1alpha1.WorkspaceReusePolicySession {
		return "", "", false, nil
	}
	if task.Spec.SessionRef == nil || strings.TrimSpace(task.Spec.SessionRef.Name) == "" {
		return "", "", false, fmt.Errorf("execution workspace reusePolicy session requires spec.sessionRef.name")
	}
	name := strings.TrimSpace(task.Spec.SessionRef.Name)
	transcriptBackedPrompt := task.Spec.SessionRef.PromptIncluded && strings.TrimSpace(task.Spec.SessionRef.ThroughMessageID) != ""
	sessionType := defaultACPSessionType
	if task.Spec.SessionRef.PromptIncluded && strings.HasPrefix(task.Spec.SessionRef.ThroughMessageID, "gateway:") {
		sessionType = store.SessionTypeGateway
	}
	return name, sessionType, transcriptBackedPrompt, nil
}

// planACPWorkspaceSessionUID resolves an existing immutable Session identity
// or proposes a fresh one without creating transcript or SessionControl state.
// The proposal is established only after the complete execution candidate has
// passed validation and canonical encoding.
func (r *TaskReconciler) planACPWorkspaceSessionUID(ctx context.Context, task *corev1alpha1.Task) (string, error) {
	name, sessionType, transcriptBackedPrompt, err := acpWorkspaceSessionIdentityRequest(task)
	if err != nil || name == "" {
		return "", err
	}
	if r.DurableControlStore == nil || r.SessionManager == nil || r.SessionManager.store == nil || r.ControllerEpochManager == nil {
		return "", errors.New("durable Session control, transcript store, and controller epoch are required for session-reused execution workspaces")
	}
	control, err := r.DurableControlStore.GetSessionControl(ctx, task.Namespace, name)
	if err == nil {
		if err := store.ValidateControlIdentifier("execution workspace Session UID", control.SessionUID); err != nil {
			return "", err
		}
		transcript, transcriptErr := r.SessionManager.store.GetSession(ctx, task.Namespace, name)
		if transcriptErr != nil {
			return "", fmt.Errorf("load transcript for execution-workspace Session control: %w", transcriptErr)
		}
		if err := validateTranscriptSession(transcript, sessionType); err != nil {
			return "", err
		}
		return control.SessionUID, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return "", err
	}
	if !task.Spec.SessionRef.Create && !transcriptBackedPrompt {
		return "", fmt.Errorf("session %s/%s does not exist and create=false: %w", task.Namespace, name, store.ErrNotFound)
	}
	transcript, transcriptErr := r.SessionManager.store.GetSession(ctx, task.Namespace, name)
	if transcriptErr == nil {
		if err := validateTranscriptSession(transcript, sessionType); err != nil {
			return "", err
		}
	} else if !errors.Is(transcriptErr, store.ErrNotFound) {
		return "", fmt.Errorf("load transcript session: %w", transcriptErr)
	} else if transcriptBackedPrompt && !task.Spec.SessionRef.Create {
		return "", fmt.Errorf("session %s/%s does not exist and create=false: %w", task.Namespace, name, store.ErrNotFound)
	}
	uid, err := newACPSessionUID()
	if err != nil {
		return "", fmt.Errorf("generate execution-workspace Session UID proposal: %w", err)
	}
	return uid, nil
}

// ensureACPWorkspaceSessionUID establishes the proposed immutable Session
// identity after candidate resolution. Concurrent creators converge on the
// first durable SessionControl UID; the caller rebuilds the still-unbound
// candidate if that winner differs from the proposal.
func (r *TaskReconciler) ensureACPWorkspaceSessionUID(
	ctx context.Context,
	task *corev1alpha1.Task,
	proposedUID string,
) (string, error) {
	name, sessionType, transcriptBackedPrompt, err := acpWorkspaceSessionIdentityRequest(task)
	if err != nil || name == "" {
		return "", err
	}
	if r.DurableControlStore == nil || r.SessionManager == nil || r.SessionManager.store == nil || r.ControllerEpochManager == nil {
		return "", errors.New("durable Session control, transcript store, and controller epoch are required for session-reused execution workspaces")
	}
	proposedUID = strings.TrimSpace(proposedUID)
	if err := store.ValidateControlIdentifier("proposed execution workspace Session UID", proposedUID); err != nil {
		return "", err
	}
	if !task.Spec.SessionRef.Create && !transcriptBackedPrompt {
		if _, err := r.DurableControlStore.GetSessionControl(ctx, task.Namespace, name); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return "", fmt.Errorf("session %s/%s does not exist and create=false: %w", task.Namespace, name, store.ErrNotFound)
			}
			return "", err
		}
	}
	fence, err := r.ControllerEpochManager.CurrentFence(ctx)
	if err != nil {
		return "", err
	}
	continuity, err := NewHarnessV1SessionContinuity(HarnessV1SessionContinuityConfig{
		SessionControls: r.DurableControlStore,
		Transcripts:     r.SessionManager.store,
		NewSessionUID:   func() (string, error) { return proposedUID, nil },
	})
	if err != nil {
		return "", err
	}
	control, err := continuity.EnsureSession(ctx, ACPEnsureSessionRequest{
		Namespace: task.Namespace, SessionName: name, SessionType: sessionType,
		RequireExistingTranscript: transcriptBackedPrompt && !task.Spec.SessionRef.Create,
		Fence:                     fence,
	})
	if err != nil {
		return "", err
	}
	return control.SessionUID, nil
}

// acpWorkspaceBindingDigest canonically digests the binding identity fields.
// Class-path keys are added only for class-backed bindings so every legacy
// binding digest remains byte-identical to its pre-class encoding.
func acpWorkspaceBindingDigest(binding *ACPRuntimeWorkspaceBinding) (string, error) {
	return acpWorkspaceBindingDigestWithClassOnDetach(binding, false)
}

// legacyACPWorkspaceBindingDigest reproduces schema-v1 class binding digests
// written before the effective detach action became attachment-scoped.
func legacyACPWorkspaceBindingDigest(binding *ACPRuntimeWorkspaceBinding) (string, error) {
	return acpWorkspaceBindingDigestWithClassOnDetach(binding, true)
}

func acpWorkspaceBindingDigestWithClassOnDetach(
	binding *ACPRuntimeWorkspaceBinding,
	includeClassOnDetach bool,
) (string, error) {
	if binding == nil {
		return "", fmt.Errorf("execution workspace binding is required")
	}
	fields := map[string]string{
		"provider":                   string(binding.Provider),
		"reusePolicy":                string(binding.ReusePolicy),
		"cleanupPolicy":              string(binding.CleanupPolicy),
		acpWorkspaceSlotMapKey:       binding.WorkspaceSlot,
		acpWorkspaceSessionUIDMapKey: binding.SessionUID,
		"sessionKey":                 binding.SessionKey,
		"templateNamespace":          binding.TemplateNamespace,
		"templateName":               binding.TemplateName,
	}
	if binding.Class != nil {
		fields["className"] = binding.Class.Name
		fields["classUID"] = binding.Class.UID
		fields["classGeneration"] = fmt.Sprintf("%d", binding.Class.Generation)
		fields["classProfileHash"] = binding.Class.ProfileHash
		fields["classProviderName"] = binding.Class.ProviderName
		fields["classProviderUID"] = binding.Class.ProviderUID
		fields["classProviderConfigUID"] = binding.Class.ProviderConfigUID
		if includeClassOnDetach {
			fields["classOnDetach"] = binding.Class.EffectiveOnDetach
		}
		// The effective detach action is deliberately NOT part of the pool
		// binding: it is attachment-scoped (a session suspended by one Task
		// can be continued by a Task selecting Delete), and folding it into
		// the immutable physical-pool identity would fail pool validation for
		// every continuation that chooses a different allowed action.
		if binding.Class.SuspendMode != "" {
			fields["classSuspendMode"] = binding.Class.SuspendMode
		}
		if binding.Class.SandboxVolume != nil {
			fields["classSandboxVolume"] = strings.Join([]string{
				binding.Class.SandboxVolume.StorageClassName,
				binding.Class.SandboxVolume.StorageClassUID,
				strings.Join(binding.Class.SandboxVolume.AccessModes, ","),
				binding.Class.SandboxVolume.Capacity,
			}, "|")
		}
		if binding.Class.MaxSuspendedWorkspaces != nil {
			fields["classMaxSuspended"] = fmt.Sprintf("%d", *binding.Class.MaxSuspendedWorkspaces)
		}
	}
	return acpDomainDigest("execution-workspace-binding", fields)
}

// applyACPWorkspaceBindingToPlan folds a resolved workspace binding into the
// RuntimePool identity so workspace-backed sessions never share a plain pool.
func applyACPWorkspaceBindingToPlan(plan ACPRuntimePlan, binding *ACPRuntimeWorkspaceBinding) (ACPRuntimePlan, error) {
	if binding == nil {
		return plan, nil
	}
	if strings.TrimSpace(binding.BindingDigest) == "" {
		return ACPRuntimePlan{}, fmt.Errorf("execution workspace binding digest is required")
	}
	identityFields := map[string]string{"workspaceBindingDigest": binding.BindingDigest}
	poolKind := plan.Profile.ProviderKind
	if binding.ReusePolicy == corev1alpha1.WorkspaceReusePolicySession {
		// One immutable Session UID owns one physical workspace. Keep its pool
		// name stable across every mutable runtime and workspace-selection input
		// so an incompatible continuation reaches the existing pool and fails
		// closed instead of silently materializing a fresh filesystem.
		identityFields = map[string]string{
			acpWorkspaceSessionUIDMapKey: binding.SessionUID,
			acpWorkspaceSlotMapKey:       binding.WorkspaceSlot,
		}
		poolKind = "session"
	} else {
		identityFields[acpRuntimePoolIdentityProfileDigestKey] = string(plan.Digest)
		identityFields[acpRuntimePoolIdentityRuntimeImageKey] = plan.Image
	}
	identity, err := acpDomainDigest("runtime-pool-identity", identityFields)
	if err != nil {
		return ACPRuntimePlan{}, err
	}
	workspace := *binding
	plan.PoolName = acpWorkspaceRuntimePoolName(poolKind, harnessv2.ProfileDigest(identity))
	plan.Workspace = &workspace
	return plan, nil
}

func acpWorkspaceRuntimePoolName(providerKind string, digest harnessv2.ProfileDigest) string {
	hexDigest := strings.TrimPrefix(string(digest), "sha256:")
	return fmt.Sprintf("acp-ws-%s-%s", providerKind, hexDigest[:16])
}

// projectACPExecutionWorkspaceStatus advances the provider-neutral Task
// workspace projection alongside the ACP execution state machine:
// Pending -> Ready once the prompt is running in the workspace-backed pool,
// and Pending/Ready -> Released once the attempt settles terminally. It never
// writes provider-native identifiers and never overrides a Failed projection.
func (r *TaskReconciler) projectACPExecutionWorkspaceStatus(ctx context.Context, task *corev1alpha1.Task) error {
	current := task.Status.ExecutionWorkspace
	if task.Spec.Type != corev1alpha1.TaskTypeAgent || current == nil || task.Status.Execution == nil || !taskManagedByACP(task) {
		return nil
	}
	if (task.Status.Execution.State == corev1alpha1.TaskExecutionStateRunning ||
		task.Status.Execution.State == corev1alpha1.TaskExecutionStateSettling) &&
		current.Phase == corev1alpha1.ExecutionWorkspacePhaseReady {
		return r.refreshACPClassAttachmentIdentity(ctx, task)
	}
	update := statusrules.Update{
		Provider:      current.Provider,
		ReusePolicy:   current.ReusePolicy,
		CleanupPolicy: current.CleanupPolicy,
		Reused:        current.Reused,
	}
	switch {
	case taskExecutionStateTerminal(task.Status.Execution.State):
		if task.Status.Execution.State == corev1alpha1.TaskExecutionStateSucceeded &&
			(task.Status.Delivery == nil || !store.IsTerminalPromptDeliveryState(store.PromptDeliveryState(task.Status.Delivery.State))) {
			return nil
		}
		if current.Phase != corev1alpha1.ExecutionWorkspacePhasePending && current.Phase != corev1alpha1.ExecutionWorkspacePhaseReady {
			return nil
		}
		update.Phase = corev1alpha1.ExecutionWorkspacePhaseReleased
		update.Reason = corev1alpha1.ExecutionWorkspaceReasonReleased
		update.Message = "RuntimeSession attempt settled; this Task's workspace demand is released"
	case task.Status.Execution.State == corev1alpha1.TaskExecutionStateRunning ||
		task.Status.Execution.State == corev1alpha1.TaskExecutionStateSettling:
		if current.Phase != corev1alpha1.ExecutionWorkspacePhasePending {
			return nil
		}
		update.Phase = corev1alpha1.ExecutionWorkspacePhaseReady
		update.Reason = corev1alpha1.ExecutionWorkspaceReasonReady
		update.Message = "RuntimeSession is executing in a workspace-provider-backed RuntimePool"
	default:
		return nil
	}
	next := update.Status()
	statusrules.PreserveReadyTelemetry(next, current)
	if err := r.projectACPClassAttachmentIdentity(ctx, task, next); err != nil {
		// The rebuilt projection has no class-backed identity yet; persisting
		// the advanced phase over a transient read failure would drop
		// ClassRef/WorkspaceRef/State/AttachedEpoch with no retry (the phase
		// gate above would then skip this projection forever). Surface the
		// error so the transition itself retries.
		return err
	}
	base := task.DeepCopy()
	task.Status.ExecutionWorkspace = next
	return r.Status().Patch(ctx, task, client.MergeFrom(base))
}

func (r *TaskReconciler) refreshACPClassAttachmentIdentity(ctx context.Context, task *corev1alpha1.Task) error {
	current := task.Status.ExecutionWorkspace
	if current == nil || strings.TrimSpace(task.Labels[acpExecutionWorkspaceLinkLabel]) == "" {
		return nil
	}
	next := current.DeepCopy()
	next.ClassRef = nil
	next.WorkspaceRef = nil
	next.State = ""
	next.AttachedEpoch = 0
	if err := r.projectACPClassAttachmentIdentity(ctx, task, next); err != nil {
		return err
	}
	if reflect.DeepEqual(current.ClassRef, next.ClassRef) &&
		reflect.DeepEqual(current.WorkspaceRef, next.WorkspaceRef) &&
		current.State == next.State && current.AttachedEpoch == next.AttachedEpoch {
		return nil
	}
	now := metav1.Now()
	next.LastUpdateTime = &now
	base := task.DeepCopy()
	task.Status.ExecutionWorkspace = next
	return r.Status().Patch(ctx, task, client.MergeFrom(base))
}

// projectACPClassAttachmentIdentity surfaces the epoch-fenced class attachment
// in Task status for class-backed executions: the selected class, the concrete
// workspace incarnation, its observed state, and the enforced attachment
// epoch. Session-reused workspaces carry no Task owner reference, so the
// generic workspace controller cannot project these fields; the ACP
// projection is their only writer.
func (r *TaskReconciler) projectACPClassAttachmentIdentity(
	ctx context.Context,
	task *corev1alpha1.Task,
	next *corev1alpha1.ExecutionWorkspaceStatus,
) error {
	name := strings.TrimSpace(task.Labels[acpExecutionWorkspaceLinkLabel])
	if name == "" || next == nil {
		return nil
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, workspace); err != nil {
		if apierrors.IsNotFound(err) {
			// A deleted workspace legitimately leaves no identity to project.
			return nil
		}
		return fmt.Errorf("project class attachment identity: %w", err)
	}
	if recorded := task.Annotations[acpExecutionWorkspaceUIDAnnotation]; recorded != "" &&
		recorded != string(workspace.UID) {
		return nil
	}
	if workspace.Spec.ClassBinding.Name != "" {
		next.ClassRef = &corev1alpha1.WorkspaceClassReference{Name: workspace.Spec.ClassBinding.Name}
	}
	next.WorkspaceRef = &corev1alpha1.WorkspaceObjectReference{Name: workspace.Name, UID: string(workspace.UID)}
	next.State = string(workspace.Status.State)
	if attachment := workspace.Spec.Attachment; attachment != nil && attachment.TaskRef.UID == task.UID &&
		workspace.Status.AttachedEpoch == attachment.Epoch {
		// The requested spec epoch and the adapter-enforced status epoch
		// deliberately diverge while attachment is pending and after
		// max-lifetime enforcement clears the enforced epoch; the Task may
		// only claim an epoch the adapter is actually enforcing for it.
		next.AttachedEpoch = attachment.Epoch
	}
	return nil
}

// validateACPWorkspaceBindingValues re-verifies a frozen snapshot workspace
// binding without consulting live cluster state.
//
//nolint:gocyclo // Frozen binding validation keeps all fail-closed identity checks in one auditable boundary.
func validateACPWorkspaceBindingValues(binding *ACPRuntimeWorkspaceBinding) error {
	if binding == nil {
		return nil
	}
	switch binding.Provider {
	case corev1alpha1.WorkspaceProviderAgentSandbox:
		if binding.TemplateNamespace != "" || binding.TemplateName != "" {
			return fmt.Errorf("frozen agent-sandbox execution workspace binding must not carry a template reference")
		}
	case corev1alpha1.WorkspaceProviderSubstrate:
		if strings.TrimSpace(binding.TemplateNamespace) == "" || strings.TrimSpace(binding.TemplateName) == "" {
			return fmt.Errorf("frozen substrate execution workspace binding is missing the infrastructure template reference")
		}
		if err := validateSubstrateWorkspaceTemplateReference(binding.TemplateNamespace, binding.TemplateName); err != nil {
			return fmt.Errorf("frozen substrate execution workspace binding is invalid: %w", err)
		}
	default:
		return fmt.Errorf("frozen execution workspace provider %q is not supported", binding.Provider)
	}
	if binding.Class != nil && strings.TrimSpace(binding.Class.ProviderConfigUID) == "" {
		return fmt.Errorf("frozen class-backed execution workspace binding is missing the provider config identity")
	}
	if binding.CleanupPolicy != corev1alpha1.WorkspaceCleanupPolicyDelete {
		return fmt.Errorf("frozen execution workspace cleanup policy %q is not supported", binding.CleanupPolicy)
	}
	if binding.ReusePolicy != corev1alpha1.WorkspaceReusePolicyNone && binding.ReusePolicy != corev1alpha1.WorkspaceReusePolicySession {
		return fmt.Errorf("frozen execution workspace reuse policy %q is not supported", binding.ReusePolicy)
	}
	if strings.TrimSpace(binding.WorkspaceSlot) == "" || strings.TrimSpace(binding.SessionKey) == "" {
		return fmt.Errorf("frozen execution workspace binding is incomplete")
	}
	switch binding.ReusePolicy {
	case corev1alpha1.WorkspaceReusePolicyNone:
		if binding.SessionUID != "" || !strings.HasPrefix(binding.SessionKey, "task:") || strings.TrimPrefix(binding.SessionKey, "task:") == "" {
			return fmt.Errorf("frozen per-Task execution workspace binding carries an invalid session identity")
		}
	case corev1alpha1.WorkspaceReusePolicySession:
		if strings.TrimSpace(binding.SessionUID) == "" || binding.SessionKey != "session:"+binding.SessionUID || binding.WorkspaceSlot != defaultWorkspaceSlotName {
			return fmt.Errorf("frozen session-reused execution workspace binding carries an invalid immutable Session identity")
		}
		if err := store.ValidateControlIdentifier("frozen execution workspace Session UID", binding.SessionUID); err != nil {
			return err
		}
	}
	if err := validateACPWorkspaceClassBindingValues(binding.Class); err != nil {
		return err
	}
	if binding.Class != nil && binding.Class.EffectiveOnDetach == string(workspacev1alpha1.WorkspaceOnDetachSuspend) {
		switch binding.Provider {
		case corev1alpha1.WorkspaceProviderSubstrate:
			if binding.Class.SandboxVolume != nil {
				return fmt.Errorf("frozen substrate execution workspace binding must not carry an agent-sandbox durable volume")
			}
		case corev1alpha1.WorkspaceProviderAgentSandbox:
			if binding.Class.SandboxVolume == nil {
				return fmt.Errorf("frozen agent-sandbox execution workspace binding permits Suspend without a frozen durable volume")
			}
		default:
			return fmt.Errorf("frozen execution workspace binding permits Suspend for provider %q", binding.Provider)
		}
		if binding.ReusePolicy != corev1alpha1.WorkspaceReusePolicySession {
			return fmt.Errorf("frozen execution workspace binding permits Suspend without session reuse")
		}
	}
	digest, err := acpWorkspaceBindingDigest(binding)
	if err != nil {
		return err
	}
	if digest != binding.BindingDigest {
		return fmt.Errorf("frozen execution workspace binding digest does not match its canonical identity")
	}
	return nil
}

// validateSnapshotACPWorkspaceBindingValues accepts the prior schema-v1 class
// digest while re-verifying an immutable snapshot. New bindings never use this
// compatibility path.
func validateSnapshotACPWorkspaceBindingValues(binding *ACPRuntimeWorkspaceBinding) error {
	if binding == nil {
		return nil
	}
	currentDigest, err := acpWorkspaceBindingDigest(binding)
	if err != nil {
		return err
	}
	canonical := *binding
	canonical.BindingDigest = currentDigest
	if err := validateACPWorkspaceBindingValues(&canonical); err != nil {
		return err
	}
	if binding.BindingDigest == currentDigest {
		return nil
	}
	if binding.Class != nil {
		legacyDigest, err := legacyACPWorkspaceBindingDigest(binding)
		if err != nil {
			return err
		}
		if binding.BindingDigest == legacyDigest {
			return nil
		}
	}
	return fmt.Errorf("frozen execution workspace binding digest does not match its canonical identity")
}
