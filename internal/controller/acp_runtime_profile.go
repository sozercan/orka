package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/types"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/tools"
)

type ACPRuntimeImages struct {
	Codex    string
	Claude   string
	Copilot  string
	Opencode string
}

const (
	acpRuntimePoolIdentityProfileDigestKey = "profileDigest"
	acpRuntimePoolIdentityRuntimeImageKey  = "runtimeImage"
)

type ACPRuntimePlan struct {
	PoolName string
	Image    string
	Profile  harnessv2.RuntimeProfile
	Digest   harnessv2.ProfileDigest
	// Workspace, when set, binds the pool to an execution-workspace provider.
	// It changes PoolName so workspace-backed sessions never share a plain pool.
	Workspace *ACPRuntimeWorkspaceBinding
}

type acpRuntimeDeliverySelection struct {
	plan                   ACPRuntimePlan
	allowPoolCreation      bool
	requiredRuntimePoolUID types.UID
}

// validateACPRuntimePlanningAgent gates ACP planning on a complete built-in
// runtime with an explicit orka.harness.v2 classification and a valid v2
// OpenCode shape. A missing selector is never protocol evidence.
func validateACPRuntimePlanningAgent(task *corev1alpha1.Task, agent *corev1alpha1.Agent) error {
	if task == nil || agent == nil || agent.Spec.Runtime == nil || agent.Spec.Runtime.Type == "" {
		return fmt.Errorf("built-in agent runtime is required")
	}
	if agent.BuiltInContractVersion() != corev1alpha1.AgentRuntimeContractHarnessV2 {
		return fmt.Errorf("ACP runtime planning requires an Agent explicitly classified %s; a missing runtime.contractVersion selector is never protocol evidence", corev1alpha1.AgentRuntimeContractHarnessV2)
	}
	return ValidateOpenCodeAgentSpec(agent)
}

func PlanACPRuntimeWithConfiguration(
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
	images ACPRuntimeImages,
	configuration harnessv2.AgentSessionConfiguration,
) (ACPRuntimePlan, error) {
	if err := validateACPRuntimePlanningAgent(task, agent); err != nil {
		return ACPRuntimePlan{}, err
	}
	provider := string(agent.Spec.Runtime.Type)
	model := ""
	if agent.Spec.Model != nil {
		model = strings.TrimSpace(agent.Spec.Model.Name)
	}
	if model == "" {
		return ACPRuntimePlan{}, fmt.Errorf("ACP runtime requires an explicit model")
	}
	if err := configuration.Validate(); err != nil {
		return ACPRuntimePlan{}, fmt.Errorf("ACP Agent session configuration: %w", err)
	}
	if configuration.ProviderKind != provider || configuration.Model != model ||
		configuration.MaxTurns != effectiveACPMaxTurns(task, agent) ||
		configuration.ReasoningEffort != effectiveACPReasoningEffort(agent) ||
		configuration.AgentUID != string(agent.UID) || configuration.AgentGeneration != agent.Generation {
		return ACPRuntimePlan{}, fmt.Errorf("resolved ACP Agent session configuration does not match Task and Agent")
	}
	intent := effectiveACPWorkspaceIntent(task)
	adapterDigests, image, err := acpRuntimeArtifacts(agent.Spec.Runtime.Type, images)
	if err != nil {
		return ACPRuntimePlan{}, err
	}
	if !ACPRuntimeImageAvailable(image) {
		return ACPRuntimePlan{}, fmt.Errorf("ACP runtime image for %s must be a configured digest-pinned image", provider)
	}
	allowed := effectiveACPAllowedTools(task, agent)
	disallowed := []string(nil)
	if task.Spec.AgentRuntime != nil {
		disallowed = sortedUnique(task.Spec.AgentRuntime.DisallowedTools)
	}
	allowBash := effectiveACPAllowBash(task, agent)
	allowed, disallowed, allowBash = normalizeACPRuntimeToolPolicy(provider, intent, allowed, disallowed, allowBash)
	if err := validateACPProviderNativePolicy(provider, intent, allowed, disallowed, allowBash); err != nil {
		return ACPRuntimePlan{}, err
	}
	if err := validateACPProviderSystemPrompt(provider, configuration); err != nil {
		return ACPRuntimePlan{}, err
	}
	for _, name := range allowed {
		if _, localOnly := controllerLocalOnlyTools[name]; localOnly {
			return ACPRuntimePlan{}, fmt.Errorf("built-in tool %q is local-process-only and cannot be exposed through the controller MCP broker", name)
		}
	}
	var modelLimits *harnessv2.ModelTokenLimits
	if agent.Spec.Runtime.Type == corev1alpha1.AgentRuntimeOpencode {
		modelLimits = &harnessv2.ModelTokenLimits{
			Context: int64(*agent.Spec.Model.ContextWindow),
			Output:  int64(*agent.Spec.Model.MaxTokens),
		}
	}
	agentDigest, err := harnessv2.CanonicalAgentConfigurationDigest(configuration)
	if err != nil {
		return ACPRuntimePlan{}, err
	}
	toolDigest, err := harnessv2.CanonicalRuntimeToolPolicyDigest(allowed, disallowed, allowBash)
	if err != nil {
		return ACPRuntimePlan{}, err
	}
	approvalTools := []string(nil)
	if agent.Spec.Coordination != nil {
		approvalTools = sortedUnique(agent.Spec.Coordination.ApprovalRequiredTools)
	}
	approvalDigest, err := harnessv2.CanonicalMCPApprovalPolicyDigest(harnessv2.MCPApprovalPolicy{RequiredTools: approvalTools})
	if err != nil {
		return ACPRuntimePlan{}, err
	}
	mcpDigest, err := harnessv2.CanonicalMCPConfigurationDigest(allowed)
	if err != nil {
		return ACPRuntimePlan{}, err
	}
	profile := harnessv2.RuntimeProfile{
		ACPProfile: harnessv2.ACPProfileV1, AdapterDigests: adapterDigests, ProviderKind: provider, Model: model,
		ModelLimits: modelLimits, AgentConfigurationDigest: agentDigest, ToolPolicyDigest: toolDigest, ApprovalPolicyDigest: approvalDigest,
		MCPConfigurationDigest: mcpDigest, WorkspaceIntent: harnessv2.WorkspaceIntent(intent),
		ProxyCredentialRole: "provider-inference", ProxyCredentialScope: "model:" + model, ResourceClass: "standard",
	}
	digest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		return ACPRuntimePlan{}, err
	}
	poolIdentityDigest, err := acpDomainDigest("runtime-pool-identity", map[string]string{
		acpRuntimePoolIdentityProfileDigestKey: string(digest), acpRuntimePoolIdentityRuntimeImageKey: image,
	})
	if err != nil {
		return ACPRuntimePlan{}, err
	}
	return ACPRuntimePlan{
		PoolName: acpRuntimePoolName(provider, harnessv2.ProfileDigest(poolIdentityDigest)),
		Image:    image, Profile: profile, Digest: digest,
	}, nil
}

// currentACPRuntimeDeliveryPlan validates the controller-owned runtime
// artifacts and refreshes the derived plain-pool identity while preserving the
// Task's immutable runtime profile. Workspace-backed pools keep their frozen
// plan because rotating their physical workspace has separate lifecycle and
// lineage requirements. A retired workspace image is usable only through an
// exact preexisting pool, so the caller must preserve the creation decision.
func currentACPRuntimeDeliveryPlan(plan ACPRuntimePlan, images ACPRuntimeImages) (acpRuntimeDeliverySelection, error) {
	adapterDigests, image, err := acpRuntimeArtifacts(
		corev1alpha1.AgentRuntimeType(strings.TrimSpace(plan.Profile.ProviderKind)),
		images,
	)
	if err != nil {
		return acpRuntimeDeliverySelection{}, err
	}
	image = strings.TrimSpace(image)
	if image != "" && !ACPRuntimeImageAvailable(image) {
		return acpRuntimeDeliverySelection{}, fmt.Errorf("current ACP runtime image for %s must be a configured digest-pinned image", plan.Profile.ProviderKind)
	}
	if !maps.Equal(adapterDigests, plan.Profile.AdapterDigests) {
		return acpRuntimeDeliverySelection{}, fmt.Errorf("current ACP runtime adapters for %s do not match the frozen runtime profile", plan.Profile.ProviderKind)
	}
	if plan.Workspace != nil {
		return acpRuntimeDeliverySelection{
			plan:              plan,
			allowPoolCreation: image != "" && image == strings.TrimSpace(plan.Image),
		}, nil
	}
	if !ACPRuntimeImageAvailable(image) {
		return acpRuntimeDeliverySelection{}, fmt.Errorf("current ACP runtime image for %s must be a configured digest-pinned image", plan.Profile.ProviderKind)
	}
	if image == plan.Image {
		return acpRuntimeDeliverySelection{plan: plan, allowPoolCreation: true}, nil
	}
	identity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
		acpRuntimePoolIdentityProfileDigestKey: string(plan.Digest), acpRuntimePoolIdentityRuntimeImageKey: image,
	})
	if err != nil {
		return acpRuntimeDeliverySelection{}, err
	}
	plan.Image = image
	plan.PoolName = acpRuntimePoolName(plan.Profile.ProviderKind, harnessv2.ProfileDigest(identity))
	return acpRuntimeDeliverySelection{plan: plan, allowPoolCreation: true}, nil
}

func configuredACPRuntimeImage(provider string, images ACPRuntimeImages) (string, error) {
	_, image, err := acpRuntimeArtifacts(corev1alpha1.AgentRuntimeType(strings.TrimSpace(provider)), images)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(image), nil
}

func acpRuntimePoolImageRequiresHistoricalRecovery(pool *corev1alpha1.RuntimePool, images ACPRuntimeImages) bool {
	if pool == nil || validateRuntimePoolImageReference(pool) != nil {
		return false
	}
	if pool.Spec.ExecutionWorkspace == nil {
		identity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
			acpRuntimePoolIdentityProfileDigestKey: pool.Spec.Runtime.Profile.Digest,
			acpRuntimePoolIdentityRuntimeImageKey:  strings.TrimSpace(pool.Spec.Runtime.Image),
		})
		if err != nil || pool.Name != acpRuntimePoolName(
			pool.Spec.Runtime.Profile.ProviderKind,
			harnessv2.ProfileDigest(identity),
		) {
			return false
		}
	}
	approved, err := configuredACPRuntimeImage(pool.Spec.Runtime.Profile.ProviderKind, images)
	return err == nil && approved != strings.TrimSpace(pool.Spec.Runtime.Image)
}

func acpRuntimePoolImageSuperseded(pool *corev1alpha1.RuntimePool, images ACPRuntimeImages) bool {
	return pool != nil && pool.Spec.ExecutionWorkspace == nil &&
		acpRuntimePoolImageRequiresHistoricalRecovery(pool, images)
}

func RuntimePoolProfileFromPlan(plan ACPRuntimePlan) corev1alpha1.RuntimePoolProfileSpec {
	var modelLimits *corev1alpha1.ModelTokenLimits
	if plan.Profile.ModelLimits != nil {
		modelLimits = &corev1alpha1.ModelTokenLimits{
			Context: plan.Profile.ModelLimits.Context,
			Output:  plan.Profile.ModelLimits.Output,
		}
	}
	return corev1alpha1.RuntimePoolProfileSpec{
		ProtocolVersion: corev1alpha1.RuntimePoolProtocolHarnessV2,
		Digest:          string(plan.Digest), DigestSchemaVersion: fmt.Sprintf("%d", harnessv2.ProfileDigestSchemaVersion),
		ACPProfile: plan.Profile.ACPProfile, AdapterDigests: cloneMap(plan.Profile.AdapterDigests),
		ProviderKind: plan.Profile.ProviderKind, Model: plan.Profile.Model, ModelLimits: modelLimits,
		AgentConfigurationDigest: plan.Profile.AgentConfigurationDigest, ToolPolicyDigest: plan.Profile.ToolPolicyDigest,
		ApprovalPolicyDigest: plan.Profile.ApprovalPolicyDigest, MCPConfigurationDigest: plan.Profile.MCPConfigurationDigest,
		WorkspaceIntent:     corev1alpha1.WorkspaceIntent(plan.Profile.WorkspaceIntent),
		ProxyCredentialRole: plan.Profile.ProxyCredentialRole, ProxyCredentialScope: plan.Profile.ProxyCredentialScope,
		ResourceClass: plan.Profile.ResourceClass,
	}
}

// acpRuntimePoolWorkspaceMatchesPlan requires an exact match between the
// pool's immutable execution-workspace binding and the frozen plan: plain
// plans never bind to workspace-backed pools and vice versa.
func acpRuntimePoolWorkspaceMatchesPlan(pool *corev1alpha1.RuntimePool, plan ACPRuntimePlan) bool {
	if pool == nil {
		return false
	}
	if plan.Workspace == nil {
		return pool.Spec.ExecutionWorkspace == nil
	}
	workspace := pool.Spec.ExecutionWorkspace
	if workspace == nil ||
		workspace.Provider != plan.Workspace.Provider ||
		workspace.BindingDigest != plan.Workspace.BindingDigest {
		return false
	}
	switch plan.Workspace.Provider {
	case corev1alpha1.WorkspaceProviderAgentSandbox:
		planSuspendMode := ""
		var planVolume *ACPSandboxDurableVolume
		if plan.Workspace.Class != nil {
			planSuspendMode = plan.Workspace.Class.SuspendMode
			planVolume = plan.Workspace.Class.SandboxVolume
		}
		if planVolume == nil {
			if workspace.AgentSandbox != nil {
				return false
			}
		} else {
			sandbox := workspace.AgentSandbox
			if sandbox == nil || sandbox.SuspendMode != planSuspendMode || sandbox.SuspendVolume == nil ||
				sandbox.SuspendVolume.StorageClassName != planVolume.StorageClassName ||
				sandbox.SuspendVolume.StorageClassUID != planVolume.StorageClassUID ||
				sandbox.SuspendVolume.Capacity != planVolume.Capacity ||
				!slices.Equal(sandbox.SuspendVolume.AccessModes, planVolume.AccessModes) {
				return false
			}
		}
		return workspace.Substrate == nil &&
			plan.Workspace.TemplateNamespace == "" && plan.Workspace.TemplateName == ""
	case corev1alpha1.WorkspaceProviderSubstrate:
		poolSuspendMode := ""
		if workspace.Substrate != nil {
			poolSuspendMode = workspace.Substrate.SuspendMode
		}
		// A stray agentSandbox block on a substrate pool is drift the CRD
		// cannot express away; reject it so cross-provider suspend settings
		// can never be smuggled onto a mismatched backend.
		return workspace.AgentSandbox == nil && workspace.Substrate != nil &&
			workspace.Substrate.BaseTemplateNamespace == plan.Workspace.TemplateNamespace &&
			workspace.Substrate.BaseTemplateName == plan.Workspace.TemplateName &&
			acpSubstratePoolSuspendModeMatches(plan.Workspace, poolSuspendMode)
	default:
		return false
	}
}

func acpRuntimePoolBindingMatches(status *corev1alpha1.TaskExecutionStatus, pool *corev1alpha1.RuntimePool) bool {
	return status != nil && pool != nil && pool.UID != "" &&
		strings.TrimSpace(status.RuntimePoolName) == pool.Name &&
		strings.TrimSpace(status.RuntimePoolUID) == string(pool.UID)
}

func effectiveACPWorkspaceIntent(task *corev1alpha1.Task) corev1alpha1.WorkspaceIntent {
	if task == nil {
		return corev1alpha1.WorkspaceIntentRead
	}
	workspace := task.Spec.Workspace
	if workspace != nil && workspace.Intent != "" {
		return workspace.Intent
	}
	return corev1alpha1.WorkspaceIntentRead
}

func effectiveACPAllowedTools(task *corev1alpha1.Task, agent *corev1alpha1.Agent) []string {
	var values []string
	if agent != nil && agent.Spec.Runtime != nil {
		runtime := agent.Spec.Runtime
		if runtime.Type == corev1alpha1.AgentRuntimeOpencode && runtime.DefaultAllowedTools == nil {
			values = acp.OpenCodeDefaultAllowedTools()
		} else if runtime.DefaultAllowedTools != nil {
			values = append([]string{}, runtime.DefaultAllowedTools...)
		}
	}
	if task != nil && task.Spec.AgentRuntime != nil && task.Spec.AgentRuntime.AllowedTools != nil {
		values = append([]string{}, task.Spec.AgentRuntime.AllowedTools...)
	}
	if taskRequestsReadOnlyAgent(task) && taskUsesReadOnlyAgentToolPreset(task) &&
		agent != nil && agent.Spec.Runtime != nil {
		brokered := repositoryMonitorReadOnlyBrokeredTools(task)
		// Repository-monitor presets use Claude-style path-scoped names, which
		// are not canonical provider-native descriptors, so translate the
		// preset into the exact read-only surface each runtime can enforce.
		switch agent.Spec.Runtime.Type {
		case corev1alpha1.AgentRuntimeOpencode:
			// OpenCode's Grep permission cannot carry the path-specific
			// secret-file exclusions applied to Read, so it stays disabled.
			values = []string{providerNativeToolRead, providerNativeToolGlob}
		case corev1alpha1.AgentRuntimeClaude, corev1alpha1.AgentRuntimeCopilot:
			// Copilot exposes the same read-only trio; it has no LS tool, so
			// the untranslated preset would be rejected as an invalid
			// runtime profile.
			values = []string{providerNativeToolGlob, providerNativeToolGrep, providerNativeToolRead}
		case corev1alpha1.AgentRuntimeCodex:
			// Codex has no per-tool switches; this exact surface is enforced
			// by the RuntimeSession boundary: elevation requests are rejected,
			// file writes are supervisor-mediated, and the read-intent
			// workspace delta classification fails any modifying turn.
			values = []string{providerNativeToolGlob, providerNativeToolGrep, providerNativeToolRead}
		}
		values = append(values, brokered...)
	}
	if task != nil {
		_, delegatedChild := task.Labels[labels.LabelParentTask]
		disableCoordinationToolInjection := task.Annotations[labels.AnnotationDisableCoordinationToolInject] == scheduledRunLabelValue
		runtimeRefAgent := agent != nil && agent.Spec.Runtime != nil && agent.Spec.Runtime.RuntimeRef != nil &&
			strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) != ""
		if delegatedChild && !disableCoordinationToolInjection && !runtimeRefAgent {
			values = append(values, "send_message", "check_messages")
		}
	}
	return sortedUnique(values)
}

func taskUsesReadOnlyAgentToolPreset(task *corev1alpha1.Task) bool {
	if task == nil || task.Spec.AgentRuntime == nil || task.Spec.AgentRuntime.AllowedTools == nil {
		return false
	}
	allowed := sortedUnique(task.Spec.AgentRuntime.AllowedTools)
	base := sortedUnique(readOnlyAgentAllowedTools())
	for _, required := range base {
		if !slices.Contains(allowed, required) {
			return false
		}
	}
	for _, name := range allowed {
		if slices.Contains(base, name) || name == tools.RunValidationToolName || name == repositoryMonitorWaitForTasksToolName {
			continue
		}
		return false
	}
	return true
}

func repositoryMonitorReadOnlyBrokeredTools(task *corev1alpha1.Task) []string {
	if task == nil || task.Spec.AgentRuntime == nil {
		return nil
	}
	var brokered []string
	for _, name := range task.Spec.AgentRuntime.AllowedTools {
		if name == tools.RunValidationToolName || name == repositoryMonitorWaitForTasksToolName {
			brokered = append(brokered, name)
		}
	}
	return sortedUnique(brokered)
}

func normalizeACPRuntimeToolPolicy(
	provider string,
	intent corev1alpha1.WorkspaceIntent,
	allowed, disallowed []string,
	allowBash bool,
) ([]string, []string, bool) {
	allowed, disallowed, allowBash = normalizeACPProviderNativeToolPolicy(provider, allowed, disallowed, allowBash)
	if !strings.EqualFold(provider, string(corev1alpha1.AgentRuntimeOpencode)) {
		return allowed, disallowed, allowBash
	}
	return acp.NormalizeOpenCodeToolPolicy(intent == corev1alpha1.WorkspaceIntentRead, allowed, disallowed, allowBash)
}

func effectiveACPMaxTurns(task *corev1alpha1.Task, agent *corev1alpha1.Agent) int32 {
	if task != nil && task.Spec.AgentRuntime != nil && task.Spec.AgentRuntime.MaxTurns != nil {
		return *task.Spec.AgentRuntime.MaxTurns
	}
	if agent != nil && agent.Spec.Runtime != nil && agent.Spec.Runtime.DefaultMaxTurns != nil {
		return *agent.Spec.Runtime.DefaultMaxTurns
	}
	return 50
}

func acpRuntimeArtifacts(runtime corev1alpha1.AgentRuntimeType, images ACPRuntimeImages) (map[string]string, string, error) {
	digests := acp.BuiltInRuntimeAdapterDigests(string(runtime))
	if digests == nil {
		return nil, "", fmt.Errorf("runtime %q is not supported by the ACP core pool", runtime)
	}
	var image string
	switch runtime {
	case corev1alpha1.AgentRuntimeCodex:
		image = images.Codex
	case corev1alpha1.AgentRuntimeClaude:
		image = images.Claude
	case corev1alpha1.AgentRuntimeCopilot:
		image = images.Copilot
	case corev1alpha1.AgentRuntimeOpencode:
		image = images.Opencode
	}
	return digests, strings.TrimSpace(image), nil
}

// ACPRuntimeImageAvailable reports whether a built-in runtime image is an
// immutable, non-placeholder reference suitable for task admission and chat.
func ACPRuntimeImageAvailable(image string) bool {
	image = strings.TrimSpace(image)
	if !digestPinnedImagePattern.MatchString(image) {
		return false
	}
	return !strings.HasSuffix(image, "@sha256:"+strings.Repeat("0", 64))
}

func acpDomainDigest(domain string, value any) (string, error) {
	canonical, err := harnessv2.CanonicalValue(value)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("orka.acp." + domain + "\x00"))
	_, _ = hash.Write(canonical)
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func acpRuntimePoolName(provider string, digest harnessv2.ProfileDigest) string {
	hexDigest := strings.TrimPrefix(string(digest), "sha256:")
	return fmt.Sprintf("acp-%s-%s", provider, hexDigest[:16])
}

func sortedUnique(values []string) []string {
	if values == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func cloneMap(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	maps.Copy(result, input)
	return result
}
