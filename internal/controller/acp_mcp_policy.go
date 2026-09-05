package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/tools"
)

// Only tools whose every invocation leaves durable state unchanged belong here.
// check_messages is consequential because mark_read defaults to true.
var readOnlyBrokeredTools = map[string]struct{}{
	"check_pr_review_marker": {}, "check_pull_request_ci": {}, "check_task_progress": {},
	"fetch_task_output": {}, "file_read": {}, "get_issue": {},
	"list_agents": {}, "list_issues": {}, "list_pull_requests": {}, "list_tasks": {},
	"list_tools": {}, "recall_memory": {}, "search_transcript": {}, "wait_for_task": {},
	"wait_for_tasks": {}, "web_fetch": {}, "web_search": {},
}

var controllerLocalOnlyTools = map[string]struct{}{
	"code_exec":  {},
	"file_read":  {},
	"file_write": {},
}

const (
	providerNativeToolBash      = "Bash"
	providerNativeToolEdit      = "Edit"
	providerNativeToolGlob      = "Glob"
	providerNativeToolGrep      = "Grep"
	providerNativeToolRead      = "Read"
	providerNativeToolWebFetch  = "WebFetch"
	providerNativeToolWebSearch = "WebSearch"
	providerNativeToolWrite     = "Write"
)

var providerNativeTools = map[string]map[string]string{
	"codex":    canonicalToolSet(acp.BuiltInRuntimeNativeToolNames("codex")...),
	"claude":   canonicalToolSet(acp.BuiltInRuntimeNativeToolNames("claude")...),
	"copilot":  canonicalToolSet(acp.BuiltInRuntimeNativeToolNames("copilot")...),
	"opencode": canonicalToolSet(acp.BuiltInRuntimeNativeToolNames("opencode")...),
}

func canonicalToolSet(names ...string) map[string]string {
	result := make(map[string]string, len(names))
	for _, name := range names {
		result[strings.ToLower(name)] = name
	}
	return result
}

// normalizeACPProviderNativeToolPolicy expands only the provider-native tools
// that are implicit when the runtime allowlist is empty. Brokered/custom tools
// are never implicit: when configured they already appear in allowed, so the
// explicit list is returned unchanged and those descriptors remain intact.
func normalizeACPProviderNativeToolPolicy(
	provider string,
	allowed, disallowed []string,
	allowBash bool,
) ([]string, []string, bool) {
	return acp.NormalizeBuiltInRuntimeToolPolicy(provider, allowed, disallowed, allowBash)
}

func buildRuntimeSessionMCPConfiguration(
	ctx context.Context,
	reader client.Reader,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
	profile harnessv2.RuntimeProfile,
) (harnessv2.MCPPolicyConfiguration, error) {
	return buildRuntimeSessionMCPConfigurationWithRegistry(ctx, reader, task, agent, profile, tools.DefaultRegistry)
}

func buildRuntimeSessionMCPConfigurationWithRegistry(
	ctx context.Context,
	reader client.Reader,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
	profile harnessv2.RuntimeProfile,
	registry *tools.Registry,
) (harnessv2.MCPPolicyConfiguration, error) {
	if task == nil || agent == nil {
		return harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("task and Agent are required for MCP policy")
	}
	allowed := effectiveACPAllowedTools(task, agent)
	disallowed := []string(nil)
	if task.Spec.AgentRuntime != nil {
		disallowed = sortedUnique(task.Spec.AgentRuntime.DisallowedTools)
	}
	allowBash := true
	if agent.Spec.Runtime != nil && agent.Spec.Runtime.DefaultAllowBash != nil {
		allowBash = *agent.Spec.Runtime.DefaultAllowBash
	}
	if task.Spec.AgentRuntime != nil && task.Spec.AgentRuntime.AllowBash != nil {
		allowBash = *task.Spec.AgentRuntime.AllowBash
	}
	allowed, disallowed, allowBash = normalizeACPRuntimeToolPolicy(
		profile.ProviderKind, corev1alpha1.WorkspaceIntent(profile.WorkspaceIntent), allowed, disallowed, allowBash,
	)
	approval := harnessv2.MCPApprovalPolicy{}
	if agent.Spec.Coordination != nil {
		approval.RequiredTools = sortedUnique(agent.Spec.Coordination.ApprovalRequiredTools)
	}
	return buildMCPPolicyConfigurationWithRegistry(
		ctx, reader, task.Namespace, profile, allowed, disallowed, allowBash, approval, registry,
	)
}

func buildExternalRuntimeSessionMCPConfigurationWithRegistry(
	ctx context.Context,
	reader client.Reader,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
	runtime *corev1alpha1.AgentRuntime,
	profile harnessv2.RuntimeProfile,
	registry *tools.Registry,
) (harnessv2.MCPPolicyConfiguration, error) {
	if task == nil || agent == nil || runtime == nil || runtime.Spec.Capabilities == nil {
		return harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("task, Agent, and external AgentRuntime policy are required")
	}
	policy := runtime.Spec.Capabilities.MCPPolicy
	if err := validateAgentRuntimeMCPPolicyClaims(policy, profile); err != nil {
		return harnessv2.MCPPolicyConfiguration{}, err
	}
	if task.Spec.AgentRuntime == nil || task.Spec.AgentRuntime.AllowedTools == nil {
		return harnessv2.MCPPolicyConfiguration{}, permanentACPAgentConfiguration(
			fmt.Errorf("task agentRuntime.allowedTools must be an explicit list for an external AgentRuntime"),
		)
	}
	requested := effectiveACPAllowedTools(task, agent)
	if !slices.Equal(requested, policy.AllowedTools) {
		return harnessv2.MCPPolicyConfiguration{}, permanentACPAgentConfiguration(
			fmt.Errorf("task allowedTools do not exactly match the registered external AgentRuntime MCP policy"),
		)
	}
	return buildAgentRuntimeMCPConfigurationWithRegistry(ctx, reader, runtime, profile, registry)
}

func buildAgentRuntimeMCPConfigurationWithRegistry(
	ctx context.Context,
	reader client.Reader,
	runtime *corev1alpha1.AgentRuntime,
	profile harnessv2.RuntimeProfile,
	registry *tools.Registry,
) (harnessv2.MCPPolicyConfiguration, error) {
	if runtime == nil || runtime.Spec.Capabilities == nil {
		return harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("external AgentRuntime MCP policy is required")
	}
	policy := runtime.Spec.Capabilities.MCPPolicy
	if err := validateAgentRuntimeMCPPolicyClaims(policy, profile); err != nil {
		return harnessv2.MCPPolicyConfiguration{}, err
	}
	return buildMCPPolicyConfigurationWithRegistry(
		ctx,
		reader,
		runtime.Namespace,
		profile,
		append([]string{}, policy.AllowedTools...),
		append([]string{}, policy.DisallowedTools...),
		policy.AllowBash,
		agentRuntimeMCPApprovalPolicy(policy),
		registry,
	)
}

func validateAgentRuntimeMCPPolicyClaims(
	policy *corev1alpha1.AgentRuntimeMCPPolicySpec,
	profile harnessv2.RuntimeProfile,
) error {
	if policy == nil {
		return fmt.Errorf("external AgentRuntime capabilities.mcpPolicy is required")
	}
	for _, field := range []struct {
		name   string
		values []string
	}{
		{name: "allowedTools", values: policy.AllowedTools},
		{name: "disallowedTools", values: policy.DisallowedTools},
		{name: "approvalRequiredTools", values: policy.ApprovalRequiredTools},
	} {
		if field.values == nil {
			return fmt.Errorf("external AgentRuntime MCP policy %s must be an explicit list", field.name)
		}
		if !slices.Equal(field.values, sortedUnique(field.values)) {
			return fmt.Errorf("external AgentRuntime MCP policy %s must be sorted, unique, and non-empty by item", field.name)
		}
	}
	toolDigest, err := harnessv2.CanonicalRuntimeToolPolicyDigest(
		policy.AllowedTools, policy.DisallowedTools, policy.AllowBash,
	)
	if err != nil || toolDigest != profile.ToolPolicyDigest {
		return fmt.Errorf("registered external AgentRuntime MCP tool policy does not match the runtime profile")
	}
	approval := agentRuntimeMCPApprovalPolicy(policy)
	approvalDigest, err := harnessv2.CanonicalMCPApprovalPolicyDigest(approval)
	if err != nil || approvalDigest != profile.ApprovalPolicyDigest {
		return fmt.Errorf("registered external AgentRuntime MCP approval policy does not match the runtime profile")
	}
	mcpDigest, err := harnessv2.CanonicalMCPConfigurationDigest(policy.AllowedTools)
	if err != nil || mcpDigest != profile.MCPConfigurationDigest {
		return fmt.Errorf("registered external AgentRuntime MCP configuration does not match the runtime profile")
	}
	return nil
}

func agentRuntimeMCPApprovalPolicy(policy *corev1alpha1.AgentRuntimeMCPPolicySpec) harnessv2.MCPApprovalPolicy {
	if policy == nil || len(policy.ApprovalRequiredTools) == 0 {
		return harnessv2.MCPApprovalPolicy{}
	}
	return harnessv2.MCPApprovalPolicy{RequiredTools: append([]string(nil), policy.ApprovalRequiredTools...)}
}

func buildMCPPolicyConfigurationWithRegistry(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	profile harnessv2.RuntimeProfile,
	allowed, disallowed []string,
	allowBash bool,
	approval harnessv2.MCPApprovalPolicy,
	registry *tools.Registry,
) (harnessv2.MCPPolicyConfiguration, error) {
	if len(approval.RequiredTools) > 0 {
		return harnessv2.MCPPolicyConfiguration{}, permanentACPAgentConfiguration(
			fmt.Errorf("approval-required ACP MCP tools are unavailable until controller-owned permission review is implemented"),
		)
	}
	toolDigest, err := harnessv2.CanonicalRuntimeToolPolicyDigest(allowed, disallowed, allowBash)
	if err != nil || toolDigest != profile.ToolPolicyDigest {
		return harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("effective MCP tool policy does not match runtime profile")
	}
	descriptors, err := buildCanonicalMCPToolDescriptors(
		ctx, reader, namespace, profile.ProviderKind, allowed, disallowed, allowBash, registry,
	)
	if err != nil {
		return harnessv2.MCPPolicyConfiguration{}, err
	}
	descriptorDigest, err := harnessv2.CanonicalMCPToolDescriptorDigest(descriptors)
	if err != nil {
		return harnessv2.MCPPolicyConfiguration{}, err
	}
	approvalDigest, err := harnessv2.CanonicalMCPApprovalPolicyDigest(approval)
	if err != nil || approvalDigest != profile.ApprovalPolicyDigest {
		return harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("effective MCP approval policy does not match runtime profile")
	}
	mcpDigest, err := harnessv2.CanonicalMCPConfigurationDigest(allowed)
	if err != nil || mcpDigest != profile.MCPConfigurationDigest {
		return harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("effective MCP configuration does not match runtime profile")
	}
	configuration := harnessv2.MCPPolicyConfiguration{
		ToolPolicyDigest: toolDigest, ApprovalPolicyDigest: approvalDigest, MCPConfigurationDigest: mcpDigest,
		ToolPolicy: harnessv2.MCPToolPolicy{
			AllowedToolNames: allowed, DisallowedToolNames: disallowed, AllowBash: allowBash,
			Tools: descriptors, DescriptorDigest: descriptorDigest,
		},
		ApprovalPolicy: approval,
	}
	if err := configuration.ValidateProfile(profile); err != nil {
		return harnessv2.MCPPolicyConfiguration{}, err
	}
	return configuration, nil
}

func buildPromptMCPAuthorization(
	configuration harnessv2.MCPPolicyConfiguration,
	fence harnessv2.Fence,
	profile harnessv2.RuntimeProfile,
	metadata harnessv2.MutationMetadata,
	lease harnessv2.PromptLease,
	expiresAt time.Time,
) (harnessv2.PromptMCPAuthorization, error) {
	if err := configuration.ValidateProfile(profile); err != nil {
		return harnessv2.PromptMCPAuthorization{}, err
	}
	authorization := harnessv2.PromptMCPAuthorization{
		RuntimeSessionUID: fence.RuntimeSessionUID, SessionGeneration: fence.RuntimeSessionGeneration,
		TaskUID: metadata.TaskUID, TaskAttempt: metadata.TaskAttempt, PromptID: metadata.PromptID,
		LeaseGeneration: lease.Generation, ToolPolicyDigest: configuration.ToolPolicyDigest,
		ApprovalPolicyDigest:   configuration.ApprovalPolicyDigest,
		MCPConfigurationDigest: configuration.MCPConfigurationDigest,
		ToolPolicy:             configuration.ToolPolicy, ApprovalPolicy: configuration.ApprovalPolicy,
		ExpiresAt: expiresAt,
	}
	if err := authorization.ValidateForAt(metadata, lease, time.Now().UTC()); err != nil {
		return harnessv2.PromptMCPAuthorization{}, err
	}
	if err := authorization.ValidateProfile(profile); err != nil {
		return harnessv2.PromptMCPAuthorization{}, err
	}
	return authorization, nil
}

func buildCanonicalMCPToolDescriptors(
	ctx context.Context,
	reader client.Reader,
	namespace, provider string,
	allowed, disallowed []string,
	allowBash bool,
	registry *tools.Registry,
) ([]harnessv2.MCPToolDescriptor, error) {
	policy := harnessv2.MCPToolPolicy{AllowedToolNames: allowed, DisallowedToolNames: disallowed, AllowBash: allowBash}
	descriptors := make([]harnessv2.MCPToolDescriptor, 0, len(allowed))
	for _, name := range allowed {
		if !policy.Allows(name) {
			continue
		}
		if registry != nil {
			if tool, ok := registry.Get(name); ok {
				if _, localOnly := controllerLocalOnlyTools[name]; localOnly {
					return nil, permanentACPAgentConfiguration(fmt.Errorf("built-in tool %q is local-process-only and cannot be exposed through the controller MCP broker", name))
				}
				descriptors = append(descriptors, harnessv2.MCPToolDescriptor{
					Name: name, Description: tool.Description(), InputSchema: append(json.RawMessage(nil), tool.Parameters()...),
					Source: harnessv2.MCPToolSourceBrokeredBuiltin, Effect: brokeredToolEffect(name),
				})
				continue
			}
		}
		if native := providerNativeTools[strings.ToLower(provider)]; native != nil {
			if _, ok := native[strings.ToLower(name)]; ok {
				descriptors = append(descriptors, harnessv2.MCPToolDescriptor{
					Name: name, Description: "Provider-native tool; not exposed by the Orka MCP broker.",
					Source: harnessv2.MCPToolSourceProviderNative, Effect: providerNativeToolEffect(name),
				})
				continue
			}
		}
		if reader == nil {
			return nil, permanentACPAgentConfiguration(fmt.Errorf("allowed tool %q is not a known provider-native or brokered built-in tool", name))
		}
		custom := &corev1alpha1.Tool{}
		err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, custom)
		if apierrors.IsNotFound(err) {
			return nil, permanentACPAgentConfiguration(fmt.Errorf("allowed tool %q is not a known provider-native, built-in, or Tool resource", name))
		}
		if err != nil {
			return nil, fmt.Errorf("load allowed Tool %q: %w", name, err)
		}
		descriptor, descriptorErr := customACPMCPToolDescriptor(custom)
		if descriptorErr != nil {
			return nil, descriptorErr
		}
		descriptors = append(descriptors, descriptor)
	}
	sort.Slice(descriptors, func(i, j int) bool { return descriptors[i].Name < descriptors[j].Name })
	return descriptors, nil
}

func customACPMCPToolDescriptor(tool *corev1alpha1.Tool) (harnessv2.MCPToolDescriptor, error) {
	if tool == nil || strings.TrimSpace(tool.Name) == "" {
		return harnessv2.MCPToolDescriptor{}, fmt.Errorf("custom tool identity is required")
	}
	if tool.Spec.BrokeredToolClass == "" {
		return harnessv2.MCPToolDescriptor{}, fmt.Errorf("tool %q does not declare brokeredToolClass and cannot be exposed to ACP", tool.Name)
	}
	parameters := json.RawMessage(`{"type":"object","properties":{}}`)
	if tool.Spec.Parameters != nil && len(tool.Spec.Parameters.Raw) > 0 {
		parameters = append(json.RawMessage(nil), tool.Spec.Parameters.Raw...)
	}
	effect := harnessv2.MCPToolEffectConsequential
	if tool.Spec.BrokeredToolClass == corev1alpha1.AgentRuntimeBrokeredToolClassRead {
		effect = harnessv2.MCPToolEffectReadOnly
	}
	// Consequential calls run under the external-effect ledger, whose fixed
	// lease can only account for maxACPExternalEffectCallDuration of execution.
	// A longer configured timeout would be cut off mid-flight and settled
	// OutcomeUnknown, so reject it at exposure time instead.
	if effect == harnessv2.MCPToolEffectConsequential && tool.Spec.HTTP != nil && tool.Spec.HTTP.Timeout != nil &&
		tool.Spec.HTTP.Timeout.Duration > maxACPExternalEffectCallDuration {
		return harnessv2.MCPToolDescriptor{}, fmt.Errorf(
			"tool %q spec.http.timeout %s exceeds the maximum brokered consequential call duration %s",
			tool.Name, tool.Spec.HTTP.Timeout.Duration, maxACPExternalEffectCallDuration,
		)
	}
	definitionDigest, err := acpDomainDigest("mcp-custom-tool-definition", map[string]any{
		"uid": string(tool.UID), "generation": tool.Generation, "spec": tool.Spec,
		"endpoint": tool.Status.Endpoint, "actor": tool.Status.Actor,
	})
	if err != nil {
		return harnessv2.MCPToolDescriptor{}, err
	}
	descriptor := harnessv2.MCPToolDescriptor{
		Name: tool.Name, Description: tool.Spec.Description, InputSchema: parameters,
		Source: harnessv2.MCPToolSourceBrokeredCustom, Effect: effect, DefinitionDigest: definitionDigest,
	}
	if err := descriptor.Validate(); err != nil {
		return harnessv2.MCPToolDescriptor{}, err
	}
	return descriptor, nil
}

func brokeredToolEffect(name string) harnessv2.MCPToolEffect {
	if _, ok := readOnlyBrokeredTools[name]; ok {
		return harnessv2.MCPToolEffectReadOnly
	}
	return harnessv2.MCPToolEffectConsequential
}

func providerNativeToolEffect(name string) harnessv2.MCPToolEffect {
	switch strings.ToLower(name) {
	case "read", "glob", "grep", "websearch", "webfetch":
		return harnessv2.MCPToolEffectReadOnly
	default:
		return harnessv2.MCPToolEffectConsequential
	}
}
