package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/acp"
	"github.com/orka-agents/orka/internal/labels"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const transactionSecretCredentialReadScope = "orka:secrets:credentials:read"

type transactionProviderInfo struct {
	Name      string
	Namespace string
	Type      string
}

type childTransactionContext struct {
	agentName           string
	agentNamespace      string
	childType           corev1alpha1.TaskType
	agent               *corev1alpha1.Agent
	provider            *corev1alpha1.Provider
	providerInfo        transactionProviderInfo
	model               string
	fallbacks           []transactionProviderModel
	aiTools             []string
	runtimeTools        []string
	runtimeBash         bool
	runtimeProviderKind string
}

type transactionProviderModel struct {
	provider transactionProviderInfo
	model    string
}

func validateChildTaskAgainstParentTransaction(ctx context.Context, k8sClient client.Reader, parent, child *corev1alpha1.Task, agentName string) error {
	if parent == nil || parent.Spec.Transaction == nil || child == nil {
		return nil
	}
	txCtx := parent.Spec.Transaction.Context
	childCtx, err := resolveChildTransactionContext(ctx, k8sClient, child, agentName)
	if err != nil {
		return err
	}
	agentName = childCtx.agentName
	agentNamespace := childCtx.agentNamespace

	if agentName == "" && child.Spec.AgentRef != nil {
		agentName = child.Spec.AgentRef.Name
	}

	if err := validateChildDependencyNamespaces(txCtx, child, agentNamespace, childCtx); err != nil {
		return err
	}
	if want := strings.TrimSpace(txCtx["taskType"]); want != "" && string(child.Spec.Type) != want {
		return fmt.Errorf("child task type %q does not match transaction context %q", child.Spec.Type, want)
	}
	if allowed, ok := transactionContextStringList(txCtx["allowedAgents"]); ok && !transactionAgentAllowed(agentName, agentNamespace, allowed) {
		return fmt.Errorf("child task agent %q is not allowed by transaction context", namespacedToolName(agentNamespace, agentName))
	} else if !ok {
		if want := strings.TrimSpace(txCtx["agent"]); want != "" && !transactionAgentMatches(agentName, agentNamespace, want) {
			return fmt.Errorf("child task agent %q does not match transaction context %q", namespacedToolName(agentNamespace, agentName), want)
		}
	}

	workspace := taskWorkspace(child)
	if workspace != nil {
		for _, credential := range []struct {
			role string
			ref  *corev1alpha1.WorkspaceCredentialReference
		}{
			{role: "source-read", ref: workspace.ReadCredentialRef},
			{role: "target-read", ref: workspace.PublicationReadCredentialRef},
			{role: "target-write", ref: workspace.PublicationCredentialRef},
			{role: "forge", ref: workspace.ForgeCredentialRef},
		} {
			if credential.ref == nil || strings.TrimSpace(credential.ref.Name) == "" {
				continue
			}
			if !TransactionHasScope(parent.Spec.Transaction, transactionSecretCredentialReadScope) {
				return fmt.Errorf("child task %s credential %q requires transaction scope %q", credential.role, credential.ref.Name, transactionSecretCredentialReadScope)
			}
			if want := strings.TrimSpace(txCtx["secret"]); want != "" && credential.ref.Name != want {
				return fmt.Errorf("child task %s credential %q does not match transaction context %q", credential.role, credential.ref.Name, want)
			}
		}
	}
	if len(txCtx) == 0 {
		return validateChildToolCredentialConstraints(ctx, k8sClient, parent, child, childCtx)
	}
	if err := validateChildWorkspaceSelectorConstraints(txCtx, workspace); err != nil {
		return err
	}
	if err := validateChildProviderModelConstraints(txCtx, childCtx); err != nil {
		return err
	}
	if err := validateChildToolConstraints(txCtx, childCtx); err != nil {
		return err
	}
	return validateChildToolCredentialConstraints(ctx, k8sClient, parent, child, childCtx)
}

func TransactionHasScope(tx *corev1alpha1.TaskTransaction, want string) bool {
	if tx == nil || strings.TrimSpace(want) == "" {
		return false
	}
	if slices.Contains(tx.Scopes, want) {
		return true
	}
	for _, scope := range strings.FieldsFunc(tx.Scope, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r' }) {
		if strings.TrimSpace(scope) == want {
			return true
		}
	}
	return false
}

func resolveChildTransactionContext(ctx context.Context, k8sClient client.Reader, child *corev1alpha1.Task, agentName string) (childTransactionContext, error) {
	childCtx := childTransactionContext{
		agentName:      agentName,
		agentNamespace: child.Namespace,
		childType:      child.Spec.Type,
	}
	if child.Spec.AgentRef != nil {
		if childCtx.agentName == "" {
			childCtx.agentName = child.Spec.AgentRef.Name
		}
		if child.Spec.AgentRef.Namespace != "" {
			childCtx.agentNamespace = child.Spec.AgentRef.Namespace
		}
	}
	if k8sClient != nil && childCtx.agentName != "" {
		agent := &corev1alpha1.Agent{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: childCtx.agentName, Namespace: childCtx.agentNamespace}, agent); err != nil {
			if !apierrors.IsNotFound(err) {
				return childCtx, fmt.Errorf("resolve child agent %q in namespace %q: %w", childCtx.agentName, childCtx.agentNamespace, err)
			}
		} else {
			childCtx.agent = agent
		}
	}

	providerRef := childTransactionProviderRef(child, childCtx.agent)
	if providerRef != nil && strings.TrimSpace(providerRef.Name) != "" {
		providerNamespace := providerRef.Namespace
		if providerNamespace == "" {
			providerNamespace = child.Namespace
		}
		childCtx.providerInfo = transactionProviderInfo{Name: providerRef.Name, Namespace: providerNamespace}
		if k8sClient != nil {
			provider := &corev1alpha1.Provider{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: providerRef.Name, Namespace: providerNamespace}, provider); err != nil {
				if !apierrors.IsNotFound(err) {
					return childCtx, fmt.Errorf("resolve child provider %q in namespace %q: %w", providerRef.Name, providerNamespace, err)
				}
			} else {
				childCtx.provider = provider
			}
		}
	}
	childCtx.providerInfo, childCtx.model = childTransactionEffectiveProviderModel(child, childCtx.agent, childCtx.provider, childCtx.providerInfo)
	runtimePolicy, err := resolveRuntimeRefPolicy(ctx, k8sClient, child.Namespace, childCtx.agent)
	if err != nil {
		return childCtx, err
	}
	if runtimePolicy != nil {
		if child.Spec.AgentRuntime == nil || !slices.Equal(child.Spec.AgentRuntime.AllowedTools, runtimePolicy.allowedTools) {
			return childCtx, fmt.Errorf("child task allowedTools do not exactly match external AgentRuntime policy")
		}
		childCtx.providerInfo = transactionProviderInfo{Type: runtimePolicy.providerKind}
		childCtx.model = runtimePolicy.model
		childCtx.runtimeProviderKind = runtimePolicy.providerKind
	}
	childCtx.fallbacks, err = childTransactionFallbackProviderModels(ctx, k8sClient, child.Namespace, childCtx.agent)
	if err != nil {
		return childCtx, err
	}
	childCtx.aiTools = childTransactionEffectiveAITools(child, childCtx.agent)
	childCtx.runtimeTools, childCtx.runtimeBash = childTransactionEffectiveRuntimePolicy(child, childCtx.agent)
	if runtimePolicy != nil {
		childCtx.runtimeTools = acp.BuiltInRuntimeEffectiveAllowedTools(
			runtimePolicy.allowedTools, runtimePolicy.disallowedTools, runtimePolicy.allowBash,
		)
		childCtx.runtimeBash = acp.BuiltInRuntimeEffectiveAllowBash(
			runtimePolicy.allowedTools, runtimePolicy.disallowedTools, runtimePolicy.allowBash,
		)
	}
	return childCtx, nil
}

func childTransactionProviderRef(child *corev1alpha1.Task, agent *corev1alpha1.Agent) *corev1alpha1.ProviderReference {
	if childTransactionOpenCodeAgentTask(child, agent) {
		return nil
	}
	if child.Spec.AI != nil && child.Spec.AI.ProviderRef != nil {
		return child.Spec.AI.ProviderRef
	}
	if agent != nil && agent.Spec.ProviderRef != nil {
		return agent.Spec.ProviderRef
	}
	return nil
}

func childTransactionEffectiveProviderModel(child *corev1alpha1.Task, agent *corev1alpha1.Agent, provider *corev1alpha1.Provider, providerInfo transactionProviderInfo) (transactionProviderInfo, string) {
	model := ""
	openCodeAgent := childTransactionOpenCodeAgent(agent)
	openCodeAgentTask := childTransactionOpenCodeAgentTask(child, agent)
	if provider != nil && !openCodeAgent {
		providerInfo = transactionProviderInfo{
			Name:      provider.Name,
			Namespace: provider.Namespace,
			Type:      string(provider.Spec.Type),
		}
		model = provider.Spec.DefaultModel
	}
	if agent != nil && agent.Spec.Model != nil {
		if openCodeAgent {
			providerInfo = transactionProviderInfo{Type: childTransactionOpenCodeModelProvider(agent)}
		} else if strings.TrimSpace(agent.Spec.Model.Provider) != "" {
			providerInfo = transactionProviderInfo{Type: agent.Spec.Model.Provider}
		}
		if strings.TrimSpace(agent.Spec.Model.Name) != "" {
			model = agent.Spec.Model.Name
		}
	}
	if child.Spec.AI != nil && !openCodeAgentTask {
		if strings.TrimSpace(child.Spec.AI.Provider) != "" {
			providerInfo = transactionProviderInfo{Type: child.Spec.AI.Provider}
		}
		if strings.TrimSpace(child.Spec.AI.Model) != "" {
			model = child.Spec.AI.Model
		}
	}
	if provider != nil && !openCodeAgent {
		providerInfo = transactionProviderInfo{
			Name:      provider.Name,
			Namespace: provider.Namespace,
			Type:      string(provider.Spec.Type),
		}
	}
	return providerInfo, model
}

func childTransactionOpenCodeAgentTask(child *corev1alpha1.Task, agent *corev1alpha1.Agent) bool {
	return child != nil && child.Spec.Type == corev1alpha1.TaskTypeAgent && childTransactionOpenCodeAgent(agent)
}

func childTransactionOpenCodeAgent(agent *corev1alpha1.Agent) bool {
	return agent != nil && agent.Spec.Runtime != nil && agent.Spec.Runtime.Type == corev1alpha1.AgentRuntimeOpencode
}

func childTransactionOpenCodeModelProvider(agent *corev1alpha1.Agent) string {
	if !childTransactionOpenCodeAgent(agent) || agent.Spec.Model == nil {
		return ""
	}
	provider, model, ok := strings.Cut(strings.TrimSpace(agent.Spec.Model.Name), "/")
	if !ok || strings.TrimSpace(model) == "" {
		return ""
	}
	return strings.TrimSpace(provider)
}

func childTransactionFallbackProviderModels(ctx context.Context, k8sClient client.Reader, namespace string, agent *corev1alpha1.Agent) ([]transactionProviderModel, error) {
	if k8sClient == nil || agent == nil || agent.Spec.Model == nil || len(agent.Spec.Model.Fallbacks) == 0 {
		return nil, nil
	}
	fallbacks := make([]transactionProviderModel, 0, len(agent.Spec.Model.Fallbacks))
	for _, fb := range agent.Spec.Model.Fallbacks {
		if strings.TrimSpace(fb.ProviderRef) == "" {
			continue
		}
		provider := &corev1alpha1.Provider{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: fb.ProviderRef, Namespace: namespace}, provider); err != nil {
			return nil, fmt.Errorf("resolve child fallback provider %q in namespace %q: %w", fb.ProviderRef, namespace, err)
		}
		model := strings.TrimSpace(fb.Model)
		if model == "" {
			model = provider.Spec.DefaultModel
		}
		fallbacks = append(fallbacks, transactionProviderModel{
			provider: transactionProviderInfo{
				Name:      provider.Name,
				Namespace: provider.Namespace,
				Type:      string(provider.Spec.Type),
			},
			model: model,
		})
	}
	return fallbacks, nil
}

func validateChildProviderModelConstraints(txCtx map[string]string, childCtx childTransactionContext) error {
	if !childHasProviderModelConstraints(txCtx) {
		return nil
	}
	tokenNamespace, hasTokenNamespace := txCtx["namespace"], strings.TrimSpace(txCtx["namespace"]) != ""
	if err := validateChildProviderModel(txCtx, childCtx.providerInfo, childCtx.model, tokenNamespace, hasTokenNamespace, ""); err != nil {
		return err
	}
	for _, fb := range childCtx.fallbacks {
		if err := validateChildProviderModel(txCtx, fb.provider, fb.model, tokenNamespace, hasTokenNamespace, "fallback "); err != nil {
			return err
		}
	}
	return nil
}

func childHasProviderModelConstraints(txCtx map[string]string) bool {
	for _, key := range []string{"provider", "allowedProviders", "model", "allowedModels"} {
		if strings.TrimSpace(txCtx[key]) != "" {
			return true
		}
	}
	return false
}

func validateChildProviderModel(txCtx map[string]string, provider transactionProviderInfo, model, tokenNamespace string, hasTokenNamespace bool, prefix string) error {
	if want := strings.TrimSpace(txCtx["provider"]); want != "" && !transactionProviderMatches(provider, want, tokenNamespace, hasTokenNamespace) {
		return fmt.Errorf("child task %sprovider %q is not allowed by transaction context", prefix, transactionProviderDisplayName(provider))
	}
	if allowed, ok := transactionContextStringList(txCtx["allowedProviders"]); ok && !transactionProviderAllowed(provider, allowed, tokenNamespace, hasTokenNamespace) {
		return fmt.Errorf("child task %sprovider %q is not allowed by transaction context", prefix, transactionProviderDisplayName(provider))
	}
	if want := strings.TrimSpace(txCtx["model"]); want != "" && model != want {
		return fmt.Errorf("child task %smodel %q does not match transaction context %q", prefix, model, want)
	}
	if allowed, ok := transactionContextStringList(txCtx["allowedModels"]); ok && !transactionModelAllowed(provider, model, allowed, tokenNamespace, hasTokenNamespace) {
		return fmt.Errorf("child task %smodel %q is not allowed by transaction context", prefix, model)
	}
	return nil
}

func validateChildToolConstraints(txCtx map[string]string, childCtx childTransactionContext) error {
	allowed, ok := transactionContextStringList(txCtx["allowedTools"])
	if !ok {
		return nil
	}
	if childCtx.agent != nil && childCtx.agent.Spec.Runtime != nil && childCtx.agent.Spec.Runtime.Type == corev1alpha1.AgentRuntimeOpencode {
		allowed = acp.NormalizeOpenCodeAuthorizationTools(allowed)
	}
	if childCtx.childType == corev1alpha1.TaskTypeAgent && childTransactionRuntimeToolsUnrestricted(childCtx.runtimeTools) {
		return fmt.Errorf("child task agent runtime tools are unrestricted by task or agent while transaction context restricts allowedTools")
	}
	runtimeTools := childTransactionRuntimeToolConstraints(childCtx)
	for _, tool := range append(append([]string{}, childCtx.aiTools...), runtimeTools...) {
		tool = strings.TrimSpace(tool)
		if tool == "" {
			continue
		}
		if !slices.Contains(allowed, tool) {
			return fmt.Errorf("child task tool %q is not allowed by transaction context", tool)
		}
	}
	return nil
}

type childTransactionCredentialRequirements struct {
	requiresScope bool
	secretNames   map[string]struct{}
}

func validateChildToolCredentialConstraints(
	ctx context.Context,
	k8sClient client.Reader,
	parent, child *corev1alpha1.Task,
	childCtx childTransactionContext,
) error {
	for _, toolName := range childTransactionCustomToolNames(childCtx) {
		if k8sClient == nil {
			return fmt.Errorf("child task tool %q is unresolved because a Kubernetes client is unavailable", toolName)
		}
		tool := &corev1alpha1.Tool{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: toolName, Namespace: child.Namespace}, tool); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("child task tool %q is unresolved", toolName)
			}
			return fmt.Errorf("resolve child task Tool %q in namespace %q: %w", toolName, child.Namespace, err)
		}
		if tool.Spec.HTTP == nil {
			continue
		}

		requirements := childTransactionCredentialRequirements{secretNames: map[string]struct{}{}}
		if authSecretRef := tool.Spec.HTTP.AuthSecretRef; authSecretRef != nil {
			secretName := strings.TrimSpace(authSecretRef.Name)
			if secretName == "" {
				return fmt.Errorf("child task tool %q has an unresolved HTTP auth Secret reference", toolName)
			}
			requirements.addSecret(secretName)
		}
		if policyRef := tool.Spec.HTTP.OutboundAccessPolicyRef; policyRef != nil {
			policyName := strings.TrimSpace(policyRef.Name)
			if policyName == "" {
				return fmt.Errorf("child task tool %q has an unresolved OutboundAccessPolicy reference", toolName)
			}
			policy := &corev1alpha1.OutboundAccessPolicy{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: policyName, Namespace: child.Namespace}, policy); err != nil {
				if apierrors.IsNotFound(err) {
					return fmt.Errorf("child task tool %q references unresolved OutboundAccessPolicy %q", toolName, policyName)
				}
				return fmt.Errorf("resolve child task Tool %q OutboundAccessPolicy %q: %w", toolName, policyName, err)
			}
			if !childTransactionOutboundAccessPolicyReady(policy) {
				return fmt.Errorf("child task tool %q references not-ready OutboundAccessPolicy %q", toolName, policyName)
			}
			if err := requirements.addOutboundAccessPolicy(policy); err != nil {
				return fmt.Errorf("child task tool %q OutboundAccessPolicy %q credentials: %w", toolName, policyName, err)
			}
		}
		if err := validateChildTransactionCredentialRequirements(parent, fmt.Sprintf("tool %q credential", toolName), requirements); err != nil {
			return err
		}
	}
	return nil
}

func childTransactionCustomToolNames(childCtx childTransactionContext) []string {
	builtInTools := make(map[string]struct{})
	for _, name := range KnownBuiltInToolNames() {
		builtInTools[name] = struct{}{}
	}
	seen := map[string]struct{}{}
	custom := []string{}
	toolNames := append(append([]string{}, childCtx.aiTools...), childTransactionRuntimeToolConstraints(childCtx)...)
	for _, name := range toolNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, builtIn := builtInTools[name]; builtIn || childTransactionProviderNativeToolName(childCtx, name) {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		custom = append(custom, name)
	}
	sort.Strings(custom)
	return custom
}

func childTransactionProviderNativeToolName(childCtx childTransactionContext, name string) bool {
	providerKind := childCtx.runtimeProviderKind
	if providerKind == "" && childCtx.agent != nil && childCtx.agent.Spec.Runtime != nil {
		providerKind = string(childCtx.agent.Spec.Runtime.Type)
	}
	return acp.IsBuiltInRuntimeNativeTool(providerKind, name)
}

func (requirements *childTransactionCredentialRequirements) addSecret(name string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	if requirements.secretNames == nil {
		requirements.secretNames = map[string]struct{}{}
	}
	requirements.secretNames[name] = struct{}{}
	requirements.requiresScope = true
}

func (requirements *childTransactionCredentialRequirements) addOutboundAccessPolicy(policy *corev1alpha1.OutboundAccessPolicy) error {
	if policy == nil {
		return fmt.Errorf("policy is unresolved")
	}
	addSecretRef := func(role string, ref *corev1alpha1.NamespacedSecretKeySelector) error {
		if ref == nil {
			return nil
		}
		name := strings.TrimSpace(ref.Name)
		if name == "" {
			return fmt.Errorf("%s Secret reference is unresolved", role)
		}
		if namespace := strings.TrimSpace(ref.Namespace); namespace != "" && namespace != policy.Namespace {
			return fmt.Errorf("%s Secret namespace %q does not match policy namespace %q", role, namespace, policy.Namespace)
		}
		requirements.addSecret(name)
		return nil
	}
	addTokenSource := func(role string, source *corev1alpha1.OutboundTokenSource) error {
		if source == nil {
			return nil
		}
		if source.Source == corev1alpha1.OutboundTokenSourceServiceAccount {
			requirements.requiresScope = true
		}
		if source.Source == corev1alpha1.OutboundTokenSourceSecretRef && source.SecretRef == nil {
			return fmt.Errorf("%s Secret reference is unresolved", role)
		}
		return addSecretRef(role, source.SecretRef)
	}
	addTLS := func(role string, tlsConfig *corev1alpha1.OutboundTLSConfig) error {
		if tlsConfig == nil {
			return nil
		}
		return addSecretRef(role, tlsConfig.CASecretRef)
	}

	if direct := policy.Spec.Direct; direct != nil {
		if err := addTokenSource("subject", &direct.Subject); err != nil {
			return err
		}
		if err := addTokenSource("actor", direct.Actor); err != nil {
			return err
		}
		if auth := direct.ClientAuthentication; auth != nil {
			if err := addSecretRef("client authentication", auth.ClientSecretRef); err != nil {
				return err
			}
			if err := addSecretRef("private-key authentication", auth.PrivateKeyRef); err != nil {
				return err
			}
		}
		if err := addTLS("token endpoint CA", direct.TokenEndpoint.TLS); err != nil {
			return err
		}
	}
	if gateway := policy.Spec.Gateway; gateway != nil {
		if err := addTLS("gateway CA", gateway.TLS); err != nil {
			return err
		}
	}
	return nil
}

func validateChildTransactionCredentialRequirements(
	parent *corev1alpha1.Task,
	description string,
	requirements childTransactionCredentialRequirements,
) error {
	if !requirements.requiresScope {
		return nil
	}
	if !TransactionHasScope(parent.Spec.Transaction, transactionSecretCredentialReadScope) {
		return fmt.Errorf("child task %s requires transaction scope %q", description, transactionSecretCredentialReadScope)
	}
	want := strings.TrimSpace(parent.Spec.Transaction.Context["secret"])
	if want == "" {
		return nil
	}
	secretNames := make([]string, 0, len(requirements.secretNames))
	for name := range requirements.secretNames {
		secretNames = append(secretNames, name)
	}
	sort.Strings(secretNames)
	for _, name := range secretNames {
		if name != want {
			return fmt.Errorf("child task %s %q does not match transaction context %q", description, name, want)
		}
	}
	return nil
}

func childTransactionOutboundAccessPolicyReady(policy *corev1alpha1.OutboundAccessPolicy) bool {
	if policy == nil || !policy.DeletionTimestamp.IsZero() || policy.Status.ObservedGeneration != policy.Generation {
		return false
	}
	for _, conditionType := range []string{
		corev1alpha1.OutboundAccessPolicyConditionAccepted,
		corev1alpha1.OutboundAccessPolicyConditionResolvedRefs,
	} {
		condition := meta.FindStatusCondition(policy.Status.Conditions, conditionType)
		if condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != policy.Generation {
			return false
		}
	}
	return true
}

func childTransactionRuntimeToolConstraints(childCtx childTransactionContext) []string {
	runtimeTools := append([]string{}, childCtx.runtimeTools...)
	if childTransactionRuntimeRefAgent(childCtx.agent) ||
		(childCtx.agent != nil && childCtx.agent.Spec.Runtime != nil && childCtx.agent.Spec.Runtime.Type == corev1alpha1.AgentRuntimeOpencode) {
		return runtimeTools
	}
	if childCtx.childType == corev1alpha1.TaskTypeAgent && childCtx.runtimeBash {
		runtimeTools = append(runtimeTools, "Bash")
	}
	return runtimeTools
}

func hasNonEmptyTransactionTools(tools []string) bool {
	return slices.ContainsFunc(tools, func(tool string) bool {
		return strings.TrimSpace(tool) != ""
	})
}

func childTransactionRuntimeToolsUnrestricted(tools []string) bool {
	return tools == nil || (len(tools) > 0 && !hasNonEmptyTransactionTools(tools))
}

func childTransactionEffectiveAITools(child *corev1alpha1.Task, agent *corev1alpha1.Agent) []string {
	tools := []string{}
	if agent != nil {
		for _, tool := range agent.Spec.Tools {
			if tool.Enabled != nil && !*tool.Enabled {
				continue
			}
			if strings.TrimSpace(tool.Name) != "" {
				tools = append(tools, tool.Name)
			}
		}
		if !childTransactionRuntimeRefAgent(agent) && agent.Spec.Coordination != nil && agent.Spec.Coordination.Enabled &&
			child.Annotations[labels.AnnotationDisableCoordinationToolInject] != trueStr {
			for _, tool := range transactionCoordinationToolNames() {
				if !slices.Contains(tools, tool) {
					tools = append(tools, tool)
				}
			}
		}
	}
	if child.Spec.AI != nil {
		for _, tool := range child.Spec.AI.Tools {
			if strings.TrimSpace(tool) != "" {
				tools = append(tools, tool)
			}
		}
	}
	if child.Spec.Type == corev1alpha1.TaskTypeAI {
		for _, tool := range transactionMemoryToolNames() {
			if !slices.Contains(tools, tool) {
				tools = append(tools, tool)
			}
		}
	}
	messagingCapable := child.Spec.Type == corev1alpha1.TaskTypeAI || child.Spec.Type == corev1alpha1.TaskTypeAgent
	if _, delegatedChild := child.Labels[labels.LabelParentTask]; messagingCapable && delegatedChild &&
		!childTransactionRuntimeRefAgent(agent) && child.Annotations[labels.AnnotationDisableCoordinationToolInject] != trueStr {
		for _, tool := range []string{sendMessageToolName, checkMessagesToolName} {
			if !slices.Contains(tools, tool) {
				tools = append(tools, tool)
			}
		}
	}
	return tools
}

func childTransactionRuntimeRefAgent(agent *corev1alpha1.Agent) bool {
	return agent != nil && agent.Spec.Runtime != nil && agent.Spec.Runtime.RuntimeRef != nil &&
		strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) != ""
}

func transactionMemoryToolNames() []string {
	return []string{
		"recall_memory",
		"remember",
		"propose_memory",
		"search_transcript",
	}
}

func childTransactionAgentRuntimeAllowedTools(agent *corev1alpha1.Agent) []string {
	if agent == nil || agent.Spec.Runtime == nil {
		return nil
	}
	runtime := agent.Spec.Runtime
	if runtime.Type == corev1alpha1.AgentRuntimeOpencode && runtime.DefaultAllowedTools == nil {
		return acp.OpenCodeDefaultAllowedTools()
	}
	if runtime.DefaultAllowedTools != nil {
		return append([]string{}, runtime.DefaultAllowedTools...)
	}
	return nil
}

func childTransactionAgentRuntimeAllowBash(agent *corev1alpha1.Agent) bool {
	allowBash := true
	if agent != nil && agent.Spec.Runtime != nil && agent.Spec.Runtime.DefaultAllowBash != nil {
		allowBash = *agent.Spec.Runtime.DefaultAllowBash
	}
	return allowBash
}

func childTransactionEffectiveRuntimePolicy(child *corev1alpha1.Task, agent *corev1alpha1.Agent) ([]string, bool) {
	allowedTools := childTransactionAgentRuntimeAllowedTools(agent)
	if child.Spec.AgentRuntime != nil && child.Spec.AgentRuntime.AllowedTools != nil {
		allowedTools = append([]string{}, child.Spec.AgentRuntime.AllowedTools...)
	}
	allowBash := childTransactionAgentRuntimeAllowBash(agent)
	disallowedTools := []string(nil)
	if child.Spec.AgentRuntime != nil && child.Spec.AgentRuntime.AllowBash != nil {
		allowBash = *child.Spec.AgentRuntime.AllowBash
	}
	if child.Spec.AgentRuntime != nil {
		disallowedTools = append(disallowedTools, child.Spec.AgentRuntime.DisallowedTools...)
	}
	if agent == nil || agent.Spec.Runtime == nil {
		return allowedTools, allowBash
	}
	if agent.Spec.Runtime.Type != corev1alpha1.AgentRuntimeOpencode {
		switch agent.Spec.Runtime.Type {
		case corev1alpha1.AgentRuntimeClaude, corev1alpha1.AgentRuntimeCodex, corev1alpha1.AgentRuntimeCopilot:
			// Preserve a non-empty all-blank allowlist as the existing fail-closed
			// unrestricted sentinel instead of collapsing it into explicit deny-all.
			if len(allowedTools) > 0 && !hasNonEmptyTransactionTools(allowedTools) {
				return allowedTools, allowBash
			}
			allowedTools, disallowedTools, allowBash = acp.NormalizeBuiltInRuntimeToolPolicy(
				string(agent.Spec.Runtime.Type), allowedTools, disallowedTools, allowBash,
			)
			allowedTools = acp.BuiltInRuntimeEffectiveAllowedTools(allowedTools, disallowedTools, allowBash)
			allowBash = acp.BuiltInRuntimeEffectiveAllowBash(allowedTools, disallowedTools, allowBash)
		}
		return allowedTools, allowBash
	}
	workspace := child.Spec.Workspace
	readIntent := workspace == nil || workspace.Intent == "" || workspace.Intent == corev1alpha1.WorkspaceIntentRead
	allowedTools, disallowedTools, allowBash = acp.NormalizeOpenCodeToolPolicy(readIntent, allowedTools, disallowedTools, allowBash)
	allowedTools = acp.OpenCodeEffectiveAllowedTools(allowedTools, disallowedTools, allowBash)
	return allowedTools, allowBash && slices.Contains(allowedTools, "bash")
}

func transactionCoordinationToolNames() []string {
	return []string{
		"delegate_task",
		"wait_for_tasks",
		"create_container_task",
		"cancel_task",
		"send_message",
		"check_messages",
		"recall_memory",
		"remember",
		"propose_memory",
		"search_transcript",
		"create_pull_request",
		"list_pull_requests",
		"check_pr_review_marker",
		"check_pull_request_ci",
		"merge_pull_request",
		"auto_merge_pull_request",
		"review_pull_request",
		"post_review_comment",
		"create_agent",
		"delete_agent",
		"update_plan",
	}
}

func transactionContextStringList(value string) ([]string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, false
	}
	var decoded []string
	if err := json.Unmarshal([]byte(value), &decoded); err == nil {
		return decoded, true
	}
	return splitCSV(value), true
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func transactionAgentAllowed(name, namespace string, allowed []string) bool {
	return slices.ContainsFunc(allowed, func(want string) bool {
		return transactionAgentMatches(name, namespace, want)
	})
}

func transactionAgentMatches(name, namespace, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" || strings.TrimSpace(name) == "" {
		return false
	}
	return name == want || namespacedToolName(namespace, name) == want
}

func namespacedToolName(namespace, name string) string {
	if namespace == "" || name == "" {
		return name
	}
	return namespace + "/" + name
}

func transactionProviderAllowed(provider transactionProviderInfo, allowed []string, tokenNamespace string, hasTokenNamespace bool) bool {
	return slices.ContainsFunc(allowed, func(want string) bool {
		return transactionProviderMatches(provider, want, tokenNamespace, hasTokenNamespace)
	})
}

func transactionProviderMatches(provider transactionProviderInfo, want string, tokenNamespace string, hasTokenNamespace bool) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	if !transactionProviderNamespaceMatchesContext(provider, tokenNamespace, hasTokenNamespace) {
		return false
	}
	if provider.Name != "" && namespacedToolName(provider.Namespace, provider.Name) == want {
		return true
	}
	if provider.Name != "" && provider.Name == want {
		return true
	}
	return provider.Type != "" && provider.Type == want
}

func transactionModelAllowed(provider transactionProviderInfo, model string, allowed []string, tokenNamespace string, hasTokenNamespace bool) bool {
	if !transactionProviderNamespaceMatchesContext(provider, tokenNamespace, hasTokenNamespace) {
		return false
	}
	for _, want := range allowed {
		want = strings.TrimSpace(want)
		switch want {
		case "":
			continue
		case model:
			return true
		}
		if provider.Name != "" && want == provider.Name+"/"+model {
			return true
		}
		if provider.Name != "" && want == namespacedToolName(provider.Namespace, provider.Name)+"/"+model {
			return true
		}
		if provider.Type != "" && want == provider.Type+"/"+model {
			return true
		}
	}
	return false
}

func transactionProviderNamespaceMatchesContext(provider transactionProviderInfo, tokenNamespace string, hasTokenNamespace bool) bool {
	if !hasTokenNamespace {
		return true
	}
	providerNamespace := strings.TrimSpace(provider.Namespace)
	return providerNamespace == "" || providerNamespace == tokenNamespace
}

func transactionProviderDisplayName(provider transactionProviderInfo) string {
	if provider.Name != "" {
		return namespacedToolName(provider.Namespace, provider.Name)
	}
	return provider.Type
}

func workspaceGitRepo(workspace *corev1alpha1.WorkspaceConfig) string {
	if workspace == nil {
		return ""
	}
	return workspace.GitRepo
}

// validateChildDependencyNamespaces binds a transaction-context namespace
// constraint to every resolved dependency, not only the child Task,
// mirroring the direct API's dependency-namespace rule: cross-namespace
// Agent or provider authority must not be exercised under a
// namespace-scoped delegated token.
func validateChildDependencyNamespaces(txCtx map[string]string, child *corev1alpha1.Task, agentNamespace string, childCtx childTransactionContext) error {
	want := strings.TrimSpace(txCtx["namespace"])
	if want == "" {
		return nil
	}
	if child.Namespace != want {
		return fmt.Errorf("child task namespace %q does not match transaction context %q", child.Namespace, want)
	}
	if agentNamespace != "" && agentNamespace != want {
		return fmt.Errorf("child task agent namespace %q does not match transaction context %q", agentNamespace, want)
	}
	if providerNamespace := strings.TrimSpace(childCtx.providerInfo.Namespace); providerNamespace != "" && providerNamespace != want {
		return fmt.Errorf("child task provider namespace %q does not match transaction context %q", providerNamespace, want)
	}
	return nil
}

// validateChildWorkspaceSelectorConstraints enforces the transaction-context
// repo/branch/ref constraints on the child workspace and rejects a ref that
// would override a branch-only constraint, since execution gives
// workspace.ref precedence over branch.
func validateChildWorkspaceSelectorConstraints(txCtx map[string]string, workspace *corev1alpha1.WorkspaceConfig) error {
	for _, constraint := range []struct {
		key string
		got string
	}{
		{key: "repo", got: workspaceGitRepo(workspace)},
		{key: "branch", got: workspaceBranch(workspace)},
		{key: "ref", got: workspaceRef(workspace)},
	} {
		if want := strings.TrimSpace(txCtx[constraint.key]); want != "" && constraint.got != want {
			return fmt.Errorf("child task workspace %s %q does not match transaction context %q", constraint.key, constraint.got, want)
		}
	}
	if strings.TrimSpace(txCtx["branch"]) != "" && strings.TrimSpace(txCtx["ref"]) == "" && workspaceRef(workspace) != "" {
		return fmt.Errorf("child task workspace ref %q overrides the branch constrained by transaction context", workspaceRef(workspace))
	}
	return nil
}

func workspaceBranch(workspace *corev1alpha1.WorkspaceConfig) string {
	if workspace == nil {
		return ""
	}
	return workspace.Branch
}

func workspaceRef(workspace *corev1alpha1.WorkspaceConfig) string {
	if workspace == nil {
		return ""
	}
	return workspace.Ref
}
