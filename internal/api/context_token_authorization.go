/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/gofiber/fiber/v3"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/acp"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/llm"
	"github.com/orka-agents/orka/internal/metrics"
	"github.com/orka-agents/orka/internal/redact"
	toolspkg "github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/internal/tracing"
	"github.com/orka-agents/orka/internal/workerenv"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultTaskUpdateScope = "orka:tasks:update"

	// ContextTokenAuthorizationModeOff disables context-token authorization checks.
	ContextTokenAuthorizationModeOff = "off"
	// ContextTokenAuthorizationModeAudit logs context-token authorization failures but allows the request.
	ContextTokenAuthorizationModeAudit = "audit"
	// ContextTokenAuthorizationModeEnforce rejects requests that fail context-token authorization.
	ContextTokenAuthorizationModeEnforce = "enforce"

	// ContextTokenScopeTaskCreate authorizes context-token callers to create Orka Tasks.
	ContextTokenScopeTaskCreate = "orka:tasks:create"
	// ContextTokenScopeTaskGet authorizes context-token callers to read a Task and its related data.
	ContextTokenScopeTaskGet = "orka:tasks:get"
	// ContextTokenScopeTaskList authorizes context-token callers to list Tasks.
	ContextTokenScopeTaskList = "orka:tasks:list"
	// ContextTokenScopeTaskDelete authorizes context-token callers to delete Tasks.
	ContextTokenScopeTaskDelete = "orka:tasks:delete"
	// ContextTokenScopeToolsRead authorizes context-token callers to read Tool definitions.
	ContextTokenScopeToolsRead = "orka:tools:read"
	// ContextTokenScopeToolsUse authorizes context-token callers to execute Orka-managed tools.
	ContextTokenScopeToolsUse = "orka:tools:use"
	// ContextTokenScopeProvidersUse authorizes context-token callers to use configured model providers.
	ContextTokenScopeProvidersUse = "orka:providers:use"
	// ContextTokenScopeSecretsRead authorizes context-token callers to read Secret metadata.
	ContextTokenScopeSecretsRead = "orka:secrets:read"
	// ContextTokenScopeSecretsCredentialsRead authorizes use of cluster-managed outbound credential material, including Secret data and ServiceAccount tokens.
	ContextTokenScopeSecretsCredentialsRead = "orka:secrets:credentials:read"
	// ContextTokenScopeAgentsRead authorizes context-token callers to read Agent definitions.
	ContextTokenScopeAgentsRead = "orka:agents:read"
	// ContextTokenScopeAgentsWrite authorizes context-token callers to mutate Agent definitions.
	ContextTokenScopeAgentsWrite = "orka:agents:write"
	// ContextTokenScopeMemoryRead authorizes context-token callers to read memory resources.
	ContextTokenScopeMemoryRead = "orka:memory:read"
	// ContextTokenScopeMemoryWrite authorizes context-token callers to mutate memory resources.
	ContextTokenScopeMemoryWrite = "orka:memory:write"
	// ContextTokenScopeSessionsRead authorizes context-token callers to read sessions.
	ContextTokenScopeSessionsRead = "orka:sessions:read"
	// ContextTokenScopeSessionsWrite authorizes context-token callers to delete or mutate sessions.
	ContextTokenScopeSessionsWrite = "orka:sessions:write"
	// ContextTokenScopeSecurityRead authorizes context-token callers to read security scan resources.
	ContextTokenScopeSecurityRead = "orka:security:read"
	// ContextTokenScopeSecurityWrite authorizes context-token callers to mutate security scan resources.
	ContextTokenScopeSecurityWrite = "orka:security:write"
	// ContextTokenScopeMonitorsRead authorizes context-token callers to read repository monitor resources.
	ContextTokenScopeMonitorsRead = "orka:monitors:read"
	// ContextTokenScopeMonitorsWrite authorizes context-token callers to mutate repository monitor resources.
	ContextTokenScopeMonitorsWrite = "orka:monitors:write"
	// ContextTokenScopeMonitorsOperate authorizes context-token callers to enqueue repository monitor operations.
	ContextTokenScopeMonitorsOperate = "orka:monitors:operate"
	// ContextTokenScopeSkillsRead authorizes context-token callers to read Skills.
	ContextTokenScopeSkillsRead = "orka:skills:read"
	// ContextTokenScopeSkillsWrite authorizes context-token callers to mutate Skills.
	ContextTokenScopeSkillsWrite = "orka:skills:write"
)

// ContextTokenAuthorizationConfig controls optional authorization checks derived
// from verified context-token scope and transaction context claims.
type ContextTokenAuthorizationConfig struct {
	Mode                          string
	TaskCreateScopes              []string
	TaskReadScopes                []string
	TaskListScopes                []string
	TaskDeleteScopes              []string
	TaskUpdateScopes              []string
	ToolReadScopes                []string
	ToolUseScopes                 []string
	ProviderUseScopes             []string
	SecretReadScopeList           []string
	SecretCredentialReadScopeList []string
	AgentReadScopes               []string
	AgentWriteScopes              []string
	MemoryReadScopes              []string
	MemoryWriteScopes             []string
	SessionReadScopes             []string
	SessionWriteScopes            []string
	SecurityReadScopes            []string
	SecurityWriteScopes           []string
	MonitorReadScopes             []string
	MonitorWriteScopes            []string
	MonitorOperateScopes          []string
	SkillReadScopes               []string
	SkillWriteScopes              []string
	GatewayReadScopes             []string
	GatewayOperateScopes          []string
	ConfigMapReadScopeList        []string
}

// ContextTokenAuthorizationConfigOptions names the inputs used to build
// context-token authorization config.
type ContextTokenAuthorizationConfigOptions struct {
	Mode                       string
	TaskCreateScopes           string
	TaskReadScopes             string
	TaskListScopes             string
	TaskDeleteScopes           string
	TaskUpdateScopes           string
	ToolReadScopes             string
	ToolUseScopes              string
	ProviderUseScopes          string
	SecretReadScopes           string
	SecretCredentialReadScopes string
	AgentReadScopes            string
	AgentWriteScopes           string
	MemoryReadScopes           string
	MemoryWriteScopes          string
	SessionReadScopes          string
	SessionWriteScopes         string
	SecurityReadScopes         string
	SecurityWriteScopes        string
	MonitorReadScopes          string
	MonitorWriteScopes         string
	MonitorOperateScopes       string
	SkillReadScopes            string
	SkillWriteScopes           string
	GatewayReadScopes          string
	GatewayOperateScopes       string
	ConfigMapReadScopes        string
}

// NewContextTokenAuthorizationConfig builds context-token authorization config.
func NewContextTokenAuthorizationConfig(opts ContextTokenAuthorizationConfigOptions) (ContextTokenAuthorizationConfig, error) {
	mode := strings.ToLower(strings.TrimSpace(opts.Mode))
	if mode == "" {
		mode = ContextTokenAuthorizationModeOff
	}
	switch mode {
	case ContextTokenAuthorizationModeOff, ContextTokenAuthorizationModeAudit, ContextTokenAuthorizationModeEnforce:
	default:
		return ContextTokenAuthorizationConfig{}, fmt.Errorf("unsupported context-token authorization mode %q", mode)
	}

	createScopes := defaultScopes(opts.TaskCreateScopes, ContextTokenScopeTaskCreate)
	readScopes := defaultScopes(opts.TaskReadScopes, ContextTokenScopeTaskGet)
	listScopes := defaultScopes(opts.TaskListScopes, ContextTokenScopeTaskList)
	deleteScopes := defaultScopes(opts.TaskDeleteScopes, ContextTokenScopeTaskDelete)
	updateScopes := defaultScopes(opts.TaskUpdateScopes, defaultTaskUpdateScope)
	toolRead := defaultScopes(opts.ToolReadScopes, ContextTokenScopeToolsRead)
	toolUse := defaultScopes(opts.ToolUseScopes, ContextTokenScopeToolsUse)
	providerUse := defaultScopes(opts.ProviderUseScopes, ContextTokenScopeProvidersUse)
	secretRead := defaultScopes(opts.SecretReadScopes, ContextTokenScopeSecretsRead)
	secretCredentialRead := defaultScopes(opts.SecretCredentialReadScopes, ContextTokenScopeSecretsCredentialsRead)
	agentRead := defaultScopes(opts.AgentReadScopes, ContextTokenScopeAgentsRead)
	agentWrite := defaultScopes(opts.AgentWriteScopes, ContextTokenScopeAgentsWrite)
	memoryRead := defaultScopes(opts.MemoryReadScopes, ContextTokenScopeMemoryRead)
	memoryWrite := defaultScopes(opts.MemoryWriteScopes, ContextTokenScopeMemoryWrite)
	sessionRead := defaultScopes(opts.SessionReadScopes, ContextTokenScopeSessionsRead)
	sessionWrite := defaultScopes(opts.SessionWriteScopes, ContextTokenScopeSessionsWrite)
	securityRead := defaultScopes(opts.SecurityReadScopes, ContextTokenScopeSecurityRead)
	securityWrite := defaultScopes(opts.SecurityWriteScopes, ContextTokenScopeSecurityWrite)
	monitorRead := defaultScopes(opts.MonitorReadScopes, ContextTokenScopeMonitorsRead)
	monitorWrite := defaultScopes(opts.MonitorWriteScopes, ContextTokenScopeMonitorsWrite)
	monitorOperate := defaultScopes(opts.MonitorOperateScopes, ContextTokenScopeMonitorsOperate)
	skillRead := defaultScopes(opts.SkillReadScopes, ContextTokenScopeSkillsRead)
	skillWrite := defaultScopes(opts.SkillWriteScopes, ContextTokenScopeSkillsWrite)
	gatewayRead := defaultScopes(opts.GatewayReadScopes, ContextScopeGatewaysRead)
	gatewayOperate := defaultScopes(opts.GatewayOperateScopes, ContextScopeGatewaysOperate)
	configMapRead := defaultScopes(opts.ConfigMapReadScopes, ContextTokenScopeConfigMapsRead)
	return ContextTokenAuthorizationConfig{
		Mode:                          mode,
		TaskCreateScopes:              createScopes,
		TaskReadScopes:                readScopes,
		TaskListScopes:                listScopes,
		TaskDeleteScopes:              deleteScopes,
		TaskUpdateScopes:              updateScopes,
		ToolReadScopes:                toolRead,
		ToolUseScopes:                 toolUse,
		ProviderUseScopes:             providerUse,
		SecretReadScopeList:           secretRead,
		SecretCredentialReadScopeList: secretCredentialRead,
		AgentReadScopes:               agentRead,
		AgentWriteScopes:              agentWrite,
		MemoryReadScopes:              memoryRead,
		MemoryWriteScopes:             memoryWrite,
		SessionReadScopes:             sessionRead,
		SessionWriteScopes:            sessionWrite,
		SecurityReadScopes:            securityRead,
		SecurityWriteScopes:           securityWrite,
		MonitorReadScopes:             monitorRead,
		MonitorWriteScopes:            monitorWrite,
		MonitorOperateScopes:          monitorOperate,
		SkillReadScopes:               skillRead,
		SkillWriteScopes:              skillWrite,
		GatewayReadScopes:             gatewayRead,
		GatewayOperateScopes:          gatewayOperate,
		ConfigMapReadScopeList:        configMapRead,
	}, nil
}

// Enabled reports whether context-token authorization is configured.
func (c ContextTokenAuthorizationConfig) Enabled() bool {
	return c.Mode == ContextTokenAuthorizationModeAudit || c.Mode == ContextTokenAuthorizationModeEnforce
}

func (c ContextTokenAuthorizationConfig) enforcing() bool {
	return c.Mode == ContextTokenAuthorizationModeEnforce
}

func (c ContextTokenAuthorizationConfig) SecretReadScopes() []string {
	return c.SecretReadScopeList
}

func (c ContextTokenAuthorizationConfig) SecretCredentialReadScopes() []string {
	return c.SecretCredentialReadScopeList
}

func (c ContextTokenAuthorizationConfig) ConfigMapReadScopes() []string {
	return c.ConfigMapReadScopeList
}

type contextTokenTaskCreateAuthorizationContext struct {
	Request             CreateTaskRequest
	Namespace           string
	PolicyFailures      []string
	Agent               *corev1alpha1.Agent
	AgentName           string
	AgentNamespace      string
	Provider            *corev1alpha1.Provider
	ProviderRef         ProviderResolutionInfo
	EffectiveProvider   ProviderResolutionInfo
	EffectiveModel      string
	Fallbacks           []contextTokenProviderModel
	EffectiveAITools    []string
	RuntimeAllowedTools []string
	RuntimeAllowBash    bool
	RuntimeProviderKind string
}

type contextTokenAgentSpecAuthorizationContext struct {
	Agent               *corev1alpha1.Agent
	EffectiveProvider   ProviderResolutionInfo
	EffectiveModel      string
	Fallbacks           []contextTokenProviderModel
	EffectiveAITools    []string
	RuntimeAllowedTools []string
	RuntimeAllowBash    bool
}

type contextTokenProviderModel struct {
	Provider ProviderResolutionInfo
	Model    string
}

func defaultScopes(value, fallback string) []string {
	if scopes := workerenv.SplitCSV(value); len(scopes) > 0 {
		return scopes
	}
	return []string{fallback}
}

func (h *Handlers) authorizeContextTokenTaskCreate(c fiber.Ctx, req CreateTaskRequest, namespace string) error {
	if !h.contextTokenAuthorization.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}

	reader := h.contextTokenAuthorizationReader()
	authzCtx, err := resolveContextTokenTaskCreateAuthorizationContext(c.Context(), reader, req, namespace)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}

	failures := contextTokenTaskCreateFailures(ui.ContextToken, h.contextTokenAuthorization, authzCtx)
	credentialFailures, err := contextTokenTaskToolCredentialFailures(c.Context(), reader, ui.ContextToken, h.contextTokenAuthorization, authzCtx)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	failures = append(failures, credentialFailures...)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization("createTask", "allowed", "ok")
		return nil
	}

	return h.handleContextTokenAuthorizationFailures(ui.ContextToken, "createTask", failures)
}

func authorizeContextTokenTaskCreateObject(ctx context.Context, reader client.Reader, token *ContextToken, cfg ContextTokenAuthorizationConfig, action string, task *corev1alpha1.Task) error {
	if !cfg.Enabled() || token == nil || task == nil {
		return nil
	}

	req := createTaskRequestFromTask(task)
	namespace := task.Namespace
	if namespace == "" {
		namespace = req.Namespace
	}

	authzCtx, err := resolveContextTokenTaskCreateAuthorizationContext(ctx, reader, req, namespace)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}

	failures := contextTokenTaskCreateFailures(token, cfg, authzCtx)
	credentialFailures, err := contextTokenTaskToolCredentialFailures(ctx, reader, token, cfg, authzCtx)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	failures = append(failures, credentialFailures...)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}

	return handleContextTokenAuthorizationFailures(cfg, token, action, failures)
}

func authorizeAndStampToolTaskCreate(ctx context.Context, reader client.Reader, kubeClient kubernetes.Interface, token *ContextToken, cfg ContextTokenAuthorizationConfig, action string, ui *UserInfo, task *corev1alpha1.Task) error {
	if err := authorizeContextTokenTaskCreateObject(ctx, reader, token, cfg, action, task); err != nil {
		return err
	}
	if err := authorizeKubernetesTaskCreate(ctx, kubeClient, ui, task); err != nil {
		return err
	}
	stampTaskRequesterFromUserInfo(task, ui)
	tracing.StampTaskTraceContext(ctx, task)
	return nil
}

func authorizeAndStampTaskContext(ctx context.Context, reader client.Reader, kubeClient kubernetes.Interface, token *ContextToken, cfg ContextTokenAuthorizationConfig, action string, ui *UserInfo, task *corev1alpha1.Task) error {
	if err := authorizeContextTokenTaskContextObject(ctx, reader, token, cfg, action, task); err != nil {
		return err
	}
	if err := authorizeKubernetesTaskCreate(ctx, kubeClient, ui, task); err != nil {
		return err
	}
	stampTaskRequesterFromUserInfo(task, ui)
	tracing.StampTaskTraceContext(ctx, task)
	return nil
}

func authorizeContextTokenToolAgentCreate(ctx context.Context, reader client.Reader, token *ContextToken, cfg ContextTokenAuthorizationConfig, action string, agent *corev1alpha1.Agent) error {
	if !cfg.Enabled() || token == nil || agent == nil {
		return nil
	}
	failures := []string{}
	if !hasAnyScope(token.Scopes, cfg.AgentWriteScopes) {
		failures = append(failures, fmt.Sprintf("missing one of required scopes %q", strings.Join(cfg.AgentWriteScopes, ",")))
	}
	failures = append(failures, contextTokenAgentMutationFailures(token, agent.Namespace, agent.Name)...)
	specFailures, err := contextTokenAgentSpecFailures(ctx, reader, token, agent)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	failures = append(failures, specFailures...)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, token, action, failures)
}

func authorizeContextTokenAgentSpec(ctx context.Context, reader client.Reader, token *ContextToken, cfg ContextTokenAuthorizationConfig, action string, agent *corev1alpha1.Agent) error {
	if !cfg.Enabled() || token == nil || agent == nil {
		return nil
	}
	failures, err := contextTokenAgentSpecFailures(ctx, reader, token, agent)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, token, action, failures)
}

func authorizeContextTokenTaskContextObject(ctx context.Context, reader client.Reader, token *ContextToken, cfg ContextTokenAuthorizationConfig, action string, task *corev1alpha1.Task) error {
	if !cfg.Enabled() || token == nil || task == nil {
		return nil
	}
	req := createTaskRequestFromTask(task)
	namespace := task.Namespace
	if namespace == "" {
		namespace = req.Namespace
	}
	authzCtx, err := resolveContextTokenTaskCreateAuthorizationContext(ctx, reader, req, namespace)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	failures := contextTokenTaskContextFailures(token, authzCtx, true)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, token, action, failures)
}

func authorizeContextTokenTaskDeleteObject(ctx context.Context, reader client.Reader, token *ContextToken, cfg ContextTokenAuthorizationConfig, action string, task *corev1alpha1.Task) error {
	if !cfg.Enabled() || token == nil || task == nil {
		return nil
	}
	failures := []string{}
	if !hasAnyScope(token.Scopes, cfg.TaskDeleteScopes) {
		failures = append(failures, fmt.Sprintf("missing one of required scopes %q", strings.Join(cfg.TaskDeleteScopes, ",")))
	}
	contextFailures, err := contextTokenLoadedTaskContextFailures(ctx, reader, token, task, true)
	if err != nil {
		return err
	}
	failures = append(failures, contextFailures...)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, token, action, failures)
}

func authorizeContextTokenToolAgentUpdate(ctx context.Context, reader client.Reader, token *ContextToken, cfg ContextTokenAuthorizationConfig, action string, agent *corev1alpha1.Agent) error {
	if !cfg.Enabled() || token == nil || agent == nil {
		return nil
	}
	failures := contextTokenAgentWriteFailures(token, cfg, agent.Namespace, agent.Name)
	specFailures, err := contextTokenAgentSpecFailures(ctx, reader, token, agent)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	failures = append(failures, specFailures...)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, token, action, failures)
}

func authorizeContextTokenToolAgentDelete(token *ContextToken, cfg ContextTokenAuthorizationConfig, action string, agent *corev1alpha1.Agent) error {
	if !cfg.Enabled() || token == nil || agent == nil {
		return nil
	}
	failures := contextTokenAgentWriteFailures(token, cfg, agent.Namespace, agent.Name)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, token, action, failures)
}

func authorizeContextTokenConfigMapRead(token *ContextToken, cfg ContextTokenAuthorizationConfig, action, namespace, configMapName string) error {
	if !cfg.Enabled() || token == nil {
		return nil
	}
	failures := []string{}
	requiredScopes := cfg.ConfigMapReadScopes()
	if !hasAnyScope(token.Scopes, requiredScopes) {
		failures = append(failures, fmt.Sprintf("missing one of required scopes %q", strings.Join(requiredScopes, ",")))
	}
	if want, ok := contextString(token.TransactionContext, "namespace"); ok && strings.TrimSpace(namespace) != "" && namespace != want {
		failures = append(failures, fmt.Sprintf("namespace %q does not match token context %q", namespace, want))
	}
	if want, ok := contextString(token.TransactionContext, "configMap"); ok && strings.TrimSpace(configMapName) != "" && configMapName != want {
		failures = append(failures, fmt.Sprintf("configMap %q does not match token context %q", configMapName, want))
	}
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, token, action, failures)
}

func (h *Handlers) authorizeContextTokenPolicyConfigMapName(c fiber.Ctx, action, namespace, configMapName string) error {
	configMapName = strings.TrimSpace(configMapName)
	if configMapName == "" || !h.contextTokenAuthorization.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}
	return authorizeContextTokenConfigMapRead(ui.ContextToken, h.contextTokenAuthorization, action, namespace, configMapName)
}

func authorizeContextTokenPolicyConfigMapForUser(ui *UserInfo, cfg ContextTokenAuthorizationConfig, action, namespace, configMapName string) error {
	configMapName = strings.TrimSpace(configMapName)
	if configMapName == "" || !cfg.Enabled() {
		return nil
	}
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}
	return authorizeContextTokenConfigMapRead(ui.ContextToken, cfg, action, namespace, configMapName)
}

func authorizeContextTokenSecretRead(token *ContextToken, cfg ContextTokenAuthorizationConfig, action, namespace, secretName string) error {
	if !cfg.Enabled() || token == nil {
		return nil
	}
	failures := []string{}
	requiredScopes := cfg.SecretCredentialReadScopes()
	if !hasAnyScope(token.Scopes, requiredScopes) {
		failures = append(failures, fmt.Sprintf("missing one of required scopes %q", strings.Join(requiredScopes, ",")))
	}
	if want, ok := contextString(token.TransactionContext, "namespace"); ok && strings.TrimSpace(namespace) != "" && namespace != want {
		failures = append(failures, fmt.Sprintf("namespace %q does not match token context %q", namespace, want))
	}
	if want, ok := contextString(token.TransactionContext, "secret"); ok && strings.TrimSpace(secretName) != "" && secretName != want {
		failures = append(failures, fmt.Sprintf("secret %q does not match token context %q", secretName, want))
	}
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, token, action, failures)
}

func (h *Handlers) authorizeContextTokenGitCredentialSecretName(c fiber.Ctx, action, namespace, secretName string) error {
	secretName = strings.TrimSpace(secretName)
	if secretName == "" || !h.contextTokenAuthorization.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}
	return authorizeContextTokenSecretRead(ui.ContextToken, h.contextTokenAuthorization, action, namespace, secretName)
}

func authorizeContextTokenGitCredentialSecretForUser(ui *UserInfo, cfg ContextTokenAuthorizationConfig, action, namespace, secretName string) error {
	secretName = strings.TrimSpace(secretName)
	if secretName == "" || !cfg.Enabled() {
		return nil
	}
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}
	return authorizeContextTokenSecretRead(ui.ContextToken, cfg, action, namespace, secretName)
}

func contextTokenAgentWriteFailures(token *ContextToken, cfg ContextTokenAuthorizationConfig, namespace, agentName string) []string {
	failures := []string{}
	if !hasAnyScope(token.Scopes, cfg.AgentWriteScopes) {
		failures = append(failures, fmt.Sprintf("missing one of required scopes %q", strings.Join(cfg.AgentWriteScopes, ",")))
	}
	failures = append(failures, contextTokenAgentMutationFailures(token, namespace, agentName)...)
	return failures
}

func contextTokenFromUserInfo(ui *UserInfo) *ContextToken {
	if ui == nil || ui.AuthType != AuthTypeContextToken {
		return nil
	}
	return ui.ContextToken
}

func (h *Handlers) authorizeContextTokenLoadedTask(c fiber.Ctx, action string, task *corev1alpha1.Task) error {
	return h.authorizeContextTokenLoadedTaskWithIdentity(c, action, task, true)
}

func (h *Handlers) authorizeContextTokenLoadedTaskWithIdentity(c fiber.Ctx, action string, task *corev1alpha1.Task, includeTaskIdentity bool) error {
	if !h.contextTokenAuthorization.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}

	failures, err := h.contextTokenLoadedTaskContextFailures(c.Context(), ui.ContextToken, task, includeTaskIdentity)
	if err != nil {
		return err
	}
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}

	return h.handleContextTokenAuthorizationFailures(ui.ContextToken, action, failures)
}

func (h *Handlers) contextTokenAllowsLoadedTask(c fiber.Ctx, action string, task *corev1alpha1.Task) (bool, error) {
	return h.contextTokenAllowsLoadedTaskWithIdentity(c, action, task, true)
}

func (h *Handlers) contextTokenAllowsLoadedTaskWithIdentity(c fiber.Ctx, action string, task *corev1alpha1.Task, includeTaskIdentity bool) (bool, error) {
	if !h.contextTokenAuthorization.Enabled() {
		return true, nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return true, nil
	}

	failures, err := h.contextTokenLoadedTaskContextFailures(c.Context(), ui.ContextToken, task, includeTaskIdentity)
	if err != nil {
		return false, err
	}
	if len(failures) == 0 {
		return true, nil
	}
	if h.contextTokenAuthorization.enforcing() {
		return false, nil
	}
	return true, h.handleContextTokenAuthorizationFailures(ui.ContextToken, action, failures)
}

func (h *Handlers) contextTokenLoadedTaskContextFailures(ctx context.Context, token *ContextToken, task *corev1alpha1.Task, includeTaskIdentity bool) ([]string, error) {
	return contextTokenLoadedTaskContextFailures(ctx, h.contextTokenAuthorizationReader(), token, task, includeTaskIdentity)
}

func contextTokenLoadedTaskContextFailures(ctx context.Context, reader client.Reader, token *ContextToken, task *corev1alpha1.Task, includeTaskIdentity bool) ([]string, error) {
	if token == nil || task == nil {
		return nil, nil
	}

	req := createTaskRequestFromTask(task)
	namespace := task.Namespace
	if namespace == "" {
		namespace = req.Namespace
	}

	authzCtx, err := resolveContextTokenTaskCreateAuthorizationContext(ctx, reader, req, namespace)
	if err != nil {
		return nil, fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}

	return contextTokenTaskContextFailures(token, authzCtx, includeTaskIdentity), nil
}

func createTaskRequestFromTask(task *corev1alpha1.Task) CreateTaskRequest {
	if task == nil {
		return CreateTaskRequest{}
	}

	req := CreateTaskRequest{
		Name:              task.Name,
		Namespace:         task.Namespace,
		Annotations:       task.Annotations,
		Type:              task.Spec.Type,
		Image:             task.Spec.Image,
		Command:           task.Spec.Command,
		Args:              task.Spec.Args,
		Env:               task.Spec.Env,
		Priority:          task.Spec.Priority,
		RetryPolicy:       task.Spec.RetryPolicy,
		WebhookURL:        task.Spec.WebhookURL,
		SecretRef:         task.Spec.SecretRef,
		SessionRef:        task.Spec.SessionRef,
		AI:                task.Spec.AI,
		AgentRef:          task.Spec.AgentRef,
		Prompt:            task.Spec.Prompt,
		AgentRuntime:      task.Spec.AgentRuntime,
		Execution:         task.Spec.Execution,
		Workspace:         task.Spec.Workspace,
		PriorTaskRef:      task.Spec.PriorTaskRef,
		Schedule:          task.Spec.Schedule,
		TimeZone:          task.Spec.TimeZone,
		ConcurrencyPolicy: string(task.Spec.ConcurrencyPolicy),
		Suspend:           task.Spec.Suspend,
	}
	if task.Spec.Timeout != nil {
		req.Timeout = task.Spec.Timeout.Duration.String()
	}

	return req
}

func (h *Handlers) authorizeContextTokenAction(c fiber.Ctx, action string, requiredScopes []string) error {
	return authorizeContextTokenActionWithConfig(c, h.contextTokenAuthorization, action, requiredScopes)
}

func (h *Handlers) handleContextTokenAuthorizationFailures(token *ContextToken, action string, failures []string) error {
	return handleContextTokenAuthorizationFailures(h.contextTokenAuthorization, token, action, failures)
}

func authorizeContextTokenActionWithConfig(c fiber.Ctx, cfg ContextTokenAuthorizationConfig, action string, requiredScopes []string) error {
	if !cfg.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}

	failures := []string{}
	if !hasAnyScope(ui.ContextToken.Scopes, requiredScopes) {
		failures = append(failures, fmt.Sprintf("missing one of required scopes %q", strings.Join(requiredScopes, ",")))
	}
	if want, ok := contextString(ui.ContextToken.TransactionContext, "namespace"); ok {
		if got, ok := c.Locals(resolvedNamespaceLocalKey).(string); ok && strings.TrimSpace(got) != "" && got != want {
			failures = append(failures, fmt.Sprintf("namespace %q does not match token context %q", got, want))
		}
	}
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, ui.ContextToken, action, failures)
}

func (h *Handlers) authorizeContextTokenTaskRead(c fiber.Ctx, action, namespace, taskName string) error {
	return authorizeContextTokenTaskReadWithConfig(c, h.contextTokenAuthorization, action, namespace, taskName)
}

func authorizeContextTokenTaskReadWithConfig(c fiber.Ctx, cfg ContextTokenAuthorizationConfig, action, namespace, taskName string) error {
	if !cfg.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}

	failures := contextTokenTaskReadFailures(ui.ContextToken, cfg, namespace, taskName)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, ui.ContextToken, action, failures)
}

func contextTokenTaskReadFailures(token *ContextToken, cfg ContextTokenAuthorizationConfig, namespace, taskName string) []string {
	failures := []string{}
	if !hasAnyScope(token.Scopes, cfg.TaskReadScopes) {
		failures = append(failures, fmt.Sprintf("missing one of required scopes %q", strings.Join(cfg.TaskReadScopes, ",")))
	}
	failures = append(failures, contextTokenTaskIdentityFailures(token, namespace, taskName)...)
	return failures
}

func contextTokenTaskIdentityFailures(token *ContextToken, namespace, taskName string) []string {
	failures := []string{}
	if want, ok := contextString(token.TransactionContext, "namespace"); ok && strings.TrimSpace(namespace) != "" && namespace != want {
		failures = append(failures, fmt.Sprintf("namespace %q does not match token context %q", namespace, want))
	}
	if want, ok := contextString(token.TransactionContext, "taskName"); ok && strings.TrimSpace(taskName) != "" && taskName != want {
		failures = append(failures, fmt.Sprintf("task name %q does not match token context %q", taskName, want))
	}
	if want, ok := contextString(token.TransactionContext, "task"); ok && strings.TrimSpace(taskName) != "" && !taskMatchesContext(namespace, taskName, want) {
		failures = append(failures, fmt.Sprintf("task %q does not match token context %q", namespacedNameString(namespace, taskName), want))
	}
	return failures
}

func taskMatchesContext(namespace, taskName, want string) bool {
	if taskName == want {
		return true
	}
	return strings.TrimSpace(namespace) != "" && namespacedNameString(namespace, taskName) == want
}

func authorizeContextTokenProviderUse(c fiber.Ctx, cfg ContextTokenAuthorizationConfig, action, namespace string, provider ProviderResolutionInfo, model string) error {
	if !cfg.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}

	failures := contextTokenProviderUseFailures(ui.ContextToken, cfg, namespace, provider, model)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, ui.ContextToken, action, failures)
}

func authorizeContextTokenProviderReference(c fiber.Ctx, cfg ContextTokenAuthorizationConfig, action, namespace string, provider ProviderResolutionInfo) error {
	if !cfg.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}

	failures := contextTokenProviderReferenceFailures(ui.ContextToken, cfg, namespace, provider)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, ui.ContextToken, action, failures)
}

func contextTokenAllowsListedProviderModel(c fiber.Ctx, cfg ContextTokenAuthorizationConfig, action, namespace string, provider ProviderResolutionInfo, model string) bool {
	if !cfg.Enabled() {
		return true
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return true
	}

	failures := contextTokenProviderUseFailures(ui.ContextToken, cfg, namespace, provider, model)
	if len(failures) == 0 {
		return true
	}
	if cfg.enforcing() {
		return false
	}
	_ = handleContextTokenAuthorizationFailures(cfg, ui.ContextToken, action, failures)
	return true
}

func authorizeContextTokenToolUse(c fiber.Ctx, cfg ContextTokenAuthorizationConfig, action string, toolNames []string) error {
	if !cfg.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}

	failures := []string{}
	if len(toolNames) > 0 && !hasAnyScope(ui.ContextToken.Scopes, cfg.ToolUseScopes) {
		failures = append(failures, fmt.Sprintf("missing one of required scopes %q", strings.Join(cfg.ToolUseScopes, ",")))
	}
	if allowed, ok := contextStringList(ui.ContextToken.TransactionContext, "allowedTools"); ok && !toolNamesAllowed(toolNames, allowed) {
		failures = append(failures, "one or more tools are not allowed by token context")
	}
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, ui.ContextToken, action, failures)
}

func authorizeContextTokenToolMetadata(c fiber.Ctx, cfg ContextTokenAuthorizationConfig, action, toolName string) error {
	if !cfg.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}
	failures := contextTokenToolMetadataFailures(ui.ContextToken, toolName)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, ui.ContextToken, action, failures)
}

func contextTokenAllowsToolMetadata(c fiber.Ctx, cfg ContextTokenAuthorizationConfig, action, toolName string) (bool, error) {
	if !cfg.Enabled() {
		return true, nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return true, nil
	}
	failures := contextTokenToolMetadataFailures(ui.ContextToken, toolName)
	if len(failures) == 0 {
		return true, nil
	}
	if cfg.enforcing() {
		return false, nil
	}
	return true, handleContextTokenAuthorizationFailures(cfg, ui.ContextToken, action, failures)
}

func contextTokenToolMetadataFailures(token *ContextToken, toolName string) []string {
	if allowed, ok := contextStringList(token.TransactionContext, "allowedTools"); ok && !slices.Contains(allowed, toolName) {
		return []string{fmt.Sprintf("tool %q is not allowed by token context", toolName)}
	}
	return nil
}

func authorizeContextTokenAgentContext(c fiber.Ctx, cfg ContextTokenAuthorizationConfig, action, namespace, agentName string) error {
	if !cfg.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}
	failures := contextTokenAgentContextFailures(ui.ContextToken, namespace, agentName)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return handleContextTokenAuthorizationFailures(cfg, ui.ContextToken, action, failures)
}

func contextTokenAllowsAgentContext(c fiber.Ctx, cfg ContextTokenAuthorizationConfig, action, namespace, agentName string) (bool, error) {
	if !cfg.Enabled() {
		return true, nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return true, nil
	}
	failures := contextTokenAgentContextFailures(ui.ContextToken, namespace, agentName)
	if len(failures) == 0 {
		return true, nil
	}
	if cfg.enforcing() {
		return false, nil
	}
	return true, handleContextTokenAuthorizationFailures(cfg, ui.ContextToken, action, failures)
}

func contextTokenAgentContextFailures(token *ContextToken, namespace, agentName string) []string {
	failures := []string{}
	if want, ok := contextString(token.TransactionContext, "namespace"); ok && namespace != want {
		failures = append(failures, fmt.Sprintf("namespace %q does not match token context %q", namespace, want))
	}
	if want, ok := contextString(token.TransactionContext, "agent"); ok && !agentMatches(agentName, namespace, want) {
		failures = append(failures, fmt.Sprintf("agent %q does not match token context %q", namespacedNameString(namespace, agentName), want))
	}
	if allowed, ok := contextStringList(token.TransactionContext, "allowedAgents"); ok && !agentAllowed(agentName, namespace, allowed) {
		failures = append(failures, fmt.Sprintf("agent %q is not allowed by token context", namespacedNameString(namespace, agentName)))
	}
	return failures
}

func contextTokenAgentMutationFailures(token *ContextToken, namespace, agentName string) []string {
	failures := []string{}
	if want, ok := contextString(token.TransactionContext, "namespace"); ok && namespace != want {
		failures = append(failures, fmt.Sprintf("namespace %q does not match token context %q", namespace, want))
	}
	if allowed, ok := contextStringList(token.TransactionContext, "allowedAgents"); ok {
		if !agentAllowed(agentName, namespace, allowed) {
			failures = append(failures, fmt.Sprintf("agent %q is not allowed by token context", namespacedNameString(namespace, agentName)))
		}
		return failures
	}
	if want, ok := contextString(token.TransactionContext, "agent"); ok && !agentMatches(agentName, namespace, want) {
		failures = append(failures, fmt.Sprintf("agent %q does not match token context %q", namespacedNameString(namespace, agentName), want))
	}
	return failures
}

func handleContextTokenAuthorizationFailures(cfg ContextTokenAuthorizationConfig, token *ContextToken, action string, failures []string) error {
	result := "audit"
	if cfg.enforcing() {
		result = "denied"
	}
	metrics.RecordContextTokenAuthorization(action, result, contextTokenAuthorizationFailureReason(failures))

	log.Info("context-token authorization failed",
		"mode", cfg.Mode,
		"action", action,
		"transactionID", token.TransactionID,
		"subject", token.Subject,
		"issuer", token.Issuer,
		"failures", redactedContextTokenAuthorizationFailures(failures),
	)
	if cfg.enforcing() {
		return fiber.NewError(fiber.StatusForbidden, "context token is not authorized for "+action)
	}
	return nil
}

func redactedContextTokenAuthorizationFailures(failures []string) string {
	return redact.SensitiveText(strings.Join(failures, "; "))
}

func contextTokenAuthorizationFailureReason(failures []string) string {
	if len(failures) == 0 {
		return "unknown"
	}
	joined := strings.ToLower(strings.Join(failures, "; "))
	switch {
	case strings.Contains(joined, "missing one of required scopes"):
		return "missing_scope"
	case strings.Contains(joined, "namespace"):
		return "namespace_mismatch"
	case strings.Contains(joined, "task name") || strings.Contains(joined, `task "`):
		return "task_mismatch"
	case strings.Contains(joined, "agent"):
		return "agent_mismatch"
	case strings.Contains(joined, "workspace repo") || strings.Contains(joined, "repository"):
		return "repo_mismatch"
	case strings.Contains(joined, "workspace branch"):
		return "branch_mismatch"
	case strings.Contains(joined, "workspace ref"):
		return "ref_mismatch"
	case strings.Contains(joined, "provider"):
		return "provider_mismatch"
	case strings.Contains(joined, "model"):
		return "model_mismatch"
	case strings.Contains(joined, "tool"):
		return "tool_not_allowed"
	default:
		return "context_violation"
	}
}

func contextTokenProviderUseFailures(token *ContextToken, cfg ContextTokenAuthorizationConfig, namespace string, provider ProviderResolutionInfo, model string) []string {
	failures := contextTokenProviderReferenceFailures(token, cfg, namespace, provider)
	tokenNamespace, hasTokenNamespace := contextString(token.TransactionContext, "namespace")
	if want, ok := contextString(token.TransactionContext, "model"); ok && model != want {
		failures = append(failures, fmt.Sprintf("model %q does not match token context %q", model, want))
	}
	if allowed, ok := contextStringList(token.TransactionContext, "allowedModels"); ok && !modelAllowed(provider, model, allowed, tokenNamespace, hasTokenNamespace) {
		failures = append(failures, fmt.Sprintf("model %q is not allowed by token context", model))
	}

	return failures
}

func contextTokenProviderReferenceFailures(token *ContextToken, cfg ContextTokenAuthorizationConfig, namespace string, provider ProviderResolutionInfo) []string {
	failures := []string{}
	if !hasAnyScope(token.Scopes, cfg.ProviderUseScopes) {
		failures = append(failures, fmt.Sprintf("missing one of required scopes %q", strings.Join(cfg.ProviderUseScopes, ",")))
	}

	tokenNamespace, hasTokenNamespace := contextString(token.TransactionContext, "namespace")
	if hasTokenNamespace {
		if namespace != tokenNamespace {
			failures = append(failures, fmt.Sprintf("namespace %q does not match token context %q", namespace, tokenNamespace))
		}
		if !providerNamespaceMatchesContext(provider, tokenNamespace, hasTokenNamespace) {
			failures = append(failures, fmt.Sprintf("provider namespace %q does not match token context %q", provider.Namespace, tokenNamespace))
		}
	}
	if want, ok := contextString(token.TransactionContext, "provider"); ok && !providerMatches(provider, want, tokenNamespace, hasTokenNamespace) {
		failures = append(failures, fmt.Sprintf("provider %q is not allowed by token context", provider.Name))
	}
	if allowed, ok := contextStringList(token.TransactionContext, "allowedProviders"); ok && !providerAllowed(provider, allowed, tokenNamespace, hasTokenNamespace) {
		failures = append(failures, fmt.Sprintf("provider %q is not allowed by token context", provider.Name))
	}

	return failures
}
func contextTokenAgentSpecFailures(ctx context.Context, reader client.Reader, token *ContextToken, agent *corev1alpha1.Agent) ([]string, error) {
	if token == nil || agent == nil {
		return nil, nil
	}
	authzCtx, err := resolveContextTokenAgentSpecAuthorizationContext(ctx, reader, agent)
	if err != nil {
		return nil, err
	}
	tokenNamespace, hasTokenNamespace := contextString(token.TransactionContext, "namespace")
	failures := contextTokenAgentSpecNamespaceFailures(authzCtx.Agent, tokenNamespace, hasTokenNamespace)
	failures = append(failures, contextTokenProviderModelConstraintFailures(token, authzCtx.EffectiveProvider, authzCtx.EffectiveModel, tokenNamespace, hasTokenNamespace, "agent ")...)
	for _, fb := range authzCtx.Fallbacks {
		failures = append(failures, contextTokenProviderModelConstraintFailures(token, fb.Provider, fb.Model, tokenNamespace, hasTokenNamespace, "agent fallback ")...)
	}
	failures = append(failures, contextTokenAgentSpecToolFailures(token, authzCtx)...)
	return failures, nil
}

func contextTokenAgentSpecNamespaceFailures(agent *corev1alpha1.Agent, tokenNamespace string, hasTokenNamespace bool) []string {
	if agent == nil || !hasTokenNamespace || agent.Spec.ProviderRef == nil {
		return nil
	}
	providerNamespace := strings.TrimSpace(agent.Spec.ProviderRef.Namespace)
	if providerNamespace == "" || providerNamespace == tokenNamespace {
		return nil
	}
	return []string{fmt.Sprintf("agent provider namespace %q does not match token context %q", providerNamespace, tokenNamespace)}
}

func resolveContextTokenAgentSpecAuthorizationContext(ctx context.Context, reader client.Reader, agent *corev1alpha1.Agent) (contextTokenAgentSpecAuthorizationContext, error) {
	authzCtx := contextTokenAgentSpecAuthorizationContext{
		Agent: agent,
	}
	var provider *corev1alpha1.Provider
	if agent != nil && agent.Spec.ProviderRef != nil && strings.TrimSpace(agent.Spec.ProviderRef.Name) != "" {
		providerNamespace := agent.Spec.ProviderRef.Namespace
		if providerNamespace == "" {
			providerNamespace = agent.Namespace
		}
		if reader != nil {
			provider = &corev1alpha1.Provider{}
			key := types.NamespacedName{Name: agent.Spec.ProviderRef.Name, Namespace: providerNamespace}
			if err := reader.Get(ctx, key, provider); err != nil {
				if !apierrors.IsNotFound(err) {
					return authzCtx, fmt.Errorf("resolve provider %q in namespace %q: %w", agent.Spec.ProviderRef.Name, providerNamespace, err)
				}
				provider = nil
			}
		}
	}
	authzCtx.EffectiveProvider, authzCtx.EffectiveModel = contextTokenTaskCreateEffectiveProviderModel(CreateTaskRequest{}, agent, provider)
	fallbacks, err := contextTokenTaskCreateFallbackProviderModels(ctx, reader, agent.Namespace, agent)
	if err != nil {
		return authzCtx, err
	}
	authzCtx.Fallbacks = fallbacks
	authzCtx.EffectiveAITools = contextTokenTaskCreateEffectiveAITools(CreateTaskRequest{}, agent)
	externalProfile, err := resolveContextTokenExternalRuntimeProfile(ctx, reader, agent.Namespace, agent)
	if err != nil {
		return authzCtx, err
	}
	if externalProfile != nil {
		authzCtx.EffectiveProvider = externalProfile.provider
		authzCtx.EffectiveModel = externalProfile.model
		authzCtx.RuntimeAllowedTools = externalProfile.allowedTools
		authzCtx.RuntimeAllowBash = externalProfile.allowBash
	} else {
		authzCtx.RuntimeAllowedTools, authzCtx.RuntimeAllowBash = contextTokenAgentRuntimeAuthorizationPolicy(agent)
	}
	return authzCtx, nil
}

func contextTokenAgentSpecToolFailures(token *ContextToken, authzCtx contextTokenAgentSpecAuthorizationContext) []string {
	allowed, ok := contextStringList(token.TransactionContext, "allowedTools")
	if !ok {
		return nil
	}
	if authzCtx.Agent != nil && authzCtx.Agent.Spec.Runtime != nil && authzCtx.Agent.Spec.Runtime.Type == corev1alpha1.AgentRuntimeOpencode {
		allowed = acp.NormalizeOpenCodeAuthorizationTools(allowed)
	}
	failures := []string{}
	if authzCtx.Agent != nil && authzCtx.Agent.Spec.Runtime != nil && contextTokenRuntimeToolsUnrestricted(authzCtx.RuntimeAllowedTools) {
		failures = append(failures, "agent runtime default tools are unrestricted while token context restricts allowedTools")
	}
	runtimeTools := append([]string{}, authzCtx.RuntimeAllowedTools...)
	runtime := (*corev1alpha1.AgentCLIRuntime)(nil)
	if authzCtx.Agent != nil {
		runtime = authzCtx.Agent.Spec.Runtime
	}
	if runtime != nil && runtime.RuntimeRef == nil &&
		runtime.Type != corev1alpha1.AgentRuntimeOpencode && authzCtx.RuntimeAllowBash {
		runtimeTools = append(runtimeTools, "Bash")
	}
	for _, tool := range append(append([]string{}, authzCtx.EffectiveAITools...), runtimeTools...) {
		tool = strings.TrimSpace(tool)
		if tool == "" {
			continue
		}
		if !slices.Contains(allowed, tool) {
			failures = append(failures, fmt.Sprintf("agent tool %q is not allowed by token context", tool))
		}
	}
	return failures
}

func (h *Handlers) contextTokenAuthorizationReader() client.Reader {
	if h.apiReader != nil {
		return h.apiReader
	}
	return h.client
}

func resolveContextTokenTaskCreateAuthorizationContext(ctx context.Context, reader client.Reader, req CreateTaskRequest, namespace string) (contextTokenTaskCreateAuthorizationContext, error) {
	authzCtx := contextTokenTaskCreateAuthorizationContext{
		Request:   req,
		Namespace: namespace,
	}

	if req.AgentRef != nil {
		authzCtx.AgentName = req.AgentRef.Name
		authzCtx.AgentNamespace = req.AgentRef.Namespace
		if authzCtx.AgentNamespace == "" {
			authzCtx.AgentNamespace = namespace
		}

		if authzCtx.AgentName != "" && reader != nil {
			agent := &corev1alpha1.Agent{}
			key := types.NamespacedName{Name: authzCtx.AgentName, Namespace: authzCtx.AgentNamespace}
			if err := reader.Get(ctx, key, agent); err != nil {
				if !apierrors.IsNotFound(err) {
					return authzCtx, fmt.Errorf("resolve agent %q in namespace %q: %w", authzCtx.AgentName, authzCtx.AgentNamespace, err)
				}
			} else {
				authzCtx.Agent = agent
			}
		}
	}

	providerRef := contextTokenTaskCreateProviderRef(req, authzCtx.Agent)
	if providerRef != nil && strings.TrimSpace(providerRef.Name) != "" {
		providerNamespace := providerRef.Namespace
		if providerNamespace == "" {
			providerNamespace = namespace
		}
		authzCtx.ProviderRef = ProviderResolutionInfo{Name: providerRef.Name, Namespace: providerNamespace}
		if reader != nil {
			provider := &corev1alpha1.Provider{}
			key := types.NamespacedName{Name: providerRef.Name, Namespace: providerNamespace}
			if err := reader.Get(ctx, key, provider); err != nil {
				if !apierrors.IsNotFound(err) {
					return authzCtx, fmt.Errorf("resolve provider %q in namespace %q: %w", providerRef.Name, providerNamespace, err)
				}
			} else {
				authzCtx.Provider = provider
			}
		}
	}

	authzCtx.EffectiveProvider, authzCtx.EffectiveModel = contextTokenTaskCreateEffectiveProviderModel(req, authzCtx.Agent, authzCtx.Provider)
	externalProfile, err := resolveContextTokenExternalRuntimeProfile(ctx, reader, namespace, authzCtx.Agent)
	if err != nil {
		return authzCtx, err
	}
	if externalProfile != nil {
		authzCtx.EffectiveProvider = externalProfile.provider
		authzCtx.EffectiveModel = externalProfile.model
		authzCtx.RuntimeProviderKind = externalProfile.providerKind
	}
	authzCtx.Fallbacks, err = contextTokenTaskCreateFallbackProviderModels(ctx, reader, namespace, authzCtx.Agent)
	if err != nil {
		return authzCtx, err
	}
	authzCtx.EffectiveAITools = contextTokenTaskCreateEffectiveAITools(req, authzCtx.Agent)
	if externalProfile != nil {
		if req.AgentRuntime == nil || req.AgentRuntime.AllowedTools == nil {
			authzCtx.PolicyFailures = append(authzCtx.PolicyFailures, "task agentRuntime.allowedTools must be an explicit list for an external AgentRuntime")
		} else if !slices.Equal(contextTokenSortedUniqueToolNames(req.AgentRuntime.AllowedTools), externalProfile.registeredAllowedTools) {
			authzCtx.PolicyFailures = append(authzCtx.PolicyFailures, "task allowedTools do not exactly match the registered external AgentRuntime MCP policy")
		}
		authzCtx.RuntimeAllowedTools = externalProfile.allowedTools
		authzCtx.RuntimeAllowBash = externalProfile.allowBash
	} else {
		authzCtx.RuntimeAllowedTools, authzCtx.RuntimeAllowBash = contextTokenTaskCreateEffectiveRuntimePolicy(req, authzCtx.Agent)
	}

	return authzCtx, nil
}

type contextTokenExternalRuntimeProfile struct {
	provider               ProviderResolutionInfo
	providerKind           string
	model                  string
	registeredAllowedTools []string
	allowedTools           []string
	allowBash              bool
}

func resolveContextTokenExternalRuntimeProfile(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	agent *corev1alpha1.Agent,
) (*contextTokenExternalRuntimeProfile, error) {
	if agent == nil || agent.Spec.Runtime == nil || agent.Spec.Runtime.RuntimeRef == nil {
		return nil, nil
	}
	runtimeName := strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name)
	if runtimeName == "" {
		return nil, nil
	}
	if reader == nil {
		return nil, fmt.Errorf("resolve AgentRuntime %q in namespace %q: Kubernetes client is required", runtimeName, namespace)
	}

	runtime := &corev1alpha1.AgentRuntime{}
	if err := reader.Get(ctx, types.NamespacedName{Name: runtimeName, Namespace: namespace}, runtime); err != nil {
		return nil, fmt.Errorf("resolve AgentRuntime %q in namespace %q: %w", runtimeName, namespace, err)
	}
	if runtime.RegisteredContractVersion() != corev1alpha1.AgentRuntimeContractHarnessV2 {
		return nil, nil
	}
	if runtime.Spec.Capabilities == nil || runtime.Spec.Capabilities.Profile == nil {
		return nil, fmt.Errorf("external AgentRuntime %q is missing capabilities.profile", runtimeName)
	}

	providerKind := strings.TrimSpace(runtime.Spec.Capabilities.Profile.ProviderKind)
	if providerKind == "" {
		return nil, fmt.Errorf("external AgentRuntime %q capabilities.profile.providerKind is required", runtimeName)
	}
	model := strings.TrimSpace(runtime.Spec.Capabilities.Profile.Model)
	if model == "" {
		return nil, fmt.Errorf("external AgentRuntime %q capabilities.profile.model is required", runtimeName)
	}
	policy := runtime.Spec.Capabilities.MCPPolicy
	if policy == nil {
		return nil, fmt.Errorf("external AgentRuntime %q is missing capabilities.mcpPolicy", runtimeName)
	}
	if policy.AllowedTools == nil || policy.DisallowedTools == nil || policy.ApprovalRequiredTools == nil {
		return nil, fmt.Errorf("external AgentRuntime %q capabilities.mcpPolicy tool lists must be explicit", runtimeName)
	}
	return &contextTokenExternalRuntimeProfile{
		provider:               ProviderResolutionInfo{Type: providerKind},
		providerKind:           providerKind,
		model:                  model,
		registeredAllowedTools: contextTokenSortedUniqueToolNames(policy.AllowedTools),
		allowedTools: acp.BuiltInRuntimeEffectiveAllowedTools(
			policy.AllowedTools, policy.DisallowedTools, policy.AllowBash,
		),
		allowBash: acp.BuiltInRuntimeEffectiveAllowBash(
			policy.AllowedTools, policy.DisallowedTools, policy.AllowBash,
		),
	}, nil
}

func contextTokenTaskCreateFallbackProviderModels(ctx context.Context, reader client.Reader, namespace string, agent *corev1alpha1.Agent) ([]contextTokenProviderModel, error) {
	if reader == nil || agent == nil || agent.Spec.Model == nil || len(agent.Spec.Model.Fallbacks) == 0 {
		return nil, nil
	}
	fallbacks := make([]contextTokenProviderModel, 0, len(agent.Spec.Model.Fallbacks))
	for _, fb := range agent.Spec.Model.Fallbacks {
		if strings.TrimSpace(fb.ProviderRef) == "" {
			continue
		}
		provider := &corev1alpha1.Provider{}
		if err := reader.Get(ctx, types.NamespacedName{Name: fb.ProviderRef, Namespace: namespace}, provider); err != nil {
			return nil, fmt.Errorf("resolve fallback provider %q in namespace %q: %w", fb.ProviderRef, namespace, err)
		}
		model := strings.TrimSpace(fb.Model)
		if model == "" {
			model = provider.Spec.DefaultModel
		}
		fallbacks = append(fallbacks, contextTokenProviderModel{
			Provider: providerResolutionInfo(provider),
			Model:    model,
		})
	}
	return fallbacks, nil
}

func contextTokenTaskCreateProviderRef(req CreateTaskRequest, agent *corev1alpha1.Agent) *corev1alpha1.ProviderReference {
	if contextTokenOpenCodeAgentTask(req, agent) {
		return nil
	}
	if req.AI != nil && req.AI.ProviderRef != nil {
		return req.AI.ProviderRef
	}
	if agent != nil && agent.Spec.ProviderRef != nil {
		return agent.Spec.ProviderRef
	}
	return nil
}

func contextTokenTaskCreateEffectiveProviderModel(req CreateTaskRequest, agent *corev1alpha1.Agent, provider *corev1alpha1.Provider) (ProviderResolutionInfo, string) {
	providerInfo := ProviderResolutionInfo{}
	model := ""
	openCodeAgent := contextTokenOpenCodeAgent(agent)
	openCodeAgentTask := contextTokenOpenCodeAgentTask(req, agent)

	if provider != nil && !openCodeAgent {
		providerInfo = providerResolutionInfo(provider)
		model = provider.Spec.DefaultModel
	}

	if agent != nil && agent.Spec.Model != nil {
		if openCodeAgent {
			providerInfo = ProviderResolutionInfo{Type: contextTokenOpenCodeModelProvider(agent)}
		} else if strings.TrimSpace(agent.Spec.Model.Provider) != "" {
			providerInfo = ProviderResolutionInfo{Type: agent.Spec.Model.Provider}
		}
		if strings.TrimSpace(agent.Spec.Model.Name) != "" {
			model = agent.Spec.Model.Name
		}
	}

	// Built-in OpenCode executes the immutable Agent model identity. Task AI
	// fields are not part of that runtime profile and must not authorize a
	// different provider/model than the one that will execute.
	if req.AI != nil && !openCodeAgentTask {
		if strings.TrimSpace(req.AI.Provider) != "" {
			providerInfo = ProviderResolutionInfo{Type: req.AI.Provider}
		}
		if strings.TrimSpace(req.AI.Model) != "" {
			model = req.AI.Model
		}
	}

	// Provider CRD type is authoritative for provider-backed execution. OpenCode
	// instead derives its immutable provider identity from the qualified model ID.
	if provider != nil && !openCodeAgent {
		providerInfo = providerResolutionInfo(provider)
	}

	return providerInfo, model
}

func contextTokenOpenCodeAgentTask(req CreateTaskRequest, agent *corev1alpha1.Agent) bool {
	return req.Type == corev1alpha1.TaskTypeAgent && contextTokenOpenCodeAgent(agent)
}

func contextTokenOpenCodeAgent(agent *corev1alpha1.Agent) bool {
	return agent != nil && agent.Spec.Runtime != nil && agent.Spec.Runtime.Type == corev1alpha1.AgentRuntimeOpencode
}

func contextTokenOpenCodeModelProvider(agent *corev1alpha1.Agent) string {
	if !contextTokenOpenCodeAgent(agent) || agent.Spec.Model == nil {
		return ""
	}
	provider, model, ok := strings.Cut(strings.TrimSpace(agent.Spec.Model.Name), "/")
	if !ok || strings.TrimSpace(model) == "" {
		return ""
	}
	return strings.TrimSpace(provider)
}

func contextTokenTaskCreateEffectiveAITools(req CreateTaskRequest, agent *corev1alpha1.Agent) []string {
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
		if agent.Spec.Coordination != nil && agent.Spec.Coordination.Enabled &&
			(agent.Spec.Runtime == nil || agent.Spec.Runtime.RuntimeRef == nil) &&
			req.Annotations[labels.AnnotationDisableCoordinationToolInject] != queryTrue {
			for _, tool := range coordinationToolNames() {
				if !slices.Contains(tools, tool) {
					tools = append(tools, tool)
				}
			}
		}
	}
	if req.AI != nil {
		for _, tool := range req.AI.Tools {
			if strings.TrimSpace(tool) != "" {
				tools = append(tools, tool)
			}
		}
	}
	if req.Type == corev1alpha1.TaskTypeAI {
		for _, tool := range memoryToolNames() {
			if !slices.Contains(tools, tool) {
				tools = append(tools, tool)
			}
		}
	}
	return tools
}

func memoryToolNames() []string {
	return []string{
		"recall_memory",
		"remember",
		"propose_memory",
		"search_transcript",
	}
}

func coordinationToolNames() []string {
	return []string{
		"delegate_task", "wait_for_tasks", "create_container_task", "cancel_task",
		"send_message", "check_messages", "recall_memory", "remember",
		"propose_memory", "search_transcript", "create_pull_request",
		"list_pull_requests", "check_pr_review_marker", "check_pull_request_ci",
		"merge_pull_request", "auto_merge_pull_request", "review_pull_request",
		"post_review_comment", "create_agent", "delete_agent", "update_plan",
	}
}

func contextTokenAgentRuntimeAllowedTools(agent *corev1alpha1.Agent) []string {
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

func contextTokenAgentRuntimeAllowBash(agent *corev1alpha1.Agent) bool {
	allowBash := true
	if agent != nil && agent.Spec.Runtime != nil && agent.Spec.Runtime.DefaultAllowBash != nil {
		allowBash = *agent.Spec.Runtime.DefaultAllowBash
	}
	return allowBash
}

func contextTokenAgentRuntimeAuthorizationPolicy(agent *corev1alpha1.Agent) ([]string, bool) {
	allowedTools := contextTokenAgentRuntimeAllowedTools(agent)
	allowBash := contextTokenAgentRuntimeAllowBash(agent)
	if agent == nil || agent.Spec.Runtime == nil {
		return allowedTools, allowBash
	}
	if agent.Spec.Runtime.Type != corev1alpha1.AgentRuntimeOpencode {
		if len(allowedTools) > 0 && !hasNonEmptyToolNames(allowedTools) {
			return allowedTools, allowBash
		}
		allowedTools, _, allowBash = acp.NormalizeBuiltInRuntimeToolPolicy(
			string(agent.Spec.Runtime.Type), allowedTools, nil, allowBash,
		)
		allowedTools = acp.BuiltInRuntimeEffectiveAllowedTools(allowedTools, nil, allowBash)
		allowBash = acp.BuiltInRuntimeEffectiveAllowBash(allowedTools, nil, allowBash)
		return allowedTools, allowBash
	}
	allowedTools, disallowedTools, allowBash := acp.NormalizeOpenCodeToolPolicy(false, allowedTools, nil, allowBash)
	allowedTools = acp.OpenCodeEffectiveAllowedTools(allowedTools, disallowedTools, allowBash)
	return allowedTools, allowBash && slices.Contains(allowedTools, "bash")
}

func contextTokenTaskCreateEffectiveRuntimePolicy(req CreateTaskRequest, agent *corev1alpha1.Agent) ([]string, bool) {
	allowedTools := contextTokenAgentRuntimeAllowedTools(agent)
	if req.AgentRuntime != nil && req.AgentRuntime.AllowedTools != nil {
		allowedTools = append([]string{}, req.AgentRuntime.AllowedTools...)
	}
	allowBash := contextTokenAgentRuntimeAllowBash(agent)
	disallowedTools := []string(nil)
	if req.AgentRuntime != nil && req.AgentRuntime.AllowBash != nil {
		allowBash = *req.AgentRuntime.AllowBash
	}
	if req.AgentRuntime != nil {
		disallowedTools = append(disallowedTools, req.AgentRuntime.DisallowedTools...)
	}
	if agent == nil || agent.Spec.Runtime == nil {
		return allowedTools, allowBash
	}
	if agent.Spec.Runtime.Type != corev1alpha1.AgentRuntimeOpencode {
		if len(allowedTools) > 0 && !hasNonEmptyToolNames(allowedTools) {
			return allowedTools, allowBash
		}
		allowedTools, disallowedTools, allowBash = acp.NormalizeBuiltInRuntimeToolPolicy(
			string(agent.Spec.Runtime.Type), allowedTools, disallowedTools, allowBash,
		)
		allowedTools = acp.BuiltInRuntimeEffectiveAllowedTools(allowedTools, disallowedTools, allowBash)
		allowBash = acp.BuiltInRuntimeEffectiveAllowBash(allowedTools, disallowedTools, allowBash)
		return allowedTools, allowBash
	}
	workspace := taskRequestWorkspace(req)
	readIntent := workspace == nil || workspace.Intent == "" || workspace.Intent == corev1alpha1.WorkspaceIntentRead
	allowedTools, disallowedTools, allowBash = acp.NormalizeOpenCodeToolPolicy(readIntent, allowedTools, disallowedTools, allowBash)
	allowedTools = acp.OpenCodeEffectiveAllowedTools(allowedTools, disallowedTools, allowBash)
	return allowedTools, allowBash && slices.Contains(allowedTools, "bash")
}

func contextTokenSortedUniqueToolNames(values []string) []string {
	if values == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	slices.Sort(normalized)
	return normalized
}

func contextTokenTaskToolCredentialFailures(
	ctx context.Context,
	reader client.Reader,
	token *ContextToken,
	cfg ContextTokenAuthorizationConfig,
	authzCtx contextTokenTaskCreateAuthorizationContext,
) ([]string, error) {
	if token == nil || reader == nil {
		return nil, nil
	}
	toolNames := append([]string{}, authzCtx.EffectiveAITools...)
	runtimeToolNames := contextTokenRuntimeToolConstraints(authzCtx)
	toolNames = append(toolNames, runtimeToolNames...)
	requiresResolution := make(map[string]bool, len(toolNames))
	for _, name := range authzCtx.EffectiveAITools {
		name = strings.TrimSpace(name)
		if name != "" && !contextTokenPlatformAIToolName(authzCtx, name) {
			requiresResolution[name] = true
		}
	}
	for _, name := range runtimeToolNames {
		name = strings.TrimSpace(name)
		if name != "" && !contextTokenNativeRuntimeToolName(authzCtx, name) {
			requiresResolution[name] = true
		}
	}
	seenTools := map[string]struct{}{}
	credentialSecrets := map[string]struct{}{}
	requiresCredentialScope := false
	failures := []string{}
	for _, toolName := range toolNames {
		toolName = strings.TrimSpace(toolName)
		if toolName == "" {
			continue
		}
		if _, ok := seenTools[toolName]; ok {
			continue
		}
		seenTools[toolName] = struct{}{}
		if !requiresResolution[toolName] {
			continue
		}
		tool := &corev1alpha1.Tool{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: authzCtx.Namespace, Name: toolName}, tool); err != nil {
			if apierrors.IsNotFound(err) {
				failures = append(failures, fmt.Sprintf("Tool %q is unresolved", toolName))
				continue
			}
			return nil, fmt.Errorf("resolve Tool %q credential policy: %w", toolName, err)
		}
		if tool.Spec.HTTP == nil {
			continue
		}
		if tool.Spec.HTTP.AuthSecretRef != nil {
			requiresCredentialScope = true
			credentialSecrets[tool.Spec.HTTP.AuthSecretRef.Name] = struct{}{}
		}
		if tool.Spec.HTTP.OutboundAccessPolicyRef == nil {
			continue
		}
		policyName := strings.TrimSpace(tool.Spec.HTTP.OutboundAccessPolicyRef.Name)
		policy := &corev1alpha1.OutboundAccessPolicy{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: authzCtx.Namespace, Name: policyName}, policy); err != nil {
			if apierrors.IsNotFound(err) {
				failures = append(failures, fmt.Sprintf("Tool %q references unresolved OutboundAccessPolicy %q", toolName, policyName))
				continue
			}
			return nil, fmt.Errorf("resolve OutboundAccessPolicy %q: %w", policyName, err)
		}
		if !outboundAccessPolicyReadyForContextAuthorization(policy) {
			failures = append(failures, fmt.Sprintf("Tool %q references unresolved OutboundAccessPolicy %q", toolName, policyName))
			continue
		}
		if direct := policy.Spec.Direct; direct != nil {
			collectOutboundCredentialSecrets(credentialSecrets, direct)
			if outboundAccessUsesServiceAccount(direct) {
				requiresCredentialScope = true
			}
		}
		if gateway := policy.Spec.Gateway; gateway != nil {
			collectOutboundTLSCredentialSecret(credentialSecrets, gateway.TLS)
		}
	}
	if len(credentialSecrets) > 0 {
		requiresCredentialScope = true
	}
	if !requiresCredentialScope {
		return failures, nil
	}
	requiredScopes := cfg.SecretCredentialReadScopes()
	if !hasAnyScope(token.Scopes, requiredScopes) {
		failures = append(failures, fmt.Sprintf("Tool outbound credentials require one of scopes %q", strings.Join(requiredScopes, ",")))
	}
	if want, ok := contextString(token.TransactionContext, "secret"); ok {
		for secretName := range credentialSecrets {
			if secretName != want {
				failures = append(failures, fmt.Sprintf("credential secret %q does not match token context %q", secretName, want))
			}
		}
	}
	return failures, nil
}

func outboundAccessPolicyReadyForContextAuthorization(policy *corev1alpha1.OutboundAccessPolicy) bool {
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

func outboundAccessUsesServiceAccount(direct *corev1alpha1.DirectOutboundAccess) bool {
	if direct == nil {
		return false
	}
	if direct.Subject.Source == corev1alpha1.OutboundTokenSourceServiceAccount {
		return true
	}
	return direct.Actor != nil && direct.Actor.Source == corev1alpha1.OutboundTokenSourceServiceAccount
}

func collectOutboundCredentialSecrets(names map[string]struct{}, direct *corev1alpha1.DirectOutboundAccess) {
	if direct == nil {
		return
	}
	collect := func(source *corev1alpha1.OutboundTokenSource) {
		if source != nil && source.SecretRef != nil && strings.TrimSpace(source.SecretRef.Name) != "" {
			names[source.SecretRef.Name] = struct{}{}
		}
	}
	collect(&direct.Subject)
	collect(direct.Actor)
	if auth := direct.ClientAuthentication; auth != nil {
		if auth.ClientSecretRef != nil && strings.TrimSpace(auth.ClientSecretRef.Name) != "" {
			names[auth.ClientSecretRef.Name] = struct{}{}
		}
		if auth.PrivateKeyRef != nil && strings.TrimSpace(auth.PrivateKeyRef.Name) != "" {
			names[auth.PrivateKeyRef.Name] = struct{}{}
		}
	}
	collectOutboundTLSCredentialSecret(names, direct.TokenEndpoint.TLS)
}

func collectOutboundTLSCredentialSecret(names map[string]struct{}, tlsConfig *corev1alpha1.OutboundTLSConfig) {
	if tlsConfig == nil || tlsConfig.CASecretRef == nil {
		return
	}
	if name := strings.TrimSpace(tlsConfig.CASecretRef.Name); name != "" {
		names[name] = struct{}{}
	}
}

func contextTokenTaskCreateFailures(token *ContextToken, cfg ContextTokenAuthorizationConfig, authzCtx contextTokenTaskCreateAuthorizationContext) []string {
	failures := contextTokenTaskCreateScopeFailures(token, cfg)
	failures = append(failures, contextTokenWorkspaceCredentialFailures(token, cfg, taskRequestWorkspace(authzCtx.Request))...)
	failures = append(failures, contextTokenTaskContextFailures(token, authzCtx, true)...)
	return failures
}

func contextTokenTaskCreateScopeFailures(token *ContextToken, cfg ContextTokenAuthorizationConfig) []string {
	if hasAnyScope(token.Scopes, cfg.TaskCreateScopes) {
		return nil
	}
	return []string{fmt.Sprintf("missing one of required scopes %q", strings.Join(cfg.TaskCreateScopes, ","))}
}

func contextTokenWorkspaceCredentialFailures(token *ContextToken, cfg ContextTokenAuthorizationConfig, workspace *corev1alpha1.WorkspaceConfig) []string {
	if workspace == nil {
		return nil
	}
	credentials := []struct {
		role string
		ref  *corev1alpha1.WorkspaceCredentialReference
	}{
		{role: "source-read", ref: workspace.ReadCredentialRef},
		{role: "target-read", ref: workspace.PublicationReadCredentialRef},
		{role: "target-write", ref: workspace.PublicationCredentialRef},
		{role: "forge", ref: workspace.ForgeCredentialRef},
	}
	hasCredential := false
	for _, credential := range credentials {
		if credential.ref != nil && strings.TrimSpace(credential.ref.Name) != "" {
			hasCredential = true
			break
		}
	}
	if !hasCredential {
		return nil
	}
	requiredScopes := cfg.SecretCredentialReadScopes()
	if len(requiredScopes) == 0 {
		return nil
	}
	failures := []string{}
	if !hasAnyScope(token.Scopes, requiredScopes) {
		failures = append(failures, fmt.Sprintf(
			"workspace credentials require one of scopes %q",
			strings.Join(requiredScopes, ","),
		))
	}
	if want, ok := contextString(token.TransactionContext, "secret"); ok {
		for _, credential := range credentials {
			if credential.ref == nil || strings.TrimSpace(credential.ref.Name) == "" {
				continue
			}
			if credential.ref.Name != want {
				failures = append(failures, fmt.Sprintf("workspace %s credential %q does not match token context %q", credential.role, credential.ref.Name, want))
			}
		}
	}
	return failures
}

func contextTokenTaskContextFailures(token *ContextToken, authzCtx contextTokenTaskCreateAuthorizationContext, includeTaskIdentity bool) []string {
	failures := append([]string{}, authzCtx.PolicyFailures...)
	req := authzCtx.Request

	if includeTaskIdentity {
		failures = append(failures, contextTokenTaskIdentityFailures(token, authzCtx.Namespace, req.Name)...)
	}
	tokenNamespace, hasTokenNamespace := contextString(token.TransactionContext, "namespace")
	if hasTokenNamespace && !includeTaskIdentity {
		failures = append(failures, contextTokenTaskCreateNamespaceFailures(authzCtx, tokenNamespace)...)
	} else if hasTokenNamespace {
		failures = append(failures, contextTokenTaskDependencyNamespaceFailures(authzCtx, tokenNamespace)...)
	}
	if want, ok := contextString(token.TransactionContext, "taskType"); ok && string(req.Type) != want {
		failures = append(failures, fmt.Sprintf("task type %q does not match token context %q", req.Type, want))
	}
	if want, ok := contextString(token.TransactionContext, "agent"); ok {
		if !agentMatches(authzCtx.AgentName, authzCtx.AgentNamespace, want) {
			failures = append(failures, fmt.Sprintf("agent %q does not match token context %q", namespacedNameString(authzCtx.AgentNamespace, authzCtx.AgentName), want))
		}
	}
	if allowed, ok := contextStringList(token.TransactionContext, "allowedAgents"); ok {
		if authzCtx.AgentName == "" {
			failures = append(failures, "task does not specify an agent allowed by token context")
		} else if !agentAllowed(authzCtx.AgentName, authzCtx.AgentNamespace, allowed) {
			failures = append(failures, fmt.Sprintf("agent %q is not allowed by token context", namespacedNameString(authzCtx.AgentNamespace, authzCtx.AgentName)))
		}
	}
	failures = append(failures, contextTokenProviderModelConstraintFailures(token, authzCtx.EffectiveProvider, authzCtx.EffectiveModel, tokenNamespace, hasTokenNamespace, "")...)
	for _, fb := range authzCtx.Fallbacks {
		failures = append(failures, contextTokenProviderModelConstraintFailures(token, fb.Provider, fb.Model, tokenNamespace, hasTokenNamespace, "fallback ")...)
	}

	failures = append(failures, contextTokenWorkspaceFailures(token, taskRequestWorkspace(req))...)
	failures = append(failures, contextTokenTaskToolFailures(token, authzCtx)...)

	return failures
}

func contextTokenProviderModelConstraintFailures(token *ContextToken, provider ProviderResolutionInfo, model, tokenNamespace string, hasTokenNamespace bool, prefix string) []string {
	failures := []string{}
	if want, ok := contextString(token.TransactionContext, "provider"); ok && !providerMatches(provider, want, tokenNamespace, hasTokenNamespace) {
		failures = append(failures, fmt.Sprintf("%sprovider %q is not allowed by token context", prefix, providerDisplayName(provider)))
	}
	if allowed, ok := contextStringList(token.TransactionContext, "allowedProviders"); ok && !providerAllowed(provider, allowed, tokenNamespace, hasTokenNamespace) {
		failures = append(failures, fmt.Sprintf("%sprovider %q is not allowed by token context", prefix, providerDisplayName(provider)))
	}
	if want, ok := contextString(token.TransactionContext, "model"); ok && model != want {
		failures = append(failures, fmt.Sprintf("%smodel %q does not match token context %q", prefix, model, want))
	}
	if allowed, ok := contextStringList(token.TransactionContext, "allowedModels"); ok && !modelAllowed(provider, model, allowed, tokenNamespace, hasTokenNamespace) {
		failures = append(failures, fmt.Sprintf("%smodel %q is not allowed by token context", prefix, model))
	}
	return failures
}

func contextTokenWorkspaceFailures(token *ContextToken, workspace *corev1alpha1.WorkspaceConfig) []string {
	failures := []string{}
	if want, ok := contextString(token.TransactionContext, "repo"); ok && workspaceGitRepo(workspace) != want {
		failures = append(failures, fmt.Sprintf("workspace repo %q does not match token context %q", workspaceGitRepo(workspace), want))
	}

	gotBranch := workspaceBranch(workspace)
	gotRef := workspaceRef(workspace)
	wantRef, hasWantRef := contextString(token.TransactionContext, "ref")
	if want, ok := contextString(token.TransactionContext, "branch"); ok {
		refOnlyWorkspaceMatches := gotBranch == "" && hasWantRef && gotRef == wantRef
		if !refOnlyWorkspaceMatches && gotBranch != want {
			failures = append(failures, fmt.Sprintf("workspace branch %q does not match token context %q", gotBranch, want))
		}
		// Execution gives workspace.ref precedence over branch, so a
		// branch-only token constraint must not be bypassed by submitting the
		// allowed branch together with an unconstrained ref selector.
		if !hasWantRef && gotRef != "" {
			failures = append(failures, fmt.Sprintf("workspace ref %q overrides the branch constrained by token context", gotRef))
		}
	}
	if hasWantRef && gotRef != wantRef {
		failures = append(failures, fmt.Sprintf("workspace ref %q does not match token context %q", gotRef, wantRef))
	}
	return failures
}

func contextTokenTaskToolFailures(token *ContextToken, authzCtx contextTokenTaskCreateAuthorizationContext) []string {
	allowed, ok := contextStringList(token.TransactionContext, "allowedTools")
	if !ok {
		return nil
	}
	if authzCtx.Agent != nil && authzCtx.Agent.Spec.Runtime != nil && authzCtx.Agent.Spec.Runtime.Type == corev1alpha1.AgentRuntimeOpencode {
		allowed = acp.NormalizeOpenCodeAuthorizationTools(allowed)
	}
	failures := []string{}
	if authzCtx.Request.Type == corev1alpha1.TaskTypeAgent && contextTokenRuntimeToolsUnrestricted(authzCtx.RuntimeAllowedTools) {
		failures = append(failures, "agent runtime tools are unrestricted by task or agent while token context restricts allowedTools")
	}
	runtimeTools := contextTokenRuntimeToolConstraints(authzCtx)
	for _, tool := range append(append([]string{}, authzCtx.EffectiveAITools...), runtimeTools...) {
		tool = strings.TrimSpace(tool)
		if tool == "" {
			continue
		}
		if !slices.Contains(allowed, tool) {
			failures = append(failures, fmt.Sprintf("tool %q is not allowed by token context", tool))
		}
	}
	return failures
}

func contextTokenPlatformAIToolName(authzCtx contextTokenTaskCreateAuthorizationContext, name string) bool {
	if slices.Contains(memoryToolNames(), name) {
		return true
	}
	if slices.Contains(toolspkg.CoordinationToolNames(), name) {
		coordinationEnabled := authzCtx.Agent != nil && authzCtx.Agent.Spec.Coordination != nil && authzCtx.Agent.Spec.Coordination.Enabled
		return coordinationEnabled || slices.Contains(toolspkg.ChatToolNames(), name)
	}
	_, builtin := toolspkg.DefaultRegistry.Get(name)
	return builtin
}

func contextTokenNativeRuntimeToolName(authzCtx contextTokenTaskCreateAuthorizationContext, name string) bool {
	base := strings.TrimSpace(name)
	if index := strings.IndexByte(base, '('); index >= 0 {
		base = strings.TrimSpace(base[:index])
	}
	runtime := (*corev1alpha1.AgentCLIRuntime)(nil)
	if authzCtx.Agent != nil {
		runtime = authzCtx.Agent.Spec.Runtime
	}
	if runtime != nil && runtime.RuntimeRef != nil {
		if authzCtx.RuntimeProviderKind != "" {
			if slices.Contains(toolspkg.KnownBuiltInToolNames(), base) {
				return true
			}
			return acp.IsBuiltInRuntimeNativeTool(authzCtx.RuntimeProviderKind, base)
		}
		brokeredOverride := authzCtx.Request.AgentRuntime != nil && hasNonEmptyToolNames(authzCtx.Request.AgentRuntime.AllowedTools)
		if !brokeredOverride {
			return true
		}
		if slices.Contains([]string{"delegate_task", "wait_for_tasks", "cancel_task", "send_message", "check_messages"}, base) {
			return true
		}
		explicitBash := slices.ContainsFunc(authzCtx.Request.AgentRuntime.AllowedTools, func(value string) bool {
			return strings.TrimSpace(value) == "Bash"
		})
		return base == "Bash" && !explicitBash
	}
	if _, builtin := toolspkg.DefaultRegistry.Get(base); builtin {
		return true
	}
	if runtime != nil && runtime.Type != "" {
		return contextTokenBuiltInRuntimeNativeToolName(runtime.Type, base)
	}
	if slices.Contains(toolspkg.CoordinationToolNames(), base) {
		return true
	}
	switch strings.ToLower(base) {
	case "bash", "read", "write", "edit", "glob", "grep", "websearch", "webfetch", "ls", "create_file", "web_search":
		return true
	default:
		return false
	}
}

// Use the shared built-in runtime policy so credential authorization and MCP projection cannot drift.
func contextTokenBuiltInRuntimeNativeToolName(runtimeType corev1alpha1.AgentRuntimeType, name string) bool {
	return acp.IsBuiltInRuntimeNativeTool(string(runtimeType), name)
}

func contextTokenRuntimeToolConstraints(authzCtx contextTokenTaskCreateAuthorizationContext) []string {
	runtimeTools := append([]string{}, authzCtx.RuntimeAllowedTools...)
	if authzCtx.Agent != nil && authzCtx.Agent.Spec.Runtime != nil {
		runtime := authzCtx.Agent.Spec.Runtime
		if runtime.RuntimeRef != nil || runtime.Type == corev1alpha1.AgentRuntimeOpencode {
			return runtimeTools
		}
	}
	if authzCtx.Request.Type == corev1alpha1.TaskTypeAgent && authzCtx.RuntimeAllowBash {
		runtimeTools = append(runtimeTools, "Bash")
	}
	return runtimeTools
}

func hasNonEmptyToolNames(tools []string) bool {
	return slices.ContainsFunc(tools, func(tool string) bool {
		return strings.TrimSpace(tool) != ""
	})
}

func contextTokenRuntimeToolsUnrestricted(tools []string) bool {
	return tools == nil || (len(tools) > 0 && !hasNonEmptyToolNames(tools))
}

func contextTokenTaskCreateNamespaceFailures(authzCtx contextTokenTaskCreateAuthorizationContext, tokenNamespace string) []string {
	failures := []string{}
	if authzCtx.Namespace != tokenNamespace {
		failures = append(failures, fmt.Sprintf("namespace %q does not match token context %q", authzCtx.Namespace, tokenNamespace))
	}
	failures = append(failures, contextTokenTaskDependencyNamespaceFailures(authzCtx, tokenNamespace)...)
	return failures
}

func contextTokenTaskDependencyNamespaceFailures(authzCtx contextTokenTaskCreateAuthorizationContext, tokenNamespace string) []string {
	failures := []string{}
	if authzCtx.AgentName != "" && authzCtx.AgentNamespace != "" && authzCtx.AgentNamespace != tokenNamespace {
		failures = append(failures, fmt.Sprintf("agent namespace %q does not match token context %q", authzCtx.AgentNamespace, tokenNamespace))
	}

	providerNamespaceInfo := authzCtx.EffectiveProvider
	if authzCtx.ProviderRef.Name != "" {
		providerNamespaceInfo = authzCtx.ProviderRef
	}
	if !providerNamespaceMatchesContext(providerNamespaceInfo, tokenNamespace, true) {
		failures = append(failures, fmt.Sprintf("provider namespace %q does not match token context %q", providerNamespaceInfo.Namespace, tokenNamespace))
	}

	return failures
}

func filterCompletionToolsForContextToken(c fiber.Ctx, cfg ContextTokenAuthorizationConfig, tools []llm.Tool) []llm.Tool {
	if !cfg.Enabled() || !cfg.enforcing() {
		return tools
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return tools
	}

	allowed, ok := contextStringList(ui.ContextToken.TransactionContext, "allowedTools")
	if !ok {
		return tools
	}
	return filterCompletionToolsByName(tools, allowed)
}

func filterCompletionToolsByName(tools []llm.Tool, allowed []string) []llm.Tool {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		name = strings.TrimSpace(name)
		if name != "" {
			allowedSet[name] = struct{}{}
		}
	}

	filtered := make([]llm.Tool, 0, len(tools))
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			continue
		}
		if _, ok := allowedSet[name]; ok {
			filtered = append(filtered, tool)
		}
	}
	return filtered
}

func completionToolNames(tools []llm.Tool) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		if strings.TrimSpace(tool.Name) != "" {
			names = append(names, tool.Name)
		}
	}
	return names
}

func completionToolNameSet(tools []llm.Tool) map[string]struct{} {
	names := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name != "" {
			names[name] = struct{}{}
		}
	}
	return names
}

func openAIContextTokenAuthorizationError(c fiber.Ctx, err error) error {
	if err == nil {
		return nil
	}
	if ferr, ok := err.(*fiber.Error); ok && ferr.Code == fiber.StatusForbidden {
		return c.Status(fiber.StatusForbidden).JSON(OAIError{Error: OAIErrorDetail{
			Message: ferr.Message,
			Type:    "permission_error",
		}})
	}
	return err
}

func anthropicContextTokenAuthorizationError(c fiber.Ctx, err error) error {
	if err == nil {
		return nil
	}
	if ferr, ok := err.(*fiber.Error); ok && ferr.Code == fiber.StatusForbidden {
		return anthropicError(c, fiber.StatusForbidden, "permission_error", ferr.Message)
	}
	return err
}

func agentAllowed(name, namespace string, allowed []string) bool {
	for _, want := range allowed {
		if agentMatches(name, namespace, want) {
			return true
		}
	}
	return false
}

func agentMatches(name, namespace, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" || strings.TrimSpace(name) == "" {
		return false
	}
	return name == want || namespacedNameString(namespace, name) == want
}

func namespacedNameString(namespace, name string) string {
	if namespace == "" {
		return name
	}
	if name == "" {
		return ""
	}
	return namespace + "/" + name
}

func providerDisplayName(provider ProviderResolutionInfo) string {
	if provider.Name != "" {
		return namespacedNameString(provider.Namespace, provider.Name)
	}
	return provider.Type
}

func providerAllowed(provider ProviderResolutionInfo, allowed []string, tokenNamespace string, hasTokenNamespace bool) bool {
	for _, want := range allowed {
		if providerMatches(provider, want, tokenNamespace, hasTokenNamespace) {
			return true
		}
	}
	return false
}

func providerMatches(provider ProviderResolutionInfo, want string, tokenNamespace string, hasTokenNamespace bool) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	if !providerNamespaceMatchesContext(provider, tokenNamespace, hasTokenNamespace) {
		return false
	}
	if provider.Name != "" && namespacedNameString(provider.Namespace, provider.Name) == want {
		return true
	}
	if provider.Name != "" && provider.Name == want {
		return true
	}
	return provider.Type != "" && provider.Type == want
}

func modelAllowed(provider ProviderResolutionInfo, model string, allowed []string, tokenNamespace string, hasTokenNamespace bool) bool {
	if !providerNamespaceMatchesContext(provider, tokenNamespace, hasTokenNamespace) {
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
		if provider.Name != "" && want == namespacedNameString(provider.Namespace, provider.Name)+"/"+model {
			return true
		}
		if provider.Type != "" && want == provider.Type+"/"+model {
			return true
		}
	}
	return false
}

func providerNamespaceMatchesContext(provider ProviderResolutionInfo, tokenNamespace string, hasTokenNamespace bool) bool {
	if !hasTokenNamespace {
		return true
	}
	providerNamespace := strings.TrimSpace(provider.Namespace)
	return providerNamespace == "" || providerNamespace == tokenNamespace
}

func toolNamesAllowed(tools []string, allowed []string) bool {
	for _, tool := range tools {
		if tool == "" {
			continue
		}
		if !slices.Contains(allowed, tool) {
			return false
		}
	}
	return true
}

func hasAnyScope(actual, required []string) bool {
	for _, scope := range actual {
		if slices.Contains(required, scope) {
			return true
		}
	}
	return false
}

func contextString(ctx any, name string) (string, bool) {
	value, ok := contextValue(ctx, name)
	if !ok {
		return "", false
	}
	s, ok := contextValueString(value)
	if !ok || strings.TrimSpace(s) == "" {
		return "", false
	}
	return s, true
}

func contextStringList(ctx any, name string) ([]string, bool) {
	value, ok := contextValue(ctx, name)
	if !ok {
		return nil, false
	}
	switch v := value.(type) {
	case []string:
		return append([]string{}, v...), true
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok || strings.TrimSpace(s) == "" {
				return nil, false
			}
			out = append(out, s)
		}
		return out, true
	case string:
		out := workerenv.SplitCSV(v)
		return out, len(out) > 0
	default:
		return contextValueStringSlice(value)
	}
}

func contextValue(ctx any, name string) (any, bool) {
	switch v := ctx.(type) {
	case map[string]any:
		value, ok := v[name]
		return value, ok
	case map[string]string:
		value, ok := v[name]
		return value, ok
	}

	rv := reflect.ValueOf(ctx)
	if !rv.IsValid() || rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
		return nil, false
	}

	key := reflect.ValueOf(name)
	if !key.Type().AssignableTo(rv.Type().Key()) {
		if !key.Type().ConvertibleTo(rv.Type().Key()) {
			return nil, false
		}
		key = key.Convert(rv.Type().Key())
	}

	value := rv.MapIndex(key)
	if !value.IsValid() {
		return nil, false
	}
	return value.Interface(), true
}

func contextValueString(value any) (string, bool) {
	if s, ok := value.(string); ok {
		return s, true
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || rv.Kind() != reflect.String {
		return "", false
	}
	return rv.String(), true
}

func contextValueStringSlice(value any) ([]string, bool) {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || rv.Kind() != reflect.Slice || rv.Type().Elem().Kind() != reflect.String {
		return nil, false
	}
	out := make([]string, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out = append(out, rv.Index(i).String())
	}
	return out, true
}

func taskRequestWorkspace(req CreateTaskRequest) *corev1alpha1.WorkspaceConfig {
	return req.Workspace
}

func workspaceGitRepo(workspace *corev1alpha1.WorkspaceConfig) string {
	if workspace == nil {
		return ""
	}
	return workspace.GitRepo
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
