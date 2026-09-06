/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

// Package workerenv defines the process contract shared by the controller job
// builder and worker binaries.
package workerenv

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

const (
	// Base worker env vars used by all worker types.
	TaskName          = "ORKA_TASK_NAME"
	TaskNamespace     = "ORKA_TASK_NAMESPACE"
	ResultEndpoint    = "ORKA_RESULT_ENDPOINT"
	ResultStdout      = "ORKA_RESULT_STDOUT"
	ResultStdoutToken = "ORKA_RESULT_STDOUT_TOKEN"
	ControllerURL     = "ORKA_CONTROLLER_URL"
	Command           = "ORKA_COMMAND"
	AgentName         = "ORKA_AGENT_NAME"

	// Task relationship env vars.
	PriorTask           = "ORKA_PRIOR_TASK"
	PriorTaskNamespace  = "ORKA_PRIOR_TASK_NAMESPACE"
	PriorTaskDiffSHA256 = "ORKA_PRIOR_TASK_DIFF_SHA256"
	ParentTask          = "ORKA_PARENT_TASK"

	// Transaction context env vars.
	TransactionID                              = "ORKA_TRANSACTION_ID"
	TransactionProfile                         = "ORKA_TRANSACTION_PROFILE"
	TransactionIssuer                          = "ORKA_TRANSACTION_ISSUER"
	TransactionSubject                         = "ORKA_TRANSACTION_SUBJECT"
	TransactionRequestingWorkload              = "ORKA_TRANSACTION_REQUESTING_WORKLOAD"
	TransactionScope                           = "ORKA_TRANSACTION_SCOPE"
	TransactionScopes                          = "ORKA_TRANSACTION_SCOPES"
	TransactionContextDigest                   = "ORKA_TRANSACTION_CONTEXT_DIGEST"
	TransactionRequesterContextDigest          = "ORKA_TRANSACTION_REQUESTER_CONTEXT_DIGEST"
	TransactionCredentialSecret                = "ORKA_TRANSACTION_CREDENTIAL_SECRET"
	TransactionCredentialReadScopes            = "ORKA_TRANSACTION_CREDENTIAL_READ_SCOPES"
	TransactionCredentialAuthorizationEnforced = "ORKA_TRANSACTION_CREDENTIAL_AUTHORIZATION_ENFORCED"
	TransactionTokenFile                       = "ORKA_TRANSACTION_TOKEN_FILE"
	ContextTokenTTSEndpoint                    = "ORKA_CONTEXT_TOKEN_TTS_ENDPOINT"
	ContextTokenTTSAudience                    = "ORKA_CONTEXT_TOKEN_TTS_AUDIENCE"
	ContextTokenTTSTimeout                     = "ORKA_CONTEXT_TOKEN_TTS_TIMEOUT"
	ContextTokenTTSTokenSource                 = "ORKA_CONTEXT_TOKEN_TTS_TOKEN_SOURCE"
	ContextTokenSubjectTokenFile               = "ORKA_CONTEXT_TOKEN_SUBJECT_TOKEN_FILE"
	ContextTokenSubjectTokenType               = "ORKA_CONTEXT_TOKEN_SUBJECT_TOKEN_TYPE"
	ContextTokenOutboundScope                  = "ORKA_CONTEXT_TOKEN_OUTBOUND_SCOPE"
	ContextTokenChildScope                     = "ORKA_CONTEXT_TOKEN_CHILD_SCOPE"
	ContextTokenChildTokenTTL                  = "ORKA_CONTEXT_TOKEN_CHILD_TOKEN_TTL"
	ContextTokenToolTokenTTL                   = "ORKA_CONTEXT_TOKEN_TOOL_TOKEN_TTL"
	OutboundAccessTrustedGatewayServices       = "ORKA_OUTBOUND_ACCESS_TRUSTED_GATEWAY_SERVICES"
	OutboundAccessTrustedTokenEndpointServices = "ORKA_OUTBOUND_ACCESS_TRUSTED_TOKEN_ENDPOINT_SERVICES"

	// AI worker env vars.
	AIProvider        = "ORKA_AI_PROVIDER"
	AIModel           = "ORKA_AI_MODEL"
	AIPrompt          = "ORKA_AI_PROMPT"
	AISystemPrompt    = "ORKA_AI_SYSTEM_PROMPT"
	AIBaseURL         = "ORKA_AI_BASE_URL"
	AIAzureAPIVersion = "ORKA_AI_AZURE_API_VERSION"
	AITools           = "ORKA_AI_TOOLS"
	AIFallbackCount   = "ORKA_AI_FALLBACK_COUNT"
	ControllerMode    = "ORKA_CONTROLLER_MODE"

	// Telemetry env vars.
	EnableTelemetry = "ORKA_ENABLE_TELEMETRY"
	TraceParent     = "ORKA_TRACEPARENT"
	TraceState      = "ORKA_TRACESTATE"
	TraceBaggage    = "ORKA_BAGGAGE"

	// Coordination/autonomous env vars used by AI worker and coordination tools.
	CoordinationEnabled       = "ORKA_COORDINATION_ENABLED"
	CoordinationMaxDepth      = "ORKA_COORDINATION_MAX_DEPTH"
	CoordinationMaxChildren   = "ORKA_COORDINATION_MAX_CHILDREN"
	CoordinationAllowedAgents = "ORKA_COORDINATION_ALLOWED_AGENTS"
	CoordinationDepth         = "ORKA_COORDINATION_DEPTH"
	AutonomousMode            = "ORKA_AUTONOMOUS_MODE"
	AutonomousIteration       = "ORKA_AUTONOMOUS_ITERATION"
	AutonomousMaxIterations   = "ORKA_AUTONOMOUS_MAX_ITERATIONS"
	TaskUID                   = "ORKA_TASK_UID"
	ApprovalRequiredTools     = "ORKA_APPROVAL_REQUIRED_TOOLS"
	ResolvedApprovals         = "ORKA_RESOLVED_APPROVALS"

	// Agent runtime env vars.
	Prompt                      = "ORKA_PROMPT"
	Model                       = "ORKA_MODEL"
	SystemPrompt                = "ORKA_SYSTEM_PROMPT"
	MaxTurns                    = "ORKA_MAX_TURNS"
	AgentReadOnly               = "ORKA_AGENT_READ_ONLY"
	AllowedTools                = "ORKA_ALLOWED_TOOLS"
	DisallowedTools             = "ORKA_DISALLOWED_TOOLS"
	AllowBash                   = "ORKA_ALLOW_BASH"
	TimeoutSeconds              = "ORKA_TIMEOUT_SECONDS"
	SessionName                 = "ORKA_SESSION_NAME"
	SessionPromptIncluded       = "ORKA_SESSION_PROMPT_INCLUDED"
	ClaudeBare                  = "ORKA_CLAUDE_BARE"
	ClaudeDisableSettingSources = "ORKA_CLAUDE_DISABLE_SETTING_SOURCES"
	ClaudePermissionMode        = "ORKA_CLAUDE_PERMISSION_MODE"
	CodexSandboxMode            = "ORKA_CODEX_SANDBOX_MODE"
	CodexAutoCompactTokenLimit  = "ORKA_CODEX_AUTO_COMPACT_TOKEN_LIMIT"
	CodexDisableSandbox         = "ORKA_CODEX_DISABLE_SANDBOX"
	AnthropicBaseURL            = "ANTHROPIC_BASE_URL"
	OpenAIBaseURL               = "OPENAI_BASE_URL"
	AnthropicAPIKey             = "ANTHROPIC_API_KEY"
	OpenAIAPIKey                = "OPENAI_API_KEY"
	CodexAPIKey                 = "CODEX_API_KEY"
	GitHubToken                 = "GITHUB_TOKEN"
	GitToken                    = "GIT_TOKEN"
	GitAskpass                  = "GIT_ASKPASS"
	GitUsername                 = "GIT_USERNAME"
	ClaudeCLIPath               = "CLAUDE_CLI_PATH"
	CodexCLIPath                = "CODEX_CLI_PATH"
	CopilotCLIPath              = "COPILOT_CLI_PATH"

	// Tool integration env vars.
	SearchAPIKey = "SEARCH_API_KEY"
	SearchAPIURL = "SEARCH_API_URL"

	// Kubernetes namespace env vars used by workers/tools.
	PodNamespace = "POD_NAMESPACE"
	Namespace    = "NAMESPACE"

	// Code execution tool env vars.
	CodeExecBackend                 = "ORKA_CODE_EXEC_BACKEND"
	CodeExecLocalCPUSeconds         = "ORKA_CODE_EXEC_LOCAL_CPU_SECONDS"
	CodeExecLocalMemoryKB           = "ORKA_CODE_EXEC_LOCAL_MEMORY_KB"
	CodeExecLocalMaxProcesses       = "ORKA_CODE_EXEC_LOCAL_MAX_PROCESSES"
	CodeExecKubernetesImage         = "ORKA_CODE_EXEC_K8S_IMAGE"
	CodeExecKubernetesPythonImage   = "ORKA_CODE_EXEC_K8S_PYTHON_IMAGE"
	CodeExecKubernetesNodeImage     = "ORKA_CODE_EXEC_K8S_NODE_IMAGE"
	CodeExecKubernetesBashImage     = "ORKA_CODE_EXEC_K8S_BASH_IMAGE"
	CodeExecKubernetesCPURequest    = "ORKA_CODE_EXEC_K8S_CPU_REQUEST"
	CodeExecKubernetesCPULimit      = "ORKA_CODE_EXEC_K8S_CPU_LIMIT"
	CodeExecKubernetesMemoryRequest = "ORKA_CODE_EXEC_K8S_MEMORY_REQUEST"
	CodeExecKubernetesMemoryLimit   = "ORKA_CODE_EXEC_K8S_MEMORY_LIMIT"
	CodeExecKubernetesNetworkPolicy = "ORKA_CODE_EXEC_K8S_NETWORK_POLICY"
	CodeExecKubernetesRuntimeClass  = "ORKA_CODE_EXEC_K8S_RUNTIME_CLASS_NAME"
	CodeExecKubernetesAppArmor      = "ORKA_CODE_EXEC_K8S_APPARMOR_PROFILE"
	OrkaNamespace                   = "ORKA_NAMESPACE"

	// Workspace env vars used by general and agent-runtime workers.
	GitRepo              = "ORKA_GIT_REPO"
	GitBranch            = "ORKA_GIT_BRANCH"
	GitRef               = "ORKA_GIT_REF"
	GitRefShallow        = "ORKA_GIT_REF_SHALLOW"
	WorkspaceSubpath     = "ORKA_WORKSPACE_SUBPATH"
	WorkspacePrepared    = "ORKA_WORKSPACE_PREPARED"
	ForkRepo             = "ORKA_FORK_REPO"
	PRBaseBranch         = "ORKA_PR_BASE_BRANCH"
	PRBaseRepo           = "ORKA_PR_BASE_REPO"
	PRBaseSHA            = "ORKA_PR_BASE_SHA"
	PushBranch           = "ORKA_PUSH_BRANCH"
	RequirePushBranch    = "ORKA_REQUIRE_PUSH_BRANCH"
	AllowEmptyPushBranch = "ORKA_ALLOW_EMPTY_PUSH_BRANCH"
	WorkDir              = "ORKA_WORK_DIR"
	SkillsDir            = "ORKA_SKILLS_DIR"

	// Agent sandbox workspace env vars used by agent-runtime workers.
	ExecutionWorkspaceEnabled               = "ORKA_EXECUTION_WORKSPACE_ENABLED"
	ExecutionWorkspaceProvider              = "ORKA_EXECUTION_WORKSPACE_PROVIDER"
	ExecutionWorkspaceTemplateName          = "ORKA_EXECUTION_WORKSPACE_TEMPLATE_NAME"
	ExecutionWorkspaceTemplateNamespace     = "ORKA_EXECUTION_WORKSPACE_TEMPLATE_NAMESPACE"
	ExecutionWorkspaceClaimNamespace        = "ORKA_EXECUTION_WORKSPACE_CLAIM_NAMESPACE"
	ExecutionWorkspaceClaimName             = "ORKA_EXECUTION_WORKSPACE_CLAIM_NAME"
	ExecutionWorkspaceReusePolicy           = "ORKA_EXECUTION_WORKSPACE_REUSE_POLICY"
	ExecutionWorkspaceReuseKey              = "ORKA_EXECUTION_WORKSPACE_REUSE_KEY"
	ExecutionWorkspaceCleanupPolicy         = "ORKA_EXECUTION_WORKSPACE_CLEANUP_POLICY"
	ExecutionWorkspaceBoot                  = "ORKA_EXECUTION_WORKSPACE_BOOT"
	ExecutionWorkspacePoolName              = "ORKA_EXECUTION_WORKSPACE_POOL_NAME"
	ExecutionWorkspacePoolNamespace         = "ORKA_EXECUTION_WORKSPACE_POOL_NAMESPACE"
	ExecutionWorkspaceSnapshotRestoreURI    = "ORKA_EXECUTION_WORKSPACE_SNAPSHOT_RESTORE_URI"
	ExecutionWorkspaceSnapshotCheckpointURI = "ORKA_EXECUTION_WORKSPACE_SNAPSHOT_CHECKPOINT_URI"
	ExecutionWorkspaceSnapshotOnRelease     = "ORKA_EXECUTION_WORKSPACE_SNAPSHOT_ON_RELEASE"
	ExecutionWorkspaceProcessMode           = "ORKA_EXECUTION_WORKSPACE_PROCESS_MODE"
	ExecutionWorkspaceResidentKey           = "ORKA_EXECUTION_WORKSPACE_RESIDENT_KEY"
	ExecutionWorkspaceClaimTimeoutSeconds   = "ORKA_EXECUTION_WORKSPACE_CLAIM_TIMEOUT_SECONDS"
	ExecutionWorkspaceCommandTimeoutSeconds = "ORKA_EXECUTION_WORKSPACE_COMMAND_TIMEOUT_SECONDS"
	ExecutionWorkspaceStatusEndpoint        = "ORKA_EXECUTION_WORKSPACE_STATUS_ENDPOINT"
	ExecutionWorkspaceDepth                 = "ORKA_EXECUTION_WORKSPACE_DEPTH"

	SubstrateAPIEndpoint             = "ORKA_SUBSTRATE_API_ENDPOINT"
	SubstrateAPICAFile               = "ORKA_SUBSTRATE_API_CA_FILE"
	SubstrateAPIInsecureSkipVerify   = "ORKA_SUBSTRATE_API_INSECURE_SKIP_VERIFY"
	SubstrateRouterURL               = "ORKA_SUBSTRATE_ROUTER_URL"
	SubstrateActorDNSSuffix          = "ORKA_SUBSTRATE_ACTOR_DNS_SUFFIX"
	SubstrateSessionIdentityToken    = "ORKA_SUBSTRATE_SESSION_IDENTITY_TOKEN"
	SubstrateSessionIdentityRequired = "ORKA_SUBSTRATE_SESSION_IDENTITY_REQUIRED"
	SubstrateSessionIdentityMintCert = "ORKA_SUBSTRATE_SESSION_IDENTITY_MINT_CERT"
	SubstrateSessionIdentityAudience = "ORKA_SUBSTRATE_SESSION_IDENTITY_AUDIENCE"
	SubstrateSessionIdentityAppID    = "ORKA_SUBSTRATE_SESSION_IDENTITY_APP_ID"
	SubstrateSessionIdentityUserID   = "ORKA_SUBSTRATE_SESSION_IDENTITY_USER_ID"
	WorkspaceBootstrapToken          = "ORKA_WORKSPACE_BOOTSTRAP_TOKEN"

	AgentSandboxEnabled               = "ORKA_AGENT_SANDBOX_ENABLED"
	AgentSandboxRouterURL             = "ORKA_AGENT_SANDBOX_ROUTER_URL"
	AgentSandboxTemplateName          = "ORKA_AGENT_SANDBOX_TEMPLATE_NAME"
	AgentSandboxTemplateNamespace     = "ORKA_AGENT_SANDBOX_TEMPLATE_NAMESPACE"
	AgentSandboxClaimNamespace        = "ORKA_AGENT_SANDBOX_CLAIM_NAMESPACE"
	AgentSandboxReusePolicy           = "ORKA_AGENT_SANDBOX_REUSE_POLICY"
	AgentSandboxReuseKey              = "ORKA_AGENT_SANDBOX_REUSE_KEY"
	AgentSandboxCleanupPolicy         = "ORKA_AGENT_SANDBOX_CLEANUP_POLICY"
	AgentSandboxWarmPoolPolicy        = "ORKA_AGENT_SANDBOX_WARM_POOL_POLICY"
	AgentSandboxNamespaceStrategy     = "ORKA_AGENT_SANDBOX_NAMESPACE_STRATEGY"
	AgentSandboxClaimTimeoutSeconds   = "ORKA_AGENT_SANDBOX_CLAIM_TIMEOUT_SECONDS"
	AgentSandboxCommandTimeoutSeconds = "ORKA_AGENT_SANDBOX_COMMAND_TIMEOUT_SECONDS"
	AgentSandboxDepth                 = "ORKA_AGENT_SANDBOX_DEPTH"

	// Git config env vars used to mark the prepared workspace as safe.
	GitConfigCount  = "GIT_CONFIG_COUNT"
	GitConfigKey0   = "GIT_CONFIG_KEY_0"
	GitConfigValue0 = "GIT_CONFIG_VALUE_0"

	// Memory/controller context env vars used by AI worker memory integration.
	MemoryContextEnabled    = "ORKA_MEMORY_CONTEXT_ENABLED"
	MemoryContextLimit      = "ORKA_MEMORY_CONTEXT_LIMIT"
	MemoryContextMaxChars   = "ORKA_MEMORY_CONTEXT_MAX_CHARS"
	ServiceAccountToken     = "ORKA_SA_TOKEN"
	ServiceAccountTokenPath = "ORKA_SA_TOKEN_PATH"

	ServiceAccountTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"

	ResultStdoutPrefix = "ORKA_RESULT_B64:"

	// RepositoryValidationUnavailableExitCode identifies a validation worker
	// that could not exec the configured validation command. The controller
	// treats this as unavailable infrastructure rather than a command failure.
	RepositoryValidationUnavailableExitCode = 125

	// RepositoryValidationMaxProcesses bounds processes and threads created by
	// one repository validation command.
	RepositoryValidationMaxProcesses = 512

	// RepositoryValidationMaxCommandBytes bounds the command selected by a
	// repository reviewer and materialized for the validation container.
	RepositoryValidationMaxCommandBytes = 8192
)

const trueString = "true"

// Env returns a simple Kubernetes environment variable.
func Env(name, value string) corev1.EnvVar {
	return corev1.EnvVar{Name: name, Value: value}
}

// AppendIfSet appends name=value to envVars only when value is non-empty.
func AppendIfSet(envVars []corev1.EnvVar, name, value string) []corev1.EnvVar {
	if value == "" {
		return envVars
	}
	return append(envVars, Env(name, value))
}

// IsTrue returns true when value enables a boolean env flag.
func IsTrue(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), trueString)
}

// SplitCSV returns trimmed, non-empty items from a comma-separated env value.
func SplitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// ReadTokenFile reads and trims a token file. It fails closed when the
// configured path cannot be read or contains only whitespace.
func ReadTokenFile(path, description string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("%s file path is empty", description)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read %s file: %w", description, err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("%s file %q is empty", description, path)
	}
	return token, nil
}

// ReadTokenFileEnv reads and trims a token file path referenced by envName.
// It returns ok=false when envName is unset, and fails closed when a configured
// path cannot be read or contains only whitespace.
func ReadTokenFileEnv(envName, description string) (string, bool, error) {
	path := strings.TrimSpace(os.Getenv(envName))
	if path == "" {
		return "", false, nil
	}
	token, err := ReadTokenFile(path, description)
	return token, true, err
}

// RequireTokenFileEnv reads a token file path referenced by envName and returns
// an error when the env var is unset.
func RequireTokenFileEnv(envName, description string) (string, error) {
	token, ok, err := ReadTokenFileEnv(envName, description)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("%s is required", envName)
	}
	return token, nil
}

// JoinCSV joins values using the comma-separated format used by worker env vars.
func JoinCSV(values []string) string {
	return strings.Join(values, ",")
}

// BaseEnv is the common env contract passed to all Orka worker containers.
type BaseEnv struct {
	TaskName           string
	TaskNamespace      string
	TaskUID            string
	ResultEndpoint     string
	ControllerURL      string
	AgentName          string
	TransactionID      string
	TransactionProfile string
}

// EnvVars renders the base worker environment.
func (e BaseEnv) EnvVars() []corev1.EnvVar {
	envVars := []corev1.EnvVar{
		Env(TaskName, e.TaskName),
		Env(TaskNamespace, e.TaskNamespace),
		Env(ResultEndpoint, e.ResultEndpoint),
		Env(ControllerURL, e.ControllerURL),
	}
	envVars = AppendIfSet(envVars, TaskUID, e.TaskUID)
	envVars = AppendIfSet(envVars, AgentName, e.AgentName)
	envVars = AppendIfSet(envVars, TransactionID, e.TransactionID)
	envVars = AppendIfSet(envVars, TransactionProfile, e.TransactionProfile)
	return envVars
}

// ParseBaseEnv reads the common worker environment.
func ParseBaseEnv(getenv func(string) string) BaseEnv {
	return BaseEnv{
		TaskName:           getenv(TaskName),
		TaskNamespace:      getenv(TaskNamespace),
		TaskUID:            getenv(TaskUID),
		ResultEndpoint:     getenv(ResultEndpoint),
		ControllerURL:      getenv(ControllerURL),
		AgentName:          getenv(AgentName),
		TransactionID:      getenv(TransactionID),
		TransactionProfile: getenv(TransactionProfile),
	}
}

// TransactionLogFields returns safe key/value fragments for worker stdout logs.
func TransactionLogFields(transactionID, profile string) string {
	fields := []string{}
	if transactionID != "" {
		fields = append(fields, "transactionID="+strconv.Quote(transactionID))
	}
	if profile != "" {
		fields = append(fields, "contextTokenProfile="+strconv.Quote(profile))
	}
	if len(fields) == 0 {
		return ""
	}
	return " " + strings.Join(fields, " ")
}

// FallbackProviderEnv is one fallback provider entry from the AI worker env contract.
type FallbackProviderEnv struct {
	Provider        string
	APIKey          string
	Model           string
	BaseURL         string
	AzureAPIVersion string
}

// FallbackPrefix returns the prefix for fallback provider i.
func FallbackPrefix(i int) string {
	return fmt.Sprintf("ORKA_AI_FALLBACK_%d", i)
}

func FallbackProviderKey(i int) string        { return FallbackPrefix(i) + "_PROVIDER" }
func FallbackAPIKeyKey(i int) string          { return FallbackPrefix(i) + "_API_KEY" }
func FallbackModelKey(i int) string           { return FallbackPrefix(i) + "_MODEL" }
func FallbackBaseURLKey(i int) string         { return FallbackPrefix(i) + "_BASE_URL" }
func FallbackAzureAPIVersionKey(i int) string { return FallbackPrefix(i) + "_AZURE_API_VERSION" }

// EnvVars renders the non-secret fallback metadata env vars for fallback i.
// API keys should normally be rendered from SecretKeyRef by the caller.
func (e FallbackProviderEnv) EnvVars(i int) []corev1.EnvVar {
	envVars := []corev1.EnvVar{
		Env(FallbackProviderKey(i), e.Provider),
		Env(FallbackModelKey(i), e.Model),
	}
	envVars = AppendIfSet(envVars, FallbackBaseURLKey(i), e.BaseURL)
	envVars = AppendIfSet(envVars, FallbackAzureAPIVersionKey(i), e.AzureAPIVersion)
	if e.APIKey != "" {
		envVars = append(envVars, Env(FallbackAPIKeyKey(i), e.APIKey))
	}
	return envVars
}

// ParseFallbackProviderEnv reads fallback provider i.
func ParseFallbackProviderEnv(getenv func(string) string, i int) FallbackProviderEnv {
	return FallbackProviderEnv{
		Provider:        getenv(FallbackProviderKey(i)),
		APIKey:          getenv(FallbackAPIKeyKey(i)),
		Model:           getenv(FallbackModelKey(i)),
		BaseURL:         getenv(FallbackBaseURLKey(i)),
		AzureAPIVersion: getenv(FallbackAzureAPIVersionKey(i)),
	}
}

// ParseFallbacks reads all fallback provider entries from env. Invalid counts are
// treated as no fallbacks to preserve historical worker behavior.
func ParseFallbacks(getenv func(string) string) []FallbackProviderEnv {
	countStr := strings.TrimSpace(getenv(AIFallbackCount))
	if countStr == "" {
		return nil
	}
	count, err := strconv.Atoi(countStr)
	if err != nil || count <= 0 {
		return nil
	}
	fallbacks := make([]FallbackProviderEnv, 0, count)
	for i := range count {
		fallbacks = append(fallbacks, ParseFallbackProviderEnv(getenv, i))
	}
	return fallbacks
}

// AIWorkerEnv is the typed AI worker env contract shared by JobBuilder and the
// AI worker binary.
type AIWorkerEnv struct {
	BaseEnv
	Provider                         string
	Model                            string
	Prompt                           string
	SystemPrompt                     string
	BaseURL                          string
	AzureAPIVersion                  string
	Tools                            []string
	Fallbacks                        []FallbackProviderEnv
	EnforceTransactionCredentialAuth bool
	TransactionCredentialReadScopes  []string
	EnableTelemetry                  bool
	TraceParent                      string
	TraceState                       string
	TraceBaggage                     string
	ControllerMode                   string
}

// EnvVars renders AI worker env vars. Fallback API keys are included only when
// set on the struct; JobBuilder should usually add them separately via secrets.
func (e AIWorkerEnv) EnvVars() []corev1.EnvVar {
	envVars := make([]corev1.EnvVar, 0, 8+len(e.Fallbacks)*4)
	if e.BaseEnv != (BaseEnv{}) {
		envVars = append(envVars, e.BaseEnv.EnvVars()...)
	}
	envVars = append(envVars,
		Env(AIProvider, e.Provider),
		Env(AIModel, e.Model),
		Env(AIPrompt, e.Prompt),
		Env(AISystemPrompt, e.SystemPrompt),
	)
	envVars = AppendIfSet(envVars, AIBaseURL, e.BaseURL)
	envVars = AppendIfSet(envVars, AIAzureAPIVersion, e.AzureAPIVersion)
	if len(e.Tools) > 0 {
		envVars = append(envVars, Env(AITools, JoinCSV(e.Tools)))
	}
	if e.EnforceTransactionCredentialAuth {
		envVars = append(envVars, Env(TransactionCredentialAuthorizationEnforced, "true"))
	}
	if len(e.TransactionCredentialReadScopes) > 0 {
		envVars = append(envVars, Env(TransactionCredentialReadScopes, JoinCSV(e.TransactionCredentialReadScopes)))
	}
	if e.EnableTelemetry {
		envVars = append(envVars, Env(EnableTelemetry, "true"))
	}
	envVars = AppendIfSet(envVars, TraceParent, e.TraceParent)
	envVars = AppendIfSet(envVars, TraceState, e.TraceState)
	envVars = AppendIfSet(envVars, TraceBaggage, e.TraceBaggage)
	envVars = AppendIfSet(envVars, ControllerMode, e.ControllerMode)
	if len(e.Fallbacks) > 0 {
		envVars = append(envVars, Env(AIFallbackCount, strconv.Itoa(len(e.Fallbacks))))
		for i, fallback := range e.Fallbacks {
			envVars = append(envVars, fallback.EnvVars(i)...)
		}
	}
	return envVars
}

// ParseAIWorkerEnv reads the AI worker environment.
func ParseAIWorkerEnv(getenv func(string) string) AIWorkerEnv {
	return AIWorkerEnv{
		BaseEnv:                          ParseBaseEnv(getenv),
		Provider:                         getenv(AIProvider),
		Model:                            getenv(AIModel),
		Prompt:                           getenv(AIPrompt),
		SystemPrompt:                     getenv(AISystemPrompt),
		BaseURL:                          getenv(AIBaseURL),
		AzureAPIVersion:                  getenv(AIAzureAPIVersion),
		Tools:                            SplitCSV(getenv(AITools)),
		Fallbacks:                        ParseFallbacks(getenv),
		EnforceTransactionCredentialAuth: IsTrue(getenv(TransactionCredentialAuthorizationEnforced)),
		TransactionCredentialReadScopes:  SplitCSV(getenv(TransactionCredentialReadScopes)),
		EnableTelemetry:                  IsTrue(getenv(EnableTelemetry)),
		TraceParent:                      getenv(TraceParent),
		TraceState:                       getenv(TraceState),
		TraceBaggage:                     getenv(TraceBaggage),
		ControllerMode:                   getenv(ControllerMode),
	}
}

// ValidateRequired returns an error when required AI worker fields are missing.
func (e AIWorkerEnv) ValidateRequired() error {
	if e.Provider == "" {
		return fmt.Errorf("%s is required", AIProvider)
	}
	if e.Model == "" {
		return fmt.Errorf("%s is required", AIModel)
	}
	if e.Prompt == "" {
		return fmt.Errorf("%s is required", AIPrompt)
	}
	return nil
}

// ExecutionWorkspaceEnv is the provider-neutral execution workspace env
// contract passed to agent-runtime workers.
type ExecutionWorkspaceEnv struct {
	Enabled               bool
	Provider              string
	TemplateName          string
	TemplateNamespace     string
	ClaimNamespace        string
	ClaimName             string
	ReusePolicy           string
	ReuseKey              string
	CleanupPolicy         string
	Boot                  bool
	PoolName              string
	PoolNamespace         string
	SnapshotRestoreURI    string
	SnapshotCheckpointURI string
	SnapshotOnRelease     bool
	ProcessMode           string
	ResidentKey           string
	ClaimTimeout          time.Duration
	CommandTimeout        time.Duration
	StatusEndpoint        string
	Depth                 int
}

// EnvVars renders the generic execution workspace environment.
func (e ExecutionWorkspaceEnv) EnvVars() []corev1.EnvVar {
	if !e.Enabled {
		return nil
	}

	return []corev1.EnvVar{
		Env(ExecutionWorkspaceEnabled, strconv.FormatBool(e.Enabled)),
		Env(ExecutionWorkspaceProvider, e.Provider),
		Env(ExecutionWorkspaceTemplateName, e.TemplateName),
		Env(ExecutionWorkspaceTemplateNamespace, e.TemplateNamespace),
		Env(ExecutionWorkspaceClaimNamespace, e.ClaimNamespace),
		Env(ExecutionWorkspaceClaimName, e.ClaimName),
		Env(ExecutionWorkspaceReusePolicy, e.ReusePolicy),
		Env(ExecutionWorkspaceReuseKey, e.ReuseKey),
		Env(ExecutionWorkspaceCleanupPolicy, e.CleanupPolicy),
		Env(ExecutionWorkspaceBoot, strconv.FormatBool(e.Boot)),
		Env(ExecutionWorkspacePoolName, e.PoolName),
		Env(ExecutionWorkspacePoolNamespace, e.PoolNamespace),
		Env(ExecutionWorkspaceSnapshotRestoreURI, e.SnapshotRestoreURI),
		Env(ExecutionWorkspaceSnapshotCheckpointURI, e.SnapshotCheckpointURI),
		Env(ExecutionWorkspaceSnapshotOnRelease, strconv.FormatBool(e.SnapshotOnRelease)),
		Env(ExecutionWorkspaceProcessMode, e.ProcessMode),
		Env(ExecutionWorkspaceResidentKey, e.ResidentKey),
		Env(ExecutionWorkspaceClaimTimeoutSeconds, strconv.FormatInt(int64(e.ClaimTimeout/time.Second), 10)),
		Env(ExecutionWorkspaceCommandTimeoutSeconds, strconv.FormatInt(int64(e.CommandTimeout/time.Second), 10)),
		Env(ExecutionWorkspaceStatusEndpoint, e.StatusEndpoint),
		Env(ExecutionWorkspaceDepth, strconv.Itoa(e.Depth)),
	}
}

// SubstrateEnv is the Substrate-specific worker env contract.
type SubstrateEnv struct {
	APIEndpoint             string
	APICAFile               string
	APIInsecureSkipVerify   bool
	RouterURL               string
	ActorDNSSuffix          string
	SessionIdentityToken    string
	SessionIdentityRequired bool
	SessionIdentityMintCert bool
	SessionIdentityAudience string
	SessionIdentityAppID    string
	SessionIdentityUserID   string
}

// EnvVars renders Substrate-specific worker env vars.
func (e SubstrateEnv) EnvVars() []corev1.EnvVar {
	envVars := []corev1.EnvVar{
		Env(SubstrateAPIEndpoint, e.APIEndpoint),
		Env(SubstrateAPICAFile, e.APICAFile),
		Env(SubstrateAPIInsecureSkipVerify, strconv.FormatBool(e.APIInsecureSkipVerify)),
		Env(SubstrateRouterURL, e.RouterURL),
		Env(SubstrateActorDNSSuffix, e.ActorDNSSuffix),
		Env(SubstrateSessionIdentityRequired, strconv.FormatBool(e.SessionIdentityRequired)),
		Env(SubstrateSessionIdentityMintCert, strconv.FormatBool(e.SessionIdentityMintCert)),
		Env(SubstrateSessionIdentityAudience, e.SessionIdentityAudience),
		Env(SubstrateSessionIdentityAppID, e.SessionIdentityAppID),
		Env(SubstrateSessionIdentityUserID, e.SessionIdentityUserID),
	}
	if strings.TrimSpace(e.SessionIdentityToken) != "" {
		envVars = append(envVars, Env(SubstrateSessionIdentityToken, e.SessionIdentityToken))
	}
	return envVars
}

// AgentSandboxEnv is the resolved sandbox workspace env contract passed to
// agent-runtime workers.
type AgentSandboxEnv struct {
	Enabled           bool
	RouterURL         string
	TemplateName      string
	TemplateNamespace string
	ClaimNamespace    string
	ReusePolicy       string
	ReuseKey          string
	CleanupPolicy     string
	WarmPoolPolicy    string
	NamespaceStrategy string
	ClaimTimeout      time.Duration
	CommandTimeout    time.Duration
}

// EnvVars renders the agent sandbox workspace environment.
func (e AgentSandboxEnv) EnvVars() []corev1.EnvVar {
	if !e.Enabled {
		return nil
	}

	return []corev1.EnvVar{
		Env(AgentSandboxEnabled, strconv.FormatBool(e.Enabled)),
		Env(AgentSandboxRouterURL, e.RouterURL),
		Env(AgentSandboxTemplateName, e.TemplateName),
		Env(AgentSandboxTemplateNamespace, e.TemplateNamespace),
		Env(AgentSandboxClaimNamespace, e.ClaimNamespace),
		Env(AgentSandboxReusePolicy, e.ReusePolicy),
		Env(AgentSandboxReuseKey, e.ReuseKey),
		Env(AgentSandboxCleanupPolicy, e.CleanupPolicy),
		Env(AgentSandboxWarmPoolPolicy, e.WarmPoolPolicy),
		Env(AgentSandboxNamespaceStrategy, e.NamespaceStrategy),
		Env(AgentSandboxClaimTimeoutSeconds, strconv.FormatInt(int64(e.ClaimTimeout/time.Second), 10)),
		Env(AgentSandboxCommandTimeoutSeconds, strconv.FormatInt(int64(e.CommandTimeout/time.Second), 10)),
		Env(AgentSandboxDepth, "0"),
	}
}

// CoordinationEnv is the coordination/autonomous env contract used by AI tasks.
type CoordinationEnv struct {
	Enabled       bool
	MaxDepth      int
	MaxChildren   int
	AllowedAgents []string
	Depth         string

	AutonomousMode          bool
	AutonomousIteration     int
	AutonomousMaxIterations int
	ApprovalRequiredTools   []string
}

// EnvVars renders coordination/autonomous env vars.
func (e CoordinationEnv) EnvVars() []corev1.EnvVar {
	if !e.Enabled {
		return nil
	}
	envVars := []corev1.EnvVar{
		Env(CoordinationEnabled, trueString),
		Env(CoordinationMaxDepth, strconv.Itoa(e.MaxDepth)),
		Env(CoordinationMaxChildren, strconv.Itoa(e.MaxChildren)),
	}
	if len(e.AllowedAgents) > 0 {
		envVars = append(envVars, Env(CoordinationAllowedAgents, JoinCSV(e.AllowedAgents)))
	}
	if e.Depth != "" {
		envVars = append(envVars, Env(CoordinationDepth, e.Depth))
	}
	if e.AutonomousMode {
		envVars = append(envVars,
			Env(AutonomousMode, trueString),
			Env(AutonomousIteration, strconv.Itoa(e.AutonomousIteration)),
		)
		if e.AutonomousMaxIterations > 0 {
			envVars = append(envVars, Env(AutonomousMaxIterations, strconv.Itoa(e.AutonomousMaxIterations)))
		}
	}
	if len(e.ApprovalRequiredTools) > 0 {
		envVars = append(envVars, Env(ApprovalRequiredTools, JoinCSV(e.ApprovalRequiredTools)))
	}
	return envVars
}

// ParseCoordinationEnv reads coordination/autonomous settings.
func ParseCoordinationEnv(getenv func(string) string) CoordinationEnv {
	return CoordinationEnv{
		Enabled:                 IsTrue(getenv(CoordinationEnabled)),
		MaxDepth:                parsePositiveInt(getenv(CoordinationMaxDepth)),
		MaxChildren:             parsePositiveInt(getenv(CoordinationMaxChildren)),
		AllowedAgents:           SplitCSV(getenv(CoordinationAllowedAgents)),
		Depth:                   getenv(CoordinationDepth),
		AutonomousMode:          IsTrue(getenv(AutonomousMode)),
		AutonomousIteration:     parsePositiveInt(getenv(AutonomousIteration)),
		AutonomousMaxIterations: parsePositiveInt(getenv(AutonomousMaxIterations)),
		ApprovalRequiredTools:   SplitCSV(getenv(ApprovalRequiredTools)),
	}
}

func parsePositiveInt(value string) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
}
