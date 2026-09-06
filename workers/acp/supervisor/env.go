package supervisor

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/orka-agents/orka/internal/acp"
	"github.com/orka-agents/orka/internal/artifactcap"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const (
	providerKindCodex            = "codex"
	providerKindClaude           = "claude"
	providerKindCopilot          = "copilot"
	providerKindOpencode         = "opencode"
	providerToolBash             = "Bash"
	providerToolEdit             = "Edit"
	providerToolGlob             = "Glob"
	providerToolGrep             = "Grep"
	providerToolRead             = "Read"
	providerToolWebFetch         = "WebFetch"
	providerToolWebSearch        = "WebSearch"
	providerToolWrite            = "Write"
	architectureARM64            = "arm64"
	EnvListenAddress             = "ORKA_ACP_LISTEN_ADDRESS"
	EnvRuntimeInstanceID         = "ORKA_ACP_RUNTIME_INSTANCE_ID"
	EnvSupervisorBootID          = "ORKA_ACP_SUPERVISOR_BOOT_ID"
	EnvPodUID                    = "ORKA_ACP_POD_UID"
	EnvControllerEpoch           = "ORKA_ACP_CONTROLLER_EPOCH"
	EnvRuntimePoolUID            = "ORKA_ACP_RUNTIME_POOL_UID"
	EnvRuntimePoolGeneration     = "ORKA_ACP_RUNTIME_POOL_GENERATION"
	EnvProvider                  = "ORKA_ACP_PROVIDER"
	EnvModel                     = "ORKA_ACP_MODEL"
	EnvModelContextLimit         = "ORKA_ACP_MODEL_CONTEXT_LIMIT"
	EnvModelOutputLimit          = "ORKA_ACP_MODEL_OUTPUT_LIMIT"
	EnvWorkspaceIntent           = "ORKA_ACP_WORKSPACE_INTENT"
	EnvAgentConfigurationDigest  = "ORKA_ACP_AGENT_CONFIGURATION_DIGEST"
	EnvToolPolicyDigest          = "ORKA_ACP_TOOL_POLICY_DIGEST"
	EnvApprovalPolicyDigest      = "ORKA_ACP_APPROVAL_POLICY_DIGEST"
	EnvMCPConfigurationDigest    = "ORKA_ACP_MCP_CONFIGURATION_DIGEST"
	EnvProxyCredentialRole       = "ORKA_ACP_PROXY_CREDENTIAL_ROLE"
	EnvProxyCredentialScope      = "ORKA_ACP_PROXY_CREDENTIAL_SCOPE"
	EnvResourceClass             = "ORKA_ACP_RESOURCE_CLASS"
	EnvProviderProxyBaseURL      = "ORKA_ACP_PROVIDER_PROXY_BASE_URL"
	EnvProviderTokenFile         = "ORKA_ACP_PROVIDER_TOKEN_FILE"
	EnvArtifactAPIURL            = "ORKA_ACP_ARTIFACT_API_URL"
	EnvWorkspaceArtifactMaxBytes = "ORKA_ACP_WORKSPACE_MAX_ARTIFACT_BYTES"
	EnvMCPBrokerURL              = "ORKA_ACP_MCP_BROKER_URL"
	EnvTrustNamespace            = "ORKA_ACP_TRUST_NAMESPACE"
	EnvControllerTokenFile       = "ORKA_ACP_CONTROLLER_TOKEN_FILE"
	EnvCapabilitySecretFile      = "ORKA_ACP_CAPABILITY_SECRET_FILE"
	// The *_BOOTSTRAP variables carry read-once secrets for provider-hosted
	// supervisors (for example Substrate Actors) that have no Kubernetes
	// Secret mounts. The corresponding *_FILE variable always wins when set.
	EnvControllerTokenBootstrap  = "ORKA_ACP_CONTROLLER_TOKEN_BOOTSTRAP"
	EnvCapabilitySecretBootstrap = "ORKA_ACP_CAPABILITY_SECRET_BOOTSTRAP"
	EnvProviderTokenBootstrap    = "ORKA_ACP_PROVIDER_TOKEN_BOOTSTRAP"
	EnvSessionBaseDir            = "ORKA_ACP_SESSION_BASE_DIR"
	EnvDurableWorkspaceDir       = "ORKA_ACP_DURABLE_WORKSPACE_DIR"
	EnvFirstSessionUID           = "ORKA_ACP_FIRST_SESSION_UID"
	EnvLastSessionUID            = "ORKA_ACP_LAST_SESSION_UID"
	EnvSessionGID                = "ORKA_ACP_SESSION_GID"
	EnvE2EPromptWriteAmbiguity   = "ORKA_ACP_E2E_PROMPT_WRITE_AMBIGUITY_MARKER"

	openCodeProviderID          = "orka"
	openCodeProviderEnvName     = "ORKA_OPENCODE_PROVIDER_TOKEN"
	openCodePermissionAllow     = "allow"
	openCodePermissionDeny      = "deny"
	openCodeRootInstructionPath = "/opt/opencode/AGENTS.md"

	// Built-in ACP runtimes flush buffered token/tool updates in short bursts
	// far above the generic protocol default (a live OpenCode research prompt
	// exceeded 100/s while streaming tool output, breaking the stream at its
	// terminal event). Keep one bounded runtime ceiling that the supervisor
	// advertises and the controller then enforces symmetrically.
	runtimeMaxUpdateEventsPerSecond = 1000
	// supervisorMaxBufferedPromptEvents bounds the per-prompt ACP event buffer
	// between the child adapter and the controller-facing prompt stream. The
	// controller journals every event before reading the next one, so under
	// concurrent prompts a tool-output burst (Codex streams file contents as
	// many small update events) can outrun it for a few seconds; an overflow
	// cancels an otherwise healthy prompt, so the buffer must absorb such
	// bursts. Six concurrent Codex prompts overflowed 4096 events at 2.5 MiB,
	// so the count is the protocol maximum and the byte cap below is the
	// effective memory bound (events are small; the line limit bounds each).
	supervisorMaxBufferedPromptEvents = 16_384
	// supervisorMaxBufferedPromptEventBytes bounds the aggregate raw payload of
	// buffered, unconsumed events per prompt: the count limit alone would let a
	// burst of line-limit-sized events reserve gigabytes before overflowing.
	supervisorMaxBufferedPromptEventBytes = 32 << 20

	// Publisher workspace artifacts are inbound runtime materialization inputs,
	// while workspace deltas are outbound runtime artifacts. Keep independently
	// bounded defaults and configure the inbound limit from Publisher capability
	// propagation instead of conflating it with the delta capability.
	defaultWorkspaceArtifactDownloadBytes int64 = artifactcap.DefaultWorkspaceArtifactMaxBytes
	defaultWorkspaceDeltaUploadBytes      int64 = 100 << 20
)

func LoadConfigFromEnv() (Config, error) {
	providerKind := requiredEnv(EnvProvider)
	model := requiredEnv(EnvModel)
	intent := harnessv2.WorkspaceIntent(requiredEnv(EnvWorkspaceIntent))
	modelLimits, err := modelTokenLimitsFromEnv()
	if err != nil {
		return Config{}, err
	}
	profile := harnessv2.RuntimeProfile{
		ACPProfile:               harnessv2.ACPProfileV1,
		ProviderKind:             providerKind,
		Model:                    model,
		ModelLimits:              modelLimits,
		AgentConfigurationDigest: requiredEnv(EnvAgentConfigurationDigest),
		ToolPolicyDigest:         requiredEnv(EnvToolPolicyDigest),
		ApprovalPolicyDigest:     requiredEnv(EnvApprovalPolicyDigest),
		MCPConfigurationDigest:   requiredEnv(EnvMCPConfigurationDigest),
		WorkspaceIntent:          intent,
		ProxyCredentialRole:      requiredEnv(EnvProxyCredentialRole),
		ProxyCredentialScope:     requiredEnv(EnvProxyCredentialScope),
		ResourceClass:            envDefault(EnvResourceClass, "standard"),
	}
	providerBaseURL := envDefault(EnvProviderProxyBaseURL, defaultProxyBaseURL())
	modelOutputLimit := int64(0)
	if modelLimits != nil {
		modelOutputLimit = modelLimits.Output
	}
	provider, err := providerProfile(providerKind, model, intent, modelLimits)
	if err != nil {
		return Config{}, err
	}
	profile.AdapterDigests = providerAdapterDigests(providerKind)
	if err := profile.Validate(); err != nil {
		return Config{}, fmt.Errorf("runtime profile: %w", err)
	}
	profileDigest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		return Config{}, err
	}
	limits := defaultProtocolLimits(providerKind)
	controllerEpoch, err := parsePositiveUint(EnvControllerEpoch, requiredEnv(EnvControllerEpoch))
	if err != nil {
		return Config{}, err
	}
	poolGeneration, err := parsePositiveUint(EnvRuntimePoolGeneration, requiredEnv(EnvRuntimePoolGeneration))
	if err != nil {
		return Config{}, err
	}
	firstUID, err := parsePositiveInt(EnvFirstSessionUID, envDefault(EnvFirstSessionUID, "20000"))
	if err != nil {
		return Config{}, err
	}
	lastUID, err := parsePositiveInt(EnvLastSessionUID, envDefault(EnvLastSessionUID, "29999"))
	if err != nil {
		return Config{}, err
	}
	firstGID, err := parsePositiveInt(EnvSessionGID, envDefault(EnvSessionGID, "20000"))
	if err != nil {
		return Config{}, err
	}
	allocator, err := acp.NewUIDAllocator(firstUID, lastUID, firstGID)
	if err != nil {
		return Config{}, err
	}
	controllerToken, err := readRequiredSecret(EnvControllerTokenFile, EnvControllerTokenBootstrap)
	if err != nil {
		return Config{}, err
	}
	capabilitySecret, err := readRequiredSecret(EnvCapabilitySecretFile, EnvCapabilitySecretBootstrap)
	if err != nil {
		return Config{}, err
	}
	workspaceMaterializer := EmptyWorkspaceMaterializer()
	var artifactUploader ArtifactUploader
	artifactAPIURL := strings.TrimSpace(os.Getenv(EnvArtifactAPIURL))
	if artifactAPIURL != "" {
		authorizationProvider, providerErr := NewBrokerArtifactAuthorizationProvider(
			artifactAPIURL, requiredEnv(EnvTrustNamespace), controllerToken, []byte(capabilitySecret),
		)
		if providerErr != nil {
			return Config{}, providerErr
		}
		maxWorkspaceArtifactBytes, limitErr := workspaceArtifactDownloadLimitFromEnv()
		if limitErr != nil {
			return Config{}, limitErr
		}
		artifactClient, clientErr := newArtifactClient(
			artifactAPIURL, nil, authorizationProvider, artifactClientLimits{
				MaxDownloadBytes: maxWorkspaceArtifactBytes,
				MaxUploadBytes:   limits.MaxWorkspaceDeltaBytes,
			},
		)
		if clientErr != nil {
			return Config{}, clientErr
		}
		workspaceMaterializer, clientErr = NewRemoteWorkspaceMaterializer(artifactClient, WorkspaceMaterializerLimits{})
		if clientErr != nil {
			return Config{}, clientErr
		}
		artifactUploader, clientErr = NewRemoteArtifactUploader(artifactClient)
		if clientErr != nil {
			return Config{}, clientErr
		}
	}
	mcpBrokerURL := strings.TrimSpace(os.Getenv(EnvMCPBrokerURL))
	if mcpBrokerURL == "" {
		mcpBrokerURL = strings.TrimSpace(os.Getenv(EnvArtifactAPIURL))
	}
	mcpBroker, err := NewControllerMCPBrokerClient(
		mcpBrokerURL, requiredEnv(EnvTrustNamespace), controllerToken, []byte(capabilitySecret),
	)
	if err != nil {
		return Config{}, err
	}
	durableWorkspaceDir := strings.TrimSpace(os.Getenv(EnvDurableWorkspaceDir))
	e2ePromptWriteAmbiguityMarker := strings.TrimSpace(os.Getenv(EnvE2EPromptWriteAmbiguity))
	var e2ePromptWriteFaultRecorder E2EPromptWriteFaultRecorder
	if e2ePromptWriteAmbiguityMarker != "" && durableWorkspaceDir == "" {
		e2ePromptWriteFaultRecorder, err = NewControllerE2EPromptWriteFaultRecorder(
			mcpBrokerURL, requiredEnv(EnvTrustNamespace), controllerToken, []byte(capabilitySecret),
		)
		if err != nil {
			return Config{}, err
		}
	}
	providerToken, err := readRequiredSecret(EnvProviderTokenFile, EnvProviderTokenBootstrap)
	if err != nil {
		return Config{}, err
	}
	bootID := strings.TrimSpace(os.Getenv(EnvSupervisorBootID))
	if bootID == "" {
		bootID = uuid.NewString()
	}
	runtimeInstanceID := strings.TrimSpace(os.Getenv(EnvRuntimeInstanceID))
	if runtimeInstanceID == "" {
		podUID := requiredEnv(EnvPodUID)
		runtimeInstanceID = podUID + "." + bootID
	}
	capabilities := harnessv2.CapabilitiesResponse{
		Protocol: harnessv2.ProtocolVersion, Transport: "http+ndjson", ACPVersion: harnessv2.ACPProfileV1,
		RuntimeProfileDigest: profileDigest, ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		AdapterDigests: profile.AdapterDigests, Limits: limits, SupportsDrain: true, SupportsPublicationFinalization: true,
		SupportsAgentSessionConfiguration: true,
		Provider: harnessv2.ProviderCapabilities{
			ProviderKinds: []string{providerKind}, Models: []string{model}, SupportsPermissions: true,
			SupportsCancel: true, SupportsTools: true, SupportsImages: true,
			SupportsEmbeddedResources: true,
		},
		WorkspaceGovernance: harnessv2.StrictWorkspaceGovernanceCapabilities(),
	}
	cfg := Config{
		ListenAddress: envDefault(EnvListenAddress, ":8080"),
		Fence: harnessv2.Fence{
			RuntimeInstanceID: harnessv2.RuntimeInstanceID(runtimeInstanceID),
			SupervisorBootID:  harnessv2.SupervisorBootID(bootID), ControllerEpoch: controllerEpoch,
			RuntimePoolUID: harnessv2.RuntimePoolUID(requiredEnv(EnvRuntimePoolUID)), RuntimePoolGeneration: poolGeneration,
			RuntimeProfileDigest: profileDigest, ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		},
		Capabilities: capabilities, Provider: provider,
		ControllerBearerToken: controllerToken, CapabilitySecret: []byte(capabilitySecret), RequireCapabilities: true,
		SessionBaseDir:      envDefault(EnvSessionBaseDir, "/sessions"),
		DurableWorkspaceDir: durableWorkspaceDir,
		UIDAllocator:        allocator,
		ProviderProxy: ProviderProxyConfig{
			UpstreamBaseURL: providerUpstreamBaseURL(providerKind, providerBaseURL), UpstreamBearerToken: providerToken,
			ProviderKind: providerKind, Model: model,
		},
		MCPBroker:                     mcpBroker,
		WorkspaceMaterializer:         workspaceMaterializer,
		ArtifactUploader:              artifactUploader,
		E2EPromptWriteFaultRecorder:   e2ePromptWriteFaultRecorder,
		E2EPromptWriteAmbiguityMarker: e2ePromptWriteAmbiguityMarker,
	}
	cfg.ProviderProxy.ModelOutputLimit = modelOutputLimit
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// providerAdapterDigests keeps the supervisor's default-nil unknown-provider
// behavior while sourcing the shared built-in adapter digest table.
func providerAdapterDigests(provider string) map[string]string {
	return acp.BuiltInRuntimeAdapterDigests(provider)
}

// codexAgentModeOrkaExternal is the Orka-patched codex-acp agent mode whose
// externalSandbox policy keeps the runtime Pod as the enforcement boundary.
const codexAgentModeOrkaExternal = "orka-external"

var providerNativeToolNames = []string{
	providerToolBash, providerToolEdit, providerToolGlob, providerToolGrep,
	providerToolRead, providerToolWebFetch, providerToolWebSearch, providerToolWrite,
}

type providerNativePolicy struct {
	unrestricted bool
	allowed      map[string]struct{}
}

func (p providerNativePolicy) allows(name string) bool {
	_, ok := p.allowed[name]
	return ok
}

func providerSessionPolicy(
	request harnessv2.CreateRuntimeSessionRequest,
	provider string,
	model string,
) (providerNativePolicy, error) {
	if request.AgentConfiguration == nil {
		return providerNativePolicy{}, fmt.Errorf("agent session configuration is required")
	}
	if err := request.MCPConfiguration.ValidateProfile(request.Profile); err != nil {
		return providerNativePolicy{}, fmt.Errorf("MCP policy configuration: %w", err)
	}
	if err := request.AgentConfiguration.ValidateProfileOrLegacy(request.Profile, request.MCPConfiguration.ToolPolicy.AllowBash); err != nil {
		return providerNativePolicy{}, fmt.Errorf("agent session configuration: %w", err)
	}
	if request.Profile.ProviderKind != provider || request.AgentConfiguration.ProviderKind != provider ||
		request.Profile.Model != model || request.AgentConfiguration.Model != model {
		return providerNativePolicy{}, fmt.Errorf("provider session configuration does not match runtime profile")
	}
	toolPolicy := request.MCPConfiguration.ToolPolicy
	unrestricted := toolPolicy.AllowedToolNames == nil && len(toolPolicy.DisallowedToolNames) == 0 && toolPolicy.AllowBash
	policy := providerNativePolicy{unrestricted: unrestricted, allowed: make(map[string]struct{}, len(providerNativeToolNames))}
	for _, descriptor := range toolPolicy.Tools {
		if descriptor.Source != harnessv2.MCPToolSourceProviderNative {
			continue
		}
		name, ok := canonicalProviderNativeToolName(descriptor.Name)
		if !ok {
			return providerNativePolicy{}, fmt.Errorf("provider-native tool %q is not supported by the %s projection", descriptor.Name, provider)
		}
		if _, duplicate := policy.allowed[name]; duplicate {
			return providerNativePolicy{}, fmt.Errorf("provider-native tool %q is duplicated after canonicalization", descriptor.Name)
		}
		policy.allowed[name] = struct{}{}
	}
	return policy, nil
}

func canonicalProviderNativeToolName(value string) (string, bool) {
	for _, name := range providerNativeToolNames {
		if strings.EqualFold(value, name) {
			return name, true
		}
	}
	return "", false
}

func providerNativePolicyLists(policy providerNativePolicy) ([]string, []string) {
	allowed := make([]string, 0, len(providerNativeToolNames))
	disallowed := make([]string, 0, len(providerNativeToolNames))
	for _, name := range providerNativeToolNames {
		if policy.allows(name) {
			allowed = append(allowed, name)
		} else {
			disallowed = append(disallowed, name)
		}
	}
	return allowed, disallowed
}

func codexSessionProjection(
	request harnessv2.CreateRuntimeSessionRequest,
	_ acp.SessionPaths,
	proxy ProviderProxyBinding,
	model string,
) (ProviderSessionProjection, error) {
	policy, err := providerSessionPolicy(request, providerKindCodex, model)
	if err != nil {
		return ProviderSessionProjection{}, err
	}
	if !policy.unrestricted && !codexReadOnlySessionPolicy(request, policy) {
		return ProviderSessionProjection{}, fmt.Errorf("codex ACP runtime cannot exactly enforce provider-native tool restrictions")
	}
	config := codexBaseConfig(model, proxy.BaseURL)
	if systemPrompt := request.AgentConfiguration.SystemPrompt; systemPrompt != "" {
		config["developer_instructions"] = systemPrompt
	}
	if effort := request.AgentConfiguration.ReasoningEffort; effort != "" {
		config["model_reasoning_effort"] = effort
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return ProviderSessionProjection{}, err
	}
	const maxCodexConfigEnvironmentBytes = 96 << 10
	if len(encoded) > maxCodexConfigEnvironmentBytes {
		return ProviderSessionProjection{}, fmt.Errorf("codex session configuration exceeds the safe environment limit")
	}
	// Read-only sessions keep the default orka-external agent mode: Codex's
	// own sandbox needs unprivileged user namespaces that the runtime Pod
	// forbids, so the RuntimeSession boundary enforces the surface instead —
	// safe read commands execute, every elevation request is rejected
	// unconditionally by the controller, file writes are mediated by the
	// supervisor, and the read-intent workspace delta classification fails
	// any turn that modifies the workspace.
	return ProviderSessionProjection{Environment: map[string]string{"CODEX_CONFIG": string(encoded)}}, nil
}

// codexBaseConfig is the Codex configuration every session starts from. The
// provider proxy only admits the immutable HTTP profile, so the Responses
// WebSocket transports are disabled up front: otherwise Codex attempts the
// upgrade, receives 403 from the proxy, and prepends "Warning: Falling back
// from WebSockets to HTTPS transport" to the agent's first message, which then
// leaks into Task results.
func codexBaseConfig(model, baseURL string) map[string]any {
	return map[string]any{
		"model": model, "model_provider": codexProviderID, "check_for_update_on_startup": false,
		"model_providers": map[string]any{
			codexProviderID: codexProviderDefinition(baseURL),
		},
	}
}

// codexProviderID names the Codex model provider that points at the session
// provider proxy. Codex's built-in "openai" provider (selected implicitly by
// openai_base_url) always opens a Responses WebSocket first; the proxy admits
// only the immutable HTTP profile, so that attempt failed with 403 and Codex
// prepended "Warning: Falling back from WebSockets to HTTPS transport" to the
// first agent message. A custom provider with wire_api=responses uses plain
// HTTPS from the start (verified against codex-cli 0.145.0).
const codexProviderID = "orka"

func codexProviderDefinition(baseURL string) map[string]any {
	return map[string]any{
		codexProviderNameKey: "Orka provider proxy",
		"base_url":           baseURL,
		"wire_api":           "responses",
		"env_key":            "CODEX_API_KEY",
	}
}

// codexProviderNameKey is the Codex provider display-name key.
const codexProviderNameKey = "name"

// codexReadOnlySessionPolicy reports whether the session's restricted tool
// policy is exactly the read-intent {Glob, Grep, Read} surface, which is the
// only restricted shape the codex read-only agent mode enforces.
func codexReadOnlySessionPolicy(request harnessv2.CreateRuntimeSessionRequest, policy providerNativePolicy) bool {
	if request.Profile.WorkspaceIntent != harnessv2.WorkspaceIntentRead {
		return false
	}
	if len(policy.allowed) != 3 {
		return false
	}
	return policy.allows(providerToolGlob) && policy.allows(providerToolGrep) && policy.allows(providerToolRead)
}

func claudeSessionProjection(
	request harnessv2.CreateRuntimeSessionRequest,
	_ acp.SessionPaths,
	_ ProviderProxyBinding,
	model string,
) (ProviderSessionProjection, error) {
	policy, err := providerSessionPolicy(request, providerKindClaude, model)
	if err != nil {
		return ProviderSessionProjection{}, err
	}
	options := map[string]any{"maxTurns": request.AgentConfiguration.MaxTurns}
	if effort := request.AgentConfiguration.ReasoningEffort; effort != "" {
		options["effort"] = effort
	}
	if !policy.unrestricted {
		allowed, disallowed := providerNativePolicyLists(policy)
		options["tools"] = allowed
		options["disallowedTools"] = disallowed
	}
	meta := acp.Meta{"claudeCode": map[string]any{"options": options}}
	if systemPrompt := request.AgentConfiguration.SystemPrompt; systemPrompt != "" {
		meta["systemPrompt"] = systemPrompt
	}
	return ProviderSessionProjection{NewSessionMeta: meta}, nil
}

var copilotToolIDs = map[string][]string{
	providerToolBash:      {"bash", "list_bash", "read_bash", "stop_bash", "write_bash"},
	providerToolEdit:      {"edit", "str_replace_editor", "apply_patch"},
	providerToolGlob:      {"glob"},
	providerToolGrep:      {"grep", "rg"},
	providerToolRead:      {"view"},
	providerToolWebFetch:  {"web_fetch"},
	providerToolWebSearch: {"web_search"},
	providerToolWrite:     {"create"},
}

// copilotAlwaysExcludedToolIDs are the built-in Copilot CLI tools every Orka
// session removes from the model's catalog regardless of policy: sub-agent
// orchestration, skills, SQL, and background task tools have no Orka
// equivalent and would bypass the RuntimeSession boundary. The list is exactly
// the set GitHub Copilot CLI 1.0.77 registers under the supervisor's flags
// (verified in --acp mode): the CLI prints an "Unknown tool name" diagnostic
// into the agent message stream for every --excluded-tools name outside its
// catalog, and names such as ask_user, report_intent, exit_plan_mode, lsp, or
// code_review are not registered tool names in that build at all. GitHub MCP
// tools register as github-mcp-server-<tool> and are removed by
// --disable-builtin-mcps; the ask_user tool is removed by --no-ask-user.
var copilotAlwaysExcludedToolIDs = []string{"list_agents", "read_agent", "skill", "sql", "task", "write_agent"}

func copilotSessionProjection(
	request harnessv2.CreateRuntimeSessionRequest,
	_ acp.SessionPaths,
	_ ProviderProxyBinding,
	model string,
) (ProviderSessionProjection, error) {
	policy, err := providerSessionPolicy(request, providerKindCopilot, model)
	if err != nil {
		return ProviderSessionProjection{}, err
	}
	if request.AgentConfiguration.SystemPrompt != "" {
		return ProviderSessionProjection{}, fmt.Errorf("copilot ACP runtime cannot exactly enforce Agent systemPrompt")
	}
	if request.AgentConfiguration.ReasoningEffort != "" {
		return ProviderSessionProjection{}, fmt.Errorf("copilot ACP runtime cannot enforce reasoning effort")
	}
	excluded := append([]string(nil), copilotAlwaysExcludedToolIDs...)
	if !policy.unrestricted {
		if policy.allows(providerToolWebSearch) {
			return ProviderSessionProjection{}, fmt.Errorf("copilot ACP runtime cannot exactly enforce the WebSearch provider-native tool")
		}
		for _, name := range providerNativeToolNames {
			if policy.allows(name) {
				continue
			}
			excluded = append(excluded, copilotToolIDs[name]...)
		}
	}
	// The CLI reports the exclusion list back as "Info:" agent message chunks
	// at prompt start; the filter withholds exactly those chunks.
	return ProviderSessionProjection{
		AdditionalArgs:        []string{"--excluded-tools=" + strings.Join(excluded, ",")},
		AgentDiagnosticFilter: &AgentDiagnosticFilter{Startup: copilotStartupDiagnostic(excluded)},
	}, nil
}

func openCodeSessionProjection(
	request harnessv2.CreateRuntimeSessionRequest,
	_ acp.SessionPaths,
	_ ProviderProxyBinding,
	model string,
) (ProviderSessionProjection, error) {
	if request.AgentConfiguration == nil {
		return ProviderSessionProjection{}, fmt.Errorf("agent session configuration is required")
	}
	if err := request.MCPConfiguration.ValidateProfile(request.Profile); err != nil {
		return ProviderSessionProjection{}, fmt.Errorf("MCP policy configuration: %w", err)
	}
	if err := request.AgentConfiguration.ValidateProfileOrLegacy(
		request.Profile,
		request.MCPConfiguration.ToolPolicy.AllowBash,
	); err != nil {
		return ProviderSessionProjection{}, fmt.Errorf("agent session configuration: %w", err)
	}
	if request.Profile.ProviderKind != providerKindOpencode ||
		request.AgentConfiguration.ProviderKind != providerKindOpencode ||
		request.Profile.Model != model || request.AgentConfiguration.Model != model {
		return ProviderSessionProjection{}, fmt.Errorf("provider session configuration does not match runtime profile")
	}
	if request.AgentConfiguration.SystemPrompt != "" {
		return ProviderSessionProjection{}, fmt.Errorf("opencode ACP runtime cannot exactly enforce Agent systemPrompt")
	}
	if request.AgentConfiguration.ReasoningEffort != "" {
		return ProviderSessionProjection{}, fmt.Errorf("opencode ACP runtime cannot enforce reasoning effort")
	}
	for _, descriptor := range request.MCPConfiguration.ToolPolicy.Tools {
		if descriptor.Source != harnessv2.MCPToolSourceProviderNative {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(descriptor.Name)) {
		case "apply_patch", "bash", "edit", "glob", "grep", "read", "write":
		default:
			return ProviderSessionProjection{}, fmt.Errorf(
				"provider-native tool %q is not supported by the opencode projection",
				descriptor.Name,
			)
		}
	}
	return ProviderSessionProjection{}, nil
}

func providerProfile(
	kind, model string, intent harnessv2.WorkspaceIntent,
	modelLimitOptions ...*harnessv2.ModelTokenLimits,
) (ProviderProfile, error) {
	var modelLimits *harnessv2.ModelTokenLimits
	if len(modelLimitOptions) > 0 {
		modelLimits = modelLimitOptions[0]
	}
	switch kind {
	case providerKindCodex:
		return ProviderProfile{
			Kind: kind, Model: model, Command: "/usr/bin/node", Args: []string{"/opt/codex-acp/dist/index.js"},
			AuthMethodID: "api-key", AdapterName: "codex-acp-orka-dist", AdapterDigest: "sha256:" + acp.CodexACPOrkaDistSHA256,
			ProjectSession: func(request harnessv2.CreateRuntimeSessionRequest, paths acp.SessionPaths, proxy ProviderProxyBinding) (ProviderSessionProjection, error) {
				return codexSessionProjection(request, paths, proxy, model)
			},
			EnvironmentForSession: func(_ harnessv2.CreateRuntimeSessionRequest, paths acp.SessionPaths, proxy ProviderProxyBinding) (map[string]string, error) {
				// The Orka-patched adapter uses Codex's externalSandbox policy so the
				// restricted Runtime Pod remains the enforcement boundary without asking
				// the child to create nested Linux namespaces. Network remains restricted
				// and on-request approvals remain active for explicit elevation requests.
				mode := codexAgentModeOrkaExternal
				config, err := json.Marshal(codexBaseConfig(model, proxy.BaseURL))
				if err != nil {
					return nil, err
				}
				return map[string]string{
					"NO_BROWSER": "1", "CODEX_PATH": "/opt/codex/bin/codex", "CODEX_HOME": filepath.Join(paths.Home, ".codex"),
					"CODEX_CONFIG": string(config), "INITIAL_AGENT_MODE": mode, "CODEX_API_KEY": proxy.Credential,
				}, nil
			},
			PrepareSession: prepareCodexHome,
		}, nil
	case providerKindClaude:
		return ProviderProfile{
			Kind: kind, Model: model, Command: "/usr/bin/node", Args: []string{"/opt/claude-agent-acp/dist/index.js", "--hide-claude-auth"},
			AdapterName: "claude-agent-acp", AdapterDigest: "sha256:" + acp.ClaudeACPTarSHA256,
			ProjectSession: func(request harnessv2.CreateRuntimeSessionRequest, paths acp.SessionPaths, proxy ProviderProxyBinding) (ProviderSessionProjection, error) {
				return claudeSessionProjection(request, paths, proxy, model)
			},
			EnvironmentForSession: func(_ harnessv2.CreateRuntimeSessionRequest, paths acp.SessionPaths, proxy ProviderProxyBinding) (map[string]string, error) {
				return map[string]string{
					"CLAUDE_CONFIG_DIR": filepath.Join(paths.Home, ".claude"), "CLAUDE_CODE_EXECUTABLE": "/opt/claude/bin/claude",
					"NO_BROWSER": "1", "DISABLE_UPDATES": "1", "DISABLE_AUTOUPDATER": "1", "DISABLE_INSTALLATION_CHECKS": "1",
					"ANTHROPIC_BASE_URL": proxy.BaseURL, "ANTHROPIC_API_KEY": proxy.Credential, "ANTHROPIC_MODEL": model,
				}, nil
			},
		}, nil
	case providerKindCopilot:
		adapterName, adapterDigest, err := copilotAdapterIdentity(runtime.GOARCH)
		if err != nil {
			return ProviderProfile{}, err
		}
		return ProviderProfile{
			Kind: kind, Model: model, Command: "/opt/copilot/bin/copilot",
			Args:        []string{"--acp", "--stdio", "--no-auto-update", "--disable-builtin-mcps", "--no-custom-instructions", "--no-experimental", "--no-remote", "--no-remote-export", "--no-ask-user", "--no-bash-env", "--disallow-temp-dir", "--no-color", "--log-level", "none"},
			AdapterName: adapterName, AdapterDigest: adapterDigest,
			ProjectSession: func(request harnessv2.CreateRuntimeSessionRequest, paths acp.SessionPaths, proxy ProviderProxyBinding) (ProviderSessionProjection, error) {
				return copilotSessionProjection(request, paths, proxy, model)
			},
			EnvironmentForSession: func(_ harnessv2.CreateRuntimeSessionRequest, paths acp.SessionPaths, proxy ProviderProxyBinding) (map[string]string, error) {
				return map[string]string{
					"COPILOT_AUTO_UPDATE": "false", "CI": "true", "COPILOT_HOME": filepath.Join(paths.Home, ".copilot"),
					"COPILOT_PROVIDER_BASE_URL": proxy.BaseURL, "COPILOT_PROVIDER_BEARER_TOKEN": proxy.Credential, "COPILOT_PROVIDER_TYPE": "openai",
					"COPILOT_PROVIDER_WIRE_API": "responses", "COPILOT_MODEL": model,
				}, nil
			},
		}, nil
	case providerKindOpencode:
		if modelLimits == nil {
			return ProviderProfile{}, fmt.Errorf("OpenCode model token limits are required")
		}
		if err := modelLimits.Validate(); err != nil {
			return ProviderProfile{}, fmt.Errorf("OpenCode model token limits: %w", err)
		}
		if strings.ContainsAny(model, "{}") {
			return ProviderProfile{}, fmt.Errorf("OpenCode model must not contain substitution braces")
		}
		providerID, modelID, valid := strings.Cut(strings.TrimSpace(model), "/")
		if !valid || strings.TrimSpace(providerID) == "" || strings.TrimSpace(modelID) == "" {
			return ProviderProfile{}, fmt.Errorf("OpenCode model must use provider/model form")
		}
		return ProviderProfile{
			Kind: kind, Model: model, Command: "/opt/opencode/bin/opencode",
			Args:        []string{"--pure", "acp", "--hostname", "127.0.0.1", "--port", "0", "--no-mdns"},
			AdapterName: openCodeAdapterName(), AdapterDigest: openCodeAdapterDigest(),
			ProjectSession: func(request harnessv2.CreateRuntimeSessionRequest, paths acp.SessionPaths, proxy ProviderProxyBinding) (ProviderSessionProjection, error) {
				return openCodeSessionProjection(request, paths, proxy, model)
			},
			EnvironmentForSession: func(request harnessv2.CreateRuntimeSessionRequest, paths acp.SessionPaths, proxy ProviderProxyBinding) (map[string]string, error) {
				// The credential below is not an upstream provider secret. It is a random,
				// per-session loopback capability accepted only while the prompt lease is active
				// and only for the immutable profile model. OpenCode needs it during provider
				// construction; write-intent shells share the child environment by design, as
				// with the other built-in runtimes, while read-intent sessions disable bash.
				config, err := openCodeSessionConfig(model, modelLimits, intent, request, paths, proxy)
				if err != nil {
					return nil, err
				}
				return map[string]string{
					"CI":                                        "true",
					"NO_BROWSER":                                "1",
					"OPENCODE_AUTH_CONTENT":                     "{}",
					"OPENCODE_CONFIG_CONTENT":                   string(config),
					"OPENCODE_CONFIG_DIR":                       filepath.Join(paths.Config, "opencode"),
					"OPENCODE_DISABLE_AUTOUPDATE":               "1",
					"OPENCODE_DISABLE_CLAUDE_CODE":              "1",
					"OPENCODE_DISABLE_DEFAULT_PLUGINS":          "1",
					"OPENCODE_DISABLE_EMBEDDED_WEB_UI":          "1",
					"OPENCODE_DISABLE_EXTERNAL_SKILLS":          "1",
					"OPENCODE_DISABLE_LSP_DOWNLOAD":             "1",
					"OPENCODE_DISABLE_MODELS_FETCH":             "1",
					"OPENCODE_DISABLE_PROJECT_CONFIG":           "1",
					"OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER": "1",
					"OPENCODE_PURE":                             "1",
					"OPENCODE_SERVER_PASSWORD":                  openCodeServerPassword(proxy.Credential),
					"OPENCODE_SERVER_USERNAME":                  "orka",
					openCodeProviderEnvName:                     proxy.Credential,
				}, nil
			},
			PrepareSession: prepareOpenCodeConfig,
		}, nil
	default:
		return ProviderProfile{}, fmt.Errorf("unsupported ACP provider %q", kind)
	}
}

func prepareOpenCodeConfig(paths acp.SessionPaths) error {
	configDir := filepath.Join(paths.Config, "opencode")
	if err := os.MkdirAll(filepath.Join(configDir, "node_modules"), 0o700); err != nil {
		return err
	}
	files := map[string][]byte{
		"package.json": fmt.Appendf(nil,
			"{\n  \"private\": true,\n  \"dependencies\": {\n    \"@opencode-ai/plugin\": %q\n  }\n}\n",
			acp.OpenCodeVersion,
		),
		"package-lock.json": fmt.Appendf(nil,
			"{\n  \"name\": \"orka-opencode-runtime\",\n  \"lockfileVersion\": 3,\n  \"requires\": true,\n  \"packages\": {\n    \"\": {\n      \"dependencies\": {\n        \"@opencode-ai/plugin\": %q\n      }\n    }\n  }\n}\n",
			acp.OpenCodeVersion,
		),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(configDir, name), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func openCodeSessionConfig(
	model string,
	modelLimits *harnessv2.ModelTokenLimits,
	intent harnessv2.WorkspaceIntent,
	request harnessv2.CreateRuntimeSessionRequest,
	paths acp.SessionPaths,
	proxy ProviderProxyBinding,
) ([]byte, error) {
	if modelLimits == nil {
		return nil, fmt.Errorf("OpenCode model token limits are required")
	}
	if err := modelLimits.Validate(); err != nil {
		return nil, fmt.Errorf("OpenCode model token limits: %w", err)
	}
	providerID, modelID, ok := strings.Cut(strings.TrimSpace(model), "/")
	if !ok || providerID == "" || modelID == "" {
		return nil, fmt.Errorf("OpenCode model must use provider/model form")
	}
	model = providerID + "/" + modelID
	toolPolicy := request.MCPConfiguration.ToolPolicy
	permissions := map[string]any{
		"*":                  openCodePermissionDeny,
		"doom_loop":          openCodePermissionDeny,
		"external_directory": openCodePermissionDeny,
		"list":               openCodePermissionDeny,
		"lsp":                openCodePermissionDeny,
		"question":           openCodePermissionDeny,
		"skill":              openCodePermissionDeny,
		"task":               openCodePermissionDeny,
		"todowrite":          openCodePermissionDeny,
		"webfetch":           openCodePermissionDeny,
		"websearch":          openCodePermissionDeny,
	}
	for _, permission := range []string{"bash", "glob", "grep", "read"} {
		if openCodeToolPolicyAllows(toolPolicy, permission) {
			permissions[permission] = openCodePermissionAllow
		} else {
			permissions[permission] = openCodePermissionDeny
		}
	}
	mutationAction := openCodePermissionDeny
	if openCodeMutationPolicyAllows(toolPolicy) {
		mutationAction = openCodePermissionAllow
	}
	for _, permission := range []string{"apply_patch", "edit", "write"} {
		permissions[permission] = mutationAction
	}
	brokeredPermissions, err := openCodeBrokeredPermissions(toolPolicy)
	if err != nil {
		return nil, err
	}
	for permission, allowed := range brokeredPermissions {
		if allowed {
			permissions[permission] = openCodePermissionAllow
		} else {
			permissions[permission] = openCodePermissionDeny
		}
	}
	if permissions["read"] == openCodePermissionAllow {
		permissions["read"] = map[string]string{
			"*":             openCodePermissionAllow,
			"*.env":         openCodePermissionDeny,
			"*.env.*":       openCodePermissionDeny,
			"*.env.example": openCodePermissionAllow,
		}
	}
	if intent == harnessv2.WorkspaceIntentRead {
		permissions["apply_patch"] = openCodePermissionDeny
		permissions["bash"] = openCodePermissionDeny
		permissions["edit"] = openCodePermissionDeny
		permissions["grep"] = openCodePermissionDeny
		permissions["write"] = openCodePermissionDeny
	}
	return json.Marshal(map[string]any{
		"$schema":           "https://opencode.ai/config.json",
		"autoupdate":        false,
		"enabled_providers": []string{openCodeProviderID},
		"formatter":         false,
		"instructions":      []string{openCodeRootInstructionPath, filepath.Join(paths.Workspace, "AGENTS.md")},
		"lsp":               false,
		"mcp":               map[string]any{},
		"model":             openCodeProviderID + "/" + model,
		"permission":        permissions,
		"plugin":            []string{},
		"share":             "disabled",
		"small_model":       openCodeProviderID + "/" + model,
		"snapshot":          false,
		"subagent_depth":    0,
		"provider": map[string]any{
			openCodeProviderID: map[string]any{
				"env":       []string{},
				"name":      "Orka session proxy",
				"npm":       "@ai-sdk/openai-compatible",
				"whitelist": []string{model},
				"models": map[string]any{
					model: map[string]any{
						"limit": map[string]int64{
							"context": modelLimits.Context,
							"output":  modelLimits.Output,
						},
					},
				},
				"options": map[string]any{
					"apiKey":  "{env:" + openCodeProviderEnvName + "}",
					"baseURL": proxy.BaseURL,
				},
			},
		},
	})
}

func openCodeBrokeredPermissions(policy harnessv2.MCPToolPolicy) (map[string]bool, error) {
	permissions := make(map[string]bool)
	owners := make(map[string]string)
	for _, descriptor := range policy.Tools {
		if !descriptor.Source.Brokered() {
			continue
		}
		// OpenCode prefixes tools from the controller MCP server with `orka_`,
		// keeping brokered permissions disjoint from unprefixed native keys.
		name := "orka_" + strings.Map(func(value rune) rune {
			if value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' ||
				value >= '0' && value <= '9' || value == '_' || value == '-' {
				return value
			}
			return '_'
		}, descriptor.Name)
		if owner, exists := owners[name]; exists && owner != descriptor.Name {
			return nil, fmt.Errorf("brokered tools %q and %q collide in OpenCode permission name %q", owner, descriptor.Name, name)
		}
		owners[name] = descriptor.Name
		permissions[name] = openCodeToolPolicyAllows(policy, descriptor.Name)
	}
	return permissions, nil
}

// openCodeMutationPolicyAllows consumes the controller-normalized mutation
// group. Any denied alias closes the entire shared OpenCode edit permission.
func openCodeMutationPolicyAllows(policy harnessv2.MCPToolPolicy) bool {
	mutationPermissions := []string{"apply_patch", "edit", "write"}
	for _, denied := range policy.DisallowedToolNames {
		for _, permission := range mutationPermissions {
			if strings.EqualFold(denied, permission) {
				return false
			}
		}
	}
	for _, permission := range mutationPermissions {
		if openCodeToolPolicyAllows(policy, permission) {
			return true
		}
	}
	return false
}

func openCodeToolPolicyAllows(policy harnessv2.MCPToolPolicy, permission string) bool {
	for _, denied := range policy.DisallowedToolNames {
		if strings.EqualFold(denied, permission) {
			return false
		}
	}
	for _, allowed := range policy.AllowedToolNames {
		if strings.EqualFold(allowed, permission) && policy.Allows(allowed) {
			return true
		}
	}
	return false
}

func openCodeServerPassword(credential string) string {
	digest := sha256.Sum256([]byte("orka-opencode-server\x00" + credential))
	return fmt.Sprintf("%x", digest[:])
}

func openCodeAdapterName() string {
	if runtime.GOARCH == architectureARM64 {
		return "opencode-cli-linux-arm64"
	}
	return "opencode-cli-linux-amd64"
}

func openCodeAdapterDigest() string {
	if runtime.GOARCH == architectureARM64 {
		return "sha256:" + acp.OpenCodeLinuxARM64BinarySHA256
	}
	return "sha256:" + acp.OpenCodeLinuxX64BinarySHA256
}

func prepareCodexHome(paths acp.SessionPaths) error {
	dir := filepath.Join(paths.Home, ".codex")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "config.toml"), []byte(codexHomeConfigTOML), 0o600)
}

// codexHomeConfigTOML is the static CODEX_HOME/config.toml; the per-session
// provider definition travels in CODEX_CONFIG (see codexBaseConfig).
const codexHomeConfigTOML = "check_for_update_on_startup = false\n"

func openAIProxyURL(base string) string {
	base = strings.TrimSuffix(strings.TrimSpace(base), "/")
	if strings.HasSuffix(base, "/v1") {
		return base
	}
	return base + "/v1"
}

func defaultProxyBaseURL() string {
	return "http://vekil.vekil-system.svc:1337"
}

func providerUpstreamBaseURL(provider, base string) string {
	base = strings.TrimSuffix(strings.TrimSpace(base), "/")
	if provider == providerKindCodex || provider == providerKindCopilot || provider == providerKindOpencode {
		return openAIProxyURL(base)
	}
	return base
}

func copilotAdapterIdentity(goarch string) (string, string, error) {
	switch goarch {
	case "amd64":
		return "copilot-cli-linux-amd64", "sha256:" + acp.CopilotCLILinuxX64SHA256, nil
	case "arm64":
		return "copilot-cli-linux-arm64", "sha256:" + acp.CopilotCLILinuxARM64SHA256, nil
	default:
		return "", "", fmt.Errorf("unsupported Copilot runtime architecture %q", goarch)
	}
}

func defaultProtocolLimits(provider string) harnessv2.ProtocolLimits {
	maxUpdates := runtimeMaxUpdateEventsPerSecond
	return harnessv2.ProtocolLimits{
		MaxResidentSessions: 10, MaxConcurrentPrompts: 4, MaxRequestBytes: 2 << 20,
		MaxEventLineBytes: 1 << 20, MaxTerminalResultBytes: 1 << 20, MaxBufferedEvents: supervisorMaxBufferedPromptEvents,
		MaxUpdateEventsPerSecond: maxUpdates, MinPromptLeaseMillis: 5_000, MaxPromptLeaseMillis: 120_000,
		MaxPendingPermissions: 32, MaxWorkspaceDeltaBytes: defaultWorkspaceDeltaUploadBytes,
	}
}

func workspaceArtifactDownloadLimitFromEnv() (int64, error) {
	raw := strings.TrimSpace(os.Getenv(EnvWorkspaceArtifactMaxBytes))
	if raw == "" {
		return 0, fmt.Errorf("%s is required when the artifact API is configured", EnvWorkspaceArtifactMaxBytes)
	}
	limit, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || limit <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", EnvWorkspaceArtifactMaxBytes)
	}
	return limit, nil
}

func requiredEnv(name string) string { return strings.TrimSpace(os.Getenv(name)) }
func envDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func modelTokenLimitsFromEnv() (*harnessv2.ModelTokenLimits, error) {
	contextValue := strings.TrimSpace(os.Getenv(EnvModelContextLimit))
	outputValue := strings.TrimSpace(os.Getenv(EnvModelOutputLimit))
	if contextValue == "" && outputValue == "" {
		return nil, nil
	}
	if contextValue == "" || outputValue == "" {
		return nil, fmt.Errorf("%s and %s must be set together", EnvModelContextLimit, EnvModelOutputLimit)
	}
	contextLimit, err := strconv.ParseInt(contextValue, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s must be an integer", EnvModelContextLimit)
	}
	outputLimit, err := strconv.ParseInt(outputValue, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s must be an integer", EnvModelOutputLimit)
	}
	limits := &harnessv2.ModelTokenLimits{Context: contextLimit, Output: outputLimit}
	if err := limits.Validate(); err != nil {
		return nil, fmt.Errorf("model token limits: %w", err)
	}
	return limits, nil
}

func parsePositiveUint(name, value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func parsePositiveInt(name, value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

// readRequiredSecret prefers the mounted secret file and falls back to the
// read-once bootstrap environment variable when no file is named. The bootstrap
// variable is unset immediately so later Go-level environment reads and any
// explicitly inherited child environments never see it; like the workspace
// agent's bootstrap token, the original exec-time environment block remains
// visible only to same-UID processes inside the same isolation boundary.
func readRequiredSecret(fileEnvName, bootstrapEnvName string) (string, error) {
	value := strings.TrimSpace(os.Getenv(bootstrapEnvName))
	_ = os.Unsetenv(bootstrapEnvName)
	if strings.TrimSpace(os.Getenv(fileEnvName)) != "" {
		return readRequiredSecretFile(fileEnvName)
	}
	if value == "" {
		return "", fmt.Errorf("%s must name an absolute file, or %s must carry the read-once bootstrap secret", fileEnvName, bootstrapEnvName)
	}
	return value, nil
}

func readRequiredSecretFile(envName string) (string, error) {
	path := requiredEnv(envName)
	if path == "" || !filepath.IsAbs(path) {
		return "", fmt.Errorf("%s must name an absolute file", envName)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", envName, err)
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", fmt.Errorf("%s file is empty", envName)
	}
	return value, nil
}
