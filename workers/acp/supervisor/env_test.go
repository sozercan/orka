package supervisor

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

func TestProviderProfilesDisableUpdatesAndUsePrivateHomes(t *testing.T) {
	paths := acp.SessionPaths{Home: "/sessions/private/home"}
	request := harnessv2.CreateRuntimeSessionRequest{}
	proxy := ProviderProxyBinding{BaseURL: "http://127.0.0.1:43210/_orka/provider/session", Credential: "local-session-credential"}

	codex, err := providerProfile(providerKindCodex, "gpt-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	codexEnv, err := codex.EnvironmentForSession(request, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	if codexEnv["NO_BROWSER"] != "1" || codexEnv["CODEX_HOME"] != "/sessions/private/home/.codex" || !strings.Contains(codexEnv["CODEX_CONFIG"], proxy.BaseURL) || codexEnv["CODEX_API_KEY"] != proxy.Credential {
		t.Fatalf("unexpected Codex environment: %#v", codexEnv)
	}
	var codexConfig map[string]any
	if err := json.Unmarshal([]byte(codexEnv["CODEX_CONFIG"]), &codexConfig); err != nil {
		t.Fatal(err)
	}
	if codexConfig["model"] != "gpt-test" {
		t.Fatalf("Codex model config = %#v, want gpt-test", codexConfig["model"])
	}
	assertCodexWebSocketTransportsDisabled(t, codexConfig)
	if strings.Contains(strings.Join(codex.Args, " "), "npx") {
		t.Fatalf("Codex runtime uses a download-on-start command: %v", codex.Args)
	}

	claude, err := providerProfile("claude", "claude-test", harnessv2.WorkspaceIntentWrite)
	if err != nil {
		t.Fatal(err)
	}
	claudeEnv, err := claude.EnvironmentForSession(request, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"DISABLE_UPDATES", "DISABLE_AUTOUPDATER", "DISABLE_INSTALLATION_CHECKS"} {
		if claudeEnv[name] != "1" {
			t.Fatalf("%s = %q", name, claudeEnv[name])
		}
	}
	if claudeEnv["CLAUDE_CONFIG_DIR"] != "/sessions/private/home/.claude" {
		t.Fatalf("unexpected Claude config dir: %q", claudeEnv["CLAUDE_CONFIG_DIR"])
	}
	if claudeEnv["ANTHROPIC_BASE_URL"] != proxy.BaseURL || claudeEnv["ANTHROPIC_API_KEY"] != proxy.Credential ||
		claudeEnv["ANTHROPIC_MODEL"] != "claude-test" {
		t.Fatalf("unexpected Claude proxy environment: %#v", claudeEnv)
	}

	copilot, err := providerProfile(providerKindCopilot, "copilot-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	copilotEnv, err := copilot.EnvironmentForSession(request, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	if copilotEnv["COPILOT_AUTO_UPDATE"] != "false" || !containsArg(copilot.Args, "--no-auto-update") || !containsArg(copilot.Args, "--disable-builtin-mcps") {
		t.Fatalf("Copilot update/tool hardening missing: env=%#v args=%v", copilotEnv, copilot.Args)
	}
	if copilotEnv["COPILOT_PROVIDER_BASE_URL"] != proxy.BaseURL || copilotEnv["COPILOT_PROVIDER_BEARER_TOKEN"] != proxy.Credential {
		t.Fatalf("unexpected Copilot proxy environment: %#v", copilotEnv)
	}
	wantAdapterName, wantAdapterDigest, err := copilotAdapterIdentity(runtime.GOARCH)
	if err != nil {
		t.Fatal(err)
	}
	if copilot.AdapterName != wantAdapterName || copilot.AdapterDigest != wantAdapterDigest {
		t.Fatalf("Copilot adapter identity = %s/%s, want %s/%s", copilot.AdapterName, copilot.AdapterDigest, wantAdapterName, wantAdapterDigest)
	}
}

const testProjectionAgentUID = "agent-uid"

func TestCodexProviderSessionProjection(t *testing.T) {
	paths := acp.SessionPaths{Home: "/sessions/private/home"}
	proxy := ProviderProxyBinding{BaseURL: "http://127.0.0.1:43210/_orka/provider/session", Credential: "test-auth-token"}
	codex, err := providerProfile(providerKindCodex, "gpt-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	request := testProviderProjectionRequest(t, providerKindCodex, "gpt-test", "codex system", "high", nil, nil, true)
	projection, err := codex.ProjectSession(request, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	environment, err := codex.EnvironmentForSession(request, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	maps.Copy(environment, projection.Environment)
	if environment["NO_BROWSER"] != "1" || environment["CODEX_HOME"] != "/sessions/private/home/.codex" ||
		!strings.Contains(environment["CODEX_CONFIG"], proxy.BaseURL) || environment["CODEX_API_KEY"] != proxy.Credential {
		t.Fatalf("unexpected Codex environment: %#v", environment)
	}
	var config map[string]any
	if err := json.Unmarshal([]byte(environment["CODEX_CONFIG"]), &config); err != nil {
		t.Fatal(err)
	}
	if config["model"] != "gpt-test" || config["developer_instructions"] != "codex system" || config["model_reasoning_effort"] != "high" {
		t.Fatalf("Codex config = %#v", config)
	}
	assertCodexWebSocketTransportsDisabled(t, config)
	if strings.Contains(strings.Join(codex.Args, " "), "npx") {
		t.Fatalf("Codex runtime uses a download-on-start command: %v", codex.Args)
	}
}

func TestCodexProviderSessionProjectionReadOnlySurface(t *testing.T) {
	paths := acp.SessionPaths{Home: "/sessions/private/home"}
	proxy := ProviderProxyBinding{BaseURL: "http://127.0.0.1:43210/_orka/provider/session", Credential: "test-auth-token"}
	codex, err := providerProfile(providerKindCodex, "gpt-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	surface := []string{providerToolGlob, providerToolGrep, providerToolRead}
	request := testProviderProjectionRequest(t, providerKindCodex, "gpt-test", "", "", surface, nil, false)
	projection, err := codex.ProjectSession(request, paths, proxy)
	if err != nil {
		t.Fatalf("read-only codex projection error = %v", err)
	}
	environment, err := codex.EnvironmentForSession(request, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	maps.Copy(environment, projection.Environment)
	// Read-only sessions keep the orka-external agent mode: Codex's own
	// sandbox needs unprivileged user namespaces the runtime Pod forbids, so
	// the RuntimeSession boundary enforces the read-only surface instead.
	if environment["INITIAL_AGENT_MODE"] != codexAgentModeOrkaExternal {
		t.Fatalf("INITIAL_AGENT_MODE = %q, want orka-external", environment["INITIAL_AGENT_MODE"])
	}
	if !strings.Contains(environment["CODEX_CONFIG"], proxy.BaseURL) || environment["CODEX_API_KEY"] != proxy.Credential {
		t.Fatalf("unexpected Codex environment: %#v", environment)
	}

	rejected := testProviderProjectionRequest(t, providerKindCodex, "gpt-test", "", "", []string{providerToolGlob, providerToolRead, providerToolWrite}, nil, false)
	if _, err := codex.ProjectSession(rejected, paths, proxy); err == nil {
		t.Fatal("restricted codex projection with Write was accepted")
	}

	writeIntent := testProviderProjectionRequest(t, providerKindCodex, "gpt-test", "", "", surface, nil, false)
	writeIntent.Profile.WorkspaceIntent = harnessv2.WorkspaceIntentWrite
	if _, err := codex.ProjectSession(writeIntent, paths, proxy); err == nil {
		t.Fatal("write-intent restricted codex projection was accepted")
	}
}

func TestClaudeProviderSessionProjection(t *testing.T) {
	paths := acp.SessionPaths{Home: "/sessions/private/home"}
	proxy := ProviderProxyBinding{BaseURL: "http://127.0.0.1:43210/_orka/provider/session", Credential: "test-auth-token"}
	claude, err := providerProfile(providerKindClaude, "claude-test", harnessv2.WorkspaceIntentWrite)
	if err != nil {
		t.Fatal(err)
	}
	request := testProviderProjectionRequest(t, providerKindClaude, "claude-test", "claude system", "max", []string{providerToolRead, providerToolWebFetch}, []string{providerToolBash}, false)
	projection, err := claude.ProjectSession(request, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	environment, err := claude.EnvironmentForSession(request, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	maps.Copy(environment, projection.Environment)
	for _, name := range []string{"DISABLE_UPDATES", "DISABLE_AUTOUPDATER", "DISABLE_INSTALLATION_CHECKS"} {
		if environment[name] != "1" {
			t.Fatalf("%s = %q", name, environment[name])
		}
	}
	if environment["CLAUDE_CONFIG_DIR"] != "/sessions/private/home/.claude" || environment["ANTHROPIC_BASE_URL"] != proxy.BaseURL ||
		environment["ANTHROPIC_API_KEY"] != proxy.Credential || environment["ANTHROPIC_MODEL"] != "claude-test" {
		t.Fatalf("unexpected Claude environment: %#v", environment)
	}
	if projection.NewSessionMeta["systemPrompt"] != "claude system" {
		t.Fatalf("Claude system prompt metadata = %#v", projection.NewSessionMeta)
	}
	claudeCode := projection.NewSessionMeta["claudeCode"].(map[string]any)
	options := claudeCode["options"].(map[string]any)
	if options["maxTurns"] != int32(7) || options["effort"] != "max" || !slices.Equal(options["tools"].([]string), []string{providerToolRead, providerToolWebFetch}) ||
		!slices.Contains(options["disallowedTools"].([]string), providerToolBash) {
		t.Fatalf("Claude options = %#v", options)
	}
}

func TestCopilotProviderSessionProjection(t *testing.T) {
	paths := acp.SessionPaths{Home: "/sessions/private/home"}
	proxy := ProviderProxyBinding{BaseURL: "http://127.0.0.1:43210/_orka/provider/session", Credential: "test-auth-token"}
	copilot, err := providerProfile(providerKindCopilot, "copilot-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	request := testProviderProjectionRequest(t, providerKindCopilot, "copilot-test", "", "", []string{providerToolRead, providerToolGrep}, nil, false)
	projection, err := copilot.ProjectSession(request, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	environment, err := copilot.EnvironmentForSession(request, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	maps.Copy(environment, projection.Environment)
	args := append(append([]string(nil), copilot.Args...), projection.AdditionalArgs...)
	if environment["COPILOT_AUTO_UPDATE"] != "false" || !containsArg(args, "--no-auto-update") || !containsArg(args, "--disable-builtin-mcps") {
		t.Fatalf("Copilot update/tool hardening missing: env=%#v args=%v", environment, args)
	}
	if environment["COPILOT_PROVIDER_BASE_URL"] != proxy.BaseURL || environment["COPILOT_PROVIDER_BEARER_TOKEN"] != proxy.Credential {
		t.Fatalf("unexpected Copilot proxy environment: %#v", environment)
	}
	excluded := strings.Split(strings.TrimPrefix(projection.AdditionalArgs[0], "--excluded-tools="), ",")
	if !slices.Contains(excluded, "bash") || !slices.Contains(excluded, "create") || !slices.Contains(excluded, "web_search") ||
		!slices.Contains(excluded, "edit") || !slices.Contains(excluded, "str_replace_editor") || !slices.Contains(excluded, "apply_patch") ||
		!slices.Contains(excluded, "list_agents") || !slices.Contains(excluded, "read_agent") || !slices.Contains(excluded, "write_agent") ||
		slices.Contains(excluded, "view") || slices.Contains(excluded, "grep") || slices.Contains(excluded, "rg") {
		t.Fatalf("Copilot excluded tools = %v", excluded)
	}
	editRequest := testProviderProjectionRequest(
		t, providerKindCopilot, "copilot-test", "", "",
		[]string{providerToolEdit, providerToolWrite}, nil, false,
	)
	editProjection, err := copilot.ProjectSession(editRequest, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	editExcluded := strings.Split(strings.TrimPrefix(editProjection.AdditionalArgs[0], "--excluded-tools="), ",")
	for _, allowedID := range []string{"edit", "str_replace_editor", "apply_patch", "create"} {
		if slices.Contains(editExcluded, allowedID) {
			t.Fatalf("Copilot excluded authorized tool alias %q: %v", allowedID, editExcluded)
		}
	}
	wantAdapterName, wantAdapterDigest, err := copilotAdapterIdentity(runtime.GOARCH)
	if err != nil {
		t.Fatal(err)
	}
	if copilot.AdapterName != wantAdapterName || copilot.AdapterDigest != wantAdapterDigest {
		t.Fatalf("Copilot adapter identity = %s/%s, want %s/%s", copilot.AdapterName, copilot.AdapterDigest, wantAdapterName, wantAdapterDigest)
	}
}

func TestCopilotUnrestrictedProjectionKeepsPermanentExclusions(t *testing.T) {
	paths := acp.SessionPaths{Home: "/sessions/private/home"}
	proxy := ProviderProxyBinding{BaseURL: "http://127.0.0.1:43210/_orka/provider/session", Credential: "test-auth-token"}
	copilot, err := providerProfile(providerKindCopilot, "copilot-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	request := testProviderProjectionRequest(t, providerKindCopilot, "copilot-test", "", "", nil, nil, true)
	projection, err := copilot.ProjectSession(request, paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.AdditionalArgs) != 1 {
		t.Fatalf("Copilot unrestricted projection args = %v", projection.AdditionalArgs)
	}
	excluded := strings.Split(strings.TrimPrefix(projection.AdditionalArgs[0], "--excluded-tools="), ",")
	for _, excludedID := range copilotAlwaysExcludedToolIDs {
		if !slices.Contains(excluded, excludedID) {
			t.Fatalf("Copilot unrestricted projection omitted permanent exclusion %q: %v", excludedID, excluded)
		}
	}
}

func TestProviderSessionProjectionFailsClosed(t *testing.T) {
	paths := acp.SessionPaths{Home: "/sessions/private/home"}
	proxy := ProviderProxyBinding{BaseURL: "http://127.0.0.1:43210/_orka/provider/session", Credential: "test-auth-token"}

	codex, err := providerProfile(providerKindCodex, "gpt-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codex.ProjectSession(testProviderProjectionRequest(t, providerKindCodex, "gpt-test", "", "", []string{providerToolRead}, nil, true), paths, proxy); err == nil || !strings.Contains(err.Error(), "cannot exactly enforce") {
		t.Fatalf("Codex restricted policy error = %v", err)
	}

	copilot, err := providerProfile(providerKindCopilot, "copilot-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copilot.ProjectSession(testProviderProjectionRequest(t, providerKindCopilot, "copilot-test", "unsupported", "", nil, nil, true), paths, proxy); err == nil || !strings.Contains(err.Error(), "systemPrompt") {
		t.Fatalf("Copilot system prompt error = %v", err)
	}
	if _, err := copilot.ProjectSession(testProviderProjectionRequest(t, providerKindCopilot, "copilot-test", "", "", []string{providerToolWebSearch}, nil, true), paths, proxy); err == nil || !strings.Contains(err.Error(), providerToolWebSearch) {
		t.Fatalf("Copilot WebSearch policy error = %v", err)
	}
	largeCodexPrompt := strings.Repeat("\u0001", 17<<10)
	if _, err := codex.ProjectSession(testProviderProjectionRequest(t, providerKindCodex, "gpt-test", largeCodexPrompt, "", nil, nil, true), paths, proxy); err == nil || !strings.Contains(err.Error(), "safe environment limit") {
		t.Fatalf("Codex oversized session configuration error = %v", err)
	}
}

func TestProviderSessionPolicyDistinguishesOmittedAndExplicitEmptyToolPolicies(t *testing.T) {
	tests := []struct {
		provider string
		model    string
	}{
		{provider: providerKindCodex, model: "gpt-test"},
		{provider: providerKindClaude, model: "claude-test"},
		{provider: providerKindCopilot, model: "copilot-test"},
	}
	for _, test := range tests {
		t.Run(test.provider, func(t *testing.T) {
			omitted := testProviderProjectionRequest(t, test.provider, test.model, "", "", nil, nil, true)
			policy, err := providerSessionPolicy(omitted, test.provider, test.model)
			if err != nil {
				t.Fatal(err)
			}
			if !policy.unrestricted {
				t.Fatal("omitted provider-native tool policy was not unrestricted")
			}

			explicitEmpty := testProviderProjectionRequest(t, test.provider, test.model, "", "", []string{}, nil, true)
			policy, err = providerSessionPolicy(explicitEmpty, test.provider, test.model)
			if err != nil {
				t.Fatal(err)
			}
			if policy.unrestricted {
				t.Fatal("explicit-empty provider-native tool policy was unrestricted")
			}

			emptyDisallowed := testProviderProjectionRequest(t, test.provider, test.model, "", "", nil, []string{}, true)
			policy, err = providerSessionPolicy(emptyDisallowed, test.provider, test.model)
			if err != nil {
				t.Fatal(err)
			}
			if !policy.unrestricted {
				t.Fatal("explicit-empty disallowlist was not treated as deny-none")
			}
		})
	}
}

func TestCopilotAdapterIdentity(t *testing.T) {
	tests := []struct {
		goarch     string
		wantName   string
		wantDigest string
	}{
		{goarch: "amd64", wantName: "copilot-cli-linux-amd64", wantDigest: "sha256:" + acp.CopilotCLILinuxX64SHA256},
		{goarch: "arm64", wantName: "copilot-cli-linux-arm64", wantDigest: "sha256:" + acp.CopilotCLILinuxARM64SHA256},
	}
	for _, test := range tests {
		t.Run(test.goarch, func(t *testing.T) {
			name, digest, err := copilotAdapterIdentity(test.goarch)
			if err != nil {
				t.Fatal(err)
			}
			if name != test.wantName || digest != test.wantDigest {
				t.Fatalf("identity = %s/%s, want %s/%s", name, digest, test.wantName, test.wantDigest)
			}
		})
	}
	if _, _, err := copilotAdapterIdentity("riscv64"); err == nil {
		t.Fatal("unsupported Copilot architecture unexpectedly accepted")
	}
}

func TestCodexProviderProfileUsesExternalRuntimeSandbox(t *testing.T) {
	paths := acp.SessionPaths{Home: "/sessions/private/home"}
	proxy := ProviderProxyBinding{BaseURL: "http://127.0.0.1:43210/_orka/provider/session", Credential: "local-session-credential"}
	for _, intent := range []harnessv2.WorkspaceIntent{harnessv2.WorkspaceIntentRead, harnessv2.WorkspaceIntentWrite} {
		t.Run(string(intent), func(t *testing.T) {
			profile, err := providerProfile(providerKindCodex, "gpt-test", intent)
			if err != nil {
				t.Fatal(err)
			}
			environment, err := profile.EnvironmentForSession(harnessv2.CreateRuntimeSessionRequest{}, paths, proxy)
			if err != nil {
				t.Fatal(err)
			}
			if got := environment["INITIAL_AGENT_MODE"]; got != codexAgentModeOrkaExternal {
				t.Fatalf("INITIAL_AGENT_MODE = %q, want orka-external", got)
			}
		})
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	dir := t.TempDir()
	controllerToken := filepath.Join(dir, "controller-token")
	capabilitySecret := filepath.Join(dir, "capability-secret")
	providerToken := filepath.Join(dir, "provider-token")
	for path, value := range map[string]string{
		controllerToken: strings.Repeat("t", 32), capabilitySecret: strings.Repeat("s", 32), providerToken: "provider-capability",
	} {
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	values := map[string]string{
		EnvPodUID: "pod-uid", EnvSupervisorBootID: "boot", EnvControllerEpoch: "1", EnvRuntimePoolUID: "pool-uid",
		EnvRuntimePoolGeneration: "1", EnvProvider: providerKindCodex, EnvModel: "gpt-test", EnvWorkspaceIntent: "read",
		EnvAgentConfigurationDigest: testDigest("agent"), EnvToolPolicyDigest: testDigest("tool"),
		EnvApprovalPolicyDigest: testDigest("approval"), EnvMCPConfigurationDigest: testDigest("mcp"),
		EnvProxyCredentialRole: "provider", EnvProxyCredentialScope: "model:gpt-test", EnvResourceClass: "standard",
		EnvControllerTokenFile: controllerToken, EnvCapabilitySecretFile: capabilitySecret, EnvProviderTokenFile: providerToken,
		EnvMCPBrokerURL: "http://orka-controller.orka-system.svc:8080", EnvTrustNamespace: "default",
		EnvSessionBaseDir: filepath.Join(dir, "sessions"), EnvFirstSessionUID: "20000", EnvLastSessionUID: "20010", EnvSessionGID: "20000",
		EnvE2EPromptWriteAmbiguity: testE2EPromptWriteAmbiguityMarker,
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider.Kind != providerKindCodex || cfg.Capabilities.ACPVersion != harnessv2.ACPProfileV1 || cfg.Fence.RuntimeProfileDigest == "" ||
		cfg.Fence.RuntimeInstanceID != "pod-uid.boot" || cfg.Fence.SupervisorBootID != "boot" {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	if !cfg.Capabilities.SupportsAgentSessionConfiguration {
		t.Fatal("supervisor did not advertise Agent session configuration support")
	}
	if cfg.E2EPromptWriteAmbiguityMarker != testE2EPromptWriteAmbiguityMarker {
		t.Fatalf("E2E prompt write ambiguity marker = %q", cfg.E2EPromptWriteAmbiguityMarker)
	}
	if cfg.Capabilities.AdapterDigests["codex-acp"] != "sha256:"+acp.CodexACPTarSHA256 ||
		cfg.Capabilities.AdapterDigests["codex-acp-orka-patch"] != "sha256:"+acp.CodexACPOrkaPatchSHA256 ||
		cfg.Capabilities.AdapterDigests["codex-acp-orka-dist"] != "sha256:"+acp.CodexACPOrkaDistSHA256 ||
		cfg.Provider.AdapterName != "codex-acp-orka-dist" || cfg.Provider.AdapterDigest != "sha256:"+acp.CodexACPOrkaDistSHA256 {
		t.Fatalf("unexpected adapter digests: capabilities=%#v provider=%#v", cfg.Capabilities.AdapterDigests, cfg.Provider)
	}
	if cfg.Capabilities.Limits.MaxUpdateEventsPerSecond != runtimeMaxUpdateEventsPerSecond {
		t.Fatalf("max update events per second = %d, want %d", cfg.Capabilities.Limits.MaxUpdateEventsPerSecond, runtimeMaxUpdateEventsPerSecond)
	}
	if cfg.ProviderProxy.UpstreamBaseURL != "http://vekil.vekil-system.svc:1337/v1" {
		t.Fatalf("unexpected provider proxy base URL: %q", cfg.ProviderProxy.UpstreamBaseURL)
	}
	if cfg.ProviderProxy.UpstreamBearerToken != "provider-capability" {
		t.Fatal("provider proxy token was not loaded from the supervisor-only file")
	}

	t.Setenv(EnvProvider, providerKindCopilot)
	copilotCfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	wantAdapterName, wantAdapterDigest, err := copilotAdapterIdentity(runtime.GOARCH)
	if err != nil {
		t.Fatal(err)
	}
	if copilotCfg.Provider.Kind != providerKindCopilot || copilotCfg.Provider.AdapterName != wantAdapterName ||
		copilotCfg.Provider.AdapterDigest != wantAdapterDigest {
		t.Fatalf("unexpected Copilot provider: %#v", copilotCfg.Provider)
	}
	if copilotCfg.Capabilities.AdapterDigests["copilot-cli-linux-amd64"] != "sha256:"+acp.CopilotCLILinuxX64SHA256 ||
		copilotCfg.Capabilities.AdapterDigests["copilot-cli-linux-arm64"] != "sha256:"+acp.CopilotCLILinuxARM64SHA256 {
		t.Fatalf("unexpected Copilot adapter digests: %#v", copilotCfg.Capabilities.AdapterDigests)
	}
}

func TestLoadConfigFromEnvBootstrapSecrets(t *testing.T) {
	dir := t.TempDir()
	values := map[string]string{
		EnvPodUID: "actor:orka-acp-actor", EnvSupervisorBootID: "boot", EnvControllerEpoch: "1", EnvRuntimePoolUID: "pool-uid",
		EnvRuntimePoolGeneration: "1", EnvProvider: providerKindCodex, EnvModel: "gpt-test", EnvWorkspaceIntent: "read",
		EnvAgentConfigurationDigest: testDigest("agent"), EnvToolPolicyDigest: testDigest("tool"),
		EnvApprovalPolicyDigest: testDigest("approval"), EnvMCPConfigurationDigest: testDigest("mcp"),
		EnvProxyCredentialRole: "provider", EnvProxyCredentialScope: "model:gpt-test", EnvResourceClass: "standard",
		EnvControllerTokenBootstrap: strings.Repeat("t", 32), EnvCapabilitySecretBootstrap: strings.Repeat("s", 32),
		EnvProviderTokenBootstrap: "provider-capability",
		EnvMCPBrokerURL:           "http://orka-controller.orka-system.svc:8080", EnvTrustNamespace: "default",
		EnvSessionBaseDir: filepath.Join(dir, "sessions"), EnvFirstSessionUID: "20000", EnvLastSessionUID: "20010", EnvSessionGID: "20000",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ControllerBearerToken != strings.Repeat("t", 32) || string(cfg.CapabilitySecret) != strings.Repeat("s", 32) ||
		cfg.ProviderProxy.UpstreamBearerToken != "provider-capability" {
		t.Fatal("bootstrap secrets were not loaded into the supervisor config")
	}
	if cfg.Fence.RuntimeInstanceID != "actor:orka-acp-actor.boot" {
		t.Fatalf("instance ID = %q, want actor-scoped identity", cfg.Fence.RuntimeInstanceID)
	}
	for _, name := range []string{EnvControllerTokenBootstrap, EnvCapabilitySecretBootstrap, EnvProviderTokenBootstrap} {
		if value, present := os.LookupEnv(name); present && value != "" {
			t.Fatalf("read-once bootstrap env %s survived config load", name)
		}
	}

	// The file variable always wins over the bootstrap variable.
	controllerToken := filepath.Join(dir, "controller-token")
	if err := os.WriteFile(controllerToken, []byte(strings.Repeat("f", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvControllerTokenFile, controllerToken)
	t.Setenv(EnvControllerTokenBootstrap, strings.Repeat("x", 32))
	t.Setenv(EnvCapabilitySecretBootstrap, strings.Repeat("s", 32))
	t.Setenv(EnvProviderTokenBootstrap, "provider-capability")
	fileCfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if fileCfg.ControllerBearerToken != strings.Repeat("f", 32) {
		t.Fatal("mounted secret file did not take precedence over the bootstrap env")
	}
	if value, present := os.LookupEnv(EnvControllerTokenBootstrap); present && value != "" {
		t.Fatal("unused bootstrap secret survived file-backed config load")
	}

	// Neither the file nor the bootstrap variable fails closed.
	t.Setenv(EnvControllerTokenFile, "")
	t.Setenv(EnvControllerTokenBootstrap, "")
	t.Setenv(EnvCapabilitySecretBootstrap, strings.Repeat("s", 32))
	t.Setenv(EnvProviderTokenBootstrap, "provider-capability")
	if _, err := LoadConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "read-once bootstrap secret") {
		t.Fatalf("missing controller secret error = %v, want fail-closed bootstrap message", err)
	}
}

func TestDefaultProtocolLimitsUseProviderSpecificUpdateRates(t *testing.T) {
	tests := []struct {
		provider string
		want     int
	}{
		{provider: providerKindCodex, want: runtimeMaxUpdateEventsPerSecond},
		{provider: providerKindClaude, want: runtimeMaxUpdateEventsPerSecond},
		{provider: providerKindCopilot, want: runtimeMaxUpdateEventsPerSecond},
		{provider: providerKindOpencode, want: runtimeMaxUpdateEventsPerSecond},
	}
	for _, test := range tests {
		t.Run(test.provider, func(t *testing.T) {
			if got := defaultProtocolLimits(test.provider).MaxUpdateEventsPerSecond; got != test.want {
				t.Fatalf("MaxUpdateEventsPerSecond = %d, want %d", got, test.want)
			}
		})
	}
}

func TestProviderUpstreamBaseURLPreservesProviderSemantics(t *testing.T) {
	if got := providerUpstreamBaseURL(providerKindCodex, "http://vekil:1337"); got != "http://vekil:1337/v1" {
		t.Fatalf("Codex upstream base URL = %q", got)
	}
	if got := providerUpstreamBaseURL(providerKindCopilot, "http://vekil:1337/v1"); got != "http://vekil:1337/v1" {
		t.Fatalf("Copilot upstream base URL = %q", got)
	}
	if got := providerUpstreamBaseURL("claude", "http://vekil:1337/"); got != "http://vekil:1337" {
		t.Fatalf("Claude upstream base URL = %q", got)
	}
}

func testProviderProjectionRequest(
	t *testing.T,
	provider string,
	model string,
	systemPrompt string,
	reasoningEffort string,
	allowed []string,
	disallowed []string,
	allowBash bool,
) harnessv2.CreateRuntimeSessionRequest {
	t.Helper()
	allowed = slices.Clone(allowed)
	disallowed = slices.Clone(disallowed)
	slices.Sort(allowed)
	slices.Sort(disallowed)
	toolPolicy := harnessv2.MCPToolPolicy{
		AllowedToolNames: allowed, DisallowedToolNames: disallowed, AllowBash: allowBash,
	}
	for _, name := range allowed {
		if !toolPolicy.Allows(name) {
			continue
		}
		if _, ok := canonicalProviderNativeToolName(name); !ok {
			t.Fatalf("unknown provider-native test tool %q", name)
		}
		toolPolicy.Tools = append(toolPolicy.Tools, harnessv2.MCPToolDescriptor{
			Name: name, Description: "provider native", Source: harnessv2.MCPToolSourceProviderNative,
			Effect: harnessv2.MCPToolEffectReadOnly,
		})
	}
	slices.SortFunc(toolPolicy.Tools, func(a, b harnessv2.MCPToolDescriptor) int { return strings.Compare(a.Name, b.Name) })
	var err error
	toolPolicy.DescriptorDigest, err = harnessv2.CanonicalMCPToolDescriptorDigest(toolPolicy.Tools)
	if err != nil {
		t.Fatal(err)
	}
	approval := harnessv2.MCPApprovalPolicy{}
	configuration := harnessv2.AgentSessionConfiguration{
		AgentUID: testProjectionAgentUID, AgentGeneration: 1, ProviderKind: provider, Model: model,
		MaxTurns: 7, ReasoningEffort: reasoningEffort, SystemPrompt: systemPrompt,
	}
	agentDigest, err := harnessv2.CanonicalAgentConfigurationDigest(configuration)
	if err != nil {
		t.Fatal(err)
	}
	toolDigest, err := harnessv2.CanonicalRuntimeToolPolicyDigest(allowed, disallowed, allowBash)
	if err != nil {
		t.Fatal(err)
	}
	approvalDigest, err := harnessv2.CanonicalMCPApprovalPolicyDigest(approval)
	if err != nil {
		t.Fatal(err)
	}
	mcpDigest, err := harnessv2.CanonicalMCPConfigurationDigest(allowed)
	if err != nil {
		t.Fatal(err)
	}
	profile := harnessv2.RuntimeProfile{
		ACPProfile: harnessv2.ACPProfileV1, AdapterDigests: map[string]string{"adapter": testDigest("adapter")},
		ProviderKind: provider, Model: model,
		AgentConfigurationDigest: agentDigest, ToolPolicyDigest: toolDigest, ApprovalPolicyDigest: approvalDigest,
		MCPConfigurationDigest: mcpDigest, WorkspaceIntent: harnessv2.WorkspaceIntentRead,
		ProxyCredentialRole: "provider", ProxyCredentialScope: "model:" + model, ResourceClass: "standard",
	}
	return harnessv2.CreateRuntimeSessionRequest{
		Profile: profile, AgentConfiguration: &configuration,
		MCPConfiguration: harnessv2.MCPPolicyConfiguration{
			ToolPolicyDigest: toolDigest, ApprovalPolicyDigest: approvalDigest, MCPConfigurationDigest: mcpDigest,
			ToolPolicy: toolPolicy, ApprovalPolicy: approval,
		},
	}
}

func containsArg(args []string, want string) bool {
	return slices.Contains(args, want)
}

// assertCodexWebSocketTransportsDisabled proves the session config selects
// the custom HTTPS-only provider instead of Codex's built-in "openai"
// provider, whose Responses WebSocket attempt the proxy rejects with 403 and
// whose fallback warning would leak into the agent's first message.
func assertCodexWebSocketTransportsDisabled(t *testing.T, config map[string]any) {
	t.Helper()
	if config["model_provider"] != codexProviderID {
		t.Fatalf("Codex model_provider = %#v, want %q", config["model_provider"], codexProviderID)
	}
	if _, ok := config["openai_base_url"]; ok {
		t.Fatalf("Codex config still selects the built-in openai provider: %#v", config)
	}
	providers, _ := config["model_providers"].(map[string]any)
	provider, _ := providers[codexProviderID].(map[string]any)
	if provider["wire_api"] != "responses" || provider["env_key"] != "CODEX_API_KEY" || provider["base_url"] == "" {
		t.Fatalf("Codex provider definition = %#v", provider)
	}
}

func TestPrepareCodexHomeDisablesResponsesWebSockets(t *testing.T) {
	paths := acp.SessionPaths{Home: t.TempDir()}
	if err := prepareCodexHome(paths); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(paths.Home, ".codex", "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	config := string(data)
	if !strings.Contains(config, "check_for_update_on_startup = false") {
		t.Fatalf("config.toml lacks the update opt-out:\n%s", config)
	}
}
