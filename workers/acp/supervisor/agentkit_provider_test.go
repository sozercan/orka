package supervisor

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/harness/v2/conformance"
)

const testAgentKitAdapterDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestAgentKitProviderProfileUsesFrozenACPContract(t *testing.T) {
	t.Setenv(EnvAgentKitAdapterDigest, testAgentKitAdapterDigest)
	configurationDigest := testDigest("agentkit-config")
	t.Setenv(EnvAgentConfigurationDigest, configurationDigest)
	profile, err := providerProfile(providerKindAgentKit, "gpt-test", harnessv2.WorkspaceIntentWrite)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Command != "/opt/agentkit/bin/agentkit-serve" ||
		!slices.Equal(profile.Args, []string{"--config", agentKitConfigPath, "--protocol", "acp"}) {
		t.Fatalf("AgentKit ACP command = %q %v", profile.Command, profile.Args)
	}
	if profile.AdapterName != agentKitAdapterName || profile.AdapterDigest != testAgentKitAdapterDigest {
		t.Fatalf("AgentKit adapter identity = %s/%s", profile.AdapterName, profile.AdapterDigest)
	}
	digests := providerAdapterDigests(providerKindAgentKit)
	if len(digests) != 1 || digests[agentKitAdapterName] != testAgentKitAdapterDigest {
		t.Fatalf("AgentKit adapter digests = %#v", digests)
	}

	proxy := ProviderProxyBinding{
		BaseURL:    "http://127.0.0.1:43210/_orka/provider/session",
		Credential: "local-session-credential",
	}
	environment, err := profile.EnvironmentForSession(
		harnessv2.CreateRuntimeSessionRequest{}, acp.SessionPaths{Home: "/sessions/private/home"}, proxy,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantEnvironment := map[string]string{
		agentKitProviderBaseURLEnv: proxy.BaseURL,
		agentKitProviderTokenEnv:   proxy.Credential,
		agentKitModelEnv:           "gpt-test",
		agentKitConfigDigestEnv:    configurationDigest,
	}
	if !maps.Equal(environment, wantEnvironment) {
		t.Fatalf("AgentKit session environment = %#v, want %#v", environment, wantEnvironment)
	}
}

func TestAgentKitProviderCapabilitiesDescribeComposedRuntime(t *testing.T) {
	capabilities := providerCapabilities(providerKindAgentKit, "gpt-test")
	if capabilities.SupportsPermissions || !capabilities.SupportsCancel || !capabilities.SupportsTools {
		t.Fatalf("AgentKit composed runtime capabilities = %#v", capabilities)
	}
	if capabilities.SupportsImages || capabilities.SupportsAudio || capabilities.SupportsEmbeddedResources {
		t.Fatalf("AgentKit advertised unsupported rich content capabilities: %#v", capabilities)
	}
}

func TestAgentKitSessionProjectionAcceptsOnlyBrokeredTools(t *testing.T) {
	t.Setenv(EnvAgentKitAdapterDigest, testAgentKitAdapterDigest)
	profile, err := providerProfile(providerKindAgentKit, "gpt-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	request := agentKitBrokeredProjectionRequest(t)
	projection, err := profile.ProjectSession(request, acp.SessionPaths{}, ProviderProxyBinding{})
	if err != nil {
		t.Fatalf("brokered AgentKit projection error = %v", err)
	}
	if len(projection.AdditionalArgs) != 0 || len(projection.Environment) != 0 || len(projection.NewSessionMeta) != 0 {
		t.Fatalf("AgentKit projected per-Task overrides: %#v", projection)
	}

	providerNative := testProviderProjectionRequest(
		t, providerKindAgentKit, "gpt-test", "", "", []string{providerToolRead}, nil, false,
	)
	providerNative.AgentConfiguration = nil
	if _, err := profile.ProjectSession(providerNative, acp.SessionPaths{}, ProviderProxyBinding{}); err == nil ||
		!strings.Contains(err.Error(), "forbids provider-native tool") {
		t.Fatalf("provider-native AgentKit tool error = %v", err)
	}
}

func TestAgentKitSessionProjectionRejectsPerTaskOverrides(t *testing.T) {
	t.Setenv(EnvAgentKitAdapterDigest, testAgentKitAdapterDigest)
	profile, err := providerProfile(providerKindAgentKit, "gpt-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	request := agentKitBrokeredProjectionRequest(t)
	request.AgentConfiguration = &harnessv2.AgentSessionConfiguration{}
	if _, err := profile.ProjectSession(request, acp.SessionPaths{}, ProviderProxyBinding{}); err == nil ||
		!strings.Contains(err.Error(), "does not support per-Task AgentConfiguration") {
		t.Fatalf("AgentKit per-Task configuration error = %v", err)
	}
}

func TestAgentKitSessionProjectionRejectsApprovalRequiredTools(t *testing.T) {
	t.Setenv(EnvAgentKitAdapterDigest, testAgentKitAdapterDigest)
	profile, err := providerProfile(providerKindAgentKit, "gpt-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	request := agentKitBrokeredProjectionRequest(t)
	request.MCPConfiguration.ApprovalPolicy.RequiredTools = []string{"lookup"}
	approvalDigest, err := harnessv2.CanonicalMCPApprovalPolicyDigest(request.MCPConfiguration.ApprovalPolicy)
	if err != nil {
		t.Fatal(err)
	}
	request.MCPConfiguration.ApprovalPolicyDigest = approvalDigest
	request.Profile.ApprovalPolicyDigest = approvalDigest
	if _, err := profile.ProjectSession(request, acp.SessionPaths{}, ProviderProxyBinding{}); err == nil ||
		!strings.Contains(err.Error(), "does not support approval-required MCP tools") {
		t.Fatalf("AgentKit approval-required tool error = %v", err)
	}
}

func newTestAgentKitServer(t *testing.T) (*Server, Config, harnessv2.RuntimeProfile) {
	t.Helper()
	cfg, profile := newTestConfigWithUpstream(
		t, "immediate", "http://127.0.0.1:1", strings.Repeat("p", 32),
	)
	profile.ProviderKind = providerKindAgentKit
	profile.AgentConfigurationDigest = testDigest("agentkit-config")
	profile.AdapterDigests = map[string]string{agentKitAdapterName: testAgentKitAdapterDigest}
	profileDigest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Fence.RuntimeProfileDigest = profileDigest
	cfg.Capabilities.RuntimeProfileDigest = profileDigest
	cfg.Capabilities.AdapterDigests = maps.Clone(profile.AdapterDigests)
	cfg.Capabilities.SupportsAgentSessionConfiguration = false
	cfg.Capabilities.Provider = providerCapabilities(providerKindAgentKit, profile.Model)
	cfg.Provider.Kind = providerKindAgentKit
	cfg.Provider.AdapterName = agentKitAdapterName
	cfg.Provider.AdapterDigest = testAgentKitAdapterDigest
	cfg.Provider.ProjectSession = func(
		request harnessv2.CreateRuntimeSessionRequest,
		_ acp.SessionPaths,
		_ ProviderProxyBinding,
	) (ProviderSessionProjection, error) {
		return agentKitSessionProjection(request, profile.Model)
	}
	cfg.ProviderProxy.ProviderKind = providerKindAgentKit

	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Close(ctx)
	})
	return server, cfg, profile
}

func TestSupervisorAgentKitPassesConformanceAuthProbes(t *testing.T) {
	server, cfg, profile := newTestAgentKitServer(t)
	endpoint := httptest.NewServer(server.Handler())
	defer endpoint.Close()
	remaining := cfg.UIDAllocator.Remaining()
	policy := testServerMCPPolicyConfiguration(profile)
	result := conformance.Check(t.Context(), conformance.Target{
		BaseURL: endpoint.URL, ControllerBearerToken: cfg.ControllerBearerToken,
		OperationCapabilitySecret: cfg.CapabilitySecret, ControlTimeout: 5 * time.Second,
		ExpectedRuntimeInstanceID: cfg.Fence.RuntimeInstanceID, ExpectedControllerEpoch: cfg.Fence.ControllerEpoch,
		Profile: profile, ToolPolicy: policy.ToolPolicy, ApprovalPolicy: policy.ApprovalPolicy,
		Limits: cfg.Capabilities.Limits, SupportsDrain: cfg.Capabilities.SupportsDrain,
		SupportsPublicationFinalization: cfg.Capabilities.SupportsPublicationFinalization,
		WorkspaceGovernance:             cfg.Capabilities.WorkspaceGovernance,
	})
	if !result.Passed {
		t.Fatalf("production supervisor conformance auth probes failed: %s", result.Message)
	}
	if result.LifecycleProbeExecuted || cfg.UIDAllocator.Remaining() != remaining {
		t.Fatal("authentication probes started a runtime session")
	}
}

func TestSupervisorAgentKitPassesLifecycleConformance(t *testing.T) {
	if runtime.GOOS != linuxGOOS {
		t.Skip("workspace freeze and descendant cleanup proof require Linux")
	}
	server, cfg, profile := newTestAgentKitServer(t)
	// The ACP child uses a distinct UID and must traverse the test-owned
	// ancestors of its session tree.
	testDir := filepath.Dir(cfg.SessionBaseDir)
	for _, dir := range []string{testDir, filepath.Dir(testDir)} {
		if err := os.Chmod(dir, 0711); err != nil {
			t.Fatal(err)
		}
	}
	server.cfg.Provider.EnvironmentForSession = func(request harnessv2.CreateRuntimeSessionRequest, _ acp.SessionPaths, _ ProviderProxyBinding) (map[string]string, error) {
		mode := "wait"
		if strings.HasPrefix(string(request.RuntimeSessionID), "conformance-session-workspace-") {
			mode = "immediate"
		}
		return map[string]string{"SUPERVISOR_ACP_HELPER_MODE": mode}, nil
	}
	endpoint := httptest.NewServer(server.Handler())
	defer endpoint.Close()
	policy := testServerMCPPolicyConfiguration(profile)
	result := conformance.Check(t.Context(), conformance.Target{
		BaseURL: endpoint.URL, ControllerBearerToken: cfg.ControllerBearerToken,
		OperationCapabilitySecret: cfg.CapabilitySecret, ControlTimeout: 30 * time.Second,
		ExpectedRuntimeInstanceID: cfg.Fence.RuntimeInstanceID, ExpectedControllerEpoch: cfg.Fence.ControllerEpoch,
		Profile: profile, ToolPolicy: policy.ToolPolicy, ApprovalPolicy: policy.ApprovalPolicy,
		Limits: cfg.Capabilities.Limits, SupportsDrain: cfg.Capabilities.SupportsDrain,
		SupportsPublicationFinalization: cfg.Capabilities.SupportsPublicationFinalization,
		WorkspaceGovernance:             cfg.Capabilities.WorkspaceGovernance, ProbeLifecycle: true,
	})
	if !result.Passed || !result.LifecycleProbeExecuted {
		t.Fatalf("production supervisor lifecycle conformance failed: %s", result.Message)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.sessions) != 0 {
		t.Fatalf("conformance leaked %d runtime sessions", len(server.sessions))
	}
}

func TestSupervisorAgentKitAcceptsNilConfigurationWithDefaultProviderLimit(t *testing.T) {
	server, cfg, profile := newTestAgentKitServer(t)
	request := testCreateSessionRequest(t, cfg, profile)
	request.AgentConfiguration = nil
	request.Metadata.RequestDigest = ""
	sealRequest(t, &request.Metadata.RequestDigest, request)
	response := performMutation(
		t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", request, cfg,
	)
	if response.Code != http.StatusCreated {
		t.Fatalf("AgentKit create status = %d body=%s", response.Code, response.Body.String())
	}
	server.mu.Lock()
	maxTurns := server.sessions[request.RuntimeSessionID].agentConfiguration.MaxTurns
	server.mu.Unlock()
	if maxTurns != defaultProviderProxyMaxTurns {
		t.Fatalf("AgentKit provider request limit = %d, want 50", maxTurns)
	}

}

func TestAgentKitAdapterDigestIsRequiredAndCanonical(t *testing.T) {
	for _, digest := range []string{
		"",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"sha256:short",
		"sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"sha256:gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
	} {
		t.Run(digest, func(t *testing.T) {
			t.Setenv(EnvAgentKitAdapterDigest, digest)
			if _, err := providerProfile(providerKindAgentKit, "gpt-test", harnessv2.WorkspaceIntentRead); err == nil ||
				!strings.Contains(err.Error(), EnvAgentKitAdapterDigest) {
				t.Fatalf("AgentKit adapter digest error = %v", err)
			}
		})
	}
}

func agentKitBrokeredProjectionRequest(t *testing.T) harnessv2.CreateRuntimeSessionRequest {
	t.Helper()
	request := testProviderProjectionRequest(
		t, providerKindAgentKit, "gpt-test", "", "", nil, nil, false,
	)
	tool := harnessv2.MCPToolDescriptor{
		Name: "lookup", Description: "look up a test value", InputSchema: json.RawMessage(`{"type":"object"}`),
		Source: harnessv2.MCPToolSourceBrokeredBuiltin, Effect: harnessv2.MCPToolEffectReadOnly,
	}
	request.MCPConfiguration.ToolPolicy.AllowedToolNames = []string{tool.Name}
	request.MCPConfiguration.ToolPolicy.Tools = []harnessv2.MCPToolDescriptor{tool}
	var err error
	request.MCPConfiguration.ToolPolicy.DescriptorDigest, err = harnessv2.CanonicalMCPToolDescriptorDigest(
		request.MCPConfiguration.ToolPolicy.Tools,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.MCPConfiguration.ToolPolicyDigest, err = harnessv2.CanonicalRuntimeToolPolicyDigest(
		request.MCPConfiguration.ToolPolicy.AllowedToolNames,
		request.MCPConfiguration.ToolPolicy.DisallowedToolNames,
		request.MCPConfiguration.ToolPolicy.AllowBash,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.MCPConfiguration.MCPConfigurationDigest, err = harnessv2.CanonicalMCPConfigurationDigest(
		request.MCPConfiguration.ToolPolicy.AllowedToolNames,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Profile.ToolPolicyDigest = request.MCPConfiguration.ToolPolicyDigest
	request.Profile.MCPConfigurationDigest = request.MCPConfiguration.MCPConfigurationDigest
	request.AgentConfiguration = nil
	return request
}
