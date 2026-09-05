package supervisor

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const (
	providerKindAgentKit       = "agentkit"
	EnvAgentKitAdapterDigest   = "ORKA_ACP_AGENTKIT_ADAPTER_DIGEST"
	agentKitAdapterName        = "agentkit-serve-acp"
	agentKitConfigPath         = "/agent/agent.yaml"
	agentKitProviderBaseURLEnv = "AGENTKIT_ACP_PROVIDER_BASE_URL"
	agentKitProviderTokenEnv   = "AGENTKIT_ACP_PROVIDER_TOKEN"
	agentKitModelEnv           = "AGENTKIT_ACP_MODEL"
	agentKitConfigDigestEnv    = "AGENTKIT_ACP_AGENT_CONFIGURATION_DIGEST"
)

func agentKitProviderProfile(model string) (ProviderProfile, error) {
	adapterDigest, err := agentKitAdapterDigestFromEnv()
	if err != nil {
		return ProviderProfile{}, err
	}
	return ProviderProfile{
		Kind:          providerKindAgentKit,
		Model:         model,
		Command:       "/opt/agentkit/bin/agentkit-serve",
		Args:          []string{"--config", agentKitConfigPath, "--protocol", "acp"},
		AdapterName:   agentKitAdapterName,
		AdapterDigest: adapterDigest,
		ProjectSession: func(
			request harnessv2.CreateRuntimeSessionRequest,
			_ acp.SessionPaths,
			_ ProviderProxyBinding,
		) (ProviderSessionProjection, error) {
			return agentKitSessionProjection(request, model)
		},
		EnvironmentForSession: func(
			_ harnessv2.CreateRuntimeSessionRequest,
			_ acp.SessionPaths,
			proxy ProviderProxyBinding,
		) (map[string]string, error) {
			return map[string]string{
				agentKitProviderBaseURLEnv: proxy.BaseURL,
				agentKitProviderTokenEnv:   proxy.Credential,
				agentKitModelEnv:           model,
				agentKitConfigDigestEnv:    requiredEnv(EnvAgentConfigurationDigest),
			}, nil
		},
	}, nil
}

func agentKitSessionProjection(
	request harnessv2.CreateRuntimeSessionRequest,
	model string,
) (ProviderSessionProjection, error) {
	if request.AgentConfiguration != nil {
		return ProviderSessionProjection{}, fmt.Errorf("AgentKit ACP runtime does not support per-Task AgentConfiguration")
	}
	if request.Profile.ProviderKind != providerKindAgentKit || request.Profile.Model != model {
		return ProviderSessionProjection{}, fmt.Errorf("AgentKit session configuration does not match runtime profile")
	}
	if err := request.MCPConfiguration.ValidateProfile(request.Profile); err != nil {
		return ProviderSessionProjection{}, fmt.Errorf("AgentKit MCP policy configuration: %w", err)
	}
	if len(request.MCPConfiguration.ApprovalPolicy.RequiredTools) > 0 {
		return ProviderSessionProjection{}, fmt.Errorf("AgentKit ACP runtime does not support approval-required MCP tools")
	}
	for _, descriptor := range request.MCPConfiguration.ToolPolicy.Tools {
		if !descriptor.Source.Brokered() {
			return ProviderSessionProjection{}, fmt.Errorf(
				"AgentKit ACP runtime forbids provider-native tool %q; tools must use the Orka session MCP server",
				descriptor.Name,
			)
		}
	}
	return ProviderSessionProjection{}, nil
}

func agentKitAdapterDigestFromEnv() (string, error) {
	digest := strings.TrimSpace(os.Getenv(EnvAgentKitAdapterDigest))
	encoded, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || len(encoded) != 64 || encoded != strings.ToLower(encoded) {
		return "", fmt.Errorf("%s must be a sha256 digest", EnvAgentKitAdapterDigest)
	}
	if _, err := hex.DecodeString(encoded); err != nil {
		return "", fmt.Errorf("%s must be a sha256 digest", EnvAgentKitAdapterDigest)
	}
	return digest, nil
}

func agentKitAdapterDigests(adapterDigest string) map[string]string {
	return map[string]string{
		agentKitAdapterName: adapterDigest,
	}
}

func providerCapabilities(provider, model string) harnessv2.ProviderCapabilities {
	capabilities := harnessv2.ProviderCapabilities{
		ProviderKinds:             []string{provider},
		Models:                    []string{model},
		SupportsPermissions:       true,
		SupportsCancel:            true,
		SupportsTools:             true,
		SupportsImages:            true,
		SupportsEmbeddedResources: true,
	}
	if provider == providerKindAgentKit {
		// The AgentKit ACP adapter does not emit session/request_permission.
		capabilities.SupportsPermissions = false
		capabilities.SupportsImages = false
		capabilities.SupportsAudio = false
		capabilities.SupportsEmbeddedResources = false
	}
	return capabilities
}
