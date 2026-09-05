package conformancetest

import (
	"fmt"
	"strings"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const DeterministicPromptResult = "deterministic external runtime result"

// DeterministicProfile returns the shared profile used by the in-cluster v2
// fixture and the E2E AgentRuntime registrations that point at it.
func DeterministicProfile(adapterName string) (harnessv2.RuntimeProfile, error) {
	zeroDigest := "sha256:" + strings.Repeat("0", 64)
	toolPolicyDigest, err := harnessv2.CanonicalRuntimeToolPolicyDigest([]string{}, []string{}, false)
	if err != nil {
		return harnessv2.RuntimeProfile{}, fmt.Errorf("canonicalize deterministic tool policy: %w", err)
	}
	approvalPolicyDigest, err := harnessv2.CanonicalMCPApprovalPolicyDigest(harnessv2.MCPApprovalPolicy{})
	if err != nil {
		return harnessv2.RuntimeProfile{}, fmt.Errorf("canonicalize deterministic approval policy: %w", err)
	}
	mcpConfigurationDigest, err := harnessv2.CanonicalMCPConfigurationDigest([]string{})
	if err != nil {
		return harnessv2.RuntimeProfile{}, fmt.Errorf("canonicalize deterministic MCP configuration: %w", err)
	}
	return harnessv2.RuntimeProfile{
		ACPProfile:               harnessv2.ACPProfileV1,
		AdapterDigests:           map[string]string{adapterName: zeroDigest},
		ProviderKind:             "external",
		Model:                    "orka-harness-v2-e2e",
		AgentConfigurationDigest: zeroDigest,
		ToolPolicyDigest:         toolPolicyDigest,
		ApprovalPolicyDigest:     approvalPolicyDigest,
		MCPConfigurationDigest:   mcpConfigurationDigest,
		WorkspaceIntent:          harnessv2.WorkspaceIntentRead,
		ProxyCredentialRole:      "provider-inference",
		ProxyCredentialScope:     "model:orka-harness-v2-e2e",
		ResourceClass:            "standard",
	}, nil
}
