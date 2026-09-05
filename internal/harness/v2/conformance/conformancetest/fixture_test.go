package conformancetest

import (
	"testing"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

func TestDeterministicProfilePinsEmptyPolicy(t *testing.T) {
	profile, err := DeterministicProfile("fixture-runtime")
	if err != nil {
		t.Fatal(err)
	}
	if err := profile.Validate(); err != nil {
		t.Fatalf("profile validation failed: %v", err)
	}
	if got := profile.AdapterDigests["fixture-runtime"]; got == "" {
		t.Fatal("fixture adapter digest is empty")
	}

	toolDigest, err := harnessv2.CanonicalRuntimeToolPolicyDigest([]string{}, []string{}, false)
	if err != nil {
		t.Fatal(err)
	}
	approvalDigest, err := harnessv2.CanonicalMCPApprovalPolicyDigest(harnessv2.MCPApprovalPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	mcpDigest, err := harnessv2.CanonicalMCPConfigurationDigest([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if profile.ToolPolicyDigest != toolDigest || profile.ApprovalPolicyDigest != approvalDigest || profile.MCPConfigurationDigest != mcpDigest {
		t.Fatalf("fixture policy digests = %q/%q/%q, want %q/%q/%q",
			profile.ToolPolicyDigest, profile.ApprovalPolicyDigest, profile.MCPConfigurationDigest,
			toolDigest, approvalDigest, mcpDigest)
	}
}
