package v2

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestPromptMCPAuthorizationEnforcesCanonicalDescriptorsAndProfile(t *testing.T) {
	metadata := testMutationMetadata(t, true)
	lease := PromptLease{Generation: 3, IssuedAt: testNow, ExpiresAt: testNow.Add(2 * time.Minute)}
	authorization := testMCPAuthorizationWithTools(t, metadata, lease)
	if err := authorization.ValidateForAt(metadata, lease, testNow.Add(time.Second)); err != nil {
		t.Fatalf("ValidateForAt() error = %v", err)
	}
	profile := testRuntimeProfile()
	profile.ToolPolicyDigest = authorization.ToolPolicyDigest
	profile.ApprovalPolicyDigest = authorization.ApprovalPolicyDigest
	profile.MCPConfigurationDigest = authorization.MCPConfigurationDigest
	if err := authorization.ValidateProfile(profile); err != nil {
		t.Fatalf("ValidateProfile() error = %v", err)
	}

	tampered := authorization
	tampered.ToolPolicy.Tools = append([]MCPToolDescriptor(nil), authorization.ToolPolicy.Tools...)
	tampered.ToolPolicy.Tools[0].Description = "tampered"
	if err := tampered.ValidateForAt(metadata, lease, testNow.Add(time.Second)); err == nil ||
		!strings.Contains(err.Error(), "descriptor digest") {
		t.Fatalf("tampered descriptor validation error = %v", err)
	}

	tamperedAllowlist := authorization
	tamperedAllowlist.ToolPolicy.AllowBash = false
	if err := tamperedAllowlist.ValidateForAt(metadata, lease, testNow.Add(time.Second)); err == nil ||
		!strings.Contains(err.Error(), "tool policy digest") {
		t.Fatalf("tampered allowlist validation error = %v", err)
	}

	tamperedApproval := authorization
	tamperedApproval.ApprovalPolicy.RequiredTools = nil
	if err := tamperedApproval.ValidateForAt(metadata, lease, testNow.Add(time.Second)); err == nil ||
		!strings.Contains(err.Error(), "approval policy digest") {
		t.Fatalf("tampered approval validation error = %v", err)
	}

	staleProfile := profile
	staleProfile.ToolPolicyDigest = testSHA256("stale-tool-policy")
	if err := authorization.ValidateProfile(staleProfile); err == nil {
		t.Fatal("authorization unexpectedly matched a stale runtime profile")
	}
}

func TestMCPPolicyRequiresPermissionCapability(t *testing.T) {
	brokered := MCPToolDescriptor{Source: MCPToolSourceBrokeredBuiltin}
	providerNative := MCPToolDescriptor{Source: MCPToolSourceProviderNative}
	for _, test := range []struct {
		name       string
		toolPolicy MCPToolPolicy
		approval   MCPApprovalPolicy
		want       bool
	}{
		{name: "approval-free brokered tool", toolPolicy: MCPToolPolicy{Tools: []MCPToolDescriptor{brokered}}},
		{name: "approval-required brokered tool", toolPolicy: MCPToolPolicy{Tools: []MCPToolDescriptor{brokered}}, approval: MCPApprovalPolicy{RequiredTools: []string{"lookup"}}, want: true},
		{name: "provider-native tool", toolPolicy: MCPToolPolicy{Tools: []MCPToolDescriptor{providerNative}}, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := MCPPolicyRequiresPermissionCapability(test.toolPolicy, test.approval); got != test.want {
				t.Fatalf("MCPPolicyRequiresPermissionCapability() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestMCPBrokerCallRequiresRunningLeaseAllowlistAndApproval(t *testing.T) {
	metadata := testMutationMetadata(t, true)
	metadata.OperationID = "mcp-call-1"
	metadata.ExpiresAt = testNow.Add(30 * time.Second)
	lease := PromptLease{Generation: 3, IssuedAt: testNow, ExpiresAt: testNow.Add(2 * time.Minute)}
	authorization := testMCPAuthorizationWithTools(t, metadata, lease)
	request := MCPBrokerCallRequest{
		Protocol: ProtocolVersion, Namespace: "default", SessionState: RuntimeSessionStatePromptRunning,
		Metadata: metadata, Lease: lease, Authorization: authorization,
		Call: MCPToolCall{CallID: "call-1", ToolName: "mutate", Arguments: json.RawMessage(`{"value":"x"}`)},
	}
	sealRequest(t, request, &request.Metadata.RequestDigest)
	if _, err := request.ValidateAt(testNow.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "requires approval") {
		t.Fatalf("missing approval validation error = %v", err)
	}

	request.Call.Approval = &MCPApprovalEvidence{
		PermissionRequestID: "permission-1", ToolCallID: "provider-tool-call-1", ToolName: "mutate",
		GrantedAt: testNow, ExpiresAt: testNow.Add(time.Minute),
	}
	sealRequest(t, request, &request.Metadata.RequestDigest)
	descriptor, err := request.ValidateAt(testNow.Add(time.Second))
	if err != nil {
		t.Fatalf("ValidateAt() error = %v", err)
	}
	if descriptor.Effect != MCPToolEffectConsequential {
		t.Fatalf("descriptor effect = %q", descriptor.Effect)
	}

	idle := request
	idle.SessionState = RuntimeSessionStateIdle
	sealRequest(t, idle, &idle.Metadata.RequestDigest)
	if _, err := idle.ValidateAt(testNow.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "prompt_running") {
		t.Fatalf("idle call validation error = %v", err)
	}

	disallowed := request
	disallowed.Call.ToolName = "not-allowed"
	disallowed.Call.Approval = nil
	sealRequest(t, disallowed, &disallowed.Metadata.RequestDigest)
	if _, err := disallowed.ValidateAt(testNow.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "not an allowed brokered tool") {
		t.Fatalf("disallowed call validation error = %v", err)
	}

	expired := request
	expired.Authorization.ExpiresAt = testNow
	sealRequest(t, expired, &expired.Metadata.RequestDigest)
	if _, err := expired.ValidateAt(testNow.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired call validation error = %v", err)
	}
}

func TestMCPToolPolicyRequiresSortedCompleteDescriptors(t *testing.T) {
	policy := testMCPAuthorizationWithTools(t, testMutationMetadata(t, true), PromptLease{
		Generation: 3, IssuedAt: testNow, ExpiresAt: testNow.Add(time.Minute),
	}).ToolPolicy
	if err := policy.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	missing := policy
	missing.Tools = missing.Tools[:1]
	missing.DescriptorDigest, _ = CanonicalMCPToolDescriptorDigest(missing.Tools)
	if err := missing.Validate(); err == nil || !strings.Contains(err.Error(), "has no canonical descriptor") {
		t.Fatalf("missing descriptor validation error = %v", err)
	}

	unsorted := policy
	unsorted.Tools = append([]MCPToolDescriptor(nil), policy.Tools...)
	unsorted.Tools[0], unsorted.Tools[1] = unsorted.Tools[1], unsorted.Tools[0]
	unsorted.DescriptorDigest, _ = CanonicalMCPToolDescriptorDigest(unsorted.Tools)
	if err := unsorted.Validate(); err == nil || !strings.Contains(err.Error(), "sorted") {
		t.Fatalf("unsorted descriptor validation error = %v", err)
	}
}

func testMCPAuthorizationWithTools(t *testing.T, metadata MutationMetadata, lease PromptLease) PromptMCPAuthorization {
	t.Helper()
	descriptors := []MCPToolDescriptor{
		{Name: "lookup", Description: "Read data", InputSchema: json.RawMessage(`{"type":"object"}`), Source: MCPToolSourceBrokeredBuiltin, Effect: MCPToolEffectReadOnly},
		{Name: "mutate", Description: "Change data", InputSchema: json.RawMessage(`{"type":"object","properties":{"value":{"type":"string"}}}`), Source: MCPToolSourceBrokeredCustom, Effect: MCPToolEffectConsequential, DefinitionDigest: testSHA256("mutate-definition")},
	}
	descriptorDigest, err := CanonicalMCPToolDescriptorDigest(descriptors)
	if err != nil {
		t.Fatal(err)
	}
	toolPolicy := MCPToolPolicy{
		AllowedToolNames: []string{"lookup", "mutate"}, AllowBash: true,
		Tools: descriptors, DescriptorDigest: descriptorDigest,
	}
	approvalPolicy := MCPApprovalPolicy{RequiredTools: []string{"mutate"}}
	toolDigest, err := CanonicalRuntimeToolPolicyDigest(toolPolicy.AllowedToolNames, toolPolicy.DisallowedToolNames, toolPolicy.AllowBash)
	if err != nil {
		t.Fatal(err)
	}
	approvalDigest, err := CanonicalMCPApprovalPolicyDigest(approvalPolicy)
	if err != nil {
		t.Fatal(err)
	}
	mcpDigest, err := CanonicalMCPConfigurationDigest(toolPolicy.AllowedToolNames)
	if err != nil {
		t.Fatal(err)
	}
	return PromptMCPAuthorization{
		RuntimeSessionUID: metadata.Fence.RuntimeSessionUID, SessionGeneration: metadata.Fence.RuntimeSessionGeneration,
		TaskUID: metadata.TaskUID, TaskAttempt: metadata.TaskAttempt, PromptID: metadata.PromptID,
		LeaseGeneration: lease.Generation, ToolPolicyDigest: toolDigest,
		ApprovalPolicyDigest: approvalDigest, MCPConfigurationDigest: mcpDigest,
		ToolPolicy: toolPolicy, ApprovalPolicy: approvalPolicy, ExpiresAt: testNow.Add(time.Minute),
	}
}
