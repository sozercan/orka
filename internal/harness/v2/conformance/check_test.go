package conformance_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/harness/v2/conformance"
	"github.com/orka-agents/orka/internal/harness/v2/conformance/conformancetest"
)

func TestCheckPassesStrictHostileLifecycleOnce(t *testing.T) {
	target, config := testTargetAndConfig(t)
	target.ProbeLifecycle = true
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if !result.Passed {
		t.Fatalf("Check() failed: %s", result.Message)
	}
	if !result.LifecycleProbeExecuted {
		t.Fatal("hostile lifecycle probe was not executed")
	}
	if result.ObservedCapabilities == nil || result.ObservedStatus == nil {
		t.Fatalf("observations are incomplete: %#v", result)
	}
	counts := server.Counts()
	if counts.SessionCreates != 2 || counts.PromptStarts != 2 || counts.PromptCancels != 2 || counts.WorkspaceDeltas != 1 || counts.SessionDeletes != 2 {
		t.Fatalf("hostile cycle counts = %#v, want separate completion and cancellation sessions and one workspace delta", counts)
	}
	if counts.ReplayClassifications != 8 || counts.DigestConflicts != 8 {
		t.Fatalf("hostile replay counts = %#v, want eight exact replays and eight digest conflicts", counts)
	}
}

func TestCheckPassesInClusterFixtureMode(t *testing.T) {
	target, config := testTargetAndConfig(t)
	target.ProbeLifecycle = true
	target.ExpectedControllerEpoch = 7
	config.ListenAddress = "127.0.0.1:0"
	config.ControllerEpoch = 7
	config.CompleteNonConformancePrompts = true
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if !result.Passed {
		t.Fatalf("Check() failed: %s", result.Message)
	}
	if counts := server.Counts(); counts.PromptStarts != 2 || counts.PromptCancels != 2 {
		t.Fatalf("in-cluster fixture prompt counts = %#v, want completion and cancellation checks", counts)
	}
}

func TestCheckPromptReplayCompletionRace(t *testing.T) {
	for _, test := range []struct {
		name           string
		beforeConflict bool
		publication    bool
		mutateReplay   func(*harnessv2.PromptAdmissionResponse)
		mutateConflict func(*harnessv2.Classification)
		wantError      string
	}{
		{name: "completes before identical replay"},
		{name: "completes between replay and conflict", beforeConflict: true},
		{name: "completed workspace requires publication finalization", publication: true},
		{
			name: "changed acceptance timestamp",
			mutateReplay: func(admission *harnessv2.PromptAdmissionResponse) {
				admission.AcceptedAt = admission.AcceptedAt.Add(time.Second)
			},
			wantError: "original acceptance",
		},
		{
			name: "changed settlement timestamp",
			mutateReplay: func(admission *harnessv2.PromptAdmissionResponse) {
				admission.Settlement.SettledAt = admission.Settlement.SettledAt.Add(time.Second)
			},
			wantError: "original settlement",
		},
		{
			name: "replay terminal disagrees with settlement",
			mutateReplay: func(admission *harnessv2.PromptAdmissionResponse) {
				admission.Classification.TerminalEvent = harnessv2.EventFailed
			},
			wantError: "settlement terminal event",
		},
		{
			name: "conflict regresses to accepted",
			mutateConflict: func(classification *harnessv2.Classification) {
				classification.Phase = harnessv2.OperationPhaseAccepted
				classification.TerminalEvent = ""
			},
			wantError: "regressed",
		},
		{
			name: "conflict has different terminal event", beforeConflict: true,
			mutateConflict: func(classification *harnessv2.Classification) {
				classification.TerminalEvent = harnessv2.EventCancelled
			},
			wantError: "original settlement",
		},
		{
			name: "settled conflict omits terminal event", beforeConflict: true,
			mutateConflict: func(classification *harnessv2.Classification) {
				classification.TerminalEvent = ""
			},
			wantError: "terminal event",
		},
		{
			name: "conflict uses unrelated phase", beforeConflict: true,
			mutateConflict: func(classification *harnessv2.Classification) {
				classification.Phase = harnessv2.OperationPhaseApplied
				classification.TerminalEvent = ""
			},
			wantError: "conflicting accepted prompt admission",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, config := testTargetAndConfig(t)
			target.ProbeLifecycle = true
			config.CompletePromptBeforeReplay = !test.beforeConflict
			config.CompletePromptBeforeConflict = test.beforeConflict
			if test.publication {
				target.Profile.WorkspaceIntent = harnessv2.WorkspaceIntentWrite
				target.SupportsPublicationFinalization = true
				config.Profile = target.Profile
				config.SupportsPublicationFinalization = true
			}
			server, err := conformancetest.NewServer(config)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			backend, err := url.Parse(server.URL())
			if err != nil {
				t.Fatal(err)
			}
			proxy := httputil.NewSingleHostReverseProxy(backend)
			proxy.ModifyResponse = func(response *http.Response) error {
				if !strings.Contains(response.Request.URL.Path, "/prompts/") ||
					strings.Contains(response.Request.URL.Path, "conformance-session-workspace-") ||
					strings.Contains(response.Request.URL.Path, "/cancel") {
					return nil
				}
				var value any
				switch {
				case response.StatusCode == http.StatusOK && response.Header.Get("Content-Type") == "application/json" && test.mutateReplay != nil:
					var admission harnessv2.PromptAdmissionResponse
					if err := json.NewDecoder(response.Body).Decode(&admission); err != nil {
						return err
					}
					test.mutateReplay(&admission)
					value = admission
				case response.StatusCode == http.StatusConflict && test.mutateConflict != nil:
					var envelope harnessv2.ErrorResponse
					if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
						return err
					}
					test.mutateConflict(envelope.Classification)
					value = envelope
				default:
					return nil
				}
				_ = response.Body.Close()
				body, err := json.Marshal(value)
				if err != nil {
					return err
				}
				response.Body = io.NopCloser(bytes.NewReader(body))
				response.ContentLength = int64(len(body))
				response.Header.Del("Content-Length")
				return nil
			}
			endpoint := httptest.NewServer(proxy)
			defer endpoint.Close()
			target.BaseURL = endpoint.URL
			result := conformance.Check(t.Context(), target)
			if test.wantError != "" {
				if result.Passed || !strings.Contains(result.Message, test.wantError) {
					t.Fatalf("Check() passed=%v message=%q, want error containing %q", result.Passed, result.Message, test.wantError)
				}
				return
			}
			if !result.Passed || !result.LifecycleProbeExecuted {
				t.Fatalf("Check() failed: %s", result.Message)
			}
			if counts := server.Counts(); counts.PromptStarts != 2 || counts.PromptCancels != 2 || counts.WorkspaceDeltas != 2 || counts.SessionDeletes != 2 {
				t.Fatalf("conformance counts = %#v, want two prompt executions, workspace validations, and complete cleanup", counts)
			}
		})
	}
}

func TestCheckRejectsClaimedDuplicateSafetyWithoutReplaySemantics(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*conformancetest.Config)
		want   string
	}{
		{
			name: "identical replay",
			mutate: func(config *conformancetest.Config) {
				config.BreakDuplicateSafeMutations = true
			},
			want: "identical runtime-session creation classified",
		},
		{
			name: "digest conflict",
			mutate: func(config *conformancetest.Config) {
				config.BreakDigestConflictClassification = true
			},
			want: "conflicting runtime-session creation succeeded",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, config := testTargetAndConfig(t)
			target.ProbeLifecycle = true
			test.mutate(&config)
			server, err := conformancetest.NewServer(config)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			target.BaseURL = server.URL()

			result := conformance.Check(t.Context(), target)
			if result.Passed || !strings.Contains(result.Message, test.want) {
				t.Fatalf("Check() = %#v, want duplicate-safety failure containing %q", result, test.want)
			}
		})
	}
}

func TestCheckNeverReconnectsOrReplaysDisconnectedPrompt(t *testing.T) {
	target, config := testTargetAndConfig(t)
	target.ProbeLifecycle = true
	config.DisconnectPromptAfterAccepted = true
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "consume workspace probe prompt stream") {
		t.Fatalf("Check() = %#v, want disconnected-stream failure", result)
	}
	if got := server.Counts().PromptStarts; got != 1 {
		t.Fatalf("prompt starts = %d, want exactly one with no reconnect or replay", got)
	}
}

func TestCheckRejectsUnauthenticatedStatusExposure(t *testing.T) {
	target, config := testTargetAndConfig(t)
	config.AllowUnauthenticatedStatus = true
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "unauthenticated status negative probe") {
		t.Fatalf("Check() = %#v, want status auth-negative failure", result)
	}
}

func TestCheckRequiresExpectedControllerEpoch(t *testing.T) {
	target, _ := testTargetAndConfig(t)
	target.BaseURL = "https://runtime.example.invalid"
	target.ExpectedControllerEpoch = 0

	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "expected controller epoch is required") {
		t.Fatalf("Check() = %#v, want missing expected controller epoch failure", result)
	}
}

func TestCheckRejectsStaleControllerEpoch(t *testing.T) {
	target, config := testTargetAndConfig(t)
	target.ExpectedControllerEpoch++
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "authenticated status controller epoch 1 does not match expected 2") {
		t.Fatalf("Check() = %#v, want stale controller epoch failure", result)
	}
}

func TestCheckRejectsMissingControllerEpochInAuthenticatedStatus(t *testing.T) {
	target, config := testTargetAndConfig(t)
	config.OmitStatusControllerEpoch = true
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "authenticated status probe") ||
		!strings.Contains(result.Message, "controller epoch must be positive") {
		t.Fatalf("Check() = %#v, want missing authenticated status controller epoch failure", result)
	}
}

func TestCheckRejectsAgentSessionConfigurationCapability(t *testing.T) {
	target, config := testTargetAndConfig(t)
	config.SupportsAgentSessionConfiguration = true
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "supportsAgentSessionConfiguration must be false") {
		t.Fatalf("Check() = %#v, want Agent session configuration capability rejection", result)
	}
}

func TestCheckAllowsApprovalFreeBrokeredPolicyWithoutPermissionSupport(t *testing.T) {
	target, config := testTargetAndConfig(t)
	setTestConformanceMCPPolicy(t, &target, &config, harnessv2.MCPApprovalPolicy{})
	supportsPermissions := false
	config.SupportsPermissions = &supportsPermissions
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if !result.Passed {
		t.Fatalf("Check() failed for approval-free brokered policy: %s", result.Message)
	}
}

func TestCheckRejectsApprovalPolicyWithoutPermissionSupport(t *testing.T) {
	target, config := testTargetAndConfig(t)
	setTestConformanceMCPPolicy(t, &target, &config, harnessv2.MCPApprovalPolicy{RequiredTools: []string{"lookup"}})
	supportsPermissions := false
	config.SupportsPermissions = &supportsPermissions
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "policy requires permission support") {
		t.Fatalf("Check() = %#v, want permission-capability rejection", result)
	}
}

func TestCheckExercisesPublicationFinalizationForWriteRuntime(t *testing.T) {
	target, config := testTargetAndConfig(t)
	target.Profile.WorkspaceIntent = harnessv2.WorkspaceIntentWrite
	config.Profile = target.Profile
	target.SupportsPublicationFinalization = true
	target.ProbeLifecycle = true
	config.SupportsPublicationFinalization = true
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()
	result := conformance.Check(t.Context(), target)
	if !result.Passed || !result.LifecycleProbeExecuted {
		t.Fatalf("Check() = %#v, want successful publication-finalization lifecycle", result)
	}
}

func TestCheckRetriesCleanupAfterUnconfirmedSessionDelete(t *testing.T) {
	target, config := testTargetAndConfig(t)
	target.ProbeLifecycle = true
	config.FailFirstSessionDeleteBeforeApply = true
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "delete conformance runtime session") {
		t.Fatalf("Check() = %#v, want injected deletion failure", result)
	}
	counts := server.Counts()
	if counts.SessionDeleteAttempts != 2 || counts.SessionDeletes != 1 {
		t.Fatalf("delete cleanup counts = attempts:%d applied:%d, want attempts:2 applied:1", counts.SessionDeleteAttempts, counts.SessionDeletes)
	}
}

func TestCheckRejectsFreshPublicationFinalizationAppliedAtReplacement(t *testing.T) {
	target, config := testTargetAndConfig(t)
	target.Profile.WorkspaceIntent = harnessv2.WorkspaceIntentWrite
	config.Profile = target.Profile
	target.SupportsPublicationFinalization = true
	target.ProbeLifecycle = true
	config.SupportsPublicationFinalization = true
	config.BreakFreshPublicationFinalizationAppliedAt = true
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "did not preserve the original receipt") {
		t.Fatalf("Check() = %#v, want fresh-recovery AppliedAt failure", result)
	}
}

func TestCheckRejectsConflictingFreshPublicationFinalization(t *testing.T) {
	target, config := testTargetAndConfig(t)
	target.Profile.WorkspaceIntent = harnessv2.WorkspaceIntentWrite
	config.Profile = target.Profile
	target.SupportsPublicationFinalization = true
	target.ProbeLifecycle = true
	config.SupportsPublicationFinalization = true
	config.BreakFreshPublicationFinalizationConflictGuard = true
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()

	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "conflicting fresh publication finalization succeeded") {
		t.Fatalf("Check() = %#v, want conflicting fresh-receipt failure", result)
	}
}

func TestCheckRejectsExactInstanceMismatch(t *testing.T) {
	target, config := testTargetAndConfig(t)
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target.BaseURL = server.URL()
	target.ExpectedRuntimeInstanceID = "different-runtime-instance"

	// The instance mismatch now trips the status capability binding (the
	// probe binds the expected instance, the supervisor verifies its own), so
	// the authenticated status probe is rejected before the exact-status
	// fence comparison runs.
	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "status") {
		t.Fatalf("Check() = %#v, want exact-instance status-probe failure", result)
	}
}

func TestCheckRejectsTrustedRuntimeWithoutExplicitTrust(t *testing.T) {
	target, _ := testTargetAndConfig(t)
	target.BaseURL = "http://example.invalid"
	target.WorkspaceGovernance = conformance.WorkspaceGovernanceClaims{Mode: conformance.WorkspaceGovernanceTrusted}
	result := conformance.Check(t.Context(), target)
	if result.Passed || !strings.Contains(result.Message, "explicitly marked trusted") {
		t.Fatalf("Check() = %#v, want explicit-trust failure", result)
	}
}

func testTargetAndConfig(t *testing.T) (conformance.Target, conformancetest.Config) {
	t.Helper()
	toolPolicy := harnessv2.MCPToolPolicy{AllowedToolNames: []string{}, Tools: []harnessv2.MCPToolDescriptor{}}
	toolPolicy.DescriptorDigest, _ = harnessv2.CanonicalMCPToolDescriptorDigest(toolPolicy.Tools)
	approvalPolicy := harnessv2.MCPApprovalPolicy{}
	toolPolicyDigest, _ := harnessv2.CanonicalRuntimeToolPolicyDigest(toolPolicy.AllowedToolNames, toolPolicy.DisallowedToolNames, toolPolicy.AllowBash)
	approvalPolicyDigest, _ := harnessv2.CanonicalMCPApprovalPolicyDigest(approvalPolicy)
	mcpConfigurationDigest, _ := harnessv2.CanonicalMCPConfigurationDigest(toolPolicy.AllowedToolNames)
	profile := harnessv2.RuntimeProfile{
		ACPProfile:               harnessv2.ACPProfileV1,
		AdapterDigests:           map[string]string{"codex": testDigest("adapter")},
		ProviderKind:             "codex",
		Model:                    "gpt-test",
		AgentConfigurationDigest: testDigest("agent"),
		ToolPolicyDigest:         toolPolicyDigest,
		ApprovalPolicyDigest:     approvalPolicyDigest,
		MCPConfigurationDigest:   mcpConfigurationDigest,
		WorkspaceIntent:          harnessv2.WorkspaceIntentRead,
		ProxyCredentialRole:      "provider-proxy",
		ProxyCredentialScope:     "session-and-prompt",
		ResourceClass:            "standard",
	}
	limits := harnessv2.DefaultProtocolLimits()
	claims := conformance.WorkspaceGovernanceClaims{
		Mode:                            conformance.WorkspaceGovernanceStrict,
		OrkaOwnedWorkspaceDeltas:        true,
		PromptScopedBrokerAuthorization: true,
		NoDirectSCMPublication:          true,
		OrkaOwnedCleanRoomPublication:   true,
		ExactInstanceFencing:            true,
		DuplicateSafeMutations:          true,
		CancellationSettlement:          true,
	}
	token := strings.Repeat("t", 32)
	secret := []byte(strings.Repeat("s", 32))
	config := conformancetest.Config{
		ControllerBearerToken:     token,
		OperationCapabilitySecret: secret,
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	target := conformance.Target{
		ControllerBearerToken:     token,
		OperationCapabilitySecret: secret,
		ControlTimeout:            10 * time.Second,
		ExpectedRuntimeInstanceID: config.RuntimeInstanceID,
		ExpectedControllerEpoch:   1,
		Profile:                   profile,
		ToolPolicy:                toolPolicy,
		ApprovalPolicy:            approvalPolicy,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	return target, config
}

func setTestConformanceMCPPolicy(
	t *testing.T,
	target *conformance.Target,
	config *conformancetest.Config,
	approvalPolicy harnessv2.MCPApprovalPolicy,
) {
	t.Helper()
	tool := harnessv2.MCPToolDescriptor{
		Name: "lookup", Description: "look up a test value", InputSchema: []byte(`{"type":"object"}`),
		Source: harnessv2.MCPToolSourceBrokeredBuiltin, Effect: harnessv2.MCPToolEffectReadOnly,
	}
	toolPolicy := harnessv2.MCPToolPolicy{AllowedToolNames: []string{tool.Name}, Tools: []harnessv2.MCPToolDescriptor{tool}}
	var err error
	toolPolicy.DescriptorDigest, err = harnessv2.CanonicalMCPToolDescriptorDigest(toolPolicy.Tools)
	if err != nil {
		t.Fatal(err)
	}
	target.Profile.ToolPolicyDigest, err = harnessv2.CanonicalRuntimeToolPolicyDigest(
		toolPolicy.AllowedToolNames, toolPolicy.DisallowedToolNames, toolPolicy.AllowBash,
	)
	if err != nil {
		t.Fatal(err)
	}
	target.Profile.ApprovalPolicyDigest, err = harnessv2.CanonicalMCPApprovalPolicyDigest(approvalPolicy)
	if err != nil {
		t.Fatal(err)
	}
	target.Profile.MCPConfigurationDigest, err = harnessv2.CanonicalMCPConfigurationDigest(toolPolicy.AllowedToolNames)
	if err != nil {
		t.Fatal(err)
	}
	target.ToolPolicy = toolPolicy
	target.ApprovalPolicy = approvalPolicy
	config.Profile = target.Profile
}

func testDigest(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return "sha256:" + hex.EncodeToString(sum[:])
}
