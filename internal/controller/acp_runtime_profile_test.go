package controller

import (
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/labels"
)

func TestEffectiveACPAllowedToolsPreservesDelegatedChildMessagingInjection(t *testing.T) {
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
		DefaultAllowedTools: []string{providerNativeToolRead},
	}}}
	tests := []struct {
		name string
		task *corev1alpha1.Task
		want []string
	}{
		{
			name: "delegated child",
			task: &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{labels.LabelParentTask: "parent"},
			}},
			want: []string{providerNativeToolRead, "check_messages", "send_message"},
		},
		{
			name: "task override remains authoritative before child injection",
			task: &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labels.LabelParentTask: "parent"}},
				Spec: corev1alpha1.TaskSpec{AgentRuntime: &corev1alpha1.AgentRuntimeSpec{
					AllowedTools: []string{providerNativeToolWrite},
				}},
			},
			want: []string{providerNativeToolWrite, "check_messages", "send_message"},
		},
		{
			name: "explicitly disabled child injection",
			task: &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{labels.LabelParentTask: "parent"},
				Annotations: map[string]string{labels.AnnotationDisableCoordinationToolInject: scheduledRunLabelValue},
			}},
			want: []string{providerNativeToolRead},
		},
		{
			name: "non-child",
			task: &corev1alpha1.Task{},
			want: []string{providerNativeToolRead},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := effectiveACPAllowedTools(test.task, agent); !slices.Equal(got, test.want) {
				t.Fatalf("effectiveACPAllowedTools() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestEffectiveACPAllowedToolsPreservesOpenCodeExplicitEmptyChildPolicy(t *testing.T) {
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
		Type: corev1alpha1.AgentRuntimeOpencode,
	}}}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labels.LabelParentTask: "parent"}},
		Spec:       corev1alpha1.TaskSpec{AgentRuntime: &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{}}},
	}
	if got, want := effectiveACPAllowedTools(task, agent), []string{"check_messages", "send_message"}; !slices.Equal(got, want) {
		t.Fatalf("delegated OpenCode child tools = %#v, want %#v", got, want)
	}

	task.Annotations = map[string]string{labels.AnnotationDisableCoordinationToolInject: scheduledRunLabelValue}
	got := effectiveACPAllowedTools(task, agent)
	if got == nil || len(got) != 0 {
		t.Fatalf("disabled delegated OpenCode child tools = %#v, want explicit empty", got)
	}
}

func TestEffectiveACPAllowedToolsUsesMaterializedRuntimeRefPolicyForDelegatedChild(t *testing.T) {
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
		RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
	}}}
	for _, tt := range []struct {
		name    string
		allowed []string
	}{
		{name: "registered messaging tools", allowed: []string{"check_messages", "send_message"}},
		{name: "registered deny all", allowed: []string{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labels.LabelParentTask: "parent"}},
				Spec: corev1alpha1.TaskSpec{AgentRuntime: &corev1alpha1.AgentRuntimeSpec{
					AllowedTools: append([]string{}, tt.allowed...),
				}},
			}
			got := effectiveACPAllowedTools(task, agent)
			if !slices.Equal(got, tt.allowed) {
				t.Fatalf("effectiveACPAllowedTools() = %#v, want %#v", got, tt.allowed)
			}
			if got == nil {
				t.Fatal("effectiveACPAllowedTools() = nil, want explicit list")
			}
		})
	}
}

func TestPlanACPRuntimeHashesNormalizedDenyOnlyProviderNativePolicy(t *testing.T) {
	tests := []struct {
		name           string
		provider       corev1alpha1.AgentRuntimeType
		images         ACPRuntimeImages
		disallowed     []string
		wantAllowed    []string
		wantDisallowed []string
	}{
		{
			name: "claude", provider: corev1alpha1.AgentRuntimeClaude,
			images:         ACPRuntimeImages{Claude: "docker.io/example/claude@sha256:" + strings.Repeat("a", 64)},
			disallowed:     []string{"write"},
			wantAllowed:    []string{providerNativeToolBash, providerNativeToolEdit, providerNativeToolGlob, providerNativeToolGrep, providerNativeToolRead, providerNativeToolWebFetch, providerNativeToolWebSearch},
			wantDisallowed: []string{"write"},
		},
		{
			name: "copilot", provider: corev1alpha1.AgentRuntimeCopilot,
			images:         ACPRuntimeImages{Copilot: "docker.io/example/copilot@sha256:" + strings.Repeat("b", 64)},
			disallowed:     []string{"write", providerNativeToolWebSearch},
			wantAllowed:    []string{providerNativeToolBash, providerNativeToolEdit, providerNativeToolGlob, providerNativeToolGrep, providerNativeToolRead, providerNativeToolWebFetch},
			wantDisallowed: []string{providerNativeToolWebSearch, "write"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
				AgentRuntime: &corev1alpha1.AgentRuntimeSpec{
					DisallowedTools: test.disallowed,
				},
			}}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 1},
				Spec: corev1alpha1.AgentSpec{
					Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
					Runtime: &corev1alpha1.AgentCLIRuntime{
						Type:            test.provider,
						ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
					},
				},
			}
			plan, err := PlanACPRuntime(task, agent, test.images)
			if err != nil {
				t.Fatal(err)
			}
			wantToolDigest, err := harnessv2.CanonicalRuntimeToolPolicyDigest(test.wantAllowed, test.wantDisallowed, true)
			if err != nil {
				t.Fatal(err)
			}
			wantMCPDigest, err := harnessv2.CanonicalMCPConfigurationDigest(test.wantAllowed)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Profile.ToolPolicyDigest != wantToolDigest {
				t.Fatalf("tool policy digest = %q, want %q", plan.Profile.ToolPolicyDigest, wantToolDigest)
			}
			if plan.Profile.MCPConfigurationDigest != wantMCPDigest {
				t.Fatalf("MCP configuration digest = %q, want %q", plan.Profile.MCPConfigurationDigest, wantMCPDigest)
			}
			allowed, disallowed, allowBash := normalizeACPProviderNativeToolPolicy(
				string(test.provider), nil, test.wantDisallowed, true,
			)
			if !slices.Equal(allowed, test.wantAllowed) || !slices.Equal(disallowed, test.wantDisallowed) || !allowBash {
				t.Fatalf("normalized policy = allowed=%v disallowed=%v allowBash=%t", allowed, disallowed, allowBash)
			}
		})
	}
}

func TestPlanACPRuntimeTreatsExplicitEmptyDisallowedAsUnrestricted(t *testing.T) {
	for _, tt := range []struct {
		name    string
		runtime corev1alpha1.AgentRuntimeType
		images  ACPRuntimeImages
	}{
		{name: "codex", runtime: corev1alpha1.AgentRuntimeCodex, images: ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)}},
		{name: "claude", runtime: corev1alpha1.AgentRuntimeClaude, images: ACPRuntimeImages{Claude: "docker.io/example/claude@sha256:" + strings.Repeat("b", 64)}},
		{name: "copilot", runtime: corev1alpha1.AgentRuntimeCopilot, images: ACPRuntimeImages{Copilot: "docker.io/example/copilot@sha256:" + strings.Repeat("c", 64)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type:         corev1alpha1.TaskTypeAgent,
				AgentRuntime: &corev1alpha1.AgentRuntimeSpec{DisallowedTools: []string{}},
			}}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 1},
				Spec: corev1alpha1.AgentSpec{Model: &corev1alpha1.ModelConfig{Name: acpTestModel}, Runtime: &corev1alpha1.AgentCLIRuntime{
					Type:            tt.runtime,
					ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
				}},
			}
			if _, err := PlanACPRuntime(task, agent, tt.images); err != nil {
				t.Fatalf("PlanACPRuntime() rejected explicit deny-none policy: %v", err)
			}
		})
	}
}

func TestPlanACPRuntimeRejectsCodexExplicitEmptyProviderNativePolicy(t *testing.T) {
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
		Type: corev1alpha1.TaskTypeAgent,
		AgentRuntime: &corev1alpha1.AgentRuntimeSpec{
			AllowedTools: []string{},
		},
	}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeCodex,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	_, err := PlanACPRuntime(
		task, agent,
		ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("c", 64)},
	)
	if err == nil || !strings.Contains(err.Error(), "cannot exactly enforce") {
		t.Fatalf("Codex explicit-empty policy error = %v, want fail-closed rejection", err)
	}
}

func TestPlanACPRuntimeRejectsCodexDenyOnlyProviderNativePolicy(t *testing.T) {
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
		Type: corev1alpha1.TaskTypeAgent,
		AgentRuntime: &corev1alpha1.AgentRuntimeSpec{
			DisallowedTools: []string{providerNativeToolWrite},
		},
	}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeCodex,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	_, err := PlanACPRuntime(
		task, agent,
		ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("c", 64)},
	)
	if err == nil || !strings.Contains(err.Error(), "cannot exactly enforce") {
		t.Fatalf("Codex deny-only policy error = %v, want fail-closed rejection", err)
	}
}

func TestPlanACPRuntimeRejectsCopilotDenyOnlyPolicyThatRetainsWebSearch(t *testing.T) {
	allowBash := false
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
		Type: corev1alpha1.TaskTypeAgent,
		AgentRuntime: &corev1alpha1.AgentRuntimeSpec{
			DisallowedTools: []string{providerNativeToolWrite}, AllowBash: &allowBash,
		},
	}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeCopilot,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	_, err := PlanACPRuntime(
		task, agent,
		ACPRuntimeImages{Copilot: "docker.io/example/copilot@sha256:" + strings.Repeat("d", 64)},
	)
	if err == nil || !strings.Contains(err.Error(), providerNativeToolWebSearch) {
		t.Fatalf("Copilot deny-only WebSearch error = %v, want fail-closed rejection", err)
	}
}

func TestPlanACPRuntimeDeterministicAndIntentScoped(t *testing.T) {
	maxTurns := int32(20)
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 3},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type: corev1alpha1.AgentRuntimeCodex, DefaultMaxTurns: &maxTurns, DefaultReasoningEffort: "high",
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
		Type: corev1alpha1.TaskTypeAgent, Workspace: &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentRead},
	}}
	images := ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)}
	first, err := PlanACPRuntime(task, agent, images)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanACPRuntime(task, agent, images)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest || first.PoolName != second.PoolName {
		t.Fatalf("runtime plan is not deterministic: %#v %#v", first, second)
	}
	if first.Profile.WorkspaceIntent != "read" || first.Profile.AdapterDigests["codex-acp"] != "sha256:"+acp.CodexACPTarSHA256 ||
		first.Profile.AdapterDigests["codex-acp-orka-patch"] != "sha256:"+acp.CodexACPOrkaPatchSHA256 ||
		first.Profile.AdapterDigests["codex-acp-orka-dist"] != "sha256:"+acp.CodexACPOrkaDistSHA256 {
		t.Fatalf("unexpected profile: %#v", first.Profile)
	}
	overrideTurns := int32(21)
	task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{MaxTurns: &overrideTurns}
	overridePlan, err := PlanACPRuntime(task, agent, images)
	if err != nil {
		t.Fatal(err)
	}
	if overridePlan.Digest == first.Digest {
		t.Fatal("maxTurns override did not rotate the Agent-configuration-bound runtime profile")
	}
	task.Spec.AgentRuntime = nil
	task.Spec.Workspace.Intent = corev1alpha1.WorkspaceIntentWrite
	writePlan, err := PlanACPRuntime(task, agent, images)
	if err != nil {
		t.Fatal(err)
	}
	if writePlan.Digest == first.Digest || writePlan.PoolName == first.PoolName {
		t.Fatal("workspace intent did not rotate runtime profile")
	}
}

func TestPlanACPRuntimeRequiresModelAndPinnedImage(t *testing.T) {
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
		Type:            corev1alpha1.AgentRuntimeCodex,
		ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
	}}}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	if _, err := PlanACPRuntime(task, agent, ACPRuntimeImages{}); err == nil {
		t.Fatal("missing model unexpectedly accepted")
	}
	agent.Spec.Model = &corev1alpha1.ModelConfig{Name: acpTestModel}
	if _, err := PlanACPRuntime(task, agent, ACPRuntimeImages{Codex: "docker.io/example/codex:latest"}); err == nil {
		t.Fatal("mutable image unexpectedly accepted")
	}
}

func TestPlanACPRuntimeSelectsCopilotProfileAndPinnedImage(t *testing.T) {
	image := "docker.io/example/copilot@sha256:" + strings.Repeat("c", 64)
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("copilot-agent"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: "gpt-5"},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeCopilot,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}

	plan, err := PlanACPRuntime(task, agent, ACPRuntimeImages{Copilot: image})
	if err != nil {
		t.Fatalf("PlanACPRuntime() error = %v", err)
	}
	if plan.Image != image {
		t.Fatalf("image = %q, want %q", plan.Image, image)
	}
	if plan.Profile.ProviderKind != string(corev1alpha1.AgentRuntimeCopilot) {
		t.Fatalf("provider kind = %q, want copilot", plan.Profile.ProviderKind)
	}
	if plan.Profile.AdapterDigests["copilot-cli-linux-amd64"] != "sha256:"+acp.CopilotCLILinuxX64SHA256 ||
		plan.Profile.AdapterDigests["copilot-cli-linux-arm64"] != "sha256:"+acp.CopilotCLILinuxARM64SHA256 {
		t.Fatalf("unexpected Copilot adapter digests: %#v", plan.Profile.AdapterDigests)
	}

	if _, err := PlanACPRuntime(task, agent, ACPRuntimeImages{}); err == nil || !strings.Contains(err.Error(), "digest-pinned image") {
		t.Fatalf("missing Copilot image error = %v, want digest-pinning rejection", err)
	}
}

func TestPlanACPRuntimePreservesExplicitEmptyAllowedTools(t *testing.T) {
	image := "docker.io/example/claude@sha256:" + strings.Repeat("a", 64)
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: "claude-test"},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeClaude,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	omitted, err := PlanACPRuntime(task, agent, ACPRuntimeImages{Claude: image})
	if err != nil {
		t.Fatal(err)
	}
	if got := effectiveACPAllowedTools(task, agent); got != nil {
		t.Fatalf("omitted allowed tools = %#v, want nil", got)
	}

	task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{}, AllowBash: new(false)}
	explicitEmpty, err := PlanACPRuntime(task, agent, ACPRuntimeImages{Claude: image})
	if err != nil {
		t.Fatal(err)
	}
	if got := effectiveACPAllowedTools(task, agent); got == nil || len(got) != 0 {
		t.Fatalf("explicit empty allowed tools = %#v, want non-nil empty", got)
	}
	if omitted.Profile.ToolPolicyDigest == explicitEmpty.Profile.ToolPolicyDigest || omitted.Digest == explicitEmpty.Digest {
		t.Fatal("explicit deny-all tools collapsed into omitted/unrestricted ACP policy")
	}
}

func TestPlanACPRuntimeOpenCodeUsesNativeACPImage(t *testing.T) {
	imageDigest := "sha256:" + strings.Repeat("d", 64)
	image := "docker.io/example/opencode@" + imageDigest
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("opencode-agent-uid"), Generation: 2},
		Spec: corev1alpha1.AgentSpec{
			Model: testOpenCodeModelConfig(),
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeOpencode,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}

	plan, err := PlanACPRuntime(task, agent, ACPRuntimeImages{Opencode: image})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Image != image {
		t.Fatalf("image = %q, want %q", plan.Image, image)
	}
	if plan.Profile.ProviderKind != string(corev1alpha1.AgentRuntimeOpencode) || plan.Profile.Model != "openai/gpt-test" {
		t.Fatalf("unexpected OpenCode profile: %#v", plan.Profile)
	}
	if plan.Profile.ModelLimits == nil || plan.Profile.ModelLimits.Context != int64(testOpenCodeContextWindow) ||
		plan.Profile.ModelLimits.Output != int64(testOpenCodeMaxTokens) {
		t.Fatalf("OpenCode model limits = %#v", plan.Profile.ModelLimits)
	}
	profileSpec := RuntimePoolProfileFromPlan(plan)
	if profileSpec.ModelLimits == nil || profileSpec.ModelLimits.Context != int64(testOpenCodeContextWindow) ||
		profileSpec.ModelLimits.Output != int64(testOpenCodeMaxTokens) {
		t.Fatalf("RuntimePool model limits = %#v", profileSpec.ModelLimits)
	}
	dispatchProfile := runtimeProfileFromPool(profileSpec)
	if dispatchProfile.ModelLimits == nil || dispatchProfile.ModelLimits.Context != int64(testOpenCodeContextWindow) ||
		dispatchProfile.ModelLimits.Output != int64(testOpenCodeMaxTokens) {
		t.Fatalf("dispatcher model limits = %#v", dispatchProfile.ModelLimits)
	}
	dispatchDigest, err := harnessv2.CanonicalProfileDigest(dispatchProfile)
	if err != nil {
		t.Fatal(err)
	}
	if dispatchDigest != plan.Digest {
		t.Fatalf("dispatcher profile digest = %q, want %q", dispatchDigest, plan.Digest)
	}
	wantAdapterDigests := map[string]string{
		"opencode-cli-linux-amd64":     "sha256:" + acp.OpenCodeLinuxX64BinarySHA256,
		"opencode-cli-linux-arm64":     "sha256:" + acp.OpenCodeLinuxARM64BinarySHA256,
		"opencode-ripgrep-linux-amd64": "sha256:" + acp.OpenCodeRipgrepLinuxX64BinarySHA256,
		"opencode-ripgrep-linux-arm64": "sha256:" + acp.OpenCodeRipgrepLinuxARM64BinarySHA256,
		"acp-schema":                   "sha256:" + acp.ACPSchemaSHA256,
	}
	if !reflect.DeepEqual(plan.Profile.AdapterDigests, wantAdapterDigests) {
		t.Fatalf("adapter digests = %#v, want %#v", plan.Profile.AdapterDigests, wantAdapterDigests)
	}
	if !strings.HasPrefix(plan.PoolName, "acp-opencode-") {
		t.Fatalf("pool name = %q, want OpenCode prefix", plan.PoolName)
	}

	fallbackAgent := agent.DeepCopy()
	fallbackAgent.Spec.Model.Fallbacks = []corev1alpha1.ModelFallback{{ProviderRef: "fallback-provider", Model: "fallback-model"}}
	if _, err := PlanACPRuntime(task, fallbackAgent, ACPRuntimeImages{Opencode: image}); err == nil || !strings.Contains(err.Error(), "spec.model.fallbacks") {
		t.Fatalf("OpenCode fallbacks error = %v, want unsupported fallbacks rejection", err)
	}

	providerRefAgent := agent.DeepCopy()
	providerRefAgent.Spec.ProviderRef = &corev1alpha1.ProviderReference{Name: "allowed-provider"}
	if _, err := PlanACPRuntime(task, providerRefAgent, ACPRuntimeImages{Opencode: image}); err == nil || !strings.Contains(err.Error(), "does not accept spec.providerRef") {
		t.Fatalf("OpenCode providerRef error = %v, want providerRef rejection", err)
	}

	changedAgent := agent.DeepCopy()
	changedContext := *changedAgent.Spec.Model.ContextWindow + 1
	changedAgent.Spec.Model.ContextWindow = &changedContext
	changedPlan, err := PlanACPRuntime(task, changedAgent, ACPRuntimeImages{Opencode: image})
	if err != nil {
		t.Fatal(err)
	}
	if changedPlan.Digest == plan.Digest || changedPlan.PoolName == plan.PoolName {
		t.Fatal("changing OpenCode model limits did not rotate the immutable RuntimePool profile")
	}

	if _, err := PlanACPRuntime(task, agent, ACPRuntimeImages{Opencode: "docker.io/example/opencode:latest"}); err == nil ||
		!strings.Contains(err.Error(), "digest-pinned image") {
		t.Fatalf("mutable OpenCode image error = %v, want digest-pinned rejection", err)
	}

	reasoningAgent := agent.DeepCopy()
	reasoningAgent.Spec.Runtime.DefaultReasoningEffort = agentReasoningEffortHigh
	if _, err := PlanACPRuntime(task, reasoningAgent, ACPRuntimeImages{Opencode: image}); err == nil ||
		!strings.Contains(err.Error(), "does not support spec.runtime.defaultReasoningEffort") {
		t.Fatalf("OpenCode reasoning-effort error = %v, want unsupported-setting rejection", err)
	}
}

func TestACPRuntimeImageAvailableRejectsPlaceholderAndMutableReferences(t *testing.T) {
	valid := "docker.io/example/opencode@sha256:" + strings.Repeat("a", 64)
	if !ACPRuntimeImageAvailable(valid) {
		t.Fatalf("ACPRuntimeImageAvailable(%q) = false", valid)
	}
	for _, image := range []string{
		"",
		"docker.io/example/opencode:latest",
		"docker.io/example/opencode@sha256:" + strings.Repeat("0", 64),
	} {
		if ACPRuntimeImageAvailable(image) {
			t.Fatalf("ACPRuntimeImageAvailable(%q) = true, want false", image)
		}
	}
}

func TestRuntimePoolProfileFromPlan(t *testing.T) {
	plan := ACPRuntimePlan{Profile: harnessProfileForTest(), Digest: harnessv2.ProfileDigest("sha256:" + strings.Repeat("b", 64))}
	profile := RuntimePoolProfileFromPlan(plan)
	if profile.ProviderKind != plan.Profile.ProviderKind || profile.Digest != string(plan.Digest) || profile.DigestSchemaVersion != strconv.FormatUint(uint64(harnessv2.ProfileDigestSchemaVersion), 10) {
		t.Fatalf("unexpected API profile: %#v", profile)
	}
}

func harnessProfileForTest() harnessv2.RuntimeProfile {
	digest := "sha256:" + strings.Repeat("c", 64)
	return harnessv2.RuntimeProfile{
		ACPProfile: harnessv2.ACPProfileV1, AdapterDigests: map[string]string{"adapter": digest},
		ProviderKind: "codex", Model: acpTestModel, AgentConfigurationDigest: digest,
		ToolPolicyDigest: digest, ApprovalPolicyDigest: digest, MCPConfigurationDigest: digest,
		WorkspaceIntent: harnessv2.WorkspaceIntentRead, ProxyCredentialRole: "provider",
		ProxyCredentialScope: "model:gpt-test", ResourceClass: "standard",
	}
}

func TestCurrentACPRuntimeDeliveryPlanRequiresCompatibleAdapters(t *testing.T) {
	oldImage := "docker.io/example/codex@sha256:" + strings.Repeat("a", 64)
	newImage := "docker.io/example/codex@sha256:" + strings.Repeat("b", 64)
	buildPlan := func(profile harnessv2.RuntimeProfile) ACPRuntimePlan {
		t.Helper()
		digest, err := harnessv2.CanonicalProfileDigest(profile)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
			"profileDigest": string(digest), "runtimeImage": oldImage,
		})
		if err != nil {
			t.Fatal(err)
		}
		return ACPRuntimePlan{
			PoolName: acpRuntimePoolName(profile.ProviderKind, harnessv2.ProfileDigest(identity)),
			Image:    oldImage, Profile: profile, Digest: digest,
		}
	}

	compatibleProfile := harnessProfileForTest()
	compatibleProfile.AdapterDigests = acp.BuiltInRuntimeAdapterDigests(compatibleProfile.ProviderKind)
	compatible := buildPlan(compatibleProfile)
	rotatedSelection, err := currentACPRuntimeDeliveryPlan(compatible, ACPRuntimeImages{Codex: newImage})
	if err != nil {
		t.Fatal(err)
	}
	rotated := rotatedSelection.plan
	if !rotatedSelection.allowPoolCreation {
		t.Fatal("compatible plain-pool image rotation unexpectedly forbids pool creation")
	}
	if rotated.Image != newImage || rotated.PoolName == compatible.PoolName || rotated.Digest != compatible.Digest {
		t.Fatalf("compatible image rotation = %#v, want new image/pool with frozen profile digest %q", rotated, compatible.Digest)
	}

	incompatibleProfile := compatibleProfile
	incompatibleProfile.AdapterDigests = cloneMap(compatibleProfile.AdapterDigests)
	incompatibleProfile.AdapterDigests["codex-acp"] = "sha256:" + strings.Repeat("9", 64)
	incompatible := buildPlan(incompatibleProfile)
	if _, err := currentACPRuntimeDeliveryPlan(incompatible, ACPRuntimeImages{Codex: newImage}); err == nil ||
		!strings.Contains(err.Error(), "do not match the frozen runtime profile") {
		t.Fatalf("incompatible adapter rotation error = %v, want frozen-profile rejection", err)
	}
	if _, err := currentACPRuntimeDeliveryPlan(compatible, ACPRuntimeImages{}); err == nil ||
		!strings.Contains(err.Error(), "configured digest-pinned image") {
		t.Fatalf("missing approved image error = %v, want configured-image rejection", err)
	}

	workspace := compatible
	workspace.PoolName = "acp-ws-codex-0123456789abcdef"
	workspace.Workspace = &ACPRuntimeWorkspaceBinding{
		Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
		BindingDigest: "sha256:" + strings.Repeat("c", 64),
	}
	currentWorkspace, err := currentACPRuntimeDeliveryPlan(workspace, ACPRuntimeImages{Codex: oldImage})
	if err != nil {
		t.Fatal(err)
	}
	if !currentWorkspace.allowPoolCreation || !reflect.DeepEqual(currentWorkspace.plan, workspace) {
		t.Fatalf("current workspace delivery = %#v, want frozen plan with creation allowed", currentWorkspace)
	}
	retiredWorkspace, err := currentACPRuntimeDeliveryPlan(workspace, ACPRuntimeImages{Codex: newImage})
	if err != nil {
		t.Fatal(err)
	}
	if retiredWorkspace.allowPoolCreation || !reflect.DeepEqual(retiredWorkspace.plan, workspace) {
		t.Fatalf("retired workspace delivery = %#v, want frozen plan with creation forbidden", retiredWorkspace)
	}
	removedWorkspace, err := currentACPRuntimeDeliveryPlan(workspace, ACPRuntimeImages{})
	if err != nil {
		t.Fatal(err)
	}
	if removedWorkspace.allowPoolCreation || !reflect.DeepEqual(removedWorkspace.plan, workspace) {
		t.Fatalf("removed workspace delivery = %#v, want exact-pool-only frozen plan", removedWorkspace)
	}
	incompatibleWorkspace := incompatible
	incompatibleWorkspace.PoolName = workspace.PoolName
	incompatibleWorkspace.Workspace = workspace.Workspace
	if _, err := currentACPRuntimeDeliveryPlan(incompatibleWorkspace, ACPRuntimeImages{Codex: newImage}); err == nil ||
		!strings.Contains(err.Error(), "do not match the frozen runtime profile") {
		t.Fatalf("incompatible workspace adapter error = %v, want frozen-profile rejection", err)
	}
}

func TestPlanACPRuntimePoolIdentityRotatesWithImageDigest(t *testing.T) {
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: acpTestModel},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeClaude,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	first, err := PlanACPRuntime(task, agent, ACPRuntimeImages{
		Claude: "docker.io/example/claude@sha256:" + strings.Repeat("a", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanACPRuntime(task, agent, ACPRuntimeImages{
		Claude: "docker.io/example/claude@sha256:" + strings.Repeat("b", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest {
		t.Fatalf("runtime image changed protocol profile digest: first=%q second=%q", first.Digest, second.Digest)
	}
	if first.PoolName == second.PoolName {
		t.Fatalf("runtime image did not rotate RuntimePool identity: %q", first.PoolName)
	}
}
