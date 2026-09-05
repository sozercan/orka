package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

func TestBuildACPAgentSessionConfigurationRejectsUnsupportedModelControls(t *testing.T) {
	temperature := 0.2
	contextWindow := int32(32768)
	maxTokens := int32(128)
	fallbacks := []corev1alpha1.ModelFallback{{ProviderRef: "fallback-provider", Model: "fallback-model"}}
	opencodeTemperature := testOpenCodeModelConfig()
	opencodeTemperature.Temperature = &temperature
	opencodeFallbacks := testOpenCodeModelConfig()
	opencodeFallbacks.Fallbacks = fallbacks
	for _, tt := range []struct {
		name      string
		runtime   corev1alpha1.AgentRuntimeType
		model     *corev1alpha1.ModelConfig
		wantError string
	}{
		{
			name:      "codex temperature",
			runtime:   corev1alpha1.AgentRuntimeCodex,
			model:     &corev1alpha1.ModelConfig{Name: "model", Temperature: &temperature},
			wantError: "spec.model.temperature",
		},
		{
			name:      "copilot context window",
			runtime:   corev1alpha1.AgentRuntimeCopilot,
			model:     &corev1alpha1.ModelConfig{Name: "model", ContextWindow: &contextWindow},
			wantError: "spec.model.contextWindow",
		},
		{
			name:      "claude max tokens",
			runtime:   corev1alpha1.AgentRuntimeClaude,
			model:     &corev1alpha1.ModelConfig{Name: "model", MaxTokens: &maxTokens},
			wantError: "spec.model.maxTokens",
		},
		{
			name:      "codex fallbacks",
			runtime:   corev1alpha1.AgentRuntimeCodex,
			model:     &corev1alpha1.ModelConfig{Name: "model", Fallbacks: fallbacks},
			wantError: "spec.model.fallbacks",
		},
		{
			name:      "claude fallbacks",
			runtime:   corev1alpha1.AgentRuntimeClaude,
			model:     &corev1alpha1.ModelConfig{Name: "model", Fallbacks: fallbacks},
			wantError: "spec.model.fallbacks",
		},
		{
			name:      "copilot fallbacks",
			runtime:   corev1alpha1.AgentRuntimeCopilot,
			model:     &corev1alpha1.ModelConfig{Name: "model", Fallbacks: fallbacks},
			wantError: "spec.model.fallbacks",
		},
		{
			name:      "opencode temperature",
			runtime:   corev1alpha1.AgentRuntimeOpencode,
			model:     opencodeTemperature,
			wantError: "spec.model.temperature",
		},
		{
			name:      "opencode fallbacks",
			runtime:   corev1alpha1.AgentRuntimeOpencode,
			model:     opencodeFallbacks,
			wantError: "spec.model.fallbacks",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 1},
				Spec: corev1alpha1.AgentSpec{
					Model:   tt.model,
					Runtime: &corev1alpha1.AgentCLIRuntime{Type: tt.runtime},
				},
			}

			_, err := buildACPAgentSessionConfiguration(task, agent, "")
			if err == nil || !isPermanentACPAgentConfigurationError(err) || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("buildACPAgentSessionConfiguration() error = %v, permanent=%t, want %q", err, isPermanentACPAgentConfigurationError(err), tt.wantError)
			}
		})
	}
}

func TestBuildACPAgentSessionConfigurationAcceptsOpenCodeTokenLimits(t *testing.T) {
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model:   testOpenCodeModelConfig(),
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeOpencode},
		},
	}
	if _, err := buildACPAgentSessionConfiguration(task, agent, ""); err != nil {
		t.Fatalf("buildACPAgentSessionConfiguration() error = %v", err)
	}
}

func TestBuildACPAgentSessionConfigurationDefaultsMaxTurnsTo50(t *testing.T) {
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model:   &corev1alpha1.ModelConfig{Name: "model"},
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex},
		},
	}

	configuration, err := buildACPAgentSessionConfiguration(task, agent, "")
	if err != nil {
		t.Fatal(err)
	}
	if configuration.MaxTurns != 50 {
		t.Fatalf("MaxTurns = %d, want built-in default 50", configuration.MaxTurns)
	}
}

func TestResolveACPAgentSessionConfigurationInlineAndConfigMap(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-prompt", Namespace: "default"},
		Data:       map[string]string{"prompt": "trusted system instructions"},
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(configMap).Build()
	maxTurns := int32(7)
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default", UID: types.UID("agent-uid"), Generation: 3},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: "model"},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type: corev1alpha1.AgentRuntimeClaude, DefaultMaxTurns: &maxTurns, DefaultReasoningEffort: agentReasoningEffortHigh,
			},
			SystemPrompt: &corev1alpha1.PromptSource{Inline: "trusted system instructions"},
		},
	}
	inline, err := resolveACPAgentSessionConfiguration(context.Background(), reader, task, agent)
	if err != nil {
		t.Fatal(err)
	}
	agent.Spec.SystemPrompt = &corev1alpha1.PromptSource{
		ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: configMap.Name, Key: "prompt"},
	}
	fromConfigMap, err := resolveACPAgentSessionConfiguration(context.Background(), reader, task, agent)
	if err != nil {
		t.Fatal(err)
	}
	if inline != fromConfigMap || inline.SystemPrompt != "trusted system instructions" || inline.MaxTurns != maxTurns || inline.ReasoningEffort != agentReasoningEffortHigh {
		t.Fatalf("resolved configurations differ: inline=%#v configMap=%#v", inline, fromConfigMap)
	}
}

func TestResolveACPAgentSessionConfigurationFailsClosed(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).Build()
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default", UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model:   &corev1alpha1.ModelConfig{Name: "model"},
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex},
		},
	}

	agent.Spec.SystemPrompt = &corev1alpha1.PromptSource{
		Inline: "inline", ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: "prompt", Key: "prompt"},
	}
	if _, err := resolveACPAgentSessionConfiguration(context.Background(), reader, task, agent); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("dual system prompt source error = %v", err)
	}

	agent.Spec.SystemPrompt = &corev1alpha1.PromptSource{ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: "missing", Key: "prompt"}}
	if _, err := resolveACPAgentSessionConfiguration(context.Background(), reader, task, agent); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing ConfigMap error = %v", err)
	}
}

func TestResolveACPAgentSessionConfigurationPreservesTransientReadErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	transient := errors.New("temporary API outage")
	reader := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return transient
		},
	}).Build()
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default", UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model:   &corev1alpha1.ModelConfig{Name: "model"},
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex},
			SystemPrompt: &corev1alpha1.PromptSource{
				ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: "prompt", Key: "text"},
			},
		},
	}
	_, err := resolveACPAgentSessionConfiguration(context.Background(), reader, task, agent)
	if !errors.Is(err, transient) || isPermanentACPAgentConfigurationError(err) {
		t.Fatalf("transient resolution error = %v, permanent=%t", err, isPermanentACPAgentConfigurationError(err))
	}
}

func TestACPAgentConfigurationAndNativePolicyRotateOrReject(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "prompt", Namespace: "default"},
		Data:       map[string]string{"text": "first"},
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(configMap).Build()
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default", UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: "model"},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeClaude,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
			SystemPrompt: &corev1alpha1.PromptSource{ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: "prompt", Key: "text"}},
		},
	}
	configuration, err := resolveACPAgentSessionConfiguration(context.Background(), reader, task, agent)
	if err != nil {
		t.Fatal(err)
	}
	images := ACPRuntimeImages{Claude: "docker.io/example/claude@sha256:" + strings.Repeat("a", 64)}
	first, err := PlanACPRuntimeWithConfiguration(task, agent, images, configuration)
	if err != nil {
		t.Fatal(err)
	}
	updated := configMap.DeepCopy()
	updated.ResourceVersion = configMap.ResourceVersion
	updated.Data["text"] = "second"
	if err := reader.Update(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	configuration, err = resolveACPAgentSessionConfiguration(context.Background(), reader, task, agent)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanACPRuntimeWithConfiguration(task, agent, images, configuration)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest == second.Digest || first.Profile.AgentConfigurationDigest == second.Profile.AgentConfigurationDigest {
		t.Fatal("ConfigMap system prompt content did not rotate the runtime profile")
	}

	allowBash := false
	task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{"Read"}, AllowBash: &allowBash}
	agent.Spec.Runtime.Type = corev1alpha1.AgentRuntimeCodex
	agent.Spec.SystemPrompt = nil
	configuration, err = buildACPAgentSessionConfiguration(task, agent, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = PlanACPRuntimeWithConfiguration(
		task, agent,
		ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("b", 64)},
		configuration,
	)
	if err == nil || !strings.Contains(err.Error(), "cannot exactly enforce") {
		t.Fatalf("unenforceable Codex native policy error = %v", err)
	}

	agent.Spec.SystemPrompt = &corev1alpha1.PromptSource{Inline: strings.Repeat("<", 16<<10)}
	task.Spec.AgentRuntime = nil
	configuration, err = buildACPAgentSessionConfiguration(task, agent, agent.Spec.SystemPrompt.Inline)
	if err != nil {
		t.Fatal(err)
	}
	_, err = PlanACPRuntimeWithConfiguration(
		task, agent,
		ACPRuntimeImages{Codex: "docker.io/example/codex@sha256:" + strings.Repeat("b", 64)},
		configuration,
	)
	if err == nil || !strings.Contains(err.Error(), "safe environment limit") {
		t.Fatalf("oversized Codex system prompt error = %v", err)
	}

	agent.Spec.Runtime.Type = corev1alpha1.AgentRuntimeCopilot
	agent.Spec.SystemPrompt = &corev1alpha1.PromptSource{Inline: "unsupported"}
	task.Spec.AgentRuntime = nil
	configuration, err = buildACPAgentSessionConfiguration(task, agent, "unsupported")
	if err != nil {
		t.Fatal(err)
	}
	_, err = PlanACPRuntimeWithConfiguration(
		task, agent,
		ACPRuntimeImages{Copilot: "docker.io/example/copilot@sha256:" + strings.Repeat("c", 64)},
		configuration,
	)
	if err == nil || !strings.Contains(err.Error(), "cannot enforce Agent systemPrompt") {
		t.Fatalf("unenforceable Copilot system prompt error = %v", err)
	}
}

func TestResolvedACPAgentConfigurationDigestMatchesProfile(t *testing.T) {
	maxTurns := int32(9)
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default", UID: types.UID("agent-uid"), Generation: 2},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: "model"},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeClaude,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
				DefaultMaxTurns: &maxTurns,
			},
			SystemPrompt: &corev1alpha1.PromptSource{Inline: "system"},
		},
	}
	configuration, err := buildACPAgentSessionConfiguration(task, agent, "system")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanACPRuntimeWithConfiguration(
		task, agent,
		ACPRuntimeImages{Claude: "docker.io/example/claude@sha256:" + strings.Repeat("d", 64)},
		configuration,
	)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := harnessv2.CanonicalAgentConfigurationDigest(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Profile.AgentConfigurationDigest != digest {
		t.Fatalf("profile AgentConfigurationDigest = %q, want %q", plan.Profile.AgentConfigurationDigest, digest)
	}
}

func TestBuildACPAgentSessionConfigurationAcceptsLegacyDefaultTemperature(t *testing.T) {
	temperature := legacyDefaultACPTemperature
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("agent-uid"), Generation: 1},
		Spec: corev1alpha1.AgentSpec{
			Model:   &corev1alpha1.ModelConfig{Name: "model", Temperature: &temperature},
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex},
		},
	}
	if _, err := buildACPAgentSessionConfiguration(task, agent, ""); err != nil {
		t.Fatalf("legacy default temperature rejected: %v", err)
	}
}
