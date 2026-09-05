/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

const testNS = "test-ns"

func setupAgentReconciler(objs ...runtime.Object) *AgentReconciler {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	clientObjs := make([]runtime.Object, len(objs))
	copy(clientObjs, objs)

	builder := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(clientObjs...)
	return &AgentReconciler{
		Client: builder.Build(),
		Scheme: scheme,
	}
}

func baseAgent(name string) *corev1alpha1.Agent {
	return &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNS,
		},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "openai",
				Name:     "gpt-4",
			},
		},
	}
}

// ---------- validateAgent ----------

func TestValidateAgent_ValidModelProvider(t *testing.T) {
	r := setupAgentReconciler()
	agent := baseAgent("valid")

	if err := r.validateAgent(context.Background(), agent); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestValidateAgent_RuntimeAndProviderRefMutuallyExclusive(t *testing.T) {
	r := setupAgentReconciler()
	agent := baseAgent("exclusive")
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCopilot}
	agent.Spec.ProviderRef = &corev1alpha1.ProviderReference{Name: "my-provider"}

	err := r.validateAgent(context.Background(), agent)
	if err == nil {
		t.Fatal("expected error for mutually exclusive runtime+providerRef")
	}
	if got := err.Error(); got != "runtime and providerRef are mutually exclusive" {
		t.Errorf("unexpected error: %s", got)
	}
}

func TestValidateAgent_NoProviderRefNoModelProvider(t *testing.T) {
	r := setupAgentReconciler()
	agent := baseAgent("missing")
	agent.Spec.Model = nil
	agent.Spec.ProviderRef = nil
	agent.Spec.Runtime = nil

	err := r.validateAgent(context.Background(), agent)
	if err == nil {
		t.Fatal("expected error when neither providerRef nor model.provider is set")
	}
}

func TestValidateAgent_RuntimeOnly(t *testing.T) {
	r := setupAgentReconciler()
	agent := baseAgent("runtime-only")
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeClaude}
	agent.Spec.ProviderRef = nil
	agent.Spec.Model = nil

	if err := r.validateAgent(context.Background(), agent); err != nil {
		t.Errorf("runtime-only agent should be valid, got %v", err)
	}
}

func TestValidateAgent_OpenCodeRequirements(t *testing.T) {
	for _, test := range []struct {
		name          string
		model         string
		contextWindow *int32
		maxTokens     *int32
		secret        string
		systemPrompt  string
		wantErr       string
	}{
		{name: "valid", model: "openai/gpt-5.4", contextWindow: new(int32(32768)), maxTokens: new(int32(4096))},
		{name: "system prompt", model: "openai/gpt-5.4", contextWindow: new(int32(32768)), maxTokens: new(int32(4096)), systemPrompt: "ignored", wantErr: "does not support spec.systemPrompt"},
		{name: "missing model", wantErr: "requires spec.model.name"},
		{name: "missing context window", model: "openai/gpt-5.4", maxTokens: new(int32(4096)), wantErr: "requires a positive spec.model.contextWindow"},
		{name: "missing max tokens", model: "openai/gpt-5.4", contextWindow: new(int32(32768)), wantErr: "requires a positive spec.model.maxTokens"},
		{name: "inverted limits", model: "openai/gpt-5.4", contextWindow: new(int32(4096)), maxTokens: new(int32(4096)), wantErr: "contextWindow must exceed"},
		{name: "substitution model", model: "{env:ORKA_OPENCODE_PROVIDER_TOKEN}", contextWindow: new(int32(32768)), maxTokens: new(int32(4096)), wantErr: "substitution braces"},
		{name: "legacy secret", model: "openai/gpt-5.4", contextWindow: new(int32(32768)), maxTokens: new(int32(4096)), secret: "placeholder", wantErr: "does not support agent secretRef"},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := baseAgent("opencode")
			agent.Spec.ProviderRef = nil
			agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeOpencode,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			}
			agent.Spec.Model = &corev1alpha1.ModelConfig{Name: test.model, ContextWindow: test.contextWindow, MaxTokens: test.maxTokens}
			if test.secret != "" {
				agent.Spec.SecretRef = &corev1.LocalObjectReference{Name: test.secret}
			}
			if test.systemPrompt != "" {
				agent.Spec.SystemPrompt = &corev1alpha1.PromptSource{Inline: test.systemPrompt}
			}
			err := setupAgentReconciler().validateAgent(context.Background(), agent)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("validateAgent() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("validateAgent() error = %v, want %q", err, test.wantErr)
			}
		})
	}

	// runtime.type: opencode exists in both harness protocols and is never
	// protocol evidence: the v2 rules must not fire for v1-classified or
	// still-unclassified stored legacy OpenCode Agents, even when they carry
	// historically valid v1 shapes the v2 contract forbids.
	for _, test := range []struct {
		name            string
		contractVersion *corev1alpha1.AgentRuntimeContractVersion
	}{
		{name: "unclassified legacy opencode agent is preserved", contractVersion: nil},
		{name: "harness v1 opencode agent is preserved", contractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := baseAgent("opencode-legacy")
			agent.Spec.ProviderRef = nil
			agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeOpencode,
				ContractVersion: test.contractVersion,
			}
			agent.Spec.SystemPrompt = &corev1alpha1.PromptSource{Inline: "legacy prompt"}
			agent.Spec.Model = &corev1alpha1.ModelConfig{Name: "gpt-5.4"}
			agent.Spec.SecretRef = &corev1.LocalObjectReference{Name: "legacy-provider-creds"}

			if err := ValidateOpenCodeAgentSpec(agent); err != nil {
				t.Fatalf("ValidateOpenCodeAgentSpec() error = %v, want nil for preserved legacy opencode agent", err)
			}
		})
	}
}

func TestValidateAgent_BuiltInRuntimeRejectsCredentialSecretRef(t *testing.T) {
	for _, runtimeType := range []corev1alpha1.AgentRuntimeType{
		corev1alpha1.AgentRuntimeCodex,
		corev1alpha1.AgentRuntimeClaude,
		corev1alpha1.AgentRuntimeCopilot,
		corev1alpha1.AgentRuntimeOpencode,
	} {
		t.Run(string(runtimeType), func(t *testing.T) {
			r := setupAgentReconciler()
			agent := baseAgent(string(runtimeType))
			agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{Type: runtimeType}
			agent.Spec.SecretRef = &corev1.LocalObjectReference{Name: "legacy-provider-creds"}

			err := r.validateAgent(context.Background(), agent)
			if err == nil || !strings.Contains(err.Error(), "does not support agent secretRef") {
				t.Fatalf("validateAgent() error = %v, want built-in ACP secretRef rejection", err)
			}
		})
	}
}

func TestValidateAgent_CredentialSecretRefRemainsValidForProviderBackedAgent(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-creds", Namespace: testNS}}
	r := setupAgentReconciler(secret.DeepCopy())
	agent := baseAgent("provider-backed-agent")
	agent.Spec.SecretRef = &corev1.LocalObjectReference{Name: secret.Name}

	if err := r.validateAgent(context.Background(), agent); err != nil {
		t.Fatalf("validateAgent() error = %v", err)
	}
}

func TestValidateAgent_BuiltInRuntimeRejectsUnsupportedModelControls(t *testing.T) {
	temperature := 0.2
	maxTokens := int32(128)
	zeroMaxTokens := int32(0)
	for _, tt := range []struct {
		name      string
		runtime   corev1alpha1.AgentRuntimeType
		configure func(*corev1alpha1.ModelConfig)
		wantError string
	}{
		{
			name:    "codex temperature",
			runtime: corev1alpha1.AgentRuntimeCodex,
			configure: func(model *corev1alpha1.ModelConfig) {
				model.Temperature = &temperature
			},
			wantError: "spec.model.temperature",
		},
		{
			name:    "claude max tokens",
			runtime: corev1alpha1.AgentRuntimeClaude,
			configure: func(model *corev1alpha1.ModelConfig) {
				model.MaxTokens = &maxTokens
			},
			wantError: "spec.model.maxTokens",
		},
		{
			name:    "copilot explicit zero max tokens",
			runtime: corev1alpha1.AgentRuntimeCopilot,
			configure: func(model *corev1alpha1.ModelConfig) {
				model.MaxTokens = &zeroMaxTokens
			},
			wantError: "spec.model.maxTokens",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := setupAgentReconciler()
			agent := baseAgent(tt.name)
			agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{Type: tt.runtime}
			tt.configure(agent.Spec.Model)

			err := r.validateAgent(context.Background(), agent)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateAgent() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateAgent_ModelControlsRemainValidOutsideBuiltInACPRuntimes(t *testing.T) {
	temperature := 0.2
	maxTokens := int32(128)
	for _, tt := range []struct {
		name    string
		runtime *corev1alpha1.AgentCLIRuntime
	}{
		{name: "provider-backed agent"},
		{
			name: "custom runtimeRef agent",
			runtime: &corev1alpha1.AgentCLIRuntime{
				RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "custom-runtime"},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := setupAgentReconciler()
			agent := baseAgent(tt.name)
			agent.Spec.Runtime = tt.runtime
			agent.Spec.Model.Temperature = &temperature
			agent.Spec.Model.MaxTokens = &maxTokens

			if err := r.validateAgent(context.Background(), agent); err != nil {
				t.Fatalf("validateAgent() error = %v", err)
			}
		})
	}
}

func TestValidateAgent_RuntimeRefRequiresName(t *testing.T) {
	agent := baseAgent("missing-runtime-ref-name")
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "  "},
	}

	err := setupAgentReconciler().validateAgent(context.Background(), agent)
	if err == nil || !strings.Contains(err.Error(), "runtimeRef.name is required") {
		t.Fatalf("validateAgent() error = %v, want runtimeRef.name rejection", err)
	}
}

func TestValidateAgent_RuntimeRefRejectsUnsupportedAgentPolicy(t *testing.T) {
	for _, tt := range []struct {
		name      string
		configure func(*corev1alpha1.Agent)
		wantError string
	}{
		{
			name: "default allowed tools",
			configure: func(agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.DefaultAllowedTools = []string{"read"}
			},
			wantError: "defaultAllowedTools",
		},
		{
			name: "explicitly empty default allowed tools",
			configure: func(agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.DefaultAllowedTools = []string{}
			},
			wantError: "defaultAllowedTools",
		},
		{
			name: "default allow bash",
			configure: func(agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.DefaultAllowBash = new(false)
			},
			wantError: "defaultAllowBash",
		},
		{
			name: "default reasoning effort",
			configure: func(agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.DefaultReasoningEffort = "high"
			},
			wantError: "defaultReasoningEffort",
		},
		{
			name: "credential",
			configure: func(agent *corev1alpha1.Agent) {
				agent.Spec.SecretRef = &corev1.LocalObjectReference{Name: "runtime-credential"}
			},
			wantError: "agent secretRef credential delivery",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			agent := baseAgent(tt.name)
			agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
				RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "custom-runtime"},
			}
			tt.configure(agent)

			err := setupAgentReconciler().validateAgent(context.Background(), agent)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateAgent() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

// ---------- validateProviderRef ----------

func TestValidateProviderRef(t *testing.T) {
	readyProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "ready-prov", Namespace: testNS},
		Status:     corev1alpha1.ProviderStatus{Ready: true},
	}
	notReadyProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "not-ready-prov", Namespace: testNS},
		Status:     corev1alpha1.ProviderStatus{Ready: false},
	}

	tests := []struct {
		name      string
		agent     *corev1alpha1.Agent
		objs      []runtime.Object
		wantErr   bool
		errSubstr string
	}{
		{
			name:  "nil providerRef",
			agent: baseAgent("no-ref"),
		},
		{
			name: "provider exists and ready",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("with-ref")
				a.Spec.ProviderRef = &corev1alpha1.ProviderReference{Name: "ready-prov"}
				return a
			}(),
			objs: []runtime.Object{readyProvider},
		},
		{
			name: "provider not found",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("missing-ref")
				a.Spec.ProviderRef = &corev1alpha1.ProviderReference{Name: "nonexistent"}
				return a
			}(),
			wantErr:   true,
			errSubstr: "not found",
		},
		{
			name: "provider not ready",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("not-ready-ref")
				a.Spec.ProviderRef = &corev1alpha1.ProviderReference{Name: "not-ready-prov"}
				return a
			}(),
			objs:      []runtime.Object{notReadyProvider},
			wantErr:   true,
			errSubstr: "not ready",
		},
		{
			name: "provider in custom namespace",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("cross-ns")
				a.Spec.ProviderRef = &corev1alpha1.ProviderReference{Name: "ready-prov", Namespace: "other-ns"}
				return a
			}(),
			objs: []runtime.Object{
				&corev1alpha1.Provider{
					ObjectMeta: metav1.ObjectMeta{Name: "ready-prov", Namespace: "other-ns"},
					Status:     corev1alpha1.ProviderStatus{Ready: true},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := setupAgentReconciler(tt.objs...)
			err := r.validateProviderRef(context.Background(), tt.agent)
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.errSubstr != "" && err != nil {
				if got := err.Error(); !strContains(got, tt.errSubstr) {
					t.Errorf("error %q should contain %q", got, tt.errSubstr)
				}
			}
		})
	}
}

// ---------- validateSecretRef ----------

func TestValidateSecretRef(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "my-secret", Namespace: testNS},
		Data:       map[string][]byte{"api-key": []byte("secret-val")},
	}

	tests := []struct {
		name      string
		agent     *corev1alpha1.Agent
		objs      []runtime.Object
		wantErr   bool
		errSubstr string
	}{
		{
			name:  "nil secretRef",
			agent: baseAgent("no-secret"),
		},
		{
			name: "secret exists",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("with-secret")
				a.Spec.SecretRef = &corev1.LocalObjectReference{Name: "my-secret"}
				return a
			}(),
			objs: []runtime.Object{secret},
		},
		{
			name: "secret not found",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("missing-secret")
				a.Spec.SecretRef = &corev1.LocalObjectReference{Name: "nonexistent"}
				return a
			}(),
			wantErr:   true,
			errSubstr: "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := setupAgentReconciler(tt.objs...)
			err := r.validateSecretRef(context.Background(), tt.agent)
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.errSubstr != "" && err != nil {
				if got := err.Error(); !strContains(got, tt.errSubstr) {
					t.Errorf("error %q should contain %q", got, tt.errSubstr)
				}
			}
		})
	}
}

// ---------- validateTools ----------

func TestValidateTools(t *testing.T) {
	existingTool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-tool", Namespace: testNS},
	}

	tests := []struct {
		name    string
		agent   *corev1alpha1.Agent
		objs    []runtime.Object
		wantErr bool
	}{
		{
			name:  "no tools",
			agent: baseAgent("no-tools"),
		},
		{
			name: "existing tool",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("with-tool")
				a.Spec.Tools = []corev1alpha1.ToolReference{{Name: "existing-tool"}}
				return a
			}(),
			objs: []runtime.Object{existingTool},
		},
		{
			name: "missing tool treated as built-in (no error)",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("builtin-tool")
				a.Spec.Tools = []corev1alpha1.ToolReference{{Name: "code_exec"}}
				return a
			}(),
		},
		{
			name: "disabled tool skipped",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("disabled-tool")
				a.Spec.Tools = []corev1alpha1.ToolReference{{Name: "nonexistent", Enabled: new(false)}}
				return a
			}(),
		},
		{
			name: "enabled tool that exists",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("enabled-tool")
				a.Spec.Tools = []corev1alpha1.ToolReference{{Name: "existing-tool", Enabled: new(true)}}
				return a
			}(),
			objs: []runtime.Object{existingTool},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := setupAgentReconciler(tt.objs...)
			err := r.validateTools(context.Background(), tt.agent)
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// ---------- validateSkills ----------

func TestValidateSkills(t *testing.T) {
	skillCR := &corev1alpha1.Skill{
		ObjectMeta: metav1.ObjectMeta{Name: "skill-cr", Namespace: testNS},
		Spec: corev1alpha1.SkillSpec{
			Description: "test skill",
			Content:     corev1alpha1.SkillContent{Inline: "test content"},
		},
	}
	skillCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "skill-cm", Namespace: testNS},
		Data:       map[string]string{"skill.txt": "configmap skill"},
	}

	tests := []struct {
		name      string
		agent     *corev1alpha1.Agent
		objs      []runtime.Object
		wantErr   bool
		errSubstr string
	}{
		{
			name:  "no skills",
			agent: baseAgent("no-skills"),
		},
		{
			name: "skill CR exists",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("with-skill")
				a.Spec.Skills = []corev1alpha1.SkillReference{
					{Name: "skill-cr"},
				}
				return a
			}(),
			objs: []runtime.Object{skillCR},
		},
		{
			name: "skill CR not found",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("missing-skill")
				a.Spec.Skills = []corev1alpha1.SkillReference{
					{Name: "nonexistent"},
				}
				return a
			}(),
			wantErr:   true,
			errSubstr: "not found",
		},
		{
			name: "skill ConfigMap exists",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("with-skill-configmap")
				a.Spec.Skills = []corev1alpha1.SkillReference{
					{ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: "skill-cm", Key: "skill.txt"}},
				}
				return a
			}(),
			objs: []runtime.Object{skillCM},
		},
		{
			name: "skill ConfigMap key missing",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("missing-skill-key")
				a.Spec.Skills = []corev1alpha1.SkillReference{
					{ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: "skill-cm", Key: "missing.txt"}},
				}
				return a
			}(),
			objs:      []runtime.Object{skillCM},
			wantErr:   true,
			errSubstr: "not found in skill ConfigMap",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := setupAgentReconciler(tt.objs...)
			err := r.validateSkills(context.Background(), tt.agent)
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.errSubstr != "" && err != nil {
				if got := err.Error(); !strContains(got, tt.errSubstr) {
					t.Errorf("error %q should contain %q", got, tt.errSubstr)
				}
			}
		})
	}
}

// ---------- validateSystemPromptConfigMap ----------

func TestValidateSystemPromptConfigMap(t *testing.T) {
	promptCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "prompt-cm", Namespace: testNS},
		Data:       map[string]string{"prompt": "You are a helpful assistant."},
	}

	tests := []struct {
		name      string
		agent     *corev1alpha1.Agent
		objs      []runtime.Object
		wantErr   bool
		errSubstr string
	}{
		{
			name:  "no system prompt",
			agent: baseAgent("no-prompt"),
		},
		{
			name: "inline prompt only",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("inline-prompt")
				a.Spec.SystemPrompt = &corev1alpha1.PromptSource{Inline: "hello"}
				return a
			}(),
		},
		{
			name: "configmap exists with correct key",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("valid-cm")
				a.Spec.SystemPrompt = &corev1alpha1.PromptSource{
					ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: "prompt-cm", Key: "prompt"},
				}
				return a
			}(),
			objs: []runtime.Object{promptCM},
		},
		{
			name: "configmap not found",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("missing-cm")
				a.Spec.SystemPrompt = &corev1alpha1.PromptSource{
					ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: "nonexistent", Key: "prompt"},
				}
				return a
			}(),
			wantErr:   true,
			errSubstr: "not found",
		},
		{
			name: "configmap exists but key missing",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("wrong-key")
				a.Spec.SystemPrompt = &corev1alpha1.PromptSource{
					ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: "prompt-cm", Key: "missing-key"},
				}
				return a
			}(),
			objs:      []runtime.Object{promptCM},
			wantErr:   true,
			errSubstr: "key \"missing-key\" not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := setupAgentReconciler(tt.objs...)
			err := r.validateSystemPromptConfigMap(context.Background(), tt.agent)
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.errSubstr != "" && err != nil {
				if got := err.Error(); !strContains(got, tt.errSubstr) {
					t.Errorf("error %q should contain %q", got, tt.errSubstr)
				}
			}
		})
	}
}

// ---------- validateCoordination ----------

func TestValidateCoordination(t *testing.T) {
	delegateAgent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "delegate", Namespace: testNS},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Provider: "openai"},
		},
	}

	tests := []struct {
		name      string
		agent     *corev1alpha1.Agent
		objs      []runtime.Object
		wantErr   bool
		errSubstr string
	}{
		{
			name:  "nil coordination",
			agent: baseAgent("no-coord"),
		},
		{
			name: "coordination disabled",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("disabled-coord")
				a.Spec.Coordination = &corev1alpha1.CoordinationConfig{Enabled: false}
				return a
			}(),
		},
		{
			name: "coordination enabled, no allowed agents",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("coord-empty")
				a.Spec.Coordination = &corev1alpha1.CoordinationConfig{Enabled: true}
				return a
			}(),
		},
		{
			name: "coordination enabled, delegate exists",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("coord-ok")
				a.Spec.Coordination = &corev1alpha1.CoordinationConfig{
					Enabled:       true,
					AllowedAgents: []corev1alpha1.AllowedAgent{{Name: "delegate"}},
				}
				return a
			}(),
			objs: []runtime.Object{delegateAgent},
		},
		{
			name: "coordination enabled, delegate not found",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("coord-missing")
				a.Spec.Coordination = &corev1alpha1.CoordinationConfig{
					Enabled:       true,
					AllowedAgents: []corev1alpha1.AllowedAgent{{Name: "nonexistent"}},
				}
				return a
			}(),
			wantErr:   true,
			errSubstr: "not found",
		},
		{
			name: "coordination with cross-namespace delegate",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("coord-cross-ns")
				a.Spec.Coordination = &corev1alpha1.CoordinationConfig{
					Enabled:       true,
					AllowedAgents: []corev1alpha1.AllowedAgent{{Name: "other-agent", Namespace: "other-ns"}},
				}
				return a
			}(),
			objs: []runtime.Object{
				&corev1alpha1.Agent{
					ObjectMeta: metav1.ObjectMeta{Name: "other-agent", Namespace: "other-ns"},
					Spec:       corev1alpha1.AgentSpec{Model: &corev1alpha1.ModelConfig{Provider: "openai"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := setupAgentReconciler(tt.objs...)
			err := r.validateCoordination(context.Background(), tt.agent)
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.errSubstr != "" && err != nil {
				if got := err.Error(); !strContains(got, tt.errSubstr) {
					t.Errorf("error %q should contain %q", got, tt.errSubstr)
				}
			}
		})
	}
}

// ---------- countActiveTasks ----------

func TestCountActiveTasks(t *testing.T) {
	agent := baseAgent("count-agent")

	pendingTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-pending", Namespace: testNS},
		Spec: corev1alpha1.TaskSpec{
			AgentRef: &corev1alpha1.AgentReference{Name: "count-agent"},
			Prompt:   "do something",
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	runningTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-running", Namespace: testNS},
		Spec: corev1alpha1.TaskSpec{
			AgentRef: &corev1alpha1.AgentReference{Name: "count-agent"},
			Prompt:   "do something",
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	succeededTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-succeeded", Namespace: testNS},
		Spec: corev1alpha1.TaskSpec{
			AgentRef: &corev1alpha1.AgentReference{Name: "count-agent"},
			Prompt:   "do something",
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	failedTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-failed", Namespace: testNS},
		Spec: corev1alpha1.TaskSpec{
			AgentRef: &corev1alpha1.AgentReference{Name: "count-agent"},
			Prompt:   "do something",
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseFailed},
	}
	otherAgentTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-other", Namespace: testNS},
		Spec: corev1alpha1.TaskSpec{
			AgentRef: &corev1alpha1.AgentReference{Name: "other-agent"},
			Prompt:   "do something",
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	noAgentRefTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-no-ref", Namespace: testNS},
		Spec: corev1alpha1.TaskSpec{
			Prompt: "do something",
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}

	tests := []struct {
		name string
		objs []runtime.Object
		want int32
	}{
		{
			name: "no tasks",
			want: 0,
		},
		{
			name: "only pending and running count",
			objs: []runtime.Object{pendingTask, runningTask, succeededTask, failedTask},
			want: 2,
		},
		{
			name: "other agent tasks not counted",
			objs: []runtime.Object{runningTask, otherAgentTask},
			want: 1,
		},
		{
			name: "tasks without agentRef not counted",
			objs: []runtime.Object{noAgentRefTask},
			want: 0,
		},
		{
			name: "all terminal - zero active",
			objs: []runtime.Object{succeededTask, failedTask},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := setupAgentReconciler(tt.objs...)
			got, err := r.countActiveTasks(context.Background(), agent)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("countActiveTasks = %d, want %d", got, tt.want)
			}
		})
	}
}

// ---------- updateStatus ----------

func TestUpdateStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	t.Run("validation success sets Ready condition true", func(t *testing.T) {
		agent := baseAgent("status-ok")
		fc := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(agent).
			WithStatusSubresource(agent).Build()
		r := &AgentReconciler{Client: fc, Scheme: scheme}

		result, err := r.updateStatus(context.Background(), agent, 0, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue, got %v", result.RequeueAfter)
		}
		if agent.Status.ActiveTasks != 0 {
			t.Errorf("ActiveTasks = %d, want 0", agent.Status.ActiveTasks)
		}
		if !agent.Status.Ready {
			t.Error("Ready = false, want true")
		}
		cond := findCondition(agent.Status.Conditions, "Ready")
		if cond == nil {
			t.Fatal("Ready condition not found")
		}
		if cond.Status != metav1.ConditionTrue {
			t.Errorf("Ready status = %s, want True", cond.Status)
		}
		if cond.Reason != reasonValidationSucceeded {
			t.Errorf("Reason = %s, want ValidationSucceeded", cond.Reason)
		}
	})

	t.Run("validation failure sets Ready condition false", func(t *testing.T) {
		agent := baseAgent("status-fail")
		fc := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(agent).
			WithStatusSubresource(agent).Build()
		r := &AgentReconciler{Client: fc, Scheme: scheme}

		_, err := r.updateStatus(context.Background(), agent, 0, errTest("bad config"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		cond := findCondition(agent.Status.Conditions, "Ready")
		if cond == nil {
			t.Fatal("Ready condition not found")
		}
		if cond.Status != metav1.ConditionFalse {
			t.Errorf("Ready status = %s, want False", cond.Status)
		}
		if cond.Reason != reasonValidationFailed {
			t.Errorf("Reason = %s, want ValidationFailed", cond.Reason)
		}
		if cond.Message != "bad config" {
			t.Errorf("Message = %q, want %q", cond.Message, "bad config")
		}
		if agent.Status.Ready {
			t.Error("Ready = true, want false")
		}
	})

	t.Run("active tasks sets LastUsed", func(t *testing.T) {
		agent := baseAgent("status-active")
		fc := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(agent).
			WithStatusSubresource(agent).Build()
		r := &AgentReconciler{Client: fc, Scheme: scheme}

		_, err := r.updateStatus(context.Background(), agent, 3, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if agent.Status.ActiveTasks != 3 {
			t.Errorf("ActiveTasks = %d, want 3", agent.Status.ActiveTasks)
		}
		if agent.Status.LastUsed == nil {
			t.Fatal("LastUsed should be set when activeTasks > 0")
		}
	})

	t.Run("TTL requeue when idle", func(t *testing.T) {
		agent := baseAgent("status-ttl")
		ttl := metav1.Duration{Duration: 10 * time.Minute}
		agent.Spec.TTLAfterLastTask = &ttl
		lastUsed := metav1.NewTime(time.Now().Add(-5 * time.Minute))
		agent.Status.LastUsed = &lastUsed

		fc := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(agent).
			WithStatusSubresource(agent).Build()
		r := &AgentReconciler{Client: fc, Scheme: scheme}

		result, err := r.updateStatus(context.Background(), agent, 0, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter <= 0 {
			t.Error("expected positive RequeueAfter for non-expired TTL")
		}
		if result.RequeueAfter > 6*time.Minute {
			t.Errorf("RequeueAfter too large: %v", result.RequeueAfter)
		}
	})
}

// ---------- checkTTLExpiry ----------

func TestCheckTTLExpiry(t *testing.T) {
	tests := []struct {
		name        string
		agent       *corev1alpha1.Agent
		activeTasks int32
		wantDeleted bool
	}{
		{
			name:  "no TTL set",
			agent: baseAgent("no-ttl"),
		},
		{
			name: "zero duration TTL",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("zero-ttl")
				a.Spec.TTLAfterLastTask = &metav1.Duration{Duration: 0}
				return a
			}(),
		},
		{
			name: "active tasks prevent deletion",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("active-ttl")
				a.Spec.TTLAfterLastTask = &metav1.Duration{Duration: time.Second}
				lastUsed := metav1.NewTime(time.Now().Add(-time.Hour))
				a.Status.LastUsed = &lastUsed
				return a
			}(),
			activeTasks: 1,
		},
		{
			name: "TTL not expired yet",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("not-expired")
				a.Spec.TTLAfterLastTask = &metav1.Duration{Duration: time.Hour}
				lastUsed := metav1.NewTime(time.Now())
				a.Status.LastUsed = &lastUsed
				return a
			}(),
		},
		{
			name: "TTL expired, no LastUsed (uses creation time)",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("expired-no-last")
				a.Spec.TTLAfterLastTask = &metav1.Duration{Duration: time.Second}
				a.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
				return a
			}(),
			wantDeleted: true,
		},
		{
			name: "TTL expired with LastUsed",
			agent: func() *corev1alpha1.Agent {
				a := baseAgent("expired")
				a.Spec.TTLAfterLastTask = &metav1.Duration{Duration: time.Second}
				lastUsed := metav1.NewTime(time.Now().Add(-time.Hour))
				a.Status.LastUsed = &lastUsed
				return a
			}(),
			wantDeleted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := setupAgentReconciler(tt.agent)
			_, deleted := r.checkTTLExpiry(context.Background(), tt.agent, tt.activeTasks)
			if deleted != tt.wantDeleted {
				t.Errorf("deleted = %v, want %v", deleted, tt.wantDeleted)
			}
		})
	}
}

// ---------- Reconcile ----------

func TestAgentReconcile_NotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fc := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &AgentReconciler{Client: fc, Scheme: scheme}

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing", Namespace: testNS},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got %v", result.RequeueAfter)
	}
}

func TestAgentReconcile_ValidAgent(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	agent := baseAgent("reconcile-valid")
	fc := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(agent).
		WithStatusSubresource(agent).Build()
	r := &AgentReconciler{Client: fc, Scheme: scheme}

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "reconcile-valid", Namespace: testNS},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

func TestAgentReconcile_ValidationFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	agent := baseAgent("reconcile-invalid")
	agent.Spec.Model = nil
	agent.Spec.ProviderRef = nil
	agent.Spec.Runtime = nil
	fc := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(agent).
		WithStatusSubresource(agent).Build()
	r := &AgentReconciler{Client: fc, Scheme: scheme}

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "reconcile-invalid", Namespace: testNS},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
	// Re-fetch agent and check status condition
	updated := &corev1alpha1.Agent{}
	_ = fc.Get(context.Background(), types.NamespacedName{Name: "reconcile-invalid", Namespace: testNS}, updated)
	cond := findCondition(updated.Status.Conditions, "Ready")
	if cond != nil && cond.Status != metav1.ConditionFalse {
		t.Errorf("expected Ready=False, got %s", cond.Status)
	}
}

func TestAgentReconcile_TTLExpired(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	agent := baseAgent("reconcile-ttl")
	agent.Spec.TTLAfterLastTask = &metav1.Duration{Duration: time.Second}
	agent.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	fc := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(agent).
		WithStatusSubresource(agent).Build()
	r := &AgentReconciler{Client: fc, Scheme: scheme}

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "reconcile-ttl", Namespace: testNS},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
	// Agent should have been deleted
	updated := &corev1alpha1.Agent{}
	getErr := fc.Get(context.Background(), types.NamespacedName{Name: "reconcile-ttl", Namespace: testNS}, updated)
	if getErr == nil {
		t.Error("expected agent to be deleted after TTL expiry")
	}
}

func TestAgentReconcile_WithActiveTasks(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	agent := baseAgent("reconcile-active")
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "active-t", Namespace: testNS},
		Spec: corev1alpha1.TaskSpec{
			AgentRef: &corev1alpha1.AgentReference{Name: "reconcile-active"},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	fc := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(agent, task).
		WithStatusSubresource(agent).Build()
	r := &AgentReconciler{Client: fc, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "reconcile-active", Namespace: testNS},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	updated := &corev1alpha1.Agent{}
	_ = fc.Get(context.Background(), types.NamespacedName{Name: "reconcile-active", Namespace: testNS}, updated)
	if updated.Status.ActiveTasks != 1 {
		t.Errorf("expected ActiveTasks=1, got %d", updated.Status.ActiveTasks)
	}
}

// ---------- updateStatus additional ----------

func TestUpdateStatus_TTLExpiredNoRequeue(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	agent := baseAgent("status-ttl-expired")
	ttl := metav1.Duration{Duration: time.Second}
	agent.Spec.TTLAfterLastTask = &ttl
	lastUsed := metav1.NewTime(time.Now().Add(-time.Hour))
	agent.Status.LastUsed = &lastUsed

	fc := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(agent).
		WithStatusSubresource(agent).Build()
	r := &AgentReconciler{Client: fc, Scheme: scheme}

	result, err := r.updateStatus(context.Background(), agent, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// TTL already expired, remaining <= 0, no requeue
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue for expired TTL, got %v", result.RequeueAfter)
	}
}

func TestUpdateStatus_NoTTLNoLastUsed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	agent := baseAgent("status-nottl")

	fc := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(agent).
		WithStatusSubresource(agent).Build()
	r := &AgentReconciler{Client: fc, Scheme: scheme}

	result, err := r.updateStatus(context.Background(), agent, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got %v", result.RequeueAfter)
	}
}

// ---------- helpers ----------

type errTest string

func (e errTest) Error() string { return string(e) }

func strContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

func TestValidateAgent_BuiltInRuntimeAcceptsLegacyDefaultTemperature(t *testing.T) {
	r := setupAgentReconciler()
	agent := baseAgent("legacy-default-temperature")
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeClaude}
	temperature := legacyDefaultACPTemperature
	agent.Spec.Model.Temperature = &temperature
	if err := r.validateAgent(context.Background(), agent); err != nil {
		t.Fatalf("legacy default temperature rejected: %v", err)
	}
}
