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

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/workerenv"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testSandboxTemplatesNamespace          = "sandbox-templates"
	testSubstrateBootstrapSecretName       = "orka-substrate-bootstrap"
	testSubstrateBootstrapSecretKey        = "bootstrap-token"
	testSubstrateSessionIdentitySecretName = "orka-substrate-session-identity"
	testSubstrateSessionIdentitySecretKey  = "session-token"
	invalidAgentSandboxTestURL             = "not-a-url"
)

func TestDefaultAgentSandboxConfig(t *testing.T) {
	cfg := DefaultAgentSandboxConfig()

	if cfg.RouterURL != "" {
		t.Fatalf("RouterURL = %q, want empty", cfg.RouterURL)
	}
	if cfg.DefaultTemplate != "" {
		t.Fatalf("DefaultTemplate = %q, want empty", cfg.DefaultTemplate)
	}
	if cfg.WarmPoolPolicy != AgentSandboxWarmPoolPolicyDisabled {
		t.Fatalf("WarmPoolPolicy = %q, want %q", cfg.WarmPoolPolicy, AgentSandboxWarmPoolPolicyDisabled)
	}
	if cfg.NamespaceStrategy != AgentSandboxNamespaceStrategyTask {
		t.Fatalf("NamespaceStrategy = %q, want %q", cfg.NamespaceStrategy, AgentSandboxNamespaceStrategyTask)
	}
	if cfg.ClaimTimeout != 2*time.Minute {
		t.Fatalf("ClaimTimeout = %s, want 2m", cfg.ClaimTimeout)
	}
	if cfg.CommandTimeout != 30*time.Minute {
		t.Fatalf("CommandTimeout = %s, want 30m", cfg.CommandTimeout)
	}
	if cfg.CleanupPolicy != corev1alpha1.WorkspaceCleanupPolicyDelete {
		t.Fatalf("CleanupPolicy = %q, want %q", cfg.CleanupPolicy, corev1alpha1.WorkspaceCleanupPolicyDelete)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestAgentSandboxConfigFromEnv(t *testing.T) {
	env := map[string]string{
		EnvAgentSandboxRouterURL:         "http://sandbox-router.orka-system.svc:8080",
		EnvAgentSandboxDefaultTemplate:   "coding-template",
		EnvAgentSandboxWarmPoolPolicy:    AgentSandboxWarmPoolPolicyTemplate,
		EnvAgentSandboxNamespaceStrategy: AgentSandboxNamespaceStrategyController,
		EnvAgentSandboxClaimTimeout:      "45s",
		EnvAgentSandboxCommandTimeout:    "10m",
		EnvAgentSandboxCleanupPolicy:     string(corev1alpha1.WorkspaceCleanupPolicyRetain),
	}

	cfg, err := AgentSandboxConfigFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("AgentSandboxConfigFromEnv() error = %v", err)
	}
	if cfg.RouterURL != env[EnvAgentSandboxRouterURL] {
		t.Fatalf("RouterURL = %q, want %q", cfg.RouterURL, env[EnvAgentSandboxRouterURL])
	}
	if cfg.DefaultTemplate != env[EnvAgentSandboxDefaultTemplate] {
		t.Fatalf("DefaultTemplate = %q, want %q", cfg.DefaultTemplate, env[EnvAgentSandboxDefaultTemplate])
	}
	if cfg.WarmPoolPolicy != AgentSandboxWarmPoolPolicyTemplate {
		t.Fatalf("WarmPoolPolicy = %q, want %q", cfg.WarmPoolPolicy, AgentSandboxWarmPoolPolicyTemplate)
	}
	if cfg.NamespaceStrategy != AgentSandboxNamespaceStrategyController {
		t.Fatalf("NamespaceStrategy = %q, want %q", cfg.NamespaceStrategy, AgentSandboxNamespaceStrategyController)
	}
	if cfg.ClaimTimeout != 45*time.Second {
		t.Fatalf("ClaimTimeout = %s, want 45s", cfg.ClaimTimeout)
	}
	if cfg.CommandTimeout != 10*time.Minute {
		t.Fatalf("CommandTimeout = %s, want 10m", cfg.CommandTimeout)
	}
	if cfg.CleanupPolicy != corev1alpha1.WorkspaceCleanupPolicyRetain {
		t.Fatalf("CleanupPolicy = %q, want %q", cfg.CleanupPolicy, corev1alpha1.WorkspaceCleanupPolicyRetain)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestAgentSandboxConfigFromEnv_InvalidDuration(t *testing.T) {
	_, err := AgentSandboxConfigFromEnv(func(key string) string {
		if key == EnvAgentSandboxClaimTimeout {
			return "not-a-duration"
		}
		return ""
	})
	if err == nil {
		t.Fatal("expected invalid duration error")
	}
	if !strings.Contains(err.Error(), EnvAgentSandboxClaimTimeout) {
		t.Fatalf("error = %q, want env var name", err.Error())
	}
}

func TestSubstrateConfigFromEnv(t *testing.T) {
	env := map[string]string{
		EnvSubstrateAPIEndpoint:               "api.ate-system.svc:443",
		EnvSubstrateAPICAFile:                 "/var/run/orka/substrate/ca.crt",
		EnvSubstrateAPIInsecureSkipVerify:     "true",
		EnvSubstrateRouterURL:                 "http://atenet-router.ate-system.svc",
		EnvSubstrateActorDNSSuffix:            "actors.resources.substrate.ate.dev",
		EnvSubstrateDefaultTemplate:           "orka-codex",
		EnvSubstrateDefaultTemplateNS:         "ate-demo",
		EnvSubstrateBootstrapSecretName:       testSubstrateBootstrapSecretName,
		EnvSubstrateBootstrapSecretKey:        testSubstrateBootstrapSecretKey,
		EnvSubstrateSessionIdentitySecretName: testSubstrateSessionIdentitySecretName,
		EnvSubstrateSessionIdentitySecretKey:  testSubstrateSessionIdentitySecretKey,
		EnvSubstrateSessionIdentityRequired:   "true",
		EnvSubstrateSessionIdentityAudience:   "orka-workspace-daemon,custom-audience",
		EnvSubstrateSessionIdentityAppID:      managedByLabelValue,
		EnvSubstrateSessionIdentityUserID:     "orka-worker",
		EnvSubstrateClaimTimeout:              "45s",
		EnvSubstrateCommandTimeout:            "10m",
		EnvSubstrateCleanupPolicy:             string(corev1alpha1.WorkspaceCleanupPolicyRetain),
	}

	cfg, err := SubstrateConfigFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("SubstrateConfigFromEnv() error = %v", err)
	}
	if cfg.APIEndpoint != env[EnvSubstrateAPIEndpoint] || cfg.RouterURL != env[EnvSubstrateRouterURL] {
		t.Fatalf("unexpected substrate endpoints: %#v", cfg)
	}
	if !cfg.APIInsecureSkipVerify {
		t.Fatal("APIInsecureSkipVerify = false, want true")
	}
	if cfg.DefaultTemplate != "orka-codex" || cfg.DefaultTemplateNS != "ate-demo" {
		t.Fatalf("unexpected substrate defaults: %#v", cfg)
	}
	if cfg.BootstrapSecretName != testSubstrateBootstrapSecretName ||
		cfg.BootstrapSecretKey != testSubstrateBootstrapSecretKey {
		t.Fatalf("unexpected substrate bootstrap secret: %#v", cfg)
	}
	if cfg.SessionIdentitySecretName != testSubstrateSessionIdentitySecretName ||
		cfg.SessionIdentitySecretKey != testSubstrateSessionIdentitySecretKey ||
		!cfg.SessionIdentityRequired ||
		cfg.SessionIdentityAudience != "orka-workspace-daemon,custom-audience" ||
		cfg.SessionIdentityAppID != managedByLabelValue ||
		cfg.SessionIdentityUserID != "orka-worker" {
		t.Fatalf("unexpected substrate SessionIdentity config: %#v", cfg)
	}
	if cfg.ClaimTimeout != 45*time.Second || cfg.CommandTimeout != 10*time.Minute {
		t.Fatalf("unexpected substrate timeouts: %#v", cfg)
	}
	if cfg.CleanupPolicy != corev1alpha1.WorkspaceCleanupPolicyRetain {
		t.Fatalf("CleanupPolicy = %q, want retain", cfg.CleanupPolicy)
	}
}

func TestSubstrateConfigValidateRequiresExplicitTrust(t *testing.T) {
	cfg := DefaultSubstrateConfig()
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected missing API trust error")
	}
	if !strings.Contains(err.Error(), "substrate API trust") {
		t.Fatalf("Validate() error = %q, want API trust context", err.Error())
	}

	cfg.APICAFile = "/var/run/orka/substrate/ca.crt"
	err = cfg.Validate()
	if err == nil {
		t.Fatal("expected missing bootstrap secret error")
	}
	if !strings.Contains(err.Error(), "bootstrap token secret name") {
		t.Fatalf("Validate() error = %q, want bootstrap secret context", err.Error())
	}

	cfg.BootstrapSecretName = testSubstrateBootstrapSecretName
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with CA file and bootstrap secret error = %v", err)
	}

	cfg.APICAFile = ""
	cfg.APIInsecureSkipVerify = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with insecure skip verify error = %v", err)
	}
}

func TestSubstrateConfigValidateACPRuntimePoolDoesNotRequireLegacyBootstrapSecret(t *testing.T) {
	cfg := DefaultSubstrateConfig()
	cfg.APIInsecureSkipVerify = true

	if err := cfg.ValidateACPRuntimePool(); err != nil {
		t.Fatalf("ValidateACPRuntimePool() error = %v", err)
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "bootstrap token secret name") {
		t.Fatalf("legacy Validate() error = %v, want bootstrap secret requirement", err)
	}
}

func TestSubstrateConfigValidateACPRuntimePoolRejectsNonPositiveClaimTimeout(t *testing.T) {
	cfg := DefaultSubstrateConfig()
	cfg.APIInsecureSkipVerify = true
	cfg.ClaimTimeout = -time.Second

	if err := cfg.ValidateACPRuntimePool(); err == nil || !strings.Contains(err.Error(), "claim timeout") {
		t.Fatalf("ValidateACPRuntimePool() error = %v, want claim timeout validation", err)
	}
}

func TestSubstrateConfigValidateACPRuntimePoolRejectsInvalidRouting(t *testing.T) {
	tests := []struct {
		name      string
		routerURL string
		dnsSuffix string
		want      string
	}{
		{name: "router URL", routerURL: invalidAgentSandboxTestURL, dnsSuffix: "actors.example.test", want: "router URL is invalid"},
		{name: "DNS suffix", routerURL: "https://router.example.test", dnsSuffix: "actors..example.test", want: "DNS suffix is invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultSubstrateConfig()
			cfg.APIInsecureSkipVerify = true
			cfg.RouterURL = tt.routerURL
			cfg.ActorDNSSuffix = tt.dnsSuffix
			if err := cfg.ValidateACPRuntimePool(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateACPRuntimePool() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestSubstrateConfigValidateRequiresSessionIdentitySecretWhenRequired(t *testing.T) {
	cfg := DefaultSubstrateConfig()
	cfg.APIInsecureSkipVerify = true
	cfg.BootstrapSecretName = testSubstrateBootstrapSecretName
	cfg.SessionIdentityRequired = true

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected missing SessionIdentity token secret error")
	}
	if !strings.Contains(err.Error(), "substrate-session-identity-token-secret-name") {
		t.Fatalf("Validate() error = %q, want SessionIdentity secret flag context", err.Error())
	}

	cfg.SessionIdentitySecretName = testSubstrateSessionIdentitySecretName
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with SessionIdentity secret returned error: %v", err)
	}
	if got := cfg.WithDefaults().SessionIdentitySecretKey; got != "token" {
		t.Fatalf("default SessionIdentity secret key = %q, want token", got)
	}
}

func TestSubstrateConfigValidateRejectsSessionIdentityCertificateMinting(t *testing.T) {
	cfg := DefaultSubstrateConfig()
	cfg.APIInsecureSkipVerify = true
	cfg.BootstrapSecretName = testSubstrateBootstrapSecretName
	cfg.SessionIdentitySecretName = testSubstrateSessionIdentitySecretName
	cfg.SessionIdentityMintCert = true

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want unsupported certificate minting error")
	}
	if !strings.Contains(err.Error(), "certificate minting is not supported yet") {
		t.Fatalf("Validate() error = %q, want unsupported certificate minting context", err.Error())
	}
}

func TestValidateSubstrateWorkspaceTemplateRequiresAppStagingRoot(t *testing.T) {
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   "ate.dev",
		Version: "v1alpha1",
		Kind:    "ActorTemplate",
	}, &unstructured.Unstructured{})

	template := &unstructured.Unstructured{}
	template.SetAPIVersion("ate.dev/v1alpha1")
	template.SetKind("ActorTemplate")
	template.SetName("orka-codex")
	template.SetNamespace("ate-demo")
	template.SetLabels(map[string]string{
		"orka.ai/execution-workspace": "true",
		"orka.ai/workspace-provider":  "substrate",
	})
	template.SetAnnotations(map[string]string{
		"orka.ai/workspace-protocol":     "http-json-v1",
		"orka.ai/workspace-daemon-port":  "8080",
		"orka.ai/workspace-staging-root": "/workspace",
	})

	r := &TaskReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(template).Build(),
	}
	err := validateSubstrateActorTemplateResource(context.Background(), r.Client, &ExecutionWorkspaceRequest{
		TemplateName:      "orka-codex",
		TemplateNamespace: "ate-demo",
	})
	if err == nil {
		t.Fatal("validateSubstrateActorTemplateResource() error = nil, want unsupported staging root error")
	}
	if !strings.Contains(err.Error(), "orka.ai/workspace-staging-root=/app") {
		t.Fatalf("error = %q, want /app staging root requirement", err.Error())
	}
}

func TestValidateSubstrateWorkspaceTemplateRequiresReadyPhase(t *testing.T) {
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   "ate.dev",
		Version: "v1alpha1",
		Kind:    "ActorTemplate",
	}, &unstructured.Unstructured{})

	template := &unstructured.Unstructured{}
	template.SetAPIVersion("ate.dev/v1alpha1")
	template.SetKind("ActorTemplate")
	template.SetName("orka-codex")
	template.SetNamespace("ate-demo")
	template.SetLabels(map[string]string{
		"orka.ai/execution-workspace": "true",
		"orka.ai/workspace-provider":  "substrate",
	})
	template.SetAnnotations(map[string]string{
		"orka.ai/workspace-protocol":     "http-json-v1",
		"orka.ai/workspace-daemon-port":  "8080",
		"orka.ai/workspace-staging-root": "/app",
	})

	r := &TaskReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(template).Build(),
	}
	err := validateSubstrateActorTemplateResource(context.Background(), r.Client, &ExecutionWorkspaceRequest{
		TemplateName:      "orka-codex",
		TemplateNamespace: "ate-demo",
	})
	if err == nil {
		t.Fatal("validateSubstrateActorTemplateResource() error = nil, want missing readiness error")
	}
	if !strings.Contains(err.Error(), "is not Ready: phase=<empty>") {
		t.Fatalf("error = %q, want missing readiness context", err.Error())
	}
}

func TestValidateSubstrateWorkspaceTemplateRequiresBootstrapTokenEnv(t *testing.T) {
	template := readySubstrateActorTemplateForTest(nil)
	r := substrateTemplateValidatorForTest(t, template)

	err := validateSubstrateActorTemplateResource(context.Background(), r.Client, substrateTemplateRequestForTest())
	if err == nil {
		t.Fatal("validateSubstrateActorTemplateResource() error = nil, want missing bootstrap env error")
	}
	if !strings.Contains(err.Error(), workerenv.WorkspaceBootstrapToken) {
		t.Fatalf("error = %q, want bootstrap env context", err.Error())
	}
}

func TestValidateSubstrateWorkspaceTemplateAcceptsBootstrapTokenSecretRef(t *testing.T) {
	template := readySubstrateActorTemplateForTest([]any{
		map[string]any{
			"name": workerenv.WorkspaceBootstrapToken,
			"valueFrom": map[string]any{
				"secretKeyRef": map[string]any{
					"name": testSubstrateBootstrapSecretName,
					"key":  testSubstrateBootstrapSecretKey,
				},
			},
		},
	})
	r := substrateTemplateValidatorForTest(t, template)

	if err := validateSubstrateActorTemplateResource(context.Background(), r.Client, substrateTemplateRequestForTest()); err != nil {
		t.Fatalf("validateSubstrateActorTemplateResource() error = %v", err)
	}
}

func TestValidateSubstrateWorkspaceTemplateAcceptsLiteralBootstrapTokenEnv(t *testing.T) {
	template := readySubstrateActorTemplateForTest([]any{
		map[string]any{
			"name":  workerenv.WorkspaceBootstrapToken,
			"value": "bootstrap-token",
		},
	})
	r := substrateTemplateValidatorForTest(t, template)

	if err := validateSubstrateActorTemplateResource(context.Background(), r.Client, substrateTemplateRequestForTest()); err != nil {
		t.Fatalf("validateSubstrateActorTemplateResource() error = %v", err)
	}
}

func TestValidateSubstrateWorkspaceTemplateRejectsDaemonPortMismatch(t *testing.T) {
	template := readySubstrateActorTemplateWithContainersForTest([]any{
		map[string]any{
			"name":    "workspace",
			"command": []any{"/orka-workspace-agent"},
			"env": []any{
				map[string]any{
					"name":  workerenv.WorkspaceBootstrapToken,
					"value": "bootstrap-token",
				},
			},
		},
	})
	annotations := template.GetAnnotations()
	annotations["orka.ai/workspace-daemon-port"] = "80"
	template.SetAnnotations(annotations)
	r := substrateTemplateValidatorForTest(t, template)

	err := validateSubstrateActorTemplateResource(context.Background(), r.Client, substrateTemplateRequestForTest())
	if err == nil {
		t.Fatal("validateSubstrateActorTemplateResource() error = nil, want daemon port mismatch error")
	}
	if !strings.Contains(err.Error(), `workspace daemon container "workspace" listen port 8080`) ||
		!strings.Contains(err.Error(), "orka.ai/workspace-daemon-port=80") {
		t.Fatalf("error = %q, want daemon port mismatch context", err.Error())
	}
}

func TestValidateSubstrateWorkspaceTemplateRequiresBootstrapTokenOnDaemonContainer(t *testing.T) {
	template := readySubstrateActorTemplateWithContainersForTest([]any{
		map[string]any{
			"name": "sidecar",
			"env": []any{
				map[string]any{
					"name":  workerenv.WorkspaceBootstrapToken,
					"value": "bootstrap-token",
				},
			},
		},
		map[string]any{
			"name":    "workspace",
			"command": []any{"/orka-workspace-agent"},
			"env": []any{
				substrateWorkspaceDaemonListenEnvForTest(),
			},
		},
	})
	r := substrateTemplateValidatorForTest(t, template)

	err := validateSubstrateActorTemplateResource(context.Background(), r.Client, substrateTemplateRequestForTest())
	if err == nil {
		t.Fatal("validateSubstrateActorTemplateResource() error = nil, want daemon bootstrap env error")
	}
	if !strings.Contains(err.Error(), `workspace daemon container "workspace"`) ||
		!strings.Contains(err.Error(), workerenv.WorkspaceBootstrapToken) {
		t.Fatalf("error = %q, want daemon bootstrap env context", err.Error())
	}
}

func TestValidateSubstrateWorkspaceTemplateAcceptsBootstrapTokenOnDaemonContainer(t *testing.T) {
	template := readySubstrateActorTemplateWithContainersForTest([]any{
		map[string]any{
			"name": "sidecar",
		},
		map[string]any{
			"name":    "workspace",
			"command": []any{"/orka-workspace-agent"},
			"env": []any{
				substrateWorkspaceDaemonListenEnvForTest(),
				map[string]any{
					"name": workerenv.WorkspaceBootstrapToken,
					"valueFrom": map[string]any{
						"secretKeyRef": map[string]any{
							"name": testSubstrateBootstrapSecretName,
							"key":  testSubstrateBootstrapSecretKey,
						},
					},
				},
			},
		},
	})
	r := substrateTemplateValidatorForTest(t, template)

	if err := validateSubstrateActorTemplateResource(context.Background(), r.Client, substrateTemplateRequestForTest()); err != nil {
		t.Fatalf("validateSubstrateActorTemplateResource() error = %v", err)
	}
}

func TestValidateSubstrateWorkspaceTemplateRequiresDaemonContainerForMultiContainerTemplate(t *testing.T) {
	template := readySubstrateActorTemplateWithContainersForTest([]any{
		map[string]any{
			"name": "sidecar",
			"env": []any{
				map[string]any{
					"name":  workerenv.WorkspaceBootstrapToken,
					"value": "bootstrap-token",
				},
			},
		},
		map[string]any{
			"name": "workspace",
		},
	})
	r := substrateTemplateValidatorForTest(t, template)

	err := validateSubstrateActorTemplateResource(context.Background(), r.Client, substrateTemplateRequestForTest())
	if err == nil {
		t.Fatal("validateSubstrateActorTemplateResource() error = nil, want daemon identification error")
	}
	if !strings.Contains(err.Error(), "must identify the workspace daemon container") {
		t.Fatalf("error = %q, want daemon identification context", err.Error())
	}
}

func TestValidateSubstrateWorkspaceTemplateRejectsMismatchedBootstrapSecretRef(t *testing.T) {
	template := readySubstrateActorTemplateForTest([]any{
		map[string]any{
			"name": workerenv.WorkspaceBootstrapToken,
			"valueFrom": map[string]any{
				"secretKeyRef": map[string]any{
					"name": "other-bootstrap-secret",
					"key":  testSubstrateBootstrapSecretKey,
				},
			},
		},
	})
	r := substrateTemplateValidatorForTest(t, template)

	err := validateSubstrateActorTemplateResource(context.Background(), r.Client, substrateTemplateRequestForTest())
	if err == nil {
		t.Fatal("validateSubstrateActorTemplateResource() error = nil, want mismatched secret error")
	}
	if !strings.Contains(err.Error(), "configured bootstrap Secret") {
		t.Fatalf("error = %q, want configured secret context", err.Error())
	}
}

func TestAgentSandboxConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*AgentSandboxConfig)
		wantErr string
	}{
		{
			name: "invalid warm pool policy",
			mutate: func(cfg *AgentSandboxConfig) {
				cfg.WarmPoolPolicy = "always"
			},
			wantErr: "warm pool policy",
		},
		{
			name: "invalid namespace strategy",
			mutate: func(cfg *AgentSandboxConfig) {
				cfg.NamespaceStrategy = "cluster"
			},
			wantErr: "namespace strategy",
		},
		{
			name: "invalid claim timeout",
			mutate: func(cfg *AgentSandboxConfig) {
				cfg.ClaimTimeout = -time.Second
			},
			wantErr: "claim timeout",
		},
		{
			name: "invalid command timeout",
			mutate: func(cfg *AgentSandboxConfig) {
				cfg.CommandTimeout = -time.Second
			},
			wantErr: "command timeout",
		},
		{
			name: "invalid cleanup policy",
			mutate: func(cfg *AgentSandboxConfig) {
				cfg.CleanupPolicy = corev1alpha1.WorkspaceCleanupPolicy("archive")
			},
			wantErr: "cleanup policy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultAgentSandboxConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func substrateTemplateRequestForTest() *ExecutionWorkspaceRequest {
	return &ExecutionWorkspaceRequest{
		TemplateName:                 "orka-codex",
		TemplateNamespace:            "ate-demo",
		SubstrateBootstrapSecretName: testSubstrateBootstrapSecretName,
		SubstrateBootstrapSecretKey:  testSubstrateBootstrapSecretKey,
	}
}

func substrateTemplateValidatorForTest(t *testing.T, template *unstructured.Unstructured) *TaskReconciler {
	t.Helper()

	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   "ate.dev",
		Version: "v1alpha1",
		Kind:    "ActorTemplate",
	}, &unstructured.Unstructured{})
	return &TaskReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(template).Build(),
	}
}

func readySubstrateActorTemplateForTest(env []any) *unstructured.Unstructured {
	daemonEnv := append([]any{substrateWorkspaceDaemonListenEnvForTest()}, env...)
	return readySubstrateActorTemplateWithContainersForTest([]any{
		map[string]any{
			"name":    "workspace",
			"command": []any{"/orka-workspace-agent"},
			"env":     daemonEnv,
		},
	})
}

func substrateWorkspaceDaemonListenEnvForTest() map[string]any {
	return map[string]any{
		"name":  substrateWorkspaceDaemonListenEnv,
		"value": ":8080",
	}
}

func readySubstrateActorTemplateWithContainersForTest(containers []any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "ate.dev/v1alpha1",
		"kind":       "ActorTemplate",
		"metadata": map[string]any{
			"name":      "orka-codex",
			"namespace": "ate-demo",
			"labels": map[string]any{
				"orka.ai/execution-workspace": "true",
				"orka.ai/workspace-provider":  "substrate",
			},
			"annotations": map[string]any{
				"orka.ai/workspace-protocol":     "http-json-v1",
				"orka.ai/workspace-daemon-port":  "8080",
				"orka.ai/workspace-staging-root": "/app",
			},
		},
		"spec": map[string]any{
			"containers": containers,
		},
		"status": map[string]any{
			"phase": "Ready",
		},
	}}
}
