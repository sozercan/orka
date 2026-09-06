/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package workerenv

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAIWorkerEnvRoundTrip(t *testing.T) {
	env := AIWorkerEnv{
		BaseEnv: BaseEnv{
			TaskName:           "task-1",
			TaskNamespace:      "default",
			ResultEndpoint:     "http://controller/results/default/task-1",
			ControllerURL:      "http://controller",
			AgentName:          "agent-a",
			TransactionID:      "txn-123",
			TransactionProfile: "transaction-token",
		},
		Provider:                         "openai",
		Model:                            "gpt-5",
		Prompt:                           "do work",
		SystemPrompt:                     "be concise",
		BaseURL:                          "https://example.test/v1",
		AzureAPIVersion:                  "2024-10-21",
		Tools:                            []string{"delegate_task", "wait_for_tasks"},
		EnforceTransactionCredentialAuth: true,
		TransactionCredentialReadScopes:  []string{"tenant:outbound-credentials:read"},
		EnableTelemetry:                  true,
		TraceParent:                      "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		TraceState:                       "vendor=value",
		TraceBaggage:                     "tenant=acme",
		ControllerMode:                   "harness-v2",
		Fallbacks: []FallbackProviderEnv{{
			Provider:        "anthropic",
			APIKey:          "secret",
			Model:           "claude",
			BaseURL:         "https://anthropic.example",
			AzureAPIVersion: "ignored",
		}},
	}

	values := map[string]string{}
	for _, envVar := range env.EnvVars() {
		values[envVar.Name] = envVar.Value
	}

	parsed := ParseAIWorkerEnv(func(name string) string { return values[name] })
	if err := parsed.ValidateRequired(); err != nil {
		t.Fatalf("ValidateRequired() returned error: %v", err)
	}
	if parsed.BaseEnv != env.BaseEnv {
		t.Fatalf("base env mismatch: got %#v, want %#v", parsed.BaseEnv, env.BaseEnv)
	}
	if parsed.Provider != env.Provider || parsed.Model != env.Model || parsed.Prompt != env.Prompt {
		t.Fatalf("AI env mismatch: got %#v, want %#v", parsed, env)
	}
	if len(parsed.Tools) != 2 || parsed.Tools[0] != "delegate_task" || parsed.Tools[1] != "wait_for_tasks" {
		t.Fatalf("tools = %#v", parsed.Tools)
	}
	if !parsed.EnforceTransactionCredentialAuth ||
		len(parsed.TransactionCredentialReadScopes) != 1 ||
		parsed.TransactionCredentialReadScopes[0] != "tenant:outbound-credentials:read" {
		t.Fatalf("transaction credential env mismatch: got %#v", parsed)
	}
	if !parsed.EnableTelemetry || parsed.TraceParent != env.TraceParent || parsed.TraceState != env.TraceState || parsed.TraceBaggage != env.TraceBaggage {
		t.Fatalf("telemetry env mismatch: got %#v, want parent=%q state=%q baggage=%q", parsed, env.TraceParent, env.TraceState, env.TraceBaggage)
	}
	if parsed.ControllerMode != env.ControllerMode {
		t.Fatalf("controller mode = %q, want %q", parsed.ControllerMode, env.ControllerMode)
	}
	if len(parsed.Fallbacks) != 1 {
		t.Fatalf("fallback count = %d, want 1", len(parsed.Fallbacks))
	}
	if parsed.Fallbacks[0] != env.Fallbacks[0] {
		t.Fatalf("fallback = %#v, want %#v", parsed.Fallbacks[0], env.Fallbacks[0])
	}
}

func TestParseFallbacksInvalidCountPreservesLegacyBehavior(t *testing.T) {
	values := map[string]string{AIFallbackCount: "not-an-int"}
	fallbacks := ParseFallbacks(func(name string) string { return values[name] })
	if len(fallbacks) != 0 {
		t.Fatalf("fallbacks = %#v, want none", fallbacks)
	}
}

func TestReadTokenFileEnv(t *testing.T) {
	const envName = "TEST_TOKEN_FILE_ENV"

	t.Run("unset", func(t *testing.T) {
		t.Setenv(envName, "")
		token, ok, err := ReadTokenFileEnv(envName, "test token")
		if err != nil {
			t.Fatalf("ReadTokenFileEnv() error = %v", err)
		}
		if ok {
			t.Fatal("ReadTokenFileEnv() ok = true, want false")
		}
		if token != "" {
			t.Fatalf("ReadTokenFileEnv() token = %q, want empty", token)
		}
	})

	t.Run("trims token file content", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte("  token\n"), 0600); err != nil {
			t.Fatalf("failed to write token fixture: %v", err)
		}
		t.Setenv(envName, path)

		token, ok, err := ReadTokenFileEnv(envName, "test token")
		if err != nil {
			t.Fatalf("ReadTokenFileEnv() error = %v", err)
		}
		if !ok {
			t.Fatal("ReadTokenFileEnv() ok = false, want true")
		}
		if token != "token" {
			t.Fatalf("ReadTokenFileEnv() token = %q, want token", token)
		}
	})

	t.Run("whitespace only token file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte(" \n\t "), 0600); err != nil {
			t.Fatalf("failed to write token fixture: %v", err)
		}
		t.Setenv(envName, path)

		_, ok, err := ReadTokenFileEnv(envName, "test token")
		if err == nil || !strings.Contains(err.Error(), "empty") {
			t.Fatalf("ReadTokenFileEnv() error = %v, want empty token error", err)
		}
		if !ok {
			t.Fatal("ReadTokenFileEnv() ok = false, want true")
		}
	})

	t.Run("missing token file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing-token")
		t.Setenv(envName, path)

		_, ok, err := ReadTokenFileEnv(envName, "test token")
		if err == nil || !strings.Contains(err.Error(), "failed to read test token file") {
			t.Fatalf("ReadTokenFileEnv() error = %v, want read error", err)
		}
		if !ok {
			t.Fatal("ReadTokenFileEnv() ok = false, want true")
		}
	})
}

func TestRequireTokenFileEnvUnset(t *testing.T) {
	const envName = "TEST_REQUIRED_TOKEN_FILE_ENV"
	t.Setenv(envName, "")

	_, err := RequireTokenFileEnv(envName, "required token")
	if err == nil {
		t.Fatal("RequireTokenFileEnv() error = nil, want required error")
	}
	if got, want := err.Error(), envName+" is required"; got != want {
		t.Fatalf("RequireTokenFileEnv() error = %q, want %q", got, want)
	}
}

func TestAIWorkerEnvValidateRequired(t *testing.T) {
	for _, tt := range []struct {
		name string
		env  AIWorkerEnv
		want string
	}{
		{name: "missing provider", env: AIWorkerEnv{Model: "m", Prompt: "p"}, want: AIProvider},
		{name: "missing model", env: AIWorkerEnv{Provider: "p", Prompt: "prompt"}, want: AIModel},
		{name: "missing prompt", env: AIWorkerEnv{Provider: "p", Model: "m"}, want: AIPrompt},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.env.ValidateRequired()
			if err == nil {
				t.Fatal("expected error")
			}
			if err.Error() != tt.want+" is required" {
				t.Fatalf("error = %q, want %q", err.Error(), tt.want+" is required")
			}
		})
	}
}

func TestAgentSandboxEnvVarsDisabledReturnsEmpty(t *testing.T) {
	if got := (AgentSandboxEnv{}).EnvVars(); len(got) != 0 {
		t.Fatalf("disabled AgentSandboxEnv.EnvVars() length = %d, want 0", len(got))
	}
}

func TestExecutionWorkspaceEnvRender(t *testing.T) {
	env := ExecutionWorkspaceEnv{
		Enabled:           true,
		Provider:          "substrate",
		TemplateName:      "orka-codex",
		TemplateNamespace: "ate-demo",
		ClaimNamespace:    "ate-demo",
		ClaimName:         "orka-s-abc",
		ReusePolicy:       "session",
		ReuseKey:          "session-1",
		CleanupPolicy:     "retain",
		Boot:              true,
		ClaimTimeout:      2 * time.Minute,
		CommandTimeout:    30 * time.Minute,
		StatusEndpoint:    "http://orka/internal/v1/tasks/default/task/execution-workspace/status",
		Depth:             0,
	}

	values := map[string]string{}
	for _, envVar := range env.EnvVars() {
		values[envVar.Name] = envVar.Value
	}

	want := map[string]string{
		ExecutionWorkspaceEnabled:               "true",
		ExecutionWorkspaceProvider:              env.Provider,
		ExecutionWorkspaceClaimName:             env.ClaimName,
		ExecutionWorkspaceBoot:                  "true",
		ExecutionWorkspaceClaimTimeoutSeconds:   "120",
		ExecutionWorkspaceCommandTimeoutSeconds: "1800",
		ExecutionWorkspaceStatusEndpoint:        env.StatusEndpoint,
		ExecutionWorkspaceDepth:                 "0",
	}
	for name, wantValue := range want {
		if values[name] != wantValue {
			t.Fatalf("%s = %q, want %q", name, values[name], wantValue)
		}
	}
}

func TestSubstrateEnvRender(t *testing.T) {
	env := SubstrateEnv{
		APIEndpoint:             "api.ate-system.svc:443",
		APICAFile:               "/var/run/orka/substrate/ca.crt",
		APIInsecureSkipVerify:   true,
		RouterURL:               "http://atenet-router.ate-system.svc",
		ActorDNSSuffix:          "actors.resources.substrate.ate.dev",
		SessionIdentityToken:    "session-identity-token",
		SessionIdentityRequired: true,
		SessionIdentityAudience: "orka-workspace-daemon,custom-audience",
		SessionIdentityAppID:    "orka",
		SessionIdentityUserID:   "orka-worker",
	}

	values := map[string]string{}
	for _, envVar := range env.EnvVars() {
		values[envVar.Name] = envVar.Value
	}

	want := map[string]string{
		SubstrateAPIEndpoint:             env.APIEndpoint,
		SubstrateAPIInsecureSkipVerify:   "true",
		SubstrateRouterURL:               env.RouterURL,
		SubstrateSessionIdentityToken:    env.SessionIdentityToken,
		SubstrateSessionIdentityRequired: "true",
		SubstrateSessionIdentityAudience: env.SessionIdentityAudience,
		SubstrateSessionIdentityAppID:    env.SessionIdentityAppID,
		SubstrateSessionIdentityUserID:   env.SessionIdentityUserID,
	}
	for name, wantValue := range want {
		if values[name] != wantValue {
			t.Fatalf("%s = %q, want %q", name, values[name], wantValue)
		}
	}
}

func TestAgentSandboxEnvRender(t *testing.T) {
	env := AgentSandboxEnv{
		Enabled:           true,
		RouterURL:         "http://sandbox-router",
		TemplateName:      "agent-template",
		TemplateNamespace: "sandbox-system",
		ClaimNamespace:    "sandbox-system",
		ReusePolicy:       "session",
		ReuseKey:          "session-1",
		CleanupPolicy:     "retain",
		WarmPoolPolicy:    "template",
		NamespaceStrategy: "task",
		ClaimTimeout:      2 * time.Minute,
		CommandTimeout:    30 * time.Minute,
	}

	values := map[string]string{}
	for _, envVar := range env.EnvVars() {
		values[envVar.Name] = envVar.Value
	}

	want := map[string]string{
		AgentSandboxEnabled:               "true",
		AgentSandboxDepth:                 "0",
		AgentSandboxTemplateName:          env.TemplateName,
		AgentSandboxTemplateNamespace:     env.TemplateNamespace,
		AgentSandboxClaimNamespace:        env.ClaimNamespace,
		AgentSandboxReusePolicy:           env.ReusePolicy,
		AgentSandboxReuseKey:              env.ReuseKey,
		AgentSandboxCleanupPolicy:         env.CleanupPolicy,
		AgentSandboxClaimTimeoutSeconds:   "120",
		AgentSandboxCommandTimeoutSeconds: "1800",
	}
	for name, wantValue := range want {
		if values[name] != wantValue {
			t.Fatalf("%s = %q, want %q", name, values[name], wantValue)
		}
	}
}

func TestCoordinationEnvRenderAndParse(t *testing.T) {
	env := CoordinationEnv{
		Enabled:                 true,
		MaxDepth:                3,
		MaxChildren:             4,
		AllowedAgents:           []string{"builder", "reviewer"},
		Depth:                   "1",
		AutonomousMode:          true,
		AutonomousIteration:     2,
		AutonomousMaxIterations: 8,
	}
	values := map[string]string{}
	for _, envVar := range env.EnvVars() {
		values[envVar.Name] = envVar.Value
	}

	parsed := ParseCoordinationEnv(func(name string) string { return values[name] })
	if !parsed.Enabled || !parsed.AutonomousMode {
		t.Fatalf("parsed flags = %#v", parsed)
	}
	if parsed.MaxDepth != 3 || parsed.MaxChildren != 4 || parsed.Depth != "1" || parsed.AutonomousIteration != 2 || parsed.AutonomousMaxIterations != 8 {
		t.Fatalf("parsed numeric/depth values = %#v", parsed)
	}
	if len(parsed.AllowedAgents) != 2 || parsed.AllowedAgents[0] != "builder" || parsed.AllowedAgents[1] != "reviewer" {
		t.Fatalf("allowed agents = %#v", parsed.AllowedAgents)
	}
}

func TestTransactionLogFields(t *testing.T) {
	if got := TransactionLogFields("", ""); got != "" {
		t.Fatalf("TransactionLogFields empty = %q, want empty", got)
	}
	got := TransactionLogFields("txn-123", "transaction-token")
	want := ` transactionID="txn-123" contextTokenProfile="transaction-token"`
	if got != want {
		t.Fatalf("TransactionLogFields() = %q, want %q", got, want)
	}
}

func TestTransactionLogFields_EscapesLogForgingCharacters(t *testing.T) {
	got := TransactionLogFields("txn 123", "transaction-token\nforged=true")
	want := ` transactionID="txn 123" contextTokenProfile="transaction-token\nforged=true"`
	if got != want {
		t.Fatalf("TransactionLogFields() = %q, want %q", got, want)
	}
	if strings.Contains(got, "\n") {
		t.Fatalf("TransactionLogFields() contains a literal newline: %q", got)
	}
}

func TestCoordinationEnvRoundTripIncludesApprovalRequiredTools(t *testing.T) {
	env := CoordinationEnv{
		Enabled:               true,
		MaxDepth:              3,
		MaxChildren:           5,
		AllowedAgents:         []string{"incident"},
		Depth:                 "1",
		AutonomousMode:        true,
		AutonomousIteration:   2,
		ApprovalRequiredTools: []string{"dispatch_work_order", "escalate_incident"},
	}
	values := map[string]string{}
	for _, envVar := range env.EnvVars() {
		values[envVar.Name] = envVar.Value
	}
	parsed := ParseCoordinationEnv(func(name string) string { return values[name] })
	if strings.Join(parsed.ApprovalRequiredTools, ",") != "dispatch_work_order,escalate_incident" {
		t.Fatalf("ApprovalRequiredTools = %#v", parsed.ApprovalRequiredTools)
	}
}

func TestAIWorkerEnvTelemetryEnablement(t *testing.T) {
	env := map[string]string{
		AIProvider:      "openai",
		AIModel:         "gpt-4o",
		AIPrompt:        "hello",
		EnableTelemetry: "true",
		TraceParent:     "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		TraceState:      "vendor=value",
	}
	got := ParseAIWorkerEnv(func(key string) string { return env[key] })
	if !got.EnableTelemetry {
		t.Fatal("EnableTelemetry = false, want true")
	}
	if got.TraceParent != env[TraceParent] {
		t.Fatalf("TraceParent = %q, want %q", got.TraceParent, env[TraceParent])
	}
	if got.TraceState != env[TraceState] {
		t.Fatalf("TraceState = %q, want %q", got.TraceState, env[TraceState])
	}
	if got.TraceBaggage != env[TraceBaggage] {
		t.Fatalf("TraceBaggage = %q, want %q", got.TraceBaggage, env[TraceBaggage])
	}
}

func TestAIWorkerEnvTelemetryDoesNotAutoEnableFromOTLPEndpoint(t *testing.T) {
	env := map[string]string{
		AIProvider:                    "openai",
		AIModel:                       "gpt-4o",
		AIPrompt:                      "hello",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "otel-collector:4317",
	}
	got := ParseAIWorkerEnv(func(key string) string { return env[key] })
	if got.EnableTelemetry {
		t.Fatal("EnableTelemetry = true, want false without ORKA_ENABLE_TELEMETRY")
	}
}
