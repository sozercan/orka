/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"crypto/sha256"
	"fmt"
	"strings"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/workspace/statusrules"
)

const (
	EnvExecutionWorkspaceDefaultProvider = "ORKA_EXECUTION_WORKSPACE_DEFAULT_PROVIDER"

	defaultExecutionWorkspaceProvider = corev1alpha1.WorkspaceProviderAgentSandbox
)

// ExecutionWorkspaceDefaultProviderFromEnv reads the provider-neutral default
// workspace backend. Empty preserves the compatibility default.
func ExecutionWorkspaceDefaultProviderFromEnv(getenv func(string) string) corev1alpha1.WorkspaceProvider {
	if value := strings.TrimSpace(getenv(EnvExecutionWorkspaceDefaultProvider)); value != "" {
		return corev1alpha1.WorkspaceProvider(value)
	}
	return defaultExecutionWorkspaceProvider
}

func executionWorkspaceDefaultProvider(provider corev1alpha1.WorkspaceProvider) corev1alpha1.WorkspaceProvider {
	if provider == "" {
		return defaultExecutionWorkspaceProvider
	}
	return provider
}

// ExecutionWorkspaceRequest identifies a Substrate ActorTemplate (plus the
// controller-configured bootstrap Secret) for template validation by the Tool
// and SubstrateActorPool reconcilers.
type ExecutionWorkspaceRequest struct {
	TemplateName      string
	TemplateNamespace string

	SubstrateBootstrapSecretName string
	SubstrateBootstrapSecretKey  string
}

func resolveWorkspaceProvider(ws *corev1alpha1.ExecutionWorkspaceSpec, defaultProvider corev1alpha1.WorkspaceProvider) corev1alpha1.WorkspaceProvider {
	if ws != nil && ws.Provider != "" {
		return ws.Provider
	}
	return executionWorkspaceDefaultProvider(defaultProvider)
}

func supportedWorkspaceProvider(provider corev1alpha1.WorkspaceProvider) bool {
	return statusrules.IsSupportedProvider(provider)
}

// WorkspaceProviderSupported reports whether provider is a recognized execution
// workspace backend.
func WorkspaceProviderSupported(provider corev1alpha1.WorkspaceProvider) bool {
	return supportedWorkspaceProvider(provider)
}

func deterministicSubstratePoolActorID(prefix string, ordinal int) string {
	return fmt.Sprintf("%s-%05d", strings.Trim(strings.TrimSpace(prefix), "-"), ordinal)
}

func deterministicSubstratePoolActorOrdinal(target int32, parts ...string) int {
	if target <= 0 {
		return 0
	}
	trimmedParts := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmedParts = append(trimmedParts, strings.TrimSpace(part))
	}
	sum := sha256.Sum256([]byte(strings.Join(trimmedParts, "\x00")))
	ordinal := 0
	for _, b := range sum {
		ordinal = (ordinal*256 + int(b)) % int(target)
	}
	return ordinal
}
