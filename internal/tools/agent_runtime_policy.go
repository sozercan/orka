/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/agentruntimepolicy"
)

type resolvedRuntimeRefPolicy struct {
	providerKind    string
	model           string
	allowedTools    []string
	disallowedTools []string
	allowBash       bool
}

func toolPolicyReader(ctx context.Context, fallback client.Reader) client.Reader {
	if toolCtx := GetToolContext(ctx); toolCtx != nil && toolCtx.PolicyReader != nil {
		return toolCtx.PolicyReader
	}
	return fallback
}

func resolveRuntimeRefPolicy(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	agent *corev1alpha1.Agent,
) (*resolvedRuntimeRefPolicy, error) {
	policy, err := agentruntimepolicy.ResolveRuntimeRefPolicy(ctx, reader, namespace, agent)
	if err != nil || policy == nil {
		return nil, err
	}
	return &resolvedRuntimeRefPolicy{
		providerKind:    policy.ProviderKind,
		model:           policy.Model,
		allowedTools:    append([]string{}, policy.AllowedTools...),
		disallowedTools: append([]string{}, policy.DisallowedTools...),
		allowBash:       policy.AllowBash,
	}, nil
}

// materializeRuntimeRefAllowedTools copies the registered harness-v2 allowlist
// into a generated Task. The AgentRuntime remains the policy authority; the
// Task copy makes the exact requested broker exposure explicit for binding.
func materializeRuntimeRefAllowedTools(
	ctx context.Context,
	reader client.Reader,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
) error {
	return agentruntimepolicy.ResolveAndMaterializeRuntimeRefAllowedTools(ctx, reader, task, agent)
}
