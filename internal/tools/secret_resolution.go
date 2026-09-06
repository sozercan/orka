/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/workerenv"
)

var gitCredentialSecretCandidates = []string{"git-credentials", "github-credentials", "copilot-token", "github-token", "git-token"}

func resolveWorkspaceCredentialRef(ctx context.Context, k8sClient client.Reader, namespace string, agent *corev1alpha1.Agent, requested string) (*corev1alpha1.WorkspaceCredentialReference, error) {
	if requested != "" {
		return &corev1alpha1.WorkspaceCredentialReference{Name: requested}, nil
	}

	if agent != nil &&
		agent.Spec.Runtime != nil &&
		agent.Spec.Runtime.Type == corev1alpha1.AgentRuntimeCopilot &&
		agent.Spec.SecretRef != nil &&
		agent.Spec.SecretRef.Name != "" {
		return &corev1alpha1.WorkspaceCredentialReference{Name: agent.Spec.SecretRef.Name}, nil
	}

	name, err := firstExistingSecretName(ctx, k8sClient, namespace, append([]string(nil), gitCredentialSecretCandidates...))
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, nil
	}
	return &corev1alpha1.WorkspaceCredentialReference{Name: name}, nil
}

func validateGitCredentialSecret(ctx context.Context, k8sClient client.Reader, namespace, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	if k8sClient == nil {
		return fmt.Errorf("git secretRef %q requires a Kubernetes client", name)
	}
	secret := &corev1.Secret{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("git secretRef %q not found in namespace %q", name, namespace)
		}
		return fmt.Errorf("failed to get git secretRef %q in namespace %q: %w", name, namespace, err)
	}
	if !gitCredentialSecretHasToken(secret) {
		return fmt.Errorf("git secretRef %q in namespace %q must contain a non-empty token, password, or %s key", name, namespace, workerenv.GitHubToken)
	}
	return nil
}

func gitCredentialSecretHasToken(secret *corev1.Secret) bool {
	if secret == nil {
		return false
	}
	for _, key := range []string{tokenKey, passwordKey, workerenv.GitHubToken} {
		if value := strings.TrimSpace(string(secret.Data[key])); value != "" {
			return true
		}
	}
	return false
}

func taskWorkspace(task *corev1alpha1.Task) *corev1alpha1.WorkspaceConfig {
	if task == nil {
		return nil
	}
	return task.Spec.Workspace
}

func loadAgent(ctx context.Context, k8sClient client.Reader, namespace, agentName string) (*corev1alpha1.Agent, error) {
	if agentName == "" {
		return nil, nil
	}

	agent := &corev1alpha1.Agent{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: agentName, Namespace: namespace}, agent); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get agent %q: %w", agentName, err)
	}
	return agent, nil
}

func firstExistingSecretName(ctx context.Context, k8sClient client.Reader, namespace string, candidates []string) (string, error) {
	for _, name := range candidates {
		exists, err := secretExists(ctx, k8sClient, namespace, name)
		// Optional discovery only considers candidates the caller can read.
		if apierrors.IsForbidden(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if exists {
			return name, nil
		}
	}
	return "", nil
}

func secretExists(ctx context.Context, k8sClient client.Reader, namespace, name string) (bool, error) {
	_, exists, err := getSecret(ctx, k8sClient, namespace, name)
	return exists, err
}

func getSecret(ctx context.Context, k8sClient client.Reader, namespace, name string) (*corev1.Secret, bool, error) {
	secret := &corev1.Secret{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to get secret %q: %w", name, err)
	}
	return secret, true, nil
}
