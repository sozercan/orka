/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/orka-agents/orka/internal/workerenv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

const githubPRStateClosed = "closed"

type githubCredentialPolicy uint8

const (
	githubCredentialRead githubCredentialPolicy = iota
	githubCredentialForgeMutation
)

type githubRepoScope struct {
	source            string
	owner             string
	repo              string
	readCredentialRef *corev1alpha1.WorkspaceCredentialReference
}

type githubTaskContext struct {
	scopes             []githubRepoScope
	forgeCredentialRef *corev1alpha1.WorkspaceCredentialReference
}

// resolveReadRepoAndToken resolves GitHub owner/repo, read-only auth token, and API base URL.
// Repository resolution prefers an explicit repoURL, then task workspace config,
// then ORKA_GIT_REPO when no task context is available. If taskName, or an
// implicit ToolContext task, is available alongside repoURL, repoURL still
// selects the repository while the task workspace supplies repository scope
// and can supply credentials.
//
// For task-scoped calls, read credentials are selected by repository role:
// readCredentialRef for the source repository and publicationReadCredentialRef
// for the publication repository. Repository write and forge mutation credentials
// are never used for read-only calls.
func resolveReadRepoAndToken(ctx context.Context, k8sClient client.Client, taskName, repoURL, overrideBaseURL string) (owner, repo, token, baseURL string, err error) {
	return resolveRepoAndTokenWithPolicy(ctx, k8sClient, taskName, repoURL, overrideBaseURL, false, githubCredentialRead)
}

func resolveScopedReadRepoAndToken(ctx context.Context, k8sClient client.Client, taskName, repoURL, overrideBaseURL string) (owner, repo, token, baseURL string, err error) {
	return resolveRepoAndTokenWithPolicy(ctx, k8sClient, taskName, repoURL, overrideBaseURL, true, githubCredentialRead)
}

// resolveForgeRepoAndToken resolves GitHub owner/repo, forge-mutation auth token,
// and API base URL. Task-scoped mutations require forgeCredentialRef and never
// fall back to publication or read credentials.
func resolveForgeRepoAndToken(ctx context.Context, k8sClient client.Client, taskName, repoURL, overrideBaseURL string) (owner, repo, token, baseURL string, err error) {
	return resolveRepoAndTokenWithPolicy(ctx, k8sClient, taskName, repoURL, overrideBaseURL, false, githubCredentialForgeMutation)
}

func resolveScopedForgeRepoAndToken(ctx context.Context, k8sClient client.Client, taskName, repoURL, overrideBaseURL string) (owner, repo, token, baseURL string, err error) {
	return resolveRepoAndTokenWithPolicy(ctx, k8sClient, taskName, repoURL, overrideBaseURL, true, githubCredentialForgeMutation)
}

func resolveRepoAndTokenWithPolicy(
	ctx context.Context,
	k8sClient client.Client,
	taskName, repoURL, overrideBaseURL string,
	requireRepoURLScope bool,
	credentialPolicy githubCredentialPolicy,
) (owner, repo, token, baseURL string, err error) {
	baseURL = githubAPIBaseURL
	if overrideBaseURL != "" {
		baseURL = overrideBaseURL
	}

	hasRepoURL := strings.TrimSpace(repoURL) != ""
	if hasRepoURL {
		owner, repo, err = parseGitHubRepo(repoURL)
		if err != nil {
			return "", "", "", "", fmt.Errorf("failed to parse GitHub repo from %s: %w", repoURL, err)
		}
	}

	scopeTaskName := githubTaskNameFromContext(ctx, taskName)
	requireTaskCredentials := false
	if tc := GetToolContext(ctx); tc != nil {
		requireTaskCredentials = tc.RequireGitHubTaskCredentials
	}
	if requireTaskCredentials && scopeTaskName == "" {
		return "", "", "", "", fmt.Errorf("task_name or current Task context is required for external GitHub access")
	}
	if hasRepoURL && requireRepoURLScope && scopeTaskName == "" {
		return "", "", "", "", fmt.Errorf("repo_url repository %s/%s requires a permitted repository scope", owner, repo)
	}
	if scopeTaskName != "" {
		taskContext, err := loadGitHubTaskContext(ctx, k8sClient, scopeTaskName)
		if err != nil {
			return "", "", "", "", err
		}
		if owner == "" || repo == "" {
			if len(taskContext.scopes) == 0 {
				return "", "", "", "", fmt.Errorf("task %s workspace has no GitHub repository scope", scopeTaskName)
			}
			owner, repo = taskContext.scopes[0].owner, taskContext.scopes[0].repo
		} else if !githubRepoAllowed(owner, repo, taskContext.scopes) {
			return "", "", "", "", fmt.Errorf(
				"repo_url repository %s/%s does not match permitted repository scope %s",
				owner,
				repo,
				formatGitHubRepoScopes(taskContext.scopes),
			)
		}

		credentialRef, err := taskContext.credentialRef(scopeTaskName, owner, repo, credentialPolicy)
		if err != nil {
			return "", "", "", "", err
		}
		token, err = resolveGitSecretToken(ctx, k8sClient, credentialRef)
		if err != nil {
			return "", "", "", "", err
		}
	}

	if owner == "" || repo == "" {
		if scopeTaskName != "" {
			return "", "", "", "", fmt.Errorf("task %s workspace has no GitHub repository scope", scopeTaskName)
		}
		envRepo := os.Getenv(workerenv.GitRepo)
		if envRepo == "" {
			return "", "", "", "", fmt.Errorf("no repo_url, task_name, or ORKA_GIT_REPO provided")
		}
		owner, repo, err = parseGitHubRepo(envRepo)
		if err != nil {
			return "", "", "", "", fmt.Errorf("failed to parse GitHub repo from ORKA_GIT_REPO: %w", err)
		}
	}

	if token == "" {
		if requireTaskCredentials {
			return "", "", "", "", fmt.Errorf("task %s requires an explicit GitHub credential reference", scopeTaskName)
		}
		token = resolveToken()
	}

	if token == "" {
		return "", "", "", "", fmt.Errorf("could not resolve GitHub token from task secret, /secrets/git/token, /secrets/git/password, or GITHUB_TOKEN")
	}

	return owner, repo, token, baseURL, nil
}

func githubRepoAllowed(owner, repo string, scopes []githubRepoScope) bool {
	for _, scope := range scopes {
		if githubRepoMatches(owner, repo, scope.owner, scope.repo) {
			return true
		}
	}
	return false
}

func loadGitHubTaskContext(ctx context.Context, k8sClient client.Client, taskName string) (githubTaskContext, error) {
	taskName = githubTaskNameFromContext(ctx, taskName)
	if strings.TrimSpace(taskName) == "" {
		return githubTaskContext{}, nil
	}
	if k8sClient == nil {
		return githubTaskContext{}, fmt.Errorf("task_name %q requires a Kubernetes client for repo_url scope validation", taskName)
	}

	ns := githubTaskNamespace(ctx)

	var task corev1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: taskName, Namespace: ns}, &task); err != nil {
		return githubTaskContext{}, fmt.Errorf("failed to get task %s: %w", taskName, err)
	}

	var result githubTaskContext
	var scopes []githubRepoScope
	ws := githubTaskWorkspace(&task)
	if ws != nil {
		result.forgeCredentialRef = ws.ForgeCredentialRef
	}
	if ws != nil && strings.TrimSpace(ws.GitRepo) != "" {
		owner, repo, err := parseGitHubRepo(ws.GitRepo)
		if err != nil {
			return githubTaskContext{}, fmt.Errorf("failed to parse GitHub repo from %s: %w", ws.GitRepo, err)
		}
		scopes = append(scopes, githubRepoScope{
			source:            "task workspace",
			owner:             owner,
			repo:              repo,
			readCredentialRef: ws.ReadCredentialRef,
		})
	}
	if ws != nil && strings.TrimSpace(ws.PublicationGitRepo) != "" {
		owner, repo, err := parseGitHubRepo(ws.PublicationGitRepo)
		if err != nil {
			return githubTaskContext{}, fmt.Errorf("failed to parse GitHub publication repo from %s: %w", ws.PublicationGitRepo, err)
		}
		scopes = append(scopes, githubRepoScope{
			source:            "task publication workspace",
			owner:             owner,
			repo:              repo,
			readCredentialRef: ws.PublicationReadCredentialRef,
		})
	}
	if task.Spec.Transaction != nil {
		if txRepo := strings.TrimSpace(task.Spec.Transaction.Context["repo"]); txRepo != "" {
			owner, repo, err := parseGitHubRepo(txRepo)
			if err != nil {
				return githubTaskContext{}, fmt.Errorf("failed to parse transaction repo context: %w", err)
			}
			scopes = append(scopes, githubRepoScope{source: "transaction repo context", owner: owner, repo: repo})
		}
	}
	if len(scopes) == 0 {
		if ws == nil {
			return githubTaskContext{}, fmt.Errorf("task %s does not have workspace configuration", taskName)
		}
		if strings.TrimSpace(ws.GitRepo) == "" {
			return githubTaskContext{}, fmt.Errorf("task %s workspace has no gitRepo configured", taskName)
		}
	}
	result.scopes = scopes
	return result, nil
}

func githubRepoMatches(owner, repo, wantOwner, wantRepo string) bool {
	return strings.EqualFold(owner, wantOwner) && strings.EqualFold(repo, wantRepo)
}

func githubTaskNameFromContext(ctx context.Context, taskName string) string {
	if taskName = strings.TrimSpace(taskName); taskName != "" {
		return taskName
	}
	if tc := GetToolContext(ctx); tc != nil {
		return strings.TrimSpace(tc.TaskID)
	}
	return ""
}

func githubTaskNamespace(ctx context.Context) string {
	if tc := GetToolContext(ctx); tc != nil {
		if ns := strings.TrimSpace(tc.Namespace); ns != "" {
			return ns
		}
	}
	if ns := os.Getenv(envOrkaTaskNamespace); ns != "" {
		return ns
	}
	return defaultNamespace
}

func formatGitHubRepoScopes(scopes []githubRepoScope) string {
	parts := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		parts = append(parts, fmt.Sprintf("%s=%s/%s", scope.source, scope.owner, scope.repo))
	}
	return strings.Join(parts, ", ")
}

func githubTaskWorkspace(task *corev1alpha1.Task) *corev1alpha1.WorkspaceConfig {
	if task == nil {
		return nil
	}
	return task.Spec.Workspace
}

func (c githubTaskContext) credentialRef(
	taskName, owner, repo string,
	policy githubCredentialPolicy,
) (*corev1alpha1.WorkspaceCredentialReference, error) {
	switch policy {
	case githubCredentialRead:
		for _, scope := range c.scopes {
			if githubRepoMatches(owner, repo, scope.owner, scope.repo) && scope.readCredentialRef != nil {
				return scope.readCredentialRef, nil
			}
		}
		return nil, nil
	case githubCredentialForgeMutation:
		if c.forgeCredentialRef == nil || strings.TrimSpace(c.forgeCredentialRef.Name) == "" {
			return nil, fmt.Errorf("task %s workspace has no forgeCredentialRef configured", taskName)
		}
		return c.forgeCredentialRef, nil
	default:
		return nil, fmt.Errorf("unsupported GitHub credential policy %d", policy)
	}
}

func resolveGitSecretToken(ctx context.Context, k8sClient client.Client, credentialRef *corev1alpha1.WorkspaceCredentialReference) (string, error) {
	if credentialRef == nil {
		return "", nil
	}
	credentialName := credentialRef.Name
	if strings.TrimSpace(credentialName) == "" {
		return "", fmt.Errorf("git credential reference name is required")
	}

	namespace := githubTaskNamespace(ctx)
	if tc := GetToolContext(ctx); tc != nil {
		if tc.AuthorizeSecretRead == nil {
			if tc.RequireSecretReadAuthorization {
				return "", fmt.Errorf("git secret %s/%s requires a secret credential authorizer", namespace, credentialName)
			}
		} else if authzErr := tc.AuthorizeSecretRead(ctx, namespace, credentialName); authzErr != nil {
			return "", fmt.Errorf("not authorized to read git secret %s/%s: %s", namespace, credentialName, authzErr.Message)
		}
	}

	var secret corev1.Secret
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: credentialName, Namespace: namespace}, &secret); err != nil {
		return "", fmt.Errorf("failed to get git secret %s: %w", credentialName, err)
	}

	key := credentialRef.Key
	if key == "" {
		key = tokenKey
	}
	value, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("git secret %s does not contain configured key %q", credentialName, key)
	}
	token := strings.TrimSpace(string(value))
	if token == "" {
		return "", fmt.Errorf("git secret %s contains an empty configured key %q", credentialName, key)
	}

	return token, nil
}

// resolveToken attempts to read a GitHub token from well-known file paths
// and environment variables.
func resolveToken() string {
	// Try mounted secret files
	for _, path := range []string{"/secrets/git/token", "/secrets/git/password", "/secrets/git/" + workerenv.GitHubToken} {
		if data, err := os.ReadFile(path); err == nil {
			if t := strings.TrimSpace(string(data)); t != "" {
				return t
			}
		}
	}

	// Fall back to GITHUB_TOKEN env var
	return os.Getenv(workerenv.GitHubToken)
}
