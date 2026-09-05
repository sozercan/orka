/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

const (
	githubWebhookTestDefaultBranch = "main"
	githubWebhookTestGitSecret     = "git-credentials"
	githubWebhookTestHeadSHA       = "head-sha"
	githubWebhookTestNewHeadSHA    = "new-head-sha"
	githubWebhookTestVekilCloneURL = "https://github.com/sozercan/vekil.git"
)

func TestGitHubWebhook_IssueImplementLabelCreatesAgentTask(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{
		githubLabelTriggerAgentEnv:     "codex-agent",
		githubLabelTriggerGitSecretEnv: githubWebhookTestGitSecret,
	})
	fc := newGitHubWebhookFakeClient(t, runtimeAgent("codex-agent"), githubWebhookGitSecret())
	server := NewServer(fc, nil, ServerConfig{})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"agent:implement"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":12,"title":"Add health endpoint","body":"Please add /healthz.","html_url":"https://github.com/sozercan/vekil/issues/12"},
		"sender":{"login":"octocat"}
	}`)

	delivery := "delivery-1"
	resp := performSignedGitHubWebhook(t, server, githubEventIssues, delivery, secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}

	var task corev1alpha1.Task
	replayKey := githubWebhookReplayKey(body)
	if err := fc.Get(t.Context(), types.NamespacedName{Name: githubWebhookTaskNameForBody(githubActionImplement, 12, body), Namespace: "default"}, &task); err != nil {
		t.Fatalf("created task not found: %v", err)
	}
	if task.Spec.Type != corev1alpha1.TaskTypeAgent {
		t.Fatalf("task type = %q, want agent", task.Spec.Type)
	}
	if task.Spec.AgentRef == nil || task.Spec.AgentRef.Name != "codex-agent" {
		t.Fatalf("agentRef = %#v, want codex-agent", task.Spec.AgentRef)
	}
	if task.Spec.AgentRuntime == nil || task.Spec.Workspace == nil {
		t.Fatal("agent runtime workspace missing")
	}
	ws := task.Spec.Workspace
	if ws.GitRepo != githubWebhookTestVekilCloneURL {
		t.Errorf("gitRepo = %q", ws.GitRepo)
	}
	if ws.Branch != githubWebhookTestDefaultBranch {
		t.Errorf("branch = %q, want main", ws.Branch)
	}
	wantPushBranch := "orka/implement-issue-12-" + githubReplayKeySuffix(replayKey)
	if ws.PushBranch != wantPushBranch {
		t.Errorf("pushBranch = %q, want %q", ws.PushBranch, wantPushBranch)
	}
	if ws.ReadCredentialRef == nil || ws.ReadCredentialRef.Name != githubWebhookTestGitSecret {
		t.Fatalf("readCredentialRef = %#v, want %s", ws.ReadCredentialRef, githubWebhookTestGitSecret)
	}
	if ws.PublicationReadCredentialRef == nil || ws.PublicationReadCredentialRef.Name != githubWebhookTestGitSecret {
		t.Fatalf("publicationReadCredentialRef = %#v, want %s", ws.PublicationReadCredentialRef, githubWebhookTestGitSecret)
	}
	if task.Labels[labels.LabelCreatedBy] != githubWebhookCreatedBy {
		t.Errorf("created-by label = %q", task.Labels[labels.LabelCreatedBy])
	}
	if task.Labels[labels.LabelGitHubAction] != githubActionImplement {
		t.Errorf("github action label = %q", task.Labels[labels.LabelGitHubAction])
	}
	if task.Annotations[labels.AnnotationGitHubDelivery] != delivery {
		t.Errorf("delivery annotation = %q", task.Annotations[labels.AnnotationGitHubDelivery])
	}
	if !strings.Contains(task.Spec.Prompt, "agent:implement") || !strings.Contains(task.Spec.Prompt, "Please add /healthz.") {
		t.Errorf("prompt missing trigger context: %s", task.Spec.Prompt)
	}
}

func TestGitHubWebhook_RuntimeRefMaxTurnsCompatibility(t *testing.T) {
	body := []byte(`{
		"action":"labeled",
		"label":{"name":"agent:implement"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":12,"title":"Add health endpoint","body":"Please add /healthz.","html_url":"https://github.com/sozercan/vekil/issues/12"},
		"sender":{"login":"octocat"}
	}`)

	for _, test := range []struct {
		name         string
		contract     corev1alpha1.AgentRuntimeContractVersion
		allowedTools []string
		wantTurns    bool
	}{
		{
			name:         "harness v2 materializes registered tools and omits unsupported override",
			contract:     corev1alpha1.AgentRuntimeContractHarnessV2,
			allowedTools: []string{"read_evidence"},
		},
		{
			name:         "harness v2 preserves explicit deny all",
			contract:     corev1alpha1.AgentRuntimeContractHarnessV2,
			allowedTools: []string{},
		},
		{name: "harness v1 preserves override", contract: corev1alpha1.AgentRuntimeContractHarnessV1, wantTurns: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			secret := configureGitHubWebhookTest(t, map[string]string{
				githubLabelTriggerAgentEnv:    "external-agent",
				githubLabelTriggerMaxTurnsEnv: "17",
			})
			fc := newGitHubWebhookFakeClient(t,
				runtimeRefAgent("external-runtime"),
				registeredAgentRuntime("external-runtime", test.contract, test.allowedTools),
			)
			server := NewServer(fc, nil, ServerConfig{})

			resp := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-runtime-ref", secret, body)
			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
			}

			var task corev1alpha1.Task
			key := types.NamespacedName{Name: githubWebhookTaskNameForBody(githubActionImplement, 12, body), Namespace: "default"}
			if err := fc.Get(t.Context(), key, &task); err != nil {
				t.Fatalf("created task not found: %v", err)
			}
			if !test.wantTurns {
				if task.Spec.AgentRuntime == nil || task.Spec.AgentRuntime.MaxTurns != nil {
					t.Fatalf("agentRuntime = %#v, want allowedTools without maxTurns for external harness v2", task.Spec.AgentRuntime)
				}
				if task.Spec.AgentRuntime.AllowedTools == nil || !slices.Equal(task.Spec.AgentRuntime.AllowedTools, test.allowedTools) {
					t.Fatalf("allowedTools = %#v, want explicit %#v", task.Spec.AgentRuntime.AllowedTools, test.allowedTools)
				}
				return
			}
			if task.Spec.AgentRuntime == nil || task.Spec.AgentRuntime.MaxTurns == nil || *task.Spec.AgentRuntime.MaxTurns != 17 {
				t.Fatalf("agentRuntime = %#v, want maxTurns 17", task.Spec.AgentRuntime)
			}
		})
	}
}

func TestGitHubWebhook_RuntimeRefUsesAPIReaderPolicy(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{githubLabelTriggerAgentEnv: "external-agent"})
	cachedClient := newGitHubWebhookFakeClient(t,
		runtimeRefAgent("cached-runtime"),
		registeredAgentRuntime("cached-runtime", corev1alpha1.AgentRuntimeContractHarnessV2, []string{"cached_tool"}),
	)
	apiReader := newGitHubWebhookFakeClient(t,
		runtimeRefAgent("live-runtime"),
		registeredAgentRuntime("live-runtime", corev1alpha1.AgentRuntimeContractHarnessV2, []string{"live_tool"}),
	)
	server := NewServer(cachedClient, nil, ServerConfig{APIReader: apiReader})
	body := []byte(`{
		"action":"labeled",
		"label":{"name":"agent:implement"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":12,"title":"Add health endpoint","body":"Please add /healthz.","html_url":"https://github.com/sozercan/vekil/issues/12"},
		"sender":{"login":"octocat"}
	}`)

	resp := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-live-runtime-policy", secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}

	var task corev1alpha1.Task
	key := types.NamespacedName{Name: githubWebhookTaskNameForBody(githubActionImplement, 12, body), Namespace: "default"}
	if err := cachedClient.Get(t.Context(), key, &task); err != nil {
		t.Fatalf("created task not found: %v", err)
	}
	if task.Spec.AgentRuntime == nil || !slices.Equal(task.Spec.AgentRuntime.AllowedTools, []string{"live_tool"}) {
		t.Fatalf("agentRuntime = %#v, want live API reader allowedTools", task.Spec.AgentRuntime)
	}
}

func TestGitHubWebhook_RuntimeRefRequiresClassifiedRegistration(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{githubLabelTriggerAgentEnv: "external-agent"})
	fc := newGitHubWebhookFakeClient(t,
		runtimeRefAgent("external-runtime"),
		&corev1alpha1.AgentRuntime{ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: "default"}},
	)
	server := NewServer(fc, nil, ServerConfig{})
	body := []byte(`{
		"action":"labeled",
		"label":{"name":"agent:implement"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":12,"title":"Add health endpoint","body":"Please add /healthz.","html_url":"https://github.com/sozercan/vekil/issues/12"},
		"sender":{"login":"octocat"}
	}`)

	resp := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-unclassified-runtime", secret, body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusBadRequest, readRespBody(t, resp))
	}
	assertNoTasks(t, fc)
}

func TestGitHubWebhook_IssueImplementRejectsMissingConfiguredGitSecret(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{
		githubLabelTriggerAgentEnv:     "codex-agent",
		githubLabelTriggerGitSecretEnv: "missing-git-secret",
	})
	fc := newGitHubWebhookFakeClient(t, runtimeAgent("codex-agent"))
	server := NewServer(fc, nil, ServerConfig{})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"agent:implement"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":12,"title":"Add health endpoint","body":"Please add /healthz.","html_url":"https://github.com/sozercan/vekil/issues/12"},
		"sender":{"login":"octocat"}
	}`)

	resp := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-missing-git-secret", secret, body)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusServiceUnavailable, readRespBody(t, resp))
	}
	assertNoTasks(t, fc)
}

func TestGitHubWebhook_PullRequestUpdateBranchUsesHeadBranch(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{
		githubLabelTriggerAgentEnv:     "claude-agent",
		githubLabelTriggerGitSecretEnv: githubWebhookTestGitSecret,
	})
	fc := newGitHubWebhookFakeClient(t, runtimeAgent("claude-agent"), githubWebhookGitSecret())
	server := NewServer(fc, nil, ServerConfig{})

	body := []byte(strings.ReplaceAll(`{
		"action":"labeled",
		"label":{"name":"agent:update-branch"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":34,
			"title":"Feature branch",
			"body":"Update me",
			"html_url":"https://github.com/sozercan/vekil/pull/34",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/x","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		},
		"sender":{"login":"octocat"}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	delivery := "delivery-2"
	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, delivery, secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}

	var task corev1alpha1.Task
	if err := fc.Get(t.Context(), types.NamespacedName{Name: githubWebhookTaskNameForBody(githubActionUpdateBranch, 34, body), Namespace: "default"}, &task); err != nil {
		t.Fatalf("created task not found: %v", err)
	}
	ws := task.Spec.Workspace
	if ws.Branch != "feature/x" {
		t.Errorf("branch = %q, want feature/x", ws.Branch)
	}
	if ws.Ref != githubWebhookTestHeadSHA {
		t.Errorf("ref = %q, want %s", ws.Ref, githubWebhookTestHeadSHA)
	}
	if ws.PushBranch != "feature/x" {
		t.Errorf("pushBranch = %q, want feature/x", ws.PushBranch)
	}
	if ws.ReadCredentialRef == nil || ws.ReadCredentialRef.Name != githubWebhookTestGitSecret {
		t.Fatalf("readCredentialRef = %#v, want %s for same-repo PR", ws.ReadCredentialRef, githubWebhookTestGitSecret)
	}
	if ws.PublicationReadCredentialRef == nil || ws.PublicationReadCredentialRef.Name != githubWebhookTestGitSecret {
		t.Fatalf("publicationReadCredentialRef = %#v, want %s for same-repo PR", ws.PublicationReadCredentialRef, githubWebhookTestGitSecret)
	}
	if ws.PRBaseBranch != githubWebhookTestDefaultBranch {
		t.Errorf("prBaseBranch = %q, want main", ws.PRBaseBranch)
	}
	if len(task.Spec.Env) != 0 {
		t.Errorf("task env = %#v, want empty because agent tasks reject arbitrary task env", task.Spec.Env)
	}
	if !strings.Contains(task.Spec.Prompt, "Update the pull request branch") {
		t.Errorf("prompt = %s", task.Spec.Prompt)
	}
}

func TestGitHubWebhook_PullRequestImplementUsesForkHeadRepo(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{
		githubLabelTriggerAgentEnv:     "codex-agent",
		githubLabelTriggerGitSecretEnv: githubWebhookTestGitSecret,
	})
	fc := newGitHubWebhookFakeClient(t, runtimeAgent("codex-agent"))
	server := NewServer(fc, nil, ServerConfig{})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"agent:implement"},
		"repository":{"full_name":"orka-agents/orka","html_url":"https://github.com/orka-agents/orka","clone_url":"https://github.com/orka-agents/orka.git","default_branch":"main"},
		"pull_request":{
			"number":35,
			"title":"Fork change",
			"body":"Implement on fork",
			"html_url":"https://github.com/orka-agents/orka/pull/35",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"orka-agents/orka","html_url":"https://github.com/orka-agents/orka","clone_url":"https://github.com/orka-agents/orka.git","default_branch":"main"}},
			"head":{"ref":"feature/fork-change","sha":"fork-head-sha","repo":{"full_name":"contributor/orka","html_url":"https://github.com/contributor/orka","clone_url":"https://github.com/contributor/orka.git","default_branch":"main"}}
		},
		"sender":{"login":"octocat"}
	}`)

	delivery := "delivery-fork-pr"
	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, delivery, secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}

	var task corev1alpha1.Task
	if err := fc.Get(t.Context(), types.NamespacedName{Name: githubWebhookTaskNameForBody(githubActionImplement, 35, body), Namespace: "default"}, &task); err != nil {
		t.Fatalf("created task not found: %v", err)
	}
	ws := task.Spec.Workspace
	if ws.GitRepo != "https://github.com/contributor/orka.git" {
		t.Errorf("gitRepo = %q, want fork head repo", ws.GitRepo)
	}
	if ws.Branch != "feature/fork-change" {
		t.Errorf("branch = %q, want feature/fork-change", ws.Branch)
	}
	if ws.Ref != "fork-head-sha" {
		t.Errorf("ref = %q, want fork-head-sha", ws.Ref)
	}
	if ws.PushBranch != "" {
		t.Errorf("pushBranch = %q, want empty for fork PR without safe git credentials", ws.PushBranch)
	}
	if ws.ReadCredentialRef != nil {
		t.Fatalf("readCredentialRef = %#v, want nil for fork PR", ws.ReadCredentialRef)
	}
	if ws.PRBaseBranch != "" {
		t.Errorf("prBaseBranch = %q, want empty because prBaseBranch requires write workspace intent", ws.PRBaseBranch)
	}
	if !strings.Contains(task.Spec.Prompt, "Orka will not push them automatically") {
		t.Errorf("prompt missing no-push guidance: %s", task.Spec.Prompt)
	}
}

func TestGitHubWebhook_PullRequestMissingHeadRepoFailsClosedForGitSecret(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{
		githubLabelTriggerAgentEnv:     "codex-agent",
		githubLabelTriggerGitSecretEnv: githubWebhookTestGitSecret,
	})
	fc := newGitHubWebhookFakeClient(t, runtimeAgent("codex-agent"))
	server := NewServer(fc, nil, ServerConfig{})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"agent:implement"},
		"repository":{"full_name":"orka-agents/orka","html_url":"https://github.com/orka-agents/orka","clone_url":"https://github.com/orka-agents/orka.git","default_branch":"main"},
		"pull_request":{
			"number":36,
			"title":"Unknown head repo",
			"body":"Implement with unknown head repo",
			"html_url":"https://github.com/orka-agents/orka/pull/36",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"orka-agents/orka","html_url":"https://github.com/orka-agents/orka","clone_url":"https://github.com/orka-agents/orka.git","default_branch":"main"}},
			"head":{"ref":"feature/unknown-head","sha":"unknown-head-sha","repo":null}
		},
		"sender":{"login":"octocat"}
	}`)

	delivery := "delivery-missing-head-pr"
	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, delivery, secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}

	var task corev1alpha1.Task
	if err := fc.Get(t.Context(), types.NamespacedName{Name: githubWebhookTaskNameForBody(githubActionImplement, 36, body), Namespace: "default"}, &task); err != nil {
		t.Fatalf("created task not found: %v", err)
	}
	ws := task.Spec.Workspace
	if ws.GitRepo != "https://github.com/orka-agents/orka.git" {
		t.Errorf("gitRepo = %q, want base repository fallback", ws.GitRepo)
	}
	if ws.Branch != "feature/unknown-head" {
		t.Errorf("branch = %q, want feature/unknown-head", ws.Branch)
	}
	if ws.PushBranch != "" {
		t.Errorf("pushBranch = %q, want empty for PR without verified head repository", ws.PushBranch)
	}
	if ws.ReadCredentialRef != nil {
		t.Fatalf("readCredentialRef = %#v, want nil for PR without verified head repository", ws.ReadCredentialRef)
	}
	if !strings.Contains(task.Spec.Prompt, "Orka will not push them automatically") {
		t.Errorf("prompt missing no-push guidance: %s", task.Spec.Prompt)
	}
}

func TestGitHubWebhook_PullRequestReviewUsesInitOnlyGitSecret(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{
		githubLabelTriggerAgentEnv:     "codex-agent",
		githubLabelTriggerGitSecretEnv: githubWebhookTestGitSecret,
	})
	fc := newGitHubWebhookFakeClient(t, runtimeAgent("codex-agent"), githubWebhookGitSecret())
	server := NewServer(fc, nil, ServerConfig{})

	body := []byte(strings.ReplaceAll(`{
		"action":"labeled",
		"label":{"name":"agent:review"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":37,
			"title":"Review me",
			"body":"Please review",
			"html_url":"https://github.com/sozercan/vekil/pull/37",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/review","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		},
		"sender":{"login":"octocat"}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-review-pr", secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}

	var task corev1alpha1.Task
	if err := fc.Get(t.Context(), types.NamespacedName{Name: githubWebhookTaskNameForBody(githubActionReview, 37, body), Namespace: "default"}, &task); err != nil {
		t.Fatalf("created task not found: %v", err)
	}
	ws := task.Spec.Workspace
	if ws.PushBranch != "" {
		t.Errorf("pushBranch = %q, want empty for review action", ws.PushBranch)
	}
	if ws.ReadCredentialRef == nil || ws.ReadCredentialRef.Name != githubWebhookTestGitSecret {
		t.Fatalf("readCredentialRef = %#v, want %s for init-only clone", ws.ReadCredentialRef, githubWebhookTestGitSecret)
	}
	if task.Annotations[labels.AnnotationWorkspaceInitContainer] != queryTrue {
		t.Fatalf("workspace init annotation = %q, want true", task.Annotations[labels.AnnotationWorkspaceInitContainer])
	}
	if ws.PRBaseBranch != "" {
		t.Errorf("prBaseBranch = %q, want empty because prBaseBranch requires write workspace intent", ws.PRBaseBranch)
	}
	if len(task.Spec.Env) != 0 {
		t.Errorf("task env = %#v, want empty because agent tasks reject arbitrary task env", task.Spec.Env)
	}
}

func TestGitHubWebhook_ToIssuesMountsGitSecretWithoutPushBranch(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{
		githubLabelTriggerAgentEnv:     "codex-agent",
		githubLabelTriggerGitSecretEnv: githubWebhookTestGitSecret,
	})
	fc := newGitHubWebhookFakeClient(t, runtimeAgent("codex-agent"), githubWebhookGitSecret())
	server := NewServer(fc, nil, ServerConfig{})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"agent:to-issues"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":38,"title":"Plan this","body":"Break this into issues.","html_url":"https://github.com/sozercan/vekil/issues/38"},
		"sender":{"login":"octocat"}
	}`)

	resp := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-to-issues", secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}

	var task corev1alpha1.Task
	if err := fc.Get(t.Context(), types.NamespacedName{Name: githubWebhookTaskNameForBody(githubActionToIssues, 38, body), Namespace: "default"}, &task); err != nil {
		t.Fatalf("created task not found: %v", err)
	}
	ws := task.Spec.Workspace
	if ws.PushBranch != "" {
		t.Errorf("pushBranch = %q, want empty for to-issues action", ws.PushBranch)
	}
	if ws.ReadCredentialRef == nil || ws.ReadCredentialRef.Name != githubWebhookTestGitSecret {
		t.Fatalf("readCredentialRef = %#v, want %s", ws.ReadCredentialRef, githubWebhookTestGitSecret)
	}
}

func TestGitHubWebhook_IgnoresIssuePullRequestStub(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{
		githubLabelTriggerAgentEnv: "codex-agent",
	})
	fc := newGitHubWebhookFakeClient(t, runtimeAgent("codex-agent"))
	server := NewServer(fc, nil, ServerConfig{})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"agent:update-branch"},
		"repository":{"full_name":"orka-agents/orka","html_url":"https://github.com/orka-agents/orka","clone_url":"https://github.com/orka-agents/orka.git","default_branch":"main"},
		"issue":{"number":35,"title":"Fork change","body":"Implement on fork","html_url":"https://github.com/orka-agents/orka/issues/35","pull_request":{"html_url":"https://github.com/orka-agents/orka/pull/35"}},
		"sender":{"login":"octocat"}
	}`)

	resp := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-pr-stub", secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
	assertNoTasks(t, fc)
}

func TestGitHubWebhook_SignedPayloadControlsIdempotency(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{
		githubLabelTriggerAgentEnv: "codex-agent",
	})
	fc := newGitHubWebhookFakeClient(t, runtimeAgent("codex-agent"))
	server := NewServer(fc, nil, ServerConfig{})
	body := []byte(`{"action":"labeled","label":{"name":"agent:implement"},"repository":{"full_name":"sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},"issue":{"number":1,"title":"Do it","body":"Body","html_url":"https://github.com/sozercan/vekil/issues/1"}}`)

	first := performSignedGitHubWebhook(t, server, githubEventIssues, "same-delivery", secret, body)
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first status = %d; body: %s", first.StatusCode, readRespBody(t, first))
	}
	duplicate := performSignedGitHubWebhook(t, server, githubEventIssues, "same-delivery", secret, body)
	if duplicate.StatusCode != http.StatusAccepted {
		t.Fatalf("duplicate status = %d; body: %s", duplicate.StatusCode, readRespBody(t, duplicate))
	}
	headerReplay := performSignedGitHubWebhook(t, server, githubEventIssues, "new-delivery", secret, body)
	if headerReplay.StatusCode != http.StatusAccepted {
		t.Fatalf("header replay status = %d; body: %s", headerReplay.StatusCode, readRespBody(t, headerReplay))
	}

	changedBody := []byte(`{"action":"labeled","label":{"name":"agent:implement"},"repository":{"full_name":"sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},"issue":{"number":1,"title":"Do it","body":"Changed body","html_url":"https://github.com/sozercan/vekil/issues/1"}}`)
	second := performSignedGitHubWebhook(t, server, githubEventIssues, "new-delivery", secret, changedBody)
	if second.StatusCode != http.StatusCreated {
		t.Fatalf("changed payload status = %d; body: %s", second.StatusCode, readRespBody(t, second))
	}

	var tasks corev1alpha1.TaskList
	if err := fc.List(t.Context(), &tasks); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks.Items) != 2 {
		t.Fatalf("task count = %d, want 2", len(tasks.Items))
	}
}

func TestGitHubWebhook_IgnoresNonAgentLabelsAndUnsupportedTargets(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{
		githubLabelTriggerAgentEnv: "codex-agent",
	})
	fc := newGitHubWebhookFakeClient(t, runtimeAgent("codex-agent"))
	server := NewServer(fc, nil, ServerConfig{})

	body := []byte(`{"action":"labeled","label":{"name":"bug"},"repository":{"full_name":"sozercan/vekil"},"issue":{"number":1,"title":"Bug","body":"Body","html_url":"https://github.com/sozercan/vekil/issues/1"}}`)
	resp := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-ignore", secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
	assertNoTasks(t, fc)

	body = []byte(`{"action":"labeled","label":{"name":"agent:review"},"repository":{"full_name":"sozercan/vekil"},"issue":{"number":1,"title":"Bug","body":"Body","html_url":"https://github.com/sozercan/vekil/issues/1"}}`)
	resp = performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-review-issue", secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
	assertNoTasks(t, fc)
}

func TestGitHubWebhook_PullRequestEventQueuesRepositoryMonitorRun(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	monitorStore := setupGitHubWebhookMonitorStore(t)
	monitor := githubWebhookRepositoryMonitor("repo-monitor", true)
	fc := newGitHubWebhookFakeClient(t, monitor)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(strings.ReplaceAll(`{
		"action":"synchronize",
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":34,
			"title":"Feature branch",
			"body":"Update me",
			"html_url":"https://github.com/sozercan/vekil/pull/34",
			"state":"open",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/x","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		},
		"sender":{"login":"octocat"}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-exact-1", secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}
	assertNoTasks(t, fc)

	runs, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "repo-monitor", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("runs = %#v, want one exact event run", runs)
	}
	run := runs[0]
	if run.Trigger != githubMonitorTriggerPullRequestEvent || run.TargetKind != repositoryMonitorTargetKindPullRequest || run.TargetNumber != 34 || run.TargetSHA != githubWebhookTestHeadSHA || run.Phase != repositoryMonitorRunPhaseQueued {
		t.Fatalf("run = %#v, want queued exact PR event run", run)
	}
	var signaled corev1alpha1.RepositoryMonitor
	if err := fc.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "repo-monitor"}, &signaled); err != nil {
		t.Fatalf("Get RepositoryMonitor() error = %v", err)
	}
	if signaled.Annotations[repositoryMonitorRunRequestAnnotation] != run.ID {
		t.Fatalf("run signal annotation = %q, want %q", signaled.Annotations[repositoryMonitorRunRequestAnnotation], run.ID)
	}

	events, _, err := monitorStore.ListMonitorEvents(t.Context(), store.MonitorEventFilter{Namespace: "default", MonitorName: "repo-monitor", RunID: run.ID, EventType: githubMonitorEventTypeExactRunQueued, Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].ItemNumber != 34 || events[0].ItemSHA != githubWebhookTestHeadSHA {
		t.Fatalf("events = %#v, want exact event audit for PR head", events)
	}

	newHeadBody := []byte(strings.ReplaceAll(string(body), githubWebhookTestHeadSHA, githubWebhookTestNewHeadSHA))
	updated := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-exact-2", secret, newHeadBody)
	if updated.StatusCode != http.StatusCreated {
		t.Fatalf("updated status = %d, want %d; body: %s", updated.StatusCode, http.StatusCreated, readRespBody(t, updated))
	}
	runs, _, err = monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "repo-monitor", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns(after newer head) error = %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("runs after newer head = %#v, want old and new queued exact-event runs", runs)
	}
	foundNewHead := false
	for _, queuedRun := range runs {
		if queuedRun.TargetSHA == githubWebhookTestNewHeadSHA {
			foundNewHead = true
		}
	}
	if !foundNewHead {
		t.Fatalf("runs after newer head = %#v, want queued run for newest head", runs)
	}

	duplicate := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-exact-2", secret, newHeadBody)
	if duplicate.StatusCode != http.StatusAccepted {
		t.Fatalf("duplicate status = %d, want %d; body: %s", duplicate.StatusCode, http.StatusAccepted, readRespBody(t, duplicate))
	}
	runs, _, err = monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "repo-monitor", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns(after duplicate) error = %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("runs after duplicate = %#v, want duplicate exact event ignored", runs)
	}
}

func TestGitHubWebhook_PullRequestEventDuplicateScansBeyondFirstQueuedRunPage(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	monitorStore := setupGitHubWebhookMonitorStore(t)
	monitor := githubWebhookRepositoryMonitor("repo-monitor", true)
	fc := newGitHubWebhookFakeClient(t, monitor)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})
	baseTime := time.Now().Add(-2 * time.Hour)
	if err := monitorStore.CreateMonitorRun(t.Context(), &store.MonitorRun{
		ID:               "older-duplicate",
		MonitorNamespace: "default",
		MonitorName:      "repo-monitor",
		Trigger:          githubMonitorTriggerPullRequestEvent,
		TargetKind:       repositoryMonitorTargetKindPullRequest,
		TargetNumber:     34,
		TargetSHA:        githubWebhookTestHeadSHA,
		Phase:            repositoryMonitorRunPhaseQueued,
		StartedAt:        baseTime,
	}); err != nil {
		t.Fatalf("CreateMonitorRun(older duplicate) error = %v", err)
	}
	for i := range 100 {
		if err := monitorStore.CreateMonitorRun(t.Context(), &store.MonitorRun{
			ID:               "newer-run-" + strconv.Itoa(i),
			MonitorNamespace: "default",
			MonitorName:      "repo-monitor",
			Trigger:          githubMonitorTriggerPullRequestEvent,
			TargetKind:       repositoryMonitorTargetKindPullRequest,
			TargetNumber:     int64(1000 + i),
			TargetSHA:        "other-head-" + strconv.Itoa(i),
			Phase:            repositoryMonitorRunPhaseQueued,
			StartedAt:        baseTime.Add(time.Duration(i+1) * time.Minute),
		}); err != nil {
			t.Fatalf("CreateMonitorRun(newer %d) error = %v", i, err)
		}
	}

	body := []byte(strings.ReplaceAll(`{
		"action":"synchronize",
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":34,
			"title":"Feature branch",
			"body":"Update me",
			"html_url":"https://github.com/sozercan/vekil/pull/34",
			"state":"open",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/x","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		},
		"sender":{"login":"octocat"}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-duplicate-beyond-first-page", secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusAccepted, readRespBody(t, resp))
	}
	runs, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "repo-monitor", Limit: 200})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 101 {
		t.Fatalf("runs = %d, want existing 101 queued runs with no duplicate inserted", len(runs))
	}
}

func TestGitHubWebhook_PullRequestEventRequeuesFailedRunBeforeAudit(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	monitorStore := setupGitHubWebhookMonitorStore(t)
	monitor := githubWebhookRepositoryMonitor("repo-monitor", true)
	fc := newGitHubWebhookFakeClient(t, monitor)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})
	delivery := "delivery-failed-before-audit"
	runID := githubRepositoryMonitorExactRunID(monitor, delivery)
	if err := monitorStore.CreateMonitorRun(t.Context(), &store.MonitorRun{
		ID:               runID,
		MonitorNamespace: "default",
		MonitorName:      "repo-monitor",
		Trigger:          githubMonitorTriggerPullRequestEvent,
		TargetKind:       repositoryMonitorTargetKindPullRequest,
		TargetNumber:     34,
		TargetSHA:        githubWebhookTestHeadSHA,
		Phase:            repositoryMonitorRunPhaseFailed,
		Error:            "failed to signal repository monitor run",
	}); err != nil {
		t.Fatalf("CreateMonitorRun(failed) error = %v", err)
	}

	body := []byte(strings.ReplaceAll(`{
		"action":"synchronize",
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":34,
			"title":"Feature branch",
			"body":"Update me",
			"html_url":"https://github.com/sozercan/vekil/pull/34",
			"state":"open",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/x","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		},
		"sender":{"login":"octocat"}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, delivery, secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}
	run, err := monitorStore.GetMonitorRun(t.Context(), "default", runID)
	if err != nil {
		t.Fatalf("GetMonitorRun() error = %v", err)
	}
	if run.Phase != repositoryMonitorRunPhaseQueued || run.CompletedAt != nil || run.Error != "" {
		t.Fatalf("run = %#v, want failed pre-audit run requeued", run)
	}
	events, _, err := monitorStore.ListMonitorEvents(t.Context(), store.MonitorEventFilter{Namespace: "default", MonitorName: "repo-monitor", RunID: runID, EventType: githubMonitorEventTypeExactRunQueued, Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %#v, want one exact-event audit after requeue", events)
	}
}

func TestGitHubWebhook_PullRequestEventDoesNotRequeueAuditedFailedRun(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	monitorStore := setupGitHubWebhookMonitorStore(t)
	monitor := githubWebhookRepositoryMonitor("repo-monitor", true)
	fc := newGitHubWebhookFakeClient(t, monitor)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})
	delivery := "delivery-failed-after-audit"
	runID := githubRepositoryMonitorExactRunID(monitor, delivery)
	if err := monitorStore.CreateMonitorRun(t.Context(), &store.MonitorRun{
		ID:               runID,
		MonitorNamespace: "default",
		MonitorName:      "repo-monitor",
		Trigger:          githubMonitorTriggerPullRequestEvent,
		TargetKind:       repositoryMonitorTargetKindPullRequest,
		TargetNumber:     34,
		TargetSHA:        githubWebhookTestHeadSHA,
		Phase:            repositoryMonitorRunPhaseFailed,
		Error:            "controller processing failed after audit",
	}); err != nil {
		t.Fatalf("CreateMonitorRun(failed) error = %v", err)
	}
	if err := monitorStore.CreateMonitorEvent(t.Context(), &store.MonitorEvent{
		ID:               "event-audited-failed-run",
		MonitorNamespace: "default",
		MonitorName:      "repo-monitor",
		RunID:            runID,
		ItemKind:         repositoryMonitorTargetKindPullRequest,
		ItemNumber:       34,
		ItemSHA:          githubWebhookTestHeadSHA,
		EventType:        githubMonitorEventTypeExactRunQueued,
		Actor:            "github-webhook",
		Summary:          "Exact pull request event queued repository monitor run for PR #34",
	}); err != nil {
		t.Fatalf("CreateMonitorEvent() error = %v", err)
	}

	body := []byte(strings.ReplaceAll(`{
		"action":"synchronize",
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":34,
			"title":"Feature branch",
			"body":"Update me",
			"html_url":"https://github.com/sozercan/vekil/pull/34",
			"state":"open",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/x","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		},
		"sender":{"login":"octocat"}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, delivery, secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusAccepted, readRespBody(t, resp))
	}
	run, err := monitorStore.GetMonitorRun(t.Context(), "default", runID)
	if err != nil {
		t.Fatalf("GetMonitorRun() error = %v", err)
	}
	if run.Phase != repositoryMonitorRunPhaseFailed || run.Error == "" {
		t.Fatalf("run = %#v, want audited failed run left unchanged", run)
	}
	events, _, err := monitorStore.ListMonitorEvents(t.Context(), store.MonitorEventFilter{Namespace: "default", MonitorName: "repo-monitor", RunID: runID, EventType: githubMonitorEventTypeExactRunQueued, Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %#v, want existing exact-event audit only", events)
	}
}

func TestGitHubWebhook_PullRequestLabelReturnsMonitorResultWhenLabelAgentMissing(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	monitorStore := setupGitHubWebhookMonitorStore(t)
	monitor := githubWebhookRepositoryMonitor("repo-monitor", true)
	fc := newGitHubWebhookFakeClient(t, monitor)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(strings.ReplaceAll(`{
		"action":"labeled",
		"label":{"name":"agent:review"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":34,
			"title":"Feature branch",
			"body":"Review me",
			"html_url":"https://github.com/sozercan/vekil/pull/34",
			"state":"open",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/x","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		},
		"sender":{"login":"octocat"}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-monitor-only-label", secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}
	assertNoTasks(t, fc)

	runs, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "repo-monitor", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 1 || runs[0].Trigger != githubMonitorTriggerPullRequestEvent || runs[0].TargetSHA != githubWebhookTestHeadSHA {
		t.Fatalf("runs = %#v, want one queued exact-event monitor run", runs)
	}
}

func TestGitHubWebhook_PullRequestLabelDoesNotSuppressAgentLookupFailure(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{githubLabelTriggerAgentEnv: "review-agent"})
	monitorStore := setupGitHubWebhookMonitorStore(t)
	monitor := githubWebhookRepositoryMonitor("repo-monitor", true)
	fc := githubWebhookAgentGetErrorClient{
		Client: newGitHubWebhookFakeClient(t, monitor),
		err:    errors.New("temporary apiserver failure"),
	}
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(strings.ReplaceAll(`{
		"action":"labeled",
		"label":{"name":"agent:review"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":34,
			"title":"Feature branch",
			"body":"Review me",
			"html_url":"https://github.com/sozercan/vekil/pull/34",
			"state":"open",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/x","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		},
		"sender":{"login":"octocat"}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-agent-get-fails", secret, body)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusInternalServerError, readRespBody(t, resp))
	}
	runs, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "repo-monitor", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("runs = %#v, want monitor side effect to remain queued before retryable label-agent failure", runs)
	}
}

func TestGitHubWebhook_PullRequestEventRequiresExactEventEnabledMonitor(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	monitorStore := setupGitHubWebhookMonitorStore(t)
	monitor := githubWebhookRepositoryMonitor("repo-monitor", false)
	fc := newGitHubWebhookFakeClient(t, monitor)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(strings.ReplaceAll(`{
		"action":"synchronize",
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":34,
			"title":"Feature branch",
			"body":"Update me",
			"html_url":"https://github.com/sozercan/vekil/pull/34",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/x","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-exact-disabled", secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusAccepted, readRespBody(t, resp))
	}
	runs, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "repo-monitor", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %#v, want none when exact events are disabled", runs)
	}
	assertNoTasks(t, fc)
}

func TestGitHubWebhook_PullRequestEventQueuesBehindRunningMonitorRun(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	monitorStore := setupGitHubWebhookMonitorStore(t)
	monitor := githubWebhookRepositoryMonitor("repo-monitor", true)
	fc := newGitHubWebhookFakeClient(t, monitor)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})
	if err := monitorStore.CreateMonitorRun(t.Context(), &store.MonitorRun{
		ID:               "running-run",
		MonitorNamespace: "default",
		MonitorName:      "repo-monitor",
		Trigger:          "manual",
		Phase:            repositoryMonitorRunPhaseRunning,
	}); err != nil {
		t.Fatalf("CreateMonitorRun(running) error = %v", err)
	}

	body := []byte(strings.ReplaceAll(`{
		"action":"synchronize",
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":34,
			"title":"Feature branch",
			"body":"Update me",
			"html_url":"https://github.com/sozercan/vekil/pull/34",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/x","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-running", secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}

	queuedRuns, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "repo-monitor", Phase: repositoryMonitorRunPhaseQueued, Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns(queued) error = %v", err)
	}
	if len(queuedRuns) != 1 || queuedRuns[0].TargetSHA != githubWebhookTestHeadSHA {
		t.Fatalf("queued runs = %#v, want one queued exact event run for head %s", queuedRuns, githubWebhookTestHeadSHA)
	}

	runningRuns, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "repo-monitor", Phase: repositoryMonitorRunPhaseRunning, Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns(running) error = %v", err)
	}
	if len(runningRuns) != 1 || runningRuns[0].ID != "running-run" {
		t.Fatalf("running runs = %#v, want existing running run retained", runningRuns)
	}
}

func TestGitHubWebhook_PullRequestEventQueuesBehindQueuedManualRun(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	monitorStore := setupGitHubWebhookMonitorStore(t)
	monitor := githubWebhookRepositoryMonitor("repo-monitor", true)
	fc := newGitHubWebhookFakeClient(t, monitor)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})
	if err := monitorStore.CreateMonitorRun(t.Context(), &store.MonitorRun{
		ID:               "manual-run",
		MonitorNamespace: "default",
		MonitorName:      "repo-monitor",
		Trigger:          "manual",
		Phase:            repositoryMonitorRunPhaseQueued,
	}); err != nil {
		t.Fatalf("CreateMonitorRun(manual) error = %v", err)
	}

	body := []byte(strings.ReplaceAll(`{
		"action":"synchronize",
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":34,
			"title":"Feature branch",
			"body":"Update me",
			"html_url":"https://github.com/sozercan/vekil/pull/34",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/x","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-queued", secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}

	queuedRuns, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "repo-monitor", Phase: repositoryMonitorRunPhaseQueued, Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns(queued) error = %v", err)
	}
	if len(queuedRuns) != 2 {
		t.Fatalf("queued runs = %#v, want manual run plus exact event run", queuedRuns)
	}
	foundExact := false
	for _, run := range queuedRuns {
		if run.Trigger == githubMonitorTriggerPullRequestEvent && run.TargetSHA == githubWebhookTestHeadSHA {
			foundExact = true
		}
	}
	if !foundExact {
		t.Fatalf("queued runs = %#v, want queued exact event run retained behind manual run", queuedRuns)
	}
}

func TestGitHubWebhook_PullRequestEventUsesDistinctRunIDsForNormalizedMonitorNames(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	monitorStore := setupGitHubWebhookMonitorStore(t)
	dotted := githubWebhookRepositoryMonitor("repo.monitor", true)
	dashed := githubWebhookRepositoryMonitor("repo-monitor", true)
	fc := newGitHubWebhookFakeClient(t, dotted, dashed)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(strings.ReplaceAll(`{
		"action":"synchronize",
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{
			"number":34,
			"title":"Feature branch",
			"body":"Update me",
			"html_url":"https://github.com/sozercan/vekil/pull/34",
			"base":{"ref":"main","sha":"base-sha","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}},
			"head":{"ref":"feature/x","sha":"{{HEAD_SHA}}","repo":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"}}
		}
	}`, "{{HEAD_SHA}}", githubWebhookTestHeadSHA))

	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-normalized", secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}

	runs, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("runs = %#v, want one run per matching monitor", runs)
	}
	runIDs := map[string]struct{}{}
	for _, run := range runs {
		runIDs[run.ID] = struct{}{}
	}
	if len(runIDs) != 2 {
		t.Fatalf("runs = %#v, want distinct run IDs for distinct monitor names", runs)
	}
}

func TestGitHubWebhook_RejectsInvalidSignature(t *testing.T) {
	secret := configureGitHubWebhookTest(t, map[string]string{
		githubLabelTriggerAgentEnv: "codex-agent",
	})
	fc := newGitHubWebhookFakeClient(t, runtimeAgent("codex-agent"))
	server := NewServer(fc, nil, ServerConfig{})
	body := []byte(`{"action":"labeled","label":{"name":"agent:implement"}}`)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(githubEventHeader, githubEventIssues)
	req.Header.Set(githubDeliveryHeader, "bad-signature")
	req.Header.Set(githubSignature256Header, signGitHubWebhook(body, "wrong-secret"))
	resp, err := server.app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
	_ = secret
	assertNoTasks(t, fc)
}

func TestGitHubWebhook_PingVerifiesSignatureWithoutAuth(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	server := NewServer(newGitHubWebhookFakeClient(t), nil, ServerConfig{})
	body := []byte(`{"zen":"Keep it logically awesome."}`)

	resp := performSignedGitHubWebhook(t, server, githubEventPing, "ping-1", secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
}

func configureGitHubWebhookTest(t *testing.T, env map[string]string) string {
	t.Helper()
	secret := "webhook-secret"
	keys := []string{
		githubWebhookSecretEnv,
		githubLabelTriggerAgentEnv,
		githubLabelTriggerGitSecretEnv,
		githubLabelTriggerNamespaceEnv,
		githubLabelTriggerPrefixEnv,
		githubLabelTriggerTimeoutEnv,
		githubLabelTriggerMaxTurnsEnv,
		githubAPIBaseURLEnv,
		githubActionAgentEnv(githubActionImplement),
		githubActionAgentEnv(githubActionUpdateBranch),
		githubActionAgentEnv(githubActionReview),
		githubActionAgentEnv(githubActionToIssues),
	}
	for _, key := range keys {
		t.Setenv(key, "")
	}
	t.Setenv(githubWebhookSecretEnv, secret)
	for key, value := range env {
		t.Setenv(key, value)
	}
	return secret
}

func newGitHubWebhookFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add orka scheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

type githubWebhookAgentGetErrorClient struct {
	client.Client
	err error
}

func (c githubWebhookAgentGetErrorClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1alpha1.Agent); ok {
		return c.err
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func runtimeAgent(name string) *corev1alpha1.Agent {
	return &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex},
		},
	}
}

func runtimeRefAgent(runtimeName string) *corev1alpha1.Agent {
	return &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "external-agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: runtimeName},
			},
		},
	}
}

func registeredAgentRuntime(name string, contract corev1alpha1.AgentRuntimeContractVersion, allowedTools []string) *corev1alpha1.AgentRuntime {
	runtimeObject := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       corev1alpha1.AgentRuntimeRegistrySpec{ContractVersion: &contract},
	}
	if contract == corev1alpha1.AgentRuntimeContractHarnessV2 {
		runtimeObject.Spec.Capabilities = &corev1alpha1.AgentRuntimeCapabilitiesSpec{
			Profile: &corev1alpha1.AgentRuntimeProfileSpec{
				ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
			},
			MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
				AllowedTools:          append([]string{}, allowedTools...),
				DisallowedTools:       []string{},
				ApprovalRequiredTools: []string{},
			},
		}
	}
	return runtimeObject
}

func githubWebhookGitSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: githubWebhookTestGitSecret, Namespace: "default"},
		Data:       map[string][]byte{"token": []byte("test-token")},
	}
}

func githubWebhookRepositoryMonitor(name string, exactEvents bool) *corev1alpha1.RepositoryMonitor {
	return &corev1alpha1.RepositoryMonitor{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1alpha1.RepositoryMonitorSpec{
			RepoURL: "https://github.com/sozercan/vekil",
			Branch:  githubWebhookTestDefaultBranch,
			Agents: corev1alpha1.RepositoryMonitorAgents{
				Reviewer: &corev1alpha1.AgentReference{Name: "reviewer"},
			},
			Review: corev1alpha1.RepositoryMonitorReviewSpec{
				ExactEventEnabled: exactEvents,
			},
		},
	}
}

func setupGitHubWebhookMonitorStore(t *testing.T) store.RepositoryMonitorStore {
	t.Helper()
	db, err := sqlite.NewDB(":memory:")
	if err != nil {
		t.Fatalf("NewDB() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return sqlite.NewStore(db, ":memory:")
}

func performSignedGitHubWebhook(t *testing.T, server *Server, event, delivery, secret string, body []byte) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(githubEventHeader, event)
	req.Header.Set(githubDeliveryHeader, delivery)
	req.Header.Set(githubSignature256Header, signGitHubWebhook(body, secret))
	resp, err := server.app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	return resp
}

func githubWebhookTaskNameForBody(action string, number int, body []byte) string {
	return githubTaskName(action, number, githubWebhookReplayKey(body))
}

func signGitHubWebhook(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return githubSignature256Prefix + hex.EncodeToString(mac.Sum(nil))
}

func readRespBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close() //nolint:errcheck
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	return buf.String()
}

func assertNoTasks(t *testing.T, c client.Client) {
	t.Helper()
	var tasks corev1alpha1.TaskList
	if err := c.List(t.Context(), &tasks); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks.Items) != 0 {
		encoded, _ := json.Marshal(tasks.Items)
		t.Fatalf("task count = %d, want 0: %s", len(tasks.Items), string(encoded))
	}
}

func TestGitHubWebhook_OrkaIssueLabelCreatesDurableCommandAndIssueRun(t *testing.T) {
	permissionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/sozercan/vekil/collaborators/octocat/permission" {
			t.Fatalf("permission path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); !strings.HasPrefix(got, "Bearer ") {
			t.Fatalf("Authorization = %q, want bearer auth", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permission":"write"}`))
	}))
	t.Cleanup(permissionServer.Close)
	secret := configureGitHubWebhookTest(t, map[string]string{githubAPIBaseURLEnv: permissionServer.URL})
	pullRequestsEnabled := false
	monitor := githubWebhookRepositoryMonitor("issue-loop", false)
	monitor.Spec.ForgeCredentialRef = &corev1.LocalObjectReference{Name: githubWebhookTestGitSecret}
	monitor.Spec.Targets.PullRequests.Enabled = &pullRequestsEnabled
	monitor.Spec.Targets.Issues.Enabled = true
	monitor.Spec.Triggers.GitHub.Labels.Enabled = true
	fc := newGitHubWebhookFakeClient(t, monitor, githubWebhookGitSecret())
	monitorStore := setupGitHubWebhookMonitorStore(t)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"orka:plan"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":12,"title":"Add health endpoint","body":"Please add /healthz.","html_url":"https://github.com/sozercan/vekil/issues/12","updated_at":"2026-06-01T00:00:00Z","labels":[{"name":"bug"},{"name":"orka:plan"}]},
		"sender":{"login":"octocat"}
	}`)
	resp := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-orka-plan", secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, http.StatusCreated, readRespBody(t, resp))
	}
	commands, _, err := monitorStore.ListCommandEvents(t.Context(), store.CommandEventFilter{Namespace: "default", MonitorName: "issue-loop", Kind: "issue", Number: 12, Limit: 10})
	if err != nil {
		t.Fatalf("ListCommandEvents() error = %v", err)
	}
	if len(commands) != 1 || commands[0].Intent != "plan" || commands[0].Status != githubCommandStatusAccepted || commands[0].IssueSnapshotDigest == "" {
		t.Fatalf("commands = %#v, want accepted plan command with issue digest", commands)
	}
	runs, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "issue-loop", TargetKind: repositoryMonitorTargetKindIssue, TargetNumber: 12, Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 1 || runs[0].Trigger != githubMonitorTriggerLabelCommand {
		t.Fatalf("runs = %#v, want one label-command issue run", runs)
	}
	assertNoTasks(t, fc)

	resp = performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-orka-plan", secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("replay status = %d, want accepted; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
	commands, _, err = monitorStore.ListCommandEvents(t.Context(), store.CommandEventFilter{Namespace: "default", MonitorName: "issue-loop", Kind: "issue", Number: 12, Limit: 10})
	if err != nil {
		t.Fatalf("ListCommandEvents(replay) error = %v", err)
	}
	if len(commands) != 1 {
		t.Fatalf("command count after replay = %d, want 1", len(commands))
	}
}

func TestGitHubWebhook_OrkaClosedIssueLabelDoesNotCreateCommand(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	pullRequestsEnabled := false
	monitor := githubWebhookRepositoryMonitor("closed-issue-loop", false)
	monitor.Spec.Targets.PullRequests.Enabled = &pullRequestsEnabled
	monitor.Spec.Targets.Issues.Enabled = true
	monitor.Spec.Triggers.GitHub.Labels.Enabled = true
	fc := newGitHubWebhookFakeClient(t, monitor, githubWebhookGitSecret())
	monitorStore := setupGitHubWebhookMonitorStore(t)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"orka:plan"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":12,"state":"closed","title":"Already fixed","body":"done","html_url":"https://github.com/sozercan/vekil/issues/12","updated_at":"2026-06-01T00:00:00Z","labels":[{"name":"orka:plan"}]},
		"sender":{"login":"octocat"}
	}`)
	resp := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-orka-closed", secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want accepted ignored event; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
	commands, _, err := monitorStore.ListCommandEvents(t.Context(), store.CommandEventFilter{Namespace: "default", MonitorName: "closed-issue-loop", Kind: "issue", Number: 12, Limit: 10})
	if err != nil {
		t.Fatalf("ListCommandEvents() error = %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("commands = %#v, want none for closed issue", commands)
	}
}

func TestGitHubWebhook_CustomOrkaPRLabelDoesNotAlsoQueueExactEventRun(t *testing.T) {
	permissionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permission":"write"}`))
	}))
	t.Cleanup(permissionServer.Close)
	secret := configureGitHubWebhookTest(t, map[string]string{githubAPIBaseURLEnv: permissionServer.URL})
	monitor := githubWebhookRepositoryMonitor("custom-pr-command", true)
	monitor.Spec.ForgeCredentialRef = &corev1.LocalObjectReference{Name: githubWebhookTestGitSecret}
	monitor.Spec.Triggers.GitHub.Labels.Enabled = true
	monitor.Spec.Triggers.GitHub.Labels.PullRequests.Review = "bot:review"
	fc := newGitHubWebhookFakeClient(t, monitor, githubWebhookGitSecret())
	monitorStore := setupGitHubWebhookMonitorStore(t)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"bot:review"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{"number":7,"title":"Review me","body":"","html_url":"https://github.com/sozercan/vekil/pull/7","state":"open","draft":false,"base":{"ref":"main","sha":"base","repo":{"full_name":"sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git"}},"head":{"ref":"feature","sha":"head-sha","repo":{"full_name":"sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git"}},"labels":[{"name":"bot:review"}]},
		"sender":{"login":"octocat"}
	}`)
	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-custom-review", secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want created; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
	runs, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "custom-pr-command", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 1 || runs[0].Trigger != githubMonitorTriggerLabelCommand {
		t.Fatalf("runs = %#v, want exactly one durable command run", runs)
	}
}

func TestGitHubWebhook_OrkaGuardLabelBlocksCommandWithoutRun(t *testing.T) {
	permissionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permission":"write"}`))
	}))
	t.Cleanup(permissionServer.Close)
	secret := configureGitHubWebhookTest(t, map[string]string{githubAPIBaseURLEnv: permissionServer.URL})
	pullRequestsEnabled := false
	monitor := githubWebhookRepositoryMonitor("guarded-issue-loop", false)
	monitor.Spec.ForgeCredentialRef = &corev1.LocalObjectReference{Name: githubWebhookTestGitSecret}
	monitor.Spec.Targets.PullRequests.Enabled = &pullRequestsEnabled
	monitor.Spec.Targets.Issues.Enabled = true
	monitor.Spec.Triggers.GitHub.Labels.Enabled = true
	monitor.Spec.Policy.ProtectedLabels = []string{"security-sensitive"}
	monitor.Spec.Targets.Issues.ExcludeLabels = []string{"security-sensitive"}
	fc := newGitHubWebhookFakeClient(t, monitor, githubWebhookGitSecret())
	monitorStore := setupGitHubWebhookMonitorStore(t)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"orka:implement"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":31,"state":"open","title":"Sensitive","body":"Do not automate publicly.","html_url":"https://github.com/sozercan/vekil/issues/31","updated_at":"2026-06-01T00:00:00Z","labels":[{"name":"security-sensitive"},{"name":"orka:implement"}]},
		"sender":{"login":"octocat"}
	}`)
	resp := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-guarded-issue", secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want accepted blocked event; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
	commands, _, err := monitorStore.ListCommandEvents(t.Context(), store.CommandEventFilter{Namespace: "default", MonitorName: "guarded-issue-loop", Kind: "issue", Number: 31, Limit: 10})
	if err != nil {
		t.Fatalf("ListCommandEvents() error = %v", err)
	}
	if len(commands) != 1 || commands[0].Status != githubCommandStatusBlocked || !strings.Contains(commands[0].Error, "security-sensitive") {
		t.Fatalf("commands = %#v, want one blocked command with guard label", commands)
	}
	runs, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "guarded-issue-loop", TargetKind: repositoryMonitorTargetKindIssue, TargetNumber: 31, Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %#v, want no run for guarded command", runs)
	}
}

func TestGitHubWebhook_OrkaEquivalentCommandsCoalesceActiveWorkAction(t *testing.T) {
	permissionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permission":"write"}`))
	}))
	t.Cleanup(permissionServer.Close)
	secret := configureGitHubWebhookTest(t, map[string]string{githubAPIBaseURLEnv: permissionServer.URL})
	pullRequestsEnabled := false
	monitor := githubWebhookRepositoryMonitor("coalesce-issue-loop", false)
	monitor.Spec.ForgeCredentialRef = &corev1.LocalObjectReference{Name: githubWebhookTestGitSecret}
	monitor.Spec.Targets.PullRequests.Enabled = &pullRequestsEnabled
	monitor.Spec.Targets.Issues.Enabled = true
	monitor.Spec.Triggers.GitHub.Labels.Enabled = true
	fc := newGitHubWebhookFakeClient(t, monitor, githubWebhookGitSecret())
	monitorStore := setupGitHubWebhookMonitorStore(t)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"orka:plan"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":32,"state":"open","title":"Plan once","body":"Please plan once.","html_url":"https://github.com/sozercan/vekil/issues/32","updated_at":"2026-06-01T00:00:00Z","labels":[{"name":"orka:plan"}]},
		"sender":{"login":"octocat"}
	}`)
	first := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-coalesce-one", secret, body)
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first status = %d, want created; body: %s", first.StatusCode, readRespBody(t, first))
	}
	second := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-coalesce-two", secret, body)
	if second.StatusCode != http.StatusAccepted {
		t.Fatalf("second status = %d, want accepted coalesced event; body: %s", second.StatusCode, readRespBody(t, second))
	}
	commands, _, err := monitorStore.ListCommandEvents(t.Context(), store.CommandEventFilter{Namespace: "default", MonitorName: "coalesce-issue-loop", Kind: "issue", Number: 32, Limit: 10})
	if err != nil {
		t.Fatalf("ListCommandEvents() error = %v", err)
	}
	if len(commands) != 2 {
		t.Fatalf("commands = %#v, want two command records", commands)
	}
	completed := 0
	for _, command := range commands {
		if command.Status == githubCommandStatusCompleted && strings.Contains(command.Error, "coalesced") {
			completed++
		}
	}
	if completed != 1 {
		t.Fatalf("commands = %#v, want one completed coalesced command", commands)
	}
	runs, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "coalesce-issue-loop", TargetKind: repositoryMonitorTargetKindIssue, TargetNumber: 32, Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("runs = %#v, want one run after equivalent command coalescing", runs)
	}
}

func TestGitHubWebhook_OrkaResumeDoesNotBypassGuardLabel(t *testing.T) {
	permissionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permission":"write"}`))
	}))
	t.Cleanup(permissionServer.Close)
	secret := configureGitHubWebhookTest(t, map[string]string{githubAPIBaseURLEnv: permissionServer.URL})
	pullRequestsEnabled := false
	monitor := githubWebhookRepositoryMonitor("guarded-resume", false)
	monitor.Spec.ForgeCredentialRef = &corev1.LocalObjectReference{Name: githubWebhookTestGitSecret}
	monitor.Spec.Targets.PullRequests.Enabled = &pullRequestsEnabled
	monitor.Spec.Targets.Issues.Enabled = true
	monitor.Spec.Triggers.GitHub.Labels.Enabled = true
	monitor.Spec.Policy.PauseLabels = []string{"orka:pause"}
	fc := newGitHubWebhookFakeClient(t, monitor, githubWebhookGitSecret())
	monitorStore := setupGitHubWebhookMonitorStore(t)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})
	body := []byte(`{
		"action":"labeled",
		"label":{"name":"orka:resume"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":34,"state":"open","title":"Paused","body":"Still paused.","html_url":"https://github.com/sozercan/vekil/issues/34","updated_at":"2026-06-01T00:00:00Z","labels":[{"name":"orka:pause"},{"name":"orka:resume"}]},
		"sender":{"login":"octocat"}
	}`)
	resp := performSignedGitHubWebhook(t, server, githubEventIssues, "delivery-guarded-resume", secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want accepted blocked event; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
	commands, _, err := monitorStore.ListCommandEvents(t.Context(), store.CommandEventFilter{Namespace: "default", MonitorName: "guarded-resume", Kind: "issue", Number: 34, Limit: 10})
	if err != nil {
		t.Fatalf("ListCommandEvents() error = %v", err)
	}
	if len(commands) != 1 || commands[0].Status != githubCommandStatusBlocked || !strings.Contains(commands[0].Error, "orka:pause") {
		t.Fatalf("commands = %#v, want blocked resume command", commands)
	}
}

func TestGitHubWebhook_OrkaPRLabelRequiresExactBaseBranchCase(t *testing.T) {
	secret := configureGitHubWebhookTest(t, nil)
	monitor := githubWebhookRepositoryMonitor("case-sensitive-pr", false)
	monitor.Spec.Triggers.GitHub.Labels.Enabled = true
	monitor.Spec.Triggers.GitHub.Labels.PullRequests.Review = "orka:review"
	fc := newGitHubWebhookFakeClient(t, monitor, githubWebhookGitSecret())
	monitorStore := setupGitHubWebhookMonitorStore(t)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})
	body := []byte(`{
		"action":"labeled",
		"label":{"name":"orka:review"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"pull_request":{"number":33,"title":"Wrong case","body":"","html_url":"https://github.com/sozercan/vekil/pull/33","state":"open","draft":false,"base":{"ref":"Main","sha":"base","repo":{"full_name":"sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git"}},"head":{"ref":"feature","sha":"head-sha","repo":{"full_name":"sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git"}},"labels":[{"name":"orka:review"}]},
		"sender":{"login":"octocat"}
	}`)
	resp := performSignedGitHubWebhook(t, server, githubEventPullRequest, "delivery-pr-branch-case", secret, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want accepted ignored event; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
	commands, _, err := monitorStore.ListCommandEvents(t.Context(), store.CommandEventFilter{Namespace: "default", MonitorName: "case-sensitive-pr", Kind: repositoryMonitorTargetKindPullRequest, Number: 33, Limit: 10})
	if err != nil {
		t.Fatalf("ListCommandEvents() error = %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("commands = %#v, want none for case-mismatched base branch", commands)
	}
}

func TestGitHubWebhook_DuplicateAcceptedCommandEnsuresMissingRun(t *testing.T) {
	permissionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permission":"write"}`))
	}))
	t.Cleanup(permissionServer.Close)
	secret := configureGitHubWebhookTest(t, map[string]string{githubAPIBaseURLEnv: permissionServer.URL})
	pullRequestsEnabled := false
	monitor := githubWebhookRepositoryMonitor("dedupe-run", false)
	monitor.Spec.ForgeCredentialRef = &corev1.LocalObjectReference{Name: githubWebhookTestGitSecret}
	monitor.Spec.Targets.PullRequests.Enabled = &pullRequestsEnabled
	monitor.Spec.Targets.Issues.Enabled = true
	monitor.Spec.Triggers.GitHub.Labels.Enabled = true
	fc := newGitHubWebhookFakeClient(t, monitor, githubWebhookGitSecret())
	monitorStore := setupGitHubWebhookMonitorStore(t)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"orka:plan"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":21,"title":"Plan it","body":"Please plan.","html_url":"https://github.com/sozercan/vekil/issues/21","updated_at":"2026-06-01T00:00:00Z","labels":[{"name":"orka:plan"}]},
		"sender":{"login":"octocat"}
	}`)
	target, ok := githubLabelWebhookPayload{
		Action:     "labeled",
		Label:      githubWebhookLabel{Name: "orka:plan"},
		Repository: githubWebhookRepository{FullName: "sozercan/vekil"},
		Issue:      &githubWebhookIssue{Number: 21, Title: "Plan it", Body: "Please plan.", Labels: []githubWebhookLabel{{Name: "orka:plan"}}},
	}.target()
	if !ok {
		t.Fatal("target() failed")
	}
	delivery := "delivery-preexisting-command"
	dedupe := repositoryMonitorCommandDedupeKey(monitor, target, "orka:plan", delivery)
	processedAt := time.Now()
	if err := monitorStore.CreateCommandEvent(t.Context(), &store.CommandEvent{
		ID:               repositoryMonitorCommandID(dedupe),
		MonitorNamespace: "default",
		MonitorName:      "dedupe-run",
		Repo:             "sozercan/vekil",
		Kind:             "issue",
		Number:           21,
		Source:           githubCommandEventSourceLabel,
		DeliveryID:       delivery,
		Label:            "orka:plan",
		DedupeKey:        dedupe,
		IdempotencyKey:   dedupe,
		Intent:           "plan",
		Status:           githubCommandStatusAccepted,
		CreatedAt:        processedAt,
		ProcessedAt:      &processedAt,
	}); err != nil {
		t.Fatalf("CreateCommandEvent() error = %v", err)
	}
	resp := performSignedGitHubWebhook(t, server, githubEventIssues, delivery, secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want created; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
	runs, _, err := monitorStore.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "dedupe-run", TargetKind: repositoryMonitorTargetKindIssue, TargetNumber: 21, Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 1 || runs[0].Trigger != githubMonitorTriggerLabelCommand {
		t.Fatalf("runs = %#v, want missing run repaired for duplicate command", runs)
	}
}

func TestGitHubWebhook_DuplicateAcceptedCommandRetriesFailedRunSignal(t *testing.T) {
	permissionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permission":"write"}`))
	}))
	t.Cleanup(permissionServer.Close)
	secret := configureGitHubWebhookTest(t, map[string]string{githubAPIBaseURLEnv: permissionServer.URL})
	pullRequestsEnabled := false
	monitor := githubWebhookRepositoryMonitor("dedupe-failed-run", false)
	monitor.Spec.ForgeCredentialRef = &corev1.LocalObjectReference{Name: githubWebhookTestGitSecret}
	monitor.Spec.Targets.PullRequests.Enabled = &pullRequestsEnabled
	monitor.Spec.Targets.Issues.Enabled = true
	monitor.Spec.Triggers.GitHub.Labels.Enabled = true
	fc := newGitHubWebhookFakeClient(t, monitor, githubWebhookGitSecret())
	monitorStore := setupGitHubWebhookMonitorStore(t)
	server := NewServer(fc, nil, ServerConfig{RepositoryMonitorStore: monitorStore})

	body := []byte(`{
		"action":"labeled",
		"label":{"name":"orka:plan"},
		"repository":{"full_name":"sozercan/vekil","html_url":"https://github.com/sozercan/vekil","clone_url":"https://github.com/sozercan/vekil.git","default_branch":"main"},
		"issue":{"number":22,"state":"open","title":"Plan after retry","body":"Please plan.","html_url":"https://github.com/sozercan/vekil/issues/22","updated_at":"2026-06-01T00:00:00Z","labels":[{"name":"orka:plan"}]},
		"sender":{"login":"octocat"}
	}`)
	target, ok := githubLabelWebhookPayload{
		Action:     "labeled",
		Label:      githubWebhookLabel{Name: "orka:plan"},
		Repository: githubWebhookRepository{FullName: "sozercan/vekil"},
		Issue:      &githubWebhookIssue{Number: 22, State: "open", Title: "Plan after retry", Body: "Please plan.", Labels: []githubWebhookLabel{{Name: "orka:plan"}}},
	}.target()
	if !ok {
		t.Fatal("target() failed")
	}
	delivery := "delivery-failed-run-retry"
	dedupe := repositoryMonitorCommandDedupeKey(monitor, target, "orka:plan", delivery)
	processedAt := time.Now()
	command := &store.CommandEvent{ID: repositoryMonitorCommandID(dedupe), MonitorNamespace: "default", MonitorName: monitor.Name, Repo: "sozercan/vekil", Kind: "issue", Number: 22, Source: githubCommandEventSourceLabel, DeliveryID: delivery, Label: "orka:plan", DedupeKey: dedupe, IdempotencyKey: dedupe, Intent: "plan", Status: githubCommandStatusAccepted, CreatedAt: processedAt, ProcessedAt: &processedAt}
	if err := monitorStore.CreateCommandEvent(t.Context(), command); err != nil {
		t.Fatalf("CreateCommandEvent() error = %v", err)
	}
	completedAt := time.Now().Add(-time.Minute)
	failedRun := &store.MonitorRun{ID: repositoryMonitorCommandRunID(command), MonitorNamespace: "default", MonitorName: monitor.Name, Trigger: githubMonitorTriggerLabelCommand, TargetKind: repositoryMonitorTargetKindIssue, TargetNumber: 22, CommandEventID: command.ID, Phase: repositoryMonitorRunPhaseFailed, StartedAt: time.Now().Add(-2 * time.Minute), CompletedAt: &completedAt, Error: "failed to signal repository monitor run: conflict"}
	if err := monitorStore.CreateMonitorRun(t.Context(), failedRun); err != nil {
		t.Fatalf("CreateMonitorRun(failed) error = %v", err)
	}
	if err := monitorStore.CreateWorkAction(t.Context(), &store.WorkAction{
		ID:               store.RepositoryMonitorWorkActionID(command.ID, store.RepositoryMonitorDesiredActionForIntent(command.Intent)),
		MonitorNamespace: "default",
		MonitorName:      monitor.Name,
		CommandEventID:   command.ID,
		TargetKind:       repositoryMonitorTargetKindIssue,
		TargetNumber:     22,
		Intent:           command.Intent,
		DesiredAction:    store.RepositoryMonitorDesiredActionForIntent(command.Intent),
		Status:           store.RepositoryMonitorWorkActionStatusRetryPending,
		Phase:            store.RepositoryMonitorWorkActionStatusRetryPending,
		BlockedReason:    store.RepositoryMonitorWorkActionBlockedReasonRunSignalFailed,
		Error:            "failed to signal repository monitor run: conflict",
	}); err != nil {
		t.Fatalf("CreateWorkAction(failed) error = %v", err)
	}
	resp := performSignedGitHubWebhook(t, server, githubEventIssues, delivery, secret, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want created retry; body: %s", resp.StatusCode, readRespBody(t, resp))
	}
	run, err := monitorStore.GetMonitorRun(t.Context(), "default", failedRun.ID)
	if err != nil {
		t.Fatalf("GetMonitorRun() error = %v", err)
	}
	if run.Phase != repositoryMonitorRunPhaseQueued || run.CompletedAt != nil || run.Error != "" {
		t.Fatalf("run after retry = %#v, want queued unsignaled retry", run)
	}
	action, err := monitorStore.GetWorkAction(t.Context(), "default", store.RepositoryMonitorWorkActionID(command.ID, store.RepositoryMonitorDesiredActionForIntent(command.Intent)))
	if err != nil {
		t.Fatalf("GetWorkAction() error = %v", err)
	}
	if action.Status != repositoryMonitorRunPhaseQueued || action.CompletedAt != nil || action.Error != "" || action.BlockedReason != "" {
		t.Fatalf("work action after retry = %#v, want queued with terminal fields cleared", action)
	}
}

func TestRepositoryMonitorPermissionAllowedEnforcesMinimumAndPolicy(t *testing.T) {
	monitor := githubWebhookRepositoryMonitor("permission-policy", false)
	monitor.Spec.Triggers.GitHub.Labels.RequireActorPermission = githubPermissionAdmin
	monitor.Spec.Policy.AllowedRepositoryPermissions = []string{githubPermissionWrite, githubPermissionAdmin}
	if repositoryMonitorPermissionAllowed(monitor, githubPermissionWrite) {
		t.Fatal("write permission satisfied policy despite admin minimum")
	}
	if !repositoryMonitorPermissionAllowed(monitor, githubPermissionAdmin) {
		t.Fatal("admin permission rejected despite satisfying minimum and policy")
	}
	monitor.Spec.Triggers.GitHub.Labels.RequireActorPermission = githubPermissionMaintain
	monitor.Spec.Policy.AllowedRepositoryPermissions = nil
	if !repositoryMonitorPermissionAllowed(monitor, githubRepositoryPermissionFromResponse(githubPermissionMaintain, githubPermissionWrite)) {
		t.Fatal("maintain role_name should satisfy maintain minimum even when legacy permission is write")
	}
	if !repositoryMonitorPermissionAllowedForIntent(monitor, githubRepositoryPermissionFromResponse(githubPermissionTriage, githubPermissionRead), "plan") {
		t.Fatal("triage role_name should satisfy read-only plan command")
	}
	monitor.Spec.Triggers.GitHub.Labels.RequireActorPermission = githubPermissionWrite
	monitor.Spec.Policy.AllowedRepositoryPermissions = []string{githubPermissionWrite, githubPermissionAdmin}
	if !repositoryMonitorPermissionAllowed(monitor, githubRepositoryPermissionFromResponse("release-manager", githubPermissionWrite)) {
		t.Fatal("custom role_name should fall back to effective write permission")
	}
	monitor.Spec.Triggers.GitHub.Labels.RequireActorPermission = githubPermissionAdmin
	if !repositoryMonitorPermissionAllowed(monitor, githubRepositoryPermissionFromResponse("security-admin", githubPermissionAdmin)) {
		t.Fatal("custom role_name should fall back to effective admin permission")
	}
}

func TestGitHubIssueSnapshotDigestIgnoresVolatileUpdatedAt(t *testing.T) {
	monitor := &corev1alpha1.RepositoryMonitor{}
	target := githubLabelTarget{Kind: repositoryMonitorTargetKindIssue, Number: 42, Title: "Title", Body: "Body", Labels: []string{"bug"}, UpdatedAt: time.Unix(100, 0)}
	first := githubIssueSnapshotDigest(monitor, target)
	target.UpdatedAt = time.Unix(200, 0)
	if second := githubIssueSnapshotDigest(monitor, target); second != first {
		t.Fatalf("digest changed for identical agent input: first=%s second=%s", first, second)
	}
	target.Body = "Changed body"
	if changed := githubIssueSnapshotDigest(monitor, target); changed == first {
		t.Fatal("digest did not change when issue body changed")
	}
}
