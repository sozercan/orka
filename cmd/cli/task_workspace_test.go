package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTaskCreateWritesCanonicalWorkspaceCredentialRoles(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/agents/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "write-task"}}) //nolint:errcheck
	}))
	defer server.Close()

	root := newRootCmd()
	root.SetArgs([]string{
		"--server", server.URL, "--token", "test-token", "--namespace", "default",
		"task", "create", "Update the repository", "--type", "agent", "--agent", "codex-agent", "--name", "write-task",
		"--workspace-intent", "write", "--git-repo", "https://github.com/source/repo",
		"--source-repository-provider", "github", "--source-repository-id", "github.com/source/repo",
		"--read-credential", "repo-read", "--read-credential-key", "source-token",
		"--publication-git-repo", "https://github.com/publish/repo", "--publication-repository-provider", "github",
		"--publication-repository-id", "github.com/publish/repo",
		"--publication-read-credential", "repo-verify", "--publication-read-credential-key", "verify-token",
		"--publication-credential", "repo-write", "--publication-credential-key", "write-token",
		"--forge-credential", "repo-forge", "--forge-credential-key", "forge-token",
		"--push-branch", "orka/change", "--pr-base-branch", "main", "--create-pr",
	})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	workspace := nestedMap(body, "workspace")
	if got := anyString(workspace["intent"]); got != "write" {
		t.Fatalf("intent = %q", got)
	}
	for _, want := range []struct {
		field string
		name  string
		key   string
	}{
		{field: "readCredentialRef", name: "repo-read", key: "source-token"},
		{field: "publicationReadCredentialRef", name: "repo-verify", key: "verify-token"},
		{field: "publicationCredentialRef", name: "repo-write", key: "write-token"},
		{field: "forgeCredentialRef", name: "repo-forge", key: "forge-token"},
	} {
		if got := nestedString(workspace, want.field, "name"); got != want.name {
			t.Errorf("%s name = %q, want %q", want.field, got, want.name)
		}
		if got := nestedString(workspace, want.field, "key"); got != want.key {
			t.Errorf("%s key = %q, want %q", want.field, got, want.key)
		}
	}
	if got := anyString(workspace["createPR"]); got != "true" {
		t.Fatalf("createPR = %q", got)
	}
	if _, legacy := nestedMap(body, "agentRuntime")["workspace"]; legacy {
		t.Fatal("request contains deprecated agentRuntime.workspace")
	}
	if _, legacy := workspace["gitSecretRef"]; legacy {
		t.Fatal("request contains deprecated gitSecretRef")
	}
}

func TestTaskCreateLeavesCredentialKeyOmittedForAPIDefault(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/agents/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "read-task"}}) //nolint:errcheck
	}))
	defer server.Close()

	root := newRootCmd()
	root.SetArgs([]string{
		"--server", server.URL, "--token", "test-token",
		"task", "create", "Inspect", "--type", "agent", "--agent", "a",
		"--git-repo", "https://github.com/source/repo", "--read-credential", "repo-read",
	})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	ref := nestedMap(nestedMap(body, "workspace"), "readCredentialRef")
	if ref["name"] != "repo-read" {
		t.Fatalf("readCredentialRef = %#v", ref)
	}
	if _, ok := ref["key"]; ok {
		t.Fatalf("readCredentialRef key should be omitted for the API token default: %#v", ref)
	}
}

func TestTaskCreateOmitsWorkspaceForPromptOnlyAgentTask(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/agents/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "prompt-task"}}) //nolint:errcheck
	}))
	defer server.Close()

	root := newRootCmd()
	root.SetArgs([]string{
		"--server", server.URL, "--token", "test-token",
		"task", "create", "Answer a question", "--type", "agent", "--agent", "a",
	})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["workspace"]; ok {
		t.Fatalf("prompt-only agent task must omit workspace, got %#v", body["workspace"])
	}
}

func TestTaskCreateOmitsWorkspaceForExplicitReadIntentWithoutFields(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/agents/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "prompt-task"}}) //nolint:errcheck
	}))
	defer server.Close()

	root := newRootCmd()
	root.SetArgs([]string{
		"--server", server.URL, "--token", "test-token",
		"task", "create", "Answer a question", "--type", "agent", "--agent", "a", "--workspace-intent", "read",
	})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["workspace"]; ok {
		t.Fatalf("explicit default read intent alone must omit workspace, got %#v", body["workspace"])
	}
}

func TestTaskCreateRejectsExplicitWriteIntentWithoutGitRepo(t *testing.T) {
	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "Change things", "--type", "agent", "--agent", "a", "--workspace-intent", "write"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "--workspace-intent write requires --git-repo") {
		t.Fatalf("error = %v", err)
	}
}

func TestTaskCreateRejectsWorkspaceIntentForNonAgentTask(t *testing.T) {
	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "--type", "container", "--image", "busybox", "--workspace-intent", "read"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "workspace flags are supported only for agent tasks") {
		t.Fatalf("error = %v", err)
	}
}

func TestTaskCreateRejectsSourceSelectorsWithoutRepository(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "branch without repository", args: []string{"--branch", "main"}, want: "--branch requires --git-repo"},
		{name: "ref without repository", args: []string{"--ref", "refs/heads/main"}, want: "--ref requires --git-repo"},
		{name: "sub-path without repository", args: []string{"--sub-path", "services/api"}, want: "--sub-path requires --git-repo"},
		{name: "read credential without repository", args: []string{"--read-credential", "repo-read"}, want: "--read-credential requires --git-repo"},
		{
			name: "source repository identity without repository",
			args: []string{"--source-repository-provider", "github", "--source-repository-id", "github.com/owner/repo"},
			want: "--source-repository-provider requires --git-repo",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := newRootCmd()
			root.SetArgs(append([]string{"task", "create", "Inspect", "--type", "agent", "--agent", "a"}, tt.args...))
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestTaskCreateRejectsMalformedSourceSelectors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "unsupported ref namespace as branch",
			args: []string{"--git-repo", "https://github.com/owner/repo", "--branch", "refs/remotes/origin/main"},
			want: "--branch is invalid",
		},
		{
			name: "malformed branch",
			args: []string{"--git-repo", "https://github.com/owner/repo", "--branch", "bad..branch"},
			want: "--branch is invalid",
		},
		{
			name: "unsupported ref namespace",
			args: []string{"--git-repo", "https://github.com/owner/repo", "--ref", "refs/remotes/origin/main"},
			want: "--ref is invalid",
		},
		{
			name: "malformed ref",
			args: []string{"--git-repo", "https://github.com/owner/repo", "--ref", "refs/heads/bad..ref"},
			want: "--ref is invalid",
		},
		{
			name: "traversal sub-path",
			args: []string{"--git-repo", "https://github.com/owner/repo", "--sub-path", "../private"},
			want: "--sub-path contains an unsafe segment",
		},
		{
			name: "absolute sub-path",
			args: []string{"--git-repo", "https://github.com/owner/repo", "--sub-path", "/absolute"},
			want: "--sub-path must be a relative slash-separated path",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := newRootCmd()
			root.SetArgs(append([]string{"task", "create", "Inspect", "--type", "agent", "--agent", "a"}, tt.args...))
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestTaskCreateRejectsMismatchedRepositoryIdentitiesAndInvalidPublicationBranches(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "source identity mismatch",
			args: []string{
				"--git-repo", "https://github.com/owner/repo",
				"--source-repository-provider", "github", "--source-repository-id", "github.com/other/repo",
			},
			want: `--source-repository-id must match the canonical credential-free URL identity "github.com/owner/repo"`,
		},
		{
			name: "source identity provider not github",
			args: []string{
				"--git-repo", "https://github.com/owner/repo",
				"--source-repository-provider", "gitlab", "--source-repository-id", "github.com/owner/repo",
			},
			want: "--source-repository-provider must be github",
		},
		{
			name: "publication identity mismatch against source fallback",
			args: []string{
				"--workspace-intent", "write", "--git-repo", "https://github.com/owner/repo",
				"--publication-credential", "repo-write",
				"--publication-repository-provider", "github", "--publication-repository-id", "github.com/other/repo",
			},
			want: `--publication-repository-id must match the canonical credential-free URL identity "github.com/owner/repo"`,
		},
		{
			name: "invalid push branch",
			args: []string{
				"--workspace-intent", "write", "--git-repo", "https://github.com/owner/repo",
				"--publication-credential", "repo-write", "--push-branch", "bad..branch",
			},
			want: "--push-branch is invalid",
		},
		{
			name: "invalid pull request base branch",
			args: []string{
				"--workspace-intent", "write", "--git-repo", "https://github.com/owner/repo",
				"--publication-credential", "repo-write", "--pr-base-branch", "bad..branch",
			},
			want: "--pr-base-branch is invalid",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := newRootCmd()
			root.SetArgs(append([]string{"task", "create", "Publish", "--type", "agent", "--agent", "a"}, tt.args...))
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestTaskCreateCanonicalizesSSHRepositoryURL(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/agents/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "ssh-task"}}) //nolint:errcheck
	}))
	defer server.Close()

	root := newRootCmd()
	root.SetArgs([]string{
		"--server", server.URL, "--token", "test-token",
		"task", "create", "Inspect", "--type", "agent", "--agent", "a",
		"--git-repo", "git@github.com:owner/repo.git",
	})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if got := anyString(nestedMap(body, "workspace")["gitRepo"]); got != "https://github.com/owner/repo" {
		t.Fatalf("gitRepo = %q, want canonical https://github.com/owner/repo", got)
	}
}

func TestTaskCreateRejectsInvalidRepositoryURLs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "plain HTTP source URL",
			args: []string{"--git-repo", "http://github.com/owner/repo"},
			want: "--git-repo must be a credential-free HTTPS URL",
		},
		{
			name: "credentialed source URL",
			args: []string{"--git-repo", "https://user:token@github.com/owner/repo"},
			want: "--git-repo must be a credential-free HTTPS URL",
		},
		{
			name: "non-GitHub SSH publication URL",
			args: []string{
				"--workspace-intent", "write", "--git-repo", "https://github.com/owner/repo",
				"--publication-credential", "repo-write", "--publication-git-repo", "git@gitlab.example.com:owner/repo.git",
			},
			want: "--publication-git-repo must be a credential-free HTTPS URL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := newRootCmd()
			root.SetArgs(append([]string{"task", "create", "Inspect", "--type", "agent", "--agent", "a"}, tt.args...))
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestTaskCreateRejectsPublicationForReadIntent(t *testing.T) {
	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "Inspect", "--type", "agent", "--agent", "a", "--publication-credential", "repo-write"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "publication flags require --workspace-intent write") {
		t.Fatalf("error = %v", err)
	}
}

func TestTaskCreateRejectsIncompleteWriteWorkspace(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "source repository",
			args: []string{"--publication-credential", "repo-write"},
			want: "--workspace-intent write requires --git-repo",
		},
		{
			name: "publication credential",
			args: []string{"--git-repo", "https://github.com/source/repo"},
			want: "--workspace-intent write requires --publication-credential",
		},
		{
			name: "pull request base branch",
			args: []string{
				"--git-repo", "https://github.com/source/repo", "--publication-credential", "repo-write",
				"--forge-credential", "repo-forge", "--create-pr",
			},
			want: "--create-pr requires --pr-base-branch",
		},
		{
			name: "forge credential",
			args: []string{
				"--git-repo", "https://github.com/source/repo", "--publication-credential", "repo-write",
				"--pr-base-branch", "main", "--create-pr",
			},
			want: "--create-pr requires --forge-credential",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := newRootCmd()
			args := []string{
				"task", "create", "Publish", "--type", "agent", "--agent", "a", "--workspace-intent", "write",
			}
			root.SetArgs(append(args, tt.args...))
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestTaskCreateRejectsCredentialKeyWithoutSecretName(t *testing.T) {
	root := newRootCmd()
	root.SetArgs([]string{
		"task", "create", "Inspect", "--type", "agent", "--agent", "a", "--read-credential-key", "custom-token",
	})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "--read-credential-key requires --read-credential") {
		t.Fatalf("error = %v", err)
	}
}

func TestSafeWorkspaceStatusDoesNotTreatWriteCredentialAsForgeCredential(t *testing.T) {
	status := safeWorkspaceStatus(map[string]any{
		"spec": map[string]any{"workspace": map[string]any{
			"intent": "write", "createPR": true,
			"publicationCredentialRef": map[string]any{"name": "legacy-combined-credential"},
		}},
	})
	workspace := nestedMap(status, "workspace")
	if workspace["publicationCredentialConfigured"] != true || workspace["publicationWriteCredentialConfigured"] != true {
		t.Fatalf("write credential summary = %#v", workspace)
	}
	if workspace["forgeCredentialConfigured"] != false {
		t.Fatalf("write credential was silently treated as a forge credential: %#v", workspace)
	}
}

func TestTaskStatusRendersPoolDeliveryAndUnknownReplayPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"metadata": map[string]any{"name": "uncertain", "namespace": "default"},
			"spec":     map[string]any{"type": "agent", "workspace": map[string]any{"intent": "write"}},
			"status": map[string]any{
				"phase":     "Failed",
				"execution": map[string]any{"state": "OutcomeUnknown", "outcome": "OutcomeUnknown", "runtimePoolName": "codex-write", "runtimeInstanceID": "pod:boot", "runtimeSessionGeneration": 2},
				"delivery":  map[string]any{"state": "PublicationOutcomeUnknown", "branch": "orka/change"},
			},
		})
	}))
	defer server.Close()

	var out bytes.Buffer
	root := newRootCmd()
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"--server", server.URL, "--token", "test-token", "task", "status", "uncertain"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"OutcomeUnknown", "codex-write", "PublicationOutcomeUnknown", "No automatic replay"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output missing %q:\n%s", want, out.String())
		}
	}
}

func TestSafeWorkspaceStatusUsesCanonicalSpecAndDelivery(t *testing.T) {
	const secretValue = "must-never-render"
	status := safeWorkspaceStatus(map[string]any{
		"metadata": map[string]any{"name": "write", "namespace": "default"},
		"spec": map[string]any{"workspace": map[string]any{
			"intent": "write", "gitRepo": "https://github.com/source/repo", "createPR": true,
			"readCredentialRef":            map[string]any{"name": "repo-read", "key": "source-token", "value": secretValue},
			"publicationReadCredentialRef": map[string]any{"name": "repo-verify", "key": "verify-token", "value": secretValue},
			"publicationCredentialRef":     map[string]any{"name": "repo-write", "key": "write-token", "value": secretValue},
			"forgeCredentialRef":           map[string]any{"name": "repo-forge", "key": "forge-token", "value": secretValue},
		}},
		"status": map[string]any{"phase": "Running", "delivery": map[string]any{"state": "Publishing"}},
	})
	workspace := nestedMap(status, "workspace")
	for _, field := range []string{
		"readCredentialConfigured", "publicationReadCredentialConfigured", "publicationCredentialConfigured",
		"publicationWriteCredentialConfigured", "forgeCredentialConfigured",
	} {
		if workspace[field] != true {
			t.Errorf("%s = %#v, want true", field, workspace[field])
		}
	}
	for _, field := range []string{
		"readCredentialRef", "publicationReadCredentialRef", "publicationCredentialRef", "forgeCredentialRef",
	} {
		if _, leaked := workspace[field]; leaked {
			t.Errorf("safe workspace status leaked %s", field)
		}
	}
	if strings.Contains(fmt.Sprint(status), secretValue) {
		t.Fatal("safe workspace status leaked a Secret value")
	}
	if got := nestedString(status, "delivery", "state"); got != "Publishing" {
		t.Fatalf("delivery state = %q", got)
	}
}
