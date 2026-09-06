/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package common

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/workerenv"
)

// AgentConfig holds worker configuration from environment variables.
type AgentConfig struct {
	TaskName           string
	TaskNamespace      string
	TransactionID      string
	TransactionProfile string
	Prompt             string
	Model              string
	SystemPrompt       string
	MaxTurns           int
	AllowedTools       []string
	AllowedToolsSet    bool
	DisallowedTools    []string
	GitRepo            string
	GitBranch          string
	GitRef             string
	ShallowGitRef      bool
	PRBaseBranch       string
	PRBaseRepo         string
	PRBaseSHA          string
	SubPath            string
	TimeoutSeconds     int

	securityReviewContextArtifact string
	securityReviewContextManifest []byte
}

// LoadConfig reads and validates agent configuration from environment variables.
func LoadConfig(defaultMaxTurns int) (*AgentConfig, error) {
	return loadConfig(defaultMaxTurns, true)
}

// LoadWorkspaceConfig reads and validates workspace configuration without
// requiring an agent prompt. Container workers use this for deterministic tasks.
func LoadWorkspaceConfig() (*AgentConfig, error) {
	return loadConfig(0, false)
}

func loadConfig(defaultMaxTurns int, requirePrompt bool) (*AgentConfig, error) {
	cfg := &AgentConfig{
		TaskName:           os.Getenv(workerenv.TaskName),
		TaskNamespace:      os.Getenv(workerenv.TaskNamespace),
		TransactionID:      os.Getenv(workerenv.TransactionID),
		TransactionProfile: os.Getenv(workerenv.TransactionProfile),
		Prompt:             os.Getenv(workerenv.Prompt),
		Model:              os.Getenv(workerenv.Model),
		SystemPrompt:       os.Getenv(workerenv.SystemPrompt),
		GitRepo:            os.Getenv(workerenv.GitRepo),
		GitBranch:          os.Getenv(workerenv.GitBranch),
		GitRef:             os.Getenv(workerenv.GitRef),
		ShallowGitRef:      workerenv.IsTrue(os.Getenv(workerenv.GitRefShallow)),
		PRBaseBranch:       os.Getenv(workerenv.PRBaseBranch),
		PRBaseRepo:         os.Getenv(workerenv.PRBaseRepo),
		PRBaseSHA:          os.Getenv(workerenv.PRBaseSHA),
		SubPath:            os.Getenv(workerenv.WorkspaceSubpath),
		MaxTurns:           defaultMaxTurns,
	}

	if requirePrompt && cfg.Prompt == "" {
		return nil, fmt.Errorf("%s is required", workerenv.Prompt)
	}

	if v := os.Getenv(workerenv.MaxTurns); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid %s: %w", workerenv.MaxTurns, err)
		}
		cfg.MaxTurns = n
	}

	if v, ok := os.LookupEnv(workerenv.AllowedTools); ok {
		cfg.AllowedToolsSet = true
		if v != "" {
			cfg.AllowedTools = strings.Split(v, ",")
		}
	}
	if v := os.Getenv(workerenv.DisallowedTools); v != "" {
		cfg.DisallowedTools = strings.Split(v, ",")
	}

	if v := os.Getenv(workerenv.TimeoutSeconds); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid %s: %w", workerenv.TimeoutSeconds, err)
		}
		cfg.TimeoutSeconds = n
	}

	// Sanitize SubPath to prevent directory traversal
	if cfg.SubPath != "" {
		cleaned := filepath.Clean(cfg.SubPath)
		if filepath.IsAbs(cleaned) || strings.HasPrefix(cleaned, "..") {
			return nil, fmt.Errorf("%s %q contains path traversal", workerenv.WorkspaceSubpath, cfg.SubPath)
		}
		cfg.SubPath = cleaned
	}

	return cfg, nil
}

// SetupGitCredentials sets git credential env vars globally so both clone and
// agent-initiated git operations (push, fetch) can authenticate.
func SetupGitCredentials() {
	tokenPaths := []string{
		"/secrets/git/token",
		"/secrets/git/password",
		"/secrets/git/GITHUB_TOKEN",
	}
	for _, path := range tokenPaths {
		if data, err := os.ReadFile(path); err == nil {
			token := strings.TrimSpace(string(data))
			if token != "" {
				os.Setenv(workerenv.GitToken, token)               //nolint:errcheck
				os.Setenv(workerenv.GitHubToken, token)            //nolint:errcheck
				os.Setenv(workerenv.GitAskpass, "/bin/echo-token") //nolint:errcheck
				break
			}
		}
	}
	if data, err := os.ReadFile("/secrets/git/username"); err == nil {
		username := strings.TrimSpace(string(data))
		if username != "" {
			os.Setenv(workerenv.GitUsername, username) //nolint:errcheck
		}
	}
}

// CloneRepo clones the configured git repository into the workspace directory.
//
// When the workspace already contains a git repository (e.g. a sandbox
// workspace reused across turns of the same session), CloneRepo skips the
// clone and refreshes in place. Branch workspaces are fast-forwarded only
// when the configured branch is still checked out; a session-created branch
// is preserved as part of the reused workspace state.
//
// When ORKA_PUSH_BRANCH is set and no ORKA_GIT_REF pinned a specific commit,
// CloneRepo also creates and checks out a local branch with that name. This
// way any agent-initiated `git push origin HEAD` lands on the intended remote
// branch instead of the upstream default (often "main"). The post-run worker
// finalize step still owns the canonical commit + push.
func CloneRepo(ctx context.Context, cfg *AgentConfig, workspaceDir string) error {
	// Detect a reused workspace: if <workspaceDir>/.git exists we already
	// have a clone (sandbox session reuse). Re-running `git clone` would
	// fail with "destination path already exists". Refresh in place instead.
	if info, err := os.Stat(filepath.Join(workspaceDir, ".git")); err == nil && (info.IsDir() || info.Mode().IsRegular()) {
		fmt.Printf("Reusing existing git repo at %s (sandbox workspace reuse)\n", workspaceDir)
		if err := validateReusedGitRemote(ctx, workspaceDir, cfg.GitRepo); err != nil {
			return err
		}
		if cfg.GitBranch != "" && cfg.GitRef == "" {
			if err := refreshReusedGitBranch(ctx, workspaceDir, cfg.GitBranch); err != nil {
				return err
			}
		}
		if cfg.GitRef != "" {
			fetchMode, err := fetchGitRef(ctx, workspaceDir, cfg.GitRef)
			if err != nil {
				return err
			}
			if err := checkoutGitRef(ctx, workspaceDir, cfg.GitRef, fetchMode); err != nil {
				return err
			}
		}
		if cfg.GitRef == "" {
			if err := checkoutPushBranchForAgentRun(ctx, workspaceDir, cfg.GitBranch); err != nil {
				return err
			}
		}
		return nil
	}

	fmt.Printf("Cloning %s into %s\n", cfg.GitRepo, workspaceDir)

	args := []string{"clone"}

	if cfg.GitBranch != "" {
		args = append(args, "--branch", cfg.GitBranch)
	}

	args = append(args, "--single-branch")
	if cfg.GitRef == "" || cfg.ShallowGitRef {
		args = append(args, "--depth=1")
	}
	args = append(args, cfg.GitRepo, workspaceDir)

	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git clone failed: %w", err)
	}

	// Checkout specific ref if provided (overrides branch). Validation tasks often
	// pin workspace.ref to a pushed branch head SHA without also providing the
	// branch name, so fall back to fetching all remote heads when the server does
	// not allow fetching the object by SHA directly.
	if cfg.GitRef != "" {
		var fetchMode gitRefFetchMode
		var err error
		if cfg.ShallowGitRef {
			fetchMode, err = fetchGitRefShallow(ctx, workspaceDir, cfg.GitRef)
		} else {
			fetchMode, err = fetchGitRef(ctx, workspaceDir, cfg.GitRef)
		}
		if err != nil {
			return err
		}
		if err := checkoutGitRef(ctx, workspaceDir, cfg.GitRef, fetchMode); err != nil {
			return err
		}
	}

	// If ORKA_PUSH_BRANCH is set and we're not pinned to a specific ref, create
	// and check out a local branch with that name. This way any agent-initiated
	// `git push origin HEAD` lands on the intended remote branch rather than
	// overwriting "main" (or whatever the upstream default branch was). Skipped
	// for ref-pinned validation tasks because those aren't expected to push.
	if cfg.GitRef == "" {
		if err := checkoutPushBranchForAgentRun(ctx, workspaceDir, cfg.GitBranch); err != nil {
			return err
		}
	}

	return nil
}

func checkoutPushBranchForAgentRun(ctx context.Context, workspaceDir, baseBranch string) error {
	pushBranch := strings.TrimSpace(os.Getenv(workerenv.PushBranch))
	if pushBranch == "" {
		return nil
	}
	args := []string{"checkout", "-B", pushBranch}
	if remoteBase := remoteBranchStartPoint(ctx, workspaceDir, baseBranch); remoteBase != "" {
		args = append(args, remoteBase)
	}
	if err := execGitContext(ctx, workspaceDir, args...); err != nil {
		return fmt.Errorf("pre-checkout push branch %q failed: %w", pushBranch, err)
	}
	fmt.Printf("Pre-checked out push branch %s before agent run\n", pushBranch)
	return nil
}

func remoteBranchStartPoint(ctx context.Context, workspaceDir, branch string) string {
	branch = strings.TrimSpace(branch)
	if branch == "" {
		if err := execGitContext(ctx, workspaceDir, "fetch", "origin", "HEAD"); err != nil {
			return ""
		}
		return "FETCH_HEAD"
	}
	branch, ok := gitBranchNameFromRef(ctx, workspaceDir, branch)
	if !ok {
		return ""
	}
	remoteRef := "refs/remotes/origin/" + branch
	if err := execGitContext(ctx, workspaceDir, "rev-parse", "--verify", "--quiet", remoteRef); err != nil {
		return ""
	}
	return remoteRef
}

type gitRefFetchMode int

const (
	gitRefFetchDirect gitRefFetchMode = iota
	gitRefFetchRemoteBranch
	gitRefFetchRemoteHeads
)

func validateReusedGitRemote(ctx context.Context, workspaceDir, expectedRepo string) error {
	if strings.TrimSpace(expectedRepo) == "" {
		return nil
	}
	remoteURL, err := execGitOutputContext(ctx, workspaceDir, "remote", "get-url", "origin")
	if err != nil {
		return fmt.Errorf("git inspect origin remote on reused workspace failed: %w", err)
	}
	if strings.TrimSpace(remoteURL) != strings.TrimSpace(expectedRepo) {
		return fmt.Errorf(
			"existing git remote origin does not match configured repo (actual %q, expected %q)",
			gitRemoteForError(remoteURL),
			gitRemoteForError(expectedRepo),
		)
	}
	return nil
}

func gitRemoteForError(remote string) string {
	remote = strings.TrimSpace(remote)
	if parsed, err := url.Parse(remote); err == nil && parsed.Scheme != "" && parsed.User != nil {
		parsed.User = url.User("redacted")
		parsed.RawQuery = ""
		parsed.Fragment = ""
		return redact.SensitiveText(parsed.String())
	}
	if at := strings.Index(remote, "@"); at > 0 && strings.Contains(remote[:at], ":") {
		return redact.SensitiveText("redacted" + remote[at:])
	}
	if parsed, err := url.Parse(remote); err == nil && parsed.Scheme != "" {
		parsed.RawQuery = ""
		parsed.Fragment = ""
		remote = parsed.String()
	}
	return redact.SensitiveText(remote)
}

func fetchGitRef(ctx context.Context, workspaceDir, ref string) (gitRefFetchMode, error) {
	return fetchGitRefWithArgs(ctx, workspaceDir, ref, nil)
}

func fetchGitRefShallow(ctx context.Context, workspaceDir, ref string) (gitRefFetchMode, error) {
	fetch := func(depth int, refspec string) error {
		return execGitContext(ctx, workspaceDir, "fetch", fmt.Sprintf("--depth=%d", depth), "origin", refspec)
	}
	if branch, ok := gitBranchNameFromRef(ctx, workspaceDir, ref); ok {
		refspec := fmt.Sprintf("+refs/heads/%s:refs/remotes/origin/%s", branch, branch)
		if err := fetch(1, refspec); err == nil {
			return gitRefFetchRemoteBranch, nil
		}
	}
	if err := fetch(1, ref); err == nil {
		return gitRefFetchDirect, nil
	}

	const maxFallbackDepth = 256
	allHeadsRefspec := "+refs/heads/*:refs/remotes/origin/*"
	if err := fetch(maxFallbackDepth, allHeadsRefspec); err != nil {
		return gitRefFetchDirect, fmt.Errorf("git fetch ref %q failed: %w", ref, err)
	}
	if isHexGitObjectID(ref) && remoteBranchesContainRef(ctx, workspaceDir, ref) {
		return gitRefFetchRemoteHeads, nil
	}
	return gitRefFetchDirect, fmt.Errorf("git ref %q is not within %d commits of a remote head", ref, maxFallbackDepth)
}

func fetchGitRefWithArgs(ctx context.Context, workspaceDir, ref string, fetchArgs []string) (gitRefFetchMode, error) {
	fetch := func(refspec string) error {
		args := append([]string{"fetch"}, fetchArgs...)
		args = append(args, "origin", refspec)
		return execGitContext(ctx, workspaceDir, args...)
	}
	if branch, ok := gitBranchNameFromRef(ctx, workspaceDir, ref); ok {
		refspec := fmt.Sprintf("+refs/heads/%s:refs/remotes/origin/%s", branch, branch)
		if err := fetch(refspec); err == nil {
			return gitRefFetchRemoteBranch, nil
		}
	}
	if err := fetch(ref); err == nil {
		return gitRefFetchDirect, nil
	}
	if err := fetch("+refs/heads/*:refs/remotes/origin/*"); err != nil {
		return gitRefFetchDirect, fmt.Errorf("git fetch ref %q failed: %w", ref, err)
	}
	return gitRefFetchRemoteHeads, nil
}

func checkoutGitRef(ctx context.Context, workspaceDir, ref string, fetchMode gitRefFetchMode) error {
	if fetchMode == gitRefFetchRemoteBranch || fetchMode == gitRefFetchRemoteHeads {
		if err := checkoutRemoteGitBranch(ctx, workspaceDir, ref); err == nil {
			return nil
		}
	}
	if fetchMode == gitRefFetchDirect {
		if err := execGitContext(ctx, workspaceDir, "checkout", "FETCH_HEAD"); err != nil {
			return fmt.Errorf("git checkout fetched ref %q failed: %w", ref, err)
		}
		return nil
	}

	if isHexGitObjectID(ref) && remoteBranchesContainRef(ctx, workspaceDir, ref) {
		if err := execGitContext(ctx, workspaceDir, "checkout", ref); err != nil {
			return fmt.Errorf("git checkout fetched commit ref %q failed: %w", ref, err)
		}
		return nil
	}
	return fmt.Errorf("git checkout ref %q failed", ref)
}

func gitBranchNameFromRef(ctx context.Context, workspaceDir, ref string) (string, bool) {
	branch := strings.TrimPrefix(ref, "refs/heads/")
	branch = strings.TrimPrefix(branch, "refs/remotes/origin/")
	branch = strings.TrimPrefix(branch, "origin/")
	if branch == "" || strings.HasPrefix(branch, "-") {
		return "", false
	}
	if _, err := execGitOutputContext(ctx, workspaceDir, "check-ref-format", "--branch", branch); err != nil {
		return "", false
	}
	return branch, true
}

func checkoutRemoteGitBranch(ctx context.Context, workspaceDir, ref string) error {
	branch, ok := gitBranchNameFromRef(ctx, workspaceDir, ref)
	if !ok {
		return fmt.Errorf("git ref %q is not a branch name", ref)
	}
	return execGitContext(ctx, workspaceDir, "checkout", "-B", branch, "origin/"+branch)
}

func isHexGitObjectID(ref string) bool {
	if len(ref) < 7 || len(ref) > 64 {
		return false
	}
	for _, r := range ref {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') {
			continue
		}
		return false
	}
	return true
}

func remoteBranchesContainRef(ctx context.Context, workspaceDir, ref string) bool {
	out, err := execGitOutputContext(ctx, workspaceDir, "branch", "-r", "--contains", ref)
	if err != nil {
		return false
	}
	for line := range strings.SplitSeq(out, "\n") {
		branch := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "*"))
		if strings.HasPrefix(branch, "origin/") {
			return true
		}
	}
	return false
}

func refreshReusedGitBranch(ctx context.Context, workspaceDir, branch string) error {
	branch = strings.TrimSpace(branch)
	refspec := fmt.Sprintf("+refs/heads/%s:refs/remotes/origin/%s", branch, branch)
	if err := execGitContext(ctx, workspaceDir, "fetch", "origin", refspec); err != nil {
		return fmt.Errorf("git fetch branch %q on reused workspace failed: %w", branch, err)
	}

	currentBranch, err := execGitOutputContext(ctx, workspaceDir, "branch", "--show-current")
	if err != nil {
		return fmt.Errorf("git inspect current branch on reused workspace failed: %w", err)
	}
	if currentBranch != strings.TrimSpace(branch) {
		if currentBranch == "" {
			fmt.Printf("Reused git repo is detached; fetched origin/%s without switching\n", branch)
		} else {
			fmt.Printf("Reused git repo remains on branch %q; fetched origin/%s without switching\n", currentBranch, branch)
		}
		return nil
	}

	if err := execGitContext(ctx, workspaceDir, "merge", "--ff-only", "FETCH_HEAD"); err != nil {
		return fmt.Errorf("git fast-forward branch %q on reused workspace failed: %w", branch, err)
	}
	return nil
}

func gitSafeDirectoryArgs(dir string, args ...string) []string {
	if strings.TrimSpace(dir) == "" {
		return args
	}

	safeDir := dir
	if absDir, err := filepath.Abs(dir); err == nil {
		safeDir = absDir
	}

	return append([]string{"-c", "safe.directory=" + safeDir, "-c", "core.hooksPath=/dev/null"}, args...)
}

func execGitOutputContext(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", gitSafeDirectoryArgs(dir, args...)...)
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
}

func execGitContext(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", gitSafeDirectoryArgs(dir, args...)...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
