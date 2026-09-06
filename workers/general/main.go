/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"

	"github.com/orka-agents/orka/internal/security"
	securityslices "github.com/orka-agents/orka/internal/security/slices"
	"github.com/orka-agents/orka/internal/workerenv"

	"github.com/orka-agents/orka/workers/common"
)

var (
	workspaceDir                  = "/workspace"
	setupGitCredentialsForGeneral = common.SetupGitCredentials
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(generalWorkerExitCode(err))
	}
}

func generalWorkerExitCode(err error) int {
	if errors.Is(err, errValidationCommandUnavailable) {
		return workerenv.RepositoryValidationUnavailableExitCode
	}
	return 1
}

func run() (err error) {
	if len(os.Args) > 1 && os.Args[1] == "--run-validation-network-sandbox" {
		return runValidationNetworkSandbox(os.Args[2:])
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	if len(os.Args) > 1 && os.Args[1] == "--prepare-workspace-only" {
		return prepareWorkspace(ctx)
	}
	if len(os.Args) > 1 && os.Args[1] == "--materialize-validation-command" {
		return materializeValidationCommand(os.Args[2:])
	}
	if len(os.Args) > 1 && os.Args[1] == "--wait-for-validation-network-access" {
		return waitForValidationNetworkAccess(ctx, os.Args[2:])
	}
	if len(os.Args) > 1 && os.Args[1] == "--wait-for-validation-network-policy" {
		return waitForValidationNetworkPolicy(ctx, os.Args[2:])
	}
	if len(os.Args) > 1 && os.Args[1] == "--security-mapper" {
		return runSecurityMapper(ctx)
	}

	baseEnv := workerenv.ParseBaseEnv(os.Getenv)
	taskName := baseEnv.TaskName
	taskNamespace := baseEnv.TaskNamespace
	eventRecorder := common.NewHTTPEventRecorderFromEnv()
	defer func() {
		if err != nil {
			recordGeneralWorkerFailed(eventRecorder, taskName, err)
			return
		}
		common.RecordEventWithTimeout(eventRecorder, "WorkerCompleted", 0,
			common.WithEventTaskName(taskName),
			common.WithEventSummary("General worker completed"),
		)
	}()

	transactionLogFields := workerenv.TransactionLogFields(
		baseEnv.TransactionID, baseEnv.TransactionProfile,
	)
	fmt.Printf("Worker general started task=%s/%s%s\n",
		taskNamespace, taskName, transactionLogFields)
	common.RecordEvent(ctx, eventRecorder, "WorkerStarted",
		common.WithEventTaskName(taskName),
		common.WithEventSummary("General worker started"),
	)

	workDir, err := prepareWorkspaceIfConfigured(ctx)
	if err != nil {
		return err
	}

	// Get command from arguments or environment
	var command []string
	if len(os.Args) > 1 {
		command = os.Args[1:]
	} else {
		cmdStr := os.Getenv(workerenv.Command)
		if cmdStr == "" {
			return fmt.Errorf("no command specified")
		}
		command = strings.Fields(cmdStr)
	}

	if len(command) == 0 {
		return fmt.Errorf("command cannot be empty")
	}

	// Execute the command and print output to stdout/stderr.
	// The controller captures pod logs and writes them to a result ConfigMap.
	var stdout, stderr bytes.Buffer

	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = os.Environ()
	if workDir != "" {
		cmd.Dir = workDir
	}

	err = cmd.Run()

	if stdout.Len() > 0 {
		fmt.Print(stdout.String())
	}
	if stderr.Len() > 0 {
		fmt.Fprint(os.Stderr, stderr.String())
	}

	output := stdout.String() + stderr.String()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if submitErr := submitResult(workDir, output); submitErr == nil {
				recordGeneralResultSubmitted(eventRecorder, taskName, len(output))
			}
			recordGeneralWorkerFailed(
				eventRecorder,
				taskName,
				fmt.Errorf("command exited with code %d: %w", exitErr.ExitCode(), err),
			)
			os.Exit(exitErr.ExitCode())
		}
		return err
	}

	if err := submitResult(workDir, output); err != nil {
		return err
	}
	recordGeneralResultSubmitted(eventRecorder, taskName, len(output))

	fmt.Printf("Task %s/%s completed successfully%s\n",
		taskNamespace, taskName, transactionLogFields)
	return nil
}

func recordGeneralResultSubmitted(recorder common.EventRecorder, taskName string, resultLength int) {
	common.RecordEventWithTimeout(recorder, "ResultSubmitted", 0,
		common.WithEventTaskName(taskName),
		common.WithEventSummary("General worker submitted result"),
		common.WithEventContent(generalEventContent(map[string]any{"resultLength": resultLength})),
	)
}

func recordGeneralWorkerFailed(recorder common.EventRecorder, taskName string, err error) {
	if err == nil {
		return
	}
	common.RecordEventWithTimeout(recorder, "WorkerFailed", 0,
		common.WithEventSeverity("error"),
		common.WithEventTaskName(taskName),
		common.WithEventSummary(err.Error()),
	)
}

func generalEventContent(value map[string]any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return data
}

func prepareWorkspaceIfConfigured(ctx context.Context) (string, error) {
	cfg, err := common.LoadWorkspaceConfig()
	if err != nil {
		return "", err
	}
	if cfg.GitRepo == "" {
		return "", nil
	}
	if cfg.SubPath != os.Getenv(workerenv.WorkspaceSubpath) {
		if err := os.Setenv(workerenv.WorkspaceSubpath, cfg.SubPath); err != nil {
			return "", err
		}
	}
	setupGitCredentialsForGeneral()
	if _, err := os.Stat(filepath.Join(workspaceDir, ".git")); err == nil {
		return workspaceRoot(), nil
	} else if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("stat workspace: %w", err)
	}
	if err := prepareWorkspace(ctx); err != nil {
		return "", err
	}
	return workspaceRoot(), nil
}

func prepareWorkspace(ctx context.Context) error {
	cfg, err := common.LoadWorkspaceConfig()
	if err != nil {
		return err
	}
	if cfg.GitRepo == "" {
		return nil
	}

	setupGitCredentialsForGeneral()
	if _, err := os.Stat(filepath.Join(workspaceDir, ".git")); os.IsNotExist(err) {
		if err := common.CloneRepo(ctx, cfg, workspaceDir); err != nil {
			return err
		}
	} else if err != nil {
		return fmt.Errorf("stat workspace: %w", err)
	}
	if err := common.PrepareWorkspace(workspaceDir); err != nil {
		return err
	}
	if err := common.PreparePullRequestReviewContext(workspaceDir, cfg); err != nil {
		return err
	}
	return common.EnsureWorkspaceArtifactsLink(workspaceDir)
}

func workspaceRoot() string {
	if subPath := os.Getenv(workerenv.WorkspaceSubpath); subPath != "" {
		return filepath.Join(workspaceDir, subPath)
	}
	return workspaceDir
}

func submitResult(workDir, output string) error {
	if os.Getenv(workerenv.ResultEndpoint) == "" && os.Getenv(workerenv.ControllerURL) == "" {
		return nil
	}
	resultDir := ""
	if workDir != "" {
		resultDir = workspaceDir
		if err := configurePublicationRemote(resultDir); err != nil {
			return err
		}
	}
	resultBytes, err := common.FinalizeResult(resultDir, output)
	if err != nil {
		return err
	}
	if err := common.SubmitResult(resultBytes); err != nil {
		return err
	}
	return common.UploadArtifacts()
}

func configurePublicationRemote(workDir string) error {
	publicationRepo := strings.TrimSpace(os.Getenv(workerenv.ForkRepo))
	if publicationRepo == "" || strings.TrimSpace(os.Getenv(workerenv.PushBranch)) == "" {
		return nil
	}
	command := exec.Command("git", "-C", workDir, "remote", "set-url", "origin", publicationRepo)
	if err := command.Run(); err != nil {
		return fmt.Errorf("configure publication Git remote: %w", err)
	}
	return nil
}

func runSecurityMapper(ctx context.Context) error {
	workDir, err := prepareWorkspaceIfConfigured(ctx)
	if err != nil {
		return err
	}
	if workDir == "" {
		return fmt.Errorf("security mapper requires a git workspace")
	}
	repositoryScan := strings.TrimSpace(os.Getenv(security.EnvRepositoryScanName))
	slices, err := securityslices.MapRepository(workDir, securityslices.MapperOptions{
		RepositoryScan: repositoryScan,
		SubPath:        os.Getenv(workerenv.WorkspaceSubpath),
	})
	if err != nil {
		return err
	}
	baseCommit := strings.TrimSpace(os.Getenv(security.EnvScanBaseCommit))
	headCommit := strings.TrimSpace(os.Getenv(security.EnvScanHeadCommit))
	changedFilesComputed, changedFiles, changedLineRanges, diffSummary, changedFilesError, resolvedHeadCommit :=
		changedFilesForSecurityScan(ctx, workDir, baseCommit, headCommit)
	if headCommit == "" {
		headCommit = resolvedHeadCommit
	}
	for i := range slices {
		contextSlice := slices[i]
		contextSlice.ChangedFiles = append([]string(nil), changedFiles...)
		contextSlice.ChangedLineRanges = append([]security.ChangedLineRange(nil), changedLineRanges...)
		_, manifest, err := security.BuildReviewContext(workDir, contextSlice, security.ReviewContextOptions{})
		if err != nil {
			return fmt.Errorf("build security review context for %s: %w", contextSlice.ID, err)
		}
		manifestData, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("marshal security review context for %s: %w", contextSlice.ID, err)
		}
		if err := common.WriteArtifactFile(security.ReviewContextArtifactName(contextSlice.ID), manifestData); err != nil {
			return err
		}
	}
	artifact := security.ReviewSlicesArtifact{
		SchemaVersion:        security.SchemaVersionReviewSlices,
		BaseCommit:           baseCommit,
		HeadCommit:           headCommit,
		ChangedFilesComputed: changedFilesComputed,
		ChangedFiles:         changedFiles,
		ChangedLineRanges:    changedLineRanges,
		DiffSummary:          diffSummary,
		ChangedFilesError:    changedFilesError,
		Slices:               slices,
	}
	data, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return err
	}
	if err := common.WriteArtifactFile(security.ArtifactSlices, data); err != nil {
		return err
	}
	output := fmt.Sprintf("security mapper wrote %d review slices\n", len(slices))
	fmt.Print(output)
	return submitResult(workDir, output)
}

func changedFilesForSecurityScan(
	ctx context.Context,
	workDir, baseCommit, headCommit string,
) (bool, []string, []security.ChangedLineRange, string, string, string) {
	if headCommit == "" {
		out, err := exec.CommandContext(ctx, "git", "-C", workDir, "rev-parse", "HEAD").CombinedOutput()
		if err == nil {
			headCommit = strings.TrimSpace(string(out))
		}
	}
	if baseCommit == "" || headCommit == "" {
		return false, nil, nil, "", "", headCommit
	}
	for _, commit := range []string{baseCommit, headCommit} {
		if !safeGitCommitID(commit) {
			return false, nil, nil, "", fmt.Sprintf("commit %q is not a hex SHA", commit), headCommit
		}
		if err := ensureCommitAvailableForDiff(ctx, workDir, commit); err != nil {
			return false, nil, nil, "", err.Error(), headCommit
		}
	}

	deletedOut, err := exec.CommandContext(ctx,
		"git", "-C", workDir,
		"diff", "--name-only", "--diff-filter=D", "--relative",
		baseCommit, headCommit, "--", ".",
	).CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(deletedOut))
		if message == "" {
			message = err.Error()
		}
		return false, nil, nil, "", message, headCommit
	}
	deletedFiles := safeChangedFileLines(deletedOut)
	if len(deletedFiles) > 0 {
		message := fmt.Sprintf(
			"changed-file selection disabled because deleted files require full review: %s",
			strings.Join(deletedFiles, ", "),
		)
		return false, nil, nil, "", message, headCommit
	}

	out, err := exec.CommandContext(ctx,
		"git", "-C", workDir,
		"diff", "--name-only", "--diff-filter=ACMRT", "--relative",
		baseCommit, headCommit, "--", ".",
	).CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(out))
		if message == "" {
			message = err.Error()
		}
		return false, nil, nil, "", message, headCommit
	}

	files := safeChangedFileLines(out)
	lineRanges, err := changedLineRangesForSecurityScan(ctx, workDir, baseCommit, headCommit)
	if err != nil {
		if errors.Is(err, errChangedDiffTooLarge) {
			diffSummary := fmt.Sprintf(
				"%d changed files; changed line ranges omitted because diff exceeded safety cap",
				len(files),
			)
			return true, files, nil, diffSummary, "", headCommit
		}
		diffSummary := fmt.Sprintf(
			"%d changed files; changed line ranges omitted because diff could not be parsed: %s",
			len(files), err,
		)
		return true, files, nil, diffSummary, "", headCommit
	}
	diffSummary := fmt.Sprintf("%d changed files; %d changed line ranges", len(files), len(lineRanges))
	return true, files, lineRanges, diffSummary, "", headCommit
}

const (
	maxChangedLineRangesForArtifact  = 2000
	maxChangedDiffBytesForLineRanges = 2 * 1024 * 1024
	maxChangedDiffLinesForLineRanges = 20000
)

var (
	errChangedDiffTooLarge           = errors.New("changed diff exceeds changed-line metadata safety cap")
	changedLineRangesForSecurityScan = defaultChangedLineRangesForSecurityScan
)

func defaultChangedLineRangesForSecurityScan(
	ctx context.Context,
	workDir, baseCommit, headCommit string,
) ([]security.ChangedLineRange, error) {
	cmd := exec.CommandContext(ctx,
		"git", "-C", workDir,
		"diff", "--unified=0", "--diff-filter=ACMRT", "--relative",
		baseCommit, headCommit, "--", ".",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	lineRanges, parseErr := parseChangedLineRangesFromUnifiedDiffReader(stdout)
	if parseErr != nil {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
		return nil, parseErr
	}
	waitErr := cmd.Wait()
	if waitErr != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = waitErr.Error()
		}
		return nil, fmt.Errorf("git diff changed line ranges: %s", message)
	}
	return lineRanges, nil
}

var unifiedDiffHunkRE = regexp.MustCompile(`^@@ -[0-9]+(?:,[0-9]+)? \+([0-9]+)(?:,([0-9]+))? @@`)

func parseChangedLineRangesFromUnifiedDiffReader(r io.Reader) ([]security.ChangedLineRange, error) {
	reader := bufio.NewReaderSize(r, 64*1024)
	currentPath := ""
	expectPlusHeader := false
	inHunk := false
	ranges := make([]security.ChangedLineRange, 0)
	atLineStart := true
	bytesRead := 0
	linesRead := 0
	for {
		chunk, err := reader.ReadSlice('\n')
		bytesRead += len(chunk)
		if bytesRead > maxChangedDiffBytesForLineRanges {
			return nil, errChangedDiffTooLarge
		}
		if len(chunk) > 0 && atLineStart {
			linesRead++
			if linesRead > maxChangedDiffLinesForLineRanges {
				return nil, errChangedDiffTooLarge
			}
			line := strings.TrimRight(string(chunk), "\r\n")
			switch {
			case strings.HasPrefix(line, "diff --git "):
				currentPath = ""
				expectPlusHeader = false
				inHunk = false
			case !inHunk && strings.HasPrefix(line, "--- "):
				expectPlusHeader = true
			case expectPlusHeader && strings.HasPrefix(line, "+++ "):
				pathValue := strings.TrimSpace(strings.TrimPrefix(line, "+++ "))
				currentPath = normalizeDiffPath(pathValue)
				if currentPath != "" && !security.SafeRepoPath(currentPath) {
					currentPath = ""
				}
				expectPlusHeader = false
			case strings.HasPrefix(line, "@@ "):
				inHunk = true
				expectPlusHeader = false
				if currentPath == "" {
					break
				}
				matches := unifiedDiffHunkRE.FindStringSubmatch(line)
				if len(matches) == 0 {
					return nil, fmt.Errorf("parse changed line ranges: unsupported hunk header %q", line)
				}
				start := atoiDiffNumber(matches[1])
				count := 1
				if matches[2] != "" {
					count = atoiDiffNumber(matches[2])
				}
				if len(ranges) >= maxChangedLineRangesForArtifact {
					return nil, errChangedDiffTooLarge
				}
				if count <= 0 {
					if start <= 0 {
						start = 1
					}
					count = 1
				}
				if start <= 0 {
					break
				}
				ranges = append(ranges, security.ChangedLineRange{Path: currentPath, StartLine: start, EndLine: start + count - 1})
			}
		}
		if err == nil {
			atLineStart = true
			continue
		}
		if err == bufio.ErrBufferFull {
			atLineStart = false
			continue
		}
		if err == io.EOF {
			break
		}
		return nil, err
	}
	return mergeChangedLineRanges(ranges), nil
}

func normalizeDiffPath(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	if value == "/dev/null" || value == "" {
		return ""
	}
	if strings.HasPrefix(value, "b/") || strings.HasPrefix(value, "a/") {
		value = value[2:]
	}
	return value
}

func atoiDiffNumber(value string) int {
	out := 0
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0
		}
		out = out*10 + int(ch-'0')
	}
	return out
}

func mergeChangedLineRanges(ranges []security.ChangedLineRange) []security.ChangedLineRange {
	if len(ranges) == 0 {
		return nil
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].Path != ranges[j].Path {
			return ranges[i].Path < ranges[j].Path
		}
		if ranges[i].StartLine != ranges[j].StartLine {
			return ranges[i].StartLine < ranges[j].StartLine
		}
		return ranges[i].EndLine < ranges[j].EndLine
	})
	out := make([]security.ChangedLineRange, 0, len(ranges))
	for _, lineRange := range ranges {
		if lineRange.Path == "" || lineRange.StartLine <= 0 || lineRange.EndLine < lineRange.StartLine {
			continue
		}
		if len(out) == 0 || out[len(out)-1].Path != lineRange.Path || lineRange.StartLine > out[len(out)-1].EndLine+1 {
			out = append(out, lineRange)
			continue
		}
		if lineRange.EndLine > out[len(out)-1].EndLine {
			out[len(out)-1].EndLine = lineRange.EndLine
		}
	}
	return out
}

func safeChangedFileLines(out []byte) []string {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	seen := make(map[string]struct{}, len(lines))
	files := make([]string, 0, len(lines))
	for _, line := range lines {
		file := strings.TrimSpace(strings.ReplaceAll(line, "\\", "/"))
		if file == "" || !security.SafeRepoPath(file) {
			continue
		}
		if _, ok := seen[file]; ok {
			continue
		}
		seen[file] = struct{}{}
		files = append(files, file)
	}
	sort.Strings(files)
	return files
}

func ensureCommitAvailableForDiff(ctx context.Context, workDir, commit string) error {
	if !safeGitCommitID(commit) {
		return fmt.Errorf("commit %q is not a hex SHA", commit)
	}
	if gitCommitAvailable(ctx, workDir, commit) {
		return nil
	}

	out, err := exec.CommandContext(
		ctx,
		"git",
		"-C",
		workDir,
		"fetch",
		"--no-tags",
		"--depth=1",
		"origin",
		commit,
	).CombinedOutput()
	if err == nil && gitCommitAvailable(ctx, workDir, commit) {
		return nil
	}
	firstMessage := strings.TrimSpace(string(out))
	if firstMessage == "" && err != nil {
		firstMessage = err.Error()
	}

	args := []string{"fetch", "--no-tags", "origin"}
	if isShallowGitRepository(ctx, workDir) {
		args = []string{"fetch", "--no-tags", "--unshallow", "origin"}
	}
	out, err = exec.CommandContext(ctx, "git", append([]string{"-C", workDir}, args...)...).CombinedOutput()
	if err == nil && gitCommitAvailable(ctx, workDir, commit) {
		return nil
	}
	message := strings.TrimSpace(string(out))
	if message == "" && err != nil {
		message = err.Error()
	}
	if firstMessage != "" && message != "" {
		message = firstMessage + "; " + message
	} else if message == "" {
		message = firstMessage
	}
	if message == "" {
		message = "commit is not available after fetching origin"
	}
	return fmt.Errorf("fetch commit for incremental diff: %s", message)
}

func gitCommitAvailable(ctx context.Context, workDir, commit string) bool {
	if strings.TrimSpace(commit) == "" {
		return false
	}
	err := exec.CommandContext(ctx, "git", "-C", workDir, "cat-file", "-e", commit+"^{commit}").Run()
	return err == nil
}

func safeGitCommitID(commit string) bool {
	commit = strings.TrimSpace(commit)
	if len(commit) < 7 || len(commit) > 64 {
		return false
	}
	for _, ch := range commit {
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') {
			continue
		}
		return false
	}
	return true
}

func isShallowGitRepository(ctx context.Context, workDir string) bool {
	out, err := exec.CommandContext(ctx, "git", "-C", workDir, "rev-parse", "--is-shallow-repository").CombinedOutput()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}
