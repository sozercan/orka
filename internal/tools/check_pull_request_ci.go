/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const checkPullRequestCIStatusNoChecks = "no_checks"

// CheckPullRequestCITool checks GitHub CI status for a pull request without merging it.
type CheckPullRequestCITool struct {
	k8sClient  client.Client
	apiBaseURL string // override for testing; empty uses https://api.github.com
}

// CheckPullRequestCIArgs are the arguments for the check_pull_request_ci tool.
type CheckPullRequestCIArgs struct {
	// TaskName is the child task whose workspace config has the repo and git credentials.
	TaskName string `json:"task_name,omitempty"`
	// RepoURL is an explicit GitHub repository URL. When provided with task context, it must match that task's repository scope.
	RepoURL string `json:"repo_url,omitempty"`
	// PRNumber is the GitHub PR number to inspect.
	PRNumber int `json:"pr_number"`
	// WaitTimeout is the optional maximum time to wait for pending checks (e.g. "30m").
	// When empty, the tool performs one immediate status check.
	WaitTimeout string `json:"wait_timeout,omitempty"`
	// PollInterval is the optional delay between polls while waiting (e.g. "30s").
	// Defaults to 30s when wait_timeout is set.
	PollInterval string `json:"poll_interval,omitempty"`
}

// CheckPullRequestCIResult is the result of checking pull request CI.
type CheckPullRequestCIResult struct {
	Status        string `json:"status"`
	PRNumber      int    `json:"pr_number"`
	HeadSHA       string `json:"head_sha,omitempty"`
	ChecksPassed  bool   `json:"checks_passed"`
	ChecksFailed  bool   `json:"checks_failed"`
	ChecksPending bool   `json:"checks_pending"`
	ChecksDetails string `json:"checks_details,omitempty"`
	WaitTimedOut  bool   `json:"wait_timed_out,omitempty"`
	Attempts      int    `json:"attempts,omitempty"`
	Message       string `json:"message"`
}

// NewCheckPullRequestCITool creates a new check_pull_request_ci tool.
func NewCheckPullRequestCITool(k8sClient client.Client) *CheckPullRequestCITool {
	return &CheckPullRequestCITool{
		k8sClient: k8sClient,
	}
}

// Name returns the tool name.
func (t *CheckPullRequestCITool) Name() string {
	return checkPullRequestCIToolName
}

// Description returns the tool description.
func (t *CheckPullRequestCITool) Description() string {
	return "Check GitHub CI status for a pull request without merging it. " +
		"Optionally waits for pending checks with bounded polling so callers do not need to loop manually."
}

// Parameters returns the JSON schema for tool parameters.
func (t *CheckPullRequestCITool) Parameters() json.RawMessage {
	schema := map[string]any{jsonSchemaTypeField: jsonSchemaTypeObject, jsonSchemaPropertiesField: map[string]any{taskNameField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Task whose workspace supplies the repository and read credentials. TokenReview API callers need a Task and its referenced credential; current Task context may supply the name."}, repoURLField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Optional GitHub repository URL within the selected Task's repository scope."}, githubPRNumberField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeInteger, jsonSchemaDescriptionField: "GitHub pull request number to inspect"}, "wait_timeout": map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Optional maximum time to wait for pending checks (for example '30m'). Empty means one immediate check"},
		"poll_interval": map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Optional delay between polls while waiting (for example '30s'). Defaults to '30s' when wait_timeout is set"},
	}, jsonSchemaRequiredField: []string{githubPRNumberField},
	}
	data, _ := json.Marshal(schema)
	return data
}

// Execute checks CI status for a pull request.
func (t *CheckPullRequestCITool) Execute(ctx context.Context, argsJSON json.RawMessage) (string, error) {
	var args CheckPullRequestCIArgs
	if err := json.Unmarshal(argsJSON, &args); err != nil {
		return "", fmt.Errorf("failed to parse arguments: %w", err)
	}

	if args.PRNumber == 0 {
		return "", fmt.Errorf("pr_number is required")
	}

	waitTimeout, pollInterval, err := parsePullRequestCIWaitConfig(args.WaitTimeout, args.PollInterval)
	if err != nil {
		return "", err
	}

	owner, repo, token, baseURL, err := resolveScopedReadRepoAndToken(ctx, t.k8sClient, args.TaskName, args.RepoURL, t.apiBaseURL)
	if err != nil {
		return "", err
	}

	result, err := waitForPullRequestCI(ctx, token, owner, repo, args.PRNumber, baseURL, waitTimeout, pollInterval)
	if err != nil {
		return "", err
	}
	return marshalCheckPullRequestCIResult(*result), nil
}

func parsePullRequestCIWaitConfig(waitTimeoutArg, pollIntervalArg string) (time.Duration, time.Duration, error) {
	var waitTimeout time.Duration
	var err error
	if waitTimeoutArg != "" {
		waitTimeout, err = time.ParseDuration(waitTimeoutArg)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid wait_timeout %q: %w", waitTimeoutArg, err)
		}
		if waitTimeout < 0 {
			return 0, 0, fmt.Errorf("wait_timeout must be non-negative")
		}
	}

	pollInterval := 30 * time.Second
	if pollIntervalArg != "" {
		pollInterval, err = time.ParseDuration(pollIntervalArg)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid poll_interval %q: %w", pollIntervalArg, err)
		}
	}
	if pollInterval <= 0 {
		return 0, 0, fmt.Errorf("poll_interval must be positive")
	}

	return waitTimeout, pollInterval, nil
}

func waitForPullRequestCI(
	ctx context.Context,
	token, owner, repo string,
	prNumber int,
	baseURL string,
	waitTimeout, pollInterval time.Duration,
) (*CheckPullRequestCIResult, error) {
	attempts := 0
	var lastPending *CheckPullRequestCIResult
	var lastTransientErr error
	deadline := time.Now().Add(waitTimeout)

	for {
		attempts++
		result, terminal, err := checkPullRequestCIOnce(ctx, token, owner, repo, prNumber, baseURL)
		if err != nil {
			if waitTimeout > 0 && isTransientHTTPError(err) {
				lastTransientErr = err
			} else {
				return nil, err
			}
		} else {
			result.Attempts = attempts
			if terminal || waitTimeout == 0 {
				return result, nil
			}
			lastPending = result
		}

		if waitTimeout == 0 {
			break
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		sleepFor := min(remaining, pollInterval)
		timer := time.NewTimer(sleepFor)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	if lastPending != nil {
		lastPending.Attempts = attempts
		lastPending.WaitTimedOut = true
		// Preserve no_checks as terminal-after-timeout: if the PR genuinely
		// has no GitHub Actions workflows registered after the full
		// waitTimeout window, surface that distinctly so the coordinator
		// can report status=no_checks rather than mis-claiming CI_PENDING.
		if lastPending.Status == checkPullRequestCIStatusNoChecks {
			lastPending.Message = fmt.Sprintf(
				"no CI checks have been registered for this pull request after %s; "+
					"treating as terminal no_checks (the repository likely has no GitHub Actions workflows configured for this PR)",
				waitTimeout,
			)
			return lastPending, nil
		}
		lastPending.Status = "pending"
		lastPending.ChecksPending = true
		lastPending.Message = fmt.Sprintf("CI_PENDING: timed out after %s waiting for CI checks to finish", waitTimeout)
		return lastPending, nil
	}

	message := fmt.Sprintf("timed out after %s waiting for CI status", waitTimeout)
	if lastTransientErr != nil {
		message = fmt.Sprintf("%s; last transient error: %v", message, lastTransientErr)
	}
	return &CheckPullRequestCIResult{
		Status:       "unknown",
		PRNumber:     prNumber,
		WaitTimedOut: true,
		Attempts:     attempts,
		Message:      message,
	}, nil
}

func checkPullRequestCIOnce(ctx context.Context, token, owner, repo string, prNumber int, baseURL string) (*CheckPullRequestCIResult, bool, error) {
	headSHA, state, merged, err := getGitHubPRDetails(ctx, token, owner, repo, prNumber, baseURL)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get PR details: %w", err)
	}

	result := &CheckPullRequestCIResult{
		Status:   "checked",
		PRNumber: prNumber,
		HeadSHA:  headSHA,
	}

	if merged {
		result.Status = mergedStatusString
		result.Message = "pull request is already merged"
		return result, true, nil
	}

	if state == githubPRStateClosed {
		result.Status = githubPRStateClosed
		result.Message = "pull request is closed"
		return result, true, nil
	}

	ciResult, err := checkCIStatusDetailed(ctx, token, owner, repo, headSHA, baseURL)
	if err != nil {
		if errors.Is(err, errNoCIChecksConfigured) {
			// On a freshly opened PR, GitHub Actions workflows take 10-60
			// seconds to register check_run objects. Returning a terminal
			// "no_checks" before that grace period makes the coordinator
			// declare the PR "shipped with no CI configured" when in
			// reality CI hasn't even started yet. Mark this as
			// non-terminal so waitForPullRequestCI keeps polling until
			// checks register OR the caller's wait_timeout elapses. If
			// the wait_timeout elapses with no_checks still true, the
			// final result will correctly say no_checks — but only after
			// we've given GitHub a real chance to schedule the workflow.
			result.Status = checkPullRequestCIStatusNoChecks
			result.Message = "no CI checks have been registered yet for this pull request (may still be queueing)"
			return result, false, nil
		}
		return nil, false, fmt.Errorf("failed to check CI status: %w", err)
	}

	result.ChecksPassed = ciResult.Passed
	result.ChecksFailed = ciResult.Failed
	result.ChecksPending = ciResult.Pending
	result.ChecksDetails = ciResult.Details

	switch {
	case ciResult.Passed:
		result.Status = passedStatusString
		result.Message = "all CI checks passed"
		return result, true, nil
	case ciResult.Failed:
		result.Status = failedStatusString
		result.Message = "one or more CI checks failed"
		return result, true, nil
	case ciResult.Pending:
		result.Status = "pending"
		result.Message = "one or more CI checks are still pending"
		return result, false, nil
	default:
		result.Status = "unknown"
		result.Message = "CI status could not be categorized"
		return result, true, nil
	}
}

func marshalCheckPullRequestCIResult(result CheckPullRequestCIResult) string {
	data, _ := json.Marshal(result)
	return string(data)
}
