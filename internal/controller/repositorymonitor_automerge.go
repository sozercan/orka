package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
)

const (
	repositoryMonitorActionAutomerge                     = "pr_automerge"
	repositoryMonitorAutomergeStateMerged                = "merged"
	repositoryMonitorAutomergeStateMergeReady            = "merge_ready"
	repositoryMonitorAutomergeStateBlocked               = "blocked"
	repositoryMonitorAutomergeStateFailed                = "failed"
	repositoryMonitorAutomergeStateStarted               = "started"
	repositoryMonitorAutomergeStatePending               = repositoryMonitorReviewTaskStatePending
	repositoryMonitorAutomergeReasonDisabled             = "automerge_disabled"
	repositoryMonitorAutomergeReasonCIPending            = "ci_pending"
	repositoryMonitorAutomergeReasonCICheckRetry         = "ci_check_error_retry"
	repositoryMonitorAutomergeReasonMergeabilityPending  = "mergeability_pending"
	repositoryMonitorAutomergeReasonValidationCheckRetry = "validation_check_error_retry"
	repositoryMonitorCommandIntentAutomerge              = "automerge"
	repositoryMonitorAutomergeGateEnv                    = "ORKA_REPOSITORY_MONITOR_AUTOMERGE_GATE"
	repositoryMonitorAutomergeMethodSquash               = "squash"
)

func (r *RepositoryMonitorReconciler) tryProcessPullRequestAutomergeCommand(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, command *store.CommandEvent, owner, repository string, pr repositoryMonitorPullRequest, item *store.MonitorItem) (bool, error) {
	if command.Intent != repositoryMonitorCommandIntentAutomerge {
		return false, nil
	}
	verdict, reason := r.repositoryMonitorAutomergeGate(ctx, monitor, command, pr, item)
	if verdict != repositoryMonitorIssueVerdictReady {
		if reason == repositoryMonitorAutomergeReasonCIPending || reason == repositoryMonitorAutomergeReasonCICheckRetry || reason == repositoryMonitorAutomergeReasonMergeabilityPending || reason == repositoryMonitorAutomergeReasonValidationCheckRetry {
			item.AutomergeState = repositoryMonitorAutomergeStatePending
			item.SkipReason = reason
			if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
				return true, err
			}
			return true, r.createRepositoryMonitorAutomergeRecord(ctx, monitor, command, item, repositoryMonitorAutomergeStatePending, "waiting for CI checks", map[string]any{"reason": reason})
		}
		preserveSuccess, err := r.terminalizeRepositoryMonitorAutomerge(ctx, monitor, *command, reason)
		if err != nil {
			return true, err
		}
		if preserveSuccess {
			return true, nil
		}
		item.AutomergeState = repositoryMonitorAutomergeStateBlocked
		item.SkipReason = reason
		if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
			return true, err
		}
		return true, r.createRepositoryMonitorAutomergeRecord(ctx, monitor, command, item, repositoryMonitorAutomergeStateBlocked, reason, map[string]any{"reason": reason})
	}
	if err := r.createRepositoryMonitorAutomergeRecord(ctx, monitor, command, item, repositoryMonitorAutomergeStateStarted, "merge attempt started", map[string]any{"headSHA": pr.HeadSHA}); err != nil {
		return true, err
	}
	method := repositoryMonitorAutomergeMethod(monitor)
	mutationID := "ghmut-" + repositoryMonitorShortHash(command.ID+"-merge")
	mutation := &store.GitHubMutationRecord{ID: mutationID, RunID: run.ID, CommandEventID: command.ID, Operation: "merge_pr", TargetKind: repositoryMonitorPullRequestKind, TargetNumber: pr.Number, TargetSHA: pr.HeadSHA, Reason: "automerge", Status: repositoryMonitorAutomergeStateStarted}
	existing, getErr := r.Store.GetGitHubMutationRecord(ctx, monitor.Namespace, mutationID)
	if getErr == nil {
		mutation = existing
		if mutation.Status != repositoryMonitorRunPhaseSucceeded {
			mutation.Status = repositoryMonitorAutomergeStateStarted
			mutation.Error = ""
			mutation.ExternalID = ""
			if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
				return true, err
			}
		}
	} else if errors.Is(getErr, store.ErrNotFound) {
		if err := r.recordRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
			return true, err
		}
	} else {
		return true, getErr
	}
	sha := mutation.ExternalID
	if mutation.Status != repositoryMonitorRunPhaseSucceeded {
		var err error
		sha, err = r.mergeRepositoryMonitorPullRequest(ctx, monitor, owner, repository, pr.Number, method, pr.HeadSHA)
		if err != nil {
			failureState := repositoryMonitorRunFailureState(err)
			retryable := repositoryMonitorFailedCommandRunRetryable("[" + failureState + "]")
			mutation.Status = repositoryMonitorRunPhaseFailed
			if retryable {
				mutation.Status = repositoryMonitorAutomergeStatePending
			}
			mutation.Error = err.Error()
			if auditErr := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); auditErr != nil {
				return true, fmt.Errorf("automerge failed: %w; additionally failed to update mutation audit: %v", err, auditErr)
			}
			if retryable {
				item.AutomergeState = repositoryMonitorAutomergeStatePending
				item.SkipReason = failureState
				if updateErr := r.Store.UpsertMonitorItem(ctx, item); updateErr != nil {
					return true, updateErr
				}
				if recordErr := r.createRepositoryMonitorAutomergeRecord(ctx, monitor, command, item, repositoryMonitorAutomergeStatePending, err.Error(), map[string]any{"mergeMethod": method, "error": err.Error(), "reason": failureState}); recordErr != nil {
					return true, recordErr
				}
				return true, err
			}
			item.AutomergeState = repositoryMonitorAutomergeStateFailed
			item.SkipReason = "automerge_failed"
			if updateErr := r.Store.UpsertMonitorItem(ctx, item); updateErr != nil {
				return true, updateErr
			}
			if recordErr := r.createRepositoryMonitorAutomergeRecord(ctx, monitor, command, item, repositoryMonitorAutomergeStateFailed, err.Error(), map[string]any{"mergeMethod": method, "error": err.Error()}); recordErr != nil {
				return true, recordErr
			}
			return true, err
		}
		mutation.Status = repositoryMonitorRunPhaseSucceeded
		mutation.Error = ""
		mutation.ExternalID = sha
		if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
			return true, err
		}
	}
	item.AutomergeState = repositoryMonitorAutomergeStateMerged
	item.SkipReason = ""
	item.State = repositoryMonitorAutomergeStateMerged
	if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
		return true, err
	}
	if err := r.createRepositoryMonitorAutomergeRecord(ctx, monitor, command, item, repositoryMonitorAutomergeStateMerged, "pull request merged", map[string]any{"mergeMethod": method, "mergeSHA": sha}); err != nil {
		return true, err
	}
	return true, r.createMonitorEvent(ctx, monitor, run.ID, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "automerge_succeeded", fmt.Sprintf("Pull request #%d automerged", pr.Number), map[string]any{"mergeSHA": sha, "mergeMethod": method})
}

func (r *RepositoryMonitorReconciler) reconcileRepositoryMonitorCompletedAutomerge(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, pullRequests []repositoryMonitorPullRequest) (bool, error) {
	if run == nil || strings.TrimSpace(run.CommandEventID) == "" || run.TargetNumber == 0 {
		return false, nil
	}
	command, err := r.Store.GetCommandEvent(ctx, monitor.Namespace, run.CommandEventID)
	if err != nil {
		return false, err
	}
	if command.Intent != repositoryMonitorCommandIntentAutomerge {
		return false, nil
	}
	var pr *repositoryMonitorPullRequest
	for i := range pullRequests {
		if pullRequests[i].Number == run.TargetNumber {
			pr = &pullRequests[i]
			break
		}
	}
	if pr == nil {
		return false, nil
	}
	mutationID := "ghmut-" + repositoryMonitorShortHash(command.ID+"-merge")
	mutation, err := r.Store.GetGitHubMutationRecord(ctx, monitor.Namespace, mutationID)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if mutation.Status != repositoryMonitorRunPhaseSucceeded && !pr.Merged {
		return false, nil
	}
	targetSHA := firstNonEmptyIssueAction(strings.TrimSpace(run.TargetSHA), strings.TrimSpace(mutation.TargetSHA))
	if targetSHA == "" || pr.HeadSHA != targetSHA || (strings.TrimSpace(mutation.TargetSHA) != "" && pr.HeadSHA != strings.TrimSpace(mutation.TargetSHA)) {
		return false, nil
	}
	if mutation.Status != repositoryMonitorRunPhaseSucceeded {
		mutation.Status = repositoryMonitorRunPhaseSucceeded
		mutation.Error = ""
		mutation.ExternalID = firstNonEmptyIssueAction(pr.MergeCommitSHA, mutation.ExternalID)
		if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
			return false, err
		}
	}
	existing, getErr := r.Store.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, fmt.Sprintf("%d", pr.Number))
	if getErr != nil && !errors.Is(getErr, store.ErrNotFound) {
		return false, getErr
	}
	item := repositoryMonitorItemFromPullRequest(monitor, *pr, existing)
	item.AutomergeState = repositoryMonitorAutomergeStateMerged
	item.SkipReason = ""
	item.State = repositoryMonitorAutomergeStateMerged
	if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
		return false, err
	}
	if err := r.createRepositoryMonitorAutomergeRecord(ctx, monitor, command, item, repositoryMonitorAutomergeStateMerged, "pull request merged", map[string]any{"mergeSHA": mutation.ExternalID, "recovered": true}); err != nil {
		return false, err
	}
	if err := r.createMonitorEvent(ctx, monitor, run.ID, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "automerge_succeeded", fmt.Sprintf("Pull request #%d automerge state recovered", pr.Number), map[string]any{"mergeSHA": mutation.ExternalID, "recovered": true}); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RepositoryMonitorReconciler) repositoryMonitorAutomergeGate(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, command *store.CommandEvent, pr repositoryMonitorPullRequest, item *store.MonitorItem) (string, string) {
	switch {
	case !monitor.Spec.Automerge.Enabled:
		return repositoryMonitorIssuePhaseBlocked, repositoryMonitorAutomergeReasonDisabled
	case repositoryMonitorAutomergeRequiresGlobalGate(monitor) && !strings.EqualFold(os.Getenv(repositoryMonitorAutomergeGateEnv), "true"):
		return repositoryMonitorIssuePhaseBlocked, "global_merge_gate_disabled"
	case !repositoryMonitorAutomergeActorAllowed(monitor, command.Permission):
		return repositoryMonitorIssuePhaseBlocked, "actor_permission_insufficient"
	case command.HeadSHA == "" || command.HeadSHA != pr.HeadSHA:
		return repositoryMonitorIssuePhaseBlocked, repositoryMonitorReviewSkipReasonStaleHead
	case repositoryMonitorBlockedLabel(monitor.Spec, pr.Labels) != "":
		return repositoryMonitorIssuePhaseBlocked, repositoryMonitorSkipReasonBlockedLabel
	case item.LastVerdict != repositoryMonitorReviewVerdictPassed || item.LastReviewedHeadSHA != pr.HeadSHA:
		return repositoryMonitorIssuePhaseBlocked, "orka_review_not_passed"
	}
	validationAllowed, err := r.repositoryMonitorValidationAllowsAutomerge(ctx, monitor, item, pr.HeadSHA)
	if err != nil {
		return repositoryMonitorIssuePhaseBlocked, repositoryMonitorAutomergeReasonValidationCheckRetry
	}
	if !validationAllowed {
		return repositoryMonitorIssuePhaseBlocked, "orka_validation_not_passed"
	}
	switch {
	case repositoryMonitorAutomergeRepairStateBlocks(item.RepairState):
		return repositoryMonitorIssuePhaseBlocked, "active_or_failed_repair_state"
	case strings.TrimSpace(pr.MergeableState) == "" || strings.EqualFold(pr.MergeableState, "unknown"):
		return repositoryMonitorIssuePhaseBlocked, repositoryMonitorAutomergeReasonMergeabilityPending
	case !repositoryMonitorAutomergeMergeableStateCanCheckCI(pr.MergeableState):
		return repositoryMonitorIssuePhaseBlocked, "pull_request_not_mergeable"
	}
	ci, err := r.repositoryMonitorCheckCI(ctx, monitor, pr.HeadSHA)
	if err != nil {
		return repositoryMonitorIssuePhaseBlocked, repositoryMonitorAutomergeReasonCICheckRetry
	}
	if !ci.passed {
		return repositoryMonitorIssuePhaseBlocked, ci.reason
	}
	return repositoryMonitorIssueVerdictReady, ""
}

func (r *RepositoryMonitorReconciler) repositoryMonitorValidationAllowsAutomerge(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, headSHA string) (bool, error) {
	if r.Store == nil || item == nil || strings.TrimSpace(item.LastReviewID) == "" {
		return false, nil
	}
	record, err := r.Store.GetReviewRecord(ctx, monitor.Namespace, item.LastReviewID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("load automerge validation review record: %w", err)
	}
	if record.MonitorName != monitor.Name || record.Kind != repositoryMonitorPullRequestKind ||
		record.Number != item.Number || record.HeadSHA != headSHA || record.Verdict != repositoryMonitorReviewVerdictPassed ||
		!repositoryMonitorReviewRecordAllowsAutomerge(monitor, record) {
		return false, nil
	}
	return true, nil
}

func repositoryMonitorAutomergeMergeableStateCanCheckCI(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "clean", "unstable":
		return true
	default:
		return false
	}
}

func repositoryMonitorAutomergeRepairStateBlocks(state string) bool {
	switch strings.TrimSpace(state) {
	case "", repositoryMonitorRepairPhaseSucceeded:
		return false
	default:
		return true
	}
}

func repositoryMonitorAutomergeRequiresGlobalGate(monitor *corev1alpha1.RepositoryMonitor) bool {
	return monitor.Spec.Automerge.RequireGlobalMergeGate == nil || *monitor.Spec.Automerge.RequireGlobalMergeGate
}

func repositoryMonitorAutomergeActorAllowed(monitor *corev1alpha1.RepositoryMonitor, permission string) bool {
	if strings.EqualFold(strings.TrimSpace(permission), "orka:monitors:write") {
		return true
	}
	permission = strings.ToLower(strings.TrimSpace(permission))
	policy := monitor.Spec.Policy.AllowedRepositoryPermissions
	if len(policy) > 0 && !repositoryMonitorAutomergePermissionInList(permission, policy) {
		return false
	}
	return repositoryMonitorAutomergePermissionInList(permission, repositoryMonitorAutomergePermissionsAtLeast(monitor.Spec.Triggers.GitHub.Labels.RequireActorPermission))
}

func repositoryMonitorAutomergePermissionsAtLeast(minimum string) []string {
	switch strings.ToLower(strings.TrimSpace(minimum)) {
	case "admin":
		return []string{"admin"}
	case "maintain":
		return []string{"maintain", "admin"}
	default:
		return []string{"write", "maintain", "admin"}
	}
}

func repositoryMonitorAutomergePermissionInList(permission string, allowed []string) bool {
	for _, candidate := range allowed {
		if strings.EqualFold(strings.TrimSpace(candidate), permission) {
			return true
		}
	}
	return false
}

func repositoryMonitorAutomergeMethod(monitor *corev1alpha1.RepositoryMonitor) string {
	allowed := monitor.Spec.Automerge.AllowedMergeMethods
	if len(allowed) == 0 {
		return repositoryMonitorAutomergeMethodSquash
	}
	for _, method := range allowed {
		switch strings.TrimSpace(method) {
		case repositoryMonitorAutomergeMethodSquash, "merge", "rebase":
			return strings.TrimSpace(method)
		}
	}
	return repositoryMonitorAutomergeMethodSquash
}

type repositoryMonitorCIResult struct {
	passed bool
	reason string
}

func (r *RepositoryMonitorReconciler) repositoryMonitorCheckCI(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, sha string) (repositoryMonitorCIResult, error) {
	token, err := r.repositoryMonitorGitHubToken(ctx, monitor)
	if err != nil {
		return repositoryMonitorCIResult{}, err
	}
	owner, repo, err := security.ParseGitHubRepositoryURL(monitor.Spec.RepoURL)
	if err != nil {
		return repositoryMonitorCIResult{}, err
	}
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	total := -1
	var checks []struct {
		Name       string `json:"name"`
		Status     string `json:"status"`
		Conclusion string `json:"conclusion"`
	}
	for page := 1; ; page++ {
		endpoint := fmt.Sprintf("%s/repos/%s/%s/commits/%s/check-runs?per_page=100&page=%d", baseURL, url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(sha), page)
		var response struct {
			TotalCount int `json:"total_count"`
			CheckRuns  []struct {
				Name       string `json:"name"`
				Status     string `json:"status"`
				Conclusion string `json:"conclusion"`
			} `json:"check_runs"`
		}
		if err := r.fetchRepositoryMonitorAuthorizedJSON(ctx, endpoint, token, &response); err != nil {
			return repositoryMonitorCIResult{}, err
		}
		if total < 0 {
			total = response.TotalCount
		}
		checks = append(checks, response.CheckRuns...)
		if len(checks) >= total || len(response.CheckRuns) == 0 {
			break
		}
	}
	if total <= 0 {
		return r.repositoryMonitorCheckCommitStatus(ctx, baseURL, owner, repo, token, sha)
	}
	if len(checks) < total {
		return repositoryMonitorCIResult{reason: "ci_checks_incomplete"}, nil
	}
	var pending, failed []string
	for _, check := range checks {
		if check.Status != "completed" {
			pending = append(pending, fmt.Sprintf("%s:%s/%s", check.Name, check.Status, check.Conclusion))
			continue
		}
		if !repositoryMonitorCheckRunConclusionPassing(check.Conclusion) {
			failed = append(failed, fmt.Sprintf("%s:%s/%s", check.Name, check.Status, check.Conclusion))
		}
	}
	if len(failed) > 0 {
		return repositoryMonitorCIResult{reason: "ci_not_green"}, nil
	}
	if len(pending) > 0 {
		return repositoryMonitorCIResult{reason: repositoryMonitorAutomergeReasonCIPending}, nil
	}
	status, err := r.repositoryMonitorCheckCommitStatus(ctx, baseURL, owner, repo, token, sha)
	if err != nil {
		return repositoryMonitorCIResult{}, err
	}
	if status.reason == "ci_checks_missing" {
		return repositoryMonitorCIResult{passed: true}, nil
	}
	return status, nil
}

func repositoryMonitorCheckRunConclusionPassing(conclusion string) bool {
	switch strings.ToLower(strings.TrimSpace(conclusion)) {
	case "success", "neutral", "skipped":
		return true
	default:
		return false
	}
}

func (r *RepositoryMonitorReconciler) repositoryMonitorCheckCommitStatus(ctx context.Context, baseURL, owner, repo, token, sha string) (repositoryMonitorCIResult, error) {
	endpoint := fmt.Sprintf("%s/repos/%s/%s/commits/%s/status", baseURL, url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(sha))
	var response struct {
		State    string `json:"state"`
		Statuses []struct {
			Context string `json:"context"`
			State   string `json:"state"`
		} `json:"statuses"`
	}
	if err := r.fetchRepositoryMonitorAuthorizedJSON(ctx, endpoint, token, &response); err != nil {
		return repositoryMonitorCIResult{}, err
	}
	if len(response.Statuses) == 0 {
		return repositoryMonitorCIResult{reason: "ci_checks_missing"}, nil
	}
	switch response.State {
	case "success":
		return repositoryMonitorCIResult{passed: true}, nil
	case repositoryMonitorAutomergeStatePending:
		return repositoryMonitorCIResult{reason: repositoryMonitorAutomergeReasonCIPending}, nil
	default:
		return repositoryMonitorCIResult{reason: "ci_not_green"}, nil
	}
}

func (r *RepositoryMonitorReconciler) fetchRepositoryMonitorAuthorizedJSON(ctx context.Context, endpoint, token string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", strings.Join([]string{"Bearer", token}, " "))
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	httpClient := r.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(resp.Body, repositoryMonitorGitHubResponseLimit))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &repositoryMonitorGitHubAPIError{Operation: "automerge gate", StatusCode: resp.StatusCode, Body: string(data)}
	}
	return json.Unmarshal(data, out)
}

func (r *RepositoryMonitorReconciler) mergeRepositoryMonitorPullRequest(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, owner, repository string, number int64, method, expectedSHA string) (string, error) {
	token, err := r.repositoryMonitorForgeToken(ctx, monitor)
	if err != nil {
		return "", err
	}
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	payload, _ := json.Marshal(map[string]any{"merge_method": method, "sha": expectedSHA})
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("%s/repos/%s/%s/pulls/%d/merge", baseURL, url.PathEscape(owner), url.PathEscape(repository), number), bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", strings.Join([]string{"Bearer", token}, " "))
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	httpClient := r.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(resp.Body, repositoryMonitorGitHubResponseLimit))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &repositoryMonitorGitHubAPIError{Operation: "automerge", StatusCode: resp.StatusCode, Body: string(data)}
	}
	var parsed struct {
		SHA string `json:"sha"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return "", err
	}
	return parsed.SHA, nil
}

func (r *RepositoryMonitorReconciler) createRepositoryMonitorAutomergeRecord(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, command *store.CommandEvent, item *store.MonitorItem, verdict, summary string, payload map[string]any) error {
	if payload == nil {
		payload = map[string]any{}
	}
	payload["commandEventID"] = command.ID
	payload["headSHA"] = command.HeadSHA
	payloadJSON, _ := json.Marshal(payload)
	record := &store.ActionRecord{
		ID:                "act-" + repositoryMonitorShortHash(command.ID+"-automerge-"+verdict),
		MonitorNamespace:  monitor.Namespace,
		MonitorName:       monitor.Name,
		Kind:              repositoryMonitorPullRequestKind,
		Number:            item.Number,
		ActionKind:        repositoryMonitorActionAutomerge,
		HeadSHA:           command.HeadSHA,
		CommandEventID:    command.ID,
		MonitorGeneration: monitor.Generation,
		Verdict:           verdict,
		Summary:           boundedString(summary, repositoryMonitorReviewTextMaxRunes),
		PayloadJSON:       string(payloadJSON),
		CreatedAt:         time.Now(),
	}
	if err := r.Store.CreateActionRecord(ctx, record); err != nil && !strings.Contains(strings.ToLower(err.Error()), "constraint") {
		return err
	}
	status := repositoryMonitorWorkActionStatusSucceeded
	switch verdict {
	case repositoryMonitorAutomergeStateBlocked:
		status = repositoryMonitorWorkActionStatusBlocked
	case repositoryMonitorAutomergeStateFailed:
		status = repositoryMonitorWorkActionStatusFailed
	case repositoryMonitorAutomergeStateStarted, repositoryMonitorAutomergeStatePending:
		status = repositoryMonitorWorkActionStatusRunning
	}
	workReason := strings.TrimSpace(summary)
	return r.recordRepositoryMonitorWorkActionState(ctx, monitor, nil, command, repositoryMonitorPullRequestKind, item.Number, command.HeadSHA, "", repositoryMonitorActionAutomerge, status, verdict, "", workReason)
}
