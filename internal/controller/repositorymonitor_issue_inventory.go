package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
)

const (
	repositoryMonitorIssueKind = "issue"

	repositoryMonitorIssuePhaseDiscovered = "discovered"
	repositoryMonitorIssuePhaseBlocked    = "blocked"

	repositoryMonitorSkipReasonBlockedLabel = "blocked_label"
	repositoryMonitorSkipReasonExcluded     = "excluded_label"
	repositoryMonitorSkipReasonMissingLabel = "missing_required_label"
	repositoryMonitorSkipReasonPullRequest  = "pull_request_issue"
)

type repositoryMonitorIssue struct {
	Number    int64
	Title     string
	Body      string
	Author    string
	State     string
	HTMLURL   string
	Labels    []string
	UpdatedAt time.Time
	IsPR      bool
}

func (r *RepositoryMonitorReconciler) processRepositoryMonitorInventoryRun(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, owner, repository string) (int, int, int, error) {
	if err := validateRepositoryMonitorRunTargetKind(run); err != nil {
		return 0, 0, 0, err
	}
	targetKind := strings.TrimSpace(run.TargetKind)
	if targetKind == repositoryMonitorIssueKind {
		return r.processIssueInventoryRun(ctx, monitor, run, owner, repository)
	}
	if targetKind == repositoryMonitorPullRequestKind {
		return r.processPullRequestInventoryRun(ctx, monitor, run, owner, repository)
	}
	if targetKind == "" && (run.TargetNumber != 0 || strings.TrimSpace(run.TargetSHA) != "") {
		if repositoryMonitorPullRequestsEnabled(monitor.Spec) {
			return r.processPullRequestInventoryRun(ctx, monitor, run, owner, repository)
		}
		return r.processIssueInventoryRun(ctx, monitor, run, owner, repository)
	}

	selected, created, skipped := 0, 0, 0
	if repositoryMonitorPullRequestsEnabled(monitor.Spec) {
		prSelected, prCreated, prSkipped, err := r.processPullRequestInventoryRun(ctx, monitor, run, owner, repository)
		if err != nil {
			return selected + prSelected, created + prCreated, skipped + prSkipped, err
		}
		selected += prSelected
		created += prCreated
		skipped += prSkipped
	}
	if monitor.Spec.Targets.Issues.Enabled {
		issueSelected, issueCreated, issueSkipped, err := r.processIssueInventoryRun(ctx, monitor, run, owner, repository)
		if err != nil {
			return selected + issueSelected, created + issueCreated, skipped + issueSkipped, err
		}
		selected += issueSelected
		created += issueCreated
		skipped += issueSkipped
	}
	if selected == 0 && created == 0 && skipped == 0 {
		return 0, 0, 0, r.createMonitorEvent(ctx, monitor, run.ID, "", 0, "", "inventory_skipped", "Repository monitor has no enabled inventory targets", nil)
	}
	return selected, created, skipped, nil
}

//nolint:gocyclo // Issue inventory keeps command-target terminalization and inventory policy explicit.
func (r *RepositoryMonitorReconciler) processIssueInventoryRun(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, owner, repository string) (int, int, int, error) {
	if !monitor.Spec.Targets.Issues.Enabled {
		return 0, 0, 0, r.createMonitorEvent(ctx, monitor, run.ID, repositoryMonitorIssueKind, 0, "", "inventory_skipped", "Issue monitoring is disabled", nil)
	}
	token, err := r.repositoryMonitorGitHubToken(ctx, monitor)
	if err != nil {
		return 0, 0, 0, err
	}
	issues, err := r.listRepositoryMonitorIssuesForRun(ctx, owner, repository, token, run)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(issues) == 0 && run != nil && strings.TrimSpace(run.CommandEventID) != "" && strings.TrimSpace(run.TargetKind) == repositoryMonitorIssueKind {
		return 0, 0, 1, r.blockRepositoryMonitorIssueTargetCommand(ctx, monitor, run, "target_not_open")
	}
	seenIssueKeys := repositoryMonitorIssueKeys(issues)
	issues = filterRepositoryMonitorTargetIssues(issues, run)
	slices.SortFunc(issues, func(a, b repositoryMonitorIssue) int {
		if a.Number < b.Number {
			return -1
		}
		if a.Number > b.Number {
			return 1
		}
		return 0
	})

	maxPerRun := repositoryMonitorMaxIssuesPerRun(monitor.Spec)
	selected := 0
	createdTasks := 0
	skipped := 0
	for _, issue := range issues {
		if issue.IsPR {
			skipped++
			if strings.TrimSpace(run.CommandEventID) != "" {
				if err := r.blockRepositoryMonitorIssueTargetCommand(ctx, monitor, run, repositoryMonitorSkipReasonPullRequest); err != nil {
					return selected, createdTasks, skipped, err
				}
			}
			continue
		}
		existing, err := r.Store.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorIssueKind, fmt.Sprintf("%d", issue.Number))
		if err != nil && !errorsIsStoreNotFound(err) {
			return selected, createdTasks, skipped, err
		}
		item := repositoryMonitorItemFromIssue(monitor, issue, existing)
		skipReason := ""
		if strings.TrimSpace(run.CommandEventID) != "" {
			skipReason = repositoryMonitorIssueCommandSkipReason(monitor.Spec, issue)
		} else {
			skipReason = repositoryMonitorIssueSkipReason(monitor.Spec, issue, selected, maxPerRun)
			if skipReason == repositoryMonitorSkipReasonOverLimit &&
				repositoryMonitorIssueRetainWorkflowUnderRunLimit(item, existing) {
				// The per-run cap bounds newly selected issues. An issue already
				// past discovery keeps its recorded workflow even when its current
				// content produces a new snapshot digest. Plan approval, an open
				// PR, and verdict bookkeeping stay intact, and the retained item
				// does not consume a selection slot or count as selected.
				if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
					return selected, createdTasks, skipped, err
				}
				if err := r.createMonitorEvent(ctx, monitor, run.ID, repositoryMonitorIssueKind, issue.Number, item.SnapshotDigest, "item_retained", fmt.Sprintf("Issue #%d retained past the run limit with its workflow intact", issue.Number), nil); err != nil {
					return selected, createdTasks, skipped, err
				}
				continue
			}
		}
		if skipReason != "" {
			skipped++
			item.LastVerdict = repositoryMonitorVerdictSkipped
			item.SkipReason = skipReason
			item.WorkflowPhase = repositoryMonitorIssuePhaseBlocked
			if strings.TrimSpace(run.CommandEventID) != "" {
				command, commandErr := r.Store.GetCommandEvent(ctx, monitor.Namespace, run.CommandEventID)
				if commandErr != nil {
					return selected, createdTasks, skipped, commandErr
				}
				if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorIssueKind, item.Number, "", item.SnapshotDigest, repositoryMonitorCommandActionKind(command.Intent), repositoryMonitorWorkActionStatusBlocked, repositoryMonitorIssuePhaseBlocked, "", skipReason); err != nil {
					return selected, createdTasks, skipped, err
				}
			}
			if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
				return selected, createdTasks, skipped, err
			}
			if err := r.createMonitorEvent(ctx, monitor, run.ID, repositoryMonitorIssueKind, issue.Number, item.SnapshotDigest, "item_skipped", fmt.Sprintf("Issue #%d skipped: %s", issue.Number, skipReason), map[string]any{"reason": skipReason, "labels": issue.Labels}); err != nil {
				return selected, createdTasks, skipped, err
			}
			continue
		}
		created, err := r.processIssueCommandRun(ctx, monitor, run, item, owner, repository)
		if err != nil {
			return selected, createdTasks, skipped, err
		}
		createdTasks += created
		if item.WorkflowPhase == repositoryMonitorIssuePhaseBlocked && !repositoryMonitorIssueInventoryBlockCanClear(item.SkipReason) {
			skipped++
			if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
				return selected, createdTasks, skipped, err
			}
			continue
		}
		selected++
		item.LastVerdict = ""
		item.SkipReason = ""
		if item.WorkflowPhase == "" || item.WorkflowPhase == repositoryMonitorIssuePhaseBlocked {
			item.WorkflowPhase = repositoryMonitorIssuePhaseDiscovered
		}
		if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
			return selected, createdTasks, skipped, err
		}
		if err := r.createMonitorEvent(ctx, monitor, run.ID, repositoryMonitorIssueKind, issue.Number, item.SnapshotDigest, "item_selected", fmt.Sprintf("Issue #%d recorded in inventory", issue.Number), nil); err != nil {
			return selected, createdTasks, skipped, err
		}
	}
	if repositoryMonitorRunCoversFullInventory(run) {
		if err := r.retireMissingRepositoryMonitorIssues(ctx, monitor, run, seenIssueKeys); err != nil {
			return selected, createdTasks, skipped, err
		}
	}
	return selected, createdTasks, skipped, nil
}

func (r *RepositoryMonitorReconciler) blockRepositoryMonitorIssueTargetCommand(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, reason string) error {
	command, err := r.Store.GetCommandEvent(ctx, monitor.Namespace, run.CommandEventID)
	if err != nil {
		return err
	}
	return r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorIssueKind, run.TargetNumber, "", command.IssueSnapshotDigest, repositoryMonitorCommandActionKind(command.Intent), repositoryMonitorWorkActionStatusBlocked, repositoryMonitorIssuePhaseBlocked, "", reason)
}

func repositoryMonitorIssueKeys(issues []repositoryMonitorIssue) map[string]struct{} {
	keys := make(map[string]struct{}, len(issues))
	for _, issue := range issues {
		if issue.IsPR {
			continue
		}
		keys[fmt.Sprintf("%d", issue.Number)] = struct{}{}
	}
	return keys
}

func (r *RepositoryMonitorReconciler) listRepositoryMonitorIssueItems(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) ([]store.MonitorItem, error) {
	var allItems []store.MonitorItem
	cursor := ""
	for {
		items, next, err := r.Store.ListMonitorItems(ctx, store.MonitorItemFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, Kind: repositoryMonitorIssueKind, Limit: 200, Cursor: cursor})
		if err != nil {
			return nil, err
		}
		allItems = append(allItems, items...)
		if next == "" {
			return allItems, nil
		}
		cursor = next
	}
}

func (r *RepositoryMonitorReconciler) retireMissingRepositoryMonitorIssues(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, seenKeys map[string]struct{}) error {
	items, err := r.listRepositoryMonitorIssueItems(ctx, monitor)
	if err != nil {
		return err
	}
	for i := range items {
		item := items[i]
		if _, ok := seenKeys[item.ItemKey]; ok {
			continue
		}
		if item.State == repositoryMonitorItemStateOutOfScope && item.SkipReason == repositoryMonitorSkipReasonMissing {
			continue
		}
		item.State = repositoryMonitorItemStateOutOfScope
		item.LastVerdict = repositoryMonitorVerdictSkipped
		item.WorkflowPhase = repositoryMonitorIssuePhaseBlocked
		item.SkipReason = repositoryMonitorSkipReasonMissing
		if err := r.Store.UpsertMonitorItem(ctx, &item); err != nil {
			return err
		}
		if err := r.createMonitorEvent(ctx, monitor, run.ID, repositoryMonitorIssueKind, item.Number, item.SnapshotDigest, "item_retired", fmt.Sprintf("Issue #%d is no longer in the open issue inventory", item.Number), map[string]any{"reason": repositoryMonitorSkipReasonMissing, "state": item.State}); err != nil {
			return err
		}
	}
	return nil
}

// repositoryMonitorIssueWorkflowRetainedUnderRunLimit reports whether an
// existing item carries workflow state that a background run's selection
// cap must leave intact.
func repositoryMonitorIssueWorkflowRetainedUnderRunLimit(existing *store.MonitorItem) bool {
	if existing == nil {
		return false
	}
	switch strings.TrimSpace(existing.WorkflowPhase) {
	case "", repositoryMonitorIssuePhaseDiscovered, repositoryMonitorIssuePhaseBlocked:
		return false
	default:
		return true
	}
}

func repositoryMonitorIssueRetainWorkflowUnderRunLimit(item, existing *store.MonitorItem) bool {
	if item == nil || !repositoryMonitorIssueWorkflowRetainedUnderRunLimit(existing) {
		return false
	}
	item.WorkflowPhase = existing.WorkflowPhase
	item.LastActionID = existing.LastActionID
	item.LastActionKind = existing.LastActionKind
	item.LastActionTaskName = existing.LastActionTaskName
	item.LastVerdict = existing.LastVerdict
	item.SkipReason = existing.SkipReason
	// The retained workflow describes the snapshot it was started from, so
	// the stored snapshot must stay that one: persisting the new digest
	// next to the old workflow would make later runs treat the edited
	// issue as unchanged (never rediscovered) and let an in-flight
	// implementation result settle against requirements it never saw.
	item.SnapshotDigest = existing.SnapshotDigest
	item.Title = existing.Title
	item.Body = existing.Body
	item.LabelsJSON = existing.LabelsJSON
	item.GitHubUpdatedAt = existing.GitHubUpdatedAt
	return true
}

func repositoryMonitorIssueInventoryBlockCanClear(reason string) bool {
	switch strings.TrimSpace(reason) {
	case repositoryMonitorSkipReasonOverLimit, repositoryMonitorSkipReasonExcluded, repositoryMonitorSkipReasonMissingLabel, repositoryMonitorSkipReasonBlockedLabel, repositoryMonitorSkipReasonMissing, "":
		return true
	default:
		return false
	}
}

func repositoryMonitorMaxIssuesPerRun(spec corev1alpha1.RepositoryMonitorSpec) int {
	if spec.Targets.Issues.MaxPerRun == nil || *spec.Targets.Issues.MaxPerRun <= 0 {
		return 10
	}
	return int(*spec.Targets.Issues.MaxPerRun)
}

func filterRepositoryMonitorTargetIssues(issues []repositoryMonitorIssue, run *store.MonitorRun) []repositoryMonitorIssue {
	if run == nil || run.TargetNumber == 0 {
		return issues
	}
	filtered := make([]repositoryMonitorIssue, 0, 1)
	for _, issue := range issues {
		if issue.Number == run.TargetNumber {
			filtered = append(filtered, issue)
			break
		}
	}
	return filtered
}

func repositoryMonitorIssueCommandSkipReason(spec corev1alpha1.RepositoryMonitorSpec, issue repositoryMonitorIssue) string {
	if issue.IsPR {
		return repositoryMonitorSkipReasonPullRequest
	}
	if repositoryMonitorBlockedLabel(spec, issue.Labels) != "" {
		return repositoryMonitorSkipReasonBlockedLabel
	}
	if repositoryMonitorMatchingLabel(spec.Targets.Issues.ExcludeLabels, issue.Labels) != "" {
		return repositoryMonitorSkipReasonExcluded
	}
	if len(spec.Targets.Issues.IncludeLabels) > 0 && repositoryMonitorMatchingLabel(spec.Targets.Issues.IncludeLabels, issue.Labels) == "" {
		return repositoryMonitorSkipReasonMissingLabel
	}
	return ""
}

func repositoryMonitorIssueSkipReason(spec corev1alpha1.RepositoryMonitorSpec, issue repositoryMonitorIssue, selected, maxPerRun int) string {
	if issue.IsPR {
		return repositoryMonitorSkipReasonPullRequest
	}
	if blockedLabel := repositoryMonitorBlockedLabel(spec, issue.Labels); blockedLabel != "" {
		return repositoryMonitorSkipReasonBlockedLabel
	}
	if excluded := repositoryMonitorMatchingLabel(spec.Targets.Issues.ExcludeLabels, issue.Labels); excluded != "" {
		return repositoryMonitorSkipReasonExcluded
	}
	if len(spec.Targets.Issues.IncludeLabels) > 0 && repositoryMonitorMatchingLabel(spec.Targets.Issues.IncludeLabels, issue.Labels) == "" {
		return repositoryMonitorSkipReasonMissingLabel
	}
	if selected >= maxPerRun {
		return repositoryMonitorSkipReasonOverLimit
	}
	return ""
}

func repositoryMonitorMatchingLabel(policyLabels, itemLabels []string) string {
	wanted := map[string]struct{}{}
	for _, label := range policyLabels {
		label = strings.ToLower(strings.TrimSpace(label))
		if label != "" {
			wanted[label] = struct{}{}
		}
	}
	for _, label := range itemLabels {
		if _, ok := wanted[strings.ToLower(strings.TrimSpace(label))]; ok {
			return label
		}
	}
	return ""
}

func repositoryMonitorItemFromIssue(monitor *corev1alpha1.RepositoryMonitor, issue repositoryMonitorIssue, existing *store.MonitorItem) *store.MonitorItem {
	labelsJSON, _ := json.Marshal(issue.Labels)
	digest := repositoryMonitorIssueContentDigest(issue, repositoryMonitorIssueCommandLabelNames(monitor)...)
	item := &store.MonitorItem{
		MonitorNamespace: monitor.Namespace,
		MonitorName:      monitor.Name,
		Kind:             repositoryMonitorIssueKind,
		ItemKey:          fmt.Sprintf("%d", issue.Number),
		Number:           issue.Number,
		Title:            issue.Title,
		Body:             issue.Body,
		HTMLURL:          issue.HTMLURL,
		Author:           issue.Author,
		State:            issue.State,
		LabelsJSON:       string(labelsJSON),
		SnapshotDigest:   digest,
		GitHubUpdatedAt:  issue.UpdatedAt,
		WorkflowPhase:    repositoryMonitorIssuePhaseDiscovered,
	}
	if existing != nil {
		if existing.WorkflowPhase == repositoryMonitorIssuePhaseBlocked && !repositoryMonitorIssueInventoryBlockCanClear(existing.SkipReason) {
			item.WorkflowPhase = existing.WorkflowPhase
			item.SkipReason = existing.SkipReason
			item.LastVerdict = existing.LastVerdict
		}
		item.LastCommandID = existing.LastCommandID
		item.LastCommandIntent = existing.LastCommandIntent
		item.LinkedPRNumber = existing.LinkedPRNumber
		item.StatusCommentID = existing.StatusCommentID
		item.StatusCommentURL = existing.StatusCommentURL
		if existing.SnapshotDigest == digest {
			item.WorkflowPhase = existing.WorkflowPhase
			item.LastActionID = existing.LastActionID
			item.LastActionKind = existing.LastActionKind
			item.LastActionTaskName = existing.LastActionTaskName
			item.LastVerdict = existing.LastVerdict
			item.SkipReason = existing.SkipReason
		}
	}
	return item
}

func repositoryMonitorIssueCommandLabelNames(monitor *corev1alpha1.RepositoryMonitor) []string {
	if monitor == nil {
		return nil
	}
	labels := monitor.Spec.Triggers.GitHub.Labels.Issues
	return []string{labels.Triage, labels.Research, labels.Plan, labels.ApprovePlan, labels.Implement, labels.Decompose, labels.Stop, labels.Resume}
}

func repositoryMonitorIssueContentDigest(issue repositoryMonitorIssue, ignoredLabels ...string) string {
	labels := repositoryMonitorDigestLabels(issue.Labels, ignoredLabels)
	payload := struct {
		Number int64    `json:"number"`
		Title  string   `json:"title"`
		Body   string   `json:"body"`
		Labels []string `json:"labels"`
	}{Number: issue.Number, Title: issue.Title, Body: issue.Body, Labels: labels}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func repositoryMonitorDigestLabels(labels []string, ignoredLabels []string) []string {
	ignored := map[string]struct{}{}
	for _, label := range ignoredLabels {
		if label = strings.ToLower(strings.TrimSpace(label)); label != "" {
			ignored[label] = struct{}{}
		}
	}
	out := make([]string, 0, len(labels))
	for _, label := range labels {
		trimmed := strings.TrimSpace(label)
		lower := strings.ToLower(trimmed)
		if _, ok := ignored[lower]; ok {
			continue
		}
		if trimmed == "" || strings.HasPrefix(lower, "orka:") || strings.HasPrefix(lower, "orka-state:") {
			continue
		}
		out = append(out, trimmed)
	}
	slices.SortFunc(out, func(a, b string) int { return strings.Compare(strings.ToLower(a), strings.ToLower(b)) })
	return out
}

func (r *RepositoryMonitorReconciler) listRepositoryMonitorIssues(ctx context.Context, owner, repository, token string) ([]repositoryMonitorIssue, error) {
	var issues []repositoryMonitorIssue
	for page := 1; ; page++ {
		pageItems, err := r.fetchRepositoryMonitorIssuePage(ctx, owner, repository, token, page)
		if err != nil {
			return nil, err
		}
		issues = append(issues, pageItems...)
		if len(pageItems) < repositoryMonitorGitHubPerPage {
			break
		}
	}
	return issues, nil
}

func (r *RepositoryMonitorReconciler) listRepositoryMonitorIssuesForRun(ctx context.Context, owner, repository, token string, run *store.MonitorRun) ([]repositoryMonitorIssue, error) {
	if run != nil && run.TargetNumber > 0 {
		issue, err := r.fetchRepositoryMonitorIssue(ctx, owner, repository, token, run.TargetNumber)
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(strings.TrimSpace(issue.State), repositoryMonitorItemStateOpen) || issue.IsPR {
			return nil, nil
		}
		return []repositoryMonitorIssue{*issue}, nil
	}
	return r.listRepositoryMonitorIssues(ctx, owner, repository, token)
}

func (r *RepositoryMonitorReconciler) fetchRepositoryMonitorIssuePage(ctx context.Context, owner, repository, token string, page int) ([]repositoryMonitorIssue, error) {
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	query := url.Values{}
	query.Set("state", "open")
	query.Set("per_page", strconv.Itoa(repositoryMonitorGitHubPerPage))
	query.Set("page", strconv.Itoa(page))
	endpoint := fmt.Sprintf("%s/repos/%s/%s/issues?%s", baseURL, url.PathEscape(owner), url.PathEscape(repository), query.Encode())
	var response []repositoryMonitorIssueResponse
	if err := r.fetchRepositoryMonitorGitHubJSON(ctx, endpoint, token, "issue inventory", &response); err != nil {
		return nil, err
	}
	return repositoryMonitorIssuesFromGitHub(response), nil
}

func (r *RepositoryMonitorReconciler) fetchRepositoryMonitorIssue(ctx context.Context, owner, repository, token string, number int64) (*repositoryMonitorIssue, error) {
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/issues/%d", baseURL, url.PathEscape(owner), url.PathEscape(repository), number)
	var response repositoryMonitorIssueResponse
	if err := r.fetchRepositoryMonitorGitHubJSON(ctx, endpoint, token, "issue request", &response); err != nil {
		return nil, err
	}
	issue := repositoryMonitorIssueFromGitHub(response)
	return &issue, nil
}

func (r *RepositoryMonitorReconciler) fetchRepositoryMonitorGitHubJSON(ctx context.Context, endpoint, token, operation string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	if strings.TrimSpace(token) != "" {
		req.Header.Set("Authorization", strings.Join([]string{"Bearer", strings.TrimSpace(token)}, " "))
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	httpClient := r.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("GitHub %s failed: %w", operation, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	respBody, err := readRepositoryMonitorGitHubResponse(resp.Body, repositoryMonitorGitHubResponseLimit)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &repositoryMonitorGitHubAPIError{Operation: operation, StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("failed to parse GitHub %s response: %w", operation, err)
	}
	return nil
}

type repositoryMonitorIssueResponse struct {
	Number    int64     `json:"number"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	State     string    `json:"state"`
	HTMLURL   string    `json:"html_url"`
	UpdatedAt time.Time `json:"updated_at"`
	User      struct {
		Login string `json:"login"`
	} `json:"user"`
	PullRequest *struct {
		HTMLURL string `json:"html_url"`
	} `json:"pull_request,omitempty"`
	Labels []struct {
		Name string `json:"name"`
	} `json:"labels"`
}

func repositoryMonitorIssuesFromGitHub(response []repositoryMonitorIssueResponse) []repositoryMonitorIssue {
	issues := make([]repositoryMonitorIssue, 0, len(response))
	for _, issue := range response {
		issues = append(issues, repositoryMonitorIssueFromGitHub(issue))
	}
	return issues
}

func repositoryMonitorIssueFromGitHub(issue repositoryMonitorIssueResponse) repositoryMonitorIssue {
	labelNames := make([]string, 0, len(issue.Labels))
	for _, label := range issue.Labels {
		labelNames = append(labelNames, label.Name)
	}
	return repositoryMonitorIssue{
		Number:    issue.Number,
		Title:     issue.Title,
		Body:      issue.Body,
		Author:    issue.User.Login,
		State:     issue.State,
		HTMLURL:   issue.HTMLURL,
		Labels:    labelNames,
		UpdatedAt: issue.UpdatedAt,
		IsPR:      issue.PullRequest != nil,
	}
}
