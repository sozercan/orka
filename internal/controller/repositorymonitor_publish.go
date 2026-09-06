package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	repositoryMonitorPublishPhaseStarted   = "started"
	repositoryMonitorPublishPhaseSucceeded = "succeeded"
	repositoryMonitorPublishPhaseSkipped   = "skipped"
	repositoryMonitorPublishPhaseFailed    = "failed"

	repositoryMonitorPublishEventComment = "COMMENT"

	repositoryMonitorPublishModeSummaryOnly                     = "summary_only"
	repositoryMonitorPublishModeSummaryWithInlineFindings       = "summary_with_inline_findings"
	repositoryMonitorPublishSameHeadPolicySkip                  = "skip"
	repositoryMonitorPublishDefaultInlineMinPriority            = "P2"
	repositoryMonitorPublishDefaultInlineMaxComments      int32 = 10

	repositoryMonitorPublishSkipDisabled                   = "publish_disabled"
	repositoryMonitorPublishSkipMissingGitSecret           = "missing_git_secret"
	repositoryMonitorPublishSkipInvalidReviewResult        = "invalid_review_result"
	repositoryMonitorPublishSkipRepoMismatch               = "repo_mismatch"
	repositoryMonitorPublishSkipPRNumberMismatch           = "pr_number_mismatch"
	repositoryMonitorPublishSkipHeadSHAChanged             = "head_sha_changed"
	repositoryMonitorPublishSkipPRClosed                   = "pr_closed"
	repositoryMonitorPublishSkipBaseBranchMismatch         = "base_branch_mismatch"
	repositoryMonitorPublishSkipDraftPR                    = "draft_pr"
	repositoryMonitorPublishSkipBlockedLabel               = "blocked_label"
	repositoryMonitorPublishSkipDuplicateSameHead          = "duplicate_same_head"
	repositoryMonitorPublishSkipSecuritySensitiveNotPublic = "security_sensitive_not_public"
	repositoryMonitorPublishSkipBodyTooLarge               = "body_too_large"
	repositoryMonitorPublishSkipInlineMappingFailed        = "inline_mapping_failed"
	repositoryMonitorPublishSkipVerdictNotConfigured       = "verdict_not_configured"
	repositoryMonitorPublishSkipValidationPolicyChanged    = "validation_policy_changed"
	repositoryMonitorPublishSkipValidationUnavailable      = "validation_unavailable"

	repositoryMonitorPublishFailureGitHubPermissionDenied = "github_permission_denied"
	repositoryMonitorPublishFailureGitHubPermanent        = "github_permanent_error"
	repositoryMonitorPublishFailureGitHubAPI              = "github_api_error"

	repositoryMonitorReviewBodyMaxBytes    = 60000
	repositoryMonitorReviewInlineMaxRunes  = 4000
	repositoryMonitorReviewTextMaxRunes    = 4000
	repositoryMonitorReviewFindingMaxRunes = 1200
)

type repositoryMonitorPullRequestReviewRequest struct {
	CommitID string                                      `json:"commit_id"`
	Event    string                                      `json:"event"`
	Body     string                                      `json:"body"`
	Comments []repositoryMonitorPullRequestReviewComment `json:"comments,omitempty"`
}

type repositoryMonitorPullRequestReviewComment struct {
	Path string `json:"path"`
	Line int64  `json:"line"`
	Side string `json:"side"`
	Body string `json:"body"`
}

type repositoryMonitorPullRequestReviewResponse struct {
	ID      int64  `json:"id"`
	HTMLURL string `json:"html_url"`
	Body    string `json:"body"`
}

type repositoryMonitorPullRequestFileResponse struct {
	Filename         string `json:"filename"`
	PreviousFilename string `json:"previous_filename"`
	Status           string `json:"status"`
	Additions        int    `json:"additions"`
	Deletions        int    `json:"deletions"`
	Patch            string `json:"patch"`
}

type repositoryMonitorGitHubAPIError struct {
	Operation  string
	StatusCode int
	Body       string
}

func (e *repositoryMonitorGitHubAPIError) Error() string {
	return fmt.Sprintf("GitHub %s returned %d: %s", e.Operation, e.StatusCode, e.Body)
}

//nolint:gocyclo // Publish-time safety checks are intentionally linear and auditable.
func (r *RepositoryMonitorReconciler) publishRepositoryMonitorReview(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, task *corev1alpha1.Task, record *store.ReviewRecord) error {
	if r.Store == nil || monitor == nil || item == nil || task == nil || record == nil {
		return nil
	}

	publishRecord := repositoryMonitorBasePublishRecord(monitor, item, task, record)
	skip := func(reason, summary string, metadata map[string]any) error {
		publishRecord.Phase = repositoryMonitorPublishPhaseSkipped
		publishRecord.SkipReason = reason
		return r.finishRepositoryMonitorReviewPublish(ctx, monitor, item, publishRecord, "review_publish_skipped", summary, metadata)
	}
	fail := func(reason, message string, metadata map[string]any) error {
		publishRecord.Phase = repositoryMonitorPublishPhaseFailed
		publishRecord.SkipReason = reason
		publishRecord.Error = boundedString(message, repositoryMonitorReviewTextMaxRunes)
		return r.finishRepositoryMonitorReviewPublish(ctx, monitor, item, publishRecord, "review_publish_failed", fmt.Sprintf("Pull request #%d review publish failed: %s", item.Number, reason), metadata)
	}

	publish := monitor.Spec.Review.Publish
	if !publish.Enabled {
		return skip(repositoryMonitorPublishSkipDisabled, fmt.Sprintf("Pull request #%d review publishing skipped: publishing is disabled", item.Number), nil)
	}
	if event := effectiveRepositoryMonitorPublishEvent(publish); event != repositoryMonitorPublishEventComment {
		return skip(repositoryMonitorPublishSkipInvalidReviewResult, fmt.Sprintf("Pull request #%d review publishing skipped: unsupported publish event", item.Number), map[string]any{"event": event})
	}
	if policy := effectiveRepositoryMonitorPublishSameHeadPolicy(publish); policy != repositoryMonitorPublishSameHeadPolicySkip {
		return skip(repositoryMonitorPublishSkipInvalidReviewResult, fmt.Sprintf("Pull request #%d review publishing skipped: unsupported same-head policy", item.Number), map[string]any{"sameHeadPolicy": policy})
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		return skip(repositoryMonitorPublishSkipInvalidReviewResult, fmt.Sprintf("Pull request #%d review publishing skipped: review task did not succeed", item.Number), map[string]any{"taskPhase": task.Status.Phase})
	}
	if !repositoryMonitorReviewVerdictMarksHeadFresh(record.Verdict) {
		reason := repositoryMonitorPublishSkipInvalidReviewResult
		if record.Verdict == repositoryMonitorReviewVerdictStale {
			reason = repositoryMonitorPublishSkipHeadSHAChanged
		}
		return skip(reason, fmt.Sprintf("Pull request #%d review publishing skipped: review verdict %q is not publishable", item.Number, record.Verdict), map[string]any{"verdict": record.Verdict})
	}
	if !repositoryMonitorReviewRecordMatchesValidationPolicy(monitor, record) {
		return skip(repositoryMonitorPublishSkipValidationPolicyChanged, fmt.Sprintf("Pull request #%d review publishing skipped: validation policy changed after the review started", item.Number), map[string]any{
			"currentValidationImage": strings.TrimSpace(monitor.Spec.Validation.Image),
			"reviewValidationImage":  strings.TrimSpace(record.ValidationImage),
		})
	}
	if record.ValidationStatus == repositoryMonitorValidationStatusUnavailable {
		retryState, err := r.repositoryMonitorRepairValidationRetryState(ctx, monitor, record.Number, record.HeadSHA)
		if err != nil {
			return err
		}
		if !retryState.associated || !retryState.exhausted {
			return skip(repositoryMonitorPublishSkipValidationUnavailable, fmt.Sprintf("Pull request #%d review publishing skipped: validation is temporarily unavailable", item.Number), nil)
		}
	}
	if shouldPost, reason := repositoryMonitorPublishShouldPostVerdict(publish, record); !shouldPost {
		return skip(reason, fmt.Sprintf("Pull request #%d review publishing skipped: verdict %q is not enabled for publishing", item.Number, record.Verdict), map[string]any{"verdict": record.Verdict})
	}
	if strings.EqualFold(strings.TrimSpace(record.SecurityStatus), "security_sensitive") && !publish.PostSecuritySensitive {
		return skip(repositoryMonitorPublishSkipSecuritySensitiveNotPublic, fmt.Sprintf("Pull request #%d review publishing skipped: review result is security-sensitive", item.Number), map[string]any{"securityStatus": record.SecurityStatus, "verdict": record.Verdict})
	}
	if err := validateRepositoryMonitorPublishRecordBinding(monitor, item, task, record); err != nil {
		return skip(repositoryMonitorPublishSkipInvalidReviewResult, fmt.Sprintf("Pull request #%d review publishing skipped: %s", item.Number, err.Error()), map[string]any{"error": err.Error()})
	}

	owner, repository, err := repositoryMonitorOwnerRepository(monitor)
	if err != nil {
		return skip(repositoryMonitorPublishSkipRepoMismatch, fmt.Sprintf("Pull request #%d review publishing skipped: %s", item.Number, err.Error()), map[string]any{"error": err.Error()})
	}
	if taskRepo := strings.TrimSpace(task.Annotations[labels.AnnotationGitHubRepository]); taskRepo != "" && !strings.EqualFold(taskRepo, owner+"/"+repository) {
		return skip(repositoryMonitorPublishSkipRepoMismatch, fmt.Sprintf("Pull request #%d review publishing skipped: result repo does not match monitor repository", item.Number), map[string]any{"resultRepo": taskRepo, "monitorRepo": owner + "/" + repository})
	}
	if record.Number != item.Number {
		return skip(repositoryMonitorPublishSkipPRNumberMismatch, fmt.Sprintf("Pull request #%d review publishing skipped: review record PR number mismatch", item.Number), map[string]any{"recordNumber": record.Number, "itemNumber": item.Number})
	}
	if expectedNumber := strings.TrimSpace(task.Annotations[labels.AnnotationMonitorItemNumber]); expectedNumber != "" {
		parsed, parseErr := strconv.ParseInt(expectedNumber, 10, 64)
		if parseErr != nil || parsed != item.Number {
			return skip(repositoryMonitorPublishSkipPRNumberMismatch, fmt.Sprintf("Pull request #%d review publishing skipped: task PR number mismatch", item.Number), map[string]any{"taskNumber": expectedNumber, "itemNumber": item.Number})
		}
	}

	reviewedHead := strings.TrimSpace(record.HeadSHA)
	if reviewedHead == "" || reviewedHead != strings.TrimSpace(task.Annotations[labels.AnnotationMonitorHeadSHA]) {
		return skip(repositoryMonitorPublishSkipHeadSHAChanged, fmt.Sprintf("Pull request #%d review publishing skipped: reviewed head does not match task binding", item.Number), map[string]any{"recordHeadSHA": record.HeadSHA, "taskHeadSHA": task.Annotations[labels.AnnotationMonitorHeadSHA]})
	}

	token, err := r.repositoryMonitorForgeToken(ctx, monitor)
	if err != nil || strings.TrimSpace(token) == "" {
		message := "spec.forgeCredentialRef is required for GitHub publishing and must contain token, password, or GITHUB_TOKEN"
		if err != nil {
			message = err.Error()
		}
		return skip(repositoryMonitorPublishSkipMissingGitSecret, fmt.Sprintf("Pull request #%d review publishing skipped: missing GitHub credentials", item.Number), map[string]any{"error": message})
	}

	currentPR, err := r.fetchRepositoryMonitorPullRequest(ctx, owner, repository, token, item.Number)
	if err != nil {
		return fail(repositoryMonitorGitHubPublishFailureReason(err), err.Error(), map[string]any{"operation": "fetch_pull_request"})
	}
	if !strings.EqualFold(strings.TrimSpace(currentPR.State), repositoryMonitorItemStateOpen) {
		return skip(repositoryMonitorPublishSkipPRClosed, fmt.Sprintf("Pull request #%d review publishing skipped: pull request is not open", item.Number), map[string]any{"state": currentPR.State})
	}
	if baseBranch := effectiveRepositoryMonitorBranch(monitor); strings.TrimSpace(currentPR.BaseBranch) != baseBranch {
		return skip(repositoryMonitorPublishSkipBaseBranchMismatch, fmt.Sprintf("Pull request #%d review publishing skipped: base branch changed", item.Number), map[string]any{"currentBase": currentPR.BaseBranch, "monitorBase": baseBranch})
	}
	if strings.TrimSpace(currentPR.HeadSHA) != reviewedHead {
		return skip(repositoryMonitorPublishSkipHeadSHAChanged, fmt.Sprintf("Pull request #%d review publishing skipped: head SHA changed before publish", item.Number), map[string]any{"currentHeadSHA": currentPR.HeadSHA, "reviewedHeadSHA": reviewedHead})
	}
	if currentPR.Draft && !monitor.Spec.Targets.PullRequests.IncludeDrafts {
		return skip(repositoryMonitorPublishSkipDraftPR, fmt.Sprintf("Pull request #%d review publishing skipped: pull request is draft", item.Number), nil)
	}
	if blockedLabel := repositoryMonitorBlockedLabel(monitor.Spec, currentPR.Labels); blockedLabel != "" {
		return skip(repositoryMonitorPublishSkipBlockedLabel, fmt.Sprintf("Pull request #%d review publishing skipped: blocked label %q is present", item.Number, blockedLabel), map[string]any{"label": blockedLabel})
	}

	exists, err := r.repositoryMonitorReviewPublishRecordExists(ctx, monitor, item, reviewedHead)
	if err != nil {
		return err
	}
	if exists {
		return skip(repositoryMonitorPublishSkipDuplicateSameHead, fmt.Sprintf("Pull request #%d review publishing skipped: same head was already published", item.Number), nil)
	}
	matchedReview, matchedPublishID, err := r.repositoryMonitorGitHubReviewMarkerMatch(ctx, monitor, item, owner, repository, token, reviewedHead)
	if err != nil {
		return fail(repositoryMonitorGitHubPublishFailureReason(err), err.Error(), map[string]any{"operation": "list_reviews"})
	}
	if matchedReview != nil {
		mutationID := "ghmut-" + repositoryMonitorShortHash(matchedPublishID+"-submit-review")
		mutation, mutationErr := r.Store.GetGitHubMutationRecord(ctx, monitor.Namespace, mutationID)
		if errors.Is(mutationErr, store.ErrNotFound) {
			mutation = &store.GitHubMutationRecord{ID: mutationID, RunID: repositoryMonitorReviewRunID(task), Operation: "submit_review", TargetKind: repositoryMonitorPullRequestKind, TargetNumber: item.Number, TargetSHA: reviewedHead, Reason: "publish_review", GitHubURL: matchedReview.HTMLURL, ExternalID: strconv.FormatInt(matchedReview.ID, 10), Status: repositoryMonitorRunPhaseSucceeded}
			if err := r.recordRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
				return err
			}
		} else if mutationErr != nil {
			return mutationErr
		} else if mutation.Status != repositoryMonitorRunPhaseSucceeded {
			mutation.Status = repositoryMonitorRunPhaseSucceeded
			mutation.Error = ""
			mutation.GitHubURL = matchedReview.HTMLURL
			mutation.ExternalID = strconv.FormatInt(matchedReview.ID, 10)
			if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
				return err
			}
		}
		if existingPublish, getErr := r.Store.GetReviewPublishRecord(ctx, monitor.Namespace, matchedPublishID); getErr == nil {
			existingPublish.Phase = repositoryMonitorPublishPhaseSucceeded
			existingPublish.GitHubReviewID = strconv.FormatInt(matchedReview.ID, 10)
			existingPublish.GitHubReviewURL = matchedReview.HTMLURL
			existingPublish.Error = ""
			existingPublish.UpdatedAt = time.Now()
			if err := r.Store.UpdateReviewPublishRecord(ctx, existingPublish); err != nil {
				return err
			}
		} else if !errors.Is(getErr, store.ErrNotFound) {
			return getErr
		}
		return skip(repositoryMonitorPublishSkipDuplicateSameHead, fmt.Sprintf("Pull request #%d review publishing skipped: GitHub already has an Orka review for this head", item.Number), nil)
	}

	body, comments, err := r.renderRepositoryMonitorGitHubReview(ctx, monitor, item, task, record, publishRecord.ID, token, owner, repository)
	if err != nil {
		return fail(repositoryMonitorGitHubPublishFailureReason(err), err.Error(), map[string]any{"operation": "inline_mapping"})
	}
	if len([]byte(body)) > repositoryMonitorReviewBodyMaxBytes {
		return skip(repositoryMonitorPublishSkipBodyTooLarge, fmt.Sprintf("Pull request #%d review publishing skipped: rendered review body is too large", item.Number), map[string]any{"bytes": len([]byte(body))})
	}
	publishRecord.BodyDigest = repositoryMonitorBodyDigest(body)
	publishRecord.InlineCommentCount = len(comments)
	publishRecord.Phase = repositoryMonitorPublishPhaseStarted
	now := time.Now()
	publishRecord.CreatedAt = now
	publishRecord.UpdatedAt = now
	if err := r.Store.CreateReviewPublishRecord(ctx, publishRecord); err != nil {
		if isPublishRecordUniqueConflict(err) {
			publishRecord.ID = "monpub-" + uuid.NewString()
			publishRecord.CreatedAt = time.Time{}
			publishRecord.Phase = repositoryMonitorPublishPhaseSkipped
			publishRecord.SkipReason = repositoryMonitorPublishSkipDuplicateSameHead
			return r.finishRepositoryMonitorReviewPublish(ctx, monitor, item, publishRecord, "review_publish_skipped", fmt.Sprintf("Pull request #%d review publishing skipped: same head is already being published", item.Number), nil)
		}
		return err
	}

	if err := r.createMonitorEvent(ctx, monitor, repositoryMonitorReviewRunID(task), repositoryMonitorPullRequestKind, item.Number, reviewedHead, "review_publish_started", fmt.Sprintf("Pull request #%d review publishing started", item.Number), map[string]any{
		"reviewID":           record.ID,
		"reviewTaskName":     task.Name,
		"event":              repositoryMonitorPublishEventComment,
		"inlineCommentCount": len(comments),
	}); err != nil {
		return err
	}

	mutationID := "ghmut-" + repositoryMonitorShortHash(publishRecord.ID+"-submit-review")
	mutation := &store.GitHubMutationRecord{ID: mutationID, RunID: repositoryMonitorReviewRunID(task), Operation: "submit_review", TargetKind: repositoryMonitorPullRequestKind, TargetNumber: item.Number, TargetSHA: reviewedHead, Reason: "publish_review", Status: repositoryMonitorAutomergeStateStarted}
	existing, mutationErr := r.Store.GetGitHubMutationRecord(ctx, monitor.Namespace, mutationID)
	if mutationErr == nil {
		mutation = existing
	} else if errors.Is(mutationErr, store.ErrNotFound) {
		if err := r.recordRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
			return err
		}
	} else {
		return mutationErr
	}
	response := &repositoryMonitorPullRequestReviewResponse{HTMLURL: mutation.GitHubURL}
	if mutation.Status == repositoryMonitorRunPhaseSucceeded {
		response.ID, _ = strconv.ParseInt(mutation.ExternalID, 10, 64)
	} else {
		response, err = r.createRepositoryMonitorPullRequestReview(ctx, owner, repository, token, item.Number, repositoryMonitorPullRequestReviewRequest{CommitID: reviewedHead, Event: repositoryMonitorPublishEventComment, Body: body, Comments: comments})
		if err != nil {
			mutation.Status = repositoryMonitorRunPhaseFailed
			mutation.Error = err.Error()
			if auditErr := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); auditErr != nil {
				return fmt.Errorf("publish review failed: %w; additionally failed to update mutation audit: %v", err, auditErr)
			}
			return fail(repositoryMonitorGitHubPublishFailureReason(err), err.Error(), map[string]any{"operation": "create_review"})
		}
		mutation.Status = repositoryMonitorRunPhaseSucceeded
		mutation.Error = ""
		mutation.GitHubURL = response.HTMLURL
		mutation.ExternalID = strconv.FormatInt(response.ID, 10)
		if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
			return err
		}
	}
	publishRecord.Phase = repositoryMonitorPublishPhaseSucceeded
	publishRecord.GitHubReviewID = strconv.FormatInt(response.ID, 10)
	publishRecord.GitHubReviewURL = response.HTMLURL
	return r.finishRepositoryMonitorReviewPublish(ctx, monitor, item, publishRecord, "review_publish_succeeded", fmt.Sprintf("Pull request #%d review published to GitHub", item.Number), map[string]any{
		"reviewID":           record.ID,
		"githubReviewID":     publishRecord.GitHubReviewID,
		"githubReviewURL":    publishRecord.GitHubReviewURL,
		"inlineCommentCount": publishRecord.InlineCommentCount,
	})
}

func (r *RepositoryMonitorReconciler) publishPendingRepositoryMonitorReviewRecords(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (bool, error) {
	if r.Store == nil || !monitor.Spec.Review.Publish.Enabled {
		return false, nil
	}
	published := false
	cursor := ""
	for {
		items, next, err := r.Store.ListMonitorItems(ctx, store.MonitorItemFilter{
			Namespace:   monitor.Namespace,
			MonitorName: monitor.Name,
			Kind:        repositoryMonitorPullRequestKind,
			Limit:       100,
			Cursor:      cursor,
		})
		if err != nil {
			return published, err
		}
		for i := range items {
			item := items[i]
			if strings.TrimSpace(item.LastReviewID) == "" || item.LastVerdict == repositoryMonitorRunPhaseQueued {
				continue
			}
			record, err := r.Store.GetReviewRecord(ctx, monitor.Namespace, item.LastReviewID)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					continue
				}
				return published, err
			}
			if record.Kind != repositoryMonitorPullRequestKind || record.Number != item.Number || !repositoryMonitorReviewVerdictMarksHeadFresh(record.Verdict) {
				continue
			}
			shouldPublish, activeReservation, err := r.repositoryMonitorReviewRecordNeedsPublishRetry(ctx, monitor, record)
			if err != nil {
				return published, err
			}
			if activeReservation {
				published = true
			}
			if !shouldPublish {
				continue
			}
			var task corev1alpha1.Task
			if err := r.Get(ctx, types.NamespacedName{Namespace: record.TaskNamespace, Name: record.TaskName}, &task); err != nil {
				if client.IgnoreNotFound(err) == nil {
					continue
				}
				return published, err
			}
			if err := r.publishRepositoryMonitorReview(ctx, monitor, &item, &task, record); err != nil {
				return published, err
			}
			published = true
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return published, nil
}

func (r *RepositoryMonitorReconciler) repositoryMonitorReviewRecordNeedsPublishRetry(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, record *store.ReviewRecord) (bool, bool, error) {
	publishRecords, _, err := r.Store.ListReviewPublishRecords(ctx, store.ReviewPublishRecordFilter{
		Namespace:      monitor.Namespace,
		MonitorName:    monitor.Name,
		ReviewRecordID: record.ID,
		Limit:          20,
	})
	if err != nil {
		return false, false, err
	}
	needsPublish := true
	activeReservation := false
	for i := range publishRecords {
		publishRecord := publishRecords[i]
		switch publishRecord.Phase {
		case repositoryMonitorPublishPhaseSucceeded:
			needsPublish = false
		case repositoryMonitorPublishPhaseSkipped:
			if !repositoryMonitorPublishSkipReasonRecoverable(publishRecord.SkipReason) {
				needsPublish = false
				continue
			}
			if time.Since(publishRecord.UpdatedAt) < 5*time.Minute {
				needsPublish = false
				activeReservation = true
			}
		case repositoryMonitorPublishPhaseFailed:
			if publishRecord.SkipReason != repositoryMonitorPublishFailureGitHubAPI {
				needsPublish = false
				continue
			}
			if time.Since(publishRecord.UpdatedAt) < 5*time.Minute {
				needsPublish = false
				activeReservation = true
			}
		case repositoryMonitorPublishPhaseStarted:
			if time.Since(publishRecord.UpdatedAt) < 15*time.Minute {
				needsPublish = false
				activeReservation = true
				continue
			}
			publishRecord.Phase = repositoryMonitorPublishPhaseFailed
			publishRecord.SkipReason = repositoryMonitorPublishFailureGitHubAPI
			publishRecord.Error = "stale publish reservation recovered by controller"
			publishRecord.UpdatedAt = time.Now()
			if err := r.Store.UpdateReviewPublishRecord(ctx, &publishRecord); err != nil {
				return false, false, err
			}
		}
	}
	return needsPublish, activeReservation, nil
}

func repositoryMonitorPublishSkipReasonRecoverable(reason string) bool {
	switch strings.TrimSpace(reason) {
	case repositoryMonitorPublishSkipMissingGitSecret,
		repositoryMonitorPublishSkipPRClosed,
		repositoryMonitorPublishSkipBaseBranchMismatch,
		repositoryMonitorPublishSkipDraftPR,
		repositoryMonitorPublishSkipBlockedLabel:
		return true
	default:
		return false
	}
}

func repositoryMonitorBasePublishRecord(monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, task *corev1alpha1.Task, record *store.ReviewRecord) *store.ReviewPublishRecord {
	return &store.ReviewPublishRecord{
		ID:               "monpub-" + uuid.NewString(),
		MonitorNamespace: monitor.Namespace,
		MonitorName:      monitor.Name,
		ItemKind:         repositoryMonitorPullRequestKind,
		ItemNumber:       item.Number,
		HeadSHA:          strings.TrimSpace(record.HeadSHA),
		RunID:            repositoryMonitorReviewRunID(task),
		ReviewTaskName:   task.Name,
		ReviewRecordID:   record.ID,
		Event:            repositoryMonitorPublishEventComment,
	}
}

func (r *RepositoryMonitorReconciler) finishRepositoryMonitorReviewPublish(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, record *store.ReviewPublishRecord, eventType, summary string, metadata map[string]any) error {
	reserved := !record.CreatedAt.IsZero()
	now := time.Now()
	if !reserved {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	if !reserved {
		if err := r.Store.CreateReviewPublishRecord(ctx, record); err != nil {
			if isPublishRecordUniqueConflict(err) {
				record.ID = "monpub-" + uuid.NewString()
				record.Phase = repositoryMonitorPublishPhaseSkipped
				record.SkipReason = repositoryMonitorPublishSkipDuplicateSameHead
				record.GitHubReviewID = ""
				record.GitHubReviewURL = ""
				if createErr := r.Store.CreateReviewPublishRecord(ctx, record); createErr != nil {
					return createErr
				}
			} else {
				return err
			}
		}
	} else if err := r.Store.UpdateReviewPublishRecord(ctx, record); err != nil {
		if isPublishRecordUniqueConflict(err) {
			record.Phase = repositoryMonitorPublishPhaseSkipped
			record.SkipReason = repositoryMonitorPublishSkipDuplicateSameHead
			record.GitHubReviewID = ""
			record.GitHubReviewURL = ""
			if updateErr := r.Store.UpdateReviewPublishRecord(ctx, record); updateErr != nil {
				return updateErr
			}
		} else {
			return err
		}
	}
	item.LastPublishID = record.ID
	item.LastPublishPhase = record.Phase
	item.LastPublishReason = firstNonEmptyString(record.SkipReason, record.Error)
	item.LastPublishURL = record.GitHubReviewURL
	if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
		return err
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadata["publishID"] = record.ID
	metadata["phase"] = record.Phase
	if record.SkipReason != "" {
		metadata["skipReason"] = record.SkipReason
	}
	if record.Error != "" {
		metadata["error"] = record.Error
	}
	return r.createMonitorEvent(ctx, monitor, record.RunID, repositoryMonitorPullRequestKind, item.Number, record.HeadSHA, eventType, summary, metadata)
}

func isPublishRecordUniqueConflict(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "unique")
}

func validateRepositoryMonitorPublishRecordBinding(monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, task *corev1alpha1.Task, record *store.ReviewRecord) error {
	if err := validateRepositoryMonitorReviewTaskItemBinding(task, monitor, repositoryMonitorPullRequestKind, item.Number); err != nil {
		return err
	}
	if record.MonitorNamespace != monitor.Namespace || record.MonitorName != monitor.Name || record.Kind != repositoryMonitorPullRequestKind {
		return fmt.Errorf("review record monitor binding does not match")
	}
	if strings.TrimSpace(record.TaskName) != task.Name || strings.TrimSpace(record.TaskNamespace) != task.Namespace {
		return fmt.Errorf("review record task binding does not match")
	}
	return nil
}

func repositoryMonitorPublishShouldPostVerdict(publish corev1alpha1.RepositoryMonitorReviewPublishSpec, record *store.ReviewRecord) (bool, string) {
	switch strings.TrimSpace(record.Verdict) {
	case repositoryMonitorReviewVerdictPassed:
		if boolPtrDefault(publish.PostPassed, false) {
			return true, ""
		}
		return false, repositoryMonitorPublishSkipVerdictNotConfigured
	case repositoryMonitorReviewVerdictNeedsChanges:
		if boolPtrDefault(publish.PostNeedsChanges, true) {
			return true, ""
		}
		return false, repositoryMonitorPublishSkipVerdictNotConfigured
	case repositoryMonitorReviewVerdictNeedsHuman:
		if boolPtrDefault(publish.PostNeedsHuman, true) {
			return true, ""
		}
		return false, repositoryMonitorPublishSkipVerdictNotConfigured
	case repositoryMonitorReviewVerdictSecuritySensitive:
		if publish.PostSecuritySensitive {
			return true, ""
		}
		return false, repositoryMonitorPublishSkipSecuritySensitiveNotPublic
	default:
		return false, repositoryMonitorPublishSkipInvalidReviewResult
	}
}

func boolPtrDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func effectiveRepositoryMonitorPublishEvent(publish corev1alpha1.RepositoryMonitorReviewPublishSpec) string {
	if event := strings.TrimSpace(publish.Event); event != "" {
		return event
	}
	return repositoryMonitorPublishEventComment
}

func effectiveRepositoryMonitorPublishMode(publish corev1alpha1.RepositoryMonitorReviewPublishSpec) string {
	if mode := strings.TrimSpace(publish.Mode); mode != "" {
		return mode
	}
	return repositoryMonitorPublishModeSummaryOnly
}

func effectiveRepositoryMonitorPublishSameHeadPolicy(publish corev1alpha1.RepositoryMonitorReviewPublishSpec) string {
	if policy := strings.TrimSpace(publish.SameHeadPolicy); policy != "" {
		return policy
	}
	return repositoryMonitorPublishSameHeadPolicySkip
}

func effectiveRepositoryMonitorInlineMinPriority(publish corev1alpha1.RepositoryMonitorReviewPublishSpec) string {
	if priority := strings.TrimSpace(publish.Inline.MinPriority); priority != "" {
		return priority
	}
	return repositoryMonitorPublishDefaultInlineMinPriority
}

func effectiveRepositoryMonitorInlineMaxComments(publish corev1alpha1.RepositoryMonitorReviewPublishSpec) int {
	if publish.Inline.MaxComments == nil {
		return int(repositoryMonitorPublishDefaultInlineMaxComments)
	}
	if *publish.Inline.MaxComments < 0 {
		return 0
	}
	return int(*publish.Inline.MaxComments)
}

func repositoryMonitorPublishInlineEnabled(publish corev1alpha1.RepositoryMonitorReviewPublishSpec) bool {
	return effectiveRepositoryMonitorPublishMode(publish) == repositoryMonitorPublishModeSummaryWithInlineFindings && publish.Inline.Enabled
}

func repositoryMonitorOwnerRepository(monitor *corev1alpha1.RepositoryMonitor) (string, string, error) {
	return security.ParseGitHubRepositoryURL(monitor.Spec.RepoURL)
}

func (r *RepositoryMonitorReconciler) repositoryMonitorReviewPublishRecordExists(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, headSHA string) (bool, error) {
	records, _, err := r.Store.ListReviewPublishRecords(ctx, store.ReviewPublishRecordFilter{
		Namespace:   monitor.Namespace,
		MonitorName: monitor.Name,
		ItemKind:    repositoryMonitorPullRequestKind,
		ItemNumber:  item.Number,
		HeadSHA:     headSHA,
		Phase:       repositoryMonitorPublishPhaseSucceeded,
		Limit:       1,
	})
	if err != nil {
		return false, err
	}
	return len(records) > 0, nil
}

func (r *RepositoryMonitorReconciler) renderRepositoryMonitorGitHubReview(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, task *corev1alpha1.Task, record *store.ReviewRecord, publishID, token, owner, repository string) (string, []repositoryMonitorPullRequestReviewComment, error) {
	findings, err := repositoryMonitorReviewFindingsFromRecord(record)
	if err != nil {
		return "", nil, err
	}
	comments := []repositoryMonitorPullRequestReviewComment{}
	if repositoryMonitorPublishInlineEnabled(monitor.Spec.Review.Publish) {
		commentable, err := r.repositoryMonitorCommentableRightLines(ctx, owner, repository, token, item.Number)
		if err != nil {
			return "", nil, err
		}
		comments = repositoryMonitorInlineCommentsForFindings(record, findings, commentable, monitor.Spec.Review.Publish)
	}
	body := renderRepositoryMonitorReviewBody(monitor, item, task, record, publishID, findings)
	return body, comments, nil
}

func repositoryMonitorReviewFindingsFromRecord(record *store.ReviewRecord) ([]repositoryMonitorReviewFinding, error) {
	if strings.TrimSpace(record.FindingsJSON) == "" {
		return nil, nil
	}
	var findings []repositoryMonitorReviewFinding
	if err := json.Unmarshal([]byte(record.FindingsJSON), &findings); err != nil {
		return nil, err
	}
	return findings, nil
}

func renderRepositoryMonitorReviewBody(monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, task *corev1alpha1.Task, record *store.ReviewRecord, publishID string, findings []repositoryMonitorReviewFinding) string {
	var b strings.Builder
	b.WriteString("## Orka review\n\n")
	fmt.Fprintf(&b, "**Verdict:** %s  \n", sanitizeRepositoryMonitorReviewText(record.Verdict, 80))
	fmt.Fprintf(&b, "**Confidence:** %s  \n", sanitizeRepositoryMonitorReviewText(record.Confidence, 80))
	fmt.Fprintf(&b, "**Head:** `%s`\n\n", shortRepositoryMonitorHead(record.HeadSHA))
	summary := sanitizeRepositoryMonitorReviewText(record.Summary, repositoryMonitorReviewTextMaxRunes)
	if summary == "" {
		summary = "Orka completed a structured pull request review."
	}
	b.WriteString(summary)
	b.WriteString("\n\n")
	b.WriteString("### Findings\n\n")
	if len(findings) == 0 {
		b.WriteString("No findings reported.\n\n")
	} else {
		for _, finding := range findings {
			b.WriteString("- ")
			b.WriteString(renderRepositoryMonitorFindingSummary(finding))
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	b.WriteString("### Tests\n\n")
	validationStatus := sanitizeRepositoryMonitorReviewText(firstNonEmptyString(record.ValidationStatus, repositoryMonitorValidationStatusNotRun), 80)
	fmt.Fprintf(&b, "**Status:** %s  \n", validationStatus)
	if image := sanitizeRepositoryMonitorReviewText(record.ValidationImage, 2048); image != "" {
		fmt.Fprintf(&b, "**Image:** %s  \n", image)
	}
	evidence := sanitizeRepositoryMonitorReviewText(record.ValidationEvidence, repositoryMonitorReviewTextMaxRunes)
	if evidence == "" {
		evidence = "No validation evidence was recorded."
	}
	b.WriteString("\n")
	for line := range strings.SplitSeq(evidence, "\n") {
		b.WriteString("> ")
		b.WriteString(line)
		b.WriteString("\n")
	}
	b.WriteString("\n")
	b.WriteString(repositoryMonitorReviewMarker(monitor, item.Number, record.HeadSHA, repositoryMonitorReviewRunID(task), record.ID, publishID))
	return b.String()
}

func renderRepositoryMonitorFindingSummary(finding repositoryMonitorReviewFinding) string {
	priority := sanitizeRepositoryMonitorReviewText(firstNonEmptyString(finding.Priority, "P?"), 16)
	title := sanitizeRepositoryMonitorReviewText(firstNonEmptyString(finding.Title, "Untitled finding"), 240)
	location := ""
	if strings.TrimSpace(finding.File) != "" && finding.Line > 0 {
		location = fmt.Sprintf(" (`%s:%d`)", sanitizeRepositoryMonitorReviewText(finding.File, 300), finding.Line)
	} else if strings.TrimSpace(finding.File) != "" {
		location = fmt.Sprintf(" (`%s`)", sanitizeRepositoryMonitorReviewText(finding.File, 300))
	}
	body := sanitizeRepositoryMonitorReviewText(finding.Body, repositoryMonitorReviewFindingMaxRunes)
	recommendation := sanitizeRepositoryMonitorReviewText(finding.Recommendation, repositoryMonitorReviewFindingMaxRunes)
	parts := []string{fmt.Sprintf("%s: **%s**%s", priority, title, location)}
	if body != "" {
		parts = append(parts, body)
	}
	if recommendation != "" {
		parts = append(parts, "Recommendation: "+recommendation)
	}
	return strings.Join(parts, " — ")
}

func repositoryMonitorInlineCommentsForFindings(record *store.ReviewRecord, findings []repositoryMonitorReviewFinding, commentable map[string]map[int64]struct{}, publish corev1alpha1.RepositoryMonitorReviewPublishSpec) []repositoryMonitorPullRequestReviewComment {
	maxComments := effectiveRepositoryMonitorInlineMaxComments(publish)
	if maxComments == 0 {
		return nil
	}
	minPriority := effectiveRepositoryMonitorInlineMinPriority(publish)
	comments := make([]repositoryMonitorPullRequestReviewComment, 0, min(maxComments, len(findings)))
	for _, finding := range findings {
		if len(comments) >= maxComments {
			break
		}
		path := strings.TrimSpace(finding.File)
		if path == "" || finding.Line <= 0 || !repositoryMonitorPriorityAtOrAbove(finding.Priority, minPriority) {
			continue
		}
		lines := commentable[path]
		if _, ok := lines[finding.Line]; !ok {
			continue
		}
		body := renderRepositoryMonitorInlineFinding(record, finding)
		comments = append(comments, repositoryMonitorPullRequestReviewComment{Path: path, Line: finding.Line, Side: "RIGHT", Body: body})
	}
	return comments
}

func renderRepositoryMonitorInlineFinding(record *store.ReviewRecord, finding repositoryMonitorReviewFinding) string {
	var b strings.Builder
	b.WriteString("**")
	b.WriteString(sanitizeRepositoryMonitorReviewText(firstNonEmptyString(finding.Priority, "P?"), 16))
	b.WriteString(": ")
	b.WriteString(sanitizeRepositoryMonitorReviewText(firstNonEmptyString(finding.Title, "Orka finding"), 240))
	b.WriteString("**\n\n")
	if body := sanitizeRepositoryMonitorReviewText(finding.Body, repositoryMonitorReviewFindingMaxRunes); body != "" {
		b.WriteString(body)
		b.WriteString("\n\n")
	}
	if recommendation := sanitizeRepositoryMonitorReviewText(finding.Recommendation, repositoryMonitorReviewFindingMaxRunes); recommendation != "" {
		b.WriteString("Recommendation: ")
		b.WriteString(recommendation)
		b.WriteString("\n\n")
	}
	fmt.Fprintf(&b, "<!-- orka:repo-monitor-inline review=%s -->", sanitizeRepositoryMonitorReviewText(record.ID, 160))
	return boundedString(b.String(), repositoryMonitorReviewInlineMaxRunes)
}

func (r *RepositoryMonitorReconciler) repositoryMonitorCommentableRightLines(ctx context.Context, owner, repository, token string, number int64) (map[string]map[int64]struct{}, error) {
	files, err := r.listRepositoryMonitorPullRequestFiles(ctx, owner, repository, token, number)
	if err != nil {
		return nil, err
	}
	commentable := map[string]map[int64]struct{}{}
	for _, file := range files {
		lines := parseRepositoryMonitorPatchRightLines(file.Patch)
		if len(lines) == 0 {
			continue
		}
		commentable[file.Filename] = lines
	}
	return commentable, nil
}

func parseRepositoryMonitorPatchRightLines(patch string) map[int64]struct{} {
	lines := map[int64]struct{}{}
	var newLine int64
	for line := range strings.SplitSeq(patch, "\n") {
		if strings.HasPrefix(line, "@@") {
			newLine = parseRepositoryMonitorPatchNewStart(line)
			continue
		}
		if newLine == 0 || line == "" || strings.HasPrefix(line, `\ No newline`) {
			continue
		}
		switch line[0] {
		case '+':
			lines[newLine] = struct{}{}
			newLine++
		case ' ':
			newLine++
		case '-':
			// Removed lines are LEFT-side lines and are not commentable in V1.
		default:
			newLine++
		}
	}
	return lines
}

func parseRepositoryMonitorPatchNewStart(hunk string) int64 {
	plus := strings.Index(hunk, "+")
	if plus < 0 {
		return 0
	}
	end := plus + 1
	for end < len(hunk) && hunk[end] >= '0' && hunk[end] <= '9' {
		end++
	}
	if end == plus+1 {
		return 0
	}
	value, err := strconv.ParseInt(hunk[plus+1:end], 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func repositoryMonitorPriorityAtOrAbove(priority, minPriority string) bool {
	priorityRank, ok := repositoryMonitorPriorityRank(priority)
	if !ok {
		return false
	}
	minRank, ok := repositoryMonitorPriorityRank(minPriority)
	if !ok {
		minRank = 2
	}
	return priorityRank <= minRank
}

func repositoryMonitorPriorityRank(priority string) (int, bool) {
	switch strings.ToUpper(strings.TrimSpace(priority)) {
	case "P0":
		return 0, true
	case "P1":
		return 1, true
	case "P2":
		return 2, true
	case "P3":
		return 3, true
	default:
		return 0, false
	}
}

func sanitizeRepositoryMonitorReviewText(value string, maxRunes int) string {
	// Agent-produced text (research summaries, review verdicts, plan
	// excerpts) is published on GitHub; strip control/format runes and
	// redact credential shapes before bounding, so a secret an agent found
	// in the repository never reaches a public comment — and bounding
	// cannot split a token past the redactor.
	value = strings.TrimSpace(value)
	// A model can wrap one credential across lines. Check a joined shadow
	// before preserving the original formatting; if the joined value is
	// credential-shaped, withhold the field because line-by-line redaction
	// cannot safely reconstruct which fragments belong to the secret.
	// The shadow is taken from the original text as well as the sanitized
	// one: line-level sanitization may withhold the fragment that made the
	// wrapped credential recognizable and leave a tail fragment behind.
	sanitized := repositoryMonitorReviewContextSanitize(value)
	unwrap := strings.NewReplacer("\r", "", "\n", "")
	if security.LooksLikeSecret(unwrap.Replace(value)) || security.LooksLikeSecret(unwrap.Replace(sanitized)) {
		value = "[REDACTED]"
	} else {
		value = sanitized
	}
	return neutralizeRepositoryMonitorActiveText(neutralizeRepositoryMonitorMentions(boundedString(value, maxRunes)))
}

func neutralizeRepositoryMonitorActiveText(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	value = strings.ReplaceAll(value, "<!--", "<\u200b!--")
	value = strings.ReplaceAll(value, "-->", "--\u200b>")
	lines := strings.Split(value, "\n")
	for i, line := range lines {
		trimmed := strings.TrimLeftFunc(line, unicode.IsSpace)
		if strings.HasPrefix(trimmed, "/") {
			indentLen := len(line) - len(trimmed)
			lines[i] = line[:indentLen] + "\u200b" + trimmed
		}
	}
	return strings.Join(lines, "\n")
}

func neutralizeRepositoryMonitorMentions(value string) string {
	if value == "" {
		return value
	}
	var b strings.Builder
	b.Grow(len(value))
	for i := 0; i < len(value); i++ {
		ch := value[i]
		b.WriteByte(ch)
		if ch == '@' && i+1 < len(value) && isRepositoryMonitorMentionStart(value[i+1]) {
			b.WriteRune('\u200b')
		}
	}
	return b.String()
}

func isRepositoryMonitorMentionStart(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_'
}

func boundedString(value string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return string(runes[:maxRunes]) + "…"
}

func shortRepositoryMonitorHead(head string) string {
	head = strings.TrimSpace(head)
	if len(head) <= 8 {
		return head
	}
	return head[:8]
}

func repositoryMonitorReviewMarker(monitor *corev1alpha1.RepositoryMonitor, prNumber int64, headSHA, runID, reviewID, publishID string) string {
	return fmt.Sprintf("<!-- orka:repo-monitor namespace=%s name=%s pr=%d head=%s run=%s review=%s publish=%s -->", monitor.Namespace, monitor.Name, prNumber, strings.TrimSpace(headSHA), strings.TrimSpace(runID), strings.TrimSpace(reviewID), strings.TrimSpace(publishID))
}

func repositoryMonitorReviewMarkerPrefix(monitor *corev1alpha1.RepositoryMonitor, prNumber int64, headSHA string) string {
	return fmt.Sprintf("<!-- orka:repo-monitor namespace=%s name=%s pr=%d head=%s ", monitor.Namespace, monitor.Name, prNumber, strings.TrimSpace(headSHA))
}

func repositoryMonitorBodyDigest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func repositoryMonitorReviewRunID(task *corev1alpha1.Task) string {
	if task == nil {
		return ""
	}
	return strings.TrimSpace(task.Annotations[labels.AnnotationMonitorRunID])
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func repositoryMonitorGitHubErrorLooksRateLimited(body string) bool {
	body = strings.ToLower(body)
	return strings.Contains(body, "rate limit") || strings.Contains(body, "abuse") || strings.Contains(body, "secondary rate")
}

func repositoryMonitorGitHubPublishFailureReason(err error) string {
	var apiErr *repositoryMonitorGitHubAPIError
	if !errors.As(err, &apiErr) {
		return repositoryMonitorPublishFailureGitHubAPI
	}
	switch apiErr.StatusCode {
	case http.StatusForbidden:
		if repositoryMonitorGitHubErrorLooksRateLimited(apiErr.Body) {
			return repositoryMonitorPublishFailureGitHubAPI
		}
		return repositoryMonitorPublishFailureGitHubPermissionDenied
	case http.StatusUnauthorized, http.StatusNotFound:
		return repositoryMonitorPublishFailureGitHubPermissionDenied
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return repositoryMonitorPublishFailureGitHubPermanent
	default:
		return repositoryMonitorPublishFailureGitHubAPI
	}
}

func (r *RepositoryMonitorReconciler) listRepositoryMonitorPullRequestFiles(ctx context.Context, owner, repository, token string, number int64) ([]repositoryMonitorPullRequestFileResponse, error) {
	var files []repositoryMonitorPullRequestFileResponse
	for page := 1; ; page++ {
		pageFiles, err := r.fetchRepositoryMonitorPullRequestFilesPage(ctx, owner, repository, token, number, page)
		if err != nil {
			return nil, err
		}
		files = append(files, pageFiles...)
		if len(pageFiles) < repositoryMonitorGitHubPerPage {
			break
		}
	}
	return files, nil
}

// listRepositoryMonitorCompareFiles lists the changed files with patches for
// the exact base...head commit range through the compare endpoint, so the
// returned file set is bound to immutable SHAs rather than to whatever the
// pull request branch points at when the request is served. GitHub does not
// paginate the compare "files" array: it is returned on the first page only
// and capped at repositoryMonitorGitHubCompareMaxFiles entries, so the caller
// must reconcile the result against the pull request's changed-file total.
func (r *RepositoryMonitorReconciler) listRepositoryMonitorCompareFiles(ctx context.Context, owner, repository, token, baseSHA, headSHA string) ([]repositoryMonitorPullRequestFileResponse, error) {
	baseSHA, headSHA = strings.TrimSpace(baseSHA), strings.TrimSpace(headSHA)
	if baseSHA == "" || headSHA == "" {
		return nil, fmt.Errorf("pull request base and head SHAs are required to bind the review context")
	}
	return r.fetchRepositoryMonitorCompareFilesPage(ctx, owner, repository, token, baseSHA, headSHA, 1)
}

func (r *RepositoryMonitorReconciler) fetchRepositoryMonitorCompareFilesPage(ctx context.Context, owner, repository, token, baseSHA, headSHA string, page int) ([]repositoryMonitorPullRequestFileResponse, error) {
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/compare/%s...%s?per_page=%d&page=%d", baseURL, url.PathEscape(owner), url.PathEscape(repository), url.PathEscape(baseSHA), url.PathEscape(headSHA), repositoryMonitorGitHubPerPage, page)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	repositoryMonitorSetGitHubHeaders(req, token)
	resp, err := repositoryMonitorHTTPClient(r).Do(req)
	if err != nil {
		return nil, fmt.Errorf("GitHub compare request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	respBody, err := readRepositoryMonitorGitHubResponse(resp.Body, repositoryMonitorGitHubResponseLimit)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &repositoryMonitorGitHubAPIError{Operation: "compare request", StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	var response struct {
		Files []repositoryMonitorPullRequestFileResponse `json:"files"`
	}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, fmt.Errorf("failed to parse GitHub compare response: %w", err)
	}
	return response.Files, nil
}

func (r *RepositoryMonitorReconciler) fetchRepositoryMonitorPullRequestFilesPage(ctx context.Context, owner, repository, token string, number int64, page int) ([]repositoryMonitorPullRequestFileResponse, error) {
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/files?per_page=%d&page=%d", baseURL, url.PathEscape(owner), url.PathEscape(repository), number, repositoryMonitorGitHubPerPage, page)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	repositoryMonitorSetGitHubHeaders(req, token)
	resp, err := repositoryMonitorHTTPClient(r).Do(req)
	if err != nil {
		return nil, fmt.Errorf("GitHub pull request files request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	respBody, err := readRepositoryMonitorGitHubResponse(resp.Body, repositoryMonitorGitHubResponseLimit)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &repositoryMonitorGitHubAPIError{Operation: "pull request files request", StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	var response []repositoryMonitorPullRequestFileResponse
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, fmt.Errorf("failed to parse GitHub pull request files response: %w", err)
	}
	return response, nil
}

func (r *RepositoryMonitorReconciler) repositoryMonitorGitHubReviewMarkerMatch(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, owner, repository, token, headSHA string) (*repositoryMonitorPullRequestReviewResponse, string, error) {
	reviews, err := r.listRepositoryMonitorPullRequestReviews(ctx, owner, repository, token, item.Number)
	if err != nil {
		return nil, "", err
	}
	markerPrefix := repositoryMonitorReviewMarkerPrefix(monitor, item.Number, headSHA)
	for _, review := range reviews {
		if !strings.Contains(review.Body, markerPrefix) {
			continue
		}
		trusted, err := r.repositoryMonitorGitHubReviewMarkerTrustedByStore(ctx, monitor, item, headSHA, review.Body, markerPrefix)
		if err != nil {
			return nil, "", err
		}
		if trusted {
			return &review, repositoryMonitorPublishIDFromMarker(review.Body, markerPrefix), nil
		}
	}
	return nil, "", nil
}

func (r *RepositoryMonitorReconciler) repositoryMonitorGitHubReviewMarkerTrustedByStore(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, headSHA, body, markerPrefix string) (bool, error) {
	publishID := repositoryMonitorPublishIDFromMarker(body, markerPrefix)
	if publishID == "" {
		return false, nil
	}
	record, err := r.Store.GetReviewPublishRecord(ctx, monitor.Namespace, publishID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return record.MonitorName == monitor.Name &&
		record.ItemKind == repositoryMonitorPullRequestKind &&
		record.ItemNumber == item.Number &&
		record.HeadSHA == headSHA &&
		record.Phase != repositoryMonitorPublishPhaseSkipped, nil
}

func repositoryMonitorPublishIDFromMarker(body, markerPrefix string) string {
	start := strings.Index(body, markerPrefix)
	if start < 0 {
		return ""
	}
	end := strings.Index(body[start:], "-->")
	if end < 0 {
		return ""
	}
	marker := body[start : start+end]
	for field := range strings.FieldsSeq(marker) {
		if after, ok := strings.CutPrefix(field, "publish="); ok {
			return strings.Trim(after, " \t\n\r-<>")
		}
	}
	return ""
}

func (r *RepositoryMonitorReconciler) listRepositoryMonitorPullRequestReviews(ctx context.Context, owner, repository, token string, number int64) ([]repositoryMonitorPullRequestReviewResponse, error) {
	var reviews []repositoryMonitorPullRequestReviewResponse
	for page := 1; ; page++ {
		pageReviews, err := r.fetchRepositoryMonitorPullRequestReviewsPage(ctx, owner, repository, token, number, page)
		if err != nil {
			return nil, err
		}
		reviews = append(reviews, pageReviews...)
		if len(pageReviews) < repositoryMonitorGitHubPerPage {
			break
		}
	}
	return reviews, nil
}

func (r *RepositoryMonitorReconciler) fetchRepositoryMonitorPullRequestReviewsPage(ctx context.Context, owner, repository, token string, number int64, page int) ([]repositoryMonitorPullRequestReviewResponse, error) {
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/reviews?per_page=%d&page=%d", baseURL, url.PathEscape(owner), url.PathEscape(repository), number, repositoryMonitorGitHubPerPage, page)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	repositoryMonitorSetGitHubHeaders(req, token)
	resp, err := repositoryMonitorHTTPClient(r).Do(req)
	if err != nil {
		return nil, fmt.Errorf("GitHub pull request reviews request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	respBody, err := readRepositoryMonitorGitHubResponse(resp.Body, repositoryMonitorGitHubResponseLimit)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &repositoryMonitorGitHubAPIError{Operation: "pull request reviews request", StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	var response []repositoryMonitorPullRequestReviewResponse
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, fmt.Errorf("failed to parse GitHub pull request reviews response: %w", err)
	}
	return response, nil
}

func (r *RepositoryMonitorReconciler) createRepositoryMonitorPullRequestReview(ctx context.Context, owner, repository, token string, number int64, review repositoryMonitorPullRequestReviewRequest) (*repositoryMonitorPullRequestReviewResponse, error) {
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	payload, err := json.Marshal(review)
	if err != nil {
		return nil, err
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/reviews", baseURL, url.PathEscape(owner), url.PathEscape(repository), number)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	repositoryMonitorSetGitHubHeaders(req, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := repositoryMonitorHTTPClient(r).Do(req)
	if err != nil {
		return nil, fmt.Errorf("GitHub pull request review create request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	respBody, err := readRepositoryMonitorGitHubResponse(resp.Body, repositoryMonitorGitHubResponseLimit)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &repositoryMonitorGitHubAPIError{Operation: "pull request review create request", StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	var response repositoryMonitorPullRequestReviewResponse
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, fmt.Errorf("failed to parse GitHub pull request review create response: %w", err)
	}
	return &response, nil
}

func repositoryMonitorSetGitHubHeaders(req *http.Request, token string) {
	if strings.TrimSpace(token) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
}

func repositoryMonitorHTTPClient(r *RepositoryMonitorReconciler) *http.Client {
	if r != nil && r.HTTPClient != nil {
		return r.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second}
}
