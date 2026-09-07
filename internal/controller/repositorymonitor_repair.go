package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
)

const (
	repositoryMonitorRepairPhaseQueued              = "queued"
	repositoryMonitorRepairPhaseSucceeded           = "succeeded"
	repositoryMonitorRepairPhaseFailed              = "failed"
	repositoryMonitorRepairPRBudgetReason           = "repair_pr_budget_exhausted"
	repositoryMonitorRepairTaskCreateError          = "repair_task_create_failed"
	repositoryMonitorCommandIntentFix               = "fix"
	repositoryMonitorUpdateBranchOperation          = "update_branch"
	repositoryMonitorUpdateBranchSubmitting         = "submitting"
	repositoryMonitorUpdateBranchTimeout            = 2 * time.Minute
	repositoryMonitorUpdateBranchTimeoutReason      = "timed out waiting for GitHub to update the PR branch"
	repositoryMonitorUpdateBranchFailure            = "update_branch_failed"
	repositoryMonitorFieldBaseSHA                   = "baseSHA"
	repositoryMonitorFieldHeadSHA                   = "headSHA"
	repositoryMonitorValidationRetryExhaustedReason = "validation_retry_exhausted"
)

type repositoryMonitorPendingMutationProjectionError struct {
	projection string
	err        error
}

func (e *repositoryMonitorPendingMutationProjectionError) Error() string {
	return fmt.Sprintf("pending GitHub mutation %s projection failed: %v", e.projection, e.err)
}

func (e *repositoryMonitorPendingMutationProjectionError) Unwrap() error {
	return e.err
}

func pendingMutationProjectionError(projection string, err error) error {
	return &repositoryMonitorPendingMutationProjectionError{projection: projection, err: err}
}

type repositoryMonitorRepairValidationRetryState struct {
	associated bool
	exhausted  bool
}

func repositoryMonitorValidationRetryableAfterRepair(status string) bool {
	switch strings.TrimSpace(status) {
	case repositoryMonitorValidationStatusFailed, repositoryMonitorValidationStatusUnavailable:
		return true
	default:
		return false
	}
}

func (r *RepositoryMonitorReconciler) syncRepositoryMonitorRepairValidationAttempts(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, record *store.ReviewRecord) (repositoryMonitorRepairValidationRetryState, error) {
	if r == nil || r.Store == nil || monitor == nil || record == nil ||
		record.Kind != repositoryMonitorPullRequestKind || record.Number == 0 ||
		strings.TrimSpace(record.HeadSHA) == "" || strings.TrimSpace(record.ValidationStatus) == "" ||
		strings.TrimSpace(record.ValidationImage) == "" ||
		strings.TrimSpace(record.ValidationImage) != strings.TrimSpace(monitor.Spec.Validation.Image) {
		return repositoryMonitorRepairValidationRetryState{}, nil
	}

	job, err := r.repositoryMonitorRepairJobForValidation(ctx, monitor, record.Number, record.HeadSHA)
	if err != nil || job == nil {
		return repositoryMonitorRepairValidationRetryState{}, err
	}
	attempts, err := r.repositoryMonitorRepairValidationAttemptCount(ctx, monitor, job)
	if err != nil {
		return repositoryMonitorRepairValidationRetryState{}, err
	}
	exhausted := repositoryMonitorValidationRetryableAfterRepair(record.ValidationStatus) &&
		monitor.Spec.Repair.MaxValidationRetries != nil &&
		attempts > int(*monitor.Spec.Repair.MaxValidationRetries)
	desiredError := ""
	if exhausted {
		desiredError = repositoryMonitorValidationRetryExhaustedReason
	} else if job.LastError != repositoryMonitorValidationRetryExhaustedReason {
		desiredError = job.LastError
	}
	if job.ValidationAttempts != attempts || job.LastError != desiredError {
		job.ValidationAttempts = attempts
		job.LastError = desiredError
		if err := r.Store.UpdateRepairJob(ctx, job); err != nil {
			return repositoryMonitorRepairValidationRetryState{}, err
		}
	}
	return repositoryMonitorRepairValidationRetryState{associated: true, exhausted: exhausted}, nil
}

func (r *RepositoryMonitorReconciler) repositoryMonitorRepairJobForValidation(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, number int64, headSHA string) (*store.RepairJob, error) {
	cursor := ""
	for {
		jobs, next, err := r.Store.ListRepairJobs(ctx, store.RepairJobFilter{
			Namespace: monitor.Namespace, MonitorName: monitor.Name, PRNumber: number, Limit: 200, Cursor: cursor,
		})
		if err != nil {
			return nil, err
		}
		for i := range jobs {
			job := &jobs[i]
			if job.Phase == repositoryMonitorRepairPhaseSucceeded && job.CompletedAt != nil &&
				strings.TrimSpace(job.PushedSHA) == strings.TrimSpace(headSHA) {
				return job, nil
			}
		}
		if next == "" {
			return nil, nil
		}
		cursor = next
	}
}

func (r *RepositoryMonitorReconciler) repositoryMonitorRepairValidationAttemptCount(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, job *store.RepairJob) (int, error) {
	if job == nil || job.CompletedAt == nil {
		return 0, nil
	}
	attempts := 0
	cursor := ""
	for {
		records, next, err := r.Store.ListReviewRecords(ctx, store.ReviewRecordFilter{
			Namespace: monitor.Namespace, MonitorName: monitor.Name, Kind: repositoryMonitorPullRequestKind,
			Number: job.PRNumber, HeadSHA: job.PushedSHA, Limit: 200, Cursor: cursor,
		})
		if err != nil {
			return 0, err
		}
		for i := range records {
			record := &records[i]
			if record.CreatedAt.Before(*job.CompletedAt) ||
				strings.TrimSpace(record.ValidationImage) != strings.TrimSpace(monitor.Spec.Validation.Image) ||
				strings.TrimSpace(record.ValidationStatus) == "" {
				continue
			}
			attempts++
		}
		if next == "" {
			return attempts, nil
		}
		cursor = next
	}
}

func (r *RepositoryMonitorReconciler) repositoryMonitorRepairValidationRetryState(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, number int64, headSHA string) (repositoryMonitorRepairValidationRetryState, error) {
	if r == nil || r.Store == nil || monitor == nil {
		return repositoryMonitorRepairValidationRetryState{}, nil
	}
	job, err := r.repositoryMonitorRepairJobForValidation(ctx, monitor, number, headSHA)
	if err != nil || job == nil {
		return repositoryMonitorRepairValidationRetryState{}, err
	}
	exhausted := monitor.Spec.Repair.MaxValidationRetries != nil &&
		job.LastError == repositoryMonitorValidationRetryExhaustedReason &&
		job.ValidationAttempts > int(*monitor.Spec.Repair.MaxValidationRetries)
	return repositoryMonitorRepairValidationRetryState{associated: true, exhausted: exhausted}, nil
}

//nolint:gocyclo // PR command safety gates are intentionally explicit.
func (r *RepositoryMonitorReconciler) tryProcessPullRequestCommandRun(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, owner, repository string, pr repositoryMonitorPullRequest, item *store.MonitorItem) (bool, int, error) {
	if run == nil || strings.TrimSpace(run.CommandEventID) == "" || item == nil {
		return false, 0, nil
	}
	command, err := r.Store.GetCommandEvent(ctx, monitor.Namespace, run.CommandEventID)
	if err != nil {
		if errorsIsStoreNotFound(err) {
			return false, 0, nil
		}
		return false, 0, err
	}
	switch command.Intent {
	case repositoryMonitorCommandIntentStop:
		if err := r.cancelRepositoryMonitorTargetTasks(ctx, monitor, repositoryMonitorPullRequestKind, pr.Number, repositoryMonitorIssueSkipStoppedByCommand); err != nil {
			return true, 0, err
		}
		if _, err := r.Store.CancelWorkActions(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, pr.Number, repositoryMonitorIssueSkipStoppedByCommand); err != nil {
			return true, 0, err
		}
		if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "", repositoryMonitorCommandIntentStop, repositoryMonitorWorkActionStatusSucceeded, "blocked", "", repositoryMonitorIssueSkipStoppedByCommand); err != nil {
			return true, 0, err
		}
		item.RepairState = repositoryMonitorRepairPhaseFailed
		item.SkipReason = repositoryMonitorIssueSkipStoppedByCommand
		return true, 0, r.Store.UpsertMonitorItem(ctx, item)
	case repositoryMonitorCommandIntentResume:
		if repositoryMonitorBlockedLabel(monitor.Spec, pr.Labels) != "" {
			if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "", repositoryMonitorCommandIntentResume, repositoryMonitorWorkActionStatusBlocked, "resume_blocked", "", repositoryMonitorSkipReasonBlockedLabel); err != nil {
				return true, 0, err
			}
			return true, 0, nil
		}
		if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "", repositoryMonitorCommandIntentResume, repositoryMonitorWorkActionStatusSucceeded, "resumed", "", ""); err != nil {
			return true, 0, err
		}
		item.RepairState = ""
		item.SkipReason = ""
		return true, 0, r.Store.UpsertMonitorItem(ctx, item)
	case repositoryMonitorCommandIntentAutomerge:
		if cancelled, err := r.repositoryMonitorWorkActionCancelled(ctx, monitor, command.ID, repositoryMonitorCommandIntentAutomerge); err != nil || cancelled {
			return true, 0, err
		}
		handled, err := r.tryProcessPullRequestAutomergeCommand(ctx, monitor, run, command, owner, repository, pr, item)
		return handled, 0, err
	case "review":
		if blockedLabel := repositoryMonitorBlockedLabel(monitor.Spec, pr.Labels); blockedLabel != "" {
			item.LastVerdict = repositoryMonitorVerdictSkipped
			item.SkipReason = repositoryMonitorSkipReasonBlockedLabel
			if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "", "pr_review", repositoryMonitorWorkActionStatusBlocked, "review_blocked", "", repositoryMonitorSkipReasonBlockedLabel); err != nil {
				return true, 0, err
			}
			return true, 0, r.Store.UpsertMonitorItem(ctx, item)
		}
		if cancelled, err := r.repositoryMonitorWorkActionCancelled(ctx, monitor, command.ID, "review"); err != nil || cancelled {
			return true, 0, err
		}
		if monitor.Spec.Review.RequireGreenCI {
			ci, err := r.repositoryMonitorCheckCI(ctx, monitor, pr.HeadSHA)
			if err != nil {
				return true, 0, err
			}
			if !ci.passed {
				item.CIState = firstNonEmptyIssueAction(ci.reason, "ci_not_green")
				item.LastVerdict = repositoryMonitorVerdictSkipped
				item.SkipReason = item.CIState
				if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "", "pr_review", repositoryMonitorWorkActionStatusBlocked, "review_blocked", "", item.CIState); err != nil {
					return true, 0, err
				}
				return true, 0, r.Store.UpsertMonitorItem(ctx, item)
			}
			item.CIState = "passed"
		}
		token, err := r.repositoryMonitorGitHubToken(ctx, monitor)
		if err != nil {
			return true, 0, err
		}
		taskName, created, err := r.createRepositoryMonitorReviewTask(ctx, monitor, run, owner, repository, token, pr)
		if err != nil {
			return true, 0, err
		}
		item.LastVerdict = repositoryMonitorRunPhaseQueued
		item.LastReviewID = taskName
		item.SkipReason = ""
		if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "", "pr_review", repositoryMonitorWorkActionStatusRunning, "review_queued", taskName, ""); err != nil {
			return true, 0, err
		}
		if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
			return true, 0, err
		}
		if err := r.createMonitorEvent(ctx, monitor, run.ID, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "review_task_created", fmt.Sprintf("Pull request #%d review task queued by command", pr.Number), map[string]any{"taskName": taskName, "created": created}); err != nil {
			return true, 0, err
		}
		if created {
			return true, 1, nil
		}
		return true, 0, nil
	case repositoryMonitorCommandIntentUpdateBranch:
		reconcile, err := r.repositoryMonitorUpdateBranchRequiresReconciliation(ctx, monitor, command.ID)
		if err != nil {
			return true, 0, err
		}
		if !reconcile {
			if blockedLabel := repositoryMonitorBlockedLabel(monitor.Spec, pr.Labels); blockedLabel != "" {
				item.RepairState = repositoryMonitorRepairPhaseFailed
				item.SkipReason = repositoryMonitorSkipReasonBlockedLabel
				if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "", command.Intent, repositoryMonitorWorkActionStatusBlocked, "update_branch_blocked", "", repositoryMonitorSkipReasonBlockedLabel); err != nil {
					return true, 0, err
				}
				return true, 0, r.Store.UpsertMonitorItem(ctx, item)
			}
			if cancelled, err := r.repositoryMonitorWorkActionCancelled(ctx, monitor, command.ID, command.Intent); err != nil || cancelled {
				return true, 0, err
			}
		}
		return r.tryProcessPullRequestUpdateBranchCommand(ctx, monitor, run, command, owner, repository, pr, item)
	case repositoryMonitorCommandIntentFix, repositoryMonitorCommandIntentFixCI:
		if blockedLabel := repositoryMonitorBlockedLabel(monitor.Spec, pr.Labels); blockedLabel != "" {
			item.RepairState = repositoryMonitorRepairPhaseFailed
			item.SkipReason = repositoryMonitorSkipReasonBlockedLabel
			if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "", repositoryMonitorRepairWorkflowActionKind(command.Intent), repositoryMonitorWorkActionStatusBlocked, "repair_blocked", "", repositoryMonitorSkipReasonBlockedLabel); err != nil {
				return true, 0, err
			}
			return true, 0, r.Store.UpsertMonitorItem(ctx, item)
		}
		if cancelled, err := r.repositoryMonitorWorkActionCancelled(ctx, monitor, command.ID, repositoryMonitorRepairWorkflowActionKind(command.Intent)); err != nil || cancelled {
			return true, 0, err
		}
		monitoredRepo := owner + "/" + repository
		currentRepairJobID := "repair-" + repositoryMonitorShortHash(command.ID)
		reason, repairCountPR, repairCountHead, err := r.repositoryMonitorRepairPolicy(ctx, monitor, monitoredRepo, pr, currentRepairJobID)
		if err != nil {
			return true, 0, err
		}
		if reason != "" {
			item.RepairState = repositoryMonitorRepairPhaseFailed
			item.SkipReason = reason
			if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "", repositoryMonitorRepairWorkflowActionKind(command.Intent), repositoryMonitorWorkActionStatusBlocked, "repair_blocked", "", reason); err != nil {
				return true, 0, err
			}
			if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
				return true, 0, err
			}
			return true, 0, r.createMonitorEvent(ctx, monitor, run.ID, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "repair_blocked", fmt.Sprintf("Pull request #%d repair blocked: %s", pr.Number, reason), map[string]any{"intent": command.Intent, "reason": reason})
		}
		created, err := r.createRepositoryMonitorRepairTask(ctx, monitor, run, command, owner, repository, pr, item, repairCountPR+1, repairCountHead+1)
		if err != nil {
			return true, created, err
		}
		if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "", repositoryMonitorRepairWorkflowActionKind(command.Intent), repositoryMonitorWorkActionStatusRunning, repositoryMonitorRepairPhaseQueued, repositoryMonitorRepairTaskName(monitor, pr, command), ""); err != nil {
			return true, created, err
		}
		return true, created, nil
	default:
		return false, 0, nil
	}
}

func (r *RepositoryMonitorReconciler) tryProcessPullRequestUpdateBranchCommand(
	ctx context.Context,
	monitor *corev1alpha1.RepositoryMonitor,
	run *store.MonitorRun,
	command *store.CommandEvent,
	owner, repository string,
	pr repositoryMonitorPullRequest,
	item *store.MonitorItem,
) (bool, int, error) {
	mutationID := repositoryMonitorUpdateBranchMutationID(command.ID)
	mutation, err := r.Store.GetGitHubMutationRecord(ctx, monitor.Namespace, mutationID)
	if errorsIsStoreNotFound(err) {
		baseSHA := strings.TrimSpace(pr.BaseSHA)
		if baseSHA == "" {
			return true, 0, fmt.Errorf("pull request base SHA is required for update-branch")
		}
		mutation = &store.GitHubMutationRecord{
			ID:             mutationID,
			RunID:          run.ID,
			CommandEventID: command.ID,
			Operation:      repositoryMonitorUpdateBranchOperation,
			TargetKind:     repositoryMonitorPullRequestKind,
			TargetNumber:   pr.Number,
			TargetSHA:      pr.HeadSHA,
			Reason:         command.Intent,
			GitHubURL:      pr.BaseBranch,
			ExternalID:     baseSHA,
			Status:         repositoryMonitorAutomergeStateStarted,
		}
		if err := r.recordRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
			return true, 0, err
		}
	} else if err != nil {
		return true, 0, err
	}
	if reason := repositoryMonitorUpdateBranchMutationMismatch(mutation, command, owner+"/"+repository, pr); reason != "" {
		return true, 0, r.failRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, reason)
	}
	if mutation.Status == repositoryMonitorRunPhaseSucceeded {
		projectedPR := pr
		projectedPR.BaseSHA = mutation.ExternalID
		return true, 0, r.completeRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, projectedPR)
	}
	containsBase, err := r.repositoryMonitorRepoHeadContainsBase(ctx, monitor, owner+"/"+repository, pr.BaseSHA, pr.HeadSHA)
	if err != nil {
		return true, 0, err
	}
	if containsBase {
		return true, 0, r.completeRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, pr)
	}
	switch mutation.Status {
	case repositoryMonitorRunPhaseFailed:
		return true, 0, r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, command.HeadSHA, "", command.Intent, repositoryMonitorWorkActionStatusFailed, repositoryMonitorRepairPhaseFailed, "", mutation.Error)
	case repositoryMonitorAutomergeStatePending:
		return r.waitForRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, pr, repositoryMonitorAutomergeStatePending)
	case repositoryMonitorUpdateBranchSubmitting:
		if repositoryMonitorUpdateBranchTimedOut(mutation) {
			return true, 0, r.failRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, repositoryMonitorUpdateBranchTimeoutReason)
		}
		cancelled, err := r.repositoryMonitorWorkActionCancelled(ctx, monitor, command.ID, command.Intent)
		if err != nil {
			return true, 0, err
		}
		if cancelled || strings.TrimSpace(mutation.Error) != "" || pr.HeadSHA != mutation.TargetSHA {
			return r.waitForRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, pr, repositoryMonitorUpdateBranchSubmitting)
		}
	case repositoryMonitorAutomergeStateStarted:
		cancelled, err := r.repositoryMonitorWorkActionCancelled(ctx, monitor, command.ID, command.Intent)
		if err != nil || cancelled {
			return true, 0, err
		}
	default:
		return true, 0, r.failRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, "update-branch mutation has an invalid state")
	}

	token, err := r.repositoryMonitorForgeToken(ctx, monitor)
	if err != nil {
		return true, 0, err
	}
	if mutation.Status == repositoryMonitorAutomergeStateStarted {
		submittingAt := time.Now()
		mutation.Status = repositoryMonitorUpdateBranchSubmitting
		mutation.PendingAt = &submittingAt
		mutation.GitHubRequestID = ""
		mutation.Error = ""
		if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
			return true, 0, pendingMutationProjectionError("mutation submission", err)
		}
		cancelled, err := r.repositoryMonitorWorkActionCancelled(ctx, monitor, command.ID, command.Intent)
		if err != nil {
			return true, 0, err
		}
		if cancelled {
			return r.waitForRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, pr, repositoryMonitorUpdateBranchSubmitting)
		}
	}

	requestID, err := r.updateRepositoryMonitorPullRequestBranch(ctx, owner, repository, token, pr.Number, command.HeadSHA)
	if err != nil {
		return r.recordRepositoryMonitorUpdateBranchSubmissionError(ctx, monitor, run, command, item, mutation, pr, err)
	}
	mutation.Status = repositoryMonitorAutomergeStatePending
	pendingAt := time.Now()
	mutation.PendingAt = &pendingAt
	mutation.GitHubRequestID = requestID
	mutation.Error = ""
	if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
		return true, 0, pendingMutationProjectionError("mutation record", err)
	}
	item.RepairState = repositoryMonitorRepairPhaseQueued
	item.SkipReason = ""
	if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
		return true, 0, pendingMutationProjectionError("monitor item", err)
	}
	if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, command.HeadSHA, "", command.Intent, repositoryMonitorWorkActionStatusRunning, repositoryMonitorAutomergeStatePending, "", ""); err != nil {
		return true, 0, pendingMutationProjectionError("work action", err)
	}
	if err := r.createMonitorEvent(ctx, monitor, run.ID, repositoryMonitorPullRequestKind, pr.Number, command.HeadSHA, "update_branch_started", fmt.Sprintf("Pull request #%d branch update started", pr.Number), map[string]any{repositoryMonitorFieldBaseSHA: mutation.ExternalID}); err != nil {
		return true, 0, pendingMutationProjectionError("event", err)
	}
	return true, 0, nil
}

func (r *RepositoryMonitorReconciler) recordRepositoryMonitorUpdateBranchSubmissionError(
	ctx context.Context,
	monitor *corev1alpha1.RepositoryMonitor,
	run *store.MonitorRun,
	command *store.CommandEvent,
	item *store.MonitorItem,
	mutation *store.GitHubMutationRecord,
	pr repositoryMonitorPullRequest,
	submissionErr error,
) (bool, int, error) {
	failureState := repositoryMonitorRunFailureState(submissionErr)
	if repositoryMonitorFailedCommandRunRetryable("[" + failureState + "]") {
		mutation.Status = repositoryMonitorUpdateBranchSubmitting
		if _, rejected := errors.AsType[*repositoryMonitorGitHubAPIError](submissionErr); rejected {
			mutation.Status = repositoryMonitorAutomergeStateStarted
			mutation.PendingAt = nil
		}
		mutation.GitHubRequestID = ""
		mutation.Error = submissionErr.Error()
		if updateErr := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); updateErr != nil {
			return true, 0, fmt.Errorf("update branch retry failed: %w; additionally failed to update mutation audit: %v", submissionErr, updateErr)
		}
		item.RepairState = repositoryMonitorRepairPhaseQueued
		item.SkipReason = failureState
		if updateErr := r.Store.UpsertMonitorItem(ctx, item); updateErr != nil {
			return true, 0, updateErr
		}
		if actionErr := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, command.HeadSHA, "", command.Intent, repositoryMonitorWorkActionStatusRunning, failureState, "", submissionErr.Error()); actionErr != nil {
			return true, 0, actionErr
		}
		return true, 0, submissionErr
	}
	mutation.Status = repositoryMonitorRunPhaseFailed
	mutation.Error = submissionErr.Error()
	if updateErr := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); updateErr != nil {
		return true, 0, fmt.Errorf("update branch failed: %w; additionally failed to update mutation audit: %v", submissionErr, updateErr)
	}
	item.RepairState = repositoryMonitorRepairPhaseFailed
	item.SkipReason = repositoryMonitorUpdateBranchFailure
	if updateErr := r.Store.UpsertMonitorItem(ctx, item); updateErr != nil {
		return true, 0, updateErr
	}
	if actionErr := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, command.HeadSHA, "", command.Intent, repositoryMonitorWorkActionStatusFailed, repositoryMonitorRepairPhaseFailed, "", submissionErr.Error()); actionErr != nil {
		return true, 0, actionErr
	}
	return true, 0, submissionErr
}

func (r *RepositoryMonitorReconciler) waitForRepositoryMonitorUpdateBranch(
	ctx context.Context,
	monitor *corev1alpha1.RepositoryMonitor,
	run *store.MonitorRun,
	command *store.CommandEvent,
	item *store.MonitorItem,
	mutation *store.GitHubMutationRecord,
	pr repositoryMonitorPullRequest,
	phase string,
) (bool, int, error) {
	if repositoryMonitorUpdateBranchTimedOut(mutation) {
		return true, 0, r.failRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, repositoryMonitorUpdateBranchTimeoutReason)
	}
	item.RepairState = repositoryMonitorRepairPhaseQueued
	item.SkipReason = ""
	if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
		return true, 0, pendingMutationProjectionError("monitor item", err)
	}
	if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, command.HeadSHA, "", command.Intent, repositoryMonitorWorkActionStatusRunning, phase, "", ""); err != nil {
		return true, 0, pendingMutationProjectionError("work action", err)
	}
	return true, 0, nil
}

func repositoryMonitorUpdateBranchMutationID(commandID string) string {
	return "ghmut-" + repositoryMonitorShortHash(commandID+"-update-branch")
}

func (r *RepositoryMonitorReconciler) repositoryMonitorUpdateBranchRequiresReconciliation(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, commandID string) (bool, error) {
	mutation, err := r.Store.GetGitHubMutationRecord(ctx, monitor.Namespace, repositoryMonitorUpdateBranchMutationID(commandID))
	if errorsIsStoreNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return mutation.Status == repositoryMonitorUpdateBranchSubmitting || mutation.Status == repositoryMonitorAutomergeStatePending || mutation.Status == repositoryMonitorRunPhaseSucceeded, nil
}

func repositoryMonitorUpdateBranchMutationMismatch(mutation *store.GitHubMutationRecord, command *store.CommandEvent, repository string, pr repositoryMonitorPullRequest) string {
	if mutation == nil || command == nil {
		return "update-branch mutation identity is missing"
	}
	if reason := repositoryMonitorCommandRepositoryMismatch(command, repository); reason != "" {
		return reason
	}
	if mutation.CommandEventID != command.ID || mutation.Operation != repositoryMonitorUpdateBranchOperation || mutation.TargetKind != repositoryMonitorPullRequestKind || mutation.TargetNumber != pr.Number {
		return "update-branch mutation identity does not match the command"
	}
	if strings.TrimSpace(mutation.TargetSHA) == "" || mutation.TargetSHA != command.HeadSHA {
		return "update-branch mutation expected head does not match the command"
	}
	if strings.TrimSpace(mutation.ExternalID) == "" || strings.TrimSpace(mutation.GitHubURL) == "" {
		return "update-branch mutation is missing its captured base revision"
	}
	if mutation.GitHubURL != pr.BaseBranch {
		return "pull request base branch changed during update"
	}
	return ""
}

func repositoryMonitorCommandRepositoryMismatch(command *store.CommandEvent, repository string) string {
	if command == nil || strings.TrimSpace(command.Repo) == "" || !strings.EqualFold(strings.TrimSpace(command.Repo), strings.TrimSpace(repository)) {
		return "update-branch command repository does not match the current monitor repository"
	}
	return ""
}

func (r *RepositoryMonitorReconciler) reconcileRepositoryMonitorCompletedUpdateBranch(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, pullRequests []repositoryMonitorPullRequest) (bool, error) {
	if run == nil || strings.TrimSpace(run.CommandEventID) == "" || run.TargetNumber == 0 {
		return false, nil
	}
	command, err := r.Store.GetCommandEvent(ctx, monitor.Namespace, run.CommandEventID)
	if err != nil || command.Intent != repositoryMonitorCommandIntentUpdateBranch {
		return false, err
	}
	mutation, err := r.Store.GetGitHubMutationRecord(ctx, monitor.Namespace, repositoryMonitorUpdateBranchMutationID(command.ID))
	if errorsIsStoreNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var current *repositoryMonitorPullRequest
	for i := range pullRequests {
		if pullRequests[i].Number == run.TargetNumber {
			current = &pullRequests[i]
			break
		}
	}
	if current == nil {
		return false, nil
	}
	owner, repository, err := repositoryMonitorOwnerRepository(monitor)
	if err != nil {
		return true, err
	}
	if reason := repositoryMonitorUpdateBranchMutationMismatch(mutation, command, owner+"/"+repository, *current); reason != "" {
		item, itemErr := r.Store.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, fmt.Sprintf("%d", current.Number))
		if itemErr != nil {
			return false, itemErr
		}
		return true, r.failRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, reason)
	}
	if mutation.Status == repositoryMonitorRunPhaseSucceeded {
		existing, itemErr := r.Store.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, fmt.Sprintf("%d", current.Number))
		if itemErr != nil {
			return true, itemErr
		}
		projectedPR := *current
		projectedPR.BaseSHA = mutation.ExternalID
		item := repositoryMonitorItemFromPullRequest(monitor, projectedPR, existing)
		return true, r.completeRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, projectedPR)
	}
	verificationBaseSHA := current.BaseSHA
	if current.Merged {
		verificationBaseSHA = mutation.ExternalID
	}
	containsBase, err := r.repositoryMonitorRepoHeadContainsBase(ctx, monitor, owner+"/"+repository, verificationBaseSHA, current.HeadSHA)
	if err != nil {
		return true, err
	}
	existing, err := r.Store.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, fmt.Sprintf("%d", current.Number))
	if err != nil {
		return true, err
	}
	item := repositoryMonitorItemFromPullRequest(monitor, *current, existing)
	if containsBase {
		return true, r.completeRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, *current)
	}
	if current.Merged {
		return true, r.failRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, "merged PR head does not contain the captured base revision")
	}
	if !strings.EqualFold(strings.TrimSpace(current.State), repositoryMonitorItemStateOpen) {
		return true, r.failRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, "pull request closed before the branch update completed")
	}
	if current.HeadSHA != mutation.TargetSHA {
		return true, r.failRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, "updated PR head does not contain the live base revision")
	}
	if repositoryMonitorUpdateBranchTimedOut(mutation) {
		return true, r.failRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, repositoryMonitorUpdateBranchTimeoutReason)
	}
	return false, nil
}

func repositoryMonitorUpdateBranchDeadline(mutation *store.GitHubMutationRecord) time.Time {
	if mutation == nil || (mutation.Status != repositoryMonitorUpdateBranchSubmitting && mutation.Status != repositoryMonitorAutomergeStatePending) {
		return time.Time{}
	}
	pendingSince := mutation.CreatedAt
	if mutation.PendingAt != nil && !mutation.PendingAt.IsZero() {
		pendingSince = *mutation.PendingAt
	}
	if pendingSince.IsZero() {
		return time.Time{}
	}
	return pendingSince.Add(repositoryMonitorUpdateBranchTimeout)
}

func repositoryMonitorUpdateBranchTimedOut(mutation *store.GitHubMutationRecord) bool {
	deadline := repositoryMonitorUpdateBranchDeadline(mutation)
	return !deadline.IsZero() && !time.Now().Before(deadline)
}

func (r *RepositoryMonitorReconciler) completeRepositoryMonitorUpdateBranch(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, command *store.CommandEvent, item *store.MonitorItem, mutation *store.GitHubMutationRecord, pr repositoryMonitorPullRequest) error {
	mutation.Status = repositoryMonitorRunPhaseSucceeded
	mutation.Error = ""
	if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
		return pendingMutationProjectionError("mutation record", err)
	}
	item.HeadSHA = pr.HeadSHA
	item.BaseSHA = pr.BaseSHA
	if pr.Merged {
		item.State = repositoryMonitorAutomergeStateMerged
	}
	item.RepairState = repositoryMonitorRepairPhaseSucceeded
	item.SkipReason = ""
	if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
		return pendingMutationProjectionError("monitor item", err)
	}
	// The terminal work action stops command reconciliation. Persist its
	// success event first, using the mutation identity to make retries safe
	// even if the event committed but its response or the next write failed.
	eventID := "mevt-" + mutation.ID + "-succeeded"
	events, _, err := r.Store.ListMonitorEvents(ctx, store.MonitorEventFilter{Namespace: monitor.Namespace, ID: eventID, Limit: 1})
	if err != nil {
		return pendingMutationProjectionError("event lookup", err)
	}
	if len(events) == 0 {
		metadata, err := json.Marshal(map[string]string{repositoryMonitorFieldBaseSHA: pr.BaseSHA, repositoryMonitorFieldHeadSHA: pr.HeadSHA})
		if err != nil {
			return pendingMutationProjectionError("event metadata", err)
		}
		if err := r.Store.CreateMonitorEvent(ctx, &store.MonitorEvent{
			ID: eventID, MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name, RunID: run.ID,
			ItemKind: repositoryMonitorPullRequestKind, ItemNumber: pr.Number, ItemSHA: pr.HeadSHA,
			EventType: "update_branch_succeeded", Actor: "controller", //nolint:goconst // Audit actors are independent of workspace namespace strategies.
			Summary: fmt.Sprintf("Pull request #%d branch updated", pr.Number), MetadataJSON: string(metadata),
		}); err != nil {
			return pendingMutationProjectionError("event", err)
		}
	}
	if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, pr.Number, command.HeadSHA, "", command.Intent, repositoryMonitorWorkActionStatusSucceeded, repositoryMonitorRepairPhaseSucceeded, "", ""); err != nil {
		return pendingMutationProjectionError("work action", err)
	}
	return nil
}

func (r *RepositoryMonitorReconciler) failRepositoryMonitorUpdateBranch(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, command *store.CommandEvent, item *store.MonitorItem, mutation *store.GitHubMutationRecord, reason string) error {
	mutation.Status = repositoryMonitorRunPhaseFailed
	mutation.Error = reason
	if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
		return err
	}
	item.RepairState = repositoryMonitorRepairPhaseFailed
	item.SkipReason = repositoryMonitorUpdateBranchFailure
	if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
		return err
	}
	return r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, repositoryMonitorPullRequestKind, item.Number, command.HeadSHA, "", command.Intent, repositoryMonitorWorkActionStatusFailed, repositoryMonitorRepairPhaseFailed, "", reason)
}

func (r *RepositoryMonitorReconciler) terminalizeRepositoryMonitorUpdateBranch(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, command store.CommandEvent, reason string) (bool, error) {
	mutation, err := r.Store.GetGitHubMutationRecord(ctx, monitor.Namespace, repositoryMonitorUpdateBranchMutationID(command.ID))
	if err != nil && !errorsIsStoreNotFound(err) {
		return false, err
	}
	owner, repository, repositoryErr := repositoryMonitorOwnerRepository(monitor)
	if repositoryErr != nil {
		return false, repositoryErr
	}
	if repositoryReason := repositoryMonitorCommandRepositoryMismatch(&command, owner+"/"+repository); repositoryReason != "" {
		if mutation != nil {
			mutation.Status = repositoryMonitorRunPhaseFailed
			mutation.Error = repositoryReason
			if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	if mutation != nil && mutation.Status == repositoryMonitorRunPhaseSucceeded {
		item, itemErr := r.Store.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, fmt.Sprintf("%d", command.Number))
		if errorsIsStoreNotFound(itemErr) {
			return true, nil
		}
		if itemErr != nil {
			return true, itemErr
		}
		item.RepairState = repositoryMonitorRepairPhaseSucceeded
		item.SkipReason = ""
		return true, r.Store.UpsertMonitorItem(ctx, item)
	}
	if mutation != nil {
		mutation.Status = repositoryMonitorRunPhaseFailed
		mutation.Error = reason
		if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
			return false, err
		}
	}
	item, err := r.Store.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, fmt.Sprintf("%d", command.Number))
	if errorsIsStoreNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	item.RepairState = repositoryMonitorRepairPhaseFailed
	item.SkipReason = repositoryMonitorUpdateBranchFailure
	return false, r.Store.UpsertMonitorItem(ctx, item)
}

func (r *RepositoryMonitorReconciler) updateRepositoryMonitorPullRequestBranch(ctx context.Context, owner, repository, token string, number int64, expectedHeadSHA string) (string, error) {
	payload, err := json.Marshal(map[string]string{"expected_head_sha": strings.TrimSpace(expectedHeadSHA)})
	if err != nil {
		return "", err
	}
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/update-branch", baseURL, url.PathEscape(owner), url.PathEscape(repository), number)
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, strings.NewReader(string(payload)))
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	response, err := repositoryMonitorHTTPClient(r).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close() //nolint:errcheck
	body, err := readRepositoryMonitorGitHubResponse(response.Body, repositoryMonitorGitHubResponseLimit)
	if err != nil {
		return "", err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", &repositoryMonitorGitHubAPIError{Operation: "update pull request branch", StatusCode: response.StatusCode, Body: string(body)}
	}
	return strings.TrimSpace(response.Header.Get("X-GitHub-Request-Id")), nil
}

func (r *RepositoryMonitorReconciler) repositoryMonitorRepairPolicy(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, monitoredRepo string, pr repositoryMonitorPullRequest, currentJobID string) (string, int, int, error) {
	if monitor == nil || !monitor.Spec.Repair.Enabled {
		return "repair_disabled", 0, 0, nil
	}
	if monitor.Spec.Agents.Repairer == nil || strings.TrimSpace(monitor.Spec.Agents.Repairer.Name) == "" {
		return "missing_repairer_agent", 0, 0, nil
	}
	if !strings.EqualFold(strings.TrimSpace(pr.HeadRepo), strings.TrimSpace(monitoredRepo)) {
		return "fork_pr_repair_not_writable", 0, 0, nil
	}
	if r.Store == nil {
		return "", 0, 0, nil
	}
	repairCountPR := 0
	repairCountHead := 0
	cursor := ""
	for {
		jobs, next, err := r.Store.ListRepairJobs(ctx, store.RepairJobFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, PRNumber: pr.Number, Limit: 200, Cursor: cursor})
		if err != nil {
			return "", 0, 0, err
		}
		for _, job := range jobs {
			if strings.TrimSpace(currentJobID) != "" && job.ID == currentJobID {
				continue
			}
			consumesBudget, err := r.repositoryMonitorRepairJobConsumesBudget(ctx, monitor.Namespace, &job)
			if err != nil {
				return "", 0, 0, err
			}
			if !consumesBudget {
				continue
			}
			repairCountPR++
			if strings.TrimSpace(job.HeadSHA) == strings.TrimSpace(pr.HeadSHA) {
				repairCountHead++
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if max := monitor.Spec.Repair.MaxRepairsPerPR; max != nil && repairCountPR >= int(*max) {
		return repositoryMonitorRepairPRBudgetReason, repairCountPR, repairCountHead, nil
	}
	if max := monitor.Spec.Repair.MaxRepairsPerHead; max != nil && repairCountHead >= int(*max) {
		return "repair_head_budget_exhausted", repairCountPR, repairCountHead, nil
	}
	return "", repairCountPR, repairCountHead, nil
}

func (r *RepositoryMonitorReconciler) repositoryMonitorRepairJobConsumesBudget(ctx context.Context, namespace string, job *store.RepairJob) (bool, error) {
	if job == nil {
		return false, nil
	}
	if strings.TrimSpace(job.Phase) != repositoryMonitorRepairPhaseQueued {
		return true, nil
	}
	if strings.TrimSpace(job.TaskName) == "" {
		return false, nil
	}
	if r.Client == nil {
		return true, nil
	}
	var task corev1alpha1.Task
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: job.TaskName}, &task); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *RepositoryMonitorReconciler) createRepositoryMonitorRepairTask(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, command *store.CommandEvent, owner, repository string, pr repositoryMonitorPullRequest, item *store.MonitorItem, repairCountPR, repairCountHead int) (int, error) {
	credentialRefs, err := repositoryMonitorCredentialRefsForWrite(monitor)
	if err != nil {
		return 0, err
	}
	monitoredRepo := owner + "/" + repository
	taskName := repositoryMonitorRepairTaskName(monitor, pr, command)
	job := &store.RepairJob{
		ID:               "repair-" + repositoryMonitorShortHash(command.ID),
		MonitorNamespace: monitor.Namespace,
		MonitorName:      monitor.Name,
		Repo:             monitoredRepo,
		PRNumber:         pr.Number,
		Intent:           command.Intent,
		Source:           command.Source,
		HeadSHA:          pr.HeadSHA,
		BaseSHA:          pr.BaseSHA,
		BaseBranch:       pr.BaseBranch,
		Phase:            repositoryMonitorRepairPhaseQueued,
		RepairCountPR:    repairCountPR,
		RepairCountHead:  repairCountHead,
		TaskName:         taskName,
		Branch:           pr.HeadBranch,
	}
	if err := r.Store.CreateRepairJob(ctx, job); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "constraint") {
			return 0, err
		}
		existing, getErr := r.Store.GetRepairJob(ctx, monitor.Namespace, job.ID)
		if getErr != nil {
			return 0, getErr
		}
		job = existing
	}
	pushMutationID := "ghmut-" + repositoryMonitorShortHash(job.ID+"-push")
	if _, err := r.ensureRepositoryMonitorGitHubMutationStarted(ctx, monitor, &store.GitHubMutationRecord{ID: pushMutationID, CommandEventID: command.ID, Operation: "push_branch", TargetKind: repositoryMonitorPullRequestKind, TargetNumber: pr.Number, TargetSHA: pr.HeadSHA, Reason: command.Intent, GitHubURL: pr.HeadBranch}); err != nil {
		return 0, err
	}
	priority := int32(820)
	timeout := metav1.Duration{Duration: repositoryMonitorReviewTaskTimeout}
	repairer := *monitor.Spec.Agents.Repairer
	workspace := &corev1alpha1.WorkspaceConfig{
		Intent:                       corev1alpha1.WorkspaceIntentWrite,
		GitRepo:                      repositoryMonitorHTTPSCloneURL(owner, repository),
		Branch:                       pr.HeadBranch,
		Ref:                          pr.HeadSHA,
		ReadCredentialRef:            workspaceCredentialReference(credentialRefs.read),
		PublicationGitRepo:           repositoryMonitorHTTPSCloneURL(owner, repository),
		PublicationReadCredentialRef: workspaceCredentialReference(credentialRefs.publicationRead),
		PublicationCredentialRef:     workspaceCredentialReference(credentialRefs.publication),
		ForgeCredentialRef:           workspaceCredentialReference(credentialRefs.forge),
		PRBaseBranch:                 pr.BaseBranch,
		PushBranch:                   pr.HeadBranch,
		ExpectedRemoteSHA:            pr.HeadSHA,
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskName,
			Namespace: monitor.Namespace,
			Labels: map[string]string{
				labels.LabelManaged:           "true",
				labels.LabelCreatedBy:         "repository-monitor",
				labels.LabelRepositoryMonitor: labels.SelectorValue(monitor.Name),
				labels.LabelMonitorRun:        labels.SelectorValue(run.ID),
				labels.LabelGitHubRepository:  labels.SelectorValue(monitoredRepo),
				labels.LabelGitHubTarget:      labels.SelectorValue(repositoryMonitorPullRequestKind),
				labels.LabelGitHubNumber:      labels.SelectorValue(strconv.FormatInt(pr.Number, 10)),
			},
			Annotations: map[string]string{
				labels.AnnotationRepositoryMonitorName:    monitor.Name,
				labels.AnnotationMonitorRunID:             run.ID,
				labels.AnnotationMonitorItemKind:          repositoryMonitorPullRequestKind,
				labels.AnnotationMonitorItemNumber:        strconv.FormatInt(pr.Number, 10),
				labels.AnnotationMonitorHeadSHA:           pr.HeadSHA,
				labels.AnnotationGitHubRepository:         monitoredRepo,
				repositoryMonitorIssueAnnotationCommandID: command.ID,
				labels.AnnotationAgentRuntimeAuthOnly:     scheduledRunLabelValue,
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:      corev1alpha1.TaskTypeAgent,
			AgentRef:  &repairer,
			Prompt:    buildRepositoryMonitorRepairPrompt(command.Intent, monitoredRepo, pr, item),
			Timeout:   &timeout,
			Priority:  &priority,
			Workspace: workspace,
			SessionRef: &corev1alpha1.SessionReference{
				Name: repositoryMonitorPublicationSessionName(monitor, pr.HeadBranch), Create: true, Append: false,
			},
		},
	}
	if err := controllerutil.SetControllerReference(monitor, task, r.Scheme); err != nil {
		return 0, err
	}
	created := 1
	if err := r.Create(ctx, task); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			job.Phase = repositoryMonitorRepairPhaseQueued
			job.LastError = repositoryMonitorRepairTaskCreateError
			job.CompletedAt = nil
			_ = r.Store.UpdateRepairJob(ctx, job)
			return 0, err
		}
		created = 0
	}
	if job.LastError == repositoryMonitorRepairTaskCreateError {
		job.Phase = repositoryMonitorRepairPhaseQueued
		job.LastError = ""
		job.CompletedAt = nil
		if err := r.Store.UpdateRepairJob(ctx, job); err != nil {
			return created, err
		}
	}
	item.RepairState = repositoryMonitorRepairPhaseQueued
	item.AutomergeState = ""
	item.SkipReason = ""
	if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
		return created, err
	}
	return created, r.createMonitorEvent(ctx, monitor, run.ID, repositoryMonitorPullRequestKind, pr.Number, pr.HeadSHA, "repair_task_created", fmt.Sprintf("Pull request #%d %s repair task queued", pr.Number, command.Intent), map[string]any{"taskName": taskName, "intent": command.Intent})
}

func repositoryMonitorRepairWorkflowActionKind(intent string) string {
	if strings.TrimSpace(intent) == repositoryMonitorCommandIntentFix {
		return "pr_repair"
	}
	return strings.TrimSpace(intent)
}

func repositoryMonitorRepairTaskName(monitor *corev1alpha1.RepositoryMonitor, pr repositoryMonitorPullRequest, command *store.CommandEvent) string {
	return repositoryMonitorBoundedDNSName(fmt.Sprintf("monrepair-%s-%d-%s", monitor.Name, pr.Number, command.ID), 63)
}

func buildRepositoryMonitorRepairPrompt(intent, repo string, pr repositoryMonitorPullRequest, item *store.MonitorItem) string {
	payload := map[string]any{"schemaVersion": "orka.prRepair.input.v1", "repo": repo, "prNumber": pr.Number, repositoryMonitorFieldHeadSHA: pr.HeadSHA, "intent": intent, "lastVerdict": item.LastVerdict, "skipReason": item.SkipReason} //nolint:goconst // Stable JSON field names mirror the prompt schema.
	payloadJSON, _ := json.MarshalIndent(payload, "", "  ")
	return fmt.Sprintf("Repair this exact pull request head for intent %q. Keep scope limited, run relevant validation, and leave final changes for Orka to commit and push to the configured push branch. Do not merge or close the PR.\n\nInput:\n%s\n", intent, string(payloadJSON))
}

func (r *RepositoryMonitorReconciler) repositoryMonitorHeadContainsBase(
	ctx context.Context,
	monitor *corev1alpha1.RepositoryMonitor,
	job *store.RepairJob,
) (bool, error) {
	if monitor == nil || job == nil {
		return false, fmt.Errorf("monitor and repair job are required")
	}
	owner, repository, ok := strings.Cut(strings.TrimSpace(job.Repo), "/")
	if !ok || owner == "" || repository == "" || strings.Contains(repository, "/") {
		return false, fmt.Errorf("repair repository identity is invalid")
	}
	baseSHA := strings.TrimSpace(job.BaseSHA)
	baseBranch := strings.TrimSpace(job.BaseBranch)
	if baseBranch != "" {
		token, err := r.repositoryMonitorGitHubToken(ctx, monitor)
		if err != nil {
			return false, err
		}
		baseSHA, err = r.fetchRepositoryMonitorBranchHead(ctx, owner, repository, token, baseBranch)
		if err != nil {
			return false, err
		}
	}
	if baseSHA == "" {
		return false, fmt.Errorf("repair base branch and base SHA are missing")
	}
	return r.repositoryMonitorRepoHeadContainsBase(ctx, monitor, job.Repo, baseSHA, job.HeadSHA)
}

func (r *RepositoryMonitorReconciler) repositoryMonitorRepoHeadContainsBase(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, repo, baseSHA, headSHA string) (bool, error) {
	owner, repository, ok := strings.Cut(strings.TrimSpace(repo), "/")
	if !ok || owner == "" || repository == "" || strings.Contains(repository, "/") {
		return false, fmt.Errorf("repository identity is invalid")
	}
	baseSHA, headSHA = strings.TrimSpace(baseSHA), strings.TrimSpace(headSHA)
	if baseSHA == "" || headSHA == "" {
		return false, fmt.Errorf("base and head SHAs are required")
	}
	token, err := r.repositoryMonitorGitHubToken(ctx, monitor)
	if err != nil {
		return false, err
	}
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/compare/%s...%s", baseURL, url.PathEscape(owner), url.PathEscape(repository), url.PathEscape(baseSHA), url.PathEscape(headSHA))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	response, err := repositoryMonitorHTTPClient(r).Do(request)
	if err != nil {
		return false, err
	}
	defer response.Body.Close() //nolint:errcheck
	body, err := readRepositoryMonitorGitHubResponse(response.Body, repositoryMonitorGitHubResponseLimit)
	if err != nil {
		return false, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return false, &repositoryMonitorGitHubAPIError{Operation: "compare repair head", StatusCode: response.StatusCode, Body: string(body)}
	}
	var result struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return false, err
	}
	return result.Status == "ahead" || result.Status == "identical", nil
}

//nolint:gocyclo // Repair ingestion keeps delivery, audit, and work-action settlement in one transaction.
func (r *RepositoryMonitorReconciler) ingestCompletedRepositoryMonitorRepairTasks(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (bool, error) {
	jobs, _, err := r.Store.ListRepairJobs(ctx, store.RepairJobFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, Phase: repositoryMonitorRepairPhaseQueued, Limit: 100})
	if err != nil {
		return false, err
	}
	ingested := false
	for i := range jobs {
		job := jobs[i]
		if strings.TrimSpace(job.TaskName) == "" {
			continue
		}
		var task corev1alpha1.Task
		if err := r.Get(ctx, types.NamespacedName{Namespace: monitor.Namespace, Name: job.TaskName}, &task); err != nil {
			if client.IgnoreNotFound(err) == nil {
				continue
			}
			return ingested, err
		}
		if !repositoryMonitorReviewTaskTerminal(task.Status.Phase) {
			continue
		}
		if cancelled, cancelErr := r.repositoryMonitorWorkActionCancelled(ctx, monitor, task.Annotations[repositoryMonitorIssueAnnotationCommandID], repositoryMonitorRepairWorkflowActionKind(job.Intent)); cancelErr != nil {
			return ingested, cancelErr
		} else if cancelled {
			completedAt := time.Now()
			job.Phase = repositoryMonitorRepairPhaseFailed
			job.LastError = repositoryMonitorIssueSkipStoppedByCommand
			job.CompletedAt = &completedAt
			if err := r.Store.UpdateRepairJob(ctx, &job); err != nil {
				return ingested, err
			}
			ingested = true
			continue
		}
		if task.Status.Phase == corev1alpha1.TaskPhaseSucceeded {
			job.Phase = repositoryMonitorRepairPhaseFailed
			job.LastError = "repair task result is missing"
			if branch, headSHA, delivered := repositoryMonitorACPDeliveryReceipt(&task); delivered {
				job.PushedSHA = headSHA
				if branch != job.Branch {
					job.LastError = "repair delivery branch did not match the requested PR head branch"
				} else {
					job.Phase = repositoryMonitorRepairPhaseSucceeded
					job.LastError = ""
				}
			} else if repositoryMonitorACPNoChangeReceipt(&task, job.HeadSHA) && job.Intent == repositoryMonitorCommandIntentUpdateBranch {
				containsBase, verifyErr := r.repositoryMonitorHeadContainsBase(ctx, monitor, &job)
				if verifyErr != nil {
					return ingested, verifyErr
				}
				if containsBase {
					job.Phase = repositoryMonitorRepairPhaseSucceeded
					job.PushedSHA = job.HeadSHA
					job.LastError = ""
				} else {
					job.LastError = "PR head does not contain the requested base revision"
				}
			} else {
				job.LastError = "repair delivery " + repositoryMonitorACPDeliveryFailureReason(&task)
			}
		} else {
			job.Phase = repositoryMonitorRepairPhaseFailed
			job.LastError = fmt.Sprintf("task ended in phase %s", task.Status.Phase)
		}
		commandID := task.Annotations[repositoryMonitorIssueAnnotationCommandID]
		pushMutationID := "ghmut-" + repositoryMonitorShortHash(job.ID+"-push")
		pushMutation, err := r.Store.GetGitHubMutationRecord(ctx, monitor.Namespace, pushMutationID)
		if err != nil {
			return ingested, err
		}
		pushMutation.CommandEventID = commandID
		pushMutation.ExternalID = job.PushedSHA
		pushMutation.GitHubURL = job.Branch
		pushMutation.Error = job.LastError
		pushMutation.Status = repositoryMonitorRunPhaseSucceeded
		if job.Phase != repositoryMonitorRepairPhaseSucceeded {
			pushMutation.Status = repositoryMonitorRunPhaseFailed
		}
		if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, pushMutation); err != nil {
			return ingested, err
		}
		completedAt := time.Now()
		job.CompletedAt = &completedAt
		if err := r.Store.UpdateRepairJob(ctx, &job); err != nil {
			return ingested, err
		}
		workStatus := repositoryMonitorWorkActionStatusSucceeded
		if job.Phase != repositoryMonitorRepairPhaseSucceeded {
			workStatus = repositoryMonitorWorkActionStatusFailed
		}
		if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, nil, &store.CommandEvent{ID: commandID, Intent: job.Intent}, repositoryMonitorPullRequestKind, job.PRNumber, job.HeadSHA, "", repositoryMonitorRepairWorkflowActionKind(job.Intent), workStatus, job.Phase, job.TaskName, job.LastError); err != nil {
			return ingested, err
		}
		item, err := r.Store.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, strconv.FormatInt(job.PRNumber, 10))
		if err == nil {
			item.RepairState = job.Phase
			if job.Phase == repositoryMonitorRepairPhaseSucceeded {
				repositoryMonitorResetItemAfterRepairPush(item)
			}
			if updateErr := r.Store.UpsertMonitorItem(ctx, item); updateErr != nil {
				return ingested, updateErr
			}
		} else if !errorsIsStoreNotFound(err) {
			return ingested, err
		}
		ingested = true
	}
	return ingested, nil
}

// repositoryMonitorResetItemAfterRepairPush clears the review state that a
// repair push invalidates so the new head is reviewed afresh. A review that
// the pull_request synchronize event already queued for the pushed head is
// kept: clearing its "queued" verdict would orphan the completed review
// task, which is only ingested while the item still reports it as queued.
func repositoryMonitorResetItemAfterRepairPush(item *store.MonitorItem) {
	if item == nil {
		return
	}
	item.LastReviewedHeadSHA = ""
	item.AutomergeState = ""
	item.SkipReason = ""
	if item.LastVerdict == repositoryMonitorRunPhaseQueued && strings.TrimSpace(item.LastReviewID) != "" {
		return
	}
	item.LastVerdict = ""
}
