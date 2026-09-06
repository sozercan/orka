package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/workers/common"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	repositoryMonitorReviewSchemaVersion = "orka.prReview.v1"

	repositoryMonitorReviewConfidenceLow    = "low"
	repositoryMonitorReviewConfidenceMedium = "medium"
	repositoryMonitorReviewConfidenceHigh   = "high"

	repositoryMonitorReviewVerdictPassed            = "passed"
	repositoryMonitorReviewVerdictNeedsChanges      = "needs_changes"
	repositoryMonitorReviewVerdictNeedsHuman        = "needs_human"
	eventReviewIDField                              = "reviewID"
	eventTaskNameField                              = "taskName"
	eventVerdictField                               = "verdict"
	eventHeadSHAField                               = "headSHA"
	repositoryMonitorReviewVerdictSecuritySensitive = "security_sensitive"
	repositoryMonitorReviewVerdictStale             = "stale"
	repositoryMonitorReviewVerdictFailed            = "failed"

	repositoryMonitorReviewSkipReasonFailed          = "review_failed"
	repositoryMonitorReviewSkipReasonMalformed       = "review_result_malformed"
	repositoryMonitorReviewSkipReasonMissingResult   = "review_result_missing"
	repositoryMonitorReviewSkipReasonStaleHead       = "stale_head_sha"
	repositoryMonitorReviewSkipReasonTaskMismatch    = "review_task_mismatch"
	repositoryMonitorReviewSkipReasonTaskFailed      = "review_task_failed"
	repositoryMonitorReviewSkipReasonTaskCancelled   = "review_task_cancelled"
	repositoryMonitorReviewSkipReasonTaskResultError = "review_result_error"

	repositoryMonitorValidationStatusNotRun      = "not_run"
	repositoryMonitorValidationStatusPassed      = "passed"
	repositoryMonitorValidationStatusFailed      = "failed"
	repositoryMonitorValidationStatusUnavailable = "unavailable"
	repositoryMonitorValidationEvidenceLimit     = 8192
	repositoryMonitorValidationPurpose           = "repository-validation"
	repositoryMonitorTaskCreatedBy               = "repository-monitor"
)

type repositoryMonitorReviewResult struct {
	SchemaVersion    string                            `json:"schemaVersion"`
	Repo             string                            `json:"repo"`
	PRNumber         int64                             `json:"prNumber"`
	HeadSHA          string                            `json:"headSHA"`
	Verdict          string                            `json:"verdict"`
	Confidence       string                            `json:"confidence"`
	Repairable       bool                              `json:"repairable"`
	Summary          string                            `json:"summary"`
	Findings         []repositoryMonitorReviewFinding  `json:"findings"`
	Security         repositoryMonitorReviewSecurity   `json:"security"`
	Tests            repositoryMonitorReviewTestStatus `json:"tests"`
	SuggestedComment string                            `json:"suggestedComment"`
}

type repositoryMonitorReviewFinding struct {
	Priority       string `json:"priority"`
	Confidence     string `json:"confidence"`
	File           string `json:"file"`
	Line           int64  `json:"line"`
	Title          string `json:"title"`
	Body           string `json:"body"`
	Recommendation string `json:"recommendation"`
}

type repositoryMonitorReviewSecurity struct {
	Status string `json:"status"`
	Notes  string `json:"notes"`
}

type repositoryMonitorReviewTestStatus struct {
	Status   string `json:"status"`
	Evidence string `json:"evidence"`
}

type repositoryMonitorValidationResult struct {
	TaskName      string
	Image         string
	CommandDigest string
	Status        string
	Evidence      string
	Required      bool
}

func (r *RepositoryMonitorReconciler) ingestCompletedRepositoryMonitorReviewTasks(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (bool, bool, error) {
	if r.ResultStore == nil {
		return false, false, nil
	}

	items, err := r.listRepositoryMonitorPullRequestItems(ctx, monitor)
	if err != nil {
		return false, false, err
	}

	ingested := false
	needsRequeue := false
	for i := range items {
		item := items[i]
		if item.LastVerdict != repositoryMonitorRunPhaseQueued || strings.TrimSpace(item.LastReviewID) == "" {
			continue
		}
		var task corev1alpha1.Task
		err := r.Get(ctx, types.NamespacedName{Namespace: monitor.Namespace, Name: item.LastReviewID}, &task)
		if err != nil {
			if client.IgnoreNotFound(err) == nil {
				continue
			}
			return ingested, needsRequeue, err
		}
		if !repositoryMonitorReviewTaskTerminal(task.Status.Phase) {
			continue
		}
		handled, err := r.ingestCompletedRepositoryMonitorReviewTask(ctx, monitor, &item, &task)
		if err != nil {
			return ingested, needsRequeue, err
		}
		ingested = ingested || handled
		if !handled && strings.TrimSpace(task.Annotations[labels.AnnotationRepositoryValidationImage]) != "" {
			validationTask := &corev1alpha1.Task{}
			err := r.Get(ctx, types.NamespacedName{
				Namespace: task.Namespace,
				Name:      tools.RepositoryValidationTaskName(&task),
			}, validationTask)
			if err == nil {
				needsRequeue = true
			} else if !apierrors.IsNotFound(err) {
				return ingested, needsRequeue, err
			}
		}
	}
	return ingested, needsRequeue, nil
}

func repositoryMonitorReviewTaskTerminal(phase corev1alpha1.TaskPhase) bool {
	switch phase {
	case corev1alpha1.TaskPhaseSucceeded, corev1alpha1.TaskPhaseFailed, corev1alpha1.TaskPhaseCancelled:
		return true
	default:
		return false
	}
}

func (r *RepositoryMonitorReconciler) ingestCompletedRepositoryMonitorReviewTask(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, task *corev1alpha1.Task) (bool, error) {
	recordID := repositoryMonitorReviewRecordID(task)
	if cancelled, err := r.repositoryMonitorWorkActionCancelled(ctx, monitor, task.Annotations[repositoryMonitorIssueAnnotationCommandID], "review"); err != nil || cancelled {
		return false, err
	}
	if err := validateRepositoryMonitorReviewTaskItemBinding(task, monitor, repositoryMonitorPullRequestKind, item.Number); err != nil {
		return r.createRepositoryMonitorRejectedReviewRecord(ctx, monitor, item, task, recordID, repositoryMonitorReviewVerdictFailed, repositoryMonitorReviewSkipReasonTaskMismatch, err.Error())
	}
	if record, err := r.Store.GetReviewRecord(ctx, monitor.Namespace, recordID); err == nil {
		settled, err := r.cleanupRepositoryMonitorValidationTask(ctx, monitor, task, record)
		if err != nil || !settled {
			return false, err
		}
		return r.applyRepositoryMonitorReviewRecord(ctx, monitor, item, record, task)
	} else if !errors.Is(err, store.ErrNotFound) {
		return false, err
	}

	switch task.Status.Phase {
	case corev1alpha1.TaskPhaseFailed:
		return r.createRepositoryMonitorRejectedReviewRecord(ctx, monitor, item, task, recordID, repositoryMonitorReviewVerdictFailed, repositoryMonitorReviewSkipReasonTaskFailed, task.Status.Message)
	case corev1alpha1.TaskPhaseCancelled:
		return r.createRepositoryMonitorRejectedReviewRecord(ctx, monitor, item, task, recordID, repositoryMonitorReviewVerdictFailed, repositoryMonitorReviewSkipReasonTaskCancelled, task.Status.Message)
	}

	if task.Status.ResultRef == nil || !task.Status.ResultRef.Available {
		return r.createRepositoryMonitorRejectedReviewRecord(ctx, monitor, item, task, recordID, repositoryMonitorReviewVerdictFailed, repositoryMonitorReviewSkipReasonMissingResult, "review task completed without a stored result")
	}

	rawResult, err := r.ResultStore.GetResult(ctx, task.Namespace, task.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return r.createRepositoryMonitorRejectedReviewRecord(ctx, monitor, item, task, recordID, repositoryMonitorReviewVerdictFailed, repositoryMonitorReviewSkipReasonMissingResult, "review task result was not found")
		}
		return false, fmt.Errorf("get repository monitor review task result %s/%s: %w", task.Namespace, task.Name, err)
	}

	review, err := parseRepositoryMonitorReviewResult(rawResult)
	if err != nil {
		return r.createRepositoryMonitorRejectedReviewRecord(ctx, monitor, item, task, recordID, repositoryMonitorReviewVerdictFailed, repositoryMonitorReviewSkipReasonMalformed, err.Error())
	}
	if err := validateRepositoryMonitorReviewResult(review, item, task); err != nil {
		return r.createRepositoryMonitorRejectedReviewRecord(ctx, monitor, item, task, recordID, repositoryMonitorReviewVerdictFailed, repositoryMonitorReviewSkipReasonMalformed, err.Error())
	}
	if currentHead := strings.TrimSpace(item.HeadSHA); currentHead != "" && currentHead != expectedRepositoryMonitorReviewHeadSHA(item, task) {
		summary := fmt.Sprintf("review result applies to stale head %s; current head is %s", expectedRepositoryMonitorReviewHeadSHA(item, task), currentHead)
		return r.createRepositoryMonitorRejectedReviewRecord(ctx, monitor, item, task, recordID, repositoryMonitorReviewVerdictStale, repositoryMonitorReviewSkipReasonStaleHead, summary)
	}
	validation, pending, err := r.repositoryMonitorReviewValidation(ctx, monitor, task)
	if err != nil {
		return false, err
	}
	if pending {
		return false, nil
	}

	findingsJSON, err := repositoryMonitorReviewFindingsJSON(review.Findings)
	if err != nil {
		return false, err
	}
	verdict := strings.TrimSpace(review.Verdict)
	summary := strings.TrimSpace(review.Summary)
	if verdict == repositoryMonitorReviewVerdictPassed && validation.Required && validation.Status != repositoryMonitorValidationStatusPassed {
		verdict = repositoryMonitorReviewVerdictNeedsHuman
		if validation.Status == repositoryMonitorValidationStatusFailed {
			verdict = repositoryMonitorReviewVerdictNeedsChanges
		}
		summary = strings.TrimSpace(fmt.Sprintf("Verdict downgraded from passed to %s because required validation was %s. %s", verdict, validation.Status, summary))
		if err := r.createMonitorEvent(ctx, monitor, "", repositoryMonitorPullRequestKind, item.Number, strings.TrimSpace(review.HeadSHA), "review_verdict_downgraded", fmt.Sprintf("Pull request #%d review verdict downgraded to %s because required validation was %s", item.Number, verdict, validation.Status), map[string]any{
			eventTaskNameField: task.Name,
			eventReasonField:   "validation_" + validation.Status,
		}); err != nil {
			return false, err
		}
	}
	if gateReason := repositoryMonitorReviewVerdictGateReason(task.Spec.Prompt, verdict); gateReason != "" {
		// Do not rely on the model honoring the prompt's safety rule: a
		// passed verdict without complete diff context is downgraded before
		// it can mark the head merge-ready.
		verdict = repositoryMonitorReviewVerdictNeedsHuman
		summary = strings.TrimSpace("Verdict downgraded from passed to needs_human because " + gateReason + ". " + summary)
		if err := r.createMonitorEvent(ctx, monitor, "", repositoryMonitorPullRequestKind, item.Number, strings.TrimSpace(review.HeadSHA), "review_verdict_downgraded", fmt.Sprintf("Pull request #%d review verdict downgraded to needs_human: %s", item.Number, gateReason), map[string]any{
			eventTaskNameField: task.Name,
			eventReasonField:   gateReason,
		}); err != nil {
			return false, err
		}
	}
	record := &store.ReviewRecord{
		ID:                      recordID,
		MonitorNamespace:        monitor.Namespace,
		MonitorName:             monitor.Name,
		Kind:                    repositoryMonitorPullRequestKind,
		Number:                  item.Number,
		HeadSHA:                 strings.TrimSpace(review.HeadSHA),
		TaskName:                task.Name,
		TaskNamespace:           task.Namespace,
		Verdict:                 verdict,
		Confidence:              strings.TrimSpace(review.Confidence),
		Repairable:              review.Repairable,
		SecurityStatus:          strings.TrimSpace(review.Security.Status),
		FindingsJSON:            findingsJSON,
		Summary:                 summary,
		SuggestedComment:        strings.TrimSpace(review.SuggestedComment),
		ValidationTask:          validation.TaskName,
		ValidationImage:         validation.Image,
		ValidationCommandDigest: validation.CommandDigest,
		ValidationStatus:        validation.Status,
		ValidationEvidence:      validation.Evidence,
	}
	if err := r.Store.CreateReviewRecord(ctx, record); err != nil {
		return false, err
	}
	settled, err := r.cleanupRepositoryMonitorValidationTask(ctx, monitor, task, record)
	if err != nil || !settled {
		return false, err
	}
	reason := ""
	if record.Verdict == repositoryMonitorVerdictSkipped {
		reason = repositoryMonitorVerdictSkipped
	}
	if err := r.applyRepositoryMonitorReviewRecordToItem(ctx, monitor, item, record, reason); err != nil {
		return false, err
	}
	if commandID := strings.TrimSpace(task.Annotations[repositoryMonitorIssueAnnotationCommandID]); commandID != "" {
		status := repositoryMonitorWorkActionStatusSucceeded
		if reason != "" || record.Verdict == repositoryMonitorReviewVerdictFailed {
			status = repositoryMonitorWorkActionStatusBlocked
		}
		if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, nil, &store.CommandEvent{ID: commandID, Intent: "review"}, repositoryMonitorPullRequestKind, item.Number, record.HeadSHA, "", "pr_review", status, record.Verdict, task.Name, reason); err != nil {
			return false, err
		}
	}
	if err := r.createMonitorEvent(ctx, monitor, "", repositoryMonitorPullRequestKind, item.Number, record.HeadSHA, "review_result_ingested", fmt.Sprintf("Pull request #%d review result ingested", item.Number), map[string]any{
		eventReviewIDField: record.ID,
		eventTaskNameField: task.Name,
		eventVerdictField:  record.Verdict,
		eventHeadSHAField:  record.HeadSHA,
		"confidence":       record.Confidence,
		"validationStatus": record.ValidationStatus,
	}); err != nil {
		return false, err
	}
	if err := r.publishRepositoryMonitorReview(ctx, monitor, item, task, record); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RepositoryMonitorReconciler) repositoryMonitorReviewValidation(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) (repositoryMonitorValidationResult, bool, error) {
	configuredImage := ""
	if monitor != nil {
		configuredImage = strings.TrimSpace(monitor.Spec.Validation.Image)
	}
	result := repositoryMonitorValidationResult{
		Image:    strings.TrimSpace(reviewTask.Annotations[labels.AnnotationRepositoryValidationImage]),
		Status:   repositoryMonitorValidationStatusNotRun,
		Required: configuredImage != "",
	}
	if configuredImage != "" && result.Image == "" {
		result.Status = repositoryMonitorValidationStatusFailed
		result.Evidence = "The review task is missing the configured validation image binding."
		return result, false, nil
	}
	if result.Image == "" {
		result.Evidence = "No validation image was configured for this review."
		return result, false, nil
	}
	if result.Image != configuredImage {
		result.Status = repositoryMonitorValidationStatusFailed
		result.Evidence = "The review task validation image no longer matches the RepositoryMonitor."
		return result, false, nil
	}

	validationTask := &corev1alpha1.Task{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: reviewTask.Namespace,
		Name:      tools.RepositoryValidationTaskName(reviewTask),
	}, validationTask); apierrors.IsNotFound(err) {
		result.TaskName = tools.RepositoryValidationTaskName(reviewTask)
		binding, bindingErr := tools.FindRepositoryValidationCommandBinding(ctx, r.Store, reviewTask.Namespace, result.TaskName)
		if bindingErr != nil {
			if tools.IsRepositoryValidationCommandBindingInvalid(bindingErr) {
				result.Status = repositoryMonitorValidationStatusFailed
				result.Evidence = "The stored validation command binding is invalid."
				return result, false, nil
			}
			return result, false, fmt.Errorf("load repository validation command binding: %w", bindingErr)
		}
		if binding != nil {
			if !binding.MatchesReview(reviewTask, monitor, result.Image, strings.TrimSpace(reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])) {
				result.Status = repositoryMonitorValidationStatusFailed
				result.Evidence = "The stored validation command binding does not match this review."
				return result, false, nil
			}
			result.CommandDigest = binding.CommandDigest
			result.Status = repositoryMonitorValidationStatusUnavailable
			result.Evidence = "The validation task could not be created after its command was durably bound."
			return result, false, nil
		}
		result.TaskName = ""
		result.Evidence = "The reviewer did not run required validation."
		return result, false, nil
	} else if err != nil {
		return result, false, err
	}
	result.TaskName = validationTask.Name
	if err := validateRepositoryMonitorValidationTask(monitor, reviewTask, validationTask, result.Image); err != nil {
		result.Status = repositoryMonitorValidationStatusFailed
		result.Evidence = boundRepositoryMonitorValidationEvidence(err.Error())
		return result, false, nil
	}
	headSHA := strings.TrimSpace(reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
	binding, err := tools.FindRepositoryValidationCommandBinding(ctx, r.Store, reviewTask.Namespace, validationTask.Name)
	if err != nil {
		if tools.IsRepositoryValidationCommandBindingInvalid(err) {
			result.Status = repositoryMonitorValidationStatusFailed
			result.Evidence = "The validation task does not match its stored command binding."
			return result, false, nil
		}
		return result, false, fmt.Errorf("load repository validation command binding: %w", err)
	}
	if binding == nil || !binding.MatchesReview(reviewTask, monitor, result.Image, headSHA) {
		result.Status = repositoryMonitorValidationStatusFailed
		result.Evidence = "The validation task does not match its stored command binding."
		return result, false, nil
	}
	result.CommandDigest = binding.CommandDigest
	commandSecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: validationTask.Namespace,
		Name:      tools.RepositoryValidationCommandSecretName(validationTask.Name),
	}, commandSecret); err != nil {
		if apierrors.IsNotFound(err) {
			result.Status = repositoryMonitorValidationStatusFailed
			result.Evidence = "The immutable validation command Secret is missing."
			return result, false, nil
		}
		return result, false, err
	}
	if err := tools.ValidateRepositoryValidationCommandSecret(reviewTask, validationTask, commandSecret, binding); err != nil {
		result.Status = repositoryMonitorValidationStatusFailed
		result.Evidence = "The immutable validation command Secret does not match its stored binding."
		return result, false, nil
	}
	if !repositoryMonitorReviewTaskTerminal(validationTask.Status.Phase) {
		return result, true, nil
	}

	result.Evidence = r.repositoryMonitorValidationEvidence(ctx, validationTask)
	if validationTask.Status.Phase == corev1alpha1.TaskPhaseSucceeded {
		result.Status = repositoryMonitorValidationStatusPassed
		return result, false, nil
	}
	if validationTask.Status.Phase == corev1alpha1.TaskPhaseFailed &&
		validationTask.Status.ExecutionOutcome != nil &&
		validationTask.Status.ExecutionOutcome.Phase == corev1alpha1.TaskPhaseFailed {
		result.Status = repositoryMonitorValidationStatusFailed
		return result, false, nil
	}
	result.Status = repositoryMonitorValidationStatusUnavailable
	return result, false, nil
}

func (r *RepositoryMonitorReconciler) cleanupRepositoryMonitorValidationTask(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task, record *store.ReviewRecord) (bool, error) {
	if r == nil || monitor == nil || reviewTask == nil || record == nil {
		return true, nil
	}
	if !repositoryMonitorValidationCleanupRequired(reviewTask, record) {
		return true, nil
	}
	expectedName := tools.RepositoryValidationTaskName(reviewTask)
	validationTaskName := strings.TrimSpace(record.ValidationTask)
	if validationTaskName == "" {
		validationTaskName = expectedName
	}
	if validationTaskName != expectedName {
		return false, fmt.Errorf("refuse to clean up validation task with mismatched identity")
	}
	validationTask := &corev1alpha1.Task{}
	key := types.NamespacedName{Namespace: reviewTask.Namespace, Name: validationTaskName}
	if err := r.Get(ctx, key, validationTask); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.cleanupOrphanedRepositoryMonitorValidationCommandSecret(ctx, monitor, reviewTask, validationTaskName); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}
	if err := validateRepositoryMonitorValidationCleanupIdentity(monitor, reviewTask, validationTask); err != nil {
		if cleanupErr := r.cleanupOrphanedRepositoryMonitorValidationCommandSecret(ctx, monitor, reviewTask, validationTaskName); cleanupErr != nil {
			return false, cleanupErr
		}
		return true, nil
	}
	binding, err := r.repositoryMonitorValidationCleanupBinding(ctx, monitor, reviewTask, validationTask.Name)
	if err != nil {
		return false, err
	}
	if binding == nil {
		return true, nil
	}
	if !validationTask.DeletionTimestamp.IsZero() {
		if err := r.cleanupRepositoryMonitorValidationCommandSecret(ctx, reviewTask, validationTask, binding); err != nil {
			return false, err
		}
		return true, nil
	}
	if !repositoryMonitorReviewTaskTerminal(validationTask.Status.Phase) {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			current := &corev1alpha1.Task{}
			if err := r.Get(ctx, key, current); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return err
			}
			if !current.DeletionTimestamp.IsZero() || repositoryMonitorReviewTaskTerminal(current.Status.Phase) {
				return nil
			}
			if err := validateRepositoryMonitorValidationCleanupIdentity(monitor, reviewTask, current); err != nil {
				return err
			}
			now := metav1.Now()
			current.Status.Phase = corev1alpha1.TaskPhaseCancelled
			current.Status.CompletionTime = &now
			current.Status.Message = "parent review ended before repository validation completed"
			return r.Status().Update(ctx, current)
		}); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := r.cleanupRepositoryMonitorValidationCommandSecret(ctx, reviewTask, validationTask, binding); err != nil {
		return false, err
	}
	if err := r.Delete(ctx, validationTask); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("delete terminal validation task %s/%s: %w", validationTask.Namespace, validationTask.Name, err)
	}
	return true, nil
}

func (r *RepositoryMonitorReconciler) repositoryMonitorValidationCleanupBinding(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task, validationTaskName string) (*tools.RepositoryValidationCommandBinding, error) {
	binding, err := tools.FindRepositoryValidationCommandBinding(ctx, r.Store, reviewTask.Namespace, validationTaskName)
	if err != nil {
		if tools.IsRepositoryValidationCommandBindingInvalid(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("load validation command binding for cleanup: %w", err)
	}
	if binding == nil || !binding.MatchesReview(
		reviewTask,
		monitor,
		reviewTask.Annotations[labels.AnnotationRepositoryValidationImage],
		reviewTask.Annotations[labels.AnnotationMonitorHeadSHA],
	) {
		return nil, nil
	}
	return binding, nil
}

func (r *RepositoryMonitorReconciler) cleanupRepositoryMonitorValidationCommandSecret(ctx context.Context, reviewTask, validationTask *corev1alpha1.Task, binding *tools.RepositoryValidationCommandBinding) error {
	commandSecret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: validationTask.Namespace,
		Name:      tools.RepositoryValidationCommandSecretName(validationTask.Name),
	}
	if err := r.Get(ctx, secretKey, commandSecret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("load validation command Secret for cleanup: %w", err)
	}
	if tools.ValidateRepositoryValidationCommandSecret(reviewTask, validationTask, commandSecret, binding) != nil {
		return nil
	}
	if err := r.Delete(ctx, commandSecret); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete validation command Secret %s/%s: %w", commandSecret.Namespace, commandSecret.Name, err)
	}
	return nil
}

func repositoryMonitorValidationCleanupRequired(reviewTask *corev1alpha1.Task, record *store.ReviewRecord) bool {
	return reviewTask != nil && record != nil &&
		(strings.TrimSpace(record.ValidationTask) != "" ||
			strings.TrimSpace(record.ValidationImage) != "" ||
			strings.TrimSpace(reviewTask.Annotations[labels.AnnotationRepositoryValidationImage]) != "")
}

func (r *RepositoryMonitorReconciler) cleanupOrphanedRepositoryMonitorValidationCommandSecret(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task, validationTaskName string) error {
	commandSecret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: reviewTask.Namespace,
		Name:      tools.RepositoryValidationCommandSecretName(validationTaskName),
	}
	if err := r.Get(ctx, secretKey, commandSecret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("load orphaned validation command Secret: %w", err)
	}
	binding, err := tools.FindRepositoryValidationCommandBinding(ctx, r.Store, reviewTask.Namespace, validationTaskName)
	if err != nil {
		return fmt.Errorf("load orphaned validation command binding: %w", err)
	}
	if binding == nil || !binding.MatchesReview(
		reviewTask,
		monitor,
		reviewTask.Annotations[labels.AnnotationRepositoryValidationImage],
		reviewTask.Annotations[labels.AnnotationMonitorHeadSHA],
	) {
		return nil
	}
	if err := tools.ValidateRepositoryValidationOrphanCommandSecret(reviewTask, commandSecret, binding); err != nil {
		return nil
	}
	if err := r.Delete(ctx, commandSecret); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete orphaned validation command Secret %s/%s: %w", commandSecret.Namespace, commandSecret.Name, err)
	}
	return nil
}

func validateRepositoryMonitorValidationCleanupIdentity(monitor *corev1alpha1.RepositoryMonitor, reviewTask, validationTask *corev1alpha1.Task) error {
	if validationTask.Namespace != reviewTask.Namespace ||
		validationTask.Name != tools.RepositoryValidationTaskName(reviewTask) ||
		!metav1.IsControlledBy(reviewTask, monitor) ||
		!metav1.IsControlledBy(validationTask, reviewTask) ||
		labels.ParentTaskName(validationTask.Labels, validationTask.Annotations) != reviewTask.Name ||
		validationTask.Annotations[labels.AnnotationParentTaskUID] != string(reviewTask.UID) {
		return fmt.Errorf("refuse to clean up validation task with mismatched identity, owner, or parent")
	}
	return nil
}

func validateRepositoryMonitorValidationTask(monitor *corev1alpha1.RepositoryMonitor, reviewTask, validationTask *corev1alpha1.Task, image string) error {
	if monitor == nil || reviewTask == nil || validationTask == nil {
		return fmt.Errorf("repository monitor, review task, and validation task are required")
	}
	if err := validateRepositoryMonitorValidationTaskProvenance(monitor, reviewTask, validationTask); err != nil {
		return err
	}
	if err := validateRepositoryMonitorValidationTaskSpec(validationTask, image); err != nil {
		return err
	}
	if err := validateRepositoryMonitorValidationWorkspace(reviewTask, validationTask); err != nil {
		return err
	}
	headSHA := strings.TrimSpace(reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
	if !tools.RepositoryValidationTaskSpecMatches(reviewTask, monitor, validationTask, image, headSHA) {
		return fmt.Errorf("validation task spec does not match the canonical repository validation task")
	}
	return nil
}

func validateRepositoryMonitorValidationTaskProvenance(monitor *corev1alpha1.RepositoryMonitor, reviewTask, validationTask *corev1alpha1.Task) error {
	if !metav1.IsControlledBy(reviewTask, monitor) {
		return fmt.Errorf("review task %s/%s is not controlled by repository monitor %s/%s", reviewTask.Namespace, reviewTask.Name, monitor.Namespace, monitor.Name)
	}
	if !metav1.IsControlledBy(validationTask, reviewTask) {
		return fmt.Errorf("validation task %s/%s is not controlled by review task %s/%s", validationTask.Namespace, validationTask.Name, reviewTask.Namespace, reviewTask.Name)
	}
	if strings.TrimSpace(validationTask.Annotations[labels.AnnotationTransactionTokenSecret]) != "" {
		return fmt.Errorf("validation task must not reference a transaction-token Secret")
	}
	if validationTask.Namespace != reviewTask.Namespace ||
		labels.ParentTaskName(validationTask.Labels, validationTask.Annotations) != reviewTask.Name ||
		validationTask.Annotations[labels.AnnotationParentTaskUID] != string(reviewTask.UID) {
		return fmt.Errorf("validation task is not bound to review task %s/%s", reviewTask.Namespace, reviewTask.Name)
	}
	for _, annotation := range []string{
		labels.AnnotationRepositoryMonitorName,
		labels.AnnotationMonitorRunID,
		labels.AnnotationMonitorItemKind,
		labels.AnnotationMonitorItemNumber,
		labels.AnnotationMonitorHeadSHA,
		labels.AnnotationGitHubRepository,
		labels.AnnotationRepositoryValidationImage,
	} {
		if validationTask.Annotations[annotation] != reviewTask.Annotations[annotation] {
			return fmt.Errorf("validation task annotation %s does not match the review task", annotation)
		}
	}
	if validationTask.Labels[labels.LabelCreatedBy] != repositoryMonitorTaskCreatedBy || validationTask.Labels[labels.LabelPurpose] != repositoryMonitorValidationPurpose {
		return fmt.Errorf("validation task provenance labels are invalid")
	}
	return nil
}

func validateRepositoryMonitorValidationTaskSpec(validationTask *corev1alpha1.Task, image string) error {
	if validationTask.Spec.Type != corev1alpha1.TaskTypeContainer || strings.TrimSpace(validationTask.Spec.Image) != image {
		return fmt.Errorf("validation task image or type does not match the review policy")
	}
	if !slices.Equal(validationTask.Spec.Command, []string{"/bin/sh", "-c"}) || len(validationTask.Spec.Args) != 1 {
		return fmt.Errorf("validation task command placeholder is invalid")
	}
	if len(validationTask.Spec.Env) != 0 || validationTask.Spec.SecretRef != nil || validationTask.Spec.AgentRef != nil || validationTask.Spec.AI != nil || strings.TrimSpace(validationTask.Spec.Schedule) != "" {
		return fmt.Errorf("validation task contains capabilities outside repository validation")
	}
	return nil
}

func validateRepositoryMonitorValidationWorkspace(reviewTask, validationTask *corev1alpha1.Task) error {
	workspace := validationTask.Spec.Workspace
	reviewWorkspace := reviewTask.Spec.Workspace
	if workspace == nil || reviewWorkspace == nil || workspace.Intent != corev1alpha1.WorkspaceIntentRead ||
		strings.TrimSpace(workspace.GitRepo) != strings.TrimSpace(reviewWorkspace.GitRepo) ||
		strings.TrimSpace(workspace.Ref) != strings.TrimSpace(reviewTask.Annotations[labels.AnnotationMonitorHeadSHA]) ||
		strings.TrimSpace(workspace.SubPath) != strings.TrimSpace(reviewWorkspace.SubPath) ||
		!reflect.DeepEqual(workspace.ReadCredentialRef, reviewWorkspace.ReadCredentialRef) {
		return fmt.Errorf("validation task workspace does not match the exact reviewed head")
	}
	if strings.TrimSpace(workspace.Branch) != "" || strings.TrimSpace(workspace.PublicationGitRepo) != "" || workspace.PublicationReadCredentialRef != nil || workspace.PublicationCredentialRef != nil || workspace.ForgeCredentialRef != nil || strings.TrimSpace(workspace.PushBranch) != "" || workspace.CreatePR {
		return fmt.Errorf("validation task workspace contains publication capabilities")
	}
	return nil
}

func (r *RepositoryMonitorReconciler) repositoryMonitorValidationEvidence(ctx context.Context, task *corev1alpha1.Task) string {
	parts := make([]string, 0, 2)
	if message := strings.TrimSpace(task.Status.Message); message != "" {
		parts = append(parts, message)
	}
	if r.ResultStore != nil && task.Status.ResultRef != nil && task.Status.ResultRef.Available {
		if raw, err := r.ResultStore.GetResult(ctx, task.Namespace, task.Name); err == nil {
			if output := strings.TrimSpace(string(raw)); output != "" {
				parts = append(parts, output)
			}
		}
	}
	if len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("Validation task ended in phase %s.", task.Status.Phase))
	}
	return boundRepositoryMonitorValidationEvidence(strings.Join(parts, "\n"))
}

func boundRepositoryMonitorValidationEvidence(value string) string {
	value = strings.TrimSpace(redact.SensitiveText(strings.ToValidUTF8(value, "�")))
	if len([]rune(value)) <= repositoryMonitorValidationEvidenceLimit {
		return value
	}
	return boundedString(value, repositoryMonitorValidationEvidenceLimit) + fmt.Sprintf("\n[validation evidence truncated; original size: %d bytes]", len(value))
}

func repositoryMonitorReviewRecordMatchesValidationPolicy(monitor *corev1alpha1.RepositoryMonitor, record *store.ReviewRecord) bool {
	if monitor == nil {
		return false
	}
	image := strings.TrimSpace(monitor.Spec.Validation.Image)
	return record != nil && strings.TrimSpace(record.ValidationImage) == image
}

func repositoryMonitorReviewRecordAllowsAutomerge(monitor *corev1alpha1.RepositoryMonitor, record *store.ReviewRecord) bool {
	if !repositoryMonitorReviewRecordMatchesValidationPolicy(monitor, record) {
		return false
	}
	return strings.TrimSpace(monitor.Spec.Validation.Image) == "" ||
		record.ValidationStatus == repositoryMonitorValidationStatusPassed
}

func parseRepositoryMonitorReviewResult(raw []byte) (*repositoryMonitorReviewResult, error) {
	summary := strings.TrimSpace(common.ParseStructuredResult(string(raw)).Summary)
	if summary == "" {
		summary = strings.TrimSpace(string(raw))
	}
	payload, err := repositoryMonitorReviewJSONPayload(summary)
	if err != nil {
		return nil, err
	}
	var result repositoryMonitorReviewResult
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		return nil, fmt.Errorf("review result is not valid JSON: %w", err)
	}
	return &result, nil
}

func repositoryMonitorReviewJSONPayload(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("review result is empty")
	}
	if json.Valid([]byte(raw)) {
		return raw, nil
	}
	payload, ok := firstJSONObject(raw)
	if !ok {
		return "", fmt.Errorf("review result does not contain a JSON object")
	}
	if !json.Valid([]byte(payload)) {
		return "", fmt.Errorf("review result JSON object is invalid")
	}
	return payload, nil
}

func firstJSONObject(raw string) (string, bool) {
	start := strings.IndexByte(raw, '{')
	if start < 0 {
		return "", false
	}
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(raw); i++ {
		ch := raw[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return raw[start : i+1], true
			}
		}
	}
	return "", false
}

func validateRepositoryMonitorReviewResult(result *repositoryMonitorReviewResult, item *store.MonitorItem, task *corev1alpha1.Task) error {
	if strings.TrimSpace(result.SchemaVersion) != repositoryMonitorReviewSchemaVersion {
		return fmt.Errorf("schemaVersion must be %q", repositoryMonitorReviewSchemaVersion)
	}
	if expectedRepo := strings.TrimSpace(task.Annotations[labels.AnnotationGitHubRepository]); expectedRepo != "" && !strings.EqualFold(strings.TrimSpace(result.Repo), expectedRepo) {
		return fmt.Errorf("repo %q does not match expected repository %q", result.Repo, expectedRepo)
	}
	if result.PRNumber != item.Number {
		return fmt.Errorf("prNumber %d does not match expected PR number %d", result.PRNumber, item.Number)
	}
	if expectedHead := expectedRepositoryMonitorReviewHeadSHA(item, task); strings.TrimSpace(result.HeadSHA) != expectedHead {
		return fmt.Errorf("headSHA %q does not match expected head %q", result.HeadSHA, expectedHead)
	}
	if !repositoryMonitorReviewVerdictAllowed(result.Verdict) {
		return fmt.Errorf("verdict %q is not allowed", result.Verdict)
	}
	if !repositoryMonitorReviewConfidenceAllowed(result.Confidence) {
		return fmt.Errorf("confidence %q is not allowed", result.Confidence)
	}
	if !repositoryMonitorReviewSecurityStatusAllowed(result.Security.Status) {
		return fmt.Errorf("security.status %q is not allowed", result.Security.Status)
	}
	return nil
}

func expectedRepositoryMonitorReviewHeadSHA(item *store.MonitorItem, task *corev1alpha1.Task) string {
	if task != nil {
		if headSHA := strings.TrimSpace(task.Annotations[labels.AnnotationMonitorHeadSHA]); headSHA != "" {
			return headSHA
		}
	}
	return strings.TrimSpace(item.HeadSHA)
}

func repositoryMonitorReviewVerdictAllowed(verdict string) bool {
	switch strings.TrimSpace(verdict) {
	case repositoryMonitorReviewVerdictPassed,
		repositoryMonitorReviewVerdictNeedsChanges,
		repositoryMonitorReviewVerdictNeedsHuman,
		repositoryMonitorReviewVerdictSecuritySensitive,
		repositoryMonitorVerdictSkipped:
		return true
	default:
		return false
	}
}

func repositoryMonitorReviewConfidenceAllowed(confidence string) bool {
	switch strings.TrimSpace(confidence) {
	case repositoryMonitorReviewConfidenceLow, repositoryMonitorReviewConfidenceMedium, repositoryMonitorReviewConfidenceHigh:
		return true
	default:
		return false
	}
}

func repositoryMonitorReviewSecurityStatusAllowed(status string) bool {
	switch strings.TrimSpace(status) {
	case "clear", "needs_human", "security_sensitive":
		return true
	default:
		return false
	}
}

func repositoryMonitorReviewFindingsJSON(findings []repositoryMonitorReviewFinding) (string, error) {
	if findings == nil {
		findings = []repositoryMonitorReviewFinding{}
	}
	data, err := json.Marshal(findings)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (r *RepositoryMonitorReconciler) createRepositoryMonitorRejectedReviewRecord(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, task *corev1alpha1.Task, recordID, verdict, reason, summary string) (bool, error) {
	if cancelled, err := r.repositoryMonitorWorkActionCancelled(ctx, monitor, task.Annotations[repositoryMonitorIssueAnnotationCommandID], "review"); err != nil || cancelled {
		return false, err
	}
	summary = strings.TrimSpace(summary)
	if summary == "" {
		summary = reason
	}
	record := &store.ReviewRecord{
		ID:               recordID,
		MonitorNamespace: monitor.Namespace,
		MonitorName:      monitor.Name,
		Kind:             repositoryMonitorPullRequestKind,
		Number:           item.Number,
		HeadSHA:          expectedRepositoryMonitorReviewHeadSHA(item, task),
		TaskName:         task.Name,
		TaskNamespace:    task.Namespace,
		Verdict:          verdict,
		Confidence:       repositoryMonitorReviewConfidenceLow,
		SecurityStatus:   "unknown",
		FindingsJSON:     "[]",
		Summary:          summary,
	}
	if err := r.Store.CreateReviewRecord(ctx, record); err != nil {
		return false, err
	}
	settled, err := r.cleanupRepositoryMonitorValidationTask(ctx, monitor, task, record)
	if err != nil || !settled {
		return false, err
	}
	if err := r.applyRepositoryMonitorReviewRecordToItem(ctx, monitor, item, record, reason); err != nil {
		return false, err
	}
	if commandID := strings.TrimSpace(task.Annotations[repositoryMonitorIssueAnnotationCommandID]); commandID != "" {
		if err := r.recordRepositoryMonitorWorkActionState(ctx, monitor, nil, &store.CommandEvent{ID: commandID, Intent: "review"}, repositoryMonitorPullRequestKind, item.Number, record.HeadSHA, "", "pr_review", repositoryMonitorWorkActionStatusBlocked, record.Verdict, task.Name, reason); err != nil {
			return false, err
		}
	}
	if err := r.createMonitorEvent(ctx, monitor, "", repositoryMonitorPullRequestKind, item.Number, record.HeadSHA, "review_result_rejected", fmt.Sprintf("Pull request #%d review result rejected: %s", item.Number, reason), map[string]any{
		"reviewID": record.ID,
		"taskName": task.Name,
		"verdict":  record.Verdict,
		"reason":   reason,
	}); err != nil {
		return false, err
	}
	if err := r.publishRepositoryMonitorReview(ctx, monitor, item, task, record); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RepositoryMonitorReconciler) applyRepositoryMonitorReviewRecord(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, record *store.ReviewRecord, task *corev1alpha1.Task) (bool, error) {
	if cancelled, err := r.repositoryMonitorWorkActionCancelled(ctx, monitor, task.Annotations[repositoryMonitorIssueAnnotationCommandID], "review"); err != nil || cancelled {
		return false, err
	}
	if item.LastReviewID != task.Name || item.LastVerdict != repositoryMonitorRunPhaseQueued {
		return false, nil
	}
	reason := ""
	switch record.Verdict {
	case repositoryMonitorReviewVerdictFailed:
		reason = repositoryMonitorReviewSkipReasonFailed
	case repositoryMonitorReviewVerdictStale:
		reason = repositoryMonitorReviewSkipReasonStaleHead
	case repositoryMonitorVerdictSkipped:
		reason = repositoryMonitorVerdictSkipped
	}
	if err := r.applyRepositoryMonitorReviewRecordToItem(ctx, monitor, item, record, reason); err != nil {
		return false, err
	}
	if err := r.createMonitorEvent(ctx, monitor, "", repositoryMonitorPullRequestKind, item.Number, record.HeadSHA, "review_result_ingested", fmt.Sprintf("Pull request #%d review result ingested", item.Number), map[string]any{
		"reviewID": record.ID,
		"taskName": task.Name,
		"verdict":  record.Verdict,
		"headSHA":  record.HeadSHA,
	}); err != nil {
		return false, err
	}
	if err := r.publishRepositoryMonitorReview(ctx, monitor, item, task, record); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RepositoryMonitorReconciler) applyRepositoryMonitorReviewRecordToItem(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem, record *store.ReviewRecord, reason string) error {
	validationRetry, err := r.syncRepositoryMonitorRepairValidationAttempts(ctx, monitor, record)
	if err != nil {
		return err
	}
	item.LastReviewID = record.ID
	item.LastVerdict = record.Verdict
	item.SkipReason = reason
	if record.HeadSHA == item.HeadSHA {
		marksFresh := repositoryMonitorReviewRecordMarksHeadFresh(record)
		if validationRetry.associated && repositoryMonitorValidationRetryableAfterRepair(record.ValidationStatus) {
			marksFresh = validationRetry.exhausted
		}
		if reason == "" && marksFresh && repositoryMonitorReviewRecordMatchesValidationPolicy(monitor, record) {
			item.LastReviewedHeadSHA = record.HeadSHA
		} else {
			item.LastReviewedHeadSHA = ""
		}
	}
	if reason == "" && record.Verdict == repositoryMonitorReviewVerdictPassed && record.HeadSHA == item.HeadSHA &&
		repositoryMonitorReviewRecordAllowsAutomerge(monitor, record) && !repositoryMonitorAutomergeRepairStateBlocks(item.RepairState) {
		item.AutomergeState = repositoryMonitorAutomergeStateMergeReady
	} else {
		item.AutomergeState = ""
	}
	return r.Store.UpsertMonitorItem(ctx, item)
}

func repositoryMonitorReviewVerdictMarksHeadFresh(verdict string) bool {
	switch strings.TrimSpace(verdict) {
	case repositoryMonitorReviewVerdictPassed,
		repositoryMonitorReviewVerdictNeedsChanges,
		repositoryMonitorReviewVerdictNeedsHuman,
		repositoryMonitorReviewVerdictSecuritySensitive:
		return true
	default:
		return false
	}
}

func repositoryMonitorReviewRecordMarksHeadFresh(record *store.ReviewRecord) bool {
	return record != nil &&
		record.ValidationStatus != repositoryMonitorValidationStatusUnavailable &&
		repositoryMonitorReviewVerdictMarksHeadFresh(record.Verdict)
}

func repositoryMonitorReviewRecordID(task *corev1alpha1.Task) string {
	return repositoryMonitorBoundedDNSName("monreview-"+task.Namespace+"-"+task.Name, 120)
}
