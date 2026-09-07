package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/metrics"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
)

const (
	repositoryMonitorWorkActionStatusQueued    = "queued"
	repositoryMonitorWorkActionStatusRunning   = "running"
	repositoryMonitorWorkActionStatusSucceeded = "succeeded"
	repositoryMonitorWorkActionStatusFailed    = "failed"
	repositoryMonitorWorkActionStatusBlocked   = "blocked"
	repositoryMonitorWorkActionStatusCancelled = "cancelled"
)

func (r *RepositoryMonitorReconciler) recordRepositoryMonitorWorkActionState(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, command *store.CommandEvent, targetKind string, targetNumber int64, targetSHA, snapshotDigest, actionKind, status, phase, taskName, reason string) error {
	if r.Store == nil || monitor == nil || command == nil {
		return nil
	}
	desiredAction := store.RepositoryMonitorDesiredActionForActionKind(actionKind)
	if desiredAction == "" {
		desiredAction = store.RepositoryMonitorDesiredActionForIntent(command.Intent)
	}
	if desiredAction == "" {
		return nil
	}
	if status == "" {
		status = repositoryMonitorWorkActionStatusQueued
	}
	if phase == "" {
		phase = desiredAction + "_queued"
	}
	actionSeed := strings.TrimSpace(command.ID)
	if actionSeed == "" {
		actionSeed = fmt.Sprintf("%s|%d|%s|%s|%s|%s", targetKind, targetNumber, targetSHA, snapshotDigest, actionKind, taskName)
	}
	id := store.RepositoryMonitorWorkActionID(actionSeed, desiredAction)
	completedAt := (*time.Time)(nil)
	switch status {
	case repositoryMonitorWorkActionStatusSucceeded, repositoryMonitorWorkActionStatusFailed, repositoryMonitorWorkActionStatusBlocked, repositoryMonitorWorkActionStatusCancelled:
		now := time.Now()
		completedAt = &now
	}
	blockedReason, actionError := repositoryMonitorWorkActionReasonFields(status, reason)
	if existing, err := r.Store.GetWorkAction(ctx, monitor.Namespace, id); err == nil {
		verifiedUpdateBranchSuccess := desiredAction == repositoryMonitorCommandIntentUpdateBranch && status == repositoryMonitorWorkActionStatusSucceeded
		if existing.Status == repositoryMonitorWorkActionStatusCancelled && desiredAction != repositoryMonitorCommandIntentStop && desiredAction != repositoryMonitorCommandIntentResume && !verifiedUpdateBranchSuccess {
			return nil
		}
		existing.RunID = firstNonEmptyWorkflow(runID(run), existing.RunID)
		existing.TargetKind = firstNonEmptyWorkflow(targetKind, existing.TargetKind)
		if targetNumber != 0 {
			existing.TargetNumber = targetNumber
		}
		existing.TargetSHA = firstNonEmptyWorkflow(targetSHA, existing.TargetSHA)
		existing.TargetSnapshotDigest = firstNonEmptyWorkflow(snapshotDigest, existing.TargetSnapshotDigest)
		existing.Intent = firstNonEmptyWorkflow(command.Intent, existing.Intent)
		existing.DesiredAction = desiredAction
		existing.Status = status
		existing.Phase = phase
		existing.TaskName = firstNonEmptyWorkflow(taskName, existing.TaskName)
		existing.BlockedReason = blockedReason
		existing.Error = actionError
		existing.CompletedAt = completedAt
		metrics.RecordRepositoryMonitorWorkAction(existing.DesiredAction, existing.Status)
		return r.Store.UpdateWorkAction(ctx, existing)
	} else if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	if desiredAction != repositoryMonitorCommandIntentStop && desiredAction != repositoryMonitorCommandIntentResume {
		active, _, err := r.Store.ListWorkActions(ctx, store.WorkActionFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, DedupeKey: store.RepositoryMonitorWorkActionDedupeKey(monitor.Namespace, monitor.Name, monitor.Generation, targetKind, targetNumber, targetSHA, snapshotDigest, desiredAction), Limit: 5})
		if err != nil {
			return err
		}
		for _, candidate := range active {
			switch candidate.Status {
			case repositoryMonitorWorkActionStatusQueued, "leased", repositoryMonitorWorkActionStatusRunning, store.RepositoryMonitorWorkActionStatusRetryPending:
				return nil
			}
		}
	}
	metadata, _ := json.Marshal(map[string]any{"actionKind": actionKind})
	metrics.RecordRepositoryMonitorWorkAction(desiredAction, status)
	if err := r.Store.CreateWorkAction(ctx, &store.WorkAction{
		ID:                   id,
		MonitorNamespace:     monitor.Namespace,
		MonitorName:          monitor.Name,
		RunID:                runID(run),
		CommandEventID:       command.ID,
		MonitorGeneration:    monitor.Generation,
		TargetKind:           targetKind,
		TargetNumber:         targetNumber,
		TargetSHA:            targetSHA,
		TargetSnapshotDigest: snapshotDigest,
		Intent:               command.Intent,
		DesiredAction:        desiredAction,
		DedupeKey:            store.RepositoryMonitorWorkActionDedupeKey(monitor.Namespace, monitor.Name, monitor.Generation, targetKind, targetNumber, targetSHA, snapshotDigest, desiredAction),
		IdempotencyKey:       command.IdempotencyKey,
		Status:               status,
		Phase:                phase,
		TaskName:             taskName,
		BlockedReason:        blockedReason,
		Error:                actionError,
		MetadataJSON:         string(metadata),
		CreatedAt:            time.Now(),
		CompletedAt:          completedAt,
	}); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "constraint") {
			return nil
		}
		return err
	}
	return nil
}

func (r *RepositoryMonitorReconciler) repositoryMonitorWorkActionCancelled(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, commandID, actionKind string) (bool, error) {
	if r.Store == nil || monitor == nil || strings.TrimSpace(commandID) == "" {
		return false, nil
	}
	desiredAction := store.RepositoryMonitorDesiredActionForActionKind(actionKind)
	if desiredAction == "" {
		desiredAction = store.RepositoryMonitorDesiredActionForIntent(actionKind)
	}
	if desiredAction == "" {
		return false, nil
	}
	action, err := r.Store.GetWorkAction(ctx, monitor.Namespace, store.RepositoryMonitorWorkActionID(commandID, desiredAction))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return action.Status == repositoryMonitorWorkActionStatusCancelled, nil
}

func (r *RepositoryMonitorReconciler) recordRepositoryMonitorGitHubMutation(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, record *store.GitHubMutationRecord) error {
	if r.Store == nil || monitor == nil || record == nil {
		return nil
	}
	if record.ID == "" {
		sum := sha256.Sum256([]byte(record.Operation + "|" + record.TargetKind + "|" + fmt.Sprint(record.TargetNumber) + "|" + record.TargetSHA + "|" + record.Reason + "|" + record.GitHubURL + "|" + record.Status))
		record.ID = "ghmut-" + hex.EncodeToString(sum[:])[:16]
	}
	record.MonitorNamespace = monitor.Namespace
	record.MonitorName = monitor.Name
	record.MonitorGeneration = monitor.Generation
	if record.Actor == "" {
		record.Actor = "orka-controller"
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now()
	}
	if err := r.Store.CreateGitHubMutationRecord(ctx, record); err != nil && !strings.Contains(strings.ToLower(err.Error()), "constraint") {
		return err
	}
	metrics.RecordRepositoryMonitorGitHubMutation(record.Operation, record.Status)
	return nil
}

func (r *RepositoryMonitorReconciler) updateRepositoryMonitorGitHubMutation(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, record *store.GitHubMutationRecord) error {
	if r.Store == nil || monitor == nil || record == nil {
		return nil
	}
	record.MonitorNamespace = monitor.Namespace
	record.MonitorName = monitor.Name
	record.MonitorGeneration = monitor.Generation
	if record.Actor == "" {
		record.Actor = "orka-controller"
	}
	if err := r.Store.UpdateGitHubMutationRecord(ctx, record); err != nil {
		return err
	}
	metrics.RecordRepositoryMonitorGitHubMutation(record.Operation, record.Status)
	return nil
}

func (r *RepositoryMonitorReconciler) recordImplementationJobQueued(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, command *store.CommandEvent, item *store.MonitorItem, taskName, branch, planID string) error {
	if r.Store == nil || monitor == nil || command == nil || item == nil {
		return nil
	}
	id := repositoryMonitorImplementationJobID(taskName)
	if _, err := r.Store.GetImplementationJob(ctx, monitor.Namespace, id); err == nil {
		return nil
	} else if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	jobs, _, err := r.Store.ListImplementationJobs(ctx, store.ImplementationJobFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, IssueNumber: item.Number, Limit: 100})
	if err != nil {
		return err
	}
	return r.Store.CreateImplementationJob(ctx, &store.ImplementationJob{
		ID:                id,
		MonitorNamespace:  monitor.Namespace,
		MonitorName:       monitor.Name,
		Repo:              repositoryMonitorCanonicalRepo(monitor),
		IssueNumber:       item.Number,
		PlanID:            planID,
		SnapshotDigest:    item.SnapshotDigest,
		Phase:             repositoryMonitorIssuePhaseImplementationQueued,
		Attempt:           len(jobs) + 1,
		Branch:            branch,
		ValidationState:   "pending",
		TaskName:          taskName,
		CommandEventID:    command.ID,
		WorkActionID:      store.RepositoryMonitorWorkActionID(command.ID, store.RepositoryMonitorDesiredActionForActionKind(repositoryMonitorIssueActionImplementation)),
		MonitorGeneration: monitor.Generation,
		CreatedAt:         time.Now(),
	})
}

func repositoryMonitorCanonicalRepo(monitor *corev1alpha1.RepositoryMonitor) string {
	if monitor == nil {
		return ""
	}
	if owner, repository, err := security.ParseGitHubRepositoryURL(monitor.Spec.RepoURL); err == nil {
		return owner + "/" + repository
	}
	return strings.Trim(strings.TrimSpace(monitor.Spec.Owner)+"/"+strings.TrimSpace(monitor.Spec.Repository), "/")
}

func (r *RepositoryMonitorReconciler) updateImplementationJobForTask(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, taskName string, mutate func(*store.ImplementationJob)) error {
	if r.Store == nil || monitor == nil || strings.TrimSpace(taskName) == "" {
		return nil
	}
	jobs, _, err := r.Store.ListImplementationJobs(ctx, store.ImplementationJobFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, TaskName: taskName, Limit: 1})
	if err != nil || len(jobs) == 0 {
		return err
	}
	job := jobs[0]
	mutate(&job)
	return r.Store.UpdateImplementationJob(ctx, &job)
}

func (r *RepositoryMonitorReconciler) cancelRepositoryMonitorImplementationJobs(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, issueNumber int64, reason string) error {
	if r == nil || r.Store == nil || monitor == nil {
		return nil
	}
	cursor := ""
	for {
		jobs, next, err := r.Store.ListImplementationJobs(ctx, store.ImplementationJobFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, IssueNumber: issueNumber, Limit: 200, Cursor: cursor})
		if err != nil {
			return err
		}
		for i := range jobs {
			job := jobs[i]
			if !repositoryMonitorImplementationJobActive(job.Phase) {
				continue
			}
			now := time.Now()
			job.Phase = repositoryMonitorWorkActionStatusCancelled
			job.Error = reason
			job.CompletedAt = &now
			if err := r.Store.UpdateImplementationJob(ctx, &job); err != nil {
				return err
			}
		}
		if next == "" {
			return nil
		}
		cursor = next
	}
}

func (r *RepositoryMonitorReconciler) cancelRepositoryMonitorTargetTasks(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, targetKind string, targetNumber int64, reason string) error {
	if r == nil || r.Client == nil || monitor == nil {
		return nil
	}
	var tasks corev1alpha1.TaskList
	if err := r.List(ctx, &tasks,
		crclient.InNamespace(monitor.Namespace),
		crclient.MatchingLabels{
			labels.LabelRepositoryMonitor: labels.SelectorValue(monitor.Name),
			labels.LabelGitHubTarget:      labels.SelectorValue(targetKind),
			labels.LabelGitHubNumber:      labels.SelectorValue(fmt.Sprintf("%d", targetNumber)),
		},
	); err != nil {
		return err
	}
	for i := range tasks.Items {
		key := types.NamespacedName{Namespace: tasks.Items[i].Namespace, Name: tasks.Items[i].Name}
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			var current corev1alpha1.Task
			if err := r.Get(ctx, key, &current); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return err
			}
			if repositoryMonitorReviewTaskTerminal(current.Status.Phase) {
				return nil
			}
			now := metav1.Now()
			current.Status.Phase = corev1alpha1.TaskPhaseCancelled
			current.Status.CompletionTime = &now
			current.Status.Message = reason
			if err := r.Status().Update(ctx, &current); apierrors.IsNotFound(err) {
				return nil
			} else {
				return err
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

func repositoryMonitorImplementationJobID(taskName string) string {
	return "impl-" + repositoryMonitorShortHash(taskName)
}

func repositoryMonitorWorkActionReasonFields(status, reason string) (blockedReason, actionError string) {
	switch status {
	case repositoryMonitorWorkActionStatusBlocked:
		return reason, ""
	case repositoryMonitorWorkActionStatusFailed:
		return "", reason
	default:
		return "", ""
	}
}

func runID(run *store.MonitorRun) string {
	if run == nil {
		return ""
	}
	return run.ID
}

func firstNonEmptyWorkflow(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
