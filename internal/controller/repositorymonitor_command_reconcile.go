package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
)

const (
	repositoryMonitorTriggerLabelCommand = "github_label_command"
	repositoryMonitorCommandRetryDelay   = 30 * time.Second
	repositoryMonitorCommandMaxRetries   = 3
)

func (r *RepositoryMonitorReconciler) enqueueAcceptedRepositoryMonitorCommands(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (bool, error) {
	queued := false
	cursor := ""
	for {
		commands, next, err := r.Store.ListCommandEvents(ctx, store.CommandEventFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, Status: "accepted", Limit: 100, Cursor: cursor})
		if err != nil {
			return false, err
		}
		for i := range commands {
			command := commands[i]
			if strings.TrimSpace(command.Kind) == "" || command.Number == 0 {
				continue
			}
			runID := repositoryMonitorCommandRunIDFromCommand(command.ID)
			if handled, resetQueued, err := r.ensureNoExistingCommandRunBlocksQueue(ctx, monitor, command, runID); err != nil {
				return queued, err
			} else if handled {
				queued = queued || resetQueued
				continue
			}
			run := &store.MonitorRun{
				ID:               runID,
				MonitorNamespace: monitor.Namespace,
				MonitorName:      monitor.Name,
				Trigger:          repositoryMonitorTriggerLabelCommand,
				TargetKind:       command.Kind,
				TargetNumber:     command.Number,
				TargetSHA:        command.HeadSHA,
				CommandEventID:   command.ID,
				Phase:            repositoryMonitorRunPhaseQueued,
				StartedAt:        time.Now(),
			}
			if err := r.Store.CreateMonitorRun(ctx, run); err != nil {
				if errors.Is(err, store.ErrConflict) {
					continue
				}
				return queued, err
			}
			queued = true
			if err := r.createMonitorEvent(ctx, monitor, run.ID, command.Kind, command.Number, command.HeadSHA, "command_run_queued", fmt.Sprintf("Queued monitor run for accepted command %s", command.ID), map[string]any{"commandEventID": command.ID, "intent": command.Intent}); err != nil {
				return queued, err
			}
		}
		if next == "" {
			return queued, nil
		}
		cursor = next
	}
}

func (r *RepositoryMonitorReconciler) ensureNoExistingCommandRunBlocksQueue(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, command store.CommandEvent, runID string) (bool, bool, error) {
	terminal, err := r.repositoryMonitorCommandWorkActionTerminal(ctx, monitor, command)
	if err != nil || terminal {
		return terminal, false, err
	}
	run, err := r.repositoryMonitorCommandRun(ctx, monitor, command, runID)
	if errors.Is(err, store.ErrNotFound) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if run.Phase == repositoryMonitorRunPhaseSucceeded {
		item, itemErr := r.Store.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, command.Kind, fmt.Sprintf("%d", command.Number))
		pending := itemErr == nil && ((command.Intent == repositoryMonitorCommandIntentAutomerge && item.AutomergeState == repositoryMonitorAutomergeStatePending) ||
			(command.Intent == repositoryMonitorCommandIntentUpdateBranch && item.RepairState == repositoryMonitorRepairPhaseQueued))
		if pending {
			now := time.Now()
			run.Phase = repositoryMonitorRunPhaseQueued
			run.StartedAt = now
			run.CompletedAt = nil
			run.Error = ""
			if err := r.Store.UpdateMonitorRun(ctx, run); err != nil {
				return false, false, err
			}
			return true, true, nil
		}
	}
	if run.Phase != repositoryMonitorRunPhaseFailed {
		return true, false, nil
	}
	if command.Intent == repositoryMonitorCommandIntentUpdateBranch {
		mutation, mutationErr := r.Store.GetGitHubMutationRecord(ctx, monitor.Namespace, repositoryMonitorUpdateBranchMutationID(command.ID))
		if mutationErr != nil && !errors.Is(mutationErr, store.ErrNotFound) {
			return false, false, mutationErr
		}
		if mutation != nil && (mutation.Status == repositoryMonitorUpdateBranchSubmitting || mutation.Status == repositoryMonitorAutomergeStatePending || mutation.Status == repositoryMonitorRunPhaseSucceeded) {
			now := time.Now()
			deadline := repositoryMonitorUpdateBranchDeadline(mutation)
			needsOutcomeVerification := strings.HasPrefix(run.Error, repositoryMonitorStaleRunError)
			if !deadline.IsZero() && !now.Before(deadline) && !needsOutcomeVerification {
				run.Error = "[run_failed] " + repositoryMonitorUpdateBranchTimeoutReason
				if err := r.Store.UpdateMonitorRun(ctx, run); err != nil {
					return false, false, err
				}
				return true, false, r.terminalizeRepositoryMonitorFailedCommand(ctx, monitor, command, run, repositoryMonitorUpdateBranchTimeoutReason)
			}
			run.Phase = repositoryMonitorRunPhaseQueued
			run.StartedAt = now.Add(repositoryMonitorCommandRetryDelay)
			if !deadline.IsZero() && deadline.Before(run.StartedAt) {
				run.StartedAt = deadline
			}
			run.CompletedAt = nil
			run.Error = ""
			if err := r.Store.UpdateMonitorRun(ctx, run); err != nil {
				return false, false, err
			}
			return true, true, nil
		}
	}
	if strings.Contains(run.Error, "failed to signal repository monitor run") {
		now := time.Now()
		run.Phase = repositoryMonitorRunPhaseQueued
		run.StartedAt = now
		run.CompletedAt = nil
		run.Error = ""
		if err := r.Store.UpdateMonitorRun(ctx, run); err != nil {
			return false, false, err
		}
		return true, true, nil
	}
	if repositoryMonitorFailedCommandRunRetryable(run.Error) {
		events, _, err := r.Store.ListMonitorEvents(ctx, store.MonitorEventFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, RunID: run.ID, EventType: "run_failed", Limit: repositoryMonitorCommandMaxRetries})
		if err != nil {
			return false, false, err
		}
		if len(events) < repositoryMonitorCommandMaxRetries {
			now := time.Now()
			run.Phase = repositoryMonitorRunPhaseQueued
			run.StartedAt = now.Add(repositoryMonitorCommandRetryDelay)
			run.CompletedAt = nil
			run.Error = ""
			if err := r.Store.UpdateMonitorRun(ctx, run); err != nil {
				return false, false, err
			}
			return true, true, nil
		}
		run.Error = "[run_failed] retry_attempts_exhausted"
		if err := r.Store.UpdateMonitorRun(ctx, run); err != nil {
			return false, false, err
		}
		return true, false, r.terminalizeRepositoryMonitorFailedCommand(ctx, monitor, command, run, "retry_attempts_exhausted")
	}
	return true, false, r.terminalizeRepositoryMonitorFailedCommand(ctx, monitor, command, run, run.Error)
}

func (r *RepositoryMonitorReconciler) repositoryMonitorCommandRun(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, command store.CommandEvent, runID string) (*store.MonitorRun, error) {
	run, err := r.Store.GetMonitorRun(ctx, monitor.Namespace, runID)
	if err == nil {
		if run.CommandEventID != command.ID {
			return nil, fmt.Errorf("monitor run %q belongs to command %q, not %q", run.ID, run.CommandEventID, command.ID)
		}
		return run, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	cursor := ""
	for {
		runs, next, err := r.Store.ListMonitorRuns(ctx, store.MonitorRunFilter{
			Namespace:    monitor.Namespace,
			MonitorName:  monitor.Name,
			TargetKind:   command.Kind,
			TargetNumber: command.Number,
			TargetSHA:    command.HeadSHA,
			Limit:        100,
			Cursor:       cursor,
		})
		if err != nil {
			return nil, err
		}
		for i := range runs {
			if runs[i].CommandEventID == command.ID {
				return &runs[i], nil
			}
		}
		if next == "" {
			return nil, store.ErrNotFound
		}
		cursor = next
	}
}

func (r *RepositoryMonitorReconciler) repositoryMonitorCommandWorkActionTerminal(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, command store.CommandEvent) (bool, error) {
	actionKind := repositoryMonitorCommandActionKind(command.Intent)
	actionID := store.RepositoryMonitorWorkActionID(command.ID, store.RepositoryMonitorDesiredActionForActionKind(actionKind))
	action, err := r.Store.GetWorkAction(ctx, monitor.Namespace, actionID)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if action.Status == repositoryMonitorWorkActionStatusCancelled && command.Intent == repositoryMonitorCommandIntentUpdateBranch {
		reconcile, err := r.repositoryMonitorUpdateBranchRequiresReconciliation(ctx, monitor, command.ID)
		if err != nil || reconcile {
			return false, err
		}
	}
	switch action.Status {
	case repositoryMonitorWorkActionStatusSucceeded, repositoryMonitorWorkActionStatusFailed, repositoryMonitorWorkActionStatusBlocked, repositoryMonitorWorkActionStatusCancelled:
		now := time.Now()
		command.Status = "processed"
		command.ProcessedAt = &now
		command.Error = firstNonEmptyWorkflow(action.Error, action.BlockedReason)
		if err := r.Store.UpdateCommandEvent(ctx, &command); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, nil
	}
}

func (r *RepositoryMonitorReconciler) terminalizeRepositoryMonitorFailedCommand(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, command store.CommandEvent, run *store.MonitorRun, reason string) error {
	actionKind := repositoryMonitorCommandActionKind(command.Intent)
	if command.Intent == repositoryMonitorCommandIntentAutomerge {
		preserveSuccess, err := r.terminalizeRepositoryMonitorAutomerge(ctx, monitor, command, reason)
		if err != nil {
			return err
		}
		if preserveSuccess {
			return r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, &command, command.Kind, command.Number, command.HeadSHA, command.IssueSnapshotDigest, actionKind, repositoryMonitorWorkActionStatusSucceeded, repositoryMonitorAutomergeStateMerged, "", "")
		}
	}
	if command.Intent == repositoryMonitorCommandIntentUpdateBranch {
		preserveSuccess, err := r.terminalizeRepositoryMonitorUpdateBranch(ctx, monitor, command, reason)
		if err != nil {
			return err
		}
		if preserveSuccess {
			return r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, &command, command.Kind, command.Number, command.HeadSHA, command.IssueSnapshotDigest, actionKind, repositoryMonitorWorkActionStatusSucceeded, repositoryMonitorRepairPhaseSucceeded, "", "")
		}
	}
	desiredAction := store.RepositoryMonitorDesiredActionForActionKind(actionKind)
	actionID := store.RepositoryMonitorWorkActionID(command.ID, desiredAction)
	if existing, err := r.Store.GetWorkAction(ctx, monitor.Namespace, actionID); err == nil {
		switch existing.Status {
		case repositoryMonitorWorkActionStatusSucceeded, repositoryMonitorWorkActionStatusFailed, repositoryMonitorWorkActionStatusBlocked, repositoryMonitorWorkActionStatusCancelled:
			return nil
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	return r.recordRepositoryMonitorWorkActionState(ctx, monitor, run, &command, command.Kind, command.Number, command.HeadSHA, command.IssueSnapshotDigest, actionKind, repositoryMonitorWorkActionStatusFailed, "run_failed", "", reason)
}

func (r *RepositoryMonitorReconciler) terminalizeRepositoryMonitorAutomerge(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, command store.CommandEvent, reason string) (bool, error) {
	mutationID := "ghmut-" + repositoryMonitorShortHash(command.ID+"-merge")
	mutation, err := r.Store.GetGitHubMutationRecord(ctx, monitor.Namespace, mutationID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return false, err
	}
	item, itemErr := r.Store.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, fmt.Sprintf("%d", command.Number))
	if itemErr != nil && !errors.Is(itemErr, store.ErrNotFound) {
		return false, itemErr
	}
	if mutation != nil && mutation.Status == repositoryMonitorRunPhaseSucceeded {
		if item != nil && strings.TrimSpace(command.HeadSHA) != "" && strings.TrimSpace(item.HeadSHA) == strings.TrimSpace(command.HeadSHA) {
			item.State = repositoryMonitorAutomergeStateMerged
			item.AutomergeState = repositoryMonitorAutomergeStateMerged
			item.SkipReason = ""
			if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	sameHead := item != nil && strings.TrimSpace(command.HeadSHA) != "" && strings.TrimSpace(item.HeadSHA) == strings.TrimSpace(command.HeadSHA)
	if sameHead && (item.AutomergeState == repositoryMonitorAutomergeStateMerged || strings.EqualFold(strings.TrimSpace(item.State), repositoryMonitorAutomergeStateMerged)) {
		if mutation != nil {
			mutation.Status = repositoryMonitorRunPhaseSucceeded
			mutation.Error = ""
			if err := r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	if item != nil && strings.TrimSpace(command.HeadSHA) != "" && strings.TrimSpace(item.HeadSHA) == strings.TrimSpace(command.HeadSHA) {
		item.AutomergeState = repositoryMonitorAutomergeStateFailed
		item.SkipReason = reason
		if err := r.Store.UpsertMonitorItem(ctx, item); err != nil {
			return false, err
		}
	}
	if mutation == nil {
		return false, nil
	}
	mutation.Status = repositoryMonitorRunPhaseFailed
	mutation.Error = reason
	return false, r.updateRepositoryMonitorGitHubMutation(ctx, monitor, mutation)
}

func repositoryMonitorCommandActionKind(intent string) string {
	switch strings.TrimSpace(intent) {
	case repositoryMonitorCommandIntentReview:
		return "pr_review"
	case repositoryMonitorCommandIntentFix:
		return "pr_repair"
	case repositoryMonitorCommandIntentAutomerge:
		return repositoryMonitorActionAutomerge
	case repositoryMonitorCommandIntentApprovePlan:
		return repositoryMonitorIssueActionApprove
	default:
		return strings.TrimSpace(intent)
	}
}

func repositoryMonitorFailedCommandRunRetryable(runError string) bool {
	for _, state := range []string{"github_rate_limited", repositoryMonitorRunRetryScheduled, "cluster_capacity_blocked", "llm_rate_limited"} {
		if strings.HasPrefix(strings.TrimSpace(runError), "["+state+"]") {
			return true
		}
	}
	return false
}

func repositoryMonitorCommandRunIDFromCommand(commandID string) string {
	sum := sha256.Sum256([]byte(commandID + "|run"))
	return "monrun-" + hex.EncodeToString(sum[:])[:12]
}
