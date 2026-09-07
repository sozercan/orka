package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

func TestRepositoryMonitorUpdateBranchRetriesSuccessProjections(t *testing.T) {
	for _, projection := range []string{"monitor item", "event", "work action"} {
		t.Run(projection, func(t *testing.T) {
			ctx := context.Background()
			monitorStore := setupControllerSQLiteStore(t)
			monitor, _ := repositoryMonitorInventoryTestObjects("success-projection-" + strings.ReplaceAll(projection, " ", "-"))
			command := &store.CommandEvent{
				ID: "cmd-success-projection", MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
				Repo: "orka-agents/orka", Kind: repositoryMonitorPullRequestKind, Number: 42,
				Intent: repositoryMonitorCommandIntentUpdateBranch, HeadSHA: "old-head-sha", Status: "accepted", CreatedAt: time.Now(),
			}
			if err := monitorStore.CreateCommandEvent(ctx, command); err != nil {
				t.Fatal(err)
			}
			run := &store.MonitorRun{
				ID: repositoryMonitorCommandRunIDFromCommand(command.ID), MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
				CommandEventID: command.ID, TargetKind: command.Kind, TargetNumber: command.Number, TargetSHA: command.HeadSHA,
				Phase: repositoryMonitorRunPhaseQueued, StartedAt: time.Now(),
			}
			if err := monitorStore.CreateMonitorRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			mutation := &store.GitHubMutationRecord{
				ID: repositoryMonitorUpdateBranchMutationID(command.ID), MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
				RunID: run.ID, CommandEventID: command.ID, Operation: repositoryMonitorUpdateBranchOperation,
				TargetKind: command.Kind, TargetNumber: command.Number, TargetSHA: command.HeadSHA,
				Status: repositoryMonitorAutomergeStatePending, CreatedAt: time.Now(),
			}
			if err := monitorStore.CreateGitHubMutationRecord(ctx, mutation); err != nil {
				t.Fatal(err)
			}
			pr := repositoryMonitorPullRequest{Number: command.Number, BaseSHA: "live-base-sha", HeadSHA: "updated-head-sha"}
			item := repositoryMonitorItemFromPullRequest(monitor, pr, nil)
			reconciler := &RepositoryMonitorReconciler{Store: monitorStore}
			if err := reconciler.recordRepositoryMonitorWorkActionState(ctx, monitor, run, command, command.Kind, command.Number, command.HeadSHA, "", command.Intent, repositoryMonitorWorkActionStatusRunning, repositoryMonitorAutomergeStatePending, "", ""); err != nil {
				t.Fatal(err)
			}
			reconciler.Store = failingUpdateBranchProjectionStore{RepositoryMonitorStore: monitorStore, projection: projection}
			projectionErr := reconciler.completeRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, pr)
			if projectionErr == nil || repositoryMonitorRunFailureState(projectionErr) != repositoryMonitorRunRetryScheduled {
				t.Fatalf("success projection error = %v, want retryable failure", projectionErr)
			}
			actionID := store.RepositoryMonitorWorkActionID(command.ID, command.Intent)
			action, err := monitorStore.GetWorkAction(ctx, monitor.Namespace, actionID)
			if err != nil || action.Status != repositoryMonitorWorkActionStatusRunning {
				t.Fatal("work action became terminal before every success projection completed")
			}

			// A proved mutation must keep retrying its projections even after
			// the normal command retry budget, without submitting it again.
			reconciler.Store = monitorStore
			run.Phase = repositoryMonitorRunPhaseFailed
			run.Error = "[" + repositoryMonitorRunRetryScheduled + "] " + projectionErr.Error()
			if err := monitorStore.UpdateMonitorRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			for i := range repositoryMonitorCommandMaxRetries {
				if err := monitorStore.CreateMonitorEvent(ctx, &store.MonitorEvent{
					ID: fmt.Sprintf("failed-%d", i), MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
					RunID: run.ID, EventType: "run_failed",
				}); err != nil {
					t.Fatal(err)
				}
			}
			blocked, queued, err := reconciler.ensureNoExistingCommandRunBlocksQueue(ctx, monitor, *command, run.ID)
			if err != nil || !blocked || !queued {
				t.Fatalf("projection retry = blocked %v, queued %v, err %v", blocked, queued, err)
			}
			for range 2 {
				mutation, err = monitorStore.GetGitHubMutationRecord(ctx, monitor.Namespace, mutation.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err := reconciler.completeRepositoryMonitorUpdateBranch(ctx, monitor, run, command, item, mutation, pr); err != nil {
					t.Fatal(err)
				}
			}
			events, _, err := monitorStore.ListMonitorEvents(ctx, store.MonitorEventFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, RunID: run.ID, EventType: "update_branch_succeeded", Limit: 10})
			if err != nil || len(events) != 1 || events[0].ItemSHA != pr.HeadSHA {
				t.Fatalf("success events = %d, err %v, want one event for the updated head", len(events), err)
			}
			action, err = monitorStore.GetWorkAction(ctx, monitor.Namespace, actionID)
			if err != nil || action.Status != repositoryMonitorWorkActionStatusSucceeded {
				t.Fatal("work action did not succeed after projection recovery")
			}
		})
	}
}
