package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

const (
	publishPhaseFailed = "failed"
	testPhaseSucceeded = "succeeded"
)

func TestRepositoryMonitorStoreCRUD(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	monitor := &store.RepositoryMonitorRecord{
		Namespace:  "demo",
		Name:       "orka",
		UID:        "uid-1",
		RepoURL:    "https://github.com/orka-agents/orka",
		Owner:      "sozercan",
		Repository: "orka",
		Branch:     "main",
		Generation: 1,
	}
	if err := s.UpsertRepositoryMonitor(ctx, monitor); err != nil {
		t.Fatalf("UpsertRepositoryMonitor() error = %v", err)
	}
	monitor.Generation = 2
	monitor.Branch = "develop"
	if err := s.UpsertRepositoryMonitor(ctx, monitor); err != nil {
		t.Fatalf("UpsertRepositoryMonitor(update) error = %v", err)
	}

	got, err := s.GetRepositoryMonitor(ctx, "demo", "orka")
	if err != nil {
		t.Fatalf("GetRepositoryMonitor() error = %v", err)
	}
	if got.Generation != 2 || got.Branch != "develop" {
		t.Fatalf("monitor = %#v, want updated generation and branch", got)
	}
}

//nolint:gocyclo // This store smoke test intentionally covers related monitor tables together.
func TestMonitorStoreRunsItemsReviewsRepairsAndEvents(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	if err := s.CreateMonitorRun(ctx, &store.MonitorRun{
		ID:               "run-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Trigger:          "manual",
		TargetKind:       "pull_request",
		TargetNumber:     42,
		Phase:            "queued",
	}); err != nil {
		t.Fatalf("CreateMonitorRun() error = %v", err)
	}
	runs, _, err := s.ListMonitorRuns(ctx, store.MonitorRunFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 1 || runs[0].ID != "run-1" {
		t.Fatalf("runs = %#v, want run-1", runs)
	}

	if err := s.UpsertMonitorItem(ctx, &store.MonitorItem{
		MonitorNamespace:  "demo",
		MonitorName:       "orka",
		Kind:              "pull_request",
		Number:            42,
		Title:             "test pr",
		State:             "open",
		HeadSHA:           "abc123",
		LastVerdict:       "needs_changes",
		LastPublishID:     "publish-1",
		LastPublishPhase:  testPhaseSucceeded,
		LastPublishReason: "",
		LastPublishURL:    "https://github.example/review/1",
	}); err != nil {
		t.Fatalf("UpsertMonitorItem() error = %v", err)
	}
	item, err := s.GetMonitorItem(ctx, "demo", "orka", "pull_request", "42")
	if err != nil {
		t.Fatalf("GetMonitorItem() error = %v", err)
	}
	if item.HeadSHA != "abc123" || item.LastVerdict != "needs_changes" || item.LastPublishID != "publish-1" || item.LastPublishURL == "" {
		t.Fatalf("item = %#v, want stored head SHA, verdict, and publish status", item)
	}

	if err := s.CreateReviewRecord(ctx, &store.ReviewRecord{
		ID:                      "review-1",
		MonitorNamespace:        "demo",
		MonitorName:             "orka",
		Kind:                    "pull_request",
		Number:                  42,
		HeadSHA:                 "abc123",
		Verdict:                 "needs_changes",
		ValidationTask:          "review-1-validation",
		ValidationImage:         "ghcr.io/example/validation:1",
		ValidationCommandDigest: "sha256:1bb497e3e13a1105cf24e3359fa3ef75de08b66ff8a2839cd7f9ea97824d9eb3",
		ValidationStatus:        "failed",
		ValidationEvidence:      "package example failed",
	}); err != nil {
		t.Fatalf("CreateReviewRecord() error = %v", err)
	}
	reviews, _, err := s.ListReviewRecords(ctx, store.ReviewRecordFilter{Namespace: "demo", MonitorName: "orka", Number: 42})
	if err != nil {
		t.Fatalf("ListReviewRecords() error = %v", err)
	}
	if len(reviews) != 1 || reviews[0].ID != "review-1" || reviews[0].ValidationTask != "review-1-validation" || reviews[0].ValidationImage != "ghcr.io/example/validation:1" || reviews[0].ValidationCommandDigest != "sha256:1bb497e3e13a1105cf24e3359fa3ef75de08b66ff8a2839cd7f9ea97824d9eb3" || reviews[0].ValidationStatus != "failed" || reviews[0].ValidationEvidence != "package example failed" {
		t.Fatalf("reviews = %#v, want review-1", reviews)
	}

	if err := s.CreateReviewPublishRecord(ctx, &store.ReviewPublishRecord{
		ID:                 "publish-1",
		MonitorNamespace:   "demo",
		MonitorName:        "orka",
		ItemKind:           "pull_request",
		ItemNumber:         42,
		HeadSHA:            "abc123",
		ReviewRecordID:     "review-1",
		Phase:              testPhaseSucceeded,
		Event:              "COMMENT",
		GitHubReviewID:     "123",
		GitHubReviewURL:    "https://github.example/review/123",
		BodyDigest:         "sha256:abc",
		InlineCommentCount: 2,
	}); err != nil {
		t.Fatalf("CreateReviewPublishRecord() error = %v", err)
	}
	publishRecord, err := s.GetReviewPublishRecord(ctx, "demo", "publish-1")
	if err != nil {
		t.Fatalf("GetReviewPublishRecord() error = %v", err)
	}
	if publishRecord.GitHubReviewID != "123" || publishRecord.InlineCommentCount != 2 {
		t.Fatalf("publishRecord = %#v, want GitHub review outcome", publishRecord)
	}
	publishRecord.Phase = publishPhaseFailed
	publishRecord.Error = "ambiguous GitHub response"
	if err := s.UpdateReviewPublishRecord(ctx, publishRecord); err != nil {
		t.Fatalf("UpdateReviewPublishRecord() error = %v", err)
	}
	publishRecord, err = s.GetReviewPublishRecord(ctx, "demo", "publish-1")
	if err != nil {
		t.Fatalf("GetReviewPublishRecord(after update) error = %v", err)
	}
	if publishRecord.Phase != publishPhaseFailed || publishRecord.Error == "" {
		t.Fatalf("publishRecord = %#v, want updated failure outcome", publishRecord)
	}
	publishRecord.Phase = testPhaseSucceeded
	publishRecord.Error = ""
	if err := s.UpdateReviewPublishRecord(ctx, publishRecord); err != nil {
		t.Fatalf("UpdateReviewPublishRecord(succeeded) error = %v", err)
	}
	publishRecords, _, err := s.ListReviewPublishRecords(ctx, store.ReviewPublishRecordFilter{Namespace: "demo", MonitorName: "orka", ItemNumber: 42, HeadSHA: "abc123", Phase: testPhaseSucceeded})
	if err != nil {
		t.Fatalf("ListReviewPublishRecords() error = %v", err)
	}
	if len(publishRecords) != 1 || publishRecords[0].ID != "publish-1" {
		t.Fatalf("publishRecords = %#v, want publish-1", publishRecords)
	}

	if err := s.CreateRepairJob(ctx, &store.RepairJob{
		ID:               "repair-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Repo:             "orka-agents/orka",
		PRNumber:         42,
		Intent:           "fix_ci",
		BaseBranch:       "main",
		Phase:            "queued",
	}); err != nil {
		t.Fatalf("CreateRepairJob() error = %v", err)
	}
	repairs, _, err := s.ListRepairJobs(ctx, store.RepairJobFilter{Namespace: "demo", MonitorName: "orka", PRNumber: 42})
	if err != nil {
		t.Fatalf("ListRepairJobs() error = %v", err)
	}
	if len(repairs) != 1 || repairs[0].ID != "repair-1" || repairs[0].BaseBranch != "main" {
		t.Fatalf("repairs = %#v, want repair-1", repairs)
	}

	if err := s.CreateMonitorEvent(ctx, &store.MonitorEvent{
		ID:               "event-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		RunID:            "run-1",
		ItemKind:         "pull_request",
		ItemNumber:       42,
		EventType:        "run_queued",
		Summary:          "manual run queued",
	}); err != nil {
		t.Fatalf("CreateMonitorEvent() error = %v", err)
	}
	events, _, err := s.ListMonitorEvents(ctx, store.MonitorEventFilter{Namespace: "demo", MonitorName: "orka", RunID: "run-1"})
	if err != nil {
		t.Fatalf("ListMonitorEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].ID != "event-1" {
		t.Fatalf("events = %#v, want event-1", events)
	}
	events, _, err = s.ListMonitorEvents(ctx, store.MonitorEventFilter{Namespace: "demo", ID: "missing-event"})
	if err != nil {
		t.Fatalf("ListMonitorEvents(ID) error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("events filtered by missing ID = %#v, want none", events)
	}
}

func TestCreateMonitorRunRejectsDuplicateActiveRun(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	if err := s.CreateMonitorRun(ctx, &store.MonitorRun{
		ID:               "run-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Trigger:          "manual",
		Phase:            "queued",
	}); err != nil {
		t.Fatalf("CreateMonitorRun() error = %v", err)
	}
	if err := s.CreateMonitorRun(ctx, &store.MonitorRun{
		ID:               "run-2",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Trigger:          "manual",
		Phase:            "queued",
	}); err != nil {
		t.Fatalf("CreateMonitorRun(second queued) error = %v", err)
	}
	if err := s.CreateMonitorRun(ctx, &store.MonitorRun{
		ID:               "run-3",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Trigger:          "manual",
		Phase:            "running",
	}); err != nil {
		t.Fatalf("CreateMonitorRun(running behind queued) error = %v", err)
	}
	if err := s.CreateMonitorRun(ctx, &store.MonitorRun{
		ID:               "run-4",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Trigger:          "manual",
		Phase:            "running",
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("CreateMonitorRun(duplicate running) error = %v, want ErrConflict", err)
	}
	if err := s.CreateMonitorRun(ctx, &store.MonitorRun{
		ID:               "run-5",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Trigger:          "manual",
		Phase:            testPhaseSucceeded,
	}); err != nil {
		t.Fatalf("CreateMonitorRun(succeeded) error = %v", err)
	}
}

func TestUpsertRepositoryMonitorBranchChangeClearsDependentState(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	if err := s.UpsertRepositoryMonitor(ctx, &store.RepositoryMonitorRecord{
		Namespace:  "demo",
		Name:       "orka",
		UID:        "uid-1",
		RepoURL:    "https://github.com/orka-agents/orka",
		Owner:      "sozercan",
		Repository: "orka",
		Branch:     "main",
	}); err != nil {
		t.Fatalf("UpsertRepositoryMonitor() error = %v", err)
	}
	if err := s.CreateMonitorRun(ctx, &store.MonitorRun{
		ID:               "run-main",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Trigger:          "manual",
		Phase:            "queued",
	}); err != nil {
		t.Fatalf("CreateMonitorRun() error = %v", err)
	}
	if err := s.UpsertMonitorItem(ctx, &store.MonitorItem{
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Kind:             "pull_request",
		Number:           7,
		State:            "open",
	}); err != nil {
		t.Fatalf("UpsertMonitorItem() error = %v", err)
	}

	if err := s.UpsertRepositoryMonitor(ctx, &store.RepositoryMonitorRecord{
		Namespace:  "demo",
		Name:       "orka",
		UID:        "uid-1",
		RepoURL:    "https://github.com/orka-agents/orka",
		Owner:      "sozercan",
		Repository: "orka",
		Branch:     "release",
	}); err != nil {
		t.Fatalf("UpsertRepositoryMonitor(branch change) error = %v", err)
	}

	got, err := s.GetRepositoryMonitor(ctx, "demo", "orka")
	if err != nil {
		t.Fatalf("GetRepositoryMonitor() error = %v", err)
	}
	if got.Branch != "release" {
		t.Fatalf("branch = %q, want release", got.Branch)
	}
	runs, _, err := s.ListMonitorRuns(ctx, store.MonitorRunFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	items, _, err := s.ListMonitorItems(ctx, store.MonitorItemFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorItems() error = %v", err)
	}
	if len(runs) != 0 || len(items) != 0 {
		t.Fatalf("dependent branch state remains: runs=%d items=%d", len(runs), len(items))
	}
}

func TestDeleteRepositoryMonitorCleansDependentStateWithoutMetadata(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	if err := s.CreateMonitorRun(ctx, &store.MonitorRun{
		ID:               "orphan-run",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Trigger:          "manual",
		Phase:            "queued",
	}); err != nil {
		t.Fatalf("CreateMonitorRun() error = %v", err)
	}

	if err := s.DeleteRepositoryMonitor(ctx, "demo", "orka"); err != nil {
		t.Fatalf("DeleteRepositoryMonitor() error = %v", err)
	}

	runs, _, err := s.ListMonitorRuns(ctx, store.MonitorRunFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %#v, want orphaned run removed", runs)
	}
	if err := s.CreateMonitorRun(ctx, &store.MonitorRun{
		ID:               "new-run",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Trigger:          "manual",
		Phase:            "queued",
	}); err != nil {
		t.Fatalf("CreateMonitorRun(after cleanup) error = %v", err)
	}
}

func TestUpsertRepositoryMonitorIdentityChangeClearsDependentState(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	if err := s.UpsertRepositoryMonitor(ctx, &store.RepositoryMonitorRecord{
		Namespace:  "demo",
		Name:       "orka",
		UID:        "uid-1",
		RepoURL:    "https://github.com/orka-agents/orka",
		Owner:      "sozercan",
		Repository: "orka",
		Branch:     "main",
	}); err != nil {
		t.Fatalf("UpsertRepositoryMonitor() error = %v", err)
	}
	if err := s.CreateMonitorRun(ctx, &store.MonitorRun{
		ID:               "run-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Trigger:          "manual",
		Phase:            "queued",
	}); err != nil {
		t.Fatalf("CreateMonitorRun() error = %v", err)
	}
	if err := s.UpsertMonitorItem(ctx, &store.MonitorItem{
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Kind:             "pull_request",
		Number:           42,
		State:            "open",
	}); err != nil {
		t.Fatalf("UpsertMonitorItem() error = %v", err)
	}
	if err := s.CreateReviewRecord(ctx, &store.ReviewRecord{
		ID:               "review-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Kind:             "pull_request",
		Number:           42,
	}); err != nil {
		t.Fatalf("CreateReviewRecord() error = %v", err)
	}
	if err := s.CreateCommandEvent(ctx, &store.CommandEvent{
		ID:               "command-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
	}); err != nil {
		t.Fatalf("CreateCommandEvent() error = %v", err)
	}
	if err := s.CreateRepairJob(ctx, &store.RepairJob{
		ID:               "repair-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
	}); err != nil {
		t.Fatalf("CreateRepairJob() error = %v", err)
	}
	if err := s.CreateMonitorEvent(ctx, &store.MonitorEvent{
		ID:               "event-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		EventType:        "queued",
	}); err != nil {
		t.Fatalf("CreateMonitorEvent() error = %v", err)
	}

	if err := s.UpsertRepositoryMonitor(ctx, &store.RepositoryMonitorRecord{
		Namespace:  "demo",
		Name:       "orka",
		UID:        "uid-2",
		RepoURL:    "https://github.com/example/other",
		Owner:      "example",
		Repository: "other",
		Branch:     "main",
	}); err != nil {
		t.Fatalf("UpsertRepositoryMonitor(recreate) error = %v", err)
	}

	got, err := s.GetRepositoryMonitor(ctx, "demo", "orka")
	if err != nil {
		t.Fatalf("GetRepositoryMonitor() error = %v", err)
	}
	if got.UID != "uid-2" || got.Owner != "example" || got.Repository != "other" {
		t.Fatalf("monitor = %#v, want replacement identity", got)
	}
	runs, _, err := s.ListMonitorRuns(ctx, store.MonitorRunFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	items, _, err := s.ListMonitorItems(ctx, store.MonitorItemFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorItems() error = %v", err)
	}
	reviews, _, err := s.ListReviewRecords(ctx, store.ReviewRecordFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListReviewRecords() error = %v", err)
	}
	repairs, _, err := s.ListRepairJobs(ctx, store.RepairJobFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListRepairJobs() error = %v", err)
	}
	events, _, err := s.ListMonitorEvents(ctx, store.MonitorEventFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorEvents() error = %v", err)
	}
	if len(runs)+len(items)+len(reviews)+len(repairs)+len(events) != 0 {
		t.Fatalf("dependent state remains: runs=%d items=%d reviews=%d repairs=%d events=%d", len(runs), len(items), len(reviews), len(repairs), len(events))
	}
	if _, err := s.GetCommandEvent(ctx, "demo", "command-1"); err != store.ErrNotFound {
		t.Fatalf("GetCommandEvent() error = %v, want ErrNotFound", err)
	}
}

func TestDeleteRepositoryMonitorCascadesMonitorState(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	if err := s.UpsertRepositoryMonitor(ctx, &store.RepositoryMonitorRecord{
		Namespace: "demo",
		Name:      "orka",
		RepoURL:   "https://github.com/orka-agents/orka",
	}); err != nil {
		t.Fatalf("UpsertRepositoryMonitor() error = %v", err)
	}
	if err := s.CreateMonitorRun(ctx, &store.MonitorRun{
		ID:               "run-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Trigger:          "manual",
		Phase:            "queued",
	}); err != nil {
		t.Fatalf("CreateMonitorRun() error = %v", err)
	}
	if err := s.UpsertMonitorItem(ctx, &store.MonitorItem{
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Kind:             "pull_request",
		Number:           42,
		State:            "open",
	}); err != nil {
		t.Fatalf("UpsertMonitorItem() error = %v", err)
	}
	if err := s.CreateReviewRecord(ctx, &store.ReviewRecord{
		ID:               "review-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		Kind:             "pull_request",
		Number:           42,
	}); err != nil {
		t.Fatalf("CreateReviewRecord() error = %v", err)
	}
	if err := s.CreateCommandEvent(ctx, &store.CommandEvent{
		ID:               "command-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
	}); err != nil {
		t.Fatalf("CreateCommandEvent() error = %v", err)
	}
	if err := s.CreateRepairJob(ctx, &store.RepairJob{
		ID:               "repair-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
	}); err != nil {
		t.Fatalf("CreateRepairJob() error = %v", err)
	}
	if err := s.CreateMonitorEvent(ctx, &store.MonitorEvent{
		ID:               "event-1",
		MonitorNamespace: "demo",
		MonitorName:      "orka",
		EventType:        "queued",
	}); err != nil {
		t.Fatalf("CreateMonitorEvent() error = %v", err)
	}

	if err := s.DeleteRepositoryMonitor(ctx, "demo", "orka"); err != nil {
		t.Fatalf("DeleteRepositoryMonitor() error = %v", err)
	}

	if _, err := s.GetRepositoryMonitor(ctx, "demo", "orka"); err != store.ErrNotFound {
		t.Fatalf("GetRepositoryMonitor() error = %v, want ErrNotFound", err)
	}
	runs, _, err := s.ListMonitorRuns(ctx, store.MonitorRunFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorRuns() error = %v", err)
	}
	items, _, err := s.ListMonitorItems(ctx, store.MonitorItemFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorItems() error = %v", err)
	}
	reviews, _, err := s.ListReviewRecords(ctx, store.ReviewRecordFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListReviewRecords() error = %v", err)
	}
	repairs, _, err := s.ListRepairJobs(ctx, store.RepairJobFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListRepairJobs() error = %v", err)
	}
	events, _, err := s.ListMonitorEvents(ctx, store.MonitorEventFilter{Namespace: "demo", MonitorName: "orka", Limit: 10})
	if err != nil {
		t.Fatalf("ListMonitorEvents() error = %v", err)
	}
	if len(runs)+len(items)+len(reviews)+len(repairs)+len(events) != 0 {
		t.Fatalf("dependent state remains: runs=%d items=%d reviews=%d repairs=%d events=%d", len(runs), len(items), len(reviews), len(repairs), len(events))
	}
	if _, err := s.GetCommandEvent(ctx, "demo", "command-1"); err != store.ErrNotFound {
		t.Fatalf("GetCommandEvent() error = %v, want ErrNotFound", err)
	}
}

//nolint:gocyclo // This integration-style store test intentionally exercises the full workflow schema.
func TestMonitorWorkflowStoresActionsJobsAndMutations(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	action := &store.WorkAction{
		ID:                "wa-1",
		MonitorNamespace:  "demo",
		MonitorName:       "orka",
		MonitorGeneration: 2,
		TargetKind:        "issue",
		TargetNumber:      123,
		Intent:            "implement",
		DesiredAction:     "implement",
		DedupeKey:         "dedupe-1",
		Status:            "queued",
		Phase:             "implementation_queued",
	}
	if err := s.CreateWorkAction(ctx, action); err != nil {
		t.Fatalf("CreateWorkAction() error = %v", err)
	}
	leased, err := s.GetWorkAction(ctx, "demo", "wa-1")
	if err != nil {
		t.Fatalf("GetWorkAction() error = %v", err)
	}
	leased.Status = "running"
	leased.TaskName = "task-1"
	if err := s.UpdateWorkAction(ctx, leased); err != nil {
		t.Fatalf("UpdateWorkAction() error = %v", err)
	}
	actions, _, err := s.ListWorkActions(ctx, store.WorkActionFilter{Namespace: "demo", MonitorName: "orka", TaskName: "task-1"})
	if err != nil {
		t.Fatalf("ListWorkActions() error = %v", err)
	}
	if len(actions) != 1 || actions[0].Status != "running" {
		t.Fatalf("actions = %#v, want running task action", actions)
	}
	retryPending := &store.WorkAction{
		ID:                "wa-retry",
		MonitorNamespace:  "demo",
		MonitorName:       "orka",
		TargetKind:        "issue",
		TargetNumber:      123,
		Intent:            "implement",
		DesiredAction:     "implement",
		Status:            store.RepositoryMonitorWorkActionStatusRetryPending,
		MonitorGeneration: 2,
	}
	if err := s.CreateWorkAction(ctx, retryPending); err != nil {
		t.Fatalf("CreateWorkAction(retry pending) error = %v", err)
	}
	cancelled, err := s.CancelWorkActions(ctx, "demo", "orka", "issue", 123, "stopped_by_command")
	if err != nil {
		t.Fatalf("CancelWorkActions() error = %v", err)
	}
	if cancelled != 2 {
		t.Fatalf("CancelWorkActions() = %d, want 2", cancelled)
	}
	gotAction, err := s.GetWorkAction(ctx, "demo", "wa-1")
	if err != nil {
		t.Fatalf("GetWorkAction() error = %v", err)
	}
	if gotAction.Status != "cancelled" || gotAction.BlockedReason != "stopped_by_command" || gotAction.CompletedAt == nil {
		t.Fatalf("got action = %#v, want cancelled with reason and completion", gotAction)
	}
	gotRetry, err := s.GetWorkAction(ctx, "demo", retryPending.ID)
	if err != nil {
		t.Fatalf("GetWorkAction(retry pending) error = %v", err)
	}
	if gotRetry.Status != "cancelled" || gotRetry.BlockedReason != "stopped_by_command" || gotRetry.CompletedAt == nil {
		t.Fatalf("retry action = %#v, want cancelled with reason and completion", gotRetry)
	}

	job := &store.ImplementationJob{ID: "impl-1", MonitorNamespace: "demo", MonitorName: "orka", Repo: "orka-agents/orka", IssueNumber: 123, PlanID: "act-plan", SnapshotDigest: "sha256:issue", Phase: "implementation_queued", Attempt: 1, Branch: "orka/issue-123", TaskName: "task-1"}
	if err := s.CreateImplementationJob(ctx, job); err != nil {
		t.Fatalf("CreateImplementationJob() error = %v", err)
	}
	job.Phase = "pr_opened"
	job.PatchArtifactID = "patch.json"
	job.PRNumber = 456
	if err := s.UpdateImplementationJob(ctx, job); err != nil {
		t.Fatalf("UpdateImplementationJob() error = %v", err)
	}
	jobs, _, err := s.ListImplementationJobs(ctx, store.ImplementationJobFilter{Namespace: "demo", MonitorName: "orka", IssueNumber: 123, Phase: "pr_opened"})
	if err != nil {
		t.Fatalf("ListImplementationJobs() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].PRNumber != 456 || jobs[0].PatchArtifactID != "patch.json" {
		t.Fatalf("jobs = %#v, want updated implementation job", jobs)
	}

	mutation := &store.GitHubMutationRecord{ID: "mut-1", MonitorNamespace: "demo", MonitorName: "orka", Operation: "create_pr", TargetKind: "issue", TargetNumber: 123, Status: "started"}
	if err := s.CreateGitHubMutationRecord(ctx, mutation); err != nil {
		t.Fatalf("CreateGitHubMutationRecord() error = %v", err)
	}
	mutation.Status = testPhaseSucceeded
	mutation.ExternalID = "456"
	mutation.GitHubURL = "https://github.example/pr/456"
	pendingAt := time.Now().UTC().Truncate(time.Second)
	mutation.PendingAt = &pendingAt
	if err := s.UpdateGitHubMutationRecord(ctx, mutation); err != nil {
		t.Fatalf("UpdateGitHubMutationRecord() error = %v", err)
	}
	updatedMutation, err := s.GetGitHubMutationRecord(ctx, "demo", mutation.ID)
	if err != nil {
		t.Fatalf("GetGitHubMutationRecord() error = %v", err)
	}
	if updatedMutation.Status != testPhaseSucceeded || updatedMutation.ExternalID != "456" || updatedMutation.PendingAt == nil || !updatedMutation.PendingAt.Equal(pendingAt) {
		t.Fatalf("updated mutation = %#v, want succeeded outcome", updatedMutation)
	}
	mutations, _, err := s.ListGitHubMutationRecords(ctx, store.GitHubMutationRecordFilter{Namespace: "demo", MonitorName: "orka", Operation: "create_pr", TargetKind: "issue", TargetNumber: 123})
	if err != nil {
		t.Fatalf("ListGitHubMutationRecords() error = %v", err)
	}
	if len(mutations) != 1 || mutations[0].GitHubURL == "" {
		t.Fatalf("mutations = %#v, want create_pr mutation", mutations)
	}
}
