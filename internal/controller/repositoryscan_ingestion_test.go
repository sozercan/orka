package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
	sqlitestore "github.com/orka-agents/orka/internal/store/sqlite"
)

// Exercise the real collection order: retained threat-model, mapper, and review
// Tasks from the first run precede the same stages from the next run. The old
// mapper used to reclaim shared slices, letting both runs count reviews again.
func TestRepositoryScanRetainedPipelineTasks(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "scans.db")
	db, err := sqlitestore.NewDB(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	s := sqlitestore.NewStore(db, path)
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS, UID: "scan-uid"},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL: "https://github.com/example/repo", ValidationMode: "off",
			MaxFindingsPerRun: new(int32(14)), Suspend: new(true),
		},
		Status: corev1alpha1.RepositoryScanStatus{Phase: repositoryScanPhaseScanning},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}, &corev1alpha1.Task{}).
		WithObjects(scan).Build()
	r := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: s, ArtifactStore: s, ResultStore: s}
	reconcile := func() {
		t.Helper()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(scan)})
		require.NoError(t, err)
	}
	completedRuns := map[string]*store.ScanRun{}
	for runIndex := range 2 {
		runID := fmt.Sprintf("scan_retained_%d", runIndex)
		started := time.Now().UTC().Add(time.Duration(runIndex-1) * time.Hour)
		tasks := seedRetainedScanPipeline(t, s, cl, scan, runID, started, 14)
		finishScanTask(t, cl, tasks[0])
		finishScanTask(t, cl, tasks[1])
		reconcile()
		// Keep the last review active while terminal Tasks are reconciled again.
		for _, task := range tasks[2 : len(tasks)-1] {
			finishScanTask(t, cl, task)
		}
		reconcile()
		before, err := s.GetScanRun(ctx, defaultNS, runID)
		require.NoError(t, err)
		require.Equal(t, scanRunPhaseRunning, before.Phase, "run: %#v", before)
		require.Equal(t, 13, before.ReviewedSliceCount)
		for repeat := range 3 {
			reconcile()
			after, err := s.GetScanRun(ctx, defaultNS, runID)
			require.NoError(t, err)
			require.Equal(t, before.ReviewedSliceCount, after.ReviewedSliceCount, "run %s changed on replay %d", runID, repeat)
			require.Equal(t, before.AcceptedFindings, after.AcceptedFindings)
			require.Equal(t, before.DroppedFindings, after.DroppedFindings)
			for oldID, oldRun := range completedRuns {
				got, err := s.GetScanRun(ctx, defaultNS, oldID)
				require.NoError(t, err)
				require.Equal(t, oldRun, got, "retained Tasks changed completed run %s", oldID)
			}
		}
		finishScanTask(t, cl, tasks[len(tasks)-1])
		reconcile()
		run, err := s.GetScanRun(ctx, defaultNS, runID)
		require.NoError(t, err)
		require.Equal(t, scanRunPhaseSucceeded, run.Phase)
		require.Equal(t, 14, run.SliceCount)
		require.Equal(t, 14, run.ReviewedSliceCount)
		require.Equal(t, 14, run.AcceptedFindings)
		require.Equal(t, 14, run.DroppedFindings)
		completedRuns[runID] = run
		dropped, _, err := s.ListDroppedFindings(ctx, store.DroppedFindingFilter{
			Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: runID, Limit: 100,
		})
		require.NoError(t, err)
		require.Len(t, dropped, 14)
		findings, _, err := s.ListFindings(ctx, store.FindingFilter{Namespace: defaultNS, RepositoryScan: scan.Name, Limit: 100})
		require.NoError(t, err)
		require.Len(t, findings, 14)
		for _, finding := range findings {
			require.Equal(t, runID, finding.ScanRunID)
		}
		reviewSlices, _, err := s.ListReviewSlices(ctx, store.ReviewSliceFilter{Namespace: defaultNS, RepositoryScan: scan.Name, Limit: 100})
		require.NoError(t, err)
		for _, slice := range reviewSlices {
			require.Equal(t, runID, slice.LastScanRunID)
			require.Equal(t, reviewSliceStatusReviewed, slice.Status)
		}
		model, err := s.GetLatestThreatModel(ctx, defaultNS, scan.Name)
		require.NoError(t, err)
		require.Equal(t, runID, model.GeneratedByScan)
		for _, task := range tasks {
			receipt, err := s.GetScanTaskIngestion(ctx, scanTaskIngestionIdentity(scan, task))
			require.NoError(t, err)
			require.True(t, receipt.Completed)
		}
		// Reopen SQLite and rebuild the reconciler, retaining the same Tasks.
		require.NoError(t, db.Close())
		db, err = sqlitestore.NewDB(path)
		require.NoError(t, err)
		s = sqlitestore.NewStore(db, path)
		r = &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: s, ArtifactStore: s, ResultStore: s}
		reconcile()
		got, err := s.GetScanRun(ctx, defaultNS, runID)
		require.NoError(t, err)
		require.Equal(t, run, got, "restart changed completed ingestion")
		gotModel, err := s.GetLatestThreatModel(ctx, defaultNS, scan.Name)
		require.NoError(t, err)
		require.Equal(t, model, gotModel)
		gotFindings, _, err := s.ListFindings(ctx, store.FindingFilter{Namespace: defaultNS, RepositoryScan: scan.Name, Limit: 100})
		require.NoError(t, err)
		require.Equal(t, findings, gotFindings)
		gotDropped, _, err := s.ListDroppedFindings(ctx, store.DroppedFindingFilter{Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: runID, Limit: 100})
		require.NoError(t, err)
		require.Equal(t, dropped, gotDropped)
		gotSlices, _, err := s.ListReviewSlices(ctx, store.ReviewSliceFilter{Namespace: defaultNS, RepositoryScan: scan.Name, Limit: 100})
		require.NoError(t, err)
		require.Equal(t, reviewSlices, gotSlices)
		var retained corev1alpha1.TaskList
		require.NoError(t, cl.List(ctx, &retained))
		require.Len(t, retained.Items, (runIndex+1)*16, "ingestion must not delete retained Tasks")
	}
	// Task history cleanup can remove old Tasks and their artifacts without
	// deleting ingestion receipts or changing the completed run history.
	var oldTasks corev1alpha1.TaskList
	require.NoError(t, cl.List(ctx, &oldTasks, client.MatchingLabels{labels.LabelSecurityScanID: "scan_retained_0"}))
	for i := range oldTasks.Items {
		task := &oldTasks.Items[i]
		require.NoError(t, cl.Delete(ctx, task))
		require.NoError(t, s.DeleteResult(ctx, task.Namespace, task.Name))
		require.NoError(t, s.DeleteArtifacts(ctx, task.Namespace, task.Name))
	}
	reconcile()
	for id, before := range completedRuns {
		after, err := s.GetScanRun(ctx, defaultNS, id)
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
}

func TestRepositoryScanIngestsTerminalPipelineOnRestart(t *testing.T) {
	ctx := context.Background()
	s := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS, UID: "scan-uid"},
		Spec:       corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo", ValidationMode: "off", Suspend: new(true)},
		Status:     corev1alpha1.RepositoryScanStatus{Phase: repositoryScanPhaseScanning},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}, &corev1alpha1.Task{}).WithObjects(scan).Build()
	tasks := seedRetainedScanPipeline(t, s, cl, scan, "scan_restart", time.Now().UTC(), 2)
	for _, task := range tasks {
		finishScanTask(t, cl, task)
	}
	r := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: s, ResultStore: s, ArtifactStore: s}
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(scan)})
	require.NoError(t, err)
	run, err := s.GetScanRun(ctx, defaultNS, "scan_restart")
	require.NoError(t, err)
	require.Equal(t, scanRunPhaseSucceeded, run.Phase)
	require.Equal(t, 2, run.ReviewedSliceCount)
	require.Equal(t, 2, run.AcceptedFindings)
	require.Equal(t, 2, run.DroppedFindings)
}

func seedRetainedScanPipeline(t *testing.T, s *sqlitestore.Store, cl client.Client, scan *corev1alpha1.RepositoryScan, runID string, started time.Time, sliceCount int) []*corev1alpha1.Task {
	t.Helper()
	ctx := context.Background()
	policyDigest := security.ScannerPolicyDigest(security.ScannerPolicy{})
	run := &store.ScanRun{ID: runID, Namespace: scan.Namespace, RepositoryScan: scan.Name,
		Mode: scanModeManual, Phase: scanRunPhaseRunning, PolicyDigest: policyDigest, StartedAt: started,
		HeadCommit: "head-" + runID}
	require.NoError(t, s.CreateScanRun(ctx, run))
	newTask := func(stage, sliceID string, index int) *corev1alpha1.Task {
		name := security.ScanStageTaskName(scan.Name, runID, stage, sliceID)
		return &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: scan.Namespace, UID: types.UID(name + "-uid"),
				CreationTimestamp: metav1.NewTime(started.Add(time.Duration(index) * time.Second)),
				Labels: map[string]string{
					labels.LabelSecurityTarget: scan.Name, labels.LabelSecurityScanID: runID,
					labels.LabelSecurityStage: stage, labels.LabelSecuritySliceID: sliceID, labels.LabelSecurityMode: scanModeManual,
				},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan", Name: scan.Name, UID: scan.UID, Controller: new(true)}},
			},
			Spec:   corev1alpha1.TaskSpec{Workspace: repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead)},
			Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning, ResultRef: &corev1alpha1.ResultReference{Available: true}},
		}
	}
	threatTask := newTask(security.StageThreatModel, "", 0)
	mapperTask := newTask(security.StageMapper, "", 1)
	threatResult, err := json.Marshal(security.ThreatModelResultEnvelope{
		SchemaVersion: security.AgentResultSchemaVersion, Kind: security.AgentResultKindThreatModel,
		RepositoryScan: scan.Name, ScanID: runID, PolicyDigest: policyDigest, ThreatModel: "# Threat model for " + runID,
	})
	require.NoError(t, err)
	require.NoError(t, s.SaveResult(ctx, scan.Namespace, threatTask.Name, threatResult))
	tasks := make([]*corev1alpha1.Task, 0, 2+sliceCount)
	tasks = append(tasks, threatTask, mapperTask)
	mapperArtifact := security.ReviewSlicesArtifact{SchemaVersion: security.SchemaVersionReviewSlices, HeadCommit: run.HeadCommit}
	for i := range sliceCount {
		sliceID := fmt.Sprintf("slice_%02d", i)
		path := fmt.Sprintf("src/file_%02d.go", i)
		slice := store.ReviewSlice{ID: sliceID, RepositoryScan: scan.Name, Source: "deterministic", Title: sliceID,
			Kind: "package", Status: reviewSliceStatusPending, OwnedFiles: []store.ReviewSliceFile{{Path: path}}}
		bindReviewSliceContext(t, &slice)
		mapperArtifact.Slices = append(mapperArtifact.Slices, slice)
		task := newTask(security.StageReview, sliceID, i+2)
		findings := security.FindingsV2Artifact{
			SchemaVersion: security.SchemaVersionFindingsV2,
			Repository:    trustedFindingsRepository(scan, run),
			Scan:          security.FindingsV2Scan{Mode: run.Mode, SliceID: sliceID},
			Findings: []security.FindingsV2Finding{
				{Title: "Authorization missing in " + path, Category: "authz", Severity: "high", Confidence: "high",
					Summary: "The handler permits access to another user's data.", Remediation: "Check ownership.",
					Evidence: []security.FindingsV2EvidenceRef{{Path: path, StartLine: 5, EndLine: 8}}},
				{Title: "Unsubstantiated issue", Category: "authz", Severity: "high", Confidence: "low",
					Summary: "Evidence is outside the review context.", Remediation: "Check ownership.",
					Evidence: []security.FindingsV2EvidenceRef{{Path: "omitted.go", StartLine: 1, EndLine: 1}}},
			},
		}
		saveFindingsTaskResult(t, s, task, scan.Name, runID, policyDigest, slice.ReviewContextHash, sliceID, findings)
		tasks = append(tasks, task)
	}
	saveMapperArtifactWithContexts(t, s, mapperTask, mapperArtifact)
	for _, task := range tasks {
		require.NoError(t, cl.Create(ctx, task))
	}
	return tasks
}

func finishScanTask(t *testing.T, cl client.Client, task *corev1alpha1.Task) {
	t.Helper()
	task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	completed := metav1.NewTime(task.CreationTimestamp.Add(time.Minute))
	task.Status.CompletionTime = &completed
	require.NoError(t, cl.Status().Update(context.Background(), task))
}

type scanIngestionFaultStore struct{ store.SecurityStore }

func (s *scanIngestionFaultStore) ApplyScanTaskIngestion(ctx context.Context, receipt *store.ScanTaskIngestion, apply func(store.SecurityStore, *store.ScanRun) error) (bool, error) {
	return s.SecurityStore.ApplyScanTaskIngestion(ctx, receipt, func(tx store.SecurityStore, run *store.ScanRun) error {
		return apply(&scanIngestionFaultStore{SecurityStore: tx}, run)
	})
}

func (s *scanIngestionFaultStore) UpdateReviewSliceStatus(context.Context, string, string, string, string, string) error {
	return store.ErrNotFound
}

func TestReviewIngestionRequiresSliceUpdate(t *testing.T) {
	for _, fault := range []string{"missing row", "unassigned row", "failed update"} {
		t.Run(fault, func(t *testing.T) {
			f := newReviewIngestionFixture(t)
			switch fault {
			case "missing row":
				f.sourceTask.Labels[labels.LabelSecuritySliceID] = "missing"
			case "unassigned row":
				f.slice.LastScanRunID = ""
				require.NoError(t, f.store.UpsertReviewSlice(f.ctx, f.slice))
			case "failed update":
				f.reconciler.SecurityStore = &scanIngestionFaultStore{SecurityStore: f.store}
			}
			err := f.reconciler.ingestScanTask(f.ctx, f.scan, f.sourceTask)
			require.Error(t, err)
			run, err := f.store.GetScanRun(f.ctx, defaultNS, f.run.ID)
			require.NoError(t, err)
			require.Zero(t, run.ReviewedSliceCount)
			require.Zero(t, run.AcceptedFindings)
			require.Zero(t, run.DroppedFindings)
			findings, _, err := f.store.ListFindings(f.ctx, store.FindingFilter{Namespace: defaultNS, RepositoryScan: f.scan.Name})
			require.NoError(t, err)
			require.Empty(t, findings)
			dropped, _, err := f.store.ListDroppedFindings(f.ctx, store.DroppedFindingFilter{Namespace: defaultNS, ScanRunID: f.run.ID})
			require.NoError(t, err)
			require.Empty(t, dropped)
			_, err = f.store.GetScanTaskIngestion(f.ctx, scanTaskIngestionIdentity(f.scan, f.sourceTask))
			require.ErrorIs(t, err, store.ErrNotFound)
			// Repair the missing ownership/update and retry the same Task.
			f.sourceTask.Labels[labels.LabelSecuritySliceID] = f.slice.ID
			f.slice.LastScanRunID = f.run.ID
			require.NoError(t, f.store.UpsertReviewSlice(f.ctx, f.slice))
			f.reconciler.SecurityStore = f.store
			require.NoError(t, f.reconciler.ingestScanTask(f.ctx, f.scan, f.sourceTask))
			run, err = f.store.GetScanRun(f.ctx, defaultNS, f.run.ID)
			require.NoError(t, err)
			require.Equal(t, 1, run.ReviewedSliceCount)
		})
	}
}

var errScanIngestionFollowup = errors.New("injected scan follow-up failure")

type failingScanArtifactStore struct{ store.ArtifactStore }

func (s *failingScanArtifactStore) SaveArtifact(context.Context, string, string, string, string, []byte) error {
	return errScanIngestionFollowup
}

type lostValidationCreateResponse struct {
	client.Client
	legacyName bool
}

func (c *lostValidationCreateResponse) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if c.legacyName {
		task := obj.(*corev1alpha1.Task)
		task.Name = security.ScanStageTaskName(task.Labels[labels.LabelSecurityTarget], "validation", security.StageValidation,
			task.Labels[labels.LabelSecurityFindingID]+"-"+task.Labels[labels.LabelSecurityScanID])
	}
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	return errScanIngestionFollowup
}

type missingValidationTaskList struct{ client.Client }

func (c *missingValidationTaskList) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if tasks, ok := list.(*corev1alpha1.TaskList); ok {
		tasks.Items = slices.DeleteFunc(tasks.Items, func(task corev1alpha1.Task) bool {
			return taskSecurityStage(&task) == security.StageValidation
		})
	}
	return nil
}

func defaultRecoveredScanTask(task *corev1alpha1.Task) {
	task.Spec.ConcurrencyPolicy = corev1alpha1.ForbidConcurrent
	task.Spec.StartingDeadlineSeconds = new(int64(100))
	task.Spec.SuccessfulRunsHistoryLimit = new(int32(3))
	task.Spec.FailedRunsHistoryLimit = new(int32(1))
	task.Spec.RequestedBy = &corev1alpha1.RequestedBy{Subject: "controller"}
	task.Spec.Transaction = &corev1alpha1.TaskTransaction{ID: "test-transaction-metadata"}
}

func TestReviewIngestionResumesFollowupWithoutRecounting(t *testing.T) {
	for _, tt := range []struct {
		name           string
		validationMode string
		phase          corev1alpha1.TaskPhase
		delayedCache   bool
		legacyName     bool
	}{
		{name: "artifact"},
		{name: "validation unobserved", validationMode: "full"},
		{name: "validation pending", validationMode: "full", phase: corev1alpha1.TaskPhasePending},
		{name: "validation running at cap", validationMode: "light", phase: corev1alpha1.TaskPhaseRunning},
		{name: "validation succeeded at cap", validationMode: "light", phase: corev1alpha1.TaskPhaseSucceeded},
		{name: "validation defaulted after delayed cache", validationMode: "full", phase: corev1alpha1.TaskPhaseSucceeded, delayedCache: true},
		{name: "timestamp-named validation", validationMode: "full", phase: corev1alpha1.TaskPhasePending, legacyName: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newReviewIngestionFixture(t)
			if tt.validationMode == "" {
				f.reconciler.ArtifactStore = &failingScanArtifactStore{ArtifactStore: f.store}
			} else {
				f.scan.Spec.ValidationMode = tt.validationMode
				f.scan.Spec.ValidationMaxFindingsPerRun = new(int32(1))
				f.reconciler.Client = &lostValidationCreateResponse{Client: f.client, legacyName: tt.legacyName}
			}
			require.ErrorIs(t, f.reconciler.ingestScanTask(f.ctx, f.scan, f.sourceTask), errScanIngestionFollowup)
			identity := scanTaskIngestionIdentity(f.scan, f.sourceTask)
			receipt, err := f.store.GetScanTaskIngestion(f.ctx, identity)
			require.NoError(t, err)
			require.False(t, receipt.Completed)
			require.Len(t, receipt.FindingIDs, 1)
			finding, err := f.store.GetFinding(f.ctx, defaultNS, receipt.FindingIDs[0])
			require.NoError(t, err)
			require.Equal(t, "unvalidated", finding.ValidationStatus)
			if tt.validationMode != "" {
				var tasks corev1alpha1.TaskList
				require.NoError(t, f.client.List(f.ctx, &tasks, client.MatchingLabels{labels.LabelSecurityStage: security.StageValidation}))
				require.Len(t, tasks.Items, 1)
				task := &tasks.Items[0]
				defaultRecoveredScanTask(task)
				task.Status.Phase = tt.phase
				require.NoError(t, f.client.Update(f.ctx, task))
			}
			before, err := f.store.GetScanRun(f.ctx, defaultNS, f.run.ID)
			require.NoError(t, err)
			require.Equal(t, 1, before.ReviewedSliceCount)
			require.Equal(t, 1, before.AcceptedFindings)
			require.Equal(t, 1, before.DroppedFindings)
			// Recovery must use the receipt even when original Task results have
			// already been cleaned up, or the source slice now has another owner.
			require.NoError(t, f.store.DeleteResult(f.ctx, defaultNS, f.sourceTask.Name))
			f.slice.LastScanRunID = "later-run"
			require.NoError(t, f.store.UpsertReviewSlice(f.ctx, f.slice))
			restarted := &RepositoryScanReconciler{Client: f.client, Scheme: f.reconciler.Scheme, SecurityStore: f.store, ArtifactStore: f.store, ResultStore: f.store}
			if tt.delayedCache {
				// Lists can lag a successful creation. Retrying after the old
				// timestamp name would change must still hit AlreadyExists.
				restarted.Client = &missingValidationTaskList{Client: f.client}
				time.Sleep(time.Second)
			}
			for range 3 {
				require.NoError(t, restarted.ingestScanTask(f.ctx, f.scan, f.sourceTask))
			}
			after, err := f.store.GetScanRun(f.ctx, defaultNS, f.run.ID)
			require.NoError(t, err)
			require.Equal(t, before, after)
			receipt, err = f.store.GetScanTaskIngestion(f.ctx, identity)
			require.NoError(t, err)
			require.True(t, receipt.Completed)
			dropped, _, err := f.store.ListDroppedFindings(f.ctx, store.DroppedFindingFilter{Namespace: defaultNS, ScanRunID: f.run.ID})
			require.NoError(t, err)
			require.Len(t, dropped, 1)
			data, _, err := f.store.GetArtifact(f.ctx, defaultNS, f.sourceTask.Name, security.ArtifactDroppedFindings)
			require.NoError(t, err)
			require.JSONEq(t, receipt.DroppedFindingsJSON, string(data))
			var tasks corev1alpha1.TaskList
			require.NoError(t, f.client.List(f.ctx, &tasks, client.MatchingLabels{labels.LabelSecurityStage: security.StageValidation}))
			if tt.validationMode != "" {
				require.Len(t, tasks.Items, 1)
				finding, err = f.store.GetFinding(f.ctx, defaultNS, receipt.FindingIDs[0])
				require.NoError(t, err)
				require.Equal(t, findingValidationStatusPending, finding.ValidationStatus)
				if tt.phase == corev1alpha1.TaskPhaseSucceeded {
					task := &tasks.Items[0]
					saveValidationTaskResult(t, f.store, task, f.scan.Name, f.run.ID, f.run.PolicyDigest, security.ValidationArtifact{
						Version: 1, FindingID: finding.ID, Status: findingValidationStatusValidated,
						Summary: "Confirmed missing ownership check", ValidationSteps: []string{"Trace the request to the handler"},
						AttackPathAnalysis: "An authenticated caller supplies another user's record ID to the handler.",
					})
					require.NoError(t, restarted.ingestValidationTask(f.ctx, f.scan, task))
					require.NoError(t, restarted.ingestScanTask(f.ctx, f.scan, f.sourceTask))
					finding, err = f.store.GetFinding(f.ctx, defaultNS, finding.ID)
					require.NoError(t, err)
					require.Equal(t, findingValidationStatusValidated, finding.ValidationStatus)
				}
			} else {
				require.Empty(t, tasks.Items)
			}
		})
	}
}

func TestReviewIngestionRejectsConflictingValidationTask(t *testing.T) {
	for name, mutate := range map[string]func(*corev1alpha1.Task){
		"owner": func(task *corev1alpha1.Task) { task.OwnerReferences[0].UID = "another-scan" },
		"run": func(task *corev1alpha1.Task) {
			task.Labels[labels.LabelSecurityScanID] = "another-run"
		},
		"prompt":    func(task *corev1alpha1.Task) { task.Spec.Prompt = "different instructions" },
		"workspace": func(task *corev1alpha1.Task) { task.Spec.Workspace.GitRepo = "https://github.com/example/other" },
		"agent":     func(task *corev1alpha1.Task) { task.Spec.AgentRef.Name = "other-agent" },
		"schedule":  func(task *corev1alpha1.Task) { task.Spec.Schedule = "0 * * * *" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newReviewIngestionFixture(t)
			f.scan.Spec.ValidationMode = "full"
			f.reconciler.Client = &lostValidationCreateResponse{Client: f.client}
			require.ErrorIs(t, f.reconciler.ingestScanTask(f.ctx, f.scan, f.sourceTask), errScanIngestionFollowup)
			var tasks corev1alpha1.TaskList
			require.NoError(t, f.client.List(f.ctx, &tasks, client.MatchingLabels{labels.LabelSecurityStage: security.StageValidation}))
			require.Len(t, tasks.Items, 1)
			task := &tasks.Items[0]
			defaultRecoveredScanTask(task)
			mutate(task)
			require.NoError(t, f.client.Update(f.ctx, task))
			before, err := f.store.GetScanRun(f.ctx, defaultNS, f.run.ID)
			require.NoError(t, err)
			f.reconciler.Client = f.client
			require.ErrorIs(t, f.reconciler.ingestScanTask(f.ctx, f.scan, f.sourceTask), store.ErrConflict)
			receipt, err := f.store.GetScanTaskIngestion(f.ctx, scanTaskIngestionIdentity(f.scan, f.sourceTask))
			require.NoError(t, err)
			require.False(t, receipt.Completed)
			finding, err := f.store.GetFinding(f.ctx, defaultNS, receipt.FindingIDs[0])
			require.NoError(t, err)
			require.Equal(t, "unvalidated", finding.ValidationStatus)
			after, err := f.store.GetScanRun(f.ctx, defaultNS, f.run.ID)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.NoError(t, f.client.List(f.ctx, &tasks, client.MatchingLabels{labels.LabelSecurityStage: security.StageValidation}))
			require.Len(t, tasks.Items, 1)
		})
	}
}

func newReviewIngestionFixture(t *testing.T) *reviewResultRetryFixture {
	t.Helper()
	f := newReviewResultRetryFixture(t)
	f.scan.Spec.ValidationMode = "off"
	f.reconciler.ArtifactStore = f.store
	findings := security.FindingsV2Artifact{
		SchemaVersion: security.SchemaVersionFindingsV2, Repository: trustedFindingsRepository(f.scan, f.run),
		Scan: security.FindingsV2Scan{Mode: f.run.Mode, SliceID: f.slice.ID},
		Findings: []security.FindingsV2Finding{
			{Title: "Missing authorization", Category: "authz", Severity: "high", Confidence: "high",
				Summary: "The handler permits access to another user's data.", Remediation: "Check ownership.",
				Evidence: []security.FindingsV2EvidenceRef{{Path: "internal/api/security.go", StartLine: 5, EndLine: 8}}},
			{Title: "Unsubstantiated issue", Category: "authz", Severity: "high", Confidence: "low",
				Summary: "Evidence is outside the review context.", Remediation: "Check ownership.",
				Evidence: []security.FindingsV2EvidenceRef{{Path: "omitted.go", StartLine: 1, EndLine: 1}}},
		},
	}
	saveFindingsTaskResult(t, f.store, f.sourceTask, f.scan.Name, f.run.ID, f.run.PolicyDigest, f.slice.ReviewContextHash, f.slice.ID, findings)
	return f
}
