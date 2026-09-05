package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
)

// Existing controller fixtures predate owner checks. Give their declared scan
// Tasks the same controller reference that production task creation installs.
func repositoryScanTestObjects(scan *corev1alpha1.RepositoryScan, objects ...client.Object) []client.Object {
	for _, object := range objects {
		if task, ok := object.(*corev1alpha1.Task); ok && metav1.GetControllerOf(task) == nil &&
			task.Labels[labels.LabelSecurityTarget] == labels.SelectorValue(scan.Name) {
			task.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(scan, corev1alpha1.GroupVersion.WithKind("RepositoryScan"))}
		}
	}
	return append([]client.Object{scan}, objects...)
}

func TestRepositoryScanReconcileRetiresStaleIdentityBeforeIngestion(t *testing.T) {
	for _, tt := range []struct {
		name       string
		uid        string
		generation int64
	}{
		{name: "edited", uid: "current-uid", generation: 1},
		{name: "recreated", uid: "previous-uid", generation: 2},
		{name: "unbound legacy run"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db := setupControllerSQLiteStore(t)
			scheme := runtime.NewScheme()
			require.NoError(t, corev1alpha1.AddToScheme(scheme))
			scan := &corev1alpha1.RepositoryScan{
				ObjectMeta: metav1.ObjectMeta{Name: "identity-scan", Namespace: defaultNS, UID: "current-uid", Generation: 2},
				Spec: corev1alpha1.RepositoryScanSpec{
					RepoURL: "https://github.com/example/repo", AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
				},
				Status: corev1alpha1.RepositoryScanStatus{Phase: repositoryScanPhaseScanning, LastScanID: "scan_old"},
			}
			run := &store.ScanRun{
				ID: "scan_old", Namespace: scan.Namespace, RepositoryScan: scan.Name,
				RepositoryScanUID: tt.uid, RepositoryScanGeneration: tt.generation,
				TaskName: "old-threat-model", Mode: "initial", Phase: scanRunPhaseRunning,
				PolicyDigest: security.ScannerPolicyDigest(security.ScannerPolicy{}),
			}
			require.NoError(t, db.CreateScanRun(ctx, run))
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name: run.TaskName, Namespace: scan.Namespace,
					Labels: map[string]string{
						labels.LabelSecurityTarget: scan.Name, labels.LabelSecurityScanID: run.ID,
						labels.LabelSecurityStage: security.StageThreatModel,
					},
				},
				Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded, ResultRef: &corev1alpha1.ResultReference{Available: true}},
			}
			result, err := json.Marshal(security.ThreatModelResultEnvelope{
				SchemaVersion: security.AgentResultSchemaVersion, Kind: security.AgentResultKindThreatModel,
				RepositoryScan: scan.Name, ScanID: run.ID, PolicyDigest: run.PolicyDigest, ThreatModel: "# Stale threat model",
			})
			require.NoError(t, err)
			require.NoError(t, db.SaveResult(ctx, scan.Namespace, task.Name, result))
			objects := repositoryScanTestObjects(scan, task)
			task.OwnerReferences[0].UID = types.UID(tt.uid)
			cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(scan).WithObjects(objects...).Build()
			r := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: db, ResultStore: db}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(scan)}
			_, err = r.Reconcile(ctx, request)
			require.NoError(t, err)
			retired, err := db.GetScanRun(ctx, scan.Namespace, run.ID)
			require.NoError(t, err)
			require.Equal(t, scanRunPhaseFailed, retired.Phase)
			require.Equal(t, tt.uid, retired.RepositoryScanUID)
			require.Equal(t, tt.generation, retired.RepositoryScanGeneration)
			require.NotNil(t, retired.CompletedAt)
			current := &corev1alpha1.RepositoryScan{}
			require.NoError(t, cl.Get(ctx, request.NamespacedName, current))
			require.Empty(t, current.Status.LastScanID)
			require.Equal(t, repositoryScanPhasePending, current.Status.Phase)

			// An unscheduled scan starts a replacement instead of remaining stuck
			// in Scanning, and late output from the old run stays unconsumed.
			_, err = r.Reconcile(ctx, request)
			require.NoError(t, err)
			_, err = r.Reconcile(ctx, request)
			require.NoError(t, err)
			require.NoError(t, cl.Get(ctx, request.NamespacedName, current))
			require.NotEqual(t, run.ID, current.Status.LastScanID)
			replacement, err := db.GetScanRun(ctx, scan.Namespace, current.Status.LastScanID)
			require.NoError(t, err)
			require.True(t, security.ScanRunMatchesRepositoryScan(replacement, current))
			_, err = db.GetLatestThreatModel(ctx, scan.Namespace, scan.Name)
			require.ErrorIs(t, err, store.ErrNotFound)
		})
	}
}

func TestRepositoryScanDeletionReleasesRunReservation(t *testing.T) {
	ctx := context.Background()
	db := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{
		Name: "deleted-scan", Namespace: defaultNS, UID: "deleted-uid", Generation: 1,
		Finalizers: []string{security.RepositoryScanRunFinalizer},
	}}
	run := &store.ScanRun{
		ID: "scan_deleted", Namespace: scan.Namespace, RepositoryScan: scan.Name,
		RepositoryScanUID: string(scan.UID), RepositoryScanGeneration: scan.Generation, Phase: scanRunPhasePending,
	}
	require.NoError(t, db.CreateScanRun(ctx, run))
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(scan).WithObjects(scan).Build()
	require.NoError(t, cl.Delete(ctx, scan))
	r := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: db}
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(scan)})
	require.NoError(t, err)
	err = cl.Get(ctx, client.ObjectKeyFromObject(scan), &corev1alpha1.RepositoryScan{})
	require.True(t, apierrors.IsNotFound(err))
	retired, err := db.GetScanRun(ctx, scan.Namespace, run.ID)
	require.NoError(t, err)
	require.Equal(t, scanRunPhaseFailed, retired.Phase)
	require.NotNil(t, retired.CompletedAt)
}

func TestRepositoryScanStatusRejectsChangedIdentity(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	current := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{
		Name: "edited-scan", Namespace: defaultNS, UID: "current-uid", Generation: 2,
	}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(current).WithObjects(current).Build()
	r := &RepositoryScanReconciler{Client: cl}
	stale := current.DeepCopy()
	stale.Generation = 1
	err := r.updateStatusWithRetry(ctx, stale, func(scan *corev1alpha1.RepositoryScan) {
		scan.Status.LastScanID = "stale-run"
	})
	require.ErrorIs(t, err, store.ErrConflict)
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(current), current))
	require.Empty(t, current.Status.LastScanID)
}

func TestRepositoryScanReconcileDoesNotRetireNewerRunFromStaleCache(t *testing.T) {
	ctx := context.Background()
	db := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	current := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{
			Name: "edited-scan", Namespace: defaultNS, UID: "scan-uid", Generation: 2,
			Finalizers: []string{security.RepositoryScanRunFinalizer},
		},
		Status: corev1alpha1.RepositoryScanStatus{Phase: repositoryScanPhaseScanning, LastScanID: "scan_new"},
	}
	run := &store.ScanRun{
		ID: "scan_new", Namespace: current.Namespace, RepositoryScan: current.Name,
		RepositoryScanUID: string(current.UID), RepositoryScanGeneration: current.Generation,
		Phase: scanRunPhaseRunning,
	}
	require.NoError(t, db.CreateScanRun(ctx, run))
	stale := current.DeepCopy()
	stale.Generation = 1
	stale.Status.LastScanID = "scan_old"
	cached := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(stale).WithObjects(stale).Build()
	live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(current).Build()
	r := &RepositoryScanReconciler{Client: cached, APIReader: live, Scheme: scheme, SecurityStore: db}
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)})
	require.ErrorIs(t, err, store.ErrConflict)
	// A spec edit can also admit a newer run between the live identity read
	// and cleanup. Never retire a generation newer than the caller observed.
	_, err = security.RetireStaleScanRuns(ctx, db, stale)
	require.ErrorIs(t, err, store.ErrConflict)
	after, err := db.GetScanRun(ctx, run.Namespace, run.ID)
	require.NoError(t, err)
	require.Equal(t, scanRunPhaseRunning, after.Phase)
}

func TestCurrentRepositoryScanTasksIgnoresLateTasksFromOlderRun(t *testing.T) {
	ctx := context.Background()
	db := setupControllerSQLiteStore(t)
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "scan", Namespace: defaultNS, UID: "uid", Generation: 1}}
	old := &store.ScanRun{
		ID: "scan_old", Namespace: scan.Namespace, RepositoryScan: scan.Name,
		RepositoryScanUID: string(scan.UID), RepositoryScanGeneration: scan.Generation,
		Phase: scanRunPhaseFailed, StartedAt: time.Now().Add(-time.Hour),
	}
	require.NoError(t, db.CreateScanRun(ctx, old))
	newer := *old
	newer.ID, newer.Phase, newer.StartedAt = "scan_new", scanRunPhaseSucceeded, time.Now()
	require.NoError(t, db.CreateScanRun(ctx, &newer))
	late := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
		Name: "late", Namespace: scan.Namespace,
		Labels: map[string]string{labels.LabelSecurityTarget: scan.Name, labels.LabelSecurityScanID: old.ID, labels.LabelSecurityStage: security.StageMapper},
	}}
	repositoryScanTestObjects(scan, late)
	tasks, err := security.CurrentRepositoryScanTasks(ctx, db, scan, []corev1alpha1.Task{*late})
	require.NoError(t, err)
	require.Empty(t, tasks)
}

func TestRetireStaleScanRunsFindsReservationsBeyondFirstPage(t *testing.T) {
	ctx := context.Background()
	db := setupControllerSQLiteStore(t)
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "scan", Namespace: defaultNS, UID: "current-uid", Generation: 2}}
	old := &store.ScanRun{ID: "scan_old", Namespace: scan.Namespace, RepositoryScan: scan.Name, Phase: scanRunPhasePending}
	require.NoError(t, db.CreateScanRun(ctx, old))
	for i := range 101 {
		require.NoError(t, db.CreateScanRun(ctx, &store.ScanRun{
			ID: fmt.Sprintf("scan_history_%d", i), Namespace: scan.Namespace, RepositoryScan: scan.Name, Phase: scanRunPhaseSucceeded,
			RepositoryScanUID: string(scan.UID), RepositoryScanGeneration: scan.Generation,
		}))
	}
	_, err := security.RetireStaleScanRuns(ctx, db, scan)
	require.NoError(t, err)
	after, err := db.GetScanRun(ctx, scan.Namespace, old.ID)
	require.NoError(t, err)
	require.Equal(t, scanRunPhaseFailed, after.Phase)
}

func TestMapperStageTaskReplayValidatesRunAndSpec(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "scan", Namespace: defaultNS, UID: "uid", Generation: 1},
		Spec:       corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo"},
	}
	run := &store.ScanRun{
		ID: security.NewScanRunID(), Namespace: scan.Namespace, RepositoryScan: scan.Name,
		RepositoryScanUID: string(scan.UID), RepositoryScanGeneration: scan.Generation,
		Mode: "initial", PolicyDigest: security.ScannerPolicyDigest(security.ScannerPolicy{}),
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan).Build()
	r := &RepositoryScanReconciler{Client: cl, Scheme: scheme}
	require.NoError(t, r.createMapperTask(ctx, scan, run))
	require.NoError(t, r.createMapperTask(ctx, scan, run))
	var tasks corev1alpha1.TaskList
	require.NoError(t, cl.List(ctx, &tasks))
	require.Len(t, tasks.Items, 1)
	task := tasks.Items[0]
	task.Spec.Command = []string{"unrelated-command"}
	require.NoError(t, cl.Update(ctx, &task))
	require.ErrorIs(t, r.createMapperTask(ctx, scan, run), store.ErrConflict)
}
