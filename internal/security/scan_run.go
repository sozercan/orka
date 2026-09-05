package security

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
)

const RepositoryScanRunFinalizer = "orka.ai/security-scan-runs"

// NewScanRunID does not depend on second-resolution Task names. The full ID
// fits in a Kubernetes label and remains stable for every stage of the run.
func NewScanRunID() string {
	return "scan_" + strings.ToLower(rand.Text())
}

// ScanStageTaskNameForRun gives a stage the same name on every reconciliation
// while keeping separate runs distinct, including runs started in one second.
func ScanStageTaskNameForRun(repositoryScanName, mode, stage, scope, runID string) string {
	parts := []string{sanitizeName(repositoryScanName), sanitizeName(mode), sanitizeName(stage)}
	if scope != "" {
		parts = append(parts, sanitizeName(scope))
	}
	return boundedTaskName(append(parts, sanitizeName(runID))...)
}

// ScanRunMatchesRepositoryScan never adopts an unbound historical run into a
// live RepositoryScan, whose Kubernetes UID and generation are nonzero.
func ScanRunMatchesRepositoryScan(run *store.ScanRun, scan *corev1alpha1.RepositoryScan) bool {
	return run != nil && scan != nil && scan.DeletionTimestamp.IsZero() &&
		run.Namespace == scan.Namespace && run.RepositoryScan == scan.Name &&
		run.RepositoryScanUID == string(scan.UID) && run.RepositoryScanGeneration == scan.Generation
}

// EnsureRepositoryScanRunFinalizer installs deletion cleanup before reserving a
// run. The live reader prevents stale cache entries from retiring newer runs;
// the optimistic patch cannot attach the finalizer to a recreated or edited object.
func EnsureRepositoryScanRunFinalizer(ctx context.Context, c client.Client, reader client.Reader, scan *corev1alpha1.RepositoryScan) error {
	if reader == nil {
		reader = c
	}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &corev1alpha1.RepositoryScan{}
		if err := reader.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
			return err
		}
		if current.UID != scan.UID || current.Generation != scan.Generation || !current.DeletionTimestamp.IsZero() {
			return fmt.Errorf("%w: repository scan changed before run admission", store.ErrConflict)
		}
		if controllerutil.ContainsFinalizer(current, RepositoryScanRunFinalizer) {
			return nil
		}
		before := current.DeepCopy()
		controllerutil.AddFinalizer(current, RepositoryScanRunFinalizer)
		return c.Patch(ctx, current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
}

// RetireStaleScanRuns releases reservations from an earlier object or spec.
// It also reports a stale status binding even if that run is already terminal.
// Legacy rows remain unbound; no identity is inferred from the current object.
func RetireStaleScanRuns(ctx context.Context, s store.SecurityStore, scan *corev1alpha1.RepositoryScan) (bool, error) {
	staleStatus := false
	cursor := ""
	for {
		runs, next, err := s.ListScanRuns(ctx, scan.Namespace, scan.Name, 100, cursor)
		if err != nil {
			return false, err
		}
		for i := range runs {
			run := &runs[i]
			if ScanRunMatchesRepositoryScan(run, scan) {
				continue
			}
			if scan.DeletionTimestamp.IsZero() && run.RepositoryScanUID == string(scan.UID) && run.RepositoryScanGeneration > scan.Generation {
				return false, fmt.Errorf("%w: a newer repository scan generation has already admitted a run", store.ErrConflict)
			}
			// A deleting owner can only release its own reservations. A newer
			// incarnation may already have appeared by the time cleanup retries.
			if !scan.DeletionTimestamp.IsZero() && run.RepositoryScanUID != "" && run.RepositoryScanUID != string(scan.UID) {
				continue
			}
			staleStatus = staleStatus || run.ID == scan.Status.LastScanID
			if run.Phase != "pending" && run.Phase != "running" {
				continue
			}
			now := time.Now().UTC()
			run.Phase = "failed"
			run.CompletedAt = &now
			run.ErrorMessage = "repository scan identity changed or was deleted; start a new scan"
			run.Summary = run.ErrorMessage
			if err := s.UpdateScanRun(ctx, run); err != nil {
				return false, err
			}
		}
		if next == "" {
			return staleStatus, nil
		}
		cursor = next
	}
}

// CurrentRepositoryScanTasks excludes foreign owners and pipeline Tasks whose
// persisted run belongs to a previous RepositoryScan incarnation or generation.
func CurrentRepositoryScanTasks(ctx context.Context, s store.SecurityStore, scan *corev1alpha1.RepositoryScan, tasks []corev1alpha1.Task) ([]corev1alpha1.Task, error) {
	current := make([]corev1alpha1.Task, 0, len(tasks))
	if s == nil {
		return current, nil
	}
	runs, _, err := s.ListScanRuns(ctx, scan.Namespace, scan.Name, 1, "")
	if err != nil {
		return nil, err
	}
	var latest *store.ScanRun
	if len(runs) > 0 {
		latest = &runs[0]
	}
	for i := range tasks {
		task := &tasks[i]
		owner := metav1.GetControllerOf(task)
		if owner == nil || owner.UID != scan.UID || owner.Name != scan.Name ||
			owner.Kind != "RepositoryScan" || task.Namespace != scan.Namespace {
			continue
		}
		stage := task.Labels[labels.LabelSecurityStage]
		if stage == "" && task.Labels[labels.LabelSecurityFindingID] != "" {
			stage = task.Labels[labels.LabelSecurityMode]
		}
		switch stage {
		case StageThreatModel, StageMapper, StageReview:
			runID := task.Labels[labels.LabelSecurityScanID]
			if !ScanRunMatchesRepositoryScan(latest, scan) || latest.ID != runID {
				continue
			}
		case StageValidation, StagePatch:
			// These follow-up Tasks have their own finding/publication checks.
		default:
			continue
		}
		current = append(current, *task)
	}
	return current, nil
}
