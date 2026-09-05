/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	cron "github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
)

const (
	repositoryScanPhasePending   = "Pending"
	repositoryScanPhaseScanning  = "Scanning"
	repositoryScanPhaseReady     = "Ready"
	repositoryScanPhaseError     = "Error"
	repositoryScanPhaseSuspended = "Suspended"

	scanRunPhasePending   = "pending"
	scanRunPhaseRunning   = "running"
	scanRunPhaseSucceeded = "succeeded"
	scanRunPhaseFailed    = "failed"
	scanRunAdmissionGrace = time.Minute

	scanModeIncremental = "incremental"
	scanModeManual      = "manual"
	confidenceHigh      = "high"

	reviewSliceStatusPending   = "pending"
	reviewSliceStatusReviewed  = "reviewed"
	reviewSliceStatusFailed    = "failed"
	reviewSliceStatusSkipped   = "skipped"
	reviewSliceStatusCompleted = "completed"

	findingStateOpen                 = "open"
	findingStatePatchPending         = "patch_pending"
	findingStatePatchReady           = "patch_ready"
	findingStatePROpen               = "pr_open"
	patchProposalStatusPROpened      = "pr_opened"
	findingValidationStatusPending   = "pending"
	findingValidationStatusValidated = "validated"
	findingValidationStatusFailed    = "failed"
	validationModeOff                = "off"
	validationModeFull               = "full"
	validationThresholdLow           = "low"

	scanSummaryRunning            = "scan is running"
	scanSummaryThreatModelPending = "Threat model generated; deterministic mapper pending"

	// Kubernetes rejects condition messages longer than 32 KiB. Scan summaries can
	// exceed that, so keep the full summary in storage and only publish a capped
	// status message on the CRD.
	repositoryScanConditionMessageLimit  = 32 * 1024
	repositoryScanConditionMessageSuffix = "\n...[truncated]"

	securityReviewInitialAttempt = 0
	securityReviewRetryAttempt   = 1
	securityReviewRetryPrompt    = "\n\nThis is the only automatic result retry. " +
		"The prior review task returned an invalid terminal result. " +
		"Re-run the review from the trusted context above and return exactly one JSON object matching the required envelope. " +
		"Do not include prose, Markdown fences, or tool transcripts."
)

var errScannerPolicyDigestChanged = errors.New("scanner policy digest changed during scan run")

// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get

// RepositoryScanReconciler reconciles RepositoryScan resources.
type RepositoryScanReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	SecurityStore    store.SecurityStore
	ArtifactStore    store.ArtifactStore
	ResultStore      store.ResultStore
	PublicationStore store.PublicationStore
	// APIReader reads uncached Secrets and verifies RepositoryScan identity
	// before run admission or cleanup; nil falls back to Client.
	APIReader client.Reader
	// HTTPClient and GitHubAPIBaseURL serve the published-commit read that
	// backs harness-v2 patch evidence; zero values use the defaults.
	HTTPClient       *http.Client
	GitHubAPIBaseURL string
}

func repositoryScanConditionMessage(message, fallback string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return fallback
	}
	if len(message) <= repositoryScanConditionMessageLimit {
		return message
	}

	maxPrefixBytes := repositoryScanConditionMessageLimit - len(repositoryScanConditionMessageSuffix)
	if maxPrefixBytes <= 0 {
		return repositoryScanConditionMessageSuffix
	}

	message = message[:maxPrefixBytes]
	for len(message) > 0 && !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	message = strings.TrimRight(message, " \t\r\n")
	return message + repositoryScanConditionMessageSuffix
}

func titleCaseMode(mode string) string {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return "Scan"
	}
	return strings.ToUpper(mode[:1]) + mode[1:]
}

// +kubebuilder:rbac:groups=core.orka.ai,resources=repositoryscans,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.orka.ai,resources=repositoryscans/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.orka.ai,resources=repositoryscans/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.orka.ai,resources=tasks,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives repository scan lifecycle, task creation, and task ingestion.
func (r *RepositoryScanReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("repositoryscan")

	scan := &corev1alpha1.RepositoryScan{}
	if err := r.Get(ctx, req.NamespacedName, scan); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if done, err := r.reconcileScanRunIdentity(ctx, scan); err != nil || done {
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	if scan.Status.Phase == "" {
		if err := r.updateStatusWithRetry(ctx, scan, func(s *corev1alpha1.RepositoryScan) {
			s.Status.Phase = repositoryScanPhasePending
			meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "Pending",
				Message:            "Waiting for the first scan run",
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: s.Generation,
			})
		}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if err := r.ingestOwnedTasks(ctx, scan); err != nil {
		logger.Error(err, "failed to ingest security tasks")
		return ctrl.Result{}, err
	}

	if security.IsSuspended(scan) {
		if scan.Status.Phase != repositoryScanPhaseSuspended {
			if err := r.updateStatusWithRetry(ctx, scan, func(s *corev1alpha1.RepositoryScan) {
				s.Status.Phase = repositoryScanPhaseSuspended
				meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "Suspended",
					Message:            "Scheduled scans are suspended",
					LastTransitionTime: metav1.Now(),
					ObservedGeneration: s.Generation,
				})
			}); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	active, err := r.hasActiveScanPipelineTask(ctx, scan)
	if err != nil {
		return ctrl.Result{}, err
	}
	if active {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if scan.Status.LastScanID == "" {
		if err := r.createScanRun(ctx, scan, "initial", "", ""); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	progressed, err := r.progressLatestScanRun(ctx, scan)
	if err != nil {
		return ctrl.Result{}, err
	}
	if progressed {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if scan.Spec.Schedule == "" {
		return ctrl.Result{}, nil
	}

	sched, err := cron.ParseStandard(scan.Spec.Schedule)
	if err != nil {
		if updateErr := r.updateStatusWithRetry(ctx, scan, func(s *corev1alpha1.RepositoryScan) {
			s.Status.Phase = repositoryScanPhaseError
			meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "InvalidSchedule",
				Message:            repositoryScanConditionMessage(err.Error(), "invalid scan schedule"),
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: s.Generation,
			})
		}); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, nil
	}

	base := scan.CreationTimestamp.Time
	if scan.Status.LastSuccessfulScanAt != nil {
		base = scan.Status.LastSuccessfulScanAt.Time
	}
	nextRun := sched.Next(base)
	if time.Now().Before(nextRun) {
		return ctrl.Result{RequeueAfter: time.Until(nextRun)}, nil
	}

	if err := r.createScanRun(ctx, scan, scanModeIncremental, scan.Status.LastProcessedCommit, ""); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

func taskSecurityStage(task *corev1alpha1.Task) string {
	if task == nil {
		return ""
	}
	if stage := strings.TrimSpace(task.Labels[labels.LabelSecurityStage]); stage != "" {
		return stage
	}
	if task.Labels[labels.LabelSecurityFindingID] != "" {
		switch strings.TrimSpace(task.Labels[labels.LabelSecurityMode]) {
		case security.StagePatch:
			return security.StagePatch
		case security.StageValidation:
			return security.StageValidation
		}
	}
	return ""
}

func isScanPipelineStage(stage string) bool {
	switch stage {
	case security.StageThreatModel, security.StageMapper, security.StageReview:
		return true
	default:
		return false
	}
}

func isActiveTaskPhase(phase corev1alpha1.TaskPhase) bool {
	switch phase {
	case corev1alpha1.TaskPhasePending, corev1alpha1.TaskPhaseRunning, corev1alpha1.TaskPhaseFinalizing, corev1alpha1.TaskPhaseScheduled:
		return true
	default:
		return false
	}
}

func isTerminalScanTaskPhase(phase corev1alpha1.TaskPhase) bool {
	switch phase {
	case corev1alpha1.TaskPhaseSucceeded, corev1alpha1.TaskPhaseFailed, corev1alpha1.TaskPhaseCancelled:
		return true
	default:
		return false
	}
}

func isFailedScanTaskPhase(phase corev1alpha1.TaskPhase) bool {
	return phase == corev1alpha1.TaskPhaseFailed || phase == corev1alpha1.TaskPhaseCancelled
}

func scanTaskRunID(task *corev1alpha1.Task) string {
	if task == nil {
		return ""
	}
	if scanID := strings.TrimSpace(task.Labels[labels.LabelSecurityScanID]); scanID != "" {
		return scanID
	}
	return security.ScanRunID(task.Name)
}

func latestOwnedScanPipelineRunID(tasks []corev1alpha1.Task) string {
	var latest *corev1alpha1.Task
	for i := range tasks {
		task := &tasks[i]
		if !isScanPipelineStage(taskSecurityStage(task)) {
			continue
		}
		if latest == nil {
			latest = task
			continue
		}
		if task.CreationTimestamp.After(latest.CreationTimestamp.Time) {
			latest = task
			continue
		}
		if task.CreationTimestamp.Equal(&latest.CreationTimestamp) && task.Name > latest.Name {
			latest = task
		}
	}
	return scanTaskRunID(latest)
}

func (r *RepositoryScanReconciler) hasActiveScanPipelineTask(ctx context.Context, scan *corev1alpha1.RepositoryScan) (bool, error) {
	if r.Client == nil {
		return false, nil
	}
	tasks, err := r.listCurrentScanTasks(ctx, scan, "")
	if err != nil {
		return false, err
	}

	for _, task := range tasks.Items {
		if !isScanPipelineStage(taskSecurityStage(&task)) {
			continue
		}
		if task.Status.Phase == "" || isActiveTaskPhase(task.Status.Phase) {
			return true, nil
		}
	}
	return false, nil
}

//nolint:unparam // headCommit is usually mapper-resolved but kept for explicit scan ranges.
func (r *RepositoryScanReconciler) createScanRun(ctx context.Context, scan *corev1alpha1.RepositoryScan, mode, baseCommit, headCommit string) error {
	if err := security.EnsureRepositoryScanRunFinalizer(ctx, r.Client, r.APIReader, scan); err != nil {
		return err
	}
	if r.SecurityStore != nil {
		if _, err := security.RetireStaleScanRuns(ctx, r.SecurityStore, scan); err != nil {
			return err
		}
	}
	var threatModel string
	if r.SecurityStore != nil {
		model, err := r.SecurityStore.GetLatestThreatModel(ctx, scan.Namespace, scan.Name)
		if err == nil {
			threatModel = model.Content
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}

	policy, err := security.LoadScannerPolicy(ctx, r.Client, scan.Namespace, scan.Spec)
	if err != nil {
		if terminalScannerPolicyLoadError(err) {
			if statusErr := r.updateRepositoryScanPolicyError(ctx, scan, err); statusErr != nil {
				return errors.Join(err, statusErr)
			}
		}
		return err
	}
	scanID := security.NewScanRunID()
	taskName := security.ScanStageTaskNameForRun(scan.Name, mode, security.StageThreatModel, "", scanID)
	idempotencyKey := security.ScanRunIdempotencyKey(scan.Namespace, scan.Name, mode, baseCommit, headCommit, scan.Spec.SubPath, policy.Digest)
	if duplicate, err := r.hasActiveScanRun(ctx, scan); err != nil {
		return err
	} else if duplicate {
		return nil
	}
	timeout := metav1.Duration{Duration: 2 * time.Hour}
	priority := int32(700)
	resultBinding := security.AgentResultBinding{
		RepositoryScan: scan.Name,
		ScanID:         scanID,
		PolicyDigest:   policy.Digest,
	}

	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskName,
			Namespace: scan.Namespace,
			Labels: map[string]string{
				labels.LabelManaged:        "true",
				labels.LabelCreatedBy:      "repository-security",
				labels.LabelSecurityTarget: labels.SelectorValue(scan.Name),
				labels.LabelSecurityScanID: scanID,
				labels.LabelSecurityMode:   mode,
				labels.LabelSecurityStage:  security.StageThreatModel,
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:      corev1alpha1.TaskTypeAgent,
			AgentRef:  &scan.Spec.AnalysisAgentRef,
			Prompt:    security.BuildThreatModelResultPrompt(scan, mode, baseCommit, headCommit, threatModel, resultBinding, policy.PromptPolicy()),
			Timeout:   &timeout,
			Priority:  &priority,
			Workspace: repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead),
		},
	}
	if err := controllerutil.SetControllerReference(scan, task, r.Scheme); err != nil {
		return err
	}
	run := &store.ScanRun{
		ID:                       scanID,
		Namespace:                scan.Namespace,
		RepositoryScan:           scan.Name,
		RepositoryScanUID:        string(scan.UID),
		RepositoryScanGeneration: scan.Generation,
		TaskName:                 taskName,
		Mode:                     mode,
		Phase:                    scanRunPhasePending,
		BaseCommit:               baseCommit,
		HeadCommit:               headCommit,
		ScannerPolicyVersion:     security.ScannerPolicyVersion,
		PolicyDigest:             policy.Digest,
		IdempotencyKey:           idempotencyKey,
		StartedAt:                time.Now().UTC(),
	}
	if err := r.ensureScanRunRecord(ctx, run); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return nil
		}
		return err
	}
	if err := r.Create(ctx, task); err != nil {
		now := time.Now().UTC()
		run.Phase = scanRunPhaseFailed
		run.CompletedAt = &now
		run.ErrorMessage = "scan task creation failed"
		if releaseErr := r.SecurityStore.UpdateScanRun(ctx, run); releaseErr != nil {
			return errors.Join(err, releaseErr)
		}
		return err
	}

	return r.updateStatusWithRetry(ctx, scan, func(s *corev1alpha1.RepositoryScan) {
		s.Status.Phase = repositoryScanPhaseScanning
		s.Status.LastScanID = scanID
		s.Status.LastScanTaskName = taskName
		meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "Scanning",
			Message:            fmt.Sprintf("%s scan is running", titleCaseMode(mode)),
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: s.Generation,
		})
	})
}

func (r *RepositoryScanReconciler) hasActiveScanRun(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
) (bool, error) {
	if r.SecurityStore == nil {
		return false, nil
	}
	runs, _, err := r.SecurityStore.ListScanRuns(ctx, scan.Namespace, scan.Name, 100, "")
	if err != nil {
		return false, err
	}
	for i := range runs {
		run := &runs[i]
		if !activeScanRunPhase(run.Phase) {
			continue
		}
		hasActiveTask, err := r.scanRunHasActivePipelineTask(ctx, scan, run.ID)
		if err != nil {
			return false, err
		}
		if hasActiveTask {
			return true, nil
		}
		if run.Phase == scanRunPhasePending && time.Since(run.StartedAt) < scanRunAdmissionGrace {
			return true, nil
		}
		now := time.Now().UTC()
		run.Phase = scanRunPhaseFailed
		run.CompletedAt = &now
		run.ErrorMessage = "scan run has no active pipeline task for its idempotency key"
		if err := r.SecurityStore.UpdateScanRun(ctx, run); err != nil {
			return false, err
		}
	}
	return false, nil
}

func (r *RepositoryScanReconciler) scanRunHasActivePipelineTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, runID string) (bool, error) {
	if r.Client == nil || strings.TrimSpace(runID) == "" {
		return false, nil
	}
	tasks, err := r.listCurrentScanTasks(ctx, scan, runID)
	if err != nil {
		return false, err
	}
	for i := range tasks.Items {
		task := &tasks.Items[i]
		if !isScanPipelineStage(taskSecurityStage(task)) {
			continue
		}
		if task.Status.Phase == "" || isActiveTaskPhase(task.Status.Phase) {
			return true, nil
		}
	}
	return false, nil
}

func (r *RepositoryScanReconciler) ensureScanRunRecord(ctx context.Context, run *store.ScanRun) error {
	if r.SecurityStore == nil || run == nil {
		return nil
	}

	_, err := r.SecurityStore.GetScanRun(ctx, run.Namespace, run.ID)
	switch {
	case err == nil:
		return nil
	case !errors.Is(err, store.ErrNotFound):
		return err
	}

	if err := r.SecurityStore.CreateScanRun(ctx, run); err != nil {
		_, getErr := r.SecurityStore.GetScanRun(ctx, run.Namespace, run.ID)
		switch {
		case getErr == nil:
			return nil
		case errors.Is(getErr, store.ErrNotFound):
			return err
		default:
			return getErr
		}
	}

	return nil
}

func activeScanRunPhase(phase string) bool {
	return phase == scanRunPhasePending || phase == scanRunPhaseRunning
}

func ensureScanRunPolicyDigest(run *store.ScanRun, policy security.ScannerPolicy) error {
	if run == nil {
		return nil
	}
	if run.PolicyDigest == "" {
		run.PolicyDigest = policy.Digest
		return nil
	}
	if policy.Digest != "" && run.PolicyDigest != policy.Digest {
		return fmt.Errorf("%w: recorded %s current %s", errScannerPolicyDigestChanged, run.PolicyDigest, policy.Digest)
	}
	return nil
}

func (r *RepositoryScanReconciler) recordTerminalScanRunError(ctx context.Context, scan *corev1alpha1.RepositoryScan, run *store.ScanRun, failure error) error {
	if err := r.markScanRunTerminalError(ctx, scan, run, failure); err != nil {
		return errors.Join(failure, err)
	}
	return failure
}

func (r *RepositoryScanReconciler) updateRepositoryScanPolicyError(ctx context.Context, scan *corev1alpha1.RepositoryScan, failure error) error {
	if scan == nil || failure == nil {
		return nil
	}
	message := failure.Error()
	return r.updateStatusWithRetry(ctx, scan, func(s *corev1alpha1.RepositoryScan) {
		s.Status.Phase = repositoryScanPhaseError
		meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "ScanFailed",
			Message:            repositoryScanConditionMessage(message, "scanner policy could not be loaded"),
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: s.Generation,
		})
	})
}

func (r *RepositoryScanReconciler) markScanRunTerminalError(ctx context.Context, scan *corev1alpha1.RepositoryScan, run *store.ScanRun, failure error) error {
	if r.SecurityStore == nil || run == nil || failure == nil {
		return nil
	}
	now := time.Now().UTC()
	message := failure.Error()
	run.Phase = scanRunPhaseFailed
	run.CompletedAt = &now
	run.ErrorMessage = message
	run.Summary = message
	if err := r.SecurityStore.UpdateScanRun(ctx, run); err != nil {
		return err
	}
	if scan == nil {
		return nil
	}
	if err := r.refreshScanRunStatus(ctx, scan, run, run.ID, true); err != nil {
		return err
	}
	return nil
}

func (r *RepositoryScanReconciler) createMapperTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, run *store.ScanRun) error {
	if !security.ScanRunMatchesRepositoryScan(run, scan) {
		return store.ErrConflict
	}
	policy, err := security.LoadScannerPolicy(ctx, r.Client, scan.Namespace, scan.Spec)
	if err != nil {
		if run != nil && activeScanRunPhase(run.Phase) && terminalScannerPolicyLoadError(err) {
			return r.recordTerminalScanRunError(ctx, scan, run, err)
		}
		return err
	}
	if err := ensureScanRunPolicyDigest(run, policy); err != nil {
		if errors.Is(err, errScannerPolicyDigestChanged) {
			return r.recordTerminalScanRunError(ctx, scan, run, err)
		}
		return err
	}
	timeout := metav1.Duration{Duration: 30 * time.Minute}
	priority := int32(690)
	taskName := security.ScanStageTaskNameForRun(scan.Name, run.Mode, security.StageMapper, "", run.ID)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskName,
			Namespace: scan.Namespace,
			Labels: map[string]string{
				labels.LabelManaged:        "true",
				labels.LabelCreatedBy:      "repository-security",
				labels.LabelSecurityTarget: labels.SelectorValue(scan.Name),
				labels.LabelSecurityScanID: run.ID,
				labels.LabelSecurityMode:   run.Mode,
				labels.LabelSecurityStage:  security.StageMapper,
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeContainer,
			Command:  []string{"--security-mapper"},
			Timeout:  &timeout,
			Priority: &priority,
			Env: []corev1.EnvVar{
				{Name: security.EnvRepositoryScanName, Value: scan.Name},
				{Name: security.EnvStage, Value: security.StageMapper},
				{Name: security.EnvScanID, Value: run.ID},
				{Name: security.EnvScannerPolicyVersion, Value: security.ScannerPolicyVersion},
				{Name: security.EnvPolicyDigest, Value: policy.Digest},
				{Name: security.EnvPolicyProvenance, Value: security.PolicyProvenanceEnv(policy)},
				{Name: security.EnvScanBaseCommit, Value: run.BaseCommit},
				{Name: security.EnvScanHeadCommit, Value: run.HeadCommit},
			},
			Workspace: repositoryScanTaskWorkspace(scan, ""),
		},
	}
	if err := controllerutil.SetControllerReference(scan, task, r.Scheme); err != nil {
		return err
	}
	return r.createOrValidateScanStageTask(ctx, task)
}

type latestScanPipelineState struct {
	hasSucceededThreatModel bool
	hasMapperTasks          bool
	hasSucceededMapper      bool
	hasReviewTasks          bool
	hasActiveTask           bool
}

func latestScanPipelineStateForRun(tasks []corev1alpha1.Task, scanID string) latestScanPipelineState {
	state := latestScanPipelineState{}
	for i := range tasks {
		task := &tasks[i]
		if scanTaskRunID(task) != scanID {
			continue
		}
		switch taskSecurityStage(task) {
		case security.StageThreatModel:
			state.hasSucceededThreatModel = task.Status.Phase == corev1alpha1.TaskPhaseSucceeded || state.hasSucceededThreatModel
		case security.StageMapper:
			state.hasMapperTasks = true
			state.hasSucceededMapper = task.Status.Phase == corev1alpha1.TaskPhaseSucceeded || state.hasSucceededMapper
		case security.StageReview:
			state.hasReviewTasks = true
		}
		if isActiveTaskPhase(task.Status.Phase) {
			state.hasActiveTask = true
		}
	}
	return state
}

func (r *RepositoryScanReconciler) pendingReviewSlices(ctx context.Context, scan *corev1alpha1.RepositoryScan, runID string) ([]store.ReviewSlice, error) {
	const pageSize = 1000
	var all []store.ReviewSlice
	cursor := ""
	for {
		reviewSlices, nextCursor, err := r.SecurityStore.ListReviewSlices(ctx, store.ReviewSliceFilter{
			Namespace:      scan.Namespace,
			RepositoryScan: scan.Name,
			Status:         reviewSliceStatusPending,
			LastScanRunID:  runID,
			Limit:          pageSize,
			Cursor:         cursor,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, reviewSlices...)
		if nextCursor == "" {
			return all, nil
		}
		cursor = nextCursor
	}
}

func reviewSliceMatchesChangedFiles(slice store.ReviewSlice, changedFiles map[string]struct{}) bool {
	for _, file := range slice.OwnedFiles {
		if _, ok := changedFiles[normalizeRepoPath(file.Path)]; ok {
			return true
		}
	}
	if slice.Confidence != confidenceHigh {
		return false
	}
	for _, file := range slice.ContextFiles {
		if _, ok := changedFiles[normalizeRepoPath(file.Path)]; ok {
			return true
		}
	}
	for _, test := range slice.Tests {
		if _, ok := changedFiles[normalizeRepoPath(test.Path)]; ok {
			return true
		}
	}
	return false
}

func normalizeRepoPath(value string) string {
	return strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
}

func attachChangedMetadataToReviewSlice(slice *store.ReviewSlice, changedFiles []string, changedLineRanges []security.ChangedLineRange) {
	if slice == nil {
		return
	}
	slicePaths := reviewSlicePathSet(*slice)
	slice.ChangedFiles = reviewSliceChangedFiles(changedFiles, slicePaths)
	slice.ChangedLineRanges = reviewSliceChangedLineRanges(changedLineRanges, slicePaths)
}

func reviewSlicePathSet(slice store.ReviewSlice) map[string]struct{} {
	paths := map[string]struct{}{}
	for _, file := range slice.Entrypoints {
		if normalized := normalizeRepoPath(file.Path); security.SafeRepoPath(normalized) {
			paths[normalized] = struct{}{}
		}
	}
	for _, file := range slice.OwnedFiles {
		if normalized := normalizeRepoPath(file.Path); security.SafeRepoPath(normalized) {
			paths[normalized] = struct{}{}
		}
	}
	for _, file := range slice.ContextFiles {
		if normalized := normalizeRepoPath(file.Path); security.SafeRepoPath(normalized) {
			paths[normalized] = struct{}{}
		}
	}
	for _, test := range slice.Tests {
		if normalized := normalizeRepoPath(test.Path); security.SafeRepoPath(normalized) {
			paths[normalized] = struct{}{}
		}
	}
	return paths
}

func reviewSliceChangedFiles(changedFiles []string, slicePaths map[string]struct{}) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(changedFiles))
	for _, file := range changedFiles {
		file = normalizeRepoPath(file)
		if !security.SafeRepoPath(file) {
			continue
		}
		if _, ok := slicePaths[file]; !ok {
			continue
		}
		if _, ok := seen[file]; ok {
			continue
		}
		seen[file] = struct{}{}
		out = append(out, file)
	}
	sort.Strings(out)
	return out
}

func reviewSliceChangedLineRanges(changedLineRanges []security.ChangedLineRange, slicePaths map[string]struct{}) []security.ChangedLineRange {
	out := make([]security.ChangedLineRange, 0, len(changedLineRanges))
	for _, lineRange := range changedLineRanges {
		lineRange.Path = normalizeRepoPath(lineRange.Path)
		if !security.SafeRepoPath(lineRange.Path) || lineRange.StartLine <= 0 || lineRange.EndLine < lineRange.StartLine {
			continue
		}
		if _, ok := slicePaths[lineRange.Path]; !ok {
			continue
		}
		out = append(out, lineRange)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		if out[i].StartLine != out[j].StartLine {
			return out[i].StartLine < out[j].StartLine
		}
		return out[i].EndLine < out[j].EndLine
	})
	return out
}

func changedFileSet(files []string) map[string]struct{} {
	out := make(map[string]struct{}, len(files))
	for _, file := range files {
		file = normalizeRepoPath(file)
		if file == "" {
			continue
		}
		out[file] = struct{}{}
	}
	return out
}

func trustedFindingsRepository(scan *corev1alpha1.RepositoryScan, run *store.ScanRun) security.FindingsV2Repository {
	repo := security.FindingsV2Repository{
		RepoURL: strings.TrimSpace(scan.Spec.RepoURL),
		Branch:  trustedFindingsBranch(scan),
		SubPath: strings.Trim(strings.TrimSpace(scan.Spec.SubPath), "/"),
	}
	if run != nil {
		repo.BaseSHA = run.BaseCommit
		repo.HeadSHA = run.HeadCommit
	}
	return repo
}

func trustedFindingsBranch(scan *corev1alpha1.RepositoryScan) string {
	if branch := strings.TrimSpace(scan.Spec.Branch); branch != "" {
		return branch
	}
	if ref := security.EffectiveRef(scan); ref != "" {
		return "ref:" + ref
	}
	return security.EffectiveBranch(scan)
}

func (r *RepositoryScanReconciler) createReviewTasks(ctx context.Context, scan *corev1alpha1.RepositoryScan, run *store.ScanRun, threatModel string, reviewSlices []store.ReviewSlice) error {
	if !security.ScanRunMatchesRepositoryScan(run, scan) {
		return store.ErrConflict
	}
	policy, err := security.LoadScannerPolicy(ctx, r.Client, scan.Namespace, scan.Spec)
	if err != nil {
		if run != nil && activeScanRunPhase(run.Phase) && terminalScannerPolicyLoadError(err) {
			return r.recordTerminalScanRunError(ctx, scan, run, err)
		}
		return err
	}
	if err := ensureScanRunPolicyDigest(run, policy); err != nil {
		if errors.Is(err, errScannerPolicyDigestChanged) {
			return r.recordTerminalScanRunError(ctx, scan, run, err)
		}
		return err
	}
	for _, reviewSlice := range reviewSlices {
		task, err := r.buildReviewTask(scan, run, threatModel, reviewSlice, policy, securityReviewInitialAttempt)
		if err != nil {
			return err
		}
		if err := r.createOrValidateScanStageTask(ctx, task); err != nil {
			return err
		}
	}
	return nil
}

func (r *RepositoryScanReconciler) buildReviewTask(
	scan *corev1alpha1.RepositoryScan,
	run *store.ScanRun,
	threatModel string,
	reviewSlice store.ReviewSlice,
	policy security.ScannerPolicy,
	attempt int,
) (*corev1alpha1.Task, error) {
	if attempt != securityReviewInitialAttempt && attempt != securityReviewRetryAttempt {
		return nil, fmt.Errorf("unsupported security review attempt %d", attempt)
	}
	manifest, digest, err := security.ParseTrustedReviewContextManifest([]byte(reviewSlice.ReviewContextJSON))
	if err != nil {
		return nil, fmt.Errorf("review slice %s trusted context: %w", reviewSlice.ID, err)
	}
	if digest != reviewSlice.ReviewContextHash {
		return nil, fmt.Errorf("review slice %s trusted context digest changed", reviewSlice.ID)
	}

	taskName := security.ScanStageTaskNameForRun(scan.Name, run.Mode, security.StageReview, reviewSlice.ID, run.ID)
	if attempt > securityReviewInitialAttempt {
		taskName = security.ScanStageRetryTaskName(scan.Name, run.ID, security.StageReview, reviewSlice.ID, attempt)
	}
	resultBinding := security.AgentResultBinding{
		RepositoryScan: scan.Name,
		ScanID:         run.ID,
		PolicyDigest:   policy.Digest,
		ContextDigest:  digest,
	}
	prompt := security.BuildReviewResultPrompt(
		scan, run.Mode, run.BaseCommit, run.HeadCommit, threatModel, reviewSlice,
		resultBinding, *manifest, trustedFindingsRepository(scan, run), policy.PromptPolicy(),
	)
	if attempt == securityReviewRetryAttempt {
		prompt += securityReviewRetryPrompt
	}
	timeout := metav1.Duration{Duration: 2 * time.Hour}
	priority := int32(700)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskName,
			Namespace: scan.Namespace,
			Labels: map[string]string{
				labels.LabelManaged:         "true",
				labels.LabelCreatedBy:       "repository-security",
				labels.LabelSecurityTarget:  labels.SelectorValue(scan.Name),
				labels.LabelSecurityScanID:  run.ID,
				labels.LabelSecurityMode:    run.Mode,
				labels.LabelSecurityStage:   security.StageReview,
				labels.LabelSecuritySliceID: reviewSlice.ID,
			},
			Annotations: map[string]string{
				labels.AnnotationSecurityReviewAttempt: strconv.Itoa(attempt),
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:      corev1alpha1.TaskTypeAgent,
			AgentRef:  &scan.Spec.AnalysisAgentRef,
			Prompt:    prompt,
			Timeout:   &timeout,
			Priority:  &priority,
			Workspace: repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead),
		},
	}
	if err := controllerutil.SetControllerReference(scan, task, r.Scheme); err != nil {
		return nil, err
	}
	return task, nil
}

func (r *RepositoryScanReconciler) progressLatestScanRun(ctx context.Context, scan *corev1alpha1.RepositoryScan) (bool, error) {
	if r.Client == nil || r.SecurityStore == nil {
		return false, nil
	}

	tasks, err := r.listCurrentScanTasks(ctx, scan, "")
	if err != nil {
		return false, err
	}

	scanID := latestOwnedScanPipelineRunID(tasks.Items)
	if scanID == "" {
		scanID = strings.TrimSpace(scan.Status.LastScanID)
	}
	if scanID == "" {
		return false, nil
	}

	state := latestScanPipelineStateForRun(tasks.Items, scanID)
	if state.hasActiveTask {
		return false, nil
	}

	if !state.hasSucceededThreatModel {
		return false, nil
	}

	run, err := r.SecurityStore.GetScanRun(ctx, scan.Namespace, scanID)
	if err != nil {
		return false, err
	}
	if !security.ScanRunMatchesRepositoryScan(run, scan) {
		return false, nil
	}
	if run.Phase == scanRunPhaseSucceeded || run.Phase == scanRunPhaseFailed {
		return false, nil
	}

	if state.hasReviewTasks {
		return r.retryMissingReviewSliceTasks(ctx, scan, run, tasks.Items)
	}

	if !state.hasMapperTasks {
		if err := r.createMapperTask(ctx, scan, run); err != nil {
			return false, err
		}
		run.Phase = scanRunPhaseRunning
		run.Summary = "Threat model generated; deterministic mapper started"
		if err := r.SecurityStore.UpdateScanRun(ctx, run); err != nil {
			return false, err
		}
		return true, nil
	}
	if !state.hasSucceededMapper {
		return false, nil
	}

	return r.progressScanRunAfterMapper(ctx, scan, run)
}

func (r *RepositoryScanReconciler) retryMissingReviewSliceTasks(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	run *store.ScanRun,
	tasks []corev1alpha1.Task,
) (bool, error) {
	reviewSlices, err := r.pendingReviewSlices(ctx, scan, run.ID)
	if err != nil {
		return false, err
	}
	missing := make([]store.ReviewSlice, 0, len(reviewSlices))
	for _, reviewSlice := range reviewSlices {
		if reviewSliceTaskExists(tasks, run.ID, reviewSlice.ID) {
			continue
		}
		missing = append(missing, reviewSlice)
	}
	if len(missing) == 0 {
		return false, nil
	}

	var threatModel string
	if r.SecurityStore != nil {
		model, err := r.SecurityStore.GetLatestThreatModel(ctx, scan.Namespace, scan.Name)
		if err == nil {
			threatModel = model.Content
		} else if !errors.Is(err, store.ErrNotFound) {
			return false, err
		}
	}
	if err := r.createReviewTasks(ctx, scan, run, threatModel, missing); err != nil {
		return false, err
	}
	run.Summary = fmt.Sprintf("Threat model generated; retrying %d pending review slices", len(missing))
	run.Phase = scanRunPhaseRunning
	run.CompletedAt = nil
	run.ErrorMessage = ""
	if err := r.SecurityStore.UpdateScanRun(ctx, run); err != nil {
		return false, err
	}
	return true, nil
}

func reviewSliceTaskExists(tasks []corev1alpha1.Task, runID, sliceID string) bool {
	for i := range tasks {
		task := &tasks[i]
		if scanTaskRunID(task) != runID || taskSecurityStage(task) != security.StageReview {
			continue
		}
		if strings.TrimSpace(task.Labels[labels.LabelSecuritySliceID]) == sliceID {
			return true
		}
	}
	return false
}

func securityReviewAttempt(task *corev1alpha1.Task) (int, bool) {
	if task == nil {
		return 0, false
	}
	value := strings.TrimSpace(task.Annotations[labels.AnnotationSecurityReviewAttempt])
	switch value {
	case "":
		return securityReviewInitialAttempt, true
	case strconv.Itoa(securityReviewInitialAttempt):
		return securityReviewInitialAttempt, true
	case strconv.Itoa(securityReviewRetryAttempt):
		return securityReviewRetryAttempt, true
	default:
		return 0, false
	}
}

func terminalScannerPolicyLoadError(err error) bool {
	if err == nil {
		return false
	}
	var statusErr apierrors.APIStatus
	if errors.As(err, &statusErr) {
		switch statusErr.Status().Reason {
		case metav1.StatusReasonNotFound,
			metav1.StatusReasonInvalid,
			metav1.StatusReasonBadRequest,
			metav1.StatusReasonForbidden:
			return true
		default:
			return false
		}
	}
	message := err.Error()
	return containsAnyPolicyLoadError(message,
		"name is required",
		" is missing in ConfigMap ",
		"must be labeled or annotated",
		"policy exceeds ",
		"policy appears to contain a secret or token",
	)
}

func containsAnyPolicyLoadError(message string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(message, needle) {
			return true
		}
	}
	return false
}

func (r *RepositoryScanReconciler) progressScanRunAfterMapper(ctx context.Context, scan *corev1alpha1.RepositoryScan, run *store.ScanRun) (bool, error) {
	if strings.TrimSpace(run.ErrorMessage) != "" {
		if err := r.refreshScanRunStatus(ctx, scan, run, run.ID, true); err != nil {
			return false, err
		}
		return true, nil
	}

	var threatModel string
	if r.SecurityStore != nil {
		model, err := r.SecurityStore.GetLatestThreatModel(ctx, scan.Namespace, scan.Name)
		if err == nil {
			threatModel = model.Content
		} else if !errors.Is(err, store.ErrNotFound) {
			return false, err
		}
	}

	reviewSlices, err := r.pendingReviewSlices(ctx, scan, run.ID)
	if err != nil {
		return false, err
	}
	if len(reviewSlices) > 0 {
		if err := r.createReviewTasks(ctx, scan, run, threatModel, reviewSlices); err != nil {
			return false, err
		}
		run.Summary = fmt.Sprintf("Threat model generated; %d deterministic review slices started", len(reviewSlices))
		run.Phase = scanRunPhaseRunning
		run.CompletedAt = nil
		run.ErrorMessage = ""
		if err := r.SecurityStore.UpdateScanRun(ctx, run); err != nil {
			return false, err
		}
		return true, nil
	}

	if run.Mode == scanModeIncremental && run.SliceCount > 0 && run.SkippedSliceCount == run.SliceCount {
		now := time.Now().UTC()
		run.Phase = scanRunPhaseSucceeded
		run.CompletedAt = &now
		run.ErrorMessage = ""
		if needsNoopScanSummary(run.Summary) {
			run.Summary = "Threat model generated; no changed files matched deterministic review slices"
		}
		if err := r.SecurityStore.UpdateScanRun(ctx, run); err != nil {
			return false, err
		}
		if err := r.updateNoopScanStatus(ctx, scan, run); err != nil {
			return false, err
		}
		return true, nil
	}

	now := time.Now().UTC()
	run.Phase = scanRunPhaseSucceeded
	run.CompletedAt = &now
	run.ErrorMessage = ""
	if needsNoopScanSummary(run.Summary) {
		run.Summary = "Threat model generated; no reviewable security slices found"
	}
	if err := r.SecurityStore.UpdateScanRun(ctx, run); err != nil {
		return false, err
	}
	if err := r.updateNoopScanStatus(ctx, scan, run); err != nil {
		return false, err
	}

	return true, nil
}

func (r *RepositoryScanReconciler) updateNoopScanStatus(ctx context.Context, scan *corev1alpha1.RepositoryScan, run *store.ScanRun) error {
	counts, err := r.SecurityStore.GetFindingCounts(ctx, scan.Namespace, scan.Name)
	if err != nil {
		return err
	}

	var threatModelVersion int64
	if model, err := r.SecurityStore.GetLatestThreatModel(ctx, scan.Namespace, scan.Name); err == nil {
		threatModelVersion = model.Version
	}

	return r.updateStatusWithRetry(ctx, scan, func(s *corev1alpha1.RepositoryScan) {
		s.Status.Phase = repositoryScanPhaseReady
		s.Status.LastScanID = run.ID
		s.Status.LastScanTaskName = run.TaskName
		s.Status.LastObservedHeadSHA = run.HeadCommit
		s.Status.LastProcessedCommit = run.HeadCommit
		s.Status.ThreatModelVersion = threatModelVersion
		s.Status.FindingCounts = corev1alpha1.FindingCountsStatus{
			Total:    counts.Total,
			Critical: counts.Critical,
			High:     counts.High,
			Medium:   counts.Medium,
			Low:      counts.Low,
		}
		if run.CompletedAt != nil {
			completedAt := &metav1.Time{Time: *run.CompletedAt}
			s.Status.LastScanAt = completedAt
			s.Status.LastSuccessfulScanAt = completedAt
		}
		meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "ScanSucceeded",
			Message:            repositoryScanConditionMessage(run.Summary, "scan completed successfully"),
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: s.Generation,
		})
	})
}

func needsNoopScanSummary(summary string) bool {
	trimmed := strings.TrimSpace(summary)
	return trimmed == "" || trimmed == scanSummaryThreatModelPending
}

func (r *RepositoryScanReconciler) ingestOwnedTasks(ctx context.Context, scan *corev1alpha1.RepositoryScan) error {
	if r.SecurityStore == nil {
		return nil
	}

	tasks, err := r.listCurrentScanTasks(ctx, scan, "")
	if err != nil {
		return err
	}

	slices.SortFunc(tasks.Items, func(a, b corev1alpha1.Task) int {
		if cmp := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Name, b.Name)
	})

	latestScanRunID := latestOwnedScanPipelineRunID(tasks.Items)
	refreshLatestScanRun := false

	for i := range tasks.Items {
		task := &tasks.Items[i]
		stage := taskSecurityStage(task)
		if !isTerminalScanTaskPhase(task.Status.Phase) {
			continue
		}
		if task.Status.Phase == corev1alpha1.TaskPhaseCancelled && !isScanPipelineStage(stage) {
			continue
		}
		if stage == security.StagePatch {
			if err := r.ingestPatchTask(ctx, scan, task); err != nil {
				return err
			}
			continue
		}
		if stage == security.StageValidation {
			if err := r.ingestValidationTask(ctx, scan, task); err != nil {
				return err
			}
			continue
		}

		if !isScanPipelineStage(stage) {
			continue
		}
		if latestScanRunID != "" && scanTaskRunID(task) == latestScanRunID {
			refreshLatestScanRun = true
		}
		if err := r.ingestScanTask(ctx, scan, task); err != nil {
			return err
		}
	}

	if refreshLatestScanRun {
		run, err := r.SecurityStore.GetScanRun(ctx, scan.Namespace, latestScanRunID)
		if err != nil {
			return err
		}
		return r.refreshScanRunStatus(ctx, scan, run, latestScanRunID, true)
	}

	return nil
}

func taskPhaseToSecurityPhase(phase corev1alpha1.TaskPhase) string {
	if phase == corev1alpha1.TaskPhaseSucceeded {
		return scanRunPhaseSucceeded
	}
	if phase == corev1alpha1.TaskPhaseFailed {
		return scanRunPhaseFailed
	}
	if phase == corev1alpha1.TaskPhaseRunning {
		return scanRunPhaseRunning
	}
	return scanRunPhasePending
}

func (r *RepositoryScanReconciler) persistThreatModelIfChanged(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	scanID string,
	scanStartedAt time.Time,
	content string,
) error {
	if r.SecurityStore == nil {
		return nil
	}

	content = strings.TrimSpace(content)
	if content == "" {
		return nil
	}

	latest, latestErr := r.SecurityStore.GetLatestThreatModel(ctx, scan.Namespace, scan.Name)
	if latestErr != nil && !errors.Is(latestErr, store.ErrNotFound) {
		return latestErr
	}
	if latestErr == nil {
		if strings.TrimSpace(latest.Content) == content {
			return nil
		}
		if latest.GeneratedByScan != scanID && !scanStartedAt.IsZero() && scanStartedAt.Before(latest.UpdatedAt) {
			return nil
		}
	}

	model := &store.ThreatModel{
		Namespace:       scan.Namespace,
		RepositoryScan:  scan.Name,
		Content:         content,
		Source:          "generated",
		GeneratedByScan: scanID,
	}
	if err := r.SecurityStore.SaveThreatModel(ctx, model); err != nil {
		return err
	}

	return nil
}

func (r *RepositoryScanReconciler) getArtifactWithRetry(ctx context.Context, namespace, taskName, filename string) ([]byte, error) {
	var lastErr error
	for range 5 {
		data, _, err := r.ArtifactStore.GetArtifact(ctx, namespace, taskName, filename)
		if err == nil {
			return data, nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return nil, lastErr
}

func (r *RepositoryScanReconciler) loadReviewSlicesArtifact(ctx context.Context, task *corev1alpha1.Task) (*security.ReviewSlicesArtifact, string, error) {
	if r.ArtifactStore == nil {
		return nil, "", nil
	}

	data, _, err := r.ArtifactStore.GetArtifact(ctx, task.Namespace, task.Name, security.ArtifactSlices)
	switch {
	case err == nil:
		if len(strings.TrimSpace(string(data))) == 0 {
			return nil, fmt.Sprintf("%s is empty", security.ArtifactSlices), nil
		}
		artifact, err := security.ParseReviewSlicesArtifact(data)
		if err != nil {
			return nil, fmt.Sprintf("%s is invalid: %v", security.ArtifactSlices, err), nil
		}
		return artifact, "", nil
	case errors.Is(err, store.ErrNotFound):
		return nil, fmt.Sprintf("%s is missing", security.ArtifactSlices), nil
	default:
		return nil, "", err
	}
}

func (r *RepositoryScanReconciler) loadMapperReviewContext(
	ctx context.Context,
	task *corev1alpha1.Task,
	sliceID string,
) (string, string, string, error) {
	if r.ArtifactStore == nil {
		return "", "", repositoryScanArtifactStoreNotConfigured, nil
	}
	artifactName := security.ReviewContextArtifactName(sliceID)
	data, err := r.getArtifactWithRetry(ctx, task.Namespace, task.Name, artifactName)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		return "", "", fmt.Sprintf("%s is missing from mapper task", artifactName), nil
	default:
		return "", "", "", err
	}
	manifest, digest, err := security.ParseTrustedReviewContextManifest(data)
	if err != nil {
		return "", "", fmt.Sprintf("%s is invalid: %v", artifactName, err), nil
	}
	if strings.TrimSpace(manifest.SliceID) != strings.TrimSpace(sliceID) {
		return "", "", fmt.Sprintf("%s sliceId does not match mapper slice", artifactName), nil
	}
	canonical, err := json.Marshal(manifest)
	if err != nil {
		return "", "", "", err
	}
	return string(canonical), digest, "", nil
}

func (r *RepositoryScanReconciler) getScanRunForTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, task *corev1alpha1.Task) (*store.ScanRun, error) {
	run, err := r.SecurityStore.GetScanRun(ctx, scan.Namespace, scanTaskRunID(task))
	if err != nil {
		return nil, err
	}
	if !security.ScanRunMatchesRepositoryScan(run, scan) {
		return nil, store.ErrConflict
	}
	return run, nil
}

func conciseTaskMessage(message, fallback string) string {
	for line := range strings.SplitSeq(message, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		return truncateUTF8(line, 512)
	}
	return fallback
}

func (r *RepositoryScanReconciler) pipelineTaskSummary(ctx context.Context, task *corev1alpha1.Task, fallback string) string {
	if task.Status.Message != "" {
		return conciseTaskMessage(task.Status.Message, fallback)
	}
	if r.ResultStore != nil && task.Status.ResultRef != nil && task.Status.ResultRef.Available {
		if result, err := r.ResultStore.GetResult(ctx, task.Namespace, task.Name); err == nil {
			return conciseTaskMessage(string(result), fallback)
		}
	}
	return fallback
}

func (r *RepositoryScanReconciler) pipelineTaskFailureSummary(ctx context.Context, task *corev1alpha1.Task) string {
	stage := "scan"
	switch taskSecurityStage(task) {
	case security.StageThreatModel:
		stage = "threat model"
	case security.StageMapper:
		stage = "mapper"
	case security.StageReview:
		stage = "review"
	}
	outcome := "failed"
	if task.Status.Phase == corev1alpha1.TaskPhaseCancelled {
		outcome = "cancelled"
	}
	return r.pipelineTaskSummary(ctx, task, fmt.Sprintf("%s stage %s", stage, outcome))
}

func (r *RepositoryScanReconciler) loadAgentTaskResult(ctx context.Context, task *corev1alpha1.Task) ([]byte, string, error) {
	if r.ResultStore == nil {
		return nil, "result store is not configured", nil
	}
	if task == nil || task.Status.ResultRef == nil || !task.Status.ResultRef.Available {
		return nil, "terminal task result is not available", nil
	}
	result, err := r.ResultStore.GetResult(ctx, task.Namespace, task.Name)
	switch {
	case err == nil:
		return result, "", nil
	case errors.Is(err, store.ErrNotFound):
		return nil, "terminal task result was not found", nil
	default:
		return nil, "", err
	}
}

func pipelineTaskDisplayName(task *corev1alpha1.Task) string {
	stage := taskSecurityStage(task)
	if stage == security.StageReview {
		if sliceID := strings.TrimSpace(task.Labels[labels.LabelSecuritySliceID]); sliceID != "" {
			return fmt.Sprintf("review:%s", sliceID)
		}
	}
	return stage
}

type scanRunProgress struct {
	hasActive           bool
	hasThreatModelReady bool
	hasMapper           bool
	hasMapperReady      bool
	hasReview           bool
	reviewCount         int
	reviewSucceeded     int
	failedStages        []string
	failureMessage      string
	latestCompletion    *time.Time
}

func recordScanProgressFailure(progress *scanRunProgress, task *corev1alpha1.Task, message string) {
	progress.failedStages = append(progress.failedStages, pipelineTaskDisplayName(task))
	if progress.failureMessage == "" {
		progress.failureMessage = message
	}
}

func (r *RepositoryScanReconciler) collectScanRunProgress(
	ctx context.Context,
	tasks []corev1alpha1.Task,
) scanRunProgress {
	progress := scanRunProgress{}
	reviewTasks := map[string]*corev1alpha1.Task{}
	for i := range tasks {
		task := &tasks[i]
		stage := taskSecurityStage(task)
		if !isScanPipelineStage(stage) {
			continue
		}
		if stage == security.StageReview {
			key := strings.TrimSpace(task.Labels[labels.LabelSecuritySliceID])
			if key == "" {
				key = task.Name
			}
			key = scanTaskRunID(task) + "\x00" + key
			if current := reviewTasks[key]; current == nil || newerSecurityReviewAttempt(task, current) {
				reviewTasks[key] = task
			}
			continue
		}
		recordScanTaskTiming(&progress, task)
		switch stage {
		case security.StageThreatModel:
			if task.Status.Phase == corev1alpha1.TaskPhaseSucceeded {
				progress.hasThreatModelReady = true
			}
			if isFailedScanTaskPhase(task.Status.Phase) {
				recordScanProgressFailure(&progress, task, r.pipelineTaskFailureSummary(ctx, task))
			}
		case security.StageMapper:
			progress.hasMapper = true
			if task.Status.Phase == corev1alpha1.TaskPhaseSucceeded {
				progress.hasMapperReady = true
			}
			if isFailedScanTaskPhase(task.Status.Phase) {
				recordScanProgressFailure(&progress, task, r.pipelineTaskFailureSummary(ctx, task))
			}
		}
	}

	reviewKeys := make([]string, 0, len(reviewTasks))
	for key := range reviewTasks {
		reviewKeys = append(reviewKeys, key)
	}
	sort.Strings(reviewKeys)
	for _, key := range reviewKeys {
		task := reviewTasks[key]
		progress.hasReview = true
		progress.reviewCount++
		recordScanTaskTiming(&progress, task)
		if task.Status.Phase == corev1alpha1.TaskPhaseSucceeded {
			progress.reviewSucceeded++
		}
		if isFailedScanTaskPhase(task.Status.Phase) {
			recordScanProgressFailure(&progress, task, r.pipelineTaskFailureSummary(ctx, task))
		}
	}
	return progress
}

func recordScanTaskTiming(progress *scanRunProgress, task *corev1alpha1.Task) {
	if progress == nil || task == nil {
		return
	}
	if task.Status.Phase == "" || isActiveTaskPhase(task.Status.Phase) {
		progress.hasActive = true
	}
	if task.Status.CompletionTime != nil {
		completed := task.Status.CompletionTime.Time
		if progress.latestCompletion == nil || completed.After(*progress.latestCompletion) {
			progress.latestCompletion = &completed
		}
	}
}

func newerSecurityReviewAttempt(candidate, current *corev1alpha1.Task) bool {
	candidateAttempt, candidateValid := securityReviewAttempt(candidate)
	currentAttempt, currentValid := securityReviewAttempt(current)
	switch {
	case candidateValid != currentValid:
		return candidateValid
	case candidateAttempt != currentAttempt:
		return candidateAttempt > currentAttempt
	case candidate.CreationTimestamp.Time.Equal(current.CreationTimestamp.Time):
		return candidate.Name > current.Name
	default:
		return candidate.CreationTimestamp.After(current.CreationTimestamp.Time)
	}
}

func applyScanRunProgress(run *store.ScanRun, progress scanRunProgress) {
	if run.ErrorMessage != "" {
		run.Phase = scanRunPhaseFailed
		run.Summary = run.ErrorMessage
		if run.CompletedAt == nil && progress.latestCompletion != nil {
			completed := *progress.latestCompletion
			run.CompletedAt = &completed
		}
		return
	}
	if progress.hasActive {
		run.Phase = scanRunPhaseRunning
		run.CompletedAt = nil
		if run.Summary == "" {
			if progress.hasThreatModelReady {
				run.Summary = scanSummaryThreatModelPending
			} else {
				run.Summary = scanSummaryRunning
			}
		}
		return
	}

	if progress.failureMessage != "" || run.ErrorMessage != "" || len(progress.failedStages) > 0 {
		run.Phase = scanRunPhaseFailed
		if progress.latestCompletion != nil {
			run.CompletedAt = progress.latestCompletion
		}
		if progress.failureMessage != "" {
			run.ErrorMessage = progress.failureMessage
		} else if run.ErrorMessage == "" {
			run.ErrorMessage = fmt.Sprintf(
				"scan failed in stages: %s",
				strings.Join(progress.failedStages, ", "),
			)
		}
		run.Summary = run.ErrorMessage
		return
	}

	if progress.hasThreatModelReady && !progress.hasReview {
		if progress.hasMapper && !progress.hasMapperReady {
			run.Phase = scanRunPhaseRunning
			run.CompletedAt = nil
			run.ErrorMessage = ""
			run.Summary = "Threat model generated; deterministic mapper pending"
			return
		}
		run.Phase = scanRunPhaseRunning
		run.CompletedAt = nil
		run.ErrorMessage = ""
		if !progress.hasMapper {
			run.Summary = scanSummaryThreatModelPending
		}
		return
	}

	if progress.hasThreatModelReady && progress.hasReview {
		if progress.reviewSucceeded < progress.reviewCount {
			run.Phase = scanRunPhaseRunning
			run.CompletedAt = nil
			run.ErrorMessage = ""
			run.Summary = fmt.Sprintf(
				"Threat model generated and %d/%d review slices completed successfully",
				progress.reviewSucceeded,
				progress.reviewCount,
			)
			return
		}
		run.Phase = scanRunPhaseSucceeded
		run.ErrorMessage = ""
		if progress.latestCompletion != nil {
			run.CompletedAt = progress.latestCompletion
		}
		run.Summary = fmt.Sprintf(
			"Threat model generated and %d/%d review slices completed successfully",
			progress.reviewSucceeded,
			progress.reviewCount,
		)
		return
	}

	run.Phase = scanRunPhaseSucceeded
	run.ErrorMessage = ""
	if progress.latestCompletion != nil {
		run.CompletedAt = progress.latestCompletion
	}
	run.Summary = "Threat model generated successfully"
}

func (r *RepositoryScanReconciler) refreshScanRunStatus(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	run *store.ScanRun,
	scanID string,
	updateStatus bool,
) error {
	if !security.ScanRunMatchesRepositoryScan(run, scan) {
		return nil
	}
	if r.Client == nil {
		if run.ErrorMessage != "" {
			run.Phase = scanRunPhaseFailed
			run.Summary = run.ErrorMessage
		} else if run.Phase == "" {
			run.Phase = scanRunPhaseRunning
		}
		if err := r.SecurityStore.UpdateScanRun(ctx, run); err != nil {
			return err
		}
		return nil
	}

	tasks, err := r.listCurrentScanTasks(ctx, scan, scanID)
	if err != nil {
		return err
	}
	slices.SortFunc(tasks.Items, func(a, b corev1alpha1.Task) int {
		if cmp := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Name, b.Name)
	})

	progress := r.collectScanRunProgress(ctx, tasks.Items)
	applyScanRunProgress(run, progress)
	if err := r.keepScanRunningForPendingReviewSlices(ctx, scan, run, progress); err != nil {
		return err
	}

	if err := r.SecurityStore.UpdateScanRun(ctx, run); err != nil {
		return err
	}
	if !updateStatus {
		return nil
	}

	counts, err := r.SecurityStore.GetFindingCounts(ctx, scan.Namespace, scan.Name)
	if err != nil {
		return err
	}

	var threatModelVersion int64
	if model, err := r.SecurityStore.GetLatestThreatModel(ctx, scan.Namespace, scan.Name); err == nil {
		threatModelVersion = model.Version
	}

	return r.updateStatusWithRetry(ctx, scan, func(s *corev1alpha1.RepositoryScan) {
		s.Status.LastScanID = run.ID
		s.Status.LastScanTaskName = run.TaskName
		s.Status.LastObservedHeadSHA = run.HeadCommit
		s.Status.ThreatModelVersion = threatModelVersion
		s.Status.FindingCounts = corev1alpha1.FindingCountsStatus{
			Total:    counts.Total,
			Critical: counts.Critical,
			High:     counts.High,
			Medium:   counts.Medium,
			Low:      counts.Low,
		}

		switch run.Phase {
		case scanRunPhaseRunning, scanRunPhasePending:
			s.Status.Phase = repositoryScanPhaseScanning
			meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "Scanning",
				Message:            repositoryScanConditionMessage(run.Summary, scanSummaryRunning),
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: s.Generation,
			})
		case scanRunPhaseSucceeded:
			s.Status.Phase = repositoryScanPhaseReady
			s.Status.LastProcessedCommit = run.HeadCommit
			if run.CompletedAt != nil {
				t := &metav1.Time{Time: *run.CompletedAt}
				s.Status.LastScanAt = t
				s.Status.LastSuccessfulScanAt = t
			}
			meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "ScanSucceeded",
				Message:            repositoryScanConditionMessage(run.Summary, "scan completed successfully"),
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: s.Generation,
			})
		default:
			s.Status.Phase = repositoryScanPhaseError
			if run.CompletedAt != nil {
				s.Status.LastScanAt = &metav1.Time{Time: *run.CompletedAt}
			}
			meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "ScanFailed",
				Message:            repositoryScanConditionMessage(run.Summary, "scan failed"),
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: s.Generation,
			})
		}
	})
}

func (r *RepositoryScanReconciler) keepScanRunningForPendingReviewSlices(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	run *store.ScanRun,
	progress scanRunProgress,
) error {
	if r.SecurityStore == nil || run == nil || run.Phase != scanRunPhaseSucceeded {
		return nil
	}
	if !progress.hasReview {
		return nil
	}
	reviewSlices, err := r.pendingReviewSlices(ctx, scan, run.ID)
	if err != nil {
		return err
	}
	if len(reviewSlices) == 0 {
		return nil
	}
	run.Phase = scanRunPhaseRunning
	run.CompletedAt = nil
	run.ErrorMessage = ""
	run.Summary = fmt.Sprintf("Threat model generated; %d review slices remain pending", len(reviewSlices))
	return nil
}

func (r *RepositoryScanReconciler) shouldAutoValidateFinding(scan *corev1alpha1.RepositoryScan, finding *store.Finding, createdForRun int) bool {
	if finding == nil {
		return false
	}
	minSeverity := security.EffectiveValidationMinSeverity(scan)
	minConfidence := security.EffectiveValidationMinConfidence(scan)
	mode := security.EffectiveValidationMode(scan)
	if mode == validationModeFull {
		if scan.Spec.ValidationMinSeverity == "" {
			minSeverity = validationThresholdLow
		}
		if scan.Spec.ValidationMinConfidence == "" {
			minConfidence = validationThresholdLow
		}
	}
	severityOK := security.SeverityMeetsMinimum(finding.Severity, minSeverity)
	confidenceOK := security.ConfidenceMeetsMinimum(finding.Confidence, minConfidence)
	switch mode {
	case validationModeOff:
		return false
	case "full":
		return severityOK && confidenceOK
	default:
		limit := int(security.EffectiveValidationMaxFindingsPerRun(scan))
		if limit <= 0 || createdForRun >= limit {
			return false
		}
		return severityOK || confidenceOK
	}
}

func (r *RepositoryScanReconciler) hasActiveValidationTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, findingID string) (bool, error) {
	if r.Client == nil {
		return false, nil
	}
	var tasks corev1alpha1.TaskList
	if err := r.List(ctx, &tasks,
		client.InNamespace(scan.Namespace),
		client.MatchingLabels(map[string]string{
			labels.LabelSecurityTarget:    labels.SelectorValue(scan.Name),
			labels.LabelSecurityFindingID: findingID,
			labels.LabelSecurityStage:     security.StageValidation,
		}),
	); err != nil {
		return false, err
	}
	for i := range tasks.Items {
		if isActiveTaskPhase(tasks.Items[i].Status.Phase) {
			return true, nil
		}
	}
	return false, nil
}

func (r *RepositoryScanReconciler) createValidationTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, finding *store.Finding) error {
	if r.Client == nil {
		return nil
	}
	var run *store.ScanRun
	if r.SecurityStore != nil && strings.TrimSpace(finding.ScanRunID) != "" {
		var err error
		run, err = r.SecurityStore.GetScanRun(ctx, scan.Namespace, finding.ScanRunID)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	policy, err := security.LoadScannerPolicy(ctx, r.Client, scan.Namespace, scan.Spec)
	if err != nil {
		if run != nil && activeScanRunPhase(run.Phase) && terminalScannerPolicyLoadError(err) {
			return r.recordTerminalScanRunError(ctx, scan, run, err)
		}
		return err
	}
	if run != nil && activeScanRunPhase(run.Phase) {
		if err := ensureScanRunPolicyDigest(run, policy); err != nil {
			if errors.Is(err, errScannerPolicyDigestChanged) {
				return r.recordTerminalScanRunError(ctx, scan, run, err)
			}
			return err
		}
	}
	timeout := metav1.Duration{Duration: 90 * time.Minute}
	priority := int32(725)
	taskName := security.ScanStageTaskName(scan.Name, "validation", security.StageValidation, finding.ID)
	resultBinding := security.AgentResultBinding{
		RepositoryScan: scan.Name,
		ScanID:         finding.ScanRunID,
		PolicyDigest:   policy.Digest,
	}

	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskName,
			Namespace: scan.Namespace,
			Labels: map[string]string{
				labels.LabelManaged:           "true",
				labels.LabelCreatedBy:         "repository-security",
				labels.LabelSecurityTarget:    labels.SelectorValue(scan.Name),
				labels.LabelSecurityScanID:    finding.ScanRunID,
				labels.LabelSecurityMode:      security.StageValidation,
				labels.LabelSecurityStage:     security.StageValidation,
				labels.LabelSecurityFindingID: finding.ID,
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:      corev1alpha1.TaskTypeAgent,
			AgentRef:  &scan.Spec.AnalysisAgentRef,
			Prompt:    security.BuildValidationResultPrompt(scan, finding, resultBinding, policy.PromptPolicy()),
			Timeout:   &timeout,
			Priority:  &priority,
			Workspace: repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead),
		},
	}
	if err := controllerutil.SetControllerReference(scan, task, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, task); err != nil {
		return err
	}

	finding.ValidationStatus = findingValidationStatusPending
	return r.SecurityStore.UpsertFinding(ctx, finding)
}

func (r *RepositoryScanReconciler) ensureActiveScanRunPolicyCurrent(ctx context.Context, scan *corev1alpha1.RepositoryScan, run *store.ScanRun) error {
	if run == nil || !activeScanRunPhase(run.Phase) {
		return nil
	}
	policy, err := security.LoadScannerPolicy(ctx, r.Client, scan.Namespace, scan.Spec)
	if err != nil {
		if terminalScannerPolicyLoadError(err) {
			return r.recordTerminalScanRunError(ctx, scan, run, err)
		}
		return err
	}
	if err := ensureScanRunPolicyDigest(run, policy); err != nil {
		if errors.Is(err, errScannerPolicyDigestChanged) {
			return r.recordTerminalScanRunError(ctx, scan, run, err)
		}
		return err
	}
	return nil
}

func mergeEvidenceRefs(existing []store.FindingEvidenceRef, refs ...store.FindingEvidenceRef) []store.FindingEvidenceRef {
	merged := append([]store.FindingEvidenceRef{}, existing...)
	seen := map[string]struct{}{}
	for _, ref := range merged {
		key := evidenceRefKey(ref)
		seen[key] = struct{}{}
	}
	for _, ref := range refs {
		key := evidenceRefKey(ref)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, ref)
	}
	return merged
}

func evidenceRefKey(ref store.FindingEvidenceRef) string {
	return strings.Join([]string{
		ref.Kind,
		ref.TaskName,
		ref.Name,
		ref.Label,
		ref.Path,
		fmt.Sprint(ref.StartLine),
		fmt.Sprint(ref.EndLine),
		ref.Symbol,
		ref.Quote,
	}, "|")
}

func (r *RepositoryScanReconciler) mergeExistingFinding(ctx context.Context, scan *corev1alpha1.RepositoryScan, finding *store.Finding) error {
	existing, err := r.SecurityStore.GetFinding(ctx, scan.Namespace, finding.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if existing.State != "" && existing.State != findingStateOpen {
		finding.State = existing.State
	}
	if existing.PatchProposalID != "" {
		finding.PatchProposalID = existing.PatchProposalID
	}
	finding.PRNumber = existing.PRNumber
	finding.PRURL = existing.PRURL
	finding.CreatedAt = existing.CreatedAt
	if existing.ValidationStatus == findingValidationStatusValidated ||
		existing.ValidationStatus == findingValidationStatusPending {
		finding.ValidationStatus = existing.ValidationStatus
	}
	if len(existing.Evidence) > 0 {
		finding.Evidence = mergeEvidenceRefs(existing.Evidence, finding.Evidence...)
	}
	if existing.ValidationJSON != "" {
		finding.ValidationJSON = existing.ValidationJSON
	}
	return nil
}

func (r *RepositoryScanReconciler) persistDroppedFindingDiagnostics(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	task *corev1alpha1.Task,
	run *store.ScanRun,
	diagnostics []security.DroppedFindingDiagnostic,
) error {
	if len(diagnostics) == 0 {
		return nil
	}
	sliceID := strings.TrimSpace(task.Labels[labels.LabelSecuritySliceID])
	for _, diagnostic := range diagnostics {
		dropped := &store.DroppedFinding{
			ID:             "drop_" + security.FindingID(strings.Join([]string{run.ID, task.Name, sliceID, fmt.Sprint(diagnostic.Index), diagnostic.Reason}, "|")),
			Namespace:      scan.Namespace,
			RepositoryScan: scan.Name,
			ScanRunID:      run.ID,
			TaskName:       task.Name,
			SliceID:        sliceID,
			Reason:         diagnostic.Reason,
			Layer:          diagnostic.Layer,
			SampleJSON:     security.DroppedFindingSampleJSON(diagnostic),
		}
		if err := r.SecurityStore.CreateDroppedFinding(ctx, dropped); err != nil {
			return err
		}
	}
	if r.ArtifactStore != nil {
		artifact := security.DroppedFindingArtifact{
			SchemaVersion: 1,
			Dropped:       diagnostics,
		}
		data, err := json.MarshalIndent(artifact, "", "  ")
		if err != nil {
			return err
		}
		if err := r.ArtifactStore.SaveArtifact(ctx, task.Namespace, task.Name, security.ArtifactDroppedFindings, "application/json", data); err != nil {
			return err
		}
	}
	return nil
}

func (r *RepositoryScanReconciler) validationTaskCountForScanRun(ctx context.Context, scan *corev1alpha1.RepositoryScan, scanRunID string) (int, error) {
	if r.Client == nil || strings.TrimSpace(scanRunID) == "" {
		return 0, nil
	}
	var tasks corev1alpha1.TaskList
	if err := r.List(ctx, &tasks,
		client.InNamespace(scan.Namespace),
		client.MatchingLabels(map[string]string{
			labels.LabelSecurityTarget: labels.SelectorValue(scan.Name),
			labels.LabelSecurityStage:  security.StageValidation,
			labels.LabelSecurityScanID: scanRunID,
		}),
	); err != nil {
		return 0, err
	}
	return len(tasks.Items), nil
}

func (r *RepositoryScanReconciler) enqueueAutoValidationTasks(ctx context.Context, scan *corev1alpha1.RepositoryScan, findings []*store.Finding) error {
	createdByRun := map[string]int{}
	for _, finding := range findings {
		if finding == nil {
			continue
		}
		created, ok := createdByRun[finding.ScanRunID]
		if !ok {
			existing, err := r.validationTaskCountForScanRun(ctx, scan, finding.ScanRunID)
			if err != nil {
				return err
			}
			created = existing
		}
		if !r.shouldAutoValidateFinding(scan, finding, created) {
			createdByRun[finding.ScanRunID] = created
			continue
		}
		if finding.ValidationStatus == findingValidationStatusValidated ||
			finding.ValidationStatus == findingValidationStatusPending {
			continue
		}
		active, err := r.hasActiveValidationTask(ctx, scan, finding.ID)
		if err != nil {
			return err
		}
		if active {
			continue
		}
		if err := r.createValidationTask(ctx, scan, finding); err != nil {
			return err
		}
		created++
		createdByRun[finding.ScanRunID] = created
	}
	return nil
}

func clearRunError(run *store.ScanRun) {
	if run == nil {
		return
	}
	if run.Summary == run.ErrorMessage {
		run.Summary = ""
	}
	run.ErrorMessage = ""
}

func clearThreatModelRunError(run *store.ScanRun) {
	if run == nil || run.ErrorMessage == "" {
		return
	}
	if strings.Contains(run.ErrorMessage, security.ArtifactThreatModel) ||
		strings.Contains(run.ErrorMessage, "threat model stage failed") ||
		strings.Contains(run.ErrorMessage, "threat model terminal result") {
		clearRunError(run)
	}
}

func clearReviewRunError(run *store.ScanRun, sliceID string) {
	if run == nil || run.ErrorMessage == "" {
		return
	}
	if sliceID != "" {
		if strings.Contains(run.ErrorMessage, fmt.Sprintf("slice %s:", sliceID)) {
			clearRunError(run)
		}
		if strings.Contains(run.ErrorMessage, "slice ") {
			return
		}
	}
	if strings.Contains(run.ErrorMessage, security.ArtifactFindingsV2) ||
		strings.Contains(run.ErrorMessage, "review stage failed") {
		clearRunError(run)
	}
}

func (r *RepositoryScanReconciler) ingestThreatModelTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, task *corev1alpha1.Task, run *store.ScanRun) error {
	run.TaskName = task.Name
	if mode := task.Labels[labels.LabelSecurityMode]; mode != "" {
		run.Mode = mode
	}

	if task.Status.Phase == corev1alpha1.TaskPhaseSucceeded {
		if err := r.ensureActiveScanRunPolicyCurrent(ctx, scan, run); err != nil {
			return err
		}
		result, validationProblem, err := r.loadAgentTaskResult(ctx, task)
		if err != nil {
			return err
		}
		var threatModel string
		if validationProblem == "" {
			threatModel, err = security.ParseThreatModelResult(result, security.AgentResultBinding{
				RepositoryScan: scan.Name,
				ScanID:         run.ID,
				PolicyDigest:   run.PolicyDigest,
			})
			if err != nil {
				validationProblem = err.Error()
			}
		}
		if validationProblem == "" {
			if err := r.persistThreatModelIfChanged(ctx, scan, run.ID, run.StartedAt, threatModel); err != nil {
				return err
			}
			clearThreatModelRunError(run)
			run.Summary = scanSummaryThreatModelPending
		} else {
			run.ErrorMessage = "threat model terminal result is missing or invalid: " + validationProblem
		}
	} else {
		run.ErrorMessage = r.pipelineTaskFailureSummary(ctx, task)
	}

	return r.refreshScanRunStatus(ctx, scan, run, run.ID, false)
}

func (r *RepositoryScanReconciler) ingestReviewTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, task *corev1alpha1.Task, run *store.ScanRun) error {
	if run.Phase == scanRunPhaseFailed {
		return nil
	}
	sliceID := strings.TrimSpace(task.Labels[labels.LabelSecuritySliceID])
	reviewSlice, staleReviewTask, err := r.reviewSliceForTaskRun(ctx, scan, sliceID, run.ID)
	if err != nil {
		return err
	}
	if staleReviewTask {
		return nil
	}
	if reviewSlice != nil && reviewSlice.Status == reviewSliceStatusReviewed {
		return r.refreshScanRunStatus(ctx, scan, run, run.ID, false)
	}
	attempt, validAttempt := securityReviewAttempt(task)
	retryTaskName := security.ScanStageRetryTaskName(scan.Name, run.ID, security.StageReview, sliceID, securityReviewRetryAttempt)
	if !validAttempt ||
		(attempt == securityReviewRetryAttempt && task.Name != retryTaskName) ||
		(attempt == securityReviewInitialAttempt && task.Name == retryTaskName) {
		return r.failReviewTask(ctx, scan, run, sliceID, "invalid security review attempt identity")
	}

	if task.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		return r.failReviewTask(ctx, scan, run, sliceID, r.pipelineTaskFailureSummary(ctx, task))
	}

	manifest, findingsV2, validationProblem, retryableResult, err := r.validateReviewTaskResult(ctx, scan, task, run, reviewSlice, sliceID)
	if err != nil {
		return err
	}
	if validationProblem != "" {
		if retryableResult {
			retried, retryErr := r.ensureReviewResultRetry(ctx, scan, task, run, reviewSlice, sliceID)
			if retryErr != nil {
				if conflict, ok := errors.AsType[*reviewRetryIdentityConflictError](retryErr); ok {
					// Fail this slice closed rather than returning the
					// error: a returned error re-runs on every reconcile
					// and blocks ingestion for every other run of the scan.
					return r.failReviewTask(ctx, scan, run, sliceID, conflict.Error())
				}
				return retryErr
			}
			if retried {
				run.Phase = scanRunPhaseRunning
				run.CompletedAt = nil
				run.ErrorMessage = ""
				run.Summary = fmt.Sprintf("Retrying review slice %s after an invalid terminal result", sliceID)
				return r.SecurityStore.UpdateScanRun(ctx, run)
			}
		}
		message := validationProblem
		if sliceID != "" {
			message = fmt.Sprintf("slice %s: %s", sliceID, validationProblem)
		}
		return r.failReviewTask(ctx, scan, run, sliceID, message)
	}

	clearReviewRunError(run, sliceID)
	partition := security.ValidateFindingsV2(*findingsV2, *manifest, security.FindingValidationOptions{
		Namespace:            scan.Namespace,
		RepositoryScan:       scan.Name,
		ScanRunID:            run.ID,
		TaskName:             task.Name,
		TrustedRepository:    trustedFindingsRepository(scan, run),
		UseTrustedRepository: true,
	})
	if err := r.ensureActiveScanRunPolicyCurrent(ctx, scan, run); err != nil {
		return err
	}
	filterResult := security.FilterFindings(partition.Accepted, security.FindingFilterOptions{
		RepositoryScan: scan.Name,
		ScanRunID:      run.ID,
		TaskName:       task.Name,
		SliceID:        sliceID,
	})
	partition.Accepted = filterResult.Kept
	partition.Dropped = append(partition.Dropped, filterResult.Dropped...)
	var capDrops []security.DroppedFindingDiagnostic
	partition.Accepted, capDrops = capAcceptedFindingsForRun(scan, run, partition.Accepted)
	partition.Dropped = append(partition.Dropped, capDrops...)
	if err := r.persistDroppedFindingDiagnostics(ctx, scan, task, run, partition.Dropped); err != nil {
		return err
	}
	run.AcceptedFindings += len(partition.Accepted)
	run.DroppedFindings += len(partition.Dropped)
	run.ReviewedSliceCount++
	if findingsV2.Scan.Summary != "" {
		run.Summary = findingsV2.Scan.Summary
	} else if sliceID != "" {
		run.Summary = fmt.Sprintf("Reviewed slice %s", sliceID)
	}
	upserted := make([]*store.Finding, 0, len(partition.Accepted))
	for _, finding := range partition.Accepted {
		if err := r.mergeExistingFinding(ctx, scan, finding); err != nil {
			return err
		}
		if err := r.SecurityStore.UpsertFinding(ctx, finding); err != nil {
			return err
		}
		upserted = append(upserted, finding)
	}
	if err := r.enqueueAutoValidationTasks(ctx, scan, upserted); err != nil {
		return err
	}
	if sliceID != "" {
		if err := r.SecurityStore.UpdateReviewSliceStatus(ctx, scan.Namespace, scan.Name, sliceID, run.ID, reviewSliceStatusReviewed); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return r.refreshScanRunStatus(ctx, scan, run, run.ID, false)
}

func (r *RepositoryScanReconciler) failReviewTask(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	run *store.ScanRun,
	sliceID string,
	message string,
) error {
	run.ErrorMessage = message
	if sliceID != "" {
		if err := r.SecurityStore.UpdateReviewSliceStatus(ctx, scan.Namespace, scan.Name, sliceID, run.ID, reviewSliceStatusFailed); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return r.refreshScanRunStatus(ctx, scan, run, run.ID, false)
}

func (r *RepositoryScanReconciler) validateReviewTaskResult(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	task *corev1alpha1.Task,
	run *store.ScanRun,
	reviewSlice *store.ReviewSlice,
	sliceID string,
) (*security.ReviewContextManifest, *security.FindingsV2Artifact, string, bool, error) {
	if reviewSlice == nil {
		return nil, nil, "trusted review slice was not found", false, nil
	}

	manifest, digest, err := security.ParseTrustedReviewContextManifest([]byte(reviewSlice.ReviewContextJSON))
	if err != nil {
		return nil, nil, "trusted review context is invalid: " + err.Error(), false, nil
	}
	if digest != reviewSlice.ReviewContextHash {
		return nil, nil, "trusted review context digest changed", false, nil
	}
	if strings.TrimSpace(manifest.SliceID) != sliceID {
		return nil, nil, "trusted review context slice does not match task slice", false, nil
	}
	if err := r.ensureActiveScanRunPolicyCurrent(ctx, scan, run); err != nil {
		return nil, nil, "", false, err
	}

	if r.ResultStore == nil {
		return nil, nil, "result store is not configured", false, nil
	}
	if task == nil || task.Status.ResultRef == nil || !task.Status.ResultRef.Available {
		return nil, nil, "terminal task result is not available", true, nil
	}
	result, err := r.ResultStore.GetResult(ctx, task.Namespace, task.Name)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		return nil, nil, "terminal task result was not found", true, nil
	default:
		return nil, nil, "", false, err
	}
	findings, err := security.ParseFindingsResult(result, security.FindingsResultExpectation{
		Binding: security.AgentResultBinding{
			RepositoryScan: scan.Name,
			ScanID:         run.ID,
			PolicyDigest:   run.PolicyDigest,
			ContextDigest:  reviewSlice.ReviewContextHash,
		},
		SliceID:    sliceID,
		Mode:       run.Mode,
		Repository: trustedFindingsRepository(scan, run),
	})
	if err != nil {
		return nil, nil, err.Error(), true, nil
	}
	return manifest, findings, "", false, nil
}

func (r *RepositoryScanReconciler) ensureReviewResultRetry(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	sourceTask *corev1alpha1.Task,
	run *store.ScanRun,
	reviewSlice *store.ReviewSlice,
	sliceID string,
) (bool, error) {
	if r.Client == nil || r.Scheme == nil || r.SecurityStore == nil {
		return false, nil
	}
	if !reviewResultRetryEligible(scan, sourceTask, run, reviewSlice, sliceID) {
		return false, nil
	}

	policy, err := security.LoadScannerPolicy(ctx, r.Client, scan.Namespace, scan.Spec)
	if err != nil {
		return false, err
	}
	if err := ensureScanRunPolicyDigest(run, policy); err != nil {
		return false, err
	}
	var threatModel string
	model, err := r.SecurityStore.GetLatestThreatModel(ctx, scan.Namespace, scan.Name)
	switch {
	case err == nil:
		threatModel = model.Content
	case errors.Is(err, store.ErrNotFound):
	default:
		return false, err
	}

	desired, err := r.buildReviewTask(scan, run, threatModel, *reviewSlice, policy, securityReviewRetryAttempt)
	if err != nil {
		return false, err
	}
	if sourceTask.Name == desired.Name {
		return false, nil
	}
	if err := r.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return false, err
		}
		existing := &corev1alpha1.Task{}
		if getErr := r.Get(ctx, client.ObjectKeyFromObject(desired), existing); getErr != nil {
			return false, getErr
		}
		if !matchingReviewRetryTask(existing, desired) {
			return false, &reviewRetryIdentityConflictError{
				message: fmt.Sprintf("review retry task %s/%s conflicts with the expected retry identity: %s", desired.Namespace, desired.Name, reviewRetryTaskMismatch(existing, desired)),
			}
		}
	}
	return true, nil
}

// reviewRetryIdentityConflictError reports a retry Task that already exists
// under the deterministic retry name but is not the retry this controller
// would render (for example one rendered by an older controller build). The
// conflicting Task is never adopted; the run fails closed for that slice
// instead of aborting ingestion for every run of the scan on each reconcile.
type reviewRetryIdentityConflictError struct {
	message string
}

func (e *reviewRetryIdentityConflictError) Error() string {
	return e.message + "; re-run the scan after removing the stale retry Task"
}

func reviewResultRetryEligible(
	scan *corev1alpha1.RepositoryScan,
	sourceTask *corev1alpha1.Task,
	run *store.ScanRun,
	reviewSlice *store.ReviewSlice,
	sliceID string,
) bool {
	if scan == nil || sourceTask == nil || run == nil || reviewSlice == nil {
		return false
	}
	attempt, validAttempt := securityReviewAttempt(sourceTask)
	if !validAttempt || attempt != securityReviewInitialAttempt || sourceTask.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		return false
	}
	if !activeScanRunPhase(run.Phase) || run.Namespace != scan.Namespace || run.RepositoryScan != scan.Name {
		return false
	}
	if sourceTask.Namespace != scan.Namespace || scanTaskRunID(sourceTask) != run.ID || taskSecurityStage(sourceTask) != security.StageReview {
		return false
	}
	if strings.TrimSpace(sourceTask.Labels[labels.LabelSecuritySliceID]) != sliceID ||
		strings.TrimSpace(reviewSlice.ID) != sliceID ||
		strings.TrimSpace(reviewSlice.LastScanRunID) != run.ID ||
		reviewSlice.Status != reviewSliceStatusPending {
		return false
	}
	return metav1.IsControlledBy(sourceTask, scan)
}

// reviewRetryTaskMismatch names the first identity component that differs so
// a conflict is diagnosable from the controller log without dumping either
// Task (prompts are compared by digest and length only).
func reviewRetryTaskMismatch(existing, desired *corev1alpha1.Task) string {
	if existing == nil || desired == nil {
		return "missing task"
	}
	for key, value := range desired.Labels {
		if existing.Labels[key] != value {
			return fmt.Sprintf("label %s differs", key)
		}
	}
	if existing.Annotations[labels.AnnotationSecurityReviewAttempt] != strconv.Itoa(securityReviewRetryAttempt) {
		return "review attempt annotation differs"
	}
	existingOwner := metav1.GetControllerOf(existing)
	desiredOwner := metav1.GetControllerOf(desired)
	if existingOwner == nil || desiredOwner == nil || existingOwner.UID != desiredOwner.UID {
		return "controller owner differs"
	}
	have := taskSpecWithServerDefaults(existing.Spec)
	want := taskSpecWithServerDefaults(desired.Spec)
	if have.Prompt != want.Prompt {
		return fmt.Sprintf("prompt differs (existing sha256 %.12x len %d, desired sha256 %.12x len %d)", sha256.Sum256([]byte(have.Prompt)), len(have.Prompt), sha256.Sum256([]byte(want.Prompt)), len(want.Prompt))
	}
	have.Prompt, want.Prompt = "", ""
	// Only field paths are reported, never values: this message is persisted
	// into the scan run and the RepositoryScan condition, and a pre-created
	// conflicting Task can carry inline env values, system prompts, or
	// credential-bearing URLs in its spec.
	if paths := specFieldDiffPaths(have, want, reviewRetryMismatchPathLimit); len(paths) > 0 {
		return "spec differs at " + strings.Join(paths, ", ")
	}
	return "unknown difference"
}

// reviewRetryMismatchPathLimit bounds how many differing spec field paths a
// retry conflict diagnostic names so a wildly different Task cannot bloat the
// persisted run error.
const reviewRetryMismatchPathLimit = 8

// specFieldDiffPaths returns the JSON field paths (for example
// "workspace.gitRepo" or "env[1].name") at which two specs differ, in sorted
// order, without any field values. At most limit paths are returned; a
// trailing "…" marks truncation.
func specFieldDiffPaths(have, want any, limit int) []string {
	var haveTree, wantTree any
	haveJSON, err := json.Marshal(have)
	if err != nil {
		return []string{"(unserialisable existing spec)"}
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		return []string{"(unserialisable desired spec)"}
	}
	if err := json.Unmarshal(haveJSON, &haveTree); err != nil {
		return []string{"(unserialisable existing spec)"}
	}
	if err := json.Unmarshal(wantJSON, &wantTree); err != nil {
		return []string{"(unserialisable desired spec)"}
	}
	var paths []string
	collectJSONDiffPaths("", haveTree, wantTree, &paths)
	sort.Strings(paths)
	if limit > 0 && len(paths) > limit {
		paths = append(paths[:limit:limit], "…")
	}
	return paths
}

func collectJSONDiffPaths(prefix string, have, want any, paths *[]string) {
	haveMap, haveIsMap := have.(map[string]any)
	wantMap, wantIsMap := want.(map[string]any)
	if haveIsMap && wantIsMap {
		keys := make(map[string]struct{})
		for key := range haveMap {
			keys[key] = struct{}{}
		}
		for key := range wantMap {
			keys[key] = struct{}{}
		}
		for key := range keys {
			child := key
			if prefix != "" {
				child = prefix + "." + key
			}
			haveChild, haveOK := haveMap[key]
			wantChild, wantOK := wantMap[key]
			if !haveOK || !wantOK {
				*paths = append(*paths, child)
				continue
			}
			collectJSONDiffPaths(child, haveChild, wantChild, paths)
		}
		return
	}
	haveList, haveIsList := have.([]any)
	wantList, wantIsList := want.([]any)
	if haveIsList && wantIsList {
		if len(haveList) != len(wantList) {
			*paths = append(*paths, prefix+" (length)")
			return
		}
		for i := range haveList {
			collectJSONDiffPaths(fmt.Sprintf("%s[%d]", prefix, i), haveList[i], wantList[i], paths)
		}
		return
	}
	if !reflect.DeepEqual(have, want) {
		if prefix == "" {
			prefix = "(root)"
		}
		*paths = append(*paths, prefix)
	}
}

func matchingReviewRetryTask(existing, desired *corev1alpha1.Task) bool {
	return desired != nil && desired.Annotations[labels.AnnotationSecurityReviewAttempt] == strconv.Itoa(securityReviewRetryAttempt) &&
		matchingScanStageTask(existing, desired)
}

func matchingScanStageTask(existing, desired *corev1alpha1.Task) bool {
	if existing == nil || desired == nil || existing.Namespace != desired.Namespace || existing.Name != desired.Name {
		return false
	}
	for key, value := range desired.Labels {
		if existing.Labels[key] != value {
			return false
		}
	}
	if existing.Annotations[labels.AnnotationSecurityReviewAttempt] != desired.Annotations[labels.AnnotationSecurityReviewAttempt] {
		return false
	}
	existingOwner := metav1.GetControllerOf(existing)
	desiredOwner := metav1.GetControllerOf(desired)
	if existingOwner == nil || desiredOwner == nil ||
		existingOwner.APIVersion != desiredOwner.APIVersion ||
		existingOwner.Kind != desiredOwner.Kind ||
		existingOwner.Name != desiredOwner.Name ||
		existingOwner.UID != desiredOwner.UID {
		return false
	}
	// The API server stamps CRD defaults (priority, concurrency policy, run
	// history limits, starting deadline) onto the persisted Task, so compare
	// against the desired spec as it would be stored; otherwise the retry
	// Task the controller itself created on the previous reconcile never
	// matches and ingestion for the whole scan wedges.
	// Both sides are normalised: the API server stamps defaults on the stored
	// Task, while a fake client (tests) stores the spec verbatim. The prompt
	// stays part of the identity so a Task-creating principal cannot
	// substitute its own retry: within one run the prompt is deterministic
	// (the review context digest and policy digest are verified when the Task
	// is built and the slice metadata embedded in it is the immutable
	// projection), and the retry Task name already binds the run ID. A retry
	// rendered by an older controller build with a different prompt format
	// therefore conflicts after an upgrade; the operator re-runs the scan.
	return apiequality.Semantic.DeepEqual(taskSpecWithServerDefaults(existing.Spec), taskSpecWithServerDefaults(desired.Spec))
}

func capAcceptedFindingsForRun(scan *corev1alpha1.RepositoryScan, run *store.ScanRun, accepted []*store.Finding) ([]*store.Finding, []security.DroppedFindingDiagnostic) {
	if len(accepted) == 0 {
		return accepted, nil
	}
	limit := int(security.EffectiveMaxFindingsPerRun(scan))
	remaining := limit - run.AcceptedFindings
	if remaining >= len(accepted) {
		return accepted, nil
	}
	if remaining < 0 {
		remaining = 0
	}

	dropped := make([]security.DroppedFindingDiagnostic, 0, len(accepted)-remaining)
	for i, finding := range accepted[remaining:] {
		dropped = append(dropped, cappedFindingDiagnostic(remaining+i, finding, limit))
	}
	return accepted[:remaining], dropped
}

func cappedFindingDiagnostic(index int, finding *store.Finding, limit int) security.DroppedFindingDiagnostic {
	sample := map[string]string{}
	if finding != nil {
		if strings.TrimSpace(finding.Title) != "" {
			sample["title"] = finding.Title
		}
		if strings.TrimSpace(finding.Category) != "" {
			sample["category"] = finding.Category
		}
		if strings.TrimSpace(finding.Severity) != "" {
			sample["severity"] = finding.Severity
		}
	}
	return security.DroppedFindingDiagnostic{
		Index:  index,
		Reason: fmt.Sprintf("maxFindingsPerRun limit %d reached", limit),
		Sample: sample,
		Layer:  "cap",
	}
}

func (r *RepositoryScanReconciler) reviewSliceForTaskRun(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	sliceID string,
	runID string,
) (*store.ReviewSlice, bool, error) {
	if strings.TrimSpace(sliceID) == "" {
		return nil, false, nil
	}
	reviewSlice, err := r.SecurityStore.GetReviewSlice(ctx, scan.Namespace, scan.Name, sliceID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if reviewSlice.LastScanRunID != "" && reviewSlice.LastScanRunID != runID {
		return reviewSlice, true, nil
	}
	return reviewSlice, false, nil
}

func (r *RepositoryScanReconciler) ingestMapperTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, task *corev1alpha1.Task, run *store.ScanRun) error {
	if run.Phase == scanRunPhaseFailed {
		return nil
	}
	if task.Status.Phase == corev1alpha1.TaskPhaseSucceeded {
		artifact, validationProblem, err := r.loadReviewSlicesArtifact(ctx, task)
		if err != nil {
			return err
		}
		if validationProblem != "" {
			run.ErrorMessage = "mapper stage failed: " + validationProblem
		} else if artifact != nil {
			changedFiles := changedFileSet(artifact.ChangedFiles)
			incrementalSelection := run.Mode == scanModeIncremental && artifact.ChangedFilesComputed
			annotateChangedMetadata := artifact.ChangedFilesComputed && (run.Mode == scanModeIncremental || run.Mode == scanModeManual)
			skippedSlices := 0
			preparedSlices := make([]store.ReviewSlice, 0, len(artifact.Slices))
			for i := range artifact.Slices {
				slice := artifact.Slices[i]
				slice.Namespace = scan.Namespace
				slice.RepositoryScan = scan.Name
				slice.LastScanRunID = run.ID
				if annotateChangedMetadata {
					attachChangedMetadataToReviewSlice(&slice, artifact.ChangedFiles, artifact.ChangedLineRanges)
				}
				if incrementalSelection {
					if reviewSliceMatchesChangedFiles(slice, changedFiles) {
						slice.Status = reviewSliceStatusPending
					} else {
						slice.Status = reviewSliceStatusSkipped
						skippedSlices++
					}
				}
				contextJSON, contextHash, contextProblem, err := r.loadMapperReviewContext(ctx, task, slice.ID)
				if err != nil {
					return err
				}
				if contextProblem != "" {
					run.ErrorMessage = "mapper stage failed: " + contextProblem
					return r.refreshScanRunStatus(ctx, scan, run, run.ID, false)
				}
				slice.ReviewContextJSON = contextJSON
				slice.ReviewContextHash = contextHash
				preparedSlices = append(preparedSlices, slice)
			}
			for i := range preparedSlices {
				slice := &preparedSlices[i]
				if err := r.preserveCurrentRunReviewSliceTerminalState(ctx, scan, slice); err != nil {
					return err
				}
				if err := r.SecurityStore.UpsertReviewSlice(ctx, slice); err != nil {
					return err
				}
			}
			clearRunError(run)
			if artifact.BaseCommit != "" {
				run.BaseCommit = artifact.BaseCommit
			}
			if artifact.HeadCommit != "" {
				run.HeadCommit = artifact.HeadCommit
			}
			if run.PolicyDigest == "" {
				run.PolicyDigest = security.ScannerPolicyDigest(security.ScannerPolicy{})
			}
			if run.IdempotencyKey == "" {
				run.IdempotencyKey = security.ScanRunIdempotencyKey(scan.Namespace, scan.Name, run.Mode, run.BaseCommit, run.HeadCommit, scan.Spec.SubPath, run.PolicyDigest)
			}
			run.SliceCount = len(artifact.Slices)
			run.SkippedSliceCount = skippedSlices
			switch {
			case incrementalSelection && skippedSlices == len(artifact.Slices):
				run.Summary = fmt.Sprintf("Threat model generated; no review slices matched %d changed files", len(artifact.ChangedFiles))
			case incrementalSelection:
				run.Summary = fmt.Sprintf(
					"Threat model generated; deterministic mapper selected %d/%d review slices from %d changed files",
					len(artifact.Slices)-skippedSlices,
					len(artifact.Slices),
					len(artifact.ChangedFiles),
				)
			case run.Mode == scanModeIncremental && artifact.ChangedFilesError != "":
				run.Summary = fmt.Sprintf("Threat model generated; deterministic mapper produced %d review slices after changed-file selection failed", len(artifact.Slices))
			default:
				run.Summary = fmt.Sprintf("Threat model generated; deterministic mapper produced %d review slices", len(artifact.Slices))
			}
		}
	} else {
		run.ErrorMessage = r.pipelineTaskFailureSummary(ctx, task)
	}

	return r.refreshScanRunStatus(ctx, scan, run, run.ID, false)
}

func (r *RepositoryScanReconciler) preserveCurrentRunReviewSliceTerminalState(ctx context.Context, scan *corev1alpha1.RepositoryScan, slice *store.ReviewSlice) error {
	if slice == nil || strings.TrimSpace(slice.ID) == "" || strings.TrimSpace(slice.LastScanRunID) == "" {
		return nil
	}
	existing, err := r.SecurityStore.GetReviewSlice(ctx, scan.Namespace, scan.Name, slice.ID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if existing.LastScanRunID != slice.LastScanRunID || !terminalReviewSliceStatus(existing.Status) {
		return nil
	}
	slice.Status = existing.Status
	slice.LastReviewedAt = existing.LastReviewedAt
	return nil
}

func terminalReviewSliceStatus(status string) bool {
	switch status {
	case reviewSliceStatusReviewed, reviewSliceStatusFailed, reviewSliceStatusCompleted:
		return true
	default:
		return false
	}
}

func (r *RepositoryScanReconciler) ingestScanTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, task *corev1alpha1.Task) error {
	run, err := r.getScanRunForTask(ctx, scan, task)
	if err != nil {
		return err
	}

	switch taskSecurityStage(task) {
	case security.StageThreatModel:
		return r.ingestThreatModelTask(ctx, scan, task, run)
	case security.StageMapper:
		return r.ingestMapperTask(ctx, scan, task, run)
	case security.StageReview:
		return r.ingestReviewTask(ctx, scan, task, run)
	default:
		return nil
	}
}

func (r *RepositoryScanReconciler) ingestValidationTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, task *corev1alpha1.Task) error {
	findingID := task.Labels[labels.LabelSecurityFindingID]
	if findingID == "" {
		return nil
	}

	finding, err := r.SecurityStore.GetFinding(ctx, scan.Namespace, findingID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}

	if task.Status.Phase == corev1alpha1.TaskPhaseSucceeded {
		validationProblem := ""
		if strings.TrimSpace(task.Labels[labels.LabelSecurityScanID]) != strings.TrimSpace(finding.ScanRunID) {
			validationProblem = "validation task scan identity does not match finding"
		}
		policy, policyErr := security.LoadScannerPolicy(ctx, r.Client, scan.Namespace, scan.Spec)
		if policyErr != nil {
			return policyErr
		}
		var artifact *security.ValidationArtifact
		if validationProblem == "" {
			result, resultProblem, err := r.loadAgentTaskResult(ctx, task)
			if err != nil {
				return err
			}
			validationProblem = resultProblem
			if validationProblem == "" {
				artifact, err = security.ParseValidationResult(result, security.ValidationResultExpectation{
					Binding: security.AgentResultBinding{
						RepositoryScan: scan.Name,
						ScanID:         finding.ScanRunID,
						PolicyDigest:   policy.Digest,
					},
					Finding: finding,
				})
				if err != nil {
					validationProblem = err.Error()
				}
			}
		}
		if validationProblem != "" {
			finding.ValidationStatus = findingValidationStatusFailed
			failure := security.ValidationArtifact{
				Version:   1,
				FindingID: finding.ID,
				Status:    findingValidationStatusFailed,
				Summary:   validationProblem,
			}
			data, _ := json.Marshal(failure)
			finding.ValidationJSON = string(data)
		} else if artifact != nil {
			finding.ValidationStatus = strings.TrimSpace(artifact.Status)
			data, err := json.Marshal(artifact)
			if err != nil {
				return err
			}
			finding.ValidationJSON = string(data)
			for _, ref := range artifact.Evidence {
				ref.TaskName = task.Name
				finding.Evidence = mergeEvidenceRefs(finding.Evidence, ref)
			}
		}
	} else {
		finding.ValidationStatus = findingValidationStatusFailed
		finding.ValidationJSON = fmt.Sprintf(
			"{\"status\":\"failed\",\"summary\":%q}",
			r.pipelineTaskSummary(ctx, task, "validation task failed"),
		)
	}

	return r.SecurityStore.UpsertFinding(ctx, finding)
}

type patchVerificationResult struct {
	diffArtifact    string
	summaryArtifact string
	// summary is the normalised pre-existing summary artifact, set only by
	// the artifact contract so the caller can persist the validated form.
	summary *security.PatchSummaryArtifact
}

type securityPatchPublicationReceipt struct {
	branch      string
	prNumber    int
	prURL       string
	publication *store.PatchPublicationEvidence
}

func patchArtifactNames(findingID string) (string, string) {
	return fmt.Sprintf("security-patch-%s.diff", findingID), fmt.Sprintf("security-patch-%s.json", findingID)
}

func patchTaskRequiresArtifactVerification(task *corev1alpha1.Task, findingID string) bool {
	return task != nil && strings.TrimSpace(findingID) != ""
}

func (r *RepositoryScanReconciler) verifyPatchTaskArtifacts(ctx context.Context, scan *corev1alpha1.RepositoryScan, task *corev1alpha1.Task, findingID string) (patchVerificationResult, string, error) {
	if r.ArtifactStore == nil {
		return patchVerificationResult{}, repositoryScanArtifactStoreNotConfigured, nil
	}

	diffName, summaryName := patchArtifactNames(findingID)
	diffData, _, err := r.ArtifactStore.GetArtifact(ctx, task.Namespace, task.Name, diffName)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		return patchVerificationResult{}, fmt.Sprintf("%s is missing", diffName), nil
	default:
		return patchVerificationResult{}, "", err
	}
	summaryData, _, err := r.ArtifactStore.GetArtifact(ctx, task.Namespace, task.Name, summaryName)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		return patchVerificationResult{}, fmt.Sprintf("%s is missing", summaryName), nil
	default:
		return patchVerificationResult{}, "", err
	}

	if len(summaryData) > security.MaxPatchSummaryArtifactBytes {
		return patchVerificationResult{}, fmt.Sprintf("%s exceeds %d bytes", summaryName, security.MaxPatchSummaryArtifactBytes), nil
	}
	var rawSummary security.PatchSummaryArtifact
	if err := json.Unmarshal(summaryData, &rawSummary); err != nil {
		return patchVerificationResult{}, fmt.Sprintf("%s is invalid JSON: %v", summaryName, err), nil
	}
	if rawSummary.SchemaVersion != security.SchemaVersionPatchSummary {
		return patchVerificationResult{}, fmt.Sprintf("%s has unsupported schemaVersion %d", summaryName, rawSummary.SchemaVersion), nil
	}
	if strings.TrimSpace(rawSummary.FindingID) != findingID {
		return patchVerificationResult{}, fmt.Sprintf("%s findingId does not match finding", summaryName), nil
	}
	// A pre-existing artifact is worker-supplied through the upload API, so
	// it gets the same bounded, credential-rejecting validation as a
	// harness-v2 terminal result before it can become durable evidence.
	summary, err := security.NormalizePatchSummaryArtifact(rawSummary)
	if err != nil {
		return patchVerificationResult{}, fmt.Sprintf("%s is invalid: %v", summaryName, err), nil
	}
	if strings.TrimSpace(string(diffData)) == "" {
		return patchVerificationResult{}, "patch diff artifact is empty", nil
	}
	patchFiles, err := repositoryMonitorPathsFromPatch(string(diffData))
	if err != nil {
		return patchVerificationResult{}, fmt.Sprintf("%s is not a canonical git diff: %v", diffName, err), nil
	}
	if !sameStringSet(rootRelativePatchSummaryFiles(summary.ChangedFiles, scan), patchFiles) {
		return patchVerificationResult{}, "patch summary changedFiles do not match the patch diff", nil
	}
	return patchVerificationResult{diffArtifact: diffName, summaryArtifact: summaryName, summary: summary}, "", nil
}

func rootRelativePatchSummaryFiles(files []string, scan *corev1alpha1.RepositoryScan) []string {
	subPath := ""
	if scan != nil {
		subPath = strings.Trim(strings.TrimSpace(strings.ReplaceAll(scan.Spec.SubPath, "\\", "/")), "/")
	}
	if subPath == "" || subPath == "." || !security.SafeRepoPath(subPath) {
		return files
	}

	out := make([]string, 0, len(files))
	for _, file := range files {
		normalized := normalizeRepoPath(file)
		for strings.HasPrefix(normalized, "./") {
			normalized = strings.TrimPrefix(normalized, "./")
		}
		if normalized == "" || normalized == subPath || strings.HasPrefix(normalized, subPath+"/") || strings.HasPrefix(normalized, "/") {
			out = append(out, normalized)
			continue
		}
		out = append(out, subPath+"/"+normalized)
	}
	return out
}

func sameStringSet(left, right []string) bool {
	normalize := func(values []string) []string {
		out := make([]string, 0, len(values))
		seen := map[string]struct{}{}
		for _, value := range values {
			value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
			if value == "" {
				continue
			}
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			out = append(out, value)
		}
		slices.Sort(out)
		return out
	}
	left = normalize(left)
	right = normalize(right)
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

//nolint:gocyclo // Keeping the durable publication tuple checks together makes this fail-closed trust boundary auditable.
func (r *RepositoryScanReconciler) verifiedSecurityPatchPublication(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	task *corev1alpha1.Task,
	requestedBranch string,
) (securityPatchPublicationReceipt, string, error) {
	if scan == nil || task == nil || task.Spec.Workspace == nil {
		return securityPatchPublicationReceipt{}, "governed publication workspace is missing", nil
	}
	workspace := task.Spec.Workspace
	if task.Spec.Type != corev1alpha1.TaskTypeAgent || workspace.Intent != corev1alpha1.WorkspaceIntentWrite || !workspace.CreatePR {
		return securityPatchPublicationReceipt{}, "patch task did not request governed pull request publication", nil
	}
	requestedBranch = strings.TrimSpace(requestedBranch)
	if requestedBranch == "" || strings.TrimSpace(workspace.PushBranch) != requestedBranch {
		return securityPatchPublicationReceipt{}, "patch publication branch does not match the requested branch", nil
	}
	expectedPublicationRepo := security.CanonicalRepositoryCloneURL(scan.Spec.ForkRepo)
	if expectedPublicationRepo == "" {
		expectedPublicationRepo = security.CanonicalRepositoryCloneURL(scan.Spec.RepoURL)
	}
	// Patch Task workspaces are constructed from the canonical HTTPS clone
	// URL, so compare both sides in canonical form to keep SSH-specified scans
	// bound to the same repository identity.
	if security.CanonicalRepositoryCloneURL(workspace.GitRepo) != security.CanonicalRepositoryCloneURL(scan.Spec.RepoURL) ||
		security.CanonicalRepositoryCloneURL(workspace.PublicationGitRepo) != expectedPublicationRepo {
		return securityPatchPublicationReceipt{}, "patch publication repositories do not match the repository scan", nil
	}
	expectedBase := strings.TrimPrefix(strings.TrimSpace(scan.Spec.PRBaseBranch), "refs/heads/")
	if expectedBase == "" {
		expectedBase = strings.TrimPrefix(strings.TrimSpace(security.EffectiveBranch(scan)), "refs/heads/")
	}
	if expectedBase == "" || strings.TrimPrefix(strings.TrimSpace(workspace.PRBaseBranch), "refs/heads/") != expectedBase {
		return securityPatchPublicationReceipt{}, "patch pull request base branch does not match the repository scan", nil
	}
	if r.PublicationStore == nil {
		return securityPatchPublicationReceipt{}, "publication store is not configured", nil
	}
	if task.UID == "" || task.Status.Execution.Attempt < 1 || strings.TrimSpace(task.Status.Execution.PromptID) == "" {
		return securityPatchPublicationReceipt{}, "patch task publication identity is incomplete", nil
	}
	publicationID := publicationIDForTask(task)
	publication, err := r.PublicationStore.GetPublication(ctx, publicationID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return securityPatchPublicationReceipt{}, "durable patch publication record is missing", nil
		}
		return securityPatchPublicationReceipt{}, "", err
	}
	if publication.ID != publicationID || publication.Namespace != task.Namespace || publication.TaskUID != string(task.UID) ||
		publication.Attempt != int64(task.Status.Execution.Attempt) || publication.PromptID != strings.TrimSpace(task.Status.Execution.PromptID) ||
		publication.SessionUID != "" {
		return securityPatchPublicationReceipt{}, "durable patch publication identity does not match the task", nil
	}
	sourceOwner, sourceRepo, err := security.ParseGitHubRepositoryURL(scan.Spec.RepoURL)
	if err != nil {
		return securityPatchPublicationReceipt{}, "repository scan source is not a canonical GitHub repository", nil
	}
	targetOwner, targetRepo, err := security.ParseGitHubRepositoryURL(expectedPublicationRepo)
	if err != nil {
		return securityPatchPublicationReceipt{}, "repository scan publication target is not a canonical GitHub repository", nil
	}
	sourceRepositoryID := "github.com/" + sourceOwner + "/" + sourceRepo
	targetRepositoryID := "github.com/" + targetOwner + "/" + targetRepo
	targetRef, err := canonicalWorkspaceBranchRef(requestedBranch)
	if err != nil {
		return securityPatchPublicationReceipt{}, "patch publication branch is invalid", nil
	}
	baseRef, err := canonicalWorkspaceBranchRef(expectedBase)
	if err != nil {
		return securityPatchPublicationReceipt{}, "patch pull request base branch is invalid", nil
	}
	if publication.State != store.PublicationVerifiedExact || publication.PreparedReceipt == nil || publication.PublishReceipt == nil ||
		publication.VerificationReceipt == nil || publication.PRIntent == nil || publication.PullRequestReceipt == nil {
		return securityPatchPublicationReceipt{}, "durable patch publication is not fully verified with a pull request", nil
	}
	prepared := publication.PreparedReceipt
	published := publication.PublishReceipt
	verification := publication.VerificationReceipt
	intent := publication.PRIntent
	receipt := publication.PullRequestReceipt
	if !strings.EqualFold(publication.SourceRepositoryID, sourceRepositoryID) ||
		!strings.EqualFold(publication.TargetRepositoryID, targetRepositoryID) || publication.TargetRef != targetRef ||
		publication.SourceRef != publication.SourceBaselineSHA || store.ValidateGitObjectID("patch source baseline", publication.SourceBaselineSHA) != nil ||
		store.ValidateCanonicalDigest("patch publication artifact digest", publication.ArtifactDigest) != nil {
		return securityPatchPublicationReceipt{}, "durable patch publication repository or source binding does not match the scan", nil
	}
	expectedRemote := strings.TrimSpace(workspace.ExpectedRemoteSHA)
	if expectedRemote == "" {
		if !publication.Baseline.Absent || publication.Baseline.SHA != "" {
			return securityPatchPublicationReceipt{}, "durable patch publication baseline does not match the absent requested branch", nil
		}
	} else if publication.Baseline.Absent || publication.Baseline.SHA != expectedRemote {
		return securityPatchPublicationReceipt{}, "durable patch publication baseline does not match the requested branch head", nil
	}
	if published.TargetRepositoryID != publication.TargetRepositoryID || published.TargetRef != publication.TargetRef ||
		!published.RemoteBefore.Equal(publication.Baseline) || published.ExpectedCommitSHA != prepared.CommitSHA ||
		verification.Outcome != store.PublicationVerifiedExact || verification.ExpectedCommitSHA != prepared.CommitSHA ||
		verification.ObservedRemote.Absent || verification.ObservedRemote.SHA != prepared.CommitSHA {
		return securityPatchPublicationReceipt{}, "durable patch publication receipts do not prove the exact remote head", nil
	}
	if !strings.EqualFold(intent.BaseRepositoryID, sourceRepositoryID) || intent.BaseRef != baseRef ||
		!strings.EqualFold(intent.HeadRepositoryID, targetRepositoryID) || intent.HeadRef != targetRef ||
		intent.PublicationGeneration != publication.Generation || intent.ExpectedHeadSHA != prepared.CommitSHA ||
		receipt.State != "Open" || receipt.HeadSHA != prepared.CommitSHA || receipt.HeadSHA != intent.ExpectedHeadSHA ||
		store.ValidateCanonicalDigest("patch pull request intent key", receipt.IntentKey) != nil {
		return securityPatchPublicationReceipt{}, "durable patch pull request receipt does not match the exact publication tuple", nil
	}
	prNumber, ok := pullRequestNumberFromForgeID(receipt.ForgeID)
	if !ok {
		return securityPatchPublicationReceipt{}, "durable patch pull request forge identity is invalid", nil
	}
	expectedURL := strings.TrimSuffix(strings.TrimSuffix(security.CanonicalRepositoryCloneURL(scan.Spec.RepoURL), "/"), ".git") +
		"/pull/" + strconv.FormatInt(prNumber, 10)
	if !strings.EqualFold(strings.TrimSpace(receipt.URL), expectedURL) {
		return securityPatchPublicationReceipt{}, "durable patch pull request URL does not match the repository and number", nil
	}
	evidence := &store.PatchPublicationEvidence{
		PublicationID:      publication.ID,
		ArtifactDigest:     publication.ArtifactDigest,
		SourceRepositoryID: publication.SourceRepositoryID,
		SourceRef:          publication.SourceRef,
		SourceBaselineSHA:  publication.SourceBaselineSHA,
		TargetRepositoryID: publication.TargetRepositoryID,
		TargetRef:          publication.TargetRef,
		ExpectedCommitSHA:  prepared.CommitSHA,
		VerifiedRemoteSHA:  verification.ObservedRemote.SHA,
		PRIntent:           *intent,
		PRReceipt: store.PatchPullRequestEvidence{
			IntentKey: receipt.IntentKey,
			ForgeID:   receipt.ForgeID,
			Number:    int(prNumber),
			URL:       receipt.URL,
			State:     receipt.State,
			HeadSHA:   receipt.HeadSHA,
		},
	}
	return securityPatchPublicationReceipt{
		branch: requestedBranch, prNumber: int(prNumber), prURL: strings.TrimSpace(receipt.URL), publication: evidence,
	}, "", nil
}

func (r *RepositoryScanReconciler) updatePatchProposalFromSucceededTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, task *corev1alpha1.Task, findingID string, proposal *store.PatchProposal) error {
	requestedBranch := ""
	if task.Spec.Workspace != nil {
		requestedBranch = strings.TrimSpace(task.Spec.Workspace.PushBranch)
	}
	// The publication is verified first: the harness-v2 evidence path derives
	// the reviewable diff from the exact commit it proves.
	publication, reason, err := r.verifiedSecurityPatchPublication(ctx, scan, task, requestedBranch)
	if err != nil {
		return err
	}
	if reason != "" {
		r.failPatchProposal(ctx, task, proposal, reason)
		return nil
	}
	var verified patchVerificationResult
	if patchTaskRequiresArtifactVerification(task, findingID) {
		var reason string
		verified, reason, err = r.verifyPatchTaskEvidence(ctx, scan, task, findingID, publication)
		if err != nil {
			return err
		}
		if reason != "" {
			r.failPatchProposal(ctx, task, proposal, reason)
			return nil
		}
	}
	proposal.Reason = ""
	proposal.Branch = publication.branch
	proposal.DiffArtifact = verified.diffArtifact
	proposal.SummaryArtifact = verified.summaryArtifact
	proposal.PRNumber = &publication.prNumber
	proposal.PRURL = publication.prURL
	proposal.PublicationEvidence = publication.publication
	proposal.Status = patchProposalStatusPROpened
	r.decorateSecurityPatchPullRequest(ctx, scan, task, findingID, publication.prNumber, verified.summaryArtifact)
	return nil
}

// failPatchProposal marks a proposal failed with an operator-facing reason
// and logs it, so a succeeded patch Task whose evidence could not be verified
// is diagnosable from the API, the dashboard, and the controller log.
func (r *RepositoryScanReconciler) failPatchProposal(ctx context.Context, task *corev1alpha1.Task, proposal *store.PatchProposal, reason string) {
	proposal.Status = scanRunPhaseFailed
	// Reasons can embed agent-echoed text (for example a parser error quoting
	// the supplied result kind); strip controls and redact credential shapes
	// before the reason is persisted and logged.
	proposal.Reason = boundACPStatusMessage(repositoryMonitorReviewContextSanitize(reason))
	log.FromContext(ctx).Info("security patch proposal failed verification",
		"namespace", task.Namespace, "task", task.Name, "finding", proposal.FindingID, "proposal", proposal.ID, "reason", proposal.Reason)
}

func (r *RepositoryScanReconciler) ingestPatchTask(ctx context.Context, scan *corev1alpha1.RepositoryScan, task *corev1alpha1.Task) error {
	findingID := task.Labels[labels.LabelSecurityFindingID]
	if findingID == "" {
		return nil
	}

	proposals, err := r.SecurityStore.ListPatchProposals(ctx, scan.Namespace, findingID)
	if err != nil {
		return err
	}

	var proposal *store.PatchProposal
	for i := range proposals {
		if proposals[i].TaskName == task.Name {
			proposal = &proposals[i]
			break
		}
	}
	if proposal == nil {
		return nil
	}

	proposal.Status = taskPhaseToSecurityPhase(task.Status.Phase)
	requestedBranch := ""
	if task.Spec.Workspace != nil {
		requestedBranch = strings.TrimSpace(task.Spec.Workspace.PushBranch)
	}
	if requestedBranch != "" && strings.TrimSpace(proposal.Branch) == "" {
		proposal.Branch = requestedBranch
	}

	if task.Status.Phase == corev1alpha1.TaskPhaseSucceeded {
		if err := r.updatePatchProposalFromSucceededTask(ctx, scan, task, findingID, proposal); err != nil {
			return err
		}
	}

	if r.ArtifactStore != nil && proposal.Status != scanRunPhaseSucceeded && proposal.Status != patchProposalStatusPROpened {
		artifacts, err := r.ArtifactStore.ListArtifacts(ctx, task.Namespace, task.Name)
		if err == nil {
			for _, artifact := range artifacts {
				if strings.HasSuffix(artifact.Filename, ".diff") && strings.HasPrefix(artifact.Filename, "security-patch-") {
					proposal.DiffArtifact = artifact.Filename
				}
				if strings.HasSuffix(artifact.Filename, ".json") && strings.HasPrefix(artifact.Filename, "security-patch-") {
					proposal.SummaryArtifact = artifact.Filename
				}
			}
		}
	}

	if proposal.Status == patchProposalStatusPROpened && proposal.PublicationEvidence != nil {
		if err := r.SecurityStore.BindPatchProposalPublicationEvidence(ctx, proposal); err != nil {
			return err
		}
	} else {
		proposal.PublicationEvidence = nil
		if err := r.SecurityStore.UpdatePatchProposal(ctx, proposal); err != nil {
			return err
		}
	}

	finding, err := r.SecurityStore.GetFinding(ctx, scan.Namespace, findingID)
	if err != nil {
		return err
	}
	finding.PatchProposalID = proposal.ID
	switch proposal.Status {
	case patchProposalStatusPROpened:
		finding.State = findingStatePROpen
		finding.PRNumber = proposal.PRNumber
		finding.PRURL = proposal.PRURL
	case scanRunPhaseSucceeded:
		finding.State = findingStatePatchReady
	case scanRunPhasePending:
		finding.State = findingStatePatchPending
	default:
		finding.State = findingStateOpen
	}
	return r.SecurityStore.UpsertFinding(ctx, finding)
}

func (r *RepositoryScanReconciler) updateStatusWithRetry(ctx context.Context, scan *corev1alpha1.RepositoryScan, mutate func(*corev1alpha1.RepositoryScan)) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &corev1alpha1.RepositoryScan{}
		if err := r.Get(ctx, types.NamespacedName{Name: scan.Name, Namespace: scan.Namespace}, current); err != nil {
			return err
		}
		if current.UID != scan.UID || current.Generation != scan.Generation || !current.DeletionTimestamp.IsZero() {
			return fmt.Errorf("%w: repository scan changed before status update", store.ErrConflict)
		}
		previousRunID := current.Status.LastScanID
		mutate(current)
		if previousRunID != scan.Status.LastScanID && previousRunID != current.Status.LastScanID {
			return fmt.Errorf("%w: a newer scan run already owns repository scan status", store.ErrConflict)
		}
		return r.Status().Update(ctx, current)
	})
}

// SetupWithManager sets up the controller with the manager.
func (r *RepositoryScanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1alpha1.RepositoryScan{}).
		Owns(&corev1alpha1.Task{}).
		Named("repositoryscan").
		Complete(r)
}
