/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/security"
	storepkg "github.com/orka-agents/orka/internal/store"
	sqlitestore "github.com/orka-agents/orka/internal/store/sqlite"
	"github.com/orka-agents/orka/workers/common"
)

const (
	readyReasonScanFailed       = "ScanFailed"
	testPatchDiffHeader         = "diff --git a/app.py b/app.py"
	testRepositoryScanHeadSHA   = "2222222222222222222222222222222222222222"
	testRepositoryScanMergeSHA  = "1111111111111111111111111111111111111111"
	testRepositoryScanLateMerge = "3333333333333333333333333333333333333333"
	// testPatchFullDiff carries the same change content as the app.py commit
	// stub served by newPatchCommitServer; artifact evidence must match the
	// published commit's content, not just its file names.
	testPatchFullDiff = testPatchDiffHeader + "\n--- a/app.py\n+++ b/app.py\n@@ -1 +1 @@\n-unsafe()\n+safe()\n"
)

func TestRepositoryScanConditionMessageUsesFallback(t *testing.T) {
	got := repositoryScanConditionMessage("  \n\t ", "scan completed successfully")
	if got != "scan completed successfully" {
		t.Fatalf("repositoryScanConditionMessage() = %q, want fallback", got)
	}
}

func TestRepositoryScanConditionMessageTruncatesToKubernetesLimit(t *testing.T) {
	longMessage := strings.Repeat("世", repositoryScanConditionMessageLimit)

	got := repositoryScanConditionMessage(longMessage, "fallback")

	if len(got) > repositoryScanConditionMessageLimit {
		t.Fatalf("len(message) = %d, want <= %d", len(got), repositoryScanConditionMessageLimit)
	}
	if !utf8.ValidString(got) {
		t.Fatal("truncated message is not valid UTF-8")
	}
	if !strings.HasSuffix(got, repositoryScanConditionMessageSuffix) {
		t.Fatalf("message suffix = %q, want %q", got[len(got)-len(repositoryScanConditionMessageSuffix):], repositoryScanConditionMessageSuffix)
	}
}

func TestApplyScanRunProgressPreservesTerminalErrorWithActiveTasks(t *testing.T) {
	completed := mustParseTime(t, "2026-05-04T03:02:01Z")
	run := &storepkg.ScanRun{Phase: scanRunPhaseFailed, ErrorMessage: "scanner policy digest changed", CompletedAt: &completed}

	applyScanRunProgress(run, scanRunProgress{hasActive: true})

	if run.Phase != scanRunPhaseFailed || run.Summary != run.ErrorMessage || run.CompletedAt == nil || !run.CompletedAt.Equal(completed) {
		t.Fatalf("run = %#v, want terminal failure preserved", run)
	}
}

func TestApplyScanRunProgressStampsTerminalErrorCompletion(t *testing.T) {
	completed := mustParseTime(t, "2026-05-04T03:02:01Z")
	run := &storepkg.ScanRun{ErrorMessage: "terminal result binding mismatch"}

	applyScanRunProgress(run, scanRunProgress{latestCompletion: &completed})

	if run.Phase != scanRunPhaseFailed || run.Summary != run.ErrorMessage || run.CompletedAt == nil || !run.CompletedAt.Equal(completed) {
		t.Fatalf("run = %#v, want terminal failure stamped with pipeline completion", run)
	}
}

func TestIngestMapperTaskSkipsFailedRun(t *testing.T) {
	reconciler := &RepositoryScanReconciler{}
	run := &storepkg.ScanRun{Phase: scanRunPhaseFailed, ErrorMessage: "scanner policy digest changed"}
	task := &corev1alpha1.Task{Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded}}

	if err := reconciler.ingestMapperTask(context.Background(), &corev1alpha1.RepositoryScan{}, task, run); err != nil {
		t.Fatalf("ingestMapperTask() error = %v", err)
	}
	if run.Phase != scanRunPhaseFailed || run.ErrorMessage == "" {
		t.Fatalf("run = %#v, want failed run unchanged", run)
	}
}

//nolint:gocyclo // This table-driven regression intentionally verifies terminal state across the run, scan, slice, and retry paths.
func TestRepositoryScanReconcileTreatsCancelledPipelineTasksAsTerminalFailures(t *testing.T) {
	tests := []struct {
		name        string
		stage       string
		sliceID     string
		wantMessage string
	}{
		{name: "threat model", stage: security.StageThreatModel, wantMessage: "threat model stage cancelled"},
		{name: "mapper", stage: security.StageMapper, wantMessage: "mapper stage cancelled"},
		{name: "review", stage: security.StageReview, sliceID: "slice_api", wantMessage: "review stage cancelled"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			securityStore := setupControllerSQLiteStore(t)
			scheme := runtime.NewScheme()
			if err := corev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("AddToScheme() error = %v", err)
			}

			nameSuffix := strings.ReplaceAll(tt.stage, "-", "")
			scanName := "cancelled-" + nameSuffix
			runID := "scan_cancelled_" + nameSuffix
			taskName := scanName + "-" + tt.stage
			completed := metav1.NewTime(mustParseTime(t, "2026-05-08T03:04:05Z"))
			scan := &corev1alpha1.RepositoryScan{
				TypeMeta: metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
				ObjectMeta: metav1.ObjectMeta{
					Name:      scanName,
					Namespace: defaultNS,
				},
				Spec: corev1alpha1.RepositoryScanSpec{
					RepoURL:          "https://github.com/example/repo",
					AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"},
				},
				Status: corev1alpha1.RepositoryScanStatus{
					Phase:            repositoryScanPhaseScanning,
					LastScanID:       runID,
					LastScanTaskName: taskName,
				},
			}
			taskLabels := map[string]string{
				labels.LabelSecurityTarget: labels.SelectorValue(scanName),
				labels.LabelSecurityScanID: runID,
				labels.LabelSecurityMode:   scanModeManual,
				labels.LabelSecurityStage:  tt.stage,
			}
			if tt.sliceID != "" {
				taskLabels[labels.LabelSecuritySliceID] = tt.sliceID
			}
			task := &corev1alpha1.Task{
				TypeMeta: metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "Task"},
				ObjectMeta: metav1.ObjectMeta{
					Name:              taskName,
					Namespace:         defaultNS,
					CreationTimestamp: metav1.NewTime(completed.Add(-time.Minute)),
					Labels:            taskLabels,
				},
				Status: corev1alpha1.TaskStatus{
					Phase:          corev1alpha1.TaskPhaseCancelled,
					CompletionTime: &completed,
				},
			}
			run := &storepkg.ScanRun{
				ID:             runID,
				Namespace:      defaultNS,
				RepositoryScan: scanName,
				TaskName:       taskName,
				Mode:           scanModeManual,
				Phase:          scanRunPhaseRunning,
				StartedAt:      completed.Add(-2 * time.Minute),
			}
			if err := securityStore.CreateScanRun(ctx, run); err != nil {
				t.Fatalf("CreateScanRun() error = %v", err)
			}
			if tt.sliceID != "" {
				if err := securityStore.UpsertReviewSlice(ctx, &storepkg.ReviewSlice{
					ID:             tt.sliceID,
					Namespace:      defaultNS,
					RepositoryScan: scanName,
					Status:         reviewSliceStatusPending,
					LastScanRunID:  runID,
				}); err != nil {
					t.Fatalf("UpsertReviewSlice() error = %v", err)
				}
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
				WithObjects(scan, task).
				Build()
			reconciler := &RepositoryScanReconciler{
				Client:        cl,
				Scheme:        scheme,
				SecurityStore: securityStore,
			}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(scan)}

			assertTerminalFailure := func() (*storepkg.ScanRun, metav1.Condition) {
				t.Helper()
				storedRun, err := securityStore.GetScanRun(ctx, defaultNS, runID)
				if err != nil {
					t.Fatalf("GetScanRun() error = %v", err)
				}
				if storedRun.Phase != scanRunPhaseFailed || storedRun.ErrorMessage != tt.wantMessage || storedRun.Summary != tt.wantMessage {
					t.Fatalf("run phase/error/summary = %q/%q/%q, want failed/%q/%q", storedRun.Phase, storedRun.ErrorMessage, storedRun.Summary, tt.wantMessage, tt.wantMessage)
				}
				if storedRun.CompletedAt == nil || !storedRun.CompletedAt.Equal(completed.Time) {
					t.Fatalf("run.CompletedAt = %v, want %v", storedRun.CompletedAt, completed.Time)
				}

				current := &corev1alpha1.RepositoryScan{}
				if err := cl.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
					t.Fatalf("Get(RepositoryScan) error = %v", err)
				}
				condition := meta.FindStatusCondition(current.Status.Conditions, "Ready")
				if current.Status.Phase != repositoryScanPhaseError || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != readyReasonScanFailed || condition.Message != tt.wantMessage {
					t.Fatalf("scan status/condition = %q/%#v, want Error/ScanFailed %q", current.Status.Phase, condition, tt.wantMessage)
				}
				if current.Status.LastScanAt == nil || !current.Status.LastScanAt.Time.Equal(completed.Time) {
					t.Fatalf("scan.Status.LastScanAt = %v, want %v", current.Status.LastScanAt, completed.Time)
				}
				if current.Status.LastSuccessfulScanAt != nil {
					t.Fatalf("scan.Status.LastSuccessfulScanAt = %v, want nil", current.Status.LastSuccessfulScanAt)
				}
				if tt.sliceID != "" {
					reviewSlice, err := securityStore.GetReviewSlice(ctx, defaultNS, scanName, tt.sliceID)
					if err != nil {
						t.Fatalf("GetReviewSlice() error = %v", err)
					}
					if reviewSlice.Status != reviewSliceStatusFailed {
						t.Fatalf("review slice status = %q, want %q", reviewSlice.Status, reviewSliceStatusFailed)
					}
				}
				return storedRun, *condition
			}

			result, err := reconciler.Reconcile(ctx, req)
			if err != nil {
				t.Fatalf("first Reconcile() error = %v", err)
			}
			if result != (ctrl.Result{}) {
				t.Fatalf("first Reconcile() result = %#v, want no requeue", result)
			}
			firstRun, firstCondition := assertTerminalFailure()

			result, err = reconciler.Reconcile(ctx, req)
			if err != nil {
				t.Fatalf("second Reconcile() error = %v", err)
			}
			if result != (ctrl.Result{}) {
				t.Fatalf("second Reconcile() result = %#v, want no requeue", result)
			}
			secondRun, secondCondition := assertTerminalFailure()
			if secondRun.ErrorMessage != firstRun.ErrorMessage || secondRun.Summary != firstRun.Summary || secondRun.CompletedAt == nil || firstRun.CompletedAt == nil || !secondRun.CompletedAt.Equal(*firstRun.CompletedAt) {
				t.Fatalf("second reconcile changed terminal run: first=%#v second=%#v", firstRun, secondRun)
			}
			if !secondCondition.LastTransitionTime.Time.Equal(firstCondition.LastTransitionTime.Time) {
				t.Fatalf("Ready transition time changed across idempotent reconcile: first=%v second=%v", firstCondition.LastTransitionTime, secondCondition.LastTransitionTime)
			}

			var tasks corev1alpha1.TaskList
			if err := cl.List(ctx, &tasks, client.InNamespace(defaultNS), client.MatchingLabels(map[string]string{labels.LabelSecurityScanID: runID})); err != nil {
				t.Fatalf("List(Tasks) error = %v", err)
			}
			if len(tasks.Items) != 1 {
				t.Fatalf("len(tasks) = %d, want 1 after idempotent reconcile", len(tasks.Items))
			}
		})
	}
}

func TestTrustedFindingsRepositoryScopesCheckoutTarget(t *testing.T) {
	run := &storepkg.ScanRun{
		BaseCommit: "base",
		HeadCommit: "head",
	}
	tests := []struct {
		name string
		spec corev1alpha1.RepositoryScanSpec
		want string
	}{
		{
			name: "implicit main",
			spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo"},
			want: "main",
		},
		{
			name: "explicit ref wins over branch",
			spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo", Branch: "release", Ref: "v1.2.3"},
			want: "ref:v1.2.3",
		},
		{
			name: "ref-only scan is ref scoped",
			spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo", Ref: "refs/tags/v1.2.3"},
			want: "ref:refs/tags/v1.2.3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := &corev1alpha1.RepositoryScan{Spec: tt.spec}

			got := trustedFindingsRepository(scan, run)

			if got.Branch != tt.want {
				t.Fatalf("trustedFindingsRepository().Branch = %q, want %q", got.Branch, tt.want)
			}
			if got.BaseSHA != "base" || got.HeadSHA != "head" {
				t.Fatalf("trustedFindingsRepository() SHAs = %q/%q, want base/head", got.BaseSHA, got.HeadSHA)
			}
		})
	}
}

func TestLatestOwnedScanPipelineRunIDIgnoresPatchAndValidationTasks(t *testing.T) {
	tasks := []corev1alpha1.Task{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "kaset-manual-old",
				CreationTimestamp: metav1.NewTime(mustParseTime(t, "2026-04-10T05:00:00Z")),
				Labels: map[string]string{
					labels.LabelSecurityTarget: "kaset",
					labels.LabelSecurityScanID: "scan_old",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "kaset-validation-f1",
				CreationTimestamp: metav1.NewTime(mustParseTime(t, "2026-04-10T05:02:00Z")),
				Labels: map[string]string{
					labels.LabelSecurityTarget:    "kaset",
					labels.LabelSecurityScanID:    "scan_old",
					labels.LabelSecurityStage:     security.StageValidation,
					labels.LabelSecurityFindingID: "f1",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "kaset-manual-threat-model-new",
				CreationTimestamp: metav1.NewTime(mustParseTime(t, "2026-04-10T05:03:00Z")),
				Labels: map[string]string{
					labels.LabelSecurityTarget: "kaset",
					labels.LabelSecurityScanID: "scan_new",
					labels.LabelSecurityStage:  security.StageThreatModel,
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "kaset-patch-f1",
				CreationTimestamp: metav1.NewTime(mustParseTime(t, "2026-04-10T05:04:00Z")),
				Labels: map[string]string{
					labels.LabelSecurityTarget:    "kaset",
					labels.LabelSecurityScanID:    "scan_new",
					labels.LabelSecurityStage:     security.StagePatch,
					labels.LabelSecurityFindingID: "f1",
				},
			},
		},
	}

	if got := latestOwnedScanPipelineRunID(tasks); got != "scan_new" {
		t.Fatalf("latestOwnedScanPipelineRunID() = %q, want %q", got, "scan_new")
	}
}

func mustParseTime(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("time.Parse(%q): %v", value, err)
	}
	return parsed
}

func newSucceededSecurityTask(name, scanID, stage string, completed metav1.Time) *corev1alpha1.Task {
	labelsMap := map[string]string{
		labels.LabelSecurityTarget: "kaset",
		labels.LabelSecurityScanID: scanID,
		labels.LabelSecurityStage:  stage,
	}
	return &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: defaultNS,
			Labels:    labelsMap,
		},
		Status: corev1alpha1.TaskStatus{
			Phase:          corev1alpha1.TaskPhaseSucceeded,
			CompletionTime: &completed,
		},
	}
}

func testReviewContext(t *testing.T, sliceID string, paths ...string) (security.ReviewContextManifest, string, string) {
	t.Helper()
	prompt := "Trusted mapper review context for " + sliceID + "\n"
	manifest := security.ReviewContextManifest{
		SchemaVersion:     security.SchemaVersionReviewContext,
		SliceID:           sliceID,
		PromptBytes:       len(prompt),
		ApproximateTokens: (len(prompt) + 3) / 4,
		Prompt:            prompt,
	}
	seen := map[string]struct{}{}
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		manifest.IncludedFiles = append(manifest.IncludedFiles, security.ReviewContextIncludedFile{
			Path:               path,
			Role:               "owned",
			IncludedLineRanges: []security.ReviewContextLineRange{{StartLine: 1, EndLine: 10000}},
			Readable:           true,
		})
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("json.Marshal(review context) error = %v", err)
	}
	parsed, digest, err := security.ParseTrustedReviewContextManifest(data)
	if err != nil {
		t.Fatalf("ParseTrustedReviewContextManifest() error = %v", err)
	}
	canonical, err := json.Marshal(parsed)
	if err != nil {
		t.Fatalf("json.Marshal(canonical review context) error = %v", err)
	}
	return *parsed, string(canonical), digest
}

func reviewSlicePaths(slice storepkg.ReviewSlice) []string {
	paths := make([]string, 0, len(slice.OwnedFiles)+len(slice.ContextFiles)+len(slice.Tests))
	for _, file := range slice.OwnedFiles {
		paths = append(paths, file.Path)
	}
	for _, file := range slice.ContextFiles {
		paths = append(paths, file.Path)
	}
	for _, test := range slice.Tests {
		paths = append(paths, test.Path)
	}
	return paths
}

func saveMapperArtifactWithContexts(t *testing.T, store *sqlitestore.Store, task *corev1alpha1.Task, artifact security.ReviewSlicesArtifact) {
	t.Helper()
	ctx := context.Background()
	data, err := json.Marshal(artifact)
	if err != nil {
		t.Fatalf("json.Marshal(review slices) error = %v", err)
	}
	if err := store.SaveArtifact(ctx, task.Namespace, task.Name, security.ArtifactSlices, "application/json", data); err != nil {
		t.Fatalf("SaveArtifact(slices) error = %v", err)
	}
	for _, slice := range artifact.Slices {
		_, contextJSON, _ := testReviewContext(t, slice.ID, reviewSlicePaths(slice)...)
		if err := store.SaveArtifact(ctx, task.Namespace, task.Name, security.ReviewContextArtifactName(slice.ID), "application/json", []byte(contextJSON)); err != nil {
			t.Fatalf("SaveArtifact(review context %s) error = %v", slice.ID, err)
		}
	}
}

func bindReviewSliceContext(t *testing.T, slice *storepkg.ReviewSlice) security.ReviewContextManifest {
	t.Helper()
	manifest, contextJSON, digest := testReviewContext(t, slice.ID, reviewSlicePaths(*slice)...)
	slice.ReviewContextJSON = contextJSON
	slice.ReviewContextHash = digest
	return manifest
}

func reviewedSliceWithContext(t *testing.T, repositoryScan, runID, sliceID string, paths ...string) *storepkg.ReviewSlice {
	t.Helper()
	reviewSlice := &storepkg.ReviewSlice{
		ID:             sliceID,
		Namespace:      defaultNS,
		RepositoryScan: repositoryScan,
		Status:         reviewSliceStatusReviewed,
		LastScanRunID:  runID,
	}
	for _, path := range paths {
		reviewSlice.OwnedFiles = append(reviewSlice.OwnedFiles, storepkg.ReviewSliceFile{Path: path})
	}
	bindReviewSliceContext(t, reviewSlice)
	return reviewSlice
}

func saveFindingsTaskResult(
	t *testing.T,
	store *sqlitestore.Store,
	task *corev1alpha1.Task,
	repositoryScan, scanID, policyDigest, contextDigest, sliceID string,
	findings security.FindingsV2Artifact,
) {
	t.Helper()
	if task.Spec.Workspace == nil {
		branch := strings.TrimSpace(findings.Repository.Branch)
		ref := ""
		if after, ok := strings.CutPrefix(branch, "ref:"); ok {
			ref = after
			branch = ""
		}
		task.Spec.Workspace = &corev1alpha1.WorkspaceConfig{
			Intent:  corev1alpha1.WorkspaceIntentRead,
			GitRepo: findings.Repository.RepoURL,
			Branch:  branch,
			Ref:     ref,
			SubPath: findings.Repository.SubPath,
		}
	}
	result := security.FindingsResultEnvelope{
		SchemaVersion:  security.AgentResultSchemaVersion,
		Kind:           security.AgentResultKindFindings,
		RepositoryScan: repositoryScan,
		ScanID:         scanID,
		SliceID:        sliceID,
		PolicyDigest:   policyDigest,
		ContextDigest:  contextDigest,
		Findings:       findings,
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal(findings result) error = %v", err)
	}
	if err := store.SaveResult(context.Background(), task.Namespace, task.Name, data); err != nil {
		t.Fatalf("SaveResult(findings) error = %v", err)
	}
	task.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
}

type reviewResultRetryFixture struct {
	ctx        context.Context
	store      *sqlitestore.Store
	client     client.Client
	reconciler *RepositoryScanReconciler
	scan       *corev1alpha1.RepositoryScan
	run        *storepkg.ScanRun
	slice      *storepkg.ReviewSlice
	sourceTask *corev1alpha1.Task
}

func newReviewResultRetryFixture(t *testing.T) *reviewResultRetryFixture {
	t.Helper()
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

	scan := &corev1alpha1.RepositoryScan{
		TypeMeta: metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "retry-scan",
			Namespace: defaultNS,
			UID:       types.UID("retry-scan-uid"),
		},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          "https://github.com/example/repo",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"},
		},
		Status: corev1alpha1.RepositoryScanStatus{Phase: repositoryScanPhaseScanning, LastScanID: "scan_retry_result"},
	}
	policyDigest := security.ScannerPolicyDigest(security.ScannerPolicy{})
	run := &storepkg.ScanRun{
		ID:             "scan_retry_result",
		Namespace:      defaultNS,
		RepositoryScan: scan.Name,
		TaskName:       "retry-scan-initial-threat-model",
		Mode:           "initial",
		Phase:          scanRunPhaseRunning,
		PolicyDigest:   policyDigest,
		StartedAt:      time.Now().Add(-time.Minute),
	}
	if err := securityStore.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	if err := securityStore.SaveThreatModel(ctx, &storepkg.ThreatModel{
		Namespace:       defaultNS,
		RepositoryScan:  scan.Name,
		Content:         "# Threat model\n\nReview authentication boundaries.",
		Source:          "generated",
		GeneratedByScan: run.ID,
	}); err != nil {
		t.Fatalf("SaveThreatModel() error = %v", err)
	}
	reviewSlice := &storepkg.ReviewSlice{
		SchemaVersion:  1,
		ID:             "slice_api",
		Namespace:      defaultNS,
		RepositoryScan: scan.Name,
		Source:         "deterministic-go-package",
		Title:          "Go package internal/api",
		Summary:        "API handlers",
		Kind:           "package",
		OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "source"}},
		Confidence:     "high",
		Status:         reviewSliceStatusPending,
		LastScanRunID:  run.ID,
	}
	bindReviewSliceContext(t, reviewSlice)
	if err := securityStore.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}

	sourceTask := &corev1alpha1.Task{
		TypeMeta: metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "Task"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "retry-scan-review-source",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelManaged:         "true",
				labels.LabelCreatedBy:       "repository-security",
				labels.LabelSecurityTarget:  labels.SelectorValue(scan.Name),
				labels.LabelSecurityScanID:  run.ID,
				labels.LabelSecurityMode:    run.Mode,
				labels.LabelSecurityStage:   security.StageReview,
				labels.LabelSecuritySliceID: reviewSlice.ID,
			},
			Annotations: map[string]string{labels.AnnotationSecurityReviewAttempt: "0"},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:      corev1alpha1.TaskTypeAgent,
			AgentRef:  &corev1alpha1.AgentReference{Name: "poison-source-agent"},
			Prompt:    "POISON SOURCE PROMPT MUST NOT BE COPIED",
			Workspace: repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead),
		},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseSucceeded,
			ResultRef: &corev1alpha1.ResultReference{Available: true},
		},
	}
	if err := controllerutil.SetControllerReference(scan, sourceTask, scheme); err != nil {
		t.Fatalf("SetControllerReference() error = %v", err)
	}
	if err := securityStore.SaveResult(ctx, sourceTask.Namespace, sourceTask.Name, []byte(`{"not":"the required findings envelope"}`)); err != nil {
		t.Fatalf("SaveResult(malformed) error = %v", err)
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithObjects(scan, sourceTask).
		Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: securityStore, ResultStore: securityStore}

	return &reviewResultRetryFixture{
		ctx:        ctx,
		store:      securityStore,
		client:     cl,
		reconciler: reconciler,
		scan:       scan,
		run:        run,
		slice:      reviewSlice,
		sourceTask: sourceTask,
	}
}

func (f *reviewResultRetryFixture) retryTaskName() string {
	return security.ScanStageRetryTaskName(f.scan.Name, f.run.ID, security.StageReview, f.slice.ID, securityReviewRetryAttempt)
}

func (f *reviewResultRetryFixture) getRetryTask(t *testing.T) *corev1alpha1.Task {
	t.Helper()
	task := &corev1alpha1.Task{}
	if err := f.client.Get(f.ctx, client.ObjectKey{Namespace: defaultNS, Name: f.retryTaskName()}, task); err != nil {
		t.Fatalf("Get(retry Task) error = %v", err)
	}
	return task
}

func TestIngestReviewTaskCreatesOneControllerRebuiltRetry(t *testing.T) {
	fixture := newReviewResultRetryFixture(t)
	if err := fixture.reconciler.ingestScanTask(fixture.ctx, fixture.scan, fixture.sourceTask); err != nil {
		t.Fatalf("ingestScanTask(source) error = %v", err)
	}

	retryTask := fixture.getRetryTask(t)
	if retryTask.Annotations[labels.AnnotationSecurityReviewAttempt] != "1" {
		t.Fatalf("retry attempt annotation = %q, want 1", retryTask.Annotations[labels.AnnotationSecurityReviewAttempt])
	}
	if !metav1.IsControlledBy(retryTask, fixture.scan) {
		t.Fatalf("retry owner references = %#v, want RepositoryScan controller", retryTask.OwnerReferences)
	}
	if retryTask.Spec.AgentRef == nil || retryTask.Spec.AgentRef.Name != fixture.scan.Spec.AnalysisAgentRef.Name {
		t.Fatalf("retry AgentRef = %#v, want controller-rebuilt %q", retryTask.Spec.AgentRef, fixture.scan.Spec.AnalysisAgentRef.Name)
	}
	if strings.Contains(retryTask.Spec.Prompt, "POISON SOURCE PROMPT") ||
		!strings.Contains(retryTask.Spec.Prompt, "Trusted mapper review context for slice_api") ||
		!strings.Contains(retryTask.Spec.Prompt, "only automatic result retry") {
		t.Fatalf("retry prompt was not rebuilt from trusted context: %q", retryTask.Spec.Prompt)
	}
	if retryTask.Spec.Workspace == nil || retryTask.Spec.Workspace.Intent != corev1alpha1.WorkspaceIntentRead || len(retryTask.Spec.Env) != 0 {
		t.Fatalf("retry workspace/env = %#v/%#v, want read workspace and empty env", retryTask.Spec.Workspace, retryTask.Spec.Env)
	}

	run, err := fixture.store.GetScanRun(fixture.ctx, defaultNS, fixture.run.ID)
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.Phase != scanRunPhaseRunning || run.ErrorMessage != "" || !strings.Contains(run.Summary, "Retrying review slice") {
		t.Fatalf("run after retry creation = %#v, want running retry state", run)
	}
	reviewSlice, err := fixture.store.GetReviewSlice(fixture.ctx, defaultNS, fixture.scan.Name, fixture.slice.ID)
	if err != nil {
		t.Fatalf("GetReviewSlice() error = %v", err)
	}
	if reviewSlice.Status != reviewSliceStatusPending {
		t.Fatalf("review slice status = %q, want pending during retry", reviewSlice.Status)
	}

	if err := fixture.reconciler.ingestScanTask(fixture.ctx, fixture.scan, fixture.sourceTask); err != nil {
		t.Fatalf("second ingestScanTask(source) error = %v", err)
	}
	var tasks corev1alpha1.TaskList
	if err := fixture.client.List(fixture.ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Tasks) error = %v", err)
	}
	if len(tasks.Items) != 2 {
		t.Fatalf("len(tasks) = %d, want source plus one deterministic retry", len(tasks.Items))
	}
	progress := fixture.reconciler.collectScanRunProgress(fixture.ctx, tasks.Items)
	if progress.reviewCount != 1 || progress.reviewSucceeded != 0 || !progress.hasActive {
		t.Fatalf("logical retry progress = %#v, want one active review slice", progress)
	}
}

func TestIngestReviewTaskAcceptsRetryOnceAndCountsOneLogicalSlice(t *testing.T) {
	fixture := newReviewResultRetryFixture(t)
	if err := fixture.reconciler.ingestScanTask(fixture.ctx, fixture.scan, fixture.sourceTask); err != nil {
		t.Fatalf("ingestScanTask(source) error = %v", err)
	}
	retryTask := fixture.getRetryTask(t)
	findings := security.FindingsV2Artifact{
		SchemaVersion: security.SchemaVersionFindingsV2,
		Repository: security.FindingsV2Repository{
			RepoURL: fixture.scan.Spec.RepoURL,
			Branch:  "main",
		},
		Scan:     security.FindingsV2Scan{Mode: fixture.run.Mode, SliceID: fixture.slice.ID, Summary: "retry completed"},
		Findings: []security.FindingsV2Finding{},
	}
	saveFindingsTaskResult(
		t, fixture.store, retryTask, fixture.scan.Name, fixture.run.ID, fixture.run.PolicyDigest,
		fixture.slice.ReviewContextHash, fixture.slice.ID, findings,
	)
	retryTask.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	retryTask.Status.CompletionTime = &metav1.Time{Time: time.Now()}
	if err := fixture.client.Update(fixture.ctx, retryTask); err != nil {
		t.Fatalf("Update(retry Task) error = %v", err)
	}

	if err := fixture.reconciler.ingestOwnedTasks(fixture.ctx, fixture.scan); err != nil {
		t.Fatalf("ingestOwnedTasks() error = %v", err)
	}
	run, err := fixture.store.GetScanRun(fixture.ctx, defaultNS, fixture.run.ID)
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.Phase != scanRunPhaseSucceeded || run.ReviewedSliceCount != 1 || run.ErrorMessage != "" {
		t.Fatalf("run after successful retry = %#v, want one succeeded review slice", run)
	}
	reviewSlice, err := fixture.store.GetReviewSlice(fixture.ctx, defaultNS, fixture.scan.Name, fixture.slice.ID)
	if err != nil {
		t.Fatalf("GetReviewSlice() error = %v", err)
	}
	if reviewSlice.Status != reviewSliceStatusReviewed {
		t.Fatalf("review slice status = %q, want reviewed", reviewSlice.Status)
	}

	if err := fixture.reconciler.ingestOwnedTasks(fixture.ctx, fixture.scan); err != nil {
		t.Fatalf("second ingestOwnedTasks() error = %v", err)
	}
	run, err = fixture.store.GetScanRun(fixture.ctx, defaultNS, fixture.run.ID)
	if err != nil {
		t.Fatalf("GetScanRun(second) error = %v", err)
	}
	if run.ReviewedSliceCount != 1 || run.Phase != scanRunPhaseSucceeded {
		t.Fatalf("idempotent run = %#v, want one succeeded review slice", run)
	}
	var tasks corev1alpha1.TaskList
	if err := fixture.client.List(fixture.ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Tasks) error = %v", err)
	}
	progress := fixture.reconciler.collectScanRunProgress(fixture.ctx, tasks.Items)
	if progress.reviewCount != 1 || progress.reviewSucceeded != 1 {
		t.Fatalf("logical completed progress = %#v, want 1/1", progress)
	}
}

func TestIngestReviewTaskMalformedRetryExhaustsWithoutAttemptTwo(t *testing.T) {
	fixture := newReviewResultRetryFixture(t)
	if err := fixture.reconciler.ingestScanTask(fixture.ctx, fixture.scan, fixture.sourceTask); err != nil {
		t.Fatalf("ingestScanTask(source) error = %v", err)
	}
	retryTask := fixture.getRetryTask(t)
	if err := fixture.store.SaveResult(fixture.ctx, retryTask.Namespace, retryTask.Name, []byte(`{"still":"invalid"}`)); err != nil {
		t.Fatalf("SaveResult(retry malformed) error = %v", err)
	}
	retryTask.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	retryTask.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
	retryTask.Status.CompletionTime = &metav1.Time{Time: time.Now()}
	if err := fixture.client.Update(fixture.ctx, retryTask); err != nil {
		t.Fatalf("Update(retry Task) error = %v", err)
	}

	if err := fixture.reconciler.ingestOwnedTasks(fixture.ctx, fixture.scan); err != nil {
		t.Fatalf("ingestOwnedTasks() error = %v", err)
	}
	run, err := fixture.store.GetScanRun(fixture.ctx, defaultNS, fixture.run.ID)
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.Phase != scanRunPhaseFailed || !strings.Contains(run.ErrorMessage, "decode security task result") {
		t.Fatalf("run after malformed retry = %#v, want terminal parse failure", run)
	}
	reviewSlice, err := fixture.store.GetReviewSlice(fixture.ctx, defaultNS, fixture.scan.Name, fixture.slice.ID)
	if err != nil {
		t.Fatalf("GetReviewSlice() error = %v", err)
	}
	if reviewSlice.Status != reviewSliceStatusFailed {
		t.Fatalf("review slice status = %q, want failed", reviewSlice.Status)
	}
	var tasks corev1alpha1.TaskList
	if err := fixture.client.List(fixture.ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Tasks) error = %v", err)
	}
	if len(tasks.Items) != 2 {
		t.Fatalf("len(tasks) = %d, want no attempt two", len(tasks.Items))
	}
	if err := fixture.reconciler.ingestOwnedTasks(fixture.ctx, fixture.scan); err != nil {
		t.Fatalf("second ingestOwnedTasks() error = %v", err)
	}
	if err := fixture.client.List(fixture.ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Tasks second) error = %v", err)
	}
	if len(tasks.Items) != 2 {
		t.Fatalf("len(tasks after repeat) = %d, want exhausted retry budget", len(tasks.Items))
	}
}

func TestIngestReviewTaskRejectsConflictingDeterministicRetry(t *testing.T) {
	fixture := newReviewResultRetryFixture(t)
	conflict := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: fixture.retryTaskName(), Namespace: defaultNS}}
	if err := fixture.client.Create(fixture.ctx, conflict); err != nil {
		t.Fatalf("Create(conflicting retry) error = %v", err)
	}

	// The conflicting Task is never adopted, and the conflict must not
	// surface as a reconcile error either: that would re-run on every
	// reconcile and block ingestion for every run of the scan. The slice
	// fails closed with the diagnostic instead.
	if err := fixture.reconciler.ingestScanTask(fixture.ctx, fixture.scan, fixture.sourceTask); err != nil {
		t.Fatalf("ingestScanTask(source) error = %v, want the conflict recorded on the run instead", err)
	}
	run, getErr := fixture.store.GetScanRun(fixture.ctx, defaultNS, fixture.run.ID)
	if getErr != nil {
		t.Fatalf("GetScanRun() error = %v", getErr)
	}
	if !strings.Contains(run.ErrorMessage, "conflicts with the expected retry identity") {
		t.Fatalf("run after collision = %#v, want the retry identity conflict recorded", run)
	}
	reviewSlice, err := fixture.store.GetReviewSlice(fixture.ctx, defaultNS, fixture.scan.Name, fixture.slice.ID)
	if err != nil {
		t.Fatalf("GetReviewSlice() error = %v", err)
	}
	if reviewSlice.Status != reviewSliceStatusFailed {
		t.Fatalf("review slice status = %q, want failed (closed) after the conflict", reviewSlice.Status)
	}
	var tasks corev1alpha1.TaskList
	if err := fixture.client.List(fixture.ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Tasks) error = %v", err)
	}
	for _, task := range tasks.Items {
		if task.Name == fixture.retryTaskName() && len(task.OwnerReferences) != 0 {
			t.Fatalf("conflicting retry Task was adopted: %#v", task.OwnerReferences)
		}
	}
	// A repeat ingestion pass stays quiet instead of re-raising the conflict.
	if err := fixture.reconciler.ingestScanTask(fixture.ctx, fixture.scan, fixture.sourceTask); err != nil {
		t.Fatalf("second ingestScanTask(source) error = %v, want none", err)
	}
}

func TestIngestReviewTaskRejectsInvalidAttemptIdentityWithoutRetry(t *testing.T) {
	tests := []struct {
		name         string
		annotation   string
		useRetryName bool
	}{
		{name: "malformed", annotation: "not-a-number"},
		{name: "negative", annotation: "-1"},
		{name: "out of range", annotation: "2"},
		{name: "retry attempt on source name", annotation: "1"},
		{name: "initial attempt on retry name", annotation: "0", useRetryName: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newReviewResultRetryFixture(t)
			fixture.sourceTask.Annotations[labels.AnnotationSecurityReviewAttempt] = tt.annotation
			if tt.useRetryName {
				fixture.sourceTask.Name = fixture.retryTaskName()
			}

			if err := fixture.reconciler.ingestScanTask(fixture.ctx, fixture.scan, fixture.sourceTask); err != nil {
				t.Fatalf("ingestScanTask() error = %v", err)
			}
			run, err := fixture.store.GetScanRun(fixture.ctx, defaultNS, fixture.run.ID)
			if err != nil {
				t.Fatalf("GetScanRun() error = %v", err)
			}
			if run.Phase != scanRunPhaseFailed || !strings.Contains(run.ErrorMessage, "invalid security review attempt identity") {
				t.Fatalf("run = %#v, want invalid attempt failure", run)
			}
			var tasks corev1alpha1.TaskList
			if err := fixture.client.List(fixture.ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
				t.Fatalf("List(Tasks) error = %v", err)
			}
			if len(tasks.Items) != 1 {
				t.Fatalf("len(tasks) = %d, want no retry", len(tasks.Items))
			}
		})
	}
}

func TestIngestReviewTaskResultRetryEligibility(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*reviewResultRetryFixture)
		wantRetry bool
		wantError string
	}{
		{
			name: "result reference unavailable",
			mutate: func(f *reviewResultRetryFixture) {
				f.sourceTask.Status.ResultRef = nil
			},
			wantRetry: true,
		},
		{
			name: "result missing after available reference",
			mutate: func(f *reviewResultRetryFixture) {
				f.sourceTask.Name = "retry-scan-review-result-missing"
			},
			wantRetry: true,
		},
		{
			name: "result store unavailable",
			mutate: func(f *reviewResultRetryFixture) {
				f.reconciler.ResultStore = nil
			},
			wantError: "result store is not configured",
		},
		{
			name: "trusted context corrupt",
			mutate: func(f *reviewResultRetryFixture) {
				f.slice.ReviewContextJSON = `{"schemaVersion":1,"sliceId":"wrong"}`
				f.slice.ReviewContextHash = "sha256:wrong"
				if err := f.store.UpsertReviewSlice(f.ctx, f.slice); err != nil {
					t.Fatalf("UpsertReviewSlice(corrupt) error = %v", err)
				}
			},
			wantError: "trusted review context",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newReviewResultRetryFixture(t)
			tt.mutate(fixture)
			if err := fixture.reconciler.ingestScanTask(fixture.ctx, fixture.scan, fixture.sourceTask); err != nil {
				t.Fatalf("ingestScanTask() error = %v", err)
			}

			var tasks corev1alpha1.TaskList
			if err := fixture.client.List(fixture.ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
				t.Fatalf("List(Tasks) error = %v", err)
			}
			wantTasks := 1
			if tt.wantRetry {
				wantTasks = 2
			}
			if len(tasks.Items) != wantTasks {
				t.Fatalf("len(tasks) = %d, want %d", len(tasks.Items), wantTasks)
			}
			run, err := fixture.store.GetScanRun(fixture.ctx, defaultNS, fixture.run.ID)
			if err != nil {
				t.Fatalf("GetScanRun() error = %v", err)
			}
			if tt.wantRetry {
				if run.Phase != scanRunPhaseRunning || run.ErrorMessage != "" {
					t.Fatalf("retryable run = %#v, want active", run)
				}
				return
			}
			if run.Phase != scanRunPhaseFailed || !strings.Contains(run.ErrorMessage, tt.wantError) {
				t.Fatalf("non-retryable run = %#v, want failure containing %q", run, tt.wantError)
			}
		})
	}
}

func saveValidationTaskResult(
	t *testing.T,
	store *sqlitestore.Store,
	task *corev1alpha1.Task,
	repositoryScan, scanID, policyDigest string,
	validation security.ValidationArtifact,
) {
	t.Helper()
	result := security.ValidationResultEnvelope{
		SchemaVersion:  security.AgentResultSchemaVersion,
		Kind:           security.AgentResultKindValidation,
		RepositoryScan: repositoryScan,
		ScanID:         scanID,
		FindingID:      validation.FindingID,
		PolicyDigest:   policyDigest,
		Validation:     validation,
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal(validation result) error = %v", err)
	}
	if err := store.SaveResult(context.Background(), task.Namespace, task.Name, data); err != nil {
		t.Fatalf("SaveResult(validation) error = %v", err)
	}
	task.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
}

func TestIngestMapperTaskPersistsReviewSlices(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{
		SecurityStore: store,
		ArtifactStore: store,
	}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kaset-mapper",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget: "kaset",
				labels.LabelSecurityScanID: "scan_mapper",
				labels.LabelSecurityMode:   "initial",
				labels.LabelSecurityStage:  security.StageMapper,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	artifact := security.ReviewSlicesArtifact{
		SchemaVersion: security.SchemaVersionReviewSlices,
		Slices: []storepkg.ReviewSlice{{
			ID:             "slice_kaset_api",
			RepositoryScan: "kaset",
			Source:         "deterministic-go-package",
			Title:          "Go package internal/api",
			Summary:        "API handlers",
			Kind:           "package",
			OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "source"}},
			Confidence:     "high",
			Status:         reviewSliceStatusPending,
		}},
	}
	saveMapperArtifactWithContexts(t, store, task, artifact)

	if err := reconciler.ingestScanTask(ctx, scan, task); err != nil {
		t.Fatalf("ingestScanTask() error = %v", err)
	}
	got, err := store.GetReviewSlice(ctx, defaultNS, "kaset", "slice_kaset_api")
	if err != nil {
		t.Fatalf("GetReviewSlice() error = %v", err)
	}
	if got.LastScanRunID != "scan_mapper" || got.Namespace != defaultNS {
		t.Fatalf("review slice = %#v, want scan metadata", got)
	}
	if got.ReviewContextJSON == "" || got.ReviewContextHash == "" {
		t.Fatalf("review slice context = %q/%q, want persisted mapper context", got.ReviewContextJSON, got.ReviewContextHash)
	}
	parsedContext, digest, err := security.ParseTrustedReviewContextManifest([]byte(got.ReviewContextJSON))
	if err != nil {
		t.Fatalf("ParseTrustedReviewContextManifest(stored) error = %v", err)
	}
	if parsedContext.SliceID != got.ID || digest != got.ReviewContextHash {
		t.Fatalf("stored review context = slice %q digest %q, want %q/%q", parsedContext.SliceID, digest, got.ID, got.ReviewContextHash)
	}
	run, err := store.GetScanRun(ctx, defaultNS, "scan_mapper")
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.SliceCount != 1 {
		t.Fatalf("run.SliceCount = %d, want 1", run.SliceCount)
	}
}

func TestIngestMapperTaskSelectsIncrementalSlicesFromChangedFiles(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{
		SecurityStore: store,
		ArtifactStore: store,
	}
	maxFindings := int32(1)
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			MaxFindingsPerRun: &maxFindings,
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kaset-incremental-mapper",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget: "kaset",
				labels.LabelSecurityScanID: "scan_incremental_mapper",
				labels.LabelSecurityMode:   "incremental",
				labels.LabelSecurityStage:  security.StageMapper,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	artifact := security.ReviewSlicesArtifact{
		SchemaVersion:        security.SchemaVersionReviewSlices,
		BaseCommit:           "base123",
		HeadCommit:           "head456",
		ChangedFilesComputed: true,
		ChangedFiles:         []string{"internal/api/security.go", "internal/security/security_test.go"},
		Slices: []storepkg.ReviewSlice{
			{
				ID:             "slice_api",
				RepositoryScan: "kaset",
				Source:         "deterministic-go-package",
				Title:          "Go package internal/api",
				Summary:        "API handlers",
				Kind:           "package",
				OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "source"}},
				Confidence:     "high",
				Status:         reviewSliceStatusReviewed,
			},
			{
				ID:             "slice_security_tests",
				RepositoryScan: "kaset",
				Source:         "deterministic-go-package",
				Title:          "Go package internal/security",
				Summary:        "Security helpers",
				Kind:           "package",
				OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/security/security.go", Reason: "source"}},
				ContextFiles:   []storepkg.ReviewSliceFile{{Path: "internal/security/security_test.go", Reason: "tests"}},
				Confidence:     "high",
				Status:         reviewSliceStatusReviewed,
			},
			{
				ID:             "slice_unaffected",
				RepositoryScan: "kaset",
				Source:         "deterministic-go-package",
				Title:          "Go package internal/store",
				Summary:        "Store helpers",
				Kind:           "package",
				OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/store/store.go", Reason: "source"}},
				Confidence:     "high",
				Status:         reviewSliceStatusReviewed,
			},
		},
	}
	saveMapperArtifactWithContexts(t, store, task, artifact)
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{
		ID:             "scan_incremental_mapper",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		TaskName:       task.Name,
		Mode:           "incremental",
		Phase:          scanRunPhaseRunning,
		BaseCommit:     "base123",
		IdempotencyKey: "original-active-key",
		StartedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}

	if err := reconciler.ingestScanTask(ctx, scan, task); err != nil {
		t.Fatalf("ingestScanTask() error = %v", err)
	}

	for _, id := range []string{"slice_api", "slice_security_tests"} {
		reviewSlice, err := store.GetReviewSlice(ctx, defaultNS, "kaset", id)
		if err != nil {
			t.Fatalf("GetReviewSlice(%s) error = %v", id, err)
		}
		if reviewSlice.Status != reviewSliceStatusPending {
			t.Fatalf("%s status = %q, want pending", id, reviewSlice.Status)
		}
	}
	reviewSlice, err := store.GetReviewSlice(ctx, defaultNS, "kaset", "slice_unaffected")
	if err != nil {
		t.Fatalf("GetReviewSlice(slice_unaffected) error = %v", err)
	}
	if reviewSlice.Status != reviewSliceStatusSkipped {
		t.Fatalf("slice_unaffected status = %q, want skipped", reviewSlice.Status)
	}
	run, err := store.GetScanRun(ctx, defaultNS, "scan_incremental_mapper")
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.SliceCount != 3 || run.SkippedSliceCount != 1 {
		t.Fatalf("run slice counts = %d/%d, want 3/1", run.SliceCount, run.SkippedSliceCount)
	}
	if run.HeadCommit != "head456" {
		t.Fatalf("run.HeadCommit = %q, want head456", run.HeadCommit)
	}
	if run.IdempotencyKey != "original-active-key" {
		t.Fatalf("run.IdempotencyKey = %q, want stable active key", run.IdempotencyKey)
	}
}

func TestMapperReingestPreservesReviewedSliceForCurrentRun(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{
		SecurityStore: store,
		ArtifactStore: store,
		ResultStore:   store,
	}
	maxFindings := int32(1)
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:           "https://github.com/example/repo",
			MaxFindingsPerRun: &maxFindings,
		},
	}

	mapperTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kaset-mapper-reingest",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget: "kaset",
				labels.LabelSecurityScanID: "scan_mapper_reingest",
				labels.LabelSecurityMode:   "initial",
				labels.LabelSecurityStage:  security.StageMapper,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	mapperArtifact := security.ReviewSlicesArtifact{
		SchemaVersion: security.SchemaVersionReviewSlices,
		HeadCommit:    "head123",
		Slices: []storepkg.ReviewSlice{{
			ID:             "slice_api",
			RepositoryScan: "kaset",
			Source:         "deterministic-go-package",
			Title:          "Go package internal/api",
			Summary:        "API handlers",
			Kind:           "package",
			OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "source"}},
			Confidence:     "high",
			Status:         reviewSliceStatusPending,
		}},
	}
	saveMapperArtifactWithContexts(t, store, mapperTask, mapperArtifact)

	reviewTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kaset-review-reingest",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget:  "kaset",
				labels.LabelSecurityScanID:  "scan_mapper_reingest",
				labels.LabelSecurityMode:    "initial",
				labels.LabelSecurityStage:   security.StageReview,
				labels.LabelSecuritySliceID: "slice_api",
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	findings := security.FindingsV2Artifact{
		SchemaVersion: security.SchemaVersionFindingsV2,
		Repository: security.FindingsV2Repository{
			RepoURL: "https://github.com/example/repo",
			Branch:  "main",
			HeadSHA: "head123",
		},
		Scan: security.FindingsV2Scan{Mode: "initial", SliceID: "slice_api", Summary: "one accepted"},
		Findings: []security.FindingsV2Finding{{
			Title:       "Unsafe API behavior",
			Category:    "authz",
			Severity:    "high",
			Confidence:  "high",
			Summary:     "API path lacks authorization.",
			Remediation: "Add authorization checks.",
			Evidence: []security.FindingsV2EvidenceRef{{
				Path:      "internal/api/security.go",
				StartLine: 5,
				EndLine:   8,
			}},
		}},
	}
	if err := reconciler.ingestScanTask(ctx, scan, mapperTask); err != nil {
		t.Fatalf("ingest mapper error = %v", err)
	}
	persistedSlice, err := store.GetReviewSlice(ctx, defaultNS, scan.Name, "slice_api")
	if err != nil {
		t.Fatalf("GetReviewSlice(after mapper) error = %v", err)
	}
	runAfterMapper, err := store.GetScanRun(ctx, defaultNS, "scan_mapper_reingest")
	if err != nil {
		t.Fatalf("GetScanRun(after mapper) error = %v", err)
	}
	saveFindingsTaskResult(t, store, reviewTask, scan.Name, runAfterMapper.ID, runAfterMapper.PolicyDigest, persistedSlice.ReviewContextHash, "slice_api", findings)
	if err := reconciler.ingestScanTask(ctx, scan, reviewTask); err != nil {
		t.Fatalf("ingest review error = %v", err)
	}
	if err := reconciler.ingestScanTask(ctx, scan, mapperTask); err != nil {
		t.Fatalf("reingest mapper error = %v", err)
	}
	if err := reconciler.ingestScanTask(ctx, scan, reviewTask); err != nil {
		t.Fatalf("reingest review error = %v", err)
	}

	run, err := store.GetScanRun(ctx, defaultNS, "scan_mapper_reingest")
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.ReviewedSliceCount != 1 || run.AcceptedFindings != 1 || run.DroppedFindings != 0 {
		t.Fatalf("run counts = reviewed:%d accepted:%d dropped:%d, want 1/1/0", run.ReviewedSliceCount, run.AcceptedFindings, run.DroppedFindings)
	}
	reviewSlice, err := store.GetReviewSlice(ctx, defaultNS, "kaset", "slice_api")
	if err != nil {
		t.Fatalf("GetReviewSlice() error = %v", err)
	}
	if reviewSlice.Status != reviewSliceStatusReviewed {
		t.Fatalf("review slice status = %q, want reviewed after mapper reingest", reviewSlice.Status)
	}
}

func TestRepositoryScanCustomPolicyIncludedInReviewPrompt(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:                   "https://github.com/example/repo",
			AnalysisAgentRef:          corev1alpha1.AgentReference{Name: "scan-reviewer"},
			CustomScanInstructionsRef: &corev1alpha1.PolicyConfigMapKeyRef{Name: "scan-policy", Key: "scan"},
			FalsePositivePolicyRef:    &corev1alpha1.PolicyConfigMapKeyRef{Name: "scan-policy", Key: "fp"},
		},
	}
	policyConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "scan-policy", Namespace: defaultNS, Labels: map[string]string{security.PolicyConfigMapAllowedLabel: "true"}},
		Data: map[string]string{
			"scan": "Focus on operator RBAC drift.",
			"fp":   "Suppress intentionally public demo endpoint noise.",
		},
	}
	targetTask := newSucceededSecurityTask("kaset-policy-target", "scan_policy", security.StageThreatModel, metav1.Now())
	targetTask.Spec.Workspace = repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead)
	scan.Spec.Branch = "release"
	scan.Spec.SubPath = "services/new"
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan, policyConfig, targetTask).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: store}
	run := &storepkg.ScanRun{ID: "scan_policy", Namespace: defaultNS, RepositoryScan: "kaset", TaskName: targetTask.Name, Mode: "initial", Phase: scanRunPhaseRunning}
	reviewSlice := storepkg.ReviewSlice{ID: "slice_api", RepositoryScan: "kaset", Source: "deterministic", Title: "API", Kind: "package", Status: reviewSliceStatusPending}
	manifest := bindReviewSliceContext(t, &reviewSlice)
	if err := reconciler.createReviewTasks(ctx, scan, run, "", []storepkg.ReviewSlice{reviewSlice}); err != nil {
		t.Fatalf("createReviewTasks() error = %v", err)
	}
	var tasks corev1alpha1.TaskList
	if err := cl.List(ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Task) error = %v", err)
	}
	var reviewTask *corev1alpha1.Task
	for i := range tasks.Items {
		if taskSecurityStage(&tasks.Items[i]) == security.StageReview {
			reviewTask = &tasks.Items[i]
			break
		}
	}
	if reviewTask == nil {
		t.Fatalf("review task not found in %#v", tasks.Items)
	}
	if reviewTask.Spec.Workspace == nil || reviewTask.Spec.Workspace.Branch != "main" || reviewTask.Spec.Workspace.SubPath != "" {
		t.Fatalf("review workspace = %#v, want frozen initial target", reviewTask.Spec.Workspace)
	}
	prompt := reviewTask.Spec.Prompt
	for _, want := range []string{"Focus on operator RBAC drift", "public demo endpoint", "Default Orka security policy"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("review prompt missing %q:\n%s", want, prompt)
		}
	}
	if !strings.Contains(prompt, manifest.Prompt) || !strings.Contains(prompt, security.AgentResultKindFindings) {
		t.Fatalf("review prompt missing trusted context or terminal result contract: %q", prompt)
	}
	if strings.Contains(prompt, "REQUIRED_SECURITY_ARTIFACTS") || !strings.Contains(prompt, "Do not write artifacts") || len(tasks.Items[0].Spec.Env) != 0 {
		t.Fatalf("review task retained artifact/env contract: prompt=%q env=%#v", prompt, tasks.Items[0].Spec.Env)
	}
	if run.PolicyDigest == "" {
		t.Fatal("run.PolicyDigest was not populated")
	}
}

func TestRepositoryScanCustomPolicyMissingConfigMapFails(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:                   "https://github.com/example/repo",
			AnalysisAgentRef:          corev1alpha1.AgentReference{Name: "scan-reviewer"},
			CustomScanInstructionsRef: &corev1alpha1.PolicyConfigMapKeyRef{Name: "missing", Key: "policy"},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(scan).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: setupControllerSQLiteStore(t)}
	if err := reconciler.createScanRun(ctx, scan, "initial", "", ""); err == nil || !strings.Contains(err.Error(), "customScanInstructionsRef") {
		t.Fatalf("createScanRun() error = %v, want missing custom policy error", err)
	}
	current := &corev1alpha1.RepositoryScan{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
		t.Fatalf("Get(RepositoryScan) error = %v", err)
	}
	if current.Status.Phase != repositoryScanPhaseError {
		t.Fatalf("RepositoryScan phase = %q, want %q", current.Status.Phase, repositoryScanPhaseError)
	}
	ready := meta.FindStatusCondition(current.Status.Conditions, "Ready")
	if ready == nil || ready.Reason != readyReasonScanFailed || !strings.Contains(ready.Message, "customScanInstructionsRef") {
		t.Fatalf("Ready condition = %#v, want ScanFailed policy error", ready)
	}
}

func TestRepositoryScanIdempotencySkipsDuplicateActiveRun(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec:       corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo", AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"}},
	}
	policyDigest := security.ScannerPolicyDigest(security.ScannerPolicy{})
	key := security.ScanRunIdempotencyKey(defaultNS, "kaset", scanModeIncremental, "base", "", "", policyDigest)
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{ID: "scan_existing", Namespace: defaultNS, RepositoryScan: "kaset", TaskName: "existing", Mode: scanModeIncremental, Phase: scanRunPhaseRunning, IdempotencyKey: key, PolicyDigest: policyDigest, StartedAt: time.Now()}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	existingTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "existing-pipeline",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget: labels.SelectorValue(scan.Name),
				labels.LabelSecurityScanID: "scan_existing",
				labels.LabelSecurityStage:  security.StageThreatModel,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan, existingTask).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: store}
	if err := reconciler.createScanRun(ctx, scan, scanModeIncremental, "base", ""); err != nil {
		t.Fatalf("createScanRun() error = %v", err)
	}
	var tasks corev1alpha1.TaskList
	if err := cl.List(ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Task) error = %v", err)
	}
	if len(tasks.Items) != 1 || tasks.Items[0].Name != "existing-pipeline" {
		t.Fatalf("tasks = %#v, want existing active pipeline only", tasks.Items)
	}
}

func TestRepositoryScanIdempotencyMarksOrphanedRunFailedAndStartsReplacement(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec:       corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo", AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"}},
	}
	policyDigest := security.ScannerPolicyDigest(security.ScannerPolicy{})
	key := security.ScanRunIdempotencyKey(defaultNS, "kaset", scanModeIncremental, "base", "", "", policyDigest)
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{ID: "scan_orphaned", Namespace: defaultNS, RepositoryScan: "kaset", TaskName: "missing", Mode: scanModeIncremental, Phase: scanRunPhaseRunning, IdempotencyKey: key, PolicyDigest: policyDigest, StartedAt: time.Now().Add(-2 * scanRunAdmissionGrace)}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(scan).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: store}
	if err := reconciler.createScanRun(ctx, scan, scanModeIncremental, "base", ""); err != nil {
		t.Fatalf("createScanRun() error = %v", err)
	}
	orphaned, err := store.GetScanRun(ctx, defaultNS, "scan_orphaned")
	if err != nil {
		t.Fatalf("GetScanRun(orphaned) error = %v", err)
	}
	if orphaned.Phase != scanRunPhaseFailed || !strings.Contains(orphaned.ErrorMessage, "no active pipeline task") {
		t.Fatalf("orphaned run = %#v, want failed stale run", orphaned)
	}
	var tasks corev1alpha1.TaskList
	if err := cl.List(ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Task) error = %v", err)
	}
	if len(tasks.Items) != 1 || taskSecurityStage(&tasks.Items[0]) != security.StageThreatModel {
		t.Fatalf("tasks = %#v, want replacement threat-model task", tasks.Items)
	}
}

func TestCreateScanRunConcurrentReconcilesCreateOnePipeline(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "concurrent-security", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          "https://github.com/example/repo",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(scan).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: securityStore}

	start := make(chan struct{})
	results := make(chan error, 4)
	for range 4 {
		go func() {
			<-start
			results <- reconciler.createScanRun(ctx, scan, "manual", "base", "")
		}()
	}
	close(start)
	for range 4 {
		if err := <-results; err != nil {
			t.Fatalf("createScanRun() error = %v", err)
		}
	}

	var tasks corev1alpha1.TaskList
	if err := cl.List(ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Task) error = %v", err)
	}
	if len(tasks.Items) != 1 || taskSecurityStage(&tasks.Items[0]) != security.StageThreatModel {
		t.Fatalf("tasks = %#v, want one threat-model task", tasks.Items)
	}
	runs, _, err := securityStore.ListScanRuns(ctx, defaultNS, scan.Name, 10, "")
	if err != nil {
		t.Fatalf("ListScanRuns() error = %v", err)
	}
	if len(runs) != 1 || runs[0].Phase != scanRunPhasePending {
		t.Fatalf("runs = %#v, want one pending run", runs)
	}
}

func TestProgressLatestScanRunStartsReviewTasksForPendingSlices(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

	scan := &corev1alpha1.RepositoryScan{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "RepositoryScan",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          "https://github.com/sozercan/kaset",
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"},
		},
		Status: corev1alpha1.RepositoryScanStatus{LastScanID: "scan_review"},
	}
	threatTask := newSucceededSecurityTask("kaset-initial-threat", "scan_review", security.StageThreatModel, metav1.Now())
	threatTask.Spec.Workspace = repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead)
	mapperTask := newSucceededSecurityTask("kaset-initial-mapper", "scan_review", security.StageMapper, metav1.Now())
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithObjects(scan, threatTask, mapperTask).
		Build()
	reconciler := &RepositoryScanReconciler{
		Client:        cl,
		Scheme:        scheme,
		SecurityStore: store,
	}
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{
		ID:             "scan_review",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		TaskName:       threatTask.Name,
		Mode:           "initial",
		Phase:          scanRunPhasePending,
		StartedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	reviewSlice := &storepkg.ReviewSlice{
		SchemaVersion:  1,
		ID:             "slice_api",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		Source:         "deterministic-go-package",
		Title:          "Go package internal/api",
		Summary:        "API handlers",
		Kind:           "package",
		OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "source"}},
		Confidence:     "high",
		Status:         reviewSliceStatusPending,
		LastScanRunID:  "scan_review",
	}
	manifest := bindReviewSliceContext(t, reviewSlice)
	if err := store.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}

	progressed, err := reconciler.progressLatestScanRun(ctx, scan)
	if err != nil {
		t.Fatalf("progressLatestScanRun() error = %v", err)
	}
	if !progressed {
		t.Fatal("progressLatestScanRun() = false, want true")
	}

	var reviewTasks corev1alpha1.TaskList
	if err := cl.List(ctx, &reviewTasks,
		client.InNamespace(defaultNS),
		client.MatchingLabels(map[string]string{
			labels.LabelSecurityTarget:  "kaset",
			labels.LabelSecurityScanID:  "scan_review",
			labels.LabelSecurityStage:   security.StageReview,
			labels.LabelSecuritySliceID: "slice_api",
		}),
	); err != nil {
		t.Fatalf("List(review tasks) error = %v", err)
	}
	if len(reviewTasks.Items) != 1 {
		t.Fatalf("len(review tasks) = %d, want 1", len(reviewTasks.Items))
	}
	if !strings.Contains(reviewTasks.Items[0].Spec.Prompt, manifest.Prompt) ||
		!strings.Contains(reviewTasks.Items[0].Spec.Prompt, security.AgentResultKindFindings) {
		t.Fatalf("review prompt does not contain trusted context and terminal result contract: %q", reviewTasks.Items[0].Spec.Prompt)
	}
	if len(reviewTasks.Items[0].Spec.Env) != 0 {
		t.Fatalf("review task env = %#v, want empty harness-v2 env", reviewTasks.Items[0].Spec.Env)
	}

}

func TestProgressLatestScanRunFailsMapperArtifactValidationProblem(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

	scan := &corev1alpha1.RepositoryScan{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "RepositoryScan",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          "https://github.com/sozercan/kaset",
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"},
		},
		Status: corev1alpha1.RepositoryScanStatus{LastScanID: "scan_mapper_failed"},
	}
	completed := metav1.Now()
	threatTask := newSucceededSecurityTask("kaset-initial-threat", "scan_mapper_failed", security.StageThreatModel, completed)
	mapperTask := newSucceededSecurityTask("kaset-initial-mapper", "scan_mapper_failed", security.StageMapper, completed)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithObjects(scan, threatTask, mapperTask).
		Build()
	reconciler := &RepositoryScanReconciler{
		Client:        cl,
		Scheme:        scheme,
		SecurityStore: store,
	}
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{
		ID:             "scan_mapper_failed",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		TaskName:       threatTask.Name,
		Mode:           "initial",
		Phase:          scanRunPhaseRunning,
		ErrorMessage:   "mapper stage failed: security-slices.json is missing",
		StartedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}

	progressed, err := reconciler.progressLatestScanRun(ctx, scan)
	if err != nil {
		t.Fatalf("progressLatestScanRun() error = %v", err)
	}
	if !progressed {
		t.Fatal("progressLatestScanRun() = false, want true")
	}

	run, err := store.GetScanRun(ctx, defaultNS, "scan_mapper_failed")
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.Phase != scanRunPhaseFailed {
		t.Fatalf("run.Phase = %q, want failed", run.Phase)
	}
	if !strings.Contains(run.Summary, security.ArtifactSlices) {
		t.Fatalf("run.Summary = %q, want mapper artifact failure", run.Summary)
	}

	current := &corev1alpha1.RepositoryScan{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
		t.Fatalf("Get(scan) error = %v", err)
	}
	if current.Status.Phase != repositoryScanPhaseError {
		t.Fatalf("scan status phase = %q, want Error", current.Status.Phase)
	}
	ready := meta.FindStatusCondition(current.Status.Conditions, "Ready")
	if ready == nil {
		t.Fatal("Ready condition missing")
	}
	if ready.Status != metav1.ConditionFalse || ready.Reason != readyReasonScanFailed {
		t.Fatalf("Ready condition = %#v, want failed condition", ready)
	}
}

func TestProgressLatestScanRunRetriesPendingSlicesWithoutTasks(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

	scan := &corev1alpha1.RepositoryScan{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "RepositoryScan",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          "https://github.com/sozercan/kaset",
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"},
		},
		Status: corev1alpha1.RepositoryScanStatus{LastScanID: "scan_partial_review"},
	}
	const sliceAPI = "slice_api"
	threatTask := newSucceededSecurityTask("kaset-partial-threat", "scan_partial_review", security.StageThreatModel, metav1.Now())
	threatTask.Spec.Workspace = repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead)
	mapperTask := newSucceededSecurityTask("kaset-partial-mapper", "scan_partial_review", security.StageMapper, metav1.Now())
	reviewTask := newSucceededSecurityTask("kaset-review-slice-api", "scan_partial_review", security.StageReview, metav1.Now())
	reviewTask.Labels[labels.LabelSecuritySliceID] = sliceAPI
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithObjects(scan, threatTask, mapperTask, reviewTask).
		Build()
	reconciler := &RepositoryScanReconciler{
		Client:        cl,
		Scheme:        scheme,
		SecurityStore: store,
	}
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{
		ID:             "scan_partial_review",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		TaskName:       threatTask.Name,
		Mode:           "initial",
		Phase:          scanRunPhaseRunning,
		SliceCount:     2,
		StartedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	for _, slice := range []storepkg.ReviewSlice{
		{
			SchemaVersion:  1,
			ID:             sliceAPI,
			Namespace:      defaultNS,
			RepositoryScan: "kaset",
			Source:         "deterministic-go-package",
			Title:          "Go package internal/api",
			Summary:        "Already reviewed.",
			Kind:           "package",
			OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "source"}},
			Confidence:     "high",
			Status:         reviewSliceStatusReviewed,
			LastScanRunID:  "scan_partial_review",
		},
		{
			SchemaVersion:  1,
			ID:             "slice_store",
			Namespace:      defaultNS,
			RepositoryScan: "kaset",
			Source:         "deterministic-go-package",
			Title:          "Go package internal/store",
			Summary:        "Task creation was interrupted before this slice started.",
			Kind:           "package",
			OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/store/store.go", Reason: "source"}},
			Confidence:     "high",
			Status:         reviewSliceStatusPending,
			LastScanRunID:  "scan_partial_review",
		},
	} {
		bindReviewSliceContext(t, &slice)
		if err := store.UpsertReviewSlice(ctx, &slice); err != nil {
			t.Fatalf("UpsertReviewSlice(%s) error = %v", slice.ID, err)
		}
	}

	progressed, err := reconciler.progressLatestScanRun(ctx, scan)
	if err != nil {
		t.Fatalf("progressLatestScanRun() error = %v", err)
	}
	if !progressed {
		t.Fatal("progressLatestScanRun() = false, want true")
	}

	var reviewTasks corev1alpha1.TaskList
	if err := cl.List(ctx, &reviewTasks,
		client.InNamespace(defaultNS),
		client.MatchingLabels(map[string]string{
			labels.LabelSecurityTarget:  "kaset",
			labels.LabelSecurityScanID:  "scan_partial_review",
			labels.LabelSecurityStage:   security.StageReview,
			labels.LabelSecuritySliceID: "slice_store",
		}),
	); err != nil {
		t.Fatalf("List(review tasks) error = %v", err)
	}
	if len(reviewTasks.Items) != 1 {
		t.Fatalf("len(review tasks) = %d, want retry task for missing slice", len(reviewTasks.Items))
	}
	run, err := store.GetScanRun(ctx, defaultNS, "scan_partial_review")
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.Phase != scanRunPhaseRunning || !strings.Contains(run.Summary, "retrying 1 pending review slices") {
		t.Fatalf("run phase/summary = %q/%q, want running retry summary", run.Phase, run.Summary)
	}
}

func TestPendingReviewSlicesPaginatesAllPendingSlices(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: store}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
	}

	const totalSlices = 1005
	for i := range totalSlices {
		if err := store.UpsertReviewSlice(ctx, &storepkg.ReviewSlice{
			SchemaVersion:  1,
			ID:             fmt.Sprintf("slice_bulk_%04d", i),
			Namespace:      defaultNS,
			RepositoryScan: "kaset",
			Source:         "deterministic-generic",
			Title:          fmt.Sprintf("Bulk slice %04d", i),
			Summary:        "Bulk pending slice.",
			Kind:           "unknown",
			OwnedFiles:     []storepkg.ReviewSliceFile{{Path: fmt.Sprintf("src/file_%04d.go", i), Reason: "source"}},
			Confidence:     "medium",
			Status:         reviewSliceStatusPending,
			LastScanRunID:  "scan_review",
		}); err != nil {
			t.Fatalf("UpsertReviewSlice(%d) error = %v", i, err)
		}
	}

	if err := store.UpsertReviewSlice(ctx, &storepkg.ReviewSlice{
		SchemaVersion:  1,
		ID:             "slice_stale",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		Source:         "deterministic-generic",
		Title:          "Stale slice",
		Summary:        "Pending from another run.",
		Kind:           "unknown",
		OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "src/stale.go", Reason: "source"}},
		Confidence:     "medium",
		Status:         reviewSliceStatusPending,
		LastScanRunID:  "scan_stale",
	}); err != nil {
		t.Fatalf("UpsertReviewSlice(stale) error = %v", err)
	}

	got, err := reconciler.pendingReviewSlices(ctx, scan, "scan_review")
	if err != nil {
		t.Fatalf("pendingReviewSlices() error = %v", err)
	}
	if len(got) != totalSlices {
		t.Fatalf("len(pendingReviewSlices) = %d, want %d", len(got), totalSlices)
	}
}

func TestProgressLatestScanRunCompletesNoopIncrementalWhenNoSlicesMatch(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

	scan := &corev1alpha1.RepositoryScan{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "RepositoryScan",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          "https://github.com/sozercan/kaset",
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"},
		},
		Status: corev1alpha1.RepositoryScanStatus{LastScanID: "scan_noop_incremental"},
	}
	threatTask := newSucceededSecurityTask("kaset-incremental-threat", "scan_noop_incremental", security.StageThreatModel, metav1.Now())
	mapperTask := newSucceededSecurityTask("kaset-incremental-mapper", "scan_noop_incremental", security.StageMapper, metav1.Now())
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithObjects(scan, threatTask, mapperTask).
		Build()
	reconciler := &RepositoryScanReconciler{
		Client:        cl,
		Scheme:        scheme,
		SecurityStore: store,
	}
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{
		ID:                "scan_noop_incremental",
		Namespace:         defaultNS,
		RepositoryScan:    "kaset",
		TaskName:          threatTask.Name,
		Mode:              "incremental",
		Phase:             scanRunPhaseRunning,
		BaseCommit:        "base123",
		HeadCommit:        "head456",
		SliceCount:        2,
		SkippedSliceCount: 2,
		Summary:           "Threat model generated; no review slices matched 1 changed files",
		StartedAt:         time.Now(),
	}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	for _, id := range []string{"slice_api", "slice_store"} {
		if err := store.UpsertReviewSlice(ctx, &storepkg.ReviewSlice{
			SchemaVersion:  1,
			ID:             id,
			Namespace:      defaultNS,
			RepositoryScan: "kaset",
			Source:         "deterministic-go-package",
			Title:          id,
			Summary:        "No changed files matched this slice.",
			Kind:           "package",
			OwnedFiles:     []storepkg.ReviewSliceFile{{Path: id + ".go", Reason: "source"}},
			Confidence:     "high",
			Status:         reviewSliceStatusSkipped,
			LastScanRunID:  "scan_noop_incremental",
		}); err != nil {
			t.Fatalf("UpsertReviewSlice(%s) error = %v", id, err)
		}
	}

	progressed, err := reconciler.progressLatestScanRun(ctx, scan)
	if err != nil {
		t.Fatalf("progressLatestScanRun() error = %v", err)
	}
	if !progressed {
		t.Fatal("progressLatestScanRun() = false, want true")
	}

	run, err := store.GetScanRun(ctx, defaultNS, "scan_noop_incremental")
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.Phase != scanRunPhaseSucceeded || run.CompletedAt == nil {
		t.Fatalf("run phase/completedAt = %q/%v, want succeeded with completion", run.Phase, run.CompletedAt)
	}
	if run.Summary != "Threat model generated; no review slices matched 1 changed files" {
		t.Fatalf("run.Summary = %q, want mapper no-op summary", run.Summary)
	}
	current := &corev1alpha1.RepositoryScan{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
		t.Fatalf("Get(scan) error = %v", err)
	}
	if current.Status.Phase != repositoryScanPhaseReady || current.Status.LastProcessedCommit != "head456" {
		t.Fatalf("scan status phase/processed = %q/%q, want Ready/head456", current.Status.Phase, current.Status.LastProcessedCommit)
	}
	ready := meta.FindStatusCondition(current.Status.Conditions, "Ready")
	if ready == nil {
		t.Fatal("Ready condition missing")
	}
	if strings.Contains(ready.Message, "pending") {
		t.Fatalf("Ready condition message = %q, want completed no-op summary", ready.Message)
	}
}

func TestRefreshScanRunStatusKeepsReviewRunRunningWithPendingSlices(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

	scan := &corev1alpha1.RepositoryScan{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "RepositoryScan",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          "https://github.com/sozercan/kaset",
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"},
		},
		Status: corev1alpha1.RepositoryScanStatus{LastScanID: "scan_review_incomplete"},
	}
	completed := metav1.Now()
	threatTask := newSucceededSecurityTask("kaset-incomplete-threat", "scan_review_incomplete", security.StageThreatModel, completed)
	mapperTask := newSucceededSecurityTask("kaset-incomplete-mapper", "scan_review_incomplete", security.StageMapper, completed)
	reviewTask := newSucceededSecurityTask("kaset-incomplete-review-api", "scan_review_incomplete", security.StageReview, completed)
	reviewTask.Labels[labels.LabelSecuritySliceID] = "slice_api"
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithObjects(scan, threatTask, mapperTask, reviewTask).
		Build()
	reconciler := &RepositoryScanReconciler{
		Client:        cl,
		Scheme:        scheme,
		SecurityStore: store,
	}
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{
		ID:             "scan_review_incomplete",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		TaskName:       threatTask.Name,
		Mode:           "initial",
		Phase:          scanRunPhaseRunning,
		SliceCount:     2,
		HeadCommit:     "head456",
		StartedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	for _, slice := range []storepkg.ReviewSlice{
		{
			SchemaVersion:  1,
			ID:             "slice_api",
			Namespace:      defaultNS,
			RepositoryScan: "kaset",
			Source:         "deterministic-go-package",
			Title:          "Go package internal/api",
			Summary:        "Already reviewed.",
			Kind:           "package",
			OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "source"}},
			Confidence:     "high",
			Status:         reviewSliceStatusReviewed,
			LastScanRunID:  "scan_review_incomplete",
		},
		{
			SchemaVersion:  1,
			ID:             "slice_store",
			Namespace:      defaultNS,
			RepositoryScan: "kaset",
			Source:         "deterministic-go-package",
			Title:          "Go package internal/store",
			Summary:        "Still pending.",
			Kind:           "package",
			OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/store/store.go", Reason: "source"}},
			Confidence:     "high",
			Status:         reviewSliceStatusPending,
			LastScanRunID:  "scan_review_incomplete",
		},
	} {
		if err := store.UpsertReviewSlice(ctx, &slice); err != nil {
			t.Fatalf("UpsertReviewSlice(%s) error = %v", slice.ID, err)
		}
	}

	run, err := store.GetScanRun(ctx, defaultNS, "scan_review_incomplete")
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if err := reconciler.refreshScanRunStatus(ctx, scan, run, "scan_review_incomplete", true); err != nil {
		t.Fatalf("refreshScanRunStatus() error = %v", err)
	}

	run, err = store.GetScanRun(ctx, defaultNS, "scan_review_incomplete")
	if err != nil {
		t.Fatalf("GetScanRun() after refresh error = %v", err)
	}
	if run.Phase != scanRunPhaseRunning || run.CompletedAt != nil {
		t.Fatalf("run phase/completedAt = %q/%v, want running without completion", run.Phase, run.CompletedAt)
	}
	if !strings.Contains(run.Summary, "1 review slices remain pending") {
		t.Fatalf("run.Summary = %q, want pending slice summary", run.Summary)
	}
	current := &corev1alpha1.RepositoryScan{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
		t.Fatalf("Get(scan) error = %v", err)
	}
	if current.Status.Phase != repositoryScanPhaseScanning || current.Status.LastProcessedCommit != "" {
		t.Fatalf("scan status phase/processed = %q/%q, want Scanning with no processed commit", current.Status.Phase, current.Status.LastProcessedCommit)
	}
}

func TestIngestReviewTaskRejectsMismatchedV2SliceID(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{
		SecurityStore: store,
		ResultStore:   store,
	}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec:       corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo"},
	}
	policyDigest := security.ScannerPolicyDigest(security.ScannerPolicy{})
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{
		ID:             "scan_mismatched_slice",
		Namespace:      defaultNS,
		RepositoryScan: scan.Name,
		TaskName:       "kaset-review-mismatched-slice",
		Mode:           "initial",
		Phase:          scanRunPhaseRunning,
		PolicyDigest:   policyDigest,
		StartedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	reviewSlice := &storepkg.ReviewSlice{
		SchemaVersion:  1,
		ID:             "slice_api",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		Source:         "deterministic-go-package",
		Title:          "Go package internal/api",
		Summary:        "API handlers",
		Kind:           "package",
		OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "source"}},
		Confidence:     "high",
		Status:         reviewSliceStatusPending,
		LastScanRunID:  "scan_mismatched_slice",
	}
	bindReviewSliceContext(t, reviewSlice)
	if err := store.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kaset-review-mismatched-slice",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget:  "kaset",
				labels.LabelSecurityScanID:  "scan_mismatched_slice",
				labels.LabelSecurityMode:    "initial",
				labels.LabelSecurityStage:   security.StageReview,
				labels.LabelSecuritySliceID: "slice_api",
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	findings := security.FindingsV2Artifact{
		SchemaVersion: security.SchemaVersionFindingsV2,
		Repository: security.FindingsV2Repository{
			RepoURL: "https://github.com/example/repo",
			Branch:  "main",
		},
		Scan: security.FindingsV2Scan{
			Mode:    "initial",
			SliceID: "slice_other",
			Summary: "mismatched context",
		},
		Findings: []security.FindingsV2Finding{},
	}
	saveFindingsTaskResult(t, store, task, scan.Name, "scan_mismatched_slice", policyDigest, reviewSlice.ReviewContextHash, "slice_other", findings)

	if err := reconciler.ingestScanTask(ctx, scan, task); err != nil {
		t.Fatalf("ingestScanTask() error = %v", err)
	}

	run, err := store.GetScanRun(ctx, defaultNS, "scan_mismatched_slice")
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.Phase != scanRunPhaseFailed || !strings.Contains(run.ErrorMessage, "does not match task slice") {
		t.Fatalf("run phase/error = %q/%q, want failed slice mismatch", run.Phase, run.ErrorMessage)
	}
	storedReviewSlice, err := store.GetReviewSlice(ctx, defaultNS, "kaset", "slice_api")
	if err != nil {
		t.Fatalf("GetReviewSlice() error = %v", err)
	}
	if storedReviewSlice.Status != reviewSliceStatusFailed {
		t.Fatalf("review slice status = %q, want failed", storedReviewSlice.Status)
	}
	listed, _, err := store.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: "kaset"})
	if err != nil {
		t.Fatalf("ListFindings() error = %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("len(findings) = %d, want no accepted findings for mismatched slice", len(listed))
	}
}

func TestIngestReviewTaskPartitionsV2FindingsAndMarksSliceReviewed(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{
		SecurityStore: store,
		ArtifactStore: store,
		ResultStore:   store,
	}
	maxFindings := int32(1)
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:           "https://github.com/example/repo",
			MaxFindingsPerRun: &maxFindings,
		},
	}
	policyDigest := security.ScannerPolicyDigest(security.ScannerPolicy{})
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{
		ID:             "scan_review_ingest",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		TaskName:       "kaset-review-slice",
		Mode:           "initial",
		Phase:          scanRunPhaseRunning,
		BaseCommit:     "trusted-base",
		HeadCommit:     "trusted-head",
		PolicyDigest:   policyDigest,
		StartedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	reviewSlice := &storepkg.ReviewSlice{
		SchemaVersion:  1,
		ID:             "slice_api",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		Source:         "deterministic-go-package",
		Title:          "Go package internal/api",
		Summary:        "API handlers",
		Kind:           "package",
		OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "source"}},
		Confidence:     "high",
		Status:         reviewSliceStatusPending,
		LastScanRunID:  "scan_review_ingest",
	}
	bindReviewSliceContext(t, reviewSlice)
	if err := store.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kaset-review-slice",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget:  "kaset",
				labels.LabelSecurityScanID:  "scan_review_ingest",
				labels.LabelSecurityMode:    "initial",
				labels.LabelSecurityStage:   security.StageReview,
				labels.LabelSecuritySliceID: "slice_api",
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	findings := security.FindingsV2Artifact{
		SchemaVersion: security.SchemaVersionFindingsV2,
		Repository: security.FindingsV2Repository{
			RepoURL: "https://github.com/example/repo",
			Branch:  "main",
			BaseSHA: "trusted-base",
			HeadSHA: "trusted-head",
		},
		Scan: security.FindingsV2Scan{Mode: "initial", SliceID: "slice_api", Summary: "one accepted, two dropped"},
		Findings: []security.FindingsV2Finding{
			{
				Title:       "Unsafe API behavior",
				Category:    "authz",
				Severity:    "high",
				Confidence:  "high",
				Summary:     "API path lacks authorization.",
				Remediation: "Add authorization checks.",
				Evidence: []security.FindingsV2EvidenceRef{{
					Path:      "internal/api/security.go",
					StartLine: 5,
					EndLine:   8,
				}},
			},
			{
				Title:       "Unsafe API audit bypass",
				Category:    "authz",
				Severity:    "medium",
				Confidence:  "high",
				Summary:     "A second valid API issue exceeds the configured run cap.",
				Remediation: "Add authorization checks.",
				Evidence: []security.FindingsV2EvidenceRef{{
					Path:      "internal/api/security.go",
					StartLine: 9,
					EndLine:   12,
				}},
			},
			{
				Title:       "Speculative issue",
				Category:    "authz",
				Severity:    "high",
				Confidence:  "low",
				Summary:     "Cites an omitted file.",
				Remediation: "Fix it.",
				Evidence: []security.FindingsV2EvidenceRef{{
					Path:      "internal/api/omitted.go",
					StartLine: 1,
					EndLine:   1,
				}},
			},
		},
	}
	saveFindingsTaskResult(t, store, task, scan.Name, "scan_review_ingest", policyDigest, reviewSlice.ReviewContextHash, "slice_api", findings)

	if err := reconciler.ingestScanTask(ctx, scan, task); err != nil {
		t.Fatalf("ingestScanTask() error = %v", err)
	}
	if err := reconciler.ingestScanTask(ctx, scan, task); err != nil {
		t.Fatalf("second ingestScanTask() error = %v", err)
	}
	run, err := store.GetScanRun(ctx, defaultNS, "scan_review_ingest")
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.ReviewedSliceCount != 1 || run.AcceptedFindings != 1 || run.DroppedFindings != 2 {
		t.Fatalf("run counts = reviewed:%d accepted:%d dropped:%d, want 1/1/2", run.ReviewedSliceCount, run.AcceptedFindings, run.DroppedFindings)
	}
	if run.BaseCommit != "trusted-base" || run.HeadCommit != "trusted-head" {
		t.Fatalf("run commits = %q/%q, want trusted-base/trusted-head", run.BaseCommit, run.HeadCommit)
	}
	if run.Mode != "initial" {
		t.Fatalf("run mode = %q, want trusted initial mode", run.Mode)
	}
	storedReviewSlice, err := store.GetReviewSlice(ctx, defaultNS, "kaset", "slice_api")
	if err != nil {
		t.Fatalf("GetReviewSlice() error = %v", err)
	}
	if storedReviewSlice.Status != reviewSliceStatusReviewed || storedReviewSlice.LastReviewedAt == nil {
		t.Fatalf("review slice status = %q lastReviewedAt=%v, want reviewed with timestamp", storedReviewSlice.Status, storedReviewSlice.LastReviewedAt)
	}
	listed, _, err := store.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: "kaset", SliceID: "slice_api"})
	if err != nil {
		t.Fatalf("ListFindings() error = %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("len(findings) = %d, want one accepted review finding", len(listed))
	}
	if listed[0].CommitSHA != "trusted-head" {
		t.Fatalf("finding.CommitSHA = %q, want trusted run head", listed[0].CommitSHA)
	}
	dropped, _, err := store.ListDroppedFindings(ctx, storepkg.DroppedFindingFilter{Namespace: defaultNS, RepositoryScan: "kaset", ScanRunID: "scan_review_ingest", SliceID: "slice_api"})
	if err != nil {
		t.Fatalf("ListDroppedFindings() error = %v", err)
	}
	if len(dropped) != 2 {
		t.Fatalf("len(dropped) = %d, want two diagnostics", len(dropped))
	}
	sawCapDrop := false
	for _, item := range dropped {
		if strings.Contains(item.Reason, "maxFindingsPerRun limit 1 reached") {
			sawCapDrop = true
		}
	}
	if !sawCapDrop {
		t.Fatalf("dropped diagnostics = %#v, want maxFindingsPerRun cap diagnostic", dropped)
	}
}

func TestIngestReviewTaskPersistsFilterDroppedDiagnosticsBeforeCap(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: store, ArtifactStore: store, ResultStore: store}
	maxFindings := int32(1)
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}, Spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo", MaxFindingsPerRun: &maxFindings}}
	policyDigest := security.ScannerPolicyDigest(security.ScannerPolicy{})
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{ID: "scan_review_filter", Namespace: defaultNS, RepositoryScan: "kaset", TaskName: "kaset-review-filter", Mode: "initial", Phase: scanRunPhaseRunning, HeadCommit: "trusted-head", PolicyDigest: policyDigest, StartedAt: time.Now()}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	reviewSlice := &storepkg.ReviewSlice{SchemaVersion: 1, ID: "slice_filter", Namespace: defaultNS, RepositoryScan: "kaset", Source: "deterministic", Title: "Filter slice", Kind: "package", OwnedFiles: []storepkg.ReviewSliceFile{{Path: "docs/security.md"}, {Path: "internal/api/security.go"}}, Confidence: "high", Status: reviewSliceStatusPending, LastScanRunID: "scan_review_filter"}
	bindReviewSliceContext(t, reviewSlice)
	if err := store.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "kaset-review-filter", Namespace: defaultNS, Labels: map[string]string{labels.LabelSecurityTarget: "kaset", labels.LabelSecurityScanID: "scan_review_filter", labels.LabelSecurityMode: "initial", labels.LabelSecurityStage: security.StageReview, labels.LabelSecuritySliceID: "slice_filter"}}, Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded}}
	findings := security.FindingsV2Artifact{SchemaVersion: security.SchemaVersionFindingsV2, Repository: security.FindingsV2Repository{RepoURL: "https://github.com/example/repo", Branch: "main", HeadSHA: "trusted-head"}, Scan: security.FindingsV2Scan{Mode: "initial", SliceID: "slice_filter", Summary: "filter then cap"}, Findings: []security.FindingsV2Finding{
		{Title: "Docs-only rate limit", Category: "rate-limit", Severity: "medium", Confidence: "high", Summary: "Documentation says rate limiting is missing.", Remediation: "Document it.", Evidence: []security.FindingsV2EvidenceRef{{Path: "docs/security.md", StartLine: 1, EndLine: 1}}},
		{Title: "Unsafe API behavior", Category: "authz", Severity: "high", Confidence: "high", Summary: "Attacker-controlled request crosses auth trust boundary.", Remediation: "Add server-side authorization.", Evidence: []security.FindingsV2EvidenceRef{{Path: "internal/api/security.go", StartLine: 2, EndLine: 3}}},
		{Title: "Unsafe API audit bypass", Category: "authz", Severity: "medium", Confidence: "high", Summary: "Second concrete tenant authorization bypass.", Remediation: "Add server-side authorization.", Evidence: []security.FindingsV2EvidenceRef{{Path: "internal/api/security.go", StartLine: 4, EndLine: 5}}},
	}}
	saveFindingsTaskResult(t, store, task, scan.Name, "scan_review_filter", policyDigest, reviewSlice.ReviewContextHash, "slice_filter", findings)

	if err := reconciler.ingestScanTask(ctx, scan, task); err != nil {
		t.Fatalf("ingestScanTask() error = %v", err)
	}
	listed, _, err := store.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: "kaset", SliceID: "slice_filter"})
	if err != nil {
		t.Fatalf("ListFindings() error = %v", err)
	}
	if len(listed) != 1 || listed[0].Title != "Unsafe API behavior" {
		t.Fatalf("findings = %#v, want first concrete finding only", listed)
	}
	dropped, _, err := store.ListDroppedFindings(ctx, storepkg.DroppedFindingFilter{Namespace: defaultNS, RepositoryScan: "kaset", ScanRunID: "scan_review_filter", SliceID: "slice_filter"})
	if err != nil {
		t.Fatalf("ListDroppedFindings() error = %v", err)
	}
	var sawFilter, sawCap bool
	for _, item := range dropped {
		if item.Layer == "filter" {
			sawFilter = true
		}
		if item.Layer == "cap" {
			sawCap = true
		}
	}
	if len(dropped) != 2 || !sawFilter || !sawCap {
		t.Fatalf("dropped = %#v, want filter and cap diagnostics", dropped)
	}
}

func TestIngestReviewTaskChecksPolicyDriftBeforeFilteringFindings(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:                   "https://github.com/example/repo",
			AnalysisAgentRef:          corev1alpha1.AgentReference{Name: "scan-reviewer"},
			CustomScanInstructionsRef: &corev1alpha1.PolicyConfigMapKeyRef{Name: "scan-policy"},
		},
	}
	policyConfig := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "scan-policy", Namespace: defaultNS, Labels: map[string]string{security.PolicyConfigMapAllowedLabel: "true"}}, Data: map[string]string{"policy": "changed policy"}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(scan, policyConfig).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: store, ArtifactStore: store, ResultStore: store}
	run := &storepkg.ScanRun{ID: "scan_review_drift", Namespace: defaultNS, RepositoryScan: "kaset", TaskName: "kaset-review-drift", Mode: "initial", Phase: scanRunPhaseRunning, PolicyDigest: "sha256:old", StartedAt: time.Now()}
	if err := store.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	reviewSlice := &storepkg.ReviewSlice{SchemaVersion: 1, ID: "slice_docs", Namespace: defaultNS, RepositoryScan: "kaset", Source: "deterministic", Title: "Docs", Kind: "package", OwnedFiles: []storepkg.ReviewSliceFile{{Path: "docs/security.md"}}, Confidence: "high", Status: reviewSliceStatusPending, LastScanRunID: run.ID}
	bindReviewSliceContext(t, reviewSlice)
	if err := store.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "kaset-review-drift", Namespace: defaultNS, Labels: map[string]string{labels.LabelSecurityTarget: "kaset", labels.LabelSecurityScanID: run.ID, labels.LabelSecurityMode: "initial", labels.LabelSecurityStage: security.StageReview, labels.LabelSecuritySliceID: "slice_docs"}}, Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded}}
	findings := security.FindingsV2Artifact{SchemaVersion: security.SchemaVersionFindingsV2, Repository: security.FindingsV2Repository{RepoURL: "https://github.com/example/repo", Branch: "main"}, Scan: security.FindingsV2Scan{Mode: "initial", SliceID: "slice_docs", Summary: "docs only"}, Findings: []security.FindingsV2Finding{{Title: "Docs-only rate limit", Category: "rate-limit", Severity: "medium", Confidence: "high", Summary: "Documentation says rate limiting is missing.", Remediation: "Document it.", Evidence: []security.FindingsV2EvidenceRef{{Path: "docs/security.md", StartLine: 1, EndLine: 1}}}}}
	saveFindingsTaskResult(t, store, task, scan.Name, run.ID, run.PolicyDigest, reviewSlice.ReviewContextHash, reviewSlice.ID, findings)

	err := reconciler.ingestScanTask(ctx, scan, task)
	if err == nil || !strings.Contains(err.Error(), "scanner policy digest changed") {
		t.Fatalf("ingestScanTask() error = %v, want policy drift", err)
	}
	storedRun, err := store.GetScanRun(ctx, defaultNS, run.ID)
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if storedRun.Phase != scanRunPhaseFailed {
		t.Fatalf("run phase = %q, want failed", storedRun.Phase)
	}
	storedReviewSlice, err := store.GetReviewSlice(ctx, defaultNS, "kaset", "slice_docs")
	if err != nil {
		t.Fatalf("GetReviewSlice() error = %v", err)
	}
	if storedReviewSlice.Status == reviewSliceStatusReviewed {
		t.Fatal("review slice was marked reviewed despite policy drift")
	}
}

func TestIngestReviewTaskSkipsStaleSliceRun(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{
		SecurityStore: store,
		ArtifactStore: store,
	}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
	}
	oldCompletedAt := time.Now().Add(-30 * time.Minute)
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{
		ID:             "scan_old_review",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		TaskName:       "kaset-review-slice-old",
		Mode:           "initial",
		Phase:          scanRunPhaseSucceeded,
		BaseCommit:     "old-base",
		HeadCommit:     "old-head",
		StartedAt:      time.Now().Add(-1 * time.Hour),
		CompletedAt:    &oldCompletedAt,
	}); err != nil {
		t.Fatalf("CreateScanRun(old) error = %v", err)
	}
	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{
		ID:             "scan_new_review",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		Mode:           scanModeIncremental,
		Phase:          scanRunPhaseRunning,
		BaseCommit:     "new-base",
		HeadCommit:     "new-head",
		StartedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("CreateScanRun(new) error = %v", err)
	}
	if err := store.UpsertReviewSlice(ctx, &storepkg.ReviewSlice{
		SchemaVersion:  1,
		ID:             "slice_api",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		Source:         "deterministic-go-package",
		Title:          "Go package internal/api",
		Summary:        "API handlers",
		Kind:           "package",
		OwnedFiles:     []storepkg.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "source"}},
		Confidence:     "high",
		Status:         reviewSliceStatusPending,
		LastScanRunID:  "scan_new_review",
	}); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}

	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kaset-review-slice-old",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget:  "kaset",
				labels.LabelSecurityScanID:  "scan_old_review",
				labels.LabelSecurityMode:    "initial",
				labels.LabelSecurityStage:   security.StageReview,
				labels.LabelSecuritySliceID: "slice_api",
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	manifest := security.ReviewContextManifest{
		SchemaVersion: security.SchemaVersionReviewContext,
		SliceID:       "slice_api",
		IncludedFiles: []security.ReviewContextIncludedFile{{
			Path:               "internal/api/security.go",
			Role:               "owned",
			IncludedLineRanges: []security.ReviewContextLineRange{{StartLine: 1, EndLine: 20}},
			Readable:           true,
		}},
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("json.Marshal(manifest) error = %v", err)
	}
	if err := store.SaveArtifact(ctx, task.Namespace, task.Name, security.ReviewContextArtifactName("slice_api"), "application/json", manifestData); err != nil {
		t.Fatalf("SaveArtifact(manifest) error = %v", err)
	}
	findings := security.FindingsV2Artifact{
		SchemaVersion: security.SchemaVersionFindingsV2,
		Repository: security.FindingsV2Repository{
			RepoURL: "https://github.com/example/repo",
			Branch:  "main",
		},
		Scan: security.FindingsV2Scan{Mode: "initial", SliceID: "slice_api", Summary: "stale review output"},
		Findings: []security.FindingsV2Finding{{
			Title:       "Unsafe API behavior",
			Category:    "authz",
			Severity:    "high",
			Confidence:  "high",
			Summary:     "API path lacks authorization.",
			Remediation: "Add authorization checks.",
			Evidence: []security.FindingsV2EvidenceRef{{
				Path:      "internal/api/security.go",
				StartLine: 5,
				EndLine:   8,
			}},
		}},
	}
	findingsData, err := json.Marshal(findings)
	if err != nil {
		t.Fatalf("json.Marshal(findings) error = %v", err)
	}
	if err := store.SaveArtifact(ctx, task.Namespace, task.Name, security.ArtifactFindingsV2, "application/json", findingsData); err != nil {
		t.Fatalf("SaveArtifact(findings v2) error = %v", err)
	}

	if err := reconciler.ingestScanTask(ctx, scan, task); err != nil {
		t.Fatalf("ingestScanTask() error = %v", err)
	}
	run, err := store.GetScanRun(ctx, defaultNS, "scan_old_review")
	if err != nil {
		t.Fatalf("GetScanRun(old) error = %v", err)
	}
	if run.ReviewedSliceCount != 0 || run.AcceptedFindings != 0 || run.DroppedFindings != 0 {
		t.Fatalf("old run counts = reviewed:%d accepted:%d dropped:%d, want unchanged", run.ReviewedSliceCount, run.AcceptedFindings, run.DroppedFindings)
	}
	reviewSlice, err := store.GetReviewSlice(ctx, defaultNS, "kaset", "slice_api")
	if err != nil {
		t.Fatalf("GetReviewSlice() error = %v", err)
	}
	if reviewSlice.LastScanRunID != "scan_new_review" || reviewSlice.Status != reviewSliceStatusPending {
		t.Fatalf("review slice = %#v, want current run pending slice unchanged", reviewSlice)
	}
	listed, _, err := store.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: "kaset", SliceID: "slice_api"})
	if err != nil {
		t.Fatalf("ListFindings() error = %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("len(findings) = %d, want stale task findings ignored", len(listed))
	}
}

func TestPersistThreatModelIfChangedDeduplicatesRedactedResult(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: store}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
	}
	const scanID = "scan_current"
	data, err := json.Marshal(security.ThreatModelResultEnvelope{
		SchemaVersion: security.AgentResultSchemaVersion, Kind: security.AgentResultKindThreatModel,
		RepositoryScan: scan.Name, ScanID: scanID,
		ThreatModel: "# Threat model\n\nEnvironment assignment `API_KEY=\"" + strings.Repeat("a", 32) + "\"`.",
	})
	if err != nil {
		t.Fatal(err)
	}
	content, err := security.ParseThreatModelResult(data, security.AgentResultBinding{RepositoryScan: scan.Name, ScanID: scanID})
	if err != nil {
		t.Fatalf("ParseThreatModelResult() error = %v", err)
	}
	for range 2 {
		if err := reconciler.persistThreatModelIfChanged(ctx, scan, scanID, time.Time{}, content); err != nil {
			t.Fatalf("persistThreatModelIfChanged() error = %v", err)
		}
	}
	latest, err := store.GetLatestThreatModel(ctx, scan.Namespace, scan.Name)
	if err != nil {
		t.Fatalf("GetLatestThreatModel() error = %v", err)
	}
	if latest.Version != 1 || latest.Content != content {
		t.Fatal("identical sanitized threat-model results were not deduplicated")
	}
}

func TestPersistThreatModelIfChangedSkipsOlderGeneratedRun(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)

	reconciler := &RepositoryScanReconciler{SecurityStore: store}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
	}

	if err := store.SaveThreatModel(ctx, &storepkg.ThreatModel{
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		Content:        "# Clean Threat Model\n\nCurated content",
		Source:         "cleaned",
	}); err != nil {
		t.Fatalf("SaveThreatModel() error = %v", err)
	}

	latest, err := store.GetLatestThreatModel(ctx, defaultNS, "kaset")
	if err != nil {
		t.Fatalf("GetLatestThreatModel() error = %v", err)
	}

	if err := reconciler.persistThreatModelIfChanged(
		ctx,
		scan,
		"scan_old",
		latest.UpdatedAt.Add(-time.Minute),
		"# Generated Threat Model\n\nOlder scan output",
	); err != nil {
		t.Fatalf("persistThreatModelIfChanged() error = %v", err)
	}

	latest, err = store.GetLatestThreatModel(ctx, defaultNS, "kaset")
	if err != nil {
		t.Fatalf("GetLatestThreatModel() error = %v", err)
	}
	if latest.Source != "cleaned" {
		t.Fatalf("latest.Source = %q, want cleaned", latest.Source)
	}
	if !strings.Contains(latest.Content, "Curated content") {
		t.Fatalf("latest.Content = %q, want cleaned threat model", latest.Content)
	}
}

func TestPersistThreatModelIfChangedPromotesNewerGeneratedRun(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)

	reconciler := &RepositoryScanReconciler{SecurityStore: store}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
	}

	if err := store.SaveThreatModel(ctx, &storepkg.ThreatModel{
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		Content:        "# Clean Threat Model\n\nCurated content",
		Source:         "cleaned",
	}); err != nil {
		t.Fatalf("SaveThreatModel() error = %v", err)
	}

	latest, err := store.GetLatestThreatModel(ctx, defaultNS, "kaset")
	if err != nil {
		t.Fatalf("GetLatestThreatModel() error = %v", err)
	}

	if err := reconciler.persistThreatModelIfChanged(
		ctx,
		scan,
		"scan_new",
		latest.UpdatedAt.Add(time.Minute),
		"# Generated Threat Model\n\nFresh scan output",
	); err != nil {
		t.Fatalf("persistThreatModelIfChanged() error = %v", err)
	}

	latest, err = store.GetLatestThreatModel(ctx, defaultNS, "kaset")
	if err != nil {
		t.Fatalf("GetLatestThreatModel() error = %v", err)
	}
	if latest.Source != "generated" {
		t.Fatalf("latest.Source = %q, want generated", latest.Source)
	}
	if latest.GeneratedByScan != "scan_new" {
		t.Fatalf("latest.GeneratedByScan = %q, want scan_new", latest.GeneratedByScan)
	}
	if !strings.Contains(latest.Content, "Fresh scan output") {
		t.Fatalf("latest.Content = %q, want new generated threat model", latest.Content)
	}
}

func TestIngestValidationTaskUpdatesFindingValidationDetails(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)

	reconciler := &RepositoryScanReconciler{
		SecurityStore: store,
		ArtifactStore: store,
		ResultStore:   store,
	}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
	}
	finding := &storepkg.Finding{
		ID:               "fnd_validate",
		Namespace:        defaultNS,
		RepositoryScan:   "kaset",
		ScanRunID:        "scan_validate",
		Fingerprint:      "sha256:test",
		Title:            "Validation target",
		Summary:          "candidate finding",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: "unvalidated",
		State:            findingStateOpen,
		Evidence: []storepkg.FindingEvidenceRef{{
			Kind:      "file",
			Path:      "internal/api/security.go",
			StartLine: 10,
			EndLine:   20,
		}},
	}
	if err := store.UpsertFinding(ctx, finding); err != nil {
		t.Fatalf("UpsertFinding() error = %v", err)
	}

	validation := security.ValidationArtifact{
		Version:            1,
		FindingID:          finding.ID,
		Status:             findingValidationStatusValidated,
		Summary:            "Confirmed injection path",
		ValidationSteps:    []string{"Trace input to shell execution", "Confirm shell metacharacters are preserved"},
		AttackPathAnalysis: "Attacker controls package names which reach shell execution.",
		Evidence: []storepkg.FindingEvidenceRef{
			{Kind: "file", Path: "internal/api/security.go", StartLine: 12, EndLine: 16, Label: "Confirmed sink path"},
		},
	}

	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kaset-validation-fnd_validate",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget:    "kaset",
				labels.LabelSecurityScanID:    finding.ScanRunID,
				labels.LabelSecurityFindingID: finding.ID,
				labels.LabelSecurityStage:     security.StageValidation,
				labels.LabelSecurityMode:      security.StageValidation,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	saveValidationTaskResult(t, store, task, scan.Name, finding.ScanRunID, security.ScannerPolicyDigest(security.ScannerPolicy{}), validation)

	if err := reconciler.ingestValidationTask(ctx, scan, task); err != nil {
		t.Fatalf("ingestValidationTask() error = %v", err)
	}

	updated, err := store.GetFinding(ctx, defaultNS, finding.ID)
	if err != nil {
		t.Fatalf("GetFinding() error = %v", err)
	}
	if updated.ValidationStatus != findingValidationStatusValidated {
		t.Fatalf("ValidationStatus = %q, want validated", updated.ValidationStatus)
	}
	if !strings.Contains(updated.ValidationJSON, "Confirmed injection path") {
		t.Fatalf("ValidationJSON = %q, want validation summary", updated.ValidationJSON)
	}
	if len(updated.Evidence) < 2 {
		t.Fatalf("len(Evidence) = %d, want at least 2 refs", len(updated.Evidence))
	}
	foundValidatedRange := false
	for _, ref := range updated.Evidence {
		if ref.Kind == "file" && ref.Path == "internal/api/security.go" && ref.StartLine == 12 && ref.EndLine == 16 && ref.TaskName == task.Name {
			foundValidatedRange = true
			break
		}
	}
	if !foundValidatedRange {
		t.Fatalf("updated.Evidence = %#v, want validated file range with task name", updated.Evidence)
	}
}

func TestIngestValidationTaskIgnoresPriorFindingOccurrence(t *testing.T) {
	for _, phase := range []corev1alpha1.TaskPhase{corev1alpha1.TaskPhaseSucceeded, corev1alpha1.TaskPhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			ctx := context.Background()
			securityStore := setupControllerSQLiteStore(t)
			reconciler := &RepositoryScanReconciler{SecurityStore: securityStore}
			scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
			finding := &storepkg.Finding{
				ID:               "fnd_reopened_validation",
				Namespace:        defaultNS,
				RepositoryScan:   scan.Name,
				ScanRunID:        "scan_current",
				Fingerprint:      "reopened-validation",
				Title:            "Reopened finding",
				Severity:         "high",
				Confidence:       "high",
				ValidationStatus: "unvalidated",
				State:            findingStateOpen,
			}
			if err := securityStore.UpsertFinding(ctx, finding); err != nil {
				t.Fatalf("UpsertFinding() error = %v", err)
			}
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kaset-validation-prior-occurrence",
					Namespace: defaultNS,
					Labels: map[string]string{
						labels.LabelSecurityScanID:    "scan_old",
						labels.LabelSecurityFindingID: finding.ID,
						labels.LabelSecurityStage:     security.StageValidation,
					},
				},
				Status: corev1alpha1.TaskStatus{Phase: phase},
			}

			if err := reconciler.ingestValidationTask(ctx, scan, task); err != nil {
				t.Fatalf("ingestValidationTask() error = %v", err)
			}
			stored, err := securityStore.GetFinding(ctx, defaultNS, finding.ID)
			if err != nil || stored.ValidationStatus != "unvalidated" || stored.ValidationJSON != "" {
				t.Fatalf("finding = %#v, err %v, want current occurrence unchanged", stored, err)
			}
		})
	}
}

func TestIngestValidationTaskRedirectsDuplicateResultToCanonicalFinding(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{
		SecurityStore: securityStore,
		ArtifactStore: securityStore,
		ResultStore:   securityStore,
	}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
	canonical := &storepkg.Finding{
		ID:               "fnd_validation_canonical",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_shared",
		Fingerprint:      "validation-canonical",
		Title:            "Canonical validation target",
		Summary:          "canonical candidate",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: "unvalidated",
		State:            findingStateOpen,
		FilePath:         "internal/api/security.go",
		Line:             10,
		Evidence:         []storepkg.FindingEvidenceRef{{Kind: "file", Path: "internal/api/security.go", StartLine: 10, EndLine: 20}},
	}
	alias := &storepkg.Finding{
		ID:               "fnd_validation_alias",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        canonical.ScanRunID,
		Fingerprint:      "validation-alias",
		Title:            "Aliased validation target",
		Summary:          "aliased candidate",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusPending,
		State:            findingStateOpen,
		DuplicateOf:      canonical.ID,
		FilePath:         canonical.FilePath,
		Line:             canonical.Line,
		Evidence:         []storepkg.FindingEvidenceRef{{Kind: "file", Path: canonical.FilePath, StartLine: 10, EndLine: 20}},
	}
	for _, finding := range []*storepkg.Finding{canonical, alias} {
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
		}
	}

	validation := security.ValidationArtifact{
		Version:            1,
		FindingID:          alias.ID,
		Status:             findingValidationStatusValidated,
		Summary:            "Confirmed injection path",
		ValidationSteps:    []string{"Trace input to shell execution"},
		AttackPathAnalysis: "Attacker-controlled input reaches shell execution.",
		Evidence:           []storepkg.FindingEvidenceRef{{Kind: "file", Path: alias.FilePath, StartLine: 12, EndLine: 16, Label: "Confirmed sink path"}},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kaset-validation-fnd_validation_alias",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget:    scan.Name,
				labels.LabelSecurityScanID:    alias.ScanRunID,
				labels.LabelSecurityFindingID: alias.ID,
				labels.LabelSecurityStage:     security.StageValidation,
				labels.LabelSecurityMode:      security.StageValidation,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	saveValidationTaskResult(t, securityStore, task, scan.Name, alias.ScanRunID, security.ScannerPolicyDigest(security.ScannerPolicy{}), validation)

	if err := reconciler.ingestValidationTask(ctx, scan, task); err != nil {
		t.Fatalf("ingestValidationTask() error = %v", err)
	}
	updatedCanonical, err := securityStore.GetFinding(ctx, defaultNS, canonical.ID)
	if err != nil {
		t.Fatalf("GetFinding(canonical) error = %v", err)
	}
	if updatedCanonical.ValidationStatus != findingValidationStatusValidated || !strings.Contains(updatedCanonical.ValidationJSON, canonical.ID) || strings.Contains(updatedCanonical.ValidationJSON, alias.ID) {
		t.Fatalf("canonical validation = status %q json %q", updatedCanonical.ValidationStatus, updatedCanonical.ValidationJSON)
	}
	foundTaskEvidence := false
	for _, ref := range updatedCanonical.Evidence {
		if ref.TaskName == task.Name && ref.StartLine == 12 && ref.EndLine == 16 {
			foundTaskEvidence = true
			break
		}
	}
	if !foundTaskEvidence {
		t.Fatalf("canonical evidence = %#v, want validation task evidence", updatedCanonical.Evidence)
	}
	updatedAlias, err := securityStore.GetFinding(ctx, defaultNS, alias.ID)
	if err != nil || updatedAlias.ValidationStatus != findingValidationStatusValidated || updatedAlias.DuplicateOf != canonical.ID {
		t.Fatalf("alias validation = %#v, err %v", updatedAlias, err)
	}
}

func TestIngestValidationTaskDoesNotRedirectPriorAliasOccurrenceToCanonical(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: securityStore}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
	canonical := &storepkg.Finding{
		ID:               "fnd_reopened_canonical",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_current",
		Fingerprint:      "reopened-canonical",
		Title:            "Reopened canonical finding",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: "unvalidated",
		State:            findingStateOpen,
	}
	alias := &storepkg.Finding{
		ID:               "fnd_prior_alias",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_old",
		Fingerprint:      "prior-alias",
		Title:            "Prior alias finding",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusPending,
		State:            findingStateOpen,
		DuplicateOf:      canonical.ID,
	}
	for _, finding := range []*storepkg.Finding{canonical, alias} {
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
		}
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kaset-validation-prior-alias",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityScanID:    alias.ScanRunID,
				labels.LabelSecurityFindingID: alias.ID,
				labels.LabelSecurityStage:     security.StageValidation,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseFailed},
	}

	if err := reconciler.ingestValidationTask(ctx, scan, task); err != nil {
		t.Fatalf("ingestValidationTask() error = %v", err)
	}
	updatedCanonical, err := securityStore.GetFinding(ctx, defaultNS, canonical.ID)
	if err != nil || updatedCanonical.ValidationStatus != "unvalidated" || updatedCanonical.ValidationJSON != "" {
		t.Fatalf("canonical validation = %#v, err %v, want current occurrence unchanged", updatedCanonical, err)
	}
	updatedAlias, err := securityStore.GetFinding(ctx, defaultNS, alias.ID)
	if err != nil || updatedAlias.ValidationStatus != findingValidationStatusPending {
		t.Fatalf("alias validation = %#v, err %v, want stale result ignored", updatedAlias, err)
	}
}

func TestProgressLatestScanRunUsesNewestOwnedScanWhenStatusIsStale(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

	scan := &corev1alpha1.RepositoryScan{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "RepositoryScan",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          "https://github.com/sozercan/kaset",
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-opus46-reviewer"},
		},
		Status: corev1alpha1.RepositoryScanStatus{
			LastScanID: "scan_old",
			Phase:      repositoryScanPhaseError,
		},
	}

	oldTask := &corev1alpha1.Task{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "Task",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "kaset-incremental-old",
			Namespace:         defaultNS,
			CreationTimestamp: metav1.NewTime(mustParseTime(t, "2026-04-10T05:00:00Z")),
			Labels: map[string]string{
				labels.LabelSecurityTarget: "kaset",
				labels.LabelSecurityScanID: "scan_old",
			},
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseSucceeded,
		},
	}

	newTask := &corev1alpha1.Task{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "Task",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "kaset-manual-threat-model-new",
			Namespace:         defaultNS,
			CreationTimestamp: metav1.NewTime(mustParseTime(t, "2026-04-10T05:05:00Z")),
			Labels: map[string]string{
				labels.LabelSecurityTarget: "kaset",
				labels.LabelSecurityScanID: "scan_new",
				labels.LabelSecurityMode:   "manual",
				labels.LabelSecurityStage:  security.StageThreatModel,
			},
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseSucceeded,
		},
	}
	mapperTask := &corev1alpha1.Task{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "Task",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "kaset-manual-mapper-new",
			Namespace:         defaultNS,
			CreationTimestamp: metav1.NewTime(mustParseTime(t, "2026-04-10T05:06:00Z")),
			Labels: map[string]string{
				labels.LabelSecurityTarget: "kaset",
				labels.LabelSecurityScanID: "scan_new",
				labels.LabelSecurityMode:   "manual",
				labels.LabelSecurityStage:  security.StageMapper,
			},
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseSucceeded,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithObjects(scan, oldTask, newTask, mapperTask).
		Build()

	reconciler := &RepositoryScanReconciler{
		Client:        cl,
		Scheme:        scheme,
		SecurityStore: store,
	}

	if err := store.CreateScanRun(ctx, &storepkg.ScanRun{
		ID:             "scan_new",
		Namespace:      defaultNS,
		RepositoryScan: "kaset",
		TaskName:       newTask.Name,
		Mode:           "manual",
		Phase:          scanRunPhasePending,
		StartedAt:      newTask.CreationTimestamp.Time,
	}); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}

	progressed, err := reconciler.progressLatestScanRun(ctx, scan)
	if err != nil {
		t.Fatalf("progressLatestScanRun() error = %v", err)
	}
	if !progressed {
		t.Fatal("progressLatestScanRun() = false, want true")
	}

	run, err := store.GetScanRun(ctx, defaultNS, "scan_new")
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.Phase != scanRunPhaseSucceeded || run.CompletedAt == nil {
		t.Fatalf("run phase/completedAt = %q/%v, want succeeded with completion", run.Phase, run.CompletedAt)
	}
}

func TestCreateScanRunIsIdempotentWhenTaskAlreadyExists(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

	scan := &corev1alpha1.RepositoryScan{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "RepositoryScan",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-security-repository-20260425175643",
			Namespace: defaultNS,
		},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          "https://github.com/sozercan/actions-test.git",
			Branch:           "demo/security-python-command-injection",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "demo-security-analysis"},
		},
		Status: corev1alpha1.RepositoryScanStatus{
			Phase: repositoryScanPhasePending,
		},
	}

	taskName := security.ScanStageTaskName(scan.Name, "initial", security.StageThreatModel, "")
	scanID := security.ScanRunID(taskName)
	timeout := metav1.Duration{Duration: 2 * time.Hour}
	priority := int32(700)
	existingTask := &corev1alpha1.Task{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "Task",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskName,
			Namespace: scan.Namespace,
			Labels: map[string]string{
				labels.LabelManaged:        "true",
				labels.LabelCreatedBy:      "repository-security",
				labels.LabelSecurityTarget: labels.SelectorValue(scan.Name),
				labels.LabelSecurityScanID: scanID,
				labels.LabelSecurityMode:   "initial",
				labels.LabelSecurityStage:  security.StageThreatModel,
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &scan.Spec.AnalysisAgentRef,
			Prompt:   security.BuildThreatModelResultPrompt(scan, "initial", "", "", "", security.AgentResultBinding{RepositoryScan: scan.Name, ScanID: scanID}),
			Timeout:  &timeout,
			Priority: &priority,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithObjects(scan, existingTask).
		Build()

	reconciler := &RepositoryScanReconciler{
		Client:        cl,
		Scheme:        scheme,
		SecurityStore: store,
	}

	if err := reconciler.createScanRun(ctx, scan, "initial", "", ""); err != nil {
		t.Fatalf("createScanRun() error = %v", err)
	}

	run, err := store.GetScanRun(ctx, scan.Namespace, scanID)
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if run.TaskName != taskName {
		t.Fatalf("run.TaskName = %q, want %q", run.TaskName, taskName)
	}
	if run.Phase != scanRunPhasePending {
		t.Fatalf("run.Phase = %q, want pending", run.Phase)
	}

	current := &corev1alpha1.RepositoryScan{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
		t.Fatalf("Get(scan) error = %v", err)
	}
	if current.Status.Phase != repositoryScanPhaseScanning {
		t.Fatalf("scan.Status.Phase = %q, want %q", current.Status.Phase, repositoryScanPhaseScanning)
	}
	if current.Status.LastScanID != scanID {
		t.Fatalf("scan.Status.LastScanID = %q, want %q", current.Status.LastScanID, scanID)
	}
	if current.Status.LastScanTaskName != taskName {
		t.Fatalf("scan.Status.LastScanTaskName = %q, want %q", current.Status.LastScanTaskName, taskName)
	}
}

type patchIngestFixture struct {
	store            *sqlitestore.Store
	publicationStore *patchPublicationStore
	reconciler       *RepositoryScanReconciler
	scan             *corev1alpha1.RepositoryScan
	finding          *storepkg.Finding
	proposal         *storepkg.PatchProposal
}

type patchPublicationStore struct {
	storepkg.PublicationStore
	publication *storepkg.Publication
	err         error
}

func (s *patchPublicationStore) GetPublication(_ context.Context, id string) (*storepkg.Publication, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.publication == nil || s.publication.ID != id {
		return nil, storepkg.ErrNotFound
	}
	copyValue := *s.publication
	return &copyValue, nil
}

func newPatchIngestFixture(t *testing.T, id string) patchIngestFixture {
	t.Helper()
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)

	findingID := "fnd_patch_" + id
	taskName := "kaset-patch-" + id
	branch := "orka/security/" + findingID
	publicationStore := &patchPublicationStore{}
	fixture := patchIngestFixture{
		store:            securityStore,
		publicationStore: publicationStore,
		reconciler: &RepositoryScanReconciler{
			SecurityStore:    securityStore,
			ArtifactStore:    securityStore,
			ResultStore:      securityStore,
			PublicationStore: publicationStore,
		},
		scan: &corev1alpha1.RepositoryScan{
			ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
			Spec: corev1alpha1.RepositoryScanSpec{
				RepoURL:      "https://github.com/example/kaset",
				ForkRepo:     "https://github.com/example/kaset",
				Branch:       "main",
				PRBaseBranch: "main",
			},
		},
		finding: &storepkg.Finding{
			ID:               findingID,
			Namespace:        defaultNS,
			RepositoryScan:   "kaset",
			ScanRunID:        "scan_patch",
			Fingerprint:      "sha256:patch-" + id,
			Title:            "Patch target",
			Summary:          "candidate finding",
			Severity:         "high",
			Confidence:       "high",
			ValidationStatus: "validated",
			State:            findingStatePatchPending,
		},
		proposal: &storepkg.PatchProposal{
			ID:             "patch_" + id,
			Namespace:      defaultNS,
			RepositoryScan: "kaset",
			FindingID:      findingID,
			TaskName:       taskName,
			Branch:         branch,
			Status:         scanRunPhasePending,
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		},
	}
	if err := securityStore.UpsertFinding(ctx, fixture.finding); err != nil {
		t.Fatalf("UpsertFinding() error = %v", err)
	}
	if err := securityStore.CreatePatchProposal(ctx, fixture.proposal); err != nil {
		t.Fatalf("CreatePatchProposal() error = %v", err)
	}
	return fixture
}

func patchTaskForFixture(fixture patchIngestFixture, resultAvailable bool) *corev1alpha1.Task {
	diffName := fmt.Sprintf("security-patch-%s.diff", fixture.finding.ID)
	summaryName := fmt.Sprintf("security-patch-%s.json", fixture.finding.ID)
	remoteBeforeSHA := ""
	headSHA := strings.Repeat("b", 40)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fixture.proposal.TaskName,
			Namespace: fixture.proposal.Namespace,
			UID:       types.UID("uid-" + fixture.proposal.TaskName),
			Labels: map[string]string{
				labels.LabelSecurityTarget:    fixture.scan.Name,
				labels.LabelSecurityScanID:    fixture.finding.ScanRunID,
				labels.LabelSecurityFindingID: fixture.finding.ID,
				labels.LabelSecurityStage:     security.StagePatch,
				labels.LabelSecurityMode:      security.StagePatch,
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAgent,
			Prompt: fmt.Sprintf("REQUIRED_SECURITY_ARTIFACTS: %s, %s\n", diffName, summaryName),
			Workspace: &corev1alpha1.WorkspaceConfig{
				Intent:             corev1alpha1.WorkspaceIntentWrite,
				GitRepo:            fixture.scan.Spec.RepoURL,
				PublicationGitRepo: fixture.scan.Spec.ForkRepo,
				PushBranch:         fixture.proposal.Branch,
				ExpectedRemoteSHA:  remoteBeforeSHA,
				PRBaseBranch:       fixture.scan.Spec.PRBaseBranch,
				CreatePR:           true,
			},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseSucceeded,
			ResultRef: &corev1alpha1.ResultReference{Available: resultAvailable},
			Execution: &corev1alpha1.TaskExecutionStatus{Attempt: 1, PromptID: "prompt-" + fixture.proposal.TaskName},
			Delivery: &corev1alpha1.TaskDeliveryStatus{
				State:         corev1alpha1.TaskDeliveryStateVerifiedExact,
				Outcome:       corev1alpha1.TaskDeliveryOutcomeVerifiedExact,
				PublicationID: "publication-" + fixture.proposal.TaskName,
				PublicationRepository: &corev1alpha1.RepositoryIdentity{
					Provider: "github",
					ID:       "github.com/example/kaset",
				},
				Branch:            fixture.proposal.Branch,
				RemoteBeforeSHA:   &remoteBeforeSHA,
				ExpectedCommitSHA: headSHA,
				VerifiedRemoteSHA: headSHA,
				ArtifactDigest:    "sha256:" + strings.Repeat("a", 64),
				PRReceipt: &corev1alpha1.TaskPullRequestReceipt{
					ID:         "github:101:42",
					Number:     42,
					URL:        fixture.scan.Spec.RepoURL + "/pull/42",
					State:      "Open",
					BaseBranch: fixture.scan.Spec.PRBaseBranch,
					HeadBranch: fixture.proposal.Branch,
					HeadSHA:    headSHA,
				},
			},
		},
	}
	fixture.publicationStore.publication = patchPublicationForTask(fixture, task)
	return task
}

func patchPublicationForTask(fixture patchIngestFixture, task *corev1alpha1.Task) *storepkg.Publication {
	sourceSHA := strings.Repeat("1", 40)
	headSHA := strings.Repeat("b", 40)
	repositoryID := "github.com/example/kaset"
	targetRef := "refs/heads/" + fixture.proposal.Branch
	baseRef := "refs/heads/" + fixture.scan.Spec.PRBaseBranch
	now := time.Now().UTC()
	return &storepkg.Publication{
		ID:                  publicationIDForTask(task),
		Namespace:           task.Namespace,
		Generation:          1,
		TaskUID:             string(task.UID),
		Attempt:             int64(task.Status.Execution.Attempt),
		PromptID:            task.Status.Execution.PromptID,
		SourceRepositoryID:  repositoryID,
		SourceRef:           sourceSHA,
		SourceBaselineSHA:   sourceSHA,
		TargetRepositoryID:  repositoryID,
		TargetRef:           targetRef,
		Baseline:            storepkg.RemoteRefState{Absent: true},
		ArtifactDigest:      "sha256:" + strings.Repeat("a", 64),
		State:               storepkg.PublicationVerifiedExact,
		PreparedReceipt:     &storepkg.PreparedPublicationReceipt{CommitSHA: headSHA},
		PublishReceipt:      &storepkg.PublishOperationReceipt{TargetRepositoryID: repositoryID, TargetRef: targetRef, RemoteBefore: storepkg.RemoteRefState{Absent: true}, ExpectedCommitSHA: headSHA},
		VerificationReceipt: &storepkg.PublicationVerificationReceipt{Outcome: storepkg.PublicationVerifiedExact, ExpectedCommitSHA: headSHA, ObservedRemote: storepkg.RemoteRefState{SHA: headSHA}},
		PRIntent:            &storepkg.PullRequestIntent{BaseRepositoryID: repositoryID, BaseRef: baseRef, HeadRepositoryID: repositoryID, HeadRef: targetRef, PublicationGeneration: 1, ExpectedHeadSHA: headSHA},
		PullRequestReceipt:  &storepkg.PullRequestOperationReceipt{IntentKey: "sha256:" + strings.Repeat("c", 64), ForgeID: "github:101:42", URL: fixture.scan.Spec.RepoURL + "/pull/42", State: "Open", HeadSHA: headSHA, ReconciledAt: now},
		CreatedAt:           now,
		UpdatedAt:           now,
	}
}

func savePatchStructuredResult(t *testing.T, fixture patchIngestFixture, sr *common.StructuredResult) {
	t.Helper()
	result, err := common.FormatStructuredResult(sr)
	if err != nil {
		t.Fatalf("FormatStructuredResult() error = %v", err)
	}
	if err := fixture.store.SaveResult(context.Background(), fixture.proposal.Namespace, fixture.proposal.TaskName, result); err != nil {
		t.Fatalf("SaveResult() error = %v", err)
	}
}

func savePatchArtifacts(t *testing.T, fixture patchIngestFixture, diff string, changedFiles []string) {
	t.Helper()
	ctx := context.Background()
	diffName := fmt.Sprintf("security-patch-%s.diff", fixture.finding.ID)
	summaryName := fmt.Sprintf("security-patch-%s.json", fixture.finding.ID)
	if err := fixture.store.SaveArtifact(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, diffName, "text/x-diff", []byte(diff)); err != nil {
		t.Fatalf("SaveArtifact(diff) error = %v", err)
	}
	summary := security.PatchSummaryArtifact{
		SchemaVersion: security.SchemaVersionPatchSummary,
		FindingID:     fixture.finding.ID,
		Summary:       "patched successfully",
		ChangedFiles:  changedFiles,
		Risk:          "low",
	}
	data, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("json.Marshal(summary) error = %v", err)
	}
	if err := fixture.store.SaveArtifact(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, summaryName, "application/json", data); err != nil {
		t.Fatalf("SaveArtifact(summary) error = %v", err)
	}
}

func savePatchArtifactsWithSummary(t *testing.T, fixture patchIngestFixture, diff string, summary security.PatchSummaryArtifact) {
	t.Helper()
	ctx := context.Background()
	diffName := fmt.Sprintf("security-patch-%s.diff", fixture.finding.ID)
	summaryName := fmt.Sprintf("security-patch-%s.json", fixture.finding.ID)
	if err := fixture.store.SaveArtifact(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, diffName, "text/x-diff", []byte(diff)); err != nil {
		t.Fatalf("SaveArtifact(diff) error = %v", err)
	}
	data, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("json.Marshal(summary) error = %v", err)
	}
	if err := fixture.store.SaveArtifact(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, summaryName, "application/json", data); err != nil {
		t.Fatalf("SaveArtifact(summary) error = %v", err)
	}
}

func TestIngestPatchTaskRetriesTransientPublishedCommitFailures(t *testing.T) {
	// A GitHub outage during verification of an otherwise succeeded patch
	// must not settle the proposal as failed: for an unscheduled completed
	// scan nothing else would reconcile it again. Transport errors and
	// server-side statuses surface as reconcile errors so controller-runtime
	// retries; client-side statuses remain terminal.
	ctx := context.Background()
	for _, tt := range []struct {
		name   string
		status int
	}{
		{"service unavailable", http.StatusServiceUnavailable},
		{"rate limited", http.StatusTooManyRequests},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
			}))
			t.Cleanup(server.Close)
			fixture := patchFixtureWithForgeSecret(t, "github-"+strings.ReplaceAll(tt.name, " ", "-"), server, true)
			savePatchStructuredResult(t, fixture, &common.StructuredResult{
				Summary:    "patched successfully",
				Diff:       testPatchFullDiff,
				Files:      []string{"app.py"},
				PushBranch: fixture.proposal.Branch,
			})
			savePatchArtifacts(t, fixture, testPatchFullDiff, []string{"app.py"})

			err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true))
			if !errors.Is(err, errRepositoryScanPublishedCommitTransient) {
				t.Fatalf("ingestPatchTask() error = %v, want a transient published-commit error", err)
			}
			assertPatchIngestState(t, fixture, scanRunPhasePending, findingStatePatchPending)
		})
	}

	t.Run("transport error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		server.Close()
		fixture := patchFixtureWithForgeSecret(t, "github-down", server, true)
		savePatchStructuredResult(t, fixture, &common.StructuredResult{
			Summary:    "patched successfully",
			Diff:       testPatchFullDiff,
			Files:      []string{"app.py"},
			PushBranch: fixture.proposal.Branch,
		})
		savePatchArtifacts(t, fixture, testPatchFullDiff, []string{"app.py"})

		err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true))
		if !errors.Is(err, errRepositoryScanPublishedCommitTransient) {
			t.Fatalf("ingestPatchTask() error = %v, want a transient published-commit error", err)
		}
		assertPatchIngestState(t, fixture, scanRunPhasePending, findingStatePatchPending)
	})

	t.Run("not found stays terminal", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		t.Cleanup(server.Close)
		fixture := patchFixtureWithForgeSecret(t, "github-404", server, true)
		savePatchStructuredResult(t, fixture, &common.StructuredResult{
			Summary:    "patched successfully",
			Diff:       testPatchFullDiff,
			Files:      []string{"app.py"},
			PushBranch: fixture.proposal.Branch,
		})
		savePatchArtifacts(t, fixture, testPatchFullDiff, []string{"app.py"})

		if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
			t.Fatalf("ingestPatchTask() error = %v", err)
		}
		assertPatchIngestState(t, fixture, scanRunPhaseFailed, findingStateOpen)
	})
}

func TestIngestPatchTaskRejectsCredentialShapedPreexistingSummaryArtifact(t *testing.T) {
	// A pre-existing summary artifact is worker-supplied through the upload
	// API and must pass the same bounded, credential-rejecting validation as
	// a harness-v2 terminal result before it becomes durable evidence.
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "secret-summary", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       testPatchFullDiff,
		Files:      []string{"app.py"},
		PushBranch: fixture.proposal.Branch,
	})
	const secret = "ak-live-0123456789abcdef"
	savePatchArtifactsWithSummary(t, fixture, testPatchFullDiff, security.PatchSummaryArtifact{
		SchemaVersion: security.SchemaVersionPatchSummary,
		FindingID:     fixture.finding.ID,
		Summary:       "removed the hard-coded api_key=" + secret + " from app.py",
		ChangedFiles:  []string{"app.py"},
		Risk:          "low",
	})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, scanRunPhaseFailed, findingStateOpen)
	proposals, err := fixture.store.ListPatchProposals(ctx, fixture.proposal.Namespace, fixture.finding.ID)
	if err != nil || len(proposals) != 1 {
		t.Fatalf("ListPatchProposals() = %#v, %v", proposals, err)
	}
	if !strings.Contains(proposals[0].Reason, "credential-shaped") {
		t.Fatalf("proposal.Reason = %q, want a credential-shaped rejection", proposals[0].Reason)
	}
	if strings.Contains(proposals[0].Reason, secret) {
		t.Fatalf("proposal.Reason = %q leaks the rejected value", proposals[0].Reason)
	}
}

func TestIngestPatchTaskSanitizesPreexistingDiffArtifact(t *testing.T) {
	// A worker-written diff artifact is raw. Once it is bound to the
	// published commit, the durable copy must carry the same redaction as the
	// result-contract branch so a remediation that removed a checked-in
	// credential does not preserve it in the referenced evidence.
	ctx := context.Background()
	var seenToken string
	const secret = "ak-live-0123456789abcdef"
	hunk := "@@ -1 +1 @@\n-api_key=" + secret + "\n+api_key=os.environ[\"API_KEY\"]"
	fixture := patchFixtureWithForgeSecret(t, "secret-diff", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: hunk}}, &seenToken), true)
	rawDiff := testPatchDiffHeader + "\n--- a/app.py\n+++ b/app.py\n" + hunk + "\n"
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       rawDiff,
		Files:      []string{"app.py"},
		PushBranch: fixture.proposal.Branch,
	})
	savePatchArtifactsWithSummary(t, fixture, rawDiff, security.PatchSummaryArtifact{
		SchemaVersion: security.SchemaVersionPatchSummary,
		FindingID:     fixture.finding.ID,
		Summary:       "  moved the key to the environment  ",
		ChangedFiles:  []string{"./app.py", "app.py"},
		TestsRun:      []security.PatchTestRun{{Command: " pytest ", ExitCode: 0}},
		Risk:          "LOW",
	})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)

	diffName := fmt.Sprintf("security-patch-%s.diff", fixture.finding.ID)
	diffData, _, err := fixture.store.GetArtifact(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, diffName)
	if err != nil {
		t.Fatalf("GetArtifact(diff) error = %v", err)
	}
	if strings.Contains(string(diffData), secret) || !strings.Contains(string(diffData), "[REDACTED]") {
		t.Fatalf("stored diff artifact = %q, want the removed credential redacted", string(diffData))
	}
	summaryName := fmt.Sprintf("security-patch-%s.json", fixture.finding.ID)
	summaryData, _, err := fixture.store.GetArtifact(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, summaryName)
	if err != nil {
		t.Fatalf("GetArtifact(summary) error = %v", err)
	}
	var stored security.PatchSummaryArtifact
	if err := json.Unmarshal(summaryData, &stored); err != nil {
		t.Fatalf("stored summary is invalid JSON: %v", err)
	}
	if stored.Summary != "moved the key to the environment" || stored.Risk != "low" ||
		!reflect.DeepEqual(stored.ChangedFiles, []string{"app.py"}) ||
		!reflect.DeepEqual(stored.TestsRun, []security.PatchTestRun{{Command: "pytest", ExitCode: 0}}) {
		t.Fatalf("stored summary = %#v, want the normalised form", stored)
	}
}

func assertPatchIngestState(t *testing.T, fixture patchIngestFixture, wantProposalStatus, wantFindingState string) {
	t.Helper()
	proposals, err := fixture.store.ListPatchProposals(context.Background(), fixture.proposal.Namespace, fixture.finding.ID)
	if err != nil {
		t.Fatalf("ListPatchProposals() error = %v", err)
	}
	if len(proposals) != 1 {
		t.Fatalf("len(proposals) = %d, want 1", len(proposals))
	}
	if proposals[0].Status != wantProposalStatus {
		t.Fatalf("proposal.Status = %q, want %q", proposals[0].Status, wantProposalStatus)
	}
	updatedFinding, err := fixture.store.GetFinding(context.Background(), fixture.proposal.Namespace, fixture.finding.ID)
	if err != nil {
		t.Fatalf("GetFinding() error = %v", err)
	}
	if updatedFinding.State != wantFindingState {
		t.Fatalf("finding.State = %q, want %q", updatedFinding.State, wantFindingState)
	}
}

func TestIngestPatchTaskMarksPROpenAfterExactPublicationReceipt(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "ready", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	diff := testPatchFullDiff
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       diff,
		Files:      []string{"app.py"},
		PushBranch: fixture.proposal.Branch,
	})
	savePatchArtifacts(t, fixture, diff, []string{"app.py"})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
	proposals, err := fixture.store.ListPatchProposals(ctx, fixture.proposal.Namespace, fixture.finding.ID)
	if err != nil {
		t.Fatalf("ListPatchProposals() error = %v", err)
	}
	if len(proposals) != 1 || proposals[0].PRNumber == nil || *proposals[0].PRNumber != 42 || proposals[0].PRURL != fixture.scan.Spec.RepoURL+"/pull/42" {
		t.Fatalf("proposal publication receipt = %#v, want PR #42", proposals)
	}
	updatedFinding, err := fixture.store.GetFinding(ctx, fixture.proposal.Namespace, fixture.finding.ID)
	if err != nil {
		t.Fatalf("GetFinding() error = %v", err)
	}
	if updatedFinding.PRNumber == nil || *updatedFinding.PRNumber != 42 || updatedFinding.PRURL != fixture.scan.Spec.RepoURL+"/pull/42" {
		t.Fatalf("finding publication receipt = %#v, want PR #42", updatedFinding)
	}
}

func TestIngestPatchTaskDoesNotReopenResolvedFinding(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "resolved-reconcile", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       testPatchFullDiff,
		Files:      []string{"app.py"},
		PushBranch: fixture.proposal.Branch,
	})
	savePatchArtifacts(t, fixture, testPatchFullDiff, []string{"app.py"})
	task := patchTaskForFixture(fixture, true)

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("ingestPatchTask() first pass error = %v", err)
	}
	if err := fixture.store.UpdateFindingState(ctx, fixture.finding.Namespace, fixture.finding.ID, findingStateResolved); err != nil {
		t.Fatalf("UpdateFindingState(resolved) error = %v", err)
	}
	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("ingestPatchTask() second pass error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStateResolved)
}

func TestIngestPatchTaskDoesNotProjectPriorOccurrenceOntoReopenedFinding(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "reopened-occurrence", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       testPatchFullDiff,
		Files:      []string{"app.py"},
		PushBranch: fixture.proposal.Branch,
	})
	savePatchArtifacts(t, fixture, testPatchFullDiff, []string{"app.py"})
	task := patchTaskForFixture(fixture, true)

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("ingestPatchTask() first pass error = %v", err)
	}
	if err := fixture.store.UpdateFindingState(ctx, fixture.finding.Namespace, fixture.finding.ID, findingStateResolved); err != nil {
		t.Fatalf("UpdateFindingState(resolved) error = %v", err)
	}
	reopened, err := fixture.store.GetFinding(ctx, fixture.finding.Namespace, fixture.finding.ID)
	if err != nil {
		t.Fatalf("GetFinding() error = %v", err)
	}
	reopened.ScanRunID = "scan_recurrence"
	reopened.State = findingStateOpen
	reopened.PatchProposalID = ""
	reopened.PRNumber = nil
	reopened.PRURL = ""
	if err := fixture.store.UpsertObservedFinding(ctx, reopened); err != nil {
		t.Fatalf("UpsertObservedFinding() error = %v", err)
	}

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("ingestPatchTask() second pass error = %v", err)
	}
	stored, err := fixture.store.GetFinding(ctx, fixture.finding.Namespace, fixture.finding.ID)
	if err != nil {
		t.Fatalf("GetFinding(reopened) error = %v", err)
	}
	if stored.State != findingStateOpen || stored.PatchProposalID != "" || stored.PRNumber != nil || stored.PRURL != "" {
		t.Fatalf("reopened finding = %#v, want no prior patch projection", stored)
	}
}

func TestIngestPatchTaskAcceptsDiffArtifactWithDifferentIndexFormatting(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "diff-index-format", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	actualDiff := strings.Join([]string{
		testPatchDiffHeader,
		"index 1111111111111111111111111111111111111111..2222222222222222222222222222222222222222 100644",
		"--- a/app.py",
		"+++ b/app.py",
		"@@ -1 +1 @@",
		"-unsafe()",
		"+safe()",
		"",
	}, "\n")
	artifactDiff := strings.Join([]string{
		testPatchDiffHeader,
		"index 1111111..2222222 100644",
		"--- a/app.py",
		"+++ b/app.py",
		"@@ -1 +1 @@",
		"-unsafe()",
		"+safe()",
		"",
	}, "\n")
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       actualDiff,
		Files:      []string{"app.py"},
		PushBranch: fixture.proposal.Branch,
	})
	savePatchArtifacts(t, fixture, artifactDiff, []string{"app.py"})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
}

func TestIngestPatchTaskAcceptsSubPathRelativeChangedFiles(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "subpath", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "services/api/app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	fixture.scan.Spec.SubPath = "services/api"
	diff := "diff --git a/services/api/app.py b/services/api/app.py\n--- a/services/api/app.py\n+++ b/services/api/app.py\n@@ -1 +1 @@\n-unsafe()\n+safe()\n"
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       diff,
		Files:      []string{"services/api/app.py"},
		PushBranch: fixture.proposal.Branch,
	})
	savePatchArtifacts(t, fixture, diff, []string{"app.py"})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
}

func TestIngestPatchTaskUsesDurablePublicationWhenTaskDeliveryReceiptIsMissing(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "task-receipt-missing", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	diff := testPatchFullDiff
	savePatchArtifacts(t, fixture, diff, []string{"app.py"})
	task := patchTaskForFixture(fixture, true)
	task.Status.Delivery = nil

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
}

func TestIngestPatchTaskRejectsArtifactsNotMatchingPublishedCommit(t *testing.T) {
	ctx := context.Background()
	// A stale or namespace-seeded diff/summary pair can be internally
	// consistent; it must still be rejected unless it matches the exact
	// published commit's file set.
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "artifact-commit-mismatch", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "evil.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-x\n+y"}}, &seenToken), true)
	savePatchArtifacts(t, fixture, testPatchFullDiff, []string{"app.py"})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, scanRunPhaseFailed, findingStateOpen)
	proposals, err := fixture.store.ListPatchProposals(ctx, fixture.proposal.Namespace, fixture.finding.ID)
	if err != nil || len(proposals) != 1 {
		t.Fatalf("ListPatchProposals() = %#v, %v", proposals, err)
	}
	if !strings.Contains(proposals[0].Reason, "does not match the published commit") {
		t.Fatalf("proposal.Reason = %q, want published-commit mismatch", proposals[0].Reason)
	}
}

func TestIngestPatchTaskRejectsMissingDurablePullRequestReceipt(t *testing.T) {
	ctx := context.Background()
	fixture := newPatchIngestFixture(t, "durable-pr-receipt-missing")
	diff := testPatchDiffHeader
	savePatchArtifacts(t, fixture, diff, []string{"app.py"})
	task := patchTaskForFixture(fixture, true)
	fixture.publicationStore.publication.PullRequestReceipt = nil

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, scanRunPhaseFailed, findingStateOpen)
}

func TestVerifiedSecurityPatchPublicationRejectsDurableRecordDrift(t *testing.T) {
	const mismatchedBranchRef = "refs/heads/other"

	tests := []struct {
		name              string
		mutateTask        func(*corev1alpha1.Task)
		mutatePublication func(*storepkg.Publication)
	}{
		{name: "create PR not requested", mutateTask: func(task *corev1alpha1.Task) { task.Spec.Workspace.CreatePR = false }},
		{name: "task UID mismatch", mutatePublication: func(publication *storepkg.Publication) { publication.TaskUID = "mismatched-task-uid" }},
		{name: "publication not verified", mutatePublication: func(publication *storepkg.Publication) { publication.State = storepkg.PublicationVerifying }},
		{name: "source repository mismatch", mutatePublication: func(publication *storepkg.Publication) { publication.SourceRepositoryID = "github.com/example/other" }},
		{name: "source baseline mismatch", mutatePublication: func(publication *storepkg.Publication) { publication.SourceRef = strings.Repeat("2", 40) }},
		{name: "target repository mismatch", mutatePublication: func(publication *storepkg.Publication) { publication.TargetRepositoryID = "github.com/example/other" }},
		{name: "target ref mismatch", mutatePublication: func(publication *storepkg.Publication) { publication.TargetRef = mismatchedBranchRef }},
		{name: "target baseline mismatch", mutatePublication: func(publication *storepkg.Publication) {
			publication.Baseline = storepkg.RemoteRefState{SHA: strings.Repeat("2", 40)}
		}},
		{name: "prepared receipt missing", mutatePublication: func(publication *storepkg.Publication) { publication.PreparedReceipt = nil }},
		{name: "publish target mismatch", mutatePublication: func(publication *storepkg.Publication) { publication.PublishReceipt.TargetRef = mismatchedBranchRef }},
		{name: "verification head mismatch", mutatePublication: func(publication *storepkg.Publication) {
			publication.VerificationReceipt.ObservedRemote.SHA = strings.Repeat("c", 40)
		}},
		{name: "PR intent base mismatch", mutatePublication: func(publication *storepkg.Publication) { publication.PRIntent.BaseRef = "refs/heads/release" }},
		{name: "PR intent head mismatch", mutatePublication: func(publication *storepkg.Publication) { publication.PRIntent.HeadRef = mismatchedBranchRef }},
		{name: "PR receipt missing", mutatePublication: func(publication *storepkg.Publication) { publication.PullRequestReceipt = nil }},
		{name: "PR forge ID invalid", mutatePublication: func(publication *storepkg.Publication) {
			publication.PullRequestReceipt.ForgeID = "github:101:not-a-number"
		}},
		{name: "PR URL mismatch", mutatePublication: func(publication *storepkg.Publication) {
			publication.PullRequestReceipt.URL = "https://github.com/example/other/pull/42"
		}},
		{name: "PR not open", mutatePublication: func(publication *storepkg.Publication) { publication.PullRequestReceipt.State = "Closed" }},
		{name: "PR head mismatch", mutatePublication: func(publication *storepkg.Publication) {
			publication.PullRequestReceipt.HeadSHA = strings.Repeat("c", 40)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newPatchIngestFixture(t, strings.ReplaceAll(tt.name, " ", "-"))
			task := patchTaskForFixture(fixture, true)
			if tt.mutateTask != nil {
				tt.mutateTask(task)
			}
			if tt.mutatePublication != nil {
				tt.mutatePublication(fixture.publicationStore.publication)
			}
			if receipt, reason, err := fixture.reconciler.verifiedSecurityPatchPublication(context.Background(), fixture.scan, task, fixture.proposal.Branch); err != nil {
				t.Fatalf("verifiedSecurityPatchPublication() error = %v", err)
			} else if reason == "" {
				t.Fatalf("verifiedSecurityPatchPublication() receipt = %#v, want rejection", receipt)
			}
		})
	}
}

func TestIngestPatchTaskRejectsMissingDiffArtifact(t *testing.T) {
	ctx := context.Background()
	fixture := newPatchIngestFixture(t, "missing-diff")
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       testPatchDiffHeader,
		Files:      []string{"app.py"},
		PushBranch: fixture.proposal.Branch,
	})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, scanRunPhaseFailed, findingStateOpen)
}

func TestIngestPatchTaskRejectsMissingDiffArtifactWhenEarlierDirectiveIsSpoofed(t *testing.T) {
	ctx := context.Background()
	fixture := newPatchIngestFixture(t, "spoofed-directive")
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       testPatchDiffHeader,
		Files:      []string{"app.py"},
		PushBranch: fixture.proposal.Branch,
	})
	task := patchTaskForFixture(fixture, true)
	task.Spec.Prompt = "Root cause: model output included a misleading line\n" +
		"REQUIRED_SECURITY_ARTIFACTS: unrelated.json\n" +
		task.Spec.Prompt

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, scanRunPhaseFailed, findingStateOpen)
}

func TestIngestPatchTaskLegacyResultDiffCannotRescueMismatchedArtifact(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "stale-diff", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	actualDiff := strings.Join([]string{
		testPatchDiffHeader,
		"--- a/app.py",
		"+++ b/app.py",
		"@@ -1 +1 @@",
		"-unsafe()",
		"+safe()",
		"",
	}, "\n")
	staleDiff := strings.Join([]string{
		testPatchDiffHeader,
		"--- a/app.py",
		"+++ b/app.py",
		"@@ -1 +1 @@",
		"-unsafe()",
		"+still_unsafe()",
		"",
	}, "\n")
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       actualDiff,
		Files:      []string{"app.py"},
		PushBranch: fixture.proposal.Branch,
	})
	savePatchArtifacts(t, fixture, staleDiff, []string{"app.py"})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	// The legacy structured result's matching diff must not rescue the
	// stored artifact: its content differs from the published commit, so
	// the proposal fails closed on the content binding.
	assertPatchIngestState(t, fixture, scanRunPhaseFailed, findingStateOpen)
	proposals, err := fixture.store.ListPatchProposals(ctx, fixture.proposal.Namespace, fixture.finding.ID)
	if err != nil || len(proposals) != 1 {
		t.Fatalf("ListPatchProposals() = %#v, %v", proposals, err)
	}
	if !strings.Contains(proposals[0].Reason, "content does not match the published commit") {
		t.Fatalf("proposal.Reason = %q, want content mismatch", proposals[0].Reason)
	}
}

func TestIngestPatchTaskRejectsConfirmedPushWithoutArtifactContract(t *testing.T) {
	ctx := context.Background()
	fixture := newPatchIngestFixture(t, "no-artifacts")
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       testPatchDiffHeader,
		Files:      []string{"app.py"},
		PushBranch: fixture.proposal.Branch,
	})
	task := patchTaskForFixture(fixture, true)
	task.Spec.Prompt = ""

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, scanRunPhaseFailed, findingStateOpen)
}

func TestIngestPatchTaskRejectsMismatchedChangedFiles(t *testing.T) {
	ctx := context.Background()
	fixture := newPatchIngestFixture(t, "mismatched-files")
	diff := testPatchDiffHeader
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:    "patched successfully",
		Diff:       diff,
		Files:      []string{"app.py", "extra.py"},
		PushBranch: fixture.proposal.Branch,
	})
	savePatchArtifacts(t, fixture, diff, []string{"extra.py"})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, scanRunPhaseFailed, findingStateOpen)
}

func TestIngestPatchTaskIgnoresLegacyStructuredResultPushError(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "failed", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary:   "patch created but push failed",
		Diff:      testPatchDiffHeader,
		PushError: "git push failed: remote rejected",
	})
	savePatchArtifacts(t, fixture, testPatchFullDiff, []string{"app.py"})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
}

func TestIngestPatchTaskIgnoresLegacyStructuredResultWithoutPushBranch(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "missing-push", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary: "patch created without confirmed push",
		Diff:    testPatchDiffHeader,
	})
	savePatchArtifacts(t, fixture, testPatchFullDiff, []string{"app.py"})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
}

func TestIngestPatchTaskDoesNotRequireLegacyResultReference(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "pending-ref", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	savePatchArtifacts(t, fixture, testPatchFullDiff, []string{"app.py"})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, false)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
}

func TestIngestPatchTaskDoesNotRequireLegacyResultRecord(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "pending-result", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}, &seenToken), true)
	savePatchArtifacts(t, fixture, testPatchFullDiff, []string{"app.py"})

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
}

func TestMergeExistingFindingCollapsesSemanticDuplicates(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: securityStore}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
	prNumber := 42
	created := mustParseTime(t, "2026-08-01T00:00:00Z")
	canonical := &storepkg.Finding{
		ID:               "fnd_canonical",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_old",
		Fingerprint:      "old-fingerprint",
		Title:            "Archive extraction permits traversal",
		Category:         "path traversal",
		Summary:          "old wording",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		State:            findingStatePROpen,
		FilePath:         "archive.go",
		Line:             100,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 100, EndLine: 108, Symbol: "extractArchive", Quote: "old"}},
		PatchProposalID:  "patch-1",
		PRNumber:         &prNumber,
		PRURL:            "https://github.com/example/kaset/pull/42",
		CreatedAt:        created,
	}
	newer := &storepkg.Finding{
		ID:               "fnd_duplicate",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_middle",
		Fingerprint:      "middle-fingerprint",
		Title:            "ZIP entries can escape the destination",
		Category:         "CWE-22 path traversal",
		Summary:          "different wording",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		State:            findingStateOpen,
		FilePath:         "archive.go",
		Line:             103,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 103, EndLine: 111, Symbol: "extractArchive", Quote: "middle"}},
		CreatedAt:        created.Add(time.Hour),
	}
	for _, finding := range []*storepkg.Finding{canonical, newer} {
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
		}
	}

	incoming := &storepkg.Finding{
		ID:               "fnd_reworded",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_current",
		Fingerprint:      "current-fingerprint",
		Title:            "Untrusted ZIP paths write outside the extraction root",
		Category:         "ZIP path traversal",
		Summary:          "current wording",
		Severity:         "critical",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		State:            findingStateOpen,
		FilePath:         "archive.go",
		Line:             105,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 105, EndLine: 113, Symbol: "extractArchive", Quote: "current"}},
	}
	if err := reconciler.mergeExistingFinding(ctx, scan, incoming); err != nil {
		t.Fatalf("mergeExistingFinding() error = %v", err)
	}
	if err := securityStore.UpsertObservedFinding(ctx, incoming); err != nil {
		t.Fatalf("UpsertObservedFinding(incoming) error = %v", err)
	}

	listed, _, err := securityStore.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: scan.Name, Limit: 10})
	if err != nil || len(listed) != 1 {
		t.Fatalf("canonical findings = %#v, err %v", listed, err)
	}
	got := listed[0]
	if got.ID != canonical.ID || got.ScanRunID != incoming.ScanRunID || got.Title != incoming.Title || got.State != findingStatePROpen || got.PatchProposalID != canonical.PatchProposalID || got.PRNumber == nil || *got.PRNumber != prNumber {
		t.Fatalf("canonical finding = %#v", got)
	}
	if len(got.Evidence) != 3 {
		t.Fatalf("canonical evidence = %#v, want evidence from all observations", got.Evidence)
	}
	alias, err := securityStore.GetFinding(ctx, defaultNS, newer.ID)
	if err != nil || alias.DuplicateOf != canonical.ID {
		t.Fatalf("duplicate alias = %#v, err %v", alias, err)
	}
	counts, err := securityStore.GetFindingCounts(ctx, defaultNS, scan.Name)
	if err != nil || counts.Total != 1 {
		t.Fatalf("finding counts = %#v, err %v", counts, err)
	}
}

func TestMergeExistingFindingCollapsesSemanticDuplicatesForExistingFingerprint(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: securityStore}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
	created := mustParseTime(t, "2026-08-01T00:00:00Z")
	canonical := &storepkg.Finding{
		ID:               "fnd_existing_canonical",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_old",
		Fingerprint:      "existing-canonical-fingerprint",
		Title:            "Archive extraction permits traversal",
		Category:         "path traversal",
		Summary:          "old wording",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		State:            findingStateOpen,
		FilePath:         "archive.go",
		Line:             100,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 100, EndLine: 108, Symbol: "extractArchive"}},
		CreatedAt:        created,
	}
	duplicate := &storepkg.Finding{
		ID:               "fnd_existing_duplicate",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_middle",
		Fingerprint:      "existing-duplicate-fingerprint",
		Title:            "ZIP entries can escape the destination",
		Category:         "CWE-22 path traversal",
		Summary:          "different wording",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		State:            findingStateOpen,
		FilePath:         "archive.go",
		Line:             103,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 103, EndLine: 111, Symbol: "extractArchive"}},
		CreatedAt:        created.Add(time.Hour),
	}
	for _, finding := range []*storepkg.Finding{canonical, duplicate} {
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
		}
	}

	incoming := *duplicate
	incoming.ScanRunID = "scan_current"
	incoming.Title = "Untrusted ZIP paths write outside the extraction root"
	incoming.Summary = "current wording"
	if err := reconciler.mergeExistingFinding(ctx, scan, &incoming); err != nil {
		t.Fatalf("mergeExistingFinding() error = %v", err)
	}
	if incoming.ID != canonical.ID || incoming.Fingerprint != canonical.Fingerprint {
		t.Fatalf("incoming identity = %q/%q, want canonical %q/%q", incoming.ID, incoming.Fingerprint, canonical.ID, canonical.Fingerprint)
	}
	if err := securityStore.UpsertObservedFinding(ctx, &incoming); err != nil {
		t.Fatalf("UpsertObservedFinding(incoming) error = %v", err)
	}

	listed, _, err := securityStore.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: scan.Name, Limit: 10})
	if err != nil || len(listed) != 1 || listed[0].ID != canonical.ID {
		t.Fatalf("canonical findings = %#v, err %v", listed, err)
	}
	alias, err := securityStore.GetFinding(ctx, defaultNS, duplicate.ID)
	if err != nil || alias.DuplicateOf != canonical.ID {
		t.Fatalf("duplicate alias = %#v, err %v", alias, err)
	}
}

func TestMergeExistingFindingKeepsDifferentScanTargetsIndependent(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: securityStore}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
	mainTarget := security.FindingV2TargetKey("https://github.com/example/kaset", "main", "")
	releaseTarget := security.FindingV2TargetKey("https://github.com/example/kaset", "release", "")
	existing := &storepkg.Finding{
		ID:               "fnd_main_branch",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_main",
		Fingerprint:      "main-fingerprint",
		TargetKey:        mainTarget,
		Title:            "Archive extraction permits traversal",
		Category:         "path traversal",
		Summary:          "main branch occurrence",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		State:            findingStateOpen,
		FilePath:         "archive.go",
		Line:             100,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 100, EndLine: 108, Symbol: "extractArchive"}},
	}
	if err := securityStore.UpsertFinding(ctx, existing); err != nil {
		t.Fatalf("UpsertFinding(existing) error = %v", err)
	}

	incoming := &storepkg.Finding{
		ID:               "fnd_release_branch",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_release",
		Fingerprint:      "release-fingerprint",
		TargetKey:        releaseTarget,
		Title:            "ZIP entries can escape the destination",
		Category:         "CWE-22 path traversal",
		Summary:          "release branch occurrence",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		State:            findingStateOpen,
		FilePath:         "archive.go",
		Line:             103,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 103, EndLine: 111, Symbol: "extractArchive"}},
	}
	if err := reconciler.mergeExistingFinding(ctx, scan, incoming); err != nil {
		t.Fatalf("mergeExistingFinding() error = %v", err)
	}
	if incoming.ID != "fnd_release_branch" {
		t.Fatalf("incoming.ID = %q, want branch-specific identity preserved", incoming.ID)
	}
	if err := securityStore.UpsertObservedFinding(ctx, incoming); err != nil {
		t.Fatalf("UpsertObservedFinding(incoming) error = %v", err)
	}
	listed, _, err := securityStore.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: scan.Name, Limit: 10})
	if err != nil || len(listed) != 2 {
		t.Fatalf("findings = %#v, err %v, want one finding per branch", listed, err)
	}
}

func TestMergeExistingFindingReconcilesLegacyTargetKeyFromFrozenRunTarget(t *testing.T) {
	for _, tc := range []struct {
		name          string
		repoURL       string
		currentBranch string
		wantMerged    bool
	}{
		{name: "same target", repoURL: "https://github.com/example/kaset", currentBranch: "main", wantMerged: true},
		{name: "same SSH target", repoURL: "git@github.com:example/kaset.git", currentBranch: "main", wantMerged: true},
		{name: "same HTTPS dot git target", repoURL: "https://github.com/example/kaset.git", currentBranch: "main", wantMerged: true},
		{name: "different target", repoURL: "https://github.com/example/kaset", currentBranch: "release", wantMerged: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			securityStore := setupControllerSQLiteStore(t)
			scan := &corev1alpha1.RepositoryScan{
				ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
				Spec:       corev1alpha1.RepositoryScanSpec{RepoURL: tc.repoURL, Branch: tc.currentBranch},
			}
			legacyScan := scan.DeepCopy()
			legacyScan.Spec.Branch = "main"
			legacyTask := newSucceededSecurityTask("legacy-target-review", "scan_legacy", security.StageReview, metav1.Now())
			legacyTask.Spec.Workspace = repositoryScanTaskWorkspace(legacyScan, corev1alpha1.WorkspaceIntentRead)
			scheme := runtime.NewScheme()
			if err := corev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("AddToScheme() error = %v", err)
			}
			reconciler := &RepositoryScanReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(legacyTask).Build(), SecurityStore: securityStore}
			if err := securityStore.CreateScanRun(ctx, &storepkg.ScanRun{ID: "scan_legacy", Namespace: defaultNS, RepositoryScan: scan.Name, TaskName: legacyTask.Name, Mode: "initial", Phase: scanRunPhaseSucceeded, StartedAt: time.Now()}); err != nil {
				t.Fatalf("CreateScanRun(legacy) error = %v", err)
			}
			symbol := "extractArchive"
			legacyRepo := security.FindingsV2Repository{RepoURL: scan.Spec.RepoURL, Branch: "main"}
			legacy := security.ToFindingV2(defaultNS, scan.Name, "scan_legacy", "legacy-review", legacyRepo, security.FindingsV2Scan{SliceID: "slice_archive"}, security.FindingsV2Finding{
				Title:    "Archive extraction permits traversal",
				Category: "path traversal",
				Evidence: []security.FindingsV2EvidenceRef{{Path: "archive.go", StartLine: 100, EndLine: 108, Symbol: &symbol}},
			})
			legacy.TargetKey = ""
			legacy.Evidence = append(legacy.Evidence, storepkg.FindingEvidenceRef{Path: "archive.go", StartLine: 200, EndLine: 204, Symbol: "validateArchive"})
			if err := securityStore.UpsertFinding(ctx, legacy); err != nil {
				t.Fatalf("UpsertFinding(legacy) error = %v", err)
			}

			currentRepo := trustedFindingsRepository(scan, nil)
			incoming := security.ToFindingV2(defaultNS, scan.Name, "scan_current", "current-review", currentRepo, security.FindingsV2Scan{SliceID: "slice_archive"}, security.FindingsV2Finding{
				Title:    "ZIP entries can escape the extraction root",
				Category: "CWE-22 path traversal",
				Evidence: []security.FindingsV2EvidenceRef{{Path: "archive.go", StartLine: 103, EndLine: 111, Symbol: &symbol}},
			})
			incomingID := incoming.ID
			if err := reconciler.mergeExistingFinding(ctx, scan, incoming); err != nil {
				t.Fatalf("mergeExistingFinding() error = %v", err)
			}
			if tc.wantMerged && incoming.ID != legacy.ID {
				t.Fatalf("incoming.ID = %q, want migrated canonical %q", incoming.ID, legacy.ID)
			}
			if !tc.wantMerged && incoming.ID != incomingID {
				t.Fatalf("incoming.ID = %q, want target-specific identity %q", incoming.ID, incomingID)
			}
			if err := securityStore.UpsertObservedFinding(ctx, incoming); err != nil {
				t.Fatalf("UpsertObservedFinding(incoming) error = %v", err)
			}
			listed, _, err := securityStore.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: scan.Name, Limit: 10})
			wantCount := 2
			if tc.wantMerged {
				wantCount = 1
			}
			if err != nil || len(listed) != wantCount {
				t.Fatalf("findings = %#v, err %v, want %d canonical findings", listed, err, wantCount)
			}
			if tc.wantMerged && listed[0].TargetKey != security.FindingV2TargetKey(currentRepo.RepoURL, currentRepo.Branch, currentRepo.SubPath) {
				t.Fatalf("TargetKey = %q, want adopted current target key", listed[0].TargetKey)
			}
		})
	}
}

func TestMergeExistingFindingReconcilesLegacyRunTargetWhenRefConfigured(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL: "https://github.com/example/kaset",
			Branch:  "release",
			Ref:     "v1.2.3",
		},
	}
	legacyTask := newSucceededSecurityTask("legacy-ref-review", "scan_legacy", security.StageReview, metav1.Now())
	legacyTask.Spec.Workspace = repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	reconciler := &RepositoryScanReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(legacyTask).Build(), SecurityStore: securityStore}
	if err := securityStore.CreateScanRun(ctx, &storepkg.ScanRun{ID: "scan_legacy", Namespace: defaultNS, RepositoryScan: scan.Name, TaskName: legacyTask.Name, Mode: "initial", Phase: scanRunPhaseSucceeded, StartedAt: time.Now()}); err != nil {
		t.Fatalf("CreateScanRun(legacy) error = %v", err)
	}
	symbol := "extractArchive"
	legacyRepo := security.FindingsV2Repository{RepoURL: scan.Spec.RepoURL, Branch: scan.Spec.Branch}
	legacy := security.ToFindingV2(defaultNS, scan.Name, "scan_legacy", "legacy-review", legacyRepo, security.FindingsV2Scan{SliceID: "slice_archive"}, security.FindingsV2Finding{
		Title:    "Archive extraction permits traversal",
		Category: "path traversal",
		Evidence: []security.FindingsV2EvidenceRef{{Path: "archive.go", StartLine: 100, EndLine: 108, Symbol: &symbol}},
	})
	legacy.TargetKey = ""
	if err := securityStore.UpsertFinding(ctx, legacy); err != nil {
		t.Fatalf("UpsertFinding(legacy) error = %v", err)
	}

	currentRepo := trustedFindingsRepository(scan, nil)
	incoming := security.ToFindingV2(defaultNS, scan.Name, "scan_current", "current-review", currentRepo, security.FindingsV2Scan{SliceID: "slice_archive"}, security.FindingsV2Finding{
		Title:    "ZIP entries can escape the extraction root",
		Category: "CWE-22 path traversal",
		Evidence: []security.FindingsV2EvidenceRef{{Path: "archive.go", StartLine: 103, EndLine: 111, Symbol: &symbol}},
	})
	if incoming.ID == legacy.ID {
		t.Fatal("test requires the changed observation to have a different exact fingerprint")
	}
	if err := reconciler.mergeExistingFinding(ctx, scan, incoming); err != nil {
		t.Fatalf("mergeExistingFinding() error = %v", err)
	}
	if incoming.ID != legacy.ID {
		t.Fatalf("incoming.ID = %q, want legacy canonical %q", incoming.ID, legacy.ID)
	}
	if err := securityStore.UpsertObservedFinding(ctx, incoming); err != nil {
		t.Fatalf("UpsertObservedFinding(incoming) error = %v", err)
	}
	listed, _, err := securityStore.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: scan.Name, Limit: 10})
	if err != nil || len(listed) != 1 {
		t.Fatalf("findings = %#v, err %v, want one canonical finding", listed, err)
	}
	wantTargetKey := security.FindingV2TargetKey(currentRepo.RepoURL, currentRepo.Branch, currentRepo.SubPath)
	if listed[0].TargetKey != wantTargetKey {
		t.Fatalf("TargetKey = %q, want %q", listed[0].TargetKey, wantTargetKey)
	}
}

type failingObservedFindingStore struct {
	storepkg.SecurityStore
}

func (failingObservedFindingStore) UpsertObservedFinding(context.Context, *storepkg.Finding) error {
	return errors.New("observed finding write unavailable")
}

func TestMergeExistingFindingPersistsCanonicalBeforeMarkingDuplicates(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: failingObservedFindingStore{SecurityStore: securityStore}}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
	created := mustParseTime(t, "2026-08-01T00:00:00Z")
	decisionAt := created.Add(2 * time.Hour)
	prNumber := 42
	canonical := &storepkg.Finding{
		ID:               "fnd_durable_canonical",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_old",
		Fingerprint:      "durable-canonical",
		Title:            "Archive extraction permits traversal",
		Category:         "path traversal",
		Summary:          "old wording",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: "unvalidated",
		State:            findingStateOpen,
		FilePath:         "archive.go",
		Line:             100,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 100, EndLine: 108, Symbol: "extractArchive", Quote: "canonical"}},
		CreatedAt:        created,
	}
	remediated := &storepkg.Finding{
		ID:               "fnd_durable_remediation",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_old",
		Fingerprint:      "durable-remediation",
		Title:            "ZIP entries can escape the destination",
		Category:         "CWE-22 path traversal",
		Summary:          "remediation wording",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusPending,
		State:            findingStatePROpen,
		FilePath:         "archive.go",
		Line:             103,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 103, EndLine: 111, Symbol: "extractArchive", Quote: "remediation"}},
		PatchProposalID:  "patch-1",
		PRNumber:         &prNumber,
		PRURL:            "https://github.com/example/kaset/pull/42",
		CreatedAt:        created.Add(time.Hour),
	}
	governed := &storepkg.Finding{
		ID:               "fnd_durable_governance",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_old",
		Fingerprint:      "durable-governance",
		Title:            "Unsafe ZIP paths reach the filesystem",
		Category:         "ZIP path traversal",
		Summary:          "governance wording",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		ValidationJSON:   `{"status":"validated"}`,
		State:            "suppressed",
		DecisionAt:       decisionAt,
		FilePath:         "archive.go",
		Line:             105,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 105, EndLine: 113, Symbol: "extractArchive", Quote: "governance"}},
		CreatedAt:        decisionAt,
	}
	for _, finding := range []*storepkg.Finding{canonical, remediated, governed} {
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
		}
	}

	incoming := &storepkg.Finding{
		ID:               "fnd_durable_incoming",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_current",
		Fingerprint:      "durable-incoming",
		Title:            "Untrusted ZIP paths write outside the extraction root",
		Category:         "ZIP path traversal",
		Summary:          "current wording",
		Severity:         "critical",
		Confidence:       "high",
		ValidationStatus: "unvalidated",
		State:            findingStateOpen,
		FilePath:         "archive.go",
		Line:             106,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 106, EndLine: 114, Symbol: "extractArchive", Quote: "incoming"}},
	}
	if err := reconciler.mergeExistingFinding(ctx, scan, incoming); err != nil {
		t.Fatalf("mergeExistingFinding() error = %v", err)
	}
	if err := reconciler.SecurityStore.UpsertObservedFinding(ctx, incoming); err == nil {
		t.Fatal("UpsertObservedFinding() error = nil, want injected failure")
	}

	stored, err := securityStore.GetFinding(ctx, defaultNS, canonical.ID)
	if err != nil {
		t.Fatalf("GetFinding(canonical) error = %v", err)
	}
	if stored.State != "suppressed" || !stored.DecisionAt.Equal(decisionAt) || stored.PatchProposalID != remediated.PatchProposalID || stored.PRNumber == nil || *stored.PRNumber != prNumber || stored.ValidationStatus != findingValidationStatusValidated || stored.ValidationJSON != governed.ValidationJSON {
		t.Fatalf("canonical finding = %#v, want merged durable state", stored)
	}
	if len(stored.Evidence) != 3 {
		t.Fatalf("canonical evidence = %#v, want all durable preexisting evidence", stored.Evidence)
	}
	for _, aliasID := range []string{remediated.ID, governed.ID} {
		alias, getErr := securityStore.GetFinding(ctx, defaultNS, aliasID)
		if getErr != nil || alias.DuplicateOf != canonical.ID {
			t.Fatalf("duplicate alias %s = %#v, err %v", aliasID, alias, getErr)
		}
	}

	retry := &storepkg.Finding{
		ID:               "fnd_durable_retry",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_retry",
		Fingerprint:      "durable-retry",
		Title:            incoming.Title,
		Category:         incoming.Category,
		Summary:          incoming.Summary,
		Severity:         incoming.Severity,
		Confidence:       incoming.Confidence,
		ValidationStatus: "unvalidated",
		State:            findingStateOpen,
		FilePath:         incoming.FilePath,
		Line:             incoming.Line,
		Evidence:         incoming.Evidence,
	}
	if err := reconciler.mergeExistingFinding(ctx, scan, retry); err != nil {
		t.Fatalf("mergeExistingFinding(retry) error = %v", err)
	}
	if retry.ID != canonical.ID || retry.State != "suppressed" || retry.PatchProposalID != remediated.PatchProposalID || retry.ValidationStatus != findingValidationStatusValidated {
		t.Fatalf("retry finding = %#v, want state recovered from canonical", retry)
	}
}

func TestMergeExistingFindingDoesNotBridgeCanonicalThroughAlias(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: securityStore}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
	newFinding := func(id string, line int) *storepkg.Finding {
		return &storepkg.Finding{
			ID:               id,
			Namespace:        defaultNS,
			RepositoryScan:   scan.Name,
			ScanRunID:        "scan_old",
			Fingerprint:      id + "-fingerprint",
			Title:            "Archive extraction permits traversal",
			Category:         "path traversal",
			Summary:          "path traversal finding",
			Severity:         "high",
			Confidence:       "high",
			ValidationStatus: findingValidationStatusValidated,
			State:            findingStateOpen,
			FilePath:         "archive.go",
			Line:             line,
			Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: line, EndLine: line}},
		}
	}
	canonical := newFinding("fnd_canonical_drift", 100)
	alias := newFinding("fnd_alias_drift", 105)
	for _, finding := range []*storepkg.Finding{canonical, alias} {
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
		}
	}
	if err := securityStore.MarkFindingDuplicate(ctx, defaultNS, alias.ID, canonical.ID); err != nil {
		t.Fatalf("MarkFindingDuplicate() error = %v", err)
	}

	incoming := newFinding("fnd_independent_drift", 110)
	incoming.ScanRunID = "scan_current"
	if err := reconciler.mergeExistingFinding(ctx, scan, incoming); err != nil {
		t.Fatalf("mergeExistingFinding() error = %v", err)
	}
	if incoming.ID != "fnd_independent_drift" {
		t.Fatalf("incoming.ID = %q, want independent finding", incoming.ID)
	}
	if err := securityStore.UpsertObservedFinding(ctx, incoming); err != nil {
		t.Fatalf("UpsertObservedFinding() error = %v", err)
	}
	listed, _, err := securityStore.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: scan.Name, Limit: 10})
	if err != nil || len(listed) != 2 {
		t.Fatalf("canonical findings = %#v, err %v, want two independent findings", listed, err)
	}
}

func TestMergeExistingFindingDoesNotBridgeIndependentCanonicalMatches(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: securityStore}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
	created := mustParseTime(t, "2026-08-01T00:00:00Z")
	newFinding := func(id string, line int, createdAt time.Time) *storepkg.Finding {
		return &storepkg.Finding{
			ID:               id,
			Namespace:        defaultNS,
			RepositoryScan:   scan.Name,
			ScanRunID:        "scan_old",
			Fingerprint:      id + "-fingerprint",
			Title:            "Archive extraction permits traversal",
			Category:         "path traversal",
			Summary:          "path traversal finding",
			Severity:         "high",
			Confidence:       "high",
			ValidationStatus: findingValidationStatusValidated,
			State:            findingStateOpen,
			FilePath:         "archive.go",
			Line:             line,
			Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: line, EndLine: line}},
			CreatedAt:        createdAt,
		}
	}
	left := newFinding("fnd_left", 100, created)
	right := newFinding("fnd_right", 110, created.Add(time.Hour))
	for _, finding := range []*storepkg.Finding{left, right} {
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
		}
	}

	incoming := newFinding("fnd_middle", 105, created.Add(2*time.Hour))
	incoming.ScanRunID = "scan_current"
	if err := reconciler.mergeExistingFinding(ctx, scan, incoming); err != nil {
		t.Fatalf("mergeExistingFinding() error = %v", err)
	}
	if incoming.ID != left.ID {
		t.Fatalf("incoming.ID = %q, want oldest compatible finding %q", incoming.ID, left.ID)
	}
	if err := securityStore.UpsertObservedFinding(ctx, incoming); err != nil {
		t.Fatalf("UpsertObservedFinding() error = %v", err)
	}
	storedRight, err := securityStore.GetFinding(ctx, defaultNS, right.ID)
	if err != nil || storedRight.DuplicateOf != "" {
		t.Fatalf("right finding = %#v, err %v, want independent canonical", storedRight, err)
	}
	listed, _, err := securityStore.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: scan.Name, Limit: 10})
	if err != nil || len(listed) != 2 {
		t.Fatalf("canonical findings = %#v, err %v, want two independent findings", listed, err)
	}
}

func TestMergeExistingFindingReopensResolvedFindingWithoutRemediationProjection(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: securityStore}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
	prNumber := 42
	existing := &storepkg.Finding{
		ID:               "fnd_recurrence",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_old",
		Fingerprint:      "recurrence-fingerprint",
		Title:            "Resolved command injection",
		Category:         "command injection",
		Summary:          "old occurrence",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusPending,
		ValidationJSON:   `{"status":"pending","summary":"prior occurrence"}`,
		State:            findingStateResolved,
		FilePath:         "run.go",
		Line:             40,
		PatchProposalID:  "patch-old",
		PRNumber:         &prNumber,
		PRURL:            "https://github.com/example/kaset/pull/42",
	}
	if err := securityStore.UpsertFinding(ctx, existing); err != nil {
		t.Fatalf("UpsertFinding(existing) error = %v", err)
	}
	incoming := &storepkg.Finding{
		ID:               existing.ID,
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_current",
		Fingerprint:      existing.Fingerprint,
		Title:            "Command injection returned",
		Category:         existing.Category,
		Summary:          "new occurrence",
		Severity:         "critical",
		Confidence:       "high",
		ValidationStatus: "unvalidated",
		State:            findingStateOpen,
		FilePath:         existing.FilePath,
		Line:             existing.Line,
	}
	if err := reconciler.mergeExistingFinding(ctx, scan, incoming); err != nil {
		t.Fatalf("mergeExistingFinding() error = %v", err)
	}
	if incoming.State != findingStateOpen || incoming.ValidationStatus != "unvalidated" || incoming.ValidationJSON != "" || incoming.PatchProposalID != "" || incoming.PRNumber != nil || incoming.PRURL != "" {
		t.Fatalf("incoming recurrence = %#v", incoming)
	}
	if err := securityStore.UpsertObservedFinding(ctx, incoming); err != nil {
		t.Fatalf("UpsertObservedFinding(incoming) error = %v", err)
	}
	stored, err := securityStore.GetFinding(ctx, defaultNS, existing.ID)
	if err != nil || stored.State != findingStateOpen || stored.ValidationStatus != "unvalidated" || stored.ValidationJSON != "" || stored.PatchProposalID != "" || stored.PRNumber != nil || stored.PRURL != "" {
		t.Fatalf("stored recurrence = %#v, err %v", stored, err)
	}
}

func TestMergeExistingFindingDoesNotProjectTerminalDuplicateRemediationOntoActiveCanonical(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	reconciler := &RepositoryScanReconciler{SecurityStore: securityStore}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
	created := mustParseTime(t, "2026-08-01T00:00:00Z")
	prNumber := 42
	canonical := &storepkg.Finding{
		ID:               "fnd_active_canonical",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_old",
		Fingerprint:      "active-canonical",
		Title:            "Archive extraction permits traversal",
		Category:         "path traversal",
		Summary:          "active occurrence",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		State:            findingStateOpen,
		FilePath:         "archive.go",
		Line:             100,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 100, EndLine: 108, Symbol: "extractArchive"}},
		CreatedAt:        created,
	}
	resolved := &storepkg.Finding{
		ID:               "fnd_resolved_duplicate",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_middle",
		Fingerprint:      "resolved-duplicate",
		Title:            "ZIP entries can escape the destination",
		Category:         "CWE-22 path traversal",
		Summary:          "resolved occurrence",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		State:            findingStateResolved,
		FilePath:         "archive.go",
		Line:             103,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 103, EndLine: 111, Symbol: "extractArchive"}},
		PatchProposalID:  "patch-old",
		PRNumber:         &prNumber,
		PRURL:            "https://github.com/example/kaset/pull/42",
		CreatedAt:        created.Add(time.Hour),
	}
	for _, finding := range []*storepkg.Finding{canonical, resolved} {
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
		}
	}

	incoming := &storepkg.Finding{
		ID:               "fnd_current_observation",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_current",
		Fingerprint:      "current-observation",
		Title:            "Untrusted ZIP paths escape the extraction root",
		Category:         "ZIP path traversal",
		Summary:          "current occurrence",
		Severity:         "critical",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		State:            findingStateOpen,
		FilePath:         "archive.go",
		Line:             105,
		Evidence:         []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 105, EndLine: 113, Symbol: "extractArchive"}},
	}
	if err := reconciler.mergeExistingFinding(ctx, scan, incoming); err != nil {
		t.Fatalf("mergeExistingFinding() error = %v", err)
	}
	if incoming.ID != canonical.ID || incoming.State != findingStateOpen || incoming.PatchProposalID != "" || incoming.PRNumber != nil || incoming.PRURL != "" {
		t.Fatalf("incoming finding = %#v, want active canonical without terminal remediation", incoming)
	}
	if err := securityStore.UpsertObservedFinding(ctx, incoming); err != nil {
		t.Fatalf("UpsertObservedFinding(incoming) error = %v", err)
	}

	stored, err := securityStore.GetFinding(ctx, defaultNS, canonical.ID)
	if err != nil || stored.State != findingStateOpen || stored.PatchProposalID != "" || stored.PRNumber != nil || stored.PRURL != "" {
		t.Fatalf("stored canonical = %#v, err %v", stored, err)
	}
	alias, err := securityStore.GetFinding(ctx, defaultNS, resolved.ID)
	if err != nil || alias.DuplicateOf != canonical.ID {
		t.Fatalf("resolved alias = %#v, err %v", alias, err)
	}
}

func TestMergeExistingFindingPreservesTerminalValidationFromSemanticMatch(t *testing.T) {
	for _, validationStatus := range []string{findingValidationStatusFailed, findingValidationStatusSkipped} {
		t.Run(validationStatus, func(t *testing.T) {
			ctx := context.Background()
			securityStore := setupControllerSQLiteStore(t)
			reconciler := &RepositoryScanReconciler{SecurityStore: securityStore}
			scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
			created := mustParseTime(t, "2026-08-01T00:00:00Z")
			canonical := &storepkg.Finding{
				ID:               "fnd_canonical_" + validationStatus,
				Namespace:        defaultNS,
				RepositoryScan:   scan.Name,
				ScanRunID:        "scan_old",
				Fingerprint:      "canonical-" + validationStatus,
				Title:            "Archive extraction permits traversal",
				Category:         "path traversal",
				Summary:          "canonical wording",
				Severity:         "high",
				Confidence:       "high",
				ValidationStatus: "unvalidated",
				State:            findingStateOpen,
				FilePath:         "archive.go",
				Line:             100,
				CreatedAt:        created,
			}
			terminal := &storepkg.Finding{
				ID:               "fnd_terminal_" + validationStatus,
				Namespace:        defaultNS,
				RepositoryScan:   scan.Name,
				ScanRunID:        "scan_middle",
				Fingerprint:      "terminal-" + validationStatus,
				Title:            "ZIP entries can escape the destination",
				Category:         "CWE-22 path traversal",
				Summary:          "terminal validation wording",
				Severity:         "high",
				Confidence:       "high",
				ValidationStatus: validationStatus,
				ValidationJSON:   fmt.Sprintf(`{"version":1,"finding_id":%q,"status":%q,"summary":"terminal result"}`, "fnd_terminal_"+validationStatus, validationStatus),
				State:            findingStateOpen,
				FilePath:         "archive.go",
				Line:             103,
				CreatedAt:        created.Add(time.Hour),
			}
			for _, finding := range []*storepkg.Finding{canonical, terminal} {
				if err := securityStore.UpsertFinding(ctx, finding); err != nil {
					t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
				}
			}

			incoming := &storepkg.Finding{
				ID:               "fnd_incoming_" + validationStatus,
				Namespace:        defaultNS,
				RepositoryScan:   scan.Name,
				ScanRunID:        "scan_current",
				Fingerprint:      "incoming-" + validationStatus,
				Title:            "Untrusted ZIP paths escape the extraction root",
				Category:         "ZIP path traversal",
				Summary:          "current wording",
				Severity:         "high",
				Confidence:       "high",
				ValidationStatus: "unvalidated",
				State:            findingStateOpen,
				FilePath:         "archive.go",
				Line:             105,
			}
			if err := reconciler.mergeExistingFinding(ctx, scan, incoming); err != nil {
				t.Fatalf("mergeExistingFinding() error = %v", err)
			}
			if incoming.ID != canonical.ID || incoming.ValidationStatus != validationStatus {
				t.Fatalf("merged finding = %#v", incoming)
			}
			if !strings.Contains(incoming.ValidationJSON, canonical.ID) || strings.Contains(incoming.ValidationJSON, terminal.ID) {
				t.Fatalf("ValidationJSON = %q, want canonical finding ID", incoming.ValidationJSON)
			}
		})
	}
}

func TestFindingIdentityMatchScoreRequiresCategoryAndStableLocation(t *testing.T) {
	base := &storepkg.Finding{
		Category: "path traversal",
		FilePath: "archive.go",
		Line:     100,
		Evidence: []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 100, EndLine: 108, Symbol: "extractArchive"}},
	}
	tests := []struct {
		name  string
		other storepkg.Finding
		match bool
	}{
		{name: "same symbol with nearby line drift", other: storepkg.Finding{Category: "CWE-22 path traversal", FilePath: "archive.go", Line: 104, Evidence: []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 104, EndLine: 112, Symbol: "extractArchive"}}}, match: true},
		{name: "same symbol at a distinct location", other: storepkg.Finding{Category: "CWE-22 path traversal", FilePath: "archive.go", Line: 180, Evidence: []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 180, EndLine: 188, Symbol: "extractArchive"}}}},
		{name: "nearby line without symbols", other: storepkg.Finding{Category: "path traversal", FilePath: "archive.go", Line: 104, Evidence: []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 104, EndLine: 112}}}, match: true},
		{name: "nearby line with conflicting symbols", other: storepkg.Finding{Category: "path traversal", FilePath: "archive.go", Line: 104, Evidence: []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 104, EndLine: 112, Symbol: "writeManifest"}}}},
		{name: "different symbol and location", other: storepkg.Finding{Category: "path traversal", FilePath: "archive.go", Line: 180, Evidence: []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 180, EndLine: 188, Symbol: "writeManifest"}}}},
		{name: "different category", other: storepkg.Finding{Category: "command injection", FilePath: "archive.go", Line: 100, Evidence: []storepkg.FindingEvidenceRef{{Path: "archive.go", StartLine: 100, EndLine: 108, Symbol: "extractArchive"}}}},
		{name: "different file", other: storepkg.Finding{Category: "path traversal", FilePath: "upload.go", Line: 100, Evidence: []storepkg.FindingEvidenceRef{{Path: "upload.go", StartLine: 100, EndLine: 108, Symbol: "extractArchive"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := findingIdentityMatchScore(base, &tt.other) >= 2; got != tt.match {
				t.Fatalf("findingIdentityMatchScore() match = %v, want %v", got, tt.match)
			}
		})
	}
}

func TestFindingIdentityMatchScoreRejectsConflictingPrimarySymbolsWithSharedSupport(t *testing.T) {
	left := &storepkg.Finding{
		Category: "path traversal",
		FilePath: "archive.go",
		Line:     100,
		Evidence: []storepkg.FindingEvidenceRef{
			{Path: "archive.go", StartLine: 100, EndLine: 108, Symbol: "extractArchive"},
			{Path: "archive.go", StartLine: 200, EndLine: 205, Symbol: "sanitizePath"},
		},
	}
	right := &storepkg.Finding{
		Category: "CWE-22 path traversal",
		FilePath: "archive.go",
		Line:     104,
		Evidence: []storepkg.FindingEvidenceRef{
			{Path: "archive.go", StartLine: 104, EndLine: 112, Symbol: "writeManifest"},
			{Path: "archive.go", StartLine: 200, EndLine: 205, Symbol: "sanitizePath"},
		},
	}
	if score := findingIdentityMatchScore(left, right); score != 0 {
		t.Fatalf("findingIdentityMatchScore() = %d, want primary symbol conflict rejected", score)
	}
}

func TestFindingIdentityMatchScoreRejectsDistinctPrimaryLocationsWithSharedSupport(t *testing.T) {
	left := &storepkg.Finding{
		Category: "path traversal",
		FilePath: "archive.go",
		Line:     100,
		Evidence: []storepkg.FindingEvidenceRef{
			{Path: "archive.go", StartLine: 100, EndLine: 108, Symbol: "extractArchive"},
			{Path: "archive.go", StartLine: 250, EndLine: 255, Symbol: "sanitizePath"},
		},
	}
	right := &storepkg.Finding{
		Category: "CWE-22 path traversal",
		FilePath: "archive.go",
		Line:     180,
		Evidence: []storepkg.FindingEvidenceRef{
			{Path: "archive.go", StartLine: 180, EndLine: 188, Symbol: "extractArchive"},
			{Path: "archive.go", StartLine: 250, EndLine: 255, Symbol: "sanitizePath"},
		},
	}
	if score := findingIdentityMatchScore(left, right); score != 0 {
		t.Fatalf("findingIdentityMatchScore() = %d, want distinct primary locations rejected", score)
	}
}

func TestFindingIdentityMatchScoreRejectsSharedEnclosingRangeWithDistinctSinks(t *testing.T) {
	left := &storepkg.Finding{
		Category: "command injection",
		FilePath: "handler.go",
		Line:     100,
		Evidence: []storepkg.FindingEvidenceRef{
			{Path: "handler.go", StartLine: 100, EndLine: 220, Symbol: "handleRequest"},
			{Path: "handler.go", StartLine: 140, EndLine: 142, Symbol: "runImport"},
		},
	}
	right := &storepkg.Finding{
		Category: "CWE-78 command injection",
		FilePath: "handler.go",
		Line:     100,
		Evidence: []storepkg.FindingEvidenceRef{
			{Path: "handler.go", StartLine: 100, EndLine: 220, Symbol: "handleRequest"},
			{Path: "handler.go", StartLine: 180, EndLine: 182, Symbol: "runExport"},
		},
	}
	if score := findingIdentityMatchScore(left, right); score != 0 {
		t.Fatalf("findingIdentityMatchScore() = %d, want enclosing primary range rejected", score)
	}
}

func TestMergeFindingValidationStateRanksFailedAboveSkipped(t *testing.T) {
	failed := &storepkg.Finding{ID: "finding", ValidationStatus: findingValidationStatusFailed, ValidationJSON: `{"status":"failed"}`}
	skipped := &storepkg.Finding{ID: "finding", ValidationStatus: findingValidationStatusSkipped, ValidationJSON: `{"status":"skipped"}`}

	target := *skipped
	mergeFindingValidationState(&target, failed)
	if target.ValidationStatus != findingValidationStatusFailed || target.ValidationJSON != failed.ValidationJSON {
		t.Fatalf("skipped then failed = %#v, want failed", target)
	}

	target = *failed
	mergeFindingValidationState(&target, skipped)
	if target.ValidationStatus != findingValidationStatusFailed || target.ValidationJSON != failed.ValidationJSON {
		t.Fatalf("failed then skipped = %#v, want failed", target)
	}
}

func TestFindingCategoryMatchesRequiresSpecificSharedIdentity(t *testing.T) {
	tests := []struct {
		name        string
		left, right string
		want        bool
	}{
		{name: "exact single term", left: "SSRF", right: "ssrf", want: true},
		{name: "matching cwe", left: "CWE-78 command execution", right: "OS command injection (CWE-78)", want: true},
		{name: "mismatched cwe", left: "path traversal CWE-22", right: "path traversal CWE-23", want: false},
		{name: "two shared terms", left: "sensitive information disclosure", right: "information disclosure", want: true},
		{name: "same class with different qualifiers", left: "SQL injection via untrusted input", right: "SQL injection through user input", want: true},
		{name: "different classes with shared qualifiers", left: "SQL injection via untrusted user input", right: "command injection via untrusted user input", want: false},
		{name: "different classes with shared location", left: "SQL injection in query builder", right: "command injection in query builder", want: false},
		{name: "generic injection term", left: "command injection", right: "SQL injection", want: false},
		{name: "single generic subset", left: "injection", right: "NoSQL injection", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := findingCategoryMatches(tt.left, tt.right); got != tt.want {
				t.Fatalf("findingCategoryMatches(%q, %q) = %v, want %v", tt.left, tt.right, got, tt.want)
			}
		})
	}
}

func TestMergeFindingEvidenceAndRemediationPreservesUserDecisionAndIgnoresAliasWorkflow(t *testing.T) {
	target := &storepkg.Finding{ID: "canonical", State: findingStateOpen, UpdatedAt: mustParseTime(t, "2026-08-01T00:00:00Z")}
	dismissedAt := mustParseTime(t, "2026-08-02T00:00:00Z")
	dismissed := &storepkg.Finding{ID: "dismissed", State: "dismissed", DecisionAt: dismissedAt, UpdatedAt: dismissedAt}
	mergeFindingEvidenceAndRemediation(target, dismissed)
	target.UpdatedAt = mustParseTime(t, "2026-08-05T00:00:00Z")
	olderSuppression := &storepkg.Finding{ID: "older-suppression", State: "suppressed", DecisionAt: mustParseTime(t, "2026-08-01T00:00:00Z"), UpdatedAt: mustParseTime(t, "2026-08-06T00:00:00Z")}
	mergeFindingEvidenceAndRemediation(target, olderSuppression)
	if target.State != dismissed.State || !target.DecisionAt.Equal(dismissedAt) {
		t.Fatalf("target state/decision = %q/%s, want %q/%s", target.State, target.DecisionAt, dismissed.State, dismissedAt)
	}
	newerSuppressionAt := mustParseTime(t, "2026-08-03T00:00:00Z")
	newerSuppression := &storepkg.Finding{ID: "newer-suppression", State: "suppressed", DecisionAt: newerSuppressionAt, UpdatedAt: mustParseTime(t, "2026-08-03T00:00:00Z")}
	mergeFindingEvidenceAndRemediation(target, newerSuppression)
	if target.State != newerSuppression.State || !target.DecisionAt.Equal(newerSuppressionAt) {
		t.Fatalf("target state/decision = %q/%s, want %q/%s", target.State, target.DecisionAt, newerSuppression.State, newerSuppressionAt)
	}

	resolved := &storepkg.Finding{ID: "resolved", State: findingStateResolved}
	alias := &storepkg.Finding{ID: "alias", DuplicateOf: resolved.ID, State: findingStatePROpen}
	mergeFindingEvidenceAndRemediation(resolved, alias)
	if resolved.State != findingStateResolved {
		t.Fatalf("resolved state = %q, want alias workflow ignored", resolved.State)
	}
}

func TestRefreshScanRunStatusResolvesUnseenFindingAfterRemediationPRMerged(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	pullRequests := map[int]int{}
	compareRequests := map[string]int{}
	var concurrentDecisionErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer forge-token-value" {
			t.Fatalf("Authorization = %q", r.Header.Get("Authorization"))
		}
		if comparison, ok := strings.CutPrefix(r.URL.Path, "/repos/example/kaset/compare/"); ok {
			compareRequests[comparison]++
			switch comparison {
			case testRepositoryScanMergeSHA + "..." + testRepositoryScanHeadSHA:
				_, _ = w.Write([]byte(`{"status":"ahead"}`))
			case testRepositoryScanLateMerge + "..." + testRepositoryScanHeadSHA:
				_, _ = w.Write([]byte(`{"status":"behind"}`))
			default:
				t.Fatalf("unexpected comparison %s", comparison)
			}
			return
		}
		var prNumber int
		if _, err := fmt.Sscanf(r.URL.Path, "/repos/example/kaset/pulls/%d", &prNumber); err != nil {
			t.Fatalf("unexpected GitHub path %s", r.URL.Path)
		}
		pullRequests[prNumber]++
		switch prNumber {
		case 42:
			concurrentDecisionErr = securityStore.UpdateFindingState(ctx, defaultNS, "fnd_concurrent_decision", "dismissed")
			_, _ = fmt.Fprintf(w, `{"merged":true,"merged_at":"2026-08-30T12:00:00Z","merge_commit_sha":%q,"base":{"ref":"main"}}`, testRepositoryScanMergeSHA)
		case 43:
			_, _ = w.Write([]byte(`{"merged":false,"merged_at":null,"base":{"ref":"main"}}`))
		case 45:
			_, _ = fmt.Fprintf(w, `{"merged":true,"merged_at":"2026-08-30T12:00:00Z","merge_commit_sha":%q,"base":{"ref":"release"}}`, testRepositoryScanMergeSHA)
		case 46:
			_, _ = fmt.Fprintf(w, `{"merged":true,"merged_at":"2026-08-30T12:00:00Z","merge_commit_sha":%q,"base":{"ref":"main"}}`, testRepositoryScanLateMerge)
		default:
			t.Fatalf("unexpected pull request %d", prNumber)
		}
	}))
	t.Cleanup(server.Close)

	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:            "https://github.com/example/kaset",
			ForgeCredentialRef: &corev1.LocalObjectReference{Name: testPatchForgeSecretName},
		},
	}
	completed := metav1.Now()
	threatTask := newSucceededSecurityTask("kaset-resolve-threat", "scan_current", security.StageThreatModel, completed)
	mapperTask := newSucceededSecurityTask("kaset-resolve-mapper", "scan_current", security.StageMapper, completed)
	reviewTask := newSucceededSecurityTask("kaset-resolve-review", "scan_current", security.StageReview, completed)
	threatTask.Spec.Workspace = repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead)
	reviewTask.Labels[labels.LabelSecuritySliceID] = "slice_api"
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: testPatchForgeSecretName, Namespace: defaultNS}, Data: map[string][]byte{defaultACPWorkspaceCredentialKey: []byte("forge-token-value")}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan, secret, threatTask, mapperTask, reviewTask).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: securityStore, GitHubAPIBaseURL: server.URL, HTTPClient: server.Client()}
	run := &storepkg.ScanRun{ID: "scan_current", Namespace: defaultNS, RepositoryScan: scan.Name, TaskName: threatTask.Name, Mode: "initial", Phase: scanRunPhaseRunning, ReviewedSliceCount: 1, HeadCommit: testRepositoryScanHeadSHA, StartedAt: time.Now()}
	if err := securityStore.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	reviewSlice := reviewedSliceWithContext(t, scan.Name, run.ID, "slice_api",
		"fnd_merged.go",
		"fnd_open_pr.go",
		"fnd_observed.go",
		"fnd_wrong_base.go",
		"fnd_merge_after_scan.go",
		"fnd_concurrent_decision.go",
	)
	if err := securityStore.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}
	targetKey := security.FindingV2TargetKey(scan.Spec.RepoURL, trustedFindingsBranch(scan), scan.Spec.SubPath)
	newFinding := func(id, scanRunID string, prNumber int) *storepkg.Finding {
		return &storepkg.Finding{ID: id, Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: scanRunID, SliceID: "slice_api", Fingerprint: id, TargetKey: targetKey, Title: id, Summary: id, Severity: "high", Confidence: "high", ValidationStatus: findingValidationStatusValidated, State: findingStatePROpen, FilePath: id + ".go", Line: 1, PRNumber: &prNumber}
	}
	for _, finding := range []*storepkg.Finding{
		newFinding("fnd_merged", "scan_old", 42),
		newFinding("fnd_open_pr", "scan_old", 43),
		newFinding("fnd_observed", run.ID, 44),
		newFinding("fnd_wrong_base", "scan_old", 45),
		newFinding("fnd_merge_after_scan", "scan_old", 46),
		newFinding("fnd_concurrent_decision", "scan_old", 42),
	} {
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
		}
	}
	if err := reconciler.refreshScanRunStatus(ctx, scan, run, run.ID, false); err != nil {
		t.Fatalf("refreshScanRunStatus() error = %v", err)
	}

	for id, want := range map[string]string{"fnd_merged": findingStateResolved, "fnd_open_pr": findingStatePROpen, "fnd_observed": findingStatePROpen, "fnd_wrong_base": findingStatePROpen, "fnd_merge_after_scan": findingStatePROpen, "fnd_concurrent_decision": "dismissed"} {
		finding, err := securityStore.GetFinding(ctx, defaultNS, id)
		if err != nil || finding.State != want {
			t.Fatalf("finding %s = %#v, err %v, want state %s", id, finding, err, want)
		}
	}
	concurrentFinding, concurrentFindingErr := securityStore.GetFinding(ctx, defaultNS, "fnd_concurrent_decision")
	if pullRequests[42] != 1 || pullRequests[43] != 1 || pullRequests[44] != 0 || pullRequests[45] != 1 || pullRequests[46] != 1 ||
		compareRequests[testRepositoryScanMergeSHA+"..."+testRepositoryScanHeadSHA] != 1 || compareRequests[testRepositoryScanLateMerge+"..."+testRepositoryScanHeadSHA] != 1 ||
		concurrentDecisionErr != nil || concurrentFindingErr != nil || concurrentFinding.DecisionAt.IsZero() {
		t.Fatalf("pull request reads = %#v, comparisons = %#v, concurrent decision error = %v, concurrent finding = %#v, finding error = %v", pullRequests, compareRequests, concurrentDecisionErr, concurrentFinding, concurrentFindingErr)
	}
}

func TestRefreshScanRunStatusUsesFrozenTaskTargetAfterSpecChanges(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	requests := map[int]int{}
	compareRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/example/kaset/compare/"+testRepositoryScanMergeSHA+"..."+testRepositoryScanHeadSHA {
			compareRequests++
			_, _ = w.Write([]byte(`{"status":"ahead"}`))
			return
		}
		var prNumber int
		if _, err := fmt.Sscanf(r.URL.Path, "/repos/example/kaset/pulls/%d", &prNumber); err != nil {
			t.Fatalf("unexpected GitHub path %s", r.URL.Path)
		}
		requests[prNumber]++
		base := "main"
		if prNumber == 43 {
			base = "release"
		}
		_, _ = fmt.Fprintf(w, `{"merged":true,"merged_at":"2026-08-30T12:00:00Z","merge_commit_sha":%q,"base":{"ref":%q}}`, testRepositoryScanMergeSHA, base)
	}))
	t.Cleanup(server.Close)

	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:            "https://github.com/example/kaset",
			Branch:             "release",
			SubPath:            "services/new",
			ForgeCredentialRef: &corev1.LocalObjectReference{Name: testPatchForgeSecretName},
		},
	}
	frozenScan := scan.DeepCopy()
	frozenScan.Spec.Branch = "main"
	frozenScan.Spec.SubPath = "services/old"
	completed := metav1.Now()
	threatTask := newSucceededSecurityTask("kaset-frozen-threat", "scan_frozen", security.StageThreatModel, completed)
	threatTask.Spec.Workspace = repositoryScanTaskWorkspace(frozenScan, corev1alpha1.WorkspaceIntentRead)
	mapperTask := newSucceededSecurityTask("kaset-frozen-mapper", "scan_frozen", security.StageMapper, completed)
	reviewTask := newSucceededSecurityTask("kaset-frozen-review", "scan_frozen", security.StageReview, completed)
	reviewTask.Labels[labels.LabelSecuritySliceID] = "slice_api"
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: testPatchForgeSecretName, Namespace: defaultNS}, Data: map[string][]byte{defaultACPWorkspaceCredentialKey: []byte("forge-token-value")}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan, secret, threatTask, mapperTask, reviewTask).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: securityStore, GitHubAPIBaseURL: server.URL, HTTPClient: server.Client()}
	run := &storepkg.ScanRun{ID: "scan_frozen", Namespace: defaultNS, RepositoryScan: scan.Name, TaskName: threatTask.Name, Mode: "initial", Phase: scanRunPhaseRunning, ReviewedSliceCount: 1, HeadCommit: testRepositoryScanHeadSHA, StartedAt: time.Now()}
	if err := securityStore.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	reviewSlice := reviewedSliceWithContext(t, scan.Name, run.ID, "slice_api", "fnd_frozen_target.go", "fnd_current_spec.go")
	if err := securityStore.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}
	for _, candidate := range []struct {
		id        string
		pr        int
		targetKey string
	}{
		{id: "fnd_frozen_target", pr: 42, targetKey: security.FindingV2TargetKey(scan.Spec.RepoURL, "main", "services/old")},
		{id: "fnd_current_spec", pr: 43, targetKey: security.FindingV2TargetKey(scan.Spec.RepoURL, "release", "services/new")},
	} {
		prNumber := candidate.pr
		finding := &storepkg.Finding{ID: candidate.id, Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: "scan_old", SliceID: "slice_api", Fingerprint: candidate.id, TargetKey: candidate.targetKey, Title: candidate.id, Summary: candidate.id, Severity: "high", Confidence: "high", ValidationStatus: findingValidationStatusValidated, State: findingStatePROpen, FilePath: candidate.id + ".go", Line: 1, PRNumber: &prNumber}
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", candidate.id, err)
		}
	}

	if err := reconciler.refreshScanRunStatus(ctx, scan, run, run.ID, false); err != nil {
		t.Fatalf("refreshScanRunStatus() error = %v", err)
	}
	frozenFinding, _ := securityStore.GetFinding(ctx, defaultNS, "fnd_frozen_target")
	currentFinding, _ := securityStore.GetFinding(ctx, defaultNS, "fnd_current_spec")
	if frozenFinding.State != findingStateResolved || currentFinding.State != findingStatePROpen || requests[42] != 1 || requests[43] != 0 || compareRequests != 1 {
		t.Fatalf("frozen = %#v, current = %#v, requests = %#v, comparisons = %d", frozenFinding, currentFinding, requests, compareRequests)
	}
}

func TestRefreshScanRunStatusRetriesMergedPRLookup(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	pullAttempts := 0
	compareAttempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/example/kaset/compare/"+testRepositoryScanMergeSHA+"..."+testRepositoryScanHeadSHA {
			compareAttempts++
			_, _ = w.Write([]byte(`{"status":"ahead"}`))
			return
		}
		if r.URL.Path != "/repos/example/kaset/pulls/42" {
			t.Fatalf("unexpected GitHub path %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer forge-token-value" {
			t.Fatalf("Authorization = %q", r.Header.Get("Authorization"))
		}
		pullAttempts++
		if pullAttempts == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"bad credentials"}`))
			return
		}
		_, _ = fmt.Fprintf(w, `{"merged":true,"merged_at":"2026-08-30T12:00:00Z","merge_commit_sha":%q,"base":{"ref":"main"}}`, testRepositoryScanMergeSHA)
	}))
	t.Cleanup(server.Close)

	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:            "https://github.com/example/kaset",
			ForgeCredentialRef: &corev1.LocalObjectReference{Name: testPatchForgeSecretName},
		},
	}
	completed := metav1.Now()
	threatTask := newSucceededSecurityTask("kaset-retry-threat", "scan_retry", security.StageThreatModel, completed)
	mapperTask := newSucceededSecurityTask("kaset-retry-mapper", "scan_retry", security.StageMapper, completed)
	reviewTask := newSucceededSecurityTask("kaset-retry-review", "scan_retry", security.StageReview, completed)
	threatTask.Spec.Workspace = repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead)
	reviewTask.Labels[labels.LabelSecuritySliceID] = "slice_api"
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: testPatchForgeSecretName, Namespace: defaultNS}, Data: map[string][]byte{defaultACPWorkspaceCredentialKey: []byte("forge-token-value")}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret, threatTask, mapperTask, reviewTask).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: securityStore, GitHubAPIBaseURL: server.URL, HTTPClient: server.Client()}
	run := &storepkg.ScanRun{ID: "scan_retry", Namespace: defaultNS, RepositoryScan: scan.Name, TaskName: threatTask.Name, Mode: "initial", Phase: scanRunPhaseRunning, ReviewedSliceCount: 1, HeadCommit: testRepositoryScanHeadSHA, StartedAt: time.Now()}
	if err := securityStore.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	reviewSlice := reviewedSliceWithContext(t, scan.Name, run.ID, "slice_api", "api.go")
	if err := securityStore.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}
	prNumber := 42
	finding := &storepkg.Finding{ID: "fnd_retry", Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: "scan_old", SliceID: "slice_api", Fingerprint: "fnd_retry", TargetKey: security.FindingV2TargetKey(scan.Spec.RepoURL, trustedFindingsBranch(scan), scan.Spec.SubPath), Title: "finding", Summary: "finding", Severity: "high", Confidence: "high", ValidationStatus: findingValidationStatusValidated, State: findingStatePROpen, FilePath: "api.go", Line: 1, PRNumber: &prNumber}
	if err := securityStore.UpsertFinding(ctx, finding); err != nil {
		t.Fatalf("UpsertFinding() error = %v", err)
	}

	if err := reconciler.refreshScanRunStatus(ctx, scan, run, run.ID, false); err == nil {
		t.Fatal("refreshScanRunStatus() error = nil, want transient GitHub failure")
	}
	persistedRun, err := securityStore.GetScanRun(ctx, defaultNS, run.ID)
	if err != nil || persistedRun.Phase != scanRunPhaseRunning {
		t.Fatalf("persisted run after transient failure = %#v, err %v", persistedRun, err)
	}
	if err := reconciler.refreshScanRunStatus(ctx, scan, persistedRun, persistedRun.ID, false); err != nil {
		t.Fatalf("refreshScanRunStatus(retry) error = %v", err)
	}
	got, err := securityStore.GetFinding(ctx, defaultNS, finding.ID)
	if err != nil || got.State != findingStateResolved || pullAttempts != 2 || compareAttempts != 1 {
		t.Fatalf("finding = %#v, pull attempts = %d, compare attempts = %d, err %v", got, pullAttempts, compareAttempts, err)
	}
}

func TestRefreshScanRunStatusRetriesMergedResolutionAfterForgeCredentialReturns(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	pullRequests := 0
	compareRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/example/kaset/pulls/42":
			pullRequests++
			_, _ = fmt.Fprintf(w, `{"merged":true,"merged_at":"2026-08-30T12:00:00Z","merge_commit_sha":%q,"base":{"ref":"main"}}`, testRepositoryScanMergeSHA)
		case "/repos/example/kaset/compare/" + testRepositoryScanMergeSHA + "..." + testRepositoryScanHeadSHA:
			compareRequests++
			_, _ = w.Write([]byte(`{"status":"ahead"}`))
		default:
			t.Fatalf("unexpected GitHub path %s", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:            "https://github.com/example/kaset",
			ForgeCredentialRef: &corev1.LocalObjectReference{Name: testPatchForgeSecretName},
		},
	}
	completed := metav1.Now()
	threatTask := newSucceededSecurityTask("kaset-credential-retry-threat", "scan_credential_retry", security.StageThreatModel, completed)
	threatTask.Spec.Workspace = repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead)
	mapperTask := newSucceededSecurityTask("kaset-credential-retry-mapper", "scan_credential_retry", security.StageMapper, completed)
	reviewTask := newSucceededSecurityTask("kaset-credential-retry-review", "scan_credential_retry", security.StageReview, completed)
	reviewTask.Labels[labels.LabelSecuritySliceID] = "slice_api"
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(threatTask, mapperTask, reviewTask).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: securityStore, GitHubAPIBaseURL: server.URL, HTTPClient: server.Client()}
	run := &storepkg.ScanRun{ID: "scan_credential_retry", Namespace: defaultNS, RepositoryScan: scan.Name, TaskName: threatTask.Name, Mode: "initial", Phase: scanRunPhaseRunning, ReviewedSliceCount: 1, HeadCommit: testRepositoryScanHeadSHA, StartedAt: time.Now()}
	if err := securityStore.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	reviewSlice := reviewedSliceWithContext(t, scan.Name, run.ID, "slice_api", "api.go")
	if err := securityStore.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}
	prNumber := 42
	finding := &storepkg.Finding{ID: "fnd_credential_retry", Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: "scan_old", SliceID: "slice_api", Fingerprint: "fnd_credential_retry", TargetKey: security.FindingV2TargetKey(scan.Spec.RepoURL, trustedFindingsBranch(scan), scan.Spec.SubPath), Title: "finding", Summary: "finding", Severity: "high", Confidence: "high", ValidationStatus: findingValidationStatusValidated, State: findingStatePROpen, FilePath: "api.go", Line: 1, PRNumber: &prNumber}
	if err := securityStore.UpsertFinding(ctx, finding); err != nil {
		t.Fatalf("UpsertFinding() error = %v", err)
	}

	if err := reconciler.refreshScanRunStatus(ctx, scan, run, run.ID, false); err == nil || !strings.Contains(err.Error(), "waiting for forge credentials") {
		t.Fatalf("refreshScanRunStatus(without credential) error = %v, want retryable credential wait", err)
	}
	persistedRun, err := securityStore.GetScanRun(ctx, defaultNS, run.ID)
	if err != nil || persistedRun.Phase != scanRunPhaseRunning || pullRequests != 0 {
		t.Fatalf("run without credential = %#v, pull requests = %d, err %v", persistedRun, pullRequests, err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: testPatchForgeSecretName, Namespace: defaultNS}, Data: map[string][]byte{defaultACPWorkspaceCredentialKey: []byte("forge-token-value")}}
	if err := cl.Create(ctx, secret); err != nil {
		t.Fatalf("Create(forge secret) error = %v", err)
	}
	if err := reconciler.refreshScanRunStatus(ctx, scan, persistedRun, persistedRun.ID, false); err != nil {
		t.Fatalf("refreshScanRunStatus(with credential) error = %v", err)
	}
	resolved, err := securityStore.GetFinding(ctx, defaultNS, finding.ID)
	if err != nil || resolved.State != findingStateResolved || pullRequests != 1 || compareRequests != 1 {
		t.Fatalf("finding = %#v, pull requests = %d, comparisons = %d, err %v", resolved, pullRequests, compareRequests, err)
	}
}

type failingRepositoryScanReader struct {
	client.Reader
	err error
}

func (r failingRepositoryScanReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return r.err
}

func TestResolveMergedFindingsRetriesForgeCredentialReadErrors(t *testing.T) {
	ctx := context.Background()
	transientErr := errors.New("transient secret read failure")
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:            "https://github.com/example/kaset",
			ForgeCredentialRef: &corev1.LocalObjectReference{Name: testPatchForgeSecretName},
		},
	}
	run := &storepkg.ScanRun{ID: "scan_retry_secret", Namespace: defaultNS, RepositoryScan: scan.Name, Mode: "initial", Phase: scanRunPhaseSucceeded, ReviewedSliceCount: 1}
	securityStore := setupControllerSQLiteStore(t)
	reviewSlice := reviewedSliceWithContext(t, scan.Name, run.ID, "slice_api", "api.go")
	if err := securityStore.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}
	prNumber := 42
	if err := securityStore.UpsertFinding(ctx, &storepkg.Finding{ID: "fnd_retry_secret", Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: "scan_old", SliceID: "slice_api", Fingerprint: "fnd_retry_secret", TargetKey: security.FindingV2TargetKey(scan.Spec.RepoURL, trustedFindingsBranch(scan), scan.Spec.SubPath), Title: "finding", Summary: "finding", Severity: "high", Confidence: "high", ValidationStatus: findingValidationStatusValidated, State: findingStatePROpen, FilePath: "api.go", Line: 1, PRNumber: &prNumber}); err != nil {
		t.Fatalf("UpsertFinding() error = %v", err)
	}
	reconciler := &RepositoryScanReconciler{
		APIReader:     failingRepositoryScanReader{err: transientErr},
		SecurityStore: securityStore,
	}

	if err := reconciler.resolveMergedFindingsNotObserved(ctx, scan, run, trustedFindingsRepository(scan, run)); !errors.Is(err, transientErr) {
		t.Fatalf("resolveMergedFindingsNotObserved() error = %v, want %v", err, transientErr)
	}
}

func TestResolveMergedFindingsScopesRunToReviewedSlices(t *testing.T) {
	for _, mode := range []string{"initial", scanModeIncremental} {
		t.Run(mode, func(t *testing.T) {
			testResolveMergedFindingsScopesRunToReviewedSlices(t, mode)
		})
	}
}

func testResolveMergedFindingsScopesRunToReviewedSlices(t *testing.T, mode string) {
	t.Helper()
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	requests := map[int]int{}
	compareRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/example/kaset/compare/"+testRepositoryScanMergeSHA+"..."+testRepositoryScanHeadSHA {
			compareRequests++
			_, _ = w.Write([]byte(`{"status":"ahead"}`))
			return
		}
		var prNumber int
		if _, err := fmt.Sscanf(r.URL.Path, "/repos/example/kaset/pulls/%d", &prNumber); err != nil {
			t.Fatalf("unexpected GitHub path %s", r.URL.Path)
		}
		requests[prNumber]++
		_, _ = fmt.Fprintf(w, `{"merged":true,"merged_at":"2026-08-30T12:00:00Z","merge_commit_sha":%q,"base":{"ref":"main"}}`, testRepositoryScanMergeSHA)
	}))
	t.Cleanup(server.Close)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1alpha1.AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}, Spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/kaset", ForgeCredentialRef: &corev1.LocalObjectReference{Name: testPatchForgeSecretName}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: testPatchForgeSecretName, Namespace: defaultNS}, Data: map[string][]byte{defaultACPWorkspaceCredentialKey: []byte("forge-token-value")}}
	legacyTask := newSucceededSecurityTask("legacy-resolution-review", "scan_old", security.StageReview, metav1.Now())
	legacyTask.Spec.Workspace = repositoryScanTaskWorkspace(scan, corev1alpha1.WorkspaceIntentRead)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret, legacyTask).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, SecurityStore: securityStore, GitHubAPIBaseURL: server.URL, HTTPClient: server.Client()}
	run := &storepkg.ScanRun{ID: "scan_scoped", Namespace: defaultNS, RepositoryScan: scan.Name, Mode: mode, Phase: scanRunPhaseSucceeded, HeadCommit: testRepositoryScanHeadSHA}
	if err := securityStore.CreateScanRun(ctx, &storepkg.ScanRun{ID: "scan_old", Namespace: defaultNS, RepositoryScan: scan.Name, TaskName: legacyTask.Name, Mode: "initial", Phase: scanRunPhaseSucceeded, StartedAt: time.Now()}); err != nil {
		t.Fatalf("CreateScanRun(legacy) error = %v", err)
	}
	reviewSlice := reviewedSliceWithContext(t, scan.Name, run.ID, "slice_reviewed", "reviewed.go", "legacy.go")
	if err := securityStore.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}
	currentTargetKey := security.FindingV2TargetKey(scan.Spec.RepoURL, trustedFindingsBranch(scan), scan.Spec.SubPath)
	for _, candidate := range []struct {
		id        string
		sliceID   string
		pr        int
		targetKey string
		filePath  string
		line      int
	}{
		{id: "fnd_reviewed", sliceID: "slice_reviewed", pr: 42, targetKey: currentTargetKey, filePath: "reviewed.go", line: 10},
		{id: "fnd_unreviewed", sliceID: "slice_other", pr: 43, targetKey: currentTargetKey, filePath: "unreviewed.go", line: 10},
		{id: "fnd_old_target", sliceID: "slice_reviewed", pr: 44, targetKey: security.FindingV2TargetKey(scan.Spec.RepoURL, "release", scan.Spec.SubPath), filePath: "reviewed.go", line: 10},
		{id: "fnd_uncovered", sliceID: "slice_reviewed", pr: 46, targetKey: currentTargetKey, filePath: "reviewed.go", line: 10001},
	} {
		prNumber := candidate.pr
		finding := &storepkg.Finding{ID: candidate.id, Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: "scan_old", SliceID: candidate.sliceID, Fingerprint: candidate.id, TargetKey: candidate.targetKey, Title: candidate.id, Summary: candidate.id, Severity: "high", Confidence: "high", ValidationStatus: findingValidationStatusValidated, State: findingStatePROpen, FilePath: candidate.filePath, Line: candidate.line, PRNumber: &prNumber}
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
		}
	}
	legacyPRNumber := 45
	legacy := &storepkg.Finding{
		ID:               "fnd_legacy_target",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_old",
		SliceID:          "slice_reviewed",
		Title:            "legacy finding",
		Category:         "legacy category",
		Summary:          "legacy finding",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusValidated,
		State:            findingStatePROpen,
		FilePath:         "legacy.go",
		Line:             10,
		PRNumber:         &legacyPRNumber,
	}
	legacyRepo := trustedFindingsRepository(scan, nil)
	legacySymbol := "legacyHandler"
	legacy.Evidence = []storepkg.FindingEvidenceRef{{Path: "legacy.go", StartLine: 10, EndLine: 15, Symbol: legacySymbol}}
	legacy.Fingerprint = security.FindingV2Fingerprint(
		legacy.Namespace,
		legacy.RepositoryScan,
		legacyRepo.RepoURL,
		legacyRepo.Branch,
		legacyRepo.SubPath,
		legacy.SliceID,
		security.FindingsV2Finding{Title: legacy.Title, Category: legacy.Category, Evidence: []security.FindingsV2EvidenceRef{{Path: "legacy.go", StartLine: 10, EndLine: 15, Symbol: &legacySymbol}}},
	)
	legacy.Evidence = append(legacy.Evidence, storepkg.FindingEvidenceRef{Path: "legacy.go", StartLine: 30, EndLine: 34, Symbol: "newEvidence"})
	if err := securityStore.UpsertFinding(ctx, legacy); err != nil {
		t.Fatalf("UpsertFinding(%s) error = %v", legacy.ID, err)
	}
	if err := reconciler.resolveMergedFindingsNotObserved(ctx, scan, run, trustedFindingsRepository(scan, run)); err != nil {
		t.Fatalf("resolveMergedFindingsNotObserved() error = %v", err)
	}
	reviewed, _ := securityStore.GetFinding(ctx, defaultNS, "fnd_reviewed")
	unreviewed, _ := securityStore.GetFinding(ctx, defaultNS, "fnd_unreviewed")
	oldTarget, _ := securityStore.GetFinding(ctx, defaultNS, "fnd_old_target")
	uncovered, _ := securityStore.GetFinding(ctx, defaultNS, "fnd_uncovered")
	legacyTarget, _ := securityStore.GetFinding(ctx, defaultNS, legacy.ID)
	if reviewed.State != findingStateResolved || unreviewed.State != findingStatePROpen || oldTarget.State != findingStatePROpen || uncovered.State != findingStatePROpen || legacyTarget.State != findingStateResolved || requests[42] != 1 || requests[43] != 0 || requests[44] != 0 || requests[45] != 1 || requests[46] != 0 || compareRequests != 2 {
		t.Fatalf("reviewed = %#v, unreviewed = %#v, old target = %#v, uncovered = %#v, legacy target = %#v, requests = %#v, comparisons = %d", reviewed, unreviewed, oldTarget, uncovered, legacyTarget, requests, compareRequests)
	}
}

func TestResolveMergedFindingsCollectsAllPagesBeforeMutation(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	pullRequests := 0
	compareRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/example/kaset/compare/"+testRepositoryScanMergeSHA+"..."+testRepositoryScanHeadSHA {
			compareRequests++
			_, _ = w.Write([]byte(`{"status":"ahead"}`))
			return
		}
		if r.URL.Path != "/repos/example/kaset/pulls/42" {
			t.Fatalf("unexpected GitHub path %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer forge-token-value" {
			t.Fatalf("Authorization = %q", r.Header.Get("Authorization"))
		}
		pullRequests++
		_, _ = fmt.Fprintf(w, `{"merged":true,"merged_at":"2026-08-30T12:00:00Z","merge_commit_sha":%q,"base":{"ref":"main"}}`, testRepositoryScanMergeSHA)
	}))
	t.Cleanup(server.Close)

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:            "https://github.com/example/kaset",
			ForgeCredentialRef: &corev1.LocalObjectReference{Name: testPatchForgeSecretName},
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: testPatchForgeSecretName, Namespace: defaultNS}, Data: map[string][]byte{defaultACPWorkspaceCredentialKey: []byte("forge-token-value")}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	reconciler := &RepositoryScanReconciler{Client: kubeClient, SecurityStore: securityStore, GitHubAPIBaseURL: server.URL, HTTPClient: server.Client()}
	run := &storepkg.ScanRun{ID: "scan_current", Namespace: defaultNS, RepositoryScan: scan.Name, Mode: "initial", Phase: scanRunPhaseSucceeded, ReviewedSliceCount: 1, HeadCommit: testRepositoryScanHeadSHA}
	reviewSlice := reviewedSliceWithContext(t, scan.Name, run.ID, "slice_all", "all.go")
	if err := securityStore.UpsertReviewSlice(ctx, reviewSlice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}
	for i := range 201 {
		id := fmt.Sprintf("fnd_page_%03d", i)
		prNumber := 42
		finding := &storepkg.Finding{
			ID:               id,
			Namespace:        defaultNS,
			RepositoryScan:   scan.Name,
			ScanRunID:        "scan_old",
			Fingerprint:      id,
			TargetKey:        security.FindingV2TargetKey(scan.Spec.RepoURL, trustedFindingsBranch(scan), scan.Spec.SubPath),
			Title:            id,
			Summary:          id,
			Severity:         "high",
			Confidence:       "high",
			ValidationStatus: findingValidationStatusValidated,
			State:            findingStatePROpen,
			SliceID:          "slice_all",
			FilePath:         "all.go",
			Line:             1,
			PRNumber:         &prNumber,
		}
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", id, err)
		}
	}

	if err := reconciler.resolveMergedFindingsNotObserved(ctx, scan, run, trustedFindingsRepository(scan, run)); err != nil {
		t.Fatalf("resolveMergedFindingsNotObserved() error = %v", err)
	}
	remaining, _, err := securityStore.ListFindings(ctx, storepkg.FindingFilter{Namespace: defaultNS, RepositoryScan: scan.Name, State: findingStatePROpen, Limit: 500})
	if err != nil || len(remaining) != 0 || pullRequests != 1 || compareRequests != 1 {
		t.Fatalf("remaining = %d, pull requests = %d, comparisons = %d, err %v", len(remaining), pullRequests, compareRequests, err)
	}
}

func TestResolveMergedFindingsSkipsRunWithCappedOutput(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}}
	run := &storepkg.ScanRun{ID: "scan_capped", Namespace: defaultNS, RepositoryScan: scan.Name, Mode: "initial", Phase: scanRunPhaseSucceeded, ReviewedSliceCount: 1}
	prNumber := 42
	finding := &storepkg.Finding{ID: "fnd_capped", Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: "scan_old", Fingerprint: "fnd_capped", Title: "finding", Summary: "finding", Severity: "high", Confidence: "high", ValidationStatus: findingValidationStatusValidated, State: findingStatePROpen, PRNumber: &prNumber}
	if err := securityStore.UpsertFinding(ctx, finding); err != nil {
		t.Fatalf("UpsertFinding() error = %v", err)
	}
	if err := securityStore.CreateDroppedFinding(ctx, &storepkg.DroppedFinding{ID: "drop_cap", Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: run.ID, TaskName: "review", Reason: "maxFindingsPerRun limit 10 reached", Layer: "cap"}); err != nil {
		t.Fatalf("CreateDroppedFinding() error = %v", err)
	}
	reconciler := &RepositoryScanReconciler{SecurityStore: securityStore}
	if err := reconciler.resolveMergedFindingsNotObserved(ctx, scan, run, trustedFindingsRepository(scan, run)); err != nil {
		t.Fatalf("resolveMergedFindingsNotObserved() error = %v", err)
	}
	got, err := securityStore.GetFinding(ctx, defaultNS, finding.ID)
	if err != nil || got.State != findingStatePROpen {
		t.Fatalf("finding = %#v, err %v, want unresolved", got, err)
	}
}

func TestResolveMergedFindingsSkipsSlicesWithDroppedResults(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	requests := map[int]int{}
	compareRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/example/kaset/compare/"+testRepositoryScanMergeSHA+"..."+testRepositoryScanHeadSHA {
			compareRequests++
			_, _ = w.Write([]byte(`{"status":"ahead"}`))
			return
		}
		var prNumber int
		if _, err := fmt.Sscanf(r.URL.Path, "/repos/example/kaset/pulls/%d", &prNumber); err != nil {
			t.Fatalf("unexpected GitHub path %s", r.URL.Path)
		}
		requests[prNumber]++
		_, _ = fmt.Fprintf(w, `{"merged":true,"merged_at":"2026-08-30T12:00:00Z","merge_commit_sha":%q,"base":{"ref":"main"}}`, testRepositoryScanMergeSHA)
	}))
	t.Cleanup(server.Close)

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS}, Spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/kaset", ForgeCredentialRef: &corev1.LocalObjectReference{Name: testPatchForgeSecretName}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: testPatchForgeSecretName, Namespace: defaultNS}, Data: map[string][]byte{defaultACPWorkspaceCredentialKey: []byte("forge-token-value")}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, SecurityStore: securityStore, GitHubAPIBaseURL: server.URL, HTTPClient: server.Client()}
	run := &storepkg.ScanRun{ID: "scan_dropped", Namespace: defaultNS, RepositoryScan: scan.Name, Mode: "initial", Phase: scanRunPhaseSucceeded, ReviewedSliceCount: 2, HeadCommit: testRepositoryScanHeadSHA}
	for sliceID, path := range map[string]string{"slice_dropped": "fnd_dropped_slice.go", "slice_clean": "fnd_clean_slice.go"} {
		if err := securityStore.UpsertReviewSlice(ctx, reviewedSliceWithContext(t, scan.Name, run.ID, sliceID, path)); err != nil {
			t.Fatalf("UpsertReviewSlice(%s) error = %v", sliceID, err)
		}
	}
	for _, candidate := range []struct {
		id      string
		sliceID string
		pr      int
	}{
		{id: "fnd_dropped_slice", sliceID: "slice_dropped", pr: 42},
		{id: "fnd_clean_slice", sliceID: "slice_clean", pr: 43},
	} {
		prNumber := candidate.pr
		finding := &storepkg.Finding{ID: candidate.id, Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: "scan_old", SliceID: candidate.sliceID, Fingerprint: candidate.id, TargetKey: security.FindingV2TargetKey(scan.Spec.RepoURL, trustedFindingsBranch(scan), scan.Spec.SubPath), Title: candidate.id, Summary: candidate.id, Severity: "high", Confidence: "high", ValidationStatus: findingValidationStatusValidated, State: findingStatePROpen, FilePath: candidate.id + ".go", Line: 1, PRNumber: &prNumber}
		if err := securityStore.UpsertFinding(ctx, finding); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", finding.ID, err)
		}
	}
	if err := securityStore.CreateDroppedFinding(ctx, &storepkg.DroppedFinding{ID: "drop_validation", Namespace: defaultNS, RepositoryScan: scan.Name, ScanRunID: run.ID, TaskName: "review", SliceID: "slice_dropped", Reason: "evidence quote does not match cited file range", Layer: "validation"}); err != nil {
		t.Fatalf("CreateDroppedFinding() error = %v", err)
	}
	if err := reconciler.resolveMergedFindingsNotObserved(ctx, scan, run, trustedFindingsRepository(scan, run)); err != nil {
		t.Fatalf("resolveMergedFindingsNotObserved() error = %v", err)
	}
	droppedSlice, _ := securityStore.GetFinding(ctx, defaultNS, "fnd_dropped_slice")
	cleanSlice, _ := securityStore.GetFinding(ctx, defaultNS, "fnd_clean_slice")
	if droppedSlice.State != findingStatePROpen || cleanSlice.State != findingStateResolved || requests[42] != 0 || requests[43] != 1 || compareRequests != 1 {
		t.Fatalf("dropped = %#v, clean = %#v, requests = %#v, comparisons = %d", droppedSlice, cleanSlice, requests, compareRequests)
	}
}

func TestRefreshScanRunStatusSetsLastScanAtOnFailedRun(t *testing.T) {
	ctx := context.Background()
	secStore := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "ts-fail", Namespace: defaultNS},
		Spec:       corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo", AnalysisAgentRef: corev1alpha1.AgentReference{Name: "a"}},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(scan).Build()
	r := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: secStore}

	completed := mustParseTime(t, "2026-05-07T22:41:22Z")
	run := &storepkg.ScanRun{ID: "scan_f", Namespace: defaultNS, RepositoryScan: "ts-fail", TaskName: "t", Mode: "initial", Phase: scanRunPhaseFailed, StartedAt: completed, CompletedAt: &completed, ErrorMessage: "failed", HeadCommit: "abc"}
	if err := secStore.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	if err := r.refreshScanRunStatus(ctx, scan, run, run.ID, true); err != nil {
		t.Fatalf("refreshScanRunStatus() error = %v", err)
	}

	current := &corev1alpha1.RepositoryScan{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
		t.Fatalf("cl.Get() error = %v", err)
	}
	if current.Status.LastScanAt == nil || !current.Status.LastScanAt.Time.Equal(completed) {
		t.Fatalf("LastScanAt = %v, want %v", current.Status.LastScanAt, completed)
	}
	if current.Status.LastSuccessfulScanAt != nil {
		t.Fatalf("LastSuccessfulScanAt = %v, want nil for failed scan", current.Status.LastSuccessfulScanAt)
	}
}

func TestRefreshScanRunStatusSetsBothTimestampsOnSuccess(t *testing.T) {
	ctx := context.Background()
	secStore := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "ts-ok", Namespace: defaultNS},
		Spec:       corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo", AnalysisAgentRef: corev1alpha1.AgentReference{Name: "a"}},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(scan).Build()
	r := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: secStore}

	completed := mustParseTime(t, "2026-05-07T23:00:00Z")
	run := &storepkg.ScanRun{ID: "scan_s", Namespace: defaultNS, RepositoryScan: "ts-ok", TaskName: "t", Mode: "initial", Phase: scanRunPhaseSucceeded, StartedAt: completed, CompletedAt: &completed, HeadCommit: "def"}
	if err := secStore.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	if err := r.refreshScanRunStatus(ctx, scan, run, run.ID, true); err != nil {
		t.Fatalf("refreshScanRunStatus() error = %v", err)
	}

	current := &corev1alpha1.RepositoryScan{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
		t.Fatalf("cl.Get() error = %v", err)
	}
	if current.Status.LastScanAt == nil || !current.Status.LastScanAt.Time.Equal(completed) {
		t.Fatalf("LastScanAt = %v, want %v", current.Status.LastScanAt, completed)
	}
	if current.Status.LastSuccessfulScanAt == nil || !current.Status.LastSuccessfulScanAt.Time.Equal(completed) {
		t.Fatalf("LastSuccessfulScanAt = %v, want %v", current.Status.LastSuccessfulScanAt, completed)
	}
}

func setupControllerSQLiteStore(t *testing.T) *sqlitestore.Store {
	t.Helper()

	db, err := sqlitestore.NewDB(":memory:")
	if err != nil {
		t.Fatalf("sqlite.NewDB(:memory:) error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return sqlitestore.NewStore(db, ":memory:")
}
func TestShouldAutoValidateFindingHonorsModeAndThresholds(t *testing.T) {
	reconciler := &RepositoryScanReconciler{}
	maxOne := int32(1)
	scan := &corev1alpha1.RepositoryScan{Spec: corev1alpha1.RepositoryScanSpec{
		ValidationMode:              "light",
		ValidationMaxFindingsPerRun: &maxOne,
		ValidationMinSeverity:       "medium",
		ValidationMinConfidence:     "medium",
	}}
	finding := &storepkg.Finding{Severity: "medium", Confidence: "low"}
	if !reconciler.shouldAutoValidateFinding(scan, finding, 0) {
		t.Fatal("shouldAutoValidateFinding() = false, want true for medium severity threshold")
	}
	if reconciler.shouldAutoValidateFinding(scan, finding, 1) {
		t.Fatal("shouldAutoValidateFinding() = true, want false after validation cap")
	}
	scan.Spec.ValidationMode = "off"
	if reconciler.shouldAutoValidateFinding(scan, finding, 0) {
		t.Fatal("shouldAutoValidateFinding() = true, want false when validation is off")
	}
	scan.Spec.ValidationMode = "full"
	scan.Spec.ValidationMinSeverity = ""
	scan.Spec.ValidationMinConfidence = ""
	finding.Severity = "critical"
	finding.Confidence = "low"
	if !reconciler.shouldAutoValidateFinding(scan, finding, 99) {
		t.Fatal("shouldAutoValidateFinding() = false, want true for default full mode regardless of light cap")
	}
	scan.Spec.ValidationMinSeverity = "high"
	scan.Spec.ValidationMinConfidence = "medium"
	if reconciler.shouldAutoValidateFinding(scan, finding, 0) {
		t.Fatal("shouldAutoValidateFinding() = true, want false below full-mode severity threshold")
	}
	finding.Severity = "critical"
	finding.Confidence = "medium"
	if !reconciler.shouldAutoValidateFinding(scan, finding, 99) {
		t.Fatal("shouldAutoValidateFinding() = false, want true for full mode above thresholds regardless of per-task cap")
	}
}

func TestEnqueueAutoValidationTasksScopesActiveTaskToFindingOccurrence(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          "https://github.com/example/repo",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"},
			ValidationMode:   validationModeFull,
		},
	}
	priorTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "prior-validation",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget:    labels.SelectorValue(scan.Name),
				labels.LabelSecurityStage:     security.StageValidation,
				labels.LabelSecurityScanID:    "scan_old",
				labels.LabelSecurityFindingID: "fnd_reopened",
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan, priorTask).Build()
	securityStore := setupControllerSQLiteStore(t)
	priorFinding := &storepkg.Finding{
		ID:               "fnd_reopened",
		Namespace:        defaultNS,
		RepositoryScan:   scan.Name,
		ScanRunID:        "scan_old",
		Fingerprint:      "reopened-finding",
		Title:            "Reopened finding",
		Severity:         "high",
		Confidence:       "high",
		ValidationStatus: findingValidationStatusPending,
		ValidationJSON:   `{"status":"pending"}`,
		State:            findingStateOpen,
	}
	if err := securityStore.UpsertFinding(ctx, priorFinding); err != nil {
		t.Fatalf("UpsertFinding() error = %v", err)
	}
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: securityStore}
	finding := *priorFinding
	finding.ScanRunID = "scan_current"
	finding.ValidationStatus = "unvalidated"
	finding.ValidationJSON = ""
	if err := reconciler.mergeExistingFinding(ctx, scan, &finding); err != nil {
		t.Fatalf("mergeExistingFinding() error = %v", err)
	}
	if err := securityStore.UpsertObservedFinding(ctx, &finding); err != nil {
		t.Fatalf("UpsertObservedFinding() error = %v", err)
	}
	if finding.ValidationStatus != "unvalidated" || finding.ValidationJSON != "" {
		t.Fatalf("current validation = %q/%q, want prior pending state reset", finding.ValidationStatus, finding.ValidationJSON)
	}
	observed, err := securityStore.GetFinding(ctx, defaultNS, finding.ID)
	if err != nil || observed.ScanRunID != finding.ScanRunID || observed.ValidationStatus != "unvalidated" || observed.ValidationJSON != "" {
		t.Fatalf("observed finding = %#v, err %v, want current occurrence unvalidated", observed, err)
	}

	if err := reconciler.enqueueAutoValidationTasks(ctx, scan, []*storepkg.Finding{&finding}); err != nil {
		t.Fatalf("enqueueAutoValidationTasks() error = %v", err)
	}
	var tasks corev1alpha1.TaskList
	if err := cl.List(ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Task) error = %v", err)
	}
	if len(tasks.Items) != 2 {
		t.Fatalf("validation tasks = %d, want prior and current occurrences", len(tasks.Items))
	}
	var currentTask *corev1alpha1.Task
	for i := range tasks.Items {
		if tasks.Items[i].Labels[labels.LabelSecurityScanID] == finding.ScanRunID {
			currentTask = &tasks.Items[i]
			break
		}
	}
	if currentTask == nil {
		t.Fatal("current validation task was not created")
	}
	stored, err := securityStore.GetFinding(ctx, defaultNS, finding.ID)
	if err != nil || stored.ValidationStatus != findingValidationStatusPending {
		t.Fatalf("finding = %#v, err %v, want current validation pending", stored, err)
	}
}

func TestEnqueueAutoValidationTasksHonorsRunCapAcrossExistingTasks(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	maxOne := int32(1)
	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:                     "https://github.com/example/repo",
			AnalysisAgentRef:            corev1alpha1.AgentReference{Name: "scan-reviewer"},
			ValidationMode:              "light",
			ValidationMaxFindingsPerRun: &maxOne,
			ValidationMinSeverity:       "high",
			ValidationMinConfidence:     "high",
		},
	}
	existing := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "existing-validation",
			Namespace: defaultNS,
			Labels: map[string]string{
				labels.LabelSecurityTarget: labels.SelectorValue(scan.Name),
				labels.LabelSecurityStage:  security.StageValidation,
				labels.LabelSecurityScanID: "scan_run",
			},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan, existing).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme}
	findings := []*storepkg.Finding{{ID: "fnd_new", Namespace: defaultNS, RepositoryScan: "kaset", ScanRunID: "scan_run", Severity: "critical", Confidence: "high"}}
	if err := reconciler.enqueueAutoValidationTasks(ctx, scan, findings); err != nil {
		t.Fatalf("enqueueAutoValidationTasks() error = %v", err)
	}
	var tasks corev1alpha1.TaskList
	if err := cl.List(ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Task) error = %v", err)
	}
	if len(tasks.Items) != 1 {
		t.Fatalf("validation tasks = %d, want existing task only due run cap", len(tasks.Items))
	}
}

func TestRepositoryScanPolicyDigestDriftFailsReviewTaskCreation(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:                   "https://github.com/example/repo",
			AnalysisAgentRef:          corev1alpha1.AgentReference{Name: "scan-reviewer"},
			CustomScanInstructionsRef: &corev1alpha1.PolicyConfigMapKeyRef{Name: "scan-policy"},
		},
	}
	policyConfig := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "scan-policy", Namespace: defaultNS, Labels: map[string]string{security.PolicyConfigMapAllowedLabel: "true"}}, Data: map[string]string{"policy": "new policy text"}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(scan, policyConfig).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: store}
	run := &storepkg.ScanRun{ID: "scan_policy", Namespace: defaultNS, RepositoryScan: "kaset", Mode: "initial", Phase: scanRunPhaseRunning, PolicyDigest: "sha256:old"}
	if err := store.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	err := reconciler.createReviewTasks(ctx, scan, run, "", []storepkg.ReviewSlice{{ID: "slice_api", RepositoryScan: "kaset", Source: "deterministic", Title: "API", Kind: "package", Status: reviewSliceStatusPending}})
	if err == nil || !strings.Contains(err.Error(), "scanner policy digest changed") {
		t.Fatalf("createReviewTasks() error = %v, want policy drift error", err)
	}
	storedRun, err := store.GetScanRun(ctx, defaultNS, run.ID)
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if storedRun.Phase != scanRunPhaseFailed || storedRun.CompletedAt == nil || !strings.Contains(storedRun.ErrorMessage, "scanner policy digest changed") {
		t.Fatalf("stored run = %#v, want terminal failed policy-drift run", storedRun)
	}
	current := &corev1alpha1.RepositoryScan{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
		t.Fatalf("Get(RepositoryScan) error = %v", err)
	}
	if current.Status.Phase != repositoryScanPhaseError {
		t.Fatalf("RepositoryScan phase = %q, want %q", current.Status.Phase, repositoryScanPhaseError)
	}
	ready := meta.FindStatusCondition(current.Status.Conditions, "Ready")
	if ready == nil || ready.Reason != readyReasonScanFailed || !strings.Contains(ready.Message, "scanner policy digest changed") {
		t.Fatalf("Ready condition = %#v, want ScanFailed policy-drift message", ready)
	}
}

func TestRepositoryScanPolicyDigestDriftFailsValidationTaskCreationWithoutRequeue(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:                   "https://github.com/example/repo",
			AnalysisAgentRef:          corev1alpha1.AgentReference{Name: "scan-reviewer"},
			CustomScanInstructionsRef: &corev1alpha1.PolicyConfigMapKeyRef{Name: "scan-policy"},
		},
	}
	policyConfig := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "scan-policy", Namespace: defaultNS, Labels: map[string]string{security.PolicyConfigMapAllowedLabel: "true"}}, Data: map[string]string{"policy": "new policy text"}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(scan, policyConfig).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: store}
	run := &storepkg.ScanRun{ID: "scan_policy", Namespace: defaultNS, RepositoryScan: "kaset", Mode: "initial", Phase: scanRunPhaseRunning, PolicyDigest: "sha256:old"}
	if err := store.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}
	finding := &storepkg.Finding{ID: "finding_policy", Namespace: defaultNS, RepositoryScan: "kaset", ScanRunID: run.ID, Severity: "high", Confidence: "high"}

	if err := reconciler.ensureValidationTask(ctx, scan, finding, nil); err == nil || !strings.Contains(err.Error(), "scanner policy digest changed") {
		t.Fatalf("ensureValidationTask() error = %v, want policy drift propagated", err)
	}
	storedRun, err := store.GetScanRun(ctx, defaultNS, run.ID)
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if storedRun.Phase != scanRunPhaseFailed || !strings.Contains(storedRun.ErrorMessage, "scanner policy digest changed") {
		t.Fatalf("stored run = %#v, want terminal policy-drift failure", storedRun)
	}
	var tasks corev1alpha1.TaskList
	if err := cl.List(ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Task) error = %v", err)
	}
	if len(tasks.Items) != 0 {
		t.Fatalf("validation tasks = %d, want none on policy drift", len(tasks.Items))
	}
}

func TestRepositoryScanUnreadablePolicyRefFailsMapperTaskCreation(t *testing.T) {
	ctx := context.Background()
	store := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{Name: "kaset", Namespace: defaultNS},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:                   "https://github.com/example/repo",
			AnalysisAgentRef:          corev1alpha1.AgentReference{Name: "scan-reviewer"},
			CustomScanInstructionsRef: &corev1alpha1.PolicyConfigMapKeyRef{Name: "missing-policy"},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(scan).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, Scheme: scheme, SecurityStore: store}
	run := &storepkg.ScanRun{ID: "scan_policy", Namespace: defaultNS, RepositoryScan: "kaset", Mode: "initial", Phase: scanRunPhaseRunning}
	if err := store.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("CreateScanRun() error = %v", err)
	}

	err := reconciler.createMapperTask(ctx, scan, run)
	if err == nil || !strings.Contains(err.Error(), "customScanInstructionsRef") {
		t.Fatalf("createMapperTask() error = %v, want missing policy ref error", err)
	}
	storedRun, err := store.GetScanRun(ctx, defaultNS, run.ID)
	if err != nil {
		t.Fatalf("GetScanRun() error = %v", err)
	}
	if storedRun.Phase != scanRunPhaseFailed || !strings.Contains(storedRun.ErrorMessage, "customScanInstructionsRef") {
		t.Fatalf("stored run = %#v, want terminal missing-policy failure", storedRun)
	}
	current := &corev1alpha1.RepositoryScan{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
		t.Fatalf("Get(RepositoryScan) error = %v", err)
	}
	if current.Status.Phase != repositoryScanPhaseError {
		t.Fatalf("RepositoryScan phase = %q, want %q", current.Status.Phase, repositoryScanPhaseError)
	}
}

func TestTerminalScannerPolicyLoadErrorOnlyTerminalForDeterministicErrors(t *testing.T) {
	if !terminalScannerPolicyLoadError(fmt.Errorf("customScanInstructionsRef: key %q is missing in ConfigMap %q", "policy", "scan-policy")) {
		t.Fatal("terminalScannerPolicyLoadError() = false, want true for policy validation/config error")
	}
	if !terminalScannerPolicyLoadError(fmt.Errorf("customScanInstructionsRef: %w", apierrors.NewNotFound(schema.GroupResource{Group: "", Resource: "configmaps"}, "policy"))) {
		t.Fatal("terminalScannerPolicyLoadError() = false, want true for missing ConfigMap")
	}
	if terminalScannerPolicyLoadError(apierrors.NewInternalError(fmt.Errorf("apiserver temporarily unavailable"))) {
		t.Fatal("terminalScannerPolicyLoadError() = true, want false for transient API error")
	}
	if terminalScannerPolicyLoadError(fmt.Errorf("customScanInstructionsRef: %w", context.DeadlineExceeded)) {
		t.Fatal("terminalScannerPolicyLoadError() = true, want false for context deadline")
	}
}

const (
	testPatchForgeSecretName = "github-forge"
	testPatchBinaryFile      = "logo.png"
)

// repositoryScanPatchResultEnvelope renders the harness-v2 terminal result a
// patch agent returns instead of writing artifact files.
func repositoryScanPatchResultEnvelope(fixture patchIngestFixture, changedFiles []string) []byte {
	data, _ := json.Marshal(security.PatchResultEnvelope{
		SchemaVersion:  security.AgentResultSchemaVersion,
		Kind:           security.AgentResultKindPatch,
		RepositoryScan: fixture.scan.Name,
		FindingID:      fixture.finding.ID,
		Summary:        "escaped the redirect parameter",
		ChangedFiles:   changedFiles,
		TestsRun:       []security.PatchTestRun{{Command: "npm test", ExitCode: 0}},
		Risk:           "low",
	})
	return data
}

// newPatchCommitServer serves GET /repos/example/kaset/commits/<sha> with the
// given files, recording the bearer token it saw.
// patchPullRequestDecoration records the publisher-created PR as GitHub
// would return it and the PATCH the controller sends to decorate it.
type patchPullRequestDecoration struct {
	title   string
	body    string
	patched map[string]string
}

func newPatchCommitServer(t *testing.T, files []repositoryScanCommitFileResponse, seenToken *string) *httptest.Server {
	server, _ := newPatchCommitServerWithPullRequest(t, files, seenToken)
	return server
}

func newPatchCommitServerWithPullRequest(t *testing.T, files []repositoryScanCommitFileResponse, seenToken *string) (*httptest.Server, *patchPullRequestDecoration) {
	t.Helper()
	headSHA := strings.Repeat("b", 40)
	marker := "<!-- orka.publisher.pr-intent.v1 key=sha256:" + strings.Repeat("c", 64) + " -->"
	pr := &patchPullRequestDecoration{title: "Orka publication generation 1", body: "Created by the Orka clean-room workspace publisher.\n\nPublication generation: 1\n\n" + marker}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/example/kaset/commits/"+headSHA:
			*seenToken = r.Header.Get("Authorization")
			_ = json.NewEncoder(w).Encode(repositoryScanCommitResponse{SHA: headSHA, Files: files})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/example/kaset/pulls/42":
			_ = json.NewEncoder(w).Encode(map[string]string{repositoryScanPullRequestTitleField: pr.title, repositoryScanPullRequestBodyField: pr.body})
		case r.Method == http.MethodPatch && r.URL.Path == "/repos/example/kaset/pulls/42":
			var payload map[string]string
			_ = json.NewDecoder(r.Body).Decode(&payload)
			pr.patched = payload
			pr.title, pr.body = payload[repositoryScanPullRequestTitleField], payload[repositoryScanPullRequestBodyField]
			_ = json.NewEncoder(w).Encode(map[string]string{repositoryScanPullRequestTitleField: pr.title, repositoryScanPullRequestBodyField: pr.body})
		default:
			t.Errorf("unexpected GitHub request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server, pr
}

func patchFixtureWithForgeSecret(t *testing.T, id string, server *httptest.Server, withSecret bool) patchIngestFixture {
	t.Helper()
	fixture := newPatchIngestFixture(t, id)
	fixture.scan.Spec.ForgeCredentialRef = &corev1.LocalObjectReference{Name: testPatchForgeSecretName}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	builder := fake.NewClientBuilder().WithScheme(scheme)
	if withSecret {
		builder = builder.WithObjects(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: testPatchForgeSecretName, Namespace: defaultNS},
			Data:       map[string][]byte{defaultACPWorkspaceCredentialKey: []byte("forge-token-value")},
		})
	}
	fixture.reconciler.Client = builder.Build()
	fixture.reconciler.GitHubAPIBaseURL = server.URL
	return fixture
}

func TestIngestPatchTaskDerivesArtifactsFromV2ResultAndPublishedCommit(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	server, pullRequest := newPatchCommitServerWithPullRequest(t, []repositoryScanCommitFileResponse{
		{Filename: "app.py", Status: repositoryMonitorReviewContextStatusModified, Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"},
		{Filename: "tests/test_app.py", Status: "added", Additions: 1, Patch: "@@ -0,0 +1 @@\n+def test_safe(): pass"},
	}, &seenToken)
	fixture := patchFixtureWithForgeSecret(t, "v2-envelope", server, true)
	if err := fixture.store.SaveResult(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, repositoryScanPatchResultEnvelope(fixture, []string{"app.py", "tests/test_app.py"})); err != nil {
		t.Fatalf("SaveResult() error = %v", err)
	}
	task := patchTaskForFixture(fixture, true)
	task.Spec.Prompt = security.BuildPatchPrompt(fixture.scan, fixture.finding, fixture.proposal.Branch)

	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("ingestPatchTask() error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
	if seenToken != "Bearer forge-token-value" {
		t.Fatalf("GitHub request authorization = %q, want the forge token", seenToken)
	}
	diffName, summaryName := patchArtifactNames(fixture.finding.ID)
	diff, _, err := fixture.store.GetArtifact(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, diffName)
	if err != nil {
		t.Fatalf("GetArtifact(diff) error = %v", err)
	}
	if !strings.Contains(string(diff), "diff --git a/app.py b/app.py\n--- a/app.py\n+++ b/app.py\n@@ -1 +1 @@\n-unsafe()\n+safe()\n") ||
		!strings.Contains(string(diff), "diff --git a/tests/test_app.py b/tests/test_app.py\n--- /dev/null\n+++ b/tests/test_app.py\n") {
		t.Fatalf("derived diff = %q", diff)
	}
	summaryData, _, err := fixture.store.GetArtifact(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, summaryName)
	if err != nil {
		t.Fatalf("GetArtifact(summary) error = %v", err)
	}
	var summary security.PatchSummaryArtifact
	if err := json.Unmarshal(summaryData, &summary); err != nil || summary.FindingID != fixture.finding.ID || summary.Risk != "low" || len(summary.ChangedFiles) != 2 {
		t.Fatalf("summary artifact = %s (err %v)", summaryData, err)
	}
	proposals, _ := fixture.store.ListPatchProposals(ctx, fixture.proposal.Namespace, fixture.finding.ID)
	if proposals[0].DiffArtifact != diffName || proposals[0].SummaryArtifact != summaryName {
		t.Fatalf("proposal artifacts = %q/%q", proposals[0].DiffArtifact, proposals[0].SummaryArtifact)
	}
	// The publisher's generic pull request is decorated with the finding
	// while its intent marker stays the final body line.
	if pullRequest.patched == nil || pullRequest.title != "fix(security): Patch target" {
		t.Fatalf("pull request decoration = %#v", pullRequest.patched)
	}
	if !strings.Contains(pullRequest.body, "Security remediation for finding `"+fixture.finding.ID+"`") ||
		!strings.Contains(pullRequest.body, "escaped the redirect parameter") || !strings.Contains(pullRequest.body, "`app.py`") ||
		!strings.HasSuffix(pullRequest.body, " -->") || strings.Count(pullRequest.body, "orka.publisher.pr-intent.v1") != 1 {
		t.Fatalf("decorated body = %q", pullRequest.body)
	}
	// A second ingestion leaves the already-decorated pull request alone.
	pullRequest.patched = nil
	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("ingestPatchTask() second pass error = %v", err)
	}
	if pullRequest.patched != nil {
		t.Fatalf("decorated pull request was patched again: %#v", pullRequest.patched)
	}
}

func TestIngestPatchTaskV2ResultFailsClosed(t *testing.T) {
	ctx := context.Background()
	textFiles := []repositoryScanCommitFileResponse{{Filename: "app.py", Status: repositoryMonitorReviewContextStatusModified, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}}
	cases := []struct {
		name         string
		files        []repositoryScanCommitFileResponse
		changedFiles []string
		withSecret   bool
		result       func(fixture patchIngestFixture) []byte
	}{
		{name: "changedFiles do not match the published commit", files: textFiles, changedFiles: []string{"app.py", "other.py"}, withSecret: true},
		{name: "published commit has a binary file", files: []repositoryScanCommitFileResponse{{Filename: testPatchBinaryFile, Status: repositoryMonitorReviewContextStatusModified}}, changedFiles: []string{testPatchBinaryFile}, withSecret: true},
		{name: "published commit renames a file", files: []repositoryScanCommitFileResponse{{Filename: "b.py", PreviousFilename: "a.py", Status: "renamed", Patch: "@@ -1 +1 @@\n-x\n+y"}}, changedFiles: []string{"b.py"}, withSecret: true},
		{name: "forge credential is missing", files: textFiles, changedFiles: []string{"app.py"}, withSecret: false},
		{name: "agent-supplied diff is not trusted without a commit match", files: textFiles, changedFiles: []string{"app.py"}, withSecret: true, result: func(fixture patchIngestFixture) []byte {
			data, _ := common.FormatStructuredResult(&common.StructuredResult{Summary: "patched", Diff: "diff --git a/app.py b/app.py\n--- a/app.py\n+++ b/app.py\n@@ -1 +1 @@\n-unsafe()\n+safe()\n", Files: []string{"app.py"}})
			return data
		}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var seenToken string
			server := newPatchCommitServer(t, tc.files, &seenToken)
			fixture := patchFixtureWithForgeSecret(t, fmt.Sprintf("v2-fail-%d", i), server, tc.withSecret)
			result := repositoryScanPatchResultEnvelope(fixture, tc.changedFiles)
			if tc.result != nil {
				result = tc.result(fixture)
			}
			if err := fixture.store.SaveResult(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, result); err != nil {
				t.Fatalf("SaveResult() error = %v", err)
			}
			if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, patchTaskForFixture(fixture, true)); err != nil {
				t.Fatalf("ingestPatchTask() error = %v", err)
			}
			assertPatchIngestState(t, fixture, scanRunPhaseFailed, findingStateOpen)
			diffName, _ := patchArtifactNames(fixture.finding.ID)
			if _, _, err := fixture.store.GetArtifact(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, diffName); err == nil {
				t.Fatal("a failed proposal must not persist a diff artifact")
			}
			proposals, _ := fixture.store.ListPatchProposals(ctx, fixture.proposal.Namespace, fixture.finding.ID)
			if len(proposals) != 1 || strings.TrimSpace(proposals[0].Reason) == "" {
				t.Fatalf("failed proposal must carry a reason, got %#v", proposals)
			}
		})
	}
}

func TestPatchHunkBindingRejectsRelocatedAndPrefixAmbiguousContent(t *testing.T) {
	t.Parallel()
	commit := "diff --git a/app.py b/app.py\n--- a/app.py\n+++ b/app.py\n@@ -1 +1 @@\n-unsafe()\n+safe()\n"
	// Same added/deleted strings at a different hunk position must not match.
	relocated := "diff --git a/app.py b/app.py\n--- a/app.py\n+++ b/app.py\n@@ -40 +40 @@\n-unsafe()\n+safe()\n"
	if samePatchHunks(relocated, commit) {
		t.Fatal("relocated hunk content was accepted as the published commit")
	}
	// In-hunk content that begins with "+++"/"---" is change content, not a
	// header, and must participate in the comparison.
	plusCommit := "diff --git a/notes.md b/notes.md\n--- a/notes.md\n+++ b/notes.md\n@@ -1 +1 @@\n-old\n+++extra line\n"
	plusOther := "diff --git a/notes.md b/notes.md\n--- a/notes.md\n+++ b/notes.md\n@@ -1 +1 @@\n-old\n+different\n"
	if samePatchHunks(plusOther, plusCommit) {
		t.Fatal("in-hunk +++ content was excluded from the comparison")
	}
	if !samePatchHunks(commit, commit) {
		t.Fatal("identical diffs did not match")
	}
	// Index-line formatting differences outside hunks stay tolerated.
	withIndex := "diff --git a/app.py b/app.py\nindex 1111111..2222222 100644\n--- a/app.py\n+++ b/app.py\n@@ -1 +1 @@\n-unsafe()\n+safe()\n"
	if !samePatchHunks(withIndex, commit) {
		t.Fatal("index-line formatting difference was not tolerated")
	}
}

func TestPatchHunkBindingRejectsUnverifiableMetadata(t *testing.T) {
	t.Parallel()
	genuine := "diff --git a/app.py b/app.py\n--- a/app.py\n+++ b/app.py\n@@ -1 +1 @@\n-unsafe()\n+safe()\n"
	for name, metadata := range map[string]string{
		"mode change": "old mode 100644\nnew mode 100755\n",
		"rename":      "similarity index 100%\nrename from old.py\nrename to app.py\n",
		"binary":      "Binary files a/app.py and b/app.py differ\n",
	} {
		t.Run(name, func(t *testing.T) {
			artifact := "diff --git a/app.py b/app.py\n" + metadata + "--- a/app.py\n+++ b/app.py\n@@ -1 +1 @@\n-unsafe()\n+safe()\n"
			if samePatchHunks(artifact, genuine) {
				t.Fatalf("patch with %s metadata was accepted without commit evidence", name)
			}
		})
	}
}

func TestPatchHunkBindingRejectsMismatchedPathHeaders(t *testing.T) {
	t.Parallel()
	genuine := "diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1 @@\n-a\n+b\n" +
		"diff --git a/b.go b/b.go\n--- a/b.go\n+++ b/b.go\n@@ -1 +1 @@\n-c\n+d\n"
	swapped := "diff --git a/a.go b/a.go\n--- a/b.go\n+++ b/b.go\n@@ -1 +1 @@\n-a\n+b\n" +
		"diff --git a/b.go b/b.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1 @@\n-c\n+d\n"
	if samePatchHunks(swapped, genuine) {
		t.Fatal("path headers swapped between file blocks were accepted")
	}
	wrongChangeKind := strings.Replace(genuine, "--- a/a.go", "--- /dev/null", 1)
	if samePatchHunks(wrongChangeKind, genuine) {
		t.Fatal("path headers that changed a modified file into an addition were accepted")
	}
}

func TestPatchEvidenceRejectsTruncatedAndDuplicateCommitContent(t *testing.T) {
	t.Parallel()
	// A nonempty patch whose totals disagree with the reported counts is a
	// truncated fragment and must fail closed.
	_, _, reason := repositoryScanDiffFromPublishedCommit([]repositoryScanCommitFileResponse{
		{Filename: "big.go", Status: "modified", Additions: 400, Deletions: 10, Patch: "@@ -1 +1 @@\n-a\n+b"},
	})
	if !strings.Contains(reason, "inconsistent file patch") {
		t.Fatalf("reason = %q, want inconsistent-patch rejection", reason)
	}
	// A commit listing repeating a path must fail closed.
	_, _, reason = repositoryScanDiffFromPublishedCommit([]repositoryScanCommitFileResponse{
		{Filename: "a.go", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-a\n+b"},
		{Filename: "a.go", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-c\n+d"},
	})
	if !strings.Contains(reason, "repeats a file path") {
		t.Fatalf("reason = %q, want duplicate-path rejection", reason)
	}
	// Normalization must not silently change the identity of a legal Git path.
	_, _, reason = repositoryScanDiffFromPublishedCommit([]repositoryScanCommitFileResponse{
		{Filename: " fix.go", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-a\n+b"},
	})
	if !strings.Contains(reason, "whitespace-altered file path") {
		t.Fatalf("reason = %q, want whitespace-path rejection", reason)
	}
	// An artifact repeating a diff --git block cannot hide extra hunks
	// behind a second block that matches the commit.
	genuine := "diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1 @@\n-a\n+b\n"
	spoof := "diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -9 +9 @@\n-evil\n+worse\n" + genuine
	if samePatchHunks(spoof, genuine) {
		t.Fatal("duplicate file block was accepted as matching the commit")
	}
	// A hunkless duplicate block (arbitrary non-hunk lines under a repeated
	// header) must invalidate the diff just the same.
	hunkless := genuine + "diff --git a/a.go b/a.go\narbitrary smuggled line\n"
	if samePatchHunks(hunkless, genuine) {
		t.Fatal("hunkless duplicate file block was accepted as matching the commit")
	}
	// A fabricated reviewer-facing line before the first hunk of a single
	// block must invalidate the diff too.
	prefixSmuggle := "diff --git a/a.go b/a.go\n+fake line reviewers will see\n--- a/a.go\n+++ b/a.go\n@@ -1 +1 @@\n-a\n+b\n"
	if samePatchHunks(prefixSmuggle, genuine) {
		t.Fatal("unknown pre-hunk content was accepted as matching the commit")
	}
}

func TestIngestPatchTaskSecondReconcileAcceptsSanitizedStoredDiff(t *testing.T) {
	ctx := context.Background()
	// The result-contract path persists the commit diff credential-redacted;
	// a later reconcile enters the pre-existing-artifact branch and must not
	// fail an already verified proposal because [REDACTED] differs from the
	// removed secret.
	const secret = "ak-live-0123456789abcdef"
	commitPatch := "@@ -1 +1 @@\n-api_key=" + secret + "\n+api_key=vault://key"
	var seenToken string
	fixture := patchFixtureWithForgeSecret(t, "sanitized-second-pass", newPatchCommitServer(t, []repositoryScanCommitFileResponse{{Filename: "config.env", Status: "modified", Additions: 1, Deletions: 1, Patch: commitPatch}}, &seenToken), true)
	savePatchStructuredResult(t, fixture, &common.StructuredResult{
		Summary: "rotated credential", Files: []string{"config.env"}, PushBranch: fixture.proposal.Branch,
	})
	// First reconcile derives and persists the sanitized artifacts.
	task := patchTaskForFixture(fixture, true)
	envelope := `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"` + fixture.finding.ID + `","summary":"rotated credential","changedFiles":["config.env"],"risk":"low"}`
	if err := fixture.store.SaveResult(ctx, task.Namespace, task.Name, []byte(envelope)); err != nil {
		t.Fatalf("SaveResult() error = %v", err)
	}
	task.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("first ingest error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
	// Second reconcile takes the pre-existing-artifact path against the
	// stored (redacted) diff and must stay verified.
	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatalf("second ingest error = %v", err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
}
