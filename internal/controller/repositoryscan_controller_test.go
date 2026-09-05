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
	readyReasonScanFailed = "ScanFailed"
	testPatchDiffHeader   = "diff --git a/app.py b/app.py"
	// testPatchFullDiff carries the same change content as the app.py commit
	// stub served by newPatchCommitServer; artifact evidence must match the
	// published commit's content, not just its file names.
	testPatchFullDiff = testPatchDiffHeader + "\n--- a/app.py\n+++ b/app.py\n@@ -1 +1 @@\n-unsafe()\n+safe()\n"
)

func repositoryScanTestAgent(name string) *corev1alpha1.Agent {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	return &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: &contract,
		}},
	}
}

func repositoryScanExternalRuntimeFixtures(agentName string, allowedTools []string) (*corev1alpha1.Agent, *corev1alpha1.AgentRuntime) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	runtimeName := agentName + "-runtime"
	return &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: agentName, Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: runtimeName},
		}},
	}, &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: runtimeName, Namespace: defaultNS},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
				},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:          append([]string{}, allowedTools...),
					DisallowedTools:       []string{},
					ApprovalRequiredTools: []string{},
				},
			},
		},
	}
}

func repositoryScanExternalRuntimePolicySkew(
	scheme *runtime.Scheme,
	agentName string,
	currentAllowedTools []string,
) (*corev1alpha1.Agent, *corev1alpha1.AgentRuntime, client.Reader) {
	agent, cachedRuntime := repositoryScanExternalRuntimeFixtures(agentName, []string{"revoked_tool"})
	_, currentRuntime := repositoryScanExternalRuntimeFixtures(agentName, currentAllowedTools)
	apiReader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent.DeepCopy(), currentRuntime).
		Build()
	return agent, cachedRuntime, apiReader
}

func requireExplicitTaskAllowedTools(t *testing.T, task *corev1alpha1.Task, want []string) {
	t.Helper()
	if task.Spec.AgentRuntime == nil || task.Spec.AgentRuntime.AllowedTools == nil {
		t.Fatalf("task AgentRuntime = %#v, want explicit allowedTools %#v", task.Spec.AgentRuntime, want)
	}
	if !reflect.DeepEqual(task.Spec.AgentRuntime.AllowedTools, want) {
		t.Fatalf("task allowedTools = %#v, want %#v", task.Spec.AgentRuntime.AllowedTools, want)
	}
}

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

func TestTrustedFindingsRepositoryScopesRefOnlyScan(t *testing.T) {
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
			name: "explicit branch wins",
			spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/repo", Branch: "release", Ref: "v1.2.3"},
			want: "release",
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

func saveFindingsTaskResult(
	t *testing.T,
	store *sqlitestore.Store,
	task *corev1alpha1.Task,
	repositoryScan, scanID, policyDigest, contextDigest, sliceID string,
	findings security.FindingsV2Artifact,
) {
	t.Helper()
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
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: "poison-source-agent"},
			Prompt:   "POISON SOURCE PROMPT MUST NOT BE COPIED",
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
	analysisAgent, analysisRuntime := repositoryScanExternalRuntimeFixtures(scan.Spec.AnalysisAgentRef.Name, []string{"read_evidence"})
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithObjects(scan, sourceTask, analysisAgent, analysisRuntime).
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
	requireExplicitTaskAllowedTools(t, retryTask, []string{"read_evidence"})
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
	analysisAgent, cachedRuntime, apiReader := repositoryScanExternalRuntimePolicySkew(
		scheme, scan.Spec.AnalysisAgentRef.Name, []string{"read_evidence", "search_findings"},
	)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan, policyConfig, analysisAgent, cachedRuntime).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, APIReader: apiReader, Scheme: scheme, SecurityStore: store}
	run := &storepkg.ScanRun{ID: "scan_policy", Namespace: defaultNS, RepositoryScan: "kaset", Mode: "initial", Phase: scanRunPhaseRunning}
	reviewSlice := storepkg.ReviewSlice{ID: "slice_api", RepositoryScan: "kaset", Source: "deterministic", Title: "API", Kind: "package", Status: reviewSliceStatusPending}
	manifest := bindReviewSliceContext(t, &reviewSlice)
	if err := reconciler.createReviewTasks(ctx, scan, run, "", []storepkg.ReviewSlice{reviewSlice}); err != nil {
		t.Fatalf("createReviewTasks() error = %v", err)
	}
	var tasks corev1alpha1.TaskList
	if err := cl.List(ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Task) error = %v", err)
	}
	if len(tasks.Items) != 1 {
		t.Fatalf("len(tasks) = %d, want 1", len(tasks.Items))
	}
	requireExplicitTaskAllowedTools(t, &tasks.Items[0], []string{"read_evidence", "search_findings"})
	prompt := tasks.Items[0].Spec.Prompt
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
	analysisAgent, cachedRuntime, apiReader := repositoryScanExternalRuntimePolicySkew(
		scheme, scan.Spec.AnalysisAgentRef.Name, []string{"read_evidence"},
	)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(scan, analysisAgent, cachedRuntime).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, APIReader: apiReader, Scheme: scheme, SecurityStore: store}
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
	requireExplicitTaskAllowedTools(t, &tasks.Items[0], []string{"read_evidence"})
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
	analysisAgent := repositoryScanTestAgent(scan.Spec.AnalysisAgentRef.Name)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(scan, analysisAgent).Build()
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
	mapperTask := newSucceededSecurityTask("kaset-initial-mapper", "scan_review", security.StageMapper, metav1.Now())
	analysisAgent := repositoryScanTestAgent(scan.Spec.AnalysisAgentRef.Name)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithObjects(scan, threatTask, mapperTask, analysisAgent).
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
	mapperTask := newSucceededSecurityTask("kaset-partial-mapper", "scan_partial_review", security.StageMapper, metav1.Now())
	reviewTask := newSucceededSecurityTask("kaset-review-slice-api", "scan_partial_review", security.StageReview, metav1.Now())
	reviewTask.Labels[labels.LabelSecuritySliceID] = sliceAPI
	analysisAgent := repositoryScanTestAgent(scan.Spec.AnalysisAgentRef.Name)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithObjects(scan, threatTask, mapperTask, reviewTask, analysisAgent).
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
		WithObjects(scan, existingTask, repositoryScanTestAgent(scan.Spec.AnalysisAgentRef.Name)).
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

	if err := reconciler.createValidationTask(ctx, scan, finding); err == nil || !strings.Contains(err.Error(), "scanner policy digest changed") {
		t.Fatalf("createValidationTask() error = %v, want policy drift propagated", err)
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

func TestRepositoryScanValidationTaskMaterializesRuntimeRefAllowedTools(t *testing.T) {
	ctx := context.Background()
	securityStore := setupControllerSQLiteStore(t)
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	scan := &corev1alpha1.RepositoryScan{
		TypeMeta: metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RepositoryScan"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "validation-runtime", Namespace: defaultNS, UID: types.UID("validation-runtime-uid"),
		},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL: "https://github.com/example/repo", AnalysisAgentRef: corev1alpha1.AgentReference{Name: "scan-reviewer"},
		},
	}
	analysisAgent, cachedRuntime, apiReader := repositoryScanExternalRuntimePolicySkew(
		scheme, scan.Spec.AnalysisAgentRef.Name, []string{},
	)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan, analysisAgent, cachedRuntime).Build()
	reconciler := &RepositoryScanReconciler{Client: cl, APIReader: apiReader, Scheme: scheme, SecurityStore: securityStore}
	finding := &storepkg.Finding{
		ID: "finding-runtime", Namespace: defaultNS, RepositoryScan: scan.Name, Severity: "high", Confidence: "high",
	}

	if err := reconciler.createValidationTask(ctx, scan, finding); err != nil {
		t.Fatalf("createValidationTask() error = %v", err)
	}
	var tasks corev1alpha1.TaskList
	if err := cl.List(ctx, &tasks, client.InNamespace(defaultNS)); err != nil {
		t.Fatalf("List(Task) error = %v", err)
	}
	if len(tasks.Items) != 1 {
		t.Fatalf("validation tasks = %d, want 1", len(tasks.Items))
	}
	requireExplicitTaskAllowedTools(t, &tasks.Items[0], []string{})
	storedFinding, err := securityStore.GetFinding(ctx, defaultNS, finding.ID)
	if err != nil {
		t.Fatalf("GetFinding() error = %v", err)
	}
	if storedFinding.ValidationStatus != findingValidationStatusPending {
		t.Fatalf("validation status = %q, want %q", storedFinding.ValidationStatus, findingValidationStatusPending)
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
