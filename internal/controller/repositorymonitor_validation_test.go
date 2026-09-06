package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
)

const repositoryMonitorValidationTestImage = "ghcr.io/example/repo-validation@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

const repositoryMonitorValidationTestSecret = "ghp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

const repositoryMonitorValidationTestCommand = "go test ./..."

const repositoryMonitorValidationTestCommandDigest = "sha256:1bb497e3e13a1105cf24e3359fa3ef75de08b66ff8a2839cd7f9ea97824d9eb3"

func TestRepositoryMonitorPostRepairValidationRetriesAreBounded(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	maxRetries := int32(1)
	monitor := repositoryMonitorReviewIngestTestMonitor("post-repair-validation-retry")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	monitor.Spec.Repair.MaxValidationRetries = &maxRetries
	completedAt := time.Now().Add(-3 * time.Minute)
	job := &store.RepairJob{
		ID: "repair-validation-retry", MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
		PRNumber: 1, Phase: repositoryMonitorRepairPhaseSucceeded, PushedSHA: repositoryMonitorTestHeadSHA,
		CompletedAt: &completedAt,
	}
	if err := monitorStore.CreateRepairJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	item := &store.MonitorItem{
		MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name, Kind: repositoryMonitorPullRequestKind,
		ItemKey: "1", Number: 1, State: repositoryMonitorItemStateOpen, HeadSHA: repositoryMonitorTestHeadSHA,
	}
	reconciler := &RepositoryMonitorReconciler{Store: monitorStore}
	pr := repositoryMonitorPullRequest{Number: 1, HeadSHA: repositoryMonitorTestHeadSHA}
	firstReviewTask := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: repositoryMonitorReviewTaskName(monitor, &store.MonitorRun{ID: "run-first"}, pr), UID: types.UID("first-review-uid")}}
	secondReviewTask := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: repositoryMonitorReviewTaskName(monitor, &store.MonitorRun{ID: "run-retry"}, pr), UID: types.UID("second-review-uid")}}
	if firstReviewTask.Name == secondReviewTask.Name || tools.RepositoryValidationTaskName(firstReviewTask) == tools.RepositoryValidationTaskName(secondReviewTask) {
		t.Fatal("a validation retry did not receive a fresh review and child Task identity")
	}

	applyResult := func(id string, taskName *corev1alpha1.Task, status string) *store.ReviewRecord {
		t.Helper()
		verdict := repositoryMonitorReviewVerdictNeedsHuman
		if status == repositoryMonitorValidationStatusFailed {
			verdict = repositoryMonitorReviewVerdictNeedsChanges
		}
		record := &store.ReviewRecord{
			ID: id, MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
			Kind: repositoryMonitorPullRequestKind, Number: 1, HeadSHA: repositoryMonitorTestHeadSHA,
			TaskName: taskName.Name, Verdict: verdict,
			ValidationTask: tools.RepositoryValidationTaskName(taskName), ValidationImage: repositoryMonitorValidationTestImage,
			ValidationStatus: status, CreatedAt: time.Now().Add(-2 * time.Minute),
		}
		if err := monitorStore.CreateReviewRecord(ctx, record); err != nil {
			t.Fatal(err)
		}
		if err := reconciler.applyRepositoryMonitorReviewRecordToItem(ctx, monitor, item, record, ""); err != nil {
			t.Fatal(err)
		}
		return record
	}

	applyResult("review-first", firstReviewTask, repositoryMonitorValidationStatusFailed)
	job, err := monitorStore.GetRepairJob(ctx, monitor.Namespace, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.ValidationAttempts != 1 || job.LastError != "" || item.LastReviewedHeadSHA != "" {
		t.Fatalf("first attempt state = attempts %d error %q fresh head %q, want one retry still available", job.ValidationAttempts, job.LastError, item.LastReviewedHeadSHA)
	}

	secondRecord := applyResult("review-retry", secondReviewTask, repositoryMonitorValidationStatusUnavailable)
	job, err = monitorStore.GetRepairJob(ctx, monitor.Namespace, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.ValidationAttempts != 2 || job.LastError != repositoryMonitorValidationRetryExhaustedReason || item.LastReviewedHeadSHA != repositoryMonitorTestHeadSHA {
		t.Fatalf("exhausted state = attempts %d error %q fresh head %q, want initial attempt plus one retry and terminal state", job.ValidationAttempts, job.LastError, item.LastReviewedHeadSHA)
	}
	if err := reconciler.applyRepositoryMonitorReviewRecordToItem(ctx, monitor, item, secondRecord, ""); err != nil {
		t.Fatal(err)
	}
	job, err = monitorStore.GetRepairJob(ctx, monitor.Namespace, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.ValidationAttempts != 2 {
		t.Fatalf("replayed review changed durable attempt count to %d", job.ValidationAttempts)
	}
	fresh, err := reconciler.repositoryMonitorReviewedHeadFresh(ctx, monitor, item, repositoryMonitorTestHeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	if !fresh {
		t.Fatal("exhausted post-repair validation remained eligible for unbounded automatic retries")
	}
	ttl := metav1.Duration{Duration: time.Minute}
	monitor.Spec.Review.StaleReviewTTL = &ttl
	fresh, err = reconciler.repositoryMonitorReviewedHeadFresh(ctx, monitor, item, repositoryMonitorTestHeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	if fresh {
		t.Fatal("exhausted post-repair validation ignored staleReviewTTL")
	}
}

func TestRepositoryMonitorPostRepairUnavailableValidationPublishesOnlyWhenExhausted(t *testing.T) {
	tests := []struct {
		name       string
		maxRetries int32
		wantPosts  int
		wantPhase  string
	}{
		{name: "retry remains", maxRetries: 1, wantPhase: repositoryMonitorPublishPhaseSkipped},
		{name: "retries exhausted", maxRetries: 0, wantPosts: 1, wantPhase: repositoryMonitorPublishPhaseSucceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			monitorStore := setupControllerSQLiteStore(t)
			scheme := runtime.NewScheme()
			if err := corev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("AddToScheme() error = %v", err)
			}
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatalf("corev1 AddToScheme() error = %v", err)
			}

			monitorName := "post-repair-publish-" + repositoryMonitorBoundedDNSName(strings.ReplaceAll(tt.name, " ", "-"), 30)
			monitor := repositoryMonitorReviewIngestTestMonitor(monitorName)
			monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
			monitor.Spec.Repair.MaxValidationRetries = &tt.maxRetries
			monitor.Spec.ForgeCredentialRef = &corev1.LocalObjectReference{Name: "github-token"}
			monitor.Spec.Review.Publish = corev1alpha1.RepositoryMonitorReviewPublishSpec{Enabled: true, Event: repositoryMonitorPublishEventComment}
			publishServer := newRepositoryMonitorPublishTestServer(t, repositoryMonitorPublishTestServerConfig{HeadSHA: repositoryMonitorTestHeadSHA})
			t.Cleanup(publishServer.Close)

			task := repositoryMonitorReviewIngestTestTask(monitorName+"-task", monitorName, 1, repositoryMonitorTestHeadSHA)
			task.Annotations[labels.AnnotationRepositoryValidationImage] = repositoryMonitorValidationTestImage
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: monitor.Namespace}, Data: map[string][]byte{"token": []byte("test-token")}}
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, task, secret).Build()
			reconciler := &RepositoryMonitorReconciler{Client: cl, Scheme: scheme, Store: monitorStore, ResultStore: monitorStore, GitHubAPIBaseURL: publishServer.URL}

			completedAt := time.Now().Add(-time.Minute)
			if err := monitorStore.CreateRepairJob(ctx, &store.RepairJob{
				ID: "repair-" + monitorName, MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
				PRNumber: 1, Phase: repositoryMonitorRepairPhaseSucceeded, PushedSHA: repositoryMonitorTestHeadSHA,
				CompletedAt: &completedAt,
			}); err != nil {
				t.Fatal(err)
			}
			record := &store.ReviewRecord{
				ID: "review-" + monitorName, MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
				Kind: repositoryMonitorPullRequestKind, Number: 1, HeadSHA: repositoryMonitorTestHeadSHA,
				TaskName: task.Name, TaskNamespace: task.Namespace, Verdict: repositoryMonitorReviewVerdictNeedsHuman,
				Confidence: repositoryMonitorReviewConfidenceLow, SecurityStatus: "clear", FindingsJSON: "[]",
				Summary: "Validation remained unavailable.", ValidationTask: tools.RepositoryValidationTaskName(task),
				ValidationImage: repositoryMonitorValidationTestImage, ValidationStatus: repositoryMonitorValidationStatusUnavailable,
				ValidationEvidence: "The validation container could not start.", CreatedAt: time.Now(),
			}
			if err := monitorStore.CreateReviewRecord(ctx, record); err != nil {
				t.Fatal(err)
			}
			item := &store.MonitorItem{
				MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name, Kind: repositoryMonitorPullRequestKind,
				ItemKey: "1", Number: 1, State: repositoryMonitorItemStateOpen, HeadSHA: repositoryMonitorTestHeadSHA,
			}
			if err := reconciler.applyRepositoryMonitorReviewRecordToItem(ctx, monitor, item, record, ""); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.publishRepositoryMonitorReview(ctx, monitor, item, task, record); err != nil {
				t.Fatal(err)
			}
			if publishServer.PostCount != tt.wantPosts {
				t.Fatalf("post count = %d, want %d", publishServer.PostCount, tt.wantPosts)
			}
			publishRecords, _, err := monitorStore.ListReviewPublishRecords(ctx, store.ReviewPublishRecordFilter{
				Namespace: monitor.Namespace, MonitorName: monitor.Name, ReviewRecordID: record.ID, Limit: 5,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(publishRecords) != 1 || publishRecords[0].Phase != tt.wantPhase {
				t.Fatalf("publish records = %#v, want one %q record", publishRecords, tt.wantPhase)
			}
			if tt.wantPosts == 0 && publishRecords[0].SkipReason != repositoryMonitorPublishSkipValidationUnavailable {
				t.Fatalf("skip reason = %q, want %q", publishRecords[0].SkipReason, repositoryMonitorPublishSkipValidationUnavailable)
			}
			if tt.wantPosts == 1 && !strings.Contains(publishServer.PostedReview.Body, "**Status:** unavailable") {
				t.Fatalf("posted review body does not report unavailable validation: %s", publishServer.PostedReview.Body)
			}
		})
	}
}

func TestRepositoryMonitorReviewValidationGatesPassedVerdict(t *testing.T) {
	tests := []struct {
		name               string
		validationTask     func(*corev1alpha1.RepositoryMonitor, *corev1alpha1.Task) *corev1alpha1.Task
		wantHandled        bool
		wantVerdict        string
		wantStatus         string
		wantAutomergeState string
		wantEvidence       string
		boundCommand       string
		secretCommand      string
		wantCommandDigest  string
		wantFresh          bool
	}{
		{
			name: "passing validation preserves passed verdict",
			validationTask: func(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) *corev1alpha1.Task {
				return repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
			},
			wantHandled:        true,
			wantVerdict:        repositoryMonitorReviewVerdictPassed,
			wantStatus:         repositoryMonitorValidationStatusPassed,
			wantAutomergeState: repositoryMonitorAutomergeStateMergeReady,
			wantEvidence:       "go test ./...: ok",
			wantCommandDigest:  repositoryMonitorValidationTestCommandDigest,
			wantFresh:          true,
		},
		{
			name: "failed validation blocks passed verdict",
			validationTask: func(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) *corev1alpha1.Task {
				task := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseFailed, reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
				task.Status.Message = "command exited with status 1"
				task.Status.ExecutionOutcome = &corev1alpha1.TaskWorkloadExecutionOutcome{Phase: corev1alpha1.TaskPhaseFailed, Attempt: 1}
				return task
			},
			wantHandled:       true,
			wantVerdict:       repositoryMonitorReviewVerdictNeedsChanges,
			wantStatus:        repositoryMonitorValidationStatusFailed,
			wantEvidence:      "status 1",
			wantCommandDigest: repositoryMonitorValidationTestCommandDigest,
			wantFresh:         true,
		},
		{
			name: "infrastructure validation failure remains retryable",
			validationTask: func(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) *corev1alpha1.Task {
				task := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseFailed, reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
				task.Status.Message = "pod stuck: ErrImageNeverPull"
				return task
			},
			wantHandled:       true,
			wantVerdict:       repositoryMonitorReviewVerdictNeedsHuman,
			wantStatus:        repositoryMonitorValidationStatusUnavailable,
			wantEvidence:      "ErrImageNeverPull",
			wantCommandDigest: repositoryMonitorValidationTestCommandDigest,
		},
		{
			name:         "missing validation blocks passed verdict",
			wantHandled:  true,
			wantVerdict:  repositoryMonitorReviewVerdictNeedsHuman,
			wantStatus:   repositoryMonitorValidationStatusNotRun,
			wantEvidence: "did not run",
			wantFresh:    true,
		},
		{
			name: "stale validation checkout blocks passed verdict",
			validationTask: func(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) *corev1alpha1.Task {
				return repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, "different-head")
			},
			wantHandled:  true,
			wantVerdict:  repositoryMonitorReviewVerdictNeedsChanges,
			wantStatus:   repositoryMonitorValidationStatusFailed,
			wantEvidence: "exact reviewed head",
			wantFresh:    true,
		},
		{
			name: "removed validation image annotation is rejected",
			validationTask: func(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) *corev1alpha1.Task {
				task := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
				delete(task.Annotations, labels.AnnotationRepositoryValidationImage)
				return task
			},
			wantHandled:  true,
			wantVerdict:  repositoryMonitorReviewVerdictNeedsChanges,
			wantStatus:   repositoryMonitorValidationStatusFailed,
			wantEvidence: "repository-validation-image",
			wantFresh:    true,
		},
		{
			name: "altered validation image annotation is rejected",
			validationTask: func(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) *corev1alpha1.Task {
				task := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
				task.Annotations[labels.AnnotationRepositoryValidationImage] = "ghcr.io/example/other-validation@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
				return task
			},
			wantHandled:  true,
			wantVerdict:  repositoryMonitorReviewVerdictNeedsChanges,
			wantStatus:   repositoryMonitorValidationStatusFailed,
			wantEvidence: "repository-validation-image",
			wantFresh:    true,
		},
		{
			name: "opaque validation command is persisted only as a digest",
			validationTask: func(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) *corev1alpha1.Task {
				return repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
			},
			wantHandled:        true,
			wantVerdict:        repositoryMonitorReviewVerdictPassed,
			wantStatus:         repositoryMonitorValidationStatusPassed,
			wantAutomergeState: repositoryMonitorAutomergeStateMergeReady,
			wantEvidence:       "go test ./...: ok",
			boundCommand:       "tool -p hunter2",
			wantCommandDigest:  "sha256:4089918491e56a8fb453fc461ac427331a1588ea0cb3cb5a18bcd92ad41e156f",
			wantFresh:          true,
		},
		{
			name: "prior task overlay is rejected",
			validationTask: func(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) *corev1alpha1.Task {
				task := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
				task.Spec.PriorTaskRef = &corev1alpha1.PriorTaskReference{Name: "untrusted-overlay"}
				return task
			},
			wantHandled:  true,
			wantVerdict:  repositoryMonitorReviewVerdictNeedsChanges,
			wantStatus:   repositoryMonitorValidationStatusFailed,
			wantEvidence: "canonical repository validation task",
			wantFresh:    true,
		},
		{
			name: "altered validation task placeholder is rejected",
			validationTask: func(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) *corev1alpha1.Task {
				task := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
				task.Spec.Args = []string{"go test ./internal/..."}
				return task
			},
			wantHandled:  true,
			wantVerdict:  repositoryMonitorReviewVerdictNeedsChanges,
			wantStatus:   repositoryMonitorValidationStatusFailed,
			wantEvidence: "canonical repository validation task",
			wantFresh:    true,
		},
		{
			name: "altered validation command Secret is rejected",
			validationTask: func(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) *corev1alpha1.Task {
				return repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
			},
			secretCommand:     "go test ./internal/...",
			wantHandled:       true,
			wantVerdict:       repositoryMonitorReviewVerdictNeedsChanges,
			wantStatus:        repositoryMonitorValidationStatusFailed,
			wantEvidence:      "immutable validation command Secret",
			wantCommandDigest: repositoryMonitorValidationTestCommandDigest,
			wantFresh:         true,
		},
		{
			name: "running validation defers review ingestion",
			validationTask: func(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task) *corev1alpha1.Task {
				return repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseRunning, reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
			},
			wantHandled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			monitorStore := setupControllerSQLiteStore(t)
			scheme := repositoryMonitorValidationTestScheme(t)

			monitor := repositoryMonitorReviewIngestTestMonitor("validation-" + repositoryMonitorShortHash(tt.name))
			monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
			reviewTask := repositoryMonitorReviewIngestTestTask("review-"+repositoryMonitorShortHash(tt.name), monitor.Name, 1, repositoryMonitorTestHeadSHA)
			repositoryMonitorBindValidationForTest(reviewTask)
			objects := []client.Object{monitor, reviewTask}
			var validationTask *corev1alpha1.Task
			if tt.validationTask != nil {
				validationTask = tt.validationTask(monitor, reviewTask)
				boundCommand := tt.boundCommand
				if boundCommand == "" {
					boundCommand = repositoryMonitorValidationTestCommand
				}
				validationTask.Annotations[labels.AnnotationRepositoryValidationCommandDigest] = tools.RepositoryValidationCommandDigest(boundCommand)
				secretCommand := tt.secretCommand
				if secretCommand == "" {
					secretCommand = boundCommand
				}
				objects = append(objects, validationTask, repositoryMonitorValidationCommandSecretForTest(reviewTask, validationTask, secretCommand))
				seedRepositoryMonitorValidationBindingForTest(t, ctx, monitorStore, monitor, reviewTask, validationTask, boundCommand)
			}
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			reconciler := &RepositoryMonitorReconciler{Client: k8sClient, Scheme: scheme, Store: monitorStore, ResultStore: monitorStore}

			item := &store.MonitorItem{
				MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
				Kind: repositoryMonitorPullRequestKind, ItemKey: "1", Number: 1,
				State: repositoryMonitorItemStateOpen, HeadSHA: repositoryMonitorTestHeadSHA,
				LastVerdict: repositoryMonitorRunPhaseQueued, LastReviewID: reviewTask.Name,
			}
			if err := monitorStore.UpsertMonitorItem(ctx, item); err != nil {
				t.Fatal(err)
			}
			if err := monitorStore.SaveResult(ctx, reviewTask.Namespace, reviewTask.Name, repositoryMonitorReviewResultEnvelope(t, 1, repositoryMonitorTestHeadSHA, repositoryMonitorReviewVerdictPassed)); err != nil {
				t.Fatal(err)
			}
			if validationTask != nil && validationTask.Status.ResultRef != nil && validationTask.Status.ResultRef.Available {
				if err := monitorStore.SaveResult(ctx, validationTask.Namespace, validationTask.Name, []byte("go test ./...: ok")); err != nil {
					t.Fatal(err)
				}
			}

			handled, err := reconciler.ingestCompletedRepositoryMonitorReviewTask(ctx, monitor, item, reviewTask)
			if err != nil {
				t.Fatalf("ingestCompletedRepositoryMonitorReviewTask() error = %v", err)
			}
			if handled != tt.wantHandled {
				t.Fatalf("handled = %v, want %v", handled, tt.wantHandled)
			}
			records, _, err := monitorStore.ListReviewRecords(ctx, store.ReviewRecordFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, Number: 1, Limit: 5})
			if err != nil {
				t.Fatal(err)
			}
			if !tt.wantHandled {
				if len(records) != 0 {
					t.Fatalf("records = %#v, want no record while validation runs", records)
				}
				return
			}
			if len(records) != 1 {
				t.Fatalf("records = %#v, want one review record", records)
			}
			record := records[0]
			if record.Verdict != tt.wantVerdict || record.ValidationStatus != tt.wantStatus || record.ValidationImage != repositoryMonitorValidationTestImage {
				t.Fatalf("record verdict/validation = %q/%q image %q, want %q/%q image %q", record.Verdict, record.ValidationStatus, record.ValidationImage, tt.wantVerdict, tt.wantStatus, repositoryMonitorValidationTestImage)
			}
			if tt.wantEvidence != "" && !strings.Contains(record.ValidationEvidence, tt.wantEvidence) {
				t.Fatalf("validation evidence = %q, want containing %q", record.ValidationEvidence, tt.wantEvidence)
			}
			if record.ValidationCommandDigest != tt.wantCommandDigest {
				t.Fatalf("validation command digest = %q, want %q", record.ValidationCommandDigest, tt.wantCommandDigest)
			}
			if strings.Contains(record.ValidationCommandDigest, repositoryMonitorValidationTestSecret) || strings.Contains(record.ValidationEvidence, repositoryMonitorValidationTestSecret) {
				t.Fatal("validation record persisted credential-like command content")
			}
			updated, err := monitorStore.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, "1")
			if err != nil {
				t.Fatal(err)
			}
			if updated.AutomergeState != tt.wantAutomergeState {
				t.Fatalf("automerge state = %q, want %q", updated.AutomergeState, tt.wantAutomergeState)
			}
			fresh, err := reconciler.repositoryMonitorReviewedHeadFresh(ctx, monitor, updated, repositoryMonitorTestHeadSHA)
			if err != nil {
				t.Fatal(err)
			}
			if fresh != tt.wantFresh {
				t.Fatalf("fresh = %v, want %v for validation status %q", fresh, tt.wantFresh, record.ValidationStatus)
			}
			assertRepositoryMonitorValidationTaskCleanup(t, ctx, k8sClient, validationTask, tt.wantHandled)
		})
	}
}

func TestRepositoryMonitorPendingValidationRequeuesReviewIngestion(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	scheme := repositoryMonitorValidationTestScheme(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("pending-validation-requeue")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("pending-validation-requeue-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseRunning, repositoryMonitorTestHeadSHA)
	commandSecret := repositoryMonitorValidationCommandSecretForTest(reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	seedRepositoryMonitorValidationBindingForTest(t, ctx, monitorStore, monitor, reviewTask, validationTask, repositoryMonitorValidationTestCommand)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryMonitor{}).
		WithObjects(monitor, reviewTask, validationTask, commandSecret).
		Build()
	reconciler := &RepositoryMonitorReconciler{
		Client: k8sClient, Scheme: scheme, Store: monitorStore, ResultStore: monitorStore,
	}
	item := &store.MonitorItem{
		MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
		Kind: repositoryMonitorPullRequestKind, ItemKey: "1", Number: 1,
		State: repositoryMonitorItemStateOpen, HeadSHA: repositoryMonitorTestHeadSHA,
		LastVerdict: repositoryMonitorRunPhaseQueued, LastReviewID: reviewTask.Name,
	}
	if err := monitorStore.UpsertMonitorItem(ctx, item); err != nil {
		t.Fatal(err)
	}
	if err := monitorStore.SaveResult(ctx, reviewTask.Namespace, reviewTask.Name, repositoryMonitorReviewResultEnvelope(t, 1, repositoryMonitorTestHeadSHA, repositoryMonitorReviewVerdictPassed)); err != nil {
		t.Fatal(err)
	}

	result, err := reconciler.reconcileRepositoryMonitorRuns(ctx, monitor, repositoryMonitorReconcileState{})
	if err != nil {
		t.Fatalf("reconcileRepositoryMonitorRuns() error = %v", err)
	}
	if result.RequeueAfter != repositoryMonitorValidationRetry {
		t.Fatalf("RequeueAfter = %v, want %v while validation is running", result.RequeueAfter, repositoryMonitorValidationRetry)
	}
	records, _, err := monitorStore.ListReviewRecords(ctx, store.ReviewRecordFilter{
		Namespace: monitor.Namespace, MonitorName: monitor.Name, Number: 1, Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("review records = %#v, want none before validation completes", records)
	}
}

func assertRepositoryMonitorValidationTaskCleanup(t *testing.T, ctx context.Context, k8sClient client.Client, validationTask *corev1alpha1.Task, wantDeleted bool) {
	t.Helper()
	if validationTask == nil {
		return
	}
	remaining := &corev1alpha1.Task{}
	getErr := k8sClient.Get(ctx, client.ObjectKeyFromObject(validationTask), remaining)
	if wantDeleted && !apierrors.IsNotFound(getErr) {
		t.Fatalf("terminal validation task cleanup error = %v, task = %#v", getErr, remaining)
	}
	if !wantDeleted && getErr != nil {
		t.Fatalf("pending validation task disappeared: %v", getErr)
	}
}

func TestRepositoryMonitorValidationCleanupDeletesBoundSecretWhenChildTaskIsAbsent(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	scheme := repositoryMonitorValidationTestScheme(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("orphaned-validation-command")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("orphaned-validation-command-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhasePending, repositoryMonitorTestHeadSHA)
	seedRepositoryMonitorValidationBindingForTest(t, ctx, monitorStore, monitor, reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	commandSecret := repositoryMonitorValidationCommandSecretForTest(reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, reviewTask, commandSecret).Build()
	reconciler := &RepositoryMonitorReconciler{Client: k8sClient, Scheme: scheme, Store: monitorStore}
	record := &store.ReviewRecord{ValidationTask: validationTask.Name}

	cleaned, err := reconciler.cleanupRepositoryMonitorValidationTask(ctx, monitor, reviewTask, record)
	if err != nil || !cleaned {
		t.Fatalf("cleanupRepositoryMonitorValidationTask() = (%v, %v), want cleaned orphaned Secret", cleaned, err)
	}
	remaining := &corev1.Secret{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(commandSecret), remaining); !apierrors.IsNotFound(err) {
		t.Fatalf("orphaned validation command Secret cleanup error = %v, Secret = %#v", err, remaining)
	}
}

func TestRepositoryMonitorValidationCleanupDeletesBoundSecretWhenChildTaskIsTerminating(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	scheme := repositoryMonitorValidationTestScheme(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("terminating-validation-command")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("terminating-validation-command-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, repositoryMonitorTestHeadSHA)
	validationTask.Finalizers = []string{"test.orka.ai/hold-termination"}
	seedRepositoryMonitorValidationBindingForTest(t, ctx, monitorStore, monitor, reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	commandSecret := repositoryMonitorValidationCommandSecretForTest(reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, reviewTask, validationTask, commandSecret).Build()
	if err := k8sClient.Delete(ctx, validationTask); err != nil {
		t.Fatalf("mark validation task for deletion: %v", err)
	}
	terminating := &corev1alpha1.Task{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(validationTask), terminating); err != nil {
		t.Fatalf("load terminating validation task: %v", err)
	}
	if terminating.DeletionTimestamp.IsZero() {
		t.Fatal("validation task is not terminating")
	}

	reconciler := &RepositoryMonitorReconciler{Client: k8sClient, Scheme: scheme, Store: monitorStore}
	record := &store.ReviewRecord{ValidationTask: validationTask.Name}
	cleaned, err := reconciler.cleanupRepositoryMonitorValidationTask(ctx, monitor, reviewTask, record)
	if err != nil || !cleaned {
		t.Fatalf("cleanupRepositoryMonitorValidationTask() = (%v, %v), want terminating task settled after Secret cleanup", cleaned, err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(commandSecret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("terminating validation task command Secret cleanup error = %v", err)
	}
}

func TestRepositoryMonitorValidationCleanupRetriesBindingStoreFailure(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	scheme := repositoryMonitorValidationTestScheme(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("validation-cleanup-binding-retry")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("validation-cleanup-binding-retry-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, repositoryMonitorTestHeadSHA)
	seedRepositoryMonitorValidationBindingForTest(t, ctx, monitorStore, monitor, reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	commandSecret := repositoryMonitorValidationCommandSecretForTest(reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, reviewTask, validationTask, commandSecret).Build()
	bindingErr := errors.New("temporary validation binding store outage")
	reconciler := &RepositoryMonitorReconciler{
		Client: k8sClient,
		Scheme: scheme,
		Store:  repositoryMonitorValidationBindingErrorStore{RepositoryMonitorStore: monitorStore, err: bindingErr},
	}
	record := &store.ReviewRecord{ValidationTask: validationTask.Name}

	cleaned, err := reconciler.cleanupRepositoryMonitorValidationTask(ctx, monitor, reviewTask, record)
	if cleaned || !errors.Is(err, bindingErr) {
		t.Fatalf("cleanupRepositoryMonitorValidationTask() = (%v, %v), want retryable binding error", cleaned, err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(validationTask), &corev1alpha1.Task{}); err != nil {
		t.Fatalf("validation task deleted before binding lookup recovered: %v", err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(commandSecret), &corev1.Secret{}); err != nil {
		t.Fatalf("validation command Secret deleted before binding lookup recovered: %v", err)
	}

	reconciler.Store = monitorStore
	cleaned, err = reconciler.cleanupRepositoryMonitorValidationTask(ctx, monitor, reviewTask, record)
	if err != nil || !cleaned {
		t.Fatalf("cleanupRepositoryMonitorValidationTask() after recovery = (%v, %v), want cleanup", cleaned, err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(validationTask), &corev1alpha1.Task{}); !apierrors.IsNotFound(err) {
		t.Fatalf("validation task cleanup error after recovery: %v", err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(commandSecret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("validation command Secret cleanup error after recovery: %v", err)
	}
}

func TestRepositoryMonitorValidationCleanupLeavesUnboundTaskUntouched(t *testing.T) {
	for _, phase := range []corev1alpha1.TaskPhase{
		corev1alpha1.TaskPhaseRunning,
		corev1alpha1.TaskPhaseSucceeded,
	} {
		t.Run(string(phase), func(t *testing.T) {
			ctx := context.Background()
			monitorStore := setupControllerSQLiteStore(t)
			scheme := repositoryMonitorValidationTestScheme(t)
			monitor := repositoryMonitorReviewIngestTestMonitor("unbound-validation-cleanup-" + strings.ToLower(string(phase)))
			monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
			reviewTask := repositoryMonitorReviewIngestTestTask(monitor.Name+"-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
			repositoryMonitorBindValidationForTest(reviewTask)
			validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, phase, repositoryMonitorTestHeadSHA)
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(monitor, reviewTask, validationTask).Build()
			reconciler := &RepositoryMonitorReconciler{Client: k8sClient, Scheme: scheme, Store: monitorStore}

			cleaned, err := reconciler.cleanupRepositoryMonitorValidationTask(ctx, monitor, reviewTask, &store.ReviewRecord{ValidationTask: validationTask.Name})
			if err != nil || !cleaned {
				t.Fatalf("cleanupRepositoryMonitorValidationTask() = (%v, %v), want unbound task ignored", cleaned, err)
			}
			remaining := &corev1alpha1.Task{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(validationTask), remaining); err != nil {
				t.Fatalf("unbound validation task was deleted: %v", err)
			}
			if remaining.Status.Phase != phase {
				t.Fatalf("unbound validation task phase = %q, want %q", remaining.Status.Phase, phase)
			}
		})
	}
}

func TestRepositoryMonitorValidationCleanupSettlesForeignTaskCollision(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	scheme := repositoryMonitorValidationTestScheme(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("foreign-validation-cleanup")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("foreign-validation-cleanup-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, repositoryMonitorTestHeadSHA)
	seedRepositoryMonitorValidationBindingForTest(t, ctx, monitorStore, monitor, reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	commandSecret := repositoryMonitorValidationCommandSecretForTest(reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	foreignTask := validationTask.DeepCopy()
	foreignTask.OwnerReferences = nil
	foreignTask.Labels = map[string]string{labels.LabelCreatedBy: "foreign-controller"}
	foreignTask.Annotations = nil
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, reviewTask, foreignTask, commandSecret).Build()
	reconciler := &RepositoryMonitorReconciler{Client: k8sClient, Scheme: scheme, Store: monitorStore}

	cleaned, err := reconciler.cleanupRepositoryMonitorValidationTask(ctx, monitor, reviewTask, &store.ReviewRecord{ValidationTask: validationTask.Name})
	if err != nil || !cleaned {
		t.Fatalf("cleanupRepositoryMonitorValidationTask() = (%v, %v), want foreign collision settled", cleaned, err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(foreignTask), &corev1alpha1.Task{}); err != nil {
		t.Fatalf("foreign validation-name Task was changed or deleted: %v", err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(commandSecret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("verified parent-owned command Secret cleanup error = %v", err)
	}
}

func TestRepositoryMonitorRejectedReviewCancelsAndCleansValidationTask(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	scheme := repositoryMonitorValidationTestScheme(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("rejected-validation-cleanup")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("rejected-validation-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	reviewTask.Status.Phase = corev1alpha1.TaskPhaseFailed
	reviewTask.Status.Message = "review runtime failed"
	validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseRunning, repositoryMonitorTestHeadSHA)
	seedRepositoryMonitorValidationBindingForTest(t, ctx, monitorStore, monitor, reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(monitor, reviewTask, validationTask).Build()
	reconciler := &RepositoryMonitorReconciler{Client: k8sClient, Scheme: scheme, Store: monitorStore, ResultStore: monitorStore}
	item := &store.MonitorItem{
		MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
		Kind: repositoryMonitorPullRequestKind, ItemKey: "1", Number: 1,
		State: repositoryMonitorItemStateOpen, HeadSHA: repositoryMonitorTestHeadSHA,
		LastVerdict: repositoryMonitorRunPhaseQueued, LastReviewID: reviewTask.Name,
	}
	if err := monitorStore.UpsertMonitorItem(ctx, item); err != nil {
		t.Fatal(err)
	}

	handled, err := reconciler.ingestCompletedRepositoryMonitorReviewTask(ctx, monitor, item, reviewTask)
	if err != nil || handled {
		t.Fatalf("first ingest = (%v, %v), want cancellation pending", handled, err)
	}
	currentValidation := &corev1alpha1.Task{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(validationTask), currentValidation); err != nil {
		t.Fatal(err)
	}
	if currentValidation.Status.Phase != corev1alpha1.TaskPhaseCancelled || !strings.Contains(currentValidation.Status.Message, "parent review ended") {
		t.Fatalf("validation task status = %#v, want cancelled by rejected review", currentValidation.Status)
	}
	records, _, err := monitorStore.ListReviewRecords(ctx, store.ReviewRecordFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, Number: 1, Limit: 1})
	if err != nil || len(records) != 1 {
		t.Fatalf("records after cancellation = %#v, err = %v, want one durable rejected record", records, err)
	}

	handled, err = reconciler.ingestCompletedRepositoryMonitorReviewTask(ctx, monitor, item, reviewTask)
	if err != nil || !handled {
		t.Fatalf("replayed ingest = (%v, %v), want cleanup and apply", handled, err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(validationTask), currentValidation); !apierrors.IsNotFound(err) {
		t.Fatalf("validation task remained after replay cleanup: %v", err)
	}
	updated, err := monitorStore.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, "1")
	if err != nil {
		t.Fatal(err)
	}
	if updated.LastReviewID != records[0].ID || updated.LastVerdict != repositoryMonitorReviewVerdictFailed {
		t.Fatalf("updated item = %#v, want rejected record applied after validation cleanup", updated)
	}
}

func TestRepositoryMonitorReviewValidationMissingBoundChildStaysRetryable(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	scheme := repositoryMonitorValidationTestScheme(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("validation-missing-bound-child")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("validation-missing-bound-child-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	expectedChild := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhasePending, repositoryMonitorTestHeadSHA)
	seedRepositoryMonitorValidationBindingForTest(t, ctx, monitorStore, monitor, reviewTask, expectedChild, repositoryMonitorValidationTestCommand)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, reviewTask).Build()
	reconciler := &RepositoryMonitorReconciler{Client: k8sClient, Scheme: scheme, Store: monitorStore, ResultStore: monitorStore}
	item := &store.MonitorItem{
		MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
		Kind: repositoryMonitorPullRequestKind, ItemKey: "1", Number: 1,
		State: repositoryMonitorItemStateOpen, HeadSHA: repositoryMonitorTestHeadSHA,
		LastVerdict: repositoryMonitorRunPhaseQueued, LastReviewID: reviewTask.Name,
	}
	if err := monitorStore.UpsertMonitorItem(ctx, item); err != nil {
		t.Fatal(err)
	}
	if err := monitorStore.SaveResult(ctx, reviewTask.Namespace, reviewTask.Name, repositoryMonitorReviewResultEnvelope(t, 1, repositoryMonitorTestHeadSHA, repositoryMonitorReviewVerdictPassed)); err != nil {
		t.Fatal(err)
	}

	handled, err := reconciler.ingestCompletedRepositoryMonitorReviewTask(ctx, monitor, item, reviewTask)
	if err != nil || !handled {
		t.Fatalf("ingest = (%v, %v), want handled", handled, err)
	}
	records, _, err := monitorStore.ListReviewRecords(ctx, store.ReviewRecordFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, Number: 1, Limit: 1})
	if err != nil || len(records) != 1 {
		t.Fatalf("records = %#v, err = %v", records, err)
	}
	if records[0].ValidationStatus != repositoryMonitorValidationStatusUnavailable ||
		records[0].ValidationCommandDigest != repositoryMonitorValidationTestCommandDigest ||
		!strings.Contains(records[0].ValidationEvidence, "could not be created") {
		t.Fatalf("validation result = %#v, want retryable missing bound child", records[0])
	}
	updated, err := monitorStore.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, "1")
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := reconciler.repositoryMonitorReviewedHeadFresh(ctx, monitor, updated, repositoryMonitorTestHeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	if fresh || updated.LastReviewedHeadSHA != "" {
		t.Fatalf("missing bound child marked head fresh: fresh=%v lastReviewedHeadSHA=%q", fresh, updated.LastReviewedHeadSHA)
	}
}

func TestRepositoryMonitorReviewValidationRequiresTaskImageBinding(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	scheme := repositoryMonitorValidationTestScheme(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("missing-validation-binding")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("missing-validation-binding-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	reviewTask.Spec.Workspace = &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentRead, GitRepo: repositoryMonitorTestRepoURL, Ref: repositoryMonitorTestHeadSHA}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, reviewTask).Build()
	reconciler := &RepositoryMonitorReconciler{Client: k8sClient, Scheme: scheme, Store: monitorStore, ResultStore: monitorStore}
	item := &store.MonitorItem{MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name, Kind: repositoryMonitorPullRequestKind, ItemKey: "1", Number: 1, State: repositoryMonitorItemStateOpen, HeadSHA: repositoryMonitorTestHeadSHA, LastVerdict: repositoryMonitorRunPhaseQueued, LastReviewID: reviewTask.Name}
	if err := monitorStore.UpsertMonitorItem(ctx, item); err != nil {
		t.Fatal(err)
	}
	if err := monitorStore.SaveResult(ctx, reviewTask.Namespace, reviewTask.Name, repositoryMonitorReviewResultEnvelope(t, 1, repositoryMonitorTestHeadSHA, repositoryMonitorReviewVerdictPassed)); err != nil {
		t.Fatal(err)
	}
	handled, err := reconciler.ingestCompletedRepositoryMonitorReviewTask(ctx, monitor, item, reviewTask)
	if err != nil || !handled {
		t.Fatalf("ingest = (%v, %v), want handled", handled, err)
	}
	records, _, err := monitorStore.ListReviewRecords(ctx, store.ReviewRecordFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, Number: 1, Limit: 1})
	if err != nil || len(records) != 1 {
		t.Fatalf("records = %#v, err = %v", records, err)
	}
	if records[0].Verdict != repositoryMonitorReviewVerdictNeedsChanges || records[0].ValidationStatus != repositoryMonitorValidationStatusFailed || records[0].ValidationImage != "" || !strings.Contains(records[0].ValidationEvidence, "missing") {
		t.Fatalf("record = %#v, want missing image binding to fail closed", records[0])
	}
	updated, err := monitorStore.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, "1")
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := reconciler.repositoryMonitorReviewedHeadFresh(ctx, monitor, updated, repositoryMonitorTestHeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	if fresh {
		t.Fatal("review created without the configured validation binding remained fresh")
	}
}

func TestRepositoryMonitorReviewValidationIgnoresUnexpectedMatchingTasks(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	scheme := repositoryMonitorValidationTestScheme(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("validation-exact-child")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("validation-exact-child-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseRunning, repositoryMonitorTestHeadSHA)
	unexpected := validationTask.DeepCopy()
	unexpected.Name = "unexpected-validation-task"
	unexpected.UID = types.UID("unexpected-validation-task")
	seedRepositoryMonitorValidationBindingForTest(t, ctx, monitorStore, monitor, reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	commandSecret := repositoryMonitorValidationCommandSecretForTest(reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, reviewTask, validationTask, unexpected, commandSecret).Build()
	reconciler := &RepositoryMonitorReconciler{Client: k8sClient, Scheme: scheme, Store: monitorStore}

	result, pending, err := reconciler.repositoryMonitorReviewValidation(ctx, monitor, reviewTask)
	if err != nil {
		t.Fatalf("repositoryMonitorReviewValidation() error = %v", err)
	}
	if !pending || result.TaskName != validationTask.Name || result.Status != repositoryMonitorValidationStatusNotRun {
		t.Fatalf("validation result = %#v, pending = %v, want exact child pending", result, pending)
	}
}

func TestRepositoryMonitorReviewValidationBindingStoreFailureStaysRetryableAndSkipsPublish(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	scheme := repositoryMonitorValidationTestScheme(t)

	monitor := repositoryMonitorReviewIngestTestMonitor("validation-binding-store-unavailable")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("validation-binding-store-unavailable-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseSucceeded, repositoryMonitorTestHeadSHA)
	seedRepositoryMonitorValidationBindingForTest(t, ctx, monitorStore, monitor, reviewTask, validationTask, repositoryMonitorValidationTestCommand)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, reviewTask, validationTask).Build()
	bindingErr := errors.New("temporary validation binding store outage")
	reconciler := &RepositoryMonitorReconciler{
		Client:      k8sClient,
		Scheme:      scheme,
		Store:       repositoryMonitorValidationBindingErrorStore{RepositoryMonitorStore: monitorStore, err: bindingErr},
		ResultStore: monitorStore,
	}
	item := &store.MonitorItem{
		MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
		Kind: repositoryMonitorPullRequestKind, ItemKey: "1", Number: 1,
		State: repositoryMonitorItemStateOpen, HeadSHA: repositoryMonitorTestHeadSHA,
		LastVerdict: repositoryMonitorRunPhaseQueued, LastReviewID: reviewTask.Name,
	}
	if err := monitorStore.UpsertMonitorItem(ctx, item); err != nil {
		t.Fatal(err)
	}
	if err := monitorStore.SaveResult(ctx, reviewTask.Namespace, reviewTask.Name, repositoryMonitorReviewResultEnvelope(t, 1, repositoryMonitorTestHeadSHA, repositoryMonitorReviewVerdictPassed)); err != nil {
		t.Fatal(err)
	}

	handled, err := reconciler.ingestCompletedRepositoryMonitorReviewTask(ctx, monitor, item, reviewTask)
	if handled || !errors.Is(err, bindingErr) {
		t.Fatalf("ingest = (%v, %v), want retryable binding error", handled, err)
	}
	records, _, err := monitorStore.ListReviewRecords(ctx, store.ReviewRecordFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, Number: 1, Limit: 1})
	if err != nil || len(records) != 0 {
		t.Fatalf("records = %#v, err = %v, want no immutable review before binding read recovers", records, err)
	}
	publishRecords, _, err := monitorStore.ListReviewPublishRecords(ctx, store.ReviewPublishRecordFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, Limit: 5})
	if err != nil || len(publishRecords) != 0 {
		t.Fatalf("publish records = %#v, err = %v, want no publish audit before binding read recovers", publishRecords, err)
	}
	reconciler.Store = monitorStore
	handled, err = reconciler.ingestCompletedRepositoryMonitorReviewTask(ctx, monitor, item, reviewTask)
	if err != nil || !handled {
		t.Fatalf("ingest after binding store recovery = (%v, %v), want handled", handled, err)
	}
	updated, err := monitorStore.GetMonitorItem(ctx, monitor.Namespace, monitor.Name, repositoryMonitorPullRequestKind, "1")
	if err != nil {
		t.Fatal(err)
	}
	if updated.LastReviewedHeadSHA != repositoryMonitorTestHeadSHA {
		t.Fatalf("LastReviewedHeadSHA = %q, want recovered passed validation head", updated.LastReviewedHeadSHA)
	}
	fresh, err := reconciler.repositoryMonitorReviewedHeadFresh(ctx, monitor, updated, repositoryMonitorTestHeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	if !fresh {
		t.Fatal("recovered passed validation did not mark the reviewed head fresh")
	}
	publishRecords, _, err = monitorStore.ListReviewPublishRecords(ctx, store.ReviewPublishRecordFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(publishRecords) != 1 || publishRecords[0].Phase != repositoryMonitorPublishPhaseSkipped || publishRecords[0].SkipReason != repositoryMonitorPublishSkipDisabled {
		t.Fatalf("publish records = %#v, want one publish_disabled audit after recovery", publishRecords)
	}
}

type repositoryMonitorValidationBindingErrorStore struct {
	store.RepositoryMonitorStore
	err error
}

func (s repositoryMonitorValidationBindingErrorStore) ListMonitorEvents(context.Context, store.MonitorEventFilter) ([]store.MonitorEvent, string, error) {
	return nil, "", s.err
}

func repositoryMonitorBindValidationForTest(task *corev1alpha1.Task) {
	if task.UID == "" {
		task.UID = types.UID("uid-" + task.Name)
	}
	task.Annotations[labels.AnnotationAgentReadOnly] = scheduledRunLabelValue
	task.Annotations[labels.AnnotationMonitorRunID] = "run-validation"
	task.Annotations[labels.AnnotationRepositoryValidationImage] = repositoryMonitorValidationTestImage
	task.Labels = map[string]string{
		labels.LabelCreatedBy:         "repository-monitor",
		labels.LabelRepositoryMonitor: labels.SelectorValue(task.Annotations[labels.AnnotationRepositoryMonitorName]),
		labels.LabelMonitorRun:        "run-validation",
		labels.LabelGitHubRepository:  labels.SelectorValue(task.Annotations[labels.AnnotationGitHubRepository]),
		labels.LabelGitHubTarget:      labels.SelectorValue(repositoryMonitorPullRequestKind),
		labels.LabelGitHubNumber:      task.Annotations[labels.AnnotationMonitorItemNumber],
	}
	task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: append(readOnlyAgentAllowedTools(), tools.RunValidationToolName, repositoryMonitorWaitForTasksToolName)}
	task.Spec.Workspace = &corev1alpha1.WorkspaceConfig{
		Intent:  corev1alpha1.WorkspaceIntentRead,
		GitRepo: repositoryMonitorTestRepoURL,
		Ref:     task.Annotations[labels.AnnotationMonitorHeadSHA],
	}
}

func seedRepositoryMonitorValidationBindingForTest(t *testing.T, ctx context.Context, bindingStore tools.RepositoryValidationBindingStore, monitor *corev1alpha1.RepositoryMonitor, reviewTask, validationTask *corev1alpha1.Task, command string) {
	t.Helper()
	event, err := tools.RepositoryValidationCommandBindingEvent(reviewTask, monitor, validationTask, repositoryMonitorValidationTestImage, reviewTask.Annotations[labels.AnnotationMonitorHeadSHA], command)
	if err != nil {
		t.Fatalf("RepositoryValidationCommandBindingEvent() error = %v", err)
	}
	if err := bindingStore.CreateMonitorEvent(ctx, event); err != nil {
		t.Fatalf("CreateMonitorEvent(command binding) error = %v", err)
	}
}

func repositoryMonitorValidationTaskForTest(monitor *corev1alpha1.RepositoryMonitor, reviewTask *corev1alpha1.Task, phase corev1alpha1.TaskPhase, workspaceRef string) *corev1alpha1.Task {
	controller := true
	resultAvailable := phase == corev1alpha1.TaskPhaseSucceeded
	timeout := metav1.Duration{Duration: tools.RepositoryValidationTimeout}
	return &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tools.RepositoryValidationTaskName(reviewTask),
			Namespace: reviewTask.Namespace,
			Labels: map[string]string{
				labels.LabelCreatedBy:         "repository-monitor",
				labels.LabelPurpose:           repositoryMonitorValidationPurpose,
				labels.LabelParentTask:        labels.SelectorValue(reviewTask.Name),
				labels.LabelRepositoryMonitor: labels.SelectorValue(monitor.Name),
			},
			Annotations: map[string]string{
				labels.AnnotationParentTaskName:                    reviewTask.Name,
				labels.AnnotationParentTaskUID:                     string(reviewTask.UID),
				labels.AnnotationRepositoryMonitorName:             reviewTask.Annotations[labels.AnnotationRepositoryMonitorName],
				labels.AnnotationMonitorRunID:                      reviewTask.Annotations[labels.AnnotationMonitorRunID],
				labels.AnnotationMonitorItemKind:                   reviewTask.Annotations[labels.AnnotationMonitorItemKind],
				labels.AnnotationMonitorItemNumber:                 reviewTask.Annotations[labels.AnnotationMonitorItemNumber],
				labels.AnnotationMonitorHeadSHA:                    reviewTask.Annotations[labels.AnnotationMonitorHeadSHA],
				labels.AnnotationGitHubRepository:                  reviewTask.Annotations[labels.AnnotationGitHubRepository],
				labels.AnnotationRepositoryValidationImage:         repositoryMonitorValidationTestImage,
				labels.AnnotationRepositoryValidationCommandDigest: repositoryMonitorValidationTestCommandDigest,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: corev1alpha1.GroupVersion.String(), Kind: "Task",
				Name: reviewTask.Name, UID: reviewTask.UID, Controller: &controller,
			}},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   repositoryMonitorValidationTestImage,
			Command: []string{"/bin/sh", "-c"},
			Args:    []string{"exit 125"},
			Timeout: &timeout,
			Workspace: &corev1alpha1.WorkspaceConfig{
				Intent:  corev1alpha1.WorkspaceIntentRead,
				GitRepo: reviewTask.Spec.Workspace.GitRepo,
				Ref:     workspaceRef,
			},
		},
		Status: corev1alpha1.TaskStatus{
			Phase: phase,
			ResultRef: &corev1alpha1.ResultReference{
				Available: resultAvailable,
			},
		},
	}
}

func repositoryMonitorValidationCommandSecretForTest(reviewTask, validationTask *corev1alpha1.Task, command string) *corev1.Secret {
	controller := true
	immutable := true
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tools.RepositoryValidationCommandSecretName(validationTask.Name),
			Namespace: validationTask.Namespace,
			Labels: map[string]string{
				labels.LabelManaged:    scheduledRunLabelValue,
				labels.LabelCreatedBy:  repositoryMonitorTaskCreatedBy,
				labels.LabelPurpose:    repositoryMonitorValidationPurpose,
				labels.LabelParentTask: labels.SelectorValue(reviewTask.Name),
			},
			Annotations: map[string]string{
				labels.AnnotationParentTaskName:            reviewTask.Name,
				labels.AnnotationParentTaskUID:             string(reviewTask.UID),
				labels.AnnotationRepositoryMonitorName:     validationTask.Annotations[labels.AnnotationRepositoryMonitorName],
				labels.AnnotationMonitorHeadSHA:            validationTask.Annotations[labels.AnnotationMonitorHeadSHA],
				labels.AnnotationRepositoryValidationImage: validationTask.Annotations[labels.AnnotationRepositoryValidationImage],
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: corev1alpha1.GroupVersion.String(), Kind: "Task",
				Name: reviewTask.Name, UID: reviewTask.UID, Controller: &controller,
			}},
		},
		Immutable: &immutable,
		Type:      corev1.SecretTypeOpaque,
		Data:      map[string][]byte{tools.RepositoryValidationCommandSecretKey: []byte(command)},
	}
}

func repositoryMonitorValidationTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func TestRepositoryMonitorValidationRejectsTransactionTokenSecret(t *testing.T) {
	monitor := repositoryMonitorReviewIngestTestMonitor("validation-token-secret")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("validation-token-secret-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhasePending, repositoryMonitorTestHeadSHA)
	validationTask.Annotations[labels.AnnotationTransactionTokenSecret] = "injected-token-secret"

	err := validateRepositoryMonitorValidationTask(monitor, reviewTask, validationTask, repositoryMonitorValidationTestImage)
	if err == nil || !strings.Contains(err.Error(), "transaction-token Secret") {
		t.Fatalf("validateRepositoryMonitorValidationTask() error = %v, want injected Secret rejection", err)
	}
}

func TestRenderRepositoryMonitorReviewBodyIncludesValidationEvidence(t *testing.T) {
	monitor := repositoryMonitorReviewIngestTestMonitor("render-validation")
	item := &store.MonitorItem{Number: 1}
	task := repositoryMonitorReviewIngestTestTask("render-validation-task", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	record := &store.ReviewRecord{
		ID: "review-1", HeadSHA: repositoryMonitorTestHeadSHA,
		Verdict: repositoryMonitorReviewVerdictPassed, Confidence: repositoryMonitorReviewConfidenceHigh,
		FindingsJSON: "[]", ValidationStatus: repositoryMonitorValidationStatusPassed,
		ValidationImage: repositoryMonitorValidationTestImage, ValidationCommandDigest: repositoryMonitorValidationTestCommandDigest,
		ValidationEvidence: "ok\nall packages passed",
	}
	body := renderRepositoryMonitorReviewBody(monitor, item, task, record, "publish-1", nil)
	renderedImage := strings.Replace(repositoryMonitorValidationTestImage, "@", "@\u200b", 1)
	for _, want := range []string{"**Status:** passed", renderedImage, "> all packages passed"} {
		if !strings.Contains(body, want) {
			t.Fatalf("rendered body missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, repositoryMonitorValidationTestCommand) {
		t.Fatalf("rendered body exposed the validation command:\n%s", body)
	}
}

func TestBoundRepositoryMonitorValidationEvidenceRedactsCredentials(t *testing.T) {
	secret := "ghp_" + strings.Repeat("a", 30)
	evidence := boundRepositoryMonitorValidationEvidence("validation failed with token=" + secret)
	if strings.Contains(evidence, secret) || !strings.Contains(evidence, "[REDACTED]") {
		t.Fatalf("validation evidence was not redacted: %q", evidence)
	}
}

func TestRepositoryMonitorValidationAllowsAutomergeRequiresCurrentPassedValidation(t *testing.T) {
	ctx := context.Background()
	monitorStore := setupControllerSQLiteStore(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("validation-automerge")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	item := &store.MonitorItem{
		MonitorNamespace: monitor.Namespace,
		MonitorName:      monitor.Name,
		Kind:             repositoryMonitorPullRequestKind,
		Number:           1,
		HeadSHA:          repositoryMonitorTestHeadSHA,
		LastReviewID:     "old-image-review",
	}
	reconciler := &RepositoryMonitorReconciler{Store: monitorStore}

	for _, record := range []*store.ReviewRecord{
		{
			ID: "old-image-review", MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
			Kind: repositoryMonitorPullRequestKind, Number: item.Number, HeadSHA: item.HeadSHA,
			Verdict: repositoryMonitorReviewVerdictPassed, ValidationStatus: repositoryMonitorValidationStatusPassed,
			ValidationImage: "ghcr.io/example/repo-validation@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		},
		{
			ID: "failed-validation-review", MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
			Kind: repositoryMonitorPullRequestKind, Number: item.Number, HeadSHA: item.HeadSHA,
			Verdict: repositoryMonitorReviewVerdictPassed, ValidationStatus: repositoryMonitorValidationStatusFailed,
			ValidationImage: repositoryMonitorValidationTestImage,
		},
		{
			ID: "passed-validation-review", MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
			Kind: repositoryMonitorPullRequestKind, Number: item.Number, HeadSHA: item.HeadSHA,
			Verdict: repositoryMonitorReviewVerdictPassed, ValidationStatus: repositoryMonitorValidationStatusPassed,
			ValidationImage: repositoryMonitorValidationTestImage,
		},
	} {
		if err := monitorStore.CreateReviewRecord(ctx, record); err != nil {
			t.Fatal(err)
		}
	}

	if repositoryMonitorValidationAllowsAutomergeForTest(t, reconciler, ctx, monitor, item) {
		t.Fatal("review using an old validation image allowed automerge")
	}
	item.LastReviewID = "failed-validation-review"
	if repositoryMonitorValidationAllowsAutomergeForTest(t, reconciler, ctx, monitor, item) {
		t.Fatal("failed validation allowed automerge")
	}
	item.LastReviewID = "passed-validation-review"
	if !repositoryMonitorValidationAllowsAutomergeForTest(t, reconciler, ctx, monitor, item) {
		t.Fatal("current passed validation did not allow automerge")
	}
	monitor.Spec.Validation.Image = ""
	if repositoryMonitorValidationAllowsAutomergeForTest(t, reconciler, ctx, monitor, item) {
		t.Fatal("review bound to a removed validation image allowed automerge")
	}
	withoutValidation := &store.ReviewRecord{
		ID: "no-validation-review", MonitorNamespace: monitor.Namespace, MonitorName: monitor.Name,
		Kind: repositoryMonitorPullRequestKind, Number: item.Number, HeadSHA: item.HeadSHA,
		Verdict: repositoryMonitorReviewVerdictPassed, ValidationStatus: repositoryMonitorValidationStatusNotRun,
	}
	if err := monitorStore.CreateReviewRecord(ctx, withoutValidation); err != nil {
		t.Fatal(err)
	}
	item.LastReviewID = withoutValidation.ID
	if !repositoryMonitorValidationAllowsAutomergeForTest(t, reconciler, ctx, monitor, item) {
		t.Fatal("review created without validation did not allow automerge when validation is disabled")
	}
}

func repositoryMonitorValidationAllowsAutomergeForTest(t *testing.T, reconciler *RepositoryMonitorReconciler, ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, item *store.MonitorItem) bool {
	t.Helper()
	allowed, err := reconciler.repositoryMonitorValidationAllowsAutomerge(ctx, monitor, item, item.HeadSHA)
	if err != nil {
		t.Fatalf("repositoryMonitorValidationAllowsAutomerge() error = %v", err)
	}
	return allowed
}
