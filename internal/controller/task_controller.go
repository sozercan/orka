/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	cron "github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	"github.com/orka-agents/orka/internal/approvals"
	"github.com/orka-agents/orka/internal/artifactcap"
	execevents "github.com/orka-agents/orka/internal/events"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/outboundaccess"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/internal/tracing"
	workerpkg "github.com/orka-agents/orka/internal/worker"
	"github.com/orka-agents/orka/internal/workerenv"
	"github.com/orka-agents/orka/internal/workspace/statusrules"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	taskTransactionTokenPendingTimeout            = 2 * time.Minute
	failedMountEventStaleAfter                    = 2 * time.Minute
	podLogLimitBytes                              = int64(5 << 20)
	stdoutResultLogLimitBytes                     = int64(15 << 20)
	maxResolvedApprovalsJSONForWorkerEnvBytes     = 32 * 1024
	maxRecentResolvedApprovalsForWorkerEnv        = 32
	maxResolvedApprovalWorkerEnvFieldBytes        = 512
	resolvedApprovalWorkerEnvJSONOverheadEstimate = 128
	workspaceFinalizationTimeout                  = 5 * time.Minute

	eventInvolvedObjectNameField = "involvedObject.name"
	eventReasonField             = "reason"
)

const (
	// ConditionTypeComplete indicates the task has completed
	ConditionTypeComplete = "Complete"

	// ConditionTypeJobCreated indicates a Job has been created
	ConditionTypeJobCreated = "JobCreated"

	// ConditionTypeWaitingForApproval indicates a running task is parked on a human approval.
	ConditionTypeWaitingForApproval = "WaitingForApproval"

	// jobCreationVisibilityGracePeriod avoids failing a task when the controller cache
	// has not observed the Job immediately after create.
	jobCreationVisibilityGracePeriod = 30 * time.Second

	workerRoleBindingRecreateInterval = 100 * time.Millisecond
	workerRoleBindingRecreateTimeout  = 5 * time.Second

	booleanTrueValue       = "true"
	scheduledRunLabelValue = booleanTrueValue
	managedLabelValue      = scheduledRunLabelValue

	workerRBACReconcileFailedReason = "WorkerRBACReconcileFailed"
)

// TaskReconciler reconciles a Task object
type TaskReconciler struct {
	client.Client
	APIReader                         client.Reader
	Scheme                            *runtime.Scheme
	JobBuilder                        *JobBuilder
	SessionManager                    *SessionManager
	WebhookNotifier                   *WebhookNotifier
	Recorder                          record.EventRecorder
	KubeClient                        kubernetes.Interface
	OutboundAccessResolver            outboundaccess.Resolver
	BrokeredTransactionExchange       *workerpkg.TransactionExchangeConfig
	ResultStore                       store.ResultStore
	PlanStore                         store.PlanStore
	MessageStore                      store.MessageStore
	ArtifactStore                     store.ArtifactStore
	ExecutionEventStore               store.ExecutionEventStore
	DurableControlStore               store.DurableControlStore
	AgentExecutionSnapshots           store.AgentExecutionSnapshotStore
	RepositoryValidationBindings      tools.RepositoryValidationBindingStore
	MCPRegistry                       *tools.Registry
	ACPArtifactRetirer                artifactcap.IdentityRetirer
	ACPPublicationReclaimer           ACPPublicationReclaimer
	ControllerEpochManager            *ControllerEpochManager
	ACPAdmissionGate                  *ACPAdmissionGate
	HarnessV1Enabled                  bool
	HarnessV1Endpoint                 string
	HarnessV1AuthSecretNamespace      string
	HarnessV1AuthSecretName           string
	HarnessV1AuthSecretKey            string
	HarnessV1Attempts                 store.HarnessV1AttemptStore
	HarnessV1SettlementAcknowledger   HarnessV1SettlementAcknowledger
	ACPRuntimeEnabled                 bool
	ACPRuntimeImages                  ACPRuntimeImages
	ACPRuntimeNamespace               string
	EnforceNamespaceIsolation         bool
	MaxTasksPerNamespace              int32
	ExecutionWorkspaceDefaultProvider corev1alpha1.WorkspaceProvider
	WorkspaceProviderAPIEnabled       bool
	// WorkspaceSettlementProtected reports that Task provenance admission
	// guards the reserved acp.workspace.orka.ai/ metadata settlement reads
	// from. When false (a cleanup-only installation without the webhook),
	// class settlement runs non-destructively: it never revokes or deletes a
	// workspace from forgeable Task metadata, and existing workspaces are
	// cleaned through explicit workspace deletion instead.
	WorkspaceSettlementProtected bool
	// ACPWorkspaceDispatchEnabled admits workspace-provider-backed ACP
	// RuntimeSession dispatch. When false, workspace-backed agent Tasks fail
	// closed before any workspace or RuntimePool demand exists.
	ACPWorkspaceDispatchEnabled       bool
	AgentSandboxEnabled               bool
	AgentSandboxConfig                AgentSandboxConfig
	SubstrateEnabled                  bool
	SubstrateConfig                   SubstrateConfig
	AIWorkerServiceAccountName        string
	VendorWorkerServiceAccountName    string
	ContainerWorkerServiceAccountName string
	AIWorkerClusterRoleName           string
	VendorWorkerClusterRoleName       string
	ContainerWorkerClusterRoleName    string
	WorkerRoleBindingNamePrefix       string
	EnforceTransactionCredentialAuth  bool
	TransactionCredentialReadScopes   []string
	OutboundAccessTrust               outboundaccess.TrustConfig
	trustedServiceCleanupMu           sync.RWMutex
	trustedServiceCleanupDone         bool
}

// +kubebuilder:rbac:groups=core.orka.ai,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.orka.ai,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.orka.ai,resources=tasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.orka.ai,resources=agents,verbs=get;list;watch
// +kubebuilder:rbac:groups=core.orka.ai,resources=tools,verbs=get;list;watch
// sessions is a virtual resource: the API server authorizes session endpoints with a
// SubjectAccessReview on core.orka.ai/sessions, so the manager role must grant it.
// +kubebuilder:rbac:groups=core.orka.ai,resources=sessions,verbs=get;list;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments;replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;roles;rolebindings,verbs=get;list;watch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=create;update;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=create;update;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,resourceNames=ai-worker-role;vendor-worker-role;container-worker-role,verbs=bind
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// The Events-v1 retention recorder needs write verbs: recording emits create
// and patch requests that the read-only grant rejects at the API server.
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=get;list;create;patch
// +kubebuilder:rbac:groups=ate.dev,resources=actortemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxwarmpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=agents.x-k8s.io,resources=sandboxes,verbs=get;list;watch

// updateStatusWithRetry updates the task status with retry on conflict.
// It re-fetches the task on conflict, applies the mutate function, and retries.
func (r *TaskReconciler) taskMetadataReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *TaskReconciler) patchTaskFinalizer(ctx context.Context, key types.NamespacedName, present bool) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &corev1alpha1.Task{}
		if err := r.taskMetadataReader().Get(ctx, key, current); err != nil {
			return err
		}
		hasFinalizer := controllerutil.ContainsFinalizer(current, labels.TaskFinalizer)
		if hasFinalizer == present {
			return nil
		}
		base := current.DeepCopy()
		if present {
			controllerutil.AddFinalizer(current, labels.TaskFinalizer)
		} else {
			controllerutil.RemoveFinalizer(current, labels.TaskFinalizer)
		}
		return r.Patch(ctx, current, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	})
}

func (r *TaskReconciler) updateStatusWithRetry(ctx context.Context, task *corev1alpha1.Task, mutate func(*corev1alpha1.Task)) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// On retry, re-fetch the latest version
		if err := r.Get(ctx, types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, task); err != nil {
			return err
		}
		mutate(task)
		return r.Status().Update(ctx, task)
	})
}

func childTaskStatusesEqual(a, b []corev1alpha1.ChildTaskStatus) bool {
	if len(a) != len(b) {
		return false
	}
	return slices.EqualFunc(a, b, func(left, right corev1alpha1.ChildTaskStatus) bool {
		return left.Name == right.Name &&
			left.Agent == right.Agent &&
			left.Phase == right.Phase &&
			left.Result == right.Result
	})
}

func canStartTaskJob(phase corev1alpha1.TaskPhase) bool {
	switch phase {
	case "", corev1alpha1.TaskPhasePending, corev1alpha1.TaskPhaseScheduled:
		return true
	default:
		return false
	}
}

func taskPhaseCountsTowardConcurrency(phase corev1alpha1.TaskPhase) bool {
	switch phase {
	case corev1alpha1.TaskPhasePending, corev1alpha1.TaskPhaseRunning, corev1alpha1.TaskPhaseFinalizing:
		return true
	default:
		return false
	}
}

// Reconcile handles the reconciliation loop for Task resources
func (r *TaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the Task instance
	task := &corev1alpha1.Task{}
	if err := r.Get(ctx, req.NamespacedName, task); err != nil {
		if apierrors.IsNotFound(err) {
			if cleanupErr := r.cleanupTrustedServiceReadBindingsAfterTaskRemoval(ctx, req.Namespace); cleanupErr != nil {
				return ctrl.Result{}, cleanupErr
			}
			return ctrl.Result{}, nil
		}
		log.Error(err, "unable to fetch Task")
		return ctrl.Result{}, err
	}
	if tx := task.Spec.Transaction; tx != nil {
		values := []any{}
		if tx.ID != "" {
			values = append(values, "transactionID", tx.ID)
		}
		if tx.Profile != "" {
			values = append(values, "contextTokenProfile", tx.Profile)
		}
		if tx.RequestingWorkload != "" {
			values = append(values, "requestingWorkload", tx.RequestingWorkload)
		}
		if len(values) > 0 {
			log = log.WithValues(values...)
			ctx = logf.IntoContext(ctx, log)
		}
	}

	spanAttributes := []attribute.KeyValue{
		attribute.String("task.name", task.Name),
		attribute.String("task.namespace", task.Namespace),
		attribute.String("task.type", string(task.Spec.Type)),
	}
	if tx := task.Spec.Transaction; tx != nil {
		if tx.ID != "" {
			spanAttributes = append(spanAttributes, attribute.String("transaction.id", tx.ID))
		}
		if tx.Profile != "" {
			spanAttributes = append(spanAttributes, attribute.String("context_token.profile", tx.Profile))
		}
	}

	if task.Spec.Schedule == "" {
		ctx = tracing.ExtractTaskTraceContext(ctx, task)
	}
	tracer := tracing.Tracer("orka.controller")
	ctx, span := tracer.Start(ctx, "task.reconcile",
		trace.WithAttributes(spanAttributes...),
	)
	defer span.End()

	// Handle deletion with finalizer
	if !task.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, task)
	}

	// Add the finalizer with a metadata-only optimistic patch. A full-object
	// update can re-serialize equivalent duration strings (for example 12m as
	// 12m0s) and trip immutable-spec admission rules.
	if !controllerutil.ContainsFinalizer(task, labels.TaskFinalizer) {
		if err := r.patchTaskFinalizer(ctx, req.NamespacedName, true); err != nil {
			log.Error(err, "failed to add finalizer")
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Initialize status if empty
	if task.Status.Phase == "" {
		if err := r.updateStatusWithRetry(ctx, task, func(t *corev1alpha1.Task) {
			t.Status.Phase = corev1alpha1.TaskPhasePending
		}); err != nil {
			log.Error(err, "failed to update initial status")
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return ctrl.Result{}, err
		}
		_ = r.recordTaskLifecycleEvent(
			ctx,
			task,
			execevents.ExecutionEventTypeTaskCreated,
			execevents.ExecutionEventSeverityInfo,
			"Task status initialized to Pending",
		)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	if err := r.projectACPExecutionWorkspaceStatus(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	// Handle based on current phase
	switch task.Status.Phase {
	case corev1alpha1.TaskPhasePending:
		return r.handlePending(ctx, task)
	case corev1alpha1.TaskPhaseScheduled:
		return r.handleScheduled(ctx, task)
	case corev1alpha1.TaskPhaseRunning:
		return r.handleRunning(ctx, task)
	case corev1alpha1.TaskPhaseFinalizing:
		return r.handleFinalizing(ctx, task)
	case corev1alpha1.TaskPhaseSucceeded, corev1alpha1.TaskPhaseFailed, corev1alpha1.TaskPhaseCancelled:
		return r.handleCompleted(ctx, task)
	}

	return ctrl.Result{}, nil
}

func (r *TaskReconciler) recordTaskLifecycleEvent(
	ctx context.Context,
	task *corev1alpha1.Task,
	eventType string,
	severity string,
	summary string,
) error {
	if r == nil || r.ExecutionEventStore == nil || task == nil {
		return nil
	}
	if strings.TrimSpace(task.Namespace) == "" || strings.TrimSpace(task.Name) == "" {
		return nil
	}
	_, err := r.ExecutionEventStore.AppendExecutionEvent(ctx, &store.ExecutionEvent{
		Namespace:   task.Namespace,
		StreamType:  store.ExecutionEventStreamTypeTask,
		StreamID:    task.Name,
		TaskName:    task.Name,
		SessionName: r.executionEventSessionName(ctx, task),
		Type:        eventType,
		Severity:    severity,
		Summary:     summary,
	})
	if err != nil {
		logf.FromContext(ctx).Error(
			err,
			"failed to record task lifecycle execution event",
			"namespace", task.Namespace,
			"task", task.Name,
			"eventType", eventType,
		)
		return err
	}
	return nil
}

func (r *TaskReconciler) executionEventSessionName(ctx context.Context, task *corev1alpha1.Task) string {
	sessionName := taskSessionName(task)
	if sessionName == "" {
		return ""
	}
	if r == nil || r.SessionManager == nil || r.SessionManager.store == nil {
		return ""
	}
	if _, err := r.SessionManager.GetSession(ctx, task.Namespace, sessionName); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ""
		}
		logf.FromContext(ctx).Error(
			err,
			"failed to check session before recording task lifecycle execution event",
			"namespace", task.Namespace,
			"task", task.Name,
			"session", sessionName,
		)
		return sessionName
	}
	return sessionName
}

func taskSessionName(task *corev1alpha1.Task) string {
	if task == nil || task.Spec.SessionRef == nil {
		return ""
	}
	return strings.TrimSpace(task.Spec.SessionRef.Name)
}

func executionEventSeverityForTaskPhase(phase corev1alpha1.TaskPhase) string {
	switch phase {
	case corev1alpha1.TaskPhaseFailed:
		return execevents.ExecutionEventSeverityError
	case corev1alpha1.TaskPhaseCancelled:
		return execevents.ExecutionEventSeverityWarning
	default:
		return execevents.ExecutionEventSeverityInfo
	}
}

func (r *TaskReconciler) recordTerminalTaskLifecycleEventIfMissing(ctx context.Context, task *corev1alpha1.Task) bool {
	if r == nil || r.ExecutionEventStore == nil || task == nil {
		return true
	}
	eventType := executionEventTypeForTaskPhase(task.Status.Phase)
	if eventType == "" {
		return true
	}
	listed, err := r.ExecutionEventStore.ListExecutionEvents(ctx, store.ExecutionEventFilter{
		Namespace:  task.Namespace,
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   task.Name,
		EventTypes: []string{eventType},
		Limit:      1,
	})
	if err != nil {
		logf.FromContext(ctx).Error(
			err,
			"failed to check existing terminal task lifecycle execution event",
			"namespace", task.Namespace,
			"task", task.Name,
			"eventType", eventType,
		)
		return false
	}
	if len(listed) > 0 {
		return true
	}
	return r.recordTaskLifecycleEvent(
		ctx,
		task,
		eventType,
		executionEventSeverityForTaskPhase(task.Status.Phase),
		task.Status.Message,
	) == nil
}

func executionEventTypeForTaskPhase(phase corev1alpha1.TaskPhase) string {
	switch phase {
	case corev1alpha1.TaskPhaseSucceeded:
		return execevents.ExecutionEventTypeTaskSucceeded
	case corev1alpha1.TaskPhaseFailed:
		return execevents.ExecutionEventTypeTaskFailed
	case corev1alpha1.TaskPhaseCancelled:
		return execevents.ExecutionEventTypeTaskCancelled
	default:
		return ""
	}
}

// handleDeletion handles Task cleanup when deleted.
//
//nolint:gocyclo,unparam // Cleanup ordering is intentionally kept in one auditable flow; Result is retained for consistency.
func (r *TaskReconciler) handleDeletion(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if controllerutil.ContainsFinalizer(task, labels.TaskFinalizer) {
		if taskManagedByHarnessV1(task) {
			ready, err := r.harnessV1TaskDeletionReady(ctx, task)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !ready {
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
		} else {
			ready, err := r.acpTaskDeletionReady(ctx, task)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !ready {
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			reclaimed, err := r.reclaimACPTaskPublicationBundles(ctx, task)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !reclaimed {
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			retired, err := r.retireACPArtifactIdentities(ctx, task)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !retired {
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			settled, err := r.settleACPClassWorkspace(ctx, task)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !settled {
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
		}
		// Clean up result data from store
		if r.ResultStore != nil {
			if err := r.ResultStore.DeleteResult(ctx, task.Namespace, task.Name); err != nil {
				log.Error(err, "failed to delete result from store", "task", task.Name)
				// Continue with finalizer removal anyway
			}
		}

		// Clean up artifacts
		if r.ArtifactStore != nil {
			if err := r.ArtifactStore.DeleteArtifacts(ctx, task.Namespace, task.Name); err != nil {
				log.Error(err, "failed to delete artifacts", "task", task.Name)
			}
		}

		// Clean up plan state if any
		if r.PlanStore != nil {
			if err := r.PlanStore.DeletePlan(ctx, task.Namespace, task.Name); err != nil {
				return ctrl.Result{}, fmt.Errorf("delete plan state for task %s/%s: %w", task.Namespace, task.Name, err)
			}
		}

		// Clean up inter-agent messages
		if r.MessageStore != nil {
			if err := r.MessageStore.DeleteTaskMessages(ctx, task.Namespace, task.Name); err != nil {
				log.Error(err, "failed to delete task messages", "task", task.Name)
			}
			// If this is a coordinator, clean up all children's messages
			if err := r.MessageStore.DeleteParentMessages(ctx, task.Namespace, task.Name); err != nil {
				log.Error(err, "failed to delete parent messages", "task", task.Name)
			}
		}

		// Clean up execution timeline events before allowing a future task with the
		// same namespace/name to expose stale history.
		if r.ExecutionEventStore != nil {
			if err := r.ExecutionEventStore.DeleteExecutionEvents(ctx, task.Namespace, store.ExecutionEventStreamTypeTask, task.Name); err != nil {
				log.Error(err, "failed to delete execution events", "task", task.Name)
				return ctrl.Result{}, err
			}
		}

		waitingForJob, err := r.cleanupDeletedTaskJob(ctx, task)
		if err != nil {
			log.Error(err, "failed to delete Job")
			return ctrl.Result{}, err
		}
		if waitingForJob {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		// Release session lock if held
		if task.Spec.SessionRef != nil {
			if err := r.SessionManager.ReleaseLock(ctx, task); err != nil {
				log.Error(err, "failed to release session lock")
				// Continue with finalizer removal anyway
			}
		}

		// Remove the finalizer with the same metadata-only patch used for
		// addition so immutable Task spec fields are never re-serialized.
		key := types.NamespacedName{Name: task.Name, Namespace: task.Namespace}
		if err := r.patchTaskFinalizer(ctx, key, false); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}

			log.Error(err, "failed to remove finalizer")
			return ctrl.Result{}, err
		}
	}
	if err := r.cleanupTrustedServiceReadBindingsAfterTaskRemoval(ctx, task.Namespace); err != nil {
		log.Error(err, "failed to clean up trusted Service RBAC after Task removal")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// handlePending handles Tasks in Pending phase.
//
//nolint:gocyclo // Pending routing keeps every mutually exclusive execution path in one auditable state machine.
func (r *TaskReconciler) handlePending(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if outcome := task.Status.ExecutionOutcome; outcome != nil {
		log.Info("refusing to replay task with immutable execution outcome", "outcome", outcome.Phase)
		return r.completeTask(ctx, task, outcome.Phase, outcome.Message)
	}
	if err := r.clearApprovalDecisionNudge(ctx, task); err != nil {
		log.Error(err, "failed to clear durable approval decision nudge")
		return ctrl.Result{}, err
	}

	if taskTransactionTokenPending(task) {
		return r.handleTransactionTokenPending(ctx, task)
	}

	// Scheduled parent Tasks own a recurring fire-time lifecycle; per-run timeout
	// enforcement begins on the generated child Task rather than at parent creation.
	if task.Spec.Schedule != "" {
		return r.handleScheduledTask(ctx, task)
	}

	// A persisted execution binding is the sole routing and recovery authority.
	// Do not resolve mutable Agent/configuration state, or acquire the legacy
	// SQLite Session lock, after a Task has been bound to either harness plane.
	if task.Spec.Type == corev1alpha1.TaskTypeAgent && task.Status.AgentExecutionBinding != nil {
		return r.handleBoundAgentTaskPending(ctx, task)
	}
	if task.Spec.Type == corev1alpha1.TaskTypeAgent && task.Status.Execution == nil {
		now := time.Now().UTC()
		if deadline, ok := r.pendingAgentTaskDeadline(ctx, task, now); ok && !now.Before(deadline) {
			return r.cancelACPTaskBeforeDurableAttempt(ctx, task, "task deadline exceeded before runtime admission")
		}
	}

	// Non-agent workers retain the legacy Session lock lifecycle. Agent Tasks
	// claim protocol lineage and a fenced SessionTurn in their dispatcher only
	// after the immutable execution binding has selected v1 or v2.
	if task.Spec.SessionRef != nil && task.Spec.Type != corev1alpha1.TaskTypeAgent {
		if result, err, locked := r.acquireSessionLock(ctx, task); locked {
			return result, err
		}
	}

	// Controller-owned validation children must be able to run while their
	// review parent occupies the namespace's only ordinary task slot.
	if r.MaxTasksPerNamespace > 0 {
		validationTask, err := r.repositoryMonitorValidationTask(ctx, task)
		if err != nil {
			log.Error(err, "failed to verify repository validation provenance for namespace limit")
			if errors.Is(err, errRepositoryMonitorValidationConfinement) {
				return r.failTask(ctx, task, err.Error())
			}
			return ctrl.Result{}, err
		}
		if !validationTask {
			var namespaceTasks corev1alpha1.TaskList
			if err := r.List(ctx, &namespaceTasks, client.InNamespace(task.Namespace)); err != nil {
				log.Error(err, "failed to list namespace tasks for limit check")
				return ctrl.Result{}, err
			}
			active := int32(0)
			for _, t := range namespaceTasks.Items {
				if t.Name != task.Name && taskPhaseCountsTowardConcurrency(t.Status.Phase) {
					active++
				}
			}
			if active >= r.MaxTasksPerNamespace {
				log.Info("namespace task limit reached, requeueing",
					"namespace", task.Namespace,
					"active", active,
					"limit", r.MaxTasksPerNamespace,
				)
				r.Recorder.Eventf(task, corev1.EventTypeNormal, "NamespaceTaskLimitReached",
					"namespace %q has %d active tasks (limit: %d), requeueing", task.Namespace, active, r.MaxTasksPerNamespace)
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
		}
	}

	// Resolve agent if referenced
	agent, err := r.resolveAgent(ctx, task)
	if err != nil {
		log.Error(err, "failed to resolve agent")
		return r.failTask(ctx, task, err.Error())
	}

	// Validate task-agent compatibility
	if err := r.validateTaskAgentCompatibility(task, agent); err != nil {
		log.Error(err, "task-agent compatibility validation failed")
		return r.failTask(ctx, task, err.Error())
	}

	if err := r.validateExecutionWorkspace(task); err != nil {
		log.Error(err, "execution workspace validation failed")
		if statusErr := r.markExecutionWorkspaceValidationFailed(ctx, task, err); statusErr != nil {
			log.Error(statusErr, "failed to update execution workspace validation status")
			return ctrl.Result{}, statusErr
		}
		return r.failTask(ctx, task, err.Error())
	}

	// Validate coordination constraints for child tasks
	if result, err, done := r.validateCoordinationConstraints(ctx, task); done {
		return result, err
	}

	// Resolve provider if referenced
	provider, err := r.resolveProvider(ctx, task, agent)
	if err != nil {
		log.Error(err, "failed to resolve provider")
		return r.failTask(ctx, task, err.Error())
	}

	if task.Spec.Type == corev1alpha1.TaskTypeAgent {
		plan := r.planAgentExecution(ctx, task, agent)
		switch plan.path {
		case agentExecutionPathRejected:
			return r.rejectPlannedAgentExecution(ctx, task, plan)
		case agentExecutionPathACP:
			if result, err, handled := r.ensureAgentExecutionBinding(ctx, task, agent); handled {
				return result, err
			}
			return r.queueACPRuntimeTask(ctx, task, agent)
		case agentExecutionPathHarnessV1:
			if result, err, handled := r.ensureHarnessV1ExecutionBinding(ctx, task, agent); handled {
				return result, err
			}
			return r.queueHarnessV1Task(ctx, task)
		default:
			return ctrl.Result{}, fmt.Errorf("unknown agent execution path %q", plan.path)
		}
	}

	return r.createTaskJob(ctx, task, agent, provider)
}

func (r *TaskReconciler) pendingAgentTaskDeadline(
	ctx context.Context,
	task *corev1alpha1.Task,
	now time.Time,
) (time.Time, bool) {
	if deadline, ok := acpTaskDeadline(task, now); ok {
		return deadline, true
	}
	if task == nil || task.Spec.Type != corev1alpha1.TaskTypeAgent || task.Status.AgentExecutionBinding != nil {
		return time.Time{}, false
	}
	deadline := taskDeadlineFromTimeout(task, now, defaultACPTaskTimeout)
	if now.Before(deadline) {
		return time.Time{}, false
	}
	agent, err := r.resolveAgent(ctx, task)
	if err != nil || r.planAgentExecution(ctx, task, agent).path != agentExecutionPathACP {
		return time.Time{}, false
	}
	return deadline, true
}

func (r *TaskReconciler) handleBoundAgentTaskPending(
	ctx context.Context,
	task *corev1alpha1.Task,
) (ctrl.Result, error) {
	binding := task.Status.AgentExecutionBinding
	if binding == nil {
		return ctrl.Result{}, errors.New("bound agent Task is missing its execution binding")
	}
	switch binding.ContractVersion {
	case corev1alpha1.AgentRuntimeContractHarnessV1:
		if result, err, handled := r.ensureHarnessV1ExecutionBinding(ctx, task, nil); handled {
			return result, err
		}
		return r.queueHarnessV1Task(ctx, task)
	case corev1alpha1.AgentRuntimeContractHarnessV2:
		if task.Status.Execution == nil {
			now := time.Now().UTC()
			if deadline, ok := acpTaskDeadline(task, now); ok && !now.Before(deadline) {
				return r.cancelACPTaskBeforeDurableAttempt(ctx, task, "task deadline exceeded before runtime admission")
			}
		}
		if result, err, handled := r.ensureAgentExecutionBinding(ctx, task, nil); handled {
			return result, err
		}
		return r.queueACPRuntimeTask(ctx, task, nil)
	default:
		return r.failTask(ctx, task, fmt.Sprintf(
			"immutable execution binding has unsupported contract %q", binding.ContractVersion,
		))
	}
}

func taskTransactionTokenPending(task *corev1alpha1.Task) bool {
	if task == nil || task.Annotations == nil {
		return false
	}
	pending, err := strconv.ParseBool(task.Annotations[labels.AnnotationTransactionTokenPending])
	return err == nil && pending
}

func (r *TaskReconciler) handleTransactionTokenPending(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	now := time.Now()
	since, err := transactionTokenPendingSince(task)
	if err != nil {
		patch := client.MergeFrom(task.DeepCopy())
		if task.Annotations == nil {
			task.Annotations = map[string]string{}
		}
		task.Annotations[labels.AnnotationTransactionTokenPendingSince] = now.Format(time.RFC3339Nano)
		if updateErr := r.Patch(ctx, task, patch); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		log.Info("task is waiting for delegated transaction token setup", "pendingSinceInitialized", true)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	elapsed := now.Sub(since)
	if elapsed >= taskTransactionTokenPendingTimeout {
		msg := fmt.Sprintf("delegated transaction token setup timed out after %s", taskTransactionTokenPendingTimeout)
		r.Recorder.Event(task, corev1.EventTypeWarning, "TransactionTokenPendingTimeout", msg)
		return r.failTask(ctx, task, msg)
	}

	requeueAfter := min(taskTransactionTokenPendingTimeout-elapsed, time.Second)
	log.Info("task is waiting for delegated transaction token setup", "pendingSince", since)
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func transactionTokenPendingSince(task *corev1alpha1.Task) (time.Time, error) {
	if task == nil || task.Annotations == nil {
		return time.Time{}, fmt.Errorf("missing transaction token pending timestamp")
	}
	value := strings.TrimSpace(task.Annotations[labels.AnnotationTransactionTokenPendingSince])
	if value == "" {
		return time.Time{}, fmt.Errorf("missing transaction token pending timestamp")
	}
	since, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid transaction token pending timestamp: %w", err)
	}
	return since, nil
}

// handleScheduledTask handles transition to Scheduled phase for cron-scheduled tasks.
func (r *TaskReconciler) handleScheduledTask(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	sched, err := parser.Parse(task.Spec.Schedule)
	if err != nil {
		return r.failTask(ctx, task, fmt.Sprintf("invalid cron expression: %v", err))
	}

	now := time.Now()
	if task.Spec.TimeZone != nil {
		if loc, err := time.LoadLocation(*task.Spec.TimeZone); err == nil {
			now = now.In(loc)
		}
	}
	next := sched.Next(now)

	task.Status.Phase = corev1alpha1.TaskPhaseScheduled
	task.Status.NextScheduleTime = &metav1.Time{Time: next}
	task.Status.Message = fmt.Sprintf("Scheduled with cron: %s", task.Spec.Schedule)
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("Task scheduled", "schedule", task.Spec.Schedule, "nextRun", next)
	return ctrl.Result{RequeueAfter: time.Until(next)}, nil
}

// acquireSessionLock checks and acquires a session lock. Returns (result, err, locked)
// where locked=true means the caller should return the result/err immediately.
func (r *TaskReconciler) acquireSessionLock(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, error, bool) {
	log := logf.FromContext(ctx)

	locked, err := r.SessionManager.IsLocked(ctx, task)
	if err != nil {
		if errors.Is(err, store.ErrValidation) {
			result, failErr := r.failTask(ctx, task, err.Error())
			return result, failErr, true
		}
		log.Error(err, "failed to check session lock")
		return ctrl.Result{}, err, true
	}
	if locked {
		log.Info("session is locked, waiting", "session", task.Spec.SessionRef.Name)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil, true
	}

	if err := r.SessionManager.AcquireLock(ctx, task); err != nil {
		if strings.Contains(err.Error(), "already locked") {
			session, getErr := r.SessionManager.GetSession(ctx, task.Namespace, task.Spec.SessionRef.Name)
			if getErr != nil {
				return ctrl.Result{}, getErr, true
			}
			if session.ActiveTask == task.Name && (session.ActiveTaskUID == "" || session.ActiveTaskUID == string(task.UID)) {
				return ctrl.Result{}, nil, false
			}
			if session.ActiveTask == "" {
				return ctrl.Result{RequeueAfter: time.Second}, nil, true
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil, true
		}
		if errors.Is(err, store.ErrNotReady) {
			log.Info("gateway session ownership linkage is pending", "session", task.Spec.SessionRef.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil, true
		}
		log.Error(err, "failed to acquire session lock")
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrValidation) {
			result, failErr := r.failTask(ctx, task, err.Error())
			return result, failErr, true
		}
		return ctrl.Result{}, err, true
	}
	return ctrl.Result{}, nil, false
}

// resolveAgent fetches the Agent referenced by the task, if any.
func (r *TaskReconciler) resolveAgent(ctx context.Context, task *corev1alpha1.Task) (*corev1alpha1.Agent, error) {
	if task.Spec.AgentRef == nil {
		return nil, nil
	}
	agent := &corev1alpha1.Agent{}
	agentNS := task.Spec.AgentRef.Namespace
	if agentNS == "" {
		agentNS = task.Namespace
	}
	if r.EnforceNamespaceIsolation && agentNS != task.Namespace {
		r.Recorder.Eventf(task, corev1.EventTypeWarning, "NamespaceIsolationViolation",
			"cross-namespace agent reference not allowed: agent %q is in namespace %q", task.Spec.AgentRef.Name, agentNS)
		return nil, fmt.Errorf("cross-namespace agent reference not allowed when namespace isolation is enforced: agent %q in namespace %q, task in %q", task.Spec.AgentRef.Name, agentNS, task.Namespace)
	}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      task.Spec.AgentRef.Name,
		Namespace: agentNS,
	}, agent); err != nil {
		return nil, fmt.Errorf("failed to get agent: %v", err)
	}
	return agent, nil
}

// validateCoordinationConstraints validates depth, allowed agents, and concurrency for child tasks.
// Returns (result, err, done) where done=true means the caller should return the result/err.
func (r *TaskReconciler) validateCoordinationConstraints(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, error, bool) {
	depthStr, ok := task.Annotations[labels.AnnotationCoordinationDepth]
	if !ok {
		return ctrl.Result{}, nil, false
	}

	log := logf.FromContext(ctx)
	parentName := labels.ParentTaskName(task.Labels, task.Annotations)
	depthValue, parseErr := strconv.ParseInt(depthStr, 10, 32)
	if parseErr != nil || depthValue < 0 || strconv.FormatInt(depthValue, 10) != depthStr {
		result, err := r.failTask(ctx, task, "coordination depth annotation is invalid")
		return result, err, true
	}
	depth := int32(depthValue)

	// Look up parent task to find its agent's coordination config
	parentTask := &corev1alpha1.Task{}
	if err := r.Get(ctx, types.NamespacedName{Name: parentName, Namespace: task.Namespace}, parentTask); err != nil {
		log.Error(err, "failed to get parent task")
		result, err := r.failTask(ctx, task, fmt.Sprintf("failed to get parent task: %v", err))
		return result, err, true
	}

	parentAgent := &corev1alpha1.Agent{}
	if parentTask.Spec.AgentRef != nil {
		agentNS := parentTask.Spec.AgentRef.Namespace
		if agentNS == "" {
			agentNS = task.Namespace
		}
		if err := r.Get(ctx, types.NamespacedName{Name: parentTask.Spec.AgentRef.Name, Namespace: agentNS}, parentAgent); err != nil {
			log.Error(err, "failed to get parent agent")
			result, err := r.failTask(ctx, task, fmt.Sprintf("failed to get parent agent: %v", err))
			return result, err, true
		}
	}

	coord := parentAgent.Spec.Coordination
	if coord == nil || !coord.Enabled {
		result, err := r.failTask(ctx, task, "parent agent does not have coordination enabled")
		return result, err, true
	}

	// Enforce maxDepth
	if coord.MaxDepth > 0 && depth > coord.MaxDepth {
		result, err := r.failTask(ctx, task, fmt.Sprintf("coordination depth %d exceeds max %d", depth, coord.MaxDepth))
		return result, err, true
	}

	// Enforce allowedAgents
	if task.Spec.AgentRef != nil {
		allowed := false
		for _, a := range coord.AllowedAgents {
			if a.Name == task.Spec.AgentRef.Name {
				allowed = true
				break
			}
		}
		// Allow agents dynamically created by the parent task via create_agent tool
		if !allowed {
			childAgent := &corev1alpha1.Agent{}
			agentNS := task.Spec.AgentRef.Namespace
			if agentNS == "" {
				agentNS = task.Namespace
			}
			if err := r.Get(ctx, types.NamespacedName{Name: task.Spec.AgentRef.Name, Namespace: agentNS}, childAgent); err == nil {
				if childAgent.Labels[labels.LabelCreatedBy] == "create_agent" && labels.ParentTaskName(childAgent.Labels, childAgent.Annotations) == parentName {
					allowed = true
				}
			}
		}
		if !allowed {
			result, err := r.failTask(ctx, task, fmt.Sprintf("agent %q not in parent's allowedAgents", task.Spec.AgentRef.Name))
			return result, err, true
		}
	}

	// Enforce maxConcurrentChildren (requeue if at limit)
	if coord.MaxConcurrentChildren > 0 {
		var siblings corev1alpha1.TaskList
		if err := r.List(ctx, &siblings, client.InNamespace(task.Namespace),
			client.MatchingLabels{labels.LabelParentTask: labels.SelectorValue(parentName)}); err != nil {
			log.Error(err, "failed to list sibling tasks")
			return ctrl.Result{}, err, true
		}
		active := int32(0)
		for _, s := range siblings.Items {
			if s.Name != task.Name && taskPhaseCountsTowardConcurrency(s.Status.Phase) {
				active++
			}
		}
		if active >= coord.MaxConcurrentChildren {
			log.Info("coordination concurrency limit reached", "active", active, "max", coord.MaxConcurrentChildren)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil, true
		}
	}

	return ctrl.Result{}, nil, false
}

// resolveProvider fetches the Provider referenced by the task or agent, if any.
func (r *TaskReconciler) resolveProvider(ctx context.Context, task *corev1alpha1.Task, agent *corev1alpha1.Agent) (*corev1alpha1.Provider, error) {
	providerRef := r.resolveProviderRef(task, agent)
	if providerRef == nil {
		return nil, nil
	}
	provider := &corev1alpha1.Provider{}
	providerNS := providerRef.Namespace
	if providerNS == "" {
		providerNS = task.Namespace
	}
	if r.EnforceNamespaceIsolation && providerNS != task.Namespace {
		r.Recorder.Eventf(task, corev1.EventTypeWarning, "NamespaceIsolationViolation",
			"cross-namespace provider reference not allowed: provider %q is in namespace %q", providerRef.Name, providerNS)
		return nil, fmt.Errorf("cross-namespace provider reference not allowed when namespace isolation is enforced: provider %q in namespace %q, task in %q", providerRef.Name, providerNS, task.Namespace)
	}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      providerRef.Name,
		Namespace: providerNS,
	}, provider); err != nil {
		return nil, fmt.Errorf("failed to get provider: %v", err)
	}
	if !provider.Status.Ready {
		return nil, fmt.Errorf("provider %s is not ready: %s", providerRef.Name, provider.Status.Message)
	}
	return provider, nil
}

func taskNeedsApprovalState(task *corev1alpha1.Task, agent *corev1alpha1.Agent) bool {
	return task != nil &&
		task.Spec.Type == corev1alpha1.TaskTypeAI &&
		agent != nil &&
		agent.Spec.Coordination != nil &&
		agent.Spec.Coordination.Autonomous
}

func (r *TaskReconciler) resolvedApprovalsJSONForTask(ctx context.Context, task *corev1alpha1.Task) (string, error) {
	if r == nil || r.ExecutionEventStore == nil || task == nil {
		return "", nil
	}
	listed, err := approvals.ListEvents(ctx, r.ExecutionEventStore, task.Namespace, task.Name)
	if err != nil {
		return "", err
	}
	// Match parking semantics: only explicit terminal approval events are injected.
	// V1 does not persist consumed-action markers in Orka; workers pass the
	// approval ID as the downstream idempotency key, so cross-Job duplicate
	// suppression remains the downstream service's responsibility.
	resolved := approvals.Resolved(approvals.Derive(
		approvals.FilterEventsForTaskUID(listed, string(task.UID)),
		time.Time{},
	))
	return resolvedApprovalsJSONForWorkerEnv(resolved)
}

func resolvedApprovalsJSONForWorkerEnv(values []approvals.ResolvedApproval) (string, error) {
	if len(values) == 0 {
		return "", nil
	}

	if resolvedApprovalsLikelyFitWorkerEnv(values, resolvedApprovalWorkerEnvFullPayload) {
		bounded := append([]approvals.ResolvedApproval(nil), values...)
		if data, ok, err := marshalResolvedApprovalsForWorkerEnv(bounded); err != nil || ok {
			return data, err
		}
	}

	if resolvedApprovalsLikelyFitWorkerEnv(values, resolvedApprovalWorkerEnvNoPreviewPayload) {
		withoutPreviews := append([]approvals.ResolvedApproval(nil), values...)
		for i := range withoutPreviews {
			withoutPreviews[i].TargetArgsPreview = nil
		}
		if data, ok, err := marshalResolvedApprovalsForWorkerEnv(withoutPreviews); err != nil || ok {
			return data, err
		}
	}

	compact := compactResolvedApprovalsForWorkerEnv(values)
	if resolvedApprovalsLikelyFitWorkerEnv(compact, resolvedApprovalWorkerEnvCompactPayload) {
		if data, ok, err := marshalResolvedApprovalsForWorkerEnv(compact); err != nil || ok {
			return data, err
		}
	}

	selected, err := selectResolvedApprovalsForWorkerEnv(compact)
	if err != nil || len(selected) == 0 {
		return "", err
	}
	data, ok, err := marshalResolvedApprovalsForWorkerEnv(selected)
	if err != nil || !ok {
		return "", err
	}
	return data, nil
}

func marshalResolvedApprovalsForWorkerEnv(values []approvals.ResolvedApproval) (string, bool, error) {
	data, err := json.Marshal(values)
	if err != nil {
		return "", false, err
	}
	if len(data) > maxResolvedApprovalsJSONForWorkerEnvBytes {
		return "", false, nil
	}
	return string(data), true, nil
}

type resolvedApprovalWorkerEnvPayload int

const (
	resolvedApprovalWorkerEnvFullPayload resolvedApprovalWorkerEnvPayload = iota
	resolvedApprovalWorkerEnvNoPreviewPayload
	resolvedApprovalWorkerEnvCompactPayload
)

func resolvedApprovalsLikelyFitWorkerEnv(
	values []approvals.ResolvedApproval,
	payload resolvedApprovalWorkerEnvPayload,
) bool {
	estimated := 2
	for _, approval := range values {
		estimated += resolvedApprovalWorkerEnvJSONOverheadEstimate
		estimated += len(approval.ID) + len(approval.TaskUID) + len(approval.TargetTool)
		estimated += len(approval.TargetArgsDigest) + len(approval.TargetSpecDigest) + len(approval.Status)
		if payload != resolvedApprovalWorkerEnvCompactPayload {
			estimated += len(approval.Actor) + len(approval.DecisionTime) + len(approval.Reason)
			estimated += len(approval.Action) + len(approval.RiskSummary) + len(approval.Severity)
		}
		if payload == resolvedApprovalWorkerEnvFullPayload {
			estimated += len(approval.TargetArgsPreview)
		}
		if estimated > maxResolvedApprovalsJSONForWorkerEnvBytes {
			return false
		}
	}
	return true
}

func resolvedApprovalWorkerEnvField(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= maxResolvedApprovalWorkerEnvFieldBytes {
		return value
	}
	return value[:maxResolvedApprovalWorkerEnvFieldBytes]
}

func compactResolvedApprovalsForWorkerEnv(values []approvals.ResolvedApproval) []approvals.ResolvedApproval {
	compact := make([]approvals.ResolvedApproval, 0, len(values))
	for _, approval := range values {
		compact = append(compact, approvals.ResolvedApproval{
			ID:               resolvedApprovalWorkerEnvField(approval.ID),
			TaskUID:          resolvedApprovalWorkerEnvField(approval.TaskUID),
			TargetTool:       resolvedApprovalWorkerEnvField(approval.TargetTool),
			TargetArgsDigest: resolvedApprovalWorkerEnvField(approval.TargetArgsDigest),
			TargetSpecDigest: resolvedApprovalWorkerEnvField(approval.TargetSpecDigest),
			Status:           resolvedApprovalWorkerEnvField(approval.Status),
		})
	}
	return compact
}

func selectResolvedApprovalsForWorkerEnv(values []approvals.ResolvedApproval) ([]approvals.ResolvedApproval, error) {
	selected := make([]approvals.ResolvedApproval, 0, min(len(values), maxRecentResolvedApprovalsForWorkerEnv))
	selectedIndexes := make(map[int]struct{}, min(len(values), maxRecentResolvedApprovalsForWorkerEnv))

	// Always reserve space for the newest decisions first so recent approvals can
	// resume required tool calls even when a long history contains many old denials.
	for i := len(values) - 1; i >= 0 && len(selectedIndexes) < maxRecentResolvedApprovalsForWorkerEnv; i-- {
		var added bool
		var err error
		selected, added, err = appendResolvedApprovalIfWorkerEnvFits(selected, values[i])
		if err != nil {
			return nil, err
		}
		if added {
			selectedIndexes[i] = struct{}{}
		}
	}

	// Add older blocking terminal decisions before older approvals. Dropping an
	// old approval can re-request approval; dropping an old decline/expiry can
	// allow a previously denied target to execute.
	omittedBlocking := false
	for i, approval := range values {
		if !resolvedApprovalBlocksExecution(approval) {
			continue
		}
		if _, ok := selectedIndexes[i]; ok {
			continue
		}
		var added bool
		var err error
		selected, added, err = appendResolvedApprovalIfWorkerEnvFits(selected, approval)
		if err != nil {
			return nil, err
		}
		if added {
			selectedIndexes[i] = struct{}{}
		} else {
			omittedBlocking = true
		}
	}
	if omittedBlocking {
		var err error
		selected, err = ensureBlockingOverflowSentinelFitsWorkerEnv(selected)
		if err != nil {
			return nil, err
		}
	}

	for i := len(values) - 1; i >= 0; i-- {
		if _, ok := selectedIndexes[i]; ok {
			continue
		}
		if resolvedApprovalBlocksExecution(values[i]) {
			continue
		}
		var added bool
		var err error
		selected, added, err = appendResolvedApprovalIfWorkerEnvFits(selected, values[i])
		if err != nil {
			return nil, err
		}
		if added {
			selectedIndexes[i] = struct{}{}
		}
	}
	return selected, nil
}

func ensureBlockingOverflowSentinelFitsWorkerEnv(
	selected []approvals.ResolvedApproval,
) ([]approvals.ResolvedApproval, error) {
	sentinel := approvals.BlockingOverflowResolvedApproval()
	if slices.ContainsFunc(selected, approvals.IsResolvedApprovalBlockingOverflow) {
		return selected, nil
	}
	for {
		candidate := append([]approvals.ResolvedApproval{sentinel}, selected...)
		if _, ok, err := marshalResolvedApprovalsForWorkerEnv(candidate); err != nil {
			return nil, err
		} else if ok {
			return candidate, nil
		}
		if len(selected) == 0 {
			return nil, fmt.Errorf("blocking approval overflow sentinel does not fit worker env budget")
		}
		drop := len(selected) - 1
		selected = append(selected[:drop], selected[drop+1:]...)
	}
}

func appendResolvedApprovalIfWorkerEnvFits(
	selected []approvals.ResolvedApproval,
	approval approvals.ResolvedApproval,
) ([]approvals.ResolvedApproval, bool, error) {
	candidate := append(append([]approvals.ResolvedApproval(nil), selected...), approval)
	if _, ok, err := marshalResolvedApprovalsForWorkerEnv(candidate); err != nil {
		return nil, false, err
	} else if ok {
		return candidate, true, nil
	}
	return selected, false, nil
}

func resolvedApprovalBlocksExecution(approval approvals.ResolvedApproval) bool {
	status := strings.TrimSpace(approval.Status)
	return status != "" && status != approvals.StatusApproved
}

// createTaskJob builds the Job, sets owner reference, creates it, and updates the task status.
func (r *TaskReconciler) createTaskJob(ctx context.Context, task *corev1alpha1.Task, agent *corev1alpha1.Agent, provider *corev1alpha1.Provider) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	latest := &corev1alpha1.Task{}
	if err := r.Get(ctx, types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, latest); err != nil {
		return ctrl.Result{}, err
	}
	if !canStartTaskJob(latest.Status.Phase) || executionOutcomePreventsReplay(latest.Status.ExecutionOutcome) {
		task.Status = latest.Status
		log.Info("skipping job creation because task is no longer runnable", "phase", latest.Status.Phase)
		return ctrl.Result{}, nil
	}
	validationTask, err := r.repositoryMonitorValidationTask(ctx, latest)
	if err != nil {
		log.Error(err, "failed to verify repository validation provenance")
		if errors.Is(err, errRepositoryMonitorValidationConfinement) {
			return r.failTask(ctx, task, err.Error())
		}
		return ctrl.Result{}, err
	}
	gateReady, err := r.ensureRepositoryMonitorValidationNetworkGateForTask(ctx, latest, validationTask)
	if err != nil {
		log.Error(err, "failed to prepare repository validation network gate")
		if errors.Is(err, errRepositoryMonitorValidationConfinement) {
			return r.failTask(ctx, task, err.Error())
		}
		return ctrl.Result{}, err
	}
	if !gateReady {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	jobTask := task
	if validationTask {
		// Render from the same fresh object whose immutable binding was just
		// verified. This closes the gap between the reconcile read and Job build.
		jobTask = latest
	}

	// Ensure worker ServiceAccount and RBAC exist in the task namespace
	if err := r.ensureWorkerRBAC(ctx, task.Namespace); err != nil {
		log.Error(err, "failed to ensure worker RBAC")
		if r.Recorder != nil {
			r.Recorder.Eventf(task, corev1.EventTypeWarning, workerRBACReconcileFailedReason,
				"failed to ensure worker RBAC in namespace %q: %v", task.Namespace, err)
		}
		// Non-fatal: continue with job creation, it may still work
	}

	resolvedApprovalsJSON := ""
	if taskNeedsApprovalState(jobTask, agent) {
		resolvedApprovalsJSON, err = r.resolvedApprovalsJSONForTask(ctx, jobTask)
		if err != nil {
			log.Error(err, "failed to derive resolved approvals")
			return ctrl.Result{}, err
		}
	}

	// Create the Job
	job, err := r.JobBuilder.BuildWithOptions(ctx, jobTask, agent, provider, JobBuildOptions{
		ResolvedApprovalsJSON:       resolvedApprovalsJSON,
		RepositoryMonitorValidation: validationTask,
	})
	if err != nil {
		log.Error(err, "failed to build Job")
		return r.failTask(ctx, task, fmt.Sprintf("failed to build job: %v", err))
	}

	// Set owner reference
	if err := controllerutil.SetControllerReference(jobTask, job, r.Scheme); err != nil {
		log.Error(err, "failed to set owner reference")
		return ctrl.Result{}, err
	}

	// Create the Job
	if err := r.Create(ctx, job); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if validationTask {
				existing := &batchv1.Job{}
				if getErr := r.validationResourceReader().Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, existing); getErr != nil {
					return ctrl.Result{}, getErr
				}
				if validationErr := validateRepositoryMonitorValidationJobAgainstExpected(latest, existing, job); validationErr != nil {
					log.Error(validationErr, "refusing to adopt repository validation Job")
					return r.failTask(ctx, task, validationErr.Error())
				}
				job = existing
			}
			// Job already exists, update status.
			task.Status.JobName = job.Name
		} else {
			log.Error(err, "failed to create Job")
			return r.failTask(ctx, task, fmt.Sprintf("failed to create job: %v", err))
		}
	} else {
		task.Status.JobName = job.Name
	}

	// Update status to Running
	now := metav1.Now()
	task.Status.Phase = corev1alpha1.TaskPhaseRunning
	task.Status.StartTime = &now
	task.Status.Attempts++

	if s := trace.SpanFromContext(ctx); s.IsRecording() {
		s.AddEvent("phase.transition", trace.WithAttributes(
			attribute.String("task.phase", string(corev1alpha1.TaskPhaseRunning)),
		))
	}

	attempts := task.Status.Attempts
	jobName := task.Status.JobName
	transitionedToRunning := false
	if err := r.updateStatusWithRetry(ctx, task, func(t *corev1alpha1.Task) {
		transitionedToRunning = false
		if !canStartTaskJob(t.Status.Phase) {
			return
		}
		t.Status.Phase = corev1alpha1.TaskPhaseRunning
		t.Status.StartTime = &now
		t.Status.Attempts = attempts
		t.Status.JobName = jobName
		meta.SetStatusCondition(&t.Status.Conditions, metav1.Condition{
			Type:               ConditionTypeJobCreated,
			Status:             metav1.ConditionTrue,
			LastTransitionTime: now,
			Reason:             "JobCreated",
			Message:            fmt.Sprintf("Job %s created", job.Name),
		})
		transitionedToRunning = true
	}); err != nil {
		log.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}
	if transitionedToRunning {
		_ = r.recordTaskLifecycleEvent(
			ctx,
			task,
			execevents.ExecutionEventTypeTaskJobCreated,
			execevents.ExecutionEventSeverityInfo,
			fmt.Sprintf("Job %s created", jobName),
		)
		_ = r.recordTaskLifecycleEvent(
			ctx,
			task,
			execevents.ExecutionEventTypeTaskStarted,
			execevents.ExecutionEventSeverityInfo,
			fmt.Sprintf("Task started with Job %s", jobName),
		)
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// handleRunning handles Tasks in Running phase
func (r *TaskReconciler) handleRunning(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, error) { //nolint:gocyclo
	log := logf.FromContext(ctx)

	if taskManagedByACP(task) {
		if err := r.reconcileRunningACPClassWorkspaceAttachment(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		// The leader-elected ACPDispatcher owns the non-reconnectable prompt stream,
		// lease renewal, cancellation barrier, and terminal status projection.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if taskManagedByHarnessV1(task) {
		// HarnessV1Dispatcher owns submission, stream recovery, cancellation,
		// terminal settlement, and Task projection for binding-gated v1 work.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Check timeout
	if task.Spec.Timeout != nil && task.Status.StartTime != nil {
		elapsed := time.Since(task.Status.StartTime.Time)
		if elapsed > task.Spec.Timeout.Duration {
			if result, handled, err := r.handleAutonomousApprovalState(ctx, task); err != nil || handled {
				return result, err
			}
			log.Info("task timed out", "elapsed", elapsed, "timeout", task.Spec.Timeout.Duration)
			validationCommandStarted, err := r.repositoryMonitorValidationCommandStarted(ctx, task)
			if err != nil {
				log.Error(err, "failed to classify timed-out repository validation")
				if errors.Is(err, errRepositoryMonitorValidationConfinement) {
					return r.failTask(ctx, task, err.Error())
				}
				return ctrl.Result{}, err
			}
			if validationCommandStarted {
				return r.completeExecutedTask(ctx, task, corev1alpha1.TaskPhaseFailed, "task timed out")
			}
			return r.failTask(ctx, task, "task timed out")
		}
	}

	if task.Spec.Type == corev1alpha1.TaskTypeAgent && strings.TrimSpace(task.Status.JobName) == "" {
		return r.failTask(ctx, task, "agent task is not managed by the ACP runtime dispatcher")
	}

	// Populate ChildTaskStatus for coordinator tasks
	if _, isChild := task.Labels[labels.LabelParentTask]; !isChild {
		var children corev1alpha1.TaskList
		if err := r.List(ctx, &children, client.InNamespace(task.Namespace),
			client.MatchingLabels{labels.LabelParentTask: labels.SelectorValue(task.Name)}); err == nil {
			slices.SortFunc(children.Items, func(a, b corev1alpha1.Task) int {
				switch {
				case a.Name < b.Name:
					return -1
				case a.Name > b.Name:
					return 1
				default:
					return 0
				}
			})

			childStatuses := make([]corev1alpha1.ChildTaskStatus, 0, len(children.Items))
			for _, child := range children.Items {
				phase := child.Status.Phase
				if phase == "" {
					phase = corev1alpha1.TaskPhasePending
				}
				cs := corev1alpha1.ChildTaskStatus{
					Name:  child.Name,
					Phase: phase,
				}
				if child.Spec.AgentRef != nil {
					cs.Agent = child.Spec.AgentRef.Name
				}
				if child.Status.ResultRef != nil && child.Status.ResultRef.Available && r.ResultStore != nil {
					result, err := r.ResultStore.GetResult(ctx, child.Namespace, child.Name)
					if err != nil {
						log.Error(err, "failed to get child task result", "child", child.Name)
						cs.Result = "(result fetch error)"
					} else {
						cs.Result = string(result)
						if len(cs.Result) > 4096 {
							cs.Result = cs.Result[:4096] + "\n[truncated]"
						}
					}
				}
				childStatuses = append(childStatuses, cs)
			}
			if !childTaskStatusesEqual(task.Status.ChildTasks, childStatuses) {
				childStatusesCopy := append([]corev1alpha1.ChildTaskStatus(nil), childStatuses...)
				if err := r.updateStatusWithRetry(ctx, task, func(t *corev1alpha1.Task) {
					t.Status.ChildTasks = childStatusesCopy
				}); err != nil {
					log.Error(err, "failed to update child task status")
				}
			}
		}
	}

	// Get the Job
	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      task.Status.JobName,
		Namespace: task.Namespace,
	}, job); err != nil {
		if apierrors.IsNotFound(err) {
			if r.isAutonomousTask(ctx, task) {
				oldJob := task.Status.JobName
				latest := &corev1alpha1.Task{}
				reader := r.APIReader
				if reader == nil {
					reader = r.Client
				}
				if latestErr := reader.Get(ctx, types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, latest); latestErr != nil {
					return ctrl.Result{}, latestErr
				}
				if latest.Status.JobName != oldJob || latest.Status.Phase != corev1alpha1.TaskPhaseRunning {
					task.Status = latest.Status
					log.Info("job not found for stale autonomous task state; requeueing with latest status",
						"oldJob", oldJob,
						"latestJob", latest.Status.JobName,
						"latestPhase", latest.Status.Phase)
					return ctrl.Result{RequeueAfter: time.Second}, nil
				}
				task = latest
				if result, handled, err := r.handleAutonomousApprovalState(ctx, task); err != nil || handled {
					return result, err
				}
			}
			if r.isWithinJobCreationVisibilityGracePeriod(task) {
				log.Info("job not found shortly after creation, waiting for cache visibility",
					"job", task.Status.JobName,
					"startTime", task.Status.StartTime,
				)
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			if r.shouldRetry(task) {
				log.Info("job not found while task still has retry budget, scheduling retry", "attempt", task.Status.Attempts)
				return r.retryTask(ctx, task)
			}
			log.Info("Job not found, task may have been cleaned up")
			return r.failTask(ctx, task, "job not found")
		}
		log.Error(err, "failed to get Job")
		return ctrl.Result{}, err
	}
	if err := r.reconcileRepositoryMonitorValidationConfinement(ctx, task, job); err != nil {
		log.Error(err, "repository validation confinement failed")
		if errors.Is(err, errRepositoryMonitorValidationConfinement) {
			return r.failTask(ctx, task, err.Error())
		}
		return ctrl.Result{}, err
	}

	// Check Job status
	if job.Status.Succeeded > 0 {
		// Check if this is an autonomous task that should continue iterating
		if r.isAutonomousTask(ctx, task) {
			return r.handleAutonomousIteration(ctx, task)
		}
		// Job succeeded
		return r.completeExecutedTask(ctx, task, corev1alpha1.TaskPhaseSucceeded, "task completed successfully")
	}

	if job.Status.Failed > 0 {
		if result, handled, err := r.handleAutonomousApprovalState(ctx, task); err != nil || handled {
			return result, err
		}
		validationCommandFailed := true
		validationTask, validationErr := r.repositoryMonitorValidationTask(ctx, task)
		if validationErr != nil {
			return ctrl.Result{}, validationErr
		}
		if validationTask {
			var err error
			validationCommandFailed, err = r.repositoryMonitorValidationCommandFailed(ctx, task, job)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		if task.Spec.Timeout != nil && jobFailedDueToActiveDeadline(job) {
			if !validationCommandFailed {
				return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, "task timed out")
			}
			return r.completeExecutedTask(ctx, task, corev1alpha1.TaskPhaseFailed, "task timed out")
		}
		// Job failed, check retry policy
		if r.shouldRetry(task) {
			log.Info("retrying task", "attempt", task.Status.Attempts)
			return r.retryTask(ctx, task)
		}
		// Inspect terminated containers for a specific cause (OOMKilled, non-zero
		// exit code, etc.) so the coordinator that delegated this Task can read
		// fetch_task_output and adapt — e.g. recreate the Agent with more memory.
		// Falls back to the generic "job failed" if no signal is available.
		msg := r.diagnoseFailedJob(ctx, task)
		if !validationCommandFailed {
			return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, msg)
		}
		return r.completeExecutedTask(ctx, task, corev1alpha1.TaskPhaseFailed, msg)
	}

	// Check for pods stuck in Pending/ContainerCreating with unrecoverable errors
	// (e.g., missing secrets, missing configmaps, image pull errors)
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(task.Namespace),
		client.MatchingLabels{labels.LabelTask: labels.SelectorValue(task.Name)}); err == nil {
		for i := range podList.Items {
			pod := &podList.Items[i]
			if pod.Status.Phase != corev1.PodPending {
				continue
			}
			// Check waiting container statuses for unrecoverable errors
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Waiting != nil {
					reason := cs.State.Waiting.Reason
					if reason == "CreateContainerConfigError" || reason == "ErrImageNeverPull" {
						msg := fmt.Sprintf("pod stuck: %s - %s", reason, cs.State.Waiting.Message)
						log.Info("failing task due to unrecoverable pod error", "reason", reason)
						return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, msg)
					}
				}
			}
			// Check pod conditions for unschedulable
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse && cond.Reason == "Unschedulable" {
					msg := fmt.Sprintf("pod unschedulable: %s", cond.Message)
					log.Info("failing task due to unschedulable pod", "message", cond.Message)
					return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, msg)
				}
			}
			// Check events for volume mount failures (pod stays in ContainerCreating)
			if task.Status.StartTime != nil && time.Since(task.Status.StartTime.Time) > 2*time.Minute {
				for _, cs := range pod.Status.ContainerStatuses {
					if cs.State.Waiting != nil && cs.State.Waiting.Reason == "ContainerCreating" {
						msg := "pod stuck in ContainerCreating for over 2 minutes (possible missing secret/volume)"
						log.Info("failing task due to extended ContainerCreating state")
						return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, msg)
					}
				}
				if msg, ok, err := r.failedMountEventMessage(ctx, pod, task.Status.StartTime.Time); err != nil {
					return ctrl.Result{}, err
				} else if ok {
					msg = fmt.Sprintf("pod stuck initializing for over 2 minutes: %s", msg)
					log.Info("failing task due to failed pod mount", "message", msg)
					return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, msg)
				}
			}
		}
	}

	// Job still running, requeue
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func taskManagedByHarnessV1(task *corev1alpha1.Task) bool {
	return task != nil && task.Spec.Type == corev1alpha1.TaskTypeAgent &&
		task.Status.AgentExecutionBinding != nil &&
		task.Status.AgentExecutionBinding.ContractVersion == corev1alpha1.AgentRuntimeContractHarnessV1
}

func podWaitingForMountInitialization(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, cs := range statuses {
			if cs.State.Waiting == nil {
				continue
			}
			switch cs.State.Waiting.Reason {
			case "ContainerCreating", "PodInitializing":
				return true
			}
		}
	}
	return false
}

func eventObservedAt(event *corev1.Event) time.Time {
	if event == nil {
		return time.Time{}
	}
	if event.Series != nil && !event.Series.LastObservedTime.IsZero() {
		return event.Series.LastObservedTime.Time
	}
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}
	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.Time
	}
	if !event.FirstTimestamp.IsZero() {
		return event.FirstTimestamp.Time
	}
	if !event.CreationTimestamp.IsZero() {
		return event.CreationTimestamp.Time
	}
	return time.Time{}
}

func eventInvolvedObjectNameIndex(obj client.Object) []string {
	event, ok := obj.(*corev1.Event)
	if !ok || event.InvolvedObject.Name == "" {
		return nil
	}
	return []string{event.InvolvedObject.Name}
}

func eventReasonIndex(obj client.Object) []string {
	event, ok := obj.(*corev1.Event)
	if !ok || event.Reason == "" {
		return nil
	}
	return []string{event.Reason}
}

func (r *TaskReconciler) failedMountEventMessage(ctx context.Context, pod *corev1.Pod, since time.Time) (string, bool, error) {
	if pod == nil || !podWaitingForMountInitialization(pod) {
		return "", false, nil
	}

	var events corev1.EventList
	if err := r.List(ctx, &events,
		client.InNamespace(pod.Namespace),
		client.MatchingFields{
			eventInvolvedObjectNameField: pod.Name,
			eventReasonField:             "FailedMount",
		},
	); err != nil {
		return "", false, err
	}

	now := time.Now()
	for i := range events.Items {
		event := &events.Items[i]
		if event.Reason != "FailedMount" {
			continue
		}
		ref := event.InvolvedObject
		if ref.Kind != "Pod" || ref.Name != pod.Name {
			continue
		}
		if ref.UID != "" && pod.UID != "" && ref.UID != pod.UID {
			continue
		}
		observedAt := eventObservedAt(event)
		if observedAt.IsZero() || (!since.IsZero() && observedAt.Before(since)) {
			continue
		}
		if now.Sub(observedAt) > failedMountEventStaleAfter {
			continue
		}
		message := strings.TrimSpace(event.Message)
		if message == "" {
			message = "pod volume mount failed"
		}
		return message, true, nil
	}
	return "", false, nil
}

func jobFailedDueToActiveDeadline(job *batchv1.Job) bool {
	if job == nil {
		return false
	}

	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		if condition.Reason != batchv1.JobReasonDeadlineExceeded {
			continue
		}
		if condition.Type == batchv1.JobFailed || condition.Type == batchv1.JobFailureTarget {
			return true
		}
	}

	return false
}

// diagnoseFailedJob inspects pods belonging to a failed Task's Job and returns a
// Status.Message that is specific enough for a coordinator LLM to act on.
//
// Priority of signals:
//  1. Any container terminated with reason=OOMKilled → "job failed: container
//     OOMKilled (memory limit <X> exceeded). Recreate the agent with higher
//     resources.limits.memory or set spec.resources on the task."
//  2. Any container terminated with a non-zero exit code → "job failed:
//     container exited with code <N> (reason=<R>)".
//  3. No signal available → the generic "job failed".
//
// Pod listing failures are non-fatal — we fall back to the generic message
// rather than block task completion.
func (r *TaskReconciler) diagnoseFailedJob(ctx context.Context, task *corev1alpha1.Task) string {
	log := logf.FromContext(ctx)
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(task.Namespace),
		client.MatchingLabels{labels.LabelTask: labels.SelectorValue(task.Name)}); err != nil {
		log.V(1).Info("diagnoseFailedJob: pod list failed, using generic message", "error", err.Error())
		return "job failed"
	}

	var (
		oomMsg  string
		exitMsg string
	)
	for i := range podList.Items {
		pod := &podList.Items[i]
		if task.Status.JobName != "" && !podBelongsToJob(pod, task.Status.JobName) {
			continue
		}
		// Worker pods only have one container; iterate defensively anyway.
		for _, cs := range pod.Status.ContainerStatuses {
			term := cs.State.Terminated
			if term == nil {
				// Also check LastTerminationState — pods that crashed and restarted
				// expose the relevant terminated state there.
				term = cs.LastTerminationState.Terminated
			}
			if term == nil {
				continue
			}
			if term.Reason == "OOMKilled" || term.ExitCode == 137 {
				limit := podContainerMemoryLimit(pod, cs.Name)
				if limit == "" {
					limit = "unknown"
				}
				oomMsg = fmt.Sprintf("job failed: container OOMKilled (memory limit %s exceeded). Recreate the agent with higher resources.limits.memory or set spec.resources on the task.", limit)
				continue
			}
			if term.ExitCode != 0 && exitMsg == "" {
				reason := term.Reason
				if reason == "" {
					reason = "Error"
				}
				exitMsg = fmt.Sprintf("job failed: container exited with code %d (reason=%s)", term.ExitCode, reason)
			}
		}
	}

	if oomMsg != "" {
		return oomMsg
	}
	if exitMsg != "" {
		return exitMsg
	}
	return "job failed"
}

func podBelongsToJob(pod *corev1.Pod, jobName string) bool {
	if pod == nil || strings.TrimSpace(jobName) == "" {
		return true
	}
	hasJobIdentity := false
	if got := pod.Labels[batchv1.JobNameLabel]; got != "" {
		hasJobIdentity = true
		if got == jobName {
			return true
		}
	}
	if got := pod.Labels["job-name"]; got != "" {
		hasJobIdentity = true
		if got == jobName {
			return true
		}
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "Job" {
			hasJobIdentity = true
			if owner.Name == jobName {
				return true
			}
		}
	}
	return !hasJobIdentity
}

// podContainerMemoryLimit returns the memory limit configured on the named
// container as a string ("2Gi"), or "" if not set.
func podContainerMemoryLimit(pod *corev1.Pod, containerName string) string {
	if pod == nil {
		return ""
	}
	for _, c := range pod.Spec.Containers {
		if c.Name != containerName {
			continue
		}
		if q, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
			return q.String()
		}
	}
	return ""
}

func (r *TaskReconciler) isWithinJobCreationVisibilityGracePeriod(task *corev1alpha1.Task) bool {
	if task == nil || task.Status.JobName == "" || task.Status.StartTime == nil {
		return false
	}
	return time.Since(task.Status.StartTime.Time) < jobCreationVisibilityGracePeriod
}

// handleCompleted handles Tasks that have completed (Succeeded or Failed)
func (r *TaskReconciler) handleFinalizing(
	ctx context.Context, task *corev1alpha1.Task,
) (ctrl.Result, error) {
	outcome := task.Status.ExecutionOutcome
	if outcome == nil {
		return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, "finalizing task is missing execution outcome")
	}
	genericFinalizationTimedOut := !outcome.RecordedAt.IsZero() &&
		time.Since(outcome.RecordedAt.Time) >= workspaceFinalizationTimeout
	if taskExecutionWorkspaceNeedsFinalization(task) {
		workspaceStatus := task.Status.ExecutionWorkspace
		if workspaceStatus == nil || workspaceStatus.WorkspaceRef == nil || workspaceStatus.AttachedEpoch <= 0 {
			if genericFinalizationTimedOut {
				if err := r.quarantineFinalizingWorkspace(ctx, task); err != nil {
					return ctrl.Result{}, err
				}
				return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, "workspace authority revocation timed out; workspace quarantined")
			}
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		workspaceObject := &workspacev1alpha1.ExecutionWorkspace{}
		key := types.NamespacedName{Namespace: task.Namespace, Name: workspaceStatus.WorkspaceRef.Name}
		if err := r.Get(ctx, key, workspaceObject); err != nil {
			if apierrors.IsNotFound(err) {
				return r.completeTask(ctx, task, outcome.Phase, outcome.Message)
			}
			return ctrl.Result{}, err
		}
		if workspaceStatus.WorkspaceRef.UID != "" && string(workspaceObject.UID) != workspaceStatus.WorkspaceRef.UID {
			return ctrl.Result{}, fmt.Errorf("execution workspace UID changed during finalization")
		}
		revocationEpoch := workspaceStatus.AttachedEpoch
		acpWorkspace := workspaceObject.Labels[workspacev1alpha1.ProviderControllerLabel] == acpWorkspaceControllerLabelValue
		if acpWorkspace {
			revocationEpoch = acpWorkspaceRevocationEpochForTask(task, workspaceObject, revocationEpoch)
			// Attach and the Task epoch annotation are separate API writes.
			// Persist the enforced epoch and pending-detach barrier BEFORE
			// generic revocation clears the attachment: the Finalizing gate
			// defers class settlement, so without both stamps a continuation
			// could attach in this window and the later terminal settle could
			// reapply or skip this Task's frozen Suspend/Delete action.
			if err := r.markACPTaskAttachmentEpoch(ctx, task, revocationEpoch); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.markACPWorkspaceRevocationStarted(ctx, workspaceObject, revocationEpoch); err != nil {
				return ctrl.Result{}, err
			}
		}
		attachmentManager := WorkspaceAttachmentManager{Client: r.Client, APIReader: r.APIReader}
		if err := attachmentManager.BeginRevocation(ctx, workspaceObject, revocationEpoch); err != nil {
			return ctrl.Result{}, err
		}
		if acpWorkspace {
			result, expired, err := r.failFinalizingTaskPastACPDetachTimeout(ctx, task, workspaceObject)
			if err != nil || expired {
				return result, err
			}
		} else if genericFinalizationTimedOut {
			if err := r.quarantineFinalizingWorkspace(ctx, task); err != nil {
				return ctrl.Result{}, err
			}
			return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, "workspace authority revocation timed out; workspace quarantined")
		}
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	workspaceStatus := task.Status.ExecutionWorkspace
	if workspaceStatus != nil && workspaceStatus.WorkspaceRef != nil && workspaceStatus.AttachedEpoch > 0 {
		workspaceObject := &workspacev1alpha1.ExecutionWorkspace{}
		key := types.NamespacedName{Namespace: task.Namespace, Name: workspaceStatus.WorkspaceRef.Name}
		if err := r.Get(ctx, key, workspaceObject); err == nil {
			if workspaceStatus.WorkspaceRef.UID != "" && string(workspaceObject.UID) != workspaceStatus.WorkspaceRef.UID {
				return ctrl.Result{}, fmt.Errorf("execution workspace UID changed during finalization")
			}
			revocationEpoch := workspaceStatus.AttachedEpoch
			acpWorkspace := workspaceObject.Labels[workspacev1alpha1.ProviderControllerLabel] == acpWorkspaceControllerLabelValue
			if acpWorkspace {
				revocationEpoch = acpWorkspaceRevocationEpochForTask(task, workspaceObject, revocationEpoch)
				if err := r.markACPWorkspaceRevocationStarted(ctx, workspaceObject, revocationEpoch); err != nil {
					return ctrl.Result{}, err
				}
			}
			attachmentManager := WorkspaceAttachmentManager{Client: r.Client, APIReader: r.APIReader}
			if err := attachmentManager.FinalizeRevocation(ctx, workspaceObject, revocationEpoch, attachmentSecretName(workspaceObject.Name, revocationEpoch)); err != nil {
				if acpWorkspace {
					result, expired, timeoutErr := r.failFinalizingTaskPastACPDetachTimeout(ctx, task, workspaceObject)
					if timeoutErr != nil || expired {
						return result, timeoutErr
					}
				} else if genericFinalizationTimedOut {
					if quarantineErr := r.quarantineFinalizingWorkspace(ctx, task); quarantineErr != nil {
						return ctrl.Result{}, quarantineErr
					}
					return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, "workspace authority revocation timed out; workspace quarantined")
				}
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	return r.completeTask(ctx, task, outcome.Phase, outcome.Message)
}

func acpWorkspaceRevocationEpochForTask(
	task *corev1alpha1.Task,
	workspaceObject *workspacev1alpha1.ExecutionWorkspace,
	projectedEpoch int64,
) int64 {
	if task == nil {
		return projectedEpoch
	}
	if workspaceObject != nil && task.UID != "" && workspaceObject.Spec.Attachment != nil &&
		workspaceObject.Spec.Attachment.TaskRef.UID == task.UID && workspaceObject.Spec.Attachment.Epoch > 0 {
		return workspaceObject.Spec.Attachment.Epoch
	}
	if recordedEpoch := acpTaskRecordedAttachmentEpoch(task); recordedEpoch > projectedEpoch {
		return recordedEpoch
	}
	return projectedEpoch
}

func (r *TaskReconciler) failFinalizingTaskPastACPDetachTimeout(
	ctx context.Context,
	task *corev1alpha1.Task,
	workspaceObject *workspacev1alpha1.ExecutionWorkspace,
) (ctrl.Result, bool, error) {
	expired, err := r.quarantineACPWorkspacePastDetachTimeout(ctx, workspaceObject)
	if err != nil || !expired {
		return ctrl.Result{}, false, err
	}
	result, err := r.completeTask(
		ctx,
		task,
		corev1alpha1.TaskPhaseFailed,
		"workspace authority revocation exceeded the class detach timeout; workspace quarantined",
	)
	return result, true, err
}

func (r *TaskReconciler) quarantineFinalizingWorkspace(ctx context.Context, task *corev1alpha1.Task) error {
	if task == nil {
		return nil
	}
	status := task.Status.ExecutionWorkspace
	workspaces := []workspacev1alpha1.ExecutionWorkspace{}
	if status != nil && status.WorkspaceRef != nil && strings.TrimSpace(status.WorkspaceRef.Name) != "" {
		workspaceObject := workspacev1alpha1.ExecutionWorkspace{}
		err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: status.WorkspaceRef.Name}, &workspaceObject)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err == nil {
			workspaces = append(workspaces, workspaceObject)
		}
	} else {
		workspaceList := workspacev1alpha1.ExecutionWorkspaceList{}
		if err := r.List(ctx, &workspaceList, client.InNamespace(task.Namespace)); err != nil {
			return err
		}
		for index := range workspaceList.Items {
			owner := metav1.GetControllerOf(&workspaceList.Items[index])
			if owner != nil && owner.Kind == "Task" && owner.UID == task.UID {
				workspaces = append(workspaces, workspaceList.Items[index])
			}
		}
	}
	for index := range workspaces {
		workspaceObject := &workspaces[index]
		if status != nil && status.WorkspaceRef != nil && status.WorkspaceRef.UID != "" && string(workspaceObject.UID) != status.WorkspaceRef.UID {
			return fmt.Errorf("execution workspace UID changed during quarantine")
		}
		epoch := workspaceObject.Spec.AttachmentEpoch
		if status != nil {
			epoch = max(epoch, status.AttachedEpoch)
		}
		before := workspaceObject.DeepCopy()
		workspaceObject.Spec.AttachmentEpoch = epoch
		workspaceObject.Spec.Attachment = nil
		workspaceObject.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined
		if err := r.Patch(ctx, workspaceObject, client.MergeFrom(before)); err != nil {
			return err
		}
		if epoch > 0 {
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: attachmentSecretName(workspaceObject.Name, epoch), Namespace: workspaceObject.Namespace}}
			if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: attachmentLeaseName(workspaceObject.Name), Namespace: workspaceObject.Namespace}}
		if err := r.Delete(ctx, lease); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *TaskReconciler) handleCompleted(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	terminalEventRecorded := r.recordTerminalTaskLifecycleEventIfMissing(ctx, task)
	waitingForJob, err := r.cleanupTerminalTaskJob(ctx, task)
	if err != nil {
		log.Error(err, "failed to clean up terminal task Job")
		return ctrl.Result{}, err
	}
	if waitingForJob {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	if !terminalEventRecorded {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	if r.ACPArtifactRetirer != nil && task.Spec.Type == corev1alpha1.TaskTypeAgent && task.Status.Execution != nil && r.DurableControlStore != nil {
		ready, readyErr := r.acpTaskDeletionReady(ctx, task)
		if readyErr != nil {
			log.Error(readyErr, "failed to verify ACP artifact retirement readiness")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if !ready {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		retired, retireErr := r.retireACPArtifactIdentities(ctx, task)
		if retireErr != nil {
			log.Error(retireErr, "failed to retire ACP artifact identities")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if !retired {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
	}

	// Detach can suspend or delete the RuntimePool that terminal recovery still
	// needs. Apply it only after Job cleanup plus durable ACP artifact
	// retirement have settled.
	settled, settleErr := r.reconcileACPClassWorkspaceSettlement(ctx, task)
	if settleErr != nil {
		log.Error(settleErr, "failed to settle terminal ACP workspace")
		return ctrl.Result{}, settleErr
	}
	if !settled {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// Send optional webhooks only after durable ACP artifact identities have
	// retired. Failed delivery may retry indefinitely without delaying artifact
	// reclamation; retirement itself retains the capability safety grace period.
	if task.Spec.WebhookURL != "" && !task.Status.WebhookDelivered {
		if err := r.WebhookNotifier.Notify(ctx, task); err != nil {
			log.Error(err, "failed to send webhook")
			// Don't fail the task, just retry webhook later
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		task.Status.WebhookDelivered = true
		if err := r.Status().Update(ctx, task); err != nil {
			log.Error(err, "failed to update webhook status")
			return ctrl.Result{}, err
		}
	}

	if err := r.enforceParentScheduledTaskHistory(ctx, task); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TaskReconciler) cleanupDeletedTaskJob(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	if task.Status.JobName == "" {
		return false, nil
	}

	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: task.Status.JobName, Namespace: task.Namespace}, job); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting deleted task Job %q: %w", task.Status.JobName, err)
	}

	validationTask := r.repositoryMonitorValidationSafetyTask(ctx, task)
	propagationPolicy := metav1.DeletePropagationBackground
	if validationTask {
		propagationPolicy = metav1.DeletePropagationForeground
	}
	if err := r.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &propagationPolicy}); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("deleting deleted task Job %q: %w", task.Status.JobName, err)
	}
	return validationTask, nil
}

func (r *TaskReconciler) cleanupTerminalTaskJob(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	if task.Status.JobName == "" {
		return false, nil
	}

	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: task.Status.JobName, Namespace: task.Namespace}, job); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting terminal task Job %q: %w", task.Status.JobName, err)
	}

	deleteJob := task.Status.Phase == corev1alpha1.TaskPhaseCancelled ||
		(task.Status.Phase == corev1alpha1.TaskPhaseFailed && job.Status.Active > 0)
	if !deleteJob {
		return false, nil
	}

	validationTask := r.repositoryMonitorValidationSafetyTask(ctx, task)
	propagationPolicy := metav1.DeletePropagationBackground
	if validationTask {
		propagationPolicy = metav1.DeletePropagationForeground
	}
	if err := r.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &propagationPolicy}); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("deleting terminal task Job %q: %w", task.Status.JobName, err)
	}

	return validationTask, nil
}

func (r *TaskReconciler) enforceParentScheduledTaskHistory(ctx context.Context, task *corev1alpha1.Task) error {
	if task.Labels[labels.LabelScheduledRun] != scheduledRunLabelValue {
		return nil
	}

	parentName := labels.ParentTaskName(task.Labels, task.Annotations)
	if parentName == "" {
		return nil
	}

	parent := &corev1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKey{Name: parentName, Namespace: task.Namespace}, parent); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("getting parent scheduled task %q: %w", parentName, err)
	}

	if err := r.enforceHistoryLimits(ctx, parent); err != nil {
		return fmt.Errorf("enforcing history limits for parent task %q: %w", parentName, err)
	}

	return nil
}

// completeTask marks a task as completed without asserting that workload execution started.
func (r *TaskReconciler) completeTask(
	ctx context.Context,
	task *corev1alpha1.Task,
	phase corev1alpha1.TaskPhase,
	message string,
) (ctrl.Result, error) {
	return r.completeTaskWithOutcome(ctx, task, phase, phase, message, false)
}

// completeExecutedTask marks a task terminal and records the immutable workload outcome.
func (r *TaskReconciler) completeExecutedTask(
	ctx context.Context,
	task *corev1alpha1.Task,
	phase corev1alpha1.TaskPhase,
	message string,
) (ctrl.Result, error) {
	if taskExecutionWorkspaceNeedsFinalization(task) {
		return r.beginTaskFinalization(ctx, task, phase, message)
	}
	return r.completeTaskWithOutcome(ctx, task, phase, phase, message, true)
}

func taskExecutionWorkspaceNeedsFinalization(task *corev1alpha1.Task) bool {
	if task == nil || task.Spec.Execution == nil || task.Spec.Execution.Workspace == nil {
		return false
	}
	request := task.Spec.Execution.Workspace
	if !request.Enabled && request.ClassRef == nil {
		return false
	}
	status := task.Status.ExecutionWorkspace
	if status == nil {
		return true
	}
	if status.AttachedEpoch <= 0 {
		return false
	}
	condition := meta.FindStatusCondition(status.Conditions, "Attached")
	return condition == nil || condition.Status != metav1.ConditionFalse
}

func (r *TaskReconciler) beginTaskFinalization(
	ctx context.Context,
	task *corev1alpha1.Task,
	outcomePhase corev1alpha1.TaskPhase,
	message string,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if err := r.collectResult(ctx, task); err != nil {
		log.Error(err, "failed to collect result before workspace finalization")
		if errors.Is(err, errRepositoryMonitorValidationClassificationUnavailable) {
			return ctrl.Result{}, err
		}
	}
	now := metav1.Now()
	resultRef := task.Status.ResultRef
	if err := r.updateStatusWithRetry(ctx, task, func(current *corev1alpha1.Task) {
		current.Status.Phase = corev1alpha1.TaskPhaseFinalizing
		current.Status.CompletionTime = nil
		current.Status.Message = message
		current.Status.ResultRef = resultRef
		if current.Status.ExecutionOutcome == nil {
			attempt := max(current.Status.Attempts, 1)
			current.Status.ExecutionOutcome = &corev1alpha1.TaskWorkloadExecutionOutcome{
				Phase: outcomePhase, Attempt: attempt, ResultRef: resultRef, RecordedAt: now, Message: message,
			}
		}
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type: ConditionTypeWaitingForApproval, Status: metav1.ConditionFalse, LastTransitionTime: now,
			Reason: "TaskFinalizing", Message: "task execution is settled",
		})
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type: ConditionTypeComplete, Status: metav1.ConditionFalse, LastTransitionTime: now,
			Reason: "TaskFinalizing", Message: "workspace authority is being revoked",
		})
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

func (r *TaskReconciler) completeTaskWithOutcome(
	ctx context.Context,
	task *corev1alpha1.Task,
	phase corev1alpha1.TaskPhase,
	outcomePhase corev1alpha1.TaskPhase,
	message string,
	recordOutcome bool,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	now := metav1.Now()
	task.Status.Phase = phase
	task.Status.CompletionTime = &now
	task.Status.Message = message

	if s := trace.SpanFromContext(ctx); s.IsRecording() {
		s.AddEvent("phase.transition", trace.WithAttributes(
			attribute.String("task.phase", string(phase)),
		))
	}

	// Collect result from Job output
	if err := r.collectResult(ctx, task); err != nil {
		log.Error(err, "failed to collect result")
		if errors.Is(err, errRepositoryMonitorValidationClassificationUnavailable) {
			return ctrl.Result{}, err
		}
		// Continue anyway, result collection is best-effort
	}

	// Update session if configured
	if task.Spec.SessionRef != nil && task.Spec.SessionRef.Append {
		if err := r.SessionManager.AppendMessages(ctx, task, r.ResultStore); err != nil {
			log.Error(err, "failed to append session messages")
			// Continue anyway
		}
	}
	// Release session lock regardless of Append setting
	if task.Spec.SessionRef != nil {
		if err := r.SessionManager.ReleaseLock(ctx, task); err != nil {
			log.Error(err, "failed to release session lock")
		}
	}

	// Clean up plan state on completion (best-effort)
	if r.PlanStore != nil {
		if err := r.PlanStore.DeletePlan(ctx, task.Namespace, task.Name); err != nil {
			log.Error(err, "failed to delete plan state on completion")
		}
	}

	conditionStatus := metav1.ConditionTrue
	reason := "TaskSucceeded"
	switch phase {
	case corev1alpha1.TaskPhaseFailed:
		conditionStatus = metav1.ConditionFalse
		reason = "TaskFailed"
	case corev1alpha1.TaskPhaseCancelled:
		conditionStatus = metav1.ConditionFalse
		reason = "TaskCancelled"
	}

	resultRef := task.Status.ResultRef
	if err := r.updateStatusWithRetry(ctx, task, func(t *corev1alpha1.Task) {
		t.Status.Phase = phase
		t.Status.CompletionTime = &now
		t.Status.Message = message
		t.Status.ResultRef = resultRef
		if recordOutcome && t.Status.ExecutionOutcome == nil {
			attempt := max(t.Status.Attempts, 1)
			t.Status.ExecutionOutcome = &corev1alpha1.TaskWorkloadExecutionOutcome{
				Phase:      outcomePhase,
				Attempt:    attempt,
				ResultRef:  resultRef,
				RecordedAt: now,
				Message:    message,
			}
		}
		meta.SetStatusCondition(&t.Status.Conditions, metav1.Condition{
			Type:               ConditionTypeWaitingForApproval,
			Status:             metav1.ConditionFalse,
			LastTransitionTime: now,
			Reason:             reason,
			Message:            "task is terminal",
		})
		meta.SetStatusCondition(&t.Status.Conditions, metav1.Condition{
			Type:               ConditionTypeComplete,
			Status:             conditionStatus,
			LastTransitionTime: now,
			Reason:             reason,
			Message:            message,
		})
	}); err != nil {
		log.Error(err, "failed to update completion status")
		return ctrl.Result{}, err
	}
	terminalEventErr := r.recordTaskLifecycleEvent(
		ctx,
		task,
		executionEventTypeForTaskPhase(phase),
		executionEventSeverityForTaskPhase(phase),
		message,
	)

	// Update the Agent's LastUsed timestamp so TTL tracking works
	if task.Spec.AgentRef != nil {
		if err := r.updateAgentLastUsed(ctx, task.Namespace, task.Spec.AgentRef.Name, now); err != nil && !apierrors.IsNotFound(err) {
			log.Error(err, "failed to update agent LastUsed")
		}
	}
	if terminalEventErr != nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *TaskReconciler) updateAgentLastUsed(ctx context.Context, namespace, name string, at metav1.Time) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		agent := &corev1alpha1.Agent{}
		if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, agent); err != nil {
			return err
		}
		agent.Status.LastUsed = &at
		return r.Status().Update(ctx, agent)
	})
}

// failTask marks a task as failed
func (r *TaskReconciler) failTask(ctx context.Context, task *corev1alpha1.Task, message string) (ctrl.Result, error) {
	return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, message)
}

// executionOutcomePreventsReplay treats every recorded outcome as final.
// Retryable failed attempts are routed through retryTask before
// completeExecutedTask records an outcome; once recorded, no attempt is replayed.
func executionOutcomePreventsReplay(outcome *corev1alpha1.TaskWorkloadExecutionOutcome) bool {
	return outcome != nil
}

// shouldRetry checks if the task should be retried
func (r *TaskReconciler) shouldRetry(task *corev1alpha1.Task) bool {
	if task == nil || executionOutcomePreventsReplay(task.Status.ExecutionOutcome) {
		return false
	}
	if task.Spec.RetryPolicy == nil {
		return false
	}
	// Attempts counts the initial run plus completed retries, while MaxRetries
	// is configured as the number of additional retry attempts. Retry while the
	// current execution count is still within that additional retry budget.
	return task.Status.Attempts <= task.Spec.RetryPolicy.MaxRetries
}

// retryTask creates a new Job for a retry attempt
func (r *TaskReconciler) retryTask(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Calculate backoff delay
	delay := r.calculateRetryDelay(task)
	oldJobName := task.Status.JobName

	// Reset to pending for retry before deleting the old Job so a transient
	// NotFound from asynchronous Job deletion does not fail the task.
	if err := r.updateStatusWithRetry(ctx, task, func(t *corev1alpha1.Task) {
		t.Status.Phase = corev1alpha1.TaskPhasePending
		t.Status.JobName = ""
		t.Status.Message = ""
		t.Status.CompletionTime = nil
		t.Status.ResultRef = nil
	}); err != nil {
		log.Error(err, "failed to update status for retry")
		return ctrl.Result{}, err
	}

	// Delete the old Job after clearing the running status.
	if oldJobName != "" {
		job := &batchv1.Job{}
		err := r.Get(ctx, types.NamespacedName{
			Name:      oldJobName,
			Namespace: task.Namespace,
		}, job)
		if err == nil {
			propagationPolicy := metav1.DeletePropagationBackground
			if err := r.Delete(ctx, job, &client.DeleteOptions{
				PropagationPolicy: &propagationPolicy,
			}); err != nil && !apierrors.IsNotFound(err) {
				log.Error(err, "failed to delete old Job for retry")
			}
		}
	}

	return ctrl.Result{RequeueAfter: delay}, nil
}

// calculateRetryDelay calculates the delay before retry using exponential backoff
func (r *TaskReconciler) calculateRetryDelay(task *corev1alpha1.Task) time.Duration {
	if task.Spec.RetryPolicy == nil || task.Spec.RetryPolicy.InitialDelay == nil {
		return 10 * time.Second // Default delay
	}

	initialDelay := task.Spec.RetryPolicy.InitialDelay.Duration
	multiplier := task.Spec.RetryPolicy.BackoffMultiplier
	if multiplier == 0 {
		multiplier = 2
	}

	// Calculate delay with exponential backoff
	maxDelay := 5 * time.Minute
	delay := initialDelay
	for i := int32(1); i < task.Status.Attempts; i++ {
		delay = time.Duration(float64(delay) * multiplier)
		// Guard against overflow (negative) and cap early
		if delay <= 0 || delay > maxDelay {
			delay = maxDelay
			break
		}
	}

	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

// collectResult collects the task result from the Job's output
func (r *TaskReconciler) collectResult(ctx context.Context, task *corev1alpha1.Task) error {
	if r.ResultStore == nil {
		return nil
	}
	validationTask, err := r.repositoryMonitorValidationResultTask(ctx, task)
	if err != nil {
		return err
	}
	if validationTask {
		// Validation commands may process repository fixtures or source that
		// contain credentials. Their output is deliberately unavailable; the
		// durable review record keeps only the command digest and safe status.
		return nil
	}

	// Check if result already exists in store (written by worker via HTTP)
	_, err = r.ResultStore.GetResult(ctx, task.Namespace, task.Name)
	if err == nil {
		// Result already exists (written by worker)
		task.Status.ResultRef = &corev1alpha1.ResultReference{
			Available: true,
		}
		return nil
	}

	if !errors.Is(err, store.ErrNotFound) {
		return err
	}

	// No result yet — capture pod logs for tasks that actually created a Job.
	// Some validation failures happen before Job creation; those terminal tasks should
	// not produce noisy best-effort log collection errors for a non-existent Job.
	stdoutResult := taskUsesStdoutResult(task)
	if (task.Spec.Type != corev1alpha1.TaskTypeContainer && !stdoutResult) || r.KubeClient == nil || task.Status.JobName == "" {
		return nil
	}

	var result []byte
	if stdoutResult {
		logs, err := r.readStdoutResultPodLogs(ctx, task)
		if err != nil {
			return fmt.Errorf("reading stdout result pod logs: %w", err)
		}
		stdoutPayload, ok, decodeErr := extractStdoutTaskResult(logs)
		if decodeErr != nil {
			return decodeErr
		}
		if !ok {
			return nil
		}
		result = stdoutPayload
	} else {
		logs, err := r.readPodLogs(ctx, task)
		if err != nil {
			return fmt.Errorf("reading pod logs: %w", err)
		}
		result = []byte(logs)
	}

	if err := r.ResultStore.SaveResult(ctx, task.Namespace, task.Name, result); err != nil {
		return fmt.Errorf("saving result: %w", err)
	}

	task.Status.ResultRef = &corev1alpha1.ResultReference{
		Available: true,
	}

	return nil
}

func taskUsesStdoutResult(task *corev1alpha1.Task) bool {
	return taskRequestsReadOnlyAgent(task)
}

func extractStdoutTaskResult(logs string) ([]byte, bool, error) {
	var payload string
	for line := range strings.SplitSeq(logs, "\n") {
		line = strings.TrimSpace(line)
		if encoded, ok := strings.CutPrefix(line, workerenv.ResultStdoutPrefix); ok {
			payload = strings.TrimSpace(encoded)
		}
	}
	if payload == "" {
		return nil, false, nil
	}
	result, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, true, fmt.Errorf("decoding stdout task result: %w", err)
	}
	return result, true, nil
}

// readPodLogs reads logs from the first pod of a task's job.
func (r *TaskReconciler) readPodLogs(ctx context.Context, task *corev1alpha1.Task) (string, error) {
	return r.readPodLogsWithOptions(ctx, task, fullPodLogOptions(), true)
}

func (r *TaskReconciler) readStdoutResultPodLogs(ctx context.Context, task *corev1alpha1.Task) (string, error) {
	return r.readPodLogsWithOptions(ctx, task, stdoutResultPodLogOptions(), false)
}

func fullPodLogOptions() corev1.PodLogOptions {
	limit := podLogLimitBytes
	return corev1.PodLogOptions{
		LimitBytes: &limit,
	}
}

func stdoutResultPodLogOptions() corev1.PodLogOptions {
	limit := stdoutResultLogLimitBytes
	return corev1.PodLogOptions{
		LimitBytes: &limit,
	}
}

func (r *TaskReconciler) readPodLogsWithOptions(ctx context.Context, task *corev1alpha1.Task, opts corev1.PodLogOptions, appendTruncatedMarker bool) (string, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(task.Namespace),
		client.MatchingLabels{"job-name": task.Status.JobName},
	); err != nil {
		return "", fmt.Errorf("listing pods: %w", err)
	}

	if len(podList.Items) == 0 {
		return "", fmt.Errorf("no pods found for job %s", task.Status.JobName)
	}

	pod := podList.Items[len(podList.Items)-1]
	req := r.KubeClient.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("streaming logs: %w", err)
	}
	defer stream.Close() //nolint:errcheck

	limit := podLogLimitBytes
	if opts.LimitBytes != nil && *opts.LimitBytes > 0 {
		limit = *opts.LimitBytes
	}
	data, err := io.ReadAll(io.LimitReader(stream, limit))
	if err != nil {
		return "", fmt.Errorf("reading logs: %w", err)
	}

	if appendTruncatedMarker && int64(len(data)) == limit {
		data = append(data, "\n[truncated]"...)
	}

	return string(data), nil
}

// resolveProviderRef determines which provider reference to use
// Priority: Task.Spec.AI.ProviderRef > Agent.Spec.ProviderRef
func (r *TaskReconciler) resolveProviderRef(task *corev1alpha1.Task, agent *corev1alpha1.Agent) *corev1alpha1.ProviderReference {
	// Agent tasks don't use providers (CLI runtimes manage their own credentials)
	if task.Spec.Type == corev1alpha1.TaskTypeAgent {
		return nil
	}

	// Check task-level provider ref first
	if task.Spec.AI != nil && task.Spec.AI.ProviderRef != nil {
		return task.Spec.AI.ProviderRef
	}

	// Check agent-level provider ref
	if agent != nil && agent.Spec.ProviderRef != nil {
		return agent.Spec.ProviderRef
	}

	return nil
}

// validateExecutionWorkspace validates optional durable workspace settings.
func (r *TaskReconciler) validateExecutionWorkspace(task *corev1alpha1.Task) error {
	if task.Spec.Execution == nil || task.Spec.Execution.Workspace == nil {
		return nil
	}

	ws := task.Spec.Execution.Workspace
	if ws.ClassRef != nil {
		if strings.TrimSpace(ws.ClassRef.Name) == "" {
			return fmt.Errorf("execution workspace classRef.name is required")
		}
		if !r.WorkspaceProviderAPIEnabled {
			return fmt.Errorf("execution workspace classRef requires the workspace provider API")
		}
		if task.Spec.Type != corev1alpha1.TaskTypeAgent {
			return fmt.Errorf("execution workspace classRef is only supported for type: agent tasks")
		}
		// Class resolution, policy validation, and provider gating run on the
		// ACP execution plan path, which owns every class-shaped rejection.
		return validateExecutionWorkspacePolicyShape(task, ws)
	}
	if !ws.Enabled {
		return nil
	}
	provider := resolveWorkspaceProvider(ws, r.ExecutionWorkspaceDefaultProvider)

	if err := validateExecutionWorkspaceBasics(task, provider); err != nil {
		return err
	}
	return validateExecutionWorkspacePolicyShape(task, ws)
}

func validateExecutionWorkspaceBasics(
	task *corev1alpha1.Task,
	provider corev1alpha1.WorkspaceProvider,
) error {
	if !supportedWorkspaceProvider(provider) {
		return fmt.Errorf("unsupported execution workspace provider %q", provider)
	}
	if task.Spec.Type != corev1alpha1.TaskTypeAgent {
		return fmt.Errorf("execution workspace is only supported for type: agent tasks")
	}
	return nil
}

func validateExecutionWorkspacePolicyShape(task *corev1alpha1.Task, ws *corev1alpha1.ExecutionWorkspaceSpec) error {
	if !statusrules.IsOptionalReusePolicy(ws.ReusePolicy) {
		return fmt.Errorf("unsupported execution workspace reusePolicy %q", ws.ReusePolicy)
	}

	if !statusrules.IsOptionalCleanupPolicy(ws.CleanupPolicy) {
		return fmt.Errorf("unsupported execution workspace cleanupPolicy %q", ws.CleanupPolicy)
	}
	if ws.ReusePolicy == corev1alpha1.WorkspaceReusePolicySession && (task.Spec.SessionRef == nil || task.Spec.SessionRef.Name == "") {
		return fmt.Errorf("execution workspace reusePolicy %q requires spec.sessionRef.name", ws.ReusePolicy)
	}

	return nil
}

func (r *TaskReconciler) markExecutionWorkspaceValidationFailed(ctx context.Context, task *corev1alpha1.Task, validationErr error) error {
	if task == nil || task.Spec.Execution == nil || task.Spec.Execution.Workspace == nil ||
		(!task.Spec.Execution.Workspace.Enabled && task.Spec.Execution.Workspace.ClassRef == nil) {
		return nil
	}

	now := metav1.Now()
	message := ""
	if validationErr != nil {
		message = validationErr.Error()
	}
	ws := task.Spec.Execution.Workspace
	failure := statusrules.ValidationFailure{
		Message:    message,
		ObservedAt: &now,
	}
	// A class-shaped request has no author-selected provider or template; the
	// backend is resolved through the class and stays out of a failed
	// projection so a wrong default is never displayed.
	provider := corev1alpha1.WorkspaceProvider("")
	if ws.ClassRef == nil {
		provider = resolveWorkspaceProvider(ws, r.ExecutionWorkspaceDefaultProvider)
		if supportedWorkspaceProvider(provider) {
			failure.Provider = provider
			failure.TemplateRef = r.executionWorkspaceStatusTemplateRef(task, provider)
		}
	}
	if reusePolicy, ok := executionWorkspaceStatusReusePolicy(ws); ok {
		failure.ReusePolicy = reusePolicy
	}
	if cleanupPolicy, ok := r.executionWorkspaceStatusCleanupPolicy(ws, provider); ok {
		failure.CleanupPolicy = cleanupPolicy
	}
	status := statusrules.ValidationFailedStatus(failure)

	return r.updateStatusWithRetry(ctx, task, func(t *corev1alpha1.Task) {
		t.Status.ExecutionWorkspace = status
	})
}

func (r *TaskReconciler) executionWorkspaceStatusTemplateRef(task *corev1alpha1.Task, provider corev1alpha1.WorkspaceProvider) *corev1alpha1.WorkspaceTemplateReference {
	ws := task.Spec.Execution.Workspace
	var name string
	var namespace string
	switch provider {
	case corev1alpha1.WorkspaceProviderAgentSandbox:
		cfg := r.AgentSandboxConfig.WithDefaults()
		name = executionWorkspaceTemplateName(ws, cfg)
		namespace = executionWorkspaceTemplateNamespace(ws, task.Namespace, cfg)
	case corev1alpha1.WorkspaceProviderSubstrate:
		cfg := r.SubstrateConfig.WithDefaults()
		name = substrateTemplateName(ws, cfg)
		namespace = substrateTemplateNamespace(ws, task.Namespace, cfg)
	default:
		return nil
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	return &corev1alpha1.WorkspaceTemplateReference{
		Name:      name,
		Namespace: strings.TrimSpace(namespace),
	}
}

func executionWorkspaceStatusReusePolicy(ws *corev1alpha1.ExecutionWorkspaceSpec) (corev1alpha1.WorkspaceReusePolicy, bool) {
	if ws == nil || ws.ReusePolicy == "" {
		return corev1alpha1.WorkspaceReusePolicyNone, true
	}
	switch ws.ReusePolicy {
	case corev1alpha1.WorkspaceReusePolicyNone, corev1alpha1.WorkspaceReusePolicySession:
		return ws.ReusePolicy, true
	default:
		return "", false
	}
}

func (r *TaskReconciler) executionWorkspaceStatusCleanupPolicy(ws *corev1alpha1.ExecutionWorkspaceSpec, provider corev1alpha1.WorkspaceProvider) (corev1alpha1.WorkspaceCleanupPolicy, bool) {
	if ws != nil && ws.CleanupPolicy != "" {
		switch ws.CleanupPolicy {
		case corev1alpha1.WorkspaceCleanupPolicyDelete, corev1alpha1.WorkspaceCleanupPolicyRetain:
			return ws.CleanupPolicy, true
		default:
			return "", false
		}
	}
	switch provider {
	case corev1alpha1.WorkspaceProviderAgentSandbox:
		return executionWorkspaceStatusValidCleanupPolicy(r.AgentSandboxConfig.WithDefaults().CleanupPolicy)
	case corev1alpha1.WorkspaceProviderSubstrate:
		return executionWorkspaceStatusValidCleanupPolicy(r.SubstrateConfig.WithDefaults().CleanupPolicy)
	default:
		return corev1alpha1.WorkspaceCleanupPolicyDelete, true
	}
}

func executionWorkspaceStatusValidCleanupPolicy(cleanupPolicy corev1alpha1.WorkspaceCleanupPolicy) (corev1alpha1.WorkspaceCleanupPolicy, bool) {
	return statusrules.StatusCleanupPolicy(cleanupPolicy, "")
}

func validateRuntimeRefAgentTaskRestrictions(task *corev1alpha1.Task, agent *corev1alpha1.Agent) error {
	if agent != nil && agent.Spec.Runtime != nil {
		if len(agent.Spec.Runtime.DefaultAllowedTools) > 0 {
			return fmt.Errorf("runtimeRef custom runtimes require task-level allowedTools for brokered tool exposure and do not support defaultAllowedTools policy metadata")
		}
		if agent.Spec.Runtime.DefaultAllowBash != nil {
			return fmt.Errorf("runtimeRef custom runtimes do not support defaultAllowBash policy metadata")
		}
		if strings.TrimSpace(agent.Spec.Runtime.DefaultReasoningEffort) != "" {
			return fmt.Errorf("runtimeRef custom runtimes do not support defaultReasoningEffort policy metadata")
		}
	}
	if task != nil && task.Spec.AgentRuntime != nil {
		if len(task.Spec.AgentRuntime.DisallowedTools) > 0 {
			return fmt.Errorf("runtimeRef custom runtimes do not support disallowedTools policy metadata")
		}
		if task.Spec.AgentRuntime.AllowBash != nil {
			return fmt.Errorf("runtimeRef custom runtimes do not support allowBash policy metadata")
		}
	}
	if agent != nil && agent.Spec.SecretRef != nil && strings.TrimSpace(agent.Spec.SecretRef.Name) != "" {
		return fmt.Errorf("runtimeRef custom runtimes do not support agent secretRef credential delivery")
	}
	if task != nil && task.Spec.SecretRef != nil && strings.TrimSpace(task.Spec.SecretRef.Name) != "" {
		return fmt.Errorf("runtimeRef custom runtimes do not support task secretRef credential delivery")
	}
	if task != nil && task.Spec.PriorTaskRef != nil {
		return fmt.Errorf("runtimeRef custom runtimes do not support priorTaskRef workspace handoff")
	}
	return nil
}

// validateTaskAgentCompatibility validates that the task type and agent configuration are compatible.
func (r *TaskReconciler) validateTaskAgentCompatibility(task *corev1alpha1.Task, agent *corev1alpha1.Agent) error {
	switch task.Spec.Type {
	case corev1alpha1.TaskTypeAgent:
		return r.validateAgentRuntimeTaskCompatibility(task, agent)
	case corev1alpha1.TaskTypeAI:
		return validateAITaskAgentCompatibility(task, agent)
	case corev1alpha1.TaskTypeContainer:
		return nil
	default:
		return nil
	}
}

func (r *TaskReconciler) validateAgentRuntimeTaskCompatibility(task *corev1alpha1.Task, agent *corev1alpha1.Agent) error {
	if agent == nil {
		return fmt.Errorf("type: agent tasks require an agentRef")
	}
	if agent.Spec.Runtime == nil {
		return fmt.Errorf("agent %q does not have a runtime configured (required for type: agent tasks)", agent.Name)
	}
	hasRuntimeRef := agent.Spec.Runtime.RuntimeRef != nil && strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) != ""
	hasBuiltInRuntime := strings.TrimSpace(string(agent.Spec.Runtime.Type)) != ""
	switch {
	case hasRuntimeRef && hasBuiltInRuntime:
		return fmt.Errorf("agent %q sets both runtime.type and runtime.runtimeRef; set exactly one", agent.Name)
	case hasRuntimeRef:
		if err := validateRuntimeRefAgentTaskRestrictions(task, agent); err != nil {
			return err
		}
	case hasBuiltInRuntime:
		if err := validateBuiltInRuntimeTaskCompatibility(task, agent); err != nil {

			return err
		}
	default:
		return fmt.Errorf("agent %q runtime must set exactly one of type or runtimeRef", agent.Name)
	}
	if agent.Spec.Execution != nil && agent.Spec.Execution.Workspace != nil &&
		(agent.Spec.Execution.Workspace.Enabled || agent.Spec.Execution.Workspace.ClassRef != nil) {
		return fmt.Errorf("agent %q sets spec.execution.workspace, but execution workspace requests are only supported on Task.spec.execution.workspace", agent.Name)
	}
	if agent.Spec.ProviderRef != nil {
		return fmt.Errorf("agent %q has both runtime and providerRef set (mutually exclusive)", agent.Name)
	}
	if agent.Spec.Model != nil && agent.Spec.Model.Provider != "" {
		return fmt.Errorf("agent %q has both runtime and model.provider set (mutually exclusive for agent tasks)", agent.Name)
	}
	if len(task.Spec.Env) > 0 {
		return fmt.Errorf("type: agent ACP runtime tasks do not support arbitrary task env; use the reviewed runtime profile")
	}
	if agent.Spec.Coordination != nil && len(agent.Spec.Coordination.ApprovalRequiredTools) > 0 {
		return fmt.Errorf("agent %q approvalRequiredTools is only supported for type: ai autonomous tasks", agent.Name)
	}
	if task.Spec.Prompt == "" && (task.Spec.SessionRef == nil || !task.Spec.SessionRef.PromptIncluded ||
		strings.TrimSpace(task.Spec.SessionRef.ThroughMessageID) == "") {
		return fmt.Errorf("prompt is required for type: agent tasks unless included in a bounded Session transcript")
	}
	return nil
}

func validateBuiltInRuntimeTaskCompatibility(task *corev1alpha1.Task, agent *corev1alpha1.Agent) error {
	if err := validateBuiltInAgentRuntime(agent.Spec.Runtime.Type); err != nil {
		return err
	}
	if err := validateBuiltInACPAgentCredentialSecretRef(agent); err != nil {
		return err
	}
	if isBuiltInACPProviderRuntime(agent.Spec.Runtime.Type) && task.Spec.SecretRef != nil {
		return fmt.Errorf("built-in ACP runtime %q does not support task secretRef; provider credentials are controller-managed", agent.Spec.Runtime.Type)
	}
	if err := ValidateOpenCodeAgentSpec(agent); err != nil {
		return err
	}
	return validateReadOnlyBuiltInAgentRuntime(task, agent.Spec.Runtime.Type)
}

func validateBuiltInAgentRuntime(runtimeType corev1alpha1.AgentRuntimeType) error {
	if isBuiltInACPProviderRuntime(runtimeType) {
		return nil
	}
	return fmt.Errorf("agent runtime %q does not have an ACP runtime profile configured", runtimeType)
}

func validateReadOnlyBuiltInAgentRuntime(task *corev1alpha1.Task, runtimeType corev1alpha1.AgentRuntimeType) error {
	if !taskRequestsReadOnlyAgent(task) {
		return nil
	}
	switch runtimeType {
	case corev1alpha1.AgentRuntimeCopilot:
		return fmt.Errorf("read-only agent tasks do not support copilot runtime credentials because GITHUB_TOKEN can mutate GitHub")
	default:
		// Codex is supported: read-only tasks run inside the RuntimeSession
		// boundary with controller-rejected elevation requests,
		// supervisor-mediated file writes, and read-intent workspace delta
		// classification failing any modifying turn, with the same
		// per-session loopback provider credential the other runtimes
		// receive.
		return nil
	}
}

func validateAITaskAgentCompatibility(task *corev1alpha1.Task, agent *corev1alpha1.Agent) error {
	if agent != nil && agent.Spec.Runtime != nil {
		return fmt.Errorf("agent %q has runtime configured (use type: agent instead of type: ai)", agent.Name)
	}
	if aiTaskRequestsApprovalTooling(task, agent) && !agentHasAutonomousCoordination(agent) {
		return fmt.Errorf("request_approval requires enabled autonomous coordination mode")
	}
	if agent == nil || agent.Spec.Coordination == nil {
		return nil
	}
	approvalRequiredTools := agent.Spec.Coordination.ApprovalRequiredTools
	if len(approvalRequiredTools) > 0 && (!agent.Spec.Coordination.Enabled || !agent.Spec.Coordination.Autonomous) {
		return fmt.Errorf("agent %q approvalRequiredTools requires enabled autonomous coordination mode", agent.Name)
	}
	if invalidTool := invalidApprovalRequiredBuiltInTool(approvalRequiredTools); invalidTool != "" {
		return fmt.Errorf("agent %q approvalRequiredTools cannot include built-in tool %q", agent.Name, invalidTool)
	}
	return nil
}

func aiTaskRequestsApprovalTooling(task *corev1alpha1.Task, agent *corev1alpha1.Agent) bool {
	if agent != nil {
		for _, toolRef := range agent.Spec.Tools {
			if toolRef.Enabled != nil && !*toolRef.Enabled {
				continue
			}
			if strings.TrimSpace(toolRef.Name) == "request_approval" {
				return true
			}
		}
	}
	if task != nil && task.Spec.AI != nil {
		for _, toolName := range task.Spec.AI.Tools {
			if strings.TrimSpace(toolName) == "request_approval" {
				return true
			}
		}
	}
	return false
}

func agentHasAutonomousCoordination(agent *corev1alpha1.Agent) bool {
	return agent != nil && agent.Spec.Coordination != nil && agent.Spec.Coordination.Enabled && agent.Spec.Coordination.Autonomous
}

func invalidApprovalRequiredBuiltInTool(values []string) string {
	builtIns := approvalRequiredBuiltInToolSet()
	for _, value := range values {
		toolName := strings.TrimSpace(value)
		if builtIns[toolName] {
			return toolName
		}
	}
	return ""
}

func approvalRequiredBuiltInToolSet() map[string]bool {
	builtIns := map[string]bool{}
	for _, name := range tools.KnownBuiltInToolNames() {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			builtIns[trimmed] = true
		}
	}
	return builtIns
}

// handleScheduled manages the scheduling loop for recurring tasks.
func (r *TaskReconciler) handleScheduled(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Check if suspended
	if task.Spec.Suspend != nil && *task.Spec.Suspend {
		log.Info("Task is suspended, skipping schedule check")
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	// Parse schedule
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	sched, err := parser.Parse(task.Spec.Schedule)
	if err != nil {
		task.Status.Phase = corev1alpha1.TaskPhaseFailed
		task.Status.Message = fmt.Sprintf("invalid cron expression: %v", err)
		_ = r.Status().Update(ctx, task)
		return ctrl.Result{}, nil
	}

	// Determine time zone
	now := time.Now().UTC()
	loc := time.UTC
	if task.Spec.TimeZone != nil {
		if l, err := time.LoadLocation(*task.Spec.TimeZone); err == nil {
			loc = l
			now = now.In(loc)
		}
	}

	// Calculate the scheduled time for the next (or current) run
	var scheduledTime time.Time
	if task.Status.LastScheduleTime != nil {
		scheduledTime = sched.Next(task.Status.LastScheduleTime.In(loc))
	} else {
		scheduledTime = sched.Next(task.CreationTimestamp.In(loc))
	}

	// Not yet time
	if now.Before(scheduledTime) {
		nextSchedule := metav1.NewTime(scheduledTime)
		if task.Status.NextScheduleTime == nil || !task.Status.NextScheduleTime.Equal(&nextSchedule) {
			nextScheduleCopy := nextSchedule
			_ = r.updateStatusWithRetry(ctx, task, func(t *corev1alpha1.Task) {
				t.Status.NextScheduleTime = &nextScheduleCopy
			})
		}
		return ctrl.Result{RequeueAfter: time.Until(scheduledTime)}, nil
	}

	// Check starting deadline
	deadlineSeconds := int64(100) // default
	if task.Spec.StartingDeadlineSeconds != nil {
		deadlineSeconds = *task.Spec.StartingDeadlineSeconds
	}
	if now.Sub(scheduledTime) > time.Duration(deadlineSeconds)*time.Second {
		log.Info("Missed schedule beyond deadline, skipping", "scheduledTime", scheduledTime, "deadline", deadlineSeconds)
		r.Recorder.Eventf(task, "Warning", "MissedSchedule", "Missed scheduled run at %s (deadline %ds exceeded)", scheduledTime.Format(time.RFC3339), deadlineSeconds)
		// Advance to next schedule time. Re-anchor LastScheduleTime to now so
		// the next computation starts from the present instead of the stale
		// pre-skip slot; otherwise a suspend (or any gap) longer than the
		// starting deadline freezes the anchor and every later reconcile
		// recomputes the same long-missed slot, advancing NextScheduleTime
		// forever without ever running.
		next := sched.Next(now)
		nextSchedule := metav1.NewTime(next)
		nextScheduleCopy := nextSchedule
		// Advancing the cursor is the documented LastScheduleTime contract
		// for a skipped window: without it the same missed tick is
		// re-evaluated on every reconcile and the schedule never resumes.
		reanchor := metav1.NewTime(now)
		if err := r.updateStatusWithRetry(ctx, task, func(t *corev1alpha1.Task) {
			t.Status.NextScheduleTime = &nextScheduleCopy
			t.Status.LastScheduleTime = &reanchor
		}); err != nil {
			// A failed cursor write must retry promptly: sleeping to the
			// next cron tick with the stale cursor would skip that run too.
			return ctrl.Result{}, fmt.Errorf("re-anchoring schedule cursor: %w", err)
		}
		return ctrl.Result{RequeueAfter: time.Until(next)}, nil
	}

	// Check concurrency policy
	if task.Spec.ConcurrencyPolicy == corev1alpha1.ForbidConcurrent || task.Spec.ConcurrencyPolicy == "" {
		var childList corev1alpha1.TaskList
		if err := r.List(ctx, &childList, client.InNamespace(task.Namespace), client.MatchingLabels{
			labels.LabelParentTask: labels.SelectorValue(task.Name),
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("listing child tasks: %w", err)
		}
		for i := range childList.Items {
			if taskPhaseCountsTowardConcurrency(childList.Items[i].Status.Phase) {
				log.Info("Concurrency policy Forbid: active child task exists, skipping", "activeChild", childList.Items[i].Name)
				next := sched.Next(now)
				nextSchedule := metav1.NewTime(next)
				nextScheduleCopy := nextSchedule
				_ = r.updateStatusWithRetry(ctx, task, func(t *corev1alpha1.Task) {
					t.Status.NextScheduleTime = &nextScheduleCopy
				})
				return ctrl.Result{RequeueAfter: time.Until(next)}, nil
			}
		}
	}

	// Create child task with deterministic name
	childName := fmt.Sprintf("%s-%d", task.Name, scheduledTime.Unix())
	childAnnotations := map[string]string{
		labels.AnnotationParentTaskName: task.Name,
	}
	if task.Annotations[labels.AnnotationDisableCoordinationToolInject] == scheduledRunLabelValue {
		childAnnotations[labels.AnnotationDisableCoordinationToolInject] = scheduledRunLabelValue
	}
	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childName,
			Namespace: task.Namespace,
			Labels: map[string]string{
				labels.LabelParentTask:   labels.SelectorValue(task.Name),
				labels.LabelScheduledRun: scheduledRunLabelValue,
			},
			Annotations: childAnnotations,
		},
		Spec: *task.Spec.DeepCopy(),
	}

	// Strip scheduling fields from child
	child.Spec.Schedule = ""
	child.Spec.TimeZone = nil
	child.Spec.ConcurrencyPolicy = ""
	child.Spec.StartingDeadlineSeconds = nil
	child.Spec.SuccessfulRunsHistoryLimit = nil
	child.Spec.FailedRunsHistoryLimit = nil
	child.Spec.Suspend = nil
	tracing.StampTaskTraceContext(ctx, child)

	// Set owner reference
	if err := ctrl.SetControllerReference(task, child, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting owner reference: %w", err)
	}

	if err := r.Create(ctx, child); err != nil {
		if apierrors.IsAlreadyExists(err) {
			log.Info("Child task already exists (idempotent)", "child", childName)
		} else {
			return ctrl.Result{}, fmt.Errorf("creating child task: %w", err)
		}
	} else {
		log.Info("Created scheduled child task", "child", childName)
		r.Recorder.Eventf(task, "Normal", "ScheduledRun", "Created child task %s", childName)
	}

	// Update status
	nowTime := metav1.NewTime(scheduledTime)
	next := sched.Next(now)
	nextSchedule := metav1.NewTime(next)
	nowTimeCopy := nowTime
	nextScheduleCopy := nextSchedule
	if err := r.updateStatusWithRetry(ctx, task, func(t *corev1alpha1.Task) {
		t.Status.LastScheduleTime = &nowTimeCopy
		t.Status.NextScheduleTime = &nextScheduleCopy
	}); err != nil {
		return ctrl.Result{}, err
	}

	// Enforce history limits
	if err := r.enforceHistoryLimits(ctx, task); err != nil {
		log.Error(err, "Failed to enforce history limits")
	}

	return ctrl.Result{RequeueAfter: time.Until(next)}, nil
}

// enforceHistoryLimits removes old child tasks beyond the configured limits.
func (r *TaskReconciler) enforceHistoryLimits(ctx context.Context, task *corev1alpha1.Task) error {
	var childList corev1alpha1.TaskList
	if err := r.List(ctx, &childList, client.InNamespace(task.Namespace), client.MatchingLabels{
		labels.LabelParentTask: labels.SelectorValue(task.Name),
	}); err != nil {
		return fmt.Errorf("listing child tasks: %w", err)
	}

	successLimit := int32(3)
	if task.Spec.SuccessfulRunsHistoryLimit != nil {
		successLimit = *task.Spec.SuccessfulRunsHistoryLimit
	}
	failedLimit := int32(1)
	if task.Spec.FailedRunsHistoryLimit != nil {
		failedLimit = *task.Spec.FailedRunsHistoryLimit
	}

	var succeeded, failed []*corev1alpha1.Task
	for i := range childList.Items {
		child := &childList.Items[i]
		switch child.Status.Phase {
		case corev1alpha1.TaskPhaseSucceeded:
			succeeded = append(succeeded, child)
		case corev1alpha1.TaskPhaseFailed:
			failed = append(failed, child)
		}
	}

	// Sort by creation time (oldest first) and delete excess
	sortByCreation := func(tasks []*corev1alpha1.Task) {
		slices.SortFunc(tasks, func(a, b *corev1alpha1.Task) int {
			return a.CreationTimestamp.Compare(b.CreationTimestamp.Time)
		})
	}

	sortByCreation(succeeded)
	sortByCreation(failed)

	for i := 0; i < len(succeeded)-int(successLimit); i++ {
		if err := r.Delete(ctx, succeeded[i]); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting old succeeded child: %w", err)
		}
	}

	for i := 0; i < len(failed)-int(failedLimit); i++ {
		if err := r.Delete(ctx, failed[i]); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting old failed child: %w", err)
		}
	}

	return nil
}

type trustedServiceReadCleanupRunnable struct {
	reconciler *TaskReconciler
}

func (r *trustedServiceReadCleanupRunnable) Start(ctx context.Context) error {
	if r == nil || r.reconciler == nil {
		return errors.New("trusted Service RBAC cleanup reconciler is required")
	}
	if err := r.reconciler.pruneTrustedServiceReadBindingsOnce(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func (*trustedServiceReadCleanupRunnable) NeedLeaderElection() bool { return true }

// SetupWithManager sets up the controller with the Manager.
func (r *TaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("task-controller") //nolint:staticcheck
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if !r.EnforceNamespaceIsolation {
		var cleanup manager.Runnable = &trustedServiceReadCleanupRunnable{reconciler: r}
		if err := mgr.Add(cleanup); err != nil {
			return fmt.Errorf("register trusted Service RBAC startup cleanup: %w", err)
		}
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Event{}, eventInvolvedObjectNameField, eventInvolvedObjectNameIndex); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Event{}, eventReasonField, eventReasonIndex); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1alpha1.Task{}).
		Owns(&batchv1.Job{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&corev1alpha1.Task{}).
		Named("task").
		Complete(r)
}

const (
	DefaultAIWorkerClusterRoleName        = "orka-ai-worker-role"
	DefaultVendorWorkerClusterRoleName    = "orka-vendor-worker-role"
	DefaultContainerWorkerClusterRoleName = "orka-container-worker-role"

	maxWorkerRoleBindingNameLength = 253
	workerRoleBindingHashLength    = 10

	managedByLabelKey                         = "app.kubernetes.io/managed-by"
	managedByLabelValue                       = "orka"
	orkaManagedByLabelKey                     = "orka.ai/managed-by"
	trustedServiceReaderLabelKey              = "orka.ai/trusted-service-reader"
	trustedServiceReaderTaskNamespaceLabelKey = "orka.ai/trusted-service-reader-for"
	trustedServiceReaderLabelValue            = booleanTrueValue
)

type workerRBACSpec struct {
	serviceAccountName string
	clusterRoleName    string
	roleBindingName    string
}

// workerRBACSpecs binds cluster-scoped worker roles into each task namespace.
// The AI worker role is intentionally broader because code_exec's Kubernetes
// backend creates per-job ServiceAccounts and Secrets; vendor and container
// workers use separate, narrower roles so those cluster-wide capabilities are
// not shared with less-trusted task types.
func (r *TaskReconciler) aiWorkerServiceAccountName() string {
	return workerServiceAccountName(r.AIWorkerServiceAccountName, AIWorkerServiceAccount)
}

func (r *TaskReconciler) workerRBACSpecs(namespace string) []workerRBACSpec {
	return []workerRBACSpec{
		{
			serviceAccountName: r.aiWorkerServiceAccountName(),
			clusterRoleName:    workerClusterRoleName(r.AIWorkerClusterRoleName, DefaultAIWorkerClusterRoleName),
			roleBindingName:    workerRoleBindingName(r.WorkerRoleBindingNamePrefix, "ai", namespace),
		},
		{
			serviceAccountName: workerServiceAccountName(r.VendorWorkerServiceAccountName, VendorWorkerServiceAccount),
			clusterRoleName:    workerClusterRoleName(r.VendorWorkerClusterRoleName, DefaultVendorWorkerClusterRoleName),
			roleBindingName:    workerRoleBindingName(r.WorkerRoleBindingNamePrefix, "vendor", namespace),
		},
		{
			serviceAccountName: workerServiceAccountName(r.ContainerWorkerServiceAccountName, ContainerWorkerServiceAccount),
			clusterRoleName:    workerClusterRoleName(r.ContainerWorkerClusterRoleName, DefaultContainerWorkerClusterRoleName),
			roleBindingName:    workerRoleBindingName(r.WorkerRoleBindingNamePrefix, "container", namespace),
		},
	}
}

func workerClusterRoleName(configured, fallback string) string {
	if configured != "" {
		return configured
	}
	return fallback
}

func workerRoleBindingName(prefix, tier, namespace string) string {
	if prefix == "" {
		prefix = managedByLabelValue
	}
	name := fmt.Sprintf("%s-%s-worker-%s", prefix, tier, namespace)
	if len(name) <= maxWorkerRoleBindingNameLength {
		return name
	}

	sum := sha256.Sum256([]byte(name))
	suffix := hex.EncodeToString(sum[:])[:workerRoleBindingHashLength]
	prefixLength := maxWorkerRoleBindingNameLength - workerRoleBindingHashLength - 1
	return fmt.Sprintf("%s-%s", name[:prefixLength], suffix)
}

// ensureWorkerRBAC ensures each worker ServiceAccount and worker role binding
// exists in the given namespace so that task jobs have trust-tiered permissions.
func (r *TaskReconciler) ensureWorkerRBAC(ctx context.Context, namespace string) error {
	if err := r.ensureTrustedServiceReadBindings(ctx, namespace); err != nil {
		return err
	}
	for _, spec := range r.workerRBACSpecs(namespace) {
		if err := r.ensureWorkerServiceAccount(ctx, namespace, spec.serviceAccountName); err != nil {
			return err
		}
		if err := r.ensureWorkerRoleBinding(ctx, namespace, spec); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskReconciler) trustedServiceReadReader() client.Reader {
	if r != nil && r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *TaskReconciler) ensureTrustedServiceReadBindings(ctx context.Context, taskNamespace string) error {
	r.trustedServiceCleanupMu.RLock()
	defer r.trustedServiceCleanupMu.RUnlock()
	refs := append(r.OutboundAccessTrust.Gateways.References(), r.OutboundAccessTrust.TokenEndpoints.References()...)
	desired := map[types.NamespacedName]struct{}{}
	for _, ref := range refs {
		if ref.Namespace == "" || ref.Namespace == taskNamespace {
			continue
		}
		key := types.NamespacedName{
			Namespace: ref.Namespace,
			Name: trustedServiceReadBindingName(
				taskNamespace,
				ref.Namespace,
				ref.Name,
				r.aiWorkerServiceAccountName(),
			),
		}
		if _, ok := desired[key]; ok {
			continue
		}
		desired[key] = struct{}{}
		objectLabels := trustedServiceReadBindingLabels(taskNamespace)
		role := &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: maps.Clone(objectLabels)},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{""}, Resources: []string{"services"}, ResourceNames: []string{ref.Name}, Verbs: []string{"get"},
			}},
		}
		if err := r.createOrUpdateTrustedServiceRole(ctx, role); err != nil {
			return err
		}
		binding := &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: maps.Clone(objectLabels)},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: key.Name},
			Subjects: []rbacv1.Subject{{
				Kind: rbacv1.ServiceAccountKind, Name: r.aiWorkerServiceAccountName(), Namespace: taskNamespace,
			}},
		}
		if err := r.createOrUpdateTrustedServiceRoleBinding(ctx, binding); err != nil {
			return err
		}
	}
	if r.EnforceNamespaceIsolation {
		// Static installations reject cross-namespace trusted Service references
		// at startup, so they never discover or mutate legacy grants here.
		return nil
	}
	return r.pruneTrustedServiceReadBindings(ctx, taskNamespace, desired)
}

func trustedServiceReadBindingName(taskNamespace, serviceNamespace, serviceName string, serviceAccountNames ...string) string {
	serviceAccountName := AIWorkerServiceAccount
	if len(serviceAccountNames) > 0 && strings.TrimSpace(serviceAccountNames[0]) != "" {
		serviceAccountName = strings.TrimSpace(serviceAccountNames[0])
	}
	sum := sha256.Sum256([]byte(taskNamespace + "\x00" + serviceNamespace + "\x00" + serviceName + "\x00" + serviceAccountName))
	return "orka-outbound-service-" + hex.EncodeToString(sum[:])[:16]
}

func legacyTrustedServiceReadBindingName(taskNamespace, serviceNamespace, serviceName string) string {
	sum := sha256.Sum256([]byte(taskNamespace + "\x00" + serviceNamespace + "\x00" + serviceName))
	return "orka-outbound-service-" + hex.EncodeToString(sum[:])[:16]
}

func trustedServiceReadBindingLabels(taskNamespace string) map[string]string {
	return map[string]string{
		orkaManagedByLabelKey:                     managedByLabelValue,
		trustedServiceReaderLabelKey:              trustedServiceReaderLabelValue,
		trustedServiceReaderTaskNamespaceLabelKey: taskNamespace,
	}
}

func (r *TaskReconciler) pruneTrustedServiceReadBindings(
	ctx context.Context,
	taskNamespace string,
	desired map[types.NamespacedName]struct{},
) error {
	selector := client.MatchingLabels{
		orkaManagedByLabelKey:                     managedByLabelValue,
		trustedServiceReaderLabelKey:              trustedServiceReaderLabelValue,
		trustedServiceReaderTaskNamespaceLabelKey: taskNamespace,
	}
	bindings := &rbacv1.RoleBindingList{}
	if err := r.trustedServiceReadReader().List(ctx, bindings, selector); err != nil {
		return fmt.Errorf("listing trusted Service RoleBindings for namespace %q: %w", taskNamespace, err)
	}
	for i := range bindings.Items {
		binding := &bindings.Items[i]
		key := types.NamespacedName{Namespace: binding.Namespace, Name: binding.Name}
		if _, ok := desired[key]; ok || !strings.HasPrefix(binding.Name, "orka-outbound-service-") {
			continue
		}
		if err := r.Delete(ctx, binding); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting stale trusted Service RoleBinding %s/%s: %w", binding.Namespace, binding.Name, err)
		}
	}
	roles := &rbacv1.RoleList{}
	if err := r.trustedServiceReadReader().List(ctx, roles, selector); err != nil {
		return fmt.Errorf("listing trusted Service Roles for namespace %q: %w", taskNamespace, err)
	}
	for i := range roles.Items {
		role := &roles.Items[i]
		key := types.NamespacedName{Namespace: role.Namespace, Name: role.Name}
		if _, ok := desired[key]; ok || !strings.HasPrefix(role.Name, "orka-outbound-service-") {
			continue
		}
		if err := r.Delete(ctx, role); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting stale trusted Service Role %s/%s: %w", role.Namespace, role.Name, err)
		}
	}
	return nil
}

func (r *TaskReconciler) pruneTrustedServiceReadBindingsOnce(ctx context.Context) error {
	r.trustedServiceCleanupMu.Lock()
	defer r.trustedServiceCleanupMu.Unlock()
	if r.trustedServiceCleanupDone {
		return nil
	}
	activeTaskNamespaces, err := r.activeTaskNamespaces(ctx)
	if err != nil {
		return err
	}
	bindings := &rbacv1.RoleBindingList{}
	if err := r.trustedServiceReadReader().List(ctx, bindings); err != nil {
		return fmt.Errorf("listing trusted Service RoleBindings during startup cleanup: %w", err)
	}
	for i := range bindings.Items {
		binding := &bindings.Items[i]
		taskNamespace, ok := trustedServiceReadRoleBindingTaskNamespace(binding)
		if !ok {
			continue
		}
		key := types.NamespacedName{Namespace: binding.Namespace, Name: binding.Name}
		_, active := activeTaskNamespaces[taskNamespace]
		if active && r.trustedServiceReadBindingDesired(taskNamespace, key) {
			continue
		}
		if err := r.deleteTrustedServiceReadGrant(ctx, binding, taskNamespace); err != nil {
			return err
		}
	}
	roles := &rbacv1.RoleList{}
	if err := r.trustedServiceReadReader().List(ctx, roles, client.MatchingLabels{
		orkaManagedByLabelKey:        managedByLabelValue,
		trustedServiceReaderLabelKey: trustedServiceReaderLabelValue,
	}); err != nil {
		return fmt.Errorf("listing managed trusted Service Roles during startup cleanup: %w", err)
	}
	for i := range roles.Items {
		role := &roles.Items[i]
		taskNamespace := strings.TrimSpace(role.Labels[trustedServiceReaderTaskNamespaceLabelKey])
		if taskNamespace == "" || !strings.HasPrefix(role.Name, "orka-outbound-service-") {
			continue
		}
		key := types.NamespacedName{Namespace: role.Namespace, Name: role.Name}
		_, active := activeTaskNamespaces[taskNamespace]
		if active && r.trustedServiceReadBindingDesired(taskNamespace, key) {
			continue
		}
		if err := r.Delete(ctx, role); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting stale managed trusted Service Role %s/%s: %w", role.Namespace, role.Name, err)
		}
	}
	r.trustedServiceCleanupDone = true
	return nil
}

func (r *TaskReconciler) activeTaskNamespaces(ctx context.Context) (map[string]struct{}, error) {
	tasks := &corev1alpha1.TaskList{}
	if err := r.trustedServiceReadReader().List(ctx, tasks); err != nil {
		return nil, fmt.Errorf("listing Tasks during trusted Service RBAC startup cleanup: %w", err)
	}
	active := make(map[string]struct{}, len(tasks.Items))
	for i := range tasks.Items {
		if !tasks.Items[i].DeletionTimestamp.IsZero() {
			continue
		}
		if namespace := strings.TrimSpace(tasks.Items[i].Namespace); namespace != "" {
			active[namespace] = struct{}{}
		}
	}
	return active, nil
}

func (r *TaskReconciler) cleanupTrustedServiceReadBindingsAfterTaskRemoval(
	ctx context.Context,
	taskNamespace string,
) error {
	if r.EnforceNamespaceIsolation {
		return nil
	}
	taskNamespace = strings.TrimSpace(taskNamespace)
	if taskNamespace == "" {
		return nil
	}
	r.trustedServiceCleanupMu.Lock()
	defer r.trustedServiceCleanupMu.Unlock()
	tasks := &corev1alpha1.TaskList{}
	if err := r.trustedServiceReadReader().List(ctx, tasks, client.InNamespace(taskNamespace)); err != nil {
		return fmt.Errorf("listing Tasks before trusted Service RBAC cleanup for namespace %q: %w", taskNamespace, err)
	}
	for i := range tasks.Items {
		if tasks.Items[i].DeletionTimestamp.IsZero() {
			return nil
		}
	}
	return r.deleteTrustedServiceReadBindingsForNamespaceLocked(ctx, taskNamespace)
}

func (r *TaskReconciler) deleteTrustedServiceReadBindingsForNamespaceLocked(
	ctx context.Context,
	taskNamespace string,
) error {
	selector := client.MatchingLabels{
		orkaManagedByLabelKey:                     managedByLabelValue,
		trustedServiceReaderLabelKey:              trustedServiceReaderLabelValue,
		trustedServiceReaderTaskNamespaceLabelKey: taskNamespace,
	}
	bindings := &rbacv1.RoleBindingList{}
	if err := r.trustedServiceReadReader().List(ctx, bindings, selector); err != nil {
		return fmt.Errorf("listing trusted Service RoleBindings for inactive namespace %q: %w", taskNamespace, err)
	}
	for i := range bindings.Items {
		binding := &bindings.Items[i]
		if !strings.HasPrefix(binding.Name, "orka-outbound-service-") {
			continue
		}
		if err := r.deleteTrustedServiceReadGrant(ctx, binding, taskNamespace); err != nil {
			return err
		}
	}
	roles := &rbacv1.RoleList{}
	if err := r.trustedServiceReadReader().List(ctx, roles, selector); err != nil {
		return fmt.Errorf("listing trusted Service Roles for inactive namespace %q: %w", taskNamespace, err)
	}
	for i := range roles.Items {
		role := &roles.Items[i]
		if !strings.HasPrefix(role.Name, "orka-outbound-service-") {
			continue
		}
		if err := r.Delete(ctx, role); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting trusted Service Role %s/%s for inactive namespace %q: %w", role.Namespace, role.Name, taskNamespace, err)
		}
	}
	return r.deleteLegacyTrustedServiceReadBindingsForNamespaceLocked(ctx, taskNamespace)
}

func (r *TaskReconciler) deleteLegacyTrustedServiceReadBindingsForNamespaceLocked(
	ctx context.Context,
	taskNamespace string,
) error {
	refs := append(r.OutboundAccessTrust.Gateways.References(), r.OutboundAccessTrust.TokenEndpoints.References()...)
	seen := map[types.NamespacedName]struct{}{}
	for _, ref := range refs {
		if ref.Namespace == "" || ref.Namespace == taskNamespace {
			continue
		}
		key := types.NamespacedName{
			Namespace: ref.Namespace,
			Name:      legacyTrustedServiceReadBindingName(taskNamespace, ref.Namespace, ref.Name),
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		binding := &rbacv1.RoleBinding{}
		if err := r.trustedServiceReadReader().Get(ctx, key, binding); apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return fmt.Errorf("getting legacy trusted Service RoleBinding %s/%s: %w", key.Namespace, key.Name, err)
		}
		ownerNamespace, ok := legacyTrustedServiceReadRoleBindingTaskNamespace(binding)
		if !ok || ownerNamespace != taskNamespace {
			continue
		}
		if err := r.deleteTrustedServiceReadGrant(ctx, binding, taskNamespace); err != nil {
			return err
		}
	}
	return nil
}

func (r *TaskReconciler) trustedServiceReadBindingDesired(
	taskNamespace string,
	key types.NamespacedName,
) bool {
	refs := append(r.OutboundAccessTrust.Gateways.References(), r.OutboundAccessTrust.TokenEndpoints.References()...)
	for _, ref := range refs {
		if ref.Namespace == "" || ref.Namespace == taskNamespace || ref.Namespace != key.Namespace {
			continue
		}
		if key.Name == trustedServiceReadBindingName(taskNamespace, ref.Namespace, ref.Name, r.aiWorkerServiceAccountName()) {
			return true
		}
	}
	return false
}

func (r *TaskReconciler) deleteTrustedServiceReadGrant(
	ctx context.Context,
	binding *rbacv1.RoleBinding,
	taskNamespace string,
) error {
	key := types.NamespacedName{Namespace: binding.Namespace, Name: binding.Name}
	role := &rbacv1.Role{}
	if err := r.trustedServiceReadReader().Get(ctx, key, role); err == nil {
		serviceAccountName := ""
		if len(binding.Subjects) == 1 {
			subject := binding.Subjects[0]
			if subject.Kind == rbacv1.ServiceAccountKind && subject.APIGroup == "" && subject.Namespace == taskNamespace {
				serviceAccountName = subject.Name
			}
		}
		if trustedServiceReadRoleOwned(role, taskNamespace, serviceAccountName) {
			if err := r.Delete(ctx, role); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("deleting trusted Service Role %s/%s: %w", role.Namespace, role.Name, err)
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting trusted Service Role %s/%s: %w", key.Namespace, key.Name, err)
	}
	if err := r.Delete(ctx, binding); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting trusted Service RoleBinding %s/%s: %w", binding.Namespace, binding.Name, err)
	}
	return nil
}

func trustedServiceReadManaged(objectLabels map[string]string) bool {
	return objectLabels[orkaManagedByLabelKey] == managedByLabelValue &&
		objectLabels[trustedServiceReaderLabelKey] == trustedServiceReaderLabelValue
}

func trustedServiceReadRoleBindingTaskNamespace(binding *rbacv1.RoleBinding) (string, bool) {
	if binding != nil && trustedServiceReadManaged(binding.Labels) &&
		strings.HasPrefix(binding.Name, "orka-outbound-service-") {
		taskNamespace := strings.TrimSpace(binding.Labels[trustedServiceReaderTaskNamespaceLabelKey])
		if taskNamespace != "" {
			return taskNamespace, true
		}
	}
	return legacyTrustedServiceReadRoleBindingTaskNamespace(binding)
}

func legacyTrustedServiceReadRoleBindingTaskNamespace(binding *rbacv1.RoleBinding) (string, bool) {
	if binding == nil || !legacyTrustedServiceReadName(binding.Name) ||
		binding.RoleRef != (rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: binding.Name}) ||
		len(binding.Subjects) != 1 {
		return "", false
	}
	subject := binding.Subjects[0]
	if subject.Kind != rbacv1.ServiceAccountKind || subject.APIGroup != "" ||
		subject.Name != AIWorkerServiceAccount || strings.TrimSpace(subject.Namespace) == "" {
		return "", false
	}
	return subject.Namespace, true
}

func legacyTrustedServiceReadName(name string) bool {
	const prefix = "orka-outbound-service-"
	suffix := strings.TrimPrefix(name, prefix)
	if suffix == name || len(suffix) != 16 || suffix != strings.ToLower(suffix) {
		return false
	}
	_, err := hex.DecodeString(suffix)
	return err == nil
}

func trustedServiceReadRoleOwned(role *rbacv1.Role, taskNamespace string, serviceAccountNames ...string) bool {
	if role != nil && trustedServiceReadManaged(role.Labels) &&
		role.Labels[trustedServiceReaderTaskNamespaceLabelKey] == taskNamespace &&
		strings.HasPrefix(role.Name, "orka-outbound-service-") {
		return true
	}
	if len(serviceAccountNames) > 0 && trustedServiceReadRoleMatches(role, taskNamespace, serviceAccountNames[0]) {
		return true
	}
	return legacyTrustedServiceReadRole(role, taskNamespace)
}

func trustedServiceReadRoleMatches(role *rbacv1.Role, taskNamespace, serviceAccountName string) bool {
	if !trustedServiceReadRoleRuleValid(role) || strings.TrimSpace(serviceAccountName) == "" {
		return false
	}
	return role.Name == trustedServiceReadBindingName(taskNamespace, role.Namespace, role.Rules[0].ResourceNames[0], serviceAccountName)
}

func legacyTrustedServiceReadRole(role *rbacv1.Role, taskNamespace string) bool {
	if !trustedServiceReadRoleRuleValid(role) {
		return false
	}
	return role.Name == legacyTrustedServiceReadBindingName(taskNamespace, role.Namespace, role.Rules[0].ResourceNames[0])
}

func trustedServiceReadRoleRuleValid(role *rbacv1.Role) bool {
	if role == nil || !legacyTrustedServiceReadName(role.Name) || len(role.Rules) != 1 {
		return false
	}
	rule := role.Rules[0]
	if !slices.Equal(rule.APIGroups, []string{""}) ||
		!slices.Equal(rule.Resources, []string{"services"}) ||
		len(rule.ResourceNames) != 1 || strings.TrimSpace(rule.ResourceNames[0]) == "" ||
		!slices.Equal(rule.Verbs, []string{"get"}) || len(rule.NonResourceURLs) != 0 {
		return false
	}
	return true
}

func (r *TaskReconciler) createOrUpdateTrustedServiceRole(ctx context.Context, desired *rbacv1.Role) error {
	current := &rbacv1.Role{}
	err := r.trustedServiceReadReader().Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, current)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if reflect.DeepEqual(current.Rules, desired.Rules) && reflect.DeepEqual(current.Labels, desired.Labels) {
		return nil
	}
	return fmt.Errorf("trusted Service Role %s/%s already exists with unexpected ownership or permissions", desired.Namespace, desired.Name)
}

func (r *TaskReconciler) createOrUpdateTrustedServiceRoleBinding(ctx context.Context, desired *rbacv1.RoleBinding) error {
	current := &rbacv1.RoleBinding{}
	err := r.trustedServiceReadReader().Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, current)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if current.RoleRef == desired.RoleRef &&
		reflect.DeepEqual(current.Subjects, desired.Subjects) &&
		reflect.DeepEqual(current.Labels, desired.Labels) {
		return nil
	}
	return fmt.Errorf("trusted Service RoleBinding %s/%s already exists with unexpected ownership or subjects", desired.Namespace, desired.Name)
}

func (r *TaskReconciler) ensureWorkerServiceAccount(ctx context.Context, namespace, name string) error {
	log := logf.FromContext(ctx)

	sa := &corev1.ServiceAccount{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sa)
	if apierrors.IsNotFound(err) {
		sa = &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels: map[string]string{
					orkaManagedByLabelKey: managedByLabelValue,
				},
			},
		}
		if err := r.Create(ctx, sa); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("creating worker ServiceAccount %s/%s: %w", namespace, name, err)
			}
			if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sa); err != nil {
				return fmt.Errorf("getting worker ServiceAccount %s/%s after create conflict: %w", namespace, name, err)
			}
		} else {
			log.Info("Created worker ServiceAccount", "namespace", namespace, "serviceAccount", name)
			return nil
		}
	} else if err != nil {
		return fmt.Errorf("getting worker ServiceAccount %s/%s: %w", namespace, name, err)
	}

	if sa.Labels == nil {
		sa.Labels = map[string]string{}
	}
	if sa.Labels[orkaManagedByLabelKey] != managedByLabelValue {
		sa.Labels[orkaManagedByLabelKey] = managedByLabelValue
		if err := r.Update(ctx, sa); err != nil {
			return fmt.Errorf("updating worker ServiceAccount %s/%s labels: %w", namespace, name, err)
		}
		log.Info("Updated worker ServiceAccount", "namespace", namespace, "serviceAccount", name)
	}

	return nil
}

func (r *TaskReconciler) ensureWorkerRoleBinding(ctx context.Context, namespace string, spec workerRBACSpec) error {
	log := logf.FromContext(ctx)
	desired := workerRoleBinding(namespace, spec)

	rb := &rbacv1.RoleBinding{}
	err := r.Get(ctx, types.NamespacedName{Name: spec.roleBindingName, Namespace: namespace}, rb)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("creating worker RoleBinding %s/%s: %w", namespace, spec.roleBindingName, err)
			}
			if err := r.Get(ctx, types.NamespacedName{Name: spec.roleBindingName, Namespace: namespace}, rb); err != nil {
				return fmt.Errorf("getting worker RoleBinding %s/%s after create conflict: %w", namespace, spec.roleBindingName, err)
			}
		} else {
			log.Info("Created worker RoleBinding", "namespace", namespace, "binding", spec.roleBindingName, "serviceAccount", spec.serviceAccountName, "clusterRole", spec.clusterRoleName)
			return nil
		}
	} else if err != nil {
		return fmt.Errorf("getting worker RoleBinding %s/%s: %w", namespace, spec.roleBindingName, err)
	}

	if rb.RoleRef != desired.RoleRef {
		recreated, err := r.recreateWorkerRoleBinding(ctx, namespace, spec, rb, desired)
		if err != nil {
			return err
		}
		rb = recreated
	}

	changed := false
	if rb.Labels == nil {
		rb.Labels = map[string]string{}
	}
	if rb.Labels[managedByLabelKey] != managedByLabelValue {
		rb.Labels[managedByLabelKey] = managedByLabelValue
		changed = true
	}
	if !subjectsEqual(rb.Subjects, desired.Subjects) {
		rb.Subjects = desired.Subjects
		changed = true
	}

	if changed {
		if err := r.Update(ctx, rb); err != nil {
			return fmt.Errorf("updating worker RoleBinding %s/%s: %w", namespace, spec.roleBindingName, err)
		}
		log.Info("Updated worker RoleBinding", "namespace", namespace, "binding", spec.roleBindingName, "serviceAccount", spec.serviceAccountName, "clusterRole", spec.clusterRoleName)
	}

	return nil
}

func (r *TaskReconciler) recreateWorkerRoleBinding(ctx context.Context, namespace string, spec workerRBACSpec, current, desired *rbacv1.RoleBinding) (*rbacv1.RoleBinding, error) {
	log := logf.FromContext(ctx)
	log.Info("Recreating worker RoleBinding with stale RoleRef", "namespace", namespace, "binding", spec.roleBindingName, "currentKind", current.RoleRef.Kind, "currentName", current.RoleRef.Name, "desiredKind", desired.RoleRef.Kind, "desiredName", desired.RoleRef.Name)

	if err := r.Delete(ctx, current); err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("deleting worker RoleBinding %s/%s with stale RoleRef %s/%s: %w", namespace, spec.roleBindingName, current.RoleRef.Kind, current.RoleRef.Name, err)
	}

	var recreated *rbacv1.RoleBinding
	err := wait.PollUntilContextTimeout(ctx, workerRoleBindingRecreateInterval, workerRoleBindingRecreateTimeout, true, func(ctx context.Context) (bool, error) {
		latest := &rbacv1.RoleBinding{}
		err := r.Get(ctx, types.NamespacedName{Name: spec.roleBindingName, Namespace: namespace}, latest)
		if err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("getting worker RoleBinding %s/%s while waiting for stale RoleRef deletion: %w", namespace, spec.roleBindingName, err)
		}

		if err == nil {
			if latest.RoleRef == desired.RoleRef {
				recreated = latest
				return true, nil
			}

			// The API server may still be serving the stale object while deletion is
			// propagating, or another actor may have recreated it with the stale
			// immutable RoleRef. Keep deleting/retrying until the name is available.
			if err := r.Delete(ctx, latest); err != nil && !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("deleting worker RoleBinding %s/%s with stale RoleRef %s/%s during retry: %w", namespace, spec.roleBindingName, latest.RoleRef.Kind, latest.RoleRef.Name, err)
			}
			return false, nil
		}

		create := desired.DeepCopy()
		if err := r.Create(ctx, create); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return false, nil
			}
			return false, fmt.Errorf("recreating worker RoleBinding %s/%s with RoleRef %s/%s: %w", namespace, spec.roleBindingName, desired.RoleRef.Kind, desired.RoleRef.Name, err)
		}

		recreated = create
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("recreating worker RoleBinding %s/%s after stale RoleRef %s/%s: %w", namespace, spec.roleBindingName, current.RoleRef.Kind, current.RoleRef.Name, err)
	}

	log.Info("Recreated worker RoleBinding", "namespace", namespace, "binding", spec.roleBindingName, "serviceAccount", spec.serviceAccountName, "clusterRole", spec.clusterRoleName)
	return recreated, nil
}

func workerRoleBinding(namespace string, spec workerRBACSpec) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.roleBindingName,
			Namespace: namespace,
			Labels: map[string]string{
				managedByLabelKey: managedByLabelValue,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     spec.clusterRoleName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      spec.serviceAccountName,
				Namespace: namespace,
			},
		},
	}
}

// subjectsEqual is intentionally order-sensitive; desired worker bindings
// currently contain exactly one subject.
func subjectsEqual(a, b []rbacv1.Subject) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// isAutonomousTask checks if this task has autonomous mode enabled via its agent.
func (r *TaskReconciler) isAutonomousTask(ctx context.Context, task *corev1alpha1.Task) bool {
	if task.Spec.AgentRef == nil {
		return false
	}

	agent := &corev1alpha1.Agent{}
	agentNS := task.Namespace
	if task.Spec.AgentRef.Namespace != "" {
		agentNS = task.Spec.AgentRef.Namespace
	}
	if err := r.Get(ctx, types.NamespacedName{Name: task.Spec.AgentRef.Name, Namespace: agentNS}, agent); err != nil {
		return false
	}

	return agent.Spec.Coordination != nil && agent.Spec.Coordination.Autonomous
}

func (r *TaskReconciler) pendingApprovalsForTask(ctx context.Context, task *corev1alpha1.Task) ([]approvals.Approval, error) {
	if r == nil || r.ExecutionEventStore == nil || task == nil {
		return nil, nil
	}
	listed, err := approvals.ListEvents(ctx, r.ExecutionEventStore, task.Namespace, task.Name)
	if err != nil {
		return nil, err
	}
	// Use zero time intentionally: v1 approval parking resolves only by
	// explicit terminal approval events. There is no expiry producer yet, so
	// passive expiresAt evaluation would silently resume consequential work.
	return approvals.Pending(approvals.FilterEventsForTaskUID(listed, string(task.UID)), time.Time{}), nil
}

func (r *TaskReconciler) parkOnPendingApproval(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, bool, error) {
	log := logf.FromContext(ctx)
	pending, err := r.pendingApprovalsForTask(ctx, task)
	if err != nil {
		log.Error(err, "failed to derive pending approvals")
		return ctrl.Result{}, false, err
	}
	if len(pending) == 0 {
		return ctrl.Result{}, false, nil
	}
	approval := pending[0]
	target := approval.TargetTool
	if target == "" {
		target = approval.Action
	}
	if target == "" {
		target = "requested action"
	}
	log.Info(
		"autonomous task waiting for approval",
		"approvalID", approval.ID,
		"targetTool", approval.TargetTool,
		"iteration", task.Status.Iteration,
	)
	waitingMessage := fmt.Sprintf(
		"waiting for approval %s for %s at iteration %d",
		approval.ID,
		target,
		task.Status.Iteration,
	)
	if task.Status.Message != waitingMessage {
		task.Status.Message = waitingMessage
		if err := r.Status().Update(ctx, task); err != nil {
			return ctrl.Result{}, false, err
		}
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, true, nil
}

func (r *TaskReconciler) handleAutonomousApprovalState(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, bool, error) {
	if !r.isAutonomousTask(ctx, task) {
		return ctrl.Result{}, false, nil
	}
	if result, parked, err := r.parkOnPendingApproval(ctx, task); err != nil || parked {
		return result, true, err
	}
	resumingAfterApproval, err := r.resumingAfterApprovalDecision(ctx, task)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if resumingAfterApproval {
		result, err := r.handleAutonomousIteration(ctx, task)
		return result, true, err
	}
	return ctrl.Result{}, false, nil
}

func parseAnnotationInt64(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

func (r *TaskReconciler) resumingAfterApprovalDecision(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	if task == nil {
		return false, nil
	}
	waitingStatus := strings.HasPrefix(task.Status.Message, "waiting for approval ")
	decisionNudge := task.Annotations != nil && task.Annotations[labels.AnnotationApprovalDecidedAt] != ""
	if decisionNudge && task.Annotations != nil {
		decisionSeq := parseAnnotationInt64(task.Annotations[labels.AnnotationApprovalDecisionSeq])
		resumedSeq := parseAnnotationInt64(task.Annotations[labels.AnnotationApprovalResumedSeq])
		decisionNudge = decisionSeq == 0 || decisionSeq > resumedSeq
	}
	if !waitingStatus && !decisionNudge {
		return false, nil
	}
	if r == nil || r.ExecutionEventStore == nil {
		return false, nil
	}
	listed, err := approvals.ListEvents(ctx, r.ExecutionEventStore, task.Namespace, task.Name)
	if err != nil {
		return false, err
	}
	resolved := approvals.Resolved(approvals.Derive(
		approvals.FilterEventsForTaskUID(listed, string(task.UID)),
		time.Time{},
	))
	return len(resolved) > 0, nil
}

func (r *TaskReconciler) clearApprovalDecisionNudge(ctx context.Context, task *corev1alpha1.Task) error {
	if r == nil || task == nil || task.Annotations == nil || task.Annotations[labels.AnnotationApprovalDecidedAt] == "" {
		return nil
	}
	var updated *corev1alpha1.Task
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &corev1alpha1.Task{}
		if err := r.Get(ctx, types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, current); err != nil {
			return err
		}
		if seq := strings.TrimSpace(current.Annotations[labels.AnnotationApprovalDecisionSeq]); seq != "" {
			current.Annotations[labels.AnnotationApprovalResumedSeq] = seq
		}
		delete(current.Annotations, labels.AnnotationApprovalDecidedAt)
		delete(current.Annotations, labels.AnnotationApprovalDecisionID)
		delete(current.Annotations, labels.AnnotationApprovalDecisionStatus)
		delete(current.Annotations, labels.AnnotationApprovalDecisionSeq)
		if err := r.Update(ctx, current); err != nil {
			return err
		}
		updated = current
		return nil
	}); err != nil {
		return err
	}
	if updated != nil {
		task.ResourceVersion = updated.ResourceVersion
		task.Annotations = updated.Annotations
	}
	return nil
}

// handleAutonomousIteration handles the completion of one autonomous loop iteration.
// It saves plan state, checks termination conditions, and creates a new Job if needed.
func (r *TaskReconciler) handleAutonomousIteration(ctx context.Context, task *corev1alpha1.Task) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("handling autonomous iteration", "iteration", task.Status.Iteration)

	// Collect result from this iteration (best-effort)
	if err := r.collectResult(ctx, task); err != nil {
		log.Error(err, "failed to collect iteration result")
	}

	if result, parked, err := r.parkOnPendingApproval(ctx, task); err != nil || parked {
		return result, err
	}

	resumingAfterApproval, err := r.resumingAfterApprovalDecision(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Check plan state for termination signals
	if r.PlanStore != nil {
		plan, err := r.PlanStore.GetPlan(ctx, task.Namespace, task.Name)
		if err == nil && plan.GoalComplete && !resumingAfterApproval {
			log.Info("autonomous task goal complete", "iteration", task.Status.Iteration, "summary", plan.Summary)
			return r.completeExecutedTask(ctx, task, corev1alpha1.TaskPhaseSucceeded,
				fmt.Sprintf("goal complete after %d iterations: %s", task.Status.Iteration+1, plan.Summary))
		}
	}

	// Check max iterations
	if task.Spec.AgentRef == nil {
		return r.failTask(ctx, task, "autonomous task requires agentRef")
	}
	agent := &corev1alpha1.Agent{}
	agentNS := task.Namespace
	if task.Spec.AgentRef.Namespace != "" {
		agentNS = task.Spec.AgentRef.Namespace
	}
	if err := r.Get(ctx, types.NamespacedName{Name: task.Spec.AgentRef.Name, Namespace: agentNS}, agent); err != nil {
		log.Error(err, "failed to get agent for autonomous check")
		return r.completeTask(ctx, task, corev1alpha1.TaskPhaseFailed, "failed to resolve agent for autonomous iteration")
	}

	maxIter := agent.Spec.Coordination.MaxIterations
	if maxIter > 0 && task.Status.Iteration+1 >= maxIter && !resumingAfterApproval {
		log.Info("autonomous task reached max iterations", "maxIterations", maxIter)
		return r.completeExecutedTask(ctx, task, corev1alpha1.TaskPhaseSucceeded,
			fmt.Sprintf("reached max iterations (%d)", maxIter))
	}

	// Check if suspended — keep task Running so it can resume when Suspend is unset
	if task.Spec.Suspend != nil && *task.Spec.Suspend {
		log.Info("autonomous task suspended, waiting for resume")
		task.Status.Message = fmt.Sprintf("autonomous task suspended at iteration %d", task.Status.Iteration)
		if err := r.Status().Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Enforce child task history limits
	if err := r.enforceHistoryLimits(ctx, task); err != nil {
		log.Error(err, "failed to enforce history limits for autonomous task")
	}

	// Delete old Job
	if task.Status.JobName != "" {
		job := &batchv1.Job{}
		err := r.Get(ctx, types.NamespacedName{
			Name:      task.Status.JobName,
			Namespace: task.Namespace,
		}, job)
		if err == nil {
			propagationPolicy := metav1.DeletePropagationBackground
			if err := r.Delete(ctx, job, &client.DeleteOptions{
				PropagationPolicy: &propagationPolicy,
			}); err != nil && !apierrors.IsNotFound(err) {
				log.Error(err, "failed to delete old Job for autonomous iteration")
			}
		}
	}

	// Increment iteration and reset to Pending for next Job creation
	task.Status.Iteration++
	task.Status.Phase = corev1alpha1.TaskPhasePending
	task.Status.JobName = ""
	task.Status.Message = fmt.Sprintf("autonomous iteration %d", task.Status.Iteration)

	if err := r.Status().Update(ctx, task); err != nil {
		log.Error(err, "failed to update status for autonomous iteration")
		return ctrl.Result{}, err
	}
	if resumingAfterApproval {
		if err := r.clearApprovalDecisionNudge(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
	}

	log.Info("autonomous task advancing to next iteration", "nextIteration", task.Status.Iteration)
	if r.Recorder != nil {
		r.Recorder.Event(task, corev1.EventTypeNormal, "AutonomousIteration",
			fmt.Sprintf("Starting iteration %d", task.Status.Iteration))
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}
