package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	"github.com/orka-agents/orka/internal/artifactcap"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	v2conformance "github.com/orka-agents/orka/internal/harness/v2/conformance"
	v2eventjournal "github.com/orka-agents/orka/internal/harness/v2/eventjournal"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
)

const (
	DefaultACPDispatchInterval                           = time.Second
	DefaultACPDispatchWorkers                            = 4
	DefaultACPIdlePoolTTL                                = 15 * time.Minute
	DefaultACPRuntimePoolReservationTTL                  = 2 * time.Minute
	DefaultACPRateLimitReconcileInterval                 = time.Second
	defaultACPTaskTimeout                                = 30 * time.Minute
	acpTaskTimeoutReason                                 = "TaskTimeout"
	acpTaskTimeoutCancellationSettledMessage             = "task deadline cancellation settled"
	acpCancelledOperation                                = "cancelled"
	acpSettlingOperation                                 = "settling"
	acpSucceededOperation                                = "succeeded"
	acpCredentialBlockedOperation                        = "credential-blocked"
	acpCredentialBlockedMessage                          = "workspace credential changed or became unavailable after queue; refusing to change frozen authority"
	acpExternalRuntimeDispatchUnsupportedExecutionReason = corev1alpha1.TaskExecutionReason("ExternalRuntimeDispatchUnsupported")
	acpCredentialBlockedExecutionReason                  = corev1alpha1.TaskExecutionReason("CredentialBlocked")
)

var (
	errACPRuntimePoolAtCapacity      = errors.New("RuntimePool is at capacity")
	errACPRuntimePoolNotAdmitting    = errors.New("RuntimePool is not admitting work")
	errACPRuntimePoolReservationLost = errors.New("RuntimePool capacity reservation is no longer held")
)

type acpReservedRetryStage string

const (
	acpReservedRetryRuntimeClient        acpReservedRetryStage = "runtime-client"
	acpReservedRetryCapabilities         acpReservedRetryStage = "capabilities"
	acpReservedRetrySessionConfiguration acpReservedRetryStage = "session-configuration"
	acpReservedRetryProfile              acpReservedRetryStage = "profile"
	acpReservedRetryMCPConfiguration     acpReservedRetryStage = "mcp-configuration"
	acpReservedRetryNamespaceLineage     acpReservedRetryStage = "namespace-lineage"
	acpReservedRetrySessionPreparation   acpReservedRetryStage = "session-preparation"
	acpReservedRetryReservationResize    acpReservedRetryStage = "reservation-resize"
)

// ACPDispatcher owns long-lived v2 prompt streams outside reconcile workers.
// Task reconciliation only persists queued demand; this leader-elected runnable
// reserves capacity and advances the durable attempt state machine.
type ACPDispatcher struct {
	Client                   client.Client
	APIReader                client.Reader
	Store                    store.DurableControlStore
	ResultStore              store.ResultStore
	EventStore               store.ExecutionEventStore
	PlanStore                store.PlanStore
	Snapshots                store.AgentExecutionSnapshotStore
	Epochs                   *ControllerEpochManager
	Sessions                 *ACPSessionContinuity
	Publisher                *publisherservice.Client
	ArtifactCapabilitySecret []byte
	ArtifactReservations     artifactcap.CapabilityReservationRecorder
	MCPRegistry              *tools.Registry
	Interval                 time.Duration
	MaxConcurrent            int
	IdlePoolTTL              time.Duration
	ReservationTTL           time.Duration
	RateLimitRetryInterval   time.Duration
	AdmissionGate            *ACPAdmissionGate
	ACPRuntimeImages         ACPRuntimeImages
	runtimeContextFactory    func(context.Context, *corev1alpha1.Task) (context.Context, context.CancelFunc)

	// SubstrateRouterURL and SubstrateActorDNSSuffix route Substrate-backed
	// RuntimePool instances through the provider router while preserving the
	// exact actor route host. Empty values fail closed for substrate pools.
	SubstrateRouterURL      string
	SubstrateActorDNSSuffix string

	mu              sync.Mutex
	active          map[types.UID]struct{}
	sem             chan struct{}
	runtimeSessions map[string]ACPRuntimeSessionBinding
	finalizedTurns  map[types.UID]string

	substrateRouteOnce  sync.Once
	substrateRouteHTTP  *http.Client
	substrateRouteSetup error
}

// substrateRouteHTTPClient lazily builds the router-pinned transport for
// Substrate-backed RuntimePool instances.
func (d *ACPDispatcher) substrateRouteHTTPClient() (*http.Client, error) {
	d.substrateRouteOnce.Do(func() {
		transport, err := substrateRouteHTTPTransport(d.SubstrateRouterURL, d.SubstrateActorDNSSuffix)
		if err != nil {
			d.substrateRouteSetup = fmt.Errorf("substrate route transport is not configured: %w", err)
			return
		}
		d.substrateRouteHTTP = &http.Client{Transport: transport}
	})
	return d.substrateRouteHTTP, d.substrateRouteSetup
}

func (d *ACPDispatcher) NeedLeaderElection() bool { return true }

func (d *ACPDispatcher) Start(ctx context.Context) error {
	if d.Client == nil || d.Store == nil || d.ResultStore == nil || d.EventStore == nil || d.PlanStore == nil || d.Snapshots == nil || d.Epochs == nil {
		return fmt.Errorf("ACP dispatcher requires Kubernetes client, durable control store, result store, execution event store, plan store, immutable snapshot store, and epoch manager")
	}
	if _, ok := d.EventStore.(store.DeduplicatingExecutionEventStore); !ok {
		return fmt.Errorf("ACP dispatcher requires an execution event store with atomic deduplication")
	}
	if _, ok := d.EventStore.(store.AtomicExecutionEventPlanStore); !ok {
		return fmt.Errorf("ACP dispatcher requires an execution event store with atomic plan projection")
	}
	if _, ok := d.ResultStore.(store.PromptResultReceiptStore); !ok {
		return fmt.Errorf("ACP dispatcher requires a result store with prompt result receipts")
	}
	if d.APIReader == nil {
		d.APIReader = d.Client
	}
	if d.Interval <= 0 {
		d.Interval = DefaultACPDispatchInterval
	}
	if d.MaxConcurrent <= 0 {
		d.MaxConcurrent = DefaultACPDispatchWorkers
	}
	if d.IdlePoolTTL <= 0 {
		d.IdlePoolTTL = DefaultACPIdlePoolTTL
	}
	if d.ReservationTTL <= 0 {
		d.ReservationTTL = DefaultACPRuntimePoolReservationTTL
	}
	if d.RateLimitRetryInterval <= 0 {
		d.RateLimitRetryInterval = DefaultACPRateLimitReconcileInterval
	}
	d.mu.Lock()
	if d.active == nil {
		d.active = make(map[types.UID]struct{})
	}
	if d.sem == nil {
		d.sem = make(chan struct{}, d.MaxConcurrent)
	}
	if d.runtimeSessions == nil {
		d.runtimeSessions = make(map[string]ACPRuntimeSessionBinding)
	}
	d.mu.Unlock()
	if _, err := d.Epochs.CurrentFence(ctx); err != nil {
		return err
	}
	if err := d.recoverStaleAttempts(ctx); err != nil {
		return fmt.Errorf("recover stale ACP attempts: %w", err)
	}
	ticker := time.NewTicker(d.Interval)
	defer ticker.Stop()
	for {
		if err := d.dispatchOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logf.FromContext(ctx).Error(err, "ACP dispatcher scan failed")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (d *ACPDispatcher) dispatchOnce(ctx context.Context) error {
	if err := d.reconcileExpiredExternalEffects(ctx); err != nil {
		return err
	}
	var tasks corev1alpha1.TaskList
	if err := d.Client.List(ctx, &tasks); err != nil {
		return err
	}
	d.pruneFinalizedSessionTurns(tasks.Items)
	if err := d.scheduleACPDeliveryRecoveries(ctx, tasks.Items); err != nil {
		return err
	}
	if err := d.rejectPersistedExternalRuntimeDispatches(ctx, tasks.Items); err != nil {
		return err
	}
	queued := make([]*corev1alpha1.Task, 0)
	for i := range tasks.Items {
		task := &tasks.Items[i]
		if !taskDispatchableByACP(task) || task.Status.Execution == nil ||
			(task.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued && task.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved) ||
			strings.TrimSpace(task.Status.Execution.AgentRuntimeName) != "" || strings.TrimSpace(task.Status.Execution.RuntimePoolName) == "" {
			continue
		}
		queued = append(queued, task.DeepCopy())
	}
	sortACPTasksByQueuePriority(queued, time.Now().UTC())
	admissible := queued[:0]
	for _, queuedTask := range queued {
		keep, err := d.settleQueuedTaskBeforeAdmission(ctx, queuedTask)
		if err != nil {
			return fmt.Errorf("settle ACP task before admission %s/%s: %w", queuedTask.Namespace, queuedTask.Name, err)
		}
		if keep {
			admissible = append(admissible, queuedTask)
		}
	}
	queued = admissible
	if err := d.AdmissionGate.Check(); err != nil {
		return nil
	}
dispatchLoop:
	for _, queuedTask := range queued {
		select {
		case d.sem <- struct{}{}:
			if !d.markActive(queuedTask.UID) {
				<-d.sem
				continue
			}
			task, target, reserveErr := d.reserveTask(ctx, queuedTask)
			if reserveErr != nil || task == nil {
				<-d.sem
				d.unmarkActive(queuedTask.UID)
				if reserveErr != nil && !errors.Is(reserveErr, context.Canceled) {
					logf.FromContext(ctx).Error(reserveErr, "ACP task reservation failed", "namespace", queuedTask.Namespace, "task", queuedTask.Name)
				}
				continue
			}
			go func(task *corev1alpha1.Task, target acpDispatchTarget) {
				defer func() {
					<-d.sem
					d.unmarkActive(task.UID)
				}()
				if err := d.executeReservedTask(ctx, task, target); err != nil && !errors.Is(err, context.Canceled) {
					logf.FromContext(ctx).Error(err, "ACP task execution failed", "namespace", task.Namespace, "task", task.Name)
				}
			}(task, target)
		default:
			break dispatchLoop
		}
	}
	return d.reapIdlePools(ctx, tasks.Items)
}

func (d *ACPDispatcher) rejectPersistedExternalRuntimeDispatches(ctx context.Context, tasks []corev1alpha1.Task) error {
	for i := range tasks {
		task := &tasks[i]
		if !persistedExternalRuntimeDispatch(task) {
			continue
		}
		if err := d.rejectPersistedExternalRuntimeDispatch(ctx, task.DeepCopy()); err != nil {
			return fmt.Errorf("reject persisted external runtime dispatch for Task %s/%s: %w", task.Namespace, task.Name, err)
		}
	}
	return nil
}

func persistedExternalRuntimeDispatch(task *corev1alpha1.Task) bool {
	if !taskDispatchableByACP(task) || task.Status.Execution == nil ||
		strings.TrimSpace(task.Status.Execution.AgentRuntimeName) == "" {
		return false
	}
	return task.Status.Execution.State == corev1alpha1.TaskExecutionStateQueued ||
		task.Status.Execution.State == corev1alpha1.TaskExecutionStateReserved
}

func (d *ACPDispatcher) rejectPersistedExternalRuntimeDispatch(ctx context.Context, task *corev1alpha1.Task) error {
	runtimeName := strings.TrimSpace(task.Status.Execution.AgentRuntimeName)
	message := externalAgentRuntimeDispatchUnsupportedReason(runtimeName)
	if err := d.failPersistedExternalRuntimeAttempt(ctx, task, message); err != nil {
		return err
	}
	return d.failTask(
		ctx,
		task,
		corev1alpha1.TaskExecutionStateFailed,
		corev1alpha1.TaskExecutionOutcomeFailed,
		acpExternalRuntimeDispatchUnsupportedExecutionReason,
		message,
	)
}

func (d *ACPDispatcher) failPersistedExternalRuntimeAttempt(ctx context.Context, task *corev1alpha1.Task, message string) error {
	attemptID, err := promptAttemptIDFromTask(task)
	if err != nil {
		return nil
	}
	fence, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return err
	}
	for range 3 {
		attempt, getErr := d.Store.GetPromptAttempt(ctx, attemptID)
		if errors.Is(getErr, store.ErrNotFound) {
			return nil
		}
		if getErr != nil {
			return getErr
		}
		if store.IsTerminalPromptExecutionState(attempt.ExecutionState) {
			return nil
		}
		if transitionErr := store.ValidatePromptExecutionTransition(attempt.ExecutionState, store.PromptExecutionFailed); transitionErr != nil {
			return nil
		}
		digest, digestErr := acpDomainDigest("external-runtime-dispatch-rejection", struct {
			AttemptID string                     `json:"attemptID"`
			From      store.PromptExecutionState `json:"from"`
			Version   int64                      `json:"version"`
			Message   string                     `json:"message"`
		}{
			AttemptID: attempt.ID, From: attempt.ExecutionState, Version: attempt.Version, Message: message,
		})
		if digestErr != nil {
			return digestErr
		}
		_, transitionErr := d.Store.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState:    store.PromptExecutionFailed,
			OperationID: "reject-external-runtime-dispatch-" + strconv.FormatInt(attempt.Version, 10), OperationDigest: digest,
			TerminalReason: string(acpExternalRuntimeDispatchUnsupportedExecutionReason), UpdatedAt: time.Now().UTC(),
		})
		if errors.Is(transitionErr, store.ErrConflict) {
			continue
		}
		return transitionErr
	}
	return fmt.Errorf("prompt attempt %s changed while rejecting external runtime dispatch", attemptID)
}

//nolint:gocyclo // Terminal projection and cleanup recovery branches are audited together.
func (d *ACPDispatcher) scheduleACPDeliveryRecoveries(ctx context.Context, tasks []corev1alpha1.Task) error {
	var fence store.ControllerEpochFence
	haveFence := false
	for i := range tasks {
		task := &tasks[i]
		if !taskDispatchableByACP(task) || task.Status.Execution == nil ||
			task.Status.Execution.Attempt < 1 || strings.TrimSpace(task.Status.Execution.PromptID) == "" {
			continue
		}
		attemptID, idErr := promptAttemptIDFromTask(task)
		if idErr != nil {
			return idErr
		}
		attempt, getErr := d.Store.GetPromptAttempt(ctx, attemptID)
		if getErr != nil {
			if errors.Is(getErr, store.ErrNotFound) {
				continue
			}
			return getErr
		}
		recoveryKind := ""
		switch attempt.ExecutionState {
		case store.PromptExecutionSucceeded:
			if task.Status.Execution.State != corev1alpha1.TaskExecutionStateSucceeded ||
				!store.IsTerminalPromptDeliveryState(attempt.DeliveryState) || task.Status.Delivery == nil {
				recoveryKind = acpSucceededOperation
			}
		case store.PromptExecutionFailed, store.PromptExecutionCancelled, store.PromptExecutionOutcomeUnknown:
			if corev1alpha1.TaskExecutionState(attempt.ExecutionState) != task.Status.Execution.State || task.Status.Execution.Outcome == "" {
				recoveryKind = "terminal"
			}
		}
		if recoveryKind == "" {
			needsTurnRecovery, turnErr := d.sessionTurnRequiresTerminalRecovery(ctx, task, attempt)
			if turnErr != nil {
				return turnErr
			}
			if needsTurnRecovery {
				recoveryKind = "terminal"
			}
		}
		cleanupPending := !taskScopedRuntimeSessionCleanupComplete(task)
		if recoveryKind == "" && store.IsTerminalPromptExecutionState(attempt.ExecutionState) && store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
			if !haveFence {
				var fenceErr error
				fence, fenceErr = d.Epochs.CurrentFence(ctx)
				if fenceErr != nil {
					return fenceErr
				}
				haveFence = true
			}
			if cleanupPending || task.Status.Execution.ControllerEpoch < fence.Epoch {
				recoveryKind = "stale-terminal"
			}
		}
		if recoveryKind == "" {
			continue
		}
		if !haveFence {
			var fenceErr error
			fence, fenceErr = d.Epochs.CurrentFence(ctx)
			if fenceErr != nil {
				return fenceErr
			}
			haveFence = true
		}
		select {
		case d.sem <- struct{}{}:
			if !d.markActive(task.UID) {
				<-d.sem
				continue
			}
			go func(task *corev1alpha1.Task, attempt *store.PromptAttempt, kind string) {
				defer func() {
					<-d.sem
					d.unmarkActive(task.UID)
				}()
				var err error
				switch kind {
				case "stale-terminal":
					err = d.recoverStaleTask(ctx, task, fence)
				case acpSucceededOperation:
					err = d.recoverSucceededTaskProjection(ctx, task, attempt, fence)
				default:
					err = d.finalizeRecoveredTerminalSession(ctx, task, attempt, fence)
					if err == nil {
						err = d.patchRecoveredTerminalExecution(ctx, task, attempt, fence.Epoch)
					}
				}
				if err != nil && !errors.Is(err, context.Canceled) {
					logf.FromContext(ctx).Error(err, "ACP authoritative attempt projection recovery failed", "namespace", task.Namespace, "task", task.Name)
				}
			}(task.DeepCopy(), attempt, recoveryKind)
		default:
			return nil
		}
	}
	return nil
}

//nolint:gocyclo // Demand-state accounting and all fail-closed pool capacity gates are intentionally audited together.
func (d *ACPDispatcher) reapIdlePools(ctx context.Context, tasks []corev1alpha1.Task) error {
	fence, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return err
	}
	activeByPool := make(map[string]int)
	queuedByPool := make(map[string]int32)
	finalizingByPool := make(map[string]int32)
	settledAtByPool := make(map[string]time.Time)
	now := time.Now().UTC()
	for i := range tasks {
		task := &tasks[i]
		poolName := strings.TrimSpace(task.Labels[acpRuntimeTaskPoolLabel])
		if poolName == "" || task.Status.Execution == nil {
			continue
		}
		key := task.Namespace + "/" + poolName
		if taskExecutionStateTerminal(task.Status.Execution.State) {
			if task.Status.Execution.State == corev1alpha1.TaskExecutionStateSucceeded && task.Status.Delivery != nil &&
				!store.IsTerminalPromptDeliveryState(store.PromptDeliveryState(task.Status.Delivery.State)) {
				activeByPool[key]++
				finalizingByPool[key]++
				continue
			}
			settledAt := acpTaskDemandSettledAt(task)
			if !settledAt.IsZero() && settledAt.After(settledAtByPool[key]) {
				settledAtByPool[key] = settledAt
			}
			continue
		}
		activeByPool[key]++
		switch task.Status.Execution.State {
		case corev1alpha1.TaskExecutionStateQueued, corev1alpha1.TaskExecutionStateReserved:
			queuedByPool[key]++
		case corev1alpha1.TaskExecutionStateSettling:
			finalizingByPool[key]++
		}
	}
	var pools corev1alpha1.RuntimePoolList
	if err := d.Client.List(ctx, &pools); err != nil {
		return err
	}
	for i := range pools.Items {
		pool := &pools.Items[i]
		key := pool.Namespace + "/" + pool.Name
		latest, err := d.patchPoolCoordinatorCapacity(ctx, pool, fence.Epoch, now, queuedByPool[key], finalizingByPool[key])
		if err != nil {
			return err
		}
		if latest == nil {
			continue
		}
		pool = latest
		if pool.Spec.DesiredReplicas == 0 {
			if err := d.reapStoppedSupersededPlainPool(ctx, pool, activeByPool[key], now); err != nil {
				return err
			}
			if err := d.reapStoppedWorkspacePool(ctx, pool, activeByPool[key], now); err != nil {
				return err
			}
			continue
		}
		if runtimePoolHasActiveDemand(pool, activeByPool[key]) {
			continue
		}
		if settledAt := settledAtByPool[key]; !settledAt.IsZero() {
			pool, err = d.recordPoolLastDemandAt(ctx, pool, settledAt)
			if err != nil {
				return err
			}
			if runtimePoolHasActiveDemand(pool, activeByPool[key]) {
				continue
			}
		}
		lastDemand, err := time.Parse(time.RFC3339Nano, pool.Annotations[acpRuntimeLastDemandAnnotation])
		if err != nil {
			continue
		}
		idleTTL, workspaceAttached, err := d.runtimePoolIdlePolicy(ctx, pool)
		if err != nil {
			return err
		}
		if workspaceAttached || now.Sub(lastDemand) < idleTTL {
			continue
		}
		if hold, err := d.workspaceResumeTransitionPending(ctx, pool); err != nil {
			return err
		} else if hold {
			// The adapter lifted the suspension and raised replicas for a cold
			// resume, but the continuation has not attached or registered Task
			// demand yet; scaling back to zero now would recycle the sole
			// resumed checkpoint mid-transition.
			continue
		}
		base := pool.DeepCopy()
		pool.Spec.DesiredReplicas = 0
		patch := client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
		if err := d.Client.Patch(ctx, pool, patch); err != nil && !apierrors.IsConflict(err) {
			return err
		}
	}
	return nil
}

func (d *ACPDispatcher) reapStoppedSupersededPlainPool(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	activeTasks int,
	now time.Time,
) error {
	if !acpRuntimePoolImageSuperseded(pool, d.ACPRuntimeImages) || activeTasks > 0 ||
		pool.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopped ||
		pool.Status.ObservedGeneration != pool.Generation || pool.Status.ActiveInstance != nil ||
		pool.Status.Capacity.QueuedTasks > 0 || pool.Status.Capacity.FinalizingSessions > 0 ||
		len(pool.Status.Capacity.Reservations) > 0 {
		return nil
	}
	lastDemand, err := time.Parse(time.RFC3339Nano, pool.Annotations[acpRuntimeLastDemandAnnotation])
	if err != nil || now.Sub(lastDemand) < 2*d.IdlePoolTTL {
		return nil
	}
	if err := d.Client.Delete(ctx, pool, deleteCurrentObjectPreconditions(pool)...); err != nil &&
		!apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return err
	}
	return nil
}

// workspaceResumeTransitionPending reports a workspace-backed pool whose
// linked workspace is mid cold-resume: the Ready flip (or the adapter's
// replica raise) happened, but no attachment or durable Task demand exists
// yet, so the pool must stay exempt from ordinary idle scale-down.
func (d *ACPDispatcher) workspaceResumeTransitionPending(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
) (bool, error) {
	if pool.Spec.ExecutionWorkspace == nil {
		return false, nil
	}
	name := strings.TrimSpace(pool.Labels[acpExecutionWorkspaceLinkLabel])
	if name == "" {
		return false, nil
	}
	reader := d.APIReader
	if reader == nil {
		reader = d.Client
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: pool.Namespace, Name: name}, workspace); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if pool.Annotations[acpExecutionWorkspaceUIDAnnotation] != string(workspace.UID) {
		return false, nil
	}
	if workspace.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
		return false, nil
	}
	if workspace.Annotations[acpWorkspaceDetachActionAnnotation] == string(workspacev1alpha1.WorkspaceOnDetachSuspend) {
		// The frozen Suspend action means settlement will checkpoint this
		// workspace, but DesiredState has not flipped yet (settlement can be
		// delayed past IdlePoolTTL). Ordinary scale-down in that window
		// would delete the actor before the requested checkpoint exists.
		return true, nil
	}
	// The hold applies regardless of attachment presence: attachment is
	// persisted BEFORE queueACPRuntimeTask writes the pool label and
	// execution status, and in that gap neither the attachment nor durable
	// Task demand shields the just-restored pool from the idle reaper - a
	// scale-to-zero there would recycle the sole restored checkpoint.
	return workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateSuspended ||
		workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateSuspending ||
		workspace.Annotations[acpWorkspaceResumedLineageAnnotation] == booleanTrueValue, nil
}

// runtimePoolIdlePolicy returns the retirement threshold for a warm pool and
// whether its reciprocally linked class workspace has an active attachment.
// Other pools retain the controller-wide default and have no attachment fence.
func (d *ACPDispatcher) runtimePoolIdlePolicy(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
) (time.Duration, bool, error) {
	if pool == nil || pool.Spec.ExecutionWorkspace == nil {
		return d.IdlePoolTTL, false, nil
	}
	workspaceName := strings.TrimSpace(pool.Labels[acpExecutionWorkspaceLinkLabel])
	if workspaceName == "" {
		return d.IdlePoolTTL, false, nil
	}
	reader := d.APIReader
	if reader == nil {
		reader = d.Client
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: pool.Namespace, Name: workspaceName}, workspace); err != nil {
		if apierrors.IsNotFound(err) {
			return d.IdlePoolTTL, false, nil
		}
		return 0, false, err
	}
	if workspace.Annotations[acpExecutionWorkspacePoolAnnotation] != pool.Name ||
		pool.Annotations[acpExecutionWorkspaceUIDAnnotation] != string(workspace.UID) {
		return d.IdlePoolTTL, false, nil
	}
	attached := workspace.Spec.Attachment != nil
	idleTimeout := workspace.Spec.Lifecycle.IdleTimeout
	if idleTimeout == nil {
		return d.IdlePoolTTL, attached, nil
	}
	if idleTimeout.Duration <= 0 {
		return 0, false, fmt.Errorf("workspace %s/%s has a non-positive frozen idle timeout", workspace.Namespace, workspace.Name)
	}
	return idleTimeout.Duration, attached, nil
}

// reapStoppedWorkspacePool retires a scaled-to-zero workspace-backed pool
// object after it has proven Stopped (drained, provider workspace deleted) and
// stayed idle for another TTL. Recovery treats a missing pool as proof of
// RuntimeSession cleanup, and fresh demand deterministically recreates the pool
// by name. Plain pools are never deleted here; they are shared infrastructure.
func (d *ACPDispatcher) reapStoppedWorkspacePool(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	activeTasks int,
	now time.Time,
) error {
	if pool == nil || pool.Spec.ExecutionWorkspace == nil || activeTasks > 0 ||
		pool.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopped ||
		pool.Status.ObservedGeneration != pool.Generation ||
		pool.Status.Capacity.QueuedTasks > 0 || pool.Status.Capacity.FinalizingSessions > 0 ||
		len(pool.Status.Capacity.Reservations) > 0 {
		return nil
	}
	lastDemand, err := time.Parse(time.RFC3339Nano, pool.Annotations[acpRuntimeLastDemandAnnotation])
	if err != nil || now.Sub(lastDemand) < 2*d.IdlePoolTTL {
		return nil
	}
	// A class-backed pool is torn down through its controller-first
	// ExecutionWorkspace so the workspace lifecycle records the terminal
	// disposition; the ACP workspace adapter then deletes this pool. Only a
	// pool with no linked workspace left is deleted directly.
	if workspaceName := strings.TrimSpace(pool.Labels[acpExecutionWorkspaceLinkLabel]); workspaceName != "" {
		workspace := &workspacev1alpha1.ExecutionWorkspace{}
		reader := d.APIReader
		if reader == nil {
			reader = d.Client
		}
		getErr := reader.Get(ctx, client.ObjectKey{Namespace: pool.Namespace, Name: workspaceName}, workspace)
		if getErr == nil {
			if workspace.Annotations[acpExecutionWorkspacePoolAnnotation] != pool.Name {
				return nil
			}
			if pool.Annotations[acpExecutionWorkspaceUIDAnnotation] != string(workspace.UID) {
				// The name link is reciprocal but the pool is pinned to a
				// DIFFERENT workspace incarnation (a Session recreated under
				// the same name); a stale pool must never delete the new
				// incarnation's workspace.
				return nil
			}
			if workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredSuspended &&
				workspace.Status.State != workspacev1alpha1.ExecutionWorkspaceStateFailed {
				// A suspended (or still-suspending) workspace is deliberately
				// retained for cold resume; bounded retention and expiry
				// enforcement are the retention machinery's responsibility,
				// not the idle reaper's. A suspension that settled Failed
				// preserved no checkpoint, so nothing warrants retaining the
				// stopped pool, template, and Secrets forever.
				return nil
			}
			if workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredReady &&
				(workspace.Annotations[acpWorkspaceDetachActionAnnotation] == string(workspacev1alpha1.WorkspaceOnDetachSuspend) ||
					workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateSuspended ||
					workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateSuspending ||
					(workspace.Annotations[acpWorkspaceResumedLineageAnnotation] == booleanTrueValue &&
						workspace.Spec.Attachment == nil)) {
				// A continuation flipped the workspace to Ready for cold
				// resume but has not attached or registered pool demand yet
				// (the adapter may already have raised DesiredReplicas and
				// begun restoring the actor); deleting or scaling down here
				// would destroy the sole resumed checkpoint mid-transition.
				return nil
			}
			if idleTimeout := workspace.Spec.Lifecycle.IdleTimeout; idleTimeout != nil &&
				(idleTimeout.Duration <= 0 || now.Sub(lastDemand) < idleTimeout.Duration) {
				// The frozen class timeout is the earliest allowed idle
				// transition. The global pool reaper may run later, but it
				// must never delete the workspace before class policy allows.
				return nil
			}
			if workspace.Spec.Attachment != nil {
				// An attachment can exist before its Task acquires the pool
				// label (a crash between attachment and pool demand), so a
				// zero active count is not proof of idleness; the attached
				// Task's own settlement owns this workspace's retirement.
				return nil
			}
			if workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined ||
				workspace.Labels[workspacev1alpha1.QuarantinedLabel] == booleanTrueValue {
				// Detach-timeout settlement deliberately preserved this
				// workspace as fail-closed evidence; idleness must never
				// destroy what quarantine explicitly retained. Pool
				// teardown stays with quarantine settlement itself.
				return nil
			}
			if workspace.DeletionTimestamp.IsZero() {
				// UID+resourceVersion preconditions: a Task attaching between
				// the idle check and this delete bumps the resource version,
				// so the race settles as a retried conflict instead of
				// deleting a newly attached workspace.
				if deleteErr := d.Client.Delete(ctx, workspace, deleteCurrentObjectPreconditions(workspace)...); deleteErr != nil &&
					!apierrors.IsNotFound(deleteErr) && !apierrors.IsConflict(deleteErr) {
					return deleteErr
				}
			}
			return nil
		}
		if !apierrors.IsNotFound(getErr) {
			return getErr
		}
	}
	if err := d.Client.Delete(ctx, pool, deleteCurrentObjectPreconditions(pool)...); err != nil &&
		!apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return err
	}
	return nil
}

func runtimePoolHasActiveDemand(pool *corev1alpha1.RuntimePool, activeTasks int) bool {
	if pool == nil || pool.Spec.DesiredReplicas == 0 || activeTasks > 0 {
		return true
	}
	// Workspace-backed pools are single-session physical workspaces. Once no
	// Task, prompt, permission, reservation, or finalization work remains, their
	// authenticated scale-down drain retires an idle reusable RuntimeSession.
	// Plain shared pools must stay resident while any RuntimeSession remains.
	residentSessionsBlockScaleDown := pool.Spec.ExecutionWorkspace == nil && pool.Status.Capacity.ResidentSessions > 0
	return residentSessionsBlockScaleDown || pool.Status.Capacity.RunningPrompts > 0 ||
		pool.Status.Capacity.ReservedSessions > 0 || pool.Status.Capacity.ReservedPrompts > 0 ||
		pool.Status.Capacity.FinalizingSessions > 0 || pool.Status.Capacity.PendingPermissions > 0
}

func acpTaskDemandSettledAt(task *corev1alpha1.Task) time.Time {
	if task == nil || task.Status.Execution == nil {
		return time.Time{}
	}
	settledAt := time.Time{}
	if task.Status.Execution.LastTransitionTime != nil && !task.Status.Execution.LastTransitionTime.IsZero() {
		settledAt = task.Status.Execution.LastTransitionTime.UTC()
	}
	if task.Status.Delivery != nil && store.IsTerminalPromptDeliveryState(store.PromptDeliveryState(task.Status.Delivery.State)) &&
		task.Status.Delivery.LastTransitionTime != nil && !task.Status.Delivery.LastTransitionTime.IsZero() {
		deliverySettledAt := task.Status.Delivery.LastTransitionTime.UTC()
		if deliverySettledAt.After(settledAt) {
			settledAt = deliverySettledAt
		}
	}
	if settledAt.IsZero() && task.Status.CompletionTime != nil && !task.Status.CompletionTime.IsZero() {
		settledAt = task.Status.CompletionTime.UTC()
	}
	if settledAt.IsZero() && !task.CreationTimestamp.IsZero() {
		settledAt = task.CreationTimestamp.UTC()
	}
	return settledAt
}

func (d *ACPDispatcher) recordPoolLastDemandAt(ctx context.Context, pool *corev1alpha1.RuntimePool, settledAt time.Time) (*corev1alpha1.RuntimePool, error) {
	key := client.ObjectKeyFromObject(pool)
	var result *corev1alpha1.RuntimePool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.RuntimePool{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return err
		}
		current, err := time.Parse(time.RFC3339Nano, latest.Annotations[acpRuntimeLastDemandAnnotation])
		if err == nil && !settledAt.After(current) {
			result = latest.DeepCopy()
			return nil
		}
		base := latest.DeepCopy()
		if latest.Annotations == nil {
			latest.Annotations = make(map[string]string)
		}
		latest.Annotations[acpRuntimeLastDemandAnnotation] = settledAt.UTC().Format(time.RFC3339Nano)
		patch := client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
		if err := d.Client.Patch(ctx, latest, patch); err != nil {
			return err
		}
		result = latest.DeepCopy()
		return nil
	})
	return result, err
}

func (d *ACPDispatcher) patchPoolCoordinatorCapacity(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	controllerEpoch int64,
	now time.Time,
	queued, finalizing int32,
) (*corev1alpha1.RuntimePool, error) {
	key := types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}
	var result *corev1alpha1.RuntimePool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.RuntimePool{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			if apierrors.IsNotFound(err) {
				result = nil
				return nil
			}
			return err
		}
		reservations, changed := activeRuntimePoolReservations(latest, controllerEpoch, now)
		latest.Status.Capacity.Reservations = reservations
		reservedSessionsBefore := latest.Status.Capacity.ReservedSessions
		reservedPromptsBefore := latest.Status.Capacity.ReservedPrompts
		updateRuntimePoolReservationCounters(&latest.Status.Capacity)
		changed = changed || reservedSessionsBefore != latest.Status.Capacity.ReservedSessions || reservedPromptsBefore != latest.Status.Capacity.ReservedPrompts
		if latest.Status.Capacity.QueuedTasks != queued {
			latest.Status.Capacity.QueuedTasks = queued
			changed = true
		}
		if latest.Status.Capacity.FinalizingSessions != finalizing {
			latest.Status.Capacity.FinalizingSessions = finalizing
			changed = true
		}
		if changed {
			if err := d.Client.Status().Update(ctx, latest); err != nil {
				return err
			}
		}
		result = latest.DeepCopy()
		return nil
	})
	return result, err
}

func (d *ACPDispatcher) watchTaskCancellation(ctx context.Context, cancel context.CancelFunc, key types.NamespacedName) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			task := &corev1alpha1.Task{}
			if err := d.APIReader.Get(ctx, key, task); err != nil {
				if apierrors.IsNotFound(err) {
					cancel()
				}
				continue
			}
			if !task.DeletionTimestamp.IsZero() || task.Status.Phase == corev1alpha1.TaskPhaseCancelled {
				cancelCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				_, _ = d.cancelPreparedPublication(cancelCtx, task)
				stop()
				cancel()
				return
			}
		}
	}
}

func (d *ACPDispatcher) markActive(uid types.UID) bool {
	if uid == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.active[uid]; ok {
		return false
	}
	d.active[uid] = struct{}{}
	return true
}

func (d *ACPDispatcher) unmarkActive(uid types.UID) {
	d.mu.Lock()
	delete(d.active, uid)
	d.mu.Unlock()
}

func (d *ACPDispatcher) isActive(uid types.UID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.active[uid]
	return ok
}

// loadVerifiedACPDispatchExecution re-establishes the immutable dispatch
// authority after RuntimePool reservation and immediately before the first
// supervisor RPC. Agent and Tool resources are deliberately not read here:
// every executable value comes from the encrypted, content-addressed snapshot.
func (d *ACPDispatcher) loadVerifiedACPDispatchExecution(
	ctx context.Context,
	task *corev1alpha1.Task,
) (*verifiedAgentExecution, error) {
	if d.Snapshots == nil {
		return nil, errors.New("ACP dispatch requires an immutable execution snapshot store")
	}
	if task == nil || task.Status.AgentExecutionBinding == nil || task.Status.Execution == nil {
		return nil, errors.New("ACP dispatch requires a Task execution binding and queued attempt")
	}
	verifier := TaskReconciler{
		Client:                  d.Client,
		APIReader:               d.APIReader,
		AgentExecutionSnapshots: d.Snapshots,
	}
	bound, err := verifier.loadVerifiedBoundExecution(ctx, task, task.Status.AgentExecutionBinding)
	if err != nil {
		return nil, fmt.Errorf("verify immutable ACP execution: %w", err)
	}
	matches, err := acpQueuedTaskRequestMatchesBinding(bound, task.Status.Execution)
	if err != nil {
		return nil, fmt.Errorf("verify frozen ACP request identity: %w", err)
	}
	if !matches {
		return nil, errors.New("queued ACP request digest does not match the immutable execution binding and snapshot")
	}
	attemptID, err := promptAttemptIDFromTask(task)
	if err != nil {
		return nil, err
	}
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return nil, fmt.Errorf("load frozen ACP PromptAttempt: %w", err)
	}
	if !queuedPromptAttemptMatchesTask(attempt, task) || attempt.BindingDigest != bound.binding.BindingDigest ||
		attempt.SnapshotDigest != bound.snapshot.Digest || attempt.ExecutionState != store.PromptExecutionReserved {
		return nil, errors.New("reserved ACP PromptAttempt does not exactly match the immutable Task binding, snapshot, and request")
	}
	bound.promptAttempt = attempt
	bound.frozenTask.Status = *task.Status.DeepCopy()
	return bound, nil
}

func validateFrozenACPDispatchTarget(
	task *corev1alpha1.Task,
	target acpDispatchTarget,
	bound *verifiedAgentExecution,
	deliveryPlan ACPRuntimePlan,
) error {
	if task == nil || task.Status.Execution == nil || target.pool == nil || bound == nil || bound.binding == nil {
		return errors.New("frozen ACP Task, RuntimePool target, and execution binding are required")
	}
	if target.pool.Name != deliveryPlan.PoolName || target.pool.Spec.Runtime.Image != deliveryPlan.Image ||
		target.pool.Spec.Runtime.Profile.Digest != bound.body.ProfileDigest ||
		task.Status.Execution.RuntimePoolName != target.pool.Name ||
		task.Status.Execution.RuntimePoolUID != string(target.pool.UID) {
		return errors.New("reserved RuntimePool does not exactly match the immutable execution snapshot")
	}
	if !acpRuntimePoolWorkspaceMatchesPlan(target.pool, deliveryPlan) {
		return errors.New("reserved RuntimePool execution workspace binding does not exactly match the immutable execution snapshot")
	}
	return nil
}

func validateFrozenACPDispatchPlan(
	task *corev1alpha1.Task,
	target acpDispatchTarget,
	bound *verifiedAgentExecution,
	current ACPRuntimePlan,
) error {
	currentErr := validateFrozenACPDispatchTarget(task, target, bound, current)
	if currentErr == nil {
		return nil
	}
	if bound != nil {
		// Queue reconciliation moves an unbound Reserved attempt to the current
		// image only after the selected pool stops admitting it. If dispatch
		// already claimed that exact serving pool, its frozen plan remains the
		// authority for this attempt and avoids stranding it between rotations.
		if frozenErr := validateFrozenACPDispatchTarget(task, target, bound, bound.plan); frozenErr == nil {
			return nil
		}
	}
	return currentErr
}

//nolint:gocyclo // The explicit state-machine branches are easier to audit together.
func (d *ACPDispatcher) executeReservedTask(ctx context.Context, task *corev1alpha1.Task, target acpDispatchTarget) (retErr error) {
	var promptTrace *acpSpan
	defer func() { promptTrace.End(retErr) }()

	reservationLease := newACPRuntimePoolReservationLease(d, target.reservation)
	if reservationLease != nil {
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if releaseErr := reservationLease.release(releaseCtx); releaseErr != nil {
				logf.FromContext(ctx).Error(releaseErr, "release ACP RuntimePool reservation", "namespace", task.Namespace, "task", task.Name)
			}
		}()
	}
	bound, err := d.loadVerifiedACPDispatchExecution(ctx, task)
	if err != nil {
		return err
	}
	task = bound.frozenTask
	delivery, err := acpRuntimeDeliveryPlanForAttempt(
		bound.plan, task.Status.Execution, bound.promptAttempt, d.ACPRuntimeImages, target.pool,
	)
	if err != nil {
		if frozenErr := validateFrozenACPDispatchTarget(task, target, bound, bound.plan); frozenErr != nil {
			return err
		}
		delivery.plan = bound.plan
	}
	if err := validateFrozenACPDispatchPlan(task, target, bound, delivery.plan); err != nil {
		return err
	}
	if reservationLease != nil {
		reservationLease.startRenewal(ctx)
	}
	runtimeCtx, cancelRuntime := d.newTaskRuntimeContext(ctx, task)
	defer cancelRuntime()
	go d.watchTaskCancellation(runtimeCtx, cancelRuntime, types.NamespacedName{Namespace: task.Namespace, Name: task.Name})
	attemptID, err := promptAttemptIDFromTask(task)
	if err != nil {
		return d.failTask(ctx, task, corev1alpha1.TaskExecutionStateFailed, corev1alpha1.TaskExecutionOutcomeFailed, "InvalidAttempt", err.Error())
	}
	fence, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return err
	}
	if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
		return deadlineErr
	}
	runtimeClient, runtimeFence, profile, maxResultBytes, authErr := d.runtimeClient(runtimeCtx, target)
	if authErr != nil {
		if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
			return deadlineErr
		}
		return d.requeueReservedTask(ctx, task, acpReservedRetryRuntimeClient, authErr)
	}
	if target.pool != nil {
		capabilities, capabilityErr := runtimeClient.Capabilities(runtimeCtx)
		if capabilityErr != nil {
			return d.requeueReservedTask(ctx, task, acpReservedRetryCapabilities, capabilityErr)
		}
		if !capabilities.SupportsAgentSessionConfiguration {
			return d.requeueReservedTask(ctx, task, acpReservedRetrySessionConfiguration, fmt.Errorf("RuntimePool supervisor is waiting for Agent session configuration support"))
		}
	}
	runtimeProfileDigest, digestErr := harnessv2.CanonicalProfileDigest(profile)
	if digestErr != nil || runtimeFence.RuntimeProfileDigest != bound.plan.Digest || runtimeProfileDigest != bound.plan.Digest {
		return d.requeueReservedTask(ctx, task, acpReservedRetryProfile, errors.New("RuntimePool profile does not match the immutable execution snapshot"))
	}
	profile = bound.plan.Profile
	agentConfiguration := bound.configuration
	var agentConfigurationRef *harnessv2.AgentSessionConfiguration
	if target.pool != nil {
		agentConfigurationRef = &agentConfiguration
	}
	mcpConfiguration := bound.mcpConfiguration
	mcpBindingDigest, err := acpDomainDigest("runtime-session-mcp-configuration", mcpConfiguration)
	if err != nil {
		return d.requeueReservedTask(ctx, task, acpReservedRetryMCPConfiguration, err)
	}
	lineage := acpSessionLineageIdentity{RuntimeIdentity: bound.body.RuntimeType}
	if task.Spec.SessionRef != nil {
		lineageConfigDigest, lineageErr := acpSessionLineageConfigDigest(bound.plan)
		if lineageErr != nil {
			return d.requeueReservedTask(ctx, task, acpReservedRetrySessionConfiguration, lineageErr)
		}
		lineage.ConfigDigest = lineageConfigDigest
	}
	if bound.plan.Workspace != nil && bound.plan.Workspace.ReusePolicy == corev1alpha1.WorkspaceReusePolicySession {
		lineage.WorkspaceSessionUID = bound.plan.Workspace.SessionUID
	}
	if task.Spec.SessionRef != nil && d.Sessions.RecordsLineage() {
		taskNamespace := &corev1.Namespace{}
		if err := d.APIReader.Get(runtimeCtx, client.ObjectKey{Name: task.Namespace}, taskNamespace); err != nil {
			return d.requeueReservedTask(ctx, task, acpReservedRetryNamespaceLineage, fmt.Errorf("resolve namespace identity for session lineage: %w", err))
		}
		lineage.NamespaceUID = string(taskNamespace.UID)
	}
	var sessionExecution *acpTaskSession
	sessionCompleted := false
	sessionCtx, sessionTrace := startACPSessionSpan(runtimeCtx, task)
	endSessionTrace := func(err error) {
		reused := sessionExecution != nil && sessionExecution.Reused
		sessionTrace.setSessionReused(reused)
		sessionTrace.setSessionOutcome(acpSessionOutcome(reused, sessionCompleted, err))
		sessionTrace.End(err)
	}
	defer func() { endSessionTrace(retErr) }()
	sessionExecution, err = d.prepareTaskSession(
		sessionCtx, task, fence, runtimeFence.RuntimeProfileDigest, mcpBindingDigest,
		runtimeFence.RuntimeInstanceID, runtimeFence.SupervisorBootID, lineage,
	)
	if err != nil {
		endSessionTrace(err)
		if errors.Is(runtimeContextError(runtimeCtx), context.DeadlineExceeded) {
			recoveredSession, cleanupErr := d.quiesceInterruptedTaskSessionPreparation(ctx, task, attemptID, fence)
			if cleanupErr != nil {
				return errors.Join(err, cleanupErr)
			}
			if settleErr := d.settlePreSubmissionCancellation(
				ctx, task, attemptID, fence, "timeout-before-submission", acpTaskTimeoutReason, "task deadline exceeded before prompt submission",
			); settleErr != nil {
				return errors.Join(err, settleErr)
			}
			if recoveredSession != nil {
				cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), acpPreSubmissionCleanupTimeout)
				defer cancel()
				return d.reconcileUnfinalizedTaskSession(cleanupCtx, task, fence, recoveredSession, context.DeadlineExceeded)
			}
			return nil
		}
		return d.requeueReservedTask(ctx, task, acpReservedRetrySessionPreparation, err)
	}
	sessionTrace.setSessionReused(sessionExecution != nil && sessionExecution.Reused)
	if sessionExecution != nil && sessionExecution.Turn != nil {
		defer func() {
			if sessionExecution.finalized || sessionExecution.requeued {
				return
			}
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), acpPreSubmissionCleanupTimeout)
			defer cancel()
			finalizeErr := d.reconcileUnfinalizedTaskSession(cleanupCtx, task, fence, sessionExecution, retErr)
			if retErr == nil && finalizeErr != nil {
				retErr = finalizeErr
			}
		}()
	}
	if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
		return deadlineErr
	}
	if reservationLease != nil && sessionExecution != nil {
		residentSlots := int32(0)
		if !sessionExecution.Reused {
			residentSlots = 1
		}
		if err := reservationLease.setSlots(ctx, residentSlots); err != nil {
			if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
				ctx, task, attemptID, fence, err, &sessionExecution.Binding,
			); requeueErr != nil {
				return errors.Join(err, requeueErr)
			}
			sessionExecution.requeued = true
			return nil
		}
	}
	leaseGeneration := int64(1)
	if sessionExecution != nil {
		runtimeFence.RuntimeSessionUID = harnessv2.RuntimeSessionUID(sessionExecution.Binding.SessionUID)
		runtimeFence.RuntimeSessionGeneration = sessionExecution.Binding.Generation
		leaseGeneration = sessionExecution.LeaseGeneration
	} else {
		reservedAttempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
		if err != nil {
			return fmt.Errorf("load reserved PromptAttempt for task-scoped RuntimeSession generation: %w", err)
		}
		taskScopedGeneration, err := taskScopedRuntimeSessionGeneration(reservedAttempt)
		if err != nil {
			return err
		}
		runtimeFence.RuntimeSessionUID = harnessv2.RuntimeSessionUID(taskRuntimeSessionUID(task))
		runtimeFence.RuntimeSessionGeneration = taskScopedGeneration
		leaseGeneration = int64(taskScopedGeneration)
	}
	sessionTrace.setRuntimeSession(string(runtimeFence.RuntimeSessionUID), runtimeFence.RuntimeSessionGeneration)
	if err := d.transitionAttempt(ctx, attemptID, fence, store.PromptExecutionReserved, store.PromptExecutionSessionStarting, "session-starting", nil); err != nil {
		return err
	}
	if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
		status.State = corev1alpha1.TaskExecutionStateSessionStarting
		status.LastTransitionTime = nowMeta()
	}); err != nil {
		return err
	}
	if err := d.transitionAttempt(ctx, attemptID, fence, store.PromptExecutionSessionStarting, store.PromptExecutionPlanned, "planned", &attemptRuntimeBinding{
		RuntimeInstanceID: string(runtimeFence.RuntimeInstanceID), SessionUID: string(runtimeFence.RuntimeSessionUID), SessionGeneration: leaseGeneration,
	}); err != nil {
		return err
	}
	if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
		status.State = corev1alpha1.TaskExecutionStatePlanned
		if sessionExecution != nil {
			applyRuntimeSessionBindingToExecution(status, &sessionExecution.Binding)
		} else {
			status.RuntimeInstanceID = string(runtimeFence.RuntimeInstanceID)
			status.RuntimeSessionUID = string(runtimeFence.RuntimeSessionUID)
			status.RuntimeSessionGeneration = int64(runtimeFence.RuntimeSessionGeneration)
			status.RuntimeSessionSupervisorBootID = ""
			status.RuntimeSessionProfileDigest = ""
			status.RuntimeSessionMCPDigest = ""
			status.RuntimeSessionWorkspaceDigest = ""
			status.RuntimeSessionRecreationPending = false
		}
		status.LastTransitionTime = nowMeta()
	}); err != nil {
		return err
	}
	if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
		return deadlineErr
	}
	requeued, reconcileErr := d.reconcilePlannedRuntimeSession(
		ctx, runtimeClient, task, attemptID, fence, sessionExecution, &runtimeFence,
	)
	if runtimeContextError(runtimeCtx) != nil && sessionExecution != nil {
		// Cancellation supersedes an admission requeue. Let the deferred Session
		// reconciler finalize the open turn and release its mutation lease.
		sessionExecution.requeued = false
	}
	if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
		return deadlineErr
	}
	if runtimeContextError(runtimeCtx) != nil {
		return d.settlePreSubmissionCancellation(
			ctx, task, attemptID, fence, "cancelled-before-submission", "Cancelled", "task cancelled before prompt submission",
		)
	}
	if reconcileErr != nil {
		return reconcileErr
	} else if requeued {
		return nil
	}

	plannedAttempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return fmt.Errorf("load planned PromptAttempt for RuntimeSession creation: %w", err)
	}
	if plannedAttempt.ExecutionState != store.PromptExecutionPlanned || plannedAttempt.UpdatedAt.IsZero() {
		return fmt.Errorf("planned PromptAttempt lacks a durable RuntimeSession creation timestamp")
	}
	plannedAt := plannedAttempt.UpdatedAt.UTC()
	taskScopedRuntimeSessionReused := false
	if sessionExecution == nil {
		var requeued bool
		taskScopedRuntimeSessionReused, requeued, err = d.reconcilePlannedTaskScopedRuntimeSession(
			ctx, runtimeClient, task, attemptID, fence, runtimeFence,
		)
		if err != nil || requeued {
			return err
		}
	}
	preparedWorkspace, err := d.prepareRuntimeWorkspace(
		runtimeCtx, task, fence, sessionExecution, plannedAt, taskScopedRuntimeSessionReused,
	)
	if err != nil {
		if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
			return deadlineErr
		}
		if transitionErr := d.transitionAttemptToTerminal(ctx, attemptID, fence, store.PromptExecutionFailed, "workspace-unsupported"); transitionErr != nil {
			return transitionErr
		}
		return d.failTask(ctx, task, corev1alpha1.TaskExecutionStateFailed, corev1alpha1.TaskExecutionOutcomeFailed, "WorkspaceUnsupported", err.Error())
	}
	baseline := preparedWorkspace.baseline
	workspace := preparedWorkspace.spec
	createIssuedAt := preparedWorkspace.createIssuedAt
	// The resume expectation is stamped AFTER the binding digest was
	// computed: it asserts a transient lineage property (a committed durable
	// checkpoint must exist), not workspace identity, so it never changes
	// which pool binding the session reuses.
	expectDurableResume, resumeFloor, resumeErr := d.taskExpectsDurableResume(ctx, task)
	if resumeErr != nil {
		return resumeErr
	}
	workspace.ExpectDurableResume = expectDurableResume
	if expectDurableResume {
		workspace.ExpectDurableResumeFrom = preparedWorkspace.priorRepositoryIdentity
		workspace.ExpectDurableResumeMinGeneration = resumeFloor
	}
	workspaceAuthorization := preparedWorkspace.authorization
	if sessionExecution != nil {
		previousRuntimeFence := runtimeFence
		bound, workspaceChanged, bindErr := bindACPRuntimeSessionWorkspace(
			sessionExecution.Binding, sessionExecution.Reused, preparedWorkspace.bindingDigest,
		)
		if bindErr != nil {
			return bindErr
		}
		if workspaceChanged {
			bound.Generation = max(bound.Generation, uint64(sessionExecution.LeaseGeneration))
			bound.RecreationRequired = true
			sessionExecution.Binding = bound
			sessionExecution.Reused = false
			d.setRuntimeSessionBinding(bound)
			if err := d.patchExecution(ctx, task, func(execution *corev1alpha1.TaskExecutionStatus) {
				applyRuntimeSessionBindingToExecution(execution, &bound)
				execution.LastTransitionTime = nowMeta()
			}); err != nil {
				return err
			}
			if err := d.deleteRuntimeSessionReconciled(
				context.WithoutCancel(ctx), runtimeClient, harnessv2.RuntimeSessionID(runtimeSessionID(previousRuntimeFence)),
				task, previousRuntimeFence, "workspace_binding_changed",
			); err != nil {
				deleteErr := fmt.Errorf("retire workspace-mismatched RuntimeSession: %w", err)
				if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
					ctx, task, attemptID, fence, deleteErr, &bound,
				); requeueErr != nil {
					return requeueErr
				}
				sessionExecution.requeued = true
				return nil
			}
			if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
				return deadlineErr
			}
			if runtimeContextError(runtimeCtx) != nil {
				return d.settlePreSubmissionCancellation(
					ctx, task, attemptID, fence, "cancelled-before-submission", "Cancelled", "task cancelled before prompt submission",
				)
			}
			if err := d.requeuePreSubmissionTaskWithRuntimeBinding(
				ctx, task, attemptID, fence, errors.New("RuntimeSession workspace binding changed"), &bound,
			); err != nil {
				return err
			}
			sessionExecution.requeued = true
			return nil
		}
		runtimeFence.RuntimeSessionGeneration = bound.Generation
		sessionExecution.Binding = bound
	}
	if reservationLease != nil {
		if err := reservationLease.renew(runtimeCtx); err != nil {
			if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
				return deadlineErr
			}
			if runtimeContextError(runtimeCtx) != nil {
				if sessionExecution != nil {
					sessionExecution.requeued = false
				}
				return d.settlePreSubmissionCancellation(
					ctx, task, attemptID, fence, "cancelled-before-submission", "Cancelled", "task cancelled before prompt submission",
				)
			}
			if sessionExecution != nil {
				if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
					ctx, task, attemptID, fence, err, &sessionExecution.Binding,
				); requeueErr != nil {
					return requeueErr
				}
				sessionExecution.requeued = true
				return nil
			}
			return d.requeuePreSubmissionTask(ctx, task, attemptID, fence, err)
		}
	}
	createOperation := "create-session-v" + strconv.FormatInt(plannedAttempt.Version, 10)
	createExpiresAt := runtimeSessionCreateExpiresAt(createIssuedAt, target)
	if sessionExecution != nil {
		createOperation = "create-session-g" + strconv.FormatUint(runtimeFence.RuntimeSessionGeneration, 10)
	}
	runtimeSessionCreationRequired := sessionExecution == nil && !taskScopedRuntimeSessionReused ||
		sessionExecution != nil && !sessionExecution.Reused
	renewalExpiresAt := runtimeSessionCreateRenewalExpiresAt(createIssuedAt, createExpiresAt, workspaceAuthorization != nil)
	if runtimeSessionCreationRequired && runtimeSessionCreateAuthorizationNeedsRenewal(renewalExpiresAt, time.Now().UTC()) {
		if sessionExecution != nil {
			if err := d.rotateExpiredSessionBoundRuntimeSessionCreation(
				ctx, task, attemptID, fence, sessionExecution, &runtimeFence,
			); err != nil {
				return err
			}
			return nil
		}
		return d.requeuePreSubmissionTask(
			ctx, task, attemptID, fence,
			errors.New("task-scoped RuntimeSession creation authorization expired before submission"),
		)
	}
	createRequest := harnessv2.CreateRuntimeSessionRequest{
		Protocol:         harnessv2.ProtocolVersion,
		Metadata:         mutationMetadata(runtimeFence, task, createOperation, false, createExpiresAt),
		RuntimeSessionID: harnessv2.RuntimeSessionID(runtimeSessionID(runtimeFence)),
		Profile:          profile, AgentConfiguration: agentConfigurationRef, MCPConfiguration: mcpConfiguration,
		Workspace: workspace, WorkspaceArtifactAuthorization: workspaceAuthorization,
	}
	if err := sealMutation(&createRequest.Metadata.RequestDigest, createRequest); err != nil {
		return err
	}
	runtimeSessionRetirementRequired := sessionExecution == nil || workspace.Intent == harnessv2.WorkspaceIntentWrite
	runtimeSessionCleanupPending := taskScopedRuntimeSessionReused ||
		sessionExecution != nil && workspace.Intent == harnessv2.WorkspaceIntentWrite
	runtimeSessionSettlementRequired := false
	runtimePublicationFinalizationRequired := false
	runtimePublicationFinalized := false
	var runtimePublicationFinalization *runtimeSessionPublicationFinalization
	finalizePreparedRuntimeSession := func() error {
		if !runtimePublicationFinalizationRequired || runtimePublicationFinalized {
			return nil
		}
		if runtimePublicationFinalization == nil {
			return fmt.Errorf("%w: prepared RuntimeSession publication lacks a terminal finalization receipt", store.ErrNotReady)
		}
		if err := d.finalizeRuntimeSessionPublication(
			context.WithoutCancel(ctx), runtimeClient, createRequest.RuntimeSessionID, task, runtimeFence, *runtimePublicationFinalization,
		); err != nil {
			return err
		}
		runtimePublicationFinalized = true
		return nil
	}
	cleanupRuntimeSession := func(reason string) error {
		if !runtimeSessionCleanupPending {
			return nil
		}
		if err := finalizePreparedRuntimeSession(); err != nil {
			return err
		}
		if runtimeSessionSettlementRequired && sessionExecution != nil && sessionExecution.Turn != nil && !sessionExecution.finalized {
			return fmt.Errorf("%w: RuntimeSession deletion requires durable Session terminal settlement", store.ErrNotReady)
		}
		cleanupErr := d.deleteRuntimeSession(
			context.WithoutCancel(ctx), runtimeClient, createRequest.RuntimeSessionID, task, runtimeFence, reason,
		)
		if cleanupErr != nil {
			logf.FromContext(ctx).Error(
				cleanupErr, "delete ACP RuntimeSession", "namespace", task.Namespace, "task", task.Name,
				"runtimeSessionID", createRequest.RuntimeSessionID, "reason", reason,
			)
			return cleanupErr
		}
		if runtimeSessionRetirementRequired {
			if markErr := d.markTaskScopedRuntimeSessionCleanupComplete(
				context.WithoutCancel(ctx), task, task.UID, string(runtimeFence.RuntimeInstanceID),
				string(runtimeFence.RuntimeSessionUID), int64(runtimeFence.RuntimeSessionGeneration),
			); markErr != nil {
				return markErr
			}
			runtimeSessionCleanupPending = false
		}
		return nil
	}
	defer func() {
		// A pre-submission requeue preserves this exact binding for adoption.
		// Retiring its runtime session here would strand the retry on a tombstone.
		if !runtimeSessionCleanupPending || sessionExecution != nil && sessionExecution.requeued {
			return
		}
		if cleanupErr := cleanupRuntimeSession("task_scoped_terminal"); cleanupErr != nil && retErr == nil {
			retErr = fmt.Errorf("delete task-scoped RuntimeSession: %w", cleanupErr)
		}
	}()
	if runtimeSessionCreationRequired {
		if sessionExecution != nil {
			sessionExecution.Binding.RecreationRequired = true
			d.setRuntimeSessionBinding(sessionExecution.Binding)
			if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
				applyRuntimeSessionBindingToExecution(status, &sessionExecution.Binding)
				status.LastTransitionTime = nowMeta()
			}); err != nil {
				return err
			}
		}
		created := false
		if _, err := runtimeClient.CreateRuntimeSession(sessionCtx, createRequest); err != nil {
			if runtimeSessionRetirementRequired && runtimeSessionCreationMayHaveApplied(err) {
				runtimeSessionCleanupPending = true
			}
			if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
				return deadlineErr
			}
			digestConflict := runtimeSessionCreateDigestConflict(err)
			if sessionExecution == nil && digestConflict {
				// The runtime already processed a create for this exact attempt
				// identity: this reconcile rebuilt the request (fresh expiry and
				// workspace capability) after an earlier send. Adopt the session
				// the earlier send created when the runtime still reports it
				// admissible instead of failing a usable attempt.
				// Bound adoption by the remaining creation budget once the
				// runtime reports when the earlier send entered Creating.
				adopted, adoptErr := reconcileRuntimeSessionCreateDigestConflict(
					runtimeCtx, runtimeClient, createRequest.RuntimeSessionID,
					runtimeFence.RuntimeSessionUID, runtimeFence.RuntimeSessionGeneration, runtimeSessionCreateTimeout(target),
				)
				if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
					return deadlineErr
				}
				if runtimeContextError(runtimeCtx) != nil {
					return d.settlePreSubmissionCancellation(
						ctx, task, attemptID, fence, "cancelled-before-submission", "Cancelled", "task cancelled before prompt submission",
					)
				}
				if adoptErr != nil {
					logf.FromContext(ctx).Info(
						"ACP RuntimeSession adoption after create digest conflict failed",
						"namespace", task.Namespace, "task", task.Name,
						"runtimeSessionGeneration", runtimeFence.RuntimeSessionGeneration,
						"inconclusive", errors.Is(adoptErr, errRuntimeSessionAdoptionInconclusive), "diagnostic", adoptErr.Error(),
					)
				}
				if adopted {
					logf.FromContext(ctx).Info(
						"ACP RuntimeSession adopted after create digest conflict",
						"namespace", task.Namespace, "task", task.Name,
						"runtimeSessionGeneration", runtimeFence.RuntimeSessionGeneration,
					)
					created = true
				} else {
					// The runtime holds a record for this create identity but no
					// admissible session: the record is a deletion tombstone, the
					// session settled in an unusable state, or it never became
					// admissible within its own creation budget. The deferred
					// task-scoped cleanup retires whatever is resident.
					retrying, handleErr := d.handlePrePromptClientError(ctx, task, attemptID, fence, err)
					if !retrying {
						endSessionTrace(err)
					}
					return handleErr
				}
			} else if sessionExecution != nil && runtimeSessionCreationMayHaveApplied(err) {
				var admitted bool
				var reconcileErr error
				if digestConflict {
					admitted, reconcileErr = reconcileRuntimeSessionCreateDigestConflict(
						runtimeCtx, runtimeClient, createRequest.RuntimeSessionID,
						runtimeFence.RuntimeSessionUID, runtimeFence.RuntimeSessionGeneration, runtimeSessionCreateTimeout(target),
					)
					if reconcileErr == nil && !admitted {
						// The record is a deletion tombstone: this generation can
						// never be recreated, and planned-session reconciliation
						// only advances the generation for reused sessions, so a
						// requeue would rebuild the same conflict forever. An
						// inconclusive window is an error and requeues below.
						retrying, handleErr := d.handlePrePromptClientError(ctx, task, attemptID, fence, err)
						if !retrying {
							endSessionTrace(err)
						}
						return handleErr
					}
				} else {
					admitted, reconcileErr = waitForRuntimeSessionAdmission(
						context.WithoutCancel(ctx), runtimeClient, createRequest.RuntimeSessionID,
						runtimeFence.RuntimeSessionUID, runtimeFence.RuntimeSessionGeneration, 10*time.Second,
					)
				}
				if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
					return deadlineErr
				}
				if runtimeContextError(runtimeCtx) != nil {
					return d.settlePreSubmissionCancellation(
						ctx, task, attemptID, fence, "cancelled-before-submission", "Cancelled", "task cancelled before prompt submission",
					)
				}
				if reconcileErr != nil {
					if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
						ctx, task, attemptID, fence, reconcileErr, &sessionExecution.Binding,
					); requeueErr != nil {
						return requeueErr
					}
					sessionExecution.requeued = true
					return nil
				}
				if admitted {
					created = true
				} else {
					if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
						ctx, task, attemptID, fence, err, &sessionExecution.Binding,
					); requeueErr != nil {
						return requeueErr
					}
					sessionExecution.requeued = true
					return nil
				}
			} else if sessionExecution != nil && isACPRateLimitedClientError(err) {
				if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
					ctx, task, attemptID, fence, err, &sessionExecution.Binding,
				); requeueErr != nil {
					return requeueErr
				}
				sessionExecution.requeued = true
				return nil
			} else {
				retrying, handleErr := d.handlePrePromptClientError(ctx, task, attemptID, fence, err)
				if !retrying {
					endSessionTrace(err)
				}
				return handleErr
			}
		} else {
			created = true
		}
		if !created {
			return fmt.Errorf("RuntimeSession creation was not reconciled")
		}
		if runtimeSessionRetirementRequired {
			runtimeSessionCleanupPending = true
		}
		if sessionExecution != nil {
			sessionExecution.Binding.RecreationRequired = false
			d.setRuntimeSessionBinding(sessionExecution.Binding)
			if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
				applyRuntimeSessionBindingToExecution(status, &sessionExecution.Binding)
				status.LastTransitionTime = nowMeta()
			}); err != nil {
				if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
					ctx, task, attemptID, fence, err, &sessionExecution.Binding,
				); requeueErr != nil {
					return errors.Join(err, requeueErr)
				}
				sessionExecution.requeued = true
				return nil
			}
		}
	}
	if reservationLease != nil {
		if err := reservationLease.setSlots(ctx, 0); err != nil {
			_ = cleanupRuntimeSession("capacity_reservation_lost")
			return d.requeuePreSubmissionTask(ctx, task, attemptID, fence, err)
		}
	}

	// Persist the high-water mark on the durable Session after the provider
	// RuntimeSession is proven live. This survives controller restarts, physical
	// runtime replacement, and deletion of prior Task objects.
	if sessionExecution != nil {
		if sessionExecution.Turn == nil {
			return fmt.Errorf("session-bound RuntimeSession lacks an open SessionTurn")
		}
		committedLease, commitErr := d.Sessions.CommitRuntimeSessionGeneration(
			ctx,
			sessionExecution.Turn.Lease,
			fence,
			runtimeFence.RuntimeSessionGeneration,
			time.Now().UTC(),
		)
		if commitErr != nil {
			if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
				ctx, task, attemptID, fence, commitErr, &sessionExecution.Binding,
			); requeueErr != nil {
				return errors.Join(commitErr, requeueErr)
			}
			sessionExecution.requeued = true
			return nil
		}
		sessionExecution.Turn.Lease = *committedLease
	}

	// A live RuntimeSession - freshly created here or reconciled as reused -
	// commits (or committed) the supervisor's durable checkpoint
	// synchronously during its creation; record that on the linked workspace
	// so a later resumed lineage can assert the checkpoint exists. The
	// record GATES resume verification, so a missed stamp would make it fail
	// OPEN: a later suspension whose snapshot is lost would be silently
	// replaced by a fresh baseline. The stamp runs at this convergence point
	// exactly because a retry that reconciles the existing session as reused
	// skips the creation branch: it must still retry the stamp before any
	// prompt submission. Failure requeues (idempotent on retry).
	if stampErr := d.markLinkedWorkspaceDurableSessionCommitted(ctx, task, runtimeFence.RuntimeSessionGeneration); stampErr != nil {
		if sessionExecution != nil {
			if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
				ctx, task, attemptID, fence, stampErr, &sessionExecution.Binding,
			); requeueErr != nil {
				return errors.Join(stampErr, requeueErr)
			}
			sessionExecution.requeued = true
			return nil
		}
		return d.requeuePreSubmissionTask(ctx, task, attemptID, fence, stampErr)
	}
	agentName := ""
	if task.Status.AgentExecutionBinding != nil && task.Status.AgentExecutionBinding.Agent != nil {
		agentName = task.Status.AgentExecutionBinding.Agent.Name
	} else if task.Spec.AgentRef != nil {
		agentName = task.Spec.AgentRef.Name
	}
	journalState, err := (v2eventjournal.Journal{
		EventStore: d.EventStore,
		MapContext: v2eventjournal.MapContext{
			Namespace: task.Namespace, TaskName: task.Name, SessionName: taskSessionName(task),
			AgentName: agentName, StreamID: task.Name, Provider: profile.ProviderKind, Model: profile.Model,
		},
		RecoveryIdentity: v2eventjournal.MappedUpdateIdentity{
			Protocol:          harnessv2.ProtocolVersion,
			RuntimeInstanceID: runtimeFence.RuntimeInstanceID, SupervisorBootID: runtimeFence.SupervisorBootID,
			RuntimeSessionUID: runtimeFence.RuntimeSessionUID, RuntimeSessionGeneration: runtimeFence.RuntimeSessionGeneration,
			TaskUID: harnessv2.TaskUID(task.UID), TaskAttempt: uint32(task.Status.Execution.Attempt),
			PromptID: harnessv2.PromptID(task.Status.Execution.PromptID),
		},
	}).Open(ctx)
	if err != nil {
		openErr := fmt.Errorf("open ACP execution event journal: %w", err)
		if sessionExecution != nil {
			if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
				ctx, task, attemptID, fence, openErr, &sessionExecution.Binding,
			); requeueErr != nil {
				return errors.Join(openErr, requeueErr)
			}
			sessionExecution.requeued = true
			return nil
		}
		return d.requeuePreSubmissionTask(ctx, task, attemptID, fence, openErr)
	}
	if err := d.openTaskSessionTurn(sessionCtx, task, attemptID, fence, sessionExecution); err != nil {
		if handled, deadlineErr := d.handlePreSubmissionContextDone(ctx, runtimeCtx, task, attemptID, fence); handled {
			return deadlineErr
		}
		return err
	}
	sessionCompleted = true
	endSessionTrace(nil)
	if err := d.transitionAttempt(ctx, attemptID, fence, store.PromptExecutionPlanned, store.PromptExecutionSubmitting, "submitting", nil); err != nil {
		return err
	}
	if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
		status.State = corev1alpha1.TaskExecutionStateSubmitting
		status.LastTransitionTime = nowMeta()
	}); err != nil {
		return err
	}
	ctx, promptTrace = startACPPromptSpan(ctx, task)
	promptTrace.setRuntimeSession(string(runtimeFence.RuntimeSessionUID), runtimeFence.RuntimeSessionGeneration)
	runtimeCtx = promptTrace.withContext(runtimeCtx)

	var bootstrap string
	userPrompt := task.Spec.Prompt
	if sessionExecution != nil {
		bootstrap = bootstrapPromptText(sessionExecution.Bootstrap)
		userPrompt = sessionExecution.UserPrompt
	}
	var terminal *harnessv2.Event
	var lastAssistantUpdate *harnessv2.Event
	var assistant strings.Builder
	assistantOverflow := false
	flushInterruptedOutput := func(flushCtx context.Context) error {
		persistableAssistant, assistantContentOmitted := assistantTranscriptForPersistence(
			assistant.String(), assistantOverflow,
		)
		var assistantEvent *harnessv2.Event
		var assistantOrderSequence uint64
		if lastAssistantUpdate != nil && (persistableAssistant != "" || assistantContentOmitted) {
			assistantEvent = lastAssistantUpdate
			assistantOrderSequence = lastAssistantUpdate.Identity.Sequence
		}
		return journalState.AppendBufferedStreamsIfNew(
			flushCtx, assistantEvent, assistantOrderSequence, persistableAssistant, assistantContentOmitted,
		)
	}
	accepted := false
	admissionRetry := 0
	for {
		promptRequest, err := d.buildPromptRequest(
			task, runtimeFence, profile, mcpConfiguration, bootstrap, userPrompt, admissionRetry,
		)
		if err != nil {
			return err
		}
		if err := sealMutation(&promptRequest.Metadata.RequestDigest, promptRequest); err != nil {
			return err
		}
		leaseCtx, stopLease := context.WithCancel(runtimeCtx)
		go d.renewPromptLeaseLoop(
			leaseCtx, cancelRuntime, runtimeClient, createRequest.RuntimeSessionID, task, runtimeFence,
			promptRequest.Lease, promptRequest.MCPAuthorization,
		)
		summary, streamErr := runtimeClient.StreamPrompt(runtimeCtx, createRequest.RuntimeSessionID, promptRequest, func(event harnessv2.Event) error {
			switch event.Type {
			case harnessv2.EventAccepted:
				runtimeSessionSettlementRequired = true
				if _, _, err := journalState.AppendPromptLifecycleIfNew(ctx, event); err != nil {
					return acpUpdatePersistenceError(err, nil)
				}
				if !accepted {
					if err := d.transitionAttempt(ctx, attemptID, fence, store.PromptExecutionSubmitting, store.PromptExecutionAccepted, "accepted", nil); err != nil {
						return err
					}
					if err := d.transitionAttempt(ctx, attemptID, fence, store.PromptExecutionAccepted, store.PromptExecutionRunning, "running", nil); err != nil {
						return err
					}
					accepted = true
					if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
						status.State = corev1alpha1.TaskExecutionStateRunning
						status.Reason = ""
						status.Message = ""
						status.LastTransitionTime = nowMeta()
					}); err != nil {
						return err
					}
					if reservationLease != nil {
						releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
						releaseErr := reservationLease.release(releaseCtx)
						cancel()
						if releaseErr != nil {
							logf.FromContext(ctx).Error(releaseErr, "release accepted ACP RuntimePool reservation", "namespace", task.Namespace, "task", task.Name)
						}
					}
				}
			case harnessv2.EventUpdate:
				if event.Update != nil && event.Update.AssistantMessage != nil {
					copy := event
					lastAssistantUpdate = &copy
					text := event.Update.AssistantMessage.Text
					if !assistantOverflow {
						if maxResultBytes < 1 || len(text) > maxResultBytes-assistant.Len() {
							assistantOverflow = true
						} else {
							assistant.WriteString(text)
						}
					}
				}
				var planState *store.PlanState
				var planErr error
				if event.Update != nil && event.Update.Plan != nil {
					projection := journalState.ProjectPlanUpdate(*event.Update.Plan)
					planState = &store.PlanState{
						TaskName: task.Name, Namespace: task.Namespace, Iteration: int(task.Status.Iteration),
						Summary: projection.Summary, ProgressPct: projection.ProgressPct,
						GoalComplete: projection.GoalComplete, PlanDocument: projection.Document,
					}
				}
				var journalErr error
				if planState == nil {
					_, _, journalErr = journalState.AppendUpdateIfNew(ctx, event)
				} else if _, ok := d.EventStore.(store.AtomicExecutionEventPlanStore); ok {
					_, _, journalErr = journalState.AppendPlanUpdateIfNew(ctx, event, planState)
				} else {
					// Direct unit tests may use separate fake stores without starting
					// the dispatcher. Production startup requires the atomic path above.
					planErr = saveACPPlanUpdateWithRetry(ctx, d.PlanStore, task.Namespace, task.Name, planState)
					_, _, journalErr = journalState.AppendUpdateIfNew(ctx, event)
					planErr = reconcileACPPlanUpdateAfterJournal(
						ctx, d.PlanStore, task.Namespace, task.Name, planState, planErr, journalErr,
					)
				}
				if persistenceErr := acpUpdatePersistenceError(journalErr, planErr); persistenceErr != nil {
					return persistenceErr
				}
			case harnessv2.EventPermissionRequested:
				return d.resolvePromptPermission(runtimeCtx, runtimeClient, createRequest.RuntimeSessionID, task, runtimeFence, event)
			case harnessv2.EventCompleted, harnessv2.EventCancelled, harnessv2.EventFailed, harnessv2.EventOutcomeUnknown:
				copy := event
				terminal = &copy
			}
			return nil
		})
		stopLease()
		if accepted || summary.Accepted || streamErr == nil || !isACPRateLimitedClientError(streamErr) {
			runtimeSessionSettlementRequired = true
		}
		if streamErr == nil {
			break
		}
		if !accepted && !summary.Accepted && isACPRateLimitedClientError(streamErr) {
			if reservationLease != nil {
				releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				releaseErr := reservationLease.release(releaseCtx)
				cancel()
				if releaseErr != nil {
					return releaseErr
				}
			}
			if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
				status.Reason = corev1alpha1.TaskExecutionReasonAtCapacity
				status.Message = "RuntimePool prompt admission was rate limited and will be reconciled"
				status.LastTransitionTime = nowMeta()
			}); err != nil {
				return err
			}
			admissionRetry++
			reservationLease, err = d.waitForPromptCapacityReservation(runtimeCtx, task, target, fence, runtimeFence.RuntimeInstanceID)
			if err != nil {
				_ = cleanupRuntimeSession("prompt_admission_reconciliation_stopped")
				if runtimeContextError(runtimeCtx) != nil {
					return recordACPPromptOutcomeIfSettled(
						ctx, promptTrace, acpPromptOutcomeCancelled,
						d.finishNonSuccess(ctx, task, attemptID, fence, sessionExecution, harnessv2.Event{Type: harnessv2.EventCancelled}),
					)
				}
				return recordACPPromptOutcomeIfSettled(
					ctx, promptTrace, acpPromptOutcomeFailed,
					d.finishNonSuccess(ctx, task, attemptID, fence, sessionExecution, harnessv2.Event{Type: harnessv2.EventFailed}),
				)
			}
			continue
		}
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), acpInterruptedOutputFlushTimeout)
		if persistErr := flushInterruptedOutput(flushCtx); persistErr != nil {
			streamErr = acpUpdatePersistenceError(persistErr, nil)
		}
		cancel()
		return d.handlePromptStreamError(
			ctx, promptTrace, runtimeClient, createRequest.RuntimeSessionID, task, attemptID, fence, runtimeFence, journalState,
			accepted || summary.Accepted, summary.WriteEvidence, runtimeContextError(runtimeCtx), streamErr,
		)
	}
	if terminal == nil {
		if err := flushInterruptedOutput(ctx); err != nil {
			logf.FromContext(ctx).Error(err, "persist unterminated ACP buffered streams", "namespace", task.Namespace, "task", task.Name)
			return recordACPPromptOutcomeIfSettled(
				ctx, promptTrace, acpPromptOutcomeFailed,
				d.failPromptForExecutionEventPersistence(
					ctx, task, attemptID, fence, "unterminated buffered stream persistence failed",
				),
			)
		}
		if err := appendPromptStreamFailureLifecycleIfNew(
			ctx, journalState, time.Now().UTC(), harnessv2.ErrMissingTerminalEvent,
		); err != nil {
			logf.FromContext(ctx).Error(err, "persist unterminated ACP prompt lifecycle", "namespace", task.Namespace, "task", task.Name)
			return recordACPPromptOutcomeIfSettled(
				ctx, promptTrace, acpPromptOutcomeFailed,
				d.failPromptForExecutionEventPersistence(
					ctx, task, attemptID, fence, "unterminated prompt lifecycle persistence failed",
				),
			)
		}
		return recordACPPromptOutcomeIfSettled(
			ctx, promptTrace, acpPromptOutcomeUnknown,
			d.markOutcomeUnknown(ctx, task, attemptID, fence, "MissingTerminal", "ACP stream ended without a terminal event"),
		)
	}
	flushTerminalOutput := func(transcript string, contentOmitted bool) error {
		var assistantEvent *harnessv2.Event
		var assistantOrderSequence uint64
		if transcript != "" || contentOmitted {
			assistantEvent = terminal
			assistantOrderSequence = terminal.Identity.Sequence
			if lastAssistantUpdate != nil {
				assistantOrderSequence = lastAssistantUpdate.Identity.Sequence
			}
		}
		return journalState.AppendBufferedStreamsIfNew(
			ctx, assistantEvent, assistantOrderSequence, transcript, contentOmitted,
		)
	}
	if terminal.Type != harnessv2.EventCompleted {
		persistableAssistant, assistantContentOmitted := assistantTranscriptForPersistence(
			assistant.String(), assistantOverflow,
		)
		if err := flushTerminalOutput(persistableAssistant, assistantContentOmitted); err != nil {
			logf.FromContext(ctx).Error(err, "persist terminal ACP buffered streams", "namespace", task.Namespace, "task", task.Name)
			return recordACPPromptOutcomeIfSettled(
				ctx, promptTrace, acpPromptOutcomeFailed,
				d.failPromptForExecutionEventPersistence(
					ctx, task, attemptID, fence, "terminal buffered stream persistence failed",
				),
			)
		}
		if _, _, err := journalState.AppendPromptLifecycleIfNew(ctx, *terminal); err != nil {
			logf.FromContext(ctx).Error(err, "persist terminal ACP prompt lifecycle", "namespace", task.Namespace, "task", task.Name)
			return recordACPPromptOutcomeIfSettled(
				ctx, promptTrace, acpPromptOutcomeFailed,
				d.failPromptForExecutionEventPersistence(
					ctx, task, attemptID, fence, "terminal prompt lifecycle persistence failed",
				),
			)
		}
		outcome := acpPromptOutcomeFailed
		switch terminal.Type {
		case harnessv2.EventCancelled:
			outcome = acpPromptOutcomeCancelled
		case harnessv2.EventOutcomeUnknown:
			outcome = acpPromptOutcomeUnknown
		}
		return recordACPPromptOutcomeIfSettled(
			ctx, promptTrace, outcome,
			d.finishNonSuccess(ctx, task, attemptID, fence, sessionExecution, *terminal),
		)
	}
	settlement := harnessv2.PromptSettlement{TerminalEvent: terminal.Type, Outcome: harnessv2.PromptOutcomeSucceeded, StopReason: harnessv2.ACPStopReasonEndTurn, SettledAt: terminal.Identity.Timestamp}
	settlementDigest, err := harnessv2.CanonicalPromptSettlementDigest(settlement)
	if err != nil {
		return err
	}
	resultText, err := completedPromptResultText(terminal, assistant.String(), assistantOverflow)
	if err != nil {
		return err
	}
	persistableAssistant, assistantContentOmitted := completedAssistantTranscriptForPersistence(
		assistant.String(), assistantOverflow, resultText,
	)
	if err := d.transitionAttemptToSettlingWithResult(ctx, task, attemptID, fence, []byte(resultText)); err != nil {
		return err
	}
	if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
		status.State = corev1alpha1.TaskExecutionStateSettling
		status.LastTransitionTime = nowMeta()
	}); err != nil {
		return err
	}
	if err := flushTerminalOutput(persistableAssistant, assistantContentOmitted); err != nil {
		logf.FromContext(ctx).Error(err, "persist terminal ACP buffered streams", "namespace", task.Namespace, "task", task.Name)
		return d.failPromptForExecutionEventPersistence(
			ctx, task, attemptID, fence, "terminal buffered stream persistence failed",
		)
	}
	if _, _, err := journalState.AppendTerminalUsageIfNew(ctx, *terminal); err != nil {
		logf.FromContext(ctx).Error(err, "persist terminal ACP usage", "namespace", task.Namespace, "task", task.Name)
		return d.failPromptForExecutionEventPersistence(
			ctx, task, attemptID, fence, "terminal usage persistence failed",
		)
	}
	if err := d.persistTaskResult(ctx, task, []byte(resultText)); err != nil {
		return err
	}
	if _, _, err := journalState.AppendPromptLifecycleIfNew(ctx, *terminal); err != nil {
		logf.FromContext(ctx).Error(err, "persist terminal ACP prompt lifecycle", "namespace", task.Namespace, "task", task.Name)
		return d.failPromptForExecutionEventPersistence(
			ctx, task, attemptID, fence, "terminal prompt lifecycle persistence failed",
		)
	}
	if err := d.transitionDelivery(ctx, attemptID, fence, store.PromptDeliveryNotRequested, store.PromptDeliveryValidating, "validate-workspace", ""); err != nil {
		return err
	}
	deltaRequest := harnessv2.CreateWorkspaceDeltaRequest{
		Protocol: harnessv2.ProtocolVersion,
		Metadata: mutationMetadata(runtimeFence, task, "workspace-delta", true, time.Now().UTC().Add(30*time.Second)),
		DeltaID:  harnessv2.WorkspaceDeltaID("delta-" + task.Status.Execution.PromptID),
		Intent:   workspace.Intent, VerifiedBaseline: baseline, PromptSettlementDigest: settlementDigest,
		Limits: acpWorkspaceDeltaLimits(task),
	}
	if err := sealMutation(&deltaRequest.Metadata.RequestDigest, deltaRequest); err != nil {
		return err
	}
	delta, err := runtimeClient.CreateWorkspaceDelta(runtimeCtx, createRequest.RuntimeSessionID, deltaRequest)
	if err != nil {
		httpStatus, code, kind := 0, harnessv2.ErrorCode(""), harnessv2.ClientErrorKind("")
		if clientErr, ok := errors.AsType[*harnessv2.ClientError](err); ok {
			httpStatus, code, kind = clientErr.StatusCode, clientErr.Code, clientErr.Kind
		}
		logf.FromContext(ctx).Info(
			"ACP workspace delta failed",
			"status", httpStatus,
			"code", code,
			"kind", kind,
			"serverMessage", boundedRuntimeSessionServerMessage(err),
		)
		if saveErr := d.publishTaskResultReference(ctx, task); saveErr != nil {
			return saveErr
		}
		if transitionErr := d.transitionAttempt(ctx, attemptID, fence, store.PromptExecutionSettling, store.PromptExecutionSucceeded, acpSucceededOperation, nil); transitionErr != nil {
			return transitionErr
		}
		if patchErr := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
			status.State = corev1alpha1.TaskExecutionStateSucceeded
			status.Outcome = corev1alpha1.TaskExecutionOutcomeSucceeded
			status.LastTransitionTime = nowMeta()
		}); patchErr != nil {
			return patchErr
		}
		recordACPPromptOutcome(ctx, acpPromptOutcomeSucceeded)
		promptTrace.End(nil)
		validationMessage := acpWorkspaceValidationFailureMessage(err)
		if transitionErr := d.transitionDelivery(ctx, attemptID, fence, store.PromptDeliveryValidating, store.PromptDeliveryConflict, "workspace-validation-failed", validationMessage); transitionErr != nil {
			return transitionErr
		}
		status := corev1alpha1.TaskDeliveryStatus{
			State: corev1alpha1.TaskDeliveryStateDeliveryConflict, Outcome: corev1alpha1.TaskDeliveryOutcomeDeliveryConflict,
			Reason: "WorkspaceValidationFailed", Message: validationMessage, LastTransitionTime: nowMeta(),
		}
		_ = d.patchDeliveryStatus(ctx, task, status)
		_ = cleanupRuntimeSession("workspace_validation_failed")
		if sessionExecution != nil {
			d.forgetRuntimeSessionBinding(sessionExecution.Binding.SessionUID)
		}
		return d.failTaskForDelivery(ctx, task, status, status.Message)
	}
	runtimePublicationFinalizationRequired = delta.Delta.State == harnessv2.WorkspaceDeltaPrepared
	if err := d.publishTaskResultReference(ctx, task); err != nil {
		return err
	}
	// ACP settlement is authoritative for execution. Delivery may still fail or
	// remain ambiguous, but it must never rewrite a successfully settled prompt
	// into a generic execution failure.
	if err := d.transitionAttempt(ctx, attemptID, fence, store.PromptExecutionSettling, store.PromptExecutionSucceeded, acpSucceededOperation, nil); err != nil {
		return err
	}
	if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
		status.State = corev1alpha1.TaskExecutionStateSucceeded
		status.Outcome = corev1alpha1.TaskExecutionOutcomeSucceeded
		status.LastTransitionTime = nowMeta()
	}); err != nil {
		return err
	}
	recordACPPromptOutcome(ctx, acpPromptOutcomeSucceeded)
	promptTrace.End(nil)

	var deliveryStatus corev1alpha1.TaskDeliveryStatus
	publicationID := ""
	switch delta.Delta.State {
	case harnessv2.WorkspaceDeltaNoChange:
		terminalDelivery := store.PromptDeliveryNoChange
		deliveryStatus = corev1alpha1.TaskDeliveryStatus{
			State: corev1alpha1.TaskDeliveryStateNoChange, Outcome: corev1alpha1.TaskDeliveryOutcomeNoChange,
			StartingSHA: baseline.Revision, LastTransitionTime: nowMeta(),
		}
		if workspace.Intent == harnessv2.WorkspaceIntentRead {
			terminalDelivery = store.PromptDeliveryReadValidated
			deliveryStatus.State = corev1alpha1.TaskDeliveryStateReadValidated
			deliveryStatus.Outcome = corev1alpha1.TaskDeliveryOutcomeReadValidated
		}
		if err := d.transitionDelivery(ctx, attemptID, fence, store.PromptDeliveryValidating, terminalDelivery, "workspace-validated", ""); err != nil {
			return err
		}
	case harnessv2.WorkspaceDeltaReadOnlyModified:
		if err := d.transitionDelivery(ctx, attemptID, fence, store.PromptDeliveryValidating, store.PromptDeliveryReadOnlyWorkspaceModified, "read-only-workspace-modified", "read-only workspace was modified"); err != nil {
			return err
		}
		deliveryStatus = corev1alpha1.TaskDeliveryStatus{
			State: corev1alpha1.TaskDeliveryStateReadOnlyWorkspaceModified, Outcome: corev1alpha1.TaskDeliveryOutcomeReadOnlyWorkspaceModified,
			Reason: "ReadOnlyWorkspaceModified", Message: "read-only workspace was modified", StartingSHA: baseline.Revision,
			LastTransitionTime: nowMeta(),
		}
		if err := d.patchDeliveryStatus(ctx, task, deliveryStatus); err != nil {
			return err
		}
		_ = cleanupRuntimeSession("read_only_workspace_modified")
		if err := d.finalizeTaskSessionResult(
			ctx, task, fence, sessionExecution, resultText, "", corev1alpha1.TaskPhaseFailed, deliveryStatus,
		); err != nil {
			return err
		}
		if sessionExecution != nil {
			d.forgetRuntimeSessionBinding(sessionExecution.Binding.SessionUID)
		}
		return d.failTaskForDelivery(ctx, task, deliveryStatus, deliveryStatus.Message)
	case harnessv2.WorkspaceDeltaPrepared:
		publicationResult, publicationErr := d.publishWorkspaceDelta(runtimeCtx, task, attemptID, fence, baseline, delta.Delta, sessionExecution)
		publicationID = publicationResult.PublicationID
		deliveryStatus = publicationResult.Status
		if deliveryStatus.State != "" {
			if err := d.patchDeliveryStatus(ctx, task, deliveryStatus); err != nil {
				return err
			}
		}
		if publicationErr != nil {
			var deliveryErr *acpDeliveryError
			if errors.As(publicationErr, &deliveryErr) && deliveryStatus.State == "" {
				deliveryStatus = corev1alpha1.TaskDeliveryStatus{State: deliveryErr.state, Outcome: deliveryErr.outcome, Reason: deliveryErr.reason, Message: deliveryErr.message, LastTransitionTime: nowMeta()}
			}
			if deliveryStatus.State == "" {
				deliveryStatus = corev1alpha1.TaskDeliveryStatus{
					State: corev1alpha1.TaskDeliveryStateDeliveryConflict, Outcome: corev1alpha1.TaskDeliveryOutcomeDeliveryConflict,
					Reason: "PublicationFailed", Message: publicationErr.Error(), LastTransitionTime: nowMeta(),
				}
			}
			if err := d.terminalizeDeliveryError(ctx, attemptID, fence, deliveryStatus); err != nil {
				return err
			}
			if err := d.patchDeliveryStatus(ctx, task, deliveryStatus); err != nil {
				return err
			}
			projectionPhase := corev1alpha1.TaskPhaseFailed
			if errors.Is(publicationErr, errACPPublicationCancelled) {
				projectionPhase = corev1alpha1.TaskPhaseCancelled
			}
			var finalization runtimeSessionPublicationFinalization
			var finalizationErr error
			if publicationID != "" {
				finalization, finalizationErr = d.runtimeSessionPublicationFinalization(ctx, publicationID, delta.Delta.DeltaID)
			} else {
				finalization, finalizationErr = runtimeSessionDeltaAbandonmentFinalization(task, delta.Delta.DeltaID, deliveryStatus)
			}
			if finalizationErr != nil {
				return finalizationErr
			}
			runtimePublicationFinalization = &finalization
			if err := finalizePreparedRuntimeSession(); err != nil {
				return err
			}
			if err := d.finalizeTaskSessionResult(
				ctx, task, fence, sessionExecution, resultText, publicationID, projectionPhase, deliveryStatus,
			); err != nil {
				return err
			}
			if cleanupErr := cleanupRuntimeSession("write_task_finalized"); cleanupErr != nil {
				return cleanupErr
			}
			if sessionExecution != nil {
				d.forgetRuntimeSessionBinding(sessionExecution.Binding.SessionUID)
			}
			if errors.Is(publicationErr, errACPPublicationCancelled) {
				return d.cancelTaskAfterExecution(ctx, task, deliveryStatus, "publication cancelled before push")
			}
			if errors.As(publicationErr, &deliveryErr) {
				return d.failTaskForDelivery(ctx, task, deliveryStatus, deliveryErr.message)
			}
			return publicationErr
		}
	default:
		return fmt.Errorf("unsupported workspace delta state %q", delta.Delta.State)
	}

	if err := d.patchDeliveryStatus(ctx, task, deliveryStatus); err != nil {
		return err
	}
	// Write sessions are always recreated from the independently verified remote
	// branch. Task-scoped sessions are also deleted after finalization.
	if workspace.Intent == harnessv2.WorkspaceIntentWrite && publicationID != "" {
		finalization, finalizationErr := d.runtimeSessionPublicationFinalization(ctx, publicationID, delta.Delta.DeltaID)
		if finalizationErr != nil {
			return finalizationErr
		}
		runtimePublicationFinalization = &finalization
	}
	if err := finalizePreparedRuntimeSession(); err != nil {
		return err
	}
	if err := d.finalizeTaskSessionResult(
		ctx, task, fence, sessionExecution, resultText, publicationID, corev1alpha1.TaskPhaseSucceeded, deliveryStatus,
	); err != nil {
		return err
	}
	if sessionExecution == nil || workspace.Intent == harnessv2.WorkspaceIntentWrite {
		if cleanupErr := cleanupRuntimeSession("task_finalized"); cleanupErr != nil {
			return cleanupErr
		}
		if sessionExecution != nil {
			d.forgetRuntimeSessionBinding(sessionExecution.Binding.SessionUID)
		}
	}
	return d.completeSuccessWithDelivery(ctx, task, deliveryStatus, "ACP task completed")
}

func saveACPPlanUpdateWithRetry(
	ctx context.Context,
	planStore store.PlanStore,
	namespace,
	taskName string,
	plan *store.PlanState,
) error {
	if err := planStore.SavePlan(ctx, namespace, taskName, plan); err != nil {
		if retryErr := planStore.SavePlan(ctx, namespace, taskName, plan); retryErr != nil {
			return errors.Join(
				fmt.Errorf("save ACP plan update: %w", err),
				fmt.Errorf("retry ACP plan update: %w", retryErr),
			)
		}
	}
	return nil
}

func reconcileACPPlanUpdateAfterJournal(
	ctx context.Context,
	planStore store.PlanStore,
	namespace,
	taskName string,
	plan *store.PlanState,
	planErr,
	journalErr error,
) error {
	if planErr == nil || journalErr != nil {
		return planErr
	}
	if err := planStore.SavePlan(ctx, namespace, taskName, plan); err != nil {
		return errors.Join(planErr, fmt.Errorf("reconcile ACP plan update after journal append: %w", err))
	}
	return nil
}

type acpExecutionUpdatePersistenceError struct {
	err        error
	journalErr error
	planErr    error
}

const acpExecutionEventPersistenceFailureReason = "ExecutionEventPersistenceFailed"

func (e *acpExecutionUpdatePersistenceError) Error() string {
	if e == nil || e.err == nil {
		return "ACP execution update persistence failed"
	}
	return e.err.Error()
}

func (e *acpExecutionUpdatePersistenceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *acpExecutionUpdatePersistenceError) journalFailed() bool {
	return e != nil && e.journalErr != nil
}

func acpUpdatePersistenceError(journalErr, planErr error) error {
	persistenceErr := &acpExecutionUpdatePersistenceError{}
	var failures []error
	if journalErr != nil {
		persistenceErr.journalErr = fmt.Errorf("persist ACP execution update: %w", journalErr)
		failures = append(failures, persistenceErr.journalErr)
	}
	if planErr != nil {
		persistenceErr.planErr = fmt.Errorf("persist ACP plan update: %w", planErr)
		failures = append(failures, persistenceErr.planErr)
	}
	if len(failures) == 0 {
		return nil
	}
	persistenceErr.err = errors.Join(failures...)
	return persistenceErr
}

func assistantTranscriptForPersistence(streamed string, overflow bool) (string, bool) {
	if overflow {
		return "", true
	}
	return streamed, false
}

func completedAssistantTranscriptForPersistence(streamed string, overflow bool, result string) (string, bool) {
	transcript, omitted := assistantTranscriptForPersistence(streamed, overflow)
	if transcript == "" && !omitted {
		transcript = result
	}
	return transcript, omitted
}

func completedPromptResultText(terminal *harnessv2.Event, streamed string, streamedOverflow bool) (string, error) {
	var terminalText strings.Builder
	if terminal != nil && terminal.Completed != nil {
		for _, block := range terminal.Completed.Result.Content {
			if block.Type == harnessv2.ContentBlockText {
				terminalText.WriteString(block.Text)
			}
		}
	}
	if result := strings.TrimSpace(terminalText.String()); result != "" {
		return result, nil
	}
	if streamedOverflow {
		return "", fmt.Errorf("assistant updates exceed the negotiated terminal result limit and the terminal result is empty")
	}
	return strings.TrimSpace(streamed), nil
}

func acpWorkspaceDeltaLimits(task *corev1alpha1.Task) harnessv2.WorkspaceDeltaLimits {
	limits := harnessv2.WorkspaceDeltaLimits{MaxBytes: 100 << 20, MaxEntries: 100_000}
	if task == nil || task.Spec.Workspace == nil {
		return limits
	}
	if task.Spec.Workspace.MaxChangedFiles != nil && *task.Spec.Workspace.MaxChangedFiles > 0 {
		limits.MaxChangedFiles = uint32(*task.Spec.Workspace.MaxChangedFiles)
	}
	limits.AllowedPaths = append([]string(nil), task.Spec.Workspace.AllowedPaths...)
	limits.DenyRepositoryControlPaths = task.Spec.Workspace.DenyRepositoryControlPaths
	limits.RejectBinaryFiles = task.Spec.Workspace.RejectBinaryFiles
	limits.RejectSecretLikeContent = task.Spec.Workspace.RejectSecretLikeContent
	return limits
}

type runtimeSessionPublicationFinalization struct {
	WorkspaceDeltaID      harnessv2.WorkspaceDeltaID
	PublicationID         string
	PublicationGeneration uint64
	PublicationVersion    uint64
	TerminalState         harnessv2.PublicationTerminalState
	TerminalReceiptDigest string
}

func publicationTerminalState(state store.PublicationState) (harnessv2.PublicationTerminalState, error) {
	switch state {
	case store.PublicationVerifiedExact:
		return harnessv2.PublicationTerminalVerifiedExact, nil
	case store.PublicationDeliveredSuperseded:
		return harnessv2.PublicationTerminalDeliveredSuperseded, nil
	case store.PublicationCancelledBeforePublish:
		return harnessv2.PublicationTerminalCancelledBeforePublish, nil
	case store.PublicationDeliveryConflict:
		return harnessv2.PublicationTerminalDeliveryConflict, nil
	case store.PublicationCredentialBlocked:
		return harnessv2.PublicationTerminalCredentialBlocked, nil
	case store.PublicationPreparationFailed:
		return harnessv2.PublicationTerminalPreparationFailed, nil
	case store.PublicationOutcomeUnknown:
		return harnessv2.PublicationTerminalOutcomeUnknown, nil
	default:
		return "", fmt.Errorf("publication state %q is not terminal", state)
	}
}

func runtimeSessionDeltaAbandonmentFinalization(
	task *corev1alpha1.Task,
	deltaID harnessv2.WorkspaceDeltaID,
	delivery corev1alpha1.TaskDeliveryStatus,
) (runtimeSessionPublicationFinalization, error) {
	return runtimeSessionDeltaAbandonmentFinalizationForTaskUID(task, task.UID, deltaID, delivery)
}

func runtimeSessionDeltaAbandonmentFinalizationForTaskUID(
	task *corev1alpha1.Task,
	taskUID types.UID,
	deltaID harnessv2.WorkspaceDeltaID,
	delivery corev1alpha1.TaskDeliveryStatus,
) (runtimeSessionPublicationFinalization, error) {
	if task == nil || task.Status.Execution == nil || taskUID == "" {
		return runtimeSessionPublicationFinalization{}, fmt.Errorf("runtime session delta abandonment requires explicit Task identity")
	}
	terminal := harnessv2.PublicationTerminalDeliveryConflict
	switch delivery.State {
	case corev1alpha1.TaskDeliveryStateCredentialBlocked:
		terminal = harnessv2.PublicationTerminalCredentialBlocked
	case corev1alpha1.TaskDeliveryStateCancelledBeforePublish:
		terminal = harnessv2.PublicationTerminalCancelledBeforePublish
	case corev1alpha1.TaskDeliveryStatePublicationOutcomeUnknown:
		terminal = harnessv2.PublicationTerminalOutcomeUnknown
	}
	syntheticPublicationID := publicationIDForTaskUID(task, taskUID)
	digest, err := acpDomainDigest("runtime-session-delta-abandonment-receipt", map[string]any{
		"taskUID": taskUID, "attempt": task.Status.Execution.Attempt, "deltaID": deltaID,
		"publicationID": syntheticPublicationID, "terminal": terminal, "delivery": delivery,
	})
	if err != nil {
		return runtimeSessionPublicationFinalization{}, err
	}
	return runtimeSessionPublicationFinalization{
		WorkspaceDeltaID: deltaID, PublicationID: syntheticPublicationID,
		PublicationGeneration: 1, PublicationVersion: 1,
		TerminalState: terminal, TerminalReceiptDigest: digest,
	}, nil
}

func (d *ACPDispatcher) runtimeSessionPublicationFinalization(
	ctx context.Context, publicationID string, deltaID harnessv2.WorkspaceDeltaID,
) (runtimeSessionPublicationFinalization, error) {
	publication, err := d.Store.GetPublication(ctx, publicationID)
	if err != nil {
		return runtimeSessionPublicationFinalization{}, err
	}
	terminalState, err := publicationTerminalState(publication.State)
	if err != nil {
		return runtimeSessionPublicationFinalization{}, err
	}
	if publication.Generation < 1 || publication.Version < 1 {
		return runtimeSessionPublicationFinalization{}, fmt.Errorf("publication generation and version must be positive")
	}
	receipt := store.PublicationReceipt{
		PublicationID: publication.ID, Generation: publication.Generation, State: publication.State,
		Prepared: publication.PreparedReceipt, Publish: publication.PublishReceipt,
		Verification: publication.VerificationReceipt, PullRequest: publication.PullRequestReceipt,
	}
	digest, err := acpDomainDigest("runtime-session-publication-finalization-receipt", map[string]any{
		"publication": receipt, "version": publication.Version,
	})
	if err != nil {
		return runtimeSessionPublicationFinalization{}, err
	}
	return runtimeSessionPublicationFinalization{
		WorkspaceDeltaID: deltaID, PublicationID: publication.ID,
		PublicationGeneration: uint64(publication.Generation), PublicationVersion: uint64(publication.Version),
		TerminalState: terminalState, TerminalReceiptDigest: digest,
	}, nil
}

func (d *ACPDispatcher) finalizeRuntimeSessionPublication(
	ctx context.Context, runtimeClient *harnessv2.Client, sessionID harnessv2.RuntimeSessionID, task *corev1alpha1.Task,
	runtimeFence harnessv2.Fence, finalization runtimeSessionPublicationFinalization,
) error {
	return d.finalizeRuntimeSessionPublicationForTaskUID(ctx, runtimeClient, sessionID, task, task.UID, runtimeFence, finalization)
}

func (d *ACPDispatcher) finalizeRuntimeSessionPublicationForTaskUID(
	ctx context.Context, runtimeClient *harnessv2.Client, sessionID harnessv2.RuntimeSessionID, task *corev1alpha1.Task,
	taskUID types.UID, runtimeFence harnessv2.Fence, finalization runtimeSessionPublicationFinalization,
) error {
	if runtimeClient == nil {
		return fmt.Errorf("runtime client is required")
	}
	if task == nil || task.Status.Execution == nil || taskUID == "" {
		return fmt.Errorf("finalize RuntimeSession publication requires explicit Task identity")
	}
	finalizeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	now := time.Now().UTC()
	request := harnessv2.FinalizeRuntimeSessionPublicationRequest{
		Protocol: harnessv2.ProtocolVersion,
		Metadata: mutationMetadataForTaskUID(
			runtimeFence, task, taskUID, "fp-"+strconv.FormatInt(now.UnixNano(), 36), true, now.Add(30*time.Second),
		),
		WorkspaceDeltaID: finalization.WorkspaceDeltaID, PublicationID: finalization.PublicationID,
		PublicationGeneration: finalization.PublicationGeneration, PublicationVersion: finalization.PublicationVersion,
		TerminalState: finalization.TerminalState, TerminalReceiptDigest: finalization.TerminalReceiptDigest,
	}
	if err := sealMutation(&request.Metadata.RequestDigest, request); err != nil {
		return fmt.Errorf("seal RuntimeSession publication finalization: %w", err)
	}
	if _, err := runtimeClient.FinalizeRuntimeSessionPublication(finalizeCtx, sessionID, request); err != nil {
		var clientErr *harnessv2.ClientError
		if errors.As(err, &clientErr) && (clientErr.StatusCode == http.StatusNotFound || clientErr.StatusCode == http.StatusGone) {
			return nil
		}
		return fmt.Errorf("finalize RuntimeSession publication: %w", err)
	}
	return nil
}

func (d *ACPDispatcher) deleteRuntimeSession(
	ctx context.Context,
	runtimeClient *harnessv2.Client,
	sessionID harnessv2.RuntimeSessionID,
	task *corev1alpha1.Task,
	runtimeFence harnessv2.Fence,
	reason string,
) error {
	return d.deleteRuntimeSessionForTaskUID(ctx, runtimeClient, sessionID, task, task.UID, runtimeFence, reason)
}

func (d *ACPDispatcher) deleteRuntimeSessionForTaskUID(
	ctx context.Context,
	runtimeClient *harnessv2.Client,
	sessionID harnessv2.RuntimeSessionID,
	task *corev1alpha1.Task,
	taskUID types.UID,
	runtimeFence harnessv2.Fence,
	reason string,
) error {
	now := time.Now().UTC()
	request, err := newDeleteRuntimeSessionRequestForTaskUID(task, taskUID, runtimeFence, reason, now.Add(30*time.Second))
	if err != nil {
		return err
	}
	return d.deleteRuntimeSessionRequest(ctx, runtimeClient, sessionID, request)
}

func newDeleteRuntimeSessionRequestForTaskUID(
	task *corev1alpha1.Task,
	taskUID types.UID,
	runtimeFence harnessv2.Fence,
	reason string,
	expiresAt time.Time,
) (harnessv2.DeleteRuntimeSessionRequest, error) {
	if task == nil || task.Status.Execution == nil || taskUID == "" {
		return harnessv2.DeleteRuntimeSessionRequest{}, fmt.Errorf("delete RuntimeSession requires explicit Task identity")
	}
	now := time.Now().UTC()
	request := harnessv2.DeleteRuntimeSessionRequest{
		Protocol: harnessv2.ProtocolVersion,
		Metadata: mutationMetadataForTaskUID(
			runtimeFence, task, taskUID, "ds-"+strconv.FormatInt(now.UnixNano(), 36), false, expiresAt.UTC(),
		),
		Reason: reason,
	}
	if err := sealMutation(&request.Metadata.RequestDigest, request); err != nil {
		return harnessv2.DeleteRuntimeSessionRequest{}, fmt.Errorf("seal RuntimeSession deletion: %w", err)
	}
	return request, nil
}

func (d *ACPDispatcher) deleteRuntimeSessionRequest(
	ctx context.Context,
	runtimeClient *harnessv2.Client,
	sessionID harnessv2.RuntimeSessionID,
	request harnessv2.DeleteRuntimeSessionRequest,
) error {
	if runtimeClient == nil {
		return fmt.Errorf("runtime client is required")
	}
	deleteCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := runtimeClient.DeleteRuntimeSession(deleteCtx, sessionID, request); err != nil {
		var clientErr *harnessv2.ClientError
		if errors.As(err, &clientErr) && (clientErr.StatusCode == http.StatusNotFound || clientErr.StatusCode == http.StatusGone) {
			return nil
		}
		return fmt.Errorf("delete RuntimeSession: %w", err)
	}
	return nil
}

func runtimeSessionStatusForUID(
	ctx context.Context,
	runtimeClient *harnessv2.Client,
	sessionUID harnessv2.RuntimeSessionUID,
) (*harnessv2.StatusResponse, *harnessv2.RuntimeSessionStatus, error) {
	if runtimeClient == nil {
		return nil, nil, fmt.Errorf("runtime client is required")
	}
	statusCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	status, err := runtimeClient.Status(statusCtx)
	if err != nil {
		return nil, nil, fmt.Errorf("read RuntimeSession status: %w", err)
	}
	for i := range status.Sessions {
		if status.Sessions[i].RuntimeSessionUID == sessionUID {
			observed := status.Sessions[i]
			return status, &observed, nil
		}
	}
	return status, nil, nil
}

// waitForRuntimeSessionAdmission resolves an ambiguous create-session write:
// the request may still be in flight at the runtime, so an absent session is
// polled for until the timeout.
func waitForRuntimeSessionAdmission(
	ctx context.Context,
	runtimeClient *harnessv2.Client,
	sessionID harnessv2.RuntimeSessionID,
	sessionUID harnessv2.RuntimeSessionUID,
	generation uint64,
	timeout time.Duration,
) (bool, error) {
	return waitForRuntimeSessionAdmissionState(ctx, runtimeClient, sessionID, sessionUID, generation, timeout, false)
}

// errRuntimeSessionAdoptionInconclusive reports that the runtime did not
// confirm, within the adoption window, whether the session an earlier send
// created is admissible: status stayed unavailable or the session was still
// creating when the window closed. Callers must not read it as absence.
var errRuntimeSessionAdoptionInconclusive = errors.New("RuntimeSession adoption is inconclusive")

// reconcileRuntimeSessionCreateDigestConflict resolves a digest_conflict answer
// to create-session. The runtime already holds an operation record for this
// exact create identity, so an earlier send of the same attempt was processed
// there: the session is adopted when the runtime reports the exact generation
// admissible, waited for while it is still creating, and reported absent
// without polling when the record is a deletion tombstone, because a
// tombstoned generation can never reappear. (false, nil) therefore means
// confirmed absence; an unfinished window returns
// errRuntimeSessionAdoptionInconclusive.
func reconcileRuntimeSessionCreateDigestConflict(
	ctx context.Context,
	runtimeClient *harnessv2.Client,
	sessionID harnessv2.RuntimeSessionID,
	sessionUID harnessv2.RuntimeSessionUID,
	generation uint64,
	timeout time.Duration,
) (bool, error) {
	return waitForRuntimeSessionAdmissionState(ctx, runtimeClient, sessionID, sessionUID, generation, timeout, true)
}

func waitForRuntimeSessionAdmissionState(
	ctx context.Context,
	runtimeClient *harnessv2.Client,
	sessionID harnessv2.RuntimeSessionID,
	sessionUID harnessv2.RuntimeSessionUID,
	generation uint64,
	timeout time.Duration,
	absentIsFinal bool,
) (bool, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastStatusErr error
	creationDeadlineSet := false
	for {
		_, observed, err := runtimeSessionStatusForUID(waitCtx, runtimeClient, sessionUID)
		if err != nil {
			lastStatusErr = err
		} else if observed == nil && absentIsFinal {
			return false, nil
		} else if observed != nil {
			if observed.RuntimeSessionID != sessionID || observed.Generation != generation {
				return false, fmt.Errorf("%w: RuntimeSession status resolved to a different generation", store.ErrConflict)
			}
			if observed.State.CanAdmitPrompt() {
				return true, nil
			}
			if observed.State == harnessv2.RuntimeSessionStateCreating {
				if absentIsFinal && !creationDeadlineSet {
					// Creating starts when the runtime records the original
					// create, not when this reconcile observes its replay.
					creationCtx, cancelCreation := context.WithDeadline(waitCtx, observed.LastTransitionAt.Add(timeout))
					defer cancelCreation()
					waitCtx = creationCtx
					creationDeadlineSet = true
				}
				lastStatusErr = nil
				select {
				case <-waitCtx.Done():
					if absentIsFinal {
						return false, fmt.Errorf("%w: RuntimeSession was still creating when the adoption window closed", errRuntimeSessionAdoptionInconclusive)
					}
					return false, nil
				case <-ticker.C:
					continue
				}
			}
			return false, fmt.Errorf("%w: RuntimeSession creation settled in non-admissible state %s", store.ErrConflict, observed.State)
		}
		select {
		case <-waitCtx.Done():
			if absentIsFinal {
				if lastStatusErr != nil {
					return false, fmt.Errorf("%w: %w", errRuntimeSessionAdoptionInconclusive, lastStatusErr)
				}
				return false, fmt.Errorf("%w: RuntimeSession status was not observed before the adoption window closed", errRuntimeSessionAdoptionInconclusive)
			}
			if lastStatusErr != nil {
				return false, lastStatusErr
			}
			return false, nil
		case <-ticker.C:
		}
	}
}

func (d *ACPDispatcher) reconcilePlannedRuntimeSession(
	ctx context.Context,
	runtimeClient *harnessv2.Client,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	session *acpTaskSession,
	runtimeFence *harnessv2.Fence,
) (bool, error) {
	if session == nil || runtimeFence == nil {
		return false, nil
	}
	status, observed, err := runtimeSessionStatusForUID(ctx, runtimeClient, runtimeFence.RuntimeSessionUID)
	if err != nil {
		if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
			ctx, task, attemptID, fence, err, &session.Binding,
		); requeueErr != nil {
			return false, requeueErr
		}
		session.requeued = true
		return true, nil
	}
	expectedPoolFence := *runtimeFence
	expectedPoolFence.RuntimeSessionUID = ""
	expectedPoolFence.RuntimeSessionGeneration = 0
	if mismatch := harnessv2.CompareFence(expectedPoolFence, status.Fence, false); mismatch != harnessv2.FenceMatch {
		fenceErr := fmt.Errorf("%w: RuntimeSession status fence mismatch: %s", store.ErrConflict, mismatch)
		if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
			ctx, task, attemptID, fence, fenceErr, &session.Binding,
		); requeueErr != nil {
			return false, errors.Join(fenceErr, requeueErr)
		}
		session.requeued = true
		return true, nil
	}
	expectedID := harnessv2.RuntimeSessionID(runtimeSessionID(*runtimeFence))
	if observed == nil {
		if !session.Reused {
			return false, nil
		}
		if session.Binding.Generation >= maxControllerRuntimeSessionGeneration {
			return false, store.ValidationErrorf("ACP runtime session generation is exhausted")
		}
		nextGeneration := max(session.Binding.Generation+1, uint64(session.LeaseGeneration))
		if nextGeneration > maxControllerRuntimeSessionGeneration {
			return false, store.ValidationErrorf("ACP runtime session generation is exhausted")
		}
		session.Binding.Generation = nextGeneration
		session.Binding.WorkspaceDigest = ""
		session.Binding.RecreationRequired = true
		session.Reused = false
		runtimeFence.RuntimeSessionGeneration = nextGeneration
		d.setRuntimeSessionBinding(session.Binding)
		if err := d.patchExecution(ctx, task, func(execution *corev1alpha1.TaskExecutionStatus) {
			applyRuntimeSessionBindingToExecution(execution, &session.Binding)
			execution.LastTransitionTime = nowMeta()
		}); err != nil {
			return false, err
		}
		if err := d.requeuePreSubmissionTaskWithRuntimeBinding(
			ctx, task, attemptID, fence, errors.New("missing reusable RuntimeSession requires recreation"), &session.Binding,
		); err != nil {
			return false, err
		}
		session.requeued = true
		return true, nil
	}
	if observed.RuntimeSessionID == expectedID && observed.Generation == runtimeFence.RuntimeSessionGeneration {
		if observed.State.CanAdmitPrompt() {
			if !session.Reused {
				session.Reused = true
				session.Binding.RecreationRequired = false
				d.setRuntimeSessionBinding(session.Binding)
				if err := d.patchExecution(ctx, task, func(execution *corev1alpha1.TaskExecutionStatus) {
					applyRuntimeSessionBindingToExecution(execution, &session.Binding)
					execution.LastTransitionTime = nowMeta()
				}); err != nil {
					if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
						ctx, task, attemptID, fence, err, &session.Binding,
					); requeueErr != nil {
						return false, errors.Join(err, requeueErr)
					}
					session.requeued = true
					return true, nil
				}
			}
			return false, nil
		}
		if observed.State == harnessv2.RuntimeSessionStateCreating {
			if err := d.requeuePreSubmissionTaskWithRuntimeBinding(
				ctx, task, attemptID, fence, errors.New("RuntimeSession creation is still settling"), &session.Binding,
			); err != nil {
				return false, err
			}
			session.requeued = true
			return true, nil
		}
	}
	if observed.Generation >= maxControllerRuntimeSessionGeneration {
		return false, store.ValidationErrorf("ACP runtime session generation is exhausted")
	}
	nextGeneration := max(session.Binding.Generation, observed.Generation+1, uint64(session.LeaseGeneration))
	if session.Reused {
		if session.Binding.Generation >= maxControllerRuntimeSessionGeneration {
			return false, store.ValidationErrorf("ACP runtime session generation is exhausted")
		}
		nextGeneration = max(nextGeneration, session.Binding.Generation+1)
	}
	if nextGeneration > maxControllerRuntimeSessionGeneration {
		return false, store.ValidationErrorf("ACP runtime session generation is exhausted")
	}
	session.Binding.Generation = nextGeneration
	session.Binding.WorkspaceDigest = ""
	session.Binding.RecreationRequired = true
	session.Reused = false
	runtimeFence.RuntimeSessionGeneration = nextGeneration
	d.setRuntimeSessionBinding(session.Binding)
	if err := d.patchExecution(ctx, task, func(execution *corev1alpha1.TaskExecutionStatus) {
		applyRuntimeSessionBindingToExecution(execution, &session.Binding)
		execution.LastTransitionTime = nowMeta()
	}); err != nil {
		return false, err
	}
	observedFence := expectedPoolFence
	observedFence.RuntimeSessionUID = observed.RuntimeSessionUID
	observedFence.RuntimeSessionGeneration = observed.Generation
	if err := d.deleteRuntimeSessionReconciled(
		context.WithoutCancel(ctx), runtimeClient, observed.RuntimeSessionID, task, observedFence, "replace_stale_runtime_session",
	); err != nil {
		if requeueErr := d.requeuePreSubmissionTaskWithRuntimeBinding(
			ctx, task, attemptID, fence, err, &session.Binding,
		); requeueErr != nil {
			return false, requeueErr
		}
		session.requeued = true
		return true, nil
	}
	if err := d.requeuePreSubmissionTaskWithRuntimeBinding(
		ctx, task, attemptID, fence, errors.New("stale RuntimeSession was retired before recreation"), &session.Binding,
	); err != nil {
		return false, err
	}
	session.requeued = true
	return true, nil
}

func (d *ACPDispatcher) reconcilePlannedTaskScopedRuntimeSession(
	ctx context.Context,
	runtimeClient *harnessv2.Client,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	runtimeFence harnessv2.Fence,
) (bool, bool, error) {
	status, observed, err := runtimeSessionStatusForUID(ctx, runtimeClient, runtimeFence.RuntimeSessionUID)
	if err != nil {
		if requeueErr := d.requeuePreSubmissionTask(ctx, task, attemptID, fence, err); requeueErr != nil {
			return false, false, requeueErr
		}
		return false, true, nil
	}
	expectedPoolFence := runtimeFence
	expectedPoolFence.RuntimeSessionUID = ""
	expectedPoolFence.RuntimeSessionGeneration = 0
	if mismatch := harnessv2.CompareFence(expectedPoolFence, status.Fence, false); mismatch != harnessv2.FenceMatch {
		fenceErr := fmt.Errorf("%w: RuntimeSession status fence mismatch: %s", store.ErrConflict, mismatch)
		if requeueErr := d.requeuePreSubmissionTask(ctx, task, attemptID, fence, fenceErr); requeueErr != nil {
			return false, false, errors.Join(fenceErr, requeueErr)
		}
		return false, true, nil
	}
	if observed == nil {
		return false, false, nil
	}
	expectedID := harnessv2.RuntimeSessionID(runtimeSessionID(runtimeFence))
	if observed.RuntimeSessionID == expectedID && observed.Generation == runtimeFence.RuntimeSessionGeneration {
		if observed.State.CanAdmitPrompt() {
			return true, false, nil
		}
		if observed.State == harnessv2.RuntimeSessionStateCreating {
			if err := d.requeuePreSubmissionTask(
				ctx, task, attemptID, fence, errors.New("task-scoped RuntimeSession creation is still settling"),
			); err != nil {
				return false, false, err
			}
			return false, true, nil
		}
	}
	observedFence := expectedPoolFence
	observedFence.RuntimeSessionUID = observed.RuntimeSessionUID
	observedFence.RuntimeSessionGeneration = observed.Generation
	if err := d.deleteRuntimeSessionReconciled(
		context.WithoutCancel(ctx), runtimeClient, observed.RuntimeSessionID, task, observedFence,
		"replace_stale_task_scoped_runtime_session",
	); err != nil {
		if requeueErr := d.requeuePreSubmissionTask(ctx, task, attemptID, fence, err); requeueErr != nil {
			return false, false, errors.Join(err, requeueErr)
		}
		return false, true, nil
	}
	if err := d.requeuePreSubmissionTask(
		ctx, task, attemptID, fence, errors.New("stale task-scoped RuntimeSession was retired before recreation"),
	); err != nil {
		return false, false, err
	}
	return false, true, nil
}

func taskScopedRuntimeSessionGeneration(attempt *store.PromptAttempt) (uint64, error) {
	if attempt == nil || attempt.ExecutionState != store.PromptExecutionReserved || attempt.Version < 1 {
		return 0, fmt.Errorf("reserved PromptAttempt is required for task-scoped RuntimeSession generation")
	}
	generation := uint64(attempt.Version)
	if generation > maxControllerRuntimeSessionGeneration {
		return 0, store.ValidationErrorf("task-scoped RuntimeSession generation is exhausted")
	}
	return generation, nil
}

func (d *ACPDispatcher) rotateExpiredSessionBoundRuntimeSessionCreation(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	session *acpTaskSession,
	runtimeFence *harnessv2.Fence,
) error {
	if session == nil || runtimeFence == nil {
		return fmt.Errorf("session-bound RuntimeSession creation rotation requires a Session binding")
	}
	if session.Binding.Generation >= maxControllerRuntimeSessionGeneration {
		return store.ValidationErrorf("ACP runtime session generation is exhausted")
	}
	nextGeneration := max(session.Binding.Generation+1, uint64(session.LeaseGeneration))
	if nextGeneration > maxControllerRuntimeSessionGeneration {
		return store.ValidationErrorf("ACP runtime session generation is exhausted")
	}
	session.Binding.Generation = nextGeneration
	session.Binding.WorkspaceDigest = ""
	session.Binding.RecreationRequired = true
	session.Reused = false
	runtimeFence.RuntimeSessionGeneration = nextGeneration
	d.setRuntimeSessionBinding(session.Binding)
	if err := d.patchExecution(ctx, task, func(execution *corev1alpha1.TaskExecutionStatus) {
		applyRuntimeSessionBindingToExecution(execution, &session.Binding)
		execution.LastTransitionTime = nowMeta()
	}); err != nil {
		return err
	}
	if err := d.requeuePreSubmissionTaskWithRuntimeBinding(
		ctx, task, attemptID, fence,
		errors.New("session-bound RuntimeSession creation authorization expired before submission"),
		&session.Binding,
	); err != nil {
		return err
	}
	session.requeued = true
	return nil
}

func (d *ACPDispatcher) deleteRuntimeSessionReconciled(
	ctx context.Context,
	runtimeClient *harnessv2.Client,
	sessionID harnessv2.RuntimeSessionID,
	task *corev1alpha1.Task,
	runtimeFence harnessv2.Fence,
	reason string,
) error {
	_, observed, err := runtimeSessionStatusForUID(ctx, runtimeClient, runtimeFence.RuntimeSessionUID)
	if err != nil {
		return err
	}
	if observed == nil || observed.RuntimeSessionID != sessionID || observed.Generation != runtimeFence.RuntimeSessionGeneration {
		return nil
	}
	if observed.State != harnessv2.RuntimeSessionStateDeleting {
		if err := d.deleteRuntimeSession(ctx, runtimeClient, sessionID, task, runtimeFence, reason); err == nil {
			return nil
		} else {
			deleteErr := err
			settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				_, current, statusErr := runtimeSessionStatusForUID(settleCtx, runtimeClient, runtimeFence.RuntimeSessionUID)
				if statusErr == nil && (current == nil || current.RuntimeSessionID != sessionID || current.Generation != runtimeFence.RuntimeSessionGeneration) {
					return nil
				}
				select {
				case <-settleCtx.Done():
					return deleteErr
				case <-ticker.C:
				}
			}
		}
	}

	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, current, statusErr := runtimeSessionStatusForUID(settleCtx, runtimeClient, runtimeFence.RuntimeSessionUID)
		if statusErr == nil && (current == nil || current.RuntimeSessionID != sessionID || current.Generation != runtimeFence.RuntimeSessionGeneration) {
			return nil
		}
		select {
		case <-settleCtx.Done():
			return fmt.Errorf("RuntimeSession deletion did not settle before retry")
		case <-ticker.C:
		}
	}
}

type acpRuntimePoolReservationIdentity struct {
	PoolKey           types.NamespacedName
	PoolUID           types.UID
	TaskUID           types.UID
	Attempt           int32
	ControllerEpoch   int64
	RuntimeInstanceID string
}

type acpRuntimePoolReservationLease struct {
	dispatcher *ACPDispatcher
	identity   acpRuntimePoolReservationIdentity

	mu     sync.Mutex
	held   bool
	cancel context.CancelFunc
}

func newACPRuntimePoolReservationLease(d *ACPDispatcher, identity *acpRuntimePoolReservationIdentity) *acpRuntimePoolReservationLease {
	if d == nil || identity == nil {
		return nil
	}
	return &acpRuntimePoolReservationLease{dispatcher: d, identity: *identity, held: true}
}

func (l *acpRuntimePoolReservationLease) startRenewal(ctx context.Context) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if !l.held || l.cancel != nil {
		l.mu.Unlock()
		return
	}
	renewCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	interval := max(l.dispatcher.effectiveReservationTTL()/3, time.Second)
	l.mu.Unlock()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				if err := l.renew(renewCtx); err != nil {
					if !errors.Is(err, context.Canceled) {
						logf.FromContext(renewCtx).Error(err, "ACP RuntimePool reservation renewal stopped", "namespace", l.identity.PoolKey.Namespace, "pool", l.identity.PoolKey.Name, "taskUID", l.identity.TaskUID, "attempt", l.identity.Attempt)
					}
					return
				}
			}
		}
	}()
}

func (l *acpRuntimePoolReservationLease) renew(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.held {
		return errACPRuntimePoolReservationLost
	}
	if _, err := l.dispatcher.renewRuntimePoolReservation(ctx, l.identity); err != nil {
		if errors.Is(err, errACPRuntimePoolReservationLost) {
			l.held = false
			if l.cancel != nil {
				l.cancel()
				l.cancel = nil
			}
		}
		return err
	}
	return nil
}

func (l *acpRuntimePoolReservationLease) setSlots(ctx context.Context, residentSlots int32) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.held {
		return errACPRuntimePoolReservationLost
	}
	if err := l.dispatcher.updateRuntimePoolReservationSlots(ctx, l.identity, residentSlots, 1); err != nil {
		if errors.Is(err, errACPRuntimePoolReservationLost) {
			l.held = false
		}
		return err
	}
	return nil
}

func (l *acpRuntimePoolReservationLease) release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
	if !l.held {
		return nil
	}
	if err := l.dispatcher.releaseRuntimePoolReservation(ctx, l.identity); err != nil {
		return err
	}
	l.held = false
	return nil
}

func (d *ACPDispatcher) effectiveReservationTTL() time.Duration {
	if d.ReservationTTL > 0 {
		return d.ReservationTTL
	}
	return DefaultACPRuntimePoolReservationTTL
}

func (d *ACPDispatcher) effectiveRateLimitRetryInterval() time.Duration {
	if d.RateLimitRetryInterval > 0 {
		return d.RateLimitRetryInterval
	}
	return DefaultACPRateLimitReconcileInterval
}

func (d *ACPDispatcher) claimRuntimePoolReservation(
	ctx context.Context,
	task *corev1alpha1.Task,
	poolName string,
	fence store.ControllerEpochFence,
	residentSlots int32,
) (*corev1alpha1.RuntimePool, *acpRuntimePoolReservationIdentity, error) {
	if err := d.AdmissionGate.Check(); err != nil {
		return nil, nil, err
	}
	const promptSlots int32 = 1
	if task == nil || task.Status.Execution == nil || task.UID == "" || task.Status.Execution.Attempt < 1 {
		return nil, nil, fmt.Errorf("task identity and execution attempt are required for RuntimePool reservation")
	}
	if residentSlots < 0 || residentSlots > 1 || promptSlots < 0 || promptSlots > 1 || residentSlots+promptSlots == 0 {
		return nil, nil, fmt.Errorf("RuntimePool reservation slots must be zero or one and claim at least one slot")
	}
	poolUID := types.UID(strings.TrimSpace(task.Status.Execution.RuntimePoolUID))
	if poolUID == "" {
		return nil, nil, fmt.Errorf("task RuntimePool UID is required for capacity reservation")
	}
	key := types.NamespacedName{Namespace: task.Namespace, Name: strings.TrimSpace(poolName)}
	identity := &acpRuntimePoolReservationIdentity{
		PoolKey: key, PoolUID: poolUID, TaskUID: task.UID, Attempt: task.Status.Execution.Attempt, ControllerEpoch: fence.Epoch,
	}
	var claimed *corev1alpha1.RuntimePool
	var unavailable error
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := d.AdmissionGate.Check(); err != nil {
			return err
		}
		unavailable = nil
		latest := &corev1alpha1.RuntimePool{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		now := time.Now().UTC()
		reservations, changed := activeRuntimePoolReservations(latest, fence.Epoch, now)
		latest.Status.Capacity.Reservations = reservations
		updateRuntimePoolReservationCounters(&latest.Status.Capacity)
		if string(latest.UID) != string(poolUID) {
			unavailable = fmt.Errorf("%w: frozen RuntimePool UID no longer exists", errACPRuntimePoolNotAdmitting)
			return updateRuntimePoolReservationStatusIfChanged(ctx, d.Client, latest, changed)
		}
		for i := range reservations {
			if runtimePoolReservationMatches(reservations[i], *identity) {
				identity.RuntimeInstanceID = reservations[i].RuntimeInstanceID
				expiresAt := metav1.NewTime(now.Add(d.effectiveReservationTTL()))
				if reservations[i].ExpiresAt.Time.Before(expiresAt.Time) {
					latest.Status.Capacity.Reservations[i].ExpiresAt = expiresAt
					changed = true
				}
				if err := updateRuntimePoolReservationStatusIfChanged(ctx, d.Client, latest, changed); err != nil {
					return err
				}
				claimed = latest.DeepCopy()
				return nil
			}
		}
		runtimeInstanceID, admissionErr := runtimePoolReservationAdmission(latest, reservations, fence.Epoch, residentSlots, promptSlots)
		if admissionErr != nil {
			unavailable = admissionErr
			return updateRuntimePoolReservationStatusIfChanged(ctx, d.Client, latest, changed)
		}
		identity.RuntimeInstanceID = runtimeInstanceID
		reservedAt := metav1.NewTime(now)
		latest.Status.Capacity.Reservations = append(latest.Status.Capacity.Reservations, corev1alpha1.RuntimePoolCapacityReservationStatus{
			PoolUID: string(poolUID), TaskUID: string(task.UID), Attempt: task.Status.Execution.Attempt,
			ControllerEpoch: fence.Epoch, RuntimeInstanceID: runtimeInstanceID,
			ResidentSlots: residentSlots, PromptSlots: promptSlots, ReservedAt: reservedAt,
			ExpiresAt: metav1.NewTime(now.Add(d.effectiveReservationTTL())),
		})
		sortRuntimePoolReservations(latest.Status.Capacity.Reservations)
		updateRuntimePoolReservationCounters(&latest.Status.Capacity)
		if err := d.Client.Status().Update(ctx, latest); err != nil {
			return err
		}
		claimed = latest.DeepCopy()
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if unavailable != nil {
		return nil, nil, unavailable
	}
	if claimed == nil || identity.RuntimeInstanceID == "" {
		return nil, nil, fmt.Errorf("%w: RuntimePool disappeared while reserving capacity", errACPRuntimePoolNotAdmitting)
	}
	return claimed, identity, nil
}

func runtimePoolReservationAdmission(
	pool *corev1alpha1.RuntimePool,
	reservations []corev1alpha1.RuntimePoolCapacityReservationStatus,
	controllerEpoch int64,
	residentSlots, promptSlots int32,
) (string, error) {
	active := pool.Status.ActiveInstance
	requiresNewSession := residentSlots > 0
	if pool.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleServing ||
		(requiresNewSession && pool.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionAccepting) ||
		active == nil || active.ControllerEpoch != controllerEpoch || strings.TrimSpace(active.RuntimeInstanceID) == "" {
		return "", fmt.Errorf("%w: RuntimePool is not eligible for exact-instance admission on the current controller epoch", errACPRuntimePoolNotAdmitting)
	}
	maxResident, maxPrompts := effectiveRuntimePoolCapacityLimits(pool)
	reservedResident, reservedPrompts := runtimePoolReservedSlots(reservations)
	if int64(pool.Status.Capacity.ResidentSessions)+reservedResident+int64(residentSlots) > maxResident ||
		int64(pool.Status.Capacity.RunningPrompts)+reservedPrompts+int64(promptSlots) > maxPrompts ||
		len(reservations) >= corev1alpha1.MaxRuntimePoolCapacityReservations {
		return "", fmt.Errorf("%w: RuntimePool has no unclaimed resident or prompt slot", errACPRuntimePoolAtCapacity)
	}
	return active.RuntimeInstanceID, nil
}

func (d *ACPDispatcher) renewRuntimePoolReservation(ctx context.Context, identity acpRuntimePoolReservationIdentity) (*corev1alpha1.RuntimePool, error) {
	var renewed *corev1alpha1.RuntimePool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.RuntimePool{}
		if err := d.Client.Get(ctx, identity.PoolKey, latest); err != nil {
			if apierrors.IsNotFound(err) {
				return errACPRuntimePoolReservationLost
			}
			return err
		}
		now := time.Now().UTC()
		reservations, changed := activeRuntimePoolReservations(latest, identity.ControllerEpoch, now)
		latest.Status.Capacity.Reservations = reservations
		found := false
		for i := range reservations {
			if !runtimePoolReservationMatches(reservations[i], identity) {
				continue
			}
			found = true
			latest.Status.Capacity.Reservations[i].ExpiresAt = metav1.NewTime(now.Add(d.effectiveReservationTTL()))
			changed = true
			break
		}
		updateRuntimePoolReservationCounters(&latest.Status.Capacity)
		if !found {
			if err := updateRuntimePoolReservationStatusIfChanged(ctx, d.Client, latest, changed); err != nil {
				return err
			}
			return errACPRuntimePoolReservationLost
		}
		if err := d.Client.Status().Update(ctx, latest); err != nil {
			return err
		}
		renewed = latest.DeepCopy()
		return nil
	})
	return renewed, err
}

func (d *ACPDispatcher) updateRuntimePoolReservationSlots(ctx context.Context, identity acpRuntimePoolReservationIdentity, residentSlots, promptSlots int32) error {
	if residentSlots < 0 || residentSlots > 1 || promptSlots < 0 || promptSlots > 1 || residentSlots+promptSlots == 0 {
		return fmt.Errorf("RuntimePool reservation slots must be zero or one and claim at least one slot")
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.RuntimePool{}
		if err := d.Client.Get(ctx, identity.PoolKey, latest); err != nil {
			if apierrors.IsNotFound(err) {
				return errACPRuntimePoolReservationLost
			}
			return err
		}
		reservations, changed := activeRuntimePoolReservations(latest, identity.ControllerEpoch, time.Now().UTC())
		latest.Status.Capacity.Reservations = reservations
		found := false
		for i := range reservations {
			if !runtimePoolReservationMatches(reservations[i], identity) {
				continue
			}
			found = true
			if reservations[i].ResidentSlots != residentSlots || reservations[i].PromptSlots != promptSlots {
				if residentSlots > reservations[i].ResidentSlots || promptSlots > reservations[i].PromptSlots {
					others := make([]corev1alpha1.RuntimePoolCapacityReservationStatus, 0, len(reservations)-1)
					others = append(others, reservations[:i]...)
					others = append(others, reservations[i+1:]...)
					runtimeInstanceID, admissionErr := runtimePoolReservationAdmission(
						latest, others, identity.ControllerEpoch, residentSlots, promptSlots,
					)
					if admissionErr != nil {
						if err := updateRuntimePoolReservationStatusIfChanged(ctx, d.Client, latest, changed); err != nil {
							return err
						}
						return admissionErr
					}
					if runtimeInstanceID != reservations[i].RuntimeInstanceID {
						if err := updateRuntimePoolReservationStatusIfChanged(ctx, d.Client, latest, changed); err != nil {
							return err
						}
						return errACPRuntimePoolReservationLost
					}
				}
				latest.Status.Capacity.Reservations[i].ResidentSlots = residentSlots
				latest.Status.Capacity.Reservations[i].PromptSlots = promptSlots
				changed = true
			}
			break
		}
		updateRuntimePoolReservationCounters(&latest.Status.Capacity)
		if !found {
			if err := updateRuntimePoolReservationStatusIfChanged(ctx, d.Client, latest, changed); err != nil {
				return err
			}
			return errACPRuntimePoolReservationLost
		}
		return updateRuntimePoolReservationStatusIfChanged(ctx, d.Client, latest, changed)
	})
}

func (d *ACPDispatcher) releaseRuntimePoolReservation(ctx context.Context, identity acpRuntimePoolReservationIdentity) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.RuntimePool{}
		if err := d.Client.Get(ctx, identity.PoolKey, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		reservations := latest.Status.Capacity.Reservations
		kept := make([]corev1alpha1.RuntimePoolCapacityReservationStatus, 0, len(reservations))
		changed := false
		for i := range reservations {
			if runtimePoolReservationMatches(reservations[i], identity) {
				changed = true
				continue
			}
			kept = append(kept, reservations[i])
		}
		if !changed {
			return nil
		}
		latest.Status.Capacity.Reservations = kept
		updateRuntimePoolReservationCounters(&latest.Status.Capacity)
		return d.Client.Status().Update(ctx, latest)
	})
}

func activeRuntimePoolReservations(pool *corev1alpha1.RuntimePool, controllerEpoch int64, now time.Time) ([]corev1alpha1.RuntimePoolCapacityReservationStatus, bool) {
	if pool == nil {
		return nil, false
	}
	activeInstanceID := ""
	if pool.Status.Lifecycle == corev1alpha1.RuntimePoolLifecycleServing && pool.Status.ActiveInstance != nil &&
		pool.Status.ActiveInstance.ControllerEpoch == controllerEpoch {
		activeInstanceID = strings.TrimSpace(pool.Status.ActiveInstance.RuntimeInstanceID)
	}
	reservations := make([]corev1alpha1.RuntimePoolCapacityReservationStatus, 0, len(pool.Status.Capacity.Reservations))
	changed := false
	for i := range pool.Status.Capacity.Reservations {
		reservation := pool.Status.Capacity.Reservations[i]
		valid := reservation.PoolUID == string(pool.UID) && reservation.ControllerEpoch == controllerEpoch &&
			reservation.RuntimeInstanceID == activeInstanceID && activeInstanceID != "" && reservation.ExpiresAt.After(now) &&
			reservation.ResidentSlots >= 0 && reservation.ResidentSlots <= 1 && reservation.PromptSlots >= 0 && reservation.PromptSlots <= 1 &&
			reservation.ResidentSlots+reservation.PromptSlots > 0
		if !valid {
			changed = true
			continue
		}
		reservations = append(reservations, reservation)
	}
	if len(reservations) != len(pool.Status.Capacity.Reservations) {
		changed = true
	}
	sortRuntimePoolReservations(reservations)
	return reservations, changed
}

func runtimePoolReservationMatches(reservation corev1alpha1.RuntimePoolCapacityReservationStatus, identity acpRuntimePoolReservationIdentity) bool {
	return reservation.PoolUID == string(identity.PoolUID) && reservation.TaskUID == string(identity.TaskUID) &&
		reservation.Attempt == identity.Attempt && reservation.ControllerEpoch == identity.ControllerEpoch &&
		(identity.RuntimeInstanceID == "" || reservation.RuntimeInstanceID == identity.RuntimeInstanceID)
}

func runtimePoolReservedSlots(reservations []corev1alpha1.RuntimePoolCapacityReservationStatus) (int64, int64) {
	var resident, prompts int64
	for i := range reservations {
		resident += int64(reservations[i].ResidentSlots)
		prompts += int64(reservations[i].PromptSlots)
	}
	return resident, prompts
}

func updateRuntimePoolReservationCounters(capacity *corev1alpha1.RuntimePoolCapacityStatus) {
	if capacity == nil {
		return
	}
	resident, prompts := runtimePoolReservedSlots(capacity.Reservations)
	capacity.ReservedSessions = int32(resident)
	capacity.ReservedPrompts = int32(prompts)
}

func effectiveRuntimePoolCapacityLimits(pool *corev1alpha1.RuntimePool) (int64, int64) {
	resident := pool.Status.Capacity.MaxResidentSessions
	prompts := pool.Status.Capacity.MaxRunningPrompts
	if resident <= 0 && pool.Spec.Capacity != nil {
		resident = pool.Spec.Capacity.MaxResidentSessions
	}
	if prompts <= 0 && pool.Spec.Capacity != nil {
		prompts = pool.Spec.Capacity.MaxRunningPrompts
	}
	if resident <= 0 {
		resident = corev1alpha1.DefaultRuntimePoolMaxResidentSessions
	}
	if prompts <= 0 {
		prompts = corev1alpha1.DefaultRuntimePoolMaxRunningPrompts
	}
	return int64(resident), int64(prompts)
}

func sortRuntimePoolReservations(reservations []corev1alpha1.RuntimePoolCapacityReservationStatus) {
	sort.SliceStable(reservations, func(i, j int) bool {
		if !reservations[i].ReservedAt.Equal(&reservations[j].ReservedAt) {
			return reservations[i].ReservedAt.Before(&reservations[j].ReservedAt)
		}
		if reservations[i].TaskUID != reservations[j].TaskUID {
			return reservations[i].TaskUID < reservations[j].TaskUID
		}
		if reservations[i].Attempt != reservations[j].Attempt {
			return reservations[i].Attempt < reservations[j].Attempt
		}
		return reservations[i].ControllerEpoch < reservations[j].ControllerEpoch
	})
}

func updateRuntimePoolReservationStatusIfChanged(ctx context.Context, kubeClient client.Client, pool *corev1alpha1.RuntimePool, changed bool) error {
	if !changed {
		return nil
	}
	return kubeClient.Status().Update(ctx, pool)
}

type acpDispatchTarget struct {
	pool *corev1alpha1.RuntimePool
	// external is retained only for terminal RuntimeSession cleanup and recovery.
	// New Task dispatch must never construct an external target.
	external    *corev1alpha1.AgentRuntime
	reservation *acpRuntimePoolReservationIdentity
}

func runtimeSessionCreateTimeout(target acpDispatchTarget) time.Duration {
	minimum := time.Duration(corev1alpha1.DefaultRuntimePoolColdStartTimeoutSeconds) * time.Second
	if target.pool == nil {
		return minimum
	}
	configured := time.Duration(target.pool.Spec.ColdStartTimeoutSeconds) * time.Second
	if configured < minimum {
		return minimum
	}
	return configured
}

func runtimeSessionCreateExpiresAt(issuedAt time.Time, target acpDispatchTarget) time.Time {
	return issuedAt.UTC().Add(max(runtimeSessionCreateTimeout(target), artifactcap.MaxCapabilityTTL))
}

func runtimeSessionCreateRenewalExpiresAt(issuedAt, createExpiresAt time.Time, hasWorkspaceAuthorization bool) time.Time {
	if !hasWorkspaceAuthorization {
		return createExpiresAt
	}
	workspaceExpiresAt := issuedAt.UTC().Add(artifactcap.MaxCapabilityTTL)
	if workspaceExpiresAt.Before(createExpiresAt) {
		return workspaceExpiresAt
	}
	return createExpiresAt
}

const runtimeSessionCreateRenewalMargin = 5 * time.Second

func runtimeSessionCreateAuthorizationNeedsRenewal(expiresAt, now time.Time) bool {
	return !expiresAt.After(now.UTC().Add(runtimeSessionCreateRenewalMargin))
}

func acpTaskDeadline(task *corev1alpha1.Task, now time.Time) (time.Time, bool) {
	if task == nil {
		return time.Time{}, false
	}
	timeout := defaultACPTaskTimeout
	switch {
	case task.Spec.Timeout != nil && task.Spec.Timeout.Duration > 0:
		timeout = task.Spec.Timeout.Duration
	case task.Status.AgentExecutionBinding == nil ||
		task.Status.AgentExecutionBinding.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV2:
		return time.Time{}, false
	}
	return taskDeadlineFromTimeout(task, now, timeout), true
}

func taskDeadlineFromTimeout(task *corev1alpha1.Task, now time.Time, timeout time.Duration) time.Time {
	now = now.UTC()
	var startedAt time.Time
	if !task.CreationTimestamp.IsZero() {
		startedAt = task.CreationTimestamp.UTC()
	} else {
		startedAt = acpTaskQueuedAt(task)
		if startedAt.Equal(time.Unix(0, 0).UTC()) {
			startedAt = now
		}
	}
	return startedAt.Add(timeout)
}

func (d *ACPDispatcher) settleQueuedTaskBeforeAdmission(ctx context.Context, queued *corev1alpha1.Task) (bool, error) {
	if queued == nil {
		return false, nil
	}
	reader := d.APIReader
	if reader == nil {
		reader = d.Client
	}
	task := &corev1alpha1.Task{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: queued.Namespace, Name: queued.Name}, task); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if task.Status.Execution == nil ||
		(task.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued && task.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved) {
		return false, nil
	}
	if d.isActive(task.UID) {
		return false, nil
	}
	settled, err := d.settleTaskBeforeRuntimeAdmission(ctx, task)
	return !settled, err
}

func (d *ACPDispatcher) settleTaskBeforeRuntimeAdmission(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	if task == nil || task.Status.Execution == nil {
		return false, nil
	}
	operation := ""
	reason := corev1alpha1.TaskExecutionReason("")
	message := ""
	now := time.Now().UTC()
	deadline, hasDeadline := acpTaskDeadline(task, now)
	switch {
	case !task.DeletionTimestamp.IsZero() || task.Status.Phase == corev1alpha1.TaskPhaseCancelled:
		operation, reason, message = "cancelled-before-admission", "Cancelled", "task cancelled before runtime admission"
	case hasDeadline && !now.Before(deadline):
		operation, reason, message = "timeout-before-admission", acpTaskTimeoutReason, "task deadline exceeded before runtime admission"
	default:
		return false, nil
	}
	attemptID, err := promptAttemptIDFromTask(task)
	if err != nil {
		return true, err
	}
	fence, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return true, err
	}
	var recoveredSession *acpTaskSession
	if task.Status.Execution.State == corev1alpha1.TaskExecutionStateReserved {
		if task.Spec.SessionRef == nil {
			attempt, getErr := d.Store.GetPromptAttempt(ctx, attemptID)
			if getErr != nil {
				return true, getErr
			}
			bound, boundErr := promptAttemptSessionBound(attempt)
			if boundErr != nil {
				return true, boundErr
			}
			if bound {
				task.Status.Execution.RuntimeInstanceID = attempt.RuntimeInstanceID
				task.Status.Execution.RuntimeSessionUID = attempt.SessionUID
				task.Status.Execution.RuntimeSessionGeneration = attempt.SessionLeaseGeneration
				if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
					status.RuntimeInstanceID = attempt.RuntimeInstanceID
					status.RuntimeSessionUID = attempt.SessionUID
					status.RuntimeSessionGeneration = attempt.SessionLeaseGeneration
				}); err != nil {
					return true, err
				}
				complete, cleanupErr := d.cleanupRecoveredTaskScopedRuntimeSession(ctx, task)
				if cleanupErr != nil || !complete {
					return true, cleanupErr
				}
			}
		} else {
			recoveredSession, err = d.quiesceInterruptedTaskSessionPreparation(ctx, task, attemptID, fence)
			if err != nil {
				return true, err
			}
		}
		identity := acpRuntimePoolReservationIdentity{
			PoolKey: types.NamespacedName{Namespace: task.Namespace, Name: task.Status.Execution.RuntimePoolName},
			PoolUID: types.UID(task.Status.Execution.RuntimePoolUID), TaskUID: task.UID,
			Attempt: task.Status.Execution.Attempt, ControllerEpoch: task.Status.Execution.ControllerEpoch,
		}
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		releaseErr := d.releaseRuntimePoolReservation(releaseCtx, identity)
		cancel()
		if releaseErr != nil {
			return true, releaseErr
		}
	}
	if err := d.settlePreSubmissionCancellation(ctx, task, attemptID, fence, operation, reason, message); err != nil {
		return true, err
	}
	if recoveredSession != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), acpPreSubmissionCleanupTimeout)
		defer cancel()
		var terminalErr = context.Canceled
		if reason == corev1alpha1.TaskExecutionReason(acpTaskTimeoutReason) {
			terminalErr = context.DeadlineExceeded
		}
		if err := d.reconcileUnfinalizedTaskSession(cleanupCtx, task, fence, recoveredSession, terminalErr); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (d *ACPDispatcher) reserveTask(ctx context.Context, queued *corev1alpha1.Task) (*corev1alpha1.Task, acpDispatchTarget, error) {
	task := &corev1alpha1.Task{}
	reader := d.APIReader
	if reader == nil {
		reader = d.Client
	}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: queued.Namespace, Name: queued.Name}, task); err != nil {
		return nil, acpDispatchTarget{}, client.IgnoreNotFound(err)
	}
	if task.Status.Execution == nil || (task.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued && task.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved) {
		return nil, acpDispatchTarget{}, nil
	}
	if persistedExternalRuntimeDispatch(task) {
		return nil, acpDispatchTarget{}, d.rejectPersistedExternalRuntimeDispatch(ctx, task)
	}
	settled, err := d.settleTaskBeforeRuntimeAdmission(ctx, task)
	if err != nil || settled {
		return nil, acpDispatchTarget{}, err
	}
	attemptID, err := promptAttemptIDFromTask(task)
	if err != nil {
		return nil, acpDispatchTarget{}, err
	}
	fence, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return nil, acpDispatchTarget{}, err
	}
	var target acpDispatchTarget
	residentSlots := int32(1)
	if task.Spec.SessionRef != nil {
		// Session planning is the authority for whether an existing resident
		// RuntimeSession can be reused. Claim only prompt capacity until that
		// plan proves a new resident slot is actually required.
		residentSlots = 0
	}
	pool, reservation, claimErr := d.claimRuntimePoolReservation(
		ctx, task, task.Status.Execution.RuntimePoolName, fence, residentSlots,
	)
	if claimErr != nil {
		if errors.Is(claimErr, errACPRuntimePoolAtCapacity) || errors.Is(claimErr, errACPRuntimePoolNotAdmitting) {
			_ = d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
				status.Reason = corev1alpha1.TaskExecutionReasonAtCapacity
				status.Message = claimErr.Error()
			})
			return nil, acpDispatchTarget{}, nil
		}
		return nil, acpDispatchTarget{}, claimErr
	}
	target.pool = pool
	target.reservation = reservation
	releaseClaim := func() {
		if target.reservation == nil {
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = d.releaseRuntimePoolReservation(releaseCtx, *target.reservation)
	}
	if err := d.freezeWorkspaceCredentialVersions(ctx, task); err != nil {
		releaseClaim()
		if blocked, ok := asACPWorkspaceCredentialBlockedError(err); ok {
			if settleErr := d.settleFrozenWorkspaceCredentialBlocked(ctx, task, attemptID, fence, blocked); settleErr != nil {
				return nil, acpDispatchTarget{}, fmt.Errorf("settle frozen workspace credential failure: %w", settleErr)
			}
			return nil, acpDispatchTarget{}, nil
		}
		return nil, acpDispatchTarget{}, err
	}
	bound, err := d.refreshTaskRuntimePoolBinding(ctx, task, pool)
	if err != nil {
		releaseClaim()
		return nil, acpDispatchTarget{}, err
	}
	if !bound {
		releaseClaim()
		return nil, acpDispatchTarget{}, nil
	}
	ready, err := d.preparePromptAttemptReservation(ctx, task, attemptID, fence)
	if err != nil {
		releaseClaim()
		return nil, acpDispatchTarget{}, err
	}
	if !ready {
		releaseClaim()
		return nil, acpDispatchTarget{}, nil
	}
	bound, err = d.refreshTaskRuntimePoolBinding(ctx, task, pool)
	if err != nil {
		releaseClaim()
		return nil, acpDispatchTarget{}, err
	}
	if !bound {
		releaseClaim()
		return nil, acpDispatchTarget{}, nil
	}
	if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
		status.State = corev1alpha1.TaskExecutionStateReserved
		status.ControllerEpoch = fence.Epoch
		status.Reason = ""
		status.Message = ""
		status.LastTransitionTime = nowMeta()
	}); err != nil {
		releaseClaim()
		return nil, acpDispatchTarget{}, err
	}
	bound, err = d.refreshTaskRuntimePoolBinding(ctx, task, pool)
	if err != nil {
		releaseClaim()
		return nil, acpDispatchTarget{}, err
	}
	if !bound {
		releaseClaim()
		return nil, acpDispatchTarget{}, nil
	}
	return task, target, nil
}

func (d *ACPDispatcher) preparePromptAttemptReservation(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
) (bool, error) {
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return false, err
	}
	switch attempt.ExecutionState {
	case store.PromptExecutionQueued:
		if err := d.transitionAttempt(
			ctx, attemptID, fence, store.PromptExecutionQueued, store.PromptExecutionReserved, "reserve", nil,
		); err != nil {
			return false, err
		}
		return true, nil
	case store.PromptExecutionReserved:
		return true, nil
	case store.PromptExecutionSessionStarting, store.PromptExecutionPlanned:
		// The pre-submission path advances the durable PromptAttempt before it
		// acquires the Session lease and projects the Task state. If its fenced
		// rollback fails after a transient cross-store error, the Task remains
		// Reserved while the PromptAttempt is left one step ahead. Re-enter the
		// existing idempotent recovery barrier before strict dispatch verification
		// so the exact Session lease/turn can be retried without replaying a prompt.
		if task == nil || task.Status.Execution == nil ||
			task.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved {
			return false, nil
		}
		if err := d.requeuePreSubmissionTask(
			ctx, task, attemptID, fence, errors.New("recover incomplete pre-submission rollback"),
		); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, nil
	}
}

func (d *ACPDispatcher) settleFrozenWorkspaceCredentialBlocked(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	blocked *acpWorkspaceCredentialBlockedError,
) error {
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	if !queuedPromptAttemptMatchesTask(attempt, task) {
		return fmt.Errorf("%w: frozen credential PromptAttempt does not match Task execution identity", store.ErrConflict)
	}
	if !promptAttemptFreezesCredential(attempt, blocked) {
		return fmt.Errorf("%w: PromptAttempt does not contain the reported frozen credential binding", store.ErrConflict)
	}
	if attempt.ExecutionState == store.PromptExecutionFailed {
		if attempt.TerminalReason != acpCredentialBlockedOperation {
			return fmt.Errorf("%w: PromptAttempt already failed for reason %q", store.ErrConflict, attempt.TerminalReason)
		}
	} else {
		switch attempt.ExecutionState {
		case store.PromptExecutionQueued, store.PromptExecutionReserved:
		default:
			return fmt.Errorf("%w: cannot settle credential-blocked PromptAttempt from state %s", store.ErrConflict, attempt.ExecutionState)
		}
		if attempt.RuntimeInstanceID != "" || attempt.SessionUID != "" || attempt.SessionLeaseGeneration != 0 {
			return fmt.Errorf("%w: credential-blocked PromptAttempt already has a runtime or session binding", store.ErrConflict)
		}
		digest, digestErr := acpDomainDigest("attempt-transition", map[string]any{
			"id": attemptID, "from": attempt.ExecutionState, "to": store.PromptExecutionFailed,
			"operation": acpCredentialBlockedOperation, "version": attempt.Version,
			"terminalReason": acpCredentialBlockedOperation, "credentialRole": blocked.role,
		})
		if digestErr != nil {
			return digestErr
		}
		operationID := store.CanonicalControlID(
			acpCredentialBlockedOperation, attempt.ID, strconv.FormatInt(attempt.Version, 10), string(blocked.role),
		)
		if _, err := d.Store.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attemptID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			NewState: store.PromptExecutionFailed, OperationID: operationID,
			OperationDigest: digest, TerminalReason: acpCredentialBlockedOperation, UpdatedAt: time.Now().UTC(),
		}); err != nil {
			return err
		}
	}
	return d.failTaskBeforeSessionBinding(
		ctx, task, corev1alpha1.TaskExecutionStateFailed, corev1alpha1.TaskExecutionOutcomeFailed,
		acpCredentialBlockedExecutionReason, acpCredentialBlockedMessage,
	)
}

func (d *ACPDispatcher) refreshTaskRuntimePoolBinding(
	ctx context.Context,
	task *corev1alpha1.Task,
	pool *corev1alpha1.RuntimePool,
) (bool, error) {
	if task == nil || pool == nil {
		return false, nil
	}
	reader := d.APIReader
	if reader == nil {
		reader = d.Client
	}
	current := &corev1alpha1.Task{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, current); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if current.UID != task.UID || current.Status.Execution == nil ||
		(current.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued &&
			current.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved) ||
		current.Status.Execution.Attempt != task.Status.Execution.Attempt ||
		current.Status.Execution.PromptID != task.Status.Execution.PromptID ||
		current.Status.Execution.RequestDigest != task.Status.Execution.RequestDigest ||
		!acpRuntimePoolBindingMatches(current.Status.Execution, pool) {
		return false, nil
	}
	task.Status = current.Status
	task.Labels = current.Labels
	task.Annotations = current.Annotations
	return true, nil
}

func (d *ACPDispatcher) handlePreSubmissionContextDone(
	ctx context.Context,
	runtimeCtx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
) (bool, error) {
	if !errors.Is(runtimeContextError(runtimeCtx), context.DeadlineExceeded) {
		return false, nil
	}
	return true, d.settlePreSubmissionCancellation(
		ctx, task, attemptID, fence, "timeout-before-submission", acpTaskTimeoutReason, "task deadline exceeded before prompt submission",
	)
}

func (d *ACPDispatcher) newTaskRuntimeContext(ctx context.Context, task *corev1alpha1.Task) (context.Context, context.CancelFunc) {
	if d != nil && d.runtimeContextFactory != nil {
		return d.runtimeContextFactory(ctx, task)
	}
	if deadline, ok := acpTaskDeadline(task, time.Now().UTC()); ok {
		return context.WithDeadline(ctx, deadline)
	}
	return context.WithCancel(ctx)
}

func runtimeContextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return ctx.Err()
}

func (d *ACPDispatcher) settlePreSubmissionCancellation(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	operation string,
	reason corev1alpha1.TaskExecutionReason,
	message string,
) error {
	if err := d.transitionAttemptToCancelled(
		ctx, attemptID, fence, operation, reason, message,
	); err != nil {
		return err
	}
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	bound, err := promptAttemptSessionBound(attempt)
	if err != nil {
		return err
	}
	if task.Spec.SessionRef != nil && !bound {
		return d.failTaskBeforeSessionBinding(
			ctx, task, corev1alpha1.TaskExecutionStateCancelled, corev1alpha1.TaskExecutionOutcomeCancelled, reason, message,
		)
	}
	return d.failTask(
		ctx, task, corev1alpha1.TaskExecutionStateCancelled, corev1alpha1.TaskExecutionOutcomeCancelled, reason, message,
	)
}

type attemptRuntimeBinding struct {
	RuntimeInstanceID string
	SessionUID        string
	SessionGeneration int64
}

func (d *ACPDispatcher) transitionAttempt(ctx context.Context, id string, fence store.ControllerEpochFence, from, to store.PromptExecutionState, operation string, binding *attemptRuntimeBinding) error {
	attempt, err := d.Store.GetPromptAttempt(ctx, id)
	if err != nil {
		return err
	}
	if attempt.ExecutionState == to {
		return nil
	}
	if attempt.ExecutionState != from {
		return fmt.Errorf("prompt attempt %s state is %s, want %s", id, attempt.ExecutionState, from)
	}
	digest, err := acpDomainDigest("attempt-transition", map[string]any{"id": id, "from": from, "to": to, "operation": operation, "version": attempt.Version})
	if err != nil {
		return err
	}
	transition := store.PromptAttemptExecutionTransition{
		ID: id, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: from, NewState: to,
		OperationID: operation + "-" + strconv.FormatInt(attempt.Version, 10), OperationDigest: digest, UpdatedAt: time.Now().UTC(),
	}
	if binding != nil {
		transition.RuntimeInstanceID = binding.RuntimeInstanceID
		transition.SessionUID = binding.SessionUID
		transition.SessionLeaseGeneration = binding.SessionGeneration
	}
	_, err = d.Store.TransitionPromptAttemptExecution(ctx, transition)
	return err
}

func acpPromptResultDigest(result []byte) string {
	sum := sha256.Sum256(result)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func acpSettlingTransitionDigest(id string, version int64, result []byte) (string, error) {
	return acpDomainDigest("attempt-transition", map[string]any{
		"id": id, "from": store.PromptExecutionRunning, "to": store.PromptExecutionSettling,
		"operation": acpSettlingOperation, "version": version, "resultDigest": acpPromptResultDigest(result),
	})
}

func (d *ACPDispatcher) transitionAttemptToSettlingWithResult(
	ctx context.Context,
	task *corev1alpha1.Task,
	id string,
	fence store.ControllerEpochFence,
	result []byte,
) error {
	if task == nil || strings.TrimSpace(task.Namespace) == "" || strings.TrimSpace(task.Name) == "" {
		return fmt.Errorf("task identity is required for prompt settling")
	}
	receipts, ok := d.ResultStore.(store.PromptResultReceiptStore)
	if !ok {
		return fmt.Errorf("ACP prompt settling requires a result store with prompt result receipts")
	}
	attempt, err := d.Store.GetPromptAttempt(ctx, id)
	if err != nil {
		return err
	}
	expectedVersion := attempt.Version
	if attempt.ExecutionState == store.PromptExecutionSettling {
		expectedVersion--
	}
	if expectedVersion < 1 {
		return fmt.Errorf("prompt attempt %s has invalid settling receipt version %d", id, expectedVersion)
	}
	digest, err := acpSettlingTransitionDigest(id, expectedVersion, result)
	if err != nil {
		return err
	}
	operationID := acpSettlingOperation + "-" + strconv.FormatInt(expectedVersion, 10)
	if err := receipts.SavePromptResultReceipt(ctx, store.PromptResultReceipt{
		AttemptID: id, Namespace: task.Namespace, TaskName: task.Name,
		OperationID: operationID, OperationDigest: digest, Data: result,
	}); err != nil {
		return err
	}
	if attempt.ExecutionState == store.PromptExecutionSettling {
		if attempt.LastOperationID == operationID && attempt.LastOperationDigest == digest {
			return nil
		}
		return fmt.Errorf("prompt attempt %s settling receipt does not match the task result", id)
	}
	if attempt.ExecutionState != store.PromptExecutionRunning {
		return fmt.Errorf("prompt attempt %s state is %s, want %s", id, attempt.ExecutionState, store.PromptExecutionRunning)
	}
	_, err = d.Store.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
		ID: id, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: store.PromptExecutionRunning,
		NewState: store.PromptExecutionSettling, OperationID: operationID, OperationDigest: digest, UpdatedAt: time.Now().UTC(),
	})
	return err
}

func (d *ACPDispatcher) transitionDelivery(ctx context.Context, id string, fence store.ControllerEpochFence, from, to store.PromptDeliveryState, operation, reason string) error {
	attempt, err := d.Store.GetPromptAttempt(ctx, id)
	if err != nil {
		return err
	}
	if attempt.DeliveryState == to {
		return nil
	}
	digest, err := acpDomainDigest("delivery-transition", map[string]any{"id": id, "from": from, "to": to, "operation": operation, "version": attempt.Version})
	if err != nil {
		return err
	}
	_, err = d.Store.TransitionPromptAttemptDelivery(ctx, store.PromptAttemptDeliveryTransition{
		ID: id, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: from, NewState: to,
		OperationID: operation + "-" + strconv.FormatInt(attempt.Version, 10), OperationDigest: digest, TerminalReason: reason, UpdatedAt: time.Now().UTC(),
	})
	return err
}

func (d *ACPDispatcher) runtimeClient(ctx context.Context, target acpDispatchTarget) (*harnessv2.Client, harnessv2.Fence, harnessv2.RuntimeProfile, int, error) {
	if target.external != nil {
		return d.externalRuntimeClient(ctx, target.external)
	}
	if target.pool == nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("ACP runtime target is missing")
	}
	return d.runtimePoolClient(ctx, target.pool)
}

func (d *ACPDispatcher) runtimePoolClient(ctx context.Context, pool *corev1alpha1.RuntimePool) (*harnessv2.Client, harnessv2.Fence, harnessv2.RuntimeProfile, int, error) {
	active := pool.Status.ActiveInstance
	if active == nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("RuntimePool has no active instance")
	}
	fence, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, err
	}
	if active.ControllerEpoch != fence.Epoch {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("RuntimePool active instance is bound to stale controller epoch")
	}
	secret, err := d.runtimeAuthSecret(ctx, pool)
	if err != nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, err
	}
	controllerToken := strings.TrimSpace(string(secret.Data[runtimePoolControllerTokenKey]))
	capabilitySecret := secret.Data[runtimePoolCapabilitySecretKey]
	if controllerToken == "" || len(capabilitySecret) < harnessv2.MinCapabilitySecretBytes {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("RuntimePool auth Secret is incomplete")
	}
	endpoint := exactPodEndpoint(active.PodAddress)
	options := []harnessv2.ClientOption{
		harnessv2.WithControlTimeout(runtimeSessionCreateTimeout(acpDispatchTarget{pool: pool})),
		harnessv2.WithControllerBearerToken(controllerToken),
		harnessv2.WithOperationCapabilitySecret(capabilitySecret),
		harnessv2.WithStatusCapabilityBinding(harnessv2.StatusCapabilityBinding{
			RuntimeProfileDigest: harnessv2.ProfileDigest(pool.Spec.Runtime.Profile.Digest),
			RuntimeInstanceID:    harnessv2.RuntimeInstanceID(active.RuntimeInstanceID),
		}),
	}
	if runtimePoolIsSubstrateBacked(pool) {
		// Substrate-backed instances are reached through the provider router:
		// the endpoint is the exact actor route host, and the pinned router
		// transport preserves it as the logical Host header.
		endpoint = urlSchemeHTTP + "://" + strings.TrimSpace(active.PodAddress)
		substrateHTTPClient, substrateErr := d.substrateRouteHTTPClient()
		if substrateErr != nil {
			return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, substrateErr
		}
		options = append(options, harnessv2.WithHTTPClient(substrateHTTPClient))
	}
	runtimeClient, err := harnessv2.NewClient(endpoint, options...)
	if err != nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, err
	}
	capabilities, err := runtimeClient.Capabilities(ctx)
	if err != nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, err
	}
	if string(capabilities.RuntimeProfileDigest) != pool.Spec.Runtime.Profile.Digest || string(capabilities.RuntimeProfileDigest) != active.ProfileDigest {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("RuntimePool capability profile digest mismatch")
	}
	if !capabilities.WorkspaceGovernance.Strict() {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("RuntimePool does not provide strict workspace governance")
	}
	profile := runtimeProfileFromPool(pool.Spec.Runtime.Profile)
	if profile.WorkspaceIntent == harnessv2.WorkspaceIntentWrite && !capabilities.SupportsPublicationFinalization {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("RuntimePool does not support controller-owned RuntimeSession publication finalization required for write workspaces")
	}
	runtimeFence := harnessv2.Fence{
		RuntimeInstanceID: harnessv2.RuntimeInstanceID(active.RuntimeInstanceID), SupervisorBootID: harnessv2.SupervisorBootID(active.BootID),
		ControllerEpoch: uint64(fence.Epoch), RuntimePoolUID: harnessv2.RuntimePoolUID(pool.UID), RuntimePoolGeneration: uint64(pool.Generation),
		RuntimeProfileDigest: harnessv2.ProfileDigest(pool.Spec.Runtime.Profile.Digest), ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	return runtimeClient, runtimeFence, profile, capabilities.Limits.MaxTerminalResultBytes, nil
}

func (d *ACPDispatcher) externalRuntimeClient(ctx context.Context, runtime *corev1alpha1.AgentRuntime) (*harnessv2.Client, harnessv2.Fence, harnessv2.RuntimeProfile, int, error) {
	if reason := externalAgentRuntimeReadinessReason(nil, runtime); reason != "" {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("%s", reason)
	}
	currentFence, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, err
	}
	observed := runtime.Status.ObservedCapabilities
	if observed.ControllerEpoch != currentFence.Epoch {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("external AgentRuntime is fenced to controller epoch %d, current epoch is %d", observed.ControllerEpoch, currentFence.Epoch)
	}
	if strings.TrimSpace(observed.RuntimePoolUID) == "" || observed.RuntimePoolGeneration < 1 || strings.TrimSpace(observed.SupervisorBootID) == "" {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("external AgentRuntime did not advertise the required immutable fence")
	}
	reconciler := &AgentRuntimeReconciler{Client: d.Client, APIReader: d.APIReader}
	// Readiness was established at conformance time, but the endpoint Service,
	// EndpointSlices, and backend Pods are mutable between reconciles; revalidate
	// the endpoint policy immediately before sending the bearer and signed
	// capabilities, and for a Service endpoint capture the verified backend Pod
	// addresses so the authenticated connection is pinned to one of them rather
	// than routed through the still-mutable Service ClusterIP.
	serviceBackendPins, err := reconciler.AgentRuntimeServiceBackendPins(ctx, runtime)
	if err != nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, err
	}
	auth, err := reconciler.agentRuntimeAuthMaterial(ctx, runtime)
	if err != nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, err
	}
	if runtime.Status.ObservedControllerAuthRefResourceVersion != auth.controllerResourceVersion ||
		runtime.Status.ObservedOperationCapabilityRefResourceVersion != auth.capabilityResourceVersion {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("external AgentRuntime authentication material changed after conformance")
	}
	clientOptions := []harnessv2.ClientOption{
		harnessv2.WithControlTimeout(runtimeSessionCreateTimeout(acpDispatchTarget{external: runtime})),
		harnessv2.WithControllerBearerToken(auth.controllerBearerToken),
		harnessv2.WithOperationCapabilitySecret(auth.operationCapabilitySecret),
		harnessv2.WithStatusCapabilityBinding(harnessv2.StatusCapabilityBinding{
			RuntimeProfileDigest: harnessv2.ProfileDigest(runtime.Spec.Capabilities.Profile.Digest),
			RuntimeInstanceID:    harnessv2.RuntimeInstanceID(runtime.Spec.Capabilities.RuntimeInstanceID),
		}),
	}
	// Pin the connection: a Service endpoint dials only its verified backend
	// Pod IPs; a non-Service endpoint dialed from the controller's privileged
	// position enforces the same per-dial public-address control conformance
	// uses (a hostname that resolved publicly at conformance can rebind to an
	// internal address).
	dialTimeout := runtimeSessionCreateTimeout(acpDispatchTarget{external: runtime})
	if len(serviceBackendPins) > 0 {
		clientOptions = append(clientOptions, harnessv2.WithHTTPClient(&http.Client{
			Timeout:   dialTimeout,
			Transport: PinnedBackendDialTransport(serviceBackendPins),
		}))
	} else if agentRuntimeEndpointRequiresPublicDial(runtime.Spec.Deployment.Endpoint) {
		clientOptions = append(clientOptions, harnessv2.WithHTTPClient(&http.Client{
			Timeout:   dialTimeout,
			Transport: v2conformance.PublicAddressDialTransport(),
		}))
	}
	runtimeClient, err := harnessv2.NewClient(runtime.Spec.Deployment.Endpoint, clientOptions...)
	if err != nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, err
	}
	capabilities, err := runtimeClient.Capabilities(ctx)
	if err != nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, err
	}
	if string(capabilities.RuntimeProfileDigest) != runtime.Spec.Capabilities.Profile.Digest || !capabilities.WorkspaceGovernance.Strict() {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("external AgentRuntime capability identity/profile drifted after conformance")
	}
	if runtime.Spec.Capabilities.Profile.WorkspaceIntent == corev1alpha1.WorkspaceIntentWrite && !capabilities.SupportsPublicationFinalization {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("external AgentRuntime does not support controller-owned RuntimeSession publication finalization required for write workspaces")
	}
	status, err := runtimeClient.Status(ctx)
	if err != nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, err
	}
	if string(status.Fence.RuntimeInstanceID) != observed.RuntimeInstanceID || string(status.Fence.SupervisorBootID) != observed.SupervisorBootID ||
		int64(status.Fence.ControllerEpoch) != observed.ControllerEpoch || string(status.Fence.RuntimePoolUID) != observed.RuntimePoolUID ||
		int64(status.Fence.RuntimePoolGeneration) != observed.RuntimePoolGeneration || string(status.Fence.RuntimeProfileDigest) != runtime.Spec.Capabilities.Profile.Digest {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, fmt.Errorf("external AgentRuntime status fence drifted after conformance")
	}
	profile, err := agentRuntimeProfile(*runtime.Spec.Capabilities.Profile)
	if err != nil {
		return nil, harnessv2.Fence{}, harnessv2.RuntimeProfile{}, 0, err
	}
	fence := harnessv2.Fence{
		RuntimeInstanceID: harnessv2.RuntimeInstanceID(observed.RuntimeInstanceID), SupervisorBootID: harnessv2.SupervisorBootID(observed.SupervisorBootID),
		ControllerEpoch: uint64(currentFence.Epoch), RuntimePoolUID: harnessv2.RuntimePoolUID(observed.RuntimePoolUID),
		RuntimePoolGeneration: uint64(observed.RuntimePoolGeneration), RuntimeProfileDigest: harnessv2.ProfileDigest(runtime.Spec.Capabilities.Profile.Digest),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	return runtimeClient, fence, profile, capabilities.Limits.MaxTerminalResultBytes, nil
}

func (d *ACPDispatcher) runtimeAuthSecret(ctx context.Context, pool *corev1alpha1.RuntimePool) (*corev1.Secret, error) {
	namespace := pool.Spec.RuntimeNamespace
	if namespace == "" && pool.Status.ActiveInstance != nil {
		namespace = pool.Status.ActiveInstance.PodNamespace
	}
	if pool.Status.ActiveInstance == nil {
		return nil, fmt.Errorf("RuntimePool has no active instance")
	}
	secret, err := resolveRuntimePoolAuthSecret(
		ctx, d.APIReader, pool, namespace, pool.Status.ActiveInstance.ControllerEpoch,
	)
	if err != nil {
		return nil, err
	}
	return secret.DeepCopy(), nil
}

func runtimeProfileFromPool(profile corev1alpha1.RuntimePoolProfileSpec) harnessv2.RuntimeProfile {
	var modelLimits *harnessv2.ModelTokenLimits
	if profile.ModelLimits != nil {
		modelLimits = &harnessv2.ModelTokenLimits{
			Context: profile.ModelLimits.Context,
			Output:  profile.ModelLimits.Output,
		}
	}
	return harnessv2.RuntimeProfile{
		ACPProfile: profile.ACPProfile, AdapterDigests: cloneMap(profile.AdapterDigests), ProviderKind: profile.ProviderKind,
		Model:                    profile.Model,
		ModelLimits:              modelLimits,
		AgentConfigurationDigest: profile.AgentConfigurationDigest, ToolPolicyDigest: profile.ToolPolicyDigest,
		ApprovalPolicyDigest: profile.ApprovalPolicyDigest, MCPConfigurationDigest: profile.MCPConfigurationDigest,
		WorkspaceIntent: harnessv2.WorkspaceIntent(profile.WorkspaceIntent), ProxyCredentialRole: profile.ProxyCredentialRole,
		ProxyCredentialScope: profile.ProxyCredentialScope, ResourceClass: profile.ResourceClass,
	}
}

// emptyRuntimeWorkspace derives the repo-less protocol baseline from scope:
// the Session UID for session-bound Tasks (every turn must present the exact
// baseline the session was created with) and the Task UID otherwise.
func emptyRuntimeWorkspace(task *corev1alpha1.Task, scope string) (harnessv2.WorkspaceBaseline, harnessv2.WorkspaceSpec, error) {
	workspace := task.Spec.Workspace
	if workspace != nil && strings.TrimSpace(workspace.GitRepo) != "" {
		return harnessv2.WorkspaceBaseline{}, harnessv2.WorkspaceSpec{}, fmt.Errorf("clean-room Git workspace preparation is not implemented")
	}
	if strings.TrimSpace(scope) == "" {
		scope = string(task.UID)
	}
	digest, err := acpDomainDigest("empty-workspace", map[string]any{"taskUID": scope})
	if err != nil {
		return harnessv2.WorkspaceBaseline{}, harnessv2.WorkspaceSpec{}, err
	}
	baseline := harnessv2.WorkspaceBaseline{
		RepositoryIdentity: acpNoWorkspaceRevision + ":" + scope,
		Revision:           acpNoWorkspaceRevision,
		TreeDigest:         digest,
	}
	intent := harnessv2.WorkspaceIntent(effectiveACPWorkspaceIntent(task))
	return baseline, harnessv2.WorkspaceSpec{Intent: intent, Baseline: baseline}, nil
}

func (d *ACPDispatcher) buildPromptRequest(
	task *corev1alpha1.Task,
	fence harnessv2.Fence,
	profile harnessv2.RuntimeProfile,
	mcpConfiguration harnessv2.MCPPolicyConfiguration,
	bootstrap string,
	userPrompt string,
	admissionRetry int,
) (harnessv2.StartPromptRequest, error) {
	if fence.RuntimeSessionUID == "" {
		fence.RuntimeSessionUID = harnessv2.RuntimeSessionUID(taskRuntimeSessionUID(task))
	}
	if fence.RuntimeSessionGeneration == 0 {
		fence.RuntimeSessionGeneration = 1
	}
	now := time.Now().UTC()
	lease := harnessv2.PromptLease{Generation: 1, IssuedAt: now, ExpiresAt: now.Add(90 * time.Second)}
	operation := "start-prompt"
	if admissionRetry > 0 {
		operation += "-retry-" + strconv.Itoa(admissionRetry)
	}
	metadata := mutationMetadata(fence, task, operation, true, now.Add(60*time.Second))
	content := acpPromptInputContent(bootstrap, userPrompt)
	authorization, err := buildPromptMCPAuthorization(
		mcpConfiguration, fence, profile, metadata, lease, now.Add(60*time.Second),
	)
	if err != nil {
		return harnessv2.StartPromptRequest{}, err
	}
	return harnessv2.StartPromptRequest{
		Protocol: harnessv2.ProtocolVersion, Metadata: metadata, Lease: lease,
		MCPAuthorization: authorization,
		Input:            harnessv2.PromptInput{Content: content},
	}, nil
}

func acpPromptInputContent(bootstrap, userPrompt string) []harnessv2.ContentBlock {
	content := make([]harnessv2.ContentBlock, 0, 2)
	if strings.TrimSpace(bootstrap) != "" {
		content = append(content, harnessv2.ContentBlock{Type: harnessv2.ContentBlockText, Text: bootstrap})
	}
	content = append(content, harnessv2.ContentBlock{Type: harnessv2.ContentBlockText, Text: userPrompt})
	return content
}

func mutationMetadata(fence harnessv2.Fence, task *corev1alpha1.Task, operation string, prompt bool, expiry time.Time) harnessv2.MutationMetadata {
	return mutationMetadataForTaskUID(fence, task, task.UID, operation, prompt, expiry)
}

func mutationMetadataForTaskUID(
	fence harnessv2.Fence,
	task *corev1alpha1.Task,
	taskUID types.UID,
	operation string,
	prompt bool,
	expiry time.Time,
) harnessv2.MutationMetadata {
	if fence.RuntimeSessionUID == "" {
		fence.RuntimeSessionUID = harnessv2.RuntimeSessionUID(taskRuntimeSessionUID(task))
	}
	if fence.RuntimeSessionGeneration == 0 {
		fence.RuntimeSessionGeneration = 1
	}
	metadata := harnessv2.MutationMetadata{
		Fence: fence, TaskUID: harnessv2.TaskUID(taskUID), TaskAttempt: uint32(task.Status.Execution.Attempt),
		OperationID:                harnessv2.OperationID(operation + "-" + task.Status.Execution.PromptID),
		RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion, ExpiresAt: expiry,
	}
	if prompt {
		metadata.PromptID = harnessv2.PromptID(task.Status.Execution.PromptID)
	}
	return metadata
}

func sealMutation(target *harnessv2.RequestDigest, request any) error {
	digest, err := harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		return err
	}
	*target = digest
	return nil
}

func taskRuntimeSessionUID(task *corev1alpha1.Task) string {
	return "session-" + string(task.UID)
}

func runtimeSessionID(fence harnessv2.Fence) string {
	return "runtime-" + string(fence.RuntimeSessionUID) + "-g" + strconv.FormatUint(fence.RuntimeSessionGeneration, 10)
}

func exactPodEndpoint(address string) string {
	address = strings.TrimSpace(address)
	if parsed := net.ParseIP(address); parsed != nil {
		return (&url.URL{Scheme: "http", Host: net.JoinHostPort(address, "8080")}).String()
	}
	if _, _, err := net.SplitHostPort(address); err == nil {
		return (&url.URL{Scheme: "http", Host: address}).String()
	}
	return (&url.URL{Scheme: "http", Host: net.JoinHostPort(address, "8080")}).String()
}

func promptAttemptIDFromTask(task *corev1alpha1.Task) (string, error) {
	return promptAttemptIDFromTaskUID(task, task.UID)
}

func promptAttemptIDFromTaskUID(task *corev1alpha1.Task, taskUID types.UID) (string, error) {
	if task.Status.Execution == nil {
		return "", fmt.Errorf("task execution status is missing")
	}
	return (store.PromptAttemptKey{
		Namespace: task.Namespace, TaskUID: string(taskUID), Attempt: int64(task.Status.Execution.Attempt), PromptID: task.Status.Execution.PromptID,
	}).CanonicalID()
}

func (d *ACPDispatcher) patchExecution(ctx context.Context, task *corev1alpha1.Task, mutate func(*corev1alpha1.TaskExecutionStatus)) error {
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return err
		}
		base := latest.DeepCopy()
		if latest.Status.Execution == nil {
			latest.Status.Execution = &corev1alpha1.TaskExecutionStatus{}
		}
		mutate(latest.Status.Execution)
		switch latest.Status.Execution.State {
		case corev1alpha1.TaskExecutionStateAccepted, corev1alpha1.TaskExecutionStateRunning, corev1alpha1.TaskExecutionStateSettling:
			if latest.Status.Phase == corev1alpha1.TaskPhasePending || latest.Status.Phase == "" {
				latest.Status.Phase = corev1alpha1.TaskPhaseRunning
				if latest.Status.StartTime == nil {
					now := metav1.Now()
					latest.Status.StartTime = &now
				}
			}
		}
		if err := d.Client.Status().Patch(ctx, latest, client.MergeFrom(base)); err != nil {
			return err
		}
		task.Status = latest.Status
		return nil
	})
}

func (d *ACPDispatcher) persistTaskResult(ctx context.Context, task *corev1alpha1.Task, result []byte) error {
	return d.ResultStore.SaveResult(ctx, task.Namespace, task.Name, result)
}

func (d *ACPDispatcher) publishTaskResultReference(ctx context.Context, task *corev1alpha1.Task) error {
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return err
		}
		base := latest.DeepCopy()
		latest.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
		if err := d.Client.Status().Patch(ctx, latest, client.MergeFrom(base)); err != nil {
			return err
		}
		task.Status = latest.Status
		return nil
	})
}

func (d *ACPDispatcher) renewPromptLeaseLoop(
	ctx context.Context,
	cancelRuntime context.CancelFunc,
	runtimeClient *harnessv2.Client,
	sessionID harnessv2.RuntimeSessionID,
	task *corev1alpha1.Task,
	fence harnessv2.Fence,
	lease harnessv2.PromptLease,
	authorization harnessv2.PromptMCPAuthorization,
) {
	log := logf.FromContext(ctx).WithValues("namespace", task.Namespace, "task", task.Name)
	retryDelay := time.Duration(0)
	// pending is the exact sealed mutation of a renewal whose outcome is
	// ambiguous (transient failure after the request may have been written).
	// It is replayed verbatim so a renewal the supervisor already applied is
	// classified as a duplicate of the same operation instead of a
	// digest_conflict on a rebuilt request with fresh timestamps.
	var pending *harnessv2.RenewPromptLeaseRequest
	for {
		wait := max(time.Until(lease.ExpiresAt)/2, 5*time.Second)
		if retryDelay > 0 {
			wait = retryDelay
			retryDelay = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		now := time.Now().UTC()
		var request harnessv2.RenewPromptLeaseRequest
		switch {
		case pending != nil && pending.Metadata.ExpiresAt.After(now.Add(promptLeaseRenewalRetryMargin)):
			request = *pending
		case pending != nil:
			// The ambiguous mutation can no longer be delivered before its
			// expiry, and there is no evidence it was not applied: resealing
			// the same operation with fresh timestamps would collide with the
			// supervisor's recorded operation (digest_conflict) if it was.
			// The mutation is sealed to outlive the retry window, so reaching
			// this point means the lease itself is about to expire.
			log.Info("ACP prompt lease renewal replay expired without a settled outcome; cancelling the prompt",
				"leaseGeneration", lease.Generation, "pendingGeneration", pending.Lease.Generation)
			cancelRuntime()
			return
		default:
			proposed := harnessv2.PromptLease{Generation: lease.Generation + 1, IssuedAt: now, ExpiresAt: now.Add(90 * time.Second)}
			authorization.LeaseGeneration = proposed.Generation
			authorization.ExpiresAt = proposed.ExpiresAt
			if maximum := now.Add(60 * time.Second); authorization.ExpiresAt.After(maximum) {
				authorization.ExpiresAt = maximum
			}
			// The mutation stays valid for as long as its MCP authorization so
			// a transient failure can replay the identical sealed request for
			// the entire retry window instead of resealing it.
			metadata := mutationMetadata(
				fence, task, "renew-lease-"+strconv.FormatUint(proposed.Generation, 10), true, authorization.ExpiresAt,
			)
			authorization.RuntimeSessionUID = metadata.Fence.RuntimeSessionUID
			authorization.SessionGeneration = metadata.Fence.RuntimeSessionGeneration
			authorization.TaskUID = metadata.TaskUID
			authorization.TaskAttempt = metadata.TaskAttempt
			authorization.PromptID = metadata.PromptID
			request = harnessv2.RenewPromptLeaseRequest{
				Protocol: harnessv2.ProtocolVersion, Metadata: metadata,
				ExpectedLeaseGeneration: lease.Generation, Lease: proposed, MCPAuthorization: authorization,
			}
			if err := sealMutation(&request.Metadata.RequestDigest, request); err != nil {
				cancelRuntime()
				return
			}
		}
		proposed := request.Lease
		response, err := runtimeClient.RenewPromptLease(ctx, sessionID, request)
		if err == nil && response.Lease.Generation == proposed.Generation {
			pending = nil
			lease = response.Lease
			continue
		}
		// A prompt that already settled on the supervisor no longer needs a
		// lease: its terminal event is delivered (or recovered) through the
		// prompt stream, so renewal simply stops. Cancelling the runtime
		// context here would abort that stream and turn a completed prompt
		// into a client-error or outcome-unknown settlement.
		if promptLeaseRenewalSettled(err) {
			log.Info("ACP prompt lease renewal found the prompt settled; stopping renewal", "leaseGeneration", lease.Generation)
			return
		}
		// One slow or dropped renewal under load must not cancel a healthy
		// prompt: a transient failure is retried while the current lease is
		// still valid, replaying the identical sealed request. Only a
		// definitive supervisor rejection, a generation mismatch, or an
		// expired lease ends the prompt.
		if remaining := time.Until(lease.ExpiresAt); err != nil && promptLeaseRenewalRetryable(err) && remaining > promptLeaseRenewalRetryMargin {
			replay := request
			pending = &replay
			retryDelay = min(promptLeaseRenewalRetryDelay, remaining/4)
			log.Info("ACP prompt lease renewal failed; retrying while the lease is valid",
				"errorClass", promptLeaseRenewalErrorClass(err), "leaseGeneration", lease.Generation,
				"leaseRemaining", remaining.Round(time.Second).String(), "retryIn", retryDelay.Round(time.Millisecond).String())
			continue
		}
		// Only the low-cardinality class is logged: a supervisor rejection
		// message is runtime-supplied text and must not reach controller logs.
		if err != nil {
			log.Info("ACP prompt lease renewal rejected; cancelling the prompt",
				"errorClass", promptLeaseRenewalErrorClass(err), "leaseGeneration", lease.Generation)
		} else {
			log.Info("ACP prompt lease renewal returned an unexpected generation; cancelling the prompt",
				"leaseGeneration", lease.Generation, "proposedGeneration", proposed.Generation, "returnedGeneration", response.Lease.Generation)
		}
		cancelRuntime()
		return
	}
}

const (
	// promptLeaseRenewalRetryDelay bounds how soon a transiently failed lease
	// renewal is retried; promptLeaseRenewalRetryMargin is the remaining lease
	// time below which a retry is no longer attempted.
	promptLeaseRenewalRetryDelay  = 3 * time.Second
	promptLeaseRenewalRetryMargin = 5 * time.Second
)

// promptLeaseRenewalErrorClass renders a low-cardinality, credential-free
// class for a failed lease renewal (client error kind, HTTP status, and v2
// error code) suitable for structured logs.
func promptLeaseRenewalErrorClass(err error) string {
	if clientErr, ok := errors.AsType[*harnessv2.ClientError](err); ok {
		class := string(clientErr.Kind)
		if clientErr.StatusCode != 0 {
			class += "/" + strconv.Itoa(clientErr.StatusCode)
		}
		if clientErr.Code != "" {
			class += "/" + string(clientErr.Code)
		}
		return class
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	return "unclassified"
}

// promptLeaseRenewalSettled reports whether the supervisor rejected a lease
// renewal because the prompt has already settled (HTTP 410 with the v2
// "settled" code).
func promptLeaseRenewalSettled(err error) bool {
	var clientErr *harnessv2.ClientError
	return errors.As(err, &clientErr) && clientErr.Kind == harnessv2.ClientErrorHTTP &&
		clientErr.StatusCode == http.StatusGone && clientErr.Code == harnessv2.ErrorCodeSettled
}

// promptLeaseRenewalRetryable reports whether a failed lease renewal may be
// retried while the current lease is still valid. Definitive rejections from
// the supervisor (settled prompt, stale fence, identity or digest conflict,
// poisoned session) end the prompt; transport, protocol, stream, and
// retryable or server-side HTTP failures are transient.
func promptLeaseRenewalRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if clientErr, ok := errors.AsType[*harnessv2.ClientError](err); ok {
		switch clientErr.Kind {
		case harnessv2.ClientErrorHTTP:
			return clientErr.Retryable || clientErr.StatusCode >= http.StatusInternalServerError
		case harnessv2.ClientErrorTransport, harnessv2.ClientErrorProtocol, harnessv2.ClientErrorStream:
			return true
		default:
			return false
		}
	}
	return true
}

func (d *ACPDispatcher) resolvePromptPermission(ctx context.Context, runtimeClient *harnessv2.Client, sessionID harnessv2.RuntimeSessionID, task *corev1alpha1.Task, fence harnessv2.Fence, event harnessv2.Event) error {
	permission := event.PermissionRequested
	if permission == nil {
		return nil
	}
	decision := harnessv2.PermissionDecision{Outcome: harnessv2.PermissionDecisionCancelled}
	for _, option := range permission.Options {
		if option.Kind == harnessv2.PermissionOptionRejectOnce {
			decision = harnessv2.PermissionDecision{Outcome: harnessv2.PermissionDecisionSelected, OptionID: option.OptionID}
			break
		}
	}
	request := harnessv2.ResolvePermissionRequest{
		Protocol:  harnessv2.ProtocolVersion,
		Metadata:  mutationMetadata(fence, task, "permission-"+string(permission.RequestID), true, time.Now().UTC().Add(30*time.Second)),
		RequestID: permission.RequestID, Decision: decision,
	}
	if err := sealMutation(&request.Metadata.RequestDigest, request); err != nil {
		return err
	}
	_, err := runtimeClient.ResolvePermission(ctx, sessionID, request)
	return err
}

func isACPRateLimitedClientError(err error) bool {
	var clientErr *harnessv2.ClientError
	return errors.As(err, &clientErr) && clientErr.StatusCode == 429 &&
		clientErr.Code == harnessv2.ErrorCodeRateLimited && clientErr.Retryable
}

func (d *ACPDispatcher) requeuePreSubmissionTask(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	cause error,
) error {
	return d.requeuePreSubmissionTaskWithRuntimeBinding(ctx, task, attemptID, fence, cause, nil)
}

func (d *ACPDispatcher) requeuePreSubmissionTaskWithRuntimeBinding(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	cause error,
	runtimeBinding *ACPRuntimeSessionBinding,
) error {
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	switch attempt.ExecutionState {
	case store.PromptExecutionReserved:
	case store.PromptExecutionSessionStarting, store.PromptExecutionPlanned:
		digest, err := acpDomainDigest("pre-admission-reconciliation", map[string]any{
			"attemptID": attempt.ID, "state": attempt.ExecutionState, "version": attempt.Version, "epoch": fence.Epoch,
		})
		if err != nil {
			return err
		}
		if _, err := d.Store.RecoverPromptAttemptPreSubmission(ctx, store.PromptAttemptPreSubmissionRecovery{
			ID: attempt.ID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState,
			OperationID: "reconcile-pre-admission-" + strconv.FormatInt(attempt.Version, 10), OperationDigest: digest,
			RecoveredAt: time.Now().UTC(),
		}); err != nil {
			return err
		}
	default:
		return fmt.Errorf("prompt attempt %s cannot reconcile pre-admission state %s: %w", attemptID, attempt.ExecutionState, cause)
	}
	return d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
		status.State = corev1alpha1.TaskExecutionStateReserved
		status.ControllerEpoch = fence.Epoch
		applyRuntimeSessionBindingToExecution(status, runtimeBinding)
		status.Reason = corev1alpha1.TaskExecutionReasonAtCapacity
		status.Message = "RuntimePool admission will be retried"
		status.LastTransitionTime = nowMeta()
	})
}

func applyRuntimeSessionBindingToExecution(
	status *corev1alpha1.TaskExecutionStatus,
	binding *ACPRuntimeSessionBinding,
) {
	if status == nil {
		return
	}
	if binding == nil {
		status.RuntimeInstanceID = ""
		status.RuntimeSessionUID = ""
		status.RuntimeSessionGeneration = 0
		status.RuntimeSessionSupervisorBootID = ""
		status.RuntimeSessionProfileDigest = ""
		status.RuntimeSessionMCPDigest = ""
		status.RuntimeSessionWorkspaceDigest = ""
		status.RuntimeSessionRecreationPending = false
		return
	}
	status.RuntimeInstanceID = string(binding.RuntimeInstanceID)
	status.RuntimeSessionUID = binding.SessionUID
	status.RuntimeSessionGeneration = int64(binding.Generation)
	status.RuntimeSessionSupervisorBootID = string(binding.SupervisorBootID)
	status.RuntimeSessionProfileDigest = string(binding.ProfileDigest)
	status.RuntimeSessionMCPDigest = binding.MCPDigest
	status.RuntimeSessionWorkspaceDigest = binding.WorkspaceDigest
	status.RuntimeSessionRecreationPending = binding.RecreationRequired
}

func (d *ACPDispatcher) waitForPromptCapacityReservation(
	ctx context.Context,
	task *corev1alpha1.Task,
	target acpDispatchTarget,
	fence store.ControllerEpochFence,
	expectedInstance harnessv2.RuntimeInstanceID,
) (*acpRuntimePoolReservationLease, error) {
	for {
		timer := time.NewTimer(d.effectiveRateLimitRetryInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if target.pool == nil {
			return nil, nil
		}
		pool, identity, err := d.claimRuntimePoolReservation(ctx, task, target.pool.Name, fence, 0)
		if err != nil {
			if errors.Is(err, errACPRuntimePoolAtCapacity) || errors.Is(err, errACPRuntimePoolNotAdmitting) {
				continue
			}
			return nil, err
		}
		if pool.Status.ActiveInstance == nil || pool.Status.ActiveInstance.RuntimeInstanceID != string(expectedInstance) {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			releaseErr := d.releaseRuntimePoolReservation(releaseCtx, *identity)
			cancel()
			if releaseErr != nil {
				return nil, releaseErr
			}
			return nil, fmt.Errorf("RuntimePool exact instance changed before prompt acceptance")
		}
		lease := newACPRuntimePoolReservationLease(d, identity)
		lease.startRenewal(ctx)
		if err := d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
			status.Reason = ""
			status.Message = ""
			status.LastTransitionTime = nowMeta()
		}); err != nil {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			_ = lease.release(releaseCtx)
			cancel()
			return nil, err
		}
		return lease, nil
	}
}

func runtimeSessionStartFailureMessage(err error) string {
	const fallback = "runtime session failed to start"
	var clientErr *harnessv2.ClientError
	if !errors.As(err, &clientErr) {
		return fallback
	}
	switch clientErr.Message {
	case "runtime session failed during identity allocation",
		"runtime session failed during path preparation",
		"runtime session failed during workspace materialization",
		"runtime session failed during workspace baseline capture",
		"runtime session failed during provider home preparation",
		"runtime session failed during ownership finalization",
		"runtime session failed during provider proxy setup",
		"runtime session failed during MCP proxy setup",
		"runtime session failed during provider environment setup",
		"runtime session failed during provider adapter initialization":
		return clientErr.Message
	default:
		return fallback
	}
}

// acpWorkspaceValidationFailureMessage projects the supervisor's categorized
// workspace-validation rejection (for example "reserved workspace path" or
// "workspace delta exceeds request limits") onto the Task delivery receipt so
// operators do not need controller logs to learn why a delta was refused.
// The supervisor already bounds and categorizes the message; only the client
// error text is used and it is re-bounded here.
func acpWorkspaceValidationFailureMessage(err error) string {
	const generic = "workspace validation failed before a trusted delta was established"
	var clientErr *harnessv2.ClientError
	if !errors.As(err, &clientErr) || strings.TrimSpace(clientErr.Message) == "" {
		return generic
	}
	detail := redact.SensitiveText(boundedRuntimeSessionServerMessage(err))
	return boundACPStatusMessage(generic + ": " + detail)
}

func boundedRuntimeSessionServerMessage(err error) string {
	var clientErr *harnessv2.ClientError
	if !errors.As(err, &clientErr) {
		return "non-client runtime session error"
	}
	message := strings.TrimSpace(stripACPControlRunes(clientErr.Message))
	if message == "" {
		return "empty runtime error response"
	}
	// Redact the complete message before bounding it: truncating first could
	// cut a credential-shaped value ahead of the text its recognizer needs.
	message = redact.SensitiveText(message)
	runes := []rune(message)
	if len(runes) > 256 {
		message = string(runes[:256])
	}
	return message
}

// stripACPControlRunes removes control characters (C0, DEL, and C1) and
// Unicode format runes from runtime-supplied text. Dropping every separator
// before redaction reassembles credentials split across lines or tabs while
// keeping terminal escapes and invisible runes out of status and logs.
func stripACPControlRunes(value string) string {
	return strings.Map(func(current rune) rune {
		switch {
		case current < 0x20 || current == 0x7f || (current >= 0x80 && current < 0xa0):
			return -1
		// Format runes (zero-width spaces, joiners, directional marks) are
		// as invisible as controls and equally capable of splitting a token.
		case unicode.Is(unicode.Cf, current):
			return -1
		}
		return current
	}, strings.ToValidUTF8(value, ""))
}

// runtimeSessionCreateDigestConflict reports whether the runtime rejected a
// create-session mutation because it already holds an operation record for the
// same create identity under a different request digest. The controller
// rebuilds the create request (fresh expiry, fresh workspace capability) on
// every reconcile of one attempt, so a re-admitted attempt whose earlier send
// was processed by the runtime is answered exactly this way.
func runtimeSessionCreateDigestConflict(err error) bool {
	clientErr, ok := errors.AsType[*harnessv2.ClientError](err)
	return ok && clientErr != nil && clientErr.Kind == harnessv2.ClientErrorHTTP &&
		clientErr.StatusCode == http.StatusConflict && clientErr.Code == harnessv2.ErrorCodeDigestConflict
}

func runtimeSessionCreationMayHaveApplied(err error) bool {
	var clientErr *harnessv2.ClientError
	if !errors.As(err, &clientErr) {
		return err != nil
	}
	if runtimeSessionCreateDigestConflict(err) {
		// The runtime holds an operation record for this exact create identity:
		// an earlier send of the same attempt was processed, so the session it
		// created may be resident and must be retired with the attempt.
		return true
	}
	switch clientErr.Kind {
	case harnessv2.ClientErrorConfiguration, harnessv2.ClientErrorValidation:
		return false
	case harnessv2.ClientErrorHTTP:
		// A received non-retryable client rejection is authoritative: the
		// supervisor rejected the mutation rather than ambiguously applying it.
		if clientErr.StatusCode >= http.StatusBadRequest && clientErr.StatusCode < http.StatusInternalServerError {
			return false
		}
	}
	return !clientErr.WriteEvidence.SafeToResendSameIdentity()
}

func runtimeSessionStartDiagnostic(err error) (int, harnessv2.ErrorCode, string) {
	var clientErr *harnessv2.ClientError
	if !errors.As(err, &clientErr) {
		return 0, "", "non-client runtime session error"
	}
	message := runtimeSessionStartFailureMessage(err)
	if message == "runtime session failed to start" {
		if clientErr.Message == "runtime operation failed; consult bounded supervisor diagnostics" {
			message = clientErr.Message
		} else {
			message = "runtime session request rejected before classified creation"
		}
	}
	return clientErr.StatusCode, clientErr.Code, message
}

func runtimeSessionWorkspaceResumeLost(err error) bool {
	clientErr, ok := errors.AsType[*harnessv2.ClientError](err)
	return ok && clientErr != nil && clientErr.Kind == harnessv2.ClientErrorHTTP &&
		clientErr.Code == harnessv2.ErrorCodeWorkspaceResumeLost && !clientErr.Retryable
}

func (d *ACPDispatcher) markTaskRuntimePoolWorkspaceResumeLost(ctx context.Context, task *corev1alpha1.Task) error {
	if task == nil || task.Status.Execution == nil {
		return fmt.Errorf("task execution is required to mark RuntimePool workspace resume loss")
	}
	key := types.NamespacedName{
		Namespace: task.Namespace,
		Name:      strings.TrimSpace(task.Status.Execution.RuntimePoolName),
	}
	expectedUID := types.UID(strings.TrimSpace(task.Status.Execution.RuntimePoolUID))
	if key.Name == "" || expectedUID == "" {
		return fmt.Errorf("task RuntimePool name and UID are required to mark workspace resume loss")
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		reader := d.APIReader
		if reader == nil {
			reader = d.Client
		}
		pool := &corev1alpha1.RuntimePool{}
		if err := reader.Get(ctx, key, pool); err != nil {
			return client.IgnoreNotFound(err)
		}
		if pool.UID != expectedUID || strings.TrimSpace(pool.Annotations[runtimePoolWorkspaceResumeLostAnnotation]) != "" {
			return nil
		}
		base := pool.DeepCopy()
		if pool.Annotations == nil {
			pool.Annotations = map[string]string{}
		}
		pool.Annotations[runtimePoolWorkspaceResumeLostAnnotation] =
			"the runtime supervisor rejected durable checkpoint verification; cold resume fails closed"
		if err := d.Client.Patch(ctx, pool, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("mark RuntimePool workspace resume loss: %w", err)
		}
		return nil
	})
}

func (d *ACPDispatcher) handlePrePromptClientError(ctx context.Context, task *corev1alpha1.Task, attemptID string, fence store.ControllerEpochFence, err error) (bool, error) {
	if isACPRateLimitedClientError(err) {
		return true, d.requeuePreSubmissionTask(ctx, task, attemptID, fence, err)
	}
	if runtimeSessionWorkspaceResumeLost(err) {
		if markErr := d.markTaskRuntimePoolWorkspaceResumeLost(ctx, task); markErr != nil {
			return false, markErr
		}
	}
	status, code, diagnostic := runtimeSessionStartDiagnostic(err)
	logf.FromContext(ctx).Info(
		"ACP runtime session start failed",
		"status", status,
		"code", code,
		"diagnostic", diagnostic,
		"serverMessage", boundedRuntimeSessionServerMessage(err),
	)
	if transitionErr := d.transitionAttemptToTerminal(ctx, attemptID, fence, store.PromptExecutionFailed, "runtime-session-start-failed"); transitionErr != nil {
		return false, transitionErr
	}
	return false, d.failTask(ctx, task, corev1alpha1.TaskExecutionStateFailed, corev1alpha1.TaskExecutionOutcomeFailed, "RuntimeSessionStartFailed", runtimeSessionStartFailureMessage(err))
}

func (d *ACPDispatcher) handlePromptStreamError(
	ctx context.Context,
	promptTrace *acpSpan,
	runtimeClient *harnessv2.Client,
	sessionID harnessv2.RuntimeSessionID,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	runtimeFence harnessv2.Fence,
	journalState *v2eventjournal.State,
	accepted bool,
	writeEvidence harnessv2.RequestWriteEvidence,
	runtimeContextErr error,
	err error,
) error {
	httpStatus, code, kind := 0, harnessv2.ErrorCode(""), harnessv2.ClientErrorKind("")
	if streamClientErr, ok := errors.AsType[*harnessv2.ClientError](err); ok {
		httpStatus, code, kind = streamClientErr.StatusCode, streamClientErr.Code, streamClientErr.Kind
	}
	logf.FromContext(ctx).Info(
		"ACP prompt stream failed",
		"accepted", accepted,
		"writeState", writeEvidence.State,
		"runtimeContextDone", runtimeContextErr != nil,
		"status", httpStatus,
		"code", code,
		"kind", kind,
		"diagnostic", promptStreamDiagnostic(err),
	)
	if persistenceErr, ok := errors.AsType[*acpExecutionUpdatePersistenceError](err); ok {
		// The diagnostic above is intentionally low-cardinality; the store
		// error itself is what an operator needs to fix a failing journal or
		// plan write, so record it once here before the Task is failed.
		logf.FromContext(ctx).Error(persistenceErr, "ACP execution update persistence failed",
			"namespace", task.Namespace, "task", task.Name,
			"journalFailed", persistenceErr.journalFailed(),
		)
		return d.handlePromptUpdatePersistenceFailure(
			ctx, runtimeClient, sessionID, task, attemptID, fence, runtimeFence, journalState, accepted, persistenceErr,
		)
	}
	if runtimeContextErr != nil {
		if !accepted && writeEvidence.SafeToResendSameIdentity() {
			operation, terminalReason, message := "cancelled-before-acceptance", corev1alpha1.TaskExecutionReason("Cancelled"), "prompt cancelled before acceptance"
			if errors.Is(runtimeContextErr, context.DeadlineExceeded) {
				operation, terminalReason, message = "timeout-before-acceptance", acpTaskTimeoutReason, "task deadline exceeded before prompt acceptance"
			}
			if transitionErr := d.transitionAttemptToCancelled(
				ctx, attemptID, fence, operation, terminalReason, message,
			); transitionErr != nil {
				return transitionErr
			}
			return recordACPPromptOutcomeIfSettled(
				ctx, promptTrace, acpPromptOutcomeCancelled,
				d.failTask(ctx, task, corev1alpha1.TaskExecutionStateCancelled, corev1alpha1.TaskExecutionOutcomeCancelled, terminalReason, message),
			)
		}
		now := time.Now().UTC()
		reason := harnessv2.CancelReasonControllerShutdown
		terminalReason := corev1alpha1.TaskExecutionReason("Cancelled")
		terminalMessage := "prompt cancellation settled"
		if errors.Is(runtimeContextErr, context.DeadlineExceeded) {
			reason = harnessv2.CancelReasonTaskTimeout
			terminalReason = acpTaskTimeoutReason
			terminalMessage = acpTaskTimeoutCancellationSettledMessage
		}
		cancelRequest := harnessv2.CancelPromptRequest{
			Protocol: harnessv2.ProtocolVersion,
			Metadata: mutationMetadata(runtimeFence, task, "cancel-prompt", true, now.Add(30*time.Second)),
			Reason:   reason, SettlementDeadline: now.Add(20 * time.Second),
		}
		if sealErr := sealMutation(&cancelRequest.Metadata.RequestDigest, cancelRequest); sealErr == nil {
			cancelCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
			defer cancel()
			response, cancelErr := runtimeClient.CancelPrompt(cancelCtx, sessionID, cancelRequest)
			if cancelErr == nil && response.SettlementProven {
				if lifecycleErr := appendPromptSettlementLifecycleDetached(
					ctx, journalState, response.Settlement, reason,
				); lifecycleErr != nil {
					logf.FromContext(ctx).Error(lifecycleErr, "persist proven ACP prompt settlement", "namespace", task.Namespace, "task", task.Name)
					return recordACPPromptOutcomeIfSettled(
						ctx, promptTrace, acpPromptOutcomeFailed,
						d.failPromptForExecutionEventPersistence(
							ctx, task, attemptID, fence, "proven prompt settlement lifecycle persistence failed",
						),
					)
				}
				switch response.Settlement.TerminalEvent {
				case harnessv2.EventCancelled:
					operation := acpCancelledOperation
					if terminalReason == corev1alpha1.TaskExecutionReason(acpTaskTimeoutReason) {
						operation = "timeout-cancelled"
					}
					if transitionErr := d.transitionAttemptToCancelled(
						ctx, attemptID, fence, operation, terminalReason, terminalMessage,
					); transitionErr != nil {
						return transitionErr
					}
					return recordACPPromptOutcomeIfSettled(
						ctx, promptTrace, acpPromptOutcomeCancelled,
						d.failTask(ctx, task, corev1alpha1.TaskExecutionStateCancelled, corev1alpha1.TaskExecutionOutcomeCancelled, terminalReason, terminalMessage),
					)
				case harnessv2.EventFailed:
					if transitionErr := d.transitionAttemptToTerminal(ctx, attemptID, fence, store.PromptExecutionFailed, "cancel-failed"); transitionErr != nil {
						return transitionErr
					}
					return recordACPPromptOutcomeIfSettled(
						ctx, promptTrace, acpPromptOutcomeFailed,
						d.failTask(ctx, task, corev1alpha1.TaskExecutionStateFailed, corev1alpha1.TaskExecutionOutcomeFailed, "PromptFailed", "prompt failed during cancellation"),
					)
				default:
					return recordACPPromptOutcomeIfSettled(
						ctx, promptTrace, acpPromptOutcomeUnknown,
						d.markOutcomeUnknown(ctx, task, attemptID, fence, "RuntimeLost", "prompt cancellation settled without a recoverable task result"),
					)
				}
			}
			logACPCancelSettlementUnknown(ctx, task, reason, response, cancelErr)
		} else {
			logf.FromContext(ctx).Error(sealErr, "seal ACP prompt cancellation request", "namespace", task.Namespace, "task", task.Name)
		}
		if lifecycleErr := appendPromptStreamFailureLifecycleDetached(ctx, journalState, err); lifecycleErr != nil {
			logf.FromContext(ctx).Error(lifecycleErr, "persist unknown ACP prompt settlement", "namespace", task.Namespace, "task", task.Name)
			return recordACPPromptOutcomeIfSettled(
				ctx, promptTrace, acpPromptOutcomeFailed,
				d.failPromptForExecutionEventPersistence(
					ctx, task, attemptID, fence, "unknown prompt settlement lifecycle persistence failed",
				),
			)
		}
		return recordACPPromptOutcomeIfSettled(
			ctx, promptTrace, acpPromptOutcomeUnknown,
			d.markOutcomeUnknown(ctx, task, attemptID, fence, "RuntimeLost", "prompt cancellation settlement is unknown"),
		)
	}
	var clientErr *harnessv2.ClientError
	if !accepted && errors.As(err, &clientErr) && clientErr.WriteEvidence.SafeToResendSameIdentity() {
		if transitionErr := d.transitionAttemptToTerminal(ctx, attemptID, fence, store.PromptExecutionFailed, "prompt-not-accepted"); transitionErr != nil {
			return transitionErr
		}
		return recordACPPromptOutcomeIfSettled(
			ctx, promptTrace, acpPromptOutcomeFailed,
			d.failTask(ctx, task, corev1alpha1.TaskExecutionStateFailed, corev1alpha1.TaskExecutionOutcomeFailed, "PromptNotAccepted", "prompt transport failed before any request bytes were written"),
		)
	}
	if lifecycleErr := appendPromptStreamFailureLifecycleDetached(ctx, journalState, err); lifecycleErr != nil {
		logf.FromContext(ctx).Error(lifecycleErr, "persist failed ACP prompt stream lifecycle", "namespace", task.Namespace, "task", task.Name)
		return recordACPPromptOutcomeIfSettled(
			ctx, promptTrace, acpPromptOutcomeFailed,
			d.failPromptForExecutionEventPersistence(
				ctx, task, attemptID, fence, "prompt stream lifecycle persistence failed",
			),
		)
	}
	return recordACPPromptOutcomeIfSettled(
		ctx, promptTrace, acpPromptOutcomeUnknown,
		d.markOutcomeUnknown(ctx, task, attemptID, fence, "RuntimeLost", "accepted prompt outcome is unknown"),
	)
}

func (d *ACPDispatcher) handlePromptUpdatePersistenceFailure(
	ctx context.Context,
	runtimeClient *harnessv2.Client,
	sessionID harnessv2.RuntimeSessionID,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	runtimeFence harnessv2.Fence,
	journalState *v2eventjournal.State,
	accepted bool,
	persistenceErr *acpExecutionUpdatePersistenceError,
) error {
	if !accepted {
		return d.failPromptForExecutionEventPersistence(
			ctx, task, attemptID, fence, "execution update persistence failed before prompt acceptance",
		)
	}

	now := time.Now().UTC()
	cancelRequest := harnessv2.CancelPromptRequest{
		Protocol: harnessv2.ProtocolVersion,
		Metadata: mutationMetadata(runtimeFence, task, "cancel-prompt", true, now.Add(30*time.Second)),
		Reason:   harnessv2.CancelReasonStreamDisconnected, SettlementDeadline: now.Add(20 * time.Second),
	}
	if sealErr := sealMutation(&cancelRequest.Metadata.RequestDigest, cancelRequest); sealErr == nil {
		cancelCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
		defer cancel()
		response, cancelErr := runtimeClient.CancelPrompt(cancelCtx, sessionID, cancelRequest)
		if cancelErr == nil && response.SettlementProven {
			if !persistenceErr.journalFailed() {
				if lifecycleErr := appendPromptSettlementLifecycleDetached(
					ctx, journalState, response.Settlement, cancelRequest.Reason,
				); lifecycleErr != nil {
					logf.FromContext(ctx).Error(lifecycleErr, "persist ACP prompt settlement after plan persistence failure", "namespace", task.Namespace, "task", task.Name)
					return d.failPromptForExecutionEventPersistence(
						ctx, task, attemptID, fence, "prompt settlement lifecycle persistence failed after plan persistence failure",
					)
				}
			}
			return d.failPromptForExecutionEventPersistence(
				ctx, task, attemptID, fence, "prompt was settled after execution update persistence failed",
			)
		}
		logACPCancelSettlementUnknown(ctx, task, cancelRequest.Reason, response, cancelErr)
	}
	if !persistenceErr.journalFailed() {
		if lifecycleErr := appendPromptStreamFailureLifecycleDetached(ctx, journalState, persistenceErr); lifecycleErr != nil {
			logf.FromContext(ctx).Error(lifecycleErr, "persist unknown ACP prompt settlement after plan persistence failure", "namespace", task.Namespace, "task", task.Name)
			return d.failPromptForExecutionEventPersistence(
				ctx, task, attemptID, fence, "unknown prompt settlement lifecycle persistence failed after plan persistence failure",
			)
		}
	}
	return d.markOutcomeUnknown(
		ctx, task, attemptID, fence, acpExecutionEventPersistenceFailureReason,
		"prompt settlement is unknown after execution update persistence failed",
	)
}

func (d *ACPDispatcher) failPromptForExecutionEventPersistence(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	message string,
) error {
	if transitionErr := d.transitionAttemptToTerminal(
		ctx, attemptID, fence, store.PromptExecutionFailed, "event-persistence-failed",
	); transitionErr != nil {
		return transitionErr
	}
	return d.failTask(
		ctx, task, corev1alpha1.TaskExecutionStateFailed, corev1alpha1.TaskExecutionOutcomeFailed,
		acpExecutionEventPersistenceFailureReason, message,
	)
}

const promptStreamMissingTerminalDiagnostic = "runtime stream ended without a terminal event"

// acpInterruptedOutputFlushTimeout bounds the durable write of buffered
// assistant/tool output after a prompt stream ends. The journal shares one
// SQLite connection with every other controller write, so a short deadline
// turns ordinary contention between concurrent ACP prompts into a failed
// Task with ExecutionEventPersistenceFailed.
const acpInterruptedOutputFlushTimeout = 60 * time.Second

const (
	acpCancelLogKeyNamespace = "namespace"
	acpCancelLogKeyTask      = "task"
	acpCancelLogKeyStatus    = "status"
	acpCancelLogKeyKind      = "kind"
)

// logACPCancelSettlementUnknown records why an explicit prompt cancellation
// could not prove settlement, so an OutcomeUnknown/RuntimeLost Task can be
// diagnosed from controller logs. Fields stay low-cardinality: transport
// status/code/kind for a client error, and the runtime's barrier, forced
// termination, and terminal event when the request itself succeeded.
func logACPCancelSettlementUnknown(
	ctx context.Context,
	task *corev1alpha1.Task,
	reason harnessv2.CancelReason,
	response *harnessv2.CancelPromptResponse,
	cancelErr error,
) {
	httpStatus, code, kind := 0, harnessv2.ErrorCode(""), harnessv2.ClientErrorKind("")
	if clientErr, ok := errors.AsType[*harnessv2.ClientError](cancelErr); ok {
		httpStatus, code, kind = clientErr.StatusCode, clientErr.Code, clientErr.Kind
	}
	fields := []any{
		acpCancelLogKeyNamespace, task.Namespace,
		acpCancelLogKeyTask, task.Name,
		eventReasonField, reason,
		"requestFailed", cancelErr != nil,
		"timedOut", errors.Is(cancelErr, context.DeadlineExceeded),
		acpCancelLogKeyStatus, httpStatus,
		"code", code,
		acpCancelLogKeyKind, kind,
	}
	if response != nil {
		fields = append(fields,
			"classification", response.Classification.Class,
			"barrier", response.BarrierState,
			"forcedTermination", response.ForcedTermination,
			"terminalEvent", response.Settlement.TerminalEvent,
			"settlementProven", response.SettlementProven,
		)
	}
	logf.FromContext(ctx).Info("ACP prompt cancellation settlement unknown", fields...)
}

func promptStreamDiagnostic(err error) string {
	var persistenceErr *acpExecutionUpdatePersistenceError
	switch {
	case err == nil:
		return "no stream error"
	case errors.As(err, &persistenceErr):
		return "local execution update persistence failed"
	case errors.Is(err, harnessv2.ErrEventLineTooLarge):
		return "event line exceeded the negotiated limit"
	case errors.Is(err, harnessv2.ErrMalformedEvent):
		return "runtime emitted malformed event JSON"
	case errors.Is(err, harnessv2.ErrEventIdentityMismatch):
		return "runtime event identity did not match the prompt fence"
	case errors.Is(err, harnessv2.ErrEventSequence):
		return "runtime event sequence was invalid"
	case errors.Is(err, harnessv2.ErrEventRateExceeded):
		return "runtime update rate exceeded the negotiated limit"
	case errors.Is(err, harnessv2.ErrEventByteRateExceeded):
		return "runtime update byte rate exceeded the protocol limit"
	case errors.Is(err, harnessv2.ErrEventAfterTerminal):
		return "runtime emitted an event after terminal settlement"
	case errors.Is(err, harnessv2.ErrMissingAcceptedEvent):
		return "runtime stream omitted the accepted event"
	case errors.Is(err, harnessv2.ErrMissingTerminalEvent):
		return promptStreamMissingTerminalDiagnostic
	case errors.Is(err, harnessv2.ErrBufferedEventOverflow):
		return "runtime buffered event limit was exceeded"
	case errors.Is(err, harnessv2.ErrPromptStreamDisconnected):
		return "runtime prompt stream disconnected"
	case errors.Is(err, context.DeadlineExceeded):
		return "prompt stream deadline exceeded"
	case errors.Is(err, context.Canceled):
		return "prompt stream context was cancelled"
	case errors.Is(err, harnessv2.ErrClientProtocol):
		return "runtime prompt response violated the protocol"
	case errors.Is(err, harnessv2.ErrClientTransport):
		return "runtime prompt transport failed"
	case errors.Is(err, harnessv2.ErrClientStream):
		return "runtime prompt stream failed"
	default:
		return "non-client prompt stream error"
	}
}

func appendPromptStreamFailureLifecycleIfNew(
	ctx context.Context,
	journalState *v2eventjournal.State,
	at time.Time,
	streamErr error,
) error {
	_, _, err := journalState.AppendPromptStreamFailureIfNew(ctx, at, promptStreamDiagnostic(streamErr))
	return err
}

func appendPromptStreamFailureLifecycleDetached(
	ctx context.Context,
	journalState *v2eventjournal.State,
	streamErr error,
) error {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return appendPromptStreamFailureLifecycleIfNew(persistCtx, journalState, time.Now().UTC(), streamErr)
}

func appendPromptSettlementLifecycleDetached(
	ctx context.Context,
	journalState *v2eventjournal.State,
	settlement harnessv2.PromptSettlement,
	cancellationReason harnessv2.CancelReason,
) error {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	_, _, err := journalState.AppendPromptSettlementIfNew(persistCtx, settlement, cancellationReason)
	return err
}

func (d *ACPDispatcher) markOutcomeUnknown(ctx context.Context, task *corev1alpha1.Task, attemptID string, fence store.ControllerEpochFence, reason, message string) error {
	if err := d.persistOutcomeUnknown(ctx, attemptID, fence, reason, message); err != nil {
		return err
	}
	return d.failTask(ctx, task, corev1alpha1.TaskExecutionStateOutcomeUnknown, corev1alpha1.TaskExecutionOutcomeOutcomeUnknown, corev1alpha1.TaskExecutionReason(reason), message)
}

func (d *ACPDispatcher) persistOutcomeUnknown(ctx context.Context, attemptID string, fence store.ControllerEpochFence, reason, message string) error {
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	if attempt.ExecutionState == store.PromptExecutionSubmitting {
		if err := d.transitionAttempt(ctx, attemptID, fence, store.PromptExecutionSubmitting, store.PromptExecutionSubmittedUnknown, "submitted-unknown", nil); err != nil {
			return err
		}
		attempt, err = d.Store.GetPromptAttempt(ctx, attemptID)
		if err != nil {
			return err
		}
		if attempt == nil {
			return fmt.Errorf("%w: prompt attempt %s is missing after submitted-unknown transition", store.ErrNotFound, attemptID)
		}
	}
	from := attempt.ExecutionState
	if from == store.PromptExecutionSubmittedUnknown || from == store.PromptExecutionAccepted || from == store.PromptExecutionRunning || from == store.PromptExecutionSettling {
		attempt, err = d.Store.GetPromptAttempt(ctx, attemptID)
		if err != nil {
			return err
		}
		digest, digestErr := acpDomainDigest("attempt-transition", map[string]any{"id": attemptID, "from": from, "to": store.PromptExecutionOutcomeUnknown, "operation": "outcome-unknown", "version": attempt.Version, "marker": message})
		if digestErr != nil {
			return digestErr
		}
		if _, err := d.Store.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
			ID: attemptID, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: from, NewState: store.PromptExecutionOutcomeUnknown,
			OperationID: "outcome-unknown-" + strconv.FormatInt(attempt.Version, 10), OperationDigest: digest,
			TerminalReason: reason, OutcomeMarker: message, UpdatedAt: time.Now().UTC(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (d *ACPDispatcher) finishNonSuccess(ctx context.Context, task *corev1alpha1.Task, attemptID string, fence store.ControllerEpochFence, session *acpTaskSession, terminal harnessv2.Event) error {
	return d.finishNonSuccessWithCancellationReason(ctx, task, attemptID, fence, session, terminal, "")
}

func (d *ACPDispatcher) finishNonSuccessWithCancellationReason(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	session *acpTaskSession,
	terminal harnessv2.Event,
	cancellationReason harnessv2.CancelReason,
) error {
	switch terminal.Type {
	case harnessv2.EventCancelled:
		operation := acpCancelledOperation
		reason := corev1alpha1.TaskExecutionReason("Cancelled")
		message := "prompt cancelled"
		if cancellationReason == harnessv2.CancelReasonTaskTimeout {
			operation = "timeout-cancelled"
			reason = acpTaskTimeoutReason
			message = acpTaskTimeoutCancellationSettledMessage
		}
		if err := d.transitionAttemptToCancelled(
			ctx, attemptID, fence, operation, reason, message,
		); err != nil {
			return err
		}
		execution := corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateCancelled, Outcome: corev1alpha1.TaskExecutionOutcomeCancelled,
			Attempt: task.Status.Execution.Attempt, PromptID: task.Status.Execution.PromptID,
			Reason: reason, Message: message,
		}
		if err := d.finalizeTaskSessionMarker(ctx, task, fence, session, "Cancelled", message, corev1alpha1.TaskPhaseCancelled, execution); err != nil {
			return err
		}
		return d.failTask(ctx, task, corev1alpha1.TaskExecutionStateCancelled, corev1alpha1.TaskExecutionOutcomeCancelled, reason, message)
	case harnessv2.EventOutcomeUnknown:
		if err := d.persistOutcomeUnknown(ctx, attemptID, fence, "RuntimeLost", "prompt outcome is unknown"); err != nil {
			return err
		}
		if err := d.finalizeTaskSessionUnknown(ctx, task, fence, session, "prompt outcome is unknown"); err != nil {
			return err
		}
		return d.failTask(ctx, task, corev1alpha1.TaskExecutionStateOutcomeUnknown, corev1alpha1.TaskExecutionOutcomeOutcomeUnknown, "RuntimeLost", "prompt outcome is unknown")
	default:
		message := acpPromptFailureMessage(terminal)
		if err := d.transitionAttemptToFailed(ctx, attemptID, fence, "failed", acpPromptFailedReason, message); err != nil {
			return err
		}
		execution := corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateFailed, Outcome: corev1alpha1.TaskExecutionOutcomeFailed,
			Attempt: task.Status.Execution.Attempt, PromptID: task.Status.Execution.PromptID,
			Reason: acpPromptFailedReason, Message: message,
		}
		if err := d.finalizeTaskSessionMarker(ctx, task, fence, session, "Failed", message, corev1alpha1.TaskPhaseFailed, execution); err != nil {
			return err
		}
		return d.failTask(ctx, task, corev1alpha1.TaskExecutionStateFailed, corev1alpha1.TaskExecutionOutcomeFailed, acpPromptFailedReason, message)
	}
}

// acpPromptFailedReason classifies a prompt that the runtime settled as
// failed; the message carries the runtime's bounded failure code and detail.
const acpPromptFailedReason corev1alpha1.TaskExecutionReason = "PromptFailed"

// acpPromptFailureMessageLimit bounds the projected Task failure message so a
// runtime-supplied detail can never bloat Task status.
const acpPromptFailureMessageLimit = 512

// acpPromptFailureMessage projects the runtime's terminal Failed event into a
// human-readable Task message. The generic "prompt failed" text is kept when
// the runtime reported no code or detail; otherwise the runtime's bounded
// failure code and message are appended so operators can distinguish provider
// upstream errors, turn limits, and refusals without reading runtime logs.
func acpPromptFailureMessage(terminal harnessv2.Event) string {
	const generic = "prompt failed"
	if terminal.Failed == nil {
		return generic
	}
	// Both fields are runtime-controlled (only bounded by the harness).
	// Controls are stripped before redaction so a control byte cannot split a
	// credential-shaped value past the redactor, and the composed logical
	// value is redacted as one string: a credential assignment split across
	// the code and the message ("password" / "hunter2") is only recognizable
	// once they are joined, so redacting the fields separately would persist
	// the raw value in Task status, the PromptAttempt, and the Session turn.
	detail := strings.TrimSpace(stripACPControlRunes(terminal.Failed.Message))
	code := strings.TrimSpace(stripACPControlRunes(terminal.Failed.Code))
	switch {
	case detail == "" && code == "":
		return generic
	case detail == "":
		detail = code
	case code != "" && code != "acp_prompt_failed" && !strings.HasPrefix(detail, code):
		detail = code + ": " + detail
	}
	return boundACPStatusMessage(generic + ": " + redact.SensitiveText(detail))
}

// boundACPStatusMessage truncates a runtime-derived status message to
// acpPromptFailureMessageLimit bytes on a rune boundary so the persisted
// message stays valid UTF-8 for the control store.
func boundACPStatusMessage(message string) string {
	if len(message) <= acpPromptFailureMessageLimit {
		return message
	}
	limit := acpPromptFailureMessageLimit
	for limit > 0 && !utf8.RuneStart(message[limit]) {
		limit--
	}
	return message[:limit]
}

// transitionAttemptToFailed mirrors transitionAttemptToCancelled for the
// Failed terminal state so the durable PromptAttempt records the same reason
// and message that the Task projection exposes; controller-restart recovery
// then reproduces that classification instead of the generic default.
func (d *ACPDispatcher) transitionAttemptToFailed(
	ctx context.Context,
	id string,
	fence store.ControllerEpochFence,
	operation string,
	reason corev1alpha1.TaskExecutionReason,
	message string,
) error {
	if strings.TrimSpace(string(reason)) == "" || strings.TrimSpace(message) == "" {
		return errors.New("terminal attempt classification requires a reason and message")
	}
	attempt, err := d.Store.GetPromptAttempt(ctx, id)
	if err != nil {
		return err
	}
	target := store.PromptExecutionFailed
	if attempt.ExecutionState == target {
		if attempt.TerminalReason != string(reason) || attempt.OutcomeMarker != message {
			return fmt.Errorf("%w: prompt attempt %s terminal classification does not match", store.ErrConflict, id)
		}
		return nil
	}
	if err := store.ValidatePromptExecutionTransition(attempt.ExecutionState, target); err != nil {
		return err
	}
	digest, err := acpDomainDigest("attempt-transition", map[string]any{
		"id": id, "from": attempt.ExecutionState, "to": target, "operation": operation,
		"version": attempt.Version, "terminalReason": reason, "outcomeMarker": message,
	})
	if err != nil {
		return err
	}
	_, err = d.Store.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
		ID: id, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState, NewState: target,
		OperationID: operation + "-" + strconv.FormatInt(attempt.Version, 10), OperationDigest: digest,
		TerminalReason: string(reason), OutcomeMarker: message, UpdatedAt: time.Now().UTC(),
	})
	return err
}

func (d *ACPDispatcher) transitionAttemptToTerminal(ctx context.Context, id string, fence store.ControllerEpochFence, target store.PromptExecutionState, operation string) error {
	attempt, err := d.Store.GetPromptAttempt(ctx, id)
	if err != nil {
		return err
	}
	if attempt.ExecutionState == target {
		return nil
	}
	if err := store.ValidatePromptExecutionTransition(attempt.ExecutionState, target); err != nil {
		return err
	}
	return d.transitionAttempt(ctx, id, fence, attempt.ExecutionState, target, operation, nil)
}

func (d *ACPDispatcher) transitionAttemptToCancelled(
	ctx context.Context,
	id string,
	fence store.ControllerEpochFence,
	operation string,
	reason corev1alpha1.TaskExecutionReason,
	message string,
) error {
	if strings.TrimSpace(string(reason)) == "" || strings.TrimSpace(message) == "" {
		return errors.New("terminal attempt classification requires a reason and message")
	}
	attempt, err := d.Store.GetPromptAttempt(ctx, id)
	if err != nil {
		return err
	}
	target := store.PromptExecutionCancelled
	if attempt.ExecutionState == target {
		if attempt.TerminalReason != string(reason) || attempt.OutcomeMarker != message {
			return fmt.Errorf("%w: prompt attempt %s terminal classification does not match", store.ErrConflict, id)
		}
		return nil
	}
	if err := store.ValidatePromptExecutionTransition(attempt.ExecutionState, target); err != nil {
		return err
	}
	digest, err := acpDomainDigest("attempt-transition", map[string]any{
		"id": id, "from": attempt.ExecutionState, "to": target, "operation": operation,
		"version": attempt.Version, "terminalReason": reason, "outcomeMarker": message,
	})
	if err != nil {
		return err
	}
	_, err = d.Store.TransitionPromptAttemptExecution(ctx, store.PromptAttemptExecutionTransition{
		ID: id, Fence: fence, ExpectedVersion: attempt.Version, ExpectedState: attempt.ExecutionState, NewState: target,
		OperationID: operation + "-" + strconv.FormatInt(attempt.Version, 10), OperationDigest: digest,
		TerminalReason: string(reason), OutcomeMarker: message, UpdatedAt: time.Now().UTC(),
	})
	return err
}

func (d *ACPDispatcher) failTask(ctx context.Context, task *corev1alpha1.Task, state corev1alpha1.TaskExecutionState, outcome corev1alpha1.TaskExecutionOutcome, reason corev1alpha1.TaskExecutionReason, message string) error {
	return d.failTaskWithProjection(ctx, task, state, outcome, reason, message, false)
}

func (d *ACPDispatcher) failTaskBeforeSessionBinding(ctx context.Context, task *corev1alpha1.Task, state corev1alpha1.TaskExecutionState, outcome corev1alpha1.TaskExecutionOutcome, reason corev1alpha1.TaskExecutionReason, message string) error {
	return d.failTaskWithProjection(ctx, task, state, outcome, reason, message, true)
}

func (d *ACPDispatcher) failTaskWithProjection(
	ctx context.Context,
	task *corev1alpha1.Task,
	state corev1alpha1.TaskExecutionState,
	outcome corev1alpha1.TaskExecutionOutcome,
	reason corev1alpha1.TaskExecutionReason,
	message string,
	allowSessionRef bool,
) error {
	phase := corev1alpha1.TaskPhaseFailed
	if state == corev1alpha1.TaskExecutionStateCancelled {
		phase = corev1alpha1.TaskPhaseCancelled
	}
	// Build the projection from a deep copy of the frozen execution identity
	// and overlay only the terminal classification: reclamation validates the
	// payload against the Task's complete runtime identity, so a hand-picked
	// field subset would leave the source PromptAttempt impossible to retire.
	execution := terminalProjectionExecution(task, state, outcome, reason, message)
	payload := taskTerminalProjection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: string(task.UID), Attempt: execution.Attempt,
		Phase: phase, Message: message, Execution: execution, Delivery: task.Status.Delivery,
	}
	var projectionErr error
	if allowSessionRef {
		projectionErr = d.enqueueUnboundSessionTaskProjection(ctx, task, payload)
	} else {
		projectionErr = d.enqueueStandaloneTaskProjection(ctx, task, payload)
	}
	if projectionErr != nil {
		return projectionErr
	}
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		base := latest.DeepCopy()
		now := metav1.Now()
		if latest.Status.Execution == nil {
			latest.Status.Execution = &corev1alpha1.TaskExecutionStatus{}
		}
		latest.Status.Execution.State = state
		latest.Status.Execution.Outcome = outcome
		latest.Status.Execution.Reason = reason
		latest.Status.Execution.Message = message
		latest.Status.Execution.LastTransitionTime = &now
		latest.Status.Phase = phase
		return d.Client.Status().Patch(ctx, latest, client.MergeFrom(base))
	})
}

func (d *ACPDispatcher) requeueReservedTask(
	ctx context.Context,
	task *corev1alpha1.Task,
	stage acpReservedRetryStage,
	cause error,
) error {
	_ = cause
	// Reservation remains durable and is retried by this dispatcher. Surface the
	// bounded stage without changing the execution state or consuming a Task
	// retry. The underlying error is deliberately excluded because runtime and
	// credential errors are not a safe status surface.
	return d.patchExecution(ctx, task, func(status *corev1alpha1.TaskExecutionStatus) {
		status.Reason = corev1alpha1.TaskExecutionReasonAtCapacity
		status.Message = fmt.Sprintf("RuntimePool admission will be retried (stage: %s)", stage)
	})
}

func nowMeta() *metav1.Time {
	now := metav1.Now()
	return &now
}
