/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
)

const (
	DefaultACPUpgradeDrainBindAddress    = "127.0.0.1:8083"
	DefaultACPUpgradeDrainPath           = "/acp/upgrade-drain"
	DefaultACPUpgradeDrainTimeout        = 5 * time.Minute
	DefaultACPUpgradeDrainPollInterval   = time.Second
	DefaultACPUpgradeDrainTriggerTimeout = 5*time.Minute + 15*time.Second

	acpUpgradeDrainMarkerAnnotation = "core.orka.ai/acp-upgrade-drain-marker"
)

var (
	ErrACPAdmissionClosed       = errors.New("ACP admission is closed")
	ErrACPUpgradeDrainTimedOut  = errors.New("ACP planned upgrade drain timed out")
	ErrACPUpgradeDrainEpochLost = errors.New("controller epoch changed during ACP planned upgrade drain")
)

// ACPAdmissionGate is the process-local first barrier for new ACP demand. A
// planned drain closes it before any RuntimePool mutation. Callers must also
// check it inside the final RuntimePool reservation CAS so an admission that
// raced an earlier queue scan cannot cross the shutdown boundary.
type ACPAdmissionGate struct {
	mu       sync.RWMutex
	closed   bool
	reason   string
	closedAt time.Time
}

func NewACPAdmissionGate() *ACPAdmissionGate { return &ACPAdmissionGate{} }

func (g *ACPAdmissionGate) Close(reason string, now time.Time) {
	if g == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "ACP admission closed"
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	g.closed = true
	g.reason = reason
	g.closedAt = now
}

func (g *ACPAdmissionGate) Check() error {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if !g.closed {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrACPAdmissionClosed, g.reason)
}

func (g *ACPAdmissionGate) Closed() (bool, string, time.Time) {
	if g == nil {
		return false, "", time.Time{}
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.closed, g.reason, g.closedAt
}

// ACPUpgradeDrainOptions is shared by the normal controller process and the
// same-binary preStop trigger mode. The trigger listener is deliberately
// loopback-only; the preStop child shares the Pod network namespace.
type ACPUpgradeDrainOptions struct {
	BindAddress       string
	Timeout           time.Duration
	PollInterval      time.Duration
	MarkerNamespace   string
	WatchNamespace    string
	RuntimePoolLabels map[string]string
	TriggerURL        string
	TriggerTimeout    time.Duration
}

func DefaultACPUpgradeDrainOptions() ACPUpgradeDrainOptions {
	return ACPUpgradeDrainOptions{
		BindAddress:       DefaultACPUpgradeDrainBindAddress,
		Timeout:           DefaultACPUpgradeDrainTimeout,
		PollInterval:      DefaultACPUpgradeDrainPollInterval,
		RuntimePoolLabels: map[string]string{acpRuntimePoolLabel: scheduledRunLabelValue},
		TriggerTimeout:    DefaultACPUpgradeDrainTriggerTimeout,
	}
}

// BindFlags registers all flags required by cmd/main.go. TriggerURL is empty in
// the long-running controller and non-empty only in the preStop child process.
func (o *ACPUpgradeDrainOptions) BindFlags(fs *flag.FlagSet) {
	if o == nil || fs == nil {
		return
	}
	defaults := DefaultACPUpgradeDrainOptions()
	if strings.TrimSpace(o.BindAddress) == "" {
		o.BindAddress = defaults.BindAddress
	}
	if o.Timeout <= 0 {
		o.Timeout = defaults.Timeout
	}
	if o.PollInterval <= 0 {
		o.PollInterval = defaults.PollInterval
	}
	if o.TriggerTimeout <= 0 {
		o.TriggerTimeout = defaults.TriggerTimeout
	}
	fs.StringVar(&o.BindAddress, "acp-upgrade-drain-bind-address", o.BindAddress, "Loopback address for the ACP planned-upgrade drain trigger.")
	fs.DurationVar(&o.Timeout, "acp-upgrade-drain-timeout", o.Timeout, "Maximum time to reach ACP planned-upgrade quiescence before falling back to unplanned takeover semantics.")
	fs.DurationVar(&o.PollInterval, "acp-upgrade-drain-poll-interval", o.PollInterval, "Polling interval for ACP planned-upgrade barriers.")
	fs.StringVar(&o.MarkerNamespace, "acp-upgrade-drain-marker-namespace", o.MarkerNamespace, "Namespace containing the authoritative controller-epoch Lease used for the durable planned-upgrade marker.")
	fs.StringVar(&o.TriggerURL, "acp-upgrade-drain-trigger-url", o.TriggerURL, "Loopback URL used only by the preStop child to trigger the running controller.")
	fs.DurationVar(&o.TriggerTimeout, "acp-upgrade-drain-trigger-timeout", o.TriggerTimeout, "Timeout for the preStop child waiting for the running controller drain result.")
}

// ACPUpgradeDrainEpochSource supplies the epoch already acquired after
// controller-runtime leader election.
type ACPUpgradeDrainEpochSource interface {
	CurrentFence(context.Context) (store.ControllerEpochFence, error)
}

// ACPUpgradeDrainOutboxBarrier must report all Pending or Delivering outbox
// records, including records whose retry availability is in the future.
type ACPUpgradeDrainOutboxBarrier interface {
	CountUnsettledOutboxProjections(context.Context) (int64, error)
}

// ACPUpgradeDrainOutboxBarrierFunc adapts a function for store-specific wiring.
type ACPUpgradeDrainOutboxBarrierFunc func(context.Context) (int64, error)

func (f ACPUpgradeDrainOutboxBarrierFunc) CountUnsettledOutboxProjections(ctx context.Context) (int64, error) {
	return f(ctx)
}

// ACPUpgradeDrainBarrierSnapshot is the durable work outside RuntimePool
// supervisor state that must settle before a planned leader handoff.
type ACPUpgradeDrainBarrierSnapshot struct {
	UnsettledOutboxProjections int64 `json:"unsettledOutboxProjections"`
	NonterminalPromptAttempts  int64 `json:"nonterminalPromptAttempts"`
	ActiveSessionControls      int64 `json:"activeSessionControls"`
	NonterminalPublications    int64 `json:"nonterminalPublications"`
	NonterminalExternalEffects int64 `json:"nonterminalExternalEffects"`
}

func (s ACPUpgradeDrainBarrierSnapshot) Quiescent() bool {
	return s.UnsettledOutboxProjections == 0 && s.NonterminalPromptAttempts == 0 &&
		s.ActiveSessionControls == 0 && s.NonterminalPublications == 0 && s.NonterminalExternalEffects == 0
}

// ACPUpgradeDrainBarrierObserver reads the publication, external-effect, and
// transactional-outbox barriers without mutating or claiming them.
type ACPUpgradeDrainBarrierObserver interface {
	ObserveACPUpgradeDrainBarriers(context.Context) (ACPUpgradeDrainBarrierSnapshot, error)
}

// ACPUpgradeDrainBarrierObserverFunc adapts a function for tests or a composite
// store implementation during the Kubernetes control-store cutover.
type ACPUpgradeDrainBarrierObserverFunc func(context.Context) (ACPUpgradeDrainBarrierSnapshot, error)

func (f ACPUpgradeDrainBarrierObserverFunc) ObserveACPUpgradeDrainBarriers(ctx context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
	return f(ctx)
}

// KubernetesACPUpgradeDrainBarrierObserver treats Kubernetes PromptAttempt,
// RuntimeSessionControl, Publication, and ExternalEffect records as
// authoritative. It samples the SQLite-backed outbox before and after those
// sinks so a Delivering record cannot disappear into newly projected work.
type KubernetesACPUpgradeDrainBarrierObserver struct {
	Reader    client.Reader
	Outbox    ACPUpgradeDrainOutboxBarrier
	Namespace string
}

func (o *KubernetesACPUpgradeDrainBarrierObserver) ObserveACPUpgradeDrainBarriers(ctx context.Context) (ACPUpgradeDrainBarrierSnapshot, error) {
	if o == nil || o.Reader == nil || o.Outbox == nil {
		return ACPUpgradeDrainBarrierSnapshot{}, fmt.Errorf("ACP upgrade drain barrier observer requires Kubernetes reader and outbox barrier")
	}
	var snapshot ACPUpgradeDrainBarrierSnapshot
	before, err := o.Outbox.CountUnsettledOutboxProjections(ctx)
	if err != nil {
		return snapshot, fmt.Errorf("count unsettled outbox projections before sink observation: %w", err)
	}
	if before < 0 {
		return snapshot, fmt.Errorf("unsettled outbox projection count cannot be negative")
	}
	listOptions := []client.ListOption{}
	if strings.TrimSpace(o.Namespace) != "" {
		listOptions = append(listOptions, client.InNamespace(strings.TrimSpace(o.Namespace)))
	}
	var attempts corev1alpha1.PromptAttemptList
	if err := o.Reader.List(ctx, &attempts, listOptions...); err != nil {
		return snapshot, fmt.Errorf("list PromptAttempts for upgrade drain: %w", err)
	}
	for i := range attempts.Items {
		if promptAttemptHasUpgradeDrainWork(&attempts.Items[i]) {
			snapshot.NonterminalPromptAttempts++
		}
	}
	var sessions corev1alpha1.RuntimeSessionControlList
	if err := o.Reader.List(ctx, &sessions, listOptions...); err != nil {
		return snapshot, fmt.Errorf("list RuntimeSessionControls for upgrade drain: %w", err)
	}
	for i := range sessions.Items {
		if runtimeSessionControlHasUpgradeDrainWork(&sessions.Items[i]) {
			snapshot.ActiveSessionControls++
		}
	}
	var publications corev1alpha1.PublicationList
	if err := o.Reader.List(ctx, &publications, listOptions...); err != nil {
		return snapshot, fmt.Errorf("list Publications for upgrade drain: %w", err)
	}
	for i := range publications.Items {
		if !store.IsTerminalPublicationState(store.PublicationState(publications.Items[i].Status.State)) {
			snapshot.NonterminalPublications++
		}
	}
	var effects corev1alpha1.ExternalEffectList
	if err := o.Reader.List(ctx, &effects, listOptions...); err != nil {
		return snapshot, fmt.Errorf("list ExternalEffects for upgrade drain: %w", err)
	}
	for i := range effects.Items {
		switch store.ExternalEffectState(effects.Items[i].Status.State) {
		case store.ExternalEffectSucceeded, store.ExternalEffectFailed, store.ExternalEffectOutcomeUnknown:
		default:
			snapshot.NonterminalExternalEffects++
		}
	}
	after, err := o.Outbox.CountUnsettledOutboxProjections(ctx)
	if err != nil {
		return snapshot, fmt.Errorf("count unsettled outbox projections after sink observation: %w", err)
	}
	if after < 0 {
		return snapshot, fmt.Errorf("unsettled outbox projection count cannot be negative")
	}
	snapshot.UnsettledOutboxProjections = max(before, after)
	return snapshot, nil
}

func promptAttemptHasUpgradeDrainWork(attempt *corev1alpha1.PromptAttempt) bool {
	if attempt == nil {
		return false
	}
	execution := store.PromptExecutionState(attempt.Status.ExecutionState)
	delivery := store.PromptDeliveryState(attempt.Status.DeliveryState)
	if execution == store.PromptExecutionQueued && delivery == store.PromptDeliveryNotRequested {
		return false
	}
	return !store.IsTerminalPromptExecutionState(execution) || !store.IsTerminalPromptDeliveryState(delivery)
}

func runtimeSessionControlHasUpgradeDrainWork(control *corev1alpha1.RuntimeSessionControl) bool {
	if control == nil {
		return false
	}
	if control.Status.MutationLease != nil {
		return true
	}
	if control.Status.Availability == corev1alpha1.RuntimeSessionControlAvailability(store.SessionAvailable) &&
		strings.TrimSpace(control.Status.BlockedReason) == "" && control.Status.RelatedPromptAttemptID == "" && control.Status.RelatedPublicationID == "" {
		return false
	}
	switch control.Status.Lifecycle {
	case corev1alpha1.RuntimeSessionControlLifecycle("Idle"),
		corev1alpha1.RuntimeSessionControlLifecycle("Poisoned"),
		corev1alpha1.RuntimeSessionControlLifecycle("Deleted"):
		return false
	default:
		return true
	}
}

type ACPUpgradeDrainPhase string

const (
	ACPUpgradeDrainReady     ACPUpgradeDrainPhase = "Ready"
	ACPUpgradeDrainDraining  ACPUpgradeDrainPhase = "Draining"
	ACPUpgradeDrainCompleted ACPUpgradeDrainPhase = "Completed"
	ACPUpgradeDrainTimedOut  ACPUpgradeDrainPhase = "TimedOut"
	ACPUpgradeDrainFailed    ACPUpgradeDrainPhase = "Failed"
)

// ACPUpgradeDrainSnapshot combines exact RuntimePool and durable-store barriers.
type ACPUpgradeDrainSnapshot struct {
	ObservedPools              int                            `json:"observedPools"`
	PoolsAwaitingQuiescence    int                            `json:"poolsAwaitingQuiescence"`
	QueuedTasks                int64                          `json:"queuedTasks"`
	ReservedSessions           int64                          `json:"reservedSessions"`
	ReservedPrompts            int64                          `json:"reservedPrompts"`
	ReservationRecords         int64                          `json:"reservationRecords"`
	RunningPrompts             int64                          `json:"runningPrompts"`
	ResidentSessions           int64                          `json:"residentSessions"`
	FinalizingSessions         int64                          `json:"finalizingSessions"`
	PendingPermissions         int64                          `json:"pendingPermissions"`
	LiveDescendants            int64                          `json:"liveDescendants"`
	QueuedSupervisorAdmissions int64                          `json:"queuedSupervisorAdmissions"`
	Barriers                   ACPUpgradeDrainBarrierSnapshot `json:"barriers"`
}

func (s ACPUpgradeDrainSnapshot) Quiescent() bool {
	// Durable Queued+NotRequested attempts are safe to resume under the next
	// controller epoch. QueuedTasks remains diagnostic, while finalization is
	// fenced by the authoritative PromptAttempt, SessionControl, Publication,
	// ExternalEffect, and outbox barriers instead of a potentially stale pool
	// summary after dispatcher admission closes.
	return s.PoolsAwaitingQuiescence == 0 &&
		s.ReservedSessions == 0 && s.ReservedPrompts == 0 && s.ReservationRecords == 0 &&
		s.RunningPrompts == 0 && s.ResidentSessions == 0 &&
		s.PendingPermissions == 0 &&
		s.LiveDescendants == 0 && s.QueuedSupervisorAdmissions == 0 &&
		s.Barriers.Quiescent()
}

// ACPUpgradeDrainStatus is safe to expose through readiness and diagnostics.
type ACPUpgradeDrainStatus struct {
	Phase           ACPUpgradeDrainPhase    `json:"phase"`
	ControllerEpoch int64                   `json:"controllerEpoch,omitempty"`
	TriggeredAt     *time.Time              `json:"triggeredAt,omitempty"`
	Deadline        *time.Time              `json:"deadline,omitempty"`
	CompletedAt     *time.Time              `json:"completedAt,omitempty"`
	LastError       string                  `json:"lastError,omitempty"`
	Snapshot        ACPUpgradeDrainSnapshot `json:"snapshot"`
}

type ACPUpgradeDrainMarkerState string

const (
	ACPUpgradeDrainMarkerIntent    ACPUpgradeDrainMarkerState = "Intent"
	ACPUpgradeDrainMarkerCompleted ACPUpgradeDrainMarkerState = "Completed"
	ACPUpgradeDrainMarkerTimedOut  ACPUpgradeDrainMarkerState = "TimedOut"
)

// ACPUpgradeDrainMarker is the durable proof used by the next leader. Only a
// Completed marker for the exact previous epoch permits planned-takeover
// recovery; missing, Intent, TimedOut, malformed, or stale markers are
// deliberately classified as unplanned takeover.
type ACPUpgradeDrainMarker struct {
	Version             string                     `json:"version"`
	State               ACPUpgradeDrainMarkerState `json:"state"`
	ControllerEpochName string                     `json:"controllerEpochName"`
	ControllerEpoch     int64                      `json:"controllerEpoch"`
	HolderID            string                     `json:"holderID"`
	StartedAt           time.Time                  `json:"startedAt"`
	Deadline            time.Time                  `json:"deadline"`
	CompletedAt         *time.Time                 `json:"completedAt,omitempty"`
	TimedOutAt          *time.Time                 `json:"timedOutAt,omitempty"`
	LastError           string                     `json:"lastError,omitempty"`
	Snapshot            ACPUpgradeDrainSnapshot    `json:"snapshot"`
}

// ACPUpgradeDrainCoordinator runs only on the elected leader. It exposes a
// loopback trigger for a same-binary preStop child and retains leadership while
// RuntimePools and durable finalization barriers settle.
type ACPUpgradeDrainCoordinator struct {
	Client           client.Client
	APIReader        client.Reader
	Epochs           ACPUpgradeDrainEpochSource
	EpochStore       store.ControllerEpochStore
	AdmissionGate    *ACPAdmissionGate
	Barriers         ACPUpgradeDrainBarrierObserver
	SupervisorClient RuntimePoolSupervisorClient
	HTTPClient       *http.Client
	SubstrateConfig  SubstrateConfig
	Options          ACPUpgradeDrainOptions
	Now              func() time.Time

	initOnce sync.Once
	initErr  error

	triggerOnce sync.Once
	done        chan struct{}
	resultMu    sync.RWMutex
	result      error

	lifecycleMu  sync.RWMutex
	lifecycleCtx context.Context

	statusMu sync.RWMutex
	status   ACPUpgradeDrainStatus
}

func NewACPUpgradeDrainCoordinator(
	kubeClient client.Client,
	reader client.Reader,
	epochs ACPUpgradeDrainEpochSource,
	epochStore store.ControllerEpochStore,
	barriers ACPUpgradeDrainBarrierObserver,
	gate *ACPAdmissionGate,
	options ACPUpgradeDrainOptions,
) *ACPUpgradeDrainCoordinator {
	if gate == nil {
		gate = NewACPAdmissionGate()
	}
	return &ACPUpgradeDrainCoordinator{
		Client: kubeClient, APIReader: reader, Epochs: epochs, EpochStore: epochStore,
		Barriers: barriers, AdmissionGate: gate, Options: options,
		done: make(chan struct{}), status: ACPUpgradeDrainStatus{Phase: ACPUpgradeDrainReady},
	}
}

func (c *ACPUpgradeDrainCoordinator) NeedLeaderElection() bool { return true }

func (c *ACPUpgradeDrainCoordinator) Start(ctx context.Context) error {
	if err := c.initialize(); err != nil {
		return err
	}
	c.lifecycleMu.Lock()
	c.lifecycleCtx = ctx
	c.lifecycleMu.Unlock()

	listener, err := net.Listen("tcp", c.Options.BindAddress)
	if err != nil {
		return fmt.Errorf("listen for ACP upgrade drain trigger: %w", err)
	}
	server := &http.Server{
		Handler:           c.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       c.Options.TriggerTimeout,
		WriteTimeout:      c.Options.TriggerTimeout,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.FromContext(ctx).Info("serving loopback ACP planned-upgrade drain trigger", "address", c.Options.BindAddress, "path", DefaultACPUpgradeDrainPath)
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve ACP upgrade drain trigger: %w", err)
	}
	return nil
}

func (c *ACPUpgradeDrainCoordinator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(DefaultACPUpgradeDrainPath, c.handleTrigger)
	return mux
}

func (c *ACPUpgradeDrainCoordinator) handleTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := c.initialize(); err != nil {
		http.Error(w, "ACP upgrade drain is not configured", http.StatusServiceUnavailable)
		return
	}
	parent := c.currentLifecycleContext()
	c.startDrain(parent)
	if err := c.wait(r.Context()); err != nil {
		writeACPUpgradeDrainHTTPStatus(w, http.StatusServiceUnavailable, c.Status())
		return
	}
	writeACPUpgradeDrainHTTPStatus(w, http.StatusOK, c.Status())
}

func writeACPUpgradeDrainHTTPStatus(w http.ResponseWriter, statusCode int, status ACPUpgradeDrainStatus) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(status)
}

// Trigger starts the same drain path used by the preStop endpoint and waits for
// completion, timeout, epoch loss, or caller cancellation.
func (c *ACPUpgradeDrainCoordinator) Trigger(ctx context.Context) error {
	if err := c.initialize(); err != nil {
		return err
	}
	c.startDrain(ctx)
	return c.wait(ctx)
}

func (c *ACPUpgradeDrainCoordinator) startDrain(parent context.Context) {
	if parent == nil {
		parent = context.Background()
	}
	c.triggerOnce.Do(func() {
		go func() {
			err := c.runDrain(parent)
			c.resultMu.Lock()
			c.result = err
			c.resultMu.Unlock()
			close(c.done)
		}()
	})
}

func (c *ACPUpgradeDrainCoordinator) wait(ctx context.Context) error {
	select {
	case <-c.done:
		c.resultMu.RLock()
		defer c.resultMu.RUnlock()
		return c.result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *ACPUpgradeDrainCoordinator) Status() ACPUpgradeDrainStatus {
	if c == nil {
		return ACPUpgradeDrainStatus{Phase: ACPUpgradeDrainFailed, LastError: "ACP upgrade drain coordinator is nil"}
	}
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.status
}

func (c *ACPUpgradeDrainCoordinator) ReadyzChecker() healthz.Checker {
	return func(_ *http.Request) error {
		status := c.Status()
		if status.Phase == ACPUpgradeDrainReady {
			return nil
		}
		if status.LastError != "" {
			return fmt.Errorf("ACP upgrade drain is %s: %s", status.Phase, status.LastError)
		}
		return fmt.Errorf("ACP upgrade drain is %s", status.Phase)
	}
}

func (c *ACPUpgradeDrainCoordinator) runDrain(parent context.Context) error {
	fence, err := c.Epochs.CurrentFence(parent)
	if err != nil {
		c.failStatus(err)
		return fmt.Errorf("get controller epoch for ACP upgrade drain: %w", err)
	}
	if err := c.requireCurrentEpoch(parent, fence); err != nil {
		c.failStatus(err)
		return err
	}
	now := c.now()
	c.setDrainingStatus(fence, now, now.Add(c.Options.Timeout), ACPUpgradeDrainSnapshot{}, "")
	c.AdmissionGate.Close("planned controller upgrade drain", now)
	marker, err := c.persistIntent(parent, fence, now)
	if err != nil {
		c.failStatus(err)
		return err
	}
	if marker.State == ACPUpgradeDrainMarkerCompleted {
		c.completeStatus(fence, marker.Snapshot, marker.CompletedAt)
		return nil
	}
	deadline := marker.Deadline
	c.setDrainingStatus(fence, marker.StartedAt, deadline, ACPUpgradeDrainSnapshot{}, "")
	if !deadline.After(now) {
		return c.finishTimedOut(fence, marker, ACPUpgradeDrainSnapshot{}, fmt.Errorf("persisted planned-upgrade drain deadline already expired"))
	}
	drainCtx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()

	ticker := time.NewTicker(c.Options.PollInterval)
	defer ticker.Stop()
	var lastSnapshot ACPUpgradeDrainSnapshot
	var lastErr error
	stableQuiescentPasses := 0
	for {
		if err := c.requireCurrentEpoch(drainCtx, fence); err != nil {
			c.failStatus(err)
			return err
		}
		snapshot, observeErr := c.reconcileDrainPass(drainCtx, fence)
		lastSnapshot = snapshot
		lastErr = observeErr
		lastErrorText := ""
		if observeErr != nil {
			lastErrorText = sanitizeRuntimePoolMessage(observeErr.Error())
		}
		c.setDrainingStatus(fence, marker.StartedAt, deadline, snapshot, lastErrorText)
		if observeErr == nil && snapshot.Quiescent() {
			stableQuiescentPasses++
			if stableQuiescentPasses >= 2 {
				if err := c.requireCurrentEpoch(drainCtx, fence); err != nil {
					c.failStatus(err)
					return err
				}
				completedAt := c.now()
				if err := c.persistTerminalMarker(drainCtx, marker, ACPUpgradeDrainMarkerCompleted, snapshot, completedAt, ""); err != nil {
					c.failStatus(err)
					return err
				}
				c.completeStatus(fence, snapshot, &completedAt)
				return nil
			}
		} else {
			stableQuiescentPasses = 0
		}
		select {
		case <-drainCtx.Done():
			if errors.Is(drainCtx.Err(), context.DeadlineExceeded) {
				return c.finishTimedOut(fence, marker, lastSnapshot, lastErr)
			}
			c.failStatus(drainCtx.Err())
			return drainCtx.Err()
		case <-ticker.C:
		}
	}
}

func (c *ACPUpgradeDrainCoordinator) reconcileDrainPass(ctx context.Context, fence store.ControllerEpochFence) (ACPUpgradeDrainSnapshot, error) {
	var pools corev1alpha1.RuntimePoolList
	listOptions := []client.ListOption{client.MatchingLabels(c.Options.RuntimePoolLabels)}
	if strings.TrimSpace(c.Options.WatchNamespace) != "" {
		listOptions = append(listOptions, client.InNamespace(strings.TrimSpace(c.Options.WatchNamespace)))
	}
	if err := c.APIReader.List(ctx, &pools, listOptions...); err != nil {
		return ACPUpgradeDrainSnapshot{}, fmt.Errorf("list owned RuntimePools for planned drain: %w", err)
	}
	snapshot := ACPUpgradeDrainSnapshot{ObservedPools: len(pools.Items)}
	var errs []error
	for i := range pools.Items {
		pool := pools.Items[i].DeepCopy()
		if pool.Status.ControllerEpoch > 0 && pool.Status.ControllerEpoch != fence.Epoch {
			snapshot.PoolsAwaitingQuiescence++
			errs = append(errs, fmt.Errorf("RuntimePool %s/%s belongs to controller epoch %d, not draining epoch %d", pool.Namespace, pool.Name, pool.Status.ControllerEpoch, fence.Epoch))
			continue
		}
		if pool.Status.ActiveInstance != nil && pool.Status.ActiveInstance.ControllerEpoch != fence.Epoch {
			snapshot.PoolsAwaitingQuiescence++
			errs = append(errs, fmt.Errorf("RuntimePool %s/%s active instance belongs to controller epoch %d, not draining epoch %d", pool.Namespace, pool.Name, pool.Status.ActiveInstance.ControllerEpoch, fence.Epoch))
			continue
		}
		if err := c.setRuntimePoolDesiredReplicasZero(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}); err != nil {
			errs = append(errs, err)
		}
		if err := c.closeRuntimePoolAdmission(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}); err != nil {
			errs = append(errs, err)
		}
		latest := &corev1alpha1.RuntimePool{}
		if err := c.APIReader.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, latest); err != nil {
			if !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("refresh RuntimePool %s/%s: %w", pool.Namespace, pool.Name, err))
			}
			continue
		}
		c.addControllerPoolCapacity(&snapshot, latest)
		if err := c.observeAndDrainRuntimePool(ctx, fence, latest, &snapshot); err != nil {
			snapshot.PoolsAwaitingQuiescence++
			errs = append(errs, fmt.Errorf("RuntimePool %s/%s: %w", latest.Namespace, latest.Name, err))
		}
	}
	barriers, err := c.Barriers.ObserveACPUpgradeDrainBarriers(ctx)
	if err != nil {
		errs = append(errs, err)
	} else {
		snapshot.Barriers = barriers
	}
	return snapshot, errors.Join(errs...)
}

func (c *ACPUpgradeDrainCoordinator) addControllerPoolCapacity(snapshot *ACPUpgradeDrainSnapshot, pool *corev1alpha1.RuntimePool) {
	if snapshot == nil || pool == nil {
		return
	}
	capacity := pool.Status.Capacity
	snapshot.QueuedTasks += int64(capacity.QueuedTasks)
	snapshot.ReservedSessions += int64(capacity.ReservedSessions)
	snapshot.ReservedPrompts += int64(capacity.ReservedPrompts)
	snapshot.ReservationRecords += int64(len(capacity.Reservations))
	snapshot.FinalizingSessions += int64(capacity.FinalizingSessions)
}

func (c *ACPUpgradeDrainCoordinator) observeAndDrainRuntimePool(
	ctx context.Context,
	fence store.ControllerEpochFence,
	pool *corev1alpha1.RuntimePool,
	snapshot *ACPUpgradeDrainSnapshot,
) error {
	if pool == nil {
		return nil
	}
	active := pool.Status.ActiveInstance
	if active != nil && (active.ControllerEpoch != fence.Epoch || pool.Status.ControllerEpoch != fence.Epoch) {
		return fmt.Errorf("active instance is fenced to controller epoch %d instead of %d", active.ControllerEpoch, fence.Epoch)
	}
	if pool.Spec.ExecutionWorkspace != nil && active == nil {
		if pool.Status.Lifecycle == corev1alpha1.RuntimePoolLifecycleStopped {
			if pool.Status.ObservedGeneration != pool.Generation {
				return fmt.Errorf(
					"workspace stopped status observed generation %d instead of current generation %d",
					pool.Status.ObservedGeneration,
					pool.Generation,
				)
			}
			return nil
		}
		return fmt.Errorf(
			"has no authenticated active instance but workspace lifecycle %q does not prove the provider workspace is stopped",
			pool.Status.Lifecycle,
		)
	}
	if runtimePoolIsSubstrateBacked(pool) {
		pod, err := upgradeDrainSubstrateInstancePod(pool, active)
		if err != nil {
			return err
		}
		return c.observeAndDrainRuntimeInstance(ctx, fence, pool, active, pod, snapshot)
	}
	pods, err := c.listRuntimePoolPodsForUpgradeDrain(ctx, pool, active)
	if err != nil {
		return err
	}
	if active == nil {
		if len(pods) == 0 {
			return nil
		}
		return fmt.Errorf("has %d live runtime Pods but no authenticated active instance", len(pods))
	}
	var pod *corev1.Pod
	for i := range pods {
		candidate := &pods[i]
		if candidate.Name == active.PodName && string(candidate.UID) == active.PodUID {
			pod = candidate
			break
		}
	}
	if pod == nil {
		if len(pods) == 0 {
			return nil
		}
		return fmt.Errorf("exact runtime Pod is absent while %d other owned Pods remain", len(pods))
	}
	if len(pods) != 1 {
		return fmt.Errorf("found %d live owned runtime Pods during planned drain", len(pods))
	}
	return c.observeAndDrainRuntimeInstance(ctx, fence, pool, active, pod, snapshot)
}

func (c *ACPUpgradeDrainCoordinator) observeAndDrainRuntimeInstance(
	ctx context.Context,
	fence store.ControllerEpochFence,
	pool *corev1alpha1.RuntimePool,
	active *corev1alpha1.RuntimePoolActiveInstanceStatus,
	pod *corev1.Pod,
	snapshot *ACPUpgradeDrainSnapshot,
) error {
	auth, err := c.runtimePoolAuthSecret(ctx, pool, active, fence.Epoch)
	if err != nil {
		return err
	}
	supervisor := c.supervisorClientForPool(pool)
	endpoint := runtimePoolInstanceEndpoint(pool, pod)
	probe, err := supervisor.Probe(ctx, endpoint, string(auth.Data[runtimePoolControllerTokenKey]), auth.Data[runtimePoolCapabilitySecretKey])
	if err != nil {
		return fmt.Errorf("authenticated supervisor probe failed: %w", err)
	}
	cfg, err := upgradeDrainRuntimePoolConfig(pool, fence.Epoch)
	if err != nil {
		return err
	}
	probeGeneration := probe.Status.Fence.RuntimePoolGeneration
	if probeGeneration == 0 || probeGeneration > uint64(pool.Generation) {
		return fmt.Errorf("validate authenticated supervisor probe: runtime status generation %d is not an admitted generation of current RuntimePool generation %d", probeGeneration, pool.Generation)
	}
	validationPool := pool
	if probeGeneration != uint64(pool.Generation) {
		validationPool = pool.DeepCopy()
		validationPool.Generation = int64(probeGeneration)
	}
	observed, err := validateRuntimePoolProbe(validationPool, cfg, pod, probe, c.now())
	if err != nil {
		return fmt.Errorf("validate authenticated supervisor probe: %w", err)
	}
	if !runtimePoolRolloutActiveInstanceMatches(active, observed) {
		return fmt.Errorf("authenticated supervisor identity changed during planned drain")
	}
	addSupervisorPressure(snapshot, probe.Status)
	if !probe.Status.Drain.Requested {
		if err := supervisor.RequestDrain(
			ctx,
			endpoint,
			string(auth.Data[runtimePoolControllerTokenKey]),
			auth.Data[runtimePoolCapabilitySecretKey],
			probe.Status,
			"planned_controller_upgrade",
		); err != nil {
			return fmt.Errorf("request authenticated supervisor drain: %w", err)
		}
		return fmt.Errorf("authenticated drain requested; awaiting a subsequent quiescent observation")
	}
	if !upgradeDrainSupervisorIsQuiescent(probe.Status) {
		return fmt.Errorf("authenticated supervisor is still draining")
	}
	return nil
}

func upgradeDrainSubstrateInstancePod(
	pool *corev1alpha1.RuntimePool,
	active *corev1alpha1.RuntimePoolActiveInstanceStatus,
) (*corev1.Pod, error) {
	if pool == nil || active == nil {
		return nil, fmt.Errorf("substrate RuntimePool active instance is required")
	}
	namespace := strings.TrimSpace(active.PodNamespace)
	name := strings.TrimSpace(active.PodName)
	uid := strings.TrimSpace(active.PodUID)
	address := strings.TrimSpace(active.PodAddress)
	providerGeneration := strings.TrimSpace(active.ProviderTokenGeneration)
	if namespace == "" || name == "" || uid == "" || address == "" || !validRuntimePoolProviderTokenGeneration(providerGeneration) {
		return nil, fmt.Errorf("substrate RuntimePool active instance identity is incomplete")
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			UID:       types.UID(uid),
			Annotations: map[string]string{
				runtimePoolProfileAnnotation:                 pool.Spec.Runtime.Profile.Digest,
				runtimePoolProviderTokenGenerationAnnotation: providerGeneration,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: address},
	}, nil
}

func (c *ACPUpgradeDrainCoordinator) listRuntimePoolPodsForUpgradeDrain(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	active *corev1alpha1.RuntimePoolActiveInstanceStatus,
) ([]corev1.Pod, error) {
	namespace := strings.TrimSpace(pool.Spec.RuntimeNamespace)
	if namespace == "" {
		namespace = pool.Namespace
	}
	if active != nil && strings.TrimSpace(active.PodNamespace) != "" {
		namespace = strings.TrimSpace(active.PodNamespace)
	}
	var listed corev1.PodList
	if err := c.APIReader.List(
		ctx,
		&listed,
		client.InNamespace(namespace),
		client.MatchingLabels{
			runtimePoolKeyLabel: runtimePoolKey(pool.Namespace, pool.Name),
			runtimePoolUIDLabel: string(pool.UID),
		},
	); err != nil {
		return nil, fmt.Errorf("list owned runtime Pods: %w", err)
	}
	pods := make([]corev1.Pod, 0, len(listed.Items))
	for i := range listed.Items {
		if listed.Items[i].Status.Phase == corev1.PodSucceeded || listed.Items[i].Status.Phase == corev1.PodFailed {
			continue
		}
		pods = append(pods, listed.Items[i])
	}
	return pods, nil
}

func addSupervisorPressure(snapshot *ACPUpgradeDrainSnapshot, status harnessv2.StatusResponse) {
	if snapshot == nil {
		return
	}
	snapshot.ResidentSessions += int64(status.Pressure.ResidentSessions)
	snapshot.RunningPrompts += int64(status.Pressure.ActivePrompts)
	snapshot.QueuedSupervisorAdmissions += int64(status.Pressure.QueuedAdmissions)
	snapshot.PendingPermissions += int64(status.Pressure.PendingPermissions)
	snapshot.LiveDescendants += int64(status.Pressure.LiveDescendants)
}

func upgradeDrainSupervisorIsQuiescent(status harnessv2.StatusResponse) bool {
	return status.Drain.Requested && !status.Drain.AcceptingNewSessions &&
		status.Pressure.ResidentSessions == 0 && status.Pressure.ActivePrompts == 0 &&
		status.Pressure.QueuedAdmissions == 0 && status.Pressure.PendingPermissions == 0 &&
		status.Pressure.LiveDescendants == 0 && len(status.Sessions) == 0 &&
		len(status.ActivePrompts) == 0 && len(status.PendingPermissions) == 0
}

func upgradeDrainRuntimePoolConfig(pool *corev1alpha1.RuntimePool, epoch int64) (runtimePoolConfig, error) {
	profile, protocol, err := validateRuntimePoolProfile(pool)
	if err != nil {
		return runtimePoolConfig{}, err
	}
	maxSessions, maxPrompts, err := runtimePoolCapacity(pool)
	if err != nil {
		return runtimePoolConfig{}, err
	}
	return runtimePoolConfig{
		controllerEpoch: epoch, protocol: protocol, profile: profile,
		maxResidentSessions: maxSessions, maxRunningPrompts: maxPrompts,
	}, nil
}

func (c *ACPUpgradeDrainCoordinator) runtimePoolAuthSecret(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	active *corev1alpha1.RuntimePoolActiveInstanceStatus,
	epoch int64,
) (*corev1.Secret, error) {
	secret, err := resolveRuntimePoolAuthSecret(ctx, c.APIReader, pool, active.PodNamespace, epoch)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(secret.Data[runtimePoolControllerTokenKey])) == "" || len(secret.Data[runtimePoolCapabilitySecretKey]) == 0 {
		return nil, fmt.Errorf("RuntimePool auth Secret is missing controller credentials")
	}
	return secret, nil
}

func (c *ACPUpgradeDrainCoordinator) supervisorClientForPool(pool *corev1alpha1.RuntimePool) RuntimePoolSupervisorClient {
	reconciler := &RuntimePoolReconciler{
		SupervisorClient: c.SupervisorClient,
		HTTPClient:       c.HTTPClient,
		SubstrateConfig:  c.SubstrateConfig,
		Now:              c.Now,
	}
	return reconciler.supervisorClientForPool(pool)
}

func (c *ACPUpgradeDrainCoordinator) setRuntimePoolDesiredReplicasZero(ctx context.Context, key types.NamespacedName) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pool := &corev1alpha1.RuntimePool{}
		if err := c.Client.Get(ctx, key, pool); err != nil {
			return client.IgnoreNotFound(err)
		}
		if pool.Spec.DesiredReplicas == 0 || !pool.DeletionTimestamp.IsZero() {
			return nil
		}
		base := pool.DeepCopy()
		pool.Spec.DesiredReplicas = 0
		if err := c.Client.Patch(ctx, pool, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("set RuntimePool desired replicas to zero: %w", err)
		}
		return nil
	})
}

func (c *ACPUpgradeDrainCoordinator) closeRuntimePoolAdmission(ctx context.Context, key types.NamespacedName) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pool := &corev1alpha1.RuntimePool{}
		if err := c.Client.Get(ctx, key, pool); err != nil {
			return client.IgnoreNotFound(err)
		}
		before := pool.Status.DeepCopy()
		if pool.Status.CurrentReplicas == 0 || pool.Status.Lifecycle == corev1alpha1.RuntimePoolLifecycleStopped {
			pool.Status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		} else {
			pool.Status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
		}
		pool.Status.Message = "planned controller upgrade drain has closed ACP admission"
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type: corev1alpha1.RuntimePoolConditionAdmissionReady, Status: metav1.ConditionFalse,
			Reason: "ControllerUpgradeDraining", Message: pool.Status.Message,
			ObservedGeneration: pool.Generation,
		})
		if reflect.DeepEqual(*before, pool.Status) {
			return nil
		}
		if err := c.Client.Status().Update(ctx, pool); err != nil {
			return fmt.Errorf("close RuntimePool admission: %w", err)
		}
		return nil
	})
}

func (c *ACPUpgradeDrainCoordinator) finishTimedOut(
	fence store.ControllerEpochFence,
	marker ACPUpgradeDrainMarker,
	snapshot ACPUpgradeDrainSnapshot,
	lastErr error,
) error {
	reason := "ACP planned-upgrade barriers did not settle before the configured deadline"
	if lastErr != nil {
		reason += ": " + sanitizeRuntimePoolMessage(lastErr.Error())
	}
	now := c.now()
	markerErr := error(nil)
	markerCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.requireCurrentEpoch(markerCtx, fence); err == nil {
		markerErr = c.persistTerminalMarker(markerCtx, marker, ACPUpgradeDrainMarkerTimedOut, snapshot, now, reason)
	}
	if markerErr != nil {
		reason += "; failed to persist timeout marker: " + sanitizeRuntimePoolMessage(markerErr.Error())
	}
	c.setTerminalStatus(ACPUpgradeDrainTimedOut, fence, snapshot, nil, reason)
	return fmt.Errorf("%w: %s", ErrACPUpgradeDrainTimedOut, reason)
}

func (c *ACPUpgradeDrainCoordinator) persistIntent(
	ctx context.Context,
	fence store.ControllerEpochFence,
	startedAt time.Time,
) (ACPUpgradeDrainMarker, error) {
	desired := ACPUpgradeDrainMarker{
		Version: "v1", State: ACPUpgradeDrainMarkerIntent,
		ControllerEpochName: fence.Name, ControllerEpoch: fence.Epoch, HolderID: fence.HolderID,
		StartedAt: startedAt.UTC(), Deadline: startedAt.UTC().Add(c.Options.Timeout),
	}
	var result ACPUpgradeDrainMarker
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		lease, err := c.readControllerEpochLease(ctx, fence.Name)
		if err != nil {
			return err
		}
		if err := c.requireLeaseFence(ctx, lease, fence); err != nil {
			return err
		}
		existing, present, err := decodeACPUpgradeDrainMarker(lease.Annotations[acpUpgradeDrainMarkerAnnotation])
		if err != nil {
			return err
		}
		if present && existing.ControllerEpoch == fence.Epoch {
			if existing.ControllerEpochName != fence.Name || existing.HolderID != fence.HolderID {
				return fmt.Errorf("ACP upgrade drain marker epoch %d is owned by another controller", fence.Epoch)
			}
			switch existing.State {
			case ACPUpgradeDrainMarkerIntent, ACPUpgradeDrainMarkerCompleted:
				result = existing
				return nil
			case ACPUpgradeDrainMarkerTimedOut:
				return fmt.Errorf("%w: epoch %d already timed out", ErrACPUpgradeDrainTimedOut, fence.Epoch)
			default:
				return fmt.Errorf("unsupported ACP upgrade drain marker state %q", existing.State)
			}
		}
		encoded, err := encodeACPUpgradeDrainMarker(desired)
		if err != nil {
			return err
		}
		updated := lease.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = make(map[string]string)
		}
		updated.Annotations[acpUpgradeDrainMarkerAnnotation] = encoded
		if err := c.Client.Update(ctx, updated); err != nil {
			return err
		}
		result = desired
		return nil
	})
	if err != nil {
		return ACPUpgradeDrainMarker{}, fmt.Errorf("persist ACP upgrade drain intent on controller epoch Lease: %w", err)
	}
	return result, nil
}

func (c *ACPUpgradeDrainCoordinator) persistTerminalMarker(
	ctx context.Context,
	intent ACPUpgradeDrainMarker,
	state ACPUpgradeDrainMarkerState,
	snapshot ACPUpgradeDrainSnapshot,
	at time.Time,
	reason string,
) error {
	if state != ACPUpgradeDrainMarkerCompleted && state != ACPUpgradeDrainMarkerTimedOut {
		return fmt.Errorf("unsupported terminal ACP upgrade drain marker state %q", state)
	}
	fence := store.ControllerEpochFence{
		Name: intent.ControllerEpochName, Epoch: intent.ControllerEpoch, HolderID: intent.HolderID,
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		lease, err := c.readControllerEpochLease(ctx, fence.Name)
		if err != nil {
			return err
		}
		if err := c.requireLeaseFence(ctx, lease, fence); err != nil {
			return err
		}
		existing, present, err := decodeACPUpgradeDrainMarker(lease.Annotations[acpUpgradeDrainMarkerAnnotation])
		if err != nil {
			return err
		}
		if !present || existing.ControllerEpochName != intent.ControllerEpochName || existing.ControllerEpoch != intent.ControllerEpoch ||
			existing.HolderID != intent.HolderID || existing.State != ACPUpgradeDrainMarkerIntent {
			return fmt.Errorf("ACP upgrade drain marker no longer matches the active intent")
		}
		existing.State = state
		existing.Snapshot = snapshot
		existing.LastError = sanitizeRuntimePoolMessage(reason)
		at = at.UTC()
		if state == ACPUpgradeDrainMarkerCompleted {
			existing.CompletedAt = &at
		} else {
			existing.TimedOutAt = &at
		}
		encoded, err := encodeACPUpgradeDrainMarker(existing)
		if err != nil {
			return err
		}
		updated := lease.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = make(map[string]string)
		}
		updated.Annotations[acpUpgradeDrainMarkerAnnotation] = encoded
		return c.Client.Update(ctx, updated)
	})
}

func (c *ACPUpgradeDrainCoordinator) requireLeaseFence(
	ctx context.Context,
	lease *coordinationv1.Lease,
	fence store.ControllerEpochFence,
) error {
	if lease == nil || lease.Spec.HolderIdentity == nil || strings.TrimSpace(*lease.Spec.HolderIdentity) != fence.HolderID {
		return fmt.Errorf("%w: controller epoch Lease holder no longer matches %q", ErrACPUpgradeDrainEpochLost, fence.HolderID)
	}
	return c.requireCurrentEpoch(ctx, fence)
}

func (c *ACPUpgradeDrainCoordinator) readControllerEpochLease(
	ctx context.Context,
	epochName string,
) (*coordinationv1.Lease, error) {
	return readACPUpgradeDrainControllerEpochLease(ctx, c.APIReader, c.Options.MarkerNamespace, epochName)
}

func readACPUpgradeDrainControllerEpochLease(
	ctx context.Context,
	reader client.Reader,
	namespace, epochName string,
) (*coordinationv1.Lease, error) {
	if reader == nil {
		return nil, fmt.Errorf("kubernetes reader is required")
	}
	var epochs corev1alpha1.ControllerEpochList
	if err := reader.List(ctx, &epochs, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list ControllerEpoch records for planned drain: %w", err)
	}
	leaseName := ""
	for i := range epochs.Items {
		epoch := &epochs.Items[i]
		if epoch.Spec.Name != epochName {
			continue
		}
		if leaseName != "" && leaseName != epoch.Status.LeaseName {
			return nil, fmt.Errorf("multiple ControllerEpoch records resolve logical name %q", epochName)
		}
		leaseName = strings.TrimSpace(epoch.Status.LeaseName)
	}
	if leaseName == "" {
		return nil, fmt.Errorf("ControllerEpoch %q has no authoritative Lease", epochName)
	}
	lease := &coordinationv1.Lease{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: leaseName}, lease); err != nil {
		return nil, fmt.Errorf("get authoritative controller epoch Lease: %w", err)
	}
	return lease, nil
}

func encodeACPUpgradeDrainMarker(marker ACPUpgradeDrainMarker) (string, error) {
	encoded, err := json.Marshal(marker)
	if err != nil {
		return "", fmt.Errorf("encode ACP upgrade drain marker: %w", err)
	}
	return string(encoded), nil
}

func decodeACPUpgradeDrainMarker(raw string) (ACPUpgradeDrainMarker, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ACPUpgradeDrainMarker{}, false, nil
	}
	var marker ACPUpgradeDrainMarker
	if err := json.Unmarshal([]byte(raw), &marker); err != nil {
		return marker, true, fmt.Errorf("decode ACP upgrade drain marker: %w", err)
	}
	if marker.Version != "v1" || marker.ControllerEpochName == "" || marker.ControllerEpoch < 1 || marker.HolderID == "" || marker.StartedAt.IsZero() || marker.Deadline.IsZero() {
		return marker, true, fmt.Errorf("ACP upgrade drain marker is incomplete")
	}
	switch marker.State {
	case ACPUpgradeDrainMarkerIntent:
		if marker.CompletedAt != nil || marker.TimedOutAt != nil {
			return marker, true, fmt.Errorf("ACP upgrade drain intent marker carries terminal timestamps")
		}
	case ACPUpgradeDrainMarkerCompleted:
		if marker.CompletedAt == nil || marker.TimedOutAt != nil || !marker.Snapshot.Quiescent() {
			return marker, true, fmt.Errorf("ACP upgrade drain completion marker is invalid")
		}
	case ACPUpgradeDrainMarkerTimedOut:
		if marker.TimedOutAt == nil || marker.CompletedAt != nil {
			return marker, true, fmt.Errorf("ACP upgrade drain timeout marker is invalid")
		}
	default:
		return marker, true, fmt.Errorf("unsupported ACP upgrade drain marker state %q", marker.State)
	}
	return marker, true, nil
}

func (c *ACPUpgradeDrainCoordinator) requireCurrentEpoch(ctx context.Context, fence store.ControllerEpochFence) error {
	current, err := c.EpochStore.GetControllerEpoch(ctx, fence.Name)
	if err != nil {
		return fmt.Errorf("read authoritative controller epoch: %w", err)
	}
	if current == nil {
		return fmt.Errorf("read authoritative controller epoch: store returned no record")
	}
	if current.Epoch != fence.Epoch || current.HolderID != fence.HolderID {
		return fmt.Errorf("%w: active epoch is %d held by %q, drain epoch is %d held by %q",
			ErrACPUpgradeDrainEpochLost, current.Epoch, current.HolderID, fence.Epoch, fence.HolderID)
	}
	return nil
}

func (c *ACPUpgradeDrainCoordinator) initialize() error {
	if c == nil {
		return fmt.Errorf("ACP upgrade drain coordinator is nil")
	}
	c.initOnce.Do(func() {
		if c.Client == nil || c.Epochs == nil || c.EpochStore == nil || c.Barriers == nil {
			c.initErr = fmt.Errorf("ACP upgrade drain coordinator requires Kubernetes client, epoch source, epoch store, and barrier observer")
			return
		}
		if c.APIReader == nil {
			c.APIReader = c.Client
		}
		if c.AdmissionGate == nil {
			c.AdmissionGate = NewACPAdmissionGate()
		}
		defaults := DefaultACPUpgradeDrainOptions()
		if strings.TrimSpace(c.Options.BindAddress) == "" {
			c.Options.BindAddress = defaults.BindAddress
		}
		if c.Options.Timeout <= 0 {
			c.Options.Timeout = defaults.Timeout
		}
		if c.Options.PollInterval <= 0 {
			c.Options.PollInterval = defaults.PollInterval
		}
		if c.Options.TriggerTimeout <= 0 {
			c.Options.TriggerTimeout = defaults.TriggerTimeout
		}
		labels := cloneStringMap(defaults.RuntimePoolLabels)
		c.Options.RuntimePoolLabels = mergeStringMap(labels, c.Options.RuntimePoolLabels)
		if strings.TrimSpace(c.Options.WatchNamespace) == "" {
			c.initErr = fmt.Errorf("ACP upgrade drain watch namespace is required")
			return
		}
		if observer, ok := c.Barriers.(*KubernetesACPUpgradeDrainBarrierObserver); ok && strings.TrimSpace(observer.Namespace) == "" {
			observer.Namespace = strings.TrimSpace(c.Options.WatchNamespace)
		}
		if c.Options.PollInterval >= c.Options.Timeout {
			c.initErr = fmt.Errorf("ACP upgrade drain poll interval must be shorter than its timeout")
			return
		}
		if c.Options.TriggerTimeout <= c.Options.Timeout {
			c.initErr = fmt.Errorf("ACP upgrade drain trigger timeout must exceed its drain timeout")
			return
		}
		if strings.TrimSpace(c.Options.MarkerNamespace) == "" {
			c.initErr = fmt.Errorf("ACP upgrade drain marker namespace is required")
			return
		}
		if err := validateACPUpgradeDrainBindAddress(c.Options.BindAddress); err != nil {
			c.initErr = err
			return
		}
		if c.done == nil {
			c.done = make(chan struct{})
		}
		if c.status.Phase == "" {
			c.status.Phase = ACPUpgradeDrainReady
		}
	})
	return c.initErr
}

func validateACPUpgradeDrainBindAddress(address string) error {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return fmt.Errorf("ACP upgrade drain bind address is invalid: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("ACP upgrade drain bind address must use a literal loopback IP")
	}
	value, err := strconv.Atoi(port)
	if err != nil || value < 0 || value > 65535 {
		return fmt.Errorf("ACP upgrade drain bind port is invalid")
	}
	return nil
}

func (c *ACPUpgradeDrainCoordinator) currentLifecycleContext() context.Context {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	if c.lifecycleCtx != nil {
		return c.lifecycleCtx
	}
	return context.Background()
}

func (c *ACPUpgradeDrainCoordinator) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *ACPUpgradeDrainCoordinator) setDrainingStatus(
	fence store.ControllerEpochFence,
	triggeredAt, deadline time.Time,
	snapshot ACPUpgradeDrainSnapshot,
	lastError string,
) {
	triggeredAt = triggeredAt.UTC()
	deadline = deadline.UTC()
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.status = ACPUpgradeDrainStatus{
		Phase: ACPUpgradeDrainDraining, ControllerEpoch: fence.Epoch,
		TriggeredAt: &triggeredAt, Deadline: &deadline,
		LastError: lastError, Snapshot: snapshot,
	}
}

func (c *ACPUpgradeDrainCoordinator) completeStatus(
	fence store.ControllerEpochFence,
	snapshot ACPUpgradeDrainSnapshot,
	completedAt *time.Time,
) {
	c.setTerminalStatus(ACPUpgradeDrainCompleted, fence, snapshot, completedAt, "")
}

func (c *ACPUpgradeDrainCoordinator) setTerminalStatus(
	phase ACPUpgradeDrainPhase,
	fence store.ControllerEpochFence,
	snapshot ACPUpgradeDrainSnapshot,
	completedAt *time.Time,
	lastError string,
) {
	if completedAt != nil {
		value := completedAt.UTC()
		completedAt = &value
	}
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	status := c.status
	status.Phase = phase
	status.ControllerEpoch = fence.Epoch
	status.CompletedAt = completedAt
	status.LastError = lastError
	status.Snapshot = snapshot
	c.status = status
}

func (c *ACPUpgradeDrainCoordinator) failStatus(err error) {
	message := ""
	if err != nil {
		message = sanitizeRuntimePoolMessage(err.Error())
	}
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	status := c.status
	status.Phase = ACPUpgradeDrainFailed
	status.LastError = message
	c.status = status
}

// RunACPUpgradeDrainTriggerMode is the single early-entrypoint hook used by
// cmd/main.go after flag parsing. It returns handled=true whenever TriggerURL is
// set, so the preStop child must exit instead of opening the store or starting a
// second controller manager.
func RunACPUpgradeDrainTriggerMode(ctx context.Context, options ACPUpgradeDrainOptions) (handled bool, err error) {
	if strings.TrimSpace(options.TriggerURL) == "" {
		return false, nil
	}
	if options.TriggerTimeout <= 0 {
		options.TriggerTimeout = DefaultACPUpgradeDrainTriggerTimeout
	}
	triggerCtx, cancel := context.WithTimeout(ctx, options.TriggerTimeout)
	defer cancel()
	return true, RequestACPUpgradeDrain(triggerCtx, options.TriggerURL)
}

// RequestACPUpgradeDrain is the distroless-compatible preStop helper. cmd/main
// invokes it in trigger-only mode from a second /manager process; no shell,
// curl, wget, Service, or externally reachable drain endpoint is required.
func RequestACPUpgradeDrain(ctx context.Context, endpoint string) error {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || parsed.Scheme != urlSchemeHTTP || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("ACP upgrade drain trigger URL is invalid")
	}
	host := parsed.Hostname()
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() || parsed.Path != DefaultACPUpgradeDrainPath {
		return fmt.Errorf("ACP upgrade drain trigger URL must use the loopback drain endpoint")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return err
	}
	httpClient := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("request ACP upgrade drain: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("ACP upgrade drain returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
