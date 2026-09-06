/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8slabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/gateway/protocol"
	orkalabels "github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	orkatracing "github.com/orka-agents/orka/internal/tracing"
)

const (
	ingressStatusAccepted     = "accepted"
	ingressStatusDuplicate    = "duplicate"
	ingressStatusRejected     = "rejected"
	ingressStatusDeadLettered = "deadLettered"

	TaskGatewayNameLabel          = "gateway.orka.ai/gateway"
	TaskGatewayBindingLabel       = "gateway.orka.ai/binding"
	TaskGatewayEventLabel         = "gateway.orka.ai/event-id"
	TaskGatewayEventAnnotation    = "gateway.orka.ai/event-id"
	TaskGatewayExternalEvent      = "gateway.orka.ai/external-event-id"
	TaskGatewaySession            = "gateway.orka.ai/session"
	TaskGatewayNameAnnotation     = "gateway.orka.ai/gateway-name"
	TaskGatewayBindingAnnotation  = "gateway.orka.ai/binding-name"
	TaskGatewayDelivery           = "gateway.orka.ai/delivery-id"
	TaskGatewayProviderMessage    = "gateway.orka.ai/provider-message-id"
	defaultGatewayTaskTimeout     = 30 * time.Minute
	minimumGatewayExecutionWindow = time.Second
	gatewayTaskCleanupGrace       = 2 * time.Minute
	gatewayResultProjectionGrace  = 30 * time.Second
	minimumGatewayDeliveryWindow  = time.Minute
)

var (
	gatewayTracer             = otel.Tracer("github.com/orka-agents/orka/internal/gateway")
	errGatewayBindingNotReady = errors.New("matching gateway binding is not ready")
	errGatewayResultNotReady  = errors.New("gateway task result is not ready")
)

// Config controls bounded gateway processing behavior.
type Config struct {
	Enabled                      bool
	Namespace                    string
	PendingPerSession            int
	MaxTasksPerNamespace         int32
	MaxRecordsPerGateway         int
	MaxRejectedRecordsPerGateway int
	EventExpiry                  time.Duration
	TerminalRetention            time.Duration
	DeliveryTimeout              time.Duration
	DeliveryMaxAttempts          int
	ClaimLease                   time.Duration
	PollInterval                 time.Duration
	BatchSize                    int
	AllowInsecureLoopback        bool
}

// DefaultConfig returns the locked V1 queue, expiry, retry, and retention defaults.
func DefaultConfig() Config {
	return Config{
		Enabled: true, PendingPerSession: 100, MaxRecordsPerGateway: 1_000, MaxRejectedRecordsPerGateway: 250, EventExpiry: 24 * time.Hour,
		TerminalRetention: 30 * 24 * time.Hour, DeliveryTimeout: 15 * time.Second,
		DeliveryMaxAttempts: 10, ClaimLease: time.Minute, PollInterval: 500 * time.Millisecond,
		BatchSize: 25,
	}
}

// HTTPError is a client-safe ingress error with an HTTP status code.
type HTTPError struct {
	Code    int
	Message string
}

func (e *HTTPError) Error() string { return e.Message }

// Service owns durable admission, dispatch, terminal projection, and delivery.
type Service struct {
	Client        client.Client
	APIReader     client.Reader
	EventStore    store.GatewayEventStore
	DeliveryStore store.GatewayDeliveryStore
	ResultStore   store.ResultStore
	HTTPClient    *http.Client
	Config        Config
	Owner         string
}

func (s *Service) freshReader() client.Reader {
	if s != nil && s.APIReader != nil {
		return s.APIReader
	}
	return s.Client
}

// NewService creates a gateway processing service.
func NewService(
	kubeClient client.Client,
	events store.GatewayEventStore,
	deliveries store.GatewayDeliveryStore,
	results store.ResultStore,
	config Config,
) *Service {
	config = normalizeConfig(config)
	return &Service{
		Client: kubeClient, EventStore: events, DeliveryStore: deliveries, ResultStore: results,
		Config: config, Owner: newProcessorOwner(),
	}
}

// NeedLeaderElection serializes dispatch, delivery, and maintenance across controller replicas.
// Durable claims still provide crash recovery, while leader election makes namespace capacity checks replica-safe.
func (s *Service) NeedLeaderElection() bool { return true }

// Start runs independent dispatch/projection, delivery, and maintenance loops until cancellation.
func (s *Service) Start(ctx context.Context) error {
	if s == nil || !s.Config.Enabled {
		<-ctx.Done()
		return nil
	}
	logger := log.FromContext(ctx).WithName("gateway-service")
	logger.Info("starting generic gateway processing", "owner", s.Owner)
	deliveryDone := make(chan struct{})
	go func() {
		defer close(deliveryDone)
		s.runDeliveryLoop(ctx, logger)
	}()

	ticker := time.NewTicker(s.Config.PollInterval)
	defer ticker.Stop()
	maintenanceTicker := time.NewTicker(time.Minute)
	defer maintenanceTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			<-deliveryDone
			return nil
		case <-ticker.C:
			if err := s.processCoreOnce(ctx); err != nil {
				logger.Error(err, "gateway processing iteration failed")
			}
		case now := <-maintenanceTicker.C:
			terminalCutoff := now.Add(-s.Config.TerminalRetention)
			if _, err := s.DeliveryStore.MaintainGatewayRecords(ctx, s.Config.Namespace, now, terminalCutoff); err != nil {
				logger.Error(err, "gateway maintenance failed")
			} else if err := s.cleanupRetainedGatewayTasks(ctx, terminalCutoff); err != nil {
				logger.Error(err, "gateway Task retention cleanup failed")
			}
		}
	}
}

func (s *Service) cleanupRetainedGatewayTasks(ctx context.Context, terminalCutoff time.Time) error {
	if s == nil || s.Client == nil || s.EventStore == nil {
		return nil
	}
	requirement, err := k8slabels.NewRequirement(TaskGatewayEventLabel, selection.Exists, nil)
	if err != nil {
		return err
	}
	options := []client.ListOption{client.MatchingLabelsSelector{Selector: k8slabels.NewSelector().Add(*requirement)}}
	if s.Config.Namespace != "" {
		options = append(options, client.InNamespace(s.Config.Namespace))
	}
	var tasks corev1alpha1.TaskList
	if err := s.Client.List(ctx, &tasks, options...); err != nil {
		return fmt.Errorf("list retained gateway Tasks: %w", err)
	}
	var errs []error
	for i := range tasks.Items {
		task := &tasks.Items[i]
		if !task.DeletionTimestamp.IsZero() || task.CreationTimestamp.IsZero() || !task.CreationTimestamp.Time.Before(terminalCutoff) {
			continue
		}
		owner, gatewayOwned := TaskOwner(task)
		if !gatewayOwned || owner.GatewayNamespace == "" || owner.NamespaceUID == "" ||
			owner.GatewayName == "" || owner.GatewayUID == "" || task.UID == "" {
			continue
		}
		eventFound := false
		if task.UID != "" {
			if _, err := s.EventStore.GetGatewayEventForTask(ctx, task.Namespace, task.Name, string(task.UID)); err == nil {
				eventFound = true
			} else if !errors.Is(err, store.ErrNotFound) {
				errs = append(errs, fmt.Errorf("check retained gateway Task %s/%s: %w", task.Namespace, task.Name, err))
				continue
			}
		}
		if !eventFound {
			tombstoned, err := s.EventStore.HasGatewayTaskTombstone(ctx, task.Namespace, task.Name, string(task.UID))
			if err != nil {
				errs = append(errs, fmt.Errorf("check retained gateway Task tombstone %s/%s: %w", task.Namespace, task.Name, err))
				continue
			}
			if !tombstoned {
				continue
			}
		}
		if eventFound {
			continue
		}
		deleteOptions := []client.DeleteOption{}
		if task.UID != "" {
			uid := task.UID
			deleteOptions = append(deleteOptions, &client.DeleteOptions{
				Preconditions: &metav1.Preconditions{UID: &uid},
			})
		}
		if err := s.Client.Delete(ctx, task, deleteOptions...); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete retained gateway Task %s/%s: %w", task.Namespace, task.Name, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Service) runDeliveryLoop(ctx context.Context, logger logr.Logger) {
	ticker := time.NewTicker(s.Config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.processDeliveryBatch(ctx); err != nil {
				logger.Error(err, "gateway delivery iteration failed")
			}
		}
	}
}

func (s *Service) processDeliveryBatch(ctx context.Context) error {
	batchSize := max(s.Config.BatchSize, 1)
	errs := make(chan error, batchSize)
	var workers sync.WaitGroup
	workers.Add(batchSize)
	for range batchSize {
		go func() {
			defer workers.Done()
			if err := s.DeliverOnce(ctx); err != nil && !errors.Is(err, store.ErrNotFound) {
				errs <- err
			}
		}()
	}
	workers.Wait()
	close(errs)
	collected := make([]error, 0, batchSize)
	for err := range errs {
		collected = append(collected, err)
	}
	return errors.Join(collected...)
}

func (s *Service) processCoreOnce(ctx context.Context) error {
	var errs []error
	if err := s.RepairExpiredEvents(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := s.ExpireQueuedEvents(ctx); err != nil {
		errs = append(errs, err)
	}
	for range s.Config.BatchSize {
		if err := s.DispatchOnce(ctx); errors.Is(err, store.ErrNotFound) {
			break
		} else if err != nil {
			errs = append(errs, err)
			break
		}
	}
	if err := s.ProjectTerminals(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := s.updateQueueMetrics(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// RepairExpiredEvents creates missing outbox rows for records expired by older
// versions or interrupted migrations before maintenance can compact them.
func (s *Service) RepairExpiredEvents(ctx context.Context) error {
	now := time.Now().UTC()
	events, err := s.EventStore.ListGatewayEvents(ctx, store.GatewayEventFilter{
		Namespace: s.Config.Namespace, States: []store.GatewayEventState{store.GatewayEventExpired},
		MissingDelivery: true, OrderByExpiry: true, SessionHeadOnly: true, Limit: s.Config.BatchSize,
	})
	if err != nil {
		return err
	}
	var errs []error
	for i := range events {
		if events[i].NamespaceUID == "" || events[i].GatewayUID == "" || events[i].GatewayGeneration <= 0 {
			if err := s.EventStore.MarkExpiredGatewayEventDeadLettered(
				ctx, events[i].Namespace, events[i].ID,
				"The expired legacy event cannot be safely delivered because immutable identity is unavailable.", now,
			); err != nil && !errors.Is(err, store.ErrConflict) {
				errs = append(errs, err)
			} else if err == nil {
				gatewayDeadLettersTotal.WithLabelValues("event").Inc()
			}
			continue
		}
		reason := strings.TrimSpace(events[i].StateMessage)
		if reason == "" {
			reason = "The task could not be completed."
		}
		projectionAt := now.Add(time.Duration(i) * time.Nanosecond)
		if err := s.expireGatewayEvent(ctx, &events[i], "", reason, projectionAt, false); err != nil &&
			!errors.Is(err, store.ErrConflict) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ExpireQueuedEvents appends visible expiry errors for admitted work that never reached Task creation.
func (s *Service) ExpireQueuedEvents(ctx context.Context) error {
	now := time.Now().UTC()
	events, err := s.EventStore.ListGatewayEvents(ctx, store.GatewayEventFilter{
		Namespace:     s.Config.Namespace,
		States:        []store.GatewayEventState{store.GatewayEventAccepted, store.GatewayEventQueued},
		ExpiresBefore: &now, OrderByExpiry: true, SessionHeadOnly: true, Limit: s.Config.BatchSize,
	})
	if err != nil {
		return err
	}
	var errs []error
	for i := range events {
		projectionAt := now.Add(time.Duration(i) * time.Nanosecond)
		if err := s.expireGatewayEvent(
			ctx, &events[i], "", "The message expired before execution could start.", projectionAt, false,
		); err != nil && !errors.Is(err, store.ErrConflict) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Service) updateQueueMetrics(ctx context.Context) error {
	stats, err := s.EventStore.GetGatewayQueueStats(ctx, s.Config.Namespace)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	gatewayQueueDepth.WithLabelValues("event").Set(float64(stats.PendingEvents))
	gatewayQueueDepth.WithLabelValues("delivery").Set(float64(stats.PendingDeliveries))
	gatewayQueueOldestAge.WithLabelValues("event").Set(queueAgeSeconds(now, stats.OldestEventReceived))
	gatewayQueueOldestAge.WithLabelValues("delivery").Set(queueAgeSeconds(now, stats.OldestDeliveryDue))
	return nil
}

// AdmitEvent authenticates, validates, routes, and durably acknowledges one inbound event.
//
//nolint:gocyclo // ingress intentionally sequences independent fail-closed trust and durability checks
func (s *Service) AdmitEvent(ctx context.Context, namespace, gatewayName, authorization string, body []byte) (*protocol.IngressResponse, error) {
	ctx, span := gatewayTracer.Start(ctx, "gateway.ingress")
	defer span.End()
	if s == nil || !s.Config.Enabled {
		return nil, &HTTPError{Code: http.StatusServiceUnavailable, Message: "gateway ingress is disabled"}
	}
	if len(body) > protocol.MaxHTTPBodyBytes {
		return nil, &HTTPError{Code: http.StatusRequestEntityTooLarge, Message: "gateway event body exceeds 256 KiB"}
	}
	reader := s.freshReader()
	object := &gatewayv1alpha1.Gateway{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: gatewayName}, object); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &HTTPError{Code: http.StatusNotFound, Message: "gateway not found"}
		}
		return nil, fmt.Errorf("read Gateway: %w", err)
	}
	namespaceObject := &corev1.Namespace{}
	if err := reader.Get(ctx, client.ObjectKey{Name: namespace}, namespaceObject); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &HTTPError{Code: http.StatusNotFound, Message: "gateway namespace not found"}
		}
		return nil, fmt.Errorf("read Gateway namespace: %w", err)
	}
	// Authentication intentionally precedes mutable readiness checks so an exact retry of an
	// already-committed event can still receive its durable duplicate acknowledgement.
	inbound, err := ReadBearerSecret(ctx, reader, object, AuthDirectionInbound, "")
	if err != nil {
		return nil, &HTTPError{Code: http.StatusServiceUnavailable, Message: "gateway authentication is not ready"}
	}
	if !protocol.ConstantTimeBearerEqual(protocol.BearerToken(authorization), inbound.Token) {
		return nil, &HTTPError{Code: http.StatusUnauthorized, Message: "invalid gateway bearer token"}
	}
	eventEnvelope, err := protocol.DecodeEvent(body)
	if err != nil {
		return nil, &HTTPError{Code: http.StatusBadRequest, Message: protocol.SanitizeMessage(err.Error(), 1024)}
	}

	now := time.Now().UTC()
	eventID := stableID("gev", string(object.UID), eventEnvelope.ExternalEventID)
	span.SetAttributes(attribute.String("orka.gateway.event.id", eventID))
	baseEvent := store.GatewayEvent{
		ID: eventID, Namespace: namespace, NamespaceUID: string(namespaceObject.UID),
		GatewayUID: string(object.UID), GatewayGeneration: object.Generation, GatewayName: object.Name,
		ExternalEventID: eventEnvelope.ExternalEventID, ProtocolVersion: eventEnvelope.ProtocolVersion,
		EventType: eventEnvelope.EventType, AccountID: eventEnvelope.AccountID, ContextID: eventEnvelope.ContextID,
		ThreadID: eventEnvelope.ThreadID, SenderID: eventEnvelope.Sender.ID,
		SenderDisplayName: protocol.SanitizeMessage(eventEnvelope.Sender.DisplayName, protocol.MaxIdentityBytes), Text: eventEnvelope.Text,
		ReplyTarget: eventEnvelope.ReplyTarget, Metadata: sanitizedMetadata(eventEnvelope.Metadata),
		OccurredAt: eventEnvelope.OccurredAt, ReceivedAt: now, NextAttemptAt: now,
		ExpiresAt: now.Add(s.Config.EventExpiry), CreatedAt: now, UpdatedAt: now,
	}
	traceCarrier := orkatracing.InjectContext(ctx)
	baseEvent.TraceParent = boundedTraceValue(traceCarrier.Get("traceparent"), 256)
	baseEvent.TraceState = boundedTraceValue(traceCarrier.Get("tracestate"), 1024)
	if existing, err := s.EventStore.GetGatewayEventDuplicate(ctx, &baseEvent, now); err == nil {
		return s.acknowledgeDuplicateEvent(ctx, existing, &baseEvent)
	} else if errors.Is(err, store.ErrDuplicateMismatch) {
		return nil, &HTTPError{Code: http.StatusConflict, Message: "externalEventId already identifies a different gateway event"}
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("look up gateway event duplicate: %w", err)
	}

	if !object.Status.Ready || object.Status.ObservedGeneration != object.Generation {
		return nil, &HTTPError{Code: http.StatusServiceUnavailable, Message: "gateway is not ready"}
	}
	resolver := EndpointResolver{Client: reader, AllowInsecureLoopback: s.Config.AllowInsecureLoopback}
	if _, _, err := resolver.Resolve(ctx, object); err != nil {
		return nil, &HTTPError{Code: http.StatusServiceUnavailable, Message: "gateway endpoint is not ready"}
	}
	class := &gatewayv1alpha1.GatewayClass{}
	if err := reader.Get(ctx, client.ObjectKey{Name: object.Spec.GatewayClassName}, class); err != nil ||
		!class.Status.Accepted || class.Status.ObservedGeneration != class.Generation {
		return nil, &HTTPError{Code: http.StatusServiceUnavailable, Message: "gateway class is not ready"}
	}
	if err := protocol.ValidateAllowedMetadata(eventEnvelope.Metadata, class.Spec.AllowedMetadataKeys); err != nil {
		return nil, &HTTPError{Code: http.StatusBadRequest, Message: err.Error()}
	}

	binding, rejection, err := s.resolveBinding(ctx, namespace, object.Name, eventEnvelope)
	if errors.Is(err, errGatewayBindingNotReady) {
		return nil, &HTTPError{Code: http.StatusServiceUnavailable, Message: "gateway binding is not ready"}
	}
	if err != nil {
		return nil, fmt.Errorf("resolve GatewayBinding: %w", err)
	}
	if rejection != "" {
		return s.admitRejectedEvent(ctx, baseEvent, rejection)
	}

	baseEvent.BindingName = binding.Name
	baseEvent.BindingUID = string(binding.UID)
	baseEvent.BindingGeneration = binding.Generation
	baseEvent.AgentName = binding.Spec.AgentRef.Name
	agent := &corev1alpha1.Agent{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: baseEvent.AgentName}, agent); err != nil {
		if apierrors.IsNotFound(err) {
			return s.admitRejectedEvent(ctx, baseEvent, "bound Agent is unavailable")
		}
		return nil, fmt.Errorf("resolve bound Agent: %w", err)
	}
	baseEvent.AgentUID = string(agent.UID)

	sessionName, err := deriveSessionName(object, binding, eventEnvelope)
	if err != nil {
		return s.admitRejectedEvent(ctx, baseEvent, protocol.SanitizeMessage(err.Error(), 1024))
	}
	baseEvent.SessionName = sessionName
	baseEvent.TaskName = gatewayTaskName(string(object.UID), eventEnvelope.ExternalEventID, now)
	record, created, err := s.EventStore.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: baseEvent, AppendUserMessage: true, PendingLimit: s.Config.PendingPerSession,
		GatewayRecordLimit:  s.Config.MaxRecordsPerGateway,
		RejectedRecordLimit: s.Config.MaxRejectedRecordsPerGateway,
	})
	if err != nil {
		if errors.Is(err, store.ErrDuplicateMismatch) {
			return nil, &HTTPError{Code: http.StatusConflict, Message: "externalEventId already identifies a different gateway event"}
		}
		if errors.Is(err, store.ErrConflict) {
			return s.admitRejectedEvent(ctx, baseEvent, "the selected Session is owned by another source")
		}
		if errors.Is(err, store.ErrCapacity) {
			gatewayIngressTotal.WithLabelValues("capacity").Inc()
			return nil, &HTTPError{Code: http.StatusTooManyRequests, Message: "gateway durable storage capacity reached"}
		}
		return nil, fmt.Errorf("persist accepted gateway event: %w", err)
	}
	if !created {
		return s.acknowledgeDuplicateEvent(ctx, record, &baseEvent)
	}
	s.markBindingInbound(ctx, binding, now)
	status := ingressStatusAccepted
	if record.State == store.GatewayEventDeadLettered {
		status = ingressStatusDeadLettered
		gatewayDeadLettersTotal.WithLabelValues("event").Inc()
		if err := s.ensureDenialDelivery(ctx, record, "session queue is full"); err != nil {
			return nil, fmt.Errorf("persist gateway denial delivery: %w", err)
		}
	}
	gatewayIngressTotal.WithLabelValues(status).Inc()
	return &protocol.IngressResponse{Status: status, EventID: record.ID, State: string(record.State), Message: record.StateMessage}, nil
}

func (s *Service) admitRejectedEvent(
	ctx context.Context, event store.GatewayEvent, message string,
) (*protocol.IngressResponse, error) {
	event.State = store.GatewayEventRejected
	event.StateMessage = protocol.SanitizeMessage(message, 1024)
	record, created, err := s.EventStore.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, GatewayRecordLimit: s.Config.MaxRecordsPerGateway,
		RejectedRecordLimit: s.Config.MaxRejectedRecordsPerGateway,
	})
	if err != nil {
		if errors.Is(err, store.ErrDuplicateMismatch) {
			return nil, &HTTPError{Code: http.StatusConflict, Message: "externalEventId already identifies a different gateway event"}
		}
		if errors.Is(err, store.ErrCapacity) {
			gatewayIngressTotal.WithLabelValues("capacity").Inc()
			return nil, &HTTPError{Code: http.StatusTooManyRequests, Message: "gateway durable storage capacity reached"}
		}
		return nil, fmt.Errorf("persist rejected gateway event: %w", err)
	}
	if !created {
		return s.acknowledgeDuplicateEvent(ctx, record, &event)
	}
	if record.State == store.GatewayEventRejected || record.State == store.GatewayEventDeadLettered {
		if err := s.ensureDenialDelivery(ctx, record, event.StateMessage); err != nil {
			return nil, fmt.Errorf("persist gateway denial delivery: %w", err)
		}
	}
	gatewayIngressTotal.WithLabelValues(ingressStatusRejected).Inc()
	return &protocol.IngressResponse{
		Status: ingressStatusRejected, EventID: record.ID, State: string(record.State), Message: record.StateMessage,
	}, nil
}

func (s *Service) acknowledgeDuplicateEvent(
	ctx context.Context,
	existing, candidate *store.GatewayEvent,
) (*protocol.IngressResponse, error) {
	if !matchingGatewayEventEnvelope(existing, candidate) {
		return nil, &HTTPError{
			Code: http.StatusConflict, Message: "externalEventId already identifies a different gateway event",
		}
	}
	if existing.State == store.GatewayEventRejected || existing.State == store.GatewayEventDeadLettered {
		if err := s.ensureDenialDelivery(ctx, existing, existing.StateMessage); err != nil {
			return nil, fmt.Errorf("persist gateway denial delivery: %w", err)
		}
	}
	gatewayIngressTotal.WithLabelValues(ingressStatusDuplicate).Inc()
	return &protocol.IngressResponse{
		Status: ingressStatusDuplicate, EventID: existing.ID, State: string(existing.State), Message: existing.StateMessage,
	}, nil
}

func matchingGatewayEventEnvelope(existing, candidate *store.GatewayEvent) bool {
	if existing == nil || candidate == nil {
		return false
	}
	return existing.ID == candidate.ID &&
		existing.Namespace == candidate.Namespace &&
		existing.NamespaceUID == candidate.NamespaceUID &&
		existing.GatewayUID == candidate.GatewayUID &&
		existing.GatewayName == candidate.GatewayName &&
		existing.ExternalEventID == candidate.ExternalEventID &&
		existing.ProtocolVersion == candidate.ProtocolVersion &&
		existing.EventType == candidate.EventType &&
		existing.AccountID == candidate.AccountID &&
		existing.ContextID == candidate.ContextID &&
		existing.ThreadID == candidate.ThreadID &&
		existing.SenderID == candidate.SenderID &&
		existing.SenderDisplayName == candidate.SenderDisplayName &&
		existing.Text == candidate.Text &&
		existing.ReplyTarget == candidate.ReplyTarget &&
		maps.Equal(existing.Metadata, candidate.Metadata) &&
		matchingOptionalTime(existing.OccurredAt, candidate.OccurredAt)
}

func matchingOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

// DispatchOnce claims one FIFO event and creates/links its deterministic Task.
func (s *Service) DispatchOnce(ctx context.Context) error {
	now := time.Now().UTC()
	event, err := s.EventStore.ClaimNextGatewayEvent(ctx, s.Config.Namespace, s.Owner, now, s.Config.ClaimLease)
	if err != nil {
		return err
	}
	ctx = gatewayTraceContext(ctx, event.TraceParent, event.TraceState)
	ctx, span := gatewayTracer.Start(ctx, "gateway.dispatch")
	defer span.End()
	span.SetAttributes(attribute.String("orka.gateway.event.id", event.ID))

	recoveryNow := time.Now().UTC()
	if handled, recoveryErr := s.reconcileExistingDispatchTask(ctx, event, recoveryNow); handled || recoveryErr != nil {
		return recoveryErr
	}
	binding, ready, err := s.resolveDispatchBinding(ctx, event)
	if err != nil || !ready {
		return err
	}
	freshNow := time.Now().UTC()
	if handled, err := s.handleExpiredDispatchClaim(ctx, event, binding, freshNow); handled || err != nil {
		return err
	}
	if event.ExpiresAt.Sub(freshNow) < minimumGatewayExecutionWindow {
		if err := s.expireGatewayEvent(
			ctx, event, s.Owner, "The message expired before execution could start.", freshNow, false,
		); err != nil && !errors.Is(err, store.ErrConflict) {
			return err
		}
		return nil
	}
	renewed, err := s.EventStore.RenewGatewayEventClaim(
		ctx, event.Namespace, event.ID, s.Owner, freshNow, s.Config.ClaimLease,
	)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			return nil
		}
		return err
	}
	capacityAvailable, err := s.namespaceTaskCapacityAvailable(ctx, renewed.Namespace, renewed.TaskName)
	if err != nil {
		return err
	}
	if !capacityAvailable {
		retryAt := time.Now().UTC().Add(eventBackoff(renewed.AttemptCount))
		if err := s.EventStore.RetryGatewayEvent(
			ctx, renewed.Namespace, renewed.ID, s.Owner, "namespace task limit reached", retryAt,
		); err != nil {
			return err
		}
		gatewayDispatchTotal.WithLabelValues("namespace_limit").Inc()
		return nil
	}
	linkedTask, _, ready, err := s.createOrFindGatewayTask(ctx, renewed, binding, freshNow)
	if err != nil || !ready {
		return err
	}
	if err := s.linkGatewayTask(ctx, renewed, linkedTask, freshNow); err != nil {
		return err
	}
	gatewayDispatchTotal.WithLabelValues("task_created").Inc()
	gatewayDispatchLatency.Observe(time.Since(renewed.ReceivedAt).Seconds())
	return nil
}

func (s *Service) namespaceTaskCapacityAvailable(ctx context.Context, namespace, taskName string) (bool, error) {
	if s.Config.MaxTasksPerNamespace <= 0 {
		return true, nil
	}
	var tasks corev1alpha1.TaskList
	if err := s.freshReader().List(ctx, &tasks, client.InNamespace(namespace)); err != nil {
		return false, fmt.Errorf("list namespace Tasks for gateway capacity: %w", err)
	}
	active := int32(0)
	for i := range tasks.Items {
		task := &tasks.Items[i]
		if task.Name == taskName {
			// A deterministic Task that already exists can be linked without creating more work.
			return true, nil
		}
		switch task.Status.Phase {
		case "", corev1alpha1.TaskPhasePending, corev1alpha1.TaskPhaseRunning, corev1alpha1.TaskPhaseFinalizing:
			// The controller initializes an empty phase to Pending. Count that brief window so
			// one dispatch batch cannot create more non-terminal Tasks than the limiter can admit.
			active++
		}
	}
	return active < s.Config.MaxTasksPerNamespace, nil
}

func (s *Service) reconcileExistingDispatchTask(
	ctx context.Context, event *store.GatewayEvent, now time.Time,
) (bool, error) {
	existing := &corev1alpha1.Task{}
	if err := s.freshReader().Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.TaskName}, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return true, fmt.Errorf("read deterministic gateway Task during claim recovery: %w", err)
	}

	gatewayObject := &gatewayv1alpha1.Gateway{}
	if err := s.freshReader().Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.GatewayName}, gatewayObject); err != nil {
		if apierrors.IsNotFound(err) {
			return true, s.expireDispatchEvent(ctx, event, "The admitted Gateway no longer exists.")
		}
		return true, fmt.Errorf("read admitted Gateway during Task recovery: %w", err)
	}
	if string(gatewayObject.UID) != event.GatewayUID || gatewayObject.Generation != event.GatewayGeneration {
		return true, s.expireDispatchEvent(ctx, event, "The admitted Gateway identity changed.")
	}

	binding := &gatewayv1alpha1.GatewayBinding{}
	if err := s.freshReader().Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.BindingName}, binding); err != nil {
		if apierrors.IsNotFound(err) {
			return true, s.expireDispatchEvent(ctx, event, "The admitted GatewayBinding no longer exists.")
		}
		return true, fmt.Errorf("read admitted GatewayBinding during Task recovery: %w", err)
	}
	if !bindingIdentityMatchesAdmittedEvent(binding, event) {
		return true, s.expireDispatchEvent(ctx, event, "The admitted GatewayBinding identity or routing changed.")
	}

	agent := &corev1alpha1.Agent{}
	if err := s.freshReader().Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.AgentName}, agent); err != nil {
		if apierrors.IsNotFound(err) {
			return true, s.expireDispatchEvent(ctx, event, "The admitted Agent no longer exists.")
		}
		return true, fmt.Errorf("read admitted Agent during Task recovery: %w", err)
	}
	if event.AgentUID != "" && string(agent.UID) != event.AgentUID {
		return true, s.expireDispatchEvent(ctx, event, "The admitted Agent identity changed.")
	}

	expected := taskForGatewayEvent(event, binding, now)
	if gatewayTaskMatchesExpected(existing, expected, event, binding) {
		return true, s.EventStore.MarkGatewayEventTaskCreated(
			ctx, event.Namespace, event.ID, event.TaskName, string(existing.UID), s.Owner, now,
		)
	}
	if event.ExpiresAt.Sub(now) < minimumGatewayExecutionWindow {
		return true, s.expireDispatchEvent(ctx, event, "The message expired after a Task identity conflict.")
	}
	return false, nil
}

func (s *Service) handleExpiredDispatchClaim(
	ctx context.Context, event *store.GatewayEvent, binding *gatewayv1alpha1.GatewayBinding, now time.Time,
) (bool, error) {
	if event.ExpiresAt.Sub(now) >= minimumGatewayExecutionWindow {
		return false, nil
	}
	existing := &corev1alpha1.Task{}
	err := s.freshReader().Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.TaskName}, existing)
	switch {
	case apierrors.IsNotFound(err):
		err = s.expireGatewayEvent(ctx, event, s.Owner, "The message expired before execution could start.", now, false)
		if err == nil {
			gatewayDeadLettersTotal.WithLabelValues("event").Inc()
		}
		return true, err
	case err != nil:
		return true, err
	}
	if binding != nil {
		expected := taskForGatewayEvent(event, binding, now)
		if gatewayTaskMatchesExpected(existing, expected, event, binding) {
			return true, s.EventStore.MarkGatewayEventTaskCreated(
				ctx, event.Namespace, event.ID, event.TaskName, string(existing.UID), s.Owner, now,
			)
		}
	}
	err = s.expireGatewayEvent(ctx, event, s.Owner, "The message expired after a Task identity conflict.", now, false)
	if err == nil {
		gatewayDeadLettersTotal.WithLabelValues("event").Inc()
	}
	return true, err
}

func (s *Service) resolveDispatchBinding(
	ctx context.Context, event *store.GatewayEvent,
) (*gatewayv1alpha1.GatewayBinding, bool, error) {
	gatewayObject := &gatewayv1alpha1.Gateway{}
	err := s.freshReader().Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.GatewayName}, gatewayObject)
	if apierrors.IsNotFound(err) {
		return nil, false, s.expireDispatchEvent(ctx, event, "The admitted Gateway no longer exists.")
	}
	if err != nil {
		s.retryEvent(ctx, event, "admitted Gateway is not ready", eventBackoff(event.AttemptCount))
		gatewayDispatchTotal.WithLabelValues("gateway_not_ready").Inc()
		return nil, false, nil
	}
	if string(gatewayObject.UID) != event.GatewayUID {
		return nil, false, s.expireDispatchEvent(ctx, event, "The admitted Gateway identity changed.")
	}
	if gatewayObject.Generation != event.GatewayGeneration {
		return nil, false, s.expireDispatchEvent(ctx, event, "The admitted Gateway generation changed.")
	}
	if !gatewayObject.Status.Ready || gatewayObject.Status.ObservedGeneration != gatewayObject.Generation {
		s.retryEvent(ctx, event, "admitted Gateway is not ready", eventBackoff(event.AttemptCount))
		gatewayDispatchTotal.WithLabelValues("gateway_not_ready").Inc()
		return nil, false, nil
	}
	binding := &gatewayv1alpha1.GatewayBinding{}
	err = s.freshReader().Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.BindingName}, binding)
	if apierrors.IsNotFound(err) {
		return nil, false, s.expireDispatchEvent(ctx, event, "The admitted GatewayBinding no longer exists.")
	}
	if err != nil {
		s.retryEvent(ctx, event, "binding changed or is not ready", eventBackoff(event.AttemptCount))
		gatewayDispatchTotal.WithLabelValues("binding_not_ready").Inc()
		return nil, false, nil
	}
	if string(binding.UID) != event.BindingUID || binding.Generation != event.BindingGeneration {
		return nil, false, s.expireDispatchEvent(ctx, event, "The admitted GatewayBinding identity changed.")
	}
	if !binding.Status.Ready || binding.Status.ObservedGeneration != binding.Generation {
		s.retryEvent(ctx, event, "binding changed or is not ready", eventBackoff(event.AttemptCount))
		gatewayDispatchTotal.WithLabelValues("binding_not_ready").Inc()
		return nil, false, nil
	}
	if !bindingMatchesAdmittedEvent(binding, event) {
		return nil, false, s.expireDispatchEvent(ctx, event, "The admitted GatewayBinding routing changed.")
	}
	agent := &corev1alpha1.Agent{}
	err = s.freshReader().Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.AgentName}, agent)
	if apierrors.IsNotFound(err) {
		return nil, false, s.expireDispatchEvent(ctx, event, "The admitted Agent no longer exists.")
	}
	if err != nil {
		s.retryEvent(ctx, event, "admitted agent is not available", eventBackoff(event.AttemptCount))
		gatewayDispatchTotal.WithLabelValues("agent_not_ready").Inc()
		return nil, false, nil
	}
	if event.AgentUID != "" && string(agent.UID) != event.AgentUID {
		return nil, false, s.expireDispatchEvent(ctx, event, "The admitted Agent identity changed.")
	}
	return binding, true, nil
}

func (s *Service) expireDispatchEvent(ctx context.Context, event *store.GatewayEvent, reason string) error {
	if err := s.deleteUnlinkedGatewayTask(ctx, event); err != nil {
		return err
	}
	err := s.expireGatewayEvent(ctx, event, s.Owner, reason, time.Now().UTC(), false)
	if err == nil {
		gatewayDeadLettersTotal.WithLabelValues("event").Inc()
	}
	return err
}

func (s *Service) expireGatewayEvent(
	ctx context.Context,
	event *store.GatewayEvent,
	owner, reason string,
	now time.Time,
	taskIdentityVerified bool,
) error {
	if event == nil {
		return store.ValidationErrorf("gateway event is required")
	}
	replyTarget := strings.TrimSpace(event.ReplyTarget)
	if replyTarget == "" {
		replyTarget = strings.TrimSpace(event.ContextID)
	}
	if replyTarget == "" {
		return s.EventStore.ExpireGatewayEvent(ctx, event.Namespace, event.ID, owner, reason, now)
	}
	expiresAt := gatewayDeliveryExpiresAt(event.ExpiresAt, now, s.Config)
	deliveryID := gatewayDeliveryID(event, "expiry")
	createdAt := now
	if event.State == store.GatewayEventExpired && event.CompletedAt != nil {
		createdAt = event.CompletedAt.Add(time.Duration(event.TranscriptOrder) * time.Nanosecond)
	}
	taskName := ""
	metadata := map[string]string{"eventId": event.ID}
	if taskIdentityVerified && event.TaskUID != "" {
		taskName = event.TaskName
		metadata["taskName"] = event.TaskName
	}
	_, _, err := s.EventStore.ExpireGatewayEventWithDelivery(ctx, store.GatewayExpiryProjection{
		EventID: event.ID,
		Owner:   owner,
		Reason:  reason,
		Delivery: store.GatewayDelivery{
			ID: deliveryID, IdempotencyID: deliveryID, Namespace: event.Namespace, NamespaceUID: event.NamespaceUID,
			GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
			GatewayName: event.GatewayName, BindingName: event.BindingName, EventID: event.ID,
			TaskName: taskName, SessionName: event.SessionName, Kind: protocol.DeliveryKindError,
			State: store.GatewayDeliveryPending, AccountID: event.AccountID, ContextID: event.ContextID,
			ThreadID: event.ThreadID, ReplyTarget: replyTarget, Text: reason,
			Metadata:    metadata,
			MaxAttempts: s.Config.DeliveryMaxAttempts, NextAttemptAt: now, ExpiresAt: expiresAt,
			TraceParent: event.TraceParent, TraceState: event.TraceState, CreatedAt: createdAt, UpdatedAt: now,
		},
		CompletedAt: now,
	})
	return err
}

func (s *Service) deleteUnlinkedGatewayTask(ctx context.Context, event *store.GatewayEvent) error {
	if event == nil || event.TaskUID != "" {
		return nil
	}
	task := &corev1alpha1.Task{}
	key := client.ObjectKey{Namespace: event.Namespace, Name: event.TaskName}
	if err := s.freshReader().Get(ctx, key, task); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("read unlinked gateway Task before expiry: %w", err)
	}
	if !gatewayTaskCorrelatesWithEvent(task, event) {
		return nil
	}
	if !task.DeletionTimestamp.IsZero() && time.Since(task.DeletionTimestamp.Time) >= gatewayTaskCleanupGrace {
		return nil
	}
	if task.DeletionTimestamp.IsZero() {
		if err := deleteGatewayTaskWithUID(ctx, s.Client, task); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete unlinked gateway Task before expiry: %w", err)
		}
	}
	remaining := &corev1alpha1.Task{}
	if err := s.freshReader().Get(ctx, key, remaining); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("confirm unlinked gateway Task cleanup: %w", err)
	}
	return fmt.Errorf("%w: unlinked gateway Task cleanup is pending", store.ErrNotReady)
}

func (s *Service) createOrFindGatewayTask(
	ctx context.Context,
	event *store.GatewayEvent,
	binding *gatewayv1alpha1.GatewayBinding,
	now time.Time,
) (*corev1alpha1.Task, bool, bool, error) {
	task := taskForGatewayEvent(event, binding, now)
	orkatracing.StampTaskTraceContext(ctx, task)
	createErr := s.Client.Create(ctx, task)
	if createErr == nil {
		return s.refreshGatewayTaskUID(ctx, event, task, true)
	}
	existing := &corev1alpha1.Task{}
	lookupErr := s.freshReader().Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.TaskName}, existing)
	if lookupErr == nil {
		if !gatewayTaskMatchesExpected(existing, task, event, binding) {
			s.retryEvent(ctx, event, "deterministic task name collision", eventBackoff(event.AttemptCount))
			gatewayDispatchTotal.WithLabelValues("name_collision").Inc()
			return nil, false, false, nil
		}
		// The Create response may be ambiguous even though the API server committed
		// the deterministic Task. Treat the fresh read as authoritative and link it.
		return s.refreshGatewayTaskUID(ctx, event, existing, false)
	}
	if !apierrors.IsNotFound(lookupErr) {
		return nil, false, false, errors.Join(createErr, fmt.Errorf("read deterministic gateway Task after create: %w", lookupErr))
	}
	if definitiveGatewayTaskCreateFailure(createErr) {
		reason := "The message could not start because the configured task is invalid."
		expireErr := s.expireDispatchEvent(ctx, event, reason)
		if expireErr == nil {
			event.State = store.GatewayEventExpired
			event.StateMessage = reason
		}
		gatewayDispatchTotal.WithLabelValues("create_rejected").Inc()
		return nil, false, false, expireErr
	}
	// Unknown transport/server errors may be returned after the API server commits.
	// Keep the Dispatching claim intact so lease recovery reconciles before requeue.
	gatewayDispatchTotal.WithLabelValues("create_ambiguous").Inc()
	return nil, false, false, fmt.Errorf("gateway Task create outcome is ambiguous: %w", createErr)
}

func definitiveGatewayTaskCreateFailure(err error) bool {
	return apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsForbidden(err) ||
		apierrors.IsUnauthorized(err) || apierrors.IsMethodNotSupported(err) || apierrors.IsNotAcceptable(err)
}

func (s *Service) refreshGatewayTaskUID(
	ctx context.Context, event *store.GatewayEvent, task *corev1alpha1.Task, created bool,
) (*corev1alpha1.Task, bool, bool, error) {
	if task.UID == "" {
		refreshed := &corev1alpha1.Task{}
		if err := s.freshReader().Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.TaskName}, refreshed); err != nil {
			return nil, created, false, err
		}
		task = refreshed
	}
	if task.UID == "" {
		return nil, created, false, fmt.Errorf("linked gateway Task UID is unavailable")
	}
	return task, created, true, nil
}

func deleteGatewayTaskWithUID(ctx context.Context, kubeClient client.Client, task *corev1alpha1.Task) error {
	if kubeClient == nil || task == nil || task.UID == "" {
		return fmt.Errorf("gateway Task UID is required for deletion")
	}
	uid := task.UID
	return kubeClient.Delete(ctx, task, &client.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
}

func (s *Service) linkGatewayTask(
	ctx context.Context,
	event *store.GatewayEvent,
	linkedTask *corev1alpha1.Task,
	now time.Time,
) error {
	err := s.EventStore.MarkGatewayEventTaskCreated(
		ctx, event.Namespace, event.ID, event.TaskName, string(linkedTask.UID), s.Owner, now,
	)
	if err == nil {
		return nil
	}
	current, getErr := s.EventStore.GetGatewayEvent(ctx, event.Namespace, event.ID)
	if getErr == nil && current.State == store.GatewayEventTaskCreated && current.TaskUID == string(linkedTask.UID) {
		return nil
	}
	// A reclaimed owner may already be linking this deterministic Task. Leave it
	// in place so lease recovery can reconcile the exact UID instead of deleting
	// work another owner has adopted.
	return fmt.Errorf("link gateway event %s to Task: %w", event.ID, err)
}

// ProjectTerminals fairly rotates TaskCreated events while projecting terminal work.
func (s *Service) ProjectTerminals(ctx context.Context) error {
	now := time.Now().UTC()
	events, err := s.EventStore.ListGatewayEvents(ctx, store.GatewayEventFilter{
		Namespace: s.Config.Namespace,
		States:    []store.GatewayEventState{store.GatewayEventTaskCreated},
		DueBefore: &now, OrderByNextAttempt: true, Limit: s.Config.BatchSize,
	})
	if err != nil {
		return err
	}
	return s.projectTerminalEvents(ctx, events, now)
}

func (s *Service) projectTerminalEvents(ctx context.Context, events []store.GatewayEvent, now time.Time) error {
	var errs []error
	for i := range events {
		event := &events[i]
		task := &corev1alpha1.Task{}
		if err := s.freshReader().Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.TaskName}, task); err != nil {
			if apierrors.IsNotFound(err) {
				expireErr := s.expireGatewayEvent(ctx, event, "", "The linked task was deleted before a response could be delivered.", now, false)
				if expireErr != nil && !errors.Is(expireErr, store.ErrConflict) {
					errs = append(errs, expireErr)
				} else if expireErr == nil {
					gatewayDeadLettersTotal.WithLabelValues("event").Inc()
				}
				continue
			}
			errs = append(errs, err)
			s.deferProjection(ctx, event, now)
			continue
		}
		if event.TaskUID == "" || string(task.UID) != event.TaskUID || !gatewayTaskCorrelatesWithEvent(task, event) {
			if !now.Before(event.ExpiresAt) {
				if expireErr := s.expireGatewayEvent(
					ctx, event, "", "The task identity changed before a response could be delivered.", now, false,
				); expireErr != nil && !errors.Is(expireErr, store.ErrConflict) {
					errs = append(errs, expireErr)
				}
			} else {
				errs = append(errs, fmt.Errorf("linked gateway Task identity changed for event %s", event.ID))
				s.deferProjection(ctx, event, now)
			}
			continue
		}
		if isTerminalTaskPhase(task.Status.Phase) {
			projected, err := s.projectTerminal(ctx, event, task)
			if errors.Is(err, errGatewayResultNotReady) {
				s.deferProjection(ctx, event, now)
				continue
			}
			if err != nil {
				errs = append(errs, err)
				s.deferProjection(ctx, event, now)
				continue
			}
			if projected {
				continue
			}
		}
		if !now.Before(event.ExpiresAt) {
			if task.DeletionTimestamp.IsZero() {
				if err := deleteGatewayTaskWithUID(ctx, s.Client, task); err != nil && !apierrors.IsNotFound(err) {
					errs = append(errs, err)
				}
			} else if now.Sub(task.DeletionTimestamp.Time) >= gatewayTaskCleanupGrace {
				if err := s.expireGatewayEvent(
					ctx, event, "", "The task cleanup deadline was exceeded.", now, true,
				); err != nil && !errors.Is(err, store.ErrConflict) {
					errs = append(errs, err)
				}
				gatewayDeadLettersTotal.WithLabelValues("event").Inc()
				continue
			}
			s.deferProjection(ctx, event, now)
			continue
		}
		s.deferProjection(ctx, event, now)
	}
	return errors.Join(errs...)
}

func (s *Service) deferProjection(ctx context.Context, event *store.GatewayEvent, now time.Time) {
	_ = s.EventStore.DeferGatewayEventProjection(
		ctx, event.Namespace, event.ID, now.Add(s.Config.PollInterval),
	)
}

func (s *Service) projectTerminal(
	ctx context.Context,
	event *store.GatewayEvent,
	task *corev1alpha1.Task,
) (bool, error) {
	ctx = gatewayTraceContext(ctx, event.TraceParent, event.TraceState)
	ctx, span := gatewayTracer.Start(ctx, "gateway.project_terminal")
	defer span.End()
	span.SetAttributes(attribute.String("orka.gateway.event.id", event.ID), attribute.String("orka.task.name", task.Name))
	now := time.Now().UTC()
	kind := protocol.DeliveryKindError
	role := "assistant"
	messageID := "gateway:" + event.ID + ":error"
	text := terminalErrorText(task.Status.Phase, task.Status.Message)
	if task.Status.Phase == corev1alpha1.TaskPhaseSucceeded {
		resultReady := false
		if s.ResultStore != nil {
			result, err := s.ResultStore.GetResult(ctx, task.Namespace, task.Name)
			if err == nil {
				resultReady = true
				text = boundedText(string(result))
				if strings.TrimSpace(text) == "" {
					text = "The task completed without a text response."
				}
				kind = protocol.DeliveryKindFinal
				messageID = "gateway:" + event.ID + ":assistant"
			} else if !errors.Is(err, store.ErrNotFound) {
				return false, err
			}
		}
		if !resultReady {
			if task.Status.CompletionTime != nil && now.Before(task.Status.CompletionTime.Add(gatewayResultProjectionGrace)) {
				return false, errGatewayResultNotReady
			}
			text = "The task completed, but its response could not be retrieved. Please try again."
		}
	}
	deliveryID := gatewayDeliveryID(event, kind)
	replyTarget := strings.TrimSpace(event.ReplyTarget)
	if replyTarget == "" {
		replyTarget = event.ContextID
	}
	messageMetadata := map[string]string{
		"gateway": event.GatewayName, "binding": event.BindingName,
		"eventId": event.ID, "taskName": task.Name, "deliveryId": deliveryID,
	}
	deliveryTrace := orkatracing.InjectContext(ctx)
	deliveryExpiresAt := gatewayDeliveryExpiresAt(event.ExpiresAt, now, s.Config)
	delivery := store.GatewayDelivery{
		ID: deliveryID, IdempotencyID: deliveryID, Namespace: event.Namespace, NamespaceUID: event.NamespaceUID,
		GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
		GatewayName: event.GatewayName, BindingName: event.BindingName, EventID: event.ID, TaskName: task.Name,
		SessionName: event.SessionName, Kind: kind, AccountID: event.AccountID, ContextID: event.ContextID,
		ThreadID: event.ThreadID, ReplyTarget: replyTarget, Text: text,
		Metadata:    map[string]string{"eventId": event.ID, "taskName": task.Name},
		TraceParent: boundedTraceValue(deliveryTrace.Get("traceparent"), 256),
		TraceState:  boundedTraceValue(deliveryTrace.Get("tracestate"), 1024),
		State:       store.GatewayDeliveryPending, MaxAttempts: s.Config.DeliveryMaxAttempts,
		NextAttemptAt: now, ExpiresAt: deliveryExpiresAt, CreatedAt: now, UpdatedAt: now,
	}
	_, _, err := s.EventStore.ProjectGatewayTerminal(ctx, store.GatewayTerminalProjection{
		EventID: event.ID,
		Message: store.SessionMessage{
			ID: messageID, Role: role, Content: text, SourceType: "gateway-task", SourceRef: task.Name,
			Metadata: messageMetadata, Timestamp: now,
		},
		Delivery: delivery, CompletedAt: now,
	})
	if err != nil {
		return false, err
	}
	if task.Status.StartTime != nil && task.Status.CompletionTime != nil {
		duration := task.Status.CompletionTime.Sub(task.Status.StartTime.Time)
		if duration >= 0 {
			gatewayTaskDuration.Observe(duration.Seconds())
		}
	}
	return true, nil
}

// DeliverOnce claims one due delivery and records the synchronous terminal adapter result.
func (s *Service) DeliverOnce(ctx context.Context) error {
	claimAt := time.Now().UTC()
	delivery, err := s.DeliveryStore.ClaimNextGatewayDelivery(ctx, s.Config.Namespace, s.Owner, claimAt, s.Config.ClaimLease)
	if err != nil {
		return err
	}
	ctx = gatewayTraceContext(ctx, delivery.TraceParent, delivery.TraceState)
	ctx, span := gatewayTracer.Start(ctx, "gateway.deliver")
	defer span.End()
	span.SetAttributes(attribute.String("orka.gateway.delivery.id", delivery.ID), attribute.String("orka.gateway.event.id", delivery.EventID))

	object := &gatewayv1alpha1.Gateway{}
	err = s.freshReader().Get(ctx, client.ObjectKey{Namespace: delivery.Namespace, Name: delivery.GatewayName}, object)
	if apierrors.IsNotFound(err) {
		gatewayDeliveryTotal.WithLabelValues("non_retryable_error").Inc()
		gatewayDeadLettersTotal.WithLabelValues("delivery").Inc()
		return s.DeliveryStore.MarkGatewayDeliveryTerminal(
			ctx, delivery.Namespace, delivery.ID, s.Owner, store.GatewayDeliveryDeadLettered,
			"Gateway no longer exists", time.Now().UTC(),
		)
	}
	if err != nil {
		return s.retryOrDeadLetterDelivery(ctx, delivery, "Gateway is unavailable", time.Now().UTC())
	}
	if string(object.UID) != delivery.GatewayUID {
		gatewayDeliveryTotal.WithLabelValues("non_retryable_error").Inc()
		gatewayDeadLettersTotal.WithLabelValues("delivery").Inc()
		return s.DeliveryStore.MarkGatewayDeliveryTerminal(
			ctx, delivery.Namespace, delivery.ID, s.Owner, store.GatewayDeliveryDeadLettered,
			"Gateway identity changed", time.Now().UTC(),
		)
	}
	if delivery.GatewayGeneration <= 0 || object.Generation != delivery.GatewayGeneration {
		gatewayDeliveryTotal.WithLabelValues("non_retryable_error").Inc()
		gatewayDeadLettersTotal.WithLabelValues("delivery").Inc()
		return s.DeliveryStore.MarkGatewayDeliveryTerminal(
			ctx, delivery.Namespace, delivery.ID, s.Owner, store.GatewayDeliveryDeadLettered,
			"Gateway generation changed", time.Now().UTC(),
		)
	}
	if !object.Status.Ready || object.Status.ObservedGeneration != object.Generation {
		return s.retryOrDeadLetterDelivery(ctx, delivery, "Gateway is not ready for delivery", time.Now().UTC())
	}
	reader := s.freshReader()
	resolver := EndpointResolver{Client: reader, AllowInsecureLoopback: s.Config.AllowInsecureLoopback}
	endpoint, _, err := resolver.Resolve(ctx, object)
	if err != nil {
		return s.retryOrDeadLetterDelivery(ctx, delivery, "Gateway endpoint is unavailable", time.Now().UTC())
	}
	outbound, err := ReadBearerSecret(ctx, reader, object, AuthDirectionOutbound, endpoint)
	if err != nil {
		return s.retryOrDeadLetterDelivery(ctx, delivery, "Gateway outbound authentication is unavailable", time.Now().UTC())
	}
	if object.Status.ObservedOutboundAuthRefVersion != outbound.ResourceVersion {
		return s.retryOrDeadLetterDelivery(ctx, delivery, "Gateway outbound authentication has not been probed", time.Now().UTC())
	}
	request := protocol.DeliveryRequest{
		ProtocolVersion: protocol.Version, DeliveryID: delivery.ID, IdempotencyID: delivery.IdempotencyID,
		OriginatingEvent: delivery.EventID,
		TaskRef:          &protocol.ResourceReference{Namespace: delivery.Namespace, Name: delivery.TaskName},
		SessionRef:       &protocol.ResourceReference{Namespace: delivery.Namespace, Name: delivery.SessionName},
		Kind:             delivery.Kind, AccountID: delivery.AccountID, ContextID: delivery.ContextID, ThreadID: delivery.ThreadID,
		ReplyTarget: delivery.ReplyTarget, Text: delivery.Text, Metadata: cloneMetadata(delivery.Metadata),
	}
	if delivery.TaskName == "" {
		request.TaskRef = nil
	}
	if delivery.SessionName == "" {
		request.SessionRef = nil
	}
	if err := protocol.ValidateDeliveryRequest(&request); err != nil {
		gatewayDeliveryTotal.WithLabelValues("invalid").Inc()
		return s.DeliveryStore.MarkGatewayDeliveryTerminal(ctx, delivery.Namespace, delivery.ID, s.Owner, store.GatewayDeliveryDeadLettered, "delivery validation failed", time.Now().UTC())
	}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	deliveryWindow := delivery.ExpiresAt.Sub(time.Now().UTC())
	if deliveryWindow <= 0 {
		return s.DeliveryStore.MarkGatewayDeliveryTerminal(
			ctx, delivery.Namespace, delivery.ID, s.Owner, store.GatewayDeliveryExpired, "delivery expired", time.Now().UTC(),
		)
	}
	requestTimeout := min(s.Config.DeliveryTimeout, deliveryWindow)
	deliveryCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(deliveryCtx, http.MethodPost, strings.TrimRight(endpoint, "/")+"/v1/deliveries", bytes.NewReader(body))
	if err != nil {
		return s.retryOrDeadLetterDelivery(ctx, delivery, "adapter request creation failed", time.Now().UTC())
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+outbound.Token)
	httpClient, err := NewAdapterHTTPClient(
		s.HTTPClient, requestTimeout, object.Spec.Adapter.ServiceRef != nil, s.Config.AllowInsecureLoopback,
	)
	if err != nil {
		return s.retryOrDeadLetterDelivery(ctx, delivery, "adapter HTTP client is unsafe", time.Now().UTC())
	}
	response, err := httpClient.Do(httpRequest)
	if err != nil {
		gatewayDeliveryTotal.WithLabelValues("retryable_error").Inc()
		return s.retryOrDeadLetterDelivery(ctx, delivery, "adapter request failed", time.Now().UTC())
	}
	defer response.Body.Close() //nolint:errcheck
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, protocol.MaxAdapterResponseBytes+1))
	outcomeAt := time.Now().UTC()
	if readErr != nil || len(responseBody) > protocol.MaxAdapterResponseBytes {
		gatewayDeliveryTotal.WithLabelValues("retryable_error").Inc()
		return s.retryOrDeadLetterDelivery(ctx, delivery, "adapter response was invalid", outcomeAt)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if retryableAdapterHTTPStatus(response.StatusCode) {
			gatewayDeliveryTotal.WithLabelValues("retryable_error").Inc()
			return s.retryOrDeadLetterDelivery(ctx, delivery, fmt.Sprintf("adapter returned HTTP %d", response.StatusCode), outcomeAt)
		}
		gatewayDeliveryTotal.WithLabelValues("non_retryable_error").Inc()
		gatewayDeadLettersTotal.WithLabelValues("delivery").Inc()
		return s.DeliveryStore.MarkGatewayDeliveryTerminal(ctx, delivery.Namespace, delivery.ID, s.Owner, store.GatewayDeliveryDeadLettered, fmt.Sprintf("adapter returned HTTP %d", response.StatusCode), outcomeAt)
	}
	adapterResult, err := protocol.DecodeDeliveryResponse(responseBody)
	if err != nil {
		gatewayDeliveryTotal.WithLabelValues("retryable_error").Inc()
		return s.retryOrDeadLetterDelivery(ctx, delivery, "adapter returned an invalid delivery result", outcomeAt)
	}
	switch adapterResult.Status {
	case protocol.DeliveryStatusDelivered:
		return s.completeGatewayDelivery(ctx, delivery, adapterResult.ProviderMessageID, outcomeAt)
	case protocol.DeliveryStatusRetryableError:
		gatewayDeliveryTotal.WithLabelValues("retryable_error").Inc()
		return s.retryOrDeadLetterDelivery(ctx, delivery, adapterResult.Message, outcomeAt)
	case protocol.DeliveryStatusNonRetryableError:
		gatewayDeliveryTotal.WithLabelValues("non_retryable_error").Inc()
		gatewayDeadLettersTotal.WithLabelValues("delivery").Inc()
		return s.DeliveryStore.MarkGatewayDeliveryTerminal(ctx, delivery.Namespace, delivery.ID, s.Owner, store.GatewayDeliveryDeadLettered, adapterResult.Message, outcomeAt)
	default:
		return s.retryOrDeadLetterDelivery(ctx, delivery, "adapter returned an unsupported result", outcomeAt)
	}
}

func (s *Service) completeGatewayDelivery(
	ctx context.Context,
	delivery *store.GatewayDelivery,
	providerMessageID string,
	outcomeAt time.Time,
) error {
	providerMessageID = protocol.SanitizeMessage(providerMessageID, protocol.MaxIdentityBytes)
	// Correlate the Task before committing the delivery as terminal. If the patch fails,
	// the Sending lease expires and the same idempotent delivery is replayed, allowing
	// correlation to converge without creating a second provider-side send.
	if err := s.markTaskDeliveryCorrelation(ctx, delivery, providerMessageID); err != nil {
		return err
	}
	if err := s.DeliveryStore.MarkGatewayDeliveryDelivered(
		ctx, delivery.Namespace, delivery.ID, s.Owner, providerMessageID, outcomeAt,
	); err != nil {
		return err
	}
	gatewayDeliveryTotal.WithLabelValues("delivered").Inc()
	gatewayDeliveryLatency.Observe(max(0, outcomeAt.Sub(delivery.CreatedAt).Seconds()))
	s.markBindingOutbound(ctx, delivery.Namespace, delivery.BindingName, outcomeAt)
	return nil
}

func retryableAdapterHTTPStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusRequestTimeout,
		http.StatusTooEarly, http.StatusTooManyRequests:
		return true
	default:
		return statusCode >= 500
	}
}

// RetryDelivery manually requeues one dead-lettered delivery with a fresh bounded expiry.
func (s *Service) RetryDelivery(ctx context.Context, namespace, id string) (*store.GatewayDelivery, error) {
	if s == nil || !s.Config.Enabled || s.DeliveryStore == nil {
		return nil, &HTTPError{Code: http.StatusServiceUnavailable, Message: "gateway delivery processing is disabled"}
	}
	now := time.Now().UTC()
	return s.DeliveryStore.RetryGatewayDelivery(ctx, namespace, id, now, now.Add(s.Config.EventExpiry))
}

func (s *Service) resolveBinding(ctx context.Context, namespace, gatewayName string, event *protocol.EventEnvelope) (*gatewayv1alpha1.GatewayBinding, string, error) {
	list := &gatewayv1alpha1.GatewayBindingList{}
	if err := s.freshReader().List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, "", err
	}
	contextMatches := make([]*gatewayv1alpha1.GatewayBinding, 0)
	authorizedReady := make([]*gatewayv1alpha1.GatewayBinding, 0)
	authorizedUnready := make([]*gatewayv1alpha1.GatewayBinding, 0)
	for i := range list.Items {
		binding := &list.Items[i]
		if binding.Spec.GatewayRef.Name != gatewayName {
			continue
		}
		match := binding.Spec.Match
		if match.AccountID != event.AccountID || match.ContextID != event.ContextID {
			continue
		}
		if match.ThreadID != "" && match.ThreadID != event.ThreadID {
			continue
		}
		contextMatches = append(contextMatches, binding)
		if senderAllowed(binding, event.Sender.ID) {
			if binding.Status.Ready && binding.Status.ObservedGeneration == binding.Generation {
				authorizedReady = append(authorizedReady, binding)
			} else {
				authorizedUnready = append(authorizedUnready, binding)
			}
		}
	}
	if len(contextMatches) == 0 {
		return nil, "no ready binding matches this context", nil
	}
	if len(authorizedReady) == 0 && len(authorizedUnready) == 0 {
		return nil, "sender is not authorized for this context", nil
	}
	if len(authorizedReady) == 0 {
		return nil, "", errGatewayBindingNotReady
	}
	highest := authorizedReady[0].Spec.Priority
	for _, binding := range authorizedReady[1:] {
		if binding.Spec.Priority > highest {
			highest = binding.Spec.Priority
		}
	}
	winners := make([]*gatewayv1alpha1.GatewayBinding, 0, 1)
	for _, binding := range authorizedReady {
		if binding.Spec.Priority == highest {
			winners = append(winners, binding)
		}
	}
	if len(winners) != 1 {
		return nil, "multiple equal-priority bindings match this event", nil
	}
	return winners[0].DeepCopy(), "", nil
}

func senderAllowed(binding *gatewayv1alpha1.GatewayBinding, senderID string) bool {
	if binding.Spec.Match.SenderID != "" && binding.Spec.Match.SenderID != senderID {
		return false
	}
	mode := binding.Spec.SenderPolicy.Mode
	if mode == "" {
		mode = gatewayv1alpha1.GatewaySenderPolicyAllowlist
	}
	if mode == gatewayv1alpha1.GatewaySenderPolicyAll {
		return true
	}
	if binding.Spec.Match.SenderID == senderID && len(binding.Spec.SenderPolicy.AllowedSenderIDs) == 0 {
		return true
	}
	return slices.Contains(binding.Spec.SenderPolicy.AllowedSenderIDs, senderID)
}

func deriveSessionName(object *gatewayv1alpha1.Gateway, binding *gatewayv1alpha1.GatewayBinding, event *protocol.EventEnvelope) (string, error) {
	mode := binding.Spec.Session.Mode
	if mode == "" {
		mode = gatewayv1alpha1.GatewaySessionContext
	}
	parts := []string{string(object.UID), string(binding.UID), string(mode)}
	switch mode {
	case gatewayv1alpha1.GatewaySessionEphemeral:
		parts = append(parts, event.ExternalEventID)
	case gatewayv1alpha1.GatewaySessionContext:
		parts = append(parts, event.AccountID, event.ContextID)
	case gatewayv1alpha1.GatewaySessionThread:
		if event.ThreadID == "" {
			return "", fmt.Errorf("thread session mode requires threadId")
		}
		parts = append(parts, event.AccountID, event.ContextID, event.ThreadID)
	case gatewayv1alpha1.GatewaySessionSender:
		parts = append(parts, event.AccountID, event.Sender.ID)
	case gatewayv1alpha1.GatewaySessionContextSender:
		parts = append(parts, event.AccountID, event.ContextID, event.Sender.ID)
	case gatewayv1alpha1.GatewaySessionThreadSender:
		if event.ThreadID == "" {
			return "", fmt.Errorf("thread-sender session mode requires threadId")
		}
		parts = append(parts, event.AccountID, event.ContextID, event.ThreadID, event.Sender.ID)
	case gatewayv1alpha1.GatewaySessionExplicit:
		name := strings.TrimSpace(binding.Spec.Session.Name)
		if name == "" {
			return "", fmt.Errorf("explicit session mode requires session.name")
		}
		return name, nil
	default:
		return "", fmt.Errorf("unsupported session mode %q", mode)
	}
	return stableID("gateway-session", parts...), nil
}

func gatewayTaskCorrelatesWithEvent(task *corev1alpha1.Task, event *store.GatewayEvent) bool {
	if task == nil || event == nil || task.Name != event.TaskName || task.Namespace != event.Namespace ||
		task.Spec.Type != corev1alpha1.TaskTypeAgent || task.Spec.AgentRef == nil || task.Spec.AgentRef.Name != event.AgentName ||
		task.Spec.Prompt != "" || task.Spec.SessionRef == nil || task.Spec.SessionRef.Name != event.SessionName ||
		task.Spec.SessionRef.ThroughMessageID != store.GatewayUserMessageID(event.ID) || !task.Spec.SessionRef.PromptIncluded ||
		task.Spec.RequestedBy == nil || task.Spec.RequestedBy.Subject != event.SenderID ||
		task.Spec.RequestedBy.Issuer != gatewayTaskIssuer(event) || task.Spec.RequestedBy.Username != event.SenderDisplayName ||
		!slices.Equal(task.Spec.RequestedBy.Groups, []string{"gateway:" + event.GatewayName}) ||
		!slices.Equal(task.Spec.RequestedBy.Roles, []string{"gateway-sender"}) {
		return false
	}
	return task.Labels[TaskGatewayNameLabel] == orkalabels.SelectorValue(event.GatewayName) &&
		task.Labels[TaskGatewayBindingLabel] == orkalabels.SelectorValue(event.BindingName) &&
		task.Labels[TaskGatewayEventLabel] == event.ID &&
		task.Annotations[TaskGatewayEventAnnotation] == event.ID &&
		task.Annotations[TaskGatewayExternalEvent] == event.ExternalEventID &&
		task.Annotations[TaskGatewaySession] == event.SessionName &&
		task.Annotations[TaskGatewayNameAnnotation] == event.GatewayName &&
		task.Annotations[TaskGatewayBindingAnnotation] == event.BindingName
}

func bindingMatchesAdmittedEvent(binding *gatewayv1alpha1.GatewayBinding, event *store.GatewayEvent) bool {
	return binding != nil && binding.Status.Ready && binding.Status.ObservedGeneration == binding.Generation &&
		bindingIdentityMatchesAdmittedEvent(binding, event)
}

func bindingIdentityMatchesAdmittedEvent(binding *gatewayv1alpha1.GatewayBinding, event *store.GatewayEvent) bool {
	if binding == nil || event == nil {
		return false
	}
	if event.BindingUID != "" && string(binding.UID) != event.BindingUID {
		return false
	}
	if event.BindingGeneration > 0 && binding.Generation != event.BindingGeneration {
		return false
	}
	match := binding.Spec.Match
	return binding.Spec.GatewayRef.Name == event.GatewayName && binding.Spec.AgentRef.Name == event.AgentName &&
		match.AccountID == event.AccountID && match.ContextID == event.ContextID &&
		(match.ThreadID == "" || match.ThreadID == event.ThreadID) && senderAllowed(binding, event.SenderID)
}

func gatewayTaskIssuer(event *store.GatewayEvent) string {
	return "gateway.orka.ai/" + event.Namespace + "/" + event.NamespaceUID + "/" + event.GatewayName + "/" + event.GatewayUID
}

func gatewayTaskMatchesExpected(
	existing, expected *corev1alpha1.Task,
	event *store.GatewayEvent,
	binding *gatewayv1alpha1.GatewayBinding,
) bool {
	if existing == nil || expected == nil || existing.Spec.Timeout == nil || expected.Spec.Timeout == nil {
		return false
	}
	maxTimeout := defaultGatewayTaskTimeout
	if binding.Spec.TaskDefaults.Timeout != nil {
		maxTimeout = binding.Spec.TaskDefaults.Timeout.Duration
	}
	lifetime := event.ExpiresAt.Sub(event.ReceivedAt)
	if lifetime > 0 && maxTimeout > lifetime {
		maxTimeout = lifetime
	}
	if existing.Spec.Timeout.Duration < expected.Spec.Timeout.Duration || existing.Spec.Timeout.Duration > maxTimeout {
		return false
	}
	existingSpec := existing.Spec.DeepCopy()
	expectedSpec := expected.Spec.DeepCopy()
	existingSpec.Timeout = expectedSpec.Timeout
	if !apiequality.Semantic.DeepEqual(*existingSpec, *expectedSpec) {
		return false
	}
	for key, value := range expected.Labels {
		if existing.Labels[key] != value {
			return false
		}
	}
	for key, value := range expected.Annotations {
		if key == orkalabels.AnnotationTraceParent || key == orkalabels.AnnotationTraceState {
			continue
		}
		if existing.Annotations[key] != value {
			return false
		}
	}
	return true
}

func taskForGatewayEvent(event *store.GatewayEvent, binding *gatewayv1alpha1.GatewayBinding, now time.Time) *corev1alpha1.Task {
	defaults := binding.Spec.TaskDefaults
	var retryPolicy *corev1alpha1.RetryPolicy
	if defaults.RetryPolicy != nil {
		backoffMultiplier := defaults.RetryPolicy.BackoffMultiplier
		if backoffMultiplier == 0 {
			backoffMultiplier = 2
		}
		retryPolicy = &corev1alpha1.RetryPolicy{
			MaxRetries: defaults.RetryPolicy.MaxRetries, BackoffMultiplier: backoffMultiplier,
		}
		if defaults.RetryPolicy.InitialDelay != nil {
			retryPolicy.InitialDelay = defaults.RetryPolicy.InitialDelay.DeepCopy()
		}
	}
	var runtime *corev1alpha1.AgentRuntimeSpec
	if defaults.AgentRuntimeMaxTurns != nil {
		value := *defaults.AgentRuntimeMaxTurns
		runtime = &corev1alpha1.AgentRuntimeSpec{MaxTurns: &value}
	}
	priority := copyInt32(defaults.Priority)
	if priority == nil {
		value := int32(500)
		priority = &value
	}
	startingDeadline := int64(100)
	successfulHistory := int32(3)
	failedHistory := int32(1)
	issuer := gatewayTaskIssuer(event)
	return &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: event.TaskName, Namespace: event.Namespace,
			Labels: map[string]string{
				TaskGatewayNameLabel:    orkalabels.SelectorValue(event.GatewayName),
				TaskGatewayBindingLabel: orkalabels.SelectorValue(event.BindingName),
				TaskGatewayEventLabel:   event.ID,
			},
			Annotations: map[string]string{
				TaskGatewayEventAnnotation: event.ID, TaskGatewayExternalEvent: event.ExternalEventID,
				TaskGatewaySession: event.SessionName, TaskGatewayNameAnnotation: event.GatewayName,
				TaskGatewayBindingAnnotation: event.BindingName,
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, ConcurrencyPolicy: corev1alpha1.ForbidConcurrent,
			StartingDeadlineSeconds: &startingDeadline, SuccessfulRunsHistoryLimit: &successfulHistory,
			FailedRunsHistoryLimit: &failedHistory,
			AgentRef:               &corev1alpha1.AgentReference{Name: binding.Spec.AgentRef.Name},
			Prompt:                 "", Priority: priority, Timeout: gatewayTaskTimeout(event, defaults.Timeout, now),
			RetryPolicy: retryPolicy, AgentRuntime: runtime,
			SessionRef: &corev1alpha1.SessionReference{
				Name: event.SessionName, Create: false, Append: false, MaxMessages: 50,
				ThroughMessageID: "gateway:" + event.ID + ":user", PromptIncluded: true,
			},
			RequestedBy: &corev1alpha1.RequestedBy{
				Issuer: issuer, Subject: event.SenderID, Username: event.SenderDisplayName,
				Groups: []string{"gateway:" + event.GatewayName}, Roles: []string{"gateway-sender"},
			},
		},
	}
}

func (s *Service) ensureDenialDelivery(ctx context.Context, event *store.GatewayEvent, reason string) error {
	if s.DeliveryStore == nil || event == nil {
		return nil
	}
	replyTarget := strings.TrimSpace(event.ReplyTarget)
	if replyTarget == "" {
		replyTarget = strings.TrimSpace(event.ContextID)
	}
	if replyTarget == "" {
		return nil
	}
	now := time.Now().UTC()
	text := "This message could not be accepted by the configured gateway policy."
	if event.State == store.GatewayEventDeadLettered {
		text = "This message could not be queued. Try again later."
	}
	deliveryID := gatewayDeliveryID(event, "denial")
	_, _, err := s.DeliveryStore.CreateGatewayDelivery(ctx, &store.GatewayDelivery{
		ID: deliveryID, IdempotencyID: deliveryID, Namespace: event.Namespace, NamespaceUID: event.NamespaceUID,
		GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
		GatewayName: event.GatewayName, BindingName: event.BindingName, EventID: event.ID,
		Kind: protocol.DeliveryKindError, State: store.GatewayDeliveryPending,
		AccountID: event.AccountID, ContextID: event.ContextID, ThreadID: event.ThreadID,
		ReplyTarget: replyTarget, Text: text, Metadata: map[string]string{"eventId": event.ID},
		TraceParent: event.TraceParent, TraceState: event.TraceState,
		MaxAttempts: s.Config.DeliveryMaxAttempts, NextAttemptAt: now, ExpiresAt: event.ExpiresAt,
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		return err
	}
	_ = reason
	return nil
}

func (s *Service) retryEvent(ctx context.Context, event *store.GatewayEvent, reason string, delay time.Duration) {
	_ = s.EventStore.RetryGatewayEvent(ctx, event.Namespace, event.ID, s.Owner, protocol.SanitizeMessage(reason, 1024), time.Now().UTC().Add(delay))
}

func (s *Service) retryOrDeadLetterDelivery(ctx context.Context, delivery *store.GatewayDelivery, reason string, now time.Time) error {
	reason = protocol.SanitizeMessage(reason, 1024)
	if delivery.AttemptCount >= delivery.MaxAttempts || !now.Add(deliveryBackoff(delivery.AttemptCount)).Before(delivery.ExpiresAt) {
		gatewayDeadLettersTotal.WithLabelValues("delivery").Inc()
		return s.DeliveryStore.MarkGatewayDeliveryTerminal(ctx, delivery.Namespace, delivery.ID, s.Owner, store.GatewayDeliveryDeadLettered, reason, now)
	}
	return s.DeliveryStore.ScheduleGatewayDeliveryRetry(ctx, delivery.Namespace, delivery.ID, s.Owner, reason, now.Add(deliveryBackoff(delivery.AttemptCount)))
}

func (s *Service) markTaskDeliveryCorrelation(ctx context.Context, delivery *store.GatewayDelivery, providerMessageID string) error {
	if delivery == nil || delivery.TaskName == "" || s.Client == nil || s.EventStore == nil {
		return nil
	}
	event, err := s.EventStore.GetGatewayEvent(ctx, delivery.Namespace, delivery.EventID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("resolve delivered gateway Task identity: %w", err)
	}
	if event.TaskName != delivery.TaskName || event.TaskUID == "" {
		return nil
	}
	key := client.ObjectKey{Namespace: delivery.Namespace, Name: delivery.TaskName}
	for attempt := range 2 {
		task := &corev1alpha1.Task{}
		if err := s.freshReader().Get(ctx, key, task); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("read delivered gateway Task correlation: %w", err)
		}
		if string(task.UID) != event.TaskUID {
			return nil
		}
		before := task.DeepCopy()
		if task.Annotations == nil {
			task.Annotations = map[string]string{}
		}
		task.Annotations[TaskGatewayDelivery] = delivery.ID
		if providerMessageID != "" {
			task.Annotations[TaskGatewayProviderMessage] = protocol.SanitizeMessage(providerMessageID, protocol.MaxIdentityBytes)
		}
		patch := client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})
		if err := s.Client.Patch(ctx, task, patch); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			if apierrors.IsConflict(err) {
				if attempt == 0 {
					continue
				}
				latest := &corev1alpha1.Task{}
				if readErr := s.freshReader().Get(ctx, key, latest); readErr != nil {
					if apierrors.IsNotFound(readErr) {
						return nil
					}
					return fmt.Errorf("recheck delivered gateway Task correlation: %w", readErr)
				}
				if string(latest.UID) != event.TaskUID {
					return nil
				}
			}
			return fmt.Errorf("patch delivered gateway Task correlation: %w", err)
		}
		return nil
	}
	return nil
}

func (s *Service) markBindingInbound(ctx context.Context, binding *gatewayv1alpha1.GatewayBinding, now time.Time) {
	if binding == nil {
		return
	}
	latest := &gatewayv1alpha1.GatewayBinding{}
	if err := s.Client.Get(ctx, client.ObjectKeyFromObject(binding), latest); err != nil {
		return
	}
	timestamp := metav1.NewTime(now)
	latest.Status.LastInboundActivity = &timestamp
	_ = s.Client.Status().Update(ctx, latest)
}

func (s *Service) markBindingOutbound(ctx context.Context, namespace, name string, now time.Time) {
	if name == "" {
		return
	}
	binding := &gatewayv1alpha1.GatewayBinding{}
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, binding); err != nil {
		return
	}
	timestamp := metav1.NewTime(now)
	binding.Status.LastOutboundActivity = &timestamp
	_ = s.Client.Status().Update(ctx, binding)
}

// NewAdapterHTTPClient returns an adapter client whose transport matches the endpoint trust model.
func NewAdapterHTTPClient(
	base *http.Client, timeout time.Duration, serviceRef bool, allowInsecureLoopback bool,
) (*http.Client, error) {
	return gatewayHTTPClient(base, timeout, serviceRef, !serviceRef && !allowInsecureLoopback)
}

func gatewayHTTPClient(base *http.Client, timeout time.Duration, disableProxy, publicOnly bool) (*http.Client, error) {
	httpClient := &http.Client{Timeout: timeout}
	if base != nil {
		copy := *base
		httpClient = &copy
		if httpClient.Timeout <= 0 {
			httpClient.Timeout = timeout
		}
	}
	if disableProxy || publicOnly {
		var transport *http.Transport
		switch current := httpClient.Transport.(type) {
		case nil:
			defaultTransport, ok := http.DefaultTransport.(*http.Transport)
			if !ok {
				return nil, fmt.Errorf("default HTTP transport is not configurable")
			}
			transport = defaultTransport.Clone()
		case *http.Transport:
			transport = current.Clone()
		default:
			return nil, fmt.Errorf("custom HTTP transport cannot enforce adapter network policy")
		}
		transport.Proxy = nil
		if publicOnly {
			transport.DialContext = publicGatewayDialContext
		}
		httpClient.Transport = transport
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return httpClient, nil
}

func queueAgeSeconds(now time.Time, timestamp *time.Time) float64 {
	if timestamp == nil || timestamp.IsZero() || timestamp.After(now) {
		return 0
	}
	return now.Sub(*timestamp).Seconds()
}

func normalizeConfig(config Config) Config {
	defaults := DefaultConfig()
	if config.PendingPerSession <= 0 {
		config.PendingPerSession = defaults.PendingPerSession
	}
	if config.MaxRecordsPerGateway <= 0 {
		config.MaxRecordsPerGateway = defaults.MaxRecordsPerGateway
	}
	if config.MaxRejectedRecordsPerGateway <= 0 {
		config.MaxRejectedRecordsPerGateway = defaults.MaxRejectedRecordsPerGateway
	}
	if config.EventExpiry <= 0 {
		config.EventExpiry = defaults.EventExpiry
	}
	if config.TerminalRetention <= 0 {
		config.TerminalRetention = defaults.TerminalRetention
	}
	if config.DeliveryTimeout <= 0 {
		config.DeliveryTimeout = defaults.DeliveryTimeout
	}
	if config.DeliveryMaxAttempts <= 0 {
		config.DeliveryMaxAttempts = defaults.DeliveryMaxAttempts
	}
	if config.ClaimLease <= 0 {
		config.ClaimLease = defaults.ClaimLease
	}
	if config.ClaimLease <= config.DeliveryTimeout {
		config.ClaimLease = 2 * config.DeliveryTimeout
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.BatchSize <= 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.BatchSize > 100 {
		config.BatchSize = 100
	}
	return config
}

func gatewayDeliveryID(event *store.GatewayEvent, kind string) string {
	if event == nil {
		return stableID("gdl", kind)
	}
	incarnation := strings.TrimSpace(event.TaskName)
	if incarnation == "" {
		incarnation = event.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	return stableID("gdl", event.ID, incarnation, kind)
}

func gatewayTaskName(gatewayUID, externalEventID string, admittedAt time.Time) string {
	// The admission instant is the durable incarnation key. It keeps retries deterministic while
	// the event/tombstone exists, but avoids colliding with a deliberately re-admitted event after
	// the configured deduplication retention window has elapsed.
	return stableID("gw", gatewayUID, externalEventID, admittedAt.UTC().Format(time.RFC3339Nano))
}

func stableID(prefix string, parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return prefix + "-" + hex.EncodeToString(hash.Sum(nil))[:40]
}

func newProcessorOwner() string {
	host, _ := os.Hostname()
	var random [8]byte
	_, _ = rand.Read(random[:])
	return protocol.SanitizeMessage(host, 64) + ":" + hex.EncodeToString(random[:])
}

func sanitizedMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	result := make(map[string]string, len(metadata))
	for key, value := range metadata {
		result[key] = protocol.SanitizeMessage(value, protocol.MaxMetadataValueBytes)
	}
	return result
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	clone := make(map[string]string, len(metadata))
	maps.Copy(clone, metadata)
	return clone
}

func boundedTraceValue(value string, limit int) string {
	value = strings.TrimSpace(value)
	if value == "" || (limit > 0 && len(value) > limit) || strings.ContainsFunc(value, func(r rune) bool {
		return r < 0x20 || r == 0x7f
	}) {
		return ""
	}
	return value
}

func gatewayTraceContext(ctx context.Context, traceParent, traceState string) context.Context {
	traceParent = boundedTraceValue(traceParent, 256)
	traceState = boundedTraceValue(traceState, 1024)
	if traceParent == "" {
		return ctx
	}
	return orkatracing.ExtractContext(ctx, orkatracing.MapCarrier{
		"traceparent": traceParent,
		"tracestate":  traceState,
	})
}

func copyInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func gatewayTaskTimeout(event *store.GatewayEvent, configured *metav1.Duration, now time.Time) *metav1.Duration {
	duration := defaultGatewayTaskTimeout
	if configured != nil {
		duration = configured.Duration
	}
	remaining := max(event.ExpiresAt.Sub(now), minimumGatewayExecutionWindow)
	if duration <= 0 || duration > remaining {
		duration = remaining
	}
	return &metav1.Duration{Duration: duration}
}

func gatewayDeliveryExpiresAt(eventExpiresAt, now time.Time, config Config) time.Time {
	extension := max(config.EventExpiry, 2*config.ClaimLease, minimumGatewayDeliveryWindow)
	minimum := now.Add(extension)
	if eventExpiresAt.After(minimum) {
		return eventExpiresAt
	}
	return minimum
}

func boundedText(value string) string {
	if len(value) <= protocol.MaxTextBytes {
		return value
	}
	value = value[:protocol.MaxTextBytes]
	for !utf8.ValidString(value) && len(value) > 0 {
		value = value[:len(value)-1]
	}
	const suffix = "\n\n[response truncated by gateway]"
	if len(value)+len(suffix) > protocol.MaxTextBytes {
		value = value[:protocol.MaxTextBytes-len(suffix)]
		for !utf8.ValidString(value) && len(value) > 0 {
			value = value[:len(value)-1]
		}
	}
	return value + suffix
}

func terminalErrorText(phase corev1alpha1.TaskPhase, statusMessage string) string {
	if phase == corev1alpha1.TaskPhaseCancelled {
		return "The task was cancelled before a response was produced."
	}
	normalized := strings.ToLower(strings.TrimSpace(statusMessage))
	switch {
	case strings.Contains(normalized, "requires orka_allow_bash=true"),
		strings.Contains(normalized, "runtime configuration is invalid"):
		return "The AI agent is temporarily misconfigured. An operator needs to correct it."
	case strings.Contains(normalized, "insufficient_quota"),
		strings.Contains(normalized, "current quota"),
		strings.Contains(normalized, "quota exhausted"),
		strings.Contains(normalized, "billing"),
		strings.Contains(normalized, "monthly spend"),
		strings.Contains(normalized, "spending limit"):
		return "The AI service quota is unavailable. An operator needs to restore capacity."
	case strings.Contains(normalized, "timed out"),
		strings.Contains(normalized, "timeout"),
		strings.Contains(normalized, "deadline exceeded"):
		return "The task timed out before a response was produced. Please try again."
	case strings.Contains(normalized, "rate limit"),
		strings.Contains(normalized, "rate_limit_exceeded"),
		strings.Contains(normalized, "requests per minute"),
		strings.Contains(normalized, "tokens per minute"),
		strings.Contains(normalized, "retry-after"):
		return "The AI service is temporarily rate-limited. Please try again shortly."
	case strings.Contains(normalized, "context length"),
		strings.Contains(normalized, "context window"),
		strings.Contains(normalized, "maximum context"),
		strings.Contains(normalized, "context overflow"),
		strings.Contains(normalized, "token limit"):
		return "The conversation is too long for the configured model. Please shorten the request and try again."
	}
	return "The task could not be completed."
}

func isTerminalTaskPhase(phase corev1alpha1.TaskPhase) bool {
	return phase == corev1alpha1.TaskPhaseSucceeded || phase == corev1alpha1.TaskPhaseFailed || phase == corev1alpha1.TaskPhaseCancelled
}

func eventBackoff(attempt int) time.Duration {
	return cappedBackoff(attempt, time.Second, 5*time.Minute)
}

func deliveryBackoff(attempt int) time.Duration {
	return cappedBackoff(attempt, time.Second, 5*time.Minute)
}

func cappedBackoff(attempt int, initial, maximum time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := initial
	for i := 1; i < attempt && delay < maximum; i++ {
		delay *= 2
		if delay > maximum {
			delay = maximum
		}
	}
	return delay
}
