/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package metrics

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// API metrics
	APIRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_api_requests_total",
			Help: "Total API requests by endpoint, method, and status",
		},
		[]string{"endpoint", "method", "status"},
	)

	APIRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "orka_api_request_duration_seconds",
			Help:    "API request latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"endpoint", "method"},
	)

	// Skill metrics
	SkillsLoaded = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_skills_loaded_total",
			Help: "Skills loaded by namespace and name",
		},
		[]string{"skill", "namespace"},
	)

	// Context-token metrics
	ContextTokenAuthTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_context_token_auth_total",
			Help: "Total context-token authentication attempts by profile and result",
		},
		[]string{"profile", "result"},
	)

	ContextTokenAuthorizationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_context_token_authorization_total",
			Help: "Total context-token authorization decisions by action, result, and low-cardinality reason",
		},
		[]string{"action", "result", "reason"},
	)

	ContextTokenTTSExchangeTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_context_token_tts_exchange_total",
			Help: "Total context-token TTS exchange attempts by result and low-cardinality reason",
		},
		[]string{"result", "reason"},
	)

	ContextTokenTTSExchangeDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "orka_context_token_tts_exchange_duration_seconds",
			Help:    "Context-token TTS exchange latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"result", "reason"},
	)

	TokenExchangeTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_token_exchange_total",
			Help: "Total OAuth token exchanges by adapter, grant class, result, and low-cardinality reason",
		},
		[]string{"adapter", "grant_class", "result", "reason"},
	)

	TokenExchangeDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "orka_token_exchange_duration_seconds",
			Help:    "OAuth token exchange latency by adapter, grant class, result, and low-cardinality reason",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"adapter", "grant_class", "result", "reason"},
	)

	// ACP RuntimePool metrics mirror authoritative controller status. Pool name
	// and namespace are Kubernetes object identity labels, never Task/session IDs.
	// ACPWorkspaceRetentionActionsTotal counts retention enforcement actions on
	// class-backed ACP execution workspaces. Labels stay bounded: no object
	// names, class names, or session identifiers.
	ACPWorkspaceRetentionActionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_acp_workspace_retention_actions_total",
			Help: "Retention enforcement actions applied to class-backed ACP execution workspaces",
		},
		[]string{"action", "reason"},
	)

	ACPRuntimePoolDesiredReplicas = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "orka_acp_runtime_pool_desired_replicas",
			Help: "Desired replicas for a controller-owned ACP RuntimePool",
		},
		[]string{"namespace", "runtime_pool"},
	)

	ACPRuntimePoolReadyReplicas = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "orka_acp_runtime_pool_ready_replicas",
			Help: "Authoritatively selected Ready replicas for a controller-owned ACP RuntimePool",
		},
		[]string{"namespace", "runtime_pool"},
	)

	ACPRuntimePoolSessionsActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "orka_acp_runtime_pool_sessions_active",
			Help: "Authenticated supervisor count of resident RuntimeSessions",
		},
		[]string{"namespace", "runtime_pool"},
	)

	ACPRuntimePoolPromptsInFlight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "orka_acp_runtime_pool_prompts_in_flight",
			Help: "Authenticated supervisor count of active prompts",
		},
		[]string{"namespace", "runtime_pool"},
	)

	ACPRuntimePoolQueuedTasks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "orka_acp_runtime_pool_queued_tasks",
			Help: "Durable unsatisfied Task demand assigned to an ACP RuntimePool",
		},
		[]string{"namespace", "runtime_pool"},
	)

	ACPRuntimePoolAdmissionState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "orka_acp_runtime_pool_admission_state",
			Help: "Authoritative RuntimePool admission state as a one-hot gauge",
		},
		[]string{"namespace", "runtime_pool", "state"},
	)

	ACPRuntimePoolScaleToZeroTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_acp_runtime_pool_scale_to_zero_total",
			Help: "Completed RuntimePool scale-to-zero transitions",
		},
		[]string{"namespace", "runtime_pool"},
	)

	// Repository monitor workflow metrics. Labels are low-cardinality intent/action/status values.
	RepositoryMonitorCommandsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_repository_monitor_commands_total",
			Help: "Repository monitor command events by intent and status",
		},
		[]string{"intent", "status"},
	)

	RepositoryMonitorWorkActionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_repository_monitor_work_actions_total",
			Help: "Repository monitor workflow actions by desired action and status",
		},
		[]string{"desired_action", "status"},
	)

	RepositoryMonitorGitHubMutationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_repository_monitor_github_mutations_total",
			Help: "Repository monitor controller-owned GitHub mutations by operation and status",
		},
		[]string{"operation", "status"},
	)

	RepositoryMonitorBlocksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_repository_monitor_blocks_total",
			Help: "Repository monitor policy, stale snapshot, and rate-limit blocks by reason",
		},
		[]string{"reason"},
	)

	// Execution event metrics. Labels intentionally exclude task/session IDs.
	ExecutionEventsAppendedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_execution_events_appended_total",
			Help: "Total execution events appended by stream type and event type",
		},
		[]string{"stream_type", "event_type"},
	)

	ExecutionEventAppendFailuresTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_execution_event_append_failures_total",
			Help: "Total execution event append failures by stream type and event type",
		},
		[]string{"stream_type", "event_type"},
	)

	ExecutionEventAppendDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "orka_execution_event_append_duration_seconds",
			Help:    "Execution event append latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"stream_type", "event_type", "result"},
	)

	ExecutionEventListRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_execution_event_list_requests_total",
			Help: "Total execution event list/read-model requests by scope and result",
		},
		[]string{"scope", "result"},
	)

	ExecutionEventListDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "orka_execution_event_list_duration_seconds",
			Help:    "Execution event list/read-model latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"scope", "result"},
	)

	ExecutionEventStreamConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "orka_execution_event_stream_connections_current",
			Help: "Current execution event SSE stream connections by scope",
		},
		[]string{"scope"},
	)

	ExecutionEventStreamReconnectsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_execution_event_stream_reconnects_total",
			Help: "Total execution event SSE reconnects detected by after cursor by scope",
		},
		[]string{"scope"},
	)

	ExecutionEventStreamErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_execution_event_stream_errors_total",
			Help: "Total execution event SSE stream errors by scope and low-cardinality reason",
		},
		[]string{"scope", "reason"},
	)

	ExecutionEventRedactionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_execution_event_redactions_total",
			Help: "Total execution events whose payloads contained redacted sensitive values by stream type and event type",
		},
		[]string{"stream_type", "event_type"},
	)

	ExecutionEventTruncationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orka_execution_event_truncations_total",
			Help: "Total execution events whose payloads were truncated by stream type and event type",
		},
		[]string{"stream_type", "event_type"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		ACPWorkspaceRetentionActionsTotal,
		APIRequestsTotal,
		APIRequestDuration,
		SkillsLoaded,
		ContextTokenAuthTotal,
		ContextTokenAuthorizationTotal,
		ContextTokenTTSExchangeTotal,
		ContextTokenTTSExchangeDuration,
		TokenExchangeTotal,
		TokenExchangeDuration,
		ACPRuntimePoolDesiredReplicas,
		ACPRuntimePoolReadyReplicas,
		ACPRuntimePoolSessionsActive,
		ACPRuntimePoolPromptsInFlight,
		ACPRuntimePoolQueuedTasks,
		ACPRuntimePoolAdmissionState,
		ACPRuntimePoolScaleToZeroTotal,
		RepositoryMonitorCommandsTotal,
		RepositoryMonitorWorkActionsTotal,
		RepositoryMonitorGitHubMutationsTotal,
		RepositoryMonitorBlocksTotal,
		ExecutionEventsAppendedTotal,
		ExecutionEventAppendFailuresTotal,
		ExecutionEventAppendDuration,
		ExecutionEventListRequestsTotal,
		ExecutionEventListDuration,
		ExecutionEventStreamConnections,
		ExecutionEventStreamReconnectsTotal,
		ExecutionEventStreamErrorsTotal,
		ExecutionEventRedactionsTotal,
		ExecutionEventTruncationsTotal,
	)
}

// RecordAPIRequest records an API request
func RecordAPIRequest(endpoint, method string, status int, durationSeconds float64) {
	statusStr := "2xx"
	if status >= 400 && status < 500 {
		statusStr = "4xx"
	} else if status >= 500 {
		statusStr = "5xx"
	}
	APIRequestsTotal.WithLabelValues(endpoint, method, statusStr).Inc()
	APIRequestDuration.WithLabelValues(endpoint, method).Observe(durationSeconds)
}

// RecordContextTokenAuth records a context-token authentication attempt.
func RecordContextTokenAuth(profile, result string) {
	ContextTokenAuthTotal.WithLabelValues(normalizeMetricLabel(profile), normalizeMetricLabel(result)).Inc()
}

// RecordContextTokenAuthorization records a context-token authorization decision.
func RecordContextTokenAuthorization(action, result, reason string) {
	ContextTokenAuthorizationTotal.WithLabelValues(
		normalizeMetricLabel(action),
		normalizeMetricLabel(result),
		normalizeMetricLabel(reason),
	).Inc()
}

// RecordContextTokenTTSExchange records a transaction-token TTS exchange attempt.
func RecordContextTokenTTSExchange(result, reason string, durationSeconds float64) {
	result = normalizeMetricLabel(result)
	reason = normalizeMetricLabel(reason)
	ContextTokenTTSExchangeTotal.WithLabelValues(result, reason).Inc()
	ContextTokenTTSExchangeDuration.WithLabelValues(result, reason).Observe(durationSeconds)
}

// RecordTokenExchange records one low-cardinality OAuth exchange observation.
func RecordTokenExchange(adapter, grantClass, result, reason string, durationSeconds float64) {
	adapter = normalizeMetricLabel(adapter)
	grantClass = normalizeMetricLabel(grantClass)
	result = normalizeMetricLabel(result)
	reason = normalizeMetricLabel(reason)
	TokenExchangeTotal.WithLabelValues(adapter, grantClass, result, reason).Inc()
	TokenExchangeDuration.WithLabelValues(adapter, grantClass, result, reason).Observe(durationSeconds)
}

var acpRuntimePoolAdmissionStates = [...]string{"unknown", "closed", "accepting", "draining", "ambiguous"}

// RecordACPRuntimePoolStatus publishes one authoritative RuntimePool status
// snapshot. Admission state is one-hot so state changes cannot leave a stale 1.
func RecordACPRuntimePoolStatus(
	namespace, runtimePool string,
	desiredReplicas, readyReplicas, sessionsActive, promptsInFlight, queuedTasks int32,
	admissionState string,
) {
	namespace = normalizeMetricLabel(namespace)
	runtimePool = normalizeMetricLabel(runtimePool)
	ACPRuntimePoolDesiredReplicas.WithLabelValues(namespace, runtimePool).Set(float64(desiredReplicas))
	ACPRuntimePoolReadyReplicas.WithLabelValues(namespace, runtimePool).Set(float64(readyReplicas))
	ACPRuntimePoolSessionsActive.WithLabelValues(namespace, runtimePool).Set(float64(sessionsActive))
	ACPRuntimePoolPromptsInFlight.WithLabelValues(namespace, runtimePool).Set(float64(promptsInFlight))
	ACPRuntimePoolQueuedTasks.WithLabelValues(namespace, runtimePool).Set(float64(queuedTasks))
	admissionState = normalizeACPRuntimePoolAdmissionState(admissionState)
	for _, state := range acpRuntimePoolAdmissionStates {
		value := 0.0
		if state == admissionState {
			value = 1
		}
		ACPRuntimePoolAdmissionState.WithLabelValues(namespace, runtimePool, state).Set(value)
	}
}

// RecordACPRuntimePoolScaleToZero records one completed scale-to-zero transition.
func RecordACPRuntimePoolScaleToZero(namespace, runtimePool string) {
	ACPRuntimePoolScaleToZeroTotal.WithLabelValues(
		normalizeMetricLabel(namespace), normalizeMetricLabel(runtimePool),
	).Inc()
}

// DeleteACPRuntimePool removes stale metric series after a RuntimePool is deleted.
func DeleteACPRuntimePool(namespace, runtimePool string) {
	namespace = normalizeMetricLabel(namespace)
	runtimePool = normalizeMetricLabel(runtimePool)
	ACPRuntimePoolDesiredReplicas.DeleteLabelValues(namespace, runtimePool)
	ACPRuntimePoolReadyReplicas.DeleteLabelValues(namespace, runtimePool)
	ACPRuntimePoolSessionsActive.DeleteLabelValues(namespace, runtimePool)
	ACPRuntimePoolPromptsInFlight.DeleteLabelValues(namespace, runtimePool)
	ACPRuntimePoolQueuedTasks.DeleteLabelValues(namespace, runtimePool)
	for _, state := range acpRuntimePoolAdmissionStates {
		ACPRuntimePoolAdmissionState.DeleteLabelValues(namespace, runtimePool, state)
	}
	ACPRuntimePoolScaleToZeroTotal.DeleteLabelValues(namespace, runtimePool)
}

func normalizeACPRuntimePoolAdmissionState(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "closed":
		return "closed"
	case "accepting":
		return "accepting"
	case "draining":
		return "draining"
	case "ambiguous":
		return "ambiguous"
	default:
		return "unknown"
	}
}

// RecordExecutionEventAppend records append success/failure and latency using low-cardinality labels.
func RecordExecutionEventAppend(streamType, eventType string, success bool, durationSeconds float64) {
	streamType = normalizeMetricLabel(streamType)
	eventType = normalizeMetricLabel(eventType)
	result := "success"
	if success {
		ExecutionEventsAppendedTotal.WithLabelValues(streamType, eventType).Inc()
	} else {
		result = "error"
		ExecutionEventAppendFailuresTotal.WithLabelValues(streamType, eventType).Inc()
	}
	ExecutionEventAppendDuration.WithLabelValues(streamType, eventType, result).Observe(durationSeconds)
}

// RecordExecutionEventList records list/read-model request count and latency.
func RecordExecutionEventList(scope string, success bool, durationSeconds float64) {
	scope = normalizeMetricLabel(scope)
	result := "success"
	if !success {
		result = "error"
	}
	ExecutionEventListRequestsTotal.WithLabelValues(scope, result).Inc()
	ExecutionEventListDuration.WithLabelValues(scope, result).Observe(durationSeconds)
}

// RecordExecutionEventStreamOpen records stream lifecycle and reconnect detection.
func RecordExecutionEventStreamOpen(scope string, reconnect bool) func() {
	scope = normalizeMetricLabel(scope)
	ExecutionEventStreamConnections.WithLabelValues(scope).Inc()
	if reconnect {
		ExecutionEventStreamReconnectsTotal.WithLabelValues(scope).Inc()
	}
	return func() { ExecutionEventStreamConnections.WithLabelValues(scope).Dec() }
}

// RecordExecutionEventStreamError records a low-cardinality stream failure reason.
func RecordExecutionEventStreamError(scope, reason string) {
	ExecutionEventStreamErrorsTotal.WithLabelValues(normalizeMetricLabel(scope), normalizeMetricLabel(reason)).Inc()
}

// RecordExecutionEventPayloadSanitization records event-level redaction/truncation signals.
func RecordExecutionEventPayloadSanitization(streamType, eventType string, redacted, truncated bool) {
	streamType = normalizeMetricLabel(streamType)
	eventType = normalizeMetricLabel(eventType)
	if redacted {
		ExecutionEventRedactionsTotal.WithLabelValues(streamType, eventType).Inc()
	}
	if truncated {
		ExecutionEventTruncationsTotal.WithLabelValues(streamType, eventType).Inc()
	}
}

// CounterVecValue returns the current value of a CounterVec for the given label
// values. It is intended for tests asserting metric accuracy across packages.
func CounterVecValue(counter *prometheus.CounterVec, labels ...string) float64 {
	var m dto.Metric
	if err := counter.WithLabelValues(labels...).Write(&m); err != nil {
		return 0
	}
	return m.GetCounter().GetValue()
}

// RecordRepositoryMonitorCommand records a durable command event decision.
func RecordRepositoryMonitorCommand(intent, status string) {
	RepositoryMonitorCommandsTotal.WithLabelValues(normalizeMetricLabel(intent), normalizeMetricLabel(status)).Inc()
}

// RecordRepositoryMonitorWorkAction records a workflow action transition.
func RecordRepositoryMonitorWorkAction(desiredAction, status string) {
	RepositoryMonitorWorkActionsTotal.WithLabelValues(normalizeMetricLabel(desiredAction), normalizeMetricLabel(status)).Inc()
}

// RecordRepositoryMonitorGitHubMutation records one GitHub write audit result.
func RecordRepositoryMonitorGitHubMutation(operation, status string) {
	RepositoryMonitorGitHubMutationsTotal.WithLabelValues(normalizeMetricLabel(operation), normalizeMetricLabel(status)).Inc()
}

// RecordRepositoryMonitorBlock records a low-cardinality monitor block reason.
func RecordRepositoryMonitorBlock(reason string) {
	RepositoryMonitorBlocksTotal.WithLabelValues(normalizeMetricLabel(reason)).Inc()
}

// RecordACPWorkspaceRetentionAction records one retention enforcement action
// (suspend or delete) with its bounded reason.
func RecordACPWorkspaceRetentionAction(action, reason string) {
	ACPWorkspaceRetentionActionsTotal.WithLabelValues(normalizeMetricLabel(action), normalizeMetricLabel(reason)).Inc()
}

func normalizeMetricLabel(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}
