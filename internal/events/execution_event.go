package events

import "strings"

const (
	ExecutionEventTypeTaskCreated                   = "TaskCreated"
	ExecutionEventTypeTaskPhaseChanged              = "TaskPhaseChanged"
	ExecutionEventTypeTaskJobCreated                = "TaskJobCreated"
	ExecutionEventTypeTaskStarted                   = "TaskStarted"
	ExecutionEventTypeTaskSucceeded                 = "TaskSucceeded"
	ExecutionEventTypeTaskFailed                    = "TaskFailed"
	ExecutionEventTypeTaskCancelled                 = "TaskCancelled"
	ExecutionEventTypeWorkerStarted                 = "WorkerStarted"
	ExecutionEventTypeWorkerCompleted               = "WorkerCompleted"
	ExecutionEventTypeWorkerFailed                  = "WorkerFailed"
	ExecutionEventTypeModelRequestStarted           = "ModelRequestStarted"
	ExecutionEventTypeModelRequestCompleted         = "ModelRequestCompleted"
	ExecutionEventTypeModelRequestFailed            = "ModelRequestFailed"
	ExecutionEventTypeModelUsageUpdated             = "ModelUsageUpdated"
	ExecutionEventTypeModelContextUpdated           = "ModelContextUpdated"
	ExecutionEventTypeModelMessage                  = "ModelMessage"
	ExecutionEventTypeContextTruncated              = "ContextTruncated"
	ExecutionEventTypeToolCallStarted               = "ToolCallStarted"
	ExecutionEventTypeToolCallCompleted             = "ToolCallCompleted"
	ExecutionEventTypeToolCallFailed                = "ToolCallFailed"
	ExecutionEventTypeWorkspacePreparationStarted   = "WorkspacePreparationStarted"
	ExecutionEventTypeWorkspacePreparationCompleted = "WorkspacePreparationCompleted"
	ExecutionEventTypeWorkspacePreparationFailed    = "WorkspacePreparationFailed"
	ExecutionEventTypeAgentRuntimeStarted           = "AgentRuntimeStarted"
	ExecutionEventTypeAgentRuntimeCommandStarted    = "AgentRuntimeCommandStarted"
	ExecutionEventTypeAgentRuntimeCompleted         = "AgentRuntimeCompleted"
	ExecutionEventTypeAgentRuntimeFailed            = "AgentRuntimeFailed"
	ExecutionEventTypeAgentRuntimeCancelled         = "AgentRuntimeCancelled"
	ExecutionEventTypeResultSubmitted               = "ResultSubmitted"
	ExecutionEventTypeGatewayDeliveryCompleted      = "GatewayDeliveryCompleted"
	ExecutionEventTypeArtifactUploadCompleted       = "ArtifactUploadCompleted"
	ExecutionEventTypeArtifactUploadFailed          = "ArtifactUploadFailed"
	ExecutionEventTypeTaskForkRequested             = "TaskForkRequested"
	ExecutionEventTypeTaskForkCreated               = "TaskForkCreated"
	ExecutionEventTypeApprovalRequested             = "ApprovalRequested"
	ExecutionEventTypeApprovalApproved              = "ApprovalApproved"
	ExecutionEventTypeApprovalDeclined              = "ApprovalDeclined"
	ExecutionEventTypeApprovalExpired               = "ApprovalExpired"
	ExecutionEventTypeApprovalCancelled             = "ApprovalCancelled"
	ExecutionEventTypePlanUpdated                   = "PlanUpdated"
)

const (
	ExecutionEventSeverityDebug   = "debug"
	ExecutionEventSeverityInfo    = "info"
	ExecutionEventSeverityWarning = "warning"
	ExecutionEventSeverityError   = "error"
)

const (
	// ExecutionEventStreamTypeTask is the persisted per-task stream type.
	ExecutionEventStreamTypeTask = "task"
	// ExecutionEventStreamTypeSession names the public aggregated session stream read model.
	ExecutionEventStreamTypeSession = "session"
)

var executionEventTypes = []string{
	ExecutionEventTypeTaskCreated,
	ExecutionEventTypeTaskPhaseChanged,
	ExecutionEventTypeTaskJobCreated,
	ExecutionEventTypeTaskStarted,
	ExecutionEventTypeTaskSucceeded,
	ExecutionEventTypeTaskFailed,
	ExecutionEventTypeTaskCancelled,
	ExecutionEventTypeWorkerStarted,
	ExecutionEventTypeWorkerCompleted,
	ExecutionEventTypeWorkerFailed,
	ExecutionEventTypeModelRequestStarted,
	ExecutionEventTypeModelRequestCompleted,
	ExecutionEventTypeModelRequestFailed,
	ExecutionEventTypeModelUsageUpdated,
	ExecutionEventTypeModelContextUpdated,
	ExecutionEventTypeModelMessage,
	ExecutionEventTypeContextTruncated,
	ExecutionEventTypeToolCallStarted,
	ExecutionEventTypeToolCallCompleted,
	ExecutionEventTypeToolCallFailed,
	ExecutionEventTypeWorkspacePreparationStarted,
	ExecutionEventTypeWorkspacePreparationCompleted,
	ExecutionEventTypeWorkspacePreparationFailed,
	ExecutionEventTypeAgentRuntimeStarted,
	ExecutionEventTypeAgentRuntimeCommandStarted,
	ExecutionEventTypeAgentRuntimeCompleted,
	ExecutionEventTypeAgentRuntimeFailed,
	ExecutionEventTypeAgentRuntimeCancelled,
	ExecutionEventTypeResultSubmitted,
	ExecutionEventTypeGatewayDeliveryCompleted,
	ExecutionEventTypeArtifactUploadCompleted,
	ExecutionEventTypeArtifactUploadFailed,
	ExecutionEventTypeTaskForkRequested,
	ExecutionEventTypeTaskForkCreated,
	ExecutionEventTypeApprovalRequested,
	ExecutionEventTypeApprovalApproved,
	ExecutionEventTypeApprovalDeclined,
	ExecutionEventTypeApprovalExpired,
	ExecutionEventTypeApprovalCancelled,
	ExecutionEventTypePlanUpdated,
}

var validExecutionEventTypes = stringSet(executionEventTypes)

var terminalTaskEventTypes = []string{
	ExecutionEventTypeTaskSucceeded,
	ExecutionEventTypeTaskFailed,
	ExecutionEventTypeTaskCancelled,
}

var terminalTaskEventTypeSet = stringSet(terminalTaskEventTypes)

var terminalApprovalEventTypes = []string{
	ExecutionEventTypeApprovalApproved,
	ExecutionEventTypeApprovalDeclined,
	ExecutionEventTypeApprovalExpired,
	ExecutionEventTypeApprovalCancelled,
}

var terminalApprovalEventTypeSet = stringSet(terminalApprovalEventTypes)

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

// TerminalTaskEventTypes returns task-level terminal events that complete a task
// execution stream.
func TerminalTaskEventTypes() []string {
	return append([]string(nil), terminalTaskEventTypes...)
}

// IsTerminalTaskEventType reports whether value is a task-level terminal event.
func IsTerminalTaskEventType(value string) bool {
	_, ok := terminalTaskEventTypeSet[strings.TrimSpace(value)]
	return ok
}

// IsTerminalApprovalEventType reports whether value is an approval terminal event.
func IsTerminalApprovalEventType(value string) bool {
	_, ok := terminalApprovalEventTypeSet[strings.TrimSpace(value)]
	return ok
}

// IsValidExecutionEventType reports whether value is a known execution event type.
func IsValidExecutionEventType(value string) bool {
	_, ok := validExecutionEventTypes[strings.TrimSpace(value)]
	return ok
}

// NormalizeExecutionEventType trims a known event type. Unknown values normalize to empty.
func NormalizeExecutionEventType(value string) string {
	value = strings.TrimSpace(value)
	if !IsValidExecutionEventType(value) {
		return ""
	}
	return value
}

// IsValidExecutionEventSeverity reports whether value is a known severity after normalization.
func IsValidExecutionEventSeverity(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case ExecutionEventSeverityDebug, ExecutionEventSeverityInfo, ExecutionEventSeverityWarning, ExecutionEventSeverityError:
		return true
	default:
		return false
	}
}

// NormalizeExecutionEventSeverity normalizes severity to lowercase.
// Empty or unsupported values intentionally coerce to info so event producers can be permissive
// while stores and APIs persist a stable severity value.
func NormalizeExecutionEventSeverity(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if !IsValidExecutionEventSeverity(value) {
		return ExecutionEventSeverityInfo
	}
	return value
}

// IsValidExecutionEventStreamType reports whether value is a supported persisted stream type.
// Session timelines are an aggregated read model over task streams, not a direct P1 append target.
func IsValidExecutionEventStreamType(value string) bool {
	return strings.TrimSpace(value) == ExecutionEventStreamTypeTask
}
