/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tracing

import "go.opentelemetry.io/otel/attribute"

const (
	AttrTaskID              = "orka.task.id"
	AttrTaskNamespace       = "orka.task.namespace"
	AttrAgentName           = "orka.agent.name"
	AttrTenant              = "orka.tenant"
	AttrUserSub             = "orka.user.sub"
	AttrParentTaskID        = "orka.parent_task.id"
	AttrChildTaskID         = "orka.child_task.id"
	AttrToolName            = "orka.tool.name"
	AttrToolKind            = "orka.tool.kind"
	AttrToolResultSizeBytes = "orka.tool.result.size_bytes"
)

const (
	ToolKindBuiltin  = "builtin"
	ToolKindHTTP     = "http"
	ToolKindDelegate = "delegate"
)

// TaskAttributes returns safe Orka-native task attributes for spans.
// Empty optional values are omitted. The values must be stable metadata only;
// callers must never pass prompts, completions, tokens, credentials, or result bodies.
func TaskAttributes(taskName, namespace, tenant, agentName, userSub string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 5)
	if taskName != "" {
		attrs = append(attrs, attribute.String(AttrTaskID, taskName))
	}
	if namespace != "" {
		attrs = append(attrs, attribute.String(AttrTaskNamespace, namespace))
	}
	if tenant == "" {
		tenant = namespace
	}
	if tenant != "" {
		attrs = append(attrs, attribute.String(AttrTenant, tenant))
	}
	if agentName != "" {
		attrs = append(attrs, attribute.String(AttrAgentName, agentName))
	}
	if userSub != "" {
		attrs = append(attrs, attribute.String(AttrUserSub, userSub))
	}
	return attrs
}

// DelegateAttributes returns safe parent/child task relationship attributes.
func DelegateAttributes(parentTaskID, childTaskID string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 2)
	if parentTaskID != "" {
		attrs = append(attrs, attribute.String(AttrParentTaskID, parentTaskID))
	}
	if childTaskID != "" {
		attrs = append(attrs, attribute.String(AttrChildTaskID, childTaskID))
	}
	return attrs
}
