/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tracing

import (
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestTaskAttributesOmitEmptyFieldsAndUseTenantFallback(t *testing.T) {
	attrs := TaskAttributes("task-a", "team-a", "", "agent-a", "")
	got := attrMap(attrs)

	if got[AttrTaskID].AsString() != "task-a" {
		t.Fatalf("%s = %q", AttrTaskID, got[AttrTaskID].AsString())
	}
	if got[AttrTaskNamespace].AsString() != "team-a" {
		t.Fatalf("%s = %q", AttrTaskNamespace, got[AttrTaskNamespace].AsString())
	}
	if got[AttrTenant].AsString() != "team-a" {
		t.Fatalf("%s = %q, want namespace fallback", AttrTenant, got[AttrTenant].AsString())
	}
	if got[AttrAgentName].AsString() != "agent-a" {
		t.Fatalf("%s = %q", AttrAgentName, got[AttrAgentName].AsString())
	}
	if _, ok := got[AttrUserSub]; ok {
		t.Fatalf("%s was emitted for empty user subject", AttrUserSub)
	}
}

func TestDelegateAttributesOmitEmptyValues(t *testing.T) {
	attrs := DelegateAttributes("parent-a", "")
	got := attrMap(attrs)
	if got[AttrParentTaskID].AsString() != "parent-a" {
		t.Fatalf("%s = %q", AttrParentTaskID, got[AttrParentTaskID].AsString())
	}
	if _, ok := got[AttrChildTaskID]; ok {
		t.Fatalf("%s was emitted for empty child id", AttrChildTaskID)
	}
}

func attrMap(attrs []attribute.KeyValue) map[string]attribute.Value {
	out := make(map[string]attribute.Value, len(attrs))
	for _, kv := range attrs {
		out[string(kv.Key)] = kv.Value
	}
	return out
}
