package eventjournal

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	executionevents "github.com/orka-agents/orka/internal/events"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
)

const (
	mapperTestToolCallID    = "call-1"
	mapperTestToolKind      = "file_read"
	mapperTestDone          = "done"
	mapperTestNamespace     = "default"
	mapperTestTaskName      = "task-1"
	mapperTestSessionName   = "session-1"
	mapperTestAgentName     = "agent-1"
	mapperTestToolKindShell = "shell"
	mapperTestProvider      = "openai"
	mapperTestSecretPrefix  = "sk-"
	mapperTestServedModel   = "served-model"
	mapperTestPromptID      = "prompt-1"
)

func TestMapUpdateMapsACPUpdateKinds(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	mapCtx := testMapContext()
	tests := []struct {
		name         string
		update       harnessv2.UpdateEvent
		wantType     string
		wantSeverity string
		wantToolName string
		wantToolID   string
	}{
		{
			name: "assistant message",
			update: harnessv2.UpdateEvent{Kind: harnessv2.UpdateAssistantMessageChunk,
				AssistantMessage: &harnessv2.AssistantMessageChunk{Text: "hello from the agent"}},
			wantType: executionevents.ExecutionEventTypeModelMessage, wantSeverity: executionevents.ExecutionEventSeverityInfo,
		},
		{
			name: "tool started",
			update: harnessv2.UpdateEvent{Kind: harnessv2.UpdateToolCall, ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: mapperTestToolCallID, Title: "Read the repository", Kind: mapperTestToolKind, Status: harnessv2.ToolCallStatusPending,
				Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: "README.md"}},
			}},
			wantType: executionevents.ExecutionEventTypeToolCallStarted, wantSeverity: executionevents.ExecutionEventSeverityInfo,
			wantToolName: mapperTestToolKind, wantToolID: safeMappedToolCallID(mapperTestToolCallID),
		},
		{
			name: "tool completed",
			update: harnessv2.UpdateEvent{Kind: harnessv2.UpdateToolCallUpdate, ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: mapperTestToolCallID, Title: "Read the repository", Kind: mapperTestToolKind, Status: harnessv2.ToolCallStatusCompleted,
			}},
			wantType: executionevents.ExecutionEventTypeToolCallCompleted, wantSeverity: executionevents.ExecutionEventSeverityInfo,
			wantToolName: mapperTestToolKind, wantToolID: safeMappedToolCallID(mapperTestToolCallID),
		},
		{
			name: "tool failed",
			update: harnessv2.UpdateEvent{Kind: harnessv2.UpdateToolCallUpdate, ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: "call-2", Kind: mapperTestToolKindShell, Status: harnessv2.ToolCallStatusFailed,
			}},
			wantType: executionevents.ExecutionEventTypeToolCallFailed, wantSeverity: executionevents.ExecutionEventSeverityError,
			wantToolName: mapperTestToolKindShell, wantToolID: safeMappedToolCallID("call-2"),
		},
		{
			name: "plan",
			update: harnessv2.UpdateEvent{Kind: harnessv2.UpdatePlan, Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{
				{Content: "inspect", Status: harnessv2.PlanEntryCompleted},
				{Content: "implement", Status: harnessv2.PlanEntryInProgress},
			}}},
			wantType: executionevents.ExecutionEventTypePlanUpdated, wantSeverity: executionevents.ExecutionEventSeverityInfo,
		},
		{
			name: "usage",
			update: harnessv2.UpdateEvent{Kind: harnessv2.UpdateUsage,
				Usage: &harnessv2.UsageUpdate{InputTokens: 120, OutputTokens: 30, CachedInputTokens: 40}},
			wantType: executionevents.ExecutionEventTypeModelUsageUpdated, wantSeverity: executionevents.ExecutionEventSeverityInfo,
		},
		{
			name: "retryable diagnostic",
			update: harnessv2.UpdateEvent{Kind: harnessv2.UpdateDiagnostic,
				Diagnostic: &harnessv2.DiagnosticUpdate{Code: "provider_retry", Message: "retrying", Retryable: true}},
			wantType: executionevents.ExecutionEventTypeAgentRuntimeCommandStarted, wantSeverity: executionevents.ExecutionEventSeverityWarning,
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), test.update)
			mapped, err := mapUpdate(event, mapCtx, mapUpdateOptions{})
			if err != nil {
				t.Fatalf("mapUpdate() error = %v", err)
			}
			if mapped.Type != test.wantType || mapped.Severity != test.wantSeverity {
				t.Fatalf("mapped type/severity = %s/%s, want %s/%s", mapped.Type, mapped.Severity, test.wantType, test.wantSeverity)
			}
			if mapped.ToolName != test.wantToolName || mapped.ToolCallID != test.wantToolID {
				t.Fatalf("mapped tool = %q/%q, want %q/%q", mapped.ToolName, mapped.ToolCallID, test.wantToolName, test.wantToolID)
			}
			if mapped.Namespace != mapCtx.Namespace || mapped.StreamID != mapCtx.StreamID || mapped.TaskName != mapCtx.TaskName || mapped.SessionName != mapCtx.SessionName || mapped.AgentName != mapCtx.AgentName {
				t.Fatalf("mapped ownership = %#v", mapped)
			}
			identity, ok := MappedUpdateIdentityFromEvent(*mapped)
			if !ok || identity.Sequence != event.Identity.Sequence || identity.PromptID != event.Identity.PromptID {
				t.Fatalf("mapped identity = %#v, ok=%t", identity, ok)
			}
			if strings.Contains(string(mapped.Content), "requestDigest") || strings.Contains(string(mapped.Content), string(event.Identity.RequestDigest)) {
				t.Fatalf("mapped content exposed request digest: %s", mapped.Content)
			}
		})
	}
}

func TestMapUsagePreservesPromotedTelemetryContent(t *testing.T) {
	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind:  harnessv2.UpdateUsage,
		Usage: &harnessv2.UsageUpdate{InputTokens: 100, OutputTokens: 25, CachedInputTokens: 60},
	})
	mapped, err := mapUpdate(event, testMapContext(), mapUpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]any
	if err := json.Unmarshal(mapped.Content, &content); err != nil {
		t.Fatal(err)
	}
	if content["inputTokens"] != float64(100) || content["outputTokens"] != float64(25) || content["cachedInputTokens"] != float64(60) {
		t.Fatalf("usage content = %#v", content)
	}
	if content["provider"] != mapperTestProvider || content["model"] != "gpt-test" {
		t.Fatalf("model content = %#v", content)
	}
}

func TestMapContextWindowUsageDoesNotMasqueradeAsTokenAccounting(t *testing.T) {
	used, size := uint64(53_000), uint64(200_000)
	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateUsage,
		Usage: &harnessv2.UsageUpdate{
			ContextWindowUsed: &used,
			ContextWindowSize: &size,
		},
	})
	mapped, err := mapUpdate(event, testMapContext(), mapUpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if mapped.Type != executionevents.ExecutionEventTypeModelContextUpdated ||
		!strings.Contains(mapped.Summary, "53000 of 200000") {
		t.Fatalf("mapped context event = %#v", mapped)
	}
	var content map[string]any
	if err := json.Unmarshal(mapped.Content, &content); err != nil {
		t.Fatal(err)
	}
	if content["contextWindowUsed"] != float64(53_000) || content["contextWindowSize"] != float64(200_000) {
		t.Fatalf("context content = %#v", content)
	}
	if _, ok := content["inputTokens"]; ok {
		t.Fatalf("context occupancy exposed as model input tokens: %#v", content)
	}
}

func TestMapZeroUsageSnapshotRemainsTokenTelemetry(t *testing.T) {
	mapped, err := mapUpdate(testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateUsage, Usage: &harnessv2.UsageUpdate{},
	}), testMapContext(), mapUpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if mapped.Type != executionevents.ExecutionEventTypeModelUsageUpdated {
		t.Fatalf("zero usage event type = %q", mapped.Type)
	}
}

func TestMapDiagnosticRedactsCredentialSplitAcrossFields(t *testing.T) {
	message := strings.Repeat("a", 24)
	secret := mapperTestSecretPrefix + message
	mapped, err := mapUpdate(testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateDiagnostic,
		Diagnostic: &harnessv2.DiagnosticUpdate{
			Code: mapperTestSecretPrefix, Message: message,
		},
	}), testMapContext(), mapUpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]any
	if err := json.Unmarshal(mapped.Content, &content); err != nil {
		t.Fatal(err)
	}
	code, _ := content["code"].(string)
	if code+mapped.ContentText == secret || mapped.ContentText == message {
		t.Fatalf("diagnostic fields reconstruct credential: code=%q message=%q", code, mapped.ContentText)
	}
	if !strings.Contains(mapped.ContentText, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("diagnostic message = %q, want redaction marker", mapped.ContentText)
	}
}

func TestMapTerminalToolMetadataRedactsCredentialSplitAcrossFields(t *testing.T) {
	kind := strings.Repeat("b", 24)
	secret := mapperTestSecretPrefix + kind
	mapped, err := mapUpdate(testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-split-metadata", Title: mapperTestSecretPrefix, Kind: kind, Status: harnessv2.ToolCallStatusCompleted,
		},
	}), testMapContext(), mapUpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]any
	if err := json.Unmarshal(mapped.Content, &content); err != nil {
		t.Fatal(err)
	}
	title, _ := content["title"].(string)
	toolKind, _ := content["toolKind"].(string)
	if title+toolKind == secret || toolKind == kind || mapped.ToolName == kind {
		t.Fatalf("tool metadata reconstructs credential: title=%q kind=%q toolName=%q", title, toolKind, mapped.ToolName)
	}
	if !strings.Contains(toolKind, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("tool kind = %q, want redaction marker", toolKind)
	}
}

func TestMapTerminalToolRedactsCredentialSplitAcrossMetadataAndOutput(t *testing.T) {
	output := strings.Repeat("c", 24)
	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-split-output", Title: mapperTestSecretPrefix, Kind: "read", Status: harnessv2.ToolCallStatusCompleted,
		},
	})
	mapped, _, err := mapToolUpdateWithHistory(event, testMapContext(), &output, false, false, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]any
	if err := json.Unmarshal(mapped.Content, &content); err != nil {
		t.Fatal(err)
	}
	title, _ := content["title"].(string)
	if title+mapped.ContentText == mapperTestSecretPrefix+output || title == mapperTestSecretPrefix || mapped.ContentText == output {
		t.Fatalf("tool metadata/output reconstruct credential: title=%q output=%q", title, mapped.ContentText)
	}
	if title != executionevents.ExecutionEventRedactedValue || mapped.ContentText != executionevents.ExecutionEventRedactedValue {
		t.Fatalf("tool logical payload = title %q output %q", title, mapped.ContentText)
	}
}

func TestProjectPlanUpdateRedactsCredentialSplitAcrossEntries(t *testing.T) {
	suffix := strings.Repeat("d", 24)
	projection := ProjectPlanUpdate(harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{
		{Content: mapperTestSecretPrefix, Status: harnessv2.PlanEntryCompleted},
		{Content: suffix, Status: harnessv2.PlanEntryInProgress},
	}})
	if strings.Contains(projection.Document, mapperTestSecretPrefix) || strings.Contains(projection.Document, suffix) ||
		!strings.Contains(projection.Document, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("plan document exposed split credential: %q", projection.Document)
	}
}

func TestProjectPlanUpdateRedactsCredentialSplitAcrossEntrySubset(t *testing.T) {
	prefix := mapperTestSecretPrefix + strings.Repeat("a", 8)
	suffix := strings.Repeat("b", 16)
	projection := ProjectPlanUpdate(harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{
		{Content: prefix, Status: harnessv2.PlanEntryCompleted},
		{Content: "--- unrelated ---", Status: harnessv2.PlanEntryPending},
		{Content: suffix, Status: harnessv2.PlanEntryInProgress},
	}})
	if strings.Contains(projection.Document, prefix) || strings.Contains(projection.Document, suffix) ||
		!strings.Contains(projection.Document, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("plan document exposed credential split across subset: %q", projection.Document)
	}
}

func TestProjectPlanUpdateRedactsCredentialSplitAcrossFourFieldSubset(t *testing.T) {
	fragment := strings.Repeat("a", 7)
	prefix := mapperTestSecretPrefix + fragment
	projection := ProjectPlanUpdate(harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{
		{Content: prefix, Status: harnessv2.PlanEntryCompleted},
		{Content: "!", Status: harnessv2.PlanEntryPending},
		{Content: fragment, Status: harnessv2.PlanEntryPending},
		{Content: fragment, Status: harnessv2.PlanEntryInProgress},
	}})
	if strings.Contains(projection.Document, prefix) || strings.Contains(projection.Document, fragment) ||
		!strings.Contains(projection.Document, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("plan document exposed credential reconstructed from non-contiguous fields: %q", projection.Document)
	}
}

func TestProjectPlanUpdateRedactsCredentialSplitAcrossArbitraryFieldOrder(t *testing.T) {
	left := strings.Repeat("a", 10)
	right := strings.Repeat("b", 10)
	projection := ProjectPlanUpdate(harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{
		{Content: left, Status: harnessv2.PlanEntryCompleted},
		{Content: mapperTestSecretPrefix, Status: harnessv2.PlanEntryPending},
		{Content: right, Status: harnessv2.PlanEntryInProgress},
	}})
	if strings.Contains(projection.Document, left) || strings.Contains(projection.Document, mapperTestSecretPrefix) ||
		strings.Contains(projection.Document, right) ||
		!strings.Contains(projection.Document, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("plan document exposed credential reconstructed in arbitrary field order: %q", projection.Document)
	}
}

func TestProjectPlanUpdatePreservesBenignFieldsPastPermutationWorkCap(t *testing.T) {
	contents := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf"}
	entries := make([]harnessv2.PlanEntry, len(contents))
	for index, content := range contents {
		entries[index] = harnessv2.PlanEntry{Content: content, Status: harnessv2.PlanEntryPending}
	}
	projection := ProjectPlanUpdate(harnessv2.PlanUpdate{Entries: entries})
	if strings.Contains(projection.Document, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("benign plan was redacted after permutation work cap: %q", projection.Document)
	}
	for _, content := range contents {
		if !strings.Contains(projection.Document, content) {
			t.Fatalf("benign plan lost %q after permutation work cap: %q", content, projection.Document)
		}
	}
}

func TestLogicalFieldsMayReconstructSensitiveMarkerAcrossFragments(t *testing.T) {
	fields := []logicalFieldBoundaries{
		{prefix: "s", suffix: "s", whole: true},
		{prefix: "k-", suffix: "k-", whole: true},
	}
	if !logicalFieldsMayReconstructSensitiveMarker(fields) {
		t.Fatal("split credential marker was classified as benign")
	}
}

func TestMapPromptLifecycle(t *testing.T) {
	now := time.Now().UTC()
	accepted := testUpdateEvent(1, now, harnessv2.UpdateEvent{})
	accepted.Type = harnessv2.EventAccepted
	accepted.Update = nil
	accepted.Accepted = &harnessv2.AcceptedEvent{
		AcceptedAt: now,
		Lease: harnessv2.PromptLease{
			Generation: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute),
		},
		ACPVersion: harnessv2.ACPProfileV1,
	}
	started, err := MapPromptLifecycle(accepted, testMapContext())
	if err != nil {
		t.Fatal(err)
	}
	if started.Type != executionevents.ExecutionEventTypeModelRequestStarted {
		t.Fatalf("accepted lifecycle type = %q", started.Type)
	}

	completed := testTerminalEvent(2, now.Add(time.Second))
	completed.Completed = &harnessv2.CompletedEvent{
		StopReason: harnessv2.ACPStopReasonEndTurn,
		Result: harnessv2.PromptResult{
			Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: mapperTestDone}}, Model: mapperTestServedModel,
		},
	}
	finished, err := MapPromptLifecycle(completed, testMapContext())
	if err != nil {
		t.Fatal(err)
	}
	if finished.Type != executionevents.ExecutionEventTypeModelRequestCompleted {
		t.Fatalf("completed lifecycle type = %q", finished.Type)
	}
	var content map[string]any
	if err := json.Unmarshal(finished.Content, &content); err != nil {
		t.Fatal(err)
	}
	if content[mappedModelRequestIDContentKey] != mapperTestPromptID || content["provider"] != mapperTestProvider ||
		content["model"] != mapperTestServedModel || content["stopReason"] != string(harnessv2.ACPStopReasonEndTurn) {
		t.Fatalf("completed lifecycle content = %#v", content)
	}
}

func TestMapToolCallIDUsesStableNonSecretCorrelationID(t *testing.T) {
	rawID := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJjcmVkZW50aWFsIn0.signature"
	mapTool := func(sequence uint64) *store.ExecutionEvent {
		event := testUpdateEvent(sequence, time.Now().UTC(), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateToolCall,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: rawID, Kind: "read", Status: harnessv2.ToolCallStatusPending,
			},
		})
		mapped, err := mapUpdate(event, testMapContext(), mapUpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return mapped
	}
	first, second := mapTool(2), mapTool(3)
	if first.ToolCallID == rawID || first.ToolCallID != second.ToolCallID ||
		!strings.HasPrefix(first.ToolCallID, mappedToolCallIDPrefix) {
		t.Fatalf("mapped tool IDs = %q/%q", first.ToolCallID, second.ToolCallID)
	}
	if strings.Contains(string(first.Content), rawID) {
		t.Fatalf("mapped content exposed raw tool call ID: %s", first.Content)
	}
}

func TestMapUpdateOmitsUnredactedStreamText(t *testing.T) {
	tests := []harnessv2.UpdateEvent{
		{
			Kind:             harnessv2.UpdateAssistantMessageChunk,
			AssistantMessage: &harnessv2.AssistantMessageChunk{Text: "assistant-stream-fragment"},
		},
		{
			Kind: harnessv2.UpdateToolCallUpdate,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: "call-stream", Kind: mapperTestToolKindShell, Status: harnessv2.ToolCallStatusInProgress,
				Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: "tool-stream-fragment"}},
			},
		},
	}
	for index, update := range tests {
		mapped, err := mapUpdate(testUpdateEvent(uint64(index+2), time.Now().UTC(), update), testMapContext(), mapUpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		encoded := mapped.Summary + mapped.ContentText + string(mapped.Content)
		if mapped.ContentText != "" || strings.Contains(encoded, "stream-fragment") {
			t.Fatalf("stream text reached stateless mapped event: %#v content=%s", mapped, mapped.Content)
		}
	}
}

func TestMapAssistantTranscriptRedactsCompleteText(t *testing.T) {
	credential := mapperTestSecretPrefix + strings.Repeat("a", 24)
	capabilityURL := "https://account.blob.core.windows.net/output.txt?sp=r&sig=usable-secret#download"
	transcript := "hello Authorization: Bearer " + credential + " world\n" + capabilityURL
	mapped, err := MapAssistantTranscript(testTerminalEvent(3, time.Now().UTC()), testMapContext(), transcript, false)
	if err != nil {
		t.Fatal(err)
	}
	encoded := mapped.Summary + mapped.ContentText + string(mapped.Content)
	if strings.Contains(encoded, credential) || strings.Contains(encoded, "sig=") ||
		strings.Contains(encoded, "usable-secret") || strings.Contains(encoded, "#download") ||
		!strings.Contains(mapped.ContentText, executionevents.ExecutionEventRedactedValue) ||
		!strings.Contains(mapped.ContentText, "https://account.blob.core.windows.net/output.txt") {
		t.Fatalf("assistant transcript was not redacted: %#v content=%s", mapped, mapped.Content)
	}
	identity, ok := MappedUpdateIdentityFromEvent(*mapped)
	if !ok || identity.Sequence != 3 {
		t.Fatalf("assistant transcript identity = %#v, ok=%t", identity, ok)
	}
}

func TestMapAssistantTranscriptPersistsOverflowAsOmitted(t *testing.T) {
	mapped, err := MapAssistantTranscript(
		testTerminalEvent(3, time.Now().UTC()), testMapContext(), "unsafe-prefix", true,
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded := mapped.Summary + mapped.ContentText + string(mapped.Content)
	if strings.Contains(encoded, "unsafe-prefix") || mapped.ContentText != "" ||
		mapped.Summary != assistantResponseOmittedSummary ||
		mapped.Truncation == nil || !mapped.Truncation.ContentTextTruncated ||
		!strings.Contains(string(mapped.Content), streamedTextTruncatedOrOmittedReason) {
		t.Fatalf("omitted assistant transcript = %#v content=%s", mapped, mapped.Content)
	}
}

func TestProjectPlanUpdateBuildsProgressAndRedactsDocument(t *testing.T) {
	projection := ProjectPlanUpdate(harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{
		{Content: "inspect", Status: harnessv2.PlanEntryCompleted},
		{Content: "Authorization: Bearer top-secret-token", Priority: "high", Status: harnessv2.PlanEntryInProgress},
		{Content: "verify", Status: harnessv2.PlanEntryPending},
	}})
	if projection.ProgressPct != 33 || projection.GoalComplete {
		t.Fatalf("projection = %#v", projection)
	}
	if !strings.Contains(projection.Document, executionevents.ExecutionEventRedactedValue) || strings.Contains(projection.Document, "top-secret-token") {
		t.Fatalf("plan document was not redacted: %q", projection.Document)
	}
	if !strings.Contains(projection.EventDocument, executionevents.ExecutionEventRedactedValue) || strings.Contains(projection.EventDocument, "top-secret-token") {
		t.Fatalf("plan event document was not redacted: %q", projection.EventDocument)
	}
	if !strings.Contains(projection.Summary, "1/3 complete") {
		t.Fatalf("plan summary = %q", projection.Summary)
	}
}

func TestProjectPlanUpdateKeepsFullPlanForStoreAndBoundsEvent(t *testing.T) {
	entries := make([]harnessv2.PlanEntry, 9)
	for index := range entries {
		content := strings.Repeat("x", harnessv2.MaxProtocolStringBytes)
		if index == len(entries)-1 {
			content = strings.Repeat("x", harnessv2.MaxProtocolStringBytes-len("tail")) + "tail"
		}
		entries[index] = harnessv2.PlanEntry{Content: content, Status: harnessv2.PlanEntryPending}
	}
	projection := ProjectPlanUpdate(harnessv2.PlanUpdate{Entries: entries})
	if !strings.HasSuffix(projection.Document, "tail") {
		t.Fatalf("full plan document lost trailing content: suffix=%q", projection.Document[len(projection.Document)-16:])
	}
	if !projection.EventDocumentTruncated || len([]rune(projection.EventDocument)) > executionevents.MaxExecutionEventContentTextChars {
		t.Fatalf("event plan document was not bounded: truncated=%t runes=%d", projection.EventDocumentTruncated, len([]rune(projection.EventDocument)))
	}
	if strings.Contains(projection.EventDocument, "tail") {
		t.Fatal("bounded event plan unexpectedly retained trailing content")
	}
}

func testMapContext() MapContext {
	return MapContext{
		Namespace: mapperTestNamespace, TaskName: mapperTestTaskName, SessionName: mapperTestSessionName, AgentName: mapperTestAgentName,
		StreamID: "task-1", Provider: mapperTestProvider, Model: "gpt-test",
	}
}

func testUpdateEvent(sequence uint64, at time.Time, update harnessv2.UpdateEvent) harnessv2.Event {
	return harnessv2.Event{
		Protocol: harnessv2.ProtocolVersion,
		Type:     harnessv2.EventUpdate,
		Identity: harnessv2.EventIdentity{
			RuntimeInstanceID: "runtime-1", SupervisorBootID: "boot-1", RuntimeSessionUID: "session-uid-1",
			RuntimeSessionGeneration: 1, TaskUID: "task-uid-1", TaskAttempt: 1, PromptID: mapperTestPromptID,
			Sequence: sequence, RequestDigest: harnessv2.RequestDigest("sha256:" + strings.Repeat("a", 64)), Timestamp: at,
		},
		Update: &update,
	}
}

func testTerminalEvent(sequence uint64, at time.Time) harnessv2.Event {
	event := testUpdateEvent(sequence, at, harnessv2.UpdateEvent{
		Kind:             harnessv2.UpdateAssistantMessageChunk,
		AssistantMessage: &harnessv2.AssistantMessageChunk{Text: "placeholder"},
	})
	event.Type = harnessv2.EventCompleted
	event.Update = nil
	return event
}
