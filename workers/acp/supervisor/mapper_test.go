package supervisor

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/orka-agents/orka/internal/acp"
	executionevents "github.com/orka-agents/orka/internal/events"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const (
	supervisorTestSessionUpdateKey  = "sessionUpdate"
	supervisorTestContentKey        = "content"
	supervisorTestTypeKey           = "type"
	supervisorTestTextKey           = "text"
	supervisorTestEntriesKey        = "entries"
	supervisorTestToolCallIDKey     = "toolCallId"
	supervisorTestStatusKey         = "status"
	supervisorTestTitleKey          = "title"
	supervisorTestKindKey           = "kind"
	supervisorTestReadKind          = "read"
	supervisorTestPlanUpdate        = "plan"
	supervisorTestToolCallUpdate    = "tool_call_update"
	supervisorTestResourceLinkType  = "resource_link"
	supervisorTestCompletedStatus   = "completed"
	supervisorTestProviderSessionID = "provider-session"
)

func TestPromptContentToACPAddsRequiredResourceLinkName(t *testing.T) {
	blocks := []harnessv2.ContentBlock{
		{Type: harnessv2.ContentBlockResourceLink, URI: "https://example.com/unnamed"},
		{Type: harnessv2.ContentBlockResourceLink, URI: "https://example.com/named", Name: "report", MimeType: "text/plain"},
	}

	mapped, err := promptContentToACP(blocks)
	if err != nil {
		t.Fatal(err)
	}
	if len(mapped) != 2 {
		t.Fatalf("mapped blocks = %#v", mapped)
	}
	if mapped[0].Type != supervisorTestResourceLinkType || mapped[0].URI != blocks[0].URI || mapped[0].Name != blocks[0].URI {
		t.Fatalf("unnamed resource link = %#v", mapped[0])
	}
	if mapped[1].Type != supervisorTestResourceLinkType || mapped[1].URI != blocks[1].URI ||
		mapped[1].Name != blocks[1].Name || mapped[1].MIMEType != blocks[1].MimeType {
		t.Fatalf("named resource link = %#v", mapped[1])
	}
}

func TestMapACPUpdateDecodesAgentMessageContent(t *testing.T) {
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(`{
		"sessionUpdate":"agent_message_chunk",
		"content":{"type":"text","text":"hello"}
	}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || text != "hello" || update == nil || update.Kind != harnessv2.UpdateAssistantMessageChunk ||
		update.AssistantMessage == nil || update.AssistantMessage.Text != "hello" {
		t.Fatalf("mapped update = %#v text=%q ok=%v", update, text, ok)
	}
}

func TestACPAssistantMessagePhaseAllowsOnlyCodexProtocolEnums(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "commentary",
			raw:  `{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"checking"},"_meta":{"codex":{"phase":"commentary"}}}`,
			want: acpAssistantPhaseCommentary,
		},
		{
			name: "final answer",
			raw:  `{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"done"},"_meta":{"codex":{"phase":"final_answer"}}}`,
			want: acpAssistantPhaseFinalAnswer,
		},
		{
			name: "unknown phase",
			raw:  `{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"done"},"_meta":{"codex":{"phase":"provider-private"}}}`,
		},
		{
			name: "other update",
			raw:  `{"sessionUpdate":"tool_call","toolCallId":"call-1"}`,
		},
		{name: "malformed", raw: `{`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := acpAssistantMessagePhase(&acp.SessionNotification{Update: json.RawMessage(test.raw)})
			if got != test.want {
				t.Fatalf("phase = %q, want %q", got, test.want)
			}
		})
	}
	if got := acpAssistantMessagePhase(nil); got != "" {
		t.Fatalf("nil phase = %q", got)
	}
}

func TestMapACPUpdatePreservesWhitespaceChunk(t *testing.T) {
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(`{
		"sessionUpdate":"agent_message_chunk",
		"content":{"type":"text","text":" \n"}
	}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || update == nil || update.AssistantMessage == nil ||
		update.AssistantMessage.Text != " \n" || text != " \n" {
		t.Fatalf("mapped whitespace update = %#v text=%q ok=%v", update, text, ok)
	}
	if err := update.Validate(); err != nil {
		t.Fatalf("whitespace update validation: %v", err)
	}
}

func TestMapACPUpdatePreservesToolCallContentArray(t *testing.T) {
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(`{
		"sessionUpdate":"tool_call_update",
		"toolCallId":"call-1",
		"title":"Read LICENSE",
		"kind":"read",
		"status":"completed",
		"content":[{"type":"content","content":{"type":"text","text":"tool output"}}]
	}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || text != "" || update == nil || update.Kind != harnessv2.UpdateToolCallUpdate || update.ToolCall == nil {
		t.Fatalf("mapped update = %#v text=%q ok=%v", update, text, ok)
	}
	wantID, err := canonicalACPToolCallID("call-1")
	if err != nil {
		t.Fatal(err)
	}
	if update.ToolCall.ToolCallID != wantID || update.ToolCall.Status != harnessv2.ToolCallStatusCompleted {
		t.Fatalf("tool call = %#v", update.ToolCall)
	}
	if len(update.ToolCall.Content) != 1 || update.ToolCall.Content[0].Type != harnessv2.ContentBlockText ||
		update.ToolCall.Content[0].Text != "tool output" {
		t.Fatalf("tool call content = %#v", update.ToolCall.Content)
	}
}

func TestMapACPUpdatePreservesContentOnlyToolCallUpdate(t *testing.T) {
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(`{
		"sessionUpdate":"tool_call_update",
		"toolCallId":"call-1",
		"content":[{"type":"content","content":{"type":"text","text":"streamed output"}}]
	}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || text != "" || update == nil || update.ToolCall == nil ||
		update.ToolCall.Status != harnessv2.ToolCallStatusInProgress || len(update.ToolCall.Content) != 1 ||
		update.ToolCall.Content[0].Text != "streamed output" || !update.ToolCall.ContentReplace {
		t.Fatalf("mapped content-only update = %#v text=%q ok=%v", update, text, ok)
	}
}

func TestMapACPUpdatePreservesToolContentPresence(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		wantReplace bool
	}{
		{
			name: "omitted",
			raw:  `{"sessionUpdate":"tool_call_update","toolCallId":"call-1","status":"in_progress"}`,
		},
		{
			name:        "null clears",
			raw:         `{"sessionUpdate":"tool_call_update","toolCallId":"call-1","content":null}`,
			wantReplace: true,
		},
		{
			name:        "empty collection clears",
			raw:         `{"sessionUpdate":"tool_call_update","toolCallId":"call-1","content":[]}`,
			wantReplace: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(test.raw)})
			if err != nil {
				t.Fatal(err)
			}
			if !ok || text != "" || update == nil || update.ToolCall == nil ||
				update.ToolCall.ContentReplace != test.wantReplace {
				t.Fatalf("mapped content presence = %#v text=%q ok=%v", update, text, ok)
			}
		})
	}
}

func TestMapACPUpdatePreservesWhitespaceToolContent(t *testing.T) {
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(`{
		"sessionUpdate":"tool_call_update",
		"toolCallId":"call-1",
		"content":[{"type":"content","content":{"type":"text","text":" \n"}}]
	}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || text != "" || update == nil || update.ToolCall == nil || len(update.ToolCall.Content) != 1 ||
		update.ToolCall.Content[0].Text != " \n" || !update.ToolCall.ContentReplace {
		t.Fatalf("mapped whitespace tool content = %#v text=%q ok=%v", update, text, ok)
	}
	if err := update.Validate(); err != nil {
		t.Fatalf("whitespace tool update validation: %v", err)
	}
}

func TestMapACPUpdateOmitsInvalidPlanTelemetry(t *testing.T) {
	tooMany := make([]harnessv2.PlanEntry, 129)
	for index := range tooMany {
		tooMany[index] = harnessv2.PlanEntry{Content: "step", Status: harnessv2.PlanEntryPending}
	}
	tests := []struct {
		name    string
		entries []harnessv2.PlanEntry
	}{
		{name: "too many entries", entries: tooMany},
		{name: "oversized entry", entries: []harnessv2.PlanEntry{{
			Content: strings.Repeat("x", harnessv2.MaxProtocolStringBytes+1), Status: harnessv2.PlanEntryPending,
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				supervisorTestSessionUpdateKey: supervisorTestPlanUpdate,
				supervisorTestEntriesKey:       test.entries,
			})
			if err != nil {
				t.Fatal(err)
			}
			update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: raw})
			if err != nil || ok || update != nil || text != "" {
				t.Fatalf("mapped invalid plan = %#v text=%q ok=%t err=%v", update, text, ok, err)
			}
		})
	}
}

func TestMapACPToolCallContentOmitsSnapshotOverBlockLimit(t *testing.T) {
	items := make([]map[string]any, harnessv2.MaxContentBlocks+1)
	for index := range items {
		items[index] = map[string]any{
			supervisorTestTypeKey:    supervisorTestContentKey,
			supervisorTestContentKey: map[string]any{supervisorTestTypeKey: acpContentTypeText, supervisorTestTextKey: "tool output"},
		}
	}
	raw, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	mapped, contentOmitted, err := mapACPToolCallContent(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(mapped) != 0 || !contentOmitted {
		t.Fatalf("mapped content blocks = %#v omitted=%t", mapped, contentOmitted)
	}
	update := harnessv2.UpdateEvent{Kind: harnessv2.UpdateToolCallUpdate, ToolCall: &harnessv2.ToolCallUpdate{
		ToolCallID: "call-many-blocks", Status: harnessv2.ToolCallStatusInProgress, ContentOmitted: true,
	}}
	if err := update.Validate(); err != nil {
		t.Fatalf("bounded tool update validation: %v", err)
	}
}

func TestProjectACPContentBlockOmitsOversizedTelemetry(t *testing.T) {
	oversizedText := strings.Repeat("x", harnessv2.MaxPromptContentBytes+1)
	oversizedResource, err := json.Marshal(map[string]any{
		"uri": "file:///workspace/output.txt", "mimeType": "text/plain", supervisorTestTextKey: oversizedText,
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		block acp.ContentBlock
	}{
		{name: acpContentTypeText, block: acp.ContentBlock{Type: acpContentTypeText, Text: oversizedText}},
		{name: "resource text", block: acp.ContentBlock{Type: "resource", Resource: oversizedResource}},
		{name: "resource URI", block: acp.ContentBlock{
			Type: supervisorTestResourceLinkType, URI: "https://example.com/" + strings.Repeat("x", harnessv2.MaxResourceURIBytes),
		}},
		{name: "resource name", block: acp.ContentBlock{
			Type: supervisorTestResourceLinkType, URI: "https://example.com/output", Name: strings.Repeat("x", harnessv2.MaxContentNameBytes+1),
		}},
		{name: "resource MIME type", block: acp.ContentBlock{
			Type: supervisorTestResourceLinkType, URI: "https://example.com/output", MIMEType: strings.Repeat("x", harnessv2.MaxContentMIMETypeBytes+1),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projected, ok, bounded, err := projectACPContentBlock(test.block)
			if err != nil || ok || !bounded || projected != (harnessv2.ContentBlock{}) {
				t.Fatalf("oversized projection = %#v ok=%t bounded=%t err=%v", projected, ok, bounded, err)
			}
		})
	}
}

func TestMapACPUpdateMarksOversizedToolContentOmitted(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		supervisorTestSessionUpdateKey: supervisorTestToolCallUpdate,
		supervisorTestToolCallIDKey:    "call-oversized-content",
		supervisorTestStatusKey:        supervisorTestCompletedStatus,
		supervisorTestContentKey: []map[string]any{{
			supervisorTestTypeKey: supervisorTestContentKey,
			supervisorTestContentKey: map[string]any{
				supervisorTestTypeKey: acpContentTypeText, supervisorTestTextKey: strings.Repeat("x", harnessv2.MaxPromptContentBytes+1),
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: raw})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || text != "" || update == nil || update.ToolCall == nil ||
		len(update.ToolCall.Content) != 0 || update.ToolCall.ContentReplace || !update.ToolCall.ContentOmitted {
		t.Fatalf("mapped oversized tool content = %#v text=%q ok=%t", update, text, ok)
	}
	if err := update.Validate(); err != nil {
		t.Fatalf("validate omitted oversized tool content: %v", err)
	}
}

func TestMapACPUpdateMarksUnprojectableToolContentOmitted(t *testing.T) {
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(`{
		"sessionUpdate":"tool_call_update",
		"toolCallId":"call-unprojectable-content",
		"status":"completed",
		"content":[{"type":"diff","path":"README.md"}]
	}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || text != "" || update == nil || update.ToolCall == nil ||
		len(update.ToolCall.Content) != 0 || update.ToolCall.ContentReplace || !update.ToolCall.ContentOmitted {
		t.Fatalf("mapped unprojectable tool content = %#v text=%q ok=%t", update, text, ok)
	}
	if err := update.Validate(); err != nil {
		t.Fatalf("validate omitted unprojectable tool content: %v", err)
	}
}

func TestMapRuntimeEventBoundsAggregateToolContentToEventLine(t *testing.T) {
	server, cfg, _ := newTestServer(t, "immediate")
	fence := cfg.Fence
	fence.RuntimeSessionUID = "mapper-content-session-uid"
	fence.RuntimeSessionGeneration = 1
	state := &sessionState{
		descriptor: harnessv2.RuntimeSessionDescriptor{
			RuntimeSessionUID: fence.RuntimeSessionUID,
			Generation:        fence.RuntimeSessionGeneration,
		},
		operations:  make(map[harnessv2.OperationID]harnessv2.OperationRecord),
		permissions: make(map[harnessv2.PermissionRequestID]permissionState),
	}
	prompt := &promptState{request: testStartPromptRequest(t, cfg, fence)}
	text := strings.Repeat("x", 600<<10)
	raw, err := json.Marshal(map[string]any{
		supervisorTestSessionUpdateKey: supervisorTestToolCallUpdate,
		supervisorTestToolCallIDKey:    "call-aggregate-content",
		supervisorTestStatusKey:        supervisorTestCompletedStatus,
		supervisorTestContentKey: []map[string]any{
			{supervisorTestTypeKey: supervisorTestContentKey, supervisorTestContentKey: map[string]any{supervisorTestTypeKey: acpContentTypeText, supervisorTestTextKey: text}},
			{supervisorTestTypeKey: supervisorTestContentKey, supervisorTestContentKey: map[string]any{supervisorTestTypeKey: acpContentTypeText, supervisorTestTextKey: text}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := server.mapRuntimeEvent(state, prompt, acp.PromptEvent{
		Type: acp.PromptEventUpdate, Timestamp: time.Now().UTC(),
		Update: &acp.SessionNotification{SessionID: supervisorTestProviderSessionID, Update: raw},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mapped == nil || mapped.Update == nil || mapped.Update.ToolCall == nil {
		t.Fatalf("mapped tool update = %#v", mapped)
	}
	if len(mapped.Update.ToolCall.Content) != 0 || mapped.Update.ToolCall.ContentReplace ||
		!mapped.Update.ToolCall.ContentOmitted {
		t.Fatalf("mapped bounded tool content = %#v", mapped.Update.ToolCall)
	}
	line, err := json.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
	if len(line) > server.cfg.Capabilities.Limits.MaxEventLineBytes {
		t.Fatalf("mapped event line = %d bytes, limit %d", len(line), server.cfg.Capabilities.Limits.MaxEventLineBytes)
	}
}

func TestMapACPUpdateIgnoresStatuslessToolOutputDelta(t *testing.T) {
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(`{
		"sessionUpdate":"tool_call_update",
		"toolCallId":"call-1",
		"_meta":{"terminal_output_delta":{"terminal_id":"call-1","data":"provider output"}}
	}`)})
	if err != nil {
		t.Fatal(err)
	}
	if ok || update != nil || text != "" {
		t.Fatalf("mapped status-less tool output = %#v text=%q ok=%v", update, text, ok)
	}
}

func TestMapACPUpdateRejectsStatuslessToolOutputWithoutCallID(t *testing.T) {
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(`{
		"sessionUpdate":"tool_call_update",
		"_meta":{"terminal_output_delta":{"data":"provider output"}}
	}`)})
	if err == nil || !strings.Contains(err.Error(), "omitted toolCallId") {
		t.Fatalf("missing-ID tool output error = %v", err)
	}
	if ok || update != nil || text != "" {
		t.Fatalf("mapped malformed tool output = %#v text=%q ok=%v", update, text, ok)
	}
}

func TestMapACPUpdatePreservesToolCallLifecycleTransitions(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantKind   harnessv2.UpdateKind
		wantStatus harnessv2.ToolCallStatus
		wantTitle  string
	}{
		{
			name:       "start defaults pending",
			raw:        `{"sessionUpdate":"tool_call","toolCallId":"call-1","title":"Read repository","kind":"read"}`,
			wantKind:   harnessv2.UpdateToolCall,
			wantStatus: harnessv2.ToolCallStatusPending,
			wantTitle:  "Read repository",
		},
		{
			name:       "explicit in progress",
			raw:        `{"sessionUpdate":"tool_call_update","toolCallId":"call-1","status":"in_progress"}`,
			wantKind:   harnessv2.UpdateToolCallUpdate,
			wantStatus: harnessv2.ToolCallStatusInProgress,
		},
		{
			name:       "explicit completed",
			raw:        `{"sessionUpdate":"tool_call_update","toolCallId":"call-1","status":"completed"}`,
			wantKind:   harnessv2.UpdateToolCallUpdate,
			wantStatus: harnessv2.ToolCallStatusCompleted,
		},
		{
			name:       "visible metadata defaults in progress",
			raw:        `{"sessionUpdate":"tool_call_update","toolCallId":"call-1","title":"Reading repository","kind":"read"}`,
			wantKind:   harnessv2.UpdateToolCallUpdate,
			wantStatus: harnessv2.ToolCallStatusInProgress,
			wantTitle:  "Reading repository",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(test.raw)})
			if err != nil {
				t.Fatal(err)
			}
			if !ok || text != "" || update == nil || update.Kind != test.wantKind || update.ToolCall == nil ||
				update.ToolCall.Status != test.wantStatus || update.ToolCall.Title != test.wantTitle {
				t.Fatalf("mapped lifecycle update = %#v text=%q ok=%v", update, text, ok)
			}
		})
	}
}

func TestMapACPUpdatePreservesContextWindowUsage(t *testing.T) {
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(`{
		"sessionUpdate":"usage_update","used":53000,"size":200000
	}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || text != "" || update == nil || update.Kind != harnessv2.UpdateUsage || update.Usage == nil ||
		update.Usage.ContextWindowUsed == nil || *update.Usage.ContextWindowUsed != 53000 ||
		update.Usage.ContextWindowSize == nil || *update.Usage.ContextWindowSize != 200000 ||
		update.Usage.InputTokens != 0 {
		t.Fatalf("mapped usage update = %#v text=%q ok=%v", update, text, ok)
	}
	if err := update.Validate(); err != nil {
		t.Fatalf("mapped usage validation: %v", err)
	}
}

func TestMapACPUpdateDropsInvalidContextWindowUsage(t *testing.T) {
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: json.RawMessage(`{
		"sessionUpdate":"usage_update","used":53000,"size":0
	}`)})
	if err != nil {
		t.Fatal(err)
	}
	if ok || text != "" || update != nil {
		t.Fatalf("mapped invalid usage update = %#v text=%q ok=%v", update, text, ok)
	}
}

func TestMapACPUpdateBoundsProviderToolCallTitle(t *testing.T) {
	exactBoundary := strings.Repeat("x", maxACPToolCallTitleBytes)
	fullCommand := strings.Repeat("git diff --no-ext-diff && ", 64) + "go test ./..."
	multibyte := strings.Repeat("x", maxACPToolCallTitleBytes-len(acpToolCallTitleEllipsis)-1) + "界界"
	tests := []struct {
		name  string
		title string
		want  string
	}{
		{name: "exact boundary is unchanged", title: exactBoundary, want: exactBoundary},
		{
			name:  "codex full command is bounded",
			title: fullCommand,
			want:  fullCommand[:maxACPToolCallTitleBytes-len(acpToolCallTitleEllipsis)] + acpToolCallTitleEllipsis,
		},
		{
			name:  "multibyte cutoff remains valid UTF-8",
			title: multibyte,
			want:  strings.Repeat("x", maxACPToolCallTitleBytes-len(acpToolCallTitleEllipsis)-1) + acpToolCallTitleEllipsis,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				supervisorTestSessionUpdateKey: acpUpdateToolCall,
				supervisorTestToolCallIDKey:    "call-1",
				supervisorTestTitleKey:         test.title,
				supervisorTestKindKey:          "execute",
			})
			if err != nil {
				t.Fatal(err)
			}
			update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: raw})
			if err != nil {
				t.Fatal(err)
			}
			if !ok || text != "" || update == nil || update.ToolCall == nil {
				t.Fatalf("mapped tool call = %#v text=%q ok=%v", update, text, ok)
			}
			if update.ToolCall.Title != test.want {
				t.Fatalf("title = %q, want %q", update.ToolCall.Title, test.want)
			}
			if len(update.ToolCall.Title) > maxACPToolCallTitleBytes || !utf8.ValidString(update.ToolCall.Title) {
				t.Fatalf("bounded title length = %d validUTF8=%v", len(update.ToolCall.Title), utf8.ValidString(update.ToolCall.Title))
			}
			if err := update.Validate(); err != nil {
				t.Fatalf("mapped update violates harness v2: %v", err)
			}
		})
	}
}

func TestMapACPUpdateRedactsProviderToolCallTitleBeforeBounding(t *testing.T) {
	prefix := strings.Repeat("x", maxACPToolCallTitleBytes-9) + " "
	credential := "sk-" + strings.Repeat("a", 24)
	raw, err := json.Marshal(map[string]any{
		supervisorTestSessionUpdateKey: acpUpdateToolCall,
		supervisorTestToolCallIDKey:    "call-credential",
		supervisorTestTitleKey:         prefix + credential,
		supervisorTestKindKey:          "execute",
	})
	if err != nil {
		t.Fatal(err)
	}
	update, text, ok, err := mapACPUpdate(&acp.SessionNotification{Update: raw})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || text != "" || update == nil || update.ToolCall == nil {
		t.Fatalf("mapped tool call = %#v text=%q ok=%v", update, text, ok)
	}
	redacted := prefix + executionevents.ExecutionEventRedactedValue
	want := redacted[:maxACPToolCallTitleBytes-len(acpToolCallTitleEllipsis)] + acpToolCallTitleEllipsis
	if update.ToolCall.Title != want || strings.Contains(update.ToolCall.Title, credential[:8]) {
		t.Fatalf("bounded redacted title = %q, want %q", update.ToolCall.Title, want)
	}
}

func TestMapRuntimeEventSuppressesStatuslessToolOutputBurstWithoutWeakeningRateLimit(t *testing.T) {
	server, cfg, _ := newTestServer(t, "immediate")
	server.cfg.Capabilities.Limits.MaxUpdateEventsPerSecond = 2
	fence := cfg.Fence
	fence.RuntimeSessionUID = "mapper-session-uid"
	fence.RuntimeSessionGeneration = 1
	newStateAndPrompt := func() (*sessionState, *promptState) {
		return &sessionState{
			descriptor: harnessv2.RuntimeSessionDescriptor{
				RuntimeSessionUID: fence.RuntimeSessionUID,
				Generation:        fence.RuntimeSessionGeneration,
			},
			operations:  make(map[harnessv2.OperationID]harnessv2.OperationRecord),
			permissions: make(map[harnessv2.PermissionRequestID]permissionState),
		}, &promptState{request: testStartPromptRequest(t, cfg, fence)}
	}
	mapEvent := func(prompt *promptState, state *sessionState, eventType acp.PromptEventType, update string, at time.Time) *harnessv2.Event {
		event := acp.PromptEvent{Type: eventType, Timestamp: at}
		if update != "" {
			event.Update = &acp.SessionNotification{SessionID: supervisorTestProviderSessionID, Update: json.RawMessage(update)}
		}
		mapped, err := server.mapRuntimeEvent(state, prompt, event)
		if err != nil {
			t.Fatalf("map %s event: %v", eventType, err)
		}
		return mapped
	}

	state, prompt := newStateAndPrompt()
	limits := eventLimits(server.cfg.Capabilities.Limits)
	var stream bytes.Buffer
	encoder, err := harnessv2.NewEventEncoder(&stream, limits, harnessv2.EventExpectationFromMetadata(prompt.request.Metadata))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	acceptedEvent := func(prompt *promptState, state *sessionState) harnessv2.Event {
		prompt.sequence = 1
		return harnessv2.Event{
			Protocol: harnessv2.ProtocolVersion,
			Type:     harnessv2.EventAccepted,
			Identity: eventIdentity(server.cfg.Fence, state.descriptor, prompt.request.Metadata, prompt.sequence, now),
			Accepted: &harnessv2.AcceptedEvent{
				AcceptedAt: now,
				Lease:      prompt.request.Lease,
				ACPVersion: harnessv2.ACPProfileV1,
			},
		}
	}
	accepted := acceptedEvent(prompt, state)
	if err := encoder.Encode(accepted); err != nil {
		t.Fatalf("encode accepted: %v", err)
	}

	ignoredDelta := `{"sessionUpdate":"tool_call_update","toolCallId":"call-1","_meta":{"terminal_output_delta":{"terminal_id":"call-1","data":"x"}}}`
	for index := range runtimeMaxUpdateEventsPerSecond + 1 {
		if mapped := mapEvent(prompt, state, acp.PromptEventUpdate, ignoredDelta, now); mapped != nil {
			t.Fatalf("status-less tool output delta %d mapped to %#v", index, mapped)
		}
	}
	visibleUpdates := []string{
		`{"sessionUpdate":"tool_call","toolCallId":"call-1","title":"Read repository","kind":"read"}`,
		`{"sessionUpdate":"tool_call_update","toolCallId":"call-1","status":"completed"}`,
	}
	for _, update := range visibleUpdates {
		mapped := mapEvent(prompt, state, acp.PromptEventUpdate, update, now)
		if mapped == nil {
			t.Fatalf("visible tool lifecycle update was suppressed: %s", update)
		}
		if err := encoder.Encode(*mapped); err != nil {
			t.Fatalf("encode visible tool lifecycle update: %v", err)
		}
	}
	terminal, _, err := server.terminalEvent(state, prompt, acp.PromptResult{
		Outcome: acp.PromptOutcomeCompleted, StopReason: acp.StopReasonEndTurn,
		Accepted: true, SettledAt: now,
	})
	if err != nil {
		t.Fatalf("build terminal event: %v", err)
	}
	if err := encoder.Encode(terminal); err != nil {
		t.Fatalf("encode terminal event: %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("close event stream: %v", err)
	}
	decoder, err := harnessv2.NewEventDecoder(bytes.NewReader(stream.Bytes()), limits, harnessv2.EventExpectationFromMetadata(prompt.request.Metadata))
	if err != nil {
		t.Fatal(err)
	}
	events, err := decoder.DecodeAll()
	if err != nil {
		t.Fatalf("decode event stream: %v", err)
	}
	if len(events) != 4 || events[0].Type != harnessv2.EventAccepted || events[1].Update == nil ||
		events[1].Update.Kind != harnessv2.UpdateToolCall || events[2].Update == nil ||
		events[2].Update.Kind != harnessv2.UpdateToolCallUpdate || events[3].Type != harnessv2.EventCompleted {
		t.Fatalf("event stream = %#v", events)
	}
	if prompt.sequence != 4 {
		t.Fatalf("harness sequence = %d, want 4 visible events", prompt.sequence)
	}

	state, prompt = newStateAndPrompt()
	var guarded bytes.Buffer
	guardedEncoder, err := harnessv2.NewEventEncoder(&guarded, limits, harnessv2.EventExpectationFromMetadata(prompt.request.Metadata))
	if err != nil {
		t.Fatal(err)
	}
	if err := guardedEncoder.Encode(acceptedEvent(prompt, state)); err != nil {
		t.Fatal(err)
	}
	for index := range 3 {
		mapped := mapEvent(prompt, state, acp.PromptEventUpdate,
			`{"sessionUpdate":"tool_call","toolCallId":"call-`+string(rune('a'+index))+`","title":"Visible","kind":"read"}`, now)
		err = guardedEncoder.Encode(*mapped)
		if index < 2 && err != nil {
			t.Fatalf("visible update %d: %v", index, err)
		}
	}
	if !errors.Is(err, harnessv2.ErrEventRateExceeded) {
		t.Fatalf("third visible update error = %v, want %v", err, harnessv2.ErrEventRateExceeded)
	}
}

func TestMapACPUpdateCanonicalizesOversizedToolCallIDAcrossEvents(t *testing.T) {
	rawID := strings.Repeat("provider-tool-call-", 24)
	encoded, err := json.Marshal(map[string]any{
		supervisorTestSessionUpdateKey: acpUpdateToolCall, supervisorTestToolCallIDKey: rawID,
		supervisorTestTitleKey: "Read repository", supervisorTestKindKey: supervisorTestReadKind, supervisorTestStatusKey: "pending",
	})
	if err != nil {
		t.Fatal(err)
	}
	created, _, ok, err := mapACPUpdate(&acp.SessionNotification{Update: encoded})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(map[string]any{
		supervisorTestSessionUpdateKey: supervisorTestToolCallUpdate, supervisorTestToolCallIDKey: rawID,
		supervisorTestTitleKey: "Read repository", supervisorTestKindKey: supervisorTestReadKind, supervisorTestStatusKey: supervisorTestCompletedStatus,
	})
	if err != nil {
		t.Fatal(err)
	}
	completed, _, updateOK, err := mapACPUpdate(&acp.SessionNotification{Update: encoded})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !updateOK || created == nil || completed == nil || created.ToolCall == nil || completed.ToolCall == nil {
		t.Fatalf("mapped tool calls = created %#v completed %#v", created, completed)
	}
	canonicalID, err := canonicalACPToolCallID(rawID)
	if err != nil {
		t.Fatal(err)
	}
	if canonicalID == rawID || !strings.HasPrefix(canonicalID, canonicalACPToolCallIDPrefix) || len(canonicalID) > 253 {
		t.Fatalf("canonical tool call ID = %q", canonicalID)
	}
	spaceVariant, err := canonicalACPToolCallID(" " + rawID)
	if err != nil {
		t.Fatal(err)
	}
	reservedLooking, err := canonicalACPToolCallID(canonicalID)
	if err != nil {
		t.Fatal(err)
	}
	if spaceVariant == canonicalID || reservedLooking == canonicalID {
		t.Fatalf("canonical tool call IDs collided: raw=%q space=%q reserved=%q", canonicalID, spaceVariant, reservedLooking)
	}
	if created.ToolCall.ToolCallID != canonicalID || completed.ToolCall.ToolCallID != canonicalID {
		t.Fatalf("tool call correlation changed: created=%q completed=%q want=%q", created.ToolCall.ToolCallID, completed.ToolCall.ToolCallID, canonicalID)
	}
	if err := created.Validate(); err != nil {
		t.Fatalf("created event validation: %v", err)
	}
	if err := completed.Validate(); err != nil {
		t.Fatalf("completed event validation: %v", err)
	}

	permissionToolCall, err := json.Marshal(map[string]any{
		supervisorTestToolCallIDKey: rawID, "name": "Read", supervisorTestTitleKey: "Read repository",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	permission, err := mapPermission(&acp.PermissionRequestEvent{
		RequestID: "permission-1",
		Request: acp.RequestPermissionRequest{
			ToolCall: permissionToolCall,
			Options: []acp.PermissionOption{{
				OptionID: "allow-once", Name: "Allow", Kind: string(harnessv2.PermissionOptionAllowOnce),
			}},
		},
	}, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if permission.ToolCallID != canonicalID {
		t.Fatalf("permission tool call ID = %q, want %q", permission.ToolCallID, canonicalID)
	}
	if err := permission.Validate(now); err != nil {
		t.Fatalf("permission event validation: %v", err)
	}
}
