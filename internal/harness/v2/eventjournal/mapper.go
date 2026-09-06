package eventjournal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	executionevents "github.com/orka-agents/orka/internal/events"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
)

const (
	mappedUpdateIdentityKeySeparator  = "\x00"
	mappedToolCallIDPrefix            = "event-tool-call-v1-sha256-"
	mappedToolCallIDDomain            = "orka.harness.v2.execution-event.tool-call-id.v1\x00"
	mappedJournalDedupeKeyPrefix      = "harness-v2-event-v1-sha256-"
	mappedJournalDedupeKeyDomain      = "orka.harness.v2.execution-event.dedupe-key.v1\x00"
	mappedAssistantTranscriptKind     = "assistant_transcript"
	mappedToolStreamClosureKind       = "tool_stream_closure"
	mappedToolTerminalKind            = "tool_terminal"
	mappedTerminalUsageKind           = "terminal_usage"
	mappedPromptAcceptedKind          = "prompt_accepted"
	mappedPromptTerminalKind          = "prompt_terminal"
	mappedPromptStreamFailureCode     = "prompt_stream_error"
	mappedPromptSettlementFailureCode = "prompt_cancellation_failed"
	mappedPromptSettlementUnknownCode = "prompt_settlement_outcome_unknown"
	mappedHarnessV2ContentKey         = "harnessV2"
	mappedControllerSynthesizedKey    = "controllerSynthesized"
	mappedUpdateKindContentKey        = "updateKind"
	mappedJournalKindContentKey       = "journalKind"
	mappedModelRequestIDContentKey    = "modelRequestID"
)

type mappedJournalRecordKind uint8

const (
	mappedJournalRecordUpdate mappedJournalRecordKind = iota
	mappedJournalRecordAssistantTranscript
	mappedJournalRecordToolStreamClosure
	mappedJournalRecordTerminalUsage
	mappedJournalRecordPromptAccepted
	mappedJournalRecordPromptTerminal
)

// MapContext supplies Orka-owned stream metadata for a validated harness v2
// update. Protocol events do not own namespace, task name, or session linkage.
type MapContext struct {
	Namespace   string
	TaskName    string
	SessionName string
	AgentName   string
	StreamID    string
	Provider    string
	Model       string
}

func (c MapContext) normalized() MapContext {
	c.Namespace = strings.TrimSpace(c.Namespace)
	c.TaskName = strings.TrimSpace(c.TaskName)
	c.SessionName = strings.TrimSpace(c.SessionName)
	c.AgentName = strings.TrimSpace(c.AgentName)
	c.StreamID = strings.TrimSpace(c.StreamID)
	c.Provider = strings.TrimSpace(c.Provider)
	c.Model = strings.TrimSpace(c.Model)
	if c.StreamID == "" {
		c.StreamID = c.TaskName
	}
	return c
}

func (c MapContext) validate() error {
	c = c.normalized()
	if c.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if c.TaskName == "" {
		return fmt.Errorf("task name is required")
	}
	if c.StreamID == "" {
		return fmt.Errorf("stream id is required")
	}
	return nil
}

// MappedUpdateIdentity is the safe protocol identity persisted with each
// execution event. RequestDigest is deliberately excluded: the validated
// stream already enforces it, and journaling must not change or expose prompt
// digest/fencing data.
type MappedUpdateIdentity struct {
	Protocol                 string                      `json:"protocol"`
	RuntimeInstanceID        harnessv2.RuntimeInstanceID `json:"runtimeInstanceID"`
	SupervisorBootID         harnessv2.SupervisorBootID  `json:"supervisorBootID"`
	RuntimeSessionUID        harnessv2.RuntimeSessionUID `json:"runtimeSessionUID"`
	RuntimeSessionGeneration uint64                      `json:"runtimeSessionGeneration"`
	TaskUID                  harnessv2.TaskUID           `json:"taskUID"`
	TaskAttempt              uint32                      `json:"taskAttempt"`
	PromptID                 harnessv2.PromptID          `json:"promptID"`
	Sequence                 uint64                      `json:"sequence"`
}

func mappedUpdateIdentity(event harnessv2.Event) MappedUpdateIdentity {
	return MappedUpdateIdentity{
		Protocol:                 event.Protocol,
		RuntimeInstanceID:        event.Identity.RuntimeInstanceID,
		SupervisorBootID:         event.Identity.SupervisorBootID,
		RuntimeSessionUID:        event.Identity.RuntimeSessionUID,
		RuntimeSessionGeneration: event.Identity.RuntimeSessionGeneration,
		TaskUID:                  event.Identity.TaskUID,
		TaskAttempt:              event.Identity.TaskAttempt,
		PromptID:                 event.Identity.PromptID,
		Sequence:                 event.Identity.Sequence,
	}
}

func (i MappedUpdateIdentity) valid() bool {
	return i.promptValid() && i.Sequence > 0
}

func (i MappedUpdateIdentity) promptValid() bool {
	return i.Protocol == harnessv2.ProtocolVersion &&
		strings.TrimSpace(string(i.RuntimeInstanceID)) != "" &&
		strings.TrimSpace(string(i.SupervisorBootID)) != "" &&
		strings.TrimSpace(string(i.RuntimeSessionUID)) != "" &&
		i.RuntimeSessionGeneration > 0 &&
		strings.TrimSpace(string(i.TaskUID)) != "" &&
		i.TaskAttempt > 0 &&
		strings.TrimSpace(string(i.PromptID)) != ""
}

func (i MappedUpdateIdentity) samePrompt(other MappedUpdateIdentity) bool {
	return i.Protocol == other.Protocol &&
		i.RuntimeInstanceID == other.RuntimeInstanceID &&
		i.SupervisorBootID == other.SupervisorBootID &&
		i.RuntimeSessionUID == other.RuntimeSessionUID &&
		i.RuntimeSessionGeneration == other.RuntimeSessionGeneration &&
		i.TaskUID == other.TaskUID &&
		i.TaskAttempt == other.TaskAttempt &&
		i.PromptID == other.PromptID
}

// Key returns the stable recovery-deduplication key for one protocol update.
func (i MappedUpdateIdentity) Key() string {
	return mappedPromptIdentityKey(i) + mappedUpdateIdentityKeySeparator + strconv.FormatUint(i.Sequence, 10)
}

func mappedPromptIdentityKey(identity MappedUpdateIdentity) string {
	return strings.Join([]string{
		identity.Protocol,
		string(identity.RuntimeInstanceID),
		string(identity.SupervisorBootID),
		string(identity.RuntimeSessionUID),
		strconv.FormatUint(identity.RuntimeSessionGeneration, 10),
		string(identity.TaskUID),
		strconv.FormatUint(uint64(identity.TaskAttempt), 10),
		string(identity.PromptID),
	}, mappedUpdateIdentityKeySeparator)
}

// MappedUpdateIdentityFromEvent extracts a persisted harness v2 update
// identity. Events from other producers and malformed/truncated payloads are
// ignored.
func MappedUpdateIdentityFromEvent(event store.ExecutionEvent) (MappedUpdateIdentity, bool) {
	if len(event.Content) == 0 {
		return MappedUpdateIdentity{}, false
	}
	var content struct {
		HarnessV2 MappedUpdateIdentity `json:"harnessV2"`
	}
	if err := json.Unmarshal(event.Content, &content); err != nil || !content.HarnessV2.valid() {
		return MappedUpdateIdentity{}, false
	}
	return content.HarnessV2, true
}

func mappedExecutionEventKey(event store.ExecutionEvent) (MappedUpdateIdentity, string, bool) {
	identity, key, _, ok := mappedExecutionEventRecord(event)
	return identity, key, ok
}

func mappedExecutionEventRecord(
	event store.ExecutionEvent,
) (MappedUpdateIdentity, string, mappedJournalRecordKind, bool) {
	identity, ok := MappedUpdateIdentityFromEvent(event)
	if !ok {
		return MappedUpdateIdentity{}, "", mappedJournalRecordUpdate, false
	}
	var content struct {
		JournalKind string `json:"journalKind"`
		UpdateKind  string `json:"updateKind"`
	}
	if err := json.Unmarshal(event.Content, &content); err != nil {
		return MappedUpdateIdentity{}, "", mappedJournalRecordUpdate, false
	}
	if isMappedToolTerminalEvent(event) {
		kind := mappedJournalRecordUpdate
		if content.JournalKind == mappedToolStreamClosureKind {
			kind = mappedJournalRecordToolStreamClosure
		}
		return identity, mappedToolTerminalKey(identity, event.ToolCallID), kind, true
	}
	if content.JournalKind == mappedToolStreamClosureKind {
		return identity, mappedToolStreamClosureKey(identity), mappedJournalRecordToolStreamClosure, true
	}
	if content.JournalKind == mappedTerminalUsageKind {
		return identity, mappedTerminalUsageKey(identity), mappedJournalRecordTerminalUsage, true
	}
	if content.JournalKind == mappedPromptAcceptedKind {
		return identity, mappedPromptLifecycleKey(identity, mappedPromptAcceptedKind), mappedJournalRecordPromptAccepted, true
	}
	if content.JournalKind == mappedPromptTerminalKind {
		return identity, mappedPromptLifecycleKey(identity, mappedPromptTerminalKind), mappedJournalRecordPromptTerminal, true
	}
	if content.UpdateKind == mappedAssistantTranscriptKind {
		return identity, identity.Key(), mappedJournalRecordAssistantTranscript, true
	}
	return identity, identity.Key(), mappedJournalRecordUpdate, true
}

func mappedToolStreamClosureKey(identity MappedUpdateIdentity) string {
	return identity.Key() + mappedUpdateIdentityKeySeparator + mappedToolStreamClosureKind
}

func mappedToolTerminalKey(identity MappedUpdateIdentity, toolCallID string) string {
	return strings.Join([]string{
		mappedPromptIdentityKey(identity),
		mappedToolTerminalKind,
		strings.TrimSpace(toolCallID),
	}, mappedUpdateIdentityKeySeparator)
}

func isMappedToolTerminalEvent(event store.ExecutionEvent) bool {
	if strings.TrimSpace(event.ToolCallID) == "" {
		return false
	}
	return event.Type == executionevents.ExecutionEventTypeToolCallCompleted ||
		event.Type == executionevents.ExecutionEventTypeToolCallFailed
}

func mappedTerminalUsageKey(identity MappedUpdateIdentity) string {
	return identity.Key() + mappedUpdateIdentityKeySeparator + mappedTerminalUsageKind
}

func mappedPromptLifecycleKey(identity MappedUpdateIdentity, kind string) string {
	return identity.Key() + mappedUpdateIdentityKeySeparator + kind
}

func mappedJournalDedupeKey(key string) string {
	digest := sha256.Sum256([]byte(mappedJournalDedupeKeyDomain + key))
	return mappedJournalDedupeKeyPrefix + hex.EncodeToString(digest[:])
}

// PlanProjection is the durable/public read model derived from one ACP plan
// update.
type PlanProjection struct {
	Summary                    string
	ProgressPct                int
	GoalComplete               bool
	Document                   string
	EventDocument              string
	EventDocumentTruncated     bool
	EventDocumentOriginalChars int
	Total                      int
	Pending                    int
	InProgress                 int
	Completed                  int
}

// ProjectPlanUpdate converts structured ACP plan entries into the existing
// PlanStore contract and a bounded, redacted text representation for events.
func ProjectPlanUpdate(update harnessv2.PlanUpdate) PlanProjection {
	projection, _ := projectPlanUpdate(update, nil, false)
	return projection
}

func projectPlanUpdate(
	update harnessv2.PlanUpdate,
	history []logicalFieldBoundaries,
	historySaturated bool,
) (PlanProjection, []logicalFieldBoundaries) {
	projection := PlanProjection{Total: len(update.Entries)}
	entries, publishedFields := redactPlanEntries(update.Entries, history, historySaturated)
	var inProgressSummary string
	var document strings.Builder
	document.WriteString("# Plan")
	for _, entry := range entries {
		switch entry.Status {
		case harnessv2.PlanEntryCompleted:
			projection.Completed++
		case harnessv2.PlanEntryInProgress:
			projection.InProgress++
			if inProgressSummary == "" {
				inProgressSummary = compactSummary(entry.Content)
			}
		default:
			projection.Pending++
		}
		document.WriteString("\n- ")
		if entry.Status == harnessv2.PlanEntryCompleted {
			document.WriteString("[x] ")
		} else {
			document.WriteString("[ ] ")
		}
		document.WriteString(strings.TrimSpace(entry.Content))
		if entry.Status == harnessv2.PlanEntryInProgress {
			document.WriteString(" _(in progress)_")
		}
		if priority := strings.TrimSpace(entry.Priority); priority != "" {
			document.WriteString(" _(priority: ")
			document.WriteString(priority)
			document.WriteString(")_")
		}
	}
	if projection.Total > 0 {
		projection.ProgressPct = projection.Completed * 100 / projection.Total
		projection.GoalComplete = projection.Completed == projection.Total
	}
	switch {
	case projection.GoalComplete:
		projection.Summary = fmt.Sprintf("Plan complete (%d/%d steps)", projection.Completed, projection.Total)
	case inProgressSummary != "":
		projection.Summary = fmt.Sprintf("Plan in progress (%d/%d complete): %s", projection.Completed, projection.Total, inProgressSummary)
	default:
		projection.Summary = fmt.Sprintf("Plan updated (%d/%d steps complete)", projection.Completed, projection.Total)
	}
	projection.Summary, _, _ = executionevents.RedactAndTruncateExecutionEventText(
		projection.Summary, executionevents.MaxExecutionEventSummaryChars,
	)
	projection.Document = executionevents.RedactExecutionEventText(document.String())
	projection.EventDocument, projection.EventDocumentTruncated, projection.EventDocumentOriginalChars =
		executionevents.RedactAndTruncateExecutionEventText(projection.Document, executionevents.MaxExecutionEventContentTextChars)
	return projection, publishedFields
}

func redactPlanEntries(
	entries []harnessv2.PlanEntry,
	history []logicalFieldBoundaries,
	historySaturated bool,
) ([]harnessv2.PlanEntry, []logicalFieldBoundaries) {
	redacted := append([]harnessv2.PlanEntry(nil), entries...)
	values := make([]string, 0, len(entries)*2)
	for _, entry := range entries {
		values = append(values, entry.Content, entry.Priority)
	}
	values, publishedFields := redactLogicalFieldsWithHistory(history, historySaturated, values...)
	for index := range redacted {
		redacted[index].Content = values[index*2]
		redacted[index].Priority = values[index*2+1]
	}
	return redacted, publishedFields
}

type diagnosticProjection struct {
	code    string
	message string
}

type toolProjection struct {
	title       string
	kind        string
	contentText string
}

func projectDiagnosticUpdate(
	update harnessv2.DiagnosticUpdate,
	history []logicalFieldBoundaries,
	historySaturated bool,
) (diagnosticProjection, []logicalFieldBoundaries) {
	values, publishedFields := redactLogicalFieldsWithHistory(
		history, historySaturated, update.Code, update.Message,
	)
	return diagnosticProjection{code: values[0], message: values[1]}, publishedFields
}

func projectToolUpdate(
	tool harnessv2.ToolCallUpdate,
	history []logicalFieldBoundaries,
	historySaturated bool,
	contentText *string,
) (toolProjection, []logicalFieldBoundaries) {
	values := []string{tool.Title, tool.Kind}
	if contentText != nil {
		values = append(values, *contentText)
	}
	values, publishedFields := redactLogicalFieldsWithHistory(history, historySaturated, values...)
	projection := toolProjection{title: values[0], kind: values[1]}
	if contentText != nil {
		projection.contentText = values[2]
	}
	return projection, publishedFields
}

func redactLogicalFieldsWithHistory(
	history []logicalFieldBoundaries,
	historySaturated bool,
	values ...string,
) ([]string, []logicalFieldBoundaries) {
	redacted := make([]string, len(values))
	current := make([]logicalFieldBoundaries, 0, len(values))
	for index, value := range values {
		redacted[index] = executionevents.RedactExecutionEventText(value)
		if historySaturated {
			if redacted[index] != "" {
				redacted[index] = executionevents.ExecutionEventRedactedValue
			}
			continue
		}
		current = appendLogicalFieldBoundary(current, redacted[index])
	}
	if historySaturated {
		return redacted, nil
	}
	fields := make([]logicalFieldBoundaries, 0, len(history)+len(current))
	fields = append(fields, history...)
	fields = append(fields, current...)
	if len(fields) >= 2 && permutedLogicalFieldSubsetsSensitive(fields) {
		for index, value := range redacted {
			if value != "" {
				redacted[index] = executionevents.ExecutionEventRedactedValue
			}
		}
		return redacted, nil
	}
	return redacted, current
}

const (
	maxLogicalFieldBoundaryRunes       = 256
	maxLogicalFieldSubsetCandidates    = 4096
	maxLogicalFieldPermutationFields   = 256
	maxLogicalFieldPermutationBitWords = maxLogicalFieldPermutationFields / 64
)

type logicalFieldBoundaries struct {
	prefix string
	suffix string
	whole  bool
}

type logicalFieldPermutationCandidate struct {
	suffix string
	used   [maxLogicalFieldPermutationBitWords]uint64
}

type logicalFieldSensitiveMarkerState struct {
	marker  int
	matched int
}

var logicalFieldSensitiveMarkers = []string{
	"authorization",
	"txn-token",
	"transaction-token",
	"cookie",
	"set-cookie",
	"api-key",
	"api_key",
	"apikey",
	"api key",
	"token",
	"secret",
	"password",
	"passwd",
	"pwd",
	"credential",
	"private-key",
	"private_key",
	"private key",
	"sk-",
	"ghp_",
	"gho_",
	"ghu_",
	"ghs_",
	"ghr_",
	"github_pat_",
	"xoxb-",
	"xoxa-",
	"xoxp-",
	"xoxr-",
	"xoxs-",
	"eyj",
	"://",
	"//",
	"?",
	"#",
}

func appendLogicalFieldBoundary(fields []logicalFieldBoundaries, value string) []logicalFieldBoundaries {
	if value == "" || value == executionevents.ExecutionEventRedactedValue {
		return fields
	}
	runes := []rune(value)
	if len(runes) <= maxLogicalFieldBoundaryRunes {
		return append(fields, logicalFieldBoundaries{prefix: value, suffix: value, whole: true})
	}
	return append(fields, logicalFieldBoundaries{
		prefix: string(runes[:maxLogicalFieldBoundaryRunes]),
		suffix: string(runes[len(runes)-maxLogicalFieldBoundaryRunes:]),
	})
}

func logicalFieldsMayReconstructSensitiveMarker(fields []logicalFieldBoundaries) bool {
	states := make([]logicalFieldSensitiveMarkerState, 0)
	seen := make(map[logicalFieldSensitiveMarkerState]struct{})
	for markerIndex, marker := range logicalFieldSensitiveMarkers {
		for _, field := range fields {
			text := strings.ToLower(field.suffix)
			if strings.Contains(text, marker) {
				return true
			}
			for matched := 1; matched < len(marker) && matched <= len(text); matched++ {
				if !strings.HasSuffix(text, marker[:matched]) {
					continue
				}
				state := logicalFieldSensitiveMarkerState{marker: markerIndex, matched: matched}
				if _, exists := seen[state]; exists {
					continue
				}
				seen[state] = struct{}{}
				states = append(states, state)
			}
		}
	}
	for cursor := 0; cursor < len(states); cursor++ {
		state := states[cursor]
		marker := logicalFieldSensitiveMarkers[state.marker]
		remaining := marker[state.matched:]
		for _, field := range fields {
			text := strings.ToLower(field.prefix)
			if strings.HasPrefix(text, remaining) {
				return true
			}
			if !field.whole || !strings.HasPrefix(remaining, text) {
				continue
			}
			next := logicalFieldSensitiveMarkerState{marker: state.marker, matched: state.matched + len(text)}
			if _, exists := seen[next]; exists {
				continue
			}
			seen[next] = struct{}{}
			states = append(states, next)
		}
	}
	return false
}

func permutedLogicalFieldSubsetsSensitive(fields []logicalFieldBoundaries) bool {
	// Exact permutation tracking is bounded. Histories can contain one full
	// 128-entry plan plus a later update, so exceeding the bitset width is not
	// itself evidence of sensitive content. Fall back to the conservative marker
	// reachability check used when the candidate work cap is exhausted.
	if len(fields) > maxLogicalFieldPermutationFields {
		return logicalFieldsMayReconstructSensitiveMarker(fields)
	}
	candidates := make([]logicalFieldPermutationCandidate, 0, min(len(fields), maxLogicalFieldSubsetCandidates))
	seen := make(map[logicalFieldPermutationCandidate]struct{}, min(len(fields), maxLogicalFieldSubsetCandidates))
	for index, field := range fields {
		candidate := logicalFieldPermutationCandidate{suffix: field.suffix}
		candidate.used[index/64] = uint64(1) << uint(index%64)
		if _, exists := seen[candidate]; exists {
			continue
		}
		seen[candidate] = struct{}{}
		candidates = append(candidates, candidate)
	}
	for cursor := 0; cursor < len(candidates); cursor++ {
		candidate := candidates[cursor]
		for index, field := range fields {
			word := index / 64
			bit := uint64(1) << uint(index%64)
			if candidate.used[word]&bit != 0 {
				continue
			}
			joined := candidate.suffix + field.prefix
			if executionevents.RedactExecutionEventText(joined) != joined {
				return true
			}
			next := candidate
			next.used[word] |= bit
			if field.whole {
				next.suffix = logicalFieldSuffix(joined)
			} else {
				next.suffix = field.suffix
			}
			if _, exists := seen[next]; exists {
				continue
			}
			if len(seen) >= maxLogicalFieldSubsetCandidates {
				return logicalFieldsMayReconstructSensitiveMarker(fields)
			}
			seen[next] = struct{}{}
			candidates = append(candidates, next)
		}
	}
	return false
}

func logicalFieldSuffix(value string) string {
	runes := []rune(value)
	if len(runes) <= maxLogicalFieldBoundaryRunes {
		return value
	}
	return string(runes[len(runes)-maxLogicalFieldBoundaryRunes:])
}

type mapUpdateOptions struct {
	toolContentText                  *string
	toolContentTruncated             bool
	toolContentMultipleBlocksOmitted bool
	omitToolMetadata                 bool
	toolProjection                   *toolProjection
	planProjection                   *PlanProjection
	diagnosticProjection             *diagnosticProjection
	journalKind                      string
}

const (
	toolContentMultipleBlocksOmittedReason = "streamed_text_multiple_blocks_omitted"
	streamedTextTruncatedOrOmittedReason   = "streamed_text_truncated_or_omitted"
	assistantResponseOmittedSummary        = "Assistant response omitted"
	recoveredToolOutcomeUnknownSummary     = "Tool call outcome unknown"
)

// mapUpdate maps one validated harness v2 update to the public execution-event
// taxonomy. Streamed assistant and tool text is deliberately omitted unless
// options supply it: independently redacting chunks can leak credentials split
// across update boundaries. Journal state supplies tool text only after the
// logical stream is complete, and assistant text is persisted from the
// terminal transcript.
func mapUpdate(event harnessv2.Event, mapCtx MapContext, options mapUpdateOptions) (*store.ExecutionEvent, error) {
	if err := mapCtx.validate(); err != nil {
		return nil, err
	}
	mapCtx = mapCtx.normalized()
	if event.Protocol != harnessv2.ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol %q", event.Protocol)
	}
	if event.Type != harnessv2.EventUpdate || event.Update == nil {
		return nil, fmt.Errorf("harness v2 update event is required")
	}
	if err := event.Identity.Validate(); err != nil {
		return nil, fmt.Errorf("invalid update identity: %w", err)
	}
	if err := event.Update.Validate(); err != nil {
		return nil, fmt.Errorf("invalid update payload: %w", err)
	}

	content := map[string]any{
		mappedHarnessV2ContentKey:  mappedUpdateIdentity(event),
		mappedUpdateKindContentKey: event.Update.Kind,
	}
	if options.journalKind != "" {
		content[mappedJournalKindContentKey] = options.journalKind
	}
	mapped := &store.ExecutionEvent{
		Namespace:   mapCtx.Namespace,
		StreamType:  store.ExecutionEventStreamTypeTask,
		StreamID:    mapCtx.StreamID,
		Severity:    executionevents.ExecutionEventSeverityInfo,
		TaskName:    mapCtx.TaskName,
		SessionName: mapCtx.SessionName,
		AgentName:   mapCtx.AgentName,
		CreatedAt:   event.Identity.Timestamp.UTC(),
	}

	switch event.Update.Kind {
	case harnessv2.UpdateAssistantMessageChunk:
		mapped.Type = executionevents.ExecutionEventTypeModelMessage
		mapped.Summary = "Assistant message streamed"
		content["contentOmitted"] = "streamed_text_pending_terminal_redaction"
	case harnessv2.UpdateToolCall, harnessv2.UpdateToolCallUpdate:
		tool := event.Update.ToolCall
		mapped.ToolCallID = safeMappedToolCallID(tool.ToolCallID)
		mapped.Type, mapped.Severity = toolCallEventType(tool.Status)
		if options.omitToolMetadata {
			metadataFree := *tool
			metadataFree.Title = ""
			metadataFree.Kind = ""
			mapped.Summary = toolCallSummary(metadataFree)
			content["metadataOmitted"] = "streamed_metadata_pending_completion_redaction"
		} else {
			projection, _ := projectToolUpdate(*tool, nil, false, options.toolContentText)
			if options.toolProjection != nil {
				projection = *options.toolProjection
			}
			redactedTool := *tool
			redactedTool.Title = projection.title
			redactedTool.Kind = projection.kind
			mapped.ToolName = strings.TrimSpace(projection.kind)
			mapped.ToolName, _, _ = executionevents.RedactAndTruncateExecutionEventText(mapped.ToolName, 128)
			mapped.Summary = toolCallSummary(redactedTool)
			if options.toolContentText != nil && strings.TrimSpace(projection.title) == "" {
				if summary := compactSummary(projection.contentText); summary != "" {
					mapped.Summary = summary
				}
			}
			content["title"] = projection.title
			content["toolKind"] = projection.kind
			if options.toolContentText != nil {
				mapped.ContentText = projection.contentText
			}
		}
		content["toolCallID"] = mapped.ToolCallID
		content["status"] = tool.Status
		content["contentBlockCount"] = len(tool.Content)
		if options.toolContentText == nil && len(tool.Content) > 0 {
			content["contentOmitted"] = "streamed_text_pending_completion_redaction"
		}
		if options.toolContentTruncated || tool.ContentOmitted {
			mapped.ContentText = ""
			mapped.Truncation = &executionevents.ExecutionEventTruncation{ContentTextTruncated: true}
			content["contentOmitted"] = streamedTextTruncatedOrOmittedReason
		} else if options.toolContentMultipleBlocksOmitted {
			content["contentOmitted"] = toolContentMultipleBlocksOmittedReason
		}
	case harnessv2.UpdatePlan:
		projection := ProjectPlanUpdate(*event.Update.Plan)
		if options.planProjection != nil {
			projection = *options.planProjection
		}
		mapped.Type = executionevents.ExecutionEventTypePlanUpdated
		mapped.Summary = projection.Summary
		mapped.ContentText = projection.EventDocument
		content["totalEntries"] = projection.Total
		content["pendingEntries"] = projection.Pending
		content["inProgressEntries"] = projection.InProgress
		content["completedEntries"] = projection.Completed
		content["progressPct"] = projection.ProgressPct
		content["goalComplete"] = projection.GoalComplete
		if projection.EventDocumentTruncated {
			mapped.Truncation = &executionevents.ExecutionEventTruncation{
				ContentTextTruncated:     true,
				ContentTextOriginalChars: projection.EventDocumentOriginalChars,
			}
		}
	case harnessv2.UpdateUsage:
		mapUsageUpdate(event.Update.Usage, mapCtx, mapped, content)
	case harnessv2.UpdateDiagnostic:
		diagnostic := event.Update.Diagnostic
		projection, _ := projectDiagnosticUpdate(*diagnostic, nil, false)
		if options.diagnosticProjection != nil {
			projection = *options.diagnosticProjection
		}
		mapped.Type = executionevents.ExecutionEventTypeAgentRuntimeCommandStarted
		mapped.Severity = executionevents.ExecutionEventSeverityError
		if diagnostic.Retryable {
			mapped.Severity = executionevents.ExecutionEventSeverityWarning
		}
		mapped.Summary = compactSummary(projection.code + ": " + projection.message)
		mapped.ContentText = projection.message
		content["code"] = projection.code
		content["retryable"] = diagnostic.Retryable
	default:
		return nil, fmt.Errorf("unsupported harness v2 update kind %q", event.Update.Kind)
	}

	encoded, err := json.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("marshal mapped harness v2 update: %w", err)
	}
	mapped.Content = encoded
	if err := store.SanitizeExecutionEventPayloadFields(mapped); err != nil {
		return nil, fmt.Errorf("sanitize mapped harness v2 update: %w", err)
	}
	return mapped, nil
}

func mapUsageUpdate(
	usage *harnessv2.UsageUpdate,
	mapCtx MapContext,
	mapped *store.ExecutionEvent,
	content map[string]any,
) {
	hasTokenUsage := usage.InputTokens > 0 || usage.OutputTokens > 0 || usage.CachedInputTokens > 0
	hasContextWindow := usage.ContextWindowUsed != nil
	if hasTokenUsage || !hasContextWindow {
		mapped.Type = executionevents.ExecutionEventTypeModelUsageUpdated
		mapped.Summary = fmt.Sprintf(
			"Model usage updated: %d input, %d output, %d cached input tokens",
			usage.InputTokens, usage.OutputTokens, usage.CachedInputTokens,
		)
		content["inputTokens"] = usage.InputTokens
		content["outputTokens"] = usage.OutputTokens
		content["cachedInputTokens"] = usage.CachedInputTokens
	} else {
		mapped.Type = executionevents.ExecutionEventTypeModelContextUpdated
		mapped.Summary = fmt.Sprintf(
			"Model context updated: %d of %d tokens used",
			*usage.ContextWindowUsed, *usage.ContextWindowSize,
		)
	}
	if usage.ContextWindowUsed != nil {
		content["contextWindowUsed"] = *usage.ContextWindowUsed
		content["contextWindowSize"] = *usage.ContextWindowSize
	}
	if mapCtx.Provider != "" {
		content["provider"] = mapCtx.Provider
	}
	if mapCtx.Model != "" {
		content["model"] = mapCtx.Model
	}
}

func hasUsageTelemetry(usage harnessv2.UsageUpdate) bool {
	return usage.InputTokens > 0 || usage.OutputTokens > 0 || usage.CachedInputTokens > 0 ||
		usage.ContextWindowUsed != nil || usage.ContextWindowSize != nil
}

func mapToolUpdateWithHistory(
	event harnessv2.Event,
	mapCtx MapContext,
	contentText *string,
	contentTruncated bool,
	contentMultipleBlocksOmitted bool,
	journalKind string,
	history []logicalFieldBoundaries,
	historySaturated bool,
) (*store.ExecutionEvent, []logicalFieldBoundaries, error) {
	if event.Update == nil || event.Update.ToolCall == nil {
		return nil, nil, fmt.Errorf("harness v2 tool update is required")
	}
	projection, publishedFields := projectToolUpdate(
		*event.Update.ToolCall, history, historySaturated, contentText,
	)
	mapped, err := mapUpdate(event, mapCtx, mapUpdateOptions{
		toolContentText:                  contentText,
		toolContentTruncated:             contentTruncated,
		toolContentMultipleBlocksOmitted: contentMultipleBlocksOmitted,
		toolProjection:                   &projection,
		journalKind:                      journalKind,
	})
	return mapped, publishedFields, err
}

func mapRecoveredToolStreamClosure(
	identity MappedUpdateIdentity,
	started store.ExecutionEvent,
	at time.Time,
	mapCtx MapContext,
) (*store.ExecutionEvent, error) {
	if err := mapCtx.validate(); err != nil {
		return nil, err
	}
	mapCtx = mapCtx.normalized()
	if !identity.valid() {
		return nil, fmt.Errorf("valid harness v2 tool identity is required")
	}
	if started.Type != executionevents.ExecutionEventTypeToolCallStarted || strings.TrimSpace(started.ToolCallID) == "" {
		return nil, fmt.Errorf("persisted open tool lifecycle is required")
	}
	if at.IsZero() {
		return nil, fmt.Errorf("tool recovery timestamp is required")
	}
	content, err := json.Marshal(map[string]any{
		mappedHarnessV2ContentKey:      identity,
		mappedUpdateKindContentKey:     harnessv2.UpdateToolCallUpdate,
		mappedJournalKindContentKey:    mappedToolStreamClosureKind,
		"toolCallID":                   started.ToolCallID,
		"status":                       harnessv2.ToolCallStatusFailed,
		"outcome":                      harnessv2.EventOutcomeUnknown,
		mappedControllerSynthesizedKey: true,
		"contentOmitted":               streamedTextTruncatedOrOmittedReason,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal recovered harness v2 tool closure: %w", err)
	}
	mapped := &store.ExecutionEvent{
		Namespace:   mapCtx.Namespace,
		StreamType:  store.ExecutionEventStreamTypeTask,
		StreamID:    mapCtx.StreamID,
		Type:        executionevents.ExecutionEventTypeToolCallFailed,
		Severity:    executionevents.ExecutionEventSeverityError,
		TaskName:    mapCtx.TaskName,
		SessionName: started.SessionName,
		AgentName:   started.AgentName,
		ToolName:    started.ToolName,
		ToolCallID:  started.ToolCallID,
		Summary:     recoveredToolOutcomeUnknownSummary,
		Content:     content,
		Truncation:  &executionevents.ExecutionEventTruncation{ContentTextTruncated: true},
		CreatedAt:   at.UTC(),
	}
	if err := store.SanitizeExecutionEventPayloadFields(mapped); err != nil {
		return nil, fmt.Errorf("sanitize recovered harness v2 tool closure: %w", err)
	}
	return mapped, nil
}

func mapToolUpdateWithoutMetadata(event harnessv2.Event, mapCtx MapContext) (*store.ExecutionEvent, error) {
	return mapUpdate(event, mapCtx, mapUpdateOptions{omitToolMetadata: true})
}

// mapTerminalUsageWithHistory projects usage reported only by a completed
// result while retaining the terminal event's durable protocol identity.
func mapTerminalUsageWithHistory(
	event harnessv2.Event,
	mapCtx MapContext,
	history []logicalFieldBoundaries,
	historySaturated bool,
) (*store.ExecutionEvent, []logicalFieldBoundaries, error) {
	if event.Type != harnessv2.EventCompleted || event.Completed == nil {
		return nil, nil, fmt.Errorf("completed harness v2 event is required")
	}
	usage := event.Completed.Result.Usage
	if !hasUsageTelemetry(usage) {
		return nil, nil, fmt.Errorf("completed harness v2 usage is required")
	}
	if err := usage.Validate(); err != nil {
		return nil, nil, fmt.Errorf("invalid completed usage: %w", err)
	}
	var publishedFields []logicalFieldBoundaries
	if event.Completed.Result.Model != "" {
		fields, published := redactLogicalFieldsWithHistory(
			history, historySaturated, event.Completed.Result.Model,
		)
		mapCtx.Model = fields[0]
		publishedFields = published
	}
	update := event
	update.Type = harnessv2.EventUpdate
	update.Completed = nil
	update.Update = &harnessv2.UpdateEvent{Kind: harnessv2.UpdateUsage, Usage: &usage}
	mapped, err := mapUpdate(update, mapCtx, mapUpdateOptions{journalKind: mappedTerminalUsageKind})
	return mapped, publishedFields, err
}

// MapPromptLifecycle maps prompt acceptance and settlement into the existing
// model-request lifecycle taxonomy used by task traces and UI execution graphs.
// Journal state uses mapPromptLifecycleWithHistory directly; this entry point
// serves callers outside the package that need a mapped lifecycle event
// without journal deduplication.
func MapPromptLifecycle(event harnessv2.Event, mapCtx MapContext) (*store.ExecutionEvent, error) {
	mapped, _, err := mapPromptLifecycleWithHistory(event, mapCtx, nil, false)
	return mapped, err
}

func mapPromptLifecycleWithHistory(
	event harnessv2.Event,
	mapCtx MapContext,
	history []logicalFieldBoundaries,
	historySaturated bool,
) (*store.ExecutionEvent, []logicalFieldBoundaries, error) {
	if err := mapCtx.validate(); err != nil {
		return nil, nil, err
	}
	mapCtx = mapCtx.normalized()
	if event.Protocol != harnessv2.ProtocolVersion {
		return nil, nil, fmt.Errorf("unsupported protocol %q", event.Protocol)
	}
	if err := event.Identity.Validate(); err != nil {
		return nil, nil, fmt.Errorf("invalid lifecycle identity: %w", err)
	}
	content := map[string]any{
		mappedHarnessV2ContentKey:      mappedUpdateIdentity(event),
		mappedModelRequestIDContentKey: string(event.Identity.PromptID),
	}
	mapped := &store.ExecutionEvent{
		Namespace:   mapCtx.Namespace,
		StreamType:  store.ExecutionEventStreamTypeTask,
		StreamID:    mapCtx.StreamID,
		Severity:    executionevents.ExecutionEventSeverityInfo,
		TaskName:    mapCtx.TaskName,
		SessionName: mapCtx.SessionName,
		AgentName:   mapCtx.AgentName,
		CreatedAt:   event.Identity.Timestamp.UTC(),
	}
	model := mapCtx.Model
	var publishedFields []logicalFieldBoundaries
	switch event.Type {
	case harnessv2.EventAccepted:
		if event.Accepted == nil {
			return nil, nil, fmt.Errorf("accepted harness v2 event is required")
		}
		if err := event.Accepted.Validate(); err != nil {
			return nil, nil, fmt.Errorf("invalid accepted payload: %w", err)
		}
		if model != "" {
			fields, published := redactLogicalFieldsWithHistory(
				history, historySaturated, model,
			)
			model = fields[0]
			publishedFields = published
		}
		content[mappedJournalKindContentKey] = mappedPromptAcceptedKind
		content["acceptedAt"] = event.Accepted.AcceptedAt.UTC()
		content["acpVersion"] = event.Accepted.ACPVersion
		mapped.Type = executionevents.ExecutionEventTypeModelRequestStarted
		mapped.Summary = "Model request started"
	case harnessv2.EventCompleted:
		if event.Completed == nil {
			return nil, nil, fmt.Errorf("completed harness v2 event is required")
		}
		if err := event.Completed.Validate(); err != nil {
			return nil, nil, fmt.Errorf("invalid completed payload: %w", err)
		}
		if event.Completed.Result.Model != "" {
			fields, published := redactLogicalFieldsWithHistory(
				history, historySaturated, event.Completed.Result.Model,
			)
			model = fields[0]
			publishedFields = published
		}
		content[mappedJournalKindContentKey] = mappedPromptTerminalKind
		content["terminalEvent"] = event.Type
		content["stopReason"] = event.Completed.StopReason
		mapped.Type = executionevents.ExecutionEventTypeModelRequestCompleted
		mapped.Summary = "Model request completed"
	case harnessv2.EventCancelled:
		if event.Cancelled == nil {
			return nil, nil, fmt.Errorf("cancelled harness v2 event is required")
		}
		if err := event.Cancelled.Validate(); err != nil {
			return nil, nil, fmt.Errorf("invalid cancelled payload: %w", err)
		}
		fields, published := redactLogicalFieldsWithHistory(
			history, historySaturated, event.Cancelled.Reason,
		)
		publishedFields = published
		content[mappedJournalKindContentKey] = mappedPromptTerminalKind
		content["terminalEvent"] = event.Type
		content["stopReason"] = event.Cancelled.StopReason
		content["reason"] = fields[0]
		mapped.Type = executionevents.ExecutionEventTypeModelRequestFailed
		mapped.Severity = executionevents.ExecutionEventSeverityWarning
		mapped.Summary = "Model request cancelled"
	case harnessv2.EventFailed:
		if event.Failed == nil {
			return nil, nil, fmt.Errorf("failed harness v2 event is required")
		}
		if err := event.Failed.Validate(); err != nil {
			return nil, nil, fmt.Errorf("invalid failed payload: %w", err)
		}
		fields, published := redactLogicalFieldsWithHistory(
			history, historySaturated, event.Failed.Code, event.Failed.Message,
		)
		publishedFields = published
		content[mappedJournalKindContentKey] = mappedPromptTerminalKind
		content["terminalEvent"] = event.Type
		content["stopReason"] = event.Failed.StopReason
		content["code"] = fields[0]
		content["message"] = fields[1]
		mapped.Type = executionevents.ExecutionEventTypeModelRequestFailed
		mapped.Severity = executionevents.ExecutionEventSeverityError
		mapped.Summary = compactSummary(fields[0] + ": " + fields[1])
	case harnessv2.EventOutcomeUnknown:
		if event.OutcomeUnknown == nil {
			return nil, nil, fmt.Errorf("outcome_unknown harness v2 event is required")
		}
		if err := event.OutcomeUnknown.Validate(); err != nil {
			return nil, nil, fmt.Errorf("invalid outcome_unknown payload: %w", err)
		}
		fields, published := redactLogicalFieldsWithHistory(
			history, historySaturated, event.OutcomeUnknown.Code, event.OutcomeUnknown.Message,
		)
		publishedFields = published
		content[mappedJournalKindContentKey] = mappedPromptTerminalKind
		content["terminalEvent"] = event.Type
		content["stopReason"] = event.Type
		content["code"] = fields[0]
		content["message"] = fields[1]
		content["forcedTermination"] = event.OutcomeUnknown.ForcedTermination
		mapped.Type = executionevents.ExecutionEventTypeModelRequestFailed
		mapped.Severity = executionevents.ExecutionEventSeverityError
		mapped.Summary = compactSummary(fields[0] + ": " + fields[1])
	default:
		return nil, nil, fmt.Errorf("accepted or terminal harness v2 event is required")
	}
	if mapCtx.Provider != "" {
		content["provider"] = mapCtx.Provider
	}
	if model != "" {
		content["model"] = model
	}
	encoded, err := json.Marshal(content)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal mapped harness v2 lifecycle: %w", err)
	}
	mapped.Content = encoded
	if err := store.SanitizeExecutionEventPayloadFields(mapped); err != nil {
		return nil, nil, fmt.Errorf("sanitize mapped harness v2 lifecycle: %w", err)
	}
	return mapped, publishedFields, nil
}

// mapPromptStreamFailure maps a controller-observed stream failure into a
// terminal model-request lifecycle event. It deliberately uses only the safe
// mapped prompt identity because controller-synthesized events do not own a
// runtime request digest or wire-protocol event payload.
func mapPromptStreamFailure(
	identity MappedUpdateIdentity,
	at time.Time,
	mapCtx MapContext,
	diagnostic string,
	history []logicalFieldBoundaries,
	historySaturated bool,
) (*store.ExecutionEvent, []logicalFieldBoundaries, error) {
	if err := mapCtx.validate(); err != nil {
		return nil, nil, err
	}
	mapCtx = mapCtx.normalized()
	if !identity.valid() {
		return nil, nil, fmt.Errorf("valid harness v2 prompt identity is required")
	}
	if at.IsZero() {
		return nil, nil, fmt.Errorf("prompt stream failure timestamp is required")
	}
	fields, publishedFields := redactLogicalFieldsWithHistory(
		history, historySaturated, mappedPromptStreamFailureCode, diagnostic,
	)
	content := map[string]any{
		mappedHarnessV2ContentKey:      identity,
		mappedModelRequestIDContentKey: string(identity.PromptID),
		mappedJournalKindContentKey:    mappedPromptTerminalKind,
		"terminalEvent":                harnessv2.EventOutcomeUnknown,
		mappedControllerSynthesizedKey: true,
		"code":                         fields[0],
		"message":                      fields[1],
	}
	if mapCtx.Provider != "" {
		content["provider"] = mapCtx.Provider
	}
	if mapCtx.Model != "" {
		content["model"] = mapCtx.Model
	}
	encoded, err := json.Marshal(content)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal mapped harness v2 prompt stream failure: %w", err)
	}
	mapped := &store.ExecutionEvent{
		Namespace:   mapCtx.Namespace,
		StreamType:  store.ExecutionEventStreamTypeTask,
		StreamID:    mapCtx.StreamID,
		Type:        executionevents.ExecutionEventTypeModelRequestFailed,
		Severity:    executionevents.ExecutionEventSeverityError,
		TaskName:    mapCtx.TaskName,
		SessionName: mapCtx.SessionName,
		AgentName:   mapCtx.AgentName,
		Summary:     compactSummary(fields[0] + ": " + fields[1]),
		Content:     encoded,
		CreatedAt:   at.UTC(),
	}
	if err := store.SanitizeExecutionEventPayloadFields(mapped); err != nil {
		return nil, nil, fmt.Errorf("sanitize mapped harness v2 prompt stream failure: %w", err)
	}
	return mapped, publishedFields, nil
}

// mapPromptSettlement maps a proven settlement into the prompt lifecycle
// taxonomy when the terminal stream event was unavailable.
func mapPromptSettlement(
	identity MappedUpdateIdentity,
	settlement harnessv2.PromptSettlement,
	cancellationReason harnessv2.CancelReason,
	mapCtx MapContext,
) (*store.ExecutionEvent, error) {
	if err := mapCtx.validate(); err != nil {
		return nil, err
	}
	mapCtx = mapCtx.normalized()
	if !identity.valid() {
		return nil, fmt.Errorf("valid harness v2 prompt identity is required")
	}
	if err := settlement.Validate(); err != nil {
		return nil, fmt.Errorf("invalid prompt settlement: %w", err)
	}
	if cancellationReason != "" && !validPromptCancellationReason(cancellationReason) {
		return nil, fmt.Errorf("invalid prompt cancellation reason %q", cancellationReason)
	}
	content := map[string]any{
		mappedHarnessV2ContentKey:      identity,
		mappedModelRequestIDContentKey: string(identity.PromptID),
		mappedJournalKindContentKey:    mappedPromptTerminalKind,
		"terminalEvent":                settlement.TerminalEvent,
		"outcome":                      settlement.Outcome,
		"stopReason":                   settlement.StopReason,
		"settledAt":                    settlement.SettledAt.UTC(),
		mappedControllerSynthesizedKey: true,
		"settlementProven":             true,
	}
	if cancellationReason != "" {
		content["cancellationReason"] = cancellationReason
	}
	mapped := &store.ExecutionEvent{
		Namespace:   mapCtx.Namespace,
		StreamType:  store.ExecutionEventStreamTypeTask,
		StreamID:    mapCtx.StreamID,
		Severity:    executionevents.ExecutionEventSeverityInfo,
		TaskName:    mapCtx.TaskName,
		SessionName: mapCtx.SessionName,
		AgentName:   mapCtx.AgentName,
		CreatedAt:   settlement.SettledAt.UTC(),
	}
	switch settlement.TerminalEvent {
	case harnessv2.EventCompleted:
		mapped.Type = executionevents.ExecutionEventTypeModelRequestCompleted
		mapped.Summary = "Model request completed"
	case harnessv2.EventCancelled:
		content["reason"] = "prompt cancellation settled"
		mapped.Type = executionevents.ExecutionEventTypeModelRequestFailed
		mapped.Severity = executionevents.ExecutionEventSeverityWarning
		mapped.Summary = "Model request cancelled"
	case harnessv2.EventFailed:
		content["code"] = mappedPromptSettlementFailureCode
		content["message"] = "runtime reported prompt failure during cancellation"
		mapped.Type = executionevents.ExecutionEventTypeModelRequestFailed
		mapped.Severity = executionevents.ExecutionEventSeverityError
		mapped.Summary = "Model request failed during cancellation"
	case harnessv2.EventOutcomeUnknown:
		content["code"] = mappedPromptSettlementUnknownCode
		content["message"] = "runtime reported an unknown prompt outcome during cancellation"
		mapped.Type = executionevents.ExecutionEventTypeModelRequestFailed
		mapped.Severity = executionevents.ExecutionEventSeverityError
		mapped.Summary = "Model request outcome unknown"
	default:
		return nil, fmt.Errorf("unsupported prompt settlement terminal event %q", settlement.TerminalEvent)
	}
	if mapCtx.Provider != "" {
		content["provider"] = mapCtx.Provider
	}
	if mapCtx.Model != "" {
		content["model"] = mapCtx.Model
	}
	encoded, err := json.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("marshal mapped harness v2 prompt settlement: %w", err)
	}
	mapped.Content = encoded
	if err := store.SanitizeExecutionEventPayloadFields(mapped); err != nil {
		return nil, fmt.Errorf("sanitize mapped harness v2 prompt settlement: %w", err)
	}
	return mapped, nil
}

func validPromptCancellationReason(reason harnessv2.CancelReason) bool {
	switch reason {
	case harnessv2.CancelReasonUserRequested, harnessv2.CancelReasonTaskTimeout,
		harnessv2.CancelReasonLeaseExpired, harnessv2.CancelReasonStreamDisconnected,
		harnessv2.CancelReasonControllerShutdown:
		return true
	default:
		return false
	}
}

// MapAssistantTranscript maps the complete terminal assistant transcript as a
// single event so redaction sees credential shapes spanning protocol chunks.
func MapAssistantTranscript(
	event harnessv2.Event,
	mapCtx MapContext,
	transcript string,
	contentOmitted bool,
) (*store.ExecutionEvent, error) {
	if err := mapCtx.validate(); err != nil {
		return nil, err
	}
	mapCtx = mapCtx.normalized()
	if event.Protocol != harnessv2.ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol %q", event.Protocol)
	}
	if !event.Type.IsTerminal() &&
		(event.Type != harnessv2.EventUpdate || event.Update == nil || event.Update.AssistantMessage == nil) {
		return nil, fmt.Errorf("terminal or assistant-update harness v2 event is required")
	}
	if err := event.Identity.Validate(); err != nil {
		return nil, fmt.Errorf("invalid terminal identity: %w", err)
	}
	if transcript == "" && !contentOmitted {
		return nil, fmt.Errorf("assistant transcript is required")
	}
	if contentOmitted {
		// Never retain a prefix once the complete logical stream exceeded its
		// bound; a credential may span the discarded cutoff.
		transcript = ""
	}

	contentBody := map[string]any{
		mappedHarnessV2ContentKey:  mappedUpdateIdentity(event),
		mappedUpdateKindContentKey: mappedAssistantTranscriptKind,
	}
	if contentOmitted {
		contentBody["contentOmitted"] = streamedTextTruncatedOrOmittedReason
	}
	content, err := json.Marshal(contentBody)
	if err != nil {
		return nil, fmt.Errorf("marshal mapped harness v2 assistant transcript: %w", err)
	}
	mapped := &store.ExecutionEvent{
		Namespace:   mapCtx.Namespace,
		StreamType:  store.ExecutionEventStreamTypeTask,
		StreamID:    mapCtx.StreamID,
		Type:        executionevents.ExecutionEventTypeModelMessage,
		Severity:    executionevents.ExecutionEventSeverityInfo,
		TaskName:    mapCtx.TaskName,
		SessionName: mapCtx.SessionName,
		AgentName:   mapCtx.AgentName,
		Summary:     compactSummary(transcript),
		Content:     content,
		ContentText: transcript,
		CreatedAt:   event.Identity.Timestamp.UTC(),
	}
	if contentOmitted {
		mapped.Summary = assistantResponseOmittedSummary
		mapped.Truncation = &executionevents.ExecutionEventTruncation{ContentTextTruncated: true}
	}
	if err := store.SanitizeExecutionEventPayloadFields(mapped); err != nil {
		return nil, fmt.Errorf("sanitize mapped harness v2 assistant transcript: %w", err)
	}
	return mapped, nil
}

func safeMappedToolCallID(value string) string {
	value = strings.TrimSpace(value)
	digest := sha256.Sum256([]byte(mappedToolCallIDDomain + value))
	return mappedToolCallIDPrefix + hex.EncodeToString(digest[:])
}

func toolCallEventType(status harnessv2.ToolCallStatus) (string, string) {
	switch status {
	case harnessv2.ToolCallStatusCompleted:
		return executionevents.ExecutionEventTypeToolCallCompleted, executionevents.ExecutionEventSeverityInfo
	case harnessv2.ToolCallStatusFailed:
		return executionevents.ExecutionEventTypeToolCallFailed, executionevents.ExecutionEventSeverityError
	default:
		return executionevents.ExecutionEventTypeToolCallStarted, executionevents.ExecutionEventSeverityInfo
	}
}

func toolCallSummary(tool harnessv2.ToolCallUpdate) string {
	if title := compactSummary(tool.Title); title != "" {
		return title
	}
	return fmt.Sprintf("Tool call %s", strings.ReplaceAll(string(tool.Status), "_", " "))
}

func compactSummary(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	value, _, _ = executionevents.RedactAndTruncateExecutionEventText(value, executionevents.MaxExecutionEventSummaryChars)
	return value
}
