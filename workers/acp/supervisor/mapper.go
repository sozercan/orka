package supervisor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/orka-agents/orka/internal/acp"
	executionevents "github.com/orka-agents/orka/internal/events"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const (
	acpUpdateToolCall       = "tool_call"
	acpUpdateToolCallUpdate = "tool_call_update"
	acpContentTypeText      = "text"
)

const (
	maxACPToolCallTitleBytes = 1024
	acpToolCallTitleEllipsis = "…"
)

const (
	acpAssistantPhaseCommentary  = "commentary"
	acpAssistantPhaseFinalAnswer = "final_answer"
)

func promptContentToACP(blocks []harnessv2.ContentBlock) ([]acp.ContentBlock, error) {
	result := make([]acp.ContentBlock, 0, len(blocks))
	for index, block := range blocks {
		switch block.Type {
		case harnessv2.ContentBlockText:
			result = append(result, acp.Text(block.Text))
		case harnessv2.ContentBlockResourceLink:
			name := block.Name
			if name == "" {
				name = block.URI
			}
			result = append(result, acp.ContentBlock{Type: "resource_link", URI: block.URI, Name: name, MIMEType: block.MimeType})
		case harnessv2.ContentBlockArtifactRef:
			return nil, fmt.Errorf("prompt artifact reference %d must be materialized before ACP dispatch", index)
		default:
			return nil, fmt.Errorf("unsupported prompt content type %q", block.Type)
		}
	}
	return result, nil
}

func mapACPUpdate(notification *acp.SessionNotification) (*harnessv2.UpdateEvent, string, bool, error) {
	if notification == nil {
		return nil, "", false, nil
	}
	var envelope struct {
		SessionUpdate string                   `json:"sessionUpdate"`
		Content       json.RawMessage          `json:"content"`
		ToolCallID    string                   `json:"toolCallId"`
		Title         string                   `json:"title"`
		Kind          string                   `json:"kind"`
		Status        harnessv2.ToolCallStatus `json:"status"`
		Entries       []harnessv2.PlanEntry    `json:"entries"`
		Used          uint64                   `json:"used"`
		Size          uint64                   `json:"size"`
	}
	if err := json.Unmarshal(notification.Update, &envelope); err != nil {
		return nil, "", false, fmt.Errorf("decode ACP session update: %w", err)
	}
	switch envelope.SessionUpdate {
	case "agent_message_chunk":
		var content struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(envelope.Content, &content); err != nil {
			return nil, "", false, fmt.Errorf("decode ACP agent message content: %w", err)
		}
		if content.Type != acpContentTypeText || content.Text == "" {
			return nil, "", false, nil
		}
		return &harnessv2.UpdateEvent{
			Kind:             harnessv2.UpdateAssistantMessageChunk,
			AssistantMessage: &harnessv2.AssistantMessageChunk{Text: content.Text},
		}, content.Text, true, nil
	case acpUpdateToolCall, acpUpdateToolCallUpdate:
		toolCallID, err := canonicalACPToolCallID(envelope.ToolCallID)
		if err != nil {
			return nil, "", false, err
		}
		contentPresent := len(envelope.Content) > 0
		toolContent, contentOmitted, err := mapACPToolCallContent(envelope.Content)
		if err != nil {
			return nil, "", false, err
		}
		// ACP adapters may stream tool output through provider-specific fields
		// that harness v2 deliberately does not project. A status-less update
		// with no visible metadata would otherwise become an unbounded series of
		// synthetic in_progress events carrying identical state.
		if envelope.SessionUpdate == acpUpdateToolCallUpdate && envelope.Status == "" &&
			envelope.Title == "" && envelope.Kind == "" && !contentPresent {
			return nil, "", false, nil
		}
		status := envelope.Status
		if status == "" {
			if envelope.SessionUpdate == acpUpdateToolCall {
				status = harnessv2.ToolCallStatusPending
			} else {
				status = harnessv2.ToolCallStatusInProgress
			}
		}
		kind := harnessv2.UpdateToolCallUpdate
		if envelope.SessionUpdate == acpUpdateToolCall {
			kind = harnessv2.UpdateToolCall
		}
		return &harnessv2.UpdateEvent{
			Kind: kind,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: toolCallID, Title: boundACPToolCallTitle(envelope.Title), Kind: envelope.Kind, Status: status,
				Content: toolContent, ContentReplace: contentPresent && !contentOmitted, ContentOmitted: contentOmitted,
			},
		}, "", true, nil
	case "plan":
		plan := &harnessv2.PlanUpdate{Entries: envelope.Entries}
		if err := plan.Validate(); err != nil {
			return nil, "", false, nil
		}
		return &harnessv2.UpdateEvent{Kind: harnessv2.UpdatePlan, Plan: plan}, "", true, nil
	case "usage_update":
		usage := &harnessv2.UsageUpdate{
			ContextWindowUsed: &envelope.Used,
			ContextWindowSize: &envelope.Size,
		}
		if err := usage.Validate(); err != nil {
			return nil, "", false, nil
		}
		return &harnessv2.UpdateEvent{Kind: harnessv2.UpdateUsage, Usage: usage}, "", true, nil
	case "agent_thought_chunk", "user_message_chunk", "available_commands_update", "current_mode_update", "config_option_update", "session_info_update":
		return nil, "", false, nil
	default:
		return nil, "", false, nil
	}
}

func mapACPToolCallContent(raw json.RawMessage) ([]harnessv2.ContentBlock, bool, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, false, nil
	}
	var items []struct {
		Type    string          `json:"type"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, false, fmt.Errorf("decode ACP tool call content: %w", err)
	}
	mapped := make([]harnessv2.ContentBlock, 0, len(items))
	for index, item := range items {
		if len(mapped) == harnessv2.MaxContentBlocks {
			return nil, true, nil
		}
		if item.Type != "content" || len(item.Content) == 0 {
			return nil, true, nil
		}
		var block acp.ContentBlock
		if err := json.Unmarshal(item.Content, &block); err != nil {
			return nil, false, fmt.Errorf("decode ACP tool call content block %d: %w", index, err)
		}
		projected, ok, bounded, err := projectACPContentBlock(block)
		if err != nil {
			return nil, false, fmt.Errorf("project ACP tool call content block %d: %w", index, err)
		}
		if bounded || !ok {
			return nil, true, nil
		}
		mapped = append(mapped, projected)
	}
	return mapped, false, nil
}

func projectACPContentBlock(block acp.ContentBlock) (harnessv2.ContentBlock, bool, bool, error) {
	var projected harnessv2.ContentBlock
	switch block.Type {
	case acpContentTypeText:
		if block.Text == "" {
			return projected, false, false, nil
		}
		projected = harnessv2.ContentBlock{Type: harnessv2.ContentBlockText, Text: block.Text}
	case "resource_link":
		if block.URI == "" {
			return projected, false, false, nil
		}
		projected = harnessv2.ContentBlock{
			Type: harnessv2.ContentBlockResourceLink, URI: block.URI, Name: block.Name, MimeType: block.MIMEType,
		}
	case "resource":
		var resource struct {
			URI      string `json:"uri"`
			MIMEType string `json:"mimeType"`
			Text     string `json:"text"`
		}
		if len(block.Resource) == 0 || json.Unmarshal(block.Resource, &resource) != nil {
			return projected, false, false, nil
		}
		if resource.Text != "" {
			projected = harnessv2.ContentBlock{Type: harnessv2.ContentBlockText, Text: resource.Text}
		} else if resource.URI != "" {
			projected = harnessv2.ContentBlock{
				Type: harnessv2.ContentBlockResourceLink, URI: resource.URI, MimeType: resource.MIMEType,
			}
		} else {
			return projected, false, false, nil
		}
	default:
		return projected, false, false, nil
	}
	if acpToolContentExceedsHarnessLimits(projected) {
		return harnessv2.ContentBlock{}, false, true, nil
	}
	if err := projected.ValidateToolOutput(); err != nil {
		return harnessv2.ContentBlock{}, false, false, err
	}
	return projected, true, false, nil
}

func acpToolContentExceedsHarnessLimits(block harnessv2.ContentBlock) bool {
	return len(block.Text) > harnessv2.MaxPromptContentBytes ||
		len(block.URI) > harnessv2.MaxResourceURIBytes ||
		len(block.Name) > harnessv2.MaxContentNameBytes ||
		len(block.MimeType) > harnessv2.MaxContentMIMETypeBytes
}

func boundACPToolContentToEventLine(event *harnessv2.Event, maxLineBytes int) error {
	if event == nil || event.Update == nil || event.Update.ToolCall == nil || len(event.Update.ToolCall.Content) == 0 {
		return nil
	}
	if maxLineBytes <= 0 {
		return fmt.Errorf("%w: max event line bytes must be positive", harnessv2.ErrEventLineTooLarge)
	}

	line, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal projected ACP tool update: %w", err)
	}
	if len(line) <= maxLineBytes {
		return nil
	}

	tool := event.Update.ToolCall
	content, contentReplace, contentOmitted := tool.Content, tool.ContentReplace, tool.ContentOmitted
	tool.Content = nil
	tool.ContentReplace = false
	tool.ContentOmitted = true
	line, err = json.Marshal(event)
	if err != nil {
		tool.Content, tool.ContentReplace, tool.ContentOmitted = content, contentReplace, contentOmitted
		return fmt.Errorf("marshal projected ACP tool update: %w", err)
	}
	if len(line) > maxLineBytes {
		tool.Content, tool.ContentReplace, tool.ContentOmitted = content, contentReplace, contentOmitted
		return fmt.Errorf("%w: projected ACP tool update exceeds %d bytes without telemetry content", harnessv2.ErrEventLineTooLarge, maxLineBytes)
	}
	return nil
}

func boundACPToolCallTitle(value string) string {
	value = executionevents.RedactExecutionEventText(value)
	if len(value) <= maxACPToolCallTitleBytes {
		return value
	}
	limit := maxACPToolCallTitleBytes - len(acpToolCallTitleEllipsis)
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit] + acpToolCallTitleEllipsis
}

func acpAssistantMessagePhase(notification *acp.SessionNotification) string {
	if notification == nil {
		return ""
	}
	var envelope struct {
		SessionUpdate string `json:"sessionUpdate"`
		Meta          struct {
			Codex struct {
				Phase string `json:"phase"`
			} `json:"codex"`
		} `json:"_meta"`
	}
	if json.Unmarshal(notification.Update, &envelope) != nil || envelope.SessionUpdate != "agent_message_chunk" {
		return ""
	}
	switch envelope.Meta.Codex.Phase {
	case acpAssistantPhaseCommentary, acpAssistantPhaseFinalAnswer:
		return envelope.Meta.Codex.Phase
	default:
		return ""
	}
}

const (
	canonicalACPToolCallIDPrefix = "tool-call-v1-sha256-"
	canonicalACPToolCallIDDomain = "orka.acp.tool-call-id.v1\x00"
)

func canonicalACPToolCallID(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("ACP tool call update omitted toolCallId")
	}
	digest := sha256.Sum256([]byte(canonicalACPToolCallIDDomain + value))
	return canonicalACPToolCallIDPrefix + hex.EncodeToString(digest[:]), nil
}

func mapPermission(event *acp.PermissionRequestEvent, at time.Time, ttl time.Duration) (*harnessv2.PermissionRequestedEvent, error) {
	if event == nil {
		return nil, fmt.Errorf("ACP permission event is required")
	}
	var toolCall struct {
		ToolCallID string `json:"toolCallId"`
		ToolName   string `json:"name"`
		Title      string `json:"title"`
	}
	if err := json.Unmarshal(event.Request.ToolCall, &toolCall); err != nil {
		return nil, fmt.Errorf("decode ACP permission tool call: %w", err)
	}
	toolCallID := ""
	if strings.TrimSpace(toolCall.ToolCallID) != "" {
		var err error
		toolCallID, err = canonicalACPToolCallID(toolCall.ToolCallID)
		if err != nil {
			return nil, err
		}
	}
	if strings.TrimSpace(toolCall.Title) == "" {
		toolCall.Title = "Agent permission request"
	}
	options := make([]harnessv2.PermissionOption, 0, len(event.Request.Options))
	for _, option := range event.Request.Options {
		kind := harnessv2.PermissionOptionKind(option.Kind)
		switch kind {
		case harnessv2.PermissionOptionAllowOnce, harnessv2.PermissionOptionAllowAlways,
			harnessv2.PermissionOptionRejectOnce, harnessv2.PermissionOptionRejectAlways:
		default:
			return nil, fmt.Errorf("unsupported ACP permission option kind %q", option.Kind)
		}
		options = append(options, harnessv2.PermissionOption{OptionID: option.OptionID, Name: option.Name, Kind: kind})
	}
	return &harnessv2.PermissionRequestedEvent{
		RequestID: harnessv2.PermissionRequestID(event.RequestID), ToolCallID: toolCallID,
		ToolName: toolCall.ToolName, Title: toolCall.Title, Options: options, ExpiresAt: at.Add(ttl),
	}, nil
}
