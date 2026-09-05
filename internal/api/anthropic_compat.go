/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/llm"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
)

var anthropicLog = logf.Log.WithName("anthropic-compat")

// AnthropicCompatHandler implements Anthropic-compatible /v1/messages endpoints.
// This allows Anthropic-compatible clients to use Orka as a custom provider.
type AnthropicCompatHandler struct {
	client                    client.Client
	apiReader                 client.Reader
	kubeClient                kubernetes.Interface
	watchNamespace            string
	enforceNamespaceIsolation bool
	config                    ChatConfig
	resolver                  *ProviderResolver
	resultStore               store.ResultStore
	contextTokenAuthorization ContextTokenAuthorizationConfig
}

// NewAnthropicCompatHandler creates an Anthropic-compatible API handler.
func NewAnthropicCompatHandler(c client.Client, apiReader client.Reader, watchNamespace string, enforceNamespaceIsolation bool, config ChatConfig, resolver *ProviderResolver, rs store.ResultStore, kubeClientOpt ...kubernetes.Interface) *AnthropicCompatHandler {
	var kubeClient kubernetes.Interface
	if len(kubeClientOpt) > 0 {
		kubeClient = kubeClientOpt[0]
	}

	return &AnthropicCompatHandler{
		client:                    c,
		apiReader:                 apiReader,
		kubeClient:                kubeClient,
		watchNamespace:            watchNamespace,
		enforceNamespaceIsolation: enforceNamespaceIsolation,
		config:                    config,
		resolver:                  resolver,
		resultStore:               rs,
	}
}

// AnthropicRequest represents an incoming Anthropic Messages API request.
type AnthropicRequest struct {
	Model         string             `json:"model"`
	Messages      []AnthropicMessage `json:"messages"`
	MaxTokens     int                `json:"max_tokens"`
	System        json.RawMessage    `json:"system,omitempty"`
	Stream        bool               `json:"stream,omitempty"`
	Temperature   *float64           `json:"temperature,omitempty"`
	TopP          *float64           `json:"top_p,omitempty"`
	TopK          *int               `json:"top_k,omitempty"`
	StopSequences []string           `json:"stop_sequences,omitempty"`
	Tools         []AnthropicTool    `json:"tools,omitempty"`
	ToolChoice    json.RawMessage    `json:"tool_choice,omitempty"`
	Metadata      *AnthropicMetadata `json:"metadata,omitempty"`
	Thinking      *AnthropicThinking `json:"thinking,omitempty"`
}

// AnthropicMessage is a single message in an Anthropic conversation.
// Content is json.RawMessage because it can be a string or []ContentBlock.
type AnthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// AnthropicContentBlock represents a typed content block in Anthropic messages.
// The Type field determines which other fields are populated.
type AnthropicContentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   json.RawMessage `json:"content,omitempty"`
	Thinking  string          `json:"thinking,omitempty"`
	Source    *ImageSource    `json:"source,omitempty"`
}

// ImageSource represents a base64-encoded image source in an Anthropic content block.
type ImageSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
}

// AnthropicTool defines a tool available for the model to call.
type AnthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// AnthropicThinking configures extended thinking.
type AnthropicThinking struct {
	Type         string `json:"type"`
	BudgetTokens int    `json:"budget_tokens,omitempty"`
}

// AnthropicMetadata contains request metadata.
type AnthropicMetadata struct {
	UserID string `json:"user_id,omitempty"`
}

// AnthropicResponse is the non-streaming response from the Anthropic Messages API.
type AnthropicResponse struct {
	ID           string                  `json:"id"`
	Type         string                  `json:"type"`
	Role         string                  `json:"role"`
	Content      []AnthropicContentBlock `json:"content"`
	Model        string                  `json:"model"`
	StopReason   *string                 `json:"stop_reason"`
	StopSequence *string                 `json:"stop_sequence"`
	Usage        AnthropicUsage          `json:"usage"`
}

// AnthropicUsage contains token usage statistics.
type AnthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// AnthropicStreamEvent is a single SSE event in an Anthropic streaming response.
type AnthropicStreamEvent struct {
	Type         string                 `json:"type"`
	Message      *AnthropicResponse     `json:"message,omitempty"`
	Index        int                    `json:"index,omitempty"`
	ContentBlock *AnthropicContentBlock `json:"content_block,omitempty"`
	Delta        *AnthropicDelta        `json:"delta,omitempty"`
	Usage        *AnthropicUsage        `json:"usage,omitempty"`
}

// AnthropicDelta carries incremental updates within a streaming event.
type AnthropicDelta struct {
	Type         string  `json:"type,omitempty"`
	Text         string  `json:"text,omitempty"`
	PartialJSON  string  `json:"partial_json,omitempty"`
	Thinking     string  `json:"thinking,omitempty"`
	StopReason   *string `json:"stop_reason,omitempty"`
	StopSequence *string `json:"stop_sequence,omitempty"`
}

// AnthropicError is the standard error response format for the Anthropic API.
type AnthropicError struct {
	Type  string               `json:"type"`
	Error AnthropicErrorDetail `json:"error"`
}

// AnthropicErrorDetail contains the error type and message.
type AnthropicErrorDetail struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// parseAnthropicContent handles both string and []ContentBlock formats for message content.
func parseAnthropicContent(raw json.RawMessage) ([]AnthropicContentBlock, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	// Try string first
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return []AnthropicContentBlock{
			{Type: oaiContentTypeText, Text: s},
		}, nil
	}

	// Try []ContentBlock
	var blocks []AnthropicContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, fmt.Errorf("content must be a string or array of content blocks: %w", err)
	}
	return blocks, nil
}

// parseAnthropicSystem handles both string and []ContentBlock formats for the system field.
func parseAnthropicSystem(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}

	// Try string first
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}

	// Try []ContentBlock and concatenate text blocks
	var blocks []AnthropicContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return "", fmt.Errorf("system must be a string or array of content blocks: %w", err)
	}

	var parts []string
	for _, b := range blocks {
		if b.Type == oaiContentTypeText && b.Text != "" {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n"), nil
}

// HandleMessages handles POST /anthropic/v1/messages.
func (h *AnthropicCompatHandler) HandleMessages(c fiber.Ctx) error {
	start := time.Now()

	var req AnthropicRequest
	if err := c.Bind().JSON(&req); err != nil {
		return anthropicError(c, 400, "invalid_request_error", "invalid request body: "+err.Error())
	}

	if req.Model == "" {
		return anthropicError(c, 400, "invalid_request_error", "model is required")
	}
	if len(req.Messages) == 0 {
		return anthropicError(c, 400, "invalid_request_error", "messages is required and must be non-empty")
	}
	if req.MaxTokens <= 0 {
		return anthropicError(c, 400, "invalid_request_error", "max_tokens is required and must be greater than 0")
	}

	userInfo := GetUserInfo(c)
	var contextToken *ContextToken
	if userInfo != nil {
		contextToken = userInfo.ContextToken
	}

	ctx, cancel := context.WithTimeout(c.Context(), h.config.MaxDuration)
	defer cancel()

	namespace, err := ResolveNamespace(c, c.Query("namespace", ""), h.watchNamespace, h.enforceNamespaceIsolation)
	if err != nil {
		return anthropicContextTokenAuthorizationError(c, err)
	}

	provider, model, providerInfo, err := h.resolver.ResolveWithInfo(ctx, ResolveOpts{
		ModelStr:  req.Model,
		Namespace: namespace,
		AuthorizeProviderReference: func(provider ProviderResolutionInfo) error {
			return authorizeContextTokenProviderReference(c, h.contextTokenAuthorization, "anthropicMessagesProviderReference", namespace, provider)
		},
		AuthorizeProviderUse: func(provider ProviderResolutionInfo, model string) error {
			return authorizeContextTokenProviderUse(c, h.contextTokenAuthorization, "anthropicMessages", namespace, provider, model)
		},
		RequireModel: true,
		// Enforced scoped context tokens get no implicit Provider selection.
		RequireExplicitProvider: requestRequiresExplicitProvider(c, h.contextTokenAuthorization),
	})
	if err != nil {
		if ferr, ok := err.(*fiber.Error); ok && ferr.Code == fiber.StatusForbidden {
			return anthropicContextTokenAuthorizationError(c, err)
		}
		anthropicLog.Error(err, "failed to resolve provider", "model", req.Model)
		return anthropicError(c, 400, "invalid_request_error", "failed to resolve provider: "+err.Error())
	}

	provider = llm.NewTracingProvider(provider)

	messages, err := convertAnthropicMessages(req.Messages)
	if err != nil {
		return anthropicError(c, 400, "invalid_request_error", "failed to convert messages: "+err.Error())
	}

	convertedTools := convertAnthropicTools(req.Tools)

	systemPrompt, err := parseAnthropicSystem(req.System)
	if err != nil {
		return anthropicError(c, 400, "invalid_request_error", "failed to parse system prompt: "+err.Error())
	}

	compReq := &llm.CompletionRequest{
		Model:         model,
		Messages:      messages,
		SystemPrompt:  systemPrompt,
		MaxTokens:     req.MaxTokens,
		Tools:         convertedTools,
		StopSequences: req.StopSequences,
	}
	if req.Temperature != nil {
		compReq.Temperature = *req.Temperature
	}

	// Inject Orka tools and run the server-side agentic loop by default.
	// Set X-Orka-Tools: disabled to use as a transparent proxy instead.
	orkaToolsEnabled, err := prepareCompatCoordinatorTools(c, ctx, compReq, compatCoordinatorSetup{
		Client:              h.client,
		Namespace:           namespace,
		ToolUseAction:       "anthropicTools",
		AuthorizationConfig: h.contextTokenAuthorization,
	})
	if err != nil {
		return anthropicContextTokenAuthorizationError(c, err)
	}

	// Build ToolContext for coordinator tools (create_agent_task, wait_for_task, etc.)
	var proxyToolCtx *tools.ToolContext
	if orkaToolsEnabled {
		proxyToolCtx = newCompatProxyToolContext(compatProxyToolContextConfig{
			Client:                    h.client,
			AuthorizationReader:       h.apiReader,
			KubeClient:                h.kubeClient,
			Namespace:                 namespace,
			Provider:                  providerInfo,
			WatchNamespace:            h.watchNamespace,
			EnforceNamespaceIsolation: h.enforceNamespaceIsolation,
			ResultStore:               h.resultStore,
			GenerateTaskName:          func() string { return fmt.Sprintf("proxy-%s", uuid.New().String()[:8]) },
			Profile:                   anthropicCompatProxyToolContextProfile,
			AuthContext:               contextToken,
			AuthorizationConfig:       h.contextTokenAuthorization,
			UserInfo:                  userInfo,
		})
	}

	if req.Stream {
		if orkaToolsEnabled {
			return h.handleStreamingMessages(c, ctx, provider, compReq, model, proxyToolCtx)
		}
		return h.handleStreamingProxy(c, ctx, provider, compReq, model)
	}

	var resp *llm.CompletionResponse
	if orkaToolsEnabled {
		// Run the agentic tool loop (executes tools server-side until final text response)
		resp, err = runNonStreamingToolLoop(ctx, provider, compReq, model, h.config, proxyToolCtx)
		if err != nil {
			anthropicLog.Error(err, "tool loop failed")
			return anthropicError(c, 500, "api_error", "completion failed: "+err.Error())
		}
		resp = stripGoalStateSentinelFromResponse(resp)
	} else {
		// Transparent proxy: single LLM call, return response directly
		resp, err = provider.Complete(ctx, compReq)
		if err != nil {
			anthropicLog.Error(err, "completion failed")
			return anthropicError(c, 500, "api_error", "completion failed: "+err.Error())
		}
	}
	stopReason, ok := mapAnthropicCompletionStopReason(resp)
	if !ok {
		message := "provider returned an invalid completion outcome"
		if resp != nil && strings.TrimSpace(resp.StopReason) != "" {
			message = fmt.Sprintf("provider returned non-success outcome %q", resp.StopReason)
		}
		return anthropicError(c, fiber.StatusBadGateway, "api_error", message)
	}

	user := ""
	if ui := GetUserInfo(c); ui != nil {
		user = ui.Username
	}
	anthropicLog.Info("messages completed",
		"user", user,
		"model", model,
		"input_tokens", resp.InputTokens,
		"output_tokens", resp.OutputTokens,
		"stop_reason", resp.StopReason,
		"duration", time.Since(start).String(),
	)

	result := convertToAnthropicResponse(resp, model, stopReason)
	return c.JSON(result)
}

// HandleListModels handles GET /anthropic/v1/models.
func (h *AnthropicCompatHandler) HandleListModels(c fiber.Ctx) error {
	ctx := c.Context()

	namespace, err := ResolveNamespace(c, c.Query("namespace", ""), h.watchNamespace, h.enforceNamespaceIsolation)
	if err != nil {
		return anthropicContextTokenAuthorizationError(c, err)
	}

	if err := authorizeContextTokenActionWithConfig(c, h.contextTokenAuthorization, "anthropicListModels", h.contextTokenAuthorization.ProviderUseScopes); err != nil {
		return anthropicContextTokenAuthorizationError(c, err)
	}

	providerList := &corev1alpha1.ProviderList{}
	listOpts := []client.ListOption{}
	if namespace != "" {
		listOpts = append(listOpts, client.InNamespace(namespace))
	}
	if err := h.client.List(ctx, providerList, listOpts...); err != nil {
		anthropicLog.Error(err, "failed to list providers")
		return anthropicError(c, 500, "api_error", "failed to list providers")
	}

	now := time.Now().Unix()
	models := []OAIModel{}
	seen := make(map[string]bool)

	for _, p := range providerList.Items {
		if p.Spec.DefaultModel != "" {
			if !contextTokenAllowsListedProviderModel(c, h.contextTokenAuthorization, "anthropicListModels", namespace, providerResolutionInfo(&p), p.Spec.DefaultModel) {
				continue
			}
			modelID := fmt.Sprintf("%s/%s", p.Name, p.Spec.DefaultModel)
			if !seen[modelID] {
				models = append(models, OAIModel{
					ID:      modelID,
					Object:  "model",
					Created: now,
					OwnedBy: string(p.Spec.Type),
				})
				seen[modelID] = true
			}
			if !seen[p.Spec.DefaultModel] {
				models = append(models, OAIModel{
					ID:      p.Spec.DefaultModel,
					Object:  "model",
					Created: now,
					OwnedBy: string(p.Spec.Type),
				})
				seen[p.Spec.DefaultModel] = true
			}
		}
	}

	return c.JSON(OAIModelList{
		Object: "list",
		Data:   models,
	})
}

// convertAnthropicMessages converts Anthropic messages to internal llm.Message format.
func convertAnthropicMessages(msgs []AnthropicMessage) ([]llm.Message, error) {
	var messages []llm.Message

	for _, m := range msgs {
		blocks, err := parseAnthropicContent(m.Content)
		if err != nil {
			return nil, fmt.Errorf("failed to parse content for role %q: %w", m.Role, err)
		}

		switch m.Role {
		case "user":
			// Separate tool_result blocks from other content
			var textParts []string
			for _, b := range blocks {
				switch b.Type {
				case "tool_result":
					resultContent := ""
					if b.Content != nil {
						// tool_result content can be string or []ContentBlock
						var s string
						if json.Unmarshal(b.Content, &s) == nil {
							resultContent = s
						} else {
							var innerBlocks []AnthropicContentBlock
							if json.Unmarshal(b.Content, &innerBlocks) == nil {
								var parts []string
								for _, ib := range innerBlocks {
									if ib.Type == oaiContentTypeText {
										parts = append(parts, ib.Text)
									}
								}
								resultContent = strings.Join(parts, "\n")
							}
						}
					}
					messages = append(messages, llm.Message{
						Role:       "tool",
						ToolCallID: b.ToolUseID,
						Content:    resultContent,
					})
				case "text":
					textParts = append(textParts, b.Text)
				}
			}
			if len(textParts) > 0 {
				messages = append(messages, llm.Message{
					Role:    "user",
					Content: strings.Join(textParts, "\n"),
				})
			}

		case "assistant":
			msg := llm.Message{Role: "assistant"}
			var textParts []string
			for _, b := range blocks {
				switch b.Type {
				case "text":
					textParts = append(textParts, b.Text)
				case oaiStopReasonToolUse:
					msg.ToolCalls = append(msg.ToolCalls, llm.ToolCall{
						ID:        b.ID,
						Name:      b.Name,
						Arguments: b.Input,
					})
				}
			}
			if len(textParts) > 0 {
				msg.Content = strings.Join(textParts, "\n")
			}
			messages = append(messages, msg)

		default:
			// Pass through unknown roles
			var textParts []string
			for _, b := range blocks {
				if b.Type == oaiContentTypeText {
					textParts = append(textParts, b.Text)
				}
			}
			messages = append(messages, llm.Message{
				Role:    m.Role,
				Content: strings.Join(textParts, "\n"),
			})
		}
	}

	return messages, nil
}

// convertAnthropicTools converts Anthropic tool definitions to internal llm.Tool format.
func convertAnthropicTools(inputTools []AnthropicTool) []llm.Tool {
	if len(inputTools) == 0 {
		return nil
	}

	result := make([]llm.Tool, 0, len(inputTools))
	for _, t := range inputTools {
		result = append(result, llm.Tool{
			Name:        t.Name,
			Description: t.Description,
			Parameters:  t.InputSchema,
		})
	}
	return result
}

func mapAnthropicCompletionStopReason(resp *llm.CompletionResponse) (string, bool) {
	if resp == nil {
		return "", false
	}
	return mapAnthropicStreamStopReason(
		resp.StopReason,
		len(resp.ToolCalls) > 0,
	)
}

// convertToAnthropicResponse converts an internal CompletionResponse to Anthropic format.
func convertToAnthropicResponse(resp *llm.CompletionResponse, model, stopReason string) AnthropicResponse {
	id := "msg_" + uuid.New().String()

	content := make([]AnthropicContentBlock, 0, len(resp.ToolCalls)+1)
	if resp.Content != "" {
		content = append(content, AnthropicContentBlock{
			Type: oaiContentTypeText,
			Text: resp.Content,
		})
	}
	for _, tc := range resp.ToolCalls {
		content = append(content, AnthropicContentBlock{
			Type:  oaiStopReasonToolUse,
			ID:    tc.ID,
			Name:  tc.Name,
			Input: tc.Arguments,
		})
	}

	return AnthropicResponse{
		ID:         id,
		Type:       "message",
		Role:       "assistant",
		Content:    content,
		Model:      model,
		StopReason: &stopReason,
		Usage: AnthropicUsage{
			InputTokens:  resp.InputTokens,
			OutputTokens: resp.OutputTokens,
		},
	}
}

// anthropicError returns an error in Anthropic API format.
func anthropicError(c fiber.Ctx, status int, errType, message string) error {
	return c.Status(status).JSON(AnthropicError{
		Type: "error",
		Error: AnthropicErrorDetail{
			Type:    errType,
			Message: message,
		},
	})
}

// stripClientToolMessages removes tool_use and tool_result messages from client history
// so the LLM doesn't see stale references to tools it no longer has access to.
func stripClientToolMessages(messages []llm.Message) []llm.Message {
	filtered := make([]llm.Message, 0, len(messages))
	for _, m := range messages {
		// Skip tool result messages (from client tool execution)
		if m.Role == "tool" {
			continue
		}
		// For assistant messages, strip tool calls but keep text content
		if m.Role == "assistant" && len(m.ToolCalls) > 0 {
			if m.Content == "" {
				continue
			}
			m = llm.Message{Role: m.Role, Content: m.Content}
		}
		// Prevent consecutive same-role messages (merge into previous)
		if len(filtered) > 0 && filtered[len(filtered)-1].Role == m.Role {
			filtered[len(filtered)-1].Content += "\n" + m.Content
			continue
		}
		filtered = append(filtered, m)
	}
	return filtered
}
