/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/llm"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
)

const (
	oaiParamMaxTokens    = "max_tokens"
	oaiRoleSystem        = "system"
	oaiContentTypeText   = "text"
	oaiStopReasonEndTurn = "end_turn"
	oaiStopReasonToolUse = "tool_use"
	oaiStopReasonLength  = "length"
)

var oaiLog = logf.Log.WithName("openai-compat")

const (
	anthropicStopReasonModelContextWindowExceeded = "model_context_window_exceeded"
	completionStatusCompleted                     = "completed"
	finishReasonContentFilter                     = "content_filter"
	finishReasonFunctionCall                      = "function_call"
	finishReasonStop                              = "stop"
	finishReasonStopSequence                      = "stop_sequence"
	finishReasonToolCalls                         = "tool_calls"
)

// OpenAICompatHandler implements OpenAI-compatible /openai/v1/chat/completions and /openai/v1/models endpoints.
// This allows OpenAI-compatible clients to use Orka as a custom provider.
type OpenAICompatHandler struct {
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

// NewOpenAICompatHandler creates an OpenAI-compatible API handler.
func NewOpenAICompatHandler(c client.Client, apiReader client.Reader, watchNamespace string, enforceNS bool, config ChatConfig, resolver *ProviderResolver, rs store.ResultStore, kubeClientOpt ...kubernetes.Interface) *OpenAICompatHandler {
	var kubeClient kubernetes.Interface
	if len(kubeClientOpt) > 0 {
		kubeClient = kubeClientOpt[0]
	}

	return &OpenAICompatHandler{
		client:                    c,
		apiReader:                 apiReader,
		kubeClient:                kubeClient,
		watchNamespace:            watchNamespace,
		enforceNamespaceIsolation: enforceNS,
		config:                    config,
		resolver:                  resolver,
		resultStore:               rs,
	}
}

// --- OpenAI API types ---

// OAIRequest is the OpenAI chat completion request format.
type OAIRequest struct {
	Model            string             `json:"model"`
	Messages         []OAIMessage       `json:"messages"`
	Tools            []OAITool          `json:"tools,omitempty"`
	Temperature      *float64           `json:"temperature,omitempty"`
	MaxTokens        *int               `json:"max_tokens,omitempty"`
	MaxCompTokens    *int               `json:"max_completion_tokens,omitempty"`
	Stream           bool               `json:"stream,omitempty"`
	StreamOptions    *StreamOptions     `json:"stream_options,omitempty"`
	Stop             any                `json:"stop,omitempty"`
	TopP             *float64           `json:"top_p,omitempty"`
	FrequencyPenalty *float64           `json:"frequency_penalty,omitempty"`
	PresencePenalty  *float64           `json:"presence_penalty,omitempty"`
	N                *int               `json:"n,omitempty"`
	User             string             `json:"user,omitempty"`
	ResponseFormat   *OAIResponseFormat `json:"response_format,omitempty"`
}

// OAIResponseFormat is the OpenAI response_format field.
type OAIResponseFormat struct {
	Type       string             `json:"type"` // "text", "json_object", "json_schema"
	JSONSchema *OAIJSONSchemaSpec `json:"json_schema,omitempty"`
}

// OAIJSONSchemaSpec holds the json_schema details within response_format.
type OAIJSONSchemaSpec struct {
	Name        string         `json:"name"`
	Schema      map[string]any `json:"schema,omitempty"`
	Strict      *bool          `json:"strict,omitempty"`
	Description string         `json:"description,omitempty"`
}

// StreamOptions holds stream-specific options.
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage,omitempty"`
}

// OAIMessage represents an OpenAI chat message.
type OAIMessage struct {
	Role       string        `json:"role,omitempty"`
	Content    any           `json:"content"` // string or []ContentPart
	Name       string        `json:"name,omitempty"`
	ToolCalls  []OAIToolCall `json:"tool_calls,omitempty"`
	ToolCallID string        `json:"tool_call_id,omitempty"`
	Refusal    *string       `json:"refusal,omitempty"`
}

// OAITool is an OpenAI tool definition.
type OAITool struct {
	Type     string         `json:"type"` // "function"
	Function OAIFunctionDef `json:"function"`
}

// OAIFunctionDef is an OpenAI function definition.
type OAIFunctionDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// OAIToolCall is an OpenAI tool call.
type OAIToolCall struct {
	Index    *int            `json:"index,omitempty"`
	ID       string          `json:"id,omitempty"`
	Type     string          `json:"type,omitempty"` // "function"
	Function OAIFunctionCall `json:"function"`
}

// OAIFunctionCall is a function call within a tool call.
type OAIFunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// OAIResponse is the OpenAI chat completion response.
type OAIResponse struct {
	ID                string      `json:"id"`
	Object            string      `json:"object"`
	Created           int64       `json:"created"`
	Model             string      `json:"model"`
	Choices           []OAIChoice `json:"choices"`
	Usage             *OAIUsage   `json:"usage,omitempty"`
	SystemFingerprint string      `json:"system_fingerprint,omitempty"`
}

// OAIChoice is a choice in the response.
type OAIChoice struct {
	Index        int         `json:"index"`
	Message      *OAIMessage `json:"message,omitempty"`
	Delta        *OAIMessage `json:"delta,omitempty"`
	FinishReason *string     `json:"finish_reason"`
}

// OAIUsage contains token usage statistics.
type OAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// OAIModel is the model object for /openai/v1/models.
type OAIModel struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// OAIModelList is the response for GET /openai/v1/models.
type OAIModelList struct {
	Object string     `json:"object"`
	Data   []OAIModel `json:"data"`
}

// OAIError is the OpenAI error response format.
type OAIError struct {
	Error OAIErrorDetail `json:"error"`
}

// OAIErrorDetail is the error detail.
type OAIErrorDetail struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param"`
	Code    *string `json:"code"`
}

// HandleChatCompletions handles POST /openai/v1/chat/completions.
func (h *OpenAICompatHandler) HandleChatCompletions(c fiber.Ctx) error {
	var req OAIRequest
	if err := c.Bind().JSON(&req); err != nil {
		return c.Status(400).JSON(OAIError{Error: OAIErrorDetail{
			Message: "invalid request body: " + err.Error(),
			Type:    "invalid_request_error",
		}})
	}

	if len(req.Messages) == 0 {
		return c.Status(400).JSON(OAIError{Error: OAIErrorDetail{
			Message: "messages is required and must be non-empty",
			Type:    "invalid_request_error",
		}})
	}

	if req.N != nil && *req.N > 1 {
		nParam := "n"
		return c.Status(400).JSON(OAIError{Error: OAIErrorDetail{
			Message: "n > 1 is not supported: underlying providers do not support multiple choices",
			Type:    "invalid_request_error",
			Param:   &nParam,
		}})
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
		return openAIContextTokenAuthorizationError(c, err)
	}

	// Resolve provider and model from the request model field.
	// Supports "provider/model" format (e.g., "anthropic/claude-sonnet-4") or plain model name.
	provider, model, providerInfo, err := h.resolver.ResolveWithInfo(ctx, ResolveOpts{
		ModelStr:  req.Model,
		Namespace: namespace,
		AuthorizeProviderReference: func(provider ProviderResolutionInfo) error {
			return authorizeContextTokenProviderReference(c, h.contextTokenAuthorization, "openAIChatCompletionsProviderReference", namespace, provider)
		},
		AuthorizeProviderUse: func(provider ProviderResolutionInfo, model string) error {
			return authorizeContextTokenProviderUse(c, h.contextTokenAuthorization, "openAIChatCompletions", namespace, provider, model)
		},
		RequireModel: true,
		// Enforced scoped context tokens get no implicit Provider selection.
		RequireExplicitProvider: requestRequiresExplicitProvider(c, h.contextTokenAuthorization),
	})
	if err != nil {
		if ferr, ok := err.(*fiber.Error); ok && ferr.Code == fiber.StatusForbidden {
			return openAIContextTokenAuthorizationError(c, err)
		}
		oaiLog.Error(err, "failed to resolve provider", "model", req.Model)
		return c.Status(400).JSON(OAIError{Error: OAIErrorDetail{
			Message: "failed to resolve provider: " + err.Error(),
			Type:    "invalid_request_error",
		}})
	}

	provider = llm.NewTracingProvider(provider)

	compReq, errDetail := buildOpenAICompletionRequest(req, model)
	if errDetail != nil {
		return c.Status(400).JSON(OAIError{Error: *errDetail})
	}

	completionID := fmt.Sprintf("chatcmpl-%s", generateChatID())
	now := time.Now().Unix()

	// Inject Orka tools and run the server-side agentic loop by default.
	// Set X-Orka-Tools: disabled to use as a transparent proxy instead.
	orkaToolsEnabled, err := prepareCompatCoordinatorTools(c, ctx, compReq, compatCoordinatorSetup{
		Client:              h.client,
		Namespace:           namespace,
		ToolUseAction:       "openAITools",
		AuthorizationConfig: h.contextTokenAuthorization,
	})
	if err != nil {
		return openAIContextTokenAuthorizationError(c, err)
	}

	// Build ToolContext for coordinator tools
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
			GenerateTaskName:          func() string { return fmt.Sprintf("proxy-%s", generateChatID()) },
			Profile:                   openAICompatProxyToolContextProfile,
			AuthContext:               contextToken,
			AuthorizationConfig:       h.contextTokenAuthorization,
			UserInfo:                  userInfo,
		})

	}

	if req.Stream {
		if orkaToolsEnabled {
			return h.handleStreamingToolLoop(c, ctx, provider, compReq, completionID, model, now, req.StreamOptions, proxyToolCtx)
		}
		return h.handleStreamingCompletion(c, ctx, provider, compReq, completionID, model, now, req.StreamOptions)
	}

	if orkaToolsEnabled {
		return h.handleNonStreamingToolLoop(c, ctx, provider, compReq, completionID, model, now, proxyToolCtx)
	}
	return h.handleNonStreamingCompletion(c, ctx, provider, compReq, completionID, model, now)
}

func buildOpenAICompletionRequest(req OAIRequest, model string) (*llm.CompletionRequest, *OAIErrorDetail) {
	messages, systemPrompt := convertOAIMessages(req.Messages)
	compReq := &llm.CompletionRequest{
		Model:        model,
		Messages:     messages,
		SystemPrompt: systemPrompt,
		Tools:        convertOAITools(req.Tools),
	}

	if req.Temperature != nil {
		compReq.Temperature = *req.Temperature
	}

	maxTokens := 0
	if req.MaxCompTokens != nil {
		maxTokens = *req.MaxCompTokens
	} else if req.MaxTokens != nil {
		maxTokens = *req.MaxTokens
	}
	if maxTokens > 0 {
		if maxTokens < 16 {
			oaiLog.Info("max_tokens too small", oaiParamMaxTokens, maxTokens)
			param := oaiParamMaxTokens
			return nil, &OAIErrorDetail{
				Message: fmt.Sprintf("max_tokens must be at least 16, got %d", maxTokens),
				Type:    "invalid_request_error",
				Param:   &param,
			}
		}
		compReq.MaxTokens = maxTokens
	}

	if req.Stop != nil {
		switch v := req.Stop.(type) {
		case string:
			compReq.StopSequences = []string{v}
		case []any:
			for _, s := range v {
				if str, ok := s.(string); ok {
					compReq.StopSequences = append(compReq.StopSequences, str)
				}
			}
		}
	}

	if req.ResponseFormat != nil {
		compReq.ResponseFormat = &llm.ResponseFormat{Type: req.ResponseFormat.Type}
		if req.ResponseFormat.JSONSchema != nil {
			compReq.ResponseFormat.JSONSchema = &llm.JSONSchemaFormat{
				Name:        req.ResponseFormat.JSONSchema.Name,
				Schema:      req.ResponseFormat.JSONSchema.Schema,
				Strict:      req.ResponseFormat.JSONSchema.Strict,
				Description: req.ResponseFormat.JSONSchema.Description,
			}
		}
	}

	return compReq, nil
}

// handleNonStreamingCompletion handles a non-streaming chat completion request.
func (h *OpenAICompatHandler) handleNonStreamingCompletion(
	c fiber.Ctx,
	ctx context.Context,
	provider llm.Provider,
	req *llm.CompletionRequest,
	completionID, model string,
	created int64,
) error {
	resp, err := provider.Complete(ctx, req)
	if err != nil {
		oaiLog.Error(err, "completion failed")
		return c.Status(500).JSON(OAIError{Error: OAIErrorDetail{
			Message: "completion failed: " + err.Error(),
			Type:    "server_error",
		}})
	}

	return h.formatOAIResponse(c, resp, completionID, model, created)
}

// handleNonStreamingToolLoop runs the agentic tool loop and returns the final result in OpenAI format.
func (h *OpenAICompatHandler) handleNonStreamingToolLoop(
	c fiber.Ctx,
	ctx context.Context,
	provider llm.Provider,
	req *llm.CompletionRequest,
	completionID, model string,
	created int64,
	toolCtx *tools.ToolContext,
) error {
	resp, err := runNonStreamingToolLoop(ctx, provider, req, model, h.config, toolCtx)
	if err != nil {
		oaiLog.Error(err, "tool loop failed")
		return c.Status(500).JSON(OAIError{Error: OAIErrorDetail{
			Message: "completion failed: " + err.Error(),
			Type:    "server_error",
		}})
	}

	resp = stripGoalStateSentinelFromResponse(resp)
	return h.formatOAIResponse(c, resp, completionID, model, created)
}

// handleStreamingToolLoop runs the agentic tool loop inside the SSE response.
// Orka tool calls remain server-side; user-visible progress is emitted as
// ordinary assistant content chunks so OpenAI-compatible clients keep receiving
// bytes during multi-minute coordinator workflows.
func (h *OpenAICompatHandler) handleStreamingToolLoop(
	c fiber.Ctx,
	ctx context.Context,
	provider llm.Provider,
	req *llm.CompletionRequest,
	completionID, model string,
	created int64,
	streamOpts *StreamOptions,
	toolCtx *tools.ToolContext,
) error {
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no")

	capturedProvider := provider
	capturedReq := req
	capturedToolCtx := toolCtx
	streamBaseCtx := detachedSpanContext(ctx)

	return c.SendStreamWriter(func(w *bufio.Writer) {
		streamCtx, streamCancel := context.WithTimeout(streamBaseCtx, h.config.MaxDuration)
		defer streamCancel()

		// Send role chunk
		roleChunk := OAIResponse{
			ID:      completionID,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   model,
			Choices: []OAIChoice{{
				Index: 0,
				Delta: &OAIMessage{Role: "assistant"},
			}},
		}
		if err := writeStreamChunk(w, roleChunk); err != nil {
			streamCancel()
			return
		}

		writeContent := func(content string) {
			if err := writeOpenAIContentChunk(w, completionID, created, model, content); err != nil {
				streamCancel()
			}
		}
		observer := &toolLoopObserver{
			OnAssistantContent: func(content string) {
				writeContent(stripGoalStateSentinel(content))
			},
			OnFinalContent: func(content string) {
				writeContent(stripGoalStateSentinel(content))
			},
			OnToolResult: func(tc llm.ToolCall, result string) {
				writeContent(formatToolProgress(tc, result))
			},
			OnPrematureEndRetry: func() {
				writeContent("[Continuing workflow...]\n\n")
			},
			OnAutoPoll: func() {
				writeContent("[⏳ Auto-polling tasks...]\n")
			},
		}

		resp, err := runToolLoopWithObserver(streamCtx, capturedProvider, capturedReq, model, h.config, capturedToolCtx, observer)
		if err != nil {
			oaiLog.Error(err, "streaming tool loop failed")
			_ = writeStreamError(w, "provider_error")
			return
		}
		if resp == nil {
			_ = writeStreamError(w, "")
			return
		}
		finishReason, ok := mapStreamFinishReason(
			resp.StopReason,
			strings.TrimSpace(stripGoalStateSentinel(resp.Content)) != "",
			len(resp.ToolCalls) > 0,
		)
		if !ok {
			_ = writeStreamError(w, resp.StopReason)
			return
		}

		// Send finish chunk
		finishChunk := OAIResponse{
			ID:      completionID,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   model,
			Choices: []OAIChoice{{
				Index:        0,
				Delta:        &OAIMessage{},
				FinishReason: &finishReason,
			}},
		}
		if resp != nil && streamOpts != nil && streamOpts.IncludeUsage {
			finishChunk.Usage = &OAIUsage{
				PromptTokens:     resp.InputTokens,
				CompletionTokens: resp.OutputTokens,
				TotalTokens:      resp.InputTokens + resp.OutputTokens,
			}
		}
		_ = writeStreamChunk(w, finishChunk)
		_ = writeStreamDone(w)
	})
}

func writeOpenAIContentChunk(w *bufio.Writer, completionID string, created int64, model, content string) error {
	if content == "" {
		return nil
	}
	return writeStreamChunk(w, OAIResponse{
		ID:      completionID,
		Object:  "chat.completion.chunk",
		Created: created,
		Model:   model,
		Choices: []OAIChoice{{
			Index: 0,
			Delta: &OAIMessage{Content: content},
		}},
	})
}

func stripGoalStateSentinel(content string) string {
	if !strings.Contains(content, goalStateSentinel) {
		return content
	}
	stripped := strings.ReplaceAll(content, goalStateSentinel, "")
	return strings.TrimLeft(stripped, " \t\r\n")
}

func stripGoalStateSentinelFromResponse(resp *llm.CompletionResponse) *llm.CompletionResponse {
	if resp == nil {
		return nil
	}
	strippedContent := stripGoalStateSentinel(resp.Content)
	if strippedContent == resp.Content {
		return resp
	}
	respCopy := *resp
	respCopy.Content = strippedContent
	return &respCopy
}

// formatOAIResponse formats a CompletionResponse into OpenAI API format.
func (h *OpenAICompatHandler) formatOAIResponse(c fiber.Ctx, resp *llm.CompletionResponse, completionID, model string, created int64) error {
	if resp == nil {
		return openAICompletionOutcomeError(c, "")
	}
	finishReason, ok := mapStreamFinishReason(
		resp.StopReason,
		strings.TrimSpace(resp.Content) != "",
		len(resp.ToolCalls) > 0,
	)
	if !ok {
		return openAICompletionOutcomeError(c, resp.StopReason)
	}

	msg := &OAIMessage{
		Role:    "assistant",
		Content: resp.Content,
	}

	// Convert tool calls
	if len(resp.ToolCalls) > 0 {
		msg.ToolCalls = make([]OAIToolCall, 0, len(resp.ToolCalls))
		for i, tc := range resp.ToolCalls {
			idx := i
			msg.ToolCalls = append(msg.ToolCalls, OAIToolCall{
				Index: &idx,
				ID:    tc.ID,
				Type:  "function",
				Function: OAIFunctionCall{
					Name:      tc.Name,
					Arguments: string(tc.Arguments),
				},
			})
		}
		if finishReason == finishReasonStop {
			finishReason = finishReasonToolCalls
		}
	}

	return c.JSON(OAIResponse{
		ID:      completionID,
		Object:  "chat.completion",
		Created: created,
		Model:   model,
		Choices: []OAIChoice{{
			Index:        0,
			Message:      msg,
			FinishReason: &finishReason,
		}},
		Usage: &OAIUsage{
			PromptTokens:     resp.InputTokens,
			CompletionTokens: resp.OutputTokens,
			TotalTokens:      resp.InputTokens + resp.OutputTokens,
		},
	})
}

func openAICompletionOutcomeError(c fiber.Ctx, reason string) error {
	message := "provider returned an invalid completion outcome"
	if strings.TrimSpace(reason) != "" {
		message = fmt.Sprintf("provider returned non-success outcome %q", reason)
	}
	return c.Status(fiber.StatusBadGateway).JSON(OAIError{Error: OAIErrorDetail{
		Message: message,
		Type:    "server_error",
	}})
}

// handleStreamingCompletion handles a streaming chat completion request.
func (h *OpenAICompatHandler) handleStreamingCompletion(
	c fiber.Ctx,
	ctx context.Context,
	provider llm.Provider,
	req *llm.CompletionRequest,
	completionID, model string,
	created int64,
	streamOpts *StreamOptions,
) error {
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no")

	// Capture for closure
	capturedProvider := provider
	capturedReq := req
	streamBaseCtx := detachedSpanContext(ctx)

	return c.SendStreamWriter(func(w *bufio.Writer) {
		streamCtx, streamCancel := context.WithTimeout(streamBaseCtx, h.config.MaxDuration)
		defer streamCancel()

		streamCh, err := capturedProvider.Stream(streamCtx, capturedReq)
		if err != nil {
			// Try non-streaming fallback via Complete
			resp, completeErr := capturedProvider.Complete(streamCtx, capturedReq)
			if completeErr != nil {
				oaiLog.Error(completeErr, "stream fallback completion failed")
				_ = writeStreamError(w, "provider_error")
				return
			}
			if resp == nil {
				_ = writeStreamError(w, "")
				return
			}
			finishReason, ok := mapStreamFinishReason(
				resp.StopReason,
				strings.TrimSpace(resp.Content) != "",
				len(resp.ToolCalls) > 0,
			)
			if !ok {
				_ = writeStreamError(w, resp.StopReason)
				return
			}

			// Send role chunk
			roleChunk := OAIResponse{
				ID:      completionID,
				Object:  "chat.completion.chunk",
				Created: created,
				Model:   model,
				Choices: []OAIChoice{{
					Index: 0,
					Delta: &OAIMessage{Role: "assistant"},
				}},
			}
			_ = writeStreamChunk(w, roleChunk)

			// Send content
			if resp.Content != "" {
				contentChunk := OAIResponse{
					ID:      completionID,
					Object:  "chat.completion.chunk",
					Created: created,
					Model:   model,
					Choices: []OAIChoice{{
						Index: 0,
						Delta: &OAIMessage{Content: resp.Content},
					}},
				}
				_ = writeStreamChunk(w, contentChunk)
			}

			// Send tool calls if any
			for i, tc := range resp.ToolCalls {
				idx := i
				tcChunk := OAIResponse{
					ID:      completionID,
					Object:  "chat.completion.chunk",
					Created: created,
					Model:   model,
					Choices: []OAIChoice{{
						Index: 0,
						Delta: &OAIMessage{
							ToolCalls: []OAIToolCall{{
								Index: &idx,
								ID:    tc.ID,
								Type:  "function",
								Function: OAIFunctionCall{
									Name:      tc.Name,
									Arguments: string(tc.Arguments),
								},
							}},
						},
					}},
				}
				_ = writeStreamChunk(w, tcChunk)
			}

			// Send finish
			finishChunk := OAIResponse{
				ID:      completionID,
				Object:  "chat.completion.chunk",
				Created: created,
				Model:   model,
				Choices: []OAIChoice{{
					Index:        0,
					Delta:        &OAIMessage{},
					FinishReason: &finishReason,
				}},
			}
			_ = writeStreamChunk(w, finishChunk)

			// Send usage if requested
			if streamOpts != nil && streamOpts.IncludeUsage {
				usageChunk := OAIResponse{
					ID:      completionID,
					Object:  "chat.completion.chunk",
					Created: created,
					Model:   model,
					Choices: []OAIChoice{},
					Usage: &OAIUsage{
						PromptTokens:     resp.InputTokens,
						CompletionTokens: resp.OutputTokens,
						TotalTokens:      resp.InputTokens + resp.OutputTokens,
					},
				}
				_ = writeStreamChunk(w, usageChunk)
			}

			_ = writeStreamDone(w)
			return
		}

		// Send initial role chunk
		roleChunk := OAIResponse{
			ID:      completionID,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   model,
			Choices: []OAIChoice{{
				Index: 0,
				Delta: &OAIMessage{Role: "assistant"},
			}},
		}
		_ = writeStreamChunk(w, roleChunk)

		toolCallIndex := 0
		hasNonBlankContent := false
		terminalSeen := false
		for chunk := range streamCh {
			if chunk.Error != nil {
				oaiLog.Error(chunk.Error, "stream error")
				_ = writeStreamError(w, "provider_error")
				return
			}

			if chunk.Content != "" {
				hasNonBlankContent = hasNonBlankContent || strings.TrimSpace(chunk.Content) != ""
				contentChunk := OAIResponse{
					ID:      completionID,
					Object:  "chat.completion.chunk",
					Created: created,
					Model:   model,
					Choices: []OAIChoice{{
						Index: 0,
						Delta: &OAIMessage{Content: chunk.Content},
					}},
				}
				_ = writeStreamChunk(w, contentChunk)
			}

			if chunk.ToolCall != nil {
				tc := chunk.ToolCall
				idx := toolCallIndex
				tcChunk := OAIResponse{
					ID:      completionID,
					Object:  "chat.completion.chunk",
					Created: created,
					Model:   model,
					Choices: []OAIChoice{{
						Index: 0,
						Delta: &OAIMessage{
							ToolCalls: []OAIToolCall{{
								Index: &idx,
								ID:    tc.ID,
								Type:  "function",
								Function: OAIFunctionCall{
									Name:      tc.Name,
									Arguments: string(tc.Arguments),
								},
							}},
						},
					}},
				}
				_ = writeStreamChunk(w, tcChunk)
				toolCallIndex++
			}

			if chunk.Done {
				finishReason, ok := mapStreamFinishReason(
					chunk.StopReason,
					hasNonBlankContent,
					toolCallIndex > 0,
				)
				if !ok {
					_ = writeStreamError(w, chunk.StopReason)
					return
				}
				finishChunk := OAIResponse{
					ID:      completionID,
					Object:  "chat.completion.chunk",
					Created: created,
					Model:   model,
					Choices: []OAIChoice{{
						Index:        0,
						Delta:        &OAIMessage{},
						FinishReason: &finishReason,
					}},
				}
				_ = writeStreamChunk(w, finishChunk)
				if streamOpts != nil && streamOpts.IncludeUsage && (chunk.InputTokens > 0 || chunk.OutputTokens > 0) {
					usageChunk := OAIResponse{
						ID:      completionID,
						Object:  "chat.completion.chunk",
						Created: created,
						Model:   model,
						Choices: []OAIChoice{},
						Usage: &OAIUsage{
							PromptTokens:     chunk.InputTokens,
							CompletionTokens: chunk.OutputTokens,
							TotalTokens:      chunk.InputTokens + chunk.OutputTokens,
						},
					}
					_ = writeStreamChunk(w, usageChunk)
				}
				terminalSeen = true
				break
			}
		}
		if !terminalSeen {
			_ = writeStreamError(w, "missing_terminal_chunk")
			return
		}

		_ = writeStreamDone(w)
	})
}

// HandleListModels handles GET /openai/v1/models.
func (h *OpenAICompatHandler) HandleListModels(c fiber.Ctx) error {
	ctx := c.Context()

	namespace, err := ResolveNamespace(c, c.Query("namespace", ""), h.watchNamespace, h.enforceNamespaceIsolation)
	if err != nil {
		return openAIContextTokenAuthorizationError(c, err)
	}

	if err := authorizeContextTokenActionWithConfig(c, h.contextTokenAuthorization, "openAIListModels", h.contextTokenAuthorization.ProviderUseScopes); err != nil {
		return openAIContextTokenAuthorizationError(c, err)
	}

	// List Provider CRDs to build a model list
	providerList := &corev1alpha1.ProviderList{}
	listOpts := []client.ListOption{}
	if namespace != "" {
		listOpts = append(listOpts, client.InNamespace(namespace))
	}
	if err := h.client.List(ctx, providerList, listOpts...); err != nil {
		oaiLog.Error(err, "failed to list providers")
		return c.Status(500).JSON(OAIError{Error: OAIErrorDetail{
			Message: "failed to list providers",
			Type:    "server_error",
		}})
	}

	now := time.Now().Unix()
	models := []OAIModel{}
	seen := make(map[string]bool)

	for _, p := range providerList.Items {
		if p.Spec.DefaultModel != "" {
			if !contextTokenAllowsListedProviderModel(c, h.contextTokenAuthorization, "openAIListModels", namespace, providerResolutionInfo(&p), p.Spec.DefaultModel) {
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
			// Also add the plain model name
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

// convertOAIMessages converts OpenAI messages to internal llm.Message format.
// Extracts the system prompt from system messages.
func convertOAIMessages(msgs []OAIMessage) ([]llm.Message, string) {
	var systemPrompt string
	messages := make([]llm.Message, 0, len(msgs))

	for _, m := range msgs {
		if m.Role == oaiRoleSystem {
			systemPrompt = extractContent(m.Content)
			continue
		}

		msg := llm.Message{
			Role:       m.Role,
			Content:    extractContent(m.Content),
			ToolCallID: m.ToolCallID,
			Name:       m.Name,
		}

		// Convert tool calls
		if len(m.ToolCalls) > 0 {
			msg.ToolCalls = make([]llm.ToolCall, 0, len(m.ToolCalls))
			for _, tc := range m.ToolCalls {
				msg.ToolCalls = append(msg.ToolCalls, llm.ToolCall{
					ID:        tc.ID,
					Name:      tc.Function.Name,
					Arguments: json.RawMessage(tc.Function.Arguments),
				})
			}
		}

		messages = append(messages, msg)
	}

	return messages, systemPrompt
}

// extractContent extracts string content from an OAI message content field.
// Content can be a string or an array of content parts.
func extractContent(content any) string {
	if content == nil {
		return ""
	}

	switch v := content.(type) {
	case string:
		return v
	case []any:
		// Array of content parts — extract text parts
		var parts []string
		for _, part := range v {
			if m, ok := part.(map[string]any); ok {
				if t, ok := m["type"].(string); ok && t == oaiContentTypeText {
					if text, ok := m["text"].(string); ok {
						parts = append(parts, text)
					}
				}
			}
		}
		return strings.Join(parts, "\n")
	default:
		// Try JSON marshal/unmarshal for other types
		b, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		var s string
		if json.Unmarshal(b, &s) == nil {
			return s
		}
		return string(b)
	}
}

// convertOAITools converts OpenAI tool definitions to internal llm.Tool format.
func convertOAITools(inputTools []OAITool) []llm.Tool {
	if len(inputTools) == 0 {
		return nil
	}

	result := make([]llm.Tool, 0, len(inputTools))
	for _, t := range inputTools {
		if t.Type != "function" {
			continue
		}
		result = append(result, llm.Tool{
			Name:        t.Function.Name,
			Description: t.Function.Description,
			Parameters:  t.Function.Parameters,
		})
	}
	return result
}

func mapStreamFinishReason(reason string, hasNonBlankContent, hasToolCalls bool) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case finishReasonStopSequence:
		if hasToolCalls {
			return finishReasonToolCalls, true
		}
		return finishReasonStop, true
	case oaiStopReasonEndTurn, finishReasonStop, completionStatusCompleted, "response.completed":
		if hasToolCalls {
			return finishReasonToolCalls, true
		}
		if !hasNonBlankContent {
			return "", false
		}
		return finishReasonStop, true
	case oaiStopReasonToolUse, finishReasonToolCalls, finishReasonFunctionCall:
		if !hasToolCalls {
			return "", false
		}
		return finishReasonToolCalls, true
	case oaiParamMaxTokens, oaiStopReasonLength:
		return oaiStopReasonLength, true
	case finishReasonContentFilter:
		return finishReasonContentFilter, true
	default:
		return "", false
	}
}

// writeStreamChunk writes a single SSE chunk in OpenAI streaming format.
func writeStreamChunk(w *bufio.Writer, chunk OAIResponse) error {
	data, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	return w.Flush()
}

func writeStreamError(w *bufio.Writer, reason string) error {
	message := "provider stream ended without a successful terminal outcome"
	if strings.TrimSpace(reason) != "" {
		message = fmt.Sprintf("provider stream ended with non-success outcome %q", reason)
	}
	data, err := json.Marshal(OAIError{Error: OAIErrorDetail{
		Message: message,
		Type:    "server_error",
	}})
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	return w.Flush()
}

// writeStreamDone writes the final [DONE] marker for OpenAI streaming.
func writeStreamDone(w *bufio.Writer) error {
	if _, err := fmt.Fprintf(w, "data: [DONE]\n\n"); err != nil {
		return err
	}
	return w.Flush()
}
