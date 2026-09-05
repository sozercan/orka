/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/controller"
	"github.com/orka-agents/orka/internal/executionmode"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/llm"
	"github.com/orka-agents/orka/internal/store"
	chattools "github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/internal/tracing"
	"github.com/orka-agents/orka/internal/tracing/genai"
)

var chatLog = logf.Log.WithName("chat-handler")

const defaultNamespace = "default"

// ChatConfig holds configuration for the chat handler.
type ChatConfig struct {
	Enabled                bool
	Provider               string
	Model                  string
	MaxIterations          int
	MaxDuration            time.Duration
	ToolTimeout            time.Duration
	MaxConcurrent          int
	MaxTasksPerTurn        int
	MaxSessionSize         int // bytes
	MaxPrematureEndRetries int // re-prompts when the model emits text without the GOAL_STATE sentinel
	RuntimeAvailability    ACPRuntimeAvailability
	ExecutionMode          executionmode.Mode
}

// ACPRuntimeAvailability identifies built-in profiles backed by configured,
// digest-pinned RuntimePool images.
type ACPRuntimeAvailability struct {
	Codex    bool
	Claude   bool
	Copilot  bool
	OpenCode bool
}

// ChatRequest is the request body for POST /api/v1/chat.
type ChatRequest struct {
	Message      string   `json:"message"`
	SessionID    string   `json:"sessionId,omitempty"`
	Provider     string   `json:"provider,omitempty"`
	Model        string   `json:"model,omitempty"`
	Namespace    string   `json:"namespace,omitempty"`
	Temperature  *float64 `json:"temperature,omitempty"`
	MaxTokens    *int32   `json:"maxTokens,omitempty"`
	SystemPrompt string   `json:"systemPrompt,omitempty"`
	AgentRef     string   `json:"agentRef,omitempty"`
}

// ChatResponse is the response body for POST /api/v1/chat (JSON mode).
type ChatResponse struct {
	SessionID string         `json:"sessionId"`
	Message   string         `json:"message"`
	ToolCalls []ToolCallInfo `json:"toolCalls,omitempty"`
	Usage     ChatUsage      `json:"usage"`
}

// ToolCallInfo describes a tool invocation and its result.
type ToolCallInfo struct {
	Name   string `json:"name"`
	Args   any    `json:"args"`
	Result any    `json:"result"`
}

// ChatUsage holds usage statistics for a chat turn.
type ChatUsage struct {
	InputTokens  int    `json:"inputTokens"`
	OutputTokens int    `json:"outputTokens"`
	LLMCalls     int    `json:"llmCalls"`
	ToolCalls    int    `json:"toolCalls"`
	TasksCreated int    `json:"tasksCreated"`
	Duration     string `json:"duration"`
}

// SSEEvent is the payload for server-sent events during streaming.
type SSEEvent struct {
	SessionID string `json:"sessionId"`
	Content   string `json:"content,omitempty"`
	ToolCall  any    `json:"toolCall,omitempty"`
	Error     any    `json:"error,omitempty"`
	Usage     any    `json:"usage,omitempty"`
}

type activeChatRequest struct {
	cancel          context.CancelFunc
	done            chan struct{}
	deleteRequested bool
	deleteWaiters   int
}

type activeChatHandle struct {
	cancelContext context.Context
	finish        func()
	ownerName     string
	ownerUID      string
}

type activeChatReservation struct {
	cancelContext context.Context
	cancel        context.CancelFunc
	request       *activeChatRequest
	key           string
	once          sync.Once
}

type chatSessionLockContextKey struct{}

type chatSessionLockIdentity struct {
	ownerName string
	ownerUID  string
}

// ChatHandler implements the orchestrator chat endpoints.
type ChatHandler struct {
	client                    client.Client
	apiReader                 client.Reader
	kubeClient                kubernetes.Interface
	sessionManager            *controller.SessionManager
	config                    ChatConfig
	semaphore                 chan struct{}
	watchNamespace            string
	enforceNamespaceIsolation bool
	sessionStore              store.SessionStore
	resultStore               store.ResultStore
	contextTokenAuthorization ContextTokenAuthorizationConfig
	cooldownTracker           *llm.CooldownTracker
	resolver                  *ProviderResolver
	activeChatsMu             sync.Mutex
	activeChats               map[string]*activeChatRequest
}

// NewChatHandler creates a new ChatHandler.
func NewChatHandler(c client.Client, apiReader client.Reader, sm *controller.SessionManager, config ChatConfig, watchNamespace string, enforceNS bool, ss store.SessionStore, rs store.ResultStore, resolver *ProviderResolver, kubeClientOpt ...kubernetes.Interface) *ChatHandler {
	var kubeClient kubernetes.Interface
	if len(kubeClientOpt) > 0 {
		kubeClient = kubeClientOpt[0]
	}

	return &ChatHandler{
		client:                    c,
		apiReader:                 apiReader,
		kubeClient:                kubeClient,
		sessionManager:            sm,
		config:                    config,
		semaphore:                 make(chan struct{}, config.MaxConcurrent),
		watchNamespace:            watchNamespace,
		enforceNamespaceIsolation: enforceNS,
		sessionStore:              ss,
		resultStore:               rs,
		cooldownTracker:           llm.NewCooldownTracker(),
		resolver:                  resolver,
		activeChats:               make(map[string]*activeChatRequest),
	}
}

func (ch *ChatHandler) contextTokenAuthorizationReader() client.Reader {
	if ch.apiReader != nil {
		return ch.apiReader
	}
	return ch.client
}

// blockedNamespaces that cannot be targeted by chat requests.
var blockedNamespaces = map[string]bool{
	"kube-system": true,
	"kube-public": true,
}

// HandleChat handles POST /api/v1/chat.
//
//nolint:gocyclo // Auth, provider setup, JSON/SSE streaming, cancellation, and Session fencing share one request boundary.
func (ch *ChatHandler) HandleChat(c fiber.Ctx) error {
	var req ChatRequest
	if err := c.Bind().JSON(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}

	if req.Message == "" {
		return fiber.NewError(fiber.StatusBadRequest, "message is required")
	}

	userInfo := GetUserInfo(c)
	var contextToken *ContextToken
	if userInfo != nil {
		contextToken = userInfo.ContextToken
	}

	// Try to acquire semaphore (non-blocking)
	select {
	case ch.semaphore <- struct{}{}:
	default:
		c.Set("Retry-After", "5")
		return fiber.NewError(fiber.StatusTooManyRequests, "too many concurrent chat requests")
	}
	sseMode := false
	defer func() {
		if !sseMode {
			<-ch.semaphore
		}
	}()

	// Derive non-SSE work from the Fiber user context so chat.request,
	// tool-loop, and GenAI spans remain children of the API SERVER span.
	// SSE mode captures the span context before returning and seeds its
	// long-lived background context below.
	ctx, cancel := context.WithTimeout(c.Context(), ch.config.MaxDuration)
	defer cancel()

	// Resolve namespace from request or token
	namespace, err := ResolveNamespace(c, req.Namespace, ch.watchNamespace, ch.enforceNamespaceIsolation)
	if err != nil {
		return err
	}

	if blockedNamespaces[namespace] {
		return fiber.NewError(fiber.StatusForbidden, fmt.Sprintf("namespace %q is not allowed for chat", namespace))
	}
	if err := authorizeContextTokenAgentContext(c, ch.contextTokenAuthorization, "chat", namespace, req.AgentRef); err != nil {
		return err
	}

	// Resolve or create session ID
	sessionID := resolveChatSessionID(req.SessionID)
	reservation, err := ch.reserveActiveChat(namespace, sessionID)
	if err != nil {
		return chatSessionLockError(err)
	}
	var activeChat *activeChatHandle
	chatLockHandedOff := false
	stopRequestCancellation := context.AfterFunc(reservation.cancelContext, cancel)
	defer stopRequestCancellation()
	defer func() {
		if activeChat == nil {
			ch.finishActiveChatReservation(reservation, nil)
		} else if !chatLockHandedOff {
			activeChat.finish()
		}
	}()

	// Resolve LLM provider
	provider, model, providerInfo, err := ch.resolver.ResolveWithInfo(ctx, ResolveOpts{
		ProviderName: req.Provider,
		Model:        req.Model,
		AgentRef:     req.AgentRef,
		Namespace:    namespace,
		AuthorizeProviderReference: func(provider ProviderResolutionInfo) error {
			return authorizeContextTokenProviderReference(c, ch.contextTokenAuthorization, "chatProviderReference", namespace, provider)
		},
		AuthorizeProviderUse: func(provider ProviderResolutionInfo, model string) error {
			return authorizeContextTokenProviderUse(c, ch.contextTokenAuthorization, "chat", namespace, provider, model)
		},
		// Enforced scoped context tokens get no implicit Provider selection;
		// callers must name one directly or use an Agent bound to one.
		RequireExplicitProvider: requestRequiresExplicitProvider(c, ch.contextTokenAuthorization),
	})
	if err != nil {
		if ferr, ok := err.(*fiber.Error); ok && ferr.Code == fiber.StatusForbidden {
			return err
		}
		chatLog.Error(err, "failed to resolve provider")
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("failed to resolve provider: %v", err))
	}

	// Wrap provider with retry and fallback
	provider, err = ch.wrapWithRetryAndFallback(ctx, c, provider, req, namespace)
	if err != nil {
		return err
	}

	// Wrap provider with tracing
	provider = llm.NewTracingProvider(provider)

	// Start chat.request span
	tracer := tracing.Tracer("orka.chat")
	ctx, span := tracer.Start(ctx, "chat.request",
		trace.WithAttributes(
			attribute.String("session.id", sessionID),
			attribute.String(genai.AttrConversationID, sessionID),
			attribute.String("chat.provider", provider.Name()),
			attribute.String("chat.model", model),
		),
	)
	defer func() {
		if !sseMode {
			span.End()
		}
	}()

	// Build system prompt
	promptBuilder := NewSystemPromptBuilder(ch.client, namespace, ch.config.RuntimeAvailability)
	systemPrompt, err := promptBuilder.BuildSystemPrompt(ctx, req.SystemPrompt, PromptModeFull)
	if err != nil {
		chatLog.Error(err, "failed to build system prompt")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to build system prompt")
	}
	activeChat, err = ch.activateReservedChat(ctx, reservation, namespace, sessionID)
	if err != nil {
		return chatSessionLockError(err)
	}
	ctx = context.WithValue(ctx, chatSessionLockContextKey{}, chatSessionLockIdentity{
		ownerName: activeChat.ownerName,
		ownerUID:  activeChat.ownerUID,
	})

	// Load session history
	messages, err := ch.loadChatSession(ctx, namespace, sessionID)
	if err != nil {
		if errors.Is(err, store.ErrGatewayOwnedSession) {
			return fiber.NewError(fiber.StatusNotFound, "chat session not found")
		}
		chatLog.Info("no existing session, starting fresh", "sessionId", sessionID, "error", err)
		messages = []llm.Message{}
	}
	persistedCount := len(messages)

	// Append user message — if an agentRef is set and the agent has a runtime,
	// prepend context so the LLM knows to use create_agent_task.
	userContent := req.Message
	if req.AgentRef != "" {
		agentObj := &corev1alpha1.Agent{}
		if err := ch.client.Get(ctx, types.NamespacedName{Name: req.AgentRef, Namespace: namespace}, agentObj); err == nil {
			if agentObj.Spec.Runtime != nil {
				userContent = fmt.Sprintf("[Using agent %q which has runtime %q — use create_agent_task with agent=%q for this request.]\n\n%s",
					req.AgentRef, agentObj.Spec.Runtime.Type, req.AgentRef, req.Message)
			}
		}
	}

	// Auto-route to dev-coordinator for issue/PR workflows.
	// Chat just creates the coordinator — the coordinator creates its own specialist agents.
	if req.AgentRef == "" && looksLikeIssueWorkflow(req.Message) {
		userContent = fmt.Sprintf("[System: This is an issue workflow. "+
			"You MUST create a dev-coordinator agent using create_agent with initialPrompt (one-shot pattern). "+
			"Set coordination.enabled=true, providerRef=\"copilot\", model.name=\"gpt-5.4\", maxDepth=3, maxConcurrentChildren=5. "+
			"CRITICAL: The dev-coordinator must NOT have a runtime — it must be a plain AI agent with providerRef. "+
			"The coordinator system prompt must instruct it to: "+
			"1) Use create_agent to create any specialist agents it needs (coder as copilot runtime, reviewers as copilot runtime with different models like claude-opus-4.6, gpt-5.4, gemini-3-pro). "+
			"Set allowedAgents to include the agents it creates. "+
			"2) Follow this adaptive workflow: "+
			"Phase 1 — ANALYZE: Read the issue and determine what phases are needed. "+
			"Phase 2 — PLAN & DESIGN: If needed, delegate design/planning to a coder agent, then review with multiple reviewer agents in parallel. "+
			"Phase 3 — CODE: Delegate implementation to the coder agent with a pushBranch. "+
			"Phase 4 — VALIDATE: Before review, determine the validation image and command from repository evidence such as CI workflows, toolchain files, Dockerfiles/devcontainers, Makefiles, and docs. Run validation with create_container_task on an immutable ref when available. If the environment cannot be determined confidently, report VALIDATION_CONFIG_BLOCKED. Allow up to 6 validation repair tasks before reporting VALIDATION_BLOCKED. "+
			"Phase 5 — REVIEW LOOP: Delegate parallel reviews to reviewer agents, then delegate coder repairs on the same branch until validation passes and all reviewers approve. Bound this to at most 8 review repair tasks. "+
			"Phase 6 — PR + CI LOOP: After validation passes and reviewers approve, create or update the PR, then call check_pull_request_ci once with wait_timeout=\"30m\" and poll_interval=\"30s\". If checks fail, delegate a focused CI repair task to the coder on the PR branch, then re-run validation and reviewers before re-checking CI. Bound this to at most 3 CI repair tasks; if the CI check times out while still pending, report CI_PENDING. "+
			"Phase 7 — APPROVE: Post final approval only after validation passes, reviewers approve, and CI is green. Prefer additional focused repair iterations over stopping early when reviewers identify concrete diff-backed security, correctness, or acceptance-criteria issues. Report VALIDATION_BLOCKED, REVIEW_BLOCKED, CI_BLOCKED, or CI_PENDING when a bounded loop is exhausted. Do not merge unless the user explicitly asks. "+
			"Use initialPrompt to pass the user's request so the coordinator starts immediately. "+
			"Include the gitRepo URL in the initialPrompt.]\n\n%s", req.Message)
	}
	messages = append(messages, llm.Message{
		Role:    "user",
		Content: userContent,
	})

	// Create tool executor (also creates the chat registry)
	executor := NewToolExecutor(ch.client, ch.sessionManager, namespace, sessionID, ch.watchNamespace, ch.enforceNamespaceIsolation, ch.config.MaxTasksPerTurn, ch.config.ToolTimeout, ch.resultStore, ch.kubeClient)
	executor.SetExecutionMode(ch.config.ExecutionMode)
	executor.provider = providerInfo.Name
	executor.providerType = providerInfo.Type
	authorizationReader := ch.contextTokenAuthorizationReader()
	executor.SetPolicyReader(authorizationReader)
	executor.SetTaskCreateAuthorizer(func(ctx context.Context, task *corev1alpha1.Task) error {
		return authorizeAndStampToolTaskCreate(ctx, authorizationReader, ch.kubeClient, contextToken, ch.contextTokenAuthorization, "chatToolCreateTask", userInfo, task)
	})
	executor.SetTaskDeleteAuthorizer(func(ctx context.Context, task *corev1alpha1.Task) error {
		return authorizeContextTokenTaskDeleteObject(ctx, authorizationReader, contextToken, ch.contextTokenAuthorization, "chatToolDeleteTask", task)
	})
	executor.SetAgentCreateAuthorizer(func(ctx context.Context, agent *corev1alpha1.Agent) error {
		return authorizeContextTokenToolAgentCreate(ctx, authorizationReader, contextToken, ch.contextTokenAuthorization, "chatToolCreateAgent", agent)
	})
	executor.SetAgentUpdateAuthorizer(func(ctx context.Context, agent *corev1alpha1.Agent) error {
		return authorizeContextTokenToolAgentUpdate(ctx, authorizationReader, contextToken, ch.contextTokenAuthorization, "chatToolUpdateAgent", agent)
	})
	executor.SetAgentDeleteAuthorizer(func(ctx context.Context, agent *corev1alpha1.Agent) error {
		return authorizeContextTokenToolAgentDelete(contextToken, ch.contextTokenAuthorization, "chatToolDeleteAgent", agent)
	})
	executor.SetSecretReadAuthorizer(func(ctx context.Context, namespace, secretName string) error {
		return authorizeContextTokenSecretRead(contextToken, ch.contextTokenAuthorization, "chatToolReadSecret", namespace, secretName)
	})

	// Build tools from the chat registry and restrict execution to the exposed set.
	tools := executor.registry.ToLLMTools(chattools.ChatToolNames())
	tools = filterCompletionToolsForContextToken(c, ch.contextTokenAuthorization, tools)
	if err := authorizeContextTokenToolUse(c, ch.contextTokenAuthorization, "chatTools", completionToolNames(tools)); err != nil {
		return err
	}
	executor.SetAllowedTools(tools)

	// Build completion request parameters
	var temperature float64
	if req.Temperature != nil {
		temperature = *req.Temperature
	}
	maxTokens := 4096
	if req.MaxTokens != nil {
		maxTokens = int(*req.MaxTokens)
	}

	// Check Accept header for response format
	accept := c.Get("Accept")
	if accept == "application/json" {
		// JSON mode: run tool loop, collect all content, return JSON
		content, usage, toolCalls, err := ch.runToolLoop(ctx, provider, messages, systemPrompt, tools, executor, sessionID, namespace, model, temperature, maxTokens, persistedCount, nil)
		if err != nil {
			chatLog.Error(err, "tool loop error")
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("chat error: %v", err))
		}
		return c.JSON(ChatResponse{
			SessionID: sessionID,
			Message:   content,
			ToolCalls: toolCalls,
			Usage:     usage,
		})
	}

	// SSE mode
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no")

	// Capture values for the streaming closure (ctx from outer scope is cancelled
	// when HandleChat returns, so we create a new context inside the callback)
	sseProvider := provider
	sseMessages := messages
	sseSystemPrompt := systemPrompt
	sseTools := tools
	sseExecutor := executor
	chatDeadline, hasChatDeadline := ctx.Deadline()
	sseParentCtx := baggage.ContextWithBaggage(
		trace.ContextWithSpanContext(context.Background(), span.SpanContext()),
		baggage.FromContext(ctx),
	)
	sseParentCtx = context.WithValue(sseParentCtx, chatSessionLockContextKey{}, chatSessionLockIdentity{
		ownerName: activeChat.ownerName,
		ownerUID:  activeChat.ownerUID,
	})

	sseMode = true
	chatLockHandedOff = true
	streamErr := c.SendStreamWriter(func(w *bufio.Writer) {
		defer span.End()
		defer func() { <-ch.semaphore }()
		defer activeChat.finish()
		// SendStreamWriter outlives the handler, so use a background context
		// seeded with the originating chat span context rather than Fiber's
		// recycled request context.
		var sseCtx context.Context
		var sseCancel context.CancelFunc
		if hasChatDeadline {
			sseCtx, sseCancel = context.WithDeadline(sseParentCtx, chatDeadline)
		} else {
			sseCtx, sseCancel = context.WithTimeout(sseParentCtx, ch.config.MaxDuration)
		}
		defer sseCancel()
		stopSSECancellation := context.AfterFunc(activeChat.cancelContext, sseCancel)
		defer stopSSECancellation()

		emitSSE := func(event, data string) {
			_ = writeSSE(w, event, data)
		}

		// Emit status event
		statusData, _ := json.Marshal(map[string]string{
			"sessionId": sessionID,
			"provider":  sseProvider.Name(),
			"model":     model,
		})
		emitSSE("status", string(statusData))

		content, usage, _, err := ch.runToolLoop(sseCtx, sseProvider, sseMessages, sseSystemPrompt, sseTools, sseExecutor, sessionID, namespace, model, temperature, maxTokens, persistedCount, emitSSE)
		if err != nil {
			errData, _ := json.Marshal(map[string]string{"error": err.Error()})
			emitSSE("error", string(errData))
		}

		_ = content // content already emitted via SSE

		// Emit done event
		doneData, _ := json.Marshal(map[string]any{"usage": usage})
		emitSSE("done", string(doneData))
	})
	if streamErr != nil {
		sseMode = false
		chatLockHandedOff = false
	}
	return streamErr
}

func resolveChatSessionID(requested string) string {
	if requested != "" {
		return requested
	}
	return fmt.Sprintf("chat-%s", generateChatID())
}

func chatSessionLockError(err error) error {
	switch {
	case errors.Is(err, store.ErrGatewayOwnedSession), errors.Is(err, store.ErrNotFound):
		return fiber.NewError(fiber.StatusNotFound, "chat session not found")
	case errors.Is(err, store.ErrConflict):
		return fiber.NewError(fiber.StatusConflict, "chat session is active or its deleted name is reserved")
	default:
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to lock chat session: %v", err))
	}
}

func (ch *ChatHandler) reserveActiveChat(namespace, sessionID string) (*activeChatReservation, error) {
	cancelContext, cancel := context.WithCancel(context.Background())
	request := &activeChatRequest{cancel: cancel, done: make(chan struct{})}
	reservation := &activeChatReservation{
		cancelContext: cancelContext,
		cancel:        cancel,
		request:       request,
		key:           activeChatKey(namespace, sessionID),
	}
	ch.activeChatsMu.Lock()
	if ch.activeChats == nil {
		ch.activeChats = make(map[string]*activeChatRequest)
	}
	if _, exists := ch.activeChats[reservation.key]; exists {
		ch.activeChatsMu.Unlock()
		cancel()
		return nil, fmt.Errorf("%w: chat session %s/%s already has an active request", store.ErrConflict, namespace, sessionID)
	}
	ch.activeChats[reservation.key] = request
	ch.activeChatsMu.Unlock()
	return reservation, nil
}

func (ch *ChatHandler) finishActiveChatReservation(reservation *activeChatReservation, release func()) {
	if reservation == nil {
		return
	}
	reservation.once.Do(func() {
		reservation.cancel()
		if release != nil {
			release()
		}
		ch.activeChatsMu.Lock()
		if ch.activeChats[reservation.key] == reservation.request && reservation.request.deleteWaiters == 0 {
			delete(ch.activeChats, reservation.key)
		}
		close(reservation.request.done)
		ch.activeChatsMu.Unlock()
	})
}

func (ch *ChatHandler) activateReservedChat(
	ctx context.Context,
	reservation *activeChatReservation,
	namespace, sessionID string,
) (*activeChatHandle, error) {
	acquireCtx, acquireCancel := context.WithCancel(ctx)
	stopAcquireCancellation := context.AfterFunc(reservation.cancelContext, acquireCancel)
	release, _, lockID, err := ch.acquireChatSession(acquireCtx, namespace, sessionID)
	stopAcquireCancellation()
	acquireCancel()
	if err != nil {
		return nil, err
	}
	return &activeChatHandle{
		cancelContext: reservation.cancelContext,
		finish:        func() { ch.finishActiveChatReservation(reservation, release) },
		ownerName:     lockID,
		ownerUID:      lockID,
	}, nil
}

func (ch *ChatHandler) acquireChatSession(ctx context.Context, namespace, sessionID string) (func(), bool, string, error) {
	if ch.sessionStore == nil {
		return nil, false, "", fmt.Errorf("chat Session store is not configured")
	}
	created := false
	sessionType, err := transcriptSessionType(ctx, ch.sessionStore, namespace, sessionID)
	if errors.Is(err, store.ErrNotFound) {
		now := time.Now().UTC()
		createErr := ch.sessionStore.CreateSession(ctx, &store.SessionRecord{
			Namespace: namespace, Name: sessionID, SessionType: "chat", CreatedAt: now, UpdatedAt: now,
		})
		if createErr != nil {
			sessionType, err = transcriptSessionType(ctx, ch.sessionStore, namespace, sessionID)
			if err != nil {
				return nil, false, "", createErr
			}
		} else {
			created = true
			sessionType = "chat"
			err = nil
		}
	}
	if err != nil {
		return nil, created, "", err
	}
	if sessionType == store.SessionTypeGateway {
		return nil, created, "", store.ErrGatewayOwnedSession
	}
	lockID := "chat-request-" + generateChatID()
	var lockErr error
	if expiring, ok := ch.sessionStore.(store.ExpiringSessionLockStore); ok {
		lockExpiresAt := time.Now().UTC().Add(ch.config.MaxDuration + time.Minute)
		if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
			lockExpiresAt = deadline.UTC().Add(time.Minute)
		}
		lockErr = expiring.AcquireLockUntil(ctx, namespace, sessionID, lockID, lockID, lockExpiresAt)
	} else {
		lockErr = ch.sessionStore.AcquireLock(ctx, namespace, sessionID, lockID, lockID)
	}
	if lockErr != nil {
		return nil, created, "", lockErr
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := ch.sessionStore.ReleaseLock(releaseCtx, namespace, sessionID, lockID, lockID); err != nil && !errors.Is(err, store.ErrNotFound) {
				chatLog.Error(err, "failed to release chat Session lock", "namespace", namespace, "sessionId", sessionID)
			}
		})
	}, created, lockID, nil
}

func activeChatKey(namespace, sessionID string) string {
	return namespace + "\x00" + sessionID
}

func (ch *ChatHandler) cancelActiveChat(namespace, sessionID string) (*activeChatRequest, bool) {
	key := activeChatKey(namespace, sessionID)
	ch.activeChatsMu.Lock()
	request := ch.activeChats[key]
	if request == nil {
		ch.activeChatsMu.Unlock()
		return nil, false
	}
	request.deleteRequested = true
	request.deleteWaiters++
	ch.activeChatsMu.Unlock()
	request.cancel()
	return request, true
}

func (ch *ChatHandler) deleteChatSession(ctx context.Context, namespace, sessionID string) error {
	if ch.sessionManager != nil {
		return ch.sessionManager.DeleteSession(ctx, namespace, sessionID)
	}
	return ch.sessionStore.DeleteSession(ctx, namespace, sessionID)
}

// runToolLoop executes the agentic tool loop until the LLM produces a final text response.
func (ch *ChatHandler) runToolLoop(
	ctx context.Context,
	provider llm.Provider,
	messages []llm.Message,
	systemPrompt string,
	tools []llm.Tool,
	executor *ToolExecutor,
	sessionID, namespace, model string,
	temperature float64,
	maxTokens int,
	persistedCount int,
	emitSSE func(event, data string),
) (string, ChatUsage, []ToolCallInfo, error) {
	var usage ChatUsage
	var allToolCalls []ToolCallInfo
	repetitionTracker := make(map[string]int)
	start := time.Now()

	for iteration := 0; ; iteration++ {
		iterTracer := tracing.Tracer("orka.chat")
		iterCtx, iterSpan := iterTracer.Start(ctx, "chat.tool_loop.iteration",
			trace.WithAttributes(
				attribute.Int("chat.iteration", iteration),
				attribute.String(tracing.AttrTenant, namespace),
				attribute.String(genai.AttrRequestModel, model),
			),
		)

		select {
		case <-iterCtx.Done():
			usage.Duration = time.Since(start).Round(time.Millisecond).String()
			iterSpan.End()
			return "I ran out of time. Here's what I accomplished so far.", usage, allToolCalls, nil
		default:
		}

		if content, hit := ch.handleIterationLimit(iterCtx, iteration, provider, messages, systemPrompt, model, namespace, sessionID, maxTokens, persistedCount, temperature, emitSSE, executor, &usage, start); hit {
			setUsageSpanAttributes(iterSpan, usage)
			iterSpan.End()
			return content, usage, allToolCalls, nil
		}

		if iteration > 0 && iteration%5 == 0 {
			messages = append(messages, llm.Message{
				Role:    "user",
				Content: "[System: Progress check — summarize what you've done so far and what remains.]",
			})
		}

		if ch.config.MaxSessionSize > 0 {
			messages = llm.TruncateMessages(messages, ch.config.MaxSessionSize/4)
		}

		resp, updatedMsgs, err := ch.callLLMWithRetry(iterCtx, provider, messages, systemPrompt, model, tools, maxTokens, temperature)
		messages = updatedMsgs
		if err != nil {
			usage.Duration = time.Since(start).Round(time.Millisecond).String()
			iterSpan.RecordError(err)
			iterSpan.SetStatus(codes.Error, err.Error())
			iterSpan.End()
			return "", usage, allToolCalls, fmt.Errorf("LLM completion failed: %w", err)
		}
		iterSpan.SetAttributes(attribute.Int("chat.tool_call_count", len(resp.ToolCalls)))
		usage.LLMCalls++
		usage.InputTokens += resp.InputTokens
		usage.OutputTokens += resp.OutputTokens

		if len(resp.ToolCalls) == 0 {
			// Check if any tasks created in this session are still running.
			// If so, re-prompt the LLM to keep waiting instead of ending the session.
			if executor.tasksCreated > 0 && ch.hasRunningTasks(iterCtx, namespace, sessionID) {
				if emitSSE != nil && resp.Content != "" {
					msgData, _ := json.Marshal(map[string]string{"content": resp.Content})
					emitSSE("message", string(msgData))
				}
				messages = append(messages,
					llm.Message{Role: "assistant", Content: resp.Content},
					llm.Message{Role: "user", Content: "[System: You have tasks still running. Do NOT stop. Call wait_for_task again for each running task until it reaches Succeeded or Failed, then call fetch_task_output to get the result.]"},
				)
				// Don't increment iteration here — the for loop's post-statement handles it
				iterSpan.End()
				continue
			}
			content := ch.handleFinalResponse(iterCtx, resp.Content, messages, namespace, sessionID, persistedCount, emitSSE, executor, &usage, start)
			setUsageSpanAttributes(iterSpan, usage)
			iterSpan.End()
			return content, usage, allToolCalls, nil
		}

		var newToolCalls []ToolCallInfo
		var iterBump int
		messages, newToolCalls, iterBump = ch.executeToolCalls(iterCtx, resp, executor, emitSSE, messages, repetitionTracker)
		allToolCalls = append(allToolCalls, newToolCalls...)
		usage.ToolCalls += len(newToolCalls)
		iteration += iterBump
		iterSpan.End()
	}
}

// handleIterationLimit checks if the iteration limit is reached and if so,
// injects a termination prompt, makes a final LLM call, emits SSE, and saves the session.
func (ch *ChatHandler) handleIterationLimit(
	ctx context.Context,
	iteration int,
	provider llm.Provider,
	messages []llm.Message,
	systemPrompt, model, namespace, sessionID string,
	maxTokens, persistedCount int,
	temperature float64,
	emitSSE func(event, data string),
	executor *ToolExecutor,
	usage *ChatUsage,
	start time.Time,
) (string, bool) {
	if iteration < ch.config.MaxIterations {
		return "", false
	}

	messages = append(messages, llm.Message{
		Role:    "user",
		Content: "[System: You have reached the maximum number of iterations. Please provide a final summary of what you accomplished.]",
	})

	resp, err := provider.Complete(ctx, &llm.CompletionRequest{
		Model:        model,
		SystemPrompt: systemPrompt,
		Messages:     messages,
		MaxTokens:    maxTokens,
		Temperature:  temperature,
	})
	if err != nil {
		usage.Duration = time.Since(start).Round(time.Millisecond).String()
		return "Reached iteration limit.", true
	}
	usage.LLMCalls++
	usage.InputTokens += resp.InputTokens
	usage.OutputTokens += resp.OutputTokens

	if emitSSE != nil && resp.Content != "" {
		msgData, _ := json.Marshal(map[string]string{"content": resp.Content})
		emitSSE("message", string(msgData))
	}

	finalMessages := append(messages, llm.Message{Role: "assistant", Content: resp.Content})
	usage.Duration = time.Since(start).Round(time.Millisecond).String()
	usage.TasksCreated = executor.tasksCreated
	_ = ch.saveChatSession(ctx, namespace, sessionID, finalMessages, persistedCount, *usage)
	if err := ch.updateChatTokenCounts(ctx, namespace, sessionID, usage.InputTokens, usage.OutputTokens); err != nil {
		chatLog.Error(err, "failed to update token counts")
	}

	return resp.Content, true
}

// callLLMWithRetry calls the LLM provider and retries once with truncated messages
// if the context is too long.
func (ch *ChatHandler) callLLMWithRetry(
	ctx context.Context,
	provider llm.Provider,
	messages []llm.Message,
	systemPrompt, model string,
	tools []llm.Tool,
	maxTokens int,
	temperature float64,
) (*llm.CompletionResponse, []llm.Message, error) {
	resp, err := provider.Complete(ctx, &llm.CompletionRequest{
		Model:        model,
		SystemPrompt: systemPrompt,
		Messages:     messages,
		Tools:        tools,
		MaxTokens:    maxTokens,
		Temperature:  temperature,
	})
	if err != nil && llm.IsContextTooLongErr(err) {
		tokenEstimate := 0
		for _, m := range messages {
			tokenEstimate += len(m.Content) / 4
		}
		messages = llm.TruncateMessages(messages, tokenEstimate/2)
		resp, err = provider.Complete(ctx, &llm.CompletionRequest{
			Model:        model,
			SystemPrompt: systemPrompt,
			Messages:     messages,
			Tools:        tools,
			MaxTokens:    maxTokens,
			Temperature:  temperature,
		})
	}
	return resp, messages, err
}

// executeToolCalls iterates over tool calls from the LLM response, emits SSE events,
// executes each tool, tracks repetitions, and appends results to messages.
func (ch *ChatHandler) executeToolCalls(
	ctx context.Context,
	resp *llm.CompletionResponse,
	executor *ToolExecutor,
	emitSSE func(event, data string),
	messages []llm.Message,
	repetitionTracker map[string]int,
) ([]llm.Message, []ToolCallInfo, int) {
	messages = append(messages, llm.Message{
		Role:      "assistant",
		Content:   resp.Content,
		ToolCalls: resp.ToolCalls,
	})

	toolCalls := make([]ToolCallInfo, 0, len(resp.ToolCalls))
	var iterationBump int
	var repetitionWarning string

	for _, tc := range resp.ToolCalls {
		if emitSSE != nil {
			tcData, _ := json.Marshal(map[string]any{
				"id":   tc.ID,
				"name": tc.Name,
				"args": tc.Arguments,
			})
			emitSSE("tool_call", string(tcData))
		}

		argsHash := hashArgs(tc.Name, tc.Arguments)
		repetitionTracker[argsHash]++
		if repetitionTracker[argsHash] >= 3 {
			repetitionWarning = fmt.Sprintf("[System: Warning — you have called %s with the same arguments %d times. Try a different approach.]", tc.Name, repetitionTracker[argsHash])
			iterationBump += 5
		}

		result, execErr := executor.Execute(ctx, tc)
		if execErr != nil {
			errResult := map[string]any{"success": false, "error": execErr.Error()}
			if errJSON, jsonErr := json.Marshal(errResult); jsonErr == nil {
				result = string(errJSON)
			} else {
				result = `{"success":false,"error":"tool execution failed"}`
			}
		}

		if emitSSE != nil {
			trData, _ := json.Marshal(map[string]any{
				"id":     tc.ID,
				"name":   tc.Name,
				"result": json.RawMessage(result),
			})
			emitSSE("tool_result", string(trData))
		}

		messages = append(messages, llm.Message{
			Role:       "tool",
			ToolCallID: tc.ID,
			Name:       tc.Name,
			Content:    result,
		})

		var argsAny any
		_ = json.Unmarshal(tc.Arguments, &argsAny)
		var resultAny any
		_ = json.Unmarshal([]byte(result), &resultAny)
		toolCalls = append(toolCalls, ToolCallInfo{
			Name:   tc.Name,
			Args:   argsAny,
			Result: resultAny,
		})
	}

	if repetitionWarning != "" {
		messages = append(messages, llm.Message{
			Role:    "user",
			Content: repetitionWarning,
		})
	}

	return messages, toolCalls, iterationBump
}

// handleFinalResponse emits the final SSE message and saves the chat session.
func (ch *ChatHandler) handleFinalResponse(
	ctx context.Context,
	content string,
	messages []llm.Message,
	namespace, sessionID string,
	persistedCount int,
	emitSSE func(event, data string),
	executor *ToolExecutor,
	usage *ChatUsage,
	start time.Time,
) string {
	if emitSSE != nil && content != "" {
		msgData, _ := json.Marshal(map[string]string{"content": content})
		emitSSE("message", string(msgData))
	}

	finalMessages := append(messages, llm.Message{Role: "assistant", Content: content})
	usage.Duration = time.Since(start).Round(time.Millisecond).String()
	usage.TasksCreated = executor.tasksCreated
	_ = ch.saveChatSession(ctx, namespace, sessionID, finalMessages, persistedCount, *usage)
	if err := ch.updateChatTokenCounts(ctx, namespace, sessionID, usage.InputTokens, usage.OutputTokens); err != nil {
		chatLog.Error(err, "failed to update token counts")
	}

	return content
}

func setUsageSpanAttributes(span trace.Span, usage ChatUsage) {
	span.SetAttributes(
		attribute.Int("chat.llm_calls", usage.LLMCalls),
		attribute.Int("chat.input_tokens", usage.InputTokens),
		attribute.Int("chat.output_tokens", usage.OutputTokens),
	)
}

// loadChatSession loads chat session messages from the session store.
func (ch *ChatHandler) loadChatSession(ctx context.Context, namespace, sessionID string) ([]llm.Message, error) {
	sessionType, err := transcriptSessionType(ctx, ch.sessionStore, namespace, sessionID)
	if err != nil {
		return nil, err
	}
	if sessionType == store.SessionTypeGateway {
		return nil, store.ErrGatewayOwnedSession
	}
	messages, err := ch.sessionStore.LoadTranscript(ctx, namespace, sessionID, 0)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return nil, nil
	}

	// Convert store.SessionMessage to llm.Message
	llmMessages := make([]llm.Message, 0, len(messages))
	for _, msg := range messages {
		m := llm.Message{
			Role:       msg.Role,
			Content:    msg.Content,
			Name:       msg.Name,
			ToolCallID: msg.ToolCallID,
		}
		if msg.ToolCalls != nil {
			if data, err := json.Marshal(msg.ToolCalls); err == nil {
				var toolCalls []llm.ToolCall
				if json.Unmarshal(data, &toolCalls) == nil {
					m.ToolCalls = toolCalls
				}
			}
		}
		llmMessages = append(llmMessages, m)
	}

	return llmMessages, nil
}

// saveChatSession saves chat session messages to the session store.
func (ch *ChatHandler) saveChatSession(ctx context.Context, namespace, sessionID string, messages []llm.Message, persistedCount int, _ ChatUsage) error {
	// HandleChat creates and locks the Session before provider work begins. A
	// missing record here is a deletion race and must never recreate history.
	if _, err := ch.sessionStore.GetSession(ctx, namespace, sessionID); err != nil {
		return fmt.Errorf("failed to get locked chat session: %w", err)
	}

	// Only append messages that haven't been persisted yet
	newMessages := messages[persistedCount:]
	if len(newMessages) == 0 {
		return nil
	}

	// Convert llm.Message to store.SessionMessage
	storeMessages := make([]store.SessionMessage, 0, len(newMessages))
	now := time.Now()
	for _, msg := range newMessages {
		sm := store.SessionMessage{
			Role:       msg.Role,
			Content:    msg.Content,
			Name:       msg.Name,
			ToolCallID: msg.ToolCallID,
			Timestamp:  now,
		}
		if len(msg.ToolCalls) > 0 {
			sm.ToolCalls = msg.ToolCalls
		}
		storeMessages = append(storeMessages, sm)
	}

	if identity, ok := chatSessionLockFromContext(ctx); ok {
		if fenced, fencedOK := ch.sessionStore.(store.FencedSessionWriteStore); fencedOK {
			return fenced.AppendMessagesWithLock(
				ctx, namespace, sessionID, identity.ownerName, identity.ownerUID, storeMessages,
			)
		}
	}
	return ch.sessionStore.AppendMessages(ctx, namespace, sessionID, storeMessages)
}

func (ch *ChatHandler) updateChatTokenCounts(ctx context.Context, namespace, sessionID string, inputTokens, outputTokens int) error {
	if identity, ok := chatSessionLockFromContext(ctx); ok {
		if fenced, fencedOK := ch.sessionStore.(store.FencedSessionWriteStore); fencedOK {
			return fenced.UpdateTokenCountsWithLock(
				ctx, namespace, sessionID, identity.ownerName, identity.ownerUID, inputTokens, outputTokens,
			)
		}
	}
	return ch.sessionStore.UpdateTokenCounts(ctx, namespace, sessionID, inputTokens, outputTokens)
}

func chatSessionLockFromContext(ctx context.Context) (chatSessionLockIdentity, bool) {
	if ctx == nil {
		return chatSessionLockIdentity{}, false
	}
	identity, ok := ctx.Value(chatSessionLockContextKey{}).(chatSessionLockIdentity)
	return identity, ok && identity.ownerName != "" && identity.ownerUID != ""
}

// HandleChatConfig handles GET /api/v1/chat/config.
func (ch *ChatHandler) HandleChatConfig(c fiber.Ctx) error {
	toolNames := chattools.ChatToolNames()

	// Context-token callers must name a Provider explicitly: the resolver
	// refuses the implicit server default for them, so the UI must not offer
	// it.
	requireExplicitProvider := requestRequiresExplicitProvider(c, ch.contextTokenAuthorization)
	provider, model := ch.config.Provider, ch.config.Model
	if requireExplicitProvider {
		provider, model = "", ""
	}
	return c.JSON(fiber.Map{
		"enabled":                 ch.config.Enabled,
		"provider":                provider,
		"model":                   model,
		"requireExplicitProvider": requireExplicitProvider,
		"maxIterations":           ch.config.MaxIterations,
		"maxDuration":             ch.config.MaxDuration.String(),
		"maxTasksPerTurn":         ch.config.MaxTasksPerTurn,
		"maxConcurrent":           ch.config.MaxConcurrent,
		"availableTools":          toolNames,
	})
}

// HandleCancelChat handles DELETE /api/v1/chat/{sessionId}.
func (ch *ChatHandler) HandleCancelChat(c fiber.Ctx) error {
	sessionID := c.Params("sessionId")
	if sessionID == "" {
		return fiber.NewError(fiber.StatusBadRequest, "sessionId is required")
	}

	namespace, err := ResolveNamespace(c, c.Query("namespace", ""), ch.watchNamespace, ch.enforceNamespaceIsolation)
	if err != nil {
		return err
	}
	if err := authorizeContextTokenActionWithConfig(c, ch.contextTokenAuthorization, "cancelChat", ch.contextTokenAuthorization.SessionWriteScopes); err != nil {
		return err
	}

	ctx := c.Context()
	activeCancelled, err := ch.cancelAndWaitForActiveChat(ctx, namespace, sessionID)
	if err != nil {
		if errors.Is(err, store.ErrNotReady) {
			return fiber.NewError(fiber.StatusConflict, "chat cancellation is still in progress")
		}
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to cancel active chat: %v", err))
	}
	if activeCancelled {
		defer ch.clearChatDeletionGate(namespace, sessionID)
	}
	sessionType, err := transcriptSessionType(ctx, ch.sessionStore, namespace, sessionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			if activeCancelled {
				return c.SendStatus(fiber.StatusNoContent)
			}
			return fiber.NewError(fiber.StatusNotFound, "chat session not found")
		}
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get session type: %v", err))
	}
	if sessionType == store.SessionTypeGateway {
		return fiber.NewError(fiber.StatusNotFound, "chat session not found")
	}

	// Delete the session to cancel it. ACP-enabled servers route through the
	// fenced cross-store cleanup coordinator.
	if err := ch.deleteChatSession(ctx, namespace, sessionID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return c.SendStatus(fiber.StatusNoContent)
		}
		if errors.Is(err, store.ErrGatewayOwnedSession) {
			return fiber.NewError(fiber.StatusNotFound, "chat session not found")
		}
		if errors.Is(err, store.ErrConflict) {
			return fiber.NewError(fiber.StatusConflict, "chat session has active or unsettled work")
		}
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to cancel session: %v", err))
	}

	return c.SendStatus(fiber.StatusNoContent)
}

func (ch *ChatHandler) cancelAndWaitForActiveChat(ctx context.Context, namespace, sessionID string) (bool, error) {
	request, active := ch.cancelActiveChat(namespace, sessionID)
	if !active {
		return false, nil
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-request.done:
		return true, nil
	case <-timer.C:
		ch.abandonChatDeletionWaiter(namespace, sessionID, request)
		return true, fmt.Errorf("%w: active chat did not stop before cancellation deadline", store.ErrNotReady)
	case <-ctx.Done():
		ch.abandonChatDeletionWaiter(namespace, sessionID, request)
		return true, ctx.Err()
	}
}

func (ch *ChatHandler) clearChatDeletionGate(namespace, sessionID string) {
	key := activeChatKey(namespace, sessionID)
	ch.activeChatsMu.Lock()
	if request := ch.activeChats[key]; request != nil && request.deleteWaiters > 0 {
		request.deleteWaiters--
		if request.deleteWaiters == 0 {
			request.deleteRequested = false
			delete(ch.activeChats, key)
		}
	}
	ch.activeChatsMu.Unlock()
}

func (ch *ChatHandler) abandonChatDeletionWaiter(namespace, sessionID string, request *activeChatRequest) {
	key := activeChatKey(namespace, sessionID)
	ch.activeChatsMu.Lock()
	defer ch.activeChatsMu.Unlock()
	if ch.activeChats[key] != request || request.deleteWaiters == 0 {
		return
	}
	request.deleteWaiters--
	if request.deleteWaiters != 0 {
		return
	}
	request.deleteRequested = false
	select {
	case <-request.done:
		delete(ch.activeChats, key)
	default:
	}
}

// wrapWithRetryAndFallback wraps a provider with retry logic and adds fallback
// providers if the agent has them configured.
func (ch *ChatHandler) wrapWithRetryAndFallback(ctx context.Context, c fiber.Ctx, provider llm.Provider, req ChatRequest, namespace string) (llm.Provider, error) {
	var resultProvider llm.Provider = llm.NewRetryProvider(provider, 0)

	if req.AgentRef == "" {
		return resultProvider, nil
	}

	agent := &corev1alpha1.Agent{}
	if err := ch.client.Get(ctx, types.NamespacedName{Name: req.AgentRef, Namespace: namespace}, agent); err != nil {
		return resultProvider, nil
	}
	if agent.Spec.Model == nil || len(agent.Spec.Model.Fallbacks) == 0 {
		return resultProvider, nil
	}

	fallbacks := make([]llm.FallbackEntry, 0, len(agent.Spec.Model.Fallbacks))
	for _, fb := range agent.Spec.Model.Fallbacks {
		if err := authorizeContextTokenProviderReference(c, ch.contextTokenAuthorization, "chatFallbackProviderReference", namespace, ProviderResolutionInfo{Name: fb.ProviderRef, Namespace: namespace}); err != nil {
			return resultProvider, err
		}
		fbProviderCRD, err := ch.resolver.LookupProvider(ctx, fb.ProviderRef, namespace)
		if err != nil {
			continue
		}

		fbProviderInfo := providerResolutionInfo(fbProviderCRD)
		fbModel := strings.TrimSpace(fb.Model)
		if fbModel == "" {
			fbModel = fbProviderCRD.Spec.DefaultModel
		}
		if err := authorizeContextTokenProviderUse(c, ch.contextTokenAuthorization, "chatFallback", namespace, fbProviderInfo, fbModel); err != nil {
			return resultProvider, err
		}

		fbAPIKey, err := ch.resolver.ResolveAPIKey(ctx, fbProviderCRD)
		if err != nil {
			continue
		}

		fbConfig := llm.ProviderConfig{
			APIKey:       fbAPIKey,
			BaseURL:      fbProviderCRD.Spec.BaseURL,
			ProviderType: string(fbProviderCRD.Spec.Type),
		}
		if fbProviderCRD.Spec.Azure != nil {
			fbConfig.AzureAPIVersion = fbProviderCRD.Spec.Azure.APIVersion
		}

		fbProvider, err := llm.NewProvider(string(fbProviderCRD.Spec.Type), fbConfig)
		if err != nil {
			continue
		}

		fallbacks = append(fallbacks, llm.FallbackEntry{
			Provider: llm.NewRetryProvider(fbProvider, 0),
			Model:    fbModel,
		})
	}

	if len(fallbacks) > 0 {
		fp := llm.NewFallbackProvider(resultProvider, fallbacks)
		fp.SetCooldownTracker(ch.cooldownTracker)
		resultProvider = fp
	}

	return resultProvider, nil
}

// generateChatID returns 8 random hex characters.
func generateChatID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// hashArgs creates a short hash of tool name + arguments for repetition detection.
func hashArgs(name string, args json.RawMessage) string {
	h := sha256.New()
	h.Write([]byte(name))
	h.Write(args)
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// writeSSE writes a server-sent event to the writer.
func writeSSE(w *bufio.Writer, event, data string) error {
	_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	if err != nil {
		return err
	}
	return w.Flush()
}

// hasRunningTasks checks if any tasks created by this chat session are still running.
func (ch *ChatHandler) hasRunningTasks(ctx context.Context, namespace, sessionID string) bool {
	var taskList corev1alpha1.TaskList
	if err := ch.client.List(ctx, &taskList,
		client.InNamespace(namespace),
		client.MatchingLabels{labels.LabelChatSession: sessionID},
	); err != nil {
		return false
	}
	for i := range taskList.Items {
		switch taskList.Items[i].Status.Phase {
		case corev1alpha1.TaskPhaseSucceeded, corev1alpha1.TaskPhaseFailed, corev1alpha1.TaskPhaseCancelled:
			continue
		case corev1alpha1.TaskPhaseScheduled:
			// A recurring parent stays Scheduled by design and never completes.
			continue
		default:
			return true
		}
	}
	return false
}

// looksLikeIssueWorkflow checks if the user message contains patterns suggesting
// a GitHub issue implementation workflow (fix, implement, create PR).
// Uses word-boundary matching to avoid false positives from substrings.
func looksLikeIssueWorkflow(message string) bool {
	lower := strings.ToLower(message)
	// Must mention a GitHub issue
	hasIssue := strings.Contains(lower, "/issues/") ||
		strings.Contains(lower, "issue #") ||
		strings.Contains(lower, "issue#")
	if !hasIssue {
		return false
	}
	// Must suggest implementation work — use multi-word phrases or word boundaries
	// to avoid false positives (e.g., "pr" in "problem", "fix" in "prefix")
	actionPhrases := []string{"pick up", "work on", "pull request", "create a pr", "open a pr", "make a pr"}
	for _, phrase := range actionPhrases {
		if strings.Contains(lower, phrase) {
			return true
		}
	}
	// Single-word actions need word boundary checks
	actionWordsPattern := regexp.MustCompile(`\b(fix|implement|resolve|address)\b`)
	return actionWordsPattern.MatchString(lower)
}
