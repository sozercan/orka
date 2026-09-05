/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/controller"
	"github.com/orka-agents/orka/internal/executionmode"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/llm"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
)

const taskCreatedMsg = "Task created"

// ToolExecutor executes orchestrator LLM tool calls by creating and managing
// Kubernetes resources (Tasks, Agents, Tools, Sessions).
type ToolExecutor struct {
	client                    client.Client
	policyReader              client.Reader
	kubeClient                kubernetes.Interface
	sessionManager            *controller.SessionManager
	namespace                 string
	provider                  string
	providerType              string
	executionMode             executionmode.Mode
	sessionID                 string
	taskSeq                   atomic.Int32
	tasksCreated              int
	maxTasks                  int
	toolTimeout               time.Duration
	watchNamespace            string
	enforceNamespaceIsolation bool
	resultStore               store.ResultStore
	registry                  *tools.Registry
	allowedToolNames          map[string]struct{}
	authorizeTaskCreate       func(context.Context, *corev1alpha1.Task) error
	authorizeTaskDelete       func(context.Context, *corev1alpha1.Task) error
	authorizeAgentCreate      func(context.Context, *corev1alpha1.Agent) error
	authorizeAgentUpdate      func(context.Context, *corev1alpha1.Agent) error
	authorizeAgentDelete      func(context.Context, *corev1alpha1.Agent) error
	authorizeSecretRead       func(context.Context, string, string) error
}

// SetExecutionMode supplies the immutable installation mode used by trusted
// Agent-producing chat tools.
func (e *ToolExecutor) SetExecutionMode(mode executionmode.Mode) {
	e.executionMode = mode
}

// SetPolicyReader supplies the authoritative reader used by coordination tools.
func (e *ToolExecutor) SetPolicyReader(reader client.Reader) {
	e.policyReader = reader
}

// NewToolExecutor creates a new ToolExecutor.
func NewToolExecutor(c client.Client, sm *controller.SessionManager, namespace, sessionID, watchNamespace string, enforceNS bool, maxTasks int, toolTimeout time.Duration, rs store.ResultStore, kubeClientOpt ...kubernetes.Interface) *ToolExecutor {
	var kubeClient kubernetes.Interface
	if len(kubeClientOpt) > 0 {
		kubeClient = kubeClientOpt[0]
	}

	reg := tools.NewRegistry()
	tools.RegisterChatTools(reg)
	return &ToolExecutor{
		client:                    c,
		kubeClient:                kubeClient,
		sessionManager:            sm,
		namespace:                 namespace,
		sessionID:                 sessionID,
		maxTasks:                  maxTasks,
		toolTimeout:               toolTimeout,
		watchNamespace:            watchNamespace,
		enforceNamespaceIsolation: enforceNS,
		resultStore:               rs,
		registry:                  reg,
	}
}

// SetAllowedTools restricts execution to the tools exposed and authorized for
// the current request. When SetAllowedTools is not called, execution is
// unrestricted; an empty allowlist intentionally denies all tool calls.
func (e *ToolExecutor) SetAllowedTools(allowedTools []llm.Tool) {
	e.allowedToolNames = make(map[string]struct{}, len(allowedTools))
	for _, tool := range allowedTools {
		name := strings.TrimSpace(tool.Name)
		if name != "" {
			e.allowedToolNames[name] = struct{}{}
		}
	}
}

// SetTaskCreateAuthorizer installs an authorization hook for tools that create Tasks.
func (e *ToolExecutor) SetTaskCreateAuthorizer(authorize func(context.Context, *corev1alpha1.Task) error) {
	e.authorizeTaskCreate = authorize
}

// SetTaskDeleteAuthorizer installs an authorization hook for tools that delete Tasks.
func (e *ToolExecutor) SetTaskDeleteAuthorizer(authorize func(context.Context, *corev1alpha1.Task) error) {
	e.authorizeTaskDelete = authorize
}

// SetAgentCreateAuthorizer installs an authorization hook for tools that create Agents.
func (e *ToolExecutor) SetAgentCreateAuthorizer(authorize func(context.Context, *corev1alpha1.Agent) error) {
	e.authorizeAgentCreate = authorize
}

// SetAgentUpdateAuthorizer installs an authorization hook for tools that update Agents.
func (e *ToolExecutor) SetAgentUpdateAuthorizer(authorize func(context.Context, *corev1alpha1.Agent) error) {
	e.authorizeAgentUpdate = authorize
}

// SetAgentDeleteAuthorizer installs an authorization hook for tools that delete Agents.
func (e *ToolExecutor) SetAgentDeleteAuthorizer(authorize func(context.Context, *corev1alpha1.Agent) error) {
	e.authorizeAgentDelete = authorize
}

// SetSecretReadAuthorizer installs an authorization hook for tools that read Secrets.
func (e *ToolExecutor) SetSecretReadAuthorizer(authorize func(context.Context, string, string) error) {
	e.authorizeSecretRead = authorize
}

// ToolResult represents the result of a tool execution.
type ToolResult struct {
	Success    bool   `json:"success"`
	Data       any    `json:"data,omitempty"`
	Error      string `json:"error,omitempty"`
	ErrorType  string `json:"errorType,omitempty"`
	Suggestion string `json:"suggestion,omitempty"`
}

// Execute dispatches a tool call to the appropriate handler and returns
// the JSON-serialized result.
func (e *ToolExecutor) Execute(ctx context.Context, toolCall llm.ToolCall) (string, error) {
	if e.allowedToolNames != nil {
		if _, ok := e.allowedToolNames[toolCall.Name]; !ok {
			result := toolError("unauthorized_tool", fmt.Sprintf("tool %q is not authorized for this request", toolCall.Name), "Use one of the available tools")
			resultStr, err := marshalResult(result)
			recordRejectedToolCall(ctx, toolCall, resultStr)
			return resultStr, err
		}
	}

	var args map[string]any
	if err := json.Unmarshal(toolCall.Arguments, &args); err != nil {
		result := toolError("invalid_arguments", fmt.Sprintf("failed to parse arguments: %v", err), "Ensure arguments are valid JSON")
		resultStr, marshalErr := marshalResult(result)
		recordRejectedToolCall(ctx, toolCall, resultStr)
		return resultStr, marshalErr
	}

	toolCtx, cancel := context.WithTimeout(ctx, e.toolTimeout)
	defer cancel()

	// Set up ToolContext for registry-based tools
	tc := &tools.ToolContext{
		Client:                    e.client,
		PolicyReader:              e.policyReader,
		KubeClient:                e.kubeClient,
		Namespace:                 e.namespace,
		SessionID:                 e.sessionID,
		ToolCallID:                toolCall.ID,
		Tenant:                    e.namespace,
		Provider:                  e.provider,
		ProviderType:              e.providerType,
		ExecutionMode:             e.executionMode,
		WatchNamespace:            e.watchNamespace,
		EnforceNamespaceIsolation: e.enforceNamespaceIsolation,
		ResultStore:               e.resultStore,
		SessionDeleter:            e.sessionManager,
		GenerateTaskName:          e.generateTaskName,
		TaskLabels:                e.taskLabels,
		AuthorizeTaskCreate: func(ctx context.Context, task *corev1alpha1.Task) *tools.ChatToolError {
			return chatToolAuthorizationError(e.authorizeTaskCreate, ctx, task, "Use a task configuration authorized by the context token")
		},
		AuthorizeTaskDelete: func(ctx context.Context, task *corev1alpha1.Task) *tools.ChatToolError {
			return chatToolAuthorizationError(e.authorizeTaskDelete, ctx, task, "Use a task authorized by the context token")
		},
		AuthorizeAgentCreate: func(ctx context.Context, agent *corev1alpha1.Agent) *tools.ChatToolError {
			return chatToolAuthorizationError(e.authorizeAgentCreate, ctx, agent, "Use an agent configuration authorized by the context token")
		},
		AuthorizeAgentUpdate: func(ctx context.Context, agent *corev1alpha1.Agent) *tools.ChatToolError {
			return chatToolAuthorizationError(e.authorizeAgentUpdate, ctx, agent, "Use an agent update authorized by the context token")
		},
		AuthorizeAgentDelete: func(ctx context.Context, agent *corev1alpha1.Agent) *tools.ChatToolError {
			return chatToolAuthorizationError(e.authorizeAgentDelete, ctx, agent, "Use an agent authorized by the context token")
		},
		AuthorizeSecretRead: func(ctx context.Context, namespace, secretName string) *tools.ChatToolError {
			authorize := func(ctx context.Context, _ *corev1alpha1.Task) error {
				if e.authorizeSecretRead == nil {
					return nil
				}
				return e.authorizeSecretRead(ctx, namespace, secretName)
			}
			return chatToolAuthorizationError(authorize, ctx, nil, "Use a context token authorized to read the git credential secret")
		},
		RequireSecretReadAuthorization: e.authorizeSecretRead != nil,
		CheckTaskLimit: func() *tools.ChatToolError {
			if e.tasksCreated >= e.maxTasks {
				return &tools.ChatToolError{
					Type:       "limit_reached",
					Message:    fmt.Sprintf("task creation limit reached (max %d per turn)", e.maxTasks),
					Suggestion: "Wait for existing tasks to complete before creating new ones",
				}
			}
			return nil
		},
		IncrementTasks: func() { e.tasksCreated++ },
	}
	toolCtx = tools.WithToolContext(toolCtx, tc)

	// Marshal args to JSON for the Tool interface
	argsJSON, err := json.Marshal(args)
	if err != nil {
		result := toolError("internal_error", fmt.Sprintf("failed to marshal arguments: %v", err), "")
		return marshalResult(result)
	}

	// Execute via registry
	resultStr, err := e.registry.Execute(toolCtx, toolCall.Name, argsJSON)
	if err != nil {
		result := toolError("unknown_tool", fmt.Sprintf("unknown tool: %s", toolCall.Name), "Use one of the available tools")
		return marshalResult(result)
	}

	// Registry tools return JSON-marshaled ChatToolResult strings. Validate
	// that shape before passing the original tool result through unchanged.
	var tr ToolResult
	if jsonErr := json.Unmarshal([]byte(resultStr), &tr); jsonErr == nil {
		return resultStr, nil
	}

	// Fallback: wrap raw string as success
	result := ToolResult{Success: true, Data: resultStr}
	return marshalResult(result)
}

// ---- Task creation helpers ----

func (e *ToolExecutor) generateTaskName() string {
	seq := e.taskSeq.Add(1)
	prefix := sanitizeTaskNameComponent(e.sessionID)
	if prefix == "" {
		prefix = "session"
	}
	hash := sha256.Sum256([]byte(e.sessionID))
	hashSuffix := hex.EncodeToString(hash[:])[:8]
	seqSuffix := strconv.FormatInt(int64(seq), 10)

	// Kubernetes task names must fit DNS label rules (max 63 chars).
	const taskNameOverhead = len("chat-") + len("-") + len("-")
	maxPrefixLen := max(63-taskNameOverhead-len(hashSuffix)-len(seqSuffix), 1)
	if len(prefix) > maxPrefixLen {
		prefix = strings.Trim(prefix[:maxPrefixLen], "-")
		if prefix == "" {
			prefix = "session"
		}
	}

	return fmt.Sprintf("chat-%s-%s-%s", prefix, hashSuffix, seqSuffix)
}

func (e *ToolExecutor) taskLabels() map[string]string {
	return map[string]string{
		labels.LabelCreatedBy:   "orchestrator",
		labels.LabelChatSession: e.sessionID,
	}
}

func sanitizeTaskNameComponent(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	b.Grow(len(s))
	lastDash := false

	for _, r := range s {
		isLower := r >= 'a' && r <= 'z'
		isDigit := r >= '0' && r <= '9'
		if isLower || isDigit {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}

	return strings.Trim(b.String(), "-")
}

// ---- Error handling helpers ----

func toolError(errType, message, suggestion string) ToolResult {
	return ToolResult{
		Success:    false,
		Error:      message,
		ErrorType:  errType,
		Suggestion: suggestion,
	}
}

func marshalResult(result ToolResult) (string, error) {
	b, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("failed to marshal tool result: %w", err)
	}
	return string(b), nil
}

func recordRejectedToolCall(ctx context.Context, toolCall llm.ToolCall, result string) {
	if strings.TrimSpace(toolCall.Name) == "" || result == "" {
		return
	}
	failed, errType, message := tools.FailedToolResultForTelemetry(result)
	if !failed {
		return
	}
	tools.RecordRejectedToolCall(ctx, toolCall.Name, toolCall.ID, errType, message)
}
