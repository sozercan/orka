/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/approvals"
	"github.com/orka-agents/orka/internal/executionmode"
	"github.com/orka-agents/orka/internal/llm"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tracing"
	"github.com/orka-agents/orka/internal/tracing/genai"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MemoryReader is the least-privilege store dependency required by recall_memory.
type MemoryReader interface {
	ListMemories(context.Context, store.MemoryFilter) ([]store.Memory, error)
}

// MemoryProposalWriter is the least-privilege store dependency required by
// remember and propose_memory.
type MemoryProposalWriter interface {
	CreateMemoryProposal(context.Context, *store.MemoryProposal) error
}

// TranscriptSearcher is the least-privilege store dependency required by
// search_transcript.
type TranscriptSearcher interface {
	SearchTranscript(context.Context, store.TranscriptSearchFilter) ([]store.TranscriptSearchResult, error)
}

// ToolContext provides dependencies for tools that need K8s client access or other services.
type ToolContext struct {
	Client client.Client
	// PolicyReader bypasses informer lag when coordination tools resolve Task,
	// Agent, Provider, Tool, and AgentRuntime policy. Writes continue through Client.
	PolicyReader              client.Reader
	KubeClient                kubernetes.Interface
	Namespace                 string
	SessionID                 string
	TaskID                    string
	TaskUID                   string
	ParentTaskID              string
	AgentName                 string
	ToolCallID                string
	OperationID               string
	Tenant                    string
	Provider                  string
	ProviderType              string
	ExecutionMode             executionmode.Mode
	WatchNamespace            string
	EnforceNamespaceIsolation bool
	// Brokered marks an authenticated controller-side MCP broker execution.
	// Broker-aware tools must fail closed on missing request-scoped dependencies
	// instead of falling back to controller process environment or credentials.
	Brokered bool
	// TaskProvenanceProtected reports that Task provenance admission reserves
	// controller-authenticated lineage metadata from direct namespace writes.
	TaskProvenanceProtected bool
	// ExternalEffects and OperationID let brokered coordination tools bind
	// created resources to the controller-owned effect receipt for this exact
	// tool call. The receipt remains authoritative when provenance admission is
	// disabled and Task metadata is mutable.
	ExternalEffects store.ExternalEffectStore
	// Least-privilege durable dependencies for brokered memory tools.
	MemoryReader         MemoryReader
	MemoryProposalWriter MemoryProposalWriter
	TranscriptSearcher   TranscriptSearcher
	// RepositoryValidationBindings stores the controller-owned command binding
	// created before a repository validation Task.
	RepositoryValidationBindings RepositoryValidationBindingStore
	// ResultStore for fetching task outputs (store.ResultStore)
	ResultStore interface {
		GetResult(ctx context.Context, namespace, taskName string) ([]byte, error)
	}
	// MessageStore for inter-agent messaging when tools execute in-process from the controller broker.
	MessageStore store.MessageStore
	// SessionDeleter for deleting sessions (controller.SessionManager)
	SessionDeleter interface {
		DeleteSession(ctx context.Context, namespace, sessionID string) error
	}
	// Task creation helpers provided by the chat executor
	GenerateTaskName               func() string
	TaskLabels                     func() map[string]string
	CheckTaskLimit                 func() *ChatToolError
	AuthorizeTaskCreate            func(context.Context, *corev1alpha1.Task) *ChatToolError
	AuthorizeTaskDelete            func(context.Context, *corev1alpha1.Task) *ChatToolError
	AuthorizeAgentCreate           func(context.Context, *corev1alpha1.Agent) *ChatToolError
	AuthorizeAgentUpdate           func(context.Context, *corev1alpha1.Agent) *ChatToolError
	AuthorizeAgentDelete           func(context.Context, *corev1alpha1.Agent) *ChatToolError
	AuthorizeSecretRead            func(context.Context, string, string) *ChatToolError
	RequireSecretReadAuthorization bool
	IncrementTasks                 func()
	ApprovalEmitter                func(context.Context, approvals.ApprovalTarget) error
	ApprovalTargetSpecDigest       func(context.Context, string) (string, error)
	ApprovalTargetArguments        func(context.Context, string, json.RawMessage) (json.RawMessage, error)
	ApprovalTargetRefresh          func(context.Context, string, *corev1alpha1.Tool) error
}

type toolContextKey struct{}

// WithToolContext adds a ToolContext to a context.
func WithToolContext(ctx context.Context, tc *ToolContext) context.Context {
	return context.WithValue(ctx, toolContextKey{}, tc)
}

// GetToolContext extracts a ToolContext from a context.
func GetToolContext(ctx context.Context) *ToolContext {
	tc, _ := ctx.Value(toolContextKey{}).(*ToolContext)
	return tc
}

// ChatToolError represents a structured error from a chat tool.
type ChatToolError struct {
	Type       string `json:"errorType"`
	Message    string `json:"error"`
	Suggestion string `json:"suggestion"`
}

func (e *ChatToolError) Error() string { return e.Message }

// ChatToolResult represents the result of a chat tool execution.
type ChatToolResult struct {
	Success    bool   `json:"success"`
	Data       any    `json:"data,omitempty"`
	Error      string `json:"error,omitempty"`
	ErrorType  string `json:"errorType,omitempty"`
	Suggestion string `json:"suggestion,omitempty"`
}

// marshalChatResult marshals a ChatToolResult to a JSON string.
func marshalChatResult(r ChatToolResult) (string, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("failed to marshal chat tool result: %w", err)
	}
	return string(b), nil
}

// ChatToolSuccess returns a successful ChatToolResult JSON string.
func ChatToolSuccess(data any) (string, error) {
	return marshalChatResult(ChatToolResult{Success: true, Data: data})
}

// ChatToolErrorResult returns a failed ChatToolResult JSON string.
func ChatToolErrorResult(errType, message, suggestion string) (string, error) {
	return marshalChatResult(ChatToolResult{
		Error:      message,
		ErrorType:  errType,
		Suggestion: suggestion,
	})
}

const githubAPIBaseURL = "https://api.github.com"

const defaultNamespace = "default"

const trueStr = "true"

const defaultMergeMethod = "squash"

const (
	unknownToolTelemetryName  = "unknown_tool"
	rejectedToolTelemetryName = "rejected_tool"
)

// Tool is the interface for built-in tools
type Tool interface {
	// Name returns the tool name
	Name() string

	// Description returns the tool description for the LLM
	Description() string

	// Parameters returns the JSON Schema for the tool parameters
	Parameters() json.RawMessage

	// Execute executes the tool with the given arguments
	Execute(ctx context.Context, args json.RawMessage) (string, error)
}

// Registry manages registered tools
type Registry struct {
	mu    sync.RWMutex
	tools map[string]Tool

	telemetryMu            sync.Mutex
	tracerProvider         any
	tracer                 trace.Tracer
	meterProvider          any
	toolDurationHistogram  metric.Float64Histogram
	toolDurationMetricOpts map[toolDurationMetricKey]metric.MeasurementOption
}

type toolDurationMetricKey struct {
	toolName string
	toolType string
}

// NewRegistry creates a new tool registry
func NewRegistry() *Registry {
	return &Registry{
		tools: make(map[string]Tool),
	}
}

// Register registers a tool
func (r *Registry) Register(tool Tool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[tool.Name()] = tool
}

// Get returns a tool by name
func (r *Registry) Get(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tool, ok := r.tools[name]
	return tool, ok
}

// Names returns all registered tool names in stable order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Execute executes a tool by name. It is the DRY instrumentation point for
// built-in registry tools used by chat, proxy-compatible handlers, and workers.
func (r *Registry) Execute(ctx context.Context, name string, args json.RawMessage) (string, error) {
	tool, ok := r.Get(name)
	if telemetryDisabled() {
		if !ok {
			return "", fmt.Errorf("tool %q not found", name)
		}
		return tool.Execute(ctx, args)
	}

	toolTelemetryName := name
	if !ok {
		toolTelemetryName = unknownToolTelemetryName
	}
	toolTypeValue := toolType(ctx, tool)
	toolKind := registryToolKind(name)
	meterActive := tracing.GlobalMeterProviderActive()
	var start time.Time
	if meterActive {
		start = time.Now()
	}

	tracer := r.genAITracer()
	spanName := genai.OperationExecuteTool + " " + toolTelemetryName
	deferStartAttributes := tracing.IsDefaultGlobalTracerProvider(tracing.GlobalTracerProvider())
	var span trace.Span
	if deferStartAttributes {
		ctx, span = tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindInternal))
	} else {
		attrs := registryToolSpanAttributes(ctx, toolTelemetryName, tool, ok, toolTypeValue, toolKind)
		ctx, span = tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attrs...))
	}
	spanRecording := span.IsRecording()
	if spanRecording && deferStartAttributes {
		span.SetAttributes(registryToolSpanAttributes(ctx, toolTelemetryName, tool, ok, toolTypeValue, toolKind)...)
	}
	if !spanRecording && !meterActive {
		if !ok {
			span.End()
			return "", fmt.Errorf("tool %q not found", name)
		}
		result, err := tool.Execute(ctx, args)
		span.End()
		return result, err
	}
	defer span.End()

	if !ok {
		err := fmt.Errorf("tool %q not found", name)
		if spanRecording {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(attribute.String(genai.AttrErrorType, "tool_not_found"))
		}
		if meterActive {
			r.recordToolDuration(ctx, time.Since(start).Seconds(), toolTelemetryName, toolTypeValue, "tool_not_found")
		}
		return "", err
	}

	result, err := tool.Execute(ctx, args)
	duration := time.Since(start).Seconds()
	if spanRecording {
		span.SetAttributes(attribute.Int(tracing.AttrToolResultSizeBytes, len(result)))
	}
	metricErrType := ""
	if err != nil {
		errType := fmt.Sprintf("%T", err)
		if spanRecording {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(attribute.String(genai.AttrErrorType, errType))
		}
		metricErrType = errType
	} else if spanRecording || meterActive {
		if failed, errType, message := failedToolResult(result); failed {
			if spanRecording {
				span.SetStatus(codes.Error, message)
				span.SetAttributes(attribute.String(genai.AttrErrorType, errType))
			}
			metricErrType = errType
		}
	}
	if meterActive {
		if metricErrType != "" {
			r.recordToolDuration(ctx, duration, toolTelemetryName, toolTypeValue, metricErrType)
		} else {
			r.recordSuccessfulToolDuration(ctx, duration, toolTelemetryName, toolTypeValue)
		}
	}
	return result, err
}

func registryToolSpanAttributes(ctx context.Context, toolTelemetryName string, tool Tool, ok bool, toolTypeValue, toolKind string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 10)
	attrs = append(attrs,
		attribute.String(genai.AttrOperationName, genai.OperationExecuteTool),
		attribute.String(genai.AttrToolName, toolTelemetryName),
		attribute.String(genai.AttrToolType, toolTypeValue),
		attribute.String(tracing.AttrToolName, toolTelemetryName),
		attribute.String(tracing.AttrToolKind, toolKind),
	)
	if tc := GetToolContext(ctx); tc != nil {
		tenant := tc.Tenant
		if tenant == "" {
			tenant = tc.Namespace
		}
		if tc.TaskID != "" {
			attrs = append(attrs, attribute.String(tracing.AttrTaskID, tc.TaskID))
		}
		if tc.Namespace != "" {
			attrs = append(attrs, attribute.String(tracing.AttrTaskNamespace, tc.Namespace))
		}
		if tenant != "" {
			attrs = append(attrs, attribute.String(tracing.AttrTenant, tenant))
		}
		if tc.ToolCallID != "" {
			attrs = append(attrs, attribute.String(genai.AttrToolCallID, tc.ToolCallID))
		}
	}
	if ok {
		if description := tool.Description(); description != "" {
			attrs = append(attrs, attribute.String(genai.AttrToolDescription, description))
		}
	}
	return attrs
}

func (r *Registry) genAITracer() trace.Tracer {
	provider := tracing.GlobalTracerProvider()
	r.telemetryMu.Lock()
	defer r.telemetryMu.Unlock()
	if r.tracer != nil && tracing.SameProvider(r.tracerProvider, provider) {
		return r.tracer
	}
	r.tracerProvider = provider
	r.tracer = provider.Tracer(genai.InstrumentationName, trace.WithSchemaURL(genai.SchemaURL))
	return r.tracer
}

func (r *Registry) getToolDurationHistogram() (metric.Float64Histogram, bool) {
	if !tracing.GlobalMeterProviderActive() {
		return nil, false
	}
	provider := tracing.GlobalMeterProvider()
	r.telemetryMu.Lock()
	defer r.telemetryMu.Unlock()
	if r.toolDurationHistogram != nil && tracing.SameProvider(r.meterProvider, provider) {
		return r.toolDurationHistogram, true
	}
	meter := provider.Meter(genai.InstrumentationName, metric.WithSchemaURL(genai.SchemaURL))
	histogram, err := meter.Float64Histogram(
		genai.MetricExecuteToolDuration,
		metric.WithUnit(genai.UnitSeconds),
		metric.WithExplicitBucketBoundaries(genai.ToolDurationBuckets...),
	)
	if err != nil {
		r.meterProvider = provider
		r.toolDurationHistogram = nil
		return nil, false
	}
	r.meterProvider = provider
	r.toolDurationHistogram = histogram
	return histogram, true
}

func (r *Registry) toolDurationMetricOption(toolName, toolType string) metric.MeasurementOption {
	key := toolDurationMetricKey{toolName: toolName, toolType: toolType}
	r.telemetryMu.Lock()
	defer r.telemetryMu.Unlock()
	if r.toolDurationMetricOpts == nil {
		r.toolDurationMetricOpts = make(map[toolDurationMetricKey]metric.MeasurementOption)
	}
	if opt, ok := r.toolDurationMetricOpts[key]; ok {
		return opt
	}
	attrs := attribute.NewSet(
		attribute.String(genai.AttrOperationName, genai.OperationExecuteTool),
		attribute.String(genai.AttrToolName, toolName),
		attribute.String(genai.AttrToolType, toolType),
	)
	opt := metric.WithAttributeSet(attrs)
	r.toolDurationMetricOpts[key] = opt
	return opt
}

func (r *Registry) recordSuccessfulToolDuration(ctx context.Context, seconds float64, toolName, toolType string) {
	histogram, ok := r.getToolDurationHistogram()
	if !ok {
		return
	}
	histogram.Record(ctx, seconds, r.toolDurationMetricOption(toolName, toolType))
}

func (r *Registry) recordToolDuration(ctx context.Context, seconds float64, toolName, toolType, errType string) {
	histogram, ok := r.getToolDurationHistogram()
	if !ok {
		return
	}
	histogram.Record(ctx, seconds, metric.WithAttributes(
		attribute.String(genai.AttrOperationName, genai.OperationExecuteTool),
		attribute.String(genai.AttrToolName, toolName),
		attribute.String(genai.AttrToolType, toolType),
		attribute.String(genai.AttrErrorType, errType),
	))
}

func telemetryDisabled() bool {
	return tracing.GlobalTracerProviderExplicitNoop() && !tracing.GlobalMeterProviderActive()
}

// RecordRejectedToolCall records a failed tool invocation that is rejected before
// dispatch, for example by request-level allowlists or invalid arguments. It
// intentionally does not execute the tool.
func RecordRejectedToolCall(ctx context.Context, name, toolCallID, errType, message string) {
	if strings.TrimSpace(name) == "" || telemetryDisabled() {
		return
	}
	start := time.Now()
	attrs := []attribute.KeyValue{
		attribute.String(genai.AttrOperationName, genai.OperationExecuteTool),
		attribute.String(genai.AttrToolName, rejectedToolTelemetryName),
		attribute.String(genai.AttrToolType, genai.ToolTypeFunction),
	}
	if toolCallID != "" {
		attrs = append(attrs, attribute.String(genai.AttrToolCallID, toolCallID))
	}
	if errType == "" {
		errType = "tool_rejected"
	}
	if message == "" {
		message = errType
	}
	tracer := tracing.GenAITracer(genai.InstrumentationName)
	ctx, span := tracer.Start(ctx, genai.OperationExecuteTool+" "+rejectedToolTelemetryName, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attrs...))
	span.SetStatus(codes.Error, message)
	span.SetAttributes(attribute.String(genai.AttrErrorType, errType))
	span.End()
	metricAttrs := []attribute.KeyValue{
		attribute.String(genai.AttrOperationName, genai.OperationExecuteTool),
		attribute.String(genai.AttrToolName, rejectedToolTelemetryName),
		attribute.String(genai.AttrToolType, genai.ToolTypeFunction),
		attribute.String(genai.AttrErrorType, errType),
	}
	recordToolDuration(ctx, time.Since(start).Seconds(), metricAttrs...)
}

// FailedToolResultForTelemetry detects structured tool failures for callers
// that reject tool calls before dispatch but still need telemetry.
func FailedToolResultForTelemetry(result string) (bool, string, string) {
	return failedToolResult(result)
}

func failedToolResult(result string) (bool, string, string) {
	// Most successful tool results are compact JSON with "success":true and no
	// false literal. Avoid the generic map unmarshal on that hot path; only parse
	// when a structured failure is possible.
	if !strings.Contains(result, "false") {
		return false, "", ""
	}
	var body map[string]json.RawMessage
	if json.Unmarshal([]byte(result), &body) != nil {
		return false, "", ""
	}
	successJSON, ok := body["success"]
	if !ok {
		return false, "", ""
	}
	var success bool
	if json.Unmarshal(successJSON, &success) != nil || success {
		return false, "", ""
	}

	var tr ChatToolResult
	_ = json.Unmarshal([]byte(result), &tr)
	errType := tr.ErrorType
	if errType == "" {
		errType = "tool_error"
	}
	message := tr.Error
	if message == "" {
		message = errType
	}
	return true, errType, message
}

func toolType(ctx context.Context, _ Tool) string {
	// Registry tools are in-process functions. External Tool CRD/MCP execution is
	// handled by worker.ToolExecutor and can be modeled as extension later.
	return genai.ToolTypeFunction
}

func registryToolKind(name string) string {
	if name == delegateTaskToolName {
		return tracing.ToolKindDelegate
	}
	return tracing.ToolKindBuiltin
}

func recordToolDuration(ctx context.Context, seconds float64, attrs ...attribute.KeyValue) {
	meter := tracing.GenAIMeter(genai.InstrumentationName)
	histogram, err := meter.Float64Histogram(
		genai.MetricExecuteToolDuration,
		metric.WithUnit(genai.UnitSeconds),
		metric.WithExplicitBucketBoundaries(genai.ToolDurationBuckets...),
	)
	if err != nil {
		return
	}
	histogram.Record(ctx, seconds, metric.WithAttributes(attrs...))
}

// ToLLMTools converts the registry to LLM tool definitions
func (r *Registry) ToLLMTools(names []string) []llm.Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tools := make([]llm.Tool, 0)
	for _, name := range names {
		if tool, ok := r.tools[name]; ok {
			tools = append(tools, llm.Tool{
				Name:        tool.Name(),
				Description: tool.Description(),
				Parameters:  tool.Parameters(),
			})
		}
	}
	return tools
}

// DefaultRegistry is the default tool registry with built-in tools
var DefaultRegistry = NewRegistry()

// RegisterBuiltinTools registers all built-in tools
func RegisterBuiltinTools() {
	DefaultRegistry.Register(NewWebSearchTool())
	DefaultRegistry.Register(NewCodeExecTool())
	DefaultRegistry.Register(NewFileReadTool())
	DefaultRegistry.Register(NewWebFetchTool())
	DefaultRegistry.Register(NewFileWriteTool())
	DefaultRegistry.Register(NewRequestApprovalTool())
}

// RegisterCoordinationTools registers coordination tools that require a K8s client
func RegisterCoordinationTools(k8sClient client.Client, mode executionmode.Mode) {
	DefaultRegistry.Register(NewDelegateTaskTool(k8sClient))
	DefaultRegistry.Register(NewWaitForTasksTool(k8sClient))
	DefaultRegistry.Register(NewCreateContainerTaskTool(k8sClient))
	DefaultRegistry.Register(NewCancelTaskTool(k8sClient))
	DefaultRegistry.Register(NewSendMessageTool())
	DefaultRegistry.Register(NewCheckMessagesTool())
	DefaultRegistry.Register(NewCreatePullRequestTool(k8sClient))
	DefaultRegistry.Register(NewCheckPullRequestCITool(k8sClient))
	DefaultRegistry.Register(NewMergePullRequestTool(k8sClient))
	DefaultRegistry.Register(NewAutoMergePullRequestTool(k8sClient))
	DefaultRegistry.Register(NewReviewPullRequestTool(k8sClient))
	DefaultRegistry.Register(NewPostReviewCommentTool(k8sClient))
	DefaultRegistry.Register(NewCheckPRReviewMarkerTool(k8sClient))
	DefaultRegistry.Register(NewListIssuesTool(k8sClient))
	DefaultRegistry.Register(NewListPullRequestsTool(k8sClient))
	DefaultRegistry.Register(NewGetIssueTool(k8sClient))
	DefaultRegistry.Register(NewCommentOnIssueTool(k8sClient))
	DefaultRegistry.Register(NewCreateAgentTool(k8sClient, mode))
	DefaultRegistry.Register(NewDeleteAgentTool(k8sClient))
	DefaultRegistry.Register(NewUpdatePlanTool())
	DefaultRegistry.Register(NewRecallMemoryTool())
	DefaultRegistry.Register(NewRememberMemoryTool())
	DefaultRegistry.Register(NewProposeMemoryTool())
	DefaultRegistry.Register(NewSearchTranscriptTool())
}

// RegisterBrokeredCoordinationTools registers only coordination tools whose
// implementations can execute safely inside the authenticated controller MCP
// broker. Registration is idempotent because Registry.Register replaces the
// implementation for a stable tool name.
//
// Do not broaden this list with worker-local tools or tools that obtain forge,
// repository, or ServiceAccount credentials from process environment. Those
// implementations are not request-scoped in the controller process.
func RegisterBrokeredCoordinationTools(r *Registry, k8sClient client.Client) error {
	if r == nil {
		return fmt.Errorf("brokered coordination tool registry is required")
	}
	if k8sClient == nil {
		return fmt.Errorf("brokered coordination tools require a Kubernetes client")
	}
	r.Register(NewDelegateTaskTool(k8sClient))
	r.Register(NewWaitForTasksTool(k8sClient))
	r.Register(NewRunValidationTool(k8sClient))
	r.Register(NewSendMessageTool())
	r.Register(NewCheckMessagesTool())
	r.Register(NewRecallMemoryTool())
	r.Register(NewRememberMemoryTool())
	r.Register(NewProposeMemoryTool())
	r.Register(NewSearchTranscriptTool())
	return nil
}

// RegisterBrokeredWebTools registers public web reads whose implementations
// are safe to execute inside the controller MCP broker. Registration is
// idempotent because Registry.Register replaces the implementation for a
// stable tool name.
func RegisterBrokeredWebTools(r *Registry) error {
	if r == nil {
		return fmt.Errorf("brokered web tool registry is required")
	}
	r.Register(NewBrokeredWebSearchTool())
	r.Register(NewBrokeredWebFetchTool())
	return nil
}

// RegisterChatTools registers the chat/management tools into the given registry.
func RegisterChatTools(r *Registry) {
	r.Register(&CreateAITaskTool{})
	r.Register(&CreatePRMonitorTool{})
	r.Register(&CreateContainerTaskTool{})
	r.Register(&CreateAgentTaskTool{})
	r.Register(&CheckTaskProgressTool{})
	r.Register(&FetchTaskOutputTool{})
	r.Register(&WaitForTaskTool{})
	r.Register(&ChatCancelTaskTool{})
	r.Register(&ListAgentsTool{})
	r.Register(&ListToolsTool{})
	r.Register(&ListTasksTool{})
	r.Register(&ChatCreateAgentTool{})
	r.Register(&UpdateAgentTool{})
	r.Register(&ChatDeleteAgentTool{})
	r.Register(&CreateToolCRDTool{})
	r.Register(&DeleteToolTool{})
	r.Register(&DeleteSessionTool{})
}

// RegisterChatToolsDefault registers chat tools into DefaultRegistry for use by the proxy.
func RegisterChatToolsDefault() {
	RegisterChatTools(DefaultRegistry)
}

// RegisterProxyPRTools registers the GitHub PR coordination tools that the
// Anthropic and OpenAI proxies advertise in coordinatorProxyTools but that
// RegisterChatTools does not provide. Without this the proxy lists the tools
// for the model, ToLLMTools silently drops them (they are missing from the
// registry), and the model gets back "tool not available in this request"
// when it tries to open the PR after all the real work is done.
//
// Callers must invoke this once after the controller manager's client is
// available. Tests that exercise injectOrkaTools should also call this so the
// advertised tool set matches the runtime registration set.
func RegisterProxyPRTools(k8sClient client.Client) {
	DefaultRegistry.Register(NewCreatePullRequestTool(k8sClient))
	DefaultRegistry.Register(NewCheckPullRequestCITool(k8sClient))
}

// KnownBuiltInToolNames returns every built-in tool name known to Orka, including
// tools registered in the default proxy registry and coordination tools that are
// registered in worker processes. Controller-side validation uses this to reject
// approvalRequiredTools entries that would be handled as built-ins rather than
// Tool CRDs.
func KnownBuiltInToolNames() []string {
	seen := map[string]bool{}
	for _, group := range [][]string{DefaultRegistry.Names(), ChatToolNames(), CoordinationToolNames()} {
		for _, name := range group {
			if name != "" {
				seen[name] = true
			}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ChatToolNames returns the names of all chat tools in registration order.
func ChatToolNames() []string {
	return []string{
		createAITaskToolName,
		createPRMonitorToolName,
		createContainerTaskToolName,
		createAgentTaskToolName,
		checkTaskProgressToolName, fetchTaskOutputToolName, waitForTaskToolName, cancelTaskToolName, listAgentsToolName, listToolsToolName, listTasksToolName, createAgentToolName, updateAgentToolName, "delete_agent",
		createToolCRDToolName,
		deleteToolToolName,
		deleteSessionToolName,
	}
}

// CoordinationToolNames returns the names of all coordination tools registered by
// RegisterCoordinationTools in worker processes.
func CoordinationToolNames() []string {
	return []string{
		delegateTaskToolName,
		waitForTasksToolName,
		createContainerTaskToolName,
		cancelTaskToolName,
		sendMessageToolName,
		checkMessagesToolName,
		createPullRequestToolName,
		checkPullRequestCIToolName,
		mergePullRequestToolName,
		autoMergePullRequestToolName,
		reviewPullRequestToolName,
		postReviewCommentToolName,
		checkPRReviewMarkerToolName,
		listIssuesToolName,
		listPullRequestsToolName,
		getIssueToolName,
		commentOnIssueToolName,
		createAgentToolName,
		deleteAgentToolName,
		updatePlanToolName,
		"recall_memory",
		"remember",
		"propose_memory",
		"search_transcript",
	}
}

func init() {
	RegisterBuiltinTools()
	RegisterChatToolsDefault()
}
