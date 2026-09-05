package controller

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/outboundaccess"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
	workerexecutor "github.com/orka-agents/orka/internal/worker"
)

type ACPMCPAuthenticatedTask struct {
	Name         string
	Namespace    string
	UID          string
	ParentTaskID string
	AgentName    string
}

type acpMCPAuthenticatedTaskContextKey struct{}

func withACPMCPAuthenticatedTask(ctx context.Context, task ACPMCPAuthenticatedTask) context.Context {
	return context.WithValue(ctx, acpMCPAuthenticatedTaskContextKey{}, task)
}

func ACPMCPAuthenticatedTaskFromContext(ctx context.Context) (ACPMCPAuthenticatedTask, bool) {
	task, ok := ctx.Value(acpMCPAuthenticatedTaskContextKey{}).(ACPMCPAuthenticatedTask)
	return task, ok && task.Name != "" && task.Namespace != "" && task.UID != ""
}

type ACPMCPBrokerCredentials struct {
	ControllerBearerToken string
	CapabilitySecret      []byte
	ExpectedFence         harnessv2.Fence
	RuntimeProfile        harnessv2.RuntimeProfile
	// ExpectedMCPConfiguration freezes the exact prompt-scoped tool and
	// approval policy for external runtimes. RuntimePool callers leave it nil
	// because their controller-owned supervisor receives the same policy.
	ExpectedMCPConfiguration *harnessv2.MCPPolicyConfiguration
	ControllerFence          store.ControllerEpochFence
	Task                     ACPMCPAuthenticatedTask
}

type ACPMCPBrokerCredentialResolver interface {
	ResolveACPMCPBrokerCredentials(context.Context, harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error)
}

// ACPMCPBrokerPreAuthenticator resolves and verifies the controller bearer
// from the non-secret pool identity carried in request headers, so the broker
// can reject unauthenticated callers before consuming their request bodies.
type ACPMCPBrokerPreAuthenticator interface {
	PreAuthenticateACPMCPBroker(ctx context.Context, poolNamespace, poolUID, authorizationHeader string) error
}

type ACPMCPBrokerCredentialResolverFunc func(context.Context, harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error)

func (f ACPMCPBrokerCredentialResolverFunc) ResolveACPMCPBrokerCredentials(ctx context.Context, request harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error) {
	return f(ctx, request)
}

type ACPMCPPromptAuthorizer interface {
	AuthorizeACPMCPPrompt(context.Context, harnessv2.MCPBrokerCallRequest) error
}

type ACPMCPPromptAuthorizerFunc func(context.Context, harnessv2.MCPBrokerCallRequest) error

func (f ACPMCPPromptAuthorizerFunc) AuthorizeACPMCPPrompt(ctx context.Context, request harnessv2.MCPBrokerCallRequest) error {
	return f(ctx, request)
}

type ACPMCPToolExecutor interface {
	ExecuteACPMCPTool(context.Context, harnessv2.MCPBrokerCallRequest, harnessv2.MCPToolDescriptor) (json.RawMessage, error)
}

type ACPMCPToolExecutorFunc func(context.Context, harnessv2.MCPBrokerCallRequest, harnessv2.MCPToolDescriptor) (json.RawMessage, error)

func (f ACPMCPToolExecutorFunc) ExecuteACPMCPTool(ctx context.Context, request harnessv2.MCPBrokerCallRequest, descriptor harnessv2.MCPToolDescriptor) (json.RawMessage, error) {
	return f(ctx, request, descriptor)
}

type RegistryACPMCPToolExecutor struct {
	Registry            *tools.Registry
	Reader              client.Reader
	KubeClient          kubernetes.Interface
	HTTPClient          *http.Client
	OutboundAccess      outboundaccess.Resolver
	TransactionExchange *workerexecutor.TransactionExchangeConfig
	ContextFactory      func(context.Context, harnessv2.MCPBrokerCallRequest) (*tools.ToolContext, error)

	// EnforceTransactionCredentialAuth mirrors the controller-wide context-token
	// authorization enforcement mode. Custom-Tool executions always bind the
	// authenticated Task's transaction authority; this flag gates only its
	// Secret-credential authorization.
	EnforceTransactionCredentialAuth bool
	// TransactionCredentialReadScopes lists the transaction scopes that
	// authorize Secret-backed outbound credentials, matching the scopes the
	// controller stamps into worker Jobs.
	TransactionCredentialReadScopes []string
}

func (e RegistryACPMCPToolExecutor) ExecuteACPMCPTool(
	ctx context.Context,
	request harnessv2.MCPBrokerCallRequest,
	descriptor harnessv2.MCPToolDescriptor,
) (json.RawMessage, error) {
	var result string
	var err error
	switch descriptor.Source {
	case harnessv2.MCPToolSourceBrokeredBuiltin:
		registry := e.Registry
		if registry == nil {
			registry = tools.DefaultRegistry
		}
		if _, ok := registry.Get(descriptor.Name); !ok {
			return nil, fmt.Errorf("MCP tool %q is not registered", descriptor.Name)
		}
		if e.ContextFactory != nil {
			toolContext, contextErr := e.ContextFactory(ctx, request)
			if contextErr != nil {
				return nil, contextErr
			}
			if toolContext != nil {
				copy := *toolContext
				copy.Namespace = request.Namespace
				copy.TaskUID = string(request.Metadata.TaskUID)
				copy.ToolCallID = request.Call.CallID
				if copy.Tenant == "" {
					copy.Tenant = request.Namespace
				}
				ctx = tools.WithToolContext(ctx, &copy)
			}
		}
		result, err = registry.Execute(ctx, descriptor.Name, request.Call.Arguments)
	case harnessv2.MCPToolSourceBrokeredCustom:
		if e.Reader == nil || e.KubeClient == nil {
			return nil, fmt.Errorf("custom MCP tool executor is not configured")
		}
		tool := &corev1alpha1.Tool{}
		if getErr := e.Reader.Get(ctx, client.ObjectKey{Namespace: request.Namespace, Name: descriptor.Name}, tool); getErr != nil {
			return nil, fmt.Errorf("load custom MCP tool %q: %w", descriptor.Name, getErr)
		}
		currentDescriptor, descriptorErr := customACPMCPToolDescriptor(tool)
		if descriptorErr != nil {
			return nil, descriptorErr
		}
		expected, expectedErr := harnessv2.CanonicalValue(descriptor)
		current, currentErr := harnessv2.CanonicalValue(currentDescriptor)
		if expectedErr != nil || currentErr != nil || !bytes.Equal(expected, current) {
			return nil, fmt.Errorf("custom MCP tool %q changed after prompt authorization", descriptor.Name)
		}
		executor := workerexecutor.NewToolExecutorForNamespace(request.Namespace, e.KubeClient, e.HTTPClient, e.OutboundAccess)
		executor.SetTransactionExchangeConfig(e.TransactionExchange)
		if bindErr := e.bindTaskTransactionAuthority(ctx, request, executor); bindErr != nil {
			return nil, bindErr
		}
		execCtx := workerexecutor.WithToolCallID(ctx, request.Call.CallID)
		execCtx = workerexecutor.WithToolIdempotencyKey(execCtx, string(request.Metadata.OperationID))
		result, err = executor.Execute(execCtx, tool, request.Call.Arguments)
	default:
		return nil, fmt.Errorf("MCP tool %q is not broker-executable", descriptor.Name)
	}
	if err != nil {
		_, executionFailed := errors.AsType[workerexecutor.ToolExecutionError](err)
		// A read-only Tool may reach its own request deadline while the
		// enclosing prompt is still active. Preparation failures never carry
		// the attempt marker; consequential timeouts retain unknown outcomes.
		readTimedOut := descriptor.Effect == harnessv2.MCPToolEffectReadOnly && ctx.Err() == nil &&
			workerexecutor.ToolRequestWasAttempted(err) && errors.Is(err, context.DeadlineExceeded)
		if executionFailed || readTimedOut {
			// Keep upstream bodies out of the ACP process. The broker still
			// checks prompt cancellation and authority before committing this
			// result, including consequential-operation replay receipts.
			return json.RawMessage(`{"isError":true,"error":"MCP tool execution failed"}`), nil
		}
		return nil, err
	}
	raw := json.RawMessage(result)
	if _, err := harnessv2.CanonicalJSON(raw); err != nil {
		encoded, encodeErr := json.Marshal(result)
		if encodeErr != nil {
			return nil, encodeErr
		}
		raw = encoded
	}
	if len(raw) > harnessv2.MaxMCPResultBytes {
		return nil, fmt.Errorf("MCP tool result exceeds %d bytes", harnessv2.MaxMCPResultBytes)
	}
	return raw, nil
}

// bindTaskTransactionAuthority initializes the per-request custom-Tool
// executor with the authenticated Task's transaction and credential authority,
// mirroring the worker Job environment the controller stamps for per-Task
// execution (setTransactionCredentialAuthorizationEnv/addTransactionEnvVars
// plus the owner-referenced transaction-token Secret mount). Transaction
// tokens/scopes are bound in every mode; only Secret-credential authorization
// is gated by EnforceTransactionCredentialAuth. Missing Task authority always
// fails closed.
func (e RegistryACPMCPToolExecutor) bindTaskTransactionAuthority(
	ctx context.Context,
	request harnessv2.MCPBrokerCallRequest,
	executor *workerexecutor.ToolExecutor,
) error {
	authenticated, ok := ACPMCPAuthenticatedTaskFromContext(ctx)
	if !ok || authenticated.Namespace != request.Namespace || authenticated.UID != string(request.Metadata.TaskUID) {
		return errors.New("authenticated ACP MCP task authority is unavailable")
	}
	if e.Reader == nil {
		return errors.New("ACP MCP task authority requires a Kubernetes reader")
	}
	task := &corev1alpha1.Task{}
	if err := e.Reader.Get(ctx, client.ObjectKey{Namespace: authenticated.Namespace, Name: authenticated.Name}, task); err != nil {
		return fmt.Errorf("load authenticated ACP MCP task authority: %w", err)
	}
	if string(task.UID) != authenticated.UID {
		return errors.New("authenticated ACP MCP task identity changed")
	}
	return bindVerifiedTaskTransactionAuthority(
		ctx, e.Reader, task, e.TransactionCredentialReadScopes,
		e.EnforceTransactionCredentialAuth, executor,
	)
}

type ACPMCPBrokerDependencies struct {
	Reader                  client.Reader
	Epochs                  *ControllerEpochManager
	ControlStore            store.DurableControlStore
	AgentExecutionSnapshots store.AgentExecutionSnapshotStore
	KubeClient              kubernetes.Interface
	HTTPClient              *http.Client
	Registry                *tools.Registry
	OutboundAccess          outboundaccess.Resolver
	TransactionExchange     *workerexecutor.TransactionExchangeConfig
	ContextFactory          func(context.Context, harnessv2.MCPBrokerCallRequest) (*tools.ToolContext, error)

	// EnforceTransactionCredentialAuth and TransactionCredentialReadScopes bind
	// brokered custom-Tool executions to the authenticated Task's transaction
	// authority; see RegistryACPMCPToolExecutor.
	EnforceTransactionCredentialAuth bool
	TransactionCredentialReadScopes  []string
}

func NewProductionACPMCPBroker(dependencies ACPMCPBrokerDependencies) (*ACPMCPBroker, error) {
	if dependencies.Reader == nil || dependencies.Epochs == nil || dependencies.ControlStore == nil ||
		dependencies.AgentExecutionSnapshots == nil || dependencies.KubeClient == nil {
		return nil, fmt.Errorf("production ACP MCP broker dependencies are incomplete")
	}
	broker := &ACPMCPBroker{
		Credentials: KubernetesACPMCPBrokerCredentialResolver{
			Reader: dependencies.Reader, Epochs: dependencies.Epochs,
			AgentExecutionSnapshots: dependencies.AgentExecutionSnapshots,
		},
		Prompts: DurableACPMCPPromptAuthorizer{Attempts: dependencies.ControlStore},
		Executor: RegistryACPMCPToolExecutor{
			Registry: dependencies.Registry, Reader: dependencies.Reader, KubeClient: dependencies.KubeClient,
			HTTPClient: dependencies.HTTPClient, OutboundAccess: dependencies.OutboundAccess,
			TransactionExchange: dependencies.TransactionExchange, ContextFactory: dependencies.ContextFactory,
			EnforceTransactionCredentialAuth: dependencies.EnforceTransactionCredentialAuth,
			TransactionCredentialReadScopes: append(
				[]string(nil),
				dependencies.TransactionCredentialReadScopes...,
			),
		},
		Effects: dependencies.ControlStore,
	}
	if err := broker.Validate(); err != nil {
		return nil, err
	}
	return broker, nil
}

type ACPMCPBroker struct {
	Credentials  ACPMCPBrokerCredentialResolver
	Prompts      ACPMCPPromptAuthorizer
	Executor     ACPMCPToolExecutor
	Effects      store.ExternalEffectStore
	MaxBodyBytes int64
}

func (b *ACPMCPBroker) Validate() error {
	if b == nil || b.Credentials == nil || b.Prompts == nil || b.Executor == nil || b.Effects == nil {
		return fmt.Errorf("ACP MCP broker requires credentials, prompt authorization, tool executor, and external-effect store")
	}
	return nil
}

func (b *ACPMCPBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != harnessv2.MCPBrokerCallPath {
		http.NotFound(w, r)
		return
	}
	if err := b.Validate(); err != nil {
		writeACPMCPError(w, http.StatusServiceUnavailable, "MCP broker is unavailable")
		return
	}
	// Authenticate the controller bearer against the pool identity carried in
	// non-secret headers before reading the body, so an untrusted Pod cannot
	// occupy request handlers by drip-feeding chunked bodies without knowing a
	// bearer. Resolvers that do not support header pre-authentication (test
	// fakes) fall through to the post-decode bearer check below.
	if preAuth, ok := b.Credentials.(ACPMCPBrokerPreAuthenticator); ok {
		poolNamespace := r.Header.Get(harnessv2.MCPBrokerPoolNamespaceHeader)
		poolUID := r.Header.Get(harnessv2.MCPBrokerPoolUIDHeader)
		if err := preAuth.PreAuthenticateACPMCPBroker(r.Context(), poolNamespace, poolUID, r.Header.Get("Authorization")); err != nil {
			writeACPMCPError(w, http.StatusUnauthorized, "MCP broker authentication failed")
			return
		}
	}
	limit := b.MaxBodyBytes
	if limit <= 0 || limit > harnessv2.MaxCanonicalJSONBytes {
		limit = harnessv2.MaxCanonicalJSONBytes
	}
	body := http.MaxBytesReader(w, r.Body, limit)
	defer body.Close() //nolint:errcheck
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	var request harnessv2.MCPBrokerCallRequest
	if err := decoder.Decode(&request); err != nil {
		writeACPMCPError(w, http.StatusBadRequest, "invalid MCP broker request")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeACPMCPError(w, http.StatusBadRequest, "invalid MCP broker request")
		return
	}
	now := time.Now().UTC()
	descriptor, err := request.ValidateAt(now)
	if err != nil {
		writeACPMCPError(w, http.StatusBadRequest, "invalid MCP broker request")
		return
	}
	// The body must match the pre-authenticated pool identity so a caller
	// cannot pre-authenticate as one pool and act as another.
	if _, ok := b.Credentials.(ACPMCPBrokerPreAuthenticator); ok {
		if request.Namespace != r.Header.Get(harnessv2.MCPBrokerPoolNamespaceHeader) ||
			string(request.Metadata.Fence.RuntimePoolUID) != r.Header.Get(harnessv2.MCPBrokerPoolUIDHeader) {
			writeACPMCPError(w, http.StatusBadRequest, "MCP broker request does not match its authenticated pool identity")
			return
		}
	}
	credentials, err := b.Credentials.ResolveACPMCPBrokerCredentials(r.Context(), request)
	if err != nil || !constantTimeBearerMatch(r.Header.Get("Authorization"), credentials.ControllerBearerToken) {
		writeACPMCPError(w, http.StatusUnauthorized, "MCP broker authentication failed")
		return
	}
	if mismatch := harnessv2.CompareFence(credentials.ExpectedFence, request.Metadata.Fence, true); mismatch != harnessv2.FenceMatch {
		writeACPMCPError(w, http.StatusGone, "MCP broker fence is stale")
		return
	}
	if err := request.Authorization.ValidateProfile(credentials.RuntimeProfile); err != nil {
		writeACPMCPError(w, http.StatusGone, "MCP broker policy is stale")
		return
	}
	if credentials.ExpectedMCPConfiguration != nil &&
		!request.Authorization.Configuration().Matches(*credentials.ExpectedMCPConfiguration) {
		writeACPMCPError(w, http.StatusGone, "MCP broker policy is stale")
		return
	}
	// Verify with a fresh timestamp: credential resolution above performs
	// Kubernetes I/O, and a capability that expired while it ran must not be
	// accepted against the stale pre-resolution clock.
	if err := harnessv2.VerifyOperationCapability(
		credentials.CapabilitySecret, r.Header.Get(harnessv2.OperationCapabilityHeader), request.Metadata, true, time.Now().UTC(),
	); err != nil {
		writeACPMCPError(w, http.StatusForbidden, "MCP broker operation authorization failed")
		return
	}
	promptCtx := withACPMCPAuthenticatedTask(r.Context(), credentials.Task)
	if err := b.Prompts.AuthorizeACPMCPPrompt(promptCtx, request); err != nil {
		writeACPMCPError(w, http.StatusForbidden, "MCP prompt is not active")
		return
	}
	promptCtx, stopPrompt := b.watchPromptAuthority(promptCtx, request)
	defer stopPrompt()
	call := func(ctx context.Context) (json.RawMessage, error) {
		ctx = withACPMCPAuthenticatedTask(ctx, credentials.Task)
		result, executeErr := b.Executor.ExecuteACPMCPTool(ctx, request, descriptor)
		if executeErr != nil {
			return nil, executeErr
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := b.Prompts.AuthorizeACPMCPPrompt(ctx, request); err != nil {
			return nil, fmt.Errorf("MCP prompt authority was revoked during tool execution")
		}
		return result, nil
	}
	var result json.RawMessage
	replayed := false
	var effectIdentity *store.ExternalEffectIdentity
	if descriptor.Effect == harnessv2.MCPToolEffectConsequential {
		identity := store.ExternalEffectIdentity{
			Kind: "acp-mcp-tool", Namespace: request.Namespace,
			AggregateID: string(request.Authorization.RuntimeSessionUID),
			OperationID: string(request.Metadata.OperationID),
		}
		effectIdentity = &identity
		result, replayed, err = runExternalEffectWithReplay(
			promptCtx, b.Effects, credentials.ControllerFence, identity,
			map[string]any{"call": request.Call, "descriptor": descriptor}, call,
		)
	} else {
		result, err = call(promptCtx)
	}
	if err != nil {
		if effectIdentity != nil && !errors.Is(err, store.ErrConflict) {
			settleCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Second)
			_ = settleExternalEffectStore(settleCtx, b.Effects, credentials.ControllerFence, *effectIdentity, store.ExternalEffectOutcomeUnknown, nil)
			cancel()
		}
		writeACPMCPError(w, http.StatusBadGateway, "MCP tool execution failed")
		return
	}
	response := harnessv2.MCPBrokerCallResponse{
		Protocol: harnessv2.ProtocolVersion, CallID: request.Call.CallID, Result: result,
		IsError: mcpToolResultIsError(result), Replayed: replayed,
	}
	if err := response.Validate(); err != nil {
		writeACPMCPError(w, http.StatusBadGateway, "MCP tool returned an invalid result")
		return
	}
	writeACPMCPJSON(w, http.StatusOK, response)
}

// watchPromptAuthority stops in-flight tools when their durable prompt settles
// or disappears. Fiber's HTTP adapter cancels its request context only when the
// server shuts down, so a supervisor disconnect alone cannot stop downstream
// work. Check the current attempt rather than the request's original lease
// expiry, which a running prompt may legitimately renew during a long tool call.
func (b *ACPMCPBroker) watchPromptAuthority(ctx context.Context, request harnessv2.MCPBrokerCallRequest) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				checkCtx, stopCheck := context.WithTimeout(ctx, 5*time.Second)
				err := b.Prompts.AuthorizeACPMCPPrompt(checkCtx, request)
				stopCheck()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	return ctx, func() {
		cancel()
		// The adapter reuses its request context after the handler returns.
		// Join the authority check before releasing that context.
		<-done
	}
}

func mcpToolResultIsError(result json.RawMessage) bool {
	var envelope struct {
		Success *bool `json:"success"`
		IsError bool  `json:"isError"`
	}
	if json.Unmarshal(result, &envelope) != nil {
		return false
	}
	return envelope.IsError || (envelope.Success != nil && !*envelope.Success)
}

func constantTimeBearerMatch(header, expected string) bool {
	value := strings.TrimSpace(header)
	if len(value) >= len("Bearer ") && strings.EqualFold(value[:len("Bearer ")], "Bearer ") {
		value = strings.TrimSpace(value[len("Bearer "):])
	}
	expected = strings.TrimSpace(expected)
	return len(value) == len(expected) && len(expected) >= 32 && subtle.ConstantTimeCompare([]byte(value), []byte(expected)) == 1
}

type DurableACPMCPPromptAuthorizer struct {
	Attempts store.PromptAttemptStore
}

func (a DurableACPMCPPromptAuthorizer) AuthorizeACPMCPPrompt(ctx context.Context, request harnessv2.MCPBrokerCallRequest) error {
	if a.Attempts == nil {
		return fmt.Errorf("prompt-attempt store is required")
	}
	key := store.PromptAttemptKey{
		Namespace: request.Namespace, TaskUID: string(request.Metadata.TaskUID),
		Attempt: int64(request.Metadata.TaskAttempt), PromptID: string(request.Metadata.PromptID),
	}
	id, err := key.CanonicalID()
	if err != nil {
		return err
	}
	attempt, err := a.Attempts.GetPromptAttempt(ctx, id)
	if err != nil {
		return err
	}
	if attempt.ExecutionState == store.PromptExecutionSubmitting {
		descriptor, ok := request.Authorization.ToolPolicy.Descriptor(request.Call.ToolName)
		if !ok || descriptor.Effect != harnessv2.MCPToolEffectReadOnly {
			return fmt.Errorf("consequential MCP calls require an accepted prompt attempt")
		}
	} else if attempt.ExecutionState != store.PromptExecutionAccepted && attempt.ExecutionState != store.PromptExecutionRunning {
		return fmt.Errorf("prompt attempt is in state %s", attempt.ExecutionState)
	}
	if attempt.SessionUID != string(request.Authorization.RuntimeSessionUID) ||
		attempt.RuntimeInstanceID != string(request.Metadata.Fence.RuntimeInstanceID) ||
		attempt.ControllerEpoch != int64(request.Metadata.Fence.ControllerEpoch) {
		return fmt.Errorf("prompt attempt identity does not match MCP authorization")
	}
	if request.Authorization.ApprovalPolicy.Requires(request.Call.ToolName) {
		return fmt.Errorf("approval-required ACP MCP calls are unavailable until controller-owned permission review is implemented")
	}
	return nil
}

type KubernetesACPMCPBrokerCredentialResolver struct {
	Reader                  client.Reader
	Epochs                  *ControllerEpochManager
	AgentExecutionSnapshots store.AgentExecutionSnapshotStore
}

var errACPMCPRuntimePoolNotFound = errors.New("runtime pool active instance was not found")

// PreAuthenticateACPMCPBroker resolves the runtime's controller bearer from
// the non-secret namespace + pool UID headers and constant-time compares it to
// the presented Authorization header, before any request body is read.
func (r KubernetesACPMCPBrokerCredentialResolver) PreAuthenticateACPMCPBroker(
	ctx context.Context,
	poolNamespace, poolUID, authorizationHeader string,
) error {
	if r.Reader == nil {
		return fmt.Errorf("kubernetes MCP credential resolver is not configured")
	}
	if strings.TrimSpace(poolNamespace) == "" || strings.TrimSpace(poolUID) == "" {
		return fmt.Errorf("MCP broker pool identity headers are required")
	}
	identity := harnessv2.RuntimePoolUID(poolUID)
	pool, err := findACPMCPRuntimePoolIdentity(ctx, r.Reader, poolNamespace, identity)
	if err != nil {
		return err
	}
	runtime, err := findACPMCPExternalRuntimeIdentity(ctx, r.Reader, poolNamespace, identity)
	if err != nil {
		return err
	}
	if (pool == nil) == (runtime == nil) {
		return fmt.Errorf("MCP runtime provider identity is missing or ambiguous")
	}
	if pool != nil {
		if pool.Status.ActiveInstance == nil {
			return fmt.Errorf("runtime pool has no active instance")
		}
		bearer, _, authErr := runtimePoolACPMCPAuthMaterial(ctx, r.Reader, pool)
		if authErr != nil {
			return authErr
		}
		if !constantTimeBearerMatch(authorizationHeader, bearer) {
			return fmt.Errorf("MCP broker bearer does not match the pool")
		}
		return nil
	}
	if r.Epochs == nil {
		return fmt.Errorf("kubernetes MCP credential resolver is not configured")
	}
	controllerFence, err := r.Epochs.CurrentFence(ctx)
	if err != nil {
		return err
	}
	bearer, err := externalRuntimeACPMCPPreAuthBearer(ctx, r.Reader, runtime, controllerFence, identity)
	if err != nil {
		return err
	}
	if !constantTimeBearerMatch(authorizationHeader, bearer) {
		return fmt.Errorf("MCP broker bearer does not match the runtime identity")
	}
	return nil
}

func externalRuntimeACPMCPPreAuthBearer(
	ctx context.Context,
	reader client.Reader,
	runtime *corev1alpha1.AgentRuntime,
	controllerFence store.ControllerEpochFence,
	poolUID harnessv2.RuntimePoolUID,
) (string, error) {
	if runtime == nil {
		return "", fmt.Errorf("external AgentRuntime identity is required")
	}
	observed := runtime.Status.ObservedCapabilities
	if runtime.Status.ObservedGeneration != runtime.Generation ||
		runtime.RegisteredContractVersion() != corev1alpha1.AgentRuntimeContractHarnessV2 || runtime.Spec.Capabilities == nil ||
		runtime.Spec.Capabilities.WorkspaceGovernance == nil || runtime.Spec.Capabilities.Profile == nil ||
		!runtime.Spec.Capabilities.WorkspaceGovernance.Strict() || observed == nil {
		return "", fmt.Errorf("external AgentRuntime authority is incomplete for MCP preauthentication")
	}
	if observed.ControllerEpoch != controllerFence.Epoch || observed.RuntimePoolUID != string(poolUID) ||
		observed.RuntimeProfileDigest != runtime.Spec.Capabilities.Profile.Digest ||
		observed.ProtocolVersion != string(corev1alpha1.RuntimePoolProtocolHarnessV2) {
		return "", fmt.Errorf("external AgentRuntime preauthentication identity is stale")
	}
	if runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef == nil {
		return "", fmt.Errorf("external AgentRuntime controller bearer reference is required")
	}
	reference := *runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef
	secret, err := readAgentRuntimeMCPSecret(ctx, reader, runtime.Namespace, reference)
	if err != nil {
		return "", err
	}
	if secret.ResourceVersion != runtime.Status.ObservedControllerAuthRefResourceVersion {
		return "", fmt.Errorf("external AgentRuntime bearer changed after conformance")
	}
	bearer := strings.TrimSpace(string(secret.Data[reference.Key]))
	if len(bearer) < 32 {
		return "", fmt.Errorf("external AgentRuntime bearer is incomplete")
	}
	return bearer, nil
}

func (r KubernetesACPMCPBrokerCredentialResolver) ResolveACPMCPBrokerCredentials(
	ctx context.Context,
	request harnessv2.MCPBrokerCallRequest,
) (ACPMCPBrokerCredentials, error) {
	if r.Reader == nil || r.Epochs == nil {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("kubernetes MCP credential resolver is not configured")
	}
	controllerFence, err := r.Epochs.CurrentFence(ctx)
	if err != nil {
		return ACPMCPBrokerCredentials{}, err
	}
	if uint64(controllerFence.Epoch) != request.Metadata.Fence.ControllerEpoch {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("MCP request uses a stale controller epoch")
	}
	task, execution, err := findACPMCPTaskExecution(ctx, r.Reader, request, controllerFence)
	if err != nil {
		return ACPMCPBrokerCredentials{}, err
	}
	var credentials ACPMCPBrokerCredentials
	switch {
	case strings.TrimSpace(execution.RuntimePoolName) != "":
		credentials, err = r.resolveRuntimePoolCredentials(ctx, request, execution, controllerFence)
	case strings.TrimSpace(execution.AgentRuntimeName) != "":
		credentials, err = r.resolveExternalRuntimeCredentials(ctx, request, task, execution, controllerFence)
	default:
		return ACPMCPBrokerCredentials{}, fmt.Errorf("active MCP task has no runtime target")
	}
	if err != nil {
		return ACPMCPBrokerCredentials{}, err
	}
	credentials.Task = ACPMCPAuthenticatedTask{
		Name: task.Name, Namespace: task.Namespace, UID: string(task.UID),
		ParentTaskID: labels.ParentTaskName(task.Labels, task.Annotations),
	}
	if task.Spec.AgentRef != nil {
		credentials.Task.AgentName = strings.TrimSpace(task.Spec.AgentRef.Name)
	}
	return credentials, nil
}

func (r KubernetesACPMCPBrokerCredentialResolver) resolveRuntimePoolCredentials(
	ctx context.Context,
	request harnessv2.MCPBrokerCallRequest,
	execution *corev1alpha1.TaskExecutionStatus,
	controllerFence store.ControllerEpochFence,
) (ACPMCPBrokerCredentials, error) {
	pool, err := findACPMCPRuntimePool(ctx, r.Reader, request.Namespace, request.Metadata.Fence.RuntimePoolUID)
	if err != nil {
		return ACPMCPBrokerCredentials{}, err
	}
	if execution.RuntimePoolUID != string(pool.UID) || execution.RuntimePoolName != pool.Name {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("active MCP task is bound to a different runtime pool")
	}
	active := pool.Status.ActiveInstance
	if active == nil {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("runtime pool has no active instance")
	}
	if active.ControllerEpoch != controllerFence.Epoch || active.ProfileDigest != pool.Spec.Runtime.Profile.Digest ||
		active.ProtocolVersion != corev1alpha1.RuntimePoolProtocolHarnessV2 {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("runtime pool active instance is stale")
	}
	expected := expectedACPMCPFence(pool, execution, controllerFence)
	if mismatch := harnessv2.CompareFence(expected, request.Metadata.Fence, true); mismatch != harnessv2.FenceMatch {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("runtime pool active instance does not match MCP request")
	}
	bearer, capability, err := runtimePoolACPMCPAuthMaterial(ctx, r.Reader, pool)
	if err != nil {
		return ACPMCPBrokerCredentials{}, err
	}
	return ACPMCPBrokerCredentials{
		ControllerBearerToken: bearer, CapabilitySecret: capability, ExpectedFence: expected,
		RuntimeProfile: runtimeProfileFromPool(pool.Spec.Runtime.Profile), ControllerFence: controllerFence,
	}, nil
}

func (r KubernetesACPMCPBrokerCredentialResolver) resolveExternalRuntimeCredentials(
	ctx context.Context,
	request harnessv2.MCPBrokerCallRequest,
	task *corev1alpha1.Task,
	execution *corev1alpha1.TaskExecutionStatus,
	controllerFence store.ControllerEpochFence,
) (ACPMCPBrokerCredentials, error) {
	verified, err := r.loadVerifiedExternalExecution(ctx, task)
	if err != nil {
		return ACPMCPBrokerCredentials{}, err
	}
	runtime := verified.externalRuntime
	if runtime == nil || runtime.Name != execution.AgentRuntimeName || string(runtime.UID) != execution.AgentRuntimeUID {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("active MCP task is bound to a different external AgentRuntime")
	}
	if verified.body.ExternalRuntime == nil {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("frozen external AgentRuntime authority is unavailable")
	}
	frozen := verified.body.ExternalRuntime
	observed := runtime.Status.ObservedCapabilities
	if observed == nil {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("external AgentRuntime observation is unavailable")
	}
	if strings.TrimSpace(execution.RuntimeSessionSupervisorBootID) == "" ||
		execution.RuntimeSessionSupervisorBootID != observed.SupervisorBootID {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("external AgentRuntime supervisor boot does not match the active MCP Task")
	}
	profile := verified.plan.Profile
	profileDigest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil || profileDigest != verified.plan.Digest {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("frozen external AgentRuntime profile digest is invalid")
	}
	expected := harnessv2.Fence{
		RuntimeInstanceID: harnessv2.RuntimeInstanceID(observed.RuntimeInstanceID),
		SupervisorBootID:  harnessv2.SupervisorBootID(observed.SupervisorBootID),
		ControllerEpoch:   uint64(controllerFence.Epoch), RuntimePoolUID: harnessv2.RuntimePoolUID(observed.RuntimePoolUID),
		RuntimePoolGeneration:      uint64(observed.RuntimePoolGeneration),
		RuntimeSessionUID:          harnessv2.RuntimeSessionUID(execution.RuntimeSessionUID),
		RuntimeSessionGeneration:   uint64(execution.RuntimeSessionGeneration),
		RuntimeProfileDigest:       verified.plan.Digest,
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	if mismatch := harnessv2.CompareFence(expected, request.Metadata.Fence, true); mismatch != harnessv2.FenceMatch {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("external AgentRuntime fence does not match MCP request")
	}
	if runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef == nil || runtime.Spec.ClientAuth.OperationCapabilitySecretRef == nil {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("external AgentRuntime v2 client auth references are required")
	}
	controllerRef := *runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef
	capabilityRef := *runtime.Spec.ClientAuth.OperationCapabilitySecretRef
	bearerSecret, err := readAgentRuntimeMCPSecret(ctx, r.Reader, runtime.Namespace, controllerRef)
	if err != nil {
		return ACPMCPBrokerCredentials{}, err
	}
	capabilitySecret, err := readAgentRuntimeMCPSecret(ctx, r.Reader, runtime.Namespace, capabilityRef)
	if err != nil {
		return ACPMCPBrokerCredentials{}, err
	}
	if !frozenACPMCPSecretMatches(frozen.ControllerAuth, "controller-auth", runtime.Namespace, controllerRef, bearerSecret) ||
		!frozenACPMCPSecretMatches(frozen.OperationCapability, "operation-capability", runtime.Namespace, capabilityRef, capabilitySecret) {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("external AgentRuntime authentication authority changed after Task binding")
	}
	bearer := strings.TrimSpace(string(bearerSecret.Data[controllerRef.Key]))
	capability := append([]byte(nil), capabilitySecret.Data[capabilityRef.Key]...)
	if len(bearer) < 32 || len(capability) < harnessv2.MinCapabilitySecretBytes {
		return ACPMCPBrokerCredentials{}, fmt.Errorf("external AgentRuntime auth material is incomplete")
	}
	expectedMCPConfiguration := verified.mcpConfiguration
	return ACPMCPBrokerCredentials{
		ControllerBearerToken: bearer, CapabilitySecret: capability, ExpectedFence: expected,
		RuntimeProfile: profile, ExpectedMCPConfiguration: &expectedMCPConfiguration,
		ControllerFence: controllerFence,
	}, nil
}

func (r KubernetesACPMCPBrokerCredentialResolver) loadVerifiedExternalExecution(
	ctx context.Context,
	task *corev1alpha1.Task,
) (*verifiedAgentExecution, error) {
	if r.AgentExecutionSnapshots == nil {
		return nil, fmt.Errorf("encrypted execution snapshot store is required for external MCP authorization")
	}
	if task == nil || task.Status.AgentExecutionBinding == nil {
		return nil, fmt.Errorf("external MCP Task has no immutable execution binding")
	}
	verifier := &TaskReconciler{
		APIReader:               r.Reader,
		ControllerEpochManager:  r.Epochs,
		AgentExecutionSnapshots: r.AgentExecutionSnapshots,
	}
	verified, err := verifier.loadVerifiedBoundExecutionForActiveSession(ctx, task, task.Status.AgentExecutionBinding)
	if err != nil {
		return nil, fmt.Errorf("verify external MCP execution binding: %w", err)
	}
	if verified.binding.Backend != corev1alpha1.AgentExecutionBackendExternalEndpoint || verified.externalRuntime == nil {
		return nil, fmt.Errorf("external MCP Task binding does not select an external AgentRuntime")
	}
	return verified, nil
}

func frozenACPMCPSecretMatches(
	frozen agentExecutionSnapshotSecretRef,
	role string,
	namespace string,
	reference corev1alpha1.AgentRuntimeSecretKeyReference,
	secret *corev1.Secret,
) bool {
	return secret != nil && frozen.Role == role && frozen.Namespace == namespace && frozen.Name == reference.Name &&
		frozen.UID == string(secret.UID) && frozen.ResourceVersion == secret.ResourceVersion &&
		len(frozen.Keys) == 1 && frozen.Keys[0] == reference.Key
}

func readAgentRuntimeMCPSecret(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	reference corev1alpha1.AgentRuntimeSecretKeyReference,
) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: reference.Name}, secret); err != nil {
		return nil, err
	}
	if strings.TrimSpace(reference.Key) == "" {
		return nil, fmt.Errorf("external AgentRuntime auth key is missing")
	}
	return secret, nil
}

func findACPMCPRuntimePool(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	poolUID harnessv2.RuntimePoolUID,
) (*corev1alpha1.RuntimePool, error) {
	pool, err := findACPMCPRuntimePoolIdentity(ctx, reader, namespace, poolUID)
	if err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, errACPMCPRuntimePoolNotFound
	}
	if pool.Status.ActiveInstance == nil {
		return nil, fmt.Errorf("runtime pool has no active instance")
	}
	return pool, nil
}

func findACPMCPRuntimePoolIdentity(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	poolUID harnessv2.RuntimePoolUID,
) (*corev1alpha1.RuntimePool, error) {
	var pools corev1alpha1.RuntimePoolList
	if err := reader.List(ctx, &pools, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var pool *corev1alpha1.RuntimePool
	for index := range pools.Items {
		candidate := &pools.Items[index]
		if string(candidate.UID) != string(poolUID) {
			continue
		}
		if pool != nil {
			return nil, fmt.Errorf("runtime pool UID is ambiguous")
		}
		pool = candidate.DeepCopy()
	}
	return pool, nil
}

func findACPMCPExternalRuntimeIdentity(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	poolUID harnessv2.RuntimePoolUID,
) (*corev1alpha1.AgentRuntime, error) {
	var runtimes corev1alpha1.AgentRuntimeList
	if err := reader.List(ctx, &runtimes, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var runtime *corev1alpha1.AgentRuntime
	for index := range runtimes.Items {
		candidate := &runtimes.Items[index]
		observed := candidate.Status.ObservedCapabilities
		if observed == nil || observed.RuntimePoolUID != string(poolUID) {
			continue
		}
		if runtime != nil {
			return nil, fmt.Errorf("external AgentRuntime pool UID is ambiguous")
		}
		runtime = candidate.DeepCopy()
	}
	return runtime, nil
}

func findACPMCPTaskExecution(
	ctx context.Context,
	reader client.Reader,
	request harnessv2.MCPBrokerCallRequest,
	controllerFence store.ControllerEpochFence,
) (*corev1alpha1.Task, *corev1alpha1.TaskExecutionStatus, error) {
	var tasks corev1alpha1.TaskList
	if err := reader.List(ctx, &tasks, client.InNamespace(request.Namespace)); err != nil {
		return nil, nil, err
	}
	var task *corev1alpha1.Task
	var execution *corev1alpha1.TaskExecutionStatus
	for index := range tasks.Items {
		candidate := &tasks.Items[index]
		if string(candidate.UID) != string(request.Metadata.TaskUID) {
			continue
		}
		if task != nil {
			return nil, nil, fmt.Errorf("task UID is ambiguous")
		}
		task = candidate.DeepCopy()
		if candidate.Status.Execution != nil {
			copy := *candidate.Status.Execution
			execution = &copy
		}
	}
	if execution == nil {
		return nil, nil, fmt.Errorf("active MCP task was not found")
	}
	if execution.State != corev1alpha1.TaskExecutionStateSubmitting &&
		execution.State != corev1alpha1.TaskExecutionStateAccepted && execution.State != corev1alpha1.TaskExecutionStateRunning {
		return nil, nil, fmt.Errorf("active MCP task is not running")
	}
	if execution.Attempt != int32(request.Metadata.TaskAttempt) || execution.PromptID != string(request.Metadata.PromptID) ||
		execution.RuntimeSessionUID != string(request.Authorization.RuntimeSessionUID) ||
		execution.RuntimeSessionGeneration != int64(request.Authorization.SessionGeneration) ||
		execution.RuntimeInstanceID != string(request.Metadata.Fence.RuntimeInstanceID) ||
		execution.ControllerEpoch != controllerFence.Epoch {
		return nil, nil, fmt.Errorf("active MCP task identity does not match request")
	}
	return task, execution, nil
}

func expectedACPMCPFence(
	pool *corev1alpha1.RuntimePool,
	execution *corev1alpha1.TaskExecutionStatus,
	controllerFence store.ControllerEpochFence,
) harnessv2.Fence {
	active := pool.Status.ActiveInstance
	return harnessv2.Fence{
		RuntimeInstanceID: harnessv2.RuntimeInstanceID(active.RuntimeInstanceID),
		SupervisorBootID:  harnessv2.SupervisorBootID(active.BootID),
		ControllerEpoch:   uint64(controllerFence.Epoch), RuntimePoolUID: harnessv2.RuntimePoolUID(pool.UID),
		RuntimePoolGeneration:      uint64(pool.Generation),
		RuntimeSessionUID:          harnessv2.RuntimeSessionUID(execution.RuntimeSessionUID),
		RuntimeSessionGeneration:   uint64(execution.RuntimeSessionGeneration),
		RuntimeProfileDigest:       harnessv2.ProfileDigest(pool.Spec.Runtime.Profile.Digest),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
}

func runtimePoolACPMCPAuthMaterial(
	ctx context.Context,
	reader client.Reader,
	pool *corev1alpha1.RuntimePool,
) (string, []byte, error) {
	active := pool.Status.ActiveInstance
	namespace := pool.Spec.RuntimeNamespace
	if namespace == "" {
		namespace = active.PodNamespace
	}
	secret, err := resolveRuntimePoolAuthSecret(ctx, reader, pool, namespace, active.ControllerEpoch)
	if err != nil {
		return "", nil, err
	}
	bearer := strings.TrimSpace(string(secret.Data[runtimePoolControllerTokenKey]))
	capability := append([]byte(nil), secret.Data[runtimePoolCapabilitySecretKey]...)
	if len(bearer) < 32 || len(capability) < harnessv2.MinCapabilitySecretBytes {
		return "", nil, fmt.Errorf("runtime pool auth Secret is incomplete")
	}
	return bearer, capability, nil
}

// resolveRuntimePoolAuthSecret resolves the exact controller-auth Secret for
// one RuntimePool epoch. Provider-workspace pools bind their unpredictable
// Secret name and immutable UID on the RuntimePool; label-only discovery is
// intentionally forbidden for those pools. Deployment-backed pools retain
// epoch selection because their deterministic mounted Secret predates the
// private binding contract.
func resolveRuntimePoolAuthSecret(
	ctx context.Context,
	reader client.Reader,
	pool *corev1alpha1.RuntimePool,
	namespace string,
	epoch int64,
) (*corev1.Secret, error) {
	if reader == nil || pool == nil || strings.TrimSpace(namespace) == "" || epoch <= 0 {
		return nil, fmt.Errorf("runtime pool auth Secret lookup is incomplete")
	}
	if pool.Spec.ExecutionWorkspace != nil {
		binding := strings.TrimSpace(pool.Annotations[runtimePoolPrivateAuthSecretBindingAnnotation(epoch)])
		if binding == "" {
			return nil, fmt.Errorf("private RuntimePool auth Secret binding for controller epoch %d is missing", epoch)
		}
		name, uid, err := parseRuntimePoolPrivateSecretBinding(binding)
		if err != nil {
			return nil, err
		}
		secret := &corev1.Secret{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret); err != nil {
			return nil, fmt.Errorf("read bound private RuntimePool auth Secret: %w", err)
		}
		if secret.UID != uid {
			return nil, fmt.Errorf("bound private RuntimePool auth Secret UID changed")
		}
		cfg := runtimePoolConfig{
			namespace: namespace,
			baseName:  runtimePoolResourceName(pool.Namespace, pool.Name),
			labels: map[string]string{
				runtimePoolManagedByLabel:   runtimePoolManagedByLabelValue,
				runtimePoolApplicationLabel: runtimePoolApplicationLabelValue,
				runtimePoolKeyLabel:         runtimePoolKey(pool.Namespace, pool.Name),
				runtimePoolNameLabel:        pool.Name,
				runtimePoolNamespaceLabel:   pool.Namespace,
				runtimePoolUIDLabel:         string(pool.UID),
				runtimePoolNetworkRoleLabel: "provider-client",
			},
			controllerEpoch: epoch,
		}
		if !runtimePoolPrivateAuthSecretMatchesPool(secret, pool, cfg) {
			return nil, fmt.Errorf("bound private RuntimePool auth Secret does not carry the exact immutable RuntimePool ownership identity")
		}
		return secret.DeepCopy(), nil
	}

	var secrets corev1.SecretList
	if err := reader.List(ctx, &secrets, client.InNamespace(namespace), client.MatchingLabels{
		runtimePoolAuthLabel: booleanTrueValue, runtimePoolUIDLabel: string(pool.UID),
	}); err != nil {
		return nil, err
	}
	// During graceful epoch replacement both the draining instance's Secret
	// and the next epoch's Secret exist; select the one mounted by the pool's
	// exact active instance instead of requiring one Secret globally.
	return runtimePoolAuthSecretForEpoch(secrets.Items, epoch)
}

// runtimePoolAuthSecretForEpoch selects the auth Secret bound to the given
// controller epoch (name suffix auth-e<epoch>) from the pool's labeled
// Secrets.
func runtimePoolAuthSecretForEpoch(secrets []corev1.Secret, epoch int64) (*corev1.Secret, error) {
	matched := runtimePoolAuthSecretsForEpoch(secrets, epoch)
	if len(matched) != 1 {
		return nil, fmt.Errorf("runtime pool requires exactly one auth Secret for controller epoch %d", epoch)
	}
	return &matched[0], nil
}

func runtimePoolAuthSecretsForEpoch(secrets []corev1.Secret, epoch int64) []corev1.Secret {
	epochValue := strconv.FormatInt(epoch, 10)
	legacySuffix := "auth-e" + epochValue
	randomSuffixPrefix := legacySuffix + "-"
	matched := make([]corev1.Secret, 0, 1)
	for i := range secrets {
		secretEpoch := strings.TrimSpace(secrets[i].Labels[runtimePoolCredentialEpochLabel])
		if secretEpoch == epochValue || (secretEpoch == "" &&
			(strings.HasSuffix(secrets[i].Name, legacySuffix) ||
				strings.HasPrefix(runtimePoolAuthSuffixPattern.FindString(secrets[i].Name), randomSuffixPrefix))) {
			matched = append(matched, *secrets[i].DeepCopy())
		}
	}
	return matched
}

func writeACPMCPError(w http.ResponseWriter, status int, message string) {
	writeACPMCPJSON(w, status, map[string]any{"error": message})
}

func writeACPMCPJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
