package supervisor

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const (
	mcpProxyPathPrefix        = "/_orka/mcp/"
	mcpProtocolVersion        = "2025-06-18"
	defaultMCPMaxConnections  = 16
	defaultMCPMaxSessionCalls = 2
)

type mcpProxy struct {
	broker   MCPBroker
	listener net.Listener
	server   *http.Server
	slots    chan struct{}

	mu       sync.RWMutex
	sessions map[string]*mcpProxySession
	closed   bool
}

type mcpProxySession struct {
	proxy         *mcpProxy
	route         string
	credential    []byte
	url           string
	fence         harnessv2.Fence
	configuration harnessv2.MCPPolicyConfiguration

	mu            sync.Mutex
	state         harnessv2.RuntimeSessionState
	authorization *harnessv2.PromptMCPAuthorization
	lease         harnessv2.PromptLease
	gateContext   context.Context
	gateCancel    context.CancelFunc
	leaseTimer    *time.Timer
	leaseVersion  uint64
	approvals     map[string][]mcpApprovalGrant
	closed        bool
	calls         chan struct{}
}

type mcpApprovalGrant struct {
	evidence harnessv2.MCPApprovalEvidence
}

// approvedCallMatches reports whether an MCP call ID corresponds to the tool
// call the user approved. The approved ToolCallID stored in the evidence is
// already the canonical ACP tool-call digest (mapPermission applies
// canonicalACPToolCallID), so the incoming MCP call ID is canonicalized the
// same way before comparison — a normal JSON-RPC string ID and the ACP tool
// call ID therefore normalize to the same digest.
func approvedCallMatches(approvedToolCallID, callID string) bool {
	if approvedToolCallID == "" || callID == "" {
		return false
	}
	canonical, err := canonicalACPToolCallID(callID)
	if err != nil {
		return false
	}
	return canonical == approvedToolCallID
}

type mcpJSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type mcpJSONRPCResponse struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      json.RawMessage  `json:"id,omitempty"`
	Result  any              `json:"result,omitempty"`
	Error   *mcpJSONRPCError `json:"error,omitempty"`
}

type mcpJSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpToolsCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	// MCP clients send progress tokens and extensions in _meta. The enclosing
	// request limit bounds it, and it never contributes broker authority.
	Meta map[string]json.RawMessage `json:"_meta,omitempty"`
}

func newMCPProxy(broker MCPBroker) (*mcpProxy, error) {
	if broker == nil {
		return nil, fmt.Errorf("controller MCP broker is required")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen on MCP loopback: %w", err)
	}
	proxy := &mcpProxy{
		broker: broker, listener: listener, sessions: make(map[string]*mcpProxySession),
		slots: make(chan struct{}, defaultMCPMaxConnections),
	}
	proxy.server = &http.Server{
		Handler: http.HandlerFunc(proxy.serveHTTP), ErrorLog: log.New(io.Discard, "", 0),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10,
	}
	go func() { _ = proxy.server.Serve(listener) }()
	return proxy, nil
}

func (p *mcpProxy) newSession(
	fence harnessv2.Fence,
	configuration harnessv2.MCPPolicyConfiguration,
) (*mcpProxySession, acp.MCPServer, error) {
	if err := fence.Validate(true); err != nil {
		return nil, acp.MCPServer{}, fmt.Errorf("MCP session fence: %w", err)
	}
	credential, err := randomMCPSecret(32)
	if err != nil {
		return nil, acp.MCPServer{}, err
	}
	for range 8 {
		route, routeErr := randomMCPSecret(24)
		if routeErr != nil {
			return nil, acp.MCPServer{}, routeErr
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, acp.MCPServer{}, fmt.Errorf("MCP proxy is closed")
		}
		if _, exists := p.sessions[route]; exists {
			p.mu.Unlock()
			continue
		}
		endpoint := providerProxyScheme + "://" + p.listener.Addr().String() + mcpProxyPathPrefix + route
		session := &mcpProxySession{
			proxy: p, route: route, credential: []byte(credential), url: endpoint, fence: fence,
			configuration: cloneMCPPolicyConfiguration(configuration),
			state:         harnessv2.RuntimeSessionStateIdle, approvals: make(map[string][]mcpApprovalGrant),
			calls: make(chan struct{}, defaultMCPMaxSessionCalls),
		}
		p.sessions[route] = session
		p.mu.Unlock()
		return session, acp.MCPServer{
			Type: "http", Name: "orka", URL: endpoint,
			Headers: []acp.HTTPHeader{{Name: "Authorization", Value: "Bearer " + credential}},
		}, nil
	}
	return nil, acp.MCPServer{}, fmt.Errorf("allocate unique MCP proxy route")
}

func randomMCPSecret(size int) (string, error) {
	if size < 16 {
		return "", fmt.Errorf("MCP proxy secret size is too small")
	}
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return "", fmt.Errorf("generate MCP proxy secret: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func (s *mcpProxySession) activate(auth harnessv2.PromptMCPAuthorization, lease harnessv2.PromptLease, now time.Time) error {
	metadata := harnessv2.MutationMetadata{
		Fence: s.fence, TaskUID: auth.TaskUID, TaskAttempt: auth.TaskAttempt, PromptID: auth.PromptID,
		OperationID: "mcp-activate", RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion,
		RequestDigest: testlessRequestDigest(), ExpiresAt: auth.ExpiresAt,
	}
	if err := auth.ValidateForAt(metadata, lease, now); err != nil {
		return err
	}
	if !s.configuration.Matches(auth.Configuration()) {
		return fmt.Errorf("prompt MCP policy does not match the RuntimeSession configuration")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.authorization != nil || s.state != harnessv2.RuntimeSessionStateIdle {
		return fmt.Errorf("MCP proxy session is not idle")
	}
	cloned := clonePromptMCPAuthorization(auth)
	s.authorization = &cloned
	s.lease = lease
	s.state = harnessv2.RuntimeSessionStateIdle
	s.gateContext, s.gateCancel = context.WithCancel(context.Background())
	s.approvals = make(map[string][]mcpApprovalGrant)
	s.resetLeaseTimerLocked(now)
	return nil
}

// testlessRequestDigest supplies a structurally valid placeholder to reuse the
// exact PromptMCPAuthorization identity validator without authorizing an HTTP
// mutation. Broker calls always carry their own canonical request digest.
func testlessRequestDigest() harnessv2.RequestDigest {
	return harnessv2.RequestDigest("sha256:" + strings.Repeat("0", 64))
}

func (s *mcpProxySession) markRunning(promptID harnessv2.PromptID, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.authorization == nil || s.authorization.PromptID != promptID ||
		s.gateCancel == nil || !s.authorization.AuthorizedAt(harnessv2.RuntimeSessionStatePromptRunning, s.lease, now) {
		return fmt.Errorf("MCP prompt authority is not active")
	}
	s.state = harnessv2.RuntimeSessionStatePromptRunning
	return nil
}

func (s *mcpProxySession) renew(auth harnessv2.PromptMCPAuthorization, lease harnessv2.PromptLease, now time.Time) error {
	metadata := harnessv2.MutationMetadata{
		Fence: s.fence, TaskUID: auth.TaskUID, TaskAttempt: auth.TaskAttempt, PromptID: auth.PromptID,
		OperationID: "mcp-renew", RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion,
		RequestDigest: testlessRequestDigest(), ExpiresAt: auth.ExpiresAt,
	}
	if err := auth.ValidateForAt(metadata, lease, now); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.authorization == nil || s.gateCancel == nil || s.state != harnessv2.RuntimeSessionStatePromptRunning ||
		s.authorization.PromptID != auth.PromptID || lease.Generation != s.lease.Generation+1 ||
		!now.Before(s.lease.ExpiresAt) {
		return fmt.Errorf("MCP prompt lease is no longer renewable")
	}
	if s.authorization.ToolPolicyDigest != auth.ToolPolicyDigest ||
		s.authorization.ApprovalPolicyDigest != auth.ApprovalPolicyDigest ||
		s.authorization.MCPConfigurationDigest != auth.MCPConfigurationDigest ||
		s.authorization.ToolPolicy.DescriptorDigest != auth.ToolPolicy.DescriptorDigest {
		return fmt.Errorf("MCP prompt policy changed during lease renewal")
	}
	cloned := clonePromptMCPAuthorization(auth)
	s.authorization = &cloned
	s.lease = lease
	s.resetLeaseTimerLocked(now)
	return nil
}

func (s *mcpProxySession) resetLeaseTimerLocked(now time.Time) {
	if s.leaseTimer != nil {
		s.leaseTimer.Stop()
	}
	s.leaseVersion++
	version := s.leaseVersion
	promptID := s.authorization.PromptID
	duration := s.authorization.ExpiresAt.Sub(now)
	if leaseDuration := s.lease.ExpiresAt.Sub(now); leaseDuration < duration {
		duration = leaseDuration
	}
	if duration < 0 {
		duration = 0
	}
	s.leaseTimer = time.AfterFunc(duration, func() { s.expire(promptID, version) })
}

func (s *mcpProxySession) expire(promptID harnessv2.PromptID, version uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.authorization == nil || s.authorization.PromptID != promptID || s.leaseVersion != version {
		return
	}
	slog.Warn("ACP MCP proxy prompt authorization expired without renewal; revoking tool access",
		"promptID", promptID, "leaseVersion", version)
	s.revokeLocked(harnessv2.RuntimeSessionStateCancelling)
}

func (s *mcpProxySession) deactivate(promptID harnessv2.PromptID, next harnessv2.RuntimeSessionState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.authorization == nil {
		if !s.closed {
			s.state = next
		}
		return
	}
	if s.authorization.PromptID != promptID {
		return
	}
	s.revokeLocked(next)
}

func (s *mcpProxySession) revoke(next harnessv2.RuntimeSessionState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revokeLocked(next)
}

func (s *mcpProxySession) revokeLocked(next harnessv2.RuntimeSessionState) {
	if s.leaseTimer != nil {
		s.leaseTimer.Stop()
		s.leaseTimer = nil
	}
	if s.gateCancel != nil {
		s.gateCancel()
		s.gateCancel = nil
	}
	s.gateContext = nil
	s.authorization = nil
	s.lease = harnessv2.PromptLease{}
	s.approvals = make(map[string][]mcpApprovalGrant)
	s.leaseVersion++
	s.state = next
}

func (s *mcpProxySession) resolveApprovalToolName(candidate, title string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.authorization == nil || s.state != harnessv2.RuntimeSessionStatePromptRunning {
		return "", fmt.Errorf("MCP prompt is not active")
	}
	for _, value := range []string{strings.TrimSpace(candidate), strings.TrimSpace(title)} {
		if value != "" && s.authorization.ApprovalPolicy.Requires(value) {
			return value, nil
		}
	}
	matched := ""
	lowerTitle := strings.ToLower(title)
	for _, name := range s.authorization.ApprovalPolicy.RequiredTools {
		if strings.Contains(lowerTitle, strings.ToLower(name)) {
			if matched != "" {
				return "", fmt.Errorf("permission title matches multiple approval-required tools")
			}
			matched = name
		}
	}
	if matched == "" {
		return "", fmt.Errorf("permission does not identify an approval-required tool")
	}
	return matched, nil
}

func (s *mcpProxySession) grantApproval(promptID harnessv2.PromptID, evidence harnessv2.MCPApprovalEvidence) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.authorization == nil || s.authorization.PromptID != promptID || s.state != harnessv2.RuntimeSessionStatePromptRunning {
		return fmt.Errorf("MCP prompt is not active")
	}
	if !s.authorization.ApprovalPolicy.Requires(evidence.ToolName) {
		return fmt.Errorf("MCP tool %q does not require approval", evidence.ToolName)
	}
	if evidence.ExpiresAt.After(s.authorization.ExpiresAt) {
		evidence.ExpiresAt = s.authorization.ExpiresAt
	}
	if evidence.ExpiresAt.After(s.lease.ExpiresAt) {
		evidence.ExpiresAt = s.lease.ExpiresAt
	}
	if err := evidence.ValidateFor(evidence.ToolName, time.Now().UTC()); err != nil {
		return err
	}
	s.approvals[evidence.ToolName] = append(s.approvals[evidence.ToolName], mcpApprovalGrant{evidence: evidence})
	return nil
}

func (s *mcpProxySession) authorizeCall(toolName, callID string, now time.Time) (
	context.Context,
	harnessv2.PromptMCPAuthorization,
	harnessv2.PromptLease,
	*MCPApprovalEvidenceReservation,
	error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.authorization == nil || s.gateContext == nil || s.gateCancel == nil ||
		s.state != harnessv2.RuntimeSessionStatePromptRunning ||
		!s.authorization.AuthorizedAt(s.state, s.lease, now) {
		return nil, harnessv2.PromptMCPAuthorization{}, harnessv2.PromptLease{}, nil, fmt.Errorf("prompt-scoped MCP authority is inactive")
	}
	descriptor, ok := s.authorization.ToolPolicy.Descriptor(toolName)
	if !ok || !descriptor.Source.Brokered() {
		return nil, harnessv2.PromptMCPAuthorization{}, harnessv2.PromptLease{}, nil, fmt.Errorf("MCP tool is not allowed")
	}
	var reservation *MCPApprovalEvidenceReservation
	if s.authorization.ApprovalPolicy.Requires(toolName) {
		for index := range s.approvals[toolName] {
			grant := &s.approvals[toolName][index]
			if !grant.evidence.ExpiresAt.After(now) {
				continue
			}
			// A reusable (allow-always) grant covers any call of the tool; a
			// non-reusable (allow-once) grant is bound to the exact tool call
			// the user approved, so a child cannot approve a benign call and
			// then execute a different one.
			if !grant.evidence.Reusable && !approvedCallMatches(grant.evidence.ToolCallID, callID) {
				continue
			}
			evidence := grant.evidence
			reservation = &MCPApprovalEvidenceReservation{Evidence: evidence}
			// Consume a non-reusable (allow-once) grant for a read-only tool on
			// reservation. Consequential tools are deduplicated and replay-bound
			// by the operation journal (runExternalEffectWithReplay returns the
			// originally approved outcome for a repeated operation identity and
			// rejects a changed payload), so their grant must survive an
			// idempotent retry. Read-only tools bypass that journal entirely, so
			// an unconsumed allow-once grant would let a child re-drive the same
			// approved call ID with new arguments while the evidence is
			// unexpired; spend it here so a single approval authorizes exactly
			// one read-only call.
			if !grant.evidence.Reusable && descriptor.Effect != harnessv2.MCPToolEffectConsequential {
				s.approvals[toolName] = append(s.approvals[toolName][:index], s.approvals[toolName][index+1:]...)
			}
			break
		}
		if reservation == nil {
			return nil, harnessv2.PromptMCPAuthorization{}, harnessv2.PromptLease{}, nil, fmt.Errorf("MCP tool approval is missing")
		}
	}
	return s.gateContext, clonePromptMCPAuthorization(*s.authorization), s.lease, reservation, nil
}

type MCPApprovalEvidenceReservation struct {
	Evidence harnessv2.MCPApprovalEvidence
}

func (s *mcpProxySession) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.revokeLocked(harnessv2.RuntimeSessionStateDeleted)
	s.mu.Unlock()
	s.proxy.mu.Lock()
	delete(s.proxy.sessions, s.route)
	s.proxy.mu.Unlock()
}

func (p *mcpProxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !isLoopbackRemote(r.RemoteAddr) {
		http.Error(w, "MCP endpoint is loopback-only", http.StatusForbidden)
		return
	}
	route, ok := splitMCPProxyRoute(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	p.mu.RLock()
	session := p.sessions[route]
	p.mu.RUnlock()
	if session == nil || !session.credentialMatches(r.Header.Get("Authorization")) {
		http.Error(w, "MCP session authentication failed", http.StatusUnauthorized)
		return
	}
	if !tryAcquireMCPSlot(p.slots) {
		http.Error(w, "MCP proxy is at capacity", http.StatusTooManyRequests)
		return
	}
	defer releaseMCPSlot(p.slots)
	if !tryAcquireMCPSlot(session.calls) {
		http.Error(w, "MCP session is at capacity", http.StatusTooManyRequests)
		return
	}
	defer releaseMCPSlot(session.calls)
	body := http.MaxBytesReader(w, r.Body, int64(harnessv2.MaxMCPArgumentsBytes+(64<<10)))
	defer body.Close() //nolint:errcheck
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	var request mcpJSONRPCRequest
	if err := decoder.Decode(&request); err != nil || request.JSONRPC != "2.0" || strings.TrimSpace(request.Method) == "" {
		writeMCPRPCError(w, request.ID, -32600, "invalid MCP JSON-RPC request")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeMCPRPCError(w, request.ID, -32600, "invalid MCP JSON-RPC request")
		return
	}
	switch request.Method {
	case "initialize":
		writeMCPRPCResult(w, request.ID, map[string]any{
			"protocolVersion": mcpProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "orka-prompt-broker", "version": "v2"},
		})
	case "notifications/initialized":
		w.WriteHeader(http.StatusAccepted)
	case "ping":
		writeMCPRPCResult(w, request.ID, map[string]any{})
	case "tools/list":
		writeMCPRPCResult(w, request.ID, map[string]any{"tools": session.listTools(time.Now().UTC())})
	case "tools/call":
		session.handleToolCall(w, r, request)
	default:
		writeMCPRPCError(w, request.ID, -32601, "MCP method is not supported")
	}
}

func (s *mcpProxySession) handleToolCall(w http.ResponseWriter, r *http.Request, rpc mcpJSONRPCRequest) {
	var params mcpToolsCallParams
	decoder := json.NewDecoder(bytes.NewReader(rpc.Params))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&params); err != nil {
		writeMCPRPCError(w, rpc.ID, -32602, "invalid MCP tool call parameters")
		return
	}
	if len(params.Arguments) == 0 {
		params.Arguments = json.RawMessage(`{}`)
	}
	callID, err := canonicalMCPCallID(rpc.ID)
	if err != nil {
		writeMCPRPCError(w, rpc.ID, -32602, "MCP tool call requires a bounded request ID")
		return
	}
	now := time.Now().UTC()
	gate, authorization, lease, approval, err := s.authorizeCall(params.Name, callID, now)
	if err != nil {
		writeMCPRPCError(w, rpc.ID, -32001, "MCP tool call is not authorized")
		return
	}
	call := harnessv2.MCPToolCall{CallID: callID, ToolName: params.Name, Arguments: params.Arguments}
	if approval != nil {
		evidence := approval.Evidence
		call.Approval = &evidence
	}
	expiresAt := now.Add(30 * time.Second)
	if authorization.ExpiresAt.Before(expiresAt) {
		expiresAt = authorization.ExpiresAt
	}
	if lease.ExpiresAt.Before(expiresAt) {
		expiresAt = lease.ExpiresAt
	}
	metadata := harnessv2.MutationMetadata{
		Fence: s.fence, TaskUID: authorization.TaskUID, TaskAttempt: authorization.TaskAttempt,
		PromptID: authorization.PromptID, OperationID: mcpOperationID(authorization, callID),
		RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion, ExpiresAt: expiresAt,
	}
	request := harnessv2.MCPBrokerCallRequest{
		Protocol: harnessv2.ProtocolVersion, SessionState: harnessv2.RuntimeSessionStatePromptRunning,
		Metadata: metadata, Lease: lease, Authorization: authorization, Call: call,
	}
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(gate, cancel)
	defer func() {
		stop()
		cancel()
	}()
	response, err := s.proxy.broker.Call(ctx, request)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		writeMCPRPCError(w, rpc.ID, -32002, "MCP broker call failed")
		return
	}
	if response.CallID != callID || response.Validate() != nil {
		writeMCPRPCError(w, rpc.ID, -32002, "MCP broker returned an invalid response")
		return
	}
	result := map[string]any{
		"content": []map[string]any{{"type": "text", "text": string(response.Result)}},
		"isError": response.IsError,
	}
	var structured map[string]any
	if json.Unmarshal(response.Result, &structured) == nil && structured != nil {
		result["structuredContent"] = structured
	}
	if response.Replayed {
		result["_meta"] = map[string]any{"orka.replayed": true}
	}
	writeMCPRPCResult(w, rpc.ID, result)
}

func (s *mcpProxySession) listTools(_ time.Time) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.state == harnessv2.RuntimeSessionStateDeleted {
		return []map[string]any{}
	}
	result := make([]map[string]any, 0, len(s.configuration.ToolPolicy.Tools))
	for _, descriptor := range s.configuration.ToolPolicy.Tools {
		if !descriptor.Source.Brokered() {
			continue
		}
		var schema any
		if err := json.Unmarshal(descriptor.InputSchema, &schema); err != nil {
			continue
		}
		result = append(result, map[string]any{
			"name": descriptor.Name, "description": descriptor.Description, "inputSchema": schema,
		})
	}
	return result
}

func (s *mcpProxySession) credentialMatches(header string) bool {
	value := strings.TrimSpace(header)
	if len(value) >= len("Bearer ") && strings.EqualFold(value[:len("Bearer ")], "Bearer ") {
		value = strings.TrimSpace(value[len("Bearer "):])
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(value) != len(s.credential) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(value), s.credential) == 1
}

func cloneMCPSlice[T any](input []T) []T {
	if input == nil {
		return nil
	}
	output := make([]T, len(input))
	copy(output, input)
	return output
}

func cloneMCPPolicyConfiguration(input harnessv2.MCPPolicyConfiguration) harnessv2.MCPPolicyConfiguration {
	output := input
	output.ToolPolicy.AllowedToolNames = cloneMCPSlice(input.ToolPolicy.AllowedToolNames)
	output.ToolPolicy.DisallowedToolNames = cloneMCPSlice(input.ToolPolicy.DisallowedToolNames)
	output.ToolPolicy.Tools = cloneMCPSlice(input.ToolPolicy.Tools)
	for index := range output.ToolPolicy.Tools {
		output.ToolPolicy.Tools[index].InputSchema = append(json.RawMessage(nil), input.ToolPolicy.Tools[index].InputSchema...)
	}
	output.ApprovalPolicy.RequiredTools = cloneMCPSlice(input.ApprovalPolicy.RequiredTools)
	return output
}

func clonePromptMCPAuthorization(input harnessv2.PromptMCPAuthorization) harnessv2.PromptMCPAuthorization {
	output := input
	configuration := cloneMCPPolicyConfiguration(input.Configuration())
	output.ToolPolicy = configuration.ToolPolicy
	output.ApprovalPolicy = configuration.ApprovalPolicy
	return output
}

func canonicalMCPCallID(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || len(raw) > 512 {
		return "", fmt.Errorf("MCP request ID is required")
	}
	var text string
	if json.Unmarshal(raw, &text) == nil && strings.TrimSpace(text) != "" {
		if len(text) <= 256 && !strings.ContainsAny(text, " \t\r\n") {
			return text, nil
		}
	}
	canonical, err := harnessv2.CanonicalJSON(raw)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "rpc-" + hex.EncodeToString(sum[:16]), nil
}

func mcpOperationID(auth harnessv2.PromptMCPAuthorization, callID string) harnessv2.OperationID {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s:%d:%s:%d:%s:%s", auth.RuntimeSessionUID, auth.SessionGeneration, auth.TaskUID, auth.TaskAttempt, auth.PromptID, callID))
	return harnessv2.OperationID("mcp-" + hex.EncodeToString(sum[:]))
}

func splitMCPProxyRoute(path string) (string, bool) {
	if !strings.HasPrefix(path, mcpProxyPathPrefix) {
		return "", false
	}
	route := strings.TrimPrefix(path, mcpProxyPathPrefix)
	if route == "" || strings.Contains(route, "/") {
		return "", false
	}
	return route, true
}

func isLoopbackRemote(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func tryAcquireMCPSlot(slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func releaseMCPSlot(slots chan struct{}) { <-slots }

func writeMCPRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(mcpJSONRPCResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func writeMCPRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(mcpJSONRPCResponse{JSONRPC: "2.0", ID: id, Error: &mcpJSONRPCError{Code: code, Message: message}})
}

func (p *mcpProxy) close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	sessions := make([]*mcpProxySession, 0, len(p.sessions))
	for _, session := range p.sessions {
		sessions = append(sessions, session)
	}
	p.mu.Unlock()
	for _, session := range sessions {
		session.close()
	}
	if err := p.server.Shutdown(ctx); err != nil {
		return errors.Join(err, p.server.Close())
	}
	return nil
}
