package supervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

func TestMCPProxyIsPromptScopedAndForwardsExactAuthorization(t *testing.T) {
	var calls atomic.Int32
	captured := make(chan harnessv2.MCPBrokerCallRequest, 1)
	broker := MCPBrokerFunc(func(_ context.Context, request harnessv2.MCPBrokerCallRequest) (harnessv2.MCPBrokerCallResponse, error) {
		calls.Add(1)
		captured <- request
		return harnessv2.MCPBrokerCallResponse{
			Protocol: harnessv2.ProtocolVersion, CallID: request.Call.CallID,
			Result: json.RawMessage(`{"value":"ok"}`),
		}, nil
	})
	session, server := newTestMCPProxySession(t, broker, false)

	initialize := doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	if initialize.StatusCode != http.StatusOK {
		t.Fatalf("initialize status = %d", initialize.StatusCode)
	}
	_ = initialize.Body.Close()

	idleList := decodeMCPResponse(t, doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`))
	if idleList.Error != nil || responseToolCount(idleList.Result) != 1 {
		t.Fatalf("idle tools/list = %#v", idleList)
	}
	idleCall := decodeMCPResponse(t, doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":"call-1","method":"tools/call","params":{"name":"lookup","arguments":{},"_meta":{"progressToken":1}}}`))
	if idleCall.Error == nil || calls.Load() != 0 {
		t.Fatalf("idle tool call = %#v calls=%d", idleCall, calls.Load())
	}

	now := time.Now().UTC()
	authorization, lease := testMCPAuthorization(t, session.fence, now, false)
	if err := session.activate(authorization, lease, now); err != nil {
		t.Fatal(err)
	}
	pendingCall := decodeMCPResponse(t, doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":"call-2","method":"tools/call","params":{"name":"lookup","arguments":{},"_meta":{"progressToken":2}}}`))
	if pendingCall.Error == nil || calls.Load() != 0 {
		t.Fatalf("pre-accept tool call = %#v calls=%d", pendingCall, calls.Load())
	}
	if err := session.markRunning(authorization.PromptID, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	activeList := decodeMCPResponse(t, doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":3,"method":"tools/list"}`))
	if activeList.Error != nil || responseToolCount(activeList.Result) != 1 {
		t.Fatalf("active tools/list = %#v", activeList)
	}
	activeCall := decodeMCPResponse(t, doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":"call-3","method":"tools/call","params":{"name":"lookup","arguments":{"query":"x"}}}`))
	if activeCall.Error != nil {
		t.Fatalf("active tool call = %#v", activeCall)
	}
	select {
	case request := <-captured:
		if request.Metadata.Fence != session.fence || request.Authorization.RuntimeSessionUID != session.fence.RuntimeSessionUID ||
			request.Authorization.SessionGeneration != session.fence.RuntimeSessionGeneration ||
			request.Metadata.TaskUID != authorization.TaskUID || request.Metadata.TaskAttempt != authorization.TaskAttempt ||
			request.Metadata.PromptID != authorization.PromptID || request.Lease.Generation != lease.Generation ||
			request.Authorization.ToolPolicy.DescriptorDigest != authorization.ToolPolicy.DescriptorDigest {
			t.Fatalf("forwarded request lost exact prompt authorization: %#v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("broker did not receive active tool call")
	}

	unauthorized := doMCPRequest(t, server, "wrong", `{"jsonrpc":"2.0","id":4,"method":"tools/list"}`)
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong credential status = %d", unauthorized.StatusCode)
	}
	_ = unauthorized.Body.Close()
}

func TestMCPProxyToolCallMetadata(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fields    string
		errorCode int
	}{
		{name: "numeric progress token", fields: `,"_meta":{"progressToken":2}`},
		{name: "string progress token and extensions", fields: `,"_meta":{"progressToken":"progress-1","client":{"version":1}}`},
		{name: "unknown parameter", fields: `,"progressToken":2`, errorCode: -32602},
		{name: "metadata must be an object", fields: `,"_meta":[2]`, errorCode: -32602},
		{name: "metadata stays request bounded", fields: `,"_meta":{"padding":"` + strings.Repeat("x", harnessv2.MaxMCPArgumentsBytes+(64<<10)) + `"}`, errorCode: -32600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			captured := make(chan harnessv2.MCPBrokerCallRequest, 1)
			session, endpoint := newTestMCPProxySession(t, MCPBrokerFunc(func(_ context.Context, request harnessv2.MCPBrokerCallRequest) (harnessv2.MCPBrokerCallResponse, error) {
				captured <- request
				return harnessv2.MCPBrokerCallResponse{
					Protocol: harnessv2.ProtocolVersion, CallID: request.Call.CallID, Result: json.RawMessage(`{"value":"ok"}`),
				}, nil
			}), false)
			now := time.Now().UTC()
			authorization, lease := testMCPAuthorization(t, session.fence, now, false)
			if err := session.activate(authorization, lease, now); err != nil {
				t.Fatal(err)
			}
			if err := session.markRunning(authorization.PromptID, now); err != nil {
				t.Fatal(err)
			}
			payload := `{"jsonrpc":"2.0","id":"call-1","method":"tools/call","params":{"name":"lookup","arguments":{"query":"x"}` + tc.fields + `}}`
			response := decodeMCPResponse(t, doMCPRequest(t, endpoint, "credential", payload))
			if tc.errorCode != 0 {
				if response.Error == nil || response.Error.Code != tc.errorCode {
					t.Fatalf("MCP response = %#v, want error %d", response, tc.errorCode)
				}
				if len(captured) != 0 {
					t.Fatal("invalid tool call reached the broker")
				}
				return
			}
			if response.Error != nil {
				t.Fatalf("MCP metadata rejected: %#v", response.Error)
			}
			select {
			case request := <-captured:
				if request.Metadata.Fence != session.fence || request.Metadata.TaskUID != authorization.TaskUID ||
					request.Metadata.PromptID != authorization.PromptID || request.Call.ToolName != "lookup" ||
					!bytes.Equal(request.Call.Arguments, []byte(`{"query":"x"}`)) {
					t.Fatal("MCP metadata changed the broker call or its authority")
				}
			default:
				t.Fatal("tool call did not reach the broker")
			}
		})
	}
}

func TestMCPProxySettlementRevokesAndCancelsInflightCall(t *testing.T) {
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	var once sync.Once
	broker := MCPBrokerFunc(func(ctx context.Context, request harnessv2.MCPBrokerCallRequest) (harnessv2.MCPBrokerCallResponse, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		close(cancelled)
		return harnessv2.MCPBrokerCallResponse{}, ctx.Err()
	})
	session, server := newTestMCPProxySession(t, broker, false)
	now := time.Now().UTC()
	authorization, lease := testMCPAuthorization(t, session.fence, now, false)
	if err := session.activate(authorization, lease, now); err != nil {
		t.Fatal(err)
	}
	if err := session.markRunning(authorization.PromptID, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	responseDone := make(chan mcpJSONRPCResponse, 1)
	go func() {
		responseDone <- decodeMCPResponse(t, doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":"inflight","method":"tools/call","params":{"name":"lookup","arguments":{}}}`))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("broker call did not start")
	}
	session.deactivate(authorization.PromptID, harnessv2.RuntimeSessionStateValidating)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("settlement did not cancel in-flight broker call")
	}
	select {
	case response := <-responseDone:
		if response.Error == nil {
			t.Fatalf("cancelled call response = %#v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled MCP request did not return")
	}
	postSettlement := decodeMCPResponse(t, doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":"after","method":"tools/call","params":{"name":"lookup","arguments":{}}}`))
	if postSettlement.Error == nil {
		t.Fatalf("post-settlement call = %#v", postSettlement)
	}
}

func TestMCPProxyApprovalPolicyIsFailClosedAndOnceBound(t *testing.T) {
	var calls atomic.Int32
	broker := MCPBrokerFunc(func(_ context.Context, request harnessv2.MCPBrokerCallRequest) (harnessv2.MCPBrokerCallResponse, error) {
		calls.Add(1)
		if request.Call.Approval == nil || request.Call.Approval.ToolName != "mutate" {
			t.Fatalf("broker call omitted approval evidence: %#v", request.Call)
		}
		return harnessv2.MCPBrokerCallResponse{
			Protocol: harnessv2.ProtocolVersion, CallID: request.Call.CallID, Result: json.RawMessage(`{"changed":true}`),
		}, nil
	})
	session, server := newTestMCPProxySession(t, broker, true)
	now := time.Now().UTC()
	authorization, lease := testMCPAuthorization(t, session.fence, now, true)
	if err := session.activate(authorization, lease, now); err != nil {
		t.Fatal(err)
	}
	if err := session.markRunning(authorization.PromptID, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	withoutApproval := decodeMCPResponse(t, doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":"once","method":"tools/call","params":{"name":"mutate","arguments":{}}}`))
	if withoutApproval.Error == nil || calls.Load() != 0 {
		t.Fatalf("unapproved call = %#v calls=%d", withoutApproval, calls.Load())
	}
	approvedToolCallID, err := canonicalACPToolCallID("provider-call-1")
	if err != nil {
		t.Fatal(err)
	}
	evidence := harnessv2.MCPApprovalEvidence{
		PermissionRequestID: "permission-1", ToolCallID: approvedToolCallID, ToolName: "mutate",
		GrantedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := session.grantApproval(authorization.PromptID, evidence); err != nil {
		t.Fatal(err)
	}
	// A call whose ID differs from the approved tool call ID must not consume
	// the allow-once grant, even though it is the first to arrive.
	imposter := decodeMCPResponse(t, doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":"different","method":"tools/call","params":{"name":"mutate","arguments":{}}}`))
	if imposter.Error == nil || calls.Load() != 0 {
		t.Fatalf("non-approved call consumed allow-once = %#v calls=%d", imposter, calls.Load())
	}
	// The approved tool call ID (used as the JSON-RPC request ID) is authorized.
	approved := decodeMCPResponse(t, doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":"provider-call-1","method":"tools/call","params":{"name":"mutate","arguments":{}}}`))
	if approved.Error != nil || calls.Load() != 1 {
		t.Fatalf("approved call = %#v calls=%d", approved, calls.Load())
	}
	// A retry of the approved call still passes the proxy's approval gate (the
	// downstream broker journal deduplicates it by operation ID in production);
	// the grant is bound to the approved call ID, not consumed on first use.
	replay := decodeMCPResponse(t, doMCPRequest(t, server, "credential", `{"jsonrpc":"2.0","id":"provider-call-1","method":"tools/call","params":{"name":"mutate","arguments":{}}}`))
	if replay.Error != nil || calls.Load() != 2 {
		t.Fatalf("approved-call retry = %#v calls=%d", replay, calls.Load())
	}
}

func TestMCPProxyReadOnlyAllowOnceGrantIsConsumed(t *testing.T) {
	var calls atomic.Int32
	broker := MCPBrokerFunc(func(_ context.Context, request harnessv2.MCPBrokerCallRequest) (harnessv2.MCPBrokerCallResponse, error) {
		calls.Add(1)
		return harnessv2.MCPBrokerCallResponse{
			Protocol: harnessv2.ProtocolVersion, CallID: request.Call.CallID, Result: json.RawMessage(`{"ok":true}`),
		}, nil
	})
	proxy, err := newMCPProxy(broker)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = proxy.close(ctx)
	})
	fence := harnessv2.Fence{
		RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot", ControllerEpoch: 2,
		RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 4,
		RuntimeSessionUID: "session-uid", RuntimeSessionGeneration: 3,
		RuntimeProfileDigest:       harnessv2.ProfileDigest(testDigest("profile")),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	now := time.Now().UTC()
	// A read-only tool that still requires approval: this is exactly the class
	// that bypasses the broker's operation journal, so the allow-once grant must
	// be spent in-process rather than relying on journal dedup.
	authorization, lease := buildTestMCPAuthorization(t, fence, now, "lookup", harnessv2.MCPToolEffectReadOnly, true)
	session, binding, err := proxy.newSession(fence, authorization.Configuration())
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	session.credential = []byte("credential")
	session.mu.Unlock()
	server := binding.URL
	if err := session.activate(authorization, lease, now); err != nil {
		t.Fatal(err)
	}
	if err := session.markRunning(authorization.PromptID, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	approvedToolCallID, err := canonicalACPToolCallID("provider-call-ro")
	if err != nil {
		t.Fatal(err)
	}
	evidence := harnessv2.MCPApprovalEvidence{
		PermissionRequestID: "permission-ro", ToolCallID: approvedToolCallID, ToolName: "lookup",
		GrantedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := session.grantApproval(authorization.PromptID, evidence); err != nil {
		t.Fatal(err)
	}
	first := decodeMCPResponse(t, doMCPRequest(t, server, "credential",
		`{"jsonrpc":"2.0","id":"provider-call-ro","method":"tools/call","params":{"name":"lookup","arguments":{}}}`))
	if first.Error != nil || calls.Load() != 1 {
		t.Fatalf("first read-only approved call = %#v calls=%d", first, calls.Load())
	}
	// The single approval authorized exactly one read-only call. Replaying the
	// exact approved call ID — here with different arguments to model the reuse
	// attack — must be rejected because the grant was consumed on first use.
	replay := decodeMCPResponse(t, doMCPRequest(t, server, "credential",
		`{"jsonrpc":"2.0","id":"provider-call-ro","method":"tools/call","params":{"name":"lookup","arguments":{"q":"changed"}}}`))
	if replay.Error == nil || calls.Load() != 1 {
		t.Fatalf("read-only allow-once replay = %#v calls=%d", replay, calls.Load())
	}
}

func newTestMCPProxySession(t *testing.T, broker MCPBroker, requireApproval bool) (*mcpProxySession, string) {
	t.Helper()
	proxy, err := newMCPProxy(broker)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = proxy.close(ctx)
	})
	fence := harnessv2.Fence{
		RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot", ControllerEpoch: 2,
		RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 4,
		RuntimeSessionUID: "session-uid", RuntimeSessionGeneration: 3,
		RuntimeProfileDigest:       harnessv2.ProfileDigest(testDigest("profile")),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	authorization, _ := testMCPAuthorization(t, fence, time.Now().UTC(), requireApproval)
	session, binding, err := proxy.newSession(fence, authorization.Configuration())
	if err != nil {
		t.Fatal(err)
	}
	if binding.Type != providerProxyScheme || binding.URL == "" || len(binding.Headers) != 1 {
		t.Fatalf("MCP binding = %#v", binding)
	}
	// The helper uses a stable credential so requests are concise and the random
	// production credential never appears in test failure output.
	session.mu.Lock()
	session.credential = []byte("credential")
	session.mu.Unlock()
	return session, binding.URL
}

func testMCPAuthorization(t *testing.T, fence harnessv2.Fence, now time.Time, requireApproval bool) (harnessv2.PromptMCPAuthorization, harnessv2.PromptLease) {
	t.Helper()
	name, effect := "lookup", harnessv2.MCPToolEffectReadOnly
	if requireApproval {
		name, effect = "mutate", harnessv2.MCPToolEffectConsequential
	}
	return buildTestMCPAuthorization(t, fence, now, name, effect, requireApproval)
}

func buildTestMCPAuthorization(
	t *testing.T, fence harnessv2.Fence, now time.Time,
	name string, effect harnessv2.MCPToolEffect, requireApproval bool,
) (harnessv2.PromptMCPAuthorization, harnessv2.PromptLease) {
	t.Helper()
	descriptors := []harnessv2.MCPToolDescriptor{{
		Name: name, Description: "test tool", InputSchema: json.RawMessage(`{"type":"object"}`),
		Source: harnessv2.MCPToolSourceBrokeredBuiltin, Effect: effect,
	}}
	digest, err := harnessv2.CanonicalMCPToolDescriptorDigest(descriptors)
	if err != nil {
		t.Fatal(err)
	}
	approval := harnessv2.MCPApprovalPolicy{}
	if requireApproval {
		approval.RequiredTools = []string{name}
	}
	toolPolicy := harnessv2.MCPToolPolicy{
		AllowedToolNames: []string{name}, AllowBash: true, Tools: descriptors, DescriptorDigest: digest,
	}
	toolDigest, err := harnessv2.CanonicalRuntimeToolPolicyDigest(toolPolicy.AllowedToolNames, toolPolicy.DisallowedToolNames, toolPolicy.AllowBash)
	if err != nil {
		t.Fatal(err)
	}
	approvalDigest, err := harnessv2.CanonicalMCPApprovalPolicyDigest(approval)
	if err != nil {
		t.Fatal(err)
	}
	mcpDigest, err := harnessv2.CanonicalMCPConfigurationDigest(toolPolicy.AllowedToolNames)
	if err != nil {
		t.Fatal(err)
	}
	lease := harnessv2.PromptLease{Generation: 1, IssuedAt: now, ExpiresAt: now.Add(2 * time.Minute)}
	return harnessv2.PromptMCPAuthorization{
		RuntimeSessionUID: fence.RuntimeSessionUID, SessionGeneration: fence.RuntimeSessionGeneration,
		TaskUID: "task-uid", TaskAttempt: 1, PromptID: testPromptOneID, LeaseGeneration: lease.Generation,
		ToolPolicyDigest: toolDigest, ApprovalPolicyDigest: approvalDigest, MCPConfigurationDigest: mcpDigest,
		ToolPolicy: toolPolicy, ApprovalPolicy: approval, ExpiresAt: now.Add(time.Minute),
	}, lease
}

func doMCPRequest(t *testing.T, endpoint, credential, payload string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+credential)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decodeMCPResponse(t *testing.T, response *http.Response) mcpJSONRPCResponse {
	t.Helper()
	defer response.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var decoded mcpJSONRPCResponse
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&decoded); err != nil {
		t.Fatalf("decode MCP response: %v body=%s", err, data)
	}
	return decoded
}

func responseToolCount(value any) int {
	object, ok := value.(map[string]any)
	if !ok {
		return -1
	}
	tools, ok := object["tools"].([]any)
	if !ok {
		return -1
	}
	return len(tools)
}
