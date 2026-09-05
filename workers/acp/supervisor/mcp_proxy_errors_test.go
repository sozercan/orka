package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

func TestMCPProxyPreservesToolErrorsWithoutHidingBrokerFailures(t *testing.T) {
	for _, name := range []string{"tool failure", "broker failure", "wrong call ID", "invalid result", "wrong protocol"} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			broker := MCPBrokerFunc(func(_ context.Context, request harnessv2.MCPBrokerCallRequest) (harnessv2.MCPBrokerCallResponse, error) {
				response := harnessv2.MCPBrokerCallResponse{
					Protocol: harnessv2.ProtocolVersion, CallID: request.Call.CallID, Result: json.RawMessage(`{"ok":true}`),
				}
				if calls.Add(1) > 1 {
					return response, nil
				}
				switch name {
				case "tool failure":
					response.IsError = true
					response.Result = json.RawMessage(`{"isError":true,"error":"MCP tool execution failed"}`)
				case "broker failure":
					return harnessv2.MCPBrokerCallResponse{}, errors.New("private broker diagnostic")
				case "wrong call ID":
					response.CallID = "other-call"
				case "invalid result":
					response.Result = json.RawMessage(`{"unfinished":`)
				case "wrong protocol":
					response.Protocol = "invalid"
				}
				return response, nil
			})
			session, endpoint := newTestMCPProxySession(t, broker, false)
			now := time.Now().UTC()
			authorization, lease := testMCPAuthorization(t, session.fence, now, false)
			if err := session.activate(authorization, lease, now); err != nil {
				t.Fatal(err)
			}
			if err := session.markRunning(authorization.PromptID, now); err != nil {
				t.Fatal(err)
			}
			failed := decodeMCPResponse(t, doMCPRequest(t, endpoint, "credential", `{"jsonrpc":"2.0","id":"failure","method":"tools/call","params":{"name":"lookup","arguments":{}}}`))
			if name == "tool failure" {
				result, ok := failed.Result.(map[string]any)
				if failed.Error != nil || !ok || result["isError"] != true {
					t.Fatalf("tool execution failure = %#v, want MCP result.isError", failed)
				}
			} else if failed.Error == nil || failed.Error.Code != -32002 || failed.Result != nil {
				t.Fatalf("broker/protocol failure = %#v, want JSON-RPC -32002", failed)
			}
			encoded, err := json.Marshal(failed)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "private broker diagnostic") {
				t.Fatal("broker diagnostic reached the ACP process")
			}
			recovery := decodeMCPResponse(t, doMCPRequest(t, endpoint, "credential", `{"jsonrpc":"2.0","id":"recovery","method":"tools/call","params":{"name":"lookup","arguments":{}}}`))
			result, ok := recovery.Result.(map[string]any)
			if recovery.Error != nil || !ok || result["isError"] != false || calls.Load() != 2 {
				t.Fatalf("later authorized call = %#v calls=%d", recovery, calls.Load())
			}
		})
	}
}
