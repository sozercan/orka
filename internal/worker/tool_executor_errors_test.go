package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

func TestToolExecutorMarksOnlyCompletedAPIErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		execution bool
	}{
		{name: "service unavailable", status: http.StatusServiceUnavailable, execution: true},
		{name: "invalid tool input", status: http.StatusUnprocessableEntity, execution: true},
		{name: "rate limited", status: http.StatusTooManyRequests, execution: true},
		{name: "redirect", status: http.StatusFound},
		{name: "unauthorized", status: http.StatusUnauthorized},
		{name: "forbidden", status: http.StatusForbidden},
		{name: "proxy authentication", status: http.StatusProxyAuthRequired},
		{name: "invalid status class", status: 600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("upstream diagnostic"))
			}))
			defer server.Close()
			executor := NewToolExecutorForNamespace("default", nil, server.Client())
			tool := &corev1alpha1.Tool{Spec: corev1alpha1.ToolSpec{HTTP: &corev1alpha1.HTTPExecution{URL: server.URL}}}
			result, err := executor.Execute(context.Background(), tool, json.RawMessage(`{}`))
			var executionErr ToolExecutionError
			if err == nil || errors.As(err, &executionErr) != tc.execution || !ToolRequestWasAttempted(err) || result != "" {
				t.Fatalf("Execute() result=%q err=%v, want attempted execution error=%t", result, err, tc.execution)
			}
		})
	}
}

func TestToolHTTPRequestGatewayFailureIsNotToolExecutionError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("private gateway diagnostic"))
	}))
	defer server.Close()
	request, err := http.NewRequest(http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executeToolHTTPRequest(server.Client(), request, true)
	var executionErr ToolExecutionError
	if err == nil || errors.As(err, &executionErr) || err.Error() != "gateway returned HTTP 503" {
		t.Fatalf("gateway failure = %v, want redacted non-execution error", err)
	}
}

func TestToolExecutorTransportFailuresAreNotToolExecutionErrors(t *testing.T) {
	for _, name := range []string{"malformed response", "tool deadline", "cancelled request"} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if name == "malformed response" {
					w.Header().Set("Content-Length", "100")
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte("truncated"))
					return
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			executor := NewToolExecutorForNamespace("default", nil, server.Client())
			tool := &corev1alpha1.Tool{Spec: corev1alpha1.ToolSpec{HTTP: &corev1alpha1.HTTPExecution{URL: server.URL}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch name {
			case "tool deadline":
				tool.Spec.HTTP.Timeout = &metav1.Duration{Duration: 30 * time.Millisecond}
			case "cancelled request":
				cancel()
			}
			_, err := executor.Execute(ctx, tool, json.RawMessage(`{}`))
			var executionErr ToolExecutionError
			if err == nil || errors.As(err, &executionErr) || !ToolRequestWasAttempted(err) {
				t.Fatalf("Execute() error=%v, want attempted non-execution error", err)
			}
			if name == "tool deadline" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("tool timeout lost deadline error: %v", err)
			}
			if name == "cancelled request" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled request lost cancellation error: %v", err)
			}
		})
	}
}

func TestToolExecutorDistinguishesMCPExecutionAndProtocolErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		response  string
		execution bool
	}{
		{name: "tool error", response: `{"jsonrpc":"2.0","id":"1","result":{"isError":true,"content":[{"type":"text","text":"tool failed"}]}}`, execution: true},
		{name: "protocol error", response: `{"jsonrpc":"2.0","id":"1","error":{"code":-32602,"message":"invalid parameters"}}`},
		{name: "wrong response id", response: `{"jsonrpc":"2.0","id":"other","result":{"isError":true}}`},
		{name: "wrong protocol", response: `{"jsonrpc":"wrong","id":"1","result":{"isError":true,"content":[]}}`},
		{name: "missing content", response: `{"jsonrpc":"2.0","id":"1","result":{"isError":true}}`},
		{name: "malformed response", response: `{"jsonrpc":"2.0","id":"1","result":`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Method string `json:"method"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch request.Method {
				case mcpInitializeMethod:
					_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"initialize","result":{"protocolVersion":"2025-06-18"}}`))
				case mcpInitializedNotificationMethod:
					w.WriteHeader(http.StatusAccepted)
				case mcpToolsCallMethod:
					_, _ = w.Write([]byte(tc.response))
				default:
					t.Errorf("unexpected MCP method %q", request.Method)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			executor := NewToolExecutorForNamespace("default", nil, server.Client())
			tool := &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "lookup"},
				Spec: corev1alpha1.ToolSpec{MCP: &corev1alpha1.MCPToolServer{
					SubstrateActor: &corev1alpha1.SubstrateMCPActor{TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "fixture"}},
				}},
				Status: corev1alpha1.ToolStatus{Endpoint: server.URL, Actor: &corev1alpha1.ToolActorStatus{RouteHost: "fixture.actors.test"}},
			}
			_, err := executor.Execute(context.Background(), tool, json.RawMessage(`{}`))
			var executionErr ToolExecutionError
			if err == nil || errors.As(err, &executionErr) != tc.execution || !ToolRequestWasAttempted(err) {
				t.Fatalf("Execute() error=%v, want attempted execution error=%t", err, tc.execution)
			}
		})
	}
}
