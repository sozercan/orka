package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestClientInitializeNewSessionPrompt(t *testing.T) {
	clientConn, agentConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = agentConn.Close()
	})

	client := NewClient(clientConn, clientConn, Options{
		NotificationHandler: func(_ context.Context, notification IncomingNotification) {
			if notification.Method != MethodSessionUpdate {
				t.Errorf("notification method = %q, want %q", notification.Method, MethodSessionUpdate)
			}
		},
	})

	agentDone := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(agentConn)
		initialize, err := readTestMessage(reader)
		if err != nil {
			agentDone <- err
			return
		}
		if initialize.Method != MethodInitialize {
			agentDone <- errors.New("first request was not initialize")
			return
		}
		if err := writeTestMessage(agentConn, map[string]any{
			"jsonrpc": "2.0",
			"id":      initialize.ID,
			"result": map[string]any{
				"protocolVersion": ProtocolVersion,
				"agentInfo":       map[string]any{"name": "fake", "version": "1.0.0"},
			},
		}); err != nil {
			agentDone <- err
			return
		}

		newSession, err := readTestMessage(reader)
		if err != nil {
			agentDone <- err
			return
		}
		if newSession.Method != MethodSessionNew {
			agentDone <- errors.New("second request was not session/new")
			return
		}
		if err := writeTestMessage(agentConn, map[string]any{
			"jsonrpc": "2.0", "id": newSession.ID, "result": map[string]any{"sessionId": "provider-session"},
		}); err != nil {
			agentDone <- err
			return
		}

		prompt, err := readTestMessage(reader)
		if err != nil {
			agentDone <- err
			return
		}
		if prompt.Method != MethodSessionPrompt {
			agentDone <- errors.New("third request was not session/prompt")
			return
		}
		if err := writeTestMessage(agentConn, map[string]any{
			"jsonrpc": "2.0",
			"method":  MethodSessionUpdate,
			"params": map[string]any{
				"sessionId": "provider-session",
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content":       map[string]any{"type": "text", "text": "hello"},
				},
			},
		}); err != nil {
			agentDone <- err
			return
		}
		if err := writeTestMessage(agentConn, map[string]any{
			"jsonrpc": "2.0", "id": prompt.ID, "result": map[string]any{"stopReason": StopReasonEndTurn},
		}); err != nil {
			agentDone <- err
			return
		}
		agentDone <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	initialized, err := client.Initialize(ctx, InitializeRequest{
		ProtocolVersion: ProtocolVersion,
		ClientInfo:      &Implementation{Name: "orka", Version: "test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if initialized.AgentInfo == nil || initialized.AgentInfo.Name != "fake" {
		t.Fatalf("unexpected agent info: %#v", initialized.AgentInfo)
	}
	session, err := client.NewSession(ctx, NewSessionRequest{CWD: "/workspace", MCPServers: []MCPServer{}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Prompt(ctx, PromptRequest{SessionID: session.SessionID, Prompt: []ContentBlock{Text("work")}})
	if err != nil {
		t.Fatal(err)
	}
	if !response.StopReason.Successful() {
		t.Fatalf("stop reason = %q", response.StopReason)
	}
	if err := <-agentDone; err != nil {
		t.Fatal(err)
	}
}

func TestClientHandlesPermissionRequest(t *testing.T) {
	clientConn, agentConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = agentConn.Close()
	})

	client := NewClient(clientConn, clientConn, Options{
		RequestHandler: func(_ context.Context, request IncomingRequest) (any, *RPCError) {
			if request.Method != MethodRequestPermission {
				return nil, &RPCError{Code: -32601, Message: "unexpected method"}
			}
			var permission RequestPermissionRequest
			if err := json.Unmarshal(request.Params, &permission); err != nil {
				return nil, &RPCError{Code: -32602, Message: "invalid permission request"}
			}
			return RequestPermissionResponse{Outcome: SelectedPermissionOutcome(permission.Options[0].OptionID)}, nil
		},
	})
	_ = client

	if err := writeTestMessage(agentConn, map[string]any{
		"jsonrpc": "2.0",
		"id":      99,
		"method":  MethodRequestPermission,
		"params": map[string]any{
			"sessionId": "s1",
			"toolCall":  map[string]any{"toolCallId": "tc1", "title": "run"},
			"options":   []map[string]any{{"optionId": "allow", "name": "Allow", "kind": "allow_once"}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	message, err := readTestMessage(bufio.NewReader(agentConn))
	if err != nil {
		t.Fatal(err)
	}
	if string(message.ID) != "99" || message.Error != nil {
		t.Fatalf("unexpected response: %#v", message)
	}
	var response RequestPermissionResponse
	if err := json.Unmarshal(message.Result, &response); err != nil {
		t.Fatal(err)
	}
	if response.Outcome.Outcome != "selected" || response.Outcome.OptionID != "allow" {
		t.Fatalf("unexpected permission outcome: %#v", response.Outcome)
	}
}

func TestClientRejectsProtocolVersionMismatch(t *testing.T) {
	clientConn, agentConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = agentConn.Close()
	})
	client := NewClient(clientConn, clientConn, Options{})
	go func() {
		request, err := readTestMessage(bufio.NewReader(agentConn))
		if err == nil {
			_ = writeTestMessage(agentConn, map[string]any{
				"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"protocolVersion": 2},
			})
		}
	}()
	_, err := client.Initialize(context.Background(), InitializeRequest{ProtocolVersion: ProtocolVersion})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("Initialize error = %v, want unsupported version", err)
	}
}

func TestClientBoundsInboundMessage(t *testing.T) {
	clientConn, agentConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = agentConn.Close()
	})
	client := NewClient(clientConn, clientConn, Options{MaxMessageBytes: 64})
	go func() {
		_, _ = agentConn.Write([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"blob":"` + strings.Repeat("x", 200) + `"}}` + "\n"))
	}()
	select {
	case <-client.Done():
		if err := client.Err(); err == nil || !strings.Contains(err.Error(), "exceeds 64-byte limit") {
			t.Fatalf("client error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client did not fail oversized message")
	}
}

func TestClientContextCancellationSendsCancelRequest(t *testing.T) {
	clientConn, agentConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = agentConn.Close()
	})
	client := NewClient(clientConn, clientConn, Options{})

	gotCancel := make(chan rpcMessage, 1)
	go func() {
		reader := bufio.NewReader(agentConn)
		_, _ = readTestMessage(reader)
		message, err := readTestMessage(reader)
		if err == nil {
			gotCancel <- message
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.Call(ctx, "slow", map[string]any{"x": 1}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Call error = %v, want context canceled", err)
	}
	select {
	case message := <-gotCancel:
		if message.Method != MethodCancelRequest {
			t.Fatalf("method = %q, want %q", message.Method, MethodCancelRequest)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel request was not sent")
	}
}

func TestStopReasons(t *testing.T) {
	if err := StopReason("new_reason").Validate(); err == nil {
		t.Fatal("unknown stop reason unexpectedly accepted")
	}
	if StopReasonCancelled.Successful() {
		t.Fatal("cancelled stop reason must not be successful")
	}
}

func TestClientBoundsConcurrentRequests(t *testing.T) {
	clientConn, agentConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = agentConn.Close()
	})

	release := make(chan struct{})
	client := NewClient(clientConn, clientConn, Options{
		MaxConcurrentRequests: 2,
		RequestHandler: func(_ context.Context, _ IncomingRequest) (any, *RPCError) {
			<-release
			return map[string]any{"ok": true}, nil
		},
	})
	_ = client

	for id := 1; id <= 3; id++ {
		if err := writeTestMessage(agentConn, map[string]any{
			"jsonrpc": "2.0", "id": id, "method": MethodRequestPermission, "params": map[string]any{},
		}); err != nil {
			t.Fatal(err)
		}
	}

	reader := bufio.NewReader(agentConn)
	rejected, err := readTestMessage(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(rejected.ID) != "3" || rejected.Error == nil || rejected.Error.Code != -32000 {
		t.Fatalf("expected request 3 rejected with -32000 while the gate is full, got %#v", rejected)
	}

	close(release)
	seen := map[string]bool{}
	for range 2 {
		message, err := readTestMessage(reader)
		if err != nil {
			t.Fatal(err)
		}
		if message.Error != nil {
			t.Fatalf("gated request unexpectedly failed: %#v", message)
		}
		seen[string(message.ID)] = true
	}
	if !seen["1"] || !seen["2"] {
		t.Fatalf("gated requests 1 and 2 must complete after release, got %v", seen)
	}
}

type stalledWriter struct {
	release chan struct{}
}

func (w *stalledWriter) Write(p []byte) (int, error) {
	<-w.release
	return len(p), nil
}

func TestClientNotifyHonorsContextWhenTransportStalls(t *testing.T) {
	writer := &stalledWriter{release: make(chan struct{})}
	t.Cleanup(func() { close(writer.release) })
	reader, _ := net.Pipe()
	t.Cleanup(func() { _ = reader.Close() })

	client := NewClient(reader, writer, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := client.Notify(ctx, MethodSessionCancel, CancelNotification{SessionID: "s1"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Notify() error = %v, want context deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Notify() blocked %v on a stalled transport instead of honoring cancellation", elapsed)
	}
}

func readTestMessage(reader *bufio.Reader) (rpcMessage, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return rpcMessage{}, err
	}
	var message rpcMessage
	if err := json.Unmarshal(line, &message); err != nil {
		return rpcMessage{}, err
	}
	return message, nil
}

func writeTestMessage(conn net.Conn, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = conn.Write(data)
	return err
}
