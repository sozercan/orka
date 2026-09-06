package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

const testOptionsMetaKey = "options"

func TestRuntimeSessionPromptPermissionAndTombstone(t *testing.T) {
	session := newTestRuntimeSession(t, "permission")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, err := session.StartPrompt(ctx, "prompt-1", "sha256:one", []ContentBlock{Text("hello")})
	if err != nil {
		t.Fatal(err)
	}
	var sawAccepted, sawUpdate, sawPermission bool
	for event := range run.Events {
		switch event.Type {
		case PromptEventAccepted:
			sawAccepted = true
		case PromptEventUpdate:
			sawUpdate = true
		case PromptEventPermissionRequested:
			sawPermission = true
			if event.Permission == nil || event.Permission.RequestID != "50" {
				t.Fatalf("unexpected permission event: %#v", event.Permission)
			}
			if err := session.ResolvePermission("prompt-1", event.Permission.RequestID, SelectedPermissionOutcome("allow")); err != nil {
				t.Fatal(err)
			}
		}
	}
	result := <-run.Result
	if !sawAccepted || !sawUpdate || !sawPermission {
		t.Fatalf("events accepted=%v update=%v permission=%v", sawAccepted, sawUpdate, sawPermission)
	}
	if result.Outcome != PromptOutcomeCompleted || result.StopReason != StopReasonEndTurn || !result.Accepted {
		t.Fatalf("unexpected result: %#v", result)
	}
	if _, err := session.StartPrompt(ctx, "prompt-1", "sha256:one", []ContentBlock{Text("again")}); err == nil {
		t.Fatal("settled prompt duplicate unexpectedly started")
	} else if duplicate, ok := err.(*DuplicatePromptError); !ok || duplicate.Active || duplicate.Result == nil {
		t.Fatalf("duplicate error = %#v", err)
	}
	if _, err := session.StartPrompt(ctx, "prompt-1", "sha256:different", []ContentBlock{Text("again")}); err == nil {
		t.Fatal("digest conflict unexpectedly started")
	} else if _, ok := err.(*DigestConflictError); !ok {
		t.Fatalf("conflict error = %#v", err)
	}
}

func TestRuntimeSessionCancelBarrier(t *testing.T) {
	session := newTestRuntimeSession(t, "wait")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := session.StartPrompt(ctx, "prompt-cancel", "sha256:cancel", []ContentBlock{Text("wait")})
	if err != nil {
		t.Fatal(err)
	}
	for event := range run.Events {
		if event.Type == PromptEventAccepted {
			break
		}
	}
	result, err := session.CancelPrompt(ctx, "prompt-cancel")
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != PromptOutcomeCancelled || result.StopReason != StopReasonCancelled {
		t.Fatalf("cancel result = %#v", result)
	}
	for range run.Events {
	}
	<-run.Result
	if err := session.ResolvePermission("prompt-cancel", "50", CancelledPermissionOutcome()); err == nil {
		t.Fatal("late permission unexpectedly accepted")
	} else if _, ok := err.(*StalePromptError); !ok {
		t.Fatalf("late permission error = %#v", err)
	}
}

func TestRuntimeSessionLeaseRenewalRejectsSettledPrompt(t *testing.T) {
	session := newTestRuntimeSession(t, "immediate")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := session.StartPrompt(ctx, "prompt-done", "sha256:done", []ContentBlock{Text("done")})
	if err != nil {
		t.Fatal(err)
	}
	for range run.Events {
	}
	<-run.Result
	if err := session.RenewPromptLease("prompt-done"); err == nil {
		t.Fatal("settled prompt lease unexpectedly renewed")
	}
}

func TestRuntimeSessionCancelReturnsSettledPromptResult(t *testing.T) {
	session := newTestRuntimeSession(t, "immediate")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := session.StartPrompt(ctx, "prompt-done", "sha256:done", []ContentBlock{Text("done")})
	if err != nil {
		t.Fatal(err)
	}
	for range run.Events {
	}
	want := <-run.Result
	got, err := session.CancelPrompt(ctx, "prompt-done")
	if err != nil {
		t.Fatalf("cancel settled prompt: %v", err)
	}
	if got.Outcome != want.Outcome || got.StopReason != want.StopReason || got.Accepted != want.Accepted {
		t.Fatalf("settled result = %#v, want %#v", got, want)
	}
}

func TestRuntimeSessionExplicitRPCErrorAfterAcceptanceIsFailed(t *testing.T) {
	session := newTestRuntimeSession(t, "rpc-error")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, err := session.StartPrompt(ctx, "prompt-rpc-error", "sha256:rpc-error", []ContentBlock{Text("fail")})
	if err != nil {
		t.Fatal(err)
	}
	var sawAccepted bool
	for event := range run.Events {
		sawAccepted = sawAccepted || event.Type == PromptEventAccepted
	}
	result := <-run.Result
	if !sawAccepted || !result.Accepted {
		t.Fatalf("prompt was not accepted: event=%v result=%#v", sawAccepted, result)
	}
	if result.Outcome != PromptOutcomeFailed {
		t.Fatalf("explicit RPC error outcome = %q, want %q", result.Outcome, PromptOutcomeFailed)
	}
	var rpcErr *RPCError
	if !errors.As(result.Err, &rpcErr) || rpcErr.Code != -32603 {
		t.Fatalf("result error = %#v, want RPC error -32603", result.Err)
	}
}

func TestRuntimeSessionTransportClosureAfterAcceptanceIsOutcomeUnknown(t *testing.T) {
	session := newTestRuntimeSession(t, "transport-close")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, err := session.StartPrompt(ctx, "prompt-transport-close", "sha256:transport-close", []ContentBlock{Text("disconnect")})
	if err != nil {
		t.Fatal(err)
	}
	var sawAccepted bool
	for event := range run.Events {
		sawAccepted = sawAccepted || event.Type == PromptEventAccepted
	}
	result := <-run.Result
	if !sawAccepted || !result.Accepted {
		t.Fatalf("prompt was not accepted: event=%v result=%#v", sawAccepted, result)
	}
	if result.Outcome != PromptOutcomeOutcomeUnknown {
		t.Fatalf("transport closure outcome = %q, want %q", result.Outcome, PromptOutcomeOutcomeUnknown)
	}
	if !errors.Is(result.Err, io.EOF) {
		t.Fatalf("result error = %#v, want EOF", result.Err)
	}
}

func TestClassifyPromptErrorOutcome(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		written bool
		want    PromptOutcome
	}{
		{
			name:    "explicit RPC error after write",
			err:     &RPCError{Code: -32603, Message: "adapter failed"},
			written: true,
			want:    PromptOutcomeFailed,
		},
		{
			name:    "wrapped explicit RPC error after write",
			err:     fmt.Errorf("prompt failed: %w", &RPCError{Code: -32603, Message: "adapter failed"}),
			written: true,
			want:    PromptOutcomeFailed,
		},
		{
			name:    "transport closure after write",
			err:     io.EOF,
			written: true,
			want:    PromptOutcomeOutcomeUnknown,
		},
		{
			name:    "deadline after write",
			err:     context.DeadlineExceeded,
			written: true,
			want:    PromptOutcomeOutcomeUnknown,
		},
		{
			name:    "failure before write",
			err:     context.DeadlineExceeded,
			written: false,
			want:    PromptOutcomeFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyPromptErrorOutcome(tt.err, tt.written); got != tt.want {
				t.Fatalf("classifyPromptErrorOutcome(%v, %v) = %q, want %q", tt.err, tt.written, got, tt.want)
			}
		})
	}
}

func TestRuntimeSessionConcurrentDeleteSharesCleanupResult(t *testing.T) {
	session := newTestRuntimeSession(t, "wait")
	type deleteResult struct {
		status CleanupStatus
		err    error
	}
	start := make(chan struct{})
	results := make(chan deleteResult, 2)
	for range 2 {
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			status, err := session.Delete(ctx)
			results <- deleteResult{status: status, err: err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	if first.status.Proven != second.status.Proven || !reflect.DeepEqual(first.status.RemainingPIDs, second.status.RemainingPIDs) {
		t.Fatalf("concurrent delete statuses differ: %#v vs %#v", first.status, second.status)
	}
	if fmt.Sprint(first.err) != fmt.Sprint(second.err) {
		t.Fatalf("concurrent delete errors differ: %v vs %v", first.err, second.err)
	}
}

func TestRuntimeSessionProjectsProtectedNewSessionMetadata(t *testing.T) {
	session := newTestRuntimeSessionWithMeta(t, "metadata", Meta{
		"systemPrompt": "trusted instructions",
		"claudeCode": map[string]any{
			testOptionsMetaKey: map[string]any{"maxTurns": 7},
		},
	})
	if session.ProviderSessionID() != "provider-test-session" {
		t.Fatalf("provider session ID = %q", session.ProviderSessionID())
	}
}

func TestMergeNewSessionMetaRejectsProtectedNamespace(t *testing.T) {
	protected := Meta{sessionMetaRuntimeSessionID: "session"}
	for _, provider := range []Meta{
		{sessionMetaRuntimeSessionID: "replacement"},
		{"OrKa.shadow": true},
		{" ": true},
	} {
		if _, err := MergeNewSessionMeta(provider, protected); err == nil {
			t.Fatalf("provider metadata %#v unexpectedly accepted", provider)
		}
	}
	merged, err := MergeNewSessionMeta(Meta{"claudeCode": map[string]any{testOptionsMetaKey: map[string]any{"effort": "high"}}}, protected)
	if err != nil {
		t.Fatal(err)
	}
	if merged[sessionMetaRuntimeSessionID] != "session" || merged["claudeCode"] == nil {
		t.Fatalf("merged metadata = %#v", merged)
	}
}

func newTestRuntimeSession(t *testing.T, mode string) *RuntimeSession {
	return newTestRuntimeSessionWithMeta(t, mode, nil)
}

func newTestRuntimeSessionWithMeta(t *testing.T, mode string, newSessionMeta Meta) *RuntimeSession {
	t.Helper()
	uid, gid := os.Getuid(), os.Getgid()
	if uid == 0 {
		uid, gid = 65534, 65534
	}
	testRoot, err := os.MkdirTemp("", "orka-acp-session-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(testRoot) })
	if err := os.Chmod(testRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	paths, err := PrepareSessionPaths(filepath.Join(testRoot, "sessions"), "test-session")
	if err != nil {
		t.Fatal(err)
	}
	env, err := BuildChildEnvironment(paths, EnvironmentConfig{Values: map[string]string{
		"GO_WANT_ACP_HELPER": "1",
		"ACP_HELPER_MODE":    mode,
	}})
	if err != nil {
		t.Fatal(err)
	}
	command := testAdapterCommand(t)
	session, err := NewRuntimeSession(context.Background(), RuntimeSessionConfig{
		ID:            "test-session",
		Generation:    1,
		ProfileDigest: "sha256:profile",
		Process: ProcessConfig{
			Command:           command,
			Args:              []string{"-test.run=TestACPHelperProcess"},
			Environment:       env,
			Paths:             paths,
			UID:               uid,
			GID:               gid,
			ExecHelperCommand: testExecHelperCommand(t),
		},
		NewSessionMeta:    newSessionMeta,
		PromptLease:       5 * time.Second,
		PermissionTimeout: 5 * time.Second,
		CancelGrace:       2 * time.Second,
		MaxBufferedEvents: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = session.Delete(ctx)
	})
	return session
}

func TestACPHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_ACP_HELPER") != "1" {
		return
	}
	reader := bufio.NewReader(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	defer func() { _ = writer.Flush() }()
	mode := os.Getenv("ACP_HELPER_MODE")
	var promptID json.RawMessage
	var providerSession = "provider-test-session"
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			os.Exit(0)
		}
		var message rpcMessage
		if err := json.Unmarshal(line, &message); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		switch message.Method {
		case MethodInitialize:
			writeACPHelper(writer, map[string]any{
				"jsonrpc": "2.0", "id": rawIDValue(message.ID),
				"result": map[string]any{"protocolVersion": ProtocolVersion, "agentInfo": map[string]any{"name": "fake", "version": "1"}},
			})
		case MethodSessionNew:
			if mode == "metadata" {
				var request NewSessionRequest
				if err := json.Unmarshal(message.Params, &request); err != nil || !validTestNewSessionMetadata(request.Meta) {
					writeACPHelper(writer, map[string]any{
						"jsonrpc": "2.0", "id": rawIDValue(message.ID),
						"error": map[string]any{"code": -32602, "message": "unexpected session metadata"},
					})
					continue
				}
			}
			writeACPHelper(writer, map[string]any{"jsonrpc": "2.0", "id": rawIDValue(message.ID), "result": map[string]any{"sessionId": providerSession}})
		case MethodSessionPrompt:
			promptID = append(promptID[:0], message.ID...)
			writeACPHelper(writer, map[string]any{
				"jsonrpc": "2.0", "method": MethodSessionUpdate,
				"params": map[string]any{"sessionId": providerSession, "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "working"}}},
			})
			switch mode {
			case "permission":
				writeACPHelper(writer, map[string]any{
					"jsonrpc": "2.0", "id": 50, "method": MethodRequestPermission,
					"params": map[string]any{"sessionId": providerSession, "toolCall": map[string]any{"toolCallId": "tool-1", "title": "test"}, "options": []map[string]any{{"optionId": "allow", "name": "Allow", "kind": "allow_once"}}},
				})
			case "immediate":
				writeACPHelper(writer, map[string]any{"jsonrpc": "2.0", "id": rawIDValue(promptID), "result": map[string]any{"stopReason": StopReasonEndTurn}})
			case "rpc-error":
				writeACPHelper(writer, map[string]any{
					"jsonrpc": "2.0", "id": rawIDValue(promptID),
					"error": map[string]any{"code": -32603, "message": "adapter failed"},
				})
			case "transport-close":
				os.Exit(0)
			}
		case MethodSessionCancel:
			if len(promptID) > 0 {
				writeACPHelper(writer, map[string]any{"jsonrpc": "2.0", "id": rawIDValue(promptID), "result": map[string]any{"stopReason": StopReasonCancelled}})
				promptID = nil
			}
		default:
			if len(message.ID) > 0 && message.Method == "" && string(message.ID) == "50" && len(promptID) > 0 {
				writeACPHelper(writer, map[string]any{"jsonrpc": "2.0", "id": rawIDValue(promptID), "result": map[string]any{"stopReason": StopReasonEndTurn}})
				promptID = nil
			}
		}
	}
}

func validTestNewSessionMetadata(meta Meta) bool {
	if meta["systemPrompt"] != "trusted instructions" || meta[sessionMetaRuntimeSessionID] != "test-session" ||
		meta[sessionMetaGeneration] != float64(1) || meta[sessionMetaRuntimeProfileDigest] != "sha256:profile" {
		return false
	}
	claudeCode, ok := meta["claudeCode"].(map[string]any)
	if !ok {
		return false
	}
	options, ok := claudeCode[testOptionsMetaKey].(map[string]any)
	return ok && options["maxTurns"] == float64(7)
}

func writeACPHelper(writer *bufio.Writer, value any) {
	data, _ := json.Marshal(value)
	_, _ = writer.Write(append(data, '\n'))
	_ = writer.Flush()
}

func rawIDValue(raw json.RawMessage) any {
	if value, err := strconv.ParseInt(string(raw), 10, 64); err == nil {
		return value
	}
	var value any
	_ = json.Unmarshal(raw, &value)
	return value
}

func TestHandleRequestChargesPermissionEventsToByteBudget(t *testing.T) {
	session := &RuntimeSession{config: RuntimeSessionConfig{MaxBufferedEvents: 16, MaxBufferedEventBytes: 1 << 20, PermissionTimeout: time.Second, CancelGrace: 50 * time.Millisecond}, providerSessionID: "sess-1"}
	active := &activePrompt{id: "prompt-perm", events: make(chan PromptEvent, 16), accepted: true, done: make(chan struct{}), permissions: map[string]*pendingPermission{}}
	session.active = active
	params := json.RawMessage(`{"sessionId":"sess-1","toolCall":{"title":"` + strings.Repeat("x", 4096) + `"},"options":[{"optionId":"allow","name":"Allow","kind":"allow_once"}]}`)
	go func() {
		_, _ = session.handleRequest(context.Background(), IncomingRequest{ID: json.RawMessage(`"req-1"`), Method: MethodRequestPermission, Params: params})
	}()
	select {
	case event := <-active.events:
		if event.Type != PromptEventPermissionRequested || event.Size != len(params) {
			t.Fatalf("event = %+v, want a permission event sized %d", event, len(params))
		}
		session.mu.Lock()
		buffered := active.bufferedBytes
		var pending *pendingPermission
		for _, candidate := range active.permissions {
			pending = candidate
		}
		session.mu.Unlock()
		if buffered != len(params) {
			t.Fatalf("bufferedBytes = %d, want %d", buffered, len(params))
		}
		if pending == nil {
			t.Fatal("permission request was not registered")
		}
		pending.result <- RequestPermissionOutcome{Outcome: "selected", OptionID: "allow"}
	case <-time.After(2 * time.Second):
		t.Fatal("permission event was not emitted")
	}
}

func TestEmitLockedEnforcesBufferedByteBudget(t *testing.T) {
	session := &RuntimeSession{config: RuntimeSessionConfig{MaxBufferedEvents: 16, MaxBufferedEventBytes: 100, CancelGrace: 50 * time.Millisecond}}
	active := &activePrompt{id: "prompt-bytes", events: make(chan PromptEvent, 16), accepted: true, done: make(chan struct{})}
	session.mu.Lock()
	session.emitLocked(active, PromptEvent{Type: PromptEventUpdate, Size: 60})
	session.emitLocked(active, PromptEvent{Type: PromptEventUpdate, Size: 30})
	overflowedEarly := active.overflowed
	session.emitLocked(active, PromptEvent{Type: PromptEventUpdate, Size: 20})
	overflowedLate := active.overflowed
	session.mu.Unlock()
	if overflowedEarly {
		t.Fatal("buffer overflowed below the byte budget")
	}
	if !overflowedLate {
		t.Fatal("buffer did not overflow once the byte budget was exceeded")
	}
	if got := len(active.events); got != 2 {
		t.Fatalf("buffered events = %d, want 2 (the over-budget event was dropped)", got)
	}
	session.releaseBufferedEvent(active, <-active.events)
	session.mu.Lock()
	remaining := active.bufferedBytes
	session.mu.Unlock()
	if remaining != 30 {
		t.Fatalf("bufferedBytes after release = %d, want 30", remaining)
	}
}

// A notification the child sends before the prompt request is marked
// written is parked until acceptance and enqueued afterwards; its receipt
// time must still be the instant it arrived, while the enqueue timestamp
// reflects when the consumer could first see it.
func TestRuntimeSessionPreAcceptedEventKeepsReceiptTime(t *testing.T) {
	session := &RuntimeSession{config: RuntimeSessionConfig{MaxBufferedEvents: 4, MaxBufferedEventBytes: 1 << 20}}
	active := &activePrompt{events: make(chan PromptEvent, 4)}
	before := time.Now().UTC()
	session.emitLocked(active, PromptEvent{Type: PromptEventUpdate, Update: &SessionNotification{SessionID: "s"}, Size: 1})
	if len(active.preAccepted) != 1 || len(active.events) != 0 {
		t.Fatalf("pre-acceptance update was not parked: parked=%d enqueued=%d", len(active.preAccepted), len(active.events))
	}
	received := active.preAccepted[0].ReceivedAt
	if received.IsZero() || received.Before(before) {
		t.Fatalf("parked event receipt time = %v, want stamped at receipt (>= %v)", received, before)
	}
	time.Sleep(2 * time.Millisecond)

	active.accepted = true
	session.emitLocked(active, PromptEvent{Type: PromptEventAccepted})
	queued := append([]PromptEvent(nil), active.preAccepted...)
	active.preAccepted = nil
	for _, event := range queued {
		active.bufferedBytes -= event.Size
		session.emitLocked(active, event)
	}
	accepted, update := <-active.events, <-active.events
	if accepted.Type != PromptEventAccepted || update.Type != PromptEventUpdate {
		t.Fatalf("enqueued order = %s, %s", accepted.Type, update.Type)
	}
	if !update.ReceivedAt.Equal(received) {
		t.Fatalf("enqueued receipt time = %v, want the original %v", update.ReceivedAt, received)
	}
	if !update.Timestamp.After(received.Add(time.Millisecond)) || update.Timestamp.Before(accepted.Timestamp) {
		t.Fatalf("enqueue timestamp = %v, want after receipt %v and not before acceptance %v", update.Timestamp, received, accepted.Timestamp)
	}
}
