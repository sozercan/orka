package supervisor

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

func TestAssistantMessageCompactorKeepsCodexBurstBelowHarnessRateLimit(t *testing.T) {
	compactor := newAssistantMessageCompactor()
	compactor.flushInterval = time.Hour
	t.Cleanup(compactor.close)
	now := time.Now().UTC()
	const rawUpdates = runtimeMaxUpdateEventsPerSecond + 1

	var compacted []acp.PromptEvent
	for sequence := 1; sequence <= rawUpdates; sequence++ {
		compacted = append(compacted, compactor.push(
			testAssistantMessagePromptEvent(t, int64(sequence), now, "x"), now,
		)...)
	}
	compacted = append(compacted, compactor.flushPending()...)
	if len(compacted) != 1 {
		t.Fatalf("compacted event count = %d, want 1", len(compacted))
	}

	server, cfg, _ := newTestServer(t, "immediate")
	server.cfg.Capabilities.Limits.MaxUpdateEventsPerSecond = 1
	fence := cfg.Fence
	fence.RuntimeSessionUID = "compactor-session-uid"
	fence.RuntimeSessionGeneration = 1
	prompt := &promptState{request: testStartPromptRequest(t, cfg, fence), sequence: 1}
	state := &sessionState{descriptor: harnessv2.RuntimeSessionDescriptor{
		RuntimeSessionUID: fence.RuntimeSessionUID,
		Generation:        fence.RuntimeSessionGeneration,
	}}
	mapped, err := server.mapRuntimeEvent(state, prompt, compacted[0])
	if err != nil {
		t.Fatalf("map compacted update: %v", err)
	}
	wantText := strings.Repeat("x", rawUpdates)
	if mapped == nil || mapped.Update == nil || mapped.Update.AssistantMessage == nil ||
		mapped.Update.AssistantMessage.Text != wantText || prompt.assistant.String() != wantText {
		t.Fatalf("compacted assistant text or terminal aggregate was not preserved")
	}

	limits := eventLimits(server.cfg.Capabilities.Limits)
	var stream bytes.Buffer
	encoder, err := harnessv2.NewEventEncoder(
		&stream, limits, harnessv2.EventExpectationFromMetadata(prompt.request.Metadata),
	)
	if err != nil {
		t.Fatal(err)
	}
	acceptedAt := now.Add(-time.Millisecond)
	accepted := harnessv2.Event{
		Protocol: harnessv2.ProtocolVersion,
		Type:     harnessv2.EventAccepted,
		Identity: eventIdentity(server.cfg.Fence, state.descriptor, prompt.request.Metadata, 1, acceptedAt),
		Accepted: &harnessv2.AcceptedEvent{
			AcceptedAt: acceptedAt,
			Lease:      prompt.request.Lease,
			ACPVersion: harnessv2.ACPProfileV1,
		},
	}
	if err := encoder.Encode(accepted); err != nil {
		t.Fatalf("encode accepted: %v", err)
	}
	if err := encoder.Encode(*mapped); err != nil {
		t.Fatalf("encode compacted update: %v", err)
	}
	terminal, _, err := server.terminalEvent(state, prompt, acp.PromptResult{
		Outcome: acp.PromptOutcomeCompleted, StopReason: acp.StopReasonEndTurn,
		Accepted: true, SettledAt: now.Add(time.Millisecond),
	})
	if err != nil {
		t.Fatalf("build terminal event: %v", err)
	}
	if err := encoder.Encode(terminal); err != nil {
		t.Fatalf("encode terminal event: %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("close event stream: %v", err)
	}

	decoder, err := harnessv2.NewEventDecoder(
		bytes.NewReader(stream.Bytes()), limits, harnessv2.EventExpectationFromMetadata(prompt.request.Metadata),
	)
	if err != nil {
		t.Fatal(err)
	}
	events, err := decoder.DecodeAll()
	if err != nil {
		t.Fatalf("decode compacted stream: %v", err)
	}
	if len(events) != 3 || events[0].Identity.Sequence != 1 || events[1].Identity.Sequence != 2 ||
		events[2].Identity.Sequence != 3 || events[2].Completed == nil ||
		events[2].Completed.Result.Content[0].Text != wantText {
		t.Fatalf("compacted harness stream = %#v", events)
	}
}

func TestAssistantMessageCompactorPreservesUTF8AndPermissionBoundary(t *testing.T) {
	compactor := &assistantMessageCompactor{maxBytes: 5, flushInterval: time.Hour}
	t.Cleanup(compactor.close)
	now := time.Now().UTC()
	inputs := []string{"a", " \n", "é", "🙂"}
	var ready []acp.PromptEvent
	for index, input := range inputs {
		at := now.Add(time.Duration(index) * time.Millisecond)
		ready = append(ready, compactor.push(testAssistantMessagePromptEvent(t, int64(index+1), at, input), at)...)
	}
	permission := acp.PromptEvent{
		Type:      acp.PromptEventPermissionRequested,
		Sequence:  5,
		Timestamp: now.Add(5 * time.Millisecond),
		Permission: &acp.PermissionRequestEvent{
			RequestID: "permission-1",
		},
	}
	ready = append(ready, compactor.push(permission, now.Add(5*time.Millisecond))...)
	if len(ready) != 3 || ready[2].Type != acp.PromptEventPermissionRequested {
		t.Fatalf("boundary output = %#v", ready)
	}
	firstChunk, firstOK := decodeAssistantMessageChunk(ready[0])
	secondChunk, secondOK := decodeAssistantMessageChunk(ready[1])
	first, second := firstChunk.Content.Text, secondChunk.Content.Text
	if !firstOK || !secondOK || first != "a \né" || second != "🙂" {
		t.Fatalf("UTF-8 chunks = %q/%q, want exact source text", first, second)
	}
	if ready[0].Timestamp != now.Add(2*time.Millisecond) || ready[1].Timestamp != now.Add(3*time.Millisecond) {
		t.Fatalf("compacted timestamps = %s/%s, want latest included source timestamps", ready[0].Timestamp, ready[1].Timestamp)
	}
}

func TestAssistantMessageCompactorFlushesOnCadence(t *testing.T) {
	const message = "hello"
	compactor := &assistantMessageCompactor{maxBytes: harnessv2.MaxProtocolStringBytes, flushInterval: time.Millisecond}
	t.Cleanup(compactor.close)
	now := time.Now().UTC()
	if ready := compactor.push(testAssistantMessagePromptEvent(t, 1, now, message), now); len(ready) != 0 {
		t.Fatalf("initial push emitted %d events", len(ready))
	}
	select {
	case <-compactor.timerChannel():
	case <-time.After(time.Second):
		t.Fatal("assistant compaction cadence did not fire")
	}
	ready := compactor.flushPending()
	if len(ready) != 1 {
		t.Fatalf("cadence flush count = %d, want 1", len(ready))
	}
	chunk, ok := decodeAssistantMessageChunk(ready[0])
	if text := chunk.Content.Text; !ok || text != message {
		t.Fatalf("cadence flush text = %q ok=%v", text, ok)
	}
}

func TestAssistantMessageCompactorPreservesMessageIdentityAndCodexPhaseBoundary(t *testing.T) {
	compactor := newAssistantMessageCompactor()
	compactor.flushInterval = time.Hour
	t.Cleanup(compactor.close)
	now := time.Now().UTC()

	commentary := testAssistantMessagePromptEventWithPhase(
		t, 1, now, "commentary-message", acpAssistantPhaseCommentary, "Checking repository files.",
	)
	if ready := compactor.push(commentary, now); len(ready) != 0 {
		t.Fatalf("commentary push emitted %d events", len(ready))
	}
	firstFinal := testAssistantMessagePromptEventWithPhase(
		t, 2, now.Add(time.Millisecond), "final-message", acpAssistantPhaseFinalAnswer, `{"schemaVersion":1,`,
	)
	ready := compactor.push(firstFinal, now.Add(time.Millisecond))
	if len(ready) != 1 {
		t.Fatalf("phase boundary emitted %d events, want 1", len(ready))
	}
	secondFinal := testAssistantMessagePromptEventWithPhase(
		t, 3, now.Add(2*time.Millisecond), "final-message", acpAssistantPhaseFinalAnswer, `"ok":true}`,
	)
	if extra := compactor.push(secondFinal, now.Add(2*time.Millisecond)); len(extra) != 0 {
		t.Fatalf("same final-answer message emitted %d events", len(extra))
	}
	ready = append(ready, compactor.flushPending()...)
	if len(ready) != 2 {
		t.Fatalf("compacted phase stream count = %d, want 2", len(ready))
	}

	commentaryChunk, ok := decodeAssistantMessageChunk(ready[0])
	if !ok || commentaryChunk.MessageID != "commentary-message" ||
		commentaryChunk.Content.Text != "Checking repository files." ||
		acpAssistantMessagePhase(ready[0].Update) != acpAssistantPhaseCommentary {
		t.Fatalf("compacted commentary chunk = %#v", commentaryChunk)
	}
	finalChunk, ok := decodeAssistantMessageChunk(ready[1])
	if !ok || finalChunk.MessageID != "final-message" ||
		finalChunk.Content.Text != `{"schemaVersion":1,"ok":true}` ||
		acpAssistantMessagePhase(ready[1].Update) != acpAssistantPhaseFinalAnswer {
		t.Fatalf("compacted final chunk = %#v", finalChunk)
	}
}

func testAssistantMessagePromptEvent(t *testing.T, sequence int64, at time.Time, text string) acp.PromptEvent {
	return testAssistantMessagePromptEventWithPhase(t, sequence, at, "", "", text)
}

func testAssistantMessagePromptEventWithPhase(
	t *testing.T,
	sequence int64,
	at time.Time,
	messageID string,
	phase string,
	text string,
) acp.PromptEvent {
	t.Helper()
	payload := map[string]any{
		"sessionUpdate": "agent_message_chunk",
		"content":       map[string]any{"type": "text", "text": text},
	}
	if messageID != "" {
		payload["messageId"] = messageID
	}
	if phase != "" {
		payload["_meta"] = map[string]any{"codex": map[string]any{"phase": phase}}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return acp.PromptEvent{
		Type:      acp.PromptEventUpdate,
		Sequence:  sequence,
		Timestamp: at,
		Update:    &acp.SessionNotification{SessionID: "provider-session", Update: encoded},
	}
}
