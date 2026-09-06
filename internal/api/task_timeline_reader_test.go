package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/orka-agents/orka/internal/events"
	"github.com/orka-agents/orka/internal/store"
	storetest "github.com/orka-agents/orka/internal/store/storetest"
)

const (
	taskTimelineTestTaskName      = "task-a"
	taskTimelineToolResultSummary = "tool result"
	taskTimelinePlanSummary       = "plan"
)

func TestTaskTimelineReaderListMatchingReturnsEmptySlice(t *testing.T) {
	reader := newTaskTimelineReader(storetest.NewFakeExecutionEventStore(), defaultNamespace, taskTimelineTestTaskName)
	listed, err := reader.listMatching(context.Background(), []string{events.ExecutionEventTypeApprovalRequested})
	if err != nil {
		t.Fatalf("listMatching error = %v", err)
	}
	if listed == nil || len(listed) != 0 {
		t.Fatalf("listMatching = %#v, want non-nil empty slice", listed)
	}
}

func TestTaskTimelineReaderListThroughAllowsExactLimitAndRejectsNext(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	appendReaderEvents(t, eventStore, events.ExecutionEventTypeModelMessage, 3)
	reader := newTaskTimelineReader(eventStore, defaultNamespace, taskTimelineTestTaskName)

	listed, err := reader.listThrough(ctx, 3, 3)
	if err != nil {
		t.Fatalf("listThrough exact limit error = %v", err)
	}
	if len(listed) != 3 {
		t.Fatalf("listThrough exact limit returned %d events, want 3", len(listed))
	}
	_, err = reader.listThrough(ctx, 3, 2)
	if !errors.Is(err, errTaskTimelineReadLimitExceeded) {
		t.Fatalf("listThrough over limit error = %v, want limit exceeded", err)
	}
}

func TestTaskTimelineReaderListRecentContextThroughCoalescesAssistantChunksBeforeRetention(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	appendReaderEvent(t, eventStore, store.ExecutionEvent{Type: events.ExecutionEventTypeToolCallCompleted, Summary: taskTimelineToolResultSummary})
	content := json.RawMessage(`{"harnessV2":{"taskUID":"task-uid","taskAttempt":1,"promptID":"prompt-1","sequence":2}}`)
	for range 205 {
		appendReaderEvent(t, eventStore, store.ExecutionEvent{
			Type: events.ExecutionEventTypeModelMessage, Content: content, ContentText: "x",
		})
	}
	appendReaderEvent(t, eventStore, store.ExecutionEvent{Type: events.ExecutionEventTypePlanUpdated, Summary: taskTimelinePlanSummary})
	reader := newTaskTimelineReader(eventStore, defaultNamespace, taskTimelineTestTaskName)

	listed, scanTruncated, err := reader.listRecentContextThrough(ctx, 207, 3)
	if err != nil {
		t.Fatalf("listRecentContextThrough error = %v", err)
	}
	if scanTruncated {
		t.Fatal("listRecentContextThrough scanTruncated = true, want false")
	}
	if len(listed) != 3 || listed[0].Type != events.ExecutionEventTypeToolCallCompleted ||
		listed[1].Type != events.ExecutionEventTypeModelMessage || listed[1].Seq != 206 ||
		listed[2].Type != events.ExecutionEventTypePlanUpdated {
		t.Fatalf("listRecentContextThrough = %#v", listed)
	}
	if listed[1].ContentText != strings.Repeat("x", 205) {
		t.Fatalf("coalesced assistant length = %d, want 205", len(listed[1].ContentText))
	}
}

func TestTaskTimelineReaderListRecentContextThroughBoundsCompatibilityScan(t *testing.T) {
	ctx := context.Background()
	base := storetest.NewFakeExecutionEventStore()
	appendReaderEvent(t, base, store.ExecutionEvent{
		Seq: 10_000, Type: events.ExecutionEventTypeToolCallCompleted, Summary: taskTimelineToolResultSummary,
	})
	appendReaderEvent(t, base, store.ExecutionEvent{Type: events.ExecutionEventTypePlanUpdated, Summary: taskTimelinePlanSummary})
	eventStore := &recordingExecutionEventStore{ExecutionEventStore: base}
	reader := newTaskTimelineReader(eventStore, defaultNamespace, taskTimelineTestTaskName)

	listed, scanTruncated, err := reader.listRecentContextThrough(ctx, 10_001, 3)
	if err != nil {
		t.Fatalf("listRecentContextThrough error = %v", err)
	}
	if scanTruncated {
		t.Fatal("bounded compatibility scan marked sparse sequence as truncated")
	}
	if len(listed) != 2 || len(eventStore.filters) == 0 {
		t.Fatalf("bounded context events = %#v filters=%#v", listed, eventStore.filters)
	}
	wantAfter := int64(10_001 - taskTimelineContextCompatibilityScanLimit)
	if eventStore.filters[0].AfterSeq != wantAfter {
		t.Fatalf("first scan cursor = %d, want %d", eventStore.filters[0].AfterSeq, wantAfter)
	}
}

func TestTaskTimelineReaderListRecentContextThroughMarksCompatibilityScanTruncated(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	appendReaderEvent(t, eventStore, store.ExecutionEvent{
		Type: events.ExecutionEventTypeToolCallCompleted, Summary: taskTimelineToolResultSummary,
	})
	content := json.RawMessage(`{"harnessV2":{"taskUID":"task-uid","taskAttempt":1,"promptID":"prompt-1"}}`)
	for range taskTimelineContextCompatibilityScanLimit + 1 {
		appendReaderEvent(t, eventStore, store.ExecutionEvent{
			Type: events.ExecutionEventTypeModelMessage, Content: content, ContentText: "x",
		})
	}
	reader := newTaskTimelineReader(eventStore, defaultNamespace, taskTimelineTestTaskName)

	listed, scanTruncated, err := reader.listRecentContextThrough(
		ctx, int64(taskTimelineContextCompatibilityScanLimit+2), 3,
	)
	if err != nil {
		t.Fatalf("listRecentContextThrough error = %v", err)
	}
	if !scanTruncated || len(listed) != 0 {
		t.Fatalf("bounded coalesced context = %#v scanTruncated=%t", listed, scanTruncated)
	}
}

func TestTaskTimelineReaderListRecentContextThroughKeepsCompleteBoundaryMessage(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	for range taskTimelineContextCompatibilityScanLimit - 1 {
		appendReaderEvent(t, eventStore, store.ExecutionEvent{
			Type: events.ExecutionEventTypeToolCallCompleted, Summary: "older tool result",
		})
	}
	appendReaderEvent(t, eventStore, store.ExecutionEvent{
		Type: events.ExecutionEventTypePlanUpdated, Summary: "boundary plan",
	})
	content := json.RawMessage(`{"harnessV2":{"taskUID":"task-uid","taskAttempt":1,"promptID":"prompt-1"}}`)
	for range taskTimelineContextCompatibilityScanLimit {
		appendReaderEvent(t, eventStore, store.ExecutionEvent{
			Type: events.ExecutionEventTypeModelMessage, Content: content, ContentText: "x",
		})
	}
	reader := newTaskTimelineReader(eventStore, defaultNamespace, taskTimelineTestTaskName)

	listed, scanTruncated, err := reader.listRecentContextThrough(
		ctx, int64(taskTimelineContextCompatibilityScanLimit*2), 3,
	)
	if err != nil {
		t.Fatalf("listRecentContextThrough error = %v", err)
	}
	if !scanTruncated || len(listed) != 1 || listed[0].Type != events.ExecutionEventTypeModelMessage ||
		listed[0].ContentText != strings.Repeat("x", taskTimelineContextCompatibilityScanLimit) {
		t.Fatalf("complete boundary context = %#v scanTruncated=%t", listed, scanTruncated)
	}
}

func TestTaskTimelineReaderSeqExistsValidatesCheckpointRanges(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	appendReaderEvents(t, eventStore, events.ExecutionEventTypeModelMessage, 2)
	reader := newTaskTimelineReader(eventStore, defaultNamespace, taskTimelineTestTaskName)

	for _, seq := range []int64{0, 2} {
		ok, err := reader.seqExists(ctx, seq, 2)
		if err != nil || !ok {
			t.Fatalf("seqExists(%d, latest=2) = %t, %v; want true nil", seq, ok, err)
		}
	}
	for _, seq := range []int64{-1, 3} {
		ok, err := reader.seqExists(ctx, seq, 2)
		if err != nil || ok {
			t.Fatalf("seqExists(%d, latest=2) = %t, %v; want false nil", seq, ok, err)
		}
	}
	ok, err := reader.seqExists(ctx, 1, 2)
	if err != nil || !ok {
		t.Fatalf("seqExists(existing seq) = %t, %v; want true nil", ok, err)
	}
}

func TestTaskTimelineReaderTerminalForCompletionScansPastFilteredCursor(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	appendReaderEvents(t, eventStore, events.ExecutionEventTypeToolCallCompleted, 1)
	appendReaderEvents(t, eventStore, events.ExecutionEventTypeTaskSucceeded, 1)
	appendReaderEvents(t, eventStore, events.ExecutionEventTypeToolCallCompleted, 1)
	reader := newTaskTimelineReader(eventStore, defaultNamespace, taskTimelineTestTaskName)

	terminal, found, scannedThrough, err := reader.terminalForCompletion(ctx, 1)
	if err != nil {
		t.Fatalf("terminalForCompletion error = %v", err)
	}
	if !found || terminal.Type != events.ExecutionEventTypeTaskSucceeded || terminal.Seq != 2 {
		t.Fatalf("terminalForCompletion terminal = %#v found=%t, want TaskSucceeded seq 2", terminal, found)
	}
	if scannedThrough != 2 {
		t.Fatalf("scannedThrough = %d, want 2", scannedThrough)
	}
}

func appendReaderEvents(t *testing.T, eventStore store.ExecutionEventStore, eventType string, count int) {
	t.Helper()
	for range count {
		if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{Namespace: defaultNamespace, StreamType: store.ExecutionEventStreamTypeTask, StreamID: taskTimelineTestTaskName, Type: eventType}); err != nil {
			t.Fatalf("AppendExecutionEvent: %v", err)
		}
	}
}

func appendReaderEvent(t *testing.T, eventStore store.ExecutionEventStore, event store.ExecutionEvent) {
	t.Helper()
	event.Namespace = defaultNamespace
	event.StreamType = store.ExecutionEventStreamTypeTask
	event.StreamID = taskTimelineTestTaskName
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &event); err != nil {
		t.Fatalf("AppendExecutionEvent: %v", err)
	}
}

type recordingExecutionEventStore struct {
	store.ExecutionEventStore
	filters []store.ExecutionEventFilter
}

func (s *recordingExecutionEventStore) ListExecutionEvents(
	ctx context.Context,
	filter store.ExecutionEventFilter,
) ([]store.ExecutionEvent, error) {
	s.filters = append(s.filters, filter)
	return s.ExecutionEventStore.ListExecutionEvents(ctx, filter)
}
