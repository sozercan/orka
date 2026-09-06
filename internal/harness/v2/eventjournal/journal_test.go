package eventjournal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	executionevents "github.com/orka-agents/orka/internal/events"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
	"github.com/orka-agents/orka/internal/store/storetest"
)

const (
	testToolKindShell             = "shell"
	testJournalNamespace          = "default"
	testJournalTaskName           = "task-1"
	testJournalSecondTaskName     = "task-2"
	testJournalDone               = "done"
	testJournalToolTitle          = "Inspect repository"
	testJournalOpenToolCallID     = "call-open"
	testJournalMetadataToolCallID = "call-metadata"
	testJournalRuntimeCode        = "runtime"
	testJournalServedModel        = "served-model"
	testJournalPromptID           = "prompt-1"
	testJournalSecretPrefix       = "sk-"
)

func TestJournalDeduplicatesWithinPassAndAcrossRecovery(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	journal := Journal{EventStore: eventStore, MapContext: testMapContext()}
	state, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind:  harnessv2.UpdateUsage,
		Usage: &harnessv2.UsageUpdate{InputTokens: 10},
	})
	appended, isNew, err := state.AppendUpdateIfNew(ctx, event)
	if err != nil || !isNew || appended == nil {
		t.Fatalf("first append = %#v, new=%t, err=%v", appended, isNew, err)
	}
	if !state.HasUpdate(event) {
		t.Fatal("journal did not remember appended update")
	}
	if duplicate, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || isNew || duplicate != nil {
		t.Fatalf("same-pass duplicate = %#v, new=%t, err=%v", duplicate, isNew, err)
	}

	recovered, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !recovered.HasUpdate(event) {
		t.Fatal("recovered journal did not load persisted update")
	}
	if duplicate, isNew, err := recovered.AppendUpdateIfNew(ctx, event); err != nil || isNew || duplicate != nil {
		t.Fatalf("recovered duplicate = %#v, new=%t, err=%v", duplicate, isNew, err)
	}
	next := testUpdateEvent(3, event.Identity.Timestamp.Add(time.Millisecond), harnessv2.UpdateEvent{
		Kind:  harnessv2.UpdateUsage,
		Usage: &harnessv2.UsageUpdate{OutputTokens: 5},
	})
	if _, isNew, err := recovered.AppendUpdateIfNew(ctx, next); err != nil || !isNew {
		t.Fatalf("next append new=%t, err=%v", isNew, err)
	}

	listed, err := eventStore.ListExecutionEvents(ctx, store.ExecutionEventFilter{
		Namespace: testJournalNamespace, StreamType: store.ExecutionEventStreamTypeTask, StreamID: testJournalTaskName, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 {
		t.Fatalf("persisted events = %d, want 2", len(listed))
	}
}

func TestJournalPlanReplaySurvivesSQLiteReopen(t *testing.T) {
	const (
		firstPlanSummary  = "first"
		secondPlanSummary = testJournalDone
	)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "journal-reopen.db")
	db, err := sqlite.NewDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	firstStore := sqlite.NewStore(db, "journal-reopen-first")
	journal := Journal{EventStore: firstStore, MapContext: testMapContext()}
	state, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstEvent := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{
			{Content: firstPlanSummary, Status: harnessv2.PlanEntryInProgress},
		}},
	})
	firstPlan := &store.PlanState{
		Namespace: testJournalNamespace, TaskName: testJournalTaskName, Summary: firstPlanSummary,
		ProgressPct: 50, PlanDocument: "# First",
	}
	if appended, isNew, err := state.AppendPlanUpdateIfNew(ctx, firstEvent, firstPlan); err != nil || !isNew || appended == nil {
		t.Fatalf("first plan append = %#v new=%t err=%v", appended, isNew, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = sqlite.NewDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	reopenedStore := sqlite.NewStore(db, "journal-reopen-second")
	reopenedJournal := Journal{EventStore: reopenedStore, MapContext: testMapContext()}
	recovered, err := reopenedJournal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !recovered.HasUpdate(firstEvent) {
		t.Fatal("reopened journal did not recover the persisted plan update")
	}
	stalePlan := *firstPlan
	stalePlan.Summary = "stale replay"
	if duplicate, isNew, err := recovered.AppendPlanUpdateIfNew(ctx, firstEvent, &stalePlan); err != nil || isNew || duplicate != nil {
		t.Fatalf("reopened duplicate = %#v new=%t err=%v", duplicate, isNew, err)
	}
	persisted, err := reopenedStore.GetPlan(ctx, testJournalNamespace, testJournalTaskName)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Summary != firstPlan.Summary {
		t.Fatalf("replayed plan summary = %q, want %q", persisted.Summary, firstPlan.Summary)
	}

	secondEvent := testUpdateEvent(3, firstEvent.Identity.Timestamp.Add(time.Second), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{
			{Content: secondPlanSummary, Status: harnessv2.PlanEntryCompleted},
		}},
	})
	secondPlan := &store.PlanState{
		Namespace: testJournalNamespace, TaskName: testJournalTaskName, Summary: secondPlanSummary,
		ProgressPct: 100, GoalComplete: true, PlanDocument: "# Done",
	}
	if appended, isNew, err := recovered.AppendPlanUpdateIfNew(ctx, secondEvent, secondPlan); err != nil || !isNew || appended == nil || appended.Seq != 2 {
		t.Fatalf("post-reopen plan append = %#v new=%t err=%v", appended, isNew, err)
	}
	persisted, err = reopenedStore.GetPlan(ctx, testJournalNamespace, testJournalTaskName)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Summary != secondPlan.Summary || persisted.ProgressPct != 100 || !persisted.GoalComplete {
		t.Fatalf("post-reopen plan = %#v, want %#v", persisted, secondPlan)
	}
}

func TestJournalDeduplicatesAcrossConcurrentStates(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	journal := Journal{EventStore: eventStore, MapContext: testMapContext()}
	states := make([]*State, 2)
	for index := range states {
		state, err := journal.Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		states[index] = state
	}
	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateUsage, Usage: &harnessv2.UsageUpdate{InputTokens: 10},
	})

	type result struct {
		appended *store.ExecutionEvent
		isNew    bool
		err      error
	}
	results := make(chan result, len(states))
	var wg sync.WaitGroup
	for _, state := range states {
		wg.Go(func() {
			appended, isNew, err := state.AppendUpdateIfNew(ctx, event)
			results <- result{appended: appended, isNew: isNew, err: err}
		})
	}
	wg.Wait()
	close(results)

	newCount := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent journal append: %v", result.err)
		}
		if result.isNew {
			newCount++
			if result.appended == nil {
				t.Fatal("new concurrent append returned no event")
			}
		} else if result.appended != nil {
			t.Fatalf("deduplicated append returned %#v", result.appended)
		}
	}
	if newCount != 1 {
		t.Fatalf("new concurrent appends = %d, want 1", newCount)
	}
	if listed := listJournalEvents(t, ctx, eventStore); len(listed) != 1 {
		t.Fatalf("persisted events = %#v, want one", listed)
	}
}

func TestJournalDeduplicatesRealAndRecoveredToolTerminalOutcomes(t *testing.T) {
	for _, test := range []struct {
		name         string
		closureFirst bool
		wantType     string
	}{
		{
			name:         "real completion wins",
			closureFirst: false,
			wantType:     executionevents.ExecutionEventTypeToolCallCompleted,
		},
		{
			name:         "recovery closure wins",
			closureFirst: true,
			wantType:     executionevents.ExecutionEventTypeToolCallFailed,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			eventStore := storetest.NewFakeExecutionEventStore()
			journal := Journal{EventStore: eventStore, MapContext: testMapContext()}
			initial, err := journal.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}

			now := time.Now().UTC()
			started := testUpdateEvent(2, now, harnessv2.UpdateEvent{
				Kind: harnessv2.UpdateToolCall,
				ToolCall: &harnessv2.ToolCallUpdate{
					ToolCallID: "call-terminal-race", Status: harnessv2.ToolCallStatusPending,
				},
			})
			if appended, isNew, err := initial.AppendUpdateIfNew(ctx, started); err != nil || !isNew || appended == nil {
				t.Fatalf("append tool start = %#v new=%t err=%v", appended, isNew, err)
			}

			recoveryIdentity := mappedUpdateIdentity(started)
			recoveryIdentity.Sequence = 0
			openRecovered := func() *State {
				state, err := (Journal{
					EventStore: eventStore, MapContext: testMapContext(), RecoveryIdentity: recoveryIdentity,
				}).Open(ctx)
				if err != nil {
					t.Fatal(err)
				}
				return state
			}
			closureState := openRecovered()
			runtimeState := openRecovered()
			completed := testUpdateEvent(3, now.Add(time.Millisecond), harnessv2.UpdateEvent{
				Kind: harnessv2.UpdateToolCallUpdate,
				ToolCall: &harnessv2.ToolCallUpdate{
					ToolCallID: "call-terminal-race", Status: harnessv2.ToolCallStatusCompleted,
				},
			})

			appendCompletion := func(wantNew bool) {
				appended, isNew, err := runtimeState.AppendUpdateIfNew(ctx, completed)
				if err != nil || isNew != wantNew || (isNew && appended == nil) || (!isNew && appended != nil) {
					t.Fatalf("append real completion = %#v new=%t wantNew=%t err=%v", appended, isNew, wantNew, err)
				}
			}
			appendClosure := func() {
				if err := closureState.AppendPersistedToolClosuresIfNew(ctx, now.Add(2*time.Millisecond)); err != nil {
					t.Fatal(err)
				}
			}
			if test.closureFirst {
				appendClosure()
				appendCompletion(false)
			} else {
				appendCompletion(true)
				appendClosure()
			}

			listed := listJournalEvents(t, ctx, eventStore)
			if len(listed) != 2 || listed[0].Type != executionevents.ExecutionEventTypeToolCallStarted ||
				listed[1].Type != test.wantType || listed[0].ToolCallID != listed[1].ToolCallID {
				t.Fatalf("tool terminal race events = %#v", listed)
			}
		})
	}
}

func TestJournalRecoveryScansOnlyCurrentPromptTail(t *testing.T) {
	ctx := context.Background()
	base := storetest.NewFakeExecutionEventStore()
	var oldEvent harnessv2.Event
	for index := range store.MaxExecutionEventLimit + 5 {
		oldEvent = testUpdateEvent(uint64(index+2), time.Now().UTC(), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateUsage, Usage: &harnessv2.UsageUpdate{InputTokens: 1},
		})
		oldEvent.Identity.PromptID = "prompt-old"
		mapped, err := mapUpdate(oldEvent, testMapContext(), mapUpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := base.AppendExecutionEvent(ctx, mapped); err != nil {
			t.Fatal(err)
		}
	}
	current := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateUsage, Usage: &harnessv2.UsageUpdate{OutputTokens: 2},
	})
	mapped, err := mapUpdate(current, testMapContext(), mapUpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.AppendExecutionEvent(ctx, mapped); err != nil {
		t.Fatal(err)
	}

	recording := &recordingExecutionEventStore{ExecutionEventStore: base}
	recoveryIdentity := mappedUpdateIdentity(current)
	recoveryIdentity.Sequence = 0
	state, err := (Journal{
		EventStore: recording, MapContext: testMapContext(), RecoveryIdentity: recoveryIdentity,
	}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !state.HasUpdate(current) || state.HasUpdate(oldEvent) {
		t.Fatalf("recovered current=%t old=%t", state.HasUpdate(current), state.HasUpdate(oldEvent))
	}
	if len(recording.filters) != 2 || recording.filters[0].AfterSeq == 0 ||
		recording.filters[1].AfterSeq <= recording.filters[0].AfterSeq {
		t.Fatalf("recovery filters = %#v", recording.filters)
	}
}

func TestJournalRedactsCredentialSplitAcrossPlanUpdates(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prefix := testJournalSecretPrefix + strings.Repeat("a", 8)
	suffix := strings.Repeat("b", 16)
	first := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: prefix, Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	if projection := state.ProjectPlanUpdate(*first.Update.Plan); !strings.Contains(projection.Document, prefix) {
		t.Fatalf("first plan projection = %q, want published prefix", projection.Document)
	}
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, first); err != nil || !isNew || appended == nil {
		t.Fatalf("append first plan = %#v new=%t err=%v", appended, isNew, err)
	}

	second := testUpdateEvent(3, first.Identity.Timestamp.Add(time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: suffix, Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	projection := state.ProjectPlanUpdate(*second.Update.Plan)
	if strings.Contains(projection.Document, suffix) ||
		!strings.Contains(projection.Document, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("second plan projection exposed completing fragment: %q", projection.Document)
	}
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, second); err != nil || !isNew || appended == nil {
		t.Fatalf("append second plan = %#v new=%t err=%v", appended, isNew, err)
	}

	third := testUpdateEvent(4, second.Identity.Timestamp.Add(time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: "run focused tests", Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, third); err != nil || !isNew || appended == nil {
		t.Fatalf("append benign plan = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 3 || !strings.Contains(listed[0].ContentText, prefix) ||
		strings.Contains(listed[1].ContentText, suffix) ||
		!strings.Contains(listed[1].ContentText, executionevents.ExecutionEventRedactedValue) ||
		!strings.Contains(listed[2].ContentText, "run focused tests") {
		t.Fatalf("persisted plan updates = %#v", listed)
	}
}

func TestJournalFailsClosedAcrossSessionTaskTurns(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	firstMapContext := testMapContext()
	if _, err := eventStore.AppendExecutionEvent(ctx, &store.ExecutionEvent{
		Namespace: firstMapContext.Namespace, StreamType: store.ExecutionEventStreamTypeTask,
		StreamID: firstMapContext.StreamID, TaskName: firstMapContext.TaskName,
		SessionName: firstMapContext.SessionName, Type: executionevents.ExecutionEventTypeTaskCreated,
		Severity: executionevents.ExecutionEventSeverityInfo, Summary: "Task status initialized to Pending",
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	firstState, err := (Journal{EventStore: eventStore, MapContext: firstMapContext}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if firstState.logicalFieldHistorySaturated {
		t.Fatal("first session task entered fail-closed redaction from its lifecycle event")
	}

	prefix := testJournalSecretPrefix + strings.Repeat("a", 8)
	suffix := strings.Repeat("b", 16)
	first := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: prefix, Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	if appended, isNew, err := firstState.AppendUpdateIfNew(ctx, first); err != nil || !isNew || appended == nil ||
		!strings.Contains(appended.ContentText, prefix) {
		t.Fatalf("append first session fragment = %#v new=%t err=%v", appended, isNew, err)
	}

	secondMapContext := firstMapContext
	secondMapContext.TaskName = testJournalSecondTaskName
	secondMapContext.StreamID = testJournalSecondTaskName
	secondState, err := (Journal{EventStore: eventStore, MapContext: secondMapContext}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !secondState.logicalFieldHistorySaturated {
		t.Fatal("continued session journal did not enter fail-closed redaction mode")
	}
	second := testUpdateEvent(2, first.Identity.Timestamp.Add(time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: suffix, Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	second.Identity.TaskUID = "task-uid-2"
	second.Identity.PromptID = "prompt-2"
	appended, isNew, err := secondState.AppendUpdateIfNew(ctx, second)
	if err != nil || !isNew || appended == nil {
		t.Fatalf("append second session fragment = %#v new=%t err=%v", appended, isNew, err)
	}
	if strings.Contains(appended.Summary+appended.ContentText+string(appended.Content), suffix) ||
		!strings.Contains(appended.ContentText, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("continued session fragment was not failed closed: %#v", appended)
	}
}

func TestJournalFailsClosedAcrossNonJournalSessionText(t *testing.T) {
	tests := []struct {
		name  string
		apply func(*store.ExecutionEvent, string)
	}{
		{
			name: "summary",
			apply: func(event *store.ExecutionEvent, prefix string) {
				event.Summary = prefix
			},
		},
		{
			name: "content",
			apply: func(event *store.ExecutionEvent, prefix string) {
				event.Content = json.RawMessage(fmt.Sprintf(`{"text":%q}`, prefix))
			},
		},
		{
			name: "content text",
			apply: func(event *store.ExecutionEvent, prefix string) {
				event.ContentText = prefix
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			eventStore := storetest.NewFakeExecutionEventStore()
			firstMapContext := testMapContext()
			prefix := testJournalSecretPrefix + strings.Repeat("a", 8)
			prior := &store.ExecutionEvent{
				Namespace: firstMapContext.Namespace, StreamType: store.ExecutionEventStreamTypeTask,
				StreamID: firstMapContext.StreamID, TaskName: firstMapContext.TaskName,
				SessionName: firstMapContext.SessionName, Type: executionevents.ExecutionEventTypeModelMessage,
				Severity: executionevents.ExecutionEventSeverityInfo, CreatedAt: time.Now().UTC(),
			}
			test.apply(prior, prefix)
			if _, err := eventStore.AppendExecutionEvent(ctx, prior); err != nil {
				t.Fatal(err)
			}

			secondMapContext := firstMapContext
			secondMapContext.TaskName = testJournalSecondTaskName
			secondMapContext.StreamID = testJournalSecondTaskName
			state, err := (Journal{EventStore: eventStore, MapContext: secondMapContext}).Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !state.logicalFieldHistorySaturated {
				t.Fatal("continued session journal did not fail closed for non-journal text")
			}

			suffix := strings.Repeat("b", 16)
			next := testUpdateEvent(2, prior.CreatedAt.Add(time.Millisecond), harnessv2.UpdateEvent{
				Kind: harnessv2.UpdatePlan,
				Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
					Content: suffix, Status: harnessv2.PlanEntryInProgress,
				}}},
			})
			next.Identity.TaskUID = "task-uid-2"
			next.Identity.PromptID = "prompt-2"
			appended, isNew, err := state.AppendUpdateIfNew(ctx, next)
			if err != nil || !isNew || appended == nil {
				t.Fatalf("append continued session fragment = %#v new=%t err=%v", appended, isNew, err)
			}
			if strings.Contains(appended.Summary+appended.ContentText+string(appended.Content), suffix) ||
				!strings.Contains(appended.ContentText, executionevents.ExecutionEventRedactedValue) {
				t.Fatalf("continued session fragment was not failed closed: %#v", appended)
			}
		})
	}
}

func TestJournalIgnoresCurrentTaskLifecycleEventForSessionRedaction(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	mapCtx := testMapContext()
	now := time.Now().UTC()
	if _, err := eventStore.AppendExecutionEvent(ctx, &store.ExecutionEvent{
		Namespace: mapCtx.Namespace, StreamType: store.ExecutionEventStreamTypeTask,
		StreamID: mapCtx.StreamID, TaskName: mapCtx.TaskName, SessionName: mapCtx.SessionName,
		Type: executionevents.ExecutionEventTypeTaskCreated, Severity: executionevents.ExecutionEventSeverityInfo,
		Summary: "Task status initialized to Pending", CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	state, err := (Journal{EventStore: eventStore, MapContext: mapCtx}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.logicalFieldHistorySaturated {
		t.Fatal("current task lifecycle event saturated journal redaction history")
	}
	event := testUpdateEvent(2, now.Add(time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: "run first-turn checks", Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	appended, isNew, err := state.AppendUpdateIfNew(ctx, event)
	if err != nil || !isNew || appended == nil || !strings.Contains(appended.ContentText, "run first-turn checks") {
		t.Fatalf("append first-turn plan = %#v new=%t err=%v", appended, isNew, err)
	}
}

func TestJournalRedactsCredentialSplitAcrossDiagnosticUpdates(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prefix := testJournalSecretPrefix + strings.Repeat("a", 8)
	suffix := strings.Repeat("b", 16)
	updates := []harnessv2.DiagnosticUpdate{
		{Code: "x", Message: prefix, Retryable: true},
		{Code: "x", Message: suffix, Retryable: true},
		{Code: "x", Message: "retrying safely", Retryable: true},
	}
	for index, update := range updates {
		event := testUpdateEvent(uint64(index+2), time.Now().UTC().Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateDiagnostic, Diagnostic: &update,
		})
		if appended, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || !isNew || appended == nil {
			t.Fatalf("append diagnostic %d = %#v new=%t err=%v", index, appended, isNew, err)
		}
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 3 || !strings.Contains(listed[0].ContentText, prefix) ||
		strings.Contains(listed[1].ContentText, suffix) ||
		!strings.Contains(listed[1].ContentText, executionevents.ExecutionEventRedactedValue) ||
		listed[2].ContentText != "retrying safely" {
		t.Fatalf("persisted diagnostic updates = %#v", listed)
	}
}

func TestJournalPreservesBenignPlanUpdatesAfterFieldHistoryCap(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]harnessv2.PlanEntry, maxLogicalFieldPermutationFields/2)
	for index := range entries {
		entries[index] = harnessv2.PlanEntry{
			Content:  fmt.Sprintf("step %03d", index),
			Priority: fmt.Sprintf("priority %03d", index),
			Status:   harnessv2.PlanEntryPending,
		}
	}
	first := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan, Plan: &harnessv2.PlanUpdate{Entries: entries},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, first); err != nil || !isNew || appended == nil {
		t.Fatalf("append full plan = %#v new=%t err=%v", appended, isNew, err)
	}
	if len(state.logicalFieldHistory) != maxLogicalFieldPermutationFields {
		t.Fatalf("logical field history length = %d, want %d", len(state.logicalFieldHistory), maxLogicalFieldPermutationFields)
	}

	second := testUpdateEvent(3, first.Identity.Timestamp.Add(time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: "run focused tests", Priority: "normal", Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, second); err != nil || !isNew || appended == nil {
		t.Fatalf("append plan after history cap = %#v new=%t err=%v", appended, isNew, err)
	}
	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 || !strings.Contains(listed[1].ContentText, "run focused tests") ||
		strings.Contains(listed[1].ContentText, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("plan after history cap = %#v", listed)
	}
	if len(state.logicalFieldHistory) != maxLogicalFieldPermutationFields {
		t.Fatalf("bounded logical field history length = %d, want %d", len(state.logicalFieldHistory), maxLogicalFieldPermutationFields)
	}
}

func TestJournalFailsClosedAfterLogicalFieldHistoryCapacity(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	first := testUpdateEvent(2, now, harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: "s", Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, first); err != nil || !isNew || appended == nil {
		t.Fatalf("append initial fragment = %#v new=%t err=%v", appended, isNew, err)
	}

	entries := make([]harnessv2.PlanEntry, maxLogicalFieldPermutationFields/2)
	for index := range entries {
		entries[index] = harnessv2.PlanEntry{
			Content: fmt.Sprintf("step %03d", index), Priority: fmt.Sprintf("priority %03d", index),
			Status: harnessv2.PlanEntryPending,
		}
	}
	full := testUpdateEvent(3, now.Add(time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan, Plan: &harnessv2.PlanUpdate{Entries: entries},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, full); err != nil || !isNew || appended == nil {
		t.Fatalf("append capacity-crossing plan = %#v new=%t err=%v", appended, isNew, err)
	}
	if !state.logicalFieldHistorySaturated {
		t.Fatal("logical field history did not enter fail-closed mode")
	}

	suffix := "k-" + strings.Repeat("b", 24)
	if appended, isNew, err := state.AppendAssistantTranscriptIfNew(
		ctx, testTerminalEvent(4, now.Add(2*time.Millisecond)), suffix, false,
	); err != nil || !isNew || appended == nil {
		t.Fatalf("append post-capacity assistant fragment = %#v new=%t err=%v", appended, isNew, err)
	}
	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 3 || listed[2].ContentText != executionevents.ExecutionEventRedactedValue ||
		strings.Contains(listed[2].Summary+string(listed[2].Content), suffix) {
		t.Fatalf("post-capacity logical fields = %#v", listed)
	}
}

func TestJournalRedactsCredentialSplitAcrossPlanAndDiagnosticUpdates(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prefix := testJournalSecretPrefix + strings.Repeat("a", 10)
	suffix := strings.Repeat("b", 14)
	plan := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: prefix, Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, plan); err != nil || !isNew || appended == nil {
		t.Fatalf("append plan prefix = %#v new=%t err=%v", appended, isNew, err)
	}
	diagnostic := testUpdateEvent(3, plan.Identity.Timestamp.Add(time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateDiagnostic,
		Diagnostic: &harnessv2.DiagnosticUpdate{
			Code: testJournalRuntimeCode, Message: suffix, Retryable: true,
		},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, diagnostic); err != nil || !isNew || appended == nil {
		t.Fatalf("append diagnostic suffix = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 || !strings.Contains(listed[0].ContentText, prefix) ||
		strings.Contains(listed[1].ContentText, suffix) ||
		!strings.Contains(listed[1].ContentText, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("cross-kind logical fields = %#v", listed)
	}
}

func TestJournalRedactsCredentialSplitAcrossPlanAndAssistantTranscript(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prefix := testJournalSecretPrefix + strings.Repeat("a", 10)
	suffix := strings.Repeat("b", 14)
	now := time.Now().UTC()
	plan := testUpdateEvent(2, now, harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: prefix, Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, plan); err != nil || !isNew || appended == nil {
		t.Fatalf("append plan prefix = %#v new=%t err=%v", appended, isNew, err)
	}
	if appended, isNew, err := state.AppendAssistantTranscriptIfNew(
		ctx, testTerminalEvent(3, now.Add(time.Millisecond)), suffix, false,
	); err != nil || !isNew || appended == nil {
		t.Fatalf("append assistant suffix = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 || !strings.Contains(listed[0].ContentText, prefix) ||
		listed[1].ContentText != executionevents.ExecutionEventRedactedValue {
		t.Fatalf("plan/assistant logical fields = %#v", listed)
	}
}

func TestJournalRedactsCredentialSplitAcrossPlanAndToolMetadata(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prefix := testJournalSecretPrefix + strings.Repeat("a", 10)
	suffix := strings.Repeat("b", 14)
	now := time.Now().UTC()
	plan := testUpdateEvent(2, now, harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: prefix, Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, plan); err != nil || !isNew || appended == nil {
		t.Fatalf("append plan prefix = %#v new=%t err=%v", appended, isNew, err)
	}
	tool := testUpdateEvent(3, now.Add(time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-cross-kind", Kind: suffix, Status: harnessv2.ToolCallStatusCompleted,
		},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, tool); err != nil || !isNew || appended == nil {
		t.Fatalf("append tool suffix = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 || !strings.Contains(listed[0].ContentText, prefix) ||
		listed[1].ToolName != executionevents.ExecutionEventRedactedValue ||
		strings.Contains(string(listed[1].Content), suffix) {
		t.Fatalf("plan/tool logical fields = %#v", listed)
	}
}

func TestJournalRedactsCredentialSplitIntoTerminalUsageModel(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	prefix := testJournalSecretPrefix + strings.Repeat("a", 10)
	suffix := strings.Repeat("b", 14)
	plan := testUpdateEvent(2, now, harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: prefix, Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, plan); err != nil || !isNew || appended == nil {
		t.Fatalf("append plan prefix = %#v new=%t err=%v", appended, isNew, err)
	}
	terminal := testTerminalEvent(3, now.Add(time.Millisecond))
	terminal.Completed = &harnessv2.CompletedEvent{
		StopReason: harnessv2.ACPStopReasonEndTurn,
		Result: harnessv2.PromptResult{
			Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: testJournalDone}},
			Model:   suffix,
			Usage:   harnessv2.UsageUpdate{InputTokens: 1},
		},
	}
	if appended, isNew, err := state.AppendTerminalUsageIfNew(ctx, terminal); err != nil || !isNew || appended == nil {
		t.Fatalf("append terminal usage = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 {
		t.Fatalf("terminal usage events = %#v", listed)
	}
	var content map[string]any
	if err := json.Unmarshal(listed[1].Content, &content); err != nil {
		t.Fatal(err)
	}
	if content["model"] != executionevents.ExecutionEventRedactedValue ||
		strings.Contains(listed[1].Summary+string(listed[1].Content), suffix) {
		t.Fatalf("terminal usage content = %#v event=%#v", content, listed[1])
	}
}

func TestJournalRedactsCredentialSplitIntoTerminalLifecycle(t *testing.T) {
	for _, test := range []struct {
		name       string
		eventType  harnessv2.EventType
		contentKey string
	}{
		{name: "completed model", eventType: harnessv2.EventCompleted, contentKey: "model"},
		{name: "cancelled reason", eventType: harnessv2.EventCancelled, contentKey: "reason"},
		{name: "failure message", eventType: harnessv2.EventFailed, contentKey: "message"},
		{name: "unknown message", eventType: harnessv2.EventOutcomeUnknown, contentKey: "message"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			eventStore := storetest.NewFakeExecutionEventStore()
			state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			prefix := testJournalSecretPrefix + strings.Repeat("a", 10)
			suffix := strings.Repeat("b", 14)
			plan := testUpdateEvent(2, now, harnessv2.UpdateEvent{
				Kind: harnessv2.UpdatePlan,
				Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
					Content: prefix, Status: harnessv2.PlanEntryInProgress,
				}}},
			})
			if appended, isNew, err := state.AppendUpdateIfNew(ctx, plan); err != nil || !isNew || appended == nil {
				t.Fatalf("append plan prefix = %#v new=%t err=%v", appended, isNew, err)
			}

			terminal := testUpdateEvent(3, now.Add(time.Millisecond), harnessv2.UpdateEvent{})
			terminal.Type = test.eventType
			terminal.Update = nil
			switch test.eventType {
			case harnessv2.EventCompleted:
				terminal.Completed = &harnessv2.CompletedEvent{
					StopReason: harnessv2.ACPStopReasonEndTurn,
					Result: harnessv2.PromptResult{
						Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: testJournalDone}},
						Model:   suffix,
					},
				}
			case harnessv2.EventCancelled:
				terminal.Cancelled = &harnessv2.CancelledEvent{
					StopReason: harnessv2.ACPStopReasonCancelled, Reason: suffix,
				}
			case harnessv2.EventFailed:
				terminal.Failed = &harnessv2.FailedEvent{
					StopReason: harnessv2.ACPStopReasonRefusal, Code: testJournalRuntimeCode, Message: suffix,
				}
			case harnessv2.EventOutcomeUnknown:
				terminal.OutcomeUnknown = &harnessv2.OutcomeUnknownEvent{Code: testJournalRuntimeCode, Message: suffix}
			}
			if appended, isNew, err := state.AppendPromptLifecycleIfNew(ctx, terminal); err != nil || !isNew || appended == nil {
				t.Fatalf("append terminal lifecycle = %#v new=%t err=%v", appended, isNew, err)
			}

			listed := listJournalEvents(t, ctx, eventStore)
			if len(listed) != 2 {
				t.Fatalf("terminal lifecycle events = %#v", listed)
			}
			var content map[string]any
			if err := json.Unmarshal(listed[1].Content, &content); err != nil {
				t.Fatal(err)
			}
			if content[test.contentKey] != executionevents.ExecutionEventRedactedValue ||
				strings.Contains(listed[1].Summary+string(listed[1].Content), suffix) {
				t.Fatalf("terminal lifecycle content = %#v event=%#v", content, listed[1])
			}
		})
	}
}

func TestJournalRedactsCredentialSplitIntoPromptStreamFailure(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	accepted := testUpdateEvent(1, now, harnessv2.UpdateEvent{})
	accepted.Type = harnessv2.EventAccepted
	accepted.Update = nil
	accepted.Accepted = &harnessv2.AcceptedEvent{
		AcceptedAt: now,
		Lease: harnessv2.PromptLease{
			Generation: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute),
		},
		ACPVersion: harnessv2.ACPProfileV1,
	}
	if appended, isNew, err := state.AppendPromptLifecycleIfNew(ctx, accepted); err != nil || !isNew || appended == nil {
		t.Fatalf("append accepted lifecycle = %#v new=%t err=%v", appended, isNew, err)
	}
	prefix := testJournalSecretPrefix + strings.Repeat("a", 10)
	suffix := strings.Repeat("b", 14)
	plan := testUpdateEvent(2, now.Add(time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: prefix, Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, plan); err != nil || !isNew || appended == nil {
		t.Fatalf("append plan prefix = %#v new=%t err=%v", appended, isNew, err)
	}
	if appended, isNew, err := state.AppendPromptStreamFailureIfNew(
		ctx, now.Add(2*time.Millisecond), suffix,
	); err != nil || !isNew || appended == nil {
		t.Fatalf("append stream failure = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 3 {
		t.Fatalf("prompt stream failure events = %#v", listed)
	}
	var content map[string]any
	if err := json.Unmarshal(listed[2].Content, &content); err != nil {
		t.Fatal(err)
	}
	if content["message"] != executionevents.ExecutionEventRedactedValue ||
		strings.Contains(listed[2].Summary+string(listed[2].Content), suffix) {
		t.Fatalf("prompt stream failure content = %#v event=%#v", content, listed[2])
	}
}

func TestJournalRedactsCredentialSplitFromAcceptedModel(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	mapCtx := testMapContext()
	prefix := testJournalSecretPrefix + strings.Repeat("a", 10)
	suffix := strings.Repeat("b", 14)
	mapCtx.Model = prefix
	state, err := (Journal{EventStore: eventStore, MapContext: mapCtx}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	accepted := testUpdateEvent(1, now, harnessv2.UpdateEvent{})
	accepted.Type = harnessv2.EventAccepted
	accepted.Update = nil
	accepted.Accepted = &harnessv2.AcceptedEvent{
		AcceptedAt: now,
		Lease: harnessv2.PromptLease{
			Generation: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute),
		},
		ACPVersion: harnessv2.ACPProfileV1,
	}
	if appended, isNew, err := state.AppendPromptLifecycleIfNew(ctx, accepted); err != nil || !isNew || appended == nil {
		t.Fatalf("append accepted lifecycle = %#v new=%t err=%v", appended, isNew, err)
	}
	plan := testUpdateEvent(2, now.Add(time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdatePlan,
		Plan: &harnessv2.PlanUpdate{Entries: []harnessv2.PlanEntry{{
			Content: suffix, Status: harnessv2.PlanEntryInProgress,
		}}},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, plan); err != nil || !isNew || appended == nil {
		t.Fatalf("append plan suffix = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 {
		t.Fatalf("accepted-model events = %#v", listed)
	}
	var acceptedContent map[string]any
	if err := json.Unmarshal(listed[0].Content, &acceptedContent); err != nil {
		t.Fatal(err)
	}
	if acceptedContent["model"] != prefix {
		t.Fatalf("accepted model = %#v, want fragment preserved", acceptedContent["model"])
	}
	if strings.Contains(listed[1].ContentText, suffix) ||
		!strings.Contains(listed[1].ContentText, executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("plan suffix reconstructed accepted model credential: %#v", listed[1])
	}
}

func TestJournalRetriesAppendOnlyAfterConfirmedAbsence(t *testing.T) {
	ctx := context.Background()
	base := storetest.NewFakeExecutionEventStore()
	eventStore := &faultingAppendEventStore{
		ExecutionEventStore: base,
		faults:              []appendFault{{err: errors.New("transient append failure")}},
	}
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateUsage, Usage: &harnessv2.UsageUpdate{InputTokens: 10},
	})
	appended, isNew, err := state.AppendUpdateIfNew(ctx, event)
	if err != nil || !isNew || appended == nil {
		t.Fatalf("recovered append = %#v new=%t err=%v", appended, isNew, err)
	}
	if eventStore.appendCalls != 2 {
		t.Fatalf("append calls = %d, want 2", eventStore.appendCalls)
	}
	if listed := listJournalEvents(t, ctx, base); len(listed) != 1 {
		t.Fatalf("persisted events = %d, want 1", len(listed))
	}
}

func TestJournalReconcilesAmbiguousCommittedAppendWithoutDuplicate(t *testing.T) {
	ctx := context.Background()
	base := storetest.NewFakeExecutionEventStore()
	eventStore := &faultingAppendEventStore{
		ExecutionEventStore: base,
		faults: []appendFault{{
			persistBeforeError: true,
			err:                errors.New("append response lost"),
		}},
	}
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateUsage, Usage: &harnessv2.UsageUpdate{InputTokens: 10},
	})
	appended, isNew, err := state.AppendUpdateIfNew(ctx, event)
	if err != nil || isNew || appended != nil {
		t.Fatalf("ambiguous append = %#v new=%t err=%v", appended, isNew, err)
	}
	if eventStore.appendCalls != 1 {
		t.Fatalf("append calls = %d, want 1", eventStore.appendCalls)
	}
	if !state.HasUpdate(event) {
		t.Fatal("journal did not remember reconciled update")
	}
	if listed := listJournalEvents(t, ctx, base); len(listed) != 1 {
		t.Fatalf("persisted events = %d, want 1", len(listed))
	}
}

func TestJournalReconcilesAmbiguousCommittedRetryWithoutDuplicate(t *testing.T) {
	ctx := context.Background()
	base := storetest.NewFakeExecutionEventStore()
	eventStore := &faultingAppendEventStore{
		ExecutionEventStore: base,
		faults: []appendFault{
			{err: errors.New("transient append failure")},
			{persistBeforeError: true, err: errors.New("retry response lost")},
		},
	}
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateUsage, Usage: &harnessv2.UsageUpdate{InputTokens: 10},
	})
	appended, isNew, err := state.AppendUpdateIfNew(ctx, event)
	if err != nil || isNew || appended != nil {
		t.Fatalf("ambiguous retry = %#v new=%t err=%v", appended, isNew, err)
	}
	if eventStore.appendCalls != 2 {
		t.Fatalf("append calls = %d, want 2", eventStore.appendCalls)
	}
	if listed := listJournalEvents(t, ctx, base); len(listed) != 1 {
		t.Fatalf("persisted events = %d, want 1", len(listed))
	}
}

func TestJournalReturnsPersistentAppendFailureAfterOneRetry(t *testing.T) {
	ctx := context.Background()
	base := storetest.NewFakeExecutionEventStore()
	eventStore := &faultingAppendEventStore{
		ExecutionEventStore: base,
		faults: []appendFault{
			{err: errors.New("first append failure")},
			{err: errors.New("retry append failure")},
		},
	}
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateUsage, Usage: &harnessv2.UsageUpdate{InputTokens: 10},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, event); err == nil || isNew || appended != nil {
		t.Fatalf("persistent append = %#v new=%t err=%v", appended, isNew, err)
	}
	if eventStore.appendCalls != 2 {
		t.Fatalf("append calls = %d, want 2", eventStore.appendCalls)
	}
	if state.HasUpdate(event) {
		t.Fatal("journal remembered an update that was never persisted")
	}
}

func TestJournalDoesNotRetryWhenAppendAbsenceCannotBeConfirmed(t *testing.T) {
	ctx := context.Background()
	base := storetest.NewFakeExecutionEventStore()
	eventStore := &faultingAppendEventStore{
		ExecutionEventStore: base,
		faults:              []appendFault{{err: errors.New("append failure")}},
		listFaultAt:         2,
		listErr:             errors.New("readback failure"),
	}
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateUsage, Usage: &harnessv2.UsageUpdate{InputTokens: 10},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, event); err == nil || isNew || appended != nil {
		t.Fatalf("unreconciled append = %#v new=%t err=%v", appended, isNew, err)
	}
	if eventStore.appendCalls != 1 {
		t.Fatalf("append calls = %d, want 1", eventStore.appendCalls)
	}
}

func TestJournalRedactsCredentialsSplitAcrossAssistantChunks(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	journal := Journal{EventStore: eventStore, MapContext: testMapContext()}
	state, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	credential := testJournalSecretPrefix + strings.Repeat("a", 24)
	chunks := []string{"hello", " ", credential[:9], credential[9:] + " world"}
	transcript := strings.Join(chunks, "")
	now := time.Now().UTC()
	for index, chunk := range chunks {
		event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind:             harnessv2.UpdateAssistantMessageChunk,
			AssistantMessage: &harnessv2.AssistantMessageChunk{Text: chunk},
		})
		if appended, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || isNew || appended != nil {
			t.Fatalf("append assistant chunk %d = %#v new=%t err=%v", index, appended, isNew, err)
		}
	}
	terminal := testTerminalEvent(6, now.Add(5*time.Millisecond))
	if _, isNew, err := state.AppendAssistantTranscriptIfNew(ctx, terminal, transcript, false); err != nil || !isNew {
		t.Fatalf("append assistant transcript new=%t err=%v", isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	var persisted strings.Builder
	for _, event := range listed {
		persisted.WriteString(event.Summary)
		persisted.Write(event.Content)
		persisted.WriteString(event.ContentText)
	}
	if strings.Contains(persisted.String(), credential) ||
		strings.Contains(persisted.String(), credential[:9]) ||
		!strings.Contains(persisted.String(), executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("persisted assistant stream was reconstructable: %q", persisted.String())
	}
	if got := listed[len(listed)-1].ContentText; got != "hello "+executionevents.ExecutionEventRedactedValue+" world" {
		t.Fatalf("assistant transcript = %q", got)
	}

	recovered, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, isNew, err := recovered.AppendAssistantTranscriptIfNew(ctx, terminal, transcript, false); err != nil || isNew || duplicate != nil {
		t.Fatalf("recovered assistant transcript = %#v new=%t err=%v", duplicate, isNew, err)
	}
}

func TestJournalPersistsTerminalUsageSeparatelyFromAssistantTranscript(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	journal := Journal{EventStore: eventStore, MapContext: testMapContext()}
	state, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	terminal := testTerminalEvent(3, time.Now().UTC())
	terminal.Completed = &harnessv2.CompletedEvent{
		StopReason: harnessv2.ACPStopReasonEndTurn,
		Result: harnessv2.PromptResult{
			Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: testJournalDone}},
			Model:   testJournalServedModel,
			Usage:   harnessv2.UsageUpdate{InputTokens: 100, OutputTokens: 25, CachedInputTokens: 40},
		},
	}
	if appended, isNew, err := state.AppendTerminalUsageIfNew(ctx, terminal); err != nil || !isNew || appended == nil {
		t.Fatalf("append terminal usage = %#v new=%t err=%v", appended, isNew, err)
	}
	if appended, isNew, err := state.AppendAssistantTranscriptIfNew(ctx, terminal, testJournalDone, false); err != nil || !isNew || appended == nil {
		t.Fatalf("append assistant transcript = %#v new=%t err=%v", appended, isNew, err)
	}
	if duplicate, isNew, err := state.AppendTerminalUsageIfNew(ctx, terminal); err != nil || isNew || duplicate != nil {
		t.Fatalf("same-pass terminal usage = %#v new=%t err=%v", duplicate, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 {
		t.Fatalf("terminal journal events = %d, want 2", len(listed))
	}
	if listed[0].Type != executionevents.ExecutionEventTypeModelUsageUpdated ||
		listed[1].Type != executionevents.ExecutionEventTypeModelMessage {
		t.Fatalf("terminal journal events = %#v", listed)
	}
	var usageContent map[string]any
	if err := json.Unmarshal(listed[0].Content, &usageContent); err != nil {
		t.Fatal(err)
	}
	if usageContent["model"] != testJournalServedModel {
		t.Fatalf("terminal usage content = %#v", usageContent)
	}

	recovered, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, isNew, err := recovered.AppendTerminalUsageIfNew(ctx, terminal); err != nil || isNew || duplicate != nil {
		t.Fatalf("recovered terminal usage = %#v new=%t err=%v", duplicate, isNew, err)
	}
	if duplicate, isNew, err := recovered.AppendAssistantTranscriptIfNew(ctx, terminal, testJournalDone, false); err != nil || isNew || duplicate != nil {
		t.Fatalf("recovered assistant transcript = %#v new=%t err=%v", duplicate, isNew, err)
	}
}

func TestJournalPersistsPromptLifecycleWithRecoveryDeduplication(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	journal := Journal{EventStore: eventStore, MapContext: testMapContext()}
	state, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	accepted := testUpdateEvent(1, now, harnessv2.UpdateEvent{})
	accepted.Type = harnessv2.EventAccepted
	accepted.Update = nil
	accepted.Accepted = &harnessv2.AcceptedEvent{
		AcceptedAt: now,
		Lease: harnessv2.PromptLease{
			Generation: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute),
		},
		ACPVersion: harnessv2.ACPProfileV1,
	}
	terminal := testTerminalEvent(2, now.Add(time.Second))
	terminal.Completed = &harnessv2.CompletedEvent{
		StopReason: harnessv2.ACPStopReasonEndTurn,
		Result:     harnessv2.PromptResult{Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: testJournalDone}}},
	}
	if appended, isNew, err := state.AppendPromptLifecycleIfNew(ctx, accepted); err != nil || !isNew || appended == nil {
		t.Fatalf("append accepted lifecycle = %#v new=%t err=%v", appended, isNew, err)
	}
	if appended, isNew, err := state.AppendPromptLifecycleIfNew(ctx, terminal); err != nil || !isNew || appended == nil {
		t.Fatalf("append terminal lifecycle = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 || listed[0].Type != executionevents.ExecutionEventTypeModelRequestStarted ||
		listed[1].Type != executionevents.ExecutionEventTypeModelRequestCompleted {
		t.Fatalf("prompt lifecycle events = %#v", listed)
	}
	recovered, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, isNew, err := recovered.AppendPromptLifecycleIfNew(ctx, accepted); err != nil || isNew || duplicate != nil {
		t.Fatalf("recovered accepted lifecycle = %#v new=%t err=%v", duplicate, isNew, err)
	}
	if duplicate, isNew, err := recovered.AppendPromptLifecycleIfNew(ctx, terminal); err != nil || isNew || duplicate != nil {
		t.Fatalf("recovered terminal lifecycle = %#v new=%t err=%v", duplicate, isNew, err)
	}
}

func TestJournalPersistsPromptStreamFailureWithRecoveryDeduplication(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	journal := Journal{EventStore: eventStore, MapContext: testMapContext()}
	state, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if appended, isNew, err := state.AppendPromptStreamFailureIfNew(ctx, now, "stream failed"); err != nil || isNew || appended != nil {
		t.Fatalf("failure before acceptance = %#v new=%t err=%v", appended, isNew, err)
	}
	accepted := testUpdateEvent(1, now, harnessv2.UpdateEvent{})
	accepted.Type = harnessv2.EventAccepted
	accepted.Update = nil
	accepted.Accepted = &harnessv2.AcceptedEvent{
		AcceptedAt: now,
		Lease: harnessv2.PromptLease{
			Generation: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute),
		},
		ACPVersion: harnessv2.ACPProfileV1,
	}
	if appended, isNew, err := state.AppendPromptLifecycleIfNew(ctx, accepted); err != nil || !isNew || appended == nil {
		t.Fatalf("append accepted lifecycle = %#v new=%t err=%v", appended, isNew, err)
	}
	if appended, isNew, err := state.AppendPromptStreamFailureIfNew(
		ctx, now.Add(time.Second), "runtime prompt transport failed",
	); err != nil || !isNew || appended == nil {
		t.Fatalf("append prompt stream failure = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 || listed[0].Type != executionevents.ExecutionEventTypeModelRequestStarted ||
		listed[1].Type != executionevents.ExecutionEventTypeModelRequestFailed {
		t.Fatalf("prompt stream lifecycle events = %#v", listed)
	}
	var content map[string]any
	if err := json.Unmarshal(listed[1].Content, &content); err != nil {
		t.Fatal(err)
	}
	if content[mappedControllerSynthesizedKey] != true || content[mappedModelRequestIDContentKey] != testJournalPromptID ||
		content["code"] != mappedPromptStreamFailureCode || content["message"] != "runtime prompt transport failed" {
		t.Fatalf("prompt stream failure content = %#v", content)
	}

	recovered, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, isNew, err := recovered.AppendPromptStreamFailureIfNew(
		ctx, now.Add(2*time.Second), "different retry diagnostic",
	); err != nil || isNew || duplicate != nil {
		t.Fatalf("recovered prompt stream failure = %#v new=%t err=%v", duplicate, isNew, err)
	}
	if listed = listJournalEvents(t, ctx, eventStore); len(listed) != 2 {
		t.Fatalf("prompt stream lifecycle event count after recovery = %d, want 2", len(listed))
	}
}

func TestJournalPersistsProvenPromptSettlementWithRecoveryDeduplication(t *testing.T) {
	for _, test := range []struct {
		name               string
		settlement         harnessv2.PromptSettlement
		cancellationReason harnessv2.CancelReason
		wantType           string
	}{
		{
			name: "cancelled",
			settlement: harnessv2.PromptSettlement{
				TerminalEvent: harnessv2.EventCancelled, Outcome: harnessv2.PromptOutcomeCancelled,
				StopReason: harnessv2.ACPStopReasonCancelled,
			},
			cancellationReason: harnessv2.CancelReasonTaskTimeout,
			wantType:           executionevents.ExecutionEventTypeModelRequestFailed,
		},
		{
			name: "failed",
			settlement: harnessv2.PromptSettlement{
				TerminalEvent: harnessv2.EventFailed, Outcome: harnessv2.PromptOutcomeFailed,
				StopReason: harnessv2.ACPStopReasonRefusal,
			},
			cancellationReason: harnessv2.CancelReasonStreamDisconnected,
			wantType:           executionevents.ExecutionEventTypeModelRequestFailed,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			eventStore := storetest.NewFakeExecutionEventStore()
			journal := Journal{EventStore: eventStore, MapContext: testMapContext()}
			state, err := journal.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			accepted := testUpdateEvent(1, now, harnessv2.UpdateEvent{})
			accepted.Type = harnessv2.EventAccepted
			accepted.Update = nil
			accepted.Accepted = &harnessv2.AcceptedEvent{
				AcceptedAt: now,
				Lease: harnessv2.PromptLease{
					Generation: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute),
				},
				ACPVersion: harnessv2.ACPProfileV1,
			}
			if appended, isNew, err := state.AppendPromptLifecycleIfNew(ctx, accepted); err != nil || !isNew || appended == nil {
				t.Fatalf("append accepted lifecycle = %#v new=%t err=%v", appended, isNew, err)
			}
			test.settlement.SettledAt = now.Add(time.Second)
			if appended, isNew, err := state.AppendPromptSettlementIfNew(ctx, test.settlement, test.cancellationReason); err != nil || !isNew || appended == nil {
				t.Fatalf("append prompt settlement = %#v new=%t err=%v", appended, isNew, err)
			}

			listed := listJournalEvents(t, ctx, eventStore)
			if len(listed) != 2 || listed[0].Type != executionevents.ExecutionEventTypeModelRequestStarted || listed[1].Type != test.wantType {
				t.Fatalf("prompt settlement lifecycle events = %#v", listed)
			}
			var content map[string]any
			if err := json.Unmarshal(listed[1].Content, &content); err != nil {
				t.Fatal(err)
			}
			if content["terminalEvent"] != string(test.settlement.TerminalEvent) ||
				content["cancellationReason"] != string(test.cancellationReason) ||
				content["controllerSynthesized"] != true || content["settlementProven"] != true {
				t.Fatalf("prompt settlement lifecycle content = %#v", content)
			}

			recoveryJournal := journal
			recoveryJournal.RecoveryIdentity = mappedUpdateIdentity(accepted)
			evidence, err := recoveryJournal.FindPromptTerminal(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if evidence == nil || evidence.TerminalEvent != test.settlement.TerminalEvent ||
				evidence.CancellationReason != test.cancellationReason {
				t.Fatalf("prompt settlement recovery evidence = %#v", evidence)
			}
			recovered, err := journal.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if duplicate, isNew, err := recovered.AppendPromptSettlementIfNew(ctx, test.settlement, test.cancellationReason); err != nil || isNew || duplicate != nil {
				t.Fatalf("recovered prompt settlement = %#v new=%t err=%v", duplicate, isNew, err)
			}
			laterTerminal := testTerminalEvent(2, now.Add(2*time.Second))
			if duplicate, isNew, err := recovered.AppendPromptLifecycleIfNew(ctx, laterTerminal); err != nil || isNew || duplicate != nil {
				t.Fatalf("terminal after proven settlement = %#v new=%t err=%v", duplicate, isNew, err)
			}
			if listed = listJournalEvents(t, ctx, eventStore); len(listed) != 2 {
				t.Fatalf("prompt settlement lifecycle event count after later terminal = %d, want 2", len(listed))
			}
		})
	}
}

func TestJournalPersistsAssistantTextOnNonTerminalStreamClosure(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	journal := Journal{EventStore: eventStore, MapContext: testMapContext()}
	state, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	credential := testJournalSecretPrefix + strings.Repeat("d", 24)
	fragments := []string{"before " + credential[:10], credential[10:] + " after"}
	now := time.Now().UTC()
	var last harnessv2.Event
	for index, fragment := range fragments {
		last = testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind:             harnessv2.UpdateAssistantMessageChunk,
			AssistantMessage: &harnessv2.AssistantMessageChunk{Text: fragment},
		})
		if appended, isNew, err := state.AppendUpdateIfNew(ctx, last); err != nil || isNew || appended != nil {
			t.Fatalf("assistant chunk %d = %#v new=%t err=%v", index, appended, isNew, err)
		}
	}
	transcript := strings.Join(fragments, "")
	if appended, isNew, err := state.AppendAssistantStreamClosureIfNew(ctx, last, transcript, false); err != nil || !isNew || appended == nil {
		t.Fatalf("assistant stream closure = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 1 || listed[0].ContentText != "before "+executionevents.ExecutionEventRedactedValue+" after" {
		t.Fatalf("persisted assistant stream closure = %#v", listed)
	}
	recovered, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, isNew, err := recovered.AppendAssistantStreamClosureIfNew(ctx, last, transcript, false); err != nil || isNew || duplicate != nil {
		t.Fatalf("recovered assistant stream closure = %#v new=%t err=%v", duplicate, isNew, err)
	}
}

func TestJournalPersistsOverflowedAssistantStreamAsOmitted(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		name := "non-terminal closure"
		if terminal {
			name = "terminal closure"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			eventStore := storetest.NewFakeExecutionEventStore()
			journal := Journal{EventStore: eventStore, MapContext: testMapContext()}
			state, err := journal.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}

			now := time.Now().UTC()
			lastUpdate := testUpdateEvent(2, now, harnessv2.UpdateEvent{
				Kind:             harnessv2.UpdateAssistantMessageChunk,
				AssistantMessage: &harnessv2.AssistantMessageChunk{Text: "unsafe-prefix"},
			})
			if appended, isNew, err := state.AppendUpdateIfNew(ctx, lastUpdate); err != nil || isNew || appended != nil {
				t.Fatalf("assistant chunk = %#v new=%t err=%v", appended, isNew, err)
			}
			assistantEvent := lastUpdate
			if terminal {
				assistantEvent = testTerminalEvent(3, now.Add(time.Millisecond))
			}
			if err := state.AppendBufferedStreamsIfNew(ctx, &assistantEvent, 2, "unsafe-prefix", true); err != nil {
				t.Fatal(err)
			}

			listed := listJournalEvents(t, ctx, eventStore)
			if len(listed) != 1 {
				t.Fatalf("omitted assistant events = %#v", listed)
			}
			persisted := listed[0]
			encoded := persisted.Summary + persisted.ContentText + string(persisted.Content)
			if persisted.Type != executionevents.ExecutionEventTypeModelMessage ||
				persisted.ContentText != "" || strings.Contains(encoded, "unsafe-prefix") ||
				persisted.Summary != assistantResponseOmittedSummary ||
				persisted.Truncation == nil || !persisted.Truncation.ContentTextTruncated ||
				!strings.Contains(string(persisted.Content), streamedTextTruncatedOrOmittedReason) {
				t.Fatalf("omitted assistant event = %#v content=%s", persisted, persisted.Content)
			}

			recovered, err := journal.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := recovered.AppendBufferedStreamsIfNew(ctx, &assistantEvent, 2, "unsafe-prefix", true); err != nil {
				t.Fatalf("recover omitted assistant stream: %v", err)
			}
			if listed = listJournalEvents(t, ctx, eventStore); len(listed) != 1 {
				t.Fatalf("recovered omitted assistant events = %#v", listed)
			}
		})
	}
}

func TestJournalRedactsCredentialsSplitAcrossToolUpdates(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	credential := testJournalSecretPrefix + strings.Repeat("b", 24)
	fragments := []string{"before " + credential[:10], credential[10:] + " after"}
	now := time.Now().UTC()
	for index, fragment := range fragments {
		status := harnessv2.ToolCallStatusInProgress
		if index == len(fragments)-1 {
			status = harnessv2.ToolCallStatusCompleted
		}
		event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateToolCallUpdate,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: "call-split", Kind: testToolKindShell, Status: status,
				Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: fragment}},
			},
		})
		if _, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || !isNew {
			t.Fatalf("append tool update %d new=%t err=%v", index, isNew, err)
		}
	}

	listed := listJournalEvents(t, ctx, eventStore)
	var persisted strings.Builder
	for _, event := range listed {
		persisted.WriteString(event.Summary)
		persisted.Write(event.Content)
		persisted.WriteString(event.ContentText)
	}
	if strings.Contains(persisted.String(), credential) ||
		strings.Contains(persisted.String(), credential[:10]) ||
		!strings.Contains(persisted.String(), executionevents.ExecutionEventRedactedValue) {
		t.Fatalf("persisted tool stream was reconstructable: %q", persisted.String())
	}
	if got := listed[len(listed)-1].ContentText; got != "before "+executionevents.ExecutionEventRedactedValue+" after" {
		t.Fatalf("completed tool output = %q", got)
	}
}

func TestJournalOmitsToolContentSplitAcrossBlocks(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	prefix := "sk-aaaaaaaa"
	suffix := "aaaaaaaaaaaaaaaa"
	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-split-blocks", Kind: testToolKindShell, Status: harnessv2.ToolCallStatusCompleted,
			Content: []harnessv2.ContentBlock{
				{Type: harnessv2.ContentBlockText, Text: prefix},
				{Type: harnessv2.ContentBlockText, Text: suffix},
			},
		},
	})
	if _, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || !isNew {
		t.Fatalf("append multi-block tool update new=%t err=%v", isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 1 {
		t.Fatalf("persisted tool events = %d, want 1", len(listed))
	}
	persisted := listed[0]
	encoded := persisted.Summary + persisted.ContentText + string(persisted.Content)
	if persisted.ContentText != "" || strings.Contains(encoded, prefix) || strings.Contains(encoded, suffix) ||
		!strings.Contains(string(persisted.Content), toolContentMultipleBlocksOmittedReason) || persisted.Truncation != nil {
		t.Fatalf("multi-block tool event = %#v", persisted)
	}
}

func TestJournalAggregatesContentOnlyToolFragmentsIntoTerminalEvent(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	for index, fragment := range []string{"streamed ", "output"} {
		event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateToolCallUpdate,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: "call-content-only", Status: harnessv2.ToolCallStatusInProgress,
				Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: fragment}},
			},
		})
		wantNew := index == 0
		if appended, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || isNew != wantNew || (isNew && appended == nil) || (!isNew && appended != nil) {
			t.Fatalf("content fragment %d = %#v new=%t err=%v", index, appended, isNew, err)
		}
	}
	completed := testUpdateEvent(4, now.Add(2*time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-content-only", Status: harnessv2.ToolCallStatusCompleted,
		},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, completed); err != nil || !isNew || appended == nil {
		t.Fatalf("completed tool = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 || listed[0].Type != executionevents.ExecutionEventTypeToolCallStarted ||
		listed[1].Type != executionevents.ExecutionEventTypeToolCallCompleted || listed[1].ContentText != "streamed output" {
		t.Fatalf("persisted tool events = %#v", listed)
	}
}

func TestJournalPersistsBufferedToolOutputOnStreamClosure(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	credential := testJournalSecretPrefix + strings.Repeat("z", 24)
	now := time.Now().UTC()
	fragments := []string{"before " + credential[:10], credential[10:] + " after"}
	for index, fragment := range fragments {
		event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateToolCallUpdate,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: testJournalOpenToolCallID, Title: testJournalToolTitle, Kind: testToolKindShell,
				Status:  harnessv2.ToolCallStatusInProgress,
				Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: fragment}},
			},
		})
		wantNew := index == 0
		if appended, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || isNew != wantNew || (isNew && appended == nil) || (!isNew && appended != nil) {
			t.Fatalf("append open tool fragment %d = %#v new=%t err=%v", index, appended, isNew, err)
		}
	}
	if listed := listJournalEvents(t, ctx, eventStore); len(listed) != 1 || listed[0].Type != executionevents.ExecutionEventTypeToolCallStarted {
		t.Fatalf("tool start before closure = %#v", listed)
	}
	if err := state.AppendToolStreamClosuresIfNew(ctx); err != nil {
		t.Fatal(err)
	}
	if err := state.AppendToolStreamClosuresIfNew(ctx); err != nil {
		t.Fatal(err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 || listed[1].Type != executionevents.ExecutionEventTypeToolCallFailed ||
		listed[1].Severity != executionevents.ExecutionEventSeverityError ||
		listed[1].ContentText != "before "+executionevents.ExecutionEventRedactedValue+" after" ||
		listed[1].ToolName != testToolKindShell || listed[1].Summary != testJournalToolTitle {
		t.Fatalf("persisted tool stream closure = %#v", listed)
	}
}

func TestJournalPersistsBufferedToolClosuresInProtocolOrder(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	for index, tool := range []struct {
		id      string
		content string
	}{{id: "call-z", content: "first"}, {id: "call-a", content: "second"}} {
		event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateToolCallUpdate,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: tool.id, Status: harnessv2.ToolCallStatusInProgress,
				Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: tool.content}},
			},
		})
		if appended, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || !isNew || appended == nil {
			t.Fatalf("append open tool %q = %#v new=%t err=%v", tool.id, appended, isNew, err)
		}
	}
	if err := state.AppendToolStreamClosuresIfNew(ctx); err != nil {
		t.Fatal(err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 4 ||
		listed[0].Type != executionevents.ExecutionEventTypeToolCallStarted ||
		listed[1].Type != executionevents.ExecutionEventTypeToolCallStarted ||
		listed[2].Type != executionevents.ExecutionEventTypeToolCallFailed ||
		listed[3].Type != executionevents.ExecutionEventTypeToolCallFailed ||
		listed[2].ContentText != "first" || listed[3].ContentText != "second" {
		t.Fatalf("persisted tool closure order = %#v", listed)
	}
}

func TestJournalPersistsBufferedAssistantAndToolStreamsInProtocolOrder(t *testing.T) {
	for _, test := range []struct {
		name         string
		assistantSeq uint64
		toolSeq      uint64
		wantTypes    []string
	}{
		{
			name: "assistant before tool", assistantSeq: 2, toolSeq: 3,
			wantTypes: []string{
				executionevents.ExecutionEventTypeToolCallStarted,
				executionevents.ExecutionEventTypeModelMessage,
				executionevents.ExecutionEventTypeToolCallFailed,
			},
		},
		{
			name: "tool before assistant", assistantSeq: 3, toolSeq: 2,
			wantTypes: []string{
				executionevents.ExecutionEventTypeToolCallStarted,
				executionevents.ExecutionEventTypeToolCallFailed,
				executionevents.ExecutionEventTypeModelMessage,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			eventStore := storetest.NewFakeExecutionEventStore()
			state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
			if err != nil {
				t.Fatal(err)
			}

			now := time.Now().UTC()
			updates := []harnessv2.Event{
				testUpdateEvent(test.assistantSeq, now.Add(time.Duration(test.assistantSeq)*time.Millisecond), harnessv2.UpdateEvent{
					Kind: harnessv2.UpdateAssistantMessageChunk, AssistantMessage: &harnessv2.AssistantMessageChunk{Text: "assistant"},
				}),
				testUpdateEvent(test.toolSeq, now.Add(time.Duration(test.toolSeq)*time.Millisecond), harnessv2.UpdateEvent{
					Kind: harnessv2.UpdateToolCallUpdate,
					ToolCall: &harnessv2.ToolCallUpdate{
						ToolCallID: "call-open", Status: harnessv2.ToolCallStatusInProgress,
						Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: "tool"}},
					},
				}),
			}
			sort.Slice(updates, func(i, j int) bool { return updates[i].Identity.Sequence < updates[j].Identity.Sequence })
			for _, update := range updates {
				wantNew := update.Update != nil && update.Update.ToolCall != nil
				if appended, isNew, err := state.AppendUpdateIfNew(ctx, update); err != nil || isNew != wantNew || (isNew && appended == nil) || (!isNew && appended != nil) {
					t.Fatalf("append update %d = %#v new=%t err=%v", update.Identity.Sequence, appended, isNew, err)
				}
			}
			terminal := testTerminalEvent(4, now.Add(4*time.Millisecond))
			if err := state.AppendBufferedStreamsIfNew(ctx, &terminal, test.assistantSeq, "assistant", false); err != nil {
				t.Fatal(err)
			}
			if err := state.AppendBufferedStreamsIfNew(ctx, &terminal, test.assistantSeq, "assistant", false); err != nil {
				t.Fatalf("repeat buffered stream append: %v", err)
			}

			listed := listJournalEvents(t, ctx, eventStore)
			if len(listed) != len(test.wantTypes) {
				t.Fatalf("buffered stream events = %#v", listed)
			}
			for i, wantType := range test.wantTypes {
				if listed[i].Type != wantType {
					t.Fatalf("buffered stream event %d type = %q, want %q", i, listed[i].Type, wantType)
				}
			}
		})
	}
}

func TestJournalBuffersExplicitEmptyToolSnapshot(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-clear", Status: harnessv2.ToolCallStatusInProgress, ContentReplace: true,
		},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || !isNew || appended == nil {
		t.Fatalf("append empty tool snapshot = %#v new=%t err=%v", appended, isNew, err)
	}
	if listed := listJournalEvents(t, ctx, eventStore); len(listed) != 1 || listed[0].Type != executionevents.ExecutionEventTypeToolCallStarted {
		t.Fatalf("empty snapshot start = %#v", listed)
	}
	if err := state.AppendToolStreamClosuresIfNew(ctx); err != nil {
		t.Fatal(err)
	}
	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 || listed[1].Type != executionevents.ExecutionEventTypeToolCallFailed || listed[1].ContentText != "" {
		t.Fatalf("empty snapshot closure = %#v", listed)
	}
}

func TestJournalTerminalizesContentFreeToolAfterPersistedStart(t *testing.T) {
	ctx := context.Background()
	base := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: base, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	event := testUpdateEvent(2, time.Now().UTC(), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCall,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: testJournalOpenToolCallID, Title: testJournalToolTitle, Kind: testToolKindShell, Status: harnessv2.ToolCallStatusPending,
		},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || !isNew || appended == nil {
		t.Fatalf("append content-free tool start = %#v new=%t err=%v", appended, isNew, err)
	}

	faulting := &faultingAppendEventStore{
		ExecutionEventStore: base,
		faults:              []appendFault{{persistBeforeError: true, err: errors.New("ambiguous closure append")}},
	}
	state.journal.EventStore = faulting
	if err := state.AppendToolStreamClosuresIfNew(ctx); err != nil {
		t.Fatal(err)
	}
	if faulting.appendCalls != 1 {
		t.Fatalf("closure append calls = %d, want 1", faulting.appendCalls)
	}

	listed := listJournalEvents(t, ctx, base)
	if len(listed) != 2 || listed[0].Type != executionevents.ExecutionEventTypeToolCallStarted ||
		listed[1].Type != executionevents.ExecutionEventTypeToolCallFailed || listed[0].ToolCallID != listed[1].ToolCallID {
		t.Fatalf("content-free tool lifecycle = %#v", listed)
	}
	var content map[string]any
	if err := json.Unmarshal(listed[1].Content, &content); err != nil {
		t.Fatal(err)
	}
	if content["journalKind"] != mappedToolStreamClosureKind {
		t.Fatalf("closure journal kind = %#v", content["journalKind"])
	}
	key := mappedToolTerminalKey(mappedUpdateIdentity(event), listed[1].ToolCallID)
	if persisted, err := state.journal.hasPersistedIdentity(ctx, key); err != nil || !persisted {
		t.Fatalf("closure persisted=%t err=%v", persisted, err)
	}
}

func TestJournalReplacesACPToolContentSnapshots(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	for index, snapshot := range []string{"a", "ab"} {
		event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateToolCallUpdate,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: "call-snapshot", Status: harnessv2.ToolCallStatusInProgress,
				Content:        []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: snapshot}},
				ContentReplace: true,
			},
		})
		wantNew := index == 0
		if appended, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || isNew != wantNew || (isNew && appended == nil) || (!isNew && appended != nil) {
			t.Fatalf("content snapshot %d = %#v new=%t err=%v", index, appended, isNew, err)
		}
	}
	completed := testUpdateEvent(4, now.Add(2*time.Millisecond), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-snapshot", Status: harnessv2.ToolCallStatusCompleted,
		},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, completed); err != nil || !isNew || appended == nil {
		t.Fatalf("completed tool = %#v new=%t err=%v", appended, isNew, err)
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 2 || listed[0].Type != executionevents.ExecutionEventTypeToolCallStarted ||
		listed[1].Type != executionevents.ExecutionEventTypeToolCallCompleted || listed[1].ContentText != "ab" {
		t.Fatalf("persisted tool snapshot events = %#v", listed)
	}
}

func TestJournalRedactsToolMetadataSplitAcrossLifecycleUpdates(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	suffix := strings.Repeat("c", 24)
	credential := testJournalSecretPrefix + suffix
	now := time.Now().UTC()
	updates := []harnessv2.UpdateEvent{
		{Kind: harnessv2.UpdateToolCall, ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: testJournalMetadataToolCallID, Title: testJournalSecretPrefix, Status: harnessv2.ToolCallStatusPending,
		}},
		{Kind: harnessv2.UpdateToolCallUpdate, ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: testJournalMetadataToolCallID, Kind: suffix, Status: harnessv2.ToolCallStatusInProgress,
		}},
		{Kind: harnessv2.UpdateToolCallUpdate, ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: testJournalMetadataToolCallID, Status: harnessv2.ToolCallStatusCompleted,
		}},
	}
	for index, update := range updates {
		if _, isNew, err := state.AppendUpdateIfNew(
			ctx,
			testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), update),
		); err != nil || !isNew {
			t.Fatalf("append metadata update %d new=%t err=%v", index, isNew, err)
		}
	}

	listed := listJournalEvents(t, ctx, eventStore)
	if len(listed) != 3 {
		t.Fatalf("persisted tool events = %d, want 3", len(listed))
	}
	var persisted strings.Builder
	for _, event := range listed {
		persisted.WriteString(event.Summary)
		persisted.WriteString(event.ToolName)
		persisted.Write(event.Content)
	}
	if strings.Contains(persisted.String(), credential) || strings.Contains(persisted.String(), suffix) {
		t.Fatalf("streamed tool metadata remained reconstructable: %q", persisted.String())
	}
	for _, event := range listed[:2] {
		if event.ToolName != "" || !strings.Contains(string(event.Content), "metadataOmitted") {
			t.Fatalf("nonterminal tool metadata was persisted: %#v", event)
		}
	}
	terminal := listed[2]
	var content map[string]any
	if err := json.Unmarshal(terminal.Content, &content); err != nil {
		t.Fatal(err)
	}
	if terminal.ToolName != executionevents.ExecutionEventRedactedValue ||
		terminal.Summary != executionevents.ExecutionEventRedactedValue ||
		content["title"] != executionevents.ExecutionEventRedactedValue ||
		content["toolKind"] != executionevents.ExecutionEventRedactedValue {
		t.Fatalf("terminal tool metadata = %#v", terminal)
	}
}

func TestJournalOmitsOversizedToolStreamContent(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	credential := testJournalSecretPrefix + strings.Repeat("c", 24)
	now := time.Now().UTC()
	for index := range 9 {
		status := harnessv2.ToolCallStatusInProgress
		if index == 8 {
			status = harnessv2.ToolCallStatusCompleted
		}
		content := strings.Repeat("x", harnessv2.MaxProtocolStringBytes)
		switch index {
		case 7:
			content = strings.Repeat("x", harnessv2.MaxProtocolStringBytes-8) + credential[:8]
		case 8:
			content = credential[8:]
		}
		event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateToolCallUpdate,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: "call-large", Kind: testToolKindShell, Status: status,
				Content: []harnessv2.ContentBlock{{
					Type: harnessv2.ContentBlockText,
					Text: content,
				}},
			},
		})
		if _, _, err := state.AppendUpdateIfNew(ctx, event); err != nil {
			t.Fatalf("append oversized tool update %d: %v", index, err)
		}
	}

	listed := listJournalEvents(t, ctx, eventStore)
	completed := listed[len(listed)-1]
	encoded := completed.Summary + completed.ContentText + string(completed.Content)
	if completed.ContentText != "" || strings.Contains(encoded, credential[:8]) ||
		completed.Truncation == nil || !completed.Truncation.ContentTextTruncated ||
		!strings.Contains(string(completed.Content), streamedTextTruncatedOrOmittedReason) {
		t.Fatalf("oversized completed tool event = %#v", completed)
	}
}

func TestJournalOmitsExcessOpenToolAccumulatorWithoutFailingPrompt(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for index := range maxOpenToolAccumulators {
		event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateToolCall,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: fmt.Sprintf("call-open-%d", index), Status: harnessv2.ToolCallStatusPending,
			},
		})
		if _, _, err := state.AppendUpdateIfNew(ctx, event); err != nil {
			t.Fatalf("append open tool %d: %v", index, err)
		}
	}
	overflow := testUpdateEvent(uint64(maxOpenToolAccumulators+2), now.Add(time.Second), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCall,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-open-overflow", Status: harnessv2.ToolCallStatusPending,
		},
	})
	for attempt := range 2 {
		if appended, isNew, err := state.AppendUpdateIfNew(ctx, overflow); err != nil || isNew || appended != nil {
			t.Fatalf("open-tool overflow attempt %d = %#v new=%t err=%v", attempt, appended, isNew, err)
		}
	}
	if len(state.toolText) != maxOpenToolAccumulators {
		t.Fatalf("open tool accumulators = %d, want %d", len(state.toolText), maxOpenToolAccumulators)
	}

	terminal := testUpdateEvent(uint64(maxOpenToolAccumulators+3), now.Add(2*time.Second), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-open-overflow", Kind: testToolKindShell, Status: harnessv2.ToolCallStatusCompleted,
			Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: "final output"}}, ContentReplace: true,
		},
	})
	completed, isNew, err := state.AppendUpdateIfNew(ctx, terminal)
	if err != nil || !isNew || completed == nil || completed.Type != executionevents.ExecutionEventTypeToolCallCompleted ||
		completed.ContentText != "final output" || completed.ToolName != testToolKindShell {
		t.Fatalf("complete transient open tool = %#v new=%t err=%v", completed, isNew, err)
	}
	if len(state.toolText) != maxOpenToolAccumulators {
		t.Fatalf("terminal snapshot retained an accumulator: %d", len(state.toolText))
	}
}

func TestJournalMarksContentOmittedForOverflowToolTerminalWithoutContent(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for index := range maxOpenToolAccumulators {
		event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateToolCall,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: fmt.Sprintf("call-open-%d", index), Status: harnessv2.ToolCallStatusPending,
			},
		})
		if _, _, err := state.AppendUpdateIfNew(ctx, event); err != nil {
			t.Fatalf("append open tool %d: %v", index, err)
		}
	}
	overflow := testUpdateEvent(uint64(maxOpenToolAccumulators+2), now.Add(time.Second), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-open-overflow-omitted", Status: harnessv2.ToolCallStatusInProgress,
			Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: "dropped output"}},
		},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, overflow); err != nil || isNew || appended != nil {
		t.Fatalf("overflow update = %#v new=%t err=%v", appended, isNew, err)
	}

	terminal := testUpdateEvent(uint64(maxOpenToolAccumulators+3), now.Add(2*time.Second), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-open-overflow-omitted", Kind: testToolKindShell, Status: harnessv2.ToolCallStatusCompleted,
		},
	})
	completed, isNew, err := state.AppendUpdateIfNew(ctx, terminal)
	if err != nil || !isNew || completed == nil {
		t.Fatalf("overflow terminal = %#v new=%t err=%v", completed, isNew, err)
	}
	if completed.ContentText != "" || completed.Truncation == nil || !completed.Truncation.ContentTextTruncated ||
		!strings.Contains(string(completed.Content), streamedTextTruncatedOrOmittedReason) {
		t.Fatalf("overflow terminal omission = %#v", completed)
	}
}

func TestJournalDeduplicationStateUsesSequenceHighWater(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	updateCount := store.MaxExecutionEventLimit*2 + 1
	var first harnessv2.Event
	for index := range updateCount {
		event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateToolCallUpdate,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: fmt.Sprintf("call-high-water-%d", index), Status: harnessv2.ToolCallStatusCompleted,
			},
		})
		if index == 0 {
			first = event
		}
		if appended, isNew, err := state.AppendUpdateIfNew(ctx, event); err != nil || !isNew || appended == nil {
			t.Fatalf("append update %d = %#v new=%t err=%v", index, appended, isNew, err)
		}
	}

	wantSequence := uint64(updateCount + 1)
	if state.processedSequence != wantSequence || state.aggregatedSequence != wantSequence {
		t.Fatalf(
			"sequence state processed=%d aggregated=%d, want %d",
			state.processedSequence, state.aggregatedSequence, wantSequence,
		)
	}
	if len(state.toolText) != 0 || len(state.toolClosureSequences) != 0 {
		t.Fatalf("bounded journal state tools=%d closures=%d", len(state.toolText), len(state.toolClosureSequences))
	}
	if duplicate, isNew, err := state.AppendUpdateIfNew(ctx, first); err != nil || isNew || duplicate != nil {
		t.Fatalf("old duplicate = %#v new=%t err=%v", duplicate, isNew, err)
	}
}

func TestJournalDegradesAggregateToolContentOverflow(t *testing.T) {
	ctx := context.Background()
	state, err := (Journal{EventStore: storetest.NewFakeExecutionEventStore(), MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	content := strings.Repeat("🙂", executionevents.MaxExecutionEventContentTextChars)
	contentBytes := len(content)
	now := time.Now().UTC()
	for index := 0; index < maxBufferedToolContentBytes/contentBytes; index++ {
		event := testUpdateEvent(uint64(index+2), now.Add(time.Duration(index)*time.Millisecond), harnessv2.UpdateEvent{
			Kind: harnessv2.UpdateToolCallUpdate,
			ToolCall: &harnessv2.ToolCallUpdate{
				ToolCallID: fmt.Sprintf("call-buffer-%d", index), Status: harnessv2.ToolCallStatusInProgress,
				Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: content}}, ContentReplace: true,
			},
		})
		if _, _, err := state.AppendUpdateIfNew(ctx, event); err != nil {
			t.Fatalf("append buffered tool %d: %v", index, err)
		}
	}
	overflow := testUpdateEvent(100, now.Add(time.Second), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-buffer-overflow", Status: harnessv2.ToolCallStatusInProgress,
			Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: content}}, ContentReplace: true,
		},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, overflow); err != nil || !isNew || appended == nil ||
		appended.Type != executionevents.ExecutionEventTypeToolCallStarted {
		t.Fatalf("aggregate tool overflow = %#v new=%t err=%v", appended, isNew, err)
	}
	if state.toolBufferedBytes > maxBufferedToolContentBytes {
		t.Fatalf("buffered tool bytes = %d, max %d", state.toolBufferedBytes, maxBufferedToolContentBytes)
	}
	terminal := testUpdateEvent(101, now.Add(2*time.Second), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-buffer-overflow", Status: harnessv2.ToolCallStatusCompleted,
		},
	})
	completed, isNew, err := state.AppendUpdateIfNew(ctx, terminal)
	if err != nil || !isNew || completed == nil {
		t.Fatalf("complete overflowed tool = %#v new=%t err=%v", completed, isNew, err)
	}
	if completed.ContentText != "" || completed.Truncation == nil || !completed.Truncation.ContentTextTruncated ||
		!strings.Contains(string(completed.Content), streamedTextTruncatedOrOmittedReason) {
		t.Fatalf("degraded tool completion = %#v", completed)
	}
}

func TestJournalPersistsUpstreamOmittedToolContentAsTruncated(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	state, err := (Journal{EventStore: eventStore, MapContext: testMapContext()}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	buffered := testUpdateEvent(2, now, harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-upstream-omitted", Status: harnessv2.ToolCallStatusInProgress,
			Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: "previous output"}}, ContentReplace: true,
		},
	})
	if appended, isNew, err := state.AppendUpdateIfNew(ctx, buffered); err != nil || !isNew || appended == nil ||
		appended.Type != executionevents.ExecutionEventTypeToolCallStarted {
		t.Fatalf("buffer tool content = %#v new=%t err=%v", appended, isNew, err)
	}
	terminal := testUpdateEvent(3, now.Add(time.Second), harnessv2.UpdateEvent{
		Kind: harnessv2.UpdateToolCallUpdate,
		ToolCall: &harnessv2.ToolCallUpdate{
			ToolCallID: "call-upstream-omitted", Status: harnessv2.ToolCallStatusCompleted, ContentOmitted: true,
		},
	})
	completed, isNew, err := state.AppendUpdateIfNew(ctx, terminal)
	if err != nil || !isNew || completed == nil {
		t.Fatalf("complete omitted tool = %#v new=%t err=%v", completed, isNew, err)
	}
	if completed.ContentText != "" || completed.Truncation == nil || !completed.Truncation.ContentTextTruncated ||
		!strings.Contains(string(completed.Content), streamedTextTruncatedOrOmittedReason) {
		t.Fatalf("omitted tool completion = %#v", completed)
	}
}

func TestToolContentFragmentPreservesURIOnlyResourceLink(t *testing.T) {
	got, multipleBlocks := toolContentFragment([]harnessv2.ContentBlock{{
		Type: harnessv2.ContentBlockResourceLink,
		URI:  "https://example.com/output.txt?X-Amz-Credential=secret&X-Amz-Signature=value#download",
	}})
	if got != "resource: https://example.com/output.txt" || multipleBlocks {
		t.Fatalf("resource-link fragment = %q multipleBlocks=%t", got, multipleBlocks)
	}
}

func TestToolContentFragmentSanitizesURLShapedResourceName(t *testing.T) {
	got, multipleBlocks := toolContentFragment([]harnessv2.ContentBlock{{
		Type: harnessv2.ContentBlockResourceLink,
		Name: "https://account.blob.core.windows.net/output.txt?sp=r&sig=usable-secret#download",
		URI:  "https://fallback.example.com/output.txt",
	}})
	if got != "resource: https://account.blob.core.windows.net/output.txt" || multipleBlocks ||
		strings.Contains(got, "sig=") || strings.Contains(got, "usable-secret") {
		t.Fatalf("resource-link name fragment = %q multipleBlocks=%t", got, multipleBlocks)
	}
}

func TestToolContentFragmentSanitizesRelativeResourceNames(t *testing.T) {
	for _, test := range []struct {
		name string
		want string
	}{
		{name: "//account.blob.core.windows.net/output.txt?sig=usable-secret#download", want: "resource: //account.blob.core.windows.net/output.txt"},
		{name: "output.txt?sig=usable-secret#download", want: "resource: output.txt"},
	} {
		got, multipleBlocks := toolContentFragment([]harnessv2.ContentBlock{{
			Type: harnessv2.ContentBlockResourceLink,
			Name: test.name,
			URI:  "https://fallback.example.com/output.txt",
		}})
		if got != test.want || multipleBlocks || strings.Contains(got, "sig=") || strings.Contains(got, "usable-secret") {
			t.Fatalf("resource-link name %q fragment = %q multipleBlocks=%t", test.name, got, multipleBlocks)
		}
	}
}

func TestToolContentFragmentOmitsOpaqueResourceURI(t *testing.T) {
	got, multipleBlocks := toolContentFragment([]harnessv2.ContentBlock{{
		Type: harnessv2.ContentBlockResourceLink,
		URI:  "data:application/json,%7B%22private%22%3A%22sensitive-payload%22%7D",
	}})
	if got != "" || multipleBlocks {
		t.Fatalf("opaque resource-link fragment = %q multipleBlocks=%t", got, multipleBlocks)
	}
}

func listJournalEvents(t *testing.T, ctx context.Context, eventStore store.ExecutionEventStore) []store.ExecutionEvent {
	t.Helper()
	listed, err := eventStore.ListExecutionEvents(ctx, store.ExecutionEventFilter{
		Namespace: testJournalNamespace, StreamType: store.ExecutionEventStreamTypeTask, StreamID: testJournalTaskName, Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	return listed
}

type appendFault struct {
	persistBeforeError bool
	err                error
}

type faultingAppendEventStore struct {
	store.ExecutionEventStore
	faults      []appendFault
	appendCalls int
	listFaultAt int
	listErr     error
	listCalls   int
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

func (s *faultingAppendEventStore) AppendExecutionEvent(
	ctx context.Context,
	event *store.ExecutionEvent,
) (*store.ExecutionEvent, error) {
	s.appendCalls++
	if s.appendCalls <= len(s.faults) {
		fault := s.faults[s.appendCalls-1]
		if fault.persistBeforeError {
			if _, err := s.ExecutionEventStore.AppendExecutionEvent(ctx, event); err != nil {
				return nil, err
			}
		}
		return nil, fault.err
	}
	return s.ExecutionEventStore.AppendExecutionEvent(ctx, event)
}

func (s *faultingAppendEventStore) AppendExecutionEventIfAbsent(
	ctx context.Context,
	event *store.ExecutionEvent,
	dedupeKey string,
) (*store.ExecutionEvent, bool, error) {
	s.appendCalls++
	deduplicatingStore, ok := s.ExecutionEventStore.(store.DeduplicatingExecutionEventStore)
	if !ok {
		return nil, false, errors.New("underlying store does not support atomic deduplication")
	}
	if s.appendCalls <= len(s.faults) {
		fault := s.faults[s.appendCalls-1]
		if fault.persistBeforeError {
			if _, _, err := deduplicatingStore.AppendExecutionEventIfAbsent(ctx, event, dedupeKey); err != nil {
				return nil, false, err
			}
		}
		return nil, false, fault.err
	}
	return deduplicatingStore.AppendExecutionEventIfAbsent(ctx, event, dedupeKey)
}

func (s *faultingAppendEventStore) ListExecutionEvents(
	ctx context.Context,
	filter store.ExecutionEventFilter,
) ([]store.ExecutionEvent, error) {
	s.listCalls++
	if s.listCalls == s.listFaultAt {
		return nil, s.listErr
	}
	return s.ExecutionEventStore.ListExecutionEvents(ctx, filter)
}

func TestFindPromptTerminalCarriesJournaledFailureClassification(t *testing.T) {
	ctx := context.Background()
	eventStore := storetest.NewFakeExecutionEventStore()
	journal := Journal{EventStore: eventStore, MapContext: testMapContext()}
	state, err := journal.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	accepted := testUpdateEvent(1, now, harnessv2.UpdateEvent{})
	accepted.Type = harnessv2.EventAccepted
	accepted.Update = nil
	accepted.Accepted = &harnessv2.AcceptedEvent{
		AcceptedAt: now,
		Lease:      harnessv2.PromptLease{Generation: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute)},
		ACPVersion: harnessv2.ACPProfileV1,
	}
	if _, isNew, err := state.AppendPromptLifecycleIfNew(ctx, accepted); err != nil || !isNew {
		t.Fatalf("append accepted: new=%t err=%v", isNew, err)
	}
	terminal := testUpdateEvent(2, now.Add(time.Millisecond), harnessv2.UpdateEvent{})
	terminal.Type = harnessv2.EventFailed
	terminal.Update = nil
	terminal.Failed = &harnessv2.FailedEvent{
		StopReason: harnessv2.ACPStopReasonRefusal, Code: "provider_upstream_error",
		Message: "provider upstream returned HTTP 402 for the final inference request",
	}
	if _, isNew, err := state.AppendPromptLifecycleIfNew(ctx, terminal); err != nil || !isNew {
		t.Fatalf("append failed terminal: new=%t err=%v", isNew, err)
	}
	recoveryJournal := journal
	recoveryJournal.RecoveryIdentity = mappedUpdateIdentity(accepted)
	evidence, err := recoveryJournal.FindPromptTerminal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if evidence == nil || evidence.TerminalEvent != harnessv2.EventFailed ||
		evidence.FailureCode != "provider_upstream_error" ||
		evidence.FailureMessage != "provider upstream returned HTTP 402 for the final inference request" {
		t.Fatalf("failed terminal recovery evidence = %#v", evidence)
	}
}
