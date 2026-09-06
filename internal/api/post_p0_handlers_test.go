package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/approvals"
	"github.com/orka-agents/orka/internal/events"
	forkcontext "github.com/orka-agents/orka/internal/fork"
	gatewayruntime "github.com/orka-agents/orka/internal/gateway"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	storetest "github.com/orka-agents/orka/internal/store/storetest"
	"github.com/orka-agents/orka/internal/tasktrace"
)

type postP0FakeSessionStore struct {
	records          map[string]*store.SessionRecord
	getCalls         int
	deleteOnGetAfter int
}

func (f *postP0FakeSessionStore) key(namespace, name string) string { return namespace + "/" + name }
func (f *postP0FakeSessionStore) CreateSession(ctx context.Context, session *store.SessionRecord) error {
	f.records[f.key(session.Namespace, session.Name)] = session
	return nil
}
func (f *postP0FakeSessionStore) GetSession(ctx context.Context, namespace, name string) (*store.SessionRecord, error) {
	f.getCalls++
	if f.deleteOnGetAfter > 0 && f.getCalls >= f.deleteOnGetAfter {
		delete(f.records, f.key(namespace, name))
	}
	if s := f.records[f.key(namespace, name)]; s != nil {
		return s, nil
	}
	return nil, store.ErrNotFound
}
func (f *postP0FakeSessionStore) ListSessions(ctx context.Context, namespace string) ([]store.SessionMetadata, error) {
	return nil, nil
}
func (f *postP0FakeSessionStore) ListSessionsPage(ctx context.Context, namespace, afterName string, limit int, excludeType string) ([]store.SessionMetadata, bool, error) {
	return nil, false, nil
}
func (f *postP0FakeSessionStore) DeleteSession(ctx context.Context, namespace, name string) error {
	delete(f.records, f.key(namespace, name))
	return nil
}
func (f *postP0FakeSessionStore) AcquireLock(ctx context.Context, namespace, name, taskName, taskUID string) error {
	return nil
}
func (f *postP0FakeSessionStore) ReleaseLock(ctx context.Context, namespace, name, taskName, taskUID string) error {
	return nil
}
func (f *postP0FakeSessionStore) IsLocked(ctx context.Context, namespace, name, currentTask, currentTaskUID string) (bool, error) {
	return false, nil
}
func (f *postP0FakeSessionStore) AppendMessages(ctx context.Context, namespace, name string, messages []store.SessionMessage) error {
	return nil
}
func (f *postP0FakeSessionStore) LoadTranscript(ctx context.Context, namespace, name string, maxMessages int) ([]store.SessionMessage, error) {
	return nil, nil
}
func (f *postP0FakeSessionStore) LoadTranscriptThrough(ctx context.Context, namespace, name, throughMessageID string, maxMessages int) ([]store.SessionMessage, error) {
	return f.LoadTranscript(ctx, namespace, name, maxMessages)
}
func (f *postP0FakeSessionStore) SearchTranscript(ctx context.Context, filter store.TranscriptSearchFilter) ([]store.TranscriptSearchResult, error) {
	return nil, nil
}
func (f *postP0FakeSessionStore) UpdateTokenCounts(ctx context.Context, namespace, name string, inputTokens, outputTokens int) error {
	return nil
}

type postP0FailingAppendEventStore struct {
	store.ExecutionEventStore
	failType  string
	failAfter int
	seen      int
	failed    bool
}

func (s *postP0FailingAppendEventStore) AppendExecutionEvent(
	ctx context.Context,
	event *store.ExecutionEvent,
) (*store.ExecutionEvent, error) {
	if event != nil && event.Type == s.failType {
		s.seen++
		failAfter := s.failAfter
		if failAfter <= 0 {
			failAfter = 1
		}
		if s.seen >= failAfter {
			s.failed = true
			return nil, fmt.Errorf("injected append failure for %s", event.Type)
		}
	}
	return s.ExecutionEventStore.AppendExecutionEvent(ctx, event)
}

func TestListSessionEventsAggregatesTaskEvents(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	appendSessionEvent(t, eventStore, taskTimelineTestTaskName, events.ExecutionEventTypeTaskStarted, now)
	appendSessionEvent(t, eventStore, "task-b", events.ExecutionEventTypeWorkerStarted, now.Add(time.Second))
	appendSessionEvent(t, eventStore, taskTimelineTestTaskName, events.ExecutionEventTypeTaskSucceeded, now.Add(2*time.Second))

	h, app := setupPostP0Handlers(t, eventStore, &postP0FakeSessionStore{records: map[string]*store.SessionRecord{"default/session-1": {Namespace: "default", Name: "session-1"}}})
	app.Get("/api/v1/sessions/:id/events", h.ListSessionEvents)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/session-1/events?namespace=default&after=1", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var listed ListSessionExecutionEventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if listed.LatestSeq != 3 || len(listed.Events) != 2 || listed.Events[0].Seq != 2 || listed.Events[0].TaskName != "task-b" || listed.Events[0].TaskSeq != 1 {
		t.Fatalf("listed = %#v", listed)
	}
}

func TestSessionEventEndpointsHideGatewaySessions(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	h, app := setupPostP0Handlers(t, eventStore, &postP0FakeSessionStore{records: map[string]*store.SessionRecord{
		"default/gateway-session": {
			Namespace: "default", Name: "gateway-session", SessionType: store.SessionTypeGateway,
		},
	}})
	app.Get("/api/v1/sessions/:id/events", h.ListSessionEvents)
	app.Get("/api/v1/sessions/:id/stream", h.StreamSessionEvents)

	for _, path := range []string{
		"/api/v1/sessions/gateway-session/events?namespace=default",
		"/api/v1/sessions/gateway-session/stream?namespace=default",
	} {
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, path, nil))
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s status=%d, want %d", path, resp.StatusCode, http.StatusNotFound)
		}
	}
}

func TestStreamSessionEventsReconnect(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	appendSessionEvent(t, eventStore, taskTimelineTestTaskName, events.ExecutionEventTypeTaskStarted, now)
	appendSessionEvent(t, eventStore, "task-b", events.ExecutionEventTypeWorkerStarted, now.Add(time.Second))
	h, app := setupPostP0Handlers(t, eventStore, &postP0FakeSessionStore{records: map[string]*store.SessionRecord{"default/session-1": {Namespace: "default", Name: "session-1"}}})
	configureShortTaskEventStream(h)
	useCancelingContext(app, 20*time.Millisecond)
	app.Get("/api/v1/sessions/:id/stream", h.StreamSessionEvents)
	body := doStreamRequest(t, app, "/api/v1/sessions/session-1/stream?namespace=default&after=1")
	if strings.Contains(body, "id: 1\n") || !strings.Contains(body, "id: 2") || !strings.Contains(body, events.ExecutionEventTypeWorkerStarted) {
		t.Fatalf("session stream body = %q", body)
	}
}

func TestStreamSessionEventsCompletesWhenSessionDeleted(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	sessionStore := &postP0FakeSessionStore{
		records: map[string]*store.SessionRecord{
			"default/session-delete": {Namespace: "default", Name: "session-delete"},
		},
		deleteOnGetAfter: 2,
	}
	h, app := setupPostP0Handlers(t, eventStore, sessionStore)
	configureShortTaskEventStream(h)
	app.Get("/api/v1/sessions/:id/stream", h.StreamSessionEvents)
	body := doStreamRequest(t, app, "/api/v1/sessions/session-delete/stream?namespace=default")
	if !strings.Contains(body, "event: stream_complete") || !strings.Contains(body, "SessionDeleted") {
		t.Fatalf("session stream body = %q, want SessionDeleted completion", body)
	}
}

func TestGetTaskTraceAPI(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	appendTestTaskEvent(t, eventStore, "trace-task", events.ExecutionEventTypeTaskStarted)
	appendToolEvent(t, eventStore, "trace-task", events.ExecutionEventTypeToolCallStarted, "call-1")
	appendToolEvent(t, eventStore, "trace-task", events.ExecutionEventTypeToolCallCompleted, "call-1")
	appendTestTaskEvent(t, eventStore, "trace-task", events.ExecutionEventTypeTaskSucceeded)
	task := testTask("default", "trace-task")
	task.Spec.Type = corev1alpha1.TaskTypeAI
	task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	task.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
	h, app := setupTaskEventHandlers(t, eventStore, task)
	app.Get("/api/v1/tasks/:id/trace", h.GetTaskTrace)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/trace-task/trace?namespace=default", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var trace tasktrace.TaskTrace
	if err := json.NewDecoder(resp.Body).Decode(&trace); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if trace.LatestSeq != 4 || len(trace.ToolCalls) != 1 || trace.ToolCalls[0].Status != "completed" || !trace.Task.ResultAvailable {
		t.Fatalf("trace = %#v", trace)
	}
}

func TestGetTaskTraceAPIReadsFullEventStream(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "trace-long-task",
		TaskName:   "trace-long-task",
		Type:       events.ExecutionEventTypeTaskStarted,
		Summary:    "first event must remain visible",
	}); err != nil {
		t.Fatalf("append first event: %v", err)
	}
	for i := range forkcontext.DefaultMaxEvents + 25 {
		if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
			Namespace:   "default",
			StreamType:  store.ExecutionEventStreamTypeTask,
			StreamID:    "trace-long-task",
			TaskName:    "trace-long-task",
			Type:        events.ExecutionEventTypeModelMessage,
			Summary:     fmt.Sprintf("model message %03d", i),
			ContentText: fmt.Sprintf("message-%03d", i),
		}); err != nil {
			t.Fatalf("append model event %d: %v", i, err)
		}
	}
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "trace-long-task",
		TaskName:   "trace-long-task",
		Type:       events.ExecutionEventTypeTaskSucceeded,
		Summary:    "terminal event",
	}); err != nil {
		t.Fatalf("append terminal event: %v", err)
	}
	task := testTask("default", "trace-long-task")
	task.Spec.Type = corev1alpha1.TaskTypeAI
	task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	h, app := setupTaskEventHandlers(t, eventStore, task)
	app.Get("/api/v1/tasks/:id/trace", h.GetTaskTrace)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/trace-long-task/trace?namespace=default", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var trace tasktrace.TaskTrace
	if err := json.NewDecoder(resp.Body).Decode(&trace); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if trace.LatestSeq != int64(forkcontext.DefaultMaxEvents+27) {
		t.Fatalf("LatestSeq = %d", trace.LatestSeq)
	}
	if len(trace.Timeline) != forkcontext.DefaultMaxEvents+27 {
		t.Fatalf("timeline length = %d, want full event count %d", len(trace.Timeline), forkcontext.DefaultMaxEvents+27)
	}
	if trace.Timeline[0].Seq != 1 || trace.Timeline[0].Summary != "first event must remain visible" {
		t.Fatalf("first timeline event = %#v, want original first event", trace.Timeline[0])
	}
}

func TestGetTaskTraceAPIRejectsTooManyEvents(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	for i := range maxTaskTraceEvents + 1 {
		if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
			Namespace:  "default",
			StreamType: store.ExecutionEventStreamTypeTask,
			StreamID:   "trace-too-large-task",
			TaskName:   "trace-too-large-task",
			Type:       events.ExecutionEventTypeModelMessage,
			Summary:    fmt.Sprintf("event %d", i),
		}); err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
	}
	task := testTask("default", "trace-too-large-task")
	h, app := setupTaskEventHandlers(t, eventStore, task)
	app.Get("/api/v1/tasks/:id/trace", h.GetTaskTrace)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/trace-too-large-task/trace?namespace=default", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
}

func TestTaskApprovalDecisionAPIAppendsEvent(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	content, _ := json.Marshal(map[string]string{"approvalID": "approval-1", "action": "create_pr", "riskSummary": "opens a PR"})
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{Namespace: "default", StreamType: store.ExecutionEventStreamTypeTask, StreamID: "approval-task", TaskName: "approval-task", Type: events.ExecutionEventTypeApprovalRequested, Content: content}); err != nil {
		t.Fatal(err)
	}
	h, app := setupTaskEventHandlers(t, eventStore, testTask("default", "approval-task"))
	app.Get("/api/v1/tasks/:id/approvals", h.ListTaskApprovals)
	app.Post("/api/v1/tasks/:id/approvals/:approvalID/decision", h.DecideTaskApproval)

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/approval-task/approvals?namespace=default", nil)
	listResp, err := app.Test(listReq)
	if err != nil {
		t.Fatal(err)
	}
	var listed ListTaskApprovalsResponse
	if err := json.NewDecoder(listResp.Body).Decode(&listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Approvals) != 1 || listed.Approvals[0].Status != approvals.StatusPending {
		t.Fatalf("listed=%#v", listed)
	}

	decisionReq := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/approval-task/approvals/approval-1/decision?namespace=default", bytes.NewBufferString(`{"decision":"approve","reason":"safe"}`))
	decisionReq.Header.Set("Content-Type", "application/json")
	decisionResp, err := app.Test(decisionReq)
	if err != nil {
		t.Fatal(err)
	}
	if decisionResp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", decisionResp.StatusCode)
	}
	var approved approvals.Approval
	if err := json.NewDecoder(decisionResp.Body).Decode(&approved); err != nil {
		t.Fatal(err)
	}
	if approved.Status != approvals.StatusApproved || approved.DecisionReason != "safe" {
		t.Fatalf("approved=%#v", approved)
	}
	var patched corev1alpha1.Task
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "approval-task"}, &patched); err != nil {
		t.Fatalf("get patched task: %v", err)
	}
	if patched.Annotations[labels.AnnotationApprovalDecidedAt] == "" || patched.Annotations[labels.AnnotationApprovalDecisionID] != "approval-1" || patched.Annotations[labels.AnnotationApprovalDecisionStatus] != approvals.StatusApproved {
		t.Fatalf("approval decision annotations = %#v", patched.Annotations)
	}
}

// Regression: a very large decision reason truncates the JSON content (dropping
// the in-content approvalID), but the canonical approvalID is stamped into the
// stable top-level ToolCallID field, so approvals.Derive still resolves the
// decision and the terminal-conflict guard still fires on a second decision.
func TestTaskApprovalDecisionAPISurvivesContentTruncation(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	content, _ := json.Marshal(map[string]string{"approvalID": "approval-big", "action": "create_pr", "riskSummary": "opens a PR"})
	// ApprovalRequested carries approvalID only in content, with EMPTY ToolCallID
	// (the shape harness-emitted approvals actually have in production).
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "approval-big-task",
		TaskName:   "approval-big-task",
		Type:       events.ExecutionEventTypeApprovalRequested,
		Content:    content,
	}); err != nil {
		t.Fatal(err)
	}
	h, app := setupTaskEventHandlers(t, eventStore, testTask("default", "approval-big-task"))
	app.Post("/api/v1/tasks/:id/approvals/:approvalID/decision", h.DecideTaskApproval)

	// A reason larger than MaxExecutionEventContentJSONBytes (64 KiB) forces the
	// content object to be replaced by a content-free preview at persistence,
	// dropping the in-content approvalID.
	bigReason := strings.Repeat("x", 70*1024)
	body, _ := json.Marshal(map[string]string{"decision": "approve", "reason": bigReason})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/approval-big-task/approvals/approval-big/decision?namespace=default", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("decision status=%d, want 200", resp.StatusCode)
	}
	var approved approvals.Approval
	if err := json.NewDecoder(resp.Body).Decode(&approved); err != nil {
		t.Fatal(err)
	}
	// The approval must resolve to Approved, not remain stuck Pending.
	if approved.Status != approvals.StatusApproved {
		t.Fatalf("approval status = %q, want approved (truncated content must not orphan the decision)", approved.Status)
	}

	// The persisted terminal event must carry the canonical approvalID in the
	// stable ToolCallID field even though the content was truncated.
	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{Namespace: "default", StreamID: "approval-big-task"})
	if err != nil {
		t.Fatal(err)
	}
	decision := listed[len(listed)-1]
	if decision.Type != events.ExecutionEventTypeApprovalApproved {
		t.Fatalf("last event type = %q, want ApprovalApproved", decision.Type)
	}
	if decision.ToolCallID != "approval-big" {
		t.Fatalf("decision ToolCallID = %q, want canonical approvalID approval-big", decision.ToolCallID)
	}

	// A second, conflicting decision must be rejected (the one-decision conflict
	// guard relies on resolving the approval identity, which now survives).
	conflictBody, _ := json.Marshal(map[string]string{"decision": "decline", "reason": "changed mind"})
	conflictReq := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/approval-big-task/approvals/approval-big/decision?namespace=default", bytes.NewReader(conflictBody))
	conflictReq.Header.Set("Content-Type", "application/json")
	conflictResp, err := app.Test(conflictReq)
	if err != nil {
		t.Fatal(err)
	}
	if conflictResp.StatusCode != http.StatusConflict {
		t.Fatalf("conflicting decision status=%d, want 409", conflictResp.StatusCode)
	}
}

func TestTaskApprovalDecisionAPIOmitsDeletedSession(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	content, _ := json.Marshal(map[string]string{"approvalID": "approval-stale", "action": "create_pr"})
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:   "default",
		StreamType:  store.ExecutionEventStreamTypeTask,
		StreamID:    "approval-stale-task",
		TaskName:    "approval-stale-task",
		SessionName: "deleted-session",
		Type:        events.ExecutionEventTypeApprovalRequested,
		ToolCallID:  "approval-stale",
		Content:     content,
	}); err != nil {
		t.Fatal(err)
	}
	task := testTask("default", "approval-stale-task")
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "deleted-session"}
	h, app := setupTaskEventHandlers(t, eventStore, task)
	h.sessionStore = &postP0FakeSessionStore{records: map[string]*store.SessionRecord{}}
	app.Post("/api/v1/tasks/:id/approvals/:approvalID/decision", h.DecideTaskApproval)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/approval-stale-task/approvals/approval-stale/decision?namespace=default", bytes.NewBufferString(`{"decision":"approve"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{Namespace: "default", StreamID: "approval-stale-task"})
	if err != nil {
		t.Fatal(err)
	}
	decision := listed[len(listed)-1]
	if decision.Type != events.ExecutionEventTypeApprovalApproved || decision.SessionName != "" {
		t.Fatalf("decision event = %#v, want approved with no deleted session", decision)
	}
}

func TestTaskApprovalDecisionAPIRejectsTerminalTaskAndRecordsActor(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	content, _ := json.Marshal(map[string]string{"approvalID": "approval-1", "action": "create_pr"})
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "approval-task",
		TaskName:   "approval-task",
		Type:       events.ExecutionEventTypeApprovalRequested,
		Content:    content,
	}); err != nil {
		t.Fatal(err)
	}
	task := testTask("default", "approval-task")
	h, app := setupTaskEventHandlers(t, eventStore, task)
	app.Use(func(c fiber.Ctx) error {
		c.Locals(UserInfoContextKey, &UserInfo{Username: "reviewer-a"})
		return c.Next()
	})
	app.Post("/api/v1/tasks/:id/approvals/:approvalID/decision", h.DecideTaskApproval)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/tasks/approval-task/approvals/approval-1/decision?namespace=default",
		bytes.NewBufferString(`{"decision":"approve","reason":"safe"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var approved approvals.Approval
	if err := json.NewDecoder(resp.Body).Decode(&approved); err != nil {
		t.Fatal(err)
	}
	if approved.DecisionActor != "reviewer-a" {
		t.Fatalf("decision actor = %q, want reviewer-a", approved.DecisionActor)
	}

	content2, _ := json.Marshal(map[string]string{"approvalID": "approval-2", "action": "create_pr"})
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "approval-task",
		TaskName:   "approval-task",
		Type:       events.ExecutionEventTypeApprovalRequested,
		Content:    content2,
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "approval-task"}, task); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	if err := h.client.Update(context.Background(), task); err != nil {
		t.Fatalf("status update: %v", err)
	}
	req = httptest.NewRequest(
		http.MethodPost,
		"/api/v1/tasks/approval-task/approvals/approval-2/decision?namespace=default",
		bytes.NewBufferString(`{"decision":"approve"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	resp, err = app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("terminal task decision status=%d, want conflict", resp.StatusCode)
	}
}

func TestTaskApprovalDecisionAPIAppendsSessionEvent(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	content, _ := json.Marshal(map[string]string{"approvalID": "approval-1", "action": "create_pr"})
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "approval-task",
		TaskName:   "approval-task",
		Type:       events.ExecutionEventTypeApprovalRequested,
		Content:    content,
	}); err != nil {
		t.Fatal(err)
	}
	task := testTask("default", "approval-task")
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "session-1"}
	h, app := setupTaskEventHandlers(t, eventStore, task)
	app.Post("/api/v1/tasks/:id/approvals/:approvalID/decision", h.DecideTaskApproval)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/tasks/approval-task/approvals/approval-1/decision?namespace=default",
		bytes.NewBufferString(`{"decision":"approve"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	listed, latest, err := eventStore.ListSessionExecutionEvents(context.Background(), store.SessionExecutionEventFilter{
		Namespace: "default", SessionName: "session-1",
	})
	if err != nil {
		t.Fatalf("ListSessionExecutionEvents: %v", err)
	}
	if latest != 1 || len(listed) != 1 || listed[0].Type != events.ExecutionEventTypeApprovalApproved {
		t.Fatalf("latest=%d listed=%#v, want approved decision in session timeline", latest, listed)
	}
}

func TestTaskApprovalDecisionAPIPagesApprovalEvents(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	for i := range store.MaxExecutionEventLimit {
		content, _ := json.Marshal(map[string]string{"approvalID": fmt.Sprintf("approval-%d", i), "action": "noop"})
		if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
			Namespace:  "default",
			StreamType: store.ExecutionEventStreamTypeTask,
			StreamID:   "approval-task",
			TaskName:   "approval-task",
			Type:       events.ExecutionEventTypeApprovalRequested,
			Content:    content,
		}); err != nil {
			t.Fatal(err)
		}
	}
	targetContent, _ := json.Marshal(map[string]string{"approvalID": "approval-target", "action": "create_pr"})
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "approval-task",
		TaskName:   "approval-task",
		Type:       events.ExecutionEventTypeApprovalRequested,
		Content:    targetContent,
	}); err != nil {
		t.Fatal(err)
	}
	h, app := setupTaskEventHandlers(t, eventStore, testTask("default", "approval-task"))
	app.Post("/api/v1/tasks/:id/approvals/:approvalID/decision", h.DecideTaskApproval)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/tasks/approval-task/approvals/approval-target/decision?namespace=default",
		bytes.NewBufferString(`{"decision":"approve"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var approved approvals.Approval
	if err := json.NewDecoder(resp.Body).Decode(&approved); err != nil {
		t.Fatal(err)
	}
	if approved.ID != "approval-target" || approved.Status != approvals.StatusApproved {
		t.Fatalf("approved=%#v", approved)
	}
}

func TestForkTaskAPI(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	appendTestTaskEvent(t, eventStore, "source-task", events.ExecutionEventTypeTaskStarted)
	appendTestTaskEvent(t, eventStore, "source-task", events.ExecutionEventTypeWorkerStarted)
	source := testTask("default", "source-task")
	source.Spec.Type = corev1alpha1.TaskTypeAgent
	source.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "session-1"}
	h, app := setupTaskEventHandlers(t, eventStore, source)
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/source-task/fork?namespace=default", bytes.NewBufferString(`{"afterSeq":1,"newTaskName":"forked-task","prompt":"continue"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out ForkTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.NewTaskName != "forked-task" || out.AfterSeq != 1 || len(out.ForkContext.Events) != 1 {
		t.Fatalf("response=%#v", out)
	}
	created := &corev1alpha1.Task{}
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "forked-task"}, created); err != nil {
		t.Fatalf("get created: %v", err)
	}
	if created.Annotations[labels.AnnotationForkSourceTask] != "source-task" ||
		created.Annotations[labels.AnnotationForkSourceSeq] != "1" ||
		created.Annotations[labels.AnnotationDisableCoordinationToolInject] != queryTrue {
		t.Fatalf("created task annotations = %#v", created.Annotations)
	}
	if !strings.Contains(created.Spec.Prompt, "Fork context through execution event checkpoint") ||
		!strings.Contains(created.Spec.Prompt, events.ExecutionEventTypeTaskStarted) ||
		!strings.Contains(created.Spec.Prompt, "continue") {
		t.Fatalf("created prompt = %q, want fork context and continuation", created.Spec.Prompt)
	}
	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{Namespace: "default", StreamID: "forked-task"})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Type != events.ExecutionEventTypeTaskForkCreated {
		t.Fatalf("fork events = %#v", listed)
	}
	sessionEvents, latest, err := eventStore.ListSessionExecutionEvents(context.Background(), store.SessionExecutionEventFilter{
		Namespace: "default", SessionName: "session-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if latest != 3 || len(sessionEvents) != 3 {
		t.Fatalf("latest=%d sessionEvents=%#v, want fork request and created events in session timeline", latest, sessionEvents)
	}
}

func TestForkTaskAPIDetachesGatewaySession(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	appendTestTaskEvent(t, eventStore, "gateway-source", events.ExecutionEventTypeTaskStarted)
	source := testTask("default", "gateway-source")
	source.Spec.Type = corev1alpha1.TaskTypeAgent
	source.Spec.RequestedBy = &corev1alpha1.RequestedBy{Issuer: "gateway.orka.ai/default/namespace-uid/chat/gateway-uid"}
	source.Spec.Transaction = &corev1alpha1.TaskTransaction{ID: "gateway-transaction"}
	source.Labels = map[string]string{gatewayruntime.TaskGatewayEventLabel: "gateway-event"}
	source.Annotations = map[string]string{
		gatewayruntime.TaskGatewayEventAnnotation: "gateway-event",
		gatewayruntime.TaskGatewayNameAnnotation:  "chat",
	}
	source.Spec.SessionRef = &corev1alpha1.SessionReference{
		Name: "gateway-session", Create: false, Append: false,
		MaxMessages: 50, ThroughMessageID: "gateway:message:user", PromptIncluded: true,
	}
	sessionStore := &postP0FakeSessionStore{records: map[string]*store.SessionRecord{
		"default/gateway-session": {
			Namespace: "default", Name: "gateway-session", SessionType: store.SessionTypeGateway,
		},
	}}
	h, app := setupPostP0Handlers(t, eventStore, sessionStore, source,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: types.UID("namespace-uid")}},
		&gatewayv1alpha1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "default", UID: types.UID("gateway-uid")}},
	)
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/tasks/gateway-source/fork?namespace=default",
		bytes.NewBufferString(`{"afterSeq":1,"newTaskName":"gateway-fork","prompt":"continue independently"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	created := &corev1alpha1.Task{}
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "gateway-fork"}, created); err != nil {
		t.Fatalf("get created: %v", err)
	}
	if created.Spec.SessionRef != nil {
		t.Fatalf("created sessionRef = %#v, want gateway Session detached", created.Spec.SessionRef)
	}
	if created.Spec.RequestedBy != nil || created.Spec.Transaction != nil {
		t.Fatalf("created gateway provenance = requestedBy:%#v transaction:%#v, want detached", created.Spec.RequestedBy, created.Spec.Transaction)
	}
	if _, owned := gatewayruntime.TaskIdentity(created); owned {
		t.Fatalf("created fork still has gateway ownership: labels=%#v annotations=%#v", created.Labels, created.Annotations)
	}
	forkEvents, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace: "default", StreamID: "gateway-fork",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(forkEvents) != 1 || forkEvents[0].SessionName != "" {
		t.Fatalf("fork events = %#v, want no gateway Session association", forkEvents)
	}
	sessionEvents, _, err := eventStore.ListSessionExecutionEvents(context.Background(), store.SessionExecutionEventFilter{
		Namespace: "default", SessionName: "gateway-session",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sessionEvents) != 2 {
		t.Fatalf("gateway Session events = %#v, want only source fork request/created events", sessionEvents)
	}
}

func TestResolveForkSessionNamesDetachesGatewayOwnershipWhenSessionMissing(t *testing.T) {
	source := testTask("default", "gateway-missing-session-source")
	source.Spec.RequestedBy = &corev1alpha1.RequestedBy{Issuer: "gateway.orka.ai/default/namespace-uid/chat/gateway-uid"}
	source.Spec.Transaction = &corev1alpha1.TaskTransaction{ID: "gateway-transaction"}
	source.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "missing-gateway-session", PromptIncluded: true, ThroughMessageID: "gateway:event:user"}
	source.Labels = map[string]string{gatewayruntime.TaskGatewayEventLabel: "gateway-event"}
	source.Annotations = map[string]string{
		gatewayruntime.TaskGatewayEventAnnotation: "gateway-event",
		gatewayruntime.TaskGatewayNameAnnotation:  "chat",
	}
	forked := source.DeepCopy()
	forked.Name = "detached-fork"
	h := &Handlers{sessionStore: &postP0FakeSessionStore{records: map[string]*store.SessionRecord{}}}
	sourceSession, forkedSession, err := h.resolveForkSessionNames(context.Background(), "default", source, forked)
	if err != nil {
		t.Fatal(err)
	}
	if sourceSession != "missing-gateway-session" || forkedSession != "" {
		t.Fatalf("session names = (%q, %q)", sourceSession, forkedSession)
	}
	if forked.Spec.SessionRef != nil || forked.Spec.RequestedBy != nil || forked.Spec.Transaction != nil {
		t.Fatalf("fork retained gateway spec provenance: %+v", forked.Spec)
	}
	if _, owned := gatewayruntime.TaskIdentity(forked); owned {
		t.Fatalf("fork retained gateway metadata: labels=%#v annotations=%#v", forked.Labels, forked.Annotations)
	}
}

func TestForkTaskAPIDoesNotAppendRequestEventWhenCreateFails(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	appendTestTaskEvent(t, eventStore, "source-task", events.ExecutionEventTypeTaskStarted)
	source := testTask("default", "source-task")
	existingFork := testTask("default", "existing-fork")
	h, app := setupTaskEventHandlers(t, eventStore, source, existingFork)
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/source-task/fork?namespace=default", bytes.NewBufferString(`{"afterSeq":1,"newTaskName":"existing-fork","prompt":"continue"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status=%d, want 409", resp.StatusCode)
	}
	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{Namespace: "default", StreamID: "source-task", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range listed {
		if event.Type == events.ExecutionEventTypeTaskForkRequested {
			t.Fatalf("fork request event was appended despite create failure: %#v", listed)
		}
	}
}

// After the ordering fix, fork timeline events are best-effort and appended
// only AFTER the authoritative Task create succeeds. A failure appending the
// fork-request event must NOT fail the fork or roll back the created Task — the
// Task is the source of truth and is self-describing via its parent annotations.
func TestForkTaskAPISucceedsWhenRequestEventAppendFails(t *testing.T) {
	baseStore := storetest.NewFakeExecutionEventStore()
	appendTestTaskEvent(t, baseStore, "source-task", events.ExecutionEventTypeTaskStarted)
	eventStore := &postP0FailingAppendEventStore{
		ExecutionEventStore: baseStore,
		failType:            events.ExecutionEventTypeTaskForkRequested,
	}
	source := testTask("default", "source-task")
	h, app := setupTaskEventHandlers(t, eventStore, source)
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/source-task/fork?namespace=default", bytes.NewBufferString(`{"afterSeq":1,"newTaskName":"besteffort-fork","prompt":"continue"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d, want 201 (best-effort event append must not fail the fork)", resp.StatusCode)
	}
	if !eventStore.failed {
		t.Fatal("expected injected append failure to run")
	}
	// The authoritative Task must exist.
	created := &corev1alpha1.Task{}
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "besteffort-fork"}, created); err != nil {
		t.Fatalf("forked task should exist despite event append failure: %v", err)
	}
	if created.Annotations[labels.AnnotationForkSourceTask] != "source-task" {
		t.Fatalf("forked task missing fork lineage annotation: %#v", created.Annotations)
	}
}

func TestForkTaskAPIAddsContextToLegacyTopLevelAIPrompt(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	appendTestTaskEvent(t, eventStore, "legacy-ai-source", events.ExecutionEventTypeTaskStarted)
	source := testTask("default", "legacy-ai-source")
	source.Spec.Type = corev1alpha1.TaskTypeAI
	source.Spec.Prompt = "legacy top-level prompt"
	source.Spec.AI = &corev1alpha1.AISpec{Prompt: "ai prompt"}
	h, app := setupTaskEventHandlers(t, eventStore, source)
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/legacy-ai-source/fork?namespace=default", bytes.NewBufferString(`{"afterSeq":1,"newTaskName":"legacy-ai-fork"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	created := &corev1alpha1.Task{}
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "legacy-ai-fork"}, created); err != nil {
		t.Fatalf("get created: %v", err)
	}
	if !strings.Contains(created.Spec.Prompt, "Fork context through execution event checkpoint") ||
		!strings.Contains(created.Spec.Prompt, "legacy top-level prompt") {
		t.Fatalf("created top-level prompt = %q, want fork context and legacy prompt", created.Spec.Prompt)
	}
	if created.Spec.AI == nil ||
		!strings.Contains(created.Spec.AI.Prompt, "Fork context through execution event checkpoint") ||
		!strings.Contains(created.Spec.AI.Prompt, "ai prompt") {
		t.Fatalf("created AI prompt = %#v, want fork context and AI prompt", created.Spec.AI)
	}
}

func TestForkTaskAPIClearsScheduleForOneShotFork(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	appendTestTaskEvent(t, eventStore, "scheduled-source-task", events.ExecutionEventTypeTaskStarted)
	source := testTask("default", "scheduled-source-task")
	source.Spec.Type = corev1alpha1.TaskTypeAI
	source.Spec.AI = &corev1alpha1.AISpec{Prompt: "original"}
	source.Spec.Schedule = "*/5 * * * *"
	tz := "America/Boise"
	source.Spec.TimeZone = &tz
	suspend := true
	source.Spec.Suspend = &suspend
	h, app := setupTaskEventHandlers(t, eventStore, source)
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/scheduled-source-task/fork?namespace=default", bytes.NewBufferString(`{"afterSeq":1,"newTaskName":"scheduled-forked-task","prompt":"continue"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	created := &corev1alpha1.Task{}
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "scheduled-forked-task"}, created); err != nil {
		t.Fatalf("get created: %v", err)
	}
	if created.Spec.Schedule != "" || created.Spec.TimeZone != nil || created.Spec.Suspend != nil {
		t.Fatalf("created scheduling fields = schedule=%q timezone=%v suspend=%v, want cleared", created.Spec.Schedule, created.Spec.TimeZone, created.Spec.Suspend)
	}
	if created.Spec.AI == nil || !strings.Contains(created.Spec.AI.Prompt, "Fork context through execution event checkpoint") || !strings.Contains(created.Spec.AI.Prompt, "continue") {
		t.Fatalf("created AI prompt = %#v, want fork context and continuation", created.Spec.AI)
	}
}

func TestForkTaskAPIClearsDeletedSessionRef(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	appendTestTaskEvent(t, eventStore, "stale-source-task", events.ExecutionEventTypeTaskStarted)
	source := testTask("default", "stale-source-task")
	source.Spec.Type = corev1alpha1.TaskTypeAI
	source.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "deleted-session"}
	h, app := setupTaskEventHandlers(t, eventStore, source)
	h.sessionStore = &postP0FakeSessionStore{records: map[string]*store.SessionRecord{}}
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/stale-source-task/fork?namespace=default", bytes.NewBufferString(`{"afterSeq":1,"newTaskName":"stale-forked-task","prompt":"continue"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	created := &corev1alpha1.Task{}
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "stale-forked-task"}, created); err != nil {
		t.Fatalf("get created: %v", err)
	}
	if created.Spec.SessionRef != nil {
		t.Fatalf("created sessionRef = %#v, want stale deleted session cleared", created.Spec.SessionRef)
	}
	for _, streamID := range []string{"stale-source-task", "stale-forked-task"} {
		listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{Namespace: "default", StreamID: streamID})
		if err != nil {
			t.Fatal(err)
		}
		last := listed[len(listed)-1]
		if last.SessionName != "" {
			t.Fatalf("%s last event sessionName = %q, want omitted", streamID, last.SessionName)
		}
	}
}

func TestForkTaskAPIBoundsForkContextAndMarksTruncated(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	for range forkcontext.DefaultMaxEvents + 5 {
		appendTestTaskEvent(t, eventStore, "long-source-task", events.ExecutionEventTypeWorkerStarted)
	}
	source := testTask("default", "long-source-task")
	source.Spec.Type = corev1alpha1.TaskTypeAgent
	h, app := setupTaskEventHandlers(t, eventStore, source)
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/long-source-task/fork?namespace=default", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out ForkTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !out.ForkContext.Truncated || len(out.ForkContext.Events) != forkcontext.DefaultMaxEvents {
		t.Fatalf("fork context truncated=%v len=%d, want true and %d", out.ForkContext.Truncated, len(out.ForkContext.Events), forkcontext.DefaultMaxEvents)
	}
	if out.ForkContext.Events[0].Seq != 6 {
		t.Fatalf("first retained seq=%d, want 6", out.ForkContext.Events[0].Seq)
	}
}

func TestForkTaskAPIMarksCompatibilityScanTruncatedAfterCoalescing(t *testing.T) {
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
	source := testTask(defaultNamespace, taskTimelineTestTaskName)
	source.Spec.Type = corev1alpha1.TaskTypeAgent
	h, app := setupTaskEventHandlers(t, eventStore, source)
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/task-a/fork?namespace=default", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out ForkTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !out.ForkContext.Truncated || len(out.ForkContext.Events) != 0 {
		t.Fatalf("fork context = %#v", out.ForkContext)
	}
}

// Oversize forked Task must be rejected with 413 BEFORE any create, so it never
// orphans a fork event or returns a generic 500.
func TestForkTaskAPIRejectsOversizeForkWith413(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	appendTestTaskEvent(t, eventStore, "big-source", events.ExecutionEventTypeTaskStarted)
	source := testTask("default", "big-source")
	source.Spec.Type = corev1alpha1.TaskTypeAgent
	// A source prompt larger than the serialized-size budget guarantees the
	// forked Task exceeds the limit (the prompt is DeepCopied into the fork).
	source.Spec.Prompt = strings.Repeat("x", maxForkedTaskSerializedBytes+4096)
	h, app := setupTaskEventHandlers(t, eventStore, source)
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/big-source/fork?namespace=default", bytes.NewBufferString(`{"afterSeq":1}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d, want 413", resp.StatusCode)
	}
	// No fork event must have been appended for the oversize attempt.
	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{Namespace: "default", StreamID: "big-source", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range listed {
		if event.Type == events.ExecutionEventTypeTaskForkRequested {
			t.Fatalf("fork request event appended for oversize fork: %#v", listed)
		}
	}
}

// Bare auto-named forks (no Idempotency-Key) must mint DISTINCT Tasks, so a user
// can fork one checkpoint into several divergent branches. Idempotency is opt-in
// only — inferring it from (source, afterSeq) alone would silently alias a
// second, differently-prompted fork onto the first.
func TestForkTaskAPIAutoNameForksAreDistinct(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	appendTestTaskEvent(t, eventStore, "branch-source", events.ExecutionEventTypeTaskStarted)
	source := testTask("default", "branch-source")
	source.Spec.Type = corev1alpha1.TaskTypeAgent
	h, app := setupTaskEventHandlers(t, eventStore, source)
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)

	doFork := func(prompt string) ForkTaskResponse {
		t.Helper()
		body := fmt.Sprintf(`{"afterSeq":1,"prompt":%q}`, prompt)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/branch-source/fork?namespace=default", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("status=%d, want 201", resp.StatusCode)
		}
		var out ForkTaskResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	first := doFork("branch A")
	second := doFork("branch B")
	if first.NewTaskName == second.NewTaskName {
		t.Fatalf("two divergent auto-name forks got the same name %q (must be distinct)", first.NewTaskName)
	}
	taskList := &corev1alpha1.TaskList{}
	if err := h.client.List(context.Background(), taskList, ctrlclient.InNamespace("default"), ctrlclient.MatchingLabels{labels.LabelParentTask: labels.SelectorValue("branch-source")}); err != nil {
		t.Fatal(err)
	}
	if len(taskList.Items) != 2 {
		t.Fatalf("forked tasks = %d, want 2 distinct branches", len(taskList.Items))
	}
}

// With an explicit Idempotency-Key, a retried fork must resolve to the SAME Task
// (deterministic name + idempotent recovery), never a duplicate running fork.
func TestForkTaskAPIIdempotencyKeyRetryIsIdempotent(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	appendTestTaskEvent(t, eventStore, "idem-source", events.ExecutionEventTypeTaskStarted)
	source := testTask("default", "idem-source")
	source.Spec.Type = corev1alpha1.TaskTypeAgent
	h, app := setupTaskEventHandlers(t, eventStore, source)
	app.Post("/api/v1/tasks/:id/fork", h.ForkTask)

	doFork := func() ForkTaskResponse {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/idem-source/fork?namespace=default", bytes.NewBufferString(`{"afterSeq":1}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "fork-key-123")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d, want 201 or 200", resp.StatusCode)
		}
		var out ForkTaskResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	first := doFork()
	second := doFork()
	if first.NewTaskName != second.NewTaskName {
		t.Fatalf("idempotency-key fork retry produced different names %q vs %q", first.NewTaskName, second.NewTaskName)
	}
	taskList := &corev1alpha1.TaskList{}
	if err := h.client.List(context.Background(), taskList, ctrlclient.InNamespace("default"), ctrlclient.MatchingLabels{labels.LabelParentTask: labels.SelectorValue("idem-source")}); err != nil {
		t.Fatal(err)
	}
	if len(taskList.Items) != 1 {
		t.Fatalf("forked tasks = %d, want exactly 1 (idempotency-key retry must not duplicate)", len(taskList.Items))
	}
}

func TestGetTaskTraceAPIPagesBeyondEventLimit(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	appendToolEvent(t, eventStore, "long-trace-task", events.ExecutionEventTypeToolCallStarted, "early-call")
	for range store.MaxExecutionEventLimit {
		appendTestTaskEvent(t, eventStore, "long-trace-task", events.ExecutionEventTypeWorkerStarted)
	}
	appendTestTaskEvent(t, eventStore, "long-trace-task", events.ExecutionEventTypeTaskSucceeded)
	h, app := setupTaskEventHandlers(t, eventStore, testTask("default", "long-trace-task"))
	app.Get("/api/v1/tasks/:id/trace", h.GetTaskTrace)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/long-trace-task/trace?namespace=default", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var trace tasktrace.TaskTrace
	if err := json.NewDecoder(resp.Body).Decode(&trace); err != nil {
		t.Fatal(err)
	}
	if trace.LatestSeq != store.MaxExecutionEventLimit+2 || trace.TerminalEvent == nil {
		t.Fatalf("trace latest=%d terminal=%#v", trace.LatestSeq, trace.TerminalEvent)
	}
	if len(trace.Timeline) != store.MaxExecutionEventLimit+2 || trace.Timeline[0].Seq != 1 {
		t.Fatalf("trace timeline len=%d first=%#v, want full stream from seq 1", len(trace.Timeline), trace.Timeline[0])
	}
	if len(trace.ToolCalls) != 1 || trace.ToolCalls[0].ID != "early-call" || trace.ToolCalls[0].StartSeq != 1 {
		t.Fatalf("trace tool calls = %#v, want early tool call from seq 1", trace.ToolCalls)
	}
}

func setupPostP0Handlers(t *testing.T, eventStore store.ExecutionEventStore, sessionStore store.SessionStore, objs ...runtime.Object) (*Handlers, *fiber.App) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = gatewayv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	h := NewHandlers(HandlersConfig{Client: fakeClient, ExecutionEventStore: eventStore, SessionStore: sessionStore})
	app := fiber.New()
	return h, app
}

func appendSessionEvent(t *testing.T, eventStore store.ExecutionEventStore, taskName, typ string, createdAt time.Time) {
	t.Helper()
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{Namespace: "default", StreamType: store.ExecutionEventStreamTypeTask, StreamID: taskName, TaskName: taskName, SessionName: "session-1", Type: typ, Severity: events.ExecutionEventSeverityInfo, Summary: typ + " summary", CreatedAt: createdAt}); err != nil {
		t.Fatalf("AppendExecutionEvent: %v", err)
	}
}

func appendToolEvent(t *testing.T, eventStore store.ExecutionEventStore, taskName, typ, callID string) {
	t.Helper()
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{Namespace: "default", StreamType: store.ExecutionEventStreamTypeTask, StreamID: taskName, TaskName: taskName, Type: typ, Severity: events.ExecutionEventSeverityInfo, ToolName: "web_search", ToolCallID: callID}); err != nil {
		t.Fatalf("AppendExecutionEvent: %v", err)
	}
}

func TestTaskApprovalDecisionAPIIgnoresPassiveExpiryWithoutTerminalEvent(t *testing.T) {
	eventStore := storetest.NewFakeExecutionEventStore()
	content, _ := json.Marshal(map[string]string{
		"approvalID": "approval-expiring",
		"action":     "dispatch",
		"expiresAt":  time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	})
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "approval-expiring-task",
		TaskName:   "approval-expiring-task",
		Type:       events.ExecutionEventTypeApprovalRequested,
		Content:    content,
	}); err != nil {
		t.Fatal(err)
	}
	h, app := setupTaskEventHandlers(t, eventStore, testTask("default", "approval-expiring-task"))
	app.Post("/api/v1/tasks/:id/approvals/:approvalID/decision", h.DecideTaskApproval)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/tasks/approval-expiring-task/approvals/approval-expiring/decision?namespace=default",
		bytes.NewBufferString(`{"decision":"approve"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want OK", resp.StatusCode)
	}
}
