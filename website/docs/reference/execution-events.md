---
slug: /execution-events
description: "The event stream behind task timelines, traces, forks, and approvals."
---

# Execution events, session timelines, traces, forks, and approvals

Orka records durable execution events for normal Kubernetes Job-backed Tasks. The event system does **not** require Agent Substrate. Substrate-backed workspaces can emit the same event contract later, but task eventing, session aggregation, trace APIs, fork/checkpoint, and approval read models work against the standard controller, API server, SQLite store, and worker event client.

## Event model

Events are appended to a task stream:

```text
(namespace, streamType=task, streamID=<taskName>, seq)
```

`seq` is monotonically increasing per task stream. Task `status` remains the Kubernetes summary state; events are the durable detailed history.

Important fields:

| Field | Meaning |
| --- | --- |
| `seq` | Per-task event sequence. |
| `type` | Stable event type such as `TaskStarted`, `ToolCallCompleted`, `ApprovalRequested`, or `TaskForkCreated`. |
| `severity` | `debug`, `info`, `warning`, or `error`. Unknown producer values normalize to `info`. |
| `taskName` | Source task name. |
| `sessionName` | Optional session tag used by session aggregation. |
| `toolName` / `toolCallID` | Tool correlation keys. |
| `summary`, `content`, `contentText` | Sanitized event payload surfaces. |
| `truncation` | Indicates payload truncation without exposing removed data. |

The persisted stream type accepted by the internal append API is `task`. Session streams are currently a read model derived from task events tagged with `sessionName`.

## Task event list API

```http
GET /api/v1/tasks/:id/events?namespace=<ns>&after=0&limit=100&type=ToolCallCompleted
```

Response:

```json
{
  "namespace": "default",
  "streamType": "task",
  "streamID": "my-task",
  "afterSeq": 0,
  "latestSeq": 3,
  "events": [
    {"seq": 1, "type": "TaskStarted", "severity": "info", "taskName": "my-task"}
  ]
}
```

`after=N` returns events where task `seq > N`. `limit` is capped by the server maximum. Repeated `type` filters are supported by the HTTP API.

## Task SSE stream API

```http
GET /api/v1/tasks/:id/stream?namespace=<ns>&after=<last-seq>
```

Each event frame uses the task sequence as the SSE id:

```text
id: 2
event: execution_event
data: {"seq":2,"type":"WorkerStarted"}
```

When a terminal task event (`TaskSucceeded`, `TaskFailed`, `TaskCancelled`) is observed, the stream sends a final `stream_complete` frame and closes:

```text
id: 3
event: stream_complete
data: {"lastSeq":3,"type":"TaskSucceeded"}
```

Reconnect by passing the last observed SSE id as `after`.

## Session event aggregation

Session timelines aggregate task events whose `sessionName` equals the session id:

```http
GET /api/v1/sessions/:id/events?namespace=<ns>&after=0&limit=100
GET /api/v1/sessions/:id/stream?namespace=<ns>&after=<session-seq>
```

Session `seq` is a stable read-model cursor ordered by event append order. Delayed events with older producer timestamps do not renumber previously delivered session events.

Each session event includes both the session cursor and the source task cursor:

```json
{
  "seq": 2,
  "streamType": "session",
  "streamID": "session-1",
  "taskName": "task-b",
  "taskSeq": 1,
  "taskStreamID": "task-b",
  "type": "WorkerStarted"
}
```

Session streams do not close on task terminal events because a session can have more tasks later.

## Task trace API

This is Orka's event-derived task trace read model. It is separate from
OpenTelemetry distributed traces: `orka task trace` reads stored execution
events through the Orka API, not your OTLP collector/backend.

```http
GET /api/v1/tasks/:id/trace?namespace=<ns>
```

The trace API builds a read model from the task event stream. It groups:

- model requests,
- tool calls,
- child task references,
- workspace preparation events,
- artifact upload events,
- errors and warnings,
- the terminal event.

Unpaired completion/failure events are preserved in `rawUnpaired` with warnings instead of failing the request.

CLI:

```bash
orka task trace '<task>'
orka task trace '<task>' -o json
```

## Fork/checkpoint MVP

```http
POST /api/v1/tasks/:id/fork?namespace=<ns>
Content-Type: application/json

{
  "afterSeq": 5,
  "newTaskName": "my-task-fork",
  "agentRef": {"name": "reviewer"},
  "prompt": "Continue from the checkpoint and inspect the failed tool call."
}
```

MVP behavior:

- validates the source task and namespace;
- validates `afterSeq` as `0`, latest, or an existing task event sequence;
- creates a sibling/child Task with provenance annotations:
  - `orka.ai/fork-source-task`,
  - `orka.ai/fork-source-seq`,
  - `orka.ai/fork-context-truncated`;
- emits `TaskForkRequested` and `TaskForkCreated` events;
- includes a bounded, sanitized fork context in the API response;
- does **not** clone a workspace snapshot.

CLI:

```bash
orka task fork '<task>' --after 5 --agent reviewer --prompt "Continue from here"
```

## Durable approvals MVP

Workers emit only the `ApprovalRequested` event. The internal event submission
endpoint (`SubmitExecutionEvent`) rejects worker-submitted terminal approval
events with `403 Forbidden` ("terminal task and approval events must use
controller-owned paths").

The approval lifecycle event types carried on the task stream are:

- `ApprovalRequested` — emitted by workers,
- `ApprovalApproved`,
- `ApprovalDeclined`,
- `ApprovalExpired`,
- `ApprovalCancelled`.

The four terminal types (`ApprovalApproved`, `ApprovalDeclined`,
`ApprovalExpired`, `ApprovalCancelled`) are appended only by controller-owned
paths — for example the decision endpoint below — never by worker submissions.

Pending/current approvals are derived from the event stream:

```http
GET /api/v1/tasks/:id/approvals?namespace=<ns>
```

A user can append a decision event:

```http
POST /api/v1/tasks/:id/approvals/:approvalID/decision?namespace=<ns>
Content-Type: application/json

{"decision":"approve","reason":"safe to create PR"}
```

CLI:

```bash
orka task approvals '<task>'
orka task approve '<task>' '<approvalID>' --reason "looks safe"
orka task decline '<task>' '<approvalID>' --reason "not safe"
```

The first recommended high-risk action to integrate with these events is PR creation/merge. The API/read model is in place before broad worker policy integration.

## Redaction and truncation

Workers redact before submitting events, and the store sanitizes again before persistence. Public APIs and SSE frames return sanitized payloads only. Fake values matching bearer tokens, JWT-like strings, API keys, cookies, GitHub tokens, Anthropic/OpenAI tokens, and transaction-token headers must be redacted before they appear in SQLite rows or public responses.

## Metrics

Execution event metrics intentionally avoid task names, session names, event ids, tool call ids, and other high-cardinality labels.

Available metrics include:

- `orka_execution_events_appended_total{stream_type,event_type}`
- `orka_execution_event_append_failures_total{stream_type,event_type}`
- `orka_execution_event_append_duration_seconds{stream_type,event_type,result}`
- `orka_execution_event_list_requests_total{scope,result}`
- `orka_execution_event_list_duration_seconds{scope,result}`
- `orka_execution_event_stream_connections_current{scope}`
- `orka_execution_event_stream_reconnects_total{scope}`
- `orka_execution_event_stream_errors_total{scope,reason}`
- `orka_execution_event_redactions_total{stream_type,event_type}`
- `orka_execution_event_truncations_total{stream_type,event_type}`

Example PromQL:

```promql
sum(rate(orka_execution_events_appended_total[5m])) by (event_type)
sum(rate(orka_execution_event_append_failures_total[5m])) by (event_type)
histogram_quantile(0.95, sum(rate(orka_execution_event_append_duration_seconds_bucket[5m])) by (le, event_type))
sum(orka_execution_event_stream_connections_current) by (scope)
sum(rate(orka_execution_event_stream_reconnects_total[15m])) by (scope)
sum(rate(orka_execution_event_stream_errors_total[5m])) by (scope, reason)
sum(rate(orka_execution_event_redactions_total[1h])) by (event_type)
```

## Troubleshooting

| Symptom | Checks |
| --- | --- |
| Missing events | Confirm task exists, event store enabled, worker has internal API URL/token, and `sessionName` is set for session timelines. |
| Stuck worker | Check task events for `TaskStarted` without `WorkerStarted`, then inspect pod scheduling/logs. |
| Model failure | Use trace API; `ModelRequestFailed` appears in `errors`. |
| Tool failure | Use trace API; failed tool calls are grouped by `toolCallID`. |
| Result missing | Confirm terminal event and `status.resultRef.available`; event history does not replace result storage. |
| Stream reconnect duplicates | Reconnect with the last observed SSE id as `after`; task streams use task seq, session streams use session seq. |
| Event store failure | Check `/readyz`, API logs for append/list failures, and the execution event metrics above. |
