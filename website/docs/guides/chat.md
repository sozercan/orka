---
slug: /chat
---

# Interactive Chat

The chat endpoint provides an agentic conversational interface where an LLM orchestrator can create and manage Kubernetes resources on the user's behalf. It accepts natural language, reasons about what tasks to create, and autonomously executes them using the platform.

## Endpoint

```
POST /api/v1/chat         — Send a message (SSE streaming or JSON response)
GET  /api/v1/chat/config  — Get chat configuration and available tools
DELETE /api/v1/chat/:sessionId — Cancel a chat session
```

## Architecture

```
User ──POST /api/v1/chat──▶ API Server ──▶ Concurrency Semaphore
                               │
                               ├─▶ context.WithTimeout (--chat-max-duration, default 30m)
                               ├─▶ Resolve Provider CRD → get LLM client
                               ├─▶ Load/create chat session (prefix: chat-session-)
                               ├─▶ Build structured system prompt (cached per request)
                               ├─▶ Call LLM with tools
                               │     │
                               │     ├─▶ create_ai_task
                               │     ├─▶ create_container_task
                               │     ├─▶ create_agent_task
                               │     ├─▶ check_task_progress
                               │     ├─▶ fetch_task_output
                               │     ├─▶ wait_for_task
                               │     ├─▶ cancel_task
                               │     ├─▶ list_agents / list_tools / list_tasks
                               │     ├─▶ create_agent / update_agent / delete_agent (on-demand)
                               │     ├─▶ create_tool / delete_tool (on-demand)
                               │     └─▶ delete_session (on-demand)
                               │
                               ├─▶ Stream response via SSE (with heartbeats)
                               └─▶ Detect client disconnect → cleanup
```

## Request Format

```json
{
    "message": "Create an AI task that summarizes Kubernetes best practices",
    "sessionId": "my-session",
    "provider": "anthropic",
    "model": "claude-sonnet-4-20250514",
    "namespace": "default",
    "temperature": 0.7,
    "maxTokens": 4096,
    "systemPrompt": "Focus on security topics",
    "agentRef": "my-agent"
}
```

Only `message` is required. All other fields are optional:
- `sessionId`: Existing session for continuity (auto-created if omitted)
- `provider` / `model`: Override the default LLM provider and model
- `agentRef`: Use an Agent CRD for provider/model/temperature defaults
- `systemPrompt`: Appended to the built-in orchestrator prompt
- `namespace`: Namespace for task operations

## Response Formats

### SSE Streaming (default)

All SSE events use a structured envelope:

```
id: <monotonic-seq>
event: <type>
data: {"sessionId":"...","content":"...","toolCall":{...},"error":{...},"usage":{...}}
```

Event types:

| Event | Description |
|-------|-------------|
| `status` | Stream opened — confirms session, provider, model |
| `message` | Text content delta |
| `tool_call` | Tool invocation (name + args) |
| `tool_result` | Tool execution result |
| `error` | Error with code and message |
| `done` | Stream complete with usage stats |

### JSON Response

Send `Accept: application/json` for a blocking JSON response:

```json
{
    "sessionId": "chat-session-abc123",
    "message": "I created an AI task...",
    "toolCalls": [{"name": "create_ai_task", "args": {...}, "result": {...}}],
    "usage": {"inputTokens": 4200, "outputTokens": 1800, "llmCalls": 3}
}
```

## Live Coverage

PR-blocking live CI covers the chat endpoint against a real backend in both:

- SSE mode via `Accept: text/event-stream`
- JSON mode via `Accept: application/json`

The live suite verifies transport behavior, session creation/persistence, and usage reporting. Exact model wording is still covered primarily by deterministic chat tests in `test/e2e/` and unit tests.

## Available Tools

### Core Tools (always loaded)

| Tool | Description |
|------|-------------|
| `create_ai_task` | Create an AI/LLM-powered task |
| `create_container_task` | Create a container task for shell commands |
| `create_agent_task` | Create an ACP Task using an existing Agent configured with a built-in runtime or a ready external-v2 `runtimeRef`. |
| `check_task_progress` | Get task phase/status conditions |
| `fetch_task_output` | Get completed task result (truncated to 2K chars) |
| `wait_for_task` | Wait for task completion (max 60s per call, non-blocking) |
| `cancel_task` | Cancel/delete a task |
| `list_agents` | List Agent CRDs with projected summaries |
| `list_tools` | List Tool CRDs and built-in tools |
| `list_tasks` | List tasks with optional status filter |

### Management Tools (loaded on demand)

These are only included when the user's message signals CRUD intent:

| Tool | Description |
|------|-------------|
| `create_agent` | Create an Agent CRD |
| `update_agent` | Update an Agent CRD |
| `delete_agent` | Delete an Agent CRD |
| `create_tool` | Create a Tool CRD with HTTP endpoint |
| `delete_tool` | Delete a Tool CRD |
| `delete_session` | Delete a session and transcript |

## System Prompt

The system prompt uses XML-delimited sections for optimal tool-calling accuracy:

```xml
<identity>
You are the Orka orchestrator — an AI assistant that manages
Kubernetes-native task execution.
</identity>

<capabilities>...</capabilities>
<task_types>container, ai, agent with usage guidance</task_types>
<available_agents>...dynamically injected...</available_agents>
<available_tools>...dynamically injected...</available_tools>
<rules>Operational invariants</rules>
<examples>Complete multi-step tool-calling traces</examples>
```

Dynamic context (agents, tools) is built once at request start and cached for the tool loop duration.

## Safety Mechanisms

### Concurrency Control
- Bounded semaphore (`--chat-max-concurrent`, default 10)
- Returns `429 Too Many Requests` when full

### Timeouts
- `--chat-max-duration` (default 30m): Wall-clock timeout per request
- `--chat-tool-timeout` (default 60s): Per-tool execution timeout
- `--chat-max-iterations` (default 50): Max tool execution loops

### Resource Limits
- `--chat-max-tasks-per-turn` (default 5): Max tasks created per chat turn
- Task names use session-scoped prefix to prevent collisions

### Stuck-State Detection
- **Repetition detector**: Same tool called with identical args 3 times → warning injected, 5 iterations penalized
- **Progress assertion**: Every 5 iterations, LLM must summarize progress
- **Graceful termination**: On iteration exhaustion, LLM must provide final summary

### Error Handling
Structured error responses help the LLM self-correct:

```json
{
    "success": false,
    "error": "Agent 'my-agent' not found in namespace 'default'",
    "errorType": "not_found",
    "suggestion": "Use list_agents to see available agents"
}
```

## Session Management

- Chat sessions use prefix `chat-session-` and type `chat` in the session store
- Sessions store message summaries, not full tool outputs
- Auto-truncation when session exceeds `--chat-max-session-size` (default 500KB)
- First user message is always preserved for context

## Namespace Scoping

- Default to namespace from `ChatRequest` or authenticated user's namespace
- `kube-system`, `kube-public`, and the operator's namespace are blocked
- All orchestrator-created resources get labels: `orka.ai/created-by: orchestrator`, `orka.ai/chat-session: <sessionId>`

## Configuration Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--chat-enabled` | `true` | Enable/disable chat endpoint |
| `--chat-provider` | `""` | Default Provider CRD name |
| `--chat-model` | `""` | Default model |
| `--chat-max-iterations` | `50` | Max tool execution loops |
| `--chat-max-duration` | `30m` | Max wall-clock time per request |
| `--chat-tool-timeout` | `60s` | Max time per tool execution |
| `--chat-max-concurrent` | `10` | Max concurrent chat sessions |
| `--chat-max-tasks-per-turn` | `5` | Max tasks per chat turn |
| `--chat-max-session-size` | `512000` | Session size soft limit (bytes) |

## Example Usage

```bash
# Chat with SSE streaming
curl -N http://localhost:8080/api/v1/chat \
  -H "Authorization: Bearer $(kubectl create token orka-client)" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Create an AI task that summarizes Kubernetes best practices",
    "sessionId": "my-session"
  }'

# Chat with JSON response
curl http://localhost:8080/api/v1/chat \
  -H "Authorization: Bearer $(kubectl create token orka-client)" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{"message": "List all available agents"}'

# Get chat configuration
curl http://localhost:8080/api/v1/chat/config \
  -H "Authorization: Bearer $(kubectl create token orka-client)"

# Cancel a chat session
curl -X DELETE http://localhost:8080/api/v1/chat/my-session \
  -H "Authorization: Bearer $(kubectl create token orka-client)"
```
