---
slug: /anthropic-compat
description: "Pointing Anthropic-compatible clients at Orka's Messages API endpoint."
---

# Anthropic-compatible API

Orka speaks the Anthropic Messages API at `/anthropic/v1/messages`, so Claude Code and other
Anthropic-native clients can point at Orka instead of at a model vendor. Your cluster holds
the API keys; the client holds a ServiceAccount token.

:::warning[Coordinator mode is enabled by default]
Orka rewrites your request before sending it upstream: it **discards the tools your client
sent**, injects its own, and prepends its own system prompt. See
[Coordinator mode](openai-compat.md#coordinator-mode) for exactly what changes and how to
turn it off — the behavior is identical on both compatibility endpoints.
:::

See also [OpenAI compatibility](openai-compat.md) for the OpenAI-shaped equivalent.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/anthropic/v1/messages` | Create a message (streaming & non-streaming) |
| `GET` | `/anthropic/v1/models` | List available models from configured providers |

PR-blocking live CI exercises this API directly against a live Claude-family backend by checking `/anthropic/v1/models` and both non-streaming and streaming `/anthropic/v1/messages` requests. Those live checks keep the default Orka tool-loop behavior enabled unless a client explicitly sets `X-Orka-Tools: disabled`.

## Authentication

Kubernetes ServiceAccount tokens, and OIDC JWTs when OIDC is configured, can use either
header:

- **`x-api-key: <orka-token>`** — Anthropic convention (recommended for Anthropic clients)
- **`Authorization: Bearer <orka-token>`** — Standard Bearer token

Transaction-token authentication uses `Txn-Token: <token>` by default. To accept TxTokens
as Bearer tokens, the operator must explicitly include `Authorization:Bearer` in
`--context-token-headers`, for example `--context-token-headers=Txn-Token,Authorization:Bearer`.
Using `x-api-key` for TxTokens also requires explicit context-token header configuration.
See [Authentication](./api-reference.md#authentication).

## Model name format

The `model` field supports two formats:

- **`provider/model`** — e.g., `anthropic/claude-sonnet-4-20250514`. The part before `/` matches a Provider CRD name, and the part after is the model name sent to that provider.
- **`model`** — e.g., `claude-sonnet-4-20250514`. Uses the default provider (from `--chat-provider` flag or a Provider CRD named `default`).

## Prerequisites

1. **Provider CRD** configured in the cluster:

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Provider
metadata:
  name: anthropic
  namespace: orka-system
spec:
  type: anthropic
  secretRef:
    name: anthropic-secret
    key: api-key
  defaultModel: claude-sonnet-4-20250514
```

2. **Secret** with the API key:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: anthropic-secret
  namespace: orka-system
type: Opaque
stringData:
  api-key: sk-ant-...
```

3. **ServiceAccount token and RBAC** for authentication and coordinator tools:

Follow the [API-client setup](../getting-started.md#give-yourself-an-api-client) to
configure `orka-client` and its Task permissions. Coordinator Task creation requires
`tasks/create`. Agent creation currently does not check a ServiceAccount caller's
`agents/create` permission; see the [API authorization limitation](../operations/troubleshooting.md#i-get-403-from-the-api).
Then create a token:

```bash
export ORKA_TOKEN="$(kubectl -n orka-system create token orka-client)"
```

## Using with Claude Code

Configure Claude Code to route all API calls through Orka:

```bash
export ANTHROPIC_BASE_URL=https://orka.example.com/anthropic
export ANTHROPIC_API_KEY=$(kubectl -n orka-system create token orka-client)
# Claude Code will now route all API calls through Orka
```

## Using with curl

### Non-streaming

```bash
curl -X POST https://orka.example.com/anthropic/v1/messages \
  -H "x-api-key: $ORKA_TOKEN" \
  -H "Content-Type: application/json" \
  -H "anthropic-version: 2023-06-01" \
  -d '{
    "model": "anthropic/claude-sonnet-4-20250514",
    "max_tokens": 1024,
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

### Streaming

```bash
curl -X POST https://orka.example.com/anthropic/v1/messages \
  -H "x-api-key: $ORKA_TOKEN" \
  -H "Content-Type: application/json" \
  -H "anthropic-version: 2023-06-01" \
  -d '{
    "model": "anthropic/claude-sonnet-4-20250514",
    "max_tokens": 1024,
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": true
  }'
```

### List models

```bash
curl https://orka.example.com/anthropic/v1/models \
  -H "x-api-key: $ORKA_TOKEN"
```

## Supported features

| Feature | Supported |
|---------|-----------|
| Messages API | Yes |
| Streaming (SSE) | Yes |
| Tool use (function calling) | Server-side only by default — see [Coordinator mode](openai-compat.md#coordinator-mode) |
| System messages (string format) | Yes, but Orka's own prompt is prepended in coordinator mode |
| System messages (content block array format) | Same |
| `max_tokens` | Yes |
| `temperature` | Yes |
| `stop_sequences` | Yes |
| Extended thinking (`thinking` with `budget_tokens`) | **No.** The field is accepted and ignored — see below |
| Image inputs | Not yet |
| PDF inputs | Not supported |

:::warning[`thinking` is accepted but has no effect]
A `thinking` block in your request parses without error and is then dropped: it is never
forwarded to the provider, and Orka never returns `thinking` content blocks. A client that
sets `budget_tokens` gets an ordinary response and no error telling it why.

If you need extended thinking, call the provider directly.
:::

## Server-side tool execution

By default this endpoint runs the tool loop itself. When the model returns `tool_use` blocks,
Orka executes the tools, feeds the results back, and repeats until the model produces text.
Your client never executes a tool.

[Coordinator mode](openai-compat.md#coordinator-mode) describes the full rewrite — which
tools are injected, what happens to yours, and the `X-Orka-Tools: disabled` header that
turns all of it off. It applies identically here; the rest of this page covers what is
specific to the Anthropic shape.

### The loop

1. You `POST /anthropic/v1/messages`.
2. Orka rewrites the request (coordinator mode) and calls the model.
3. If the response contains `tool_use` blocks, Orka executes each one, appends the results
   to the conversation, and calls the model again.
4. Step 3 repeats until the model returns text only.
5. That final response goes back to you.

The loop executes the [18 built-in coordinator tools](openai-compat.md#coordinator-mode).
Custom `Tool` CRDs with `parameters` can appear in the model's tool list, but the loop
cannot execute their HTTP requests. Calling one returns `tool "..." not found`.

### Streaming behavior

With `stream: true` in coordinator mode, Orka uses one Anthropic SSE message for the
whole tool loop. Each provider turn is buffered and validated before Orka sends its text.

- `message_start` opens the message once.
- `content_block_start/delta/stop` carries model text and sanitized progress messages,
  such as `[Tool file_read completed]`.
- `message_delta` and `message_stop` close the message once.

Coordinator mode keeps `tool_use` blocks and raw tool results inside the server-side
conversation. With `X-Orka-Tools: disabled`, Orka sends provider
text and `tool_use` events to the client without running tools.

### Limits and timeouts

| Setting | Flag | Default | What it caps |
|---------|------|---------|--------------|
| Max iterations | `--chat-max-iterations` | 50 | Tool-loop iterations |
| Max duration | `--chat-max-duration` | 30m | Wall-clock time for the whole request |
| Tool timeout | `--chat-tool-timeout` | 60s | One tool execution |
| Max session size | `--chat-max-session-size` | 512,000 bytes | Conversation size before truncation |

These are the shared chat settings and apply to streaming and non-streaming requests alike.
See [Configuration](configuration.md#controller-flags).

When a non-streaming request reaches the iteration limit, Orka makes one additional
model call without tools to produce a closing summary. A streaming request closes its
response at the limit without that additional call.

### Repetition detection

If the LLM calls the same tool with identical arguments 3 or more times, the proxy injects a warning message asking it to try a different approach. This prevents infinite loops where the LLM repeatedly calls a failing tool.

### Error handling

- **Tool execution errors**: Wrapped as JSON results (`{"success": false, "error": "..."}`) and fed back to the LLM, which can decide how to recover
- **LLM errors**: If the LLM returns a context-too-long error, the proxy truncates the conversation to ~50% and retries once. Other LLM errors terminate the loop and return an Anthropic error response
- **Timeout**: If the overall request timeout is reached, the proxy returns whatever progress has been made

### Example: curl with server-side tools

Server-side tool execution is enabled by default — no special header needed:

```bash
curl -X POST https://orka.example.com/anthropic/v1/messages \
  -H "x-api-key: $ORKA_TOKEN" \
  -H "Content-Type: application/json" \
  -H "anthropic-version: 2023-06-01" \
  -d '{
    "model": "anthropic/claude-sonnet-4-20250514",
    "max_tokens": 4096,
    "messages": [{"role": "user", "content": "Search the web for Kubernetes 1.32 release highlights and summarize them."}],
    "stream": true
  }'
```

To let your client manage tools, add `X-Orka-Tools: disabled`. This disables Orka's
coordinator rewrite and tool loop. Requests and responses still pass through Orka's
format conversion. Fields including `top_p`, `top_k`, `tool_choice`, and `thinking`
are accepted but not forwarded to the provider.

## Request path

```
┌─────────────┐     ┌──────────────────────────────┐     ┌───────────────┐
│ Claude Code │────▶│ Orka API server              │────▶│ Anthropic API │
│ (or any     │◀────│ /anthropic/v1/messages       │◀────│ OpenAI API    │
│ Anthropic   │     │                              │     │ Azure OpenAI  │
│ client)     │     │ 1. Resolve Provider CRD      │     └───────────────┘
└─────────────┘     │ 2. Read API key from Secret  │
                    │ 3. Coordinator rewrite       │
                    │    (unless X-Orka-Tools:     │
                    │     disabled)                │
                    │ 4. Server-side tool loop     │
                    └──────────────────────────────┘
```

## Token budgets and "incomplete" errors

`max_tokens` caps the model's *entire* output for a turn, and for reasoning
models that budget is shared with hidden reasoning tokens. Two shapes come
back when the budget runs out:

- **Partial text** — the response was cut off mid-answer. Orka returns the
  partial text with `stop_reason: "max_tokens"`, matching upstream behavior; raise the budget
  and retry.
- **Nothing usable** — the model spent the whole budget before emitting any
  text (common when a reasoning model gets a small `max_tokens`), or the
  cutoff truncated a tool call, whose arguments would be unsafe to execute.
  Orka fails the request with an error describing an *incomplete completion
  outcome* instead of returning an empty or corrupt message.

If you see the incomplete-outcome error, it is not a proxy fault: give the
request a substantially larger `max_tokens` (reasoning models often need
thousands of tokens of headroom), or use a non-reasoning model for short
completions.
