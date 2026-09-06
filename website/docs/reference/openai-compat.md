---
slug: /openai-compat
description: "Pointing OpenAI-compatible clients at Orka's chat completions endpoint."
---

# OpenAI-compatible API

Orka speaks the OpenAI chat API at `/openai/v1/chat/completions` and `/openai/v1/models`,
so clients like [Continue](https://continue.dev/) and [Cursor](https://cursor.sh/) can point
at Orka instead of at a model vendor. Your cluster holds the API keys; the client holds a
ServiceAccount token.

:::warning[This is not a transparent proxy by default]
Orka rewrites your request before sending it upstream: it **discards the tools your client
sent**, injects its own, and prepends its own system prompt. Read
[Coordinator mode](#coordinator-mode) before wiring up a client that relies on its own
tools. One header turns it off.
:::

:::info[Endpoints moved]
These used to live at `/v1/`. They are now at `/openai/v1/`. See
[Anthropic compatibility](anthropic-compat.md) for the Anthropic-native equivalent.
:::

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/openai/v1/chat/completions` | Chat completions (streaming & non-streaming) |
| `GET` | `/openai/v1/models` | List available models from configured providers |

Both endpoints require authentication. Send a Kubernetes ServiceAccount token in
`Authorization: Bearer <token>`. OIDC tokens use the same header when OIDC is configured.

When transaction-token authentication is configured, send TxTokens in `Txn-Token: <token>`
by default. To accept TxTokens as Bearer tokens, the operator must explicitly include
`Authorization:Bearer` in `--context-token-headers`, for example
`--context-token-headers=Txn-Token,Authorization:Bearer`.
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

### Azure OpenAI provider example

If you use Azure OpenAI, configure a Provider with `type: azure-openai`:

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Provider
metadata:
  name: azure-openai
  namespace: orka-system
spec:
  type: azure-openai
  secretRef:
    name: azure-openai-secret
    key: api-key
  baseURL: https://<resource>.openai.azure.com
  defaultModel: gpt-4o-deployment
  azure:
    deploymentName: gpt-4o-deployment
    apiVersion: "2024-02-15-preview"
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

## Using with Continue

### Configuration

Configure Continue to use Orka as an OpenAI-compatible provider. Add to your Continue configuration:

```json
{
  "models": [
    {
      "title": "Claude Sonnet 4 (via Orka)",
      "provider": "openai",
      "model": "anthropic/claude-sonnet-4-20250514",
      "apiBase": "https://orka.example.com/openai/v1",
      "apiKey": "YOUR_ORKA_TOKEN"
    }
  ]
}
```

### Environment

Set your Orka API token:

```bash
export ORKA_TOKEN=$(kubectl -n orka-system create token orka-client)
```

## Using with curl

### Non-streaming

```bash
curl -X POST https://orka.example.com/openai/v1/chat/completions \
  -H "Authorization: Bearer $ORKA_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "anthropic/claude-sonnet-4-20250514",
    "messages": [{"role": "user", "content": "Hello!"}],
    "max_tokens": 1024
  }'
```

### List models

```bash
curl https://orka.example.com/openai/v1/models \
  -H "Authorization: Bearer $ORKA_TOKEN"
```

## Supported features

| Feature | Supported |
|---------|-----------|
| Chat completions | Yes |
| Streaming (SSE) | Yes |
| Tool/function calling | Server-side only by default — see [Coordinator mode](#coordinator-mode) |
| System messages | Yes, but Orka's own prompt is prepended in coordinator mode |
| Multi-part content | Yes (text parts extracted) |
| `max_tokens` / `max_completion_tokens` | Yes |
| `temperature` | Yes |
| `stop` sequences | Yes |
| `stream_options.include_usage` | Yes |
| Image inputs | Not yet (text extracted from multi-part) |
| Embeddings | Not supported |
| Audio / Vision | Not supported |

## Coordinator mode

Both compatibility endpoints — this one and
[Anthropic](anthropic-compat.md) — default to **coordinator mode**. The idea is that your
editor's chat window becomes a way to drive the cluster: ask for a change, and the model
creates Agents and Tasks, waits for them, and opens a pull request. It is not a pass-through
proxy.

Concretely, Orka does five things to every request before it reaches the model:

| # | What happens | Consequence for you |
| --- | --- | --- |
| 1 | **Your `tools` array is discarded.** Not merged — replaced. | Your client's own tools never run. |
| 2 | 18 built-in Orka tools are injected. | The model can act on your cluster with the tools listed below. |
| 3 | The tool list is filtered against your context token's allowed tools, if you use [transaction tokens](../concepts/transaction-tokens.md). | Denied tools disappear rather than failing at call time. |
| 4 | A large Orka system prompt is **prepended** to yours. | Your system prompt still applies, but it is no longer first. |
| 5 | Tool history is stripped from your messages. `role: tool` messages are dropped; assistant messages keep their text but lose their tool calls; consecutive same-role messages are merged. | Sending back a conversation that contains client-side tool use loses that structure. |

Orka then runs the tool loop itself and returns the final answer.

The 18 injected tools:

| Group | Tools |
| --- | --- |
| Built-in | `web_search`, `web_fetch`, `code_exec`, `file_read`, `file_write` |
| Create work | `create_agent`, `create_agent_task`, `create_ai_task`, `create_container_task`, `create_pr_monitor` |
| Track work | `check_task_progress`, `fetch_task_output`, `wait_for_task`, `cancel_task`, `list_agents`, `list_tasks` |
| Pull requests | `create_pull_request`, `check_pull_request_ci` |

Custom `Tool` CRDs in the namespace that define `parameters` are also advertised to the
model, but the compatibility loop cannot execute their HTTP requests. Calling one returns
`tool "..." not found`. Use the built-ins listed above for coordinator requests.

### Turning it off

```
X-Orka-Tools: disabled
```

This header disables the coordinator rewrite and Orka's tool loop. Your client manages
its own tools and tool loop. Requests still pass through Orka's OpenAI request conversion;
for example, `top_p`, `frequency_penalty`, and `presence_penalty` are not forwarded to the
provider. Provider resolution and credential handling are unchanged.

Use `disabled` when you want Orka only for centralized credentials and model routing. Leave
it on when you want the model to be able to do things in the cluster.

## Request path

```
┌─────────────┐     ┌─────────────────────────────┐     ┌───────────────┐
│ Continue    │────▶│ Orka API server             │────▶│ Anthropic API │
│ (or any     │◀────│ /openai/v1/chat/completions │◀────│ OpenAI API    │
│ OAI client) │     │                             │     │ Azure OpenAI  │
└─────────────┘     │ 1. Resolve Provider CRD     │     └───────────────┘
                    │ 2. Read API key from Secret │
                    │ 3. Coordinator rewrite      │
                    │    (unless X-Orka-Tools:    │
                    │     disabled)               │
                    │ 4. Server-side tool loop    │
                    └─────────────────────────────┘
```

## Token budgets and "incomplete" errors

`max_tokens` caps the model's *entire* output for a turn, and for reasoning
models that budget is shared with hidden reasoning tokens. Two shapes come
back when the budget runs out:

- **Partial text** — the response was cut off mid-answer. Orka returns the
  partial text with `finish_reason: "length"`, matching upstream behavior; raise the budget
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
