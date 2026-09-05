---
slug: /glossary
description: "Every term these docs use, defined once."
---

# Glossary

Orka's docs use a handful of terms with precise meanings. This page defines all of them.
If a term here is doing real work in a page you are reading, that page should link back here.

## Core resources

**Task** — one unit of work, and the resource you create to make something happen. Three
kinds: `type: ai` (Orka's own AI worker), `type: agent` (a coding-agent CLI), and
`type: container` (an arbitrary command). See [Architecture](../concepts/architecture.md).

**Agent** — a reusable configuration a Task points at: which Provider and model to use, a
system prompt, allowed tools, and runtime settings. Not a running process — a template.

**Provider** — an LLM backend (Anthropic, OpenAI, Azure OpenAI) and the Secret holding its
API key.

**Session** — the conversation a Task belongs to. Several Tasks can share one Session so
later work sees earlier context.

**Skill** — a reusable instruction bundle an Agent can load, similar to a prompt library
entry with structure.

**Tool** — something a model can call: a built-in like `web_search`, or a `Tool` custom
resource describing an HTTP endpoint or MCP server.

## Running coding agents

**ACP** — the [Agent Client Protocol](https://agentclientprotocol.com), a JSON-RPC protocol
that coding-agent CLIs speak over stdin/stdout so a program can drive them instead of a
person. Open and external to Orka. Orka speaks it to Codex, Claude Code, GitHub Copilot
CLI, and OpenCode.

**Agent runtime** — one of those CLIs, packaged as a container image Orka knows how to
drive. Four are built in; you can register your own with
[an adapter](../development/agent-runtime-adapter-contract.md).

**RuntimePool** — a controller-owned resource for one long-lived runtime Pod, pinned to
an image digest. It scales to zero when idle. Its capacity settings limit concurrent
sessions and prompts within that Pod. Reusing a warm pool avoids starting a new runtime
Pod for each Task.

**RuntimeSession** — one agent conversation inside a pool. It has its own private directory
and its own operating-system user, so two sessions in one pool cannot read each other's
files.

**Supervisor** — the small Orka process inside a runtime Pod that starts agent CLIs, keeps
sessions apart, and speaks to the controller. The agent CLI never talks to the cluster
directly.

**PromptAttempt** — the durable record of one prompt sent to a session, including whether
it was delivered, so a controller restart never sends the same prompt twice.

**Harness** — the contract between the controller and the thing executing agent work.
`harness-v1` is the older sidecar-wrapper design; `harness-v2` is the current ACP pool
design. An installation picks one at install time and cannot switch.
See [Harness modes](../operations/harness-modes.md).

**Producer** — anything that creates Tasks: a person with the CLI, CI, a webhook, another
agent. The term matters during a v1-to-v2 migration, where you point producers at one
installation's endpoint or the other.

## Safety and correctness

**Fail closed** — when Orka cannot confirm something is safe, it refuses instead of
guessing. A missing runtime image makes Tasks fail with an error rather than silently
using a different one; an unverifiable credential scope aborts the request. The opposite,
failing open, would keep working with reduced guarantees.

**Fence** — a set of identifiers attached to a request that must all still match when the
request is acted on: which controller, which pool, which Pod, which boot of that Pod,
which session, which operation. If anything has been replaced in the meantime, the request
is rejected rather than applied to the wrong target. This is what stops a stale message
from a restarted controller landing in a new session.

**Epoch** — a counter that increases each time a controller takes leadership. It is part
of every fence, so work authorized by a previous leader cannot be applied by the current
one.

**Digest-pinned** — an image is referenced as `repository@sha256:...` rather than by tag,
so the bytes cannot change under a running pool. Orka rejects tags for agent runtimes.

**Scale to zero** — an idle pool drops to no Pods and comes back when work arrives.

**Drain** — taking a pool out of service by letting in-flight sessions finish while
refusing new ones. Replacement and scale-down wait for a drain to complete.

**Clean-room publication** — pushing an agent's work to a git forge from a separate Pod
(the *publisher*) that has the credentials, rather than giving them to the agent. The
agent produces a patch; the publisher applies and pushes it. The agent process never holds
a token that can write to your repository.

## Credentials and network

**Provider proxy** — the path for agent model traffic: the supervisor's per-session
loopback proxy enforces provider routes and model selection, then calls the authenticated
`provider-auth-proxy` service, which forwards to Vekil. Agent processes receive session
proxy tokens; Vekil holds the provider API keys.
See [Provider proxy](../operations/provider-proxy.md).

**Vekil** — the upstream component the provider proxy forwards to, which holds the real
provider credentials and speaks the Anthropic, OpenAI, and Gemini APIs.
See [Provider proxy](../operations/provider-proxy.md).

**MCP** — the [Model Context Protocol](https://modelcontextprotocol.io), the standard way
to expose tools to a model. Orka can present `Tool` resources to agents as MCP servers,
scoped to a single prompt.

**Transaction token (TxToken)** — a short-lived, scoped token that travels with a request
so downstream services can verify what was authorized, independent of who is calling. Orka
keeps raw tokens out of Task specs, status, and logs. Delegated child tokens are stored in
owner-referenced Kubernetes Secrets and mounted read-only into child workers.
See [Transaction tokens](../concepts/transaction-tokens.md).

**TTS** — Token Transaction Service, the endpoint that exchanges a token for a
narrower-scoped child token when one Task delegates to another. A child's scope must be a
subset of its parent's.

**Credential broker** — the controller endpoint that hands credentials only to the
publisher, after checking what the request is for. Keeps forge credentials out of agent
Pods.

## Workspaces

**Workspace** (`Task.spec.workspace`) — the git repository an agent Task works in: the
clone URL, branch, whether it may write, and which Secrets to read or publish with.

**Execution workspace** (`Task.spec.execution.workspace`) — a different thing with a
confusingly similar name: where the agent process *runs*, when you want stronger isolation
than a normal Pod. Off by default and behind feature flags.

**agent-sandbox** — an execution workspace provider using
[kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox).
See [Agent sandbox](../concepts/agent-sandbox.md).

**Substrate** — an execution workspace provider that runs agents inside gVisor-isolated
actors. See [Substrate](../concepts/substrate.md).

**Suspend and cold resume** — pausing a workspace so its disk survives but nothing runs,
then starting fresh against that disk later. Orka never restores process memory, because
a restored process would also restore credentials that should have expired.

## Other

**Gateway** — authenticated ingress and reliable outbound delivery for external systems
(chat platforms, event buses). See [Gateways](../operations/gateways.md).

**Repository monitor** — a durable queue that watches a GitHub repository and runs review
or repair Tasks against pull requests. See
[Repository monitors](../guides/repository-monitors.md).

**Artifact** — a file produced by a Task, stored by the controller and downloadable
afterwards.

**Execution event** — a durable, ordered record of something that happened during a Task.
See [Execution events](execution-events.md).
