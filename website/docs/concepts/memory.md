---
slug: /memory
description: "The namespace-scoped memory layer: durable memories, proposals, and why writing one is a review request."
---

# Memory

Orka provides a namespace-scoped memory layer for AI worker tasks. It is designed for shared Kubernetes environments where memory should be useful, auditable, and safe by default.

The current model has three related concepts:

| Concept | Purpose | Persistence |
|---------|---------|-------------|
| **Durable memory** | Reviewed project facts, decisions, conventions, or reusable context that can help future tasks | Stored in SQLite as namespace-scoped `memories` records |
| **Memory proposal** | A worker- or user-submitted suggestion for memory, policy, workflow, or skill changes | Stored in SQLite as `memory_proposals` for review |
| **Transcript search** | Compact search over prior session messages | Stored in SQLite session transcript tables |

Memory proposal review is **non-applying**. Reviewing a proposal as accepted or rejected records the decision only. Accepted proposals with `type: "memory"` can then be applied explicitly, which creates (or idempotently returns) durable memory linked back to the proposal.

## Memory proposal lifecycle

Memory proposals follow an explicit review/apply lifecycle:

- New proposals start with `status: "pending"`.
- Review decisions only apply to pending proposals and record either `status: "accepted"` or `status: "rejected"`; review does not create durable memory.
- Accepted proposals with `type: "memory"` require an explicit apply request before they become durable memory.
- Rejected or archived proposals cannot be applied.

Accepted memory proposals can suggest durable memory tags by including a `Tags:` line in the proposal `description`. The apply step reads tags from the first `Tags:` line only, normalizes them, and ignores later `Tags:` lines. Example:

```text
Reusable release procedure discovered during task execution.
Tags: release, testing
```

## Worker behavior

When an AI worker can reach the controller internal API, it loads reviewed durable memories before invoking the model. The durable memory section is appended to the system prompt as background project context, separate from the current session transcript.

When coordination is enabled on an Agent, Orka auto-injects the memory tool family along with the coordination tools:

| Tool | Purpose |
|------|---------|
| `recall_memory` | Query durable namespace-scoped memories by text, tags, provenance, and limit |
| `remember` | Submit a durable memory proposal for review |
| `propose_memory` | Submit a memory-adjacent governance proposal such as memory, policy, workflow, or skill content |
| `search_transcript` | Search prior session transcripts and return compact snippets |

`remember` and `propose_memory` intentionally submit proposals instead of mutating durable memory directly. This keeps shared memory reviewable and prevents live model output from silently changing future task context.

### Memory context bounds

Durable memory injection is bounded to keep prompts stable:

| Setting | Default | Purpose |
|---------|---------|---------|
| `ORKA_MEMORY_CONTEXT_LIMIT` | `5` | Maximum memories to inject into the worker prompt |
| `ORKA_MEMORY_CONTEXT_MAX_CHARS` | `6000` | Maximum durable-memory prompt section size |

The worker also truncates individual memory entries before prompt injection. Memory infrastructure is best-effort: failure to load memory should not prevent task execution.

## Durable memory API

All public endpoints require a ServiceAccount bearer token and are under `/api/v1`.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/memories` | GET | List durable memories |
| `/api/v1/memories` | POST | Create durable memory |
| `/api/v1/memories/:id` | GET | Get durable memory |
| `/api/v1/memories/:id` | PUT | Update durable memory |
| `/api/v1/memories/:id` | DELETE | Soft-delete durable memory |
| `/api/v1/memories/:id/disable` | POST | Disable memory for normal recall |
| `/api/v1/memories/:id/enable` | POST | Re-enable memory for normal recall |

Supported list query parameters:

| Parameter | Description |
|-----------|-------------|
| `namespace` | Namespace to query |
| `query` or `q` | Text search over memory content and tags |
| `sessionName` | Filter by session provenance |
| `agentName` | Filter by agent provenance |
| `taskName` | Filter by task provenance |
| `parentTask` | Filter by parent task provenance |
| `source` | Filter by source such as `task`, `session`, `user`, `system`, `memory_proposal`, or `e2e` |
| `tags` | Comma-separated tags |
| `ids` | Comma-separated memory IDs |
| `includeDisabled` | Include disabled memories when `true` |
| `includeDeleted` | Include soft-deleted memories when `true` |
| `limit` | Maximum rows to return |

Example:

```bash
TOKEN=$(kubectl create token orka-client -n orka-system)

curl -sS http://localhost:8080/api/v1/memories \
  -H "Authorization: Bearer $TOKEN" \
  -G \
  --data-urlencode namespace=orka-system \
  --data-urlencode query="release checklist"
```

Create durable memory:

```bash
curl -sS -X POST http://localhost:8080/api/v1/memories \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": "orka-system",
    "source": "user",
    "content": "Release tasks should run make lint-fix and make test before merging.",
    "tags": ["release", "testing"]
  }'
```

## Memory proposal API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/memory-proposals` | GET | List memory proposals |
| `/api/v1/memory-proposals` | POST | Create a memory proposal |
| `/api/v1/memory-proposals/:id` | GET | Get a memory proposal |
| `/api/v1/memory-proposals/:id/review` | POST | Record a review decision without applying it |
| `/api/v1/memory-proposals/:id/apply` | POST | Apply an accepted `memory` proposal into durable memory |
| `/api/v1/memory-proposals/:id/archive` | POST | Archive a proposal without applying it |

Supported list query parameters:

| Parameter | Description |
|-----------|-------------|
| `namespace` | Namespace to query |
| `taskName` | Filter by task provenance |
| `agentName` | Filter by agent provenance |
| `type` | Filter by proposal type, for example `memory`, `skill`, `policy`, or `workflow` |
| `status` | Filter by status such as `pending`, `accepted`, `rejected`, `applied`, or `archived` |
| `query` or `q` | Text search over title, description, content, and skill name |
| `limit` | Maximum rows to return |

Create a proposal:

```bash
curl -sS -X POST http://localhost:8080/api/v1/memory-proposals \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": "orka-system",
    "taskName": "release-review",
    "agentName": "release-agent",
    "type": "memory",
    "title": "Release validation command",
    "description": "Reusable release procedure discovered during task execution.\nTags: release, testing",
    "content": "Run make lint-fix and make test before merging release PRs."
  }'
```

Review a proposal:

```bash
curl -sS -X POST "http://localhost:8080/api/v1/memory-proposals/mprop-example/review?namespace=orka-system" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "accepted",
    "reviewer": "platform-team",
    "reviewNote": "Valid reusable release procedure."
  }'
```

Review returns `204 No Content`. It records governance state only; it does not apply the proposal as durable memory.

Apply an accepted memory proposal:

```bash
curl -sS -X POST "http://localhost:8080/api/v1/memory-proposals/mprop-example/apply?namespace=orka-system" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "appliedBy": "platform-team"
  }'
```

Apply returns the durable memory as JSON. The created memory has `source: "memory_proposal"` and `sourceProposalId` set to the proposal ID, and the proposal is updated to `status: "applied"` with `appliedMemoryId`, `appliedBy`, and `appliedAt`. Repeating the apply request is idempotent and returns the same durable memory. Only accepted proposals with `type: "memory"` can be applied; rejected and archived proposals cannot be applied.

## Safety model

- Store durable memories only for reusable project facts, decisions, conventions, or procedures.
- Do not store secrets, credentials, tokens, raw transcripts, one-off task status, or sensitive personal data.
- Memory and proposal persistence passes content through sensitive-text redaction.
- Disabled and deleted memories are excluded from normal recall by default.
- Memory writes are namespace-scoped and authenticated through the same ServiceAccount-token API model as other Orka APIs.
- Applying accepted memory proposals preserves proposal-to-memory linkage for auditability.

## Validation

The live copilot-proxy E2E suite validates the native `type: ai` memory path
with a real model-backed worker. The proxy is test infrastructure and is
separate from the built-in Copilot ACP RuntimePool path:

- durable memory can be pre-seeded through the API
- a live worker receives memory tools through `ORKA_AI_TOOLS`
- the worker executes `recall_memory`, `remember`, `propose_memory`, and `search_transcript`
- durable recall does not create duplicate durable memory
- proposed memory remains a proposal until it is explicitly applied
- proposal review persists accepted/rejected state without applying the proposal
- accepted memory proposals apply idempotently into linked durable memory

Deterministic unit and integration tests cover store behavior, API handlers, tool registration, prompt composition, and worker/tool plumbing.

## Current limitations

- Accepted proposals are not automatically converted to durable memories; they require the explicit apply endpoint.
- Only accepted proposals with `type: "memory"` can be applied into durable memory.
- Durable memory search currently uses store-level filters and substring matching rather than semantic ranking.
- Transcript search returns snippets, not model-generated summaries.
- External memory backends are not part of the default implementation.
