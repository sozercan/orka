---
slug: /api-authorization
description: "External API permissions, namespace resolution, and Kubernetes RBAC grant examples."
---

# API authorization

Kubernetes TokenReview authenticates a caller. Orka then submits a
SubjectAccessReview for each permission listed below using that caller's username,
UID, groups, and extra attributes. The permission must cover the final namespace,
resource, verb, and name. Missing clients, review errors, denied decisions, and
ambiguous results return `403` before the requested operation runs.

The table covers all 129 authenticated external route registrations, including 50
non-GET registrations under `/api/v1` and the OpenAI and Anthropic compatibility
routes. `GET /api/v1/auth/validate` and `GET /api/v1/auth/whoami` only validate or
report the authenticated identity. They do not access tenant resources and require
no resource grant. Native chat routes are registered only when chat is enabled.

OIDC callers retain the trusted-subject allowlist and namespace policy. Transaction
tokens retain their `off`, `audit`, and `enforce` scope and `tctx` policies. Neither
identity is converted into a TokenReview identity for these checks. Existing
route-specific restrictions still apply. See [Security](../concepts/security.md)
and [Transaction Token integration](../concepts/transaction-tokens.md).

Health probes, static UI files, Gateway adapter ingress, GitHub webhooks, and
internal worker/ACP routes are outside this inventory. Their existing probe,
Gateway-bound Secret, HMAC, or internal credential policies are unchanged.

## Resources and compound operations

CRD operations use the actual Kubernetes resource and CRUD verb. Stored records
use namespaced virtual resources in SubjectAccessReview requests. They need RBAC
rules, but no new CRDs. `resource/subresource` notation maps to separate SAR
resource and subresource fields and to the same slash-separated string in RBAC.

- `sessions`, `memories`, and `memoryproposals` authorize their stored records.
  Proposal `review` and `apply` are distinct custom verbs on `memoryproposals`,
  not CRUD updates. Applying also requires `create` on `memories`. Returning an
  already-applied memory requires `get` on its final referenced memory ID.
- `chats` with `create` authorizes provider invocation and chat-session work.
  `chats/config` with `get` authorizes configuration reads. Nested tools require
  their own permissions for actual Task, Agent, Secret, and other resource
  operations, plus any applicable Gateway checks. A chat grant alone does not
  grant those tool operations.
  Supplying `sessionId` to native chat also requires `get` and `update` on that
  `sessions` name before reading history or writing the session. Supplying
  `agentRef` requires `get` on that `agents` name. Both use the resolved chat
  namespace. A newly generated chat session is covered by `create chats`.
- `repositoryscans/threatmodel`, `repositoryscans/scans`,
  `repositoryscans/slices`, `repositoryscans/droppedfindings`, and
  `repositoryscans/findings` authorize stored data or actions for one named
  RepositoryScan. `securityfindings` and its subresources use the finding ID.
- `repositorymonitors/runs`, `repositorymonitors/items`, and
  `repositorymonitors/commands` use the parent monitor name. `monitorcommands`,
  `monitoractions`, `monitorworkactions`, `monitorimplementationjobs`,
  `monitormutations`, and `monitorevents` authorize the corresponding stored
  ledgers. Their collection filters do not narrow a namespace-wide `list` grant.
- `gatewayevents` and `gatewaydeliveries` are virtual resources in
  `gateway.orka.ai`. Their permissions are additional to the current Gateway
  identity and access checks.

The HTTP method does not define the permission. `PUT /api/v1/agents/:name` patches
the Kubernetes Agent, so it requires `patch` on `agents`. Approval decisions require
`update` on `tasks/approvals` and `patch` on the parent Task. A monitor run or
command also needs `patch` on its RepositoryMonitor. A scan, validation, or patch
request that creates a Task also needs `create` on `tasks` before any work is
queued. `POST /api/v1/security/findings/:id/pull-request` only returns a stored PR
receipt, so its permission is `get`, despite the HTTP method.

Workspace-class `use` remains a separate check for every authenticated identity,
including OIDC and transaction-token callers. When a Task or workspace-backed Tool
references a class, grant `use` on `executionworkspaceclasses` in
`workspace.orka.ai`, with the selected class name and the Task or Tool namespace.
Gateway-owned Tasks and ledger records retain their current namespace/Gateway UID
checks. General session APIs continue to exclude Gateway-owned sessions.

## Namespace and name sources

The namespace column identifies the explicit request field. The first non-empty
field wins where two fields are listed. Other request fields do not override it.

| Code | Explicit namespace source |
| --- | --- |
| `Q` | `namespace` query parameter |
| `C` | JSON `namespace`, then JSON `metadata.namespace`; query ignored |
| `M` | JSON `metadata.namespace`; top-level namespace and query ignored |
| `B` | JSON `namespace`; query and metadata ignored |
| `QB` | `namespace` query parameter, then JSON `namespace` |
| `BQ` | JSON `namespace`, then `namespace` query parameter |
| `cluster` | Empty SAR namespace; GatewayClass is cluster-scoped |

A configured `--watch-namespace` is the only allowed target. An explicit mismatch
returns `403`; omitting an explicit namespace selects the watched namespace.
Without a watch namespace, resolution is explicit namespace, then the identity's
namespace, then `default`. With `--enforce-namespace-isolation`, the identity must
carry a namespace and it must match the resolved namespace. Resolving a namespace
does not itself grant access to it.

The name column is the SAR name, taken from the indicated URL parameter. `empty`
means no name, including Kubernetes collection creates. A named virtual action
uses the parent name or record ID even when its verb is `create` or `list`.
Proposal review, apply, and archive trim the proposal ID to match store lookup;
noncanonical namespaces for these actions are rejected.

## Route permissions

All additional permissions use the row's resolved namespace unless specified
otherwise. In the additional-checks column:

- `Gateway read` means `get` on `gateway.orka.ai/gateways`, using the Gateway's
  bound namespace and name, when the Task is Gateway-owned. Task collection
  responses filter out Gateway-owned records the caller cannot read.
- `Gateway operate` requires that same `get` plus `update` on the bound Gateway.
- `Class use` means the conditional workspace-class check described above, using
  the final Task or Tool configuration.

| Method | Path | API group | Resource/subresource | Verb | Name | Namespace | Additional checks |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/tasks` | `core.orka.ai` | `tasks` | `create` | empty | `C` | Class use |
| `GET` | `/api/v1/tasks` | `core.orka.ai` | `tasks` | `list` | empty | `Q` | Gateway read |
| `GET` | `/api/v1/tasks/:id` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | Gateway read |
| `DELETE` | `/api/v1/tasks/:id` | `core.orka.ai` | `tasks` | `delete` | `:id` | `Q` | Gateway operate |
| `GET` | `/api/v1/tasks/:id/logs` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | Gateway read |
| `GET` | `/api/v1/tasks/:id/events` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | Gateway read |
| `GET` | `/api/v1/tasks/:id/stream` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | Gateway read |
| `GET` | `/api/v1/tasks/:id/trace` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | Gateway read |
| `GET` | `/api/v1/tasks/:id/approvals` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | Gateway read |
| `POST` | `/api/v1/tasks/:id/approvals/:approvalID/decision` | `core.orka.ai` | `tasks/approvals` | `update` | `:id` | `Q` | `patch` on `core.orka.ai/tasks`, `:id`; Gateway operate |
| `POST` | `/api/v1/tasks/:id/fork` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | `create` on `core.orka.ai/tasks`, empty name; Gateway read; Class use |
| `GET` | `/api/v1/tasks/:id/result` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | Gateway read |
| `GET` | `/api/v1/tasks/:id/plan` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | Gateway read |
| `GET` | `/api/v1/tasks/:id/children` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | `list` on `core.orka.ai/tasks`, empty name; Gateway read |
| `GET` | `/api/v1/tasks/:id/artifacts` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | Gateway read |
| `GET` | `/api/v1/tasks/:id/artifacts/:filename` | `core.orka.ai` | `tasks` | `get` | `:id` | `Q` | Gateway read |
| `GET` | `/api/v1/sessions` | `core.orka.ai` | `sessions` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/sessions/:id` | `core.orka.ai` | `sessions` | `get` | `:id` | `Q` | none |
| `GET` | `/api/v1/sessions/:id/events` | `core.orka.ai` | `sessions` | `get` | `:id` | `Q` | none |
| `GET` | `/api/v1/sessions/:id/stream` | `core.orka.ai` | `sessions` | `get` | `:id` | `Q` | none |
| `DELETE` | `/api/v1/sessions/:id` | `core.orka.ai` | `sessions` | `delete` | `:id` | `Q` | none |
| `GET` | `/api/v1/gatewayclasses` | `gateway.orka.ai` | `gatewayclasses` | `list` | empty | `cluster` | none |
| `GET` | `/api/v1/gatewayclasses/:name` | `gateway.orka.ai` | `gatewayclasses` | `get` | `:name` | `cluster` | none |
| `GET` | `/api/v1/gateways` | `gateway.orka.ai` | `gateways` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/gateways/:name` | `gateway.orka.ai` | `gateways` | `get` | `:name` | `Q` | none |
| `GET` | `/api/v1/gatewaybindings` | `gateway.orka.ai` | `gatewaybindings` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/gatewaybindings/:name` | `gateway.orka.ai` | `gatewaybindings` | `get` | `:name` | `Q` | none |
| `GET` | `/api/v1/gateway-events` | `gateway.orka.ai` | `gatewayevents` | `list` | empty | `Q` | `get gateway.orka.ai/gateways` with `gateway` query name, or unnamed `list gateways` without it |
| `GET` | `/api/v1/gateway-events/:id` | `gateway.orka.ai` | `gatewayevents` | `get` | `:id` | `Q` | `get gateway.orka.ai/gateways` with record Gateway name |
| `GET` | `/api/v1/gateway-deliveries` | `gateway.orka.ai` | `gatewaydeliveries` | `list` | empty | `Q` | `get gateway.orka.ai/gateways` with `gateway` query name, or unnamed `list gateways` without it |
| `GET` | `/api/v1/gateway-deliveries/:id` | `gateway.orka.ai` | `gatewaydeliveries` | `get` | `:id` | `Q` | `get gateway.orka.ai/gateways` with record Gateway name |
| `POST` | `/api/v1/gateway-deliveries/:id/retry` | `gateway.orka.ai` | `gatewaydeliveries` | `update` | `:id` | `Q` | `get` and `update` on `gateway.orka.ai/gateways` with record Gateway name |
| `GET` | `/api/v1/memories` | `core.orka.ai` | `memories` | `list` | empty | `Q` | none |
| `POST` | `/api/v1/memories` | `core.orka.ai` | `memories` | `create` | empty | `B` | none |
| `GET` | `/api/v1/memories/:id` | `core.orka.ai` | `memories` | `get` | `:id` | `Q` | none |
| `PUT` | `/api/v1/memories/:id` | `core.orka.ai` | `memories` | `update` | `:id` | `QB` | none |
| `DELETE` | `/api/v1/memories/:id` | `core.orka.ai` | `memories` | `delete` | `:id` | `Q` | none |
| `POST` | `/api/v1/memories/:id/disable` | `core.orka.ai` | `memories` | `update` | `:id` | `Q` | none |
| `POST` | `/api/v1/memories/:id/enable` | `core.orka.ai` | `memories` | `update` | `:id` | `Q` | none |
| `GET` | `/api/v1/memory-proposals` | `core.orka.ai` | `memoryproposals` | `list` | empty | `Q` | none |
| `POST` | `/api/v1/memory-proposals` | `core.orka.ai` | `memoryproposals` | `create` | empty | `B` | none |
| `GET` | `/api/v1/memory-proposals/:id` | `core.orka.ai` | `memoryproposals` | `get` | `:id` | `Q` | none |
| `POST` | `/api/v1/memory-proposals/:id/review` | `core.orka.ai` | `memoryproposals` | `review` | `:id`, trimmed | `BQ` | none |
| `POST` | `/api/v1/memory-proposals/:id/apply` | `core.orka.ai` | `memoryproposals` | `apply` | `:id`, trimmed | `BQ` | `create` on `core.orka.ai/memories`, empty name; If already applied, `get core.orka.ai/memories` with final referenced memory ID |
| `POST` | `/api/v1/memory-proposals/:id/archive` | `core.orka.ai` | `memoryproposals` | `update` | `:id`, trimmed | `Q` | none |
| `GET` | `/api/v1/providers` | `core.orka.ai` | `providers` | `list` | empty | `Q` | none |
| `POST` | `/api/v1/providers` | `core.orka.ai` | `providers` | `create` | empty | `C` | none |
| `GET` | `/api/v1/providers/:name` | `core.orka.ai` | `providers` | `get` | `:name` | `Q` | none |
| `PUT` | `/api/v1/providers/:name` | `core.orka.ai` | `providers` | `update` | `:name` | `Q` | none |
| `DELETE` | `/api/v1/providers/:name` | `core.orka.ai` | `providers` | `delete` | `:name` | `Q` | none |
| `GET` | `/api/v1/tools` | `core.orka.ai` | `tools` | `list` | empty | `Q` | none |
| `POST` | `/api/v1/tools` | `core.orka.ai` | `tools` | `create` | empty | `C` | Class use |
| `GET` | `/api/v1/tools/:name` | `core.orka.ai` | `tools` | `get` | `:name` | `Q` | none |
| `PUT` | `/api/v1/tools/:name` | `core.orka.ai` | `tools` | `update` | `:name` | `Q` | Class use |
| `DELETE` | `/api/v1/tools/:name` | `core.orka.ai` | `tools` | `delete` | `:name` | `Q` | none |
| `GET` | `/api/v1/runtime-pools` | `core.orka.ai` | `runtimepools` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/runtime-pools/:name` | `core.orka.ai` | `runtimepools` | `get` | `:name` | `Q` | none |
| `GET` | `/api/v1/agent-runtimes` | `core.orka.ai` | `agentruntimes` | `list` | empty | `Q` | none |
| `POST` | `/api/v1/agent-runtimes` | `core.orka.ai` | `agentruntimes` | `create` | empty | `M` | none |
| `GET` | `/api/v1/agent-runtimes/:name` | `core.orka.ai` | `agentruntimes` | `get` | `:name` | `Q` | none |
| `PUT` | `/api/v1/agent-runtimes/:name` | `core.orka.ai` | `agentruntimes` | `update` | `:name` | `Q` | none |
| `DELETE` | `/api/v1/agent-runtimes/:name` | `core.orka.ai` | `agentruntimes` | `delete` | `:name` | `Q` | none |
| `POST` | `/api/v1/agents` | `core.orka.ai` | `agents` | `create` | empty | `C` | none |
| `GET` | `/api/v1/agents` | `core.orka.ai` | `agents` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/agents/:name` | `core.orka.ai` | `agents` | `get` | `:name` | `Q` | none |
| `PUT` | `/api/v1/agents/:name` | `core.orka.ai` | `agents` | `patch` | `:name` | `Q` | none |
| `DELETE` | `/api/v1/agents/:name` | `core.orka.ai` | `agents` | `delete` | `:name` | `Q` | none |
| `POST` | `/api/v1/skills` | `core.orka.ai` | `skills` | `create` | empty | `C` | none |
| `GET` | `/api/v1/skills` | `core.orka.ai` | `skills` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/skills/:name` | `core.orka.ai` | `skills` | `get` | `:name` | `Q` | none |
| `GET` | `/api/v1/skills/:name/content` | `core.orka.ai` | `skills` | `get` | `:name` | `Q` | none |
| `PUT` | `/api/v1/skills/:name` | `core.orka.ai` | `skills` | `update` | `:name` | `Q` | none |
| `DELETE` | `/api/v1/skills/:name` | `core.orka.ai` | `skills` | `delete` | `:name` | `Q` | none |
| `POST` | `/api/v1/security/repositories` | `core.orka.ai` | `repositoryscans` | `create` | empty | `C` | none |
| `GET` | `/api/v1/security/repositories` | `core.orka.ai` | `repositoryscans` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/security/repositories/:name` | `core.orka.ai` | `repositoryscans` | `get` | `:name` | `Q` | none |
| `PUT` | `/api/v1/security/repositories/:name` | `core.orka.ai` | `repositoryscans` | `update` | `:name` | `Q` | none |
| `DELETE` | `/api/v1/security/repositories/:name` | `core.orka.ai` | `repositoryscans` | `delete` | `:name` | `Q` | none |
| `GET` | `/api/v1/security/repositories/:name/threat-model` | `core.orka.ai` | `repositoryscans/threatmodel` | `get` | `:name` | `Q` | none |
| `PUT` | `/api/v1/security/repositories/:name/threat-model` | `core.orka.ai` | `repositoryscans/threatmodel` | `update` | `:name` | `Q` | none |
| `GET` | `/api/v1/security/repositories/:name/scans` | `core.orka.ai` | `repositoryscans/scans` | `list` | `:name` | `Q` | none |
| `POST` | `/api/v1/security/repositories/:name/scans` | `core.orka.ai` | `repositoryscans/scans` | `create` | `:name` | `Q` | `create` on `core.orka.ai/tasks`, empty name; `patch` on `core.orka.ai/repositoryscans/status`, `:name`; Class use |
| `GET` | `/api/v1/security/repositories/:name/slices` | `core.orka.ai` | `repositoryscans/slices` | `list` | `:name` | `Q` | none |
| `GET` | `/api/v1/security/repositories/:name/slices/:sliceID` | `core.orka.ai` | `repositoryscans/slices` | `get` | `:name` | `Q` | none |
| `GET` | `/api/v1/security/repositories/:name/dropped-findings` | `core.orka.ai` | `repositoryscans/droppedfindings` | `list` | `:name` | `Q` | none |
| `GET` | `/api/v1/security/repositories/:name/findings` | `core.orka.ai` | `repositoryscans/findings` | `list` | `:name` | `Q` | none |
| `GET` | `/api/v1/security/findings/:id` | `core.orka.ai` | `securityfindings` | `get` | `:id` | `Q` | none |
| `POST` | `/api/v1/security/findings/:id/dismiss` | `core.orka.ai` | `securityfindings` | `update` | `:id` | `Q` | none |
| `POST` | `/api/v1/security/findings/:id/reopen` | `core.orka.ai` | `securityfindings` | `update` | `:id` | `Q` | none |
| `POST` | `/api/v1/security/findings/:id/validate` | `core.orka.ai` | `securityfindings/validation` | `create` | `:id` | `Q` | `create` on `core.orka.ai/tasks`, empty name; Class use |
| `POST` | `/api/v1/security/findings/:id/patch` | `core.orka.ai` | `securityfindings/patches` | `create` | `:id` | `Q` | `create` on `core.orka.ai/tasks`, empty name; Class use |
| `GET` | `/api/v1/security/findings/:id/patches` | `core.orka.ai` | `securityfindings/patches` | `list` | `:id` | `Q` | none |
| `POST` | `/api/v1/security/findings/:id/pull-request` | `core.orka.ai` | `securityfindings/pullrequest` | `get` | `:id` | `Q` | Stored PR receipt read only |
| `POST` | `/api/v1/monitors/repositories` | `core.orka.ai` | `repositorymonitors` | `create` | empty | `C` | none |
| `GET` | `/api/v1/monitors/repositories` | `core.orka.ai` | `repositorymonitors` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/monitors/repositories/:name` | `core.orka.ai` | `repositorymonitors` | `get` | `:name` | `Q` | none |
| `PUT` | `/api/v1/monitors/repositories/:name` | `core.orka.ai` | `repositorymonitors` | `update` | `:name` | `Q` | none |
| `DELETE` | `/api/v1/monitors/repositories/:name` | `core.orka.ai` | `repositorymonitors` | `delete` | `:name` | `Q` | none |
| `POST` | `/api/v1/monitors/repositories/:name/runs` | `core.orka.ai` | `repositorymonitors/runs` | `create` | `:name` | `Q` | `patch` on `core.orka.ai/repositorymonitors`, `:name` |
| `GET` | `/api/v1/monitors/repositories/:name/runs` | `core.orka.ai` | `repositorymonitors/runs` | `list` | `:name` | `Q` | none |
| `GET` | `/api/v1/monitors/repositories/:name/items` | `core.orka.ai` | `repositorymonitors/items` | `list` | `:name` | `Q` | none |
| `POST` | `/api/v1/monitors/repositories/:name/commands` | `core.orka.ai` | `repositorymonitors/commands` | `create` | `:name` | `Q` | `patch` on `core.orka.ai/repositorymonitors`, `:name` |
| `GET` | `/api/v1/monitors/commands` | `core.orka.ai` | `monitorcommands` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/monitors/commands/:id` | `core.orka.ai` | `monitorcommands` | `get` | `:id` | `Q` | none |
| `GET` | `/api/v1/monitors/actions` | `core.orka.ai` | `monitoractions` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/monitors/actions/:id` | `core.orka.ai` | `monitoractions` | `get` | `:id` | `Q` | none |
| `GET` | `/api/v1/monitors/work-actions` | `core.orka.ai` | `monitorworkactions` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/monitors/work-actions/:id` | `core.orka.ai` | `monitorworkactions` | `get` | `:id` | `Q` | none |
| `GET` | `/api/v1/monitors/implementation-jobs` | `core.orka.ai` | `monitorimplementationjobs` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/monitors/implementation-jobs/:id` | `core.orka.ai` | `monitorimplementationjobs` | `get` | `:id` | `Q` | none |
| `GET` | `/api/v1/monitors/implementation-jobs/:id/patch-preview` | `core.orka.ai` | `monitorimplementationjobs` | `get` | `:id` | `Q` | none |
| `GET` | `/api/v1/monitors/mutations` | `core.orka.ai` | `monitormutations` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/monitors/mutations/:id` | `core.orka.ai` | `monitormutations` | `get` | `:id` | `Q` | none |
| `GET` | `/api/v1/monitors/events` | `core.orka.ai` | `monitorevents` | `list` | empty | `Q` | none |
| `GET` | `/api/v1/substrate-actor-pools` | `core.orka.ai` | `substrateactorpools` | `list` | empty | `Q` | none |
| `POST` | `/api/v1/substrate-actor-pools` | `core.orka.ai` | `substrateactorpools` | `create` | empty | `C` | none |
| `GET` | `/api/v1/substrate-actor-pools/:name` | `core.orka.ai` | `substrateactorpools` | `get` | `:name` | `Q` | none |
| `PUT` | `/api/v1/substrate-actor-pools/:name` | `core.orka.ai` | `substrateactorpools` | `update` | `:name` | `Q` | none |
| `DELETE` | `/api/v1/substrate-actor-pools/:name` | `core.orka.ai` | `substrateactorpools` | `delete` | `:name` | `Q` | none |
| `GET` | `/api/v1/auth/validate` | none | none | identity only | empty | not applicable | Authenticated identity only |
| `GET` | `/api/v1/auth/whoami` | none | none | identity only | empty | not applicable | Authenticated identity only |
| `GET` | `/api/v1/secrets` | `""` | `secrets` | `list` | empty | `Q` | none |
| `POST` | `/api/v1/chat` | `core.orka.ai` | `chats` | `create` | empty | `B` | Supplied JSON `sessionId`: `get` and `update` on `core.orka.ai/sessions` with that name; supplied JSON `agentRef`: `get core.orka.ai/agents` with that name; each nested tool requires its own resource permissions |
| `GET` | `/api/v1/chat/config` | `core.orka.ai` | `chats/config` | `get` | empty | `Q` | none |
| `DELETE` | `/api/v1/chat/:sessionId` | `core.orka.ai` | `sessions` | `delete` | `:sessionId` | `Q` | none |
| `POST` | `/openai/v1/chat/completions` | `core.orka.ai` | `chats` | `create` | empty | `Q` | Each nested tool requires its own resource permissions; custom Tool metadata requires unnamed `list core.orka.ai/tools` and is omitted on denial |
| `GET` | `/openai/v1/models` | `core.orka.ai` | `providers` | `list` | empty | `Q` | none |
| `POST` | `/anthropic/v1/messages` | `core.orka.ai` | `chats` | `create` | empty | `Q` | Each nested tool requires its own resource permissions; custom Tool metadata requires unnamed `list core.orka.ai/tools` and is omitted on denial |
| `GET` | `/anthropic/v1/models` | `core.orka.ai` | `providers` | `list` | empty | `Q` | none |

## Grant access

Use a RoleBinding in the installation namespace. A ClusterRoleBinding would grant
namespaced permissions across the cluster. The Kustomize bundle includes
`orka-api-viewer-role` and `orka-api-editor-role`. The viewer covers the read
permissions above. The editor also covers the listed Orka mutations and their
secondary permissions. Nested tools that run Kubernetes workloads need separate
workload grants, including `get` on `pods/log` when reading Pod logs. Neither
helper grants Secrets or workspace-class use.
Kubernetes `code_exec` preflights `create` and `delete` on its temporary Secrets,
ServiceAccounts, Jobs, and optional NetworkPolicies before creating any of them.
The delete permissions cover cleanup after completion or failed setup.
The source files in `config/rbac` have unprefixed names when applied directly.

Existing Task helper roles retain session access; Task editor/admin roles also
grant `update` on `tasks/approvals`. Gateway helper roles include ledger reads;
Gateway editor/admin roles also grant delivery retry. GatewayClass reads require
a cluster-scoped binding, such as the existing `orka-gatewayclass-viewer-role`.
A namespaced RoleBinding cannot grant those cluster-scoped reads.

The Helm client Role includes virtual reads, chat creation and continuation,
approval decisions, and monitor actions alongside its existing resource grants.
Memory review/apply, security mutations, and Gateway retry require separate
operator grants. Its read-only Agent, Tool, and Provider permissions do not
authorize nested chat tools to mutate those resources.

For example, bind the installed API viewer to an existing ServiceAccount:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: api-reader
  namespace: team-a
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: orka-api-viewer-role
subjects:
- kind: ServiceAccount
  name: api-client
  namespace: team-a
```

For a narrower grant, this Role allows an existing ServiceAccount to read, review,
and apply one proposal. The separate unnamed `create memories` grant is required
for apply. Reading an already-applied memory also needs `get memories` with that
memory's ID; add that grant only for records the caller may read.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: apply-selected-proposal
  namespace: team-a
rules:
- apiGroups: [core.orka.ai]
  resources: [memoryproposals]
  resourceNames: [proposal-123]
  verbs: [get, review, apply]
- apiGroups: [core.orka.ai]
  resources: [memories]
  verbs: [create]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: apply-selected-proposal
  namespace: team-a
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: apply-selected-proposal
subjects:
- kind: ServiceAccount
  name: memory-reviewer
  namespace: team-a
```

`GET /api/v1/secrets` returns names only, but it requires actual core Kubernetes
`list` access on `secrets`. That same grant permits listing Secret objects through
the Kubernetes API, including their data. Keep it separate from general viewer
roles. The API group is the empty string, not `core.orka.ai`.

Grant workspace-class use with an explicit `resourceNames` entry on a Role in the
Task or Tool namespace. Broader API edit permission does not substitute for it:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: use-reviewed-workspace
  namespace: team-a
rules:
- apiGroups: [workspace.orka.ai]
  resources: [executionworkspaceclasses]
  resourceNames: [reviewed-workspace]
  verbs: [use]
```

Bind this Role to the caller with a RoleBinding in `team-a`, as above. Check
permissions using the same resource, namespace, verb, and name as the route. A
successful `/auth/validate` or `/auth/whoami` response proves authentication only.
