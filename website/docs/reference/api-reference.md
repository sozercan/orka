---
slug: /api-reference
---

# API Reference

The controller exposes a REST API for programmatic access. All `/api/v1/*` endpoints require authentication. By default Orka accepts Kubernetes ServiceAccount bearer tokens; when configured, external callers can use a valid OIDC JWT or generic context token instead.

## Authentication

Send Kubernetes ServiceAccount and OIDC credentials with the standard bearer token header:

```http
Authorization: Bearer <token>
```

Authentication modes:

- **Kubernetes ServiceAccount token** — default mode. Tokens are validated with the Kubernetes TokenReview API.
- **OIDC JWT** — enabled when the controller is configured with `--oidc-issuer` and `--oidc-audience` (or `ORKA_OIDC_ISSUER` / `ORKA_OIDC_AUDIENCE`). Tokens are validated against the issuer, audience, expiration, RS256 signature, and `--oidc-allowed-subjects`; authorized OIDC callers are assigned `--oidc-namespace` for namespace isolation. If `--oidc-jwks-url` is omitted, Orka discovers the JWKS URL from the issuer metadata.
- **Context token / `transaction-token` TxToken** — enabled with `--context-token-profile=transaction-token`, `--context-token-issuer`, and `--context-token-audience` (or the matching `ORKA_CONTEXT_TOKEN_*` env vars). The built-in profile validates RS256 TxTokens with `typ: txntoken+jwt`, issuer/audience/time claims, `kid`, and required `iat`, `txn`, `scope`, and `req_wl` claims. By default tokens are read from the raw `Txn-Token` header; `Authorization: Bearer` support is opt-in with `--context-token-headers=Txn-Token,Authorization:Bearer`.

```http
Txn-Token: <txntoken+jwt>
```

When a Task is created through OIDC or context-token authentication, Orka stamps the verified caller identity into immutable `spec.requestedBy` (`subject`, `issuer`, `username`, `email`, `groups`, and `roles` when present). Context-token Task creation also stamps immutable `spec.transaction` plus transaction labels/annotations for audit correlation. Clients cannot provide or override `requestedBy` or `transaction`; requests containing top-level or nested `spec.requestedBy`/`spec.transaction` are rejected with `400`. See [Transaction Token integration](../concepts/transaction-tokens.md) for scope/`tctx` authorization, TTS exchange, delegation, and audit behavior.

## Webhooks

GitHub webhooks use HMAC verification instead of bearer-token authentication.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/webhooks/github` | POST | Accept GitHub `issues` / `pull_request` label triggers and pull request events for exact-head repository monitor runs |

The controller requires `ORKA_GITHUB_WEBHOOK_SECRET` and verifies the `X-Hub-Signature-256` header. Label trigger events can create agent Tasks for labels such as `agent:implement`. Pull request events can also queue exact-head `RepositoryMonitor` runs when a matching monitor has `spec.review.exactEventEnabled: true`. See [GitHub Label Triggers](../guides/github-label-triggers.md) for configuration and webhook behavior.

## Tasks

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/tasks` | POST | Create a task |
| `/api/v1/tasks` | GET | List tasks (paginated) |
| `/api/v1/tasks/:id` | GET | Get task details |
| `/api/v1/tasks/:id` | DELETE | Cancel/delete task |
| `/api/v1/tasks/:id/logs` | GET | Stream task logs |
| `/api/v1/tasks/:id/result` | GET | Get task result |
| `/api/v1/tasks/:id/artifacts` | GET | List task artifacts |
| `/api/v1/tasks/:id/artifacts/:filename` | GET | Download a task artifact |
| `/api/v1/tasks/:id/plan` | GET | Get task plan |
| `/api/v1/tasks/:id/children` | GET | Get child tasks |

### Agent Task workspace and delivery schema

`POST /api/v1/tasks` accepts the Task CRD shape. Agent repository configuration belongs at top-level `spec.workspace`; `spec.agentRuntime` contains only per-Task runtime overrides.

| Path | Type | Values/default | Notes |
| --- | --- | --- | --- |
| `spec.workspace.intent` | string | `read` for agent Tasks; `read` or `write` | Immutable effective intent for the attempt. |
| `spec.workspace.gitRepo` | string | empty | Credential-free source repository URL. Embedded credentials, query strings, and fragments are rejected. |
| `spec.workspace.sourceRepository` | object | empty | Optional canonical provider/ID source identity. |
| `spec.workspace.branch` / `ref` | string | empty | Source branch or exact ref/commit/tag. When both are empty, the Publisher resolves and freezes the repository's advertised default branch before execution. |
| `spec.workspace.readCredentialRef.name` | string | empty | Secret used only by the clean-room source clone/read operation. |
| `spec.workspace.publicationGitRepo` | string | empty | Credential-free publication repository URL. |
| `spec.workspace.publicationRepository` | object | empty | Optional canonical provider/ID publication identity. |
| `spec.workspace.publicationReadCredentialRef.name` | string | empty | Target-read Secret used only for publication preflight and independent verification. |
| `spec.workspace.publicationCredentialRef.name` | string | empty | Target-write Secret used only for the exact branch compare-and-swap push. |
| `spec.workspace.forgeCredentialRef.name` | string | empty | Forge API Secret used only for pull-request reconciliation; required when `createPR` is true. |
| `spec.workspace.subPath` | string | empty | Repository subdirectory exposed as workspace root. |
| `spec.workspace.pushBranch` | string | generated for write Tasks when omitted | Publication branch; Orka-generated names use full Task or Session identity entropy. |
| `spec.workspace.prBaseBranch` | string | empty | Pull-request base branch. |
| `spec.workspace.createPR` | boolean | `false` | Reconcile a pull request only after branch publication when true; requires `intent: write`. |
| `spec.agentRuntime.maxTurns` | integer | Agent default | Per-Task prompt-loop limit. |
| `spec.agentRuntime.allowedTools` / `disallowedTools` | list | Agent defaults | Per-Task tool policy override. |
| `spec.agentRuntime.allowBash` | boolean | Agent default | Per-Task bash policy override. |
| `spec.timeout` | duration | `30m` for ACP v2 agent Tasks | Maximum wall-clock duration measured from Task creation, including queue, runtime admission, and prompt execution time. An explicit positive value overrides the default. |

Source read, target read, target write, and forge references are distinct
credential roles. The selected Secret UID/resourceVersion is frozen for the
attempt, and the credential broker releases a value only to the
Workspace/Publisher for the exact active operation. Secret contents are never
copied to Task status or delivered to the ACP process tree.

The durable ACP attempt is exposed in `status.execution`. Workspace validation and publication use `status.delivery`, including publication ID, repository identities, branch, starting/remote/tree/commit SHAs, artifact digest, and optional PR receipt. A Task is not delivered merely because the model reports success; require a terminal verified delivery outcome.

`Task.spec.execution.workspace` is not supported by the current ACP core runtime. Upstream agent-sandbox and Substrate integration is deferred behind the v2 RuntimeSession seam.

### Get Task Plan

Retrieve the autonomous plan state for a task.

**Endpoint:** `GET /api/v1/tasks/{id}/plan`

**Response (200):**
```json
{
  "TaskName": "build-feature",
  "Namespace": "default",
  "Iteration": 3,
  "Summary": "Completed auth module, working on CRUD endpoints",
  "ProgressPct": 40,
  "GoalComplete": false,
  "PlanDocument": "# Plan\n- [x] Auth\n- [ ] CRUD\n...",
  "CreatedAt": "2024-01-15T10:00:00Z",
  "UpdatedAt": "2024-01-15T12:30:00Z"
}
```

**Errors:**
- `404` — No plan found for this task
- `501` — Plan store not configured

## Sessions

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/sessions` | GET | List sessions |
| `/api/v1/sessions/:id` | GET | Get session transcript |
| `/api/v1/sessions/:id` | DELETE | Delete session |


## Memory

Memory endpoints manage namespace-scoped durable memories and reviewable memory proposals. See [Memory](../concepts/memory.md) for the full lifecycle, worker behavior, and examples.

### Durable Memories

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/memories` | GET | List durable memories |
| `/api/v1/memories` | POST | Create durable memory |
| `/api/v1/memories/:id` | GET | Get durable memory |
| `/api/v1/memories/:id` | PUT | Update durable memory |
| `/api/v1/memories/:id` | DELETE | Soft-delete durable memory |
| `/api/v1/memories/:id/disable` | POST | Disable memory for normal recall |
| `/api/v1/memories/:id/enable` | POST | Re-enable memory for normal recall |

Common list query parameters: `namespace`, `query`/`q`, `sessionName`, `agentName`, `taskName`, `parentTask`, `source`, `tags`, `ids`, `includeDisabled`, `includeDeleted`, and `limit`.

### Memory Proposals

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/memory-proposals` | GET | List memory proposals |
| `/api/v1/memory-proposals` | POST | Create a memory proposal |
| `/api/v1/memory-proposals/:id` | GET | Get a memory proposal |
| `/api/v1/memory-proposals/:id/review` | POST | Record a review decision without applying it |
| `/api/v1/memory-proposals/:id/apply` | POST | Apply an accepted `memory` proposal into durable memory |
| `/api/v1/memory-proposals/:id/archive` | POST | Archive a proposal without applying it |

Common list query parameters: `namespace`, `taskName`, `agentName`, `type`, `status`, `query`/`q`, and `limit`. Review and archive return `204 No Content`. Apply accepts optional `appliedBy` and returns the linked durable memory JSON; repeated apply requests return the same memory.

## ACP runtime resources

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/v1/runtime-pools` | GET | List controller-owned RuntimePools and lifecycle/admission/capacity status. |
| `/api/v1/runtime-pools/:name` | GET | Get one RuntimePool. |
| `/api/v1/agent-runtimes` | GET | List external `orka.harness.v2` registrations. |
| `/api/v1/agent-runtimes` | POST | Create an external v2 registration. |
| `/api/v1/agent-runtimes/:name` | GET | Get an external registration and observed capabilities. |
| `/api/v1/agent-runtimes/:name` | PUT | Replace an external registration. |
| `/api/v1/agent-runtimes/:name` | DELETE | Delete an external registration. |

RuntimePools are controller-owned for built-in Codex, OpenCode, Claude, and Copilot Tasks; the public API is read-only. A current-generation ready, strict-governed external registration can be selected through `Agent.spec.runtime.runtimeRef`. Orka revalidates its frozen endpoint, profile, authentication authority, and observed instance before dispatch and recovery mutations.

## Agents

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/agents` | POST | Create an agent |
| `/api/v1/agents` | GET | List agents |
| `/api/v1/agents/:name` | GET | Get agent details |
| `/api/v1/agents/:name` | PUT | Update an agent |
| `/api/v1/agents/:name` | DELETE | Delete an agent |

## Skills

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/skills` | POST | Create a skill |
| `/api/v1/skills` | GET | List skills |
| `/api/v1/skills/:name` | GET | Get skill details |
| `/api/v1/skills/:name/content` | GET | Get raw `spec.content.inline` markdown |
| `/api/v1/skills/:name` | PUT | Update a skill |
| `/api/v1/skills/:name` | DELETE | Delete a skill |


## Generic Gateways

See [Generic Gateway API](gateway-api.md) for the adapter contract, Kubernetes resources, durable ledger endpoints, filters, and retry workflow.

## Tools

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/tools` | GET | List tools (built-in + CRDs) |
| `/api/v1/tools/:name` | GET | Get tool details |

### Tool CRD Schema

`GET /api/v1/tools/:name` returns built-in tool metadata or the full `Tool` CRD. Custom Tool CRDs can call plain HTTP endpoints or MCP servers hosted in durable Substrate actors.

Plain HTTP tools set `spec.http.url` and may inject authentication from a Kubernetes Secret into either the `Authorization: Bearer` header or the JSON request body:

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Tool
metadata:
  name: tavily-search
spec:
  description: "Search the web for current information"
  parameters:
    type: object
    properties:
      query:
        type: string
    required:
      - query
  http:
    url: "https://api.tavily.com/search"
    method: POST
    authSecretRef:
      name: tavily-secret
      key: api-key
    authInject: body
    authBodyKey: api_key
```

MCP actor-backed tools set `spec.mcp.substrateActor` and may omit `spec.http` entirely. Orka creates or reuses the Substrate actor, waits for the MCP endpoint, stores the resolved endpoint in `status.endpoint`, and workers call the MCP tool through JSON-RPC `tools/call` using the Tool name as the MCP tool name.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Tool
metadata:
  name: repo-inspector
spec:
  description: "Inspect repository metadata through an MCP server"
  parameters:
    type: object
    properties:
      message:
        type: string
    required:
      - message
  mcp:
    path: /mcp
    substrateActor:
      templateRef:
        name: orka-mcp
        namespace: ate-demo
      poolRef:
        name: mcp-substrate-pool
      boot: true
```

| Path | Type | Values/default | Notes |
|------|------|----------------|-------|
| `spec.description` | string | required | Description shown to the LLM. |
| `spec.parameters` | JSON Schema | empty | Tool argument schema in function-calling format. |
| `spec.http.url` | string | required for plain HTTP tools | Endpoint called by workers. MCP actor-backed tools may omit it because Orka uses `status.endpoint`. |
| `spec.http.method` | string | default `POST`; allowed `GET`, `POST`, `PUT`, `PATCH`, `DELETE` | HTTP method for plain HTTP tools. MCP actor-backed tools use `POST`. |
| `spec.http.headers` | map | empty | Static headers sent with the request. Reserved token propagation headers cannot be overridden when outbound TxToken propagation is enabled. |
| `spec.http.timeout` | duration | default `30s` | Per-call request timeout. |
| `spec.http.authSecretRef` | Secret key selector | empty | Secret value used as the auth token. Cannot coexist with a direct OutboundAccessPolicy. |
| `spec.http.outboundAccessPolicyRef.name` | string | empty | Same-namespace `OutboundAccessPolicy` required to be Accepted with ResolvedRefs. |
| `spec.http.authInject` | string | default `header`; allowed `header`, `body` | `header` sends `Authorization: Bearer <token>`. `body` injects the token into the JSON request body and is invalid for MCP actor-backed tools. |
| `spec.http.authBodyKey` | string | empty | JSON key used when `authInject: body`. |
| `spec.mcp.path` | string | `/mcp` | HTTP path exposed by the MCP server inside the actor. |
| `spec.mcp.substrateActor.templateRef.name` | string | required | Substrate `ActorTemplate` hosting the MCP server. |
| `spec.mcp.substrateActor.templateRef.namespace` | string | Tool namespace or configured default | Namespace containing the actor template. |
| `spec.mcp.substrateActor.poolRef.name` | string | empty | Optional `SubstrateActorPool` for actor placement and reuse. The pool template must match the MCP actor template. |
| `spec.mcp.substrateActor.poolRef.namespace` | string | Tool namespace | Namespace containing the referenced actor pool. |
| `spec.mcp.substrateActor.boot` | boolean | `false` | Boots the actor from scratch on first resume; later reconciles reuse an already booted actor. |
| `status.available` | boolean | false | Whether the controller can reach the resolved endpoint. |
| `status.endpoint` | string | empty | Resolved non-secret endpoint used by workers. For MCP actor-backed tools this is the Substrate router endpoint. |
| `status.actor` | object | empty | Safe actor metadata, including provider, actor ID, route host, resolved template, and pool reference. |

MCP actor-backed tools require Substrate support to be enabled on the controller. If transport auth is needed for an MCP endpoint, set `spec.http.authSecretRef`, keep `authInject` as `header` or omit it, and omit `spec.http.url`.

## OutboundAccessPolicy

`OutboundAccessPolicy` is namespaced and selects exactly one adapter. Direct mode performs RFC 8693/RFC 7523 exchange and injects a validated Bearer resource credential. Gateway mode dials a trusted Kubernetes Service while preserving the original Tool authority, path, query, method, body, and protocol headers.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: OutboundAccessPolicy
metadata:
  name: resource-api
  namespace: default
spec:
  direct:
    grant: TokenExchange
    tokenEndpoint:
      url: https://identity.example.test/oauth/token
    subject:
      source: TransactionToken
    scopes: [api.read]
    requestedTokenType: urn:ietf:params:oauth:token-type:access_token
    expectedIssuedTokenType: urn:ietf:params:oauth:token-type:access_token
```

Policy status contains only `observedGeneration`, `Accepted`, and `ResolvedRefs`. Secret references are key-specific and same-namespace. Cross-namespace Service refs require exact controller allowlist entries. See [Outbound Access Policies](../concepts/outbound-access.md).

## Security

Repository security endpoints manage `RepositoryScan` configurations and their generated threat models, scan runs, findings, patch proposals, and remediation pull requests. Like other `/api/v1/*` endpoints, they require ServiceAccount bearer token authentication.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/security/repositories` | POST | Create a repository scan |
| `/api/v1/security/repositories` | GET | List repository scans |
| `/api/v1/security/repositories/:name` | GET | Get repository scan details |
| `/api/v1/security/repositories/:name` | PUT | Update repository scan spec |
| `/api/v1/security/repositories/:name` | DELETE | Delete repository scan |
| `/api/v1/security/repositories/:name/threat-model` | GET | Get latest threat model |
| `/api/v1/security/repositories/:name/threat-model` | PUT | Update threat model |
| `/api/v1/security/repositories/:name/scans` | GET | List scan runs |
| `/api/v1/security/repositories/:name/scans` | POST | Trigger manual scan |
| `/api/v1/security/repositories/:name/slices` | GET | List deterministic review slices |
| `/api/v1/security/repositories/:name/slices/:sliceID` | GET | Get review slice details |
| `/api/v1/security/repositories/:name/dropped-findings` | GET | List v2 dropped-finding diagnostics |
| `/api/v1/security/repositories/:name/findings` | GET | List findings |
| `/api/v1/security/findings/:id` | GET | Get finding details |
| `/api/v1/security/findings/:id/dismiss` | POST | Dismiss finding |
| `/api/v1/security/findings/:id/reopen` | POST | Reopen finding |
| `/api/v1/security/findings/:id/validate` | POST | Trigger validation |
| `/api/v1/security/findings/:id/patch` | POST | Generate patch proposal |
| `/api/v1/security/findings/:id/patches` | GET | List patch proposals |
| `/api/v1/security/findings/:id/pull-request` | POST | Create remediation PR |

Common query parameters:

- `namespace` — Kubernetes namespace to operate in.
- `limit` — page size for list endpoints that support pagination.
- `continue` — Kubernetes continue token for `GET /api/v1/security/repositories`.
- `cursor` — store cursor for `GET /api/v1/security/repositories/:name/scans`, `GET /api/v1/security/repositories/:name/slices`, `GET /api/v1/security/repositories/:name/dropped-findings`, and `GET /api/v1/security/repositories/:name/findings`.
- `severity`, `validationStatus`, `state`, `sliceID`, `category` — filters for `GET /api/v1/security/repositories/:name/findings`.
- `status` — filter for `GET /api/v1/security/repositories/:name/slices`.
- `scanRunID`, `sliceID`, `layer` — filters for `GET /api/v1/security/repositories/:name/dropped-findings`. `layer` is one of `validation`, `filter`, or `cap`.
- `reason` — exact dropped-finding reason filter; use `reason=contains=<text>` for substring matching.
- `recommended=true` — filters findings to recommended remediation candidates.

### Create Repository Scan

**Endpoint:** `POST /api/v1/security/repositories`

**Request Body:**
```json
{
  "name": "example-repo",
  "namespace": "default",
  "spec": {
    "provider": "github",
    "repoURL": "https://github.com/example/app",
    "branch": "main",
    "ref": "v1.2.3",
    "schedule": "0 2 * * *",
    "validationMode": "light",
    "validationMaxFindingsPerRun": 8,
    "validationMinSeverity": "medium",
    "validationMinConfidence": "medium",
    "customScanInstructionsRef": {"name": "repo-security-policy", "key": "policy"},
    "falsePositivePolicyRef": {"name": "repo-security-policy", "key": "false-positives"},
    "analysisAgentRef": {"name": "security-reviewer"}
  }
}
```

**Response (201):** The created `RepositoryScan` resource.

Required fields are `name`, `spec.repoURL`, and `spec.analysisAgentRef.name`. The API defaults or infers provider, owner, repository, branch, and validation mode where possible. Set `spec.ref` to pin scan tasks to a specific tag, branch, or commit SHA; when `ref` is set without `branch`, scan workspaces check out that ref directly instead of forcing the default `main` branch.

The request accepts the same `RepositoryScan` spec fields as the CRD, including automatic validation tuning (`validationMaxFindingsPerRun`, `validationMinSeverity`, `validationMinConfidence`) and ConfigMap-backed scanner policy refs (`customScanInstructionsRef`, `falsePositivePolicyRef`). Policy ConfigMaps must be in the same namespace and opt in with `orka.ai/security-policy: "true"` as a label or annotation.

### Security Findings Workflow

A typical remediation workflow is:

1. List findings with `GET /api/v1/security/repositories/:name/findings?namespace=default&recommended=true`.
2. Inspect evidence with `GET /api/v1/security/findings/:id`.
3. Optionally validate with `POST /api/v1/security/findings/:id/validate`.
4. Generate a patch with `POST /api/v1/security/findings/:id/patch`.
5. Review patch proposals with `GET /api/v1/security/findings/:id/patches`. A proposal is successful only after the governed publication is verified and the agent's patch result envelope matches the diff derived from the published commit; the stored diff and summary artifacts come from that verification, never from agent-written files.
6. Create a remediation pull request with `POST /api/v1/security/findings/:id/pull-request`.

Review slice and dropped-output inspection:

1. List slices with `GET /api/v1/security/repositories/:name/slices?namespace=default`.
2. Inspect one slice with `GET /api/v1/security/repositories/:name/slices/:sliceID?namespace=default`.
3. List rejected v2 model output with `GET /api/v1/security/repositories/:name/dropped-findings?namespace=default&scanRunID=scan_...&layer=filter&reason=contains=rate-limit`.

## Repository Monitors

Repository monitor endpoints manage `RepositoryMonitor` configurations and their durable monitor runs, issue/PR inventory, command events, workflow actions, typed action records, implementation jobs, GitHub mutation audit records, review/repair state, readiness state, and audit events.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/monitors/repositories` | POST | Create a repository monitor |
| `/api/v1/monitors/repositories` | GET | List repository monitors |
| `/api/v1/monitors/repositories/:name` | GET | Get repository monitor details |
| `/api/v1/monitors/repositories/:name` | PUT | Update repository monitor spec |
| `/api/v1/monitors/repositories/:name` | DELETE | Delete repository monitor |
| `/api/v1/monitors/repositories/:name/runs` | POST | Trigger a manual monitor run |
| `/api/v1/monitors/repositories/:name/runs` | GET | List monitor runs |
| `/api/v1/monitors/repositories/:name/items` | GET | List current monitor items |
| `/api/v1/monitors/repositories/:name/commands` | POST | Create an explicit issue/PR workflow command |
| `/api/v1/monitors/commands` | GET | List durable command events |
| `/api/v1/monitors/commands/:id` | GET | Get a command event |
| `/api/v1/monitors/work-actions` | GET | List durable workflow actions and leases |
| `/api/v1/monitors/work-actions/:id` | GET | Get a workflow action |
| `/api/v1/monitors/actions` | GET | List typed action records |
| `/api/v1/monitors/actions/:id` | GET | Get a typed action record |
| `/api/v1/monitors/implementation-jobs` | GET | List issue implementation jobs |
| `/api/v1/monitors/implementation-jobs/:id` | GET | Get an issue implementation job |
| `/api/v1/monitors/mutations` | GET | List controller-owned GitHub mutation audit records |
| `/api/v1/monitors/mutations/:id` | GET | Get a GitHub mutation audit record |
| `/api/v1/monitors/events` | GET | List monitor audit events |

Common query parameters:

- `namespace` - Kubernetes namespace to operate in.
- `limit` - page size for list endpoints.
- `continue` or `cursor` - pagination cursor for store-backed list endpoints.
- `kind`, `number`, `state`, `verdict`, `repairState`, and `automergeState` - filters for `GET /api/v1/monitors/repositories/:name/items`.
- `name`, `runID`, `itemKind`, `itemNumber`, and `eventType` - filters for `GET /api/v1/monitors/events`; `name` is required.

Context-token authorization scopes are `orka:monitors:read` for list/get endpoints, `orka:monitors:write` for create/update/delete, and `orka:monitors:operate` for manual run creation.

### Create Repository Monitor

**Endpoint:** `POST /api/v1/monitors/repositories`

**Request Body:**
```json
{
  "name": "example-app",
  "namespace": "default",
  "spec": {
    "provider": "github",
    "repoURL": "https://github.com/example/app",
    "branch": "main",
    "gitSecretRef": {"name": "repo-monitor-github"},
    "schedule": "*/30 * * * *",
    "targets": {
      "pullRequests": {
        "enabled": true,
        "includeDrafts": false,
        "maxPerRun": 10
      }
    },
    "agents": {
      "reviewer": {"name": "repo-reviewer"}
    },
    "review": {
      "event": "COMMENT",
      "staleReviewTTL": "24h",
      "exactEventEnabled": true
    },
    "policy": {
      "protectedLabels": ["security-sensitive"],
      "pauseLabels": ["orka:pause"]
    },
    "validation": {
      "image": "ghcr.io/example/app-validation@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  }
}
```

**Response (201):** The created `RepositoryMonitor` resource.

Required fields are `name`, `spec.repoURL`, and `spec.agents.reviewer.name` when pull request monitoring is enabled. The API defaults or infers provider, owner, repository, branch, pull request enablement, pull request `maxPerRun`, and `review.event` where possible. `spec.repoURL` must be a credential-free GitHub repository root URL such as `https://github.com/owner/repo`, `https://github.com/owner/repo.git`, or `git@github.com:owner/repo.git`; pull request, issue, branch/tree, blob/file, commit, query-string, fragment, non-GitHub, HTTP, and embedded-credential URLs are rejected.

`spec.validation.image` optionally configures isolated pull request validation. The image must use an immutable `@sha256:` digest. The reviewer chooses one offline shell command after inspecting the repository. Orka runs it in the configured image against the exact read-only PR head, releases it only after a deny-all NetworkPolicy exists, and independently verifies the child Task before accepting a `passed` verdict. The image must contain `/bin/sh`, every required tool such as `golangci-lint`, Terraform, or Azure CLI, and any dependencies the command needs. Commands, args, credentials, and network access are not configured on the monitor.

GitHub pull request and issue targets are supported. Commit targets are rejected. `review.requireGreenCI` is supported for gating review selection on green CI. Pull request monitoring requires `spec.agents.reviewer.name`; the reviewer Agent must use `runtime.type: claude`, must reference a Secret in the monitor namespace, and that Secret must contain a non-empty `ANTHROPIC_API_KEY` or `ANTHROPIC_FOUNDRY_API_KEY` key. Issue-only monitors can set `targets.pullRequests.enabled: false` and `targets.issues.enabled: true`. When `gitSecretRef` is set, the Git Secret must exist in the monitor namespace and contain a non-empty `token`, `password`, or `GITHUB_TOKEN` key.

### Trigger Manual Monitor Run

**Endpoint:** `POST /api/v1/monitors/repositories/{name}/runs`

**Request Body:**
```json
{
  "targetKind": "pull_request",
  "targetNumber": 123,
  "targetSHA": "abc123"
}
```

The request body can be omitted to run a full inventory pass. `targetKind` may be empty, `pull_request`, or `issue`; `targetNumber` and `targetSHA` narrow the run to one issue, one PR, or an exact PR head. When `targetNumber` is set, the controller fetches that target directly from GitHub and does not retire unrelated monitor items. The API returns `409` when the monitor already has a queued or running run.

### Create Monitor Command

**Endpoint:** `POST /api/v1/monitors/repositories/{name}/commands`

**Request Body:**
```json
{
  "kind": "issue",
  "number": 123,
  "intent": "plan",
  "targetSHA": ""
}
```

Supported issue intents are `triage`, `research`, `plan`, `approve_plan`, `implement`, `decompose`, `stop`, and `resume`. Supported pull request intents are `review`, `fix`, `fix_ci`, `update_branch`, `automerge`, `stop`, and `resume`. Head-bound pull request commands (`review`, `fix`, `fix_ci`, `update_branch`, and `automerge`) must include `targetSHA`; `stop` and `resume` can omit it. The command creation endpoint always requires `orka:monitors:operate`. Mutating intents (including approve, implement, repair, update-branch, automerge, stop, and resume) additionally require `orka:monitors:write`; `review` also requires monitor-write when review publishing is enabled. The endpoint validates that the target kind is enabled, records a durable command event, and queues a targeted monitor run.

### List Monitor Commands, Actions, Implementations, and Mutations

**Endpoints:**

- `GET /api/v1/monitors/commands?namespace=&name=&kind=&number=&intent=&status=`
- `GET /api/v1/monitors/commands/{id}`
- `GET /api/v1/monitors/work-actions?namespace=&name=&kind=&number=&intent=&desiredAction=&status=&taskName=`
- `GET /api/v1/monitors/work-actions/{id}`
- `GET /api/v1/monitors/actions?namespace=&name=&kind=&number=&actionKind=&taskName=`
- `GET /api/v1/monitors/actions/{id}`
- `GET /api/v1/monitors/implementation-jobs?namespace=&name=&issueNumber=&phase=&taskName=`
- `GET /api/v1/monitors/implementation-jobs/{id}`
- `GET /api/v1/monitors/implementation-jobs/{id}/patch-preview`
- `GET /api/v1/monitors/mutations?namespace=&name=&kind=&number=&operation=&status=`
- `GET /api/v1/monitors/mutations/{id}`

Command events record label/API intake, actor/source authorization, target SHA/snapshot bindings, status, and errors. Work actions are the durable queue/lease view for prerequisites and follow-up work. Action records store typed triage/research/plan/implementation/review/repair/automerge outcomes. Implementation jobs track issue coding attempts, patch artifacts, validation state, branches, and linked PRs. Mutation records audit every controller-owned GitHub write such as label consumption, review submission, branch pushes, PR creation, and automerge attempts.

See [Repository Monitors](../guides/repository-monitors.md) for the full workflow and CRD example.

## Auth

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/auth/validate` | GET | Validate auth token |

## Secrets

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/secrets` | GET | List secret names (metadata only) |

## Chat

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/chat` | POST | Send message (SSE streaming or JSON) |
| `/api/v1/chat/config` | GET | Get chat configuration and available tools |
| `/api/v1/chat/:sessionId` | DELETE | Cancel a chat session |

See [Interactive Chat](../guides/chat.md) for full chat documentation.

## OpenAI-Compatible API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/openai/v1/chat/completions` | POST | Chat completions (streaming & non-streaming) |
| `/openai/v1/models` | GET | List available models |

See [OpenAI Compatibility](openai-compat.md) for details.

## Anthropic-Compatible API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/anthropic/v1/messages` | POST | Create a message (streaming & non-streaming) |
| `/anthropic/v1/models` | GET | List available models |

The `/anthropic/v1/messages` endpoint injects built-in tools and runs server-side tool execution by default. Set `X-Orka-Tools: disabled` header to use as a transparent proxy instead. See [Anthropic Compatibility](anthropic-compat.md) for details.

## Internal API (Worker Communication)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/internal/v1/results/:namespace/:taskName` | POST | Submit task result |
| `/internal/v1/artifacts/:namespace/:taskName/:filename` | POST | Upload task artifact |
| `/internal/v1/sessions/:namespace/:name/transcript` | GET | Get session transcript |
| `/internal/v1/plans/:namespace/:taskName` | POST | Save plan state |
| `/internal/v1/plans/:namespace/:taskName` | GET | Get plan state |
| `/internal/v1/messages/:namespace` | POST | Send inter-agent message |
| `/internal/v1/messages/:namespace/:taskName` | GET | Get messages for a task |

### Save Plan State

Workers call this to persist autonomous plan state.

**Endpoint:** `POST /internal/v1/plans/{namespace}/{taskName}`

**Request Body:**
```json
{
  "summary": "Completed phase 1",
  "progress_pct": 25,
  "goal_complete": false,
  "plan_document": "# Plan\n..."
}
```

**Response:** `204 No Content`

### Get Plan State

Workers call this to load the current plan state at startup.

**Endpoint:** `GET /internal/v1/plans/{namespace}/{taskName}`

**Response (200):** Same as public plan endpoint.

**Errors:**
- `404` — No plan found

### Send Message

Workers call this to send messages to sibling tasks (same parent coordinator).

**Endpoint:** `POST /internal/v1/messages/{namespace}`

**Request Body:**
```json
{
  "fromTask": "worker-a",
  "toTask": "worker-b",
  "parentTask": "coordinator",
  "content": "Found a bug in the auth module"
}
```

Use `"toTask": "*"` to broadcast to all siblings.

**Response:** `204 No Content`

### Get Messages

Workers call this to check for unread messages.

**Endpoint:** `GET /internal/v1/messages/{namespace}/{taskName}?parentTask={parentTask}&markRead={true|false}`

**Query Parameters:**
- `parentTask` (required) — Parent coordinator task name (scopes messages to siblings)
- `markRead` (optional, default: `true`) — Whether to mark returned messages as read

**Response (200):**
```json
[
  {
    "id": 1,
    "namespace": "default",
    "fromTask": "worker-b",
    "toTask": "worker-a",
    "parentTask": "coordinator",
    "content": "Found a bug in the auth module",
    "read": false,
    "createdAt": "2026-01-15T10:30:00Z"
  }
]
```

## Health

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/healthz` | GET | Health check |
| `/readyz` | GET | Readiness check |

## Example Usage

```bash
# Create a task
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Authorization: Bearer $(kubectl create token orka-client)" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-task",
    "type": "ai",
    "agentRef": {"name": "assistant"},
    "prompt": "Explain microservices architecture"
  }'

# Get task result
curl http://localhost:8080/api/v1/tasks/my-task/result \
  -H "Authorization: Bearer $(kubectl create token orka-client)"

# List task artifacts
curl http://localhost:8080/api/v1/tasks/my-task/artifacts \
  -H "Authorization: Bearer $(kubectl create token orka-client)"

# Download an artifact
curl -L http://localhost:8080/api/v1/tasks/my-task/artifacts/output.json \
  -H "Authorization: Bearer $(kubectl create token orka-client)" \
  -o output.json

# Chat with SSE streaming
curl -N http://localhost:8080/api/v1/chat \
  -H "Authorization: Bearer $(kubectl create token orka-client)" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Create an AI task that summarizes Kubernetes best practices",
    "sessionId": "my-session"
  }'
```

## Built-in Tools

These tools are available to AI worker agents:

| Tool | Description | Parameters |
|------|-------------|------------|
| `web_search` | Search the web via configurable API (Tavily, etc.) | `query` (required), `limit` (default 5) |
| `code_exec` | Execute code in a sandboxed environment | `language` (python/javascript/bash), `code`, `timeout` (max 60s) |
| `file_read` | Read files from the workspace | `path`, `offset`, `limit` (max 1MB) |
| `web_fetch` | Fetch and extract URL content | `url` (required), `max_chars` (default 50000), `raw` |
| `file_write` | Write or append files in workspace paths | `path` (required), `content` (required), `mode` (`write`/`append`), `create_dirs` |

### Coordination Tools

These tools are injected into AI worker agents when the Agent has `coordination.enabled: true`. They are not returned by `GET /api/v1/tools`.

The following tools are **auto-injected** when coordination is enabled:

| Tool | Description | Parameters |
|------|-------------|------------|
| `delegate_task` | Delegate a subtask to another agent | `agent`, `prompt` (required); `namespace`, `priority`, `auto_retry`, `max_retries` |
| `wait_for_tasks` | Wait for delegated tasks to complete | `tasks` (required), `timeout` (default 10m) |
| `create_container_task` | Create a child container task | `name`, `image`, `command`/`args`, env/workspace fields |
| `cancel_task` | Cancel a running child task | `task_name` (required); `namespace`, `reason` |
| `send_message` | Send a message to a sibling task | `to_task` (required, or `*` to broadcast), `content` (required) |
| `check_messages` | Check for messages from sibling tasks | `mark_read` (boolean, default true) |
| `recall_memory` | Recall durable namespace-scoped memories | `query`, `tags`, `task_name`, `agent_name`, `source`, `limit`, `include_disabled` |
| `remember` | Submit a durable memory proposal for review | `content` (required); `title`, `description`, `tags`, `agent_name` |
| `propose_memory` | Submit a memory-adjacent governance proposal | `title` (required); `type`, `skill_name`, `description`, `content`, `patch`, `agent_name` |
| `search_transcript` | Search prior session transcripts | `query` (required); `session_name`, `exclude_session_name`, `roles`, `limit`, `max_snippet_length` |
| `create_pull_request` | Create a GitHub pull request | `task_name`, `head_branch`, `base_branch`, `title` (required); `body` |
| `check_pull_request_ci` | Check GitHub CI status without merging | `pr_number` (required); `task_name`, `repo_url`, `wait_timeout`, `poll_interval` |
| `list_pull_requests` | List open pull requests in a repository | `task_name`, `repo_url`; `per_page`, `page` |
| `check_pr_review_marker` | Check for an existing Orka PR review marker on a PR head | `pr_number` (required); `task_name`, `repo_url`, `head_sha` |
| `merge_pull_request` | Merge a GitHub pull request | `task_name`, `pr_number` (required); `merge_method`, `commit_title`, `commit_message` |
| `auto_merge_pull_request` | Poll CI checks and merge a PR when all pass | `task_name`, `pr_number` (required); `merge_method`, `commit_title`, `commit_message`, `timeout` |
| `review_pull_request` | Fetch PR diff for review | `pr_number` (required); `task_name`, `repo_url` |
| `post_review_comment` | Post a review on a PR | `pr_number`, `body`, `event` (required); `task_name`, `repo_url`, `comments` |
| `create_agent` | Create an Agent CRD at runtime | `name`, `provider`, `model` (required); `systemPrompt` (required except OpenCode; omit for OpenCode), `tools`, `coordination` |
| `delete_agent` | Delete an Agent CRD | `name` (required), `namespace` |
| `update_plan` | Update the autonomous execution plan | `summary`, `plan_document` (required); `progress_pct`, `goal_complete` |

The following tools require explicit `spec.tools[]` entries on the Agent CRD:

| Tool | Description | Parameters |
|------|-------------|------------|
| `list_issues` | List open GitHub issues in a repository | `task_name`, `repo_url`; `unassigned_only` (default true), `per_page`, `page` |
| `get_issue` | Fetch full details of a GitHub issue | `issue_number` (required); `task_name`, `repo_url` |
| `comment_on_issue` | Post a comment on a GitHub issue | `issue_number`, `body` (required); `task_name`, `repo_url` |

For GitHub tools that accept `repo_url`, explicit repository URLs are scope-checked when task context is available. The requested repository must match the current task's workspace repository or signed transaction repository context; otherwise the tool fails closed before resolving credentials or calling GitHub.

`create_pr_monitor` is exposed through the chat/management tool set rather than auto-injected into every coordinator worker. Parameters are `name`, `repo_url`, `schedule`, and `agent_ref` (required), plus optional `namespace`, `provider_ref`, `gitSecretRef`, `per_page`, `review_event`, and `prompt`. `repo_url` must be a credential-free GitHub repository root URL such as `https://github.com/owner/repo`, `https://github.com/owner/repo.git`, or `git@github.com:owner/repo.git`; pull request, issue, branch/tree, blob/file, commit, query-string, fragment, non-GitHub, HTTP, and embedded-credential URLs are rejected.

`create_pr_monitor` is the compatibility path for prompt-orchestrated scheduled PR monitors. It creates a scheduled `type: ai` Task, sets `spec.workspace.gitRepo` to `repo_url`, injects only the PR review loop tools, and instructs the task to pass the same `repo_url` to `list_pull_requests`, `check_pr_review_marker`, `check_pull_request_ci`, `review_pull_request`, and `post_review_comment`. The Agent referenced by `agent_ref` must be an AI Agent with coordination enabled and autonomous coordination disabled. The `create_pr_monitor` tool accepts its compatibility `gitSecretRef` parameter (or a supported default Secret name) and maps the selected Secret into the created Task's top-level `spec.workspace.readCredentialRef`. When the parameter is omitted, the supported default Secret names are `git-credentials`, `github-credentials`, `copilot-token`, `github-token`, and `git-token`. The selected Secret must contain a non-empty `token`, `password`, or `GITHUB_TOKEN` key.

`check_pr_review_marker` returns a hidden marker that the monitor should include unchanged in the subsequent review body. The marker includes `repo`, `pr`, `head_sha`, and `sig` fields, and matching is scoped to that repository, pull request, and exact head. Marker signatures are stable across GitHub token rotation. Operators can set `ORKA_PR_REVIEW_MARKER_SECRET` in the worker Task environment for dedicated marker signing and `ORKA_PR_REVIEW_MARKER_PREVIOUS_SECRETS` for comma-separated previous keys during rotation. Legacy markers are accepted only from a trusted review author, configured with `ORKA_PR_REVIEW_MARKER_TRUSTED_AUTHOR` or resolved from the Task's authenticated GitHub user.
