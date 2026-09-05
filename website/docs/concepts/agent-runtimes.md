---
slug: /agent-runtimes
---

# Agent Runtimes

`type: agent` Tasks use the ACP core runtime and the `orka.harness.v2` session protocol. Built-in Codex, Claude, Copilot, and OpenCode profiles run in controller-owned `RuntimePool` resources; there is no per-Task agent Job and no fallback runtime path.

Orka owns the durable Task attempt, RuntimeSession identity, queueing, workspace validation, delivery receipt, transcript, and result projection. The runtime Pod owns only the short-lived provider process and ACP session for a fenced RuntimeSession.

The supported built-in set is intentionally closed: `codex`, `claude`,
`copilot`, and `opencode`. Operators can also register and conformance-test an
external `orka.harness.v2` `AgentRuntime`. A ready strict-governed registration
selected through `runtimeRef` uses the same durable Task and RuntimeSession
state machines, while the operator remains responsible for the external
service lifecycle and capacity.

## Supported runtime profiles

| Runtime | `Agent.spec.runtime.type` | Packaging status |
| --- | --- | --- |
| Codex | `codex` | Immutable Codex ACP image definition is included. Configure a digest-pinned image. |
| Claude | `claude` | Immutable Claude ACP image definition is included. Configure a digest-pinned image. |
| Copilot | `copilot` | Immutable Copilot ACP image definition is included. Configure a digest-pinned image. |
| OpenCode | `opencode` | Immutable OpenCode ACP image definition is included. Configure a digest-pinned image. |

External runtimes use an `AgentRuntime` registration with `contractVersion: orka.harness.v2`. Current-generation conformance proves the exact instance, profile, cancellation, duplicate, and workspace-governance claims. Dispatch revalidates the frozen registration, authentication authority, and observed runtime identity before every mutation. See [Bring your own AgentRuntime](../guides/bring-your-own-agent-runtime.md) and the [adapter contract](../development/agent-runtime-adapter-contract.md).

## Architecture

```text
Task (type: agent)
  -> durable ACP attempt and queue reservation
  -> built-in path: central authenticated provider proxy -> Vekil/model backend
     -> controller-owned RuntimePool for one trust domain/profile
        -> one exact runtime Pod (0 or 1 replica)
           -> private RuntimeSession HOME/workspace/UID + provider process
           -> per-session loopback MCP proxy -> controller prompt broker
  -> external path: authenticated operator-owned AgentRuntime endpoint
     -> exact fenced RuntimeSession + non-reconnectable prompt stream
     -> registered provider, MCP, and workspace-governance profile
  -> workspace delta validation
  -> artifact + credential brokers -> separate Workspace/Publisher identity
  -> clean-room clone, prepare, push, verify, and optional PR reconciliation
  -> durable execution and delivery status projected onto the Task
```

A shared runtime Pod is a same-trust-domain density optimization, not a tenant isolation boundary. Different namespaces/profile digests are assigned different logical pools.

## RuntimePools

RuntimePools are controller-owned implementation resources. Do not hand-author them for normal built-in Tasks. The controller derives a pool from the Task namespace, runtime type, model, immutable adapter/image digests, tool and approval policy, workspace intent, proxy credential scope, and resource class.

The first-release defaults are:

- one active Pod per pool;
- up to 10 resident RuntimeSessions;
- up to 4 concurrently running prompts;
- scale from zero on durable demand;
- admit new sessions only while lifecycle is `Serving` and admission is `Accepting`;
- drain to `Quiescent` before scale-down or replacement.

Pool lifecycle is:

```text
Stopped -> Starting -> Serving
Serving -> Draining -> Quiescent -> Stopping -> Stopped
```

`Degraded` and `Ambiguous` close admission. Every mutating request is fenced by the controller epoch, pool UID/generation, exact runtime instance and supervisor boot ID, RuntimeSession UID/generation, operation ID, request digest, and expiry.

Capacity reservations live in `RuntimePool.status` and use Kubernetes
`resourceVersion` compare-and-swap. The dispatcher claims a resident/prompt slot
before changing an attempt to `Reserved`, renews or converts it as work moves
through the session lifecycle, and reclaims expired reservations after restart.

Inspect pools with:

```bash
orka runtime-pool list
orka runtime-pool get <pool-name> -o yaml
kubectl get runtimepools
```

## ACP v2 Task lifecycle

The durable Task execution state is independent from the top-level compatibility phase:

```text
Queued -> Reserved -> SessionStarting -> Planned -> Submitting
       -> Accepted -> Running -> Settling
       -> Succeeded | Failed | Cancelled | OutcomeUnknown
```

`SubmittedUnknown` records a loss in the submit acknowledgement window. `OutcomeUnknown` is terminal for that attempt and must not be treated as a generic retryable failure.

Within the pool, a RuntimeSession moves through:

```text
creating -> idle -> prompt_running -> validating
         -> finalizing -> idle
```

A write Task with changes additionally uses:

```text
validating -> preparing_publication -> publication_prepared
           -> publishing -> verifying -> finalizing
```

Cancellation moves an active session through `cancelling`. A session that cannot prove process cleanup, workspace integrity, or publication safety becomes `poisoned` and is deleted; the pool may be recycled.

Only an `idle` RuntimeSession can accept a prompt or be evicted. Validation, publication, finalization, and Session lease release complete before terminal Task status is exposed.

## Durable control authority

ACP control decisions are Kubernetes-authoritative:

- `ControllerEpoch`, `PromptAttempt`, `RuntimeSessionControl`, `BranchClaim`,
  `Publication`, and `ExternalEffect` status transitions use
  `resourceVersion` compare-and-swap;
- coordination Leases serialize controller-epoch mutation and one logical
  Session mutation at a time;
- `RuntimePool.status` owns exact-instance lifecycle, demand, capacity, and
  reservation state.

SQLite is not a second ACP control authority. It persists transcript and
SessionTurn payloads, deferred outbox projections, and artifacts
behind the Kubernetes fences. Cross-store finalization binds the exact outbox
projection before releasing the Session lease.

## Create an Agent

Provider credentials are separate from Git credentials. Built-in ACP Agents do
not set `secretRef`: the controller scopes each RuntimePool to the central
authenticated provider proxy, which injects the upstream Vekil/model credential
outside the ACP child process.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: codex-reviewer
spec:
  runtime:
    type: codex
    contractVersion: orka.harness.v2
    defaultMaxTurns: 20
    defaultAllowBash: true
    defaultAllowedTools:
      - Read
      - Glob
      - Grep
  model:
    name: gpt-5.4
```

OpenCode model IDs use provider/model form, for example `openai/gpt-5.4`.
OpenCode selects `apply_patch` for GPT-family models and `edit`/`write` for
others; Orka normalizes those aliases into one governed mutation capability, so
allowing one exposes the group and disallowing one closes the group. For
read-intent workspaces, Orka also disables Bash and OpenCode Grep because Grep
cannot carry the secret-file exclusions applied to OpenCode Read; Read and Glob
remain available when allowed by policy.

For built-in RuntimePools, Task-level `spec.agentRuntime` contains runtime overrides such as `maxTurns`, `allowedTools`, `disallowedTools`, and `allowBash`. For `runtimeRef`, the registered external profile owns provider, model, prompt, skill, tool, and runtime defaults. Its `capabilities.mcpPolicy` stores the exact tool and approval policy represented by the profile digests, and Orka uses that same policy for conformance and dispatch. The Agent must omit `spec.model`, `spec.systemPrompt`, `spec.skills`, enabled `spec.tools`, `defaultMaxTurns`, `defaultAllowedTools`, `defaultAllowBash`, and `defaultReasoningEffort`. Disabled Agent tool entries are inert and accepted. Task-level `allowedTools` must equal the registered allowlist when brokered tools are exposed. Orka rejects `maxTurns`, `disallowedTools`, and `allowBash` because those values are fixed by the registration. Repository configuration belongs at top-level `spec.workspace`.

## Read-only workspace Task

Agent Tasks default to `intent: read` when `workspace.intent` is omitted, but setting it explicitly makes review intent clear.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: review-repository
spec:
  type: agent
  agentRef:
    name: codex-reviewer
  prompt: Review the authentication package. Do not modify files.
  workspace:
    intent: read
    gitRepo: https://github.com/example/project.git
    ref: 0123456789abcdef0123456789abcdef01234567
    readCredentialRef:
      name: project-read
    subPath: services/auth
  agentRuntime:
    maxTurns: 20
    allowBash: true
  timeout: 20m
```

The clean-room workspace boundary resolves `project-read` for one clone/read operation, verifies the source baseline, removes credentials, and stores a sanitized workspace artifact. The runtime downloads that artifact without SCM credentials or direct SCM publication access.

A read Task succeeds only after validation proves the final tree still matches the verified baseline. Unexpected changes are classified `ReadOnlyWorkspaceModified` and poison the RuntimeSession.

## Write and publication Task

Source read, target read, target write, and forge API credentials are distinct
roles, even when an operator deliberately backs multiple roles with the same
Secret.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: update-documentation
spec:
  type: agent
  agentRef:
    name: codex-reviewer
  prompt: Update the authentication documentation and run its focused checks.
  workspace:
    intent: write
    gitRepo: https://github.com/example/project.git
    branch: main
    readCredentialRef:
      name: project-read
    publicationGitRepo: https://github.com/example/project.git
    publicationReadCredentialRef:
      name: project-publication-read
    publicationCredentialRef:
      name: project-publish
    forgeCredentialRef:
      name: project-forge
    pushBranch: orka/update-auth-docs
    prBaseBranch: main
    createPR: true
  agentRuntime:
    maxTurns: 40
    allowBash: true
  timeout: 40m
```

The ACP child may edit the materialized checkout, but its Git configuration, refs, hooks, filters, remotes, and history are never trusted for publication. After prompt settlement, the supervisor freezes the session process tree and produces a bounded, content-addressed workspace delta. The separate Workspace/Publisher then:

1. creates a clean clone at the persisted baseline with the source-read credential;
2. applies the validated delta;
3. prepares a deterministic Orka-owned commit;
4. records the exact tree, commit, and artifact digests;
5. compare-and-swap publishes the exact commit with the target-write credential;
6. independently verifies the remote ref with the target-read credential;
7. reconciles a pull request with the forge-only credential when `createPR: true`.

At reservation, Orka freezes the selected Secret UID/resourceVersion and key for
each role. The credential broker releases a value only to the
Workspace/Publisher for the exact active operation. No Git or forge credential
enters the runtime Pod. Branch push is the minimum durable delivery;
pull-request reconciliation is opt-in.

Workspace deltas and prepared publication artifacts cross the controller API
through short-lived artifact capabilities bound to the operation, method,
path, digest, size, media type, identity, and expiry. The Publisher does not
hold the artifact signing key.

## Workspace fields

| Field | Purpose |
| --- | --- |
| `intent` | `read` or `write`; defaults to `read` for agent Tasks and is immutable for an active attempt. |
| `gitRepo` | Credential-free source repository URL. |
| `sourceRepository` | Optional canonical provider repository identity. |
| `branch` / `ref` | Source branch or exact ref/commit/tag. |
| `readCredentialRef.name` | Secret used only by the clean-room clone/read boundary. |
| `publicationGitRepo` | Credential-free repository URL that receives the publication branch. |
| `publicationRepository` | Optional canonical publication repository identity. |
| `publicationReadCredentialRef.name` | Target-read Secret used only for publication preflight and independent verification. |
| `publicationCredentialRef.name` | Target-write Secret used only for the exact compare-and-swap push. |
| `forgeCredentialRef.name` | Forge API Secret used only for pull-request reconciliation. Required when `createPR` is true. |
| `subPath` | Repository subdirectory exposed as the workspace root. |
| `pushBranch` | Publication branch. If omitted for a write Task, Orka derives a full-entropy Task- or Session-owned branch. |
| `prBaseBranch` | Pull-request base branch when `createPR` is true. |
| `createPR` | Explicitly request PR reconciliation after verified branch publication. |

Do not embed credentials, query strings, or fragments in repository URLs.

## Delivery status

`status.delivery` is the authoritative non-secret receipt. Intermediate states include `Validating`, `Preparing`, `Prepared`, `Publishing`, and `Verifying`. Terminal outcomes include:

- `ReadValidated` — read-only tree matches the verified baseline;
- `NoChange` — a write Task produced no validated change;
- `VerifiedExact` — the remote branch equals the prepared commit;
- `DeliveredSuperseded` — the prepared commit is present and a verified descendant now leads the branch;
- `ReadOnlyWorkspaceModified` — a read Task changed the workspace;
- `CredentialBlocked` — a required credential could not be resolved or used;
- `DeliveryConflict` — branch ownership or exact-ref reconciliation failed;
- `PublicationOutcomeUnknown` — the remote could not be observed after bounded reconciliation;
- `CancelledBeforePublish` — cancellation won before publication.

Use `orka task status <name>` or inspect the Task YAML to see execution, RuntimePool, and delivery data.

## Session continuity

Use `spec.sessionRef` for canonical transcript continuity and serialized mutation of one logical session:

```yaml
spec:
  sessionRef:
    name: repository-review-session
  workspace:
    intent: read
    gitRepo: https://github.com/example/project.git
```

A RuntimeSession is ephemeral. If its Pod is replaced, Orka may create a fresh provider session from the verified workspace baseline and canonical transcript. ACP v2 intentionally does not provide prompt replay, stream reconnect, provider-session load, or workspace checkpoint endpoints.

## Runtime and credential boundaries

Built-in runtime Pods:

- use a digest-pinned image and immutable runtime-profile digest;
- have no Git read or publication credential;
- have no direct SCM egress;
- use a read-only root filesystem and no service-account token;
- isolate resident sessions with private directories, distinct non-reused UIDs/GIDs, and process-tree cleanup;
- expose only non-sensitive health/capability probes without authentication;
- require controller authentication and operation-scoped authorization for status and mutations.

Provider traffic goes only to the central authenticated provider proxy. The
proxy accepts the controller-issued RuntimePool bearer, enforces the configured
provider/model path, and forwards to Vekil. The `config/acp-production` overlay
also applies the Vekil ingress policy so runtime Pods cannot bypass that proxy.

Each RuntimeSession receives a credential-protected loopback MCP endpoint. It
lists the canonical prompt tool policy while idle, but executes calls only for
the active Task/attempt/prompt/lease. The controller broker revalidates the
exact runtime fences, tool allowlist, approval evidence, and lease expiry on
every call. Consequential calls reserve an `ExternalEffect` identity before
execution and replay only a committed matching response.

The Workspace/Publisher has brokered SCM/forge credentials and artifact access,
but no provider or prompt-scoped MCP access.

## Controller configuration

ACP is active when the controller runs in `harness-v2` mode; there is no
separate ACP enablement flag. At minimum, set the controller mode and configure
immutable images:

```text
--controller-mode=harness-v2
--acp-runtime-namespace=orka-runtimes
--acp-provider-proxy-namespace=vekil-system
--acp-provider-proxy-base-url=http://provider-auth-proxy.orka-system.svc:8080
--acp-codex-runtime-image=<repository>@sha256:<digest>
--acp-claude-runtime-image=<repository>@sha256:<digest>
--acp-copilot-runtime-image=<repository>@sha256:<digest>
--acp-opencode-runtime-image=<repository>@sha256:<digest>
```

Mutable image tags are rejected for built-in pools. For Kustomize deployment,
apply `config/acp-production`; `config/default` alone omits the required
cross-namespace Vekil ingress boundary.

## Troubleshooting

- **Task fails before queueing**: for a built-in runtime, verify the Agent has `runtime.type: codex`, `claude`, `copilot`, or `opencode`, ACP is enabled, and the matching digest-pinned image is configured. For `runtimeRef`, verify the registration is current-generation, ready, strict-governed, and matches the Task workspace intent.
- **Task remains queued**: inspect the selected RuntimePool lifecycle/admission/capacity and the runtime Pod scheduling conditions.
- **Task reports `OutcomeUnknown`**: do not retry the same attempt automatically. Inspect the recorded attempt, controller epoch, runtime instance, and event timeline.
- **Read Task fails validation**: inspect `status.delivery` for `ReadOnlyWorkspaceModified` and treat the session as poisoned.
- **Write Task is blocked**: verify all required source-read, target-read, target-write, and forge credential references, the publication repository/branch, and the Workspace/Publisher broker configuration. Require a terminal verified delivery receipt before claiming success.
- **`runtimeRef` Task is rejected**: inspect `AgentRuntime.status` for current-generation conformance, exact profile and instance identity, strict workspace governance, and valid authentication Secret bindings. Remove unsupported Task overrides such as `maxTurns`, `disallowedTools`, or `allowBash`.
- **`spec.execution.workspace` Task is rejected**: workspace-provider-backed RuntimeSession dispatch is fail-closed unless `--acp-workspace-dispatch-enabled` is set together with the matching provider flag: `--agent-sandbox-enabled` for `provider: agent-sandbox` (which must omit `templateRef`), or `--substrate-enabled` for `provider: substrate` (which requires an infrastructure `templateRef`). Other options (`retain`, boot/pool/snapshot/hibernation, `onDetach`) stay fail-closed. The rejection reason is projected to `Task.status.executionWorkspace`. Repository access always uses top-level `spec.workspace`; see [Agent Sandbox](agent-sandbox.md) and [Substrate](substrate.md).
