# 24. Workspace-provider-backed ACP RuntimePools

Date: 2026-08-19

## Status

Accepted. Implements the in-tree adapter half of issue #343 (Phase 1:
Kubernetes Agent Sandbox). Supersedes the unconditional
`Task.spec.execution.workspace` rejection that ADR 0006's cutover note left in
`planAgentExecution`.

## Context

`Task.spec.workspace` is the agent repository/read/publication surface;
`Task.spec.execution.workspace` describes a physical execution-workspace
provider. Since the ACP v2 cutover, every agent Task carrying an enabled
execution workspace was rejected before routing, because no lifecycle adapter
mapped an ACP RuntimeSession onto a provider-owned workspace (a
`SandboxClaim` or a Substrate Actor).

The v2 platform already isolates every trust decision behind exact-instance
fencing: `RuntimePool.status.activeInstance` is the only routing surface, the
fence carries controller epoch, pool UID/generation, runtime instance/boot,
RuntimeSession UID/generation, attempt, prompt, operation, request digest, and
expiry, and the supervisor re-verifies every mutation. Nothing in that contract
is Deployment-specific. The only Deployment-specific parts of a RuntimePool are
workload materialization and instance replacement.

## Decision

**A workspace-provider-backed RuntimePool.** A Task with an enabled
`spec.execution.workspace` routes through the normal ACP v2 path, but binds to
a dedicated single-session RuntimePool whose workload is materialized through
the externally operated provider control plane instead of a controller-owned
Deployment. Everything above the workload — session creation, fenced prompts,
cancellation, permissions, workspace deltas, clean-room publication, recovery —
uses the existing `orka.harness.v2` protocol against the pool's
`ActiveInstance`, byte-for-byte unchanged.

### Relationship between the four surfaces

```text
Task.spec.workspace             repository and publication surface (unchanged;
                                clean-room publisher, frozen credentials)
Task.spec.execution.workspace   physical execution environment request; resolved
                                into a canonical, provider-neutral workspace
                                binding frozen into the execution snapshot
ACP RuntimeSession              durable logical model session; unchanged
                                identity (session UID + monotonic generation)
SandboxClaim (provider-owned)   the physical workspace hosting exactly one
                                supervisor instance for one workspace-backed pool
```

### Provider-neutral lifecycle mapping

| Contract step        | Owner and mechanism |
| -------------------- | ------------------- |
| Acquire              | Pool reconciler ensures a controller-rendered, credential-free `SandboxTemplate` (exact supervisor image, fence env, bootstrap nonce/public key, hardened security context), a zero-replica `SandboxWarmPool`, and one `SandboxClaim` for the exact pool generation. Auth/provider Secrets use unpredictable names that never enter provider objects. |
| WaitReady            | claim → sandbox → Pod Ready; Orka attests the provider's immutable `Sandbox` blueprint against the validated template, performs a controller-signed one-time credential bootstrap, then runs the authenticated exact-instance probe before publishing `ActiveInstance`. |
| CreateRuntimeSession | Existing fenced `PUT /v2/runtime-sessions/{id}` against the `ActiveInstance`. |
| ExecutePrompt        | Existing fenced, digest-sealed prompt stream; duplicate/replay classification unchanged. |
| Cancel               | Existing fenced cancel with proven settlement; unproven outcomes settle to `OutcomeUnknown`. |
| Suspend / Resume     | Not supported by Agent Sandbox; requests requiring them (Substrate options, `onDetach`) fail closed before any demand. |
| Drain                | Existing authenticated supervisor drain with persisted quiescence barriers (scale-down, rollout, identity-capacity rotation). |
| Delete               | Claim deletion (cascades sandbox and Pod) after proven drain; pool finalizer deletes claim, warm pool, template, Secrets, and policies idempotently. |
| Recover              | Existing exact-fence recovery: a missing pool or replaced instance is already proof of task-scoped RuntimeSession cleanup. |

### Pool identity and the frozen binding

The workspace request distills to a canonical binding
`{provider, reusePolicy, cleanupPolicy, workspaceSlot, sessionKey}` where
`sessionKey` is `task:<taskUID>` (reuse `none`) or
`session:<immutable SessionControl UID>` (reuse `session`). The binding digest
folds into the RuntimePool identity digest, producing
`acp-ws-<runtime>-<hash16>` pools that can never collide with
shared plain pools. The binding is frozen into the write-once execution
snapshot; snapshot verification recomputes the digest, the session key against
the immutable Task identity, and the pool name exactly, and the dispatcher
additionally requires the reserved pool's immutable
`spec.executionWorkspace` binding to match the frozen plan.

Workspace-backed pools are single-session: capacity is pinned to
`1 resident / 1 running prompt`. Continuation Tasks under `reusePolicy:
session` deterministically bind to the same pool (and therefore the same
supervisor and claim while it is alive), which preserves logical RuntimeSession
continuity without duplicating bootstrap history; a pool that has been drained
and recreated continues the session through the existing generation-increment
recreation path. Because the current provider rollout replaces the physical
workspace, a continuation that changes the runtime image or profile fails
closed before new pool demand; callers must keep the original runtime
configuration or create a new Session.

### Ownership state machine (pool workload)

```text
Stopped -> Starting  (demand: template+warm pool ensured, claim created)
Starting -> Serving  (one Ready Pod, authenticated fence probe passes)
Serving -> Draining  (scale-to-zero, rollout, identity-capacity rotation)
Draining -> Quiescent (observed + persisted supervisor/controller quiescence)
Quiescent -> Stopping (claim deleted; provider cascades sandbox + Pod)
Stopping -> Stopped  (claim and Pods gone)
Serving -> Degraded  (probe/validation failure; admission closed, no fallback)
any -> Ambiguous     (more than one Ready Pod; admission closed)
```

An in-place supervisor restart or an unhealthy supervisor recycles the exact
instance by deleting the claim (never by reusing the Pod), exactly as the
Deployment path deletes the runtime Pod. A stopped, idle workspace pool object
is garbage-collected by the dispatcher after a second idle TTL; recovery
treats the missing pool as cleanup proof and fresh demand recreates it by name.

### Fail-closed boundaries

- Unsupported options—`cleanupPolicy: retain`, `boot`, `poolRef`, `snapshot`,
  `hibernation`, and `onDetach`—are rejected before any workspace or RuntimePool
  demand exists.
- Agent Sandbox `templateRef` is rejected because the supervisor's immutable
  image, fence environment, materialization attestation, and signed
  credential-bootstrap key must be controller-rendered. Provider-visible
  templates carry no credential references; claim-side env and volume
  injection are `Disallowed`.
- Substrate follows ADR 0025 instead: its infrastructure `templateRef` is
  required, while the controller derives and owns the immutable runtime
  ActorTemplate that carries the supervisor contract without credentials.
- The provider's managed NetworkPolicy is `Unmanaged`; the pool's own
  default-deny NetworkPolicies select the workspace Pod through propagated
  pool labels, preserving the exact controller-ingress/provider-proxy-egress
  boundary.
- Dispatch requires `--agent-sandbox-enabled` and
  `--acp-workspace-dispatch-enabled`; either missing fails closed with the
  workspace validation status projected on the Task.
- The harness-v1 path rejects execution workspaces with a v1-specific message;
  there is no cross-mode fallback in either direction.
- Missing provider CRDs degrade the pool and close admission; there is no
  fallback to a Deployment workload.
- Task status projection stays provider-neutral (`provider`, `phase`,
  `reason`, policies); claim, sandbox, template names, Pod IPs, and any other
  provider-native identifiers never enter public Task status.

## Consequences

- Phase 2 (Substrate) plugs into the same seams: a second workload backend for
  the pool reconciler plus suspend/resume/snapshot semantics behind the same
  binding contract; the Task-facing API and dispatcher remain unchanged.
- The legacy worker-path workspace resolution (`runAgentInWorkspace`,
  router-based exec) was unreachable from the agent path and has since been
  removed from `workers/common`; its agent-sandbox `templateRef` semantics are
  retired for agent Tasks. The `internal/workspace` agent-sandbox adapter
  remains for the direct-adapter smoke in the live agent-sandbox E2E.
- Live E2E promotion (claim → execute → continue → cancel → restart → cleanup
  through Vekil-backed runtimes) is tracked by issue #343's acceptance list;
  the agent-sandbox kind workflow can now exercise the Task path once runtime
  images and the dispatch flag are enabled.
