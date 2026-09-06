---
slug: /substrate
description: "Running an agent's workspace in an Agent Substrate gVisor actor, behind Orka's ACP lifecycle."
---

# Agent Substrate workspaces

Agent Substrate is an externally installed and operated execution-workspace
provider. Orka can host a built-in agent Task's ACP RuntimeSession inside a
gVisor-isolated Substrate Actor through a **workspace-provider-backed
RuntimePool** (Phase 2 of the seam established for
[Agent Sandbox](agent-sandbox.md)). The integration is disabled by default and
fails closed; there is never a fallback to the removed worker-based path or to
the agent-sandbox backend.

`Task.spec.workspace` remains the only repository surface:

```yaml
spec:
  type: agent
  workspace:
    intent: write
    gitRepo: https://github.com/example/project.git
    readCredentialRef:
      name: project-read
    publicationGitRepo: https://github.com/example/project.git
    publicationCredentialRef:
      name: project-publish
    pushBranch: orka/example-change
```

The separate Workspace/Publisher performs clone, deterministic commit
preparation, exact-ref push, independent verification, and optional PR
reconciliation. The Actor never receives publication credentials.

`Task.spec.execution.workspace` requests the Substrate backend. Unlike
agent-sandbox, `templateRef` is **required**: it names the operator-owned
*infrastructure* ActorTemplate whose placement fields (workerPoolRef, runsc
build, snapshot location) seed the controller-rendered runtime template.

```yaml
spec:
  type: agent
  execution:
    workspace:
      enabled: true
      provider: substrate
      templateRef:
        namespace: ate-demo
        name: orka-codex-infra
```

## Execution model

```text
Task
  -> workspace binding (provider, policies, session key, infrastructure
     template) frozen into the immutable execution snapshot
  -> dedicated single-session RuntimePool (acp-ws-<runtime>-<hash>)
  -> derived, controller-owned ActorTemplate: operator infrastructure fields
     (including the provider-required snapshotsConfig) + the immutable ACP
     runtime container with fence env literals and NO credential material —
     only the public per-pool bootstrap nonce
  -> one Actor, initially booted from scratch (ResumeActor boot=true); a
     supported DataOnly continuation cold-boots it from workspace data
  -> post-boot credential bootstrap: the supervisor boots credential-free into
     an awaiting phase; the controller seeds the pool tokens over the router
     with a one-time, nonce-gated, idempotent PUT (a payload conflict recycles
     the exact instance)
  -> authenticated exact-instance fence probe through the router
     (Host: <actorID>.<actorDNSSuffix>) selects the ActiveInstance
  -> ephemeral RuntimeSession, fenced prompts, workspace validation,
     optional clean-room Workspace/Publisher transaction — all unchanged
```

Orka remains authoritative for Task attempt state, `OutcomeUnknown`
classification, epochs/fences/request digests, prompt leases, permissions,
cancellation, canonical transcripts, workspace deltas, publication, and
delivery receipts. Substrate supplies physical placement and gVisor isolation.

## Suspension modes

The provider's default `Full` snapshot scope checkpoints Actor process memory.
A running supervisor holds live pool and provider credentials, so Orka never
uses full-memory suspension for a live ACP actor. Provider-initiated suspension
or snapshots also fail closed: admission closes, the actor is recycled, and
the replacement boots fresh.

Class-backed workspaces define one narrower contract for a future
fencing-capable provider client. A session-reused Substrate class may declare
`onDetach: Suspend` when its `RuntimeWorkspaceProfile` sets
`substrate.suspend.mode: DataOnly`, but the current in-tree client rejects any
such pool before actor creation. It cannot return the immutable Actor and
ActorSnapshot proof required to fence the checkpoint and resume mutations.

Once a provider client can supply that proof, the controller can drain the
RuntimeSession, prove the supervisor is quiescent, and revalidate the derived
ActorTemplate's exact snapshot policy before requesting a checkpoint. That
policy persists only the controller-owned `DurableDir` mounted at
`/durable/orka-workspace` with `onPause: Data`, `onCommit: Data`, and
`onResume.fromData: ColdBoot`. Process memory, session roots, and credentials
stay ephemeral. Resume restores the workspace data into a fresh supervisor
boot and repeats the signed credential bootstrap.

Every legacy provider-shaped request remains non-suspendable. Options that
imply warm or retained workspaces (`boot`, `poolRef`, `snapshot`,
`hibernation`, `onDetach`, and `cleanupPolicy: retain`) are rejected on that
path. Operators must also disable provider-side idle suspension for ACP actor
templates because only the controller-authorized DataOnly flow records the
required consent and fences.

During ordinary deletion, Orka destroys the workload's memory by deleting its
single-workload worker Pod before settling and deleting the actor. The
provider-required golden snapshot remains safe because the rendered container
is credential-free until the controller seeds the real booted actor.

See `docs/adr/0025-substrate-backed-runtime-pools.md` for the full-memory safety
analysis and `docs/adr/0027-substrate-data-only-suspension.md` for the DataOnly
cold suspension contract.

## Enablement and operator requirements

Dispatch requires `--substrate-enabled` (with valid `--substrate-api-*`,
`--substrate-router-url`, and `--substrate-actor-dns-suffix` configuration)
plus `--acp-workspace-dispatch-enabled`. Either missing fails closed with the
reason projected to `Task.status.executionWorkspace`.

- The infrastructure ActorTemplate must exist in the referenced namespace; its
  containers are never executed by ACP pools.
- The controller needs an operator-provided RoleBinding in the template
  namespace granting `ate.dev` ActorTemplate CRUD (the derived template is
  created there). No Secret access is needed: pool credentials stay in the
  controller's runtime namespace and are seeded post-boot over the nonce-gated
  credential bootstrap endpoint.
- The controller also needs Pod `get`/`list`/`delete` in the provider worker
  namespace for the credential-safe teardown of live actors.
- The referenced WorkerPool must be dedicated to Orka ACP runtimes. The
  controller needs NetworkPolicy CRUD in its namespace and installs an
  egress-only default deny plus DNS, controller API, and provider-proxy
  allowlists selecting `ate.dev/worker-pool` before any Actor receives
  credentials. Those policies remain until the Actor is gone.
- The Substrate control plane and router share the cluster with Orka;
  cross-cluster topologies are unsupported and fail closed.
- The Actor's egress must reach the Orka controller API and the provider
  proxy (Vekil).
- Pool finalization deletes the actor through the control API before the pool
  is released; an unreachable control plane blocks pool deletion rather than
  leaking a credentialed workload.

Task status stays provider-neutral: provider, phase, reason, and policies.
Route hosts, worker names, snapshot URIs, and every other provider-assigned
identifier never enter public Task status. `status.execution.runtimeInstanceID`
uses the opaque Orka fence `workspace:<sha256(actorID)>.<bootID>`; the raw Actor
ID remains internal, exactly as provider routes and worker placement do.
