---
slug: /agent-sandbox
description: "Running an agent's workspace in a kubernetes-sigs Agent Sandbox, behind Orka's ACP lifecycle."
---

# Agent Sandbox workspaces

Upstream `agent-sandbox` is an externally installed and operated
execution-workspace provider. Orka can host a built-in agent Task's ACP
RuntimeSession inside a provider-owned sandbox through a
**workspace-provider-backed RuntimePool**. The integration is disabled by
default and fails closed.

Orka requires agent-sandbox `v1.0.0`; older releases are unsupported. Operators
upgrading an existing v0.5 installation must complete the upstream
[storage migration](https://github.com/kubernetes-sigs/agent-sandbox/blob/v0.5.6/docs/api-migration-guide.md)
before installing v1.0.0. Orka does not install, upgrade, or migrate the
provider in production.

`Task.spec.workspace` remains the only repository surface — verified source,
workspace intent, and clean-room publication policy:

```yaml
spec:
  type: agent
  workspace:
    intent: read
    gitRepo: https://github.com/example/project.git
    readCredentialRef:
      name: project-read
```

`Task.spec.execution.workspace` additionally requests a physical
execution-workspace provider for the RuntimeSession:

```yaml
spec:
  type: agent
  execution:
    workspace:
      enabled: true
      provider: agent-sandbox
      # reusePolicy: session   # with spec.sessionRef, continuation reuses the
      #                        # same workspace-backed pool while it is alive
```

## Execution model

```text
Task
  -> workspace binding frozen into the immutable execution snapshot
  -> dedicated single-session RuntimePool (acp-ws-<runtime>-<hash>)
  -> credential-free controller-rendered SandboxTemplate + zero-replica SandboxWarmPool
  -> one SandboxClaim; the sandbox Pod runs the immutable ACP runtime image
  -> exact Sandbox blueprint attestation + controller-signed credential bootstrap
  -> the authenticated exact-instance fence probe selects the ActiveInstance
  -> ephemeral RuntimeSession, fenced prompts, workspace validation,
     optional clean-room Workspace/Publisher transaction — all unchanged
```

Only workload materialization changes: the provider control plane owns the
sandbox and its Pod, while Orka owns the Task attempt, RuntimeSession, prompt
lease, fences, publication records, drain barriers, and recovery. The sandbox
Pod has no Git credential and no direct SCM publication egress; the pool's own
default-deny NetworkPolicies select it, and the provider's managed
NetworkPolicy is disabled.

## Enablement and fail-closed boundaries

Dispatch requires both controller flags:

- `--agent-sandbox-enabled` — the provider is installed and admitted;
- `--acp-workspace-dispatch-enabled` — workspace-provider-backed RuntimeSession
  dispatch (also `ORKA_ACP_WORKSPACE_DISPATCH_ENABLED=true`).

Everything the adapter cannot host is rejected before any workspace or
RuntimePool demand exists, with the reason projected to
`Task.status.executionWorkspace`:

- unsupported providers (only `agent-sandbox` and `substrate` are implemented; see the [Substrate](substrate.md) page for the Phase 2 backend);
- `templateRef` — ACP RuntimeSessions run only controller-rendered sandbox
  templates, because the immutable runtime image, fence environment,
  materialization attestation, and signed bootstrap key must be rendered as one
  exact unit. The provider-visible template carries no credential references;
- `cleanupPolicy: retain`, `onDetach`, `boot`, `poolRef`, `snapshot`,
  `hibernation`;
- any workspace request on the harness-v1 path — there is no cross-mode
  fallback in either direction;
- missing provider CRDs — the pool degrades and closes admission rather than
  falling back to a Deployment workload.

Task status stays provider-neutral: provider, phase, reason, and policies.
Claim, sandbox, and template names, Pod IPs, and other provider-native
identifiers never enter public Task status.

## Lifecycle

The claim is deleted after an authenticated supervisor drain and a persisted
quiescence barrier (scale-to-zero, rollout, supervisor restart, or
identity-capacity rotation), and the provider cascades the sandbox and Pod.
Pool finalization removes the claim, warm pool, and template idempotently. A
stopped, idle workspace pool object is garbage-collected after a second idle
TTL; recovery treats the missing pool as proof of RuntimeSession cleanup and
fresh demand recreates it deterministically by name.

See `docs/adr/0024-acp-execution-workspace-runtime-pools.md` for the full
provider-neutral contract, ownership state machine, and recovery semantics.

## RuntimeClass

`Task.spec.execution.runtimeClassName`, per-Task placement, and custom Task
resource requests remain unsupported by the built-in ACP path. Runtime
isolation and resources are selected through reviewed RuntimePool profiles.
Container and native `ai` Tasks keep their existing `spec.execution` behavior.

## Local evaluation material

The repository still contains local/kind evaluation scripts for the older
worker-based execution-workspace prototype. They are not the supported ACP v2
deployment path and should not be used as release evidence. Live ACP
validation should verify RuntimePool scale-up, exact-instance fencing, Session
continuation, cancellation, workspace validation, clean-room publication,
controller restart behavior, pool replacement, and cleanup — including the
workspace-backed pool variants when the dispatch flag is enabled.
