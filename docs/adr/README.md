# Architecture decision records

An architecture decision record (ADR) is a short document explaining **why** Orka works
the way it does. Each one captures a single decision: the problem, the options, what was
chosen, and what that choice costs. They exist so that a year later nobody has to guess
whether something is load-bearing or accidental.

ADRs are historical records, not documentation. They are never rewritten to match the
current code — when a decision is replaced, the old record stays and a new one supersedes
it. If you want to know how Orka behaves *today*, read the
[documentation](../../website/docs/getting-started.md); if you want to know why it behaves
that way, read here.

## Status labels

| Status | Means |
| --- | --- |
| **Accepted** | The decision is in force. The code should match it. |
| **Superseded** | A later ADR replaced this one. Kept for the reasoning it records. |
| **Deferred** | The decision was made but the work has not landed, usually because it depends on something outside Orka. |

## The records

### Harness v1 → v2 migration

How Orka moved from sidecar-wrapped agents to ACP RuntimePools without breaking running
clusters. See [Harness modes](../../website/docs/operations/harness-modes.md) for what
shipped.

| # | Decision | Status |
| --- | --- | --- |
| [0016](0016-harness-migration-strategy.md) | Full active coexistence for the v1/v2 migration | Superseded by 0018 |
| [0017](0017-harness-coexistence-architecture.md) | Coexistence architecture and ownership contracts | Superseded by 0018 |
| [0018](0018-static-harness-modes-and-namespace-isolation.md) | Static harness modes and namespace-isolated control planes | Accepted |

### The execution workspace model

What a workspace *is* as an API object, who owns which half of its status, and how an
attachment is fenced against a stale controller.

| # | Decision | Status |
| --- | --- | --- |
| [0001](0001-execution-workspace-default-provider.md) | An explicit default provider for execution workspaces | Superseded by the ACP RuntimePool cutover |
| [0002](0002-provider-neutral-execution-workspace-status.md) | Provider-neutral workspace status | Superseded for built-in agent Tasks |
| [0006](0006-use-wrapper-first-execution-workspace-providers.md) | Wrapper-first workspace providers | Superseded by 0022 and the ACP RuntimePool cutover |
| [0021](0021-execution-workspace-domain-and-ownership.md) | Workspaces as provider-neutral owned resources | Accepted |
| [0022](0022-workspace-crd-control-plane-and-agent-data-plane.md) | Generic CRDs for control, a versioned agent for data | Accepted |
| [0023](0023-workspace-attachment-fencing-and-transport-security.md) | Fence attachments with epochs, rotated tokens, and TLS | Accepted |

### Workspace-backed ACP RuntimePools

Binding a Task's workspace to a dedicated RuntimePool, and what suspend/resume may and may
not do with a running agent's memory. See
[Agent Sandbox](../../website/docs/concepts/agent-sandbox.md) and
[Substrate](../../website/docs/concepts/substrate.md).

| # | Decision | Status |
| --- | --- | --- |
| [0024](0024-acp-execution-workspace-runtime-pools.md) | Workspace-provider-backed ACP RuntimePools | Accepted |
| [0025](0025-substrate-backed-runtime-pools.md) | Substrate-backed pools, without suspension | Accepted |
| [0026](0026-acp-class-backed-execution-workspaces.md) | Class-backed execution workspaces | Accepted |
| [0027](0027-substrate-data-only-suspension.md) | Substrate data-only cold suspension and resume | Accepted |
| [0028](0028-agent-sandbox-pvc-cold-resume.md) | Agent Sandbox PVC-backed cold suspension and resume | Accepted |
| [0029](0029-acp-workspace-retention.md) | Bounded retention for ACP execution workspaces | Accepted |
| [0030](0030-substrate-full-memory-restore-gate.md) | The security gate before full-memory restore is ever allowed | Accepted |

### The Substrate control plane

Decisions about talking to Agent Substrate specifically. Several are deferred: the pinned
upstream release does not yet expose the API they need.

| # | Decision | Status |
| --- | --- | --- |
| [0003](0003-use-provider-route-for-workspace-daemon.md) | Use the provider route for Workspace Daemon calls | Deferred |
| [0004](0004-use-minimal-substrate-control-client.md) | A minimal Substrate control API client | Deferred |
| [0005](0005-require-explicit-substrate-api-trust.md) | Require explicit trust for the Substrate control API | Accepted, not yet active |
| [0007](0007-substrate-actor-pool-oversubscription.md) | Oversubscription as controller-owned actor pools | Deferred |

### Sessions, pools, and task outcomes

Where session state lives, and the rule that keeps a Task's recorded outcome honest when
publication fails afterwards.

| # | Decision | Status |
| --- | --- | --- |
| [0008](0008-runtime-session-internal-store-first.md) | Start RuntimeSession persistence as an internal store | Superseded by the Kubernetes cutover |
| [0009](0009-defer-runtime-session-ui-until-public-api.md) | Defer runtime-session UI until a public API exists | Accepted |
| [0014](0014-task-outcome-and-workspace-finalization.md) | Separate immutable Task outcome from workspace finalization | Accepted |
| [0015](0015-provider-versioning-draining-pools-and-services.md) | Pin provider/class revisions; model draining generically | Accepted |

### Gateways

How Orka accepts work from and returns work to external systems exactly once. See
[Gateways](../../website/docs/operations/gateways.md).

| # | Decision | Status |
| --- | --- | --- |
| [0012](0012-gateway-inbox-outbox-semantics.md) | Durable at-least-once inbox and outbox semantics | Accepted |
| [0013](0013-stage-gateway-resource-rollout.md) | Stage the generic gateway resource rollout | Accepted |
| [0020](0020-gateway-session-canonical-history.md) | Keep external conversation history in Orka Sessions | Accepted |

### Governance and telemetry

| # | Decision | Status |
| --- | --- | --- |
| [0010](0010-genai-metrics-export.md) | Export GenAI metrics through OTLP push | Accepted |
| [0011](0011-vendor-neutral-transaction-and-outbound-access.md) | Separate transaction governance from outbound resource access | Accepted |
| [0019](0019-genai-semconv-constants-strategy.md) | Hand-roll GenAI semantic-convention constants | Accepted |

## Writing a new one

Copy the shape of a recent record — [0028](0028-agent-sandbox-pvc-cold-resume.md) is a
good example:

```markdown
# NN. Short imperative title

Date: YYYY-MM-DD

## Status

Accepted. One or two sentences: what this implements, what it supersedes, what it leaves open.

## Context

The forces at play. What made the decision necessary.

## Decision

What was chosen, stated as rules the code must follow.

## Consequences

What this costs, what it rules out, and what now has to stay true.
```

Take the next free number. If your ADR replaces an earlier one, say so in your **Status**
section, edit the older record's **Status** to point at yours, and update the table above —
but leave the rest of the older record untouched.
