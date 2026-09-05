---
description: "Registering your own external agent runtime with Orka using the AgentRuntime resource."
---

# Bring your own AgentRuntime

`AgentRuntime` registers an operator-owned external service that implements `orka.harness.v2`. Orka probes the service, verifies its pinned capability/profile claims, and records sanitized readiness data.

```text
AgentRuntime registration
  -> GET /v2/health
  -> GET /v2/capabilities
  -> authenticated GET /v2/status
  -> hostile mutation/conformance checks
  -> status.ready + observed v2 identity/profile
```

Registration and conformance are available today, but
`Agent.spec.runtime.runtimeRef` Task planning remains fail-closed until the
external v2 dispatcher support boundary is enabled. There is no legacy adapter
or agent Job fallback.

## When to use an external registration

Use an external registration to validate an operator-owned service with a stable, immutable runtime instance identity and a reviewed v2 implementation. Built-in Codex, Claude, Copilot, and OpenCode Tasks use controller-owned RuntimePools.

External services are not managed by Orka Kubernetes pool scaling. They must implement their own lifecycle, instance replacement, process cleanup, and capacity controls while preserving the portable v2 semantics.

## Authentication Secrets

Create two independent Secrets in the AgentRuntime namespace:

- a controller bearer token for authenticated requests;
- an HMAC capability secret used to bind each mutation to its operation/fence/request digest.

Each Secret must contain at least 32 bytes and be bound to the registration and endpoint:

```yaml
metadata:
  labels:
    orka.ai/agent-runtime-auth: "true"
    orka.ai/agent-runtime-name: external-acp
  annotations:
    orka.ai/agent-runtime-endpoint: http://external-acp.default.svc.cluster.local:8080
```

Do not reuse provider, Git read, Git publication, forge, or downstream Tool credentials for either role.

## Strict governed registration

The following abbreviated registration declares the strict guarantees that a
future `runtimeRef` Task dispatcher will require. The complete sample includes
all required profile digests and limits. Every claim must match the runtime's
public capabilities, authenticated status, and hostile conformance behavior.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: AgentRuntime
metadata:
  name: external-acp
spec:
  contractVersion: orka.harness.v2
  deployment:
    mode: external-endpoint
    endpoint: http://external-acp.default.svc.cluster.local:8080
  clientAuth:
    controllerBearerTokenSecretRef:
      name: external-acp-controller-auth
      key: token
    operationCapabilitySecretRef:
      name: external-acp-operation-auth
      key: capability-secret
  capabilities:
    runtimeInstanceID: external-acp-instance-01
    profile:
      digest: sha256:0000000000000000000000000000000000000000000000000000000000000000
      digestSchemaVersion: 1
      acpProfile: acp.v1
      adapterName: operator-reviewed-adapter
      adapterDigest: sha256:0000000000000000000000000000000000000000000000000000000000000000
      providerKind: operator-managed
      model: operator-reviewed-model
      agentConfigurationDigest: sha256:0000000000000000000000000000000000000000000000000000000000000000
      toolPolicyDigest: sha256:0000000000000000000000000000000000000000000000000000000000000000
      approvalPolicyDigest: sha256:0000000000000000000000000000000000000000000000000000000000000000
      mcpConfigurationDigest: sha256:0000000000000000000000000000000000000000000000000000000000000000
      workspaceIntent: read
      proxyCredentialRole: operator-managed
      proxyCredentialScope: external-runtime
      resourceClass: external
    limits:
      maxResidentSessions: 10
      maxConcurrentPrompts: 4
      maxRequestBytes: 1048576
      maxEventLineBytes: 262144
      maxTerminalResultBytes: 1048576
      maxBufferedEvents: 4096
      maxUpdateEventsPerSecond: 100
      minPromptLeaseMillis: 5000
      maxPromptLeaseMillis: 120000
      maxPendingPermissions: 32
      maxWorkspaceDeltaBytes: 104857600
    supportsDrain: false
    workspaceGovernance:
      mode: strict-governed
      trusted: false
      orkaOwnedWorkspaceDeltas: true
      promptScopedBrokerAuthorization: true
      noDirectSCMPublication: true
      orkaOwnedCleanRoomPublication: true
      exactInstanceFencing: true
      duplicateSafeMutations: true
      cancellationSettlement: true
```

Apply the complete checked-in sample:

```bash
kubectl apply -f config/samples/core_v1alpha1_agentruntime.yaml
kubectl get agentruntime sample-external-v2-runtime -o yaml
```

Wait for `status.ready: true` to confirm registration and conformance. Do not
select the registration from a production Agent yet: the controller currently
rejects `runtimeRef` Task planning at the external dispatch support boundary.
The future Agent selection shape is:

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: external-v2-agent
spec:
  model:
    name: operator-reviewed-model
  runtime:
    runtimeRef:
      name: sample-external-v2-runtime
```

When the dispatch boundary is enabled, the referenced Task must use the same
workspace intent pinned in the immutable runtime profile and provide an
explicit task-level `allowedTools` policy when brokered tools are exposed.

## Trusted non-governed registrations

`trusted-non-governed` remains an explicit registration mode for operator
inventory and conformance diagnostics, but it cannot satisfy the strict `read`
or `write` workspace guarantees required by future `type: agent` Task dispatch. It
must not claim Orka-owned deltas, prompt-scoped broker authorization, clean-room
publication, exact-instance fencing, duplicate safety, or cancellation
settlement.

## Strict governed behavior

A strict external runtime must advertise every required workspace-governance guarantee and pass the matching conformance checks. It must not receive Git publication credentials, publish from child-controlled Git state, or hold durable session-wide broker authority.

Strict mode does not turn an external service into a RuntimePool. Orka does not create, count, drain, or replace its Pods unless a separate operator does so.

## Implement the protocol

Implement the endpoints and semantics in the [AgentRuntime adapter contract](../development/agent-runtime-adapter-contract.md):

- safe health/capability probes;
- authenticated status and mutations;
- RuntimeSession create/delete;
- prompt stream, lease, permission, and cancellation operations;
- workspace-delta operation when strict workspace support is claimed;
- exact duplicate handling, request-digest conflicts, and stale-fence rejection;
- bounded diagnostics and terminal event rules.

## Validate

Run the conformance suite from `internal/harness/v2/conformance` against the service before applying a registration. Then verify:

```bash
orka agent-runtime list
orka agent-runtime get external-acp -o yaml
```

`status.ready: true` proves the configured registration passed the current
probe/conformance cycle. It does not enable Task dispatch; `runtimeRef`
planning remains fail-closed until the external v2 dispatcher support boundary
is enabled.
