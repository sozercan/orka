# Bring your own AgentRuntime

`AgentRuntime` registers an operator-owned external service that implements `orka.harness.v2`. Orka probes the service, verifies its pinned capability and profile claims, records sanitized readiness data, and dispatches `runtimeRef` Tasks only while the frozen registration still matches.

```text
AgentRuntime registration
  -> GET /v2/health
  -> GET /v2/capabilities
  -> authenticated GET /v2/status
  -> hostile mutation/conformance checks
  -> status.ready + observed v2 identity/profile
```

There is no legacy adapter or agent Job fallback. A registration that is not
ready, is not strict-governed, or changes after Task binding fails closed before
Orka performs a runtime mutation.

## When to use an external registration

Use an external registration to validate an operator-owned service with a stable, immutable runtime instance identity and a reviewed v2 implementation. Built-in Codex, Claude, Copilot, and OpenCode Tasks use controller-owned RuntimePools.

External services are not managed by Orka Kubernetes pool scaling. They must implement their own lifecycle, instance replacement, process cleanup, and capacity controls while preserving the portable v2 semantics.

The external supervisor must start with `ORKA_ACP_CONTROLLER_EPOCH` set to the
current Orka controller epoch. Read it from the controller namespace after the
controller starts:

```bash
kubectl -n <orka-controller-namespace> get cepoch -o json |
  jq -er '[.items[] | select(.spec.name == "orka-controller") | .status.epoch] |
    if length == 1 and (.[0] | type == "number" and . > 0)
    then .[0] else error("expected one initialized controller epoch") end'
```

The record's Kubernetes name is hashed. Select it by `spec.name`, using the
controller's configured logical name if it differs from `orka-controller`.

The supervisor reads this value once at startup. Its operator must watch that
record and restart or replace the supervisor whenever the epoch changes. Keep
the registered `runtimeInstanceID` stable across that restart and assign a new
`ORKA_ACP_SUPERVISOR_BOOT_ID`. Orka marks a stale-epoch runtime not ready and
blocks new Task bindings until authenticated status reports the current epoch.

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
`runtimeRef` Task requires. The complete sample includes
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
    mcpPolicy:
      allowedTools: []
      disallowedTools: []
      allowBash: false
      approvalRequiredTools: []
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

Wait for `status.ready: true` to confirm registration and conformance, then
select the registration from an Agent:

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: external-v2-agent
spec:
  runtime:
    runtimeRef:
      name: sample-external-v2-runtime
```

The referenced Task must use the same workspace intent pinned in the immutable
runtime profile. `capabilities.mcpPolicy` materializes the exact non-secret tool
and approval policy represented by the profile digests. A Task that exposes
brokered tools must set task-level `allowedTools` to that registered allowlist;
Orka rejects a different list before creating a RuntimeSession. External runtimes do not support
`Task.spec.execution.workspace`; the operator owns their infrastructure and
lifecycle.

Orka freezes the AgentRuntime UID, generation, profile, endpoint, authentication
Secret resource versions, and observed runtime instance into the Task binding.
It revalidates that authority before dispatch and recovery mutations. A changed
registration is never silently adopted by an already-bound Task.

External session creation sends no per-Task `AgentConfiguration`. The runtime's
registered profile and `agentConfigurationDigest` are the immutable authority
for image-bound configuration. A `runtimeRef` Agent must therefore omit
`spec.model`, `spec.systemPrompt`, `spec.skills`, enabled `spec.tools`, and all
runtime defaults (`defaultMaxTurns`, `defaultAllowedTools`, `defaultAllowBash`,
and `defaultReasoningEffort`). Disabled Agent tool entries are inert and may
remain. Task-level `agentRuntime.allowedTools` selects the registered
prompt-scoped MCP broker allowlist; it cannot change the registered policy.
External runtimes must advertise
`supportsAgentSessionConfiguration: false` and reject non-null configuration.
For upgrade compatibility, Orka tolerates a persisted `defaultMaxTurns: 50`
written by the older CRD default. New runtimeRef Agents should omit the field.

## Trusted non-governed registrations

`trusted-non-governed` remains an explicit registration mode for operator
inventory and conformance diagnostics, but it cannot satisfy the strict `read`
or `write` workspace guarantees required by `type: agent` Task dispatch. It
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
probe and conformance cycle. Dispatch still revalidates the frozen endpoint,
profile, observed instance, and authentication authority before each external
mutation.
