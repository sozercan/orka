# AgentRuntime security operations

AgentRuntime integrations preserve one invariant: runtimes may request work,
but Orka owns authorization, durable effects, and delivery.

## Trust boundaries

| Zone | Trust | Notes |
| --- | --- | --- |
| Orka control plane | trusted for policy, approvals, idempotency, audit | owns Tool execution and credentials |
| AgentRuntime adapter | least-privileged `orka.harness.v2` adapter | must not store downstream Tool, provider, Git, or forge secrets |
| Remote execution backend | untrusted/semi-trusted workload runtime | may be prompt-injected or compromised |
| Downstream tools | protected systems | reached only through Orka Tool execution |

## Required controls

- Keep credentials out of `AgentRuntime.spec.deployment.endpoint`.
- Bind runtime bearer Secrets with `orka.ai/agent-runtime-auth=true`, optional `orka.ai/agent-runtime-name`, and required `orka.ai/agent-runtime-endpoint`.
- Use namespace-local facades unless and until a tenant runtime catalog is intentionally designed.
- Materialize the exact brokered policy in `AgentRuntime.spec.capabilities.mcpPolicy`, and make `Task.spec.agentRuntime.allowedTools` equal its allowlist. Brokered mode does not imply access to all Tools.
- Classify remote-exposed Tools with `spec.brokeredToolClass`.
- Use write-class Tools for consequential actions so approval and exact-argument digest checks run before execution.
- Require the per-session loopback MCP proxy to present the active Task,
  attempt, prompt, lease, RuntimeSession, RuntimePool, runtime-instance, and
  controller fences on every call.
- Reserve a durable `ExternalEffect` identity before consequential execution;
  replay only a committed matching response.
- Treat `WaitingForApproval=True` as an intentional parked state, not a failure.
- Investigate `tool_execution_outcome_unknown` before retrying a write; Orka failed closed to avoid duplicate side effects.

## Production hardening checklist

- Prefer HTTPS, mTLS, private networking, and the v2 bearer plus
  operation-capability scheme for external adapters.
- Rotate runtime bearer Secrets and ensure the `AgentRuntime` observes the new Secret resourceVersion before use.
- Keep built-in provider credentials behind the central provider proxy; do not
  add `Agent.spec.secretRef` to Codex, Claude, Copilot, or OpenCode Agents.
- Use `config/acp-production` for direct Kustomize deployments so Vekil accepts
  ingress only from the authenticated provider proxy.
- Keep source-read, target-read, target-write, and forge credentials in
  separate Task workspace references. They are delivered only through the
  controller credential broker to the clean-room Publisher.
- Ensure the Publisher uses the controller artifact authorization broker and
  does not mount the artifact capability signing key.
- Ensure downstream tools honor `Idempotency-Key` for write requests.
- Keep large artifacts in artifact storage and return safe references rather than huge summaries.
- Redact auth headers, tokens, TxTokens, and raw transcripts from adapter logs.
