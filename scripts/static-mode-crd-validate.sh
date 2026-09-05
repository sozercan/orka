#!/usr/bin/env bash
# Validates the shared harness v1/v2 CRDs against a live cluster:
# both-baseline object acceptance, discriminator CEL, contract immutability,
# legacy-surface ratcheting, and static-mode Task binding invariants. The CRDs
# from config/crd/bases must already be applied and Established by their one
# designated platform owner.
#
# Usage: KUBECTL="kubectl" scripts/static-mode-crd-validate.sh
set -euo pipefail

KUBECTL=${KUBECTL:-kubectl}
NAMESPACE=${NAMESPACE:-static-mode-crd-validate}
ZERO_DIGEST="sha256:0000000000000000000000000000000000000000000000000000000000000000"
FAILURES=0

k() {
  # shellcheck disable=SC2086
  ${KUBECTL} "$@"
}

pass() { printf 'ok      %s\n' "$1"; }
fail() {
  printf 'FAILED  %s\n' "$1" >&2
  FAILURES=$((FAILURES + 1))
}

# expect_accept <description> — applies stdin, expecting success.
expect_accept() {
  local description="$1"
  if k apply -f - >/dev/null 2>&1; then
    pass "${description}"
  else
    fail "${description} (expected acceptance)"
  fi
}

# expect_reject <description> <message-fragment> — applies stdin, expecting a
# rejection whose error mentions the fragment.
expect_reject() {
  local description="$1" fragment="$2" output
  if output=$(k apply -f - 2>&1); then
    fail "${description} (expected rejection, got acceptance)"
    return
  fi
  if [[ "${output}" != *"${fragment}"* ]]; then
    fail "${description} (rejected for the wrong reason: ${output})"
    return
  fi
  pass "${description}"
}

# expect_status_patch <accept|reject> <description> <kind/name> <merge-patch> [fragment]
expect_status_patch() {
  local mode="$1" description="$2" target="$3" patch="$4" fragment="${5:-}" output
  if output=$(k -n "${NAMESPACE}" patch "${target}" --subresource=status --type=merge -p "${patch}" 2>&1); then
    if [[ "${mode}" == accept ]]; then pass "${description}"; else fail "${description} (expected rejection, got acceptance)"; fi
    return
  fi
  if [[ "${mode}" == reject ]]; then
    if [[ -n "${fragment}" && "${output}" != *"${fragment}"* ]]; then
      fail "${description} (rejected for the wrong reason: ${output})"
    else
      pass "${description}"
    fi
  else
    fail "${description} (expected acceptance: ${output})"
  fi
}

for removed_crd in \
  agentexecutioncontrols.core.orka.ai \
  agentexecutionpolicies.core.orka.ai \
  agentexecutionadjudications.core.orka.ai; do
  if k get crd "${removed_crd}" >/dev/null 2>&1; then
    fail "superseded coexistence CRD is absent: ${removed_crd}"
  else
    pass "superseded coexistence CRD is absent: ${removed_crd}"
  fi
done

k create namespace "${NAMESPACE}" --dry-run=client -o yaml | k apply -f - >/dev/null
cleanup() { k delete namespace "${NAMESPACE}" --ignore-not-found --wait=false >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "== AgentRuntime discriminated union =="

expect_accept "stored v1 AgentRuntime shape is accepted" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: AgentRuntime
metadata: {name: v1-runtime, namespace: ${NAMESPACE}}
spec:
  contractVersion: orka.harness.v1
  deployment: {mode: external-endpoint, endpoint: "https://harness.example.com"}
  clientAuth:
    bearerTokenSecretRef: {name: harness-auth, key: token}
  capabilities:
    toolExecutionModes: [observed, brokered]
    brokeredToolClasses: [read]
    supportsCancel: true
EOF

expect_accept "v2 AgentRuntime shape is accepted" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: AgentRuntime
metadata: {name: v2-runtime, namespace: ${NAMESPACE}}
spec:
  contractVersion: orka.harness.v2
  deployment: {mode: external-endpoint, endpoint: "https://runtime.example.com"}
  clientAuth:
    controllerBearerTokenSecretRef: {name: auth, key: controller}
    operationCapabilitySecretRef: {name: auth, key: capability}
  capabilities:
    runtimeInstanceID: instance-1
    profile:
      digest: ${ZERO_DIGEST}
      digestSchemaVersion: 1
      acpProfile: acp.v1
      adapterName: adapter
      adapterDigest: ${ZERO_DIGEST}
      providerKind: codex
      model: gpt-5.2-codex
      agentConfigurationDigest: ${ZERO_DIGEST}
      toolPolicyDigest: ${ZERO_DIGEST}
      approvalPolicyDigest: ${ZERO_DIGEST}
      mcpConfigurationDigest: ${ZERO_DIGEST}
      workspaceIntent: read
      proxyCredentialRole: provider-inference
      proxyCredentialScope: "model:gpt-5.2-codex"
      resourceClass: standard
    mcpPolicy:
      allowedTools: []
      disallowedTools: []
      allowBash: false
      approvalRequiredTools: []
    limits:
      maxResidentSessions: 10
      maxConcurrentPrompts: 4
      maxRequestBytes: 1048576
      maxEventLineBytes: 1048576
      maxTerminalResultBytes: 1048576
      maxBufferedEvents: 256
      maxUpdateEventsPerSecond: 100
      minPromptLeaseMillis: 5000
      maxPromptLeaseMillis: 120000
      maxPendingPermissions: 32
      maxWorkspaceDeltaBytes: 536870912
    supportsDrain: true
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
EOF

expect_reject "mixed v1+v2 client auth shapes are rejected" "mutually exclusive" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: AgentRuntime
metadata: {name: mixed-auth, namespace: ${NAMESPACE}}
spec:
  contractVersion: orka.harness.v1
  deployment: {mode: external-endpoint, endpoint: "https://harness.example.com"}
  clientAuth:
    bearerTokenSecretRef: {name: harness-auth, key: token}
    controllerBearerTokenSecretRef: {name: auth, key: controller}
    operationCapabilitySecretRef: {name: auth, key: capability}
EOF

expect_reject "v1 contract with v2 auth shape is rejected" "legacy bearerTokenSecretRef" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: AgentRuntime
metadata: {name: v1-with-v2-auth, namespace: ${NAMESPACE}}
spec:
  contractVersion: orka.harness.v1
  deployment: {mode: external-endpoint, endpoint: "https://harness.example.com"}
  clientAuth:
    controllerBearerTokenSecretRef: {name: auth, key: controller}
    operationCapabilitySecretRef: {name: auth, key: capability}
EOF

expect_reject "v2 contract without pinned capabilities is rejected" "pinned instance" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: AgentRuntime
metadata: {name: v2-no-caps, namespace: ${NAMESPACE}}
spec:
  contractVersion: orka.harness.v2
  deployment: {mode: external-endpoint, endpoint: "https://runtime.example.com"}
  clientAuth:
    controllerBearerTokenSecretRef: {name: auth, key: controller}
    operationCapabilitySecretRef: {name: auth, key: capability}
EOF

expect_reject "contractVersion mutation is rejected" "immutable" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: AgentRuntime
metadata: {name: v1-runtime, namespace: ${NAMESPACE}}
spec:
  contractVersion: orka.harness.v2
  deployment: {mode: external-endpoint, endpoint: "https://harness.example.com"}
  clientAuth:
    controllerBearerTokenSecretRef: {name: auth, key: controller}
    operationCapabilitySecretRef: {name: auth, key: capability}
  capabilities:
    runtimeInstanceID: instance-1
EOF

expect_accept "unclassified stored AgentRuntime is tolerated by the bridge schema" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: AgentRuntime
metadata: {name: unclassified-runtime, namespace: ${NAMESPACE}}
spec:
  deployment: {mode: external-endpoint, endpoint: "https://harness.example.com"}
  clientAuth:
    bearerTokenSecretRef: {name: harness-auth, key: token}
EOF

expect_accept "one-time absent-to-explicit classification is accepted" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: AgentRuntime
metadata: {name: unclassified-runtime, namespace: ${NAMESPACE}}
spec:
  contractVersion: orka.harness.v1
  deployment: {mode: external-endpoint, endpoint: "https://harness.example.com"}
  clientAuth:
    bearerTokenSecretRef: {name: harness-auth, key: token}
EOF

echo "== Agent contract selector =="

expect_accept "v2-classified built-in Agent is accepted" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata: {name: v2-agent, namespace: ${NAMESPACE}}
spec:
  runtime: {type: codex, contractVersion: orka.harness.v2}
  model: {name: gpt-5.2-codex}
EOF

expect_reject "Agent selector mutation is rejected" "immutable" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata: {name: v2-agent, namespace: ${NAMESPACE}}
spec:
  runtime: {type: codex, contractVersion: orka.harness.v1}
  model: {name: gpt-5.2-codex}
EOF

expect_reject "selector with runtimeRef is rejected" "runtimeRef derives the protocol" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata: {name: ref-agent, namespace: ${NAMESPACE}}
spec:
  runtime:
    runtimeRef: {name: external}
    contractVersion: orka.harness.v2
EOF

expect_reject "v2 OpenCode Agent with systemPrompt is rejected" "does not support spec.systemPrompt" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata: {name: v2-opencode, namespace: ${NAMESPACE}}
spec:
  runtime: {type: opencode, contractVersion: orka.harness.v2}
  model: {name: engine/gpt-5.2, contextWindow: 128000, maxTokens: 8192}
  systemPrompt: {inline: "not allowed"}
EOF

expect_accept "stored v1 OpenCode Agent with legacy shape survives" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata: {name: v1-opencode, namespace: ${NAMESPACE}}
spec:
  runtime: {type: opencode, contractVersion: orka.harness.v1}
  model: {name: gpt-5.2, maxTokens: 8192}
  systemPrompt: {inline: "You are a careful engineer."}
  secretRef: {name: opencode-credentials}
EOF

expect_status_patch accept "stored v1 OpenCode Agent /status remains updatable" \
  agents.core.orka.ai/v1-opencode '{"status":{"ready":true,"activeTasks":0}}'

echo "== Task legacy workspace ratchet and binding invariants =="

expect_reject "new Task cannot introduce legacy agentRuntime.workspace" "preserved harness v1 compatibility surface" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata: {name: legacy-workspace-task, namespace: ${NAMESPACE}}
spec:
  type: agent
  prompt: fix it
  agentRef: {name: v2-agent}
  agentRuntime:
    workspace: {gitRepo: "https://github.com/org/repo.git"}
EOF

expect_accept "plain agent Task is accepted" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata: {name: bound-task, namespace: ${NAMESPACE}}
spec:
  type: agent
  prompt: fix it
  agentRef: {name: v2-agent}
EOF

BINDING=$(cat <<JSON
{"schemaVersion":1,"contractVersion":"orka.harness.v1","backend":"harness-wrapper","bindingDigest":"${ZERO_DIGEST}","task":{"namespaceUID":"ns-uid","uid":"task-uid","boundSpecGeneration":1},"snapshot":{"id":"task-uid/${ZERO_DIGEST}","digest":"${ZERO_DIGEST}","schemaVersion":1},"boundAt":"2026-08-05T00:00:00Z"}
JSON
)

expect_status_patch accept "v1 execution binding writes once" \
  tasks.core.orka.ai/bound-task "{\"status\":{\"agentExecutionBinding\":${BINDING}}}"

MUTATED_BINDING=${BINDING/harness-wrapper/external-endpoint}
expect_status_patch reject "binding mutation is rejected" \
  tasks.core.orka.ai/bound-task "{\"status\":{\"agentExecutionBinding\":${MUTATED_BINDING}}}" "write-once and immutable"

expect_status_patch reject "binding removal is rejected" \
  tasks.core.orka.ai/bound-task '{"status":{"agentExecutionBinding":null}}' "write-once and immutable"

expect_status_patch reject "a v1-bound Task cannot acquire v2 execution state" \
  tasks.core.orka.ai/bound-task '{"status":{"execution":{"state":"Queued"}}}' "cannot acquire new v2 execution"

expect_status_patch accept "a v1-bound Task may record v1 harness state" \
  tasks.core.orka.ai/bound-task '{"status":{"harnessRuntime":{"contractVersion":"orka.harness.v1","runtimeName":"wrapper"}}}'

expect_accept "second plain agent Task is accepted" <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata: {name: incoherent-binding-task, namespace: ${NAMESPACE}}
spec:
  type: agent
  prompt: fix it
  agentRef: {name: v2-agent}
EOF

INCOHERENT_BINDING=$(cat <<JSON
{"schemaVersion":1,"contractVersion":"orka.harness.v1","backend":"runtime-pool","bindingDigest":"${ZERO_DIGEST}","task":{"namespaceUID":"a","uid":"b","boundSpecGeneration":1},"snapshot":{"id":"b/${ZERO_DIGEST}","digest":"${ZERO_DIGEST}","schemaVersion":1},"boundAt":"2026-08-05T00:00:00Z"}
JSON
)
expect_status_patch reject "runtime-pool backend requires a v2 binding (coherence)" \
  tasks.core.orka.ai/incoherent-binding-task "{\"status\":{\"agentExecutionBinding\":${INCOHERENT_BINDING}}}" "requires an orka.harness.v2 binding"

echo
if [[ ${FAILURES} -gt 0 ]]; then
  echo "static-mode CRD validation FAILED: ${FAILURES} case(s)" >&2
  exit 1
fi
echo "static-mode CRD validation passed"
