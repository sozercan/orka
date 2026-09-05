# Getting Started

Orka is a Kubernetes-native platform for running AI agents and tool-using workflows as
durable, observable Tasks. Native AI and container work runs in hardened worker Jobs; ACP
coding agents run as fenced RuntimeSessions in controller-owned RuntimePools. The controller
stores results and delivery receipts and handles sessions, priorities, and delegation.

## Mental Model

Three custom resources cover most use cases:

- **Provider** — an LLM backend (Anthropic, OpenAI, or Azure OpenAI) plus its API-key Secret.
- **Agent** — a reusable configuration: Provider/model, system prompt, tools, skills,
  ACP runtime profile, or coordination settings.
- **Task** — one unit of work. `type: ai` runs through Orka's built-in AI worker, `type: agent`
  runs a Codex, Claude, Copilot, or OpenCode ACP session in a RuntimePool, and `type: container` runs an
  arbitrary container command.

A Task references an Agent, an Agent references a Provider. Results are retrieved over the
REST API, the CLI, or the embedded dashboard. See [Architecture](concepts/architecture.md)
for the full component picture.

## Prerequisites

- Docker 17.03+
- kubectl (version compatible with your cluster)
- Access to a Kubernetes cluster
- An LLM API key (Anthropic, OpenAI, or Azure OpenAI)

For development, you also need:
- Go 1.25.3+
- Bun (for UI build)

## Installation

### Using Helm

A harness-v2 installation requires operator-managed secret material before
`helm install`: a 32-byte agent-execution snapshot key, a webhook serving
certificate with its CA, and the Vekil-backed provider proxy enabled. Set the
file paths first, then run the block; Helm fails rendering if any of these
values are missing:

```bash
: "${SNAPSHOT_KEY_FILE:?set SNAPSHOT_KEY_FILE to a 32-byte key file, e.g. from: openssl rand 32 > snapshot.key}"
: "${WEBHOOK_CERT_FILE:?set WEBHOOK_CERT_FILE to the webhook serving certificate}"
: "${WEBHOOK_PRIVATE_KEY_FILE:?set WEBHOOK_PRIVATE_KEY_FILE to the webhook private key}"
: "${WEBHOOK_CA_FILE:?set WEBHOOK_CA_FILE to the CA certificate}"

kubectl create -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: orka-system
  labels:
    orka.ai/controller-mode: harness-v2
EOF

kubectl -n orka-system create secret generic agent-execution-snapshot-key \
  --from-file=snapshot-key="${SNAPSHOT_KEY_FILE}"
kubectl -n orka-system create secret generic orka-webhook-tls \
  --type=kubernetes.io/tls \
  --from-file=tls.crt="${WEBHOOK_CERT_FILE}" \
  --from-file=tls.key="${WEBHOOK_PRIVATE_KEY_FILE}" \
  --from-file=ca.crt="${WEBHOOK_CA_FILE}"

WEBHOOK_CA_BUNDLE="$(kubectl -n orka-system get secret orka-webhook-tls \
  -o jsonpath='{.data.ca\.crt}')"

helm install orka charts/orka \
  --namespace orka-system \
  --set controller.mode=harness-v2 \
  --set controller.watchNamespace=orka-system \
  --set controller.image.repository=docker.io/sozercan/orka \
  --set controller.image.digest=sha256:<controller-digest> \
  --set publisher.image.repository=docker.io/sozercan/orka-workspace-publisher \
  --set publisher.image.digest=sha256:<publisher-digest> \
  --set controller.acpRuntime.codexImage=docker.io/sozercan/orka-acp-codex@sha256:<codex-digest> \
  --set controller.acpRuntime.claudeImage=docker.io/sozercan/orka-acp-claude@sha256:<claude-digest> \
  --set controller.acpRuntime.copilotImage=docker.io/sozercan/orka-acp-copilot@sha256:<copilot-digest> \
  --set controller.acpRuntime.opencodeImage=docker.io/sozercan/orka-acp-opencode@sha256:<opencode-digest> \
  --set-string controller.agentExecutionSnapshot.existingSecret=agent-execution-snapshot-key \
  --set-string controller.agentExecutionSnapshot.key=snapshot-key \
  --set-string webhooks.tls.existingSecret=orka-webhook-tls \
  --set-string webhooks.caBundle="${WEBHOOK_CA_BUNDLE}" \
  --set providerProxy.enabled=true
```

The provider proxy requires Vekil reachable at
`http://vekil.vekil-system.svc:1337`; alternate upstreams are rejected.

The chart defaults new installations to `harness-v2`. Controller mode remains
an immutable installation identity and cannot be changed during an upgrade.

A normal fresh install creates Orka's 26 cluster-scoped CRDs before the
controller resources. Use `--skip-crds` only when one designated platform or
release owner already manages compatible Orka CRDs for the cluster; all other
Orka releases should use that flag.

:::important[CRDs before every upgrade]
Helm does not create or update files from `crds/` during `helm upgrade`.
Apply the CRDs from the exact target chart before **every** upgrade. This also
applies when upgrading from a chart that installed no CRDs. Helm retains CRDs
and Orka custom resources on uninstall.
:::

Follow the complete commands and ownership guidance in
[`charts/orka/README.md`](https://github.com/orka-agents/orka/blob/main/charts/orka/README.md).

This installs one static `harness-v2` control plane. To keep harness v1 on the
same cluster, install it as a different release with a different controller
namespace, labeled watch namespace, endpoint, RBAC, Lease, store, Secrets, and
wrapper data plane. Existing Tasks and Sessions never move between releases.
See [Operating harness v1 and v2 on one cluster](operations/harness-modes.md).

### Using kubectl

The development target creates the required ACP artifact, publisher, provider-proxy, and SCM-proxy Secrets without replacing existing values:

```bash
# Install CRDs
make install

# Claim the controller namespace for this immutable installation mode
kubectl create -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: orka-system
  labels:
    orka.ai/controller-mode: harness-v2
EOF

# For local evaluation only, generate a seven-day self-signed serving
# certificate and provision the admission runtime's required TLS Secret.
# Production installations should provision an operator-managed certificate.
bash scripts/lib/e2e-admission-tls.sh

# Deploy controller
make deploy \
  IMG=docker.io/sozercan/orka@sha256:<controller-digest> \
  WORKSPACE_PUBLISHER_IMG=docker.io/sozercan/orka-workspace-publisher@sha256:<publisher-digest> \
  ACP_CODEX_RUNTIME_IMG=docker.io/sozercan/orka-acp-codex@sha256:<codex-digest> \
  ACP_CLAUDE_RUNTIME_IMG=docker.io/sozercan/orka-acp-claude@sha256:<claude-digest> \
  ACP_COPILOT_RUNTIME_IMG=docker.io/sozercan/orka-acp-copilot@sha256:<copilot-digest> \
  ACP_OPENCODE_RUNTIME_IMG=docker.io/sozercan/orka-acp-opencode@sha256:<opencode-digest>
```

The Kustomize install does not create an API client identity. The Helm chart
creates the `orka-client` ServiceAccount used by the examples below; for a
Kustomize install create it yourself with the same scope as the Helm client
Role — Task create/delete, RepositoryMonitor management, read-only catalog
resources, session and gateway reads:

```bash
kubectl -n orka-system create serviceaccount orka-client
kubectl -n orka-system create role orka-client \
  --verb=get,list,watch,create,delete --resource=tasks.core.orka.ai
kubectl -n orka-system create role orka-client-monitors \
  --verb=get,list,watch,create,update,patch,delete --resource=repositorymonitors.core.orka.ai
kubectl -n orka-system create role orka-client-read \
  --verb=get,list,watch \
  --resource=agents.core.orka.ai,tools.core.orka.ai,skills.core.orka.ai,providers.core.orka.ai,runtimepools.core.orka.ai,agentruntimes.core.orka.ai
kubectl -n orka-system create role orka-client-sessions \
  --verb=get,list,delete --resource=sessions.core.orka.ai
kubectl -n orka-system create role orka-client-gateway \
  --verb=get,list,watch --resource=gateways.gateway.orka.ai,gatewaybindings.gateway.orka.ai
kubectl create clusterrole orka-client-gatewayclass-viewer \
  --verb=get,list,watch --resource=gatewayclasses.gateway.orka.ai
kubectl -n orka-system create rolebinding orka-client \
  --role=orka-client --serviceaccount=orka-system:orka-client
kubectl -n orka-system create rolebinding orka-client-monitors \
  --role=orka-client-monitors --serviceaccount=orka-system:orka-client
kubectl -n orka-system create rolebinding orka-client-read \
  --role=orka-client-read --serviceaccount=orka-system:orka-client
kubectl -n orka-system create rolebinding orka-client-sessions \
  --role=orka-client-sessions --serviceaccount=orka-system:orka-client
kubectl -n orka-system create rolebinding orka-client-gateway \
  --role=orka-client-gateway --serviceaccount=orka-system:orka-client
kubectl create clusterrolebinding orka-client-gatewayclass-viewer \
  --clusterrole=orka-client-gatewayclass-viewer --serviceaccount=orka-system:orka-client
```

`sessions` is a virtual API resource: the REST API authorizes session reads
and deletes with a SubjectAccessReview even though no CRD backs it. `providers`
read access is what the dashboard Chat provider picker (`GET /providers`)
checks. The dashboard Gateways page lists `gateways` and `gatewaybindings`
(namespaced) and cluster-scoped `gatewayclasses`, each authorized by a
SubjectAccessReview, so the gateway Role and the `gatewayclasses` ClusterRole
mirror the reads the Helm client identity is granted.

`make deploy` applies the same resources as the canonical
`config/acp-production` Kustomize overlay. For direct Kustomize workflows, use
that overlay rather than `config/default`; it includes the Vekil ingress policy
that permits model traffic only through the authenticated provider proxy.

## Quick Start

### 1. Create an LLM Provider

```bash
# Create an API key secret
kubectl create secret generic anthropic-secret \
  --from-literal=api-key=your-api-key

# Create a Provider
kubectl apply -f - <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Provider
metadata:
  name: anthropic
spec:
  type: anthropic
  secretRef:
    name: anthropic-secret
    key: api-key
  defaultModel: claude-sonnet-4-20250514
EOF
```

### 2. Create an Agent

```yaml
kubectl apply -f - <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: assistant
spec:
  providerRef:
    name: anthropic
  model:
    temperature: 0.7
  systemPrompt:
    inline: "You are a helpful assistant."
EOF
```

### 3. Run a Task

```yaml
kubectl apply -f - <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: hello-task
spec:
  type: ai
  agentRef:
    name: assistant
  prompt: "What is Kubernetes?"
EOF
```

### 4. Check the Result

```bash
kubectl get task hello-task

# Get the result via the REST API
curl http://localhost:8080/api/v1/tasks/hello-task/result \
  -H "Authorization: Bearer $(kubectl create token orka-client)"
```

### 5. Retrieve Artifacts

After a task completes, you can list and download generated artifacts:

```bash
# API: list and download artifacts
curl http://localhost:8080/api/v1/tasks/hello-task/artifacts \
  -H "Authorization: Bearer $(kubectl create token orka-client)"
curl -L http://localhost:8080/api/v1/tasks/hello-task/artifacts/output.json \
  -H "Authorization: Bearer $(kubectl create token orka-client)" \
  -o output.json

# CLI
orka task artifacts <task-name>
orka task download <task-name> [filename] -o <path>
```

## Agent Runtimes Quick Start

ACP agent runtimes run the supported Codex, Claude, Copilot, and OpenCode profiles as
fenced RuntimeSessions in controller-owned RuntimePools. External
`orka.harness.v2` registrations remain operator-owned. Once a registration is
current-generation, ready, and strict-governed, an Agent can select it through
`runtimeRef` and use the same durable Task and RuntimeSession lifecycle.

### 1. Configure the central provider proxy

Built-in ACP Agents never reference provider Secrets. Configure Vekil with the
upstream provider credentials and keep Orka's authenticated provider proxy in
front of it. The controller gives RuntimePools only the proxy bearer and the
reviewed provider/model scope; the upstream credential never enters the ACP
process tree.

For Kustomize installs, verify that `config/acp-production` is applied and that
the `provider-auth-proxy` Deployment is Ready before submitting ACP Tasks. Helm
deployments enable the same boundary with `providerProxy.enabled=true`.

### 2. Create an Agent with Runtime

```yaml
kubectl apply -f - <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: claude-agent
spec:
  model:
    name: claude-sonnet-4-20250514
  runtime:
    type: claude
    contractVersion: orka.harness.v2
    defaultMaxTurns: 50
    defaultAllowBash: true
    defaultAllowedTools:
      - Read
      - Write
      - Edit
      - Bash
EOF
```

For Codex Agents, keep `defaultAllowBash: true` for now. The current Codex
runtime implementation fails fast when bash is disabled because the upstream
Codex CLI does not yet expose a reliable shell-disable mode. For OpenCode
Agents, set `runtime.type: opencode`, use the provider/model form expected by
OpenCode (such as `openai/gpt-5.4`), and set reviewed `model.contextWindow`
and `model.maxTokens` ceilings. Orka requires both values and pins them into the
immutable RuntimePool profile so OpenCode compaction and proxy output limits do
not depend on mutable catalog discovery.

### 3. Run an Agent Task

```yaml
kubectl apply -f - <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: code-review
spec:
  type: agent
  agentRef:
    name: claude-agent
  prompt: "Review the code in this repo for security issues. Do not modify files."
  workspace:
    intent: read
    gitRepo: "https://github.com/example/repo.git"
    branch: main
    # Optional for a private source repository. This Secret is resolved only
    # by the clean-room credential broker, never by the ACP runtime.
    # readCredentialRef:
    #   name: repository-read
  agentRuntime:
    maxTurns: 20
EOF
```

### 4. Check the Result

```bash
kubectl get task code-review
kubectl get runtimepools
orka task status code-review

curl http://localhost:8080/api/v1/tasks/code-review/result \
  -H "Authorization: Bearer $(kubectl create token orka-client)"
```

See [Agent Runtimes](concepts/agent-runtimes.md) for full configuration reference.

## Optional Runtime Isolation

If your cluster exposes Kubernetes `RuntimeClass` objects such as `gvisor` or `kata-qemu`, native `ai` and container Tasks can route worker Jobs through them with `spec.execution`. Built-in ACP agent Tasks instead use reviewed RuntimePool resource profiles.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: isolated-hello
spec:
  type: ai
  agentRef:
    name: assistant
  prompt: "Summarize the repo"
```

Use `Agent.spec.execution` for defaults, then override it per task when needed. See [Configuration](concepts/configuration.md#execution), [Agent Runtimes](concepts/agent-runtimes.md#runtime-and-credential-boundaries), and [Security](concepts/security.md#execution-workloads) for details.

## Accessing the Dashboard

```bash
# Port-forward the controller service
kubectl port-forward -n orka-system svc/orka 8080:8080

# Open in browser
open http://localhost:8080
```

## CLI Tool

The `orka` CLI provides browser-based authentication for the web dashboard.

```bash
# Build the CLI
make build-cli

# Login (extracts token from kubeconfig and opens browser)
./bin/orka login

# Login with custom server
./bin/orka login --server https://orka.example.com

# Login with explicit token
./bin/orka login --token <token>

# Specify kubeconfig
./bin/orka login --kubeconfig ~/.kube/my-config
```

The CLI supports token extraction from bearer tokens, token files, exec-based auth (GKE, AWS IAM), and OIDC auth providers.

## Next Steps

**Core concepts**

- [Architecture](concepts/architecture.md) — Controller, workers, CRDs, and task lifecycle
- [Configuration](concepts/configuration.md) — Helm values, controller flags, and metrics
- [Memory](concepts/memory.md) — Namespace-scoped durable memory and reviewable proposals
- [Transaction Token Integration](concepts/transaction-tokens.md) — Request-scoped transaction-token auth
- [Agent Sandbox Workspaces](concepts/agent-sandbox.md) / [Substrate](concepts/substrate.md) — Deferred execution-workspace providers behind the ACP v2 seam
- [Security](concepts/security.md) — Pod hardening, authentication, and multi-tenancy

**Guides & reference**

- [Agent Runtimes](concepts/agent-runtimes.md) — ACP v2 RuntimePools, RuntimeSessions, workspace policy, and delivery
- [Interactive Chat](guides/chat.md) — Chat endpoint with tool execution
- [Multi-Agent Coordination](guides/multi-agent-coordination.md) — Coordinator agents and delegation
- [OpenAI Compatibility](reference/openai-compat.md) — Use any OpenAI-compatible client via `/openai/v1/`
- [Anthropic Compatibility](reference/anthropic-compat.md) — Use Anthropic clients (Claude Code, etc.) via `/anthropic/v1/`
- [API Reference](reference/api-reference.md) — REST API endpoints
