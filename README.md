<div align="center">

<img src="website/static/img/orka-logo.png" alt="Orka" width="400" />

# Orka

**Kubernetes-native AI agent orchestration.**

[Getting Started](website/docs/getting-started.md) · [Architecture](website/docs/concepts/architecture.md) · [API Reference](website/docs/reference/api-reference.md) · [Documentation](#documentation)

</div>

---

Orka turns your Kubernetes cluster into an AI-powered task execution platform. Native AI and container work run as Kubernetes Jobs; ACP coding agents run as fenced RuntimeSessions in controller-owned, scale-to-zero RuntimePools. A coordinator agent dynamically decomposes complex tasks, spawns specialist agents to work in parallel, and synthesizes their results — no manual orchestration graphs required.

One `helm install`, one LLM secret, and you're chatting with an orchestrator that handles the rest.

> [!IMPORTANT]
> **Orka is experimental and under active development.** APIs, CRDs, and behavior may change without notice between releases, and it is not yet recommended for production use. Feedback, bug reports, and feature ideas are very welcome — please [open an issue](https://github.com/orka-agents/orka/issues).

> [!NOTE]
> The organization and repositories are intended to be donated to a community-governed foundation at the appropriate time. Until then, the project is governed by Microsoft policy, and external contributors are required to sign the Microsoft Contributor License Agreement (CLA).

## Why Run AI Agents on Kubernetes?

**No API keys on developer machines** — LLM credentials live in Kubernetes Secrets, managed by your platform team. Developers connect via ServiceAccount tokens — no risk of leaked keys in dotfiles, shell history, or laptops.

**Centralized control** — One place to set model policies, rate limits, and allowed providers across every team. Swap models or providers without touching developer configs.

**Every agent action is auditable** — Tasks have durable execution events, Prometheus metrics, structured results, and, for ACP agents, fenced attempt/session and delivery receipts. Know exactly what every agent did, when, and at what cost.

**Hardened execution** — Native workers use hardened per-Task Pods. ACP runtimes use digest-pinned shared Pods with private per-session directories and identities; a RuntimePool is a same-trust-domain boundary, not cross-tenant isolation.

**Scale with your cluster** — Priority scheduling, retry policies, concurrency limits, and cron-based execution — all handled by the Kubernetes control plane you already operate.

## What Can You Build?

**Parallel code review** — Spawn a swarm of review agents — security, performance, test coverage, accessibility, whatever you need. Each reviews independently and in parallel, then the coordinator synthesizes findings into a single report.

**Autonomous dev workflows** — A coordinator agent dynamically breaks down a feature request, delegates implementation to specialist agents (backend, frontend, tests), and opens a PR with the combined result — no predefined workflow graphs.

**Research with competing hypotheses** — Multiple agents investigate different theories in parallel, challenge each other's findings, and converge on the strongest explanation. The adversarial structure avoids the anchoring bias of sequential investigation.

**Scheduled operations** — Cron-based agents that run daily security scans, dependency audits, or report generation — all with retry policies and webhook notifications.

**Use your favorite AI client** — Connect Continue, Cursor, or any OpenAI-compatible client to Orka's API. Your cluster manages the LLM credentials — developers just code.

**CI/CD integration** — Trigger agent tasks from GitHub Actions, monitor progress via the REST API, and gate deployments on agent analysis.

## Features

- 🤖 **AI Agents** — Anthropic, OpenAI, or Azure OpenAI with tools, skills, and session persistence
- 🛠️ **ACP Agent Runtimes** — Run Codex, Claude, Copilot, and OpenCode through digest-pinned RuntimePools, or dispatch to strict-governed external `orka.harness.v2` runtimes through `runtimeRef`
- 🔁 **Autonomous Task Loops** — Coordinators can iterate on long-running goals until complete, canceled, or at an iteration limit
- 🔀 **Multi-Agent Coordination** — Coordinators delegate to specialists with depth and concurrency controls
- 💬 **Interactive Chat** — Agentic orchestrator with SSE streaming that creates and manages agents and tasks for you
- 🌐 **Generic Gateways** — Versioned, authenticated ingress and idempotent outbound delivery for external messaging and event systems
- 🧠 **Durable Memory** — Namespace-scoped recall, transcript search, and reviewable memory proposals that can be applied
- 🛡️ **Repository Security Scanning** — Scheduled and incremental repository scans with threat models, validated findings, patch generation, and remediation PRs
- 🔎 **Repository Monitors** — Durable GitHub PR review queues with scheduled and webhook-triggered review runs
- 🧰 **Deferred Workspace Providers** — Evaluate `agent-sandbox` or Substrate separately; neither is a current ACP execution path
- 🖥️ **Web Dashboard** — Built-in React UI embedded in the controller binary — zero extra deployments
- 📦 **Declarative Control** — Workload, gateway, workspace, and Kubernetes-authoritative ACP control CRDs for GitOps workflows
- ⏰ **Scheduled Tasks** — Cron-based recurring execution with concurrency policies
- 🔌 **REST & OpenAI-Compatible API** — Full CRUD + `/openai/v1/chat/completions` endpoint for Continue, Cursor, and any OpenAI-compatible client
- 🔐 **Kubernetes, OIDC & Transaction-Token Auth** — ServiceAccount tokens by default, with optional OIDC and scoped vendor-neutral transaction governance
- 🔮 **Anthropic-Compatible API** — `/anthropic/v1/messages` endpoint for Claude Code and other Anthropic-native clients
- 📊 **Observability** — Prometheus metrics, structured logging, health probes, and optional OpenTelemetry traces + GenAI OTLP metrics
- 🔒 **Hardened by Default** — Non-root native workers, fenced private ACP child identities, read-only filesystems, and authenticated broker boundaries

The ACP hard cutover keeps control authority in Kubernetes:
`ControllerEpoch`, `PromptAttempt`, `RuntimeSessionControl`, `BranchClaim`,
`Publication`, and `ExternalEffect` status plus coordination Leases. SQLite is
limited to transcript/SessionTurn payloads, deferred outbox projections, and
artifact payloads (including result bodies). Provider traffic uses the central authenticated proxy;
prompt tools use prompt-scoped MCP; and source-read, target-read, target-write,
and forge credentials reach only the clean-room Publisher through the
credential broker. Artifact access is separately operation-scoped.

## Quick Start

### Install

```bash
kubectl create -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: orka-system
  labels:
    orka.ai/controller-mode: harness-v2
EOF

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
  --set controller.acpRuntime.opencodeImage=docker.io/sozercan/orka-acp-opencode@sha256:<opencode-digest>
```

The chart defaults new installations to `harness-v2`. Controller mode remains
an immutable installation identity and cannot be changed during an upgrade.

For direct Kustomize deployments, use `config/acp-production`, not
`config/default`. The production overlay includes the cross-namespace Vekil
ingress policy that permits model traffic only through Orka's authenticated
provider proxy:

```bash
kubectl create -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: orka-system
  labels:
    orka.ai/controller-mode: harness-v2
EOF
kubectl apply -k config/acp-production
```

Provision the required system Secrets and digest-pinned images before applying
the overlay; `make deploy` performs those checks and applies the equivalent
resource set.

A fresh Helm install creates the chart CRDs unless `--skip-crds` is used. Helm does not update CRDs during `helm upgrade`, so apply the CRDs from the exact target chart before every controller upgrade. Designate one lifecycle owner for cluster-scoped CRDs and see the [Helm CRD lifecycle guide](charts/orka/README.md).

Harness v1 and v2 may share a cluster only as separate static-mode releases
with disjoint namespaces, endpoints, RBAC, Leases, stores, and data planes.
Tasks and Sessions never migrate between them. See the
[harness mode operations guide](website/docs/operations/harness-modes.md).

### Set Up a Provider

```bash
kubectl create secret generic anthropic-secret \
  --from-literal=api-key=your-api-key

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

That `Provider` Secret is used by native `type: ai` Tasks and the compatible
chat APIs. Built-in ACP Agents do **not** reference provider Secrets. Codex,
Claude, Copilot, and OpenCode RuntimeSessions reach Vekil only through the central authenticated
provider proxy. Source-read, target-read, target-write, and forge credentials
are brokered separately to the clean-room Workspace/Publisher.

### Start Chatting

Use the built-in dashboard, or connect any OpenAI-compatible client:

```bash
kubectl port-forward -n orka-system svc/orka 8080:8080

# Open the web dashboard
open http://localhost:8080
```

The built-in orchestrator creates agents, runs tasks, monitors progress, and returns results — all from natural language. See the [OpenAI Compatibility](website/docs/reference/openai-compat.md) and [Anthropic Compatibility](website/docs/reference/anthropic-compat.md) docs for proxy setup with your preferred client.

## Documentation

|                                                              |                                                       |
| ------------------------------------------------------------ | ----------------------------------------------------- |
| [Getting Started](website/docs/getting-started.md)                   | Installation, quick start, CLI setup                  |
| [Architecture](website/docs/concepts/architecture.md)                         | System design, components, and data flow              |
| [Configuration](website/docs/concepts/configuration.md)                       | CRD reference, Helm values, controller flags, metrics |
| [Observability](website/docs/guides/observability.md)                        | OpenTelemetry traces, GenAI metrics, and task trace guidance |
| [Agent Runtimes](website/docs/concepts/agent-runtimes.md)                     | ACP v2 RuntimePools, workspace policy, delivery, and external registrations |
| [AgentRuntime Adapter Contract](website/docs/development/agent-runtime-adapter-contract.md) | Portable `orka.harness.v2` session and fencing contract |
| [Agent Sandbox](website/docs/concepts/agent-sandbox.md)                       | Deferred execution-workspace integration behind the ACP v2 lifecycle |
| [Interactive Chat](website/docs/guides/chat.md)                             | Chat endpoint, tools, and SSE streaming               |
| [Multi-Agent Coordination](website/docs/guides/multi-agent-coordination.md) | Coordinator agents and task delegation                |
| [Autonomous Tasks](website/docs/guides/autonomous-tasks.md)                 | Long-running coordinator loops with persisted plan state |
| [Memory](website/docs/concepts/memory.md)                                   | Durable memory, proposals, transcript search, and validation |
| [API Reference](website/docs/reference/api-reference.md)                       | REST API endpoints and usage examples                 |
| [OpenAI Compatibility](website/docs/reference/openai-compat.md)                | OpenAI-compatible chat completions API                |
| [Anthropic Compatibility](website/docs/reference/anthropic-compat.md)          | Anthropic-compatible Messages API                     |
| [Gateway API](website/docs/reference/gateway-api.md)                           | Generic Gateway resources, ingress, delivery, and operator APIs |
| [Harness Modes](website/docs/operations/harness-modes.md)                      | Isolated v1/v2 releases, rollout, rollback, and retirement |
| [Operating Gateways](website/docs/operations/gateways.md)                      | Gateway readiness, TLS, recovery, upgrades, and operations |
| [Web Dashboard](website/docs/guides/ui.md)                                  | Frontend architecture and pages                       |
| [Security](website/docs/concepts/security.md)                                 | Security model and hardening                          |
| [Transaction Tokens](website/docs/concepts/transaction-tokens.md)             | Configure strict transaction governance and TTS |
| [Outbound Access Policies](website/docs/concepts/outbound-access.md)           | Exchange resource credentials or route Tools through a trusted gateway |
| [Repository Security Scanning](website/docs/guides/repository-security-scanning.md) | Repository scan workflow, threat models, findings, and remediation |
| [Repository Monitors](website/docs/guides/repository-monitors.md) | Durable GitHub pull request monitor runs, review tasks, and dashboard state |
| [GitHub Label Triggers](website/docs/guides/github-label-triggers.md) | Trigger Orka agent tasks from GitHub labels such as `agent:implement` and `agent:review` |
| [Development](website/docs/development/development.md)                           | Building, generated charts, releases, and contributing |
| [Testing](website/docs/development/testing.md)                                   | Test structure, patterns, and commands                |
