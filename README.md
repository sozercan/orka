<div align="center">

<img src="website/static/img/orka-logo.png" alt="Orka" width="400" />

# Orka

**Kubernetes-native AI agent orchestration.**

[Getting Started](website/docs/getting-started.md) · [Architecture](website/docs/concepts/architecture.md) · [API Reference](website/docs/reference/api-reference.md) · [Documentation](#documentation)

</div>

---

Orka turns your Kubernetes cluster into an AI task execution platform. You describe work
as a **Task**; Orka runs it in a Pod, records what happened, and returns the result over a
REST API, a CLI, or a built-in web dashboard. The LLM credentials stay in the cluster —
developers get a ServiceAccount token, not an API key.

Three kinds of work run three different ways:

- **`type: ai`** — Orka's own AI worker, in a per-Task Kubernetes Job.
- **`type: container`** — any container command. No model involved.
- **`type: agent`** — a real coding-agent CLI (Codex, Claude Code, GitHub Copilot CLI,
  OpenCode) driven over the [Agent Client Protocol](https://agentclientprotocol.com), an
  open JSON-RPC protocol those CLIs speak over stdin/stdout. Orka keeps them warm in
  pooled, scale-to-zero **RuntimePools** rather than starting a container per request.

A coordinator agent can break a large request into pieces, run specialists in parallel,
and combine their results — you do not write an orchestration graph.

New to the terminology? The [glossary](website/docs/reference/glossary.md) defines ACP,
fences, epochs, fail-closed, and the rest in one place.

> [!IMPORTANT]
> **Orka is experimental and under active development.** APIs, CRDs, and behavior may change without notice between releases, and it is not yet recommended for production use. Feedback, bug reports, and feature ideas are very welcome — please [open an issue](https://github.com/orka-agents/orka/issues).

> [!NOTE]
> The organization and repositories are intended to be donated to a community-governed foundation at the appropriate time. Until then, the project is governed by Microsoft policy, and external contributors are required to sign the Microsoft Contributor License Agreement (CLA).

## Why run AI agents on Kubernetes?

**No API keys on developer machines** — LLM credentials live in Kubernetes Secrets, managed by your platform team. Developers connect via ServiceAccount tokens — no risk of leaked keys in dotfiles, shell history, or laptops.

**Centralized control** — One place to set model policies, rate limits, and allowed providers across every team. Swap models or providers without touching developer configs.

**Every agent action is auditable** — Tasks have durable execution events, Prometheus metrics, structured results, and, for ACP agents, fenced attempt/session and delivery receipts. Know exactly what every agent did, when, and at what cost.

**Hardened execution** — Native workers use hardened per-Task Pods. ACP runtimes use digest-pinned shared Pods with private per-session directories and identities; a RuntimePool is a same-trust-domain boundary, not cross-tenant isolation.

**Scale with your cluster** — Priority scheduling, retry policies, concurrency limits, and cron-based execution — all handled by the Kubernetes control plane you already operate.

## What can you build?

**Parallel code review** — Spawn a swarm of review agents — security, performance, test coverage, accessibility, whatever you need. Each reviews independently and in parallel, then the coordinator synthesizes findings into a single report.

**Autonomous dev workflows** — A coordinator agent dynamically breaks down a feature request, delegates implementation to specialist agents (backend, frontend, tests), and opens a PR with the combined result — no predefined workflow graphs.

**Research with competing hypotheses** — Multiple agents investigate different theories in parallel, challenge each other's findings, and converge on the strongest explanation. The adversarial structure avoids the anchoring bias of sequential investigation.

**Scheduled operations** — Cron-based agents that run daily security scans, dependency audits, or report generation — all with retry policies and webhook notifications.

**Use your favorite AI client** — Connect Continue, Cursor, or any OpenAI-compatible client to Orka's API. Your cluster manages the LLM credentials — developers just code.

**CI/CD integration** — Trigger agent tasks from GitHub Actions, monitor progress via the REST API, and gate deployments on agent analysis.

## Features

- 🤖 **AI Agents** — Anthropic, OpenAI, or Azure OpenAI with tools, skills, and session persistence
- 🛠️ **Coding Agent Runtimes** — Run Codex, Claude Code, GitHub Copilot CLI, and OpenCode over ACP in digest-pinned, scale-to-zero RuntimePools. You can register your own runtime and run the conformance suite; dispatching Tasks to an external v2 `runtimeRef` is not wired up yet and fails closed
- 🔁 **Autonomous Task Loops** — Coordinators can iterate on long-running goals until complete, canceled, or at an iteration limit
- 🔀 **Multi-Agent Coordination** — Coordinators delegate to specialists with depth and concurrency controls
- 💬 **Interactive Chat** — Agentic orchestrator with SSE streaming that creates and manages agents and tasks for you
- 🌐 **Generic Gateways** — Versioned, authenticated ingress and idempotent outbound delivery for external messaging and event systems
- 🧠 **Durable Memory** — Namespace-scoped recall, transcript search, and reviewable memory proposals that can be applied
- 🛡️ **Repository Security Scanning** — Scheduled and incremental repository scans with threat models, validated findings, patch generation, and remediation PRs
- 🔎 **Repository Monitors** — Durable GitHub PR review queues with scheduled and webhook-triggered review runs
- 🧰 **Execution Workspaces** — Give an agent a sandboxed machine instead of a plain Pod, backed by [agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox) or Substrate. Flag-gated and operator-owned: users pick a class by name, operators own the provider
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

## Quick start

### Install

Two versions of Orka exist and they are not the same product yet.

| | Latest release (v0.1.3) | `main` |
| --- | --- | --- |
| Install | Published images, no clone | Build the images yourself |
| `type: ai` and `type: container` Tasks | Yes | Yes |
| Chat, gateways, monitors, security scanning | Yes | Yes |
| `type: agent` coding agents | Yes, via the legacy Job path | Yes, via RuntimePools |
| RuntimePools, harness modes, workspace providers | **No** | Yes |

This README and the docs describe `main`. See
[Release status](website/docs/reference/release-status.md) for the full difference.

**Latest release** — no clone needed:

```bash
# The manifest mounts a harness-wrapper-auth Secret but does not create it,
# so make the namespace and that Secret first or the Pods never start.
kubectl create namespace orka-system
kubectl -n orka-system create secret generic harness-wrapper-auth \
  --from-literal=token="$(openssl rand -hex 32)"

kubectl apply -f https://raw.githubusercontent.com/orka-agents/orka/v0.1.3/deploy/orka.yaml
```

or with Helm:

```bash
helm repo add orka https://orka-agents.github.io/orka/charts
helm install orka orka/orka --namespace orka-system --create-namespace
```

**Current `main`** — images are published only for release tags, so build them and make
them available to your cluster. Follow [Installing from source](website/docs/getting-started.md#option-b-current-main-from-source)
for the matching build, push, and Helm commands, including both native worker images and
the required controller, publisher, and runtime digests.

The guide also covers the namespace label, authenticated
[provider proxy](website/docs/operations/provider-proxy.md), webhook TLS certificate, and
snapshot key required by the chart. If something fails,
[troubleshooting](website/docs/operations/troubleshooting.md) lists the exact error strings.

> [!WARNING]
> Do not install from `charts/orka/` or `deploy/orka.yaml` at the repo root. Those are
> promoted release snapshots, refreshed only during release preparation, and on `main`
> they are behind the source. Use `manifest_staging/charts/orka/`, which `make manifests`
> regenerates from current source.

New installations default to `harness-v2`. Controller mode is an immutable installation
identity and cannot be changed by an upgrade.

For Kustomize instead of Helm, follow the
[`make deploy` prerequisites and image settings](website/docs/operations/provider-proxy.md#enable-it-in-orka).
This guarded path checks the shared CRDs and image pins, provisions the system Secrets,
and applies `config/acp-production`. The overlay carries the cross-namespace ingress policy
that permits model traffic only through Orka's authenticated provider proxy. Its checked-in
image references are placeholders, so applying it directly is not a runnable installation.

> [!IMPORTANT]
> Helm creates a chart's CRDs on install and **never updates them on upgrade**. Apply the
> CRDs from the exact target chart before every controller upgrade, and designate one
> owner for cluster-scoped CRDs. See [Upgrading](website/docs/operations/upgrading.md).

Harness v1 and v2 can share a cluster only as separate static-mode releases with disjoint
namespaces, endpoints, RBAC, Leases, stores, and data planes. Tasks and Sessions never move
between them. See [harness modes](website/docs/operations/harness-modes.md).

### Create an API client

Raw-manifest and Kustomize installs need the
[client ServiceAccount and RBAC setup](website/docs/operations/troubleshooting.md#i-get-403-from-the-api)
before using the dashboard or API. A Helm release named `orka` creates `orka-client`
automatically. Once the account exists, create a client token:

```bash
export ORKA_TOKEN="$(kubectl -n orka-system create token orka-client)"
```

### Set up a provider

```bash
kubectl -n orka-system create secret generic anthropic-secret \
  --from-literal=api-key=your-api-key

kubectl apply -f - <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Provider
metadata:
  name: anthropic
  namespace: orka-system
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

### Start chatting

Forward the API port for the installation you chose. For the release manifest:

```bash
kubectl port-forward -n orka-system svc/orka-api 8080:8080
```

For a Helm release named `orka`, use this instead:

```bash
kubectl port-forward -n orka-system svc/orka 8080:8080
```

Open <http://localhost:8080> and sign in with the client token created above.
You can also connect an OpenAI-compatible client using that token.

The built-in orchestrator creates agents, runs tasks, monitors progress, and returns results — all from natural language. See the [OpenAI Compatibility](website/docs/reference/openai-compat.md) and [Anthropic Compatibility](website/docs/reference/anthropic-compat.md) docs for proxy setup with your preferred client.

## Documentation

|                                                              |                                                       |
| ------------------------------------------------------------ | ----------------------------------------------------- |
| [Getting started](website/docs/getting-started.md)                   | Installation, first task, CLI setup                   |
| [Glossary](website/docs/reference/glossary.md)                       | Every term these docs use, defined once               |
| [Release status](website/docs/reference/release-status.md)           | What is in v0.1.3 versus `main`                       |
| [Troubleshooting](website/docs/operations/troubleshooting.md)        | Error strings, causes, and fixes                      |
| [Upgrading](website/docs/operations/upgrading.md)                    | The CRD step Helm will not do for you                 |
| [Architecture](website/docs/concepts/architecture.md)                         | System design, components, and data flow              |
| [Configuration](website/docs/reference/configuration.md)                       | CRD reference, Helm values, controller flags, metrics |
| [Observability](website/docs/guides/observability.md)                        | OpenTelemetry traces, GenAI metrics, and task trace guidance |
| [Agent Runtimes](website/docs/concepts/agent-runtimes.md)                     | ACP v2 RuntimePools, workspace policy, delivery, and external registrations |
| [AgentRuntime Adapter Contract](website/docs/development/agent-runtime-adapter-contract.md) | Portable `orka.harness.v2` session and fencing contract |
| [Agent Sandbox](website/docs/concepts/agent-sandbox.md)                       | Execution-workspace integration behind the ACP v2 lifecycle |
| [Interactive Chat](website/docs/guides/chat.md)                             | Chat endpoint, tools, and SSE streaming               |
| [Container tasks](website/docs/guides/container-tasks.md)                   | Writable paths, cache directories, and shell gotchas  |
| [Provider proxy](website/docs/operations/provider-proxy.md)                 | Installing Vekil, the proxy every coding agent uses   |
| [Multi-agent coordination](website/docs/reference/multi-agent-coordination.md) | Coordinator agents and task delegation               |
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
