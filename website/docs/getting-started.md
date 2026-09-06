---
description: "Install Orka on a Kubernetes cluster and run your first agent task."
---

# Getting started

Orka runs AI agents on Kubernetes. You describe work as a **Task**, and Orka runs it in
a Pod, keeps a durable record of what happened, and gives you the result over a REST API,
a CLI, or a built-in web dashboard.

The point is that the API keys stay in the cluster. Developers get a ServiceAccount token,
not an LLM key, and the platform team decides which models and providers are allowed.

## Mental model

Three custom resources cover most of what you will do:

| Resource | What it is |
| --- | --- |
| **Provider** | An LLM backend — Anthropic, OpenAI, or Azure OpenAI — plus the Secret holding its API key. |
| **Agent** | A reusable configuration: which Provider and model to use, a system prompt, which tools it may call. |
| **Task** | One unit of work. This is the thing you create to make something happen. |

A Task points at an Agent; an Agent points at a Provider.

There are three kinds of Task, and the difference matters because they run in different places:

- **`type: ai`** — Orka's own AI worker. It runs in a per-Task Kubernetes Job, calls the
  model, and can use built-in tools like web search and code execution.
- **`type: agent`** — a real coding-agent CLI (Codex, Claude Code, GitHub Copilot CLI, or
  OpenCode) running inside Orka. See [What ACP means](#what-acp-means) below.
- **`type: container`** — an arbitrary container command. No model involved. Useful for
  build and test steps that an agent needs done. See
  [Container tasks](guides/container-tasks.md) for the filesystem rules, which trip
  most people up the first time.

[Architecture](concepts/architecture.md) has the full component picture.

### What ACP means

ACP is the **Agent Client Protocol** — a JSON-RPC protocol that coding-agent CLIs speak
over stdin/stdout, so a program can drive them instead of a human typing at a terminal.
It is an external open protocol, not an Orka invention:
see [agentclientprotocol.com](https://agentclientprotocol.com).

Orka uses it to run agent CLIs as a service. Rather than starting a fresh container per
request, Orka keeps a pool of long-lived agent processes (a **RuntimePool**) and talks to
them over ACP. That is why `type: agent` Tasks start faster than a container would, and
why the docs talk about pools and sessions rather than Jobs.

Terms like *fence*, *epoch*, and *fail closed* show up throughout these docs.
The [Glossary](reference/glossary.md) defines all of them in one place.

## Prerequisites

- A Kubernetes cluster and a `kubectl` that can reach it. For a laptop,
  [kind](https://kind.sigs.k8s.io/) or [minikube](https://minikube.sigs.k8s.io/) is fine.
- OpenSSL for generating the installation credentials and certificates.
- An API key for at least one LLM provider (Anthropic, OpenAI, or Azure OpenAI).

That is all you need for the released install below. Running `type: agent` coding agents
on the newer RuntimePool path needs more — see
[Installing from source](#option-b-current-main-from-source).

For building Orka yourself, see [Development](development/development.md) for the
toolchain versions.

## Install

There are two versions of Orka, and it is worth being clear about which one you are getting.

| | Latest release (v0.1.3) | `main` |
| --- | --- | --- |
| Install | Published images, no clone | Build the images yourself |
| `type: ai` and `type: container` Tasks | Yes | Yes |
| Chat, gateways, repository monitors, security scanning | Yes | Yes |
| `type: agent` coding agents | Yes, via the legacy Job path | Yes, via RuntimePools |
| Harness modes, RuntimePools, workspace providers | **No** | Yes |

Most of these docs describe `main`. v0.1.3 does run `type: agent` Tasks, but through an
older per-Task Job and harness-wrapper path, so any page here that mentions ACP,
RuntimePools, or harness modes does not apply to it. See [Release status](reference/release-status.md) for the full breakdown.

### Option A: latest release

```bash
# The manifest mounts a harness-wrapper-auth Secret but does not create it,
# so make the namespace and that Secret first or the Pods never start.
kubectl create namespace orka-system
kubectl -n orka-system create secret generic harness-wrapper-auth \
  --from-literal=token="$(openssl rand -hex 32)"

kubectl apply -f https://raw.githubusercontent.com/orka-agents/orka/v0.1.3/deploy/orka.yaml
```

That installs the CRDs, RBAC, the controller, and the harness wrapper. Wait for it:

```bash
kubectl -n orka-system rollout status deploy/orka-controller-manager
```

Or with Helm:

```bash
helm repo add orka https://orka-agents.github.io/orka/charts
helm repo update
helm install orka orka/orka --version 0.1.3 \
  --namespace orka-system --create-namespace
```

Check [the tag list](https://github.com/orka-agents/orka/tags) for a newer version before
pinning to v0.1.3. The project publishes tags and chart artifacts; it does not currently
create GitHub Release entries, so the tags are the list to watch.

Then continue with [Give yourself an API client](#give-yourself-an-api-client).

### Option B: current `main`, from source

No container images are published from `main` — the release workflow only runs on `v*`
tags — so this path builds them locally. Use it for coding-agent Tasks that run through
ACP, or for developing Orka itself.

You will need, in addition to the prerequisites above:

- Go, Bun, and Docker — see [Development](development/development.md#prerequisites) for versions
- [Helm](https://helm.sh/docs/intro/install/) for the chart install below
- A **provider proxy**. Built-in coding agents never receive an LLM API key directly;
  all their model traffic goes through an authenticated proxy in front of
  [Vekil](operations/provider-proxy.md). Set that up first — the chart refuses to
  install without it.
- A TLS certificate for the admission webhooks, and a 32-byte encryption key for
  execution snapshots.

```bash
git clone https://github.com/orka-agents/orka.git
cd orka
```

Choose a registry prefix you can push to and your cluster can pull from. Replace
`ghcr.io/your-org/orka` below, authenticate Docker to that registry, and run the build,
push, and install commands in the same shell. Use a fresh tag when rebuilding with local
source changes.

```bash
export ORKA_IMAGE_PREFIX=ghcr.io/your-org/orka
export ORKA_IMAGE_TAG="dev-$(git rev-parse --short=12 HEAD)"
export IMG="${ORKA_IMAGE_PREFIX}:${ORKA_IMAGE_TAG}"
export AI_WORKER_IMG="${ORKA_IMAGE_PREFIX}/ai-worker:${ORKA_IMAGE_TAG}"
export GENERAL_WORKER_IMG="${ORKA_IMAGE_PREFIX}/general-worker:${ORKA_IMAGE_TAG}"
export HARNESS_WRAPPER_IMG="${ORKA_IMAGE_PREFIX}/agent-harness-wrapper:${ORKA_IMAGE_TAG}"
export ACP_CODEX_RUNTIME_IMG="${ORKA_IMAGE_PREFIX}/acp-codex-runtime:${ORKA_IMAGE_TAG}"
export ACP_CLAUDE_RUNTIME_IMG="${ORKA_IMAGE_PREFIX}/acp-claude-runtime:${ORKA_IMAGE_TAG}"
export ACP_COPILOT_RUNTIME_IMG="${ORKA_IMAGE_PREFIX}/acp-copilot-runtime:${ORKA_IMAGE_TAG}"
export ACP_OPENCODE_RUNTIME_IMG="${ORKA_IMAGE_PREFIX}/acp-opencode-runtime:${ORKA_IMAGE_TAG}"
export WORKSPACE_PUBLISHER_IMG="${ORKA_IMAGE_PREFIX}/workspace-publisher:${ORKA_IMAGE_TAG}"

make docker-build-all
make docker-push-all
```

The Helm command below uses these repositories and the same tag for both native workers.
The controller, publisher, and ACP runtimes require registry digests. After pushing, list
their references and replace the corresponding digest placeholders in the Helm command:

```bash
docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' \
  "$IMG" "$WORKSPACE_PUBLISHER_IMG" \
  "$ACP_CODEX_RUNTIME_IMG" "$ACP_CLAUDE_RUNTIME_IMG" \
  "$ACP_COPILOT_RUNTIME_IMG" "$ACP_OPENCODE_RUNTIME_IMG"
```

Claim a namespace for the install. The label is not optional — the controller checks it at
startup and exits if it is missing or does not match:

```bash
kubectl create -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: orka-system
  labels:
    orka.ai/controller-mode: harness-v2
EOF
```

Create the two required Secrets. The snapshot key encrypts stored agent execution records;
keep it somewhere safe, because rotating it makes existing snapshots unreadable:

```bash
kubectl -n orka-system create secret generic orka-agent-snapshot-key \
  --from-literal=key="$(openssl rand -base64 32)"
```

Now the webhook certificate. The chart serves admission on the Service
`orka-webhook.orka-system.svc`, so the certificate has to name exactly that — a
certificate for any other name is rejected by the API server at admission time, not at
install time. For local evaluation a self-signed certificate is fine; use your own CA or
[cert-manager](https://cert-manager.io/) for anything real.

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout /tmp/webhook.key -out /tmp/webhook.crt \
  -subj "/CN=orka-webhook.orka-system.svc" \
  -addext "subjectAltName=DNS:orka-webhook.orka-system.svc,DNS:orka-webhook.orka-system.svc.cluster.local"

kubectl -n orka-system create secret generic orka-webhook-tls \
  --type=kubernetes.io/tls \
  --from-file=tls.crt=/tmp/webhook.crt \
  --from-file=tls.key=/tmp/webhook.key \
  --from-file=ca.crt=/tmp/webhook.crt
```

:::note[Why `ca.crt` is the certificate again]
The certificate is self-signed, so it is its own issuer. The chart reads `ca.crt` to build
the `caBundle` the API server uses to trust the webhook. With a real CA, `ca.crt` is that
CA's certificate instead.
:::

Install the chart. Use `manifest_staging/charts/orka` — that is the chart that matches
`main`. The `charts/orka` directory at the repo root is the snapshot of the last release
and is a generation behind:

```bash
WEBHOOK_CA_BUNDLE="$(kubectl -n orka-system get secret orka-webhook-tls -o jsonpath='{.data.ca\.crt}')"

helm install orka ./manifest_staging/charts/orka \
  --namespace orka-system \
  --set controller.mode=harness-v2 \
  --set controller.watchNamespace=orka-system \
  --set controller.image.repository="${ORKA_IMAGE_PREFIX}" \
  --set controller.image.digest="sha256:<controller-digest>" \
  --set workers.ai.image.repository="${ORKA_IMAGE_PREFIX}/ai-worker" \
  --set-string workers.ai.image.tag="${ORKA_IMAGE_TAG}" \
  --set workers.general.image.repository="${ORKA_IMAGE_PREFIX}/general-worker" \
  --set-string workers.general.image.tag="${ORKA_IMAGE_TAG}" \
  --set publisher.image.repository="${ORKA_IMAGE_PREFIX}/workspace-publisher" \
  --set publisher.image.digest="sha256:<publisher-digest>" \
  --set controller.acpRuntime.codexImage="${ORKA_IMAGE_PREFIX}/acp-codex-runtime@sha256:<codex-digest>" \
  --set controller.acpRuntime.claudeImage="${ORKA_IMAGE_PREFIX}/acp-claude-runtime@sha256:<claude-digest>" \
  --set controller.acpRuntime.copilotImage="${ORKA_IMAGE_PREFIX}/acp-copilot-runtime@sha256:<copilot-digest>" \
  --set controller.acpRuntime.opencodeImage="${ORKA_IMAGE_PREFIX}/acp-opencode-runtime@sha256:<opencode-digest>" \
  --set-string controller.agentExecutionSnapshot.existingSecret=orka-agent-snapshot-key \
  --set-string controller.agentExecutionSnapshot.key=key \
  --set-string webhooks.tls.existingSecret=orka-webhook-tls \
  --set-string webhooks.caBundle="${WEBHOOK_CA_BUNDLE}" \
  --set providerProxy.enabled=true
```

You can leave out the four `acpRuntime` image lines. Any runtime you do not configure is
simply unavailable, and Tasks that ask for it fail with a clear error rather than falling
back to something else.

If Helm refuses to render, that is deliberate — the chart checks its inputs up front
rather than installing something broken. [Troubleshooting](operations/troubleshooting.md)
lists the guards and what each one wants.

:::info[Kustomize instead of Helm]
Install the shared CRDs from `config/crd` through your cluster's designated CRD owner
before deploying workloads. The `config/acp-production` workload overlay excludes CRDs;
it adds the network policy that stops model traffic from bypassing the provider proxy.
`make deploy` checks the shared CRD prerequisite and creates the artifact, publisher,
and proxy Secrets before applying that overlay. Its controller, publisher, and runtime
image variables must use the pushed `repository@sha256:...` references.
:::

### Two installs on one cluster

Controller mode is fixed for the life of an install and cannot be changed by upgrading.
To run the older `harness-v1` contract alongside `harness-v2`, install it as a separate
release in a separate namespace. Tasks never move between them.
See [Harness modes](operations/harness-modes.md).

### Upgrades

Helm does not update CRDs on `helm upgrade` — that is a Helm behavior, not an Orka one.
Apply the CRDs from the target chart yourself first, every time.
[Upgrading](operations/upgrading.md) has the procedure.

## Give yourself an API client

The REST API authenticates with Kubernetes ServiceAccount tokens. A Helm release named
`orka` creates an `orka-client` ServiceAccount with the right permissions. For a raw-manifest
or Kustomize install, first [create the client ServiceAccount and its RBAC roles](operations/troubleshooting.md#i-get-403-from-the-api),
then continue here.

Forward the API port. For Option A's release manifest:

```bash
kubectl port-forward -n orka-system svc/orka-api 8080:8080
```

For a Helm release named `orka`, use this instead:

```bash
kubectl port-forward -n orka-system svc/orka 8080:8080
```

In another terminal, create a client token:

```bash
export ORKA_TOKEN="$(kubectl -n orka-system create token orka-client)"
```

:::warning[Namespace matters]
Almost every command on this page needs `-n orka-system`. Orka watches exactly one
namespace, and resources created elsewhere are silently ignored — no error, they just
never run. `kubectl create token orka-client` fails the same way without it.
:::

## Your first task

### 1. Create a Provider

```bash
kubectl -n orka-system create secret generic anthropic-secret \
  --from-literal=api-key=your-api-key

kubectl apply -f - <<'EOF'
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

### 2. Create an Agent

```bash
kubectl apply -f - <<'EOF'
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: assistant
  namespace: orka-system
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

```bash
kubectl apply -f - <<'EOF'
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: hello-task
  namespace: orka-system
spec:
  type: ai
  agentRef:
    name: assistant
  prompt: "What is Kubernetes?"
EOF
```

### 4. Read the result

```bash
kubectl -n orka-system get task hello-task

curl -H "Authorization: Bearer ${ORKA_TOKEN}" \
  http://localhost:8080/api/v1/tasks/hello-task/result
```

If the Task never leaves `Pending`, see
[Troubleshooting](operations/troubleshooting.md#my-task-stays-pending).

### 5. Collect artifacts

Files a Task writes to its artifact directory are retrievable once it finishes.
The sample `hello-task` asks for a text answer, so an empty artifact list is expected:

```bash
curl -H "Authorization: Bearer ${ORKA_TOKEN}" \
  http://localhost:8080/api/v1/tasks/hello-task/artifacts
```

For the optional CLI commands, use an Orka source checkout with the
[Go toolchain](development/development.md#prerequisites). For a Task that writes artifacts,
replace `<task-name>` with its name and `<artifact-name>` with a filename returned by
`task artifacts`:

```bash
make build-cli
./bin/orka --server http://localhost:8080 --token "$ORKA_TOKEN" -n orka-system \
  task artifacts '<task-name>'
./bin/orka --server http://localhost:8080 --token "$ORKA_TOKEN" -n orka-system \
  task download '<task-name>' '<artifact-name>'
```

## Running a coding agent

This section needs an [Option B](#option-b-current-main-from-source) install.

A `type: agent` Task runs a real coding-agent CLI against a git repository. Orka clones the
repo, hands the agent a working copy, and records everything it does.

### 1. Check the provider proxy is up

Built-in agent runtimes never see a provider Secret. They get a token for Orka's proxy and
a specific model they are allowed to use; the real API key stays with
[Vekil](operations/provider-proxy.md). Confirm the `provider-auth-proxy` Deployment is
Ready before submitting agent Tasks.

### 2. Create an Agent with a runtime

```bash
kubectl apply -f - <<'EOF'
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: claude-agent
  namespace: orka-system
spec:
  model:
    name: claude-sonnet-4-20250514
  runtime:
    type: claude
    contractVersion: orka.harness.v2
    defaultMaxTurns: 50
    defaultAllowBash: true
    defaultAllowedTools: [Read, Write, Edit, Bash]
EOF
```

Two runtime-specific notes:

- **Codex** needs `defaultAllowBash: true`. The upstream Codex CLI has no reliable way to
  disable its shell, so Orka fails fast rather than pretending the restriction holds.
- **OpenCode** uses `provider/model` names such as `openai/gpt-5.4`, and requires
  `model.contextWindow` and `model.maxTokens`. Orka pins both into the pool so compaction
  limits do not shift when a catalog updates.

### 3. Run it

```bash
kubectl apply -f - <<'EOF'
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: code-review
  namespace: orka-system
spec:
  type: agent
  agentRef:
    name: claude-agent
  prompt: "Review this repo for security issues. Do not modify files."
  workspace:
    intent: read
    gitRepo: "https://github.com/example/repo.git"
    branch: main
    # For a private repo. Only the clean-room publisher resolves this Secret —
    # the agent process never sees the credential.
    # readCredentialRef:
    #   name: repository-read
  agentRuntime:
    maxTurns: 20
EOF
```

### 4. Watch it

```bash
kubectl -n orka-system get task code-review
kubectl -n orka-system get runtimepools

make build-cli
./bin/orka --server http://localhost:8080 --token "$ORKA_TOKEN" -n orka-system \
  task status code-review
```

[Agent runtimes](concepts/agent-runtimes.md) has the full configuration reference.

## Stronger isolation

If your cluster has `RuntimeClass` objects such as `gvisor` or `kata-qemu`, `ai` and
`container` Tasks can run through them via `spec.execution`. Set a default on the Agent and
override per Task. Coding-agent Tasks use RuntimePool resource profiles instead.
See [Configuration](reference/configuration.md#execution) and
[Security](concepts/security.md#execution-workloads).

## The dashboard

```bash
# Helm names the Service after the release (svc/orka); the release
# manifest from Option A names it svc/orka-api. Pick the one you installed.
kubectl port-forward -n orka-system svc/orka 8080:8080
open http://localhost:8080
```

The UI ships inside the controller binary — there is nothing extra to deploy.
See [Web dashboard](guides/ui.md).

## The CLI

```bash
make build-cli
./bin/orka login                                  # reads your kubeconfig, opens a browser
./bin/orka login --server https://orka.example.com
./bin/orka login --token '<token>'
```

It can pull a token from a bearer token, a token file, exec-based auth (GKE, AWS IAM), or
an OIDC provider. Full command list: [CLI reference](reference/cli.md).

## Next steps

**Learn the pieces**

- [Glossary](reference/glossary.md) — every term these docs assume
- [Architecture](concepts/architecture.md) — how a Task becomes a Pod
- [Configuration](reference/configuration.md) — Helm values and controller flags
- [Security](concepts/security.md) — hardening, auth, and tenancy

**Do something with it**

- [Interactive chat](guides/chat.md) — talk to an orchestrator that creates Tasks for you
- [Container tasks](guides/container-tasks.md) — build and test steps that actually work
- [Multi-agent coordination](reference/multi-agent-coordination.md) — one agent delegating to several
- [Repository monitors](guides/repository-monitors.md) — automatic PR review queues
- [Scheduled tasks](guides/scheduled-tasks.md) — cron-driven agents

**Connect your own tools**

- [OpenAI-compatible API](reference/openai-compat.md) — Continue, Cursor, and similar
- [Anthropic-compatible API](reference/anthropic-compat.md) — Claude Code and similar
- [REST API](reference/api-reference.md)

**When it breaks**

- [Troubleshooting](operations/troubleshooting.md)
- [Operations runbook](operations/runbook.md)
