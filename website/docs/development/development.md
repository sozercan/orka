---
slug: /development
description: "Building, running, and regenerating Orka locally."
---

# Development

## Prerequisites

| Tool | Version | Notes |
| --- | --- | --- |
| Go | 1.26.2 or newer | `go.mod` sets `go 1.26.2` and pins `toolchain go1.27.0`, so Go downloads 1.27.0 for you. CI builds on 1.27. |
| Bun | current | Builds the React dashboard, which is embedded into the controller binary. |
| Docker | with BuildKit | The Dockerfiles use BuildKit syntax. Docker Desktop and any modern Docker Engine have it on by default. |
| kubectl | matching your cluster | |
| A Kubernetes cluster | | [kind](https://kind.sigs.k8s.io/) is fine for development. |

## Build commands

For the local run, replace `/path/outside-the-repository` with a private, writable
directory for the persistent database and snapshot key. `RUN_STORE_PATH` overrides
the controller's `/data/orka.db` default.

```bash
# Generate Go types, the installer manifest, and the Helm staging chart
make generate
make manifests

# Build (includes UI)
make build

# Build CLI only
make build-cli

# Run locally with one persistent AES-256 snapshot key
openssl rand 32 > /path/outside-the-repository/orka-snapshot-key
chmod 600 /path/outside-the-repository/orka-snapshot-key
make run RUN_STORE_PATH=/path/outside-the-repository/orka.db \
  RUN_AGENT_EXECUTION_SNAPSHOT_KEY_FILE=/path/outside-the-repository/orka-snapshot-key
```

## Helm chart generation and releases

Orka uses a staged chart flow. The editable Helm generator and static chart inputs live under `cmd/build/helmify/`; canonical Kubernetes resources live under `config/`. Generated and promoted outputs are committed so pull requests and release preparation review the exact manifests that will ship.

| Path | Purpose | Edit directly? |
| --- | --- | --- |
| `cmd/build/helmify/` | Helm generator, Kustomize input, and static chart files | Yes |
| `manifest_staging/deploy/orka.yaml` | Generated next-release installer manifest | No |
| `manifest_staging/charts/orka/` | Generated next-release Helm chart used by CI and upgrade tests | No |
| `deploy/` and `charts/orka/` | Promoted release snapshots | No |

For a normal manifest or chart contribution:

1. Edit `config/` and/or the generator inputs under `cmd/build/helmify/`.
2. Run `make manifests`.
3. Review and commit the source changes together with all changes under `manifest_staging/`.
4. Do not promote the chart in an ordinary feature PR. The root snapshots may intentionally remain at the current release while staging contains the next release.

`make manifests` rebuilds staging from scratch, so direct changes in `manifest_staging/` are clobbered. CI reruns generation and requires a clean diff to detect stale output; run `make manifests` and inspect `git diff` for the same drift check locally.

Release preparation runs the staged release targets:

```bash
make release-manifest NEWVERSION=vX.Y.Z[-beta.N|-rc.N]
make promote-staging-manifest
```

The first target updates release inputs and regenerates staging. The second copies the reviewed staging installer and chart into `deploy/` and `charts/orka/`. Normally `.github/workflows/release-pr.yml` runs both and opens the release-preparation PR. A matching `v*` tag packages and publishes those committed root snapshots; tag workflows do not regenerate or promote manifests.

CRDs are generated into `config/crd/bases/`, while `config/crd/kustomization.yaml` selects the production APIs packaged in the installer and chart. The development-only fake workspace CRDs and RBAC are kept in the separate `config/development/fake-workspace-provider` package. Helm makes production CRDs available on fresh install but does not update them during upgrades. Apply the CRDs from the exact target chart before upgrading the controller — see [Upgrading](../operations/upgrading.md).

## Testing

```bash
# Run test pipeline (manifests, generate, fmt, vet, then Go tests)
make test

# Lint
make lint
make lint-fix

# E2E tests (uses isolated Kind cluster)
make test-e2e
```

See [Testing](testing.md) for full test structure and patterns.

### CI validation

The repository has additional GitHub Actions workflows in addition to the normal test matrix:

- `Live ACP Runtime E2E` — runs on trusted default-branch changes, nightly, or by manual dispatch. It builds the current controller and all four built-in runtime images, bootstraps Kind plus Vekil and the production ACP topology, and executes live Codex, OpenCode, Claude, and Copilot RuntimePools through the canonical smoke validator.
- `Live ACP Release Gate` — is a manual, protected-environment destructive gate that adds result/fork checks, clean-room publication to a distinct fork, PR verification and cleanup, scale-to-zero recovery, and immutable-image assertions.
- `Live Copilot Proxy E2E` — exercises native `type: ai` and compatibility API paths through an external proxy used as test infrastructure. The canonical live ACP workflow separately executes the built-in Codex, OpenCode, Claude, and Copilot RuntimePools end to end.
- `Live Agent Sandbox E2E` — installs the pinned upstream `agent-sandbox` release in Kind, builds the PR controller, the immutable Codex ACP runtime image, and fixture/router images, then validates the direct workspace-adapter lifecycle (claim, exec, cleanup, retained reuse, token scrubbing) **and** a workspace-backed ACP Task end to end: a `Task.spec.execution.workspace` agent Task binds a dedicated `acp-ws-*` RuntimePool whose SandboxClaim hosts the real supervisor, executes a real Codex prompt against the local Responses-compatible fixture, reaches `Succeeded`, keeps Task status provider-neutral, and cleans up. It also runs the class-backed suspend/cold-resume conformance: with the workspace provider API enabled, a session-scoped `classRef` Task suspends its workspace on detach (the exact Sandbox is consensually suspended through `operatingMode: Suspended` while its durable workspace PVC stays Bound and no runtime Pod remains), a continuation Task cold-resumes the same Sandbox, and explicit workspace deletion removes the pool, claim, Sandbox, and PVC. A lifecycle/recovery conformance additionally proves Session continuation with a preserved RuntimeSession UID, explicit cancellation of a Running prompt with bounded controller-owned settlement and no replay, a controller restart during a Running prompt with no prompt replay, and physical runtime replacement that recovers the Session from zero. It requires no external model access.
- `Live GitHub Label Trigger E2E` — builds the PR controller image, deploys it to Kind, configures a generated webhook secret and synthetic runtime Agent, then verifies signed label webhooks create scoped agent Tasks while invalid signatures and duplicate deliveries are handled correctly. This workflow is manual, model-free, and secret-free.
- `Live GitHub OIDC E2E` — builds the PR controller image, deploys it to Kind, authenticates to Orka with a real GitHub Actions OIDC token, and verifies `spec.requestedBy` stamping plus client provenance-tampering rejection.
- `Gateway Live E2E` — runs on relevant pushes and pull requests or by manual dispatch. It creates a fresh Kind cluster, generates disposable TLS and bearer credentials, deploys the TLS reference adapter and deterministic echo `AgentRuntime`, and verifies invalid authentication, accepted and duplicate ingress, runtime-backed Task completion, final delivery, idempotency, and correlation metadata. It is model-free and secret-free and does not use repository or provider credentials.
- `Repository Monitor Smoke` — runs automatically on PRs and pushes touching monitor-relevant Go, CRD/config, worker, or dependency paths. It creates the UI embed stub and runs focused Go tests for monitor store/API/controller behavior, GitHub pull request event queueing, targeted single-PR inventory runs, read-only review task job construction, stdout result forwarding, `create_pr_monitor` repository URL and credential validation, GitHub tool `repo_url` scope enforcement, and PR review marker tooling.
- `Agent Substrate E2E` — builds the PR controller, the immutable Codex ACP runtime image, and Substrate fixture images on a gVisor Kind cluster, validates the direct Substrate actor/router/daemon lifecycle and Substrate-backed MCP Tools, and runs a workspace-backed ACP Task end to end: a `provider: substrate` Task binds an `acp-ws-*` RuntimePool, the controller renders a derived ActorTemplate from the operator infrastructure template, the supervisor boots inside a gVisor Actor, and a real Codex prompt against the local Responses-compatible fixture reaches `Succeeded` through the atenet-router. The class-backed suspend/cold-resume lane is gated off by default (`SUBSTRATE_E2E_SUSPEND_RESUME=0`): the pinned Substrate release prunes the `snapshotsConfig` policy fields and offers no snapshot scope on `SuspendActor`, so the data-only contract cannot be expressed and the controller fails suspend-capable pools closed before booting any actor. Enable the gate only when the Substrate pin provides both the per-template snapshot-scope policy API and a control protocol that atomically binds data-only resume to the verified actor UID/version and immutable Data snapshot UID/version. A lifecycle/recovery conformance additionally proves Session continuation with a preserved RuntimeSession UID, explicit cancellation, controller restart during a Running prompt with no replay, and physical runtime replacement recovering the Session from zero. It requires no external model access; clean-room publication remains live-ACP-release-gate coverage.

Validate workflow/script edits locally before pushing:

```bash
bash -n scripts/live-copilot-proxy-e2e.sh
bash -n scripts/live-acp-runtime-e2e.sh scripts/live-acp-runtime-kind-e2e.sh scripts/lib/live-acp-runtime-kind-bootstrap.sh
bash -n scripts/live-agent-sandbox-e2e.sh
bash -n scripts/live-github-label-trigger-e2e.sh
bash -n scripts/live-github-oidc-e2e.sh
bash -n scripts/agent-substrate-e2e.sh
go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/live-copilot-proxy-e2e.yml
go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/live-acp-runtime-e2e.yml
go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/live-acp-release-gate.yml
go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/live-agent-sandbox-e2e.yml
go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/live-github-label-trigger-e2e.yml
go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/live-github-oidc-e2e.yml
go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/gateway-e2e.yml
go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/repository-monitor-smoke.yml
go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/agent-substrate-e2e.yml
```

The agent-sandbox and Substrate scripts provide the initial workspace-backed ACP v2 happy-path evidence, including fixture-backed prompt completion. They are not the full release gate: external-provider execution, clean-room publication, restart/replacement recovery, and the broader runtime matrix remain covered by the live ACP workflows. Workspace-provider-backed dispatch is still flag-gated behind `--acp-workspace-dispatch-enabled` plus the matching provider flag (`--agent-sandbox-enabled` or `--substrate-enabled`) and fails closed otherwise.

The GitHub OIDC live script requires GitHub Actions `id-token: write` or a manual `ORKA_GITHUB_OIDC_TOKEN`; without either, it fails fast before creating a cluster. Transaction-token provider E2E now lives in the external integration repository.

Run the Agent Substrate E2E locally with:

```bash
PATH="$(go env GOPATH)/bin:$PATH" \
SUBSTRATE_E2E_EXTENDED=1 \
bash scripts/agent-substrate-e2e.sh
```


## Harness wrapper real-world validation

When changing ACP runtime supervision or broker boundaries, validate them against a live cluster, not only unit tests. Use `scripts/live-acp-runtime-e2e.sh` for an already deployed cluster, or `scripts/live-acp-runtime-kind-e2e.sh` to create the same ephemeral Kind/Vekil topology used by CI.

## OpenTelemetry development

Telemetry is enabled with `--enable-telemetry` (or the legacy alias
`--enable-tracing`) and exported through `OTEL_EXPORTER_OTLP_ENDPOINT`. When the
controller flag is enabled and a worker-reachable OTLP endpoint is configured,
AI worker Jobs receive `ORKA_ENABLE_TELEMETRY=true`, `ORKA_TRACEPARENT`, and the
non-secret standard OTLP environment. ACP attempt, RuntimeSession, and
publication spans run in the controller and use its exporter. Managed
RuntimePool workloads do not currently inherit controller OTLP configuration or
expose a supervisor telemetry opt-in. Delegated child Tasks continue the active
parent trace through Task annotations.

GenAI semantic-convention constants live in `internal/tracing/genai` rather than
upstream `semconv` because the GenAI conventions are still Development-stage.
Run focused telemetry tests with:

```bash
go test ./internal/tracing/... ./internal/llm/ ./internal/tools/ ./internal/worker ./workers/ai ./internal/harness/v2/... ./internal/acp/... ./workers/acp/... -run 'Tracing|Telemetry|GenAI|ExecuteTool|TraceContext|Traceparent|TaskRun|RuntimeSession|Fence' -v
```

The live Kind e2e coverage for collector export lives in
`test/e2e/otel_genai_test.go`. It patches the controller with
`--enable-telemetry`, points it at an in-cluster OpenTelemetry Collector, and
asserts that AI worker Jobs export GenAI model/tool spans and metrics.

Run disabled-telemetry hot-path benchmarks with:

```bash
go test ./internal/llm ./internal/tools ./internal/worker -run '^$' -bench 'Telemetry|Tracing|ExecuteTool|ToolExecutor' -benchmem
```

## UI development

```bash
make ui-install         # Install UI dependencies (bun)
make ui-dev             # Run UI dev server
make ui-build           # Build UI and copy to embed directory
make ui-lint            # Lint UI code
make ui-test            # Run UI unit tests
make ui-test-coverage   # Run UI tests with coverage
```

## Docker images

```bash
# Build images
make docker-build                       # Controller image
make docker-build-ai-worker             # Native AI worker
make docker-build-general-worker        # General worker
make docker-build-acp-codex-runtime      # Immutable Codex ACP runtime
make docker-build-acp-claude-runtime     # Immutable Claude ACP runtime
make docker-build-acp-copilot-runtime    # Immutable GitHub Copilot ACP runtime
make docker-build-acp-opencode-runtime    # Immutable OpenCode ACP runtime
make docker-build-workspace-publisher    # Clean-room Workspace/Publisher
make docker-build-all

# Push images
make docker-push
make docker-push-ai-worker
make docker-push-general-worker
make docker-push-acp-codex-runtime
make docker-push-acp-claude-runtime
make docker-push-acp-copilot-runtime
make docker-push-acp-opencode-runtime
make docker-push-workspace-publisher
make docker-push-all
```

## Local development with Kind

```bash
kind create cluster
make docker-build-all
# Push/load the images, then use immutable runtime digests for deployment.
make deploy \
  IMG='<repo>@sha256:<controller-digest>' \
  ACP_CODEX_RUNTIME_IMG='<registry>/acp-codex@sha256:<digest>' \
  ACP_CLAUDE_RUNTIME_IMG='<registry>/acp-claude@sha256:<digest>' \
  ACP_COPILOT_RUNTIME_IMG='<registry>/acp-copilot@sha256:<digest>' \
  ACP_OPENCODE_RUNTIME_IMG='<registry>/acp-opencode@sha256:<digest>' \
  WORKSPACE_PUBLISHER_IMG='<repo>@sha256:<publisher-digest>'
```

### Demo cluster + recordings

For interactive presentations and asciinema recordings of `hack/demos/`,
a one-shot bootstrap is available:

```bash
make demo-cluster-up      # kind cluster + Orka + agent-sandbox
make demo-images          # build + load demo runtime images
hack/demos/00-preflight.sh
# ... run ./hack/demos/10-chat-pr.sh, 20-..., etc.
make demo-cluster-down
```

The scripts pace themselves via `DEMO_RECORD_PROFILE=presenter|docs|social|hero`
and pick a short or long request body via
`DEMO_REQUEST_PRESET=quiet-flag|readme-fix|vekil-metrics`. See
`hack/demos/RECORDING.md` for the full design.

## Generate installer YAML

The installer manifest is generated into `manifest_staging/deploy/orka.yaml` by
the staged manifest flow:

```bash
make manifests
```

See [Helm Chart Generation and Releases](#helm-chart-generation-and-releases)
for how staging output is promoted into `deploy/` at release time.

## Build gotchas

### UI embedding

`make build` embeds the React UI into the controller binary via `//go:embed`. The UI must be built first:

```bash
make ui-build    # Build UI and copy to internal/uiembed/dist/
make build       # Now the Go build will succeed
```

If the UI isn't built, the `ensure-ui-embed` Makefile target creates a stub `internal/uiembed/dist/index.html` so the Go build doesn't fail — but the embedded UI won't work.

### CLI version injection

`make build-cli` injects Git version info via `-ldflags`:

```bash
make build-cli   # Produces bin/orka with embedded version
```

### Metrics disabled by default

The controller's `--metrics-bind-address` defaults to `0` (disabled). Set it explicitly to enable Prometheus metrics:

```
--metrics-bind-address=:8443
```

### HTTP/2 disabled by default

HTTP/2 is disabled for metrics and webhook servers due to CVEs ([GHSA-qppj-fm5r-hxr3](https://github.com/advisories/GHSA-qppj-fm5r-hxr3), [GHSA-4374-p667-p6c8](https://github.com/advisories/GHSA-4374-p667-p6c8)). Use `--enable-http2=true` only if needed.

### Leader election

Leader election ID is hardcoded as `03b49a10.orka.ai`, and its Lease is stored
in the controller's required non-empty watch namespace. Static `harness-v1` and
`harness-v2` installations use different watched namespaces and therefore
different Leases; they do not coordinate ownership of one Task population.
