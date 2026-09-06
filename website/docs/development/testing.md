---
slug: /testing
description: "The test suites Orka runs, what each one covers, and how to run them."
---

# Testing

Orka has four kinds of tests: Go unit tests, controller integration tests against a real
API server (envtest), end-to-end tests against a throwaway Kind cluster, and frontend tests
in the browser-like Vitest environment. This page describes what each covers and how to run
it.

## Running tests

```bash
# Run test pipeline (manifests, generate, fmt, vet, then Go tests)
make test

# Run Go tests with coverage report
make test
go tool cover -func=cover.out | grep total

# Run frontend tests
make ui-test                # or: cd ui && bun run test
make ui-test-coverage       # or: cd ui && bun run test:coverage

# Run E2E tests (requires isolated Kind cluster)
make test-e2e

# Run only the deterministic Gateway live E2E
KIND_CLUSTER=orka-gateway-e2e \
E2E_GATEWAY=true \
E2E_EPHEMERAL_CLUSTER=true \
E2E_GINKGO_FOCUS="Gateway live E2E" \
make test-e2e

# Run Agent Substrate E2E (requires Docker, Go, git, curl, kind, kubectl, ko, jq)
SUBSTRATE_E2E_EXTENDED=1 bash scripts/agent-substrate-e2e.sh

# Lint
make lint
make lint-fix
make ui-lint
```

### Local environment notes

- **Script test suites need bash >= 4.** The suites under `scripts/tests/`
  rely on `set -e` stopping on failed `(( ))` arithmetic, which macOS's stock
  bash 3.2 does not honor — failures would pass silently. The suites refuse to
  run under bash < 4; on macOS install a modern bash (`brew install bash`) and
  invoke the suites with it.
- **A green Gateway E2E can be an empty one.** Without `E2E_GATEWAY=true` the
  gateway specs skip themselves and Ginkgo prints `Ran 0 of N Specs` with a
  `SUCCESS` exit. If you see that line, nothing was validated — re-run with
  the environment shown above. (CI fails this shape explicitly.)
- **Running `go test` directly on controller packages needs envtest assets.**
  `make test` wires `KUBEBUILDER_ASSETS` automatically; for a bare `go test`
  on packages that start an envtest API server, export it first:
  `KUBEBUILDER_ASSETS="$(bin/setup-envtest use -p path)"`.

## Test structure

### Go tests

Tests use **Ginkgo + Gomega** (BDD style) for controller/integration tests and standard Go `testing` for unit tests.

| Package | Test Files | Coverage Areas |
|---------|-----------|----------------|
| `internal/api/` | `handlers_test.go`, `internal_handlers_test.go`, `auth_test.go`, `middleware_test.go`, `pagination_test.go`, `server_test.go`, `openai_compat_test.go` | REST API handlers, internal API handlers, memory/session APIs, authentication, middleware, pagination, OpenAI compatibility |
| `internal/controller/` | `task_controller_test.go`, `agent_controller_test.go`, `tool_controller_test.go`, `session_manager_test.go`, `job_builder_test.go`, `repositoryscan_controller_test.go`, `webhook_test.go` | Reconciliation logic, session management, job building, coordination enforcement, repository scan mapper/finding/patch ingestion |
| `internal/security/` | `security_test.go`, `contracts_test.go` | Repository security artifact contracts, v2 evidence validation, fingerprinting, bounded context manifests, prompt helpers |
| `internal/security/slices/` | `mapper_test.go` | Deterministic review-slice mapper coverage for Go, Node/TypeScript, Python, workflows, scripts, config, path skipping, and stable output |
| `internal/store/sqlite/` | `security_store_test.go` | Repository security store migrations, findings, review slices, dropped finding diagnostics, patch proposals |
| `internal/llm/` | `provider_test.go` | Provider registry |
| `internal/llm/anthropic/` | `provider_test.go` | Anthropic API integration |
| `internal/llm/openai/` | `provider_test.go` | OpenAI API integration |
| `internal/metrics/` | `metrics_test.go` | Prometheus metrics recording |
| `internal/tools/` | `registry_test.go`, memory tool tests, coordination tool tests, PR tool tests, agent-management tool tests, `integration_test.go` | Built-in tool implementations, memory tools, coordination tools, PR tools, agent management tools |
| `internal/worker/` | `tool_executor_test.go` | Custom Tool CRD executor |
| `workers/ai/` | `main_test.go` | AI worker functions |
| `workers/general/` | `main_test.go` | General worker functions |
| `internal/harness/v2/`, `internal/acp/`, `workers/acp/` | ACP contract, client, supervisor, and conformance tests | RuntimeSession lifecycle, exact fences, duplicate handling, event bounds, cancellation, workspace deltas, process cleanup, and redaction |

### E2E tests

End-to-end tests run against a dedicated Kind cluster:

| Test File | Coverage |
|-----------|----------|
| `test/e2e/e2e_test.go` | Core task lifecycle |
| `test/e2e/agent_test.go` | Agent task execution |
| `test/e2e/agent_copilot_test.go` | Copilot built-in profile admission plus exact digest-pinned RuntimePool image selection, without requiring live provider authentication |
| `test/e2e/agent_claude_test.go` | Claude runtime |
| `test/e2e/agent_workspace_test.go` | Workspace/git clone |
| `test/e2e/agent_session_test.go` | Session continuity |
| `test/e2e/autonomous_mode_test.go` | Autonomous iterations, max-iteration stop, Plan API, suspend behavior |
| `test/e2e/coordination_advanced_test.go` | `cancel_task`, inter-task messaging, auto-retry, dynamic agent create/delete |
| `test/e2e/pr_workflow_test.go` | PR tool workflow (`create_pull_request`, review/comment/merge) and workspace PR env wiring |
| `test/e2e/api_coverage_test.go` | Sessions, agent update API, single-tool API, auth validation, secrets API, chat delete, non-autonomous plan 404 |
| `test/e2e/chat_advanced_test.go` | JSON chat mode, `agentRef` chat routing, management tools via chat |
| `test/e2e/security_enforcement_test.go` | Non-root execution, read-only filesystem, deny-pattern enforcement, kube-system chat block |
| `test/e2e/agent_advanced_test.go` | Skills ConfigMap wiring, agent resource propagation, session maxMessages behavior |
| `test/e2e/workspace_advanced_test.go` | Workspace source/ref/subPath, separate read/publication credential roles, delivery status, and Session behavior |
| `test/e2e/provider_advanced_test.go` | Provider rate-limit config coverage |
| `test/e2e/live_copilot_proxy_test.go` | Native `type: ai` Provider compatibility against the separately deployed copilot-proxy service; this is separate from built-in Copilot ACP RuntimePool coverage |
| `test/e2e/live_chat_api_test.go` | Live chat SSE and JSON transport/session coverage using a proxy-backed Provider |
| `test/e2e/live_anthropic_compat_test.go` | Live Anthropic-compatible `/anthropic/v1/models` and `/anthropic/v1/messages` coverage with default tools-enabled behavior |
| `test/e2e/live_agent_runtime_matrix_test.go` | Historical live Codex/Claude provider execution plus a digest-pinned Copilot image smoke assertion; it is not the canonical ACP release gate |
| `test/e2e/gateway_test.go` | Authenticated Gateway ingress through a deterministic external `AgentRuntime`, including TLS adapter readiness, invalid bearer rejection, accepted and duplicate events, Task execution, completed events, delivered replies, idempotency, and Task/delivery correlation |
| `.github/workflows/gateway-e2e.yml` | Focused, model-free, secret-free Gateway live E2E in Kind using generated bearer tokens, an ephemeral CA, the TLS reference adapter, and the deterministic echo runtime |
| `.github/workflows/live-agent-sandbox-e2e.yml` / `scripts/live-agent-sandbox-e2e.sh` | Live upstream `agent-sandbox` Kind validation for Orka agent workspace claim, sandbox execution, delete cleanup, retained-session reuse, and token scrubbing using a fake model-free Claude runtime |
| `.github/workflows/live-github-label-trigger-e2e.yml` / `scripts/live-github-label-trigger-e2e.sh` | Manual model-free GitHub label trigger validation for HMAC rejection, signed webhook Task creation, scoped workspace settings, and duplicate delivery idempotency |
| `.github/workflows/repository-monitor-smoke.yml` | Focused RepositoryMonitor smoke coverage for store CRUD, API handlers, pull request event handling, targeted single-PR inventory runs, controller queue/review flow, blocked status counts, read-only review task job building, result stdout forwarding, `create_pr_monitor` repository URL and credential validation, GitHub tool `repo_url` scope enforcement, and PR review marker tooling |
| `.github/workflows/security-scan-e2e.yml` / `scripts/security-scan-e2e.sh` | Secret-free repository security scan Kind validation against pinned `sozercan/nodejs-goof` using the real mapper, deterministic fake Codex analyzer, v2 finding ingestion/drop diagnostics, threat-model rejection, idempotent rescan, and HITL no-auto-patch gating |
| `test/e2e/tools_test.go` | Built-in tools (including `web_fetch`, `file_write`) and custom Tool CRD |
| `test/e2e/scheduled_task_test.go` | Cron scheduling, suspend, `concurrencyPolicy: Forbid`, history-limit cleanup |
| `test/e2e/task_lifecycle_test.go` | Timeout/retry/cancel plus session serialization and lock release |
| `scripts/live-acp-runtime-e2e.sh` | Canonical deployed-cluster ACP smoke/release gate for Codex, OpenCode, Claude, and Copilot RuntimePools, exact Pod/runtime identity, workspace read/write, continuation/fork, cancellation/timeout, restart/replacement, publication/PR verification, drain/scale-to-zero, immutable images, and cleanup |
| `.github/workflows/live-acp-runtime-e2e.yml` / `scripts/live-acp-runtime-kind-e2e.sh` | Trusted-branch/nightly/manual live ACP smoke that bootstraps an ephemeral Kind cluster, Vekil, and the production ACP topology before invoking the canonical validator |
| `.github/workflows/live-acp-release-gate.yml` / `scripts/live-acp-runtime-kind-e2e.sh` | Manual, protected-environment release acceptance with destructive publication, independent GitHub verification, PR reconciliation, and cleanup |

The Gateway Live E2E workflow (`.github/workflows/gateway-e2e.yml`) runs on manual dispatch and on pull requests or pushes that touch Gateway-relevant source, configuration, E2E, image, or dependency paths. It creates a dedicated Kind cluster, generates disposable TLS and bearer credentials, deploys the TLS reference adapter and deterministic echo `AgentRuntime`, and verifies invalid bearer rejection, accepted and duplicate ingress, runtime-backed Task completion, final delivery, idempotency, and correlation metadata. The workflow is model-free and secret-free; it does not use repository or provider credentials.

The Repository Monitor Smoke workflow runs in GitHub Actions on pull requests and pushes that touch the workflow, API, controller, CRD/config, worker, or Go dependency paths. It creates the UI embed stub and runs focused `go test` selections for the monitor store, API handlers, GitHub pull request event handling, targeted single-PR inventory runs, controller queue/review flow, blocked status counts, read-only review job construction, result stdout forwarding, `create_pr_monitor` repository URL and credential validation, GitHub tool `repo_url` scope enforcement, and PR review marker signing/detection tooling. The workflow is secret-free: exact PR event queueing is tested with synthetic signed webhook payloads and fake GitHub clients rather than live repository credentials. The normal Go Tests workflow runs `make test` for non-doc code changes and covers worker-level PR review diff context generation.

Repository security E2E coverage should include initial deterministic slice creation,
incremental scan behavior, invalid v2 evidence being dropped and visible through API,
validation task persistence, successful verified patch proposals, and patch proposals with
missing or mismatched artifacts staying not ready.

### E2E key requirements

- `scripts/live-acp-runtime-e2e.sh --context <context>` is the canonical ACP
  deployed-cluster validator. Its default mode is a smoke test; set
  `RELEASE_GATE=1` for destructive release acceptance. A smoke result explicitly
  reports the publication, remote-verification, Task result/fork, and
  scale-to-zero scenarios that remain release-only.
- `scripts/live-acp-runtime-kind-e2e.sh` is the CI/local bootstrap entrypoint. It
  creates an ephemeral Kind cluster, deploys Vekil and the production ACP
  topology with digest-pinned local images, and then calls the canonical script.
- `.github/workflows/live-acp-runtime-e2e.yml` runs on relevant pushes to the
  default branch, nightly, and manual dispatch. It uses the
  `live-acp-runtime-smoke` environment and requires its
  `COPILOT_GITHUB_TOKEN` secret so Vekil can exercise Codex, OpenCode, Claude, and Copilot
  without mounting provider credentials into RuntimePools. Restrict that
  environment to the default branch; do not require reviewers if scheduled runs
  must proceed unattended.
- `.github/workflows/live-acp-release-gate.yml` is manual-only and serialized.
  Protect the `live-acp-release-gate` environment with required reviewers and a
  default-branch deployment rule. It accepts explicit source/fork URLs, a full
  source SHA that must equal the dispatched workflow commit and default-branch
  head, and the default branch as the PR base. It requires these environment
  secrets:
  `COPILOT_GITHUB_TOKEN`, `ACP_E2E_WRITE_READ_CREDENTIAL_TOKEN`,
  `ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_TOKEN`,
  `ACP_E2E_WRITE_CREDENTIAL_TOKEN`, and
  `ACP_E2E_WRITE_FORGE_CREDENTIAL_TOKEN`. Configure the four publication
  credentials as distinct, least-privilege GitHub credentials for source read,
  target read, target write, and forge/verification cleanup respectively.
- Neither live ACP workflow runs for `pull_request`, so PR-controlled code never
  receives provider or publication credentials. Both check out with persisted
  credentials disabled and expose secrets only to the final local script step.
  They intentionally keep `permissions` at `contents: read` and do not request
  `id-token: write`; GitHub OIDC validation remains isolated in
  `live-github-oidc-e2e.yml`.
- The release gate additionally requires digest-pinned controller, Publisher,
  Codex, OpenCode, Claude, and Copilot images; a Ready central provider proxy; the
  `config/acp-production` Vekil ingress boundary; durable controller/Publisher
  PVCs; a distinct publication fork; and authenticated `gh` access for
  independent remote and PR verification/cleanup.
- Canonical ACP acceptance runs all Codex scenarios, including publication,
  before deleting the Codex pool and starting OpenCode. It validates OpenCode
  native ACP read, continuation, and read-intent tool policy, deletes the
  OpenCode pool, starts Claude, and then runs live Copilot read/continuation
  after Claude cleanup. Every provider phase verifies exact Pod UID, image ID,
  runtime instance/profile, and RuntimeSession identity. Codex additionally
  covers cancellation and timeout settlement, controller restart, pool
  replacement, drain/scale-to-zero, and publication/remote delivery; release
  mode also exercises Task forks.
- `E2E_OPENAI_API_KEY` and `E2E_ANTHROPIC_API_KEY` remain inputs for older native
  `type: ai` test cases. They are not mounted into built-in ACP RuntimePools.
- `COPILOT_GITHUB_TOKEN` also remains the credential for
  `live-copilot-proxy-e2e.yml`, which covers the external proxy as native
  Provider test infrastructure. The canonical live ACP workflow is the
  provider-execution evidence for the built-in RuntimePool profiles.
- Structural e2e tests for native worker Jobs run without external model keys.
- `Live Agent Sandbox E2E` and `Agent Substrate E2E` do run workspace-backed ACP Tasks
  end to end against a local model fixture, but they are not the full release gate:
  external-provider execution and clean-room publication stay with the live ACP workflows.
  The Substrate suspend/resume lane is off by default (`SUBSTRATE_E2E_SUSPEND_RESUME=0`)
  because the pinned Substrate release cannot express the data-only snapshot contract.
- Security Scan E2E is secret-free and model-free, but requires Docker plus the
  local Go, Kind, kubectl, curl, and jq toolchain.

- The live GitHub label trigger workflow is manual, model-free, and secret-free. It requires Docker, Kind, kubectl, curl, jq, and Python locally, accepts `GITHUB_LABEL_TRIGGER_TARGET_REPO_URL` and `GITHUB_LABEL_TRIGGER_TARGET_NUMBER` overrides, and sends only synthetic webhook payloads to the local Orka API.
- Gateway Live E2E is model-free and secret-free. Its focused invocation sets `E2E_GATEWAY=true` and `E2E_EPHEMERAL_CLUSTER=true`; the last flag skips per-resource suite cleanup because the caller deletes the entire Kind cluster.
- GitHub Actions `id-token: write` permission: required by the live GitHub OIDC workflow. For local/manual runs of `scripts/live-github-oidc-e2e.sh`, set `ORKA_GITHUB_OIDC_TOKEN` to a valid JWT instead. Provider-specific transaction-token E2E lives in the external integration repositories.
- `E2E_LIVE_COPILOT_PROXY_BASE_URL` (or `E2E_COPILOT_PROXY_BASE_URL` / `COPILOT_PROXY_BASE_URL`): enables the focused live copilot-proxy spec against a running proxy
- `E2E_LIVE_COPILOT_PROXY_SERVICE_NAMESPACE`, `E2E_LIVE_COPILOT_PROXY_SERVICE_NAME`, `E2E_LIVE_COPILOT_PROXY_SERVICE_PORT`: direct-access proxy coordinates used by the legacy Provider, Chat, and Anthropic compatibility specs.
- `E2E_LIVE_ACP_PROVIDER_PROXY_SERVICE_NAMESPACE`, `E2E_LIVE_ACP_PROVIDER_PROXY_SERVICE_NAME`, `E2E_LIVE_ACP_PROVIDER_PROXY_SERVICE_PORT`: model-discovery coordinates for the ACP RuntimePool matrix. The full live script pins these to `vekil-system`, `vekil`, and `1337` so built-in RuntimePools traverse the production provider-proxy DNS and NetworkPolicy boundary.


Run the trusted smoke bootstrap locally with the token exported in the shell
rather than placed on a command line:

```bash
read -rsp 'Copilot provider token: ' COPILOT_GITHUB_TOKEN && echo
export COPILOT_GITHUB_TOKEN
export ACP_E2E_OPENCODE_MODEL=openai/gpt-5.4
export ACP_E2E_OPENCODE_CONTEXT_WINDOW=32768
export ACP_E2E_OPENCODE_MAX_TOKENS=4096
ACP_E2E_KIND_TAG=local bash scripts/live-acp-runtime-kind-e2e.sh
```

For a manual release gate, also export the four role-specific credential values,
set `ACP_E2E_WRITE_SOURCE_REPO`, `ACP_E2E_WRITE_PUBLICATION_REPO`,
`ACP_E2E_WRITE_SOURCE_REF`, and `ACP_E2E_WRITE_PR_BASE`, then run:

```bash
export RELEASE_GATE=1
export ACP_E2E_WRITE_CREATE_PR=1
export ACP_E2E_WRITE_SOURCE_REPO=https://github.com/orka-agents/orka.git
export ACP_E2E_WRITE_PUBLICATION_REPO=https://github.com/OWNER/orka.git
export ACP_E2E_WRITE_SOURCE_REF="$(git rev-parse HEAD)"
export ACP_E2E_WRITE_PR_BASE=main
read -rsp 'Source-read token: ' ACP_E2E_WRITE_READ_CREDENTIAL_TOKEN && echo
read -rsp 'Target-read token: ' ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_TOKEN && echo
read -rsp 'Target-write token: ' ACP_E2E_WRITE_CREDENTIAL_TOKEN && echo
read -rsp 'Forge token: ' ACP_E2E_WRITE_FORGE_CREDENTIAL_TOKEN && echo
export ACP_E2E_WRITE_READ_CREDENTIAL_TOKEN ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_TOKEN
export ACP_E2E_WRITE_CREDENTIAL_TOKEN ACP_E2E_WRITE_FORGE_CREDENTIAL_TOKEN
export GH_TOKEN="${ACP_E2E_WRITE_FORGE_CREDENTIAL_TOKEN}"
bash scripts/live-acp-runtime-kind-e2e.sh
```

In release mode the Kind wrapper binds `ACP_E2E_REPO` and `ACP_E2E_REF` to the
write source repository and SHA, and rejects explicitly supplied read values
that differ. The read/runtime phases and publication phase therefore validate
the same immutable source.

Do not enable shell xtrace for either invocation. The scripts create Kubernetes
Secrets without printing their values and redact provider/GitHub token patterns
from failure diagnostics.


The live agent-sandbox workflow validates both the direct workspace-adapter
lifecycle and the initial workspace-backed ACP v2 happy path. It builds the
real Codex supervisor, routes a prompt through a local Responses-compatible
fixture, waits for the Task to succeed, verifies provider-neutral status, and
cleans up the dedicated RuntimePool. It does not replace the broader live ACP
release gate or provide publication evidence. Its direct-adapter assertions
also include:

- the adapter creates a v1beta1 `SandboxClaim` with the expected `warmPoolRef` and executes a command with caller-supplied env inside the sandbox
- `cleanupPolicy: delete` removes the generated `SandboxClaim`
- `cleanupPolicy: retain` plus `reusePolicy: session` reattaches to the deterministic session claim
- retained workspace state persists across tasks

The live GitHub label trigger workflow (`.github/workflows/live-github-label-trigger-e2e.yml`) runs `scripts/live-github-label-trigger-e2e.sh` from manual `workflow_dispatch`. It builds the controller from the PR, deploys it to a fresh Kind cluster, configures a generated `ORKA_GITHUB_WEBHOOK_SECRET`, creates a synthetic runtime Agent, and posts a signed `agent:implement` issue label payload to `/webhooks/github`. The script asserts:

- invalid webhook signatures return `401`
- a signed label event returns `201` and creates a `type: agent` Task
- the created Task points at the configured GitHub repository clone URL and default branch
- no push branch or git credential Secret is configured for the synthetic task
- GitHub delivery annotations are recorded on the Task
- a repeated delivery returns `202` with the original task name

The live GitHub OIDC workflow (`.github/workflows/live-github-oidc-e2e.yml`) runs `scripts/live-github-oidc-e2e.sh` in GitHub Actions with `id-token: write`. It builds the controller from the PR, deploys it to a fresh Kind cluster, configures the GitHub OIDC issuer and workflow audience, fetches a real Actions OIDC token, and validates:

- unauthenticated API requests return `401`
- OIDC-authenticated Task creation returns `201`
- the created Task contains verified `spec.requestedBy` provenance
- top-level `requestedBy` and nested `spec.requestedBy` client tampering are rejected with `400`
- the OIDC token does not appear in controller logs

The Agent Substrate workflow (`.github/workflows/agent-substrate-e2e.yml`) is secret-free and runs `scripts/agent-substrate-e2e.sh` against a fresh Kind cluster. It pins the Substrate checkout with `SUBSTRATE_REF`, verifies and applies the reviewed patches in `hack/agent-substrate/`, initializes the local RustFS snapshot bucket, builds the local Orka controller and archived workspace-provider images, then validates:

- the injected upstream unit tests for authorization redaction and bounded, fail-closed `runsc delete` recovery
- direct Substrate Actor create/resume/router/daemon exec/suspend/delete
- live proof that `atenet-router` logs an explicit redaction marker without either bootstrap or handoff bearer credentials
- worker-Pod deletion, store removal, Deployment replacement, lost-Actor settlement, and successful direct routing on the replacement fleet
- repeated checkpoint/delete cycles with no Actor left in `STATUS_SUSPENDING`
- Orka `SubstrateActorPool` reconciliation and density reporting
- MCP Actor-backed `Tool` execution through a pooled Substrate Actor
- MCP Actor reuse across forced Tool reconciles without rebooting an already booted Actor
- pool scale-down plus Tool, lease, bound Actor, and precreated Actor cleanup

The workflow also validates a successful workspace-backed ACP Task by booting
the real Codex supervisor in a gVisor Actor, routing a prompt through the local
Responses-compatible fixture, waiting for `Succeeded`, checking provider-
neutral status, and cleaning up the pool. Broader runtime coverage and
clean-room publication remain responsibilities of the live ACP workflows.

The patches are source-blob pinned and fail closed when `SUBSTRATE_REF` changes or a patch touches an undeclared path. See `hack/agent-substrate/README.md` for the patch contracts and review procedure. Run the fast static checks with:

```bash
bash scripts/tests/agent-substrate-patches-test.sh
```

Run the full destructive Kind validation locally with:

```bash
PATH="$(go env GOPATH)/bin:$PATH" \
SUBSTRATE_E2E_EXTENDED=1 \
KEEP_CLUSTER=1 \
bash scripts/agent-substrate-e2e.sh
```

### Frontend tests

Frontend tests use **Vitest + Testing Library + MSW**.

```bash
cd ui && bun run test           # what CI runs
cd ui && bun run test:coverage  # adds the coverage report and threshold check
```

:::warning[The UI coverage thresholds are a local target, not a merge gate]
`ui/vite.config.ts` declares thresholds of 95% statements, 80% branches, 90% functions, and
95% lines. Those only apply to `bun run test:coverage`. The `ui-test` job in
`.github/workflows/test.yml` runs plain `bun run test`, so a pull request that drops coverage
still passes CI. Run the coverage command yourself before assuming a number.
:::

## Testing patterns

### Table-driven tests

```go
tests := []struct {
    name    string
    input   string
    want    string
    wantErr bool
}{
    {"valid", "input", "output", false},
    {"invalid", "bad", "", true},
}
for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
        // test logic
    })
}
```

### Fake Kubernetes client

```go
scheme := runtime.NewScheme()
corev1alpha1.AddToScheme(scheme)
corev1.AddToScheme(scheme)
client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
```

### HTTP mocking

```go
server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    w.Write([]byte(`{"result": "ok"}`))
}))
defer server.Close()
```

### Fiber test app

```go
app := fiber.New()
app.Get("/test", handler)
req := httptest.NewRequest(http.MethodGet, "/test", nil)
resp, _ := app.Test(req)
```

### Frontend test mocking

```typescript
// Mock zustand persist middleware
vi.mock('zustand/middleware', () => ({ persist: (fn: unknown) => fn }))

// Use test utils with QueryClient wrapper
import { render } from '@/test/test-utils'
```

## Testing with chat

When testing features via the chat endpoint, use **natural prompts** — the kind a human would actually type. Never reference internal concepts like agent names, tool names, or implementation details. Describe what you want done, not how the system should do it. The chat should infer the right agents, tools, delegation patterns, and cancellation logic on its own.

Good examples:
- "Research the benefits of Kubernetes and write a technical guide based on the findings."
- "What's the best container orchestration tool? Get me an answer as fast as possible."
- "Draft an outline for a blog post about containers and turn it into a full post."
- "Compare microservices vs monoliths from three angles, then synthesize into a recommendation."

Bad examples:
- "Create a coordinator agent and a researcher agent, then delegate two tasks..."
- "Use the send_message tool to send a message to task msg-receiver..."
- "Have three researchers race to answer..." (users don't think in terms of "researchers")
- "Use the first answer and cancel the others." (the system should infer this automatically)
