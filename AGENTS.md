# AGENTS.md

Orka is a Kubernetes-native task execution platform that manages Jobs and Pods for container tasks and AI agent tasks.

## Non-negotiables

- **Never commit, log, or print credentials** — API keys, tokens, or secrets of any kind. Use Kubernetes Secrets or env vars.
- **No binaries in the repository** — put build artifacts in `bin/` (gitignored) or CI release pipelines.
- **Scope discipline** — implement exactly what was asked, nothing more.
- **Never push to `main`.** Push to the current branch after a change when it is not `main`.
- **Transaction-token integration is fail-closed** — never store raw TxTokens in Task specs/status/logs. Use owner-referenced Secrets for child tokens, safe metadata/digests for audit, subset checks for child scopes, and fail-closed TTS exchanges for outbound scopes.
- **Never edit generated files** (below), and never delete `// +kubebuilder:scaffold:*` comments.

## Generated — do not edit

For non-trivial code changes, run `$autoreview` (`.agents/skills/autoreview/SKILL.md`) before final/commit/ship and keep going until there are no accepted/actionable findings, unless the change is trivial/docs-only, equivalent manual review already happened, or the human opts out.

- Treat review output as advisory: verify every finding against the real code path before changing code.
- If review-triggered fixes change code, rerun focused tests and rerun `$autoreview`.
- Format before review when formatting can move line locations; focused tests and review may run in parallel only after formatting is stable.

## PR Closeout

After creating or updating an agent-authored PR, use `$pr-closeout` (`.agents/skills/pr-closeout/SKILL.md`) by default, like `$autoreview` is used before landing. Resolve merge conflicts, fix failing CI, address or push back on unresolved review threads, reply on GitHub and resolve addressed comments, push the non-main PR branch, and repeat until current CI is green and no unresolved actionable review threads remain. Skip only when the human opts out, the PR is intentionally draft/WIP, or the remaining blocker is external/human-only. Do not merge or enable auto-merge unless explicitly asked.

## Build & Test

```bash
make manifests          # Regenerate CRDs (after editing *_types.go or markers)
make generate           # Regenerate Go types
make build              # Build (includes UI)
make test               # Run tests
make lint-fix           # Lint and fix
make docker-build-all   # Controller, AI/general workers, ACP runtimes, publisher
make deploy IMG=<repo>@sha256:<digest> ACP_CODEX_RUNTIME_IMG=<repo>@sha256:<digest> ACP_CLAUDE_RUNTIME_IMG=<repo>@sha256:<digest> ACP_COPILOT_RUNTIME_IMG=<repo>@sha256:<digest> ACP_OPENCODE_RUNTIME_IMG=<repo>@sha256:<digest> WORKSPACE_PUBLISHER_IMG=<repo>@sha256:<digest>
```

UI: `cd ui && bun install && bun run dev` (dev server on :5173). See @website/docs/development/development.md for full commands.

For testing against a local Kubernetes cluster, use the `$kindctl` skill to manage repo/worktree-scoped kind clusters without touching the global kubeconfig.

To stand up a reverse proxy for Anthropic/Gemini/OpenAI-compatible clients, use the `$vekil-reverse-proxy-deploy` skill. When it falls back to GitHub Copilot device-code login, surface the login code and URL to the user and wait for their confirmation before continuing — never complete the login on their behalf.

To stand up an execution-workspace provider on a local kind cluster for evaluation, use the `$agent-sandbox-deploy` skill (kubernetes-sigs agent-sandbox; pairs with `$kindctl` for the cluster and `$orka-kind-deploy` for the controller) or the `$agent-substrate-deploy` skill (Agent Substrate; owns its own gVisor kind cluster, so it is not hosted on a `$kindctl` cluster). Both are local/kind eval only — Orka does not install or manage these providers in production — and both surface the `$vekil-reverse-proxy-deploy` device-code login to the user for confirmation rather than completing it.

## Verification

Run after every change:

```bash
make manifests generate          # After *_types.go or marker edits
make lint-fix && make test       # After any *.go edits
cd ui && bun run lint && bun run test  # After UI edits
bash -n scripts/*.sh                  # After shell script edits
go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/<workflow>.yml  # After workflow edits
```

Single test: `go test ./internal/api/ -run TestHandlerName -v`

## Auto-Generated — Do NOT Edit

- `config/crd/bases/*.yaml`, `config/rbac/role.yaml` — `make manifests`
- `manifest_staging/deploy/orka.yaml`, `manifest_staging/charts/orka/**` — `make manifests`
- `deploy/**`, `charts/orka/**` — `make promote-staging-manifest` (release-preparation only)
- `**/zz_generated.*.go` — `make generate`
- `PROJECT` — kubebuilder CLI
- `ui/src/routeTree.gen.ts` — TanStack Router

Do NOT delete `// +kubebuilder:scaffold:*` comments.

## Code Style

- Structured logging: `log := log.FromContext(ctx); log.Info("msg", "key", val)`
- LLM tool args for nested objects arrive as `map[string]any`, not strings — always type-switch
- Put model-readable tool constraints in JSON Schema (`maximum`, `minimum`, `enum`, `default`), then validate and enforce them again in `Execute`; schema is guidance, not a runtime trust boundary
- Memory features are governance-first: `remember` and `propose_memory` create review proposals, not durable memories
- Kontxt integration is fail-closed: never store raw TxTokens in Task specs/status/logs; use owner-referenced Secrets for child tokens, safe metadata/digests for audit, subset checks for child scopes, and fail-closed TTS exchanges for outbound scopes.

## Gotchas

- Worker filesystem is read-only except `/tmp`, `/home/worker`, and `/workspace`
- `make build` requires UI assets — run `make ui-build` first (or `ensure-ui-embed` creates a stub)
- AI worker truncates messages on context overflow — keeps system prompt + newest, drops middle atomically with structured metadata
- `code_exec` timeout is clamped to 60s — a larger caller value becomes 60s, not an error. 30s is the default only when the caller supplies none
- Built-in AI worker tools: `web_search`, `code_exec`, `file_read`, `web_fetch`, `file_write`, `request_approval`. Only the first five are proxied through the OpenAI/Anthropic compatibility endpoints (`builtinProxyTools`)
- Built-in agent runtimes (`codex`, `claude`, `copilot`, `opencode`) use only the `orka.harness.v2` ACP RuntimePool path; there is no per-Task Job or legacy fallback.
- `Task.spec.execution.workspace` is flag-gated behind `--acp-workspace-dispatch-enabled` plus the provider flag: `agent-sandbox` (`--agent-sandbox-enabled`, `templateRef` forbidden — SandboxClaim hosts the supervisor) or `substrate` (`--substrate-enabled`, infrastructure `templateRef` required — a gVisor Actor hosts it via a controller-derived ActorTemplate; live actors are never suspended, and teardown calls `SuspendActor` only after workload memory is proven absent). Each binds a dedicated single-session `acp-ws-*` RuntimePool; `retain`, boot/pool/snapshot/hibernation, legacy-path `onDetach`, and harness-v1 requests fail closed, and provider-native identifiers never enter Task status (ADRs 0024/0025).
- `Task.spec.execution.workspace.classRef` (with `--enable-workspace-provider-api` plus the dispatch/provider flags) binds the same ACP path to the controller-first `ExecutionWorkspaceClass` lifecycle: reserved adapter `controllerName acp.workspace.orka.ai/runtime-pool`, `RuntimeProviderConfig`/`RuntimeWorkspaceProfile` parameter kinds, frozen class identity in the execution snapshot, and epoch-fenced Task attachment. `Delete` is always executable; `Suspend` is executable only for session-reused classes whose profile permits `DataOnly` suspension: substrate via `substrate.suspend.mode` (derived template renders a DurableDir volume with explicit `Data`/`Data`/`ColdBoot` snapshot policy) or agent-sandbox via `agentSandbox.suspend` (the claim requests one durable workspace PVC — forcing a cold start — and suspension patches the exact Sandbox to `operatingMode: Suspended`). Resume rotates bootstrap material, refreshes the derived template (and Sandbox blueprint) in place, and cold-boots against the preserved data. Retained deletion policies and pooled classes fail closed until retention lands (ADRs 0026/0027/0028).
- `Task.spec.workspace` is the only agent repository surface. Keep clone/read credentials in `readCredentialRef` and publication/forge credentials in `publicationCredentialRef`; neither enters the ACP process tree.
- RuntimePools are controller-owned, digest-pinned, scale-to-zero resources. Only `Serving` + `Accepting` admits new RuntimeSessions; drain/finalization must complete before replacement or scale-down.
- Safe v2 probes are `GET /v2/health` and `GET /v2/capabilities`; status and all mutations require controller authentication plus operation-scoped authorization and exact fences.
- External `runtimeRef` registrations are v2-only. Registration/conformance exists, but external Task dispatch currently fails closed until its v2 dispatcher is wired.
- ACP runtime Pods run the supervisor as root with narrowly added process/identity capabilities; ACP children use distinct non-reused UIDs/GIDs, private session trees, and no Git credentials.
- Coordination memory tools: `recall_memory`, `remember`, `propose_memory`, `search_transcript`
- Do not store secrets, credentials, tokens, raw transcripts, or one-off task status in durable memory
- Reviewing a memory proposal does not apply it; use the explicit proposal apply endpoint for accepted `memory` proposals when durable memory should be created
- Kontxt TxTokens are accepted via `Txn-Token` by default; `Authorization: Bearer` context-token support is opt-in so ServiceAccount/OIDC auth can coexist
- Live GitHub OIDC/kontxt E2E requires GitHub Actions `id-token: write` or `ORKA_GITHUB_OIDC_TOKEN`; redact JWTs, TxTokens, and request tokens in logs
- OpenTelemetry GenAI constants are hand-rolled in `internal/tracing/genai`; telemetry is enabled with `--enable-telemetry`/`--enable-tracing`, workers honor `ORKA_ENABLE_TELEMETRY`, and prompt/completion content capture remains default-off/fail-closed
- ACP real-world validation should include Codex, Claude, and OpenCode through Vekil, Copilot image/profile admission (plus live execution when provider auth is available), workspace clone/read, Session continuation, cancellation/timeout, unsafe workspace rejection, controller restart, pool replacement, clean-room branch publication, PR reconciliation, and cleanup.
