# Demo Magic Scenarios

This directory contains a small `demo-magic` kit for showing Orka in six ways:

- `10-chat-pr.sh`: Claude Code -> Orka's Anthropic-compatible API -> prepared coordinator -> coder/reviewer/CI loops -> PR
- `20-manual-workflow.sh`: explicit coordinator Task CR for a focused Vekil metrics first-PR workflow
- `30-cron-workflow.sh`: scheduled runtime task with recurring child runs
- `40-security-scanning.sh`: repository scan -> findings -> patch -> PR
- `60-agent-sandbox.sh`: archived execution-workspace prototype; not a current ACP v2 path
- `70-agent-substrate.sh`: archived Substrate prototype; requires an Actor-backed v2 supervisor before it is supported again

There is also:

- `00-preflight.sh`: quick readiness check before the live demo
- `reset.sh`: cleanup helper for the named demo resources

## Prerequisites

- A running Orka controller reachable at `ORKA_API_BASE`
- `kubectl`, `curl`, `jq`, and `claude` (Claude Code) on your local PATH
- Optional: an upstream `demo-magic.sh` if you want to override the vendored fallback
- Demo resources live in the controller's watched namespace (`orka-system` for `make deploy` and the Helm chart): the static harness-v2 controller reconciles exactly one namespace, so `DEMO_NAMESPACE` must equal `--watch-namespace`
- A Provider CRD and runtime credential Secret in that namespace
- Separate read/clone and publication/forge credential Secrets for any ACP workspace demo. The archived 60/70 scripts still assume one broad Git Secret and must be migrated before use.

The demo scripts include a lightweight `demo-magic.sh` fallback at `hack/demos/lib/demo-magic.sh`, so no separate checkout is required. If you prefer the upstream `demo-magic` behavior, set `DEMO_MAGIC_PATH` to your local checkout. If `DEMO_MAGIC_PATH` points at a missing file, the scripts ignore it and use the vendored fallback.

## Required Environment

Set these before running the scenarios:

```bash
# Optional override for upstream demo-magic. Usually unnecessary because a fallback is vendored.
# export DEMO_MAGIC_PATH="$HOME/src/demo-magic/demo-magic.sh"
export ORKA_API_BASE="http://127.0.0.1:8080"
export DEMO_NAMESPACE="orka-system"   # must be the controller's --watch-namespace

# Must match metadata.name from: kubectl get provider -n "$DEMO_NAMESPACE"
export DEMO_PROVIDER_REF="<existing-provider-name>"
export DEMO_AI_MODEL="claude-opus-4.8"

export DEMO_RUNTIME_TYPE="codex"
export DEMO_RUNTIME_MODEL="gpt-5.5"
export DEMO_RUNTIME_SECRET_REF="<runtime-secret-name>"

export DEMO_GIT_REPO="https://github.com/sozercan/vekil.git"
export DEMO_GIT_BRANCH="main"
export DEMO_GIT_SECRET_REF="<git-secret-name>"
```

`DEMO_PROVIDER_REF` is an exact Kubernetes resource name, not a display label. The preflight checks `providers.core.orka.ai/$DEMO_PROVIDER_REF` in `$DEMO_NAMESPACE`; if the name does not exist, the scripts now print the visible Providers and non-secret setup hints. The name `copilot-proxy-openai` is only valid after you create a Provider with that exact `metadata.name`.

For the current shared demo cluster, the immediate fix is usually:

```bash
kubectl get provider -n "$DEMO_NAMESPACE"
export DEMO_PROVIDER_REF="copilot"
export DEMO_RUNTIME_SECRET_REF="codex-runtime-copilot"
export DEMO_GIT_SECRET_REF="github-credentials"
```

If you want to keep `DEMO_PROVIDER_REF="copilot-proxy-openai"`, create an alias Provider in `$DEMO_NAMESPACE` instead:

```bash
cat <<'YAML' | kubectl apply -f -
apiVersion: core.orka.ai/v1alpha1
kind: Provider
metadata:
  name: copilot-proxy-openai
  namespace: orka-system   # $DEMO_NAMESPACE
spec:
  type: openai
  secretRef:
    name: copilot-provider-key
    key: api-key
  baseURL: https://api.githubcopilot.com
  defaultModel: gpt-5.5
YAML
```

## One-time Namespace, Provider, and Secret Setup

These commands show resource shape only. Replace placeholder values locally and do not commit, echo, or paste real tokens into logs.

Confirm the namespace exists and carries the controller's static-mode label (`make deploy` creates it; creating a different, unlabeled namespace here would leave every demo Task unreconciled):

```bash
kubectl get namespace "$DEMO_NAMESPACE" -o jsonpath='{.metadata.labels.orka\.ai/controller-mode}'; echo
```

Create the Provider's API-key Secret and Provider CR. `spec.secretRef.name` and `spec.secretRef.key` must point at the Secret/key you create here. `baseURL` is optional for the built-in provider types, but useful for OpenAI-compatible proxies.

```bash
export DEMO_PROVIDER_REF="copilot-proxy-openai"
export DEMO_PROVIDER_TYPE="openai"              # openai, anthropic, or azure-openai
export DEMO_PROVIDER_SECRET_REF="provider-api-key"
export DEMO_PROVIDER_SECRET_KEY="api-key"
# Optional, for OpenAI-compatible proxies:
# export DEMO_PROVIDER_BASE_URL="http://<openai-compatible-proxy>/v1"

kubectl -n "$DEMO_NAMESPACE" create secret generic "$DEMO_PROVIDER_SECRET_REF" \
  --from-literal="$DEMO_PROVIDER_SECRET_KEY=<provider-api-key-or-proxy-placeholder>" \
  --dry-run=client -o yaml | kubectl apply -f -

cat <<YAML | kubectl apply -f -
apiVersion: core.orka.ai/v1alpha1
kind: Provider
metadata:
  name: ${DEMO_PROVIDER_REF}
  namespace: ${DEMO_NAMESPACE}
spec:
  type: ${DEMO_PROVIDER_TYPE}
  secretRef:
    name: ${DEMO_PROVIDER_SECRET_REF}
    key: ${DEMO_PROVIDER_SECRET_KEY}
  # Uncomment for an OpenAI-compatible proxy:
  # baseURL: ${DEMO_PROVIDER_BASE_URL:-http://<openai-compatible-proxy>/v1}
  defaultModel: ${DEMO_AI_MODEL}
YAML
```

Create the provider/proxy credential Secret for the ACP runtime. Keep this role separate from both Git read and publication credentials. Codex accepts `OPENAI_API_KEY` or `CODEX_API_KEY`; Copilot accepts `GITHUB_TOKEN`; Claude accepts `ANTHROPIC_API_KEY`.

```bash
# Codex example:
kubectl -n "$DEMO_NAMESPACE" create secret generic "$DEMO_RUNTIME_SECRET_REF" \
  --from-literal=OPENAI_API_KEY='<codex-or-openai-token>' \
  --dry-run=client -o yaml | kubectl apply -f -

# Copilot runtime example:
# kubectl -n "$DEMO_NAMESPACE" create secret generic "$DEMO_RUNTIME_SECRET_REF" \
#   --from-literal=GITHUB_TOKEN='<github-token>' \
#   --dry-run=client -o yaml | kubectl apply -f -
```

For current ACP manifests, create separate repository credentials. The clean-room workspace boundary uses the read Secret; the Workspace/Publisher alone uses the publication Secret:

```bash
kubectl -n "$DEMO_NAMESPACE" create secret generic repository-read \
  --from-literal=token='<read-only-repository-token>' \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$DEMO_NAMESPACE" create secret generic repository-publish \
  --from-literal=token='<branch-and-pr-publication-token>' \
  --dry-run=client -o yaml | kubectl apply -f -
```

If you use the default local API URL, the scripts auto-start a port-forward to `svc/orka-api` when needed and write its PID/log under `DEMO_WORKDIR`. To manage it yourself instead, set `DEMO_AUTO_PORT_FORWARD=0` and keep this running in another terminal:

```bash
kubectl -n orka-system port-forward svc/orka-api 8080:8080
```

Claude Code chat defaults:

```bash
export DEMO_CHAT_CLIENT="claude-code"
export DEMO_CLAUDE_BIN="claude"
# Optional override. By default the scripts compute provider/model, e.g. copilot-proxy-openai/gpt-5.5.
# export DEMO_CLAUDE_MODEL="copilot-proxy-openai/gpt-5.5"
```

Optional tuning:

```bash
export DEMO_GIT_FORK_REPO="https://github.com/your-org/your-demo-fork.git"
export DEMO_PR_BASE_BRANCH="main"
export DEMO_GIT_SUB_PATH="services/api"

# export DEMO_CHAT_REQUEST="..."
# export DEMO_MANUAL_REQUEST="..."
# Or put the live request in a file. DEMO_REQUEST_FILE feeds both chat and manual demos;
# DEMO_CHAT_REQUEST_FILE and DEMO_MANUAL_REQUEST_FILE override one path only.
# export DEMO_REQUEST_FILE="/tmp/live-demo-request.txt"
# The suite defaults to a short “implement GitHub issue #77” request so agents discover requirements live.
# Override the request variables or request files to prove the workflow is not tied to the default story.
# export DEMO_VEKIL_METRICS_SLICE_REQUEST="..."
# export DEMO_CRON_REQUEST="..."
export DEMO_CRON_SCHEDULE="*/1 * * * *"
export DEMO_SECURITY_SCAN_NAME="demo-security-repository"
export DEMO_SECURITY_SCHEDULE="0 */6 * * *"
export DEMO_AUTO_PORT_FORWARD="1"
export DEMO_AGENT_MEMORY_REQUEST="512Mi"
export DEMO_AGENT_MEMORY_LIMIT="2Gi"
export DEMO_PR_WORKFLOW_TIMEOUT="300m"
export DEMO_VALIDATION_REPAIR_LIMIT="6"
export DEMO_REVIEW_REPAIR_LIMIT="8"
export DEMO_CI_REPAIR_LIMIT="3"

# Claude Code knobs used by 10-chat-pr.sh.
export DEMO_CLAUDE_PERMISSION_MODE="dontAsk"
export DEMO_CLAUDE_OUTPUT_FORMAT="json"
export DEMO_CLAUDE_SETTING_SOURCES=""
export DEMO_CLAUDE_TOOLS=""
export DEMO_SHOW_CHAT_CLIENT_RESULT="0"
# Transparency defaults are on: print the exact rendered prompt/manifest before execution.
export DEMO_SHOW_FULL_PROMPT="1"
export DEMO_SHOW_FULL_MANIFEST="1"
```

## Live request / transparency mode

The demos are still scripted for repeatability, but the request is intentionally overridable:

```bash
cat > /tmp/live-demo-request.txt <<'EOF'
Implement a small, reviewable repository change. Include tests or documentation when appropriate, and keep the PR focused.
EOF

export DEMO_GIT_REPO="https://github.com/your-org/your-repo.git"
export DEMO_GIT_BRANCH="main"
export DEMO_GIT_FORK_REPO="https://github.com/your-org/your-demo-fork.git"
export DEMO_REQUEST_FILE="/tmp/live-demo-request.txt"
export DEMO_SHOW_FULL_PROMPT="1"
export DEMO_SHOW_FULL_MANIFEST="1"

hack/demos/10-chat-pr.sh
# or
hack/demos/20-manual-workflow.sh
```

Transparency mode prints the exact rendered prompt or Task manifest before the run. That makes the demo auditable: the final child Tasks and PR should correspond to the visible input rather than a hidden canned result.

## Suggested Run Order

```bash
hack/demos/reset.sh
hack/demos/00-preflight.sh
hack/demos/10-chat-pr.sh
hack/demos/20-manual-workflow.sh
hack/demos/30-cron-workflow.sh
hack/demos/40-security-scanning.sh
hack/demos/60-agent-sandbox.sh    # requires hack/demos/cluster/install-agent-sandbox.sh
```

Demo 70 (Agent Substrate) runs on its **own** kind cluster, not the shared
`orka-demo` cluster (Substrate needs a custom registry + gVisor node config):

```bash
make demo-substrate-up                                # stand up the dedicated cluster
kubectl config use-context kind-orka-agent-substrate-e2e
DEMO_SUBSTRATE_NAMESPACE=default ./hack/demos/70-agent-substrate.sh
make demo-substrate-down                              # tear it down
```

### One cluster for everything (`demo-cluster-up-all`)

Because the Substrate cluster is the superset (custom registry + gVisor nodes),
a single bootstrap can host the current demos (00–40 and 60–70) on it:

```bash
make demo-cluster-up-all        # substrate cluster + Orka + agent-sandbox + vekil + Provider/secrets
                                # (one-time GitHub device-code login for vekil — follow the log prompt)
```

> **Re-running is guarded.** If a kind cluster named `orka-agent-substrate-e2e`
> already exists, the bootstrap prompts **reuse / recreate / cancel** instead of
> silently destroying it (the Substrate standup does `kind delete` + rebuild,
> which would wipe a completed vekil login). Choose **reuse** to keep the cluster
> and just reconcile the add-ons. For non-interactive runs, set
> `DEMO_CLUSTER_REUSE=reuse|recreate|cancel` to skip the prompt; with no TTY and
> no override it defaults to `recreate` (the historical behavior).

```bash
# Workspace demos bring their own namespace/env:
kubectl config use-context kind-orka-agent-substrate-e2e

# Demo 60 (agent-sandbox): the bootstrap installs the SandboxTemplate
# (orka-live-template) and the sandbox-model-key Secret into the controller's
# watched namespace (orka-system by default), so the demo MUST run there —
# any other DEMO_NAMESPACE leaves the Tasks unreconciled.
DEMO_NAMESPACE=orka-system DEMO_RUNTIME_TYPE=codex DEMO_RUNTIME_MODEL=gpt-5.5 \
  DEMO_RUNTIME_SECRET_REF=sandbox-model-key DEMO_GIT_SECRET_REF=github-credentials \
  DEMO_SANDBOX_TEMPLATE_REF=orka-live-template ./hack/demos/60-agent-sandbox.sh

# Demo 70 (substrate): sets provider: substrate explicitly; runs in `default`.
./hack/demos/70-agent-substrate.sh

# Model-backed SDLC demos share one env file (points at the in-cluster vekil + secrets):
source hack/demos/cluster/demo-env.sh
./hack/demos/20-manual-workflow.sh
./hack/demos/30-cron-workflow.sh
./hack/demos/40-security-scanning.sh
./hack/demos/10-chat-pr.sh        # also needs the `claude` CLI on PATH

make demo-cluster-up-all-down     # tear it all down
```

Notes: the agent-sandbox/Substrate bootstrap and demos are retained only for
prototype archaeology. Current ACP v2 validation must not set a default execution
workspace provider or rely on a per-Task worker path. kontxt's `enforce` mode only
gates requests carrying a `Txn-Token`, so the other demos remain independent.

Known flake (Demo 70): the warm-reuse Task occasionally fails during workspace
release with a gVisor `RestoreWorkload: ... eth0: Link not found` daemon error
*after* the agent's work and PR have already landed. This is Substrate runtime
nondeterminism on `runsc`, not an Orka bug — the demo's story (cold run opens a
real PR, warm run reattaches with `reused=true`) still completes. Re-run if the
final card matters for a recording.

## Recording

The demo scripts are recording-ready: they pace themselves via the
`DEMO_RECORD_PROFILE` env var rather than a wrapper. To capture an
asciicast, point asciinema at the script directly:

```bash
asciinema rec --idle-time-limit 1.5 --cols 110 --rows 30 \
  -c "DEMO_RECORD_PROFILE=docs ./hack/demos/10-chat-pr.sh" \
  /tmp/10.cast
```

### `DEMO_RECORD_PROFILE` (default `presenter`)

| Profile     | Typewriter | Chapters       | Best for                                  |
|-------------|------------|----------------|-------------------------------------------|
| `presenter` | on         | all + audit JSON | live audience, full transparency        |
| `docs`      | off        | all + narration cues | embedded GIFs / docs              |
| `social`    | off        | 1–3 only       | short clips, social media                 |
| `hero`      | off        | suppressed     | ≤60s hero loops, minimal text             |

### `DEMO_REQUEST_PRESET` (default `quiet-flag`)

Selects the chat / manual request body so recordings finish quickly:

| Preset                | What it asks for                                          |
|-----------------------|-----------------------------------------------------------|
| `quiet-flag`          | Add a `--quiet` flag to vekil + a test (short, default)   |
| `readme-fix`          | Fix one broken link in the README                         |
| `vekil-metrics`       | Full `/metrics` endpoint implementation (long-form story) |
| `vekil-metrics-slice` | Focused regression fix on the metrics path                |

An explicit `DEMO_CHAT_REQUEST` / `DEMO_MANUAL_REQUEST` env var or
`*_REQUEST_FILE` always wins over the preset.

### Bootstrapping a demo cluster

For Demo 60 (and a clean rerun of 10–40) you can spin up a dedicated kind
cluster and load the sandbox runtime image:

```bash
make demo-cluster-up
make demo-images
# ... run demos ...
make demo-cluster-down
```

See `RECORDING.md` for the full design (visual style §5, helper API §5.5,
per-script tightening §6, new-scenario storyboards §7, work order §11.5).

## Presenter Flow

Each demo uses the same rhythm:

- `Brief`: one sentence about the scenario and outcome.
- `Show`: the object or command the audience should notice.
- `Run`: the action that changes state.
- `Follow`: waits for Orka to finish the real work.
- `Inspect`: Kubernetes/API views of what happened.
- `Summary`: compact JSON of the final state.

The scenarios are:

- `00-preflight.sh`: prove the namespace, controller, API tunnel, provider, Secrets, model surface, and local client are ready.
- `10-chat-pr.sh`: a live or default repo-change request starts as a Claude Code chat request and ends as a coordinator Task, specialist child Tasks, validation/review/CI repair if needed, and PR result.
- `20-manual-workflow.sh`: the same kind of live or default repo-change workflow runs from declarative Kubernetes YAML.
- `30-cron-workflow.sh`: a scheduled parent Task triages stale GitHub PRs every tick and produces a paste-ready markdown report — the same Task primitive as demos 10/20 with a `schedule:` field added.
- `40-security-scanning.sh`: an open finding becomes a patch proposal and human-reviewable PR.

## How the Claude Code Scenario Works

`10-chat-pr.sh` intentionally uses Claude Code only as a local Anthropic client. The script applies the prepared coordinator/coder/reviewer Agents, renders the exact operational prompt to `${DEMO_WORKDIR}/chat-request.txt`, prints it by default for transparency, sends it to Orka, and saves the raw Claude Code JSON response to `${DEMO_WORKDIR}/chat-client-result.json` for debugging. Set `DEMO_SHOW_FULL_PROMPT=0` if you want the presenter view to show only the story and file path.

Use a fully qualified Anthropic model name for this path, such as `copilot-proxy-openai/gpt-5.5`. If `DEMO_CLAUDE_MODEL` is empty, the helper computes it from `DEMO_PROVIDER_REF` and `DEMO_AI_MODEL`.

The default `DEMO_CLAUDE_SETTING_SOURCES=""` is deliberate: the generated Claude Code command passes `--setting-sources ""` so local `~/.claude/settings.json` environment overrides do not silently point Claude Code at a different API base URL. When `DEMO_CLAUDE_TOOLS=""`, the scripts omit `--tools` entirely because current Claude Code builds reject an empty tool name; set `DEMO_CLAUDE_TOOLS` only when you intentionally want to pass a local Claude Code tool allow-list. Orka injects the Kubernetes task tools server-side.

## Notes

- The scripts render working files into `DEMO_WORKDIR`, which defaults to `/tmp/orka-demo`.
- `10-chat-pr.sh` discovers the Task created through the Anthropic proxy by the generated `sessionRef` and the `orka.ai/source=anthropic-proxy` label, then shows the prepared Agents, child Tasks, and final result through `kubectl` and the Orka HTTP API.
- `20-manual-workflow.sh` streams task logs with `kubectl logs` against the Pod created for the Task; it no longer depends on the Orka CLI.
- `30-cron-workflow.sh` intentionally waits for at least one scheduled child run before it starts presenting commands, and labels this as accelerated demo pacing.
- `40-security-scanning.sh` intentionally seeds the repository scan if needed before the presenter view. The very first run can take a while.
- The security demo is one-shot by default. Set `DEMO_SECURITY_SCHEDULE` if you want the `RepositoryScan` to recur.
- Security scan history is stored in SQLite. Deleting the `RepositoryScan` CR cleans up Kubernetes resources, but historical findings and scan runs may still exist. If you want a visually clean slate for that demo, use a fresh `DEMO_SECURITY_SCAN_NAME`.
