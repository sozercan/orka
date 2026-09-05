---
slug: /cli-reference
description: "The orka CLI: installing it, pointing it at a controller, and what it can do."
---

# CLI reference

The `orka` CLI talks to the Orka controller REST API and is intended for day-to-day task inspection, resource CRUD, and operator workflows. It uses the same authentication and namespace rules as the API.

Build the CLI locally with:

```bash
make build-cli
bin/orka --help
```

Install or copy `bin/orka` wherever you keep developer tools if you want `orka` on your `PATH`. For exhaustive flag and subcommand help generated from Cobra, see [CLI Command Reference](./cli-commands.md).

## Connection and authentication

Most commands accept these global flags:

| Flag | Description |
| --- | --- |
| `--server`, `-s` | Orka API server URL. Defaults to `http://localhost:8080`. |
| `--namespace`, `-n` | Kubernetes namespace. Defaults to `default` unless config or kubeconfig sets one. |
| `--token`, `-t` | Bearer token for API authentication. Prefer config or kubeconfig over passing real tokens in shell history. |
| `--txn-token` | Transaction token sent with the `Txn-Token` header. |
| `--txn-token-file` | Read a transaction token from a file, or `-` for stdin. |
| `--kubeconfig` | Kubeconfig path used for local discovery/token extraction fallback. |

The CLI reads persistent config from `~/.orka/config.yaml`:

```bash
orka config set-server http://127.0.0.1:8080
orka config set-namespace orka-system
kubectl create token orka-client -n orka-system | orka config set-token --file -
orka config view
```

`config view` masks the token. Prefer `config set-token --file <path>` or `--file -` over passing real tokens as process arguments. If you use `config set-token <token>` directly, use short-lived tokens and avoid pasting long-lived secrets into shell history.

Validate auth before running larger workflows:

```bash
orka auth validate
orka auth whoami -o json
```

## Output formats

Many read/list commands support `-o table`, `-o json`, and/or `-o yaml`. Prefer structured output for scripts:

```bash
orka task list -o json
orka provider list -o yaml
orka session get SESSION_ID -o json
```

Do not rely on full table layouts in automation; table output is optimized for people.

## Task workflows

Create tasks from manifests:

```bash
orka task create -f task.yaml
orka task wait my-task --timeout 5m
orka task result my-task
orka task logs my-task
orka task delete my-task
```

Create a simple container task from flags:

```bash
orka task create \
  --type container \
  --name hello-container \
  --image busybox:latest \
  --command sh \
  --command -c \
  --arg 'echo hello from orka'
```

Create an ACP agent task with explicit read-only workspace intent:

```bash
orka task create "Inspect the repository" \
  --type agent \
  --agent codex-reviewer \
  --workspace-intent read \
  --git-repo https://github.com/example/project \
  --read-credential repository-read
```

Create a write-intent task with independently scoped publication credentials and pull-request reconciliation:

```bash
orka task create "Implement the requested change" \
  --type agent \
  --agent codex-builder \
  --workspace-intent write \
  --git-repo https://github.com/example/project \
  --read-credential repository-read \
  --publication-git-repo https://github.com/example/project \
  --publication-read-credential repository-publication-read \
  --publication-credential repository-publish \
  --forge-credential repository-forge \
  --pr-base-branch main \
  --create-pr
```

The four roles are independent: source read for clone, target read for
preflight/verification, target write for the exact branch update, and forge for
PR reconciliation. Values are brokered only to the clean-room Publisher and
are never delivered to the ACP process tree.

Common task commands:

| Command | Purpose |
| --- | --- |
| `orka task create -f FILE` | Create a Task from YAML/JSON. |
| `orka task create --type container ...` | Create a Task directly from flags. |
| `orka task list [--status PHASE] [--transaction ID]` | List tasks, optionally client-side filtered. |
| `orka task get NAME [-o json|-o yaml]` | Read complete Task details. |
| `orka task status NAME [-o table|-o json|-o yaml]` | Show durable execution, delivery, and selected RuntimePool status. |
| `orka task wait NAME --timeout DURATION` | Wait for completion; exits nonzero for failed/cancelled tasks. |
| `orka task result NAME` | Print stored task result. |
| `orka task logs NAME` | Print completed task logs/result-store output, or live pod logs when available. |
| `orka task children NAME` | List child tasks. |
| `orka task plan NAME` | Read autonomous plan state. |
| `orka task artifacts NAME` | List task artifacts. |
| `orka task download NAME [FILENAME] --output PATH` | Download task artifact content. |
| `orka task delete NAME` | Delete/cancel a task. |

## Chat and dashboard helpers

`orka run` is a chat interface backed by Orka chat and provider configuration:

```bash
orka run "explain this task failure"
orka run --session incident-123
orka run --agent reviewer "review this diff"
```

It may depend on live provider credentials, model configuration, and server-side chat support.

`orka login` creates a ServiceAccount token and opens the dashboard with that token in the URL fragment:

```bash
orka login --service-account orka-client --namespace orka-system
```

For automation, tests, or terminals where token-bearing URLs could be captured, use the safe print-only mode:

```bash
orka login \
  --service-account orka-client \
  --namespace orka-system \
  --no-open \
  --redact-token
```

`--no-open` skips browser launch. `--redact-token` prints `<redacted>` instead of the raw token while still using the full token internally if browser opening is enabled. A redacted URL is not usable for manual login; rerun without `--redact-token` only in a trusted terminal if you need to copy the full URL.

Do not use default `login` output in logs or shared terminals where the generated browser URL might be captured.

## Resource management commands

The CLI can create/read/list/update/delete the core resource types through the controller API:

| Resource | Typical commands |
| --- | --- |
| Providers | `orka provider create`, `get`, `list`, `update`, `delete` |
| Agents | `orka agent create`, `get`, `list`, `update`, `delete` |
| Tools | `orka tool create`, `get`, `list`, `update`, `delete` |
| Skills | `orka skill init`, `validate`, `import`, `content`, `get`, `list`, `update`, `delete` |
| Secrets | `orka secret list` (metadata only; no Secret data is printed) |

Examples:

```bash
orka provider create -f provider.yaml
orka agent list -o json
orka tool update my-tool -f tool-updated.yaml
orka skill init ./my-skill --name my-skill --description "My skill"
orka skill validate ./my-skill/SKILL.md
orka skill import ./my-skill/SKILL.md --name my-skill
orka secret list -o json
```

## Sessions and memory

Sessions and durable memory are store-backed workflows rather than Kubernetes CRDs.

```bash
orka session list -o json
orka session get SESSION_ID -o json
orka session delete SESSION_ID

orka memory create --content "stable project fact" --source cli --tags docs,cli
orka memory list -o json --query "project fact"
orka memory get MEMORY_ID -o json
orka memory disable MEMORY_ID
orka memory enable MEMORY_ID
orka memory update MEMORY_ID --content "updated fact"
orka memory delete MEMORY_ID
orka memory proposal list -o json
orka memory proposal get PROPOSAL_ID -o json
orka memory proposal review PROPOSAL_ID --status accepted --reviewer OPERATOR
orka memory proposal apply PROPOSAL_ID --applied-by OPERATOR
orka memory proposal archive PROPOSAL_ID
```

Memory governance is explicit: reviewing a memory proposal does not automatically create durable memory. Use `orka memory proposal apply PROPOSAL_ID` only for an accepted proposal that should become durable memory.

## Security scans and repository monitors

Repository security scan configuration:

```bash
orka security repo create -f repository-scan.yaml
orka security repo get my-repo -o json
orka security repo list -o json
orka security threat-model update my-repo --content "Threat model" --source cli
orka security threat-model get my-repo -o json
orka security scan list my-repo -o json
orka security finding list my-repo -o json
orka security slice list my-repo -o json
orka security dropped-findings list my-repo -o json
orka security dropped-findings list my-repo --layer filter --reason contains=rate-limit -o json
orka security repo delete my-repo
```

Repository monitor configuration:

```bash
orka monitor create -f repository-monitor.yaml
orka monitor get my-monitor -o json
orka monitor list -o json
orka monitor runs my-monitor -o json
orka monitor items my-monitor -o json
orka monitor events my-monitor -o json
orka monitor delete my-monitor
```

Manual run/action commands such as `orka security scan run`, `orka monitor run`, and finding patch/PR actions can create downstream Tasks and may require live GitHub, provider, and agent configuration.

## Live-gated workflows

Some commands intentionally create downstream work or require external services. Keep these behind explicit operator intent in automation and e2e tests.

### `orka run`

`orka run` streams chat responses over the Orka chat API. A positive smoke needs server-side chat enabled plus a configured provider/model or agent:

```bash
ORKA_API=http://127.0.0.1:8080
ORKA_TOKEN="$(kubectl create token orka-client -n orka-system)"

orka --server "$ORKA_API" --token "$ORKA_TOKEN" \
  run --session cli-live-smoke "Reply with one short sentence."
```

For normal non-live validation, prefer a negative smoke against an unreachable or deliberately unconfigured server and assert a clean error without printing tokens.

### `orka security scan run`

Manual security scan runs can create scan Tasks and may require GitHub credentials, analysis agents, provider credentials, and repository network access:

```bash
orka security scan run my-repository-scan
orka security scan list my-repository-scan -o json
```

Gate this path with explicit environment variables in CI, for example `ORKA_CLI_E2E_LIVE_ACTIONS=1`, and skip by default when credentials or fixtures are missing.

### `orka monitor run`

Manual repository monitor runs can enqueue repository review/repair work and may require GitHub credentials plus reviewer/repair agents:

```bash
orka monitor run my-monitor --target-kind pull_request --target-number 123
orka monitor runs my-monitor -o json
orka monitor items my-monitor -o json
```

Use live-gated tests or manual verification for this path until stable GitHub/provider fixtures are available.

## ACP runtime management

Controller-owned pools and external v2 registrations use separate command groups:

```bash
orka runtime-pool list
orka runtime-pool get codex-read -o yaml

orka agent-runtime list
orka agent-runtime get external-codex -o yaml
orka agent-runtime create -f agent-runtime.yaml
orka agent-runtime update external-codex -f agent-runtime.yaml
orka agent-runtime delete external-codex
```

RuntimePool commands are read-only because pools are controller-owned. Table output reports lifecycle, admission, Pod count, resident-session capacity, prompt capacity, and queued demand. AgentRuntime table output reports only the `orka.harness.v2` identity/profile surface; legacy continuation and turn-capability fields are not rendered.

## Substrate actor pools

Substrate actor pools are managed through the `substrate pool` command group:

```bash
orka substrate pool create -f pool.yaml
orka substrate pool get my-pool -o json
orka substrate pool list -o json
orka substrate pool update my-pool -f pool-updated.yaml
orka substrate pool delete my-pool
```

Pool manifests require at least `spec.templateRef.name`; pool reconciliation may depend on the configured Substrate environment.

## Shell completion

The CLI includes Cobra-generated shell completion. Generate the script for your shell with:

```bash
orka completion bash
orka completion zsh
orka completion fish
orka completion powershell
```

Install the generated script using your shell's standard completion path. Common examples:

```bash
# Bash, current session
source <(orka completion bash)

# Bash, user-local install on Linux
mkdir -p ~/.local/share/bash-completion/completions
orka completion bash > ~/.local/share/bash-completion/completions/orka

# Zsh, current session
source <(orka completion zsh)

# Zsh, user-local install
mkdir -p ~/.zsh/completions
orka completion zsh > ~/.zsh/completions/_orka
# Ensure ~/.zsh/completions is in fpath before compinit, for example:
# fpath=(~/.zsh/completions $fpath)
# autoload -Uz compinit && compinit

# Fish, user-local install
mkdir -p ~/.config/fish/completions
orka completion fish > ~/.config/fish/completions/orka.fish
```

Regenerate completions after upgrading `orka` if commands or flags change.

## Other utility commands

| Command | Purpose |
| --- | --- |
| `orka status` | Show health, readiness, task counts, and agent count. |
| `orka models list --compat openai` / `anthropic` | List model IDs in provider-compatible formats. |
| `orka workspace status TASK` | Inspect canonical workspace policy and delivery status without credential references. |
| `orka task status TASK` | Inspect execution outcome, RuntimePool identity, and publication verification. |
| `orka audit trace TRANSACTION_ID` | Show tasks correlated by Kontxt transaction ID. |

## Binary e2e coverage matrix

Normal binary e2e tests build and invoke `bin/orka` directly with isolated config/home directories. They intentionally avoid real secrets in command arguments and assert stdout/stderr do not leak configured tokens or sentinel secret values.

| CLI area | Binary e2e status | Notes |
| --- | --- | --- |
| `auth` | Covered | `validate`, `whoami`; invalid-token negative path. |
| `models` | Covered | OpenAI and Anthropic compatibility list output. |
| `config` | Covered | Isolated `HOME`, `set-server`, fake `set-token`, masked `view`. |
| `status` | Covered | Health/readiness/task/agent summary. |
| `task` | Covered | Manifest create, flag create (including read/write workspace policy), structured status, list/filter, get, wait, result, logs, children, plan, artifacts, download, delete. |
| `workspace` | Covered | Canonical workspace intent, credential-role presence, and delivery status. |
| `runtime-pool` | Focused unit coverage | CRUD routing plus lifecycle/admission/capacity table output. |
| `agent-runtime` | Focused unit coverage | CRUD routing plus v2-only identity/profile table output. |
| `provider` | Covered | CRUD and secret redaction expectations. |
| `agent` | Covered | CRUD. |
| `tool` | Covered | CRUD. |
| `skill` | Covered | init, validate, import, list, get, content, update, delete. |
| `secret` | Covered | Metadata-only list. |
| `audit` | Covered | Trace no-match path. |
| `session` | Covered | List/get/delete against a controlled fixture. |
| `memory` | Covered | Create/list/get/disable/enable/update/delete and proposal-list smoke. |
| `security` | Partially covered | Repository scan create/get/list/delete, threat model update/get, scan/finding/slice/dropped-finding list; repository scan update is not covered. |
| `monitor` | Partially covered | Repository monitor create/get/list/delete plus runs/items/events list; monitor update is not covered. |
| `substrate` | Covered | Pool create/get/list/update/delete. |
| `run` | Negative covered; positive live-gated | Unreachable-server error path is safe for normal e2e; positive chat/SSE flow needs provider fixtures. |
| `login` | Safe mode covered | `--no-open --redact-token` is covered; full browser-open token URL remains unsuitable for normal e2e logs. |
| `completion` | Covered | Binary smoke generates bash, zsh, fish, and PowerShell completion output. |
| `security scan run` | Deferred/live-gated | Creates downstream scan Tasks and requires live agent/GitHub/provider setup. |
| `monitor run` | Deferred/live-gated | Creates downstream monitor work and can require GitHub/provider setup. |
| security finding actions | Deferred/live-gated | Need stable finding fixtures or live scan data. |

Use this matrix as a coverage guide when adding CLI commands: non-live command groups should have at least one compiled-binary e2e smoke path, and CRUD-style groups should cover create/get/list/update/delete where the API supports it.
