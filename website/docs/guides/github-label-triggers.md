---
slug: /github-label-triggers
description: "Starting agent tasks by applying a label to a GitHub issue or pull request."
---

# GitHub label triggers

Orka can create an ACP `type: agent` Task when a GitHub issue or pull request receives a label such as `agent:implement`, `agent:update-branch`, `agent:review`, or `agent:to-issues`.

## Webhook endpoint

Configure a GitHub repository webhook:

- **Payload URL:** `https://<orka-api-host>/webhooks/github`
- **Content type:** `application/json`
- **Secret:** a shared secret stored outside git, provided to the controller as `ORKA_GITHUB_WEBHOOK_SECRET`
- **Events:** `Issues` and `Pull requests`

Orka verifies `X-Hub-Signature-256` before reading the payload. Requests without a valid HMAC signature are rejected and do not create Tasks.

## Controller configuration

Set these environment variables on the controller Deployment:

| Variable | Required | Description |
| --- | --- | --- |
| `ORKA_GITHUB_WEBHOOK_SECRET` | yes | Shared webhook secret used for HMAC verification. Use a Kubernetes Secret. |
| `ORKA_GITHUB_LABEL_TRIGGER_AGENT` | yes | Default runtime Agent CR used for created `type: agent` Tasks. |
| `ORKA_GITHUB_LABEL_TRIGGER_NAMESPACE` | no | Namespace for created Tasks. Defaults to the controller watch namespace, then `default`. |
| `ORKA_GITHUB_LABEL_TRIGGER_GIT_SECRET` | no | Compatibility Secret selection for the label-trigger API. Orka maps it to `workspace.readCredentialRef`; for write actions it also maps the same reference to `workspace.publicationCredentialRef`. The Secret never enters the ACP runtime. Use direct Task/workflow creation when strict separation requires different read and publication Secrets. |
| `ORKA_GITHUB_LABEL_TRIGGER_PREFIX` | no | Label prefix. Defaults to `agent:`. |
| `ORKA_GITHUB_LABEL_TRIGGER_TIMEOUT` | no | Task timeout. Defaults to `30m`. |
| `ORKA_GITHUB_LABEL_TRIGGER_MAX_TURNS` | no | Agent max turns. Defaults to `100`. |
| `ORKA_GITHUB_LABEL_AGENT_<ACTION>` | no | Action-specific Agent override, for example `ORKA_GITHUB_LABEL_AGENT_REVIEW=review-agent`. Hyphens become underscores. |

Helm values expose the same settings under `github.webhook` and `github.labelTrigger`.

## Behavior

When GitHub sends a `labeled` event and the label starts with the configured prefix, Orka creates an idempotent Task named from the action, target number, and delivery ID.

Default action prompts:

- `agent:implement` - creates a write-intent workspace on a generated `orka/implement-...` branch (or an eligible same-repository PR head). The ACP child edits files only; the Workspace/Publisher owns deterministic commit preparation, exact-ref push, and verification.
- `agent:update-branch` - for pull requests only; creates a write-intent Task for an eligible same-repository PR head. Publication still occurs through the Workspace/Publisher.
- `agent:review` - for pull requests only; creates a read-intent Task at the exact PR head SHA. The clean-room source boundary may resolve the compatibility Git Secret for a private repository, but the ACP runtime receives neither read nor publication credentials.
- `agent:to-issues` - break the request into independently implementable GitHub issues, creating them when credentials/tools permit or returning drafts.
- Other `agent:<action>` labels create a generic action task with a scoped prompt.

For pull request actions, Orka records the PR base branch, repository identity, and exact head/base SHA on the Task context. ACP review Tasks consume a verified read-only workspace and must not rely on credential mounts or child-controlled Git metadata for authority.

GitHub delivery IDs make retries safe: if the same delivery is received again, Orka returns `202 Accepted` with the existing task name instead of creating a duplicate.

## Repository monitor events

The same `/webhooks/github` endpoint can queue exact-head `RepositoryMonitor` runs from pull request events. This path does not require an `agent:*` label. A monitor is eligible when `spec.review.exactEventEnabled: true`, pull request monitoring is enabled, the webhook repository matches `spec.repoURL`, the PR base branch matches the monitor branch, and the monitor is not suspended.

For `opened`, `reopened`, `synchronize`, `ready_for_review`, `labeled`, and `unlabeled` pull request events, Orka queues a monitor run for the exact PR head SHA and records an audit event. If the controller has a watch namespace, only monitors in that namespace are considered; otherwise monitors across all namespaces are eligible. Duplicate deliveries or already-queued runs for the same PR head are accepted without creating duplicate work. If a previous exact-event run for the same delivery failed before the queued audit event was recorded, a webhook retry can requeue that failed run.

## CI coverage

`.github/workflows/live-github-label-trigger-e2e.yml` is a manual GitHub Actions workflow for the label trigger path. It runs focused Go tests for webhook and PR monitor tooling, then builds the controller image, deploys Orka into a fresh Kind cluster, creates a synthetic Agent and webhook fixture, and sends signed webhook payloads to `/webhooks/github`.

The workflow is model-free and secret-free, so it validates webhook idempotency and Task construction rather than a provider-live ACP RuntimePool execution. It generates the webhook secret during the run and uses a synthetic `agent:implement` issue label payload for the configured `target_repo_url` and `target_number` inputs. The script verifies that invalid signatures return `401`, a valid label event creates one scoped agent Task, and a repeated GitHub delivery returns `202` with the original task name.

Run the same validation locally with:

```bash
GITHUB_LABEL_TRIGGER_TARGET_REPO_URL=https://github.com/orka-agents/orka \
GITHUB_LABEL_TRIGGER_TARGET_NUMBER=1 \
bash scripts/live-github-label-trigger-e2e.sh
```

## Minimal Helm configuration

```yaml
github:
  webhook:
    secretName: github-webhook-secret
    secretKey: secret
  labelTrigger:
    agent: codex-agent
    namespace: default
    gitSecret: git-credentials
    agents:
      review: review-agent
```

Create the referenced Secret outside git:

```bash
kubectl create secret generic github-webhook-secret \
  --from-literal=secret='<webhook-secret>'
```

## Durable `orka:*` RepositoryMonitor commands

`agent:*` labels remain the lightweight direct Task path. For durable monitor-owned workflows, configure `RepositoryMonitor.spec.triggers.github.labels.enabled: true` and use `orka:*` labels such as `orka:plan`, `orka:implement`, `orka:review`, `orka:fix`, and the optional head-bound `orka:automerge`.

The `orka:*` path differs from direct `agent:*` labels:

- the webhook becomes a durable command event instead of an immediate agent-owned GitHub mutation path;
- the GitHub sender's current repository permission is checked with the monitor's `spec.forgeCredentialRef`;
- protected or pause labels block the command without queueing work;
- accepted issue commands queue exact issue monitor runs, and accepted PR review commands queue exact-head monitor runs;
- duplicate GitHub deliveries reuse the same command event.

Use `agent:*` when you explicitly want a direct one-off agent task. Use `orka:*` when the RepositoryMonitor should own durable state, policy, auditability, and follow-up workflow decisions.

### Credentials the `orka:*` path needs

Turning on `spec.triggers.github.labels.enabled` makes the controller call the GitHub
API on your behalf — at minimum to look up whether the person who applied the label is
allowed to. That call needs its own token.

| What the monitor does | Required in `spec` |
| --- | --- |
| Any `orka:*` label handling | `forgeCredentialRef` |
| Plus implementation or repair (a monitor that pushes branches and opens PRs) | `readCredentialRef`, `publicationReadCredentialRef`, `publicationCredentialRef`, and `forgeCredentialRef` — four **different** Secrets |

:::warning[`gitSecretRef` is not enough here]
`gitSecretRef` is the old single-Secret field. It still works, but only as a fallback for
*reading* the source repository. It never supplies a publication or forge role. A monitor
with `labels.enabled: true` and no `forgeCredentialRef` is rejected with
`spec.forgeCredentialRef is required for controller-owned GitHub mutations`.
:::

Each Secret must hold a non-empty `token`, `password`, or `GITHUB_TOKEN` key. See
[Repository monitors](repository-monitors.md#prerequisites) for what each of the four roles
is allowed to do and why they are kept apart.

RepositoryMonitor command labels are one-shot intents. For custom labels, configure them under `spec.triggers.github.labels`; Orka excludes both default `orka:*` labels and configured custom command labels from issue snapshot digests so consuming a command label does not stale the issue workflow.
