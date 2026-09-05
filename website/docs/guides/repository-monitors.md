---
slug: /repository-monitors
description: "Watching a repository and having agents triage, review, and repair on a schedule."
---

# Repository monitors

Repository monitors are durable, Kubernetes-native PR review automation for GitHub repositories. A `RepositoryMonitor` stores the repository scope, review agent, schedule, and safety policy in a CRD. The controller records runs, PR inventory, review results, and audit events in the SQLite store, then exposes that state through the REST API and embedded dashboard.

This is the durable successor path for prompt-orchestrated PR monitor tasks created by the `create_pr_monitor` tool. The implementation supports GitHub pull request and issue inventory, durable `orka:*` command intake, issue triage/research/planning/implementation, controller-owned issue-to-PR mutation, exact-head PR review, bounded repair, readiness state, mutation auditing, and optional controller-owned GitHub `COMMENT` review publishing. Automerge is available only when explicitly configured and remains disabled by default.

## What it does

A repository monitor can:

- list open pull requests for one GitHub repository and base branch
- inventory open GitHub issues, excluding pull-request-shaped issues
- skip drafts unless explicitly configured to include them
- skip PRs blocked by configured protected or pause labels
- skip PR heads that already have a fresh review result
- queue one read-only review task per selected PR head
- refetch one pull request for targeted manual and webhook runs before queueing review work
- queue exact-head monitor runs from GitHub pull request webhook events
- ingest typed JSON review results from completed review tasks
- store monitor runs, issue/PR items, command events, workflow actions, action records, implementation jobs, mutation records, review records, and audit events durably
- show monitor status, recent runs, workflow timeline, blocked reasons, implementation jobs, mutation audit, issues, and the PR queue in the dashboard under **Monitors**

The review Task is bound to the exact PR head SHA. It runs as a `type: agent` Task with top-level `workspace.intent: read`; `RepositoryMonitor.spec.readCredentialRef` is mapped to `workspace.readCredentialRef`, with `spec.gitSecretRef` retained only as a backward-compatible read-only fallback. The ACP runtime receives no Git credential and must leave the verified tree unchanged. If `spec.review.publish.enabled` is true, the controller later revalidates the PR state and may publish a deterministic neutral `COMMENT` review with `spec.forgeCredentialRef`.

## Current limits

The first implementation is intentionally narrow:

- GitHub is the only supported provider.
- Pull requests and issues are supported target types; commit monitoring is still rejected.
- Pull request monitoring is enabled by default when no target is specified.
- Issue-only monitors can set `spec.targets.pullRequests.enabled: false` and `spec.targets.issues.enabled: true`.
- `spec.review.requireGreenCI` gates review selection until CI is green.
- GitHub webhook-driven exact runs are opt-in with `spec.review.exactEventEnabled`.
- Repair, maintainer command routing, issue action workflows, implementation budgets (`maxActive`, `maxAttemptsPerIssue`, `maxChangedFiles`, `allowedPaths`), and optional head-bound automerge are active monitor-owned workflows. Automerge remains disabled by default and requires explicit configuration plus a one-shot command.
- Built-in reviewer Agents may use `runtime.type: claude`, `codex`, or `opencode`. Codex reviewers are confined by the RuntimeSession boundary: elevation requests are rejected by the controller, file writes are mediated by the supervisor, and the read-intent workspace delta classification fails any turn that modifies the workspace. Reviewer Agents must omit `spec.secretRef`; provider credentials come from the controller-managed runtime proxy and never enter the Task spec.

## CI coverage

Repository monitor backend coverage has a focused GitHub Actions workflow at `.github/workflows/repository-monitor-smoke.yml`. It runs on pull requests and pushes that touch the workflow, Go API/controller/store code, CRD/config paths, worker code, or Go dependency files.

The smoke workflow creates the UI embed stub and runs targeted Go tests for monitor store CRUD, API handlers, GitHub pull request event handling, targeted single-PR inventory runs, controller queue and review flow, blocked status counts, read-only review Task construction, stdout result forwarding, `create_pr_monitor` repository URL and credential validation, GitHub tool `repo_url` scope enforcement, and PR review marker signing/detection tooling. Worker-level PR review diff context generation is covered by the normal Go test workflow. UI monitor pages are covered by the normal frontend test workflow rather than this smoke workflow.

The smoke workflow is secret-free. Exact pull request event queueing is exercised with synthetic signed webhook payloads and test clients, so repository monitor PRs do not need live GitHub credentials just to verify queueing, scope checks, or review result ingestion in CI.

## Prerequisites

Create a built-in runtime Agent in the same namespace as the monitor, or set `spec.agents.reviewer.namespace` explicitly. Built-in runtime Agents must not set `spec.secretRef`; provider authentication is supplied by the controller-managed runtime proxy.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: repo-reviewer
  namespace: default
spec:
  runtime:
    type: claude
    contractVersion: orka.harness.v2
    defaultMaxTurns: 50
    defaultAllowedTools:
      - Read
      - Grep
      - Glob
  model:
    name: claude-opus-5
  systemPrompt:
    inline: |
      Review the exact pull request head for correctness, tests, security, and maintainability.
      Return concise, structured findings and do not mutate GitHub.
```

For private repositories or higher GitHub rate limits, create a source-read Secret in the monitor namespace. When a monitor is created or updated through the API, Orka validates that each configured credential Secret exists and contains a non-empty `token`, `password`, or `GITHUB_TOKEN` key.

```bash
kubectl create secret generic repo-monitor-source-read \
  --namespace default \
  --from-literal=token='<read-only-github-token>'
```

For review, triage, research, and planning Tasks, the controller maps `RepositoryMonitor.spec.readCredentialRef` to top-level `workspace.readCredentialRef`. `spec.gitSecretRef` is accepted only as a compatibility fallback for this source-read role. The clean-room source boundary resolves it for same-repository private PR heads; the ACP runtime never receives the Secret. Fork PR heads use the eligible source repository URL and remain read-only.

Implementation and repair are write workflows and require four explicit, pairwise-distinct Secret references:

| RepositoryMonitor field | Task workspace role | Required capability |
| --- | --- | --- |
| `readCredentialRef` | `workspace.readCredentialRef` | Source clone/read only |
| `publicationReadCredentialRef` | `workspace.publicationReadCredentialRef` | Target preflight and independent verification only |
| `publicationCredentialRef` | `workspace.publicationCredentialRef` | Exact compare-and-swap branch push |
| `forgeCredentialRef` | `workspace.forgeCredentialRef` | Controller-owned GitHub API operations and PR reconciliation |

`gitSecretRef` never supplies a write, publication, or forge role. A write Task is not created when any explicit role is missing or when two roles reference the same Secret. Credential values are resolved only by the controller/Workspace Publisher brokers and never appear in ACP process environment, prompts, Task status, or delivery receipts.

## Review workspace context

The review Task is pinned to the exact PR head SHA with `workspace.intent: read`. Before creating it, the controller fetches the pull request identity, lists the pull request's changed files with their patches, and refetches the pull request to ensure the base, head, and head repository did not change during context assembly. The drift check runs even when the file listing failed, so a race fails closed instead of queueing a stale review; a GitHub read failure does not fail the run and instead marks the context `contextUnavailable`.

The controller embeds a bounded `orka.prReview.context.v1` payload in the prompt: at most 700 KiB encoded context, patch excerpts (at most 64 KiB encoded each) for the first 100 changed files, and path-only identity entries (`patchOmitted: "capped"`) for up to 2,000 changed files, with bounded paths/short metadata fields. Identities take precedence over patches inside the byte budget: patches are dropped first, and `truncated.files` is set only when the complete change set could not be represented — a missing identity, a compare listing shorter than the pull request's `changed_files` total (GitHub caps the compare file array at 300 entries), or an omitted patch that hides content the checkout cannot show (a removed file, a renamed file's previous contents, or deleted lines in any changed file). Because the checkout carries no Git metadata, the prompt then requires a non-`passed` verdict (`needs_human` or stricter). Patches and paths pass through the shared credential redaction before they are persisted in the Task prompt. The prompt treats titles, labels, paths, patches, and repository content as untrusted data and tells the reviewer to inspect the verified checkout whenever GitHub context is incomplete.

If a Task with the predictable review name already exists, the controller adopts it only when its spec is byte-identical to the freshly rendered review (including the diff context) or carries only the controller-shaped `contextUnavailable` envelope; any other pre-existing Task fails the run.

The reviewer must not mutate the verified tree. Any unexpected workspace change fails read validation. GitHub review publishing, when enabled, happens later through the controller's deterministic publisher path; the ACP child has no GitHub mutation credential.

If `spec.validation.image` is configured, the review Task also receives the brokered `run_validation` tool. The reviewer inspects the repository and supplies one shell command. Orka creates one container Task with the configured image, a 45-minute timeout, and the same exact-head read-only workspace. The checkout init container finishes first. Orka then installs a deny-all NetworkPolicy before releasing the validation command. The reviewer cannot change the image, checkout, credentials, publication settings, or network policy. It can wait only for its own child Task through `wait_for_tasks` before returning the review result.

The image is repository-specific, not language-specific. It must contain `/bin/sh` plus every compiler, linter, package manager, dependency, or infrastructure CLI that validation may need. The command cannot download modules, packages, providers, or contact cloud APIs. A Go repository that uses `golangci-lint` should point at a Go-based image with dependencies and `golangci-lint` installed. Repositories that run offline Terraform or Azure CLI checks should include those tools and required local data. Maintainers can build and publish that image once, then reuse it in the repository's monitor configuration.

Validation fails closed when configured. A reviewer that does not call the tool, a failed command, a non-terminal child Task, or a child Task whose image or checkout no longer matches the review cannot produce a `passed` or merge-ready result. Orka stores the image, command digest, status, and safe evidence in the durable review record. The validation binding retains the same SHA-256 command digest. Validation stdout and stderr are suppressed so repository or fixture secrets cannot enter Pod logs or result storage. A failure before workload execution is recorded as `unavailable`, and the same head remains eligible for a later review. No validation Task is required when `spec.validation.image` is empty; the review records validation as `not_run`.

## Create a monitor

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: RepositoryMonitor
metadata:
  name: example-app
  namespace: default
spec:
  provider: github
  repoURL: https://github.com/example/app
  branch: main
  readCredentialRef:
    name: repo-monitor-source-read
  schedule: "*/30 * * * *"
  timeZone: "UTC"
  targets:
    pullRequests:
      enabled: true
      includeDrafts: false
      maxPerRun: 10
  agents:
    reviewer:
      name: repo-reviewer
  review:
    event: COMMENT
    staleReviewTTL: 24h
    exactEventEnabled: true
  policy:
    protectedLabels:
      - security-sensitive
    pauseLabels:
      - orka:pause
  validation:
    image: ghcr.io/example/app-validation@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
```

Apply it with:

```bash
kubectl apply -f repository-monitor.yaml
```

The controller normalizes `provider`, `owner`, `repository`, `branch`, pull request enablement, `maxPerRun`, and `review.event` when omitted. `review.publish.enabled` defaults to `false`; when enabled, V1 rejects publish events other than `COMMENT` and same-head policies other than `skip`.

If an existing monitor still uses `validation.mode` and `validation.commands`, migrate it before upgrading the controller. Apply the new CRD first, replace the legacy fields with a digest-pinned `validation.image`, and remove the old fields. The compatibility schema preserves a non-empty legacy command list long enough for the controller to report `LegacyValidationCommandsUnsupported`; it will not run the monitor with validation silently disabled.

## Run manually

Scheduled runs are queued from `spec.schedule` when the monitor is not suspended. You can also trigger a manual run through the API:

```bash
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://<orka-api-host>/api/v1/monitors/repositories/example-app/runs?namespace=default" \
  -d '{}'
```

To target one pull request, include `targetKind` and `targetNumber`:

```json
{
  "targetKind": "pull_request",
  "targetNumber": 123
}
```

`targetSHA` can also be supplied to require an exact head SHA match.

When `targetNumber` is set, the controller fetches that pull request directly from GitHub before applying the monitor's open-state, base-branch, draft, label, stale-review, and optional `targetSHA` checks. Targeted runs do not retire missing or out-of-scope items from the repository-wide inventory, so monitor status counts continue to summarize the stored PR queue rather than only the targeted PR.

## Run from GitHub events

Repository monitors can also receive exact pull request events through the same signed GitHub webhook endpoint used by label triggers. Configure the repository webhook for `Pull requests` events and set `spec.review.exactEventEnabled: true` on the monitor.

When `/webhooks/github` receives an `opened`, `reopened`, `synchronize`, `ready_for_review`, `labeled`, or `unlabeled` pull request event, Orka matches monitors by repository and base branch. If the controller has a watch namespace, only monitors in that namespace are considered; otherwise monitors across all namespaces are eligible. A matching monitor queues a run with `targetKind: pull_request`, the PR number, and the exact head SHA from the webhook payload. Replayed deliveries and already-queued runs for the same PR head are accepted without creating duplicate monitor work. If a previous exact-event run for the same delivery failed before the queued audit event was recorded, a webhook retry can requeue that failed run.

Exact event runs are still read-only review runs. They are stored with trigger `pull_request_event`, create an `exact_event_run_queued` audit event, and wait behind any active or queued monitor run. When the run executes, Orka refetches the current pull request by number and skips review work if the PR is no longer open, moved to another base branch, or no longer matches the event head SHA.

## Inspect state

Use `kubectl` for CRD-level status:

```bash
kubectl get repositorymonitors -n default
kubectl describe repositorymonitor example-app -n default
```

Use the API or dashboard for durable run and item state:

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "https://<orka-api-host>/api/v1/monitors/repositories?namespace=default"

curl -H "Authorization: Bearer $TOKEN" \
  "https://<orka-api-host>/api/v1/monitors/repositories/example-app/runs?namespace=default"

curl -H "Authorization: Bearer $TOKEN" \
  "https://<orka-api-host>/api/v1/monitors/repositories/example-app/items?namespace=default&kind=pull_request"

curl -H "Authorization: Bearer $TOKEN" \
  "https://<orka-api-host>/api/v1/monitors/events?namespace=default&name=example-app"
```

The embedded dashboard shows the same state under **Monitors**:

- monitor list with phase, schedule, repository, and summary counts
- detail page with open PR count, pending reviews, blocked items, merge-ready count, recent runs, and PR queue
- manual **Run** action for an immediate monitor run

## Review results

Review tasks must return a JSON object with schema version `orka.prReview.v1`. The controller validates the repository, PR number, and exact head SHA before accepting the result. When validation is configured, the controller also verifies the child validation Task independently of the reviewer's reported test status. Accepted results are stored as immutable review records and copied onto the current monitor item.

Valid review verdicts are:

- `passed`
- `needs_changes`
- `needs_human`
- `security_sensitive`
- `skipped`

For status summaries, open PRs with `needs_changes`, `needs_human`, `security_sensitive`, stale, failed, or skipped review state count as blocked items. Open PRs with queued review work count as pending reviews.

If a review task fails, is cancelled, returns malformed JSON, or returns a stale head SHA, the controller records a rejected review result and leaves an audit event explaining why. If GitHub publishing is enabled, the controller performs publish-time safety checks immediately after ingestion: it refetches the PR, requires the PR to remain open on the monitor base branch and exact reviewed head SHA, rejects draft or protected-label PRs, skips duplicate same-head publications using Orka publish records and hidden GitHub markers, neutralizes mentions in rendered text, and never posts `security_sensitive` results unless explicitly configured.

## API and authorization

Repository monitor endpoints live under `/api/v1/monitors/*` and require normal Orka API authentication. When context-token authorization is enabled, monitor reads require `orka:monitors:read`, monitor CRUD requires `orka:monitors:write`, and manual run creation requires `orka:monitors:operate`.

Context-token `tctx` constraints can also restrict monitor access by namespace, repository URL, branch, reviewer Agent, or allowed Agent set.

See [API Reference](../reference/api-reference.md#repository-monitors) for endpoint details.

## Prompt-orchestrated PR monitor tool

`create_pr_monitor` remains the compatibility path for prompt-orchestrated scheduled PR monitors. It creates a scheduled `type: ai` Task with `spec.workspace.gitRepo` set to the requested GitHub repository, injects the PR review loop tools, and instructs the monitor to call `list_pull_requests`, `check_pr_review_marker`, `check_pull_request_ci`, `review_pull_request`, and `post_review_comment` with the same `repo_url`.

`repo_url` must be a credential-free GitHub repository root URL, for example `https://github.com/owner/repo`, `https://github.com/owner/repo.git`, or `git@github.com:owner/repo.git`. Do not pass a pull request, issue, branch/tree, blob/file, commit, query-string, fragment, non-GitHub, HTTP, or token-bearing URL. Orka rejects non-root repository URLs before it creates the monitor Task, which prevents prompts or copied browser URLs from widening the monitor's repository scope.

The tool requires an AI Agent with coordination enabled and autonomous coordination disabled. The created Task uses a narrow explicit tool set instead of the full coordination tool set, and it requires a Git credential Secret either through `gitSecretRef` or one of the supported default Secret names in the target namespace: `git-credentials`, `github-credentials`, `copilot-token`, `github-token`, or `git-token`. Orka validates the selected Secret before creating the monitor Task; it must contain a non-empty `token`, `password`, or `GITHUB_TOKEN` key.

The scheduled monitor prompt tells the worker to pass the same `repo_url` to every PR review loop tool. Those GitHub tools are scoped to the current Task: when task context is available, the requested repository must match the Task workspace repository or signed transaction repository context. If it does not match, Orka rejects the tool call before resolving credentials or calling GitHub. This means a monitor created for `owner/repo` cannot use its Task credential to list, review, or comment on another repository by changing tool arguments.

### PR review markers

`check_pr_review_marker` returns the exact hidden marker that the monitor should include in the GitHub review body:

```html
<!-- orka:pr-review repo=owner/repo pr=123 head_sha=abc123 sig=... -->
```

The marker binds the review to one repository, pull request number, and head SHA. Future monitor runs skip that PR head only when they find a matching marker in a GitHub pull request review.

Markers are stable across GitHub token rotation. They are not signed with the live GitHub token by default. To make marker verification independent of the review author, provide a stable worker environment secret named `ORKA_PR_REVIEW_MARKER_SECRET` to the monitor Task. During rotation, keep the old value in comma-separated `ORKA_PR_REVIEW_MARKER_PREVIOUS_SECRETS` until reviews signed with it have aged out.

For compatibility, Orka also recognizes legacy markers and markers signed before a dedicated marker secret was configured, but only from a trusted reviewer account. Set `ORKA_PR_REVIEW_MARKER_TRUSTED_AUTHOR` to that GitHub login, or omit it to let Orka resolve the authenticated GitHub user for the Task's Git credential. Do not store marker signing secrets in the repository; use Kubernetes Secrets or another secret injection path for Task environment.

## Related workflows

- [GitHub Label Triggers](github-label-triggers.md) create one-off agent tasks from labels such as `agent:review` or `agent:implement`.
- [Repository Security Scanning](repository-security-scanning.md) scans repository history for security findings and supports patch proposal workflows.
- `create_pr_monitor` remains available for prompt-orchestrated scheduled PR monitor tasks, but it does not provide the durable per-PR run, item, review, publish, and event records described here.

## Issue inventory and label commands

Repository monitors can also inventory open GitHub issues when `spec.targets.issues.enabled: true`. Issue inventory excludes GitHub issues that represent pull requests, stores the item as `monitor_items.kind = issue`, and records an issue content digest over human-controlled inputs: issue number, title, body, and non-`orka:*` / non-`orka-state:*` labels. Orka-authored command labels and state labels therefore do not stale issue plans or future issue workflow artifacts.

A monitor can be run against one exact issue without retiring unrelated inventory:

```bash
orka monitor run orka-main --target-kind issue --target-number 123 --namespace default
orka monitor issues list orka-main --namespace default
```

Durable `orka:*` label command intake is enabled per monitor:

```yaml
spec:
  targets:
    pullRequests:
      enabled: false
    issues:
      enabled: true
      maxPerRun: 10
      excludeLabels:
        - blocked
        - waiting-external
  triggers:
    github:
      labels:
        enabled: true
        requireActorPermission: write
        issues:
          plan: orka:plan
          implement: orka:implement
        pullRequests:
          review: orka:review
          fix: orka:fix
          automerge: orka:automerge
```

When a matching label webhook arrives, Orka verifies the webhook signature, matches the repository monitor by repository and target kind, checks the sender's current GitHub repository permission using `spec.forgeCredentialRef`, records a durable command event, and queues a targeted monitor run for accepted commands. Replayed deliveries are idempotent. Guard labels from `spec.policy.protectedLabels` and `spec.policy.pauseLabels` record blocked commands and do not queue work.

Inspect command intake with:

```bash
orka monitor commands list orka-main --namespace default
orka monitor commands get '<command-id>' --namespace default
```

### Label quick reference

Once label intake is enabled, applying one label on GitHub is the whole user
interface. This table maps each default label to what Orka does and where the
result appears:

| Label | Target | What Orka does | Where you see the result |
|---|---|---|---|
| `orka:triage` | issue | Read-only triage task classifies the issue | Orka's status comment on the issue; `orka monitor actions list` |
| `orka:research` | issue | Read-only research task investigates the problem | Status comment (problem statement and findings); action record |
| `orka:plan` | issue | Read-only planning task drafts an implementation plan | Status comment; issue moves to `plan_ready` (or `approval_required`) |
| `orka:approve-plan` | issue | Records human approval of the plan | Issue state moves to `approved` |
| `orka:implement` | issue | Write task implements the approved plan in a sanitized workspace | A pull request opened by the clean-room publisher |
| `orka:review` | PR | Exact-head review of the PR | Review comment and readiness state on the PR |
| `orka:fix` | PR | Repair task on the PR head branch | New commits pushed to the PR branch |
| `orka:fix-ci` | PR | CI-focused repair on the PR head branch | New commits pushed to the PR branch |
| `orka:update-branch` | PR | Merges the base branch into the PR head | Updated PR branch |
| `orka:automerge` | PR | Arms the optional automerge workflow (if enabled) | PR merges once review and CI gates pass |

Notes:

- Label names are configurable per monitor (`spec.triggers.github.labels`);
  the table shows the conventional defaults.
- Orka maintains **one status comment per issue** and edits it in place as
  phases complete, rather than posting a new comment per phase.
- Labels listed in `spec.policy.pauseLabels` (and `protectedLabels`) block
  command intake for that item: the command is recorded as blocked and no work
  is queued.
- Commands act on the monitor's *inventoried* view of an item. If the PR or
  issue changed very recently, run a targeted inventory pass first
  (`orka monitor run <name> --target-kind pr --target-number <n>`) so the
  command binds to the current head.

## Issue triage, research, planning, and implementation

When issue command labels are enabled, accepted issue commands now drive monitor-owned task phases:

- `orka:triage` creates a read-only issue triage task and stores an `issue_triage` action record.
- `orka:research` creates a read-only issue research task and stores an `issue_research` action record.
- `orka:plan` creates a read-only planning task and stores an `issue_plan` action record. Plans that require approval move the issue to `approval_required`.
- `orka:approve-plan` records an approval action and moves the issue to `approved`.
- `orka:implement` creates an implementation task only when policy permits it. By default, implementation requires an approved plan; otherwise Orka queues planning first.

Issue action tasks are bound to the issue snapshot digest. Result payloads with mismatched issue numbers or stale digests are recorded as stale/failed action records instead of advancing workflow state.

Implementation Tasks use `workspace.intent: write`, a controller-selected push branch (`spec.issueWorkflow.implementation.branchPrefix`, default `orka/issue`), and the four explicit credential roles above. The ACP child edits only the sanitized workspace and receives no Git or forge credential. After prompt completion, the separate Workspace Publisher freezes and validates the delta, performs the exact publication, independently verifies the remote, and records a non-secret `status.delivery` receipt.

The controller advances the workflow only from a `VerifiedExact` delivery whose publication repository, branch, expected commit, verified remote SHA, prior remote SHA, and artifact digest all match the Task contract. Missing, conflicting, superseded, cancelled, credential-blocked, or otherwise unverifiable delivery is recorded as a blocked implementation. After verified publication, the controller uses `forgeCredentialRef` to create or reuse the pull request with a deterministic Orka-rendered body.

Inspect action records with:

```bash
orka monitor actions list orka-main --namespace default --kind issue --number 123
orka monitor actions get '<action-id>' --namespace default
```

## PR repair and readiness

Pull request command labels can start bounded controller-tracked repair tasks:

- `orka:review` queues an exact-head review run.
- `orka:fix` queues a repair task on the current same-repository PR head branch.
- `orka:fix-ci` queues a CI repair task using the same repair path.
- `orka:update-branch` queues a base-update repair task and allows empty push-branch updates.

Repair Tasks are exact-head write Tasks: `workspace.ref` and `workspace.expectedRemoteSHA` bind the selected PR head, and publication targets that same branch. A repair succeeds only with a matching `VerifiedExact` delivery receipt. An `update-branch` no-change result is accepted only after the controller independently verifies that the exact PR head contains the requested base revision. Successful repairs clear stale review state so the next exact-head review can recompute readiness. By default, a PR with a passed exact-head review and no active repair is surfaced as merge-ready state for humans to merge; Orka only merges automatically when the optional automerge workflow below is explicitly enabled.


## Optional automerge

Automerge is disabled by default. To enable it, set `spec.automerge.enabled: true` and use a one-shot pull request command label such as `orka:automerge`. When `spec.automerge.requireGlobalMergeGate` is omitted or true, the controller also requires the process environment variable `ORKA_REPOSITORY_MONITOR_AUTOMERGE_GATE=true`; set `requireGlobalMergeGate: false` only for tightly scoped test or local deployments.

Before merging, Orka verifies that the command is bound to the current PR head SHA, the actor permission satisfies the automerge policy, the PR has a passed exact-head Orka review, CI checks are green, the PR is mergeable, there are no protected/pause labels, and no repair is active or failed. Every merge attempt writes an action record before or during the attempt, and failures are surfaced in the PR item `automergeState`.
