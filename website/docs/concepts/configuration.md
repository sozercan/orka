---
slug: /configuration
---

# Configuration

## Custom Resources

### Task

The core work unit. Supports container commands, native AI prompts, or ACP v2 coding-agent RuntimeSessions.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: my-task
spec:
  type: ai  # or "container" or "agent"
  agentRef:
    name: my-agent
  prompt: "Analyze the latest Kubernetes security best practices"
  execution:
    runtimeClassName: gvisor
    nodeSelector:
      sandbox-runtime: gvisor
  sessionRef:
    name: my-session
    create: false  # default: false
    append: true
    maxMessages: 50
  priority: 500
  timeout: 5m
  retryPolicy:
    maxRetries: 3
    backoffMultiplier: 2
    initialDelay: 10s
  webhookURL: "https://example.com/webhook"
  # Scheduled/recurring task fields (optional)
  schedule: "0 */6 * * *"      # Cron expression
  timeZone: "America/New_York" # IANA timezone
  concurrencyPolicy: Forbid    # Allow or Forbid concurrent runs
  startingDeadlineSeconds: 100  # Deadline for starting missed scheduled runs (default: 100)
  suspend: false
  successfulRunsHistoryLimit: 3
  failedRunsHistoryLimit: 1
```

For `type: agent`, repository and delivery policy belong at top-level `spec.workspace`:

```yaml
workspace:
  intent: write
  gitRepo: https://github.com/example/project.git
  branch: main
  readCredentialRef:
    name: project-source-read
  publicationGitRepo: https://github.com/example/project.git
  publicationReadCredentialRef:
    name: project-target-read
  publicationCredentialRef:
    name: project-target-write
  forgeCredentialRef:
    name: project-forge
  pushBranch: orka/example-change
  prBaseBranch: main
  createPR: true
```

`readCredentialRef` is source-read only;
`publicationReadCredentialRef` is target preflight/verification only;
`publicationCredentialRef` is target-write only; and `forgeCredentialRef` is
PR-reconciliation only. The controller freezes each selected Secret version and
the credential broker releases it only to the Workspace/Publisher for the exact
operation. None enters the ACP runtime process tree.

### Agent

Reusable agent configurations with model settings, tools, skills, and optional agent-to-agent coordination.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: researcher-agent
spec:
  providerRef:
    name: anthropic-prod
  execution:
    runtimeClassName: kata-qemu
    nodeSelector:
      sandbox-runtime: kata
  model:
    temperature: 0.7
    maxTokens: 4096
  systemPrompt:
    inline: "You are a research specialist..."
  tools:
    - name: web-search
    - name: github-search
  skills:
    - name: skill-researcher
  session:
    maxMessages: 50
  coordination:
    enabled: true
    autonomous: true          # Enable autonomous loop mode
    maxIterations: 20         # Max loop iterations (0 = unlimited)
    allowedAgents:
      - name: coder-agent
    maxConcurrentChildren: 5
    maxDepth: 3
```

**Coordination fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable agent-to-agent coordination tools |
| `autonomous` | bool | `false` | Enables autonomous loop mode. When true, the controller re-creates Jobs in a loop instead of marking the task as Succeeded |
| `maxIterations` | int32 | `0` | Limits the number of autonomous loop iterations. Only used when `autonomous` is true. `0` means unlimited |
| `approvalRequiredTools` | list | `[]` | Custom Tool CRD names that require human approval before execution in enabled autonomous coordination mode. Built-in tools, including `request_approval`, are rejected |
| `allowedAgents` | list | `[]` | List of agent names this agent is allowed to delegate to |
| `maxConcurrentChildren` | int32 | `5` | Maximum number of concurrent child tasks |
| `maxDepth` | int32 | `3` | Maximum delegation depth |

**Auto-injected coordination tools** (when `enabled: true`):

`delegate_task`, `wait_for_tasks`, `create_container_task`, `cancel_task`, `send_message`, `check_messages`, `recall_memory`, `remember`, `propose_memory`, `search_transcript`, `create_pull_request`, `list_pull_requests`, `check_pr_review_marker`, `check_pull_request_ci`, `merge_pull_request`, `auto_merge_pull_request`, `review_pull_request`, `post_review_comment`, `create_agent`, `delete_agent`, `update_plan`

When `autonomous: true`, `request_approval` is also injected so the worker can park the task after an explicit human approval request.

**Opt-in coordination tools** (require explicit `spec.tools[]` entries on the Agent):

`list_issues`, `get_issue`, `comment_on_issue`

**PR review marker environment:**

Prompt-orchestrated PR monitors use `check_pr_review_marker` to produce and detect hidden review markers. These variables are read by the worker Task that runs the tool:

| Environment variable | Description |
|----------------------|-------------|
| `ORKA_PR_REVIEW_MARKER_SECRET` | Optional stable HMAC key for PR review marker signatures. Use a Kubernetes Secret or another secret injection path. |
| `ORKA_PR_REVIEW_MARKER_PREVIOUS_SECRETS` | Optional comma-separated previous marker keys accepted during rotation. |
| `ORKA_PR_REVIEW_MARKER_TRUSTED_AUTHOR` | Optional GitHub login trusted for legacy marker compatibility. When omitted, Orka resolves the authenticated GitHub user for the Task credential. |

### RepositoryScan

Repository security scan configuration. A `RepositoryScan` is namespace-scoped and tells Orka which repository to scan, how to schedule incremental scans, and which Agents should perform analysis and remediation.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: RepositoryScan
metadata:
  name: example-repo
  namespace: default
spec:
  provider: github
  repoURL: "https://github.com/example/app"
  owner: example
  repository: app
  branch: main
  ref: "v1.2.3"                 # optional tag, branch, or commit SHA checkout override
  subPath: "services/api"       # optional monorepo scope
  gitSecretRef:                  # optional for private repositories
    name: github-credentials
  forkRepo: "https://github.com/example/app-security-fork" # optional remediation fork
  prBaseBranch: main             # optional PR base branch override
  schedule: "0 2 * * *"         # optional cron expression for incremental scans
  timeZone: "UTC"               # optional IANA time zone
  historyDays: 30                # optional initial history window
  validationMode: light          # off, light, or full
  validationMaxFindingsPerRun: 8 # optional auto-validation task cap for light mode
  validationMinSeverity: medium  # optional auto-validation severity threshold
  validationMinConfidence: medium # optional auto-validation confidence threshold
  customScanInstructionsRef:     # optional ConfigMap-backed additive scanner instructions
    name: repo-security-policy
    key: policy                  # optional; defaults to policy
  falsePositivePolicyRef:        # optional ConfigMap-backed false-positive policy
    name: repo-security-policy
    key: false-positives
  analysisAgentRef:
    name: security-reviewer
  patchAgentRef:                 # optional; defaults to the analysis agent when omitted
    name: security-patcher
  maxFindingsPerRun: 25
  suspend: false                 # pause scheduled incremental scans when true
```

**Spec fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `provider` | string | No | Source control provider. `github` is the supported v1 provider and default. |
| `repoURL` | string | Yes | Repository URL to scan. |
| `owner` | string | No | Repository owner or organization. Inferred from `repoURL` when omitted. |
| `repository` | string | No | Repository name. Inferred from `repoURL` when omitted. |
| `branch` | string | No | Base branch to scan. Defaults to the literal `main` when omitted (not resolved from the repository's actual default branch). Set this explicitly for repositories whose default branch is not `main`. |
| `ref` | string | No | Specific git ref, tag, or commit SHA to check out for scan tasks. When `ref` is set and `branch` is omitted, scan workspaces check out the ref directly instead of forcing `main`; PR remediation still uses `prBaseBranch` or `main` unless `branch` is set. |
| `subPath` | string | No | Optional subdirectory to scan in a monorepo. |
| `gitSecretRef` | LocalObjectReference | No | Compatibility source-read Secret for scan Tasks; `readCredentialRef` takes precedence when both are set. Never used for publication. |
| `readCredentialRef` | LocalObjectReference | For patches | Source clone/read Secret. Required, together with the three publication roles below, before any patch proposal or remediation PR. |
| `publicationReadCredentialRef` | LocalObjectReference | For patches | Target-repository read Secret used only for publication preflight and independent verification. |
| `publicationCredentialRef` | LocalObjectReference | For patches | Target-repository write Secret used only for the exact compare-and-swap branch push. |
| `forgeCredentialRef` | LocalObjectReference | For patches | Forge API Secret used for controller-side forge reads and PR upkeep: fetching the published remediation commit to derive and verify patch evidence, and reconciling/decorating the remediation pull request. Never mounted into any Task. The four patch roles must reference pairwise-distinct Secrets. |
| `forkRepo` | string | No | Writable fork repository URL for patch proposal branches and remediation PRs. |
| `prBaseBranch` | string | No | Pull request base branch for remediation. Defaults to `branch` when omitted. |
| `schedule` | string | No | Cron expression for scheduled incremental scans. |
| `timeZone` | string | No | IANA time zone used by `schedule`. |
| `historyDays` | int32 | No | How far back the initial scan should inspect repository history. |
| `validationMode` | string | No | Validation aggressiveness: `off`, `light`, or `full`. Defaults to `light`. |
| `validationMaxFindingsPerRun` | int32 | No | Maximum automatic validation tasks to enqueue per scan run in `light` mode. |
| `validationMinSeverity` | string | No | Minimum severity eligible for automatic validation. Defaults are mode-dependent. |
| `validationMinConfidence` | string | No | Minimum confidence eligible for automatic validation. Defaults are mode-dependent. |
| `customScanInstructionsRef` | PolicyConfigMapKeyRef | No | Same-namespace ConfigMap key containing additive scanner instructions. The ConfigMap must opt in with `orka.ai/security-policy: "true"` as a label or annotation. |
| `falsePositivePolicyRef` | PolicyConfigMapKeyRef | No | Same-namespace ConfigMap key containing additive false-positive policy text. The ConfigMap must opt in with `orka.ai/security-policy: "true"` as a label or annotation. |
| `analysisAgentRef` | AgentReference | Yes | Agent used for repository scan runs and threat model generation. |
| `patchAgentRef` | AgentReference | No | Agent used for patch proposal runs. |
| `maxFindingsPerRun` | int32 | No | Bounds accepted scan findings per run after validation and deterministic dropped-finding filters. |
| `suspend` | bool | No | Pauses scheduled incremental scans while preserving the scan configuration. |

`PolicyConfigMapKeyRef` uses `name` plus optional `key`; when `key` is omitted, Orka reads the `policy` key. Policy ConfigMap values are capped at 32 KiB and rejected if they look like they contain secrets, tokens, private keys, or credentials. Custom policy text is additive only; it cannot disable Orka's default evidence, no-secret, or finding-quality rules.

**Status fields:**

`status.phase`, `status.lastScanID`, `status.lastScanTaskName`, `status.lastSuccessfulScanAt`, `status.lastObservedHeadSHA`, `status.lastProcessedCommit`, `status.threatModelVersion`, `status.findingCounts`, and `status.conditions` summarize the latest scan lifecycle and open findings. Dynamic scan runs, threat models, findings, and patch proposals are stored by the controller and surfaced through the security API/UI rather than embedded directly in the CRD status.

### RepositoryMonitor

Durable GitHub pull request monitor configuration. A `RepositoryMonitor` is namespace-scoped and tells Orka which repository and branch to inspect, which Claude runtime Agent should review selected PR heads, and which labels or scheduling rules should control review selection.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: RepositoryMonitor
metadata:
  name: example-app
  namespace: default
spec:
  provider: github
  repoURL: "https://github.com/example/app"
  owner: example                 # optional; inferred from repoURL
  repository: app                # optional; inferred from repoURL
  branch: main
  gitSecretRef:                  # optional for private repositories or higher API rate limits
    name: repo-monitor-github
  schedule: "*/30 * * * *"      # optional cron expression
  timeZone: "UTC"               # optional IANA time zone
  suspend: false
  targets:
    pullRequests:
      enabled: true
      includeDrafts: false
      maxPerRun: 20
  agents:
    reviewer:
      name: repo-reviewer
  review:
    event: COMMENT              # legacy task input only; does not publish to GitHub
    staleReviewTTL: 24h
    exactEventEnabled: true     # queue exact-head runs from signed PR webhooks
    publish:
      enabled: true             # default false; controller-owned GitHub side effect
      mode: summary_with_inline_findings
      event: COMMENT            # V1 only supports neutral COMMENT reviews
      postPassed: false
      postNeedsChanges: true
      postNeedsHuman: true
      postSecuritySensitive: false
      sameHeadPolicy: skip
      inline:
        enabled: true
        minPriority: P2
        maxComments: 10
  policy:
    protectedLabels:
      - security-sensitive
    pauseLabels:
      - orka:pause
  validation:
    image: ghcr.io/example/app-validation@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa # must contain /bin/sh and the repository's validation tools
```

**Spec fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `provider` | string | No | Source control provider. `github` is the supported v1 provider and default. |
| `repoURL` | string | Yes | Credential-free GitHub repository root URL to monitor, such as `https://github.com/owner/repo`, `https://github.com/owner/repo.git`, or `git@github.com:owner/repo.git`. Pull request, issue, branch/tree, blob/file, commit, query-string, fragment, non-GitHub, HTTP, and embedded-credential URLs are rejected. |
| `owner` | string | No | Repository owner or organization. Inferred from `repoURL` when omitted. |
| `repository` | string | No | Repository name. Inferred from `repoURL` when omitted. |
| `branch` | string | No | Base branch used for pull request inventory. Defaults to `main`. |
| `gitSecretRef` | LocalObjectReference | No | Git Secret containing `token`, `password`, or `GITHUB_TOKEN` for GitHub API access and same-repository PR checkout. This is separate from the reviewer Agent's runtime credential Secret. |
| `schedule` | string | No | Cron expression for scheduled monitor runs. |
| `timeZone` | string | No | IANA time zone used by `schedule`. |
| `suspend` | bool | No | Pauses scheduled monitor runs while preserving the monitor configuration. |
| `targets.pullRequests.enabled` | bool | No | Enables pull request monitoring. Currently this must be true or omitted. |
| `targets.pullRequests.includeDrafts` | bool | No | Select draft pull requests for review when true. Defaults to false. |
| `targets.pullRequests.maxPerRun` | int32 | No | Maximum selected PRs per run. Defaults to `20`; allowed range is `1` to `100`. |
| `agents.reviewer` | AgentReference | Yes | Claude runtime Agent used for read-only PR review tasks. The Agent must reference a Secret in the monitor namespace with `ANTHROPIC_API_KEY` or `ANTHROPIC_FOUNDRY_API_KEY`. |
| `review.event` | string | No | Legacy/default review event value included in review task input. It does not publish to GitHub; use `review.publish.event`. Defaults to `COMMENT`. |
| `review.publish.enabled` | bool | No | Enables controller-owned GitHub pull request review publishing. Defaults to `false`. |
| `review.publish.mode` | string | No | `summary_only` or `summary_with_inline_findings`. Inline comments are only attempted for changed RIGHT-side diff lines. |
| `review.publish.event` | string | No | GitHub review event submitted by the controller. V1 only supports neutral `COMMENT` reviews; `APPROVE` and `REQUEST_CHANGES` are rejected. |
| `review.publish.postPassed` | bool | No | Post clean/passed reviews when true. Defaults to `false`. |
| `review.publish.postNeedsChanges` | bool | No | Post `needs_changes` reviews when true. Defaults to `true`. |
| `review.publish.postNeedsHuman` | bool | No | Post `needs_human` reviews when true. Defaults to `true`. |
| `review.publish.postSecuritySensitive` | bool | No | Allow public publishing of `security_sensitive` results. Defaults to `false`; sensitive findings are skipped by default. |
| `review.publish.sameHeadPolicy` | string | No | Duplicate policy for the same monitor, PR, and head SHA. V1 only supports `skip`. |
| `review.publish.inline.enabled` | bool | No | Enables inline GitHub review comments when `mode` is `summary_with_inline_findings`. |
| `review.publish.inline.minPriority` | string | No | Lowest priority eligible for inline comments (`P0`-`P3`). Defaults to `P2`; lower-priority findings remain in the summary. |
| `review.publish.inline.maxComments` | int32 | No | Max inline comments per GitHub review. Defaults to `10`, allowed range `0` to `50`. |
| `review.staleReviewTTL` | duration | No | Re-review an unchanged head after the previous accepted review is older than this duration. |
| `review.exactEventEnabled` | bool | No | Queue exact-head monitor runs from signed GitHub pull request webhook events when true. |
| `policy.protectedLabels` | list | No | PR labels that block automated review selection. |
| `policy.pauseLabels` | list | No | PR labels that pause monitor automation for that item. |
| `validation.image` | string | No | Container image for isolated pull request validation. The reviewer inspects the checked-out code and selects one shell command; Orka fixes the image and exact read-only PR head. The image must contain `/bin/sh` and every tool the repository may require. |

When `validation.image` is set, the reviewer must call `run_validation` once and wait for its child Task to finish before returning a `passed` verdict. A missing, failed, malformed, or stale validation Task blocks `passed` and merge-ready state. Orka records the image and status with the review and retains only a SHA-256 digest of the selected command in its durable validation binding. Validation stdout and stderr are suppressed so repository or fixture secrets cannot enter Pod logs or result storage. The command runs against a read-only checkout with deny-all ingress and egress, so the image must already contain every required tool and dependency. If a Go repository needs `golangci-lint`, for example, use a Go-based image that includes dependencies and `golangci-lint`. The same rule applies to offline Terraform, Azure CLI, or other repository-specific checks. Maintainers normally set this image once per `RepositoryMonitor`; commands, args, credentials, and network access are not part of the monitor configuration.

Before upgrading a monitor that uses the former `validation.mode` and `validation.commands` fields, replace them with `validation.image`. The CRD retains the old fields so the controller can detect existing policies, but it does not execute them. A non-empty legacy command list puts the monitor in `Error` with reason `LegacyValidationCommandsUnsupported`; Orka does not silently disable validation. Apply the updated CRD before upgrading the controller, then update each affected monitor with a digest-pinned image and remove the legacy fields.

`targets.issues`, durable `orka:*` label commands, issue triage/research/planning/implementation, PR review/repair, `review.requireGreenCI`, and optional head-bound automerge are active RepositoryMonitor workflows. `targets.commits` remain rejected until commit inventory is implemented. Review tasks check out the exact PR head and receive generated read-only context files under `/workspace/.git/orka/`: `pr-review.md`, `pr-review.files`, and `pr-review.diff`. GitHub publishing, branch pushes, PR creation, label consumption, and automerge attempts are controller-owned and audited through mutation records; read-only agents never receive the GitHub mutation token.

**Status fields:**

`status.phase`, `status.lastRunID`, `status.lastRunTime`, `status.lastSuccessfulRunTime`, `status.observedGeneration`, `status.openPullRequests`, `status.pendingReviews`, `status.activeRepairs`, `status.blockedItems`, `status.mergeReadyItems`, and `status.conditions` summarize the monitor lifecycle and current queue. Dynamic runs, PR items, review records, repair records, command events, and audit events are stored by the controller and surfaced through the monitor API/UI rather than embedded directly in CRD status.

See [Repository Monitors](../guides/repository-monitors.md) for the workflow, API examples, and current limits.

### Execution

Native `ai` and container Tasks support `spec.execution` for worker Pod runtime selection and placement. Built-in ACP agent Tasks do not accept per-Task placement, custom resources, or `spec.execution.workspace`; reviewed RuntimePool profiles own those settings.

```yaml
execution:
  runtimeClassName: gvisor
  nodeSelector:
    sandbox-runtime: gvisor
  tolerations:
    - key: sandbox-runtime
      operator: Equal
      value: gvisor
      effect: NoSchedule
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: sandbox-runtime
                operator: In
                values: ["gvisor"]
```

| Field | Type | Description |
|-------|------|-------------|
| `runtimeClassName` | string | Selects a Kubernetes `RuntimeClass` such as `gvisor` or `kata-qemu` |
| `nodeSelector` | map[string]string | Restricts native worker Pods to nodes with matching labels |
| `tolerations` | list | Allows native worker Pods onto tainted runtime-specific node pools |
| `affinity` | object | Adds Kubernetes affinity or anti-affinity rules for native worker Pods |
| `workspace` | object | Execution-workspace provider request. With workspace dispatch enabled, `provider: agent-sandbox` (no `templateRef`) or `provider: substrate` (with a required infrastructure `templateRef`) hosts the Task's RuntimeSession in a workspace-provider-backed RuntimePool; everything else fails closed. Repository access always uses top-level `Task.spec.workspace`. |

Resolution order:

- `Agent.spec.execution` provides defaults for tasks that reference the Agent
- `Task.spec.execution` overrides Agent defaults
- `runtimeClassName` is a scalar override
- `nodeSelector`, `tolerations`, and `affinity` replace Agent defaults when they are set on the Task

#### Execution Workspace Requests

`Task.spec.execution.workspace` requests a physical execution-workspace provider for the Task's ACP RuntimeSession. With `--acp-workspace-dispatch-enabled`, `provider: agent-sandbox` (requires `--agent-sandbox-enabled`; `templateRef` must be omitted) or `provider: substrate` (requires `--substrate-enabled`; an infrastructure `templateRef` is required) executes the RuntimeSession in a dedicated workspace-provider-backed RuntimePool. Unsupported options (`cleanupPolicy: retain`, boot/pool/snapshot/hibernation, `onDetach`) fail closed before any workspace or RuntimePool demand, with the reason projected to `Task.status.executionWorkspace`. There is no worker Job fallback and no harness-v1 fallback. Top-level `Task.spec.workspace` remains the verified source/publication contract.

See [Agent Sandbox Workspaces](agent-sandbox.md), [Agent Substrate Workspaces](substrate.md), and ADRs 0024/0025 for the provider-neutral contract.

#### SubstrateActorPool

`SubstrateActorPool` is an operator-owned pool of deterministic Substrate actors for pooled Task placement, MCP actor-backed Tools, and density reporting.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: SubstrateActorPool
metadata:
  name: codex-substrate-pool
spec:
  templateRef:
    name: orka-codex
    namespace: ate-demo
  workerPoolRef:
    name: orka-workers
    namespace: ate-demo
  targetActors: 4
  precreateActors: true
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `templateRef.name` | string | required | Substrate `ActorTemplate` used for pool members. |
| `templateRef.namespace` | string | Pool namespace | Namespace containing the `ActorTemplate`. |
| `workerPoolRef.name` | string | empty | Optional Substrate `WorkerPool` used for capacity and density reporting. |
| `workerPoolRef.namespace` | string | Pool namespace | Namespace containing the `WorkerPool`. |
| `targetActors` | integer | `0` | Desired stateful actor count, capped at `1000`. References from Tasks or Tools require at least `1`. |
| `precreateActors` | boolean | `false` | Pre-create deterministic warm actors up to `targetActors`. |

For built-in OpenCode Agents, `spec.model.name` must use literal
`provider/model` form and both `spec.model.contextWindow` and
`spec.model.maxTokens` are required positive reviewed ceilings, with
`contextWindow > maxTokens`. They are included in the immutable runtime profile;
Orka does not discover or guess them from a mutable model catalog.

### Provider Fallback Chain

You can configure fallback providers that are automatically tried when the primary provider fails (e.g., due to auth errors, provider outages, or rate limiting). Fallbacks are configured on the Agent CRD's `spec.model.fallbacks` field.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: resilient-agent
spec:
  providerRef:
    name: my-openai
  model:
    name: gpt-4o
    fallbacks:
      - providerRef: my-anthropic
        model: claude-sonnet-4-20250514
      - providerRef: my-azure-openai
        model: gpt-4o
```

#### How fallbacks work

1. The primary provider is tried first with automatic retries (exponential backoff on 429/5xx errors).
2. If the primary provider fails with an auth error (401/403), network error, or exhausts all retries, the first fallback provider is tried.
3. Each fallback provider also gets automatic retries.
4. If all providers fail, the last error is returned.

#### Fallback fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `providerRef` | string | Yes | Name of a Provider CRD to fall back to |
| `model` | string | No | Model to use with this provider. If empty, uses the provider's `defaultModel` |

#### Notes

- Fallbacks are only supported on Agent-based tasks. Agent-less tasks get retries only.
- Each fallback provider must have its own Provider CRD with a valid secret reference.
- Rate-limited providers (429 responses) are temporarily cooled down and skipped in subsequent requests.
- Streaming requests are retried/failed over only on the initial connection — mid-stream failures are not retried.

### Agent (with Runtime)

Agent configuration for the supported built-in ACP runtime profiles: Claude,
Codex, Copilot, and OpenCode. Built-in ACP Agents do not reference provider Secrets; RuntimePools reach
Vekil through the central authenticated provider proxy.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: claude-agent
spec:
  model:
    name: "claude-sonnet-4-20250514"
  systemPrompt:
    inline: "You are a senior software engineer."
  runtime:
    type: claude         # or "codex" / "copilot" / "opencode"
    contractVersion: orka.harness.v2
    defaultMaxTurns: 50
    defaultAllowBash: true
    defaultAllowedTools:
      - Read
      - Write
      - Edit
      - Bash
```

OpenCode Agents must omit `spec.systemPrompt` because the runtime cannot enforce
Agent-level prompts; put instructions in each Task's `spec.prompt` instead. OpenCode
model IDs use provider/model form, for example `openai/gpt-5.4`, and require reviewed
`contextWindow` and `maxTokens` values.

Operator-owned runtimes outside the built-in set can use `orka.harness.v2`
`AgentRuntime` registration and conformance. A current-generation ready,
strict-governed registration can be selected through `runtime.runtimeRef`;
Orka freezes and revalidates its endpoint, profile, authentication authority,
and observed runtime identity for dispatch.

Agent runtime tasks reference an Agent with `runtime` configured:

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: code-review
spec:
  type: agent
  agentRef:
    name: claude-agent
  prompt: "Review the code in this repo for security issues. Do not modify files."
  workspace:
    intent: read
    gitRepo: "https://github.com/example/repo.git"
    branch: main
    # readCredentialRef:
    #   name: repository-read
    # subPath: "services/api"
  agentRuntime:
    maxTurns: 100
    allowBash: true
    allowedTools:
      - Read
      - Write
      - Edit
      - Bash
      - Glob
      - Grep
```

### Skill

Reusable skill definitions (Agent Skills standard) that are referenced by Agents and AI Tasks.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Skill
metadata:
  name: skill-researcher
  labels:
    orka.ai/category: "research"
spec:
  displayName: "Research Methodology"
  description: "Structured research workflow and source validation guidance"
  version: "1.0.0"
  author: "platform-team"
  tags: ["research", "analysis"]
  content:
    inline: |
      # Research Skill
      Use primary sources and cite references.
    files:
      templates/checklist.md: |
        - [ ] Validate source credibility
        - [ ] Cross-check key claims
  # source tracks where a skill was imported from (for updates)
  # source:
  #   github: "anthropics/skills"
  #   skillName: "researcher"
status:
  phase: Ready
  contentHash: sha256:...
```

### Tool

Custom tool definitions for agents. Tools can call plain HTTP endpoints or MCP servers hosted in durable Substrate actors. Plain HTTP tools require `http.url` and support header-based or body-based auth injection.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Tool
metadata:
  name: tavily-search
spec:
  description: "Search the web for current information"
  parameters:
    type: object
    properties:
      query:
        type: string
        description: "Search query"
    required: ["query"]
  http:
    url: "https://api.tavily.com/search"
    method: POST
    timeout: 30s
    authSecretRef:
      name: tavily-secret
      key: api-key
    authInject: body     # "header" (Bearer token) or "body" (JSON key)
    authBodyKey: api_key # JSON key name when authInject=body
```

MCP actor-backed tools can also set `http.authSecretRef` for transport auth.
For these tools, `http.url` may be omitted because Orka uses the resolved actor
endpoint from Tool status. MCP transport auth must use header injection;
`authInject: body` is only valid for plain HTTP tools because MCP call arguments
are forwarded to the MCP server as tool input.

Example MCP actor-backed Tool:

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Tool
metadata:
  name: repo-inspector
spec:
  description: "Inspect repository metadata through an MCP server"
  parameters:
    type: object
    properties:
      message:
        type: string
    required:
      - message
  mcp:
    path: /mcp
    substrateActor:
      templateRef:
        name: orka-mcp
        namespace: ate-demo
      poolRef:
        name: mcp-substrate-pool
      boot: true
```

MCP actor-backed Tools require `mcp.substrateActor.templateRef.name`. `mcp.path`
defaults to `/mcp`, `poolRef` is optional, and `boot` only affects the first
actor resume. `spec.http` may be omitted unless the resolved actor endpoint
needs transport auth settings; when `spec.http` is present for MCP auth only,
omit `http.url`.

#### URL Path Interpolation

Tool CRD URLs can contain `{{paramName}}` placeholders that are replaced with parameter values at runtime. Interpolated values are URL path-escaped, and the matching parameters are removed from the request body. This is useful for REST APIs that require path parameters.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Tool
metadata:
  name: github-merge-pr
spec:
  description: "Merge a GitHub pull request"
  parameters:
    type: object
    properties:
      owner:
        type: string
      repo:
        type: string
      pull_number:
        type: integer
      merge_method:
        type: string
        enum: [merge, squash, rebase]
    required: [owner, repo, pull_number]
  http:
    url: "https://api.github.com/repos/{{owner}}/{{repo}}/pulls/{{pull_number}}/merge"
    method: PUT
    authSecretRef:
      name: github-token
      key: token
    authInject: header
```

In this example, `owner`, `repo`, and `pull_number` are interpolated into the URL path and removed from the JSON body. Only `merge_method` is sent in the request body.

### Provider

LLM provider configuration with credentials. Supports Anthropic, OpenAI, and Azure OpenAI.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Provider
metadata:
  name: anthropic-prod
spec:
  type: anthropic  # or "openai", "azure-openai"
  secretRef:
    name: anthropic-secret
    key: api-key
  baseURL: ""  # optional custom endpoint for proxies
  defaultModel: claude-sonnet-4-20250514
  # Azure-specific (only for type: azure-openai)
  # azure:
  #   deploymentName: my-deployment
  #   apiVersion: "2024-02-15-preview"
```

## Helm Chart

Key configuration values for the Helm chart:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `controller.replicas` | `1` | Controller replicas |
| `controller.image.repository` | `ghcr.io/orka-agents/orka` | Controller image |
| `controller.mode` | `harness-v2` | Static agent execution mode: `harness-v1` or `harness-v2`. Select v1 explicitly for a compatibility release; a release never serves both or changes mode in place. |
| `controller.watchNamespace` | required | One non-empty namespace labeled `orka.ai/controller-mode` with the matching mode. Cluster-wide watch is rejected. |
| `controller.enforceNamespaceIsolation` | `true` | Restrict namespace-bound API callers and default Helm RBAC to their namespace |
| `service.port` | `8080` | Controller Service port used by controller and Publisher in-cluster URLs. |
| `controller.apiPort` | `8080` | Controller container listener and Service target port. |
| `controller.metricsPort` | `8081` | Metrics endpoint port |
| `controller.healthPort` | `8082` | Health probe port |
| `controller.logLevel` | `info` | Log level (debug/info/warn/error) |
| `controller.acpRuntime.namespace` | `orka-runtimes` | Namespace for controller-owned RuntimePool workloads. |
| `controller.acpRuntime.providerProxyNamespace` | `""` | Compatibility guard for the chart-managed provider proxy. Leave empty or set exactly to the Helm release namespace; any other nonempty value is rejected when the proxy is enabled. |
| `controller.acpRuntime.codexImage` | `""` | Digest-pinned Codex ACP image; Tasks fail closed when empty. |
| `controller.acpRuntime.claudeImage` | `""` | Digest-pinned Claude ACP image; Tasks fail closed when empty. |
| `controller.acpRuntime.copilotImage` | `""` | Digest-pinned GitHub Copilot ACP image; Tasks fail closed when empty. |
| `controller.acpRuntime.opencodeImage` | `""` | Digest-pinned OpenCode ACP image; Tasks fail closed when empty. |
| `controller.acpRuntime.upgradeDrain.*` | enabled | Two-phase planned-upgrade admission closure and RuntimePool drain settings. |
| `harnessV1.image.digest` | `""` | Required immutable wrapper image digest for a `harness-v1` release. |
| `harnessV1.auth.existingSecret` | `""` | Dedicated v1 wrapper bearer/TLS Secret. Never share it with v2. |
| `providerProxy.enabled` | `false` | Deploy the authenticated provider boundary in front of Vekil. Required for built-in ACP profiles. |
| `providerProxy.upstreamBaseURL` | `http://vekil.vekil-system.svc:1337` | Exact supported Vekil upstream. An optional trailing slash is normalized; alternate hosts, namespaces, and ports are rejected to preserve the fixed NetworkPolicies. |
| `providerProxy.auth.existingSecret` | `""` | Existing current/optional-overlap proxy bearer Secret. RuntimePool copies are controller-managed. |
| `providerProxy.tokenReloadInterval` | `5s` | Atomic projected-Secret reload interval. Invalid generations fail readiness and forwarding closed. |
| `publisher.enabled` | `true` | Deploy the separate clean-room Workspace/Publisher service. |
| `publisher.image.repository` / `publisher.image.tag` | workspace publisher image / `latest` | Publisher image; production deployments should pin an immutable digest. |
| `publisher.allowedSCMHosts` | `github.com` | Exact lower-case SCM hosts accepted by both Publisher validation and the SCM egress proxy. |
| `publisher.auth.existingSecret` | `""` | Existing controller-auth/capability Secret for publisher operations. |
| `publisher.auth.rolloutNonce` | `""` | Non-secret revision marker that restarts controller and Publisher during coordinated publisher-auth Secret rotation. |
| `scmEgressProxy.enabled` | `true` | Require all Publisher Git and forge HTTPS traffic to traverse the dedicated authenticated proxy. The chart rejects `publisher.enabled=true` when this is false. |
| `scmEgressProxy.auth.existingSecret` | `""` | Existing Secret shared only by Publisher and proxy. Its token must contain 32-256 RFC 3986 unreserved characters. |
| `scmEgressProxy.auth.rolloutNonce` | `""` | Non-secret revision marker that restarts Publisher and SCM proxy during coordinated proxy-auth Secret rotation. |
| `scmEgressProxy.maxTunnelBytes` | `1073741824` | Maximum bytes allowed in each CONNECT tunnel direction. |
| `scmEgressProxy.maxConcurrent` | `8` | Maximum concurrent forward requests and CONNECT tunnels. |
| `webhooks.tls.existingSecret` | `""` | Required existing TLS Secret for the controller-served admission webhooks; the chart never generates webhook certificates. |
| `webhooks.tls.certKey` / `webhooks.tls.privateKeyKey` | `tls.crt` / `tls.key` | Certificate and private-key keys inside the webhook TLS Secret. |
| `webhooks.caBundle` | `""` | Base64-encoded PEM CA bundle for the chart ValidatingWebhookConfiguration. Leave empty when `webhooks.caInjectionAnnotations` configures an injector. |
| `webhooks.caInjectionAnnotations` | `{}` | CA-injection annotations (for example cert-manager) placed on the chart ValidatingWebhookConfiguration. Rendering fails unless this or `webhooks.caBundle` is set. |
| `webhooks.timeoutSeconds` | `10` | Admission webhook timeout. |
| `controller.agentSandbox.enabled` | `false` | Enable experimental workspace-backed execution for agent Tasks that set `execution.workspace` |
| `controller.agentSandbox.routerUrl` | `""` | Optional upstream agent-sandbox router base URL used for workspace claims |
| `controller.agentSandbox.defaultTemplate` | `""` | Default agent-sandbox `SandboxWarmPool` name when a Task omits `templateRef.name` |
| `controller.agentSandbox.warmPoolPolicy` | `disabled` | Legacy compatibility setting: `disabled` or `template`; v1 claims use `SandboxWarmPool` references |
| `controller.agentSandbox.namespaceStrategy` | `task` | Sandbox resource namespace strategy: `task` or `controller` |
| `controller.agentSandbox.claimTimeout` | `2m` | Timeout for workspace claim and readiness operations |
| `controller.agentSandbox.commandTimeout` | `30m` | Timeout for agent runtime execution inside the sandbox |
| `controller.agentSandbox.cleanupPolicy` | `delete` | Default workspace cleanup policy: `delete` or `retain` |
| `workers.ai.image.repository` | `ghcr.io/orka-agents/orka/ai-worker` | AI worker image |
| `workers.general.image.repository` | `ghcr.io/orka-agents/orka/general-worker` | General worker image |
| `service.type` | `ClusterIP` | Service type |
| `client.create` | `true` | Create client ServiceAccount for API access |
| `client.name` | `orka-client` | Client ServiceAccount name |
| `client.namespace` | `""` | Client ServiceAccount namespace override. Empty defaults to `controller.watchNamespace` when namespace isolation is enforced and `watchNamespace` is set, otherwise the release namespace. |

### Helm authentication Secret rotation

The Publisher, SCM proxy, and controller clients load these credentials at process startup. Update the Secret and bump its corresponding nonce in the same Helm upgrade: use `publisher.auth.rolloutNonce` for the publisher-auth Secret, and `scmEgressProxy.auth.rolloutNonce` for the SCM proxy-auth Secret. The publisher nonce is applied only to controller and Publisher Pod templates; the SCM nonce is applied only to Publisher and SCM proxy Pod templates. Nonces are safe revision strings, not Secret values. Coordinated rotation can briefly fail closed while Pods roll but prevents stale or split credential generations from persisting.

### Canonical Kustomize overlay

Direct Kustomize deployments must use:

```bash
kubectl apply -k config/acp-production
```

`config/acp-production` composes the CRD-free `config/acp-workload` base with the cross-namespace Vekil
ingress NetworkPolicy. Applying `config/default` alone omits the boundary that
prevents runtime Pods from bypassing the authenticated provider proxy. Configure
the required system Secrets and digest-pinned controller, Publisher, Codex,
Claude, Copilot, and OpenCode images before applying the overlay. `make deploy` validates those image
references and applies the equivalent resource set.

### ACP storage split

The `--store-backend=sqlite` flag does not make SQLite authoritative for ACP
control transitions. `ControllerEpoch`, `PromptAttempt`,
`RuntimeSessionControl`, `BranchClaim`, `Publication`, and `ExternalEffect`
status plus coordination Leases are the control authority. SQLite stores
transcript/SessionTurn payloads, deferred outbox projections, and artifacts
behind those Kubernetes fences.


For the Kustomize deployment, create the proxy-auth Secret before applying
`config/acp-production` (the Secret is intentionally not stored in Git):

```bash
token="$(openssl rand -hex 32)"
kubectl -n orka-system create secret generic scm-egress-proxy-auth \
  --from-literal=token="$token"
unset token
```

The token is an ingress credential for the proxy only; it is not a Git or forge credential. `config/publisher` sets `HTTPS_PROXY`/`NO_PROXY`, and the Publisher copies only those validated proxy variables into its otherwise empty Git subprocess environments. The Publisher NetworkPolicy has no public `0.0.0.0/0` rule. Only the SCM egress proxy may reach public port 443.

`cmd/orka-workspace-publisher` also requires `ORKA_PUBLISHER_ARTIFACT_AUTHORIZATION_BROKER_URL`, `ORKA_PUBLISHER_CREDENTIAL_BROKER_URL`, and `ORKA_PUBLISHER_SCM_EGRESS_PROXY_REQUIRED=true` in normal startup. For isolated local tests only, `ORKA_PUBLISHER_ALLOW_DEVELOPMENT_FALLBACKS=true` permits the legacy local artifact signing key, filesystem credential root, and proxy-less mode. Do not set that flag in cluster manifests.

When `ORKA_PUBLISHER_PUBLISH_TIMEOUT` is raised above the default on the Publisher, set the same value on the controller Deployment as well. The controller bounds each publisher-backed external-effect call and sizes the effect's ledger lease from this timeout plus a settlement margin; without the controller-side value, publisher operations are clamped to the default four-minute call bound. Brokered custom-Tool calls always keep the fixed four-minute clamp that their Tool descriptors were admitted under.

### Helm CRD lifecycle

CRD behavior is not controlled through chart values. A fresh install creates all CRDs in the chart unless `--skip-crds` is used. Because CRDs are cluster-scoped, designate one lifecycle owner and use `--skip-crds` for other Orka releases. Helm does not update CRDs during `helm upgrade`; apply the CRDs from the exact target chart before upgrading the controller. Helm retains CRDs and Orka custom resources on uninstall. See the [Helm CRD lifecycle guide](https://github.com/orka-agents/orka/blob/main/charts/orka/README.md).

Harness v1 and v2 use separate releases, endpoints, watched namespaces, RBAC,
Leases, stores, and data planes. They do not migrate Tasks or continue Sessions
across modes. See [Operating harness v1 and v2 on one cluster](../operations/harness-modes.md).

Context-token flags can also be configured through Helm under
`controller.contextToken`. For example:

```yaml
controller:
  contextToken:
    profile: transaction-token
    issuer: https://issuer.example.com
    audience: orka
    headers: Txn-Token
    authzMode: enforce
    scopes:
      taskCreate: orka:tasks:create
      providerUse: orka:providers:use
      toolUse: orka:tools:use
      secretCredentialRead: orka:secrets:credentials:read
      monitorRead: orka:monitors:read
      monitorWrite: orka:monitors:write
      monitorOperate: orka:monitors:operate
      gatewayRead: orka:gateways:read
      gatewayOperate: orka:gateways:operate
    tts:
      endpoint: https://tts.example.com/oauth/token
      audience: orka-workers
      timeout: 5s
      tokenSource: serviceAccount
      childScope: orka:tasks:create
      outboundScope: orka:tools:use
      childTokenTTL: 5m
      toolTokenTTL: 2m
  outboundAccess:
    trustedGatewayServices:
      - gateway-system/agentgateway:8080
    trustedTokenEndpointServices:
      - identity-system/token-service:8443
```

The Helm keys mirror the controller flags: for example,
`controller.contextToken.jwksUrl` renders `--context-token-jwks-url`,
`controller.contextToken.scopes.secretRead` renders
`--context-token-secret-read-scopes`,
`controller.contextToken.scopes.secretCredentialRead` renders
`--context-token-secret-credential-read-scopes`,
`controller.contextToken.scopes.monitorRead` renders
`--context-token-monitor-read-scopes`,
`controller.contextToken.scopes.gatewayRead` renders
`--context-token-gateway-read-scopes`, and
`controller.contextToken.tts.toolTokenTTL` renders
`--context-token-tool-token-ttl`.

See [charts/orka/values.yaml](https://github.com/orka-agents/orka/blob/main/charts/orka/values.yaml) for the full list.

## Controller Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--api-port` | `8080` | REST API server port |
| `--gateway-enabled` | `true` | Enable generic gateway reconciliation and ingress |
| `--gateway-pending-per-session` | `100` | Maximum pending gateway events per Session |
| `--gateway-max-records-per-gateway` | `1000` | Maximum retained accepted/dead-letter event records per Gateway before ingress is throttled |
| `--gateway-max-rejected-records-per-gateway` | `250` | Separate audit budget for rejected events so unauthorized traffic cannot consume operational capacity |
| `--gateway-event-expiry` | `24h` | Queue and delivery retry expiry |
| `--gateway-terminal-retention` | `720h` | Terminal event and delivery retention |
| `--gateway-delivery-timeout` | `15s` | One synchronous adapter delivery timeout |
| `--gateway-delivery-max-attempts` | `10` | Delivery attempts before dead-lettering |
| `--gateway-claim-lease` | `1m` | Event and delivery claim lease |
| `--gateway-poll-interval` | `500ms` | Dispatcher and delivery poll interval |
| `--gateway-batch-size` | `25` | Maximum gateway records processed per iteration |
| `--controller-mode` / `ORKA_CONTROLLER_MODE` | required | Static controller mode: `harness-v1` or `harness-v2`. `dual`, `auto`, and drain modes are rejected. |
| `--watch-namespace` | required | One non-empty watched namespace carrying the matching `orka.ai/controller-mode` label. |
| `--enforce-namespace-isolation` | `false` | Restrict users to their ServiceAccount's namespace |
| `--max-tasks-per-namespace` | `0` | Max active tasks per namespace (0 = unlimited) |
| `--agent-sandbox-enabled` | `ORKA_AGENT_SANDBOX_ENABLED` env or `false` | Admit the agent-sandbox execution-workspace provider for agent Tasks that set `execution.workspace` |
| `--acp-workspace-dispatch-enabled` | `ORKA_ACP_WORKSPACE_DISPATCH_ENABLED` env or `false` | Admit workspace-provider-backed ACP RuntimeSession dispatch; when false, workspace-backed agent Tasks fail closed |
| `--agent-sandbox-router-url` | `ORKA_AGENT_SANDBOX_ROUTER_URL` env or `""` | Optional upstream agent-sandbox router base URL used for workspace claims |
| `--agent-sandbox-default-template` | `ORKA_AGENT_SANDBOX_DEFAULT_TEMPLATE` env or `""` | Default agent-sandbox `SandboxWarmPool` name when a Task omits `templateRef.name` |
| `--agent-sandbox-warm-pool-policy` | `ORKA_AGENT_SANDBOX_WARM_POOL_POLICY` env or `disabled` | Legacy compatibility setting: `disabled` or `template`; v1 claims use `SandboxWarmPool` references |
| `--agent-sandbox-namespace-strategy` | `ORKA_AGENT_SANDBOX_NAMESPACE_STRATEGY` env or `task` | Sandbox resource namespace strategy: `task` or `controller` |
| `--agent-sandbox-claim-timeout` | `ORKA_AGENT_SANDBOX_CLAIM_TIMEOUT` env or `2m` | Timeout for workspace claim and readiness operations |
| `--agent-sandbox-command-timeout` | `ORKA_AGENT_SANDBOX_COMMAND_TIMEOUT` env or `30m` | Timeout for agent runtime execution inside the sandbox |
| `--agent-sandbox-cleanup-policy` | `ORKA_AGENT_SANDBOX_CLEANUP_POLICY` env or `delete` | Default workspace cleanup policy: `delete` or `retain` |
| `--controller-url` | `""` | Base URL workers use to reach the controller API (e.g., `http://orka-api.orka-system.svc:8080`). Required for worker result callbacks and session transcript fetching |
| `--oidc-issuer` | `ORKA_OIDC_ISSUER` env or `""` | OIDC issuer URL for external API bearer token validation. Requires `--oidc-audience` when set |
| `--oidc-audience` | `ORKA_OIDC_AUDIENCE` env or `""` | Expected OIDC audience for external API bearer tokens. Requires `--oidc-issuer` when set |
| `--oidc-jwks-url` | `ORKA_OIDC_JWKS_URL` env or `""` | Optional JWKS URL. When empty, Orka discovers it from the issuer metadata |
| `--oidc-allowed-subjects` | `ORKA_OIDC_ALLOWED_SUBJECTS` env or `""` | Required comma-separated OIDC subject allowlist patterns when OIDC is enabled |
| `--oidc-namespace` | `ORKA_OIDC_NAMESPACE` env or `default` | Namespace assigned to authorized OIDC callers for namespace isolation |
| `--context-token-profile` | `ORKA_CONTEXT_TOKEN_PROFILE` env or `""` | Context-token profile for external API requests. Currently supports `transaction-token` |
| `--context-token-issuer` | `ORKA_CONTEXT_TOKEN_ISSUER` env or `""` | Context-token issuer URL. Requires `--context-token-profile` and `--context-token-audience` when set |
| `--context-token-audience` | `ORKA_CONTEXT_TOKEN_AUDIENCE` env or `""` | Expected context-token audience. Requires `--context-token-profile` and `--context-token-issuer` when set |
| `--context-token-jwks-url` | `ORKA_CONTEXT_TOKEN_JWKS_URL` env or `""` | Optional context-token JWKS URL. For `transaction-token`, defaults to `<issuer>/.well-known/jwks.json` |
| `--context-token-headers` | `ORKA_CONTEXT_TOKEN_HEADERS` env or `""` | Comma-separated context-token header locations. Use `Header` for raw tokens or `Header:Scheme` for scheme-prefixed tokens. The `transaction-token` default is `Txn-Token` |
| `--context-token-authz-mode` | `ORKA_CONTEXT_TOKEN_AUTHZ_MODE` env or `""` | Context-token authorization mode: `off`, `audit`, or `enforce`. Empty defaults to `off` |
| `--context-token-task-create-scopes` | `ORKA_CONTEXT_TOKEN_TASK_CREATE_SCOPES` env or `""` | Comma-separated scopes authorizing Task creation. Defaults to `orka:tasks:create` |
| `--context-token-task-read-scopes` | `ORKA_CONTEXT_TOKEN_TASK_READ_SCOPES` env or `""` | Comma-separated scopes authorizing Task reads and related data. Defaults to `orka:tasks:get` |
| `--context-token-task-list-scopes` | `ORKA_CONTEXT_TOKEN_TASK_LIST_SCOPES` env or `""` | Comma-separated scopes authorizing Task listing. Defaults to `orka:tasks:list` |
| `--context-token-task-delete-scopes` | `ORKA_CONTEXT_TOKEN_TASK_DELETE_SCOPES` env or `""` | Comma-separated scopes authorizing Task deletion. Defaults to `orka:tasks:delete` |
| `--context-token-tool-read-scopes` | `ORKA_CONTEXT_TOKEN_TOOL_READ_SCOPES` env or `""` | Comma-separated scopes authorizing Tool reads. Defaults to `orka:tools:read` |
| `--context-token-tool-use-scopes` | `ORKA_CONTEXT_TOKEN_TOOL_USE_SCOPES` env or `""` | Comma-separated scopes authorizing Orka-managed chat/OpenAI/Anthropic tool execution. Defaults to `orka:tools:use` |
| `--context-token-provider-use-scopes` | `ORKA_CONTEXT_TOKEN_PROVIDER_USE_SCOPES` env or `""` | Comma-separated scopes authorizing chat/OpenAI/Anthropic model-provider use and model listing. Defaults to `orka:providers:use` |
| `--context-token-secret-read-scopes` | `ORKA_CONTEXT_TOKEN_SECRET_READ_SCOPES` env or `""` | Comma-separated scopes authorizing Secret metadata reads. Defaults to `orka:secrets:read` |
| `--context-token-secret-credential-read-scopes` | `ORKA_CONTEXT_TOKEN_SECRET_CREDENTIAL_READ_SCOPES` env or `""` | Comma-separated scopes authorizing Secret data or ServiceAccount tokens as outbound credentials. Defaults to `orka:secrets:credentials:read` |
| `--context-token-agent-read-scopes` | `ORKA_CONTEXT_TOKEN_AGENT_READ_SCOPES` env or `""` | Comma-separated scopes authorizing Agent reads. Defaults to `orka:agents:read` |
| `--context-token-agent-write-scopes` | `ORKA_CONTEXT_TOKEN_AGENT_WRITE_SCOPES` env or `""` | Comma-separated scopes authorizing Agent writes. Defaults to `orka:agents:write` |
| `--context-token-memory-read-scopes` | `ORKA_CONTEXT_TOKEN_MEMORY_READ_SCOPES` env or `""` | Comma-separated scopes authorizing memory reads. Defaults to `orka:memory:read` |
| `--context-token-memory-write-scopes` | `ORKA_CONTEXT_TOKEN_MEMORY_WRITE_SCOPES` env or `""` | Comma-separated scopes authorizing memory writes. Defaults to `orka:memory:write` |
| `--context-token-session-read-scopes` | `ORKA_CONTEXT_TOKEN_SESSION_READ_SCOPES` env or `""` | Comma-separated scopes authorizing session reads. Defaults to `orka:sessions:read` |
| `--context-token-session-write-scopes` | `ORKA_CONTEXT_TOKEN_SESSION_WRITE_SCOPES` env or `""` | Comma-separated scopes authorizing session writes/deletes. Defaults to `orka:sessions:write` |
| `--context-token-security-read-scopes` | `ORKA_CONTEXT_TOKEN_SECURITY_READ_SCOPES` env or `""` | Comma-separated scopes authorizing security scan reads. Defaults to `orka:security:read` |
| `--context-token-security-write-scopes` | `ORKA_CONTEXT_TOKEN_SECURITY_WRITE_SCOPES` env or `""` | Comma-separated scopes authorizing security scan creates, updates, deletes, and other mutations. Defaults to `orka:security:write` |
| `--context-token-monitor-read-scopes` | `ORKA_CONTEXT_TOKEN_MONITOR_READ_SCOPES` env or `""` | Comma-separated scopes authorizing repository monitor reads. Defaults to `orka:monitors:read` |
| `--context-token-monitor-write-scopes` | `ORKA_CONTEXT_TOKEN_MONITOR_WRITE_SCOPES` env or `""` | Comma-separated scopes authorizing repository monitor create, update, and delete operations. Defaults to `orka:monitors:write` |
| `--context-token-monitor-operate-scopes` | `ORKA_CONTEXT_TOKEN_MONITOR_OPERATE_SCOPES` env or `""` | Comma-separated scopes authorizing repository monitor manual runs. Defaults to `orka:monitors:operate` |
| `--context-token-skill-read-scopes` | `ORKA_CONTEXT_TOKEN_SKILL_READ_SCOPES` env or `""` | Comma-separated scopes authorizing Skill reads. Defaults to `orka:skills:read` |
| `--context-token-skill-write-scopes` | `ORKA_CONTEXT_TOKEN_SKILL_WRITE_SCOPES` env or `""` | Comma-separated scopes authorizing Skill writes. Defaults to `orka:skills:write` |
| `--context-token-gateway-read-scopes` | `ORKA_CONTEXT_TOKEN_GATEWAY_READ_SCOPES` env or `""` | Comma-separated scopes authorizing gateway resource and ledger reads. Defaults to `orka:gateways:read` |
| `--context-token-gateway-operate-scopes` | `ORKA_CONTEXT_TOKEN_GATEWAY_OPERATE_SCOPES` env or `""` | Comma-separated scopes authorizing dead-lettered delivery retries. Defaults to `orka:gateways:operate` |
| `--context-token-tts-endpoint` | `ORKA_CONTEXT_TOKEN_TTS_ENDPOINT` env or `""` | Exact transaction-token TTS OAuth endpoint for optional exchange/replacement |
| `--context-token-tts-audience` | `ORKA_CONTEXT_TOKEN_TTS_AUDIENCE` env or `""` | Audience requested from transaction-token TTS exchanges |
| `--context-token-tts-timeout` | `ORKA_CONTEXT_TOKEN_TTS_TIMEOUT` env or `""` | Timeout for transaction-token TTS exchanges. Defaults to `5s` when TTS is enabled |
| `--context-token-tts-token-source` | `ORKA_CONTEXT_TOKEN_TTS_TOKEN_SOURCE` env or `""` | Subject token source for TTS exchanges: `serviceAccount`, `incoming`, or `none`. Defaults to `serviceAccount` when TTS is enabled |
| `--context-token-subject-token-type` | `ORKA_CONTEXT_TOKEN_SUBJECT_TOKEN_TYPE` env or `""` | Subject token type for worker-side TTS exchanges. Workers default to TxToken subject tokens when empty |
| `--context-token-child-scope` | `ORKA_CONTEXT_TOKEN_CHILD_SCOPE` env or `""` | Scope workers request for child delegated TxTokens when TTS is configured |
| `--context-token-outbound-scope` | `ORKA_CONTEXT_TOKEN_OUTBOUND_SCOPE` env or `""` | Scope workers request for outbound HTTP Tool TxTokens when TTS is configured |
| `--context-token-child-token-ttl` | `ORKA_CONTEXT_TOKEN_CHILD_TOKEN_TTL` env or `""` | Requested TTL for child delegation TxTokens. Defaults to `5m` when TTS is enabled |
| `--context-token-tool-token-ttl` | `ORKA_CONTEXT_TOKEN_TOOL_TOKEN_TTL` env or `""` | Requested TTL for outbound tool TxTokens. Defaults to `2m` when TTS is enabled |
| `--outbound-access-trusted-gateway-services` | `ORKA_OUTBOUND_ACCESS_TRUSTED_GATEWAY_SERVICES` env or `""` | Comma-separated exact `namespace/name:port` cross-namespace gateway Service refs; wildcards are rejected |
| `--outbound-access-trusted-token-endpoint-services` | `ORKA_OUTBOUND_ACCESS_TRUSTED_TOKEN_ENDPOINT_SERVICES` env or `""` | Comma-separated exact `namespace/name:port` cross-namespace token endpoint Service refs; wildcards are rejected |
| `--task-provenance-admission-enabled` | `ORKA_TASK_PROVENANCE_ADMISSION_ENABLED` env or `false` | Enable validating admission that rejects untrusted direct Kubernetes Task writes to Orka-managed provenance fields (`spec.requestedBy`, `spec.transaction`, and transaction metadata labels/annotations) |
| `--task-provenance-admission-trusted-users` | `ORKA_TASK_PROVENANCE_ADMISSION_TRUSTED_USERS` env or controller ServiceAccount usernames | Comma-separated Kubernetes usernames trusted to set Orka-managed Task provenance fields |
| `--task-provenance-admission-trusted-service-accounts` | `ORKA_TASK_PROVENANCE_ADMISSION_TRUSTED_SERVICE_ACCOUNTS` env or `orka-ai-worker` | Comma-separated ServiceAccount names trusted in the target Task namespace to set Orka-managed Task provenance fields for child Task creation |
| `--ai-worker-image` | `ghcr.io/orka-agents/orka/ai-worker:latest` | Native AI worker container image |
| `--acp-runtime-namespace` / `ORKA_ACP_RUNTIME_NAMESPACE` | `orka-runtimes` | Namespace for managed runtime Deployments, Services, Secrets, and policies. |
| `--acp-provider-proxy-namespace` / `ORKA_ACP_PROVIDER_PROXY_NAMESPACE` | `vekil-system` | Approved provider-proxy namespace selector. |
| `--acp-provider-proxy-base-url` / `ORKA_ACP_PROVIDER_PROXY_BASE_URL` | unset | Authenticated provider-proxy URL injected into built-in RuntimePools. |
| `--acp-provider-proxy-pod-labels` / `ORKA_ACP_PROVIDER_PROXY_POD_LABELS` | `orka.ai/network-role=provider-auth-proxy` | Exact Pod labels selected by RuntimePool egress policy. |
| `--acp-provider-proxy-token-file` / `ORKA_ACP_PROVIDER_PROXY_TOKEN_FILE` | unset | Controller-mounted bearer file copied into generation-scoped immutable RuntimePool Secrets. |
| `--acp-codex-runtime-image` / `ORKA_ACP_CODEX_RUNTIME_IMAGE` | unset | Required digest-pinned Codex runtime image when Codex Tasks are used. |
| `--acp-claude-runtime-image` / `ORKA_ACP_CLAUDE_RUNTIME_IMAGE` | unset | Required digest-pinned Claude runtime image when Claude Tasks are used. |
| `--acp-copilot-runtime-image` / `ORKA_ACP_COPILOT_RUNTIME_IMAGE` | unset | Required digest-pinned GitHub Copilot runtime image when Copilot Tasks are used. |
| `--acp-opencode-runtime-image` / `ORKA_ACP_OPENCODE_RUNTIME_IMAGE` | unset | Required digest-pinned OpenCode runtime image when OpenCode Tasks are used. |
| `--general-worker-image` | `ghcr.io/orka-agents/orka/general-worker:latest` | General worker container image |
| `--store-backend` | `sqlite` | Payload/read-model backend. ACP control authority remains Kubernetes CRDs and Leases. |
| `--store-path` | `/data/orka.db` | Path to the SQLite transcript/outbox/artifact database file. |

| `--task-provenance-admission-trusted-service-accounts` | `ORKA_TASK_PROVENANCE_ADMISSION_TRUSTED_SERVICE_ACCOUNTS` env or configured AI/vendor worker ServiceAccounts | Comma-separated ServiceAccount names trusted in the target Task namespace to set Orka-managed Task provenance fields for child Task creation. Explicit values override the worker ServiceAccount defaults. |
| `--ai-worker-image` | `ghcr.io/orka-agents/orka/ai-worker:latest` | AI worker container image |
| `--ai-worker-service-account-name` | `orka-ai-worker` | ServiceAccount name for AI worker Jobs and dynamically ensured worker RBAC |
| `--vendor-worker-service-account-name` | `orka-vendor-worker` | ServiceAccount name for vendor/agent worker Jobs and dynamically ensured worker RBAC |
| `--container-worker-service-account-name` | `orka-container-worker` | ServiceAccount name for container worker Jobs and dynamically ensured worker RBAC |
| `--store-backend` | `sqlite` | Storage backend (sqlite) |
| `--store-path` | `/data/orka.db` | Path to SQLite database file |
| `--chat-enabled` | `true` | Enable the chat endpoint |
| `--chat-provider` | `""` | Default Provider CRD name for chat |
| `--chat-model` | `""` | Default model for chat |
| `--chat-max-iterations` | `50` | Max tool execution loops per chat request |
| `--chat-max-duration` | `30m` | Max wall-clock time per chat request |
| `--chat-tool-timeout` | `60s` | Max time for single tool execution |
| `--chat-max-concurrent` | `10` | Max concurrent chat sessions |
| `--chat-max-tasks-per-turn` | `5` | Max tasks created per chat turn |
| `--chat-max-session-size` | `512000` | Soft limit for session size before truncation (bytes) |
| `--leader-elect` | `false` | Enable leader election. Static controller installations require `true`; the Lease is stored in the watched namespace. |
| `--metrics-bind-address` | `0` | Metrics endpoint address |
| `--health-probe-bind-address` | `:8081` | Health probe address |
| `--metrics-secure` | `true` | Serve metrics via HTTPS |
| `--enable-http2` | `false` | Enable HTTP/2 for metrics and webhook servers |
| `--enable-telemetry` / `--enable-tracing` | `false` | Enable OpenTelemetry traces and metrics (requires worker-reachable OTLP endpoint for worker telemetry) |

### Provider-neutral Workspace Controller Settings

The `workspace.orka.ai/v1alpha1` control plane is installed additively and its controllers are disabled by default during rollout. Enable the generic provider, class, pool, and workspace reconcilers with `--enable-workspace-provider-api` (or `ORKA_ENABLE_WORKSPACE_PROVIDER_API=true`). Enabling the workspace provider API also requires `--task-provenance-admission-enabled=true`: the manager fails startup otherwise, because the Task provenance admission webhook is what protects the reserved `acp.workspace.orka.ai/` workspace settlement metadata on Tasks from untrusted direct writes. Install the provenance webhook (the controller-served webhook or the dedicated admission runtime) alongside the flag so that protection is actually enforced. The development-only fake adapter additionally requires `--enable-fake-workspace-provider` (or `ORKA_ENABLE_FAKE_WORKSPACE_PROVIDER=true`). These are controller flags/environment variables only; the Helm chart does not expose values for them. The release chart intentionally excludes the fake adapter's two CRDs; before enabling it, install the development package from a matching source checkout with `bin/kustomize build --load-restrictor LoadRestrictionsNone config/development/fake-workspace-provider | kubectl apply -f -`.

Helm installs files from a chart's `crds/` directory on a fresh install but does not upgrade an existing CRD schema. Before enabling the workspace provider API on an upgraded cluster, apply the current chart CRDs explicitly, for example with `helm show crds <chart> | kubectl apply --server-side -f -`, so the `workspace.orka.ai` schemas match the controller.

Task and Tool `classRef` selection is always protected by shipped `ValidatingAdmissionPolicy` resources that perform a live Kubernetes `use` authorization check, even while workspace execution gates are disabled. When the workspace provider API is enabled, the manager also requires the TLS-backed `--workspace-class-use-admission-enabled` webhook as defense in depth. The webhooks submit a Kubernetes `SubjectAccessReview` for the live admission caller using verb `use` on the selected namespaced `ExecutionWorkspaceClass`; requests are denied when the SAR is denied or unavailable. How the class-use webhooks are installed depends on the installation method. A `harness-v2` Helm release installs them automatically: the chart renders the fail-closed `task-workspace-class.harness-v2.orka.ai` and `tool-workspace-class.harness-v2.orka.ai` webhooks against the release-local controller webhook Service and runs the controller with `--workspace-class-use-admission-enabled=true`; rendering requires `webhooks.tls.existingSecret` plus either `webhooks.caBundle` or `webhooks.caInjectionAnnotations`. Do not additionally apply the Kustomize admission packages to a Helm release; that installs a duplicate second set of validating webhooks. Kustomize installations instead serve the class-use webhooks (`taskworkspaceclassuse.core.orka.ai` and `toolworkspaceclassuse.core.orka.ai`) from the dedicated admission runtime: install `config/orka-admission` first, then apply the fail-closed `config/orka-admission-webhooks` configuration after the readiness and TLS prerequisites in its README are met.

Task and Tool users select namespaced `ExecutionWorkspaceClass` objects. Provider identity, provider-specific parameters, pool implementation, and provider versions remain operator-owned. The legacy direct Agent Sandbox and Substrate settings below remain available during migration.

Agent Tasks may bind their ACP RuntimeSession to a class through `Task.spec.execution.workspace.classRef` (optionally with `reusePolicy`, `workspaceSlot`, and `onDetach`; `workspaceSlot` composes with `reusePolicy: none`, while session reuse supports only the default slot and fails closed for other values until RuntimeSession controls are slot-scoped). The class must resolve to an `ExecutionWorkspaceProvider` carrying the reserved in-tree adapter identity `controllerName: acp.workspace.orka.ai/runtime-pool`, whose cluster-scoped `RuntimeProviderConfig` parameters select the `agent-sandbox` or `substrate` backend, and whose namespaced `RuntimeWorkspaceProfile` class parameters carry the operator-owned backend inputs (a substrate profile names the infrastructure ActorTemplate and may set `substrate.suspend`; an agent-sandbox profile has no required fields — it stays empty unless the class should permit PVC-backed cold suspension, in which case it sets `agentSandbox.suspend` with `mode: DataOnly` and the frozen durable `volume` shape (`capacity`, optional `storageClassName` and `accessModes`) described below). Class-backed dispatch obeys the same `--acp-workspace-dispatch-enabled` plus provider-flag gates as the legacy request shape, additionally requires `--enable-workspace-provider-api`, and freezes the class identity, profile hash, lifecycle, and effective detach action into the immutable execution snapshot. The `Delete` detach action is always executable. Substrate `Suspend` is currently contract-only. A session-reused substrate class may declare `substrate.suspend.mode: DataOnly`, and the derived ActorTemplate carries a controller-owned `DurableDir` workspace volume with an explicit `onPause: Data`, `onCommit: Data`, `onResume.fromData: ColdBoot` snapshot policy. The current in-tree client rejects such pools before actor creation because it cannot provide the immutable checkpoint fence described below. A future fencing-capable client can preserve only repository/workspace data, never process memory or credentials, and cold-resume it with a fresh boot and repeated signed credential bootstrap. Agent-sandbox classes permit `Suspend` through `agentSandbox.suspend` with a frozen durable workspace PVC shape: the pool's SandboxClaim requests that PVC (forcing a cold start instead of warm-pool adoption), suspension patches the exact adopted Sandbox to `operatingMode: Suspended` so its Pod terminates while the PVC persists, and resume rotates bootstrap material, refreshes the Sandbox blueprint, and returns it to `Running` against the preserved volume. Retention is bounded: the class lifecycle `idleTimeout` and `maxLifetime` are enforced on class-backed ACP workspaces (idle suspended workspaces expire, idle Ready workspaces follow the class default action, and `maxLifetime` forces terminal cleanup), and `RuntimeWorkspaceProfile.spec.retention.maxSuspendedWorkspaces` caps concurrently suspended workspaces per class and namespace with admission-time rejection and settlement-time retry that preserves the frozen Suspend action. A queued continuation can take a still-Ready workspace directly, while `maxLifetime` remains the hard cleanup bound. A suspend-capable class must set `idleTimeout` or `maxLifetime`; `maxSuspendedWorkspaces` may also cap occupancy but is not an expiry mechanism. Without either expiry bound, class readiness and Task binding fail closed. Suspend-capable workspaces created by an older controller without a frozen expiry receive a 24-hour migration grace period from their first post-upgrade observation, then enter UID-fenced terminal cleanup. Deletion policies that retain data past workspace deletion remain rejected. Substrate full-memory restore remains disabled. The contract admits only `DataOnly` suspension, and the current client still fails closed before actor creation. ADR 0030 documents the credential-safety prerequisites for a future fencing-capable implementation. See ADRs 0026–0030 for the complete contract.

Substrate `DataOnly` schema admission is not enough to execute suspension. The provider control client must return an immutable Actor and ActorSnapshot UID/version proof with observed `Data` scope, then compare that proof atomically with the resume mutation. The current in-tree client cannot make that guarantee, so suspend-capable Substrate pools fail closed before actor creation. Pools suspended by an older controller without the proof remain quarantined; Orka does not infer or backfill consent from a later provider observation. ADR 0030 records the protocol requirement.

### Agent Sandbox Controller Settings

Workspace-provider-backed ACP RuntimeSession dispatch requires `--acp-workspace-dispatch-enabled` plus the matching provider flag (`--agent-sandbox-enabled` or `--substrate-enabled`); with either unset, `Task.spec.execution.workspace` agent Tasks fail closed. The Substrate backend also uses `--substrate-api-*`, `--substrate-router-url`, and `--substrate-actor-dns-suffix`. The agent-sandbox router/template/timeout settings below belong to the earlier worker-path prototype and are not used by the ACP RuntimePool backend (which renders its own sandbox templates):

| Flag | Environment variable | Helm value | Default |
|------|----------------------|------------|---------|
| `--agent-sandbox-enabled` | `ORKA_AGENT_SANDBOX_ENABLED` | `controller.agentSandbox.enabled` | `false` |
| `--agent-sandbox-router-url` | `ORKA_AGENT_SANDBOX_ROUTER_URL` | `controller.agentSandbox.routerUrl` | empty |
| `--agent-sandbox-default-template` | `ORKA_AGENT_SANDBOX_DEFAULT_TEMPLATE` | `controller.agentSandbox.defaultTemplate` | empty |
| `--agent-sandbox-warm-pool-policy` | `ORKA_AGENT_SANDBOX_WARM_POOL_POLICY` | `controller.agentSandbox.warmPoolPolicy` | `disabled` |
| `--agent-sandbox-namespace-strategy` | `ORKA_AGENT_SANDBOX_NAMESPACE_STRATEGY` | `controller.agentSandbox.namespaceStrategy` | `task` |
| `--agent-sandbox-claim-timeout` | `ORKA_AGENT_SANDBOX_CLAIM_TIMEOUT` | `controller.agentSandbox.claimTimeout` | `2m` |
| `--agent-sandbox-command-timeout` | `ORKA_AGENT_SANDBOX_COMMAND_TIMEOUT` | `controller.agentSandbox.commandTimeout` | `30m` |
| `--agent-sandbox-cleanup-policy` | `ORKA_AGENT_SANDBOX_CLEANUP_POLICY` | `controller.agentSandbox.cleanupPolicy` | `delete` |

Supported values are `disabled` or `template` for the legacy warm pool policy setting, `task` or `controller` for namespace strategy, and `delete` or `retain` for cleanup policy. `task` defaults sandbox claims to the Task namespace; `controller` defaults them to the controller namespace when discoverable, and explicit `templateRef.namespace` values are honored as the claim/warm-pool namespace. See [Agent Sandbox Workspaces](agent-sandbox.md) for the deferred status and the invariants a future ACP-backed provider must preserve.

Any future ACP-backed integration will need a separately reviewed identity and RBAC design. Do not grant these permissions to managed ACP RuntimePods; they intentionally run without Kubernetes service-account tokens or Kubernetes RBAC.

### External API OIDC Authentication

ServiceAccount bearer token authentication is always available. To allow external callers such as GitHub Actions to authenticate directly with OIDC JWTs, configure issuer, audience, an explicit subject allowlist, and the namespace assigned to OIDC callers:

```bash
--oidc-issuer=https://token.actions.githubusercontent.com
--oidc-audience=orka-ci
--oidc-allowed-subjects=repo:my-org/my-repo:ref:refs/heads/main
--oidc-namespace=ci
```

The same settings can be supplied with environment variables:

```bash
ORKA_OIDC_ISSUER=https://token.actions.githubusercontent.com
ORKA_OIDC_AUDIENCE=orka-ci
ORKA_OIDC_ALLOWED_SUBJECTS=repo:my-org/my-repo:ref:refs/heads/main
ORKA_OIDC_NAMESPACE=ci
# Optional; when omitted, Orka discovers the JWKS URL from the issuer metadata.
ORKA_OIDC_JWKS_URL=https://token.actions.githubusercontent.com/.well-known/jwks
```

OIDC validation requires RS256-signed JWTs with matching `iss` and `aud`, valid time claims, a non-empty `sub`, and a `sub` value that matches `--oidc-allowed-subjects`. Wildcards `*` and `?` are supported in allowlist patterns; use the narrowest GitHub Actions subject for the trusted repository, branch, environment, or workflow. Authorized OIDC callers are bound to `--oidc-namespace` (or `default` when omitted) so namespace isolation can reject requests for other namespaces. When an OIDC-authenticated caller creates a Task, Orka records the verified identity in `spec.requestedBy`. Clients cannot set `requestedBy` themselves.

### External API Context-Token Authentication

Orka can also authenticate external API requests with generic transaction/context tokens. The built-in `transaction-token` profile validates RS256-signed JWTs with JOSE header `typ: txntoken+jwt`, matching `iss` and `aud`, valid time claims, a non-empty `sub`, and the required transaction-token claims `iat`, `txn`, `scope`, and `req_wl`.

For the strict profile contract, see [Transaction Tokens](transaction-tokens.md). For the breaking configuration change, see the [migration guide](../guides/transaction-token-migration.md).

Enable the profile by configuring the profile, issuer, and audience:

```bash
--context-token-profile=transaction-token
--context-token-issuer=https://issuer.example.com
--context-token-audience=orka-api
```

The same settings can be supplied with environment variables:

```bash
ORKA_CONTEXT_TOKEN_PROFILE=transaction-token
ORKA_CONTEXT_TOKEN_ISSUER=https://issuer.example.com
ORKA_CONTEXT_TOKEN_AUDIENCE=orka-api
# Optional for transaction-token; when omitted, Orka uses <issuer>/.well-known/jwks.json.
ORKA_CONTEXT_TOKEN_JWKS_URL=https://issuer.example.com/.well-known/jwks.json
```

By default, the `transaction-token` profile reads raw transaction tokens from the `Txn-Token` header:

```bash
curl -H "Txn-Token: $TXN_TOKEN" https://orka.example.com/api/v1/tasks
```

To customize token locations, set `--context-token-headers` or `ORKA_CONTEXT_TOKEN_HEADERS` to a comma-separated list. Use `Header` for raw token headers and `Header:Scheme` for scheme-prefixed headers. For example, keep the default `Txn-Token` header and explicitly opt in to `Authorization: Bearer` context-token support:

```bash
--context-token-headers=Txn-Token,Authorization:Bearer
```

`Authorization: Bearer` remains the default location for Kubernetes ServiceAccount and OIDC JWT authentication. Context-token bearer authentication is only attempted when `Authorization:Bearer` is explicitly configured and the bearer JWT has `typ: txntoken+jwt`; other bearer tokens continue through the standard OIDC or Kubernetes TokenReview flow. When an external context-token caller creates a Task, Orka records the verified subject and issuer in immutable `spec.requestedBy` and records safe transaction metadata in immutable `spec.transaction`, transaction labels, and transaction annotations. Clients cannot set `requestedBy` or `transaction` themselves.

Optional authorization is controlled by `--context-token-authz-mode` / `ORKA_CONTEXT_TOKEN_AUTHZ_MODE`. In `audit` mode, Orka logs safe authorization failures and allows the request. In `enforce` mode, Orka rejects context-token callers that lack the configured operation scope or violate signed `tctx` constraints. Task creation can be constrained by `tctx.namespace`, `tctx.taskType`, `tctx.agent`, `tctx.allowedAgents`, workspace `tctx.repo`/`tctx.branch`/`tctx.ref`, and `tctx.allowedTools`. Chat, OpenAI-compatible, and Anthropic-compatible model calls require the provider-use scope (default `orka:providers:use`) and honor `tctx.namespace`, `tctx.provider`, `tctx.allowedProviders`, `tctx.model`, and `tctx.allowedModels`. When Orka-managed server-side tools are exposed to those endpoints, they also require the tool-use scope (default `orka:tools:use`) and honor `tctx.allowedTools`. Security scan read/list/get endpoints require the security-read scope (default `orka:security:read`), and security scan create/update/delete and mutation endpoints require the security-write scope (default `orka:security:write`). Repository monitor read endpoints require the monitor-read scope (default `orka:monitors:read`), monitor create/update/delete endpoints require the monitor-write scope (default `orka:monitors:write`), and manual monitor runs require the monitor-operate scope (default `orka:monitors:operate`). Repository monitor access can also be constrained by `tctx.namespace`, `tctx.repo`, `tctx.branch`, `tctx.agent`, and `tctx.allowedAgents`. The raw TxToken is never logged or persisted in Task specs/status.

### Transaction-token TTS Exchange and Propagation

Configure `--context-token-tts-endpoint` / `ORKA_CONTEXT_TOKEN_TTS_ENDPOINT` when workers should exchange a mounted subject token for child or outbound replacement TxTokens. Delegation tools require `ORKA_CONTEXT_TOKEN_SUBJECT_TOKEN_FILE` and `ORKA_CONTEXT_TOKEN_CHILD_SCOPE`; HTTP Tool calls can use `ORKA_CONTEXT_TOKEN_OUTBOUND_SCOPE` or fall back to the current transaction scope. Child scopes are fail-closed: Orka rejects a requested child scope that is not already present in the parent transaction scopes before it creates the child Task.

Successful delegation exchanges store the raw child TxToken only in an owner-referenced Kubernetes Secret and annotate the child Task with the Secret name. The controller mounts that Secret into the child worker and sets `ORKA_TRANSACTION_TOKEN_FILE` / `ORKA_CONTEXT_TOKEN_SUBJECT_TOKEN_FILE` so deeper delegation and downstream Tool calls can continue the same transaction with configured child/outbound scopes.

### Task Provenance Admission Hardening

The REST API rejects client-supplied `requestedBy` and `transaction` fields and stamps verified provenance itself. To also protect direct Kubernetes `Task` CRD writes, enable the optional validating admission webhook:

```bash
--task-provenance-admission-enabled=true
```

The webhook denies untrusted `CREATE` or `UPDATE` requests that set or modify Orka-managed provenance fields: `spec.requestedBy`, `spec.transaction`, `orka.ai/transaction-*` labels/annotations, `orka.ai/context-token-profile`, and the child token Secret annotation. By default, trusted writers are the Orka controller ServiceAccount usernames in the controller namespace and the `orka-ai-worker` ServiceAccount name in the target Task namespace; override them with `--task-provenance-admission-trusted-users` and `--task-provenance-admission-trusted-service-accounts`.

How admission is deployed depends on the installation method. Helm releases install and enable Task-provenance admission automatically: the chart renders `task-provenance.<mode>.orka.ai` with `failurePolicy: Fail` against the release-local controller webhook Service and runs the controller with `--task-provenance-admission-enabled=true`, trusting the release controller identity. For Kustomize installations, admission deployment is opt-in and served by the dedicated admission runtime, not the controller manager: install `config/orka-admission` (Deployment, Service, NetworkPolicy, and RBAC for the admission runtime), then apply `config/orka-admission-webhooks` — which includes `taskprovenance.core.orka.ai` with `failurePolicy: Fail` — only after the readiness, TLS Secret, and CA-injection prerequisites in `config/orka-admission-webhooks/README.md` are met and the trusted identities embedded in `validating_webhook.yaml` match the admission-runtime arguments.

## Prometheus Metrics

Orka registers the following Prometheus metrics on the controller-runtime registry. The metrics endpoint is disabled by default (`--metrics-bind-address=0`); enable it by setting an explicit bind address, for example:

```bash
--metrics-bind-address=:8443   # HTTPS (default when --metrics-secure=true)
--metrics-bind-address=:8080   # HTTP, with --metrics-secure=false
```

Scrape configuration (for example a Prometheus Operator ServiceMonitor) is not shipped with the chart; point your monitoring stack at the metrics port directly.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `orka_api_requests_total` | Counter | `endpoint`, `method`, `status` | Total API requests (status bucketed as `2xx`/`4xx`/`5xx`) |
| `orka_api_request_duration_seconds` | Histogram | `endpoint`, `method` | API request latency in seconds |
| `orka_skills_loaded_total` | Counter | `skill`, `namespace` | Skills loaded by namespace and name |
| `orka_store_db_size_bytes` | Gauge | — | Size of the SQLite database file in bytes |
| `orka_context_token_auth_total` | Counter | `profile`, `result` | Context-token authentication attempts |
| `orka_context_token_authorization_total` | Counter | `action`, `result`, `reason` | Context-token authorization decisions (allow/deny/audit) |
| `orka_context_token_tts_exchange_total` | Counter | `result`, `reason` | transaction-token TTS token-exchange attempts |
| `orka_context_token_tts_exchange_duration_seconds` | Histogram | `result`, `reason` | transaction-token TTS token-exchange latency in seconds |
| `orka_token_exchange_total` | Counter | `adapter`, `grant_class`, `result`, `reason` | Direct and transaction-token OAuth exchange attempts |
| `orka_token_exchange_duration_seconds` | Histogram | `adapter`, `grant_class`, `result`, `reason` | OAuth exchange latency in seconds |
| `orka_acp_runtime_pool_desired_replicas` | Gauge | `namespace`, `runtime_pool` | Desired RuntimePool replicas |
| `orka_acp_runtime_pool_ready_replicas` | Gauge | `namespace`, `runtime_pool` | Authoritatively selected Ready RuntimePool replicas |
| `orka_acp_runtime_pool_sessions_active` | Gauge | `namespace`, `runtime_pool` | Authenticated resident RuntimeSession count |
| `orka_acp_runtime_pool_prompts_in_flight` | Gauge | `namespace`, `runtime_pool` | Authenticated active prompt count |
| `orka_acp_runtime_pool_queued_tasks` | Gauge | `namespace`, `runtime_pool` | Durable unsatisfied Task demand assigned to the pool |
| `orka_acp_runtime_pool_admission_state` | Gauge | `namespace`, `runtime_pool`, `state` | One-hot authoritative admission state (`unknown`, `closed`, `accepting`, `draining`, or `ambiguous`) |
| `orka_acp_runtime_pool_scale_to_zero_total` | Counter | `namespace`, `runtime_pool` | Completed RuntimePool scale-to-zero transitions |

Context-token metrics are described in more detail in [Transaction Tokens](transaction-tokens.md). All context-token labels use low-cardinality values only.

## OpenTelemetry telemetry

Orka supports opt-in OpenTelemetry traces and GenAI metrics for debugging,
performance analysis, and backend cost/latency dashboards. Telemetry is
disabled by default and uses OpenTelemetry no-op providers until enabled.

### Enabling telemetry

Add the `--enable-telemetry` flag to the controller and configure an OTLP
collector endpoint. The legacy `--enable-tracing` alias enables the same traces
and metrics:

```yaml
args:
  - --enable-telemetry
env:
  - name: OTEL_EXPORTER_OTLP_ENDPOINT
    value: "jaeger-collector.observability.svc:4317"
  - name: OTEL_EXPORTER_OTLP_INSECURE
    value: "true"
```

| Flag / Environment Variable | Default | Description |
|------------------------------|---------|-------------|
| `--enable-telemetry` / `--enable-tracing` | `false` | Enable OpenTelemetry traces and metrics |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | SDK default `localhost:4317` | OTLP collector endpoint for traces and metrics |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | unset | Trace-specific OTLP endpoint |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | unset | Metrics-specific OTLP endpoint |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | SDK default gRPC | Set to `http/protobuf` for OTLP/HTTP collectors |
| `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` / `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` | unset | Signal-specific exporter protocol overrides |
| `OTEL_EXPORTER_OTLP_INSECURE` and signal-specific insecure vars | SDK default | Disable TLS for in-cluster/dev collectors that require it |
| `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` | SDK default | Standard OpenTelemetry sampler configuration |

Controller-local defaults such as `localhost:4317` are valid only for the
controller process. AI worker Jobs receive telemetry enablement only when the
controller has a non-loopback, worker-reachable OTLP endpoint. The controller
copies non-secret OTLP endpoint/protocol/insecure/timeout/compression settings
to AI worker Pods and intentionally does not copy OTLP headers, certificate or
client-key env vars, `OTEL_RESOURCE_ATTRIBUTES`, or baggage.

ACP attempt, RuntimeSession, and publication spans are controller-side and use the
controller exporter. Managed RuntimePool workloads do not currently receive the
controller OTLP configuration, and there is no supported supervisor telemetry
opt-in surface. Do not inject credential-bearing OTLP headers into provider
children.

### Instrumented Components

| Tracer | Span | Attributes |
|--------|------|------------|
| `orka.api` | HTTP/API middleware spans | HTTP request/route/status metadata |
| `orka.chat` | `chat.request`, `chat.tool_loop.iteration` | session metadata; `chat.iteration`, `orka.tenant`, requested model, tool-call count |
| `orka.worker` | `task.run` | `orka.task.id`, namespace, and agent name when known |
| `orka.acp` | `acp.prompt`, `acp.session.create`, `acp.session.continue`, `acp.publication.reconcile` | `orka.task.id`, namespace, attempt/prompt identity, RuntimePool/RuntimeSession identity, prompt/session outcome, publication identity, and agent name when known |
| `orka.agent` | `agent.step` | iteration, requested model/provider, tool-call count, Orka task metadata |
| `orka.gen_ai` | `chat {model}` | `gen_ai.*` provider/model/token metadata and `error.type` |
| `orka.gen_ai` | `execute_tool {tool.name}` | `gen_ai.tool.*`, `orka.tool.name`, `orka.tool.kind`, `orka.tool.result.size_bytes`, parent/child task fields for delegation |
| `orka.controller` | `task.reconcile` | task name, namespace, type, and propagated trace context |

Use `orka.task.id` to find a Task trace, `orka.tool.name` to find specific tool
executions, and `orka.parent_task.id` / `orka.child_task.id` to follow delegated
children. Tool spans do not include raw arguments or result bodies.

### Example: Jaeger Setup

```bash
# Deploy Jaeger all-in-one (development only)
kubectl create namespace observability
kubectl apply -n observability -f https://raw.githubusercontent.com/jaegertracing/jaeger-operator/main/examples/simplest.yaml

# Configure the controller
kubectl set env deployment/orka-controller \
  OTEL_EXPORTER_OTLP_ENDPOINT=jaeger-collector.observability.svc:4317
```

## Context Engineering Best Practices

Research on LLM agent context files ([arxiv 2602.11988](https://arxiv.org/abs/2602.11988)) shows that verbose context hurts more than it helps: LLM-generated context files reduce task success rates by 0.5–2% while increasing inference costs by 20–23%. Even developer-written files yield only marginal improvements (~4%) with similar cost increases. The guidelines below translate these findings into practical advice for Orka's `systemPrompt`, `Skill`, and `Agent` configuration.

### Writing Effective System Prompts

Keep Agent `systemPrompt` content **minimal and requirement-focused**:

- **Include only**: tooling commands (build/test/lint invocations), non-discoverable gotchas (e.g., "provider secret key defaults to `api-key`"), and hard constraints the agent cannot infer from source code.
- **Avoid codebase overviews** — agents discover project structure efficiently on their own through file listing and search tools. Overviews add tokens without improving navigation speed.
- **Don't duplicate** information already present in `website/docs/`, `README`, or inline code comments. Redundant instructions increase reasoning token usage (14–22% more) without improving outcomes.

### Writing Effective Skills

Skills are prepended to the system prompt on **every LLM call** for every task that uses the parent Agent. Each Skill directly increases per-request token cost.

- Keep Skill `content` concise and **action-oriented** — write instructions ("run `make lint-fix` after changes"), not descriptions ("this project uses a Makefile-based build system").
- Split large Skills so Agents only reference the ones they need. A research Agent doesn't need a coding-standards Skill.
- Regularly audit Skill content and remove instructions the agent follows by default.

### Monitoring Recommendations

- Track `orka_task_duration_seconds` and LLM token metrics — more instructions ≠ better outcomes.
- A/B test agent performance with and without specific `systemPrompt` or `Skill` content to validate that each addition provides measurable benefit.
- Well-documented repositories benefit least from additional context; focus context engineering effort on repos with limited existing documentation.
