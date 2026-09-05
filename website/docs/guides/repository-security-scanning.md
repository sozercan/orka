---
slug: /repository-security-scanning
description: "Scanning a repository for security findings and turning them into remediation pull requests."
---

# Repository security scanning

Repository security scanning provides a human-in-the-loop workflow: register a GitHub
repository, generate an editable threat model, scan history and new commits for likely
vulnerabilities, validate findings in an isolated worker, and remediate with generated
patches and pull requests — all human-in-the-loop.

The feature is GitHub-first and built on Orka's existing task, agent runtime, artifact,
scheduling, and PR plumbing. Remediation (patch generation and PR creation) always requires
an explicit user action.

- For the CRD field reference, see [Configuration → RepositoryScan](../reference/configuration.md#repositoryscan).
- For the REST endpoints, see [API Reference → Security](../reference/api-reference.md#security).
- For the internal design and storage model, see [Repository Security Scanning Design](../development/security-scanning-design.md).

## How it works

```text
Register repository (UI or RepositoryScan CRD)
  → initial scan: clone repo, generate threat model, map review slices, store findings
  → scheduled incremental scans process new commits and reuse slice metadata where possible
  → review threat model + findings in the dashboard
  → from a finding: generate a patch, then open a remediation PR
```

Each scan runs as a `type: agent` task with a git workspace. The agent writes structured
artifacts (threat model, deterministic review slices, bounded context manifests, findings
JSON, validation evidence, patch summaries, and patch diffs) that the
`RepositoryScan` controller ingests into the security store and surfaces through the
`/api/v1/security/*` API and the **Security** area of the dashboard.

## Quick start (dashboard)

1. Create or choose an Agent that can run repository security analysis (a `type: agent`
   runtime Agent with a git workspace, e.g. a Claude or Codex runtime agent).
2. Open the embedded dashboard and go to **Security** (`/security`).
3. Select **New Repository**, enter the GitHub repository URL, branch, optional `subPath`,
   scan schedule, validation mode, and analysis agent, then save.
4. Use **Scan Now** on the repository card or detail page to start a manual scan
   immediately, or rely on the configured cron schedule for incremental scans.
5. Review the generated threat model, scan runs, recommended findings, evidence, and
   validation status from the repository detail page.
6. From a finding detail page, optionally validate/reproduce the finding, generate a patch
   proposal, review the patch artifacts, and create a remediation pull request.

## Quick start (GitOps / API)

You can drive the same workflow declaratively with the `RepositoryScan` CRD or the
`/api/v1/security/*` endpoints.

These examples use `orka-system`, the namespace watched by the standard installation.
Create the referenced Agents and Secrets there, or adapt the namespace references to
your installation.

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: RepositoryScan
metadata:
  name: example-repo
  namespace: orka-system
spec:
  provider: github
  repoURL: "https://github.com/example/app"
  branch: main
  ref: "v1.2.3"                 # optional tag, branch, or commit SHA checkout override
  subPath: "services/api"        # optional monorepo scope
  gitSecretRef:                   # optional for private repositories (scan read only)
    name: github-credentials
  # Required before patch generation / remediation PRs: four distinct Secrets.
  readCredentialRef:
    name: github-source-read
  publicationReadCredentialRef:
    name: github-publication-read
  publicationCredentialRef:
    name: github-publication-write
  forgeCredentialRef:
    name: github-forge
  schedule: "0 2 * * *"          # optional cron for incremental scans
  validationMode: light           # off, light, or full
  analysisAgentRef:
    name: security-reviewer
  patchAgentRef:                  # optional; defaults to the analysis agent
    name: security-patcher
  maxFindingsPerRun: 25
```

For patch Tasks, the Workspace/Publisher—not the ACP child—prepares and publishes the branch.

See [Configuration → RepositoryScan](../reference/configuration.md#repositoryscan) for every
spec and status field, and [API Reference → Security](../reference/api-reference.md#security)
for the endpoint and query-parameter reference, including the typical findings → validate →
patch → pull-request remediation flow.

## Scan phases

| Phase | What happens |
|-------|--------------|
| **Initial scan** | Clones the repo, generates `security-threat-model.md`, runs a deterministic mapper that writes `security-slices.json`, reviews selected stored slices, and stores a run summary. A threat model is generated even when no findings are emitted. |
| **Threat model review** | The repository detail page shows the generated threat model in an editor. Saving an edit (or a regenerated model) replaces the current threat model and influences ranking on later scans. Prior threat models are not retained as history. |
| **Incremental scans** | Run on the configured schedule and process commits after the last completed run. Slice metadata drives changed-file-based selection. Manual re-scan stays available and intentionally reruns even if the same range was scanned before; scheduled/incremental active runs use an idempotency key to avoid duplicate in-flight work. |
| **Evidence ingestion** | v2 findings are stored only when their evidence cites safe repo-relative paths and line ranges included in the review context manifest. Valid candidates then pass a deterministic false-positive filter before the max-finding cap. Invalid, filtered, or capped output is recorded as dropped diagnostics instead of becoming a finding. |
| **Patch generation** | From a finding, Orka creates a dedicated write-intent patch task. The agent edits the workspace and returns an identity-bound `orka.security.patch.v1` result envelope (summary, changed files, tests run, risk); it never writes artifact files into the workspace. The clean-room Workspace/Publisher commits the delta and opens the pull request, and the controller derives the reviewable diff from that published commit (read with `forgeCredentialRef`) and stores the diff and summary artifacts. The proposal is marked ready only when the envelope's changed files exactly match the published commit. |
| **PR receipt** | The pull request already exists at this point — the clean-room publisher opened it during patch generation as part of the same governed, verified publication. The pull-request endpoint is idempotent: it records and returns the verified PR receipt (number and URL) from the proposal rather than opening a second PR, and the controller re-titles the publisher's generic PR with the finding (`fix(security): …`). |

## Validation modes

| Mode | Behavior |
|------|----------|
| `off` | No validation tasks; schema, evidence, and deterministic false-positive filters still run. |
| `light` | Default. Validates a small number of likely high-impact/high-confidence findings when safe and practical. |
| `full` | More aggressive validation/reproduction for kept findings, including builds where useful. |

Scanning never requires a buildable repository; validation may build when useful. Automatic
validation can be tuned with `validationMaxFindingsPerRun`, `validationMinSeverity`, and
`validationMinConfidence`; failed validations are excluded from recommended patch candidates,
while validated findings rank above unvalidated findings of the same severity.

## Custom scan policy ConfigMaps

Teams can attach additive policy text from same-namespace ConfigMaps instead of putting long
instructions directly in the `RepositoryScan` spec. The ConfigMap must opt in with
`orka.ai/security-policy: "true"` as a label or annotation, and `key` can be omitted to use
the default `policy` key. Each policy value is capped at 32 KiB and is rejected if it looks
like it contains a secret, token, private key, or credential. Custom policy cannot remove Orka's default
finding quality policy, no-secret rules, evidence requirements, or deterministic hard
exclusions.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: repo-security-policy
  namespace: orka-system
  labels:
    orka.ai/security-policy: "true"
data:
  scan: |
    For this Kubernetes operator, prioritize RBAC escalation, controller-runtime cache
    trust boundaries, admission webhook bypasses, namespace isolation, and unsafe owner
    reference handling.
  false-positives: |
    Do not report docs-only YAML snippets unless they are applied by automation. Keep
    concrete prompt/tool injection findings when they can affect privileged tools, memory,
    artifacts, patches, or PR creation.
---
apiVersion: core.orka.ai/v1alpha1
kind: RepositoryScan
metadata:
  name: example-repo
  namespace: orka-system
spec:
  repoURL: "https://github.com/example/app"
  analysisAgentRef:
    name: security-reviewer
  customScanInstructionsRef:
    name: repo-security-policy
    key: scan
  falsePositivePolicyRef:
    name: repo-security-policy
    key: false-positives
```

The ConfigMap and the `RepositoryScan` must live in the same namespace. `repoURL` and
`analysisAgentRef` are the only two required fields on a `RepositoryScan`; everything else
above is what the two policy references add.

Suggested custom policy themes:

- Kubernetes/operator repositories: RBAC, admission, controller caches, reconciliation
  trust boundaries, owner references, namespace isolation.
- AI-agent/tooling repositories: prompt/tool injection into privileged tools, memory,
  artifacts, tasks, patches, PRs, and raw transcript/token persistence.
- Web applications: server-side authorization, session/token handling, trusted HTML sinks,
  SSRF/file access, and tenant boundaries.
- Monorepos: subpath-specific trust boundaries, generated runtime config, shared CI/CD
  scripts, and cross-package credential flows.

## Dropped findings and filtering

The scanner favors precision before persistence. Review prompts ask for concrete exploit
paths and exclude common noise. The controller also applies a deterministic filter before
findings are stored. Default dropped classes include docs-only issues, test-only issues,
generic missing rate limits, generic DoS/resource exhaustion, dependency-version reports,
client-side-only auth complaints, React XSS without unsafe HTML sinks, shell injection with
no untrusted input path, non-sensitive logging, and generic prompt injection without a
privileged Orka effect.

Dropped diagnostics are visible from the API, CLI, and repository detail page. Each dropped
item has a layer (`validation`, `filter`, or `cap`), reason, and sanitized compact sample.
Use `orka security dropped-findings list <repo> --layer filter` or
`--reason contains=rate-limit` to inspect scanner noise without exposing raw model output.

Incremental and manual scans include changed-file and changed-line context when commit
metadata is available. Review agents are instructed to focus on newly introduced, exposed,
or materially worsened risk; unchanged code can support a finding but should not produce
old repository-wide findings by itself.

## Safety

- Scan and patch agent Tasks use ACP RuntimePools with private RuntimeSessions; deterministic mapper/container work keeps the native hardened worker path.
- `RepositoryScan.spec.gitSecretRef` remains the scan-level compatibility reference for read-only scan Tasks (`readCredentialRef` takes precedence when both are set). Patch proposals and remediation pull requests are write workflows and require the four explicit, pairwise-distinct roles `readCredentialRef`, `publicationReadCredentialRef`, `publicationCredentialRef`, and `forgeCredentialRef`; a patch request is rejected with `spec.<role> is required for repository scan patch publication` until all four are set. `gitSecretRef` never supplies a publication or forge role, and the ACP child never receives any of these Secrets.
- Patches and PRs are never created automatically — both are explicit user actions.
- Patch proposals cannot reach `patch_ready` without a verified publisher branch receipt, patch summary, and
  diff artifact that matches the validated workspace delta.
- Edited threat models are treated as ranking input, not executable instructions.
- Finding evidence is structured as repo-relative file/line references or flat sanitized
  artifacts within the per-file (10 MB) and total (50 MB) artifact upload limits.
- Dropped-finding diagnostics contain compact reasons and samples only; they must not
  include raw tokens, credentials, full transcripts, or sensitive request context.

## See also

- [Repository Security Scanning Design](../development/security-scanning-design.md) — CRD,
  storage schema, controller ingestion, artifact contract, and prompt contracts.
- [Configuration → RepositoryScan](../reference/configuration.md#repositoryscan)
- [API Reference → Security](../reference/api-reference.md#security)
