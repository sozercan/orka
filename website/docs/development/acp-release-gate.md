---
description: Qualify an ACP release candidate with deployed publication, independent GitHub verification, and safe cleanup.
---

# ACP publication release qualification

A release containing the RuntimePool ACP path requires a successful **Live ACP
Release Gate** report for its exact candidate commit. Ordinary ACP smoke, Task
success, component tests, and a report for another commit do not qualify it.
Complete this check after reviewing release metadata and before tagging the
candidate. The tag publication workflow does not run this credentialed gate for
you.

## Canary and environment

The source is `orka-agents/orka`. The dedicated publication target is
[`sozercan/orka-acp-release-gate`](https://github.com/sozercan/orka-acp-release-gate),
an actual fork of that source. Do not use it for development branches. Each
attempt creates a unique `orka/acp-release-gate-*` branch and a temporary PR
against the source default branch. The validator checks the fork relationship
through GitHub before submitting work.

Configure the `live-acp-release-gate` environment in `orka-agents/orka`:

- Select deployment branches by name and allow only the `main` branch. Do not
  allow tags, PR refs, or arbitrary branches. Required reviewers can be added if
  maintainer approval is desired.
- Set the environment variable `ACP_E2E_WRITE_PUBLICATION_REPO` to
  `https://github.com/sozercan/orka-acp-release-gate.git`. An optional dispatch
  override must identify this same repository.
- Configure the following secrets. The four GitHub credentials must have
  distinct values. Each Kubernetes copy uses the `token` key.

| Secret | Required access and use |
| --- | --- |
| `COPILOT_GITHUB_TOKEN` | Provider authentication for the configured Codex, OpenCode, Claude, and Copilot models through Vekil. The repository secret can supply this value. It is not Git publication authority. |
| `ACP_E2E_WRITE_READ_CREDENTIAL_TOKEN` | Source repository Contents read and metadata read. Used by the source clone boundary. |
| `ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_TOKEN` | Canary fork Contents read and metadata read. Used for target preflight and publication verification. |
| `ACP_E2E_WRITE_CREDENTIAL_TOKEN` | Canary fork Contents write and metadata read. Used by the separate publisher for the branch compare-and-swap push. No source write or PR authority is needed. |
| `ACP_E2E_WRITE_FORGE_CREDENTIAL_TOKEN` | Source Pull requests write, source and fork Contents read, and fork Contents write for branch cleanup. Used for PR reconciliation and by the independent `gh`/Git observer to verify both repositories, close the exact unmerged PR, and delete its branch with an exact-head lease. |

The forge/observer credential must work across the source organization and the
fork owner. A fine-grained PAT grants write access under only one resource
owner. For this public repository pair, a dedicated canary account with a
classic PAT scoped to `public_repo` is one option; keep that account's write
memberships limited to the canary resources. Follow organization token policy
and expiry requirements. See GitHub's
[personal access token permissions and limitations](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens).

Set values through the environment settings or the interactive `gh secret set`
prompt. Do not put values in dispatch inputs, shell command literals, reports,
or PR descriptions. Git and forge credentials never enter the ACP child process
tree. The publisher obtains operation-scoped authority through its existing
broker and must continue to fail the gate if its ServiceAccount can read these
Secrets directly. Reports record only Secret names, namespaces, and resource
versions.

## Run and qualify a candidate

The workflow is manual, serialized, and restricted to the trusted default
branch. Its code must already be on that branch. Build and publication use the
same full source SHA. Release changes made on another branch must first land on
the default branch; evidence for the parent commit cannot qualify a different
release commit.

From a trusted checkout, dispatch the current candidate:

```bash
candidate="$(gh api repos/orka-agents/orka/commits/main --jq .sha)"
gh workflow run live-acp-release-gate.yml --repo orka-agents/orka --ref main \
  -f source_ref="${candidate}" \
  -f source_repository=https://github.com/orka-agents/orka.git \
  -f pr_base=main

gh run list --repo orka-agents/orka --workflow live-acp-release-gate.yml \
  --commit "${candidate}" --event workflow_dispatch \
  --json databaseId,headSha,status,conclusion,url
```

Select the dispatched run ID from that list, wait for completion, and verify it:

```bash
gh run watch RUN_ID --repo orka-agents/orka --exit-status
bash scripts/verify-acp-release-qualification.sh "${candidate}" RUN_ID
```

The verifier requires a successful dispatch of `live-acp-release-gate.yml` from
this repository's default branch at the exact candidate SHA. It downloads the
final report for the current run attempt and checks its candidate, workflow
identity, publication evidence, image observations, and cleanup results. It
exits nonzero for missing, expired, incomplete, or mismatched evidence. Keep its
local `bin/acp-release-qualification-RUN_ID-ATTEMPT/acceptance.json` with the
release records and link the workflow run from the release checklist. Do not tag
an ACP candidate until this command succeeds. A local disposable-cluster run is
useful for diagnosis; release qualification requires the trusted workflow run.

## Evidence and failures

The job uploads `live-acp-release-evidence-RUN_ID-ATTEMPT` before tearing down
Kind, then `live-acp-release-acceptance-RUN_ID-ATTEMPT` after teardown. Both
contain only `acceptance.json` and remain available for 90 days. Archive the
verified final artifact with longer-lived release records before it expires.
The first artifact is diagnostic evidence and cannot qualify a release while
cluster cleanup is pending.

The report includes the dispatched and checked-out SHAs, built image references,
actual controller, publisher, and runtime Pod image digests, Task UID and attempt
fences, publication and PR receipts, independently observed remote head and PR
identity, frozen credential versions, and separate cleanup outcomes. It omits
Task prompts/results, free-form messages, Pod environment values, and Secret
contents. A failure before deployment records the candidate and failed stage;
unobserved fields remain absent or incomplete.

One Codex Task must create exactly the requested new file. The gate compares its
bytes at the expected commit and independently observed remote head, verifies
the commit parent/tree and one-file diff, and reads the open PR from GitHub.
`checks.publication: true` records successful publication verification;
`result: qualified` additionally requires the remaining runtime checks and all
cleanup to succeed. A preserved cluster, failed branch deletion, skipped
publication, or missing credential cannot produce a qualified report.

The source base must still equal the candidate at preflight, Task submission,
PR verification, and completion. If `main` moves, the report shows the observed
base SHA and the candidate remains unqualified. Let safe cleanup finish, then
dispatch the new full head SHA. Do not edit the report or reuse an earlier
report for the new candidate.

Cleanup first proves that the Task and publisher can no longer write. It closes
only the exact unmerged canary PR, then deletes the unique branch only if its
head still equals the independently observed head, using `--force-with-lease`.
It rereads both effects before removing owned Kubernetes resources and temporary
credentials. A changed head, ambiguous receipt, merged PR, or failed read fails
the gate and identifies preserved resources in the report. Inspect those exact
resources, establish writer quiescence and their current identities, and remove
only confirmed canary effects. Never replace the lease with unconditional branch
deletion or force-remove Task finalizers to get a passing result.

After the validator starts, cluster teardown requires an explicit completed
remote-cleanup result, or confirmation that no write Task started. A timeout or
interrupted cleanup with incomplete evidence preserves the cluster and registry
and fails qualification, even if the validator could not finish its exit trap.

Local preserved clusters remain available through the run's `kindctl` tag.
GitHub-hosted runners are disposable, so download the failure artifact for
inspection; runner disposal does not count as verified cleanup. After resolving
a preserved canary, start a new attempt with a new branch.
