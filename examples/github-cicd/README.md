# GitHub CI/CD Integration

This example shows how to use Orka's multi-agent coordination with the ACP v2 workspace boundary:

1. An AI coordinator delegates a write-intent Task to a Claude ACP runtime.
2. The runtime edits the verified workspace but never receives Git credentials or publishes directly.
3. Orka's separate Workspace/Publisher prepares and verifies the branch update.
4. The coordinator opens a PR with `create_pull_request`, waits for CI, and merges with `auto_merge_pull_request`.
5. If CI fails, the coordinator delegates a focused repair against the same claimed branch.

## Credential roles

The example uses two independent Git credential roles; the Claude ACP runtime
obtains provider access through the controller-managed provider proxy and
carries no Agent `secretRef`:

- `repository-read` — clone/read credential used only by the clean-room workspace boundary;
- `repository-publish` — branch/forge credential used only by the Workspace/Publisher and GitHub coordination tools.

Neither Git Secret is delivered to the ACP process tree.

## Files

| File | Description |
| --- | --- |
| `agents.yaml` | Coordinator and Claude Agent definitions with ACP-safe prompts |
| `secret.yaml` | Example read and publication credential Secrets |
| `task.yaml` | Sample coordinator Task |
| `github-actions-webhook.yaml` | Optional workflow that creates a direct ACP write Task after CI failure |

## Setup

```bash
# Update spec.providerRef.name in agents.yaml to match your Provider CRD.
# Replace the placeholder values in secret.yaml before applying it.
kubectl -n orka-system apply -f examples/github-cicd/secret.yaml

kubectl -n orka-system apply -k examples/github-cicd
kubectl -n orka-system apply -f examples/github-cicd/task.yaml
```

Before running the Task, edit `task.yaml` and replace:

- `gitRepo` and `publicationGitRepo`;
- `branch` and `pushBranch`;
- `readCredentialRef` and `publicationCredentialRef`.

A branch update is successful only when the child Task has a terminal verified `status.delivery` receipt. The ACP child reporting that it changed files is not proof of publication.

## Optional GitHub Actions integration

Copy `github-actions-webhook.yaml` into `.github/workflows/` in a repository. On CI failure it creates a direct `type: agent` Task with top-level `workspace.intent: write`. The runtime edits the checkout; the Workspace/Publisher owns the exact-ref push.

Configure these repository secrets:

- `ORKA_API_URL`;
- `ORKA_TOKEN`.

The Orka Task namespace must contain the `repository-read` and `repository-publish` Secrets referenced by the payload.

:::caution Current write-path limitation
This worktree still fails non-empty workspace deltas closed until dispatcher-to-publisher delivery is fully wired. Treat this example as the ACP v2 manifest shape, and require a verified delivery receipt in live testing.
:::
