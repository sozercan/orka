# Code and review example

A coordinator delegates implementation to a coder and review to a second agent. It opens
a pull request if the reviewer approves, or reports the requested changes and stops.

> [!IMPORTANT]
> Automatic repair rounds are a follow-up. The delegation tools do not expose
> `workspace.expectedRemoteSHA`, which a new write Task needs to update an existing
> publication branch. A repair also needs to start from the prior verified published head.
> This example stops on `CHANGES_NEEDED` instead of attempting that unsupported loop.

Three Agents play distinct parts:

| Agent | Runtime | What it does |
| --- | --- | --- |
| `coordinator` | native `type: ai` | Delegates implementation and review, then opens a PR if approved. |
| `coder` | Claude ACP runtime, write workspace | Edits files. It cannot commit or push — see below. |
| `reviewer` | Claude ACP runtime, read workspace | Reads the verified published commit and answers `APPROVED` or `CHANGES_NEEDED`. |

## Apply it

Before applying, edit `iterative-task.yaml`: the repository URLs, branches, and Secret
names in the prompt are placeholders. `coordinator-agent.yaml` also points at a Provider
named `my-provider` — change it to a Provider that exists in your cluster.

For this same-repository example, three Secrets cover four credential roles:

```bash
# Read the source and verify the publication repository
kubectl -n orka-system create secret generic repository-read \
  --from-literal=token='<read-token>'

# Push the branch to the target repository
kubectl -n orka-system create secret generic repository-publish \
  --from-literal=token='<write-token>'

# Open the pull request through the forge API
kubectl -n orka-system create secret generic repository-forge \
  --from-literal=token='<forge-token>'
```

`readCredentialRef` and `publicationReadCredentialRef` both use `repository-read` here.
If the publication repository differs, set `publicationReadCredentialRef` to a Secret
that can read that repository. It authorizes target preflight and post-push verification;
`publicationCredentialRef` authorizes the push. Opening the PR separately requires
`forgeCredentialRef`.

After configuring the files and creating the Secrets, apply the Agents before creating
the Task that references them. There is no kustomization here:

```bash
kubectl apply -n orka-system \
  -f examples/iterative-review/coder-agent.yaml \
  -f examples/iterative-review/reviewer-agent.yaml \
  -f examples/iterative-review/coordinator-agent.yaml
kubectl apply -n orka-system -f examples/iterative-review/iterative-task.yaml
```

Watch the Tasks run:

```bash
kubectl get tasks -n orka-system -w
```

Child Tasks appear with generated names like `deliver-auth-feature-child-xxxxx` and are
deleted along with the parent.

## Why the coder cannot push

The coder agent has the repository checked out but holds no Git credentials. When it
finishes, Orka's clean-room publisher — a separate process that has the publication
credential and no model access — validates the resulting tree and pushes it to
`pushBranch`. A prompt injection that fully captures the coder still cannot write to your
repository. See [Security](../../website/docs/concepts/security.md).

Step 4 pins the reviewer to the coder's verified delivery. Set the review workspace's
`gitRepo` to `publicationGitRepo`, `branch` to `pushBranch`, and `ref` to the coder
result's `headSHA`, using `publicationReadCredentialRef` for checkout. The exact SHA
keeps the reviewed content fixed even if `pushBranch` moves. If delivery is not verified
or `headSHA` is missing, the coordinator reports the result and stops.

## Where the guardrails come from

`coordinator-agent.yaml` sets `spec.coordination`:

- `allowedAgents` — the only Agents this coordinator may delegate to. Anything else is
  rejected by the controller, not by the prompt.
- `maxDepth: 3` — a delegated agent may delegate further, but only three levels deep.
- `maxConcurrentChildren: 2` — at most two child Tasks running at once.

These are enforced when the child Task is reconciled, so a coordinator that ignores its
system prompt still cannot exceed them. See
[Multi-agent coordination](../../website/docs/reference/multi-agent-coordination.md) for
the full tool schemas and controller checks.
