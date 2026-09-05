# GitHub label-triggered issue-to-PR loop

This example configures a `RepositoryMonitor` for the durable `orka:*` workflow:

```text
issue label -> command event -> triage/research/plan -> approval -> implementation -> PR -> exact-head review -> repair/readiness
```

## Secrets

This monitor writes: it pushes a branch and opens or updates a pull request. Orka keeps the
four jobs on separate GitHub tokens so a leak of one does not grant the others, and it
refuses to run if any two of them are the same Secret. Create all four outside git:

```bash
kubectl -n orka-system create secret generic orka-monitor-source-read \
  --from-literal=token='<read-only token for the source repository>'

kubectl -n orka-system create secret generic orka-monitor-publication-read \
  --from-literal=token='<read-only token for the target repository>'

kubectl -n orka-system create secret generic orka-monitor-publication \
  --from-literal=token='<token that may push branches to the target repository>'

kubectl -n orka-system create secret generic orka-monitor-forge \
  --from-literal=token='<token for GitHub API calls: PRs, comments, permission checks>'
```

Each Secret needs a non-empty `token`, `password`, or `GITHUB_TOKEN` key. The ACP runtime
never receives any of them — only the controller and the Workspace/Publisher resolve them.

The Codex and Claude Agents are built-in ACP runtimes: provider credentials are
controller-managed through the provider proxy, so the Agents carry no
`secretRef`.

Configure your GitHub webhook to send `issues` and `pull_request` events to `/webhooks/github` with the controller webhook secret.

## Try it

1. Update `repoURL` in `repository-monitor.yaml`.
2. Apply the example: `kubectl -n orka-system apply -k examples/github-label-triggered-issue-loop`.
3. Add `orka:plan` or `orka:implement` to an issue.
4. Inspect state:

```bash
orka -n orka-system monitor commands list issue-to-pr-loop
orka -n orka-system monitor actions list issue-to-pr-loop --kind issue --number '<issue-number>'
orka -n orka-system monitor issues list issue-to-pr-loop
```

Automerge is intentionally disabled in this example. Enable `spec.automerge.enabled` only after validating review, CI, and merge-gate behavior in your environment.
