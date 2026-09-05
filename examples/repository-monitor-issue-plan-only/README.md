# RepositoryMonitor issue plan-only workflow

Use this example when you want durable triage/research/planning and approval records but do **not** want Orka to implement code.

Apply with:

```bash
kubectl -n orka-system apply -k examples/repository-monitor-issue-plan-only
```

Then add `orka:plan` to an issue and inspect:

```bash
orka -n orka-system monitor actions list issue-plan-only --kind issue --number '<issue-number>'
```

## Secrets

This monitor only reads and plans, so it needs two tokens rather than the four a writing
monitor needs. Create both outside git:

```bash
kubectl -n orka-system create secret generic orka-monitor-source-read \
  --from-literal=token='<read-only token for the repository>'

kubectl -n orka-system create secret generic orka-monitor-forge \
  --from-literal=token='<token for GitHub API calls, including the label sender permission check>'
```

Each Secret needs a non-empty `token`, `password`, or `GITHUB_TOKEN` key. `forgeCredentialRef`
is required because `spec.triggers.github.labels.enabled` is on: before acting on an
`orka:*` label, Orka asks GitHub whether the person who applied it is allowed to.

This example includes the `issue-researcher` Agent manifest; as a built-in Claude ACP
runtime it obtains provider credentials through the controller-managed provider proxy and
carries no `secretRef`.
