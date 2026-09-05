# GitHub Label Trigger Example

This example wires GitHub labels to Orka ACP `type: agent` Tasks.

1. Deploy an Agent that can work in a git workspace.
2. Configure the controller with a webhook secret, default Agent, and the label-trigger compatibility Git Secret.
3. Add labels such as `agent:implement`, `agent:update-branch`, `agent:review`, or `agent:to-issues` to issues/PRs.

## Secrets

Create secrets outside git:

```bash
kubectl -n orka-system create secret generic github-webhook-secret \
  --from-literal=secret='<github-webhook-secret>'

kubectl -n orka-system create secret generic git-credentials \
  --from-literal=token='<repository-scoped-github-token>'
```

The compatibility setting maps this Secret to the Task read role and, for write labels, the publication role. The ACP runtime never receives it. Runtime agents must leave changes uncommitted; the separate Workspace/Publisher prepares, publishes, and verifies configured branches. For strict least privilege with different read and publication Secrets, create Tasks through an API/workflow that sets both top-level references explicitly.

## Controller env

For a Kustomize install, patch the controller Deployment with:

```yaml
env:
  - name: ORKA_GITHUB_WEBHOOK_SECRET
    valueFrom:
      secretKeyRef:
        name: github-webhook-secret
        key: secret
  - name: ORKA_GITHUB_LABEL_TRIGGER_AGENT
    value: codex-agent
  - name: ORKA_GITHUB_LABEL_TRIGGER_GIT_SECRET
    value: git-credentials
```

For Helm, use:

```yaml
github:
  webhook:
    secretName: github-webhook-secret
    secretKey: secret
  labelTrigger:
    agent: codex-agent
    gitSecret: git-credentials
```

Then configure a GitHub repository webhook that posts `Issues` and `Pull requests` events to:

```text
https://<orka-api-host>/webhooks/github
```
