---
slug: /troubleshooting
description: "Error messages you are likely to hit, what causes them, and how to fix them."
---

# Troubleshooting

Common failures, what causes them, and what to do. If you are looking for day-to-day
operational procedures instead, see the [runbook](runbook.md).

## Install

### What is my controller Deployment called?

Two, depending on how you installed. Every `kubectl` command in these docs assumes one of
them, so check before you copy one:

| Install method | Deployment name |
| --- | --- |
| `kubectl apply -f .../deploy/orka.yaml` | `orka-controller-manager` |
| `helm install orka ...` | `orka-controller` |

The Helm name is `<release-name>-controller`, so it changes if you name the release something
other than `orka`. When in doubt:

```bash
kubectl -n orka-system get deploy
```

### Helm refuses to render

The chart validates its inputs before producing any manifests, so a bad install fails at
`helm install` rather than at 3 a.m. The message names the value it wants. The common ones:

| Message mentions | What it wants |
| --- | --- |
| `agentExecutionSnapshot.existingSecret` | A Secret holding a 32-byte key. See [below](#the-snapshot-key). |
| `watchNamespace` | Must be set, and must equal the release namespace. |
| `providerProxy` | `providerProxy.enabled=true` is required for `harness-v2`. See [Provider proxy](provider-proxy.md). |
| `upstreamBaseURL` | Must be exactly `http://vekil.vekil-system.svc:1337`. |
| `@sha256:` | Runtime and controller images must be digest references, not tags. |
| `replicas` / `leaderElect` | Must be `1` and `true`. The controller is a single writer. |
| `webhooks.tls.existingSecret` | A TLS Secret for the admission webhooks. |
| `mode` | Only `harness-v1` or `harness-v2`, and it cannot change on upgrade. |

### The snapshot key

`controller.agentExecutionSnapshot` encrypts stored agent execution records. It needs a
Secret containing either 32 raw bytes or their base64 encoding:

```bash
kubectl -n orka-system create secret generic orka-agent-snapshot-key \
  --from-literal=key="$(openssl rand -base64 32)"
```

:::danger[Do not rotate this casually]
The Secret name, the item key, and the key material must stay the same for the life of the
release. Changing any of them makes every retained snapshot permanently unreadable.

The chart guards only two of those three. On upgrade it compares the Secret **name** and
**item key** against the live Deployment and fails if either changed. It cannot see the
key material, so replacing the bytes under the same name and key passes the guard
silently — and the controller then restarts unable to read any snapshot it wrote before.
Treat the material as immutable yourself; nothing in the chart will stop you.
:::

### The controller crashes immediately

Check the logs first. The Deployment name depends on how you installed, so discover it
rather than guessing:

```bash
# The two installs label the controller differently, so try both.
# Helm sets app.kubernetes.io/component=controller; the release manifest
# sets control-plane=controller-manager and no component label at all.
CONTROLLER="$(kubectl -n orka-system get deploy \
  -l app.kubernetes.io/component=controller \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
CONTROLLER="${CONTROLLER:-$(kubectl -n orka-system get deploy \
  -l control-plane=controller-manager \
  -o jsonpath='{.items[0].metadata.name}')}"

kubectl -n orka-system logs "deploy/$CONTROLLER" --previous
```

That is `orka-controller` for a Helm release named `orka`, and `orka-controller-manager`
for the release manifest, as the table above shows.

**`--watch-namespace is required; controller modes cannot use a cluster-wide watch`**

Orka watches exactly one namespace. Set `controller.watchNamespace` to the release
namespace.

**`controller-mode namespace claim failed`** or `namespace "..." is claimed by execution
mode "..."`

The namespace must carry a label matching the controller's mode:

```bash
kubectl label namespace orka-system orka.ai/controller-mode=harness-v2
```

This is how two installs on one cluster avoid fighting over the same Tasks. If the label
says `harness-v1` and you are installing `harness-v2`, use a different namespace — do not
relabel a namespace that another install is using.

**`unable to read controller-mode namespace`**

The namespace does not exist yet, or the controller's ServiceAccount cannot read it.

## Tasks

### My Task stays Pending

In order of likelihood:

1. **Wrong namespace.** Orka only sees its watch namespace. A Task in `default` is never
   reconciled and produces no error — it just sits there.

   ```bash
   kubectl get task '<name>' -A          # where did it actually land?
   kubectl -n orka-system get tasks    # where Orka is looking
   ```

2. **The Agent or Provider is missing**, or is in a different namespace. Check
   `kubectl -n orka-system describe task <name>` for the reason.

3. **Concurrency limit reached.** If `maxTasksPerNamespace` is set, new Tasks queue.
   `kubectl -n orka-system get tasks` shows how many are Running.

4. **No pool capacity** for a `type: agent` Task. `kubectl -n orka-system get runtimepools`
   — a pool that is not `Serving` and `Accepting` will not take new sessions.

### A `type: agent` Task fails immediately

**The runtime image was not configured.** If you did not set
`controller.acpRuntime.codexImage` (or claude/copilot/opencode), that runtime is
unavailable and Tasks asking for it fail rather than falling back to another. This is
intentional.

**The provider proxy is not reachable or not ready.** Check Vekil first — it fails
independently of Orka:

```bash
kubectl -n vekil-system get deploy
kubectl -n orka-system get deploy -l app.kubernetes.io/component=provider-auth-proxy
```

See [Provider proxy](provider-proxy.md).

**Codex with `allowBash: false`.** The Codex CLI has no reliable shell-disable mode, so
Orka fails fast instead of pretending the restriction is enforced. Set
`defaultAllowBash: true`.

**OpenCode without `contextWindow` and `maxTokens`.** Both are required and must be set on
the Agent.

### `go: command not found` or read-only filesystem errors

The Pod filesystem is read-only outside `/tmp`, `/home/worker`, and `/workspace`, and
`bash -lc` breaks official language images. Both are covered in
[Container tasks](../guides/container-tasks.md).

### A pool will not replace or scale down

A pool drains before it is replaced: in-flight sessions finish, new ones are refused. If it
is stuck, something is still holding a session. Check
`kubectl -n orka-system describe runtimepool <name>` for the sessions it is waiting on.
Deleting the Pods directly does not help — the controller will wait for the drain
regardless.

## API access

### I get 403 from the API

**Namespace mismatch.** The API serves exactly one namespace. Asking for another returns
`namespace not allowed`, with the requested and allowed namespaces in the controller log.

**The token has no namespace.** With `enforceNamespaceIsolation` on (the default),
authenticated callers must carry a namespace. A token from a ServiceAccount in the wrong
namespace, or a user token with no namespace at all, is rejected.

**The ServiceAccount lacks Task permissions.** Task creation checks the caller's
Kubernetes RBAC through a SubjectAccessReview. This is an endpoint-specific check.

:::warning[Agent creation does not enforce caller RBAC]
The REST Agent-creation endpoint and compatibility `create_agent` tool do not check a
ServiceAccount caller's `agents/create` permission. Read-only Agent roles therefore do
not prevent Agent creation through these API paths. Context-token authorization is a
separate check for context-token callers.
:::

The Helm chart creates `orka-client` with the roles below; raw-manifest and Kustomize
installs do not. To create one by hand:

```bash
kubectl -n orka-system create serviceaccount orka-client

kubectl -n orka-system create role orka-client \
  --verb=get,list,watch,create,delete --resource=tasks.core.orka.ai
kubectl -n orka-system create role orka-client-monitors \
  --verb=get,list,watch,create,update,patch,delete --resource=repositorymonitors.core.orka.ai
kubectl -n orka-system create role orka-client-read \
  --verb=get,list,watch \
  --resource=agents.core.orka.ai,tools.core.orka.ai,skills.core.orka.ai,providers.core.orka.ai,runtimepools.core.orka.ai,agentruntimes.core.orka.ai
kubectl -n orka-system create role orka-client-sessions \
  --verb=get,list,delete --resource=sessions.core.orka.ai
kubectl -n orka-system create role orka-client-gateway \
  --verb=get,list,watch --resource=gateways.gateway.orka.ai,gatewaybindings.gateway.orka.ai
kubectl create clusterrole orka-client-gatewayclass-viewer \
  --verb=get,list,watch --resource=gatewayclasses.gateway.orka.ai

for r in orka-client orka-client-monitors orka-client-read orka-client-sessions orka-client-gateway; do
  kubectl -n orka-system create rolebinding "$r" \
    --role="$r" --serviceaccount=orka-system:orka-client
done
kubectl create clusterrolebinding orka-client-gatewayclass-viewer \
  --clusterrole=orka-client-gatewayclass-viewer --serviceaccount=orka-system:orka-client
```

Two of those look odd and are correct:

- **`sessions`** has no CRD. It is a virtual API resource; the REST API still runs a
  SubjectAccessReview against it.
- **`providers`** read access is what the dashboard's provider picker checks.

### `kubectl create token orka-client` fails

Add `-n orka-system`. The ServiceAccount lives in the watch namespace. If it is still not
found, [create it and its RBAC roles](#i-get-403-from-the-api); raw-manifest and Kustomize
installs do not create it.

### Browser requests fail with a CORS error

`ORKA_CORS_ALLOWED_ORIGINS` controls the allowed origins and defaults to `*`. If you have
narrowed it, add your origin. See [Configuration](../reference/configuration.md).

## Upgrades

### Custom resources disappear or the controller reports unknown fields

Helm does not create or update CRDs during `helm upgrade` — a Helm behavior, not an Orka
one. Apply the CRDs from the target chart yourself first. See [Upgrading](upgrading.md).

### The chart refuses to upgrade

Several values are immutable for the life of a release: `controller.mode`,
`controller.watchNamespace`, the snapshot key Secret and item key, the release fullname,
and the ACP runtime namespace. Changing any of them means a new release, not an upgrade.

## Gateways

Gateway state lives in the controller's SQLite store, and its failure modes are specific
enough to have their own page — including the two ways to corrupt a backup. See
[Gateways](gateways.md).

## Getting more detail

```bash
kubectl -n orka-system logs "deploy/$CONTROLLER" -f   # $CONTROLLER from above
kubectl -n orka-system describe task '<name>'
kubectl -n orka-system get events --sort-by=.lastTimestamp
orka task events '<name>'
orka task trace '<name>'
```

Durable per-Task history is in [execution events](../reference/execution-events.md), which
survive Pod restarts and are the right thing to read when the logs have rotated away.

Set `controller.logLevel=debug` for more detail. If you think you have found a bug,
[open an issue](https://github.com/orka-agents/orka/issues).
