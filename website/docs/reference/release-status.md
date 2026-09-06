---
slug: /release-status
description: "What is in the latest tagged release versus what these docs describe."
---

# Release status

**These docs describe `main`.** The newest tagged release, **v0.1.3**, is older than a
large part of what is written here.

That gap is deliberate — `main` is where the current work lands — but it means you can
follow a page here exactly and get "unknown field" or "no matches for kind" from a v0.1.3
cluster. This page tells you which one you are on and what the difference is.

## Which one am I running?

```bash
# Neither install creates a Deployment named plain `orka`: a Helm release named `orka`
# creates `orka-controller`, and the release manifest creates `orka-controller-manager`.
kubectl -n orka-system get deploy -l app.kubernetes.io/name=orka \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.template.spec.containers[0].image}{"\n"}{end}'

# Orka CRDs carry no common label, so count them by group instead.
kubectl get crd -o name | grep -c '\.orka\.ai$'
```

| CRDs | You have |
| --- | --- |
| 17 | v0.1.3 |
| 26 | a build from `main` |
| 12 | a stale `charts/orka/` snapshot from the repo root — see the warning below |

## What v0.1.3 does not have

The ACP execution path and runtime pools landed after v0.1.3. That release already
included the `AgentRuntime` CRD and the `orka.harness.v1` contract for external runtimes.

| | v0.1.3 | `main` |
| --- | --- | --- |
| Container tasks (`type: container`) | Yes | Yes |
| Native AI tasks (`type: ai`) | Yes | Yes |
| Chat and compatibility APIs | Yes | Yes |
| Repository monitors, scans, gateways | Yes | Yes |
| Coding-agent tasks (`type: agent`) | Yes, on a per-Task Job with a harness wrapper | Yes |
| Coding agents over [ACP](glossary.md#running-coding-agents) | **No** | Yes |
| `RuntimePool` / `RuntimeSession` | **No** | Yes |
| `PromptAttempt`, `ControllerEpoch`, `Publication`, `BranchClaim`, `ExternalEffect` | **No** | Yes |
| `RuntimeProviderConfig`, `RuntimeWorkspaceProfile`, `RuntimeSessionControl` | **No** | Yes |
| [Harness modes](../operations/harness-modes.md) (`orka.ai/controller-mode`) | **No** | Yes |
| `--watch-namespace` | Optional; empty watches the whole cluster | **Required** |

Nine CRDs are new on `main`. If a page here mentions a `RuntimePool`, a supervisor, a
prompt attempt, or clean-room publication, it does not apply to v0.1.3. v0.1.3 does run
`type: agent` Tasks — what it lacks is the ACP execution path that replaced the older
per-Task Job.

## Installing v0.1.3

No clone needed:

```bash
# The manifest mounts a harness-wrapper-auth Secret but does not create it,
# so make the namespace and that Secret first or the Pods never start.
kubectl create namespace orka-system
kubectl -n orka-system create secret generic harness-wrapper-auth \
  --from-literal=token="$(openssl rand -hex 32)"

kubectl apply -f https://raw.githubusercontent.com/orka-agents/orka/v0.1.3/deploy/orka.yaml
```

Or with Helm:

```bash
helm repo add orka https://orka-agents.github.io/orka/charts
helm repo update
helm install orka orka/orka --namespace orka-system --create-namespace
```

Both are published by the release workflow, which runs only on a `v*` tag.

## Installing `main`

There is no published image for `main` — the release workflow is the only thing that
builds and pushes images, and it is tag-triggered. To run current features you build the
images yourself. [Getting started](../getting-started.md) has the full path.

:::warning[Do not install from `charts/orka/` or `deploy/orka.yaml` at the repo root]
Those are *promoted release snapshots*, refreshed only during release preparation. On
`main` today they still hold v0.1.1 — `charts/orka/Chart.yaml` says `0.1.1`, its values
deploy `ghcr.io/orka-agents/orka:0.1.1`, and it ships 12 CRDs. It is internally consistent,
so it installs and runs; it just gives you a release that is two versions old, without
saying so.

Build from `manifest_staging/charts/orka/` instead — that is the chart regenerated from
current source by `make manifests`.
:::

## Version support

Orka is pre-1.0.

- CRD schemas may change between minor releases. Diff the CRDs before upgrading, and follow
  [Upgrading](../operations/upgrading.md) — Helm will not update CRDs for you.
- Only the latest release gets fixes. There are no patch backports to older tags.
- No release is supported for production use yet.

There are currently no GitHub Release entries and no written release notes: `release.yml`
publishes images and the Helm repository on a `v*` tag, and nothing else. To see what
changed between two versions, compare the tags directly —
[github.com/orka-agents/orka/tags](https://github.com/orka-agents/orka/tags) lists them,
and `git diff v0.1.2..v0.1.3 -- config/crd/bases` shows the schema changes that matter
most for an upgrade.
