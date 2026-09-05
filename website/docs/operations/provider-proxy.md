---
slug: /provider-proxy
description: "Setting up model access without exposing provider credentials to agent processes."
---

# Provider proxy

Built-in coding agents in Orka never receive your LLM API key. Every model call they make
goes through the supervisor's session proxy, Orka's auth proxy, and Vekil, which adds the
real credential. This page explains the path and how to set up the two services.

If you are only running `type: ai` Tasks, you do not need any of this — the AI worker reads
provider Secrets directly. This is required for `type: agent` Tasks, and the Helm chart
refuses to install a `harness-v2` release without it.

## Why

A coding agent runs shell commands that a model chose. If that process also held your
Anthropic or OpenAI key, any command it ran could read the key and use it for anything.

So Orka splits it up:

```
agent process → session loopback proxy → provider-auth-proxy → Vekil → model provider
                (inside runtime Pod)    (Orka namespace)      (real keys)
```

The agent gets a session token for the supervisor's loopback proxy. That proxy validates
the provider routes and model against the immutable session profile, then uses a separate
credential to call `provider-auth-proxy`. The upstream credential never enters the agent's
process tree.

## The two pieces

**`provider-auth-proxy`** ships with Orka. The Helm chart deploys it into the release
namespace when `providerProxy.enabled=true`. It checks the shared current or previous
bearer credential and forwards requests to Vekil. Provider and model enforcement belongs
to the supervisor's per-session loopback proxy.

**[Vekil](https://github.com/sozercan/vekil)** is a separate open-source reverse proxy that
holds the actual provider credentials and presents Anthropic, OpenAI Chat Completions,
OpenAI Responses, and Gemini-compatible endpoints on one port. Orka does not install or
manage it.

Orka only accepts Vekil at exactly `http://vekil.vekil-system.svc:1337`. Other hosts,
namespaces, and ports are rejected at chart render time, so a misconfigured install cannot
quietly route model traffic somewhere unreviewed. One trailing slash is tolerated.

## Install Vekil

Vekil's own repository has full instructions. The short version, for a cluster:

```bash
# From an Orka checkout — this helper ships with Orka, not with Vekil
.agents/skills/vekil-reverse-proxy-deploy/scripts/deploy_vekil_reverse_proxy.sh \
  --context '<kubectl-context>'
```

Defaults: namespace `vekil-system`, service `vekil`, port `1337`, image
`ghcr.io/sozercan/vekil:latest`, ClusterIP. Those defaults are exactly what Orka expects,
so do not change the namespace or port.

For a workstation:

```bash
docker run -p 1337:1337 \
  -v ~/.config/vekil:/home/nonroot/.config/vekil \
  ghcr.io/sozercan/vekil:latest
```

### Giving it credentials

Point Vekil at a providers file that references Secrets rather than inline keys:

```bash
.agents/skills/vekil-reverse-proxy-deploy/scripts/deploy_vekil_reverse_proxy.sh \
  --context '<kubectl-context>' \
  --providers-config ./providers.yaml \
  --env-secret AZURE_OPENAI_API_KEY=azure-openai:key
```

Use `api_key_env` in that file, not `api_key` — the deploy script stores the config in a
ConfigMap and rejects inline credentials.

With no providers file, Vekil runs in zero-config mode against GitHub Copilot. That needs
a GitHub token:

```bash
# Preferred: reference a Secret you already manage
.agents/skills/vekil-reverse-proxy-deploy/scripts/deploy_vekil_reverse_proxy.sh \
  --env-secret COPILOT_GITHUB_TOKEN=copilot-github-token:token
```

Without a token, Vekil falls back to device-code login and prints a code and URL to its
Pod logs. Deploy with `--skip-wait`, watch `kubectl -n vekil-system logs deploy/vekil`,
complete the login in a browser, then check readiness.

OpenAI Codex providers additionally need an `auth.json` from `codex login`, mounted with
`--codex-auth-secret <secret>[:auth.json]`.

:::tip[Cache the tokens across restarts]
Vekil's token cache is an `emptyDir` by default, so a Pod restart discards cached auth and
may force another login. Pass `--token-pvc <claim>` if that matters.
:::

## Verify Vekil before wiring Orka

```bash
kubectl -n vekil-system port-forward svc/vekil 1337:1337

curl http://127.0.0.1:1337/healthz
curl http://127.0.0.1:1337/readyz
curl http://127.0.0.1:1337/v1/models
```

Do not continue until `/readyz` succeeds and the model you plan to use appears in
`/v1/models`. A Vekil that is up but has no working upstream will make every agent Task
fail with an authentication error that looks like an Orka problem.

:::warning[The base URL differs by client]
Anthropic and Gemini clients use `http://host:1337`. OpenAI and Codex clients use
`http://host:1337/v1`. Getting this wrong produces 404s that look like a broken proxy.
:::

An end-to-end check with a real client:

```bash
env ANTHROPIC_BASE_URL=http://127.0.0.1:1337 ANTHROPIC_API_KEY=dummy \
  claude --model claude-sonnet-4 --print --output-format text "Reply with exactly PROXY_OK"
```

The API key is deliberately `dummy` — Vekil supplies the real one.

## Enable it in Orka

For a fresh Helm installation, complete the in-cluster Vekil setup above, then follow
the [source-install procedure](../getting-started.md#option-b-current-main-from-source).
It includes the required image pins, Secret setup, and `providerProxy.enabled=true`.

Existing `harness-v2` releases already require the proxy to be enabled. Follow
[Upgrading](upgrading.md) to update an existing release.

For Kustomize, use `make deploy` from a matching source checkout. First have the cluster's
CRD owner install the shared `config/crd` bundle and prepare
[`orka-system/orka-admission-tls`](https://github.com/orka-agents/orka/blob/main/config/orka-admission/README.md)
with its serving certificate, private key, and CA bundle. Supply real
`repository@sha256:...` values for `IMG`, `WORKSPACE_PUBLISHER_IMG`, and all four
`ACP_*_RUNTIME_IMG` variables, as shown in the [deployment command](../development/development.md#local-development-with-kind).

`make deploy` checks the shared CRDs and image pins, provisions the system authentication
and snapshot Secrets, and applies `config/acp-production`. That overlay includes the network
policy that permits model traffic only through the auth proxy. Its checked-in image
references are placeholders; direct application does not perform the required setup.

Confirm the Deployment is Ready before submitting agent Tasks:

```bash
kubectl -n orka-system get deploy -l app.kubernetes.io/component=provider-auth-proxy
```

## Rotating the proxy token

The proxy reads its token from a mounted Secret and reloads it every 5 seconds — no
restart needed. To rotate without dropping requests, publish the new token as current and
the old one as `previous-token`, with `previous-token-valid-until` set to an absolute
RFC3339 time. Both are accepted until that deadline, capped at
`providerProxy.previousTokenOverlap` (default 10 minutes, maximum 24 hours).

Order matters depending on which side you roll first:

- **Proxy first** — publish new-as-current and old-as-previous, wait for the reload, then
  roll the controller and runtime pools.
- **Controller first** — pre-stage the new token as `previous-token` while current stays
  old, verify it works through the proxy, then swap and roll. The pre-staged value covers
  a controller request that arrives before the proxy sees the swap.

Remove the old token once every workload reports the new generation.

:::danger[A failed reload stops all forwarding]
If either token file becomes unreadable or invalid, the proxy fails its readiness check and
refuses every authenticated request until both files are valid again. It does not keep
serving with the last known-good value.
:::

## When something is wrong

| Symptom | Where to look |
| --- | --- |
| Chart refuses to install, mentions `upstreamBaseURL` | The value must be exactly `http://vekil.vekil-system.svc:1337`. |
| Agent Tasks fail with an auth error | Check Vekil `/readyz` first; it fails independently of Orka. |
| Vekil `/healthz` passes but `/readyz` fails | Provider auth, upstream reachability, or a missing secret env var. |
| 404s from the proxy | Base URL path — see the warning above. |
| Rotation left requests failing | Check `previous-token-valid-until` is absolute RFC3339 and not in the past. |

More in [Troubleshooting](troubleshooting.md).
