---
description: "Orka's vendor-neutral OAuth Transaction Token profile for governing who asked for a piece of work."
---

# Transaction tokens

Orka implements a strict, vendor-neutral OAuth Transaction Token profile for request governance. Transaction tokens are not downstream resource credentials.

## Profile

Configure the canonical profile only:

```yaml
controller:
  contextToken:
    profile: transaction-token
    issuer: https://transactions.example.test
    audience: orka
    headers: Txn-Token
    authzMode: enforce
```

The token must be an RS256 JWT with `typ: txntoken+jwt`, a matching issuer/audience, valid time claims, and non-empty `sub`, `exp`, `iat`, `txn`, `scope`, and `req_wl` claims. `Txn-Token` is the default raw header. `Authorization: Bearer` is opt-in so ServiceAccount and OIDC bearer authentication can coexist.

## Governance retained by Orka

- operation-scope authorization and signed `tctx` constraints
- immutable `spec.requestedBy` and `spec.transaction` provenance
- safe transaction labels, annotations, and context digests
- child scope subset validation
- owner-referenced Secrets for delegated child tokens
- request-time propagation and fail-closed replacement
- credential redaction from status, events, logs, metrics, errors, and Tool results

## Default scopes

When authorization is enabled, Orka defaults to operation-specific scopes such as `orka:tasks:create`, `orka:tasks:get`, `orka:tasks:list`, `orka:tasks:delete`, `orka:tools:read`, `orka:tools:use`, `orka:providers:use`, `orka:agents:read`, `orka:agents:write`, `orka:memory:read`, `orka:memory:write`, `orka:sessions:read`, `orka:sessions:write`, `orka:security:read`, `orka:security:write`, `orka:monitors:read`, `orka:monitors:write`, `orka:monitors:operate`, `orka:skills:read`, and `orka:skills:write`. Using credential-bearing Secrets or minting ServiceAccount tokens for outbound access requires `orka:secrets:credentials:read`. Every default can be replaced with its `--context-token-*-scopes` flag or matching environment/Helm value.

## Exact TTS endpoint

```yaml
controller:
  contextToken:
    tts:
      endpoint: https://transactions.example.test/oauth/token
      audience: orka
      tokenSource: incoming
      childScope: orka:agents:run
      outboundScope: orka:tools:http
```

Equivalent flags and environment variables are `--context-token-tts-endpoint` and `ORKA_CONTEXT_TOKEN_TTS_ENDPOINT`. The value is the exact OAuth endpoint; Orka does not append a path.

Transaction-token TTS calls use RFC 8693, request `urn:ietf:params:oauth:token-type:txn_token`, and require a matching `issued_token_type` plus `token_type=N_A`.

## Provider integrations

Provider installation and live tests are out of tree. See the [Kontxt integration repository](https://github.com/orka-agents/orka-integration-kontxt) and the [agentgateway integration repository](https://github.com/orka-agents/orka-integration-agentgateway).
