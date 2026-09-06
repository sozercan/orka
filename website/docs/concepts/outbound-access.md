---
description: "Giving Tools a reusable, namespaced way to reach an external API without handing agents the credential."
---

# Outbound access policies

`OutboundAccessPolicy` gives HTTP and MCP-over-HTTP Tools one reusable, namespaced access adapter. The Tool and policy must be in the same namespace.

## Direct credential exchange

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: OutboundAccessPolicy
metadata:
  name: resource-api
  namespace: default
spec:
  direct:
    grant: TokenExchange
    tokenEndpoint:
      url: https://identity.example.test/oauth/token
    subject:
      source: TransactionToken
    audiences: [resource-api]
    scopes: [api.read]
    requestedTokenType: urn:ietf:params:oauth:token-type:access_token
    expectedIssuedTokenType: urn:ietf:params:oauth:token-type:access_token
    output:
      header: Authorization
      prefix: "Bearer "
```

Direct mode supports RFC 8693 and RFC 7523, optional actor tokens, arbitrary token-type URNs, audiences, scopes, resources, static non-reserved parameters, ServiceAccount/Secret subjects, client-secret basic/post, and `private_key_jwt`.

Resource responses must be non-empty Bearer tokens with the expected `issued_token_type` for RFC 8693 (RFC 7523 may omit it). `Txn-Token` cannot be the output header, direct mode cannot coexist with `authSecretRef`, and Secret references cannot cross namespaces. Transaction-token scopes cannot expand the parent scope. Context-token Task creation fails closed when a referenced policy is unresolved and requires `orka:secrets:credentials:read` when direct mode reads Secret credentials or mints a ServiceAccount token.

For `ServiceAccount` sources, the controller creates a policy-owned namespaced `Role` whose `serviceaccounts/token` permission is restricted with `resourceNames` to the exact resolved ServiceAccount references. The AI worker is bound only to that Role. Invalid or unresolved policies revoke the binding before execution can mint a token.

The in-process AI worker is a trusted Orka worker; arbitrary task code runs in separate Jobs. These policy grants are shared by trusted AI tasks in the policy namespace. If your threat model includes compromise of the AI worker process itself, use a controller-side credential broker or per-task worker identities rather than enabling ServiceAccount token sources.

## Trusted gateway routing

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: OutboundAccessPolicy
metadata:
  name: agentgateway
  namespace: default
spec:
  gateway:
    serviceRef:
      name: agentgateway
      namespace: gateway-system
      port: 8080
    scheme: http
```

```yaml
spec:
  http:
    url: https://api.example.test/v1/resource?version=1
    outboundAccessPolicyRef:
      name: agentgateway
```

Orka dials the gateway Service but preserves the original target authority, path, query, method, body, explicit `Authorization`, `Txn-Token`, MCP protocol headers, idempotency key, timeout, and cancellation. Orka revalidates public Tool authorities at execution time as defense in depth.

:::danger[Two things the gateway must do that Orka cannot do for you]
- **Strip the transaction token.** Orka forwards `Txn-Token` to the gateway; the final
  downstream must never see it. Configure stripping and resource-token exchange in the
  gateway integration.
- **Bind each authority to a configured upstream.** The allowlisted gateway is the
  enforcement boundary. If it behaves as an unrestricted DNS forward proxy, the allowlist
  means nothing.
:::

Same-namespace Services are automatic. Cross-namespace Services require exact `namespace/name:port` entries:

```yaml
controller:
  outboundAccess:
    trustedGatewayServices:
      - gateway-system/agentgateway:8080
    trustedTokenEndpointServices:
      - identity-system/token-service:8443
```

Wildcards are not supported. Gateway and token-endpoint allowlists are separate.

## Status and safety

Policy status contains only `observedGeneration`, `Accepted`, and `ResolvedRefs`. Reconciliation validates structure, Secret keys, Service ports, ServiceAccounts, TLS CA refs, and trust entries. Tool execution revalidates critical references immediately before use.
