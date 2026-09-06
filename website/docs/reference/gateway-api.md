---
description: "The GatewayClass, Gateway, and GatewayBinding resources, field by field."
---

# Generic Gateway API

Orka exposes a provider-neutral external conversation plane through `GatewayClass`, `Gateway`, and `GatewayBinding` plus durable event and delivery ledgers.

## Kubernetes resources

- `GatewayClass` is cluster-scoped and declares `orka.gateway.v1`, category, required capabilities, and allowed metadata keys.
- `Gateway` is namespaced and selects one HTTPS endpoint or TLS-authenticated selector-backed same-namespace Service plus separate inbound/outbound bearer Secrets.
- `GatewayBinding` is namespaced and maps exact normalized external identity to one Agent with bounded Task defaults and deterministic Session routing.

## REST

```text
POST /api/v1/gateways/{namespace}/{name}/events
GET  /api/v1/gatewayclasses
GET  /api/v1/gateways
GET  /api/v1/gatewaybindings
GET  /api/v1/gateway-events
GET  /api/v1/gateway-deliveries
POST /api/v1/gateway-deliveries/{id}/retry
```

Event and delivery list endpoints accept `namespace`, `gateway`, `binding`, `session`, `task`, `state`, and `limit` filters where applicable.

## CLI

```bash
orka gateway class list
orka gateway list
orka gateway binding list
orka gateway events list --state DeadLettered
orka gateway deliveries list --state DeadLettered
orka gateway deliveries retry '<delivery-id>'
```

The dashboard **Gateways** view shows readiness and capabilities, semantic bindings, Session queue correlation, event timelines, delivery attempts, dead letters, and manual retry.

The normative adapter contract is in `docs/development/gateway-protocol-v1.md`.
