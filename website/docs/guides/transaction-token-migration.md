---
description: "Moving from the removed provider-specific transaction-token profile to the generic one."
---

# Migration: provider-specific transaction tokens to the generic profile

This release intentionally removes the in-tree Kontxt profile and deployment assets.

1. Change `--context-token-profile=kontxt` to `--context-token-profile=transaction-token` (or the matching environment/Helm value).
2. Replace the former TTS base URL with the exact endpoint:
   - `--context-token-tts-endpoint`
   - `ORKA_CONTEXT_TOKEN_TTS_ENDPOINT`
   - `controller.contextToken.tts.endpoint`
3. Move provider installation, signing/JWKS, and live E2E configuration to [`orka-agents/orka-integration-kontxt`](https://github.com/orka-agents/orka-integration-kontxt).
4. Use [`orka-agents/orka-integration-agentgateway`](https://github.com/orka-agents/orka-integration-agentgateway) for gateway routing and downstream OAuth exchange examples.
5. Remove the old URL flag/environment/value; there is no compatibility alias.

Rollback requires deploying the previous Orka release and restoring its old Kontxt configuration. Do not run the previous controller with the new exact-endpoint-only values.
