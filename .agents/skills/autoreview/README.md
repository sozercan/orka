# Autoreview

This directory tracks the OpenClaw `autoreview` runtime with three narrow downstream behavior overrides.

## Upstream

- Repository: [`openclaw/agent-skills`](https://github.com/openclaw/agent-skills)
- Source snapshot: [`skills/autoreview` at commit `3f392d7531673127ebfa9ed87148e64c26c8153f`](https://github.com/openclaw/agent-skills/tree/3f392d7531673127ebfa9ed87148e64c26c8153f/skills/autoreview)
- Commit: [`3f392d7531673127ebfa9ed87148e64c26c8153f`](https://github.com/openclaw/agent-skills/commit/3f392d7531673127ebfa9ed87148e64c26c8153f)

The vendored runtime starts from that snapshot and carries only the behavior overrides below.
`AGENTS.md` records the downstream sync policy.

## Local differences

- Codex defaults to `gpt-6-astra` with `max` reasoning instead of upstream's `gpt-5.6-sol` with `high` reasoning.
- Claude defaults to `claude-opus-5` with `max` reasoning instead of upstream's `claude-fable-5` default.
- Codex preserves `openai_base_url` from an external `CODEX_HOME/config.toml`, passes it through an internal OpenAI-compatible provider because isolated runs use `--ignore-user-config`, and disables WebSocket transport for that provider.

No other runtime behavior is intentionally changed.

## Packaging

The upstream test suites, fixtures, and optional `scripts/test-review-harness*` live-provider smoke wrappers are omitted. `scripts/autoreview` does not depend on them. A small downstream test covers only the three local behavior overrides.

## Upstream license

OpenClaw's work is used under the MIT License:

```text
MIT License

Copyright (c) 2026 openclaw

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
