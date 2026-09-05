# Examples

These manifests are working starting points, not abstract templates: CI
strict-decodes every Orka document in this directory against the typed API
(unknown fields fail the build) and runs each Agent through the same admission
contract a live cluster enforces.

## What's here

| Example | What it shows | Read more |
| --- | --- | --- |
| [`tavily/`](tavily) | Using Tavily as an alternative to built-in web search by declaring an HTTP API as a `Tool` | [Tool CRD schema](../website/docs/reference/api-reference.md#tool-crd-schema) |
| [`iterative-review/`](iterative-review) | One code-and-review pass: opens a PR on approval, or reports requested changes and stops | [Multi-agent coordination](../website/docs/reference/multi-agent-coordination.md) |
| [`github-cicd/`](github-cicd) | Delegating a write-intent Task to a Claude runtime and publishing the result through the clean-room publisher | [Agent runtimes](../website/docs/concepts/agent-runtimes.md) |
| [`self-bootstrapping/`](self-bootstrapping) | A coordinator that creates the specialist agents it needs, then delegates to them | [Multi-agent coordination](../website/docs/reference/multi-agent-coordination.md) |
| [`autonomous-task.yaml`](autonomous-task.yaml) | An autonomous coordinator that plans, executes, and re-plans until a goal is met | [Autonomous Task execution](../website/docs/guides/autonomous-tasks.md) |
| [`github-label-trigger/`](github-label-trigger) | Turning a GitHub label into an agent Task via a signed webhook | [GitHub label triggers](../website/docs/guides/github-label-triggers.md) |
| [`github-label-triggered-issue-loop/`](github-label-triggered-issue-loop) | The `orka:*` label workflow from issue through implementation, PR creation, and review; automerge is disabled by default | [Issue-to-PR automation](../website/docs/guides/issue-to-pr-automation.md) |
| [`repository-monitor-issue-plan-only/`](repository-monitor-issue-plan-only) | Durable triage and planning records, with no code written | [Repository monitors](../website/docs/guides/repository-monitors.md) |
| [`repository-monitor-pr-review-repair/`](repository-monitor-pr-review-repair) | Reviewing PRs, pushing repairs, and optional automerge command labels | [Repository monitors](../website/docs/guides/repository-monitors.md) |

Each subdirectory has its own README with the exact apply commands and the Secrets it
needs.

## Two things to change before you apply anything

**Apply into the controller's watched namespace.** The static harness-v2 controller
reconciles exactly one namespace (`--watch-namespace`, which is `orka-system` for
`make deploy` and the Helm chart). No manifest here carries a `metadata.namespace`, so pass
it explicitly, for example
`kubectl -n orka-system apply -k examples/repository-monitor-pr-review-repair`; a resource
created anywhere else is never reconciled.

The same applies to the Secrets each example needs. Secret references are namespace-local,
so a Secret created in `default` is invisible to a monitor in `orka-system` — the commands
in each README use `-n orka-system` for exactly this reason.

**Models and providers are placeholders.** Agent `spec.model.name`, Provider names, and
credential Secret names reflect one working setup. Swap in the models and Secrets that
exist in your cluster. Note that built-in runtime Agents *must* set `spec.model.name` — the
ACP session has no default model, and admission rejects an Agent without one.

New to any of these terms? The [glossary](../website/docs/reference/glossary.md) defines
them once.
