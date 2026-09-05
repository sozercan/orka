---
description: "Running any Orka Task on a cron schedule."
---

# Scheduled Tasks

Any Orka Task can run on a schedule. Set `spec.schedule` to a cron expression
and the Task stops being a one-shot job: it becomes a template, and the
controller creates a fresh child Task from it on every cron tick. Each child
runs independently with the full spec — the same agent, workspace, and tools a
manual Task would get.

If you have used a Kubernetes CronJob, the model will feel familiar; the
details below are where scheduled Tasks make their own choices.

## A minimal scheduled Task

```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: nightly-triage
spec:
  type: ai
  agentRef:
    name: triage-agent
  prompt: "Summarize new issues opened in the last 24 hours"
  schedule: "0 2 * * *"        # every day at 02:00
  timeZone: "America/New_York" # IANA name; defaults to UTC
```

The parent Task never runs itself. Its status tracks the schedule
(`status.lastScheduleTime`, `status.nextScheduleTime`), and each tick creates a
child named `<parent>-<unix-timestamp>` — so `kubectl get tasks` shows the
history at a glance.

## Fields that shape the schedule

| Field | Default | What it controls |
|---|---|---|
| `schedule` | — | Standard cron expression; setting it makes the Task scheduled |
| `timeZone` | UTC | IANA time zone the cron expression is evaluated in |
| `concurrencyPolicy` | `Forbid` | `Forbid` skips a tick while the previous child still runs; `Allow` lets runs overlap |
| `startingDeadlineSeconds` | `100` | How late a run may start and still count (see below) |
| `successfulRunsHistoryLimit` | `3` | Completed successful children retained before cleanup |
| `failedRunsHistoryLimit` | `1` | Failed children retained before cleanup |
| `suspend` | `false` | Pauses new runs without deleting the schedule |

## Missed runs and the starting deadline

A tick can be missed — the controller was restarting, or the schedule was
suspended when the tick came due. `startingDeadlineSeconds` decides what
happens next:

- Missed by **less** than the deadline: the run starts late, once.
- Missed by **more** than the deadline: the run is **skipped**. Scheduled
  Tasks never queue a backlog of catch-up runs.

When a run is skipped this way, the schedule re-anchors to the present: the
next run happens at the next cron tick from now, as if the missed window never
existed.

## Suspending and resuming

Set `spec.suspend: true` to pause a schedule. Children that are already
running finish normally; no new children are created while suspended.

Set it back to `false` to resume. If the suspension outlasted
`startingDeadlineSeconds` (it usually does), the missed ticks are skipped —
not replayed — and runs continue from the next cron tick after the resume.
Resuming a schedule that was paused for a week does not fire a week of
backlogged runs.

## Operational notes

- **History is pruned automatically.** Only the most recent successful and
  failed children (per the history limits) are kept; raise the limits if you
  audit past runs.
- **`Forbid` is the safe default** for agent work. An agent run that overlaps
  itself — two children editing the same repository, for example — rarely does
  what you want. Use `Allow` only for idempotent, read-only work.
- Scheduled Tasks are also the mechanism behind prompt-orchestrated monitors
  such as `create_pr_monitor`; see
  [Repository Monitors](repository-monitors.md) for the purpose-built
  monitoring resource that supersedes hand-rolled schedules for that use case.
