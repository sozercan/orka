import { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { RefreshCw, Radio } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { SonarPing } from '@/components/ui/sonar-ping'
import { EmptyState } from '@/components/ui/empty-state'
import { PageHeader } from '@/components/layout/page-header'
import { useTaskListAll, useTaskEvents } from '@/hooks/use-tasks'
import { ApiError } from '@/lib/api-client'
import { useUIStore } from '@/stores/ui'
import { isLiveTask, selectActiveTask, type LatestActivityByTask } from '@/lib/runtime-activity'
import type { Task } from '@/schemas/task'
import { ActivitySpotlight } from './activity-spotlight'
import { AgentsRoster } from './agents-roster'
import { TaskFlowPanel } from './task-flow-panel'

/**
 * Spotlight bound to the active task's latest execution event. Fetching events
 * for the single spotlit task (not every row) keeps the namespace view cheap
 * while still surfacing "latest event summary" — the Phase 2 criterion. Polls
 * only while following; falls back silently to status.message when events are
 * unavailable (501/empty).
 */
function ActiveSpotlight({ task, following }: { task: Task | null; following: boolean }) {
  const { data, error, failureReason } = useTaskEvents(
    task?.metadata.name ?? '',
    following ? 5000 : false,
    task?.metadata.uid,
  )
  const latestEvent = data?.events[data.events.length - 1]
  const issue = error ?? failureReason
  const eventError = Boolean(issue) && !(issue instanceof ApiError && issue.status === 501)
  return <ActivitySpotlight task={task} latestEvent={latestEvent} following={following} eventError={eventError} />
}

/**
 * Orka-native Runtime Canvas (Slice A): a read-only operator view of agent/task
 * execution backed entirely by real Orka Tasks. Slice A is namespace-scoped and
 * exposes no mutating controls beyond refresh and a follow toggle. The store's
 * task source is the only truth; nothing here is synthetic or editable.
 */
export function RuntimeCanvas() {
  const namespace = useUIStore((s) => s.namespace)
  const queryClient = useQueryClient()
  const [following, setFollowing] = useState(true)
  const { data, isLoading, error: taskListError, refetch: refetchTasks } = useTaskListAll('100', following ? 10000 : false)

  const tasks = data?.items ?? []
  // The full-list walk is bounded; beyond that the roster is a prefix and
  // active tasks outside it are invisible, so say so instead of reporting
  // a misleading "0 active".
  const rosterTruncated = Boolean(data?.truncated)
  const runningTasks = tasks.filter(isLiveTask)
  // Read task-event caches populated by focused views (notably the spotlight)
  // without issuing one history request per running task. Without a backend
  // aggregate/latest-activity endpoint, unknown tasks fall back to status/start-time
  // ordering instead of creating namespace-wide event scans.
  const latestActivity = runningTasks.reduce<LatestActivityByTask>((acc, task) => {
    const queryKey = ['taskEvents', task.metadata.name, namespace, task.metadata.uid ?? ''] as const
    const cached = queryClient.getQueryData<{ events?: { createdAt?: string }[] }>(queryKey)
    const events = cached?.events ?? []
    const latestEvent = events[events.length - 1]
    const eventTime = latestEvent?.createdAt ? new Date(latestEvent.createdAt).getTime() : NaN
    if (!Number.isNaN(eventTime)) acc[task.metadata.name] = eventTime
    return acc
  }, {})
  const active = selectActiveTask(tasks, latestActivity)
  const refreshCanvas = () => {
    queryClient.invalidateQueries({ queryKey: ['tasks'] })
    if (active) {
      queryClient.invalidateQueries({
        queryKey: ['taskEvents', active.metadata.name, namespace, active.metadata.uid ?? ''],
      })
    }
  }

  const controls = (
    <div className="flex items-center gap-2">
      <Button
        variant={following ? 'secondary' : 'outline'}
        size="sm"
        onClick={() => setFollowing((f) => !f)}
        aria-pressed={following}
      >
        <Radio className="size-3.5" />
        {following ? 'Following' : 'Paused'}
      </Button>
      <Button
        variant="outline"
        size="sm"
        onClick={refreshCanvas}
      >
        <RefreshCw className="size-3.5" />
        Refresh
      </Button>
    </div>
  )

  return (
    <div className="space-y-4">
      <PageHeader
        title="Runtime Canvas"
        description={`${runningTasks.length} active · namespace ${namespace}`}
        action={controls}
      />

      {isLoading ? (
        <div className="grid gap-4 lg:grid-cols-3">
          <Skeleton className="h-40 w-full lg:col-span-2" />
          <Skeleton className="h-40 w-full" />
        </div>
      ) : taskListError ? (
        <Card>
          <CardContent className="flex flex-col items-center gap-3 py-12">
            <p className="text-sm font-medium text-destructive" role="alert">Failed to load tasks</p>
            <Button variant="outline" size="sm" onClick={() => refetchTasks()}>Retry</Button>
          </CardContent>
        </Card>
      ) : tasks.length === 0 ? (
        <Card>
          <CardContent className="flex flex-col items-center gap-4 py-12">
            {rosterTruncated ? (
              <p className="text-sm text-muted-foreground" role="status">
                Task roster is truncated: only the first pages of this namespace were loaded, so active tasks may be missing.
              </p>
            ) : null}
            <SonarPing />
            <EmptyState
              headline={`No tasks in namespace "${namespace}"`}
              hint="This view shows only the selected namespace. Running agents appear here as tasks start."
              className="py-0"
            />
          </CardContent>
        </Card>
      ) : (
        <>
          {rosterTruncated ? (
            <p className="text-sm text-muted-foreground" role="status">
              Task roster is truncated: only the first pages of this namespace were loaded, so active tasks may be missing.
            </p>
          ) : null}
        <div className="grid gap-4 lg:grid-cols-3">
          <div className="space-y-4 lg:col-span-2">
            <ActiveSpotlight task={active} following={following} />
            <TaskFlowPanel task={active} tasks={runningTasks} />
          </div>
          <AgentsRoster tasks={runningTasks} activeTaskName={active?.metadata.name} latestActivity={latestActivity} />
        </div>
        </>
      )}
    </div>
  )
}
