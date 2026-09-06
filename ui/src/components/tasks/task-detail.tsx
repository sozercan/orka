import { useEffect, useRef, useState } from 'react'
import { Link } from '@tanstack/react-router'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { ArrowLeft, Trash2 } from 'lucide-react'
import { PageHeader } from '@/components/layout/page-header'
import { TaskStatusBadge } from './task-status-badge'
import { PRStatusBadge } from './pr-status-badge'
import { TaskResultViewer } from './task-result-viewer'
import { StructuredLogViewer } from './structured-log-viewer'
import { TaskExecutionPanel } from './task-execution-panel'
import { TaskEventTimeline } from './task-event-timeline'
import { TaskTracePanel } from './task-trace-panel'
import { TaskApprovalPanel } from './task-approval-panel'
import { ForkProvenance } from './fork-provenance'
import { ExecutionGraph } from './execution-graph'
import { RunTimeline } from './run-timeline'
import { TaskRuntimeView } from '@/components/runtime/task-runtime-view'
import { TaskExecutionRouteLedger } from '@/components/execution/execution-route-ledger'
import { useTask, useDeleteTask, useTaskEvents } from '@/hooks/use-tasks'
import { useTaskTrace, useTaskApprovals } from '@/hooks/use-execution-events'
import { useTaskArtifacts } from '@/hooks/use-task-artifacts'
import { ApiError, isForbiddenError, isNotFoundError } from '@/lib/api-client'
import { useNavigate, useSearch } from '@tanstack/react-router'
import type { ExecutionEvent, PlanState } from '@/schemas/task'
import { timeAgo } from '@/lib/time'

function latestEventPlan(events: ExecutionEvent[]): PlanState | undefined {
  let latest: ExecutionEvent | undefined
  for (const event of events) {
    if (event.type === 'PlanUpdated' && (!latest || event.seq > latest.seq)) {
      latest = event
    }
  }
  if (!latest) return undefined

  const content = latest.content
  const fields = content && typeof content === 'object' && !Array.isArray(content)
    ? content as Record<string, unknown>
    : undefined

  return {
    summary: latest.summary,
    progressPct: typeof fields?.progressPct === 'number' ? fields.progressPct : undefined,
    goalComplete: typeof fields?.goalComplete === 'boolean' ? fields.goalComplete : undefined,
    planDocument: latest.contentText,
  }
}

export function TaskDetail({ taskId }: { taskId: string }) {
  const [following, setFollowing] = useState(true)
  const { data: task, isLoading, error: taskError } = useTask(taskId, following ? 5000 : false)
  // Once the primary task query 404s or 403s, every dependent query (events,
  // trace, approvals, artifacts) is disabled so a missing or forbidden task
  // stops polling entirely instead of generating hidden 403 traffic.
  const taskMissing = isNotFoundError(taskError)
  const taskForbidden = isForbiddenError(taskError)
  const taskUnavailable = taskMissing || taskForbidden
  const { data: taskEventsResponse, error: taskEventsError, failureReason: taskEventsFailureReason } = useTaskEvents(
    taskId,
    following ? 5000 : false,
    task?.metadata.uid,
    !taskUnavailable,
  )
  // Fork and the runtime timeline need execution-event storage; a 501 means it's off.
  // While retries are pending, failureReason carries the current fetch failure.
  const taskEventsIssue = taskEventsError ?? taskEventsFailureReason
  const taskEventsUnsupported = taskEventsIssue instanceof ApiError && taskEventsIssue.status === 501
  const taskEventsFailed = Boolean(taskEventsIssue) && !taskEventsUnsupported
  const taskEventsStreamStatus = taskEventsUnsupported ? 'unsupported' : taskEventsFailed ? 'error' : undefined
  const forkSupported = !taskEventsUnsupported && !taskEventsFailed
  const taskEvents = taskEventsResponse?.events ?? []
  const plan = task?.plan ?? latestEventPlan(taskEvents)
  const hasPlanHistory = Boolean(plan) || (task?.status?.iteration ?? 0) > 0 ||
    taskEvents.some((event) => event.type === 'PlanUpdated')
  const deleteTask = useDeleteTask()
  const navigate = useNavigate()
  const search = useSearch({ from: '/tasks/$taskId' })
  // Local override gives instant tab response; it is cleared whenever the URL
  // search changes (deep link, back/forward, external link) so the URL stays the
  // source of truth and the override can't permanently shadow it.
  const [tabState, setTabState] = useState<{ override: string | null; seen?: string }>({ override: null })
  if (tabState.seen !== search.tab) setTabState({ override: null, seen: search.tab })
  const availableTabs = new Set(['runtime', 'overview', 'execution', 'timeline', 'trace', 'approvals', 'result', 'logs'])
  if (hasPlanHistory) availableTabs.add('plan')
  if ((task?.status?.childTasks?.length ?? 0) > 0) availableTabs.add('children')
  const requestedTab = tabState.override ?? search.tab ?? 'runtime'
  const activeTab = availableTabs.has(requestedTab) ? requestedTab : 'runtime'
  const setTab = (tab: string) => {
    setTabState({ override: tab, seen: search.tab })
    navigate({ to: '/tasks/$taskId', params: { taskId }, search: { tab }, replace: true })
  }
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)
  // Scope the armed delete to the loaded task's namespace+uid+name; a namespace
  // switch or same-name/new-uid recreation drops a stale confirm so it can't
  // delete a different task than the one armed.
  const deleteIdentity = `${task?.metadata.namespace ?? ''}/${task?.metadata.uid ?? ''}/${taskId}`
  const deleteArmed = confirmDelete === deleteIdentity
  // Runtime-tab data: fetched only when that tab is active so other tabs don't
  // pay for trace/approvals/artifacts. Each hook is namespace+uid scoped.
  const runtimeActive = activeTab === 'runtime' && !taskUnavailable
  const taskRunning = task?.status?.phase === 'Running'
  const taskTerminal = ['Succeeded', 'Failed', 'Cancelled'].includes(task?.status?.phase ?? '')
  const traceRefetchInterval = runtimeActive && taskRunning && following ? 5000 : false
  const terminalTraceRefetchKey = useRef<string | null>(null)
  const terminalArtifactRefetchKey = useRef<string | null>(null)
  const { data: trace, refetch: refetchTrace } = useTaskTrace(
    taskId,
    runtimeActive,
    task?.metadata.uid,
    traceRefetchInterval,
  )
  // Poll approvals while live so a new blocking approval surfaces in the runtime
  // health panel; stops once terminal (matches TaskApprovalPanel semantics).
  const approvalRefetchInterval = following ? 5000 : undefined
  const { data: approvalsResp } = useTaskApprovals(
    taskId,
    runtimeActive,
    approvalRefetchInterval,
    taskRunning,
    taskTerminal,
    task?.metadata.uid,
  )
  const artifactRefetchInterval = runtimeActive && taskRunning && following ? 5000 : false
  const { data: artifactsResp, refetch: refetchArtifacts } = useTaskArtifacts(
    taskId,
    runtimeActive,
    task?.metadata.uid,
    artifactRefetchInterval,
  )
  useEffect(() => {
    if (!runtimeActive || !taskTerminal || !task?.metadata.uid) return
    const key = `${task.metadata.uid}/${task.status?.phase ?? ''}`
    if (terminalTraceRefetchKey.current !== key) {
      terminalTraceRefetchKey.current = key
      refetchTrace()
    }
    if (terminalArtifactRefetchKey.current !== key) {
      terminalArtifactRefetchKey.current = key
      refetchArtifacts()
    }
  }, [refetchArtifacts, refetchTrace, runtimeActive, task?.metadata.uid, task?.status?.phase, taskTerminal])

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-64 w-full" />
      </div>
    )
  }

  // A 403 is an actionable permission failure, not a missing task; it must
  // not fall through to "Task not found" just because `task` is undefined.
  if (taskForbidden) {
    return (
      <div role="alert" className="space-y-1">
        <p className="text-sm font-medium">Not authorized to view this task</p>
        <p className="text-sm text-muted-foreground">
          Your token lacks <code>tasks</code> read permission ({taskError.message}).
        </p>
      </div>
    )
  }

  // A 404 wins over cached data: React Query keeps the last successful `data`
  // when a refetch fails, so a task deleted after it loaded would otherwise
  // keep rendering as if it still existed.
  if (taskMissing || !task) {
    return <div className="text-muted-foreground">Task not found</div>
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex min-w-0 flex-wrap items-center gap-4">
          <Link to="/tasks">
            <Button variant="ghost" size="icon" aria-label="Back to tasks">
              <ArrowLeft className="h-4 w-4" />
            </Button>
          </Link>
          <PageHeader
            title={task.metadata.name}
            description={`${task.metadata.namespace} · ${task.spec.type}`}
          />
          <TaskStatusBadge phase={task.status?.phase} />
          <PRStatusBadge annotations={task.metadata.annotations} />
        </div>
        <div className="flex items-center gap-2">
          {deleteArmed ? (
            <span className="flex items-center gap-1">
              <Button
                variant="destructive"
                size="sm"
                onClick={async () => {
                  await deleteTask.mutateAsync(task.metadata.name)
                  navigate({ to: '/tasks' })
                }}
              >
                Confirm delete
              </Button>
              <Button variant="ghost" size="sm" onClick={() => setConfirmDelete(null)}>
                Cancel
              </Button>
            </span>
          ) : (
            <Button variant="destructive" size="sm" onClick={() => setConfirmDelete(deleteIdentity)}>
              <Trash2 className="mr-2 h-4 w-4" /> Delete
            </Button>
          )}
        </div>
      </div>

      <TaskExecutionRouteLedger task={task} />

      <Tabs value={activeTab} onValueChange={setTab}>
        <div className="overflow-x-auto">
        <TabsList>
          <TabsTrigger value="runtime">Runtime</TabsTrigger>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="execution">Execution</TabsTrigger>
          <TabsTrigger value="timeline">Timeline</TabsTrigger>
          <TabsTrigger value="trace">Trace</TabsTrigger>
          <TabsTrigger value="approvals">Approvals</TabsTrigger>
          <TabsTrigger value="result">Result</TabsTrigger>
          <TabsTrigger value="logs">Logs</TabsTrigger>
          {hasPlanHistory && (
            <TabsTrigger value="plan">Plan</TabsTrigger>
          )}
          {(task.status?.childTasks?.length ?? 0) > 0 && (
            <TabsTrigger value="children">Children</TabsTrigger>
          )}
        </TabsList>
        </div>

        <TabsContent value="runtime" className="space-y-4">
          <TaskRuntimeView
            task={task}
            events={taskEvents}
            trace={trace}
            approvals={approvalsResp?.approvals}
            artifacts={artifactsResp?.artifacts}
            following={following}
            onToggleFollow={() => setFollowing((f) => !f)}
            forkSupported={forkSupported}
            streamStatus={taskEventsStreamStatus}
            latestSeq={taskEventsResponse?.latestSeq}
            artifactRefetchInterval={artifactRefetchInterval}
          />
        </TabsContent>

        <TabsContent value="overview" className="space-y-4">
          <ForkProvenance annotations={task.metadata.annotations} />
          <Card>
            <CardHeader>
              <CardTitle>Metadata</CardTitle>
            </CardHeader>
            <CardContent className="grid gap-2 text-sm md:grid-cols-2">
              <div>
                <span className="text-muted-foreground">UID:</span>{' '}
                <span className="font-mono text-xs break-all">{task.metadata.uid}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Created:</span>{' '}
                <span className="tabular-nums">
                  {timeAgo(task.metadata.creationTimestamp)}
                </span>
              </div>
              <div>
                <span className="text-muted-foreground">Priority:</span>{' '}
                {task.spec.priority ?? 500}
              </div>
              <div>
                <span className="text-muted-foreground">Attempts:</span>{' '}
                {task.status?.attempts ?? 0}
              </div>
              {task.status?.jobName && (
                <div>
                  <span className="text-muted-foreground">Job:</span>{' '}
                  <span className="font-mono text-xs break-all">
                    {task.status.jobName}
                  </span>
                </div>
              )}
              {task.status?.startTime && (
                <div>
                  <span className="text-muted-foreground">Started:</span>{' '}
                  <span className="tabular-nums">
                    {timeAgo(task.status.startTime)}
                  </span>
                </div>
              )}
              {task.status?.completionTime && (
                <div>
                  <span className="text-muted-foreground">Completed:</span>{' '}
                  <span className="tabular-nums">
                    {timeAgo(task.status.completionTime)}
                  </span>
                </div>
              )}
              {task.status?.message && (
                <div className="md:col-span-2 break-words">
                  <span className="text-muted-foreground">Message:</span>{' '}
                  {task.status.message}
                </div>
              )}
            </CardContent>
          </Card>

          {hasPlanHistory && (
            <Card>
              <CardHeader>
                <CardTitle>Plan</CardTitle>
              </CardHeader>
              <CardContent>
                <RunTimeline
                  task={task}
                  plan={plan}
                  events={taskEvents}
                />
              </CardContent>
            </Card>
          )}

          {task.spec.type === 'container' && (
            <Card>
              <CardHeader>
                <CardTitle>Container Config</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                <div>
                  <span className="text-muted-foreground">Image:</span>{' '}
                  {task.spec.image}
                </div>
                {task.spec.command && (
                  <div>
                    <span className="text-muted-foreground">Command:</span>{' '}
                    {task.spec.command.join(' ')}
                  </div>
                )}
                {task.spec.args && (
                  <div>
                    <span className="text-muted-foreground">Args:</span>{' '}
                    {task.spec.args.join(' ')}
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {task.spec.type === 'ai' && (task.spec.ai || task.spec.agentRef) && (
            <Card>
              <CardHeader>
                <CardTitle>AI Config</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                {task.spec.agentRef && (
                  <div>
                    <span className="text-muted-foreground">Agent:</span>{' '}
                    {!task.spec.agentRef.namespace || task.spec.agentRef.namespace === task.metadata.namespace ? (
                      <Link
                        to="/agents/$agentId"
                        params={{ agentId: task.spec.agentRef.name }}
                        className="underline-offset-4 hover:underline"
                      >
                        {task.spec.agentRef.name}
                      </Link>
                    ) : (
                      // The agent detail route resolves in the dashboard's
                      // current namespace, so a cross-namespace reference is
                      // shown qualified instead of linking to the wrong Agent.
                      <span className="font-mono">
                        {task.spec.agentRef.namespace}/{task.spec.agentRef.name}
                      </span>
                    )}
                  </div>
                )}
                <div>
                  <span className="text-muted-foreground">Provider:</span>{' '}
                  {task.spec.ai?.provider || (task.spec.agentRef ? <span className="text-muted-foreground">Agent default</span> : '-')}
                </div>
                <div>
                  <span className="text-muted-foreground">Model:</span>{' '}
                  {task.spec.ai?.model || (task.spec.agentRef ? <span className="text-muted-foreground">Agent default</span> : '-')}
                </div>
                {task.spec.ai?.prompt && (
                  <div>
                    <span className="text-muted-foreground">Prompt:</span>
                    <pre className="mt-1 overflow-x-auto rounded-md bg-muted p-3 whitespace-pre-wrap break-words">
                      {task.spec.ai.prompt}
                    </pre>
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {task.spec.type === 'agent' && task.spec.agentRef && (
            <Card>
              <CardHeader>
                <CardTitle>Agent Config</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                <div>
                  <span className="text-muted-foreground">Agent:</span>{' '}
                  {task.spec.agentRef.name}
                </div>
                {task.spec.prompt && (
                  <div>
                    <span className="text-muted-foreground">Prompt:</span>
                    <pre className="mt-1 overflow-x-auto rounded-md bg-muted p-3 whitespace-pre-wrap break-words">
                      {task.spec.prompt}
                    </pre>
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {(task.status?.conditions?.length ?? 0) > 0 && (
            <Card>
              <CardHeader>
                <CardTitle>Conditions</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  {task.status!.conditions!.map((c, i) => (
                    <div key={i} className="flex items-center gap-2 text-sm">
                      <Badge
                        variant={c.status === 'True' ? 'default' : 'secondary'}
                      >
                        {c.type}
                      </Badge>
                      <span className="text-muted-foreground">{c.status}</span>
                      {c.message && (
                        <span className="text-muted-foreground">
                          — {c.message}
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        <TabsContent value="result">
          <TaskResultViewer taskId={taskId} />
        </TabsContent>

        <TabsContent value="execution">
          <TaskExecutionPanel task={task} events={taskEvents} />
        </TabsContent>

        <TabsContent value="timeline">
          <TaskEventTimeline
            key={`${task.metadata.namespace ?? ''}/${taskId}/${task.metadata.uid ?? ''}`}
            taskId={taskId}
            taskPhase={task.status?.phase}
            taskUid={task.metadata.uid}
          />
        </TabsContent>

        <TabsContent value="trace">
          <TaskTracePanel taskId={taskId} taskUid={task.metadata.uid} />
        </TabsContent>

        <TabsContent value="approvals">
          <TaskApprovalPanel taskId={taskId} taskPhase={task.status?.phase} taskUid={task.metadata.uid} />
        </TabsContent>

        <TabsContent value="logs">
          <StructuredLogViewer taskId={taskId} taskPhase={task.status?.phase} />
        </TabsContent>

        {hasPlanHistory && (
          <TabsContent value="plan">
            <Card>
              <CardHeader>
                <CardTitle>Agent Plan</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <RunTimeline task={task} plan={plan} events={taskEvents} />
                  {plan?.planDocument && (
                    <div>
                      <p className="mb-1 text-xs font-medium text-muted-foreground">
                        Plan document
                      </p>
                      <pre className="rounded-md bg-muted p-4 whitespace-pre-wrap break-words max-h-[600px] overflow-auto text-xs">
                        {plan.planDocument}
                      </pre>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        )}

        {(task.status?.childTasks?.length ?? 0) > 0 && (
          <TabsContent value="children">
            <Card>
              <CardHeader>
                <CardTitle>Execution Graph</CardTitle>
              </CardHeader>
              <CardContent>
                <ExecutionGraph task={task} events={taskEvents} />
              </CardContent>
            </Card>
          </TabsContent>
        )}
      </Tabs>
    </div>
  )
}
