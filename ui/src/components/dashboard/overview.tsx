import { useTaskListAll } from '@/hooks/use-tasks'
import { useSessionListAll } from '@/hooks/use-sessions'
import { useAgentListAll } from '@/hooks/use-agents'
import { useToolListAll } from '@/hooks/use-tools'
import { isForbiddenError } from '@/lib/api-client'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { PageHeader } from '@/components/layout/page-header'
import { Distribution } from '@/components/ui/distribution'
import { taskPhaseSchema } from '@/schemas/task'
import { StatsCards } from './stats-cards'
import { RecentTasks } from './recent-tasks'

// Drive the distribution from the full phase enum (Pending/Running/Succeeded/
// Failed/Scheduled/Cancelled) so every task the Total card counts is also
// represented here — the segment counts always sum to the task total.
const PHASES = taskPhaseSchema.options

export function Overview() {
  // The dashboard is a glanceable summary: it polls the bounded list walks
  // at a relaxed cadence instead of hammering full-history queries.
  const { data: tasksData, isLoading: tasksLoading, error: tasksError } = useTaskListAll('100', 60000)
  const { data: sessionsData, isLoading: sessionsLoading, error: sessionsError } = useSessionListAll('100', 60000)
  const { data: agentsData, isLoading: agentsLoading, error: agentsError } = useAgentListAll()
  const { data: toolsData, isLoading: toolsLoading, error: toolsError } = useToolListAll()

  const isLoading = tasksLoading || sessionsLoading || agentsLoading || toolsLoading
  // A 403 leaves `data` undefined; surface it per resource instead of letting
  // the missing collection render as a fabricated zero.
  const forbidden = (error: unknown) => (isForbiddenError(error) ? error.message : undefined)
  const tasksForbiddenMessage = forbidden(tasksError)

  const tasks = tasksData?.items ?? []
  const tasksTruncated = tasksData?.truncated ?? Boolean(tasksData?.metadata?.continue)
  const sessionsTruncated = sessionsData?.truncated ?? Boolean(sessionsData?.metadata?.continue)
  const distribution = PHASES.map((phase) => ({
    phase,
    count: tasks.filter((t) => (t.status?.phase ?? 'Pending') === phase).length,
  })).filter((seg) => seg.count > 0)

  return (
    <div className="space-y-6">
      <PageHeader title="Dashboard" description="Overview of your Orka workspace" />
      {tasksTruncated && !tasksForbiddenMessage && (
        <p className="text-sm text-muted-foreground" role="status">
          Task counts and phase distribution use {tasks.length.toLocaleString()} loaded tasks in resource-key order. More tasks exist.
        </p>
      )}
      <StatsCards
        tasks={tasksData?.items}
        tasksTruncated={tasksTruncated}
        tasksForbiddenMessage={tasksForbiddenMessage}
        sessionCount={sessionsData?.items?.length}
        sessionsTruncated={sessionsTruncated}
        sessionsForbiddenMessage={forbidden(sessionsError)}
        agentCount={agentsData?.items?.length}
        agentsForbiddenMessage={forbidden(agentsError)}
        toolCount={toolsData?.items?.length}
        toolsForbiddenMessage={forbidden(toolsError)}
        isLoading={isLoading}
      />
      <div className="grid gap-6 lg:grid-cols-3">
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle className="text-sm font-medium">
              {tasksTruncated ? 'Loaded Phase Distribution' : 'Phase Distribution'}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {tasksForbiddenMessage ? (
              <p className="text-sm text-muted-foreground" role="alert">
                Not authorized to list tasks ({tasksForbiddenMessage}).
              </p>
            ) : (
              <Distribution segments={distribution} />
            )}
          </CardContent>
        </Card>
        <div className="lg:col-span-2">
          <RecentTasks
            tasks={tasksError ? undefined : tasksData?.items}
            isLoading={tasksLoading}
            forbiddenMessage={tasksForbiddenMessage}
            isTruncated={tasksTruncated}
          />
        </div>
      </div>
    </div>
  )
}
