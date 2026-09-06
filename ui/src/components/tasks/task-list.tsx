import { useMemo, useState } from 'react'
import { Link } from '@tanstack/react-router'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'
import { ListAccessError } from '@/components/ui/list-access-error'
import { Plus, Trash2 } from 'lucide-react'
import { PageHeader } from '@/components/layout/page-header'
import { TaskStatusBadge } from './task-status-badge'
import { useTaskListPages, useDeleteTask } from '@/hooks/use-tasks'
import type { Task } from '@/schemas/task'
import { taskTypeLabel } from '@/lib/task-status'
import { matchesTaskFilter } from '@/lib/task-filter'
import { timeAgo } from '@/lib/time'

export function TaskList() {
  const { data, isLoading, error, fetchNextPage, hasNextPage, isFetchingNextPage } = useTaskListPages()
  const deleteTask = useDeleteTask()
  const [filter, setFilter] = useState('')

  // An errored list must not keep rendering a previous token's or
  // namespace's rows; the access panel replaces it.
  const tasks = useMemo(() => (error ? [] : (data?.pages ?? []).flatMap((page) => page.items)), [data, error])
  const visible = useMemo(() => tasks.filter((task) => matchesTaskFilter(task, filter)), [tasks, filter])
  const lastPage = data?.pages[data.pages.length - 1]
  const remaining = lastPage?.metadata?.remainingItemCount

  return (
    <div className="space-y-4">
      <PageHeader
        title="Tasks"
        description="Manage your task execution"
        action={
          <Link to="/tasks/new">
            <Button>
              <Plus className="mr-2 h-4 w-4" />
              New Task
            </Button>
          </Link>
        }
      />
      <div className="flex flex-wrap items-center gap-2">
        <Input
          aria-label="Filter tasks"
          placeholder="Filter by name, type, or phase"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="h-9 max-w-sm"
        />
        {tasks.length > 0 && (
          <span className="text-xs text-muted-foreground">
            {filter ? `${visible.length} of ${tasks.length} loaded` : `${tasks.length} loaded`}
            {hasNextPage ? ' · more available' : ''}
          </span>
        )}
      </div>
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Type</TableHead>
              <TableHead>Phase</TableHead>
              <TableHead>Namespace</TableHead>
              <TableHead>Age</TableHead>
              <TableHead className="w-12"></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <TableRow key={i}>
                  {Array.from({ length: 6 }).map((_, j) => (
                    <TableCell key={j}><Skeleton className="h-4 w-20" /></TableCell>
                  ))}
                </TableRow>
              ))
            ) : error ? (
              <TableRow>
                <TableCell colSpan={6} className="p-0">
                  <ListAccessError error={error} resource="tasks" />
                </TableCell>
              </TableRow>
            ) : tasks.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                  No tasks found. Create one to get started.
                </TableCell>
              </TableRow>
            ) : visible.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                  No loaded tasks match "{filter.trim()}".
                </TableCell>
              </TableRow>
            ) : (
              visible.map((task: Task) => (
                <TableRow key={task.metadata.uid || task.metadata.name} className="cursor-pointer">
                  <TableCell>
                    <Link to="/tasks/$taskId" params={{ taskId: task.metadata.name }} className="font-mono text-sm font-medium hover:underline">
                      {task.metadata.name}
                    </Link>
                  </TableCell>
                  <TableCell>{taskTypeLabel(task.spec.type)}</TableCell>
                  <TableCell><TaskStatusBadge phase={task.status?.phase} /></TableCell>
                  <TableCell>{task.metadata.namespace}</TableCell>
                  <TableCell>{timeAgo(task.metadata.creationTimestamp, { compact: true })}</TableCell>
                  <TableCell>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={(e) => {
                        e.preventDefault()
                        e.stopPropagation()
                        deleteTask.mutate(task.metadata.name)
                      }}
                    >
                      <Trash2 className="h-4 w-4 text-muted-foreground" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
      {hasNextPage && (
        <div className="flex justify-center">
          <Button variant="outline" onClick={() => fetchNextPage()} disabled={isFetchingNextPage}>
            {isFetchingNextPage ? 'Loading…' : remaining ? `Load more (${remaining} remaining)` : 'Load more'}
          </Button>
        </div>
      )}
    </div>
  )
}
