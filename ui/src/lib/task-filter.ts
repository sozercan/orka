import type { Task } from '@/schemas/task'
import { taskTypeLabel } from '@/lib/task-status'

// matchesTaskFilter is a client-side name/type/phase search over the pages
// loaded so far; it never hides the "Load more" control, so a task that is
// not loaded yet can still be reached.
export function matchesTaskFilter(task: Task, filter: string): boolean {
  const needle = filter.trim().toLowerCase()
  if (!needle) return true
  const haystack = [task.metadata.name, task.spec.type, task.status?.phase ?? '', taskTypeLabel(task.spec.type)]
    .join(' ')
    .toLowerCase()
  return needle.split(/\s+/).every((term) => haystack.includes(term))
}

