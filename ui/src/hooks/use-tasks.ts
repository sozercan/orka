import { useInfiniteQuery, useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { ApiError, api, isForbiddenError, isNotFoundError } from '@/lib/api-client'
import { isPaginationProtocolError, pageParams, pollUnlessForbidden, retryUnlessForbidden, maxListWalkPages, walkAllPages, type ListResponse } from '@/lib/list-api'
import { useAuthStore } from '@/stores/auth'
import { useUIStore } from '@/stores/ui'
import type { ExecutionEvent, Task, TaskEventsResponse } from '@/schemas/task'

function fetchTaskListPage(namespace: string, limit: string, continueToken?: string) {
  return api.get<ListResponse<Task>>('/tasks', pageParams({ namespace, limit }, continueToken))
}

export function useTaskList(limit = '25', refetchInterval: number | false = 10000) {
  const namespace = useUIStore((s) => s.namespace)
  const token = useAuthStore((s) => s.token)
  return useQuery({
    queryKey: ['tasks', namespace, limit],
    queryFn: () => fetchTaskListPage(namespace, limit),
    enabled: Boolean(token),
    retry: retryUnlessForbidden,
    refetchInterval: pollUnlessForbidden(refetchInterval),
  })
}

// Page-by-page task listing for the Tasks view: the first page loads on its
// own and every later page follows metadata.continue on demand, so a
// namespace with more tasks than one page is never silently truncated.
export function useTaskListPages(limit = '25', refetchInterval: number | false = 10000) {
  const namespace = useUIStore((s) => s.namespace)
  const token = useAuthStore((s) => s.token)
  return useInfiniteQuery({
    queryKey: ['tasks', 'pages', namespace, limit],
    queryFn: ({ pageParam }) => fetchTaskListPage(namespace, limit, pageParam || undefined),
    initialPageParam: '',
    getNextPageParam: (lastPage) => lastPage.metadata?.continue || undefined,
    enabled: Boolean(token),
    retry: retryUnlessForbidden,
    refetchInterval: pollUnlessForbidden(refetchInterval),
  })
}

export function useTaskListAll(pageLimit = '100', refetchInterval: number | false = 10000) {
  const namespace = useUIStore((s) => s.namespace)
  const token = useAuthStore((s) => s.token)
  return useQuery({
    queryKey: ['tasks', 'all', namespace, pageLimit],
    enabled: Boolean(token),
    // Bounded by maxListWalkPages; views built on this walk are summaries and
    // must surface `truncated`.
    queryFn: () => walkAllPages(
      (continueToken) => fetchTaskListPage(namespace, pageLimit, continueToken),
      { subject: 'task list', maxPages: maxListWalkPages },
    ),
    // A 403 is permanent for this identity, and a repeated continuation
    // cursor is a server-side protocol fault: neither improves on retry, so
    // stop retrying (and, for 403, polling) instead of generating denied or
    // looping requests and audit noise.
    retry: (failureCount, error) =>
      !isForbiddenError(error) && !isPaginationProtocolError(error) && failureCount < 3,
    refetchInterval: pollUnlessForbidden(refetchInterval),
  })
}

export function useTask(id: string, refetchInterval: number | false = 5000) {
  const namespace = useUIStore((s) => s.namespace)
  return useQuery({
    queryKey: ['task', id, namespace],
    queryFn: () => api.get<Task>(`/tasks/${id}`, { namespace }),
    // A forbidden task stays forbidden for this token, and a task that was
    // loaded once and now 404s stays deleted; polling those only spams the
    // API. A 404 before the task was ever seen is different: a just-created
    // Task can transiently 404 while the detail read's cache catches up with
    // the list, so polling continues until the task appears.
    retry: (failureCount, error) =>
      !isForbiddenError(error) && !isNotFoundError(error) && failureCount < 3,
    refetchInterval: (query) => {
      if (isForbiddenError(query.state.error)) return false
      if (isNotFoundError(query.state.error) && query.state.data !== undefined) return false
      return refetchInterval
    },
  })
}

export function useTaskResult(id: string) {
  const namespace = useUIStore((s) => s.namespace)
  return useQuery({
    queryKey: ['taskResult', id, namespace],
    queryFn: () =>
      api.get<{ result: string }>(`/tasks/${id}/result`, { namespace }),
    enabled: false,
  })
}

export function useCreateTask() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: Record<string, unknown>) =>
      api.post<Task>('/tasks', body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['tasks'] })
    },
  })
}

export function useDeleteTask() {
  const queryClient = useQueryClient()
  const namespace = useUIStore((s) => s.namespace)
  return useMutation({
    mutationFn: (id: string) => api.delete<void>(`/tasks/${id}`, { namespace }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['tasks'] })
    },
  })
}

const taskEventsPageLimit = '1000'

async function fetchTaskEvents(
  id: string,
  namespace: string,
  previous?: TaskEventsResponse,
): Promise<TaskEventsResponse> {
  let afterSeq = previous?.latestSeq ?? 0
  let targetLatestSeq: number | undefined
  let response: TaskEventsResponse | undefined
  let events: ExecutionEvent[] = [...(previous?.events ?? [])]
  let streamRestarted = false

  let keepFetching = true
  while (keepFetching) {
    const params: Record<string, string> = {
      namespace,
      limit: taskEventsPageLimit,
    }
    if (afterSeq > 0) {
      params.after = String(afterSeq)
    }

    const pageResponse = await api.get<TaskEventsResponse>(
      `/tasks/${id}/events`,
      params,
    )
    if (previous && pageResponse.latestSeq < afterSeq) {
      events = []
      afterSeq = 0
      targetLatestSeq = undefined
      response = undefined
      streamRestarted = true
      continue
    }
    response = pageResponse
    targetLatestSeq ??= pageResponse.latestSeq
    events.push(...pageResponse.events)

    const lastEvent = pageResponse.events[pageResponse.events.length - 1]
    const lastSeq = lastEvent?.seq ?? afterSeq
    if (lastSeq <= afterSeq) {
      keepFetching = false
      continue
    }
    afterSeq = lastSeq
    if (afterSeq >= targetLatestSeq || pageResponse.events.length === 0) {
      keepFetching = false
    }
  }

  const latestFetchedSeq = events.reduce(
    (latest, event) => Math.max(latest, event.seq),
    streamRestarted ? 0 : (previous?.latestSeq ?? 0),
  )

  const fetchedThroughServerLatest = response
    ? response.events.length === 0 || latestFetchedSeq >= response.latestSeq
    : false

  return {
    namespace: response?.namespace ?? previous?.namespace ?? namespace,
    streamType: response?.streamType ?? previous?.streamType ?? 'task',
    streamID: response?.streamID ?? previous?.streamID ?? id,
    afterSeq: previous?.afterSeq ?? 0,
    latestSeq: fetchedThroughServerLatest
      ? Math.max(response?.latestSeq ?? 0, latestFetchedSeq)
      : latestFetchedSeq,
    events,
  }
}

export function useTaskEvents(
  id: string,
  refetchInterval: number | false = 5000,
  taskUID?: string,
  enabled = true,
) {
  const queryClient = useQueryClient()
  const namespace = useUIStore((s) => s.namespace)
  const queryKey = ['taskEvents', id, namespace, taskUID ?? ''] as const
  return useQuery({
    queryKey,
    queryFn: () =>
      fetchTaskEvents(
        id,
        namespace,
        queryClient.getQueryData<TaskEventsResponse>(queryKey),
      ),
    enabled: enabled && Boolean(id),
    // 501 means the feature is off, 404 means the task is gone, and 403 means
    // this token may not read it; none changes on retry, so only transient
    // failures are retried.
    retry: (failureCount, error) =>
      !(error instanceof ApiError && (error.status === 501 || error.status === 404 || error.status === 403)) &&
      failureCount < 3,
    refetchInterval: (query) => {
      const error = query.state.error
      if (!(error instanceof ApiError)) return refetchInterval
      // 501 (feature off) and 403 (forbidden for this token) never change on
      // their own; a 404 after events were seen means the task is gone.
      // Consumers without the detail page's enabled-guard (the runtime
      // canvas spotlight) must not poll those forever. A 404 before any
      // events were seen keeps polling like the task-detail query: a fresh
      // task can transiently 404 while caches catch up.
      if (error.status === 501 || error.status === 403) return false
      if (error.status === 404 && query.state.data !== undefined) return false
      return refetchInterval
    },
  })
}
