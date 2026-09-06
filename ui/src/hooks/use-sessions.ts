import { useInfiniteQuery, useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { ApiError, api } from '@/lib/api-client'
import { pageParams, pollUnlessForbidden, maxListWalkPages, walkAllPages, type ListResponse } from '@/lib/list-api'
import { useAuthStore } from '@/stores/auth'
import { useUIStore } from '@/stores/ui'
import type { Session, SessionListItem } from '@/schemas/session'

// Client errors (403/404/400) do not clear on retry; only transient failures
// do. 408 (request timeout) and 429 (throttled) are client statuses that an
// ingress or API throttle returns transiently, so they keep retrying.
const transientClientStatuses = new Set([408, 429])
export const retryUnlessClientError = (failureCount: number, error: unknown) =>
  failureCount < 3 &&
  !(error instanceof ApiError && error.status >= 400 && error.status < 500 && !transientClientStatuses.has(error.status))
// Page-by-page session listing for the Sessions view; later pages follow
// metadata.continue on demand instead of stopping at the first page.
export function useSessionListPages(limit = '25', refetchInterval: number | false = 15000) {
  const namespace = useUIStore((s) => s.namespace)
  const token = useAuthStore((s) => s.token)
  return useInfiniteQuery({
    queryKey: ['sessions', 'pages', namespace, limit],
    queryFn: ({ pageParam }) =>
      api.get<ListResponse<SessionListItem>>('/sessions', pageParams({ namespace, limit }, pageParam || undefined)),
    initialPageParam: '',
    getNextPageParam: (lastPage) => lastPage.metadata?.continue || undefined,
    enabled: Boolean(token),
    retry: retryUnlessClientError,
    refetchInterval: pollUnlessForbidden(refetchInterval),
  })
}

export function useSessionListAll(pageLimit = '100', refetchInterval: number | false = 15000) {
  const namespace = useUIStore((s) => s.namespace)
  const token = useAuthStore((s) => s.token)
  return useQuery({
    queryKey: ['sessions', 'all', namespace, pageLimit],
    enabled: Boolean(token),
    // Bounded like every full-list walk (maxListWalkPages): unbounded history
    // on a polling interval grows without limit.
    queryFn: () => walkAllPages(
      (continueToken) => api.get<ListResponse<SessionListItem>>('/sessions', pageParams({ namespace, limit: pageLimit }, continueToken)),
      { subject: 'session list', maxPages: maxListWalkPages },
    ),
    retry: retryUnlessClientError,
    refetchInterval: pollUnlessForbidden(refetchInterval),
  })
}

export function useSession(id: string) {
  const namespace = useUIStore((s) => s.namespace)
  return useQuery({
    queryKey: ['session', id, namespace],
    queryFn: () => api.get<Session>(`/sessions/${id}`, { namespace }),
    retry: retryUnlessClientError,
  })
}

export function useDeleteSession() {
  const queryClient = useQueryClient()
  const namespace = useUIStore((s) => s.namespace)
  return useMutation({
    mutationFn: (id: string) => api.delete<void>(`/sessions/${id}`, { namespace }),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['sessions'] }) },
  })
}
