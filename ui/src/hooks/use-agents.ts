import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api-client'
import { pageParams, retryUnlessForbidden, walkAllPages, type ListResponse } from '@/lib/list-api'
import { useAuthStore } from '@/stores/auth'
import { useUIStore } from '@/stores/ui'
import type { Agent } from '@/schemas/agent'

interface AgentListOptions {
  namespace?: string
  enabled?: boolean
}

export function useAgentList(options: AgentListOptions = {}) {
  const selectedNamespace = useUIStore((s) => s.namespace)
  const namespace = options.namespace ?? selectedNamespace
  const token = useAuthStore((s) => s.token)
  return useQuery({
    queryKey: ['agents', namespace],
    queryFn: () => api.get<ListResponse<Agent>>('/agents', { namespace }),
    enabled: Boolean(token) && (options.enabled ?? true),
    retry: retryUnlessForbidden,
  })
}

// Follows metadata.continue so selectors that must see every Agent in a
// namespace (for example the task creation form) are not capped at one page.
export function useAgentListAll(options: AgentListOptions = {}) {
  const selectedNamespace = useUIStore((s) => s.namespace)
  const namespace = options.namespace ?? selectedNamespace
  const token = useAuthStore((s) => s.token)
  return useQuery({
    queryKey: ['agents', 'all', namespace],
    queryFn: () => walkAllPages(
      (continueToken) => api.get<ListResponse<Agent>>('/agents', pageParams({ namespace, limit: '100' }, continueToken)),
      { subject: 'agent list' },
    ),
    enabled: Boolean(token) && (options.enabled ?? true),
  })
}

export function useAgent(name: string) {
  const namespace = useUIStore((s) => s.namespace)
  return useQuery({
    queryKey: ['agent', name, namespace],
    queryFn: () => api.get<Agent>(`/agents/${name}`, { namespace }),
  })
}

export function useCreateAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: Record<string, unknown>) => api.post<Agent>('/agents', body),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['agents'] }) },
  })
}

export function useDeleteAgent() {
  const queryClient = useQueryClient()
  const namespace = useUIStore((s) => s.namespace)
  return useMutation({
    mutationFn: (name: string) => api.delete<void>(`/agents/${name}`, { namespace }),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['agents'] }) },
  })
}
