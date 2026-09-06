import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api-client'
import { pageParams, walkAllPages, type ListResponse } from '@/lib/list-api'
import { useAuthStore } from '@/stores/auth'
import { useUIStore } from '@/stores/ui'
import type { ToolListItem, Tool } from '@/schemas/tool'

export function useToolList() {
  const namespace = useUIStore((s) => s.namespace)
  const token = useAuthStore((s) => s.token)
  return useQuery({
    queryKey: ['tools', namespace],
    queryFn: () => api.get<ListResponse<ToolListItem>>('/tools', { namespace }),
    enabled: Boolean(token),
  })
}

export function useToolListAll(pageLimit = '100') {
  const namespace = useUIStore((s) => s.namespace)
  const token = useAuthStore((s) => s.token)
  return useQuery({
    queryKey: ['tools', 'all', namespace, pageLimit],
    enabled: Boolean(token),
    queryFn: () => walkAllPages(
      (continueToken) => api.get<ListResponse<ToolListItem>>('/tools', pageParams({ namespace, limit: pageLimit }, continueToken)),
      { subject: 'tool list' },
    ),
  })
}

export function useTool(name: string) {
  const namespace = useUIStore((s) => s.namespace)
  return useQuery({
    queryKey: ['tool', name, namespace],
    queryFn: () => api.get<Tool>(`/tools/${name}`, { namespace }),
  })
}
