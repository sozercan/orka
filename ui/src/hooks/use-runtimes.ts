import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api-client'
import { pageParams, walkAllPages, type ListResponse } from '@/lib/list-api'
import { agentRuntimeListSchema, runtimePoolListSchema } from '@/schemas/runtime'
import { useUIStore } from '@/stores/ui'

interface RuntimeListPageSchema<T> {
  parse(value: unknown): ListResponse<T>
}

function fetchAllRuntimePages<T>(
  path: '/runtime-pools' | '/agent-runtimes',
  namespace: string,
  schema: RuntimeListPageSchema<T>,
) {
  return walkAllPages<T>(
    async (continueToken) => schema.parse(await api.get<unknown>(path, pageParams({ namespace, limit: '100' }, continueToken))),
    { subject: 'runtime list', path },
  )
}

export function useRuntimePools(refetchInterval: number | false = 5000) {
  const namespace = useUIStore((state) => state.namespace)
  return useQuery({
    queryKey: ['runtime-pools', namespace],
    queryFn: () => fetchAllRuntimePages('/runtime-pools', namespace, runtimePoolListSchema),
    refetchInterval,
  })
}

export function useAgentRuntimes(refetchInterval: number | false = 10000) {
  const namespace = useUIStore((state) => state.namespace)
  return useQuery({
    queryKey: ['agent-runtimes', namespace],
    queryFn: () => fetchAllRuntimePages('/agent-runtimes', namespace, agentRuntimeListSchema),
    refetchInterval,
  })
}
