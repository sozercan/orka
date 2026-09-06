import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { api } from '@/lib/api-client'
import { pageParams, walkAllPages, type ListResponse } from '@/lib/list-api'
import { useUIStore } from '@/stores/ui'
import type { Gateway, GatewayBinding, GatewayDelivery, GatewayEvent } from '@/schemas/gateway'

interface GatewayLedgerFilters {
  gateway?: string
  state?: string
  limit?: string
  continue?: string
}

interface GatewayLedgerPaginationState {
  key: string
  cursors: string[]
  pageIndex: number
}

function listAllPages<T>(path: string, params: Record<string, string>) {
  return walkAllPages<T>(
    (continueToken) => api.get<ListResponse<T>>(path, pageParams({ ...params, limit: '500' }, continueToken)),
    { subject: 'Gateway list', path },
  )
}

export function useGatewayLedgerPagination(resetKey: string) {
  const namespace = useUIStore((state) => state.namespace)
  const scopedResetKey = `${namespace}:${resetKey}`
  const initialState = (): GatewayLedgerPaginationState => ({ key: scopedResetKey, cursors: [''], pageIndex: 0 })
  const [saved, setSaved] = useState<GatewayLedgerPaginationState>(initialState)
  const current = saved.key === scopedResetKey ? saved : initialState()

  // React permits a guarded render-time adjustment when state is derived from a changed input.
  // Persist the reset so returning to a previously selected namespace cannot resurrect its cursor.
  if (saved.key !== scopedResetKey) setSaved(current)

  return {
    cursor: current.cursors[current.pageIndex] ?? '',
    page: current.pageIndex + 1,
    hasPrevious: current.pageIndex > 0,
    previous: () => setSaved((state) => {
      const active = state.key === scopedResetKey ? state : initialState()
      return { ...active, pageIndex: Math.max(0, active.pageIndex - 1) }
    }),
    next: (cursor?: string) => {
      if (!cursor) return
      setSaved((state) => {
        const active = state.key === scopedResetKey ? state : initialState()
        const cursors = active.cursors.slice(0, active.pageIndex + 1)
        cursors[active.pageIndex + 1] = cursor
        return { ...active, cursors, pageIndex: active.pageIndex + 1 }
      })
    },
  }
}

export function useGateways() {
  const namespace = useUIStore((state) => state.namespace)
  return useQuery({
    queryKey: ['gateways', namespace],
    queryFn: () => listAllPages<Gateway>('/gateways', { namespace }),
    refetchInterval: 10_000,
  })
}

export function useGateway(name: string) {
  const namespace = useUIStore((state) => state.namespace)
  return useQuery({
    queryKey: ['gateway', namespace, name],
    queryFn: () => api.get<Gateway>(`/gateways/${name}`, { namespace }),
    enabled: !!name,
    refetchInterval: 10_000,
  })
}

export function useGatewayBindings(gateway?: string) {
  const namespace = useUIStore((state) => state.namespace)
  return useQuery({
    queryKey: ['gateway-bindings', namespace, gateway],
    queryFn: async () => {
      const response = await listAllPages<GatewayBinding>('/gatewaybindings', { namespace })
      return gateway
        ? { ...response, items: response.items.filter((binding) => binding.spec.gatewayRef.name === gateway) }
        : response
    },
    refetchInterval: 10_000,
  })
}

export function useGatewayBinding(name: string) {
  const namespace = useUIStore((state) => state.namespace)
  return useQuery({
    queryKey: ['gateway-binding', namespace, name],
    queryFn: () => api.get<GatewayBinding>(`/gatewaybindings/${name}`, { namespace }),
    enabled: !!name,
    refetchInterval: 10_000,
  })
}

export function useGatewayEvents(filters: GatewayLedgerFilters = {}) {
  const namespace = useUIStore((state) => state.namespace)
  return useQuery({
    queryKey: ['gateway-events', namespace, filters],
    queryFn: () => api.get<ListResponse<GatewayEvent>>('/gateway-events', {
      namespace,
      gateway: filters.gateway ?? '',
      state: filters.state ?? '',
      limit: filters.limit ?? '100',
      continue: filters.continue ?? '',
    }),
    refetchInterval: 5_000,
  })
}

export function useGatewayDeliveries(filters: GatewayLedgerFilters = {}) {
  const namespace = useUIStore((state) => state.namespace)
  return useQuery({
    queryKey: ['gateway-deliveries', namespace, filters],
    queryFn: () => api.get<ListResponse<GatewayDelivery>>('/gateway-deliveries', {
      namespace,
      gateway: filters.gateway ?? '',
      state: filters.state ?? '',
      limit: filters.limit ?? '100',
      continue: filters.continue ?? '',
    }),
    refetchInterval: 5_000,
  })
}

export function useRetryGatewayDelivery() {
  const namespace = useUIStore((state) => state.namespace)
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (deliveryId: string) => api.post<GatewayDelivery>(
      `/gateway-deliveries/${deliveryId}/retry`, {}, { namespace },
    ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['gateway-deliveries'] })
    },
    onError: (error) => {
      const message = error instanceof Error ? error.message : 'Unknown error'
      toast.error(`Failed to retry gateway delivery: ${message}`)
    },
  })
}
