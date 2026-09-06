import { renderHook, waitFor, act } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'

vi.mock('zustand/middleware', () => ({
  persist: (fn: unknown) => fn,
}))

import { useUIStore } from '@/stores/ui'
import { useAuthStore } from '@/stores/auth'
import {
  useAgentList,
  useAgent,
  useCreateAgent,
  useDeleteAgent,
} from './use-agents'

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 }, mutations: { retry: false } },
  })
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
}

beforeEach(() => {
  useAuthStore.setState({ token: 'test-token' })
  useUIStore.setState({ namespace: 'default', sidebarCollapsed: false, theme: 'light' })
})

describe('useAgentList', () => {
  it('returns agent list from API', async () => {
    const { result } = renderHook(() => useAgentList(), { wrapper: createWrapper() })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data).toEqual({ items: [], metadata: {} })
  })
})

describe('useAgent', () => {
  it('returns a single agent by name', async () => {
    const { result } = renderHook(() => useAgent('my-agent'), { wrapper: createWrapper() })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data).toMatchObject({
      metadata: { name: 'my-agent', namespace: 'default' },
    })
  })
})

describe('useCreateAgent', () => {
  it('creates an agent via mutation', async () => {
    const { result } = renderHook(() => useCreateAgent(), { wrapper: createWrapper() })
    act(() => {
      result.current.mutate({ name: 'new-agent' })
    })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data).toMatchObject({ metadata: { name: 'new-agent' } })
  })
})

describe('useDeleteAgent', () => {
  it('deletes an agent via mutation', async () => {
    const { result } = renderHook(() => useDeleteAgent(), { wrapper: createWrapper() })
    act(() => {
      result.current.mutate('my-agent')
    })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
  })
})
