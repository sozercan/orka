import { renderHook, waitFor, act } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { http, HttpResponse } from 'msw'
import type { ReactNode } from 'react'
import { server } from '@/test/mocks/server'

vi.mock('zustand/middleware', () => ({
  persist: (fn: unknown) => fn,
}))

import { useUIStore } from '@/stores/ui'
import { useAuthStore } from '@/stores/auth'
import {
  useTaskList,
  useTaskListAll,
  useTask,
  useTaskResult,
  useCreateTask,
  useDeleteTask,
  useTaskEvents,
} from './use-tasks'
import { maxListWalkPages } from '@/lib/list-api'

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

describe('useTaskList', () => {
  it('returns task list from API', async () => {
    const { result } = renderHook(() => useTaskList(), { wrapper: createWrapper() })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data).toEqual({ items: [], metadata: {} })
  })
})

describe('useTaskListAll', () => {
  it('follows continue tokens and returns all task pages', async () => {
    const seen: (string | null)[] = []
    server.use(http.get('/api/v1/tasks', ({ request }) => {
      const token = new URL(request.url).searchParams.get('continue')
      seen.push(token)
      if (!token) {
        return HttpResponse.json({ items: [], metadata: { continue: 'next-page' } })
      }
      return HttpResponse.json({
        items: [{ metadata: { name: 'late-running', namespace: 'default', uid: 'late' }, spec: { type: 'agent' }, status: { phase: 'Running' } }],
        metadata: {},
      })
    }))

    const { result } = renderHook(() => useTaskListAll('100', false), { wrapper: createWrapper() })

    await waitFor(() => expect(result.current.data?.items[0]?.metadata.name).toBe('late-running'))
    expect(seen).toEqual([null, 'next-page'])
  })

  it('rejects a repeated continuation cursor', async () => {
    server.use(http.get('/api/v1/tasks', () =>
      HttpResponse.json({ items: [], metadata: { continue: 'same-page' } }),
    ))

    const { result } = renderHook(() => useTaskListAll('100', false), { wrapper: createWrapper() })
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.error).toEqual(new Error('task list pagination repeated continuation cursor'))
  })

  it('reports when the bounded list walk stops before the collection ends', async () => {
    let calls = 0
    server.use(http.get('/api/v1/tasks', () => {
      calls += 1
      return HttpResponse.json({
        items: [{ metadata: { name: `task-${calls}`, namespace: 'default', uid: `uid-${calls}` }, spec: { type: 'agent' } }],
        metadata: { continue: `page-${calls}` },
      })
    }))

    const { result } = renderHook(() => useTaskListAll('100', false), { wrapper: createWrapper() })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(calls).toBe(maxListWalkPages)
    expect(result.current.data?.items).toHaveLength(maxListWalkPages)
    expect(result.current.data?.truncated).toBe(true)
    expect(result.current.data?.metadata.continue).toBe(`page-${maxListWalkPages}`)
  })
})

describe('useTask', () => {
  it('returns a single task by id', async () => {
    const { result } = renderHook(() => useTask('my-task'), { wrapper: createWrapper() })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data).toMatchObject({
      metadata: { name: 'my-task', namespace: 'default' },
      status: { phase: 'Succeeded' },
    })
  })

  it('uses the supplied refetch interval for task detail polling', async () => {
    let calls = 0
    server.use(http.get('/api/v1/tasks/poll-task', () => {
      calls += 1
      return HttpResponse.json({
        metadata: { name: 'poll-task', namespace: 'default', uid: 'uid-poll' },
        spec: { type: 'container', image: 'alpine' },
        status: { phase: 'Running' },
      })
    }))

    renderHook(() => useTask('poll-task', 20), { wrapper: createWrapper() })

    await waitFor(() => expect(calls).toBeGreaterThan(1))
  })
})

describe('useTask on 404', () => {
  it('keeps polling a task that has never been seen, without burst retries', async () => {
    // A just-created Task can transiently 404 while the detail read's cache
    // catches up with the list; polling continues until the task appears.
    let calls = 0
    server.use(http.get('/api/v1/tasks/gone-task', () => {
      calls += 1
      return HttpResponse.json({ error: { code: 404, message: 'task not found' } }, { status: 404 })
    }))

    const { result } = renderHook(() => useTask('gone-task', 20), { wrapper: createWrapper() })
    await waitFor(() => expect(result.current.isError).toBe(true))
    const seen = calls
    await waitFor(() => expect(calls).toBeGreaterThan(seen))
  })

  it('stops polling once a previously loaded task 404s', async () => {
    let calls = 0
    server.use(http.get('/api/v1/tasks/deleted-task', () => {
      calls += 1
      if (calls === 1) {
        return HttpResponse.json({ metadata: { name: 'deleted-task' }, status: { phase: 'Running' } })
      }
      return HttpResponse.json({ error: { code: 404, message: 'task not found' } }, { status: 404 })
    }))

    const { result } = renderHook(() => useTask('deleted-task', 20), { wrapper: createWrapper() })
    // The task loads once, then every refetch 404s; polling must settle
    // instead of hammering the API forever.
    await waitFor(() => expect(result.current.data).toBeTruthy())
    await waitFor(() => expect(calls).toBeGreaterThanOrEqual(2))
    await new Promise((resolve) => setTimeout(resolve, 200))
    const seen = calls
    await new Promise((resolve) => setTimeout(resolve, 200))
    expect(calls).toBe(seen)
  })
})

describe('useTaskResult', () => {
  it('starts disabled and returns result on refetch', async () => {
    const { result } = renderHook(() => useTaskResult('my-task'), { wrapper: createWrapper() })
    // Should not fetch automatically
    expect(result.current.isFetching).toBe(false)
    expect(result.current.data).toBeUndefined()

    // Trigger manual refetch
    await act(async () => {
      await result.current.refetch()
    })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data).toEqual({ result: 'task output' })
  })
})



describe('useTaskEvents', () => {
  it('fetches pages until the initial latest sequence is covered', async () => {
    const requests: string[] = []
    server.use(
      http.get('/api/v1/tasks/:id/events', ({ request, params }) => {
        const url = new URL(request.url)
        requests.push(url.search)
        const after = url.searchParams.get('after')
        if (!after) {
          return HttpResponse.json({
            namespace: 'default',
            streamType: 'task',
            streamID: params.id,
            afterSeq: 0,
            latestSeq: 1001,
            events: [{
              id: 'default/task/my-task/1000',
              namespace: 'default',
              streamType: 'task',
              streamID: params.id,
              seq: 1000,
              type: 'ModelRequestCompleted',
              severity: 'info',
              inputTokens: 5,
              outputTokens: 7,
              createdAt: '2026-01-01T00:00:00Z',
            }],
          })
        }
        if (after === '1000') {
          return HttpResponse.json({
            namespace: 'default',
            streamType: 'task',
            streamID: params.id,
            afterSeq: 1000,
            latestSeq: 1001,
            events: [{
              id: 'default/task/my-task/1001',
              namespace: 'default',
              streamType: 'task',
              streamID: params.id,
              seq: 1001,
              type: 'ModelRequestCompleted',
              severity: 'info',
              inputTokens: 11,
              outputTokens: 13,
              createdAt: '2026-01-01T00:00:01Z',
            }],
          })
        }
        expect(after).toBe('1001')
        return HttpResponse.json({
          namespace: 'default',
          streamType: 'task',
          streamID: params.id,
          afterSeq: 1001,
          latestSeq: 1001,
          events: [],
        })
      }),
    )

    const { result } = renderHook(() => useTaskEvents('my-task'), { wrapper: createWrapper() })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))

    expect(requests).toEqual([
      '?namespace=default&limit=1000',
      '?namespace=default&limit=1000&after=1000',
    ])
    expect(result.current.data?.latestSeq).toBe(1001)
    expect(result.current.data?.events.map((event) => event.seq)).toEqual([1000, 1001])

    await act(async () => {
      await result.current.refetch()
    })
    expect(requests).toEqual([
      '?namespace=default&limit=1000',
      '?namespace=default&limit=1000&after=1000',
      '?namespace=default&limit=1000&after=1001',
    ])
    expect(result.current.data?.events.map((event) => event.seq)).toEqual([1000, 1001])
  })

  it('does not advance cursor past retained events when latest grows mid-fetch', async () => {
    const requests: string[] = []
    server.use(
      http.get('/api/v1/tasks/:id/events', ({ request, params }) => {
        const url = new URL(request.url)
        requests.push(url.search)
        const after = url.searchParams.get('after')
        if (!after) {
          return HttpResponse.json({
            namespace: 'default',
            streamType: 'task',
            streamID: params.id,
            afterSeq: 0,
            latestSeq: 2000,
            events: [{
              id: 'default/task/my-task/1999',
              namespace: 'default',
              streamType: 'task',
              streamID: params.id,
              seq: 1999,
              type: 'ModelRequestCompleted',
              severity: 'info',
              createdAt: '2026-01-01T00:00:00Z',
            }],
          })
        }
        if (after === '1999') {
          return HttpResponse.json({
            namespace: 'default',
            streamType: 'task',
            streamID: params.id,
            afterSeq: 1999,
            latestSeq: 2500,
            events: [{
              id: 'default/task/my-task/2000',
              namespace: 'default',
              streamType: 'task',
              streamID: params.id,
              seq: 2000,
              type: 'ModelRequestCompleted',
              severity: 'info',
              createdAt: '2026-01-01T00:00:01Z',
            }],
          })
        }
        expect(after).toBe('2000')
        return HttpResponse.json({
          namespace: 'default',
          streamType: 'task',
          streamID: params.id,
          afterSeq: 2000,
          latestSeq: 2500,
          events: [],
        })
      }),
    )

    const { result } = renderHook(() => useTaskEvents('my-task'), { wrapper: createWrapper() })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))

    expect(result.current.data?.latestSeq).toBe(2000)
    expect(result.current.data?.events.map((event) => event.seq)).toEqual([1999, 2000])

    await act(async () => {
      await result.current.refetch()
    })
    await waitFor(() => expect(result.current.data?.latestSeq).toBe(2500))
    expect(requests).toEqual([
      '?namespace=default&limit=1000',
      '?namespace=default&limit=1000&after=1999',
      '?namespace=default&limit=1000&after=2000',
    ])
  })
})


describe('useCreateTask', () => {
  it('creates a task via mutation', async () => {
    const { result } = renderHook(() => useCreateTask(), { wrapper: createWrapper() })
    act(() => {
      result.current.mutate({ type: 'container', image: 'alpine' })
    })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data).toMatchObject({ metadata: { name: 'new-task' } })
  })
})

describe('useDeleteTask', () => {
  it('deletes a task via mutation', async () => {
    const { result } = renderHook(() => useDeleteTask(), { wrapper: createWrapper() })
    act(() => {
      result.current.mutate('task-to-delete')
    })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
  })
})
