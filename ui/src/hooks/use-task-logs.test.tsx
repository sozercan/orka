import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { renderHook, waitFor, act } from '@/test/test-utils'

vi.mock('zustand/middleware', () => ({
  persist: (fn: unknown) => fn,
}))

import { useUIStore } from '@/stores/ui'
import { useAuthStore } from '@/stores/auth'
import { useTaskLogs } from './use-task-logs'

describe('useTaskLogs', () => {
  beforeEach(() => {
    useUIStore.setState({ namespace: 'default' })
    useAuthStore.setState({ token: 'test-token' })
    vi.restoreAllMocks()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('fetches logs with correct URL and auth header', async () => {
    const mockResponse = new Response(JSON.stringify({ logs: 'line1\nline2\nline3' }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
    Object.defineProperty(mockResponse, 'body', { value: null })
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(mockResponse)

    const { result } = renderHook(() => useTaskLogs('my-task'))

    await waitFor(() => {
      expect(result.current.logs.length).toBeGreaterThan(0)
    })

    expect(fetchSpy).toHaveBeenCalledWith(
      '/api/v1/tasks/my-task/logs?namespace=default',
      expect.objectContaining({
        headers: { Authorization: 'Bearer test-token' },
      })
    )
    expect(result.current.logs).toEqual(['line1', 'line2', 'line3'])
  })

  it('handles fetch errors', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(null, { status: 500, statusText: 'Internal Server Error' })
    )

    const { result } = renderHook(() => useTaskLogs('bad-task'))

    await waitFor(() => {
      expect(result.current.error).toBe('Failed to fetch logs: Internal Server Error')
    })
    expect(result.current.isStreaming).toBe(false)
  })

  it('clear function resets logs', async () => {
    const mockResponse = new Response(JSON.stringify({ logs: 'line1' }), { status: 200 })
    Object.defineProperty(mockResponse, 'body', { value: null })
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(mockResponse)

    const { result } = renderHook(() => useTaskLogs('my-task'))

    await waitFor(() => {
      expect(result.current.logs.length).toBe(1)
    })

    act(() => { result.current.clear() })

    expect(result.current.logs).toEqual([])
  })

  it('does not fetch when disabled', () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch')
    renderHook(() => useTaskLogs('my-task', false))
    expect(fetchSpy).not.toHaveBeenCalled()
  })

  it('uses tailLines param for running tasks', async () => {
    const mockResponse = new Response(JSON.stringify({ logs: 'live-line1\nlive-line2' }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
    Object.defineProperty(mockResponse, 'body', { value: null })
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(mockResponse)

    const { result } = renderHook(() => useTaskLogs('my-task', true, 'Running'))

    await waitFor(() => {
      expect(result.current.logs.length).toBeGreaterThan(0)
    })

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining('tailLines=200'),
      expect.any(Object)
    )
    expect(result.current.logs).toEqual(['live-line1', 'live-line2'])
    expect(result.current.isLive).toBe(true)
  })

  it('keeps finalizing tasks on the live polling path', async () => {
    const mockResponse = new Response(JSON.stringify({ logs: 'finalizing-line' }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
    Object.defineProperty(mockResponse, 'body', { value: null })
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(mockResponse)

    const { result } = renderHook(() => useTaskLogs('my-task', true, 'Finalizing'))

    await waitFor(() => {
      expect(result.current.logs).toEqual(['finalizing-line'])
    })

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining('tailLines=200'),
      expect.any(Object),
    )
    expect(result.current.isLive).toBe(true)
  })

  it('sets isLive false for completed tasks', async () => {
    const mockResponse = new Response(JSON.stringify({ logs: 'done-line' }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
    Object.defineProperty(mockResponse, 'body', { value: null })
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(mockResponse)

    const { result } = renderHook(() => useTaskLogs('my-task', true, 'Succeeded'))

    await waitFor(() => {
      expect(result.current.logs.length).toBeGreaterThan(0)
    })

    expect(result.current.isLive).toBe(false)
  })

  it('times out a stalled request so manual refetch can recover', async () => {
    vi.useFakeTimers()

    const signals: AbortSignal[] = []
    const fetchSpy = vi.spyOn(globalThis, 'fetch')
      .mockImplementationOnce((_input, init) => {
        signals.push(init?.signal as AbortSignal)
        return new Promise<Response>(() => {})
      })
      .mockImplementationOnce((_input, init) => {
        signals.push(init?.signal as AbortSignal)
        const response = new Response(JSON.stringify({ logs: 'recovered-line' }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
        Object.defineProperty(response, 'body', { value: null })
        return Promise.resolve(response)
      })

    const { result, unmount } = renderHook(() =>
      useTaskLogs('stalled-task', true, 'Succeeded'),
    )

    expect(fetchSpy).toHaveBeenCalledTimes(1)
    expect(result.current.isStreaming).toBe(true)
    expect(signals[0].aborted).toBe(false)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(30_000)
    })

    expect(signals[0].aborted).toBe(true)
    expect(result.current.isStreaming).toBe(false)
    expect(result.current.error).toBe('Log request timed out')

    await act(async () => {
      await result.current.refetch()
    })

    expect(fetchSpy).toHaveBeenCalledTimes(2)
    expect(signals[1].aborted).toBe(false)
    expect(result.current.logs).toEqual(['recovered-line'])
    expect(result.current.error).toBeNull()
    unmount()
  })

  it('keeps one slow running-task log request in flight until it completes', async () => {
    vi.useFakeTimers()

    const signals: AbortSignal[] = []
    const resolvers: Array<(response: Response) => void> = []
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation((_input, init) => {
      signals.push(init?.signal as AbortSignal)
      return new Promise<Response>((resolve) => {
        resolvers.push(resolve)
      })
    })

    const { unmount } = renderHook(() => useTaskLogs('slow-task', true, 'Running'))

    expect(fetchSpy).toHaveBeenCalledTimes(1)
    expect(signals[0].aborted).toBe(false)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(9000)
    })

    expect(fetchSpy).toHaveBeenCalledTimes(1)
    expect(signals[0].aborted).toBe(false)

    await act(async () => {
      resolvers[0]({
        json: async () => ({ logs: 'slow-line' }),
        ok: true,
      } as Response)
      await Promise.resolve()
      await Promise.resolve()
    })

    await act(async () => {
      await vi.advanceTimersByTimeAsync(3000)
    })

    expect(fetchSpy).toHaveBeenCalledTimes(2)
    unmount()
  })

  it('aborts in-flight log requests promptly on task change and unmount', () => {
    vi.useFakeTimers()

    const signals: AbortSignal[] = []
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation((_input, init) => {
      const signal = init?.signal as AbortSignal
      signals.push(signal)
      return new Promise<Response>((_resolve, reject) => {
        signal.addEventListener('abort', () => {
          reject(new DOMException('Aborted', 'AbortError'))
        }, { once: true })
      })
    })

    const { rerender, unmount } = renderHook(
      ({ taskId }) => useTaskLogs(taskId, true, 'Running'),
      { initialProps: { taskId: 'task-a' } },
    )

    expect(fetchSpy).toHaveBeenCalledTimes(1)
    expect(signals[0].aborted).toBe(false)

    rerender({ taskId: 'task-b' })

    expect(signals[0].aborted).toBe(true)
    expect(fetchSpy).toHaveBeenCalledTimes(2)
    expect(signals[1].aborted).toBe(false)

    unmount()

    expect(signals[1].aborted).toBe(true)
  })

  it('clears task-scoped logs and errors before fetching a replacement task', async () => {
    let replacementSignal: AbortSignal | undefined
    const fetchSpy = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response(JSON.stringify({ logs: 'task-a-line' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }))
      .mockResolvedValueOnce(new Response(null, {
        status: 500,
        statusText: 'Internal Server Error',
      }))
      .mockImplementationOnce((_input, init) => {
        replacementSignal = init?.signal as AbortSignal
        return new Promise<Response>((_resolve, reject) => {
          replacementSignal?.addEventListener('abort', () => {
            reject(new DOMException('Aborted', 'AbortError'))
          }, { once: true })
        })
      })

    const { result, rerender, unmount } = renderHook(
      ({ taskId }) => useTaskLogs(taskId, true, 'Running'),
      { initialProps: { taskId: 'task-a' } },
    )

    await waitFor(() => {
      expect(result.current.logs).toEqual(['task-a-line'])
      expect(result.current.isStreaming).toBe(false)
    })

    await act(async () => {
      await result.current.refetch()
    })

    expect(result.current.logs).toEqual(['task-a-line'])
    expect(result.current.error).toBe('Failed to fetch logs: Internal Server Error')
    expect(result.current.isLive).toBe(true)

    rerender({ taskId: 'task-b' })

    expect(fetchSpy).toHaveBeenCalledTimes(3)
    expect(replacementSignal?.aborted).toBe(false)
    expect(result.current.logs).toEqual([])
    expect(result.current.error).toBeNull()
    expect(result.current.isLive).toBe(false)
    expect(result.current.isStreaming).toBe(true)
    unmount()
  })

  it('aborts and restarts an in-flight log request when the namespace changes', () => {
    vi.useFakeTimers()

    const signals: AbortSignal[] = []
    const urls: string[] = []
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation((input, init) => {
      const signal = init?.signal as AbortSignal
      signals.push(signal)
      urls.push(String(input))
      return new Promise<Response>((_resolve, reject) => {
        signal.addEventListener('abort', () => {
          reject(new DOMException('Aborted', 'AbortError'))
        }, { once: true })
      })
    })

    const { unmount } = renderHook(() => useTaskLogs('task-a', true, 'Running'))

    expect(fetchSpy).toHaveBeenCalledTimes(1)
    expect(urls[0]).toContain('namespace=default')

    act(() => {
      useUIStore.setState({ namespace: 'team-blue' })
    })

    expect(signals[0].aborted).toBe(true)
    expect(fetchSpy).toHaveBeenCalledTimes(2)
    expect(urls[1]).toContain('namespace=team-blue')
    expect(signals[1].aborted).toBe(false)
    unmount()
  })

  it('clears logs before fetching the same task from a replacement namespace', async () => {
    let replacementSignal: AbortSignal | undefined
    const urls: string[] = []
    const fetchSpy = vi.spyOn(globalThis, 'fetch')
      .mockImplementationOnce((input) => {
        urls.push(String(input))
        return Promise.resolve(new Response(JSON.stringify({ logs: 'default-line' }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }))
      })
      .mockImplementationOnce((input, init) => {
        urls.push(String(input))
        replacementSignal = init?.signal as AbortSignal
        return new Promise<Response>((_resolve, reject) => {
          replacementSignal?.addEventListener('abort', () => {
            reject(new DOMException('Aborted', 'AbortError'))
          }, { once: true })
        })
      })

    const { result, unmount } = renderHook(() => useTaskLogs('task-a', true, 'Running'))

    await waitFor(() => {
      expect(result.current.logs).toEqual(['default-line'])
      expect(result.current.isStreaming).toBe(false)
    })

    act(() => {
      useUIStore.setState({ namespace: 'team-blue' })
    })

    expect(fetchSpy).toHaveBeenCalledTimes(2)
    expect(urls[0]).toContain('namespace=default')
    expect(urls[1]).toContain('namespace=team-blue')
    expect(replacementSignal?.aborted).toBe(false)
    expect(result.current.logs).toEqual([])
    expect(result.current.error).toBeNull()
    expect(result.current.isLive).toBe(false)
    expect(result.current.isStreaming).toBe(true)
    unmount()
  })

  it('clears streaming state when disabling an in-flight log request', () => {
    vi.useFakeTimers()

    let signal: AbortSignal | undefined
    vi.spyOn(globalThis, 'fetch').mockImplementation((_input, init) => {
      signal = init?.signal as AbortSignal
      return new Promise<Response>((_resolve, reject) => {
        signal?.addEventListener('abort', () => {
          reject(new DOMException('Aborted', 'AbortError'))
        }, { once: true })
      })
    })

    const { result, rerender, unmount } = renderHook(
      ({ enabled }) => useTaskLogs('task-a', enabled, 'Running'),
      { initialProps: { enabled: true } },
    )

    expect(result.current.isStreaming).toBe(true)

    rerender({ enabled: false })

    expect(signal?.aborted).toBe(true)
    expect(result.current.isStreaming).toBe(false)
    unmount()
  })
})
