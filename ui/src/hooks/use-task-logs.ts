import { useState, useEffect, useRef, useCallback } from 'react'
import { useAuthStore } from '@/stores/auth'
import { API_BASE_URL } from '@/lib/constants'
import { useUIStore } from '@/stores/ui'
import type { TaskPhase } from '@/schemas/task'

const taskLogRequestIdleTimeoutMs = 30_000

interface InFlightLogRequest {
  controller: AbortController
  timeoutId: ReturnType<typeof setTimeout> | null
}

function isRunningPhase(phase?: TaskPhase): boolean {
  return phase === 'Running' || phase === 'Finalizing' || phase === 'Pending'
}

export function useTaskLogs(taskId: string, enabled = true, taskPhase?: TaskPhase) {
  const namespace = useUIStore((s) => s.namespace)
  const taskScope = `${namespace}\u0000${taskId}`
  const [stateScope, setStateScope] = useState(taskScope)
  const [logs, setLogs] = useState<string[]>([])
  const [isStreaming, setIsStreaming] = useState(false)
  const [isLive, setIsLive] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const inFlightRef = useRef<InFlightLogRequest | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const fetchLogs = useCallback(async () => {
    if (!enabled || !taskId) {
      setIsStreaming(false)
      return
    }
    if (inFlightRef.current) return

    const token = useAuthStore.getState().token
    const running = isRunningPhase(taskPhase)
    const controller = new AbortController()
    const request: InFlightLogRequest = { controller, timeoutId: null }
    const armIdleTimeout = () => {
      if (request.timeoutId !== null) clearTimeout(request.timeoutId)
      request.timeoutId = setTimeout(() => {
        request.timeoutId = null
        if (inFlightRef.current !== request) return

        inFlightRef.current = null
        controller.abort()
        setError('Log request timed out')
        setIsStreaming(false)
      }, taskLogRequestIdleTimeoutMs)
    }

    inFlightRef.current = request
    armIdleTimeout()

    try {
      setIsStreaming(true)
      setError(null)

      const params = new URLSearchParams({ namespace })
      if (running) {
        params.set('tailLines', '200')
      }

      const response = await fetch(
        `${API_BASE_URL}/tasks/${taskId}/logs?${params.toString()}`,
        {
          headers: token ? { Authorization: `Bearer ${token}` } : {},
          signal: controller.signal,
        }
      )

      if (controller.signal.aborted) return
      armIdleTimeout()

      if (!response.ok) {
        throw new Error(`Failed to fetch logs: ${response.statusText}`)
      }

      if (running) {
        // For running tasks, parse JSON response and replace logs
        const data = await response.json()
        if (controller.signal.aborted) return
        const text: string = data.logs ?? ''
        setLogs(text.split('\n').filter(Boolean))
        setIsLive(true)
      } else {
        // For completed tasks, use streaming reader or text fallback
        const reader = response.body?.getReader()
        if (!reader) {
          const text = await response.text()
          try {
            const data = JSON.parse(text)
            setLogs((data.logs ?? '').split('\n').filter(Boolean))
          } catch {
            setLogs(text.split('\n').filter(Boolean))
          }
        } else {
          const decoder = new TextDecoder()
          let buffer = ''
          const allLines: string[] = []

          while (true) {
            const { done, value } = await reader.read()
            if (done) break

            armIdleTimeout()
            buffer += decoder.decode(value, { stream: true })
            const lines = buffer.split('\n')
            buffer = lines.pop() || ''
            allLines.push(...lines.filter(Boolean))
          }

          if (buffer) allLines.push(buffer)

          // Try parsing as JSON in case of completed task response
          if (allLines.length === 1) {
            try {
              const data = JSON.parse(allLines[0])
              if (data.logs) {
                setLogs(data.logs.split('\n').filter(Boolean))
              } else {
                setLogs(allLines)
              }
            } catch {
              setLogs(allLines)
            }
          } else {
            setLogs(allLines)
          }
        }
        if (controller.signal.aborted) return
        setIsLive(false)
      }
    } catch (err) {
      if (!controller.signal.aborted && err instanceof Error && err.name !== 'AbortError') {
        setError(err.message)
      }
    } finally {
      if (request.timeoutId !== null) {
        clearTimeout(request.timeoutId)
        request.timeoutId = null
      }
      if (inFlightRef.current === request) {
        inFlightRef.current = null
        setIsStreaming(false)
      }
    }
  }, [taskId, enabled, taskPhase, namespace])

  useEffect(() => {
    setStateScope(taskScope)
    setLogs([])
    setIsStreaming(false)
    setIsLive(false)
    setError(null)
  }, [taskScope])

  useEffect(() => {
    fetchLogs()

    // Set up polling for running tasks
    if (pollRef.current) {
      clearInterval(pollRef.current)
      pollRef.current = null
    }

    if (enabled && isRunningPhase(taskPhase)) {
      pollRef.current = setInterval(fetchLogs, 3000)
    }

    return () => {
      const request = inFlightRef.current
      if (request) {
        inFlightRef.current = null
        if (request.timeoutId !== null) {
          clearTimeout(request.timeoutId)
          request.timeoutId = null
        }
        request.controller.abort()
      }
      if (pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
      }
    }
  }, [fetchLogs, enabled, taskPhase])

  // When task completes, stop polling and do a final fetch
  useEffect(() => {
    if (!isRunningPhase(taskPhase) && isLive) {
      setIsLive(false)
      if (pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
      }
      fetchLogs()
    }
  }, [taskPhase, isLive, fetchLogs])

  const clear = useCallback(() => setLogs([]), [])

  const isCurrentScope = stateScope === taskScope

  return {
    logs: isCurrentScope ? logs : [],
    isStreaming: isCurrentScope ? isStreaming : false,
    isLive: isCurrentScope ? isLive : false,
    error: isCurrentScope ? error : null,
    refetch: fetchLogs,
    clear,
  }
}
