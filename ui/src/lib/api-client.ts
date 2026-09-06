import { useAuthStore } from '@/stores/auth'
import { API_BASE_URL } from './constants'

interface RequestOptions extends Omit<RequestInit, 'headers'> {
  headers?: Record<string, string>
  params?: Record<string, string>
}

class ApiError extends Error {
  /** Raw response body, kept for diagnostics; `message` is the readable form. */
  readonly body: string

  constructor(public status: number, body: string) {
    super(apiErrorMessage(body))
    this.name = 'ApiError'
    this.body = body
  }
}

/**
 * Extract a human-readable message from an API error body. The server wraps
 * errors as `{"error":{"code":403,"message":"not authorized"}}` (or, from some
 * paths, `{"error":"..."}` / `{"message":"..."}`); anything else is returned
 * verbatim so callers never render raw JSON.
 */
export function apiErrorMessage(body: string): string {
  const text = body.trim()
  if (!text.startsWith('{')) return text
  try {
    const parsed = JSON.parse(text) as unknown
    if (parsed && typeof parsed === 'object') {
      const record = parsed as Record<string, unknown>
      const error = record.error
      if (typeof error === 'string' && error) return error
      if (error && typeof error === 'object') {
        const message = (error as Record<string, unknown>).message
        if (typeof message === 'string' && message) return message
      }
      if (typeof record.message === 'string' && record.message) return record.message
    }
  } catch {
    // Not JSON; fall through to the raw text.
  }
  return text
}

function isApiErrorStatus(error: unknown, status: number): error is ApiError {
  return error instanceof ApiError && error.status === status
}

export const isForbiddenError = (error: unknown) => isApiErrorStatus(error, 403)
export const isNotFoundError = (error: unknown) => isApiErrorStatus(error, 404)

async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { params, ...fetchOptions } = options
  const token = useAuthStore.getState().token

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...options.headers,
  }

  if (token) {
    headers['Authorization'] = `Bearer ${token}`
  }

  let url = `${API_BASE_URL}${path}`
  if (params) {
    const searchParams = new URLSearchParams()
    for (const [key, value] of Object.entries(params)) {
      if (value) searchParams.set(key, value)
    }
    const qs = searchParams.toString()
    if (qs) url += `?${qs}`
  }

  const response = await fetch(url, { ...fetchOptions, headers })

  if (!response.ok) {
    if (response.status === 401) {
      useAuthStore.getState().clearToken()
    }
    const text = await response.text().catch(() => 'Unknown error')
    throw new ApiError(response.status, text)
  }

  if (response.status === 204) {
    return undefined as T
  }

  const text = await response.text()
  if (!text) {
    return undefined as T
  }

  const contentType = response.headers.get('Content-Type')?.split(';', 1)[0].trim().toLowerCase()
  if (contentType === 'application/json' || contentType?.endsWith('+json')) {
    return JSON.parse(text) as T
  }

  return text as T
}

export const api = {
  get: <T>(path: string, params?: Record<string, string>) =>
    request<T>(path, { method: 'GET', params }),

  post: <T>(path: string, body?: unknown, params?: Record<string, string>, headers?: Record<string, string>) =>
    request<T>(path, { method: 'POST', body: body ? JSON.stringify(body) : undefined, params, headers }),

  put: <T>(path: string, body?: unknown, params?: Record<string, string>) =>
    request<T>(path, { method: 'PUT', body: body ? JSON.stringify(body) : undefined, params }),

  delete: <T>(path: string, params?: Record<string, string>) =>
    request<T>(path, { method: 'DELETE', params }),
}

export { ApiError }
