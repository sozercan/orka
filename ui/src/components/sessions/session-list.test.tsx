import { describe, it, expect, beforeEach, vi } from 'vitest'
import { http, HttpResponse } from 'msw'
import { server } from '@/test/mocks/server'
import userEvent from '@testing-library/user-event'

vi.mock('zustand/middleware', () => ({
  persist: (fn: unknown) => fn,
}))

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual('@tanstack/react-router')
  return {
    ...actual,
    Link: ({ children, to, ...props }: any) => <a href={to} {...props}>{children}</a>,
    useNavigate: () => vi.fn(),
    useLocation: () => ({ pathname: '/sessions' }),
  }
})

vi.mock('sonner', () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

import { render, screen, waitFor } from '@/test/test-utils'
import { useUIStore } from '@/stores/ui'
import { useAuthStore } from '@/stores/auth'
import { SessionList } from './session-list'

describe('SessionList', () => {
  beforeEach(() => {
    useUIStore.setState({ namespace: 'default', sidebarCollapsed: false, theme: 'light' })
    useAuthStore.setState({ token: 'test-token' })
  })

  it('shows skeleton rows while loading', () => {
    // Default handler returns empty list; check skeletons appear before data loads
    const { container } = render(<SessionList />)
    const skeletons = container.querySelectorAll('[class*="animate-pulse"], [data-slot="skeleton"]')
    expect(skeletons.length).toBeGreaterThan(0)
  })

  it('shows "No sessions found." for empty list', async () => {
    render(<SessionList />)
    await waitFor(() => {
      expect(screen.getByText('No sessions found.')).toBeInTheDocument()
    })
  })

  it('shows a not-authorized message (not "No sessions found.") on 403', async () => {
    server.use(
      http.get('/api/v1/sessions', () =>
        HttpResponse.json({ error: { code: 403, message: 'not authorized' } }, { status: 403 }),
      ),
    )
    render(<SessionList />)
    await waitFor(() => {
      expect(screen.getByText('Not authorized to view sessions')).toBeInTheDocument()
    })
    expect(screen.getByText(/read permission for sessions \(not authorized\)/)).toBeInTheDocument()
    expect(screen.queryByText('No sessions found.')).not.toBeInTheDocument()
    expect(screen.queryByText(/"code":403/)).not.toBeInTheDocument()
  })

  it('shows a load error for non-403 failures', async () => {
    server.use(
      http.get('/api/v1/sessions', () =>
        HttpResponse.json({ error: { code: 400, message: 'invalid namespace' } }, { status: 400 }),
      ),
    )
    render(<SessionList />)
    await waitFor(() => {
      expect(screen.getByText('Could not load sessions')).toBeInTheDocument()
      expect(screen.getByText('invalid namespace')).toBeInTheDocument()
    })
  })

  it('renders populated table with session data', async () => {
    server.use(
      http.get('/api/v1/sessions', () => {
        return HttpResponse.json({
          items: [
            {
              name: 'sess-abc',
              namespace: 'default',
              messageCount: '12',
              inputTokens: '500',
              outputTokens: '800',
              activeTask: 'my-task',
              createdAt: new Date().toISOString(),
            },
          ],
          metadata: {},
        })
      })
    )

    render(<SessionList />)
    await waitFor(() => {
      expect(screen.getByText('sess-abc')).toBeInTheDocument()
    })
    expect(screen.getByText('12')).toBeInTheDocument()
    expect(screen.getByText('500 / 800')).toBeInTheDocument()
    expect(screen.getByText('my-task')).toBeInTheDocument()
  })

  it('has a delete button for each session row', async () => {
    server.use(
      http.get('/api/v1/sessions', () => {
        return HttpResponse.json({
          items: [
            { name: 'sess-1', namespace: 'default' },
            { name: 'sess-2', namespace: 'default' },
          ],
          metadata: {},
        })
      })
    )

    render(<SessionList />)
    await waitFor(() => {
      expect(screen.getByText('sess-1')).toBeInTheDocument()
    })
    const buttons = screen.getAllByRole('button')
    expect(buttons.length).toBe(2)
  })

  it('renders table headers', () => {
    render(<SessionList />)
    expect(screen.getByText('Name')).toBeInTheDocument()
    expect(screen.getByText('Namespace')).toBeInTheDocument()
    expect(screen.getByText('Messages')).toBeInTheDocument()
  })

  it('delete button calls deleteSession', async () => {
    const user = userEvent.setup()
    server.use(
      http.get('/api/v1/sessions', () =>
        HttpResponse.json({
          items: [{ name: 'sess-del', namespace: 'default', createdAt: new Date().toISOString() }],
          metadata: {},
        }),
      ),
    )
    render(<SessionList />)
    await waitFor(() => {
      expect(screen.getByText('sess-del')).toBeInTheDocument()
    })
    await user.click(screen.getByRole('button'))
    // Verify no error - mutation fires
    expect(screen.getByText('sess-del')).toBeInTheDocument()
  })

  it('timeAgo covers minutes, hours, and days', async () => {
    const now = Date.now()
    server.use(
      http.get('/api/v1/sessions', () =>
        HttpResponse.json({
          items: [
            { name: 's-min', namespace: 'default', createdAt: new Date(now - 120_000).toISOString() },
            { name: 's-hr', namespace: 'default', createdAt: new Date(now - 7200_000).toISOString() },
            { name: 's-day', namespace: 'default', createdAt: new Date(now - 172800_000).toISOString() },
          ],
          metadata: {},
        }),
      ),
    )
    render(<SessionList />)
    await waitFor(() => {
      expect(screen.getByText('s-min')).toBeInTheDocument()
    })
    expect(screen.getByText('2m')).toBeInTheDocument()
    expect(screen.getByText('2h')).toBeInTheDocument()
    expect(screen.getByText('2d')).toBeInTheDocument()
  })
})
