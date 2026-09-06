import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, waitFor, within } from '@/test/test-utils'
import { http, HttpResponse } from 'msw'
import { server } from '@/test/mocks/server'

vi.mock('zustand/middleware', () => ({
  persist: (fn: unknown) => fn,
}))

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual('@tanstack/react-router')
  return {
    ...actual,
    Link: ({ children, to, ...props }: any) => <a href={to} {...props}>{children}</a>,
    useNavigate: () => vi.fn(),
    useLocation: () => ({ pathname: '/' }),
    Outlet: () => <div data-testid="outlet" />,
  }
})

import { useUIStore } from '@/stores/ui'
import { useAuthStore } from '@/stores/auth'
import { Overview } from './overview'

describe('Overview', () => {
  beforeEach(() => {
    useUIStore.setState({ sidebarCollapsed: false, theme: 'light', namespace: 'default' })
    useAuthStore.setState({ token: 'test-token' })
  })

  it('renders Dashboard heading', () => {
    render(<Overview />)
    expect(screen.getByText('Dashboard')).toBeInTheDocument()
  })

  it('renders without crashing', () => {
    render(<Overview />)
    expect(screen.getByText('Overview of your Orka workspace')).toBeInTheDocument()
  })

  it('passes a 403 on the sessions list through to the Sessions stat card', async () => {
    server.use(
      http.get('/api/v1/sessions', () =>
        HttpResponse.json({ error: { code: 403, message: 'not authorized' } }, { status: 403 }),
      ),
    )
    render(<Overview />)
    await waitFor(() => {
      expect(screen.getByText('Not authorized')).toBeInTheDocument()
    })
    expect(screen.getByText(/read permission \(not authorized\)/)).toBeInTheDocument()
  })

  it('renders Not authorized for tasks, agents, and tools that 403 instead of zero counts', async () => {
    const forbidden = () => HttpResponse.json({ error: { code: 403, message: 'scope missing' } }, { status: 403 })
    server.use(http.get('/api/v1/tasks', forbidden), http.get('/api/v1/agents', forbidden), http.get('/api/v1/tools', forbidden))
    render(<Overview />)
    await waitFor(() => {
      expect(screen.getAllByText('Not authorized').length).toBeGreaterThanOrEqual(6)
    })
    // Both task surfaces — phase distribution and recent tasks — show it.
    expect(screen.getAllByText(/Not authorized to list tasks \(scope missing\)/)).toHaveLength(2)
    expect(screen.getAllByText(/lacks/).map((el) => el.textContent)).toEqual(
      expect.arrayContaining([expect.stringContaining('agents'), expect.stringContaining('tools'), expect.stringContaining('tasks')]),
    )
  })

  it('includes Scheduled and Cancelled tasks in the phase distribution', async () => {
    const mk = (name: string, phase: string) => ({
      metadata: { name, namespace: 'default', uid: name, creationTimestamp: new Date().toISOString() },
      spec: { type: 'container' },
      status: { phase },
    })
    server.use(
      http.get('/api/v1/tasks', () =>
        HttpResponse.json({
          items: [mk('a', 'Running'), mk('b', 'Scheduled'), mk('c', 'Cancelled')],
          metadata: {},
        }),
      ),
    )
    render(<Overview />)
    const heading = await screen.findByText('Phase Distribution')
    // Scope assertions to the distribution card (the phase labels also appear
    // as StatusDots in the Recent Tasks list).
    const card = heading.closest('[data-slot="card"]') as HTMLElement
    expect(card).not.toBeNull()
    await waitFor(() => {
      expect(within(card).getByText('Scheduled')).toBeInTheDocument()
    })
    expect(within(card).getByText('Cancelled')).toBeInTheDocument()
    expect(within(card).getByText('Running')).toBeInTheDocument()
  })

  it('follows list pagination before calculating dashboard totals', async () => {
    const paged = (resource: string, first: unknown[], second: unknown[]) =>
      http.get(`/api/v1/${resource}`, ({ request }) => {
        const cursor = new URL(request.url).searchParams.get('continue')
        return HttpResponse.json(cursor ? { items: second, metadata: {} } : { items: first, metadata: { continue: `${resource}-next` } })
      })
    const task = (name: string) => ({
      metadata: { name, namespace: 'default', uid: name, creationTimestamp: new Date().toISOString() },
      spec: { type: 'container' },
      status: { phase: 'Succeeded' },
    })
    server.use(
      paged('tasks', [task('one')], [task('two')]),
      paged('sessions', [{ id: 'one' }], [{ id: 'two' }, { id: 'three' }]),
      paged('agents', [{ metadata: { name: 'one' }, spec: {} }], [{ metadata: { name: 'two' }, spec: {} }, { metadata: { name: 'three' }, spec: {} }, { metadata: { name: 'four' }, spec: {} }]),
      paged('tools', [{ metadata: { name: 'one' } }], [{ metadata: { name: 'two' } }, { metadata: { name: 'three' } }, { metadata: { name: 'four' } }, { metadata: { name: 'five' } }]),
    )

    render(<Overview />)
    const cardValue = async (title: string, value: string) => {
      const heading = await screen.findByText(title)
      const card = heading.closest('[data-slot="card"]') as HTMLElement
      await waitFor(() => expect(within(card).getByText(value)).toBeInTheDocument())
    }
    await cardValue('Total Tasks', '2')
    await cardValue('Sessions', '3')
    await cardValue('Agents', '4')
    await cardValue('Tools', '5')
  })
})
