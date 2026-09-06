import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@/test/test-utils'
import type { Task } from '@/schemas/task'

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

import { RecentTasks } from './recent-tasks'

function makeTask(name: string, type: string, phase: string, timestamp?: string): Task {
  return {
    metadata: {
      name,
      namespace: 'default',
      uid: `uid-${name}`,
      creationTimestamp: timestamp,
    },
    spec: { type: type as any },
    status: { phase: phase as any },
  }
}

describe('RecentTasks', () => {
  it('loading state shows skeletons', () => {
    const { container } = render(<RecentTasks isLoading />)
    const skeletons = container.querySelectorAll('[class*="animate-pulse"], [data-slot="skeleton"]')
    expect(skeletons.length).toBeGreaterThan(0)
  })

  it('empty state shows "No tasks yet"', () => {
    render(<RecentTasks tasks={[]} />)
    expect(screen.getByText('No tasks yet')).toBeInTheDocument()
  })

  it('renders task items with name, type, namespace, phase badge', () => {
    const tasks = [
      makeTask('my-task', 'container', 'Running', new Date().toISOString()),
    ]
    render(<RecentTasks tasks={tasks} />)
    expect(screen.getByText('my-task')).toBeInTheDocument()
    expect(screen.getByText('Running')).toBeInTheDocument()
    // type and namespace shown in description line
    expect(screen.getByText(/container/)).toBeInTheDocument()
    expect(screen.getByText(/default/)).toBeInTheDocument()
  })

  it('timeAgo shows seconds ago for recent timestamps', () => {
    const now = new Date().toISOString()
    const tasks = [makeTask('recent-task', 'ai', 'Succeeded', now)]
    render(<RecentTasks tasks={tasks} />)
    expect(screen.getByText(/\ds ago/)).toBeInTheDocument()
  })

  it('timeAgo shows minutes ago', () => {
    const fiveMinAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString()
    const tasks = [makeTask('min-task', 'ai', 'Succeeded', fiveMinAgo)]
    render(<RecentTasks tasks={tasks} />)
    expect(screen.getByText(/5m ago/)).toBeInTheDocument()
  })

  it('timeAgo shows hours ago', () => {
    const twoHoursAgo = new Date(Date.now() - 2 * 3600 * 1000).toISOString()
    const tasks = [makeTask('hr-task', 'container', 'Failed', twoHoursAgo)]
    render(<RecentTasks tasks={tasks} />)
    expect(screen.getByText(/2h ago/)).toBeInTheDocument()
  })

  it('timeAgo shows days ago', () => {
    const threeDaysAgo = new Date(Date.now() - 3 * 86400 * 1000).toISOString()
    const tasks = [makeTask('day-task', 'container', 'Succeeded', threeDaysAgo)]
    render(<RecentTasks tasks={tasks} />)
    expect(screen.getByText(/3d ago/)).toBeInTheDocument()
  })

  it('limits to 10 tasks max', () => {
    const tasks = Array.from({ length: 15 }, (_, i) =>
      makeTask(`task-${i}`, 'container', 'Succeeded', new Date().toISOString())
    )
    render(<RecentTasks tasks={tasks} />)
    // Only 10 task links should be rendered
    const links = screen.getAllByRole('link')
    expect(links.length).toBe(10)
  })

  it('sorts a complete list by creation time before choosing recent tasks', () => {
    const tasks = [
      makeTask('older', 'container', 'Succeeded', '2026-01-01T00:00:00Z'),
      makeTask('newer', 'container', 'Succeeded', '2026-01-03T00:00:00Z'),
      makeTask('middle', 'container', 'Succeeded', '2026-01-02T00:00:00Z'),
    ]
    render(<RecentTasks tasks={tasks} />)
    expect(screen.getAllByRole('link').map((link) => link.textContent)).toEqual([
      expect.stringContaining('newer'),
      expect.stringContaining('middle'),
      expect.stringContaining('older'),
    ])
  })

  it('describes a truncated list as a sample rather than recent tasks', () => {
    render(<RecentTasks tasks={[makeTask('loaded', 'container', 'Running')]} isTruncated />)
    expect(screen.getByText('Task sample')).toBeInTheDocument()
    expect(screen.queryByText('Recent Tasks')).not.toBeInTheDocument()
    expect(screen.getByText(/Newer tasks may exist outside it/)).toBeInTheDocument()
  })
})
