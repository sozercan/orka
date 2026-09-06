import { describe, it, expect } from 'vitest'
import { render, screen } from '@/test/test-utils'
import { StatsCards } from './stats-cards'
import type { Task } from '@/schemas/task'

function makeTask(name: string, phase: string): Task {
  return {
    metadata: { name, namespace: 'default', uid: `uid-${name}` },
    spec: { type: 'container' },
    status: { phase: phase as any },
  }
}

describe('StatsCards', () => {
  it('loading state shows skeletons', () => {
    const { container } = render(<StatsCards isLoading />)
    // 4 skeleton cards rendered in loading state
    const skeletons = container.querySelectorAll('[class*="animate-pulse"], [data-slot="skeleton"]')
    expect(skeletons.length).toBeGreaterThan(0)
  })

  it('renders correct counts for each task phase', () => {
    const tasks = [
      makeTask('t1', 'Running'),
      makeTask('t2', 'Running'),
      makeTask('t3', 'Succeeded'),
      makeTask('t4', 'Failed'),
    ]
    render(<StatsCards tasks={tasks} sessionCount={5} agentCount={3} toolCount={7} />)
    expect(screen.getByText('Total Tasks')).toBeInTheDocument()
    expect(screen.getByText('4')).toBeInTheDocument() // total
    expect(screen.getByText('Running')).toBeInTheDocument()
    expect(screen.getByText('Succeeded')).toBeInTheDocument()
    expect(screen.getByText('Failed')).toBeInTheDocument()
    expect(screen.getByText('5')).toBeInTheDocument() // sessions
    expect(screen.getByText('3')).toBeInTheDocument() // agents
    expect(screen.getByText('7')).toBeInTheDocument() // tools
  })

  it('renders sessions, agents, tools counts', () => {
    render(<StatsCards tasks={[]} sessionCount={10} agentCount={7} toolCount={4} />)
    expect(screen.getByText('Sessions')).toBeInTheDocument()
    expect(screen.getByText('10')).toBeInTheDocument()
    expect(screen.getByText('Agents')).toBeInTheDocument()
    expect(screen.getByText('7')).toBeInTheDocument()
    expect(screen.getByText('Tools')).toBeInTheDocument()
    expect(screen.getByText('4')).toBeInTheDocument()
  })

  it('shows a not-authorized message on the Sessions card instead of 0', () => {
    render(<StatsCards tasks={[]} sessionsForbiddenMessage="not authorized" agentCount={1} toolCount={2} />)
    expect(screen.getByText('Not authorized')).toBeInTheDocument()
    expect(screen.getByText(/read permission \(not authorized\)/)).toBeInTheDocument()
    // total, running, succeeded, failed — the Sessions card no longer renders a 0
    expect(screen.getAllByText('0').length).toBe(4)
  })

  it('zero counts when no data', () => {
    render(<StatsCards />)
    expect(screen.getByText('Total Tasks')).toBeInTheDocument()
    // All counts should show 0
    const zeros = screen.getAllByText('0')
    expect(zeros.length).toBe(7) // total, running, succeeded, failed, sessions, agents, tools
  })

  it('shows a success-rate indicator derived from finished tasks', () => {
    const tasks = [
      makeTask('t1', 'Succeeded'),
      makeTask('t2', 'Succeeded'),
      makeTask('t3', 'Succeeded'),
      makeTask('t4', 'Failed'),
    ]
    // 3 succeeded / 4 finished = 75%
    render(<StatsCards tasks={tasks} />)
    expect(screen.getByText('75% success rate')).toBeInTheDocument()
  })

  it('omits the success rate when nothing has finished', () => {
    render(<StatsCards tasks={[makeTask('t1', 'Running'), makeTask('t2', 'Pending')]} />)
    expect(screen.queryByText(/success rate/)).not.toBeInTheDocument()
  })

  it('labels counts and rates as partial when more task pages exist', () => {
    render(<StatsCards tasks={[makeTask('t1', 'Succeeded')]} tasksTruncated />)
    expect(screen.getByText('Tasks loaded')).toBeInTheDocument()
    expect(screen.queryByText('Total Tasks')).not.toBeInTheDocument()
    expect(screen.getByText('100% of loaded finished tasks')).toBeInTheDocument()
  })

  it('labels the session count as loaded when more session pages exist', () => {
    render(<StatsCards tasks={[]} sessionCount={20} sessionsTruncated />)
    expect(screen.getByText('Sessions loaded')).toBeInTheDocument()
    expect(screen.queryByText('Sessions')).not.toBeInTheDocument()
  })

  it('renders trend sparklines when there is more than one task', () => {
    const tasks = [
      { metadata: { name: 'a', namespace: 'default', uid: 'a', creationTimestamp: '2026-01-01T00:00:00Z' }, spec: { type: 'container' }, status: { phase: 'Succeeded' } },
      { metadata: { name: 'b', namespace: 'default', uid: 'b', creationTimestamp: '2026-01-02T00:00:00Z' }, spec: { type: 'container' }, status: { phase: 'Running' } },
    ] as Task[]
    render(<StatsCards tasks={tasks} />)
    expect(screen.getAllByRole('img', { name: /trend/i }).length).toBeGreaterThan(0)
  })

  it('plots a per-status trend, not the aggregate, on each status card', () => {
    // 2 succeeded, 0 failed → the Failed sparkline must be flat (all zero),
    // while Succeeded trends upward. Guards the regression where every card
    // reused the total-task series.
    const tasks = [
      { metadata: { name: 'a', namespace: 'default', uid: 'a', creationTimestamp: '2026-01-01T00:00:00Z' }, spec: { type: 'container' }, status: { phase: 'Succeeded' } },
      { metadata: { name: 'b', namespace: 'default', uid: 'b', creationTimestamp: '2026-01-02T00:00:00Z' }, spec: { type: 'container' }, status: { phase: 'Succeeded' } },
    ] as Task[]
    render(<StatsCards tasks={tasks} />)
    const failed = screen.getByRole('img', { name: /failed trend/i })
    const succeeded = screen.getByRole('img', { name: /succeeded trend/i })
    // Failed has no tasks → every point sits on the baseline (single y value).
    const failedYs = new Set(
      (failed.querySelector('polyline')?.getAttribute('points') ?? '')
        .trim()
        .split(' ')
        .map((p) => p.split(',')[1]),
    )
    expect(failedYs.size).toBe(1)
    // Succeeded accumulates → more than one distinct y value.
    const succeededYs = new Set(
      (succeeded.querySelector('polyline')?.getAttribute('points') ?? '')
        .trim()
        .split(' ')
        .map((p) => p.split(',')[1]),
    )
    expect(succeededYs.size).toBeGreaterThan(1)
  })
})
