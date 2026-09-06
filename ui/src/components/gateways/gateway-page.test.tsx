import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('zustand/middleware', () => ({ persist: (fn: unknown) => fn }))
vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual('@tanstack/react-router')
  return { ...actual, Link: ({ children, to, ...props }: any) => <a href={to} {...props}>{children}</a> }
})

import { http, HttpResponse } from 'msw'
import userEvent from '@testing-library/user-event'
import { render, screen, waitFor } from '@/test/test-utils'
import { server } from '@/test/mocks/server'
import { useAuthStore } from '@/stores/auth'
import { useUIStore } from '@/stores/ui'
import { GatewayPage } from './gateway-page'

const list = (items: unknown[]) => HttpResponse.json({ items, metadata: {} })

describe('GatewayPage', () => {
  beforeEach(() => {
    useAuthStore.setState({ ['token']: 'test-token' })
    useUIStore.setState({ namespace: 'default' })
  })
  it('renders switchboard inventory and durable ledgers', async () => {
    server.use(
      http.get('/api/v1/gateways', () => list([{
        metadata: { name: 'chat', namespace: 'default', generation: 1 },
        spec: { gatewayClassName: 'generic-chat', adapter: { endpoint: 'https://adapter.example' } },
        status: { ready: true, observedGeneration: 1, resolvedEndpoint: 'https://adapter.example', observedCapabilities: { adapterName: 'adapter', adapterVersion: 'v1', capabilities: { inboundText: true, outboundText: true, idempotentDelivery: true } } },
      }])),
      http.get('/api/v1/gatewaybindings', () => list([])),
      http.get('/api/v1/gateway-events', () => list([])),
      http.get('/api/v1/gateway-deliveries', () => list([])),
    )
    render(<GatewayPage />)
    expect(await screen.findByText('Gateway switchboard')).toBeInTheDocument()
    expect(await screen.findByText('chat')).toBeInTheDocument()
    expect(screen.getByText('Ingress records loaded')).toBeInTheDocument()
    expect(screen.getByText('Current ledger page sample')).toBeInTheDocument()
    expect(screen.getByText(/not namespace totals/i)).toBeInTheDocument()
    expect(screen.getByText('idempotent delivery')).toBeInTheDocument()
  })

  it('navigates gateway event pages explicitly', async () => {
    const cursors: (string | null)[] = []
    server.use(
      http.get('/api/v1/gateways', () => list([])),
      http.get('/api/v1/gatewaybindings', () => list([])),
      http.get('/api/v1/gateway-events', ({ request }) => {
        const cursor = new URL(request.url).searchParams.get('continue') || null
        cursors.push(cursor)
        return HttpResponse.json({ items: [], metadata: cursor ? {} : { continue: 'next-events' } })
      }),
      http.get('/api/v1/gateway-deliveries', () => list([])),
    )
    const user = userEvent.setup()
    render(<GatewayPage />)

    await screen.findByText('No Gateways are configured in this namespace.')
    await user.click(screen.getByRole('tab', { name: 'Event ledger' }))
    const next = await screen.findByRole('button', { name: 'Next events page' })
    expect(next).toBeEnabled()
    await user.click(next)

    await waitFor(() => expect(cursors).toContain('next-events'))
    expect(screen.getByText('Page 2 · up to 100 records')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Previous events page' })).toBeEnabled()
  })

  it('does not present a stale Gateway status as Ready', async () => {
    server.use(
      http.get('/api/v1/gateways', () => list([{
        metadata: { name: 'reconciling-chat', namespace: 'default', generation: 7 },
        spec: { gatewayClassName: 'generic-chat', adapter: { endpoint: 'https://adapter.example' } },
        status: { ready: true, observedGeneration: 6, resolvedEndpoint: 'https://adapter.example' },
      }])),
      http.get('/api/v1/gatewaybindings', () => list([])),
      http.get('/api/v1/gateway-events', () => list([])),
      http.get('/api/v1/gateway-deliveries', () => list([])),
    )

    render(<GatewayPage />)

    expect(await screen.findByText('reconciling-chat')).toBeInTheDocument()
    expect(screen.getByText('NotReady')).toBeInTheDocument()
    expect(screen.getByText(/status generation 6 is stale for generation 7/i)).toBeInTheDocument()
    expect(screen.queryByText('Ready')).not.toBeInTheDocument()
  })

  it('renders inventory failures instead of an empty-state claim', async () => {
    server.use(
      http.get('/api/v1/gateways', () => HttpResponse.text('inventory unavailable', { status: 500 })),
      http.get('/api/v1/gatewaybindings', () => list([])),
      http.get('/api/v1/gateway-events', () => list([])),
      http.get('/api/v1/gateway-deliveries', () => list([])),
    )

    render(<GatewayPage />)

    expect(await screen.findByText('Could not load Gateways')).toBeInTheDocument()
    expect(screen.getByText('inventory unavailable')).toBeInTheDocument()
    expect(screen.queryByText('No Gateways are configured in this namespace.')).not.toBeInTheDocument()
  })
})
