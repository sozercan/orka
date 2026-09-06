import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw'
import { render, screen } from '@/test/test-utils'
import { server } from '@/test/mocks/server'
import { useAuthStore } from '@/stores/auth'
import { useUIStore } from '@/stores/ui'
import { GatewayBindingDetail } from './gateway-binding-detail'

vi.mock('zustand/middleware', () => ({ persist: (fn: unknown) => fn }))
vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual('@tanstack/react-router')
  return {
    ...actual,
    Link: ({ children, to, params, className }: { children: ReactNode; to: string; params?: Record<string, string>; className?: string }) => {
      let href = to
      for (const [name, value] of Object.entries(params ?? {})) href = href.replace(`$${name}`, value)
      return <a href={href} className={className}>{children}</a>
    },
  }
})

const binding = {
  metadata: { name: 'support-binding', namespace: 'default', generation: 4, creationTimestamp: '2026-07-17T08:00:00Z' },
  spec: {
    gatewayRef: { name: 'chat' },
    agentRef: { name: 'operator' },
    match: { accountId: 'acme', contextId: 'support', threadId: 'thread-7', senderId: 'sender-exact' },
    senderPolicy: { mode: 'allowlist', allowedSenderIds: ['sender-exact', 'sender-backup'] },
    priority: 25,
    session: { mode: 'thread-sender' },
    activeTurnBehavior: 'queue',
    taskDefaults: {
      priority: 50,
      timeout: '5m',
      retryPolicy: { maxRetries: 3, backoffMultiplier: 2, initialDelay: '10s' },
      agentRuntimeMaxTurns: 12,
    },
  },
  status: {
    accepted: true,
    resolvedRefs: true,
    programmed: true,
    ready: true,
    observedGeneration: 4,
    resolvedCapabilities: { inboundText: true, outboundText: true, senderIdentity: true },
    lastInboundActivity: '2026-07-18T09:00:00Z',
    lastOutboundActivity: '2026-07-18T09:01:00Z',
    conditions: [{ type: 'Ready', status: 'True', reason: 'Programmed' }],
  },
}

describe('GatewayBindingDetail', () => {
  beforeEach(() => {
    useAuthStore.setState({ ['token']: 'test-token' })
    useUIStore.setState({ namespace: 'default' })
    server.use(http.get('/api/v1/gatewaybindings/support-binding', () => HttpResponse.json(binding)))
  })

  it('renders query failures instead of a not-found state', async () => {
    server.use(http.get('/api/v1/gatewaybindings/support-binding', () => HttpResponse.json(
      { error: 'gateway backend unavailable' },
      { status: 503 },
    )))

    render(<GatewayBindingDetail name="support-binding" />)

    expect(await screen.findByText('Could not load GatewayBinding support-binding')).toBeInTheDocument()
    expect(screen.queryByText('GatewayBinding not found.')).not.toBeInTheDocument()
  })

  it('shows routing, authorization, Session, Task, and readiness details', async () => {
    render(<GatewayBindingDetail name="support-binding" />)

    expect(await screen.findByRole('heading', { name: 'support-binding' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'chat' })).toHaveAttribute('href', '/gateways/chat')
    expect(screen.getByRole('link', { name: 'operator' })).toHaveAttribute('href', '/agents/operator')
    expect(screen.getByText('thread-7')).toBeInTheDocument()
    expect(screen.getByText('sender-backup')).toBeInTheDocument()
    expect(screen.getByText('thread-sender')).toBeInTheDocument()
    expect(screen.getByText('FIFO per Session')).toBeInTheDocument()
    expect(screen.getByText('5m')).toBeInTheDocument()
    expect(screen.getByText('sender identity')).toBeInTheDocument()
    expect(screen.getByText('Programmed')).toBeInTheDocument()
  })
})
