import { describe, it, expect, vi } from 'vitest'
import { act, render, screen } from '@/test/test-utils'

vi.mock('zustand/middleware', () => ({
  persist: (fn: unknown) => fn,
}))

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual('@tanstack/react-router')
  return {
    ...actual,
    createRouter: () => ({}),
    RouterProvider: () => <div data-testid="router">Router</div>,
  }
})

vi.mock('./routeTree.gen', () => ({
  routeTree: {},
}))

import { App } from './app'
import { useChatStore } from './stores/chat'
import { useUIStore } from './stores/ui'

describe('App', () => {
  it('renders without crashing', () => {
    render(<App />)
    expect(screen.getByTestId('router')).toBeInTheDocument()
  })

  it('resets chat when the namespace changes outside the Chat page', () => {
    act(() => useUIStore.getState().setNamespace('default'))
    useChatStore.setState({
      activeNamespace: 'default',
      messages: [{ id: 'message-1', role: 'user', content: 'still running', timestamp: '2026-08-31T00:00:00Z' }],
      currentSessionId: 'session-default',
      isStreaming: true,
      turnEpoch: 4,
    })

    act(() => useUIStore.getState().setNamespace('team'))

    expect(useChatStore.getState()).toMatchObject({
      activeNamespace: 'team',
      messages: [],
      currentSessionId: null,
      isStreaming: false,
      turnEpoch: 5,
    })
  })
})
