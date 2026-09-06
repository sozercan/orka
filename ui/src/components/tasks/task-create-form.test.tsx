import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@/test/test-utils'
import { act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { focusManager } from '@tanstack/react-query'
import { server } from '@/test/mocks/server'

let useStateTypeOverride: string | null = null

vi.mock('zustand/middleware', () => ({
  persist: (fn: unknown) => fn,
}))

vi.mock('react', async () => {
  const actual = await vi.importActual('react')
  return {
    ...actual,
    useState: (initial: any) => {
      if (initial === 'container' && useStateTypeOverride) {
        const override = useStateTypeOverride
        useStateTypeOverride = null
        return (actual as any).useState(override)
      }
      return (actual as any).useState(initial)
    },
  }
})

const mockNavigate = vi.fn()
vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual('@tanstack/react-router')
  return {
    ...actual,
    Link: ({ children, to, ...props }: any) => <a href={to} {...props}>{children}</a>,
    useNavigate: () => mockNavigate,
    useLocation: () => ({ pathname: '/tasks/new' }),
  }
})

vi.mock('sonner', () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

import { toast } from 'sonner'
import { useUIStore } from '@/stores/ui'
import { useAuthStore } from '@/stores/auth'
import { TaskCreateForm } from './task-create-form'

async function openWriteWorkspace(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByText(/Advanced Options/))
  await user.click(screen.getByText(/Workspace policy/))
  const intentTrigger = screen.getByText('Workspace intent').closest('.space-y-2')!.querySelector('[role="combobox"]')!
  fireEvent.pointerDown(intentTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
  fireEvent.click(await screen.findByRole('option', { name: /Write — produce/ }))
}

describe('TaskCreateForm', () => {
  beforeEach(() => {
    useUIStore.setState({ sidebarCollapsed: false, theme: 'light', namespace: 'default' })
    useAuthStore.setState({ token: 'test-token' })
    useStateTypeOverride = null
    mockNavigate.mockClear()
    vi.mocked(toast.success).mockClear()
    vi.mocked(toast.error).mockClear()
    // Polyfill pointer capture methods missing in jsdom (needed by Radix Select)
    if (!Element.prototype.hasPointerCapture) {
      Element.prototype.hasPointerCapture = () => false
    }
    if (!Element.prototype.setPointerCapture) {
      Element.prototype.setPointerCapture = () => {}
    }
    if (!Element.prototype.releasePointerCapture) {
      Element.prototype.releasePointerCapture = () => {}
    }
    if (!Element.prototype.scrollIntoView) {
      Element.prototype.scrollIntoView = () => {}
    }
    if (!globalThis.ResizeObserver) {
      globalThis.ResizeObserver = class {
        observe() {}
        unobserve() {}
        disconnect() {}
      } as unknown as typeof ResizeObserver
    }
  })

  it('renders form with name and type fields', () => {
    render(<TaskCreateForm />)
    expect(screen.getByText('Name')).toBeInTheDocument()
    expect(screen.getByText('Type')).toBeInTheDocument()
    expect(screen.getByPlaceholderText('my-task')).toBeInTheDocument()
  })

  it('container type shows image and command inputs', () => {
    render(<TaskCreateForm />)
    // Container is default type
    expect(screen.getByText('Image')).toBeInTheDocument()
    expect(screen.getByText('Command')).toBeInTheDocument()
    expect(screen.getByPlaceholderText('alpine:latest')).toBeInTheDocument()
    expect(screen.getByPlaceholderText('echo hello')).toBeInTheDocument()
  })

  it('AI type shows provider, model, prompt fields', async () => {
    render(<TaskCreateForm />)

    // Open the type select
    const typeTrigger = screen.getByText('Type').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    fireEvent.pointerDown(typeTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    await waitFor(() => {
      expect(screen.getByRole('option', { name: 'AI' })).toBeInTheDocument()
    })
    fireEvent.click(screen.getByRole('option', { name: 'AI' }))

    await waitFor(() => {
      expect(screen.getByText('Provider')).toBeInTheDocument()
    })
    expect(screen.getByText('Model')).toBeInTheDocument()
    expect(screen.getByText('Prompt')).toBeInTheDocument()
  })

  it('Agent type shows agent reference and prompt fields', async () => {
    render(<TaskCreateForm />)

    const typeTrigger = screen.getByText('Type').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    fireEvent.pointerDown(typeTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    await waitFor(() => {
      expect(screen.getByRole('option', { name: 'Agent' })).toBeInTheDocument()
    })
    fireEvent.click(screen.getByRole('option', { name: 'Agent' }))

    await waitFor(() => {
      expect(screen.getByText('Agent Reference')).toBeInTheDocument()
    })
    expect(screen.getByText('Prompt')).toBeInTheDocument()
  })

  it('renders Create Task and Cancel buttons', () => {
    render(<TaskCreateForm />)
    expect(screen.getByRole('button', { name: 'Create Task' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument()
  })

  it('submits container task and navigates', async () => {
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'test-task')
    await user.type(screen.getByPlaceholderText('alpine:latest'), 'nginx:latest')
    await user.type(screen.getByPlaceholderText('echo hello'), 'ls -la')

    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() => {
      expect(toast.success).toHaveBeenCalledWith('Task created')
    })
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/tasks/$taskId', params: { taskId: 'new-task' } })
  })

  it('tokenizes the container command like a shell, keeping quoted arguments intact', async () => {
    let submitted: Record<string, unknown> | undefined
    server.use(
      http.post('/api/v1/tasks', async ({ request }) => {
        submitted = (await request.json()) as Record<string, unknown>
        return HttpResponse.json({ metadata: { name: 'quoted', namespace: 'default' }, spec: { type: 'container' } })
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'quoted')
    await user.type(screen.getByPlaceholderText('alpine:latest'), 'alpine:latest')
    await user.type(screen.getByPlaceholderText('echo hello'), 'sh -c "echo UI_TASK_OK"')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() => {
      expect(toast.success).toHaveBeenCalledWith('Task created')
    })
    expect(submitted?.command).toEqual(['sh', '-c', 'echo UI_TASK_OK'])
  })

  it('flags an unterminated quote inline and blocks submission', async () => {
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    expect(screen.getByText(/Split like a shell/)).toBeInTheDocument()
    await user.type(screen.getByPlaceholderText('my-task'), 'bad-quote')
    await user.type(screen.getByPlaceholderText('alpine:latest'), 'alpine:latest')
    await user.type(screen.getByPlaceholderText('echo hello'), 'sh -c "echo oops')

    expect(screen.getByRole('alert')).toHaveTextContent('Unterminated double quote in command')
    expect(screen.getByPlaceholderText('echo hello')).toHaveAttribute('aria-invalid', 'true')

    await user.click(screen.getByRole('button', { name: 'Create Task' }))
    expect(toast.error).toHaveBeenCalledWith('Command is invalid: Unterminated double quote in command')
    expect(toast.success).not.toHaveBeenCalled()
    expect(mockNavigate).not.toHaveBeenCalled()
  })

  it('submits container task without command', async () => {
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'no-cmd-task')
    await user.type(screen.getByPlaceholderText('alpine:latest'), 'nginx:latest')
    // Don't fill in command to test the `if (command)` branch false path

    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() => {
      expect(toast.success).toHaveBeenCalledWith('Task created')
    })
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/tasks/$taskId', params: { taskId: 'new-task' } })
  })

  it('cancel button navigates to tasks', async () => {
    const user = userEvent.setup()
    render(<TaskCreateForm />)
    await user.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/tasks' })
  })

  it('shows error toast when submission fails', async () => {
    server.use(
      http.post('/api/v1/tasks', () => new HttpResponse('Bad request', { status: 400 })),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'bad-task')
    await user.type(screen.getByPlaceholderText('alpine:latest'), 'nginx')

    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalled()
    })
  })

  it('submits AI task form and navigates', async () => {
    useStateTypeOverride = 'ai'
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'ai-task')
    const providerTrigger = screen.getByRole('combobox', { name: 'AI provider' })
    fireEvent.pointerDown(providerTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    fireEvent.click(await screen.findByRole('option', { name: 'Anthropic' }))
    await user.type(screen.getByPlaceholderText('claude-sonnet-4-20250514'), 'my-model')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Hello AI')

    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() => {
      expect(toast.success).toHaveBeenCalledWith('Task created')
    })
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/tasks/$taskId', params: { taskId: 'new-task' } })
  })

  it('AI type lists only Agents without a built-in runtime and submits agentRef', async () => {
    useStateTypeOverride = 'ai'
    let submitted: Record<string, unknown> | undefined
    server.use(
      http.get('/api/v1/agents', () =>
        HttpResponse.json({
          items: [
            { metadata: { name: 'native-agent', namespace: 'default' }, spec: { providerRef: { name: 'anthropic' }, model: { provider: 'anthropic', name: 'claude' } } },
            { metadata: { name: 'codex-agent', namespace: 'default' }, spec: { runtime: { type: 'codex' } } },
          ],
        }),
      ),
      http.post('/api/v1/tasks', async ({ request }) => {
        submitted = (await request.json()) as Record<string, unknown>
        return HttpResponse.json({ metadata: { name: 'ai-task', namespace: 'default' }, spec: { type: 'ai' } })
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    const agentTrigger = await screen.findByRole('combobox', { name: 'AI agent' })
    await waitFor(() => expect(agentTrigger).not.toBeDisabled())
    fireEvent.pointerDown(agentTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    expect(await screen.findByRole('option', { name: /native-agent/ })).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: /codex-agent/ })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('option', { name: /native-agent/ }))

    await waitFor(() => expect(screen.getByTestId('ai-agent-info-card')).toBeInTheDocument())
    // Provider/model collapse into optional overrides once an Agent is chosen.
    expect(screen.queryByRole('combobox', { name: 'AI provider' })).not.toBeInTheDocument()
    expect(screen.getByText(/Provider \/ model overrides/)).toBeInTheDocument()
    expect(screen.getByText(/built-in CLI runtime are hidden/)).toBeInTheDocument()

    await user.type(screen.getByPlaceholderText('my-task'), 'ai-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Summarize')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Task created'))
    expect(submitted).toMatchObject({ type: 'ai', agentRef: { name: 'native-agent' }, ai: { prompt: 'Summarize' } })
    expect((submitted?.ai as Record<string, unknown>).provider).toBeUndefined()
  })

  it('drops a selected Agent when the list refetch errors instead of silently submitting it', async () => {
    useStateTypeOverride = 'ai'
    let submitted: Record<string, unknown> | undefined
    let agentCalls = 0
    server.use(
      http.get('/api/v1/agents', () => {
        agentCalls += 1
        if (agentCalls > 1) {
          return HttpResponse.json({ error: { code: 403, message: 'forbidden' } }, { status: 403 })
        }
        return HttpResponse.json({
          items: [
            { metadata: { name: 'native-agent', namespace: 'default' }, spec: { providerRef: { name: 'anthropic' }, model: { provider: 'anthropic', name: 'claude' } } },
          ],
        })
      }),
      http.post('/api/v1/tasks', async ({ request }) => {
        submitted = (await request.json()) as Record<string, unknown>
        return HttpResponse.json({ metadata: { name: 'ai-task', namespace: 'default' }, spec: { type: 'ai' } })
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    const agentTrigger = await screen.findByRole('combobox', { name: 'AI agent' })
    await waitFor(() => expect(agentTrigger).not.toBeDisabled())
    fireEvent.pointerDown(agentTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    fireEvent.click(await screen.findByRole('option', { name: /native-agent/ }))
    await waitFor(() => expect(screen.getByTestId('ai-agent-info-card')).toBeInTheDocument())

    // A refocus refetch now 403s; the selection must not survive as a
    // hidden agentRef while the selector renders empty.
    await act(async () => {
      focusManager.setFocused(false)
      focusManager.setFocused(true)
    })
    await waitFor(() => expect(agentCalls).toBeGreaterThan(1))
    await waitFor(() => expect(screen.queryByTestId('ai-agent-info-card')).not.toBeInTheDocument())

    await user.type(screen.getByPlaceholderText('my-task'), 'ai-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Summarize')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))
    // With the stale selection dropped, submission demands the visible
    // Agent/Provider choice instead of silently sending the hidden Agent.
    expect(toast.error).toHaveBeenCalledWith('Select an Agent or a Provider for the AI task')
    expect(submitted).toBeUndefined()
  })

  it('clears inline provider/model when an Agent is picked and only sends re-entered overrides', async () => {
    useStateTypeOverride = 'ai'
    const submitted: Array<Record<string, unknown>> = []
    server.use(
      http.get('/api/v1/agents', () =>
        HttpResponse.json({
          items: [
            { metadata: { name: 'native-agent', namespace: 'default' }, spec: { providerRef: { name: 'anthropic' }, model: { provider: 'anthropic', name: 'claude' } } },
          ],
        }),
      ),
      http.post('/api/v1/tasks', async ({ request }) => {
        submitted.push((await request.json()) as Record<string, unknown>)
        return HttpResponse.json({ metadata: { name: 'ai-task', namespace: 'default' }, spec: { type: 'ai' } })
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    // Configure the inline provider/model first, as if no Agent were going to be used.
    fireEvent.pointerDown(screen.getByRole('combobox', { name: 'AI provider' }), { button: 0, pointerId: 1, pointerType: 'mouse' })
    fireEvent.click(await screen.findByRole('option', { name: 'OpenAI' }))
    await user.type(screen.getByPlaceholderText('claude-sonnet-4-20250514'), 'gpt-5')

    const agentTrigger = await screen.findByRole('combobox', { name: 'AI agent' })
    await waitFor(() => expect(agentTrigger).not.toBeDisabled())
    fireEvent.pointerDown(agentTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    fireEvent.click(await screen.findByRole('option', { name: /native-agent/ }))
    await waitFor(() => expect(screen.getByTestId('ai-agent-info-card')).toBeInTheDocument())

    const disclosure = screen.getByRole('button', { name: /Provider \/ model overrides/ })
    expect(disclosure).toHaveAttribute('aria-expanded', 'false')
    expect(disclosure).toHaveAttribute('aria-controls', 'ai-model-overrides')
    expect(document.getElementById('ai-model-overrides')).toBeNull()

    await user.type(screen.getByPlaceholderText('my-task'), 'ai-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Summarize')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))
    await waitFor(() => expect(submitted).toHaveLength(1))
    // The stale inline values must not travel as hidden overrides.
    expect(submitted[0]).toMatchObject({ agentRef: { name: 'native-agent' }, ai: { prompt: 'Summarize' } })
    expect(submitted[0].ai).not.toHaveProperty('provider')
    expect(submitted[0].ai).not.toHaveProperty('model')

    // Overrides re-entered after opening the disclosure are sent.
    await user.click(disclosure)
    expect(disclosure).toHaveAttribute('aria-expanded', 'true')
    expect(document.getElementById('ai-model-overrides')).not.toBeNull()
    expect(screen.getByPlaceholderText('Agent default')).toHaveValue('')
    await user.type(screen.getByPlaceholderText('Agent default'), 'claude-opus-4-1')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))
    await waitFor(() => expect(submitted).toHaveLength(2))
    expect(submitted[1].ai).toEqual({ prompt: 'Summarize', model: 'claude-opus-4-1' })
  })

  it('hides the ignored provider override for Provider-backed Agents and resets the selection on namespace change', async () => {
    useStateTypeOverride = 'ai'
    server.use(
      http.get('/api/v1/agents', ({ request }) => {
        const ns = new URL(request.url).searchParams.get('namespace')
        return HttpResponse.json({
          items: ns === 'default'
            ? [{ metadata: { name: 'native-agent', namespace: 'default' }, spec: { providerRef: { name: 'anthropic' }, model: { provider: 'anthropic', name: 'claude' } } }]
            : [],
        })
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    const agentTrigger = await screen.findByRole('combobox', { name: 'AI agent' })
    await waitFor(() => expect(agentTrigger).not.toBeDisabled())
    fireEvent.pointerDown(agentTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    fireEvent.click(await screen.findByRole('option', { name: /native-agent/ }))
    await waitFor(() => expect(screen.getByTestId('ai-agent-info-card')).toBeInTheDocument())

    await user.click(screen.getByRole('button', { name: /Provider \/ model overrides/ }))
    // A providerRef Agent's Provider CRD is authoritative: no provider picker, only the model.
    expect(screen.queryByRole('combobox', { name: 'AI provider' })).not.toBeInTheDocument()
    expect(screen.getByTestId('ai-provider-locked')).toHaveTextContent('anthropic')
    expect(screen.getByPlaceholderText('Agent default')).toBeInTheDocument()

    // Switching namespaces drops the (namespace-scoped) selection.
    act(() => {
      useUIStore.setState({ namespace: 'other' })
    })
    await waitFor(() => expect(screen.queryByTestId('ai-agent-info-card')).not.toBeInTheDocument())
    expect(screen.queryByRole('button', { name: /Provider \/ model overrides/ })).not.toBeInTheDocument()
  })

  it('flags a trailing backslash inline and blocks submission', async () => {
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'bad-escape')
    await user.type(screen.getByPlaceholderText('alpine:latest'), 'alpine:latest')
    await user.type(screen.getByPlaceholderText('echo hello'), 'echo foo\\')

    expect(screen.getByRole('alert')).toHaveTextContent('Trailing backslash in command')
    expect(screen.getByPlaceholderText('echo hello')).toHaveAttribute('aria-invalid', 'true')

    await user.click(screen.getByRole('button', { name: 'Create Task' }))
    expect(toast.error).toHaveBeenCalledWith('Command is invalid: Trailing backslash in command')
    expect(toast.success).not.toHaveBeenCalled()
  })

  it('drops an AI Agent selection that disappears from the refreshed list', async () => {
    useStateTypeOverride = 'ai'
    let agents = [{ metadata: { name: 'native-agent', namespace: 'default' }, spec: { model: { provider: 'anthropic', name: 'claude' } } }]
    server.use(http.get('/api/v1/agents', () => HttpResponse.json({ items: agents })))
    render(<TaskCreateForm />)

    const agentTrigger = await screen.findByRole('combobox', { name: 'AI agent' })
    await waitFor(() => expect(agentTrigger).not.toBeDisabled())
    fireEvent.pointerDown(agentTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    fireEvent.click(await screen.findByRole('option', { name: /native-agent/ }))
    await waitFor(() => expect(screen.getByTestId('ai-agent-info-card')).toBeInTheDocument())

    // The Agent is deleted; the next refetch no longer lists it.
    agents = []
    window.dispatchEvent(new Event('visibilitychange'))
    await waitFor(() => expect(screen.queryByTestId('ai-agent-info-card')).not.toBeInTheDocument())
    expect(screen.getByRole('combobox', { name: 'AI provider' })).toBeInTheDocument()
  })

  it('AI type without an Agent still submits the inline provider path', async () => {
    useStateTypeOverride = 'ai'
    let submitted: Record<string, unknown> | undefined
    server.use(
      http.get('/api/v1/agents', () => HttpResponse.json({ items: [] })),
      http.post('/api/v1/tasks', async ({ request }) => {
        submitted = (await request.json()) as Record<string, unknown>
        return HttpResponse.json({ metadata: { name: 'ai-task', namespace: 'default' }, spec: { type: 'ai' } })
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'inline-ai')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Hello')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))
    expect(toast.error).toHaveBeenCalledWith('Select an Agent or a Provider for the AI task')

    const providerTrigger = screen.getByRole('combobox', { name: 'AI provider' })
    fireEvent.pointerDown(providerTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    fireEvent.click(await screen.findByRole('option', { name: 'OpenAI' }))
    await user.type(screen.getByPlaceholderText('claude-sonnet-4-20250514'), 'gpt-5')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Task created'))
    expect(submitted).toMatchObject({ type: 'ai', ai: { provider: 'openai', model: 'gpt-5', prompt: 'Hello' } })
    expect(submitted?.agentRef).toBeUndefined()
  })

  it('submits Agent task form and navigates', async () => {
    useStateTypeOverride = 'agent'
    server.use(
      http.get('/api/v1/agents', () =>
        HttpResponse.json({
          items: [
            { metadata: { name: 'my-agent', namespace: 'default' }, spec: { model: { name: 'claude' } } },
          ],
        }),
      ),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'agent-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Do something')

    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() => {
      expect(toast.success).toHaveBeenCalledWith('Task created')
    })
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/tasks/$taskId', params: { taskId: 'new-task' } })
  })

  it('toggles advanced options visibility', async () => {
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    expect(screen.queryByText('Priority')).not.toBeInTheDocument()
    expect(screen.queryByText('Timeout')).not.toBeInTheDocument()

    await user.click(screen.getByText(/Advanced Options/))

    expect(screen.getByText('Priority')).toBeInTheDocument()
    expect(screen.getByText('Timeout')).toBeInTheDocument()
    expect(screen.getByPlaceholderText('500')).toBeInTheDocument()
    expect(screen.getByPlaceholderText('30m')).toBeInTheDocument()
  })

  it('shows role-specific workspace credential names and optional keys', async () => {
    useStateTypeOverride = 'agent'
    server.use(
      http.get('/api/v1/agents', () =>
        HttpResponse.json({
          items: [
            { metadata: { name: 'my-agent', namespace: 'default' }, spec: { runtime: { type: 'codex' } } },
          ],
        }),
      ),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.click(screen.getByText(/Advanced Options/))
    expect(screen.queryByText('Max Turns')).not.toBeInTheDocument()
    expect(screen.queryByText('Allow Bash')).not.toBeInTheDocument()

    await user.click(screen.getByText(/Workspace policy/))
    expect(screen.getByText('Workspace intent')).toBeInTheDocument()
    expect(screen.getAllByText(/Read — verified workspace must remain unchanged/).length).toBeGreaterThan(0)
    expect(screen.getByText('Source repository URL')).toBeInTheDocument()
    expect(screen.getByLabelText('Source repository URL')).not.toBeRequired()
    expect(screen.getByLabelText('Source repository URL identity')).toHaveAttribute('placeholder', 'github.com/org/repo')
    expect(screen.queryByPlaceholderText('R_kgDOExample')).not.toBeInTheDocument()
    expect(screen.getByText(/normalized credential-free URL identity/)).toBeInTheDocument()
    expect(screen.getByLabelText('Read credential Secret')).toBeInTheDocument()
    expect(screen.getByLabelText('Read credential key')).toHaveAttribute('placeholder', 'token (default)')
    expect(screen.queryByLabelText('Publication write credential Secret')).not.toBeInTheDocument()

    const intentTrigger = screen.getByText('Workspace intent').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    fireEvent.pointerDown(intentTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    await waitFor(() => expect(screen.getByRole('option', { name: /Write — produce/ })).toBeInTheDocument())
    fireEvent.click(screen.getByRole('option', { name: /Write — produce/ }))

    for (const label of [
      'Publication read credential Secret',
      'Publication read credential key',
      'Publication write credential Secret',
      'Publication write credential key',
      'Forge credential Secret',
      'Forge credential key',
    ]) {
      expect(await screen.findByLabelText(label)).toBeInTheDocument()
    }
    expect(screen.getByLabelText('Source repository URL')).toBeRequired()
    expect(screen.getByLabelText('Publication write credential Secret')).toBeRequired()
    expect(screen.getByLabelText('Pull request base branch')).not.toBeRequired()
    expect(screen.getByLabelText('Forge credential Secret')).not.toBeRequired()
    expect(screen.getByText('Publication repository URL')).toBeInTheDocument()
    expect(screen.getByLabelText('Publication repository URL identity')).toHaveAttribute('placeholder', 'github.com/org/repo')
    expect(screen.getAllByText(/normalized credential-free URL identity/)).toHaveLength(2)
    expect(screen.getByText('Publication branch')).toBeInTheDocument()
    expect(screen.getByText(/Secret values are never shown/)).toBeInTheDocument()
    expect(screen.getByText(/Reconcile a pull request/)).toBeInTheDocument()
    await user.click(screen.getByRole('switch', { name: /Reconcile a pull request/ }))
    expect(screen.getByLabelText('Pull request base branch')).toBeRequired()
    expect(screen.getByLabelText('Forge credential Secret')).toBeRequired()
  })

  it('omits workspace from prompt-only agent tasks', async () => {
    useStateTypeOverride = 'agent'
    let submitted: any
    server.use(
      http.get('/api/v1/agents', () => HttpResponse.json({ items: [] })),
      http.post('/api/v1/tasks', async ({ request }) => {
        submitted = await request.json()
        return HttpResponse.json({ metadata: { name: submitted.name }, spec: submitted })
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'prompt-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Answer a question')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Task created'))
    expect(submitted.workspace).toBeUndefined()
  })

  it('rejects a source branch without a repository URL before mutation', async () => {
    useStateTypeOverride = 'agent'
    let submitted = false
    server.use(
      http.get('/api/v1/agents', () => HttpResponse.json({ items: [] })),
      http.post('/api/v1/tasks', () => {
        submitted = true
        return HttpResponse.json({ metadata: { name: 'never' } })
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'branch-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Review the repository')
    await user.click(screen.getByText(/Advanced Options/))
    await user.click(screen.getByText(/Workspace policy/))
    await user.type(screen.getByLabelText('Source branch'), 'main')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() =>
      expect(toast.error).toHaveBeenCalledWith('Source branch requires a source repository URL'),
    )
    expect(submitted).toBe(false)
  })

  it('canonicalizes a GitHub SSH source repository URL before submitting', async () => {
    useStateTypeOverride = 'agent'
    let submitted: any
    server.use(
      http.get('/api/v1/agents', () => HttpResponse.json({ items: [] })),
      http.post('/api/v1/tasks', async ({ request }) => {
        submitted = await request.json()
        return HttpResponse.json({ metadata: { name: submitted.name }, spec: submitted })
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'ssh-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Review the repository')
    await user.click(screen.getByText(/Advanced Options/))
    await user.click(screen.getByText(/Workspace policy/))
    await user.type(screen.getByLabelText('Source repository URL'), 'git@github.com:owner/repo.git')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Task created'))
    expect(submitted.workspace.gitRepo).toBe('https://github.com/owner/repo')
  })

  it('rejects an invalid source repository URL before mutation', async () => {
    useStateTypeOverride = 'agent'
    let submitted = false
    server.use(
      http.get('/api/v1/agents', () => HttpResponse.json({ items: [] })),
      http.post('/api/v1/tasks', () => {
        submitted = true
        return HttpResponse.json({ metadata: { name: 'never' } })
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'bad-url-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Review the repository')
    await user.click(screen.getByText(/Advanced Options/))
    await user.click(screen.getByText(/Workspace policy/))
    await user.type(screen.getByLabelText('Source repository URL'), 'http://github.com/owner/repo')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() =>
      expect(toast.error).toHaveBeenCalledWith(expect.stringContaining('credential-free HTTPS URL')),
    )
    expect(submitted).toBe(false)
  })

  it('submits top-level write workspace with distinct credential roles and keys', async () => {
    useStateTypeOverride = 'agent'
    let submitted: any
    server.use(
      http.get('/api/v1/agents', () => HttpResponse.json({ items: [] })),
      http.post('/api/v1/tasks', async ({ request }) => {
        submitted = await request.json()
        return HttpResponse.json({ metadata: { name: submitted.name }, spec: submitted })
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'write-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Update the repository')
    await user.click(screen.getByText(/Advanced Options/))
    await user.click(screen.getByText(/Workspace policy/))

    const intentTrigger = screen.getByText('Workspace intent').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    fireEvent.pointerDown(intentTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    fireEvent.click(await screen.findByRole('option', { name: /Write — produce/ }))

    const repositoryURLs = screen.getAllByPlaceholderText('https://github.com/org/repo')
    await user.type(repositoryURLs[0], 'https://github.com/source/repo')
    await user.type(screen.getByLabelText('Source repository provider'), 'github')
    await user.type(screen.getByLabelText('Source repository URL identity'), 'github.com/source/repo')
    await user.type(screen.getByLabelText('Read credential Secret'), 'source-read')
    await user.type(screen.getByLabelText('Read credential key'), 'source-token')
    await user.type(repositoryURLs[1], 'https://github.com/publish/repo')
    await user.type(screen.getByLabelText('Publication provider'), 'github')
    await user.type(screen.getByLabelText('Publication repository URL identity'), 'github.com/publish/repo')
    await user.type(screen.getByLabelText('Publication read credential Secret'), 'target-read')
    await user.type(screen.getByLabelText('Publication read credential key'), 'verify-token')
    await user.type(screen.getByLabelText('Publication write credential Secret'), 'target-write')
    await user.type(screen.getByLabelText('Publication write credential key'), 'write-token')
    await user.type(screen.getByLabelText('Forge credential Secret'), 'forge-api')
    await user.type(screen.getByLabelText('Forge credential key'), 'forge-token')
    await user.type(screen.getByPlaceholderText('Leave empty for an Orka-owned branch'), 'orka/change')
    await user.type(screen.getByLabelText('Pull request base branch'), 'main')
    await user.click(screen.getByRole('switch', { name: /Reconcile a pull request/ }))
    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Task created'))
    expect(submitted.workspace).toEqual({
      intent: 'write',
      gitRepo: 'https://github.com/source/repo',
      sourceRepository: { provider: 'github', id: 'github.com/source/repo' },
      readCredentialRef: { name: 'source-read', key: 'source-token' },
      publicationGitRepo: 'https://github.com/publish/repo',
      publicationRepository: { provider: 'github', id: 'github.com/publish/repo' },
      publicationReadCredentialRef: { name: 'target-read', key: 'verify-token' },
      publicationCredentialRef: { name: 'target-write', key: 'write-token' },
      forgeCredentialRef: { name: 'forge-api', key: 'forge-token' },
      pushBranch: 'orka/change',
      prBaseBranch: 'main',
      createPR: true,
    })
    expect(JSON.stringify(submitted)).not.toContain('must-never-render')
    expect(submitted.agentRuntime?.workspace).toBeUndefined()
    expect(submitted.workspace.gitSecretRef).toBeUndefined()
    expect(submitted.workspace.forkRepo).toBeUndefined()
  }, 10_000)

  it('requires a source repository URL for write workspaces', async () => {
    useStateTypeOverride = 'agent'
    let postCount = 0
    server.use(
      http.get('/api/v1/agents', () => HttpResponse.json({ items: [] })),
      http.post('/api/v1/tasks', () => {
        postCount += 1
        return HttpResponse.json({})
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'write-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Update the repository')
    await openWriteWorkspace(user)
    await user.type(screen.getByLabelText('Source repository URL'), '   ')
    await user.type(screen.getByLabelText('Publication write credential Secret'), 'target-write')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    expect(toast.error).toHaveBeenCalledWith('Source repository URL is required for write workspaces')
    expect(postCount).toBe(0)
    expect(toast.success).not.toHaveBeenCalled()
  })

  it('requires a publication write credential for write workspaces', async () => {
    useStateTypeOverride = 'agent'
    let postCount = 0
    server.use(
      http.get('/api/v1/agents', () => HttpResponse.json({ items: [] })),
      http.post('/api/v1/tasks', () => {
        postCount += 1
        return HttpResponse.json({})
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'write-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Update the repository')
    await openWriteWorkspace(user)
    await user.type(screen.getByLabelText('Source repository URL'), 'https://github.com/source/repo')
    await user.type(screen.getByLabelText('Publication write credential Secret'), '   ')
    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    expect(toast.error).toHaveBeenCalledWith('Publication write credential Secret is required for write workspaces')
    expect(postCount).toBe(0)
    expect(toast.success).not.toHaveBeenCalled()
  })

  it('requires a pull request base branch when creating a pull request', async () => {
    useStateTypeOverride = 'agent'
    let postCount = 0
    server.use(
      http.get('/api/v1/agents', () => HttpResponse.json({ items: [] })),
      http.post('/api/v1/tasks', () => {
        postCount += 1
        return HttpResponse.json({})
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'write-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Update the repository')
    await openWriteWorkspace(user)
    await user.type(screen.getByLabelText('Source repository URL'), 'https://github.com/source/repo')
    await user.type(screen.getByLabelText('Publication write credential Secret'), 'target-write')
    await user.type(screen.getByLabelText('Forge credential Secret'), 'forge-api')
    await user.type(screen.getByLabelText('Pull request base branch'), '   ')
    await user.click(screen.getByRole('switch', { name: /Reconcile a pull request/ }))
    await user.click(screen.getByRole('button', { name: 'Create Task' }))

    expect(toast.error).toHaveBeenCalledWith('Pull request base branch is required when creating a pull request')
    expect(postCount).toBe(0)
    expect(toast.success).not.toHaveBeenCalled()
  })

  it('requires a forge credential before creating a pull request', async () => {
    useStateTypeOverride = 'agent'
    let postCount = 0
    server.use(
      http.get('/api/v1/agents', () => HttpResponse.json({ items: [] })),
      http.post('/api/v1/tasks', () => {
        postCount += 1
        return HttpResponse.json({})
      }),
    )
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.type(screen.getByPlaceholderText('my-task'), 'write-task')
    await user.type(screen.getByPlaceholderText('Enter your prompt...'), 'Update the repository')
    await openWriteWorkspace(user)
    await user.type(screen.getByLabelText('Source repository URL'), 'https://github.com/source/repo')
    await user.type(screen.getByLabelText('Publication write credential Secret'), 'target-write')
    await user.type(screen.getByLabelText('Pull request base branch'), 'main')
    await user.click(screen.getByRole('switch', { name: /Reconcile a pull request/ }))
    const submitButton = screen.getByRole('button', { name: 'Create Task' })
    fireEvent.submit(submitButton.closest('form')!)

    expect(toast.error).toHaveBeenCalledWith('Forge credential Secret is required when creating a pull request')
    expect(postCount).toBe(0)
    expect(toast.success).not.toHaveBeenCalled()
  })

  it('does not show workspace config for non-agent types', async () => {
    const user = userEvent.setup()
    render(<TaskCreateForm />)

    await user.click(screen.getByText(/Advanced Options/))

    expect(screen.queryByText('Workspace policy')).not.toBeInTheDocument()
  })

  it('shows agent info card when agent is selected', async () => {
    useStateTypeOverride = 'agent'
    server.use(
      http.get('/api/v1/agents', () =>
        HttpResponse.json({
          items: [
            {
              metadata: { name: 'coord-agent', namespace: 'default' },
              spec: {
                model: { provider: 'anthropic', name: 'claude-sonnet' },
                runtime: { type: 'codex' },
                coordination: { enabled: true },
                tools: [{ name: 'tool1' }, { name: 'tool2' }],
              },
            },
          ],
        }),
      ),
    )
    render(<TaskCreateForm />)

    // Wait for agents to load and select the agent
    const trigger = screen.getByText('Agent Reference').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    await waitFor(() => expect(trigger).not.toBeDisabled())
    fireEvent.pointerDown(trigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    await waitFor(() => {
      expect(screen.getByRole('option', { name: /coord-agent/ })).toBeInTheDocument()
    })
    fireEvent.click(screen.getByRole('option', { name: /coord-agent/ }))

    await waitFor(() => {
      expect(screen.getByTestId('agent-info-card')).toBeInTheDocument()
    })
    expect(screen.getByText('anthropic')).toBeInTheDocument()
    expect(screen.getByText('claude-sonnet')).toBeInTheDocument()
    expect(screen.getByText('codex ACP')).toBeInTheDocument()
    expect(screen.getByText('Coordination')).toBeInTheDocument()
    expect(screen.getByText('2 tools')).toBeInTheDocument()
  })

  it('hides external-runtime agents that cannot be dispatched', async () => {
    useStateTypeOverride = 'agent'
    server.use(
      http.get('/api/v1/agents', () =>
        HttpResponse.json({
          items: [
            {
              metadata: { name: 'built-in-agent', namespace: 'default' },
              spec: { runtime: { type: 'codex' } },
            },
            {
              metadata: { name: 'external-agent', namespace: 'default' },
              spec: { runtime: { runtimeRef: { name: 'external-codex' } } },
            },
            {
              metadata: { name: 'provider-agent', namespace: 'default' },
              spec: { model: { provider: 'openai', name: 'gpt-5.4' } },
            },
          ],
        }),
      ),
    )
    render(<TaskCreateForm />)

    expect(await screen.findByText(/Agents without a built-in CLI runtime are hidden/)).toBeInTheDocument()
    const trigger = screen.getByText('Agent Reference').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    fireEvent.pointerDown(trigger, { button: 0, pointerId: 1, pointerType: 'mouse' })

    expect(await screen.findByRole('option', { name: /built-in-agent/ })).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: /external-agent/ })).not.toBeInTheDocument()
    expect(screen.queryByRole('option', { name: /provider-agent/ })).not.toBeInTheDocument()
  })

  it('clears a runtime Agent selection when the namespace changes', async () => {
    useStateTypeOverride = 'agent'
    server.use(
      http.get('/api/v1/agents', ({ request }) => {
        const namespace = new URL(request.url).searchParams.get('namespace')
        return HttpResponse.json({
          items: [{ metadata: { name: 'shared-agent', namespace }, spec: { runtime: { type: 'codex' } } }],
          metadata: {},
        })
      }),
    )
    render(<TaskCreateForm />)

    const trigger = screen.getByText('Agent Reference').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    await waitFor(() => expect(trigger).not.toBeDisabled())
    fireEvent.pointerDown(trigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    fireEvent.click(await screen.findByRole('option', { name: /shared-agent/ }))
    await waitFor(() => expect(screen.getByTestId('agent-info-card')).toBeInTheDocument())

    act(() => useUIStore.setState({ namespace: 'other' }))
    await waitFor(() => expect(screen.queryByTestId('agent-info-card')).not.toBeInTheDocument())
    expect(trigger).toHaveTextContent('Select an agent')
  })
})
