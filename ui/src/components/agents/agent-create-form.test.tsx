import { describe, it, expect, beforeEach, vi } from 'vitest'

let useStateModeOverride: string | null = null

vi.mock('zustand/middleware', () => ({ persist: (fn: unknown) => fn }))

const mockNavigate = vi.fn()
vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual('@tanstack/react-router')
  return {
    ...actual,
    Link: ({ children, to, ...props }: any) => <a href={to} {...props}>{children}</a>,
    useNavigate: () => mockNavigate,
    useLocation: () => ({ pathname: '/agents/new' }),
  }
})

vi.mock('sonner', () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

vi.mock('react', async () => {
  const actual = await vi.importActual('react')
  return {
    ...actual,
    useState: (initial: any) => {
      if (initial === 'ai' && useStateModeOverride) {
        const override = useStateModeOverride
        useStateModeOverride = null
        return (actual as any).useState(override)
      }
      return (actual as any).useState(initial)
    },
  }
})

import { render, screen, waitFor, fireEvent, act } from '@/test/test-utils'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { server } from '@/test/mocks/server'
import { toast } from 'sonner'
import { useUIStore } from '@/stores/ui'
import { useAuthStore } from '@/stores/auth'
import { AgentCreateForm } from './agent-create-form'

function installDOMPolyfills() {
  if (typeof globalThis.ResizeObserver === 'undefined') {
    globalThis.ResizeObserver = class { observe() {}; unobserve() {}; disconnect() {} } as any
  }
  if (!HTMLElement.prototype.hasPointerCapture) HTMLElement.prototype.hasPointerCapture = () => false
  if (!HTMLElement.prototype.setPointerCapture) HTMLElement.prototype.setPointerCapture = () => {}
  if (!HTMLElement.prototype.releasePointerCapture) HTMLElement.prototype.releasePointerCapture = () => {}
  if (!HTMLElement.prototype.scrollIntoView) HTMLElement.prototype.scrollIntoView = () => {}
}

describe('AgentCreateForm', () => {
  beforeEach(() => {
    installDOMPolyfills()
    useUIStore.setState({ namespace: 'default', sidebarCollapsed: false, theme: 'light' })
    useAuthStore.setState({ token: 'test-token' })
    useStateModeOverride = null
    mockNavigate.mockClear()
    vi.mocked(toast.success).mockClear()
    vi.mocked(toast.error).mockClear()
  })

  it('preserves native AI configuration fields', () => {
    render(<AgentCreateForm />)
    expect(screen.getAllByText('Native AI (LLM provider)').length).toBeGreaterThan(0)
    expect(screen.getByText('Provider')).toBeInTheDocument()
    expect(screen.getByText('Temperature')).toBeInTheDocument()
    expect(screen.getByText('Max Tokens')).toBeInTheDocument()
    expect(screen.getByText('Secret Reference')).toBeInTheDocument()
  })

  it('shows all built-in ACP runtime options without legacy loop controls', async () => {
    useStateModeOverride = 'runtime'
    const user = userEvent.setup()
    render(<AgentCreateForm />)
    expect(screen.getByText('Runtime source')).toBeInTheDocument()
    expect(screen.getByText('Runtime profile')).toBeInTheDocument()
    expect(screen.getAllByText('Orka-managed RuntimePool').length).toBeGreaterThan(0)
    expect(screen.getByLabelText('Model')).toBeRequired()
    expect(screen.getByLabelText('Model')).toHaveAttribute('placeholder', 'Enter model identifier')
    expect(screen.queryByText('Max Turns')).not.toBeInTheDocument()
    expect(screen.queryByText('Allowed Tools')).not.toBeInTheDocument()
    expect(screen.queryByText('Allow Bash')).not.toBeInTheDocument()

    const profileTrigger = screen.getByText('Runtime profile').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    await user.click(profileTrigger)
    expect(await screen.findByRole('option', { name: 'Claude ACP' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'OpenAI Codex ACP' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'GitHub Copilot ACP' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'OpenCode ACP' })).toBeInTheDocument()
  })

  it('submits a built-in ACP runtime profile', async () => {
    useStateModeOverride = 'runtime'
    let submitted: any
    server.use(http.post('/api/v1/agents', async ({ request }) => {
      submitted = await request.json()
      return HttpResponse.json({ metadata: { name: submitted.name }, spec: submitted.spec })
    }))

    const user = userEvent.setup()
    render(<AgentCreateForm />)
    await user.type(screen.getByPlaceholderText('my-agent'), 'runtime-agent')
    await user.type(screen.getByLabelText('Model'), 'claude-sonnet-4-20250514')
    await user.click(screen.getByRole('button', { name: 'Create Agent' }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Agent created'))
    expect(submitted.spec.runtime).toEqual({ type: 'claude' })
    expect(submitted.spec.model).toEqual({ name: 'claude-sonnet-4-20250514' })
    expect(submitted.spec.runtime.defaultMaxTurns).toBeUndefined()
    expect(submitted.spec.secretRef).toBeUndefined()
  })

  it('submits a built-in Copilot ACP runtime profile', async () => {
    useStateModeOverride = 'runtime'
    let submitted: any
    server.use(http.post('/api/v1/agents', async ({ request }) => {
      submitted = await request.json()
      return HttpResponse.json({ metadata: { name: submitted.name }, spec: submitted.spec })
    }))

    const user = userEvent.setup()
    render(<AgentCreateForm />)
    await user.type(screen.getByPlaceholderText('my-agent'), 'copilot-agent')
    const profileTrigger = screen.getByText('Runtime profile').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    await act(async () => {
      fireEvent.pointerDown(profileTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    })
    const copilotOption = await screen.findByRole('option', { name: 'GitHub Copilot ACP' })
    await act(async () => {
      fireEvent.click(copilotOption)
    })
    await waitFor(() => expect(profileTrigger).toHaveTextContent('GitHub Copilot ACP'))
    await user.type(screen.getByLabelText('Model'), 'gpt-5.3-codex')
    await user.click(screen.getByRole('button', { name: 'Create Agent' }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Agent created'))
    expect(submitted.spec.runtime).toEqual({ type: 'copilot' })
    expect(submitted.spec.model).toEqual({ name: 'gpt-5.3-codex' })
  })

  it('submits OpenCode with its reviewed defaults and required provider/model ID', async () => {
    useStateModeOverride = 'runtime'
    let submitted: any
    server.use(http.post('/api/v1/agents', async ({ request }) => {
      submitted = await request.json()
      return HttpResponse.json({ metadata: { name: submitted.name }, spec: submitted.spec })
    }))

    const user = userEvent.setup()
    render(<AgentCreateForm />)
    await user.type(screen.getByPlaceholderText('my-agent'), 'opencode-agent')
    const profileTrigger = screen.getByText('Runtime profile').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    await act(async () => {
      fireEvent.pointerDown(profileTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    })
    const opencodeOption = await screen.findByRole('option', { name: 'OpenCode ACP' })
    await act(async () => {
      fireEvent.click(opencodeOption)
    })
    await waitFor(() => expect(profileTrigger).toHaveTextContent('OpenCode ACP'))

    const modelInput = screen.getByLabelText('Model')
    expect(modelInput).toBeRequired()
    expect(modelInput).toHaveAttribute('placeholder', 'openai/gpt-5.4')
    expect(screen.getByText('OpenCode model IDs use provider/model form.')).toBeInTheDocument()
    expect(screen.getByText(/Reviewed native-tool defaults: Read, Write, Edit, Bash, Glob, and Grep/)).toBeInTheDocument()
    await user.type(modelInput, 'openai/gpt-5.4')
    await user.type(screen.getByLabelText('Context Window'), '32768')
    await user.type(screen.getByLabelText('Max Output Tokens'), '4096')
    await user.click(screen.getByRole('button', { name: 'Create Agent' }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Agent created'))
    expect(submitted.spec.runtime).toEqual({
      type: 'opencode',
      defaultAllowedTools: ['Read', 'Write', 'Edit', 'Bash', 'Glob', 'Grep'],
      defaultAllowBash: true,
    })
    expect(submitted.spec.model).toEqual({ name: 'openai/gpt-5.4', contextWindow: 32768, maxTokens: 4096 })
  })

  it('rejects OpenCode when reviewed model limits are missing', async () => {
    useStateModeOverride = 'runtime'
    let postCount = 0
    server.use(http.post('/api/v1/agents', () => {
      postCount += 1
      return HttpResponse.json({})
    }))

    const user = userEvent.setup()
    render(<AgentCreateForm />)
    await user.type(screen.getByPlaceholderText('my-agent'), 'opencode-agent')
    const profileTrigger = screen.getByText('Runtime profile').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    await act(async () => {
      fireEvent.pointerDown(profileTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    })
    fireEvent.click(await screen.findByRole('option', { name: 'OpenCode ACP' }))
    await user.type(screen.getByLabelText('Model'), 'openai/gpt-5.4')
    await user.click(screen.getByRole('button', { name: 'Create Agent' }))

    expect(toast.error).toHaveBeenCalledWith('OpenCode requires a positive context window')
    expect(postCount).toBe(0)
  })

  it('rejects a bare OpenCode model name before posting', async () => {
    useStateModeOverride = 'runtime'
    let postCount = 0
    server.use(http.post('/api/v1/agents', () => {
      postCount += 1
      return HttpResponse.json({})
    }))

    const user = userEvent.setup()
    render(<AgentCreateForm />)
    await user.type(screen.getByPlaceholderText('my-agent'), 'opencode-agent')
    const profileTrigger = screen.getByText('Runtime profile').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    await act(async () => {
      fireEvent.pointerDown(profileTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    })
    const opencodeOption = await screen.findByRole('option', { name: 'OpenCode ACP' })
    await act(async () => {
      fireEvent.click(opencodeOption)
    })
    await user.type(screen.getByLabelText('Model'), 'gpt-5.4')
    await user.click(screen.getByRole('button', { name: 'Create Agent' }))

    expect(toast.error).toHaveBeenCalledWith('OpenCode requires a literal provider/model ID')
    expect(postCount).toBe(0)
    expect(toast.success).not.toHaveBeenCalled()
  })

  it('rejects a blank model for built-in ACP runtimes before posting', async () => {
    useStateModeOverride = 'runtime'
    let postCount = 0
    server.use(http.post('/api/v1/agents', () => {
      postCount += 1
      return HttpResponse.json({})
    }))

    const user = userEvent.setup()
    render(<AgentCreateForm />)
    await user.type(screen.getByPlaceholderText('my-agent'), 'runtime-agent')
    await user.type(screen.getByLabelText('Model'), '   ')
    await user.click(screen.getByRole('button', { name: 'Create Agent' }))

    expect(toast.error).toHaveBeenCalledWith('Model is required for built-in ACP runtimes')
    expect(postCount).toBe(0)
    expect(toast.success).not.toHaveBeenCalled()
    expect(mockNavigate).not.toHaveBeenCalled()
  })

  it('submits an external v2 AgentRuntime reference', async () => {
    useStateModeOverride = 'runtime'
    let submitted: any
    server.use(http.post('/api/v1/agents', async ({ request }) => {
      submitted = await request.json()
      return HttpResponse.json({ metadata: { name: submitted.name }, spec: submitted.spec })
    }))

    const user = userEvent.setup()
    render(<AgentCreateForm />)
    await user.type(screen.getByPlaceholderText('my-agent'), 'external-agent')
    await user.type(screen.getByLabelText('Model'), 'stale-built-in-model')
    const sourceTrigger = screen.getByText('Runtime source').closest('.space-y-2')!.querySelector('[role="combobox"]')!
    await act(async () => {
      fireEvent.pointerDown(sourceTrigger, { button: 0, pointerId: 1, pointerType: 'mouse' })
    })
    const externalOption = await screen.findByRole('option', { name: 'External v2 AgentRuntime' })
    await act(async () => {
      fireEvent.click(externalOption)
    })
    await waitFor(() => expect(sourceTrigger).toHaveTextContent('External v2 AgentRuntime'))
    await user.type(screen.getByPlaceholderText('external-codex'), 'external-codex')
    expect(screen.queryByLabelText('Model')).not.toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Create Agent' }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Agent created'))
    expect(submitted.spec.runtime).toEqual({ runtimeRef: { name: 'external-codex' } })
    expect(submitted.spec.model).toBeUndefined()
  })

  it('submits native AI agents unchanged', async () => {
    let submitted: any
    server.use(http.post('/api/v1/agents', async ({ request }) => {
      submitted = await request.json()
      return HttpResponse.json({ metadata: { name: submitted.name }, spec: submitted.spec })
    }))
    const user = userEvent.setup()
    render(<AgentCreateForm />)
    await user.type(screen.getByPlaceholderText('my-agent'), 'native-agent')
    await user.type(screen.getByPlaceholderText('claude-sonnet-4-20250514'), 'native-model')
    await user.click(screen.getByRole('button', { name: 'Create Agent' }))

    await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Agent created'))
    expect(submitted.spec.runtime).toBeUndefined()
    expect(submitted.spec.model.name).toBe('native-model')
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/agents' })
  })
})
