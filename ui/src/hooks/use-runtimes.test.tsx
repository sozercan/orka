import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { renderHook, waitFor } from '@testing-library/react'
import { http, HttpResponse } from 'msw'
import type { ReactNode } from 'react'
import { server } from '@/test/mocks/server'

vi.mock('zustand/middleware', () => ({
  persist: (fn: unknown) => fn,
}))

import { useUIStore } from '@/stores/ui'
import { useAgentRuntimes, useRuntimePools } from './use-runtimes'

const digest = `sha256:${'a'.repeat(64)}`

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  })
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
}

function runtimePool(name: string) {
  return {
    metadata: { name, namespace: 'default', uid: `${name}-uid` },
    spec: {
      trustDomain: { namespace: 'default', identity: 'default/operators' },
      runtime: {
        image: 'registry.example.test/orka-acp-codex@sha256:abc',
        profile: {
          protocolVersion: 'orka.harness.v2',
          digest,
          digestSchemaVersion: 'v1',
          acpProfile: 'schema-v1.20.0',
          adapterDigests: { 'codex-acp': digest },
          providerKind: 'openai',
          model: 'gpt-5',
          agentConfigurationDigest: digest,
          toolPolicyDigest: digest,
          approvalPolicyDigest: digest,
          mcpConfigurationDigest: digest,
          workspaceIntent: 'read',
          proxyCredentialRole: 'provider',
          proxyCredentialScope: 'codex',
          resourceClass: 'standard',
        },
      },
    },
  }
}

function agentRuntime(name: string) {
  return {
    metadata: { name, namespace: 'default', uid: `${name}-uid` },
    spec: {
      contractVersion: 'orka.harness.v2',
      deployment: { mode: 'external-endpoint', endpoint: 'https://runtime.example.test' },
      clientAuth: {
        controllerBearerTokenSecretRef: { name: 'auth', key: 'controller-token' },
        operationCapabilitySecretRef: { name: 'auth', key: 'capability-secret' },
      },
      capabilities: {
        runtimeInstanceID: `${name}-instance`,
        profile: {
          digest,
          digestSchemaVersion: 1,
          acpProfile: 'schema-v1.20.0',
          adapterName: 'codex-acp',
          adapterDigest: digest,
          providerKind: 'openai',
          model: 'gpt-5',
          agentConfigurationDigest: digest,
          toolPolicyDigest: digest,
          approvalPolicyDigest: digest,
          mcpConfigurationDigest: digest,
          workspaceIntent: 'read',
          proxyCredentialRole: 'provider',
          proxyCredentialScope: 'codex',
          resourceClass: 'standard',
        },
        mcpPolicy: {
          allowedTools: ['web_search'],
          disallowedTools: [],
          allowBash: false,
          approvalRequiredTools: [],
        },
        limits: {
          maxResidentSessions: 10,
          maxConcurrentPrompts: 4,
          maxRequestBytes: 1000,
          maxEventLineBytes: 1000,
          maxTerminalResultBytes: 1000,
          maxBufferedEvents: 100,
          maxUpdateEventsPerSecond: 50,
          minPromptLeaseMillis: 1000,
          maxPromptLeaseMillis: 10000,
          maxPendingPermissions: 4,
          maxWorkspaceDeltaBytes: 100000,
        },
        workspaceGovernance: {
          mode: 'strict-governed',
          trusted: false,
          orkaOwnedWorkspaceDeltas: true,
          promptScopedBrokerAuthorization: true,
          noDirectSCMPublication: true,
          orkaOwnedCleanRoomPublication: true,
          exactInstanceFencing: true,
          duplicateSafeMutations: true,
          cancellationSettlement: true,
        },
      },
    },
  }
}

function agentRuntimeV1(name: string) {
  return {
    metadata: { name, namespace: 'default', uid: `${name}-uid` },
    spec: {
      contractVersion: 'orka.harness.v1',
      deployment: { mode: 'external-endpoint', endpoint: 'https://legacy-runtime.example.test' },
      clientAuth: { bearerTokenSecretRef: { name: 'legacy-auth', key: 'token' } },
    },
    status: {
      ready: true,
      observedCapabilities: {
        protocolVersion: 'orka.harness.v1',
        runtimeName: 'agentkit',
        runtimeVersion: '1.4.2',
      },
    },
  }
}

function unclassifiedAgentRuntime(name: string) {
  return {
    metadata: { name, namespace: 'default', uid: `${name}-uid` },
    spec: {
      deployment: { mode: 'external-endpoint', endpoint: 'https://unclassified.example.test' },
      clientAuth: {
        controllerBearerTokenSecretRef: { name: 'auth', key: 'controller-token' },
        operationCapabilitySecretRef: { name: 'auth', key: 'capability-secret' },
      },
    },
    status: { ready: false, message: 'AgentRuntime contractVersion is unclassified' },
  }
}

beforeEach(() => {
  useUIStore.setState({ namespace: 'default', sidebarCollapsed: false, theme: 'light' })
})

describe('useRuntimePools', () => {
  it('follows continue tokens and aggregates every page', async () => {
    const seen: Array<{ namespace: string | null; limit: string | null; token: string | null }> = []
    server.use(
      http.get('/api/v1/runtime-pools', ({ request }) => {
        const url = new URL(request.url)
        const token = url.searchParams.get('continue')
        seen.push({
          namespace: url.searchParams.get('namespace'),
          limit: url.searchParams.get('limit'),
          token,
        })
        if (!token) {
          return HttpResponse.json({
            items: [runtimePool('pool-first')],
            metadata: { continue: 'pool-next' },
          })
        }
        return HttpResponse.json({ items: [runtimePool('pool-second')], metadata: {} })
      }),
    )

    const { result } = renderHook(() => useRuntimePools(false), { wrapper: createWrapper() })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data?.items.map((item) => item.metadata.name)).toEqual([
      'pool-first',
      'pool-second',
    ])
    expect(seen).toEqual([
      { namespace: 'default', limit: '100', token: null },
      { namespace: 'default', limit: '100', token: 'pool-next' },
    ])
  })

  it('fails instead of following a repeated continuation cursor forever', async () => {
    let calls = 0
    server.use(
      http.get('/api/v1/runtime-pools', () => {
        calls += 1
        return HttpResponse.json({ items: [], metadata: { continue: 'same-token' } })
      }),
    )

    const { result } = renderHook(() => useRuntimePools(false), { wrapper: createWrapper() })

    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(calls).toBe(2)
    expect(result.current.error).toEqual(
      new Error('runtime list pagination repeated continuation cursor for /runtime-pools'),
    )
  })
})

describe('useAgentRuntimes', () => {
  it('follows continue tokens and aggregates every page', async () => {
    const seen: Array<{ namespace: string | null; limit: string | null; token: string | null }> = []
    server.use(
      http.get('/api/v1/agent-runtimes', ({ request }) => {
        const url = new URL(request.url)
        const token = url.searchParams.get('continue')
        seen.push({
          namespace: url.searchParams.get('namespace'),
          limit: url.searchParams.get('limit'),
          token,
        })
        if (!token) {
          return HttpResponse.json({
            items: [unclassifiedAgentRuntime('runtime-unclassified'), agentRuntimeV1('runtime-first')],
            metadata: { continue: 'runtime-next' },
          })
        }
        return HttpResponse.json({ items: [agentRuntime('runtime-second')], metadata: {} })
      }),
    )

    const { result } = renderHook(() => useAgentRuntimes(false), { wrapper: createWrapper() })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data?.items.map((item) => item.metadata.name)).toEqual([
      'runtime-unclassified',
      'runtime-first',
      'runtime-second',
    ])
    expect(result.current.data?.items.map((item) => item.spec.contractVersion)).toEqual([
      undefined,
      'orka.harness.v1',
      'orka.harness.v2',
    ])
    expect(result.current.data?.items.at(-1)?.spec).toMatchObject({
      contractVersion: 'orka.harness.v2',
      capabilities: { supportsDrain: false },
    })
    expect(seen).toEqual([
      { namespace: 'default', limit: '100', token: null },
      { namespace: 'default', limit: '100', token: 'runtime-next' },
    ])
  })

  it('fails instead of following a repeated continuation cursor forever', async () => {
    let calls = 0
    server.use(
      http.get('/api/v1/agent-runtimes', () => {
        calls += 1
        return HttpResponse.json({ items: [], metadata: { continue: 'same-token' } })
      }),
    )

    const { result } = renderHook(() => useAgentRuntimes(false), { wrapper: createWrapper() })

    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(calls).toBe(2)
    expect(result.current.error).toEqual(
      new Error('runtime list pagination repeated continuation cursor for /agent-runtimes'),
    )
  })
})
