import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@/test/test-utils'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { server } from '@/test/mocks/server'
import { useAuthStore } from '@/stores/auth'
import { useUIStore } from '@/stores/ui'
import { RuntimeRegistry } from './runtime-registry'

vi.mock('zustand/middleware', () => ({ persist: (fn: unknown) => fn }))

const digest = `sha256:${'a'.repeat(64)}`

function pool() {
  return {
    metadata: { name: 'codex-read', namespace: 'default', uid: 'pool-uid' },
    spec: {
      trustDomain: { namespace: 'default', identity: 'default/operators' },
      desiredReplicas: 1,
      runtime: {
        image: 'docker.io/sozercan/orka-acp-codex@sha256:abc',
        profile: {
          protocolVersion: 'orka.harness.v2', digest, digestSchemaVersion: 'v1', acpProfile: 'schema-v1.20.0',
          adapterDigests: { 'codex-acp': digest }, providerKind: 'openai', model: 'gpt-5',
          agentConfigurationDigest: digest, toolPolicyDigest: digest, approvalPolicyDigest: digest,
          mcpConfigurationDigest: digest, workspaceIntent: 'read', proxyCredentialRole: 'provider',
          proxyCredentialScope: 'codex', resourceClass: 'standard',
        },
      },
    },
    status: {
      lifecycle: 'Serving', admissionState: 'Accepting', desiredReplicas: 1, currentReplicas: 1,
      capacity: { maxResidentSessions: 10, maxRunningPrompts: 4, residentSessions: 3, runningPrompts: 2, queuedTasks: 1 },
    },
  }
}

function externalRuntime() {
  return {
    metadata: { name: 'external-codex', namespace: 'default', uid: 'runtime-uid' },
    spec: {
      contractVersion: 'orka.harness.v2',
      deployment: { mode: 'external-endpoint', endpoint: 'https://runtime.example.test' },
      clientAuth: {
        controllerBearerTokenSecretRef: { name: 'auth', key: 'controller-token' },
        operationCapabilitySecretRef: { name: 'auth', key: 'capability-secret' },
      },
      capabilities: {
        runtimeInstanceID: 'external-1',
        profile: {
          digest, digestSchemaVersion: 1, acpProfile: 'schema-v1.20.0', adapterName: 'codex-acp', adapterDigest: digest,
          providerKind: 'openai', model: 'gpt-5', agentConfigurationDigest: digest, toolPolicyDigest: digest,
          approvalPolicyDigest: digest, mcpConfigurationDigest: digest, workspaceIntent: 'read',
          proxyCredentialRole: 'provider', proxyCredentialScope: 'codex', resourceClass: 'standard',
        },
        mcpPolicy: {
          allowedTools: ['web_search'], disallowedTools: [], allowBash: false, approvalRequiredTools: [],
        },
        limits: {
          maxResidentSessions: 10, maxConcurrentPrompts: 4, maxRequestBytes: 1000, maxEventLineBytes: 1000,
          maxTerminalResultBytes: 1000, maxBufferedEvents: 100, maxUpdateEventsPerSecond: 50,
          minPromptLeaseMillis: 1000, maxPromptLeaseMillis: 10000, maxPendingPermissions: 4, maxWorkspaceDeltaBytes: 100000,
        },
        supportsDrain: true,
        supportsPublicationFinalization: true,
        workspaceGovernance: {
          mode: 'strict-governed', trusted: false, orkaOwnedWorkspaceDeltas: true,
          promptScopedBrokerAuthorization: true, noDirectSCMPublication: true,
          orkaOwnedCleanRoomPublication: true, exactInstanceFencing: true,
          duplicateSafeMutations: true, cancellationSettlement: true,
        },
      },
    },
    status: { ready: true, lastValidated: '2026-07-24T00:00:00Z' },
  }
}

function externalV1Runtime() {
  return {
    metadata: { name: 'external-v1', namespace: 'default', uid: 'runtime-v1-uid' },
    spec: {
      contractVersion: 'orka.harness.v1',
      deployment: { mode: 'external-endpoint', endpoint: 'https://legacy-runtime.example.test' },
      clientAuth: { bearerTokenSecretRef: { name: 'legacy-auth', key: 'token' } },
      capabilities: {
        toolExecutionModes: ['observed', 'brokered'],
        brokeredToolClasses: ['read'],
        supportsCancel: true,
        supportsRuntimeSessions: true,
        supportsContinuation: true,
        supportsArtifacts: false,
      },
    },
    status: {
      ready: true,
      lastValidated: '2026-07-24T00:00:00Z',
      message: 'authenticated orka.harness.v1 conformance passed',
      observedCapabilities: {
        protocolVersion: 'orka.harness.v1',
        transport: 'http+sse',
        runtimeName: 'agentkit',
        runtimeVersion: '1.4.2',
        providerKind: 'generic',
        toolExecutionModes: ['observed', 'brokered'],
        brokeredToolClasses: ['read', 'coordination'],
        supportsCancel: true,
        supportsRuntimeSessions: true,
        supportsContinuation: true,
        supportsArtifacts: false,
        maxConcurrentTurns: 4,
        maxTurnSeconds: 1800,
        maxOutputBytes: 1048576,
      },
    },
  }
}

function unclassifiedRuntime() {
  return {
    metadata: { name: 'legacy-unclassified', namespace: 'default', uid: 'runtime-unclassified-uid' },
    spec: {
      deployment: { mode: 'external-endpoint', endpoint: 'https://unclassified.example.test' },
      clientAuth: {
        controllerBearerTokenSecretRef: { name: 'auth', key: 'controller-token' },
        operationCapabilitySecretRef: { name: 'auth', key: 'capability-secret' },
      },
    },
    status: {
      ready: false,
      message: 'AgentRuntime contractVersion is unclassified; explicit classification is required',
    },
  }
}

describe('RuntimeRegistry', () => {
  beforeEach(() => {
    useUIStore.setState({ namespace: 'default', sidebarCollapsed: false, theme: 'light' })
    useAuthStore.setState({ token: 'test-token' })
    server.use(
      http.get('/api/v1/runtime-pools', () => HttpResponse.json({ items: [pool()], metadata: {} })),
      http.get('/api/v1/agent-runtimes', () => HttpResponse.json({ items: [externalRuntime()], metadata: {} })),
    )
  })

  it('renders a readable not-authorized state for a 403 instead of raw JSON', async () => {
    server.use(
      http.get('/api/v1/runtime-pools', () =>
        HttpResponse.json({ error: { code: 403, message: 'not authorized' } }, { status: 403 }),
      ),
    )
    render(<RuntimeRegistry />)
    await waitFor(() => expect(screen.getByText('Not authorized to view RuntimePool resources')).toBeInTheDocument())
    expect(screen.getByText(/lacks read permission for RuntimePool resources \(not authorized\)/)).toBeInTheDocument()
    expect(screen.queryByText(/"code":403/)).not.toBeInTheDocument()
    expect(screen.queryByText('Could not load runtimepool')).not.toBeInTheDocument()
  })

  it('renders pool admission, capacity, and profile identity', async () => {
    render(<RuntimeRegistry />)
    await waitFor(() => expect(screen.getByText('codex-read')).toBeInTheDocument())
    expect(screen.getByText('Serving')).toBeInTheDocument()
    expect(screen.getByText('Accepting')).toBeInTheDocument()
    expect(screen.getByText('3 / 10')).toBeInTheDocument()
    expect(screen.getByText('2 / 4')).toBeInTheDocument()
    expect(screen.getByText('schema-v1.20.0')).toBeInTheDocument()
  })

  it('renders the external v2 governance surface without v1 fields', async () => {
    const user = userEvent.setup()
    render(<RuntimeRegistry />)
    await user.click(screen.getByRole('tab', { name: 'External runtimes' }))
    await waitFor(() => expect(screen.getByText('external-codex')).toBeInTheDocument())
    expect(screen.getByText('orka.harness.v2')).toBeInTheDocument()
    expect(screen.getByText('strict-governed')).toBeInTheDocument()
    expect(screen.getByText('Exact-instance fencing')).toBeInTheDocument()
    expect(screen.queryByText(/continuation/i)).not.toBeInTheDocument()
  })

  it('renders the external v1 capability surface without v2 profile fields', async () => {
    server.use(
      http.get('/api/v1/agent-runtimes', () => HttpResponse.json({ items: [externalV1Runtime()], metadata: {} })),
    )
    const user = userEvent.setup()
    render(<RuntimeRegistry />)
    await user.click(screen.getByRole('tab', { name: 'External runtimes' }))

    await waitFor(() => expect(screen.getByText('external-v1')).toBeInTheDocument())
    expect(screen.getByText('orka.harness.v1')).toBeInTheDocument()
    expect(screen.getByText('agentkit')).toBeInTheDocument()
    expect(screen.getByText('1.4.2')).toBeInTheDocument()
    expect(screen.getByText('observed, brokered')).toBeInTheDocument()
    expect(screen.getByText('read, coordination')).toBeInTheDocument()
    expect(screen.getByText('Cancel: Yes')).toBeInTheDocument()
    expect(screen.getByText('Runtime sessions: Yes')).toBeInTheDocument()
    expect(screen.getByText('Continuation: Yes')).toBeInTheDocument()
    expect(screen.getByText('Artifacts: No')).toBeInTheDocument()
    expect(screen.getByText('1800s')).toBeInTheDocument()
    expect(screen.getByText('1048576 bytes')).toBeInTheDocument()
    expect(screen.getByText('authenticated orka.harness.v1 conformance passed')).toBeInTheDocument()
    expect(screen.queryByText('ACP profile')).not.toBeInTheDocument()
    expect(screen.queryByText('Profile digest')).not.toBeInTheDocument()
    expect(screen.queryByText('strict-governed')).not.toBeInTheDocument()
  })

  it('renders an unclassified runtime as non-executable without inferring v1 or v2', async () => {
    server.use(
      http.get('/api/v1/agent-runtimes', () => HttpResponse.json({ items: [unclassifiedRuntime()], metadata: {} })),
    )
    const user = userEvent.setup()
    render(<RuntimeRegistry />)
    await user.click(screen.getByRole('tab', { name: 'External runtimes' }))

    await waitFor(() => expect(screen.getByText('legacy-unclassified')).toBeInTheDocument())
    expect(screen.getByText('Unclassified')).toBeInTheDocument()
    expect(screen.getByText('Not ready')).toBeInTheDocument()
    expect(screen.getByText('https://unclassified.example.test')).toBeInTheDocument()
    expect(screen.getByText(/explicit classification is required/)).toBeInTheDocument()
    expect(screen.queryByText('Execution')).not.toBeInTheDocument()
    expect(screen.queryByText('ACP profile')).not.toBeInTheDocument()
    expect(screen.queryByText('Tool modes')).not.toBeInTheDocument()
  })
})
