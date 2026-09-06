import { describe, it, expect } from 'vitest'
import {
  modelConfigSchema,
  toolRefSchema,
  builtInAgentRuntimeTypeSchema,
  builtInAgentRuntimeSchema,
  externalAgentRuntimeSchema,
  agentRuntimeSchema,
  agentSpecSchema,
  agentStatusSchema,
  agentSchema,
} from './agent'
import type { Agent, AgentSpec, AgentStatus } from './agent'

describe('modelConfigSchema', () => {
  it('parses valid data with all fields', () => {
    const data = { provider: 'openai', name: 'gpt-4', temperature: 0.7, contextWindow: 32768, maxTokens: 1000 }
    expect(modelConfigSchema.parse(data)).toEqual(data)
  })

  it('parses empty object and rejects invalid numeric fields', () => {
    expect(modelConfigSchema.parse({})).toEqual({})
    expect(() => modelConfigSchema.parse({ temperature: 'warm' })).toThrow()
    expect(() => modelConfigSchema.parse({ contextWindow: '32768' })).toThrow()
    expect(() => modelConfigSchema.parse({ maxTokens: '1000' })).toThrow()
  })
})

describe('toolRefSchema', () => {
  it('parses valid data and rejects a missing name', () => {
    expect(toolRefSchema.parse({ name: 'search-tool', enabled: true })).toEqual({ name: 'search-tool', enabled: true })
    expect(() => toolRefSchema.parse({ enabled: true })).toThrow()
  })
})

describe('agentRuntimeSchema', () => {
  it('accepts reviewed built-in ACP profiles', () => {
    for (const type of ['claude', 'codex', 'copilot', 'opencode']) {
      expect(builtInAgentRuntimeTypeSchema.parse(type)).toBe(type)
      expect(builtInAgentRuntimeSchema.parse({ type })).toEqual({ type })
    }
  })

  it('accepts a v2 AgentRuntime reference', () => {
    const runtime = { runtimeRef: { name: 'external-codex' }, defaultMaxTurns: 20 }
    expect(externalAgentRuntimeSchema.parse(runtime)).toEqual(runtime)
    expect(agentRuntimeSchema.parse(runtime)).toEqual(runtime)
  })

  it('accepts supported runtime defaults', () => {
    const runtime = {
      type: 'opencode',
      defaultMaxTurns: 50,
      defaultAllowedTools: ['Read', 'Write', 'Edit', 'Bash', 'Glob', 'Grep'],
      defaultAllowBash: true,
      defaultReasoningEffort: 'max',
    }
    expect(builtInAgentRuntimeSchema.parse(runtime)).toEqual(runtime)
    expect(agentRuntimeSchema.parse(runtime)).toEqual(runtime)
  })

  it('rejects removed, ambiguous, or malformed runtime selectors', () => {
    expect(() => agentRuntimeSchema.parse({ type: 'unknown' })).toThrow()
    expect(() => agentRuntimeSchema.parse({})).toThrow()
    expect(() => agentRuntimeSchema.parse({ type: 'codex', runtimeRef: { name: 'external' } })).toThrow()
    expect(() => agentRuntimeSchema.parse({ type: 'codex', defaultMaxTurns: 'many' })).toThrow()
    expect(() => agentRuntimeSchema.parse({ type: 'claude', defaultAllowBash: 'yes' })).toThrow()
    expect(() => agentRuntimeSchema.parse({ type: 'claude', defaultReasoningEffort: 'maximum' })).toThrow()
  })
})

describe('agentSpecSchema', () => {
  it('parses valid data with all native AI fields', () => {
    const data = {
      providerRef: { name: 'openai', namespace: 'default' },
      model: { provider: 'openai', name: 'gpt-4', temperature: 0.7 },
      systemPrompt: { inline: 'You are helpful', configMapRef: { name: 'prompt-cm', key: 'prompt.txt' } },
      tools: [{ name: 'search', enabled: true }],
      skills: [{ configMapRef: { name: 'skill-cm', key: 'skill.md' } }],
      resources: { limits: { memory: '256Mi' } },
      secretRef: { name: 'api-keys' },
      session: { maxMessages: 100 },
      coordination: {
        enabled: true,
        allowedAgents: [{ name: 'helper', namespace: 'default' }],
        maxConcurrentChildren: 5,
        maxDepth: 3,
      },
    }
    expect(agentSpecSchema.parse(data)).toEqual(data)
  })

  it('parses built-in and external runtime agents', () => {
    expect(agentSpecSchema.parse({ runtime: { type: 'codex' } })).toEqual({ runtime: { type: 'codex' } })
    expect(agentSpecSchema.parse({ model: { maxTokens: 0 }, runtime: { type: 'codex' } })).toEqual({
      model: { maxTokens: 0 },
      runtime: { type: 'codex' },
    })
    expect(agentSpecSchema.parse({
      model: { name: 'openai/gpt-5.4', contextWindow: 32768, maxTokens: 4096 },
      runtime: { type: 'opencode' },
    })).toEqual({
      model: { name: 'openai/gpt-5.4', contextWindow: 32768, maxTokens: 4096 },
      runtime: { type: 'opencode' },
    })
    expect(agentSpecSchema.parse({ runtime: { runtimeRef: { name: 'external' } } })).toEqual({ runtime: { runtimeRef: { name: 'external' } } })
  })

  it('parses empty object and rejects malformed nested fields', () => {
    expect(agentSpecSchema.parse({})).toEqual({})
    expect(() => agentSpecSchema.parse({ coordination: {} })).toThrow()
    expect(() => agentSpecSchema.parse({ systemPrompt: { configMapRef: { name: 'x' } } })).toThrow()
    expect(() => agentSpecSchema.parse({ runtime: { type: 'opencode' } })).toThrow()
    expect(() => agentSpecSchema.parse({ model: { name: '   ', contextWindow: 32768, maxTokens: 4096 }, runtime: { type: 'opencode' } })).toThrow()
    expect(() => agentSpecSchema.parse({ model: { name: 'openai/gpt-5.4', maxTokens: 4096 }, runtime: { type: 'opencode' } })).toThrow()
    expect(() => agentSpecSchema.parse({ model: { name: 'openai/gpt-5.4', contextWindow: 32768 }, runtime: { type: 'opencode' } })).toThrow()
    expect(() => agentSpecSchema.parse({ model: { name: 'openai/gpt-5.4', contextWindow: 32768, maxTokens: 0 }, runtime: { type: 'opencode' } })).toThrow('OpenCode requires a positive integer model.maxTokens')
    expect(() => agentSpecSchema.parse({ model: { name: 'openai/gpt-5.4', contextWindow: 32768, maxTokens: 1.5 }, runtime: { type: 'opencode' } })).toThrow('OpenCode requires a positive integer model.maxTokens')
    expect(() => agentSpecSchema.parse({ model: { name: 'openai/gpt-5.4', contextWindow: 4096, maxTokens: 4096 }, runtime: { type: 'opencode' } })).toThrow()
    expect(() => agentSpecSchema.parse({
      model: { name: 'openai/gpt-5.4', contextWindow: 32768, maxTokens: 4096 },
      systemPrompt: { inline: 'You write code' },
      runtime: { type: 'opencode' },
    })).toThrow('OpenCode does not support Agent systemPrompt')
    expect(() => agentSpecSchema.parse({
      model: { name: 'openai/gpt-5.4', contextWindow: 32768, maxTokens: 4096 },
      systemPrompt: { configMapRef: { name: 'prompt', key: 'system.txt' } },
      runtime: { type: 'opencode' },
    })).toThrow('OpenCode does not support Agent systemPrompt')
  })
})

describe('agentStatusSchema', () => {
  it('parses valid status and rejects missing activeTasks', () => {
    const data = {
      activeTasks: 3,
      lastUsed: '2026-07-24T00:00:00Z',
      conditions: [{ type: 'Ready', status: 'True', reason: 'Configured' }],
    }
    expect(agentStatusSchema.parse(data)).toEqual(data)
    expect(() => agentStatusSchema.parse({})).toThrow()
  })
})

describe('agentSchema', () => {
  it('parses full and minimal agents', () => {
    const full = {
      apiVersion: 'core.orka.ai/v1alpha1',
      kind: 'Agent',
      metadata: {
        name: 'my-agent',
        namespace: 'default',
        uid: 'abc-123',
        creationTimestamp: '2026-07-24T00:00:00Z',
        labels: { app: 'test' },
      },
      spec: { runtime: { type: 'claude' } },
      status: { activeTasks: 1 },
    }
    expect(agentSchema.parse(full)).toEqual(full)
    expect(agentSchema.parse({ metadata: { name: 'minimal' }, spec: {} })).toEqual({ metadata: { name: 'minimal' }, spec: {} })
  })

  it('rejects malformed resources', () => {
    expect(() => agentSchema.parse({ spec: {} })).toThrow()
    expect(() => agentSchema.parse({ metadata: { name: 'x' } })).toThrow()
  })
})

describe('exported types', () => {
  it('match their schemas', () => {
    const agent: Agent = { metadata: { name: 'test' }, spec: {} }
    const spec: AgentSpec = { runtime: { type: 'codex' } }
    const status: AgentStatus = { activeTasks: 0 }
    expect(agentSchema.parse(agent)).toBeDefined()
    expect(agentSpecSchema.parse(spec)).toBeDefined()
    expect(agentStatusSchema.parse(status)).toBeDefined()
  })
})
