import { z } from 'zod'
import { conditionSchema } from './task'

export const modelConfigSchema = z.object({
  provider: z.string().optional(),
  name: z.string().optional(),
  temperature: z.number().optional(),
  contextWindow: z.number().int().positive().optional(),
  maxTokens: z.number().optional(),
})

export const toolRefSchema = z.object({
  name: z.string(),
  enabled: z.boolean().optional(),
})

const agentRuntimeDefaultsSchema = {
  defaultMaxTurns: z.number().optional(),
  defaultAllowedTools: z.array(z.string()).optional(),
  defaultAllowBash: z.boolean().optional(),
  defaultReasoningEffort: z.enum(['low', 'medium', 'high', 'xhigh', 'max']).optional(),
}

export const builtInAgentRuntimeTypes = ['claude', 'codex', 'copilot', 'opencode'] as const

export const builtInAgentRuntimeTypeSchema = z.enum(builtInAgentRuntimeTypes)

export const builtInAgentRuntimeSchema = z.object({
  type: builtInAgentRuntimeTypeSchema,
  ...agentRuntimeDefaultsSchema,
}).strict()

export const externalAgentRuntimeSchema = z.object({
  runtimeRef: z.object({
    name: z.string(),
  }),
  ...agentRuntimeDefaultsSchema,
}).strict()

export const agentRuntimeSchema = z.union([builtInAgentRuntimeSchema, externalAgentRuntimeSchema])

export const agentSpecSchema = z.object({
  providerRef: z.object({ name: z.string(), namespace: z.string().optional() }).optional(),
  model: modelConfigSchema.optional(),
  systemPrompt: z.object({
    inline: z.string().optional(),
    configMapRef: z.object({ name: z.string(), key: z.string() }).optional(),
  }).optional(),
  tools: z.array(toolRefSchema).optional(),
  skills: z.array(z.object({ configMapRef: z.object({ name: z.string(), key: z.string().optional() }) })).optional(),
  resources: z.any().optional(),
  secretRef: z.object({ name: z.string() }).optional(),
  session: z.object({
    maxMessages: z.number().optional(),
  }).optional(),
  coordination: z.object({
    enabled: z.boolean(),
    allowedAgents: z.array(z.object({ name: z.string(), namespace: z.string().optional() })).optional(),
    maxConcurrentChildren: z.number().optional(),
    maxDepth: z.number().optional(),
  }).optional(),
  runtime: agentRuntimeSchema.optional(),
}).superRefine((spec, ctx) => {
  if (spec.runtime && 'type' in spec.runtime && spec.runtime.type === 'opencode') {
    if (spec.systemPrompt?.inline?.trim() || spec.systemPrompt?.configMapRef) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'OpenCode does not support Agent systemPrompt; use Task prompts instead',
        path: ['systemPrompt'],
      })
    }
    if (!spec.model?.name?.trim()) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'OpenCode requires model.name',
        path: ['model', 'name'],
      })
    }
    if (spec.model?.contextWindow === undefined) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'OpenCode requires model.contextWindow',
        path: ['model', 'contextWindow'],
      })
    }
    if (spec.model?.maxTokens === undefined || !Number.isInteger(spec.model.maxTokens) || spec.model.maxTokens <= 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'OpenCode requires a positive integer model.maxTokens',
        path: ['model', 'maxTokens'],
      })
    }
    if (
      spec.model?.contextWindow !== undefined
      && spec.model?.maxTokens !== undefined
      && spec.model.contextWindow <= spec.model.maxTokens
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'OpenCode model.contextWindow must exceed model.maxTokens',
        path: ['model', 'contextWindow'],
      })
    }
  }
})

export const agentStatusSchema = z.object({
  activeTasks: z.number(),
  lastUsed: z.string().optional(),
  conditions: z.array(conditionSchema).optional(),
})

export const agentSchema = z.object({
  apiVersion: z.string().optional(),
  kind: z.string().optional(),
  metadata: z.object({
    name: z.string(),
    namespace: z.string().optional(),
    uid: z.string().optional(),
    creationTimestamp: z.string().optional(),
    labels: z.record(z.string()).optional(),
    annotations: z.record(z.string()).optional(),
  }),
  spec: agentSpecSchema,
  status: agentStatusSchema.optional(),
})

export type BuiltInAgentRuntimeType = z.infer<typeof builtInAgentRuntimeTypeSchema>
export type Agent = z.infer<typeof agentSchema>
export type AgentSpec = z.infer<typeof agentSpecSchema>
export type AgentStatus = z.infer<typeof agentStatusSchema>
