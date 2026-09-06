import { z } from 'zod'

// Chat API request
export const chatRequestSchema = z.object({
  message: z.string(),
  sessionId: z.string().optional(),
  namespace: z.string().optional(),
  provider: z.string().optional(),
  model: z.string().optional(),
  temperature: z.number().optional(),
  maxTokens: z.number().optional(),
  systemPrompt: z.string().optional(),
  agentRef: z.string().optional(),
})

export type ChatRequest = z.infer<typeof chatRequestSchema>

// Chat API JSON response
export const chatUsageSchema = z.object({
  inputTokens: z.number().optional(),
  outputTokens: z.number().optional(),
  llmCalls: z.number().optional(),
  toolCalls: z.number().optional(),
  tasksCreated: z.number().optional(),
  duration: z.string().optional(),
})

export type ChatUsage = z.infer<typeof chatUsageSchema>

export const chatToolCallSchema = z.object({
  name: z.string(),
  args: z.unknown(),
  result: z.unknown().optional(),
})

export type ChatToolCall = z.infer<typeof chatToolCallSchema>

export const chatResponseSchema = z.object({
  sessionId: z.string(),
  message: z.string(),
  toolCalls: z.array(chatToolCallSchema).optional(),
  usage: chatUsageSchema.optional(),
})

export type ChatResponse = z.infer<typeof chatResponseSchema>

// Chat config from GET /chat/config
export const chatConfigSchema = z.object({
  enabled: z.boolean(),
  provider: z.string(),
  model: z.string(),
  maxIterations: z.number(),
  maxDuration: z.string(),
  maxTasksPerTurn: z.number(),
  maxConcurrent: z.number(),
  availableTools: z.array(z.string()),
  // Set for context-token callers, for whom the server refuses the implicit default provider.
  requireExplicitProvider: z.boolean().optional(),
})

export type ChatConfig = z.infer<typeof chatConfigSchema>

// SSE event data types
export interface SSEStatusEvent {
  sessionId: string
  provider: string
  model: string
}

export interface SSEToolCallEvent {
  id: string
  name: string
  args: unknown
}

export interface SSEToolResultEvent {
  id: string
  name: string
  result: unknown
}

export interface SSEMessageEvent {
  content: string
}

export interface SSEDoneEvent {
  usage: ChatUsage
}

// UI message types for the chat store
type ChatMessageRole = 'user' | 'assistant' | 'tool_call' | 'tool_result' | 'status' | 'error'

export interface ChatMessage {
  id: string
  role: ChatMessageRole
  content: string
  timestamp: string
  // Tool call metadata
  toolCallId?: string
  toolName?: string
  toolArgs?: unknown
  toolResult?: unknown
  toolSuccess?: boolean
  // Usage metadata (on assistant messages)
  usage?: ChatUsage
  // Names of tasks created during this turn (for cross-linking chips), when
  // surfaced by the backend/tool stream. Count lives in usage.tasksCreated.
  tasksCreatedNames?: string[]
  // Status metadata
  provider?: string
  model?: string
  sessionId?: string
}
