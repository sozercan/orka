import { create } from 'zustand'
import { persist, type PersistStorage, type StorageValue } from 'zustand/middleware'
import type { ChatMessage, ChatUsage } from '@/schemas/chat'

interface ChatState {
  messages: ChatMessage[]
  currentSessionId: string | null
  isStreaming: boolean
  // Provider CRD name and model sent with each turn; empty means "server default".
  // These mirror `selections[activeNamespace]`: Providers are namespace-scoped,
  // so the persisted picker choice is keyed by namespace and swapped in with
  // `selectNamespace` whenever the active namespace changes.
  provider: string
  model: string
  activeNamespace: string
  selections: Record<string, ChatSelection>
  // Incremented whenever the transcript is reset (namespace switch, new
  // session) so an in-flight turn can tell it no longer owns the transcript.
  turnEpoch: number
  // Actions
  setProvider: (provider: string) => void
  setModel: (model: string) => void
  selectNamespace: (namespace: string) => void
  addMessage: (message: ChatMessage) => void
  updateLastAssistantMessage: (content: string) => void
  setSessionId: (id: string) => void
  setStreaming: (streaming: boolean) => void
  newSession: () => void
  resetForIdentityChange: () => void
  setUsageOnLastAssistant: (usage: ChatUsage, tasksCreatedNames?: string[]) => void
}

// Browser localStorage when available; an in-memory fallback otherwise (Node
// exposes a bare `localStorage` global that is unusable without a backing file,
// and some jsdom setups have none). The fallback keeps the picker working for
// the page lifetime without persisting across reloads.
function rawStorage(): Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> {
  try {
    if (typeof window !== 'undefined' && window.localStorage) return window.localStorage
  } catch {
    // Access can throw (opaque origin, blocked site data); fall through.
  }
  const memory = new Map<string, string>()
  return {
    getItem: (key) => memory.get(key) ?? null,
    setItem: (key, value) => {
      memory.set(key, value)
    },
    removeItem: (key) => {
      memory.delete(key)
    },
  }
}

interface ChatSelection {
  provider: string
  model: string
}

type PersistedChatState = Pick<ChatState, 'selections'>

// Hand-rolled JSON storage (instead of createJSONStorage) so test suites that
// stub `zustand/middleware` with only `persist` keep working.
function chatStorage(): PersistStorage<PersistedChatState> {
  const raw = rawStorage()
  return {
    getItem: (name) => {
      const value = raw.getItem(name)
      if (!value) return null
      try {
        return JSON.parse(value) as StorageValue<PersistedChatState>
      } catch {
        return null
      }
    },
    setItem: (name, value) => raw.setItem(name, JSON.stringify(value)),
    removeItem: (name) => raw.removeItem(name),
  }
}

let msgCounter = 0
export function generateMessageId(): string {
  return `msg-${Date.now()}-${++msgCounter}`
}

export const useChatStore = create<ChatState>()(
  persist(
    (set) => ({
  messages: [],
  currentSessionId: null,
  isStreaming: false,
  provider: '',
  model: '',
  activeNamespace: '',
  turnEpoch: 0,
  selections: {},

  setProvider: (provider) =>
    set((state) => ({
      provider,
      selections: { ...state.selections, [state.activeNamespace]: { provider, model: state.model } },
    })),
  setModel: (model) =>
    set((state) => ({
      model,
      selections: { ...state.selections, [state.activeNamespace]: { provider: state.provider, model } },
    })),
  selectNamespace: (namespace) =>
    set((state) => {
      const selection = state.selections[namespace]
      // Chat sessions are server-side namespace-scoped: moving between two
      // namespaces drops the previous transcript and session ID (and orphans
      // any turn still streaming) instead of replaying them against a
      // different namespace. The initial activation keeps whatever is there.
      const switching = state.activeNamespace !== '' && state.activeNamespace !== namespace
      return {
        activeNamespace: namespace,
        provider: selection?.provider ?? '',
        model: selection?.model ?? '',
        ...(switching ? { messages: [], currentSessionId: null, isStreaming: false, turnEpoch: state.turnEpoch + 1 } : {}),
      }
    }),

  addMessage: (message) =>
    set((state) => ({ messages: [...state.messages, message] })),

  updateLastAssistantMessage: (content) =>
    set((state) => {
      const msgs = [...state.messages]
      for (let i = msgs.length - 1; i >= 0; i--) {
        if (msgs[i].role === 'assistant') {
          msgs[i] = { ...msgs[i], content }
          break
        }
      }
      return { messages: msgs }
    }),

  setSessionId: (id) => set({ currentSessionId: id }),
  setStreaming: (streaming) => set({ isStreaming: streaming }),
  newSession: () => set((state) => ({ messages: [], currentSessionId: null, isStreaming: false, turnEpoch: state.turnEpoch + 1 })),

  // Chat state belongs to the identity that created it. On a token change the
  // transcript, session, provider/model choices, and their persisted
  // per-namespace selections are all dropped, and the epoch bump orphans any
  // in-flight turn (aborting its request via the turn's epoch subscription).
  resetForIdentityChange: () =>
    set((state) => ({
      messages: [],
      currentSessionId: null,
      isStreaming: false,
      provider: '',
      model: '',
      selections: {},
      turnEpoch: state.turnEpoch + 1,
    })),

  setUsageOnLastAssistant: (usage, tasksCreatedNames) =>
    set((state) => {
      const msgs = [...state.messages]
      for (let i = msgs.length - 1; i >= 0; i--) {
        if (msgs[i].role === 'assistant') {
          msgs[i] = {
            ...msgs[i],
            usage,
            ...(tasksCreatedNames && tasksCreatedNames.length > 0
              ? { tasksCreatedNames }
              : {}),
          }
          break
        }
      }
      return { messages: msgs }
    }),
    }),
    // Only the per-namespace picker choices survive reloads; transcripts stay
    // per-page. Version 0 persisted one global {provider, model}; it is dropped
    // on upgrade because it cannot be attributed to a namespace.
    {
      name: 'orka-chat',
      version: 1,
      storage: chatStorage(),
      partialize: (state) => ({ selections: state.selections }),
      migrate: (persisted, version) => {
        const state = (persisted ?? {}) as Partial<PersistedChatState>
        return { selections: version >= 1 && state.selections ? state.selections : {} }
      },
    },
  ),
)
