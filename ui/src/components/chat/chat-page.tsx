import { useEffect } from 'react'
import { ChatMessageList } from './chat-message-list'
import { ChatInput } from './chat-input'
import { useSendMessage, useChatConfig } from '@/hooks/use-chat'
import { useProviderList } from '@/hooks/use-providers'
import { useChatStore } from '@/stores/chat'
import { useUIStore } from '@/stores/ui'
import { isForbiddenError } from '@/lib/api-client'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Plus } from 'lucide-react'

const SERVER_DEFAULT = '__default__'

export function ChatPage() {
  const sendMessage = useSendMessage()
  const { data: config } = useChatConfig()
  const { data: providersData, error: providersError } = useProviderList()
  const namespace = useUIStore((s) => s.namespace)
  const {
    currentSessionId,
    newSession,
    messages,
    provider,
    model,
    setProvider,
    setModel,
    selectNamespace,
    activeNamespace,
    isStreaming,
  } = useChatStore()
  // Providers are namespace-scoped, so the persisted picker choice is too.
  useEffect(() => {
    selectNamespace(namespace)
  }, [namespace, selectNamespace])
  const providersForbidden = isForbiddenError(providersError)
  // React Query keeps the last successful data alongside a failed refetch; a
  // 403 must not keep rendering Provider names the current token may not list.
  const providers = providersForbidden ? [] : (providersData?.items ?? [])
  // A persisted pick can outlive its Provider (deleted, or listing now
  // forbidden). Sending that stale name would fail provider resolution on
  // every turn, so fall back to the server default once the list has
  // answered and does not contain it.
  // Gated on the store having switched to the rendered namespace: during a
  // namespace change this render still holds the previous namespace's pick
  // while the list already belongs to the new one, and clearing then would
  // erase the new namespace's valid selection right after selectNamespace
  // restores it.
  const providerGone =
    activeNamespace === namespace &&
    provider !== '' &&
    (providersForbidden || (providersData !== undefined && !providers.some((p) => p.name === provider)))
  useEffect(() => {
    if (!providerGone) return
    setProvider('')
    setModel('')
  }, [providerGone, setProvider, setModel])
  // Without a server default provider an empty pick cannot send at all, so
  // when the namespace lists exactly one ready Provider it is preselected;
  // any other shape leaves the picker empty and disables sending with a hint.
  const readyProviders = providers.filter((p) => p.ready !== false)
  const soleReadyProvider =
    activeNamespace === namespace &&
    provider === '' &&
    config !== undefined &&
    !config.provider &&
    readyProviders.length === 1 &&
    providers.length === 1
      ? readyProviders[0]
      : undefined
  useEffect(() => {
    if (!soleReadyProvider) return
    setProvider(soleReadyProvider.name)
    setModel(soleReadyProvider.defaultModel ?? '')
  }, [soleReadyProvider, setProvider, setModel])
  const hasServerDefault = Boolean(config?.provider)
  const noProviderSelected = !provider && config !== undefined && !hasServerDefault
  const providersErrorMessage = providersError
    ? providersForbidden
      ? hasServerDefault
        ? `Not authorized to list Providers in ${namespace}; only the server default is available.`
        : `Not authorized to list Providers in ${namespace}; an explicit Provider is required to send.`
      : `Providers unavailable: ${providersError instanceof Error ? providersError.message : 'unknown error'}`
    : undefined
  const serverDefaultLabel = config?.provider
    ? `Server default (${config.provider}${config.model ? ` / ${config.model}` : ''})`
    : 'Server default'
  const effectiveModel = model || (provider ? providers.find((p) => p.name === provider)?.defaultModel : config?.model)
  // A model override only reaches the server alongside a provider; without a
  // Provider CRD pick it is pinned to the server's configured default provider,
  // and when there is none to pin to the input is disabled.
  const modelNeedsProvider = !provider && !config?.provider

  const handleProviderChange = (value: string) => {
    const next = value === SERVER_DEFAULT ? '' : value
    setProvider(next)
    // Prefill the model with the provider's default so a plain pick just works;
    // the user can still overwrite it.
    setModel(next ? providers.find((p) => p.name === next)?.defaultModel ?? '' : '')
  }

  return (
    <div className="flex h-full flex-col -m-4 md:-m-6">
      {/* Header */}
      <div className="border-b border-border bg-card px-4 py-2">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <h1 className="text-sm font-semibold">Chat</h1>
            {currentSessionId && (
              <Badge variant="secondary" className="font-mono text-[10px]">
                {currentSessionId}
              </Badge>
            )}
            {effectiveModel && (
              <Badge variant="outline" className="text-[10px]">
                {effectiveModel}
              </Badge>
            )}
          </div>
          <div className="flex flex-wrap items-center justify-end gap-2">
            <Select value={provider || (hasServerDefault ? SERVER_DEFAULT : '')} onValueChange={handleProviderChange} disabled={isStreaming}>
              <SelectTrigger className="h-7 w-auto min-w-40 text-xs" aria-label="Chat provider">
                <SelectValue placeholder="Provider" />
              </SelectTrigger>
              <SelectContent>
                {hasServerDefault && <SelectItem value={SERVER_DEFAULT}>{serverDefaultLabel}</SelectItem>}
                {providers.map((p) => (
                  <SelectItem key={p.name} value={p.name}>
                    {p.name}
                    {p.type && ` (${p.type})`}
                    {p.ready === false && ' — not ready'}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Input
              aria-label="Chat model"
              aria-describedby={modelNeedsProvider ? 'chat-model-hint' : undefined}
              value={model}
              onChange={(e) => setModel(e.target.value)}
              placeholder={provider ? providers.find((p) => p.name === provider)?.defaultModel || 'model' : config?.model || 'model (server default)'}
              disabled={isStreaming || modelNeedsProvider}
              className="h-7 w-48 text-xs"
            />
            {messages.length > 0 && (
              <Button variant="ghost" size="sm" onClick={newSession} className="h-7 text-xs">
                <Plus className="mr-1 h-3 w-3" /> New Chat
              </Button>
            )}
          </div>
        </div>
        {(providersErrorMessage || modelNeedsProvider) && (
          <div className="mt-1 flex flex-wrap justify-end gap-x-4 text-[11px]">
            {providersErrorMessage && (
              <p role="alert" className={providersForbidden ? 'text-muted-foreground' : 'text-destructive'}>
                {providersErrorMessage}
              </p>
            )}
            {modelNeedsProvider && (
              <p id="chat-model-hint" className="text-muted-foreground">
                Choose a provider to override the model.
              </p>
            )}
          </div>
        )}
      </div>

      {/* Messages */}
      <ChatMessageList />

      {/* Input */}
      <ChatInput onSend={sendMessage} disabled={noProviderSelected} disabledHint={noProviderSelected ? 'Choose a provider before sending.' : undefined} />
    </div>
  )
}
