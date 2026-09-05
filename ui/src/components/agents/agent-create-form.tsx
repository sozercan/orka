import { useState } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { PageHeader } from '@/components/layout/page-header'
import { useCreateAgent } from '@/hooks/use-agents'
import { useSecretNames } from '@/hooks/use-secrets'
import { useUIStore } from '@/stores/ui'
import { toast } from 'sonner'
import {
  OPENCODE_REVIEWED_RUNTIME_DEFAULTS,
  isOpenCodeModelID,
  type BuiltInAgentRuntimeType,
} from '@/lib/agent-runtime'

export function AgentCreateForm() {
  const navigate = useNavigate()
  const createAgent = useCreateAgent()
  const { data: secretsData } = useSecretNames()
  const namespace = useUIStore((s) => s.namespace)

  const [name, setName] = useState('')
  const [mode, setMode] = useState<'ai' | 'runtime'>('ai')

  // Native AI mode fields remain unchanged by the ACP runtime cutover.
  const [provider, setProvider] = useState('')
  const [model, setModel] = useState('')
  const [temperature, setTemperature] = useState('0.7')
  const [contextWindow, setContextWindow] = useState('')
  const [maxTokens, setMaxTokens] = useState('')
  const [secretRef, setSecretRef] = useState('')
  const [systemPrompt, setSystemPrompt] = useState('')

  // ACP runtime mode selects either an Orka-managed profile or a registered v2 runtime.
  const [runtimeSource, setRuntimeSource] = useState<'built-in' | 'external'>('built-in')
  const [runtimeType, setRuntimeType] = useState<BuiltInAgentRuntimeType>('claude')
  const [runtimeRef, setRuntimeRef] = useState('')

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    const trimmedModel = model.trim()
    if (
      mode === 'runtime'
      && runtimeSource === 'built-in'
      && runtimeType === 'opencode'
      && !isOpenCodeModelID(trimmedModel)
    ) {
      toast.error('OpenCode requires a literal provider/model ID')
      return
    }
    if (mode === 'runtime' && runtimeSource === 'built-in' && !trimmedModel) {
      toast.error('Model is required for built-in ACP runtimes')
      return
    }
    const parsedContextWindow = Number(contextWindow)
    const parsedMaxTokens = Number(maxTokens)
    if (mode === 'runtime' && runtimeSource === 'built-in' && runtimeType === 'opencode') {
      if (!Number.isInteger(parsedContextWindow) || parsedContextWindow <= 0) {
        toast.error('OpenCode requires a positive context window')
        return
      }
      if (!Number.isInteger(parsedMaxTokens) || parsedMaxTokens <= 0) {
        toast.error('OpenCode requires a positive max output token limit')
        return
      }
      if (parsedContextWindow <= parsedMaxTokens) {
        toast.error('OpenCode context window must exceed max output tokens')
        return
      }
    }
    if (mode === 'runtime' && runtimeSource === 'external' && !runtimeRef.trim()) {
      toast.error('AgentRuntime name is required')
      return
    }

    const spec: Record<string, unknown> = {}

    if (mode === 'ai') {
      spec.model = {
        provider,
        name: model,
        ...(temperature ? { temperature: parseFloat(temperature) } : {}),
        ...(maxTokens ? { maxTokens: parseInt(maxTokens) } : {}),
      }
      if (systemPrompt) {
        spec.systemPrompt = { inline: systemPrompt }
      }
      if (secretRef && secretRef !== '__none__') {
        spec.secretRef = { name: secretRef }
      }
    } else {
      spec.runtime = runtimeSource === 'built-in'
        ? runtimeType === 'opencode'
          ? {
              type: runtimeType,
              defaultAllowedTools: [...OPENCODE_REVIEWED_RUNTIME_DEFAULTS.defaultAllowedTools],
              defaultAllowBash: OPENCODE_REVIEWED_RUNTIME_DEFAULTS.defaultAllowBash,
            }
          : { type: runtimeType }
        : { runtimeRef: { name: runtimeRef.trim() } }
      if (runtimeSource === 'built-in' && trimmedModel) {
        spec.model = runtimeType === 'opencode'
          ? { name: trimmedModel, contextWindow: parsedContextWindow, maxTokens: parsedMaxTokens }
          : { name: trimmedModel }
      }
    }

    try {
      await createAgent.mutateAsync({ name, namespace, spec })
      toast.success('Agent created')
      navigate({ to: '/agents' })
    } catch (err) {
      toast.error(`Failed to create agent: ${err instanceof Error ? err.message : 'Unknown error'}`)
    }
  }

  return (
    <div className="space-y-4">
      <PageHeader title="Create Agent" description="Configure a native AI agent or an ACP runtime profile" />
      <Card>
        <CardHeader>
          <CardTitle>Agent Configuration</CardTitle>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <label htmlFor="agent-name" className="text-sm font-medium">Name</label>
                <Input id="agent-name" value={name} onChange={(e) => setName(e.target.value)} placeholder="my-agent" required />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Mode</label>
                <Select value={mode} onValueChange={(value) => setMode(value as 'ai' | 'runtime')}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="ai">Native AI (LLM provider)</SelectItem>
                    <SelectItem value="runtime">ACP agent runtime</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            {mode === 'ai' && (
              <div className="space-y-4">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <label htmlFor="agent-provider" className="text-sm font-medium">Provider</label>
                    <Select value={provider} onValueChange={setProvider}>
                      <SelectTrigger id="agent-provider"><SelectValue placeholder="Select provider" /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="anthropic">Anthropic</SelectItem>
                        <SelectItem value="openai">OpenAI</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <label htmlFor="agent-model" className="text-sm font-medium">Model</label>
                    <Input id="agent-model" value={model} onChange={(e) => setModel(e.target.value)} placeholder="claude-sonnet-4-20250514" />
                  </div>
                </div>
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <label htmlFor="agent-temperature" className="text-sm font-medium">Temperature</label>
                    <Input id="agent-temperature" type="number" step="0.1" min="0" max="2" value={temperature} onChange={(e) => setTemperature(e.target.value)} />
                  </div>
                  <div className="space-y-2">
                    <label htmlFor="agent-max-tokens" className="text-sm font-medium">Max Tokens</label>
                    <Input id="agent-max-tokens" type="number" value={maxTokens} onChange={(e) => setMaxTokens(e.target.value)} />
                  </div>
                </div>
                <div className="space-y-2">
                  <label htmlFor="agent-system-prompt" className="text-sm font-medium">System Prompt</label>
                  <textarea
                    id="agent-system-prompt"
                    className="flex min-h-24 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    value={systemPrompt}
                    onChange={(e) => setSystemPrompt(e.target.value)}
                    placeholder="Optional system prompt..."
                  />
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Secret Reference</label>
                  <Select value={secretRef} onValueChange={setSecretRef}>
                    <SelectTrigger><SelectValue placeholder="Select a secret..." /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__none__">None</SelectItem>
                      {(secretsData?.items ?? []).map((secret) => (
                        <SelectItem key={secret.name} value={secret.name}>{secret.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground">Kubernetes Secret containing native AI provider credentials</p>
                </div>
              </div>
            )}

            {mode === 'runtime' && (
              <div className="space-y-4 rounded-md border bg-muted/20 p-4">
                <div className="space-y-2">
                  <label className="text-sm font-medium">Runtime source</label>
                  <Select value={runtimeSource} onValueChange={(value) => setRuntimeSource(value as 'built-in' | 'external')}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="built-in">Orka-managed RuntimePool</SelectItem>
                      <SelectItem value="external">External v2 AgentRuntime</SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground">
                    Runtime credentials and publication credentials are never copied into the ACP process tree.
                  </p>
                </div>

                {runtimeSource === 'built-in' ? (
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Runtime profile</label>
                    <Select value={runtimeType} onValueChange={(value) => setRuntimeType(value as BuiltInAgentRuntimeType)}>
                      <SelectTrigger><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="claude">Claude ACP</SelectItem>
                        <SelectItem value="codex">OpenAI Codex ACP</SelectItem>
                        <SelectItem value="copilot">GitHub Copilot ACP</SelectItem>
                        <SelectItem value="opencode">OpenCode ACP</SelectItem>
                      </SelectContent>
                    </Select>
                    {runtimeType === 'opencode' && (
                      <p className="text-xs text-muted-foreground">
                        Reviewed native-tool defaults: Read, Write, Edit, Bash, Glob, and Grep; bash enabled.
                      </p>
                    )}
                  </div>
                ) : (
                  <div className="space-y-2">
                    <label htmlFor="agent-runtime-ref" className="text-sm font-medium">AgentRuntime name</label>
                    <Input
                      id="agent-runtime-ref"
                      value={runtimeRef}
                      onChange={(e) => setRuntimeRef(e.target.value)}
                      placeholder="external-codex"
                      required
                    />
                    <p className="text-xs text-muted-foreground">The registration must pass the orka.harness.v2 conformance profile.</p>
                  </div>
                )}

                {runtimeSource === 'built-in' && (
                  <div className="space-y-2">
                    <label htmlFor="runtime-model" className="text-sm font-medium">Model</label>
                    <Input
                      id="runtime-model"
                      value={model}
                      onChange={(e) => setModel(e.target.value)}
                      placeholder={runtimeType === 'opencode' ? 'openai/gpt-5.4' : 'Enter model identifier'}
                      required
                    />
                    {runtimeType === 'opencode' && (
                      <p className="text-xs text-muted-foreground">OpenCode model IDs use provider/model form.</p>
                    )}
                  </div>
                )}
                {runtimeSource === 'built-in' && runtimeType === 'opencode' && (
                  <div className="grid gap-4 md:grid-cols-2">
                    <div className="space-y-2">
                      <label htmlFor="runtime-context-window" className="text-sm font-medium">Context Window</label>
                      <Input
                        id="runtime-context-window"
                        type="number"
                        min="1"
                        step="1"
                        value={contextWindow}
                        onChange={(e) => setContextWindow(e.target.value)}
                        placeholder="32768"
                      />
                    </div>
                    <div className="space-y-2">
                      <label htmlFor="runtime-max-tokens" className="text-sm font-medium">Max Output Tokens</label>
                      <Input
                        id="runtime-max-tokens"
                        type="number"
                        min="1"
                        step="1"
                        value={maxTokens}
                        onChange={(e) => setMaxTokens(e.target.value)}
                        placeholder="4096"
                      />
                    </div>
                    <p className="text-xs text-muted-foreground md:col-span-2">
                      Use reviewed ceilings for the selected model. Orka pins them into the immutable runtime profile.
                    </p>
                  </div>
                )}
              </div>
            )}

            <div className="flex gap-2">
              <Button type="submit" disabled={createAgent.isPending}>
                {createAgent.isPending ? 'Creating...' : 'Create Agent'}
              </Button>
              <Button type="button" variant="outline" onClick={() => navigate({ to: '/agents' })}>
                Cancel
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  )
}
