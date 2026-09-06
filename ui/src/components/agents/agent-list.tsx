import { Link } from '@tanstack/react-router'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { ListAccessError } from '@/components/ui/list-access-error'
import { PageHeader } from '@/components/layout/page-header'
import { Bot, Plus } from 'lucide-react'
import { useAgentList } from '@/hooks/use-agents'
import type { Agent } from '@/schemas/agent'
import { builtInAgentRuntimeLabel } from '@/lib/agent-runtime'

export function AgentList() {
  const { data, isLoading, error } = useAgentList()
  const agents = error ? [] : (data?.items ?? [])

  return (
    <div className="space-y-4">
      <PageHeader
        title="Agents"
        description="Registered AI agent configurations"
        action={
          <Link to="/agents/new">
            <Button><Plus className="mr-2 h-4 w-4" />New Agent</Button>
          </Link>
        }
      />

      {isLoading ? (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {Array.from({ length: 6 }).map((_, i) => (
            <Card key={i}>
              <CardHeader><Skeleton className="h-5 w-32" /></CardHeader>
              <CardContent><Skeleton className="h-4 w-48" /></CardContent>
            </Card>
          ))}
        </div>
      ) : error ? (
        <Card><CardContent className="pt-6"><ListAccessError error={error} resource="agents" /></CardContent></Card>
      ) : agents.length === 0 ? (
        <div className="text-center py-12 text-muted-foreground">No agents registered.</div>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {agents.map((agent: Agent) => (
            <Link key={agent.metadata.uid || agent.metadata.name} to="/agents/$agentId" params={{ agentId: agent.metadata.name }}>
              <Card className="hover:border-primary/50 transition-colors cursor-pointer">
                <CardHeader className="flex flex-row items-center gap-3 space-y-0">
                  <div className="flex h-10 w-10 items-center justify-center rounded-full bg-primary/10">
                    <Bot className="h-5 w-5 text-primary" />
                  </div>
                  <div>
                    <CardTitle className="text-base">{agent.metadata.name}</CardTitle>
                    <p className="text-xs text-muted-foreground">{agent.metadata.namespace}</p>
                  </div>
                </CardHeader>
                <CardContent className="space-y-2">
                  <div className="flex flex-wrap gap-2 text-xs">
                    {agent.spec.model?.provider && <Badge variant="secondary">{agent.spec.model.provider}</Badge>}
                    {agent.spec.model?.name && <Badge variant="outline">{agent.spec.model.name}</Badge>}
                    {agent.spec.runtime && (
                      <Badge variant="secondary">
                        {'type' in agent.spec.runtime ? builtInAgentRuntimeLabel(agent.spec.runtime.type) : `AgentRuntime ${agent.spec.runtime.runtimeRef.name}`}
                      </Badge>
                    )}
                  </div>
                  <div className="flex items-center justify-between text-sm text-muted-foreground">
                    <span>Active: {agent.status?.activeTasks ?? 0}</span>
                    {(agent.spec.tools?.length ?? 0) > 0 && <span>{agent.spec.tools!.length} tools</span>}
                  </div>
                </CardContent>
              </Card>
            </Link>
          ))}
        </div>
      )}
    </div>
  )
}
