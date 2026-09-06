import { Link } from '@tanstack/react-router'
import { ArrowLeft, Bot, Clock3, Filter, RadioTower, Settings2, ShieldCheck, Workflow } from 'lucide-react'
import { PageHeader } from '@/components/layout/page-header'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useGatewayBinding } from '@/hooks/use-gateways'
import { ListAccessError } from '@/components/ui/list-access-error'
import { isGatewayResourceReady, isGatewayStatusFresh } from './gateway-readiness'
import { GatewayStatusBadge } from './gateway-tables'
import { formatTimestamp } from '@/lib/time'
import { humanizeCamelCase } from '@/lib/utils'

export function GatewayBindingDetail({ name }: { name: string }) {
  const binding = useGatewayBinding(name)

  if (binding.isLoading) return <Skeleton className="h-96 rounded-xl" />
  if (binding.error) return <ListAccessError resource={`GatewayBinding ${name}`} error={binding.error} />
  if (!binding.data) {
    return <Card><CardContent className="py-12 text-center text-muted-foreground">GatewayBinding not found.</CardContent></Card>
  }

  const item = binding.data
  const senderMode = item.spec.senderPolicy?.mode || 'allowlist'
  const allowedSenders = item.spec.senderPolicy?.allowedSenderIds ?? []
  const taskDefaults = item.spec.taskDefaults
  const resolvedCapabilities = item.status?.resolvedCapabilities
  const ready = isGatewayResourceReady(item)
  const statusFresh = isGatewayStatusFresh(item)

  return (
    <div className="space-y-6">
      <Link
        to="/gateways/$gatewayId"
        params={{ gatewayId: item.spec.gatewayRef.name }}
        className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground"
      >
        <ArrowLeft className="h-4 w-4" />
        Back to Gateway {item.spec.gatewayRef.name}
      </Link>

      <PageHeader
        eyebrow={`GatewayBinding · priority ${item.spec.priority ?? 0}`}
        title={item.metadata.name}
        description="Inspect the exact external identity boundary, sender authorization, Session routing, and Task defaults programmed by this binding."
        action={<GatewayStatusBadge state={ready ? 'Ready' : 'NotReady'} />}
      />

      <Card className="overflow-hidden border-primary/20">
        <div className="h-1 bg-[linear-gradient(90deg,var(--color-primary),transparent_78%)]" aria-hidden="true" />
        <CardContent className="grid gap-0 p-0 md:grid-cols-[1fr_auto_1fr]">
          <RouteEndpoint
            icon={RadioTower}
            label="Gateway"
            name={item.spec.gatewayRef.name}
            link={<Link to="/gateways/$gatewayId" params={{ gatewayId: item.spec.gatewayRef.name }} className="hover:underline">{item.spec.gatewayRef.name}</Link>}
          />
          <div className="hidden items-center px-4 text-muted-foreground md:flex" aria-hidden="true">
            <Workflow className="h-5 w-5" />
          </div>
          <RouteEndpoint
            icon={Bot}
            label="Agent"
            name={item.spec.agentRef.name}
            link={<Link to="/agents/$agentId" params={{ agentId: item.spec.agentRef.name }} className="hover:underline">{item.spec.agentRef.name}</Link>}
          />
        </CardContent>
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader><CardTitle className="flex items-center gap-2"><Filter className="h-5 w-5 text-primary" />Exact match boundary</CardTitle></CardHeader>
          <CardContent className="grid gap-4 text-sm sm:grid-cols-2">
            <KeyValue label="Account" value={item.spec.match.accountId} mono />
            <KeyValue label="Context" value={item.spec.match.contextId} mono />
            <KeyValue label="Thread" value={item.spec.match.threadId || 'Any thread'} mono={!!item.spec.match.threadId} />
            <KeyValue label="Exact sender" value={item.spec.match.senderId || 'Any authorized sender'} mono={!!item.spec.match.senderId} />
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle className="flex items-center gap-2"><ShieldCheck className="h-5 w-5 text-primary" />Sender authorization</CardTitle></CardHeader>
          <CardContent className="space-y-4 text-sm">
            <div className="flex items-center justify-between gap-3 rounded-md border bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Policy mode</span>
              <Badge variant={senderMode === 'all' ? 'secondary' : 'outline'}>{senderMode}</Badge>
            </div>
            {senderMode === 'all' ? (
              <p className="text-sm text-muted-foreground">Every normalized sender in the matched external context is authorized.</p>
            ) : allowedSenders.length === 0 ? (
              <p className="text-sm text-muted-foreground">No sender IDs are allowlisted; ingress fails closed unless an exact sender match is configured.</p>
            ) : (
              <div className="space-y-2">
                <p className="text-xs uppercase tracking-wide text-muted-foreground">Allowed sender IDs</p>
                <div className="flex flex-wrap gap-2">
                  {allowedSenders.map((sender) => <Badge key={sender} variant="outline" className="font-mono">{sender}</Badge>)}
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle className="flex items-center gap-2"><Workflow className="h-5 w-5 text-primary" />Session policy</CardTitle></CardHeader>
          <CardContent className="grid gap-4 text-sm sm:grid-cols-2">
            <KeyValue label="Derivation mode" value={item.spec.session?.mode || 'context'} />
            <KeyValue label="Explicit name" value={item.spec.session?.name || 'Derived at admission'} mono={!!item.spec.session?.name} />
            <KeyValue label="Active turn" value={item.spec.activeTurnBehavior || 'queue'} />
            <KeyValue label="Ordering" value="FIFO per Session" />
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle className="flex items-center gap-2"><Settings2 className="h-5 w-5 text-primary" />Task defaults</CardTitle></CardHeader>
          <CardContent className="grid gap-4 text-sm sm:grid-cols-2">
            <KeyValue label="Task priority" value={formatOptionalNumber(taskDefaults?.priority)} />
            <KeyValue label="Timeout" value={taskDefaults?.timeout || 'Controller default'} mono={!!taskDefaults?.timeout} />
            <KeyValue label="Max retries" value={formatOptionalNumber(taskDefaults?.retryPolicy?.maxRetries)} />
            <KeyValue label="Retry multiplier" value={formatOptionalNumber(taskDefaults?.retryPolicy?.backoffMultiplier)} />
            <KeyValue label="Initial retry delay" value={taskDefaults?.retryPolicy?.initialDelay || 'Controller default'} mono={!!taskDefaults?.retryPolicy?.initialDelay} />
            <KeyValue label="Runtime max turns" value={formatOptionalNumber(taskDefaults?.agentRuntimeMaxTurns)} />
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle>Readiness gates</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            {!statusFresh && (
              <p className="rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-sm text-destructive">
                Status generation {item.status?.observedGeneration ?? 'unobserved'} is stale for generation {item.metadata.generation ?? 'unknown'}; this binding is not currently routable.
              </p>
            )}
            <div className="grid gap-2 sm:grid-cols-2">
              <ReadinessGate label="Accepted" ready={statusFresh && item.status?.accepted} />
              <ReadinessGate label="References resolved" ready={statusFresh && item.status?.resolvedRefs} />
              <ReadinessGate label="Programmed" ready={statusFresh && item.status?.programmed} />
              <ReadinessGate label="Ready" ready={ready} />
            </div>
            {item.status?.message && <p className="rounded-md border bg-muted/20 px-3 py-2 text-sm text-muted-foreground">{item.status.message}</p>}
            {(item.status?.conditions?.length ?? 0) > 0 && (
              <div className="space-y-2 border-t pt-4">
                <p className="text-xs uppercase tracking-wide text-muted-foreground">Controller conditions</p>
                {item.status?.conditions?.map((condition) => (
                  <div key={condition.type} className="flex flex-wrap items-center gap-2 text-sm">
                    <Badge variant={condition.status === 'True' ? 'outline' : 'secondary'}>{condition.type}</Badge>
                    <span>{condition.status}</span>
                    {condition.reason && <span className="text-muted-foreground">· {condition.reason}</span>}
                    {condition.message && <span className="basis-full text-xs text-muted-foreground">{condition.message}</span>}
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle className="flex items-center gap-2"><Clock3 className="h-5 w-5 text-primary" />Activity and capabilities</CardTitle></CardHeader>
          <CardContent className="space-y-4 text-sm">
            <div className="grid gap-4 sm:grid-cols-2">
              <KeyValue label="Last inbound" value={formatTimestamp(item.status?.lastInboundActivity)} />
              <KeyValue label="Last outbound" value={formatTimestamp(item.status?.lastOutboundActivity)} />
              <KeyValue label="Observed generation" value={item.status?.observedGeneration?.toString() || 'Not observed'} />
              <KeyValue label="Created" value={formatTimestamp(item.metadata.creationTimestamp)} />
            </div>
            <div className="space-y-2 border-t pt-4">
              <p className="text-xs uppercase tracking-wide text-muted-foreground">Resolved adapter capabilities</p>
              <div className="flex flex-wrap gap-2">
                {Object.entries(resolvedCapabilities ?? {}).filter(([, enabled]) => enabled).map(([capability]) => (
                  <Badge key={capability} variant="outline">{humanizeCamelCase(capability)}</Badge>
                ))}
                {!resolvedCapabilities && <span className="text-sm text-muted-foreground">No capabilities have been resolved.</span>}
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function RouteEndpoint({ icon: Icon, label, name, link }: {
  icon: typeof RadioTower
  label: string
  name: string
  link: React.ReactNode
}) {
  return (
    <div className="flex items-center gap-3 p-5">
      <div className="rounded-md border bg-muted/30 p-2"><Icon className="h-5 w-5 text-primary" /></div>
      <div className="min-w-0">
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-muted-foreground">{label}</p>
        <p className="truncate font-mono text-base font-semibold" title={name}>{link}</p>
      </div>
    </div>
  )
}

function KeyValue({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="min-w-0">
      <p className="text-xs uppercase tracking-wide text-muted-foreground">{label}</p>
      <p className={`mt-1 break-words font-medium ${mono ? 'font-mono text-xs' : ''}`}>{value}</p>
    </div>
  )
}

function ReadinessGate({ label, ready }: { label: string; ready?: boolean }) {
  return (
    <div className="flex items-center justify-between gap-3 rounded-md border bg-muted/20 px-3 py-2 text-sm">
      <span>{label}</span>
      <GatewayStatusBadge state={ready ? 'Ready' : 'NotReady'} />
    </div>
  )
}

function formatOptionalNumber(value?: number) {
  return value === undefined ? 'Controller default' : value.toString()
}

