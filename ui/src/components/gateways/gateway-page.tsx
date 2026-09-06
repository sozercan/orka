import { Link } from '@tanstack/react-router'
import { Bot, Inbox, Network, RadioTower, Send, ShieldAlert } from 'lucide-react'
import { PageHeader } from '@/components/layout/page-header'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useGatewayBindings, useGatewayDeliveries, useGatewayEvents, useGatewayLedgerPagination, useGateways } from '@/hooks/use-gateways'
import { GatewayLedgerPagination } from './gateway-pagination'
import { ListAccessError } from '@/components/ui/list-access-error'
import { isGatewayResourceReady, isGatewayStatusFresh } from './gateway-readiness'
import { GatewaySessionQueue } from './gateway-session-queue'
import { GatewayBindingsTable, GatewayDeliveriesTable, GatewayEventsTable, GatewayStatusBadge } from './gateway-tables'

export function GatewayPage() {
  const eventPage = useGatewayLedgerPagination('all-events')
  const deliveryPage = useGatewayLedgerPagination('all-deliveries')
  const gateways = useGateways()
  const bindings = useGatewayBindings()
  const events = useGatewayEvents({ continue: eventPage.cursor })
  const deliveries = useGatewayDeliveries({ continue: deliveryPage.cursor })

  const eventItems = events.data?.items ?? []
  const deliveryItems = deliveries.data?.items ?? []
  const queued = eventItems.filter((event) => ['Accepted', 'Queued', 'Dispatching'].includes(event.state)).length
  const activeTasks = eventItems.filter((event) => event.state === 'TaskCreated').length
  const pendingDeliveries = deliveryItems.filter((delivery) => ['Pending', 'Sending', 'RetryScheduled'].includes(delivery.state)).length

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="External conversation plane"
        title="Gateway switchboard"
        description="Follow normalized messages from durable admission through Session-ordered Tasks to idempotent adapter delivery."
      />

      <SignalRail
        visibleEvents={eventItems.length}
        queuedOnPage={queued}
        tasksOnPage={activeTasks}
        pendingDeliveriesOnPage={pendingDeliveries}
      />

      <Tabs defaultValue="gateways" className="gap-4">
        <TabsList variant="line" aria-label="Gateway operator views">
          <TabsTrigger value="gateways">Gateways</TabsTrigger>
          <TabsTrigger value="bindings">Bindings</TabsTrigger>
          <TabsTrigger value="queues">Session queues</TabsTrigger>
          <TabsTrigger value="events">Event ledger</TabsTrigger>
          <TabsTrigger value="deliveries">Delivery outbox</TabsTrigger>
        </TabsList>
        <TabsContent value="gateways">
          {gateways.error ? (
            <ListAccessError resource="Gateways" error={gateways.error} />
          ) : gateways.isLoading ? (
            <div className="grid gap-4 lg:grid-cols-2">
              {Array.from({ length: 2 }).map((_, index) => <Skeleton key={index} className="h-52 rounded-xl" />)}
            </div>
          ) : (gateways.data?.items ?? []).length === 0 ? (
            <Card><CardContent className="py-12 text-center text-sm text-muted-foreground">No Gateways are configured in this namespace.</CardContent></Card>
          ) : (
            <div className="grid gap-4 lg:grid-cols-2">
              {(gateways.data?.items ?? []).map((gateway) => {
                const observed = gateway.status?.observedCapabilities
                const capabilities = observed?.capabilities
                const ready = isGatewayResourceReady(gateway)
                const statusFresh = isGatewayStatusFresh(gateway)
                return (
                  <Card key={gateway.metadata.name} className="overflow-hidden transition-colors hover:border-primary/50">
                    <div className="h-1 bg-[linear-gradient(90deg,var(--color-primary),transparent_78%)]" aria-hidden="true" />
                    <CardHeader className="flex flex-row items-start justify-between gap-4 space-y-0">
                      <div className="space-y-1">
                        <CardTitle className="flex items-center gap-2 text-lg">
                          <RadioTower className="h-5 w-5 text-primary" />
                          <Link to="/gateways/$gatewayId" params={{ gatewayId: gateway.metadata.name }} className="hover:underline">
                            {gateway.metadata.name}
                          </Link>
                        </CardTitle>
                        <p className="font-mono text-xs text-muted-foreground">{gateway.spec.gatewayClassName}</p>
                      </div>
                      <GatewayStatusBadge state={ready ? 'Ready' : 'NotReady'} />
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div className="rounded-md border bg-muted/30 px-3 py-2 font-mono text-xs text-muted-foreground">
                        {gateway.status?.resolvedEndpoint || 'Endpoint unresolved'}
                      </div>
                      <div className="flex flex-wrap gap-2">
                        {capabilities?.inboundText && <Badge variant="secondary">inbound text</Badge>}
                        {capabilities?.outboundText && <Badge variant="secondary">outbound text</Badge>}
                        {capabilities?.threads && <Badge variant="outline">threads</Badge>}
                        {capabilities?.senderIdentity && <Badge variant="outline">sender identity</Badge>}
                        {capabilities?.idempotentDelivery && <Badge variant="outline">idempotent delivery</Badge>}
                      </div>
                      <div className="flex items-center justify-between text-xs text-muted-foreground">
                        <span>{observed?.adapterName || 'Adapter not observed'}</span>
                        <span>{observed?.adapterVersion || ''}</span>
                      </div>
                      {!statusFresh && (
                        <p className="flex items-start gap-2 text-sm text-destructive">
                          <ShieldAlert className="mt-0.5 h-4 w-4 shrink-0" />
                          Status generation {gateway.status?.observedGeneration ?? 'unobserved'} is stale for generation {gateway.metadata.generation ?? 'unknown'}.
                        </p>
                      )}
                      {gateway.status?.message && !ready && statusFresh && (
                        <p className="flex items-start gap-2 text-sm text-destructive">
                          <ShieldAlert className="mt-0.5 h-4 w-4 shrink-0" />
                          {gateway.status.message}
                        </p>
                      )}
                    </CardContent>
                  </Card>
                )
              })}
            </div>
          )}
        </TabsContent>
        <TabsContent value="bindings">
          {bindings.error
            ? <ListAccessError resource="GatewayBindings" error={bindings.error} />
            : <GatewayBindingsTable bindings={bindings.data?.items ?? []} loading={bindings.isLoading} />}
        </TabsContent>
        <TabsContent value="queues" className="space-y-3">
          {events.error ? (
            <ListAccessError resource="gateway Session queues" error={events.error} />
          ) : (
            <>
              <GatewaySessionQueue events={eventItems} loading={events.isLoading} />
              <GatewayLedgerPagination
                label="queue event records"
                page={eventPage.page}
                hasPrevious={eventPage.hasPrevious}
                nextCursor={events.data?.metadata?.continue}
                onPrevious={eventPage.previous}
                onNext={eventPage.next}
              />
            </>
          )}
        </TabsContent>
        <TabsContent value="events" className="space-y-3">
          {events.error ? (
            <ListAccessError resource="gateway events" error={events.error} />
          ) : (
            <>
              <GatewayEventsTable events={eventItems} loading={events.isLoading} />
              <GatewayLedgerPagination
                label="events"
                page={eventPage.page}
                hasPrevious={eventPage.hasPrevious}
                nextCursor={events.data?.metadata?.continue}
                onPrevious={eventPage.previous}
                onNext={eventPage.next}
              />
            </>
          )}
        </TabsContent>
        <TabsContent value="deliveries" className="space-y-3">
          {deliveries.error ? (
            <ListAccessError resource="gateway deliveries" error={deliveries.error} />
          ) : (
            <>
              <GatewayDeliveriesTable deliveries={deliveryItems} loading={deliveries.isLoading} />
              <GatewayLedgerPagination
                label="deliveries"
                page={deliveryPage.page}
                hasPrevious={deliveryPage.hasPrevious}
                nextCursor={deliveries.data?.metadata?.continue}
                onPrevious={deliveryPage.previous}
                onNext={deliveryPage.next}
              />
            </>
          )}
        </TabsContent>
      </Tabs>
    </div>
  )
}

function SignalRail({ visibleEvents, queuedOnPage, tasksOnPage, pendingDeliveriesOnPage }: {
  visibleEvents: number
  queuedOnPage: number
  tasksOnPage: number
  pendingDeliveriesOnPage: number
}) {
  const stages = [
    { label: 'Ingress records loaded', value: visibleEvents, icon: Inbox, note: 'event page' },
    { label: 'Queue records loaded', value: queuedOnPage, icon: Network, note: 'event page' },
    { label: 'Task records loaded', value: tasksOnPage, icon: Bot, note: 'event page' },
    { label: 'Pending deliveries loaded', value: pendingDeliveriesOnPage, icon: Send, note: 'delivery page' },
  ]
  return (
    <Card className="overflow-hidden border-primary/20 bg-[radial-gradient(circle_at_18%_0%,color-mix(in_oklab,var(--color-primary)_12%,transparent),transparent_42%)]">
      <CardContent className="p-0">
        <div className="flex flex-wrap items-center justify-between gap-2 border-b border-primary/10 bg-card/60 px-4 py-2 text-xs text-muted-foreground">
          <span className="font-medium text-foreground">Current ledger page sample</span>
          <span>Each value reflects only its loaded page (up to 100 records), not namespace totals.</span>
        </div>
        <div className="grid md:grid-cols-4">
          {stages.map(({ label, value, icon: Icon, note }, index) => (
            <div key={label} className="relative border-b p-4 last:border-b-0 md:border-b-0 md:border-r md:last:border-r-0">
              {index < stages.length - 1 && (
                <span className="absolute right-[-5px] top-1/2 z-10 hidden h-2 w-2 -translate-y-1/2 rotate-45 border-r border-t border-primary/50 bg-card md:block" aria-hidden="true" />
              )}
              <div className="flex items-center justify-between gap-3">
                <div className="space-y-1">
                  <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">{String(index + 1).padStart(2, '0')} · {note}</p>
                  <p className="text-sm font-medium">{label}</p>
                </div>
                <Icon className="h-5 w-5 text-primary" />
              </div>
              <p className="mt-4 font-mono text-3xl font-semibold tabular-nums">{value}</p>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}
