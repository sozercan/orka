import { ArrowLeft, Clock3, RadioTower } from 'lucide-react'
import { Link } from '@tanstack/react-router'
import { PageHeader } from '@/components/layout/page-header'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useGateway, useGatewayBindings, useGatewayDeliveries, useGatewayEvents, useGatewayLedgerPagination } from '@/hooks/use-gateways'
import { GatewayLedgerPagination } from './gateway-pagination'
import { ListAccessError } from '@/components/ui/list-access-error'
import { isGatewayResourceReady, isGatewayStatusFresh } from './gateway-readiness'
import { GatewaySessionQueue } from './gateway-session-queue'
import { GatewayBindingsTable, GatewayDeliveriesTable, GatewayEventsTable, GatewayStatusBadge } from './gateway-tables'
import { formatTimestamp } from '@/lib/time'
import { humanizeCamelCase } from '@/lib/utils'

export function GatewayDetail({ name }: { name: string }) {
  const eventPage = useGatewayLedgerPagination(`${name}:events`)
  const deliveryPage = useGatewayLedgerPagination(`${name}:deliveries`)
  const gateway = useGateway(name)
  const bindings = useGatewayBindings(name)
  const events = useGatewayEvents({ gateway: name, continue: eventPage.cursor })
  const deliveries = useGatewayDeliveries({ gateway: name, continue: deliveryPage.cursor })

  if (gateway.isLoading) return <Skeleton className="h-96 rounded-xl" />
  if (gateway.error) return <ListAccessError resource="Gateway" error={gateway.error} />
  if (!gateway.data) return <Card><CardContent className="py-12 text-center text-muted-foreground">Gateway not found.</CardContent></Card>

  const item = gateway.data
  const observed = item.status?.observedCapabilities
  const capabilities = observed?.capabilities
  const ready = isGatewayResourceReady(item)
  const statusFresh = isGatewayStatusFresh(item)
  return (
    <div className="space-y-6">
      <Link to="/gateways" className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground"><ArrowLeft className="h-4 w-4" />Back to switchboard</Link>
      <PageHeader
        eyebrow={item.spec.gatewayClassName}
        title={item.metadata.name}
        description="Adapter readiness, semantic bindings, Session queue, and delivery history."
        action={<GatewayStatusBadge state={ready ? 'Ready' : 'NotReady'} />}
      />

      <div className="grid gap-4 lg:grid-cols-[1.25fr_0.75fr]">
        <Card>
          <CardHeader><CardTitle className="flex items-center gap-2"><RadioTower className="h-5 w-5 text-primary" />Adapter boundary</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="rounded-md border bg-muted/30 p-3 font-mono text-sm">{item.status?.resolvedEndpoint || 'Unresolved endpoint'}</div>
            {!statusFresh && (
              <p className="rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-sm text-destructive">
                Status generation {item.status?.observedGeneration ?? 'unobserved'} is stale for generation {item.metadata.generation ?? 'unknown'}; adapter details may describe the previous spec.
              </p>
            )}
            <div className="grid gap-3 text-sm sm:grid-cols-2">
              <KeyValue label="Adapter" value={observed?.adapterName || 'Not observed'} />
              <KeyValue label="Version" value={observed?.adapterVersion || '—'} />
              <KeyValue label="Contract" value={observed?.contractVersion || '—'} />
              <KeyValue label="Last probe" value={formatTimestamp(item.status?.lastSuccessfulProbe)} />
            </div>
            {item.status?.message && <p className="text-sm text-muted-foreground">{item.status.message}</p>}
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle>Observed capabilities</CardTitle></CardHeader>
          <CardContent className="flex flex-wrap gap-2">
            {Object.entries(capabilities ?? {}).filter(([, enabled]) => enabled).map(([name]) => <Badge key={name} variant="outline">{humanizeCamelCase(name)}</Badge>)}
            {!capabilities && <p className="text-sm text-muted-foreground">Capabilities have not been observed.</p>}
          </CardContent>
        </Card>
      </div>

      <Tabs defaultValue="events" className="gap-4">
        <TabsList variant="line">
          <TabsTrigger value="queues">Session queues</TabsTrigger>
          <TabsTrigger value="events">Event timeline</TabsTrigger>
          <TabsTrigger value="deliveries">Deliveries</TabsTrigger>
          <TabsTrigger value="bindings">Bindings</TabsTrigger>
        </TabsList>
        <TabsContent value="queues" className="space-y-3">
          {events.error ? <ListAccessError resource="gateway Session queues" error={events.error} /> : <>
            <GatewaySessionQueue events={events.data?.items ?? []} loading={events.isLoading} />
            <GatewayLedgerPagination
              label="queue event records"
              page={eventPage.page}
              hasPrevious={eventPage.hasPrevious}
              nextCursor={events.data?.metadata?.continue}
              onPrevious={eventPage.previous}
              onNext={eventPage.next}
            />
          </>}
        </TabsContent>
        <TabsContent value="events" className="space-y-3">
          {events.error ? <ListAccessError resource="gateway events" error={events.error} /> : <>
            <GatewayEventsTable events={events.data?.items ?? []} loading={events.isLoading} />
            <GatewayLedgerPagination
              label="events"
              page={eventPage.page}
              hasPrevious={eventPage.hasPrevious}
              nextCursor={events.data?.metadata?.continue}
              onPrevious={eventPage.previous}
              onNext={eventPage.next}
            />
          </>}
        </TabsContent>
        <TabsContent value="deliveries" className="space-y-3">
          {deliveries.error ? <ListAccessError resource="gateway deliveries" error={deliveries.error} /> : <>
            <GatewayDeliveriesTable deliveries={deliveries.data?.items ?? []} loading={deliveries.isLoading} />
            <GatewayLedgerPagination
              label="deliveries"
              page={deliveryPage.page}
              hasPrevious={deliveryPage.hasPrevious}
              nextCursor={deliveries.data?.metadata?.continue}
              onPrevious={deliveryPage.previous}
              onNext={deliveryPage.next}
            />
          </>}
        </TabsContent>
        <TabsContent value="bindings">
          {bindings.error
            ? <ListAccessError resource="GatewayBindings" error={bindings.error} />
            : <GatewayBindingsTable bindings={bindings.data?.items ?? []} loading={bindings.isLoading} />}
        </TabsContent>
      </Tabs>
    </div>
  )
}

function KeyValue({ label, value }: { label: string; value: string }) {
  return <div><p className="text-xs uppercase tracking-wide text-muted-foreground">{label}</p><p className="mt-1 flex items-center gap-1.5 font-medium"><Clock3 className="h-3.5 w-3.5 text-muted-foreground" />{value}</p></div>
}

