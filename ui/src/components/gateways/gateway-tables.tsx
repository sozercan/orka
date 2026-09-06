import { Link } from '@tanstack/react-router'
import { ArrowUpRight, LockKeyhole, RotateCcw } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useRetryGatewayDelivery } from '@/hooks/use-gateways'
import type { GatewayBinding, GatewayDelivery, GatewayEvent } from '@/schemas/gateway'
import { isGatewayResourceReady } from './gateway-readiness'
import { timeAgo } from '@/lib/time'

export function GatewayStatusBadge({ state }: { state?: string }) {
  const destructive = state === 'Rejected' || state === 'DeadLettered' || state === 'Expired' || state === 'Failed' || state === 'NotReady'
  const active = state === 'Dispatching' || state === 'TaskCreated' || state === 'Sending' || state === 'RetryScheduled'
  return <Badge variant={destructive ? 'destructive' : active ? 'secondary' : 'outline'}>{state || 'Unknown'}</Badge>
}

export function GatewayBindingsTable({ bindings, loading }: { bindings: GatewayBinding[]; loading: boolean }) {
  return (
    <TableFrame>
      <Table>
        <TableHeader><TableRow><TableHead>Binding</TableHead><TableHead>Gateway → Agent</TableHead><TableHead>External context</TableHead><TableHead>Session mode</TableHead><TableHead>Last activity</TableHead><TableHead>Status</TableHead></TableRow></TableHeader>
        <TableBody>
          {loading ? <LoadingRows columns={6} /> : bindings.length === 0 ? <EmptyRow columns={6} text="No GatewayBindings are configured." /> : bindings.map((binding) => (
            <TableRow key={binding.metadata.name}>
              <TableCell>
                <Link
                  to="/gateways/bindings/$bindingId"
                  params={{ bindingId: binding.metadata.name }}
                  className="inline-flex items-center gap-1 font-mono text-xs font-medium text-primary hover:underline"
                >
                  {binding.metadata.name}
                  <ArrowUpRight className="h-3.5 w-3.5" aria-hidden="true" />
                </Link>
              </TableCell>
              <TableCell><span className="font-medium">{binding.spec.gatewayRef.name}</span> <span className="text-muted-foreground">→</span> {binding.spec.agentRef.name}</TableCell>
              <TableCell className="max-w-72 truncate font-mono text-xs">{binding.spec.match.accountId}/{binding.spec.match.contextId}{binding.spec.match.threadId ? `/${binding.spec.match.threadId}` : ''}</TableCell>
              <TableCell>{binding.spec.session?.mode || 'context'}</TableCell>
              <TableCell className="text-xs text-muted-foreground">{timeAgo(latestActivity(
                binding.status?.lastInboundActivity,
                binding.status?.lastOutboundActivity,
              ), { empty: 'Never' })}</TableCell>
              <TableCell><GatewayStatusBadge state={isGatewayResourceReady(binding) ? 'Ready' : 'NotReady'} /></TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableFrame>
  )
}

export function GatewayEventsTable({ events, loading }: { events: GatewayEvent[]; loading: boolean }) {
  return (
    <TableFrame>
      <Table>
        <TableHeader><TableRow><TableHead>Event</TableHead><TableHead>State</TableHead><TableHead>Context</TableHead><TableHead>Session</TableHead><TableHead>Task</TableHead><TableHead>Received</TableHead></TableRow></TableHeader>
        <TableBody>
          {loading ? <LoadingRows columns={6} /> : events.length === 0 ? <EmptyRow columns={6} text="No gateway events have been admitted." /> : events.map((event) => (
            <TableRow key={event.id}>
              <TableCell><div className="font-mono text-xs">{event.id}</div><div className="mt-1 max-w-56 truncate text-xs text-muted-foreground">{event.externalEventId}</div></TableCell>
              <TableCell><GatewayStatusBadge state={event.state} />{event.stateMessage && <p className="mt-1 max-w-52 text-xs text-muted-foreground">{event.stateMessage}</p>}</TableCell>
              <TableCell className="font-mono text-xs">{event.accountId}/{event.contextId}</TableCell>
              <TableCell>
                {event.sessionName ? (
                  <div className="space-y-1">
                    <div className="font-mono text-xs">{event.sessionName}</div>
                    <div className="flex items-center gap-1 text-[11px] text-muted-foreground" title="Gateway-managed Session history is protected from the generic Session surface">
                      <LockKeyhole className="h-3 w-3" aria-hidden="true" />
                      gateway-managed
                    </div>
                  </div>
                ) : '—'}
              </TableCell>
              <TableCell>{event.taskName ? <Link to="/tasks/$taskId" params={{ taskId: event.taskName }} className="font-mono text-xs hover:underline">{event.taskName}</Link> : '—'}</TableCell>
              <TableCell className="text-xs text-muted-foreground">{timeAgo(event.receivedAt, { empty: 'Never' })}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableFrame>
  )
}

export function GatewayDeliveriesTable({ deliveries, loading }: { deliveries: GatewayDelivery[]; loading: boolean }) {
  const retry = useRetryGatewayDelivery()
  return (
    <TableFrame>
      <Table>
        <TableHeader><TableRow><TableHead>Delivery</TableHead><TableHead>State</TableHead><TableHead>Kind</TableHead><TableHead>Correlation</TableHead><TableHead>Attempts</TableHead><TableHead>Updated</TableHead><TableHead className="w-16" /></TableRow></TableHeader>
        <TableBody>
          {loading ? <LoadingRows columns={7} /> : deliveries.length === 0 ? <EmptyRow columns={7} text="No adapter deliveries have been created." /> : deliveries.map((delivery) => (
            <TableRow key={delivery.id}>
              <TableCell><div className="font-mono text-xs">{delivery.id}</div>{delivery.providerMessageId && <div className="mt-1 max-w-56 truncate text-xs text-muted-foreground">{delivery.providerMessageId}</div>}</TableCell>
              <TableCell><GatewayStatusBadge state={delivery.state} />{delivery.lastError && <p className="mt-1 max-w-56 text-xs text-muted-foreground">{delivery.lastError}</p>}</TableCell>
              <TableCell>{delivery.kind}</TableCell>
              <TableCell><div className="font-mono text-xs">{delivery.eventId}</div>{delivery.taskName && <div className="mt-1 text-xs text-muted-foreground">{delivery.taskName}</div>}</TableCell>
              <TableCell className="font-mono text-xs tabular-nums">{delivery.attemptCount}/{delivery.maxAttempts}</TableCell>
              <TableCell className="text-xs text-muted-foreground">{timeAgo(delivery.updatedAt, { empty: 'Never' })}</TableCell>
              <TableCell>{['DeadLettered', 'Failed'].includes(delivery.state) && <Button size="icon" variant="ghost" aria-label={`Retry ${delivery.id}`} disabled={retry.isPending} onClick={() => retry.mutate(delivery.id)}><RotateCcw className="h-4 w-4" /></Button>}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableFrame>
  )
}

function TableFrame({ children }: { children: React.ReactNode }) {
  return <div className="overflow-hidden rounded-lg border bg-card">{children}</div>
}

function LoadingRows({ columns }: { columns: number }) {
  return Array.from({ length: 4 }).map((_, row) => <TableRow key={row}>{Array.from({ length: columns }).map((__, column) => <TableCell key={column}><Skeleton className="h-4 w-24" /></TableCell>)}</TableRow>)
}

function EmptyRow({ columns, text }: { columns: number; text: string }) {
  return <TableRow><TableCell colSpan={columns} className="py-12 text-center text-sm text-muted-foreground">{text}</TableCell></TableRow>
}

function latestActivity(inbound?: string, outbound?: string) {
  if (!inbound) return outbound
  if (!outbound) return inbound
  const inboundTime = new Date(inbound).getTime()
  const outboundTime = new Date(outbound).getTime()
  if (Number.isNaN(inboundTime)) return Number.isNaN(outboundTime) ? inbound : outbound
  if (Number.isNaN(outboundTime)) return inbound
  return outboundTime > inboundTime ? outbound : inbound
}
