import { Clock3, LockKeyhole, Network } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import type { GatewayEvent } from '@/schemas/gateway'
import { GatewayStatusBadge } from './gateway-tables'
import { formatTimestamp } from '@/lib/time'

const ACTIVE_QUEUE_STATES = new Set<GatewayEvent['state']>([
  'Accepted',
  'Queued',
  'Dispatching',
  'TaskCreated',
])

interface VisibleSessionQueue {
  sessionName?: string
  events: GatewayEvent[]
}

export function GatewaySessionQueue({ events, loading }: { events: GatewayEvent[]; loading: boolean }) {
  const queues = buildVisibleSessionQueues(events)
  const activeRecords = queues.reduce((total, queue) => total + queue.events.length, 0)
  const assignedSessions = queues.filter((queue) => queue.sessionName).length

  return (
    <Card className="overflow-hidden">
      <CardHeader className="gap-3 border-b bg-muted/20 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-1.5">
          <CardTitle className="flex items-center gap-2">
            <Network className="h-5 w-5 text-primary" />
            Session FIFO view
          </CardTitle>
          <p className="max-w-3xl text-sm text-muted-foreground">
            Active records derived from the events loaded on this ledger page. Terminal records are omitted;
            counts and ordering may be partial across pages and are not namespace totals.
          </p>
        </div>
        <Badge variant="outline" className="shrink-0 font-mono text-[10px] uppercase tracking-[0.14em]">
          Current page only
        </Badge>
      </CardHeader>
      <CardContent className="space-y-4 p-4">
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-xs text-muted-foreground">
          <span><strong className="font-mono text-foreground">{events.length}</strong> events loaded</span>
          <span><strong className="font-mono text-foreground">{activeRecords}</strong> active records</span>
          <span><strong className="font-mono text-foreground">{assignedSessions}</strong> Sessions visible</span>
        </div>

        {loading ? (
          <div className="grid gap-3 lg:grid-cols-2">
            {Array.from({ length: 2 }).map((_, index) => <Skeleton key={index} className="h-48 rounded-lg" />)}
          </div>
        ) : queues.length === 0 ? (
          <div className="rounded-lg border border-dashed px-6 py-12 text-center text-sm text-muted-foreground">
            No active Session queue records are visible on this event page.
          </div>
        ) : (
          <div className="grid gap-3 lg:grid-cols-2">
            {queues.map((queue) => {
              const label = queue.sessionName ?? 'Awaiting Session assignment'
              return (
                <section
                  key={queue.sessionName ?? '__unassigned__'}
                  aria-label={`${label} visible FIFO queue`}
                  className="overflow-hidden rounded-lg border bg-card"
                >
                  <div className="flex items-start justify-between gap-3 border-b bg-muted/20 px-4 py-3">
                    <div className="min-w-0 space-y-1">
                      <div className="flex items-center gap-2">
                        <LockKeyhole className="h-4 w-4 shrink-0 text-primary" />
                        <h3 className="truncate font-mono text-sm font-semibold">{label}</h3>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        {queue.sessionName ? 'Gateway-managed history · oldest visible event first' : 'Events not yet assigned to a Session'}
                      </p>
                    </div>
                    <Badge variant="secondary" className="shrink-0 font-mono tabular-nums">
                      {queue.events.length} visible
                    </Badge>
                  </div>
                  <ol className="divide-y">
                    {queue.events.map((event, index) => (
                      <li key={event.id} className="grid grid-cols-[2.25rem_1fr] gap-3 px-4 py-3">
                        <div className="pt-0.5 font-mono text-xs text-muted-foreground" aria-label={`Visible queue position ${index + 1}`}>
                          {String(index + 1).padStart(2, '0')}
                        </div>
                        <div className="min-w-0 space-y-2">
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <span className="truncate font-mono text-xs font-medium">{event.externalEventId}</span>
                            <GatewayStatusBadge state={event.state} />
                          </div>
                          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
                            <span className="font-mono">{event.accountId}/{event.contextId}</span>
                            {hasTranscriptOrder(event) && <span className="font-mono">Session order {event.transcriptOrder}</span>}
                            {event.bindingName && <span>Binding {event.bindingName}</span>}
                            <span className="inline-flex items-center gap-1">
                              <Clock3 className="h-3.5 w-3.5" />
                              {formatTimestamp(event.receivedAt, { empty: '' })}
                            </span>
                          </div>
                        </div>
                      </li>
                    ))}
                  </ol>
                </section>
              )
            })}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function buildVisibleSessionQueues(events: GatewayEvent[]): VisibleSessionQueue[] {
  const grouped = new Map<string, VisibleSessionQueue>()
  for (const event of events) {
    if (!ACTIVE_QUEUE_STATES.has(event.state)) continue
    const key = event.sessionName || '__unassigned__'
    const queue = grouped.get(key) ?? { sessionName: event.sessionName, events: [] }
    queue.events.push(event)
    grouped.set(key, queue)
  }

  return Array.from(grouped.values())
    .map((queue) => ({ ...queue, events: [...queue.events].sort(compareEvents) }))
    .sort((left, right) => compareEvents(left.events[0], right.events[0]))
}

function compareEvents(left: GatewayEvent, right: GatewayEvent) {
  const leftHasOrder = hasTranscriptOrder(left)
  const rightHasOrder = hasTranscriptOrder(right)
  if (leftHasOrder !== rightHasOrder) return leftHasOrder ? -1 : 1
  if (leftHasOrder && rightHasOrder && left.transcriptOrder !== right.transcriptOrder) {
    return left.transcriptOrder! - right.transcriptOrder!
  }
  const timeDifference = timestampValue(left.receivedAt) - timestampValue(right.receivedAt)
  if (timeDifference !== 0) return timeDifference
  return left.id.localeCompare(right.id)
}

function hasTranscriptOrder(event: GatewayEvent): event is GatewayEvent & { transcriptOrder: number } {
  return typeof event.transcriptOrder === 'number' && Number.isFinite(event.transcriptOrder)
}

function timestampValue(value: string) {
  const timestamp = new Date(value).getTime()
  return Number.isNaN(timestamp) ? Number.MAX_SAFE_INTEGER : timestamp
}

