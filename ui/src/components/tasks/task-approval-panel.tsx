import { useState } from 'react'
import { toast } from 'sonner'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { EmptyState } from '@/components/ui/empty-state'
import { ShieldCheck, Check, X, Clock, RefreshCw } from 'lucide-react'
import { useTaskApprovals, useDecideApproval } from '@/hooks/use-execution-events'
import { ApiError } from '@/lib/api-client'
import type { Approval } from '@/schemas/execution-event'
import type { TaskPhase } from '@/schemas/task'
import { formatTimestamp } from '@/lib/time'

function statusStyle(status: string): { className: string; live: boolean; label: string } {
  switch (status) {
    case 'pending':
      return { className: 'text-status-running', live: true, label: 'Pending' }
    case 'approved':
      return { className: 'text-status-succeeded', live: false, label: 'Approved' }
    case 'declined':
      return { className: 'text-status-failed', live: false, label: 'Declined' }
    case 'expired':
      return { className: 'text-status-pending', live: false, label: 'Expired' }
    case 'cancelled':
      return { className: 'text-muted-foreground', live: false, label: 'Cancelled' }
    default:
      return { className: 'text-muted-foreground', live: false, label: status || 'unknown' }
  }
}

function ApprovalStatusBadge({ status }: { status: string }) {
  const { className, live, label } = statusStyle(status)
  return (
    <Badge variant="outline" className={`gap-1.5 ${className}`}>
      <span
        aria-hidden="true"
        className={`inline-block size-1.5 rounded-full bg-current ${live ? 'motion-safe:animate-pulse-live' : ''}`}
      />
      {label}
    </Badge>
  )
}

function ApprovalCard({
  approval,
  taskId,
  taskTerminal,
  onConflict,
}: {
  approval: Approval
  taskId: string
  // Whether the owning task has reached a terminal phase. The backend rejects
  // decisions on terminal tasks (409), so such approvals are non-actionable even
  // if still derived as pending.
  taskTerminal?: boolean
  // Called when a decision returns 409 so the panel can refetch settled state.
  onConflict?: () => void
}) {
  const decide = useDecideApproval(taskId)
  const [reason, setReason] = useState('')
  const [conflict, setConflict] = useState<string | null>(null)
  const isPending = approval.status === 'pending'
  // A pending approval on a completed task can't be decided — show it read-only.
  const actionable = isPending && !taskTerminal
  const submitting = decide.isPending

  async function submit(decision: 'approve' | 'decline') {
    if (submitting) return
    setConflict(null)
    try {
      const updated = await decide.mutateAsync({
        approvalId: approval.id,
        decision,
        reason: reason.trim() || undefined,
      })
      toast.success(`Approval ${updated.status}`)
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        // The approval changed underneath us (decided elsewhere, expired, or
        // cancelled). Surface it and refetch so the card reflects the settled
        // server state instead of waiting for the next poll — useDecideApproval
        // only invalidates on success, so we trigger the refresh here.
        setConflict(err.message || 'This approval was already decided.')
        toast.error('Approval already decided')
        onConflict?.()
      } else {
        const message = err instanceof Error ? err.message : 'Unknown error'
        setConflict(message)
        toast.error(`Failed to record decision: ${message}`)
      }
    }
  }

  return (
    <Card data-testid="approval-card">
      <CardContent className="space-y-3 pt-6">
        <div className="flex flex-wrap items-center gap-2">
          <ApprovalStatusBadge status={approval.status} />
          <span className="font-medium">{approval.action || 'Action'}</span>
          {approval.toolCallID && (
            <Badge variant="outline" className="font-mono text-[10px]" title={approval.toolCallID}>
              call: {approval.toolCallID.length > 12 ? approval.toolCallID.slice(0, 12) + '…' : approval.toolCallID}
            </Badge>
          )}
          <span className="ml-auto font-mono text-xs text-muted-foreground">{approval.id}</span>
        </div>

        {approval.riskSummary && (
          <p className="break-words text-sm text-muted-foreground">{approval.riskSummary}</p>
        )}

        <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
          {approval.createdAt && (
            <span>Requested <span className="tabular-nums">{formatTimestamp(approval.createdAt, { empty: '', hour12: false })}</span></span>
          )}
          {approval.expiresAt && (
            <span className="inline-flex items-center gap-1">
              <Clock className="h-3 w-3" aria-hidden="true" />
              Expires <span className="tabular-nums">{formatTimestamp(approval.expiresAt, { empty: '', hour12: false })}</span>
            </span>
          )}
          {approval.timeout && <span>Timeout {approval.timeout}</span>}
        </div>

        {/* Terminal decision details. */}
        {!isPending && (approval.decisionActor || approval.decisionReason || approval.decisionTime) && (
          <div className="rounded-md bg-muted px-3 py-2 text-xs">
            {approval.decisionActor && <div>Decided by <span className="font-medium">{approval.decisionActor}</span></div>}
            {approval.decisionTime && <div className="tabular-nums text-muted-foreground">{formatTimestamp(approval.decisionTime, { empty: '', hour12: false })}</div>}
            {approval.decisionReason && <div className="mt-1 break-words">{approval.decisionReason}</div>}
          </div>
        )}

        {conflict && (
          <p className="rounded-md border border-status-pending/40 bg-status-pending-bg px-3 py-2 text-xs text-status-pending">
            {conflict}
          </p>
        )}

        {isPending && taskTerminal && (
          <p className="rounded-md bg-muted px-3 py-2 text-xs text-muted-foreground">
            This approval can no longer be decided because the task has completed.
          </p>
        )}

        {actionable && (
          <div className="space-y-2">
            <input
              type="text"
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              placeholder="Optional reason…"
              className="h-9 w-full rounded-md border bg-background px-3 text-sm"
              aria-label="Decision reason"
              disabled={submitting}
            />
            <div className="flex gap-2">
              <Button
                size="sm"
                onClick={() => submit('approve')}
                disabled={submitting}
                aria-label="Approve"
              >
                <Check className="mr-1 h-4 w-4" /> Approve
              </Button>
              <Button
                size="sm"
                variant="destructive"
                onClick={() => submit('decline')}
                disabled={submitting}
                aria-label="Decline"
              >
                <X className="mr-1 h-4 w-4" /> Decline
              </Button>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

export function TaskApprovalPanel({ taskId, taskPhase, taskUid }: { taskId: string; taskPhase?: TaskPhase; taskUid?: string }) {
  // Poll while approvals are pending or the task is still running, so a live
  // ApprovalRequested surfaces even if the panel opened before any existed.
  const taskRunning = taskPhase === 'Running' || taskPhase === 'Pending'
  // The backend rejects decisions on terminal tasks, so their pending approvals
  // render read-only — and polling them is pointless since no event will flip
  // their status. Gate both on taskTerminal.
  const taskTerminal = taskPhase === 'Succeeded' || taskPhase === 'Failed' || taskPhase === 'Cancelled'
  const { data, isLoading, error, refetch } = useTaskApprovals(taskId, true, 5000, taskRunning, taskTerminal, taskUid)
  const approvals = data?.approvals ?? []
  const pending = approvals.filter((a) => a.status === 'pending')

  if (isLoading) {
    return (
      <Card>
        <CardContent className="space-y-3 pt-6">
          <Skeleton className="h-6 w-40" />
          <Skeleton className="h-24 w-full" />
        </CardContent>
      </Card>
    )
  }

  if (error) {
    const notImplemented = error instanceof ApiError && error.status === 501
    return (
      <Card>
        <CardContent className="pt-6">
          <EmptyState
            icon={ShieldCheck}
            headline={
              notImplemented
                ? 'Execution event storage is not enabled on this server.'
                : 'Failed to load approvals.'
            }
            action={
              !notImplemented && (
                <Button variant="outline" size="sm" onClick={() => refetch()}>
                  <RefreshCw className="mr-1 h-3 w-3" /> Retry
                </Button>
              )
            }
          />
        </CardContent>
      </Card>
    )
  }

  if (approvals.length === 0) {
    return (
      <Card>
        <CardContent className="pt-6">
          <EmptyState
            icon={ShieldCheck}
            headline="No approvals requested."
            hint="High-risk actions that require human approval will appear here."
          />
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-3" data-testid="approval-panel">
      <div className="flex items-center gap-2">
        <h2 className="text-base font-semibold">Approvals</h2>
        {pending.length > 0 && (
          <Badge variant="outline" className="gap-1.5 text-status-running">
            <span aria-hidden="true" className="inline-block size-1.5 rounded-full bg-current motion-safe:animate-pulse-live" />
            {pending.length} pending
          </Badge>
        )}
      </div>
      {approvals.map((approval) => (
        <ApprovalCard key={approval.id} approval={approval} taskId={taskId} taskTerminal={taskTerminal} onConflict={() => refetch()} />
      ))}
    </div>
  )
}
