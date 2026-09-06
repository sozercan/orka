import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import type { PatchProposal } from '@/schemas/security'
import { timeAgo } from '@/lib/time'

export function PatchProposalCard({ proposal }: { proposal: PatchProposal }) {
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0">
        <CardTitle className="text-base">{proposal.branch}</CardTitle>
        <Badge variant={proposal.status === 'succeeded' || proposal.status === 'pr_opened' ? 'default' : 'secondary'}>
          {proposal.status}
        </Badge>
      </CardHeader>
      <CardContent className="space-y-2 text-sm">
        <div>Task: <span className="font-medium">{proposal.taskName}</span></div>
        <div>Created: <span className="font-medium">{timeAgo(proposal.createdAt)}</span></div>
        {proposal.reason && <div className="text-destructive">Reason: {proposal.reason}</div>}
        {proposal.diffArtifact && <div>Diff artifact: <span className="font-mono text-xs">{proposal.diffArtifact}</span></div>}
        {proposal.summaryArtifact && <div>Summary artifact: <span className="font-mono text-xs">{proposal.summaryArtifact}</span></div>}
        {proposal.prURL && (
          <a href={proposal.prURL} target="_blank" rel="noreferrer" className="text-primary hover:underline">
            Open PR #{proposal.prNumber}
          </a>
        )}
      </CardContent>
    </Card>
  )
}
