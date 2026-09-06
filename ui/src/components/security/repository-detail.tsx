import { toast } from 'sonner'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { PageHeader } from '@/components/layout/page-header'
import { useAllFindings, useDroppedFindings, useRepositoryScan, useReviewSlices, useRunSecurityScan, useScanRuns } from '@/hooks/use-security'
import { ThreatModelEditor } from './threat-model-editor'
import { RecommendedFindings } from './recommended-findings'
import { FindingTable } from './finding-table'
import { repositoryDisplayName } from '@/lib/repository-name'
import { timeAgo } from '@/lib/time'

export function RepositoryDetail({ repositoryName }: { repositoryName: string }) {
  const { data: repo, isLoading } = useRepositoryScan(repositoryName)
  const findings = useAllFindings(repositoryName)
  const scanRuns = useScanRuns(repositoryName)
  const reviewSlices = useReviewSlices(repositoryName)
  const droppedFindings = useDroppedFindings(repositoryName, repo?.status?.lastScanID)
  const runScan = useRunSecurityScan(repositoryName)
  const latestDropped = droppedFindings.data?.items ?? []
  const droppedByLayer = latestDropped.reduce<Record<string, number>>((acc, item) => {
    const layer = item.layer || 'unknown'
    acc[layer] = (acc[layer] ?? 0) + 1
    return acc
  }, {})
  const topDroppedReasons = Object.entries(latestDropped.reduce<Record<string, number>>((acc, item) => {
    acc[item.reason] = (acc[item.reason] ?? 0) + 1
    return acc
  }, {})).sort((a, b) => b[1] - a[1]).slice(0, 3)

  if (isLoading) {
    return <Skeleton className="h-96 w-full" />
  }

  if (!repo) {
    return <div className="text-muted-foreground">Repository not found.</div>
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title={repositoryDisplayName(repo.spec, repo.metadata.name)}
        description={repo.spec.repoURL}
        action={
          <>
            <Badge variant={repo.status?.phase === 'Ready' ? 'default' : 'secondary'}>{repo.status?.phase || 'Pending'}</Badge>
            <Button
              onClick={async () => {
                try {
                  await runScan.mutateAsync()
                  toast.success('Manual scan started')
                } catch (error) {
                  toast.error(`Failed to start scan: ${error instanceof Error ? error.message : 'Unknown error'}`)
                }
              }}
              disabled={runScan.isPending}
            >
              Scan Now
            </Button>
          </>
        }
      />

      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardHeader><CardTitle className="text-base">Branch</CardTitle></CardHeader>
          <CardContent>{repo.spec.branch || 'main'}</CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-base">Open Findings</CardTitle></CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{repo.status?.findingCounts?.total ?? 0}</div>
            <div className="mt-2 flex flex-wrap gap-2 text-xs">
              <Badge variant="destructive">{repo.status?.findingCounts?.critical ?? 0} critical</Badge>
              <Badge variant="destructive">{repo.status?.findingCounts?.high ?? 0} high</Badge>
              <Badge variant="secondary">{repo.status?.findingCounts?.medium ?? 0} medium</Badge>
              <Badge variant="outline">{repo.status?.findingCounts?.low ?? 0} low</Badge>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-base">Last Successful Scan</CardTitle></CardHeader>
          <CardContent>{timeAgo(repo.status?.lastSuccessfulScanAt, { empty: 'Never' })}</CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-base">Threat Model Version</CardTitle></CardHeader>
          <CardContent>{repo.status?.threatModelVersion ?? 0}</CardContent>
        </Card>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardHeader><CardTitle className="text-base">Review Slices</CardTitle></CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{reviewSlices.data?.items?.length ?? 0}</div>
            <div className="mt-1 text-xs text-muted-foreground">Deterministic repository review units</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-base">Accepted Output</CardTitle></CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{scanRuns.data?.items?.[0]?.acceptedFindings ?? 0}</div>
            <div className="mt-1 text-xs text-muted-foreground">Latest scan v2 findings accepted</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-base">Dropped Output</CardTitle></CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{droppedFindings.data?.items?.length ?? scanRuns.data?.items?.[0]?.droppedFindings ?? 0}</div>
            <div className="mt-1 text-xs text-muted-foreground">Rejected model findings with diagnostics</div>
            <div className="mt-2 flex flex-wrap gap-1 text-xs">
              {Object.entries(droppedByLayer).map(([layer, count]) => (
                <Badge key={layer} variant="outline">{count} {layer}</Badge>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {latestDropped.length > 0 && (
        <Card>
          <CardHeader><CardTitle>Scan Quality Diagnostics</CardTitle></CardHeader>
          <CardContent className="space-y-3 text-sm">
            <div className="text-muted-foreground">Latest scan dropped findings by validation, filter, or cap layer. Samples are sanitized server-side.</div>
            <div className="grid gap-2 md:grid-cols-3">
              {latestDropped.slice(0, 6).map((item) => (
                <div key={item.id} className="rounded-md border border-border p-2">
                  <div className="flex items-center justify-between gap-2">
                    <Badge variant="secondary">{item.layer || 'unknown'}</Badge>
                    <span className="text-xs text-muted-foreground">{timeAgo(item.createdAt, { empty: 'Never' })}</span>
                  </div>
                  <div className="mt-2 text-xs text-muted-foreground">{item.reason}</div>
                </div>
              ))}
            </div>
            {topDroppedReasons.length > 0 && (
              <div className="flex flex-wrap gap-2 text-xs">
                {topDroppedReasons.map(([reason, count]) => (
                  <Badge key={reason} variant="outline">{count}× {reason}</Badge>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      )}

      <ThreatModelEditor repositoryName={repositoryName} />
      <RecommendedFindings repositoryName={repositoryName} />

      <Card>
        <CardHeader>
          <CardTitle>All Findings</CardTitle>
        </CardHeader>
        <CardContent>
          <FindingTable findings={findings.data?.items ?? []} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Recent Scan Runs</CardTitle>
        </CardHeader>
        <CardContent>
          {scanRuns.isLoading ? (
            <Skeleton className="h-24 w-full" />
          ) : (scanRuns.data?.items ?? []).length === 0 ? (
            <div className="text-sm text-muted-foreground">No scan runs yet.</div>
          ) : (
            <div className="space-y-3">
              {(scanRuns.data?.items ?? []).map((run) => (
                <div key={run.id} className="rounded-md border border-border p-3 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <div className="font-medium">{run.mode}</div>
                    <Badge variant={run.phase === 'succeeded' ? 'default' : 'secondary'}>{run.phase}</Badge>
                  </div>
                  <div className="mt-1 text-muted-foreground">{run.summary || run.taskName}</div>
                  <div className="mt-2 text-xs text-muted-foreground">
                    Started {timeAgo(run.startedAt, { empty: 'Never' })} · Commits {run.commitCount ?? 0} · Slices {run.sliceCount ?? 0} · Accepted {run.acceptedFindings ?? 0} · Dropped {run.droppedFindings ?? 0} · Policy {run.scannerPolicyVersion || 'default'}
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
