import { Link } from '@tanstack/react-router'
import { Shield, Plus, RefreshCcw } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { ListAccessError } from '@/components/ui/list-access-error'
import { PageHeader } from '@/components/layout/page-header'
import { useRepositoryScans, useRunSecurityScan } from '@/hooks/use-security'
import type { RepositoryScan } from '@/schemas/security'
import { repositoryDisplayName } from '@/lib/repository-name'
import { timeAgo } from '@/lib/time'

export function RepositoryList() {
  const { data, isLoading, error } = useRepositoryScans()
  const repositories = error ? [] : (data?.items ?? [])

  return (
    <div className="space-y-4">
      <PageHeader
        title="Security"
        description="Repository security scans, threat models, and findings"
        action={
          <Link to="/security/new">
            <Button><Plus className="mr-2 h-4 w-4" />New Repository</Button>
          </Link>
        }
      />

      {isLoading ? (
        <div className="grid gap-4 md:grid-cols-2">
          {Array.from({ length: 4 }).map((_, index) => (
            <Card key={index}>
              <CardHeader><Skeleton className="h-6 w-48" /></CardHeader>
              <CardContent className="space-y-2">
                <Skeleton className="h-4 w-full" />
                <Skeleton className="h-4 w-2/3" />
              </CardContent>
            </Card>
          ))}
        </div>
      ) : error ? (
        <Card><CardContent className="pt-6"><ListAccessError error={error} resource="security repositories" /></CardContent></Card>
      ) : repositories.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            No repositories configured for security scanning yet.
          </CardContent>
        </Card>
      ) : (
        <div className="grid gap-4 md:grid-cols-2">
          {repositories.map((repo) => (
            <RepositoryCard key={repo.metadata.name} repo={repo} />
          ))}
        </div>
      )}
    </div>
  )
}

function RepositoryCard({ repo }: { repo: RepositoryScan }) {
  const runScan = useRunSecurityScan(repo.metadata.name)
  const lastScanAt = repo.status?.lastScanAt ?? repo.status?.lastSuccessfulScanAt
  const displayName = repositoryDisplayName(repo.spec, repo.metadata.name)

  return (
    <Card className="transition-colors hover:border-primary/50">
      <CardHeader className="flex flex-row items-start justify-between gap-3 space-y-0">
        <div className="space-y-1">
          <CardTitle className="flex items-center gap-2 text-lg">
            <Shield className="h-5 w-5 text-primary" />
            <Link to="/security/$repoId" params={{ repoId: repo.metadata.name }} className="hover:underline">
              {displayName.split('/').pop() || repo.metadata.name}
            </Link>
          </CardTitle>
          <p className="text-sm text-muted-foreground">{displayName} · {repo.spec.branch || 'main'}</p>
        </div>
        <Badge variant={repo.status?.phase === 'Ready' ? 'default' : 'secondary'}>
          {repo.status?.phase || 'Pending'}
        </Badge>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="text-sm text-muted-foreground">{repo.spec.repoURL}</div>
        <div className="grid gap-2 text-sm md:grid-cols-2">
          <div>Open findings: <span className="font-medium text-foreground">{repo.status?.findingCounts?.total ?? 0}</span></div>
          <div>Last scan: <span className="font-medium text-foreground">{timeAgo(lastScanAt, { empty: 'Never' })}</span></div>
        </div>
        <div className="flex flex-wrap gap-2 text-xs">
          <Badge variant="destructive">{repo.status?.findingCounts?.critical ?? 0} critical</Badge>
          <Badge variant="destructive">{repo.status?.findingCounts?.high ?? 0} high</Badge>
          <Badge variant="secondary">{repo.status?.findingCounts?.medium ?? 0} medium</Badge>
          <Badge variant="outline">{repo.status?.findingCounts?.low ?? 0} low</Badge>
        </div>
        <div className="flex items-center justify-between">
          <Link to="/security/$repoId" params={{ repoId: repo.metadata.name }}>
            <Button variant="outline">Open</Button>
          </Link>
          <Button variant="secondary" onClick={() => runScan.mutate()} disabled={runScan.isPending}>
            <RefreshCcw className="mr-2 h-4 w-4" />
            Scan Now
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
