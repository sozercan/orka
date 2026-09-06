/**
 * Derives the "owner/repo" label for a RepositoryScan from its repoURL. The
 * API only stores repoURL (owner/repository are never populated), so the
 * label is parsed from the URL and falls back to the resource name.
 */
export function repositoryDisplayName(spec: { repoURL?: string; owner?: string; repository?: string }, fallback: string): string {
  if (spec.owner && spec.repository) return `${spec.owner}/${spec.repository}`
  const parsed = parseRepositoryURL(spec.repoURL)
  if (parsed) return `${parsed.owner}/${parsed.repository}`
  return fallback
}

export function parseRepositoryURL(repoURL?: string): { owner: string; repository: string } | undefined {
  const raw = (repoURL ?? '').trim()
  if (!raw) return undefined
  let path = raw
  const scpLike = /^[^/@:]+@[^/:]+:(.+)$/.exec(raw)
  if (scpLike) {
    path = scpLike[1]
  } else {
    try {
      path = new URL(raw).pathname
    } catch {
      path = raw
    }
  }
  const segments = path.split('/').filter(Boolean)
  if (segments.length < 2) return undefined
  const owner = segments[segments.length - 2]
  const repository = segments[segments.length - 1].replace(/\.git$/i, '')
  if (!owner || !repository) return undefined
  return { owner, repository }
}
