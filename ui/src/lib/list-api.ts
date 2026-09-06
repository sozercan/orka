import { isForbiddenError } from '@/lib/api-client'

export interface ListMetadata {
  continue?: string
  remainingItemCount?: number
}

export interface ListResponse<T> {
  items: T[]
  metadata?: ListMetadata
  /** Set by walkAllPages when the bounded walk stopped before the collection ended. */
  truncated?: boolean
}

/** A completed walk always carries the last page's metadata and a truncation flag. */
export interface ListWalkResult<T> extends ListResponse<T> {
  metadata: ListMetadata
  truncated: boolean
}

// maxListWalkPages bounds every full-list walk: terminal objects accumulate
// without limit, and an unbounded walk on a polling interval would grow into
// an ever-larger request burst against the API server (and browser memory).
// Views built on these walks are summaries. Beyond the cap they receive a
// partial resource-key-ordered sample and must surface that truncation.
export const maxListWalkPages = 20

export interface WalkAllPagesOptions {
  /** List name used in the repeated-cursor error, e.g. "task list". */
  subject: string
  /** Optional request path appended to the repeated-cursor error. */
  path?: string
  /**
   * Page cap. Defaults to unbounded so selectors and inventories stay
   * exhaustive; summary views pass maxListWalkPages.
   */
  maxPages?: number
}

/**
 * Follows metadata.continue across pages, refusing to loop on a repeated
 * cursor and stopping after maxPages. `truncated` reports whether the walk
 * stopped with a continuation still outstanding.
 */
export async function walkAllPages<T>(
  fetchPage: (continueToken: string | undefined) => Promise<ListResponse<T>>,
  { subject, path, maxPages = Number.POSITIVE_INFINITY }: WalkAllPagesOptions,
): Promise<ListWalkResult<T>> {
  const items: T[] = []
  const seen = new Set<string>()
  let metadata: ListMetadata = {}
  let continueToken: string | undefined
  let pages = 0
  do {
    const page = await fetchPage(continueToken)
    items.push(...page.items)
    metadata = page.metadata ?? {}
    const next = metadata.continue || undefined
    if (next) {
      if (seen.has(next)) {
        throw new Error(`${subject} pagination repeated continuation cursor${path ? ` for ${path}` : ''}`)
      }
      seen.add(next)
    }
    continueToken = next
    pages += 1
  } while (continueToken && pages < maxPages)
  return { items, metadata, truncated: Boolean(continueToken) }
}

/** Query params for one page: the base params plus `continue` when a cursor is set. */
export function pageParams(params: Record<string, string>, continueToken?: string): Record<string, string> {
  return continueToken ? { ...params, continue: continueToken } : params
}

export function isPaginationProtocolError(error: unknown): boolean {
  return error instanceof Error && error.message.includes('repeated continuation cursor')
}

// A 403 is permanent for this identity: retrying it only generates denied
// requests and audit noise, so only other failures get the usual three tries.
export const retryUnlessForbidden = (failureCount: number, error: unknown) =>
  !isForbiddenError(error) && failureCount < 3

// A 403 will not clear on its own; polling it just spams the audit log.
export const pollUnlessForbidden = (interval: number | false) =>
  (query: { state: { error: unknown } }) => (isForbiddenError(query.state.error) ? false : interval)
