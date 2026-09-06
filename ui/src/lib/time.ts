/** Compact duration for table cells: 5s, 3m, 2h, 1d. Negative input clamps to 0. */
export function humanizeDuration(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds))
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.floor(s / 60)}m`
  if (s < 86400) return `${Math.floor(s / 3600)}h`
  return `${Math.floor(s / 86400)}d`
}

export interface TimeAgoOptions {
  /** Returned for a missing or unparseable timestamp. Defaults to "-". */
  empty?: string
  /** Drop the trailing " ago" (compact list columns). */
  compact?: boolean
}

/** Relative age of an RFC 3339 timestamp: "30s ago", "2h ago". */
export function timeAgo(ts?: string, { empty = '-', compact = false }: TimeAgoOptions = {}): string {
  if (!ts) return empty
  const ms = new Date(ts).getTime()
  if (Number.isNaN(ms)) return empty
  const duration = humanizeDuration((Date.now() - ms) / 1000)
  return compact ? duration : `${duration} ago`
}

export interface FormatTimestampOptions {
  /** Returned for a missing timestamp. Defaults to "Never". */
  empty?: string
  /** Force 12- or 24-hour clock; the locale default applies when omitted. */
  hour12?: boolean
}

/** Locale-formatted absolute timestamp; an unparseable value is shown as-is. */
export function formatTimestamp(ts?: string, { empty = 'Never', hour12 }: FormatTimestampOptions = {}): string {
  if (!ts) return empty
  const parsed = new Date(ts)
  if (Number.isNaN(parsed.getTime())) return ts
  return parsed.toLocaleString(undefined, hour12 === undefined ? undefined : { hour12 })
}
