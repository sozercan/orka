import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { formatTimestamp, humanizeDuration, timeAgo } from './time'

describe('humanizeDuration', () => {
  it('picks the largest whole unit', () => {
    expect(humanizeDuration(0)).toBe('0s')
    expect(humanizeDuration(59)).toBe('59s')
    expect(humanizeDuration(60)).toBe('1m')
    expect(humanizeDuration(3599)).toBe('59m')
    expect(humanizeDuration(3600)).toBe('1h')
    expect(humanizeDuration(86399)).toBe('23h')
    expect(humanizeDuration(86400)).toBe('1d')
    expect(humanizeDuration(172800)).toBe('2d')
  })

  it('clamps negative and fractional input', () => {
    expect(humanizeDuration(-5)).toBe('0s')
    expect(humanizeDuration(90.9)).toBe('1m')
  })
})

describe('timeAgo', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T12:00:00Z'))
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('formats elapsed time with an "ago" suffix', () => {
    expect(timeAgo('2026-01-01T11:59:30Z')).toBe('30s ago')
    expect(timeAgo('2026-01-01T11:58:00Z')).toBe('2m ago')
    expect(timeAgo('2026-01-01T10:00:00Z')).toBe('2h ago')
    expect(timeAgo('2025-12-30T12:00:00Z')).toBe('2d ago')
  })

  it('omits the suffix in compact mode', () => {
    expect(timeAgo('2026-01-01T11:58:00Z', { compact: true })).toBe('2m')
  })

  it('clamps timestamps from the future to zero', () => {
    expect(timeAgo('2026-01-01T12:05:00Z')).toBe('0s ago')
  })

  it('returns the empty placeholder for missing or unparseable input', () => {
    expect(timeAgo(undefined)).toBe('-')
    expect(timeAgo('')).toBe('-')
    expect(timeAgo('not a date')).toBe('-')
    expect(timeAgo(undefined, { empty: 'Never' })).toBe('Never')
  })
})

describe('formatTimestamp', () => {
  it('formats a parseable timestamp with the locale', () => {
    const ts = '2026-01-01T12:00:00Z'
    expect(formatTimestamp(ts)).toBe(new Date(ts).toLocaleString())
    expect(formatTimestamp(ts, { hour12: false })).toBe(new Date(ts).toLocaleString(undefined, { hour12: false }))
  })

  it('returns the placeholder for a missing value', () => {
    expect(formatTimestamp(undefined)).toBe('Never')
    expect(formatTimestamp('', { empty: '' })).toBe('')
  })

  it('shows an unparseable value verbatim', () => {
    expect(formatTimestamp('yesterday-ish')).toBe('yesterday-ish')
  })
})
