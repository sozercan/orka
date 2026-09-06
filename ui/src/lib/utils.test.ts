import { describe, it, expect } from 'vitest'
import { cn, humanizeCamelCase } from './utils'

describe('cn', () => {
  it('merges class names', () => {
    expect(cn('foo', 'bar')).toBe('foo bar')
  })

  it('merges multiple class names', () => {
    expect(cn('a', 'b', 'c')).toBe('a b c')
  })

  it('handles empty arguments', () => {
    expect(cn()).toBe('')
  })

  it('handles undefined/null/false values', () => {
    expect(cn('a', undefined, null, false, 'b')).toBe('a b')
  })

  it('resolves Tailwind conflicts', () => {
    expect(cn('p-4', 'p-2')).toBe('p-2')
  })

  it('works with conditional classes via clsx', () => {
    expect(cn('base', { active: true, hidden: false })).toBe('base active')
  })
})

describe('humanizeCamelCase', () => {
  it('splits camelCase into lowercase words', () => {
    expect(humanizeCamelCase('threadReplies')).toBe('thread replies')
    expect(humanizeCamelCase('lastInboundActivity')).toBe('last inbound activity')
  })

  it('leaves single words alone', () => {
    expect(humanizeCamelCase('reactions')).toBe('reactions')
  })
})
