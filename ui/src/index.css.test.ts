import { describe, it, expect } from 'vitest'
import { readFileSync } from 'node:fs'
import path from 'node:path'

/**
 * Phase 0 token invariants — parsed directly from index.css so the design
 * system's structural guarantees are enforced in CI, not just by eye.
 */
const css = readFileSync(
  path.resolve(__dirname, './index.css'),
  'utf8',
)

/** Extract the body of a top-level block (`@theme {…}` or `.dark {…}`). */
function block(selector: string): string {
  const start = css.indexOf(selector)
  expect(start, `selector ${selector} present`).toBeGreaterThan(-1)
  const open = css.indexOf('{', start)
  let depth = 0
  for (let i = open; i < css.length; i++) {
    if (css[i] === '{') depth++
    else if (css[i] === '}') {
      depth--
      if (depth === 0) return css.slice(open + 1, i)
    }
  }
  throw new Error(`unbalanced block for ${selector}`)
}

function tokenValue(body: string, name: string): string | undefined {
  const m = body.match(new RegExp(`${name.replace(/[-]/g, '\\-')}\\s*:\\s*([^;]+);`))
  return m?.[1].trim()
}

describe('index.css design tokens', () => {
  const themeBody = block('@theme')
  const darkBody = block('.dark')

  it('card surface is distinct from the page background in light theme', () => {
    const card = tokenValue(themeBody, '--color-card')
    const bg = tokenValue(themeBody, '--color-background')
    expect(card).toBeTruthy()
    expect(bg).toBeTruthy()
    expect(card).not.toBe(bg)
  })

  it('card surface is distinct from the page background in dark theme', () => {
    const card = tokenValue(darkBody, '--color-card')
    const bg = tokenValue(darkBody, '--color-background')
    expect(card).toBeTruthy()
    expect(bg).toBeTruthy()
    expect(card).not.toBe(bg)
  })

  it('Inter is still declared as the sans font', () => {
    expect(themeBody).toMatch(/--font-sans:\s*"Inter"/)
  })

  it('defines a monospace font token for machine data', () => {
    expect(tokenValue(themeBody, '--font-mono')).toMatch(/JetBrains Mono/)
  })

  it('defines the four phase status tokens in both themes', () => {
    for (const body of [themeBody, darkBody]) {
      for (const phase of ['pending', 'running', 'succeeded', 'failed']) {
        expect(
          tokenValue(body, `--color-status-${phase}`),
          `--color-status-${phase}`,
        ).toBeTruthy()
      }
    }
  })

  it('reserves a liveness token in both themes', () => {
    expect(tokenValue(themeBody, '--color-live')).toBeTruthy()
    expect(tokenValue(darkBody, '--color-live')).toBeTruthy()
  })

  it('defines task-type tokens in both themes', () => {
    for (const body of [themeBody, darkBody]) {
      for (const type of ['container', 'ai', 'agent']) {
        expect(
          tokenValue(body, `--color-type-${type}`),
          `--color-type-${type}`,
        ).toBeTruthy()
      }
    }
  })

  it('honors reduced motion globally', () => {
    expect(css).toMatch(/prefers-reduced-motion:\s*reduce/)
  })
})
