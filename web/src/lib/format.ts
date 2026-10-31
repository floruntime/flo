/* Formatting + seeded-random helpers shared across screens (ported from the design). */

/** Deterministic seeded PRNG — stable mock data across renders. */
export function crng(seed: number): () => number {
  let s = seed
  return () => (s = (s * 9301 + 49297) % 233280) / 233280
}

/** Compact number: 1.2M / 3.4K / 1,234 */
export function cfmt(n: number): string {
  return n >= 1e6
    ? (n / 1e6).toFixed(1) + 'M'
    : n >= 1e3
      ? (n / 1e3).toFixed(1) + 'K'
      : Math.round(n).toLocaleString()
}

/** Timestamp → "02 Jun 09:41:12" */
export function tfmt(ms: number): string {
  return new Date(ms).toLocaleString('en-GB', {
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

/** JSON syntax highlight → HTML string (used with dangerouslySetInnerHTML in <pre class="code">). */
export function jhl(obj: unknown): string {
  const j = JSON.stringify(obj, null, 2).replace(/&/g, '&amp;').replace(/</g, '&lt;')
  return j
    .replace(/"(\w[\w:.-]*)":/g, '<span class="jk">"$1"</span>:')
    .replace(/: "([^"]*)"/g, ': <span class="js">"$1"</span>')
    .replace(/: (\d+\.?\d*)/g, ': <span class="jn">$1</span>')
}

/** What a field the API answers as null (unknown, not zero) renders as. */
export const DASH = '—'

/** `fmt(v)`, or "—" when the API sent null for it. */
export function orDash<T>(v: T | null | undefined, fmt: (x: T) => string | number = String): string | number {
  return v == null ? DASH : fmt(v)
}

/** The sum of `vs`, or null if any is unknown: an unknown never counts as 0. */
export function sumKnown(vs: (number | null | undefined)[]): number | null {
  let t = 0
  for (const v of vs) {
    if (v == null) return null
    t += v
  }
  return t
}
