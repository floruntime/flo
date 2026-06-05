/* Shared helpers for the Compute screens (Actions + Workers).
   Presentation-only constants/formatters live here (relocated out of the deleted
   mock module) so both live screens can share them. */
import type { ReactNode } from 'react'
import { StatusPill } from '@/components'

/* ---- section-header icon paths (ASEC in the design) ---- */
export const ASEC: Record<string, string> = {
  zap: 'M13 3 5 13h6l-1 8 8-10h-6z',
  cpu: 'M8.5 8.5h7v7h-7z|M10 3v2.5M14 3v2.5M10 18.5V21M14 18.5V21M3 10h2.5M3 14h2.5M18.5 10H21M18.5 14H21',
  list: 'M9 6h11|M9 12h11|M9 18h11|M4.5 6h.01|M4.5 12h.01|M4.5 18h.01',
  info: 'M12 3.5a8.5 8.5 0 1 0 0 17 8.5 8.5 0 0 0 0-17z|M12 11v5M12 8h.01',
}

/* ---- status colour maps ([color, label]) ---- */
export const A_RUNST: Record<string, [string, string]> = {
  pending: ['var(--tx-faint)', 'pending'],
  running: ['#6F9BD1', 'running'],
  completed: ['var(--accent)', 'completed'],
  failed: ['var(--crit)', 'failed'],
  cancelled: ['var(--tx-faint)', 'cancelled'],
  timed_out: ['var(--warn)', 'timed out'],
}

export const W_ST: Record<string, [string, string]> = {
  active: ['var(--accent)', 'active'],
  idle: ['var(--tx-3)', 'idle'],
  draining: ['var(--warn)', 'draining'],
  unhealthy: ['var(--crit)', 'unhealthy'],
}

/** Worker-type tag colour (TypeTag in the design). */
export function typeColor(t: string): string {
  return t === 'stream' ? '#6F9BD1' : 'var(--tx-3)'
}

/* ---- formatting helpers ---- */
/** Compact number: 1.2M / 3.4K / 1,234 */
export function afmtN(n: number): string {
  return n >= 1e6
    ? (n / 1e6).toFixed(1) + 'M'
    : n >= 1e3
      ? (n / 1e3).toFixed(1) + 'K'
      : Math.round(n).toLocaleString()
}

/** Seconds-ago → "12s ago" / "4m ago" / "2h ago" */
export function aAgo(s: number): string {
  return s < 60 ? s + 's ago' : s < 3600 ? Math.floor(s / 60) + 'm ago' : Math.floor(s / 3600) + 'h ago'
}

/** Milliseconds-timestamp → integer seconds ago from now (clamped at 0). */
export function secsAgo(ms: number): number {
  if (!ms) return 0
  return Math.max(0, Math.round((Date.now() - ms) / 1000))
}

/** Section header with icon + title (ASvc in the design — a 16px-icon `.phsec`). */
export function ASvc({ icon, title, right }: { icon: keyof typeof ASEC; title: ReactNode; right?: ReactNode }) {
  return (
    <div className="phsec" style={{ justifyContent: right ? 'space-between' : 'flex-start' }}>
      <span style={{ display: 'flex', alignItems: 'center', gap: 9 }}>
        <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
          {ASEC[icon].split('|').map((d, i) => (
            <path key={i} d={d} />
          ))}
        </svg>
        <span>{title}</span>
      </span>
      {right}
    </div>
  )
}

/** Status pill driven by a [color,label] map (APill in the design). */
export function MapPill({ map, s }: { map: Record<string, [string, string]>; s: string }) {
  const [c, l] = map[s] || ['var(--tx-faint)', s]
  return <StatusPill color={c} label={l} />
}
