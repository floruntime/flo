import { cx } from '@/lib/cx'
import './feedback.css'

export type StatusPillProps = {
  /** Color (any CSS color or var). Drives both dot + text. */
  color: string
  label: string
  className?: string
}

/** Colored-dot status label (`.wpill`) used by workflows / processing / compute. */
export function StatusPill({ color, label, className }: StatusPillProps) {
  return (
    <span className={cx('wpill', className)} style={{ color }}>
      <span className="wd" style={{ background: color }} />
      {label}
    </span>
  )
}

export type OpPillProps = {
  color: string
  children: React.ReactNode
}

/** Operator / kind tag (`.op-pill`) — tinted bg derived from `color`. */
export function OpPill({ color, children }: OpPillProps) {
  return (
    <span className="op-pill" style={{ color, background: `color-mix(in srgb,${color} 14%,transparent)` }}>
      {children}
    </span>
  )
}
