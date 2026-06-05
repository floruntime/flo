import type { ReactNode } from 'react'
import { cx } from '@/lib/cx'
import './feedback.css'

export type PillTone = 'default' | 'ok' | 'lag'

export type PillProps = {
  tone?: PillTone
  children: ReactNode
  className?: string
  style?: React.CSSProperties
  /** Show the leading status dot. */
  dot?: boolean
}

/** Soft status pill (`.pill`). Tones: default · ok (sage) · lag (amber). */
export function Pill({ tone = 'default', dot = true, children, className, style }: PillProps) {
  return (
    <span className={cx('pill', tone !== 'default' && tone, className)} style={style}>
      {dot && <span className="d" />}
      {children}
    </span>
  )
}
