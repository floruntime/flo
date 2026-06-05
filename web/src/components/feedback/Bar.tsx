import { cx } from '@/lib/cx'
import './feedback.css'

export type BarProps = {
  /** 0..1 fill fraction. */
  value: number
  warn?: boolean
  /** Override fill color (else accent / warn). */
  color?: string
  className?: string
  style?: React.CSSProperties
}

/** Thin progress bar (`.bar`). */
export function Bar({ value, warn, color, className, style }: BarProps) {
  const pct = Math.max(0, Math.min(1, value)) * 100
  return (
    <div className={cx('bar', warn && 'warn', className)} style={style}>
      <span style={{ width: pct + '%', background: color }} />
    </div>
  )
}
