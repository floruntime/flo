import { cx } from '@/lib/cx'
import './feedback.css'

export type DotProps = {
  color?: string
  size?: number
  className?: string
  style?: React.CSSProperties
}

/** Small status dot (`.dot`). */
export function Dot({ color = 'var(--tx-3)', size, className, style }: DotProps) {
  return (
    <span
      className={cx('dot', className)}
      style={{ background: color, width: size, height: size, ...style }}
    />
  )
}
