import type { ReactNode } from 'react'
import { cx } from '@/lib/cx'
import './feedback.css'

export type TagProps = {
  children: ReactNode
  className?: string
  style?: React.CSSProperties
}

/** Monospace mini-tag (`.tag`). */
export function Tag({ children, className, style }: TagProps) {
  return (
    <span className={cx('tag', className)} style={style}>
      {children}
    </span>
  )
}
