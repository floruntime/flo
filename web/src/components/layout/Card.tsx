import type { ReactNode } from 'react'
import { cx } from '@/lib/cx'
import './layout.css'

export type CardProps = {
  children: ReactNode
  /** Apply the default 20px padding (`.card-p`). */
  pad?: boolean
  className?: string
  style?: React.CSSProperties
  onClick?: () => void
}

/** Flat surface card (`.card`). */
export function Card({ children, pad = true, className, style, onClick }: CardProps) {
  return (
    <div className={cx('card', pad && 'card-p', className)} style={style} onClick={onClick}>
      {children}
    </div>
  )
}
