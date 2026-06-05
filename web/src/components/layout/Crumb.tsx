import type { ReactNode } from 'react'
import './layout.css'

export type CrumbProps = {
  /** Leading link text. */
  root: ReactNode
  onRoot?: () => void
  /** Current (trailing) segment. */
  current: ReactNode
  currentMono?: boolean
}

/** Breadcrumb trail (`.crumb`). */
export function Crumb({ root, onRoot, current, currentMono = true }: CrumbProps) {
  return (
    <div className="crumb">
      <a onClick={onRoot}>{root}</a>
      <span className="sep">/</span>
      <span className={currentMono ? 'cur mono' : 'cur'}>{current}</span>
    </div>
  )
}
