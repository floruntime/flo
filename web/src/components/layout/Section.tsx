import type { ReactNode } from 'react'
import { cx } from '@/lib/cx'
import { Icon } from '@/lib/icons'
import './layout.css'

export type SectionProps = {
  /** Small heading text. */
  title?: ReactNode
  /** Right-aligned meta (mono, faint). */
  meta?: ReactNode
  children: ReactNode
  className?: string
  style?: React.CSSProperties
}

/** Vertical section with optional `.sec-h` header (`.sec`). */
export function Section({ title, meta, children, className, style }: SectionProps) {
  return (
    <div className={cx('sec', className)} style={style}>
      {(title != null || meta != null) && (
        <div className="sec-h">
          {title != null ? <h2>{title}</h2> : <span />}
          {meta != null && <span className="meta">{meta}</span>}
        </div>
      )}
      {children}
    </div>
  )
}

export type PhSecProps = {
  /** Icon path data (see PHI / NAV_ICONS) or an explicit node. */
  icon?: string
  title: ReactNode
  /** Show the trailing info "i" badge. */
  info?: boolean
  right?: ReactNode
}

/** Icon + title sub-section header (`.phsec`). */
export function PhSec({ icon, title, info, right }: PhSecProps) {
  return (
    <div className="phsec" style={{ justifyContent: right ? 'space-between' : 'flex-start' }}>
      <span style={{ display: 'flex', alignItems: 'center', gap: 9 }}>
        {icon && <Icon paths={icon} size={17} />}
        <span>{title}</span>
        {info && <span className="phinfo">i</span>}
      </span>
      {right}
    </div>
  )
}
