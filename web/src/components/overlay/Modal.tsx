import { useEffect } from 'react'
import type { ReactNode } from 'react'
import { XIcon } from '@/lib/icons'
import './Modal.css'

export type ModalProps = {
  title: ReactNode
  sub?: ReactNode
  /** CSS width (defaults to min(540px,94vw)). */
  width?: string
  onClose: () => void
  children: ReactNode
  /** Footer actions (right-aligned). */
  foot?: ReactNode
}

/** Centered modal dialog (`.cm`). Closes on scrim click or Escape. */
export function Modal({ title, sub, width, onClose, children, foot }: ModalProps) {
  useEffect(() => {
    const k = (e: KeyboardEvent) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', k)
    return () => window.removeEventListener('keydown', k)
  }, [onClose])

  return (
    <div className="cm-scrim" onClick={onClose}>
      <div className="cm" style={{ width: width || 'min(540px,94vw)' }} onClick={(e) => e.stopPropagation()}>
        <div className="cm-head">
          <div>
            <div style={{ fontSize: 15, fontWeight: 600 }}>{title}</div>
            {sub != null && <div style={{ color: 'var(--tx-3)', marginTop: 2, fontSize: 12 }}>{sub}</div>}
          </div>
          <button className="cm-x" onClick={onClose}>
            <XIcon />
          </button>
        </div>
        <div className="cm-body">{children}</div>
        {foot && <div className="cm-foot">{foot}</div>}
      </div>
    </div>
  )
}
