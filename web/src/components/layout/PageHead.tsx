import type { ReactNode } from 'react'
import './layout.css'

export type PageHeadProps = {
  title: ReactNode
  subtitle?: ReactNode
  /** Right-aligned actions (buttons, search). */
  actions?: ReactNode
}

/** Page header with title + subtitle (`.ph`). */
export function PageHead({ title, subtitle, actions }: PageHeadProps) {
  return (
    <div className="ph">
      {actions ? (
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'flex-start',
            gap: 16,
            flexWrap: 'wrap',
          }}
        >
          <div style={{ minWidth: 0 }}>
            <h1 className="ph-t">{title}</h1>
            {subtitle != null && <div className="ph-s">{subtitle}</div>}
          </div>
          <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>{actions}</div>
        </div>
      ) : (
        <>
          <h1 className="ph-t">{title}</h1>
          {subtitle != null && <div className="ph-s">{subtitle}</div>}
        </>
      )}
    </div>
  )
}
