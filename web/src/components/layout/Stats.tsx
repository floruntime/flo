import type { ReactNode } from 'react'
import './layout.css'

export type StatProps = {
  label: ReactNode
  value: ReactNode
  /** Inline unit shown after the value. */
  unit?: ReactNode
  sub?: ReactNode
  valueColor?: string
}

/** Single stat cell (`.stat`). */
export function Stat({ label, value, unit, sub, valueColor }: StatProps) {
  return (
    <div className="stat">
      <div className="l">{label}</div>
      <div className="v" style={{ color: valueColor }}>
        {value}
        {unit != null && <span className="u">{unit}</span>}
      </div>
      {sub != null && <div className="sub">{sub}</div>}
    </div>
  )
}

export type StatsProps = {
  children: ReactNode
  /** Column count (defaults to 4). */
  columns?: number
  style?: React.CSSProperties
}

/** Flat borderless stat strip (`.stats`). */
export function Stats({ children, columns, style }: StatsProps) {
  return (
    <div
      className="stats"
      style={columns ? { gridTemplateColumns: `repeat(${columns},1fr)`, ...style } : style}
    >
      {children}
    </div>
  )
}
