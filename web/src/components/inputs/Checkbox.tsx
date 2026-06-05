import type { ReactNode } from 'react'
import './inputs.css'

export type CheckboxProps = {
  checked: boolean
  onChange: (checked: boolean) => void
  children: ReactNode
}

/** Checkbox + label row (`.kv-check`). */
export function Checkbox({ checked, onChange, children }: CheckboxProps) {
  return (
    <label className="kv-check">
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} />
      <span>{children}</span>
    </label>
  )
}
