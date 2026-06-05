import type { InputHTMLAttributes, ReactNode } from 'react'
import { cx } from '@/lib/cx'
import './inputs.css'

export type FieldProps = InputHTMLAttributes<HTMLInputElement> & {
  /** Wrapper class (e.g. width) applied to the `.field` container. */
  wrapClassName?: string
  wrapStyle?: React.CSSProperties
  /** Optional trailing adornment (e.g. a clear button). */
  trailing?: ReactNode
  invalid?: boolean
}

/** Single-line text input wrapped in the `.field` shell. */
export function Field({ wrapClassName, wrapStyle, trailing, invalid, className, ...rest }: FieldProps) {
  return (
    <div
      className={cx('field', wrapClassName)}
      style={{ borderColor: invalid ? 'color-mix(in srgb,var(--crit) 50%,transparent)' : undefined, ...wrapStyle }}
    >
      <input className={className} {...rest} />
      {trailing}
    </div>
  )
}
