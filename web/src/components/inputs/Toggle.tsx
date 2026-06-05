import { cx } from '@/lib/cx'
import './inputs.css'

export type ToggleProps = {
  on: boolean
  onChange?: (on: boolean) => void
}

/** Pill toggle switch (`.wtoggle`). */
export function Toggle({ on, onChange }: ToggleProps) {
  return (
    <span
      role="switch"
      aria-checked={on}
      className={cx('wtoggle', on && 'on')}
      onClick={() => onChange?.(!on)}
    />
  )
}
