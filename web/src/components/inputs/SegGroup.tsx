import { cx } from '@/lib/cx'
import './inputs.css'

export type SegOption<T extends string> = { id: T; label: string }

export type SegGroupProps<T extends string> = {
  options: ReadonlyArray<SegOption<T>>
  value: T
  onChange: (id: T) => void
  className?: string
  style?: React.CSSProperties
  btnStyle?: React.CSSProperties
}

/** Segmented control (`.seg-group`). */
export function SegGroup<T extends string>({
  options,
  value,
  onChange,
  className,
  style,
  btnStyle,
}: SegGroupProps<T>) {
  return (
    <div className={cx('seg-group', className)} style={style}>
      {options.map((o) => (
        <button
          key={o.id}
          type="button"
          className={cx('seg-btn', value === o.id && 'on')}
          style={btnStyle}
          onClick={() => onChange(o.id)}
        >
          {o.label}
        </button>
      ))}
    </div>
  )
}
