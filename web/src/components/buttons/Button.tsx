import type { ButtonHTMLAttributes } from 'react'
import { cx } from '@/lib/cx'
import './Button.css'

export type ButtonVariant = 'default' | 'quiet' | 'accent' | 'danger'

export type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant
}

const VARIANT_CLASS: Record<ButtonVariant, string> = {
  default: '',
  quiet: 'q',
  accent: 'acc',
  danger: 'danger',
}

/** Steel-style button. Variants: default · quiet · accent · danger. */
export function Button({ variant = 'default', className, children, ...rest }: ButtonProps) {
  return (
    <button className={cx('btn', VARIANT_CLASS[variant], className)} {...rest}>
      {children}
    </button>
  )
}
