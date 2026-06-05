import type { TextareaHTMLAttributes } from 'react'
import { cx } from '@/lib/cx'
import './inputs.css'

export type TextAreaProps = TextareaHTMLAttributes<HTMLTextAreaElement> & {
  invalid?: boolean
}

/** Monospace code/value editor (`.kv-editor`). */
export function TextArea({ invalid, className, ...rest }: TextAreaProps) {
  return <textarea className={cx('kv-editor', invalid && 'err', className)} spellCheck={false} {...rest} />
}
