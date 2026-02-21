import { type ButtonHTMLAttributes, forwardRef } from 'react';
import { cn } from '../../lib/utils';

export type ButtonVariant = 'primary' | 'secondary' | 'danger' | 'ghost';
export type ButtonSize = 'sm' | 'md' | 'lg';

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
}

const variantClasses: Record<ButtonVariant, string> = {
  // Filled green — primary action (Send Signal, Retry, etc.)
  primary:
    'bg-primary hover:bg-primary/90 text-background border border-transparent font-medium',
  // "Docs" style — surface background, subtle border, muted text
  secondary:
    'bg-surface hover:bg-surface-hover text-text-secondary hover:text-text-primary border border-surface-border',
  // Destructive — same surface look but hovers red
  danger:
    'bg-surface hover:bg-error/10 text-text-secondary hover:text-error border border-surface-border hover:border-error/40',
  // No background, no border — inline / toolbar
  ghost:
    'bg-transparent hover:bg-surface-hover text-text-secondary hover:text-text-primary border border-transparent',
};

const sizeClasses: Record<ButtonSize, string> = {
  sm: 'px-2.5 py-1 text-xs gap-1.5 [&_svg]:w-3 [&_svg]:h-3',
  md: 'px-3 py-1.5 text-sm gap-2 [&_svg]:w-3.5 [&_svg]:h-3.5',
  lg: 'px-4 py-2 text-sm gap-2 [&_svg]:w-4 [&_svg]:h-4',
};

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = 'secondary', size = 'sm', children, ...props }, ref) => {
    return (
      <button
        ref={ref}
        className={cn(
          'inline-flex items-center justify-center rounded-md font-medium transition-colors cursor-pointer',
          'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/50',
          'disabled:opacity-50 disabled:pointer-events-none',
          variantClasses[variant],
          sizeClasses[size],
          className,
        )}
        {...props}
      >
        {children}
      </button>
    );
  }
);

Button.displayName = 'Button';
