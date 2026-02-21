import type { ComponentType } from 'react';
import { Card, CardContent } from './Card';
import { cn } from '../../lib/utils';

// =============================================================================
// StatCard
//
// A compact summary card used in list-page overviews (Queues, Streams, etc.)
//
// Usage:
//   <StatCard label="Queues" value={8} subtitle="in this namespace" icon={MessageSquare} />
//   <StatCard label="Dead Letters" value={12} subtitle="needs attention" icon={AlertCircle} alert />
// =============================================================================

interface StatCardProps {
  label: string;
  value: string | number;
  subtitle?: string;
  icon?: ComponentType<{ className?: string }>;
  /** When true: red value + red border to signal a problem */
  alert?: boolean;
}

export function StatCard({ label, value, subtitle, icon: Icon, alert }: StatCardProps) {
  return (
    <Card className={cn('bg-surface', alert && 'border-error/30')}>
      <CardContent className="p-4">
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs text-text-secondary uppercase tracking-wider">{label}</span>
          {Icon && (
            <Icon className={cn('w-4 h-4', alert ? 'text-error' : 'text-text-secondary')} />
          )}
        </div>
        <p className={cn('text-2xl font-semibold', alert ? 'text-error' : 'text-text-primary')}>
          {value}
        </p>
        {subtitle && (
          <p className="text-xs text-text-secondary mt-1">{subtitle}</p>
        )}
      </CardContent>
    </Card>
  );
}
