import type { ComponentType, ReactNode } from 'react';
import { Search, ChevronDown } from 'lucide-react';
import { cn } from '../../lib/utils';

// =============================================================================
// FilterBar
//
// Workflow-style inline filter bar: search input + dropdown selects + actions.
// Mirrors the FilterToolbar pattern from WorkflowsListPage.
//
// Usage:
//   <FilterBar
//     search={search}
//     onSearchChange={setSearch}
//     searchPlaceholder="Search queues..."
//     selects={[
//       { icon: Filter, value: status, onChange: setStatus,
//         options: [{ value: 'all', label: 'Status' }, { value: 'active', label: 'Active' }] }
//     ]}
//     actions={<Button variant="primary">New Queue</Button>}
//     resultCount={12}
//     resultLabel="queue"
//   />
// =============================================================================

export interface SelectOption {
  value: string;
  label: string;
}

export interface FilterBarSelect {
  icon?: ComponentType<{ className?: string }>;
  value: string;
  onChange: (v: string) => void;
  options: SelectOption[];
}

interface FilterBarProps {
  search: string;
  onSearchChange: (v: string) => void;
  searchPlaceholder?: string;
  selects?: FilterBarSelect[];
  /** Extra content placed at the right end (e.g. a CTA button) */
  actions?: ReactNode;
  /** When provided, renders a row below the bar: "N {resultLabel}s" */
  resultCount?: number;
  resultLabel?: string;
}

export function FilterBar({
  search,
  onSearchChange,
  searchPlaceholder = 'Search...',
  selects = [],
  actions,
  resultCount,
  resultLabel = 'result',
}: FilterBarProps) {
  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-2 flex-wrap">
        {/* Search */}
        <div className="relative flex-1 min-w-[180px] max-w-xs">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-secondary pointer-events-none" />
          <input
            type="text"
            placeholder={searchPlaceholder}
            value={search}
            onChange={e => onSearchChange(e.target.value)}
            className="w-full pl-8 pr-3 py-1.5 text-sm bg-surface border border-surface-border rounded-md focus:outline-none focus:ring-1 focus:ring-primary text-text-primary placeholder:text-text-secondary/60"
          />
        </div>

        {/* Select dropdowns */}
        {selects.map((sel, i) => {
          const Icon = sel.icon;
          return (
            <div key={i} className="relative">
              {Icon && (
                <Icon className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3 h-3 text-text-secondary pointer-events-none" />
              )}
              <select
                value={sel.value}
                onChange={e => sel.onChange(e.target.value)}
                className={cn(
                  'pr-7 py-1.5 text-sm bg-surface border border-surface-border rounded-md focus:outline-none focus:ring-1 focus:ring-primary text-text-primary appearance-none cursor-pointer',
                  Icon ? 'pl-7' : 'pl-3'
                )}
              >
                {sel.options.map(opt => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
              </select>
              <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3 h-3 text-text-secondary pointer-events-none" />
            </div>
          );
        })}

        {/* Spacer */}
        <div className="flex-1" />

        {/* Actions */}
        {actions}
      </div>

      {/* Result count */}
      {resultCount !== undefined && (
        <p className="text-xs text-text-secondary">
          {resultCount} {resultLabel}{resultCount !== 1 ? 's' : ''}
        </p>
      )}
    </div>
  );
}
