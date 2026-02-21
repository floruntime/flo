import { cn } from '../../lib/utils';

// =============================================================================
// Supabase-style underline tabs
// Usage:
//   <PageTabs
//     tabs={[{ id: 'runs', label: 'Recent Runs' }, { id: 'defs', label: 'Definitions' }]}
//     activeTab="runs"
//     onChange={setTab}
//   />
// =============================================================================

export interface Tab {
  id: string;
  label: string;
  count?: number;
}

interface PageTabsProps {
  tabs: Tab[];
  activeTab: string;
  onChange: (id: string) => void;
  className?: string;
}

export function PageTabs({ tabs, activeTab, onChange, className }: PageTabsProps) {
  return (
    <div className={cn('flex items-end gap-0 border-b border-surface-border', className)}>
      {tabs.map((tab) => {
        const isActive = tab.id === activeTab;
        return (
          <button
            key={tab.id}
            onClick={() => onChange(tab.id)}
            className={cn(
              'relative px-4 py-2.5 text-sm font-medium transition-colors whitespace-nowrap',
              'focus:outline-none',
              isActive
                ? 'text-text-primary'
                : 'text-text-secondary hover:text-text-primary'
            )}
          >
            {tab.label}
            {tab.count !== undefined && (
              <span className="ml-1.5 text-xs text-text-secondary/60 font-normal">
                {tab.count}
              </span>
            )}
            {/* Active underline */}
            {isActive && (
              <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-text-primary rounded-t" />
            )}
          </button>
        );
      })}
    </div>
  );
}
