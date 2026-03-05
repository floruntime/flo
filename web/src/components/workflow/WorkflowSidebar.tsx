import { useMemo, useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  GitGraph,
  Play,
  Clock,
  AlertCircle,
  CalendarClock,
  Bookmark,
  Layers,
  PanelLeftClose,
  PanelLeft,
  Plus,
  X,
  Check,
} from 'lucide-react';
import { cn } from '../../lib/utils';
import type { SavedView } from '../../lib/workflow-types';
import { useWorkflowRuns } from '../../lib/workflow-hooks';

// =============================================================================
// Data
// =============================================================================

export const SYSTEM_VIEWS: SavedView[] = [
  { id: 'all',       name: 'All Workflows',    icon: 'layers',       isSystem: true, filters: {}                         },
  { id: 'parents',   name: 'Parent Workflows', icon: 'git-graph',    isSystem: true, filters: { hideChildren: true }     },
  { id: 'running',   name: 'Running',          icon: 'play',         isSystem: true, filters: { status: 'running' }      },
  { id: 'failed',    name: 'Failed',           icon: 'alert-circle', isSystem: true, filters: { status: 'failed' }       },
  { id: 'today',     name: 'Today',            icon: 'calendar',     isSystem: true, filters: { timeRange: 'today' }     },
  { id: 'last-hour', name: 'Last Hour',        icon: 'clock',        isSystem: true, filters: { timeRange: 'last_hour' } },
];

export const VIEW_ICONS: Record<string, typeof Play> = {
  layers: Layers, 'git-graph': GitGraph, play: Play, 'alert-circle': AlertCircle,
  calendar: CalendarClock, clock: Clock, bookmark: Bookmark,
};

// =============================================================================
// WorkflowSidebar
// =============================================================================

interface WorkflowSidebarProps {
  activeViewId: string;
  onSelectView: (view: SavedView) => void;
  collapsed: boolean;
  onToggleCollapse: () => void;
  customViews: SavedView[];
  onAddView: (name: string) => void;
  onRemoveView: (id: string) => void;
}

export function WorkflowSidebar({ activeViewId, onSelectView, collapsed, onToggleCollapse, customViews, onAddView, onRemoveView }: WorkflowSidebarProps) {
  const [isAdding, setIsAdding] = useState(false);
  const [newName, setNewName] = useState('');
  const nameInputRef = useRef<HTMLInputElement>(null);

  // Auto-focus the input when it appears
  useEffect(() => {
    if (isAdding && nameInputRef.current) {
      nameInputRef.current.focus();
    }
  }, [isAdding]);

  const handleSubmitNewView = () => {
    const trimmed = newName.trim();
    if (trimmed) {
      onAddView(trimmed);
    }
    setNewName('');
    setIsAdding(false);
  };

  const handleCancelAdd = () => {
    setNewName('');
    setIsAdding(false);
  };

  // Fetch runs from API for badge counts (poll every 10s for live counts)
  const { runs } = useWorkflowRuns({ pollInterval: 10000 });

  const viewCounts = useMemo(() => {
    const now = Date.now();
    const todayStart = new Date().setHours(0, 0, 0, 0);
    const hourAgo = now - 3_600_000;
    const c: Record<string, number> = {};
    c['all']       = runs.length;
    c['parents']   = runs.filter(r => !r.parent_run_id).length;
    c['running']   = runs.filter(r => r.status === 'running' || r.status === 'waiting').length;
    c['failed']    = runs.filter(r => r.status === 'failed').length;
    c['today']     = runs.filter(r => r.started_at >= todayStart).length;
    c['last-hour'] = runs.filter(r => r.started_at >= hourAgo).length;
    // Dynamic counts for custom views — apply ALL saved filters
    for (const cv of customViews) {
      c[cv.id] = runs.filter(r => {
        if (cv.filters.workflow && r.workflow_name !== cv.filters.workflow) return false;
        if (cv.filters.status && cv.filters.status !== 'all' && r.status !== cv.filters.status) return false;
        if (cv.filters.hideChildren && r.parent_run_id) return false;
        if (cv.filters.search) {
          const q = cv.filters.search.toLowerCase();
          if (!r.run_id.toLowerCase().includes(q) && !r.workflow_name.toLowerCase().includes(q)) return false;
        }
        if (cv.filters.timeRange === 'today' && r.started_at < todayStart) return false;
        if (cv.filters.timeRange === 'last_hour' && r.started_at < hourAgo) return false;
        return true;
      }).length;
    }
    return c;
  }, [runs, customViews]);

  if (collapsed) {
    return (
      <div className="flex flex-col items-center py-2 w-10 border-r border-surface-border bg-surface/50 shrink-0">
        <button
          onClick={onToggleCollapse}
          className="p-1.5 rounded hover:bg-surface-hover text-text-secondary transition-colors mb-3"
          title="Expand views"
        >
          <PanelLeft className="w-4 h-4" />
        </button>
        {SYSTEM_VIEWS.map(view => {
          const Icon = VIEW_ICONS[view.icon ?? 'layers'] || Layers;
          return (
            <button
              key={view.id}
              onClick={() => onSelectView(view)}
              className={cn(
                'p-1.5 rounded transition-colors mb-0.5',
                activeViewId === view.id ? 'bg-primary/10 text-primary' : 'hover:bg-surface-hover text-text-secondary',
              )}
              title={view.name}
            >
              <Icon className="w-3.5 h-3.5" />
            </button>
          );
        })}
        {customViews.length > 0 && (
          <div className="border-t border-surface-border my-1 pt-1">
            {customViews.map(view => {
              const Icon = VIEW_ICONS[view.icon ?? 'bookmark'] || Bookmark;
              return (
                <button
                  key={view.id}
                  onClick={() => onSelectView(view)}
                  className={cn(
                    'p-1.5 rounded transition-colors mb-0.5',
                    activeViewId === view.id ? 'bg-primary/10 text-primary' : 'hover:bg-surface-hover text-text-secondary',
                  )}
                  title={view.name}
                >
                  <Icon className="w-3.5 h-3.5" />
                </button>
              );
            })}
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="w-[200px] border-r border-surface-border bg-surface/50 shrink-0 flex flex-col">
      <div className="flex items-center justify-between px-3 py-2 border-b border-surface-border">
        <span className="text-xs font-medium text-text-secondary uppercase tracking-wider">Views</span>
        <button
          onClick={onToggleCollapse}
          className="p-1 rounded hover:bg-surface-hover text-text-secondary transition-colors"
          title="Collapse sidebar"
        >
          <PanelLeftClose className="w-3.5 h-3.5" />
        </button>
      </div>

      <div className="px-2 py-2 space-y-0.5">
        <p className="text-[10px] uppercase tracking-wider text-text-secondary/60 font-medium px-2 mb-1">System</p>
        {SYSTEM_VIEWS.map(view => {
          const Icon = VIEW_ICONS[view.icon ?? 'layers'] || Layers;
          const isActive = activeViewId === view.id;
          const count = viewCounts[view.id] || 0;
          return (
            <button
              key={view.id}
              onClick={() => onSelectView(view)}
              className={cn(
                'w-full flex items-center gap-2 px-2 py-1.5 rounded-md text-xs transition-colors text-left',
                isActive ? 'bg-primary/10 text-primary font-medium' : 'text-text-secondary hover:text-text-primary hover:bg-surface-hover',
              )}
            >
              <Icon className="w-3 h-3 shrink-0" />
              <span className="flex-1 truncate">{view.name}</span>
              <span className={cn(
                'text-[10px] font-mono px-1.5 py-0.5 rounded-full min-w-5 text-center',
                isActive ? 'bg-primary/20 text-primary' : 'bg-surface-hover text-text-secondary',
              )}>
                {count}
              </span>
            </button>
          );
        })}
      </div>

      <div className="px-2 py-2 space-y-0.5 border-t border-surface-border">
        <div className="flex items-center justify-between px-2 mb-1">
          <p className="text-[10px] uppercase tracking-wider text-text-secondary/60 font-medium">Custom</p>
          <button
            onClick={() => setIsAdding(true)}
            className="p-0.5 rounded hover:bg-surface-hover text-text-secondary/60 hover:text-text-secondary transition-colors"
            title="Save current filters as a view"
          >
            <Plus className="w-3 h-3" />
          </button>
        </div>
        {isAdding && (
          <div className="flex items-center gap-1 px-1 mb-1">
            <Bookmark className="w-3 h-3 text-text-secondary/60 shrink-0" />
            <input
              ref={nameInputRef}
              type="text"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleSubmitNewView();
                if (e.key === 'Escape') handleCancelAdd();
              }}
              onBlur={handleCancelAdd}
              placeholder="View name…"
              className="flex-1 min-w-0 bg-background border border-surface-border rounded px-1.5 py-1 text-xs text-text-primary placeholder:text-text-secondary/40 outline-none focus:border-primary/50"
            />
            <button
              onMouseDown={(e) => { e.preventDefault(); handleSubmitNewView(); }}
              className="p-0.5 rounded hover:bg-primary/10 text-text-secondary/60 hover:text-primary transition-colors"
              title="Save"
            >
              <Check className="w-3 h-3" />
            </button>
            <button
              onMouseDown={(e) => { e.preventDefault(); handleCancelAdd(); }}
              className="p-0.5 rounded hover:bg-error/10 text-text-secondary/60 hover:text-error transition-colors"
              title="Cancel"
            >
              <X className="w-3 h-3" />
            </button>
          </div>
        )}
        {customViews.map(view => {
          const Icon = VIEW_ICONS[view.icon ?? 'bookmark'] || Bookmark;
          const isActive = activeViewId === view.id;
          const count = viewCounts[view.id] || 0;
          return (
            <div
              key={view.id}
              className="group relative"
            >
              <button
                onClick={() => onSelectView(view)}
                className={cn(
                  'w-full flex items-center gap-2 px-2 py-1.5 rounded-md text-xs transition-colors text-left',
                  isActive ? 'bg-primary/10 text-primary font-medium' : 'text-text-secondary hover:text-text-primary hover:bg-surface-hover',
                )}
              >
                <Icon className="w-3 h-3 shrink-0" />
                <span className="flex-1 truncate">{view.name}</span>
                <span className={cn(
                  'text-[10px] font-mono px-1.5 py-0.5 rounded-full min-w-5 text-center group-hover:hidden',
                  isActive ? 'bg-primary/20 text-primary' : 'bg-surface-hover text-text-secondary',
                )}>
                  {count}
                </span>
              </button>
              <button
                onClick={(e) => { e.stopPropagation(); onRemoveView(view.id); }}
                className="absolute right-2 top-1/2 -translate-y-1/2 hidden group-hover:flex p-0.5 rounded hover:bg-error/10 text-text-secondary/40 hover:text-error transition-colors"
                title="Remove view"
              >
                <X className="w-3 h-3" />
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// =============================================================================
// useWorkflowSidebarNav – for use on detail pages (clicking a view navigates back)
// =============================================================================

export function useWorkflowSidebarNav() {
  const navigate = useNavigate();
  const handleSelectView = (_view: SavedView) => {
    navigate('/workflows');
  };
  return { handleSelectView };
}
