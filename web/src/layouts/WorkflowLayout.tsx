import { useState, useRef, useCallback } from 'react';
import { Outlet, useNavigate } from 'react-router-dom';
import { WorkflowSidebar } from '../components/workflow/WorkflowSidebar';
import { useCustomViews } from '../hooks/useCustomViews';
import type { SavedView } from '../lib/workflow-types';

// =============================================================================
// Outlet context – shared between all /workflows/* pages
// =============================================================================

export interface WorkflowOutletContext {
  /** Currently active sidebar view ID */
  activeViewId: string;
  /** Call this to both update activeViewId and navigate to /workflows */
  onSelectView: (view: SavedView) => void;
  /** Custom views from localStorage */
  customViews: SavedView[];
  /** Pages call this to report their active filter state (used when saving views) */
  reportFilters: (filters: SavedView['filters']) => void;
}

// =============================================================================
// Layout – renders sidebar once; <Outlet /> swaps only the right panel
// =============================================================================

export function WorkflowLayout() {
  const navigate = useNavigate();
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [activeViewId, setActiveViewId] = useState('all');
  const { customViews, addView, removeView } = useCustomViews();

  // Ref holds whatever filters the current page has active — read only at save-time
  const filtersRef = useRef<SavedView['filters']>({});
  const reportFilters = useCallback((f: SavedView['filters']) => {
    filtersRef.current = f;
  }, []);

  const handleSelectView = (view: SavedView) => {
    setActiveViewId(view.id);
    navigate('/workflows');
  };

  const handleAddView = (name: string) => {
    const newView = addView({ name, icon: 'bookmark', filters: { ...filtersRef.current } });
    // Auto-select the newly created view
    setActiveViewId(newView.id);
  };

  const ctx: WorkflowOutletContext = {
    activeViewId,
    onSelectView: handleSelectView,
    customViews,
    reportFilters,
  };

  return (
    <div className="flex h-full min-h-0">
      <WorkflowSidebar
        activeViewId={activeViewId}
        onSelectView={handleSelectView}
        collapsed={sidebarCollapsed}
        onToggleCollapse={() => setSidebarCollapsed((c) => !c)}
        customViews={customViews}
        onAddView={handleAddView}
        onRemoveView={removeView}
      />
      <Outlet context={ctx} />
    </div>
  );
}
