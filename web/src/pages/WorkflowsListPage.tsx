import { useState, useMemo, useEffect } from 'react';
import { useNavigate, useOutletContext } from 'react-router-dom';
import type { WorkflowOutletContext } from '../layouts/WorkflowLayout';
import {
  Play,
  CheckCircle,
  XCircle,
  Clock,
  Pause,
  Timer,
  Ban,
  Search,
  Filter,
  ChevronDown,
  Zap,
  Eye,
  EyeOff,
  GitBranch,
  GitGraph,
  CalendarClock,
  FileCode,
  Radio,
  Shield,
  ChevronRight,
  Plus,
  X,
  AlertCircle,
} from 'lucide-react';
import { Button } from '../components/ui/Button';
import { SYSTEM_VIEWS } from '../components/workflow/WorkflowSidebar';

import { cn } from '../lib/utils';
import { DataTable } from '../components/ui/DataTable';
import type { TableColumn } from '../components/ui/DataTable';
import { PageTabs } from '../components/ui/PageTabs';
import type { WorkflowRunStatus, WorkflowRun, SavedView } from '../lib/workflow-types';
import { useWorkflowRuns, useWorkflowDefinitions } from '../lib/workflow-hooks';
import type { DefinitionListEntry } from '../lib/workflow-api';
import * as workflowApi from '../lib/workflow-api';

// =============================================================================
// Shared helpers
// =============================================================================

const STATUS_CONFIG: Record<
  WorkflowRunStatus,
  { label: string; icon: typeof Play; color: string; bgColor: string }
> = {
  pending:   { label: 'Pending',    icon: Clock,         color: 'text-text-secondary', bgColor: 'bg-text-secondary/10' },
  running:   { label: 'Running',    icon: Play,          color: 'text-blue-500',       bgColor: 'bg-blue-500/10'        },
  waiting:   { label: 'Waiting',    icon: Pause,         color: 'text-amber-500',      bgColor: 'bg-amber-500/10'       },
  completed: { label: 'Completed',  icon: CheckCircle,   color: 'text-success',        bgColor: 'bg-success/10'         },
  failed:    { label: 'Failed',     icon: XCircle,       color: 'text-error',          bgColor: 'bg-error/10'           },
  cancelled: { label: 'Cancelled',  icon: Ban,           color: 'text-text-secondary', bgColor: 'bg-text-secondary/10'  },
  timed_out: { label: 'Timed Out',  icon: Timer,         color: 'text-orange-500',     bgColor: 'bg-orange-500/10'      },
};

function StatusBadge({ status }: { status: WorkflowRunStatus }) {
  const cfg = STATUS_CONFIG[status];
  const Icon = cfg.icon;
  return (
    <span className={cn('inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium', cfg.bgColor, cfg.color)}>
      <Icon className="w-3 h-3" />
      {cfg.label}
    </span>
  );
}

function formatDuration(ms: number | undefined | null): string {
  if (ms == null) return '-';
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ${Math.floor((ms % 60_000) / 1000)}s`;
  return `${Math.floor(ms / 3_600_000)}h ${Math.floor((ms % 3_600_000) / 60_000)}m`;
}

function formatTimeAgo(ts: number): string {
  const d = Date.now() - ts;
  if (d < 60_000) return `${Math.floor(d / 1000)}s ago`;
  if (d < 3_600_000) return `${Math.floor(d / 60_000)}m ago`;
  if (d < 86_400_000) return `${Math.floor(d / 3_600_000)}h ago`;
  return `${Math.floor(d / 86_400_000)}d ago`;
}


// =============================================================================
// Start Workflow Modal
// =============================================================================

function StartWorkflowModal({ onClose, definitions }: { onClose: () => void; definitions: DefinitionListEntry[] }) {
  const [selectedWorkflow, setSelectedWorkflow] = useState(
    definitions[0]?.name ?? ''
  );
  const [runId, setRunId] = useState('');
  const [jsonInput, setJsonInput] = useState('{}');
  const [jsonError, setJsonError] = useState('');

  const validateJson = (val: string) => {
    if (!val.trim()) { setJsonError(''); return; }
    try { JSON.parse(val); setJsonError(''); }
    catch { setJsonError('Invalid JSON'); }
  };

  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState('');

  const handleSubmit = async () => {
    if (jsonError || submitting) return;
    setSubmitting(true);
    setSubmitError('');
    try {
      await workflowApi.startRun({
        workflow: selectedWorkflow,
        input: jsonInput,
        ...(runId ? { run_id: runId } : {}),
      });
      onClose();
    } catch (e) {
      setSubmitError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm"
      onClick={e => e.target === e.currentTarget && onClose()}
    >
      <div className="relative bg-surface border border-surface-border rounded-xl shadow-2xl w-full max-w-lg mx-4 flex flex-col">
        {/* Header */}
        <div className="flex items-center gap-3 px-6 pt-5 pb-4 border-b border-surface-border">
          <div className="w-8 h-8 rounded-lg bg-primary/15 flex items-center justify-center shrink-0">
            <Play className="w-4 h-4 text-primary" />
          </div>
          <div className="flex-1 min-w-0">
            <h2 className="text-base font-semibold text-text-primary">Start Workflow</h2>
            <p className="text-xs text-text-secondary mt-0.5">Create a new workflow run</p>
          </div>
          <button
            onClick={onClose}
            className="w-7 h-7 flex items-center justify-center rounded-md hover:bg-surface-hover text-text-secondary hover:text-text-primary transition-colors cursor-pointer"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Body */}
        <div className="px-6 py-5 space-y-5">
          {/* Workflow select */}
          <div>
            <label className="block text-xs font-medium text-text-secondary uppercase tracking-wider mb-1.5">
              Workflow
            </label>
            <div className="relative">
              <GitGraph className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-secondary pointer-events-none" />
              <select
                value={selectedWorkflow}
                onChange={e => setSelectedWorkflow(e.target.value)}
                className="w-full pl-9 pr-8 py-2 text-sm bg-background border border-surface-border rounded-md focus:outline-none focus:ring-1 focus:ring-primary text-text-primary appearance-none cursor-pointer"
              >
                {definitions.map(d => (
                  <option key={d.name} value={d.name}>{d.name}</option>
                ))}
              </select>
              <ChevronDown className="absolute right-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-secondary pointer-events-none" />
            </div>
          </div>

          {/* Run ID */}
          <div>
            <label className="block text-xs font-medium text-text-secondary uppercase tracking-wider mb-1.5">
              Run ID{' '}
              <span className="normal-case font-normal text-text-secondary/60">(optional)</span>
            </label>
            <input
              type="text"
              placeholder="Auto-generated if empty"
              value={runId}
              onChange={e => setRunId(e.target.value)}
              className="w-full px-3 py-2 text-sm bg-background border border-surface-border rounded-md focus:outline-none focus:ring-1 focus:ring-primary text-text-primary placeholder:text-text-secondary/40 font-mono"
            />
          </div>

          {/* JSON input */}
          <div>
            <div className="flex items-center justify-between mb-1.5">
              <label className="block text-xs font-medium text-text-secondary uppercase tracking-wider">
                Input
              </label>
              {jsonError ? (
                <span className="flex items-center gap-1 text-[10px] text-error">
                  <AlertCircle className="w-3 h-3" />
                  {jsonError}
                </span>
              ) : (
                <span className="text-[10px] text-text-secondary/50 font-mono">JSON</span>
              )}
            </div>
            <textarea
              value={jsonInput}
              onChange={e => { setJsonInput(e.target.value); validateJson(e.target.value); }}
              rows={6}
              spellCheck={false}
              className={cn(
                'w-full px-3 py-2.5 text-xs font-mono bg-background border rounded-md resize-none focus:outline-none focus:ring-1',
                jsonError
                  ? 'border-error/50 focus:ring-error/40'
                  : 'border-surface-border focus:ring-primary',
                'text-text-primary placeholder:text-text-secondary/40'
              )}
              placeholder="{}"
            />
          </div>
        </div>

        {/* Footer */}
        <div className="flex flex-col gap-2 px-6 py-4 border-t border-surface-border">
          {submitError && (
            <div className="text-xs text-error flex items-center gap-1">
              <AlertCircle className="w-3 h-3" />
              {submitError}
            </div>
          )}
          <div className="flex items-center justify-end gap-2">
            <Button variant="secondary" size="md" onClick={onClose}>Cancel</Button>
            <Button variant="primary" size="md" onClick={handleSubmit} disabled={!!jsonError || submitting}>
              <Play className="w-3.5 h-3.5" />
              {submitting ? 'Starting…' : 'Start Workflow'}
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}

// =============================================================================
// Runs tab – filter bar
// =============================================================================

type StatusFilter = WorkflowRunStatus | 'all';
type SortField = 'started_at' | 'duration_ms' | 'status' | 'workflow_name';

interface Filters {
  status: StatusFilter;
  workflow: string;
  search: string;
  hideChildren: boolean;
}

function FilterToolbar({
  filters, onChange, childCount, onStartWorkflow, definitions,
}: {
  filters: Filters;
  onChange: (f: Filters) => void;
  childCount: number;
  onStartWorkflow: () => void;
  definitions: DefinitionListEntry[];
}) {
  const workflows = useMemo(() => ['all', ...definitions.map(d => d.name)], [definitions]);
  const statuses: StatusFilter[] = ['all','running','waiting','completed','failed','cancelled','timed_out','pending'];

  return (
    <div className="flex items-center gap-2 flex-wrap">
      {/* Search */}
      <div className="relative flex-1 min-w-[180px] max-w-xs">
        <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-secondary pointer-events-none" />
        <input
          type="text"
          placeholder="Search for a run..."
          value={filters.search}
          onChange={e => onChange({ ...filters, search: e.target.value })}
          className="w-full pl-8 pr-3 py-1.5 text-sm bg-surface border border-surface-border rounded-md focus:outline-none focus:ring-1 focus:ring-primary text-text-primary placeholder:text-text-secondary/60"
        />
      </div>

      {/* Status */}
      <div className="relative">
        <Filter className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3 h-3 text-text-secondary pointer-events-none" />
        <select
          value={filters.status}
          onChange={e => onChange({ ...filters, status: e.target.value as StatusFilter })}
          className="pl-7 pr-7 py-1.5 text-sm bg-surface border border-surface-border rounded-md focus:outline-none focus:ring-1 focus:ring-primary text-text-primary appearance-none cursor-pointer"
        >
          {statuses.map(s => (
            <option key={s} value={s}>{s === 'all' ? 'Status' : STATUS_CONFIG[s].label}</option>
          ))}
        </select>
        <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3 h-3 text-text-secondary pointer-events-none" />
      </div>

      {/* Workflow */}
      <div className="relative">
        <GitGraph className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3 h-3 text-text-secondary pointer-events-none" />
        <select
          value={filters.workflow}
          onChange={e => onChange({ ...filters, workflow: e.target.value })}
          className="pl-7 pr-7 py-1.5 text-sm bg-surface border border-surface-border rounded-md focus:outline-none focus:ring-1 focus:ring-primary text-text-primary appearance-none cursor-pointer"
        >
          {workflows.map(w => (
            <option key={w} value={w}>{w === 'all' ? 'Workflow' : w}</option>
          ))}
        </select>
        <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3 h-3 text-text-secondary pointer-events-none" />
      </div>

      {/* Hide Children */}
      <button
        onClick={() => onChange({ ...filters, hideChildren: !filters.hideChildren })}
        className={cn(
          'inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md border transition-all',
          filters.hideChildren
            ? 'bg-primary/10 text-primary border-primary/30'
            : 'bg-surface text-text-secondary border-surface-border hover:border-text-secondary/50'
        )}
      >
        {filters.hideChildren ? <EyeOff className="w-3 h-3" /> : <Eye className="w-3 h-3" />}
        Hide Children
        {childCount > 0 && (
          <span className={cn('text-[10px] font-mono px-1 py-0.5 rounded-full',
            filters.hideChildren ? 'bg-primary/20 text-primary' : 'bg-surface-hover text-text-secondary')}>
            {childCount}
          </span>
        )}
      </button>

      <div className="flex-1" />

      {/* Start Workflow CTA */}
      <Button variant="primary" size="md" onClick={onStartWorkflow}>
        <Plus className="w-3.5 h-3.5" />
        Start Workflow
      </Button>
    </div>
  );
}

// =============================================================================
// Runs tab content
// =============================================================================

function RunsTab({
  filters, onChange, childCount, activeViewId, onStartWorkflow, navigate, allViews, runs, definitions,
}: {
  filters: Filters;
  onChange: (f: Filters) => void;
  childCount: number;
  activeViewId: string;
  onStartWorkflow: () => void;
  navigate: (path: string) => void;
  allViews: SavedView[];
  runs: WorkflowRun[];
  definitions: DefinitionListEntry[];
}) {
  const [sortKey, setSortKey] = useState<SortField>('started_at');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  // Updated periodically to keep time-range filters fresh without calling Date.now() during render
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 10_000);
    return () => clearInterval(id);
  }, []);

  const handleSort = (key: string) => {
    const k = key as SortField;
    if (k === sortKey) setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    else { setSortKey(k); setSortDir('desc'); }
  };

  const childCountByParent = useMemo(() => {
    const counts: Record<string, number> = {};
    runs.forEach(r => {
      if (r.parent_run_id) counts[r.parent_run_id] = (counts[r.parent_run_id] || 0) + 1;
    });
    return counts;
  }, [runs]);

  const filteredRuns = useMemo(() => {
    let filtered = [...runs];
    if (filters.hideChildren) filtered = filtered.filter(r => !r.parent_run_id);
    if (filters.status !== 'all') {
      if (filters.status === 'running') filtered = filtered.filter(r => r.status === 'running' || r.status === 'waiting');
      else filtered = filtered.filter(r => r.status === filters.status);
    }
    if (filters.workflow !== 'all') filtered = filtered.filter(r => r.workflow_name === filters.workflow);
    if (filters.search) {
      const q = filters.search.toLowerCase();
      filtered = filtered.filter(r =>
        r.run_id.toLowerCase().includes(q) ||
        r.workflow_name.toLowerCase().includes(q) ||
        (r.current_step || '').toLowerCase().includes(q)
      );
    }
    const activeView = allViews.find(v => v.id === activeViewId);
    if (activeView?.filters.timeRange) {
      if (activeView.filters.timeRange === 'today') {
        const todayStart = new Date(now).setHours(0, 0, 0, 0);
        filtered = filtered.filter(r => r.started_at >= todayStart);
      } else if (activeView.filters.timeRange === 'last_hour') {
        filtered = filtered.filter(r => r.started_at >= (now - 3_600_000));
      }
    }
    filtered.sort((a, b) => {
      let cmp = 0;
      if (sortKey === 'started_at') cmp = a.started_at - b.started_at;
      else if (sortKey === 'duration_ms') cmp = (a.duration_ms || 0) - (b.duration_ms || 0);
      else if (sortKey === 'status') cmp = a.status.localeCompare(b.status);
      else cmp = a.workflow_name.localeCompare(b.workflow_name);
      return sortDir === 'desc' ? -cmp : cmp;
    });
    return filtered;
  }, [runs, filters, activeViewId, allViews, now, sortKey, sortDir]);

  const columns: TableColumn<WorkflowRun>[] = [
    {
      key: 'run_id', label: 'Run ID', width: '18%',
      renderCell: (run) => (
        <div className="flex items-center gap-2">
          <span className="font-mono text-xs text-text-secondary group-hover:text-primary transition-colors">{run.run_id}</span>
          {run.parent_run_id && (
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-indigo-500/10 text-indigo-500 font-medium flex items-center gap-0.5">
              <GitBranch className="w-2.5 h-2.5" />child
            </span>
          )}
          {(childCountByParent[run.run_id] || 0) > 0 && (
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-purple-500/10 text-purple-500 font-medium flex items-center gap-0.5">
              <GitBranch className="w-2.5 h-2.5" />{childCountByParent[run.run_id]} child{childCountByParent[run.run_id] !== 1 ? 'ren' : ''}
            </span>
          )}
        </div>
      ),
    },
    {
      key: 'workflow_name', label: 'Workflow', sortable: true,
      renderCell: (run) => (
        <div className="flex items-center gap-2">
          <GitGraph className="w-3.5 h-3.5 text-primary shrink-0" />
          <span className="font-medium text-text-primary">{run.workflow_name}</span>
          <span className="text-[10px] text-text-secondary">v{run.workflow_version}</span>
        </div>
      ),
    },
    {
      key: 'status', label: 'Status', width: '12%', sortable: true,
      renderCell: (run) => <StatusBadge status={run.status} />,
    },
    {
      key: 'current_step', label: 'Current Step',
      renderCell: (run) => run.current_step ? (
        <div className="flex items-center gap-1.5">
          <Zap className="w-3 h-3 text-text-secondary" />
          <span className="text-text-secondary font-mono text-xs">{run.current_step}</span>
          {run.wait_type && (
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-amber-500/10 text-amber-500 font-medium">{run.wait_type}</span>
          )}
        </div>
      ) : <span className="text-text-secondary/40">—</span>,
    },
    {
      key: 'duration_ms', label: 'Duration', width: '10%', sortable: true,
      renderCell: (run) => (
        <span className="font-mono text-xs text-text-secondary">
          {run.status === 'running' || run.status === 'waiting'
            ? formatDuration(Date.now() - run.started_at)
            : formatDuration(run.duration_ms)}
        </span>
      ),
    },
    {
      key: 'started_at', label: 'Started', width: '10%', sortable: true,
      renderCell: (run) => (
        <div className="flex items-center gap-1 text-text-secondary text-xs">
          <Clock className="w-3 h-3 shrink-0" />
          {formatTimeAgo(run.started_at)}
        </div>
      ),
    },
    {
      key: 'history_event_count', label: 'Events', width: '7%', align: 'right',
      renderCell: (run) => <span className="font-mono text-xs text-text-secondary">{run.history_event_count}</span>,
    },
  ];

  return (
    <div className="flex flex-col gap-4">
      <FilterToolbar
        filters={filters}
        onChange={onChange}
        childCount={childCount}
        onStartWorkflow={onStartWorkflow}
        definitions={definitions}
      />
      <div className="flex items-center gap-1.5 text-xs text-text-secondary">
        <span>{filteredRuns.length} run{filteredRuns.length !== 1 ? 's' : ''}</span>
        {filters.hideChildren && <span className="text-text-secondary/50">· children hidden</span>}
        {activeViewId !== 'all' && (
          <span className="bg-primary/10 text-primary px-2 py-0.5 rounded-full font-medium">
            {allViews.find(v => v.id === activeViewId)?.name}
          </span>
        )}
      </div>
      <DataTable
        columns={columns}
        rows={filteredRuns}
        rowKey={r => r.run_id}
        sortKey={sortKey}
        sortDir={sortDir}
        onSort={handleSort}
        onRowClick={r => navigate(`/workflows/runs/${r.run_id}`)}
        emptyState={
          <div className="py-8">
            <GitGraph className="w-8 h-8 mx-auto mb-2 opacity-30" />
            <p>No workflow runs match your filters.</p>
          </div>
        }
      />
    </div>
  );
}

// =============================================================================
// Definitions tab content
// =============================================================================

function DefinitionRow({ def }: { def: DefinitionListEntry }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div className="border-b border-surface-border last:border-0">
      <div
        className="grid grid-cols-[1fr_180px_160px_90px_70px] items-center px-4 py-3 cursor-pointer hover:bg-surface-hover transition-colors group"
        onClick={() => setExpanded(e => !e)}
      >
        {/* Name */}
        <div className="flex items-center gap-2 min-w-0">
          <ChevronRight className={cn('w-3.5 h-3.5 text-text-secondary/50 transition-transform shrink-0', expanded && 'rotate-90')} />
          <span className="font-mono text-sm text-text-primary font-medium group-hover:text-primary transition-colors truncate">{def.name}</span>
        </div>
        {/* Schedule */}
        <div className="text-xs">
          {def.has_schedule ? (
            <span className="flex items-center gap-1.5 text-text-secondary">
              <CalendarClock className="w-3 h-3 shrink-0" />
              Scheduled
            </span>
          ) : (
            <span className="text-text-secondary/30">—</span>
          )}
        </div>
        {/* Trigger */}
        <div className="text-xs">
          {def.has_trigger ? (
            <span className="flex items-center gap-1.5 text-purple-400">
              <Radio className="w-3 h-3 shrink-0" />
              Stream trigger
            </span>
          ) : (
            <span className="text-text-secondary/30">—</span>
          )}
        </div>
        {/* Steps */}
        <div className="flex items-center gap-2 text-xs text-text-secondary">
          <span className="flex items-center gap-1"><FileCode className="w-3 h-3" />{def.step_count}</span>
          <span className="flex items-center gap-1"><Shield className="w-3 h-3" />{def.plan_count}</span>
        </div>
        {/* Version */}
        <div className="flex justify-end">
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-surface-hover text-text-secondary font-mono">v{def.version}</span>
        </div>
      </div>
      {expanded && (
        <div className="px-8 pb-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
            <div>
              <span className="text-text-secondary">Start Step:</span>{' '}
              <span className="font-mono">{def.start_step}</span>
            </div>
            <div>
              <span className="text-text-secondary">Terminals:</span>{' '}
              <span className="font-mono">{def.terminals.map(t => t.name).join(', ')}</span>
            </div>
            <div>
              <span className="text-text-secondary">Enabled:</span>{' '}
              <span className={def.enabled ? 'text-success' : 'text-error'}>{def.enabled ? 'Yes' : 'No'}</span>
            </div>
            <div>
              <span className="text-text-secondary">Steps:</span>{' '}
              <span className="font-mono">{def.steps.map(s => s.name).join(' → ')}</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function DefinitionsTab({ onCreateDefinition, definitions }: { onCreateDefinition: () => void; definitions: DefinitionListEntry[] }) {
  const [search, setSearch] = useState('');
  const filtered = useMemo(() => {
    if (!search) return definitions;
    const q = search.toLowerCase();
    return definitions.filter(d => d.name.toLowerCase().includes(q));
  }, [search, definitions]);

  return (
    <div className="flex flex-col gap-4">
      {/* Toolbar */}
      <div className="flex items-center gap-2">
        <div className="relative flex-1 min-w-[180px] max-w-xs">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-secondary pointer-events-none" />
          <input
            type="text"
            placeholder="Search for a definition..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="w-full pl-8 pr-3 py-1.5 text-sm bg-surface border border-surface-border rounded-md focus:outline-none focus:ring-1 focus:ring-primary text-text-primary placeholder:text-text-secondary/60"
          />
        </div>
        <div className="flex-1" />
        <Button variant="primary" size="md" onClick={onCreateDefinition}>
          <Plus className="w-3.5 h-3.5" />
          Create a new definition
        </Button>
      </div>

      {/* Table header */}
      <div className="border border-surface-border rounded-md overflow-hidden">
        <div className="grid grid-cols-[1fr_180px_160px_90px_70px] items-center px-4 py-2.5 border-b border-surface-border bg-surface/50">
          <span className="text-xs font-medium text-text-secondary uppercase tracking-wider">Name</span>
          <span className="text-xs font-medium text-text-secondary uppercase tracking-wider">Schedule</span>
          <span className="text-xs font-medium text-text-secondary uppercase tracking-wider">Trigger</span>
          <span className="text-xs font-medium text-text-secondary uppercase tracking-wider">Steps</span>
          <span className="text-xs font-medium text-text-secondary uppercase tracking-wider text-right">Version</span>
        </div>
        {filtered.length === 0 ? (
          <div className="py-12 text-center text-text-secondary text-sm">No definitions found.</div>
        ) : (
          filtered.map(def => <DefinitionRow key={def.name} def={def} />)
        )}
      </div>
    </div>
  );
}

// =============================================================================
// Main page
// =============================================================================

export function WorkflowsListPage() {
  const navigate = useNavigate();
  const { activeViewId, customViews, reportFilters } = useOutletContext<WorkflowOutletContext>();
  const allViews = useMemo(() => [...SYSTEM_VIEWS, ...customViews], [customViews]);
  const [activeTab, setActiveTab] = useState<'runs' | 'definitions'>('runs');
  const [showStartModal, setShowStartModal] = useState(false);
  const [filters, setFilters] = useState<Filters>({
    status: 'all', workflow: 'all', search: '', hideChildren: true,
  });

  // Fetch runs and definitions from the real API (poll runs every 10s)
  const { runs, error: runsError, refetch: refetchRuns } = useWorkflowRuns({ pollInterval: 10000 });
  const { definitions } = useWorkflowDefinitions();

  // Report current filters to layout so "Save View" captures the right state
  useEffect(() => {
    reportFilters({
      status: filters.status !== 'all' ? filters.status : undefined,
      workflow: filters.workflow !== 'all' ? filters.workflow : undefined,
      search: filters.search || undefined,
      hideChildren: filters.hideChildren,
    });
  }, [filters, reportFilters]);

  const childCount = useMemo(() => runs.filter(r => !!r.parent_run_id).length, [runs]);

  // Sync filter state when the sidebar view selection changes
  useEffect(() => {
    const view = allViews.find(v => v.id === activeViewId);
    if (!view) return;
    setFilters(f => ({
      ...f,
      status: (view.filters.status as StatusFilter) || 'all',
      workflow: view.filters.workflow || 'all',
      search: view.filters.search || '',
      hideChildren: view.filters.hideChildren ?? f.hideChildren,
    }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeViewId]);

  const totalRuns = runs.length;
  const totalDefs = definitions.length;

  const tabs = [
    { id: 'runs', label: 'Recent Runs', count: totalRuns },
    { id: 'definitions', label: 'Definitions', count: totalDefs },
  ];

  return (
    <>
    <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        {/* Page header */}
        <div className="px-8 pt-8 pb-0 shrink-0">
          <div className="flex items-start justify-between mb-1">
            <div>
              <h1 className="text-2xl font-bold tracking-tight">Workflows</h1>
              <p className="text-sm text-text-secondary mt-0.5">
                Distributed workflow orchestration for long-running processes
              </p>
            </div>
          </div>

          {/* Tabs */}
          <div className="mt-5">
            <PageTabs
              tabs={tabs}
              activeTab={activeTab}
              onChange={id => setActiveTab(id as 'runs' | 'definitions')}
            />
          </div>
        </div>

        {/* Tab content */}
        <div className="flex-1 overflow-auto px-8 py-6">
          {runsError && (
            <div className="mb-4 px-4 py-3 rounded-md bg-error/10 border border-error/20 text-sm text-error flex items-center gap-2">
              <AlertCircle className="w-4 h-4 shrink-0" />
              <span>Failed to load workflow runs: {runsError}</span>
              <button onClick={refetchRuns} className="ml-auto text-xs underline hover:no-underline">Retry</button>
            </div>
          )}
          {activeTab === 'runs' ? (
            <RunsTab
              filters={filters}
              onChange={setFilters}
              childCount={childCount}
              activeViewId={activeViewId}
              onStartWorkflow={() => setShowStartModal(true)}
              navigate={navigate}
              allViews={allViews}
              runs={runs}
              definitions={definitions}
            />
          ) : (
            <DefinitionsTab onCreateDefinition={() => navigate('/workflows/definitions')} definitions={definitions} />
          )}
        </div>
    </div>

    {showStartModal && <StartWorkflowModal onClose={() => { setShowStartModal(false); refetchRuns(); }} definitions={definitions} />}
    </>
  );
}
