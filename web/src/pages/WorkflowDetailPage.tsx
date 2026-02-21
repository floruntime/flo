import { useState, useMemo, useCallback, useRef, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  ArrowLeft,
  GitGraph,
  Play,
  CheckCircle,
  XCircle,
  Clock,
  Pause,
  Timer,
  Ban,
  Zap,
  Signal,
  Activity,
  ChevronDown,
  AlertTriangle,
  Copy,
  FileCode,
  History,
  Layers,
  Send,
  Link2,
  Tag,
  GanttChart,
  List,
  GitBranch,
  Filter,
  ZoomIn,
  ZoomOut,
  Maximize2,
} from 'lucide-react';
import { Card, CardContent, CardHeader } from '../components/ui/Card';
import { Button } from '../components/ui/Button';
import { cn } from '../lib/utils';


import type {
  WorkflowRunStatus,
  WorkflowRun,
  HistoryEvent,
  HistoryEventType,
  EventGroup,
  EventGroupCategory,
  EventGroupStatus,
} from '../lib/workflow-types';
import { useWorkflowRun, useWorkflowRuns, useWorkflowDefinitions } from '../lib/workflow-hooks';
import type { DefinitionListEntry } from '../lib/workflow-api';
import * as workflowApi from '../lib/workflow-api';

// =============================================================================
// Hooks
// =============================================================================

/** Returns a current timestamp that updates every `intervalMs`. Avoids calling Date.now() directly during render. */
function useNow(intervalMs = 1000) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return now;
}

// =============================================================================
// Constants & Helpers
// =============================================================================

const STATUS_CONFIG: Record<
  WorkflowRunStatus,
  { label: string; icon: typeof Play; color: string; bgColor: string; ringColor: string }
> = {
  pending: { label: 'Pending', icon: Clock, color: 'text-text-secondary', bgColor: 'bg-text-secondary/10', ringColor: 'ring-text-secondary/30' },
  running: { label: 'Running', icon: Play, color: 'text-blue-500', bgColor: 'bg-blue-500/10', ringColor: 'ring-blue-500/30' },
  waiting: { label: 'Waiting', icon: Pause, color: 'text-amber-500', bgColor: 'bg-amber-500/10', ringColor: 'ring-amber-500/30' },
  completed: { label: 'Completed', icon: CheckCircle, color: 'text-success', bgColor: 'bg-success/10', ringColor: 'ring-success/30' },
  failed: { label: 'Failed', icon: XCircle, color: 'text-error', bgColor: 'bg-error/10', ringColor: 'ring-error/30' },
  cancelled: { label: 'Cancelled', icon: Ban, color: 'text-text-secondary', bgColor: 'bg-text-secondary/10', ringColor: 'ring-text-secondary/30' },
  timed_out: { label: 'Timed Out', icon: Timer, color: 'text-orange-500', bgColor: 'bg-orange-500/10', ringColor: 'ring-orange-500/30' },
};

const EVENT_ICONS: Partial<Record<HistoryEventType, typeof Play>> = {
  workflow_started: Play,
  workflow_completed: CheckCircle,
  workflow_failed: XCircle,
  workflow_cancelled: Ban,
  workflow_timed_out: Timer,
  step_started: Zap,
  step_completed: CheckCircle,
  step_failed: XCircle,
  plan_started: Layers,
  plan_executor_tried: Activity,
  plan_completed: CheckCircle,
  signal_received: Signal,
  timer_scheduled: Clock,
  timer_fired: Timer,
  child_workflow_started: GitGraph,
  child_workflow_completed: CheckCircle,
};

const EVENT_COLORS: Partial<Record<HistoryEventType, string>> = {
  workflow_started: 'text-blue-500 bg-blue-500/10',
  workflow_completed: 'text-success bg-success/10',
  workflow_failed: 'text-error bg-error/10',
  workflow_cancelled: 'text-text-secondary bg-text-secondary/10',
  workflow_timed_out: 'text-orange-500 bg-orange-500/10',
  step_started: 'text-blue-400 bg-blue-400/10',
  step_completed: 'text-success bg-success/10',
  step_failed: 'text-error bg-error/10',
  plan_started: 'text-purple-500 bg-purple-500/10',
  plan_executor_tried: 'text-amber-500 bg-amber-500/10',
  plan_completed: 'text-purple-500 bg-purple-500/10',
  signal_received: 'text-cyan-500 bg-cyan-500/10',
  timer_scheduled: 'text-text-secondary bg-text-secondary/10',
  timer_fired: 'text-orange-500 bg-orange-500/10',
  child_workflow_started: 'text-indigo-500 bg-indigo-500/10',
  child_workflow_completed: 'text-indigo-500 bg-indigo-500/10',
};

const CATEGORY_ICONS: Record<EventGroupCategory, typeof Play> = {
  workflow: GitGraph,
  step: Zap,
  plan: Layers,
  signal: Signal,
  timer: Clock,
  child_workflow: GitBranch,
};

const CATEGORY_COLORS: Record<EventGroupCategory, string> = {
  workflow: 'text-blue-500 bg-blue-500/10 border-blue-500/30',
  step: 'text-emerald-500 bg-emerald-500/10 border-emerald-500/30',
  plan: 'text-purple-500 bg-purple-500/10 border-purple-500/30',
  signal: 'text-cyan-500 bg-cyan-500/10 border-cyan-500/30',
  timer: 'text-orange-500 bg-orange-500/10 border-orange-500/30',
  child_workflow: 'text-indigo-500 bg-indigo-500/10 border-indigo-500/30',
};

const STATUS_BAR_COLORS: Record<EventGroupStatus, string> = {
  completed: 'bg-success',
  failed: 'bg-error',
  running: 'bg-blue-500',
  pending: 'bg-purple-400',
  cancelled: 'bg-text-secondary',
  timed_out: 'bg-orange-500',
};

function formatDuration(ms: number | undefined | null): string {
  if (ms == null) return '-';
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ${Math.floor((ms % 60_000) / 1000)}s`;
  if (ms < 86_400_000) return `${Math.floor(ms / 3_600_000)}h ${Math.floor((ms % 3_600_000) / 60_000)}m`;
  return `${Math.floor(ms / 86_400_000)}d ${Math.floor((ms % 86_400_000) / 3_600_000)}h`;
}

function formatTimestamp(ts: number): string {
  return new Date(ts).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function formatTimeAgo(ts: number): string {
  const diff = Date.now() - ts;
  if (diff < 60_000) return `${Math.floor(diff / 1000)}s ago`;
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
  return `${Math.floor(diff / 86_400_000)}d ago`;
}

function copyToClipboard(text: string) {
  navigator.clipboard.writeText(text);
}

// =============================================================================
// Event Grouping Logic (Temporal-style)
// =============================================================================

function buildEventGroups(events: HistoryEvent[]): EventGroup[] {
  const groups: EventGroup[] = [];
  const stepGroups: Record<string, EventGroup> = {};

  for (const evt of events) {
    // Workflow lifecycle events are standalone groups
    if (evt.event_type === 'workflow_started') {
      groups.push({
        id: 'workflow:start',
        category: 'workflow',
        label: 'Workflow Started',
        status: 'completed',
        events: [evt],
        started_at: evt.timestamp,
        completed_at: evt.timestamp,
        duration_ms: 0,
        summary: evt.details,
      });
      continue;
    }
    if (
      evt.event_type === 'workflow_completed' ||
      evt.event_type === 'workflow_failed' ||
      evt.event_type === 'workflow_cancelled' ||
      evt.event_type === 'workflow_timed_out'
    ) {
      const status: EventGroupStatus =
        evt.event_type === 'workflow_completed'
          ? 'completed'
          : evt.event_type === 'workflow_failed'
            ? 'failed'
            : evt.event_type === 'workflow_cancelled'
              ? 'cancelled'
              : 'timed_out';
      groups.push({
        id: `workflow:${status}`,
        category: 'workflow',
        label: `Workflow ${status.charAt(0).toUpperCase() + status.slice(1)}`,
        status,
        events: [evt],
        started_at: evt.timestamp,
        completed_at: evt.timestamp,
        duration_ms: 0,
        summary: { ...evt.details, error: evt.error },
      });
      continue;
    }

    // Step events are grouped by step_name
    if (evt.step_name) {
      const key = evt.step_name;
      if (!stepGroups[key]) {
        const category: EventGroupCategory = 'step';
        stepGroups[key] = {
          id: `step:${key}`,
          category,
          label: key,
          step_name: key,
          status: 'running',
          events: [],
          started_at: evt.timestamp,
          summary: {},
          executor_attempts: [],
        };
      }
      const sg = stepGroups[key];
      sg.events.push(evt);

      // Upgrade category if we see plan events
      if (
        evt.event_type === 'plan_started' ||
        evt.event_type === 'plan_executor_tried' ||
        evt.event_type === 'plan_completed'
      ) {
        sg.category = 'plan';
      }
      if (evt.event_type === 'signal_received') {
        sg.category = 'signal';
        sg.signal_type = evt.signal_type;
        sg.signal_payload = evt.signal_payload;
      }
      if (evt.event_type === 'timer_scheduled' || evt.event_type === 'timer_fired') {
        if (sg.category === 'step') sg.category = 'timer';
      }
      if (evt.event_type === 'child_workflow_started' || evt.event_type === 'child_workflow_completed') {
        sg.category = 'child_workflow';
      }

      // Track executor attempts
      if (evt.event_type === 'plan_executor_tried' && evt.executor_name) {
        sg.executor_attempts!.push({
          executor: evt.executor_name,
          success: !evt.error && !!evt.details?.success,
          error: evt.error || (evt.details?.error as string | undefined),
          duration_ms: evt.details?.duration_ms as number | undefined,
        });
      }
      if (evt.event_type === 'plan_completed' && evt.details?.selected_executor) {
        sg.selected_executor = evt.details.selected_executor as string;
      }

      // Update status on completion/failure
      if (evt.event_type === 'step_completed') {
        sg.status = 'completed';
        sg.completed_at = evt.timestamp;
        sg.duration_ms = evt.duration_ms || evt.timestamp - sg.started_at;
        sg.summary = { ...sg.summary, outcome: evt.outcome, output: evt.details?.output };
      }
      if (evt.event_type === 'step_failed') {
        sg.status = 'failed';
        sg.completed_at = evt.timestamp;
        sg.duration_ms = evt.duration_ms || evt.timestamp - sg.started_at;
        sg.summary = { ...sg.summary, error: evt.error };
      }

      // Check if this is a signal wait step
      if (evt.event_type === 'step_started' && evt.details?.signal_type) {
        sg.category = 'signal';
        sg.signal_type = evt.details.signal_type as string;
      }
    }
  }

  // Convert step groups to array and add to groups
  const orderedStepGroups = Object.values(stepGroups).sort((a, b) => a.started_at - b.started_at);
  groups.push(...orderedStepGroups);

  // Sort all groups by start time
  groups.sort((a, b) => a.started_at - b.started_at);

  return groups;
}

// =============================================================================
// Detail tabs
// =============================================================================

type HistoryViewMode = 'compact' | 'timeline' | 'full';
type DetailTab = 'history' | 'relationships' | 'metadata' | 'definition';

// =============================================================================
// Status Banner
// =============================================================================

function StatusBanner({ run, onCancel, onSignal, onRetry }: {
  run: WorkflowRun;
  onCancel: () => void;
  onSignal: () => void;
  onRetry: () => void;
}) {
  const now = useNow();
  const config = STATUS_CONFIG[run.status];
  const Icon = config.icon;
  const isTerminal = ['completed', 'failed', 'cancelled', 'timed_out'].includes(run.status);
  const elapsed = isTerminal ? run.duration_ms : now - run.started_at;

  return (
    <div className={cn('rounded-lg border border-surface-border p-4')}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className={cn('rounded-full p-2', config.bgColor)}>
            <Icon className={cn('w-5 h-5', config.color)} />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className={cn('font-semibold text-lg', config.color)}>{config.label}</span>
              {run.wait_type && (
                <span className="text-xs px-2 py-0.5 rounded-full bg-amber-500/10 text-amber-500 font-medium">
                  waiting: {run.wait_type}
                </span>
              )}
            </div>
            <p className="text-sm text-text-secondary mt-0.5">
              {run.error ||
                (isTerminal
                  ? `Finished ${formatTimeAgo(run.completed_at!)}`
                  : `Running for ${formatDuration(elapsed)}`)}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {run.status === 'running' || run.status === 'waiting' ? (
            <>
              <Button variant="danger" onClick={onCancel}>
                Cancel
              </Button>
              {run.wait_type === 'signal' && (
                <Button variant="primary" onClick={onSignal}>
                  <Send />
                  Send Signal
                </Button>
              )}
            </>
          ) : run.status === 'failed' ? (
            <Button variant="primary" onClick={onRetry}>
              <Play />
              Retry
            </Button>
          ) : null}
        </div>
      </div>
    </div>
  );
}

// =============================================================================
// Summary Cards Row
// =============================================================================

function SummaryCards({ run, definition }: { run: WorkflowRun; definition?: DefinitionListEntry }) {
  const now = useNow();
  const totalSteps = definition?.step_count ?? 0;
  const completedSteps = Object.keys(run.step_results || {}).length;

  const items = [
    { label: 'Workflow', value: run.workflow_name, sub: `v${run.workflow_version}` },
    { label: 'Run ID', value: run.run_id, mono: true, copyable: true },
    { label: 'Started', value: formatTimestamp(run.started_at) },
    { label: 'Duration', value: formatDuration(run.duration_ms || now - run.started_at) },
    { label: 'Steps', value: `${completedSteps} / ${totalSteps}` },
    { label: 'Events', value: String(run.history_event_count ?? 0) },
  ];

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
      {items.map((item) => (
        <Card key={item.label}>
          <CardContent className="p-3">
            <p className="text-[10px] uppercase tracking-wider text-text-secondary font-medium mb-1">
              {item.label}
            </p>
            <div className="flex items-center gap-1.5">
              <p className={cn('text-sm font-medium truncate', item.mono && 'font-mono text-xs')}>
                {item.value}
              </p>
              {item.sub && <span className="text-[10px] text-text-secondary">{item.sub}</span>}
              {item.copyable && (
                <button
                  onClick={() => copyToClipboard(item.value)}
                  className="text-text-secondary hover:text-text-primary transition-colors shrink-0"
                >
                  <Copy className="w-3 h-3" />
                </button>
              )}
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

// =============================================================================
// Step Progress Bar with animated pending lines
// =============================================================================

function StepProgressBar({ run, definition }: { run: WorkflowRun; definition?: DefinitionListEntry }) {
  if (!definition) return null;

  // Use step names from definition (already ordered)
  const stepOrder = definition.steps.map(s => s.name);

  const isRunFinished = ['completed', 'failed', 'timed_out', 'cancelled'].includes(run.status);

  type StepState = 'done' | 'done-error' | 'active' | 'waiting' | 'pending';
  const getState = (name: string): StepState => {
    if (name === '__END__') return isRunFinished ? 'done' : 'pending';
    const result = (run.step_results || {})[name];
    if (result) {
      return result.outcome === 'success' || result.outcome === 'received' ? 'done' : 'done-error';
    }
    if (run.current_step === name) return run.status === 'waiting' ? 'waiting' : 'active';
    return 'pending';
  };

  // colours per state
  const STATE_STYLES: Record<StepState, { pill: string; icon: string; label: string; connector: string }> = {
    'done':       { pill: 'border-success/30 bg-success/5',         icon: 'text-success',          label: 'text-text-primary',           connector: 'bg-success/40' },
    'done-error': { pill: 'border-error/40 bg-error/5',             icon: 'text-error',             label: 'text-text-primary',           connector: 'bg-error/40' },
    'active':     { pill: 'border-blue-500/50 bg-blue-500/5',       icon: 'text-blue-400',          label: 'text-text-primary',           connector: 'bg-blue-500/30' },
    'waiting':    { pill: 'border-amber-500/50 bg-amber-500/5',     icon: 'text-amber-400',         label: 'text-text-primary',           connector: 'bg-amber-500/30' },
    'pending':    { pill: 'border-surface-border/60 bg-transparent', icon: 'text-text-secondary/30', label: 'text-text-secondary/40',     connector: 'bg-surface-border/30' },
  };

  function StepIcon({ state, isEnd }: { state: StepState; isEnd?: boolean }) {
    if (isEnd) {
      const s = STATE_STYLES[state];
      return (
        <div className={cn('w-3.5 h-3.5 rounded-sm flex items-center justify-center shrink-0', s.icon)}>
          {state === 'done' ? (
            <CheckCircle className="w-3 h-3" />
          ) : state === 'done-error' ? (
            <XCircle className="w-3 h-3" />
          ) : (
            <div className="w-1.5 h-1.5 rounded-full bg-current opacity-40" />
          )}
        </div>
      );
    }
    if (state === 'done')
      return <CheckCircle className={cn('w-3.5 h-3.5 shrink-0', STATE_STYLES[state].icon)} />;
    if (state === 'done-error')
      return <XCircle className={cn('w-3.5 h-3.5 shrink-0', STATE_STYLES[state].icon)} />;
    if (state === 'active')
      return (
        <div className="w-3.5 h-3.5 shrink-0 flex items-center justify-center">
          <div className="w-2 h-2 rounded-full bg-blue-500 animate-pulse" />
        </div>
      );
    if (state === 'waiting')
      return (
        <div className="w-3.5 h-3.5 shrink-0 flex items-center justify-center">
          <div className="w-2 h-2 rounded-full bg-amber-400 animate-pulse" />
        </div>
      );
    return (
      <div className="w-3.5 h-3.5 shrink-0 flex items-center justify-center">
        <div className="w-1.5 h-1.5 rounded-full bg-surface-border/60" />
      </div>
    );
  }

  const allNodes = [...stepOrder, '__END__'];

  return (
    <div className="flex items-center gap-0 overflow-x-auto py-1 scrollbar-thin">
      {allNodes.map((name, idx) => {
        const isEnd = name === '__END__';
        const state = getState(name);
        const s = STATE_STYLES[state];
        const result = !isEnd ? (run.step_results || {})[name] : null;
        const label = isEnd ? 'END' : name;
        const isLast = idx === allNodes.length - 1;

        return (
          <div key={name} className="flex items-center shrink-0">
            {/* Connector from previous */}
            {idx > 0 && (
              <div className="flex items-center shrink-0">
                <div className={cn('w-1.5 h-1.5 rounded-full shrink-0', STATE_STYLES[getState(allNodes[idx - 1])].connector)} />
                <div className={cn('w-6 h-px shrink-0', s.connector)} />
                <div className={cn('w-1.5 h-1.5 rounded-full shrink-0', s.connector)} />
              </div>
            )}

            {/* Pill */}
            <div
              title={label}
              className={cn(
                'flex items-center gap-1.5 px-2.5 py-1 rounded-md border shrink-0 transition-all',
                s.pill,
                isEnd ? 'px-2' : '',
              )}
            >
              <StepIcon state={state} isEnd={isEnd} />
              <span className={cn(
                'font-medium leading-none',
                isEnd ? 'text-[10px] font-mono' : 'text-xs',
                s.label,
              )}>
                {label}
              </span>
              {result?.duration_ms != null && (
                <span className="text-[10px] text-text-secondary/40 font-mono leading-none ml-0.5">
                  {formatDuration(result.duration_ms)}
                </span>
              )}
            </div>

            {/* Right-edge connector dot for last item */}
            {isLast && isRunFinished && (
              <div className="flex items-center shrink-0 ml-0">
                <div className={cn('w-1.5 h-1.5 rounded-full shrink-0 ml-1', s.connector)} />
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// =============================================================================
// Compact View (Temporal-style event groups)
// =============================================================================

function CompactView({
  groups,
  eventFilter,
}: {
  groups: EventGroup[];
  eventFilter: Set<EventGroupCategory>;
}) {
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());

  const filtered = useMemo(
    () => groups.filter((g) => eventFilter.size === 0 || eventFilter.has(g.category)),
    [groups, eventFilter]
  );

  // Detect same-time groups (within 5ms) for vertical stacking
  const timeGroups: EventGroup[][] = [];
  for (const g of filtered) {
    const lastGroup = timeGroups[timeGroups.length - 1];
    if (lastGroup && Math.abs(g.started_at - lastGroup[0].started_at) < 5) {
      lastGroup.push(g);
    } else {
      timeGroups.push([g]);
    }
  }

  const toggle = (id: string) => {
    setExpandedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  return (
    <div className="space-y-1">
      {timeGroups.map((stack, stackIdx) => (
        <div key={stackIdx} className="flex flex-col gap-1">
          {stack.map((group) => {
            const isExpanded = expandedGroups.has(group.id);
            const catColors = CATEGORY_COLORS[group.category];
            const catParts = catColors.split(' ');
            const textCol = catParts[0];
            const bgCol = catParts[1];
            const Icon = CATEGORY_ICONS[group.category];

            return (
              <div key={group.id}>
                <div
                  className={cn(
                    'flex items-center gap-3 px-3 py-2 rounded-md border border-surface-border cursor-pointer transition-all hover:shadow-sm',
                    isExpanded && 'shadow-sm'
                  )}
                  onClick={() => toggle(group.id)}
                >
                  {/* Icon */}
                  <div className={cn('rounded-full p-1.5 shrink-0', bgCol)}>
                    <Icon className={cn('w-3 h-3', textCol)} />
                  </div>

                  {/* Label & info */}
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-medium text-text-primary truncate">
                        {group.label}
                      </span>
                      {group.category === 'plan' && group.selected_executor && (
                        <span className="text-[10px] px-1.5 py-0.5 rounded bg-purple-500/10 text-purple-500 font-medium">
                          {group.selected_executor}
                        </span>
                      )}
                      {group.category === 'plan' &&
                        group.executor_attempts &&
                        group.executor_attempts.length > 1 && (
                          <span className="text-[10px] text-text-secondary">
                            {group.executor_attempts.length} attempts
                          </span>
                        )}
                      {group.category === 'signal' && group.signal_type && (
                        <span className="text-[10px] px-1.5 py-0.5 rounded bg-cyan-500/10 text-cyan-500 font-mono">
                          {group.signal_type}
                        </span>
                      )}
                    </div>
                  </div>

                  {/* Duration & status */}
                  <div className="flex items-center gap-2 shrink-0">
                    {group.duration_ms != null && (
                      <span className="text-[10px] font-mono text-text-secondary px-1.5 py-0.5 bg-surface-hover rounded">
                        {formatDuration(group.duration_ms)}
                      </span>
                    )}
                    <span
                      className={cn(
                        'text-[10px] font-medium px-1.5 py-0.5 rounded',
                        group.status === 'completed'
                          ? 'bg-success/10 text-success'
                          : group.status === 'failed'
                            ? 'bg-error/10 text-error'
                            : group.status === 'running'
                              ? 'bg-blue-500/10 text-blue-500'
                              : group.status === 'pending'
                                ? 'bg-purple-400/10 text-purple-400'
                                : 'bg-text-secondary/10 text-text-secondary'
                      )}
                    >
                      {group.status}
                    </span>
                    <span className="text-[10px] text-text-secondary w-[85px] text-right">
                      {formatTimestamp(group.started_at)}
                    </span>
                    <ChevronDown
                      className={cn(
                        'w-3 h-3 text-text-secondary transition-transform',
                        isExpanded && 'rotate-180'
                      )}
                    />
                  </div>
                </div>

                {/* Expanded summary */}
                {isExpanded && (
                  <div
                    className="ml-10 mt-1 mb-2 border border-surface-border rounded-md p-3 space-y-2"
                  >
                    {/* Executor attempts for plan groups */}
                    {Array.isArray(group.executor_attempts) && group.executor_attempts.length > 0 && (
                      <div>
                        <p className="text-[10px] uppercase tracking-wider text-text-secondary font-medium mb-1.5">
                          Executor Attempts
                        </p>
                        <div className="space-y-1">
                          {group.executor_attempts.map((att, i) => (
                            <div key={i} className="flex items-center gap-2 text-xs">
                              <div
                                className={cn(
                                  'w-2 h-2 rounded-full shrink-0',
                                  att.success ? 'bg-success' : 'bg-error'
                                )}
                              />
                              <span className="font-mono text-text-primary">{att.executor}</span>
                              {att.success ? (
                                <CheckCircle className="w-3 h-3 text-success" />
                              ) : (
                                <span className="text-error truncate">
                                  {att.error || 'failed'}
                                </span>
                              )}
                              {att.duration_ms != null && (
                                <span className="text-text-secondary font-mono ml-auto">
                                  {att.duration_ms}ms
                                </span>
                              )}
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                    {/* Signal payload */}
                    {group.signal_payload != null && (
                      <div>
                        <p className="text-[10px] uppercase tracking-wider text-text-secondary font-medium mb-1">
                          Signal Payload
                        </p>
                        <pre className="text-xs bg-background rounded p-2 overflow-x-auto text-text-secondary font-mono border border-surface-border">
                          {JSON.stringify(group.signal_payload, null, 2)}
                        </pre>
                      </div>
                    )}
                    {/* Summary data */}
                    {group.summary && Object.keys(group.summary).length > 0 && (
                      <div>
                        <p className="text-[10px] uppercase tracking-wider text-text-secondary font-medium mb-1">
                          Details
                        </p>
                        <pre className="text-xs bg-background rounded p-2 overflow-x-auto text-text-secondary font-mono border border-surface-border">
                          {JSON.stringify(group.summary, null, 2)}
                        </pre>
                      </div>
                    )}
                    {/* Individual events */}
                    <div>
                      <p className="text-[10px] uppercase tracking-wider text-text-secondary font-medium mb-1">
                        Events ({group.events.length})
                      </p>
                      <div className="space-y-0.5">
                        {group.events.map((evt) => {
                          const EvtIcon = EVENT_ICONS[evt.event_type] || Activity;
                          const evtColor =
                            EVENT_COLORS[evt.event_type] ||
                            'text-text-secondary bg-text-secondary/10';
                          const evtParts = evtColor.split(' ');
                          const eTxt = evtParts[0];
                          const eBg = evtParts[1];
                          return (
                            <div
                              key={evt.event_id}
                              className="flex items-center gap-2 text-xs py-0.5"
                            >
                              <div className={cn('rounded-full p-0.5', eBg)}>
                                <EvtIcon className={cn('w-2.5 h-2.5', eTxt)} />
                              </div>
                              <span className="text-text-primary">
                                {evt.event_type.replace(/_/g, ' ')}
                              </span>
                              <span className="text-text-secondary font-mono text-[10px] ml-auto">
                                #{evt.event_id}
                              </span>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}

// =============================================================================
// Timeline / Gantt View (Temporal-style horizontal time bars)
// =============================================================================

function TimelineView({
  groups,
  eventFilter,
  workflowStartedAt,
  workflowEndedAt,
}: {
  groups: EventGroup[];
  eventFilter: Set<EventGroupCategory>;
  workflowStartedAt: number;
  workflowEndedAt: number;
}) {
  const [zoomLevel, setZoomLevel] = useState(1);
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());
  const containerRef = useRef<HTMLDivElement>(null);

  const filtered = useMemo(
    () => groups.filter((g) => eventFilter.size === 0 || eventFilter.has(g.category)),
    [groups, eventFilter]
  );

  const totalDuration = Math.max(workflowEndedAt - workflowStartedAt, 1);
  const baseWidth = 800;
  const timelineWidth = baseWidth * zoomLevel;

  const getPosition = useCallback(
    (ts: number) => {
      return ((ts - workflowStartedAt) / totalDuration) * timelineWidth;
    },
    [workflowStartedAt, totalDuration, timelineWidth]
  );

  const getWidth = useCallback(
    (start: number, end: number) => {
      return Math.max(((end - start) / totalDuration) * timelineWidth, 4);
    },
    [totalDuration, timelineWidth]
  );

  // Generate time axis labels
  const timeLabels = useMemo(() => {
    const labels: { position: number; label: string }[] = [];
    const labelCount = Math.max(4, Math.floor(zoomLevel * 6));
    for (let i = 0; i <= labelCount; i++) {
      const ts = workflowStartedAt + (totalDuration / labelCount) * i;
      labels.push({
        position: getPosition(ts),
        label: formatDuration(ts - workflowStartedAt),
      });
    }
    return labels;
  }, [workflowStartedAt, totalDuration, zoomLevel, getPosition]);

  const toggle = (id: string) => {
    setExpandedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  return (
    <div className="space-y-2">
      {/* Zoom controls */}
      <div className="flex items-center gap-2 justify-end">
        <button
          onClick={() => setZoomLevel((z) => Math.max(0.5, z - 0.5))}
          className="p-1 rounded hover:bg-surface-hover text-text-secondary transition-colors"
          title="Zoom out"
        >
          <ZoomOut className="w-3.5 h-3.5" />
        </button>
        <span className="text-[10px] text-text-secondary font-mono">{zoomLevel.toFixed(1)}x</span>
        <button
          onClick={() => setZoomLevel((z) => Math.min(5, z + 0.5))}
          className="p-1 rounded hover:bg-surface-hover text-text-secondary transition-colors"
          title="Zoom in"
        >
          <ZoomIn className="w-3.5 h-3.5" />
        </button>
        <button
          onClick={() => setZoomLevel(1)}
          className="p-1 rounded hover:bg-surface-hover text-text-secondary transition-colors"
          title="Fit to view"
        >
          <Maximize2 className="w-3.5 h-3.5" />
        </button>
      </div>

      {/* Timeline container */}
      <div
        ref={containerRef}
        className="overflow-x-auto border border-surface-border rounded-md bg-background"
      >
        <div style={{ width: timelineWidth + 180, minWidth: '100%' }}>
          {/* Time axis */}
          <div
            className="flex items-end h-6 border-b border-surface-border relative"
            style={{ marginLeft: 160 }}
          >
            {timeLabels.map((tl, i) => (
              <div
                key={i}
                className="absolute text-[9px] text-text-secondary font-mono transform -translate-x-1/2"
                style={{ left: tl.position }}
              >
                {tl.label}
              </div>
            ))}
          </div>

          {/* Workflow execution bar (top row) */}
          <div className="flex items-center h-8 border-b border-surface-border/50">
            <div className="w-40 shrink-0 px-3 flex items-center gap-1.5">
              <GitGraph className="w-3 h-3 text-blue-500" />
              <span className="text-[10px] font-medium text-text-primary truncate">
                Workflow Execution
              </span>
            </div>
            <div className="relative flex-1 h-full flex items-center">
              <div
                className="h-3 rounded-full bg-blue-500/30 border border-blue-500/50 absolute"
                style={{
                  left: 0,
                  width: Math.max(getWidth(workflowStartedAt, workflowEndedAt), 20),
                }}
              />
            </div>
          </div>

          {/* Event group rows */}
          {filtered.map((group) => {
            const catColors = CATEGORY_COLORS[group.category];
            const catParts = catColors.split(' ');
            const textCol = catParts[0];
            const Icon = CATEGORY_ICONS[group.category];
            const barColor = STATUS_BAR_COLORS[group.status] || 'bg-surface-border';
            const isExpanded = expandedGroups.has(group.id);
            const endTime = group.completed_at || workflowEndedAt;
            const isPending = group.status === 'running' || group.status === 'pending';
            const left = getPosition(group.started_at);
            const width = getWidth(group.started_at, endTime);

            return (
              <div key={group.id}>
                <div
                  className="flex items-center h-8 border-b border-surface-border/30 hover:bg-surface-hover/50 cursor-pointer transition-colors"
                  onClick={() => toggle(group.id)}
                >
                  <div className="w-40 shrink-0 px-3 flex items-center gap-1.5 overflow-hidden">
                    <Icon className={cn('w-3 h-3 shrink-0', textCol)} />
                    <span className="text-[10px] font-mono text-text-primary truncate">
                      {group.label}
                    </span>
                  </div>
                  <div className="relative flex-1 h-full flex items-center">
                    {/* The bar */}
                    <div
                      className={cn(
                        'h-4 rounded-sm absolute flex items-center justify-end pr-1 transition-all',
                        barColor,
                        isPending && 'animate-pulse opacity-70'
                      )}
                      style={{ left, width, minWidth: 4 }}
                      title={`${group.label}: ${formatDuration(group.duration_ms)} (${group.status})`}
                    >
                      {width > 50 && group.duration_ms != null && (
                        <span className="text-[8px] text-white font-mono drop-shadow-sm">
                          {formatDuration(group.duration_ms)}
                        </span>
                      )}
                    </div>
                    {/* Dashed extension for pending */}
                    {isPending && (
                      <div
                        className="h-4 border-t-2 border-b-2 border-dashed border-purple-400/40 absolute"
                        style={{
                          left: left + width,
                          width: Math.min(timelineWidth - (left + width), 60),
                        }}
                      />
                    )}
                    {/* Retry markers */}
                    {group.executor_attempts &&
                      group.executor_attempts
                        .filter((a) => !a.success)
                        .map((att, i) => (
                          <div
                            key={i}
                            className="absolute w-2 h-2 rounded-full bg-error/80 border border-error"
                            style={{
                              left:
                                left +
                                (width / (group.executor_attempts!.length + 1)) * (i + 1),
                              top: 1,
                            }}
                            title={`${att.executor}: ${att.error}`}
                          />
                        ))}
                  </div>
                </div>

                {/* Expanded: show sub-events as mini timeline */}
                {isExpanded && (
                  <div className="bg-surface-hover/30 border-b border-surface-border/30">
                    {group.events.map((evt) => {
                      const EvtIcon = EVENT_ICONS[evt.event_type] || Activity;
                      const evtColor =
                        EVENT_COLORS[evt.event_type] ||
                        'text-text-secondary bg-text-secondary/10';
                      const eTxt = evtColor.split(' ')[0];
                      const evtLeft = getPosition(evt.timestamp);
                      return (
                        <div key={evt.event_id} className="flex items-center h-6">
                          <div className="w-40 shrink-0 px-3 pl-8 flex items-center gap-1">
                            <EvtIcon className={cn('w-2.5 h-2.5', eTxt)} />
                            <span className="text-[9px] text-text-secondary truncate">
                              {evt.event_type.replace(/_/g, ' ')}
                            </span>
                          </div>
                          <div className="relative flex-1 h-full flex items-center">
                            <div
                              className={cn(
                                'w-2 h-2 rounded-full absolute',
                                eTxt.replace('text-', 'bg-')
                              )}
                              style={{ left: evtLeft }}
                              title={`${evt.event_type} at ${formatTimestamp(evt.timestamp)}`}
                            />
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

// =============================================================================
// Full History View (git-tree style with all events)
// =============================================================================

function FullHistoryView({
  events,
  eventFilter,
}: {
  events: HistoryEvent[];
  eventFilter: Set<EventGroupCategory>;
}) {
  const [expandedEvents, setExpandedEvents] = useState<Set<number>>(new Set());

  // Map event types to categories for filtering
  const getEventCategory = (type: HistoryEventType): EventGroupCategory => {
    if (type.startsWith('workflow_')) return 'workflow';
    if (type.startsWith('plan_')) return 'plan';
    if (type.startsWith('signal_')) return 'signal';
    if (type.startsWith('timer_')) return 'timer';
    if (type.startsWith('child_')) return 'child_workflow';
    return 'step';
  };

  const filtered = useMemo(
    () =>
      events.filter(
        (evt) => eventFilter.size === 0 || eventFilter.has(getEventCategory(evt.event_type))
      ),
    [events, eventFilter]
  );

  const toggle = (eventId: number) => {
    setExpandedEvents((prev) => {
      const next = new Set(prev);
      if (next.has(eventId)) next.delete(eventId);
      else next.add(eventId);
      return next;
    });
  };

  const getEventLabel = (evt: HistoryEvent): string => {
    const labels: Record<string, string> = {
      workflow_started: 'Workflow Started',
      workflow_completed: 'Workflow Completed',
      workflow_failed: 'Workflow Failed',
      workflow_cancelled: 'Workflow Cancelled',
      workflow_timed_out: 'Workflow Timed Out',
      step_started: `Step Started: ${evt.step_name}`,
      step_completed: `Step Completed: ${evt.step_name}`,
      step_failed: `Step Failed: ${evt.step_name}`,
      plan_started: `Plan Started: ${evt.details?.plan || evt.step_name}`,
      plan_executor_tried: `Executor: ${evt.executor_name}`,
      plan_completed: 'Plan Completed',
      signal_received: `Signal: ${evt.signal_type}`,
      timer_scheduled: `Timer Scheduled: ${evt.timer_type}`,
      timer_fired: `Timer Fired: ${evt.timer_type}`,
      child_workflow_started: `Child Workflow: ${evt.child_workflow_name}`,
      child_workflow_completed: 'Child Workflow Completed',
    };
    return labels[evt.event_type] || evt.event_type;
  };

  return (
    <div className="relative">
      {/* Main vertical timeline line — centered at x=16px to match workflow event icon centers */}
      <div className="absolute left-4 top-0 bottom-0 w-0.5 bg-surface-border/60" />

      <div className="space-y-0">
        {filtered.map((evt) => {
          const Icon = EVENT_ICONS[evt.event_type] || Activity;
          const colorClasses =
            EVENT_COLORS[evt.event_type] || 'text-text-secondary bg-text-secondary/10';
          const colorParts = colorClasses.split(' ');
          const textColor = colorParts[0];
          const bgColor = colorParts[1];
          const isExpanded = expandedEvents.has(evt.event_id);
          const hasDetails =
            Object.keys(evt.details).length > 0 || evt.error || !!evt.executor_name;
          const isWorkflowEvent = evt.event_type.startsWith('workflow_');

          return (
            <div
              key={evt.event_id}
              className={cn(
                'relative flex items-start gap-3 py-1.5 pr-2 rounded-md transition-colors',
                hasDetails ? 'cursor-pointer hover:bg-surface-hover' : '',
                isExpanded && 'bg-surface-hover'
              )}
              style={{ paddingLeft: isWorkflowEvent ? 2 : 32 }}
              onClick={() => hasDetails && toggle(evt.event_id)}
            >
              {/* Horizontal connector for step events: bridges from line (x=16) to icon (x=32) */}
              {!isWorkflowEvent && (
                <div className="absolute left-4 top-5 w-4 h-px bg-surface-border/60" />
              )}

              {/* Icon — z-10 so it sits above the vertical line */}
              <div className="relative z-10 shrink-0 w-7 h-7 flex items-center justify-center">
                <div
                  className={cn(
                    'rounded-full p-1',
                    bgColor
                  )}
                >
                  <Icon className={cn('w-3 h-3', textColor)} />
                </div>
              </div>

              {/* Content */}
              <div className="flex-1 min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-2 min-w-0">
                    <span
                      className="text-sm font-medium text-text-primary truncate"
                    >
                      {getEventLabel(evt)}
                    </span>
                    {evt.duration_ms != null && (
                      <span className="text-[10px] font-mono text-text-secondary px-1.5 py-0.5 bg-surface-hover rounded">
                        {formatDuration(evt.duration_ms)}
                      </span>
                    )}
                    {evt.outcome && (
                      <span
                        className={cn(
                          'text-[10px] font-medium px-1.5 py-0.5 rounded',
                          evt.outcome === 'success' || evt.outcome === 'received'
                            ? 'bg-success/10 text-success'
                            : 'bg-error/10 text-error'
                        )}
                      >
                        {evt.outcome}
                      </span>
                    )}
                    {evt.error && !isExpanded && (
                      <AlertTriangle className="w-3 h-3 text-error shrink-0" />
                    )}
                    {evt.attempt && evt.attempt > 1 && (
                      <span className="text-[10px] text-amber-500 font-medium">
                        ↻ attempt #{evt.attempt}
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    <span className="text-[10px] font-mono text-text-secondary">
                      #{evt.event_id}
                    </span>
                    <span className="text-[10px] text-text-secondary">
                      {formatTimestamp(evt.timestamp)}
                    </span>
                    {hasDetails && (
                      <ChevronDown
                        className={cn(
                          'w-3 h-3 text-text-secondary transition-transform',
                          isExpanded && 'rotate-180'
                        )}
                      />
                    )}
                  </div>
                </div>

                {/* Expanded details */}
                {isExpanded && (
                  <div className="mt-2 space-y-2">
                    {evt.error && (
                      <div className="text-xs bg-error/5 border border-error/20 rounded p-2 text-error font-mono">
                        {evt.error}
                      </div>
                    )}
                    {Object.keys(evt.details).length > 0 && (
                      <pre className="text-xs bg-background rounded p-3 overflow-x-auto text-text-secondary font-mono border border-surface-border">
                        {JSON.stringify(evt.details, null, 2)}
                      </pre>
                    )}
                    {evt.signal_payload != null && (
                      <div>
                        <p className="text-[10px] uppercase tracking-wider text-text-secondary font-medium mb-1">
                          Signal Payload
                        </p>
                        <pre className="text-xs bg-background rounded p-3 overflow-x-auto text-text-secondary font-mono border border-surface-border">
                          {JSON.stringify(evt.signal_payload, null, 2)}
                        </pre>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// =============================================================================
// Event Type Filter Chips
// =============================================================================

const ALL_CATEGORIES: { category: EventGroupCategory; label: string; icon: typeof Play }[] = [
  { category: 'step', label: 'Steps', icon: Zap },
  { category: 'plan', label: 'Plans', icon: Layers },
  { category: 'signal', label: 'Signals', icon: Signal },
  { category: 'timer', label: 'Timers', icon: Clock },
  { category: 'workflow', label: 'Workflow', icon: GitGraph },
  { category: 'child_workflow', label: 'Children', icon: GitBranch },
];

function EventFilterChips({
  activeFilters,
  onToggle,
}: {
  activeFilters: Set<EventGroupCategory>;
  onToggle: (cat: EventGroupCategory) => void;
}) {
  return (
    <div className="flex items-center gap-1.5 flex-wrap">
      <Filter className="w-3.5 h-3.5 text-text-secondary shrink-0" />
      {ALL_CATEGORIES.map(({ category, label, icon: CatIcon }) => {
        const isActive = activeFilters.has(category);
        const catColor = CATEGORY_COLORS[category].split(' ')[0];
        return (
          <button
            key={category}
            onClick={() => onToggle(category)}
            className={cn(
              'inline-flex items-center gap-1 text-[10px] font-medium px-2 py-1 rounded-full border transition-all',
              isActive
                ? cn(catColor, 'border-current bg-current/10')
                : 'text-text-secondary border-surface-border hover:border-text-secondary/50'
            )}
          >
            <CatIcon className="w-2.5 h-2.5" />
            {label}
          </button>
        );
      })}
      {activeFilters.size > 0 && (
        <button
          onClick={() => activeFilters.forEach(onToggle)}
          className="text-[10px] text-text-secondary hover:text-text-primary transition-colors underline"
        >
          Clear
        </button>
      )}
    </div>
  );
}

// =============================================================================
// History Tab (all three views + filter)
// =============================================================================

function HistoryTab({ run }: { run: WorkflowRun }) {
  const [viewMode, setViewMode] = useState<HistoryViewMode>('timeline');
  const [eventFilter, setEventFilter] = useState<Set<EventGroupCategory>>(new Set());

  const groups = useMemo(() => buildEventGroups(run.history || []), [run.history]);

  const toggleFilter = useCallback((cat: EventGroupCategory) => {
    setEventFilter((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  }, []);

  const now = useNow();
  const workflowStartedAt = run.started_at;
  const workflowEndedAt = run.completed_at || now;

  const viewModes: { id: HistoryViewMode; label: string; icon: typeof List }[] = [
    { id: 'timeline', label: 'Timeline', icon: GanttChart },
    { id: 'compact', label: 'Compact', icon: List },
    { id: 'full', label: 'Full History', icon: GitBranch },
  ];

  return (
    <div className="space-y-3">
      {/* View mode toggle + event filter */}
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <EventFilterChips activeFilters={eventFilter} onToggle={toggleFilter} />
        <div className="flex items-center gap-1 bg-surface-hover rounded-md p-0.5">
          {viewModes.map((vm) => (
            <button
              key={vm.id}
              onClick={() => setViewMode(vm.id)}
              className={cn(
                'flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition-all',
                viewMode === vm.id
                  ? 'bg-surface text-primary shadow-sm'
                  : 'text-text-secondary hover:text-text-primary'
              )}
            >
              <vm.icon className="w-3 h-3" />
              {vm.label}
            </button>
          ))}
        </div>
      </div>

      {/* View content */}
      {viewMode === 'compact' && <CompactView groups={groups} eventFilter={eventFilter} />}
      {viewMode === 'timeline' && (
        <TimelineView
          groups={groups}
          eventFilter={eventFilter}
          workflowStartedAt={workflowStartedAt}
          workflowEndedAt={workflowEndedAt}
        />
      )}
      {viewMode === 'full' && <FullHistoryView events={run.history || []} eventFilter={eventFilter} />}
    </div>
  );
}

// =============================================================================
// Relationships Tab (parent/child + inline child workflow)
// =============================================================================

function RelationshipsTab({ run, allRuns }: { run: WorkflowRun; allRuns: WorkflowRun[] }) {
  const now = useNow();
  const navigate = useNavigate();
  const [expandedChild, setExpandedChild] = useState<string | null>(null);

  // Find child workflows
  const childRuns = allRuns.filter((r) => r.parent_run_id === run.run_id);
  const parentRun = run.parent_run_id
    ? allRuns.find((r) => r.run_id === run.parent_run_id)
    : null;

  const hasRelationships = parentRun || childRuns.length > 0;

  if (!hasRelationships) {
    return (
      <div className="text-center py-12 text-text-secondary">
        <Link2 className="w-8 h-8 mx-auto mb-3 opacity-30" />
        <p className="text-sm">No parent or child workflows</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Parent */}
      {parentRun && (
        <div>
          <h4 className="text-xs uppercase tracking-wider text-text-secondary font-medium mb-2 flex items-center gap-1.5">
            <ArrowLeft className="w-3 h-3" /> Parent Workflow
          </h4>
          <div
            className="border border-surface-border rounded-md p-3 hover:border-primary/30 cursor-pointer transition-colors"
            onClick={() => navigate(`/workflows/runs/${parentRun.run_id}`)}
          >
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <GitGraph className="w-4 h-4 text-primary" />
                <span className="font-medium">{parentRun.workflow_name}</span>
                <span className="text-[10px] text-text-secondary">
                  v{parentRun.workflow_version}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <span
                  className={cn(
                    'text-[10px] font-medium px-2 py-0.5 rounded-full',
                    STATUS_CONFIG[parentRun.status].bgColor,
                    STATUS_CONFIG[parentRun.status].color
                  )}
                >
                  {STATUS_CONFIG[parentRun.status].label}
                </span>
                <span className="font-mono text-[10px] text-text-secondary">
                  {parentRun.run_id}
                </span>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Children */}
      {childRuns.length > 0 && (
        <div>
          <h4 className="text-xs uppercase tracking-wider text-text-secondary font-medium mb-2 flex items-center gap-1.5">
            <GitBranch className="w-3 h-3" /> Child Workflows ({childRuns.length})
          </h4>
          <div className="space-y-2">
            {childRuns.map((child) => {
              const isExpanded = expandedChild === child.run_id;
              const childGroups = buildEventGroups(child.history || []);
              const childStart = child.started_at;
              const childEnd = child.completed_at || now;

              return (
                <div
                  key={child.run_id}
                  className="border border-surface-border rounded-md overflow-hidden"
                >
                  <div
                    className="flex items-center justify-between p-3 cursor-pointer hover:bg-surface-hover transition-colors"
                    onClick={() => setExpandedChild(isExpanded ? null : child.run_id)}
                  >
                    <div className="flex items-center gap-2">
                      <GitBranch className="w-3.5 h-3.5 text-indigo-500" />
                      <span className="font-medium text-sm">{child.workflow_name}</span>
                      <span className="text-[10px] text-text-secondary">
                        v{child.workflow_version}
                      </span>
                      {child.current_step && (
                        <span className="text-[10px] font-mono text-text-secondary">
                          @ {child.current_step}
                        </span>
                      )}
                    </div>
                    <div className="flex items-center gap-2">
                      <span
                        className={cn(
                          'text-[10px] font-medium px-2 py-0.5 rounded-full',
                          STATUS_CONFIG[child.status].bgColor,
                          STATUS_CONFIG[child.status].color
                        )}
                      >
                        {STATUS_CONFIG[child.status].label}
                      </span>
                      {child.duration_ms != null && (
                        <span className="text-[10px] font-mono text-text-secondary">
                          {formatDuration(child.duration_ms)}
                        </span>
                      )}
                      <span className="font-mono text-[10px] text-text-secondary">
                        {child.run_id}
                      </span>
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          navigate(`/workflows/runs/${child.run_id}`);
                        }}
                        className="text-[10px] text-primary hover:text-primary-hover transition-colors"
                      >
                        Open
                      </button>
                      <ChevronDown
                        className={cn(
                          'w-3 h-3 text-text-secondary transition-transform',
                          isExpanded && 'rotate-180'
                        )}
                      />
                    </div>
                  </div>

                  {/* Inline child workflow timeline (Temporal feature) */}
                  {isExpanded && (
                    <div className="border-t border-surface-border p-3 bg-surface-hover/30">
                      <TimelineView
                        groups={childGroups}
                        eventFilter={new Set()}
                        workflowStartedAt={childStart}
                        workflowEndedAt={childEnd}
                      />
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Ancestry info */}
      <div className="text-[10px] text-text-secondary flex items-center gap-3 mt-2">
        <span>Ancestry Depth: {run.ancestry_depth}</span>
        {run.idempotency_key && (
          <span>
            Idempotency Key: <span className="font-mono">{run.idempotency_key}</span>
          </span>
        )}
      </div>
    </div>
  );
}

// =============================================================================
// Metadata Tab (search attributes, input/output, step results)
// =============================================================================

function MetaJsonPanel({
  title,
  data,
  emptyMessage,
}: {
  title: string;
  data: unknown;
  emptyMessage?: string;
}) {
  if (!data || (typeof data === 'object' && Object.keys(data as object).length === 0)) {
    return (
      <div className="text-center py-6 text-text-secondary">
        <p className="text-sm">{emptyMessage || 'No data'}</p>
      </div>
    );
  }
  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <h4 className="text-xs uppercase tracking-wider text-text-secondary font-medium">
          {title}
        </h4>
        <button
          onClick={() => copyToClipboard(JSON.stringify(data, null, 2))}
          className="text-text-secondary hover:text-text-primary transition-colors"
          title="Copy to clipboard"
        >
          <Copy className="w-3.5 h-3.5" />
        </button>
      </div>
      <pre className="text-xs bg-background rounded-md p-4 overflow-x-auto text-text-secondary font-mono border border-surface-border leading-relaxed">
        {JSON.stringify(data, null, 2)}
      </pre>
    </div>
  );
}

function MetadataTab({ run }: { run: WorkflowRun }) {
  return (
    <div className="space-y-6">
      {/* Search Attributes */}
      {run.search_attributes && Object.keys(run.search_attributes).length > 0 && (
        <div>
          <h4 className="text-xs uppercase tracking-wider text-text-secondary font-medium mb-2 flex items-center gap-1.5">
            <Tag className="w-3 h-3" /> Search Attributes
          </h4>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
            {Object.entries(run.search_attributes).map(([key, value]) => (
              <div key={key} className="bg-background rounded p-2 border border-surface-border">
                <p className="text-[10px] text-text-secondary font-mono">{key}</p>
                <p className="text-xs font-medium truncate">{String(value)}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Input */}
      <MetaJsonPanel title="Input" data={run.input} emptyMessage="No input data" />

      {/* Output */}
      <MetaJsonPanel
        title="Output"
        data={run.output}
        emptyMessage={
          run.status === 'running' || run.status === 'waiting'
            ? 'Workflow still in progress'
            : 'No output'
        }
      />

      {/* Step Results */}
      {Object.keys(run.step_results || {}).length > 0 && (
        <div>
          <h4 className="text-xs uppercase tracking-wider text-text-secondary font-medium mb-2 flex items-center gap-1.5">
            <Zap className="w-3 h-3" /> Step Results ({Object.keys(run.step_results || {}).length})
          </h4>
          <div className="space-y-2">
            {Object.entries(run.step_results || {}).map(([stepName, result]) => (
              <div key={stepName} className="border border-surface-border rounded-md p-3">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <Zap className="w-3.5 h-3.5 text-primary" />
                    <span className="font-mono text-sm font-medium">{stepName}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span
                      className={cn(
                        'text-[10px] font-medium px-2 py-0.5 rounded-full',
                        result.outcome === 'success' || result.outcome === 'received'
                          ? 'bg-success/10 text-success'
                          : 'bg-error/10 text-error'
                      )}
                    >
                      {result.outcome}
                    </span>
                    <span className="text-[10px] font-mono text-text-secondary">
                      {formatDuration(result.duration_ms)}
                    </span>
                    {result.attempts > 1 && (
                      <span className="text-[10px] text-text-secondary">
                        {result.attempts} attempts
                      </span>
                    )}
                  </div>
                </div>
                {result.output != null && (
                  <pre className="text-xs bg-background rounded p-2 overflow-x-auto text-text-secondary font-mono border border-surface-border">
                    {JSON.stringify(result.output, null, 2)}
                  </pre>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Misc metadata */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div className="bg-background rounded p-2 border border-surface-border">
          <p className="text-[10px] uppercase tracking-wider text-text-secondary">Namespace</p>
          <p className="text-xs font-medium font-mono">{run.namespace}</p>
        </div>
        {run.idempotency_key && (
          <div className="bg-background rounded p-2 border border-surface-border">
            <p className="text-[10px] uppercase tracking-wider text-text-secondary">
              Idempotency Key
            </p>
            <p className="text-xs font-medium font-mono">{run.idempotency_key}</p>
          </div>
        )}
        <div className="bg-background rounded p-2 border border-surface-border">
          <p className="text-[10px] uppercase tracking-wider text-text-secondary">
            Pending Signals
          </p>
          <p className="text-xs font-medium">{run.pending_signals ?? 0}</p>
        </div>
        <div className="bg-background rounded p-2 border border-surface-border">
          <p className="text-[10px] uppercase tracking-wider text-text-secondary">
            Ancestry Depth
          </p>
          <p className="text-xs font-medium">{run.ancestry_depth}</p>
        </div>
      </div>
    </div>
  );
}

// =============================================================================
// Definition Tab (YAML + plans + health)
// =============================================================================

function DefinitionTab({ definition }: { definition?: DefinitionListEntry }) {
  const [yaml, setYaml] = useState<string | null>(null);
  const [yamlLoading, setYamlLoading] = useState(false);
  const [showYaml, setShowYaml] = useState(false);

  const fetchYaml = useCallback(async () => {
    if (!definition || yaml) return;
    setYamlLoading(true);
    try {
      const result = await workflowApi.getDefinition(definition.name, definition.version);
      setYaml(result.definition_yaml);
    } catch {
      setYaml('# Failed to load definition YAML');
    } finally {
      setYamlLoading(false);
    }
  }, [definition, yaml]);

  if (!definition) {
    return (
      <div className="text-center py-8 text-text-secondary text-sm">Definition not found</div>
    );
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <FileCode className="w-4 h-4 text-text-secondary" />
          <span className="text-sm font-medium">{definition.name}</span>
          <span className="text-[10px] text-text-secondary">v{definition.version}</span>
          <span
            className={cn(
              'text-[10px] px-2 py-0.5 rounded-full font-medium',
              definition.enabled ? 'bg-success/10 text-success' : 'bg-error/10 text-error'
            )}
          >
            {definition.enabled ? 'Enabled' : 'Disabled'}
          </span>
        </div>
      </div>

      {/* Definition summary */}
      <div className="space-y-4">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div className="bg-surface border border-surface-border rounded-md p-3">
            <p className="text-[10px] uppercase tracking-wider text-text-secondary font-medium mb-1">Steps</p>
            <p className="text-sm font-medium">{definition.step_count}</p>
          </div>
          <div className="bg-surface border border-surface-border rounded-md p-3">
            <p className="text-[10px] uppercase tracking-wider text-text-secondary font-medium mb-1">Plans</p>
            <p className="text-sm font-medium">{definition.plan_count}</p>
          </div>
          <div className="bg-surface border border-surface-border rounded-md p-3">
            <p className="text-[10px] uppercase tracking-wider text-text-secondary font-medium mb-1">Schedule</p>
            <p className="text-sm font-medium">{definition.has_schedule ? 'Yes' : 'No'}</p>
          </div>
          <div className="bg-surface border border-surface-border rounded-md p-3">
            <p className="text-[10px] uppercase tracking-wider text-text-secondary font-medium mb-1">Trigger</p>
            <p className="text-sm font-medium">{definition.has_trigger ? 'Yes' : 'No'}</p>
          </div>
        </div>

        {/* Step flow */}
        <div>
          <h4 className="text-xs font-medium text-text-secondary uppercase tracking-wider mb-2">Step Flow</h4>
          <div className="flex items-center gap-1 flex-wrap">
            <span className="text-xs font-mono bg-primary/10 text-primary px-2 py-1 rounded">{definition.start_step}</span>
            {definition.steps.map((s, i) => (
              <span key={i} className="flex items-center gap-1">
                <span className="text-text-secondary/40">→</span>
                <span className="text-xs font-mono bg-surface-hover px-2 py-1 rounded">{s.name}</span>
              </span>
            ))}
            {definition.terminals.map((t, i) => (
              <span key={`t-${i}`} className="flex items-center gap-1">
                <span className="text-text-secondary/40">→</span>
                <span className="text-xs font-mono bg-success/10 text-success px-2 py-1 rounded">{t.name}</span>
              </span>
            ))}
          </div>
        </div>

        {/* YAML Source */}
        <div>
          <button
            onClick={() => { setShowYaml(!showYaml); if (!yaml) fetchYaml(); }}
            className="flex items-center gap-1.5 text-xs text-text-secondary hover:text-text-primary transition-colors"
          >
            <FileCode className="w-3.5 h-3.5" />
            {showYaml ? 'Hide' : 'Show'} YAML Source
            <ChevronDown className={cn('w-3 h-3 transition-transform', showYaml && 'rotate-180')} />
          </button>
          {showYaml && (
            <div className="mt-2 relative">
              {yamlLoading ? (
                <div className="text-xs text-text-secondary py-4 text-center">Loading…</div>
              ) : (
                <>
                  <button
                    onClick={() => yaml && copyToClipboard(yaml)}
                    className="absolute top-2 right-2 text-text-secondary hover:text-text-primary transition-colors"
                    title="Copy YAML"
                  >
                    <Copy className="w-3.5 h-3.5" />
                  </button>
                  <pre className="text-xs bg-background rounded-md p-4 overflow-x-auto text-text-secondary font-mono border border-surface-border leading-relaxed whitespace-pre">
                    {yaml}
                  </pre>
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// =============================================================================
// Main Page
// =============================================================================

export function WorkflowDetailPage() {
  const { runId } = useParams<{ runId: string }>();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState<DetailTab>('history');
  const [showSignalModal, setShowSignalModal] = useState(false);
  const [signalType, setSignalType] = useState('');
  const [signalPayload, setSignalPayload] = useState('');

  // Fetch from real API
  const { run: apiRun, loading, error, sseStatus, refetch } = useWorkflowRun(runId);
  const { runs: allRuns } = useWorkflowRuns();
  const { definitions } = useWorkflowDefinitions();

  const handleCancel = useCallback(async () => {
    if (!runId) return;
    try {
      await workflowApi.cancelRun(runId, 'Cancelled by user');
      refetch();
    } catch (e) {
      console.error('Failed to cancel run:', e);
    }
  }, [runId, refetch]);

  const handleSignalSubmit = useCallback(async () => {
    if (!runId || !signalType) return;
    try {
      const payload = signalPayload ? JSON.parse(signalPayload) : undefined;
      await workflowApi.signalRun(runId, signalType, payload);
      setShowSignalModal(false);
      setSignalType('');
      setSignalPayload('');
      refetch();
    } catch (e) {
      console.error('Failed to send signal:', e);
    }
  }, [runId, signalType, signalPayload, refetch]);

  const handleRetry = useCallback(async () => {
    if (!apiRun) return;
    try {
      const result = await workflowApi.startRun({
        workflow: apiRun.workflow_name,
      }, apiRun.namespace);
      navigate(`/workflows/${result.run_id}`);
    } catch (e) {
      console.error('Failed to retry run:', e);
    }
  }, [apiRun, navigate]);

  const run = apiRun ?? null;
  const definition = run ? definitions.find(d => d.name === run.workflow_name) : undefined;

  if (loading) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center gap-4">
        <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
        <p className="text-text-secondary text-sm">Loading workflow run…</p>
      </div>
    );
  }

  if (!run) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center gap-4">
        <GitGraph className="w-12 h-12 text-text-secondary/30" />
        <p className="text-text-secondary">
          {error ? `Error loading run: ${error}` : `Workflow run not found: ${runId}`}
        </p>
        <button
          onClick={() => navigate('/workflows')}
          className="text-primary hover:text-primary-hover text-sm transition-colors"
        >
          Back to Workflows
        </button>
      </div>
    );
  }

  // Count children for badge
  const childCount = run.child_run_ids?.length
    ?? allRuns.filter((r) => r.parent_run_id === run.run_id).length;

  const tabs: { id: DetailTab; label: string; icon: typeof History; badge?: number }[] = [
    { id: 'history', label: 'History', icon: History },
    {
      id: 'relationships',
      label: 'Relationships',
      icon: Link2,
      badge: childCount + (run.parent_run_id ? 1 : 0),
    },
    { id: 'metadata', label: 'Metadata', icon: Tag },
    { id: 'definition', label: 'Definition', icon: FileCode },
  ];

  return (
    <div className="flex-1 min-w-0 overflow-auto">
        <div className="px-8 py-6 space-y-4">
          {/* Back nav */}
          <button
            onClick={() => navigate('/workflows')}
            className="flex items-center gap-1 text-sm text-text-secondary hover:text-text-primary transition-colors"
          >
            <ArrowLeft className="w-4 h-4" />
            Back to Workflows
          </button>

          {/* Status Banner */}
          <div className="relative">
            <StatusBanner
              run={run}
              onCancel={handleCancel}
              onSignal={() => setShowSignalModal(true)}
              onRetry={handleRetry}
            />
            {sseStatus === 'open' && (
              <span className="absolute top-3 right-3 flex items-center gap-1.5 text-xs text-emerald-400" title="Live updates via SSE">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
                </span>
                Live
              </span>
            )}
          </div>

          {/* Signal Modal */}
          {showSignalModal && (
            <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={() => setShowSignalModal(false)}>
              <div className="bg-surface rounded-lg border border-surface-border p-6 w-[400px] space-y-4" onClick={e => e.stopPropagation()}>
                <h3 className="text-lg font-semibold">Send Signal</h3>
                <div>
                  <label className="block text-sm text-text-secondary mb-1">Signal Type</label>
                  <input
                    type="text"
                    value={signalType}
                    onChange={e => setSignalType(e.target.value)}
                    placeholder="e.g. approval, retry, cancel"
                    className="w-full bg-background border border-surface-border rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                  />
                </div>
                <div>
                  <label className="block text-sm text-text-secondary mb-1">Payload (JSON, optional)</label>
                  <textarea
                    value={signalPayload}
                    onChange={e => setSignalPayload(e.target.value)}
                    placeholder='{"approved": true}'
                    rows={4}
                    className="w-full bg-background border border-surface-border rounded-md px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
                  />
                </div>
                <div className="flex justify-end gap-2">
                  <Button variant="ghost" onClick={() => setShowSignalModal(false)}>Cancel</Button>
                  <Button variant="primary" onClick={handleSignalSubmit} disabled={!signalType.trim()}>Send Signal</Button>
                </div>
              </div>
            </div>
          )}

          {/* Summary Cards */}
          <SummaryCards run={run} definition={definition} />

          {/* Step Progress */}
          <Card>
            <CardContent className="p-4">
              <StepProgressBar run={run} definition={definition} />
            </CardContent>
          </Card>

          {/* Tabbed Detail Area */}
          <Card>
            <CardHeader className="pb-0">
              <div className="flex items-center gap-1 border-b border-surface-border -mx-6 px-6">
                {tabs.map((tab) => (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id)}
                    className={cn(
                      'flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px',
                      activeTab === tab.id
                        ? 'text-primary border-primary'
                        : 'text-text-secondary hover:text-text-primary border-transparent'
                    )}
                  >
                    <tab.icon className="w-3.5 h-3.5" />
                    {tab.label}
                    {tab.badge != null && tab.badge > 0 && (
                      <span className="text-[10px] bg-surface-hover text-text-secondary rounded-full px-1.5 py-0.5 font-mono">
                        {tab.badge}
                      </span>
                    )}
                  </button>
                ))}
              </div>
            </CardHeader>
            <CardContent className="pt-4">
              {activeTab === 'history' && <HistoryTab run={run} />}
              {activeTab === 'relationships' && <RelationshipsTab run={run} allRuns={allRuns} />}
              {activeTab === 'metadata' && <MetadataTab run={run} />}
              {activeTab === 'definition' && <DefinitionTab definition={definition} />}
            </CardContent>
          </Card>
        </div>
    </div>
  );
}
