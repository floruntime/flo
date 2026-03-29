// =============================================================================
// Workflow data-fetching hooks
//
// Wraps workflow-api.ts calls in React hooks with loading/error states.
// Maps server response shapes to the UI WorkflowRun type.
//
// useWorkflowRun: SSE for active runs (instant updates), falls back to REST
//                 polling when SSE is unavailable or the run is terminal.
// =============================================================================

import { useState, useEffect, useCallback, useRef } from 'react';
import type { WorkflowRun, WorkflowRunStatus, WaitType } from './workflow-types';
import * as api from './workflow-api';
import type { RunListEntry, RunStatusResult, DefinitionListEntry } from './workflow-api';

// ---------------------------------------------------------------------------
// SSE base URL — dashboard server (port 9002 in dev, proxied via Vite)
// ---------------------------------------------------------------------------
const SSE_BASE = '/api/v1/workflow';

// ---------------------------------------------------------------------------
// Polling helper
// ---------------------------------------------------------------------------

function usePoll(callback: () => void, intervalMs: number | null) {
  const savedCb = useRef(callback);

  useEffect(() => {
    savedCb.current = callback;
  });

  useEffect(() => {
    if (!intervalMs || intervalMs <= 0) return;
    const id = setInterval(() => savedCb.current(), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
}

// ---------------------------------------------------------------------------
// Mapping helpers
// ---------------------------------------------------------------------------

function mapRunEntry(e: RunListEntry): WorkflowRun {
  return {
    run_id: e.run_id,
    workflow_name: e.workflow,
    workflow_version: e.version,
    namespace: 'default',
    status: e.status as WorkflowRunStatus,
    started_at: e.started_at,
    current_step: e.current_step,
    completed_at: e.completed_at ?? undefined,
    duration_ms: e.duration_ms ?? undefined,
    error: e.error ?? undefined,
    wait_type: (e.wait_type as WaitType) ?? undefined,
    parent_run_id: e.parent_run_id ?? undefined,
  };
}

function mapRunStatus(s: RunStatusResult): Partial<WorkflowRun> {
  return {
    run_id: s.run_id,
    workflow_name: s.workflow,
    workflow_version: s.version,
    status: s.status as WorkflowRunStatus,
    current_step: s.current_step,
    error: s.error_message ?? undefined,
    started_at: s.started_at_ms,
    completed_at: s.completed_at ?? undefined,
    duration_ms: s.duration_ms ?? undefined,
    input: s.input as Record<string, unknown> | undefined,
    output: s.output as Record<string, unknown> | undefined,
    step_results: s.step_results as WorkflowRun['step_results'],
    search_attributes: s.search_attributes,
  };
}

// ---------------------------------------------------------------------------
// useWorkflowRuns — powers WorkflowsListPage Runs tab
// ---------------------------------------------------------------------------

export interface UseWorkflowRunsResult {
  runs: WorkflowRun[];
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useWorkflowRuns(params?: {
  status?: string;
  workflow?: string;
  search?: string;
  limit?: number;
  pollInterval?: number;
}, namespace?: string): UseWorkflowRunsResult {
  const [runs, setRuns] = useState<WorkflowRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const paramsRef = useRef(params);
  paramsRef.current = params;
  const nsRef = useRef(namespace);
  nsRef.current = namespace;

  const fetch_ = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const entries = await api.listRuns(paramsRef.current, nsRef.current);
      setRuns(Array.isArray(entries) ? entries.map(mapRunEntry) : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  // Debounced search: refetch when search term stabilises (300ms)
  const searchTerm = params?.search;
  useEffect(() => {
    const id = setTimeout(() => fetch_(), 300);
    return () => clearTimeout(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchTerm]);

  useEffect(() => { fetch_(); }, [fetch_]);
  usePoll(fetch_, params?.pollInterval ?? null);

  return { runs, loading, error, refetch: fetch_ };
}

// ---------------------------------------------------------------------------
// useWorkflowRun — SSE for active runs, REST fetch for initial + fallback
// ---------------------------------------------------------------------------

export interface UseWorkflowRunResult {
  run: WorkflowRun | null;
  loading: boolean;
  error: string | null;
  /** SSE connection status */
  sseStatus: 'idle' | 'connecting' | 'open' | 'closed';
  refetch: () => void;
}

const TERMINAL_STATUSES = new Set([
  'completed', 'failed', 'cancelled', 'timed_out',
]);

export function useWorkflowRun(runId: string | undefined, namespace?: string): UseWorkflowRunResult {
  const [run, setRun] = useState<WorkflowRun | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sseStatus, setSseStatus] = useState<'idle' | 'connecting' | 'open' | 'closed'>('idle');
  // Tracks whether the initial REST fetch for the current runId has completed.
  // Unlike `loading`, this never goes back to false on subsequent poll fetches,
  // so the SSE effect doesn't tear down and re-create the connection on every poll.
  const [initialLoaded, setInitialLoaded] = useState(false);
  const esRef = useRef<EventSource | null>(null);
  const nsRef = useRef(namespace);
  nsRef.current = namespace;

  // Reset initialLoaded whenever the run we're watching changes.
  useEffect(() => { setInitialLoaded(false); }, [runId]);

  // REST fetch — used for initial load and as fallback
  const fetch_ = useCallback(async () => {
    if (!runId) return;
    setLoading(true);
    setError(null);
    try {
      const [status, history] = await Promise.all([
        api.getRunStatus(runId, nsRef.current),
        api.getRunHistory(runId, 100, nsRef.current).catch(() => []),
      ]);

      const mapped = mapRunStatus(status);
      setRun({
        run_id: status.run_id,
        workflow_name: mapped.workflow_name ?? runId,
        workflow_version: mapped.workflow_version ?? '',
        namespace: nsRef.current ?? 'default',
        status: mapped.status ?? 'pending',
        started_at: mapped.started_at ?? Date.now(),
        completed_at: mapped.completed_at,
        duration_ms: mapped.duration_ms,
        current_step: mapped.current_step,
        error: mapped.error,
        input: mapped.input,
        output: mapped.output,
        step_results: mapped.step_results,
        search_attributes: mapped.search_attributes,
        history: history.map((h, i) => ({
          event_id: i,
          event_type: h.event_type,
          timestamp: h.timestamp,
          step_name: h.step_name,
          error: h.error,
          output: h.output,
          details: {},
        })) as WorkflowRun['history'],
        history_event_count: history.length,
      } as WorkflowRun);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
      setInitialLoaded(true);
    }
  }, [runId]);

  // Initial REST fetch
  useEffect(() => { fetch_(); }, [fetch_]);

  // SSE connection for active (non-terminal) runs
  const runStatus = run?.status;
  useEffect(() => {
    if (!runId) return;
    // Don't connect if run is already terminal
    if (runStatus && TERMINAL_STATUSES.has(runStatus)) {
      setSseStatus('idle');
      return;
    }
    // Don't connect until initial load completes (avoid racing)
    if (!initialLoaded) return;

    const ns = nsRef.current ?? 'default';
    const url = `${SSE_BASE}/runs/${encodeURIComponent(runId)}/watch?namespace=${encodeURIComponent(ns)}`;
    const es = new EventSource(url);
    esRef.current = es;
    setSseStatus('connecting');

    es.addEventListener('connected', () => {
      setSseStatus('open');
    });

    es.addEventListener('status', (e: MessageEvent) => {
      try {
        const data = JSON.parse(e.data) as RunStatusResult;
        setRun((prev) => {
          if (!prev) return prev;
          const mapped = mapRunStatus(data);
          return {
            ...prev,
            ...mapped,
          };
        });
      } catch { /* ignore malformed */ }
    });

    es.addEventListener('history', (e: MessageEvent) => {
      try {
        const events = JSON.parse(e.data) as api.HistoryEventEntry[];
        setRun((prev) => {
          if (!prev) return prev;
          return {
            ...prev,
            history: events.map((h, i) => ({
              event_id: i,
              event_type: h.event_type,
              timestamp: h.timestamp,
              step_name: h.step_name,
              error: h.error,
              output: h.output,
              details: {},
            })) as WorkflowRun['history'],
            history_event_count: events.length,
          };
        });
      } catch { /* ignore malformed */ }
    });

    es.addEventListener('terminal', () => {
      // Run reached a terminal state — do a final REST fetch for completeness
      setSseStatus('closed');
      es.close();
      fetch_();
    });

    es.onerror = () => {
      // SSE connection error — close immediately to suppress browser auto-reconnect,
      // then fall back to the REST polling path.
      setSseStatus('closed');
      es.close();
    };

    return () => {
      es.close();
      esRef.current = null;
      setSseStatus('idle');
    };
  }, [runId, runStatus, initialLoaded, fetch_]);

  // Fallback: poll via REST every 3s if SSE is not connected and run is active
  const isActive = run && !TERMINAL_STATUSES.has(run.status);
  const needsPoll = isActive && sseStatus !== 'open' && sseStatus !== 'connecting';
  usePoll(fetch_, needsPoll ? 3000 : null);

  return { run, loading, error, sseStatus, refetch: fetch_ };
}

// ---------------------------------------------------------------------------
// useWorkflowDefinitions — powers Definitions tabs + dropdowns
// ---------------------------------------------------------------------------

export interface UseWorkflowDefinitionsResult {
  definitions: DefinitionListEntry[];
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useWorkflowDefinitions(namespace?: string): UseWorkflowDefinitionsResult {
  const [definitions, setDefinitions] = useState<DefinitionListEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetch_ = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const entries = await api.listDefinitions(namespace);
      setDefinitions(Array.isArray(entries) ? entries : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetch_(); }, [fetch_]);

  return { definitions, loading, error, refetch: fetch_ };
}
