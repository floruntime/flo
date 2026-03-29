// =============================================================================
// Workflow API Client
//
// Typed fetch wrappers for the Workflow REST API.
// Routes: /api/v1/workflow/:namespace/...
// Proxied: Vite dev → port 9000 (main server)
// =============================================================================

import { getAuthToken } from './AuthContext';

const API_BASE = '/api/v1/workflow';
const DEFAULT_NS = 'default';

// ---------------------------------------------------------------------------
// Response types (matching server JSON output)
// ---------------------------------------------------------------------------

/** Minimal run entry returned by list endpoint */
export interface RunListEntry {
  run_id: string;
  workflow: string;
  version: string;
  status: string;
  started_at: number; // epoch ms
  updated_at: number; // epoch ms
  current_step: string | null;
  completed_at: number | null;
  duration_ms: number | null;
  error: string | null;
  wait_type: string | null;
  parent_run_id: string | null;
  terminal_name: string | null;
  triggered_by?: string; // "manual" | "schedule" | "stream"
}

/** Status result from GET /runs/:run_id */
export interface RunStatusResult {
  run_id: string;
  status: string;
  workflow?: string;
  version?: string;
  current_step: string | null;
  output?: Record<string, unknown> | null;
  error_message?: string | null;
  started_at_ms?: number;
  completed_at?: number;
  updated_at_ms?: number;
  duration_ms?: number;
  input?: unknown;
  step_results?: Record<string, { outcome: string; output: unknown }>;
  triggered_by?: string;
  pending_signals?: number;
  history_event_count?: number;
  search_attributes?: Record<string, unknown>;
}

/** History event from GET /runs/:run_id/history */
export interface HistoryEventEntry {
  event_type: string;
  timestamp: number;
  step_name?: string;
  error?: string;
  output?: string;
  [key: string]: unknown; // extensible
}

/** Start run response */
export interface StartRunResult {
  run_id: string;
  already_exists: boolean;
}

/** Create definition response */
export interface CreateDefinitionResult {
  workflow_name: string;
}

/** Definition YAML response */
export interface DefinitionResult {
  definition_yaml: string;
}

/** Definition summary returned by list endpoint */
export interface DefinitionListEntry {
  name: string;
  version: string;
  enabled: boolean;
  created_at: number;
  step_count: number;
  plan_count: number;
  has_schedule: boolean;
  has_trigger: boolean;
  start_step: string;
  terminals: { name: string }[];
  steps: { name: string }[];
}

// ---------------------------------------------------------------------------
// API error
// ---------------------------------------------------------------------------

export class WorkflowApiError extends Error {
  status: number;
  body: string;
  constructor(status: number, body: string) {
    super(`Workflow API error ${status}: ${body}`);
    this.name = 'WorkflowApiError';
    this.status = status;
    this.body = body;
  }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  if (!headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }
  const token = getAuthToken();
  if (token && !headers.has('Authorization')) {
    headers.set('Authorization', `Bearer ${token}`);
  }

  const res = await fetch(path, { ...init, headers });
  const text = await res.text();

  if (res.status === 401) {
    localStorage.removeItem('flo:auth');
    if (window.location.pathname !== '/login') {
      window.location.href = '/login';
    }
    throw new WorkflowApiError(401, text);
  }

  if (!res.ok) {
    throw new WorkflowApiError(res.status, text);
  }
  return text ? JSON.parse(text) : ({} as T);
}

function qs(params: Record<string, string | number | boolean | null | undefined>): string {
  const parts: string[] = [];
  for (const [k, v] of Object.entries(params)) {
    if (v != null && v !== '' && v !== false) parts.push(`${k}=${encodeURIComponent(String(v))}`);
  }
  return parts.length ? `?${parts.join('&')}` : '';
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

export async function listRuns(
  params?: {
    status?: string;
    workflow?: string;
    search?: string;
    limit?: number;
    cursor?: string;
  },
  namespace = DEFAULT_NS,
): Promise<RunListEntry[]> {
  const q = qs({
    namespace,
    status: params?.status,
    workflow: params?.workflow,
    search: params?.search,
    limit: params?.limit,
    cursor: params?.cursor,
  });
  return request<RunListEntry[]>(`${API_BASE}/runs${q}`);
}

export async function getRunStatus(
  runId: string,
  namespace = DEFAULT_NS,
): Promise<RunStatusResult> {
  return request<RunStatusResult>(`${API_BASE}/runs/${runId}${qs({ namespace })}`);
}

export async function getRunHistory(
  runId: string,
  limit = 100,
  namespace = DEFAULT_NS,
): Promise<HistoryEventEntry[]> {
  return request<HistoryEventEntry[]>(`${API_BASE}/runs/${runId}/history${qs({ namespace, limit })}`);
}

export async function startRun(
  params: {
    workflow: string;
    version?: string;
    input?: string;
    run_id?: string;
    idempotency_key?: string;
  },
  namespace = DEFAULT_NS,
): Promise<StartRunResult> {
  return request<StartRunResult>(`${API_BASE}/runs${qs({ namespace })}`, {
    method: 'POST',
    body: JSON.stringify(params),
  });
}

export async function cancelRun(
  runId: string,
  reason?: string,
  namespace = DEFAULT_NS,
): Promise<void> {
  await request<unknown>(`${API_BASE}/runs/${runId}/cancel${qs({ namespace })}`, {
    method: 'POST',
    body: reason ? JSON.stringify({ reason }) : undefined,
  });
}

export async function signalRun(
  runId: string,
  signalType: string,
  payload?: string,
  namespace = DEFAULT_NS,
): Promise<void> {
  await request<unknown>(`${API_BASE}/runs/${runId}/signal${qs({ namespace })}`, {
    method: 'POST',
    body: JSON.stringify({ signal_type: signalType, payload }),
  });
}

// ---------------------------------------------------------------------------
// Definitions
// ---------------------------------------------------------------------------

export async function listDefinitions(
  namespace = DEFAULT_NS,
): Promise<DefinitionListEntry[]> {
  return request<DefinitionListEntry[]>(`${API_BASE}/definitions${qs({ namespace })}`);
}

export async function createDefinition(
  yaml: string,
  namespace = DEFAULT_NS,
): Promise<CreateDefinitionResult> {
  return request<CreateDefinitionResult>(`${API_BASE}/definitions${qs({ namespace })}`, {
    method: 'POST',
    headers: { 'Content-Type': 'text/yaml' },
    body: yaml,
  });
}

export async function getDefinition(
  name: string,
  version?: string,
  namespace = DEFAULT_NS,
): Promise<DefinitionResult> {
  return request<DefinitionResult>(`${API_BASE}/definitions/${name}${qs({ namespace, version })}`);
}

export async function enableDefinition(
  name: string,
  namespace = DEFAULT_NS,
): Promise<void> {
  await request<unknown>(`${API_BASE}/definitions/${name}/enable${qs({ namespace })}`, {
    method: 'POST',
  });
}

export async function disableDefinition(
  name: string,
  namespace = DEFAULT_NS,
): Promise<void> {
  await request<unknown>(`${API_BASE}/definitions/${name}/disable${qs({ namespace })}`, {
    method: 'POST',
  });
}
