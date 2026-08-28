/* Response types for the Flo dashboard API (`/api/v1`).
   Grown per primitive as each screen is wired. Mirrors the Zig handlers in
   src/node/dashboard/api/*.zig — re-verify shapes there when adding a primitive. */

// ── namespaces ──────────────────────────────────────────────
export interface NamespaceInfo {
  name: string
  stream_count: number
  queue_count: number
  kv_count: number
  workflow_count: number
  processing_count: number
  action_count: number
  created_at?: number
  is_system?: boolean
}

// ── cluster / system ────────────────────────────────────────
export interface NodeStatus {
  id: string
  status: 'healthy' | 'unhealthy' | 'unknown'
  role: string
  region?: string
  cpu?: number
  mem?: number
  io?: number
}

export interface ClusterStats {
  rps: number
  active_connections: number
  uptime: string
  version: string
  num_shards: number
  commands_total: number
  bytes_received: number
  bytes_sent: number
  subscriptions: number
  nodes: NodeStatus[]
}

// ── metrics (GET /metrics) ──────────────────────────────────
export interface MetricsInfo {
  server: {
    connections: number
    subscriptions: number
    commands_total: number
    bytes_received: number
    bytes_sent: number
    uptime_seconds: number
  }
  streams: number
  queues: number
  kv_namespaces: number
  workflows: {
    active_runs: number
    started_total: number
    completed_total: number
    failed_total: number
    cancelled_total: number
    timed_out_total: number
    signals_delivered_total: number
    timers_fired_total: number
    steps_executed_total: number
    active_schedules: number
  }
}

// ── kv ──────────────────────────────────────────────────────
export interface KVKeyListItem {
  key: string
  size: number
  /** Entry LSN (not the MVCC version count). */
  version: number
}
export interface KVKeysResponse {
  keys: KVKeyListItem[]
  has_more: boolean
  cursor: string | null
  count: number
  namespace: string
}
export interface KVKeyValue {
  key: string
  namespace: string
  found: boolean
  value?: string
  /** MVCC version. */
  version?: number
  size?: number
  updated_at?: number
  /** Expiry timestamp in ms, or null for no expiry. */
  ttl_ms?: number | null
}
export interface KVHistoryEntry {
  version: number
  term: number
  timestamp_ms: number
  size: number
  tombstone: boolean
}
export interface KVHistoryResponse {
  key: string
  namespace: string
  versions: KVHistoryEntry[]
  version_count: number
}
export interface KVPutBody {
  value: string
  ttl_seconds?: number | null
  nx?: boolean
}

// ── streams ─────────────────────────────────────────────────
export interface StreamInfo {
  name: string
  namespace: string
  partitions: number
  ingest_rate: number
  reads: number
  retention: string
}
export interface StreamPartition {
  id: number
  record_count: number
  stored_bytes: number
}
export interface StreamConsumerGroup {
  name: string
  members: number
  pending_count: number
  /** Timestamp (ms) of the group's last-delivered record — its position on the log. */
  last_delivered_ms: number
}
export interface StreamDetailInfo {
  name: string
  namespace: string
  total_count: number
  total_bytes: number
  partitions: StreamPartition[]
  consumer_groups: StreamConsumerGroup[]
}
export interface StreamMessage {
  id_ms: number
  id_seq: number
  ual_index: number
  size: number
  payload: string
}
export interface StreamMessagesResponse {
  messages: StreamMessage[]
  next_cursor?: string
  limit: number
  count: number
  total_count: number
  total_bytes: number
}

// ── queues ──────────────────────────────────────────────────
export interface QueueInfo {
  name: string
  namespace: string
  ready: number
  inflight: number
  pending: number
  available: number
  enqueued: number
  dequeued: number
  dlq_count: number
}
export type QueueDetailInfo = QueueInfo
export interface QueueMessage {
  seq: number
  priority: number
  state: 'ready' | 'leased' | 'dlq'
  attempts: number
  enqueued_at: number
  lease_remaining_ms: number
  size: number
  payload: string
}
export interface QueueMessagesResponse {
  messages: QueueMessage[]
  count: number
  total: number
  queue: string
}

// ── timeseries ──────────────────────────────────────────────
// NB: the TS projection keys write buffers by `measurement\0field` only — no
// namespace and no tag dimension. So `series_count` == field count, `points`
// is the buffered point total (tags collapse into one series per field), and
// the `?namespace=` filter is currently ignored server-side (see gap log).
export interface TsMeasurement {
  name: string
  series_count: number
  field_count: number
  points: number
}
export interface TsField {
  name: string
  type: string
}
export interface TsMeasurementDetail {
  name: string
  namespace: string
  field_count: number
  fields: TsField[]
  series_count: number
  /** Per-series rollup — empty until the projection tracks tag series. */
  series: unknown[]
  retention: string | null
}
export interface TsPoint {
  timestamp: number
  value: number
}
export interface TsDataResponse {
  measurement: string
  field: string
  aggregation: string
  window_ms: number
  from_ms: number
  to_ms: number
  series: TsPoint[]
}
export interface TsFloqlSeries {
  key: string
  field: string
  point_count: number
  tags: { key: string; value: string }[]
  points: TsPoint[]
}
export interface TsFloqlResponse {
  query: string
  /** Computed by the FloQL engine — one entry per resulting series. */
  series: TsFloqlSeries[]
  error?: string
}

// ── actions ─────────────────────────────────────────────────
export type ActionRunStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled' | 'timed_out'
export interface ActionRunCounts {
  total: number
  pending: number
  running: number
  completed: number
  failed: number
  cancelled: number
  timed_out: number
}
export interface ActionInfo {
  name: string
  namespace: string
  type: string
  owner: string
  description: string
  version: number
  enabled: boolean
  /** Hardcoded default — not persisted on the action record (see gap log). */
  timeout_ms: number
  max_retries: number
  created_at: number
  updated_at: number
  worker_count: number
  runs: ActionRunCounts
}
export interface ActionRun {
  run_id: string
  /** Present on the `/runs` list endpoint, absent in `detail.recent_runs`. */
  action?: string
  status: ActionRunStatus
  attempt?: number
  created_at: number
  started_at: number | null
  completed_at: number | null
  worker_id: string | null
  error: string | null
  outcome: string | null
  input: string | null
  output: string | null
  source: 'direct' | 'workflow' | 'trigger'
  caller_run_id: string | null
  caller_workflow: string | null
}
export interface ActionDetailInfo {
  name: string
  namespace: string
  type: string
  owner: string
  description: string
  version: number
  enabled: boolean
  timeout_ms: number
  max_retries: number
  retry_delay_ms: number
  created_at: number
  updated_at: number
  runs: ActionRunCounts
  recent_runs: ActionRun[]
  /** Workers handling this action. */
  workers: WorkerInfo[]
}
export interface ActionInvokeResult {
  action: string
  namespace: string
  status: string
  run_id: string
}

// ── processing ──────────────────────────────────────────────
export type JobStatus = 'RUNNING' | 'STOPPED' | 'CANCELLED' | 'FAILED' | 'COMPLETED' | 'unknown'
export interface ProcessingJobInfo {
  job_id: string
  name: string
  namespace: string
  status: JobStatus
  parallelism: number
  batch_size: number
  created_at: number
  records_processed: number
}
export interface ProcessingSavepoint {
  savepoint_id: string
  created_at: number
  records_at_savepoint: number
}
export interface ProcessingJobDetail extends ProcessingJobInfo {
  /** Full submitted pipeline definition (parsed client-side into the DAG). */
  yaml: string
  savepoints: ProcessingSavepoint[]
}
export interface ProcessingMutationResult {
  ok: boolean
  job_id: string
  status?: string
  state?: string
}

// ── workflows ───────────────────────────────────────────────
export type WorkflowRunStatus =
  | 'pending'
  | 'running'
  | 'waiting'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'timed_out'
export interface WorkflowDefInfo {
  name: string
  version: string
  created_at: number
  enabled: boolean
  step_count: number
  plan_count: number
  has_schedule: boolean
  has_trigger: boolean
  start_step: string
  terminals: string[]
  /** Often empty in the list — the start step isn't a named step. */
  steps: unknown[]
}
export interface WorkflowDefDetail {
  name: string
  version: string
  created_at: number
  enabled: boolean
  status: string
  definition_yaml: string
  run_count: number
}
export interface WorkflowRunInfo {
  run_id: string
  workflow: string
  version: string
  status: WorkflowRunStatus
  triggered_by: string
  current_step: string | null
  started_at: number
  completed_at: number | null
  duration_ms: number | null
  wait_type: string | null
  parent_run_id: string | null
  terminal_name: string | null
  error: string | null
  history_event_count: number
}
export interface WorkflowStepResult {
  outcome: string
  output: unknown
  started_at?: number
  completed_at?: number
  duration_ms?: number
  attempts?: number
}
export interface WorkflowRunDetail {
  run_id: string
  workflow: string
  version: string
  status: WorkflowRunStatus
  current_step: string | null
  started_at_ms: number
  completed_at: number | null
  updated_at_ms: number
  duration_ms: number | null
  error_message: string | null
  input?: Record<string, unknown>
  output: unknown
  step_results?: Record<string, WorkflowStepResult>
  triggered_by: string
  pending_signals: number
  history_event_count: number
}
export interface WorkflowHistoryEvent {
  event_type: string
  /** Reused field — carries the step name, or the input/terminal for lifecycle events. */
  step_name: string
  timestamp: number
}
export interface WorkflowMutationResult {
  ok: boolean
  run_id?: string
  workflow?: string
  name?: string
  status?: string
  enabled?: boolean
}

// ── workers ─────────────────────────────────────────────────
export type WorkerStatus = 'active' | 'idle' | 'draining' | 'unhealthy'
export interface WorkerProcess {
  name: string
  kind: 'action' | 'stream_consumer'
  run_count: number
  fail_count: number
  /** ms timestamp of the last run, or 0 if never. */
  last_run_at: number
}
export interface WorkerInfo {
  worker_id: string
  status: WorkerStatus
  worker_type: 'action' | 'stream'
  namespace: string
  machine_id: string | null
  current_load: number
  max_concurrent: number
  tasks_completed: number
  tasks_failed: number
  /** ms timestamp of the last heartbeat. */
  last_seen: number
  /** ms timestamp of registration. */
  registered_at: number
  /** Raw metadata string (often JSON), or null. */
  metadata: string | null
  processes: WorkerProcess[]
}
