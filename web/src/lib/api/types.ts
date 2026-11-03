/* Response types for the Flo dashboard API (`/api/v1`).
   Grown per primitive as each screen is wired. Mirrors the Zig handlers in
   src/node/dashboard/api/*.zig — re-verify shapes there when adding a primitive. */

// ── namespaces ──────────────────────────────────────────────
export interface NamespaceInfo {
  name: string
  stream_count: number | null
  queue_count: number | null
  kv_count: number | null
  workflow_count: number | null
  processing_count: number | null
  action_count: number | null
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
  /** The key's MVCC version. */
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
  /** Absolute expiry, epoch ms, or null for no expiry. */
  expires_at_ms?: number | null
}
export interface KVHistoryEntry {
  version: number
  timestamp_ms: number
  size: number
  tombstone: boolean
}
export interface KVHistoryResponse {
  key: string
  namespace: string
  versions: KVHistoryEntry[]
  version_count: number
  /** Older kept versions follow: ask again with before_version. */
  more: boolean
}
export interface KVPutBody {
  value: string
  /** Milliseconds from now; null or 0 for no expiry. */
  ttl_ms?: number | null
  nx?: boolean
}

// ── streams ─────────────────────────────────────────────────
export interface StreamInfo {
  name: string
  namespace: string
  partitions: number
  ingest_rate: number
  reads: number
  /** Human form, e.g. "1d", "1000 records", "1d, 1000 records" or "forever". */
  retention: string
  /** 0 when unset. */
  retention_age_s: number
  retention_count: number
}
export interface StreamPartition {
  id: number
  /** Null when the stream has more than one partition: no op answers per-partition counts yet. */
  record_count: number | null
  stored_bytes: number | null
}
/* A group's members, pending entries and last-delivered times live only on
   the stream's leader and aren't replicated: a leader change resets them. */
export interface StreamConsumerGroup {
  name: string
  members: number
  pending: number
  created_at: number
  ack_timeout_ms: number
  max_deliver: number
  /** "ms-seq" id of the last record delivered to the group — its position on the log. */
  last_delivered_id: string
}
export interface StreamDetailInfo {
  name: string
  namespace: string
  total_count: number
  total_bytes: number
  partitions: StreamPartition[]
  retention: string
  retention_age_s: number
  retention_count: number
  /** Null only when the read was refused. At most 256; `consumer_groups_more` says there are more. */
  consumer_groups: StreamConsumerGroup[] | null
  consumer_groups_more: boolean
}
export interface StreamGroupMember {
  id: string
  state: 'active' | 'leaving'
  pending: number
  joined_at: number
  /** When the member was last delivered a record (there is no heartbeat). */
  last_delivered_at: number | null
}
/** GET /streams/:name/groups/:group/members. */
export interface StreamGroupMembersResponse {
  members: StreamGroupMember[] | null
  members_more: boolean
}
export interface StreamMessage {
  id_ms: number
  id_seq: number
  ual_index: number | null
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
  /** ready + inflight. */
  pending: number
  /** Same as ready. */
  available: number
  enqueued: number
  dequeued: number
  dlq_count: number
}
export type QueueDetailInfo = QueueInfo
/** A ready message as peeked: not taken. `payload` is cut at 4 KiB; `size` is the full length. */
export interface QueueMessage {
  seq: number
  priority: number
  attempts: number
  enqueued_at: number
  size: number
  payload: string
  truncated: boolean
}
/** GET /queues/:name/messages: the next ready messages in dequeue order. */
export interface QueueMessagesResponse {
  queue: string
  messages: QueueMessage[]
  more: boolean
}
export interface QueueDLQEntry extends QueueMessage {
  moved_at: number
}
/** GET /queues/:name/dlq: oldest first, a page at a time; `count` is every dead letter. */
export interface QueueDLQResponse {
  entries: QueueDLQEntry[]
  count: number
  has_more: boolean
  next_after: number | null
  queue: string
}

// ── timeseries ──────────────────────────────────────────────
// NB: the TS projection keys write buffers by `measurement\0field\0tag_hash`,
// so tags ARE a series dimension — `series_count` counts distinct
// measurement+field+tag-set series (no longer just fields), and `points` counts
// every point across them, flushed and buffered.
export interface TsMeasurement {
  name: string
  series_count: number
  field_count: number
  /** The server stops counting fields at a cap: field_count is then a lower bound. */
  field_count_capped: boolean
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
  field_count_capped: boolean
  fields: TsField[]
  series_count: number
  series: unknown[] | null
  points: number
  oldest_ms: number | null
  newest_ms: number | null
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
/** `pending` is counted up to 100,000 (`pending_capped`: at least that).
 *  The others read every run in the namespace, so they are null when it
 *  holds too many runs to count (`too_many_runs`: the action's shard holds
 *  more than 10,000 of the namespace's runs). */
export interface ActionRunCounts {
  pending: number
  pending_capped: boolean
  running: number | null
  completed: number | null
  failed: number | null
  timed_out: number | null
  too_many_runs: boolean
}
/** Over the same runs as ActionRunCounts; null when they aren't counted. */
export interface ActionLatency {
  count: number
  avg_ms: number
  p50_ms: number
  p95_ms: number
  max_ms: number
}
/** `runs` and `latency` are null in the list; the detail answers them. */
export interface ActionInfo {
  name: string
  namespace: string
  owner: string
  description: string
  version: number
  timeout_ms: number
  max_attempts: number
  created_at: number
  latency: ActionLatency | null
  runs: ActionRunCounts | null
}
export interface ActionRun {
  run_id: string
  /** Present on the `/runs` list endpoint, absent in `detail.recent_runs`. */
  action?: string
  status: ActionRunStatus
  attempt?: number | null
  created_at: number
  started_at: number | null
  completed_at: number | null
  worker_id: string | null
  error: string | null
  /** The worker's named outcome, "success" for a completed run without one, else null. */
  outcome: string | null
  input: string | null
  output: string | null
  source: 'direct' | 'workflow' | 'trigger' | null
  caller_run_id: string | null
  caller_workflow: string | null
}
export interface ActionDetailInfo extends Omit<ActionInfo, 'runs'> {
  runs: ActionRunCounts
  recent_runs: ActionRun[]
  /** Workers running this action, as the worker list gives them. */
  workers: WorkerListItem[]
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
  batch_size: number
  created_at: number
  records_processed: number
  /** Null in the list; the detail answers it. */
  metrics: ProcessingJobMetrics | null
}
/** Since-start figures on the answering node. Not replicated: a restart or
 *  leader change resets them. */
export interface ProcessingJobMetrics {
  running: boolean
  records_in: number
  records_out: number
  throughput_per_sec: number
  output_per_sec: number
  latency_avg_ms: number
  latency_max_ms: number
  tick_avg_us: number
  tick_max_us: number
  watermark_ms: number | null
  uptime_ms: number
}
export interface ProcessingJobDetail extends ProcessingJobInfo {
  /** Full submitted pipeline definition (parsed client-side into the DAG). */
  yaml: string
  reason?: string | null
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
  version: string | null
  created_at: number | null
  enabled: boolean | null
  /** 'unknown' when the definition isn't there. */
  status: 'active' | 'disabled' | 'unknown' | null
  definition_yaml: string
  /** Planned; always null for now. */
  run_count: null
}
/** One page of the run list, newest first. A page can end short of its
 *  limit with more after it: only `has_more` says the list is whole. */
export interface WorkflowRunsPage {
  runs: WorkflowRunInfo[]
  has_more: boolean
  /** Pass back as `?cursor=` for the next page. */
  next_cursor: string | null
}
/** One row of the run list. Fields read per run (version, current_step,
 *  completed_at, duration_ms, wait_type) are null past the first 100 rows. */
export interface WorkflowRunInfo {
  run_id: string
  workflow: string
  version: string | null
  status: WorkflowRunStatus
  triggered_by: string | null
  current_step: string | null
  started_at: number
  completed_at: number | null
  duration_ms: number | null
  wait_type: string | null
  parent_run_id: string | null
  terminal_name: string | null
  error: string | null
  history_event_count: number | null
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
  started_at_ms: number | null
  completed_at: number | null
  updated_at_ms: number | null
  duration_ms: number | null
  error_message: string | null
  input: Record<string, unknown> | null
  output: unknown
  step_results: Record<string, WorkflowStepResult> | null
  triggered_by: string | null
  search_attributes: Record<string, unknown> | null
  pending_signals: number | null
  history_event_count: number | null
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
/** One row of the worker list: what worker.list gives, without processes or metadata. */
export type WorkerListItem = Omit<WorkerInfo, 'metadata' | 'processes'>
export interface WorkersPage {
  workers: WorkerListItem[]
  has_more: boolean
  /** The last id on the page; pass it back for the next. */
  cursor: string | null
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
