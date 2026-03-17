/**
 * Flo Dashboard API Client
 *
 * Provides typed API calls to the Flo server dashboard endpoints.
 * In development, requests are proxied through Vite to the Flo server.
 * In production, the API is served from the same origin.
 *
 * API v1 - Unified Namespaces Model
 * Namespaces are first-class resources that can contain streams, queues, and KV keys.
 */

import { getAuthToken } from './AuthContext';

// API base URL - configurable via environment variable for production deployments
const API_BASE = import.meta.env.VITE_API_URL || '/api/v1';

// =============================================================================
// Types - Namespaces
// =============================================================================

/**
 * Namespace information with resource counts
 */
export interface NamespaceInfo {
  name: string;
  stream_count: number;
  queue_count: number;
  kv_count: number;
  workflow_count: number;
  processing_count: number;
  action_count: number;
}

/**
 * Detailed namespace information including resource details
 */
export interface NamespaceDetail {
  name: string;
  streams: StreamInfo[];
  queues: QueueInfo[];
  kv_keys: KVKeyInfo[];
}

// =============================================================================
// Types - Cluster
// =============================================================================

export interface ClusterStats {
  rps: number;
  active_connections: number;
  uptime: string;
  version: string;
  num_shards: number;
  commands_total: number;
  bytes_received: number;
  bytes_sent: number;
  subscriptions: number;
  nodes: NodeStatus[];
}

export interface NodeStatus {
  id: string;
  status: 'healthy' | 'unhealthy' | 'unknown';
  role: string;
  cpu?: number;
  mem?: number;
}

// =============================================================================
// Types - Streams
// =============================================================================

export interface StreamInfo {
  name: string;
  namespace: string;
  partitions: number;
  ingest_rate: number;
  reads: number;
  retention: string;
}

export interface StreamDetail {
  name: string;
  namespace?: string;
  total_count: number;
  total_bytes: number;
  partitions: PartitionInfo[];
  consumer_groups: ConsumerGroupInfo[];
}

export interface PartitionInfo {
  id: number;
  message_count: number;
  bytes: number;
  status: 'healthy' | 'hot' | 'error';
}

export interface ConsumerGroupInfo {
  name: string;
  members: number;
  generation: number;
  lag: number;
  status: 'active' | 'idle';
}

export interface StreamMessage {
  id_ms: number;
  id_seq: number;
  ual_index: number;
  size: number;
}

export interface StreamMessagesResponse {
  messages: StreamMessage[];
  next_cursor?: string;
  limit: number;
  count: number;
  total_count: number;
  total_bytes: number;
}

// Consumer group detail types
export interface GroupMember {
  id: string;
  last_seen: number;
  blocking_until: number;
  stale_sweep_count: number;
}

export interface GroupDetail {
  stream: string;
  group: string;
  namespace: string;
  generation: number;
  partition_count: number;
  member_count: number;
  members?: GroupMember[];
  assignments?: number[];
  pending_count: number;
}

export interface PendingEntry {
  id_ms: number;
  id_seq: number;
  consumer: string;
  delivered_at_ms: number;
  delivery_count: number;
}

export interface GroupPendingResponse {
  pending: PendingEntry[];
  count: number;
}

// =============================================================================
// Types - Queues
// =============================================================================

export interface QueueInfo {
  name: string;
  namespace: string;
  pending: number;
  available: number;
  enqueued: number;
  dequeued: number;
  acked: number;
  nacked: number;
  dlq_count: number;
  bytes_total: number;
}

export interface QueueDetail {
  name: string;
  namespace: string;
  pending: number;
  available: number;
  enqueued: number;
  dequeued: number;
  acked: number;
  nacked: number;
  dlq_count: number;
  bytes_total: number;
  // Optional fields — not yet supported by backend, default to 0
  delayed?: number;
  lease_timeout_ms?: number;
  max_retries?: number;
  created_at_ms?: number;
  enqueue_rate?: number;
  dequeue_rate?: number;
}

export interface QueueMessage {
  seq: number;
  priority: number;
  status: 'available' | 'leased' | 'delayed';
  header: string;
  payload: string;
  enqueued_at_ms: number;
  lease_expires_ms?: number;
  consumer?: string;
  delivery_count: number;
  delay_until_ms?: number;
  dedup_key?: string;
  message_type?: string;
  correlation_id?: string;
  labels?: Record<string, string>;
}

export interface QueueDLQEntry {
  seq: number;
  header: string;
  payload: string;
  error_msg: string;
  attempts: number;
  dlq_at_ms: number;
  partition: number;
  message_type?: string;
}

export interface QueueMessagesResponse {
  messages: QueueMessage[];
  count: number;
  total: number;
}

export interface QueueDLQResponse {
  entries: QueueDLQEntry[];
  count: number;
}

// =============================================================================
// Types - KV
// =============================================================================

export interface KVNamespaceInfo {
  name: string;
  key_count: number;
  bytes_stored: number;
  get_ops: number;
  set_ops: number;
  delete_ops: number;
}

export interface KVNamespaceDetail {
  namespace: string;
  key_count: number;
  bytes_stored: number;
  get_ops: number;
  set_ops: number;
  delete_ops: number;
}

export interface KVKeyInfo {
  key: string;
  namespace: string;
  current_lsn: number;
  version_count: number;
  size: number;
  ttl_expiry?: number;
  last_modified: number;
}

/**
 * KV key value response from GET /kv/namespaces/:ns/keys/:key
 */
export interface KVKeyValueResponse {
  key: string;
  namespace: string;
  value?: string;
  version?: number;
  found: boolean;
}

/**
 * KV scan response from GET /kv/namespaces/:ns/keys
 */
export interface KVScanResponse {
  keys: KVScanEntry[];
  has_more: boolean;
  cursor: string | null;
  count: number;
}

export interface KVScanEntry {
  key: string;
  namespace: string;
  value: string;
  version: number;
  size: number;
}

/**
 * KV history response from GET /kv/namespaces/:ns/keys/:key/history
 */
export interface KVHistoryResponse {
  key: string;
  namespace: string;
  versions: KVVersionEntry[];
  version_count: number;
}

export interface KVVersionEntry {
  version: number;
  lsn: number;
  value: string;
  deleted: boolean;
}

/**
 * KV put response from PUT /kv/namespaces/:ns/keys/:key
 */
export interface KVPutResponse {
  key: string;
  namespace: string;
  ok: boolean;
  version: number;
}

/**
 * KV delete response from DELETE /kv/namespaces/:ns/keys/:key
 */
export interface KVDeleteResponse {
  key: string;
  namespace: string;
  ok: boolean;
}

// =============================================================================
// Types - Actions & Workers (Layer 2)
// =============================================================================

/** Run statistics for an action */
export interface ActionRunCounts {
  total: number;
  pending: number;
  running: number;
  completed: number;
  failed: number;
  cancelled: number;
  timed_out: number;
}

/** Action info returned from GET /actions */
export interface ActionInfo {
  name: string;
  namespace: string;
  type: 'user' | 'wasm';
  owner: string;
  description: string;
  version: string;
  enabled: boolean;
  timeout_ms: number;
  max_retries: number;
  created_at: number;
  updated_at: number;
  trigger_stream?: string;
  trigger_group?: string;
  runs: ActionRunCounts;
  worker_count: number;
  wasm_module_size?: number;
}

/** Action run info */
export interface ActionRunInfo {
  run_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled' | 'timed_out';
  attempt: number;
  created_at: number;
  started_at?: number;
  completed_at?: number;
  worker_id?: string;
  error?: string;
  outcome?: string;
  input?: string;
  output?: string;
  source?: 'direct' | 'workflow' | 'trigger';
}

/** Action detail returned from GET /actions/:name */
export interface ActionDetail {
  name: string;
  namespace: string;
  type: 'user' | 'wasm';
  owner: string;
  description: string;
  version: string;
  enabled: boolean;
  timeout_ms: number;
  max_retries: number;
  retry_delay_ms: number;
  created_at: number;
  updated_at: number;
  trigger_stream?: string;
  trigger_group?: string;
  wasm_module_size?: number;
  runs: ActionRunCounts;
  recent_runs: ActionRunInfo[];
  workers: WorkerInfo[];
}

/** Process info within a worker */
export interface ProcessInfo {
  name: string;
  kind: 'action' | 'stream_consumer';
  run_count: number;
  fail_count: number;
  last_run_at: number;
}

/** Worker info from GET /workers */
export interface WorkerInfo {
  worker_id: string;
  status: 'active' | 'idle' | 'draining' | 'unhealthy';
  worker_type: 'action' | 'stream';
  namespace: string;
  machine_id: string | null;
  current_load: number;
  max_concurrent: number;
  tasks_completed: number;
  tasks_failed: number;
  last_seen: number;
  registered_at: number;
  metadata: string | null;
  processes: ProcessInfo[];
}

/** Action invoke response */
export interface ActionInvokeResult {
  status: string;
  run_id: string;
}

export interface WorkflowInfo {
  id: string;
  name: string;
  status: 'running' | 'completed' | 'failed' | 'pending';
  step: string;
  started: string;
}

// =============================================================================
// Types - Processing Jobs
// =============================================================================

/** Job entry returned by GET /processing/jobs (from statusJson) */
export interface ProcessingJobInfo {
  job_id: string;
  name: string;
  namespace: string;
  state: 'CREATED' | 'RUNNING' | 'FINISHED' | 'STOPPED' | 'CANCELLED' | 'FAILED';
  parallelism: number;
  records_processed: number;
  created_at_ms: number;
  updated_at_ms: number;
  completed_at_ms?: number;
  last_savepoint_id?: string;
  error?: string;
  source?: {
    kind: string;
    name: string;
    namespace: string;
  };
  sink?: {
    kind: string;
    name: string;
    namespace: string;
  };
  operators?: string;
}

/** Response from POST /processing/jobs */
export interface ProcessingSubmitResult {
  job_id: string;
  state: string;
}

/** Response from POST /processing/jobs/:id/stop|cancel|restore */
export interface ProcessingActionResult {
  ok: boolean;
  job_id: string;
  state?: string;
}

/** Response from POST /processing/jobs/:id/savepoint */
export interface ProcessingSavepointResult {
  ok: boolean;
  job_id: string;
  savepoint_id: string;
}

/** Response from POST /processing/jobs/:id/rescale */
export interface ProcessingRescaleResult {
  ok: boolean;
  job_id: string;
  parallelism: number;
}

// =============================================================================
// Types - Metrics
// =============================================================================

export interface MetricsInfo {
  server: {
    connections: number;
    subscriptions: number;
    commands_total: number;
    bytes_received: number;
    bytes_sent: number;
    uptime_seconds: number;
  };
  streams: number;
  queues: number;
}

export interface ApiError {
  error: string;
}

// =============================================================================
// API Client
// =============================================================================

class FloApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE) {
    this.baseUrl = baseUrl;
  }

  private async fetch<T>(path: string, options?: RequestInit): Promise<T> {
    const headers = new Headers(options?.headers);
    const token = getAuthToken();
    if (token && !headers.has('Authorization')) {
      headers.set('Authorization', `Bearer ${token}`);
    }

    const response = await fetch(`${this.baseUrl}/${path}`, { ...options, headers });

    if (response.status === 401) {
      // Clear stale auth and redirect to login
      localStorage.removeItem('flo:auth');
      if (window.location.pathname !== '/login') {
        window.location.href = '/login';
      }
      throw new Error('Session expired');
    }

    if (!response.ok) {
      const error: ApiError = await response.json().catch(() => ({
        error: `HTTP ${response.status}: ${response.statusText}`,
      }));
      throw new Error(error.error);
    }

    return response.json();
  }

  // ---------------------------------------------------------------------------
  // Namespaces (Unified Resource)
  // ---------------------------------------------------------------------------

  /**
   * List all namespaces with resource counts.
   * Returns namespaces across all primitives (streams, queues, KV).
   */
  async getNamespaces(): Promise<NamespaceInfo[]> {
    return this.fetch<NamespaceInfo[]>('namespaces');
  }

  /**
   * Create a new namespace.
   */
  async createNamespace(name: string): Promise<{ name: string; ok: boolean }> {
    return this.fetch<{ name: string; ok: boolean }>('namespaces', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
  }

  /**
   * Get detailed information about a specific namespace.
   * Includes lists of all resources (streams, queues, KV keys) in the namespace.
   */
  async getNamespace(name: string): Promise<NamespaceDetail> {
    return this.fetch<NamespaceDetail>(`namespaces/${encodeURIComponent(name)}`);
  }

  /**
   * Get streams in a specific namespace
   */
  async getNamespaceStreams(namespace: string): Promise<StreamInfo[]> {
    return this.fetch<StreamInfo[]>(`namespaces/${encodeURIComponent(namespace)}/streams`);
  }

  /**
   * Get queues in a specific namespace
   */
  async getNamespaceQueues(namespace: string): Promise<QueueInfo[]> {
    return this.fetch<QueueInfo[]>(`namespaces/${encodeURIComponent(namespace)}/queues`);
  }

  /**
   * Get KV keys in a specific namespace
   */
  async getNamespaceKV(namespace: string): Promise<KVKeyInfo[]> {
    return this.fetch<KVKeyInfo[]>(`namespaces/${encodeURIComponent(namespace)}/kv`);
  }

  // ---------------------------------------------------------------------------
  // KV
  // ---------------------------------------------------------------------------

  /**
   * Get all KV namespaces with stats
   */
  async getKVNamespaces(): Promise<KVNamespaceInfo[]> {
    return this.fetch<KVNamespaceInfo[]>('kv/namespaces');
  }

  /**
   * Get namespace-level KV stats
   */
  async getKVNamespaceDetail(namespace: string): Promise<KVNamespaceDetail> {
    return this.fetch<KVNamespaceDetail>(`kv/namespaces/${encodeURIComponent(namespace)}`);
  }

  /**
   * Scan/list keys in a KV namespace
   * @param namespace - The namespace to scan
   * @param prefix - Optional key prefix filter
   * @param limit - Max keys to return (default 200, max 1000)
   * @param cursor - Pagination cursor from previous response
   */
  async getKVKeys(namespace: string, prefix = '', limit = 200, cursor?: string): Promise<KVScanResponse> {
    let url = `kv/namespaces/${encodeURIComponent(namespace)}/keys?limit=${limit}`;
    if (prefix) url += `&prefix=${encodeURIComponent(prefix)}`;
    if (cursor) url += `&cursor=${encodeURIComponent(cursor)}`;
    return this.fetch<KVScanResponse>(url);
  }

  /**
   * Get a specific key's value
   * @param namespace - The namespace
   * @param key - The key name
   * @param version - Optional version for time-travel reads
   */
  async getKVKeyValue(namespace: string, key: string, version?: number): Promise<KVKeyValueResponse> {
    let url = `kv/namespaces/${encodeURIComponent(namespace)}/keys/${encodeURIComponent(key)}`;
    if (version !== undefined) url += `?version=${version}`;
    return this.fetch<KVKeyValueResponse>(url);
  }

  /**
   * Get version history for a key
   * @param namespace - The namespace
   * @param key - The key name
   * @param limit - Max versions to return (default 10)
   */
  async getKVKeyHistory(namespace: string, key: string, limit = 20): Promise<KVHistoryResponse> {
    return this.fetch<KVHistoryResponse>(
      `kv/namespaces/${encodeURIComponent(namespace)}/keys/${encodeURIComponent(key)}/history?limit=${limit}`
    );
  }

  /**
   * Set a key's value (create or update)
   * @param namespace - The namespace
   * @param key - The key name
   * @param value - The value to set
   */
  async putKVKey(namespace: string, key: string, value: string): Promise<KVPutResponse> {
    return this.fetch<KVPutResponse>(
      `kv/namespaces/${encodeURIComponent(namespace)}/keys/${encodeURIComponent(key)}`,
      { method: 'PUT', body: value }
    );
  }

  /**
   * Delete a key
   * @param namespace - The namespace
   * @param key - The key name
   */
  async deleteKVKey(namespace: string, key: string): Promise<KVDeleteResponse> {
    return this.fetch<KVDeleteResponse>(
      `kv/namespaces/${encodeURIComponent(namespace)}/keys/${encodeURIComponent(key)}`,
      { method: 'DELETE' }
    );
  }

  // ---------------------------------------------------------------------------
  // Cluster
  // ---------------------------------------------------------------------------

  /**
   * Get cluster health and statistics
   */
  async getClusterStats(): Promise<ClusterStats> {
    return this.fetch<ClusterStats>('cluster/stats');
  }

  // ---------------------------------------------------------------------------
  // Streams
  // ---------------------------------------------------------------------------

  /**
   * List all streams across all namespaces
   */
  async getStreams(): Promise<StreamInfo[]> {
    return this.fetch<StreamInfo[]>('streams');
  }

  /**
   * Get stream details
   */
  async getStreamDetail(name: string): Promise<StreamDetail> {
    return this.fetch<StreamDetail>(`streams/${encodeURIComponent(name)}`);
  }

  /**
   * Get messages from a stream with cursor-based pagination
   * @param cursor - StreamID cursor "ts-seq" (omit for start of stream)
   * @param partition - optional partition number (omit for partition 0)
   */
  async getStreamMessages(name: string, cursor?: string, limit = 2000, partition?: number): Promise<StreamMessagesResponse> {
    let url = `streams/${encodeURIComponent(name)}/messages?limit=${limit}`;
    if (cursor) {
      url += `&cursor=${encodeURIComponent(cursor)}`;
    }
    if (partition !== undefined && partition > 0) {
      url += `&partition=${partition}`;
    }
    return this.fetch<StreamMessagesResponse>(url);
  }

  /**
   * Get consumer group detail including members and assignments
   */
  async getGroupDetail(streamName: string, groupName: string): Promise<GroupDetail> {
    return this.fetch<GroupDetail>(
      `streams/${encodeURIComponent(streamName)}/groups/${encodeURIComponent(groupName)}`
    );
  }

  /**
   * Get consumer group members
   */
  async getGroupMembers(streamName: string, groupName: string): Promise<GroupMember[]> {
    return this.fetch<GroupMember[]>(
      `streams/${encodeURIComponent(streamName)}/groups/${encodeURIComponent(groupName)}/members`
    );
  }

  /**
   * Get pending messages for a consumer group
   */
  async getGroupPending(streamName: string, groupName: string): Promise<GroupPendingResponse> {
    return this.fetch<GroupPendingResponse>(
      `streams/${encodeURIComponent(streamName)}/groups/${encodeURIComponent(groupName)}/pending`
    );
  }

  // ---------------------------------------------------------------------------
  // Queues
  // ---------------------------------------------------------------------------

  /**
   * List all queues, optionally filtered by namespace
   */
  async getQueues(namespace?: string): Promise<QueueInfo[]> {
    const url = namespace ? `queues?namespace=${encodeURIComponent(namespace)}` : 'queues';
    return this.fetch<QueueInfo[]>(url);
  }

  /**
   * Get queue details
   */
  async getQueueDetail(name: string): Promise<QueueDetail> {
    return this.fetch<QueueDetail>(`queues/${encodeURIComponent(name)}`);
  }

  /**
   * Get messages from a queue
   */
  async getQueueMessages(name: string, status?: string, limit = 50): Promise<QueueMessagesResponse> {
    let url = `queues/${encodeURIComponent(name)}/messages?limit=${limit}`;
    if (status) url += `&status=${status}`;
    return this.fetch<QueueMessagesResponse>(url);
  }

  /**
   * Get DLQ entries for a queue
   */
  async getQueueDLQ(name: string, limit = 50): Promise<QueueDLQResponse> {
    return this.fetch<QueueDLQResponse>(`queues/${encodeURIComponent(name)}/dlq?limit=${limit}`);
  }

  /**
   * Requeue a DLQ entry back to the main queue
   */
  async requeueDLQEntry(queueName: string, seq: number): Promise<{ ok: boolean }> {
    return this.fetch<{ ok: boolean }>(`queues/${encodeURIComponent(queueName)}/dlq/${seq}/requeue`, {
      method: 'POST',
    });
  }

  /**
   * Delete a DLQ entry
   */
  async deleteDLQEntry(queueName: string, seq: number): Promise<{ ok: boolean }> {
    return this.fetch<{ ok: boolean }>(`queues/${encodeURIComponent(queueName)}/dlq/${seq}`, {
      method: 'DELETE',
    });
  }

  /**
   * Purge all messages from a queue
   */
  async purgeQueue(name: string): Promise<{ ok: boolean; purged: number }> {
    return this.fetch<{ ok: boolean; purged: number }>(`queues/${encodeURIComponent(name)}/purge`, {
      method: 'POST',
    });
  }

  // ---------------------------------------------------------------------------
  // Actions & Workers (Layer 2)
  // ---------------------------------------------------------------------------

  /**
   * List registered actions with run statistics
   */
  async getActions(namespace?: string): Promise<ActionInfo[]> {
    const params = namespace ? `?namespace=${encodeURIComponent(namespace)}` : '';
    return this.fetch<ActionInfo[]>(`actions${params}`);
  }

  /**
   * Get detailed action information including recent runs and workers
   */
  async getActionDetail(name: string): Promise<ActionDetail> {
    return this.fetch<ActionDetail>(`actions/${encodeURIComponent(name)}`);
  }

  /**
   * Get runs for a specific action
   */
  async getActionRuns(name: string, limit = 50): Promise<ActionRunInfo[]> {
    return this.fetch<ActionRunInfo[]>(`actions/${encodeURIComponent(name)}/runs?limit=${limit}`);
  }

  /**
   * Trigger an action from the dashboard
   */
  async invokeAction(name: string, input?: string): Promise<ActionInvokeResult> {
    return this.fetch<ActionInvokeResult>(`actions/${encodeURIComponent(name)}/invoke`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: input || '{}',
    });
  }

  /**
   * List all registered workers
   */
  async getWorkers(namespace?: string): Promise<WorkerInfo[]> {
    const qs = namespace ? `?namespace=${encodeURIComponent(namespace)}` : '';
    return this.fetch<WorkerInfo[]>(`workers${qs}`);
  }

  /**
   * Get a single worker by ID
   */
  async getWorkerDetail(workerId: string): Promise<WorkerInfo> {
    return this.fetch<WorkerInfo>(`workers/${encodeURIComponent(workerId)}`);
  }

  /**
   * List workflows
   */
  async getWorkflows(): Promise<WorkflowInfo[]> {
    return this.fetch<WorkflowInfo[]>('workflows');
  }

  /**
   * Get workflow status
   */
  async getWorkflowStatus(runId: string): Promise<WorkflowInfo> {
    return this.fetch<WorkflowInfo>(`workflows/${encodeURIComponent(runId)}`);
  }

  /**
   * Get workflow execution history
   */
  async getWorkflowHistory(runId: string): Promise<unknown[]> {
    return this.fetch<unknown[]>(`workflows/${encodeURIComponent(runId)}/history`);
  }

  // ---------------------------------------------------------------------------
  // Processing Jobs
  // ---------------------------------------------------------------------------

  /**
   * List all processing jobs
   */
  async getProcessingJobs(): Promise<ProcessingJobInfo[]> {
    return this.fetch<ProcessingJobInfo[]>('processing/jobs');
  }

  /**
   * Get full detail for a specific processing job
   */
  async getProcessingJobDetail(jobId: string): Promise<import('./processing-types').ProcessingJobDetail> {
    return this.fetch<import('./processing-types').ProcessingJobDetail>(`processing/jobs/${encodeURIComponent(jobId)}`);
  }

  /**
   * Submit a new processing job from YAML definition
   */
  async submitProcessingJob(yaml: string): Promise<ProcessingSubmitResult> {
    return this.fetch<ProcessingSubmitResult>('processing/jobs', {
      method: 'POST',
      body: yaml,
    });
  }

  /**
   * Stop a running processing job gracefully
   */
  async stopProcessingJob(jobId: string): Promise<ProcessingActionResult> {
    return this.fetch<ProcessingActionResult>(`processing/jobs/${encodeURIComponent(jobId)}/stop`, {
      method: 'POST',
    });
  }

  /**
   * Cancel a processing job immediately
   */
  async cancelProcessingJob(jobId: string): Promise<ProcessingActionResult> {
    return this.fetch<ProcessingActionResult>(`processing/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: 'POST',
    });
  }

  /**
   * Create a savepoint for a processing job
   */
  async createProcessingSavepoint(jobId: string): Promise<ProcessingSavepointResult> {
    return this.fetch<ProcessingSavepointResult>(`processing/jobs/${encodeURIComponent(jobId)}/savepoint`, {
      method: 'POST',
    });
  }

  /**
   * Restore a processing job from a savepoint
   */
  async restoreProcessingJob(jobId: string, savepointId?: string): Promise<ProcessingActionResult> {
    return this.fetch<ProcessingActionResult>(`processing/jobs/${encodeURIComponent(jobId)}/restore`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: savepointId ? JSON.stringify({ savepoint_id: savepointId }) : '{}',
    });
  }

  /**
   * Rescale a processing job's parallelism
   */
  async rescaleProcessingJob(jobId: string, parallelism: number): Promise<ProcessingRescaleResult> {
    return this.fetch<ProcessingRescaleResult>(`processing/jobs/${encodeURIComponent(jobId)}/rescale`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ parallelism }),
    });
  }

  // ---------------------------------------------------------------------------
  // Time Series
  // ---------------------------------------------------------------------------

  /**
   * List all time-series measurements, optionally filtered by namespace
   */
  async getTimeSeries(namespace?: string): Promise<import('./ts-types').TsMeasurement[]> {
    const url = namespace ? `timeseries?namespace=${encodeURIComponent(namespace)}` : 'timeseries';
    return this.fetch<import('./ts-types').TsMeasurement[]>(url);
  }

  /**
   * Get detailed information about a specific measurement
   */
  async getTimeSeriesDetail(measurement: string, namespace?: string): Promise<import('./ts-types').TsMeasurementDetail> {
    let url = `timeseries/${encodeURIComponent(measurement)}`;
    if (namespace) url += `?namespace=${encodeURIComponent(namespace)}`;
    return this.fetch<import('./ts-types').TsMeasurementDetail>(url);
  }

  /**
   * Get time-series data points for charting.
   * Dispatches ts_query across all shards and returns aggregated JSON.
   */
  async getTimeSeriesData(
    measurement: string,
    options?: {
      namespace?: string;
      field?: string;
      from?: number;
      to?: number;
      window?: number;
      aggregation?: string;
    }
  ): Promise<import('./ts-types').TsDataResponse> {
    const params = new URLSearchParams();
    if (options?.namespace) params.set('namespace', options.namespace);
    if (options?.field) params.set('field', options.field);
    if (options?.from) params.set('from', String(options.from));
    if (options?.to) params.set('to', String(options.to));
    if (options?.window) params.set('window', String(options.window));
    if (options?.aggregation) params.set('aggregation', options.aggregation);
    const qs = params.toString();
    const url = `timeseries/${encodeURIComponent(measurement)}/data${qs ? `?${qs}` : ''}`;
    return this.fetch<import('./ts-types').TsDataResponse>(url);
  }

  /**
   * Execute a FloQL query.
   * Dispatches ts_floql across all shards and returns aggregated results.
   */
  async executeFloqlQuery(
    query: string,
    namespace?: string
  ): Promise<import('./ts-types').TsFloqlResponse> {
    const params = new URLSearchParams();
    if (namespace) params.set('namespace', namespace);
    const qs = params.toString();
    const url = `timeseries/floql${qs ? `?${qs}` : ''}`;
    return this.fetch<import('./ts-types').TsFloqlResponse>(url, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain' },
      body: query,
    });
  }

  // ---------------------------------------------------------------------------
  // Metrics
  // ---------------------------------------------------------------------------

  /**
   * Get metrics in JSON format
   */
  async getMetrics(): Promise<MetricsInfo> {
    return this.fetch<MetricsInfo>('metrics');
  }

  /**
   * Health check endpoint
   */
  async healthCheck(): Promise<{ status: string }> {
    const response = await fetch('/health');
    return response.json();
  }
}

// Default client instance
export const api = new FloApiClient();

// Factory for custom base URLs
export function createApiClient(baseUrl: string): FloApiClient {
  return new FloApiClient(baseUrl);
}
