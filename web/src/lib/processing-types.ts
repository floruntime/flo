/**
 * Flo Processing — Domain Types
 *
 * Models the full processing pipeline feature set:
 * YAML-defined pipelines, operators, windowing, checkpointing, metrics, WASM.
 */

// =============================================================================
// Job State & Lifecycle
// =============================================================================

export type JobState = 'CREATED' | 'RUNNING' | 'FINISHED' | 'STOPPED' | 'CANCELLED' | 'FAILED';

export function isTerminalState(state: JobState): boolean {
  return state === 'FINISHED' || state === 'CANCELLED' || state === 'FAILED';
}

// =============================================================================
// Pipeline Definition (mirrors YAML schema)
// =============================================================================

export type SinkKind = 'stream' | 'kv' | 'queue';
export type WriteMode = 'upsert' | 'if_absent' | 'versioned';

export interface SourceSpec {
  name: string;
  stream: string;
  namespace?: string;
  partition?: string; // "all", "0", "0-63", "0,3,7"
  batch_size?: number;
}

export interface SinkSpec {
  name: string;
  kind: SinkKind;
  target: string;
  namespace?: string;
  tags?: string[];
  key_prefix?: string;
  separator?: string;
  write_mode?: WriteMode;
  ttl_ms?: number;
  priority?: number;
  delay_ms?: number;
  use_key_as_dedup?: boolean;
}

export interface OperatorConfig {
  key: string;
  value: string;
}

export interface OperatorSpec {
  type_name: string;
  name: string;
  module?: string; // WASM module path
  config?: OperatorConfig[];
}

export interface CheckpointingSpec {
  interval_ms?: number;
}

export interface PipelineDefinition {
  kind: 'Processing';
  name: string;
  parallelism: number;
  batch_size?: number;
  source: SourceSpec;
  sink: SinkSpec;
  operators: OperatorSpec[];
  checkpointing?: CheckpointingSpec;
}

// =============================================================================
// Operator Types (from native registry)
// =============================================================================

export type OperatorType = 'filter' | 'passthrough' | 'keyby' | 'aggregate' | 'map' | 'flatmap' | 'kv_lookup' | 'classify' | 'wasm';

export interface OperatorInfo {
  name: string;
  type: OperatorType;
  stateful: boolean;
  records_in: number;
  records_out: number;
  processing_time_ns: number;
  errors: number;
  last_processed_ms: number;
  config?: OperatorConfig[];
}

// =============================================================================
// Windowing
// =============================================================================

export type WindowType = 'tumbling' | 'sliding' | 'global' | 'count' | 'session';
export type TriggerType = 'event_time' | 'processing_time' | 'count' | 'continuous';

export interface WindowSpec {
  type: WindowType;
  size_ms?: number;
  slide_ms?: number;
  offset_ms?: number;
  max_count?: number;
  gap_ms?: number; // session windows
}

export interface TriggerSpec {
  type: TriggerType;
  threshold?: number;
  interval_ms?: number;
}

// =============================================================================
// Checkpointing
// =============================================================================

export type CheckpointStatus = 'in_progress' | 'completed' | 'failed';

export interface CheckpointMeta {
  checkpoint_id: number;
  timestamp_ms: number;
  status: CheckpointStatus;
  acked_operators: number;
  total_operators: number;
  offsets_saved: boolean;
  duration_ms?: number;
  size_bytes?: number;
}

// =============================================================================
// Metrics
// =============================================================================

export interface LatencyBucket {
  label: string;
  count: number;
  range_ms: [number, number];
}

export interface ThroughputPoint {
  timestamp: number;
  records_per_sec: number;
}

export interface BackpressureGauge {
  busy_ratio: number; // 0.0 – 1.0
  is_backpressured: boolean; // > 0.8
}

export interface PipelineMetrics {
  input_throughput: number;     // records/sec
  output_throughput: number;    // records/sec
  e2e_latency_ms: number;      // avg end-to-end
  watermark_lag_ms: number;
  checkpoints_completed: number;
  last_checkpoint_duration_ms: number;
  records_dropped: number;
  backpressure: BackpressureGauge;
  latency_histogram: LatencyBucket[];
  throughput_history: ThroughputPoint[];
}

// =============================================================================
// Watermark & Time
// =============================================================================

export type WatermarkStrategy = 'none' | 'ascending' | 'bounded_out_of_order';

export interface WatermarkInfo {
  strategy: WatermarkStrategy;
  current_watermark_ms: number;
  max_delay_ms?: number;
}

// =============================================================================
// Tags
// =============================================================================

export interface TagInfo {
  name: string;
  bit: number;
  total_matched: number;
}

// =============================================================================
// Job (full detail)
// =============================================================================

export interface JobEndpoint {
  name: string;
  type: 'stream' | 'kv' | 'queue';
  target: string;
  namespace?: string;
}

export interface ProcessingJobDetail {
  job_id: string;
  namespace: string;
  job_name: string;
  state: JobState;
  parallelism: number;
  records_processed: number;
  created_at_ms: number;
  updated_at_ms: number;
  completed_at_ms?: number;
  error_message?: string;
  definition_yaml: string;
  source: JobEndpoint;
  sink: JobEndpoint;
  operators: OperatorInfo[];
  metrics: PipelineMetrics;
  checkpoints: CheckpointMeta[];
  watermark?: WatermarkInfo;
  tags?: TagInfo[];
  savepoint_id?: string;
}

// =============================================================================
// Job State Transition (Timeline)
// =============================================================================

export interface JobEvent {
  timestamp_ms: number;
  event_type: 'created' | 'started' | 'checkpoint' | 'savepoint' | 'rescaled' | 'stopped' | 'restored' | 'failed' | 'finished' | 'cancelled';
  description: string;
  metadata?: Record<string, string>;
}
