// =============================================================================
// Workflow Types — modeled after src/workflow/ Zig module
// Only types actively used by UI components are exported here.
// =============================================================================

// --- Run Status (types.zig: RunStatus enum u8) ---
export type WorkflowRunStatus =
  | 'pending'
  | 'running'
  | 'waiting'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'timed_out';

// --- Wait Types (types.zig: WaitType) ---
export type WaitType =
  | 'signal'
  | 'approval'
  | 'callback'
  | 'timer'
  | 'child_workflow'
  | 'awaiting_action'
  | 'polling';

// --- History Event Types (history.zig: EventType enum u8) ---
export type HistoryEventType =
  // Workflow lifecycle
  | 'workflow_started'
  | 'workflow_completed'
  | 'workflow_failed'
  | 'workflow_cancelled'
  | 'workflow_timed_out'
  | 'schedule_started'
  | 'trigger_started'
  // Step lifecycle
  | 'step_started'
  | 'step_completed'
  | 'step_failed'
  // Plan lifecycle
  | 'plan_started'
  | 'plan_executor_tried'
  | 'plan_completed'
  | 'step_retry'
  // Signal
  | 'waiting_for_signal'
  | 'signal_received'
  | 'signal_matched'
  | 'signal_timeout'
  // Action
  | 'action_completed'
  | 'awaiting_action'
  | 'action_not_found'
  | 'action_disabled'
  // Timer
  | 'timer_scheduled'
  | 'timer_fired'
  // Child workflow
  | 'child_workflow_started'
  | 'child_workflow_completed';

// =============================================================================
// Core Structures
// =============================================================================

// --- History Event (history.zig + event_store.zig) ---

export interface HistoryEvent {
  event_id: number;
  event_type: HistoryEventType;
  timestamp: number;
  step_name?: string;
  // Payload varies by event type
  details: Record<string, unknown>;
  // For step events
  duration_ms?: number;
  outcome?: string;
  error?: string;
  // For plan events
  executor_name?: string;
  attempt?: number;
  // For signal events
  signal_type?: string;
  signal_payload?: unknown;
  // For timer events
  timer_type?: string;
  fire_at?: number;
  // For child workflow events
  child_run_id?: string;
  child_workflow_name?: string;
}

// --- Workflow Run (types.zig: RunSnapshot) ---

export interface WorkflowRun {
  run_id: string;
  workflow_name: string;
  workflow_version: string;
  namespace: string;
  status: WorkflowRunStatus;
  current_step?: string | null;
  wait_type?: WaitType;
  // Timing
  started_at: number;
  completed_at?: number;
  duration_ms?: number;
  // Input / Output
  input?: Record<string, unknown>;
  output?: Record<string, unknown>;
  error?: string;
  // Step results (accumulated)
  step_results?: Record<string, {
    outcome: string;
    output: unknown;
    started_at: number;
    completed_at: number;
    duration_ms: number;
    attempts: number;
  }>;
  // Trigger source
  triggered_by?: string;
  // Ancestry
  parent_run_id?: string;
  child_run_ids?: string[];
  ancestry_depth?: number;
  // Search attributes
  search_attributes?: Record<string, unknown>;
  // Signals
  pending_signals?: number;
  // Idempotency
  idempotency_key?: string;
  // History
  history_event_count?: number;
  history?: HistoryEvent[];
}

// --- Event Group (grouping of related events) ---

export type EventGroupCategory =
  | 'workflow'
  | 'step'
  | 'plan'
  | 'signal'
  | 'timer'
  | 'child_workflow';

export type EventGroupStatus = 'completed' | 'failed' | 'running' | 'pending' | 'cancelled' | 'timed_out';

export interface EventGroup {
  id: string;                        // e.g. "step:enrich_email" or "workflow:start"
  category: EventGroupCategory;
  label: string;                     // Display label e.g. "enrich_email (Plan)"
  step_name?: string;
  status: EventGroupStatus;
  events: HistoryEvent[];            // All events in this group
  started_at: number;
  completed_at?: number;
  duration_ms?: number;
  // For plan groups
  executor_attempts?: { executor: string; success: boolean; error?: string; duration_ms?: number }[];
  selected_executor?: string;
  // For signal groups
  signal_type?: string;
  signal_payload?: unknown;
  // Summary details (high-value user data)
  summary: Record<string, unknown>;
}

// --- Saved View (for workflow list filtering) ---

export interface SavedView {
  id: string;
  name: string;
  icon?: string;
  isSystem: boolean;
  filters: {
    status?: WorkflowRunStatus | 'all';
    workflow?: string;
    search?: string;
    hideChildren?: boolean;
    timeRange?: 'all' | 'last_hour' | 'today' | 'last_24h' | 'last_7d';
  };
}
