//! Workflow Definition Types
//!
//! Types for defining workflow structure: steps, transitions, terminals, and inline plans.
//!
//! # Key Types
//!
//! - `WorkflowDefinition`: Complete workflow definition (stored in KV)
//! - `Step`: A workflow step (run action/plan or wait for signal)
//! - `Transition`: State transition based on outcome
//! - `Terminal`: End state with status mapping
//! - `InlinePlan`: Local plan defined within the workflow
//!
//! # YAML Structure
//!
//! ```yaml
//! kind: Workflow
//! name: process-order
//! version: "1.0.0"
//! idempotency: required
//!
//! # Optional: recurring schedule (cron or interval)
//! schedule:
//!   cron: "0 */6 * * *"     # or: interval: 30000
//!   max_concurrent: 1
//!   input: '{"mode": "full"}'
//!
//! # Inline plans (local to this workflow)
//! plans:
//!   payment:
//!     selection: health-weighted
//!     executors:
//!       - action: "@actions/stripe-charge"
//!         priority: 1
//!       - action: "@actions/paypal-charge"
//!         priority: 2
//!
//! start:
//!   run: "@actions/validate-order"
//!   transitions:
//!     success: charge_payment
//!     failure: flo.Failed
//!
//! steps:
//!   charge_payment:
//!     run: "@plan/payment"  # References local plan
//!     transitions:
//!       success: flo.Completed
//!       failure: flo.Failed
//!
//! terminals:  # Optional - only for custom terminals
//!   PaymentFailed:
//!     status: failed
//! ```

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const types = @import("types.zig");
const plan_types = @import("plan_types.zig");

// Re-export from types for convenience
pub const RunStatus = types.RunStatus;

// Re-export RetryPolicy and BackoffType from plan_types
pub const RetryPolicy = plan_types.RetryPolicy;
pub const BackoffType = plan_types.BackoffType;

// =============================================================================
// Poll Configuration
// =============================================================================

/// Configuration for polling on "pending" outcome.
/// When an action returns outcome="pending", if poll is configured,
/// the workflow will re-execute the action with backoff until a terminal
/// outcome (success, failure, timeout, cancelled) or max_attempts exceeded.
pub const PollConfig = struct {
    /// Initial delay before first poll (ms)
    initial_delay_ms: i64 = 0,
    /// Maximum number of poll attempts
    max_attempts: u32 = 10,
    /// Backoff strategy between polls
    backoff: BackoffType = .exponential,
    /// Base delay between polls (ms)
    base_delay_ms: u32 = 1000,
    /// Maximum delay cap (ms)
    max_delay_ms: u32 = 60000,

    pub fn encode(self: PollConfig, buf: *std.ArrayList(u8), allocator: Allocator) !void {
        var i64_bytes: [8]u8 = undefined;
        mem.writeInt(i64, &i64_bytes, self.initial_delay_ms, .big);
        try buf.appendSlice(allocator, &i64_bytes);

        var u32_bytes: [4]u8 = undefined;
        mem.writeInt(u32, &u32_bytes, self.max_attempts, .big);
        try buf.appendSlice(allocator, &u32_bytes);

        try buf.append(allocator, @intFromEnum(self.backoff));

        mem.writeInt(u32, &u32_bytes, self.base_delay_ms, .big);
        try buf.appendSlice(allocator, &u32_bytes);

        mem.writeInt(u32, &u32_bytes, self.max_delay_ms, .big);
        try buf.appendSlice(allocator, &u32_bytes);
    }

    pub fn decode(data: []const u8, pos: *usize) !PollConfig {
        if (pos.* + 21 > data.len) return error.InvalidData;

        const initial_delay_ms = mem.readInt(i64, data[pos.*..][0..8], .big);
        pos.* += 8;

        const max_attempts = mem.readInt(u32, data[pos.*..][0..4], .big);
        pos.* += 4;

        const backoff: BackoffType = @enumFromInt(data[pos.*]);
        pos.* += 1;

        const base_delay_ms = mem.readInt(u32, data[pos.*..][0..4], .big);
        pos.* += 4;

        const max_delay_ms = mem.readInt(u32, data[pos.*..][0..4], .big);
        pos.* += 4;

        return .{
            .initial_delay_ms = initial_delay_ms,
            .max_attempts = max_attempts,
            .backoff = backoff,
            .base_delay_ms = base_delay_ms,
            .max_delay_ms = max_delay_ms,
        };
    }

    /// Calculate delay for a given attempt (0-indexed)
    pub fn calculateDelay(self: PollConfig, attempt: u32) u32 {
        const delay: u32 = switch (self.backoff) {
            .constant => self.base_delay_ms,
            .linear => self.base_delay_ms * (attempt + 1),
            .exponential => blk: {
                const multiplier = std.math.powi(u32, 2, attempt) catch std.math.maxInt(u32);
                break :blk self.base_delay_ms *| multiplier;
            },
            .exponential_jitter => blk: {
                const base_multiplier = std.math.powi(u32, 2, attempt) catch std.math.maxInt(u32);
                const base_delay = self.base_delay_ms *| base_multiplier;
                // Add up to 25% jitter
                var rng = std.Random.DefaultPrng.init(@intCast(std.time.timestamp()));
                const jitter = rng.random().intRangeAtMost(u32, 0, base_delay / 4);
                break :blk base_delay +| jitter;
            },
        };
        return @min(delay, self.max_delay_ms);
    }
};

// Re-export plan types for inline plans
pub const SelectionStrategy = plan_types.SelectionStrategy;
pub const ErrorClassification = plan_types.ErrorClassification;
pub const ExecutorConfig = plan_types.ExecutorConfig;
pub const HealthConfig = plan_types.HealthConfig;
pub const CacheConfig = plan_types.CacheConfig;
pub const FallbackConfig = plan_types.FallbackConfig;

// =============================================================================
// Idempotency Mode
// =============================================================================

/// Idempotency mode for workflow execution
pub const IdempotencyMode = enum(u8) {
    /// No idempotency check
    none = 0,
    /// Idempotency key optional
    optional = 1,
    /// Idempotency key required
    required = 2,

    pub fn toString(self: IdempotencyMode) []const u8 {
        return switch (self) {
            .none => "none",
            .optional => "optional",
            .required => "required",
        };
    }

    pub fn fromString(s: []const u8) ?IdempotencyMode {
        const map = std.StaticStringMap(IdempotencyMode).initComptime(.{
            .{ "none", .none },
            .{ "optional", .optional },
            .{ "required", .required },
        });
        return map.get(s);
    }
};

// =============================================================================
// Search Attribute Definition
// =============================================================================

/// Type of search attribute
pub const SearchAttrType = enum(u8) {
    string = 0,
    number = 1,
    timestamp = 2,

    pub fn toString(self: SearchAttrType) []const u8 {
        return switch (self) {
            .string => "string",
            .number => "number",
            .timestamp => "timestamp",
        };
    }

    pub fn fromString(s: []const u8) ?SearchAttrType {
        const map = std.StaticStringMap(SearchAttrType).initComptime(.{
            .{ "string", .string },
            .{ "number", .number },
            .{ "timestamp", .timestamp },
        });
        return map.get(s);
    }
};

/// Search attribute definition in workflow spec
pub const SearchAttrDef = struct {
    /// Attribute name (e.g., "customer_id")
    name: []const u8,
    /// Attribute type
    attr_type: SearchAttrType,
    /// Path to extract from input (e.g., "input.customer_id")
    from: []const u8,

    pub fn deinit(self: *SearchAttrDef, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.from);
    }

    pub fn clone(self: SearchAttrDef, allocator: Allocator) !SearchAttrDef {
        return .{
            .name = try allocator.dupe(u8, self.name),
            .attr_type = self.attr_type,
            .from = try allocator.dupe(u8, self.from),
        };
    }
};

// =============================================================================
// Transition
// =============================================================================

/// State transition definition
pub const Transition = struct {
    /// Outcome that triggers this transition (e.g., "success", "failure")
    outcome: []const u8,
    /// Target state or terminal (e.g., "charge_payment", "flo.Completed")
    target: []const u8,

    pub fn deinit(self: *Transition, allocator: Allocator) void {
        allocator.free(self.outcome);
        allocator.free(self.target);
    }

    pub fn clone(self: Transition, allocator: Allocator) !Transition {
        return .{
            .outcome = try allocator.dupe(u8, self.outcome),
            .target = try allocator.dupe(u8, self.target),
        };
    }

    /// Check if target is a built-in terminal (flo.*)
    pub fn isBuiltinTerminal(self: Transition) bool {
        return mem.startsWith(u8, self.target, "flo.");
    }

    /// Check if target is a terminal (built-in or custom)
    pub fn isTerminal(self: Transition, terminals: []const Terminal) bool {
        if (self.isBuiltinTerminal()) return true;
        for (terminals) |t| {
            if (mem.eql(u8, t.name, self.target)) return true;
        }
        return false;
    }
};

// =============================================================================
// Terminal
// =============================================================================

/// Terminal state definition (custom terminals only)
/// Built-in terminals (flo.Completed, flo.Failed, flo.Cancelled, flo.TimedOut)
/// don't need to be declared.
pub const Terminal = struct {
    /// Terminal name (e.g., "PaymentFailed", "FraudDetected")
    name: []const u8,
    /// Maps to base status (completed, failed, cancelled, timed_out)
    status: RunStatus,

    pub fn deinit(self: *Terminal, allocator: Allocator) void {
        allocator.free(self.name);
    }

    pub fn clone(self: Terminal, allocator: Allocator) !Terminal {
        return .{
            .name = try allocator.dupe(u8, self.name),
            .status = self.status,
        };
    }
};

/// Well-known step outcome strings.
/// Business outcomes (driven by action/child logic):
pub const StepOutcome = struct {
    pub const success = "success";
    pub const failure = "failure";
    pub const timeout = "timeout";
    pub const pending = "pending";

    // Execution-level outcomes (target dispatch failed before any business logic ran).
    // These map to specific ErrorCode values from the protocol layer.
    pub const target_not_found = "target_not_found";
    pub const target_disabled = "target_disabled";
    pub const execution_failure = "execution_failure";
};

/// Built-in terminal names
pub const BuiltinTerminal = struct {
    pub const Completed = "flo.Completed";
    pub const Failed = "flo.Failed";
    pub const Cancelled = "flo.Cancelled";
    pub const TimedOut = "flo.TimedOut";

    /// Get status for a built-in terminal name
    pub fn statusFor(name: []const u8) ?RunStatus {
        const map = std.StaticStringMap(RunStatus).initComptime(.{
            .{ Completed, .completed },
            .{ Failed, .failed },
            .{ Cancelled, .cancelled },
            .{ TimedOut, .timed_out },
        });
        return map.get(name);
    }

    /// Check if name is a built-in terminal
    pub fn isBuiltin(name: []const u8) bool {
        return statusFor(name) != null;
    }
};

// =============================================================================
// Run Step
// =============================================================================

/// Run step - executes an action, plan, or child workflow
pub const RunStep = struct {
    /// Target to invoke:
    /// - "@actions/action-name" for actions
    /// - "@plan/plan-name" for plans
    /// - "@workflow/workflow-name" for child workflows
    target: []const u8,
    /// JSON path mapping for input transformation (optional)
    input_mapping: ?[]const u8,
    /// Retry policy for execution failures (only for @actions, plans have their own)
    retry: ?RetryPolicy,
    /// Poll configuration for handling "pending" outcomes (action-only)
    /// When action returns "pending", re-execute with backoff until terminal outcome
    poll: ?PollConfig,
    /// Transitions to next states based on outcome
    transitions: []const Transition,

    pub fn deinit(self: *RunStep, allocator: Allocator) void {
        allocator.free(self.target);
        if (self.input_mapping) |m| allocator.free(m);
        for (self.transitions) |t| {
            var mt = t;
            mt.deinit(allocator);
        }
        if (self.transitions.len > 0) allocator.free(@constCast(self.transitions));
    }

    pub fn clone(self: RunStep, allocator: Allocator) !RunStep {
        const transitions = try allocator.alloc(Transition, self.transitions.len);
        for (self.transitions, 0..) |t, i| {
            transitions[i] = try t.clone(allocator);
        }
        return .{
            .target = try allocator.dupe(u8, self.target),
            .input_mapping = if (self.input_mapping) |m| try allocator.dupe(u8, m) else null,
            .retry = self.retry,
            .poll = self.poll,
            .transitions = transitions,
        };
    }

    /// Check if target is an action (@actions/*)
    pub fn isAction(self: RunStep) bool {
        return mem.startsWith(u8, self.target, "@actions/");
    }

    /// Check if target is a plan (@plan/*)
    pub fn isPlan(self: RunStep) bool {
        return mem.startsWith(u8, self.target, "@plan/");
    }

    /// Check if target is a child workflow (@workflow/*)
    pub fn isChildWorkflow(self: RunStep) bool {
        return mem.startsWith(u8, self.target, "@workflow/");
    }

    /// Get the name part of the target (without prefix)
    pub fn targetName(self: RunStep) []const u8 {
        if (self.isAction()) {
            return self.target["@actions/".len..];
        } else if (self.isPlan()) {
            return self.target["@plan/".len..];
        } else if (self.isChildWorkflow()) {
            return self.target["@workflow/".len..];
        }
        return self.target;
    }

    /// Get transition for a given outcome (exact match only)
    pub fn getTransition(self: RunStep, outcome: []const u8) ?Transition {
        for (self.transitions) |t| {
            if (mem.eql(u8, t.outcome, outcome)) return t;
        }
        return null;
    }

    /// Resolve a transition with fallback chain for execution-level outcomes.
    ///
    /// Lookup order:
    ///   1. Exact outcome (e.g. "target_not_found")
    ///   2. "execution_failure" — catch-all for any execution-level outcome
    ///   3. "failure" — broadest catch-all
    ///
    /// Business outcomes ("success", "timeout", custom strings from actions)
    /// do NOT fall through — they match exactly or return null.
    pub fn resolveTransition(self: RunStep, outcome: []const u8) ?Transition {
        // 1. Exact match always wins
        if (self.getTransition(outcome)) |t| return t;

        // 2. Only execution-level outcomes cascade
        if (isExecutionOutcome(outcome)) {
            // Try the generic execution_failure bucket
            if (!mem.eql(u8, outcome, StepOutcome.execution_failure)) {
                if (self.getTransition(StepOutcome.execution_failure)) |t| return t;
            }
            // Fall through to blanket failure
            if (self.getTransition(StepOutcome.failure)) |t| return t;
        }

        return null;
    }

    /// Returns true if this outcome is an execution-level outcome that should
    /// cascade through the fallback chain.
    fn isExecutionOutcome(outcome: []const u8) bool {
        return mem.eql(u8, outcome, StepOutcome.target_not_found) or
            mem.eql(u8, outcome, StepOutcome.target_disabled) or
            mem.eql(u8, outcome, StepOutcome.execution_failure);
    }
};

// =============================================================================
// Wait For Signal Step
// =============================================================================

/// Wait for signal step - pauses workflow until signal received
pub const WaitForSignalStep = struct {
    /// Signal type to wait for (e.g., "payment.confirmed")
    signal_type: []const u8,
    /// Timeout in milliseconds (null = no timeout)
    timeout_ms: ?i64,
    /// Step to transition to on timeout (null = use timeout transition)
    on_timeout: ?[]const u8,
    /// Transitions based on signal payload or timeout
    transitions: []const Transition,

    pub fn deinit(self: *WaitForSignalStep, allocator: Allocator) void {
        allocator.free(self.signal_type);
        if (self.on_timeout) |t| allocator.free(t);
        for (self.transitions) |t| {
            var mt = t;
            mt.deinit(allocator);
        }
        if (self.transitions.len > 0) allocator.free(@constCast(self.transitions));
    }

    pub fn clone(self: WaitForSignalStep, allocator: Allocator) !WaitForSignalStep {
        const transitions = try allocator.alloc(Transition, self.transitions.len);
        for (self.transitions, 0..) |t, i| {
            transitions[i] = try t.clone(allocator);
        }
        return .{
            .signal_type = try allocator.dupe(u8, self.signal_type),
            .timeout_ms = self.timeout_ms,
            .on_timeout = if (self.on_timeout) |t| try allocator.dupe(u8, t) else null,
            .transitions = transitions,
        };
    }

    /// Get transition for a given outcome
    pub fn getTransition(self: WaitForSignalStep, outcome: []const u8) ?Transition {
        for (self.transitions) |t| {
            if (mem.eql(u8, t.outcome, outcome)) return t;
        }
        return null;
    }
};

// =============================================================================
// Step (Union)
// =============================================================================

/// Workflow step - either run or wait for signal
pub const Step = union(enum) {
    /// Execute an action, plan, or child workflow
    run: RunStep,
    /// Wait for an external signal
    wait_for_signal: WaitForSignalStep,

    pub fn deinit(self: *Step, allocator: Allocator) void {
        switch (self.*) {
            .run => |*r| r.deinit(allocator),
            .wait_for_signal => |*w| w.deinit(allocator),
        }
    }

    pub fn clone(self: Step, allocator: Allocator) !Step {
        return switch (self) {
            .run => |r| .{ .run = try r.clone(allocator) },
            .wait_for_signal => |w| .{ .wait_for_signal = try w.clone(allocator) },
        };
    }

    /// Get all transitions for this step
    pub fn transitions(self: Step) []const Transition {
        return switch (self) {
            .run => |r| r.transitions,
            .wait_for_signal => |w| w.transitions,
        };
    }

    /// Get transition for a given outcome
    pub fn getTransition(self: Step, outcome: []const u8) ?Transition {
        return switch (self) {
            .run => |r| r.getTransition(outcome),
            .wait_for_signal => |w| w.getTransition(outcome),
        };
    }
};

// =============================================================================
// Named Step (for storage in map)
// =============================================================================

/// Step with its name for map storage
pub const NamedStep = struct {
    name: []const u8,
    step: Step,

    pub fn deinit(self: *NamedStep, allocator: Allocator) void {
        allocator.free(self.name);
        self.step.deinit(allocator);
    }

    pub fn clone(self: NamedStep, allocator: Allocator) !NamedStep {
        return .{
            .name = try allocator.dupe(u8, self.name),
            .step = try self.step.clone(allocator),
        };
    }
};

// =============================================================================
// Schedule Definition (embedded cron/interval in workflow)
// =============================================================================

/// Schedule definition for recurring workflow execution.
/// Embedded in workflow YAML under the `schedule:` key.
///
/// YAML structure:
/// ```yaml
/// schedule:
///   cron: "0 */6 * * *"     # cron expression (mutually exclusive with interval)
///   interval: 30000          # interval in ms (mutually exclusive with cron)
///   max_concurrent: 1        # max concurrent runs (default 1)
///   input: '{"mode": "full"}' # per-schedule input override (optional)
/// ```
pub const ScheduleDef = struct {
    /// Cron expression (e.g., "*/5 * * * *")
    cron_expr: ?[]const u8 = null,
    /// Interval in milliseconds (simple periodic)
    interval_ms: ?i64 = null,
    /// Maximum concurrent runs from this schedule (default 1)
    max_concurrent: u32 = 1,
    /// Input JSON override for scheduled runs (null = use empty)
    input: ?[]const u8 = null,
    /// Whether the schedule starts paused
    paused: bool = false,

    pub fn deinit(self: *ScheduleDef, allocator: Allocator) void {
        if (self.cron_expr) |c| allocator.free(c);
        if (self.input) |i| allocator.free(i);
    }

    pub fn clone(self: ScheduleDef, allocator: Allocator) !ScheduleDef {
        return .{
            .cron_expr = if (self.cron_expr) |c| try allocator.dupe(u8, c) else null,
            .interval_ms = self.interval_ms,
            .max_concurrent = self.max_concurrent,
            .input = if (self.input) |i| try allocator.dupe(u8, i) else null,
            .paused = self.paused,
        };
    }

    /// Check if this schedule is valid (has exactly one of cron or interval)
    pub fn isValid(self: ScheduleDef) bool {
        const has_cron = self.cron_expr != null;
        const has_interval = self.interval_ms != null;
        return has_cron != has_interval; // exactly one must be set
    }
};

// =============================================================================
// Stream Trigger Definition (event-driven workflow activation)
// =============================================================================

/// Consumer group mode for stream triggers.
/// Mirrors stream/log.zig GroupMode but is self-contained so definition.zig
/// has no dependency on the stream module.
pub const TriggerMode = enum(u8) {
    /// Multiple consumers compete for messages (scale-out, no ordering)
    shared = 0,
    /// Single consumer with lease (strict ordering, failover)
    exclusive = 1,
    /// Messages with same partition key go to same consumer
    key_shared = 2,

    pub fn toString(self: TriggerMode) []const u8 {
        return switch (self) {
            .shared => "shared",
            .exclusive => "exclusive",
            .key_shared => "key_shared",
        };
    }

    pub fn fromString(s: []const u8) ?TriggerMode {
        const map = std.StaticStringMap(TriggerMode).initComptime(.{
            .{ "shared", .shared },
            .{ "exclusive", .exclusive },
            .{ "key_shared", .key_shared },
            .{ "key-shared", .key_shared },
        });
        return map.get(s);
    }
};

/// Stream trigger definition — starts a workflow run on each stream event.
/// Embedded in workflow YAML under the `trigger:` key.
///
/// YAML structure:
/// ```yaml
/// trigger:
///   stream: "orders"              # source stream name (required)
///   namespace: "prod"             # source namespace (optional, defaults to workflow ns)
///   consumer_group: "wf-orders"   # consumer group name (optional, auto-generated)
///   mode: shared                  # shared | exclusive | key_shared
///   batch_size: 1                 # events per workflow run (1 = one event, N = array)
/// ```
///
/// The event payload (or array of payloads when batch_size > 1) becomes `$.input`
/// inside the workflow and is accessible via JSONPath resolution.
pub const StreamTriggerDef = struct {
    /// Source stream name (required)
    stream: []const u8,
    /// Source namespace (null = inherit workflow's namespace)
    namespace: ?[]const u8 = null,
    /// Consumer group name (null = auto-generate "wf-{workflow_name}")
    consumer_group: ?[]const u8 = null,
    /// Consumer group mode
    mode: TriggerMode = .shared,
    /// Number of events per workflow run (1 = single event, >1 = array of events)
    batch_size: u32 = 1,
    /// Timeout in milliseconds to flush a partial batch (default 5s)
    batch_timeout_ms: u32 = 5000,

    pub fn deinit(self: *StreamTriggerDef, allocator: Allocator) void {
        allocator.free(self.stream);
        if (self.namespace) |ns| allocator.free(ns);
        if (self.consumer_group) |cg| allocator.free(cg);
    }

    pub fn clone(self: StreamTriggerDef, allocator: Allocator) !StreamTriggerDef {
        const stream_copy = try allocator.dupe(u8, self.stream);
        errdefer allocator.free(stream_copy);
        const ns_copy = if (self.namespace) |ns| try allocator.dupe(u8, ns) else null;
        errdefer if (ns_copy) |ns| allocator.free(ns);
        const cg_copy = if (self.consumer_group) |cg| try allocator.dupe(u8, cg) else null;
        return .{
            .stream = stream_copy,
            .namespace = ns_copy,
            .consumer_group = cg_copy,
            .mode = self.mode,
            .batch_size = self.batch_size,
            .batch_timeout_ms = self.batch_timeout_ms,
        };
    }

    /// Check if this trigger definition is valid
    pub fn isValid(self: StreamTriggerDef) bool {
        return self.stream.len > 0 and self.batch_size > 0;
    }

    /// Get the effective consumer group name (auto-generate if not set)
    pub fn effectiveConsumerGroup(self: StreamTriggerDef, allocator: Allocator, workflow_name: []const u8) ![]u8 {
        if (self.consumer_group) |cg| {
            return try allocator.dupe(u8, cg);
        }
        // Auto-generate: "wf-{workflow_name}"
        const prefix = "wf-";
        const result = try allocator.alloc(u8, prefix.len + workflow_name.len);
        @memcpy(result[0..prefix.len], prefix);
        @memcpy(result[prefix.len..], workflow_name);
        return result;
    }
};

// =============================================================================
// Inline Plan (local plan embedded in workflow)
// =============================================================================

/// Inline plan definition - embedded within a workflow's `plans:` section.
/// Plans don't have separate name/version fields as those come from the
/// map key in the workflow definition.
///
/// YAML structure:
/// ```yaml
/// plans:
///   payment:              # <- name comes from here
///     selection: health-weighted
///     executors:
///       - action: "@actions/stripe-charge"
///         priority: 1
/// ```
pub const InlinePlan = struct {
    /// Plan name (from the YAML map key)
    name: []const u8,
    /// Selection strategy for executors
    selection: SelectionStrategy,
    /// Error classification rules
    error_classification: ?ErrorClassification,
    /// Executor configurations (at least one required)
    executors: []const ExecutorConfig,
    /// Health tracking configuration
    health_config: ?HealthConfig,
    /// Cache configuration
    cache_config: ?CacheConfig,
    /// Fallback configuration
    fallback_config: ?FallbackConfig,

    pub fn deinit(self: *InlinePlan, allocator: Allocator) void {
        allocator.free(self.name);
        if (self.error_classification) |*ec| {
            var mec = ec.*;
            mec.deinit(allocator);
        }
        for (self.executors) |exec| {
            var e = exec;
            e.deinit(allocator);
        }
        if (self.executors.len > 0) allocator.free(@constCast(self.executors));
        if (self.cache_config) |*cc| {
            var mcc = cc.*;
            mcc.deinit(allocator);
        }
        if (self.fallback_config) |*fc| {
            var mfc = fc.*;
            mfc.deinit(allocator);
        }
    }

    pub fn clone(self: InlinePlan, allocator: Allocator) !InlinePlan {
        const executors = try allocator.alloc(ExecutorConfig, self.executors.len);
        errdefer allocator.free(executors);
        for (self.executors, 0..) |exec, i| {
            executors[i] = try exec.clone(allocator);
        }

        var error_classification: ?ErrorClassification = null;
        if (self.error_classification) |ec| {
            error_classification = try ec.clone(allocator);
        }

        var cache_config: ?CacheConfig = null;
        if (self.cache_config) |cc| {
            cache_config = try cc.clone(allocator);
        }

        var fallback_config: ?FallbackConfig = null;
        if (self.fallback_config) |fc| {
            fallback_config = try fc.clone(allocator);
        }

        return .{
            .name = try allocator.dupe(u8, self.name),
            .selection = self.selection,
            .error_classification = error_classification,
            .executors = executors,
            .health_config = self.health_config,
            .cache_config = cache_config,
            .fallback_config = fallback_config,
        };
    }

    /// Encode to wire format
    pub fn encode(self: InlinePlan, buf: *std.ArrayList(u8), allocator: Allocator) !void {
        try writeString(buf, allocator, self.name);
        try buf.append(allocator, @intFromEnum(self.selection));

        // Error classification
        if (self.error_classification) |ec| {
            try buf.append(allocator, 1);
            try ec.encode(buf, allocator);
        } else {
            try buf.append(allocator, 0);
        }

        // Executors
        try writeU16(buf, allocator, @intCast(self.executors.len));
        for (self.executors) |exec| {
            try exec.encode(buf, allocator);
        }

        // Health config
        if (self.health_config) |hc| {
            try buf.append(allocator, 1);
            try hc.encode(buf, allocator);
        } else {
            try buf.append(allocator, 0);
        }

        // Cache config
        if (self.cache_config) |cc| {
            try buf.append(allocator, 1);
            try cc.encode(buf, allocator);
        } else {
            try buf.append(allocator, 0);
        }

        // Fallback config
        if (self.fallback_config) |fc| {
            try buf.append(allocator, 1);
            try fc.encode(buf, allocator);
        } else {
            try buf.append(allocator, 0);
        }
    }

    /// Decode from wire format
    pub fn decode(allocator: Allocator, data: []const u8, pos: *usize) !InlinePlan {
        const name = try readString(allocator, data, pos);
        errdefer allocator.free(name);

        if (pos.* >= data.len) return error.InvalidData;
        const selection: SelectionStrategy = @enumFromInt(data[pos.*]);
        pos.* += 1;

        // Error classification
        if (pos.* >= data.len) return error.InvalidData;
        const has_ec = data[pos.*] == 1;
        pos.* += 1;
        var error_classification: ?ErrorClassification = null;
        if (has_ec) {
            error_classification = try ErrorClassification.decode(allocator, data, pos);
        }

        // Executors
        const exec_count = try readU16(data, pos);
        const executors = try allocator.alloc(ExecutorConfig, exec_count);
        errdefer {
            for (executors) |*exec| exec.deinit(allocator);
            allocator.free(executors);
        }
        for (executors, 0..) |*exec, i| {
            _ = i;
            exec.* = try ExecutorConfig.decode(allocator, data, pos);
        }

        // Health config
        if (pos.* >= data.len) return error.InvalidData;
        const has_hc = data[pos.*] == 1;
        pos.* += 1;
        const health_config: ?HealthConfig = if (has_hc) try HealthConfig.decode(data, pos) else null;

        // Cache config
        if (pos.* >= data.len) return error.InvalidData;
        const has_cc = data[pos.*] == 1;
        pos.* += 1;
        var cache_config: ?CacheConfig = null;
        if (has_cc) {
            cache_config = try CacheConfig.decode(allocator, data, pos);
        }

        // Fallback config
        if (pos.* >= data.len) return error.InvalidData;
        const has_fc = data[pos.*] == 1;
        pos.* += 1;
        var fallback_config: ?FallbackConfig = null;
        if (has_fc) {
            fallback_config = try FallbackConfig.decode(allocator, data, pos);
        }

        return .{
            .name = name,
            .selection = selection,
            .error_classification = error_classification,
            .executors = executors,
            .health_config = health_config,
            .cache_config = cache_config,
            .fallback_config = fallback_config,
        };
    }
};

// =============================================================================
// Workflow Definition
// =============================================================================

/// Complete workflow definition
/// Stored at: _wf:def:{namespace}:{workflow}:{version}
pub const WorkflowDefinition = struct {
    /// Workflow name
    name: []const u8,
    /// Optional human-readable description (empty string if not provided)
    description: []const u8,
    /// Workflow version (semantic versioning)
    version: []const u8,
    /// Idempotency mode
    idempotency: IdempotencyMode,
    /// Search attribute definitions for queryable fields
    search_attributes: []const SearchAttrDef,
    /// Inline plans (local to this workflow)
    plans: []const InlinePlan,
    /// Start step (always runs first)
    start: Step,
    /// Named steps (map name -> step)
    steps: []const NamedStep,
    /// Custom terminals (built-in don't need declaration)
    terminals: []const Terminal,
    /// Optional schedule (cron/interval) — makes this a recurring workflow
    schedule: ?ScheduleDef = null,
    /// Optional stream trigger — starts a run for each stream event
    trigger: ?StreamTriggerDef = null,

    const WIRE_VERSION: u8 = 1;

    pub fn deinit(self: *WorkflowDefinition, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.description);
        allocator.free(self.version);

        for (self.search_attributes) |attr| {
            var ma = attr;
            ma.deinit(allocator);
        }
        if (self.search_attributes.len > 0) allocator.free(@constCast(self.search_attributes));

        // Free inline plans
        for (self.plans) |plan| {
            var mp = plan;
            mp.deinit(allocator);
        }
        if (self.plans.len > 0) allocator.free(@constCast(self.plans));

        self.start.deinit(allocator);

        for (self.steps) |step| {
            var ms = step;
            ms.deinit(allocator);
        }
        if (self.steps.len > 0) allocator.free(@constCast(self.steps));

        for (self.terminals) |t| {
            var mt = t;
            mt.deinit(allocator);
        }
        if (self.terminals.len > 0) allocator.free(@constCast(self.terminals));

        if (self.schedule) |*s| {
            var ms = s.*;
            ms.deinit(allocator);
        }

        if (self.trigger) |*t| {
            var mt = t.*;
            mt.deinit(allocator);
        }
    }

    pub fn clone(self: WorkflowDefinition, allocator: Allocator) !WorkflowDefinition {
        const search_attributes = try allocator.alloc(SearchAttrDef, self.search_attributes.len);
        for (self.search_attributes, 0..) |attr, i| {
            search_attributes[i] = try attr.clone(allocator);
        }

        // Clone inline plans
        const plans = try allocator.alloc(InlinePlan, self.plans.len);
        for (self.plans, 0..) |plan, i| {
            plans[i] = try plan.clone(allocator);
        }

        const steps = try allocator.alloc(NamedStep, self.steps.len);
        for (self.steps, 0..) |step, i| {
            steps[i] = try step.clone(allocator);
        }

        const terminals = try allocator.alloc(Terminal, self.terminals.len);
        for (self.terminals, 0..) |t, i| {
            terminals[i] = try t.clone(allocator);
        }

        return .{
            .name = try allocator.dupe(u8, self.name),
            .description = try allocator.dupe(u8, self.description),
            .version = try allocator.dupe(u8, self.version),
            .idempotency = self.idempotency,
            .search_attributes = search_attributes,
            .plans = plans,
            .start = try self.start.clone(allocator),
            .steps = steps,
            .terminals = terminals,
            .schedule = if (self.schedule) |s| try s.clone(allocator) else null,
            .trigger = if (self.trigger) |t| try t.clone(allocator) else null,
        };
    }

    /// Get an inline plan by name
    pub fn getPlan(self: WorkflowDefinition, name: []const u8) ?InlinePlan {
        for (self.plans) |plan| {
            if (mem.eql(u8, plan.name, name)) return plan;
        }
        return null;
    }

    /// Check if a plan name exists
    pub fn hasPlan(self: WorkflowDefinition, name: []const u8) bool {
        return self.getPlan(name) != null;
    }

    /// Get a step by name
    pub fn getStep(self: WorkflowDefinition, name: []const u8) ?Step {
        for (self.steps) |step| {
            if (mem.eql(u8, step.name, name)) return step.step;
        }
        return null;
    }

    /// Get terminal by name (including built-ins)
    pub fn getTerminal(self: WorkflowDefinition, name: []const u8) ?Terminal {
        // Check built-in first
        if (BuiltinTerminal.statusFor(name)) |status| {
            return .{ .name = name, .status = status };
        }
        // Check custom terminals
        for (self.terminals) |t| {
            if (mem.eql(u8, t.name, name)) return t;
        }
        return null;
    }

    /// Check if a target is a valid terminal
    pub fn isTerminal(self: WorkflowDefinition, name: []const u8) bool {
        return self.getTerminal(name) != null;
    }

    /// Check if a target is a valid step
    pub fn isStep(self: WorkflowDefinition, name: []const u8) bool {
        return self.getStep(name) != null;
    }

    /// Validate the workflow definition
    pub fn validate(self: WorkflowDefinition) !void {
        // All transition targets must be valid (step or terminal)
        try self.validateTransitions(self.start.transitions());

        for (self.steps) |step| {
            try self.validateTransitions(step.step.transitions());
        }

        // Validate all @plan/* references point to existing plans
        try self.validatePlanReferences(self.start);
        for (self.steps) |step| {
            try self.validatePlanReferences(step.step);
        }
    }

    fn validateTransitions(self: WorkflowDefinition, trans: []const Transition) !void {
        for (trans) |t| {
            if (!self.isTerminal(t.target) and !self.isStep(t.target)) {
                return error.InvalidTransitionTarget;
            }
        }
    }

    fn validatePlanReferences(self: WorkflowDefinition, step: Step) !void {
        switch (step) {
            .run => |r| {
                if (r.isPlan()) {
                    const plan_name = r.targetName();
                    if (!self.hasPlan(plan_name)) {
                        return error.InvalidPlanReference;
                    }
                }
            },
            .wait_for_signal => {},
        }
    }

    /// Serialize to wire format
    pub fn encode(self: WorkflowDefinition, allocator: Allocator) ![]u8 {
        var buf = std.ArrayList(u8){};
        errdefer buf.deinit(allocator);

        try buf.append(allocator, WIRE_VERSION);
        try writeString(&buf, allocator, self.name);
        try writeString(&buf, allocator, self.description);
        try writeString(&buf, allocator, self.version);
        try buf.append(allocator, @intFromEnum(self.idempotency));

        // Search attributes
        try writeU16(&buf, allocator, @intCast(self.search_attributes.len));
        for (self.search_attributes) |attr| {
            try writeString(&buf, allocator, attr.name);
            try buf.append(allocator, @intFromEnum(attr.attr_type));
            try writeString(&buf, allocator, attr.from);
        }

        // Inline plans
        try writeU16(&buf, allocator, @intCast(self.plans.len));
        for (self.plans) |plan| {
            try plan.encode(&buf, allocator);
        }

        // Start step
        try encodeStep(&buf, allocator, self.start);

        // Named steps
        try writeU16(&buf, allocator, @intCast(self.steps.len));
        for (self.steps) |step| {
            try writeString(&buf, allocator, step.name);
            try encodeStep(&buf, allocator, step.step);
        }

        // Terminals
        try writeU16(&buf, allocator, @intCast(self.terminals.len));
        for (self.terminals) |t| {
            try writeString(&buf, allocator, t.name);
            try buf.append(allocator, @intFromEnum(t.status));
        }

        // Schedule (optional)
        if (self.schedule) |sched| {
            try buf.append(allocator, 1); // has schedule
            try buf.append(allocator, if (sched.cron_expr != null) @as(u8, 1) else @as(u8, 0));
            if (sched.cron_expr) |c| try writeString(&buf, allocator, c);
            const has_interval: u8 = if (sched.interval_ms != null) 1 else 0;
            try buf.append(allocator, has_interval);
            if (sched.interval_ms) |iv| {
                var iv_bytes: [8]u8 = undefined;
                mem.writeInt(i64, &iv_bytes, iv, .big);
                try buf.appendSlice(allocator, &iv_bytes);
            }
            var mc_bytes: [4]u8 = undefined;
            mem.writeInt(u32, &mc_bytes, sched.max_concurrent, .big);
            try buf.appendSlice(allocator, &mc_bytes);
            try buf.append(allocator, if (sched.input != null) @as(u8, 1) else @as(u8, 0));
            if (sched.input) |inp| try writeString(&buf, allocator, inp);
            try buf.append(allocator, if (sched.paused) @as(u8, 1) else @as(u8, 0));
        } else {
            try buf.append(allocator, 0); // no schedule
        }

        // Stream trigger (optional)
        if (self.trigger) |trig| {
            try buf.append(allocator, 1); // has trigger
            try writeString(&buf, allocator, trig.stream);
            try buf.append(allocator, if (trig.namespace != null) @as(u8, 1) else @as(u8, 0));
            if (trig.namespace) |ns| try writeString(&buf, allocator, ns);
            try buf.append(allocator, if (trig.consumer_group != null) @as(u8, 1) else @as(u8, 0));
            if (trig.consumer_group) |cg| try writeString(&buf, allocator, cg);
            try buf.append(allocator, @intFromEnum(trig.mode));
            var bs_bytes: [4]u8 = undefined;
            mem.writeInt(u32, &bs_bytes, trig.batch_size, .big);
            try buf.appendSlice(allocator, &bs_bytes);
            var bt_bytes: [4]u8 = undefined;
            mem.writeInt(u32, &bt_bytes, trig.batch_timeout_ms, .big);
            try buf.appendSlice(allocator, &bt_bytes);
        } else {
            try buf.append(allocator, 0); // no trigger
        }

        return buf.toOwnedSlice(allocator);
    }

    /// Deserialize from wire format
    pub fn decode(allocator: Allocator, data: []const u8) !WorkflowDefinition {
        if (data.len < 1) return error.InvalidData;
        var pos: usize = 0;

        const version = data[pos];
        pos += 1;
        if (version != WIRE_VERSION) return error.UnsupportedVersion;

        const name = try readString(allocator, data, &pos);
        errdefer allocator.free(name);

        const desc = try readString(allocator, data, &pos);
        errdefer allocator.free(desc);

        const def_version = try readString(allocator, data, &pos);
        errdefer allocator.free(def_version);

        if (pos >= data.len) return error.InvalidData;
        const idempotency: IdempotencyMode = @enumFromInt(data[pos]);
        pos += 1;

        // Search attributes
        const attr_count = try readU16(data, &pos);
        const search_attributes = try allocator.alloc(SearchAttrDef, attr_count);
        errdefer {
            for (search_attributes) |*attr| attr.deinit(allocator);
            allocator.free(search_attributes);
        }
        for (search_attributes, 0..) |*attr, i| {
            _ = i;
            const attr_name = try readString(allocator, data, &pos);
            if (pos >= data.len) return error.InvalidData;
            const attr_type: SearchAttrType = @enumFromInt(data[pos]);
            pos += 1;
            const from = try readString(allocator, data, &pos);
            attr.* = .{
                .name = attr_name,
                .attr_type = attr_type,
                .from = from,
            };
        }

        // Inline plans
        const plan_count = try readU16(data, &pos);
        const plans = try allocator.alloc(InlinePlan, plan_count);
        errdefer {
            for (plans) |*p| p.deinit(allocator);
            allocator.free(plans);
        }
        for (plans, 0..) |*plan, i| {
            _ = i;
            plan.* = try InlinePlan.decode(allocator, data, &pos);
        }

        // Start step
        const start = try decodeStep(allocator, data, &pos);
        errdefer {
            var s = start;
            s.deinit(allocator);
        }

        // Named steps
        const step_count = try readU16(data, &pos);
        const steps = try allocator.alloc(NamedStep, step_count);
        errdefer {
            for (steps) |*step| step.deinit(allocator);
            allocator.free(steps);
        }
        for (steps, 0..) |*step, i| {
            _ = i;
            const step_name = try readString(allocator, data, &pos);
            const step_def = try decodeStep(allocator, data, &pos);
            step.* = .{
                .name = step_name,
                .step = step_def,
            };
        }

        // Terminals
        const terminal_count = try readU16(data, &pos);
        const terminals = try allocator.alloc(Terminal, terminal_count);
        errdefer {
            for (terminals) |*t| t.deinit(allocator);
            allocator.free(terminals);
        }
        for (terminals, 0..) |*t, i| {
            _ = i;
            const t_name = try readString(allocator, data, &pos);
            if (pos >= data.len) return error.InvalidData;
            const status: RunStatus = @enumFromInt(data[pos]);
            pos += 1;
            t.* = .{
                .name = t_name,
                .status = status,
            };
        }

        // Schedule (optional)
        var schedule: ?ScheduleDef = null;
        if (pos < data.len) {
            const has_schedule = data[pos];
            pos += 1;
            if (has_schedule == 1) {
                var sched = ScheduleDef{};
                // cron_expr
                if (pos >= data.len) return error.InvalidData;
                const has_cron = data[pos];
                pos += 1;
                if (has_cron == 1) {
                    sched.cron_expr = try readString(allocator, data, &pos);
                }
                // interval_ms
                if (pos >= data.len) return error.InvalidData;
                const has_interval = data[pos];
                pos += 1;
                if (has_interval == 1) {
                    if (pos + 8 > data.len) return error.InvalidData;
                    sched.interval_ms = mem.readInt(i64, data[pos..][0..8], .big);
                    pos += 8;
                }
                // max_concurrent
                if (pos + 4 > data.len) return error.InvalidData;
                sched.max_concurrent = mem.readInt(u32, data[pos..][0..4], .big);
                pos += 4;
                // input
                if (pos >= data.len) return error.InvalidData;
                const has_input = data[pos];
                pos += 1;
                if (has_input == 1) {
                    sched.input = try readString(allocator, data, &pos);
                }
                // paused
                if (pos >= data.len) return error.InvalidData;
                sched.paused = data[pos] == 1;
                pos += 1;
                schedule = sched;
            }
        }

        // Stream trigger (optional)
        var trigger: ?StreamTriggerDef = null;
        errdefer if (trigger) |*t| {
            var mt = t.*;
            mt.deinit(allocator);
        };
        if (pos < data.len) {
            const has_trigger = data[pos];
            pos += 1;
            if (has_trigger == 1) {
                var trig = StreamTriggerDef{ .stream = undefined };
                trig.stream = try readString(allocator, data, &pos);
                // namespace
                if (pos >= data.len) return error.InvalidData;
                const has_ns = data[pos];
                pos += 1;
                if (has_ns == 1) {
                    trig.namespace = try readString(allocator, data, &pos);
                }
                // consumer_group
                if (pos >= data.len) return error.InvalidData;
                const has_cg = data[pos];
                pos += 1;
                if (has_cg == 1) {
                    trig.consumer_group = try readString(allocator, data, &pos);
                }
                // mode
                if (pos >= data.len) return error.InvalidData;
                trig.mode = @enumFromInt(data[pos]);
                pos += 1;
                // batch_size
                if (pos + 4 > data.len) return error.InvalidData;
                trig.batch_size = mem.readInt(u32, data[pos..][0..4], .big);
                pos += 4;
                // batch_timeout_ms
                if (pos + 4 > data.len) return error.InvalidData;
                trig.batch_timeout_ms = mem.readInt(u32, data[pos..][0..4], .big);
                pos += 4;
                trigger = trig;
            }
        }

        return .{
            .name = name,
            .description = desc,
            .version = def_version,
            .idempotency = idempotency,
            .search_attributes = search_attributes,
            .plans = plans,
            .start = start,
            .steps = steps,
            .terminals = terminals,
            .schedule = schedule,
            .trigger = trigger,
        };
    }
};

// =============================================================================
// Wire Format Helpers
// =============================================================================

fn writeString(buf: *std.ArrayList(u8), allocator: Allocator, s: []const u8) !void {
    const len: u16 = @intCast(@min(s.len, std.math.maxInt(u16)));
    var len_bytes: [2]u8 = undefined;
    mem.writeInt(u16, &len_bytes, len, .big);
    try buf.appendSlice(allocator, &len_bytes);
    try buf.appendSlice(allocator, s[0..len]);
}

fn readString(allocator: Allocator, data: []const u8, pos: *usize) ![]u8 {
    if (pos.* + 2 > data.len) return error.InvalidData;
    const len = mem.readInt(u16, data[pos.*..][0..2], .big);
    pos.* += 2;
    if (pos.* + len > data.len) return error.InvalidData;
    const result = try allocator.dupe(u8, data[pos.* .. pos.* + len]);
    pos.* += len;
    return result;
}

fn writeU16(buf: *std.ArrayList(u8), allocator: Allocator, v: u16) !void {
    var bytes: [2]u8 = undefined;
    mem.writeInt(u16, &bytes, v, .big);
    try buf.appendSlice(allocator, &bytes);
}

fn readU16(data: []const u8, pos: *usize) !u16 {
    if (pos.* + 2 > data.len) return error.InvalidData;
    const result = mem.readInt(u16, data[pos.*..][0..2], .big);
    pos.* += 2;
    return result;
}

fn writeI64(buf: *std.ArrayList(u8), allocator: Allocator, v: i64) !void {
    var bytes: [8]u8 = undefined;
    mem.writeInt(i64, &bytes, v, .big);
    try buf.appendSlice(allocator, &bytes);
}

fn readI64(data: []const u8, pos: *usize) !i64 {
    if (pos.* + 8 > data.len) return error.InvalidData;
    const result = mem.readInt(i64, data[pos.*..][0..8], .big);
    pos.* += 8;
    return result;
}

fn writeU32(buf: *std.ArrayList(u8), allocator: Allocator, v: u32) !void {
    var bytes: [4]u8 = undefined;
    mem.writeInt(u32, &bytes, v, .big);
    try buf.appendSlice(allocator, &bytes);
}

fn readU32(data: []const u8, pos: *usize) !u32 {
    if (pos.* + 4 > data.len) return error.InvalidData;
    const result = mem.readInt(u32, data[pos.*..][0..4], .big);
    pos.* += 4;
    return result;
}

fn encodeStep(buf: *std.ArrayList(u8), allocator: Allocator, step: Step) !void {
    switch (step) {
        .run => |r| {
            try buf.append(allocator, 0); // Tag for run
            try writeString(buf, allocator, r.target);
            if (r.input_mapping) |m| {
                try buf.append(allocator, 1);
                try writeString(buf, allocator, m);
            } else {
                try buf.append(allocator, 0);
            }
            if (r.retry) |retry| {
                try buf.append(allocator, 1);
                try retry.encode(buf, allocator);
            } else {
                try buf.append(allocator, 0);
            }
            if (r.poll) |poll| {
                try buf.append(allocator, 1);
                try poll.encode(buf, allocator);
            } else {
                try buf.append(allocator, 0);
            }
            try encodeTransitions(buf, allocator, r.transitions);
        },
        .wait_for_signal => |w| {
            try buf.append(allocator, 1); // Tag for wait_for_signal
            try writeString(buf, allocator, w.signal_type);
            if (w.timeout_ms) |t| {
                try buf.append(allocator, 1);
                try writeI64(buf, allocator, t);
            } else {
                try buf.append(allocator, 0);
            }
            if (w.on_timeout) |t| {
                try buf.append(allocator, 1);
                try writeString(buf, allocator, t);
            } else {
                try buf.append(allocator, 0);
            }
            try encodeTransitions(buf, allocator, w.transitions);
        },
    }
}

fn decodeStep(allocator: Allocator, data: []const u8, pos: *usize) !Step {
    if (pos.* >= data.len) return error.InvalidData;
    const tag = data[pos.*];
    pos.* += 1;

    if (tag == 0) {
        // Run step
        const target = try readString(allocator, data, pos);
        errdefer allocator.free(target);

        if (pos.* >= data.len) return error.InvalidData;
        const has_mapping = data[pos.*] == 1;
        pos.* += 1;
        const input_mapping: ?[]u8 = if (has_mapping) try readString(allocator, data, pos) else null;
        errdefer if (input_mapping) |m| allocator.free(m);

        if (pos.* >= data.len) return error.InvalidData;
        const has_retry = data[pos.*] == 1;
        pos.* += 1;
        const retry: ?RetryPolicy = if (has_retry) try RetryPolicy.decode(data, pos) else null;

        if (pos.* >= data.len) return error.InvalidData;
        const has_poll = data[pos.*] == 1;
        pos.* += 1;
        const poll: ?PollConfig = if (has_poll) try PollConfig.decode(data, pos) else null;

        const transitions = try decodeTransitions(allocator, data, pos);

        return .{
            .run = .{
                .target = target,
                .input_mapping = input_mapping,
                .retry = retry,
                .poll = poll,
                .transitions = transitions,
            },
        };
    } else {
        // Wait for signal step
        const signal_type = try readString(allocator, data, pos);
        errdefer allocator.free(signal_type);

        if (pos.* >= data.len) return error.InvalidData;
        const has_timeout = data[pos.*] == 1;
        pos.* += 1;
        const timeout_ms: ?i64 = if (has_timeout) try readI64(data, pos) else null;

        if (pos.* >= data.len) return error.InvalidData;
        const has_on_timeout = data[pos.*] == 1;
        pos.* += 1;
        const on_timeout: ?[]u8 = if (has_on_timeout) try readString(allocator, data, pos) else null;
        errdefer if (on_timeout) |t| allocator.free(t);

        const transitions = try decodeTransitions(allocator, data, pos);

        return .{
            .wait_for_signal = .{
                .signal_type = signal_type,
                .timeout_ms = timeout_ms,
                .on_timeout = on_timeout,
                .transitions = transitions,
            },
        };
    }
}

fn encodeTransitions(buf: *std.ArrayList(u8), allocator: Allocator, transitions: []const Transition) !void {
    try writeU16(buf, allocator, @intCast(transitions.len));
    for (transitions) |t| {
        try writeString(buf, allocator, t.outcome);
        try writeString(buf, allocator, t.target);
    }
}

fn decodeTransitions(allocator: Allocator, data: []const u8, pos: *usize) ![]Transition {
    const count = try readU16(data, pos);
    const transitions = try allocator.alloc(Transition, count);
    errdefer {
        for (transitions) |*t| t.deinit(allocator);
        allocator.free(transitions);
    }
    for (transitions, 0..) |*t, i| {
        _ = i;
        const outcome = try readString(allocator, data, pos);
        const target = try readString(allocator, data, pos);
        t.* = .{
            .outcome = outcome,
            .target = target,
        };
    }
    return transitions;
}

// =============================================================================
// Tests
// =============================================================================

test "BuiltinTerminal: statusFor" {
    const testing = std.testing;

    try testing.expectEqual(RunStatus.completed, BuiltinTerminal.statusFor("flo.Completed").?);
    try testing.expectEqual(RunStatus.failed, BuiltinTerminal.statusFor("flo.Failed").?);
    try testing.expectEqual(RunStatus.cancelled, BuiltinTerminal.statusFor("flo.Cancelled").?);
    try testing.expectEqual(RunStatus.timed_out, BuiltinTerminal.statusFor("flo.TimedOut").?);
    try testing.expectEqual(@as(?RunStatus, null), BuiltinTerminal.statusFor("CustomTerminal"));
}

test "RunStep: target type detection" {
    const step = RunStep{
        .target = "@actions/charge-stripe",
        .input_mapping = null,
        .retry = null,
        .poll = null,
        .transitions = &[_]Transition{},
    };

    const testing = std.testing;
    try testing.expect(step.isAction());
    try testing.expect(!step.isPlan());
    try testing.expect(!step.isChildWorkflow());
    try testing.expectEqualStrings("charge-stripe", step.targetName());

    const plan_step = RunStep{
        .target = "@plan/payment-processing",
        .input_mapping = null,
        .retry = null,
        .poll = null,
        .transitions = &[_]Transition{},
    };
    try testing.expect(!plan_step.isAction());
    try testing.expect(plan_step.isPlan());
    try testing.expectEqualStrings("payment-processing", plan_step.targetName());
}

test "WorkflowDefinition: encode and decode roundtrip" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Create transitions
    var transitions = try allocator.alloc(Transition, 2);
    transitions[0] = .{
        .outcome = try allocator.dupe(u8, "success"),
        .target = try allocator.dupe(u8, "charge_payment"),
    };
    transitions[1] = .{
        .outcome = try allocator.dupe(u8, "failure"),
        .target = try allocator.dupe(u8, "flo.Failed"),
    };

    // Create step transitions
    var step_transitions = try allocator.alloc(Transition, 2);
    step_transitions[0] = .{
        .outcome = try allocator.dupe(u8, "success"),
        .target = try allocator.dupe(u8, "flo.Completed"),
    };
    step_transitions[1] = .{
        .outcome = try allocator.dupe(u8, "failure"),
        .target = try allocator.dupe(u8, "PaymentFailed"),
    };

    // Create steps
    var steps = try allocator.alloc(NamedStep, 1);
    steps[0] = .{
        .name = try allocator.dupe(u8, "charge_payment"),
        .step = .{
            .run = .{
                .target = try allocator.dupe(u8, "@plan/payment-processing"),
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = step_transitions,
            },
        },
    };

    // Create terminals
    var terminals = try allocator.alloc(Terminal, 1);
    terminals[0] = .{
        .name = try allocator.dupe(u8, "PaymentFailed"),
        .status = .failed,
    };

    // Create search attributes
    var search_attrs = try allocator.alloc(SearchAttrDef, 1);
    search_attrs[0] = .{
        .name = try allocator.dupe(u8, "customer_id"),
        .attr_type = .string,
        .from = try allocator.dupe(u8, "input.customer_id"),
    };

    var def = WorkflowDefinition{
        .name = try allocator.dupe(u8, "process-order"),
        .description = "",
        .version = try allocator.dupe(u8, "1.0.0"),
        .idempotency = .required,
        .search_attributes = search_attrs,
        .plans = &.{},
        .start = .{
            .run = .{
                .target = try allocator.dupe(u8, "@actions/validate-order"),
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = transitions,
            },
        },
        .steps = steps,
        .terminals = terminals,
    };
    defer def.deinit(allocator);

    // Encode
    const encoded = try def.encode(allocator);
    defer allocator.free(encoded);

    // Decode
    var decoded = try WorkflowDefinition.decode(allocator, encoded);
    defer decoded.deinit(allocator);

    // Verify
    try testing.expectEqualStrings("process-order", decoded.name);
    try testing.expectEqualStrings("1.0.0", decoded.version);
    try testing.expectEqual(IdempotencyMode.required, decoded.idempotency);

    // Check search attributes
    try testing.expectEqual(@as(usize, 1), decoded.search_attributes.len);
    try testing.expectEqualStrings("customer_id", decoded.search_attributes[0].name);

    // Check start step
    try testing.expectEqualStrings("@actions/validate-order", decoded.start.run.target);
    try testing.expectEqual(@as(usize, 2), decoded.start.run.transitions.len);

    // Check steps
    try testing.expectEqual(@as(usize, 1), decoded.steps.len);
    try testing.expectEqualStrings("charge_payment", decoded.steps[0].name);

    // Check terminals
    try testing.expectEqual(@as(usize, 1), decoded.terminals.len);
    try testing.expectEqualStrings("PaymentFailed", decoded.terminals[0].name);

    // Check getStep and getTerminal
    try testing.expect(decoded.getStep("charge_payment") != null);
    try testing.expect(decoded.getStep("nonexistent") == null);
    try testing.expect(decoded.getTerminal("flo.Completed") != null);
    try testing.expect(decoded.getTerminal("PaymentFailed") != null);
}

test "RunStep: resolveTransition fallback chain" {
    const testing = std.testing;

    // Step with: success → step_b, execution_failure → error_handler, failure → flo.Failed
    const transitions = [_]Transition{
        .{ .outcome = "success", .target = "step_b" },
        .{ .outcome = StepOutcome.execution_failure, .target = "error_handler" },
        .{ .outcome = StepOutcome.failure, .target = BuiltinTerminal.Failed },
    };

    const step = RunStep{
        .target = "@actions/do-thing",
        .input_mapping = null,
        .retry = null,
        .poll = null,
        .transitions = &transitions,
    };

    // Business outcome: exact match only
    try testing.expectEqualStrings("step_b", step.resolveTransition("success").?.target);

    // Business outcome: "failure" matches exactly
    try testing.expectEqualStrings(BuiltinTerminal.Failed, step.resolveTransition("failure").?.target);

    // Execution outcome: target_not_found → no exact match → falls to execution_failure
    try testing.expectEqualStrings("error_handler", step.resolveTransition(StepOutcome.target_not_found).?.target);

    // Execution outcome: target_disabled → no exact match → falls to execution_failure
    try testing.expectEqualStrings("error_handler", step.resolveTransition(StepOutcome.target_disabled).?.target);

    // Execution outcome: execution_failure → exact match
    try testing.expectEqualStrings("error_handler", step.resolveTransition(StepOutcome.execution_failure).?.target);

    // Unknown business outcome: no match, no fallback
    try testing.expect(step.resolveTransition("custom_outcome") == null);
}

test "RunStep: resolveTransition falls through to failure when no execution_failure" {
    const testing = std.testing;

    // Step with only: success → step_b, failure → flo.Failed
    const transitions = [_]Transition{
        .{ .outcome = "success", .target = "step_b" },
        .{ .outcome = StepOutcome.failure, .target = BuiltinTerminal.Failed },
    };

    const step = RunStep{
        .target = "@actions/do-thing",
        .input_mapping = null,
        .retry = null,
        .poll = null,
        .transitions = &transitions,
    };

    // target_not_found → no exact → no execution_failure → falls to failure
    try testing.expectEqualStrings(BuiltinTerminal.Failed, step.resolveTransition(StepOutcome.target_not_found).?.target);

    // target_disabled → same fallback to failure
    try testing.expectEqualStrings(BuiltinTerminal.Failed, step.resolveTransition(StepOutcome.target_disabled).?.target);
}

test "RunStep: resolveTransition with specific target_not_found handler" {
    const testing = std.testing;

    // Step that explicitly handles target_not_found but not target_disabled
    const transitions = [_]Transition{
        .{ .outcome = "success", .target = "step_b" },
        .{ .outcome = StepOutcome.target_not_found, .target = "provision_target" },
        .{ .outcome = StepOutcome.failure, .target = BuiltinTerminal.Failed },
    };

    const step = RunStep{
        .target = "@actions/do-thing",
        .input_mapping = null,
        .retry = null,
        .poll = null,
        .transitions = &transitions,
    };

    // target_not_found: exact match
    try testing.expectEqualStrings("provision_target", step.resolveTransition(StepOutcome.target_not_found).?.target);

    // target_disabled: no exact → no execution_failure → falls to failure
    try testing.expectEqualStrings(BuiltinTerminal.Failed, step.resolveTransition(StepOutcome.target_disabled).?.target);
}
