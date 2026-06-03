//! Workflow YAML/JSON Parser
//!
//! Parses workflow and plan definitions from YAML or JSON format.
//!
//! # Supported Formats
//!
//! - YAML (primary, converted to JSON internally)
//! - JSON (native via std.json)
//!
//! # Usage
//!
//! ```zig
//! const parser = @import("workflow/parser.zig");
//!
//! // Parse workflow definition
//! var def = try parser.parseWorkflow(allocator, yaml_content);
//! defer def.deinit(allocator);
//!
//! // Parse plan definition
//! var plan = try parser.parsePlan(allocator, yaml_content);
//! defer plan.deinit(allocator);
//! ```

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

const definition = @import("definition.zig");
const plan_types = @import("plan_types.zig");
const types = @import("types.zig");
const yaml_to_json = @import("../util/yaml_to_json.zig");

// Re-export types for convenience
pub const WorkflowDefinition = definition.WorkflowDefinition;
pub const Step = definition.Step;
pub const RunStep = definition.RunStep;
pub const WaitForSignalStep = definition.WaitForSignalStep;
pub const Transition = definition.Transition;
pub const Terminal = definition.Terminal;
pub const NamedStep = definition.NamedStep;
pub const IdempotencyMode = definition.IdempotencyMode;
pub const SearchAttrDef = definition.SearchAttrDef;
pub const SearchAttrType = definition.SearchAttrType;
pub const InlinePlan = definition.InlinePlan;
pub const ScheduleDef = definition.ScheduleDef;
pub const StreamTriggerDef = definition.StreamTriggerDef;
pub const TriggerMode = definition.TriggerMode;
pub const RetryPolicy = plan_types.RetryPolicy;
pub const BackoffType = plan_types.BackoffType;
pub const ExecutorConfig = plan_types.ExecutorConfig;
pub const CircuitBreakerConfig = plan_types.CircuitBreakerConfig;
pub const TrackingConfig = plan_types.TrackingConfig;
pub const TrackingMode = plan_types.TrackingMode;
pub const RateLimitConfig = plan_types.RateLimitConfig;
pub const CacheConfig = plan_types.CacheConfig;
pub const FallbackConfig = plan_types.FallbackConfig;
pub const FallbackCondition = plan_types.FallbackCondition;
pub const HealthConfig = plan_types.HealthConfig;
pub const SelectionStrategy = plan_types.SelectionStrategy;
pub const ErrorClassification = plan_types.ErrorClassification;

// =============================================================================
// Parse Errors
// =============================================================================

pub const ParseError = error{
    InvalidFormat,
    MissingRequiredField,
    InvalidFieldType,
    InvalidKind,
    InvalidIdempotencyMode,
    InvalidSelectionStrategy,
    InvalidBackoffType,
    InvalidSearchAttrType,
    InvalidFallbackCondition,
    InvalidTrackingMode,
    DuplicateStepName,
    DuplicateExecutorName,
    DuplicatePlanName,
    EmptyExecutors,
    OutOfMemory,
};

// =============================================================================
// JSON Value Helpers
// =============================================================================

const JsonValue = std.json.Value;

fn getString(obj: JsonValue, key: []const u8) ?[]const u8 {
    if (obj != .object) return null;
    const val = obj.object.get(key) orelse return null;
    return if (val == .string) val.string else null;
}

fn getInt(obj: JsonValue, key: []const u8) ?i64 {
    if (obj != .object) return null;
    const val = obj.object.get(key) orelse return null;
    return if (val == .integer) val.integer else null;
}

fn getFloat(obj: JsonValue, key: []const u8) ?f64 {
    if (obj != .object) return null;
    const val = obj.object.get(key) orelse return null;
    return switch (val) {
        .float => val.float,
        .integer => @floatFromInt(val.integer),
        else => null,
    };
}

fn getBool(obj: JsonValue, key: []const u8) ?bool {
    if (obj != .object) return null;
    const val = obj.object.get(key) orelse return null;
    return if (val == .bool) val.bool else null;
}

fn getObject(obj: JsonValue, key: []const u8) ?JsonValue {
    if (obj != .object) return null;
    const val = obj.object.get(key) orelse return null;
    return if (val == .object) val else null;
}

fn getArray(obj: JsonValue, key: []const u8) ?[]const JsonValue {
    if (obj != .object) return null;
    const val = obj.object.get(key) orelse return null;
    return if (val == .array) val.array.items else null;
}

// =============================================================================
// Workflow Parser
// =============================================================================

/// Parse a workflow definition from YAML or JSON
pub fn parseWorkflow(allocator: Allocator, content: []const u8) ParseError!WorkflowDefinition {
    // First, try to parse as JSON directly
    if (std.json.parseFromSlice(JsonValue, allocator, content, .{})) |parsed| {
        defer parsed.deinit();
        return parseWorkflowFromJson(allocator, parsed.value);
    } else |_| {
        // JSON parse failed - try converting from YAML
        const json_content = yaml_to_json.convert(allocator, content) catch {
            return ParseError.InvalidFormat;
        };
        defer allocator.free(json_content);

        const parsed = std.json.parseFromSlice(JsonValue, allocator, json_content, .{}) catch {
            return ParseError.InvalidFormat;
        };
        defer parsed.deinit();

        return parseWorkflowFromJson(allocator, parsed.value);
    }
}

/// Parse workflow from parsed JSON value
pub fn parseWorkflowFromJson(allocator: Allocator, root: JsonValue) ParseError!WorkflowDefinition {
    if (root != .object) return ParseError.InvalidFormat;

    // Validate kind
    const kind = getString(root, "kind") orelse return ParseError.MissingRequiredField;
    if (!mem.eql(u8, kind, "Workflow")) return ParseError.InvalidKind;

    // Required fields
    const name = getString(root, "name") orelse return ParseError.MissingRequiredField;
    const version = getString(root, "version") orelse return ParseError.MissingRequiredField;

    // Optional description (defaults to "")
    const description = getString(root, "description") orelse "";

    // Idempotency mode
    const idempotency = blk: {
        const idem_str = getString(root, "idempotency") orelse "none";
        break :blk IdempotencyMode.fromString(idem_str) orelse return ParseError.InvalidIdempotencyMode;
    };

    // Search attributes (optional)
    const search_attributes = try parseSearchAttributes(allocator, root);
    errdefer {
        for (search_attributes) |*attr| {
            var ma = attr.*;
            ma.deinit(allocator);
        }
        allocator.free(search_attributes);
    }

    // Inline plans (optional map)
    const plans = try parseInlinePlans(allocator, root);
    errdefer {
        for (plans) |*p| {
            p.deinit(allocator);
        }
        allocator.free(plans);
    }

    // Start step (required)
    const start_obj = getObject(root, "start") orelse return ParseError.MissingRequiredField;
    const start = try parseStep(allocator, start_obj);
    errdefer {
        var s = start;
        s.deinit(allocator);
    }

    // Steps (optional map)
    const steps = try parseSteps(allocator, root);
    errdefer {
        for (steps) |*step| {
            step.deinit(allocator);
        }
        allocator.free(steps);
    }

    // Terminals (optional map for custom terminals)
    const terminals = try parseTerminals(allocator, root);
    errdefer {
        for (terminals) |*t| {
            t.deinit(allocator);
        }
        allocator.free(terminals);
    }

    // Schedule (optional)
    const schedule = try parseSchedule(allocator, root);
    errdefer {
        if (schedule) |*s| {
            var ms = s.*;
            ms.deinit(allocator);
        }
    }

    // Stream trigger (optional)
    const trigger = try parseTrigger(allocator, root);
    errdefer {
        if (trigger) |*t| {
            var mt = t.*;
            mt.deinit(allocator);
        }
    }

    // Output expression (optional JSONPath, e.g. "$.steps.process_expense.output")
    const output_expr: ?[]const u8 = if (getString(root, "output")) |expr|
        allocator.dupe(u8, expr) catch return ParseError.OutOfMemory
    else
        null;
    errdefer if (output_expr) |o| allocator.free(o);

    return WorkflowDefinition{
        .name = allocator.dupe(u8, name) catch return ParseError.OutOfMemory,
        .description = allocator.dupe(u8, description) catch return ParseError.OutOfMemory,
        .version = allocator.dupe(u8, version) catch return ParseError.OutOfMemory,
        .idempotency = idempotency,
        .search_attributes = search_attributes,
        .plans = plans,
        .start = start,
        .steps = steps,
        .terminals = terminals,
        .schedule = schedule,
        .trigger = trigger,
        .output = output_expr,
    };
}

fn parseSearchAttributes(allocator: Allocator, root: JsonValue) ParseError![]SearchAttrDef {
    const arr = getArray(root, "searchAttributes") orelse return allocator.alloc(SearchAttrDef, 0) catch return ParseError.OutOfMemory;

    var attrs: std.ArrayList(SearchAttrDef) = .empty;
    errdefer {
        for (attrs.items) |*a| {
            a.deinit(allocator);
        }
        attrs.deinit(allocator);
    }

    for (arr) |item| {
        if (item != .object) continue;

        const attr_name = getString(item, "name") orelse continue;
        const type_str = getString(item, "type") orelse "string";
        const from = getString(item, "from") orelse continue;

        const attr_type = SearchAttrType.fromString(type_str) orelse return ParseError.InvalidSearchAttrType;

        attrs.append(allocator, .{
            .name = allocator.dupe(u8, attr_name) catch return ParseError.OutOfMemory,
            .attr_type = attr_type,
            .from = allocator.dupe(u8, from) catch return ParseError.OutOfMemory,
        }) catch return ParseError.OutOfMemory;
    }

    return attrs.toOwnedSlice(allocator) catch return ParseError.OutOfMemory;
}

/// Parse inline plans from workflow YAML
/// plans:
///   payment:
///     selection: health-weighted
///     executors:
///       - name: stripe
///         run: "@actions/stripe-charge"
///         priority: 1
fn parseInlinePlans(allocator: Allocator, root: JsonValue) ParseError![]InlinePlan {
    const plans_obj = getObject(root, "plans") orelse
        return allocator.alloc(InlinePlan, 0) catch return ParseError.OutOfMemory;

    if (plans_obj != .object) return ParseError.InvalidFieldType;

    var plans: std.ArrayList(InlinePlan) = .empty;
    errdefer {
        for (plans.items) |*p| {
            p.deinit(allocator);
        }
        plans.deinit(allocator);
    }

    var iter = plans_obj.object.iterator();
    while (iter.next()) |entry| {
        const plan_name = entry.key_ptr.*;
        const plan_obj = entry.value_ptr.*;

        if (plan_obj != .object) continue;

        const plan = try parseInlinePlan(allocator, plan_name, plan_obj);
        plans.append(allocator, plan) catch return ParseError.OutOfMemory;
    }

    return plans.toOwnedSlice(allocator) catch return ParseError.OutOfMemory;
}

/// Parse a single inline plan definition
fn parseInlinePlan(allocator: Allocator, name: []const u8, obj: JsonValue) ParseError!InlinePlan {
    // Selection strategy (default: static_order)
    const selection = blk: {
        const sel_str = getString(obj, "selection") orelse "static-order";
        break :blk SelectionStrategy.fromString(sel_str) orelse return ParseError.InvalidSelectionStrategy;
    };

    // Error classification (optional)
    const error_classification = try parseErrorClassification(allocator, obj);
    errdefer if (error_classification) |*ec| {
        var mec = ec.*;
        mec.deinit(allocator);
    };

    // Executors (required, at least one)
    const executors = try parseExecutors(allocator, obj);
    if (executors.len == 0) return ParseError.EmptyExecutors;
    errdefer {
        for (executors) |*e| {
            var me = e.*;
            me.deinit(allocator);
        }
        allocator.free(executors);
    }

    // Health config (optional)
    const health_config = try parseHealthConfig(obj);

    // Cache config (optional)
    const cache_config = try parseCacheConfig(allocator, obj);
    errdefer if (cache_config) |*cc| {
        var mcc = cc.*;
        mcc.deinit(allocator);
    };

    // Fallback config (optional)
    const fallback_config = try parseFallbackConfig(allocator, obj);
    errdefer if (fallback_config) |*fc| {
        var mfc = fc.*;
        mfc.deinit(allocator);
    };

    return InlinePlan{
        .name = allocator.dupe(u8, name) catch return ParseError.OutOfMemory,
        .selection = selection,
        .error_classification = error_classification,
        .executors = executors,
        .health_config = health_config,
        .cache_config = cache_config,
        .fallback_config = fallback_config,
    };
}

fn parseStep(allocator: Allocator, obj: JsonValue) ParseError!Step {
    if (obj != .object) return ParseError.InvalidFieldType;

    // Check if it's a run step or waitForSignal step
    if (getObject(obj, "waitForSignal")) |wait_obj| {
        return parseWaitForSignalStep(allocator, obj, wait_obj);
    } else if (getString(obj, "run")) |_| {
        return parseRunStep(allocator, obj);
    } else {
        return ParseError.MissingRequiredField;
    }
}

fn parseRunStep(allocator: Allocator, obj: JsonValue) ParseError!Step {
    const target = getString(obj, "run") orelse return ParseError.MissingRequiredField;

    // Input mapping (optional)
    const input_mapping: ?[]u8 = if (getString(obj, "inputMapping") orelse getString(obj, "input_mapping")) |m|
        allocator.dupe(u8, m) catch return ParseError.OutOfMemory
    else
        null;
    errdefer if (input_mapping) |m| allocator.free(m);

    // Retry policy (optional)
    const retry: ?RetryPolicy = if (getObject(obj, "retry")) |r|
        try parseRetryPolicy(r)
    else
        null;

    // Poll config (optional)
    const poll: ?definition.PollConfig = if (getObject(obj, "poll")) |p|
        try parsePollConfig(p)
    else
        null;

    // Transitions
    const transitions = try parseTransitions(allocator, obj);

    return .{
        .run = .{
            .target = allocator.dupe(u8, target) catch return ParseError.OutOfMemory,
            .input_mapping = input_mapping,
            .retry = retry,
            .poll = poll,
            .transitions = transitions,
        },
    };
}

fn parseWaitForSignalStep(allocator: Allocator, obj: JsonValue, wait_obj: JsonValue) ParseError!Step {
    const signal_type = getString(wait_obj, "type") orelse return ParseError.MissingRequiredField;
    const timeout_ms: ?i64 = getInt(wait_obj, "timeoutMs") orelse getInt(wait_obj, "timeout_ms");
    const on_timeout: ?[]u8 = if (getString(wait_obj, "onTimeout") orelse getString(wait_obj, "on_timeout")) |t|
        allocator.dupe(u8, t) catch return ParseError.OutOfMemory
    else
        null;
    errdefer if (on_timeout) |t| allocator.free(t);

    // Transitions
    const transitions = try parseTransitions(allocator, obj);

    return .{
        .wait_for_signal = .{
            .signal_type = allocator.dupe(u8, signal_type) catch return ParseError.OutOfMemory,
            .timeout_ms = timeout_ms,
            .on_timeout = on_timeout,
            .transitions = transitions,
        },
    };
}

fn parseTransitions(allocator: Allocator, obj: JsonValue) ParseError![]Transition {
    const trans_obj = getObject(obj, "transitions") orelse {
        return allocator.alloc(Transition, 0) catch return ParseError.OutOfMemory;
    };

    var transitions: std.ArrayList(Transition) = .empty;
    errdefer {
        for (transitions.items) |*t| {
            t.deinit(allocator);
        }
        transitions.deinit(allocator);
    }

    var iter = trans_obj.object.iterator();
    while (iter.next()) |entry| {
        const outcome = entry.key_ptr.*;
        const target = if (entry.value_ptr.* == .string)
            entry.value_ptr.string
        else
            continue;

        transitions.append(allocator, .{
            .outcome = allocator.dupe(u8, outcome) catch return ParseError.OutOfMemory,
            .target = allocator.dupe(u8, target) catch return ParseError.OutOfMemory,
        }) catch return ParseError.OutOfMemory;
    }

    return transitions.toOwnedSlice(allocator) catch return ParseError.OutOfMemory;
}

fn parseSteps(allocator: Allocator, root: JsonValue) ParseError![]NamedStep {
    const steps_obj = getObject(root, "steps") orelse {
        return allocator.alloc(NamedStep, 0) catch return ParseError.OutOfMemory;
    };

    var steps: std.ArrayList(NamedStep) = .empty;
    errdefer {
        for (steps.items) |*s| {
            s.deinit(allocator);
        }
        steps.deinit(allocator);
    }

    var iter = steps_obj.object.iterator();
    while (iter.next()) |entry| {
        const step_name = entry.key_ptr.*;
        const step_obj = entry.value_ptr.*;

        if (step_obj != .object) continue;

        const step = try parseStep(allocator, step_obj);

        steps.append(allocator, .{
            .name = allocator.dupe(u8, step_name) catch return ParseError.OutOfMemory,
            .step = step,
        }) catch return ParseError.OutOfMemory;
    }

    return steps.toOwnedSlice(allocator) catch return ParseError.OutOfMemory;
}

fn parseTerminals(allocator: Allocator, root: JsonValue) ParseError![]Terminal {
    const terms_obj = getObject(root, "terminals") orelse {
        return allocator.alloc(Terminal, 0) catch return ParseError.OutOfMemory;
    };

    var terminals: std.ArrayList(Terminal) = .empty;
    errdefer {
        for (terminals.items) |*t| {
            t.deinit(allocator);
        }
        terminals.deinit(allocator);
    }

    var iter = terms_obj.object.iterator();
    while (iter.next()) |entry| {
        const term_name = entry.key_ptr.*;
        const term_obj = entry.value_ptr.*;

        if (term_obj != .object) continue;

        const status_str = getString(term_obj, "status") orelse "failed";
        const status = types.RunStatus.fromString(status_str) orelse types.RunStatus.failed;

        terminals.append(allocator, .{
            .name = allocator.dupe(u8, term_name) catch return ParseError.OutOfMemory,
            .status = status,
        }) catch return ParseError.OutOfMemory;
    }

    return terminals.toOwnedSlice(allocator) catch return ParseError.OutOfMemory;
}

/// Parse optional schedule block from workflow YAML
/// ```yaml
/// schedule:
///   cron: "*/5 * * * *"        # or interval: 30000
///   max_concurrent: 1
///   input: '{"mode": "full"}'
///   paused: false
/// ```
fn parseSchedule(allocator: Allocator, root: JsonValue) ParseError!?ScheduleDef {
    const sched_obj = getObject(root, "schedule") orelse return null;

    var sched = ScheduleDef{};

    // cron expression
    if (getString(sched_obj, "cron")) |cron| {
        sched.cron_expr = allocator.dupe(u8, cron) catch return ParseError.OutOfMemory;
    }

    // interval in ms
    if (getInt(sched_obj, "interval")) |iv| {
        sched.interval_ms = iv;
    }

    // Must have exactly one of cron or interval
    if (!sched.isValid()) {
        if (sched.cron_expr) |c| allocator.free(c);
        return ParseError.InvalidFieldType;
    }

    // max_concurrent (default 1)
    if (getInt(sched_obj, "maxConcurrent")) |mc| {
        sched.max_concurrent = @intCast(mc);
    } else if (getInt(sched_obj, "max_concurrent")) |mc| {
        sched.max_concurrent = @intCast(mc);
    }

    // input override
    if (getString(sched_obj, "input")) |inp| {
        sched.input = allocator.dupe(u8, inp) catch return ParseError.OutOfMemory;
    }

    // paused
    if (getBool(sched_obj, "paused")) |p| {
        sched.paused = p;
    }

    return sched;
}

/// Parse optional stream trigger block from workflow YAML
/// ```yaml
/// trigger:
///   stream: "orders"               # source stream (required)
///   namespace: "prod"              # source namespace (optional)
///   consumer_group: "wf-orders"    # consumer group name (optional)
///   mode: shared                   # shared | exclusive | key_shared
///   batch_size: 1                  # events per workflow run
/// ```
fn parseTrigger(allocator: Allocator, root: JsonValue) ParseError!?StreamTriggerDef {
    const trig_obj = getObject(root, "trigger") orelse return null;

    // stream is required
    const stream = getString(trig_obj, "stream") orelse return ParseError.MissingRequiredField;

    var trig = StreamTriggerDef{
        .stream = allocator.dupe(u8, stream) catch return ParseError.OutOfMemory,
    };
    errdefer allocator.free(trig.stream);

    // namespace (optional)
    if (getString(trig_obj, "namespace")) |ns| {
        trig.namespace = allocator.dupe(u8, ns) catch return ParseError.OutOfMemory;
    }
    errdefer if (trig.namespace) |ns| allocator.free(ns);

    // consumer_group (optional, supports both camelCase and snake_case)
    if (getString(trig_obj, "consumerGroup") orelse getString(trig_obj, "consumer_group")) |cg| {
        trig.consumer_group = allocator.dupe(u8, cg) catch return ParseError.OutOfMemory;
    }
    errdefer if (trig.consumer_group) |cg| allocator.free(cg);

    // mode (default shared)
    if (getString(trig_obj, "mode")) |mode_str| {
        trig.mode = TriggerMode.fromString(mode_str) orelse return ParseError.InvalidFieldType;
    }

    // batch_size (default 1)
    if (getInt(trig_obj, "batchSize") orelse getInt(trig_obj, "batch_size")) |bs| {
        if (bs < 1) return ParseError.InvalidFieldType;
        trig.batch_size = @intCast(bs);
    }

    // batch_timeout_ms (default 5000)
    if (getInt(trig_obj, "batchTimeoutMs") orelse getInt(trig_obj, "batch_timeout_ms")) |bt| {
        if (bt < 0) return ParseError.InvalidFieldType;
        trig.batch_timeout_ms = @intCast(bt);
    }

    if (!trig.isValid()) {
        return ParseError.InvalidFieldType;
    }

    return trig;
}

/// Parse a backoff strategy string. Accepts both the canonical spellings
/// ("constant", "linear", "exponential", "exponential_jitter") and the
/// duration-suffixed forms used elsewhere ("exp-jitter-200ms", "constant-500ms").
/// The jitter variants are matched before the bare "exp" prefix so the
/// documented "exponential_jitter" does not silently degrade to "exponential".
fn parseBackoffStr(backoff_str: []const u8) BackoffType {
    if (mem.startsWith(u8, backoff_str, "exp-jitter") or
        mem.startsWith(u8, backoff_str, "exponential_jitter") or
        mem.startsWith(u8, backoff_str, "exponential-jitter"))
    {
        return .exponential_jitter;
    } else if (mem.startsWith(u8, backoff_str, "exp")) {
        return .exponential;
    } else if (mem.startsWith(u8, backoff_str, "linear")) {
        return .linear;
    } else if (mem.startsWith(u8, backoff_str, "constant")) {
        return .constant;
    } else {
        return BackoffType.fromString(backoff_str) orelse .exponential;
    }
}

fn parseRetryPolicy(obj: JsonValue) ParseError!RetryPolicy {
    if (obj != .object) return ParseError.InvalidFieldType;

    const max_attempts: u32 = if (getInt(obj, "max") orelse getInt(obj, "maxAttempts") orelse getInt(obj, "max_attempts")) |m| @intCast(m) else 3;
    const initial_delay_ms: u32 = if (getInt(obj, "initialDelayMs") orelse getInt(obj, "initial_delay_ms")) |d| @intCast(d) else 1000;
    const max_delay_ms: u32 = if (getInt(obj, "maxDelayMs") orelse getInt(obj, "max_delay_ms")) |d| @intCast(d) else 30000;
    const within_ms: ?u64 = if (getInt(obj, "withinMs") orelse getInt(obj, "within_ms")) |w| @intCast(w) else null;

    const backoff: BackoffType = parseBackoffStr(getString(obj, "backoff") orelse "exponential");

    return .{
        .max_attempts = max_attempts,
        .backoff = backoff,
        .initial_delay_ms = initial_delay_ms,
        .max_delay_ms = max_delay_ms,
        .within_ms = within_ms,
    };
}

fn parsePollConfig(obj: JsonValue) ParseError!definition.PollConfig {
    if (obj != .object) return ParseError.InvalidFieldType;

    const initial_delay_ms: i64 = getInt(obj, "initialDelayMs") orelse getInt(obj, "initial_delay_ms") orelse 0;
    const max_attempts: u32 = if (getInt(obj, "maxAttempts") orelse getInt(obj, "max_attempts") orelse getInt(obj, "max")) |m| @intCast(m) else 10;
    const base_delay_ms: u32 = if (getInt(obj, "baseDelayMs") orelse getInt(obj, "base_delay_ms")) |d| @intCast(d) else 1000;
    const max_delay_ms: u32 = if (getInt(obj, "maxDelayMs") orelse getInt(obj, "max_delay_ms")) |d| @intCast(d) else 60000;

    const backoff: BackoffType = parseBackoffStr(getString(obj, "backoff") orelse "exponential");

    return .{
        .initial_delay_ms = initial_delay_ms,
        .max_attempts = max_attempts,
        .backoff = backoff,
        .base_delay_ms = base_delay_ms,
        .max_delay_ms = max_delay_ms,
    };
}

// =============================================================================
// Error Classification Parser
// =============================================================================

fn parseErrorClassification(allocator: Allocator, root: JsonValue) ParseError!?ErrorClassification {
    const classify_obj = getObject(root, "classifyError") orelse return null;

    var retryable: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (retryable.items) |s| allocator.free(s);
        retryable.deinit(allocator);
    }

    var fatal: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (fatal.items) |s| allocator.free(s);
        fatal.deinit(allocator);
    }

    if (getArray(classify_obj, "retryable")) |arr| {
        for (arr) |item| {
            if (item == .string) {
                retryable.append(allocator, allocator.dupe(u8, item.string) catch return ParseError.OutOfMemory) catch return ParseError.OutOfMemory;
            }
        }
    }

    if (getArray(classify_obj, "fatal")) |arr| {
        for (arr) |item| {
            if (item == .string) {
                fatal.append(allocator, allocator.dupe(u8, item.string) catch return ParseError.OutOfMemory) catch return ParseError.OutOfMemory;
            }
        }
    }

    return ErrorClassification{
        .retryable = retryable.toOwnedSlice(allocator) catch return ParseError.OutOfMemory,
        .fatal = fatal.toOwnedSlice(allocator) catch return ParseError.OutOfMemory,
    };
}

fn parseExecutors(allocator: Allocator, root: JsonValue) ParseError![]ExecutorConfig {
    const arr = getArray(root, "executors") orelse return ParseError.MissingRequiredField;

    var executors: std.ArrayList(ExecutorConfig) = .empty;
    errdefer {
        for (executors.items) |*e| {
            e.deinit(allocator);
        }
        executors.deinit(allocator);
    }

    for (arr) |item| {
        if (item != .object) continue;

        const exec = try parseExecutorConfig(allocator, item);
        executors.append(allocator, exec) catch return ParseError.OutOfMemory;
    }

    return executors.toOwnedSlice(allocator) catch return ParseError.OutOfMemory;
}

fn parseExecutorConfig(allocator: Allocator, obj: JsonValue) ParseError!ExecutorConfig {
    const name = getString(obj, "name") orelse return ParseError.MissingRequiredField;
    const action = getString(obj, "run") orelse return ParseError.MissingRequiredField;
    const priority: i32 = if (getInt(obj, "priority")) |p| @intCast(p) else 100;

    // Retry policy
    const retry: ?RetryPolicy = if (getObject(obj, "retry")) |r|
        try parseRetryPolicy(r)
    else
        null;

    // Circuit breaker
    const breaker: ?CircuitBreakerConfig = if (getObject(obj, "breaker")) |b|
        try parseCircuitBreakerConfig(b)
    else
        null;

    // Tracking
    const tracking: ?TrackingConfig = if (getObject(obj, "tracking")) |t|
        try parseTrackingConfig(t)
    else
        null;

    // Rate limit
    const rate_limit: ?RateLimitConfig = if (getObject(obj, "rateLimit")) |r|
        try parseRateLimitConfig(r)
    else
        null;

    return ExecutorConfig{
        .name = allocator.dupe(u8, name) catch return ParseError.OutOfMemory,
        .action_name = allocator.dupe(u8, action) catch return ParseError.OutOfMemory,
        .priority = priority,
        .retry = retry,
        .breaker = breaker,
        .tracking = tracking,
        .rate_limit = rate_limit,
    };
}

fn parseCircuitBreakerConfig(obj: JsonValue) ParseError!CircuitBreakerConfig {
    if (obj != .object) return ParseError.InvalidFieldType;

    return .{
        .failure_threshold = if (getInt(obj, "failureThreshold") orelse getInt(obj, "failure_threshold")) |f| @intCast(f) else 5,
        .cooldown_ms = getInt(obj, "cooldownMs") orelse getInt(obj, "cooldown_ms") orelse 60000,
        .half_open_max_calls = if (getInt(obj, "halfOpenMaxCalls") orelse getInt(obj, "half_open_max_calls")) |h| @intCast(h) else 2,
    };
}

fn parseTrackingConfig(obj: JsonValue) ParseError!TrackingConfig {
    if (obj != .object) return ParseError.InvalidFieldType;

    const mode_str = getString(obj, "mode") orelse "sync";
    const mode: TrackingMode = if (mem.eql(u8, mode_str, "async"))
        .async_mode
    else
        .sync;

    return .{
        .mode = mode,
        .timeout_ms = getInt(obj, "timeoutMs") orelse getInt(obj, "timeout_ms"),
    };
}

fn parseRateLimitConfig(obj: JsonValue) ParseError!RateLimitConfig {
    if (obj != .object) return ParseError.InvalidFieldType;

    return .{
        .max_per_second = if (getInt(obj, "maxPerSecond")) |m| @intCast(m) else null,
        .max_per_minute = if (getInt(obj, "maxPerMinute")) |m| @intCast(m) else null,
        .max_per_hour = if (getInt(obj, "maxPerHour")) |m| @intCast(m) else null,
    };
}

fn parseHealthConfig(root: JsonValue) ParseError!?HealthConfig {
    const health_obj = getObject(root, "health") orelse return null;

    // Parse window like "5m" -> 300000 ms
    const window_ms: i64 = blk: {
        const window_str = getString(health_obj, "window") orelse "5m";
        break :blk parseTimeString(window_str) orelse 300000;
    };

    return .{
        .window_ms = window_ms,
        .decay = getFloat(health_obj, "decay") orelse 0.9,
        .min_samples = if (getInt(health_obj, "minSamples")) |m| @intCast(m) else 50,
    };
}

fn parseCacheConfig(allocator: Allocator, root: JsonValue) ParseError!?CacheConfig {
    const cache_obj = getObject(root, "cache") orelse return null;

    const ttl_ms = getInt(cache_obj, "ttlMs") orelse 300000;
    const key_template = getString(cache_obj, "keyTemplate") orelse return null;

    var invalidate_on: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (invalidate_on.items) |s| allocator.free(s);
        invalidate_on.deinit(allocator);
    }

    if (getArray(cache_obj, "invalidateOn")) |arr| {
        for (arr) |item| {
            if (item == .string) {
                invalidate_on.append(allocator, allocator.dupe(u8, item.string) catch return ParseError.OutOfMemory) catch return ParseError.OutOfMemory;
            }
        }
    }

    return CacheConfig{
        .ttl_ms = ttl_ms,
        .key_template = allocator.dupe(u8, key_template) catch return ParseError.OutOfMemory,
        .invalidate_on = invalidate_on.toOwnedSlice(allocator) catch return ParseError.OutOfMemory,
    };
}

fn parseFallbackConfig(allocator: Allocator, root: JsonValue) ParseError!?FallbackConfig {
    const fallback_obj = getObject(root, "fallback") orelse return null;

    const value = getString(fallback_obj, "value") orelse return null;

    const condition: FallbackCondition = blk: {
        const cond_str = getString(fallback_obj, "condition") orelse "exhausted";
        if (mem.eql(u8, cond_str, "any_error")) {
            break :blk .any_error;
        } else {
            break :blk .exhausted;
        }
    };

    return FallbackConfig{
        .value = allocator.dupe(u8, value) catch return ParseError.OutOfMemory,
        .condition = condition,
    };
}

/// Parse time string like "5m", "1h", "30s" to milliseconds
fn parseTimeString(s: []const u8) ?i64 {
    if (s.len < 2) return null;

    const unit = s[s.len - 1];
    const num_str = s[0 .. s.len - 1];
    const num = std.fmt.parseInt(i64, num_str, 10) catch return null;

    return switch (unit) {
        's' => num * 1000,
        'm' => num * 60 * 1000,
        'h' => num * 60 * 60 * 1000,
        'd' => num * 24 * 60 * 60 * 1000,
        else => null,
    };
}

// =============================================================================
// Tests
// =============================================================================

test "parseWorkflow: basic workflow" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const json =
        \\{
        \\  "kind": "Workflow",
        \\  "name": "process-order",
        \\  "version": "1.0.0",
        \\  "idempotency": "required",
        \\  "start": {
        \\    "run": "@actions/validate-order",
        \\    "transitions": {
        \\      "success": "charge_payment",
        \\      "failure": "flo.Failed"
        \\    }
        \\  },
        \\  "steps": {
        \\    "charge_payment": {
        \\      "run": "@plan/payment-processing",
        \\      "transitions": {
        \\        "success": "flo.Completed",
        \\        "failure": "flo.Failed"
        \\      }
        \\    }
        \\  }
        \\}
    ;

    var def = try parseWorkflow(allocator, json);
    defer def.deinit(allocator);

    try testing.expectEqualStrings("process-order", def.name);
    try testing.expectEqualStrings("1.0.0", def.version);
    try testing.expectEqual(IdempotencyMode.required, def.idempotency);
    try testing.expectEqualStrings("@actions/validate-order", def.start.run.target);
    try testing.expectEqual(@as(usize, 2), def.start.run.transitions.len);
    try testing.expectEqual(@as(usize, 1), def.steps.len);
}

// The docs (and BackoffType.fromString) spell the jittered strategy
// "exponential_jitter". parseBackoffStr must honor that spelling in both retry
// and poll configs rather than letting the bare "exp" prefix degrade it to
// plain .exponential.
test "parseWorkflow: exponential_jitter backoff parses to jitter" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const json =
        \\{
        \\  "kind": "Workflow",
        \\  "name": "backoff-test",
        \\  "version": "1.0.0",
        \\  "start": {
        \\    "run": "@actions/x",
        \\    "retry": { "max_attempts": 3, "backoff": "exponential_jitter", "initial_delay_ms": 10 },
        \\    "poll":  { "maxAttempts": 3, "backoff": "exponential_jitter", "baseDelayMs": 10 },
        \\    "transitions": { "success": "flo.Completed", "failure": "flo.Failed" }
        \\  }
        \\}
    ;

    var def = try parseWorkflow(allocator, json);
    defer def.deinit(allocator);

    try testing.expectEqual(BackoffType.exponential_jitter, def.start.run.retry.?.backoff);
    try testing.expectEqual(BackoffType.exponential_jitter, def.start.run.poll.?.backoff);
}

test "parseWorkflow: with search attributes" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const json =
        \\{
        \\  "kind": "Workflow",
        \\  "name": "order-flow",
        \\  "version": "1.0.0",
        \\  "searchAttributes": [
        \\    {"name": "customer_id", "type": "string", "from": "input.customer_id"},
        \\    {"name": "order_amount", "type": "number", "from": "input.amount"}
        \\  ],
        \\  "start": {
        \\    "run": "@actions/validate",
        \\    "transitions": {"success": "flo.Completed"}
        \\  }
        \\}
    ;

    var def = try parseWorkflow(allocator, json);
    defer def.deinit(allocator);

    try testing.expectEqual(@as(usize, 2), def.search_attributes.len);
    try testing.expectEqualStrings("customer_id", def.search_attributes[0].name);
    try testing.expectEqual(SearchAttrType.string, def.search_attributes[0].attr_type);
    try testing.expectEqualStrings("order_amount", def.search_attributes[1].name);
    try testing.expectEqual(SearchAttrType.number, def.search_attributes[1].attr_type);
}

test "parseWorkflow: with custom terminals" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const json =
        \\{
        \\  "kind": "Workflow",
        \\  "name": "payment-flow",
        \\  "version": "1.0.0",
        \\  "start": {
        \\    "run": "@actions/charge",
        \\    "transitions": {
        \\      "success": "flo.Completed",
        \\      "fraud": "FraudDetected"
        \\    }
        \\  },
        \\  "terminals": {
        \\    "FraudDetected": {"status": "failed"},
        \\    "Refunded": {"status": "cancelled"}
        \\  }
        \\}
    ;

    var def = try parseWorkflow(allocator, json);
    defer def.deinit(allocator);

    try testing.expectEqual(@as(usize, 2), def.terminals.len);
}

test "parseTimeString" {
    const testing = std.testing;

    try testing.expectEqual(@as(?i64, 5000), parseTimeString("5s"));
    try testing.expectEqual(@as(?i64, 300000), parseTimeString("5m"));
    try testing.expectEqual(@as(?i64, 3600000), parseTimeString("1h"));
    try testing.expectEqual(@as(?i64, 86400000), parseTimeString("1d"));
    try testing.expectEqual(@as(?i64, null), parseTimeString("x"));
}

test "parseWorkflow: YAML with inline plans" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Test YAML similar to what YamlBuilder produces
    const yaml =
        \\kind: Workflow
        \\name: order-process
        \\version: "1.0.0"
        \\
        \\plans:
        \\  payment:
        \\    selection: health-weighted
        \\    executors:
        \\      - name: stripe
        \\        run: "@actions/charge-stripe"
        \\        priority: 100
        \\
        \\start:
        \\  run: "@plan/payment"
        \\  transitions:
        \\    success: flo.Completed
        \\    failure: flo.Failed
    ;

    var def = parseWorkflow(allocator, yaml) catch |err| {
        std.debug.print("Parse error: {any}\n", .{err});
        return err;
    };
    defer def.deinit(allocator);

    try testing.expectEqualStrings("order-process", def.name);
    try testing.expectEqualStrings("1.0.0", def.version);
    try testing.expectEqual(@as(usize, 1), def.plans.len);
    try testing.expectEqualStrings("payment", def.plans[0].name);
}

test "parseWorkflow: YAML with schedule block" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const yaml =
        \\kind: Workflow
        \\name: e2e2-scheduled
        \\version: "1.0.0"
        \\
        \\schedule:
        \\  cron: "0 */6 * * *"
        \\  max_concurrent: 1
        \\  input: '{"mode":"full"}'
        \\
        \\start:
        \\  run: "@actions/e2e2-reconcile"
        \\  transitions:
        \\    success: generate-report
        \\    failure: flo.Failed
        \\
        \\steps:
        \\  generate-report:
        \\    run: "@actions/e2e2-report"
        \\    transitions:
        \\      success: flo.Completed
        \\      failure: flo.Failed
    ;

    var def = parseWorkflow(allocator, yaml) catch |err| {
        std.debug.print("Parse error: {any}\n", .{err});
        return err;
    };
    defer def.deinit(allocator);

    try testing.expectEqualStrings("e2e2-scheduled", def.name);
    try testing.expectEqualStrings("1.0.0", def.version);
    try testing.expect(def.schedule != null);
    try testing.expectEqualStrings("0 */6 * * *", def.schedule.?.cron_expr.?);
    try testing.expectEqual(@as(u32, 1), def.schedule.?.max_concurrent);
    try testing.expectEqualStrings("{\"mode\":\"full\"}", def.schedule.?.input.?);
}

test "parseWorkflow: YAML with output mapping" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const yaml =
        \\kind: Workflow
        \\name: test-output
        \\version: "1.0.0"
        \\output: '{"order_id": "$.input.orderId", "tracking": "$.steps.ship.output.trackingId"}'
        \\start:
        \\  run: "@actions/ship"
        \\  transitions:
        \\    success: flo.Completed
    ;

    var def = parseWorkflow(allocator, yaml) catch |err| {
        std.debug.print("Parse error: {any}\n", .{err});
        return err;
    };
    defer def.deinit(allocator);

    try testing.expectEqualStrings("test-output", def.name);
    try testing.expect(def.output != null);
    // Single-quoted YAML string should be preserved as the inner JSON content
    try testing.expectEqualStrings(
        \\{"order_id": "$.input.orderId", "tracking": "$.steps.ship.output.trackingId"}
    , def.output.?);
}

test "parseWorkflow: YAML with direct step output passthrough" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const yaml =
        \\kind: Workflow
        \\name: audio-pipeline
        \\version: "1.0.0"
        \\output: "$.steps.encode.output"
        \\start:
        \\  run: "@actions/encode"
        \\  transitions:
        \\    success: flo.Completed
    ;

    var def = parseWorkflow(allocator, yaml) catch |err| {
        std.debug.print("Parse error: {any}\n", .{err});
        return err;
    };
    defer def.deinit(allocator);

    try testing.expectEqualStrings("audio-pipeline", def.name);
    try testing.expect(def.output != null);
    try testing.expectEqualStrings("$.steps.encode.output", def.output.?);
}
