//! Workflow YAML/JSON Parser
//!
//! Parses workflow definitions from YAML (converted to JSON internally) or
//! JSON. Every key is spelled one way, in snake_case; a key the parser
//! doesn't read, a value of the wrong kind, or an unknown enum value is
//! refused, naming the key and where it is. A typo must not change how a
//! workflow runs.
//!
//! ```zig
//! var diag: parser.Diagnostic = .{};
//! var def = parser.parseWorkflow(allocator, yaml, &diag) catch |err| {
//!     // diag.message(): e.g. `unknown key "trasitions" at steps.charge`
//! };
//! defer def.deinit(allocator);
//! ```

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

const definition = @import("definition.zig");
const plan_types = @import("plan_types.zig");
const types = @import("types.zig");
const yaml_to_json = @import("../util/yaml_to_json.zig");
const definition_diag = @import("../util/definition_diag.zig");

pub const Diagnostic = definition_diag.Diagnostic;

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
    UnknownKey,
    DuplicateKey,
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
// Strict JSON Value Helpers
// =============================================================================

const JsonValue = std.json.Value;
const D = *Diagnostic;
const kindName = definition_diag.kindName;

fn checkKeys(d: D, obj: JsonValue, comptime allowed: []const []const u8) ParseError!void {
    return definition_diag.checkKeys(d, obj, allowed, ParseError.UnknownKey);
}

fn wrongKind(d: D, key: []const u8, want: []const u8, v: JsonValue) ParseError {
    return d.fail(ParseError.InvalidFieldType, "\"{s}\" must be {s}, not {s}", .{ key, want, kindName(v) });
}

fn missing(d: D, key: []const u8) ParseError {
    return d.fail(ParseError.MissingRequiredField, "missing required key \"{s}\"", .{key});
}

fn optString(d: D, obj: JsonValue, key: []const u8) ParseError!?[]const u8 {
    const v = obj.object.get(key) orelse return null;
    return if (v == .string) v.string else wrongKind(d, key, "a string", v);
}

fn reqString(d: D, obj: JsonValue, key: []const u8) ParseError![]const u8 {
    return try optString(d, obj, key) orelse missing(d, key);
}

fn optInt(d: D, obj: JsonValue, key: []const u8) ParseError!?i64 {
    const v = obj.object.get(key) orelse return null;
    return if (v == .integer) v.integer else wrongKind(d, key, "an integer", v);
}

/// An integer key as the field's type, or refused when it doesn't fit (a
/// negative count, or more than the field holds).
fn optIntAs(comptime T: type, d: D, obj: JsonValue, key: []const u8) ParseError!?T {
    const v = try optInt(d, obj, key) orelse return null;
    return std.math.cast(T, v) orelse d.fail(
        ParseError.InvalidFieldType,
        "\"{s}\" must be from {d} to {d}, not {d}",
        .{ key, std.math.minInt(T), std.math.maxInt(T), v },
    );
}

/// An integer key that must be at least `min` (0 where zero means "now",
/// 1 where a zero would never fire or never expire).
fn optAtLeast(d: D, obj: JsonValue, key: []const u8, min: i64) ParseError!?i64 {
    const v = try optInt(d, obj, key) orelse return null;
    if (v < min) return d.fail(ParseError.InvalidFieldType, "\"{s}\" must be at least {d}, not {d}", .{ key, min, v });
    return v;
}

fn optFloat(d: D, obj: JsonValue, key: []const u8) ParseError!?f64 {
    const v = obj.object.get(key) orelse return null;
    return switch (v) {
        .float => v.float,
        .integer => @floatFromInt(v.integer),
        else => wrongKind(d, key, "a number", v),
    };
}

fn optBool(d: D, obj: JsonValue, key: []const u8) ParseError!?bool {
    const v = obj.object.get(key) orelse return null;
    return if (v == .bool) v.bool else wrongKind(d, key, "true or false", v);
}

fn optObject(d: D, obj: JsonValue, key: []const u8) ParseError!?JsonValue {
    const v = obj.object.get(key) orelse return null;
    return if (v == .object) v else wrongKind(d, key, "a map", v);
}

fn optArray(d: D, obj: JsonValue, key: []const u8) ParseError!?[]const JsonValue {
    const v = obj.object.get(key) orelse return null;
    return if (v == .array) v.array.items else wrongKind(d, key, "a list", v);
}

/// `s` as one of `names`, or refused listing them.
fn oneOf(
    comptime T: type,
    d: D,
    key: []const u8,
    s: []const u8,
    comptime names: []const struct { []const u8, T },
    err: ParseError,
) ParseError!T {
    inline for (names) |n| {
        if (mem.eql(u8, s, n[0])) return n[1];
    }
    const list = comptime blk: {
        var l: []const u8 = "";
        for (names, 0..) |n, i| l = l ++ (if (i == 0) "" else "|") ++ n[0];
        break :blk l;
    };
    return d.fail(err, "\"{s}\" must be one of " ++ list ++ ", not \"{s}\"", .{ key, s });
}

fn dupe(allocator: Allocator, s: []const u8) ParseError![]u8 {
    return allocator.dupe(u8, s) catch ParseError.OutOfMemory;
}

// =============================================================================
// Workflow Parser
// =============================================================================

/// Parse a workflow definition from YAML or JSON. On a refusal other than
/// OutOfMemory, `diag` (when given) says what and where.
pub fn parseWorkflow(allocator: Allocator, content: []const u8, diag: ?*Diagnostic) ParseError!WorkflowDefinition {
    var scratch: Diagnostic = .{};
    const d = diag orelse &scratch;

    if (std.json.parseFromSlice(JsonValue, allocator, content, .{})) |parsed| {
        defer parsed.deinit();
        return parseWorkflowFromJson(allocator, parsed.value, d);
    } else |err| switch (err) {
        error.OutOfMemory => return ParseError.OutOfMemory,
        error.DuplicateField => return definition_diag.failDuplicateKey(allocator, d, content, ParseError.DuplicateKey),
        else => {},
    }

    // Not JSON: YAML, converted to JSON.
    const json_content = yaml_to_json.convert(allocator, content) catch
        return d.fail(ParseError.InvalidFormat, "the definition is neither JSON nor YAML", .{});
    defer allocator.free(json_content);

    const parsed = std.json.parseFromSlice(JsonValue, allocator, json_content, .{}) catch |err| switch (err) {
        error.OutOfMemory => return ParseError.OutOfMemory,
        error.DuplicateField => return definition_diag.failDuplicateKey(allocator, d, json_content, ParseError.DuplicateKey),
        else => return d.fail(ParseError.InvalidFormat, "the definition is neither JSON nor YAML", .{}),
    };
    defer parsed.deinit();

    return parseWorkflowFromJson(allocator, parsed.value, d);
}

fn parseWorkflowFromJson(allocator: Allocator, root: JsonValue, d: D) ParseError!WorkflowDefinition {
    if (root != .object) return d.fail(ParseError.InvalidFormat, "a workflow definition must be a map, not {s}", .{kindName(root)});
    try checkKeys(d, root, &.{
        "kind",     "name",     "version",  "description", "idempotency", "search_attributes",
        "plans",    "start",    "steps",    "terminals",   "schedule",    "trigger",
        "output",
    });

    const kind = try reqString(d, root, "kind");
    if (!mem.eql(u8, kind, "Workflow")) return d.fail(ParseError.InvalidKind, "\"kind\" must be Workflow, not \"{s}\"", .{kind});

    const name = try reqString(d, root, "name");
    const version = try reqString(d, root, "version");
    const description = try optString(d, root, "description") orelse "";

    const idempotency = try oneOf(IdempotencyMode, d, "idempotency", try optString(d, root, "idempotency") orelse "none", &.{
        .{ "none", .none }, .{ "optional", .optional }, .{ "required", .required },
    }, ParseError.InvalidIdempotencyMode);

    const search_attributes = try parseSearchAttributes(allocator, root, d);
    errdefer {
        for (search_attributes) |*attr| {
            var ma = attr.*;
            ma.deinit(allocator);
        }
        allocator.free(search_attributes);
    }

    const plans = try parseInlinePlans(allocator, root, d);
    errdefer {
        for (plans) |*p| p.deinit(allocator);
        allocator.free(plans);
    }

    const start_obj = try optObject(d, root, "start") orelse return missing(d, "start");
    const start_mark = d.push("start");
    const start = try parseStep(allocator, start_obj, d);
    d.pop(start_mark);
    errdefer {
        var s = start;
        s.deinit(allocator);
    }

    const steps = try parseSteps(allocator, root, d);
    errdefer {
        for (steps) |*step| step.deinit(allocator);
        allocator.free(steps);
    }

    const terminals = try parseTerminals(allocator, root, d);
    errdefer {
        for (terminals) |*t| t.deinit(allocator);
        allocator.free(terminals);
    }

    const schedule = try parseSchedule(allocator, root, d);
    errdefer if (schedule) |*s| {
        var ms = s.*;
        ms.deinit(allocator);
    };

    const trigger = try parseTrigger(allocator, root, d);
    errdefer if (trigger) |*t| {
        var mt = t.*;
        mt.deinit(allocator);
    };

    // Output expression (optional JSONPath, e.g. "$.steps.process_expense.output")
    const output_expr: ?[]const u8 = if (try optString(d, root, "output")) |expr| try dupe(allocator, expr) else null;
    errdefer if (output_expr) |o| allocator.free(o);

    const name_d = try dupe(allocator, name);
    errdefer allocator.free(name_d);
    const description_d = try dupe(allocator, description);
    errdefer allocator.free(description_d);

    return WorkflowDefinition{
        .name = name_d,
        .description = description_d,
        .version = try dupe(allocator, version),
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

fn parseSearchAttributes(allocator: Allocator, root: JsonValue, d: D) ParseError![]SearchAttrDef {
    const arr = try optArray(d, root, "search_attributes") orelse return allocator.alloc(SearchAttrDef, 0) catch ParseError.OutOfMemory;
    const list_mark = d.push("search_attributes");
    defer d.pop(list_mark);

    var attrs: std.ArrayList(SearchAttrDef) = .empty;
    errdefer {
        for (attrs.items) |*a| a.deinit(allocator);
        attrs.deinit(allocator);
    }

    for (arr, 0..) |item, i| {
        const mark = d.pushIndex(i);
        defer d.pop(mark);
        if (item != .object) return d.fail(ParseError.InvalidFieldType, "a search attribute must be a map, not {s}", .{kindName(item)});
        try checkKeys(d, item, &.{ "name", "type", "from" });

        const attr_name = try reqString(d, item, "name");
        const from = try reqString(d, item, "from");
        const attr_type = try oneOf(SearchAttrType, d, "type", try optString(d, item, "type") orelse "string", &.{
            .{ "string", .string }, .{ "number", .number }, .{ "timestamp", .timestamp },
        }, ParseError.InvalidSearchAttrType);

        const name_d = try dupe(allocator, attr_name);
        errdefer allocator.free(name_d);
        const from_d = try dupe(allocator, from);
        errdefer allocator.free(from_d);
        attrs.append(allocator, .{ .name = name_d, .attr_type = attr_type, .from = from_d }) catch return ParseError.OutOfMemory;
    }

    return attrs.toOwnedSlice(allocator) catch ParseError.OutOfMemory;
}

/// Inline plans, a map of plan name to plan:
/// plans:
///   payment:
///     selection: health-weighted
///     executors:
///       - name: stripe
///         run: "@actions/stripe-charge"
///         priority: 1
fn parseInlinePlans(allocator: Allocator, root: JsonValue, d: D) ParseError![]InlinePlan {
    const plans_obj = try optObject(d, root, "plans") orelse return allocator.alloc(InlinePlan, 0) catch ParseError.OutOfMemory;
    const plans_mark = d.push("plans");
    defer d.pop(plans_mark);

    var plans: std.ArrayList(InlinePlan) = .empty;
    errdefer {
        for (plans.items) |*p| p.deinit(allocator);
        plans.deinit(allocator);
    }

    var iter = plans_obj.object.iterator();
    while (iter.next()) |entry| {
        const plan_name = entry.key_ptr.*;
        const plan_obj = entry.value_ptr.*;
        if (plan_obj != .object) return wrongKind(d, plan_name, "a map", plan_obj);

        const mark = d.push(plan_name);
        defer d.pop(mark);
        const plan = try parseInlinePlan(allocator, plan_name, plan_obj, d);
        plans.append(allocator, plan) catch return ParseError.OutOfMemory;
    }

    return plans.toOwnedSlice(allocator) catch ParseError.OutOfMemory;
}

fn parseInlinePlan(allocator: Allocator, name: []const u8, obj: JsonValue, d: D) ParseError!InlinePlan {
    try checkKeys(d, obj, &.{ "selection", "errors", "executors", "health", "cache", "fallback" });

    const selection = try oneOf(SelectionStrategy, d, "selection", try optString(d, obj, "selection") orelse "static-order", &.{
        .{ "static-order", .static_order },       .{ "round-robin", .round_robin },
        .{ "random", .random },                   .{ "health-weighted", .health_weighted },
    }, ParseError.InvalidSelectionStrategy);

    const error_classification = try parseErrorClassification(allocator, obj, d);
    errdefer if (error_classification) |*ec| {
        var mec = ec.*;
        mec.deinit(allocator);
    };

    const executors = try parseExecutors(allocator, obj, d);
    errdefer {
        for (executors) |*e| {
            var me = e.*;
            me.deinit(allocator);
        }
        allocator.free(executors);
    }
    if (executors.len == 0) return d.fail(ParseError.EmptyExecutors, "\"executors\" must name at least one executor", .{});

    const health_config = try parseHealthConfig(obj, d);

    const cache_config = try parseCacheConfig(allocator, obj, d);
    errdefer if (cache_config) |*cc| {
        var mcc = cc.*;
        mcc.deinit(allocator);
    };

    const fallback_config = try parseFallbackConfig(allocator, obj, d);
    errdefer if (fallback_config) |*fc| {
        var mfc = fc.*;
        mfc.deinit(allocator);
    };

    return InlinePlan{
        .name = try dupe(allocator, name),
        .selection = selection,
        .error_classification = error_classification,
        .executors = executors,
        .health_config = health_config,
        .cache_config = cache_config,
        .fallback_config = fallback_config,
    };
}

fn parseStep(allocator: Allocator, obj: JsonValue, d: D) ParseError!Step {
    if (obj != .object) return d.fail(ParseError.InvalidFieldType, "a step must be a map, not {s}", .{kindName(obj)});

    const has_wait = obj.object.contains("wait_for_signal");
    const has_run = obj.object.contains("run");
    if (has_wait and has_run) return d.fail(ParseError.InvalidFieldType, "a step has \"run\" or \"wait_for_signal\", not both", .{});
    if (has_wait) return parseWaitForSignalStep(allocator, obj, d);
    if (has_run) return parseRunStep(allocator, obj, d);
    return d.fail(ParseError.MissingRequiredField, "a step needs \"run\" or \"wait_for_signal\"", .{});
}

fn parseRunStep(allocator: Allocator, obj: JsonValue, d: D) ParseError!Step {
    try checkKeys(d, obj, &.{ "run", "input_mapping", "retry", "poll", "transitions" });
    const target = try reqString(d, obj, "run");

    const input_mapping: ?[]u8 = if (try optString(d, obj, "input_mapping")) |m| try dupe(allocator, m) else null;
    errdefer if (input_mapping) |m| allocator.free(m);

    const retry: ?RetryPolicy = if (try optObject(d, obj, "retry")) |r| blk: {
        const mark = d.push("retry");
        defer d.pop(mark);
        break :blk try parseRetryPolicy(r, d);
    } else null;

    const poll: ?definition.PollConfig = if (try optObject(d, obj, "poll")) |p| blk: {
        const mark = d.push("poll");
        defer d.pop(mark);
        break :blk try parsePollConfig(p, d);
    } else null;

    const transitions = try parseTransitions(allocator, obj, d);
    errdefer freeTransitions(allocator, transitions);

    return .{
        .run = .{
            .target = try dupe(allocator, target),
            .input_mapping = input_mapping,
            .retry = retry,
            .poll = poll,
            .transitions = transitions,
        },
    };
}

fn parseWaitForSignalStep(allocator: Allocator, obj: JsonValue, d: D) ParseError!Step {
    try checkKeys(d, obj, &.{ "wait_for_signal", "transitions" });
    const wait_obj = (try optObject(d, obj, "wait_for_signal")).?;

    const mark = d.push("wait_for_signal");
    try checkKeys(d, wait_obj, &.{ "type", "timeout_ms", "on_timeout" });
    const signal_type = try reqString(d, wait_obj, "type");
    const timeout_ms = try optAtLeast(d, wait_obj, "timeout_ms", 1);
    const on_timeout: ?[]u8 = if (try optString(d, wait_obj, "on_timeout")) |t| try dupe(allocator, t) else null;
    errdefer if (on_timeout) |t| allocator.free(t);
    d.pop(mark);

    const transitions = try parseTransitions(allocator, obj, d);
    errdefer freeTransitions(allocator, transitions);

    return .{
        .wait_for_signal = .{
            .signal_type = try dupe(allocator, signal_type),
            .timeout_ms = timeout_ms,
            .on_timeout = on_timeout,
            .transitions = transitions,
        },
    };
}

fn freeTransitions(allocator: Allocator, transitions: []Transition) void {
    for (transitions) |*t| t.deinit(allocator);
    allocator.free(transitions);
}

/// `transitions` maps an outcome (any name) to the step or terminal it leads to.
fn parseTransitions(allocator: Allocator, obj: JsonValue, d: D) ParseError![]Transition {
    const trans_obj = try optObject(d, obj, "transitions") orelse return allocator.alloc(Transition, 0) catch ParseError.OutOfMemory;
    const mark = d.push("transitions");
    defer d.pop(mark);

    var transitions: std.ArrayList(Transition) = .empty;
    errdefer {
        for (transitions.items) |*t| t.deinit(allocator);
        transitions.deinit(allocator);
    }

    var iter = trans_obj.object.iterator();
    while (iter.next()) |entry| {
        const outcome = entry.key_ptr.*;
        const target = entry.value_ptr.*;
        if (target != .string) return wrongKind(d, outcome, "a step or terminal name", target);

        const outcome_d = try dupe(allocator, outcome);
        errdefer allocator.free(outcome_d);
        const target_d = try dupe(allocator, target.string);
        errdefer allocator.free(target_d);
        transitions.append(allocator, .{ .outcome = outcome_d, .target = target_d }) catch return ParseError.OutOfMemory;
    }

    return transitions.toOwnedSlice(allocator) catch ParseError.OutOfMemory;
}

fn parseSteps(allocator: Allocator, root: JsonValue, d: D) ParseError![]NamedStep {
    const steps_obj = try optObject(d, root, "steps") orelse return allocator.alloc(NamedStep, 0) catch ParseError.OutOfMemory;
    const steps_mark = d.push("steps");
    defer d.pop(steps_mark);

    var steps: std.ArrayList(NamedStep) = .empty;
    errdefer {
        for (steps.items) |*s| s.deinit(allocator);
        steps.deinit(allocator);
    }

    var iter = steps_obj.object.iterator();
    while (iter.next()) |entry| {
        const step_name = entry.key_ptr.*;
        const mark = d.push(step_name);
        defer d.pop(mark);

        var step = try parseStep(allocator, entry.value_ptr.*, d);
        errdefer step.deinit(allocator);
        const name_d = try dupe(allocator, step_name);
        errdefer allocator.free(name_d);
        steps.append(allocator, .{ .name = name_d, .step = step }) catch return ParseError.OutOfMemory;
    }

    return steps.toOwnedSlice(allocator) catch ParseError.OutOfMemory;
}

fn parseTerminals(allocator: Allocator, root: JsonValue, d: D) ParseError![]Terminal {
    const terms_obj = try optObject(d, root, "terminals") orelse return allocator.alloc(Terminal, 0) catch ParseError.OutOfMemory;
    const terms_mark = d.push("terminals");
    defer d.pop(terms_mark);

    var terminals: std.ArrayList(Terminal) = .empty;
    errdefer {
        for (terminals.items) |*t| t.deinit(allocator);
        terminals.deinit(allocator);
    }

    var iter = terms_obj.object.iterator();
    while (iter.next()) |entry| {
        const term_name = entry.key_ptr.*;
        const term_obj = entry.value_ptr.*;
        if (term_obj != .object) return wrongKind(d, term_name, "a map", term_obj);

        const mark = d.push(term_name);
        defer d.pop(mark);
        try checkKeys(d, term_obj, &.{"status"});
        const status = try oneOf(types.RunStatus, d, "status", try optString(d, term_obj, "status") orelse "failed", &.{
            .{ "completed", .completed }, .{ "failed", .failed }, .{ "cancelled", .cancelled }, .{ "timed_out", .timed_out },
        }, ParseError.InvalidFieldType);

        terminals.append(allocator, .{ .name = try dupe(allocator, term_name), .status = status }) catch return ParseError.OutOfMemory;
    }

    return terminals.toOwnedSlice(allocator) catch ParseError.OutOfMemory;
}

/// Optional schedule block:
/// ```yaml
/// schedule:
///   cron: "*/5 * * * *"        # or interval: 30000
///   max_concurrent: 1
///   input: '{"mode": "full"}'
///   paused: false
/// ```
fn parseSchedule(allocator: Allocator, root: JsonValue, d: D) ParseError!?ScheduleDef {
    const sched_obj = try optObject(d, root, "schedule") orelse return null;
    const mark = d.push("schedule");
    defer d.pop(mark);
    try checkKeys(d, sched_obj, &.{ "cron", "interval", "max_concurrent", "input", "paused" });

    const cron = try optString(d, sched_obj, "cron");
    const interval = try optAtLeast(d, sched_obj, "interval", 1);
    if ((cron == null) == (interval == null)) return d.fail(ParseError.InvalidFieldType, "a schedule needs exactly one of \"cron\" or \"interval\"", .{});
    const max_concurrent = try optIntAs(u32, d, sched_obj, "max_concurrent") orelse 1;
    const input = try optString(d, sched_obj, "input");
    const paused = try optBool(d, sched_obj, "paused") orelse false;

    const cron_d: ?[]u8 = if (cron) |c| try dupe(allocator, c) else null;
    errdefer if (cron_d) |c| allocator.free(c);
    return ScheduleDef{
        .cron_expr = cron_d,
        .interval_ms = interval,
        .max_concurrent = max_concurrent,
        .input = if (input) |inp| try dupe(allocator, inp) else null,
        .paused = paused,
    };
}

/// Optional stream trigger block:
/// ```yaml
/// trigger:
///   stream: "orders"               # source stream (required)
///   namespace: "prod"              # source namespace (optional)
///   consumer_group: "wf-orders"    # consumer group name (optional)
///   mode: shared                   # shared | exclusive | key_shared
///   batch_size: 1                  # events per workflow run
///   batch_timeout_ms: 5000
/// ```
fn parseTrigger(allocator: Allocator, root: JsonValue, d: D) ParseError!?StreamTriggerDef {
    const trig_obj = try optObject(d, root, "trigger") orelse return null;
    const mark = d.push("trigger");
    defer d.pop(mark);
    try checkKeys(d, trig_obj, &.{ "stream", "namespace", "consumer_group", "mode", "batch_size", "batch_timeout_ms" });

    const stream = try reqString(d, trig_obj, "stream");
    if (stream.len == 0) return d.fail(ParseError.InvalidFieldType, "\"stream\" must not be empty", .{});
    const namespace = try optString(d, trig_obj, "namespace");
    const consumer_group = try optString(d, trig_obj, "consumer_group");
    const mode = try oneOf(TriggerMode, d, "mode", try optString(d, trig_obj, "mode") orelse "shared", &.{
        .{ "shared", .shared }, .{ "exclusive", .exclusive }, .{ "key_shared", .key_shared },
    }, ParseError.InvalidFieldType);
    const batch_size = try optIntAs(u32, d, trig_obj, "batch_size") orelse 1;
    if (batch_size == 0) return d.fail(ParseError.InvalidFieldType, "\"batch_size\" must be at least 1", .{});
    const batch_timeout_ms = try optIntAs(u32, d, trig_obj, "batch_timeout_ms") orelse 5000;

    var trig = StreamTriggerDef{
        .stream = try dupe(allocator, stream),
        .mode = mode,
        .batch_size = batch_size,
        .batch_timeout_ms = batch_timeout_ms,
    };
    errdefer trig.deinit(allocator);
    if (namespace) |ns| trig.namespace = try dupe(allocator, ns);
    if (consumer_group) |cg| trig.consumer_group = try dupe(allocator, cg);
    return trig;
}

fn parseBackoff(d: D, obj: JsonValue) ParseError!BackoffType {
    return oneOf(BackoffType, d, "backoff", try optString(d, obj, "backoff") orelse "exponential", &.{
        .{ "constant", .constant },       .{ "linear", .linear },
        .{ "exponential", .exponential }, .{ "exponential_jitter", .exponential_jitter },
    }, ParseError.InvalidBackoffType);
}

fn parseRetryPolicy(obj: JsonValue, d: D) ParseError!RetryPolicy {
    try checkKeys(d, obj, &.{ "max_attempts", "initial_delay_ms", "max_delay_ms", "within_ms", "backoff" });
    return .{
        .max_attempts = try optIntAs(u32, d, obj, "max_attempts") orelse 3,
        .backoff = try parseBackoff(d, obj),
        .initial_delay_ms = try optIntAs(u32, d, obj, "initial_delay_ms") orelse 1000,
        .max_delay_ms = try optIntAs(u32, d, obj, "max_delay_ms") orelse 30000,
        .within_ms = try optIntAs(u64, d, obj, "within_ms"),
    };
}

fn parsePollConfig(obj: JsonValue, d: D) ParseError!definition.PollConfig {
    try checkKeys(d, obj, &.{ "initial_delay_ms", "max_attempts", "base_delay_ms", "max_delay_ms", "backoff" });
    return .{
        .initial_delay_ms = try optAtLeast(d, obj, "initial_delay_ms", 0) orelse 0,
        .max_attempts = try optIntAs(u32, d, obj, "max_attempts") orelse 10,
        .backoff = try parseBackoff(d, obj),
        .base_delay_ms = try optIntAs(u32, d, obj, "base_delay_ms") orelse 1000,
        .max_delay_ms = try optIntAs(u32, d, obj, "max_delay_ms") orelse 60000,
    };
}

// =============================================================================
// Plan Parsers
// =============================================================================

fn parseStringList(allocator: Allocator, d: D, obj: JsonValue, key: []const u8, list: *std.ArrayList([]const u8)) ParseError!void {
    const arr = try optArray(d, obj, key) orelse return;
    const mark = d.push(key);
    defer d.pop(mark);
    for (arr, 0..) |item, i| {
        if (item != .string) {
            const im = d.pushIndex(i);
            defer d.pop(im);
            return d.fail(ParseError.InvalidFieldType, "must be a string, not {s}", .{kindName(item)});
        }
        const s = try dupe(allocator, item.string);
        list.append(allocator, s) catch {
            allocator.free(s);
            return ParseError.OutOfMemory;
        };
    }
}

fn freeStringList(allocator: Allocator, list: *std.ArrayList([]const u8)) void {
    for (list.items) |s| allocator.free(s);
    list.deinit(allocator);
}

fn parseErrorClassification(allocator: Allocator, root: JsonValue, d: D) ParseError!?ErrorClassification {
    const classify_obj = try optObject(d, root, "errors") orelse return null;
    const mark = d.push("errors");
    defer d.pop(mark);
    try checkKeys(d, classify_obj, &.{ "retryable", "fatal" });

    var retryable: std.ArrayList([]const u8) = .empty;
    errdefer freeStringList(allocator, &retryable);
    var fatal: std.ArrayList([]const u8) = .empty;
    errdefer freeStringList(allocator, &fatal);

    try parseStringList(allocator, d, classify_obj, "retryable", &retryable);
    try parseStringList(allocator, d, classify_obj, "fatal", &fatal);

    const retryable_s = retryable.toOwnedSlice(allocator) catch return ParseError.OutOfMemory;
    errdefer {
        for (retryable_s) |s| allocator.free(s);
        allocator.free(retryable_s);
    }
    return ErrorClassification{
        .retryable = retryable_s,
        .fatal = fatal.toOwnedSlice(allocator) catch return ParseError.OutOfMemory,
    };
}

fn parseExecutors(allocator: Allocator, root: JsonValue, d: D) ParseError![]ExecutorConfig {
    const arr = try optArray(d, root, "executors") orelse return missing(d, "executors");
    const list_mark = d.push("executors");
    defer d.pop(list_mark);

    var executors: std.ArrayList(ExecutorConfig) = .empty;
    errdefer {
        for (executors.items) |*e| e.deinit(allocator);
        executors.deinit(allocator);
    }

    for (arr, 0..) |item, i| {
        const mark = d.pushIndex(i);
        defer d.pop(mark);
        if (item != .object) return d.fail(ParseError.InvalidFieldType, "an executor must be a map, not {s}", .{kindName(item)});
        var exec = try parseExecutorConfig(allocator, item, d);
        errdefer exec.deinit(allocator);
        executors.append(allocator, exec) catch return ParseError.OutOfMemory;
    }

    return executors.toOwnedSlice(allocator) catch ParseError.OutOfMemory;
}

fn parseExecutorConfig(allocator: Allocator, obj: JsonValue, d: D) ParseError!ExecutorConfig {
    try checkKeys(d, obj, &.{ "name", "run", "priority", "retry", "breaker", "tracking", "rate_limit" });
    const name = try reqString(d, obj, "name");
    const action = try reqString(d, obj, "run");
    const priority = try optIntAs(i32, d, obj, "priority") orelse 100;

    const retry: ?RetryPolicy = if (try optObject(d, obj, "retry")) |r| blk: {
        const mark = d.push("retry");
        defer d.pop(mark);
        break :blk try parseRetryPolicy(r, d);
    } else null;
    const breaker: ?CircuitBreakerConfig = if (try optObject(d, obj, "breaker")) |b| blk: {
        const mark = d.push("breaker");
        defer d.pop(mark);
        break :blk try parseCircuitBreakerConfig(b, d);
    } else null;
    const tracking: ?TrackingConfig = if (try optObject(d, obj, "tracking")) |t| blk: {
        const mark = d.push("tracking");
        defer d.pop(mark);
        break :blk try parseTrackingConfig(t, d);
    } else null;
    const rate_limit: ?RateLimitConfig = if (try optObject(d, obj, "rate_limit")) |r| blk: {
        const mark = d.push("rate_limit");
        defer d.pop(mark);
        break :blk try parseRateLimitConfig(r, d);
    } else null;

    const name_d = try dupe(allocator, name);
    errdefer allocator.free(name_d);
    return ExecutorConfig{
        .name = name_d,
        .action_name = try dupe(allocator, action),
        .priority = priority,
        .retry = retry,
        .breaker = breaker,
        .tracking = tracking,
        .rate_limit = rate_limit,
    };
}

fn parseCircuitBreakerConfig(obj: JsonValue, d: D) ParseError!CircuitBreakerConfig {
    try checkKeys(d, obj, &.{ "failure_threshold", "cooldown_ms", "half_open_max_calls" });
    return .{
        .failure_threshold = try optIntAs(u32, d, obj, "failure_threshold") orelse 5,
        .cooldown_ms = try optAtLeast(d, obj, "cooldown_ms", 1) orelse 60000,
        .half_open_max_calls = try optIntAs(u32, d, obj, "half_open_max_calls") orelse 2,
    };
}

fn parseTrackingConfig(obj: JsonValue, d: D) ParseError!TrackingConfig {
    try checkKeys(d, obj, &.{ "mode", "timeout_ms" });
    return .{
        .mode = try oneOf(TrackingMode, d, "mode", try optString(d, obj, "mode") orelse "sync", &.{
            .{ "sync", .sync }, .{ "async", .async_mode },
        }, ParseError.InvalidTrackingMode),
        .timeout_ms = try optAtLeast(d, obj, "timeout_ms", 1),
    };
}

fn parseRateLimitConfig(obj: JsonValue, d: D) ParseError!RateLimitConfig {
    try checkKeys(d, obj, &.{ "max_per_second", "max_per_minute", "max_per_hour" });
    return .{
        .max_per_second = try optIntAs(u32, d, obj, "max_per_second"),
        .max_per_minute = try optIntAs(u32, d, obj, "max_per_minute"),
        .max_per_hour = try optIntAs(u32, d, obj, "max_per_hour"),
    };
}

fn parseHealthConfig(root: JsonValue, d: D) ParseError!?HealthConfig {
    const health_obj = try optObject(d, root, "health") orelse return null;
    const mark = d.push("health");
    defer d.pop(mark);
    try checkKeys(d, health_obj, &.{ "window_ms", "decay", "min_samples" });

    const window_ms: i64 = try optIntAs(u32, d, health_obj, "window_ms") orelse 300000;
    if (window_ms == 0) return d.fail(ParseError.InvalidFieldType, "\"window_ms\" must be at least 1", .{});

    return .{
        .window_ms = window_ms,
        .decay = try optFloat(d, health_obj, "decay") orelse 0.9,
        .min_samples = try optIntAs(u32, d, health_obj, "min_samples") orelse 50,
    };
}

fn parseCacheConfig(allocator: Allocator, root: JsonValue, d: D) ParseError!?CacheConfig {
    const cache_obj = try optObject(d, root, "cache") orelse return null;
    const mark = d.push("cache");
    defer d.pop(mark);
    try checkKeys(d, cache_obj, &.{ "ttl_ms", "key", "invalidate_on" });

    const ttl_ms = try optAtLeast(d, cache_obj, "ttl_ms", 1) orelse 300000;
    const key_template = try reqString(d, cache_obj, "key");

    var invalidate_on: std.ArrayList([]const u8) = .empty;
    errdefer freeStringList(allocator, &invalidate_on);
    try parseStringList(allocator, d, cache_obj, "invalidate_on", &invalidate_on);

    const key_d = try dupe(allocator, key_template);
    errdefer allocator.free(key_d);
    return CacheConfig{
        .ttl_ms = ttl_ms,
        .key_template = key_d,
        .invalidate_on = invalidate_on.toOwnedSlice(allocator) catch return ParseError.OutOfMemory,
    };
}

fn parseFallbackConfig(allocator: Allocator, root: JsonValue, d: D) ParseError!?FallbackConfig {
    const fallback_obj = try optObject(d, root, "fallback") orelse return null;
    const mark = d.push("fallback");
    defer d.pop(mark);
    try checkKeys(d, fallback_obj, &.{ "value", "condition" });

    const value = try reqString(d, fallback_obj, "value");
    const condition = try oneOf(FallbackCondition, d, "condition", try optString(d, fallback_obj, "condition") orelse "exhausted", &.{
        .{ "exhausted", .exhausted }, .{ "any_error", .any_error },
    }, ParseError.InvalidFallbackCondition);

    return FallbackConfig{
        .value = try dupe(allocator, value),
        .condition = condition,
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

    var def = try parseWorkflow(allocator, json, null);
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
        \\    "poll":  { "max_attempts": 3, "backoff": "exponential_jitter", "base_delay_ms": 10 },
        \\    "transitions": { "success": "flo.Completed", "failure": "flo.Failed" }
        \\  }
        \\}
    ;

    var def = try parseWorkflow(allocator, json, null);
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
        \\  "search_attributes": [
        \\    {"name": "customer_id", "type": "string", "from": "input.customer_id"},
        \\    {"name": "order_amount", "type": "number", "from": "input.amount"}
        \\  ],
        \\  "start": {
        \\    "run": "@actions/validate",
        \\    "transitions": {"success": "flo.Completed"}
        \\  }
        \\}
    ;

    var def = try parseWorkflow(allocator, json, null);
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

    var def = try parseWorkflow(allocator, json, null);
    defer def.deinit(allocator);

    try testing.expectEqual(@as(usize, 2), def.terminals.len);
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

    var def = parseWorkflow(allocator, yaml, null) catch |err| {
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

    var def = parseWorkflow(allocator, yaml, null) catch |err| {
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

    var def = parseWorkflow(allocator, yaml, null) catch |err| {
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

    var def = parseWorkflow(allocator, yaml, null) catch |err| {
        std.debug.print("Parse error: {any}\n", .{err});
        return err;
    };
    defer def.deinit(allocator);

    try testing.expectEqualStrings("audio-pipeline", def.name);
    try testing.expect(def.output != null);
    try testing.expectEqualStrings("$.steps.encode.output", def.output.?);
}

test "parseWorkflow: a count or delay that doesn't fit its field is refused" {
    const testing = std.testing;
    const allocator = testing.allocator;
    for ([_][]const u8{
        "\"retry\": { \"max_attempts\": -1 }",
        "\"retry\": { \"initial_delay_ms\": 5000000000 }",
        "\"poll\": { \"max_attempts\": 5000000000 }",
        "\"poll\": { \"base_delay_ms\": -1 }",
    }) |field| {
        const json = try std.fmt.allocPrint(allocator,
            \\{{ "kind": "Workflow", "name": "w", "version": "1",
            \\  "start": {{ "run": "@actions/x", {s},
            \\    "transitions": {{ "success": "flo.Completed", "failure": "flo.Failed" }} }} }}
        , .{field});
        defer allocator.free(json);
        try testing.expectError(ParseError.InvalidFieldType, parseWorkflow(allocator, json, null));
    }
}


/// The diagnostic a definition is refused with.
fn expectRefused(content: []const u8, expected: ParseError, message: []const u8) !void {
    var diag: Diagnostic = .{};
    try std.testing.expectError(expected, parseWorkflow(std.testing.allocator, content, &diag));
    try std.testing.expectEqualStrings(message, diag.message());
}

fn wrap(comptime start_extra: []const u8, comptime top_extra: []const u8) []const u8 {
    return
    \\{ "kind": "Workflow", "name": "w", "version": "1",
    ++ top_extra ++
    \\  "start": { "run": "@actions/x",
    ++ start_extra ++
    \\    "transitions": { "success": "flo.Completed" } } }
    ;
}

test "parseWorkflow: an unknown key is refused by name and place" {
    try expectRefused(wrap("", "\"trasitions\": {},"), ParseError.UnknownKey, "unknown key \"trasitions\" at the top level");
    try expectRefused(wrap("\"transition\": {},", ""), ParseError.UnknownKey, "unknown key \"transition\" at start");
    try expectRefused(wrap("\"retry\": { \"max\": 3 },", ""), ParseError.UnknownKey, "unknown key \"max\" at start.retry");
    try expectRefused(wrap("\"poll\": { \"maxAttempts\": 3 },", ""), ParseError.UnknownKey, "unknown key \"maxAttempts\" at start.poll");
    try expectRefused(wrap("\"inputMapping\": \"{}\",", ""), ParseError.UnknownKey, "unknown key \"inputMapping\" at start");
    try expectRefused(wrap("", "\"steps\": { \"b\": { \"wait_for_signal\": { \"type\": \"go\", \"timeoutMs\": 5 } } },"), ParseError.UnknownKey, "unknown key \"timeoutMs\" at steps.b.wait_for_signal");
    try expectRefused(wrap("", "\"trigger\": { \"stream\": \"s\", \"consumerGroup\": \"g\" },"), ParseError.UnknownKey, "unknown key \"consumerGroup\" at trigger");
    try expectRefused(wrap("", "\"search_attributes\": [ { \"name\": \"a\", \"from\": \"$.input.a\", \"kind\": \"string\" } ],"), ParseError.UnknownKey, "unknown key \"kind\" at search_attributes[0]");
    try expectRefused(wrap("",
        \\"plans": { "p": { "executors": [ { "name": "e", "run": "@actions/e", "breaker": { "cooldownMs": 1 } } ] } },
    ), ParseError.UnknownKey, "unknown key \"cooldownMs\" at plans.p.executors[0].breaker");
    try expectRefused(wrap("", "\"terminals\": { \"T\": { \"status\": \"failed\", \"code\": 1 } },"), ParseError.UnknownKey, "unknown key \"code\" at terminals.T");
}

test "parseWorkflow: a value of the wrong kind is refused, not defaulted" {
    try expectRefused(wrap("\"retry\": \"3\",", ""), ParseError.InvalidFieldType, "\"retry\" must be a map, not a string at start");
    try expectRefused(wrap("\"retry\": { \"max_attempts\": \"3\" },", ""), ParseError.InvalidFieldType, "\"max_attempts\" must be an integer, not a string at start.retry");
    try expectRefused(wrap("", "\"steps\": { \"b\": \"oops\" },"), ParseError.InvalidFieldType, "a step must be a map, not a string at steps.b");
    try expectRefused(
        \\{ "kind": "Workflow", "name": "w", "version": "1",
        \\  "start": { "run": "@actions/x", "transitions": { "success": 3 } } }
    , ParseError.InvalidFieldType, "\"success\" must be a step or terminal name, not an integer at start.transitions");
    try expectRefused(wrap("", "\"search_attributes\": [ \"a\" ],"), ParseError.InvalidFieldType, "a search attribute must be a map, not a string at search_attributes[0]");
}

test "parseWorkflow: an unknown enum value is refused, not defaulted" {
    try expectRefused(wrap("\"retry\": { \"backoff\": \"exp-jitter-200ms\" },", ""), ParseError.InvalidBackoffType, "\"backoff\" must be one of constant|linear|exponential|exponential_jitter, not \"exp-jitter-200ms\" at start.retry");
    try expectRefused(wrap("", "\"terminals\": { \"T\": { \"status\": \"done\" } },"), ParseError.InvalidFieldType, "\"status\" must be one of completed|failed|cancelled|timed_out, not \"done\" at terminals.T");
    try expectRefused(wrap("", "\"trigger\": { \"stream\": \"s\", \"mode\": \"key-shared\" },"), ParseError.InvalidFieldType, "\"mode\" must be one of shared|exclusive|key_shared, not \"key-shared\" at trigger");
    try expectRefused(wrap("",
        \\"plans": { "p": { "selection": "round_robin", "executors": [ { "name": "e", "run": "@actions/e" } ] } },
    ), ParseError.InvalidSelectionStrategy, "\"selection\" must be one of static-order|round-robin|random|health-weighted, not \"round_robin\" at plans.p");
    try expectRefused(wrap("",
        \\"plans": { "p": { "executors": [ { "name": "e", "run": "@actions/e", "tracking": { "mode": "later" } } ] } },
    ), ParseError.InvalidTrackingMode, "\"mode\" must be one of sync|async, not \"later\" at plans.p.executors[0].tracking");
    try expectRefused(wrap("",
        \\"plans": { "p": { "executors": [ { "name": "e", "run": "@actions/e" } ], "fallback": { "value": "{}", "condition": "any" } } },
    ), ParseError.InvalidFallbackCondition, "\"condition\" must be one of exhausted|any_error, not \"any\" at plans.p.fallback");
    try expectRefused(wrap("",
        \\"plans": { "p": { "executors": [ { "name": "e", "run": "@actions/e" } ], "health": { "window": "5m" } } },
    ), ParseError.UnknownKey, "unknown key \"window\" at plans.p.health");
}

test "parseWorkflow: a required key that was silently optional is required" {
    try expectRefused(wrap("",
        \\"plans": { "p": { "executors": [ { "name": "e", "run": "@actions/e" } ], "cache": { "ttl_ms": 5 } } },
    ), ParseError.MissingRequiredField, "missing required key \"key\" at plans.p.cache");
    try expectRefused(wrap("", "\"search_attributes\": [ { \"name\": \"a\" } ],"), ParseError.MissingRequiredField, "missing required key \"from\" at search_attributes[0]");
    try expectRefused(
        \\{ "kind": "Workflow", "name": "w", "version": "1", "start": { "transitions": {} } }
    , ParseError.MissingRequiredField, "a step needs \"run\" or \"wait_for_signal\" at start");
}

test "parseWorkflow: a duplicated key is refused by name, in JSON and YAML" {
    try expectRefused(
        \\{ "kind": "Workflow", "name": "w", "version": "1",
        \\  "start": { "run": "@actions/x", "run": "@actions/y", "transitions": {} } }
    , ParseError.DuplicateKey, "key \"run\" appears twice at start");
    try expectRefused(
        \\kind: Workflow
        \\name: w
        \\version: "1"
        \\start:
        \\  run: "@actions/x"
        \\  transitions:
        \\    success: flo.Completed
        \\    success: flo.Failed
    , ParseError.DuplicateKey, "key \"success\" appears twice at start.transitions");
}

test "parseWorkflow: snake_case keys parse" {
    const allocator = std.testing.allocator;
    var diag: Diagnostic = .{};
    var def = parseWorkflow(allocator,
        \\kind: Workflow
        \\name: all-keys
        \\version: "1"
        \\search_attributes:
        \\  - name: customer
        \\    from: $.input.customer
        \\trigger:
        \\  stream: orders
        \\  consumer_group: g
        \\  batch_size: 2
        \\  batch_timeout_ms: 100
        \\plans:
        \\  pay:
        \\    selection: round-robin
        \\    errors:
        \\      retryable: [timeout]
        \\    executors:
        \\      - name: a
        \\        run: "@actions/a"
        \\        rate_limit:
        \\          max_per_second: 5
        \\        breaker:
        \\          failure_threshold: 3
        \\          cooldown_ms: 100
        \\          half_open_max_calls: 1
        \\    health:
        \\      window_ms: 60000
        \\      min_samples: 10
        \\    cache:
        \\      ttl_ms: 1000
        \\      key: "k"
        \\      invalidate_on: [x]
        \\start:
        \\  run: "@plan/pay"
        \\  input_mapping: '{"a": "$.input.a"}'
        \\  poll:
        \\    initial_delay_ms: 0
        \\    max_attempts: 2
        \\    base_delay_ms: 10
        \\    max_delay_ms: 20
        \\  transitions:
        \\    success: wait
        \\steps:
        \\  wait:
        \\    wait_for_signal:
        \\      type: approval
        \\      timeout_ms: 1000
        \\      on_timeout: flo.Failed
        \\    transitions:
        \\      success: flo.Completed
    , &diag) catch |err| {
        std.debug.print("refused: {s}\n", .{diag.message()});
        return err;
    };
    defer def.deinit(allocator);
    try std.testing.expectEqualStrings("{\"a\": \"$.input.a\"}", def.start.run.input_mapping.?);
    try std.testing.expectEqual(@as(u32, 2), def.start.run.poll.?.max_attempts);
    try std.testing.expectEqual(@as(?i64, 1000), def.steps[0].step.wait_for_signal.timeout_ms);
    try std.testing.expectEqualStrings("g", def.trigger.?.consumer_group.?);
    try std.testing.expectEqual(@as(?u32, 5), def.plans[0].executors[0].rate_limit.?.max_per_second);
    try std.testing.expectEqual(@as(u32, 10), def.plans[0].health_config.?.min_samples);
    try std.testing.expectEqual(@as(i64, 60000), def.plans[0].health_config.?.window_ms);
    try std.testing.expectEqualStrings("k", def.plans[0].cache_config.?.key_template);
    try std.testing.expectEqual(@as(usize, 1), def.plans[0].error_classification.?.retryable.len);
}

test "parseWorkflow: a negative or zero duration is refused" {
    try expectRefused(wrap("\"poll\": { \"initial_delay_ms\": -1 },", ""), ParseError.InvalidFieldType, "\"initial_delay_ms\" must be at least 0, not -1 at start.poll");
    try expectRefused(wrap("", "\"steps\": { \"b\": { \"wait_for_signal\": { \"type\": \"go\", \"timeout_ms\": 0 } } },"), ParseError.InvalidFieldType, "\"timeout_ms\" must be at least 1, not 0 at steps.b.wait_for_signal");
    try expectRefused(wrap("", "\"schedule\": { \"interval\": -5 },"), ParseError.InvalidFieldType, "\"interval\" must be at least 1, not -5 at schedule");
    try expectRefused(wrap("",
        \\"plans": { "p": { "executors": [ { "name": "e", "run": "@actions/e", "breaker": { "cooldown_ms": -1 }, "tracking": { "timeout_ms": 0 } } ] } },
    ), ParseError.InvalidFieldType, "\"cooldown_ms\" must be at least 1, not -1 at plans.p.executors[0].breaker");
}
